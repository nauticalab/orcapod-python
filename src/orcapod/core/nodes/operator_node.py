"""OperatorNode hierarchy — pure blueprint + DB-backed execution node.

Three classes:

* ``OperatorNodeBase`` — shared base; no DB state.  Holds identity,
  schema, and all non-DB properties.
* ``OperatorNode`` — thin blueprint descriptor.  Raises
  ``PipelineJobRequiredError`` on ``iter_data()``.  This is the node
  recorded in a ``Pipeline`` and serialized to disk.
* ``OperatorJobNode`` — DB-backed execution node with full persistence
  logic.  Created by ``PipelineJob`` at run time.

``OperatorNode`` and ``OperatorJobNode`` are *siblings*: both inherit
directly from ``OperatorNodeBase``, neither from the other.
"""

from __future__ import annotations

import asyncio
import dataclasses
import sys
import logging
from abc import abstractmethod
from collections.abc import Collection, Iterator, Mapping, Sequence
from typing import TYPE_CHECKING, Any, Literal, cast

from orcapod import contexts
from orcapod.channels import Channel, ChannelClosed, ReadableChannel, WritableChannel
from orcapod.config import OrcapodConfig
from orcapod.core.operators.static_output_pod import StaticOutputOperatorPod
from orcapod.core.streams.arrow_table_stream import ArrowTableStream
from orcapod.core.streams.base import (
    ZERO_ROW_INPUT_COLUMNS,
    StreamBase,
    check_head_size,
    short_stream_repr,
)
from orcapod.core.streams.view import (
    JOB_NODE_NOT_RUN_NOTE,
    LOADED_ONLY_OPERATOR_NOTE,
    STALE_NOTE,
    BLUEPRINT_NOTE,
    ViewStatus,
    StreamView,
    head_of,
)
from orcapod.core.tracker import DEFAULT_TRACKER_MANAGER
from orcapod.core.nodes.staleness import RunHoldingNode
from orcapod.errors import (
    PipelineJobRequiredError,
    StoreReadError,
    StoreWriteError,
)
from orcapod.contexts.stored import (
    keep_rows_of_data_context,
    require_same_data_context,
    account_for_stale_tables,
    describe_store_table,
    find_stale_tables,
    report_rows_of_other_data_contexts,
    report_unexpected_row_count,
    with_data_context,
)
from orcapod.protocols.core_protocols import (
    DataProtocol,
    StreamProtocol,
    TagProtocol,
    TrackerManagerProtocol,
)
from orcapod.protocols.core_protocols.operator_pod import OperatorPodProtocol
from orcapod.protocols.database_protocols import ArrowDatabaseProtocol
from orcapod.system_constants import OPERATOR_DB_SCHEMA_VERSION, constants
from orcapod.types import CacheMode, ColumnConfig, ContentHash, Schema
from orcapod.utils import arrow_utils
from orcapod.utils.lazy_module import LazyModule

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    import pyarrow as pa
    import pyarrow.compute as pc

    from orcapod.protocols.observability_protocols import ExecutionObserverProtocol
else:
    pa = LazyModule("pyarrow")
    pc = LazyModule("pyarrow.compute")


async def _discard(channel: ReadableChannel[Any]) -> None:
    """Read ``channel`` to its end, keeping nothing.

    Args:
        channel: The channel to drain.
    """
    async for _ in channel:
        pass


class _InputTap:
    """An input channel reader that keeps the rows needed to rebuild its columns.

    An async operator node hands its input channels to the operator, so it sees
    no input row itself. When the run produces no output row, the node rebuilds
    each input with no rows and applies the operator to those, as the sync path
    applies it to the rebuilt upstream rows (ITL-1080). Those columns and types
    are what ``StaticOutputOperatorPod._rows_to_table`` makes of the input's
    rows, and the only thing in them that depends on more than one row is the
    nullability of a source-info column (nullable when some row's token is
    ``None``). So the tap keeps the first row, and the first row with a ``None``
    token for each source-info column, never the whole input.

    Attributes:
        rows: The kept rows, in arrival order.
    """

    def __init__(self, reader: ReadableChannel[tuple[TagProtocol, DataProtocol]]):
        self._reader = reader
        self.rows: list[tuple[TagProtocol, DataProtocol]] = []
        self._null_sources: set[str] = set()

    def _keep(self, item: tuple[TagProtocol, DataProtocol]) -> None:
        """Keep ``item`` if it is the first row or has a new ``None`` token."""
        nulls = {name for name, token in item[1].source_info().items() if token is None}
        if not self.rows or not nulls <= self._null_sources:
            self.rows.append(item)
            self._null_sources |= nulls

    async def receive(self) -> tuple[TagProtocol, DataProtocol]:
        """Receive the next row from the wrapped reader, keeping it if needed."""
        item = await self._reader.receive()
        self._keep(item)
        return item

    def __aiter__(self) -> "_InputTap":
        return self

    async def __anext__(self) -> tuple[TagProtocol, DataProtocol]:
        try:
            return await self.receive()
        except ChannelClosed:
            raise StopAsyncIteration

    async def collect(self) -> list[tuple[TagProtocol, DataProtocol]]:
        """Drain every remaining row."""
        return [item async for item in self]


# ---------------------------------------------------------------------------
# OperatorNodeBase — shared base (no DB)
# ---------------------------------------------------------------------------


class _LoadedCacheUnreadableError(RuntimeError):
    """A loaded operator node in REPLAY cannot read its run's stored rows.

    ``as_table()`` and ``iter_data()`` raise it; ``head()``, which never raises on a
    loaded node, shows no rows with ``head_note`` as the reason.

    Attributes:
        head_note: The short reason a head shows.
    """

    def __init__(self, message: str, head_note: str) -> None:
        super().__init__(message)
        self.head_note = head_note


class OperatorNodeBase(StreamBase):
    """Shared base for ``OperatorNode`` and ``OperatorJobNode``.

    Carries all non-DB state: identity, schema, upstreams, and properties
    shared by both the blueprint and the execution variant.
    """

    node_type = "operator"
    HASH_COLUMN_NAME = "_record_hash"
    # The content hash a saved job recorded for this node's run (ITL-1043); set only
    # on a node rebuilt by ``OperatorJobNode.from_descriptor``. A class default, so
    # nodes built without ``__init__`` have it too.
    _saved_run_content_hash: str | None = None

    def __init__(
        self,
        operator: OperatorPodProtocol,
        input_streams: Collection[StreamProtocol],
        tracker_manager: TrackerManagerProtocol | None = None,
        label: str | None = None,
        config: OrcapodConfig | None = None,
        table_scope: Literal["pipeline_hash", "content_hash"] = "pipeline_hash",
    ) -> None:
        """Initialize the shared operator-node state.

        Args:
            operator: The operator pod that defines the transformation logic
                and output schema.
            input_streams: One or more upstream streams consumed by this
                operator.  Passed through as a tuple.
            tracker_manager: Optional tracker manager override.  Defaults to
                ``DEFAULT_TRACKER_MANAGER``.
            label: Optional display label for this node.
            config: Optional config override; defaults to the global config.
            table_scope: Determines how the database table path is scoped.
                ``"pipeline_hash"`` (default) shares a table across all runs
                with the same topology and schemas regardless of which concrete
                data is bound.  ``"content_hash"`` creates a separate table per
                unique data combination.
        """
        if tracker_manager is None:
            tracker_manager = DEFAULT_TRACKER_MANAGER
        self.tracker_manager = tracker_manager

        self._operator = operator
        self._input_streams = tuple(input_streams)

        super().__init__(label=label, config=config)

        # Validate inputs eagerly
        self._operator.validate_inputs(*self._input_streams)

        self._set_modified_time(None)

        # Descriptor fields — populated by from_descriptor() for read-only/UNAVAILABLE
        # nodes. Initialized here so they are always present on the concrete class
        # (avoids getattr access for possibly-absent attributes).
        from orcapod.pipeline.serialization import LoadStatus

        self._load_status: LoadStatus = LoadStatus.FULL
        self._stored_content_hash: str | None = None
        self._stored_pipeline_hash: str | None = None
        self._stored_schema: dict = {}
        self._stored_node_uri: tuple[str, ...] = ()
        self._stored_pipeline_path: tuple[str, ...] = ()
        self._descriptor: dict = {}

        if table_scope not in ("pipeline_hash", "content_hash"):
            raise ValueError(
                f"Unknown table_scope {table_scope!r}. "
                "Expected one of: 'pipeline_hash', 'content_hash'."
            )
        self._table_scope = table_scope
        self._node_identity_path_cache: tuple[str, ...] | None = None

    # ------------------------------------------------------------------
    # load_status
    # ------------------------------------------------------------------

    @property
    def load_status(self) -> Any:
        """Return the load status of this node.

        Returns:
            The ``LoadStatus`` enum value indicating how this node was
            loaded.  Defaults to ``FULL`` for nodes created via
            ``__init__``.
        """
        return self._load_status

    # ------------------------------------------------------------------
    # Identity
    # ------------------------------------------------------------------

    def identity_structure(self) -> Any:
        return self.pipeline_identity_structure()

    def pipeline_identity_structure(self) -> Any:
        return (self._operator, self._operator.argument_symmetry(self._input_streams))

    # ------------------------------------------------------------------
    # Read-only overrides (for deserialized nodes without live operator)
    # ------------------------------------------------------------------

    def content_hash(self, hasher=None) -> ContentHash:
        """Return the content hash: the run's when a loaded job recorded it.

        A node of a job loaded from a file that records its run's content hash returns
        that: the rows the run stored carry it, and inputs rebuilt from the file (an
        in-memory source is not saved) cannot reproduce it (ITL-1043). Otherwise a
        read-only node returns its descriptor's value, and any other node computes it.
        """
        if self._saved_run_content_hash is not None:
            return ContentHash.from_string(self._saved_run_content_hash)
        if self._operator is None and self._stored_content_hash is not None:
            return ContentHash.from_string(self._stored_content_hash)
        return super().content_hash(hasher)

    def pipeline_hash(self, hasher=None) -> ContentHash:
        """Return the pipeline hash; a node rebuilt from a descriptor returns the saved one.

        A node rebuilt with its operator still returns the saved value: its inputs are
        rebuilt from the saved schemas, which do not keep every property that the run's
        inputs hashed (ITL-1043: a column's nullability), so recomputing could give
        another value and another table path than the run's.
        """
        if self._stored_pipeline_hash is not None:
            return ContentHash.from_string(self._stored_pipeline_hash)
        return super().pipeline_hash(hasher)

    # ------------------------------------------------------------------
    # Stream interface
    # ------------------------------------------------------------------

    @property
    def producer(self) -> OperatorPodProtocol:
        return self._operator

    @property
    def data_context(self) -> contexts.DataContext:
        return contexts.resolve_context(self.data_context_key)

    @property
    def data_context_key(self) -> str:
        """Key of the node's data context.

        A node loaded from a saved job without its operator (read-only) has the data
        context saved in its descriptor.
        """
        if self._operator is None:
            return self._data_context.context_key
        return self._operator.data_context_key

    @property
    def upstreams(self) -> tuple[StreamProtocol, ...]:
        return self._input_streams

    @upstreams.setter
    def upstreams(self, value: tuple[StreamProtocol, ...]) -> None:
        self._input_streams = value
        self._forget_output_schema()

    def keys(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        if self._operator is None:
            tag_keys = tuple(self._stored_schema.get("tag", {}).keys())
            data_keys = tuple(self._stored_schema.get("data", {}).keys())
            return tag_keys, data_keys
        tag_schema, data_schema = self.output_schema(columns=columns, all_info=all_info)
        return tuple(tag_schema.keys()), tuple(data_schema.keys())

    def output_schema(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        """Return output schema, using stored value in read-only mode.

        Raises:
            ValueError: If the node was loaded and a column's saved type cannot be
                restored to a Python type (an unregistered logical type).
        """
        if self._operator is None:
            from orcapod.pipeline.serialization import (
                deserialize_output_schema,
                SavedSchemaVersionError,
            )
            from orcapod.logical_types.identifier import LogicalTypeIdentifierError

            try:
                return deserialize_output_schema(
                    self._stored_schema,
                    self.data_context.type_converter,
                )
            except (LogicalTypeIdentifierError, SavedSchemaVersionError) as exc:
                exc.args = (
                    f"Cannot restore the output schema of loaded node {self.label!r}: {exc}",
                )
                raise
            except ValueError as exc:
                raise ValueError(
                    f"Cannot restore the output schema of loaded node {self.label!r}: "
                    f"{exc}"
                ) from exc
        # Memoized (ITL-1111). The prediction depends on the operator's configuration
        # and on the input streams' schemas and pipeline hashes; the ``upstreams``
        # setter and a job's ``bind`` clear it when an input changes. Never on rows.
        operator = self._operator
        return self._memoized_output_schema(
            columns,
            all_info,
            lambda: operator.output_schema(
                *self._input_streams, columns=columns, all_info=all_info
            ),
        )

    # ------------------------------------------------------------------
    # Node identity path
    # ------------------------------------------------------------------

    @property
    def node_identity_path(self) -> tuple[str, ...]:
        """Return the node identity path for observer contextualization.

        When ``table_scope="pipeline_hash"`` (default) the path is
        ``operator.uri + (schema:{pipeline_hash},)`` — all runs that share
        the same pipeline structure use one shared table, with per-run
        disambiguation via the ``_node_content_hash`` row-level column.

        When ``table_scope="content_hash"`` the legacy path is returned:
        ``operator.uri + (schema:{pipeline_hash}, instance:{content_hash})``.

        In read-only/UNAVAILABLE mode (no operator) the path stored from the
        deserialized descriptor is returned (empty tuple when absent).
        """
        if self._operator is None:
            return self._stored_pipeline_path
        if self._node_identity_path_cache is not None:
            return self._node_identity_path_cache
        # Hashes inside a path are bare hex digests: a label never enters a name.
        path = self.node_uri + (f"schema:{self.pipeline_hash().to_hex()}",)
        if self._table_scope != "pipeline_hash":
            path += (f"instance:{self.content_hash().to_hex()}",)
        self._node_identity_path_cache = path
        return path

    @property
    def node_uri(self) -> tuple[str, ...]:
        """Canonical URI tuple identifying this computation.

        Identical to ``operator.uri`` at runtime.
        Returns stored value in read-only (deserialized) mode.
        """
        if self._operator is None:
            return self._stored_node_uri
        return self._operator.uri

    # ------------------------------------------------------------------
    # Caching
    # ------------------------------------------------------------------

    def clear_cache(self) -> None:
        """Clear the node identity path cache."""
        self._node_identity_path_cache = None
        self._update_modified_time()

    def __repr__(self) -> str:
        # Upstreams by class and label only: their own repr would nest every path (ITL-1111).
        upstreams = ", ".join(short_stream_repr(s) for s in self._input_streams)
        return (
            f"{type(self).__name__}(label={self.label!r}, operator={self._operator!r}, "
            f"upstreams=({upstreams}))"
        )

    @abstractmethod
    def set_ephemeral_store(self, store: "ArrowDatabaseProtocol | None") -> None:
        """Assign or remove the ephemeral result store for this node.

        No-op for operator nodes — full ephemeral support for operators
        is deferred to ITL-509.

        Args:
            store: An ``ArrowDatabaseProtocol`` instance to attach, or ``None``
                to detach. Ignored by operator nodes in v1.
        """


# ---------------------------------------------------------------------------
# OperatorNode — thin blueprint (no DB)
# ---------------------------------------------------------------------------


class OperatorNode(OperatorNodeBase):
    """Thin blueprint descriptor for an operator pod invocation.

    Carries no database references.  Calling ``iter_data()`` raises
    ``PipelineJobRequiredError`` — wrap the containing ``Pipeline`` in a
    ``PipelineJob`` to obtain an executable ``OperatorJobNode``.

    This is the node type recorded inside a ``Pipeline`` context manager
    and serialized to disk via ``Pipeline.save()``.
    """

    def iter_data(self) -> Iterator[tuple[TagProtocol, DataProtocol]]:
        """Raise ``PipelineJobRequiredError`` — blueprint nodes cannot produce data.

        Raises:
            PipelineJobRequiredError: Always.
        """
        raise PipelineJobRequiredError(
            f"OperatorNode '{self.label}' is a blueprint — it carries no database "
            "references and cannot produce data directly.  "
            "Wrap the containing Pipeline in a PipelineJob to obtain an executable "
            "OperatorJobNode."
        )
        # Python classifies a function as a generator (returning an Iterator
        # lazily) based purely on the *syntactic* presence of a ``yield``
        # statement anywhere in the function body — this is determined at
        # compile time, not at runtime.  Without ``yield``, this would be a
        # plain function: calling it would execute the body immediately and
        # raise ``PipelineJobRequiredError`` before the caller ever gets an
        # iterator object.  With ``yield`` present (even though it is
        # unreachable due to the ``return`` above), Python compiles the
        # function as a generator function.  Calling it therefore returns a
        # generator object instantly without executing any body code; the
        # ``raise`` is deferred until the caller first calls ``next()`` on
        # the iterator (i.e., when iteration actually begins).  This matches
        # the expected ``Iterator`` return-type contract.
        return  # pragma: no cover
        yield  # pragma: no cover

    def head(
        self,
        n: int = 10,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> StreamView:
        """Return no rows: a blueprint node holds no data."""
        check_head_size(n)
        return self._head_without_rows(
            ColumnConfig.handle_config(columns, all_info=all_info),
            ViewStatus.UNAVAILABLE,
            note=BLUEPRINT_NOTE,
        )

    def as_table(
        self,
        *,
        columns: ColumnConfig | dict[Any, Any] | None = None,
        all_info: bool = False,
    ) -> pa.Table:
        """Raise ``PipelineJobRequiredError`` — blueprint nodes cannot produce data.

        Raises:
            PipelineJobRequiredError: Always.
        """
        raise PipelineJobRequiredError(
            f"OperatorNode '{self.label}' is a blueprint — it carries no database "
            "references and cannot produce data directly.  "
            "Wrap the containing Pipeline in a PipelineJob to obtain an executable "
            "OperatorJobNode."
        )

    @classmethod
    def from_descriptor(
        cls,
        descriptor: dict[str, Any],
        operator: OperatorPodProtocol | None,
        input_streams: tuple[StreamProtocol, ...] | list[StreamProtocol],
        databases: dict[str, Any],
    ) -> "OperatorNode":
        """Construct an OperatorNode from a serialized descriptor.

        When *operator* and *input_streams* are provided the node operates
        in full mode — constructed normally via ``__init__``.  When
        *operator* is ``None`` the node is created in read-only mode with
        metadata from the descriptor; computation methods will raise
        ``PipelineJobRequiredError``.

        Args:
            descriptor: The serialized node descriptor dict.
            operator: An optional live operator instance.  ``None`` for
                read-only mode.
            input_streams: Input streams for the operator.  Empty tuple
                for read-only mode.
            databases: Unused — kept for API parity with
                ``OperatorJobNode.from_descriptor``.

        Returns:
            A new ``OperatorNode`` instance.

        Raises:
            ValueError: If ``table_scope`` is missing or invalid.
        """
        from orcapod.pipeline.serialization import LoadStatus

        if "table_scope" not in descriptor:
            raise ValueError(
                f"OperatorNode descriptor is missing required 'table_scope' field: "
                f"{descriptor.get('label', '<unlabeled>')}"
            )
        raw_table_scope = descriptor["table_scope"]
        if raw_table_scope not in ("pipeline_hash", "content_hash"):
            raise ValueError(
                f"OperatorNode descriptor has invalid 'table_scope' value "
                f"{raw_table_scope!r} for {descriptor.get('label', '<unlabeled>')}; "
                "expected one of ('pipeline_hash', 'content_hash')"
            )
        table_scope: Literal["pipeline_hash", "content_hash"] = raw_table_scope

        if operator is not None and input_streams:
            # Full mode: construct normally.
            node = cls(
                operator=operator,
                input_streams=input_streams,
                label=descriptor.get("label"),
                table_scope=table_scope,
            )
            node._descriptor = descriptor
            # The run's pipeline hash, not one recomputed from the rebuilt inputs.
            node._stored_pipeline_hash = descriptor.get("pipeline_hash")
            node._load_status = LoadStatus.FULL
            return node

        # Read-only mode: bypass __init__ (which calls validate_inputs) and
        # manually set the minimum required state.
        node = cls.__new__(cls)

        # From LabelableMixin
        node._label = descriptor.get("label")

        # From DataContextMixin
        from orcapod.config import DEFAULT_CONFIG, OrcapodConfig

        node._data_context = contexts.resolve_saved_context(
            descriptor.get("data_context_key")
        )
        config_dict = descriptor.get("config")
        node._orcapod_config = (
            OrcapodConfig.from_dict(config_dict)
            if config_dict is not None
            else DEFAULT_CONFIG
        )

        # From ContentIdentifiableBase
        node._content_hash_cache = {}
        node._cached_int_hash = None

        # From PipelineElementBase
        node._pipeline_hash_cache = {}

        # From TemporalMixin
        node._modified_time = None

        # From OperatorNodeBase
        node._operator = None
        node._input_streams = ()
        node.tracker_manager = DEFAULT_TRACKER_MANAGER

        # Descriptor metadata for read-only access
        node._descriptor = descriptor
        node._stored_schema = descriptor.get("output_schema", {})
        node._stored_content_hash = descriptor.get("content_hash")
        node._stored_pipeline_hash = descriptor.get("pipeline_hash")
        node._stored_pipeline_path = tuple(descriptor.get("pipeline_path", ()))
        node._stored_node_uri = tuple(descriptor.get("node_uri") or [])
        node._table_scope = table_scope
        node._node_identity_path_cache = None

        # Blueprint nodes loaded read-only are always UNAVAILABLE (no DB)
        node._load_status = LoadStatus.UNAVAILABLE

        return node

    def set_ephemeral_store(self, store: "ArrowDatabaseProtocol | None") -> None:
        """No-op — blueprint operator nodes carry no database references."""

    def as_node(self) -> OperatorNode:
        """Return the lightweight blueprint equivalent of this node.

        Returns a fresh ``OperatorNode`` with the same operator, input streams,
        label, table scope, and tracker manager.  Its ``content_hash()`` /
        ``pipeline_hash()`` are identical to those of this node.

        Returns:
            A new equivalent ``OperatorNode``.

        Raises:
            RuntimeError: If this node is in UNAVAILABLE state (no live
                operator).  UNAVAILABLE nodes cannot be cloned into a usable
                blueprint — callers must handle this status before invoking
                ``as_node()``.
        """
        if self._operator is None:
            raise RuntimeError(
                f"Cannot clone OperatorNode {self._label!r} into a blueprint: "
                "the node is UNAVAILABLE (no live operator was provided when "
                "it was loaded). Only FULL or READ_ONLY nodes support as_node()."
            )
        return OperatorNode(
            operator=self._operator,
            input_streams=self._input_streams,
            label=self._label,
            table_scope=self._table_scope,
            tracker_manager=self.tracker_manager,
        )


# ---------------------------------------------------------------------------
# OperatorJobNode — DB-backed execution node
# ---------------------------------------------------------------------------


class OperatorJobNode(RunHoldingNode, OperatorNodeBase):
    """DB-backed execution node for operator pod invocations.

    Stream node representing an operator invocation with optional DB persistence.

    When constructed without database parameters, provides the core stream
    interface (identity, schema, iteration) without any persistence.  When
    databases are provided (either at construction or via ``attach_databases``),
    adds pipeline record storage with per-row deduplication, ``get_all_records()``
    for retrieving stored results, ``as_source()`` for creating a
    ``DerivedSource`` from DB records, and three-tier cache mode
    (OFF / LOG / REPLAY).

    Node identity path structure::

        operator.uri / schema:{pipeline_hash} / instance:{content_hash}

    Where ``pipeline_hash`` encodes the pipeline structure (operator +
    upstream topology) and ``instance:{content_hash}`` is the
    data-inclusive hash that encodes upstream source identities, ensuring
    each unique source combination gets its own cache table.

    Cache modes:
        - **OFF** (default): compute, don't write to DB.
        - **LOG**: compute AND write to DB (append-only historical record).
        - **REPLAY**: skip computation, flow cached results downstream.
    """

    # Set once a read has reported the stored rows it skipped (rows of another data
    # context); a class default so nodes rebuilt from a saved descriptor start unset.
    _skipped_rows_reported: bool = False
    # Set once a table found at the older layout has been accounted for.
    _stale_tables_checked: bool = False
    # The job file's expected row count for a loaded node, and whether it was checked
    # against the attached store (ITL-1043). Advisory: a mismatch only warns.
    _expected_row_count: int | None = None
    _row_count_checked: bool = False

    @property
    def _stored_table_path(self) -> tuple[str, ...]:
        """Path of this node's stored table: the identity path plus the layout version.

        Every read and write of the operator-cache table uses this path. The identity
        path itself is where Orcapod up to v0.2 stored the table (the old layout).
        """
        return self.node_identity_path + (OPERATOR_DB_SCHEMA_VERSION,)

    def _account_for_stale_tables(self) -> None:
        """Report, once per node object, a table found at the older layout.

        A table written by an earlier version of Orcapod holds no ``__data_context``
        column. Its rows are not read as results and are left unchanged; their number
        is reported (ITL-944).
        """
        if self._stale_tables_checked or self._pipeline_database is None:
            return
        self._stale_tables_checked = True
        account_for_stale_tables(
            find_stale_tables(self._pipeline_database, (self.node_identity_path,)),
            delivers_side_effects=False,
            current_context=self.data_context_key,
        )

    def __init__(
        self,
        operator: OperatorPodProtocol,
        input_streams: tuple[StreamProtocol, ...] | list[StreamProtocol],
        tracker_manager: TrackerManagerProtocol | None = None,
        label: str | None = None,
        config: OrcapodConfig | None = None,
        # Optional DB params for persistent mode:
        pipeline_database: ArrowDatabaseProtocol | None = None,
        cache_mode: CacheMode = CacheMode.OFF,
        table_scope: Literal["pipeline_hash", "content_hash"] = "pipeline_hash",
    ):
        super().__init__(
            operator=operator,
            input_streams=input_streams,
            tracker_manager=tracker_manager,
            label=label,
            config=config,
            table_scope=table_scope,
        )

        # Stream-level caching state
        self._cached_output_stream: StreamProtocol | None = None
        self._cached_output_table: pa.Table | None = None

        # DB persistence state (initially None; set via __init__ params or attach_databases)
        self._pipeline_database: ArrowDatabaseProtocol | None = None
        self._cache_mode = CacheMode.OFF

        # True for a node rebuilt by ``from_descriptor`` (a job loaded by
        # ``PipelineJob.load``): its output was computed by an earlier run.
        self._loaded = False

        if pipeline_database is not None:
            self.attach_databases(
                pipeline_database=pipeline_database,
                cache_mode=cache_mode,
            )
        # A node that has not run in this process holds nothing and is stale (ITL-1095).
        self._set_modified_time(None)

    # ------------------------------------------------------------------
    # as_node — return the lightweight OperatorNode equivalent
    # ------------------------------------------------------------------

    def as_node(self) -> OperatorNode:
        """Return the lightweight ``OperatorNode`` equivalent of this job node.

        Returns:
            A new ``OperatorNode`` with the same operator, input streams,
            label, table scope, and tracker manager.  Its ``content_hash()`` /
            ``pipeline_hash()`` are identical to those of this ``OperatorJobNode``.
        """
        return OperatorNode(
            operator=self._operator,
            input_streams=self._input_streams,
            label=self._label,
            table_scope=self._table_scope,
            tracker_manager=self.tracker_manager,
        )

    def set_ephemeral_store(self, store: "ArrowDatabaseProtocol | None") -> None:
        """No-op — full ephemeral support for operators is deferred to ITL-509."""

    # ------------------------------------------------------------------
    # Override clear_cache to also clear output stream caches
    # ------------------------------------------------------------------

    def clear_cache(self) -> None:
        """Clear output caches and node identity path cache.

        The node then holds no run: it is stale and reads empty, as before its first run
        in this process (ITL-1095). Rows it stored stay stored (``as_source()``).
        """
        super().clear_cache()  # clears _node_identity_path_cache + _update_modified_time
        self._cached_output_stream = None
        self._cached_output_table = None
        self._run_load_only = False
        # No content established: the node, and every node downstream, is stale.
        self._set_modified_time(None)

    # ------------------------------------------------------------------
    # attach_databases
    # ------------------------------------------------------------------

    def attach_databases(
        self,
        pipeline_database: ArrowDatabaseProtocol,
        cache_mode: CacheMode = CacheMode.OFF,
    ) -> None:
        """Attach a database for persistent caching and pipeline records.

        Args:
            pipeline_database: Database for pipeline records.
            cache_mode: Caching behaviour (OFF, LOG, or REPLAY).
        """
        # Another store has its own tables of other generations to account for.
        self._stale_tables_checked = False
        self._skipped_rows_reported = False
        self._row_count_checked = False
        self._pipeline_database = pipeline_database
        self._cache_mode = cache_mode

        # Clear caches
        self._node_identity_path_cache = None
        self.clear_cache()
        self._invalidate_content_hash_cache()
        self._invalidate_pipeline_hash_cache()

    # ------------------------------------------------------------------
    # from_descriptor — reconstruct from a serialized pipeline descriptor
    # ------------------------------------------------------------------

    @classmethod
    def from_descriptor(
        cls,
        descriptor: dict[str, Any],
        operator: OperatorPodProtocol | None,
        input_streams: tuple[StreamProtocol, ...] | list[StreamProtocol],
        databases: dict[str, Any],
        job_content_hash: str | None = None,
        expected_row_count: int | None = None,
    ) -> OperatorJobNode:
        """Construct an OperatorJobNode from a serialized descriptor.

        When *operator* and *input_streams* are provided the node operates
        in full mode — constructed normally via ``__init__``.  When
        *operator* is ``None`` the node is created in read-only mode with
        metadata from the descriptor; computation methods will raise
        ``RuntimeError``.

        Args:
            descriptor: The serialized node descriptor dict.
            operator: An optional live operator instance.  ``None`` for
                read-only mode.
            input_streams: Input streams for the operator.  Empty tuple
                for read-only mode.
            databases: Mapping of database role names (``"pipeline"``)
                to database instances.
            job_content_hash: The content hash of the node in the saved job's run,
                which its stored rows carry; ``None`` when the file does not record it.
            expected_row_count: How many rows the store held under the run's key when
                the job was saved, from the job file's optional ``run.expected_rows``;
                ``None`` when not recorded. Advisory: a read returns what the store
                holds and only warns (``StoreContentWarning``) when it differs.

        Returns:
            A new ``OperatorJobNode`` instance.
        """
        from orcapod.pipeline.serialization import LoadStatus

        if "table_scope" not in descriptor:
            raise ValueError(
                f"OperatorJobNode descriptor is missing required 'table_scope' field: "
                f"{descriptor.get('label', '<unlabeled>')}"
            )
        raw_table_scope = descriptor["table_scope"]
        if raw_table_scope not in ("pipeline_hash", "content_hash"):
            raise ValueError(
                f"OperatorJobNode descriptor has invalid 'table_scope' value "
                f"{raw_table_scope!r} for {descriptor.get('label', '<unlabeled>')}; "
                "expected one of ('pipeline_hash', 'content_hash')"
            )
        table_scope: Literal["pipeline_hash", "content_hash"] = raw_table_scope

        pipeline_db = databases.get("pipeline")
        cache_mode_str = descriptor.get("cache_mode", "off")
        try:
            cache_mode = CacheMode(cache_mode_str)
        except ValueError:
            cache_mode = CacheMode.OFF

        if operator is not None and input_streams:
            # Full mode: construct normally.
            node = cls(
                operator=operator,
                input_streams=input_streams,
                label=descriptor.get("label"),
                table_scope=table_scope,
            )
            if pipeline_db is not None:
                node.attach_databases(
                    pipeline_database=pipeline_db,
                    cache_mode=cache_mode,
                )
            node._descriptor = descriptor
            # The run's pipeline hash, not one recomputed from the rebuilt inputs: it
            # names the table the run's rows are stored in.
            node._stored_pipeline_hash = descriptor.get("pipeline_hash")
            node._saved_run_content_hash = job_content_hash
            node._expected_row_count = expected_row_count
            node._load_status = LoadStatus.FULL
            node._loaded = True
            return node

        # Read-only mode: bypass __init__, set minimum required state
        node = cls.__new__(cls)

        # From LabelableMixin
        node._label = descriptor.get("label")

        # From DataContextMixin
        from orcapod.config import DEFAULT_CONFIG, OrcapodConfig

        node._data_context = contexts.resolve_saved_context(
            descriptor.get("data_context_key")
        )
        config_dict = descriptor.get("config")
        node._orcapod_config = (
            OrcapodConfig.from_dict(config_dict)
            if config_dict is not None
            else DEFAULT_CONFIG
        )

        # From ContentIdentifiableBase
        node._content_hash_cache = {}
        node._cached_int_hash = None

        # From PipelineElementBase
        node._pipeline_hash_cache = {}

        # From TemporalMixin
        node._modified_time = None

        # From OperatorNodeBase
        node._operator = None
        node._input_streams = tuple(input_streams)
        node.tracker_manager = DEFAULT_TRACKER_MANAGER

        # From OperatorJobNode
        node._cached_output_stream = None
        node._cached_output_table = None
        node._loaded = True

        # DB persistence state
        node._pipeline_database = pipeline_db
        node._cache_mode = cache_mode

        # Descriptor metadata for read-only access
        node._descriptor = descriptor
        node._stored_schema = descriptor.get("output_schema", {})
        node._stored_content_hash = descriptor.get("content_hash")
        node._saved_run_content_hash = job_content_hash
        node._expected_row_count = expected_row_count
        node._stored_pipeline_hash = descriptor.get("pipeline_hash")
        node._stored_pipeline_path = tuple(descriptor.get("pipeline_path", ()))
        node._stored_node_uri = tuple(descriptor.get("node_uri") or [])
        node._table_scope = table_scope
        node._node_identity_path_cache = None

        # Determine load status based on DB availability and cache mode.
        # An uncached operator (cache_mode=OFF) never writes records to the
        # database, so even when a pipeline_db exists there is nothing to
        # read back.  Only operators that actively persist results
        # (LOG or REPLAY mode) can legitimately serve data in read-only mode.
        node._load_status = LoadStatus.UNAVAILABLE
        if pipeline_db is not None and cache_mode != CacheMode.OFF:
            node._load_status = LoadStatus.READ_ONLY

        return node

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _run_content_hash(self) -> ContentHash | None:
        """Return the content hash this node's run stores its rows under, if known.

        A node that runs computes it. A loaded node knows it only when its saved job
        recorded it (ITL-1043); otherwise ``None``, because the hash it recomputes from
        inputs rebuilt from the file is not the run's.
        """
        if not self._loaded:
            return self.content_hash()
        if self._saved_run_content_hash is None:
            return None
        return ContentHash.from_string(self._saved_run_content_hash)

    def _expected_row_count_to_save(self, run_finished: bool) -> int | None:
        """Return the count ``PipelineJob.save`` records for this node, if certain.

        A loaded node keeps the count its file recorded, or none. A node of a job whose
        last run finished (its writes committed) records the rows the store holds under
        its run's content hash; otherwise nothing is certain.

        Args:
            run_finished: Whether the job's last run finished and committed.
        """
        if self._loaded:
            return self._expected_row_count
        if not run_finished or self._pipeline_database is None:
            return None
        records = self._read_stored_records()
        return 0 if records is None else records.num_rows

    def _check_expected_rows(self, found: int) -> None:
        """Warn, once per store, if a loaded node finds another number of rows than recorded.

        The store is the source of truth: the read returns what it holds (ITL-1043).

        Args:
            found: The rows the store holds under the node's run key.
        """
        if self._expected_row_count is None or self._row_count_checked:
            return
        self._row_count_checked = True
        if found != self._expected_row_count and self._pipeline_database is not None:
            report_unexpected_row_count(
                f"OperatorJobNode {self.label!r}",
                describe_store_table(self._pipeline_database, self._stored_table_path),
                self._expected_row_count,
                found,
            )

    def _no_stored_results_error(self) -> RuntimeError:
        """Return the error for reading a loaded operator node with nothing to serve.

        Operator outputs are stored only when caching is enabled. A loaded node
        needs a readable cache; returning an empty table when none is available
        would look like the operator produced nothing.
        """
        inputs = ", ".join(repr(stream.label) for stream in self._input_streams)
        return RuntimeError(
            f"OperatorJobNode {self.label!r} has no stored results available for this read. "
            "Operator outputs require a stored cache and replay configuration. Read the "
            f"stored results of its input nodes ({inputs}) instead; an input source "
            "whose data was not saved must be bound first with job.bind(sources=...)."
        )

    def _cannot_compute_error(self) -> RuntimeError:
        """Describe why this loaded node cannot execute an operator."""
        return RuntimeError(
            f"OperatorJobNode {self.label!r} was loaded by PipelineJob.load and "
            "cannot compute; fill it from the store with run(load_only=True), read "
            "its stored results with as_source(), or create a fresh PipelineJob to "
            "run the pipeline again."
        )

    def _filter_by_content_hash(self, table: pa.Table) -> pa.Table:
        """Filter *table* to rows whose ``NODE_CONTENT_HASH_COL`` matches this node.

        Only applied when ``table_scope="pipeline_hash"`` because in that mode
        multiple runs share the same DB table and must be disambiguated at read
        time.  In ``"content_hash"`` mode every run has its own table.
        """
        if self._table_scope != "pipeline_hash":
            return table
        col_name = constants.NODE_CONTENT_HASH_COL
        if col_name not in table.column_names:
            raise ValueError(
                f"Cannot isolate records for table_scope='pipeline_hash': "
                f"required column {col_name!r} is missing from the stored table. "
                "This may indicate records written by an older version of the code."
            )
        col_type = table.schema.field(col_name).type
        if col_type != pa.large_binary():
            raise ValueError(
                f"Cannot isolate records for table_scope='pipeline_hash': "
                f"column {col_name!r} has type {col_type!r}, expected large_binary. "
                "This table was written by an older version of the code that stored "
                "this column as large_string. Drop and recreate the affected pipeline "
                "DB tables (see design_issues/ ON1)."
            )
        own_hash = self.content_hash().to_prefixed_digest()
        mask = pc.equal(table.column(col_name), own_hash)
        return table.filter(mask)

    # ------------------------------------------------------------------
    # Computation and caching
    # ------------------------------------------------------------------

    def _store_output_stream(self, stream: StreamProtocol) -> None:
        """Materialize stream and store in the pipeline database with per-row dedup."""
        pipeline_database = self._pipeline_database
        if pipeline_database is None:
            raise RuntimeError(
                f"OperatorJobNode {self.label!r} has no pipeline database attached."
            )
        output_table = stream.as_table(
            columns={"source": True, "system_tags": True, "context": True},
        )
        # The inputs and the rows name their data context; all must be this node's.
        for input_stream in self._input_streams:
            require_same_data_context(
                input_stream.data_context_key, self.data_context_key
            )
        row_context_keys = cast(
            "list[str]",
            pc.unique(output_table.column(constants.CONTEXT_KEY)).to_pylist(),
        )
        for row_context_key in row_context_keys:
            require_same_data_context(row_context_key, self.data_context_key)
        output_table = output_table.drop([constants.CONTEXT_KEY])

        # Always append the node content hash column first so that it is included
        # in the per-row HASH_COLUMN_NAME computation below.  This makes record IDs
        # run-scoped: identical rows from different runs carry a different
        # NODE_CONTENT_HASH_COL value and therefore hash to different record IDs,
        # preventing skip_duplicates=True from silently dropping a later run's rows.
        # When table_scope="pipeline_hash" this column is also used at read time by
        # _filter_by_content_hash to isolate rows belonging to the current run.
        # In content_hash scope each run has its own isolated table so the column
        # is present in storage but filtering is never applied on reads.
        n_rows = output_table.num_rows
        node_content_hash = self.content_hash()
        # The row that is hashed carries the node content hash as its raw digest:
        # a hash label is never part of a hash input. The stored column is labelled.
        hashed_table = output_table.append_column(
            constants.NODE_CONTENT_HASH_COL,
            pa.array([node_content_hash.digest] * n_rows, type=pa.large_binary()),
        )
        output_table = output_table.append_column(
            constants.NODE_CONTENT_HASH_COL,
            pa.array(
                [node_content_hash.to_prefixed_digest()] * n_rows,
                type=pa.large_binary(),
            ),
        )

        # Per-row record hashes for dedup: hash(tag + data + system_tags + node_content_hash).
        arrow_hasher = self.data_context.arrow_hasher
        record_hashes = []
        for batch in hashed_table.to_batches():
            for i in range(len(batch)):
                # A hash stored as a value carries its label: b"<label>:<digest>".
                record_hashes.append(
                    arrow_hasher.hash_table(batch.slice(i, 1)).to_prefixed_digest()
                )

        output_table = output_table.add_column(
            0,
            self.HASH_COLUMN_NAME,
            pa.array(record_hashes, type=pa.large_binary()),
        )

        # Store — record IDs are run-scoped (NODE_CONTENT_HASH_COL is included in
        # the hash), so rows from different runs with identical output will have
        # distinct record IDs and both be stored.  skip_duplicates=True still
        # deduplicates exact re-runs of the same node within a single run.
        # The data context column is appended after the record hashes are computed:
        # it names the generation of hashing that produced them and is not hashed.
        self._account_for_stale_tables()
        try:
            pipeline_database.add_records(
                self._stored_table_path,
                with_data_context(output_table, self.data_context_key),
                record_id_column=self.HASH_COLUMN_NAME,
                skip_duplicates=True,
            )
        except StoreReadError:
            # The stored table could not be read: report the read failure itself.
            raise
        except Exception as exc:
            raise StoreWriteError(
                self._stored_table_path, node_label=self.label
            ) from exc

        self._cached_output_table = output_table.drop(
            [self.HASH_COLUMN_NAME, constants.NODE_CONTENT_HASH_COL]
        )

    def _read_stored_records(self) -> "pa.Table | None":
        """Read this node's stored rows written under the current data context.

        Every read of the whole stored table goes through here. Rows are narrowed to
        the current data context (``__data_context``) and then to this node's content
        hash (``_filter_by_content_hash``). Rows of another data context stay in the
        table untouched, and a table at the older layout is not read (ITL-944).

        Returns:
            The stored rows, with ``HASH_COLUMN_NAME`` and ``NODE_CONTENT_HASH_COL``
            still present and without the ``__data_context`` column, or ``None``
            when the table does not exist.
        """
        records = self._read_current_context_records()
        if records is None:
            return None
        return self._filter_by_content_hash(records)

    def _read_current_context_records(self) -> "pa.Table | None":
        """Read every row of this node's stored table written under the current data context.

        The rows of every run that shares the table, before ``_filter_by_content_hash``
        keeps this node's. Rows of another data context are left out and reported once.

        Returns:
            The rows, with ``HASH_COLUMN_NAME`` and ``NODE_CONTENT_HASH_COL`` and without
            the ``__data_context`` column, or ``None`` when the table does not exist.
        """
        assert self._pipeline_database is not None
        self._account_for_stale_tables()
        records = self._pipeline_database.get_all_records(
            self._stored_table_path, record_id_column=self.HASH_COLUMN_NAME
        )
        if records is None:
            return None
        stored_rows = records.num_rows
        records = keep_rows_of_data_context(records, self.data_context_key)
        if records.num_rows < stored_rows and not self._skipped_rows_reported:
            self._skipped_rows_reported = True
            report_rows_of_other_data_contexts(
                self._pipeline_database.base_path + self._stored_table_path,
                stored_rows - records.num_rows,
            )
        return records.drop([constants.DATA_CONTEXT_COL])

    def _typed_zero_row_table(self) -> "pa.Table | None":
        """This node's zero-row table: its operator applied to zero-row inputs.

        The sync path's rule (ITL-912): each input is the upstream's zero-row table
        (``StaticOutputOperatorPod._zero_row_input``), and the operator's result over
        those is this node's table with no rows, with the Arrow types, nullability,
        system-tag and source-info columns its rows have. No row is computed or read,
        no function is called and nothing is stored, and a result the node holds is
        not consulted: the types follow from the inputs' types alone.

        Returns:
            The zero-row table with system tags and source info, or ``None`` when the
            node has no operator or an input cannot say its Arrow types without rows
            (see ``typed_zero_row_table``).
        """
        if self._operator is None:
            return None
        inputs: list[StreamProtocol] = []
        for upstream in self._input_streams:
            zero_rows = StaticOutputOperatorPod._zero_row_input(upstream)
            if zero_rows is None:
                return None
            inputs.append(zero_rows)
        # zero_row_result calls static_process, not process: an invocation recorded by
        # a tracker would add a step to whatever pipeline is being built while this
        # table is read.
        operator = cast(StaticOutputOperatorPod, self._operator)
        result = operator.zero_row_result(*inputs)
        table = result.as_table(columns=ZERO_ROW_INPUT_COLUMNS)
        # The column order of the node's rows (``_rows_to_table``): user tags, system
        # tags, data, source info; an operator's own table may order them otherwise
        # (a join lists its data before its system tags).
        tag_keys, data_keys = result.keys()
        names = table.column_names
        system_tags = [n for n in names if n.startswith(constants.SYSTEM_TAG_PREFIX)]
        source_info = [n for n in names if n.startswith(constants.SOURCE_PREFIX)]
        data = [n for n in data_keys if n in names]
        return table.select([*tag_keys, *system_tags, *data, *source_info])

    def _make_empty_table(self) -> "pa.Table":
        """Build the zero-row table this node reads as when it holds no rows.

        The node's operator applied to its inputs' zero-row tables
        (``_typed_zero_row_table``), so the columns, Arrow types and nullability are
        those of the node's rows. When an input cannot say its Arrow types without
        rows (a known limitation, see ``as_table``), the table is built from the
        node's Python output schema with ``arrow_utils.make_empty_table``: plain
        types give ``nullable=False`` fields and ``T | None`` types
        ``nullable=True`` fields.

        Requires ``self._operator is not None`` (pre-existing limitation shared
        with ``_replay_from_cache``).
        """
        typed = self._typed_zero_row_table()
        if typed is not None:
            return typed
        tag_schema, data_schema = self.output_schema()
        return arrow_utils.make_empty_table(
            {**tag_schema, **data_schema},
            self.data_context.type_converter,
        )

    def _read_run_records(self) -> "pa.Table | None":
        """Read the stored rows of this node's run: its content hash, current context.

        A loaded node names its run by the content hash its job file records. A file
        saved before Orcapod recorded it leaves the node only the hash the loaded job
        computes; when the store holds no rows under that hash, an empty answer would
        make stored results look dropped, so this raises instead (ITL-1043).

        Returns:
            The run's stored rows (``_read_stored_records``), or ``None`` when the table
            does not exist.

        Raises:
            RuntimeError: If a loaded node's job file does not record its run's
                content hash and the store holds no rows under the hash the loaded
                job computes.
        """
        stored = self._read_current_context_records()
        records = None if stored is None else self._filter_by_content_hash(stored)
        if (
            self._loaded
            and self._saved_run_content_hash is None
            and (records is None or records.num_rows == 0)
        ):
            found = (
                "is absent from the attached store"
                if stored is None
                else f"holds {stored.num_rows} stored rows, none of them under the "
                f"content hash the loaded job computes ({self.content_hash().to_string()})"
            )
            raise _LoadedCacheUnreadableError(
                f"OperatorJobNode {self.label!r}: the job file does not record the "
                "content hash of this node's run (a file saved before Orcapod recorded "
                "it), so the loaded job cannot name the run's rows, and its cache table "
                f"({'/'.join(self._stored_table_path)}) {found}. Upgrading such files is "
                "left to the migration tool after v0.3 (ITL-1048); until then build the "
                "job from code and attach the node with CacheMode.REPLAY to read them.",
                head_note=(
                    "loaded node: the job file does not record its run's content hash "
                    "(an older file; ITL-1048)"
                ),
            )
        return records

    def _load_cached_stream_from_db(self) -> "ArrowTableStream | None":
        """Read from DB in CacheMode.REPLAY only, without modifying node state.

        Returns an ``ArrowTableStream`` (possibly wrapping zero rows) when
        ``CacheMode.REPLAY`` is active and a database is attached; ``None``
        otherwise.

        This method is intentionally **state-free**: it never assigns to
        ``_cached_output_stream`` and never calls ``_update_modified_time()``.
        Repeated calls re-query the DB each time — in-memory caching is the
        responsibility of the computation paths (``run()``, ``execute()``).

        Guards:
            - Returns ``None`` if ``_pipeline_database is None``.
            - Returns ``None`` if ``_cache_mode != CacheMode.REPLAY``
              (LOG/OFF modes may have stale historical records in the DB).

        The store is the source of truth: a loaded node whose job file records its
        run's content hash reads exactly as a node that runs does, so an absent table or
        a table with no rows under that hash is an empty result.

        Raises:
            RuntimeError: If a loaded REPLAY node's job file does not record its run's
                content hash and the store holds no rows under the hash the loaded job
                computes: the node cannot name its run's rows, so an empty result would
                make stored results look dropped.
        """
        if self._pipeline_database is None:
            return None
        if self._cache_mode != CacheMode.REPLAY:
            return None
        records = self._read_run_records()
        if self._loaded:
            self._check_expected_rows(0 if records is None else records.num_rows)
        if records is None:
            if self._operator is None and not self._loaded:
                # Read-only (deserialized) node with no operator: cannot derive
                # schema without a live operator. Return None so callers fall
                # back to iter([]) / _make_empty_table() guarded paths.
                return None
            records_table = self._make_empty_table()
        else:
            records_table = records
            # Drop internal columns that must not surface to stream consumers.
            cols_to_drop = [
                c
                for c in [constants.NODE_CONTENT_HASH_COL, self.HASH_COLUMN_NAME]
                if c in records_table.column_names
            ]
            if cols_to_drop:
                records_table = records_table.drop(cols_to_drop)
        tag_keys = self.keys()[0]
        # Stored rows are this node's: give them its data context, not the default.
        return ArrowTableStream(
            records_table, tag_columns=tag_keys, data_context=self.data_context
        )

    def get_cached_output(self) -> StreamProtocol | None:
        """Return cached output stream in REPLAY mode, else None.

        Returns:
            The cached stream if REPLAY mode and DB records exist,
            otherwise None.
        """
        if self._pipeline_database is None:
            return None
        if self._cache_mode != CacheMode.REPLAY:
            return None
        self._replay_from_cache()
        return self._cached_output_stream

    def execute(
        self,
        *input_streams: StreamProtocol,
        observer: ExecutionObserverProtocol | None = None,
        load_only: bool = False,
    ) -> list[tuple[TagProtocol, DataProtocol]]:
        """Execute input streams: compute, persist, and cache.

        Args:
            *input_streams: Input streams to execute.
            observer: Optional execution observer for hooks.
            load_only: A load-only run (ITL-1095): the operator still runs on the rows
                that reach it (it is not a stored computation), but nothing is
                written: a ``LOG`` node does not store its output. ``REPLAY`` replays
                as in any run.

        Returns:
            Materialized list of (tag, data) pairs.
        """
        from orcapod.pipeline.observer import NoOpObserver

        # A loaded node runs only in a load-only run, with its rebuilt operator.
        if self._loaded and (not load_only or self._operator is None):
            raise self._cannot_compute_error()

        node_label = self.label
        node_hash = self.content_hash().to_string()

        obs = observer if observer is not None else NoOpObserver()
        ctx_obs = obs.contextualize(*self.node_identity_path)

        try:
            ctx_obs.on_node_start(node_label, node_hash)

            self._run_load_only = load_only
            # Check REPLAY cache first
            cached_output = self.get_cached_output()
            if cached_output is not None:
                output = list(cached_output.iter_data())
                ctx_obs.on_node_end(node_label, node_hash)
                return output

            # Compute
            result_stream = self._operator.process(*input_streams)

            # Materialize
            output = list(result_stream.iter_data())

            # Cache
            if output:
                self._cached_output_stream = (
                    StaticOutputOperatorPod._materialize_to_stream(output)
                )
            else:
                self._cached_output_stream = result_stream

            self._update_modified_time()

            # Persist to DB only in LOG mode, and never in a load-only run.
            if (
                not load_only
                and self._pipeline_database is not None
                and self._cache_mode == CacheMode.LOG
                and self._cached_output_stream is not None
            ):
                self._store_output_stream(self._cached_output_stream)

            ctx_obs.on_node_end(node_label, node_hash)
            return output
        except BaseException:
            # A run that raised (its store write or an observer included) defines no rows.
            self.clear_cache()
            raise

    def _compute_and_store(self, *, store: bool = True) -> None:
        """Compute operator output, optionally store in DB.

        Args:
            store: Whether a ``LOG`` node stores the output (not in a load-only run).
        """
        self._cached_output_stream = self._operator.process(
            *self._input_streams,
        )

        if self._cache_mode == CacheMode.OFF or not store:
            self._update_modified_time()
            return

        self._store_output_stream(self._cached_output_stream)
        self._update_modified_time()

    def _replay_from_cache(self) -> None:
        """Load cached results from DB, skip computation.

        If no cached records exist yet, produces an empty stream with
        the correct schema (zero rows, correct columns). A loaded node reads through
        ``_load_cached_stream_from_db``, which raises when its run's rows cannot be
        read, so an unreadable cache is never kept as an empty stream.
        """
        if self._loaded:
            stream = self._load_cached_stream_from_db()
            if stream is None:
                raise self._no_stored_results_error()
            self._cached_output_stream = stream
            self._update_modified_time()
            return
        records = self._read_stored_records()
        if records is None:
            records = self._make_empty_table()
        else:
            # Drop internal columns that must not surface to stream consumers.
            cols_to_drop = [
                c
                for c in [constants.NODE_CONTENT_HASH_COL, self.HASH_COLUMN_NAME]
                if c in records.column_names
            ]
            if cols_to_drop:
                records = records.drop(cols_to_drop)

        tag_keys = self.keys()[0]
        self._cached_output_stream = ArrowTableStream(
            records, tag_columns=tag_keys, data_context=self.data_context
        )
        self._update_modified_time()

    def run(self, *, load_only: bool = False) -> StreamView:
        """Run the node as a job run would, and return a ``StreamView`` of its rows.

        State-driven (developer, November 13, 2026; ITL-1095): a node that holds the
        output of a completed run and is not stale is not run again; the view shows it.
        Otherwise the node runs, after every job node it depends on that holds no
        current run (each once, in dependency order, as ``job.run()`` would run them;
        ``last_run`` reports the counts), and the run's writes are committed. A node
        built over streams outside a job runs its operator on them directly.

        Without a database:
            Always compute via the operator's ``process()`` method.

        With a database:
            - **OFF**: always compute, no DB writes.
            - **LOG**: always compute, write results to DB.
            - **REPLAY**: skip computation, load from DB.

        Args:
            load_only: As ``job.run(load_only=True)``: the operator runs on the rows
                that reach it, a ``LOG`` node writes nothing, and the nodes run first
                load their stored results only.

        Returns:
            A ``StreamView`` of every row ``as_table()`` then returns, naming its rows
            as ``head()`` then names them.

        Raises:
            RuntimeError: If the node was loaded by ``PipelineJob.load`` and
                ``load_only`` is false.
        """
        from orcapod.pipeline.node_run import (
            depends_on_job_nodes,
            run_with_dependencies,
        )
        from orcapod.pipeline.run_summary import warn_row_failures

        if self._loaded and not load_only:
            raise self._cannot_compute_error()
        if self.holds_current_run(load_only=load_only):
            return self._run_view(computed=False)
        if depends_on_job_nodes(self):
            summary = run_with_dependencies(self, load_only=load_only)
            self._last_run = summary
            if not summary.ok:
                warn_row_failures(summary, sys._getframe(1))  # pyright: ignore[reportPrivateUsage]  # the caller's line is where the warning is shown
            return self._run_view(computed=False)
        if self._loaded:
            raise self._cannot_compute_error()
        if self.is_stale or not self._holds_run(load_only=load_only):
            # Stale, or a load-only run's output asked for by a full run: run again.
            self.clear_cache()

        computed = False
        if self._cached_output_stream is None:
            try:
                if self._pipeline_database is not None:
                    if self._cache_mode == CacheMode.REPLAY:
                        self._replay_from_cache()
                    else:
                        self._compute_and_store(store=not load_only)
                        computed = True
                        if self._cache_mode == CacheMode.LOG and not load_only:
                            # Committed before run() returns, as at the end of a job
                            # run.
                            from orcapod.pipeline.node_run import commit_run

                            commit_run([self._pipeline_database], [self])
                    self._run_load_only = load_only
                else:
                    self._cached_output_stream = self._operator.process(
                        *self._input_streams,
                    )
                    self._update_modified_time()
                    computed = True
            except BaseException:
                # A run that raised defines no rows.
                self.clear_cache()
                raise
        return self._run_view(computed=computed)

    def _run_view(self, *, computed: bool) -> StreamView:
        """The ``run()`` view of the node's output.

        Args:
            computed: Whether this ``run()`` computed the output lazily just now.
        """
        # Read the kept output itself: a lazy input that finishes computing during this
        # read stamps itself after the node, which would make as_table() judge it stale.
        assert self._cached_output_stream is not None
        table = self._cached_output_stream.as_table()
        if computed:
            # Without a database and in OFF mode the output stream is lazy: as_table()
            # computes it, and an uncomputed input finishes only then. Stamp the node
            # after that, or it would be older than its input and judged stale. A run
            # that reused the kept result computed nothing and is not stamped.
            self._update_modified_time()
        # The view names its rows as head() names them afterwards: computed by a live
        # computation, plain rows when replayed from the store or kept by execute().
        kept = self._cached_output_stream
        origin = "computed" if kept is None else head_of(kept.head(1), self).origin
        view = self._view_of_rows(table, origin)
        if self._run_load_only:
            return dataclasses.replace(
                view, status=ViewStatus.LOADED_ONLY, note=LOADED_ONLY_OPERATOR_NOTE
            )
        return view

    # Whether the output the node holds came from a load-only run (ITL-1095): its
    # input may lack rows, so a full run does not take it as complete.
    _run_load_only: bool = False

    def _holds_run(self, *, load_only: bool) -> bool:
        """Whether the node holds a run's output; a load-only run's only for ``load_only``."""
        return self._cached_output_stream is not None and (
            load_only or not self._run_load_only
        )

    def _current_output(self) -> StreamProtocol | None:
        """The output of the node's current run, or ``None`` when it holds none.

        A node holds a run's output from the end of ``run()``, ``execute()`` or
        ``async_execute()`` until it is cleared or becomes stale (an input or the
        operator changed since). Before a run in this process it holds none, whatever
        its cache mode and store hold (ITL-1095).
        """
        if self._cached_output_stream is None or self.is_stale:
            return None
        return self._cached_output_stream

    def iter_data(self) -> Iterator[tuple[TagProtocol, DataProtocol]]:
        """Return an iterator over the (tag, data) rows of the node's current run.

        Read-only: never computes and never reads the store. Yields the rows of the
        latest ``run()``, ``execute()`` or ``async_execute()``; nothing before a run in
        this process, after ``clear_cache()`` or once the node is stale, in every cache
        mode (ITL-1095). ``as_source()`` reads what the node stored (``LOG``).
        """
        current = self._current_output()
        if current is None:
            return iter([])
        return current.iter_data()

    def as_table(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> "pa.Table":
        """Return the output of the node's current run as a PyArrow table.

        Read-only: never computes and never reads the store. Returns the rows of the
        latest ``run()``, ``execute()`` or ``async_execute()``; before a run in this
        process, after ``clear_cache()`` or once the node is stale, a zero-row table
        with the node's columns, in every cache mode (ITL-1095).

        That zero-row table has the columns, Arrow types and nullability of the
        node's rows, system tags and source info included: it is the operator's
        result over its inputs' zero-row tables, got without computing or reading
        a row (ITL-912). Known limitation: when an input cannot say its Arrow
        types without rows (a ``PollingSource`` before its first poll, an unbound
        input slot or ``SourceProxy``, a ``DerivedSource``, or a loaded node with
        no live upstream), the table is built from the node's Python output
        schema: an ``int8`` column reads as ``int64`` and the system-tag columns
        are absent (ITL-1089, ITL-985).
        """
        current = self._current_output()
        if current is not None:
            return current.as_table(columns=columns, all_info=all_info)
        if self._operator is None:
            # A node rebuilt without its operator: the columns of its saved schema.
            tag_schema, data_schema = self.output_schema()
            empty = pa.Table.from_pylist(
                [],
                schema=self.data_context.type_converter.python_schema_to_arrow_schema(
                    tag_schema + data_schema
                ),
            )
            return ArrowTableStream(
                empty, tag_columns=tuple(tag_schema), data_context=self.data_context
            ).as_table(columns=columns, all_info=all_info)
        empty_stream = ArrowTableStream(
            self._make_empty_table(),
            tag_columns=self.keys()[0],
            data_context=self.data_context,
        )
        return empty_stream.as_table(columns=columns, all_info=all_info)

    def head(
        self,
        n: int = 10,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> StreamView:
        """Return the first rows of the node's current run; never runs the operator.

        After a run (or ``execute``) the rows are the node's output in memory. Before a
        run in this process the node shows no rows and says so, pointing to ``run()``
        and to ``as_source()``, in every cache mode; once stale it says its inputs
        changed (ITL-1095). The store is never read. See ``StreamBase.head``.
        """
        check_head_size(n)
        config = ColumnConfig.handle_config(columns, all_info=all_info)
        if self._cached_output_stream is None:
            return self._head_without_rows(
                config, ViewStatus.NOT_COMPUTED, note=JOB_NODE_NOT_RUN_NOTE
            )
        if self.is_stale:
            # run() would recompute; the kept rows are not the current result.
            return self._head_without_rows(
                config, ViewStatus.NOT_COMPUTED, note=STALE_NOTE
            )
        head = head_of(self._cached_output_stream.head(n, columns=config), self)
        if self._run_load_only:
            # Output of a load-only run: its input may lack rows (ITL-1095).
            return dataclasses.replace(
                head, status=ViewStatus.LOADED_ONLY, note=LOADED_ONLY_OPERATOR_NOTE
            )
        return head

    # ------------------------------------------------------------------
    # DB retrieval
    # ------------------------------------------------------------------

    def get_all_records(
        self,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> "pa.Table | None":
        """Retrieve all stored records from the pipeline database.

        Returns the stored output table with column filtering applied
        per ``ColumnConfig`` conventions.  Returns ``None`` when no
        database is attached or no matching records exist. Switching to OFF
        does not remove records stored by an earlier LOG run.
        """
        if self._pipeline_database is None:
            return None

        # A loaded node that cannot name its run's rows raises rather than show the
        # stored rows as none (ITL-1043): this is the read behind node.as_source().
        results = self._read_run_records()
        if self._loaded:
            # The advisory check of every read of a loaded node (ITL-1043).
            self._check_expected_rows(0 if results is None else results.num_rows)
        if results is None:
            return None

        column_config = ColumnConfig.handle_config(columns, all_info=all_info)

        # Always drop internal row-level columns.
        drop_columns = [
            c
            for c in [constants.NODE_CONTENT_HASH_COL, self.HASH_COLUMN_NAME]
            if c in results.column_names
        ]
        if not column_config.meta and not column_config.all_info:
            drop_columns.extend(
                c for c in results.column_names if c.startswith(constants.META_PREFIX)
            )
        if not column_config.source and not column_config.all_info:
            drop_columns.extend(
                c for c in results.column_names if c.startswith(constants.SOURCE_PREFIX)
            )
        if not column_config.system_tags and not column_config.all_info:
            drop_columns.extend(
                c
                for c in results.column_names
                if c.startswith(constants.SYSTEM_TAG_PREFIX)
            )
        if drop_columns:
            # Deduplicate: NODE_CONTENT_HASH_COL now starts with META_PREFIX so it
            # can appear in both the explicit list and the meta-prefix sweep above.
            unique_drop = list(
                dict.fromkeys(c for c in drop_columns if c in results.column_names)
            )
            if unique_drop:
                results = results.drop(unique_drop)

        return results if results.num_rows > 0 else None

    # ------------------------------------------------------------------
    # DerivedSource
    # ------------------------------------------------------------------

    def as_source(self, tags_as_data: "Mapping[str, str] | None" = None):
        """Return a DerivedSource backed by the DB records of this node.

        Its ``source_id`` is the ``DerivedSource`` default: without
        ``tags_as_data``, ``derived:<algorithm label>:<hex>`` of this node's
        content hash.

        Args:
            tags_as_data: Maps a tag column of this node to the name of a data
                column that receives a copy of it. When given, the result is a
                new root source with its own provenance and a ``source_id``
                derived from this node's content hash and the mapping; see
                ``DerivedSource``.

        Raises:
            RuntimeError: If no database is attached.
            InputValidationError: If ``tags_as_data`` is not valid for this
                node's columns.
        """
        if self._pipeline_database is None:
            raise RuntimeError("Cannot create a DerivedSource without a database")

        from orcapod.core.sources.derived_source import DerivedSource

        # No source_id is given: the source takes the DerivedSource default
        # (``derived:<algorithm label>:<hex>`` of this node's content hash, or the
        # ``tags_as_data`` id when a mapping is given).
        return DerivedSource(
            origin=self,
            tags_as_data=tags_as_data,
            data_context=self.data_context_key,
            config=self.orcapod_config,
        )

    # ------------------------------------------------------------------
    # Async channel execution
    # ------------------------------------------------------------------

    async def async_execute(
        self,
        inputs: Sequence[ReadableChannel[tuple[TagProtocol, DataProtocol]]],
        output: WritableChannel[tuple[TagProtocol, DataProtocol]],
        *,
        observer: "ExecutionObserverProtocol | None" = None,
        load_only: bool = False,
    ) -> None:
        """Async execution with cache mode handling when DB is attached.

        Every mode except REPLAY runs the wrapped operator's ``async_execute``,
        forwards its rows downstream and keeps them on the node, exactly as
        ``execute()`` does, so ``iter_data()`` and ``as_table()`` return them
        after the run whichever orchestrator ran the node.

        Without a database, or with one in OFF mode, nothing is stored.

        With a database:
            - REPLAY: emit from DB while reading and discarding every input, so
              the upstream nodes run as with the sync orchestrator; close output.
            - OFF: delegate to operator, forward and keep results.
            - LOG: delegate to operator, forward and keep results, then store
              them in the DB.

        Args:
            inputs: Sequence of readable channels from upstream nodes.
            output: Writable channel for output (tag, data) pairs.
            observer: Optional execution observer for hooks.
            load_only: As for ``execute``: a ``LOG`` node stores nothing (ITL-1095).
        """
        from orcapod.pipeline.observer import NoOpObserver

        # A loaded node runs only in a load-only run, with its rebuilt operator.
        if self._loaded and (not load_only or self._operator is None):
            await output.close()
            raise self._cannot_compute_error()

        obs = observer if observer is not None else NoOpObserver()
        node_label = self.label
        node_hash = self.content_hash().to_string()
        ctx_obs = obs.contextualize(*self.node_identity_path)

        try:
            ctx_obs.on_node_start(node_label, node_hash)
            self._run_load_only = load_only

            if (
                self._pipeline_database is not None
                and self._cache_mode == CacheMode.REPLAY
            ):
                # Read and discard every input while the stored rows are sent: the
                # upstream nodes run over every row, as with the sync orchestrator,
                # and an input no one reads blocks its producer, and the run, once
                # its channel is full (ITL-1075).
                discards = [asyncio.create_task(_discard(ch)) for ch in inputs]
                try:
                    self._replay_from_cache()
                    assert self._cached_output_stream is not None
                    for tag, data in self._cached_output_stream.iter_data():
                        await output.send((tag, data))
                    _ = await asyncio.gather(*discards)
                    # The inputs are closed, so every upstream node has stamped itself:
                    # stamp after them, or the replayed rows would read as stale.
                    self._update_modified_time()
                finally:
                    for discard in discards:
                        _ = discard.cancel()
                    # Wait for the readers to end; a failure of the replay stays the
                    # error raised.
                    _ = await asyncio.gather(*discards, return_exceptions=True)
                ctx_obs.on_node_end(node_label, node_hash)
                return  # finally block closes output

            # Delegate to the operator, forward its rows downstream and keep them.
            intermediate: Channel[tuple[TagProtocol, DataProtocol]] = Channel()
            should_store = (
                not load_only
                and self._pipeline_database is not None
                and self._cache_mode == CacheMode.LOG
            )
            if should_store:
                # Rows are forwarded while the operator runs, so an input this node
                # will refuse to store must be refused before the first row leaves.
                for input_stream in self._input_streams:
                    require_same_data_context(
                        input_stream.data_context_key, self.data_context_key
                    )
            collected: list[tuple[TagProtocol, DataProtocol]] = []

            async def forward() -> None:
                async for item in intermediate.reader:
                    collected.append(item)
                    await output.send(item)

            hashes = [s.pipeline_hash() for s in self._input_streams]
            operator = cast(StaticOutputOperatorPod, self._operator)
            taps = [_InputTap(reader) for reader in inputs]
            try:
                async with asyncio.TaskGroup() as tg:
                    tg.create_task(
                        operator.async_execute(
                            taps,
                            intermediate.writer,
                            input_pipeline_hashes=hashes,
                        )
                    )
                    tg.create_task(forward())
            except ExceptionGroup as exc:
                # Preserve direct operator errors; concurrent failures stay grouped.
                if len(exc.exceptions) == 1:
                    raise exc.exceptions[0] from exc
                raise

            # TaskGroup has completed. Keep the output on the node, as execute()
            # does, and store it in LOG mode (sync DB write, post-hoc). A run
            # with no rows replaces an earlier run's output too: the node then
            # reads as a zero-row table.
            if collected:
                stream = StaticOutputOperatorPod._materialize_to_stream(collected)
                self._cached_output_stream = stream
                if should_store:
                    self._store_output_stream(stream)
            else:
                # A completed run with no rows keeps the operator's result over its
                # inputs rebuilt with no rows, exactly as the sync orchestrator
                # rebuilds them, so the node reads as computed and empty with the
                # sync path's columns and Arrow types (ITL-1080). An input that
                # delivered no row is its upstream's zero-row table, as on the sync
                # path (ITL-912). Nothing is stored for it.
                empty_result = operator.process(
                    *(
                        StaticOutputOperatorPod._materialize_upstream(
                            tap.rows, upstream, zero_rows=True
                        )
                        for tap, upstream in zip(taps, self._input_streams, strict=True)
                    )
                )
                # Computed, as execute() computes it, so the node reads as run.
                _ = list(empty_result.iter_data())
                self._cached_output_stream = empty_result

            self._update_modified_time()

            ctx_obs.on_node_end(node_label, node_hash)
        except BaseException:
            # A run that raised or was cancelled defines no rows.
            self.clear_cache()
            raise
        finally:
            try:
                await output.close()
            except BaseException:
                # A close that raises or is cancelled fails the run: no rows.
                self.clear_cache()
                raise
