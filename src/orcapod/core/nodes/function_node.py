"""FunctionNode hierarchy — pure blueprint + DB-backed execution node.

Three classes:

* ``FunctionNodeBase`` — shared base; no DB state.  Holds identity,
  schema, and all non-DB properties.
* ``FunctionNode`` — thin blueprint descriptor.  Raises
  ``PipelineJobRequiredError`` on ``iter_data()``.  This is the node
  recorded in a ``Pipeline`` and serialized to disk.
* ``FunctionJobNode`` — DB-backed execution node with full persistence
  logic.  Created by ``PipelineJob`` at run time.

``FunctionNode`` and ``FunctionJobNode`` are *siblings*: both inherit
directly from ``FunctionNodeBase``, neither from the other.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from abc import abstractmethod
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any, Literal, NamedTuple, cast

from orcapod import contexts
from orcapod.channels import Channel, ReadableChannel, WritableChannel
from orcapod.config import OrcapodConfig
from orcapod.core.cached_function_pod import CachedFunctionPod
from orcapod.core.streams.arrow_table_stream import ArrowTableStream
from orcapod.core.streams.base import StreamBase
from orcapod.core.tracker import DEFAULT_TRACKER_MANAGER
from orcapod.errors import PipelineJobRequiredError
from orcapod.protocols.core_protocols import (
    FunctionPodProtocol,
    DataFunctionExecutorProtocol,
    DataFunctionProtocol,
    DataProtocol,
    StreamProtocol,
    TagProtocol,
    TrackerManagerProtocol,
)
from orcapod.protocols.database_protocols import ArrowDatabaseProtocol
from orcapod.protocols.observability_protocols import (
    DataExecutionLoggerProtocol,
    ExecutionObserverProtocol,
)
from orcapod.system_constants import constants
from orcapod.types import (
    ColumnConfig,
    ContentHash,
    NodeConfig,
    Schema,
)
from orcapod.utils import arrow_utils, schema_utils
from orcapod.utils.lazy_module import LazyModule

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    import polars as pl
    import pyarrow as pa
    import pyarrow.compute as pc
else:
    pa = LazyModule("pyarrow")
    pc = LazyModule("pyarrow.compute")
    pl = LazyModule("polars")

# Pipeline entry ID column name used when fetching records from the pipeline
# database. Always present in the table returned by _fetch_joined_records.
_PIPELINE_ENTRY_ID_COL = "__pipeline_entry_id"

# Column name for the stable base entry ID (no recomputation index).
# Stores the per-logical-input identity; later tasks wire this into the
# Phase 1 filter and the in-memory cache key.
_PIPELINE_BASE_ENTRY_ID_COL = "__pipeline_base_entry_id"

# Column storing the recomputation chain index (pa.int32).
# 0 for the first computation, N+1 for each miss-triggered recompute.
_PIPELINE_RECOMPUTATION_INDEX_COL = "__pipeline_recomputation_index"

# Private meta-column name stamped into tags routed to the compute channel.
# Carries the correlation key (bytes) that links an execution result back to
# its original (tag, input_data) pair. Stripped from output tags in
# record_and_forward() before downstream emission.
_TAG_NODE_INPUT_REF = "_tag_node_input_ref"


def _executor_supports_concurrent(
    data_function: DataFunctionProtocol,
) -> bool:
    """Return True if the data function's executor supports concurrent execution."""
    executor = data_function.executor
    return executor is not None and executor.supports_concurrent_execution


# ---------------------------------------------------------------------------
# FunctionNodeBase — shared base (no DB)
# ---------------------------------------------------------------------------


class FunctionNodeBase(StreamBase):
    """Shared base for ``FunctionNode`` and ``FunctionJobNode``.

    Carries all non-DB state: identity, schema, upstreams, and properties
    shared by both the blueprint and the execution variant.
    """

    node_type = "function"

    def __init__(
        self,
        function_pod: FunctionPodProtocol,
        input_stream: StreamProtocol,
        tracker_manager: TrackerManagerProtocol | None = None,
        label: str | None = None,
        config: OrcapodConfig | None = None,
        table_scope: Literal["pipeline_hash", "content_hash"] = "pipeline_hash",
    ):
        if tracker_manager is None:
            tracker_manager = DEFAULT_TRACKER_MANAGER
        self.tracker_manager = tracker_manager
        self._data_function = function_pod.data_function

        # FunctionPod used for the ``producer`` property and pipeline identity
        self._function_pod = function_pod
        super().__init__(label=label, config=config)

        # validate the input stream — skip for UNAVAILABLE streams because their
        # stored schema uses serialized type strings (e.g. 'int64') that the
        # schema-compatibility checker cannot handle, and we will never actually
        # iterate such a stream (CACHE_ONLY mode reads directly from the DB).
        from orcapod.pipeline.serialization import LoadStatus

        _stream_unavailable = (
            hasattr(input_stream, "load_status")
            and input_stream.load_status == LoadStatus.UNAVAILABLE
        )
        if not _stream_unavailable:
            _, incoming_data_types = input_stream.output_schema()
            expected_data_schema = self._data_function.input_data_schema
            if not schema_utils.check_schema_compatibility(
                incoming_data_types, expected_data_schema
            ):
                raise ValueError(
                    f"Incoming data data type {incoming_data_types} from {input_stream} "
                    f"is not compatible with expected input schema {expected_data_schema}"
                )

        self._input_stream = input_stream

        # Descriptor fields — populated by from_descriptor() for read-only/UNAVAILABLE
        # nodes.  Initialized here so they are always present on the concrete class
        # (avoids getattr access for possibly-absent attributes).
        from orcapod.pipeline.serialization import LoadStatus
        self._load_status: LoadStatus = LoadStatus.FULL
        self._stored_content_hash: str | None = None
        self._stored_pipeline_hash: str | None = None
        self._stored_schema: dict = {}
        self._stored_node_uri: tuple[str, ...] = ()
        self._stored_pipeline_path: tuple[str, ...] = ()
        self._stored_result_record_path: tuple[str, ...] = ()
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
    # Core properties
    # ------------------------------------------------------------------

    @property
    def producer(self) -> FunctionPodProtocol:
        return self._function_pod

    @property
    def data_context(self) -> contexts.DataContext:
        return contexts.resolve_context(self.data_context_key)

    @property
    def data_context_key(self) -> str:
        return self._function_pod.data_context_key

    @property
    def executor(self) -> DataFunctionExecutorProtocol | None:
        """The executor set on the underlying data function."""
        return self._data_function.executor

    @executor.setter
    def executor(self, executor: DataFunctionExecutorProtocol | None) -> None:
        """Set or clear the executor on the underlying data function."""
        self._data_function.executor = executor

    @property
    def upstreams(self) -> tuple[StreamProtocol, ...]:
        return (self._input_stream,)

    @upstreams.setter
    def upstreams(self, value: tuple[StreamProtocol, ...]) -> None:
        if len(value) != 1:
            raise ValueError("FunctionPod can only have one upstream")
        self._input_stream = value[0]

    # ------------------------------------------------------------------
    # Read-only overrides (for deserialized nodes without live function_pod)
    # ------------------------------------------------------------------

    def content_hash(self, hasher=None) -> ContentHash:
        """Return the content hash, using stored value in read-only mode."""
        if self._function_pod is None and self._stored_content_hash is not None:
            from orcapod.types import ContentHash as CH

            return CH.from_string(self._stored_content_hash)
        return super().content_hash(hasher)

    def pipeline_hash(self, hasher=None) -> ContentHash:
        """Return the pipeline hash, using stored value in read-only mode."""
        if self._function_pod is None and self._stored_pipeline_hash is not None:
            from orcapod.types import ContentHash as CH

            return CH.from_string(self._stored_pipeline_hash)
        return super().pipeline_hash(hasher)

    def output_schema(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        """Return output schema, using stored value in read-only mode."""
        if self._function_pod is None:
            tag = Schema(self._stored_schema.get("tag", {}))
            data = Schema(self._stored_schema.get("data", {}))
            return tag, data
        tag_schema = self._input_stream.output_schema(
            columns=columns, all_info=all_info
        )[0]
        return tag_schema, self._data_function.output_data_schema

    def keys(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        if self._function_pod is None:
            tag_keys = tuple(self._stored_schema.get("tag", {}).keys())
            data_keys = tuple(self._stored_schema.get("data", {}).keys())
            return tag_keys, data_keys
        tag_schema, data_schema = self.output_schema(
            columns=columns, all_info=all_info
        )
        return tuple(tag_schema.keys()), tuple(data_schema.keys())

    # ------------------------------------------------------------------
    # Pipeline path
    # ------------------------------------------------------------------

    @property
    def node_identity_path(self) -> tuple[str, ...]:
        """Return the node identity path for observer contextualization.

        When ``table_scope="pipeline_hash"`` (default) the path is
        ``pod.uri + (schema:{pipeline_hash},)`` — all runs that share the same
        pipeline structure are routed to one shared table, with per-run
        disambiguation via the ``_node_content_hash`` row-level column.

        When ``table_scope="content_hash"`` the legacy path is returned:
        ``pod.uri + (schema:{pipeline_hash}, instance:{content_hash})``.

        In read-only/UNAVAILABLE mode (no pod) the path stored from the
        deserialized descriptor is returned (empty tuple when absent).
        """
        if self._data_function is None:
            return self._stored_pipeline_path
        if self._node_identity_path_cache is not None:
            return self._node_identity_path_cache
        pf = self._function_pod
        path = pf.uri + (f"schema:{self.pipeline_hash().to_string()}",)
        if self._table_scope != "pipeline_hash":
            path += (f"instance:{self.content_hash().to_string()}",)
        self._node_identity_path_cache = path
        return path

    @property
    def node_uri(self) -> tuple[str, ...]:
        """Canonical URI tuple identifying this computation.

        Identical to ``data_function.uri`` at runtime.
        Returns stored value in read-only (deserialized) mode.
        """
        if self._data_function is None:
            return self._stored_node_uri
        return self._data_function.uri

    # ------------------------------------------------------------------
    # Caching
    # ------------------------------------------------------------------

    def clear_cache(self) -> None:
        """Clear the node identity path cache."""
        self._node_identity_path_cache = None
        self._update_modified_time()

    # ------------------------------------------------------------------
    # as_table
    # ------------------------------------------------------------------

    def as_table(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> "pa.Table":
        if self._cached_output_table is None:
            all_tags: list[dict] = []
            all_data: list[dict] = []
            # Derive Arrow schemas from the declared output_schema upfront so that
            # the cached table always has a consistent, well-formed schema regardless
            # of whether any rows were produced.
            tag_python_schema, data_python_schema = self.output_schema(all_info=True)
            for tag, data in self.iter_data():
                all_tags.append(tag.as_dict(all_info=True))
                all_data.append(data.as_dict(all_info=True))

            converter = self.data_context.type_converter
            tag_arrow_schema = converter.python_schema_to_arrow_schema(tag_python_schema)
            data_arrow_schema = converter.python_schema_to_arrow_schema(data_python_schema)

            if not all_tags:
                # No data was produced (node has not been run yet or produced zero
                # outputs).  Build a zero-row table whose schema is derived from the
                # pod's declared output so that callers always receive a well-formed
                # schema regardless of whether the node has actually executed.
                self._cached_output_table = pa.Table.from_pylist(
                    [],
                    schema=converter.python_schema_to_arrow_schema(
                        tag_python_schema + data_python_schema
                    ),
                )
            else:
                struct_data = converter.python_dicts_to_struct_dicts(
                    all_data, python_schema=data_python_schema
                )
                all_tags_as_tables: pa.Table = pa.Table.from_pylist(
                    all_tags, schema=tag_arrow_schema
                )
                if constants.CONTEXT_KEY in all_tags_as_tables.column_names:
                    all_tags_as_tables = all_tags_as_tables.drop([constants.CONTEXT_KEY])
                all_data_as_tables: pa.Table = pa.Table.from_pylist(
                    struct_data, schema=data_arrow_schema
                )
                self._cached_output_table = arrow_utils.hstack_tables(
                    all_tags_as_tables, all_data_as_tables
                )

        column_config = ColumnConfig.handle_config(columns, all_info=all_info)
        output_table = arrow_utils.apply_column_config(
            self._cached_output_table, column_config, self.keys()[0]
        )

        if column_config.content_hash:
            if self._cached_content_hash_column is None:
                content_hashes = []
                for tag, data in self.iter_data():
                    content_hashes.append(data.content_hash().to_string())
                self._cached_content_hash_column = pa.array(
                    content_hashes, type=pa.large_string()
                )
            assert self._cached_content_hash_column is not None
            hash_column_name = (
                "_content_hash"
                if column_config.content_hash is True
                else column_config.content_hash
            )
            output_table = output_table.append_column(
                hash_column_name, self._cached_content_hash_column
            )

        return output_table

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}(data_function={self._data_function!r}, "
            f"input_stream={self._input_stream!r})"
        )

    @abstractmethod
    def set_ephemeral_store(self, store: "ArrowDatabaseProtocol | None") -> None:
        """Assign or remove the ephemeral result store for this node.

        ``FunctionJobNode`` overrides this with a real implementation that routes
        new computation results to ``store`` instead of the persistent result
        database. ``FunctionNode`` (blueprint) provides a no-op override.

        Args:
            store: An ``ArrowDatabaseProtocol`` instance to attach, or ``None``
                to detach.
        """


# ---------------------------------------------------------------------------
# FunctionNode — thin blueprint (no DB)
# ---------------------------------------------------------------------------


class FunctionNode(FunctionNodeBase):
    """Thin blueprint descriptor for a function pod invocation.

    Carries no database references.  Calling ``iter_data()`` raises
    ``PipelineJobRequiredError`` — wrap the containing ``Pipeline`` in a
    ``PipelineJob`` to obtain an executable ``FunctionJobNode``.

    This is the node type recorded inside a ``Pipeline`` context manager
    and serialized to disk via ``Pipeline.save()``.
    """

    def __init__(
        self,
        function_pod: FunctionPodProtocol,
        input_stream: StreamProtocol,
        tracker_manager: TrackerManagerProtocol | None = None,
        label: str | None = None,
        config: OrcapodConfig | None = None,
        table_scope: Literal["pipeline_hash", "content_hash"] = "pipeline_hash",
    ):
        super().__init__(
            function_pod=function_pod,
            input_stream=input_stream,
            tracker_manager=tracker_manager,
            label=label,
            config=config,
            table_scope=table_scope,
        )
        # Blueprint nodes have no in-memory output table cache
        self._cached_output_table: "pa.Table | None" = None
        self._cached_content_hash_column: "pa.Array | None" = None

    def set_ephemeral_store(self, store: "ArrowDatabaseProtocol | None") -> None:
        """No-op — blueprint nodes carry no database references."""

    # ------------------------------------------------------------------
    # from_descriptor — reconstruct from a serialized pipeline descriptor
    # ------------------------------------------------------------------

    @classmethod
    def from_descriptor(
        cls,
        descriptor: dict[str, Any],
        function_pod: FunctionPodProtocol | None,
        input_stream: StreamProtocol | None,
        databases: dict[str, Any],
    ) -> "FunctionNode":
        """Construct a FunctionNode from a serialized descriptor.

        When *function_pod* and *input_stream* are both provided the node
        operates in full mode -- constructed normally via ``__init__``.
        When *function_pod* is ``None`` the node is created in read-only
        mode with metadata from the descriptor; computation methods will
        raise ``PipelineJobRequiredError``.

        Args:
            descriptor: The serialized node descriptor dict.
            function_pod: An optional live function pod.  ``None`` for
                read-only mode.
            input_stream: An optional live input stream.  ``None`` for
                read-only mode.
            databases: Mapping of database role names (``"pipeline"``,
                ``"result"``) to database instances.

        Returns:
            A new ``FunctionNode`` instance.
        """
        from orcapod.pipeline.serialization import LoadStatus

        if "table_scope" not in descriptor:
            raise ValueError(
                f"FunctionNode descriptor is missing required 'table_scope' field: "
                f"{descriptor.get('label', '<unlabeled>')}"
            )
        raw_table_scope = descriptor["table_scope"]
        if raw_table_scope not in ("pipeline_hash", "content_hash"):
            raise ValueError(
                f"FunctionNode descriptor has invalid 'table_scope' value "
                f"{raw_table_scope!r} for {descriptor.get('label', '<unlabeled>')}; "
                f"expected one of ('pipeline_hash', 'content_hash')"
            )
        table_scope = cast(Literal["pipeline_hash", "content_hash"], raw_table_scope)

        if function_pod is not None and input_stream is not None:
            # Full / READ_ONLY / CACHE_ONLY mode: construct normally via __init__.
            node = cls(
                function_pod=function_pod,
                input_stream=input_stream,
                label=descriptor.get("label"),
                table_scope=table_scope,
            )
            node._descriptor = descriptor

            # Determine mode based on upstream availability and function type.
            from orcapod.core.data_function_proxy import DataFunctionProxy

            input_unavailable = (
                hasattr(input_stream, "load_status")
                and input_stream.load_status == LoadStatus.UNAVAILABLE
            )
            if input_unavailable:
                node._load_status = LoadStatus.CACHE_ONLY
            elif isinstance(function_pod.data_function, DataFunctionProxy):
                node._load_status = LoadStatus.READ_ONLY
            else:
                node._load_status = LoadStatus.FULL
            return node

        # Read-only mode: bypass __init__, set minimum required state
        node = cls.__new__(cls)

        # From LabelableMixin
        node._label = descriptor.get("label")

        # From DataContextMixin
        node._data_context = contexts.resolve_context(
            descriptor.get("data_context_key")
        )
        from orcapod.config import DEFAULT_CONFIG, OrcapodConfig

        config_dict = descriptor.get("config")
        node._orcapod_config = (
            OrcapodConfig.from_dict(config_dict) if config_dict is not None else DEFAULT_CONFIG
        )

        # From ContentIdentifiableBase
        node._content_hash_cache = {}
        node._cached_int_hash = None

        # From PipelineElementBase
        node._pipeline_hash_cache = {}

        # From TemporalMixin
        node._modified_time = None

        # From FunctionNodeBase
        node._function_pod = None
        node._data_function = None
        node._input_stream = None
        node.tracker_manager = DEFAULT_TRACKER_MANAGER

        # Blueprint-level table caches
        node._cached_output_table = None
        node._cached_content_hash_column = None

        # Descriptor metadata for read-only access
        node._descriptor = descriptor
        node._stored_schema = descriptor.get("output_schema", {})
        node._stored_content_hash = descriptor.get("content_hash")
        node._stored_pipeline_hash = descriptor.get("pipeline_hash")
        node._stored_pipeline_path = tuple(descriptor.get("pipeline_path", ()))
        node._stored_node_uri = tuple(descriptor.get("node_uri") or [])
        node._stored_result_record_path = tuple(
            descriptor.get("result_record_path", ())
        )
        node._table_scope = table_scope
        node._node_identity_path_cache = None

        # FunctionNode loaded read-only is always UNAVAILABLE (no DB)
        node._load_status = LoadStatus.UNAVAILABLE

        return node

    # ------------------------------------------------------------------
    # iter_data — raises PipelineJobRequiredError
    # ------------------------------------------------------------------

    def iter_data(self) -> Iterator[tuple[TagProtocol, DataProtocol]]:
        """Raise ``PipelineJobRequiredError`` — blueprint nodes cannot produce data.

        Raises:
            PipelineJobRequiredError: Always.
        """
        raise PipelineJobRequiredError(
            f"FunctionNode '{self.label}' is a blueprint — it carries no database "
            "references and cannot produce data directly.  "
            "Wrap the containing Pipeline in a PipelineJob to obtain an executable "
            "FunctionJobNode."
        )
        # yield is needed to satisfy the Iterator return type annotation
        return  # pragma: no cover
        yield  # pragma: no cover

    def as_node(self) -> FunctionNode:
        """Return the lightweight blueprint equivalent of this node.

        Returns a fresh ``FunctionNode`` with the same function pod, input
        stream, label, table scope, and tracker manager.  Its
        ``content_hash()`` / ``pipeline_hash()`` are identical to those of
        this node.

        Returns:
            A new equivalent ``FunctionNode``.

        Raises:
            RuntimeError: If this node is in UNAVAILABLE state (no live
                function pod).  UNAVAILABLE nodes cannot be cloned into a
                usable blueprint — callers must handle this status before
                invoking ``as_node()``.
        """
        if self._function_pod is None:
            from orcapod.pipeline.serialization import LoadStatus

            raise RuntimeError(
                f"Cannot clone FunctionNode {self._label!r} into a blueprint: "
                "the node is UNAVAILABLE (no live function pod was provided when "
                "it was loaded). Only FULL or READ_ONLY nodes support as_node()."
            )
        return FunctionNode(
            function_pod=self._function_pod,
            input_stream=self._input_stream,
            label=self._label,
            table_scope=self._table_scope,
            tracker_manager=self.tracker_manager,
        )


# ---------------------------------------------------------------------------
# _ResultDatabaseReader — minimal result accessor for read-only stubs
# ---------------------------------------------------------------------------


class _ResultDatabaseReader:
    """Minimal result-database accessor for read-only ``FunctionJobNode`` stubs.

    Used by ``FunctionJobNode.attach_databases()`` when ``_function_pod`` is
    ``None`` (i.e., nodes loaded from a saved job without a live function pod).
    Provides the ``result_database`` and ``record_path`` attributes that
    ``get_all_records()`` and ``_load_cached_entries()`` require, without
    needing a ``CachedFunctionPod``.
    """

    def __init__(
        self,
        result_database: ArrowDatabaseProtocol,
        record_path: tuple[str, ...],
    ) -> None:
        self.result_database = result_database
        self._record_path = record_path

    @property
    def record_path(self) -> tuple[str, ...]:
        """Path to cached records in the result store."""
        return self._record_path


# ---------------------------------------------------------------------------
# FunctionJobNode — DB-backed execution node
# ---------------------------------------------------------------------------


class _JoinedRecords(NamedTuple):
    """Internal result type returned by ``_fetch_joined_records``.

    Attributes:
        table: The joined ``pa.Table``, always including a
            ``__pipeline_entry_id`` column (the pipeline DB row key) and
            ``DATA_RECORD_ID``. Does not have ``ColumnConfig`` filtering
            applied — that is the caller's responsibility.
        taginfo_columns: Column names from the pipeline database fetch,
            captured before the join. Used by ``_load_cached_entries`` to
            derive tag keys in the CACHE_ONLY (``_input_stream is None``)
            fallback path, where the tag columns cannot be inferred from the
            input stream and must be identified by exclusion from the taginfo
            column set.
    """

    table: pa.Table
    taginfo_columns: tuple[str, ...]


class FunctionJobNode(FunctionNodeBase):
    """DB-backed execution node for function pod invocations.

    Created by ``PipelineJob`` at run time; never recorded inside a plain
    ``Pipeline``.  Carries all persistence logic: ``CachedFunctionPod``
    wrapping, pipeline records, and two-phase ``iter_data()`` / async
    execution.
    """

    def __init__(
        self,
        function_pod: FunctionPodProtocol,
        input_stream: StreamProtocol,
        tracker_manager: TrackerManagerProtocol | None = None,
        label: str | None = None,
        config: OrcapodConfig | None = None,
        # Optional DB params for persistent mode:
        pipeline_database: ArrowDatabaseProtocol | None = None,
        result_database: ArrowDatabaseProtocol | None = None,
        ephemeral_database: ArrowDatabaseProtocol | None = None,
        table_scope: Literal["pipeline_hash", "content_hash"] = "pipeline_hash",
    ):
        super().__init__(
            function_pod=function_pod,
            input_stream=input_stream,
            tracker_manager=tracker_manager,
            label=label,
            config=config,
            table_scope=table_scope,
        )

        # stream-level caching state
        self._cached_output_datas: dict[
            bytes, tuple[TagProtocol, DataProtocol | None]
        ] = {}
        self._cached_output_table: "pa.Table | None" = None
        self._cached_content_hash_column: "pa.Array | None" = None

        # DB persistence state (initially None; set via __init__ params or attach_databases)
        self._pipeline_database: ArrowDatabaseProtocol | None = None
        self._cached_function_pod: CachedFunctionPod | None = None

        # Ephemeral result store (None until set_ephemeral_store() is called by the pipeline)
        self.ephemeral_result_store: "ArrowDatabaseProtocol | None" = None
        self._ephemeral_cached_pod: CachedFunctionPod | None = None

        # Node-level orchestrator config — applied post-construction via the
        # .node_config setter or PipelineJob.apply_node_config().
        self._node_config: NodeConfig = NodeConfig()

        self.attach_databases(
            pipeline_database=pipeline_database,
            result_database=result_database,
            ephemeral_database=ephemeral_database,
        )

    # ------------------------------------------------------------------
    # node_config property
    # ------------------------------------------------------------------

    @property
    def node_config(self) -> NodeConfig:
        """Per-node pipeline execution configuration."""
        return self._node_config

    @node_config.setter
    def node_config(self, value: NodeConfig) -> None:
        self._node_config = value

    # ------------------------------------------------------------------
    # attach_databases
    # ------------------------------------------------------------------

    def attach_databases(
        self,
        pipeline_database: ArrowDatabaseProtocol | None = None,
        result_database: ArrowDatabaseProtocol | None = None,
        ephemeral_database: ArrowDatabaseProtocol | None = None,
    ) -> None:
        """Attach databases for persistent caching and pipeline records.

        For live nodes (``_function_pod`` is set), creates a
        ``CachedFunctionPod`` wrapping the original function pod for result
        caching.  For read-only stubs (``_function_pod`` is ``None``, e.g.
        nodes loaded from a saved job), creates a ``_ResultDatabaseReader``
        using the record path stored in the descriptor so that
        ``get_all_records()`` and ``_load_cached_entries()`` still work.

        When ``pipeline_database`` is ``None``, the persistent-DB portion of
        setup is skipped entirely — no ``CachedFunctionPod`` is created and
        ``_pipeline_database`` is left as ``None``.  Only
        ``ephemeral_database`` wiring proceeds in that case.

        The databases are expected to be pre-scoped by the pipeline (via
        ``db.at(*pipeline_name).at("_result")`` etc.) so no additional path
        prefix is needed here.

        Args:
            pipeline_database: Database for pipeline records. Pass ``None``
                to skip persistent-DB setup (ephemeral-only or deferred
                wiring).
            result_database: Database for cached results. Defaults to
                ``pipeline_database.at("_result")`` when ``pipeline_database``
                is not ``None``.
            ephemeral_database: Optional ephemeral store to attach immediately
                via ``set_ephemeral_store()``. Equivalent to calling
                ``set_ephemeral_store(ephemeral_database)`` after
                ``attach_databases()``.
        """
        if pipeline_database is not None:
            if result_database is None:
                # Default result database is pipeline_database scoped to "_result"
                # so that results are stored separately from pipeline-level records.
                result_database = pipeline_database.at("_result")

            if self._function_pod is not None:
                # Normal path: wrap in CachedFunctionPod for compute + cache.
                self._cached_function_pod = CachedFunctionPod(
                    self._function_pod,
                    result_database=result_database,
                )
            else:
                # Read-only stub path (loaded job without a live function pod).
                # Use the record path stored from the descriptor so that
                # get_all_records() / _load_cached_entries() can query the DB.
                self._cached_function_pod = _ResultDatabaseReader(  # type: ignore[assignment]
                    result_database=result_database,
                    record_path=self._stored_result_record_path,
                )

            self._pipeline_database = pipeline_database

            # Clear all caches
            self._node_identity_path_cache = None
            self.clear_cache()
            self._invalidate_content_hash_cache()
            self._invalidate_pipeline_hash_cache()

        if ephemeral_database is not None:
            self.set_ephemeral_store(ephemeral_database)

    # ------------------------------------------------------------------
    # from_descriptor — read-only stub for loaded jobs
    # ------------------------------------------------------------------

    @classmethod
    def from_descriptor(
        cls,
        descriptor: dict[str, Any],
        input_stream: StreamProtocol | None = None,
        databases: dict[str, Any] | None = None,
        job_content_hash: str | None = None,
    ) -> "FunctionJobNode":
        """Create a read-only ``FunctionJobNode`` stub from a serialized descriptor.

        Used by ``PipelineJob.load()`` to populate ``_persistent_node_map``
        with database-backed stubs for function nodes.  The stubs support
        ``get_all_records()`` and ``_load_cached_entries()`` on previously
        computed results without needing a live ``function_pod``.

        Computation-path methods (``execute()``, ``iter_data()``) will
        fail for stub nodes because ``_function_pod`` is ``None``.

        Args:
            descriptor: Serialized node descriptor dict (as produced by
                ``Pipeline.save()``).
            input_stream: Optional upstream stream.  May be ``None`` for
                read-only stubs where execution is not needed.
            databases: Optional mapping ``{"pipeline": db, "result": db}``.
                When ``"pipeline"`` is present, ``attach_databases()`` is
                called immediately so the stub is ready for DB queries.
            job_content_hash: Optional live (data-inclusive) content hash
                from the run that produced the records being loaded.  When
                provided, overrides the blueprint hash stored in *descriptor*
                so that ``content_hash()`` matches the ``_node_content_hash``
                column written to the DB at execution time.  Required for
                ``get_all_records()`` to correctly filter DB rows when
                ``table_scope="pipeline_hash"`` (the default).

        Returns:
            A ``FunctionJobNode`` in read-only stub mode
            (``load_status=UNAVAILABLE``).
        """
        from orcapod.config import DEFAULT_CONFIG, OrcapodConfig
        from orcapod.pipeline.serialization import LoadStatus

        if "table_scope" not in descriptor:
            raise ValueError(
                f"FunctionJobNode descriptor missing required 'table_scope' field: "
                f"{descriptor.get('label', '<unlabeled>')}"
            )
        raw_table_scope = descriptor["table_scope"]
        if raw_table_scope not in ("pipeline_hash", "content_hash"):
            raise ValueError(
                f"FunctionJobNode descriptor has invalid 'table_scope' value "
                f"{raw_table_scope!r} for {descriptor.get('label', '<unlabeled>')}; "
                "expected one of ('pipeline_hash', 'content_hash')"
            )

        node: FunctionJobNode = cls.__new__(cls)

        # From LabelableMixin
        node._label = descriptor.get("label")

        # From DataContextMixin
        node._data_context = contexts.resolve_context(
            descriptor.get("data_context_key")
        )
        config_dict = descriptor.get("config")
        node._orcapod_config = (
            OrcapodConfig.from_dict(config_dict) if config_dict is not None else DEFAULT_CONFIG
        )

        # From ContentIdentifiableBase
        node._content_hash_cache = {}
        node._cached_int_hash = None

        # From PipelineElementBase
        node._pipeline_hash_cache = {}

        # From TemporalMixin
        node._modified_time = None

        # From FunctionNodeBase
        node._function_pod = None
        node._data_function = None
        node._input_stream = input_stream
        node.tracker_manager = DEFAULT_TRACKER_MANAGER

        # FunctionNodeBase descriptor fields (normally set by __init__)
        node._load_status = LoadStatus.UNAVAILABLE
        # Use the live (data-inclusive) content hash when available — it must
        # match the _node_content_hash column written to the DB at run time.
        # The blueprint hash stored in descriptor["content_hash"] is computed
        # from schema-only upstreams and differs from the live hash, causing
        # _filter_by_content_hash() to return zero rows in get_all_records().
        node._stored_content_hash = (
            job_content_hash if job_content_hash is not None
            else descriptor.get("content_hash")
        )
        node._stored_pipeline_hash = descriptor.get("pipeline_hash")
        node._stored_schema = descriptor.get("output_schema", {})
        node._stored_node_uri = tuple(descriptor.get("node_uri") or ())
        node._stored_result_record_path = tuple(descriptor.get("node_uri") or ())
        node._descriptor = descriptor
        node._table_scope = raw_table_scope
        node._node_identity_path_cache = None

        # Compute _stored_pipeline_path from node_uri + pipeline_hash so that
        # node_identity_path (used by get_all_records and _load_cached_entries)
        # resolves to the correct DB path.  This mirrors the live computation:
        #   path = function_pod.uri + (f"schema:{pipeline_hash}",)
        # Since node_uri == function_pod.uri (same fields), we reconstruct it here.
        pipeline_hash_str = descriptor.get("pipeline_hash", "")
        node._stored_pipeline_path = node._stored_node_uri + (
            f"schema:{pipeline_hash_str}",
        )
        if raw_table_scope != "pipeline_hash":
            # Use _stored_content_hash (already set to job_content_hash when
            # provided, otherwise falls back to the blueprint hash from the
            # descriptor) so the instance fragment matches the DB path written
            # during the original run for table_scope="content_hash".
            content_hash_str = node._stored_content_hash or ""
            node._stored_pipeline_path += (f"instance:{content_hash_str}",)

        # FunctionJobNode — stream-level caching state
        node._cached_output_datas = {}
        node._cached_output_table = None
        node._cached_content_hash_column = None

        # FunctionJobNode — orchestrator config
        node._node_config = NodeConfig()

        # FunctionJobNode — DB persistence state (wired by attach_databases)
        node._pipeline_database = None
        node._cached_function_pod = None

        # Wire databases if provided
        pipeline_db = (databases or {}).get("pipeline")
        result_db = (databases or {}).get("result")
        if pipeline_db is not None:
            node.attach_databases(
                pipeline_database=pipeline_db,
                result_database=result_db,
            )

        return node

    # ------------------------------------------------------------------
    # Override clear_cache to also clear DB caches
    # ------------------------------------------------------------------

    def clear_cache(self) -> None:
        """Clear in-memory output caches, content hash cache, and node identity path cache."""
        super().clear_cache()
        self._cached_output_datas.clear()
        self._cached_output_table = None
        self._cached_content_hash_column = None

    def set_ephemeral_store(self, store: "ArrowDatabaseProtocol | None") -> None:
        """Assign or remove the ephemeral result store.

        When *store* is not ``None``, creates a ``CachedFunctionPod`` backed by
        *store* so that ephemeral writes use the same format as persistent writes.
        When *store* is ``None``, clears both the store and the ephemeral pod.

        Note:
            For deserialized read-only nodes where ``_function_pod`` is ``None``,
            ``ephemeral_result_store`` is assigned but ``_ephemeral_cached_pod``
            remains ``None``. Such nodes cannot compute new results anyway, so
            ``_process_data_internal`` must guard against this by checking
            ``_ephemeral_cached_pod is not None`` before attempting ephemeral writes.

        Args:
            store: The ``ArrowDatabaseProtocol`` to use for ephemeral result
                storage, or ``None`` to detach and revert to persistent-only writes.
        """
        self.ephemeral_result_store = store
        if store is not None and self._function_pod is not None:
            self._ephemeral_cached_pod = CachedFunctionPod(
                self._function_pod,
                result_database=store,
            )
        else:
            self._ephemeral_cached_pod = None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _require_pipeline_database(self) -> None:
        """Raise a clear RuntimeError if no pipeline database is attached.

        Called at the top of methods that unconditionally access
        ``self._pipeline_database``.  Provides an actionable error message
        instead of an opaque ``AttributeError: 'NoneType' object has no
        attribute ...`` when a definition-level pipeline is executed without
        supplying a database.
        """
        if self._pipeline_database is None:
            raise RuntimeError(
                f"FunctionJobNode '{self.label}' has no pipeline database attached. "
                "Either construct the pipeline with a pipeline_database argument, "
                "or supply one via Pipeline.load(..., pipeline_database=<db>)."
            )

    def _filter_by_content_hash(self, table: "pa.Table") -> "pa.Table":
        """Filter *table* to rows whose ``NODE_CONTENT_HASH_COL`` matches this node.

        Only applied when ``table_scope="pipeline_hash"`` because in that mode
        multiple runs share the same DB table and must be disambiguated at read
        time.  In ``"content_hash"`` mode every run has its own table so no
        filtering is needed.
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
        own_hash = self.content_hash().to_string()
        mask = pc.equal(table.column(col_name), own_hash)
        return table.filter(mask)

    # ------------------------------------------------------------------
    # as_node — return the lightweight FunctionNode equivalent
    # ------------------------------------------------------------------

    def as_node(self) -> FunctionNode:
        """Return the lightweight ``FunctionNode`` equivalent of this job node.

        Returns:
            A new ``FunctionNode`` with the same function pod, input stream,
            label, table scope, and tracker manager.  Its ``content_hash()`` /
            ``pipeline_hash()`` are identical to those of this ``FunctionJobNode``.
        """
        return FunctionNode(
            function_pod=self._function_pod,
            input_stream=self._input_stream,
            label=self._label,
            table_scope=self._table_scope,
            tracker_manager=self.tracker_manager,
        )

    # ------------------------------------------------------------------
    # Data processing
    # ------------------------------------------------------------------

    def execute_data(
        self,
        tag: TagProtocol,
        data: DataProtocol,
    ) -> tuple[TagProtocol, DataProtocol | None]:
        """Execute a single data: compute, persist, and cache.

        Internal method for orchestrators. The caller must guarantee that
        the tag and data conform to the expected input schema (matching
        ``self._input_stream``). No validation is performed.

        Args:
            tag: The tag associated with the data.
            data: The input data to process.

        Returns:
            A ``(tag, output_data)`` tuple.
        """
        tag_out, result = self._process_data_internal(tag, data)
        return tag_out, result

    def execute(
        self,
        input_stream: StreamProtocol,
        *,
        observer: ExecutionObserverProtocol | None = None,
        error_policy: Literal["continue", "fail_fast"] = "continue",
    ) -> list[tuple[TagProtocol, DataProtocol]]:
        """Execute all data from a stream: compute, persist, and cache.

        For each data: fire ``on_data_start``, check the in-memory cache
        (populated from DB if needed), compute if missing, fire
        ``on_data_end`` or ``on_data_crash``.

        Args:
            input_stream: The input stream to process.
            observer: Optional execution observer for hooks.
            error_policy: ``"continue"`` skips failed data;
                ``"fail_fast"`` re-raises on the first failure.

        Returns:
            Materialized list of (tag, output_data) pairs, excluding
            ``None`` outputs and failed data.
        """
        from orcapod.pipeline.observer import NoOpObserver

        node_label = self.label
        node_hash = self.content_hash().to_string()

        obs = observer if observer is not None else NoOpObserver()
        ctx_obs = obs.contextualize(*self.node_identity_path)

        tag_schema = input_stream.output_schema(columns={"system_tags": True})[0]
        ctx_obs.on_node_start(node_label, node_hash, tag_schema=tag_schema)

        # Collect upstream entries and resolve base_entry_ids (stable across recomputation)
        upstream_entries: list[tuple[TagProtocol, DataProtocol, bytes]] = [
            (tag, data, self.compute_base_entry_id(tag, data))
            for tag, data in input_stream.iter_data()
        ]
        base_entry_ids = [eid for _, _, eid in upstream_entries]

        # Hot-load any already-computed results from DB into _cached_output_datas.
        # get_cached_results() is called for its side effect (populating the
        # in-memory cache); the returned dict is intentionally discarded here so
        # that the per-data cache-hit check below uses _cached_output_datas
        # directly — which includes None-output entries (function returned None)
        # and prevents spurious recomputation of already-processed data.
        self.get_cached_results(base_entry_ids=base_entry_ids)

        output: list[tuple[TagProtocol, DataProtocol]] = []
        for tag, data, base_entry_id in upstream_entries:
            ctx_obs.on_data_start(node_label, tag, data)

            if base_entry_id in self._cached_output_datas:
                tag_out, result = self._cached_output_datas[base_entry_id]
                ctx_obs.on_data_end(node_label, tag, data, result, cached=True)
                if result is not None:
                    output.append((tag_out, result))
            else:
                pkt_logger = ctx_obs.create_data_logger(tag, data)
                try:
                    tag_out, result = self._process_data_internal(
                        tag, data, logger=pkt_logger
                    )
                except Exception as exc:
                    logger.warning(
                        "Data execution failed in %s: %s",
                        node_label,
                        exc,
                        exc_info=True,
                    )
                    ctx_obs.on_data_crash(node_label, tag, data, exc)
                    if error_policy == "fail_fast":
                        ctx_obs.on_node_end(node_label, node_hash)
                        raise
                else:
                    ctx_obs.on_data_end(
                        node_label, tag, data, result, cached=False
                    )
                    if result is not None:
                        output.append((tag_out, result))

        ctx_obs.on_node_end(node_label, node_hash)
        # Mark this node as freshly computed so subsequent iter_data() calls
        # skip the is_stale check and serve results directly from the in-memory cache.
        self._update_modified_time()
        return output

    def _process_data_internal(
        self,
        tag: TagProtocol,
        data: DataProtocol,
        *,
        logger: DataExecutionLoggerProtocol | None = None,
    ) -> tuple[TagProtocol, DataProtocol | None]:
        """Core compute + persist + cache.

        Used by ``execute_data`` and ``execute``.
        Stores result in ``_cached_output_datas`` keyed by base_entry_id.
        Exceptions propagate to the caller — no error handling here.

        When ``node_config.is_result_ephemeral=True``:
        - Uses ``_ephemeral_cached_pod`` for both compute and storage.
        - Raises ``RuntimeError`` if no ephemeral store has been set.
        - Calls ``add_pipeline_record`` which computes the next recomputation
          index (``max_index + 1``) and writes with ``skip_duplicates=True``
          so concurrent coroutines are safely serialised.

        When ``node_config.is_result_ephemeral=False`` (default):
        - Uses ``_cached_function_pod`` (persistent DB) or raw function pod.
        - Calls ``add_pipeline_record`` which computes the next recomputation
          index (``max_index + 1``) and writes with ``skip_duplicates=True``
          so concurrent coroutines are safely serialised.

        Returns:
            A ``(tag, output_data)`` 2-tuple.
        """
        ephemeral_result = self._node_config.is_result_ephemeral or False

        if ephemeral_result:
            if self._ephemeral_cached_pod is None:
                raise RuntimeError(
                    f"FunctionJobNode '{self.label}' has is_result_ephemeral=True but no "
                    "ephemeral store has been assigned. Call set_ephemeral_store() with "
                    "an ArrowDatabaseProtocol before executing this node."
                )
            tag_out, output_data = self._ephemeral_cached_pod.process_data(
                tag, data, logger=logger
            )
            if output_data is not None:
                result_computed = bool(
                    output_data.get_meta_value(
                        self._ephemeral_cached_pod.RESULT_COMPUTED_FLAG, False
                    )
                )
                if self._pipeline_database is not None:
                    self.add_pipeline_record(
                        tag,
                        data,
                        data_record_id=output_data.datagram_uuid,
                        computed=result_computed,
                        is_ephemeral=True,
                    )
        elif self._cached_function_pod is not None:
            tag_out, output_data = self._cached_function_pod.process_data(
                tag, data, logger=logger
            )
            if output_data is not None:
                result_computed = bool(
                    output_data.get_meta_value(
                        self._cached_function_pod.RESULT_COMPUTED_FLAG, False
                    )
                )
                self.add_pipeline_record(
                    tag,
                    data,
                    data_record_id=output_data.datagram_uuid,
                    computed=result_computed,
                )
        else:
            tag_out, output_data = self._function_pod.process_data(
                tag, data, logger=logger
            )

        # Store by base_entry_id (stable across recomputation cycles) and invalidate caches
        base_entry_id = self.compute_base_entry_id(tag, data)
        self._cached_output_datas[base_entry_id] = (tag_out, output_data)
        self._cached_output_table = None
        self._cached_content_hash_column = None

        return tag_out, output_data

    def get_cached_results(
        self, base_entry_ids: list[bytes]
    ) -> dict[bytes, tuple[TagProtocol, DataProtocol]]:
        """Public cache façade: return already-computed results for the given base entry IDs.

        Serves hits directly from the in-memory cache (``_cached_output_datas``).
        For IDs not yet cached, delegates to ``_load_cached_entries`` which calls
        ``_fetch_joined_records`` to load from the pipeline and result databases.
        Add-only semantics: existing in-memory entries are never cleared or
        overwritten.

        Does NOT apply user-facing column filtering — see ``get_all_records``
        for that.

        Args:
            base_entry_ids: Stable base entry IDs (from ``compute_base_entry_id``)
                to look up.

        Returns:
            Mapping from base_entry_id to ``(tag, output_data)`` for found entries.
            Empty dict if no DB is attached, ``base_entry_ids`` is empty, or no
            matches are found.
        """
        if self._cached_function_pod is None or not base_entry_ids:
            return {}

        missing = [eid for eid in base_entry_ids if eid not in self._cached_output_datas]
        if missing:
            loaded = self._load_cached_entries(missing)
            self._cached_output_datas.update(loaded)
            if loaded:
                self._cached_output_table = None
                self._cached_content_hash_column = None

        return {
            eid: self._cached_output_datas[eid]
            for eid in base_entry_ids
            if eid in self._cached_output_datas
            and self._cached_output_datas[eid][1] is not None
        }

    async def _async_process_data_internal(
        self,
        tag: TagProtocol,
        data: DataProtocol,
        *,
        logger: DataExecutionLoggerProtocol | None = None,
    ) -> tuple[TagProtocol, DataProtocol | None]:
        """Async counterpart of ``_process_data_internal``.

        Mirrors the sync ephemeral/persistent branch logic using async variants
        of ``process_data``. See ``_process_data_internal`` for full behaviour
        documentation.

        Returns:
            A ``(tag, output_data)`` 2-tuple.
        """
        ephemeral_result = self._node_config.is_result_ephemeral or False

        if ephemeral_result:
            if self._ephemeral_cached_pod is None:
                raise RuntimeError(
                    f"FunctionJobNode '{self.label}' has is_result_ephemeral=True but no "
                    "ephemeral store has been assigned. Call set_ephemeral_store() with "
                    "an ArrowDatabaseProtocol before executing this node."
                )
            tag_out, output_data = await self._ephemeral_cached_pod.async_process_data(
                tag, data, logger=logger
            )
            if output_data is not None:
                result_computed = bool(
                    output_data.get_meta_value(
                        self._ephemeral_cached_pod.RESULT_COMPUTED_FLAG, False
                    )
                )
                if self._pipeline_database is not None:
                    self.add_pipeline_record(
                        tag,
                        data,
                        data_record_id=output_data.datagram_uuid,
                        computed=result_computed,
                        is_ephemeral=True,
                    )
        elif self._cached_function_pod is not None:
            tag_out, output_data = await self._cached_function_pod.async_process_data(
                tag, data, logger=logger
            )
            if output_data is not None:
                result_computed = bool(
                    output_data.get_meta_value(
                        self._cached_function_pod.RESULT_COMPUTED_FLAG, False
                    )
                )
                self.add_pipeline_record(
                    tag,
                    data,
                    data_record_id=output_data.datagram_uuid,
                    computed=result_computed,
                )
        else:
            tag_out, output_data = await self._function_pod.async_process_data(
                tag, data, logger=logger
            )

        # Store by base_entry_id (stable across recomputation cycles) and invalidate caches
        base_entry_id = self.compute_base_entry_id(tag, data)
        self._cached_output_datas[base_entry_id] = (tag_out, output_data)
        self._cached_output_table = None
        self._cached_content_hash_column = None

        return tag_out, output_data

    def _build_entry_id_preimage(
        self,
        tag: TagProtocol,
        input_data: DataProtocol,
    ) -> pa.Table:
        """Builds the shared Arrow preimage used by both entry-ID methods.

        Combines the tag's system columns with the input data hash and
        node content hash into a single-row Arrow table.

        Args:
            tag: The tag datagram for the input row.
            input_data: The data datagram for the input row.

        Returns:
            A single-row ``pa.Table`` with system-tag columns,
            ``INPUT_DATA_HASH_COL``, and ``NODE_CONTENT_HASH_COL``.
        """
        return (
            tag.as_table(columns={"system_tags": True})
            .append_column(
                constants.INPUT_DATA_HASH_COL,
                pa.array([input_data.content_hash().to_string()], type=pa.large_string()),
            )
            .append_column(
                constants.NODE_CONTENT_HASH_COL,
                pa.array([self.content_hash().to_string()], type=pa.large_string()),
            )
        )

    def compute_base_entry_id(
        self,
        tag: TagProtocol,
        input_data: DataProtocol,
    ) -> bytes:
        """Computes the stable (recomputation-index-free) entry ID for a (tag, data) pair.

        This value is identical to the pre-ITL-508 ``compute_pipeline_entry_id`` output:
        it hashes the tag's system columns plus ``INPUT_DATA_HASH_COL`` and
        ``NODE_CONTENT_HASH_COL``. Because it excludes the recomputation index it is
        stable across all recomputation attempts for the same logical input.

        The base entry ID is stored in ``_PIPELINE_BASE_ENTRY_ID_COL`` and will be
        used as the in-memory cache key and Phase 1 filter in subsequent tasks.

        Args:
            tag: The tag datagram for the input row.
            input_data: The data datagram for the input row.

        Returns:
            A bytes value in ``b"{method}:{digest}"`` format.
        """
        return self.data_context.arrow_hasher.hash_table(
            self._build_entry_id_preimage(tag, input_data)
        ).to_prefixed_digest()

    def compute_pipeline_entry_id(
        self,
        tag: TagProtocol,
        input_data: DataProtocol,
        recomputation_index: int = 0,
    ) -> bytes:
        """Compute a versioned pipeline entry ID from tag + system tags + input data hash + index.

        Extends the base preimage (see ``_build_entry_id_preimage``) with a
        ``_PIPELINE_RECOMPUTATION_INDEX_COL`` column (value: ``recomputation_index``,
        type ``pa.int32()``) so that each recomputation attempt receives a distinct
        DB primary key.

        At ``recomputation_index=0`` this produces a hash that differs from the
        pre-ITL-508 implementation (the index column is now part of the preimage).
        Existing pipeline DB records are implicitly invalidated — acceptable
        because the project is pre-v0.1.0.

        ``NODE_CONTENT_HASH_COL`` is always included so that two runs processing
        identical inputs each get a distinct entry ID, regardless of table scope.

        Args:
            tag: The tag datagram for the input row.
            input_data: The data datagram for the input row.
            recomputation_index: Position in the recomputation chain. ``0`` for
                the first computation, ``N+1`` for each miss-triggered recompute.

        Returns:
            Method-prefixed raw bytes (``b"{method}:{digest}"``) uniquely
            identifying this (tag, input_data, node run, recomputation attempt).
            Suitable for storage in a ``pa.large_binary()`` column.
        """
        preimage = self._build_entry_id_preimage(tag, input_data).append_column(
            _PIPELINE_RECOMPUTATION_INDEX_COL,
            pa.array([recomputation_index], type=pa.int32()),
        )
        return self.data_context.arrow_hasher.hash_table(preimage).to_prefixed_digest()

    def add_pipeline_record(
        self,
        tag: TagProtocol,
        input_data: DataProtocol,
        data_record_id: uuid.UUID,
        computed: bool,
        is_ephemeral: bool = False,
    ) -> None:
        """Add a pipeline record to the database for a processed data.

        Computes the next recomputation index by querying existing rows in the
        pipeline DB that share the same ``base_entry_id``, then writes at
        ``max_index + 1``. The write uses ``skip_duplicates=True`` so that
        concurrent asyncio coroutines competing for the same versioned entry ID
        are safely serialised: the first writer lands, subsequent writers whose
        compute happened to produce the same hash are silently no-oped.

        The pipeline record stores:

        - Tag columns (including system tags)
        - All source columns of the input data (provenance, not data values)
        - Output data record ID (for joining with result records)
        - Base entry ID (``_PIPELINE_BASE_ENTRY_ID_COL``)
        - Recomputation index (``_PIPELINE_RECOMPUTATION_INDEX_COL``)
        - Whether the result is stored in the ephemeral store
        - Input data context key
        - Whether the result was freshly computed or cached

        Args:
            tag: The tag associated with the input data.
            input_data: The input data that was processed.
            data_record_id: UUID of the result record in the result database.
            computed: Whether the result was freshly computed (``True``) or
                served from a cache (``False``).
            is_ephemeral: Whether the result is stored in the ephemeral store.
        """
        self._require_pipeline_database()
        base_entry_id = self.compute_base_entry_id(tag, input_data)

        # Guard against pre-ITL-508 pipeline DB records that are missing the new
        # versioning columns. If such records exist, fail fast with a clear message
        # rather than letting the subsequent filter crash with a cryptic Arrow error.
        _all_existing = self._pipeline_database.get_all_records(self.node_identity_path)
        if _all_existing is not None and _all_existing.num_rows > 0:
            _missing = [
                col
                for col in (_PIPELINE_BASE_ENTRY_ID_COL, _PIPELINE_RECOMPUTATION_INDEX_COL)
                if col not in _all_existing.schema.names
            ]
            if _missing:
                raise ValueError(
                    f"Pipeline database at {self.node_identity_path!r} contains records "
                    f"that are missing required ITL-508 columns: {_missing!r}. "
                    "Please clear or migrate the pipeline database before using this node."
                )

        # Determine the next recomputation index by querying all existing rows
        # for this base_entry_id. No await is used here, so within a single-threaded
        # asyncio event loop this read-then-write sequence is uninterrupted.
        existing = self._pipeline_database.get_records_with_column_value(
            self.node_identity_path,
            {_PIPELINE_BASE_ENTRY_ID_COL: base_entry_id},
        )
        if existing is None or existing.num_rows == 0:
            new_index = 0
        else:
            indices = existing.column(_PIPELINE_RECOMPUTATION_INDEX_COL).to_pylist()
            new_index = max(indices) + 1

        versioned_entry_id = self.compute_pipeline_entry_id(tag, input_data, new_index)

        # Extract source columns only (no data columns) from the input data
        input_table_with_source = input_data.as_table(columns={"source": True})
        source_col_names = [
            c
            for c in input_table_with_source.column_names
            if c.startswith(constants.SOURCE_PREFIX)
        ]
        input_source_table = input_table_with_source.select(source_col_names)

        # Build the meta columns table
        meta_table = pa.table(
            {
                constants.DATA_RECORD_ID: pa.array(
                    [data_record_id.bytes], type=pa.large_binary()
                ),
                constants.NODE_CONTENT_HASH_COL: pa.array(
                    [self.content_hash().to_string()], type=pa.large_string()
                ),
                f"{constants.META_PREFIX}input_data{constants.CONTEXT_KEY}": pa.array(
                    [input_data.data_context_key], type=pa.large_string()
                ),
                f"{constants.META_PREFIX}computed": pa.array(
                    [computed], type=pa.bool_()
                ),
                constants.IS_EPHEMERAL_COL: pa.array(
                    [is_ephemeral], type=pa.bool_()
                ),
                _PIPELINE_BASE_ENTRY_ID_COL: pa.array(
                    [base_entry_id], type=pa.large_binary()
                ),
                _PIPELINE_RECOMPUTATION_INDEX_COL: pa.array(
                    [new_index], type=pa.int32()
                ),
            }
        )

        # Combine: tag (with system tags) + input source columns + meta columns
        combined_record = arrow_utils.hstack_tables(
            tag.as_table(columns={"system_tags": True}),
            input_source_table,
            meta_table,
        )

        self._pipeline_database.add_record(
            self.node_identity_path,
            versioned_entry_id,
            combined_record,
            skip_duplicates=True,
        )

    # ------------------------------------------------------------------
    # Records and sources
    # ------------------------------------------------------------------

    def get_all_records(
        self,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> pa.Table | None:
        """Public table view: return all computed results joined with their pipeline records.

        Calls ``_fetch_joined_records`` to obtain the raw joined table, then
        applies ``ColumnConfig``-driven column dropping to produce a
        user-facing result. ``_PIPELINE_ENTRY_ID_COL`` and
        ``NODE_CONTENT_HASH_COL`` are always dropped — they are internal
        discriminator columns, not user-facing data.

        Does NOT populate the in-memory cache — see ``get_cached_results``
        for that.

        Args:
            columns: Column configuration controlling which groups are
                included. Accepts a ``ColumnConfig`` instance or a dict
                shorthand (e.g. ``{"meta": True}``).
            all_info: If ``True``, equivalent to enabling all column groups.

        Returns:
            A ``pa.Table`` of joined results, or ``None`` if no database is
            attached, either DB fetch returns ``None``, or the join produces
            no rows.
        """
        fetched = self._fetch_joined_records()
        if fetched is None:
            return None

        joined = fetched.table
        column_config = ColumnConfig.handle_config(columns, all_info=all_info)

        # Always drop internal discriminator columns regardless of column_config.
        # _PIPELINE_ENTRY_ID_COL starts with META_PREFIX so it is covered by
        # the meta drop in the default case, but must be listed explicitly
        # here so it is also dropped when all_info=True (which skips the
        # meta-prefix sweep).
        drop_columns = [
            constants.NODE_CONTENT_HASH_COL,
            _PIPELINE_ENTRY_ID_COL,
            _PIPELINE_BASE_ENTRY_ID_COL,
            _PIPELINE_RECOMPUTATION_INDEX_COL,
        ]
        if not column_config.meta and not column_config.all_info:
            drop_columns.extend(
                c for c in joined.column_names if c.startswith(constants.META_PREFIX)
            )
        if not column_config.source and not column_config.all_info:
            drop_columns.extend(
                c for c in joined.column_names if c.startswith(constants.SOURCE_PREFIX)
            )
        if not column_config.system_tags and not column_config.all_info:
            drop_columns.extend(
                c
                for c in joined.column_names
                if c.startswith(constants.SYSTEM_TAG_PREFIX)
            )
        if drop_columns:
            # pa.Table.drop() requires no duplicate names in its input list.
            # Guard against duplicates that arise when _PIPELINE_ENTRY_ID_COL (which
            # starts with META_PREFIX) ends up in both the always-drop seed and the
            # meta-prefix sweep extension.
            unique_drop = list(
                dict.fromkeys(c for c in drop_columns if c in joined.column_names)
            )
            if unique_drop:
                joined = joined.drop(unique_drop)

        return joined if joined.num_rows > 0 else None

    def as_source(self):
        """Return a DerivedSource backed by the DB records of this node.

        Raises:
            RuntimeError: If no database is attached.
        """
        if self._pipeline_database is None:
            raise RuntimeError("Cannot create a DerivedSource without a database")

        from orcapod.core.sources.derived_source import DerivedSource

        path_str = "/".join(self.node_identity_path)
        source_id = f"{path_str}:{self.content_hash().to_string()}"
        return DerivedSource(
            origin=self,
            source_id=source_id,
            data_context=self.data_context_key,
            config=self.orcapod_config,
        )

    # ------------------------------------------------------------------
    # Cache-only helpers (PLT-1156)
    # ------------------------------------------------------------------

    def _fetch_joined_records(
        self,
        base_entry_ids: list[bytes] | None = None,
    ) -> _JoinedRecords | None:
        """Internal primitive: fetch both DBs and inner-join, supporting two stores.

        Fetches ``taginfo`` from the pipeline database, partitions rows by
        ``IS_EPHEMERAL_COL`` into persistent and ephemeral groups, performs two
        independent inner joins (one per store), merges with persistent priority
        via an anti-join, and returns the combined result.

        Persistent miss rows (tag entry with no matching result DB row) emit a
        WARNING-level log. Ephemeral miss rows (cross-session miss) are silently
        dropped.

        If ``base_entry_ids`` is provided, the result is filtered to matching
        ``_PIPELINE_BASE_ENTRY_ID_COL`` values before conversion to Arrow.

        Args:
            base_entry_ids: If given, return only rows whose
                ``_PIPELINE_BASE_ENTRY_ID_COL`` value is in this list.
                If ``None``, return all rows.

        Returns:
            A ``_JoinedRecords`` whose ``table`` always includes a
            ``_PIPELINE_BASE_ENTRY_ID_COL`` column, or ``None`` when either
            the pipeline database or cached function pod is absent. A 0-row
            table (not ``None``) is returned when both fetches succeed but
            no matching rows exist — callers check ``num_rows`` themselves.
        """
        if self._cached_function_pod is None or self._pipeline_database is None:
            return None

        taginfo = self._pipeline_database.get_all_records(
            self.node_identity_path,
            record_id_column=_PIPELINE_ENTRY_ID_COL,
        )

        if taginfo is None:
            return None

        taginfo_columns = tuple(taginfo.column_names)
        taginfo = self._filter_by_content_hash(taginfo)
        taginfo_schema = taginfo.schema

        is_ephemeral_col = constants.IS_EPHEMERAL_COL
        taginfo_df = pl.DataFrame(taginfo)

        # Partition by IS_EPHEMERAL_COL (backward-compat: missing col → all persistent)
        if is_ephemeral_col in taginfo.column_names:
            persistent_taginfo_df = taginfo_df.filter(
                ~pl.col(is_ephemeral_col).fill_null(False)
            )
            ephemeral_taginfo_df = taginfo_df.filter(
                pl.col(is_ephemeral_col).fill_null(False)
            )
        else:
            persistent_taginfo_df = taginfo_df
            ephemeral_taginfo_df = pl.DataFrame()

        # ------------------------------------------------------------------
        # Persistent join
        # ------------------------------------------------------------------
        results_schema = None
        persistent_df = pl.DataFrame()
        if persistent_taginfo_df.height > 0:
            results = self._cached_function_pod.result_database.get_all_records(
                self._cached_function_pod.record_path,
                record_id_column=constants.DATA_RECORD_ID,
            )
            if results is None:
                # Tag table has persistent entries but result DB is empty — data loss
                logger.warning(
                    "%d pipeline DB entries have no match in persistent result DB "
                    "— data may have been deleted externally. "
                    "These inputs will be recomputed.",
                    persistent_taginfo_df.height,
                )
            else:
                results_schema = results.schema
                full_persistent_df = persistent_taginfo_df.join(
                    pl.DataFrame(results),
                    on=constants.DATA_RECORD_ID,
                    how="inner",
                )
                # Warn about persistent tag rows that found no match in the result DB
                missing_count = persistent_taginfo_df.height - full_persistent_df.height
                if missing_count > 0:
                    logger.warning(
                        "%d pipeline DB entries have no match in persistent result DB "
                        "— data may have been deleted externally. "
                        "These inputs will be recomputed.",
                        missing_count,
                    )
                persistent_df = full_persistent_df

        # ------------------------------------------------------------------
        # Ephemeral join
        # ------------------------------------------------------------------
        ephemeral_df = pl.DataFrame()
        if ephemeral_taginfo_df.height > 0 and self._ephemeral_cached_pod is not None:
            eph_results = self._ephemeral_cached_pod.result_database.get_all_records(
                self._ephemeral_cached_pod.record_path,
                record_id_column=constants.DATA_RECORD_ID,
            )
            if eph_results is not None:
                if results_schema is None:
                    results_schema = eph_results.schema
                ephemeral_df = ephemeral_taginfo_df.join(
                    pl.DataFrame(eph_results),
                    on=constants.DATA_RECORD_ID,
                    how="inner",
                )
            # Cross-session miss: eph_results is None → silently drop ephemeral entries

        # ------------------------------------------------------------------
        # Merge with persistent priority (anti-join + concat)
        # ------------------------------------------------------------------
        if ephemeral_df.height > 0 and persistent_df.height > 0:
            ephemeral_only_df = ephemeral_df.join(
                persistent_df.select([_PIPELINE_BASE_ENTRY_ID_COL]),
                on=_PIPELINE_BASE_ENTRY_ID_COL,
                how="anti",
            )
            merged_df = pl.concat([persistent_df, ephemeral_only_df], how="diagonal")
        elif ephemeral_df.height > 0:
            merged_df = ephemeral_df
        elif persistent_df.height > 0:
            merged_df = persistent_df
        else:
            # No results found in either store — return empty table preserving taginfo schema
            empty_table = taginfo.slice(0, 0)
            return _JoinedRecords(table=empty_table, taginfo_columns=taginfo_columns)

        # Apply base_entry_id filter if requested
        if base_entry_ids is not None:
            merged_df = merged_df.filter(
                pl.col(_PIPELINE_BASE_ENTRY_ID_COL).is_in(base_entry_ids)
            )

        joined = merged_df.to_arrow()
        if results_schema is not None:
            joined = arrow_utils.restore_schema_nullability(
                joined, taginfo_schema, results_schema
            )
        return _JoinedRecords(table=joined, taginfo_columns=taginfo_columns)

    def _load_cached_entries(
        self,
        base_entry_ids: list[bytes] | None = None,
    ) -> "dict[bytes, tuple[TagProtocol, DataProtocol]]":
        """DB loader: fetch ``(tag, data)`` pairs from the pipeline and result databases.

        Calls ``_fetch_joined_records`` to obtain the raw joined table, then
        converts each row into a ``(tag, data)`` tuple keyed by base entry ID.

        If ``base_entry_ids`` is given, only those entries are fetched from DB.
        If ``None``, all records for this node are loaded.

        Does NOT read from or write to the in-memory cache
        (``_cached_output_datas``). Callers that want to populate the cache
        must call ``self._cached_output_datas.update(loaded)`` themselves.

        Does NOT apply user-facing column filtering — see ``get_all_records``
        for that.

        Args:
            base_entry_ids: If provided, load only these specific base entry IDs.
                If ``None``, load all records for this node.

        Returns:
            dict mapping base_entry_id → ``(tag, data)``. Empty dict when either
            database is absent, either DB fetch returns ``None``, or no rows
            match after joining.
        """
        fetched = self._fetch_joined_records(base_entry_ids=base_entry_ids)
        if fetched is None or fetched.table.num_rows == 0:
            return {}

        joined = fetched.table

        # Derive tag keys: prefer input_stream when available; fall back to
        # taginfo column exclusion for CACHE_ONLY / deserialized nodes.
        # taginfo_columns from _fetch_joined_records preserves the pipeline DB
        # column names before joining, which is the correct exclusion set.
        if self._input_stream is not None:
            tag_keys = self._input_stream.keys()[0]
        else:
            tag_keys = tuple(
                c
                for c in fetched.taginfo_columns
                if not c.startswith(constants.META_PREFIX)
                and not c.startswith(constants.SOURCE_PREFIX)
                and not c.startswith(constants.SYSTEM_TAG_PREFIX)
                and c != _PIPELINE_ENTRY_ID_COL
                and c != constants.NODE_CONTENT_HASH_COL
            )

        # Drop internal columns (SOURCE_PREFIX is kept — ArrowTableStream needs it).
        # All meta columns (starting with META_PREFIX "__") are dropped including
        # _PIPELINE_ENTRY_ID_COL and _PIPELINE_BASE_ENTRY_ID_COL.
        base_entry_ids_col = joined.column(_PIPELINE_BASE_ENTRY_ID_COL).to_pylist()
        drop_cols = [
            c
            for c in joined.column_names
            if c.startswith(constants.META_PREFIX)
            or c == constants.NODE_CONTENT_HASH_COL
        ]
        data_table = joined.drop([c for c in drop_cols if c in joined.column_names])
        stream = ArrowTableStream(data_table, tag_columns=tag_keys)

        loaded: dict[bytes, tuple[TagProtocol, DataProtocol]] = {}
        for base_eid, (tag, data) in zip(base_entry_ids_col, stream.iter_data()):
            loaded[base_eid] = (tag, data)
        return loaded

    async def _async_execute_cache_only(
        self,
        output: "WritableChannel[tuple[TagProtocol, DataProtocol]]",
        *,
        observer: Any | None = None,
    ) -> None:
        """Send all DB-cached (tag, data) pairs to *output*.

        Used in ``CACHE_ONLY`` mode when the upstream is unavailable.
        Does not access ``_input_stream``.
        """
        from orcapod.pipeline.observer import NoOpObserver

        obs = observer if observer is not None else NoOpObserver()
        node_label = self.label
        node_hash = self.content_hash().to_string()
        ctx_obs = obs.contextualize(*self.node_identity_path)

        ctx_obs.on_node_start(node_label, node_hash, tag_schema=None)
        try:
            loaded = self._load_cached_entries()
            self._cached_output_datas.update(loaded)
            if loaded:
                self._cached_output_table = None
                self._cached_content_hash_column = None

            for tag, data in self._cached_output_datas.values():
                if data is not None:
                    ctx_obs.on_data_start(node_label, tag, data)
                    ctx_obs.on_data_end(node_label, tag, data, data, cached=True)
                    await output.send((tag, data))
            ctx_obs.on_node_end(node_label, node_hash)
        finally:
            await output.close()

    # ------------------------------------------------------------------
    # Iteration
    # ------------------------------------------------------------------

    def iter_data(self) -> Iterator[tuple[TagProtocol, DataProtocol]]:
        """Yield all computed (tag, data) pairs for this node.

        Strictly read-only — never triggers computation. Callers must call
        ``run()`` or ``execute()`` first if they want results computed.

        On the first call with an empty in-memory store and a DB attached,
        hot-loads all existing records from the DB (one-shot, no recompute).

        Raises:
            RuntimeError: If ``load_status`` is UNAVAILABLE.
        """
        from orcapod.pipeline.serialization import LoadStatus

        status = self.load_status
        if status == LoadStatus.UNAVAILABLE:
            raise RuntimeError(
                f"FunctionJobNode {self.label!r} is unavailable: "
                "no function pod and no database attached."
            )

        if status == LoadStatus.CACHE_ONLY:
            # Upstream unavailable; serve entirely from DB.
            if not self._cached_output_datas:
                loaded = self._load_cached_entries()
                self._cached_output_datas.update(loaded)
                if loaded:
                    self._cached_output_table = None
                    self._cached_content_hash_column = None
            yield from (
                (tag, pkt)
                for tag, pkt in self._cached_output_datas.values()
                if pkt is not None
            )
            return

        # FULL / READ_ONLY — in-memory store may be populated from computation
        # (via execute/run) or hot-loaded from DB.
        if self.is_stale:
            self.clear_cache()

        if not self._cached_output_datas and self._cached_function_pod is not None:
            # Hot-load from DB on the first call when store is empty.
            loaded = self._load_cached_entries()
            self._cached_output_datas.update(loaded)
            if loaded:
                self._cached_output_table = None
                self._cached_content_hash_column = None

        yield from (
            (tag, pkt)
            for tag, pkt in self._cached_output_datas.values()
            if pkt is not None
        )

    def run(self) -> None:
        """Eagerly compute all input data, filling pipeline and result databases.

        Raises:
            RuntimeError: If ``load_status`` is UNAVAILABLE (no pod, no DB).
        """
        from orcapod.pipeline.serialization import LoadStatus

        if self._load_status == LoadStatus.UNAVAILABLE:
            raise RuntimeError(
                f"FunctionJobNode {self.label!r} is unavailable: "
                "no function pod and no database attached."
            )
        if self._load_status in (LoadStatus.CACHE_ONLY, LoadStatus.READ_ONLY):
            # CACHE_ONLY: upstream unavailable; computation requires a live input stream.
            # READ_ONLY: function pod is a proxy placeholder — cannot compute.
            # Callers should use iter_data() to serve existing DB results.
            return
        if self.is_stale:
            # Discard any stale in-memory entries before a fresh computation run
            # so that rerunning does not mix old cached entries with new results.
            self.clear_cache()
        self.execute(self._input_stream)

    # ------------------------------------------------------------------
    # Async channel execution
    # ------------------------------------------------------------------

    async def async_execute(
        self,
        input_channel: ReadableChannel[tuple[TagProtocol, DataProtocol]],
        output: WritableChannel[tuple[TagProtocol, DataProtocol]],
        *,
        observer: ExecutionObserverProtocol | None = None,
    ) -> None:
        """Streaming async execution for FunctionJobNode.

        When a database is attached, uses two-phase execution: replay cached
        results first, then compute missing data concurrently.  Otherwise,
        routes each data through ``async_process_data`` directly.

        In ``CACHE_ONLY`` mode the upstream is unavailable; all cached results
        are served directly from persistent storage without touching the input
        channel.

        Args:
            input_channel: Single readable channel of (tag, data) pairs.
            output: Writable channel for output (tag, data) pairs.
            observer: Optional execution observer for hooks.
        """
        from orcapod.pipeline.serialization import LoadStatus

        status = self.load_status
        if status == LoadStatus.CACHE_ONLY:
            await self._async_execute_cache_only(output, observer=observer)
            return

        if status == LoadStatus.UNAVAILABLE:
            await output.close()
            raise RuntimeError(
                f"FunctionJobNode {self.label!r} is unavailable: "
                "no function pod and no database attached."
            )

        from orcapod.pipeline.observer import NoOpObserver

        node_label = self.label
        node_hash = self.content_hash().to_string()

        obs = observer if observer is not None else NoOpObserver()
        ctx_obs = obs.contextualize(*self.node_identity_path)

        try:
            tag_schema = self._input_stream.output_schema(columns={"system_tags": True})[0]
            ctx_obs.on_node_start(node_label, node_hash, tag_schema=tag_schema)

            if self._cached_function_pod is not None:
                # ----------------------------------------------------------
                # DB path — 3-stage concurrent pipeline
                # ----------------------------------------------------------
                is_ephemeral = bool(self._node_config.is_result_ephemeral)
                if is_ephemeral:
                    if self._ephemeral_cached_pod is None:
                        raise RuntimeError(
                            f"FunctionJobNode '{self.label}' has is_result_ephemeral=True "
                            "but no ephemeral store has been assigned. Call "
                            "set_ephemeral_store() with an ArrowDatabaseProtocol before "
                            "executing this node."
                        )
                    execution_pod = self._ephemeral_cached_pod
                else:
                    execution_pod = self._cached_function_pod

                # Load pipeline DB → in-memory cache keyed by base_entry_id.
                loaded = self._load_cached_entries()
                self._cached_output_datas.update(loaded)
                if loaded:
                    self._cached_output_table = None
                    self._cached_content_hash_column = None
                cached_by_base_entry_id: dict[bytes, tuple[TagProtocol, DataProtocol]] = dict(loaded)

                # Intermediate channels (bounded for backpressure).
                compute_channel: Channel[tuple[TagProtocol, DataProtocol]] = Channel(buffer_size=16)
                result_channel: Channel[tuple[TagProtocol, DataProtocol]] = Channel(buffer_size=16)

                # Local dict: correlation_key → (original_tag, original_input_data)
                input_store: dict[bytes, tuple[TagProtocol, DataProtocol]] = {}

                async def route_inputs() -> None:
                    """Stage 1: send cache hits to output; stamp misses for computation."""
                    try:
                        async for tag, data in input_channel:
                            base_entry_id = self.compute_base_entry_id(tag, data)
                            if base_entry_id in cached_by_base_entry_id:
                                cached_tag, cached_data = cached_by_base_entry_id[base_entry_id]
                                ctx_obs.on_data_start(node_label, tag, data)
                                ctx_obs.on_data_end(
                                    node_label, tag, data, cached_data, cached=True
                                )
                                await output.send((cached_tag, cached_data))
                            else:
                                correlation_key = uuid.uuid4().bytes
                                input_store[correlation_key] = (tag, data)
                                stamped_tag = tag.with_meta_columns(
                                    **{_TAG_NODE_INPUT_REF: correlation_key}
                                )
                                await compute_channel.writer.send((stamped_tag, data))
                    finally:
                        await compute_channel.writer.close()

                async def record_and_forward() -> None:
                    """Stage 3: record pipeline entry, strip key, emit to output."""
                    async for output_tag, output_data in result_channel.reader:
                        correlation_key = output_tag.get_meta_value(_TAG_NODE_INPUT_REF)
                        original_tag, input_data = input_store.pop(correlation_key)
                        result_computed = bool(
                            output_data.get_meta_value(
                                execution_pod.RESULT_COMPUTED_FLAG, False
                            )
                        )
                        self.add_pipeline_record(
                            original_tag,
                            input_data,
                            data_record_id=output_data.datagram_uuid,
                            computed=result_computed,
                            is_ephemeral=is_ephemeral,
                        )
                        # Update in-memory cache so iter_data() sees the result.
                        base_entry_id = self.compute_base_entry_id(original_tag, input_data)
                        clean_tag = output_tag.drop_meta_columns(_TAG_NODE_INPUT_REF)
                        self._cached_output_datas[base_entry_id] = (clean_tag, output_data)
                        self._cached_output_table = None
                        self._cached_content_hash_column = None
                        await output.send((clean_tag, output_data))

                # Wrap ctx_obs so that data-level events emitted by the pod
                # carry the node label, not the pod's own label — preserving
                # the observable contract of the old DB path.
                class _NodeLabelObserver:
                    """Relay all observer calls, replacing any label with node_label."""

                    def contextualize(self, *path: str) -> "_NodeLabelObserver":
                        return self

                    def on_node_start(self, lbl: str, h: str, **kw: Any) -> None:
                        ctx_obs.on_node_start(node_label, h, **kw)

                    def on_node_end(self, lbl: str, h: str) -> None:
                        ctx_obs.on_node_end(node_label, h)

                    def on_data_start(self, lbl: str, tag: Any, data: Any) -> None:
                        ctx_obs.on_data_start(node_label, tag, data)

                    def on_data_end(
                        self, lbl: str, tag: Any, inp: Any, out: Any, *, cached: bool
                    ) -> None:
                        ctx_obs.on_data_end(node_label, tag, inp, out, cached=cached)

                    def on_data_crash(
                        self, lbl: str, tag: Any, data: Any, error: Any
                    ) -> None:
                        ctx_obs.on_data_crash(node_label, tag, data, error)

                    def create_data_logger(self, tag: Any, data: Any) -> Any:
                        # Strip the correlation key before creating the logger
                        # so that internal columns never leak into log records.
                        clean = tag.drop_meta_columns(
                            _TAG_NODE_INPUT_REF, ignore_missing=True
                        )
                        return ctx_obs.create_data_logger(clean, data)

                async with asyncio.TaskGroup() as tg:
                    tg.create_task(route_inputs())
                    tg.create_task(
                        execution_pod.async_execute(
                            [compute_channel.reader],
                            result_channel.writer,
                            observer=_NodeLabelObserver(),
                        )
                    )
                    tg.create_task(record_and_forward())
            else:
                # No-DB path: delegate dispatch entirely to the pod.
                # The pod's async_execute() closes `output` in its own finally.
                await self._function_pod.async_execute(
                    [input_channel], output, observer=ctx_obs
                )

            ctx_obs.on_node_end(node_label, node_hash)
        finally:
            await output.close()

