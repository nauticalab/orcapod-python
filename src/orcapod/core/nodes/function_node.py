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
import contextlib
import dataclasses
import logging
import sys
import uuid
from abc import abstractmethod
from collections import Counter
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from typing import TYPE_CHECKING, Any, Literal, NamedTuple, cast

from orcapod import contexts
from orcapod.channels import Channel, ReadableChannel, WritableChannel
from orcapod.config import OrcapodConfig
from orcapod.core.cached_function_pod import CachedFunctionPod
from orcapod.core.streams.arrow_table_stream import ArrowTableStream
from orcapod.core.streams.base import (
    ZERO_ROW_INPUT_COLUMNS,
    StreamBase,
    check_head_size,
    short_stream_repr,
)
from orcapod.core.streams.view import (
    BLUEPRINT_NOTE,
    JOB_NODE_NOT_RUN_NOTE,
    STALE_NOTE,
    ViewStatus,
    StreamView,
)
from orcapod.core.tracker import DEFAULT_TRACKER_MANAGER
from orcapod.core.nodes.staleness import RunHoldingNode
from orcapod.core.datagrams.tag_data import Data, EmptyData, Tag
from orcapod.errors import (
    EphemeralResultMissingError,
    InputValidationError,
    PipelineJobRequiredError,
    OrcapodDatabaseError,
    TypeIdentityError,
    OrcapodDatabaseReadError,
    OrcapodDatabaseWriteError,
)
from orcapod.contexts.stored import (
    StaleTables,
    keep_rows_of_data_context,
    require_same_data_context,
    account_for_stale_tables,
    describe_database_table,
    find_stale_tables,
    report_rows_of_other_data_contexts,
    report_unexpected_row_count,
)
from orcapod.utils.database_errors import (
    commit_best_effort,
    keep_database_error,
    database_error_of,
)
from orcapod.protocols.core_protocols import (
    FunctionPodProtocol,
    DataFunctionExecutorProtocol,
    DataFunctionProtocol,
    DataProtocol,
    StreamProtocol,
    TagProtocol,
    TrackerManagerProtocol,
)
from orcapod.databases.utils import as_table_committing
from orcapod.protocols.database_protocols import ArrowDatabaseProtocol
from orcapod.protocols.observability_protocols import (
    DataExecutionLoggerProtocol,
    ExecutionObserverProtocol,
)
from orcapod.system_constants import (
    constants,
    OLD_PIPELINE_DB_SCHEMA_VERSIONS,
    PIPELINE_DB_SCHEMA_VERSION,
    RESULT_DB_SCHEMA_VERSION,
)
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
else:
    pa = LazyModule("pyarrow")
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


def _build_record_id_preimage(
    tag: "TagProtocol",
    input_data: "DataProtocol",
) -> "pa.Table":
    """Build the Arrow preimage table for record_id / base_entry_id computation.

    Combines the tag's system-tag columns with the binary input data hash into a
    single-row Arrow table.  ``NODE_CONTENT_HASH_COL`` is intentionally excluded —
    it is redundant (fully determined by the table path + system tags).

    The input data hash enters with its label, ``b"<label>:" + digest``, the form
    the stored ``INPUT_DATA_HASH_COL`` column of the pipeline and result tables
    keeps: a hash fed into another hash carries the name of the algorithm that
    produced it (ITL-1154).

    Args:
        tag: The tag datagram for the input row.
        input_data: The data datagram for the input row.

    Returns:
        A single-row ``pa.Table`` with system-tag columns and
        ``INPUT_DATA_HASH_COL`` (``large_binary``, the prefixed digest).
    """
    return tag.as_table(columns={"system_tags": True}).append_column(
        constants.INPUT_DATA_HASH_COL,
        pa.array(
            [input_data.content_hash().to_prefixed_digest()], type=pa.large_binary()
        ),
    )


def _large_layout(table: "pa.Table") -> "pa.Table":
    """Return ``table`` with every column in its large layout, ``null`` kept ``null``.

    Args:
        table: The table to normalize.

    Returns:
        The table with large-normalized column types; names, nullability and
        metadata kept.
    """
    schema = pa.schema(
        [
            field.with_type(
                arrow_utils.normalize_to_large_types(field.type, keep_null=True)
            )
            for field in table.schema
        ]
    )
    return table if schema.equals(table.schema) else table.cast(schema)


def _concat_row_tables(tables: "list[pa.Table]") -> "pa.Table":
    """Concatenate one-row tables by column name, refusing a column whose types differ.

    Rows never make a column of a type one of them does not have: two rows whose
    Arrow types for one column differ (other than by their large variants) are an
    error, never widened or cast.

    Args:
        tables: The rows' tables, in row order; not empty.

    Returns:
        The concatenated table, with the first table's column order.

    Raises:
        InputValidationError: If two rows give one column different Arrow types,
            naming the column and both types.
    """
    # Large and non-large layouts of one type are one type: give every row the
    # large layout, so they also concatenate. Layouts only: ``null`` stays ``null``.
    tables = [_large_layout(t) for t in tables]
    # The first type seen for each column, whichever row first has the column.
    seen: dict[str, pa.DataType] = {}
    for table in tables:
        for name, row_type in zip(table.schema.names, table.schema.types):
            expected = seen.setdefault(name, row_type)
            if arrow_utils.shared_arrow_type(expected, row_type) is None:
                message = f"Rows disagree on the Arrow type of column {name!r}: {expected} and {row_type}"
                raise InputValidationError(message)
    return arrow_utils.concat_tables_by_name(tables)


def _bare_digest(hash_string: str) -> str:
    """Return the bare hex digest of a stored ``"<label>:<hex>"`` hash string.

    Args:
        hash_string: A hash as stored in a saved job, or ``""`` when absent.

    Returns:
        The hex digest without its label, or ``""`` for an empty string.
    """
    if not hash_string:
        return ""
    return ContentHash.from_string(hash_string).to_hex()


# Private meta-column name stamped into tags routed to the compute channel.
# Carries the correlation key (bytes) that links an execution result back to
# its original (tag, input_data) pair. Stripped from output tags in
# record_and_forward() before downstream emission.
_TAG_NODE_INPUT_REF = "_tag_node_input_ref"


def _with_datagram_id(data: Data, datagram_id: uuid.UUID) -> Data:
    """Return ``data`` rebuilt with ``datagram_id`` as its datagram id.

    A stored row read back is built by a stream, which gives every row a fresh id.
    The rebuilt datagram has the same columns, types, source info and data context.

    Args:
        data: The datagram a stream built from a stored row.
        datagram_id: The id the row was stored under.

    Returns:
        A datagram equal to ``data`` whose ``datagram_uuid`` is ``datagram_id``.
    """
    return Data(
        data.as_table(columns={"source": True}),
        record_uuid=datagram_id,
        data_context=data.data_context_key,
    )


def _executor_supports_concurrent(
    data_function: DataFunctionProtocol,
) -> bool:
    """Return True if the data function's executor supports concurrent execution."""
    executor = data_function.executor
    return executor is not None and executor.supports_concurrent_execution


class _CloseDeferredWriter:
    """A writer that forwards rows to ``output`` and leaves closing it to its owner.

    A node hands it to a pod that closes the writer it is given, when the node must
    still do something (stamp its modified time) before its downstream nodes see the
    end. The node closes ``output`` itself afterwards.
    """

    def __init__(self, output: WritableChannel[tuple[TagProtocol, DataProtocol]]):
        """Wrap ``output``."""
        self._output = output

    async def send(self, item: tuple[TagProtocol, DataProtocol]) -> None:
        """Send ``item`` to the wrapped output."""
        await self._output.send(item)

    async def close(self) -> None:
        """Do nothing: the owner closes the wrapped output."""


# ---------------------------------------------------------------------------
# FunctionNodeBase — shared base (no DB)
# ---------------------------------------------------------------------------


def run_job_node(
    node: Any,
    *,
    load_only: bool,
    run_alone: Callable[[tuple[str, ...]], Any],
    caller: Any,
) -> None:
    """Run a job node as its ``run()`` does (developer, November 13, 2026; ITL-1095).

    Every source feeding the node is first checked for new data (``check_sources``,
    ITL-1102). Nothing runs when the node then holds a current run
    (``holds_current_run``). Otherwise a node that depends on job nodes runs with them
    (``run_with_dependencies``) and one built over streams outside a job runs alone
    (``run_alone``). The run's summary, if
    it gives one, is kept as the node's ``last_run``, and failed rows warn at the
    caller's line, as ``job.run()`` warns.

    Args:
        node: The job node (a ``RunHoldingNode``).
        load_only: Whether to load stored results only.
        run_alone: Runs the node over its input when no job node feeds it, given the
            sources the check could not check; returns its ``RunSummary`` or ``None``.
        caller: The frame of the ``run()`` caller.
    """
    from orcapod.pipeline.node_run import (
        check_sources,
        depends_on_job_nodes,
        refuse_unloadable_inputs,
        run_with_dependencies,
    )
    from orcapod.pipeline.run_summary import warn_row_failures

    # Every source feeding the run first takes in any new data (ITL-1102).
    unchecked = check_sources([node])
    if node.holds_current_run(load_only=load_only):
        return
    if depends_on_job_nodes(node):
        summary = run_with_dependencies(
            node, load_only=load_only, unchecked_sources=unchecked
        )
    else:
        if load_only:
            refuse_unloadable_inputs(node)
        summary = run_alone(unchecked)
    if summary is None:
        return
    node._last_run = summary
    if not summary.ok:
        warn_row_failures(summary, caller)


class FunctionNodeBase(StreamBase):
    """Shared base for ``FunctionNode`` and ``FunctionJobNode``.

    Carries all non-DB state: identity, schema, upstreams, and properties
    shared by both the blueprint and the execution variant.
    """

    # Set by each subclass's ``__init__``: the output table and content hashes that
    # ``as_table`` builds once.
    _cached_output_table: "pa.Table | None"
    _cached_content_hash_column: "pa.Array[Any] | None"

    def __init__(
        self,
        function_pod: FunctionPodProtocol,
        input_stream: StreamProtocol,
        tracker_manager: TrackerManagerProtocol | None = None,
        label: str | None = None,
        config: OrcapodConfig | None = None,
        table_scope: Literal["pipeline_hash", "content_hash"] = "pipeline_hash",
    ):
        # Derive node_type from the pod: "side_effect_function" when ctx_arg_name is set
        self._node_type: str = (
            "side_effect_function"
            if function_pod.ctx_arg_name is not None
            else "function"
        )
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
            incoming_tag_types, incoming_data_types = input_stream.output_schema()
            # Use the pod's exposed input schema (ctx excluded) rather than the
            # data function's full schema, because ctx is auto-injected per row.
            expected_data_schema = function_pod.input_data_schema
            reason = schema_utils.schema_incompatibility(
                incoming_data_types, expected_data_schema
            )
            if reason is not None:
                raise ValueError(
                    f"Incoming data schema {incoming_data_types} from {input_stream} "
                    f"is not compatible with expected input schema "
                    f"{expected_data_schema}: {reason}"
                )

            schema_utils.require_output_keys_not_tags(
                incoming_tag_types.keys(),
                function_pod.data_function.output_data_schema.keys(),
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
    def node_type(self) -> str:
        """Return ``"side_effect_function"`` when the pod injects ``InvocationContext``, else ``"function"``."""
        return self._node_type

    @property
    def producer(self) -> FunctionPodProtocol:
        return self._function_pod

    @property
    def data_context(self) -> contexts.DataContext:
        return contexts.resolve_context(self.data_context_key)

    @property
    def data_context_key(self) -> str:
        """Key of the node's data context.

        A node loaded from a saved job without its function pod (read-only) has
        the data context saved in its descriptor.
        """
        if self._function_pod is None:
            return self._data_context.context_key
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
        self._forget_output_schema()

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
        """Return output schema, using stored value in read-only mode.

        Raises:
            ValueError: If the node was loaded and a column's saved type cannot be
                restored to a Python type (an unregistered logical type).
        """
        if self._function_pod is None:
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
        # Memoized (ITL-1111). The prediction depends on the data function's declared
        # output types and on the input stream's schema; the ``upstreams`` setter and a
        # job's ``bind`` clear it when the input changes. Never on rows.
        return self._memoized_output_schema(
            columns,
            all_info,
            lambda: (
                self._input_stream.output_schema(columns=columns, all_info=all_info)[0],
                self._data_function.output_data_schema,
            ),
        )

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
        tag_schema, data_schema = self.output_schema(columns=columns, all_info=all_info)
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
        # Hashes inside a path are bare hex digests: a label never enters a name.
        path = pf.uri + (f"schema:{self.pipeline_hash().to_hex()}",)
        if self._table_scope != "pipeline_hash":
            path += (f"instance:{self.content_hash().to_hex()}",)
        self._node_identity_path_cache = path
        return path

    @property
    def node_name_path(self) -> tuple[str, ...]:
        """The beginning of ``node_identity_path`` up to the function's name.

        ``(<function name>,)``, or ``("side_effect_function", <function name>)`` for
        a pod that takes the invocation context. Unlike the rest of the path it
        holds no hash, so it is the same in every version of Orcapod; tables an
        earlier version wrote for this function are found under it (ITL-954).
        """
        depth = 2 if self.node_type == "side_effect_function" else 1
        return self.node_identity_path[:depth]

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
        # A stale cache is cleared before any of it is used: the table and the content
        # hashes are then built from the same rows, and they are the current ones
        # (ITL-1045). ``iter_data`` clears a stale cache too, but only after the
        # cached table would already have been read.
        if self._read_finds_stale():
            self.clear_cache()
        if self._cached_output_table is None:
            self._cached_output_table = self._table_from_pairs(list(self.iter_data()))

        column_config = ColumnConfig.handle_config(columns, all_info=all_info)

        def all_content_hashes() -> "pa.Array[Any]":
            if self._cached_content_hash_column is None:
                self._cached_content_hash_column = pa.array(
                    [data.content_hash().to_string() for _, data in self.iter_data()],
                    type=pa.large_string(),
                )
            return self._cached_content_hash_column

        return self._shape_table(
            self._cached_output_table, column_config, all_content_hashes
        )

    def _read_finds_stale(self) -> bool:
        """Whether a read finds the rows this node holds out of date."""
        return self.is_stale

    def _zero_row_output_table(self) -> "pa.Table | None":
        """This node's rows' table with no row; see ``function_zero_row_table``."""
        from orcapod.core.function_pod import function_zero_row_table

        return function_zero_row_table(
            self._input_stream, self.output_schema()[1], self.data_context
        )

    def _typed_zero_row_table(self) -> "pa.Table | None":
        """This node's zero-row table, without calling the function (ITL-912)."""
        return self._zero_row_table(ZERO_ROW_INPUT_COLUMNS)

    def _zero_row_table(self, config: ColumnConfig) -> "pa.Table | None":
        """``as_table(columns=config)`` with no rows, shaped as ``as_table`` shapes it."""
        table = self._zero_row_output_table()
        if table is None:
            return None
        return self._shape_table(
            table, config, lambda: pa.array([], type=pa.large_string())
        )

    def _table_from_pairs(
        self, pairs: "list[tuple[TagProtocol, DataProtocol]]"
    ) -> "pa.Table":
        """Return the output rows ``pairs`` as one table with every column group.

        Built from each row's own Arrow tables, so every column keeps the type its
        row has: a tag the type of the input's tag, never a type inferred from
        Python values (ITL-928). A row's meta columns (a computed row's
        ``__computed`` flag) are its own bookkeeping, not part of the node's
        output, as for the pod's stream outside a job. With no rows the table is
        ``function_zero_row_table``: the input's tags and system tags and the
        declared data, context and source-info columns, typed as computed rows are
        (ITL-912); only when the input cannot say its Arrow types without rows is
        it built from the declared Python schema.

        Raises:
            InputValidationError: If two rows give one column different Arrow
                types, naming this node, the column and both types.
        """
        if not pairs:
            # No data was produced (node has not been run yet or produced zero
            # outputs): the table computed rows would have, with no row (ITL-912).
            typed = self._zero_row_output_table()
            if typed is not None:
                return typed
            # The input cannot say its Arrow types without rows: the declared schema.
            tag_python_schema, data_python_schema = self.output_schema(all_info=True)
            return pa.Table.from_pylist(
                [],
                schema=self.data_context.type_converter.python_schema_to_arrow_schema(
                    tag_python_schema + data_python_schema
                ),
            )
        try:
            tag_table = _concat_row_tables(
                [tag.as_table(columns={"system_tags": True}) for tag, _ in pairs]
            )
            data_table = _concat_row_tables(
                [
                    data.as_table(columns={"source": True, "context": True})
                    for _, data in pairs
                ]
            )
        except InputValidationError as exc:
            raise InputValidationError(f"Function node {self.label!r}: {exc}") from exc
        return arrow_utils.hstack_tables(tag_table, data_table)

    def _shape_table(
        self,
        table: "pa.Table",
        column_config: ColumnConfig,
        content_hashes: "Callable[[], pa.Array[Any]]",
    ) -> "pa.Table":
        """Return ``table`` with the column groups of ``column_config``.

        The content hashes are attached before the rows are sorted by tag
        (``sort_by_tags``), so every row keeps its own hash (ITL-1029).

        Args:
            table: Output rows with every column group (``_table_from_pairs``).
            column_config: The column groups to keep.
            content_hashes: Returns the content hash of each row of ``table``, in the
                order of ``table``; called only when ``column_config`` asks for them.
        """
        tag_keys = self.keys()[0]
        # The sort sees the hash column and the system tags (they break ties).
        return arrow_utils.with_content_hash_column(
            table,
            column_config.content_hash,
            content_hashes,
            lambda carried: arrow_utils.apply_column_config(
                carried, column_config, tag_keys
            ),
        )

    def head(
        self,
        n: int = 10,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> StreamView:
        """Return no rows: a node of this kind holds no data. See ``StreamBase.head``."""
        check_head_size(n)
        return self._head_without_rows(
            ColumnConfig.handle_config(columns, all_info=all_info),
            ViewStatus.UNAVAILABLE,
            note=BLUEPRINT_NOTE,
        )

    def _head_of_pairs(
        self,
        pairs: "Sequence[tuple[TagProtocol, DataProtocol | None]]",
        n: int,
        config: ColumnConfig,
        origin: str,
    ) -> StreamView:
        """Return the first ``n`` of ``pairs`` as this node's head.

        A ``None`` output (a filtered input) is not a row. An ``EmptyData`` placeholder
        (a result kept in an ephemeral database that is gone) has no values to show: it is
        left out and counted in the note.
        """
        rows = [
            (t, d) for t, d in pairs if d is not None and not isinstance(d, EmptyData)
        ]
        missing = sum(1 for _, d in pairs if isinstance(d, EmptyData))
        # With sorting, every row is shaped and sorted (each keeps its own content
        # hash), then the first n are taken: the head is ``as_table(config)[:n]``.
        # At least one row is shaped even for n == 0, so that the empty head has the
        # columns of the rows' own table (the context column among them).
        shown = rows if config.sort_by_tags else rows[: max(n, 1)]
        table = self._shape_table(
            self._table_from_pairs(shown),
            config,
            lambda: pa.array(
                [data.content_hash().to_string() for _, data in shown],
                type=pa.large_string(),
            ),
        ).slice(0, n)
        note = None
        if missing:
            note = (
                f"{missing:,} {'result' if missing == 1 else 'results'} not shown: "
                "kept in an ephemeral database that is gone"
            )
        return StreamView.of_rows(
            self,
            table,
            tag_columns=self.keys(columns=config)[0],
            total_rows=len(rows),
            origin=origin,
            note=note,
        )

    def __repr__(self) -> str:
        # The input by class and label only: its own repr would nest every path (ITL-1111).
        return (
            f"{type(self).__name__}(label={self.label!r}, "
            f"data_function={self._data_function!r}, "
            f"input_stream={short_stream_repr(self._input_stream)})"
        )

    @abstractmethod
    def set_ephemeral_database(self, database: "ArrowDatabaseProtocol | None") -> None:
        """Assign or remove the ephemeral result database for this node.

        ``FunctionJobNode`` overrides this with a real implementation that routes
        new computation results to ``database`` instead of the persistent result
        database. ``FunctionNode`` (blueprint) provides a no-op override.

        Args:
            database: An ``ArrowDatabaseProtocol`` instance to attach, or ``None``
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

    def set_ephemeral_database(self, database: "ArrowDatabaseProtocol | None") -> None:
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
        node._data_context = contexts.resolve_saved_context(
            descriptor.get("data_context_key")
        )
        from orcapod.config import DEFAULT_CONFIG, OrcapodConfig

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

        # Restore node_type from the stored URI prefix
        node._node_type = (
            "side_effect_function"
            if descriptor.get("node_uri", [""])[0] == "side_effect_function"
            else "function"
        )

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
        """Path to cached records in the result database (versioned).

        Returns the v1 schema path: ``_record_path + (RESULT_DB_SCHEMA_VERSION,)``.
        Matches ``CachedFunctionPod.record_path`` semantics so that stub nodes
        (loaded from a saved job) resolve to the same storage location as live nodes.
        """
        return self._record_path + (RESULT_DB_SCHEMA_VERSION,)


# ---------------------------------------------------------------------------
# FunctionJobNode — DB-backed execution node
# ---------------------------------------------------------------------------


class _JoinedRecords(NamedTuple):
    """Internal result type returned by ``_fetch_joined_records``.

    Attributes:
        table: The joined ``pa.Table`` for rows that matched in the result DB.
            Always includes a ``_PIPELINE_BASE_ENTRY_ID_COL`` column.
        taginfo_columns: Column names from the pipeline database fetch,
            captured before the join. Used by ``_load_cached_entries`` to
            derive tag keys in the CACHE_ONLY fallback path.
        empty_data_tokens: Mapping from ``base_entry_id`` bytes to an
            ``EmptyData`` token for each ephemeral tag row whose result
            was not found in either database (cross-session miss). Empty dict
            when no ephemeral misses occurred.
        empty_taginfo_rows: Mapping from ``base_entry_id`` bytes to the raw
            taginfo row dict for each entry in ``empty_data_tokens``. Used
            by ``_load_cached_entries`` to reconstruct the tag for each token.
    """

    table: pa.Table
    taginfo_columns: tuple[str, ...]
    empty_data_tokens: dict[bytes, EmptyData]
    empty_taginfo_rows: dict[bytes, dict]


class FunctionJobNode(RunHoldingNode, FunctionNodeBase):
    """DB-backed execution node for function pod invocations.

    Created by ``PipelineJob`` at run time; never recorded inside a plain
    ``Pipeline``.  Carries all persistence logic: ``CachedFunctionPod``
    wrapping, pipeline records, and two-phase ``iter_data()`` / async
    execution.
    """

    # Set once a read has reported the stored rows it skipped (rows of another data
    # context); a class default so nodes rebuilt from a saved descriptor start unset.
    _skipped_rows_reported: bool = False
    # The job file's expected row count and whether it was checked (ITL-1043); class
    # defaults so a node attached before ``from_descriptor`` sets them has them.
    _expected_row_count: int | None = None
    _row_count_checked: bool = False
    # Set once stored rows of other generations have been accounted for. A node
    # that delivers side effects unsets it at the start of every run.
    _stale_tables_checked: bool = False
    # Set once the rows of the current tables were counted by data context.
    _current_tables_checked: bool = False
    # Set once a database that cannot list its tables was reported.
    _unlisted_store_reported: bool = False
    # Base entry ids of the inputs of the last completed run in this process, each with
    # its number of inputs; None when no run completed (ITL-1093). A class default so
    # nodes rebuilt from a saved descriptor start without a run.
    _run_entry_counts: Counter[bytes] | None = None
    # Whether the last completed run was load-only, and how many of its inputs had no
    # stored result (ITL-1095). Class defaults for nodes rebuilt from a descriptor.
    _run_load_only: bool = False
    _run_not_stored: int = 0
    # Rows of the last completed run whose function raised (error_policy "continue"):
    # such a run is never current, so the next run retries them (ITL-1095).
    _run_failed: int = 0

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
        self._run_entry_counts = None

        # DB persistence state (initially None; set via __init__ params or attach_databases)
        self._pipeline_database: ArrowDatabaseProtocol | None = None
        self._cached_function_pod: CachedFunctionPod | None = None

        # Ephemeral result database (None until set_ephemeral_database() is called by the pipeline)
        self.ephemeral_result_database: "ArrowDatabaseProtocol | None" = None
        self._ephemeral_cached_pod: CachedFunctionPod | None = None

        # Node-level orchestrator config — applied post-construction via the
        # .node_config setter or PipelineJob.apply_node_config().
        self._node_config: NodeConfig = NodeConfig()

        # Set by from_descriptor() for a loaded node whose job file records how many
        # pipeline records its run had in the database (ITL-1043); checked once per
        # database, with a warning only: the database is the source of truth.
        self._expected_row_count: int | None = None
        self._row_count_checked: bool = False
        # Batched commits, set only while PipelineJob.run runs the node (ITL-919).
        self._commit_every_rows: int | None = None
        self._rows_since_commit = 0
        self._commit_failed = False

        self.attach_databases(
            pipeline_database=pipeline_database,
            result_database=result_database,
            ephemeral_database=ephemeral_database,
        )
        # A node that has not run in this process holds nothing and is stale (ITL-1095).
        self._set_modified_time(None)

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

    @property
    def _versioned_pipeline_path(self) -> tuple[str, ...]:
        """Pipeline DB path with schema version suffix appended.

        All pipeline DB reads and writes use this path (not ``node_identity_path``
        directly) so that v0 and v1 schema data live at physically separate locations.
        """
        return self.node_identity_path + (PIPELINE_DB_SCHEMA_VERSION,)

    @property
    def _stored_data_context_key(self) -> str:
        """Key of the data context this node writes on its rows and reads back.

        A node loaded from a saved job has no function pod; its context then comes
        from the descriptor.
        """
        context = (
            self._data_context if self._function_pod is None else self.data_context
        )
        return context.context_key

    @property
    def _delivers_side_effects(self) -> bool:
        """Whether running this node is an external action.

        True for a function pod that takes the invocation context (``ctx_arg``):
        its call is a side effect, and its completions are its function tables.
        """
        return (
            self._function_pod is not None
            and self._function_pod.ctx_arg_name is not None
        )

    def _begin_run(self) -> None:
        """Make a node that delivers side effects look for older completions again.

        Tables written before v0.3 can appear in a database between two runs of one
        node object. Such a node must find them before it delivers anything, so it
        looks for them at the start of every run, also when every input is already
        held in memory; other nodes report once, when they first read the database.

        Raises:
            SideEffectLogDataContextError: If this node delivers side effects and
                completions it cannot match exist; nothing has been delivered then.
        """
        if self._delivers_side_effects:
            self._stale_tables_checked = False
            self._account_for_stale_tables()

    def _account_for_stale_tables(self) -> None:
        """Report or refuse stored rows of other generations.

        Done once per node object; a node that delivers side effects does it at
        the start of every run (``_begin_run``). Its current tables are read and
        counted by data context the first time only: the later runs list the database
        for tables at an older layout, which reads no table.

        A pipeline table written by an earlier version of Orcapod (``pdb_v1``) holds
        no ``__data_context`` column: its rows are not read as results and are left
        unchanged, and their number is reported (ITL-944). Such tables are looked for
        at this node's own path and, because the hashes in a path can differ between
        versions, anywhere under the function's name in the database (ITL-954). A node
        that delivers side effects fails instead, and for it the result tables and
        rows of another data context in the current tables count too: none of them
        can be matched with current inputs, so every side effect would be delivered
        again.

        Raises:
            SideEffectLogDataContextError: If this node delivers side effects and
                such rows exist; nothing has been delivered or written then.
        """
        if self._stale_tables_checked or self._pipeline_database is None:
            return
        context_key = self._stored_data_context_key
        delivers = self._delivers_side_effects
        found = find_stale_tables(
            self._pipeline_database,
            # Orcapod v0.1 stored the table at the path without a layout component.
            tuple(
                self.node_identity_path + (layout,)
                for layout in OLD_PIPELINE_DB_SCHEMA_VERSIONS
            )
            + (self.node_identity_path,),
            current=(self._versioned_pipeline_path, context_key)
            if delivers and not self._current_tables_checked
            else None,
            name_path=self.node_name_path or None,
        )
        if delivers:
            for cached_pod in (self._cached_function_pod, self._ephemeral_cached_pod):
                if cached_pod is not None:
                    found.include(
                        cached_pod.stale_result_tables(
                            include_current=not self._current_tables_checked
                        )
                    )
            if found.unlisted:
                # That the database cannot list its tables is reported once per node
                # object and attached database, not at every run.
                if self._unlisted_store_reported:
                    found = StaleTables(tables=found.tables)
                self._unlisted_store_reported = True
        try:
            account_for_stale_tables(
                found, delivers_side_effects=delivers, current_context=context_key
            )
        except OrcapodDatabaseError as exc:
            exc.add_context(node_label=self.label)
            raise
        if delivers:
            for cached_pod in (self._cached_function_pod, self._ephemeral_cached_pod):
                if cached_pod is not None:
                    cached_pod.stale_result_tables_accounted_for()
        self._stale_tables_checked = True
        self._current_tables_checked = True

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
            ephemeral_database: Optional ephemeral database to attach immediately
                via ``set_ephemeral_database()``. Equivalent to calling
                ``set_ephemeral_database(ephemeral_database)`` after
                ``attach_databases()``.
        """
        # Another database has its own tables of other generations to account for.
        self._stale_tables_checked = False
        self._current_tables_checked = False
        self._unlisted_store_reported = False
        self._skipped_rows_reported = False
        # ... and its own rows to compare with the job file's expected count.
        self._row_count_checked = False
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
                # With a database attached, the stub can serve the results its saved
                # run stored, though it still cannot compute.
                from orcapod.pipeline.serialization import LoadStatus

                self._load_status = LoadStatus.READ_ONLY

            self._pipeline_database = pipeline_database

            # Clear all caches
            self._node_identity_path_cache = None
            self.clear_cache()
            self._invalidate_content_hash_cache()
            self._invalidate_pipeline_hash_cache()

        if ephemeral_database is not None:
            self.set_ephemeral_database(ephemeral_database)

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
        expected_row_count: int | None = None,
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
            expected_row_count: How many pipeline records of this node (with its
                run's content hash) the database held when the job was saved, from the
                job file's optional ``run.expected_rows``; ``None`` when not recorded.
                Advisory: a read always returns what the database holds and only warns
                (``OrcapodDatabaseContentWarning``) when the database holds another number.

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

        # From FunctionNodeBase
        node._function_pod = None
        node._data_function = None
        node._input_stream = input_stream
        node.tracker_manager = DEFAULT_TRACKER_MANAGER

        # FunctionNodeBase descriptor fields (normally set by __init__)
        node._load_status = LoadStatus.UNAVAILABLE
        # Restore the live content hash from the serialised descriptor so that
        # ``content_hash()`` on the deserialised node matches the hash that was
        # computed when the original node ran.
        node._stored_content_hash = (
            job_content_hash
            if job_content_hash is not None
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
        # The descriptor stores hashes as labelled values; a path holds bare digests.
        pipeline_hash_str = descriptor.get("pipeline_hash", "")
        node._stored_pipeline_path = node._stored_node_uri + (
            f"schema:{_bare_digest(pipeline_hash_str)}",
        )
        if raw_table_scope != "pipeline_hash":
            # Use _stored_content_hash (already set to job_content_hash when
            # provided, otherwise falls back to the blueprint hash from the
            # descriptor) so the instance fragment matches the DB path written
            # during the original run for table_scope="content_hash".
            content_hash_str = node._stored_content_hash or ""
            node._stored_pipeline_path += (
                f"instance:{_bare_digest(content_hash_str)}",
            )

        # FunctionJobNode — stream-level caching state
        node._cached_output_datas = {}
        node._cached_output_table = None
        node._cached_content_hash_column = None

        # FunctionJobNode — orchestrator config
        node._node_config = NodeConfig()
        node._expected_row_count = expected_row_count
        node._row_count_checked = False
        node._commit_every_rows = None
        node._rows_since_commit = 0
        node._commit_failed = False

        # FunctionJobNode — DB persistence state (wired by attach_databases)
        node._pipeline_database = None
        node._cached_function_pod = None

        # FunctionJobNode — ephemeral result database (same defaults as __init__).
        # A loaded node has no function pod, so it never gets an ephemeral pod;
        # _fetch_joined_records reads it to resolve ephemeral pipeline records.
        node.ephemeral_result_database = None
        node._ephemeral_cached_pod = None

        # Wire databases if provided
        pipeline_db = (databases or {}).get("pipeline")
        result_db = (databases or {}).get("result")
        if pipeline_db is not None:
            node.attach_databases(
                pipeline_database=pipeline_db,
                result_database=result_db,
            )

        # Restore node_type from the stored URI prefix
        node._node_type = (
            "side_effect_function"
            if descriptor.get("node_uri", [""])[0] == "side_effect_function"
            else "function"
        )

        return node

    # ------------------------------------------------------------------
    # Override clear_cache to also clear DB caches
    # ------------------------------------------------------------------

    def clear_cache(self) -> None:
        """Clear in-memory output caches, content hash cache, and node identity path cache.

        The node then holds no run: it is stale and reads empty, as before its first run
        in this process (ITL-1095). Its stored results stay stored (``as_source()``).
        """
        super().clear_cache()
        self._cached_output_datas.clear()
        self._cached_output_table = None
        self._cached_content_hash_column = None
        self._run_entry_counts = None
        self._run_load_only = False
        self._run_not_stored = 0
        self._run_failed = 0
        # No content established: the node, and every node downstream, is stale.
        self._set_modified_time(None)

    def _held_pairs(self) -> Iterator[tuple[TagProtocol, DataProtocol]]:
        """Yield the (tag, data) rows this node holds; every read of them goes through here.

        After a completed run, the rows are those of that run's inputs: one per input
        (two equal inputs give two rows, as downstream received them). The in-memory
        cache may hold more (results of other inputs, read from the database), which stay
        cached for reuse but are not rows of this run (ITL-1093). Without a completed
        run in this process there are no rows, whatever the cache or the database holds
        (ITL-1095). Rows come in the order the cache holds them. An input without output
        (an inactive function) is not a row.
        """
        counts = self._run_entry_counts
        if counts is None:
            return
        pairs: Iterable[tuple[TagProtocol, DataProtocol | None]] = (
            pair
            for eid, pair in self._cached_output_datas.items()
            for _ in range(counts.get(eid, 0))
        )
        for tag, data in pairs:
            if data is not None:
                yield tag, data

    def _set_run_rows(
        self,
        entry_ids: list[bytes],
        *,
        load_only: bool = False,
        not_stored: int = 0,
        failed: int = 0,
    ) -> None:
        """Make the inputs of a completed run, by base entry id, the rows this node holds.

        Args:
            entry_ids: The base entry ids of the run's inputs whose rows the node holds.
            load_only: Whether the run was load-only (ITL-1095).
            not_stored: How many inputs of a load-only run had no stored result.
            failed: How many rows of the run failed (error policy ``"continue"``).
        """
        self._run_entry_counts = Counter(entry_ids)
        self._run_load_only = load_only
        self._run_not_stored = not_stored
        self._run_failed = failed
        self._cached_output_table = None
        self._cached_content_hash_column = None

    def _read_finds_stale(self) -> bool:
        """Whether a read finds the rows held out of date; never for ``CACHE_ONLY``,
        whose input is unavailable (``head`` and ``iter_data`` judge it the same way)."""
        from orcapod.pipeline.serialization import LoadStatus

        return self.load_status != LoadStatus.CACHE_ONLY and self.is_stale

    def _holds_run(self, *, load_only: bool) -> bool:
        """Whether the node holds a completed run that a run need not redo.

        A load-only run's rows are good only for another load-only run. A run that left
        failed rows is never good: the next run retries them, reading the rest from the
        database (ITL-1095).
        """
        return (
            self._run_entry_counts is not None
            and self._run_failed == 0
            and (load_only or not self._run_load_only)
        )

    @property
    def last_run_not_stored(self) -> int:
        """Inputs the node's last completed run skipped because nothing was stored.

        Non-zero only after a load-only run (``run(load_only=True)``); 0 before a run.
        """
        return self._run_not_stored

    def _stored_entry(
        self, entry_id: bytes
    ) -> tuple[TagProtocol, DataProtocol | None] | None:
        """The result held for ``entry_id`` after a lookup, or ``None`` if none is stored.

        A placeholder for a result kept in an ephemeral database that is gone is not a
        stored result.
        """
        entry = self._cached_output_datas.get(entry_id)
        if entry is None or isinstance(entry[1], EmptyData):
            return None
        return entry

    def _execute_load_only(
        self,
        input_stream: StreamProtocol,
        observer: ExecutionObserverProtocol | None,
    ) -> list[tuple[TagProtocol, DataProtocol]]:
        """Fill the node with the stored results of ``input_stream``'s rows; compute nothing.

        The load-only form of ``execute`` (ITL-1095): each input's entry is looked up
        among the node's own stored entries (its pipeline records under its content
        hash, the lookup a run makes before computing). An input with a stored result
        is a loaded row (``on_data_start`` and ``on_data_end(cached=True)``); one
        without is skipped and counted (``last_run_not_stored``), with no observer
        call. No function is called and nothing is written.

        Returns:
            The loaded (tag, output data) rows, excluding ``None`` outputs.

        Raises:
            RuntimeError: If no database is attached (``UNAVAILABLE``).
            InputValidationError: If the input stream is of another data context.
        """
        from orcapod.pipeline.observer import NoOpObserver
        from orcapod.pipeline.serialization import LoadStatus

        node_label = self.label
        node_hash = self.content_hash().to_string()
        obs = observer if observer is not None else NoOpObserver()
        ctx_obs = obs.contextualize(*self.node_identity_path)
        output: list[tuple[TagProtocol, DataProtocol]] = []
        loaded_ids: list[bytes] = []
        not_stored = 0
        try:
            if self.load_status == LoadStatus.UNAVAILABLE:
                raise self._no_database_error()
            require_same_data_context(
                input_stream.data_context_key, self._stored_data_context_key
            )
            tag_schema = input_stream.output_schema(columns={"system_tags": True})[0]
            ctx_obs.on_node_start(node_label, node_hash, tag_schema=tag_schema)
            inputs = [
                (tag, data, self.compute_base_entry_id(tag, data))
                for tag, data in input_stream.iter_data()
            ]
            _ = self.get_cached_results(base_entry_ids=[eid for _, _, eid in inputs])
            for tag, data, entry_id in inputs:
                entry = self._stored_entry(entry_id)
                if entry is None:
                    not_stored += 1
                    continue
                ctx_obs.on_data_start(node_label, tag, data)
                ctx_obs.on_data_end(node_label, tag, data, entry[1], cached=True)
                loaded_ids.append(entry_id)
                if entry[1] is not None:
                    output.append((entry[0], entry[1]))
            self._set_run_rows(loaded_ids, load_only=True, not_stored=not_stored)
            # Current before the observer hears the end, so a read there sees the rows.
            self._update_modified_time()
            ctx_obs.on_node_end(node_label, node_hash)
        except BaseException:
            # A run that raised (an observer at the node's end included) defines no rows.
            self.clear_cache()
            raise
        return output

    async def _async_execute_load_only(
        self,
        input_channel: ReadableChannel[tuple[TagProtocol, DataProtocol]],
        output: WritableChannel[tuple[TagProtocol, DataProtocol]],
        observer: ExecutionObserverProtocol | None,
    ) -> None:
        """The async form of ``_execute_load_only``: same rows, counts and observer calls.

        The node reads its whole input before it looks the entries up, then sends the
        loaded rows; it stamps itself before its output closes (ITL-1042), and closes
        the output whatever happens.
        """
        from orcapod.pipeline.observer import NoOpObserver
        from orcapod.pipeline.serialization import LoadStatus

        try:
            if self.load_status == LoadStatus.UNAVAILABLE:
                raise self._no_database_error()
            require_same_data_context(
                self._input_stream.data_context_key, self._stored_data_context_key
            )
            node_label = self.label
            node_hash = self.content_hash().to_string()
            obs = observer if observer is not None else NoOpObserver()
            ctx_obs = obs.contextualize(*self.node_identity_path)
            tag_schema = self._input_stream.output_schema(
                columns={"system_tags": True}
            )[0]
            ctx_obs.on_node_start(node_label, node_hash, tag_schema=tag_schema)
            inputs = [
                (tag, data, self.compute_base_entry_id(tag, data))
                async for tag, data in input_channel
            ]
            _ = self.get_cached_results(base_entry_ids=[eid for _, _, eid in inputs])
            loaded_ids: list[bytes] = []
            not_stored = 0
            for tag, data, entry_id in inputs:
                entry = self._stored_entry(entry_id)
                if entry is None:
                    not_stored += 1
                    continue
                ctx_obs.on_data_start(node_label, tag, data)
                ctx_obs.on_data_end(node_label, tag, data, entry[1], cached=True)
                loaded_ids.append(entry_id)
                if entry[1] is not None:
                    await output.send((entry[0], entry[1]))
            self._set_run_rows(loaded_ids, load_only=True, not_stored=not_stored)
            # Current before the observer hears the end, so a read there sees the rows.
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

    def set_ephemeral_database(self, database: "ArrowDatabaseProtocol | None") -> None:
        """Assign or remove the ephemeral result database.

        When *database* is not ``None``, creates a ``CachedFunctionPod`` backed by
        *database* so that ephemeral writes use the same format as persistent writes.
        When *database* is ``None``, clears both the database and the ephemeral pod.

        Note:
            For deserialized read-only nodes where ``_function_pod`` is ``None``,
            ``ephemeral_result_database`` is assigned but ``_ephemeral_cached_pod``
            remains ``None``. Such nodes cannot compute new results anyway, so
            ``_process_data_internal`` must guard against this by checking
            ``_ephemeral_cached_pod is not None`` before attempting ephemeral writes.

        Args:
            database: The ``ArrowDatabaseProtocol`` to use for ephemeral result
                storage, or ``None`` to detach and revert to persistent-only writes.
        """
        # Another database has its own tables of other generations to account for,
        # as in ``attach_databases``.
        self._stale_tables_checked = False
        self._current_tables_checked = False
        self._unlisted_store_reported = False
        self.ephemeral_result_database = database
        if database is not None and self._function_pod is not None:
            self._ephemeral_cached_pod = CachedFunctionPod(
                self._function_pod,
                result_database=database,
            )
        else:
            self._ephemeral_cached_pod = None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _no_database_error(self) -> RuntimeError:
        """Return the error for reading a loaded node that has no database attached.

        Only a node loaded by ``PipelineJob.load`` can be ``UNAVAILABLE``: it has no
        function pod, so its results can come only from the database.
        """
        return RuntimeError(
            f"FunctionJobNode {self.label!r} was loaded without a database, so its stored "
            "results cannot be read. Pass a database to PipelineJob.load(path, "
            "database=...) or call job.bind(database=...)."
        )

    def _cannot_compute_error(self) -> RuntimeError:
        """Return the error for asking a loaded node to compute.

        A node loaded by ``PipelineJob.load`` has no function pod: it can serve
        the results its saved run stored, never compute new ones.
        """
        return RuntimeError(
            f"FunctionJobNode {self.label!r} was loaded by PipelineJob.load and has no "
            "function pod, so it cannot compute; fill it from the database with "
            "run(load_only=True), or read its stored results with as_source()."
        )

    def _stored_record_count(self) -> int:
        """Return how many pipeline records of this node the attached database holds.

        A record belongs to this node when its ``_node_content_hash`` is this
        node's content hash: the rows ``iter_data()`` and ``get_all_records()``
        read back. Records of other runs or data contexts do not count.
        """
        if self._pipeline_database is None:
            return 0
        if not self._pipeline_database.table_exists(self._versioned_pipeline_path):
            return 0
        own_records = self._pipeline_database.get_records_with_column_value(
            self._versioned_pipeline_path,
            {
                constants.NODE_CONTENT_HASH_COL: self.content_hash().to_prefixed_digest(),
                constants.DATA_CONTEXT_COL: self._stored_data_context_key,
            },
        )
        return 0 if own_records is None else own_records.num_rows

    def _expected_row_count_to_save(self, run_finished: bool) -> int | None:
        """Return the count ``PipelineJob.save`` records for this node, if certain.

        A loaded node keeps the count its file recorded, or none. A node of a job
        whose last run finished (its writes committed) records the pipeline records
        the database holds under its run's key; otherwise nothing is certain.

        Args:
            run_finished: Whether the job's last run finished and committed.
        """
        if self._function_pod is None:
            return self._expected_row_count
        if not run_finished or self._pipeline_database is None:
            return None
        return self._stored_record_count()

    def _require_readable_stub(self) -> None:
        """Raise unless this loaded node can read its stored results.

        Raises:
            RuntimeError: If no database is attached.
        """
        from orcapod.pipeline.serialization import LoadStatus

        if self.load_status == LoadStatus.UNAVAILABLE:
            raise self._no_database_error()
        self._check_expected_rows()

    def _check_expected_rows(self) -> None:
        """Warn, once per database, if the database holds another number of rows than recorded.

        The database is the source of truth: the read goes on and returns what it holds
        (ITL-1043). Only a job file that records the node's count is checked.
        """
        if self._expected_row_count is None or self._row_count_checked:
            return
        self._row_count_checked = True
        if self._pipeline_database is None:
            return
        found = self._stored_record_count()
        if found != self._expected_row_count:
            report_unexpected_row_count(
                f"FunctionJobNode {self.label!r}",
                describe_database_table(
                    self._pipeline_database, self._versioned_pipeline_path
                ),
                self._expected_row_count,
                found,
            )

    def _require_pipeline_database(self) -> ArrowDatabaseProtocol:
        """Return the attached pipeline database, raising if there is none.

        Called at the top of methods that unconditionally access
        ``self._pipeline_database``.  Provides an actionable error message
        instead of an opaque ``AttributeError: 'NoneType' object has no
        attribute ...`` when a definition-level pipeline is executed without
        supplying a database.

        Returns:
            The attached pipeline database.

        Raises:
            RuntimeError: If no pipeline database is attached.
        """
        if self._pipeline_database is None:
            raise RuntimeError(
                f"FunctionJobNode '{self.label}' has no pipeline database attached. "
                "Either construct the pipeline with a pipeline_database argument, "
                "or supply one via Pipeline.load(..., pipeline_database=<db>)."
            )
        return self._pipeline_database

    # ------------------------------------------------------------------
    # Batched commits (ITL-919)
    # ------------------------------------------------------------------

    @contextlib.contextmanager
    def _batched_commits(self, rows_per_commit: int) -> Iterator[None]:
        """Commit this node's writes every ``rows_per_commit`` rows inside the block.

        ``PipelineJob.run`` runs each function node inside this block. Its result
        caches stop committing after every stored result, and the node commits its
        pending writes (ephemeral results, then its result table, then the pipeline
        records) every ``rows_per_commit`` rows and at its end. Outside the block
        (a node run on its own) every result is committed as it is stored.

        Args:
            rows_per_commit: How many processed rows' writes one commit holds.

        Yields:
            Nothing; the node commits in batches while the block runs.
        """
        pods = [
            pod
            for pod in (self._cached_function_pod, self._ephemeral_cached_pod)
            if isinstance(pod, CachedFunctionPod)
        ]
        self._commit_every_rows = rows_per_commit
        self._rows_since_commit = 0
        self._commit_failed = False
        for pod in pods:
            pod.set_auto_flush(False)
        try:
            yield
        finally:
            self._commit_every_rows = None
            for pod in pods:
                pod.set_auto_flush(True)

    def _ephemeral_database_in_use(self) -> ArrowDatabaseProtocol | None:
        """Return the ephemeral database this node writes results to, if it does."""
        if self._node_config.is_result_ephemeral and self._ephemeral_cached_pod:
            return self.ephemeral_result_database
        return None

    def _commit_result_table(self) -> None:
        """Commit this node's pending result rows, before its pipeline records.

        Every view of a database shares one set of pending writes, and a flush commits
        them table by table in no guaranteed order. Committing the result table on
        its own first means a crash between the two commits leaves results without
        pipeline records (found in the result cache on the rerun, so nothing is
        recomputed), never pipeline records pointing at results that were lost.
        Only a database that commits table by table
        (``TableCommittingDatabaseProtocol``, the Delta database) needs and offers this;
        for another database the pipeline flush that follows commits everything.
        """
        pod = self._cached_function_pod
        if not isinstance(pod, CachedFunctionPod):
            return
        result_database = as_table_committing(pod.result_database)
        if result_database is not None:
            result_database.flush_batch(pod.record_path)

    def _commit_pending_writes(self) -> None:
        """Commit this node's pending writes, results first (see ``_batched_commits``).

        Raises:
            OrcapodDatabaseReadError: If a stored table could not be read; it names this node.
            OrcapodDatabaseWriteError: If a commit failed, naming this node and the database
                (``flush=True``, no row: the batch holds many rows). The failed
                writes stay pending, and earlier commits are kept.
        """
        pipeline_database = self._require_pipeline_database()
        ephemeral = self._ephemeral_database_in_use()
        try:
            if ephemeral is not None:
                try:
                    ephemeral.flush()
                except OrcapodDatabaseReadError:
                    raise
                except Exception as exc:
                    raise OrcapodDatabaseWriteError(
                        ephemeral.base_path, node_label=self.label, flush=True
                    ) from exc
            try:
                self._commit_result_table()
                pipeline_database.flush()
            except OrcapodDatabaseReadError:
                raise
            except Exception as exc:
                raise OrcapodDatabaseWriteError(
                    pipeline_database.base_path, node_label=self.label, flush=True
                ) from exc
        except OrcapodDatabaseError as exc:
            self._commit_failed = True
            exc.add_context(node_label=self.label)
            raise
        self._rows_since_commit = 0

    def _row_processed(self) -> None:
        """Count a processed row; commit once a batch is full (only inside a job run)."""
        if self._commit_every_rows is None:
            return
        self._rows_since_commit += 1
        if self._rows_since_commit >= self._commit_every_rows:
            self._commit_pending_writes()

    def _commit_at_node_end(self) -> None:
        """Commit what is still pending when the node finishes (only inside a job run)."""
        if self._commit_every_rows is not None:
            self._commit_pending_writes()

    def _commit_after_failure(self, run_error: BaseException) -> None:
        """Commit what succeeded before a failure, never replacing the run's error.

        The job flushes its database after a failed run, but not a node's ephemeral
        database, a separate database, and not table by table: so the node commits its
        ephemeral results and then its result table here, best-effort (a failure
        is noted on ``run_error``). Skipped when the failure was this node's own
        commit: the job's flush is that commit's one retry.

        Args:
            run_error: The error the node is failing with.
        """
        if self._commit_every_rows is None or self._commit_failed:
            return
        ephemeral = self._ephemeral_database_in_use()
        if ephemeral is not None:
            commit_best_effort(run_error, ephemeral.flush)
        commit_best_effort(run_error, self._commit_result_table)

    @staticmethod
    def _commit_or_end_node(
        commit: Any, ctx_obs: Any, node_label: str, node_hash: str
    ) -> None:
        """Run a commit step; if it fails, end the node for the observer and re-raise.

        Args:
            commit: ``_row_processed`` or ``_commit_at_node_end``.
            ctx_obs: The node's contextualized observer.
            node_label: The node's label, for the observer.
            node_hash: The node's content hash, for the observer.
        """
        try:
            commit()
        except OrcapodDatabaseError as exc:
            with keep_database_error(exc, "Observer on_node_end"):
                ctx_obs.on_node_end(node_label, node_hash)
            raise

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
        run_id: str | None = None,
        load_only: bool = False,
    ) -> list[tuple[TagProtocol, DataProtocol]]:
        """Execute all data from a stream: compute, persist, and cache.

        For each data: fire ``on_data_start``, check the in-memory cache
        (populated from DB if needed), compute if missing, fire
        ``on_data_end`` or ``on_data_crash``.

        Afterwards the node's rows (``iter_data``, ``as_table``, ``head``) are those
        of this run's inputs. If the run raises, it defines no rows: the node drops
        its in-memory rows and reads as a node with no run in this process; what was
        stored stays stored.

        Args:
            input_stream: The input stream to process.
            observer: Optional execution observer for hooks.
            error_policy: ``"continue"`` skips failed data;
                ``"fail_fast"`` re-raises on the first failure.
            run_id: Optional pipeline run identifier threaded through to
                ``InvocationContext.pipeline_run_id``.
            load_only: Fill the node with the results already stored for the
                stream's rows and compute nothing (ITL-1095): no function call, no
                write; inputs without a stored result are skipped and counted
                (``last_run_not_stored``). A loaded node (no function pod) can run
                this way.

        Returns:
            Materialized list of (tag, output_data) pairs, excluding
            ``None`` outputs and failed data.

        Raises:
            RuntimeError: If the node was loaded by ``PipelineJob.load`` and so
                has no function pod to compute with.
            InputValidationError: If the input stream is of another data context
                than this node; raised before any row is processed, whatever the
                error policy.
        """
        from orcapod.pipeline.observer import NoOpObserver

        if load_only:
            return self._execute_load_only(input_stream, observer)
        if self._function_pod is None:
            raise self._cannot_compute_error()

        try:
            self._begin_run()
            # A whole-input condition, not a row failure: the error policy does not
            # apply.
            require_same_data_context(
                input_stream.data_context_key, self._stored_data_context_key
            )
        except BaseException:
            # A run that raised defines no rows, also when it was refused at once.
            self.clear_cache()
            raise
        node_label = self.label
        node_hash = self.content_hash().to_string()

        obs = observer if observer is not None else NoOpObserver()
        ctx_obs = obs.contextualize(*self.node_identity_path)

        tag_schema = input_stream.output_schema(columns={"system_tags": True})[0]
        ctx_obs.on_node_start(node_label, node_hash, tag_schema=tag_schema)

        output: list[tuple[TagProtocol, DataProtocol]] = []
        try:
            # Collect upstream entries and resolve base_entry_ids (stable across
            # recomputation)
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

            # Base entry ids of the inputs that gave a row (a cache hit or a
            # successful computation), one per input: the node's rows afterwards.
            emitted_entry_ids: list[bytes] = []
            failed_rows = 0
            for tag, data, base_entry_id in upstream_entries:
                ctx_obs.on_data_start(node_label, tag, data)

                if base_entry_id in self._cached_output_datas and not isinstance(
                    self._cached_output_datas[base_entry_id][1], EmptyData
                ):
                    tag_out, result = self._cached_output_datas[base_entry_id]
                    ctx_obs.on_data_end(node_label, tag, data, result, cached=True)
                    emitted_entry_ids.append(base_entry_id)
                    if result is not None:
                        output.append((tag_out, result))
                    continue

                pkt_logger = ctx_obs.create_data_logger(tag, data)
                try:
                    tag_out, result = self._process_data_internal(
                        tag,
                        data,
                        logger=pkt_logger,
                        run_id=run_id,
                        base_entry_id=base_entry_id,
                    )
                except OrcapodDatabaseError as exc:
                    # A failed database read or write fails the run whatever the error
                    # policy: error_policy only decides about rows whose function
                    # raised.
                    exc.add_context(node_label=node_label, tag=tag.as_dict())
                    with keep_database_error(exc, "Observer on_data_crash"):
                        ctx_obs.on_data_crash(node_label, tag, data, exc)
                    with keep_database_error(exc, "Observer on_node_end"):
                        ctx_obs.on_node_end(node_label, node_hash)
                    raise
                except TypeIdentityError as exc:
                    # A stored row that cannot be decoded as its class is never
                    # skipped as a row failure, whatever the error policy (ITL-881).
                    with keep_database_error(exc, "Observer on_data_crash"):
                        ctx_obs.on_data_crash(node_label, tag, data, exc)
                    with keep_database_error(exc, "Observer on_node_end"):
                        ctx_obs.on_node_end(node_label, node_hash)
                    raise
                except Exception as exc:
                    # Reported at the end of a job run (RunSummary, RowFailuresWarning),
                    # so one traceback per failed row is only for debugging.
                    logger.debug(
                        "Data execution failed in %s, skipping: %s",
                        node_label,
                        exc,
                        exc_info=True,
                    )
                    ctx_obs.on_data_crash(node_label, tag, data, exc)
                    if error_policy == "fail_fast":
                        ctx_obs.on_node_end(node_label, node_hash)
                        raise
                    failed_rows += 1
                    continue

                ctx_obs.on_data_end(node_label, tag, data, result, cached=False)
                emitted_entry_ids.append(base_entry_id)
                if result is not None:
                    output.append((tag_out, result))
                self._commit_or_end_node(
                    self._row_processed, ctx_obs, node_label, node_hash
                )

            self._commit_or_end_node(
                self._commit_at_node_end, ctx_obs, node_label, node_hash
            )
            # The node's rows are those of this run's inputs, whatever else it caches.
            self._set_run_rows(emitted_entry_ids, failed=failed_rows)
            # Mark this node as freshly computed before the observer hears the end, so
            # a read there (and every later one) serves this run's rows instead of
            # finding the node stale and clearing them (ITL-1121).
            self._update_modified_time()
            ctx_obs.on_node_end(node_label, node_hash)
        except Exception as run_error:
            self._commit_after_failure(run_error)
            # A run that raised defines no rows: the node reads as one with no run.
            self.clear_cache()
            raise
        except BaseException:
            self.clear_cache()
            raise

        return output

    def _process_data_internal(
        self,
        tag: TagProtocol,
        data: DataProtocol,
        *,
        logger: DataExecutionLoggerProtocol | None = None,
        run_id: str | None = None,
        base_entry_id: bytes | None = None,
    ) -> tuple[TagProtocol, DataProtocol | None]:
        """Core compute + persist + cache.

        Used by ``execute_data`` and ``execute``.
        Stores result in ``_cached_output_datas`` keyed by base_entry_id.
        Exceptions propagate to the caller — no error handling here.

        Args:
            tag: The tag associated with the data.
            data: The input data to process.
            logger: Optional data execution logger.
            run_id: Optional pipeline run identifier threaded through to
                ``InvocationContext.pipeline_run_id``.
            base_entry_id: The row's ``compute_base_entry_id``, when the caller
                already has it; computed here otherwise.

        When ``node_config.is_result_ephemeral=True``:
        - Uses ``_ephemeral_cached_pod`` for both compute and storage.
        - Raises ``RuntimeError`` if no ephemeral database has been set.
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
        # A node that delivers side effects must not run on completions it cannot match.
        self._account_for_stale_tables()
        require_same_data_context(data.data_context_key, self._stored_data_context_key)
        # Guard: EmptyData inputs skip all computation and go straight to cache lookup.
        # EmptyData.content_hash() returns the cached_content_hash, so
        # result_cache.lookup(data) works transparently — it finds the downstream
        # cached result keyed by the same input hash as the original upstream execution.
        # add_pipeline_record is intentionally skipped for EmptyData inputs because
        # EmptyData.as_table() raises EmptyDataAccessError (no payload to read source
        # columns from). Pipeline DB write for this path is deferred.
        if base_entry_id is None:
            base_entry_id = self.compute_base_entry_id(tag, data)
        if isinstance(data, EmptyData):
            if self._cached_function_pod is not None:
                cached = self._cached_function_pod.lookup_cached_data(data)
                if cached is not None:
                    self._cached_output_datas[base_entry_id] = (tag, cached)
                    self._cached_output_table = None
                    self._cached_content_hash_column = None
                    return tag, cached
            raise EphemeralResultMissingError(
                tag=tag,
                cached_content_hash=data.cached_content_hash,
                node_identity_path=self.node_identity_path,
                message=(
                    "Downstream cache miss for EmptyData input — "
                    "ephemeral result is gone and downstream has not yet computed "
                    "a result for this input hash."
                ),
            )

        ephemeral_result = bool(self._node_config.is_result_ephemeral)

        if ephemeral_result:
            if self._ephemeral_cached_pod is None:
                raise RuntimeError(
                    f"FunctionJobNode '{self.label}' has is_result_ephemeral=True but no "
                    "ephemeral database has been assigned. Call set_ephemeral_database() with "
                    "an ArrowDatabaseProtocol before executing this node."
                )
            tag_out, output_data = self._ephemeral_cached_pod.process_data(
                tag, data, logger=logger, run_id=run_id
            )
            if output_data is not None:
                result_computed = bool(
                    output_data.get_meta_value(
                        self._ephemeral_cached_pod.RESULT_COMPUTED_FLAG, False
                    )
                )
                if self._pipeline_database is not None:
                    self._add_pipeline_record(
                        tag,
                        data,
                        base_entry_id,
                        data_record_id=output_data.datagram_uuid,
                        computed=result_computed,
                        is_ephemeral=True,
                        output_data_hash=output_data.content_hash(),
                    )
        elif self._cached_function_pod is not None:
            tag_out, output_data = self._cached_function_pod.process_data(
                tag, data, logger=logger, run_id=run_id
            )
            if output_data is not None:
                result_computed = bool(
                    output_data.get_meta_value(
                        self._cached_function_pod.RESULT_COMPUTED_FLAG, False
                    )
                )
                self._add_pipeline_record(
                    tag,
                    data,
                    base_entry_id,
                    data_record_id=output_data.datagram_uuid,
                    computed=result_computed,
                    output_data_hash=output_data.content_hash(),
                )
        else:
            tag_out, output_data = self._function_pod.process_data(
                tag, data, logger=logger, run_id=run_id
            )

        # Store by base_entry_id (stable across recomputation cycles) and invalidate caches
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

        missing = [
            eid for eid in base_entry_ids if eid not in self._cached_output_datas
        ]
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
        base_entry_id: bytes | None = None,
    ) -> tuple[TagProtocol, DataProtocol | None]:
        """Async counterpart of ``_process_data_internal``.

        Mirrors the sync ephemeral/persistent branch logic using async variants
        of ``process_data``. See ``_process_data_internal`` for full behaviour
        documentation, including ``base_entry_id``.

        Returns:
            A ``(tag, output_data)`` 2-tuple.
        """
        # A node that delivers side effects must not run on completions it cannot match.
        self._account_for_stale_tables()
        require_same_data_context(data.data_context_key, self._stored_data_context_key)
        # Guard: same logic as sync _process_data_internal — see inline comment there.
        if base_entry_id is None:
            base_entry_id = self.compute_base_entry_id(tag, data)
        if isinstance(data, EmptyData):
            if self._cached_function_pod is not None:
                cached = self._cached_function_pod.lookup_cached_data(data)
                if cached is not None:
                    self._cached_output_datas[base_entry_id] = (tag, cached)
                    self._cached_output_table = None
                    self._cached_content_hash_column = None
                    return tag, cached
            raise EphemeralResultMissingError(
                tag=tag,
                cached_content_hash=data.cached_content_hash,
                node_identity_path=self.node_identity_path,
                message=(
                    "Downstream cache miss for EmptyData input — "
                    "ephemeral result is gone and downstream has not yet computed "
                    "a result for this input hash."
                ),
            )

        ephemeral_result = bool(self._node_config.is_result_ephemeral)

        if ephemeral_result:
            if self._ephemeral_cached_pod is None:
                raise RuntimeError(
                    f"FunctionJobNode '{self.label}' has is_result_ephemeral=True but no "
                    "ephemeral database has been assigned. Call set_ephemeral_database() with "
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
                    self._add_pipeline_record(
                        tag,
                        data,
                        base_entry_id,
                        data_record_id=output_data.datagram_uuid,
                        computed=result_computed,
                        is_ephemeral=True,
                        output_data_hash=output_data.content_hash(),
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
                self._add_pipeline_record(
                    tag,
                    data,
                    base_entry_id,
                    data_record_id=output_data.datagram_uuid,
                    computed=result_computed,
                    output_data_hash=output_data.content_hash(),
                )
        else:
            tag_out, output_data = await self._function_pod.async_process_data(
                tag, data, logger=logger
            )

        # Store by base_entry_id (stable across recomputation cycles) and invalidate caches
        self._cached_output_datas[base_entry_id] = (tag_out, output_data)
        self._cached_output_table = None
        self._cached_content_hash_column = None

        return tag_out, output_data

    def _build_entry_id_preimage(
        self,
        tag: TagProtocol,
        input_data: DataProtocol,
    ) -> "pa.Table":
        """Builds the shared Arrow preimage used by both entry-ID methods.

        Delegates to the module-level ``_build_record_id_preimage()`` helper.
        The preimage contains system-tag columns and ``INPUT_DATA_HASH_COL``
        (``large_binary``).  ``NODE_CONTENT_HASH_COL`` is excluded — it is
        redundant and fully determined by the table path and system tags.

        Args:
            tag: The tag datagram for the input row.
            input_data: The data datagram for the input row.

        Returns:
            A single-row ``pa.Table`` with system-tag columns and
            ``INPUT_DATA_HASH_COL``.
        """
        return _build_record_id_preimage(tag, input_data)

    def compute_base_entry_id(
        self,
        tag: TagProtocol,
        input_data: DataProtocol,
    ) -> bytes:
        """Computes the stable (recomputation-index-free) entry ID for a (tag, data) pair.

        Hashes the tag's system-tag columns plus ``INPUT_DATA_HASH_COL`` (binary).
        Because it excludes the recomputation index it is stable across all
        recomputation attempts for the same logical input.

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

        The preimage is ``system_tags + INPUT_DATA_HASH_COL + recomputation_index``.

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
        output_data_hash: ContentHash | None = None,
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
        - Whether the result is stored in the ephemeral database
        - Input data context key
        - Whether the result was freshly computed or cached
        - Output data content hash (``OUTPUT_DATA_HASH_COL``) — used by downstream
          ``EmptyData`` flow-through to look up the downstream result cache.

        Args:
            tag: The tag associated with the input data.
            input_data: The input data that was processed.
            data_record_id: UUID of the result record in the result database.
            computed: Whether the result was freshly computed (``True``) or
                served from a cache (``False``).
            is_ephemeral: Whether the result is stored in the ephemeral database.
            output_data_hash: Content hash of the output produced by processing
                ``input_data``. When provided, stored as ``OUTPUT_DATA_HASH_COL``
                so that downstream ``EmptyData`` flow-through can locate its cached
                result keyed by the same hash (= the downstream's ``INPUT_DATA_HASH_COL``).
        """
        self._add_pipeline_record(
            tag,
            input_data,
            self.compute_base_entry_id(tag, input_data),
            data_record_id=data_record_id,
            computed=computed,
            is_ephemeral=is_ephemeral,
            output_data_hash=output_data_hash,
        )

    def _add_pipeline_record(
        self,
        tag: TagProtocol,
        input_data: DataProtocol,
        base_entry_id: bytes,
        data_record_id: uuid.UUID,
        computed: bool,
        is_ephemeral: bool = False,
        output_data_hash: ContentHash | None = None,
    ) -> None:
        """``add_pipeline_record`` with the row's base entry id already computed.

        Args:
            tag: The tag associated with the input data.
            input_data: The input data that was processed.
            base_entry_id: ``compute_base_entry_id(tag, input_data)``.
            data_record_id: UUID of the result record in the result database.
            computed: Whether the result was freshly computed.
            is_ephemeral: Whether the result is stored in the ephemeral database.
            output_data_hash: Content hash of the output, if any.
        """
        pipeline_database = self._require_pipeline_database()
        # The row key combines the input's hash with this node's hasher: an input
        # of another data context would be stored under a mixed key.
        require_same_data_context(
            input_data.data_context_key, self._stored_data_context_key
        )
        self._account_for_stale_tables()

        # Determine the next recomputation index by querying all existing rows
        # for this base_entry_id. No await is used here, so within a single-threaded
        # asyncio event loop this read-then-write sequence is uninterrupted.
        existing = pipeline_database.get_records_with_column_value(
            self._versioned_pipeline_path,
            {
                _PIPELINE_BASE_ENTRY_ID_COL: base_entry_id,
                constants.DATA_CONTEXT_COL: self._stored_data_context_key,
            },
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

        # Build the meta columns table.
        meta_table = pa.table(
            {
                constants.DATA_RECORD_ID: pa.array(
                    [data_record_id.bytes], type=pa.large_binary()
                ),
                constants.NODE_CONTENT_HASH_COL: pa.array(
                    [self.content_hash().to_prefixed_digest()], type=pa.large_binary()
                ),
                constants.INPUT_DATA_HASH_COL: pa.array(
                    [input_data.content_hash().to_prefixed_digest()],
                    type=pa.large_binary(),
                ),
                constants.OUTPUT_DATA_HASH_COL: pa.array(
                    [
                        output_data_hash.to_prefixed_digest()
                        if output_data_hash is not None
                        else None
                    ],
                    type=pa.large_binary(),
                ),
                constants.DATA_CONTEXT_COL: pa.array(
                    [self._stored_data_context_key], type=pa.large_string()
                ),
                f"{constants.META_PREFIX}computed": pa.array(
                    [computed], type=pa.bool_()
                ),
                constants.IS_EPHEMERAL_COL: pa.array([is_ephemeral], type=pa.bool_()),
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

        try:
            pipeline_database.add_record(
                self._versioned_pipeline_path,
                versioned_entry_id,
                combined_record,
                skip_duplicates=True,
            )
        except OrcapodDatabaseReadError:
            # The stored table could not be read: report the read failure itself.
            raise
        except Exception as exc:
            raise OrcapodDatabaseWriteError(
                self._versioned_pipeline_path,
                node_label=self.label,
                tag=tag.as_dict(),
            ) from exc

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
        user-facing result. ``NODE_CONTENT_HASH_COL``, ``_PIPELINE_ENTRY_ID_COL``,
        ``_PIPELINE_BASE_ENTRY_ID_COL``, and ``_PIPELINE_RECOMPUTATION_INDEX_COL``
        are always dropped — they are internal discriminator columns, not
        user-facing data. These columns are listed explicitly to ensure they are
        dropped even when ``all_info=True`` (which skips the meta-prefix sweep).

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

        Raises:
            RuntimeError: If the node was loaded by ``PipelineJob.load`` and no
                database is attached.

        Warns:
            OrcapodDatabaseContentWarning: Once per database, if the node was loaded from a job file
                that records another number of pipeline records for its run than the
                database holds. The database is the source of truth.
        """
        if self._function_pod is None:
            self._require_readable_stub()
        fetched = self._fetch_joined_records()
        if fetched is None:
            return None

        joined = fetched.table
        column_config = ColumnConfig.handle_config(columns, all_info=all_info)

        # Always drop internal discriminator columns regardless of column_config.
        # Listing them explicitly ensures they are dropped even when all_info=True,
        # which skips the meta-prefix sweep.
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
    # Cache-only helpers (PLT-1156)
    # ------------------------------------------------------------------

    def _fetch_joined_records(
        self,
        base_entry_ids: list[bytes] | None = None,
    ) -> _JoinedRecords | None:
        """Internal primitive: fetch both DBs and inner-join, supporting two databases.

        Fetches ``taginfo`` from the pipeline database, partitions rows by
        ``IS_EPHEMERAL_COL`` into persistent and ephemeral groups, performs two
        independent inner joins (one per database), merges with persistent priority
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

        self._account_for_stale_tables()
        taginfo = self._pipeline_database.get_all_records(
            self._versioned_pipeline_path,
            record_id_column=_PIPELINE_ENTRY_ID_COL,
        )

        if taginfo is None:
            return None

        # Rows written under another data context are not this node's current
        # entries: they stay in the table untouched (ITL-944). Result rows are
        # reached only through the pipeline rows kept here.
        context_key = self._stored_data_context_key
        stored_rows = taginfo.num_rows
        taginfo = keep_rows_of_data_context(taginfo, context_key)
        if taginfo.num_rows < stored_rows and not self._skipped_rows_reported:
            self._skipped_rows_reported = True
            report_rows_of_other_data_contexts(
                self._pipeline_database.base_path + self._versioned_pipeline_path,
                stored_rows - taginfo.num_rows,
            )

        taginfo_columns = tuple(taginfo.column_names)
        taginfo_schema = taginfo.schema

        is_ephemeral_col = constants.IS_EPHEMERAL_COL
        taginfo_df = pl.DataFrame(taginfo)

        # Per-node isolation: when sharing a pipeline table in pipeline_hash scope,
        # filter to rows written by this node to prevent cross-node contamination.
        if self._table_scope == "pipeline_hash":
            own_hash = self.content_hash().to_prefixed_digest()
            taginfo_df = taginfo_df.filter(
                pl.col(constants.NODE_CONTENT_HASH_COL) == own_hash
            )

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
            if results is not None:
                # A result row of another data context is not this entry's result,
                # even when a current pipeline row names its record id: it is left
                # out and the entry counts as having no stored result.
                results = keep_rows_of_data_context(results, context_key).drop(
                    [constants.DATA_CONTEXT_COL]
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
        empty_data_tokens: dict[bytes, EmptyData] = {}
        empty_taginfo_rows: dict[bytes, dict] = {}

        if ephemeral_taginfo_df.height > 0:
            eph_results = None
            if self._ephemeral_cached_pod is not None:
                eph_results = (
                    self._ephemeral_cached_pod.result_database.get_all_records(
                        self._ephemeral_cached_pod.record_path,
                        record_id_column=constants.DATA_RECORD_ID,
                    )
                )
            if eph_results is not None:
                eph_results = keep_rows_of_data_context(eph_results, context_key).drop(
                    [constants.DATA_CONTEXT_COL]
                )
                if results_schema is None:
                    results_schema = eph_results.schema
                ephemeral_df = ephemeral_taginfo_df.join(
                    pl.DataFrame(eph_results),
                    on=constants.DATA_RECORD_ID,
                    how="inner",
                )

            # Emit EmptyData tokens for unmatched ephemeral rows
            # (cross-session miss: ephemeral data is gone).
            if eph_results is not None:
                unmatched_df = ephemeral_taginfo_df.join(
                    pl.DataFrame(eph_results),
                    on=constants.DATA_RECORD_ID,
                    how="anti",
                )
            else:
                # No ephemeral database or empty database — all ephemeral rows are misses.
                unmatched_df = ephemeral_taginfo_df
            for row in unmatched_df.iter_rows(named=True):
                base_eid = row[_PIPELINE_BASE_ENTRY_ID_COL]
                previous = empty_taginfo_rows.get(base_eid)
                if (
                    previous is not None
                    and previous[_PIPELINE_RECOMPUTATION_INDEX_COL]
                    > row[_PIPELINE_RECOMPUTATION_INDEX_COL]
                ):
                    continue
                # Use OUTPUT_DATA_HASH_COL so that the downstream result cache
                # lookup in _process_data_internal finds the correct entry.
                # The downstream's result cache is keyed by the INPUT to the
                # downstream (= the OUTPUT of this ephemeral node), so we must
                # use the output hash. INPUT_DATA_HASH_COL is deliberately not
                # used as a fallback — it carries the wrong hash (the upstream's
                # input, not its output) and would silently cause cache misses.
                raw_hash = row.get(constants.OUTPUT_DATA_HASH_COL)
                if raw_hash is None:
                    logger.warning(
                        "Pipeline DB row missing %r column — EmptyData will have "
                        "no cached hash; flow-through unavailable for this row. "
                        "base_entry_id: %r",
                        constants.OUTPUT_DATA_HASH_COL,
                        base_eid,
                    )
                    cached_hash = None
                else:
                    cached_hash = ContentHash.from_prefixed_digest(raw_hash)
                empty_data_tokens[base_eid] = EmptyData(
                    cached_content_hash=cached_hash,
                    data_context=self.data_context,
                )
                empty_taginfo_rows[base_eid] = row

        # ------------------------------------------------------------------
        # Keep both databases' rows until latest-recomputation selection. Persistent
        # rows come last so they retain priority when recomputation indices tie.
        # ------------------------------------------------------------------
        if ephemeral_df.height > 0 and persistent_df.height > 0:
            merged_df = pl.concat([ephemeral_df, persistent_df], how="diagonal")
        elif ephemeral_df.height > 0:
            merged_df = ephemeral_df
        elif persistent_df.height > 0:
            merged_df = persistent_df
        else:
            # No results found in either database — return empty table preserving taginfo schema
            empty_table = taginfo.slice(0, 0)
            return _JoinedRecords(
                table=empty_table,
                taginfo_columns=taginfo_columns,
                empty_data_tokens=empty_data_tokens,
                empty_taginfo_rows=empty_taginfo_rows,
            )

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
        # In memory, each row names its data context in the datagram context column.
        joined = joined.append_column(
            pa.field(constants.CONTEXT_KEY, pa.large_string(), nullable=False),
            pa.array([context_key] * joined.num_rows, type=pa.large_string()),
        )
        return _JoinedRecords(
            table=joined,
            taginfo_columns=taginfo_columns,
            empty_data_tokens=empty_data_tokens,
            empty_taginfo_rows=empty_taginfo_rows,
        )

    def _stored_tag_keys(self, fetched: _JoinedRecords) -> tuple[str, ...]:
        """Return the user tag columns of the records in *fetched*.

        Prefers the input stream's tag keys; falls back to excluding system columns
        from the pipeline records for CACHE_ONLY / deserialized nodes.
        """
        if self._input_stream is not None:
            return self._input_stream.keys()[0]
        return tuple(
            c
            for c in fetched.taginfo_columns
            if not c.startswith(constants.META_PREFIX)
            and not c.startswith(constants.SOURCE_PREFIX)
            and not c.startswith(constants.SYSTEM_TAG_PREFIX)
            and c != _PIPELINE_ENTRY_ID_COL
            and c != constants.NODE_CONTENT_HASH_COL
        )

    def _stored_rows_stream(
        self, fetched: _JoinedRecords
    ) -> tuple[list[bytes], ArrowTableStream, list[uuid.UUID]] | None:
        """Return the persistent stored rows of *fetched* as a stream.

        The stream is built over the stored Arrow records with the internal
        columns dropped, so every column keeps the type it was stored with. A
        base entry with several records contributes only the one with the
        greatest recomputation index.

        Returns:
            The base entry ID of each row, in stream order, the stream, and the
            stored ``__data_id`` of each row (the id of the result record, in
            stream order); or ``None`` when no persistent row is stored.
        """
        joined = fetched.table
        if joined.num_rows == 0:
            return None
        all_base_entry_ids = cast(
            list[bytes], joined.column(_PIPELINE_BASE_ENTRY_ID_COL).to_pylist()
        )
        # One row per base entry ID: when a base entry has several records (it
        # was recomputed), the one with the greatest recomputation index -- the
        # latest result -- wins, in the position where the base entry first
        # appears. Ties keep the later row.
        recomputation_indices = cast(
            list[int], joined.column(_PIPELINE_RECOMPUTATION_INDEX_COL).to_pylist()
        )
        chosen_row: dict[bytes, int] = {}
        for row, base_entry_id in enumerate(all_base_entry_ids):
            current = chosen_row.get(base_entry_id)
            if (
                current is None
                or recomputation_indices[row] >= recomputation_indices[current]
            ):
                chosen_row[base_entry_id] = row
        chosen_row = {
            base_eid: row
            for base_eid, row in chosen_row.items()
            if base_eid not in fetched.empty_taginfo_rows
            or recomputation_indices[row]
            >= fetched.empty_taginfo_rows[base_eid][_PIPELINE_RECOMPUTATION_INDEX_COL]
        }
        if not chosen_row:
            return None
        base_entry_ids = list(chosen_row)
        if len(base_entry_ids) < joined.num_rows:
            joined = joined.take(pa.array(list(chosen_row.values()), type=pa.int64()))
        drop_cols = [
            c
            for c in joined.column_names
            if c.startswith(constants.META_PREFIX)
            or c == constants.NODE_CONTENT_HASH_COL
        ]
        data_ids = [
            uuid.UUID(bytes=bytes(cast(bytes, record_id)))
            for record_id in joined.column(constants.DATA_RECORD_ID).to_pylist()
        ]
        stream = ArrowTableStream(
            joined.drop(drop_cols), tag_columns=self._stored_tag_keys(fetched)
        )
        return base_entry_ids, stream, data_ids

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
        if fetched is None:
            return {}
        if fetched.table.num_rows == 0 and not fetched.empty_data_tokens:
            return {}

        tag_keys = self._stored_tag_keys(fetched)
        loaded: dict[bytes, tuple[TagProtocol, DataProtocol]] = {}

        # --- Process normal (fully matched) rows ---
        stored = self._stored_rows_stream(fetched)
        if stored is not None:
            base_entry_ids_col, stream, data_ids = stored
            for base_eid, data_id, (tag, data) in zip(
                base_entry_ids_col, data_ids, stream.iter_data(), strict=True
            ):
                # A stored result is the datagram its own provenance names: it
                # carries the stored ``__data_id`` as its datagram id (ITL-1060).
                loaded[base_eid] = (tag, _with_datagram_id(data, data_id))

        # --- Process EmptyData tokens (ephemeral misses) ---
        for base_eid, empty_data in fetched.empty_data_tokens.items():
            if base_eid in loaded:
                # A matched result at least as recent as the missing one wins.
                continue
            raw_row = fetched.empty_taginfo_rows[base_eid]
            tag_data = {k: v for k, v in raw_row.items() if k in tag_keys}
            system_tags = {
                k: v
                for k, v in raw_row.items()
                if k.startswith(constants.SYSTEM_TAG_PREFIX)
            }
            tag = Tag(tag_data, system_tags=system_tags, data_context=self.data_context)
            loaded[base_eid] = (tag, empty_data)

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

            sent: list[bytes] = []
            for entry_id, (tag, data) in list(self._cached_output_datas.items()):
                if data is not None and not isinstance(data, EmptyData):
                    ctx_obs.on_data_start(node_label, tag, data)
                    ctx_obs.on_data_end(node_label, tag, data, data, cached=True)
                    await output.send((tag, data))
                    sent.append(entry_id)
            # The node holds the rows it sent: a completed load of what is stored
            # (ITL-1095), stamped before the output closes (ITL-1042).
            self._set_run_rows(sent, load_only=True)
            self._update_modified_time()
            ctx_obs.on_node_end(node_label, node_hash)
        except BaseException:
            self.clear_cache()
            raise
        finally:
            try:
                await output.close()
            except BaseException:
                self.clear_cache()
                raise

    # ------------------------------------------------------------------
    # Head
    # ------------------------------------------------------------------

    def head(
        self,
        n: int = 10,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> StreamView:
        """Return the first rows of the node's current run; never runs the function.

        After a run (or ``execute``) the rows are the node's results in memory for that
        run's inputs, the first of what ``as_table`` returns. A node that has not run in
        this process, or whose input changed since (stale), shows no rows and says so,
        pointing to ``run()`` and to ``as_source()`` for its stored results (ITL-1095);
        the database is not read. The node has all of its rows, so with ``sort_by_tags``
        (or ``all_info``) the head is exactly ``as_table(...).slice(0, n)``, whatever
        order the rows were computed in. See ``StreamBase.head``.
        """
        from orcapod.pipeline.serialization import LoadStatus

        check_head_size(n)
        config = ColumnConfig.handle_config(columns, all_info=all_info)
        status = self.load_status
        if status == LoadStatus.UNAVAILABLE:
            return self._head_without_rows(
                config, ViewStatus.UNAVAILABLE, note="no database attached"
            )
        if self._run_entry_counts is None:
            return self._head_without_rows(
                config, ViewStatus.NOT_COMPUTED, note=JOB_NODE_NOT_RUN_NOTE
            )
        if status != LoadStatus.CACHE_ONLY and self.is_stale:
            return self._head_without_rows(
                config, ViewStatus.NOT_COMPUTED, note=STALE_NOTE
            )
        head = self._head_of_pairs(list(self._held_pairs()), n, config, origin="held")
        if not self._run_load_only:
            return head
        # A load-only run's rows have their own status: they may lack the inputs it
        # skipped (ITL-1095).
        note = self._load_only_note()
        if head.note:
            note += f"; {head.note}"
        return dataclasses.replace(head, status=ViewStatus.LOADED_ONLY, note=note)

    # ------------------------------------------------------------------
    # Iteration
    # ------------------------------------------------------------------

    def as_table(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> "pa.Table":
        """Return the rows of this node's current run as a PyArrow table.

        Read-only: never computes and never reads the database. Before a run in this
        process, or once the node is stale, the table has zero rows and the node's
        columns (ITL-1095); ``as_source()`` reads what the database holds.

        Raises:
            RuntimeError: If the node was loaded and no database is attached.
        """
        from orcapod.pipeline.serialization import LoadStatus

        if self.load_status == LoadStatus.UNAVAILABLE:
            raise self._no_database_error()
        return super().as_table(columns=columns, all_info=all_info)

    def iter_data(self) -> Iterator[tuple[TagProtocol, DataProtocol]]:
        """Yield the (tag, data) rows of this node's current run.

        Strictly read-only: never computes and never reads the database. Callers must
        call ``run()`` (or run the job) first.

        After a completed run (``execute``, ``async_execute``, ``run()`` or a job run)
        the rows are exactly those of that run's inputs, on either orchestrator, even
        though results of other inputs stay cached and stored (ITL-1093). A node that
        has not run in this process, whose run raised, or that is stale (its input
        changed since its run) yields nothing (ITL-1095); ``as_source()`` reads every
        row stored for it.

        Raises:
            RuntimeError: If ``load_status`` is UNAVAILABLE.
        """
        from orcapod.pipeline.serialization import LoadStatus

        status = self.load_status
        if status == LoadStatus.UNAVAILABLE:
            raise self._no_database_error()
        # A CACHE_ONLY node has no live input to be stale against.
        if status != LoadStatus.CACHE_ONLY and self.is_stale:
            self.clear_cache()
        yield from self._held_pairs()

    def run(self, *, load_only: bool = False) -> StreamView:
        """Run the node as a job run would, and return a ``StreamView`` of its rows.

        State-driven (developer, November 13, 2026; ITL-1095): a node that holds the rows
        of a completed run and is not stale is not run again; the view shows what it
        holds, and nothing is computed, read or written. Otherwise the node runs, after
        every job node it depends on that holds no current run (each once, in
        dependency order, as ``job.run()`` would run them; ``last_run`` reports the
        counts), and the run's writes are committed. A node built over streams outside a
        job reads its input as it is.

        Args:
            load_only: Fill the node, and the nodes it runs first, with what is already
                stored and compute nothing (as ``job.run(load_only=True)``). A node
                filled this way shows status ``loaded_only`` and is not current for a
                later full ``run()``, which computes what it skipped.

        Returns:
            A ``StreamView`` of every row ``as_table()`` then returns: status ``rows``,
            or ``loaded_only`` after a load-only run.

        Raises:
            RuntimeError: If no database is attached (``UNAVAILABLE``); if the node cannot
                compute (loaded without a function pod, or a placeholder pod) and
                ``load_only`` is false; or if its input is unavailable
                (``CACHE_ONLY``): read its stored rows with ``as_source()``.
        """
        from orcapod.pipeline.serialization import LoadStatus

        status = self._load_status
        if status == LoadStatus.UNAVAILABLE:
            raise self._no_database_error()
        if status == LoadStatus.CACHE_ONLY:
            raise RuntimeError(
                f"FunctionJobNode {self.label!r}: its input is unavailable, so it has "
                "no inputs to run or look up; read its stored results with as_source()."
            )
        if not load_only and (
            self._function_pod is None or status == LoadStatus.READ_ONLY
        ):
            raise self._cannot_compute_error()
        run_job_node(
            self,
            load_only=load_only,
            run_alone=lambda unchecked: self._run_alone(
                load_only=load_only, unchecked_sources=unchecked
            ),
            caller=sys._getframe(1),  # pyright: ignore[reportPrivateUsage]  # the caller's line is where a failure warning is shown
        )
        return self._run_view()

    def _run_alone(
        self, *, load_only: bool, unchecked_sources: tuple[str, ...] = ()
    ) -> Any:
        """Run this node over its input stream, which is not a job node; commit.

        Args:
            load_only: Whether to load stored results only.
            unchecked_sources: The sources the start-of-run check could not check.

        Returns:
            The run's ``RunSummary``.
        """
        from dataclasses import replace

        from orcapod.pipeline.node_run import commit_run
        from orcapod.pipeline.run_summary import RunCountingObserver, RunSummary
        from orcapod.utils.database_errors import flush_best_effort

        if self.is_stale:
            # Discard any stale in-memory entries before a fresh computation run
            # so that rerunning does not mix old cached entries with new results.
            self.clear_cache()
        counter = RunCountingObserver()
        databases = [
            database
            for database in (
                self._pipeline_database,
                getattr(self._cached_function_pod, "result_database", None),
            )
            if database is not None
        ]
        try:
            _ = self.execute(self._input_stream, observer=counter, load_only=load_only)
        except Exception as run_error:
            if not load_only:
                # Commit what succeeded; a load-only run writes nothing (ITL-1095).
                flush_best_effort(run_error, *databases)
            self.clear_cache()
            raise
        if not load_only:
            commit_run(databases, [self])
        node_summary = replace(
            counter.node_summary(self.label), not_stored=self.last_run_not_stored
        )
        base_path = getattr(self._pipeline_database, "base_path", ())
        return RunSummary(
            "/".join(base_path),
            [node_summary],
            load_only=load_only,
            target=self.label,
            unchecked_sources=unchecked_sources,
        )

    def _incomplete_inputs(self) -> list[str]:
        """Labels of the function nodes above this one whose load-only run skipped inputs."""
        labels: list[str] = []
        seen: list[Any] = []
        pending: list[Any] = list(self.upstreams)
        while pending:
            node = pending.pop()
            if any(node is other for other in seen):
                continue
            seen.append(node)
            if isinstance(node, FunctionJobNode) and node._run_load_only:
                if node._run_not_stored:
                    labels.append(node.label)
            pending.extend(getattr(node, "upstreams", ()))
        return sorted(labels)

    def _load_only_note(self) -> str:
        """The caption of a node filled by a load-only run (ITL-1095)."""
        note = (
            f"loaded only: {self._run_loaded_count():,} loaded, "
            f"{self._run_not_stored:,} inputs without a stored result"
        )
        incomplete = self._incomplete_inputs()
        if incomplete:
            note += f"; inputs incomplete: {', '.join(incomplete)}"
        return note

    def _run_loaded_count(self) -> int:
        """How many inputs of the last run had a stored result (loaded)."""
        counts = self._run_entry_counts
        return 0 if counts is None else sum(counts.values())

    def _run_view(self) -> StreamView:
        """The ``run()`` view of the node's rows, with the status of its last run."""
        table = self.as_table()
        if not self._run_load_only:
            # The rows are the ones held in memory, as head() names them afterwards:
            # some may have been read from the result database rather than computed.
            return self._view_of_rows(table, "held")
        return StreamView.of_rows(
            self,
            table,
            tag_columns=self.keys()[0],
            total_rows=table.num_rows,
            status=ViewStatus.LOADED_ONLY,
            note=self._load_only_note(),
            display_all_rows=False,
        )

    # ------------------------------------------------------------------
    # Async channel execution
    # ------------------------------------------------------------------

    async def async_execute(
        self,
        input_channel: ReadableChannel[tuple[TagProtocol, DataProtocol]],
        output: WritableChannel[tuple[TagProtocol, DataProtocol]],
        *,
        observer: ExecutionObserverProtocol | None = None,
        error_policy: Literal["continue", "fail_fast"] = "continue",
        run_id: str | None = None,
        load_only: bool = False,
    ) -> None:
        """Streaming async execution for FunctionJobNode.

        When a database is attached, uses two-phase execution: replay cached
        results first, then compute missing data concurrently.  Otherwise,
        routes each data through ``async_process_data`` directly.

        In ``CACHE_ONLY`` mode the upstream is unavailable; all cached results
        are served directly from persistent storage without touching the input
        channel.

        Afterwards the node's rows are those of this run's inputs, as after
        ``execute``, although every stored result was loaded into its cache. If the
        run raises or is cancelled, it defines no rows: the node drops its in-memory
        rows and reads as a node with no run in this process.

        Args:
            input_channel: Single readable channel of (tag, data) pairs.
            output: Writable channel for output (tag, data) pairs.
            observer: Optional execution observer for hooks.
            error_policy: ``"continue"`` skips rows whose function raised;
                ``"fail_fast"`` raises the exception of the first row whose function
                raised, as ``execute`` does, and cancels the rows still in flight.
                Rows run concurrently, so that is the first row to fail, not
                necessarily the first in input order.
            run_id: Optional pipeline run identifier threaded through to
                ``InvocationContext.pipeline_run_id``.
            load_only: As for ``execute``: fill the node with the stored results of
                its input rows and compute nothing (ITL-1095).

        Raises:
            Exception: Under ``"fail_fast"``, the exception of the first row whose
                function raised, inside the ``ExceptionGroup`` of the node's task
                group. *output* is closed first.
            RuntimeError: If the node was loaded by ``PipelineJob.load``: without a
                store it has nothing to serve, and it has no function pod to
                compute with. *output* is closed first.
        """
        from orcapod.pipeline.serialization import LoadStatus

        if load_only:
            await self._async_execute_load_only(input_channel, output, observer)
            return
        try:
            self._begin_run()
        except OrcapodDatabaseError:
            # Nothing was delivered; downstream must not wait for rows. A run that
            # raised defines no rows.
            self.clear_cache()
            await output.close()
            raise
        status = self.load_status
        if status == LoadStatus.CACHE_ONLY:
            await self._async_execute_cache_only(output, observer=observer)
            return

        if status == LoadStatus.UNAVAILABLE:
            await output.close()
            raise self._no_database_error()

        if self._function_pod is None:
            await output.close()
            raise self._cannot_compute_error()

        from orcapod.pipeline.observer import NoOpObserver

        # A whole-input condition, not a row failure: the error policy does not apply.
        if self._input_stream is not None:
            try:
                require_same_data_context(
                    self._input_stream.data_context_key, self._stored_data_context_key
                )
            except InputValidationError:
                # A run that raised defines no rows, also when it was refused at once.
                self.clear_cache()
                await output.close()
                raise
        node_label = self.label
        node_hash = self.content_hash().to_string()

        obs = observer if observer is not None else NoOpObserver()
        ctx_obs = obs.contextualize(*self.node_identity_path)

        # Whether the node has started for the observer and not yet ended.
        node_open = False
        try:
            tag_schema = self._input_stream.output_schema(
                columns={"system_tags": True}
            )[0]
            ctx_obs.on_node_start(node_label, node_hash, tag_schema=tag_schema)
            node_open = True

            if self._cached_function_pod is not None:
                # ----------------------------------------------------------
                # DB path — 3-stage concurrent pipeline
                # ----------------------------------------------------------
                is_ephemeral = bool(self._node_config.is_result_ephemeral)
                if is_ephemeral:
                    if self._ephemeral_cached_pod is None:
                        raise RuntimeError(
                            f"FunctionJobNode '{self.label}' has is_result_ephemeral=True "
                            "but no ephemeral database has been assigned. Call "
                            "set_ephemeral_database() with an ArrowDatabaseProtocol before "
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
                cached_by_base_entry_id: dict[
                    bytes, tuple[TagProtocol, DataProtocol]
                ] = dict(loaded)

                # Intermediate channels (bounded for backpressure).
                compute_channel: Channel[tuple[TagProtocol, DataProtocol]] = Channel(
                    buffer_size=16
                )
                result_channel: Channel[tuple[TagProtocol, DataProtocol]] = Channel(
                    buffer_size=16
                )

                # Local dict: correlation_key → (original_tag, original_input_data)
                input_store: dict[bytes, tuple[TagProtocol, DataProtocol, bytes]] = {}
                # Base entry ids of this run's inputs that gave a row (a cache hit or a
                # forwarded computed row), one per input (ITL-1093).
                run_entry_ids: list[bytes] = []
                # Rows whose function raised (error policy "continue").
                failed_rows: list[int] = [0]

                async def route_inputs() -> None:
                    """Stage 1: send cache hits to output; stamp misses for computation."""
                    try:
                        async for tag, data in input_channel:
                            base_entry_id = self.compute_base_entry_id(tag, data)
                            # A cached EmptyData is a result that is gone (an
                            # ephemeral result from an earlier run): compute the row
                            # again, as the sync path does (ITL-923).
                            if base_entry_id in cached_by_base_entry_id and not (
                                isinstance(
                                    cached_by_base_entry_id[base_entry_id][1],
                                    EmptyData,
                                )
                            ):
                                cached_tag, cached_data = cached_by_base_entry_id[
                                    base_entry_id
                                ]
                                ctx_obs.on_data_start(node_label, tag, data)
                                ctx_obs.on_data_end(
                                    node_label, tag, data, cached_data, cached=True
                                )
                                run_entry_ids.append(base_entry_id)
                                await output.send((cached_tag, cached_data))
                            else:
                                correlation_key = uuid.uuid4().bytes
                                input_store[correlation_key] = (
                                    tag,
                                    data,
                                    base_entry_id,
                                )
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
                        original_tag, input_data, base_entry_id = input_store.pop(
                            cast(bytes, correlation_key)
                        )
                        result_computed = bool(
                            output_data.get_meta_value(
                                execution_pod.RESULT_COMPUTED_FLAG, False
                            )
                        )
                        try:
                            self._add_pipeline_record(
                                original_tag,
                                input_data,
                                base_entry_id,
                                data_record_id=output_data.datagram_uuid,
                                computed=result_computed,
                                is_ephemeral=is_ephemeral,
                                output_data_hash=output_data.content_hash(),
                            )
                        except OrcapodDatabaseError as exc:
                            # This runs outside the pod's per-row handler: name the
                            # node and row here.
                            exc.add_context(
                                node_label=node_label, tag=original_tag.as_dict()
                            )
                            raise
                        # Update in-memory cache so iter_data() sees the result.
                        clean_tag = output_tag.drop_meta_columns(
                            _TAG_NODE_INPUT_REF, ignore_missing=True
                        )
                        self._cached_output_datas[base_entry_id] = (
                            clean_tag,
                            output_data,
                        )
                        self._cached_output_table = None
                        self._cached_content_hash_column = None
                        # Commits every commit_every_rows rows inside a job (ITL-919);
                        # no await in between, so no other row interleaves.
                        self._row_processed()
                        run_entry_ids.append(base_entry_id)
                        await output.send((clean_tag, output_data))

                # Capture outer self so _NodeLabelObserver methods can
                # update FunctionJobNode state without shadowing with 'self'.
                fn_node = self

                # Wrap ctx_obs so that data-level events emitted by the pod
                # carry the node label, not the pod's own label — preserving
                # the observable contract of the old DB path.
                class _NodeLabelObserver:
                    """Relay all observer calls, replacing any label with node_label."""

                    def contextualize(self, *path: str) -> "_NodeLabelObserver":
                        return self

                    def on_run_start(self, run_id: str, pipeline_uri: str = "") -> None:
                        ctx_obs.on_run_start(run_id, pipeline_uri)

                    def on_run_end(self, run_id: str) -> None:
                        ctx_obs.on_run_end(run_id)

                    def on_node_start(self, lbl: str, h: str, **kw: Any) -> None:
                        ctx_obs.on_node_start(node_label, h, **kw)

                    def on_node_end(self, lbl: str, h: str) -> None:
                        ctx_obs.on_node_end(node_label, h)

                    def on_data_start(self, lbl: str, tag: Any, data: Any) -> None:
                        clean = tag.drop_meta_columns(
                            _TAG_NODE_INPUT_REF, ignore_missing=True
                        )
                        ctx_obs.on_data_start(node_label, clean, data)

                    def on_data_end(
                        self, lbl: str, tag: Any, inp: Any, out: Any, *, cached: bool
                    ) -> None:
                        clean = tag.drop_meta_columns(
                            _TAG_NODE_INPUT_REF, ignore_missing=True
                        )
                        ctx_obs.on_data_end(node_label, clean, inp, out, cached=cached)
                        if out is None:
                            # The function filtered this item (returned None).
                            # The pod does not emit to result_channel, so
                            # record_and_forward() never sees it.  Pop the
                            # input_store entry and update the in-memory cache
                            # so iter_data() (the sync path) sees a cached None
                            # result — restoring the old _async_process_data_internal
                            # behaviour of always writing (tag, None) to
                            # _cached_output_datas.
                            # Note: route_inputs() checks cached_by_base_entry_id
                            # (pipeline DB) and filtered items are never written
                            # to the pipeline DB, so subsequent async_execute()
                            # calls still re-send filtered inputs to the pod.
                            correlation_key = tag.get_meta_value(
                                _TAG_NODE_INPUT_REF, None
                            )
                            if correlation_key is not None:
                                original = input_store.pop(correlation_key, None)
                                if original is not None:
                                    _, _, base_entry_id = original
                                    fn_node._cached_output_datas[base_entry_id] = (
                                        clean,
                                        None,
                                    )
                                    fn_node._cached_output_table = None
                                    fn_node._cached_content_hash_column = None

                    def on_data_crash(
                        self, lbl: str, tag: Any, data: Any, error: Any
                    ) -> None:
                        clean = tag.drop_meta_columns(
                            _TAG_NODE_INPUT_REF, ignore_missing=True
                        )
                        if isinstance(error, OrcapodDatabaseError):
                            # The pod re-raises a failed database read or write after
                            # this call; name the node it belongs to.
                            error.add_context(node_label=node_label)
                        else:
                            failed_rows[0] += 1
                        ctx_obs.on_data_crash(node_label, clean, data, error)
                        # Clean up the dangling input_store entry so it doesn't
                        # accumulate memory during long-running calls with errors.
                        correlation_key = tag.get_meta_value(_TAG_NODE_INPUT_REF, None)
                        if correlation_key is not None:
                            input_store.pop(correlation_key, None)

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
                            error_policy=error_policy,
                            run_id=run_id,
                        )
                    )
                    tg.create_task(record_and_forward())
                # Commit the last rows inside a job (ITL-919); a no-op otherwise.
                self._commit_at_node_end()
                # The node's rows are those of this run's inputs, as the sync path's:
                # the cache also holds every stored result loaded above.
                self._set_run_rows(run_entry_ids, failed=failed_rows[0])
            else:
                # No-DB path: delegate dispatch entirely to the pod. The pod closes
                # the writer it is given; that close waits for the finally below, so
                # the node is stamped before its downstream nodes see the end.
                await self._function_pod.async_execute(
                    [input_channel],
                    _CloseDeferredWriter(output),
                    observer=ctx_obs,
                    error_policy=error_policy,
                    run_id=run_id,
                )

            # The run is complete: stamp the node as the sync execute() does, before
            # the observer hears the end (a read there sees the rows, ITL-1121) and
            # before the finally closes the output, so every downstream node, which
            # stamps after it has seen that close, is newer than this one (ITL-1042).
            self._update_modified_time()
            # Ended for the observer here, also when the call raises: not again below.
            node_open = False
            ctx_obs.on_node_end(node_label, node_hash)
        except asyncio.CancelledError as cancelled:
            # Another node's failure cancelled this one: commit what it wrote, as a
            # failing node does. The job's flush commits neither a node's ephemeral
            # database nor its result table ahead of its pipeline records.
            self._commit_after_failure(cancelled)
            # A cancelled run defines no rows: the node reads as one with no run.
            self.clear_cache()
            raise
        except Exception as run_error:
            try:
                if node_open:
                    # The node ends for the observer before it raises, as in execute;
                    # a failing observer must not replace a database error on its way out.
                    with keep_database_error(
                        database_error_of(run_error), "Observer on_node_end"
                    ):
                        ctx_obs.on_node_end(node_label, node_hash)
            finally:
                # Also when the observer raised instead, as execute commits then.
                self._commit_after_failure(run_error)
                # A run that raised defines no rows: the node reads as one with no
                # run, as on the sync path.
                self.clear_cache()
            raise
        except BaseException:
            # Cancelled part-way: no rows either.
            self.clear_cache()
            raise
        finally:
            try:
                await output.close()
            except BaseException:
                # A close that raises or is cancelled fails the run: no rows.
                self.clear_cache()
                raise
