"""OperatorNode — stream node for operator invocations with optional DB persistence."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Iterator, Sequence
from typing import TYPE_CHECKING, Any, Literal

from orcapod import contexts
from orcapod.channels import Channel, ReadableChannel, WritableChannel
from orcapod.config import Config
from orcapod.core.operators.static_output_pod import StaticOutputOperatorPod
from orcapod.core.streams.arrow_table_stream import ArrowTableStream
from orcapod.core.streams.base import StreamBase
from orcapod.core.tracker import DEFAULT_TRACKER_MANAGER
from orcapod.protocols.core_protocols import (
    PacketProtocol,
    StreamProtocol,
    TagProtocol,
    TrackerManagerProtocol,
)
from orcapod.protocols.core_protocols.operator_pod import OperatorPodProtocol
from orcapod.protocols.database_protocols import ArrowDatabaseProtocol
from orcapod.system_constants import constants
from orcapod.types import CacheMode, ColumnConfig, ContentHash, Schema
from orcapod.utils.lazy_module import LazyModule

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    import pyarrow as pa
    import pyarrow.compute as pc

    from orcapod.protocols.observability_protocols import ExecutionObserverProtocol
else:
    pa = LazyModule("pyarrow")
    pc = LazyModule("pyarrow.compute")


class OperatorNode(StreamBase):
    """Stream node representing an operator invocation with optional DB persistence.

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

    node_type = "operator"
    HASH_COLUMN_NAME = "_record_hash"

    def __init__(
        self,
        operator: OperatorPodProtocol,
        input_streams: tuple[StreamProtocol, ...] | list[StreamProtocol],
        tracker_manager: TrackerManagerProtocol | None = None,
        label: str | None = None,
        config: Config | None = None,
        # Optional DB params for persistent mode:
        pipeline_database: ArrowDatabaseProtocol | None = None,
        cache_mode: CacheMode = CacheMode.OFF,
        table_scope: Literal["pipeline_hash", "content_hash"] = "pipeline_hash",
    ):
        if tracker_manager is None:
            tracker_manager = DEFAULT_TRACKER_MANAGER
        self.tracker_manager = tracker_manager

        self._operator = operator
        self._input_streams = tuple(input_streams)

        super().__init__(label=label, config=config)

        # Validate inputs eagerly
        self._operator.validate_inputs(*self._input_streams)

        # Stream-level caching state
        self._cached_output_stream: StreamProtocol | None = None
        self._cached_output_table: pa.Table | None = None
        self._set_modified_time(None)

        # DB persistence state (initially None; set via __init__ params or attach_databases)
        self._pipeline_database: ArrowDatabaseProtocol | None = None
        self._cache_mode = CacheMode.OFF

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
        self._table_scope: Literal["pipeline_hash", "content_hash"] = table_scope
        self._node_identity_path_cache: tuple[str, ...] | None = None

        if pipeline_database is not None:
            self.attach_databases(
                pipeline_database=pipeline_database,
                cache_mode=cache_mode,
            )

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
        self._pipeline_database = pipeline_database
        self._cache_mode = cache_mode

        # Clear caches
        self._node_identity_path_cache = None
        self.clear_cache()
        self._content_hash_cache.clear()
        self._pipeline_hash_cache.clear()

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
    ) -> "OperatorNode":
        """Construct an OperatorNode from a serialized descriptor.

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

        Returns:
            A new ``OperatorNode`` instance.
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
            node._load_status = LoadStatus.FULL
            return node

        # Read-only mode: bypass __init__, set minimum required state
        node = cls.__new__(cls)

        # From LabelableMixin
        node._label = descriptor.get("label")

        # From DataContextMixin
        from orcapod.config import DEFAULT_CONFIG

        node._data_context = contexts.resolve_context(
            descriptor.get("data_context_key")
        )
        node._orcapod_config = DEFAULT_CONFIG

        # From ContentIdentifiableBase
        node._content_hash_cache = {}
        node._cached_int_hash = None

        # From PipelineElementBase
        node._pipeline_hash_cache = {}

        # From TemporalMixin
        node._modified_time = None

        # From OperatorNode
        node._operator = None
        node._input_streams = ()
        node.tracker_manager = DEFAULT_TRACKER_MANAGER
        node._cached_output_stream = None
        node._cached_output_table = None

        # DB persistence state
        node._pipeline_database = pipeline_db
        node._cache_mode = cache_mode

        # Descriptor metadata for read-only access
        node._descriptor = descriptor
        node._stored_schema = descriptor.get("output_schema", {})
        node._stored_content_hash = descriptor.get("content_hash")
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
        return (self._operator, self._operator.argument_symmetry(self._input_streams))

    def pipeline_identity_structure(self) -> Any:
        return (self._operator, self._operator.argument_symmetry(self._input_streams))

    # ------------------------------------------------------------------
    # Read-only overrides (for deserialized nodes without live operator)
    # ------------------------------------------------------------------

    def content_hash(self, hasher=None) -> ContentHash:
        """Return the content hash, using stored value in read-only mode."""
        if self._operator is None and self._stored_content_hash is not None:
            return ContentHash.from_string(self._stored_content_hash)
        return super().content_hash(hasher)

    def pipeline_hash(self, hasher=None) -> ContentHash:
        """Return the pipeline hash, using stored value in read-only mode."""
        if self._operator is None and self._stored_pipeline_hash is not None:
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
        return contexts.resolve_context(self._operator.data_context_key)

    @property
    def data_context_key(self) -> str:
        return self._operator.data_context_key

    @property
    def upstreams(self) -> tuple[StreamProtocol, ...]:
        return self._input_streams

    @upstreams.setter
    def upstreams(self, value: tuple[StreamProtocol, ...]) -> None:
        self._input_streams = value

    def keys(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        if self._operator is None:
            tag_keys = tuple(self._stored_schema.get("tag", {}).keys())
            packet_keys = tuple(self._stored_schema.get("packet", {}).keys())
            return tag_keys, packet_keys
        tag_schema, packet_schema = self.output_schema(
            columns=columns, all_info=all_info
        )
        return tuple(tag_schema.keys()), tuple(packet_schema.keys())

    def output_schema(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        """Return output schema, using stored value in read-only mode."""
        if self._operator is None:
            tag = Schema(self._stored_schema.get("tag", {}))
            packet = Schema(self._stored_schema.get("packet", {}))
            return tag, packet
        return self._operator.output_schema(
            *self._input_streams,
            columns=columns,
            all_info=all_info,
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
        if self._table_scope == "pipeline_hash":
            path = self._operator.uri + (
                f"schema:{self.pipeline_hash().to_string()}",
            )
        else:
            path = self._operator.uri + (
                f"schema:{self.pipeline_hash().to_string()}",
                f"instance:{self.content_hash().to_string()}",
            )
        self._node_identity_path_cache = path
        return path

    def _filter_by_content_hash(self, table: "pa.Table") -> "pa.Table":
        """Filter *table* to rows whose ``_node_content_hash`` matches this node.

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
        own_hash = self.content_hash().to_string()
        mask = pc.equal(table.column(col_name), own_hash)
        return table.filter(mask)

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
    # Computation and caching
    # ------------------------------------------------------------------

    def clear_cache(self) -> None:
        """Discard all in-memory cached state."""
        self._cached_output_stream = None
        self._cached_output_table = None
        self._node_identity_path_cache = None
        self._update_modified_time()

    def _store_output_stream(self, stream: StreamProtocol) -> None:
        """Materialize stream and store in the pipeline database with per-row dedup."""
        output_table = stream.as_table(
            columns={"source": True, "system_tags": True},
        )

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
        output_table = output_table.append_column(
            constants.NODE_CONTENT_HASH_COL,
            pa.repeat(self.content_hash().to_string(), n_rows).cast(pa.large_string()),
        )

        # Per-row record hashes for dedup: hash(tag + packet + system_tags + node_content_hash).
        arrow_hasher = self.data_context.arrow_hasher
        record_hashes = []
        for batch in output_table.to_batches():
            for i in range(len(batch)):
                record_hashes.append(arrow_hasher.hash_table(batch.slice(i, 1)).to_hex())

        output_table = output_table.add_column(
            0,
            self.HASH_COLUMN_NAME,
            pa.array(record_hashes, type=pa.large_string()),
        )

        # Store — record IDs are run-scoped (NODE_CONTENT_HASH_COL is included in
        # the hash), so rows from different runs with identical output will have
        # distinct record IDs and both be stored.  skip_duplicates=True still
        # deduplicates exact re-runs of the same node within a single run.
        self._pipeline_database.add_records(
            self.node_identity_path,
            output_table,
            record_id_column=self.HASH_COLUMN_NAME,
            skip_duplicates=True,
        )

        self._cached_output_table = output_table.drop(
            [self.HASH_COLUMN_NAME, constants.NODE_CONTENT_HASH_COL]
        )

    def _make_empty_table(self) -> "pa.Table":
        """Build a zero-row PyArrow table matching this node's full output schema.

        Uses ``output_schema()`` for column names/types and
        ``data_context.type_converter`` for the Python → Arrow type mapping.
        Requires ``self._operator is not None`` (pre-existing limitation shared
        with ``_replay_from_cache``).
        """
        tag_schema, packet_schema = self.output_schema()
        type_converter = self.data_context.type_converter
        empty_fields: dict = {}
        for name, py_type in {**tag_schema, **packet_schema}.items():
            arrow_type = type_converter.python_type_to_arrow_type(py_type)
            empty_fields[name] = pa.array([], type=arrow_type)
        return pa.table(empty_fields)

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
        """
        if self._pipeline_database is None:
            return None
        if self._cache_mode != CacheMode.REPLAY:
            return None
        records = self._pipeline_database.get_all_records(self.node_identity_path)
        if records is None:
            if self._operator is None:
                # Read-only (deserialized) node with no operator: cannot derive
                # schema without a live operator. Return None so callers fall
                # back to iter([]) / _make_empty_table() guarded paths.
                return None
            records_table = self._make_empty_table()
        else:
            records_table = self._filter_by_content_hash(records)
            # Drop internal columns that must not surface to stream consumers.
            cols_to_drop = [
                c
                for c in [constants.NODE_CONTENT_HASH_COL, self.HASH_COLUMN_NAME]
                if c in records_table.column_names
            ]
            if cols_to_drop:
                records_table = records_table.drop(cols_to_drop)
        tag_keys = self.keys()[0]
        return ArrowTableStream(records_table, tag_columns=tag_keys)

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
    ) -> list[tuple[TagProtocol, PacketProtocol]]:
        """Execute input streams: compute, persist, and cache.

        Args:
            *input_streams: Input streams to execute.
            observer: Optional execution observer for hooks.

        Returns:
            Materialized list of (tag, packet) pairs.
        """
        from orcapod.pipeline.observer import NoOpObserver

        node_label = self.label
        node_hash = self.content_hash().to_string()

        obs = observer if observer is not None else NoOpObserver()
        ctx_obs = obs.contextualize(*self.node_identity_path)

        ctx_obs.on_node_start(node_label, node_hash)

        # Check REPLAY cache first
        cached_output = self.get_cached_output()
        if cached_output is not None:
            output = list(cached_output.iter_packets())
            ctx_obs.on_node_end(node_label, node_hash)
            return output

        # Compute
        result_stream = self._operator.process(*input_streams)

        # Materialize
        output = list(result_stream.iter_packets())

        # Cache
        if output:
            self._cached_output_stream = StaticOutputOperatorPod._materialize_to_stream(
                output
            )
        else:
            self._cached_output_stream = result_stream

        self._update_modified_time()

        # Persist to DB only in LOG mode
        if (
            self._pipeline_database is not None
            and self._cache_mode == CacheMode.LOG
            and self._cached_output_stream is not None
        ):
            self._store_output_stream(self._cached_output_stream)

        ctx_obs.on_node_end(node_label, node_hash)
        return output

    def _compute_and_store(self) -> None:
        """Compute operator output, optionally store in DB."""
        self._cached_output_stream = self._operator.process(
            *self._input_streams,
        )

        if self._cache_mode == CacheMode.OFF:
            self._update_modified_time()
            return

        self._store_output_stream(self._cached_output_stream)
        self._update_modified_time()

    def _replay_from_cache(self) -> None:
        """Load cached results from DB, skip computation.

        If no cached records exist yet, produces an empty stream with
        the correct schema (zero rows, correct columns).
        """
        records = self._pipeline_database.get_all_records(self.node_identity_path)
        if records is None:
            records = self._make_empty_table()
        else:
            records = self._filter_by_content_hash(records)
            # Drop internal columns that must not surface to stream consumers.
            cols_to_drop = [
                c
                for c in [constants.NODE_CONTENT_HASH_COL, self.HASH_COLUMN_NAME]
                if c in records.column_names
            ]
            if cols_to_drop:
                records = records.drop(cols_to_drop)

        tag_keys = self.keys()[0]
        self._cached_output_stream = ArrowTableStream(records, tag_columns=tag_keys)
        self._update_modified_time()

    def run(self) -> None:
        """Execute the operator according to the current cache mode.

        Without a database:
            Always compute via the operator's ``process()`` method.

        With a database:
            - **OFF**: always compute, no DB writes.
            - **LOG**: always compute, write results to DB.
            - **REPLAY**: skip computation, load from DB.
        """
        if self.is_stale:
            self.clear_cache()

        if self._cached_output_stream is not None:
            return

        if self._pipeline_database is not None:
            if self._cache_mode == CacheMode.REPLAY:
                self._replay_from_cache()
            else:
                self._compute_and_store()
        else:
            self._cached_output_stream = self._operator.process(
                *self._input_streams,
            )
            self._update_modified_time()

    def iter_packets(self) -> Iterator[tuple[TagProtocol, PacketProtocol]]:
        """Return an iterator over (tag, packet) pairs.

        Read-only: never triggers computation. Returns empty before ``run()``
        or ``execute()`` populates the cache. Call ``node.is_stale`` before
        iterating if you need to detect outdated cached data.
        """
        if self._cached_output_stream is not None:
            return self._cached_output_stream.iter_packets()
        db_stream = self._load_cached_stream_from_db()
        if db_stream is not None:
            return db_stream.iter_packets()
        return iter([])

    def as_table(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> "pa.Table":
        """Return the output as a PyArrow table.

        Read-only: never triggers computation. Returns a zero-row table
        with correct schema before ``run()`` or ``execute()`` is called.
        """
        if self._cached_output_stream is not None:
            return self._cached_output_stream.as_table(columns=columns, all_info=all_info)
        db_stream = self._load_cached_stream_from_db()
        if db_stream is not None:
            return db_stream.as_table(columns=columns, all_info=all_info)
        # No cached or stored records yet: construct a zero-row table with the
        # correct schema and route it through ArrowTableStream so that the
        # ColumnConfig / all_info shaping is applied consistently.
        # Requires a live operator for schema derivation (pre-existing limitation).
        if self._operator is None:
            return pa.table({})
        empty_records = self._make_empty_table()
        tag_keys = self.keys()[0]
        empty_stream = ArrowTableStream(empty_records, tag_columns=tag_keys)
        return empty_stream.as_table(columns=columns, all_info=all_info)

    # ------------------------------------------------------------------
    # DB retrieval
    # ------------------------------------------------------------------

    def get_all_records(
        self,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> pa.Table | None:
        """Retrieve all stored records from the pipeline database.

        Returns the stored output table with column filtering applied
        per ``ColumnConfig`` conventions.  Returns ``None`` when no
        database is attached or no records exist.
        """
        if self._pipeline_database is None:
            return None

        results = self._pipeline_database.get_all_records(self.node_identity_path)
        if results is None:
            return None

        results = self._filter_by_content_hash(results)

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
            results = results.drop(
                [c for c in drop_columns if c in results.column_names]
            )

        return results if results.num_rows > 0 else None

    # ------------------------------------------------------------------
    # DerivedSource
    # ------------------------------------------------------------------

    def as_source(self):
        """Return a DerivedSource backed by the DB records of this node.

        Raises:
            RuntimeError: If no database is attached.
        """
        if self._pipeline_database is None:
            raise RuntimeError("Cannot create a DerivedSource without a database")

        from orcapod.core.sources.derived_source import DerivedSource

        path_str = "/".join(self.node_identity_path)
        content_frag = self.content_hash().to_string()[:16]
        source_id = f"{path_str}:{content_frag}"
        return DerivedSource(
            origin=self,
            source_id=source_id,
            data_context=self.data_context_key,
            config=self.orcapod_config,
        )

    # ------------------------------------------------------------------
    # Async channel execution
    # ------------------------------------------------------------------

    async def async_execute(
        self,
        inputs: Sequence[ReadableChannel[tuple[TagProtocol, PacketProtocol]]],
        output: WritableChannel[tuple[TagProtocol, PacketProtocol]],
        *,
        observer: ExecutionObserverProtocol | None = None,
    ) -> None:
        """Async execution with cache mode handling when DB is attached.

        Without a database, delegates to the wrapped operator's
        ``async_execute``.

        With a database:
            - REPLAY: emit from DB, close output.
            - OFF: delegate to operator, forward results.
            - LOG: delegate to operator, forward + collect results, then store in DB.

        Args:
            inputs: Sequence of readable channels from upstream nodes.
            output: Writable channel for output (tag, packet) pairs.
            observer: Optional execution observer for hooks.
        """
        from orcapod.pipeline.observer import NoOpObserver

        obs = observer if observer is not None else NoOpObserver()
        node_label = self.label
        node_hash = self.content_hash().to_string()
        ctx_obs = obs.contextualize(*self.node_identity_path)

        if self._pipeline_database is None:
            # Simple delegation without DB
            ctx_obs.on_node_start(node_label, node_hash)
            hashes = [s.pipeline_hash() for s in self._input_streams]
            await self._operator.async_execute(
                inputs, output, input_pipeline_hashes=hashes
            )
            ctx_obs.on_node_end(node_label, node_hash)
            return

        try:
            ctx_obs.on_node_start(node_label, node_hash)

            if self._cache_mode == CacheMode.REPLAY:
                self._replay_from_cache()
                assert self._cached_output_stream is not None
                for tag, packet in self._cached_output_stream.iter_packets():
                    await output.send((tag, packet))
                ctx_obs.on_node_end(node_label, node_hash)
                return  # finally block closes output

            # OFF or LOG: delegate to operator, forward results downstream
            intermediate: Channel[tuple[TagProtocol, PacketProtocol]] = Channel()
            should_collect = self._cache_mode == CacheMode.LOG
            collected: list[tuple[TagProtocol, PacketProtocol]] = []

            async def forward() -> None:
                async for item in intermediate.reader:
                    if should_collect:
                        collected.append(item)
                    await output.send(item)

            hashes = [s.pipeline_hash() for s in self._input_streams]
            async with asyncio.TaskGroup() as tg:
                tg.create_task(
                    self._operator.async_execute(
                        inputs,
                        intermediate.writer,
                        input_pipeline_hashes=hashes,
                    )
                )
                tg.create_task(forward())

            # TaskGroup has completed — store if LOG mode (sync DB write, post-hoc)
            if should_collect and collected:
                stream = StaticOutputOperatorPod._materialize_to_stream(collected)
                self._cached_output_stream = stream
                self._store_output_stream(stream)

            self._update_modified_time()

            ctx_obs.on_node_end(node_label, node_hash)
        finally:
            await output.close()

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}(operator={self._operator!r}, "
            f"upstreams={self._input_streams!r})"
        )
