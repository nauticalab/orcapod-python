"""OperatorNode — stream node for operator invocations with optional DB persistence."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Iterator, Sequence
from typing import TYPE_CHECKING, Any

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
else:
    pa = LazyModule("pyarrow")


class OperatorNode(StreamBase):
    """Stream node representing an operator invocation with optional DB persistence.

    When constructed without database parameters, provides the core stream
    interface (identity, schema, iteration) without any persistence.  When
    databases are provided (either at construction or via ``attach_databases``),
    adds pipeline record storage with per-row deduplication, ``get_all_records()``
    for retrieving stored results, ``as_source()`` for creating a
    ``DerivedSource`` from DB records, and three-tier cache mode
    (OFF / LOG / REPLAY).

    Pipeline path structure::

        pipeline_path_prefix / operator.uri / node:{content_hash}

    Where ``content_hash`` is the data-inclusive hash that encodes both
    pipeline structure and upstream source identities, ensuring each
    unique source combination gets its own cache table.

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
        pipeline_path_prefix: tuple[str, ...] = (),
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
        self._pipeline_path_prefix: tuple[str, ...] = ()
        self._cache_mode = CacheMode.OFF
        self._pipeline_node_hash: str | None = None

        if pipeline_database is not None:
            self.attach_databases(
                pipeline_database=pipeline_database,
                cache_mode=cache_mode,
                pipeline_path_prefix=pipeline_path_prefix,
            )

    # ------------------------------------------------------------------
    # attach_databases
    # ------------------------------------------------------------------

    def attach_databases(
        self,
        pipeline_database: ArrowDatabaseProtocol,
        cache_mode: CacheMode = CacheMode.OFF,
        pipeline_path_prefix: tuple[str, ...] = (),
    ) -> None:
        """Attach a database for persistent caching and pipeline records.

        Args:
            pipeline_database: Database for pipeline records.
            cache_mode: Caching behaviour (OFF, LOG, or REPLAY).
            pipeline_path_prefix: Path prefix for pipeline records.
        """
        self._pipeline_database = pipeline_database
        self._pipeline_path_prefix = pipeline_path_prefix
        self._cache_mode = cache_mode

        # Clear caches
        self.clear_cache()
        self._content_hash_cache.clear()
        self._pipeline_hash_cache.clear()

        # Use content_hash (data-inclusive) for pipeline node hash
        self._pipeline_node_hash = self.content_hash().to_string()

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

        pipeline_db = databases.get("pipeline")
        cache_mode_str = descriptor.get("cache_mode", "OFF")
        cache_mode = (
            CacheMode[cache_mode_str]
            if isinstance(cache_mode_str, str)
            else CacheMode.OFF
        )

        if operator is not None and input_streams:
            # Full mode: construct normally
            pipeline_path = tuple(descriptor.get("pipeline_path", ()))
            # Derive pipeline_path_prefix by stripping the suffix that
            # __init__ appends: operator.uri (2 elements) + node:{hash} (1 element).
            uri_len = len(operator.uri) + 1  # +1 for node:{hash}
            prefix = pipeline_path[:-uri_len] if len(pipeline_path) > uri_len else ()

            node = cls(
                operator=operator,
                input_streams=input_streams,
                pipeline_database=pipeline_db,
                cache_mode=cache_mode,
                pipeline_path_prefix=prefix,
                label=descriptor.get("label"),
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
        node._pipeline_path_prefix = ()
        node._cache_mode = cache_mode
        node._pipeline_node_hash = None

        # Descriptor metadata for read-only access
        node._descriptor = descriptor
        node._stored_schema = descriptor.get("output_schema", {})
        node._stored_content_hash = descriptor.get("content_hash")
        node._stored_pipeline_hash = descriptor.get("pipeline_hash")
        node._stored_pipeline_path = tuple(descriptor.get("pipeline_path", ()))

        # Determine load status based on DB availability
        node._load_status = LoadStatus.UNAVAILABLE
        if pipeline_db is not None:
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
        from orcapod.pipeline.serialization import LoadStatus

        return getattr(self, "_load_status", LoadStatus.FULL)

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

    def content_hash(self, hasher=None) -> "ContentHash":
        """Return the content hash, using stored value in read-only mode."""
        stored = getattr(self, "_stored_content_hash", None)
        if self._operator is None and stored is not None:
            return ContentHash.from_string(stored)
        return super().content_hash(hasher)

    def pipeline_hash(self, hasher=None) -> "ContentHash":
        """Return the pipeline hash, using stored value in read-only mode."""
        stored = getattr(self, "_stored_pipeline_hash", None)
        if self._operator is None and stored is not None:
            return ContentHash.from_string(stored)
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
            stored = getattr(self, "_stored_schema", {})
            tag_keys = tuple(stored.get("tag", {}).keys())
            packet_keys = tuple(stored.get("packet", {}).keys())
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
            stored = getattr(self, "_stored_schema", {})
            tag = Schema(stored.get("tag", {}))
            packet = Schema(stored.get("packet", {}))
            return tag, packet
        return self._operator.output_schema(
            *self._input_streams,
            columns=columns,
            all_info=all_info,
        )

    # ------------------------------------------------------------------
    # Pipeline path
    # ------------------------------------------------------------------

    @property
    def pipeline_path(self) -> tuple[str, ...]:
        """Return the pipeline path for DB record scoping.

        Raises:
            RuntimeError: If no database is attached and this is not a
                read-only deserialized node.
        """
        stored = getattr(self, "_stored_pipeline_path", None)
        if self._operator is None and stored is not None:
            return stored
        if self._pipeline_database is None:
            raise RuntimeError(
                "pipeline_path requires a database. Call attach_databases() first."
            )
        return (
            self._pipeline_path_prefix
            + self._operator.uri
            + (f"node:{self._pipeline_node_hash}",)
        )

    # ------------------------------------------------------------------
    # Computation and caching
    # ------------------------------------------------------------------

    def clear_cache(self) -> None:
        """Discard all in-memory cached state."""
        self._cached_output_stream = None
        self._cached_output_table = None
        self._update_modified_time()

    def _store_output_stream(self, stream: StreamProtocol) -> None:
        """Materialize stream and store in the pipeline database with per-row dedup."""
        output_table = stream.as_table(
            columns={"source": True, "system_tags": True},
        )

        # Per-row record hashes for dedup: hash(tag + packet + system_tag)
        arrow_hasher = self.data_context.arrow_hasher
        record_hashes = []
        for batch in output_table.to_batches():
            for i in range(len(batch)):
                record_hashes.append(
                    arrow_hasher.hash_table(batch.slice(i, 1)).to_hex()
                )

        output_table = output_table.add_column(
            0,
            self.HASH_COLUMN_NAME,
            pa.array(record_hashes, type=pa.large_string()),
        )

        # Store (identical rows across runs naturally deduplicate)
        self._pipeline_database.add_records(
            self.pipeline_path,
            output_table,
            record_id_column=self.HASH_COLUMN_NAME,
            skip_duplicates=True,
        )

        self._cached_output_table = output_table.drop(self.HASH_COLUMN_NAME)

    def get_cached_output(self) -> "StreamProtocol | None":
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
        observer: Any = None,
    ) -> list[tuple[TagProtocol, PacketProtocol]]:
        """Execute input streams: compute, persist, and cache.

        Args:
            *input_streams: Input streams to execute.
            observer: Optional execution observer for hooks.

        Returns:
            Materialized list of (tag, packet) pairs.
        """
        if observer is not None:
            observer.on_node_start(self)

        # Check REPLAY cache first
        cached_output = self.get_cached_output()
        if cached_output is not None:
            output = list(cached_output.iter_packets())
            if observer is not None:
                observer.on_node_end(self)
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

        if observer is not None:
            observer.on_node_end(self)
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
        records = self._pipeline_database.get_all_records(self.pipeline_path)
        if records is None:
            # Build an empty table with the correct schema
            tag_schema, packet_schema = self.output_schema()
            type_converter = self.data_context.type_converter
            empty_fields = {}
            for name, py_type in {**tag_schema, **packet_schema}.items():
                arrow_type = type_converter.python_type_to_arrow_type(py_type)
                empty_fields[name] = pa.array([], type=arrow_type)
            records = pa.table(empty_fields)

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
        self.run()
        assert self._cached_output_stream is not None
        return self._cached_output_stream.iter_packets()

    def as_table(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> "pa.Table":
        self.run()
        assert self._cached_output_stream is not None
        return self._cached_output_stream.as_table(columns=columns, all_info=all_info)

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
        database is attached or no records exist.
        """
        if self._pipeline_database is None:
            return None

        results = self._pipeline_database.get_all_records(self.pipeline_path)
        if results is None:
            return None

        column_config = ColumnConfig.handle_config(columns, all_info=all_info)

        drop_columns = []
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

        path_str = "/".join(self.pipeline_path)
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
    ) -> None:
        """Async execution with cache mode handling when DB is attached.

        Without a database, delegates to the wrapped operator's
        ``async_execute``.

        With a database:
            - REPLAY: emit from DB, close output.
            - OFF: delegate to operator, forward results.
            - LOG: delegate to operator, forward + collect results, then store in DB.
        """
        if self._pipeline_database is None:
            # Simple delegation without DB
            hashes = [s.pipeline_hash() for s in self._input_streams]
            await self._operator.async_execute(
                inputs, output, input_pipeline_hashes=hashes
            )
            return

        try:
            if self._cache_mode == CacheMode.REPLAY:
                self._replay_from_cache()
                assert self._cached_output_stream is not None
                for tag, packet in self._cached_output_stream.iter_packets():
                    await output.send((tag, packet))
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
        finally:
            await output.close()

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}(operator={self._operator!r}, "
            f"upstreams={self._input_streams!r})"
        )
