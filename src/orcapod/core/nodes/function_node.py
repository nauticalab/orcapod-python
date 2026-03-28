"""FunctionNode — stream node for packet function invocations with optional DB persistence."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any, Literal, cast

from orcapod import contexts
from orcapod.channels import ReadableChannel, WritableChannel
from orcapod.config import Config
from orcapod.core.cached_function_pod import CachedFunctionPod
from orcapod.core.streams.arrow_table_stream import ArrowTableStream
from orcapod.core.streams.base import StreamBase
from orcapod.core.tracker import DEFAULT_TRACKER_MANAGER
from orcapod.protocols.core_protocols import (
    FunctionPodProtocol,
    PacketFunctionExecutorProtocol,
    PacketFunctionProtocol,
    PacketProtocol,
    StreamProtocol,
    TagProtocol,
    TrackerManagerProtocol,
)
from orcapod.protocols.database_protocols import ArrowDatabaseProtocol
from orcapod.system_constants import constants
from orcapod.types import (
    ColumnConfig,
    ContentHash,
    NodeConfig,
    PipelineConfig,
    Schema,
    resolve_concurrency,
)
from orcapod.utils import arrow_utils, schema_utils
from orcapod.utils.lazy_module import LazyModule

logger = logging.getLogger(__name__)

from orcapod.protocols.observability_protocols import (
    ExecutionObserverProtocol,
    PacketExecutionLoggerProtocol,
)

if TYPE_CHECKING:
    import polars as pl
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")
    pl = LazyModule("polars")


def _executor_supports_concurrent(
    packet_function: PacketFunctionProtocol,
) -> bool:
    """Return True if the packet function's executor supports concurrent execution."""
    executor = packet_function.executor
    return executor is not None and executor.supports_concurrent_execution


class FunctionNode(StreamBase):
    """Stream node representing a packet function invocation with optional DB persistence.

    When constructed without database parameters, provides the core stream
    interface (identity, schema, iteration) without any persistence.  When
    databases are provided (either at construction or via ``attach_databases``),
    adds result caching via ``CachedFunctionPod``, pipeline record storage,
    and two-phase iteration (cached first, then compute missing).
    """

    node_type = "function"

    def __init__(
        self,
        function_pod: FunctionPodProtocol,
        input_stream: StreamProtocol,
        tracker_manager: TrackerManagerProtocol | None = None,
        label: str | None = None,
        config: Config | None = None,
        # Optional DB params for persistent mode:
        pipeline_database: ArrowDatabaseProtocol | None = None,
        result_database: ArrowDatabaseProtocol | None = None,
        result_path_prefix: tuple[str, ...] | None = None,
        pipeline_path_prefix: tuple[str, ...] = (),
    ):
        if tracker_manager is None:
            tracker_manager = DEFAULT_TRACKER_MANAGER
        self.tracker_manager = tracker_manager
        self._packet_function = function_pod.packet_function

        # FunctionPod used for the `producer` property and pipeline identity
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
            _, incoming_packet_types = input_stream.output_schema()
            expected_packet_schema = self._packet_function.input_packet_schema
            if not schema_utils.check_schema_compatibility(
                incoming_packet_types, expected_packet_schema
            ):
                raise ValueError(
                    f"Incoming packet data type {incoming_packet_types} from {input_stream} "
                    f"is not compatible with expected input schema {expected_packet_schema}"
                )

        self._input_stream = input_stream

        # stream-level caching state (iterator acquired lazily on first use)
        self._cached_input_iterator: (
            Iterator[tuple[TagProtocol, PacketProtocol]] | None
        ) = None
        self._needs_iterator = True
        self._cached_output_packets: dict[
            int, tuple[TagProtocol, PacketProtocol | None]
        ] = {}
        self._cached_output_table: pa.Table | None = None
        self._cached_content_hash_column: pa.Array | None = None

        # DB persistence state (initially None; set via __init__ params or attach_databases)
        self._pipeline_database: ArrowDatabaseProtocol | None = None
        self._cached_function_pod: CachedFunctionPod | None = None
        self._pipeline_path_prefix: tuple[str, ...] = ()
        self._pipeline_node_hash: str | None = None
        self._output_schema_hash: str | None = None

        if pipeline_database is not None:
            self.attach_databases(
                pipeline_database=pipeline_database,
                result_database=result_database,
                result_path_prefix=result_path_prefix,
                pipeline_path_prefix=pipeline_path_prefix,
            )

    # ------------------------------------------------------------------
    # attach_databases
    # ------------------------------------------------------------------

    def attach_databases(
        self,
        pipeline_database: ArrowDatabaseProtocol,
        result_database: ArrowDatabaseProtocol | None = None,
        result_path_prefix: tuple[str, ...] | None = None,
        pipeline_path_prefix: tuple[str, ...] = (),
    ) -> None:
        """Attach databases for persistent caching and pipeline records.

        Creates a ``CachedFunctionPod`` wrapping the original function pod
        for result caching.  The pipeline database is used separately for
        pipeline-level provenance records (tag + packet hash).

        Args:
            pipeline_database: Database for pipeline records.
            result_database: Database for cached results. Defaults to
                pipeline_database.
            result_path_prefix: Path prefix for result records.
            pipeline_path_prefix: Path prefix for pipeline records.
        """
        computed_result_path_prefix: tuple[str, ...] = ()
        if result_database is None:
            result_database = pipeline_database
            computed_result_path_prefix = (
                result_path_prefix
                if result_path_prefix is not None
                else pipeline_path_prefix + ("_result",)
            )
        elif result_path_prefix is not None:
            computed_result_path_prefix = result_path_prefix

        # Always wrap the original function_pod (not a previous cached wrapper)
        self._cached_function_pod = CachedFunctionPod(
            self._function_pod,
            result_database=result_database,
            record_path_prefix=computed_result_path_prefix,
        )

        self._pipeline_database = pipeline_database
        self._pipeline_path_prefix = pipeline_path_prefix

        # Clear all caches
        self.clear_cache()
        self._content_hash_cache.clear()
        self._pipeline_hash_cache.clear()

        # Compute pipeline node hash
        self._pipeline_node_hash = self.pipeline_hash().to_string()
        self._output_schema_hash = self.data_context.semantic_hasher.hash_object(
            self._packet_function.output_packet_schema
        ).to_string()

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
        raise ``RuntimeError``.

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

        pipeline_db = databases.get("pipeline")
        result_db = databases.get("result", pipeline_db)

        if function_pod is not None and input_stream is not None:
            # Full / READ_ONLY / CACHE_ONLY mode: construct normally via __init__.
            pipeline_path = tuple(descriptor.get("pipeline_path", ()))
            # Derive pipeline_path_prefix by stripping the suffix that
            # __init__ appends (packet_function.uri + two hash elements).
            # The descriptor stores the complete pipeline_path; we need
            # to reconstruct the prefix that was originally passed to
            # __init__. The suffix added is:
            #   pf.uri + (f"schema:{pipeline_hash}", f"instance:{content_hash}")
            pf_uri_len = len(function_pod.packet_function.uri) + 2  # +2 for schema/instance
            prefix = (
                pipeline_path[:-pf_uri_len] if len(pipeline_path) > pf_uri_len else ()
            )

            # Derive result_path_prefix from the stored result_record_path
            # by stripping the URI suffix that CachedFunctionPod appends.
            stored_result_path = tuple(
                descriptor.get("result_record_path", ())
            )
            uri_len = len(function_pod.packet_function.uri)
            result_prefix: tuple[str, ...] | None = None
            if stored_result_path and len(stored_result_path) > uri_len:
                result_prefix = stored_result_path[:-uri_len]

            node = cls(
                function_pod=function_pod,
                input_stream=input_stream,
                pipeline_database=pipeline_db,
                result_database=result_db,
                result_path_prefix=result_prefix,
                pipeline_path_prefix=prefix,
                label=descriptor.get("label"),
            )
            node._descriptor = descriptor

            # Determine mode based on upstream availability and function type.
            from orcapod.core.packet_function_proxy import PacketFunctionProxy

            input_unavailable = (
                hasattr(input_stream, "load_status")
                and input_stream.load_status == LoadStatus.UNAVAILABLE
            )
            if input_unavailable:
                node._load_status = LoadStatus.CACHE_ONLY
            elif isinstance(function_pod.packet_function, PacketFunctionProxy):
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
        from orcapod.config import DEFAULT_CONFIG

        node._orcapod_config = DEFAULT_CONFIG

        # From ContentIdentifiableBase
        node._content_hash_cache = {}
        node._cached_int_hash = None

        # From PipelineElementBase
        node._pipeline_hash_cache = {}

        # From TemporalMixin
        node._modified_time = None

        # From FunctionNode
        node._function_pod = None
        node._packet_function = None
        node._input_stream = None
        node.tracker_manager = DEFAULT_TRACKER_MANAGER
        node._cached_input_iterator = None
        node._needs_iterator = True
        node._cached_output_packets = {}
        node._cached_output_table = None
        node._cached_content_hash_column = None

        # DB persistence state
        node._pipeline_database = pipeline_db
        node._cached_function_pod = None
        node._pipeline_path_prefix = ()
        node._pipeline_node_hash = None
        node._output_schema_hash = None

        # Descriptor metadata for read-only access
        node._descriptor = descriptor
        node._stored_schema = descriptor.get("output_schema", {})
        node._stored_content_hash = descriptor.get("content_hash")
        node._stored_pipeline_hash = descriptor.get("pipeline_hash")
        node._stored_pipeline_path = tuple(descriptor.get("pipeline_path", ()))
        node._stored_result_record_path = tuple(
            descriptor.get("result_record_path", ())
        )

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
    # Core properties
    # ------------------------------------------------------------------

    @property
    def producer(self) -> FunctionPodProtocol:
        return self._function_pod

    @property
    def data_context(self) -> contexts.DataContext:
        return contexts.resolve_context(self._function_pod.data_context_key)

    @property
    def data_context_key(self) -> str:
        return self._function_pod.data_context_key

    @property
    def executor(self) -> PacketFunctionExecutorProtocol | None:
        """The executor set on the underlying packet function."""
        return self._packet_function.executor

    @executor.setter
    def executor(self, executor: PacketFunctionExecutorProtocol | None) -> None:
        """Set or clear the executor on the underlying packet function."""
        self._packet_function.executor = executor

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
        stored = getattr(self, "_stored_content_hash", None)
        if self._function_pod is None and stored is not None:
            from orcapod.types import ContentHash as CH

            return CH.from_string(stored)
        return super().content_hash(hasher)

    def pipeline_hash(self, hasher=None) -> ContentHash:
        """Return the pipeline hash, using stored value in read-only mode."""
        stored = getattr(self, "_stored_pipeline_hash", None)
        if self._function_pod is None and stored is not None:
            from orcapod.types import ContentHash as CH

            return CH.from_string(stored)
        return super().pipeline_hash(hasher)

    def output_schema(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        """Return output schema, using stored value in read-only mode."""
        if self._function_pod is None:
            stored = getattr(self, "_stored_schema", {})
            tag = Schema(stored.get("tag", {}))
            packet = Schema(stored.get("packet", {}))
            return tag, packet
        tag_schema = self._input_stream.output_schema(
            columns=columns, all_info=all_info
        )[0]
        return tag_schema, self._packet_function.output_packet_schema

    def keys(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        if self._function_pod is None:
            stored = getattr(self, "_stored_schema", {})
            tag_keys = tuple(stored.get("tag", {}).keys())
            packet_keys = tuple(stored.get("packet", {}).keys())
            return tag_keys, packet_keys
        tag_schema, packet_schema = self.output_schema(
            columns=columns, all_info=all_info
        )
        return tuple(tag_schema.keys()), tuple(packet_schema.keys())

    # ------------------------------------------------------------------
    # Pipeline path
    # ------------------------------------------------------------------

    @property
    def pipeline_path(self) -> tuple[str, ...]:
        """Return the pipeline path for DB record scoping.

        Returns ``()`` when no pipeline database is attached.
        """
        stored = getattr(self, "_stored_pipeline_path", None)
        if self._packet_function is None and stored is not None:
            return stored
        if self._pipeline_database is None:
            return ()
        return (
            self._pipeline_path_prefix
            + self._packet_function.uri
            + (
                f"schema:{self.pipeline_hash().to_string()}",
                f"instance:{self.content_hash().to_string()}",
            )
        )

    # ------------------------------------------------------------------
    # Caching
    # ------------------------------------------------------------------

    def _ensure_iterator(self) -> None:
        """Lazily acquire the upstream iterator on first use."""
        if self._needs_iterator:
            self._cached_input_iterator = self._input_stream.iter_packets()
            self._needs_iterator = False
            self._update_modified_time()

    def clear_cache(self) -> None:
        self._cached_input_iterator = None
        self._needs_iterator = True
        self._cached_output_packets.clear()
        self._cached_output_table = None
        self._cached_content_hash_column = None
        self._update_modified_time()

    # ------------------------------------------------------------------
    # Packet processing
    # ------------------------------------------------------------------

    def execute_packet(
        self,
        tag: TagProtocol,
        packet: PacketProtocol,
    ) -> tuple[TagProtocol, PacketProtocol | None]:
        """Execute a single packet: compute, persist, and cache.

        Internal method for orchestrators. The caller must guarantee that
        the tag and packet conform to the expected input schema (matching
        ``self._input_stream``). No validation is performed.

        Args:
            tag: The tag associated with the packet.
            packet: The input packet to process.

        Returns:
            A ``(tag, output_packet)`` tuple.
        """
        tag_out, result = self._process_packet_internal(tag, packet)
        return tag_out, result

    def execute(
        self,
        input_stream: StreamProtocol,
        *,
        observer: ExecutionObserverProtocol | None = None,
        error_policy: Literal["continue", "fail_fast"] = "continue",
    ) -> list[tuple[TagProtocol, PacketProtocol]]:
        """Execute all packets from a stream: compute, persist, and cache.

        Args:
            input_stream: The input stream to process.
            observer: Optional execution observer for hooks.
            error_policy: ``"continue"`` (default) skips failed packets;
                ``"fail_fast"`` re-raises on the first failure.

        Returns:
            Materialized list of (tag, output_packet) pairs, excluding
            None outputs and failed packets.
        """
        from orcapod.pipeline.observer import NoOpObserver

        node_label = self.label
        node_hash = self.content_hash().to_string()

        obs = observer if observer is not None else NoOpObserver()

        pp = self.pipeline_path
        tag_schema = input_stream.output_schema(columns={"system_tags": True})[0]
        obs.on_node_start(node_label, node_hash, pipeline_path=pp, tag_schema=tag_schema)

        # Gather entry IDs and check cache
        upstream_entries = [
            (tag, packet, self.compute_pipeline_entry_id(tag, packet))
            for tag, packet in input_stream.iter_packets()
        ]
        entry_ids = [eid for _, _, eid in upstream_entries]
        cached = self.get_cached_results(entry_ids=entry_ids)

        output: list[tuple[TagProtocol, PacketProtocol]] = []
        for tag, packet, entry_id in upstream_entries:
            obs.on_packet_start(node_label, tag, packet)

            if entry_id in cached:
                tag_out, result = cached[entry_id]
                obs.on_packet_end(
                    node_label, tag, packet, result, cached=True
                )
                output.append((tag_out, result))
            else:
                ctx_obs = obs.contextualize(node_hash, node_label)
                pkt_logger = ctx_obs.create_packet_logger(
                    tag, packet, pipeline_path=pp
                )
                try:
                    tag_out, result = self._process_packet_internal(
                        tag, packet, logger=pkt_logger
                    )
                except Exception as exc:
                    logger.warning(
                        "Packet execution failed in %s: %s", node_label, exc,
                        exc_info=True,
                    )
                    obs.on_packet_crash(node_label, tag, packet, exc)
                    if error_policy == "fail_fast":
                        obs.on_node_end(node_label, node_hash, pipeline_path=pp)
                        raise
                else:
                    obs.on_packet_end(
                        node_label, tag, packet, result, cached=False
                    )
                    if result is not None:
                        output.append((tag_out, result))

        obs.on_node_end(node_label, node_hash, pipeline_path=pp)
        return output

    def _process_packet_internal(
        self,
        tag: TagProtocol,
        packet: PacketProtocol,
        cache_index: int | None = None,
        *,
        logger: PacketExecutionLoggerProtocol | None = None,
    ) -> tuple[TagProtocol, PacketProtocol | None]:
        """Core compute + persist + cache.

        Used by ``execute_packet``, ``execute``, and ``iter_packets``.
        No input validation is performed — the caller guarantees correctness.
        Exceptions propagate to the caller.

        Returns:
            A ``(tag, output_packet)`` 2-tuple.

        Args:
            tag: The input tag.
            packet: The input packet.
            cache_index: Optional explicit index for the internal cache.
                When ``None``, auto-assigns at ``len(_cached_output_packets)``.
            logger: Optional packet execution logger.
        """
        if self._cached_function_pod is not None:
            tag_out, output_packet = self._cached_function_pod.process_packet(
                tag, packet, logger=logger
            )

            if output_packet is not None:
                result_computed = bool(
                    output_packet.get_meta_value(
                        self._cached_function_pod.RESULT_COMPUTED_FLAG, False
                    )
                )
                self.add_pipeline_record(
                    tag,
                    packet,
                    packet_record_id=output_packet.datagram_id,
                    computed=result_computed,
                )
        else:
            tag_out, output_packet = self._function_pod.process_packet(
                tag, packet, logger=logger
            )

        # Cache internally and invalidate derived caches
        idx = (
            cache_index if cache_index is not None else len(self._cached_output_packets)
        )
        self._cached_output_packets[idx] = (tag_out, output_packet)
        self._cached_input_iterator = None
        self._needs_iterator = False
        self._cached_output_table = None
        self._cached_content_hash_column = None

        return tag_out, output_packet

    def get_cached_results(
        self, entry_ids: list[str]
    ) -> dict[str, tuple[TagProtocol, PacketProtocol]]:
        """Retrieve cached results for specific pipeline entry IDs.

        Looks up the pipeline DB and result DB, joins them, and filters
        to the requested entry IDs. Returns a mapping from entry ID to
        (tag, output_packet).

        Args:
            entry_ids: Pipeline entry IDs to look up.

        Returns:
            Mapping from entry_id to (tag, output_packet) for found entries.
            Empty dict if no DB is attached or no matches found.
        """
        if self._cached_function_pod is None or not entry_ids:
            return {}

        PIPELINE_ENTRY_ID_COL = "__pipeline_entry_id"
        entry_id_set = set(entry_ids)

        taginfo = self._pipeline_database.get_all_records(
            self.pipeline_path,
            record_id_column=PIPELINE_ENTRY_ID_COL,
        )
        results = self._cached_function_pod._result_database.get_all_records(
            self._cached_function_pod.record_path,
            record_id_column=constants.PACKET_RECORD_ID,
        )

        if taginfo is None or results is None:
            return {}

        joined = (
            pl.DataFrame(taginfo)
            .join(
                pl.DataFrame(results),
                on=constants.PACKET_RECORD_ID,
                how="inner",
            )
            .to_arrow()
        )

        if joined.num_rows == 0:
            return {}

        # Filter to requested entry IDs
        all_entry_ids = joined.column(PIPELINE_ENTRY_ID_COL).to_pylist()
        mask = [eid in entry_id_set for eid in all_entry_ids]
        filtered = joined.filter(pa.array(mask))

        if filtered.num_rows == 0:
            return {}

        tag_keys = self._input_stream.keys()[0]
        drop_cols = [
            c
            for c in filtered.column_names
            if c.startswith(constants.META_PREFIX) or c == PIPELINE_ENTRY_ID_COL
        ]
        data_table = filtered.drop([c for c in drop_cols if c in filtered.column_names])

        stream = ArrowTableStream(data_table, tag_columns=tag_keys)
        filtered_entry_ids = [eid for eid, m in zip(all_entry_ids, mask) if m]

        result_dict: dict[str, tuple[TagProtocol, PacketProtocol]] = {}
        for entry_id, (tag, packet) in zip(filtered_entry_ids, stream.iter_packets()):
            result_dict[entry_id] = (tag, packet)

        # Populate internal cache with retrieved results (clear first to
        # avoid duplicates on repeated orchestrator runs)
        self._cached_output_packets.clear()
        self._cached_output_table = None
        self._cached_content_hash_column = None
        for entry_id, (tag, packet) in result_dict.items():
            next_idx = len(self._cached_output_packets)
            self._cached_output_packets[next_idx] = (tag, packet)
        self._cached_input_iterator = None
        self._needs_iterator = False

        return result_dict

    async def _async_process_packet_internal(
        self,
        tag: TagProtocol,
        packet: PacketProtocol,
        cache_index: int | None = None,
        *,
        logger: PacketExecutionLoggerProtocol | None = None,
    ) -> tuple[TagProtocol, PacketProtocol | None]:
        """Async counterpart of ``_process_packet_internal``.

        Computes via async path, writes pipeline provenance, and caches
        internally — no schema validation. Exceptions propagate.

        Returns:
            A ``(tag, output_packet)`` 2-tuple.

        Args:
            tag: The input tag.
            packet: The input packet.
            cache_index: Optional explicit index for the internal cache.
                When ``None``, auto-assigns at ``len(_cached_output_packets)``.
            logger: Optional packet execution logger.
        """
        if self._cached_function_pod is not None:
            tag_out, output_packet = (
                await self._cached_function_pod.async_process_packet(
                    tag, packet, logger=logger
                )
            )

            if output_packet is not None:
                result_computed = bool(
                    output_packet.get_meta_value(
                        self._cached_function_pod.RESULT_COMPUTED_FLAG, False
                    )
                )
                self.add_pipeline_record(
                    tag,
                    packet,
                    packet_record_id=output_packet.datagram_id,
                    computed=result_computed,
                )
        else:
            tag_out, output_packet = (
                await self._function_pod.async_process_packet(
                    tag, packet, logger=logger
                )
            )

        # Cache internally and invalidate derived caches
        idx = (
            cache_index if cache_index is not None else len(self._cached_output_packets)
        )
        self._cached_output_packets[idx] = (tag_out, output_packet)
        self._cached_input_iterator = None
        self._needs_iterator = False
        self._cached_output_table = None
        self._cached_content_hash_column = None

        return tag_out, output_packet

    def compute_pipeline_entry_id(
        self, tag: TagProtocol, input_packet: PacketProtocol
    ) -> str:
        """Compute a unique pipeline entry ID from tag + system tags + input packet hash.

        This ID uniquely identifies a (tag, system_tags, input_packet) combination
        and is used as the record ID in the pipeline database.

        Args:
            tag: The tag (including system tags).
            input_packet: The input packet.

        Returns:
            A hash string uniquely identifying this combination.
        """
        tag_with_hash = tag.as_table(columns={"system_tags": True}).append_column(
            constants.INPUT_PACKET_HASH_COL,
            pa.array([input_packet.content_hash().to_string()], type=pa.large_string()),
        )
        return self.data_context.arrow_hasher.hash_table(tag_with_hash).to_string()

    def add_pipeline_record(
        self,
        tag: TagProtocol,
        input_packet: PacketProtocol,
        packet_record_id: str,
        computed: bool,
        skip_cache_lookup: bool = False,
    ) -> None:
        """Add a pipeline record to the database for a processed packet.

        The pipeline record stores:
        - Tag columns (including system tags)
        - All source columns of the input packet (provenance, not data)
        - Output packet record ID (for joining with result records)
        - Input packet data context key
        - Whether the result was freshly computed or cached
        """
        entry_id = self.compute_pipeline_entry_id(tag, input_packet)

        # Check for existing entry
        existing_record = None
        if not skip_cache_lookup:
            existing_record = self._pipeline_database.get_record_by_id(
                self.pipeline_path,
                entry_id,
            )

        if existing_record is not None:
            logger.debug(
                f"Record with entry_id {entry_id} already exists. Skipping addition."
            )
            return

        # Extract source columns only (no data columns) from the input packet
        input_table_with_source = input_packet.as_table(columns={"source": True})
        source_col_names = [
            c
            for c in input_table_with_source.column_names
            if c.startswith(constants.SOURCE_PREFIX)
        ]
        input_source_table = input_table_with_source.select(source_col_names)

        # Build the meta columns table
        meta_table = pa.table(
            {
                constants.PACKET_RECORD_ID: pa.array(
                    [packet_record_id], type=pa.large_string()
                ),
                f"{constants.META_PREFIX}input_packet{constants.CONTEXT_KEY}": pa.array(
                    [input_packet.data_context_key], type=pa.large_string()
                ),
                f"{constants.META_PREFIX}computed": pa.array(
                    [computed], type=pa.bool_()
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
            self.pipeline_path,
            entry_id,
            combined_record,
            skip_duplicates=False,
        )

    # ------------------------------------------------------------------
    # Records and sources
    # ------------------------------------------------------------------

    def get_all_records(
        self,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> pa.Table | None:
        """Return all computed results joined with their pipeline tag records.

        Args:
            columns: Column configuration controlling which groups are included.
            all_info: Shorthand to include all info columns.

        Returns:
            A PyArrow table of joined results, or ``None`` if no database is
            attached or no records exist.
        """
        if self._cached_function_pod is None:
            return None

        results = self._cached_function_pod._result_database.get_all_records(
            self._cached_function_pod.record_path,
            record_id_column=constants.PACKET_RECORD_ID,
        )
        taginfo = self._pipeline_database.get_all_records(self.pipeline_path)

        if results is None or taginfo is None:
            return None

        joined = (
            pl.DataFrame(taginfo)
            .join(pl.DataFrame(results), on=constants.PACKET_RECORD_ID, how="inner")
            .to_arrow()
        )

        column_config = ColumnConfig.handle_config(columns, all_info=all_info)

        drop_columns = []
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
            joined = joined.drop([c for c in drop_columns if c in joined.column_names])

        return joined if joined.num_rows > 0 else None

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
    # Cache-only helpers (PLT-1156)
    # ------------------------------------------------------------------

    def _load_all_cached_records(
        self,
    ) -> "tuple[tuple[str, ...], Any] | None":
        """Join pipeline DB and result DB; return (tag_keys, data_table).

        Returns ``None`` when either database is empty or unavailable.
        Does not access ``_input_stream``.
        """
        import polars as pl

        if self._cached_function_pod is None or self._pipeline_database is None:
            return None

        PIPELINE_ENTRY_ID_COL = "__pipeline_entry_id"

        taginfo = self._pipeline_database.get_all_records(
            self.pipeline_path,
            record_id_column=PIPELINE_ENTRY_ID_COL,
        )
        results = self._cached_function_pod._result_database.get_all_records(
            self._cached_function_pod.record_path,
            record_id_column=constants.PACKET_RECORD_ID,
        )

        if taginfo is None or results is None:
            return None

        joined = (
            pl.DataFrame(taginfo)
            .join(
                pl.DataFrame(results),
                on=constants.PACKET_RECORD_ID,
                how="inner",
            )
            .to_arrow()
        )

        if joined.num_rows == 0:
            return None

        # Tag keys are the user-facing tag columns from the pipeline DB table.
        # Exclude: meta columns (__*), source columns (_source_*),
        # system-tag columns (e.g. __tag_*), and the entry-ID column.
        tag_keys = tuple(
            c
            for c in taginfo.column_names
            if not c.startswith(constants.META_PREFIX)
            and not c.startswith(constants.SOURCE_PREFIX)
            and not c.startswith(constants.SYSTEM_TAG_PREFIX)
            and c != PIPELINE_ENTRY_ID_COL
        )

        drop_cols = [
            c
            for c in joined.column_names
            if c.startswith(constants.META_PREFIX) or c == PIPELINE_ENTRY_ID_COL
        ]
        data_table = joined.drop([c for c in drop_cols if c in joined.column_names])
        return tag_keys, data_table

    def _iter_all_from_database(
        self,
    ) -> Iterator[tuple[TagProtocol, PacketProtocol]]:
        """Yield all cached (tag, packet) pairs from the DB.

        Used in ``CACHE_ONLY`` mode when the upstream is unavailable.
        Does not access ``_input_stream``.
        """
        result = self._load_all_cached_records()
        if result is None:
            return
        tag_keys, data_table = result
        stream = ArrowTableStream(data_table, tag_columns=tag_keys)
        for i, (tag, packet) in enumerate(stream.iter_packets()):
            self._cached_output_packets[i] = (tag, packet)
            yield tag, packet

    async def _async_execute_cache_only(
        self,
        output: "WritableChannel[tuple[TagProtocol, PacketProtocol]]",
        *,
        observer: Any | None = None,
    ) -> None:
        """Send all DB-cached (tag, packet) pairs to *output*.

        Used in ``CACHE_ONLY`` mode when the upstream is unavailable.
        Does not access ``_input_stream``.
        """
        from orcapod.pipeline.observer import NoOpObserver

        obs = observer if observer is not None else NoOpObserver()
        node_label = self.label
        node_hash = self.content_hash().to_string()
        pp = self.pipeline_path

        obs.on_node_start(node_label, node_hash, pipeline_path=pp, tag_schema=None)
        try:
            result = self._load_all_cached_records()
            if result is not None:
                tag_keys, data_table = result
                stream = ArrowTableStream(data_table, tag_columns=tag_keys)
                for tag, packet in stream.iter_packets():
                    obs.on_packet_start(node_label, tag, packet)
                    obs.on_packet_end(node_label, tag, packet, packet, cached=True)
                    await output.send((tag, packet))
            obs.on_node_end(node_label, node_hash, pipeline_path=pp)
        finally:
            await output.close()

    # ------------------------------------------------------------------
    # Iteration
    # ------------------------------------------------------------------

    def iter_packets(self) -> Iterator[tuple[TagProtocol, PacketProtocol]]:
        from orcapod.pipeline.serialization import LoadStatus

        status = self.load_status
        if status == LoadStatus.CACHE_ONLY:
            yield from self._iter_all_from_database()
            return

        if status == LoadStatus.UNAVAILABLE:
            raise RuntimeError(
                f"FunctionNode {self.label!r} is unavailable: "
                "no function pod and no database attached."
            )

        if self.is_stale:
            self.clear_cache()
        self._ensure_iterator()

        if self._cached_function_pod is not None:
            # Two-phase iteration with DB backing
            if self._cached_input_iterator is not None:
                input_iter = self._cached_input_iterator
                # --- Phase 1: yield already-computed results from the databases ---
                # Retrieve pipeline records with their entry_ids (record IDs)
                # and join with result records to reconstruct (tag, output_packet).
                PIPELINE_ENTRY_ID_COL = "__pipeline_entry_id"
                existing_entry_ids: set[str] = set()

                taginfo = self._pipeline_database.get_all_records(
                    self.pipeline_path,
                    record_id_column=PIPELINE_ENTRY_ID_COL,
                )
                results = self._cached_function_pod._result_database.get_all_records(
                    self._cached_function_pod.record_path,
                    record_id_column=constants.PACKET_RECORD_ID,
                )

                if taginfo is not None and results is not None:
                    joined = (
                        pl.DataFrame(taginfo)
                        .join(
                            pl.DataFrame(results),
                            on=constants.PACKET_RECORD_ID,
                            how="inner",
                        )
                        .to_arrow()
                    )
                    if joined.num_rows > 0:
                        tag_keys = self._input_stream.keys()[0]
                        # Collect pipeline entry_ids for Phase 2 skip check
                        existing_entry_ids = set(
                            cast(
                                list[str],
                                joined.column(PIPELINE_ENTRY_ID_COL).to_pylist(),
                            )
                        )
                        # Drop internal columns before yielding as stream
                        drop_cols = [
                            c
                            for c in joined.column_names
                            if c.startswith(constants.META_PREFIX)
                            or c == PIPELINE_ENTRY_ID_COL
                        ]
                        data_table = joined.drop(
                            [c for c in drop_cols if c in joined.column_names]
                        )
                        existing_stream = ArrowTableStream(
                            data_table, tag_columns=tag_keys
                        )
                        for i, (tag, packet) in enumerate(
                            existing_stream.iter_packets()
                        ):
                            self._cached_output_packets[i] = (tag, packet)
                            yield tag, packet

                # --- Phase 2: process only missing input packets ---
                # Skip inputs whose pipeline entry_id (tag+system_tags+packet_hash)
                # already exists in the pipeline database.
                for tag, packet in input_iter:
                    entry_id = self.compute_pipeline_entry_id(tag, packet)
                    if entry_id in existing_entry_ids:
                        continue
                    tag, output_packet = self._process_packet_internal(tag, packet)
                    if output_packet is not None:
                        yield tag, output_packet

                self._cached_input_iterator = None
            else:
                # Yield from snapshot of complete cache
                for i in range(len(self._cached_output_packets)):
                    tag, packet = self._cached_output_packets[i]
                    if packet is not None:
                        yield tag, packet
        else:
            # Simple iteration without DB
            if self._cached_input_iterator is not None:
                if _executor_supports_concurrent(self._packet_function):
                    yield from self._iter_packets_concurrent(
                        self._cached_input_iterator
                    )
                else:
                    yield from self._iter_packets_sequential(
                        self._cached_input_iterator
                    )
            else:
                for i in range(len(self._cached_output_packets)):
                    tag, packet = self._cached_output_packets[i]
                    if packet is not None:
                        yield tag, packet

    def _iter_packets_sequential(
        self, input_iter: Iterator[tuple[TagProtocol, PacketProtocol]]
    ) -> Iterator[tuple[TagProtocol, PacketProtocol]]:
        for i, (tag, packet) in enumerate(input_iter):
            if i in self._cached_output_packets:
                tag, packet = self._cached_output_packets[i]
                if packet is not None:
                    yield tag, packet
            else:
                tag, output_packet = self._process_packet_internal(tag, packet)
                if output_packet is not None:
                    yield tag, output_packet
        self._cached_input_iterator = None

    def _iter_packets_concurrent(
        self,
        input_iter: Iterator[tuple[TagProtocol, PacketProtocol]],
    ) -> Iterator[tuple[TagProtocol, PacketProtocol]]:
        """Collect remaining inputs, execute concurrently, and yield results in order."""

        all_inputs: list[tuple[int, TagProtocol, PacketProtocol]] = []
        to_compute: list[tuple[int, TagProtocol, PacketProtocol]] = []
        for i, (tag, packet) in enumerate(input_iter):
            all_inputs.append((i, tag, packet))
            if i not in self._cached_output_packets:
                to_compute.append((i, tag, packet))
        self._cached_input_iterator = None

        if to_compute:
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = None

            if loop is not None:
                # Already in event loop — fall back to sequential sync
                for i, tag, pkt in to_compute:
                    self._process_packet_internal(tag, pkt, cache_index=i)
            else:

                async def _gather() -> list[tuple[TagProtocol, PacketProtocol | None]]:
                    return list(
                        await asyncio.gather(
                            *[
                                self._async_process_packet_internal(
                                    tag, pkt, cache_index=i
                                )
                                for i, tag, pkt in to_compute
                            ]
                        )
                    )

                asyncio.run(_gather())

        # Yield all results in order from internal cache
        for idx in sorted(self._cached_output_packets.keys()):
            tag, packet = self._cached_output_packets[idx]
            if packet is not None:
                yield tag, packet

    def run(self) -> None:
        """Eagerly process all input packets, filling the pipeline and result databases."""
        for _ in self.iter_packets():
            pass

    # ------------------------------------------------------------------
    # as_table
    # ------------------------------------------------------------------

    def as_table(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> pa.Table:
        if self._cached_output_table is None:
            all_tags = []
            all_packets = []
            tag_schema, packet_schema = None, None
            for tag, packet in self.iter_packets():
                if tag_schema is None:
                    tag_schema = tag.arrow_schema(all_info=True)
                if packet_schema is None:
                    packet_schema = packet.arrow_schema(all_info=True)
                all_tags.append(tag.as_dict(all_info=True))
                all_packets.append(packet.as_dict(all_info=True))

            converter = self.data_context.type_converter

            struct_packets = converter.python_dicts_to_struct_dicts(all_packets)
            all_tags_as_tables: pa.Table = pa.Table.from_pylist(
                all_tags, schema=tag_schema
            )
            if constants.CONTEXT_KEY in all_tags_as_tables.column_names:
                all_tags_as_tables = all_tags_as_tables.drop([constants.CONTEXT_KEY])
            all_packets_as_tables: pa.Table = pa.Table.from_pylist(
                struct_packets, schema=packet_schema
            )

            self._cached_output_table = arrow_utils.hstack_tables(
                all_tags_as_tables, all_packets_as_tables
            )
        assert self._cached_output_table is not None, (
            "_cached_output_table should not be None here."
        )

        column_config = ColumnConfig.handle_config(columns, all_info=all_info)

        drop_columns = []
        if not column_config.system_tags:
            drop_columns.extend(
                [
                    c
                    for c in self._cached_output_table.column_names
                    if c.startswith(constants.SYSTEM_TAG_PREFIX)
                ]
            )
        if not column_config.source:
            drop_columns.extend(f"{constants.SOURCE_PREFIX}{c}" for c in self.keys()[1])
        if not column_config.context:
            drop_columns.append(constants.CONTEXT_KEY)
        if not column_config.meta:
            drop_columns.extend(
                c
                for c in self._cached_output_table.column_names
                if c.startswith(constants.META_PREFIX)
            )
        elif not isinstance(column_config.meta, bool):
            # Collection[str]: keep only meta columns matching the specified prefixes
            drop_columns.extend(
                c
                for c in self._cached_output_table.column_names
                if c.startswith(constants.META_PREFIX)
                and not any(c.startswith(p) for p in column_config.meta)
            )
        output_table = self._cached_output_table.drop(
            [c for c in drop_columns if c in self._cached_output_table.column_names]
        )

        if column_config.content_hash:
            if self._cached_content_hash_column is None:
                content_hashes = []
                for tag, packet in self.iter_packets():
                    content_hashes.append(packet.content_hash().to_string())
                self._cached_content_hash_column = pa.array(
                    content_hashes, type=pa.large_string()
                )
            assert self._cached_content_hash_column is not None, (
                "_cached_content_hash_column should not be None here."
            )
            hash_column_name = (
                "_content_hash"
                if column_config.content_hash is True
                else column_config.content_hash
            )
            output_table = output_table.append_column(
                hash_column_name, self._cached_content_hash_column
            )

        if column_config.sort_by_tags:
            output_table = (
                pl.DataFrame(output_table)
                .sort(by=self.keys()[0], descending=False)
                .to_arrow()
            )
        return output_table

    # ------------------------------------------------------------------
    # Async channel execution
    # ------------------------------------------------------------------

    async def async_execute(
        self,
        input_channel: ReadableChannel[tuple[TagProtocol, PacketProtocol]],
        output: WritableChannel[tuple[TagProtocol, PacketProtocol]],
        *,
        observer: ExecutionObserverProtocol | None = None,
    ) -> None:
        """Streaming async execution for FunctionNode.

        When a database is attached, uses two-phase execution: replay cached
        results first, then compute missing packets concurrently.  Otherwise,
        routes each packet through ``async_process_packet`` directly.

        In ``CACHE_ONLY`` mode the upstream is unavailable; all cached results
        are served directly from persistent storage without touching the input
        channel.

        Args:
            input_channel: Single readable channel of (tag, packet) pairs.
            output: Writable channel for output (tag, packet) pairs.
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
                f"FunctionNode {self.label!r} is unavailable: "
                "no function pod and no database attached."
            )

        from orcapod.pipeline.observer import NoOpObserver

        node_label = self.label
        node_hash = self.content_hash().to_string()

        obs = observer if observer is not None else NoOpObserver()

        pp = self.pipeline_path

        try:
            # Resolve concurrency limit from node config (pipeline config is not
            # threaded through the orchestrator, so we fall back to defaults).
            node_config = getattr(self._function_pod, "node_config", NodeConfig())
            max_concurrency = resolve_concurrency(node_config, PipelineConfig())

            sem = (
                asyncio.Semaphore(max_concurrency)
                if max_concurrency is not None
                else None
            )

            tag_schema = self._input_stream.output_schema(columns={"system_tags": True})[0]
            obs.on_node_start(node_label, node_hash, pipeline_path=pp, tag_schema=tag_schema)

            if self._cached_function_pod is not None:
                # DB-backed async execution:
                # Phase 1: build cache lookup from pipeline DB
                PIPELINE_ENTRY_ID_COL = "__pipeline_entry_id"
                cached_by_entry_id: dict[
                    str, tuple[TagProtocol, PacketProtocol]
                ] = {}

                taginfo = self._pipeline_database.get_all_records(
                    self.pipeline_path,
                    record_id_column=PIPELINE_ENTRY_ID_COL,
                )
                results = self._cached_function_pod._result_database.get_all_records(
                    self._cached_function_pod.record_path,
                    record_id_column=constants.PACKET_RECORD_ID,
                )

                if taginfo is not None and results is not None:
                    joined = (
                        pl.DataFrame(taginfo)
                        .join(
                            pl.DataFrame(results),
                            on=constants.PACKET_RECORD_ID,
                            how="inner",
                        )
                        .to_arrow()
                    )
                    if joined.num_rows > 0:
                        tag_keys = self._input_stream.keys()[0]
                        entry_ids_col = joined.column(
                            PIPELINE_ENTRY_ID_COL
                        ).to_pylist()
                        drop_cols = [
                            c
                            for c in joined.column_names
                            if c.startswith(constants.META_PREFIX)
                            or c == PIPELINE_ENTRY_ID_COL
                        ]
                        data_table = joined.drop(
                            [c for c in drop_cols if c in joined.column_names]
                        )
                        existing_stream = ArrowTableStream(
                            data_table, tag_columns=tag_keys
                        )
                        for eid, (tag_out, pkt_out) in zip(
                            entry_ids_col, existing_stream.iter_packets()
                        ):
                            cached_by_entry_id[eid] = (tag_out, pkt_out)

                # Phase 2: drive output from input channel — cached or compute
                async def _process_one_db(
                    tag: TagProtocol, packet: PacketProtocol
                ) -> None:
                    entry_id = self.compute_pipeline_entry_id(tag, packet)
                    if entry_id in cached_by_entry_id:
                        tag_out, result_packet = cached_by_entry_id[entry_id]
                        obs.on_packet_start(node_label, tag, packet)
                        obs.on_packet_end(
                            node_label, tag, packet, result_packet, cached=True
                        )
                        await output.send((tag_out, result_packet))
                    else:
                        await self._async_execute_one_packet(
                            tag, packet, output,
                            observer=obs,
                            node_label=node_label,
                            node_hash=node_hash,
                        )

                async with asyncio.TaskGroup() as tg:
                    async for tag, packet in input_channel:
                        async def _guarded_db(
                            t: TagProtocol = tag, p: PacketProtocol = packet
                        ) -> None:
                            try:
                                await _process_one_db(t, p)
                            finally:
                                if sem is not None:
                                    sem.release()

                        if sem is not None:
                            await sem.acquire()
                        tg.create_task(_guarded_db())
            else:
                # Simple async execution without DB
                async with asyncio.TaskGroup() as tg:
                    async for tag, packet in input_channel:
                        async def _guarded_simple(
                            t: TagProtocol = tag, p: PacketProtocol = packet
                        ) -> None:
                            try:
                                await self._async_execute_one_packet(
                                    t, p, output,
                                    observer=obs,
                                    node_label=node_label,
                                    node_hash=node_hash,
                                )
                            finally:
                                if sem is not None:
                                    sem.release()

                        if sem is not None:
                            await sem.acquire()
                        tg.create_task(_guarded_simple())

            obs.on_node_end(node_label, node_hash, pipeline_path=pp)
        finally:
            await output.close()

    async def _async_execute_one_packet(
        self,
        tag: TagProtocol,
        packet: PacketProtocol,
        output: WritableChannel[tuple[TagProtocol, PacketProtocol]],
        *,
        observer: ExecutionObserverProtocol,
        node_label: str,
        node_hash: str,
    ) -> None:
        """Process one non-cached packet in the async execute path."""
        pp = self.pipeline_path

        observer.on_packet_start(node_label, tag, packet)
        ctx_obs = observer.contextualize(node_hash, node_label)
        pkt_logger = ctx_obs.create_packet_logger(tag, packet, pipeline_path=pp)

        try:
            tag_out, result_packet = await self._async_process_packet_internal(
                tag, packet, logger=pkt_logger
            )
        except Exception as exc:
            logger.warning(
                "Packet execution failed in %s: %s", node_label, exc,
                exc_info=True,
            )
            observer.on_packet_crash(node_label, tag, packet, exc)
        else:
            observer.on_packet_end(
                node_label, tag, packet, result_packet, cached=False
            )
            if result_packet is not None:
                await output.send((tag_out, result_packet))

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}(packet_function={self._packet_function!r}, "
            f"input_stream={self._input_stream!r})"
        )
