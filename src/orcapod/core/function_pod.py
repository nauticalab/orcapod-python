from __future__ import annotations

import asyncio
import logging
from abc import abstractmethod
from collections.abc import Callable, Collection, Iterator, Sequence
from functools import wraps
from typing import TYPE_CHECKING, Any, Protocol, cast

from orcapod import contexts
from orcapod.channels import ReadableChannel, WritableChannel
from orcapod.config import Config
from orcapod.core.base import TraceableBase
from orcapod.core.packet_function import CachedPacketFunction, PythonPacketFunction
from orcapod.core.streams.base import StreamBase
from orcapod.core.tracker import DEFAULT_TRACKER_MANAGER
from orcapod.protocols.core_protocols import (
    ArgumentGroup,
    FunctionPodProtocol,
    PacketFunctionExecutorProtocol,
    PacketFunctionProtocol,
    PacketProtocol,
    PodProtocol,
    StreamProtocol,
    TagProtocol,
    TrackerManagerProtocol,
)
from orcapod.protocols.database_protocols import ArrowDatabaseProtocol
from orcapod.system_constants import constants
from orcapod.types import (
    ColumnConfig,
    NodeConfig,
    PipelineConfig,
    Schema,
    resolve_concurrency,
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


def _executor_supports_concurrent(
    packet_function: PacketFunctionProtocol,
) -> bool:
    """Return True if the packet function's executor supports concurrent execution."""
    executor = packet_function.executor
    return executor is not None and executor.supports_concurrent_execution


class _FunctionPodBase(TraceableBase):
    """Base pod that applies a packet function to each input packet."""

    def __init__(
        self,
        packet_function: PacketFunctionProtocol,
        tracker_manager: TrackerManagerProtocol | None = None,
        label: str | None = None,
        data_context: str | contexts.DataContext | None = None,
        config: Config | None = None,
    ) -> None:
        super().__init__(
            label=label,
            data_context=data_context,
            config=config,
        )
        self.tracker_manager = tracker_manager or DEFAULT_TRACKER_MANAGER
        self._packet_function = packet_function
        self._output_schema_hash = None

    def computed_label(self) -> str | None:
        """Use the packet function's canonical name as the default label."""
        return self._packet_function.canonical_function_name

    @property
    def packet_function(self) -> PacketFunctionProtocol:
        return self._packet_function

    @property
    def executor(self) -> PacketFunctionExecutorProtocol | None:
        """The executor set on the underlying packet function, or ``None``."""
        return self._packet_function.executor

    @executor.setter
    def executor(self, executor: PacketFunctionExecutorProtocol | None) -> None:
        """Set or clear the executor on the underlying packet function."""
        self._packet_function.executor = executor

    def identity_structure(self) -> Any:
        return self.packet_function.identity_structure()

    def pipeline_identity_structure(self) -> Any:
        return self.packet_function

    @property
    def uri(self) -> tuple[str, ...]:
        if self._output_schema_hash is None:
            self._output_schema_hash = self.data_context.semantic_hasher.hash_object(
                # hash the vanilla output schema with no extra columns
                self.packet_function.output_packet_schema
            ).to_string()
        return (
            self.packet_function.canonical_function_name,
            self._output_schema_hash,
            f"v{self.packet_function.major_version}",
            self.packet_function.packet_function_type_id,
        )

    def multi_stream_handler(self) -> PodProtocol:
        from orcapod.core.operators import Join

        return Join()

    def validate_inputs(self, *streams: StreamProtocol) -> None:
        """Validate input streams, raising exceptions if invalid.

        Args:
            *streams: Input streams to validate.

        Raises:
            ValueError: If inputs are incompatible with the packet function schema.
        """
        input_stream = self.handle_input_streams(*streams)
        _, incoming_packet_schema = input_stream.output_schema()
        self._validate_input_schema(incoming_packet_schema)

    def _validate_input_schema(self, input_schema: Schema) -> None:
        expected_packet_schema = self.packet_function.input_packet_schema
        if not schema_utils.check_schema_compatibility(
            input_schema, expected_packet_schema
        ):
            # TODO: use custom exception type for better error handling
            raise ValueError(
                f"Incoming packet data type {input_schema} is not compatible with expected input schema {expected_packet_schema}"
            )

    def process_packet(
        self, tag: TagProtocol, packet: PacketProtocol
    ) -> tuple[TagProtocol, PacketProtocol | None]:
        """Process a single packet using the pod's packet function.

        Args:
            tag: The tag associated with the packet.
            packet: The input packet to process.

        Returns:
            A ``(tag, output_packet)`` tuple; output_packet is ``None`` if
            the function filters the packet out.
        """
        return tag, self.packet_function.call(packet)

    async def async_process_packet(
        self, tag: TagProtocol, packet: PacketProtocol
    ) -> tuple[TagProtocol, PacketProtocol | None]:
        """Async counterpart of ``process_packet``."""
        return tag, await self.packet_function.async_call(packet)

    def handle_input_streams(self, *streams: StreamProtocol) -> StreamProtocol:
        """Handle multiple input streams by joining them if necessary.

        Args:
            *streams: Input streams to handle.
        """
        # handle multiple input streams
        if len(streams) == 0:
            raise ValueError("At least one input stream is required")
        elif len(streams) > 1:
            # TODO: simplify the multi-stream handling logic
            multi_stream_handler = self.multi_stream_handler()
            joined_stream = multi_stream_handler.process(*streams)
            return joined_stream
        return streams[0]

    @abstractmethod
    def process(
        self, *streams: StreamProtocol, label: str | None = None
    ) -> StreamProtocol:
        """Invoke the packet processor on the input stream(s).

        If multiple streams are passed in, they are joined before processing.

        Args:
            *streams: Input streams to process.
            label: Optional label for tracking.

        Returns:
            The resulting output stream.
        """
        ...

    def __call__(
        self, *streams: StreamProtocol, label: str | None = None
    ) -> StreamProtocol:
        """Convenience alias for ``process``."""
        logger.debug(f"Invoking pod {self} on streams through __call__: {streams}")
        # perform input stream validation
        return self.process(*streams, label=label)

    def argument_symmetry(self, streams: Collection[StreamProtocol]) -> ArgumentGroup:
        return self.multi_stream_handler().argument_symmetry(streams)

    def output_schema(
        self,
        *streams: StreamProtocol,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        tag_schema, incoming_packet_schema = self.multi_stream_handler().output_schema(
            *streams, columns=columns, all_info=all_info
        )
        # validate that incoming_packet_schema is valid
        self._validate_input_schema(incoming_packet_schema)
        # The output schema of the FunctionPodProtocol is determined by the packet function
        # TODO: handle and extend to include additional columns
        # Namely, the source columns
        return tag_schema, self.packet_function.output_packet_schema


class FunctionPod(_FunctionPodBase):
    def __init__(
        self,
        packet_function: PacketFunctionProtocol,
        node_config: NodeConfig | None = None,
        **kwargs,
    ) -> None:
        super().__init__(packet_function, **kwargs)
        self._node_config = node_config or NodeConfig()

    @property
    def node_config(self) -> NodeConfig:
        return self._node_config

    def process(
        self, *streams: StreamProtocol, label: str | None = None
    ) -> FunctionPodStream:
        """Invoke the packet processor on the input stream(s).

        Args:
            *streams: Input streams to process.
            label: Optional label for tracking.

        Returns:
            A ``FunctionPodStream`` wrapping the computation.
        """
        logger.debug(f"Invoking kernel {self} on streams: {streams}")

        input_stream = self.handle_input_streams(*streams)

        # perform input stream schema validation
        self._validate_input_schema(input_stream.output_schema()[1])
        self.tracker_manager.record_function_pod_invocation(
            self, input_stream, label=label
        )
        output_stream = FunctionPodStream(
            function_pod=self,
            input_stream=input_stream,
            label=label,
        )
        return output_stream

    def __call__(
        self, *streams: StreamProtocol, label: str | None = None
    ) -> FunctionPodStream:
        """Convenience alias for ``process``."""
        logger.debug(f"Invoking pod {self} on streams through __call__: {streams}")
        # perform input stream validation
        return self.process(*streams, label=label)

    def to_config(self) -> dict[str, Any]:
        """Serialize this function pod to a JSON-compatible config dict.

        Returns:
            A JSON-serializable dict containing the URI, packet function config,
            and node config for this function pod.
        """
        config: dict[str, Any] = {
            "uri": list(self.uri),
            "packet_function": self.packet_function.to_config(),
            "node_config": None,
        }
        if (
            self._node_config is not None
            and self._node_config.max_concurrency is not None
        ):
            config["node_config"] = {
                "max_concurrency": self._node_config.max_concurrency,
            }
        return config

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> "FunctionPod":
        """Reconstruct a FunctionPod from a config dict.

        Args:
            config: A dict as produced by :meth:`to_config`.

        Returns:
            A new ``FunctionPod`` instance.
        """
        from orcapod.pipeline.serialization import resolve_packet_function_from_config

        pf_config = config["packet_function"]
        packet_function = resolve_packet_function_from_config(pf_config)

        node_config = None
        if config.get("node_config") is not None:
            node_config = NodeConfig(**config["node_config"])

        return cls(packet_function=packet_function, node_config=node_config)

    # ------------------------------------------------------------------
    # Async channel execution (streaming mode)
    # ------------------------------------------------------------------

    async def async_execute(
        self,
        inputs: Sequence[ReadableChannel[tuple[TagProtocol, PacketProtocol]]],
        output: WritableChannel[tuple[TagProtocol, PacketProtocol]],
        pipeline_config: PipelineConfig | None = None,
    ) -> None:
        """Streaming async execution with per-packet concurrency control.

        Each input (tag, packet) is processed independently. A semaphore
        controls how many packets are in-flight concurrently.
        """
        try:
            pipeline_config = pipeline_config or PipelineConfig()
            max_concurrency = resolve_concurrency(self._node_config, pipeline_config)

            sem = (
                asyncio.Semaphore(max_concurrency)
                if max_concurrency is not None
                else None
            )

            async def process_one(tag: TagProtocol, packet: PacketProtocol) -> None:
                try:
                    tag, result_packet = await self.async_process_packet(tag, packet)
                    if result_packet is not None:
                        await output.send((tag, result_packet))
                finally:
                    if sem is not None:
                        sem.release()

            async with asyncio.TaskGroup() as tg:
                async for tag, packet in inputs[0]:
                    if sem is not None:
                        await sem.acquire()
                    tg.create_task(process_one(tag, packet))
        finally:
            await output.close()


class FunctionPodStream(StreamBase):
    """Recomputable stream wrapping a packet function."""

    def __init__(
        self, function_pod: FunctionPodProtocol, input_stream: StreamProtocol, **kwargs
    ) -> None:
        self._function_pod = function_pod
        self._input_stream = input_stream
        super().__init__(**kwargs)

        # Iterator acquired lazily on first use to avoid triggering upstream
        # computation during construction.
        self._cached_input_iterator: (
            Iterator[tuple[TagProtocol, PacketProtocol]] | None
        ) = None
        self._needs_iterator = True

        # PacketProtocol-level caching (for the output packets)
        self._cached_output_packets: dict[
            int, tuple[TagProtocol, PacketProtocol | None]
        ] = {}
        self._cached_output_table: pa.Table | None = None
        self._cached_content_hash_column: pa.Array | None = None

    @property
    def producer(self) -> PodProtocol:
        return self._function_pod

    @property
    def executor(self) -> PacketFunctionExecutorProtocol | None:
        """The executor set on the underlying packet function."""
        return self._function_pod.packet_function.executor

    @executor.setter
    def executor(self, executor: PacketFunctionExecutorProtocol | None) -> None:
        """Set or clear the executor on the underlying packet function."""
        self._function_pod.packet_function.executor = executor

    @property
    def upstreams(self) -> tuple[StreamProtocol, ...]:
        return (self._input_stream,)

    def identity_structure(self) -> Any:
        return (
            self._function_pod,
            self._function_pod.argument_symmetry((self._input_stream,)),
        )

    def pipeline_identity_structure(self) -> Any:
        return self.identity_structure()

    def keys(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
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
        return self._function_pod.output_schema(
            self._input_stream, columns=columns, all_info=all_info
        )

    def _ensure_iterator(self) -> None:
        """Lazily acquire the upstream iterator on first use."""
        if self._needs_iterator:
            self._cached_input_iterator = self._input_stream.iter_packets()
            self._needs_iterator = False
            self._update_modified_time()

    def clear_cache(self) -> None:
        """Discard all in-memory cached state."""
        self._cached_input_iterator = None
        self._needs_iterator = True
        self._cached_output_packets.clear()
        self._cached_output_table = None
        self._cached_content_hash_column = None
        self._update_modified_time()

    def __iter__(self) -> Iterator[tuple[TagProtocol, PacketProtocol]]:
        return self.iter_packets()

    def iter_packets(self) -> Iterator[tuple[TagProtocol, PacketProtocol]]:
        if self.is_stale:
            self.clear_cache()
        self._ensure_iterator()
        if self._cached_input_iterator is not None:
            if _executor_supports_concurrent(self._function_pod.packet_function):
                yield from self._iter_packets_concurrent()
            else:
                yield from self._iter_packets_sequential()
        else:
            # Yield from snapshot of complete cache
            for i in range(len(self._cached_output_packets)):
                tag, packet = self._cached_output_packets[i]
                if packet is not None:
                    yield tag, packet

    def _iter_packets_sequential(
        self,
    ) -> Iterator[tuple[TagProtocol, PacketProtocol]]:
        input_iter = self._cached_input_iterator
        assert input_iter is not None
        for i, (tag, packet) in enumerate(input_iter):
            if i in self._cached_output_packets:
                # Use cached result
                tag, packet = self._cached_output_packets[i]
                if packet is not None:
                    yield tag, packet
            else:
                # Process packet
                tag, output_packet = self._function_pod.process_packet(tag, packet)
                self._cached_output_packets[i] = (tag, output_packet)
                if output_packet is not None:
                    yield tag, output_packet

        # Mark completion by releasing the iterator
        self._cached_input_iterator = None

    def _iter_packets_concurrent(
        self,
    ) -> Iterator[tuple[TagProtocol, PacketProtocol]]:
        """Collect remaining inputs, execute concurrently, and yield results in order."""
        input_iter = self._cached_input_iterator
        assert input_iter is not None

        # Materialise remaining inputs and separate cached from uncached.
        all_inputs: list[tuple[int, TagProtocol, PacketProtocol]] = []
        to_compute: list[tuple[int, TagProtocol, PacketProtocol]] = []
        for i, (tag, packet) in enumerate(input_iter):
            all_inputs.append((i, tag, packet))
            if i not in self._cached_output_packets:
                to_compute.append((i, tag, packet))
        self._cached_input_iterator = None

        # Submit uncached packets concurrently via async_process_packet.
        if to_compute:
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = None

            if loop is not None:
                # Already in event loop — fall back to sequential sync
                results = [
                    self._function_pod.process_packet(tag, pkt)
                    for _, tag, pkt in to_compute
                ]
            else:

                async def _gather() -> list[tuple[TagProtocol, PacketProtocol | None]]:
                    return list(
                        await asyncio.gather(
                            *[
                                self._function_pod.async_process_packet(tag, pkt)
                                for _, tag, pkt in to_compute
                            ]
                        )
                    )

                results = asyncio.run(_gather())

            for (i, _, _), (tag, output_packet) in zip(to_compute, results):
                self._cached_output_packets[i] = (tag, output_packet)

        # Yield everything in original order.
        for i, *_ in all_inputs:
            tag, packet = self._cached_output_packets[i]
            if packet is not None:
                yield tag, packet

    def as_table(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> "pa.Table":
        if self._cached_output_table is None:
            all_tags = []
            all_packets = []
            tag_schema, packet_schema = None, None
            for tag, packet in self.iter_packets():
                if tag_schema is None:
                    tag_schema = tag.arrow_schema(all_info=True)
                if packet_schema is None:
                    packet_schema = packet.arrow_schema(all_info=True)
                # TODO: make use of arrow_compat dict
                all_tags.append(tag.as_dict(all_info=True))
                all_packets.append(packet.as_dict(all_info=True))

            # TODO: re-verify the implemetation of this conversion
            converter = self.data_context.type_converter

            struct_packets = converter.python_dicts_to_struct_dicts(all_packets)
            all_tags_as_tables: pa.Table = pa.Table.from_pylist(
                all_tags, schema=tag_schema
            )
            # drop context key column from tags table (guard: column absent on empty stream)
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
            # TODO: get system tags more effiicently
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

        output_table = self._cached_output_table.drop(
            [c for c in drop_columns if c in self._cached_output_table.column_names]
        )

        # lazily prepare content hash column if requested
        if column_config.content_hash:
            if self._cached_content_hash_column is None:
                content_hashes = []
                # TODO: verify that order will be preserved
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
            # TODO: reimplement using polars natively
            output_table = (
                pl.DataFrame(output_table)
                .sort(by=self.keys()[0], descending=False)
                .to_arrow()
            )
            # output_table = output_table.sort_by(
            #     [(column, "ascending") for column in self.keys()[0]]
            # )
        return output_table


class CallableWithPod(Protocol):
    @property
    def pod(self) -> _FunctionPodBase:
        """Return the associated function pod."""
        ...

    def __call__(self, *args, **kwargs):
        """Call the underlying function."""
        ...


def function_pod(
    output_keys: str | Sequence[str] | None = None,
    function_name: str | None = None,
    version: str = "v0.0",
    label: str | None = None,
    result_database: ArrowDatabaseProtocol | None = None,
    pod_cache_database: ArrowDatabaseProtocol | None = None,
    executor: PacketFunctionExecutorProtocol | None = None,
    **kwargs,
) -> Callable[..., CallableWithPod]:
    """Decorator that attaches a ``FunctionPod`` as a ``pod`` attribute.

    Args:
        output_keys: Keys for the function output(s).
        function_name: Name of the function pod; defaults to ``func.__name__``.
        version: Version string for the packet function.
        label: Optional label for tracking.
        result_database: Optional database for packet-level caching
            (wraps the packet function in ``CachedPacketFunction``).
        pod_cache_database: Optional database for pod-level caching
            (wraps the pod in ``CachedFunctionPod``, which caches at the
            ``process_packet`` level using input packet content hash).
        executor: Optional executor for running the packet function.
        **kwargs: Forwarded to ``PythonPacketFunction``.

    Returns:
        A decorator that adds a ``pod`` attribute to the wrapped function.
    """

    def decorator(func: Callable) -> CallableWithPod:
        if func.__name__ == "<lambda>":
            raise ValueError("Lambda functions cannot be used with function_pod")

        packet_function = PythonPacketFunction(
            func,
            output_keys=output_keys,
            function_name=function_name or func.__name__,
            version=version,
            label=label,
            executor=executor,
            **kwargs,
        )

        # if database is provided, wrap in CachedPacketFunction
        if result_database is not None:
            packet_function = CachedPacketFunction(
                packet_function,
                result_database=result_database,
            )

        # Create a simple typed function pod
        pod: _FunctionPodBase = FunctionPod(
            packet_function=packet_function,
        )

        # if pod_cache_database is provided, wrap in CachedFunctionPod
        if pod_cache_database is not None:
            from orcapod.core.cached_function_pod import CachedFunctionPod

            pod = CachedFunctionPod(
                function_pod=pod,
                result_database=pod_cache_database,
            )

        @wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        setattr(wrapper, "pod", pod)
        return cast(CallableWithPod, wrapper)

    return decorator


class WrappedFunctionPod(_FunctionPodBase):
    """Wrapper for a function pod, delegating call logic to the inner pod."""

    def __init__(
        self,
        function_pod: FunctionPodProtocol,
        data_context: str | contexts.DataContext | None = None,
        **kwargs,
    ) -> None:
        # if data_context is not explicitly given, use that of the contained pod
        if data_context is None:
            data_context = function_pod.data_context_key
        super().__init__(
            packet_function=function_pod.packet_function,
            data_context=data_context,
            **kwargs,
        )
        self._function_pod = function_pod

    def computed_label(self) -> str | None:
        return self._function_pod.label

    @property
    def uri(self) -> tuple[str, ...]:
        return self._function_pod.uri

    def validate_inputs(self, *streams: StreamProtocol) -> None:
        self._function_pod.validate_inputs(*streams)

    def argument_symmetry(self, streams: Collection[StreamProtocol]) -> ArgumentGroup:
        return self._function_pod.argument_symmetry(streams)

    def output_schema(
        self,
        *streams: StreamProtocol,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        return self._function_pod.output_schema(
            *streams, columns=columns, all_info=all_info
        )

    # TODO: reconsider whether to return FunctionPodStream here in the signature
    def process(
        self, *streams: StreamProtocol, label: str | None = None
    ) -> StreamProtocol:
        return self._function_pod.process(*streams, label=label)
