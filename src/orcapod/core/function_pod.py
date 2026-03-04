from __future__ import annotations

import asyncio
import logging
from abc import abstractmethod
from collections.abc import Callable, Collection, Iterator, Sequence
from typing import TYPE_CHECKING, Any, Protocol, cast

from orcapod import contexts
from orcapod.channels import ReadableChannel, WritableChannel
from orcapod.config import Config
from orcapod.core.base import TraceableBase
from orcapod.core.packet_function import CachedPacketFunction, PythonPacketFunction
from orcapod.core.streams.arrow_table_stream import ArrowTableStream
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
from orcapod.types import ColumnConfig, NodeConfig, PipelineConfig, Schema, resolve_concurrency
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

            sem = asyncio.Semaphore(max_concurrency) if max_concurrency is not None else None

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

        # capture the iterator over the input stream
        self._cached_input_iterator: (
            Iterator[tuple[TagProtocol, PacketProtocol]] | None
        ) = input_stream.iter_packets()
        self._update_modified_time()  # update the modified time to AFTER we obtain the iterator
        # note that the invocation of iter_packets on upstream likely triggeres the modified time
        # to be updated on the usptream. Hence you want to set this stream's modified time after that.

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

    def clear_cache(self) -> None:
        """Discard all in-memory cached state and re-acquire the input iterator."""
        self._cached_input_iterator = self._input_stream.iter_packets()
        self._cached_output_packets.clear()
        self._cached_output_table = None
        self._cached_content_hash_column = None
        self._update_modified_time()

    def __iter__(self) -> Iterator[tuple[TagProtocol, PacketProtocol]]:
        return self.iter_packets()

    def iter_packets(self) -> Iterator[tuple[TagProtocol, PacketProtocol]]:
        if self.is_stale:
            self.clear_cache()
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
    executor: PacketFunctionExecutorProtocol | None = None,
    **kwargs,
) -> Callable[..., CallableWithPod]:
    """Decorator that attaches a ``FunctionPod`` as a ``pod`` attribute.

    Args:
        output_keys: Keys for the function output(s).
        function_name: Name of the function pod; defaults to ``func.__name__``.
        version: Version string for the packet function.
        label: Optional label for tracking.
        result_database: Optional database for caching results.
        executor: Optional executor for running the packet function.
        **kwargs: Forwarded to ``PythonPacketFunction``.

    Returns:
        A decorator that adds a ``pod`` attribute to the wrapped function.
    """

    def decorator(func: Callable) -> CallableWithPod:
        if func.__name__ == "<lambda>":
            raise ValueError("Lambda functions cannot be used with function_pod")

        # Store the original function in the module for pickling purposes
        # and make sure to change the name of the function

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
        pod = FunctionPod(
            packet_function=packet_function,
        )
        setattr(func, "pod", pod)
        return cast(CallableWithPod, func)

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


class FunctionNode(StreamBase):
    """Non-persistent stream node representing a packet function invocation.

    Provides the core stream interface (identity, schema, iteration) without
    any database persistence.  Subclass ``PersistentFunctionNode`` adds
    DB-backed caching and pipeline record storage.
    """

    node_type = "function"

    def __init__(
        self,
        function_pod: FunctionPodProtocol,
        input_stream: StreamProtocol,
        tracker_manager: TrackerManagerProtocol | None = None,
        label: str | None = None,
        data_context: str | contexts.DataContext | None = None,
        config: Config | None = None,
    ):
        if tracker_manager is None:
            tracker_manager = DEFAULT_TRACKER_MANAGER
        self.tracker_manager = tracker_manager
        self._packet_function = function_pod.packet_function

        # FunctionPod used for the `producer` property and pipeline identity
        self._function_pod = function_pod
        super().__init__(
            label=label,
            data_context=data_context,
            config=config,
        )

        # validate the input stream
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

        # stream-level caching state
        self._cached_input_iterator: (
            Iterator[tuple[TagProtocol, PacketProtocol]] | None
        ) = input_stream.iter_packets()
        self._update_modified_time()  # set modified time AFTER obtaining the iterator
        self._cached_output_packets: dict[
            int, tuple[TagProtocol, PacketProtocol | None]
        ] = {}
        self._cached_output_table: pa.Table | None = None
        self._cached_content_hash_column: pa.Array | None = None

    @property
    def producer(self) -> FunctionPodProtocol:
        return self._function_pod

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
        tag_schema = self._input_stream.output_schema(
            columns=columns, all_info=all_info
        )[0]
        return tag_schema, self._packet_function.output_packet_schema

    def clear_cache(self) -> None:
        self._cached_input_iterator = self._input_stream.iter_packets()
        self._cached_output_packets.clear()
        self._cached_output_table = None
        self._cached_content_hash_column = None
        self._update_modified_time()

    def iter_packets(self) -> Iterator[tuple[TagProtocol, PacketProtocol]]:
        if self.is_stale:
            self.clear_cache()
        if self._cached_input_iterator is not None:
            if _executor_supports_concurrent(self._packet_function):
                yield from self._iter_packets_concurrent()
            else:
                yield from self._iter_packets_sequential()
        else:
            for i in range(len(self._cached_output_packets)):
                tag, packet = self._cached_output_packets[i]
                if packet is not None:
                    yield tag, packet

    def process_packet(
        self, tag: TagProtocol, packet: PacketProtocol
    ) -> tuple[TagProtocol, PacketProtocol | None]:
        """Process a single packet by delegating to the function pod."""
        return self._function_pod.process_packet(tag, packet)

    async def async_process_packet(
        self, tag: TagProtocol, packet: PacketProtocol
    ) -> tuple[TagProtocol, PacketProtocol | None]:
        """Async counterpart of ``process_packet``."""
        return await self._function_pod.async_process_packet(tag, packet)

    def _iter_packets_sequential(
        self,
    ) -> Iterator[tuple[TagProtocol, PacketProtocol]]:
        input_iter = self._cached_input_iterator
        for i, (tag, packet) in enumerate(input_iter):
            if i in self._cached_output_packets:
                tag, packet = self._cached_output_packets[i]
                if packet is not None:
                    yield tag, packet
            else:
                tag, output_packet = self.process_packet(tag, packet)
                self._cached_output_packets[i] = (tag, output_packet)
                if output_packet is not None:
                    yield tag, output_packet
        self._cached_input_iterator = None

    def _iter_packets_concurrent(
        self,
    ) -> Iterator[tuple[TagProtocol, PacketProtocol]]:
        """Collect remaining inputs, execute concurrently, and yield results in order."""
        input_iter = self._cached_input_iterator

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
                results = [
                    self.process_packet(tag, pkt)
                    for _, tag, pkt in to_compute
                ]
            else:
                async def _gather() -> list[tuple[TagProtocol, PacketProtocol | None]]:
                    return list(
                        await asyncio.gather(
                            *[
                                self.async_process_packet(tag, pkt)
                                for _, tag, pkt in to_compute
                            ]
                        )
                    )

                results = asyncio.run(_gather())

            for (i, _, _), (tag, output_packet) in zip(to_compute, results):
                self._cached_output_packets[i] = (tag, output_packet)

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
        inputs: Sequence[ReadableChannel[tuple[TagProtocol, PacketProtocol]]],
        output: WritableChannel[tuple[TagProtocol, PacketProtocol]],
        pipeline_config: PipelineConfig | None = None,
    ) -> None:
        """Streaming async execution for FunctionNode."""
        try:
            pipeline_config = pipeline_config or PipelineConfig()
            node_config = (
                self._function_pod.node_config
                if hasattr(self._function_pod, "node_config")
                else NodeConfig()
            )
            max_concurrency = resolve_concurrency(node_config, pipeline_config)
            sem = asyncio.Semaphore(max_concurrency) if max_concurrency is not None else None

            async def process_one(tag: TagProtocol, packet: PacketProtocol) -> None:
                try:
                    result_packet = self._packet_function.call(packet)
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

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}(packet_function={self._packet_function!r}, "
            f"input_stream={self._input_stream!r})"
        )


class PersistentFunctionNode(FunctionNode):
    """DB-backed stream node that applies a cached packet function to an input stream.

    Extends ``FunctionNode`` with result caching via ``CachedPacketFunction``,
    pipeline record storage, and two-phase iteration (cached first, then compute
    missing).
    """

    def __init__(
        self,
        function_pod: FunctionPodProtocol,
        input_stream: StreamProtocol,
        pipeline_database: ArrowDatabaseProtocol,
        result_database: ArrowDatabaseProtocol | None = None,
        result_path_prefix: tuple[str, ...] | None = None,
        pipeline_path_prefix: tuple[str, ...] = (),
        tracker_manager: TrackerManagerProtocol | None = None,
        label: str | None = None,
        data_context: str | contexts.DataContext | None = None,
        config: Config | None = None,
    ):
        super().__init__(
            function_pod=function_pod,
            input_stream=input_stream,
            tracker_manager=tracker_manager,
            label=label,
            data_context=data_context,
            config=config,
        )

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

        # replace the packet function with a cached version
        self._packet_function = CachedPacketFunction(
            self._packet_function,
            result_database=result_database,
            record_path_prefix=computed_result_path_prefix,
        )

        self._pipeline_database = pipeline_database
        self._pipeline_path_prefix = pipeline_path_prefix

        # use pipeline_hash() (schema+topology only), not content_hash() (data-inclusive)
        self._pipeline_node_hash = self.pipeline_hash().to_string()

        self._output_schema_hash = self.data_context.semantic_hasher.hash_object(
            self._packet_function.output_packet_schema
        ).to_string()

    def identity_structure(self) -> Any:
        return (self._packet_function, self._input_stream)

    def pipeline_identity_structure(self) -> Any:
        return (self._packet_function, self._input_stream)

    @property
    def pipeline_path(self) -> tuple[str, ...]:
        return (
            self._pipeline_path_prefix
            + self._packet_function.uri
            + (f"node:{self._pipeline_node_hash}",)
        )

    def process_packet(
        self,
        tag: TagProtocol,
        packet: PacketProtocol,
        skip_cache_lookup: bool = False,
        skip_cache_insert: bool = False,
    ) -> tuple[TagProtocol, PacketProtocol | None]:
        """Process a single packet, recording the result in the pipeline database.

        Args:
            tag: The tag associated with the packet.
            packet: The input packet to process.
            skip_cache_lookup: If True, bypass DB lookup for existing result.
            skip_cache_insert: If True, skip writing result to DB.

        Returns:
            A ``(tag, output_packet)`` tuple; output_packet is ``None`` if
            the function filters the packet out.
        """
        output_packet = self._packet_function.call(
            packet,
            skip_cache_lookup=skip_cache_lookup,
            skip_cache_insert=skip_cache_insert,
        )

        if output_packet is not None:
            # check if the packet was computed or retrieved from cache
            result_computed = bool(
                output_packet.get_meta_value(
                    self._packet_function.RESULT_COMPUTED_FLAG, False
                )
            )
            self.add_pipeline_record(
                tag,
                packet,
                packet_record_id=output_packet.datagram_id,
                computed=result_computed,
            )

        return tag, output_packet

    async def async_process_packet(
        self,
        tag: TagProtocol,
        packet: PacketProtocol,
        skip_cache_lookup: bool = False,
        skip_cache_insert: bool = False,
    ) -> tuple[TagProtocol, PacketProtocol | None]:
        """Async counterpart of ``process_packet``.

        Uses the CachedPacketFunction's async_call for computation + result
        caching.  Pipeline record storage is synchronous (DB protocol is sync).
        """
        output_packet = await self._packet_function.async_call(
            packet,
            skip_cache_lookup=skip_cache_lookup,
            skip_cache_insert=skip_cache_insert,
        )

        if output_packet is not None:
            result_computed = bool(
                output_packet.get_meta_value(
                    self._packet_function.RESULT_COMPUTED_FLAG, False
                )
            )
            self.add_pipeline_record(
                tag,
                packet,
                packet_record_id=output_packet.datagram_id,
                computed=result_computed,
            )

        return tag, output_packet

    def add_pipeline_record(
        self,
        tag: TagProtocol,
        input_packet: PacketProtocol,
        packet_record_id: str,
        computed: bool,
        skip_cache_lookup: bool = False,
    ) -> None:
        # combine TagProtocol with packet content hash to compute entry hash
        # TODO: add system tag columns
        # TODO: consider using bytes instead of string representation
        tag_with_hash = tag.as_table(columns={"system_tags": True}).append_column(
            constants.INPUT_PACKET_HASH_COL,
            pa.array([input_packet.content_hash().to_string()], type=pa.large_string()),
        )

        # unique entry ID is determined by the combination of tags, system_tags, and input_packet hash
        entry_id = self.data_context.arrow_hasher.hash_table(tag_with_hash).to_string()

        # check presence of an existing entry with the same entry_id
        existing_record = None
        if not skip_cache_lookup:
            existing_record = self._pipeline_database.get_record_by_id(
                self.pipeline_path,
                entry_id,
            )

        if existing_record is not None:
            # if the record already exists, then skip adding
            logger.debug(
                f"Record with entry_id {entry_id} already exists. Skipping addition."
            )
            return

        # rename all keys to avoid potential collision with result columns
        renamed_input_packet = input_packet.rename(
            {k: f"_input_{k}" for k in input_packet.keys()}
        )
        input_packet_info = (
            renamed_input_packet.as_table(columns={"source": True})
            .append_column(
                constants.PACKET_RECORD_ID,  # record ID for the packet function output packet
                pa.array([packet_record_id], type=pa.large_string()),
            )
            .append_column(
                f"{constants.META_PREFIX}input_packet{constants.CONTEXT_KEY}",  # data context key for the input packet
                pa.array([input_packet.data_context_key], type=pa.large_string()),
            )
            .append_column(
                f"{constants.META_PREFIX}computed",
                pa.array([computed], type=pa.bool_()),
            )
            .drop_columns(list(renamed_input_packet.keys()))
        )

        combined_record = arrow_utils.hstack_tables(
            tag.as_table(columns={"system_tags": True}), input_packet_info
        )

        self._pipeline_database.add_record(
            self.pipeline_path,
            entry_id,
            combined_record,
            skip_duplicates=False,
        )

    def get_all_records(
        self,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> "pa.Table | None":
        """Return all computed results joined with their pipeline tag records.

        Args:
            columns: Column configuration controlling which groups are included.
            all_info: Shorthand to include all info columns.

        Returns:
            A PyArrow table of joined results, or ``None`` if no records exist.
        """
        results = self._packet_function._result_database.get_all_records(
            self._packet_function.record_path,
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

    def iter_packets(self) -> Iterator[tuple[TagProtocol, PacketProtocol]]:
        if self.is_stale:
            self.clear_cache()
        if self._cached_input_iterator is not None:
            input_iter = self._cached_input_iterator
            # --- Phase 1: yield already-computed results from the databases ---
            existing = self.get_all_records(columns={"meta": True})
            computed_hashes: set[str] = set()
            if existing is not None and existing.num_rows > 0:
                tag_keys = self._input_stream.keys()[0]
                # Strip the meta column before handing to ArrowTableStream so it only
                # sees tag + output-packet columns.
                hash_col = constants.INPUT_PACKET_HASH_COL
                hash_values = cast(list[str], existing.column(hash_col).to_pylist())
                computed_hashes = set(hash_values)
                data_table = existing.drop([hash_col])
                existing_stream = ArrowTableStream(data_table, tag_columns=tag_keys)
                for i, (tag, packet) in enumerate(existing_stream.iter_packets()):
                    self._cached_output_packets[i] = (tag, packet)
                    yield tag, packet

            # --- Phase 2: process only missing input packets ---
            next_idx = len(self._cached_output_packets)
            for tag, packet in input_iter:
                input_hash = packet.content_hash().to_string()
                if input_hash in computed_hashes:
                    continue
                tag, output_packet = self.process_packet(tag, packet)
                self._cached_output_packets[next_idx] = (tag, output_packet)
                next_idx += 1
                if output_packet is not None:
                    yield tag, output_packet

            self._cached_input_iterator = None
        else:
            # Yield from snapshot of complete cache
            for i in range(len(self._cached_output_packets)):
                tag, packet = self._cached_output_packets[i]
                if packet is not None:
                    yield tag, packet

    def run(self) -> None:
        """Eagerly process all input packets, filling the pipeline and result databases."""
        for _ in self.iter_packets():
            pass

    # ------------------------------------------------------------------
    # Async channel execution (two-phase)
    # ------------------------------------------------------------------

    async def async_execute(
        self,
        inputs: Sequence[ReadableChannel[tuple[TagProtocol, PacketProtocol]]],
        output: WritableChannel[tuple[TagProtocol, PacketProtocol]],
    ) -> None:
        """Two-phase async execution: replay cached, then compute missing."""
        try:
            # Phase 1: emit existing results from DB
            existing = self.get_all_records(columns={"meta": True})
            computed_hashes: set[str] = set()
            if existing is not None and existing.num_rows > 0:
                tag_keys = self._input_stream.keys()[0]
                hash_col = constants.INPUT_PACKET_HASH_COL
                computed_hashes = set(
                    cast(list[str], existing.column(hash_col).to_pylist())
                )
                data_table = existing.drop([hash_col])
                existing_stream = ArrowTableStream(data_table, tag_columns=tag_keys)
                for tag, packet in existing_stream.iter_packets():
                    await output.send((tag, packet))

            # Phase 2: process packets not already in the DB
            async for tag, packet in inputs[0]:
                input_hash = packet.content_hash().to_string()
                if input_hash in computed_hashes:
                    continue
                tag, output_packet = await self.async_process_packet(tag, packet)
                if output_packet is not None:
                    await output.send((tag, output_packet))
        finally:
            await output.close()

    def as_source(self):
        """Return a DerivedSource backed by the DB records of this node."""
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
