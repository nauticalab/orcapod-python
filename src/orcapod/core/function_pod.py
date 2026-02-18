from __future__ import annotations

import logging
from collections.abc import Callable, Collection, Iterator
from typing import TYPE_CHECKING, Any, Protocol, cast

from orcapod import contexts
from orcapod.core.base import TraceableBase
from orcapod.core.operators import Join
from orcapod.core.packet_function import CachedPacketFunction, PythonPacketFunction
from orcapod.core.streams.base import StreamBase
from orcapod.core.tracker import DEFAULT_TRACKER_MANAGER
from orcapod.protocols.core_protocols import (
    ArgumentGroup,
    ColumnConfig,
    Packet,
    PacketFunction,
    Pod,
    Stream,
    Tag,
    TrackerManager,
)
from orcapod.protocols.database_protocols import ArrowDatabase
from orcapod.system_constants import constants
from orcapod.config import Config
from orcapod.types import Schema
from orcapod.utils import arrow_utils, schema_utils
from orcapod.utils.lazy_module import LazyModule

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    import polars as pl
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")
    pl = LazyModule("polars")


class FunctionPod(TraceableBase):
    def __init__(
        self,
        packet_function: PacketFunction,
        tracker_manager: TrackerManager | None = None,
        label: str | None = None,
        data_context: str | contexts.DataContext | None = None,
        orcapod_config: Config | None = None,
    ) -> None:
        super().__init__(
            label=label,
            data_context=data_context,
            orcapod_config=orcapod_config,
        )
        self.tracker_manager = tracker_manager or DEFAULT_TRACKER_MANAGER
        self._packet_function = packet_function
        self._output_schema_hash = self.data_context.object_hasher.hash_object(
            self.packet_function.output_packet_schema
        ).to_string()

    @property
    def packet_function(self) -> PacketFunction:
        return self._packet_function

    def identity_structure(self) -> Any:
        return self.packet_function.identity_structure()

    @property
    def uri(self) -> tuple[str, ...]:
        return (
            self.packet_function.canonical_function_name,
            self.packet_function.packet_function_type_id,
            f"v{self.packet_function.major_version}",
            self._output_schema_hash,
        )

    def multi_stream_handler(self) -> Pod:
        return Join()

    def validate_inputs(self, *streams: Stream) -> None:
        """
        Validate input streams, raising exceptions if invalid.

        Should check:
        - Number of input streams
        - Stream types and schemas
        - Kernel-specific requirements
        - Business logic constraints

        Args:
            *streams: Input streams to validate

        Raises:
            PodInputValidationError: If inputs are invalid
        """
        input_stream = self.handle_input_streams(*streams)
        self._validate_input(input_stream)

    def _validate_input(self, input_stream: Stream) -> None:
        _, incoming_packet_types = input_stream.output_schema()
        expected_packet_schema = self.packet_function.input_packet_schema
        if not schema_utils.check_typespec_compatibility(
            incoming_packet_types, expected_packet_schema
        ):
            # TODO: use custom exception type for better error handling
            raise ValueError(
                f"Incoming packet data type {incoming_packet_types} from {input_stream} is not compatible with expected input typespec {expected_packet_schema}"
            )

    def process_packet(self, tag: Tag, packet: Packet) -> tuple[Tag, Packet | None]:
        """
        Process a single packet using the pod's packet function.

        Args:
            tag: The tag associated with the packet
            packet: The input packet to process

        Returns:
            Packet | None: The processed output packet, or None if filtered out
        """
        return tag, self.packet_function.call(packet)

    def handle_input_streams(self, *streams: Stream) -> Stream:
        """
        Handle multiple input streams by joining them if necessary.

        Args:
            *streams: Input streams to handle
        """
        # handle multiple input streams
        if len(streams) == 0:
            raise ValueError("At least one input stream is required")
        elif len(streams) > 1:
            multi_stream_handler = self.multi_stream_handler()
            joined_stream = multi_stream_handler.process(*streams)
            return joined_stream
        return streams[0]

    def process(
        self, *streams: Stream, label: str | None = None
    ) -> "FunctionPodStream":
        """
        Invoke the packet processor on the input stream.
        If multiple streams are passed in, all streams are joined before processing.

        Args:
            *streams: Input streams to process

        Returns:
            cp.Stream: The resulting output stream
        """
        logger.debug(f"Invoking kernel {self} on streams: {streams}")

        input_stream = self.handle_input_streams(*streams)

        # perform input stream validation
        self._validate_input(input_stream)
        self.tracker_manager.record_packet_function_invocation(
            self.packet_function, input_stream, label=label
        )
        output_stream = FunctionPodStream(
            function_pod=self,
            input_stream=input_stream,
        )
        return output_stream

    def __call__(
        self, *streams: Stream, label: str | None = None
    ) -> "FunctionPodStream":
        """
        Convenience method to invoke the pod process on a collection of streams,
        """
        logger.debug(f"Invoking pod {self} on streams through __call__: {streams}")
        # perform input stream validation
        return self.process(*streams, label=label)

    def argument_symmetry(self, streams: Collection[Stream]) -> ArgumentGroup:
        return self.multi_stream_handler().argument_symmetry(streams)

    def output_schema(
        self,
        *streams: Stream,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        tag_schema = self.multi_stream_handler().output_schema(
            *streams, columns=columns, all_info=all_info
        )[0]
        # The output schema of the FunctionPod is determined by the packet function
        # TODO: handle and extend to include additional columns
        return tag_schema, self.packet_function.output_packet_schema


class FunctionPodStream(StreamBase):
    """
    Recomputable stream wrapping a packet function.
    """

    def __init__(
        self, function_pod: FunctionPod, input_stream: Stream, **kwargs
    ) -> None:
        self._function_pod = function_pod
        self._input_stream = input_stream
        super().__init__(**kwargs)

        # capture the iterator over the input stream
        self._cached_input_iterator = input_stream.iter_packets()
        self._update_modified_time()  # update the modified time to AFTER we obtain the iterator
        # note that the invocation of iter_packets on upstream likely triggeres the modified time
        # to be updated on the usptream. Hence you want to set this stream's modified time after that.

        # Packet-level caching (for the output packets)
        self._cached_output_packets: dict[int, tuple[Tag, Packet | None]] = {}
        self._cached_output_table: pa.Table | None = None
        self._cached_content_hash_column: pa.Array | None = None

    @property
    def source(self) -> Pod:
        return self._function_pod

    @property
    def upstreams(self) -> tuple[Stream, ...]:
        return (self._input_stream,)

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
        packet_schema = self._function_pod.packet_function.output_packet_schema
        return (tag_schema, packet_schema)

    def __iter__(self) -> Iterator[tuple[Tag, Packet]]:
        return self.iter_packets()

    def iter_packets(self) -> Iterator[tuple[Tag, Packet]]:
        if self._cached_input_iterator is not None:
            for i, (tag, packet) in enumerate(self._cached_input_iterator):
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
                        # Update shared cache for future iterators (optimization)
                        yield tag, output_packet

            # Mark completion by releasing the iterator
            self._cached_input_iterator = None
        else:
            # Yield from snapshot of complete cache
            for i in range(len(self._cached_output_packets)):
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
            # drop context key column from tags table
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

        output_table = self._cached_output_table.drop(drop_columns)

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
    def pod(self) -> FunctionPod:
        """
        Returns associated function pod
        """
        ...


def function_pod(
    output_keys: str | Collection[str] | None = None,
    function_name: str | None = None,
    version: str = "v0.0",
    label: str | None = None,
    result_database: ArrowDatabase | None = None,
    **kwargs,
) -> Callable[..., CallableWithPod]:
    """
    Decorator that attaches FunctionPod as pod attribute.

    Args:
        output_keys: Keys for the function output(s)
        function_name: Name of the function pod; if None, defaults to the function name
        **kwargs: Additional keyword arguments to pass to the FunctionPod constructor. Please refer to the FunctionPod documentation for details.

    Returns:
        CallableWithPod: Decorated function with `pod` attribute holding the FunctionPod instance
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


class WrappedFunctionPod(FunctionPod):
    """
    A wrapper for a function pod, allowing for additional functionality or modifications without changing the original pod.
    This class is meant to serve as a base class for other pods that need to wrap existing pods.
    Note that only the call logic is pass through to the wrapped pod, but the forward logic is not.
    """

    def __init__(
        self,
        function_pod: FunctionPod,
        data_context: str | contexts.DataContext | None = None,
        **kwargs,
    ) -> None:
        # if data_context is not explicitly given, use that of the contained pod
        if data_context is None:
            data_context = function_pod.data_context_key
        super().__init__(
            data_context=data_context,
            **kwargs,
        )
        self._function_pod = function_pod

    def computed_label(self) -> str | None:
        return self._function_pod.label

    @property
    def uri(self) -> tuple[str, ...]:
        return self._function_pod.uri

    def validate_inputs(self, *streams: Stream) -> None:
        self._function_pod.validate_inputs(*streams)

    def argument_symmetry(self, streams: Collection[Stream]) -> ArgumentGroup:
        return self._function_pod.argument_symmetry(streams)

    def output_schema(
        self,
        *streams: Stream,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        return self._function_pod.output_schema(
            *streams, columns=columns, all_info=all_info
        )

    # TODO: reconsider whether to return FunctionPodStream here in the signature
    def process(self, *streams: Stream, label: str | None = None) -> FunctionPodStream:
        return self._function_pod.process(*streams, label=label)


class FunctionPodNode(TraceableBase):
    """
    A pod that caches the results of the wrapped packet function.
    This is useful for packet functions that are expensive to compute and can benefit from caching.
    """

    def __init__(
        self,
        packet_function: PacketFunction,
        input_stream: Stream,
        pipeline_database: ArrowDatabase,
        result_database: ArrowDatabase | None = None,
        pipeline_path_prefix: tuple[str, ...] = (),
        tracker_manager: TrackerManager | None = None,
        label: str | None = None,
        data_context: str | contexts.DataContext | None = None,
        orcapod_config: Config | None = None,
    ):
        if tracker_manager is None:
            tracker_manager = DEFAULT_TRACKER_MANAGER
        self.tracker_manager = tracker_manager
        result_path_prefix = ()
        if result_database is None:
            result_database = pipeline_database
            # set result path to be within the pipeline path with "_result" appended
            result_path_prefix = pipeline_path_prefix + ("_result",)

        self._cached_packet_function = CachedPacketFunction(
            packet_function,
            result_database=result_database,
            record_path_prefix=result_path_prefix,
        )

        # initialize the base FunctionPod with the cached packet function
        super().__init__(
            label=label,
            data_context=data_context,
            orcapod_config=orcapod_config,
        )

        # validate the input stream
        _, incoming_packet_types = input_stream.output_schema()
        expected_packet_schema = packet_function.input_packet_schema
        if not schema_utils.check_typespec_compatibility(
            incoming_packet_types, expected_packet_schema
        ):
            # TODO: use custom exception type for better error handling
            raise ValueError(
                f"Incoming packet data type {incoming_packet_types} from {input_stream} is not compatible with expected input typespec {expected_packet_schema}"
            )

        self._input_stream = input_stream

        self._pipeline_database = pipeline_database
        self._pipeline_path_prefix = pipeline_path_prefix

        # take the pipeline node hash and schema hashes
        self._pipeline_node_hash = self.content_hash().to_string()

        self._output_schema_hash = self.data_context.object_hasher.hash_object(
            self._cached_packet_function.output_packet_schema
        ).to_string()

        # compute tag schema hash, inclusive of system tags
        tag_schema, _ = self.output_schema(columns={"system_tags": True})
        self._tag_schema_hash = self.data_context.object_hasher.hash_object(
            tag_schema
        ).to_string()

    def identity_structure(self) -> Any:
        # Identity of function pod node is the identity of the
        # (cached) packet function + input stream
        return (self._cached_packet_function, (self._input_stream,))

    @property
    def pipeline_path(self) -> tuple[str, ...]:
        return self._pipeline_path_prefix + self.uri

    @property
    def uri(self) -> tuple[str, ...]:
        # TODO: revisit organization of the URI components
        return self._cached_packet_function.uri + (
            f"node:{self._pipeline_node_hash}",
            f"tag:{self._tag_schema_hash}",
        )

    def validate_inputs(self, *streams: Stream) -> None:
        if len(streams) > 0:
            raise ValueError(
                "FunctionPodNode.validate_inputs does not accept external streams; input streams are fixed at initialization."
            )

    def process_packet(
        self,
        tag: Tag,
        packet: Packet,
        skip_cache_lookup: bool = False,
        skip_cache_insert: bool = False,
    ) -> tuple[Tag, Packet | None]:
        """
        Process a single packet using the pod's packet function.

        Args:
            tag: The tag associated with the packet
            packet: The input packet to process

        Returns:
            Packet | None: The processed output packet, or None if filtered out
        """
        output_packet = self._cached_packet_function.call(
            packet,
            skip_cache_lookup=skip_cache_lookup,
            skip_cache_insert=skip_cache_insert,
        )

        if output_packet is not None:
            # check if the packet was computed or retrieved from cache
            result_computed = bool(
                output_packet.get_meta_value(
                    self._cached_packet_function.RESULT_COMPUTED_FLAG, False
                )
            )
            self.add_pipeline_record(
                tag,
                packet,
                packet_record_id=output_packet.datagram_id,
                computed=result_computed,
            )

        return tag, output_packet

    def process(
        self, *streams: Stream, label: str | None = None
    ) -> "FunctionPodNodeStream":
        """
        Invoke the packet processor on the input stream.
        If multiple streams are passed in, all streams are joined before processing.

        Args:
            *streams: Input streams to process

        Returns:
            cp.Stream: The resulting output stream
        """
        logger.debug(f"Invoking kernel {self} on streams: {streams}")

        # perform input stream validation
        self.validate_inputs(*streams)
        # TODO: add logic to handle/modify input stream based on streams passed in
        # Example includes appling semi_join on the input stream based on the streams passed in
        self.tracker_manager.record_packet_function_invocation(
            self._cached_packet_function, self._input_stream, label=label
        )
        output_stream = FunctionPodNodeStream(
            fp_node=self,
            input_stream=self._input_stream,
        )
        return output_stream

    def __call__(
        self, *streams: Stream, label: str | None = None
    ) -> "FunctionPodNodeStream":
        """
        Convenience method to invoke the pod process on a collection of streams,
        """
        logger.debug(f"Invoking pod {self} on streams through __call__: {streams}")
        # perform input stream validation
        return self.process(*streams, label=label)

    def argument_symmetry(self, streams: Collection[Stream]) -> ArgumentGroup:
        if len(streams) > 0:
            raise ValueError(
                "FunctionPodNode.argument_symmetry does not accept external streams; input streams are fixed at initialization."
            )
        return ()

    def output_schema(
        self,
        *streams: Stream,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        # TODO: decide on how to handle extra inputs if provided

        tag_schema = self._input_stream.output_schema(
            *streams, columns=columns, all_info=all_info
        )[0]
        # The output schema of the FunctionPod is determined by the packet function
        # TODO: handle and extend to include additional columns
        return tag_schema, self._cached_packet_function.output_packet_schema

    def add_pipeline_record(
        self,
        tag: Tag,
        input_packet: Packet,
        packet_record_id: str,
        computed: bool,
        skip_cache_lookup: bool = False,
    ) -> None:
        # combine dp.Tag with packet content hash to compute entry hash
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


class FunctionPodNodeStream(StreamBase):
    """
    Recomputable stream wrapping a packet function.
    """

    def __init__(
        self, fp_node: FunctionPodNode, input_stream: Stream, **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self._fp_node = fp_node
        self._input_stream = input_stream

        # capture the iterator over the input stream
        self._cached_input_iterator = input_stream.iter_packets()
        self._update_modified_time()  # update the modified time to AFTER we obtain the iterator
        # note that the invocation of iter_packets on upstream likely triggeres the modified time
        # to be updated on the usptream. Hence you want to set this stream's modified time after that.

        # Packet-level caching (for the output packets)
        self._cached_output_packets: dict[int, tuple[Tag, Packet | None]] = {}
        self._cached_output_table: pa.Table | None = None
        self._cached_content_hash_column: pa.Array | None = None

    def refresh_cache(self) -> None:
        upstream_last_modified = self._input_stream.last_modified
        if (
            upstream_last_modified is None
            or self.last_modified is None
            or upstream_last_modified > self.last_modified
        ):
            # input stream has been modified since last processing; refresh caches
            # re-cache the iterator and clear out output packet cache
            self._cached_input_iterator = self._input_stream.iter_packets()
            self._cached_output_packets.clear()
            self._cached_output_table = None
            self._cached_content_hash_column = None
            self._update_modified_time()

    @property
    def source(self) -> FunctionPodNode:
        return self._fp_node

    @property
    def upstreams(self) -> tuple[Stream, ...]:
        return (self._input_stream,)

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
        packet_schema = self._fp_node._cached_packet_function.output_packet_schema
        return (tag_schema, packet_schema)

    def __iter__(self) -> Iterator[tuple[Tag, Packet]]:
        return self.iter_packets()

    def iter_packets(self) -> Iterator[tuple[Tag, Packet]]:
        if self._cached_input_iterator is not None:
            for i, (tag, packet) in enumerate(self._cached_input_iterator):
                if i in self._cached_output_packets:
                    # Use cached result
                    tag, packet = self._cached_output_packets[i]
                    if packet is not None:
                        yield tag, packet
                else:
                    # Process packet
                    tag, output_packet = self._fp_node.process_packet(tag, packet)
                    self._cached_output_packets[i] = (tag, output_packet)
                    if output_packet is not None:
                        # Update shared cache for future iterators (optimization)
                        yield tag, output_packet

            # Mark completion by releasing the iterator
            self._cached_input_iterator = None
        else:
            # Yield from snapshot of complete cache
            for i in range(len(self._cached_output_packets)):
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
            # drop context key column from tags table
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

        output_table = self._cached_output_table.drop(drop_columns)

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


# class CachedFunctionPod(WrappedFunctionPod):
#     """
#     A pod that caches the results of the wrapped pod.
#     This is useful for pods that are expensive to compute and can benefit from caching.
#     """

#     # name of the column in the tag store that contains the packet hash
#     DATA_RETRIEVED_FLAG = f"{constants.META_PREFIX}data_retrieved"

#     def __init__(
#         self,
#         pod: cp.Pod,
#         result_database: ArrowDatabase,
#         record_path_prefix: tuple[str, ...] = (),
#         match_tier: str | None = None,
#         retrieval_mode: Literal["latest", "most_specific"] = "latest",
#         **kwargs,
#     ):
#         super().__init__(pod, **kwargs)
#         self.record_path_prefix = record_path_prefix
#         self.result_database = result_database
#         self.match_tier = match_tier
#         self.retrieval_mode = retrieval_mode
#         self.mode: Literal["production", "development"] = "production"

#     def set_mode(self, mode: str) -> None:
#         if mode not in ("production", "development"):
#             raise ValueError(f"Invalid mode: {mode}")
#         self.mode = mode

#     @property
#     def version(self) -> str:
#         return self.pod.version

#     @property
#     def record_path(self) -> tuple[str, ...]:
#         """
#         Return the path to the record in the result store.
#         This is used to store the results of the pod.
#         """
#         return self.record_path_prefix + self.reference

#     def call(
#         self,
#         tag: cp.Tag,
#         packet: cp.Packet,
#         record_id: str | None = None,
#         execution_engine: orcapod.protocols.core_protocols.execution_engine.ExecutionEngine
#         | None = None,
#         skip_cache_lookup: bool = False,
#         skip_cache_insert: bool = False,
#     ) -> tuple[cp.Tag, cp.Packet | None]:
#         # TODO: consider logic for overwriting existing records
#         execution_engine_hash = execution_engine.name if execution_engine else "default"
#         if record_id is None:
#             record_id = self.get_record_id(
#                 packet, execution_engine_hash=execution_engine_hash
#             )
#         output_packet = None
#         if not skip_cache_lookup and self.mode == "production":
#             print("Checking for cache...")
#             output_packet = self.get_cached_output_for_packet(packet)
#             if output_packet is not None:
#                 print(f"Cache hit for {packet}!")
#         if output_packet is None:
#             tag, output_packet = super().call(
#                 tag, packet, record_id=record_id, execution_engine=execution_engine
#             )
#             if (
#                 output_packet is not None
#                 and not skip_cache_insert
#                 and self.mode == "production"
#             ):
#                 self.record_packet(packet, output_packet, record_id=record_id)

#         return tag, output_packet

#     async def async_call(
#         self,
#         tag: cp.Tag,
#         packet: cp.Packet,
#         record_id: str | None = None,
#         execution_engine: orcapod.protocols.core_protocols.execution_engine.ExecutionEngine
#         | None = None,
#         skip_cache_lookup: bool = False,
#         skip_cache_insert: bool = False,
#     ) -> tuple[cp.Tag, cp.Packet | None]:
#         # TODO: consider logic for overwriting existing records
#         execution_engine_hash = execution_engine.name if execution_engine else "default"

#         if record_id is None:
#             record_id = self.get_record_id(
#                 packet, execution_engine_hash=execution_engine_hash
#             )
#         output_packet = None
#         if not skip_cache_lookup:
#             output_packet = self.get_cached_output_for_packet(packet)
#         if output_packet is None:
#             tag, output_packet = await super().async_call(
#                 tag, packet, record_id=record_id, execution_engine=execution_engine
#             )
#             if output_packet is not None and not skip_cache_insert:
#                 self.record_packet(
#                     packet,
#                     output_packet,
#                     record_id=record_id,
#                     execution_engine=execution_engine,
#                 )

#         return tag, output_packet

#     def forward(self, *streams: cp.Stream) -> cp.Stream:
#         assert len(streams) == 1, "PodBase.forward expects exactly one input stream"
#         return CachedPodStream(pod=self, input_stream=streams[0])

#     def record_packet(
#         self,
#         input_packet: cp.Packet,
#         output_packet: cp.Packet,
#         record_id: str | None = None,
#         execution_engine: orcapod.protocols.core_protocols.execution_engine.ExecutionEngine
#         | None = None,
#         skip_duplicates: bool = False,
#     ) -> cp.Packet:
#         """
#         Record the output packet against the input packet in the result store.
#         """
#         data_table = output_packet.as_table(include_context=True, include_source=True)

#         for i, (k, v) in enumerate(self.tiered_pod_id.items()):
#             # add the tiered pod ID to the data table
#             data_table = data_table.add_column(
#                 i,
#                 f"{constants.POD_ID_PREFIX}{k}",
#                 pa.array([v], type=pa.large_string()),
#             )

#         # add the input packet hash as a column
#         data_table = data_table.add_column(
#             0,
#             constants.INPUT_PACKET_HASH,
#             pa.array([str(input_packet.content_hash())], type=pa.large_string()),
#         )
#         # add execution engine information
#         execution_engine_hash = execution_engine.name if execution_engine else "default"
#         data_table = data_table.append_column(
#             constants.EXECUTION_ENGINE,
#             pa.array([execution_engine_hash], type=pa.large_string()),
#         )

#         # add computation timestamp
#         timestamp = datetime.now(timezone.utc)
#         data_table = data_table.append_column(
#             constants.POD_TIMESTAMP,
#             pa.array([timestamp], type=pa.timestamp("us", tz="UTC")),
#         )

#         if record_id is None:
#             record_id = self.get_record_id(
#                 input_packet, execution_engine_hash=execution_engine_hash
#             )

#         self.result_database.add_record(
#             self.record_path,
#             record_id,
#             data_table,
#             skip_duplicates=skip_duplicates,
#         )
#         # if result_flag is None:
#         #     # TODO: do more specific error handling
#         #     raise ValueError(
#         #         f"Failed to record packet {input_packet} in result store {self.result_store}"
#         #     )
#         # # TODO: make store return retrieved table
#         return output_packet

#     def get_cached_output_for_packet(self, input_packet: cp.Packet) -> cp.Packet | None:
#         """
#         Retrieve the output packet from the result store based on the input packet.
#         If more than one output packet is found, conflict resolution strategy
#         will be applied.
#         If the output packet is not found, return None.
#         """
#         # result_table = self.result_store.get_record_by_id(
#         #     self.record_path,
#         #     self.get_entry_hash(input_packet),
#         # )

#         # get all records with matching the input packet hash
#         # TODO: add match based on match_tier if specified
#         constraints = {constants.INPUT_PACKET_HASH: str(input_packet.content_hash())}
#         if self.match_tier is not None:
#             constraints[f"{constants.POD_ID_PREFIX}{self.match_tier}"] = (
#                 self.pod.tiered_pod_id[self.match_tier]
#             )

#         result_table = self.result_database.get_records_with_column_value(
#             self.record_path,
#             constraints,
#         )
#         if result_table is None or result_table.num_rows == 0:
#             return None

#         if result_table.num_rows > 1:
#             logger.info(
#                 f"Performing conflict resolution for multiple records for {input_packet.content_hash().display_name()}"
#             )
#             if self.retrieval_mode == "latest":
#                 result_table = result_table.sort_by(
#                     self.DATA_RETRIEVED_FLAG, ascending=False
#                 ).take([0])
#             elif self.retrieval_mode == "most_specific":
#                 # match by the most specific pod ID
#                 # trying next level if not found
#                 for k, v in reversed(self.tiered_pod_id.items()):
#                     search_result = result_table.filter(
#                         pc.field(f"{constants.POD_ID_PREFIX}{k}") == v
#                     )
#                     if search_result.num_rows > 0:
#                         result_table = search_result.take([0])
#                         break
#                 if result_table.num_rows > 1:
#                     logger.warning(
#                         f"No matching record found for {input_packet.content_hash().display_name()} with tiered pod ID {self.tiered_pod_id}"
#                     )
#                     result_table = result_table.sort_by(
#                         self.DATA_RETRIEVED_FLAG, ascending=False
#                     ).take([0])

#             else:
#                 raise ValueError(
#                     f"Unknown retrieval mode: {self.retrieval_mode}. Supported modes are 'latest' and 'most_specific'."
#                 )

#         pod_id_columns = [
#             f"{constants.POD_ID_PREFIX}{k}" for k in self.tiered_pod_id.keys()
#         ]
#         result_table = result_table.drop_columns(pod_id_columns)
#         result_table = result_table.drop_columns(constants.INPUT_PACKET_HASH)

#         # note that data context will be loaded from the result store
#         return ArrowPacket(
#             result_table,
#             meta_info={self.DATA_RETRIEVED_FLAG: str(datetime.now(timezone.utc))},
#         )
