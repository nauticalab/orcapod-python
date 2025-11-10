import logging
from collections.abc import Callable, Collection, Iterator
from typing import TYPE_CHECKING, Any, Protocol, cast

from orcapod import contexts
from orcapod.core.base import OrcapodBase
from orcapod.core.operators import Join
from orcapod.core.packet_function import PythonPacketFunction
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
from orcapod.types import PythonSchema
from orcapod.utils import arrow_utils, schema_utils
from orcapod.utils.lazy_module import LazyModule

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")


class FunctionPod(OrcapodBase):
    def __init__(
        self,
        packet_function: PacketFunction,
        tracker_manager: TrackerManager | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.tracker_manager = tracker_manager or DEFAULT_TRACKER_MANAGER
        self.packet_function = packet_function
        self._output_schema_hash = self.data_context.object_hasher.hash_object(
            self.packet_function.output_packet_schema
        ).to_string()

    def identity_structure(self) -> Any:
        return self.packet_function

    @property
    def uri(self) -> tuple[str, ...]:
        return (
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
        if len(streams) != 1:
            raise ValueError(
                f"{self.__class__.__name__} expects exactly one input stream, got {len(streams)}"
            )
        input_stream = streams[0]
        _, incoming_packet_types = input_stream.output_schema()
        expected_packet_schema = self.packet_function.input_packet_schema
        if not schema_utils.check_typespec_compatibility(
            incoming_packet_types, expected_packet_schema
        ):
            # TODO: use custom exception type for better error handling
            raise ValueError(
                f"Incoming packet data type {incoming_packet_types} from {input_stream} is not compatible with expected input typespec {expected_packet_schema}"
            )

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

        # handle multiple input streams
        if len(streams) == 0:
            raise ValueError("At least one input stream is required")
        elif len(streams) > 1:
            multi_stream_handler = self.multi_stream_handler()
            joined_stream = multi_stream_handler.process(*streams)
            streams = (joined_stream,)
        input_stream = streams[0]

        # perform input stream validation
        self.validate_inputs(*streams)
        self.tracker_manager.record_packet_function_invocation(
            self.packet_function, input_stream, label=label
        )
        output_stream = FunctionPodStream(
            function_pod=self,
            input_stream=input_stream,
        )
        return output_stream

    def __call__(self, *streams: Stream, **kwargs) -> "FunctionPodStream":
        """
        Convenience method to invoke the pod process on a collection of streams,
        """
        logger.debug(f"Invoking pod {self} on streams through __call__: {streams}")
        # perform input stream validation
        return self.process(*streams, **kwargs)

    def argument_symmetry(self, streams: Collection[Stream]) -> ArgumentGroup:
        return self.multi_stream_handler().argument_symmetry(streams)

    def output_schema(
        self,
        *streams: Stream,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[PythonSchema, PythonSchema]:
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

    def identity_structure(self):
        return (
            self._function_pod,
            self._input_stream,
        )

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
    ) -> tuple[PythonSchema, PythonSchema]:
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
                    output_packet = self._function_pod.packet_function.call(packet)
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
            all_packets_as_tables: pa.Table = pa.Table.from_pylist(
                struct_packets, schema=packet_schema
            )

            self._cached_output_table = arrow_utils.hstack_tables(
                all_tags_as_tables, all_packets_as_tables
            )
        assert self._cached_output_table is not None, (
            "_cached_output_table should not be None here."
        )

        return self._cached_output_table

        # drop_columns = []
        # if not include_system_tags:
        #     # TODO: get system tags more effiicently
        #     drop_columns.extend(
        #         [
        #             c
        #             for c in self._cached_output_table.column_names
        #             if c.startswith(constants.SYSTEM_TAG_PREFIX)
        #         ]
        #     )
        # if not include_source:
        #     drop_columns.extend(f"{constants.SOURCE_PREFIX}{c}" for c in self.keys()[1])
        # if not include_data_context:
        #     drop_columns.append(constants.CONTEXT_KEY)

        # output_table = self._cached_output_table.drop(drop_columns)

        # # lazily prepare content hash column if requested
        # if include_content_hash:
        #     if self._cached_content_hash_column is None:
        #         content_hashes = []
        #         # TODO: verify that order will be preserved
        #         for tag, packet in self.iter_packets():
        #             content_hashes.append(packet.content_hash().to_string())
        #         self._cached_content_hash_column = pa.array(
        #             content_hashes, type=pa.large_string()
        #         )
        #     assert self._cached_content_hash_column is not None, (
        #         "_cached_content_hash_column should not be None here."
        #     )
        #     hash_column_name = (
        #         "_content_hash"
        #         if include_content_hash is True
        #         else include_content_hash
        #     )
        #     output_table = output_table.append_column(
        #         hash_column_name, self._cached_content_hash_column
        #     )

        # if sort_by_tags:
        #     # TODO: reimplement using polars natively
        #     output_table = (
        #         pl.DataFrame(output_table)
        #         .sort(by=self.keys()[0], descending=False)
        #         .to_arrow()
        #     )
        #     # output_table = output_table.sort_by(
        #     #     [(column, "ascending") for column in self.keys()[0]]
        #     # )
        # return output_table


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
    ) -> tuple[PythonSchema, PythonSchema]:
        return self._function_pod.output_schema(
            *streams, columns=columns, all_info=all_info
        )

    # TODO: reconsider whether to return FunctionPodStream here in the signature
    def process(self, *streams: Stream, label: str | None = None) -> FunctionPodStream:
        return self._function_pod.process(*streams, label=label)


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

#     def get_all_cached_outputs(
#         self, include_system_columns: bool = False
#     ) -> "pa.Table | None":
#         """
#         Get all records from the result store for this pod.
#         If include_system_columns is True, include system columns in the result.
#         """
#         record_id_column = (
#             constants.PACKET_RECORD_ID if include_system_columns else None
#         )
#         result_table = self.result_database.get_all_records(
#             self.record_path, record_id_column=record_id_column
#         )
#         if result_table is None or result_table.num_rows == 0:
#             return None

#         if not include_system_columns:
#             # remove input packet hash and tiered pod ID columns
#             pod_id_columns = [
#                 f"{constants.POD_ID_PREFIX}{k}" for k in self.tiered_pod_id.keys()
#             ]
#             result_table = result_table.drop_columns(pod_id_columns)
#             result_table = result_table.drop_columns(constants.INPUT_PACKET_HASH)

#         return result_table
