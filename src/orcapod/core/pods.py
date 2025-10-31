import hashlib
import logging
from abc import abstractmethod
from collections.abc import Callable, Collection, Iterable, Sequence
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Literal, Protocol, cast

from orcapod import contexts
from orcapod.core.datagrams import (
    ArrowPacket,
    DictPacket,
)
from orcapod.utils.git_utils import get_git_info_for_python_object
from orcapod.core.kernels import KernelStream, TrackedKernelBase
from orcapod.core.operators import Join
from orcapod.core.streams import CachedPodStream, LazyPodResultStream
from orcapod.core.system_constants import constants
from orcapod.hashing.hash_utils import get_function_components, get_function_signature
from orcapod.protocols import core_protocols as cp
from orcapod.protocols import hashing_protocols as hp
from orcapod.protocols.database_protocols import ArrowDatabase
from orcapod.types import DataValue, PythonSchema, PythonSchemaLike
from orcapod.utils import types_utils
from orcapod.utils.lazy_module import LazyModule


# TODO: extract default char count as config
def combine_hashes(
    *hashes: str,
    order: bool = False,
    prefix_hasher_id: bool = False,
    hex_char_count: int | None = 20,
) -> str:
    """Combine hashes into a single hash string."""

    # Sort for deterministic order regardless of input order
    if order:
        prepared_hashes = sorted(hashes)
    else:
        prepared_hashes = list(hashes)
    combined = "".join(prepared_hashes)
    combined_hash = hashlib.sha256(combined.encode()).hexdigest()
    if hex_char_count is not None:
        combined_hash = combined_hash[:hex_char_count]
    if prefix_hasher_id:
        return "sha256@" + combined_hash
    return combined_hash


if TYPE_CHECKING:
    import pyarrow as pa
    import pyarrow.compute as pc
else:
    pa = LazyModule("pyarrow")
    pc = LazyModule("pyarrow.compute")

logger = logging.getLogger(__name__)

error_handling_options = Literal["raise", "ignore", "warn"]


class ActivatablePodBase(TrackedKernelBase):
    """
    FunctionPod is a specialized kernel that encapsulates a function to be executed on data streams.
    It allows for the execution of a function with a specific label and can be tracked by the system.
    """

    @abstractmethod
    def input_packet_types(self) -> PythonSchema:
        """
        Return the input typespec for the pod. This is used to validate the input streams.
        """
        ...

    @abstractmethod
    def output_packet_types(self) -> PythonSchema:
        """
        Return the output typespec for the pod. This is used to validate the output streams.
        """
        ...

    @property
    def version(self) -> str:
        return self._version

    @abstractmethod
    def get_record_id(self, packet: cp.Packet, execution_engine_hash: str) -> str:
        """
        Return the record ID for the input packet. This is used to identify the pod in the system.
        """
        ...

    @property
    @abstractmethod
    def tiered_pod_id(self) -> dict[str, str]:
        """
        Return the tiered pod ID for the pod. This is used to identify the pod in a tiered architecture.
        """
        ...

    def __init__(
        self,
        error_handling: error_handling_options = "raise",
        label: str | None = None,
        version: str = "v0.0",
        **kwargs,
    ) -> None:
        super().__init__(label=label, **kwargs)
        self._active = True
        self.error_handling = error_handling
        self._version = version
        import re

        match = re.match(r"\D.*(\d+)", version)
        major_version = 0
        if match:
            major_version = int(match.group(1))
        else:
            raise ValueError(
                f"Version string {version} does not contain a valid version number"
            )
        self.skip_type_checking = False
        self._major_version = major_version

    @property
    def major_version(self) -> int:
        return self._major_version

    def kernel_output_types(
        self, *streams: cp.Stream, include_system_tags: bool = False
    ) -> tuple[PythonSchema, PythonSchema]:
        """
        Return the input and output typespecs for the pod.
        This is used to validate the input and output streams.
        """
        tag_typespec, _ = streams[0].types(include_system_tags=include_system_tags)
        return tag_typespec, self.output_packet_types()

    def is_active(self) -> bool:
        """
        Check if the pod is active. If not, it will not process any packets.
        """
        return self._active

    def set_active(self, active: bool) -> None:
        """
        Set the active state of the pod. If set to False, the pod will not process any packets.
        """
        self._active = active

    @staticmethod
    def _join_streams(*streams: cp.Stream) -> cp.Stream:
        if not streams:
            raise ValueError("No streams provided for joining")
        # Join the streams using a suitable join strategy
        if len(streams) == 1:
            return streams[0]

        joined_stream = streams[0]
        for next_stream in streams[1:]:
            joined_stream = Join()(joined_stream, next_stream)
        return joined_stream

    def pre_kernel_processing(self, *streams: cp.Stream) -> tuple[cp.Stream, ...]:
        """
        Prepare the incoming streams for execution in the pod. At least one stream must be present.
        If more than one stream is present, the join of the provided streams will be returned.
        """
        # if multiple streams are provided, join them
        # otherwise, return as is
        if len(streams) <= 1:
            return streams

        output_stream = self._join_streams(*streams)
        return (output_stream,)

    def validate_inputs(self, *streams: cp.Stream) -> None:
        if len(streams) != 1:
            raise ValueError(
                f"{self.__class__.__name__} expects exactly one input stream, got {len(streams)}"
            )
        if self.skip_type_checking:
            return
        input_stream = streams[0]
        _, incoming_packet_types = input_stream.types()
        if not types_utils.check_typespec_compatibility(
            incoming_packet_types, self.input_packet_types()
        ):
            # TODO: use custom exception type for better error handling
            raise ValueError(
                f"Incoming packet data type {incoming_packet_types} from {input_stream} is not compatible with expected input typespec {self.input_packet_types()}"
            )

    def prepare_output_stream(
        self, *streams: cp.Stream, label: str | None = None
    ) -> KernelStream:
        return KernelStream(source=self, upstreams=streams, label=label)

    def forward(self, *streams: cp.Stream) -> cp.Stream:
        assert len(streams) == 1, "PodBase.forward expects exactly one input stream"
        return LazyPodResultStream(pod=self, prepared_stream=streams[0])

    @abstractmethod
    def call(
        self,
        tag: cp.Tag,
        packet: cp.Packet,
        record_id: str | None = None,
        execution_engine: cp.ExecutionEngine | None = None,
        execution_engine_opts: dict[str, Any] | None = None,
    ) -> tuple[cp.Tag, cp.Packet | None]: ...

    @abstractmethod
    async def async_call(
        self,
        tag: cp.Tag,
        packet: cp.Packet,
        record_id: str | None = None,
        execution_engine: cp.ExecutionEngine | None = None,
        execution_engine_opts: dict[str, Any] | None = None,
    ) -> tuple[cp.Tag, cp.Packet | None]: ...

    def track_invocation(self, *streams: cp.Stream, label: str | None = None) -> None:
        if not self._skip_tracking and self._tracker_manager is not None:
            self._tracker_manager.record_pod_invocation(self, streams, label=label)


class CallableWithPod(Protocol):
    def __call__(self, *args, **kwargs) -> Any: ...

    @property
    def pod(self) -> "FunctionPod": ...


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

        # Create a simple typed function pod
        pod = FunctionPod(
            function=func,
            output_keys=output_keys,
            function_name=function_name or func.__name__,
            version=version,
            label=label,
            **kwargs,
        )
        setattr(func, "pod", pod)
        return cast(CallableWithPod, func)

    return decorator


class FunctionPod(ActivatablePodBase):
    def __init__(
        self,
        function: cp.PodFunction,
        output_keys: str | Collection[str] | None = None,
        function_name=None,
        version: str = "v0.0",
        input_python_schema: PythonSchemaLike | None = None,
        output_python_schema: PythonSchemaLike | Sequence[type] | None = None,
        label: str | None = None,
        function_info_extractor: hp.FunctionInfoExtractor | None = None,
        **kwargs,
    ) -> None:
        self.function = function

        if output_keys is None:
            output_keys = []
        if isinstance(output_keys, str):
            output_keys = [output_keys]
        self.output_keys = output_keys
        if function_name is None:
            if hasattr(self.function, "__name__"):
                function_name = getattr(self.function, "__name__")
            else:
                raise ValueError(
                    "function_name must be provided if function has no __name__ attribute"
                )
        self.function_name = function_name
        # extract the first full index (potentially with leading 0) in the version string
        if not isinstance(version, str):
            raise TypeError(f"Version must be a string, got {type(version)}")

        super().__init__(label=label or self.function_name, version=version, **kwargs)

        # extract input and output types from the function signature
        input_packet_types, output_packet_types = (
            types_utils.extract_function_typespecs(
                self.function,
                self.output_keys,
                input_typespec=input_python_schema,
                output_typespec=output_python_schema,
            )
        )

        # get git info for the function
        env_info = get_git_info_for_python_object(self.function)
        if env_info is None:
            git_hash = "unknown"
        else:
            git_hash = env_info.get("git_commit_hash", "unknown")
            if env_info.get("git_repo_status") == "dirty":
                git_hash += "-dirty"
        self._git_hash = git_hash

        self._input_packet_schema = dict(input_packet_types)
        self._output_packet_schema = dict(output_packet_types)
        # TODO: add output packet converter for speed up

        self._function_info_extractor = function_info_extractor
        object_hasher = self.data_context.object_hasher
        # TODO: fix and replace with object_hasher protocol specific methods
        self._function_signature_hash = object_hasher.hash_object(
            get_function_signature(self.function)
        ).to_string()
        self._function_content_hash = object_hasher.hash_object(
            get_function_components(self.function)
        ).to_string()

        self._output_packet_type_hash = object_hasher.hash_object(
            self.output_packet_types()
        ).to_string()

        self._total_pod_id_hash = object_hasher.hash_object(
            self.tiered_pod_id
        ).to_string()

    @property
    def tiered_pod_id(self) -> dict[str, str]:
        return {
            "version": self.version,
            "signature": self._function_signature_hash,
            "content": self._function_content_hash,
            "git_hash": self._git_hash,
        }

    @property
    def reference(self) -> tuple[str, ...]:
        return (
            self.function_name,
            self._output_packet_type_hash,
            "v" + str(self.major_version),
        )

    def get_record_id(
        self,
        packet: cp.Packet,
        execution_engine_hash: str,
    ) -> str:
        return combine_hashes(
            str(packet.content_hash()),
            self._total_pod_id_hash,
            execution_engine_hash,
            prefix_hasher_id=True,
        )

    def input_packet_types(self) -> PythonSchema:
        """
        Return the input typespec for the function pod.
        This is used to validate the input streams.
        """
        return self._input_packet_schema.copy()

    def output_packet_types(self) -> PythonSchema:
        """
        Return the output typespec for the function pod.
        This is used to validate the output streams.
        """
        return self._output_packet_schema.copy()

    def __repr__(self) -> str:
        return f"FunctionPod:{self.function_name}"

    def __str__(self) -> str:
        include_module = self.function.__module__ != "__main__"
        func_sig = get_function_signature(
            self.function,
            name_override=self.function_name,
            include_module=include_module,
        )
        return f"FunctionPod:{func_sig}"

    def call(
        self,
        tag: cp.Tag,
        packet: cp.Packet,
        record_id: str | None = None,
        execution_engine: cp.ExecutionEngine | None = None,
        execution_engine_opts: dict[str, Any] | None = None,
    ) -> tuple[cp.Tag, DictPacket | None]:
        if not self.is_active():
            logger.info(
                f"Pod is not active: skipping computation on input packet {packet}"
            )
            return tag, None

        execution_engine_hash = execution_engine.name if execution_engine else "default"

        # any kernel/pod invocation happening inside the function will NOT be tracked
        if not isinstance(packet, dict):
            input_dict = packet.as_dict(include_source=False)
        else:
            input_dict = packet

        with self._tracker_manager.no_tracking():
            if execution_engine is not None:
                # use the provided execution engine to run the function
                values = execution_engine.submit_sync(
                    self.function,
                    fn_kwargs=input_dict,
                    engine_opts=execution_engine_opts,
                )
            else:
                values = self.function(**input_dict)

        output_data = self.process_function_output(values)

        # TODO: extract out this function
        def combine(*components: tuple[str, ...]) -> str:
            inner_parsed = [":".join(component) for component in components]
            return "::".join(inner_parsed)

        if record_id is None:
            # if record_id is not provided, generate it from the packet
            record_id = self.get_record_id(packet, execution_engine_hash)
        source_info = {
            k: combine(self.reference, (record_id,), (k,)) for k in output_data
        }

        output_packet = DictPacket(
            output_data,
            source_info=source_info,
            python_schema=self.output_packet_types(),
            data_context=self.data_context,
        )
        return tag, output_packet

    async def async_call(
        self,
        tag: cp.Tag,
        packet: cp.Packet,
        record_id: str | None = None,
        execution_engine: cp.ExecutionEngine | None = None,
        execution_engine_opts: dict[str, Any] | None = None,
    ) -> tuple[cp.Tag, cp.Packet | None]:
        """
        Asynchronous call to the function pod. This is a placeholder for future implementation.
        Currently, it behaves like the synchronous call.
        """
        if not self.is_active():
            logger.info(
                f"Pod is not active: skipping computation on input packet {packet}"
            )
            return tag, None

        execution_engine_hash = execution_engine.name if execution_engine else "default"

        # any kernel/pod invocation happening inside the function will NOT be tracked
        # with self._tracker_manager.no_tracking():
        # FIXME: figure out how to properly make context manager work with async/await
        # any kernel/pod invocation happening inside the function will NOT be tracked
        if not isinstance(packet, dict):
            input_dict = packet.as_dict(include_source=False)
        else:
            input_dict = packet
        if execution_engine is not None:
            # use the provided execution engine to run the function
            values = await execution_engine.submit_async(
                self.function, fn_kwargs=input_dict, engine_opts=execution_engine_opts
            )
        else:
            values = self.function(**input_dict)

        output_data = self.process_function_output(values)

        # TODO: extract out this function
        def combine(*components: tuple[str, ...]) -> str:
            inner_parsed = [":".join(component) for component in components]
            return "::".join(inner_parsed)

        if record_id is None:
            # if record_id is not provided, generate it from the packet
            record_id = self.get_record_id(packet, execution_engine_hash)
        source_info = {
            k: combine(self.reference, (record_id,), (k,)) for k in output_data
        }

        output_packet = DictPacket(
            output_data,
            source_info=source_info,
            python_schema=self.output_packet_types(),
            data_context=self.data_context,
        )
        return tag, output_packet

    def process_function_output(self, values: Any) -> dict[str, DataValue]:
        output_values = []
        if len(self.output_keys) == 0:
            output_values = []
        elif len(self.output_keys) == 1:
            output_values = [values]  # type: ignore
        elif isinstance(values, Iterable):
            output_values = list(values)  # type: ignore
        elif len(self.output_keys) > 1:
            raise ValueError(
                "Values returned by function must be a pathlike or a sequence of pathlikes"
            )

        if len(output_values) != len(self.output_keys):
            raise ValueError(
                f"Number of output keys {len(self.output_keys)}:{self.output_keys} does not match number of values returned by function {len(output_values)}"
            )

        return {k: v for k, v in zip(self.output_keys, output_values)}

    def kernel_identity_structure(
        self, streams: Collection[cp.Stream] | None = None
    ) -> Any:
        id_struct = (self.__class__.__name__,) + self.reference
        # if streams are provided, perform pre-processing step, validate, and add the
        # resulting single stream to the identity structure
        if streams is not None and len(streams) != 0:
            id_struct += tuple(streams)

        return id_struct


class WrappedPod(ActivatablePodBase):
    """
    A wrapper for an existing pod, allowing for additional functionality or modifications without changing the original pod.
    This class is meant to serve as a base class for other pods that need to wrap existing pods.
    Note that only the call logic is pass through to the wrapped pod, but the forward logic is not.
    """

    def __init__(
        self,
        pod: cp.Pod,
        label: str | None = None,
        data_context: str | contexts.DataContext | None = None,
        **kwargs,
    ) -> None:
        # if data_context is not explicitly given, use that of the contained pod
        if data_context is None:
            data_context = pod.data_context_key
        super().__init__(
            label=label,
            data_context=data_context,
            **kwargs,
        )
        self.pod = pod

    @property
    def reference(self) -> tuple[str, ...]:
        """
        Return the pod ID, which is the function name of the wrapped pod.
        This is used to identify the pod in the system.
        """
        return self.pod.reference

    def get_record_id(self, packet: cp.Packet, execution_engine_hash: str) -> str:
        return self.pod.get_record_id(packet, execution_engine_hash)

    @property
    def tiered_pod_id(self) -> dict[str, str]:
        """
        Return the tiered pod ID for the wrapped pod. This is used to identify the pod in a tiered architecture.
        """
        return self.pod.tiered_pod_id

    def computed_label(self) -> str | None:
        return self.pod.label

    def input_packet_types(self) -> PythonSchema:
        """
        Return the input typespec for the stored pod.
        This is used to validate the input streams.
        """
        return self.pod.input_packet_types()

    def output_packet_types(self) -> PythonSchema:
        """
        Return the output typespec for the stored pod.
        This is used to validate the output streams.
        """
        return self.pod.output_packet_types()

    def validate_inputs(self, *streams: cp.Stream) -> None:
        self.pod.validate_inputs(*streams)

    def call(
        self,
        tag: cp.Tag,
        packet: cp.Packet,
        record_id: str | None = None,
        execution_engine: cp.ExecutionEngine | None = None,
        execution_engine_opts: dict[str, Any] | None = None,
    ) -> tuple[cp.Tag, cp.Packet | None]:
        return self.pod.call(
            tag,
            packet,
            record_id=record_id,
            execution_engine=execution_engine,
            execution_engine_opts=execution_engine_opts,
        )

    async def async_call(
        self,
        tag: cp.Tag,
        packet: cp.Packet,
        record_id: str | None = None,
        execution_engine: cp.ExecutionEngine | None = None,
        execution_engine_opts: dict[str, Any] | None = None,
    ) -> tuple[cp.Tag, cp.Packet | None]:
        return await self.pod.async_call(
            tag,
            packet,
            record_id=record_id,
            execution_engine=execution_engine,
            execution_engine_opts=execution_engine_opts,
        )

    def kernel_identity_structure(
        self, streams: Collection[cp.Stream] | None = None
    ) -> Any:
        return self.pod.identity_structure(streams)

    def __repr__(self) -> str:
        return f"WrappedPod({self.pod!r})"

    def __str__(self) -> str:
        return f"WrappedPod:{self.pod!s}"


class CachedPod(WrappedPod):
    """
    A pod that caches the results of the wrapped pod.
    This is useful for pods that are expensive to compute and can benefit from caching.
    """

    # name of the column in the tag store that contains the packet hash
    DATA_RETRIEVED_FLAG = f"{constants.META_PREFIX}data_retrieved"

    def __init__(
        self,
        pod: cp.Pod,
        result_database: ArrowDatabase,
        record_path_prefix: tuple[str, ...] = (),
        match_tier: str | None = None,
        retrieval_mode: Literal["latest", "most_specific"] = "latest",
        **kwargs,
    ):
        super().__init__(pod, **kwargs)
        self.record_path_prefix = record_path_prefix
        self.result_database = result_database
        self.match_tier = match_tier
        self.retrieval_mode = retrieval_mode
        self.mode: Literal["production", "development"] = "production"

    def set_mode(self, mode: str) -> None:
        if mode not in ("production", "development"):
            raise ValueError(f"Invalid mode: {mode}")
        self.mode = mode

    @property
    def version(self) -> str:
        return self.pod.version

    @property
    def record_path(self) -> tuple[str, ...]:
        """
        Return the path to the record in the result store.
        This is used to store the results of the pod.
        """
        return self.record_path_prefix + self.reference

    def call(
        self,
        tag: cp.Tag,
        packet: cp.Packet,
        record_id: str | None = None,
        execution_engine: cp.ExecutionEngine | None = None,
        execution_engine_opts: dict[str, Any] | None = None,
        skip_cache_lookup: bool = False,
        skip_cache_insert: bool = False,
    ) -> tuple[cp.Tag, cp.Packet | None]:
        # TODO: consider logic for overwriting existing records
        execution_engine_hash = execution_engine.name if execution_engine else "default"
        if record_id is None:
            record_id = self.get_record_id(
                packet, execution_engine_hash=execution_engine_hash
            )
        output_packet = None
        if not skip_cache_lookup and self.mode == "production":
            print("Checking for cache...")
            output_packet = self.get_cached_output_for_packet(packet)
            if output_packet is not None:
                print(f"Cache hit for {packet}!")
        if output_packet is None:
            tag, output_packet = super().call(
                tag,
                packet,
                record_id=record_id,
                execution_engine=execution_engine,
                execution_engine_opts=execution_engine_opts,
            )
            if (
                output_packet is not None
                and not skip_cache_insert
                and self.mode == "production"
            ):
                self.record_packet(packet, output_packet, record_id=record_id)

        return tag, output_packet

    async def async_call(
        self,
        tag: cp.Tag,
        packet: cp.Packet,
        record_id: str | None = None,
        execution_engine: cp.ExecutionEngine | None = None,
        execution_engine_opts: dict[str, Any] | None = None,
        skip_cache_lookup: bool = False,
        skip_cache_insert: bool = False,
    ) -> tuple[cp.Tag, cp.Packet | None]:
        # TODO: consider logic for overwriting existing records
        execution_engine_hash = execution_engine.name if execution_engine else "default"

        if record_id is None:
            record_id = self.get_record_id(
                packet, execution_engine_hash=execution_engine_hash
            )
        output_packet = None
        if not skip_cache_lookup:
            output_packet = self.get_cached_output_for_packet(packet)
        if output_packet is None:
            tag, output_packet = await super().async_call(
                tag,
                packet,
                record_id=record_id,
                execution_engine=execution_engine,
                execution_engine_opts=execution_engine_opts,
            )
            if output_packet is not None and not skip_cache_insert:
                self.record_packet(
                    packet,
                    output_packet,
                    record_id=record_id,
                    execution_engine=execution_engine,
                    execution_engine_opts=execution_engine_opts,
                )

        return tag, output_packet

    def forward(self, *streams: cp.Stream) -> cp.Stream:
        assert len(streams) == 1, "PodBase.forward expects exactly one input stream"
        return CachedPodStream(pod=self, input_stream=streams[0])

    def record_packet(
        self,
        input_packet: cp.Packet,
        output_packet: cp.Packet,
        record_id: str | None = None,
        execution_engine: cp.ExecutionEngine | None = None,
        execution_engine_opts: dict[str, Any] | None = None,
        skip_duplicates: bool = False,
    ) -> cp.Packet:
        """
        Record the output packet against the input packet in the result store.
        """

        # TODO: consider incorporating execution_engine_opts into the record
        data_table = output_packet.as_table(include_context=True, include_source=True)

        for i, (k, v) in enumerate(self.tiered_pod_id.items()):
            # add the tiered pod ID to the data table
            data_table = data_table.add_column(
                i,
                f"{constants.POD_ID_PREFIX}{k}",
                pa.array([v], type=pa.large_string()),
            )

        # add the input packet hash as a column
        data_table = data_table.add_column(
            0,
            constants.INPUT_PACKET_HASH,
            pa.array([str(input_packet.content_hash())], type=pa.large_string()),
        )
        # add execution engine information
        execution_engine_hash = execution_engine.name if execution_engine else "default"
        data_table = data_table.append_column(
            constants.EXECUTION_ENGINE,
            pa.array([execution_engine_hash], type=pa.large_string()),
        )

        # add computation timestamp
        timestamp = datetime.now(timezone.utc)
        data_table = data_table.append_column(
            constants.POD_TIMESTAMP,
            pa.array([timestamp], type=pa.timestamp("us", tz="UTC")),
        )

        if record_id is None:
            record_id = self.get_record_id(
                input_packet, execution_engine_hash=execution_engine_hash
            )

        self.result_database.add_record(
            self.record_path,
            record_id,
            data_table,
            skip_duplicates=skip_duplicates,
        )
        # if result_flag is None:
        #     # TODO: do more specific error handling
        #     raise ValueError(
        #         f"Failed to record packet {input_packet} in result store {self.result_store}"
        #     )
        # # TODO: make store return retrieved table
        return output_packet

    def get_cached_output_for_packet(self, input_packet: cp.Packet) -> cp.Packet | None:
        """
        Retrieve the output packet from the result store based on the input packet.
        If more than one output packet is found, conflict resolution strategy
        will be applied.
        If the output packet is not found, return None.
        """
        # result_table = self.result_store.get_record_by_id(
        #     self.record_path,
        #     self.get_entry_hash(input_packet),
        # )

        # get all records with matching the input packet hash
        # TODO: add match based on match_tier if specified
        constraints = {constants.INPUT_PACKET_HASH: str(input_packet.content_hash())}
        if self.match_tier is not None:
            constraints[f"{constants.POD_ID_PREFIX}{self.match_tier}"] = (
                self.pod.tiered_pod_id[self.match_tier]
            )

        result_table = self.result_database.get_records_with_column_value(
            self.record_path,
            constraints,
        )
        if result_table is None or result_table.num_rows == 0:
            return None

        if result_table.num_rows > 1:
            logger.info(
                f"Performing conflict resolution for multiple records for {input_packet.content_hash().display_name()}"
            )
            if self.retrieval_mode == "latest":
                result_table = result_table.sort_by(
                    self.DATA_RETRIEVED_FLAG, ascending=False
                ).take([0])
            elif self.retrieval_mode == "most_specific":
                # match by the most specific pod ID
                # trying next level if not found
                for k, v in reversed(self.tiered_pod_id.items()):
                    search_result = result_table.filter(
                        pc.field(f"{constants.POD_ID_PREFIX}{k}") == v
                    )
                    if search_result.num_rows > 0:
                        result_table = search_result.take([0])
                        break
                if result_table.num_rows > 1:
                    logger.warning(
                        f"No matching record found for {input_packet.content_hash().display_name()} with tiered pod ID {self.tiered_pod_id}"
                    )
                    result_table = result_table.sort_by(
                        self.DATA_RETRIEVED_FLAG, ascending=False
                    ).take([0])

            else:
                raise ValueError(
                    f"Unknown retrieval mode: {self.retrieval_mode}. Supported modes are 'latest' and 'most_specific'."
                )

        pod_id_columns = [
            f"{constants.POD_ID_PREFIX}{k}" for k in self.tiered_pod_id.keys()
        ]
        result_table = result_table.drop_columns(pod_id_columns)
        result_table = result_table.drop_columns(constants.INPUT_PACKET_HASH)

        # note that data context will be loaded from the result store
        return ArrowPacket(
            result_table,
            meta_info={self.DATA_RETRIEVED_FLAG: str(datetime.now(timezone.utc))},
        )

    def get_all_cached_outputs(
        self, include_system_columns: bool = False
    ) -> "pa.Table | None":
        """
        Get all records from the result store for this pod.
        If include_system_columns is True, include system columns in the result.
        """
        record_id_column = (
            constants.PACKET_RECORD_ID if include_system_columns else None
        )
        result_table = self.result_database.get_all_records(
            self.record_path, record_id_column=record_id_column
        )
        if result_table is None or result_table.num_rows == 0:
            return None

        if not include_system_columns:
            # remove input packet hash and tiered pod ID columns
            pod_id_columns = [
                f"{constants.POD_ID_PREFIX}{k}" for k in self.tiered_pod_id.keys()
            ]
            result_table = result_table.drop_columns(pod_id_columns)
            result_table = result_table.drop_columns(constants.INPUT_PACKET_HASH)

        return result_table
