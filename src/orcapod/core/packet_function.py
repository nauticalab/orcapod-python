import hashlib
import logging
import re
import sys
from abc import abstractmethod
from collections.abc import Callable, Collection, Iterable, Sequence
from typing import TYPE_CHECKING, Any, Literal

from orcapod.core.base import OrcapodBase
from orcapod.core.datagrams import DictPacket, ArrowPacket
from orcapod.hashing.hash_utils import get_function_components, get_function_signature
from orcapod.protocols.core_protocols import Packet, PacketFunction, Tag, Stream
from orcapod.types import DataValue, PythonSchema, PythonSchemaLike
from orcapod.utils import schema_utils
from orcapod.utils.git_utils import get_git_info_for_python_object
from orcapod.utils.lazy_module import LazyModule
from orcapod.protocols.database_protocols import ArrowDatabase
from orcapod.system_constants import constants
from datetime import datetime, timezone


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


class PacketFunctionBase(OrcapodBase):
    """
    Abstract base class for PacketFunction, defining the interface and common functionality.
    """

    def __init__(self, version: str = "v0.0", **kwargs):
        super().__init__(**kwargs)
        self._active = True
        self._version = version

        match = re.match(r"\D.*(\d+)", version)
        if match:
            self._major_version = int(match.group(1))
            self._minor_version = version[match.end(1) :]
        else:
            raise ValueError(
                f"Version string {version} does not contain a valid version number"
            )

    @property
    def uri(self) -> tuple[str, ...]:
        # TODO: make this more efficient
        return (
            f"{self.packet_function_type_id}",
            f"{self.canonical_function_name}",
            self.data_context.object_hasher.hash_object(
                self.output_packet_schema
            ).to_string(),
        )

    def identity_structure(self) -> Any:
        return self.get_function_variation_data()

    @property
    def major_version(self) -> int:
        return self._major_version

    @property
    def minor_version_string(self) -> str:
        return self._minor_version

    @property
    @abstractmethod
    def packet_function_type_id(self) -> str:
        """
        Unique function type identifier
        """
        ...

    @property
    @abstractmethod
    def canonical_function_name(self) -> str:
        """
        Human-readable function identifier
        """
        ...

    @property
    @abstractmethod
    def input_packet_schema(self) -> PythonSchema:
        """
        Return the input typespec for the pod. This is used to validate the input streams.
        """
        ...

    @property
    @abstractmethod
    def output_packet_schema(self) -> PythonSchema:
        """
        Return the output typespec for the pod. This is used to validate the output streams.
        """
        ...

    @abstractmethod
    def get_function_variation_data(self) -> dict[str, Any]:
        """Raw data defining function variation - system computes hash"""
        ...

    @abstractmethod
    def get_execution_data(self) -> dict[str, Any]:
        """Raw data defining execution context - system computes hash"""
        ...

    @abstractmethod
    def call(self, packet: Packet) -> Packet | None:
        """
        Process the input packet and return the output packet.
        """
        ...

    @abstractmethod
    async def async_call(self, packet: Packet) -> Packet | None:
        """
        Asynchronously process the input packet and return the output packet.
        """
        ...


class PythonPacketFunction(PacketFunctionBase):
    @property
    def packet_function_type_id(self) -> str:
        """
        Unique function type identifier
        """
        return "python.function.v0"

    def __init__(
        self,
        function: Callable[..., Any],
        output_keys: str | Collection[str] | None = None,
        function_name: str | None = None,
        version: str = "v0.0",
        input_schema: PythonSchemaLike | None = None,
        output_schema: PythonSchemaLike | Sequence[type] | None = None,
        label: str | None = None,
        **kwargs,
    ) -> None:
        self._function = function

        if output_keys is None:
            output_keys = []
        if isinstance(output_keys, str):
            output_keys = [output_keys]
        self._output_keys = output_keys
        if function_name is None:
            if hasattr(self._function, "__name__"):
                function_name = getattr(self._function, "__name__")
            else:
                raise ValueError(
                    "function_name must be provided if function has no __name__"
                )

        assert function_name is not None
        self._function_name = function_name

        super().__init__(label=label or self._function_name, version=version, **kwargs)

        # extract input and output schema from the function signature
        input_schema, output_schema = schema_utils.extract_function_typespecs(
            self._function,
            self._output_keys,
            input_typespec=input_schema,
            output_typespec=output_schema,
        )

        # get git info for the function
        # TODO: turn this into optional addition
        env_info = get_git_info_for_python_object(self._function)
        if env_info is None:
            git_hash = "unknown"
        else:
            git_hash = env_info.get("git_commit_hash", "unknown")
            if env_info.get("git_repo_status") == "dirty":
                git_hash += "-dirty"
        self._git_hash = git_hash

        self._input_schema = input_schema
        self._output_schema = output_schema

        object_hasher = self.data_context.object_hasher
        self._function_signature_hash = object_hasher.hash_object(
            get_function_signature(function)
        ).to_string()
        self._function_content_hash = object_hasher.hash_object(
            get_function_components(self._function)
        ).to_string()
        self._output_schema_hash = object_hasher.hash_object(
            self.output_packet_schema
        ).to_string()

    @property
    def canonical_function_name(self) -> str:
        """
        Human-readable function identifier
        """
        return self._function_name

    def get_function_variation_data(self) -> dict[str, Any]:
        """Raw data defining function variation - system computes hash"""
        return {
            "function_name": self._function_name,
            "function_signature_hash": self._function_signature_hash,
            "function_content_hash": self._function_content_hash,
            "git_hash": self._git_hash,
        }

    def get_execution_data(self) -> dict[str, Any]:
        """Raw data defining execution context - system computes hash"""
        python_version_info = sys.version_info
        python_version_str = f"{python_version_info.major}.{python_version_info.minor}.{python_version_info.micro}"
        return {"python_version": python_version_str, "execution_context": "local"}

    @property
    def input_packet_schema(self) -> PythonSchema:
        """
        Return the input typespec for the pod. This is used to validate the input streams.
        """
        return self._input_schema

    @property
    def output_packet_schema(self) -> PythonSchema:
        """
        Return the output typespec for the pod. This is used to validate the output streams.
        """
        return self._output_schema

    def is_active(self) -> bool:
        """
        Check if the pod is active. If not, it will not process any packets.
        """
        return self._active

    def set_active(self, active: bool = True) -> None:
        """
        Set the active state of the pod. If set to False, the pod will not process any packets.
        """
        self._active = active

    def call(self, packet: Packet) -> Packet | None:
        if not self._active:
            return None
        values = self._function(**packet.as_dict())
        output_values = []

        if len(self._output_keys) == 0:
            output_values = []
        elif len(self._output_keys) == 1:
            output_values = [values]  # type: ignore
        elif isinstance(values, Iterable):
            output_values = list(values)  # type: ignore
        elif len(self._output_keys) > 1:
            raise ValueError(
                "Values returned by function must be sequence-like if multiple output keys are specified"
            )

        if len(output_values) != len(self._output_keys):
            raise ValueError(
                f"Number of output keys {len(self._output_keys)}:{self._output_keys} does not match number of values returned by function {len(output_values)}"
            )

        return DictPacket({k: v for k, v in zip(self._output_keys, output_values)})

    async def async_call(self, packet: Packet) -> Packet | None:
        raise NotImplementedError("Async call not implemented for synchronous function")


class PacketFunctionWrapper(PacketFunctionBase):
    """
    Wrapper around a PacketFunction to modify or extend its behavior.
    """

    def __init__(self, packet_function: PacketFunction, **kwargs) -> None:
        super().__init__(**kwargs)
        self._packet_function = packet_function

    def computed_label(self) -> str | None:
        return self._packet_function.label

    @property
    def major_version(self) -> int:
        return self._packet_function.major_version

    @property
    def minor_version_string(self) -> str:
        return self._packet_function.minor_version_string

    @property
    def packet_function_type_id(self) -> str:
        return self._packet_function.packet_function_type_id

    @property
    def canonical_function_name(self) -> str:
        return self._packet_function.canonical_function_name

    @property
    def input_packet_schema(self) -> PythonSchema:
        return self._packet_function.input_packet_schema

    @property
    def output_packet_schema(self) -> PythonSchema:
        return self._packet_function.output_packet_schema

    def get_function_variation_data(self) -> dict[str, Any]:
        return self._packet_function.get_function_variation_data()

    def get_execution_data(self) -> dict[str, Any]:
        return self._packet_function.get_execution_data()

    def call(self, packet: Packet) -> Packet | None:
        return self._packet_function.call(packet)

    async def async_call(self, packet: Packet) -> Packet | None:
        return await self._packet_function.async_call(packet)


class CachedPacketFunction(PacketFunctionWrapper):
    """
    Wrapper around a PacketFunction that caches results for identical input packets.
    """

    # name of the column in the tag store that contains the packet hash
    DATA_RETRIEVED_FLAG = f"{constants.META_PREFIX}data_retrieved"

    def __init__(
        self,
        packet_function: PacketFunction,
        result_database: ArrowDatabase,
        record_path_prefix: tuple[str, ...] = (),
        **kwargs,
    ) -> None:
        super().__init__(packet_function, **kwargs)
        self._record_path_prefix = record_path_prefix
        self._result_database = result_database

    @property
    def record_path(self) -> tuple[str, ...]:
        """
        Return the path to the record in the result store.
        This is used to store the results of the pod.
        """
        return self._record_path_prefix + self.uri

    def call(
        self,
        packet: Packet,
        *,
        skip_cache_lookup: bool = False,
        skip_cache_insert: bool = False,
    ) -> Packet | None:
        # execution_engine_hash = execution_engine.name if execution_engine else "default"
        output_packet = None
        if not skip_cache_lookup:
            print("Checking for cache...")
            output_packet = self.get_cached_output_for_packet(packet)
            if output_packet is not None:
                print(f"Cache hit for {packet}!")
        if output_packet is None:
            output_packet = self._packet_function.call(packet)
            if output_packet is not None and not skip_cache_insert:
                self.record_packet(packet, output_packet)

        return output_packet

    def record_packet(
        self,
        input_packet: Packet,
        output_packet: Packet,
        skip_duplicates: bool = False,
    ) -> Packet:
        """
        Record the output packet against the input packet in the result store.
        """

        # TODO: consider incorporating execution_engine_opts into the record
        data_table = output_packet.as_table(columns={"source": True, "context": True})

        # for i, (k, v) in enumerate(self.tiered_pod_id.items()):
        #     # add the tiered pod ID to the data table
        #     data_table = data_table.add_column(
        #         i,
        #         f"{constants.POD_ID_PREFIX}{k}",
        #         pa.array([v], type=pa.large_string()),
        #     )

        # add the input packet hash as a column
        data_table = data_table.add_column(
            0,
            constants.INPUT_PACKET_HASH_COL,
            pa.array([input_packet.content_hash().to_string()], type=pa.large_string()),
        )
        # # add execution engine information
        # execution_engine_hash = execution_engine.name if execution_engine else "default"
        # data_table = data_table.append_column(
        #     constants.EXECUTION_ENGINE,
        #     pa.array([execution_engine_hash], type=pa.large_string()),
        # )

        # add computation timestamp
        timestamp = datetime.now(timezone.utc)
        data_table = data_table.append_column(
            constants.POD_TIMESTAMP,
            pa.array([timestamp], type=pa.timestamp("us", tz="UTC")),
        )

        # if record_id is None:
        #     record_id = self.get_record_id(
        #         input_packet, execution_engine_hash=execution_engine_hash
        #     )

        # self.result_database.add_record(
        #     self.record_path,
        #     record_id,
        #     data_table,
        #     skip_duplicates=skip_duplicates,
        # )
        # if result_flag is None:
        #     # TODO: do more specific error handling
        #     raise ValueError(
        #         f"Failed to record packet {input_packet} in result store {self.result_store}"
        #     )
        # # TODO: make store return retrieved table
        return output_packet

    def get_cached_output_for_packet(self, input_packet: Packet) -> Packet | None:
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

        # TODO: implement matching policy/strategy
        constraints = {
            constants.INPUT_PACKET_HASH_COL: input_packet.content_hash().to_string()
        }

        result_table = self._result_database.get_records_with_column_value(
            self.record_path,
            constraints,
        )
        if result_table is None or result_table.num_rows == 0:
            return None

        if result_table.num_rows > 1:
            logger.info(
                f"Performing conflict resolution for multiple records for {input_packet.content_hash().display_name()}"
            )
            result_table = result_table.sort_by(
                constants.POD_TIMESTAMP, ascending=False
            ).take([0])

        # result_table = result_table.drop_columns(pod_id_columns)
        result_table = result_table.drop_columns(constants.INPUT_PACKET_HASH_COL)

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
        result_table = self._result_database.get_all_records(
            self.record_path, record_id_column=record_id_column
        )
        if result_table is None or result_table.num_rows == 0:
            return None

        # if not include_system_columns:
        #     # remove input packet hash and tiered pod ID columns
        #     pod_id_columns = [
        #         f"{constants.POD_ID_PREFIX}{k}" for k in self.tiered_pod_id.keys()
        #     ]
        #     result_table = result_table.drop_columns(pod_id_columns)
        #     result_table = result_table.drop_columns(constants.INPUT_PACKET_HASH_COL)

        return result_table
