from __future__ import annotations

import inspect
import logging
import re
import sys
from abc import abstractmethod
from collections.abc import Callable, Collection, Iterable, Sequence
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Literal

from uuid_utils import uuid7

from orcapod.config import Config
from orcapod.contexts import DataContext
from orcapod.core.base import TraceableBase
from orcapod.core.datagrams import ArrowPacket, DictPacket
from orcapod.hashing.hash_utils import (
    get_function_components,
    get_function_signature,
)
from orcapod.protocols.core_protocols import Packet, PacketFunction
from orcapod.protocols.database_protocols import ArrowDatabase
from orcapod.system_constants import constants
from orcapod.types import DataValue, Schema, SchemaLike
from orcapod.utils import schema_utils
from orcapod.utils.git_utils import get_git_info_for_python_object
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa
    import pyarrow.compute as pc
else:
    pa = LazyModule("pyarrow")
    pc = LazyModule("pyarrow.compute")

logger = logging.getLogger(__name__)

error_handling_options = Literal["raise", "ignore", "warn"]


def parse_function_outputs(
    output_keys: Sequence[str], values: Any
) -> dict[str, DataValue]:
    """
    Map raw function return values to a keyed output dict.

    Rules:
        - ``output_keys = []``: return value is ignored; empty dict returned.
        - ``output_keys = ["result"]``: any value (including iterables) is stored as-is
          under the single key.
        - ``output_keys = ["a", "b", ...]``: ``values`` must be iterable and its length
          must match the number of keys.

    Args:
        output_keys: Ordered list of output key names.
        values: Raw return value from the function.

    Returns:
        Dict mapping each output key to its corresponding value.

    Raises:
        ValueError: If ``values`` is not iterable when multiple keys are given, or if
            the number of values does not match the number of keys.
    """
    if len(output_keys) == 0:
        output_values: list[Any] = []
    elif len(output_keys) == 1:
        output_values = [values]
    elif isinstance(values, Iterable):
        output_values = list(values)
    else:
        raise ValueError(
            "Values returned by function must be sequence-like if multiple output keys are specified"
        )

    if len(output_values) != len(output_keys):
        raise ValueError(
            f"Number of output keys {len(output_keys)}:{output_keys} does not match "
            f"number of values returned by function {len(output_values)}"
        )

    return dict(zip(output_keys, output_values))


class PacketFunctionBase(TraceableBase):
    """
    Abstract base class for PacketFunction, defining the interface and common functionality.
    """

    def __init__(
        self,
        version: str = "v0.0",
        label: str | None = None,
        data_context: str | DataContext | None = None,
        config: Config | None = None,
    ):
        super().__init__(label=label, data_context=data_context, config=config)
        self._active = True
        self._version = version

        # Parse version string to extract major and minor versions
        # 0.5.2 -> 0 and 5.2, 1.3rc -> 1 and 3rc
        match = re.match(r"\D*(\d+)\.(.*)", version)
        if match:
            self._major_version = int(match.group(1))
            self._minor_version = match.group(2)
        else:
            raise ValueError(
                f"Version string {version} does not contain a valid version number"
            )

        self._output_packet_schema_hash = None

    def computed_label(self) -> str | None:
        """
        If no explicit label is provided, use the canonical function name as the label.
        """
        return self.canonical_function_name

    @property
    def output_packet_schema_hash(self) -> str:
        """
        Return the hash of the output packet schema as a string.

        The hash is computed lazily on first access and cached for subsequent calls.

        Returns:
            str: The hash string of the output packet schema.
        """
        if self._output_packet_schema_hash is None:
            self._output_packet_schema_hash = (
                self.data_context.semantic_hasher.hash_object(
                    self.output_packet_schema
                ).to_string()
            )
        return self._output_packet_schema_hash

    @property
    def uri(self) -> tuple[str, ...]:
        return (
            self.canonical_function_name,
            self.output_packet_schema_hash,
            f"v{self.major_version}",
            self.packet_function_type_id,
        )

    def identity_structure(self) -> Any:
        return self.uri

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
        Unique function type identifier. This identifier is used for equivalence checks.
        e.g. "python.function.v1"
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
    def input_packet_schema(self) -> Schema:
        """
        Return the input typespec for the pod. This is used to validate the input streams.
        """
        ...

    @property
    @abstractmethod
    def output_packet_schema(self) -> Schema:
        """
        Return the output typespec for the pod. This is used to validate the output streams.
        """
        ...

    @abstractmethod
    def get_function_variation_data(self) -> dict[str, Any]:
        """Raw data defining function variation"""
        ...

    @abstractmethod
    def get_execution_data(self) -> dict[str, Any]:
        """Raw data defining execution context"""
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
        input_schema: SchemaLike | None = None,
        output_schema: SchemaLike | Sequence[type] | None = None,
        label: str | None = None,
        **kwargs,
    ) -> None:
        self._function = function

        # Reject functions with variadic parameters -- PythonPacketFunction maps
        # packet keys to named parameters, so the full parameter set must be fixed.
        _sig = inspect.signature(function)
        _variadic = [
            name
            for name, param in _sig.parameters.items()
            if param.kind
            in (
                inspect.Parameter.VAR_POSITIONAL,
                inspect.Parameter.VAR_KEYWORD,
            )
        ]
        if _variadic:
            raise ValueError(
                f"PythonPacketFunction does not support functions with variadic "
                f"parameters (*args / **kwargs). "
                f"Offending parameters: {_variadic!r}."
            )

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

        super().__init__(label=label, version=version, **kwargs)

        # extract input and output schema from the function signature
        self._input_schema, self._output_schema = schema_utils.extract_function_schemas(
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

        semantic_hasher = self.data_context.semantic_hasher
        self._function_signature_hash = semantic_hasher.hash_object(
            get_function_signature(function)
        ).to_string()
        self._function_content_hash = semantic_hasher.hash_object(
            get_function_components(self._function)
        ).to_string()
        self._output_schema_hash = semantic_hasher.hash_object(
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
    def input_packet_schema(self) -> Schema:
        """
        Return the input typespec for the pod. This is used to validate the input streams.
        """
        return self._input_schema

    @property
    def output_packet_schema(self) -> Schema:
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
        output_data = parse_function_outputs(self._output_keys, values)

        def combine(*components: tuple[str, ...]) -> str:
            inner_parsed = [":".join(component) for component in components]
            return "::".join(inner_parsed)

        record_id = str(uuid7())

        source_info = {k: combine(self.uri, (record_id,), (k,)) for k in output_data}

        return DictPacket(
            output_data,
            source_info=source_info,
            record_id=record_id,
            python_schema=self.output_packet_schema,
            data_context=self.data_context,
        )

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
    def input_packet_schema(self) -> Schema:
        return self._packet_function.input_packet_schema

    @property
    def output_packet_schema(self) -> Schema:
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

    # cloumn name containing indication of whether the result was computed
    RESULT_COMPUTED_FLAG = f"{constants.META_PREFIX}computed"

    def __init__(
        self,
        packet_function: PacketFunction,
        result_database: ArrowDatabase,
        record_path_prefix: tuple[str, ...] = (),
        **kwargs,
    ) -> None:
        super().__init__(packet_function, **kwargs)
        self._result_database = result_database
        self._record_path_prefix = record_path_prefix
        self._auto_flush = True

    def set_auto_flush(self, on: bool = True) -> None:
        """
        Set the auto-flush behavior of the result database.
        If set to True, the result database will flush after each record is added.
        """
        self._auto_flush = on

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
            logger.info("Checking for cache...")
            # lookup stored result for the input packet
            output_packet = self.get_cached_output_for_packet(packet)
            if output_packet is not None:
                logger.info(f"Cache hit for {packet}!")
        if output_packet is None:
            output_packet = self._packet_function.call(packet)
            if output_packet is not None:
                if not skip_cache_insert:
                    self.record_packet(packet, output_packet)
                # add meta column to indicate that this was computed
                output_packet.with_meta_columns(**{self.RESULT_COMPUTED_FLAG: True})

        return output_packet

    def get_cached_output_for_packet(self, input_packet: Packet) -> Packet | None:
        """
        Retrieve the output packet from the result store based on the input packet.
        If more than one output packet is found, conflict resolution strategy
        will be applied.
        If the output packet is not found, return None.
        """

        # get all records with matching the input packet hash
        # TODO: add match based on match_tier if specified

        # TODO: implement matching policy/strategy
        constraints = {
            constants.INPUT_PACKET_HASH_COL: input_packet.content_hash().to_string()
        }

        RECORD_ID_COLUMN = "_record_id"
        result_table = self._result_database.get_records_with_column_value(
            self.record_path,
            constraints,
            record_id_column=RECORD_ID_COLUMN,
        )

        if result_table is None or result_table.num_rows == 0:
            return None

        if result_table.num_rows > 1:
            logger.info(
                f"Performing conflict resolution for multiple records for {input_packet.content_hash().display_name()}"
            )
            result_table = result_table.sort_by(
                [(constants.POD_TIMESTAMP, "descending")]
            ).take([0])

        # extract the record_id column
        record_id = result_table.to_pylist()[0][RECORD_ID_COLUMN]
        result_table = result_table.drop_columns(
            [RECORD_ID_COLUMN, constants.INPUT_PACKET_HASH_COL]
        )

        # note that data context will be loaded from the result store
        return ArrowPacket(
            result_table,
            record_id=record_id,
            meta_info={self.RESULT_COMPUTED_FLAG: False},
        )

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

        i = 0
        for k, v in self.get_function_variation_data().items():
            # add the tiered pod ID to the data table
            data_table = data_table.add_column(
                i,
                f"{constants.PF_VARIATION_PREFIX}{k}",
                pa.array([v], type=pa.large_string()),
            )
            i += 1

        for k, v in self.get_execution_data().items():
            # add the tiered pod ID to the data table
            data_table = data_table.add_column(
                i,
                f"{constants.PF_EXECUTION_PREFIX}{k}",
                pa.array([v], type=pa.large_string()),
            )
            i += 1

        # add the input packet hash as a column
        data_table = data_table.add_column(
            0,
            constants.INPUT_PACKET_HASH_COL,
            pa.array([input_packet.content_hash().to_string()], type=pa.large_string()),
        )

        # add computation timestamp
        timestamp = datetime.now(timezone.utc)
        data_table = data_table.append_column(
            constants.POD_TIMESTAMP,
            pa.array([timestamp], type=pa.timestamp("us", tz="UTC")),
        )

        self._result_database.add_record(
            self.record_path,
            output_packet.datagram_id,  # output packet datagram ID (uuid) is used as a unique identification
            data_table,
            skip_duplicates=skip_duplicates,
        )

        if self._auto_flush:
            self._result_database.flush()

        # TODO: make store return retrieved table
        return output_packet

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
