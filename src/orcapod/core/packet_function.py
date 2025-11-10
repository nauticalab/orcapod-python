import hashlib
import logging
import re
import sys
from abc import abstractmethod
from collections.abc import Callable, Collection, Iterable, Sequence
from typing import TYPE_CHECKING, Any, Literal

from orcapod.core.base import OrcapodBase
from orcapod.core.datagrams import DictPacket
from orcapod.hashing.hash_utils import get_function_components, get_function_signature
from orcapod.protocols.core_protocols import Packet
from orcapod.types import DataValue, PythonSchema, PythonSchemaLike
from orcapod.utils import schema_utils
from orcapod.utils.git_utils import get_git_info_for_python_object
from orcapod.utils.lazy_module import LazyModule


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

    @property
    def canonical_function_name(self) -> str:
        """
        Human-readable function identifier
        """
        return self._function_name

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
