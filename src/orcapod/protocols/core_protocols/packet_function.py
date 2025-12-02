from typing import Any, Protocol, runtime_checkable

from orcapod.protocols.core_protocols.datagrams import Packet
from orcapod.protocols.core_protocols.labelable import Labelable
from orcapod.protocols.hashing_protocols import ContentIdentifiable
from orcapod.types import PythonSchema


@runtime_checkable
class PacketFunction(ContentIdentifiable, Labelable, Protocol):
    """
    Protocol for packet-processing function.

    Processes individual packets with declared input/output schemas.
    """

    # ==================== Identity & Metadata ====================
    @property
    def packet_function_type_id(self) -> str:
        """How functions are defined and executed (e.g., python.function.v2)"""
        ...

    @property
    def canonical_function_name(self) -> str:
        """Human-readable function identifier"""
        ...

    @property
    def major_version(self) -> int:
        """Breaking changes increment this"""
        ...

    @property
    def minor_version_string(self) -> str:
        """Flexible minor version (e.g., "1", "4.3rc", "apple")"""
        ...

    @property
    def input_packet_schema(self) -> PythonSchema:
        """
        Schema for input packets that this packet function can process.

        Defines the exact schema that input packets must conform to.

        This specification is used for:
        - Runtime type validation
        - Compile-time type checking
        - Schema inference and documentation
        - Input validation and error reporting

        Returns:
            PythonSchema: Output packet schema as a dictionary mapping
        """
        ...

    @property
    def output_packet_schema(self) -> PythonSchema:
        """
        Schema for output packets that this packet function produces.

        This is typically determined by the packet function's computational logic
        and is used for:
        - Type checking downstream kernels
        - Schema inference in complex pipelines
        - Query planning and optimization
        - Documentation and developer tooling

        Returns:
            PythonSchema: Output packet schema as a dictionary mapping
        """
        ...

    # ==================== Content-Addressable Identity ====================
    def get_function_variation_data(self) -> dict[str, Any]:
        """Raw data defining function variation - system computes hash"""
        ...

    def get_execution_data(self) -> dict[str, Any]:
        """Raw data defining execution context - system computes hash"""
        ...

    async def async_call(
        self,
        packet: Packet,
    ) -> Packet | None:
        """
        Asynchronously process a single packet

        This is the core method that defines the packet function's computational behavior.
        It processes one packet at a time, enabling:
        - Fine-grained caching at the packet level
        - Parallelization opportunities
        - Just-in-time evaluation
        - Filtering operations (by returning None)

        The method signature supports:
        - Packet transformation (modify content)
        - Filtering (return None to exclude packet)
        - Pass-through (return inputs unchanged)

        Args:
            packet: The data payload to process

        Returns:
            Packet | None: Processed packet, or None to filter it out

        Raises:
            TypeError: If packet doesn't match input_packet_types
            ValueError: If packet data is invalid for processing
        """
        ...

    def call(
        self,
        packet: Packet,
    ) -> Packet | None:
        """
        Process a single packet

        This is the core method that defines the packet function's computational behavior.
        It processes one packet at a time, enabling:
        - Fine-grained caching at the packet level
        - Parallelization opportunities
        - Just-in-time evaluation
        - Filtering operations (by returning None)

        The method signature supports:
        - Packet transformation (modify content)
        - Filtering (return None to exclude packet)
        - Pass-through (return inputs unchanged)

        Args:
            packet: The data payload to process

        Returns:
            Packet | None: Processed packet, or None to filter it out

        Raises:
            TypeError: If packet doesn't match input_packet_types
            ValueError: If packet data is invalid for processing
        """
        ...
