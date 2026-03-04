from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from orcapod.protocols.core_protocols.datagrams import PacketProtocol
from orcapod.protocols.core_protocols.executor import PacketFunctionExecutorProtocol
from orcapod.protocols.core_protocols.labelable import LabelableProtocol
from orcapod.protocols.hashing_protocols import (
    ContentIdentifiableProtocol,
    PipelineElementProtocol,
)
from orcapod.types import Schema


@runtime_checkable
class PacketFunctionProtocol(
    ContentIdentifiableProtocol, PipelineElementProtocol, LabelableProtocol, Protocol
):
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
    def input_packet_schema(self) -> Schema:
        """
        Schema for input packets that this packet function can process.

        Defines the exact schema that input packets must conform to.

        This specification is used for:
        - Runtime type validation
        - Compile-time type checking
        - Schema inference and documentation
        - Input validation and error reporting

        Returns:
            Schema: Output packet schema as a dictionary mapping
        """
        ...

    @property
    def output_packet_schema(self) -> Schema:
        """
        Schema for output packets that this packet function produces.

        This is typically determined by the packet function's computational logic
        and is used for:
        - Type checking downstream kernels
        - Schema inference in complex pipelines
        - Query planning and optimization
        - Documentation and developer tooling

        Returns:
            Schema: Output packet schema as a dictionary mapping
        """
        ...

    # ==================== Content-Addressable Identity ====================
    def get_function_variation_data(self) -> dict[str, Any]:
        """Raw data defining function variation - system computes hash"""
        ...

    def get_execution_data(self) -> dict[str, Any]:
        """Raw data defining execution context - system computes hash"""
        ...

    # ==================== Executor ====================

    @property
    def executor(self) -> PacketFunctionExecutorProtocol | None:
        """The executor used to run this function, or ``None`` for direct execution."""
        ...

    @executor.setter
    def executor(self, executor: PacketFunctionExecutorProtocol | None) -> None:
        """Set or clear the executor."""
        ...

    # ==================== Execution ====================

    def call(
        self,
        packet: PacketProtocol,
    ) -> PacketProtocol | None:
        """
        Process a single packet, routing through the executor if one is set.

        Callers should use this method.  Subclasses should override
        :meth:`direct_call` to provide the native computation.

        Args:
            packet: The data payload to process

        Returns:
            PacketProtocol | None: Processed packet, or None to filter it out
        """
        ...

    async def async_call(
        self,
        packet: PacketProtocol,
    ) -> PacketProtocol | None:
        """
        Asynchronously process a single packet, routing through the executor
        if one is set.

        Args:
            packet: The data payload to process

        Returns:
            PacketProtocol | None: Processed packet, or None to filter it out
        """
        ...

    def direct_call(
        self,
        packet: PacketProtocol,
    ) -> PacketProtocol | None:
        """
        Execute the function's native computation on *packet*.

        This is the method executors invoke to bypass executor routing and
        run the computation directly.

        Args:
            packet: The data payload to process

        Returns:
            PacketProtocol | None: Processed packet, or None to filter it out
        """
        ...

    async def direct_async_call(
        self,
        packet: PacketProtocol,
    ) -> PacketProtocol | None:
        """Asynchronous counterpart of :meth:`direct_call`."""
        ...
