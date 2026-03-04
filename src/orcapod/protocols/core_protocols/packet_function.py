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
    """Protocol for a packet-processing function.

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
        """Schema describing the input packets this function accepts."""
        ...

    @property
    def output_packet_schema(self) -> Schema:
        """Schema describing the output packets this function produces."""
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
        """Process a single packet, routing through the executor if one is set.

        Args:
            packet: The data payload to process.

        Returns:
            The processed packet, or ``None`` to filter it out.
        """
        ...

    async def async_call(
        self,
        packet: PacketProtocol,
    ) -> PacketProtocol | None:
        """Asynchronously process a single packet, routing through the executor if set.

        Args:
            packet: The data payload to process.

        Returns:
            The processed packet, or ``None`` to filter it out.
        """
        ...

    def direct_call(
        self,
        packet: PacketProtocol,
    ) -> PacketProtocol | None:
        """Execute the function's native computation on *packet*.

        This is the method executors invoke, bypassing executor routing.

        Args:
            packet: The data payload to process.

        Returns:
            The processed packet, or ``None`` to filter it out.
        """
        ...

    async def direct_async_call(
        self,
        packet: PacketProtocol,
    ) -> PacketProtocol | None:
        """Asynchronous counterpart of ``direct_call``."""
        ...
