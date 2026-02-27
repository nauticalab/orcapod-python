from typing import Protocol, runtime_checkable

from orcapod.protocols.core_protocols.datagrams import PacketProtocol, TagProtocol
from orcapod.protocols.core_protocols.packet_function import PacketFunctionProtocol
from orcapod.protocols.core_protocols.pod import PodProtocol


@runtime_checkable
class FunctionPodProtocol(PodProtocol, Protocol):
    """
    PodProtocol based on PacketFunctionProtocol.
    """

    @property
    def packet_function(self) -> PacketFunctionProtocol:
        """
        The PacketFunctionProtocol that defines the computation for this FunctionPodProtocol.
        """
        ...

    def process_packet(
        self, tag: TagProtocol, packet: PacketProtocol
    ) -> tuple[TagProtocol, PacketProtocol | None]: ...
