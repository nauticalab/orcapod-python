from typing import Protocol, runtime_checkable

from orcapod.protocols.core_protocols.datagrams import Packet
from orcapod.protocols.core_protocols.packet_function import PacketFunction
from orcapod.protocols.core_protocols.pod import Pod


@runtime_checkable
class FunctionPod(Pod, Protocol):
    """
    Pod based on PacketFunction.


    """

    @property
    def packet_function(self) -> PacketFunction:
        """
        The PacketFunction that defines the computation for this FunctionPod.
        """
        ...

    def process_packet(self, packet: Packet) -> Packet | None:
        """
        Process a single packet using the pod's PacketFunction.

        Args:
            packet (Packet): The input packet to process.

        Returns:
            Packet | None: The processed packet, or None if filtered out.
        """
        ...
