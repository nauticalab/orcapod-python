from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from orcapod.protocols.core_protocols.datagrams import PacketProtocol


@runtime_checkable
class PacketFunctionExecutorProtocol(Protocol):
    """
    Strategy for executing a packet function on a single packet.

    Executors decouple *what* a packet function computes from *where/how* it
    runs.  Each executor declares which ``packet_function_type_id`` values it
    supports so that, e.g., a Ray executor only accepts Python-backed packet
    functions.
    """

    @property
    def executor_type_id(self) -> str:
        """Unique identifier for this executor type, e.g. ``'local'``, ``'ray.v0'``."""
        ...

    def supported_function_type_ids(self) -> frozenset[str]:
        """
        Set of ``packet_function_type_id`` values this executor can handle.

        Return an empty frozenset to indicate support for *all* function types.
        """
        ...

    def supports(self, packet_function_type_id: str) -> bool:
        """Return ``True`` if this executor can run functions of the given type."""
        ...

    def execute(
        self,
        packet_function: Any,
        packet: PacketProtocol,
    ) -> PacketProtocol | None:
        """
        Synchronously execute *packet_function* on *packet*.

        The executor receives the packet function so it can invoke
        ``packet_function.direct_call(packet)`` (bypassing executor routing)
        in the appropriate execution environment.
        """
        ...

    async def async_execute(
        self,
        packet_function: Any,
        packet: PacketProtocol,
    ) -> PacketProtocol | None:
        """Asynchronous counterpart of :meth:`execute`."""
        ...

    def get_execution_data(self) -> dict[str, Any]:
        """
        Return metadata describing the execution environment.

        Stored alongside results for observability/provenance but does **not**
        affect content or pipeline hashes.
        """
        ...
