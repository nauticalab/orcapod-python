from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from orcapod.protocols.core_protocols.datagrams import PacketProtocol

if TYPE_CHECKING:
    from orcapod.protocols.core_protocols.packet_function import PacketFunctionProtocol


@runtime_checkable
class PacketFunctionExecutorProtocol(Protocol):
    """Strategy for executing a packet function on a single packet.

    Executors decouple *what* a packet function computes from *where/how* it
    runs.  Each executor declares which ``packet_function_type_id`` values it
    supports.
    """

    @property
    def executor_type_id(self) -> str:
        """Unique identifier for this executor type, e.g. ``'local'``, ``'ray.v0'``."""
        ...

    def supported_function_type_ids(self) -> frozenset[str]:
        """Return the set of ``packet_function_type_id`` values this executor can handle.

        Return an empty frozenset to indicate support for *all* function types.
        """
        ...

    def supports(self, packet_function_type_id: str) -> bool:
        """Return ``True`` if this executor can run functions of the given type."""
        ...

    def execute(
        self,
        packet_function: PacketFunctionProtocol,
        packet: PacketProtocol,
    ) -> PacketProtocol | None:
        """Synchronously execute *packet_function* on *packet*.

        The executor should invoke ``packet_function.direct_call(packet)``
        in the appropriate execution environment.
        """
        ...

    async def async_execute(
        self,
        packet_function: PacketFunctionProtocol,
        packet: PacketProtocol,
    ) -> PacketProtocol | None:
        """Asynchronous counterpart of ``execute``."""
        ...

    @property
    def supports_concurrent_execution(self) -> bool:
        """Whether this executor can meaningfully run multiple packets concurrently.

        When ``True``, iteration machinery may submit all packets via
        ``async_execute`` concurrently and collect results before yielding.
        """
        ...

    def with_options(self, **opts: Any) -> "PacketFunctionExecutorProtocol":
        """Return an executor configured with the given per-node options.

        Used by the pipeline to produce node-specific executor instances
        (e.g. with different CPU/GPU allocations) from a shared base executor.
        Implementations that do not support options may return *self*.
        """
        ...

    def get_execution_data(self) -> dict[str, Any]:
        """Return metadata describing the execution environment.

        Stored alongside results for observability/provenance but does not
        affect content or pipeline hashes.
        """
        ...
