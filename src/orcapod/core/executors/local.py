from __future__ import annotations

from typing import TYPE_CHECKING

from orcapod.core.executors.base import PacketFunctionExecutorBase

if TYPE_CHECKING:
    from orcapod.protocols.core_protocols import PacketFunctionProtocol, PacketProtocol


class LocalExecutor(PacketFunctionExecutorBase):
    """
    Default executor — runs the packet function directly in the current process.

    Supports all packet function types (``supported_function_type_ids``
    returns an empty set).
    """

    @property
    def executor_type_id(self) -> str:
        return "local"

    def supported_function_type_ids(self) -> frozenset[str]:
        return frozenset()

    def execute(
        self,
        packet_function: PacketFunctionProtocol,
        packet: PacketProtocol,
    ) -> PacketProtocol | None:
        return packet_function.direct_call(packet)

    async def async_execute(
        self,
        packet_function: PacketFunctionProtocol,
        packet: PacketProtocol,
    ) -> PacketProtocol | None:
        return await packet_function.direct_async_call(packet)
