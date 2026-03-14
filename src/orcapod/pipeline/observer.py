"""Execution observer protocol for pipeline orchestration.

Provides hooks for monitoring node and packet-level execution events
during orchestrated pipeline runs.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from orcapod.core.nodes import GraphNode
    from orcapod.protocols.core_protocols import PacketProtocol, TagProtocol


@runtime_checkable
class ExecutionObserver(Protocol):
    """Observer protocol for pipeline execution events.

    ``on_packet_start`` / ``on_packet_end`` are only invoked for function
    nodes. ``on_node_start`` / ``on_node_end`` are invoked for all node
    types.
    """

    def on_node_start(self, node: "GraphNode") -> None: ...
    def on_node_end(self, node: "GraphNode") -> None: ...
    def on_packet_start(
        self,
        node: "GraphNode",
        tag: "TagProtocol",
        packet: "PacketProtocol",
    ) -> None: ...
    def on_packet_end(
        self,
        node: "GraphNode",
        tag: "TagProtocol",
        input_packet: "PacketProtocol",
        output_packet: "PacketProtocol | None",
        cached: bool,
    ) -> None: ...


class NoOpObserver:
    """Default observer that does nothing."""

    def on_node_start(self, node: "GraphNode") -> None:
        pass

    def on_node_end(self, node: "GraphNode") -> None:
        pass

    def on_packet_start(
        self,
        node: "GraphNode",
        tag: "TagProtocol",
        packet: "PacketProtocol",
    ) -> None:
        pass

    def on_packet_end(
        self,
        node: "GraphNode",
        tag: "TagProtocol",
        input_packet: "PacketProtocol",
        output_packet: "PacketProtocol | None",
        cached: bool,
    ) -> None:
        pass
