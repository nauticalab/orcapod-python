"""Node protocols for orchestrator interaction.

Defines the three node protocols (Source, Function, Operator) that
formalize the interface between orchestrators and graph nodes, plus
TypeGuard dispatch functions for runtime type narrowing.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING, Protocol, TypeGuard, runtime_checkable

if TYPE_CHECKING:
    from orcapod.core.nodes import GraphNode
    from orcapod.protocols.core_protocols import (
        PacketProtocol,
        StreamProtocol,
        TagProtocol,
    )


@runtime_checkable
class SourceNodeProtocol(Protocol):
    """Protocol for source nodes in orchestrated execution."""

    node_type: str

    def iter_packets(self) -> Iterator[tuple["TagProtocol", "PacketProtocol"]]: ...
    def store_result(
        self, results: list[tuple["TagProtocol", "PacketProtocol"]]
    ) -> None: ...


@runtime_checkable
class FunctionNodeProtocol(Protocol):
    """Protocol for function nodes in orchestrated execution."""

    node_type: str

    def get_cached_results(
        self, entry_ids: list[str]
    ) -> dict[str, tuple["TagProtocol", "PacketProtocol"]]: ...

    def compute_pipeline_entry_id(
        self, tag: "TagProtocol", packet: "PacketProtocol"
    ) -> str: ...

    def process_packet(
        self, tag: "TagProtocol", packet: "PacketProtocol"
    ) -> tuple["TagProtocol", "PacketProtocol | None"]: ...

    def store_result(
        self,
        tag: "TagProtocol",
        input_packet: "PacketProtocol",
        output_packet: "PacketProtocol | None",
    ) -> None: ...


@runtime_checkable
class OperatorNodeProtocol(Protocol):
    """Protocol for operator nodes in orchestrated execution."""

    node_type: str

    def process(
        self, *input_streams: "StreamProtocol"
    ) -> list[tuple["TagProtocol", "PacketProtocol"]]: ...

    def get_cached_output(self) -> "StreamProtocol | None": ...


def is_source_node(node: "GraphNode") -> TypeGuard[SourceNodeProtocol]:
    """Check if a node is a source node."""
    return node.node_type == "source"


def is_function_node(node: "GraphNode") -> TypeGuard[FunctionNodeProtocol]:
    """Check if a node is a function node."""
    return node.node_type == "function"


def is_operator_node(node: "GraphNode") -> TypeGuard[OperatorNodeProtocol]:
    """Check if a node is an operator node."""
    return node.node_type == "operator"
