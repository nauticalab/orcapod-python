"""Node protocols for orchestrator interaction.

Defines the three node protocols (Source, Function, Operator) that
formalize the interface between orchestrators and graph nodes, plus
TypeGuard dispatch functions for runtime type narrowing.

Each protocol exposes ``execute`` (sync) and ``async_execute`` (async).
Nodes own their execution — caching, per-packet logic, and persistence
are internal. Orchestrators are topology schedulers.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Literal, Protocol, TypeGuard, runtime_checkable

if TYPE_CHECKING:
    from orcapod.channels import ReadableChannel, WritableChannel
    from orcapod.core.nodes import GraphNode
    from orcapod.protocols.observability_protocols import ExecutionObserverProtocol
    from orcapod.protocols.core_protocols import (
        PacketProtocol,
        StreamProtocol,
        TagProtocol,
    )


@runtime_checkable
class SourceNodeProtocol(Protocol):
    """Protocol for source nodes in orchestrated execution."""

    node_type: str

    @property
    def node_uri(self) -> tuple[str, ...]:
        """Canonical URI tuple identifying this node's computation.

        Used for path scoping in persistent storage (pipeline_path construction)
        and serialization (``node_uri`` field in save files).
        """
        ...

    def execute(
        self,
        *,
        observer: ExecutionObserverProtocol | None = None,
    ) -> list[tuple[TagProtocol, PacketProtocol]]: ...

    async def async_execute(
        self,
        output: WritableChannel[tuple[TagProtocol, PacketProtocol]],
        *,
        observer: ExecutionObserverProtocol | None = None,
    ) -> None: ...


@runtime_checkable
class FunctionNodeProtocol(Protocol):
    """Protocol for function nodes in orchestrated execution."""

    node_type: str

    @property
    def node_uri(self) -> tuple[str, ...]:
        """Canonical URI tuple identifying this node's computation."""
        ...

    @property
    def pipeline_path(self) -> tuple[str, ...]:
        """The node's pipeline path for storage scoping.

        Returns ``()`` when no pipeline database is attached.
        """
        ...

    def execute(
        self,
        input_stream: StreamProtocol,
        *,
        observer: ExecutionObserverProtocol | None = None,
        error_policy: Literal["continue", "fail_fast"] = "continue",
    ) -> list[tuple[TagProtocol, PacketProtocol]]: ...

    async def async_execute(
        self,
        input_channel: ReadableChannel[tuple[TagProtocol, PacketProtocol]],
        output: WritableChannel[tuple[TagProtocol, PacketProtocol]],
        *,
        observer: ExecutionObserverProtocol | None = None,
    ) -> None: ...


@runtime_checkable
class OperatorNodeProtocol(Protocol):
    """Protocol for operator nodes in orchestrated execution."""

    node_type: str

    @property
    def node_uri(self) -> tuple[str, ...]:
        """Canonical URI tuple identifying this node's computation."""
        ...

    def execute(
        self,
        *input_streams: StreamProtocol,
        observer: ExecutionObserverProtocol | None = None,
    ) -> list[tuple[TagProtocol, PacketProtocol]]: ...

    async def async_execute(
        self,
        inputs: Sequence[ReadableChannel[tuple[TagProtocol, PacketProtocol]]],
        output: WritableChannel[tuple[TagProtocol, PacketProtocol]],
        *,
        observer: ExecutionObserverProtocol | None = None,
    ) -> None: ...


def is_source_node(node: GraphNode) -> TypeGuard[SourceNodeProtocol]:
    """Check if a node is a source node."""
    return node.node_type == "source"


def is_function_node(node: GraphNode) -> TypeGuard[FunctionNodeProtocol]:
    """Check if a node is a function node."""
    return node.node_type == "function"


def is_operator_node(node: GraphNode) -> TypeGuard[OperatorNodeProtocol]:
    """Check if a node is an operator node."""
    return node.node_type == "operator"
