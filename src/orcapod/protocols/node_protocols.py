"""Node protocols for orchestrator interaction.

Defines the four node protocols (Source, Function, Operator, SideEffect) that
formalize the interface between orchestrators and graph nodes, plus
TypeGuard dispatch functions for runtime type narrowing.

Each protocol exposes ``execute`` (sync) and ``async_execute`` (async).
Nodes own their execution — caching, per-data logic, and persistence
are internal. Orchestrators are topology schedulers.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Literal, Protocol, TypeGuard, runtime_checkable

if TYPE_CHECKING:
    from orcapod.channels import ReadableChannel, WritableChannel
    from orcapod.core.nodes import GraphNode
    from orcapod.protocols.database_protocols import ArrowDatabaseProtocol
    from orcapod.protocols.observability_protocols import ExecutionObserverProtocol
    from orcapod.protocols.core_protocols import (
        DataProtocol,
        StreamProtocol,
        TagProtocol,
    )
    from orcapod.types import NodeConfig


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
    ) -> list[tuple[TagProtocol, DataProtocol]]: ...

    async def async_execute(
        self,
        output: WritableChannel[tuple[TagProtocol, DataProtocol]],
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

    @property
    def node_config(self) -> "NodeConfig": ...

    @node_config.setter
    def node_config(self, value: "NodeConfig") -> None: ...

    def execute(
        self,
        input_stream: StreamProtocol,
        *,
        observer: ExecutionObserverProtocol | None = None,
        error_policy: Literal["continue", "fail_fast"] = "continue",
    ) -> list[tuple[TagProtocol, DataProtocol]]: ...

    async def async_execute(
        self,
        input_channel: ReadableChannel[tuple[TagProtocol, DataProtocol]],
        output: WritableChannel[tuple[TagProtocol, DataProtocol]],
        *,
        observer: ExecutionObserverProtocol | None = None,
    ) -> None: ...

    def set_ephemeral_store(self, store: "ArrowDatabaseProtocol | None") -> None:
        """Assign or remove the ephemeral result store for this node.

        Pass an ``ArrowDatabaseProtocol`` to attach the store.
        Pass ``None`` to detach it — the node falls back to persistent-only
        behaviour for subsequent writes. No-op for node types that do not
        support ephemeral result storage (e.g. blueprint ``FunctionNode``).
        """
        ...


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
    ) -> list[tuple[TagProtocol, DataProtocol]]: ...

    async def async_execute(
        self,
        inputs: Sequence[ReadableChannel[tuple[TagProtocol, DataProtocol]]],
        output: WritableChannel[tuple[TagProtocol, DataProtocol]],
        *,
        observer: ExecutionObserverProtocol | None = None,
    ) -> None: ...

    def set_ephemeral_store(self, store: "ArrowDatabaseProtocol | None") -> None:
        """Assign or remove the ephemeral result store for this node.

        No-op for operator nodes in v1 — full ephemeral support for operators
        is deferred to ITL-509.
        """
        ...


@runtime_checkable
class SideEffectNodeProtocol(Protocol):
    """Protocol for side-effect nodes in orchestrated execution."""

    node_type: str

    def execute(
        self,
        input_stream: "StreamProtocol",
        *,
        observer: "ExecutionObserverProtocol | None" = None,
        run_id: str | None = None,
    ) -> "list[tuple[TagProtocol, DataProtocol]]": ...

    async def async_execute(
        self,
        inputs: "Sequence[ReadableChannel[tuple[TagProtocol, DataProtocol]]]",
        output: "WritableChannel[tuple[TagProtocol, DataProtocol]]",
        *,
        observer: "ExecutionObserverProtocol | None" = None,
        run_id: str | None = None,
    ) -> None: ...

    def attach_databases(
        self,
        pipeline_database: "ArrowDatabaseProtocol | None" = None,
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


def is_side_effect_node(node: GraphNode) -> TypeGuard[SideEffectNodeProtocol]:
    """Check if a node is a side-effect node."""
    return node.node_type == "side_effect"
