"""AbstractPipelineBase — shared recording mechanism for Pipeline and PipelineJob."""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from orcapod.core.tracker import AutoRegisteringContextBasedTracker
from orcapod.protocols import core_protocols as cp
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import networkx as nx
else:
    nx = LazyModule("networkx")

logger = logging.getLogger(__name__)


class AbstractPipelineBase(AutoRegisteringContextBasedTracker, ABC):
    """Shared recording mechanism and graph state for Pipeline and PipelineJob.

    Manages the ``with``-block recording phase: accumulating graph edges,
    node LUT entries, and upstream stream references. Subclasses specialise
    which node types are created (blueprint vs. job nodes) and compile them
    into executable graphs.

    Args:
        name: Pipeline name (string or tuple). Used to scope database paths.
        tracker_manager: Optional tracker manager override.
    """

    def __init__(
        self,
        name: str | tuple[str, ...] = "pipeline",
        tracker_manager: "cp.TrackerManagerProtocol | None" = None,
    ) -> None:
        """Initialize shared pipeline state.

        Args:
            name: Pipeline name (string or tuple). Used to scope database paths.
                Stored internally as a tuple.
            tracker_manager: Optional tracker manager override. Uses the default
                tracker manager if ``None``.
        """
        super().__init__(tracker_manager=tracker_manager)
        self._name: tuple[str, ...] = (name,) if isinstance(name, str) else tuple(name)
        self._node_lut: dict[str, Any] = {}
        self._upstreams: dict[str, Any] = {}
        self._graph_edges: list[tuple[str, str]] = []
        self._hash_graph: "nx.DiGraph" = nx.DiGraph()
        self._persistent_node_map: dict[str, Any] = {}
        self._nodes: dict[str, Any] = {}
        self._node_graph: "nx.DiGraph | None" = None
        self._compiled: bool = False

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def name(self) -> tuple[str, ...]:
        """Pipeline name tuple."""
        return self._name

    @property
    def graph(self) -> "nx.DiGraph":
        """Directed hash graph of accumulated pipeline structure."""
        return self._hash_graph

    @property
    def compiled_nodes(self) -> dict[str, Any]:
        """Copy of the compiled nodes dict (label to node)."""
        return self._nodes.copy()

    # ------------------------------------------------------------------
    # Recording helpers
    # ------------------------------------------------------------------

    def reset(self) -> None:
        """Clear session-scoped recorded state (node LUT, upstreams, edge list).

        Note:
            ``_hash_graph`` and ``_persistent_node_map`` are intentionally
            *not* cleared -- they accumulate across ``with`` blocks.
        """
        self._node_lut.clear()
        self._upstreams.clear()
        self._graph_edges.clear()

    def __exit__(self, exc_type=None, exc_value=None, traceback=None) -> None:
        """Exit the recording context, compiling if no exception occurred."""
        super().__exit__(exc_type, exc_value, traceback)
        if exc_type is None:
            self.compile()

    def __getattr__(self, item: str) -> Any:
        """Look up compiled nodes by label as attribute access."""
        if item.startswith("_"):
            raise AttributeError(item)
        # Use object.__getattribute__ to avoid recursion
        nodes = object.__getattribute__(self, "_nodes")
        if item in nodes:
            return nodes[item]
        raise AttributeError(
            f"{type(self).__name__!r} has no attribute {item!r}. "
            f"Available node labels: {sorted(nodes.keys())}"
        )

    # ------------------------------------------------------------------
    # Abstract -- specialised per subclass
    # ------------------------------------------------------------------

    @abstractmethod
    def record_function_pod_invocation(
        self,
        pod: "cp.FunctionPodProtocol",
        input_stream: "cp.StreamProtocol",
        label: str | None = None,
    ) -> None:
        """Record a function pod invocation into the graph.

        Args:
            pod: The function pod being invoked.
            input_stream: The upstream stream.
            label: Optional display label for the resulting node.
        """
        ...

    @abstractmethod
    def record_operator_pod_invocation(
        self,
        pod: "cp.OperatorPodProtocol",
        upstreams: "tuple[cp.StreamProtocol, ...]" = (),
        label: str | None = None,
    ) -> None:
        """Record an operator pod invocation into the graph.

        Args:
            pod: The operator pod being invoked.
            upstreams: Upstream streams for this operator.
            label: Optional display label for the resulting node.
        """
        ...

    @abstractmethod
    def compile(self) -> None:
        """Compile recorded invocations into a frozen DAG.

        Transforms accumulated ``_node_lut``, ``_upstreams``, and
        ``_graph_edges`` into ``_persistent_node_map`` and ``_nodes``.
        Sets ``_compiled = True`` on completion.
        """
        ...
