from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Generator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

from orcapod.protocols import core_protocols as cp
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import networkx as nx

    from orcapod.core.nodes import GraphNode
else:
    nx = LazyModule("networkx")


class BasicTrackerManager:
    def __init__(self) -> None:
        self._active_trackers: list[cp.TrackerProtocol] = []
        self._active = True

    def set_active(self, active: bool = True) -> None:
        """Set the active state of the tracker manager."""
        self._active = active

    def register_tracker(self, tracker: cp.TrackerProtocol) -> None:
        """Register a new tracker in the system."""
        if tracker not in self._active_trackers:
            self._active_trackers.append(tracker)

    def deregister_tracker(self, tracker: cp.TrackerProtocol) -> None:
        """Remove a tracker from the system."""
        if tracker in self._active_trackers:
            self._active_trackers.remove(tracker)

    def get_active_trackers(self) -> list[cp.TrackerProtocol]:
        """Get the list of active trackers."""
        if not self._active:
            return []
        return [t for t in self._active_trackers if t.is_active()]

    def record_operator_pod_invocation(
        self,
        pod: cp.PodProtocol,
        upstreams: tuple[cp.StreamProtocol, ...] = (),
        label: str | None = None,
    ) -> None:
        """Record the invocation of a pod in the tracker."""
        for tracker in self.get_active_trackers():
            tracker.record_operator_pod_invocation(pod, upstreams, label=label)

    def record_function_pod_invocation(
        self,
        pod: cp.FunctionPodProtocol,
        input_stream: cp.StreamProtocol,
        label: str | None = None,
    ) -> None:
        """Record the invocation of a packet function to the tracker."""
        for tracker in self.get_active_trackers():
            tracker.record_function_pod_invocation(pod, input_stream, label=label)

    @contextmanager
    def no_tracking(self) -> Generator[None, Any, None]:
        original_state = self._active
        self.set_active(False)
        try:
            yield
        finally:
            self.set_active(original_state)


class AutoRegisteringContextBasedTracker(ABC):
    def __init__(
        self, tracker_manager: cp.TrackerManagerProtocol | None = None
    ) -> None:
        self._tracker_manager = tracker_manager or DEFAULT_TRACKER_MANAGER
        self._active = False

    def set_active(self, active: bool = True) -> None:
        if active:
            self._tracker_manager.register_tracker(self)
        else:
            self._tracker_manager.deregister_tracker(self)
        self._active = active

    def is_active(self) -> bool:
        return self._active

    @abstractmethod
    def record_operator_pod_invocation(
        self,
        pod: cp.OperatorPodProtocol,
        upstreams: tuple[cp.StreamProtocol, ...] = (),
        label: str | None = None,
    ) -> None: ...

    @abstractmethod
    def record_function_pod_invocation(
        self,
        pod: cp.FunctionPodProtocol,
        input_stream: cp.StreamProtocol,
        label: str | None = None,
    ) -> None: ...

    def __enter__(self):
        self.set_active(True)
        return self

    def __exit__(self, exc_type, exc_val, ext_tb):
        self.set_active(False)


# ---------------------------------------------------------------------------
# GraphTracker
# ---------------------------------------------------------------------------


class GraphTracker(AutoRegisteringContextBasedTracker):
    """A tracker that records invocations and builds a directed graph of
    typed graph nodes (FunctionNode, OperatorNode, SourceNode) connected
    by their upstream dependencies.

    Upstream resolution strategy:
    - stream.producer is None → root source → create/reuse SourceNode
    - stream.producer matched by id() → return the recorded node
    - stream.producer is a FunctionPod → look up via its packet_function
    - Unknown producer → treat as source (graceful fallback)
    """

    def __init__(
        self,
        tracker_manager: cp.TrackerManagerProtocol | None = None,
        **kwargs,
    ) -> None:
        super().__init__(tracker_manager=tracker_manager, **kwargs)
        self._node_lut: dict[str, GraphNode] = {}
        self._upstreams: dict[str, cp.StreamProtocol] = {}
        self._graph_edges: list[tuple[str, str]] = []
        self._hash_graph: nx.DiGraph = nx.DiGraph()

    def record_function_pod_invocation(
        self,
        pod: cp.FunctionPodProtocol,
        input_stream: cp.StreamProtocol,
        label: str | None = None,
    ) -> None:
        from orcapod.core.nodes import FunctionNode

        input_stream_hash = input_stream.content_hash().to_string()
        function_node = FunctionNode(
            function_pod=pod,
            input_stream=input_stream,
            label=label,
        )
        function_node_hash = function_node.content_hash().to_string()
        self._node_lut[function_node_hash] = function_node
        self._upstreams[input_stream_hash] = input_stream
        self._graph_edges.append((input_stream_hash, function_node_hash))
        self._hash_graph.add_edge(input_stream_hash, function_node_hash)
        if not self._hash_graph.nodes[function_node_hash].get("node_type"):
            self._hash_graph.nodes[function_node_hash]["node_type"] = "function"

    def record_operator_pod_invocation(
        self,
        pod: cp.OperatorPodProtocol,
        upstreams: tuple[cp.StreamProtocol, ...] = (),
        label: str | None = None,
    ) -> None:
        from orcapod.core.nodes import OperatorNode

        operator_node = OperatorNode(
            operator=pod,
            input_streams=upstreams,
            label=label,
        )
        operator_node_hash = operator_node.content_hash().to_string()
        self._node_lut[operator_node_hash] = operator_node
        upstream_hashes = [stream.content_hash().to_string() for stream in upstreams]
        for upstream_hash, upstream in zip(upstream_hashes, upstreams):
            self._upstreams[upstream_hash] = upstream
            self._graph_edges.append((upstream_hash, operator_node_hash))
            self._hash_graph.add_edge(upstream_hash, operator_node_hash)
        if not self._hash_graph.nodes[operator_node_hash].get("node_type"):
            self._hash_graph.nodes[operator_node_hash]["node_type"] = "operator"

    @property
    def nodes(self) -> list["GraphNode"]:
        return list(self._node_lut.values())

    @property
    def graph(self) -> "nx.DiGraph":
        """Directed graph of content-hash strings representing the accumulated
        pipeline structure.  Vertices are ``content_hash`` strings; node
        attributes include ``node_type`` ("source" / "function" / "operator")
        and, after ``compile()``, ``label`` and ``pipeline_hash``.

        The graph accumulates across multiple ``with`` blocks and is never
        cleared by ``reset()``.
        """
        return self._hash_graph

    def reset(self) -> None:
        """Clear session-scoped recorded state (node LUT, upstreams, edge list).

        Note: ``_hash_graph`` is intentionally *not* cleared — it accumulates
        the pipeline structure across ``with`` blocks.
        """
        self._node_lut.clear()
        self._upstreams.clear()
        self._graph_edges.clear()

    def compile(self):
        import networkx as nx

        from orcapod.core.nodes import SourceNode

        G = nx.DiGraph()
        for edge in self._graph_edges:
            G.add_edge(*edge)
        for node_hash in nx.topological_sort(G):
            if node_hash not in self._node_lut:
                stream = self._upstreams[node_hash]
                source_node = SourceNode(stream)
                self._node_lut[source_node.content_hash().to_string()] = source_node
                if node_hash in self._hash_graph and not self._hash_graph.nodes[
                    node_hash
                ].get("node_type"):
                    self._hash_graph.nodes[node_hash]["node_type"] = "source"
            else:
                node = self._node_lut[node_hash]
                upstream_as_nodes = [
                    self._node_lut[upstream.content_hash().to_string()]
                    for upstream in node.upstreams
                ]
                node.upstreams = tuple(upstream_as_nodes)


DEFAULT_TRACKER_MANAGER = BasicTrackerManager()
