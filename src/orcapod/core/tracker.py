from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Generator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

from orcapod.core.function_pod import FunctionNode
from orcapod.core.operator_node import OperatorNode
from orcapod.protocols import core_protocols as cp

if TYPE_CHECKING:
    import networkx as nx


class BasicTrackerManager:
    def __init__(self) -> None:
        self._active_trackers: list[cp.TrackerProtocol] = []
        self._active = True

    def set_active(self, active: bool = True) -> None:
        """
        Set the active state of the tracker manager.
        This is used to enable or disable the tracker manager.
        """
        self._active = active

    def register_tracker(self, tracker: cp.TrackerProtocol) -> None:
        """
        Register a new tracker in the system.
        This is used to add a new tracker to the list of active trackers.
        """
        if tracker not in self._active_trackers:
            self._active_trackers.append(tracker)

    def deregister_tracker(self, tracker: cp.TrackerProtocol) -> None:
        """
        Remove a tracker from the system.
        This is used to deactivate a tracker and remove it from the list of active trackers.
        """
        if tracker in self._active_trackers:
            self._active_trackers.remove(tracker)

    def get_active_trackers(self) -> list[cp.TrackerProtocol]:
        """
        Get the list of active trackers.
        This is used to retrieve the currently active trackers in the system.
        """
        if not self._active:
            return []
        # Filter out inactive trackers
        # This is to ensure that we only return trackers that are currently active
        return [t for t in self._active_trackers if t.is_active()]

    def record_pod_invocation(
        self,
        pod: cp.PodProtocol,
        upstreams: tuple[cp.StreamProtocol, ...] = (),
        label: str | None = None,
    ) -> None:
        """
        Record the invocation of a pod in the tracker.
        """
        for tracker in self.get_active_trackers():
            tracker.record_pod_invocation(pod, upstreams, label=label)

    def record_packet_function_invocation(
        self,
        packet_function: cp.PacketFunctionProtocol,
        input_stream: cp.StreamProtocol,
        label: str | None = None,
    ) -> None:
        """
        Record the invocation of a packet function to the tracker.
        """
        for tracker in self.get_active_trackers():
            tracker.record_packet_function_invocation(
                packet_function, input_stream, label=label
            )

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
    def record_pod_invocation(
        self,
        pod: cp.PodProtocol,
        upstreams: tuple[cp.StreamProtocol, ...] = (),
        label: str | None = None,
    ) -> None: ...

    @abstractmethod
    def record_packet_function_invocation(
        self,
        packet_function: cp.PacketFunctionProtocol,
        input_stream: cp.StreamProtocol,
        label: str | None = None,
    ) -> None: ...

    def __enter__(self):
        self.set_active(True)
        return self

    def __exit__(self, exc_type, exc_val, ext_tb):
        self.set_active(False)


# ---------------------------------------------------------------------------
# SourceNode
# ---------------------------------------------------------------------------


class SourceNode:
    """Represents a root source stream in the computation graph."""

    node_type = "source"

    def __init__(self, stream: cp.StreamProtocol, label: str | None = None) -> None:
        self.stream = stream
        self.label = label or getattr(stream, "label", None)

    @property
    def producer(self) -> None:
        return None

    @property
    def upstreams(self) -> tuple[()]:
        return ()

    def __repr__(self) -> str:
        return f"SourceNode(stream={self.stream!r}, label={self.label!r})"


GraphNode = SourceNode | FunctionNode | OperatorNode
# Full type once FunctionNode/OperatorNode are imported:
#   GraphNode = SourceNode | FunctionNode | OperatorNode
# Kept as Union[SourceNode, Any] to avoid circular imports.


# ---------------------------------------------------------------------------
# GraphTracker
# ---------------------------------------------------------------------------


class GraphTracker(AutoRegisteringContextBasedTracker):
    """
    A tracker that records invocations and builds a directed graph of
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
        super().__init__(tracker_manager=tracker_manager)
        # id(producer) → node  (Python object identity; safe within the tracker's lifetime)
        self._producer_to_node: dict[int, GraphNode] = {}
        # id(stream) → SourceNode  (dedup root sources)
        self._source_to_node: dict[int, SourceNode] = {}
        # ordered list of all recorded nodes
        self._nodes: list[GraphNode] = []

    def _get_or_create_source_node(self, stream: cp.StreamProtocol) -> SourceNode:
        sid = id(stream)
        if sid not in self._source_to_node:
            node = SourceNode(stream=stream, label=getattr(stream, "label", None))
            self._source_to_node[sid] = node
            self._nodes.append(node)
        return self._source_to_node[sid]

    def _resolve_upstream_node(self, stream: cp.StreamProtocol) -> GraphNode:
        if stream.producer is None:
            return self._get_or_create_source_node(stream)
        # Operator match: stream.producer is the pod itself
        if id(stream.producer) in self._producer_to_node:
            return self._producer_to_node[id(stream.producer)]
        # Function pod match: stream.producer is a FunctionPod,
        # look up via its packet_function
        pf = getattr(stream.producer, "packet_function", None)
        if pf is not None and id(pf) in self._producer_to_node:
            return self._producer_to_node[id(pf)]
        # Unknown producer — treat as source
        return self._get_or_create_source_node(stream)

    def _resolve_upstream_nodes(
        self, upstreams: tuple[cp.StreamProtocol, ...]
    ) -> tuple[GraphNode, ...]:
        return tuple(self._resolve_upstream_node(s) for s in upstreams)

    def record_packet_function_invocation(
        self,
        packet_function: cp.PacketFunctionProtocol,
        input_stream: cp.StreamProtocol,
        label: str | None = None,
    ) -> None:
        from orcapod.core.function_pod import FunctionNode

        upstream_nodes = self._resolve_upstream_nodes((input_stream,))
        node = FunctionNode(
            packet_function=packet_function,
            input_stream=input_stream,
            label=label,
        )
        node._upstream_graph_nodes = upstream_nodes
        self._producer_to_node[id(packet_function)] = node
        self._nodes.append(node)

    def record_pod_invocation(
        self,
        pod: cp.PodProtocol,
        upstreams: tuple[cp.StreamProtocol, ...] = (),
        label: str | None = None,
    ) -> None:
        from orcapod.core.operator_node import OperatorNode

        upstream_nodes = self._resolve_upstream_nodes(upstreams)
        node = OperatorNode(
            operator=pod,
            input_streams=upstreams,
            label=label,
        )
        node._upstream_graph_nodes = upstream_nodes
        self._producer_to_node[id(pod)] = node
        self._nodes.append(node)

    @property
    def nodes(self) -> list[GraphNode]:
        return list(self._nodes)

    def reset(self) -> None:
        """Clear all recorded state."""
        self._producer_to_node.clear()
        self._source_to_node.clear()
        self._nodes.clear()

    def generate_graph(self) -> "nx.DiGraph":
        import networkx as nx

        G = nx.DiGraph()
        for node in self._nodes:
            G.add_node(node)
            upstream_nodes = getattr(node, "_upstream_graph_nodes", None)
            if upstream_nodes is not None:
                for upstream in upstream_nodes:
                    G.add_edge(upstream, node)
        return G


DEFAULT_TRACKER_MANAGER = BasicTrackerManager()
