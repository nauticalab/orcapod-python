from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Generator, Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, TypeAlias

from orcapod import contexts
from orcapod.config import Config
from orcapod.core.streams import StreamBase
from orcapod.protocols import core_protocols as cp
from orcapod.types import ColumnConfig, Schema

if TYPE_CHECKING:
    import pyarrow as pa

    from orcapod.core.function_pod import FunctionNode
    from orcapod.core.operator_node import OperatorNode


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

    def record_operator_pod_invocation(
        self,
        pod: cp.PodProtocol,
        upstreams: tuple[cp.StreamProtocol, ...] = (),
        label: str | None = None,
    ) -> None:
        """
        Record the invocation of a pod in the tracker.
        """
        for tracker in self.get_active_trackers():
            tracker.record_operator_pod_invocation(pod, upstreams, label=label)

    def record_function_pod_invocation(
        self,
        pod: cp.FunctionPodProtocol,
        input_stream: cp.StreamProtocol,
        label: str | None = None,
    ) -> None:
        """
        Record the invocation of a packet function to the tracker.
        """
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
# SourceNode
# ---------------------------------------------------------------------------


class SourceNode(StreamBase):
    """Represents a root source stream in the computation graph."""

    node_type = "source"

    def __init__(
        self,
        stream: cp.StreamProtocol,
        label: str | None = None,
        data_context: str | contexts.DataContext | None = None,
        config: Config | None = None,
    ):
        super().__init__(
            label=label,
            data_context=data_context,
            config=config,
        )
        self.stream = stream

    def computed_label(self) -> str | None:
        return self.stream.label

    def identity_structure(self) -> Any:
        # TODO: revisit this logic for case where stream is not a root source
        return self.stream.identity_structure()

    def pipeline_identity_structure(self) -> Any:
        return self.stream.pipeline_identity_structure()

    def keys(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        return self.stream.keys(columns=columns, all_info=all_info)

    def output_schema(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        return self.stream.output_schema(columns=columns, all_info=all_info)

    @property
    def producer(self) -> None:
        return None

    @property
    def upstreams(self) -> tuple[cp.StreamProtocol, ...]:
        return ()

    @upstreams.setter
    def upstreams(self, value: tuple[cp.StreamProtocol, ...]) -> None:
        if len(value) != 0:
            raise ValueError("SourceNode upstreams must be empty")

    def __repr__(self) -> str:
        return f"SourceNode(stream={self.stream!r}, label={self.label!r})"

    def as_table(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> "pa.Table":
        return self.stream.as_table(columns=columns, all_info=all_info)

    def iter_packets(self) -> Iterator[tuple[cp.TagProtocol, cp.PacketProtocol]]:
        return self.stream.iter_packets()


GraphNode: TypeAlias = "SourceNode | FunctionNode | OperatorNode"
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
        super().__init__(tracker_manager=tracker_manager, **kwargs)
        # hash(producer) → node  (Python object identity; safe within the tracker's lifetime)
        # ordered list of all recorded nodes
        self._node_lut: dict[str, GraphNode] = {}
        self._upstreams: dict[str, cp.StreamProtocol] = {}

        # a list to keep track of all graph edges, from upstream content hash, downstream content hash
        self._graph_edges: list[tuple[str, str]] = []

    def record_function_pod_invocation(
        self,
        pod: cp.FunctionPodProtocol,
        input_stream: cp.StreamProtocol,
        label: str | None = None,
    ) -> None:
        from orcapod.core.function_pod import FunctionNode

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

    def record_operator_pod_invocation(
        self,
        pod: cp.OperatorPodProtocol,
        upstreams: tuple[cp.StreamProtocol, ...] = (),
        label: str | None = None,
    ) -> None:
        from orcapod.core.operator_node import OperatorNode

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

    @property
    def nodes(self) -> list[GraphNode]:
        return list(self._node_lut.values())

    def reset(self) -> None:
        """Clear all recorded state."""
        self._node_lut.clear()
        self._upstreams.clear()
        self._graph_edges.clear()

    def compile(self):
        import networkx as nx

        # create graph from graph_edges
        # topologically sort and visit hash str in the graph
        #
        #
        G = nx.DiGraph()
        for edge in self._graph_edges:
            G.add_edge(*edge)
        for node_hash in nx.topological_sort(G):
            if node_hash not in self._node_lut:
                stream = self._upstreams[node_hash]
                source_node = SourceNode(stream)
                self._node_lut[source_node.content_hash().to_string()] = source_node
            else:
                # make sure all upstreams of a node is another node
                node = self._node_lut[node_hash]
                upstream_as_nodes = [
                    self._node_lut[upstream.content_hash().to_string()]
                    for upstream in node.upstreams
                ]
                node.upstreams = tuple(upstream_as_nodes)


DEFAULT_TRACKER_MANAGER = BasicTrackerManager()
