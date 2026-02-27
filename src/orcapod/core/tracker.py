from __future__ import annotations

from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Generator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

from orcapod.core.base import TraceableBase
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
        Record the output stream of a pod invocation in the tracker.
        This is used to track the computational graph and the invocations of pods.
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
        Record the output stream of a pod invocation in the tracker.
        This is used to track the computational graph and the invocations of pods.
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


class Invocation(TraceableBase):
    def __init__(
        self,
        kernel: cp.PodProtocol,
        upstreams: tuple[cp.StreamProtocol, ...] = (),
        label: str | None = None,
    ) -> None:
        """
        Represents an invocation of a kernel with its upstream streams.
        This is used to track the computational graph and the invocations of kernels.
        """
        super().__init__(label=label)
        self.kernel = kernel
        self.upstreams = upstreams

    def parents(self) -> tuple["Invocation", ...]:
        parent_invoctions = []
        for stream in self.upstreams:
            if stream.source is not None:
                parent_invoctions.append(Invocation(stream.source, stream.upstreams))
            else:
                # import JIT to avoid circular imports
                from orcapod.core.sources.base import StreamSource

                source = StreamSource(stream)
                parent_invoctions.append(Invocation(source))

        return tuple(parent_invoctions)

    def computed_label(self) -> str | None:
        """
        Compute a label for this invocation based on its kernel and upstreams.
        If label is not explicitly set for this invocation and computed_label returns a valid value,
        it will be used as label of this invocation.
        """
        return self.kernel.label

    def identity_structure(self) -> Any:
        """
        Return a structure that represents the identity of this invocation.
        This is used to uniquely identify the invocation in the tracker.
        """
        # if no upstreams, then we want to identify the source directly
        if not self.upstreams:
            return self.kernel.identity_structure()
        return self.kernel.identity_structure()

    def __repr__(self) -> str:
        return f"Invocation(kernel={self.kernel}, upstreams={self.upstreams}, label={self.label})"


class GraphTracker(AutoRegisteringContextBasedTracker):
    """
    A tracker that records the invocations of operations and generates a graph
    of the invocations and their dependencies.
    """

    # Thread-local storage to track active trackers

    def __init__(
        self,
        tracker_manager: cp.TrackerManagerProtocol | None = None,
        **kwargs,
    ) -> None:
        super().__init__(tracker_manager=tracker_manager)

        # Dictionary to map kernels to the streams they have invoked
        # This is used to track the computational graph and the invocations of kernels
        self.kernel_invocations: set[Invocation] = set()
        self.invocation_to_pod_lut: dict[Invocation, cp.PodProtocol] = {}
        self.invocation_to_source_lut: dict[Invocation, cp.SourcePodProtocol] = {}

    def _record_kernel_and_get_invocation(
        self,
        kernel: cp.PodProtocol,
        upstreams: tuple[cp.StreamProtocol, ...],
        label: str | None = None,
    ) -> Invocation:
        invocation = Invocation(kernel, upstreams, label=label)
        self.kernel_invocations.add(invocation)
        return invocation

    def record_kernel_invocation(
        self,
        kernel: cp.PodProtocol,
        upstreams: tuple[cp.StreamProtocol, ...],
        label: str | None = None,
    ) -> None:
        """
        Record the output stream of a kernel invocation in the tracker.
        This is used to track the computational graph and the invocations of kernels.
        """
        self._record_kernel_and_get_invocation(kernel, upstreams, label)

    def record_source_invocation(
        self, source: cp.SourcePodProtocol, label: str | None = None
    ) -> None:
        """
        Record the output stream of a source invocation in the tracker.
        """
        invocation = self._record_kernel_and_get_invocation(source, (), label)
        self.invocation_to_source_lut[invocation] = source

    def record_pod_invocation(
        self,
        pod: cp.PodProtocol,
        upstreams: tuple[cp.StreamProtocol, ...] = (),
        label: str | None = None,
    ) -> None:
        """
        Record the output stream of a pod invocation in the tracker.
        """
        invocation = self._record_kernel_and_get_invocation(pod, upstreams, label)
        self.invocation_to_pod_lut[invocation] = pod

    def reset(self) -> dict[cp.PodProtocol, list[cp.StreamProtocol]]:
        """
        Reset the tracker and return the recorded invocations.
        """
        recorded_streams = self.kernel_to_invoked_stream_lut
        self.kernel_to_invoked_stream_lut = defaultdict(list)
        return recorded_streams

    def generate_graph(self) -> "nx.DiGraph":
        import networkx as nx

        G = nx.DiGraph()

        # Add edges for each invocation
        for invocation in self.kernel_invocations:
            G.add_node(invocation)
            for upstream_invocation in invocation.parents():
                G.add_edge(upstream_invocation, invocation)
        return G


DEFAULT_TRACKER_MANAGER = BasicTrackerManager()
