from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any

from orcapod.protocols import core_protocols as cp


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


DEFAULT_TRACKER_MANAGER = BasicTrackerManager()
