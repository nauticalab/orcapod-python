from contextlib import AbstractContextManager
from typing import Protocol, runtime_checkable

from orcapod.protocols.core_protocols.function_pod import FunctionPodProtocol
from orcapod.protocols.core_protocols.operator_pod import OperatorPodProtocol
from orcapod.protocols.core_protocols.streams import StreamProtocol


@runtime_checkable
class TrackerProtocol(Protocol):
    """
    Records kernel invocations and stream creation for computational graph tracking.

    Trackers are responsible for maintaining the computational graph by recording
    relationships between kernels, streams, and invocations. They enable:
    - Lineage tracking and data provenance
    - Caching and memoization strategies
    - Debugging and error analysis
    - Performance monitoring and optimization
    - Reproducibility and auditing

    Multiple trackers can be active simultaneously, each serving different
    purposes (e.g., one for caching, another for debugging, another for
    monitoring). This allows for flexible and composable tracking strategies.

    Trackers can be selectively activated/deactivated to control overhead
    and focus on specific aspects of the computational graph.
    """

    def set_active(self, active: bool = True) -> None:
        """
        Set the active state of the tracker.

        When active, the tracker will record all kernel invocations and
        stream creations. When inactive, no recording occurs, reducing
        overhead for performance-critical sections.

        Args:
            active: True to activate recording, False to deactivate
        """
        ...

    def is_active(self) -> bool:
        """
        Check if the tracker is currently recording invocations.

        Returns:
            bool: True if tracker is active and recording, False otherwise
        """
        ...

    def record_operator_pod_invocation(
        self,
        pod: OperatorPodProtocol,
        upstreams: tuple[StreamProtocol, ...] = (),
        label: str | None = None,
    ) -> None:
        """
        Record an operator pod invocation in the computational graph.

        This method is called whenever a pod is invoked. The tracker
        should record:
        - The pod and its properties
        - The input streams that were used as input. If no streams are provided, the pod is considered a source pod.
        - Timing and performance information
        - Any relevant metadata

        Args:
            pod: The pod that was invoked
            upstreams: The input streams used for this invocation
        """
        ...

    def record_function_pod_invocation(
        self,
        pod: FunctionPodProtocol,
        input_stream: StreamProtocol,
        label: str | None = None,
    ) -> None:
        """
        Record a function pod invocation in the computational graph.

        This method is called whenever a function pod is invoked. The tracker
        should record:
        - The function pod and its properties
        - The input stream that was used as input. If no streams are provided, the pod is considered a source pod.
        - Timing and performance information
        - Any relevant metadata

        Args:
            pod: The function pod that was invoked
            input_stream: The input stream used for this invocation
        """
        ...


@runtime_checkable
class TrackerManagerProtocol(Protocol):
    """
    Manages multiple trackers and coordinates their activity.

    The TrackerManagerProtocol provides a centralized way to:
    - Register and manage multiple trackers
    - Coordinate recording across all active trackers
    - Provide a single interface for graph recording
    - Enable dynamic tracker registration/deregistration

    This design allows for:
    - Multiple concurrent tracking strategies
    - Pluggable tracking implementations
    - Easy testing and debugging (mock trackers)
    - Performance optimization (selective tracking)
    """

    def get_active_trackers(self) -> list[TrackerProtocol]:
        """
        Get all currently active trackers.

        Returns only trackers that are both registered and active,
        providing the list of trackers that will receive recording events.

        Returns:
            list[TrackerProtocol]: List of trackers that are currently recording
        """
        ...

    def register_tracker(self, tracker: TrackerProtocol) -> None:
        """
        Register a new tracker in the system.

        The tracker will be included in future recording operations
        if it is active. Registration is separate from activation
        to allow for dynamic control of tracking overhead.

        Args:
            tracker: The tracker to register
        """
        ...

    def deregister_tracker(self, tracker: TrackerProtocol) -> None:
        """
        Remove a tracker from the system.

        The tracker will no longer receive recording notifications
        even if it is still active. This is useful for:
        - Cleaning up temporary trackers
        - Removing failed or problematic trackers
        - Dynamic tracker management

        Args:
            tracker: The tracker to remove
        """
        ...

    def record_operator_pod_invocation(
        self,
        pod: OperatorPodProtocol,
        upstreams: tuple[StreamProtocol, ...] = (),
        label: str | None = None,
    ) -> None:
        """
        Record operator pod invocation in all active trackers.

        This method broadcasts the operator pod invocation recording to all currently
        active and registered trackers. It provides a single point
        of entry for recording events, simplifying kernel implementations.

        Args:
            pod: The operator pod to record in all active trackers
            upstreams: The upstream streams to record in all active trackers
            label: The label to associate with the recording
        """
        ...

    def record_function_pod_invocation(
        self,
        pod: FunctionPodProtocol,
        input_stream: StreamProtocol,
        label: str | None = None,
    ) -> None:
        """
        Record a function pod invocation in all active trackers.

        This method broadcasts the function pod invocation recording to all currently
        active and registered trackers. It provides a single point
        of entry for recording events, simplifying kernel implementations.

        Args:
            pod: The function pod to record in all active trackers
            input_stream: The input stream to record in all active trackers
            label: The label to associate with the recording
        """
        ...

    def no_tracking(self) -> AbstractContextManager[None]: ...
