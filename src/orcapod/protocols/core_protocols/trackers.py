from contextlib import AbstractContextManager
from typing import Protocol, runtime_checkable

from orcapod.protocols.core_protocols.packet_function import PacketFunctionProtocol
from orcapod.protocols.core_protocols.pod import PodProtocol
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

    def record_pod_invocation(
        self,
        pod: PodProtocol,
        upstreams: tuple[StreamProtocol, ...] = (),
        label: str | None = None,
    ) -> None:
        """
        Record a pod invocation in the computational graph.

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

    def record_packet_function_invocation(
        self,
        packet_function: PacketFunctionProtocol,
        input_stream: StreamProtocol,
        label: str | None = None,
    ) -> None:
        """
        Record a packet function invocation in the computational graph.

        This method is called whenever a packet function is invoked. The tracker
        should record:
        - The packet function and its properties
        - The input stream that was used as input
        - Timing and performance information
        - Any relevant metadata

        Args:
            packet_function: The packet function that was invoked
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

    def record_pod_invocation(
        self,
        pod: PodProtocol,
        upstreams: tuple[StreamProtocol, ...] = (),
        label: str | None = None,
    ) -> None:
        """
        Record a stream in all active trackers.

        This method broadcasts the stream recording to all currently
        active and registered trackers. It provides a single point
        of entry for recording events, simplifying kernel implementations.

        Args:
            stream: The stream to record in all active trackers
        """
        ...

    def record_packet_function_invocation(
        self,
        packet_function: PacketFunctionProtocol,
        input_stream: StreamProtocol,
        label: str | None = None,
    ) -> None:
        """
        Record a packet function invocation in all active trackers.

        This method broadcasts the packet function recording to all currently
        active and registered trackers. It provides a single point
        of entry for recording events, simplifying kernel implementations.

        Args:
            packet_function: The packet function to record in all active trackers
        """
        ...

    def no_tracking(self) -> AbstractContextManager[None]: ...
