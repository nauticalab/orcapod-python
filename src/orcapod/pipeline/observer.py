"""No-op implementations of the observability protocols.

Provides :class:`NoOpLogger` and :class:`NoOpObserver` — the defaults used
when no observability is configured.  Every method is a zero-cost no-op.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from orcapod.protocols.observability_protocols import (  # noqa: F401  (re-exported for convenience)
    ExecutionObserverProtocol,
    PacketExecutionLoggerProtocol,
)

if TYPE_CHECKING:
    from orcapod.core.nodes import GraphNode
    from orcapod.pipeline.logging_capture import CapturedLogs
    from orcapod.protocols.core_protocols import PacketProtocol, TagProtocol


# ---------------------------------------------------------------------------
# NoOpLogger
# ---------------------------------------------------------------------------


class NoOpLogger:
    """Logger that discards all captured output.

    Returned by :class:`NoOpObserver` when no logging sink is configured.
    """

    def record(self, captured: "CapturedLogs") -> None:
        pass


# Singleton — NoOpLogger carries no state so one instance is enough.
_NOOP_LOGGER = NoOpLogger()


# ---------------------------------------------------------------------------
# NoOpObserver
# ---------------------------------------------------------------------------


class NoOpObserver:
    """Observer that does nothing.

    Satisfies :class:`~orcapod.protocols.observability_protocols.ExecutionObserverProtocol`
    and is the default when no observability is configured.
    ``create_packet_logger`` returns the shared :data:`_NOOP_LOGGER` singleton.
    """

    def on_run_start(self, run_id: str) -> None:
        pass

    def on_run_end(self, run_id: str) -> None:
        pass

    def on_node_start(self, node: "GraphNode") -> None:
        pass

    def on_node_end(self, node: "GraphNode") -> None:
        pass

    def on_packet_start(
        self,
        node: "GraphNode",
        tag: "TagProtocol",
        packet: "PacketProtocol",
    ) -> None:
        pass

    def on_packet_end(
        self,
        node: "GraphNode",
        tag: "TagProtocol",
        input_packet: "PacketProtocol",
        output_packet: "PacketProtocol | None",
        cached: bool,
    ) -> None:
        pass

    def on_packet_crash(
        self,
        node: "GraphNode",
        tag: "TagProtocol",
        packet: "PacketProtocol",
        error: Exception,
    ) -> None:
        pass

    def create_packet_logger(
        self,
        node: "GraphNode",
        tag: "TagProtocol",
        packet: "PacketProtocol",
        pipeline_path: tuple[str, ...] = (),
    ) -> NoOpLogger:
        return _NOOP_LOGGER
