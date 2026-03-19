"""No-op implementations of the observability protocols.

Provides ``NoOpLogger`` and ``NoOpObserver`` — the defaults used
when no observability is configured.  Every method is a zero-cost no-op.
"""

from __future__ import annotations

from typing import Any

from orcapod.protocols.core_protocols import PacketProtocol, TagProtocol
from orcapod.protocols.observability_protocols import (  # noqa: F401  (re-exported for convenience)
    ExecutionObserverProtocol,
    PacketExecutionLoggerProtocol,
)


# ---------------------------------------------------------------------------
# NoOpLogger
# ---------------------------------------------------------------------------


class NoOpLogger:
    """Logger that discards all captured output.

    Returned by ``NoOpObserver`` when no logging sink is configured.
    """

    def record(self, **kwargs: Any) -> None:
        pass


# Singleton — NoOpLogger carries no state so one instance is enough.
_NOOP_LOGGER = NoOpLogger()


# ---------------------------------------------------------------------------
# NoOpObserver
# ---------------------------------------------------------------------------


class NoOpObserver:
    """Observer that does nothing.

    Satisfies ``ExecutionObserverProtocol`` and is the default when no
    observability is configured.  ``create_packet_logger`` returns the
    shared ``_NOOP_LOGGER`` singleton.
    """

    def contextualize(
        self, node_hash: str, node_label: str
    ) -> NoOpObserver:
        return self

    def on_run_start(self, run_id: str) -> None:
        pass

    def on_run_end(self, run_id: str) -> None:
        pass

    def on_node_start(self, node_label: str, node_hash: str) -> None:
        pass

    def on_node_end(self, node_label: str, node_hash: str) -> None:
        pass

    def on_packet_start(
        self,
        node_label: str,
        tag: TagProtocol,
        packet: PacketProtocol,
    ) -> None:
        pass

    def on_packet_end(
        self,
        node_label: str,
        tag: TagProtocol,
        input_packet: PacketProtocol,
        output_packet: PacketProtocol | None,
        cached: bool,
    ) -> None:
        pass

    def on_packet_crash(
        self,
        node_label: str,
        tag: TagProtocol,
        packet: PacketProtocol,
        error: Exception,
    ) -> None:
        pass

    def create_packet_logger(
        self,
        tag: TagProtocol,
        packet: PacketProtocol,
        pipeline_path: tuple[str, ...] = (),
    ) -> NoOpLogger:
        return _NOOP_LOGGER
