"""Composite observer that delegates to multiple child observers.

Provides ``CompositeObserver``, a multiplexer that forwards every
``ExecutionObserverProtocol`` hook to N child observers.  This allows
combining observers (e.g. ``LoggingObserver`` + ``StatusObserver``)
without modifying the orchestrator or node code.

Example::

    from orcapod.pipeline.composite_observer import CompositeObserver
    from orcapod.pipeline.logging_observer import LoggingObserver
    from orcapod.pipeline.status_observer import StatusObserver
    from orcapod.databases import InMemoryArrowDatabase

    log_obs = LoggingObserver(log_database=InMemoryArrowDatabase())
    status_obs = StatusObserver(status_database=InMemoryArrowDatabase())
    observer = CompositeObserver(log_obs, status_obs)

    pipeline.run(orchestrator=SyncPipelineOrchestrator(observer=observer))
"""

from __future__ import annotations

from typing import Any

from orcapod.pipeline.observer import NoOpLogger
from orcapod.protocols.core_protocols import PacketProtocol, TagProtocol
from orcapod.types import SchemaLike

_NOOP_LOGGER = NoOpLogger()


class CompositeObserver:
    """Observer that delegates all hooks to multiple child observers.

    Args:
        *observers: Child observers to delegate to.  Each must satisfy
            ``ExecutionObserverProtocol``.
    """

    def __init__(self, *observers: Any) -> None:
        self._observers = observers

    def contextualize(
        self, node_hash: str, node_label: str
    ) -> CompositeObserver:
        """Return a composite of contextualized children."""
        return CompositeObserver(
            *(obs.contextualize(node_hash, node_label) for obs in self._observers)
        )

    def on_run_start(
        self,
        run_id: str,
        pipeline_uri: str = "",
    ) -> None:
        for obs in self._observers:
            obs.on_run_start(run_id, pipeline_uri=pipeline_uri)

    def on_run_end(self, run_id: str) -> None:
        for obs in self._observers:
            obs.on_run_end(run_id)

    def on_node_start(
        self, node_label: str, node_hash: str, pipeline_path: tuple[str, ...] = (), tag_schema: SchemaLike | None = None
    ) -> None:
        for obs in self._observers:
            obs.on_node_start(node_label, node_hash, pipeline_path=pipeline_path, tag_schema=tag_schema)

    def on_node_end(
        self, node_label: str, node_hash: str, pipeline_path: tuple[str, ...] = ()
    ) -> None:
        for obs in self._observers:
            obs.on_node_end(node_label, node_hash, pipeline_path=pipeline_path)

    def on_packet_start(
        self, node_label: str, tag: TagProtocol, packet: PacketProtocol
    ) -> None:
        for obs in self._observers:
            obs.on_packet_start(node_label, tag, packet)

    def on_packet_end(
        self,
        node_label: str,
        tag: TagProtocol,
        input_packet: PacketProtocol,
        output_packet: PacketProtocol | None,
        cached: bool,
    ) -> None:
        for obs in self._observers:
            obs.on_packet_end(node_label, tag, input_packet, output_packet, cached)

    def on_packet_crash(
        self, node_label: str, tag: TagProtocol, packet: PacketProtocol, error: Exception
    ) -> None:
        for obs in self._observers:
            obs.on_packet_crash(node_label, tag, packet, error)

    def create_packet_logger(
        self,
        tag: TagProtocol,
        packet: PacketProtocol,
        pipeline_path: tuple[str, ...] = (),
    ) -> Any:
        """Return the first non-no-op logger from children.

        Iterates through child observers and returns the logger from the
        first child that provides a real (non-no-op) implementation.
        Falls back to a no-op logger if all children return no-ops.
        """
        for obs in self._observers:
            pkt_logger = obs.create_packet_logger(tag, packet, pipeline_path=pipeline_path)
            if not isinstance(pkt_logger, NoOpLogger):
                return pkt_logger
        return _NOOP_LOGGER
