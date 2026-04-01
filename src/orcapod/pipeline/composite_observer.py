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

    db = InMemoryArrowDatabase()
    log_obs = LoggingObserver(log_database=db.at("my_pipeline", "_log"))
    status_obs = StatusObserver(status_database=db.at("my_pipeline", "_status"))
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
    """Fan-out observer that delegates all hooks to multiple child observers.

    Args:
        *observers: Child observers to delegate to.  Each must satisfy
            ``ExecutionObserverProtocol``.
    """

    def __init__(self, *observers: Any) -> None:
        self._observers = list(observers)

    def contextualize(self, *identity_path: str) -> "CompositeObserver":
        """Return a new CompositeObserver wrapping each child's contextualized version."""
        if not identity_path:
            raise ValueError(
                "CompositeObserver.contextualize() requires a non-empty identity_path."
            )
        return CompositeObserver(*[o.contextualize(*identity_path) for o in self._observers])

    def on_run_start(self, run_id: str, **kwargs: Any) -> None:
        for o in self._observers:
            o.on_run_start(run_id, **kwargs)

    def on_run_end(self, run_id: str) -> None:
        for o in self._observers:
            o.on_run_end(run_id)

    def on_node_start(
        self, node_label: str, node_hash: str, tag_schema: SchemaLike | None = None
    ) -> None:
        for o in self._observers:
            o.on_node_start(node_label, node_hash, tag_schema=tag_schema)

    def on_node_end(self, node_label: str, node_hash: str) -> None:
        for o in self._observers:
            o.on_node_end(node_label, node_hash)

    def on_packet_start(
        self, node_label: str, tag: TagProtocol, packet: PacketProtocol
    ) -> None:
        for o in self._observers:
            o.on_packet_start(node_label, tag, packet)

    def on_packet_end(
        self,
        node_label: str,
        tag: TagProtocol,
        input_packet: PacketProtocol,
        output_packet: PacketProtocol | None,
        cached: bool,
    ) -> None:
        for o in self._observers:
            o.on_packet_end(node_label, tag, input_packet, output_packet, cached)

    def on_packet_crash(
        self, node_label: str, tag: TagProtocol, packet: PacketProtocol, error: Exception
    ) -> None:
        for o in self._observers:
            o.on_packet_crash(node_label, tag, packet, error)

    def create_packet_logger(
        self,
        tag: TagProtocol,
        packet: PacketProtocol,
    ) -> Any:
        """Return the first non-no-op logger from children.

        Iterates through child observers and returns the logger from the
        first child that provides a real (non-no-op) implementation.
        Falls back to a no-op logger if all children return no-ops.
        """
        for o in self._observers:
            pkt_logger = o.create_packet_logger(tag, packet)
            if not isinstance(pkt_logger, NoOpLogger):
                return pkt_logger
        return _NOOP_LOGGER

    def to_config(self, db_registry: Any = None) -> dict:
        """Serialize this observer to a JSON-compatible config dict.

        Args:
            db_registry: Optional ``DatabaseRegistry`` instance.  When
                provided, each child's database config is registered and
                the emitted config uses the compact scoped reference format.

        Returns:
            A dict with ``"type": "composite"`` and an ``"observers"`` list.
        """
        return {
            "type": "composite",
            "observers": [o.to_config(db_registry=db_registry) for o in self._observers],
        }

    @classmethod
    def from_config(
        cls, config: dict, db_registry: dict | None = None
    ) -> "CompositeObserver":
        """Reconstruct a ``CompositeObserver`` from a config dict.

        Args:
            config: Dict as produced by ``to_config``.
            db_registry: Optional plain dict (output of
                ``DatabaseRegistry.to_dict()``) used to resolve child
                observer references.

        Returns:
            A new ``CompositeObserver`` instance.
        """
        from orcapod.pipeline.serialization import resolve_observer_from_config

        children = [resolve_observer_from_config(c, db_registry) for c in config["observers"]]
        return cls(*children)
