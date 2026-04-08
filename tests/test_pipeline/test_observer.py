"""Tests for ExecutionObserverProtocol, NoOpObserver, and NoOpLogger."""

from __future__ import annotations

from orcapod.pipeline.observer import NoOpLogger, NoOpObserver, _NOOP_LOGGER
from orcapod.protocols.observability_protocols import (
    ExecutionObserverProtocol,
    PacketExecutionLoggerProtocol,
)


class TestNoOpObserver:
    """NoOpObserver satisfies the protocol and does nothing."""

    def test_satisfies_protocol(self):
        observer = NoOpObserver()
        assert isinstance(observer, ExecutionObserverProtocol)

    def test_on_run_start_noop(self):
        NoOpObserver().on_run_start("run-123")

    def test_on_run_end_noop(self):
        NoOpObserver().on_run_end("run-123")

    def test_on_node_start_noop(self):
        NoOpObserver().on_node_start("label", "hash")

    def test_on_node_end_noop(self):
        NoOpObserver().on_node_end("label", "hash")

    def test_on_packet_start_noop(self):
        NoOpObserver().on_packet_start("label", None, None)  # type: ignore[arg-type]

    def test_on_packet_end_noop(self):
        NoOpObserver().on_packet_end("label", None, None, None, cached=False)  # type: ignore[arg-type]

    def test_on_packet_crash_noop(self):
        NoOpObserver().on_packet_crash("label", None, None, RuntimeError("boom"))  # type: ignore[arg-type]

    def test_create_packet_logger_returns_noop(self):
        logger = NoOpObserver().create_packet_logger(None, None)  # type: ignore[arg-type]
        assert logger is _NOOP_LOGGER

    def test_create_packet_logger_satisfies_protocol(self):
        logger = NoOpObserver().create_packet_logger(None, None)  # type: ignore[arg-type]
        assert isinstance(logger, PacketExecutionLoggerProtocol)

    def test_contextualize_returns_self(self):
        obs = NoOpObserver()
        assert obs.contextualize("hash", "label") is obs


class TestNoOpLogger:
    """NoOpLogger satisfies the protocol and discards everything."""

    def test_satisfies_protocol(self):
        assert isinstance(NoOpLogger(), PacketExecutionLoggerProtocol)

    def test_record_noop(self):
        NoOpLogger().record(stdout="hello", success=True)

    def test_noop_logger_singleton_identity(self):
        # The singleton returned by create_packet_logger is always the same object.
        obs = NoOpObserver()
        l1 = obs.create_packet_logger(None, None)  # type: ignore[arg-type]
        l2 = obs.create_packet_logger(None, None)  # type: ignore[arg-type]
        assert l1 is l2
