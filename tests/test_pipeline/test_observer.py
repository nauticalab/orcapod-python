"""Tests for ExecutionObserver protocol and NoOpObserver."""

from __future__ import annotations

from orcapod.pipeline.observer import ExecutionObserver, NoOpObserver


class TestNoOpObserver:
    """NoOpObserver satisfies the protocol and does nothing."""

    def test_satisfies_protocol(self):
        observer = NoOpObserver()
        assert isinstance(observer, ExecutionObserver)

    def test_on_node_start_noop(self):
        observer = NoOpObserver()
        observer.on_node_start(None)  # type: ignore[arg-type]

    def test_on_node_end_noop(self):
        observer = NoOpObserver()
        observer.on_node_end(None)  # type: ignore[arg-type]

    def test_on_packet_start_noop(self):
        observer = NoOpObserver()
        observer.on_packet_start(None, None, None)  # type: ignore[arg-type]

    def test_on_packet_end_noop(self):
        observer = NoOpObserver()
        observer.on_packet_end(None, None, None, None, cached=False)  # type: ignore[arg-type]
