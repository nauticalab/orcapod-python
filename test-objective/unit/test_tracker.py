"""Specification-derived tests for tracker and pipeline.

Tests based on TrackerProtocol, TrackerManagerProtocol, and
Pipeline documented behavior.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.core.function_pod import FunctionPod
from orcapod.core.operators import Join
from orcapod.core.packet_function import PythonPacketFunction
from orcapod.core.sources import ArrowTableSource
from orcapod.core.tracker import BasicTrackerManager
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline import Pipeline


def _double(x: int) -> int:
    return x * 2


def _make_pipeline(
    tracker_manager: BasicTrackerManager | None = None,
) -> Pipeline:
    return Pipeline(
        name="test",
        pipeline_database=InMemoryArrowDatabase(),
        tracker_manager=tracker_manager,
        auto_compile=False,
    )


def _make_stream(n: int = 3) -> ArrowTableSource:
    table = pa.table(
        {
            "id": pa.array(list(range(n)), type=pa.int64()),
            "x": pa.array([i * 10 for i in range(n)], type=pa.int64()),
        }
    )
    return ArrowTableSource(table, tag_columns=["id"])


class TestBasicTrackerManager:
    """Per TrackerManagerProtocol: manages tracker registration, broadcasting,
    and no_tracking context."""

    def test_register_and_get_active_trackers(self):
        mgr = BasicTrackerManager()
        tracker = _make_pipeline(tracker_manager=mgr)
        tracker.set_active(True)
        active = mgr.get_active_trackers()
        assert tracker in active

    def test_deregister_removes_tracker(self):
        mgr = BasicTrackerManager()
        tracker = _make_pipeline(tracker_manager=mgr)
        mgr.deregister_tracker(tracker)
        assert tracker not in mgr.get_active_trackers()

    def test_no_tracking_context_suspends_recording(self):
        mgr = BasicTrackerManager()
        tracker = _make_pipeline(tracker_manager=mgr)
        tracker.set_active(True)
        with mgr.no_tracking():
            # Invocations inside this block should not be recorded
            active = mgr.get_active_trackers()
            assert len(active) == 0
        # After exiting, tracker should be active again
        active = mgr.get_active_trackers()
        assert tracker in active


class TestPipelineTracker:
    """Per design, Pipeline records pipeline structure as a directed graph."""

    def test_records_function_pod_invocation(self):
        mgr = BasicTrackerManager()
        tracker = _make_pipeline(tracker_manager=mgr)
        tracker.set_active(True)

        pf = PythonPacketFunction(_double, output_keys="result")
        pod = FunctionPod(packet_function=pf)
        stream = _make_stream()

        # Explicitly record the invocation
        tracker.record_function_pod_invocation(pod, stream)

        # The tracker should have recorded at least one node
        assert len(tracker.nodes) >= 1

    def test_reset_clears_state(self):
        mgr = BasicTrackerManager()
        tracker = _make_pipeline(tracker_manager=mgr)
        tracker.set_active(True)

        pf = PythonPacketFunction(_double, output_keys="result")
        pod = FunctionPod(packet_function=pf)
        stream = _make_stream()
        tracker.record_function_pod_invocation(pod, stream)

        tracker.reset()
        assert len(tracker.nodes) == 0

    def test_compile_builds_graph(self):
        mgr = BasicTrackerManager()
        tracker = _make_pipeline(tracker_manager=mgr)
        tracker.set_active(True)

        pf = PythonPacketFunction(_double, output_keys="result")
        pod = FunctionPod(packet_function=pf)
        stream = _make_stream()
        tracker.record_function_pod_invocation(pod, stream)

        tracker.compile()
        graph = tracker.graph
        assert graph is not None
        assert graph.number_of_nodes() >= 1
