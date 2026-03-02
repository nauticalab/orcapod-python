"""
Tests for the tracker module covering:

- SourceNode: construction, properties, repr
- BasicTrackerManager: register/deregister, active state, no_tracking context
- AutoRegisteringContextBasedTracker: context manager lifecycle
- GraphTracker:
    - record_packet_function_invocation → creates FunctionNode
    - record_pod_invocation → creates OperatorNode
    - Upstream resolution: source, known producer, packet_function fallback, unknown fallback
    - Source deduplication
    - generate_graph() → correct nx.DiGraph
    - reset() clears all state
    - nodes property
- End-to-end: FunctionPod.process() and StaticOutputPod.process() inside tracker context
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.core.function_pod import FunctionNode, FunctionPod
from orcapod.core.operator_node import OperatorNode
from orcapod.core.operators import Join, SelectTagColumns
from orcapod.core.packet_function import PythonPacketFunction
from orcapod.core.streams import ArrowTableStream
from orcapod.core.tracker import (
    BasicTrackerManager,
    GraphTracker,
    SourceNode,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _double(x: int) -> int:
    return x * 2


def _inc_result(result: int) -> int:
    return result + 1


def _make_stream(n: int = 3) -> ArrowTableStream:
    """Simple stream with tag=id, packet=x."""
    table = pa.table(
        {
            "id": pa.array(list(range(n)), type=pa.int64()),
            "x": pa.array(list(range(n)), type=pa.int64()),
        }
    )
    return ArrowTableStream(table, tag_columns=["id"])


def _make_two_col_stream(n: int = 3) -> ArrowTableStream:
    """Stream with tag=id, packet={a, b} for binary operator tests."""
    table = pa.table(
        {
            "id": pa.array(list(range(n)), type=pa.int64()),
            "a": pa.array(list(range(n)), type=pa.int64()),
            "b": pa.array([i * 10 for i in range(n)], type=pa.int64()),
        }
    )
    return ArrowTableStream(table, tag_columns=["id"])


# ---------------------------------------------------------------------------
# SourceNode
# ---------------------------------------------------------------------------


class TestSourceNode:
    def test_construction(self):
        stream = _make_stream()
        node = SourceNode(stream=stream)
        assert node.stream is stream
        assert node.node_type == "source"
        assert node.producer is None
        assert node.upstreams == ()

    def test_label_from_argument(self):
        stream = _make_stream()
        node = SourceNode(stream=stream, label="my_source")
        assert node.label == "my_source"

    def test_label_from_stream(self):
        stream = _make_stream()
        stream._label = "stream_label"
        node = SourceNode(stream=stream)
        assert node.label == "stream_label"

    def test_label_argument_overrides_stream(self):
        stream = _make_stream()
        stream._label = "stream_label"
        node = SourceNode(stream=stream, label="explicit")
        assert node.label == "explicit"

    def test_repr(self):
        stream = _make_stream()
        node = SourceNode(stream=stream, label="test")
        r = repr(node)
        assert "SourceNode" in r
        assert "test" in r


# ---------------------------------------------------------------------------
# BasicTrackerManager
# ---------------------------------------------------------------------------


class TestBasicTrackerManager:
    def test_initial_state(self):
        mgr = BasicTrackerManager()
        assert mgr._active is True
        assert mgr.get_active_trackers() == []

    def test_register_and_deregister(self):
        mgr = BasicTrackerManager()
        tracker = GraphTracker(tracker_manager=mgr)
        tracker.set_active(True)
        assert tracker in mgr.get_active_trackers()
        tracker.set_active(False)
        assert tracker not in mgr.get_active_trackers()

    def test_duplicate_register_ignored(self):
        mgr = BasicTrackerManager()
        tracker = GraphTracker(tracker_manager=mgr)
        mgr.register_tracker(tracker)
        mgr.register_tracker(tracker)
        assert mgr._active_trackers.count(tracker) == 1

    def test_deregister_nonexistent_is_noop(self):
        mgr = BasicTrackerManager()
        tracker = GraphTracker(tracker_manager=mgr)
        # Should not raise
        mgr.deregister_tracker(tracker)

    def test_set_active_false_returns_empty(self):
        mgr = BasicTrackerManager()
        tracker = GraphTracker(tracker_manager=mgr)
        tracker.set_active(True)
        mgr.set_active(False)
        assert mgr.get_active_trackers() == []
        mgr.set_active(True)
        assert tracker in mgr.get_active_trackers()

    def test_no_tracking_context(self):
        mgr = BasicTrackerManager()
        tracker = GraphTracker(tracker_manager=mgr)
        tracker.set_active(True)

        with mgr.no_tracking():
            assert mgr.get_active_trackers() == []
        # Restored
        assert tracker in mgr.get_active_trackers()

    def test_no_tracking_restores_original_state(self):
        """no_tracking restores the original active state even if it was False."""
        mgr = BasicTrackerManager()
        mgr.set_active(False)
        with mgr.no_tracking():
            pass
        assert mgr._active is False


# ---------------------------------------------------------------------------
# GraphTracker — context manager lifecycle
# ---------------------------------------------------------------------------


class TestGraphTrackerLifecycle:
    def test_context_manager_activates_and_deactivates(self):
        mgr = BasicTrackerManager()
        tracker = GraphTracker(tracker_manager=mgr)
        assert not tracker.is_active()

        with tracker:
            assert tracker.is_active()
            assert tracker in mgr.get_active_trackers()

        assert not tracker.is_active()
        assert tracker not in mgr.get_active_trackers()

    def test_inactive_tracker_not_in_active_list(self):
        mgr = BasicTrackerManager()
        tracker = GraphTracker(tracker_manager=mgr)
        tracker.set_active(True)
        tracker.set_active(False)
        assert mgr.get_active_trackers() == []


# ---------------------------------------------------------------------------
# GraphTracker — recording and upstream resolution
# ---------------------------------------------------------------------------


class TestGraphTrackerRecording:
    def test_record_packet_function_creates_function_node(self):
        pf = PythonPacketFunction(_double, output_keys="result")
        stream = _make_stream()
        mgr = BasicTrackerManager()

        with GraphTracker(tracker_manager=mgr) as tracker:
            tracker.record_packet_function_invocation(pf, stream, label="dbl")

        assert len(tracker.nodes) == 2  # SourceNode + FunctionNode
        source_node = tracker.nodes[0]
        fn_node = tracker.nodes[1]

        assert isinstance(source_node, SourceNode)
        assert source_node.stream is stream

        assert isinstance(fn_node, FunctionNode)
        assert fn_node.node_type == "function"
        assert fn_node._upstream_graph_nodes == (source_node,)

    def test_record_pod_invocation_creates_operator_node(self):
        stream = _make_stream()
        op = SelectTagColumns(columns=["id"])
        mgr = BasicTrackerManager()

        with GraphTracker(tracker_manager=mgr) as tracker:
            tracker.record_pod_invocation(op, upstreams=(stream,), label="select")

        assert len(tracker.nodes) == 2  # SourceNode + OperatorNode
        source_node = tracker.nodes[0]
        op_node = tracker.nodes[1]

        assert isinstance(source_node, SourceNode)
        assert isinstance(op_node, OperatorNode)
        assert op_node.node_type == "operator"
        assert op_node._upstream_graph_nodes == (source_node,)

    def test_source_deduplication(self):
        """Same stream used in two recordings → single SourceNode."""
        pf1 = PythonPacketFunction(_double, output_keys="result")
        pf2 = PythonPacketFunction(_double, output_keys="out")
        stream = _make_stream()
        mgr = BasicTrackerManager()

        with GraphTracker(tracker_manager=mgr) as tracker:
            tracker.record_packet_function_invocation(pf1, stream)
            # Create a FunctionNode output stream to chain
            fn_node = tracker.nodes[1]
            # Use the same source stream for another function
            tracker.record_packet_function_invocation(pf2, stream)

        # Should have: 1 SourceNode, 2 FunctionNodes
        source_nodes = [n for n in tracker.nodes if isinstance(n, SourceNode)]
        fn_nodes = [n for n in tracker.nodes if isinstance(n, FunctionNode)]
        assert len(source_nodes) == 1
        assert len(fn_nodes) == 2
        # Both FunctionNodes share the same SourceNode upstream
        assert (
            fn_nodes[0]._upstream_graph_nodes[0] is fn_nodes[1]._upstream_graph_nodes[0]
        )

    def test_operator_upstream_resolution_via_producer(self):
        """When stream.producer matches a recorded pod, resolve to that node."""
        stream = _make_stream()
        op = SelectTagColumns(columns=["id"])
        mgr = BasicTrackerManager()

        with GraphTracker(tracker_manager=mgr) as tracker:
            # Simulate: operator processes the stream, output stream has producer=op
            tracker.record_pod_invocation(op, upstreams=(stream,))
            op_node = tracker.nodes[1]

            # Create an output stream whose producer is the operator
            output = op.process(stream)  # DynamicPodStream with producer=op

            # Now record another operator reading from output
            op2 = SelectTagColumns(columns=["id"])
            tracker.record_pod_invocation(op2, upstreams=(output,))

        last_node = tracker.nodes[-1]
        assert isinstance(last_node, OperatorNode)
        # The upstream should be the first OperatorNode, not a SourceNode
        assert last_node._upstream_graph_nodes == (op_node,)

    def test_function_pod_upstream_resolution_via_packet_function(self):
        """When stream.producer is a FunctionPod, resolve via packet_function."""
        pf = PythonPacketFunction(_double, output_keys="result")
        stream = _make_stream()
        pod = FunctionPod(packet_function=pf)
        mgr = BasicTrackerManager()

        with GraphTracker(tracker_manager=mgr) as tracker:
            # Record the packet function invocation (as FunctionPod.process does)
            tracker.record_packet_function_invocation(pf, stream)
            fn_node = tracker.nodes[1]

            # FunctionPod.process creates a FunctionPodStream with producer=pod
            # and pod.packet_function == pf
            output = pod.process(stream)

            # Now record a second function reading from the FunctionPodStream
            pf2 = PythonPacketFunction(_inc_result, output_keys="out")
            tracker.record_packet_function_invocation(pf2, output)

        last_fn = tracker.nodes[-1]
        assert isinstance(last_fn, FunctionNode)
        # Upstream should resolve to the first FunctionNode via packet_function
        assert last_fn._upstream_graph_nodes == (fn_node,)

    def test_unknown_producer_treated_as_source(self):
        """Stream with an unknown producer is treated as a source."""

        class FakeProducer:
            pass

        class FakeStream:
            producer = FakeProducer()
            label = "fake"

            def output_schema(self, **kwargs):
                from orcapod.types import Schema

                return Schema({"id": int}), Schema({"x": int})

            def keys(self, **kwargs):
                return ("id",), ("x",)

            def iter_packets(self):
                return iter([])

        fake = FakeStream()
        pf = PythonPacketFunction(_double, output_keys="result")
        mgr = BasicTrackerManager()

        with GraphTracker(tracker_manager=mgr) as tracker:
            tracker.record_packet_function_invocation(pf, fake)

        # Unknown producer → treated as source
        assert len(tracker.nodes) == 2
        assert isinstance(tracker.nodes[0], SourceNode)
        assert tracker.nodes[0].stream is fake

    def test_multi_input_operator(self):
        """Join with two input streams → 2 SourceNodes + 1 OperatorNode."""
        stream_a = _make_stream()
        table_b = pa.table(
            {
                "id": pa.array([0, 1, 2], type=pa.int64()),
                "y": pa.array([10, 20, 30], type=pa.int64()),
            }
        )
        stream_b = ArrowTableStream(table_b, tag_columns=["id"])
        op = Join()
        mgr = BasicTrackerManager()

        with GraphTracker(tracker_manager=mgr) as tracker:
            tracker.record_pod_invocation(op, upstreams=(stream_a, stream_b))

        source_nodes = [n for n in tracker.nodes if isinstance(n, SourceNode)]
        op_nodes = [n for n in tracker.nodes if isinstance(n, OperatorNode)]
        assert len(source_nodes) == 2
        assert len(op_nodes) == 1
        assert len(op_nodes[0]._upstream_graph_nodes) == 2


# ---------------------------------------------------------------------------
# GraphTracker — generate_graph
# ---------------------------------------------------------------------------


class TestGraphTrackerGraph:
    def test_generate_graph_simple_chain(self):
        """Source → FunctionNode: 2 nodes, 1 edge."""
        pf = PythonPacketFunction(_double, output_keys="result")
        stream = _make_stream()
        mgr = BasicTrackerManager()

        with GraphTracker(tracker_manager=mgr) as tracker:
            tracker.record_packet_function_invocation(pf, stream)

        G = tracker.generate_graph()
        assert len(G.nodes) == 2
        assert len(G.edges) == 1

        source_node = tracker.nodes[0]
        fn_node = tracker.nodes[1]
        assert G.has_edge(source_node, fn_node)

    def test_generate_graph_two_source_join(self):
        """Two sources → Join: 3 nodes, 2 edges."""
        stream_a = _make_stream()
        table_b = pa.table(
            {
                "id": pa.array([0, 1, 2], type=pa.int64()),
                "y": pa.array([10, 20, 30], type=pa.int64()),
            }
        )
        stream_b = ArrowTableStream(table_b, tag_columns=["id"])
        op = Join()
        mgr = BasicTrackerManager()

        with GraphTracker(tracker_manager=mgr) as tracker:
            tracker.record_pod_invocation(op, upstreams=(stream_a, stream_b))

        G = tracker.generate_graph()
        assert len(G.nodes) == 3
        assert len(G.edges) == 2

    def test_generate_graph_chained(self):
        """Source → FunctionNode → Operator → FunctionNode: 4 nodes, 3 edges."""
        pf1 = PythonPacketFunction(_double, output_keys="result")
        pf2 = PythonPacketFunction(_inc_result, output_keys="out")
        stream = _make_stream()
        pod = FunctionPod(packet_function=pf1)
        mgr = BasicTrackerManager()

        with GraphTracker(tracker_manager=mgr) as tracker:
            # Step 1: FunctionPod processes stream
            tracker.record_packet_function_invocation(pf1, stream)
            fn1_output = pod.process(stream)  # producer=pod, pod.packet_function=pf1

            # Step 2: Operator processes fn1_output
            op = SelectTagColumns(columns=["id"])
            tracker.record_pod_invocation(op, upstreams=(fn1_output,))
            op_output = op.process(fn1_output)  # producer=op

            # Step 3: Another function processes op_output
            tracker.record_packet_function_invocation(pf2, op_output)

        G = tracker.generate_graph()
        assert len(G.nodes) == 4  # source, fn1, op, fn2
        assert len(G.edges) == 3

        # Verify chain: source → fn1 → op → fn2
        source = tracker.nodes[0]
        fn1 = tracker.nodes[1]
        op_node = tracker.nodes[2]
        fn2 = tracker.nodes[3]
        assert G.has_edge(source, fn1)
        assert G.has_edge(fn1, op_node)
        assert G.has_edge(op_node, fn2)

    def test_generate_graph_diamond(self):
        """
        Diamond shape: source → fn1, source → fn2, (fn1,fn2) → join.
        5 nodes, 4 edges.
        """
        pf1 = PythonPacketFunction(_double, output_keys="result")
        pf2 = PythonPacketFunction(_double, output_keys="out")
        stream = _make_stream()
        pod1 = FunctionPod(packet_function=pf1)
        pod2 = FunctionPod(packet_function=pf2)
        mgr = BasicTrackerManager()

        with GraphTracker(tracker_manager=mgr) as tracker:
            # Branch 1
            tracker.record_packet_function_invocation(pf1, stream)
            fn1_output = pod1.process(stream)

            # Branch 2
            tracker.record_packet_function_invocation(pf2, stream)
            fn2_output = pod2.process(stream)

            # Merge via Join
            op = Join()
            tracker.record_pod_invocation(op, upstreams=(fn1_output, fn2_output))

        G = tracker.generate_graph()
        assert len(G.nodes) == 4  # 1 source (deduped), fn1, fn2, join
        assert len(G.edges) == 4  # source→fn1, source→fn2, fn1→join, fn2→join


# ---------------------------------------------------------------------------
# GraphTracker — reset and nodes
# ---------------------------------------------------------------------------


class TestGraphTrackerReset:
    def test_reset_clears_all(self):
        pf = PythonPacketFunction(_double, output_keys="result")
        stream = _make_stream()
        mgr = BasicTrackerManager()

        with GraphTracker(tracker_manager=mgr) as tracker:
            tracker.record_packet_function_invocation(pf, stream)
            assert len(tracker.nodes) == 2

            tracker.reset()
            assert len(tracker.nodes) == 0
            assert len(tracker._producer_to_node) == 0
            assert len(tracker._source_to_node) == 0

    def test_nodes_returns_copy(self):
        mgr = BasicTrackerManager()

        with GraphTracker(tracker_manager=mgr) as tracker:
            pf = PythonPacketFunction(_double, output_keys="result")
            tracker.record_packet_function_invocation(pf, _make_stream())
            nodes = tracker.nodes
            nodes.clear()
            # Original unaffected
            assert len(tracker.nodes) == 2


# ---------------------------------------------------------------------------
# End-to-end: FunctionPod.process() with tracker
# ---------------------------------------------------------------------------


class TestFunctionPodTrackerIntegration:
    def test_function_pod_process_records_to_tracker(self):
        """FunctionPod.process() automatically records to an active GraphTracker."""
        pf = PythonPacketFunction(_double, output_keys="result")
        pod = FunctionPod(packet_function=pf)
        stream = _make_stream()
        mgr = BasicTrackerManager()
        pod.tracker_manager = mgr

        with GraphTracker(tracker_manager=mgr) as tracker:
            _ = pod.process(stream)

        assert len(tracker.nodes) == 2
        assert isinstance(tracker.nodes[0], SourceNode)
        assert isinstance(tracker.nodes[1], FunctionNode)
        assert tracker.nodes[1]._upstream_graph_nodes == (tracker.nodes[0],)

    def test_chained_function_pods(self):
        """Two FunctionPods chained: source → fn1 → fn2."""
        pf1 = PythonPacketFunction(_double, output_keys="result")
        pf2 = PythonPacketFunction(_inc_result, output_keys="out")
        pod1 = FunctionPod(packet_function=pf1)
        pod2 = FunctionPod(packet_function=pf2)
        stream = _make_stream()
        mgr = BasicTrackerManager()
        pod1.tracker_manager = mgr
        pod2.tracker_manager = mgr

        with GraphTracker(tracker_manager=mgr) as tracker:
            mid = pod1.process(stream)
            _ = pod2.process(mid)

        assert len(tracker.nodes) == 3
        source = tracker.nodes[0]
        fn1 = tracker.nodes[1]
        fn2 = tracker.nodes[2]
        assert isinstance(source, SourceNode)
        assert isinstance(fn1, FunctionNode)
        assert isinstance(fn2, FunctionNode)
        assert fn1._upstream_graph_nodes == (source,)
        assert fn2._upstream_graph_nodes == (fn1,)


# ---------------------------------------------------------------------------
# End-to-end: StaticOutputPod.process() with tracker
# ---------------------------------------------------------------------------


class TestOperatorTrackerIntegration:
    def test_operator_process_records_to_tracker(self):
        """StaticOutputPod.process() automatically records to an active GraphTracker."""
        stream = _make_stream()
        op = SelectTagColumns(columns=["id"])
        mgr = BasicTrackerManager()
        op.tracker_manager = mgr

        with GraphTracker(tracker_manager=mgr) as tracker:
            _ = op.process(stream)

        assert len(tracker.nodes) == 2
        assert isinstance(tracker.nodes[0], SourceNode)
        assert isinstance(tracker.nodes[1], OperatorNode)

    def test_operator_chain(self):
        """Source → operator1 → operator2."""
        stream = _make_two_col_stream()
        op1 = SelectTagColumns(columns=["id"])
        op2 = SelectTagColumns(columns=["id"])
        mgr = BasicTrackerManager()
        op1.tracker_manager = mgr
        op2.tracker_manager = mgr

        with GraphTracker(tracker_manager=mgr) as tracker:
            mid = op1.process(stream)
            _ = op2.process(mid)

        assert len(tracker.nodes) == 3
        source = tracker.nodes[0]
        op1_node = tracker.nodes[1]
        op2_node = tracker.nodes[2]
        assert isinstance(source, SourceNode)
        assert isinstance(op1_node, OperatorNode)
        assert isinstance(op2_node, OperatorNode)
        assert op1_node._upstream_graph_nodes == (source,)
        assert op2_node._upstream_graph_nodes == (op1_node,)


# ---------------------------------------------------------------------------
# Manager broadcast
# ---------------------------------------------------------------------------


class TestManagerBroadcast:
    def test_records_broadcast_to_all_active_trackers(self):
        """BasicTrackerManager broadcasts recordings to all active trackers."""
        pf = PythonPacketFunction(_double, output_keys="result")
        stream = _make_stream()
        mgr = BasicTrackerManager()

        tracker1 = GraphTracker(tracker_manager=mgr)
        tracker2 = GraphTracker(tracker_manager=mgr)

        with tracker1, tracker2:
            mgr.record_packet_function_invocation(pf, stream)

        assert len(tracker1.nodes) == 2
        assert len(tracker2.nodes) == 2

    def test_no_tracking_suppresses_recording(self):
        """no_tracking context suppresses recording."""
        pf = PythonPacketFunction(_double, output_keys="result")
        stream = _make_stream()
        mgr = BasicTrackerManager()

        with GraphTracker(tracker_manager=mgr) as tracker:
            with mgr.no_tracking():
                mgr.record_packet_function_invocation(pf, stream)

        assert len(tracker.nodes) == 0
