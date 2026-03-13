"""
Tests for the tracker module covering:

- SourceNode: construction, properties, delegation, repr
- BasicTrackerManager: register/deregister, active state, no_tracking context
- AutoRegisteringContextBasedTracker: context manager lifecycle
- Pipeline (tracker functionality):
    - record_function_pod_invocation -> creates FunctionNode, stores edges
    - record_operator_pod_invocation -> creates OperatorNode, stores edges
    - compile() -> topological walk, SourceNode creation, upstream rewiring
    - Source deduplication
    - reset() clears all state
    - nodes property
- End-to-end: FunctionPod.process() and StaticOutputPod.process() inside tracker context
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.core.function_pod import FunctionPod, function_pod
from orcapod.core.nodes import FunctionNode, OperatorNode, SourceNode
from orcapod.core.operators import Join, SelectTagColumns
from orcapod.core.packet_function import PythonPacketFunction
from orcapod.core.sources.arrow_table_source import ArrowTableSource
from orcapod.core.streams import ArrowTableStream
from orcapod.core.tracker import BasicTrackerManager
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline import Pipeline

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _double(x: int) -> int:
    return x * 2


def _inc_result(result: int) -> int:
    return result + 1


def _make_pipeline(
    tracker_manager: BasicTrackerManager | None = None,
) -> Pipeline:
    """Create a Pipeline for testing (auto_compile=False)."""
    return Pipeline(
        name="test",
        pipeline_database=InMemoryArrowDatabase(),
        tracker_manager=tracker_manager,
        auto_compile=False,
    )


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


def _make_y_stream(n: int = 3) -> ArrowTableStream:
    """Stream with tag=id, packet=y (non-overlapping with _make_stream)."""
    table = pa.table(
        {
            "id": pa.array(list(range(n)), type=pa.int64()),
            "y": pa.array([i * 10 for i in range(n)], type=pa.int64()),
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
        # computed_label defers to wrapped stream's label
        assert node.label == "stream_label"

    def test_label_defaults_to_stream_label(self):
        stream = _make_stream()
        node = SourceNode(stream=stream)
        # No explicit label → computed_label defers to stream.label
        assert node.label == stream.label

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

    def test_content_hash_matches_stream(self):
        stream = _make_stream()
        node = SourceNode(stream=stream)
        assert node.content_hash() == stream.content_hash()

    def test_upstreams_setter_rejects_nonempty(self):
        stream = _make_stream()
        node = SourceNode(stream=stream)
        with pytest.raises(ValueError, match="empty"):
            node.upstreams = (_make_stream(),)

    def test_delegates_output_schema(self):
        stream = _make_stream()
        node = SourceNode(stream=stream)
        assert node.output_schema() == stream.output_schema()

    def test_delegates_keys(self):
        stream = _make_stream()
        node = SourceNode(stream=stream)
        assert node.keys() == stream.keys()

    def test_delegates_as_table(self):
        stream = _make_stream()
        node = SourceNode(stream=stream)
        node_table = node.as_table()
        stream_table = stream.as_table()
        assert node_table.equals(stream_table)

    def test_delegates_iter_packets(self):
        stream = _make_stream()
        node = SourceNode(stream=stream)
        node_packets = list(node.iter_packets())
        stream_packets = list(stream.iter_packets())
        assert len(node_packets) == len(stream_packets)

    def test_run_is_noop(self):
        stream = _make_stream()
        node = SourceNode(stream=stream)
        # run() should succeed without side effects
        node.run()
        # Data is still accessible after run()
        assert node.as_table().num_rows == stream.as_table().num_rows

    def test_delegates_data_context_key(self):
        stream = _make_stream()
        node = SourceNode(stream=stream)
        assert node.data_context_key == stream.data_context_key

    def test_delegates_data_context(self):
        stream = _make_stream()
        node = SourceNode(stream=stream)
        assert node.data_context.context_key == stream.data_context_key


# ---------------------------------------------------------------------------
# Node context delegation
# ---------------------------------------------------------------------------


class TestNodeContextDelegation:
    """All node types delegate data_context to their wrapped entity."""

    def test_source_node_context_matches_stream(self):
        stream = _make_stream()
        node = SourceNode(stream=stream)
        assert node.data_context_key == stream.data_context_key
        assert node.data_context.context_key == stream.data_context_key

    def test_function_node_context_matches_pod(self):
        stream = _make_stream()
        pf = PythonPacketFunction(_double, output_keys="result")
        pod = FunctionPod(packet_function=pf)
        node = FunctionNode(function_pod=pod, input_stream=stream)
        assert node.data_context_key == pod.data_context_key
        assert node.data_context.context_key == pod.data_context_key

    def test_operator_node_context_matches_operator(self):
        stream = _make_two_col_stream()
        op = SelectTagColumns("id")
        node = OperatorNode(operator=op, input_streams=[stream])
        assert node.data_context_key == op.data_context_key
        assert node.data_context.context_key == op.data_context_key

    def test_source_node_hash_consistent_with_stream(self):
        stream = _make_stream()
        node = SourceNode(stream=stream)
        # Both should use the same hasher (from the same data context)
        assert node.content_hash() == stream.content_hash()
        assert node.pipeline_hash() == stream.pipeline_hash()

    def test_function_node_hash_uses_pod_context(self):
        stream = _make_stream()
        pf = PythonPacketFunction(_double, output_keys="result")
        pod = FunctionPod(packet_function=pf)
        node = FunctionNode(function_pod=pod, input_stream=stream)
        # Node should produce stable hashes without error
        assert node.content_hash() is not None
        assert node.pipeline_hash() is not None


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
        tracker = _make_pipeline(tracker_manager=mgr)
        tracker.set_active(True)
        assert tracker in mgr.get_active_trackers()
        tracker.set_active(False)
        assert tracker not in mgr.get_active_trackers()

    def test_duplicate_register_ignored(self):
        mgr = BasicTrackerManager()
        tracker = _make_pipeline(tracker_manager=mgr)
        mgr.register_tracker(tracker)
        mgr.register_tracker(tracker)
        assert mgr._active_trackers.count(tracker) == 1

    def test_deregister_nonexistent_is_noop(self):
        mgr = BasicTrackerManager()
        tracker = _make_pipeline(tracker_manager=mgr)
        # Should not raise
        mgr.deregister_tracker(tracker)

    def test_set_active_false_returns_empty(self):
        mgr = BasicTrackerManager()
        tracker = _make_pipeline(tracker_manager=mgr)
        tracker.set_active(True)
        mgr.set_active(False)
        assert mgr.get_active_trackers() == []
        mgr.set_active(True)
        assert tracker in mgr.get_active_trackers()

    def test_no_tracking_context(self):
        mgr = BasicTrackerManager()
        tracker = _make_pipeline(tracker_manager=mgr)
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
# Pipeline — context manager lifecycle
# ---------------------------------------------------------------------------


class TestPipelineLifecycle:
    def test_context_manager_activates_and_deactivates(self):
        mgr = BasicTrackerManager()
        tracker = _make_pipeline(tracker_manager=mgr)
        assert not tracker.is_active()

        with tracker:
            assert tracker.is_active()
            assert tracker in mgr.get_active_trackers()

        assert not tracker.is_active()
        assert tracker not in mgr.get_active_trackers()

    def test_inactive_tracker_not_in_active_list(self):
        mgr = BasicTrackerManager()
        tracker = _make_pipeline(tracker_manager=mgr)
        tracker.set_active(True)
        tracker.set_active(False)
        assert mgr.get_active_trackers() == []


# ---------------------------------------------------------------------------
# Pipeline — recording
# ---------------------------------------------------------------------------


class TestPipelineRecording:
    def test_record_function_pod_creates_function_node(self):
        pf = PythonPacketFunction(_double, output_keys="result")
        pod = FunctionPod(packet_function=pf)
        stream = _make_stream()
        mgr = BasicTrackerManager()

        with _make_pipeline(tracker_manager=mgr) as tracker:
            tracker.record_function_pod_invocation(pod, stream, label="dbl")

        # Should have one FunctionNode in node_lut
        assert len(tracker._node_lut) == 1
        fn_node = list(tracker._node_lut.values())[0]
        assert isinstance(fn_node, FunctionNode)
        assert fn_node.node_type == "function"

    def test_record_function_pod_stores_edge(self):
        pf = PythonPacketFunction(_double, output_keys="result")
        pod = FunctionPod(packet_function=pf)
        stream = _make_stream()
        mgr = BasicTrackerManager()

        with _make_pipeline(tracker_manager=mgr) as tracker:
            tracker.record_function_pod_invocation(pod, stream)

        assert len(tracker._graph_edges) == 1
        upstream_hash, node_hash = tracker._graph_edges[0]
        assert upstream_hash == stream.content_hash().to_string()
        assert node_hash in tracker._node_lut

    def test_record_function_pod_stores_upstream_stream(self):
        pf = PythonPacketFunction(_double, output_keys="result")
        pod = FunctionPod(packet_function=pf)
        stream = _make_stream()
        mgr = BasicTrackerManager()

        with _make_pipeline(tracker_manager=mgr) as tracker:
            tracker.record_function_pod_invocation(pod, stream)

        stream_hash = stream.content_hash().to_string()
        assert stream_hash in tracker._upstreams
        assert tracker._upstreams[stream_hash] is stream

    def test_record_operator_pod_creates_operator_node(self):
        stream = _make_stream()
        op = SelectTagColumns(columns=["id"])
        mgr = BasicTrackerManager()

        with _make_pipeline(tracker_manager=mgr) as tracker:
            tracker.record_operator_pod_invocation(op, upstreams=(stream,))

        assert len(tracker._node_lut) == 1
        op_node = list(tracker._node_lut.values())[0]
        assert isinstance(op_node, OperatorNode)
        assert op_node.node_type == "operator"

    def test_record_operator_pod_stores_edges(self):
        stream_a = _make_stream()
        stream_b = _make_y_stream()
        op = Join()
        mgr = BasicTrackerManager()

        with _make_pipeline(tracker_manager=mgr) as tracker:
            tracker.record_operator_pod_invocation(op, upstreams=(stream_a, stream_b))

        assert len(tracker._graph_edges) == 2

    def test_nodes_returns_copy(self):
        mgr = BasicTrackerManager()
        pf = PythonPacketFunction(_double, output_keys="result")
        pod = FunctionPod(packet_function=pf)

        with _make_pipeline(tracker_manager=mgr) as tracker:
            tracker.record_function_pod_invocation(pod, _make_stream())
            nodes = tracker.nodes
            nodes.clear()
            # Original unaffected
            assert len(tracker.nodes) == 1

    def test_reset_clears_all(self):
        pf = PythonPacketFunction(_double, output_keys="result")
        pod = FunctionPod(packet_function=pf)
        stream = _make_stream()
        mgr = BasicTrackerManager()

        with _make_pipeline(tracker_manager=mgr) as tracker:
            tracker.record_function_pod_invocation(pod, stream)
            assert len(tracker.nodes) == 1

            tracker.reset()
            assert len(tracker.nodes) == 0
            assert len(tracker._upstreams) == 0
            assert len(tracker._graph_edges) == 0


# ---------------------------------------------------------------------------
# Pipeline — compile()
# ---------------------------------------------------------------------------


class TestPipelineCompile:
    """Tests for compile() which resolves content-hash edges into node-to-node
    upstream relationships via topological sort."""

    @staticmethod
    def _persistent_nodes(pipeline: Pipeline) -> list:
        """Return all persistent nodes after compile()."""
        return list(pipeline._persistent_node_map.values())

    def test_compile_single_function_pod(self):
        """Source stream -> FunctionNode: compile creates SourceNode and wires upstream."""
        pf = PythonPacketFunction(_double, output_keys="result")
        pod = FunctionPod(packet_function=pf)
        stream = _make_stream()
        mgr = BasicTrackerManager()

        with _make_pipeline(tracker_manager=mgr) as tracker:
            tracker.record_function_pod_invocation(pod, stream)
            tracker.compile()

        # After compile: 1 SourceNode + 1 FunctionNode in persistent map
        all_nodes = self._persistent_nodes(tracker)
        assert len(all_nodes) == 2
        source_nodes = [n for n in all_nodes if isinstance(n, SourceNode)]
        fn_nodes = [n for n in all_nodes if isinstance(n, FunctionNode)]
        assert len(source_nodes) == 1
        assert len(fn_nodes) == 1

        # SourceNode wraps the original stream
        assert source_nodes[0].stream is stream
        assert source_nodes[0].upstreams == ()

        # FunctionNode's upstream is now the SourceNode
        assert fn_nodes[0].upstreams == (source_nodes[0],)

    def test_compile_single_operator(self):
        """Source stream -> Operator: compile creates SourceNode and wires upstream."""
        stream = _make_stream()
        op = SelectTagColumns(columns=["id"])
        mgr = BasicTrackerManager()

        with _make_pipeline(tracker_manager=mgr) as tracker:
            tracker.record_operator_pod_invocation(op, upstreams=(stream,))
            tracker.compile()

        all_nodes = self._persistent_nodes(tracker)
        assert len(all_nodes) == 2
        source_nodes = [n for n in all_nodes if isinstance(n, SourceNode)]
        op_nodes = [n for n in all_nodes if isinstance(n, OperatorNode)]
        assert len(source_nodes) == 1
        assert len(op_nodes) == 1
        assert op_nodes[0].upstreams == (source_nodes[0],)

    def test_compile_operator_with_two_inputs(self):
        """Two source streams -> Join: compile creates 2 SourceNodes."""
        stream_a = _make_stream()
        stream_b = _make_y_stream()
        op = Join()
        mgr = BasicTrackerManager()

        with _make_pipeline(tracker_manager=mgr) as tracker:
            tracker.record_operator_pod_invocation(op, upstreams=(stream_a, stream_b))

        tracker.compile()
        all_nodes = self._persistent_nodes(tracker)
        assert len(all_nodes) == 3
        source_nodes = [n for n in all_nodes if isinstance(n, SourceNode)]
        op_nodes = [n for n in all_nodes if isinstance(n, OperatorNode)]
        assert len(source_nodes) == 2
        assert len(op_nodes) == 1

        # OperatorNode's upstreams are both SourceNodes
        assert len(op_nodes[0].upstreams) == 2
        assert all(isinstance(u, SourceNode) for u in op_nodes[0].upstreams)

    def test_compile_chained_function_pods(self):
        """Source -> fn1 -> fn2: compile wires SourceNode -> FunctionNode1 -> FunctionNode2.

        The key insight: FunctionNode and FunctionPodStream have the same
        identity_structure for the same (function_pod, input_stream), so
        content hashes match and edges connect across the chain.
        """
        pf1 = PythonPacketFunction(_double, output_keys="result")
        pf2 = PythonPacketFunction(_inc_result, output_keys="out")
        pod1 = FunctionPod(packet_function=pf1)
        pod2 = FunctionPod(packet_function=pf2)
        stream = _make_stream()
        mgr = BasicTrackerManager()
        pod1.tracker_manager = mgr
        pod2.tracker_manager = mgr

        with _make_pipeline(tracker_manager=mgr) as tracker:
            mid = pod1.process(stream)  # records fn1, returns FunctionPodStream
            _ = pod2.process(mid)  # records fn2, mid.content_hash == fn1.content_hash
            tracker.compile()

        all_nodes = self._persistent_nodes(tracker)
        assert len(all_nodes) == 3
        source_nodes = [n for n in all_nodes if isinstance(n, SourceNode)]
        fn_nodes = [n for n in all_nodes if isinstance(n, FunctionNode)]
        assert len(source_nodes) == 1
        assert len(fn_nodes) == 2

        # Identify fn1 and fn2 by checking their function_pod's packet_function
        fn1 = next(n for n in fn_nodes if n._function_pod.packet_function is pf1)
        fn2 = next(n for n in fn_nodes if n._function_pod.packet_function is pf2)

        # Chain: SourceNode -> fn1 -> fn2
        assert fn1.upstreams == (source_nodes[0],)
        assert fn2.upstreams == (fn1,)

    def test_compile_function_then_operator(self):
        """Source -> FunctionPod -> Operator: compile wires SourceNode -> FunctionNode -> OperatorNode."""
        pf = PythonPacketFunction(_double, output_keys="result")
        pod = FunctionPod(packet_function=pf)
        op = SelectTagColumns(columns=["id"])
        stream = _make_stream()
        mgr = BasicTrackerManager()
        pod.tracker_manager = mgr
        op.tracker_manager = mgr

        with _make_pipeline(tracker_manager=mgr) as tracker:
            mid = pod.process(stream)
            _ = op.process(mid)
            tracker.compile()

        all_nodes = self._persistent_nodes(tracker)
        source_nodes = [n for n in all_nodes if isinstance(n, SourceNode)]
        fn_nodes = [n for n in all_nodes if isinstance(n, FunctionNode)]
        op_nodes = [n for n in all_nodes if isinstance(n, OperatorNode)]
        assert len(source_nodes) == 1
        assert len(fn_nodes) == 1
        assert len(op_nodes) == 1

        assert fn_nodes[0].upstreams == (source_nodes[0],)
        assert op_nodes[0].upstreams == (fn_nodes[0],)

    def test_compile_operator_then_function(self):
        """Source -> Operator -> FunctionPod: compile wires SourceNode -> OperatorNode -> FunctionNode."""
        stream = _make_stream()
        op = SelectTagColumns(columns=["id"])
        pf = PythonPacketFunction(_double, output_keys="result")
        pod = FunctionPod(packet_function=pf)
        mgr = BasicTrackerManager()
        op.tracker_manager = mgr
        pod.tracker_manager = mgr

        with _make_pipeline(tracker_manager=mgr) as tracker:
            mid = op.process(stream)
            _ = pod.process(mid)
            tracker.compile()

        all_nodes = self._persistent_nodes(tracker)
        source_nodes = [n for n in all_nodes if isinstance(n, SourceNode)]
        fn_nodes = [n for n in all_nodes if isinstance(n, FunctionNode)]
        op_nodes = [n for n in all_nodes if isinstance(n, OperatorNode)]
        assert len(source_nodes) == 1
        assert len(op_nodes) == 1
        assert len(fn_nodes) == 1

        assert op_nodes[0].upstreams == (source_nodes[0],)
        assert fn_nodes[0].upstreams == (op_nodes[0],)

    def test_compile_diamond(self):
        """Diamond: source -> fn1, source -> fn2, (fn1, fn2) -> join.

        Same source used twice -> single SourceNode (dedup by content hash).
        """
        pf1 = PythonPacketFunction(_double, output_keys="result")
        pf2 = PythonPacketFunction(_double, output_keys="out")
        pod1 = FunctionPod(packet_function=pf1)
        pod2 = FunctionPod(packet_function=pf2)
        op = Join()
        stream = _make_stream()
        mgr = BasicTrackerManager()
        pod1.tracker_manager = mgr
        pod2.tracker_manager = mgr
        op.tracker_manager = mgr

        with _make_pipeline(tracker_manager=mgr) as tracker:
            mid1 = pod1.process(stream)
            mid2 = pod2.process(stream)
            _ = op.process(mid1, mid2)
            tracker.compile()

        all_nodes = self._persistent_nodes(tracker)
        source_nodes = [n for n in all_nodes if isinstance(n, SourceNode)]
        fn_nodes = [n for n in all_nodes if isinstance(n, FunctionNode)]
        op_nodes = [n for n in all_nodes if isinstance(n, OperatorNode)]

        # 1 source (deduped), 2 function nodes, 1 operator node
        assert len(source_nodes) == 1
        assert len(fn_nodes) == 2
        assert len(op_nodes) == 1

        # Both function nodes have the same source upstream
        for fn in fn_nodes:
            assert fn.upstreams == (source_nodes[0],)

        # Join's upstreams are the two FunctionNodes
        assert len(op_nodes[0].upstreams) == 2
        assert all(isinstance(u, FunctionNode) for u in op_nodes[0].upstreams)

    def test_compile_source_deduplication(self):
        """Same stream used as input to two separate function pods -> single SourceNode."""
        pf1 = PythonPacketFunction(_double, output_keys="result")
        pf2 = PythonPacketFunction(_double, output_keys="out")
        pod1 = FunctionPod(packet_function=pf1)
        pod2 = FunctionPod(packet_function=pf2)
        stream = _make_stream()
        mgr = BasicTrackerManager()

        with _make_pipeline(tracker_manager=mgr) as tracker:
            tracker.record_function_pod_invocation(pod1, stream)
            tracker.record_function_pod_invocation(pod2, stream)
            tracker.compile()

        all_nodes = self._persistent_nodes(tracker)
        source_nodes = [n for n in all_nodes if isinstance(n, SourceNode)]
        fn_nodes = [n for n in all_nodes if isinstance(n, FunctionNode)]
        assert len(source_nodes) == 1
        assert len(fn_nodes) == 2

        # Both FunctionNodes share the same SourceNode upstream
        assert fn_nodes[0].upstreams[0] is fn_nodes[1].upstreams[0]

    def test_compile_two_independent_sources(self):
        """Two different source streams -> two distinct SourceNodes."""
        pf = PythonPacketFunction(_double, output_keys="result")
        pod = FunctionPod(packet_function=pf)
        stream_a = _make_stream(n=3)
        stream_b = _make_stream(n=5)
        mgr = BasicTrackerManager()

        with _make_pipeline(tracker_manager=mgr) as tracker:
            tracker.record_function_pod_invocation(pod, stream_a)
            tracker.record_function_pod_invocation(pod, stream_b)
            tracker.compile()

        all_nodes = self._persistent_nodes(tracker)
        source_nodes = [n for n in all_nodes if isinstance(n, SourceNode)]
        assert len(source_nodes) == 2

    def test_compile_empty_tracker(self):
        """Compile on empty pipeline is a no-op."""
        mgr = BasicTrackerManager()

        with _make_pipeline(tracker_manager=mgr) as tracker:
            tracker.compile()

        assert len(self._persistent_nodes(tracker)) == 0


# ---------------------------------------------------------------------------
# End-to-end: FunctionPod.process() with tracker
# ---------------------------------------------------------------------------


class TestFunctionPodTrackerIntegration:
    def test_function_pod_process_records_to_tracker(self):
        """FunctionPod.process() automatically records to an active Pipeline."""
        pf = PythonPacketFunction(_double, output_keys="result")
        pod = FunctionPod(packet_function=pf)
        stream = _make_stream()
        mgr = BasicTrackerManager()
        pod.tracker_manager = mgr

        with _make_pipeline(tracker_manager=mgr) as tracker:
            _ = pod.process(stream)
            tracker.compile()

        all_nodes = list(tracker._persistent_node_map.values())
        assert len(all_nodes) == 2
        source_nodes = [n for n in all_nodes if isinstance(n, SourceNode)]
        fn_nodes = [n for n in all_nodes if isinstance(n, FunctionNode)]
        assert len(source_nodes) == 1
        assert len(fn_nodes) == 1
        assert fn_nodes[0].upstreams == (source_nodes[0],)

    def test_chained_function_pods_end_to_end(self):
        """Two FunctionPods chained: source -> fn1 -> fn2."""
        pf1 = PythonPacketFunction(_double, output_keys="result")
        pf2 = PythonPacketFunction(_inc_result, output_keys="out")
        pod1 = FunctionPod(packet_function=pf1)
        pod2 = FunctionPod(packet_function=pf2)
        stream = _make_stream()
        mgr = BasicTrackerManager()
        pod1.tracker_manager = mgr
        pod2.tracker_manager = mgr

        with _make_pipeline(tracker_manager=mgr) as tracker:
            mid = pod1.process(stream)
            _ = pod2.process(mid)
            tracker.compile()

        all_nodes = list(tracker._persistent_node_map.values())
        source_nodes = [n for n in all_nodes if isinstance(n, SourceNode)]
        fn_nodes = [n for n in all_nodes if isinstance(n, FunctionNode)]
        assert len(source_nodes) == 1
        assert len(fn_nodes) == 2

        fn1 = next(n for n in fn_nodes if n._function_pod.packet_function is pf1)
        fn2 = next(n for n in fn_nodes if n._function_pod.packet_function is pf2)
        assert fn1.upstreams == (source_nodes[0],)
        assert fn2.upstreams == (fn1,)


# ---------------------------------------------------------------------------
# End-to-end: StaticOutputPod.process() with tracker
# ---------------------------------------------------------------------------


class TestOperatorTrackerIntegration:
    def test_operator_process_records_to_tracker(self):
        """StaticOutputPod.process() automatically records to an active Pipeline."""
        stream = _make_stream()
        op = SelectTagColumns(columns=["id"])
        mgr = BasicTrackerManager()
        op.tracker_manager = mgr

        with _make_pipeline(tracker_manager=mgr) as tracker:
            _ = op.process(stream)
            tracker.compile()

        all_nodes = list(tracker._persistent_node_map.values())
        source_nodes = [n for n in all_nodes if isinstance(n, SourceNode)]
        op_nodes = [n for n in all_nodes if isinstance(n, OperatorNode)]
        assert len(source_nodes) == 1
        assert len(op_nodes) == 1
        assert op_nodes[0].upstreams == (source_nodes[0],)

    def test_operator_chain(self):
        """Source -> operator1 -> operator2."""
        stream = _make_two_col_stream()
        op1 = SelectTagColumns(columns=["id"])
        op2 = SelectTagColumns(columns=["id"])
        mgr = BasicTrackerManager()
        op1.tracker_manager = mgr
        op2.tracker_manager = mgr

        with _make_pipeline(tracker_manager=mgr) as tracker:
            mid = op1.process(stream)
            _ = op2.process(mid)
            tracker.compile()

        all_nodes = list(tracker._persistent_node_map.values())
        source_nodes = [n for n in all_nodes if isinstance(n, SourceNode)]
        op_nodes = [n for n in all_nodes if isinstance(n, OperatorNode)]
        assert len(source_nodes) == 1
        assert len(op_nodes) == 2


# ---------------------------------------------------------------------------
# Manager broadcast
# ---------------------------------------------------------------------------


class TestManagerBroadcast:
    def test_records_broadcast_to_all_active_trackers(self):
        """BasicTrackerManager broadcasts recordings to all active trackers."""
        pf = PythonPacketFunction(_double, output_keys="result")
        pod = FunctionPod(packet_function=pf)
        stream = _make_stream()
        mgr = BasicTrackerManager()

        tracker1 = _make_pipeline(tracker_manager=mgr)
        tracker2 = _make_pipeline(tracker_manager=mgr)

        with tracker1, tracker2:
            mgr.record_function_pod_invocation(pod, stream)

        assert len(tracker1.nodes) == 1
        assert len(tracker2.nodes) == 1

    def test_no_tracking_suppresses_recording(self):
        """no_tracking context suppresses recording."""
        pf = PythonPacketFunction(_double, output_keys="result")
        pod = FunctionPod(packet_function=pf)
        stream = _make_stream()
        mgr = BasicTrackerManager()

        with _make_pipeline(tracker_manager=mgr) as tracker:
            with mgr.no_tracking():
                mgr.record_function_pod_invocation(pod, stream)

        assert len(tracker.nodes) == 0


# ---------------------------------------------------------------------------
# End-to-end: BMI pipeline with ArrowTableSource, @function_pod, default tracker
# ---------------------------------------------------------------------------


@function_pod(output_keys="height_m")
def _cm_to_m(height_cm: int) -> float:
    return height_cm / 100.0


@function_pod(output_keys="bmi")
def _compute_bmi(height_m: float, weight_kg: int) -> float:
    return round(weight_kg / (height_m**2), 2)


class TestBMIPipelineEndToEnd:
    """Full pipeline: two ArrowTableSources → @function_pod → Join → @function_pod.

    Uses DEFAULT_TRACKER_MANAGER (no explicit wiring) to verify that the
    default plumbing works out of the box.

    Pipeline:
        heights(person_id, height_cm) ──► cm_to_m ──┐
                                                      ├──► Join ──► compute_bmi
        weights(person_id, weight_kg) ──────────────┘
    """

    @pytest.fixture()
    def sources(self):
        heights = ArrowTableSource(
            pa.table(
                {
                    "person_id": pa.array([1, 2, 3], type=pa.int64()),
                    "height_cm": pa.array([170, 185, 160], type=pa.int64()),
                }
            ),
            tag_columns=["person_id"],
            source_id="heights",
        )
        weights = ArrowTableSource(
            pa.table(
                {
                    "person_id": pa.array([1, 2, 3], type=pa.int64()),
                    "weight_kg": pa.array([70, 90, 55], type=pa.int64()),
                }
            ),
            tag_columns=["person_id"],
            source_id="weights",
        )
        return heights, weights

    @pytest.fixture()
    def expected_bmi(self):
        return {
            1: round(70 / (1.70**2), 2),
            2: round(90 / (1.85**2), 2),
            3: round(55 / (1.60**2), 2),
        }

    def test_pipeline_output_values(self, sources, expected_bmi):
        """The pipeline produces correct BMI values."""
        heights, weights = sources

        tracker = _make_pipeline()
        with tracker:
            converted = _cm_to_m.pod(heights)
            joined = Join()(converted, weights)
            bmi_stream = _compute_bmi.pod(joined)

        for tag, packet in bmi_stream.iter_packets():
            pid = tag["person_id"]
            assert packet["bmi"] == expected_bmi[pid], (
                f"person_id={pid}: got {packet['bmi']}, expected {expected_bmi[pid]}"
            )

    def test_compiled_graph_structure(self, sources):
        """After compile(), the graph has the expected node types and count."""
        heights, weights = sources

        tracker = _make_pipeline()
        with tracker:
            converted = _cm_to_m.pod(heights)
            joined = Join()(converted, weights)
            _cm_bmi = _compute_bmi.pod(joined)

        tracker.compile()

        all_nodes = list(tracker._persistent_node_map.values())
        src_nodes = [n for n in all_nodes if isinstance(n, SourceNode)]
        fn_nodes = [n for n in all_nodes if isinstance(n, FunctionNode)]
        op_nodes = [n for n in all_nodes if isinstance(n, OperatorNode)]

        assert len(src_nodes) == 2
        assert len(fn_nodes) == 2
        assert len(op_nodes) == 1

    def test_compiled_graph_all_upstreams_are_nodes(self, sources):
        """Every upstream reference is a graph node after compile()."""
        heights, weights = sources

        tracker = _make_pipeline()
        with tracker:
            converted = _cm_to_m.pod(heights)
            joined = Join()(converted, weights)
            _ = _compute_bmi.pod(joined)

        tracker.compile()

        all_nodes = list(tracker._persistent_node_map.values())
        for node in all_nodes:
            for up in node.upstreams:
                assert isinstance(up, (SourceNode, FunctionNode, OperatorNode)), (
                    f"Upstream of {node.label} is {type(up).__name__}, expected a graph node"
                )

    def test_compiled_graph_wiring(self, sources):
        """Verify specific upstream wiring: cm_to_m<-source, join<-(cm_to_m, source), bmi<-join."""
        heights, weights = sources

        tracker = _make_pipeline()
        with tracker:
            converted = _cm_to_m.pod(heights)
            joined = Join()(converted, weights)
            _ = _compute_bmi.pod(joined)

        tracker.compile()

        all_nodes = list(tracker._persistent_node_map.values())
        fn_nodes = [n for n in all_nodes if isinstance(n, FunctionNode)]
        op_nodes = [n for n in all_nodes if isinstance(n, OperatorNode)]

        # cm_to_m has a single SourceNode upstream
        cm_node = next(
            n
            for n in fn_nodes
            if n._function_pod.packet_function is _cm_to_m.pod.packet_function
        )
        assert len(cm_node.upstreams) == 1
        assert isinstance(cm_node.upstreams[0], SourceNode)

        # Join has one FunctionNode upstream (cm_to_m) and one SourceNode (weights)
        join_node = op_nodes[0]
        assert len(join_node.upstreams) == 2
        upstream_types = {type(u).__name__ for u in join_node.upstreams}
        assert upstream_types == {"FunctionNode", "SourceNode"}

        # compute_bmi has the Join OperatorNode as its single upstream
        bmi_node = next(
            n
            for n in fn_nodes
            if n._function_pod.packet_function is _compute_bmi.pod.packet_function
        )
        assert len(bmi_node.upstreams) == 1
        assert isinstance(bmi_node.upstreams[0], OperatorNode)
