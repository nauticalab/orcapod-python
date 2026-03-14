"""Tests for the synchronous pipeline orchestrator."""

from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.core.function_pod import FunctionPod
from orcapod.core.nodes import FunctionNode, OperatorNode, SourceNode
from orcapod.core.operators import SelectPacketColumns
from orcapod.core.operators.join import Join
from orcapod.core.operators.mappers import MapPackets
from orcapod.core.packet_function import PythonPacketFunction
from orcapod.core.sources import ArrowTableSource
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline import Pipeline
from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator


def _make_source(tag_col, packet_col, data):
    table = pa.table(
        {
            tag_col: pa.array(data[tag_col], type=pa.large_string()),
            packet_col: pa.array(data[packet_col], type=pa.int64()),
        }
    )
    return ArrowTableSource(table, tag_columns=[tag_col])


def double_value(value: int) -> int:
    return value * 2


def add_values(value: int, score: int) -> int:
    return value + score


class TestSyncOrchestratorLinear:
    """Source -> FunctionPod."""

    def test_linear_pipeline(self):
        src = _make_source("key", "value", {"key": ["a", "b", "c"], "value": [1, 2, 3]})
        pf = PythonPacketFunction(double_value, output_keys="result")
        pod = FunctionPod(pf)

        pipeline = Pipeline(name="linear", pipeline_database=InMemoryArrowDatabase())
        with pipeline:
            pod(src, label="doubler")

        orch = SyncPipelineOrchestrator()
        result = orch.run(pipeline._node_graph)

        assert len(result.node_outputs) > 0

        fn_outputs = [
            v for k, v in result.node_outputs.items() if k.node_type == "function"
        ]
        assert len(fn_outputs) == 1
        assert len(fn_outputs[0]) == 3
        values = sorted([pkt.as_dict()["result"] for _, pkt in fn_outputs[0]])
        assert values == [2, 4, 6]


class TestSyncOrchestratorWithOperator:
    """Source -> Operator -> FunctionPod."""

    def test_operator_pipeline(self):
        src = _make_source("key", "value", {"key": ["a", "b", "c"], "value": [1, 2, 3]})
        op = MapPackets(name_map={"value": "val"})

        def double_val(val: int) -> int:
            return val * 2

        pf = PythonPacketFunction(double_val, output_keys="result")
        pod = FunctionPod(pf)

        pipeline = Pipeline(name="op_pipe", pipeline_database=InMemoryArrowDatabase())
        with pipeline:
            mapped = op(src, label="mapper")
            pod(mapped, label="doubler")

        orch = SyncPipelineOrchestrator()
        result = orch.run(pipeline._node_graph)

        fn_outputs = [
            v for k, v in result.node_outputs.items() if k.node_type == "function"
        ]
        assert len(fn_outputs) == 1
        values = sorted([pkt.as_dict()["result"] for _, pkt in fn_outputs[0]])
        assert values == [2, 4, 6]


class TestSyncOrchestratorDiamond:
    """Two sources -> Join -> FunctionPod."""

    def test_diamond_dag(self):
        src_a = _make_source("key", "value", {"key": ["a", "b"], "value": [10, 20]})
        src_b = _make_source("key", "score", {"key": ["a", "b"], "score": [100, 200]})
        pf = PythonPacketFunction(add_values, output_keys="total")
        pod = FunctionPod(pf)

        pipeline = Pipeline(name="diamond", pipeline_database=InMemoryArrowDatabase())
        with pipeline:
            joined = Join()(src_a, src_b, label="join")
            pod(joined, label="adder")

        orch = SyncPipelineOrchestrator()
        result = orch.run(pipeline._node_graph)

        fn_outputs = [
            v for k, v in result.node_outputs.items() if k.node_type == "function"
        ]
        assert len(fn_outputs) == 1
        values = sorted([pkt.as_dict()["total"] for _, pkt in fn_outputs[0]])
        assert values == [110, 220]


class TestSyncOrchestratorObserver:
    """Observer hooks fire in correct order."""

    def test_observer_hooks_fire(self):
        src = _make_source("key", "value", {"key": ["a"], "value": [1]})
        pf = PythonPacketFunction(double_value, output_keys="result")
        pod = FunctionPod(pf)

        pipeline = Pipeline(name="obs", pipeline_database=InMemoryArrowDatabase())
        with pipeline:
            pod(src, label="doubler")

        events = []

        class RecordingObserver:
            def on_node_start(self, node):
                events.append(("node_start", node.node_type))

            def on_node_end(self, node):
                events.append(("node_end", node.node_type))

            def on_packet_start(self, node, tag, packet):
                events.append(("packet_start",))

            def on_packet_end(self, node, tag, input_pkt, output_pkt, cached):
                events.append(("packet_end", cached))

        orch = SyncPipelineOrchestrator(observer=RecordingObserver())
        orch.run(pipeline._node_graph)

        assert events[0] == ("node_start", "source")
        assert events[1] == ("node_end", "source")
        assert events[2] == ("node_start", "function")
        assert events[3] == ("packet_start",)
        assert events[4] == ("packet_end", False)
        assert events[5] == ("node_end", "function")


class TestSyncOrchestratorUnknownNodeType:
    """Unknown node types raise TypeError."""

    def test_raises_on_unknown_node_type(self):
        import networkx as nx

        class FakeNode:
            node_type = "unknown"

        G = nx.DiGraph()
        G.add_node(FakeNode())

        orch = SyncPipelineOrchestrator()
        with pytest.raises(TypeError, match="Unknown node type"):
            orch.run(G)


class TestPipelineRunIntegration:
    """Pipeline.run() with orchestrator parameter."""

    def test_default_run_uses_sync_orchestrator(self):
        """Pipeline.run() without args should use SyncPipelineOrchestrator."""
        src = _make_source("key", "value", {"key": ["a", "b"], "value": [1, 2]})
        pf = PythonPacketFunction(double_value, output_keys="result")
        pod = FunctionPod(pf)

        pipeline = Pipeline(name="default", pipeline_database=InMemoryArrowDatabase())
        with pipeline:
            pod(src, label="doubler")

        pipeline.run()

        records = pipeline.doubler.get_all_records()
        assert records is not None
        assert records.num_rows == 2
        values = sorted(records.column("result").to_pylist())
        assert values == [2, 4]

    def test_run_with_explicit_orchestrator(self):
        """Pipeline.run(orchestrator=...) uses the provided orchestrator."""
        src = _make_source("key", "value", {"key": ["a", "b"], "value": [1, 2]})
        pf = PythonPacketFunction(double_value, output_keys="result")
        pod = FunctionPod(pf)

        pipeline = Pipeline(name="explicit", pipeline_database=InMemoryArrowDatabase())
        with pipeline:
            pod(src, label="doubler")

        events = []

        class RecordingObserver:
            def on_node_start(self, node):
                events.append(("node_start", node.node_type))

            def on_node_end(self, node):
                events.append(("node_end", node.node_type))

            def on_packet_start(self, node, tag, packet):
                events.append(("packet_start",))

            def on_packet_end(self, node, tag, input_pkt, output_pkt, cached):
                events.append(("packet_end",))

        orch = SyncPipelineOrchestrator(observer=RecordingObserver())
        pipeline.run(orchestrator=orch)

        assert len(events) > 0
        records = pipeline.doubler.get_all_records()
        assert records is not None

    def test_run_populates_node_caches(self):
        """After run(), iter_packets()/as_table() should work on nodes."""
        src = _make_source("key", "value", {"key": ["a", "b"], "value": [1, 2]})
        pf = PythonPacketFunction(double_value, output_keys="result")
        pod = FunctionPod(pf)

        pipeline = Pipeline(name="cache", pipeline_database=InMemoryArrowDatabase())
        with pipeline:
            pod(src, label="doubler")

        pipeline.run()

        table = pipeline.doubler.as_table()
        assert table.num_rows == 2


class TestSyncAsyncParity:
    """Sync orchestrator should produce same DB results as async."""

    def test_linear_pipeline_parity(self):
        src = _make_source("key", "value", {"key": ["a", "b"], "value": [10, 20]})
        pf = PythonPacketFunction(double_value, output_keys="result")
        pod = FunctionPod(pf)

        # Sync via orchestrator
        sync_pipeline = Pipeline(name="sync", pipeline_database=InMemoryArrowDatabase())
        with sync_pipeline:
            pod(src, label="doubler")
        sync_pipeline.run()
        sync_records = sync_pipeline.doubler.get_all_records()
        sync_values = sorted(sync_records.column("result").to_pylist())

        # Async
        from orcapod.pipeline import AsyncPipelineOrchestrator

        async_pipeline = Pipeline(
            name="async", pipeline_database=InMemoryArrowDatabase()
        )
        with async_pipeline:
            pod(src, label="doubler")
        AsyncPipelineOrchestrator().run(async_pipeline)
        async_records = async_pipeline.doubler.get_all_records()
        async_values = sorted(async_records.column("result").to_pylist())

        assert sync_values == async_values

    def test_diamond_pipeline_parity(self):
        src_a = _make_source("key", "value", {"key": ["a", "b"], "value": [10, 20]})
        src_b = _make_source("key", "score", {"key": ["a", "b"], "score": [100, 200]})
        pf = PythonPacketFunction(add_values, output_keys="total")
        pod = FunctionPod(pf)

        sync_pipeline = Pipeline(
            name="sync_d", pipeline_database=InMemoryArrowDatabase()
        )
        with sync_pipeline:
            joined = Join()(src_a, src_b, label="join")
            pod(joined, label="adder")
        sync_pipeline.run()
        sync_values = sorted(
            sync_pipeline.adder.get_all_records().column("total").to_pylist()
        )

        from orcapod.pipeline import AsyncPipelineOrchestrator

        async_pipeline = Pipeline(
            name="async_d", pipeline_database=InMemoryArrowDatabase()
        )
        with async_pipeline:
            joined = Join()(src_a, src_b, label="join")
            pod(joined, label="adder")
        AsyncPipelineOrchestrator().run(async_pipeline)
        async_values = sorted(
            async_pipeline.adder.get_all_records().column("total").to_pylist()
        )

        assert sync_values == async_values
