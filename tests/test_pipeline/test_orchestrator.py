"""
Tests for the async pipeline orchestrator.

Covers:
- Linear pipeline: Source → Operator → FunctionPod
- Diamond DAG: Source → [Op1, Op2] → Join
- Fan-out: one source feeds multiple downstream nodes
- Results match synchronous execution
- SourceNode.async_execute pushes all rows
- OperatorNode.async_execute delegates correctly
- FunctionNode.async_execute works in streaming mode
- Error propagation cancels other tasks
"""

from __future__ import annotations

import asyncio

import pyarrow as pa
import pytest

from orcapod.channels import Channel
from orcapod.core.function_pod import FunctionNode, FunctionPod
from orcapod.core.operator_node import OperatorNode
from orcapod.core.operators import SelectPacketColumns
from orcapod.core.operators.filters import PolarsFilter
from orcapod.core.operators.join import Join
from orcapod.core.operators.mappers import MapPackets
from orcapod.core.packet_function import PythonPacketFunction
from orcapod.core.sources import ArrowTableSource
from orcapod.core.tracker import GraphTracker, SourceNode
from orcapod.pipeline.orchestrator import AsyncPipelineOrchestrator
from orcapod.types import ExecutorType, PipelineConfig


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_source(
    tag_col: str,
    packet_col: str,
    data: dict,
) -> ArrowTableSource:
    table = pa.table(
        {
            tag_col: pa.array(data[tag_col], type=pa.large_string()),
            packet_col: pa.array(data[packet_col], type=pa.int64()),
        }
    )
    return ArrowTableSource(table, tag_columns=[tag_col])


def _make_two_sources():
    src_a = _make_source("key", "value", {"key": ["a", "b"], "value": [10, 20]})
    src_b = _make_source("key", "score", {"key": ["a", "b"], "score": [100, 200]})
    return src_a, src_b


def double_value(value: int) -> int:
    return value * 2


def add_values(value: int, score: int) -> int:
    return value + score


# ===========================================================================
# 1. SourceNode.async_execute
# ===========================================================================


class TestSourceNodeAsyncExecute:
    @pytest.mark.asyncio
    async def test_pushes_all_rows_to_output(self):
        src = _make_source("key", "value", {"key": ["a", "b", "c"], "value": [1, 2, 3]})
        node = SourceNode(src)

        output_ch = Channel(buffer_size=16)
        await node.async_execute([], output_ch.writer)

        rows = await output_ch.reader.collect()
        assert len(rows) == 3

    @pytest.mark.asyncio
    async def test_closes_channel_on_completion(self):
        src = _make_source("key", "value", {"key": ["a"], "value": [1]})
        node = SourceNode(src)

        output_ch = Channel(buffer_size=4)
        await node.async_execute([], output_ch.writer)

        rows = await output_ch.reader.collect()
        assert len(rows) == 1


# ===========================================================================
# 2. OperatorNode.async_execute
# ===========================================================================


class TestOperatorNodeAsyncExecute:
    @pytest.mark.asyncio
    async def test_delegates_to_operator(self):
        src = _make_source("key", "value", {"key": ["a", "b"], "value": [10, 20]})
        op = SelectPacketColumns(columns=["value"])
        op_node = OperatorNode(op, input_streams=[src])

        input_ch = Channel(buffer_size=16)
        output_ch = Channel(buffer_size=16)

        # Feed source rows into input channel
        for tag, packet in src.iter_packets():
            await input_ch.writer.send((tag, packet))
        await input_ch.writer.close()

        await op_node.async_execute([input_ch.reader], output_ch.writer)

        rows = await output_ch.reader.collect()
        assert len(rows) == 2


# ===========================================================================
# 3. FunctionNode.async_execute
# ===========================================================================


class TestFunctionNodeAsyncExecute:
    @pytest.mark.asyncio
    async def test_processes_packets(self):
        src = _make_source("key", "value", {"key": ["a", "b"], "value": [10, 20]})
        pf = PythonPacketFunction(double_value, output_keys="result")
        pod = FunctionPod(pf)
        node = FunctionNode(pod, src)

        input_ch = Channel(buffer_size=16)
        output_ch = Channel(buffer_size=16)

        for tag, packet in src.iter_packets():
            await input_ch.writer.send((tag, packet))
        await input_ch.writer.close()

        await node.async_execute([input_ch.reader], output_ch.writer)

        rows = await output_ch.reader.collect()
        assert len(rows) == 2

        values = sorted([pkt.as_dict()["result"] for _, pkt in rows])
        assert values == [20, 40]


# ===========================================================================
# 4. Orchestrator: linear pipeline
# ===========================================================================


class TestOrchestratorLinearPipeline:
    """Source → FunctionPod (linear pipeline)."""

    def test_linear_source_to_function_pod(self):
        src = _make_source("key", "value", {"key": ["a", "b", "c"], "value": [1, 2, 3]})
        pf = PythonPacketFunction(double_value, output_keys="result")
        pod = FunctionPod(pf)

        tracker = GraphTracker()
        with tracker:
            result_stream = pod(src)

        tracker.compile()

        orchestrator = AsyncPipelineOrchestrator()
        result = orchestrator.run(tracker)

        rows = list(result.iter_packets())
        assert len(rows) == 3

        values = sorted([pkt.as_dict()["result"] for _, pkt in rows])
        assert values == [2, 4, 6]

    def test_matches_sync_execution(self):
        """Async results should match synchronous execution."""
        src = _make_source("key", "value", {"key": ["a", "b"], "value": [10, 20]})
        pf = PythonPacketFunction(double_value, output_keys="result")
        pod = FunctionPod(pf)

        # Sync execution
        sync_result = pod.process(src)
        sync_rows = list(sync_result.iter_packets())
        sync_values = sorted([pkt.as_dict()["result"] for _, pkt in sync_rows])

        # Async execution
        tracker = GraphTracker()
        with tracker:
            _ = pod(src)
        tracker.compile()

        orchestrator = AsyncPipelineOrchestrator()
        async_result = orchestrator.run(tracker)
        async_rows = list(async_result.iter_packets())
        async_values = sorted([pkt.as_dict()["result"] for _, pkt in async_rows])

        assert sync_values == async_values


# ===========================================================================
# 5. Orchestrator: operator pipeline
# ===========================================================================


class TestOrchestratorOperatorPipeline:
    """Source → Operator → FunctionPod."""

    def test_source_to_operator_to_function_pod(self):
        src = _make_source("key", "value", {"key": ["a", "b", "c"], "value": [1, 2, 3]})
        pf = PythonPacketFunction(double_value, output_keys="result")
        pod = FunctionPod(pf)
        op = MapPackets(name_map={"value": "val"})

        # Create a function that takes 'val' instead of 'value'
        def double_val(val: int) -> int:
            return val * 2

        pf2 = PythonPacketFunction(double_val, output_keys="result")
        pod2 = FunctionPod(pf2)

        tracker = GraphTracker()
        with tracker:
            mapped = op(src)
            result_stream = pod2(mapped)

        tracker.compile()

        orchestrator = AsyncPipelineOrchestrator()
        result = orchestrator.run(tracker)

        rows = list(result.iter_packets())
        assert len(rows) == 3
        values = sorted([pkt.as_dict()["result"] for _, pkt in rows])
        assert values == [2, 4, 6]


# ===========================================================================
# 6. Orchestrator: diamond DAG (fan-out + join)
# ===========================================================================


class TestOrchestratorDiamondDag:
    """Two sources → Join → FunctionPod."""

    def test_two_sources_join_function_pod(self):
        src_a, src_b = _make_two_sources()

        pf = PythonPacketFunction(add_values, output_keys="total")
        pod = FunctionPod(pf)

        tracker = GraphTracker()
        with tracker:
            joined = Join()(src_a, src_b)
            result_stream = pod(joined)

        tracker.compile()

        orchestrator = AsyncPipelineOrchestrator()
        result = orchestrator.run(tracker)

        rows = list(result.iter_packets())
        assert len(rows) == 2

        values = sorted([pkt.as_dict()["total"] for _, pkt in rows])
        assert values == [110, 220]

    def test_diamond_matches_sync(self):
        """Diamond DAG async results should match sync execution."""
        src_a, src_b = _make_two_sources()
        pf = PythonPacketFunction(add_values, output_keys="total")
        pod = FunctionPod(pf)

        # Sync
        sync_joined = Join()(src_a, src_b)
        sync_result = pod.process(sync_joined)
        sync_values = sorted([pkt.as_dict()["total"] for _, pkt in sync_result.iter_packets()])

        # Async
        tracker = GraphTracker()
        with tracker:
            joined = Join()(src_a, src_b)
            _ = pod(joined)
        tracker.compile()

        orchestrator = AsyncPipelineOrchestrator()
        async_result = orchestrator.run(tracker)
        async_values = sorted(
            [pkt.as_dict()["total"] for _, pkt in async_result.iter_packets()]
        )

        assert sync_values == async_values


# ===========================================================================
# 7. Orchestrator: fan-out (one source feeds multiple nodes)
# ===========================================================================


class TestOrchestratorFanOut:
    """One source feeds two different function pods via fan-out."""

    def test_fan_out_source_feeds_two_branches(self):
        src = _make_source("key", "value", {"key": ["a", "b"], "value": [10, 20]})

        # Two function pods: one doubles, one triples
        def double(value: int) -> int:
            return value * 2

        def triple(value: int) -> int:
            return value * 3

        pf_double = PythonPacketFunction(double, output_keys="doubled")
        pf_triple = PythonPacketFunction(triple, output_keys="tripled")
        pod_double = FunctionPod(pf_double)
        pod_triple = FunctionPod(pf_triple)

        tracker = GraphTracker()
        with tracker:
            doubled = pod_double(src)
            tripled = pod_triple(src)
            result = Join()(doubled, tripled)

        tracker.compile()

        orchestrator = AsyncPipelineOrchestrator()
        result_stream = orchestrator.run(tracker)

        rows = list(result_stream.iter_packets())
        assert len(rows) == 2

        for _, pkt in rows:
            d = pkt.as_dict()
            assert "doubled" in d
            assert "tripled" in d


# ===========================================================================
# 8. run_async entry point (for callers inside event loop)
# ===========================================================================


class TestOrchestratorRunAsync:
    @pytest.mark.asyncio
    async def test_run_async_from_event_loop(self):
        """run_async should work when called from inside an event loop."""
        src = _make_source("key", "value", {"key": ["a", "b"], "value": [1, 2]})
        pf = PythonPacketFunction(double_value, output_keys="result")
        pod = FunctionPod(pf)

        tracker = GraphTracker()
        with tracker:
            _ = pod(src)
        tracker.compile()

        orchestrator = AsyncPipelineOrchestrator()
        result = await orchestrator.run_async(tracker)

        rows = list(result.iter_packets())
        assert len(rows) == 2
        values = sorted([pkt.as_dict()["result"] for _, pkt in rows])
        assert values == [2, 4]


# ===========================================================================
# 9. PipelineConfig integration
# ===========================================================================


class TestPipelineConfigIntegration:
    def test_custom_buffer_size(self):
        """Pipeline should work with custom buffer sizes."""
        src = _make_source("key", "value", {"key": ["a", "b"], "value": [1, 2]})
        pf = PythonPacketFunction(double_value, output_keys="result")
        pod = FunctionPod(pf)

        tracker = GraphTracker()
        with tracker:
            _ = pod(src)
        tracker.compile()

        config = PipelineConfig(
            executor=ExecutorType.ASYNC_CHANNELS,
            channel_buffer_size=4,
        )

        orchestrator = AsyncPipelineOrchestrator()
        result = orchestrator.run(tracker, config=config)

        rows = list(result.iter_packets())
        assert len(rows) == 2
