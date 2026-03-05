"""
Tests for the async pipeline orchestrator.

The ``AsyncPipelineOrchestrator`` operates on compiled ``Pipeline``
objects.  After execution, results are retrieved from the pipeline's
persistent nodes via ``get_all_records()``.

Covers:
- Linear pipeline: Source → FunctionPod
- Operator pipeline: Source → Operator → FunctionPod
- Diamond DAG: Two sources → Join → FunctionPod
- Fan-out: one source feeds multiple downstream nodes
- Results match synchronous execution
- SourceNode / OperatorNode / FunctionNode async_execute basics
- run_async entry point (from within an event loop)
- PipelineConfig integration (custom buffer sizes)
"""

from __future__ import annotations

import asyncio

import pyarrow as pa
import pytest

from orcapod.channels import Channel
from orcapod.core.function_pod import FunctionNode, FunctionPod
from orcapod.core.operator_node import OperatorNode
from orcapod.core.operators import SelectPacketColumns
from orcapod.core.operators.join import Join
from orcapod.core.operators.mappers import MapPackets
from orcapod.core.packet_function import PythonPacketFunction
from orcapod.core.sources import ArrowTableSource
from orcapod.core.tracker import SourceNode
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline import AsyncPipelineOrchestrator, Pipeline
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

        pipeline = Pipeline(name="linear", pipeline_database=InMemoryArrowDatabase())
        with pipeline:
            pod(src, label="doubler")

        orchestrator = AsyncPipelineOrchestrator()
        orchestrator.run(pipeline)

        records = pipeline.doubler.get_all_records()
        assert records is not None
        assert records.num_rows == 3

        values = sorted(records.column("result").to_pylist())
        assert values == [2, 4, 6]

    def test_matches_sync_execution(self):
        """Async results should match synchronous execution."""
        src = _make_source("key", "value", {"key": ["a", "b"], "value": [10, 20]})
        pf = PythonPacketFunction(double_value, output_keys="result")
        pod = FunctionPod(pf)

        # Sync
        sync_pipeline = Pipeline(
            name="sync", pipeline_database=InMemoryArrowDatabase()
        )
        with sync_pipeline:
            pod(src, label="doubler")
        sync_pipeline.run()
        sync_records = sync_pipeline.doubler.get_all_records()
        sync_values = sorted(sync_records.column("result").to_pylist())

        # Async
        async_pipeline = Pipeline(
            name="async", pipeline_database=InMemoryArrowDatabase()
        )
        with async_pipeline:
            pod(src, label="doubler")
        orchestrator = AsyncPipelineOrchestrator()
        orchestrator.run(async_pipeline)
        async_records = async_pipeline.doubler.get_all_records()
        async_values = sorted(async_records.column("result").to_pylist())

        assert sync_values == async_values


# ===========================================================================
# 5. Orchestrator: operator pipeline
# ===========================================================================


class TestOrchestratorOperatorPipeline:
    """Source → Operator → FunctionPod."""

    def test_source_to_operator_to_function_pod(self):
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

        orchestrator = AsyncPipelineOrchestrator()
        orchestrator.run(pipeline)

        records = pipeline.doubler.get_all_records()
        assert records is not None
        assert records.num_rows == 3
        values = sorted(records.column("result").to_pylist())
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

        pipeline = Pipeline(name="diamond", pipeline_database=InMemoryArrowDatabase())
        with pipeline:
            joined = Join()(src_a, src_b, label="join")
            pod(joined, label="adder")

        orchestrator = AsyncPipelineOrchestrator()
        orchestrator.run(pipeline)

        records = pipeline.adder.get_all_records()
        assert records is not None
        assert records.num_rows == 2
        values = sorted(records.column("total").to_pylist())
        assert values == [110, 220]

    def test_diamond_matches_sync(self):
        """Diamond DAG async results should match sync execution."""
        src_a, src_b = _make_two_sources()
        pf = PythonPacketFunction(add_values, output_keys="total")
        pod = FunctionPod(pf)

        # Sync
        sync_pipeline = Pipeline(
            name="sync_diamond", pipeline_database=InMemoryArrowDatabase()
        )
        with sync_pipeline:
            joined = Join()(src_a, src_b, label="join")
            pod(joined, label="adder")
        sync_pipeline.run()
        sync_values = sorted(
            sync_pipeline.adder.get_all_records().column("total").to_pylist()
        )

        # Async
        async_pipeline = Pipeline(
            name="async_diamond", pipeline_database=InMemoryArrowDatabase()
        )
        with async_pipeline:
            joined = Join()(src_a, src_b, label="join")
            pod(joined, label="adder")
        orchestrator = AsyncPipelineOrchestrator()
        orchestrator.run(async_pipeline)
        async_values = sorted(
            async_pipeline.adder.get_all_records().column("total").to_pylist()
        )

        assert sync_values == async_values


# ===========================================================================
# 7. run_async entry point (for callers inside event loop)
# ===========================================================================


class TestOrchestratorRunAsync:
    @pytest.mark.asyncio
    async def test_run_async_from_event_loop(self):
        """run_async should work when called from inside an event loop."""
        src = _make_source("key", "value", {"key": ["a", "b"], "value": [1, 2]})
        pf = PythonPacketFunction(double_value, output_keys="result")
        pod = FunctionPod(pf)

        pipeline = Pipeline(
            name="async_loop", pipeline_database=InMemoryArrowDatabase()
        )
        with pipeline:
            pod(src, label="doubler")

        orchestrator = AsyncPipelineOrchestrator()
        await orchestrator.run_async(pipeline)

        records = pipeline.doubler.get_all_records()
        assert records is not None
        values = sorted(records.column("result").to_pylist())
        assert values == [2, 4]


# ===========================================================================
# 8. PipelineConfig integration
# ===========================================================================


class TestPipelineConfigIntegration:
    def test_custom_buffer_size(self):
        """Pipeline should work with custom buffer sizes."""
        src = _make_source("key", "value", {"key": ["a", "b"], "value": [1, 2]})
        pf = PythonPacketFunction(double_value, output_keys="result")
        pod = FunctionPod(pf)

        pipeline = Pipeline(
            name="bufsize", pipeline_database=InMemoryArrowDatabase()
        )
        with pipeline:
            pod(src, label="doubler")

        config = PipelineConfig(
            executor=ExecutorType.ASYNC_CHANNELS,
            channel_buffer_size=4,
        )

        orchestrator = AsyncPipelineOrchestrator()
        orchestrator.run(pipeline, config=config)

        records = pipeline.doubler.get_all_records()
        assert records is not None
        assert records.num_rows == 2
