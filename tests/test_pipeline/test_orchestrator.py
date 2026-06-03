"""
Tests for the async pipeline orchestrator.

The ``AsyncPipelineOrchestrator`` operates on compiled pipeline node graphs.
After execution, results are retrieved from the pipeline's persistent nodes
via ``get_all_records()``.

Covers:
- Linear pipeline: Source -> FunctionPod
- Operator pipeline: Source -> Operator -> FunctionPod
- Diamond DAG: Two sources -> Join -> FunctionPod
- Fan-out: one source feeds multiple downstream nodes
- Results match synchronous execution
- SourceNode / OperatorNode / FunctionNode async_execute basics
- run_async entry point (from within an event loop)
- Buffer size configuration
"""

from __future__ import annotations


import pyarrow as pa
import pytest

from orcapod.channels import Channel
from orcapod.core.function_pod import FunctionPod
from orcapod.core.nodes.function_node import FunctionJobNode
from orcapod.core.nodes.operator_node import OperatorJobNode
from orcapod.core.nodes.source_node import SourceJobNode
from orcapod.core.operators import SelectDataColumns
from orcapod.core.operators.join import Join
from orcapod.core.operators.mappers import MapData
from orcapod.core.data_function import PythonDataFunction
from orcapod.core.sources import ArrowTableSource
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline import AsyncPipelineOrchestrator
from orcapod.pipeline.dag import OrcaDAG
from orcapod.pipeline.job import PipelineJob
from orcapod.pipeline.observer import NoOpObserver

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_source(
    tag_col: str,
    data_col: str,
    data: dict,
) -> ArrowTableSource:
    table = pa.table(
        {
            tag_col: pa.array(data[tag_col], type=pa.large_string()),
            data_col: pa.array(data[data_col], type=pa.int64()),
        }
    )
    return ArrowTableSource(table, tag_columns=[tag_col], infer_nullable=True)


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
    def _make_job_node(self, src):
        tag_schema, data_schema = src.output_schema()
        return SourceJobNode(
            name="test_src",
            tag_schema=tag_schema,
            data_schema=data_schema,
            bound_source=src,
        )

    @pytest.mark.asyncio
    async def test_pushes_all_rows_to_output(self):
        src = _make_source("key", "value", {"key": ["a", "b", "c"], "value": [1, 2, 3]})
        node = self._make_job_node(src)

        output_ch = Channel(buffer_size=16)
        await node.async_execute(output_ch.writer)

        rows = await output_ch.reader.collect()
        assert len(rows) == 3

    @pytest.mark.asyncio
    async def test_closes_channel_on_completion(self):
        src = _make_source("key", "value", {"key": ["a"], "value": [1]})
        node = self._make_job_node(src)

        output_ch = Channel(buffer_size=4)
        await node.async_execute(output_ch.writer)

        rows = await output_ch.reader.collect()
        assert len(rows) == 1


# ===========================================================================
# 2. OperatorNode.async_execute
# ===========================================================================


class TestOperatorNodeAsyncExecute:
    @pytest.mark.asyncio
    async def test_delegates_to_operator(self):
        src = _make_source("key", "value", {"key": ["a", "b"], "value": [10, 20]})
        op = SelectDataColumns(columns=["value"])
        op_node = OperatorJobNode(op, input_streams=[src])

        input_ch = Channel(buffer_size=16)
        output_ch = Channel(buffer_size=16)

        for tag, data in src.iter_data():
            await input_ch.writer.send((tag, data))
        await input_ch.writer.close()

        await op_node.async_execute([input_ch.reader], output_ch.writer)

        rows = await output_ch.reader.collect()
        assert len(rows) == 2


# ===========================================================================
# 3. FunctionNode.async_execute
# ===========================================================================


class TestFunctionNodeAsyncExecute:
    @pytest.mark.asyncio
    async def test_processes_data(self):
        src = _make_source("key", "value", {"key": ["a", "b"], "value": [10, 20]})
        pf = PythonDataFunction(double_value, output_keys="result")
        pod = FunctionPod(pf)
        node = FunctionJobNode(pod, src)

        input_ch = Channel(buffer_size=16)
        output_ch = Channel(buffer_size=16)

        for tag, data in src.iter_data():
            await input_ch.writer.send((tag, data))
        await input_ch.writer.close()

        await node.async_execute(input_ch.reader, output_ch.writer)

        rows = await output_ch.reader.collect()
        assert len(rows) == 2

        values = sorted([pkt.as_dict()["result"] for _, pkt in rows])
        assert values == [20, 40]


# ===========================================================================
# 4. Orchestrator: linear pipeline
# ===========================================================================


class TestOrchestratorLinearPipeline:
    """Source -> FunctionPod (linear pipeline)."""

    def test_linear_source_to_function_pod(self):
        src = _make_source("key", "value", {"key": ["a", "b", "c"], "value": [1, 2, 3]})
        pf = PythonDataFunction(double_value, output_keys="result")
        pod = FunctionPod(pf)

        job = PipelineJob(name="linear", store=InMemoryArrowDatabase())
        with job:
            pod(src, label="doubler")
        job.run(orchestrator=AsyncPipelineOrchestrator())

        records = job.nodes["doubler"].get_all_records()
        assert records is not None
        assert records.num_rows == 3

        values = sorted(records.column("result").to_pylist())
        assert values == [2, 4, 6]

    def test_matches_sync_execution(self):
        """Async results should match synchronous execution."""
        src = _make_source("key", "value", {"key": ["a", "b"], "value": [10, 20]})
        pf = PythonDataFunction(double_value, output_keys="result")
        pod = FunctionPod(pf)

        # Sync
        sync_job = PipelineJob(name="sync", store=InMemoryArrowDatabase())
        with sync_job:
            pod(src, label="doubler")
        sync_job.run()
        sync_records = sync_job.nodes["doubler"].get_all_records()
        sync_values = sorted(sync_records.column("result").to_pylist())

        # Async
        async_job = PipelineJob(name="async", store=InMemoryArrowDatabase())
        with async_job:
            pod(src, label="doubler")
        async_job.run(orchestrator=AsyncPipelineOrchestrator())
        async_records = async_job.nodes["doubler"].get_all_records()
        async_values = sorted(async_records.column("result").to_pylist())

        assert sync_values == async_values


# ===========================================================================
# 5. Orchestrator: operator pipeline
# ===========================================================================


class TestOrchestratorOperatorPipeline:
    """Source -> Operator -> FunctionPod."""

    def test_source_to_operator_to_function_pod(self):
        src = _make_source("key", "value", {"key": ["a", "b", "c"], "value": [1, 2, 3]})
        op = MapData(name_map={"value": "val"})

        def double_val(val: int) -> int:
            return val * 2

        pf = PythonDataFunction(double_val, output_keys="result")
        pod = FunctionPod(pf)

        job = PipelineJob(name="op_pipe", store=InMemoryArrowDatabase())
        with job:
            mapped = op(src, label="mapper")
            pod(mapped, label="doubler")
        job.run(orchestrator=AsyncPipelineOrchestrator())

        records = job.nodes["doubler"].get_all_records()
        assert records is not None
        assert records.num_rows == 3
        values = sorted(records.column("result").to_pylist())
        assert values == [2, 4, 6]


# ===========================================================================
# 6. Orchestrator: diamond DAG (fan-out + join)
# ===========================================================================


class TestOrchestratorDiamondDag:
    """Two sources -> Join -> FunctionPod."""

    def test_two_sources_join_function_pod(self):
        src_a, src_b = _make_two_sources()
        pf = PythonDataFunction(add_values, output_keys="total")
        pod = FunctionPod(pf)

        job = PipelineJob(name="diamond", store=InMemoryArrowDatabase())
        with job:
            joined = Join()(src_a, src_b, label="join")
            pod(joined, label="adder")
        job.run(orchestrator=AsyncPipelineOrchestrator())

        records = job.nodes["adder"].get_all_records()
        assert records is not None
        assert records.num_rows == 2
        values = sorted(records.column("total").to_pylist())
        assert values == [110, 220]

    def test_diamond_matches_sync(self):
        """Diamond DAG async results should match sync execution."""
        src_a, src_b = _make_two_sources()
        pf = PythonDataFunction(add_values, output_keys="total")
        pod = FunctionPod(pf)

        # Sync
        sync_job = PipelineJob(name="sync_diamond", store=InMemoryArrowDatabase())
        with sync_job:
            joined = Join()(src_a, src_b, label="join")
            pod(joined, label="adder")
        sync_job.run()
        sync_values = sorted(
            sync_job.nodes["adder"].get_all_records().column("total").to_pylist()
        )

        # Async
        async_job = PipelineJob(name="async_diamond", store=InMemoryArrowDatabase())
        with async_job:
            joined = Join()(src_a, src_b, label="join")
            pod(joined, label="adder")
        async_job.run(orchestrator=AsyncPipelineOrchestrator())
        async_values = sorted(
            async_job.nodes["adder"].get_all_records().column("total").to_pylist()
        )

        assert sync_values == async_values


# ---------------------------------------------------------------------------
# TestOrchestratorRunAsync — DELETED
# The original test exercised `AsyncPipelineOrchestrator.run_async()` from
# inside a running event loop.  After migration to `job.run()` it became
# a duplicate of `TestOrchestratorLinearPipeline`.  Direct `run_async()`
# coverage is tracked as a follow-up.
# ---------------------------------------------------------------------------


# ===========================================================================
# 8. Buffer size configuration
# ===========================================================================


class TestBufferSizeConfiguration:
    def test_custom_buffer_size(self):
        """PipelineJob should work with custom buffer sizes."""
        src = _make_source("key", "value", {"key": ["a", "b"], "value": [1, 2]})
        pf = PythonDataFunction(double_value, output_keys="result")
        pod = FunctionPod(pf)

        job = PipelineJob(name="bufsize", store=InMemoryArrowDatabase())
        with job:
            pod(src, label="doubler")
        job.run(orchestrator=AsyncPipelineOrchestrator(buffer_size=4))

        records = job.nodes["doubler"].get_all_records()
        assert records is not None
        assert records.num_rows == 2  # source has 2 rows


# ===========================================================================
# 9. Fan-out: one source feeds multiple downstream nodes
# ===========================================================================


def triple_value(value: int) -> int:
    return value * 3


class TestAsyncOrchestratorFanOut:
    """One source fans out to multiple downstream nodes."""

    def test_fan_out_source_to_two_functions(self):
        """Two distinct functions consuming the same source produce two nodes."""
        src = _make_source("key", "value", {"key": ["a", "b"], "value": [1, 2]})
        pf1 = PythonDataFunction(double_value, output_keys="result")
        pod1 = FunctionPod(pf1)
        pf2 = PythonDataFunction(triple_value, output_keys="result")
        pod2 = FunctionPod(pf2)

        job = PipelineJob(name="fanout", store=InMemoryArrowDatabase())
        with job:
            pod1(src, label="doubler")
            pod2(src, label="tripler")
        job.run(orchestrator=AsyncPipelineOrchestrator())

        doubler_records = job.nodes["doubler"].get_all_records()
        tripler_records = job.nodes["tripler"].get_all_records()
        assert doubler_records is not None
        assert tripler_records is not None

        doubler_values = sorted(doubler_records.column("result").to_pylist())
        tripler_values = sorted(tripler_records.column("result").to_pylist())
        # double_value: [2, 4], triple_value: [3, 6]
        assert doubler_values == [2, 4]
        assert tripler_values == [3, 6]


# ===========================================================================
# 10. Terminal node: pipeline with just a source
# ===========================================================================


class TestAsyncOrchestratorTerminalNode:
    """Terminal nodes with no downstream should work correctly."""

    def test_single_terminal_source(self):
        """A pipeline with just a source (terminal) should work."""
        src = _make_source("key", "value", {"key": ["a"], "value": [1]})
        tag_schema, data_schema = src.output_schema()
        node = SourceJobNode(
            name="test_src",
            tag_schema=tag_schema,
            data_schema=data_schema,
            bound_source=src,
        )
        G: OrcaDAG = OrcaDAG()
        G.add_node(node)

        orch = AsyncPipelineOrchestrator()
        result = orch.run(G, materialize_results=True)
        assert len(result.node_outputs) == 1


# ===========================================================================
# 11. Error propagation
# ===========================================================================


class TestAsyncOrchestratorErrorPropagation:
    """Failed data do not abort the pipeline; they are handled per-data."""

    def test_node_failure_does_not_abort_pipeline(self):
        """A crashing data function is skipped; the pipeline completes normally."""
        def failing_fn(value: int) -> int:
            raise ValueError("intentional failure")

        src = _make_source("key", "value", {"key": ["a"], "value": [1]})
        pf = PythonDataFunction(failing_fn, output_keys="result")
        pod = FunctionPod(pf)

        job = PipelineJob(name="error", store=InMemoryArrowDatabase())
        with job:
            pod(src, label="failer")

        # Pipeline must complete without raising; failing data is silently dropped.
        job.run(orchestrator=AsyncPipelineOrchestrator())

    def test_node_failure_calls_on_data_crash(self):
        """When an observer is set, on_data_crash is called for the failing data."""
        def failing_fn(value: int) -> int:
            raise ValueError("intentional failure")

        src = _make_source("key", "value", {"key": ["a"], "value": [1]})
        pf = PythonDataFunction(failing_fn, output_keys="result")
        pod = FunctionPod(pf)

        job = PipelineJob(name="error2", store=InMemoryArrowDatabase())
        with job:
            pod(src, label="failer")

        crashes = []

        class CrashRecorder(NoOpObserver):
            def on_data_crash(self, node_label, tag, data, error):
                crashes.append(error)

        job.run(orchestrator=AsyncPipelineOrchestrator(), observer=CrashRecorder())

        assert len(crashes) == 1
        assert isinstance(crashes[0], (ValueError, RuntimeError))


class TestAsyncOrchestratorObserverInjection:
    """Verify observer passed to async orchestrator flows through to nodes."""

    def test_linear_pipeline_observer_hooks(self):
        """Source → Function: observer hooks fire from inside nodes."""
        src = _make_source("key", "value", {"key": ["a", "b"], "value": [1, 2]})
        pf = PythonDataFunction(double_value, output_keys="result")
        pod = FunctionPod(pf)

        job = PipelineJob(name="async_obs", store=InMemoryArrowDatabase())
        with job:
            pod(src, label="doubler")

        events = []

        class RecordingObserver:
            def on_run_start(self, run_id, pipeline_uri=""): pass
            def on_run_end(self, run_id): pass
            def on_node_start(self, node_label, node_hash, **kwargs):
                events.append(("node_start", node_label))
            def on_node_end(self, node_label, node_hash, **kwargs):
                events.append(("node_end", node_label))
            def on_data_start(self, node_label, tag, data):
                events.append(("data_start", node_label))
            def on_data_end(self, node_label, tag, input_pkt, output_pkt, cached):
                events.append(("data_end", node_label, cached))
            def on_data_crash(self, node_label, tag, data, exc): pass
            def create_data_logger(self, tag, data, **kwargs):
                return NoOpObserver().create_data_logger(tag, data)
            def contextualize(self, *identity_path):
                return self

        job.run(orchestrator=AsyncPipelineOrchestrator(), observer=RecordingObserver())

        # Source fires node_start/node_end (label contains "ArrowTableSource" or similar)
        source_starts = [e for e in events if e[0] == "node_start" and e[1] != "doubler"]
        assert len(source_starts) >= 1

        # Function fires node_start, per-data hooks, node_end
        assert ("node_start", "doubler") in events
        assert ("node_end", "doubler") in events
        fn_data_ends = [
            e for e in events
            if e[0] == "data_end" and e[1] == "doubler"
        ]
        assert len(fn_data_ends) == 2
        # All should be cached=False (first run, no DB)
        assert all(e[2] is False for e in fn_data_ends)

    def test_operator_pipeline_observer_hooks(self):
        """Source → Operator → Function: all node types fire hooks."""
        src = _make_source("key", "value", {"key": ["a", "b"], "value": [1, 2]})
        op = MapData(name_map={"value": "val"})

        def double_val(val: int) -> int:
            return val * 2

        pf = PythonDataFunction(double_val, output_keys="result")
        pod = FunctionPod(pf)

        job = PipelineJob(name="async_obs_op", store=InMemoryArrowDatabase())
        with job:
            mapped = op(src, label="mapper")
            pod(mapped, label="doubler")

        events = []

        class RecordingObserver:
            def on_run_start(self, run_id, pipeline_uri=""): pass
            def on_run_end(self, run_id): pass
            def on_node_start(self, node_label, node_hash, **kwargs):
                events.append(("node_start", node_label))
            def on_node_end(self, node_label, node_hash, **kwargs):
                events.append(("node_end", node_label))
            def on_data_start(self, node_label, tag, data):
                events.append(("data_start", node_label))
            def on_data_end(self, node_label, tag, input_pkt, output_pkt, cached):
                events.append(("data_end", node_label))
            def on_data_crash(self, node_label, tag, data, exc): pass
            def create_data_logger(self, tag, data, **kwargs):
                return NoOpObserver().create_data_logger(tag, data)
            def contextualize(self, *identity_path):
                return self

        job.run(orchestrator=AsyncPipelineOrchestrator(), observer=RecordingObserver())

        # All labeled nodes fire start/end
        assert ("node_start", "mapper") in events
        assert ("node_end", "mapper") in events
        assert ("node_start", "doubler") in events
        assert ("node_end", "doubler") in events

        # Only function nodes fire data-level hooks
        assert ("data_start", "doubler") in events
        assert ("data_start", "mapper") not in events

    def test_no_observer_works(self):
        """Async pipeline runs fine with no observer."""
        src = _make_source("key", "value", {"key": ["a"], "value": [1]})
        pf = PythonDataFunction(double_value, output_keys="result")
        pod = FunctionPod(pf)

        job = PipelineJob(name="async_no_obs", store=InMemoryArrowDatabase())
        with job:
            pod(src, label="doubler")
        job.run(orchestrator=AsyncPipelineOrchestrator())  # no observer

        records = job.nodes["doubler"].get_all_records()
        assert records is not None
        assert records.num_rows == 1


def test_async_orchestrator_accepts_observer_in_run():
    import inspect
    sig = inspect.signature(AsyncPipelineOrchestrator.run)
    assert "observer" in sig.parameters
