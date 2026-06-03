"""Tests for the synchronous pipeline orchestrator."""

from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.core.function_pod import FunctionPod
from orcapod.core.operators import SelectDataColumns
from orcapod.core.operators.join import Join
from orcapod.core.operators.mappers import MapData
from orcapod.core.data_function import PythonDataFunction
from orcapod.core.sources import ArrowTableSource
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline import AsyncPipelineOrchestrator
from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator
from orcapod.pipeline.job import PipelineJob


def _make_source(tag_col, data_col, data):
    table = pa.table(
        {
            tag_col: pa.array(data[tag_col], type=pa.large_string()),
            data_col: pa.array(data[data_col], type=pa.int64()),
        }
    )
    return ArrowTableSource(table, tag_columns=[tag_col], infer_nullable=True)


def double_value(value: int) -> int:
    return value * 2


def add_values(value: int, score: int) -> int:
    return value + score


class TestSyncOrchestratorLinear:
    """Source -> FunctionPod."""

    def test_linear_pipeline(self):
        src = _make_source("key", "value", {"key": ["a", "b", "c"], "value": [1, 2, 3]})
        pf = PythonDataFunction(double_value, output_keys="result")
        pod = FunctionPod(pf)

        job = PipelineJob(name="linear", store=InMemoryArrowDatabase())
        with job:
            pod(src, label="doubler")

        job.run()

        records = job.nodes["doubler"].get_all_records()
        assert records is not None
        assert records.num_rows == 3
        values = sorted(records.column("result").to_pylist())
        assert values == [2, 4, 6]


class TestSyncOrchestratorWithOperator:
    """Source -> Operator -> FunctionPod."""

    def test_operator_pipeline(self):
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

        job.run()

        records = job.nodes["doubler"].get_all_records()
        assert records is not None
        values = sorted(records.column("result").to_pylist())
        assert values == [2, 4, 6]


class TestSyncOrchestratorDiamond:
    """Two sources -> Join -> FunctionPod."""

    def test_diamond_dag(self):
        src_a = _make_source("key", "value", {"key": ["a", "b"], "value": [10, 20]})
        src_b = _make_source("key", "score", {"key": ["a", "b"], "score": [100, 200]})
        pf = PythonDataFunction(add_values, output_keys="total")
        pod = FunctionPod(pf)

        job = PipelineJob(name="diamond", store=InMemoryArrowDatabase())
        with job:
            joined = Join()(src_a, src_b, label="join")
            pod(joined, label="adder")

        job.run()

        records = job.nodes["adder"].get_all_records()
        assert records is not None
        values = sorted(records.column("total").to_pylist())
        assert values == [110, 220]


class TestSyncOrchestratorObserver:
    """Observer hooks fire in correct order."""

    def test_observer_hooks_fire(self):
        src = _make_source("key", "value", {"key": ["a"], "value": [1]})
        pf = PythonDataFunction(double_value, output_keys="result")
        pod = FunctionPod(pf)

        job = PipelineJob(name="obs", store=InMemoryArrowDatabase())
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
                events.append(("data_start",))
            def on_data_end(self, node_label, tag, input_pkt, output_pkt, cached):
                events.append(("data_end", cached))
            def on_data_crash(self, node_label, tag, data, exc): pass
            def create_data_logger(self, tag, data, **kwargs):
                from orcapod.pipeline.observer import _NOOP_LOGGER
                return _NOOP_LOGGER
            def contextualize(self, *identity_path):
                return self

        job.run(observer=RecordingObserver())

        # First two events are source node start/end
        assert events[0][0] == "node_start"
        assert events[1][0] == "node_end"
        # Then function node start, data hooks, function node end
        assert events[2] == ("node_start", "doubler")
        assert events[3] == ("data_start",)
        assert events[4] == ("data_end", False)
        assert events[5] == ("node_end", "doubler")


class TestSyncOrchestratorUnknownNodeType:
    """Unknown node types raise TypeError."""

    def test_raises_on_unknown_node_type(self):
        from orcapod.pipeline.dag import OrcaDAG

        class FakeNode:
            node_type = "unknown"

        G: OrcaDAG = OrcaDAG()
        G.add_node(FakeNode())

        orch = SyncPipelineOrchestrator()
        with pytest.raises(TypeError, match="Unknown node type"):
            orch.run(G)


class TestPipelineRunIntegration:
    """PipelineJob.run() with orchestrator parameter."""

    def test_default_run_uses_sync_orchestrator(self):
        """PipelineJob.run() without args should use SyncPipelineOrchestrator."""
        src = _make_source("key", "value", {"key": ["a", "b"], "value": [1, 2]})
        pf = PythonDataFunction(double_value, output_keys="result")
        pod = FunctionPod(pf)

        job = PipelineJob(name="default", store=InMemoryArrowDatabase())
        with job:
            pod(src, label="doubler")

        job.run()

        records = job.nodes["doubler"].get_all_records()
        assert records is not None
        assert records.num_rows == 2
        values = sorted(records.column("result").to_pylist())
        assert values == [2, 4]

    def test_run_with_explicit_orchestrator(self):
        """PipelineJob.run(orchestrator=...) uses the provided orchestrator."""
        src = _make_source("key", "value", {"key": ["a", "b"], "value": [1, 2]})
        pf = PythonDataFunction(double_value, output_keys="result")
        pod = FunctionPod(pf)

        job = PipelineJob(name="explicit", store=InMemoryArrowDatabase())
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
                events.append(("data_start",))
            def on_data_end(self, node_label, tag, input_pkt, output_pkt, cached):
                events.append(("data_end",))
            def on_data_crash(self, node_label, tag, data, exc): pass
            def create_data_logger(self, tag, data, **kwargs):
                from orcapod.pipeline.observer import _NOOP_LOGGER
                return _NOOP_LOGGER
            def contextualize(self, *identity_path):
                return self

        orch = SyncPipelineOrchestrator()
        job.run(orchestrator=orch, observer=RecordingObserver())

        assert len(events) > 0
        records = job.nodes["doubler"].get_all_records()
        assert records is not None
        assert records.num_rows == 2
        result_values = sorted(records.column("result").to_pylist())
        assert result_values == [2, 4]

    def test_run_populates_node_caches(self):
        """After run(), iter_data()/as_table() should work on nodes."""
        src = _make_source("key", "value", {"key": ["a", "b"], "value": [1, 2]})
        pf = PythonDataFunction(double_value, output_keys="result")
        pod = FunctionPod(pf)

        job = PipelineJob(name="cache", store=InMemoryArrowDatabase())
        with job:
            pod(src, label="doubler")

        job.run()

        table = job.nodes["doubler"].as_table()
        assert table.num_rows == 2


class TestSyncAsyncParity:
    """Sync orchestrator should produce same DB results as async."""

    def test_linear_pipeline_parity(self):
        src = _make_source("key", "value", {"key": ["a", "b"], "value": [10, 20]})
        pf = PythonDataFunction(double_value, output_keys="result")
        pod = FunctionPod(pf)

        # Sync via PipelineJob
        job_sync = PipelineJob(name="sync", store=InMemoryArrowDatabase())
        with job_sync:
            pod(src, label="doubler")
        job_sync.run()
        sync_records = job_sync.nodes["doubler"].get_all_records()
        sync_values = sorted(sync_records.column("result").to_pylist())

        # Async via PipelineJob with AsyncPipelineOrchestrator
        job_async = PipelineJob(name="async", store=InMemoryArrowDatabase())
        with job_async:
            pod(src, label="doubler")
        job_async.run(orchestrator=AsyncPipelineOrchestrator())
        async_records = job_async.nodes["doubler"].get_all_records()
        async_values = sorted(async_records.column("result").to_pylist())

        assert sync_values == async_values

    def test_diamond_pipeline_parity(self):
        src_a = _make_source("key", "value", {"key": ["a", "b"], "value": [10, 20]})
        src_b = _make_source("key", "score", {"key": ["a", "b"], "score": [100, 200]})
        pf = PythonDataFunction(add_values, output_keys="total")
        pod = FunctionPod(pf)

        job_sync = PipelineJob(name="sync_d", store=InMemoryArrowDatabase())
        with job_sync:
            joined = Join()(src_a, src_b, label="join")
            pod(joined, label="adder")
        job_sync.run()
        sync_values = sorted(
            job_sync.nodes["adder"].get_all_records().column("total").to_pylist()
        )

        job_async = PipelineJob(name="async_d", store=InMemoryArrowDatabase())
        with job_async:
            joined = Join()(src_a, src_b, label="join")
            pod(joined, label="adder")
        job_async.run(orchestrator=AsyncPipelineOrchestrator())
        async_values = sorted(
            job_async.nodes["adder"].get_all_records().column("total").to_pylist()
        )

        assert sync_values == async_values


class TestMaterializedStreamIdentity:
    """Materialized streams should preserve the original node's identity."""

    def test_materialized_stream_has_same_pipeline_hash(self):
        """Stream reconstructed from buffer should have same pipeline_hash as original."""
        src = _make_source("key", "value", {"key": ["a", "b"], "value": [1, 2]})
        from orcapod.core.nodes.source_node import SourceJobNode

        tag_schema, data_schema = src.output_schema()
        node = SourceJobNode(
            name="test_src",
            tag_schema=tag_schema,
            data_schema=data_schema,
            bound_source=src,
        )
        buf = list(node.iter_data())

        stream = SyncPipelineOrchestrator._materialize_as_stream(buf, node)
        assert stream.pipeline_hash() == node.pipeline_hash()

    def test_materialized_stream_has_same_content_hash(self):
        """Stream reconstructed from an operator node's buffer should have same
        content_hash.

        For OperatorNodes, the identity_structure is (operator, argument_symmetry(upstreams)),
        so the materialized stream (which carries the same producer and upstreams) shares
        the same content_hash as the original node.
        """
        src_a = _make_source("key", "value", {"key": ["a", "b"], "value": [10, 20]})
        src_b = _make_source("key", "score", {"key": ["a", "b"], "score": [100, 200]})
        from orcapod.core.nodes.operator_node import OperatorJobNode

        op_node = OperatorJobNode(Join(), input_streams=[src_a, src_b])
        op_node.run()
        buf = list(op_node.iter_data())

        stream = SyncPipelineOrchestrator._materialize_as_stream(buf, op_node)
        assert stream.content_hash() == op_node.content_hash()

    def test_materialized_stream_preserves_system_tags(self):
        """System tag column names in materialized stream should match original."""
        src_a = _make_source("key", "value", {"key": ["a", "b"], "value": [10, 20]})
        src_b = _make_source("key", "score", {"key": ["a", "b"], "score": [100, 200]})
        from orcapod.core.operators.join import Join
        from orcapod.core.nodes.operator_node import OperatorJobNode

        op = Join()
        op_node = OperatorJobNode(op, input_streams=[src_a, src_b])
        op_node.run()
        buf = list(op_node.iter_data())

        stream = SyncPipelineOrchestrator._materialize_as_stream(buf, op_node)

        expected_tag_schema = op_node.output_schema(columns={"system_tags": True})[0]
        actual_tag_schema = stream.output_schema(columns={"system_tags": True})[0]
        assert expected_tag_schema == actual_tag_schema

    def test_operator_with_materialized_upstream_produces_correct_system_tags(self):
        """When an operator receives a materialized stream, its output system
        tags should embed the correct pipeline hashes (same as if it received
        the original stream).

        PipelineJob wraps concrete sources in SourceJobNode slots, so the
        comparison is between two OperatorJobNode objects that use SourceJobNode
        upstreams with the same schema — one run through the orchestrator and
        one constructed directly."""
        src_a = _make_source("key", "value", {"key": ["a", "b"], "value": [10, 20]})
        src_b = _make_source("key", "score", {"key": ["a", "b"], "score": [100, 200]})
        pf = PythonDataFunction(add_values, output_keys="total")
        pod = FunctionPod(pf)

        # Run via PipelineJob (uses materialized streams internally)
        job = PipelineJob(name="orch", store=InMemoryArrowDatabase())
        with job:
            joined = Join()(src_a, src_b, label="join")
            pod(joined, label="adder")
        job.run()

        # Run via pull-based path using SourceJobNode wrappers to match how
        # PipelineJob represents sources (schema-based identity, not data-based)
        from orcapod.core.nodes.source_node import SourceJobNode
        from orcapod.core.nodes.operator_node import OperatorJobNode as OJN

        sjn_a = SourceJobNode(name="ArrowTableSource", bound_source=src_a)
        sjn_b = SourceJobNode(name="ArrowTableSource", bound_source=src_b)
        join_node = OJN(Join(), input_streams=[sjn_a, sjn_b])

        # Compare system tag schemas — should match
        orch_join = job.nodes["join"]
        orch_tag_schema = orch_join.output_schema(columns={"system_tags": True})[0]
        pull_tag_schema = join_node.output_schema(columns={"system_tags": True})[0]
        assert orch_tag_schema == pull_tag_schema


class TestSyncObserverInjection:
    """Verify observer passed to orchestrator flows through to nodes."""

    def test_operator_pipeline_observer_hooks(self):
        """Source → Operator → Function: all node types fire hooks."""
        src = _make_source("key", "value", {"key": ["a", "b"], "value": [1, 2]})
        op = MapData(name_map={"value": "val"})

        def double_val(val: int) -> int:
            return val * 2

        pf = PythonDataFunction(double_val, output_keys="result")
        pod = FunctionPod(pf)

        job = PipelineJob(name="obs_op", store=InMemoryArrowDatabase())
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
                events.append(("data_end", node_label, cached))
            def on_data_crash(self, node_label, tag, data, exc): pass
            def create_data_logger(self, tag, data, **kwargs):
                from orcapod.pipeline.observer import _NOOP_LOGGER
                return _NOOP_LOGGER
            def contextualize(self, *identity_path):
                return self

        job.run(observer=RecordingObserver())

        # Mapper fires node_start/node_end only (no data-level hooks)
        assert ("node_start", "mapper") in events
        assert ("node_end", "mapper") in events
        assert ("data_start", "mapper") not in events

        # Function fires node_start, per-data hooks, node_end
        assert ("node_start", "doubler") in events
        assert ("node_end", "doubler") in events
        fn_data_events = [e for e in events if e[0] == "data_start" and e[1] == "doubler"]
        assert len(fn_data_events) == 2  # 2 data

    def test_function_node_cached_flag(self):
        """Second run with DB should report cached=True for known data."""
        src = _make_source("key", "value", {"key": ["a"], "value": [1]})
        pf = PythonDataFunction(double_value, output_keys="result")
        pod = FunctionPod(pf)

        db = InMemoryArrowDatabase()
        job = PipelineJob(name="cached_obs", store=db)
        with job:
            pod(src, label="doubler")

        # First run — should be cached=False
        events1 = []

        class Obs1:
            def on_run_start(self, run_id, pipeline_uri=""): pass
            def on_run_end(self, run_id): pass
            def on_node_start(self, node_label, node_hash, **kwargs): pass
            def on_node_end(self, node_label, node_hash, **kwargs): pass
            def on_data_start(self, node_label, tag, data): pass
            def on_data_end(self, node_label, tag, input_pkt, output_pkt, cached):
                if node_label == "doubler":
                    events1.append(cached)
            def on_data_crash(self, node_label, tag, data, exc): pass
            def create_data_logger(self, tag, data, **kwargs):
                from orcapod.pipeline.observer import _NOOP_LOGGER
                return _NOOP_LOGGER
            def contextualize(self, *identity_path):
                return self

        job.run(observer=Obs1())
        assert events1 == [False]

        # Second run — should be cached=True
        events2 = []

        class Obs2:
            def on_run_start(self, run_id, pipeline_uri=""): pass
            def on_run_end(self, run_id): pass
            def on_node_start(self, node_label, node_hash, **kwargs): pass
            def on_node_end(self, node_label, node_hash, **kwargs): pass
            def on_data_start(self, node_label, tag, data): pass
            def on_data_end(self, node_label, tag, input_pkt, output_pkt, cached):
                if node_label == "doubler":
                    events2.append(cached)
            def on_data_crash(self, node_label, tag, data, exc): pass
            def create_data_logger(self, tag, data, **kwargs):
                from orcapod.pipeline.observer import _NOOP_LOGGER
                return _NOOP_LOGGER
            def contextualize(self, *identity_path):
                return self

        job.run(observer=Obs2())
        assert events2 == [True]

    def test_diamond_dag_observer_event_order(self):
        """Two sources → Join → Function: events follow topological order."""
        src_a = _make_source("key", "value", {"key": ["a"], "value": [10]})
        src_b = _make_source("key", "score", {"key": ["a"], "score": [100]})
        pf = PythonDataFunction(add_values, output_keys="total")
        pod = FunctionPod(pf)

        job = PipelineJob(name="diamond_obs", store=InMemoryArrowDatabase())
        with job:
            joined = Join()(src_a, src_b, label="join")
            pod(joined, label="adder")

        node_order = []

        class OrderObserver:
            def on_run_start(self, run_id, pipeline_uri=""): pass
            def on_run_end(self, run_id): pass
            def on_node_start(self, node_label, node_hash, **kwargs):
                node_order.append(("start", node_label))
            def on_node_end(self, node_label, node_hash, **kwargs):
                node_order.append(("end", node_label))
            def on_data_start(self, node_label, tag, data): pass
            def on_data_end(self, node_label, tag, input_pkt, output_pkt, cached): pass
            def on_data_crash(self, node_label, tag, data, exc): pass
            def create_data_logger(self, tag, data, **kwargs):
                from orcapod.pipeline.observer import _NOOP_LOGGER
                return _NOOP_LOGGER
            def contextualize(self, *identity_path):
                return self

        job.run(observer=OrderObserver())

        # Extract just the node labels in start order
        starts = [label for event, label in node_order if event == "start"]
        # Sources first, then operator ("join"), then function ("adder")
        join_idx = starts.index("join")
        adder_idx = starts.index("adder")
        # join and adder should come after source labels
        source_labels = [s for s in starts if s not in ("join", "adder")]
        assert len(source_labels) == 2
        assert join_idx > max(starts.index(s) for s in source_labels)
        assert adder_idx > join_idx

    def test_no_observer_works(self):
        """Pipeline runs fine with no observer (None)."""
        src = _make_source("key", "value", {"key": ["a"], "value": [1]})
        pf = PythonDataFunction(double_value, output_keys="result")
        pod = FunctionPod(pf)

        job = PipelineJob(name="no_obs", store=InMemoryArrowDatabase())
        with job:
            pod(src, label="doubler")

        job.run()  # no observer
        records = job.nodes["doubler"].get_all_records()
        assert records.num_rows == 1


# TestMaterializeResults deleted: PipelineJob.run() always materialises
# results to the store database. The materialize_results=False path is
# a SyncPipelineOrchestrator / AsyncPipelineOrchestrator implementation
# detail; coverage will be restored when test_orchestrator.py is migrated
# in the same ENG-491 migration pass.
