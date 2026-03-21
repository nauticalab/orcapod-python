"""Integration tests for StatusObserver with real pipelines.

Exercises the full status tracking pipeline: observer hooks →
StatusObserver → database, using InMemoryArrowDatabase and real
Pipeline objects.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.core.executors import LocalExecutor
from orcapod.core.function_pod import FunctionPod
from orcapod.core.packet_function import PythonPacketFunction
from orcapod.core.sources.arrow_table_source import ArrowTableSource
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline import (
    AsyncPipelineOrchestrator,
    Pipeline,
    SyncPipelineOrchestrator,
)
from orcapod.pipeline.status_observer import StatusObserver


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_source(n: int = 3) -> ArrowTableSource:
    table = pa.table({
        "id": pa.array([str(i) for i in range(n)], type=pa.large_string()),
        "x": pa.array([10 * (i + 1) for i in range(n)], type=pa.int64()),
    })
    return ArrowTableSource(table, tag_columns=["id"])


def _get_function_node(pipeline: Pipeline):
    """Return the first function node from the pipeline graph."""
    import networkx as nx

    for node in nx.topological_sort(pipeline._node_graph):
        if node.node_type == "function":
            return node
    raise RuntimeError("No function node found")


# ---------------------------------------------------------------------------
# 1. Sync pipeline — success status events
# ---------------------------------------------------------------------------


class TestSyncPipelineSuccessStatus:
    def test_success_produces_running_and_success_events(self):
        db = InMemoryArrowDatabase()
        source = _make_source()

        def double(x: int) -> int:
            return x * 2

        pf = PythonPacketFunction(double, output_keys="result", executor=LocalExecutor())
        pod = FunctionPod(pf)

        pipeline = Pipeline(name="test_status", pipeline_database=db)
        with pipeline:
            pod(source, label="doubler")

        obs = StatusObserver(status_database=db)
        orch = SyncPipelineOrchestrator(observer=obs)
        pipeline.run(orchestrator=orch)

        fn_node = _get_function_node(pipeline)
        status = obs.get_status(pipeline_path=fn_node.pipeline_path)

        assert status is not None
        # 3 packets × 2 events each (RUNNING + SUCCESS) = 6 rows
        assert status.num_rows == 6

        states = status.column("_status_state").to_pylist()
        assert states.count("RUNNING") == 3
        assert states.count("SUCCESS") == 3


# ---------------------------------------------------------------------------
# 2. Failing packets → FAILED status with error summary
# ---------------------------------------------------------------------------


class TestFailingPacketsStatus:
    def test_failure_status_with_error_summary(self):
        db = InMemoryArrowDatabase()
        source = _make_source(2)

        def failing(x: int) -> int:
            raise ValueError("boom")

        pf = PythonPacketFunction(failing, output_keys="result", executor=LocalExecutor())
        pod = FunctionPod(pf)

        pipeline = Pipeline(name="test_fail_status", pipeline_database=db)
        with pipeline:
            pod(source, label="failing")

        obs = StatusObserver(status_database=db)
        orch = SyncPipelineOrchestrator(observer=obs)
        pipeline.run(orchestrator=orch)

        fn_node = _get_function_node(pipeline)
        status = obs.get_status(pipeline_path=fn_node.pipeline_path)

        assert status is not None
        # 2 packets × 2 events each (RUNNING + FAILED)
        assert status.num_rows == 4

        states = status.column("_status_state").to_pylist()
        assert states.count("RUNNING") == 2
        assert states.count("FAILED") == 2

        # Error summary should be populated for FAILED events
        for i, state in enumerate(states):
            error_summary = status.column("_status_error_summary").to_pylist()[i]
            if state == "FAILED":
                assert error_summary is not None
                assert "boom" in error_summary
            else:
                assert error_summary is None


# ---------------------------------------------------------------------------
# 3. Pipeline-path-mirrored storage
# ---------------------------------------------------------------------------


class TestPipelinePathMirroredStorage:
    def test_status_path_mirrors_pipeline_path(self):
        db = InMemoryArrowDatabase()
        source = _make_source(1)

        def identity(x: int) -> int:
            return x

        pf = PythonPacketFunction(identity, output_keys="result", executor=LocalExecutor())
        pod = FunctionPod(pf)

        pipeline = Pipeline(name="test_mirror_status", pipeline_database=db)
        with pipeline:
            pod(source, label="ident")

        obs = StatusObserver(status_database=db)
        orch = SyncPipelineOrchestrator(observer=obs)
        pipeline.run(orchestrator=orch)

        fn_node = _get_function_node(pipeline)
        pp = fn_node.pipeline_path
        expected_status_path = pp[:1] + ("status",) + pp[1:]

        # Verify the status path is correct by reading directly from the DB
        raw = db.get_all_records(expected_status_path)
        assert raw is not None
        assert raw.num_rows == 2  # RUNNING + SUCCESS


# ---------------------------------------------------------------------------
# 4. Queryable tag columns
# ---------------------------------------------------------------------------


class TestQueryableTagColumns:
    def test_tag_columns_in_status_table(self):
        db = InMemoryArrowDatabase()
        source = _make_source(2)

        def identity(x: int) -> int:
            return x

        pf = PythonPacketFunction(identity, output_keys="result", executor=LocalExecutor())
        pod = FunctionPod(pf)

        pipeline = Pipeline(name="test_tags_status", pipeline_database=db)
        with pipeline:
            pod(source, label="ident")

        obs = StatusObserver(status_database=db)
        orch = SyncPipelineOrchestrator(observer=obs)
        pipeline.run(orchestrator=orch)

        fn_node = _get_function_node(pipeline)
        status = obs.get_status(pipeline_path=fn_node.pipeline_path)

        assert status is not None
        # "id" tag column should be a separate column, not JSON
        assert "id" in status.column_names
        id_values = sorted(set(status.column("id").to_pylist()))
        assert id_values == ["0", "1"]


# ---------------------------------------------------------------------------
# 5. Async orchestrator status
# ---------------------------------------------------------------------------


class TestAsyncOrchestratorStatus:
    def test_async_pipeline_captures_status(self):
        db = InMemoryArrowDatabase()
        source = _make_source(2)

        def double(x: int) -> int:
            return x * 2

        pf = PythonPacketFunction(double, output_keys="result", executor=LocalExecutor())
        pod = FunctionPod(pf)

        pipeline = Pipeline(name="test_async_status", pipeline_database=db)
        with pipeline:
            pod(source, label="doubler")

        obs = StatusObserver(status_database=db)
        orch = AsyncPipelineOrchestrator(observer=obs)
        pipeline.run(orchestrator=orch)

        fn_node = _get_function_node(pipeline)
        status = obs.get_status(pipeline_path=fn_node.pipeline_path)

        assert status is not None
        assert status.num_rows == 4  # 2 × (RUNNING + SUCCESS)

        states = status.column("_status_state").to_pylist()
        assert states.count("RUNNING") == 2
        assert states.count("SUCCESS") == 2


# ---------------------------------------------------------------------------
# 6. fail_fast error policy
# ---------------------------------------------------------------------------


class TestFailFastErrorPolicy:
    def test_fail_fast_aborts_and_records_status(self):
        db = InMemoryArrowDatabase()
        source = _make_source(3)

        def failing(x: int) -> int:
            raise RuntimeError("crash")

        pf = PythonPacketFunction(failing, output_keys="result", executor=LocalExecutor())
        pod = FunctionPod(pf)

        pipeline = Pipeline(name="test_failfast_status", pipeline_database=db)
        with pipeline:
            pod(source, label="failing")

        obs = StatusObserver(status_database=db)
        orch = SyncPipelineOrchestrator(observer=obs, error_policy="fail_fast")

        with pytest.raises(RuntimeError, match="crash"):
            pipeline.run(orchestrator=orch)

        fn_node = _get_function_node(pipeline)
        status = obs.get_status(pipeline_path=fn_node.pipeline_path)

        # At least one RUNNING + one FAILED before abort
        assert status is not None
        assert status.num_rows >= 2
        states = status.column("_status_state").to_pylist()
        assert "RUNNING" in states
        assert "FAILED" in states


# ---------------------------------------------------------------------------
# 7. Mixed success/failure — correct status per packet
# ---------------------------------------------------------------------------


class TestMixedSuccessFailure:
    def test_mixed_results_tracked_correctly(self):
        db = InMemoryArrowDatabase()
        source = ArrowTableSource(
            pa.table({
                "id": pa.array(["a", "b", "c"], type=pa.large_string()),
                "x": pa.array([10, 0, 30], type=pa.int64()),
            }),
            tag_columns=["id"],
        )

        def safe_div(x: int) -> float:
            return 100 / x  # x=0 will raise ZeroDivisionError

        pf = PythonPacketFunction(safe_div, output_keys="result", executor=LocalExecutor())
        pod = FunctionPod(pf)

        pipeline = Pipeline(name="test_mixed_status", pipeline_database=db)
        with pipeline:
            pod(source, label="divider")

        obs = StatusObserver(status_database=db)
        orch = SyncPipelineOrchestrator(observer=obs)
        pipeline.run(orchestrator=orch)

        fn_node = _get_function_node(pipeline)
        status = obs.get_status(pipeline_path=fn_node.pipeline_path)

        assert status is not None
        # 3 RUNNING + 2 SUCCESS + 1 FAILED = 6
        assert status.num_rows == 6

        states = status.column("_status_state").to_pylist()
        assert states.count("RUNNING") == 3
        assert states.count("SUCCESS") == 2
        assert states.count("FAILED") == 1


# ---------------------------------------------------------------------------
# 8. Multiple function nodes — each gets own status table
# ---------------------------------------------------------------------------


class TestMultipleFunctionNodesSeparateStatus:
    def test_two_nodes_separate_status_tables(self):
        db = InMemoryArrowDatabase()
        source = _make_source(2)

        def double(x: int) -> int:
            return x * 2

        def triple(result: int) -> int:
            return result * 3

        pf1 = PythonPacketFunction(double, output_keys="result", executor=LocalExecutor())
        pod1 = FunctionPod(pf1)
        pf2 = PythonPacketFunction(triple, output_keys="final", executor=LocalExecutor())
        pod2 = FunctionPod(pf2)

        pipeline = Pipeline(name="test_multi_status", pipeline_database=db)
        with pipeline:
            s1 = pod1(source, label="doubler")
            pod2(s1, label="tripler")

        obs = StatusObserver(status_database=db)
        orch = SyncPipelineOrchestrator(observer=obs)
        pipeline.run(orchestrator=orch)

        import networkx as nx

        fn_nodes = [
            n
            for n in nx.topological_sort(pipeline._node_graph)
            if n.node_type == "function"
        ]
        assert len(fn_nodes) == 2

        status1 = obs.get_status(pipeline_path=fn_nodes[0].pipeline_path)
        status2 = obs.get_status(pipeline_path=fn_nodes[1].pipeline_path)

        assert status1 is not None
        assert status2 is not None
        # Each node: 2 packets × 2 events = 4 rows
        assert status1.num_rows == 4
        assert status2.num_rows == 4

        # Verify they are at different paths
        assert fn_nodes[0].pipeline_path != fn_nodes[1].pipeline_path


# ---------------------------------------------------------------------------
# 9. get_status(pipeline_path) retrieves node-specific status
# ---------------------------------------------------------------------------


class TestGetStatusNodeSpecific:
    def test_get_status_filters_by_node(self):
        db = InMemoryArrowDatabase()
        source = _make_source(2)

        def double(x: int) -> int:
            return x * 2

        def triple(result: int) -> int:
            return result * 3

        pf1 = PythonPacketFunction(double, output_keys="result", executor=LocalExecutor())
        pod1 = FunctionPod(pf1)
        pf2 = PythonPacketFunction(triple, output_keys="final", executor=LocalExecutor())
        pod2 = FunctionPod(pf2)

        pipeline = Pipeline(name="test_filter_status", pipeline_database=db)
        with pipeline:
            s1 = pod1(source, label="doubler")
            pod2(s1, label="tripler")

        obs = StatusObserver(status_database=db)
        orch = SyncPipelineOrchestrator(observer=obs)
        pipeline.run(orchestrator=orch)

        import networkx as nx

        fn_nodes = [
            n
            for n in nx.topological_sort(pipeline._node_graph)
            if n.node_type == "function"
        ]

        # Each node's status contains only that node's label
        status1 = obs.get_status(pipeline_path=fn_nodes[0].pipeline_path)
        status2 = obs.get_status(pipeline_path=fn_nodes[1].pipeline_path)

        labels1 = set(status1.column("_status_node_label").to_pylist())
        labels2 = set(status2.column("_status_node_label").to_pylist())

        assert labels1 == {"doubler"}
        assert labels2 == {"tripler"}


# ---------------------------------------------------------------------------
# 10. Status columns have correct schema
# ---------------------------------------------------------------------------


class TestStatusSchema:
    def test_all_expected_columns_present(self):
        db = InMemoryArrowDatabase()
        source = _make_source(1)

        def identity(x: int) -> int:
            return x

        pf = PythonPacketFunction(identity, output_keys="result", executor=LocalExecutor())
        pod = FunctionPod(pf)

        pipeline = Pipeline(name="test_schema", pipeline_database=db)
        with pipeline:
            pod(source, label="ident")

        obs = StatusObserver(status_database=db)
        orch = SyncPipelineOrchestrator(observer=obs)
        pipeline.run(orchestrator=orch)

        fn_node = _get_function_node(pipeline)
        status = obs.get_status(pipeline_path=fn_node.pipeline_path)

        expected_system_cols = {
            "_status_id",
            "_status_run_id",
            "_status_node_label",
            "_status_node_hash",
            "_status_state",
            "_status_timestamp",
            "_status_error_summary",
        }
        assert expected_system_cols.issubset(set(status.column_names))

        # Tag column should also be present
        assert "id" in status.column_names


# ---------------------------------------------------------------------------
# 11. run_id is tracked correctly
# ---------------------------------------------------------------------------


class TestRunIdTracking:
    def test_run_id_populated_in_status(self):
        db = InMemoryArrowDatabase()
        source = _make_source(1)

        def identity(x: int) -> int:
            return x

        pf = PythonPacketFunction(identity, output_keys="result", executor=LocalExecutor())
        pod = FunctionPod(pf)

        pipeline = Pipeline(name="test_runid", pipeline_database=db)
        with pipeline:
            pod(source, label="ident")

        obs = StatusObserver(status_database=db)
        orch = SyncPipelineOrchestrator(observer=obs)
        # Pass run_id via the orchestrator's run() method directly
        orch.run(pipeline._node_graph, run_id="my-custom-run-id")

        fn_node = _get_function_node(pipeline)
        status = obs.get_status(pipeline_path=fn_node.pipeline_path)

        run_ids = set(status.column("_status_run_id").to_pylist())
        assert run_ids == {"my-custom-run-id"}
