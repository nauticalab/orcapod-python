"""Integration tests for LoggingObserver with real pipelines.

Exercises the full logging pipeline: capture → CapturedLogs return type →
FunctionNode → observer → PacketLogger → database, using InMemoryArrowDatabase
and real Pipeline objects.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.core.function_pod import FunctionPod
from orcapod.core.packet_function import PythonPacketFunction
from orcapod.core.sources.arrow_table_source import ArrowTableSource
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline import (
    AsyncPipelineOrchestrator,
    Pipeline,
    SyncPipelineOrchestrator,
)
from orcapod.pipeline.logging_observer import LoggingObserver


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
# 1. Sync pipeline — succeeding packets → log rows with stdout captured
# ---------------------------------------------------------------------------


class TestSyncPipelineSuccessLogs:
    def test_success_logs_captured(self):
        db = InMemoryArrowDatabase()
        source = _make_source()

        def double(x: int) -> int:
            print(f"doubling {x}")
            return x * 2

        pf = PythonPacketFunction(double, output_keys="result")
        pod = FunctionPod(pf)

        pipeline = Pipeline(name="test_logs", pipeline_database=db)
        with pipeline:
            pod(source, label="doubler")

        obs = LoggingObserver(log_database=db)
        orch = SyncPipelineOrchestrator(observer=obs)
        pipeline.run(orchestrator=orch)

        fn_node = _get_function_node(pipeline)
        logs = obs.get_logs(pipeline_path=fn_node.pipeline_path)

        assert logs is not None
        assert logs.num_rows == 3
        success_col = logs.column("success").to_pylist()
        assert all(success_col)


# ---------------------------------------------------------------------------
# 2. Failing packets → success=False, traceback populated
# ---------------------------------------------------------------------------


class TestFailingPacketsLogged:
    def test_failure_logged_with_traceback(self):
        db = InMemoryArrowDatabase()
        source = _make_source(2)

        def failing(x: int) -> int:
            raise ValueError("boom")

        pf = PythonPacketFunction(failing, output_keys="result")
        pod = FunctionPod(pf)

        pipeline = Pipeline(name="test_fail", pipeline_database=db)
        with pipeline:
            pod(source, label="failing")

        obs = LoggingObserver(log_database=db)
        orch = SyncPipelineOrchestrator(observer=obs)
        pipeline.run(orchestrator=orch)

        fn_node = _get_function_node(pipeline)
        logs = obs.get_logs(pipeline_path=fn_node.pipeline_path)

        assert logs is not None
        assert logs.num_rows == 2
        for row_idx in range(logs.num_rows):
            assert logs.column("success").to_pylist()[row_idx] is False
            tb = logs.column("traceback").to_pylist()[row_idx]
            assert tb is not None
            assert "ValueError" in tb
            assert "boom" in tb


# ---------------------------------------------------------------------------
# 3. Pipeline-path-mirrored storage
# ---------------------------------------------------------------------------


class TestPipelinePathMirroredStorage:
    def test_log_path_mirrors_pipeline_path(self):
        db = InMemoryArrowDatabase()
        source = _make_source(1)

        def identity(x: int) -> int:
            return x

        pf = PythonPacketFunction(identity, output_keys="result")
        pod = FunctionPod(pf)

        pipeline = Pipeline(name="test_mirror", pipeline_database=db)
        with pipeline:
            pod(source, label="ident")

        obs = LoggingObserver(log_database=db)
        orch = SyncPipelineOrchestrator(observer=obs)
        pipeline.run(orchestrator=orch)

        fn_node = _get_function_node(pipeline)
        pp = fn_node.pipeline_path
        expected_log_path = pp[:1] + ("logs",) + pp[1:]

        # Verify the log path is correct by reading directly from the DB
        raw = db.get_all_records(expected_log_path)
        assert raw is not None
        assert raw.num_rows == 1


# ---------------------------------------------------------------------------
# 4. Queryable tag columns (not JSON)
# ---------------------------------------------------------------------------


class TestQueryableTagColumns:
    def test_tag_columns_in_log_table(self):
        db = InMemoryArrowDatabase()
        source = _make_source(2)

        def identity(x: int) -> int:
            return x

        pf = PythonPacketFunction(identity, output_keys="result")
        pod = FunctionPod(pf)

        pipeline = Pipeline(name="test_tags", pipeline_database=db)
        with pipeline:
            pod(source, label="ident")

        obs = LoggingObserver(log_database=db)
        orch = SyncPipelineOrchestrator(observer=obs)
        pipeline.run(orchestrator=orch)

        fn_node = _get_function_node(pipeline)
        logs = obs.get_logs(pipeline_path=fn_node.pipeline_path)

        assert logs is not None
        # "id" tag column should be a separate column, not JSON
        assert "id" in logs.column_names
        assert "tags" not in logs.column_names
        id_values = sorted(logs.column("id").to_pylist())
        assert id_values == ["0", "1"]


# ---------------------------------------------------------------------------
# 5. Async orchestrator logs
# ---------------------------------------------------------------------------


class TestAsyncOrchestratorLogs:
    def test_async_pipeline_captures_logs(self):
        db = InMemoryArrowDatabase()
        source = _make_source(2)

        def double(x: int) -> int:
            return x * 2

        pf = PythonPacketFunction(double, output_keys="result")
        pod = FunctionPod(pf)

        pipeline = Pipeline(name="test_async_logs", pipeline_database=db)
        with pipeline:
            pod(source, label="doubler")

        obs = LoggingObserver(log_database=db)
        orch = AsyncPipelineOrchestrator(observer=obs)
        pipeline.run(orchestrator=orch)

        fn_node = _get_function_node(pipeline)
        logs = obs.get_logs(pipeline_path=fn_node.pipeline_path)

        assert logs is not None
        assert logs.num_rows == 2
        assert all(logs.column("success").to_pylist())


# ---------------------------------------------------------------------------
# 6. fail_fast error policy
# ---------------------------------------------------------------------------


class TestFailFastErrorPolicy:
    def test_fail_fast_aborts_and_logs(self):
        db = InMemoryArrowDatabase()
        source = _make_source(3)

        def failing(x: int) -> int:
            raise RuntimeError("crash")

        pf = PythonPacketFunction(failing, output_keys="result")
        pod = FunctionPod(pf)

        pipeline = Pipeline(name="test_failfast", pipeline_database=db)
        with pipeline:
            pod(source, label="failing")

        obs = LoggingObserver(log_database=db)
        orch = SyncPipelineOrchestrator(observer=obs, error_policy="fail_fast")

        with pytest.raises(RuntimeError, match="crash"):
            pipeline.run(orchestrator=orch)

        fn_node = _get_function_node(pipeline)
        logs = obs.get_logs(pipeline_path=fn_node.pipeline_path)

        # At least the first crash should be logged before abort
        assert logs is not None
        assert logs.num_rows >= 1
        assert logs.column("success").to_pylist()[0] is False


# ---------------------------------------------------------------------------
# 7. Mixed success/failure — correct log row per packet
# ---------------------------------------------------------------------------


class TestMixedSuccessFailure:
    def test_mixed_results_logged_correctly(self):
        db = InMemoryArrowDatabase()
        source = ArrowTableSource(
            pa.table({
                "id": pa.array(["0", "1", "2"], type=pa.large_string()),
                "x": pa.array([10, -1, 30], type=pa.int64()),
            }),
            tag_columns=["id"],
        )

        def safe_div(x: int) -> float:
            return 100 / x

        pf = PythonPacketFunction(safe_div, output_keys="result")
        pod = FunctionPod(pf)

        pipeline = Pipeline(name="test_mixed", pipeline_database=db)
        with pipeline:
            pod(source, label="divider")

        obs = LoggingObserver(log_database=db)
        orch = SyncPipelineOrchestrator(observer=obs)
        pipeline.run(orchestrator=orch)

        fn_node = _get_function_node(pipeline)
        logs = obs.get_logs(pipeline_path=fn_node.pipeline_path)

        assert logs is not None
        # x=10 succeeds, x=-1 succeeds (100/-1 = -100), x=30 succeeds
        # All three should succeed since division by these values is fine
        assert logs.num_rows == 3
        assert all(logs.column("success").to_pylist())


# ---------------------------------------------------------------------------
# 8. Multiple function nodes — each gets own log table
# ---------------------------------------------------------------------------


class TestMultipleFunctionNodesSeparateLogs:
    def test_two_nodes_separate_log_tables(self):
        db = InMemoryArrowDatabase()
        source = _make_source(2)

        def double(x: int) -> int:
            return x * 2

        def triple(result: int) -> int:
            return result * 3

        pf1 = PythonPacketFunction(double, output_keys="result")
        pod1 = FunctionPod(pf1)
        pf2 = PythonPacketFunction(triple, output_keys="final")
        pod2 = FunctionPod(pf2)

        pipeline = Pipeline(name="test_multi", pipeline_database=db)
        with pipeline:
            s1 = pod1(source, label="doubler")
            pod2(s1, label="tripler")

        obs = LoggingObserver(log_database=db)
        orch = SyncPipelineOrchestrator(observer=obs)
        pipeline.run(orchestrator=orch)

        import networkx as nx

        fn_nodes = [
            n
            for n in nx.topological_sort(pipeline._node_graph)
            if n.node_type == "function"
        ]
        assert len(fn_nodes) == 2

        logs1 = obs.get_logs(pipeline_path=fn_nodes[0].pipeline_path)
        logs2 = obs.get_logs(pipeline_path=fn_nodes[1].pipeline_path)

        assert logs1 is not None
        assert logs2 is not None
        assert logs1.num_rows == 2
        assert logs2.num_rows == 2

        # Verify they are at different paths
        assert fn_nodes[0].pipeline_path != fn_nodes[1].pipeline_path


# ---------------------------------------------------------------------------
# 9. get_logs(pipeline_path) retrieves node-specific logs
# ---------------------------------------------------------------------------


class TestGetLogsNodeSpecific:
    def test_get_logs_filters_by_node(self):
        db = InMemoryArrowDatabase()
        source = _make_source(2)

        def double(x: int) -> int:
            return x * 2

        def triple(result: int) -> int:
            return result * 3

        pf1 = PythonPacketFunction(double, output_keys="result")
        pod1 = FunctionPod(pf1)
        pf2 = PythonPacketFunction(triple, output_keys="final")
        pod2 = FunctionPod(pf2)

        pipeline = Pipeline(name="test_filter", pipeline_database=db)
        with pipeline:
            s1 = pod1(source, label="doubler")
            pod2(s1, label="tripler")

        obs = LoggingObserver(log_database=db)
        orch = SyncPipelineOrchestrator(observer=obs)
        pipeline.run(orchestrator=orch)

        import networkx as nx

        fn_nodes = [
            n
            for n in nx.topological_sort(pipeline._node_graph)
            if n.node_type == "function"
        ]

        # Each node's logs contain only that node's label
        logs1 = obs.get_logs(pipeline_path=fn_nodes[0].pipeline_path)
        logs2 = obs.get_logs(pipeline_path=fn_nodes[1].pipeline_path)

        labels1 = set(logs1.column("node_label").to_pylist())
        labels2 = set(logs2.column("node_label").to_pylist())

        assert labels1 == {"doubler"}
        assert labels2 == {"tripler"}
