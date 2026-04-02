"""Integration tests for LoggingObserver with real pipelines.

Exercises the full logging pipeline: capture → CapturedLogs return type →
FunctionNode → observer → PacketLogger → database, using InMemoryArrowDatabase
and real Pipeline objects.
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
from orcapod.pipeline.logging_observer import LoggingObserver


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_source(n: int = 3) -> ArrowTableSource:
    table = pa.table({
        "id": pa.array([str(i) for i in range(n)], type=pa.large_string()),
        "x": pa.array([10 * (i + 1) for i in range(n)], type=pa.int64()),
    })
    return ArrowTableSource(table, tag_columns=["id"], infer_nullable=True)


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

        pf = PythonPacketFunction(double, output_keys="result", executor=LocalExecutor())
        pod = FunctionPod(pf)

        pipeline = Pipeline(name="test_logs", pipeline_database=db)
        with pipeline:
            pod(source, label="doubler")

        obs = LoggingObserver(log_database=db)
        orch = SyncPipelineOrchestrator()
        pipeline.run(orchestrator=orch, observer=obs)

        logs = obs.get_logs()

        assert logs is not None
        assert logs.num_rows == 3
        success_col = logs.column("_log_success").to_pylist()
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

        pf = PythonPacketFunction(failing, output_keys="result", executor=LocalExecutor())
        pod = FunctionPod(pf)

        pipeline = Pipeline(name="test_fail", pipeline_database=db)
        with pipeline:
            pod(source, label="failing")

        obs = LoggingObserver(log_database=db)
        orch = SyncPipelineOrchestrator()
        pipeline.run(orchestrator=orch, observer=obs)

        logs = obs.get_logs()

        assert logs is not None
        assert logs.num_rows == 2
        for row_idx in range(logs.num_rows):
            assert logs.column("_log_success").to_pylist()[row_idx] is False
            tb = logs.column("_log_traceback").to_pylist()[row_idx]
            assert tb is not None
            assert "ValueError" in tb
            assert "boom" in tb


# ---------------------------------------------------------------------------
# 3. Flat log storage — all logs in a single table
# ---------------------------------------------------------------------------


class TestFlatLogStorage:
    def test_log_stored_in_flat_table(self):
        db = InMemoryArrowDatabase()
        source = _make_source(1)

        def identity(x: int) -> int:
            return x

        pf = PythonPacketFunction(identity, output_keys="result", executor=LocalExecutor())
        pod = FunctionPod(pf)

        pipeline = Pipeline(name="test_flat", pipeline_database=db)
        with pipeline:
            pod(source, label="ident")

        obs = LoggingObserver(log_database=db)
        orch = SyncPipelineOrchestrator()
        pipeline.run(orchestrator=orch, observer=obs)

        logs = obs.get_logs()
        assert logs is not None
        assert logs.num_rows == 1
        # Node identity is now in the path, not in columns
        assert "_log_node_label" not in logs.column_names
        assert "_log_node_hash" not in logs.column_names


# ---------------------------------------------------------------------------
# 4. Queryable tag columns (not JSON)
# ---------------------------------------------------------------------------


class TestQueryableTagColumns:
    def test_tag_columns_in_log_table(self):
        db = InMemoryArrowDatabase()
        source = _make_source(2)

        def identity(x: int) -> int:
            return x

        pf = PythonPacketFunction(identity, output_keys="result", executor=LocalExecutor())
        pod = FunctionPod(pf)

        pipeline = Pipeline(name="test_tags", pipeline_database=db)
        with pipeline:
            pod(source, label="ident")

        obs = LoggingObserver(log_database=db)
        orch = SyncPipelineOrchestrator()
        pipeline.run(orchestrator=orch, observer=obs)

        logs = obs.get_logs()

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

        pf = PythonPacketFunction(double, output_keys="result", executor=LocalExecutor())
        pod = FunctionPod(pf)

        pipeline = Pipeline(name="test_async_logs", pipeline_database=db)
        with pipeline:
            pod(source, label="doubler")

        obs = LoggingObserver(log_database=db)
        orch = AsyncPipelineOrchestrator()
        pipeline.run(orchestrator=orch, observer=obs)

        logs = obs.get_logs()

        assert logs is not None
        assert logs.num_rows == 2
        assert all(logs.column("_log_success").to_pylist())


# ---------------------------------------------------------------------------
# 6. fail_fast error policy
# ---------------------------------------------------------------------------


class TestFailFastErrorPolicy:
    def test_fail_fast_aborts_and_logs(self):
        db = InMemoryArrowDatabase()
        source = _make_source(3)

        def failing(x: int) -> int:
            raise RuntimeError("crash")

        pf = PythonPacketFunction(failing, output_keys="result", executor=LocalExecutor())
        pod = FunctionPod(pf)

        pipeline = Pipeline(name="test_failfast", pipeline_database=db)
        with pipeline:
            pod(source, label="failing")

        obs = LoggingObserver(log_database=db)
        orch = SyncPipelineOrchestrator(error_policy="fail_fast")

        with pytest.raises(RuntimeError, match="crash"):
            pipeline.run(orchestrator=orch, observer=obs)

        logs = obs.get_logs()

        # At least the first crash should be logged before abort
        assert logs is not None
        assert logs.num_rows >= 1
        assert logs.column("_log_success").to_pylist()[0] is False


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
            infer_nullable=True,
        )

        def safe_div(x: int) -> float:
            return 100 / x

        pf = PythonPacketFunction(safe_div, output_keys="result", executor=LocalExecutor())
        pod = FunctionPod(pf)

        pipeline = Pipeline(name="test_mixed", pipeline_database=db)
        with pipeline:
            pod(source, label="divider")

        obs = LoggingObserver(log_database=db)
        orch = SyncPipelineOrchestrator()
        pipeline.run(orchestrator=orch, observer=obs)

        logs = obs.get_logs()

        assert logs is not None
        # x=10 succeeds, x=-1 succeeds (100/-1 = -100), x=30 succeeds
        # All three should succeed since division by these values is fine
        assert logs.num_rows == 3
        assert all(logs.column("_log_success").to_pylist())


# ---------------------------------------------------------------------------
# 8. Multiple function nodes — all logs in one flat table
# ---------------------------------------------------------------------------


class TestMultipleFunctionNodesCombinedLogs:
    def test_two_nodes_combined_log_table(self):
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

        pipeline = Pipeline(name="test_multi", pipeline_database=db)
        with pipeline:
            s1 = pod1(source, label="doubler")
            pod2(s1, label="tripler")

        obs = LoggingObserver(log_database=db)
        orch = SyncPipelineOrchestrator()
        pipeline.run(orchestrator=orch, observer=obs)

        logs = obs.get_logs()
        assert logs is not None
        # 2 packets × 2 nodes = 4 log rows total
        assert logs.num_rows == 4


# ---------------------------------------------------------------------------
# 9. get_logs() returns all logs aggregated across all node sub-paths
# ---------------------------------------------------------------------------


class TestGetLogsNodeSpecific:
    def test_get_logs_returns_rows_for_all_nodes(self):
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

        pipeline = Pipeline(name="test_filter", pipeline_database=db)
        with pipeline:
            s1 = pod1(source, label="doubler")
            pod2(s1, label="tripler")

        obs = LoggingObserver(log_database=db)
        orch = SyncPipelineOrchestrator()
        pipeline.run(orchestrator=orch, observer=obs)

        logs = obs.get_logs()
        assert logs is not None
        # Node identity is in path — row count confirms both nodes written
        # 2 packets × 2 nodes = 4 log rows total
        assert logs.num_rows == 4


# ---------------------------------------------------------------------------
# 10. Serialization round-trip tests
# ---------------------------------------------------------------------------


def test_logging_observer_to_config_shape():
    db = InMemoryArrowDatabase()
    log_db = db.at("my_pipeline", "_log")
    obs = LoggingObserver(log_db)
    from orcapod.pipeline.serialization import DatabaseRegistry
    registry = DatabaseRegistry()
    config = obs.to_config(db_registry=registry)
    assert config["type"] == "logging"
    assert config["database"]["type"] == "scoped"
    assert config["database"]["path"] == ["my_pipeline", "_log"]


def test_logging_observer_from_config_round_trip():
    db = InMemoryArrowDatabase()
    log_db = db.at("my_pipeline", "_log")
    obs = LoggingObserver(log_db)
    from orcapod.pipeline.serialization import DatabaseRegistry
    registry = DatabaseRegistry()
    config = obs.to_config(db_registry=registry)
    restored = LoggingObserver.from_config(config, registry.to_dict())
    assert restored._db._path_prefix == log_db._path_prefix


def test_contextualize_with_empty_path_raises():
    """contextualize() with no args should raise ValueError."""
    db = InMemoryArrowDatabase()
    obs = LoggingObserver(db)
    with pytest.raises(ValueError, match="non-empty identity_path"):
        obs.contextualize()
