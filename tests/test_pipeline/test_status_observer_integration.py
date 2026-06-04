"""Integration tests for StatusObserver with real pipelines.

Exercises the full status tracking pipeline: observer hooks →
StatusObserver → database, using InMemoryArrowDatabase and real
PipelineJob objects.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.core.executors import LocalPythonFunctionExecutor
from orcapod.core.function_pod import FunctionPod
from orcapod.core.data_function import PythonDataFunction
from orcapod.core.sources.arrow_table_source import ArrowTableSource
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline.serialization import DatabaseRegistry
from orcapod.pipeline import (
    AsyncPipelineOrchestrator,
    SyncPipelineOrchestrator,
)
from orcapod.pipeline.job import PipelineJob
from orcapod.pipeline.status_observer import StatusObserver


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_source(n: int = 3) -> ArrowTableSource:
    table = pa.table({
        "id": pa.array([str(i) for i in range(n)], type=pa.large_string()),
        "x": pa.array([10 * (i + 1) for i in range(n)], type=pa.int64()),
    })
    return ArrowTableSource(table, tag_columns=["id"], infer_nullable=True)


# ---------------------------------------------------------------------------
# 1. Sync pipeline — success status events
# ---------------------------------------------------------------------------


class TestSyncPipelineSuccessStatus:
    def test_success_produces_running_and_success_events(self):
        db = InMemoryArrowDatabase()
        source = _make_source()

        def double(x: int) -> int:
            return x * 2

        pf = PythonDataFunction(double, output_keys="result", executor=LocalPythonFunctionExecutor())
        pod = FunctionPod(pf)

        job = PipelineJob(name="test_status", store=db)
        with job:
            pod(source, label="doubler")

        obs = StatusObserver(status_database=db)
        job.run(observer=obs)

        status = obs.get_status()

        assert status is not None
        # 3 data × 2 events each (RUNNING + SUCCESS) = 6 rows
        assert status.num_rows == 6

        states = status.column("_status_state").to_pylist()
        assert states.count("RUNNING") == 3
        assert states.count("SUCCESS") == 3


# ---------------------------------------------------------------------------
# 2. Failing data → FAILED status with error summary
# ---------------------------------------------------------------------------


class TestFailingDatasStatus:
    def test_failure_status_with_error_summary(self):
        db = InMemoryArrowDatabase()
        source = _make_source(2)

        def failing(x: int) -> int:
            raise ValueError("boom")

        pf = PythonDataFunction(failing, output_keys="result", executor=LocalPythonFunctionExecutor())
        pod = FunctionPod(pf)

        job = PipelineJob(name="test_fail_status", store=db)
        with job:
            pod(source, label="failing")

        obs = StatusObserver(status_database=db)
        job.run(observer=obs)

        status = obs.get_status()

        assert status is not None
        # 2 data × 2 events each (RUNNING + FAILED)
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
# 3. Flat status storage (new design: single status table, not path-mirrored)
# ---------------------------------------------------------------------------


class TestFlatStatusStorage:
    def test_status_stored_in_flat_table(self):
        db = InMemoryArrowDatabase()
        source = _make_source(1)

        def identity(x: int) -> int:
            return x

        pf = PythonDataFunction(identity, output_keys="result", executor=LocalPythonFunctionExecutor())
        pod = FunctionPod(pf)

        job = PipelineJob(name="test_flat_status", store=db)
        with job:
            pod(source, label="ident")

        obs = StatusObserver(status_database=db)
        job.run(observer=obs)

        # All status is now aggregated via get_status()
        all_status = obs.get_status()
        assert all_status is not None
        assert all_status.num_rows == 2  # RUNNING + SUCCESS


# ---------------------------------------------------------------------------
# 4. Queryable tag columns
# ---------------------------------------------------------------------------


class TestQueryableTagColumns:
    def test_tag_columns_in_status_table(self):
        db = InMemoryArrowDatabase()
        source = _make_source(2)

        def identity(x: int) -> int:
            return x

        pf = PythonDataFunction(identity, output_keys="result", executor=LocalPythonFunctionExecutor())
        pod = FunctionPod(pf)

        job = PipelineJob(name="test_tags_status", store=db)
        with job:
            pod(source, label="ident")

        obs = StatusObserver(status_database=db)
        job.run(observer=obs)

        status = obs.get_status()

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

        pf = PythonDataFunction(double, output_keys="result", executor=LocalPythonFunctionExecutor())
        pod = FunctionPod(pf)

        job = PipelineJob(name="test_async_status", store=db)
        with job:
            pod(source, label="doubler")

        obs = StatusObserver(status_database=db)
        job.run(orchestrator=AsyncPipelineOrchestrator(), observer=obs)

        status = obs.get_status()

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

        pf = PythonDataFunction(failing, output_keys="result", executor=LocalPythonFunctionExecutor())
        pod = FunctionPod(pf)

        job = PipelineJob(name="test_failfast_status", store=db)
        with job:
            pod(source, label="failing")

        obs = StatusObserver(status_database=db)
        orch = SyncPipelineOrchestrator(error_policy="fail_fast")

        with pytest.raises(RuntimeError, match="crash"):
            job.run(orchestrator=orch, observer=obs)

        status = obs.get_status()

        # At least one RUNNING + one FAILED before abort
        assert status is not None
        assert status.num_rows >= 2
        states = status.column("_status_state").to_pylist()
        assert "RUNNING" in states
        assert "FAILED" in states


# ---------------------------------------------------------------------------
# 7. Mixed success/failure — correct status per data
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
            infer_nullable=True,
        )

        def safe_div(x: int) -> float:
            return 100 / x  # x=0 will raise ZeroDivisionError

        pf = PythonDataFunction(safe_div, output_keys="result", executor=LocalPythonFunctionExecutor())
        pod = FunctionPod(pf)

        job = PipelineJob(name="test_mixed_status", store=db)
        with job:
            pod(source, label="divider")

        obs = StatusObserver(status_database=db)
        job.run(observer=obs)

        status = obs.get_status()

        assert status is not None
        # 3 RUNNING + 2 SUCCESS + 1 FAILED = 6
        assert status.num_rows == 6

        states = status.column("_status_state").to_pylist()
        assert states.count("RUNNING") == 3
        assert states.count("SUCCESS") == 2
        assert states.count("FAILED") == 1


# ---------------------------------------------------------------------------
# 8. Multiple function nodes — each gets own rows in combined status table
# ---------------------------------------------------------------------------


class TestMultipleFunctionNodesSeparateStatus:
    def test_two_nodes_combined_status_table(self):
        db = InMemoryArrowDatabase()
        source = _make_source(2)

        def double(x: int) -> int:
            return x * 2

        def triple(result: int) -> int:
            return result * 3

        pf1 = PythonDataFunction(double, output_keys="result", executor=LocalPythonFunctionExecutor())
        pod1 = FunctionPod(pf1)
        pf2 = PythonDataFunction(triple, output_keys="final", executor=LocalPythonFunctionExecutor())
        pod2 = FunctionPod(pf2)

        job = PipelineJob(name="test_multi_status", store=db)
        with job:
            s1 = pod1(source, label="doubler")
            pod2(s1, label="tripler")

        obs = StatusObserver(status_database=db)
        job.run(observer=obs)

        # Combined status contains events for both nodes
        status = obs.get_status()
        assert status is not None
        # Each node: 2 data × 2 events = 4 rows per node → 8 total
        assert status.num_rows == 8


# ---------------------------------------------------------------------------
# 9. get_status() aggregates across all node sub-paths
# ---------------------------------------------------------------------------


class TestGetStatusNodeSpecific:
    def test_get_status_returns_rows_for_both_nodes(self):
        """get_status() should return aggregated rows covering every node."""
        db = InMemoryArrowDatabase()
        source = _make_source(2)

        def double(x: int) -> int:
            return x * 2

        def triple(result: int) -> int:
            return result * 3

        pf1 = PythonDataFunction(double, output_keys="result", executor=LocalPythonFunctionExecutor())
        pod1 = FunctionPod(pf1)
        pf2 = PythonDataFunction(triple, output_keys="final", executor=LocalPythonFunctionExecutor())
        pod2 = FunctionPod(pf2)

        job = PipelineJob(name="test_filter_status", store=db)
        with job:
            s1 = pod1(source, label="doubler")
            pod2(s1, label="tripler")

        obs = StatusObserver(status_database=db)
        job.run(observer=obs)

        # get_status() aggregates across all node sub-paths:
        # 2 data × 2 nodes × 2 events = 8 rows total
        all_status = obs.get_status()
        assert all_status is not None
        assert all_status.num_rows == 8


# ---------------------------------------------------------------------------
# 10. Status columns have correct schema
# ---------------------------------------------------------------------------


class TestStatusSchema:
    def test_all_expected_columns_present(self):
        db = InMemoryArrowDatabase()
        source = _make_source(1)

        def identity(x: int) -> int:
            return x

        pf = PythonDataFunction(identity, output_keys="result", executor=LocalPythonFunctionExecutor())
        pod = FunctionPod(pf)

        job = PipelineJob(name="test_schema", store=db)
        with job:
            pod(source, label="ident")

        obs = StatusObserver(status_database=db)
        job.run(observer=obs)

        status = obs.get_status()

        expected_system_cols = {
            "_status_id",
            "_status_run_id",
            "_status_state",
            "_status_timestamp",
            "_status_error_summary",
        }
        assert expected_system_cols.issubset(set(status.column_names))
        # Node identity is now in the path, not in columns
        assert "_status_node_label" not in status.column_names
        assert "_status_node_hash" not in status.column_names

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

        pf = PythonDataFunction(identity, output_keys="result", executor=LocalPythonFunctionExecutor())
        pod = FunctionPod(pf)

        job = PipelineJob(name="test_runid", store=db)
        with job:
            pod(source, label="ident")

        obs = StatusObserver(status_database=db)
        job.run(observer=obs)

        status = obs.get_status()

        # run_id is auto-generated by PipelineJob.run(); verify it is populated
        # and consistent across all status rows for this run.
        run_ids = set(status.column("_status_run_id").to_pylist())
        assert len(run_ids) == 1
        (run_id,) = run_ids
        assert run_id is not None
        assert len(run_id) > 0


# ---------------------------------------------------------------------------
# 12. Serialization: to_config shape
# ---------------------------------------------------------------------------


def test_status_observer_to_config_shape():
    db = InMemoryArrowDatabase()
    status_db = db.at("my_pipeline", "_status")
    obs = StatusObserver(status_db)
    registry = DatabaseRegistry()
    config = obs.to_config(db_registry=registry)
    assert config["type"] == "status"
    assert config["database"]["type"] == "scoped"
    assert config["database"]["path"] == ["my_pipeline", "_status"]


# ---------------------------------------------------------------------------
# 13. Serialization: from_config round-trip
# ---------------------------------------------------------------------------


def test_status_observer_from_config_round_trip():
    db = InMemoryArrowDatabase()
    status_db = db.at("my_pipeline", "_status")
    obs = StatusObserver(status_db)
    registry = DatabaseRegistry()
    config = obs.to_config(db_registry=registry)
    db_dict = registry.to_dict()
    restored = StatusObserver.from_config(config, db_dict)
    assert restored._db._path_prefix == status_db._path_prefix


# ---------------------------------------------------------------------------
# 14. Empty identity_path guard
# ---------------------------------------------------------------------------


def test_contextualize_with_empty_path_raises():
    """contextualize() with no args should raise ValueError (not silently use fallback)."""
    db = InMemoryArrowDatabase()
    obs = StatusObserver(db)
    with pytest.raises(ValueError, match="non-empty identity_path"):
        obs.contextualize()  # no args → empty identity_path → should raise
