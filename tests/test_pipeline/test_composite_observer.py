"""Tests for CompositeObserver.

Verifies that CompositeObserver correctly delegates all hooks to
multiple child observers and that create_packet_logger returns the
first real (non-no-op) logger.
"""

from __future__ import annotations

import pytest
import pyarrow as pa

from orcapod.core.executors import LocalExecutor
from orcapod.core.function_pod import FunctionPod
from orcapod.core.packet_function import PythonPacketFunction
from orcapod.core.sources.arrow_table_source import ArrowTableSource
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline import (
    Pipeline,
    SyncPipelineOrchestrator,
)
from orcapod.pipeline.composite_observer import CompositeObserver
from orcapod.pipeline.logging_observer import LoggingObserver, PacketLogger
from orcapod.pipeline.observer import NoOpLogger
from orcapod.pipeline.serialization import DatabaseRegistry, resolve_observer_from_config
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


# ---------------------------------------------------------------------------
# 1. Integration: logging + status together
# ---------------------------------------------------------------------------


class TestLoggingAndStatusTogether:
    def test_both_observers_populated(self):
        pipeline_db = InMemoryArrowDatabase()
        obs_db = InMemoryArrowDatabase()
        source = _make_source(2)

        def double(x: int) -> int:
            return x * 2

        pf = PythonPacketFunction(double, output_keys="result", executor=LocalExecutor())
        pod = FunctionPod(pf)

        pipeline = Pipeline(name="test_composite", pipeline_database=pipeline_db)
        with pipeline:
            pod(source, label="doubler")

        log_obs = LoggingObserver(log_database=obs_db.at("test_composite", "_log"))
        status_obs = StatusObserver(status_database=obs_db.at("test_composite", "_status"))
        observer = CompositeObserver(log_obs, status_obs)

        orch = SyncPipelineOrchestrator()
        pipeline.run(orchestrator=orch, observer=observer)

        # Logging observer should have logs
        logs = log_obs.get_logs()
        assert logs is not None
        assert logs.num_rows == 2

        # Status observer should have status events
        status = status_obs.get_status()
        assert status is not None
        assert status.num_rows == 4  # 2 × (RUNNING + SUCCESS)


# ---------------------------------------------------------------------------
# 2. create_packet_logger returns the real logger, not no-op
# ---------------------------------------------------------------------------


class TestCreatePacketLoggerDelegation:
    def test_returns_logging_observer_logger(self):
        pipeline_db = InMemoryArrowDatabase()
        obs_db = InMemoryArrowDatabase()
        source = _make_source(1)

        def identity(x: int) -> int:
            print("hello")
            return x

        pf = PythonPacketFunction(identity, output_keys="result", executor=LocalExecutor())
        pod = FunctionPod(pf)

        pipeline = Pipeline(name="test_logger_delegation", pipeline_database=pipeline_db)
        with pipeline:
            pod(source, label="ident")

        log_obs = LoggingObserver(log_database=obs_db.at("test_logger_delegation", "_log"))
        status_obs = StatusObserver(status_database=obs_db.at("test_logger_delegation", "_status"))
        observer = CompositeObserver(log_obs, status_obs)

        orch = SyncPipelineOrchestrator()
        pipeline.run(orchestrator=orch, observer=observer)

        # Logs should have captured the print output, proving the real
        # LoggingObserver logger was used (not the no-op)
        logs = log_obs.get_logs()
        assert logs is not None
        stdout = logs.column("_log_stdout_log").to_pylist()[0]
        assert "hello" in stdout


# ---------------------------------------------------------------------------
# 3. Contextualize returns a composite
# ---------------------------------------------------------------------------


class TestContextualizeReturnsComposite:
    def test_contextualized_composite_delegates(self):
        pipeline_db = InMemoryArrowDatabase()
        obs_db = InMemoryArrowDatabase()
        source = _make_source(2)

        def double(x: int) -> int:
            return x * 2

        def triple(result: int) -> int:
            return result * 3

        pf1 = PythonPacketFunction(double, output_keys="result", executor=LocalExecutor())
        pod1 = FunctionPod(pf1)
        pf2 = PythonPacketFunction(triple, output_keys="final", executor=LocalExecutor())
        pod2 = FunctionPod(pf2)

        pipeline = Pipeline(name="test_ctx_composite", pipeline_database=pipeline_db)
        with pipeline:
            s1 = pod1(source, label="doubler")
            pod2(s1, label="tripler")

        log_obs = LoggingObserver(log_database=obs_db.at("test_ctx_composite", "_log"))
        status_obs = StatusObserver(status_database=obs_db.at("test_ctx_composite", "_status"))
        observer = CompositeObserver(log_obs, status_obs)

        orch = SyncPipelineOrchestrator()
        pipeline.run(orchestrator=orch, observer=observer)

        # Both observers should have received events for both function nodes
        # 2 nodes × 2 packets each = 4 log rows total
        # 2 nodes × 2 packets × 2 events (RUNNING + SUCCESS) = 8 status rows total
        logs = log_obs.get_logs()
        status = status_obs.get_status()
        assert logs is not None
        assert status is not None
        assert logs.num_rows == 4   # 2 nodes × 2 packets
        assert status.num_rows == 8  # 2 nodes × 2 × (RUNNING + SUCCESS)
        # Both node labels appear in logs
        node_labels = set(logs.column("_log_node_label").to_pylist())
        assert "doubler" in node_labels
        assert "tripler" in node_labels


# ---------------------------------------------------------------------------
# 4. Mixed success/failure with composite
# ---------------------------------------------------------------------------


class TestCompositeWithFailures:
    def test_failures_tracked_by_both_observers(self):
        pipeline_db = InMemoryArrowDatabase()
        obs_db = InMemoryArrowDatabase()
        source = _make_source(2)

        def failing(x: int) -> int:
            raise ValueError("boom")

        pf = PythonPacketFunction(failing, output_keys="result", executor=LocalExecutor())
        pod = FunctionPod(pf)

        pipeline = Pipeline(name="test_composite_fail", pipeline_database=pipeline_db)
        with pipeline:
            pod(source, label="failing")

        log_obs = LoggingObserver(log_database=obs_db.at("test_composite_fail", "_log"))
        status_obs = StatusObserver(status_database=obs_db.at("test_composite_fail", "_status"))
        observer = CompositeObserver(log_obs, status_obs)

        orch = SyncPipelineOrchestrator()
        pipeline.run(orchestrator=orch, observer=observer)

        # Logs should show failures
        logs = log_obs.get_logs()
        assert logs is not None
        assert all(s is False for s in logs.column("_log_success").to_pylist())

        # Status should show RUNNING + FAILED
        status = status_obs.get_status()
        assert status is not None
        states = status.column("_status_state").to_pylist()
        assert states.count("RUNNING") == 2
        assert states.count("FAILED") == 2


# ---------------------------------------------------------------------------
# 5. Serialization: to_config / from_config / OBSERVER_REGISTRY
# ---------------------------------------------------------------------------


def test_composite_observer_to_config_shape():
    db = InMemoryArrowDatabase()
    status_obs = StatusObserver(db.at("p", "_status"))
    log_obs = LoggingObserver(db.at("p", "_log"))
    composite = CompositeObserver(status_obs, log_obs)
    registry = DatabaseRegistry()
    config = composite.to_config(db_registry=registry)
    assert config["type"] == "composite"
    assert len(config["observers"]) == 2
    assert config["observers"][0]["type"] == "status"
    assert config["observers"][1]["type"] == "logging"


def test_composite_observer_round_trip():
    db = InMemoryArrowDatabase()
    status_obs = StatusObserver(db.at("p", "_status"))
    log_obs = LoggingObserver(db.at("p", "_log"))
    composite = CompositeObserver(status_obs, log_obs)
    registry = DatabaseRegistry()
    config = composite.to_config(db_registry=registry)
    restored = resolve_observer_from_config(config, registry.to_dict())
    assert len(restored._observers) == 2


def test_resolve_observer_from_config_unknown_type_raises():
    with pytest.raises(ValueError, match="Unknown observer type"):
        resolve_observer_from_config({"type": "unknown"})


def test_composite_contextualize_with_empty_path_raises():
    from orcapod.databases.in_memory_databases import InMemoryArrowDatabase
    from orcapod.pipeline.status_observer import StatusObserver
    db = InMemoryArrowDatabase()
    obs = CompositeObserver(StatusObserver(db))
    with pytest.raises(ValueError, match="non-empty identity_path"):
        obs.contextualize()
