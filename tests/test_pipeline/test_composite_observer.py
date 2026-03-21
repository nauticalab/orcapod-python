"""Tests for CompositeObserver.

Verifies that CompositeObserver correctly delegates all hooks to
multiple child observers and that create_packet_logger returns the
first real (non-no-op) logger.
"""

from __future__ import annotations

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
# 1. Integration: logging + status together
# ---------------------------------------------------------------------------


class TestLoggingAndStatusTogether:
    def test_both_observers_populated(self):
        db = InMemoryArrowDatabase()
        source = _make_source(2)

        def double(x: int) -> int:
            return x * 2

        pf = PythonPacketFunction(double, output_keys="result", executor=LocalExecutor())
        pod = FunctionPod(pf)

        pipeline = Pipeline(name="test_composite", pipeline_database=db)
        with pipeline:
            pod(source, label="doubler")

        log_obs = LoggingObserver(log_database=db)
        status_obs = StatusObserver(status_database=db)
        observer = CompositeObserver(log_obs, status_obs)

        orch = SyncPipelineOrchestrator(observer=observer)
        pipeline.run(orchestrator=orch)

        fn_node = _get_function_node(pipeline)

        # Logging observer should have logs
        logs = log_obs.get_logs(pipeline_path=fn_node.pipeline_path)
        assert logs is not None
        assert logs.num_rows == 2

        # Status observer should have status events
        status = status_obs.get_status(pipeline_path=fn_node.pipeline_path)
        assert status is not None
        assert status.num_rows == 4  # 2 × (RUNNING + SUCCESS)


# ---------------------------------------------------------------------------
# 2. create_packet_logger returns the real logger, not no-op
# ---------------------------------------------------------------------------


class TestCreatePacketLoggerDelegation:
    def test_returns_logging_observer_logger(self):
        db = InMemoryArrowDatabase()
        source = _make_source(1)

        def identity(x: int) -> int:
            print("hello")
            return x

        pf = PythonPacketFunction(identity, output_keys="result", executor=LocalExecutor())
        pod = FunctionPod(pf)

        pipeline = Pipeline(name="test_logger_delegation", pipeline_database=db)
        with pipeline:
            pod(source, label="ident")

        log_obs = LoggingObserver(log_database=db)
        status_obs = StatusObserver(status_database=db)
        observer = CompositeObserver(log_obs, status_obs)

        orch = SyncPipelineOrchestrator(observer=observer)
        pipeline.run(orchestrator=orch)

        fn_node = _get_function_node(pipeline)

        # Logs should have captured the print output, proving the real
        # LoggingObserver logger was used (not the no-op)
        logs = log_obs.get_logs(pipeline_path=fn_node.pipeline_path)
        assert logs is not None
        stdout = logs.column("_log_stdout_log").to_pylist()[0]
        assert "hello" in stdout


# ---------------------------------------------------------------------------
# 3. Contextualize returns a composite
# ---------------------------------------------------------------------------


class TestContextualizeReturnsComposite:
    def test_contextualized_composite_delegates(self):
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

        pipeline = Pipeline(name="test_ctx_composite", pipeline_database=db)
        with pipeline:
            s1 = pod1(source, label="doubler")
            pod2(s1, label="tripler")

        log_obs = LoggingObserver(log_database=db)
        status_obs = StatusObserver(status_database=db)
        observer = CompositeObserver(log_obs, status_obs)

        orch = SyncPipelineOrchestrator(observer=observer)
        pipeline.run(orchestrator=orch)

        import networkx as nx

        fn_nodes = [
            n
            for n in nx.topological_sort(pipeline._node_graph)
            if n.node_type == "function"
        ]

        # Both nodes should have both logs and status
        for fn_node in fn_nodes:
            logs = log_obs.get_logs(pipeline_path=fn_node.pipeline_path)
            status = status_obs.get_status(pipeline_path=fn_node.pipeline_path)
            assert logs is not None
            assert status is not None
            assert logs.num_rows == 2
            assert status.num_rows == 4  # 2 × (RUNNING + SUCCESS)


# ---------------------------------------------------------------------------
# 4. Mixed success/failure with composite
# ---------------------------------------------------------------------------


class TestCompositeWithFailures:
    def test_failures_tracked_by_both_observers(self):
        db = InMemoryArrowDatabase()
        source = _make_source(2)

        def failing(x: int) -> int:
            raise ValueError("boom")

        pf = PythonPacketFunction(failing, output_keys="result", executor=LocalExecutor())
        pod = FunctionPod(pf)

        pipeline = Pipeline(name="test_composite_fail", pipeline_database=db)
        with pipeline:
            pod(source, label="failing")

        log_obs = LoggingObserver(log_database=db)
        status_obs = StatusObserver(status_database=db)
        observer = CompositeObserver(log_obs, status_obs)

        orch = SyncPipelineOrchestrator(observer=observer)
        pipeline.run(orchestrator=orch)

        fn_node = _get_function_node(pipeline)

        # Logs should show failures
        logs = log_obs.get_logs(pipeline_path=fn_node.pipeline_path)
        assert logs is not None
        assert all(s is False for s in logs.column("_log_success").to_pylist())

        # Status should show RUNNING + FAILED
        status = status_obs.get_status(pipeline_path=fn_node.pipeline_path)
        assert status is not None
        states = status.column("_status_state").to_pylist()
        assert states.count("RUNNING") == 2
        assert states.count("FAILED") == 2
