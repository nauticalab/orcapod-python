"""Tests for Pipeline DAG snapshot (PLT-1161).

Verifies that Pipeline.save() populates run_id/snapshot_time when
provided, and that Pipeline.run() writes dag_snapshot.json to the
correct well-known path with a stable run_id.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

import pyarrow as pa
from orcapod.core.function_pod import FunctionPod
from orcapod.core.operators import Join
from orcapod.core.packet_function import PythonPacketFunction
from orcapod.core.sources import ArrowTableSource
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.pipeline import Pipeline


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_source(tag_col: str, packet_col: str, data: dict) -> ArrowTableSource:
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


def add_values(value: int, score: int) -> int:
    return value + score


def _build_and_run_pipeline(pipeline_db, run: bool = True) -> Pipeline:
    """Build a simple two-source + join + function pipeline.

    When ``run=False`` the pipeline is compiled but not executed, so
    that callers can safely invoke ``save()`` or ``_write_dag_snapshot()``,
    both of which require a compiled pipeline.
    """
    src_a, src_b = _make_two_sources()
    pf = PythonPacketFunction(add_values, output_keys="total")
    pod = FunctionPod(packet_function=pf)

    pipeline = Pipeline(name="snap_test", pipeline_database=pipeline_db)
    with pipeline:
        joined = Join()(src_a, src_b)
        pod(joined, label="adder")

    if run:
        pipeline.run()
    else:
        pipeline.compile()
    return pipeline


# ---------------------------------------------------------------------------
# Task 1: save() with run_id / snapshot_time
# ---------------------------------------------------------------------------

class TestSaveRunIdAndSnapshotTime:
    def test_save_populates_run_id_when_provided(self, tmp_path):
        """save() writes run_id into the pipeline block."""
        db = DeltaTableDatabase(base_path=tmp_path / "db")
        pipeline = _build_and_run_pipeline(db, run=False)

        out = tmp_path / "snap.json"
        pipeline.save(str(out), run_id="test-run-id-123")

        data = json.loads(out.read_text())
        assert data["pipeline"]["run_id"] == "test-run-id-123"

    def test_save_populates_snapshot_time_when_provided(self, tmp_path):
        """save() writes snapshot_time into the pipeline block."""
        db = DeltaTableDatabase(base_path=tmp_path / "db")
        pipeline = _build_and_run_pipeline(db, run=False)

        out = tmp_path / "snap.json"
        pipeline.save(str(out), snapshot_time="2026-01-01T00:00:00+00:00")

        data = json.loads(out.read_text())
        assert data["pipeline"]["snapshot_time"] == "2026-01-01T00:00:00+00:00"

    def test_save_run_id_defaults_to_none(self, tmp_path):
        """Existing callers get null run_id (backward compatible)."""
        db = DeltaTableDatabase(base_path=tmp_path / "db")
        pipeline = _build_and_run_pipeline(db, run=False)

        out = tmp_path / "snap.json"
        pipeline.save(str(out))

        data = json.loads(out.read_text())
        assert data["pipeline"]["run_id"] is None
        assert data["pipeline"]["snapshot_time"] is None


# ---------------------------------------------------------------------------
# Task 2: _write_dag_snapshot() method
# ---------------------------------------------------------------------------

class TestWriteDagSnapshot:
    def test_write_dag_snapshot_creates_file_at_correct_path(self, tmp_path):
        """_write_dag_snapshot() creates dag_snapshot.json at
        {db_root}/{pipeline_name}/dag_snapshot.json."""
        db = DeltaTableDatabase(base_path=tmp_path / "db")
        pipeline = _build_and_run_pipeline(db, run=False)

        result = pipeline._write_dag_snapshot(
            run_id="my-run-id",
            snapshot_time="2026-01-01T00:00:00+00:00",
        )

        expected = tmp_path / "db" / "snap_test" / "dag_snapshot.json"
        assert result == expected
        assert expected.exists()

    def test_write_dag_snapshot_content_is_valid_json(self, tmp_path):
        """The snapshot file contains valid JSON with pipeline/nodes/edges."""
        db = DeltaTableDatabase(base_path=tmp_path / "db")
        pipeline = _build_and_run_pipeline(db, run=False)

        pipeline._write_dag_snapshot(
            run_id="my-run-id",
            snapshot_time="2026-01-01T00:00:00+00:00",
        )

        snap = json.loads((tmp_path / "db" / "snap_test" / "dag_snapshot.json").read_text())
        assert "pipeline" in snap
        assert "nodes" in snap
        assert "edges" in snap

    def test_write_dag_snapshot_embeds_run_id(self, tmp_path):
        """Snapshot includes the provided run_id."""
        db = DeltaTableDatabase(base_path=tmp_path / "db")
        pipeline = _build_and_run_pipeline(db, run=False)

        pipeline._write_dag_snapshot(
            run_id="embed-this-run-id",
            snapshot_time="2026-01-01T00:00:00+00:00",
        )

        snap = json.loads((tmp_path / "db" / "snap_test" / "dag_snapshot.json").read_text())
        assert snap["pipeline"]["run_id"] == "embed-this-run-id"

    def test_write_dag_snapshot_embeds_snapshot_time(self, tmp_path):
        """Snapshot includes the provided snapshot_time."""
        db = DeltaTableDatabase(base_path=tmp_path / "db")
        pipeline = _build_and_run_pipeline(db, run=False)

        pipeline._write_dag_snapshot(
            run_id="my-run-id",
            snapshot_time="2026-04-03T12:00:00+00:00",
        )

        snap = json.loads((tmp_path / "db" / "snap_test" / "dag_snapshot.json").read_text())
        assert snap["pipeline"]["snapshot_time"] == "2026-04-03T12:00:00+00:00"

    def test_write_dag_snapshot_returns_none_for_in_memory_db(self):
        """_write_dag_snapshot() returns None when using InMemoryArrowDatabase
        (no local filesystem root)."""
        db = InMemoryArrowDatabase()
        pipeline = _build_and_run_pipeline(db, run=False)

        result = pipeline._write_dag_snapshot(
            run_id="my-run-id",
            snapshot_time="2026-01-01T00:00:00+00:00",
        )

        assert result is None

    def test_write_dag_snapshot_returns_none_without_pipeline_database(self):
        """_write_dag_snapshot() returns None when no pipeline_database is set."""
        src_a, _ = _make_two_sources()
        pipeline = Pipeline(name="no_db", pipeline_database=None)
        with pipeline:
            _ = src_a  # just register the source

        result = pipeline._write_dag_snapshot(
            run_id="x",
            snapshot_time="2026-01-01T00:00:00+00:00",
        )
        assert result is None


# ---------------------------------------------------------------------------
# Task 3: run() writes snapshot at start, run_id is consistent
# ---------------------------------------------------------------------------

class TestRunWritesDagSnapshot:
    def test_run_writes_dag_snapshot_to_correct_path(self, tmp_path):
        """pipeline.run() creates dag_snapshot.json at the pipeline DB root."""
        db = DeltaTableDatabase(base_path=tmp_path / "db")
        pipeline = _build_and_run_pipeline(db, run=True)

        expected = tmp_path / "db" / "snap_test" / "dag_snapshot.json"
        assert expected.exists(), "dag_snapshot.json should exist after run()"

    def test_run_snapshot_contains_nodes_and_edges(self, tmp_path):
        """Snapshot written by run() contains nodes and edges entries."""
        db = DeltaTableDatabase(base_path=tmp_path / "db")
        _build_and_run_pipeline(db, run=True)

        snap = json.loads(
            (tmp_path / "db" / "snap_test" / "dag_snapshot.json").read_text()
        )
        assert "nodes" in snap
        assert "edges" in snap
        assert len(snap["nodes"]) > 0

    def test_run_snapshot_has_non_null_run_id(self, tmp_path):
        """Snapshot written by run() has a non-null run_id."""
        db = DeltaTableDatabase(base_path=tmp_path / "db")
        _build_and_run_pipeline(db, run=True)

        snap = json.loads(
            (tmp_path / "db" / "snap_test" / "dag_snapshot.json").read_text()
        )
        assert snap["pipeline"]["run_id"] is not None
        assert isinstance(snap["pipeline"]["run_id"], str)
        assert len(snap["pipeline"]["run_id"]) > 0

    def test_run_snapshot_has_non_null_snapshot_time(self, tmp_path):
        """Snapshot written by run() has a non-null snapshot_time."""
        db = DeltaTableDatabase(base_path=tmp_path / "db")
        _build_and_run_pipeline(db, run=True)

        snap = json.loads(
            (tmp_path / "db" / "snap_test" / "dag_snapshot.json").read_text()
        )
        assert snap["pipeline"]["snapshot_time"] is not None
        assert isinstance(snap["pipeline"]["snapshot_time"], str)

    def test_run_snapshot_run_id_matches_observer_run_id(self, tmp_path):
        """The run_id in dag_snapshot.json matches the run_id
        received by the observer's on_run_start hook."""
        from orcapod.protocols.observability_protocols import ExecutionObserverProtocol

        class CapturingObserver:
            """Minimal observer that captures run_id from on_run_start."""
            def __init__(self):
                self.run_id: str | None = None

            def on_run_start(self, run_id: str, **kwargs) -> None:
                self.run_id = run_id

            def on_run_end(self, run_id: str) -> None:
                pass

            def on_node_start(self, *args, **kwargs) -> None:
                pass

            def on_node_end(self, *args, **kwargs) -> None:
                pass

            def on_packet_start(self, *args, **kwargs):
                pass

            def on_packet_end(self, *args, **kwargs) -> None:
                pass

            def on_packet_error(self, *args, **kwargs) -> None:
                pass

            def contextualize(self, *args, **kwargs):
                return self

            def create_packet_logger(self, *args, **kwargs):
                from orcapod.pipeline.observer import NoOpLogger
                return NoOpLogger()

        db = DeltaTableDatabase(base_path=tmp_path / "db")
        src_a, src_b = _make_two_sources()
        pf = PythonPacketFunction(add_values, output_keys="total")
        pod = FunctionPod(packet_function=pf)

        pipeline = Pipeline(name="snap_test", pipeline_database=db)
        with pipeline:
            joined = Join()(src_a, src_b)
            pod(joined, label="adder")

        observer = CapturingObserver()
        pipeline.run(observer=observer)

        snap = json.loads(
            (tmp_path / "db" / "snap_test" / "dag_snapshot.json").read_text()
        )
        assert snap["pipeline"]["run_id"] == observer.run_id, (
            "run_id in dag_snapshot.json must match the run_id passed to observer.on_run_start()"
        )

    def test_run_without_local_db_does_not_error(self):
        """pipeline.run() with InMemoryArrowDatabase completes without error
        (snapshot is silently skipped)."""
        db = InMemoryArrowDatabase()
        pipeline = _build_and_run_pipeline(db, run=True)
        # No exception = pass

    def test_run_snapshot_written_before_node_execution(self, tmp_path, monkeypatch):
        """dag_snapshot.json exists before any source or function node executes."""
        from orcapod.core.nodes import FunctionNode, SourceNode

        snapshot_existed_before_execute: list[bool] = []
        snap_path = tmp_path / "db" / "snap_test" / "dag_snapshot.json"

        original_fn_execute = FunctionNode.execute
        original_src_execute = SourceNode.execute

        def spy_fn_execute(self_node, *args, **kwargs):
            snapshot_existed_before_execute.append(snap_path.exists())
            return original_fn_execute(self_node, *args, **kwargs)

        def spy_src_execute(self_node, *args, **kwargs):
            snapshot_existed_before_execute.append(snap_path.exists())
            return original_src_execute(self_node, *args, **kwargs)

        monkeypatch.setattr(FunctionNode, "execute", spy_fn_execute)
        monkeypatch.setattr(SourceNode, "execute", spy_src_execute)

        db = DeltaTableDatabase(base_path=tmp_path / "db")
        _build_and_run_pipeline(db, run=True)

        # Every node execution should have seen the snapshot already present
        assert snapshot_existed_before_execute, "No node was executed (test setup issue)"
        assert all(snapshot_existed_before_execute), (
            "dag_snapshot.json must be written before any node executes"
        )

    def test_run_overwrites_snapshot_on_second_run(self, tmp_path):
        """A second run() overwrites dag_snapshot.json with a new run_id."""
        db = DeltaTableDatabase(base_path=tmp_path / "db")
        pipeline = _build_and_run_pipeline(db, run=True)
        snap_path = tmp_path / "db" / "snap_test" / "dag_snapshot.json"

        first_run_id = json.loads(snap_path.read_text())["pipeline"]["run_id"]

        pipeline.run()
        second_run_id = json.loads(snap_path.read_text())["pipeline"]["run_id"]

        assert first_run_id != second_run_id, (
            "Second run() should produce a new run_id in dag_snapshot.json"
        )

    def test_run_snapshot_pipeline_name_matches(self, tmp_path):
        """Snapshot pipeline.name matches the Pipeline's name."""
        db = DeltaTableDatabase(base_path=tmp_path / "db")
        _build_and_run_pipeline(db, run=True)

        snap = json.loads(
            (tmp_path / "db" / "snap_test" / "dag_snapshot.json").read_text()
        )
        assert snap["pipeline"]["name"] == ["snap_test"]

    def test_run_snapshot_contains_correct_node_types(self, tmp_path):
        """Snapshot nodes include source, operator, and function nodes."""
        db = DeltaTableDatabase(base_path=tmp_path / "db")
        _build_and_run_pipeline(db, run=True)

        snap = json.loads(
            (tmp_path / "db" / "snap_test" / "dag_snapshot.json").read_text()
        )
        node_types = {v["node_type"] for v in snap["nodes"].values()}
        assert "source" in node_types
        assert "operator" in node_types
        assert "function" in node_types
