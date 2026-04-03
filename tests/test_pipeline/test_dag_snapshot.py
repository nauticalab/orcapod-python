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
