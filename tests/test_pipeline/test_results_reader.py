"""Tests for ResultsReader."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from orcapod.pipeline.results_reader import ResultsReader


def _write_status_table(
    root: Path,
    pipeline_name: str,
    node_name: str,
    rows: list[dict],
) -> None:
    """Write a status Delta table mimicking StatusObserver output."""
    table_dir = (
        root / pipeline_name / "status" / node_name / "hash_a" / "v0"
        / "python.function.v0" / "node:hash_b"
    )
    table_dir.mkdir(parents=True, exist_ok=True)
    df = pl.DataFrame(rows)
    # Cast any Null-typed columns to String so Delta Lake accepts them.
    for col_name in df.columns:
        if df[col_name].dtype == pl.Null:
            df = df.with_columns(pl.col(col_name).cast(pl.String))
    df.write_delta(str(table_dir))


def _write_log_table(
    root: Path,
    pipeline_name: str,
    node_name: str,
    rows: list[dict],
) -> None:
    """Write a log Delta table mimicking LoggingObserver output."""
    table_dir = (
        root / pipeline_name / "logs" / node_name / "hash_a" / "v0"
        / "python.function.v0" / "node:hash_b"
    )
    table_dir.mkdir(parents=True, exist_ok=True)
    df = pl.DataFrame(rows)
    # Cast any Null-typed columns to String so Delta Lake accepts them.
    for col_name in df.columns:
        if df[col_name].dtype == pl.Null:
            df = df.with_columns(pl.col(col_name).cast(pl.String))
    df.write_delta(str(table_dir))


def _status_row(
    node_label: str,
    state: str,
    *,
    subject: str = "subj_A",
    session_date: str = "2026-01-01",
    error_summary: str | None = None,
    timestamp: str = "2026-01-01T00:00:00+00:00",
) -> dict:
    """Build a single status row dict."""
    return {
        "__record_id": f"rec_{node_label}_{state}_{subject}",
        "_status_id": f"sid_{node_label}_{state}_{subject}",
        "_status_run_id": "run_001",
        "_status_pipeline_uri": "test_pipeline@abc123",
        "_status_node_label": node_label,
        "_status_node_hash": f"hash_{node_label}",
        "_status_state": state,
        "_status_timestamp": timestamp,
        "_status_error_summary": error_summary,
        "subject": subject,
        "session_date": session_date,
        "_tag::source_id::abc123:0": "tag_val",
        "_tag::record_id::abc123:0": "tag_rec",
    }


def _log_row(
    node_label: str,
    *,
    success: bool = True,
    subject: str = "subj_A",
    session_date: str = "2026-01-01",
    traceback: str | None = None,
    stdout: str = "",
    stderr: str = "",
    python_logs: str = "",
    timestamp: str = "2026-01-01T00:00:00+00:00",
) -> dict:
    """Build a single log row dict."""
    return {
        "__record_id": f"rec_{node_label}_{subject}",
        "_log_id": f"lid_{node_label}_{subject}",
        "_log_run_id": "run_001",
        "_log_node_label": node_label,
        "_log_node_hash": f"hash_{node_label}",
        "_log_stdout_log": stdout,
        "_log_stderr_log": stderr,
        "_log_python_logs": python_logs,
        "_log_traceback": traceback,
        "_log_success": success,
        "_log_timestamp": timestamp,
        "subject": subject,
        "session_date": session_date,
        "_tag::source_id::abc123:0": "tag_val",
        "_tag::record_id::abc123:0": "tag_rec",
    }


@pytest.fixture()
def results_root(tmp_path: Path) -> Path:
    """Create a realistic results directory with status and log tables."""
    root = tmp_path / "results_out" / "op_pipeline"

    # Node A: 2 inputs, both succeed
    _write_status_table(root, "my_pipeline", "node_a", [
        _status_row("node_a", "RUNNING", subject="subj_A", timestamp="2026-01-01T00:00:01+00:00"),
        _status_row("node_a", "SUCCESS", subject="subj_A", timestamp="2026-01-01T00:00:02+00:00"),
        _status_row("node_a", "RUNNING", subject="subj_B", timestamp="2026-01-01T00:00:03+00:00"),
        _status_row("node_a", "SUCCESS", subject="subj_B", timestamp="2026-01-01T00:00:04+00:00"),
    ])
    _write_log_table(root, "my_pipeline", "node_a", [
        _log_row("node_a", subject="subj_A"),
        _log_row("node_a", subject="subj_B"),
    ])

    # Node B: 2 inputs, one succeeds, one fails
    _write_status_table(root, "my_pipeline", "node_b", [
        _status_row("node_b", "RUNNING", subject="subj_A", timestamp="2026-01-01T00:01:01+00:00"),
        _status_row("node_b", "SUCCESS", subject="subj_A", timestamp="2026-01-01T00:01:02+00:00"),
        _status_row("node_b", "RUNNING", subject="subj_B", timestamp="2026-01-01T00:01:03+00:00"),
        _status_row(
            "node_b", "FAILED", subject="subj_B",
            timestamp="2026-01-01T00:01:04+00:00",
            error_summary="ValueError: bad input",
        ),
    ])
    _write_log_table(root, "my_pipeline", "node_b", [
        _log_row("node_b", subject="subj_A"),
        _log_row(
            "node_b", subject="subj_B", success=False,
            traceback="Traceback (most recent call last):\n  ...\nValueError: bad input",
            stderr="Error processing subj_B",
        ),
    ])

    # Node C: status only, no logs (e.g. still running or logs not yet written)
    _write_status_table(root, "my_pipeline", "node_c", [
        _status_row("node_c", "RUNNING", subject="subj_A", timestamp="2026-01-01T00:02:01+00:00"),
    ])

    return tmp_path / "results_out"


class TestDiscovery:
    def test_discovers_nodes(self, results_root: Path):
        reader = ResultsReader(results_root)
        assert reader.nodes == ["node_a", "node_b", "node_c"]

    def test_discovers_tag_columns(self, results_root: Path):
        reader = ResultsReader(results_root)
        assert reader.tag_columns == ["session_date", "subject"]

    def test_raises_on_missing_root(self, tmp_path: Path):
        with pytest.raises(ValueError, match="does not exist"):
            ResultsReader(tmp_path / "nonexistent")

    def test_raises_on_empty_root(self, tmp_path: Path):
        empty = tmp_path / "empty"
        empty.mkdir()
        with pytest.raises(ValueError, match="No.*Delta.*tables"):
            ResultsReader(empty)
