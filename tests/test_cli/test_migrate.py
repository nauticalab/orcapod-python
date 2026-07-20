"""Smoke tests for the ``orcapod migrate`` CLI sub-commands."""
from __future__ import annotations

import json
import subprocess
from pathlib import Path

# Repo root: tests/test_cli/test_migrate.py → tests/test_cli/ → tests/ → repo root
_REPO_ROOT = Path(__file__).parent.parent.parent


def _run(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["uv", "run", "orcapod", *args],
        capture_output=True,
        text=True,
        cwd=str(_REPO_ROOT),
    )


class TestMigratePipelineDbCli:
    def test_help_exits_zero(self):
        result = _run("migrate", "pipeline-db", "--help")
        assert result.returncode == 0
        assert "pipeline" in result.stdout.lower() or "PIPELINE" in result.stdout

    def test_dry_run_exits_zero(self, tmp_path):
        """--dry-run with a non-existent DB path exits 0 (nothing to migrate)."""
        db_path = str(tmp_path / "pipeline_db")
        result = _run(
            "migrate", "pipeline-db",
            db_path, db_path, "my_node/path",
            "--dry-run", "--no-progress",
        )
        assert result.returncode == 0

    def test_json_summary_output(self, tmp_path):
        db_path = str(tmp_path / "pipeline_db")
        result = _run(
            "migrate", "pipeline-db",
            db_path, db_path, "my_node/path",
            "--dry-run", "--json-summary", "--no-progress",
        )
        assert result.returncode == 0
        summary = json.loads(result.stdout.strip())
        assert "rows_total" in summary
        assert "dry_run" in summary
        assert summary["dry_run"] is True


class TestMigrateResultDbCli:
    def test_help_exits_zero(self):
        result = _run("migrate", "result-db", "--help")
        assert result.returncode == 0

    def test_dry_run_exits_zero(self, tmp_path):
        db_path = str(tmp_path / "result_db")
        result = _run(
            "migrate", "result-db",
            db_path, "my_pod/path",
            "--dry-run", "--no-progress",
        )
        assert result.returncode == 0

    def test_json_summary_output(self, tmp_path):
        db_path = str(tmp_path / "result_db")
        result = _run(
            "migrate", "result-db",
            db_path, "my_pod/path",
            "--dry-run", "--json-summary", "--no-progress",
        )
        assert result.returncode == 0
        summary = json.loads(result.stdout.strip())
        assert "rows_total" in summary
        assert "dry_run" in summary
        assert summary["dry_run"] is True
