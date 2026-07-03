"""CLI tests for ``orcapod warm-cache``."""

from __future__ import annotations

import pytest
from typer.testing import CliRunner


@pytest.fixture
def runner():
    return CliRunner()


class TestWarmCacheCLI:
    def test_help_exits_zero(self, runner):
        from orcapod.cli import app

        result = runner.invoke(app, ["warm-cache", "--help"])
        assert result.exit_code == 0
        assert "PATH" in result.output

    def test_basic_run(self, runner, tmp_path):
        db = tmp_path / "cache.db"
        f = tmp_path / "f.bin"
        f.write_bytes(b"x" * 20)

        from orcapod.cli import app

        result = runner.invoke(
            app,
            [
                "warm-cache",
                str(tmp_path),
                "--min-size", "0.00002",   # int(0.00002 * 1024 * 1024) = 20 bytes threshold; 20-byte file qualifies
                "--db-path", str(db),
            ],
        )
        assert result.exit_code == 0, result.output
        assert "1 hashed" in result.output

    def test_already_cached_on_second_run(self, runner, tmp_path):
        db = tmp_path / "cache.db"
        f = tmp_path / "f.bin"
        f.write_bytes(b"x" * 20)

        from orcapod.cli import app

        args = [
            "warm-cache",
            str(tmp_path),
            "--min-size", "0.00002",
            "--db-path", str(db),
        ]
        runner.invoke(app, args)  # first run — populates cache
        result = runner.invoke(app, args)  # second run — all cached

        assert result.exit_code == 0
        assert "1 already cached" in result.output

    def test_nonexistent_path_exits_nonzero(self, runner):
        from orcapod.cli import app

        result = runner.invoke(app, ["warm-cache", "/nonexistent_xyz_abc_987"])
        assert result.exit_code != 0

    def test_min_size_default_shown_in_help(self, runner):
        from orcapod.cli import app

        result = runner.invoke(app, ["warm-cache", "--help"])
        assert "500 MB" in result.output  # default 500 MB should appear

    def test_workers_option_accepted(self, runner, tmp_path):
        db = tmp_path / "cache.db"
        f = tmp_path / "f.bin"
        f.write_bytes(b"x" * 20)

        from orcapod.cli import app

        result = runner.invoke(
            app,
            [
                "warm-cache",
                str(tmp_path),
                "--min-size", "0.00002",
                "--db-path", str(db),
                "--workers", "2",
            ],
        )
        assert result.exit_code == 0, result.output

    def test_file_path_exits_nonzero(self, runner, tmp_path):
        """Passing a file (not a directory) as PATH exits with a non-zero code."""
        f = tmp_path / "notadir.bin"
        f.write_bytes(b"x" * 10)

        from orcapod.cli import app

        result = runner.invoke(app, ["warm-cache", str(f)])
        assert result.exit_code != 0
        assert "not a directory" in result.output

    def test_speed_line_shown_when_files_hashed(self, runner, tmp_path):
        """'Average hashing speed:' line appears when at least one file was hashed."""
        db = tmp_path / "cache.db"
        f = tmp_path / "f.bin"
        f.write_bytes(b"x" * 20)

        from orcapod.cli import app

        result = runner.invoke(
            app,
            [
                "warm-cache",
                str(tmp_path),
                "--min-size", "0.00002",
                "--db-path", str(db),
            ],
        )
        assert result.exit_code == 0, result.output
        assert "Average hashing speed:" in result.output

    def test_speed_line_not_shown_when_nothing_hashed(self, runner, tmp_path):
        """'Average hashing speed:' line is suppressed when no files were hashed."""
        db = tmp_path / "cache.db"
        # File below threshold — will be skipped, not hashed.
        f = tmp_path / "tiny.bin"
        f.write_bytes(b"x" * 5)

        from orcapod.cli import app

        result = runner.invoke(
            app,
            [
                "warm-cache",
                str(tmp_path),
                "--min-size", "100",   # 100 MB threshold — tiny.bin won't qualify
                "--db-path", str(db),
            ],
        )
        assert result.exit_code == 0, result.output
        assert "Average hashing speed:" not in result.output

    def test_summary_output_contains_counts(self, runner, tmp_path):
        """Output summary line contains hashed, already cached, skipped, and errors counts."""
        db = tmp_path / "cache.db"
        f = tmp_path / "f.bin"
        f.write_bytes(b"x" * 20)

        from orcapod.cli import app

        result = runner.invoke(
            app,
            [
                "warm-cache",
                str(tmp_path),
                "--min-size", "0.00002",
                "--db-path", str(db),
            ],
        )
        assert result.exit_code == 0, result.output
        assert "hashed" in result.output
        assert "already cached" in result.output
        assert "skipped" in result.output
        assert "errors" in result.output
