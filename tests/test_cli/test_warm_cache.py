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
