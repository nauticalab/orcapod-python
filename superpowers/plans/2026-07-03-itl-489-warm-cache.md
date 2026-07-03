# Warm Cache Implementation Plan (ITL-489)

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `populate_hash_cache()` utility and `orcapod warm-cache` CLI subcommand that pre-populate the SQLite file-hash cache for large ephys recordings (default threshold: 500 MB).

**Architecture:** A new `src/orcapod/hashing/cache_population.py` module provides `CachePopulationStats` and `populate_hash_cache()`, which walks a directory tree recursively (via an explicit DFS stack on `UPath.iterdir()`), skips files below the size threshold, and uses `SqliteHashCacher` + `FileHasher` from ITL-472 to hash and cache qualifying files. A new `src/orcapod/cli/` package provides the `orcapod` entry point (Typer app) with a `warm-cache` subcommand that wraps this function with human-readable CLI options and output.

**Tech Stack:** Python 3.11, `upath.UPath`, `orcapod.hashing.file_hashers.{FileHasher,FileHashKey}`, `orcapod.hashing.hash_cachers.SqliteHashCacher`, `typer>=0.12`, `pytest`.

---

## File map

| File | Change |
|---|---|
| `src/orcapod/hashing/cache_population.py` | **NEW** — `CachePopulationStats`, `populate_hash_cache()` |
| `src/orcapod/hashing/__init__.py` | **Modify** — export `CachePopulationStats`, `populate_hash_cache` |
| `src/orcapod/cli/__init__.py` | **NEW** — Typer root app |
| `src/orcapod/cli/warm_cache.py` | **NEW** — `warm-cache` subcommand |
| `tests/test_hashing/test_cache_population.py` | **NEW** — unit tests for `populate_hash_cache` |
| `tests/test_cli/__init__.py` | **NEW** — empty, marks test package |
| `tests/test_cli/test_warm_cache.py` | **NEW** — CLI tests via `typer.testing.CliRunner` |
| `pyproject.toml` | **Modify** — add `typer>=0.12` dependency, add `[project.scripts]` |

---

## Task 1: `CachePopulationStats` dataclass

**Files:**
- Create: `src/orcapod/hashing/cache_population.py`
- Create: `tests/test_hashing/test_cache_population.py`

- [ ] **Step 1.1: Create the test file with two failing tests**

Create `tests/test_hashing/test_cache_population.py`:

```python
"""Tests for populate_hash_cache() and CachePopulationStats."""

import pytest


class TestCachePopulationStats:
    def test_instantiation(self):
        from orcapod.hashing.cache_population import CachePopulationStats

        stats = CachePopulationStats(
            hashed=1,
            already_cached=2,
            skipped_small=3,
            errors=0,
            total_bytes_hashed=100,
            total_duration=1.0,
            avg_hashing_speed=100.0,
        )
        assert stats.hashed == 1
        assert stats.already_cached == 2
        assert stats.skipped_small == 3
        assert stats.errors == 0
        assert stats.total_bytes_hashed == 100
        assert stats.total_duration == 1.0
        assert stats.avg_hashing_speed == 100.0

    def test_is_frozen(self):
        from orcapod.hashing.cache_population import CachePopulationStats

        stats = CachePopulationStats(
            hashed=0,
            already_cached=0,
            skipped_small=0,
            errors=0,
            total_bytes_hashed=0,
            total_duration=0.0,
            avg_hashing_speed=0.0,
        )
        with pytest.raises(Exception):
            stats.hashed = 1  # type: ignore[misc]
```

- [ ] **Step 1.2: Run tests — expect import failure**

```bash
uv run pytest tests/test_hashing/test_cache_population.py -v
```

Expected: FAIL — `ModuleNotFoundError: No module named 'orcapod.hashing.cache_population'`

- [ ] **Step 1.3: Create `cache_population.py` with just the dataclass**

Create `src/orcapod/hashing/cache_population.py`:

```python
"""File hash cache population utility.

Provides ``CachePopulationStats`` and ``populate_hash_cache()`` for pre-populating
the SQLite file-hash cache with hashes of large files before a pipeline run.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from pathlib import Path

from upath import UPath

from orcapod.hashing.file_hashers import FileHashKey, FileHasher
from orcapod.hashing.hash_cachers import SqliteHashCacher
from orcapod.types import PathLike

logger = logging.getLogger(__name__)

_DEFAULT_MIN_SIZE_BYTES: int = 500 * 1024 * 1024  # 500 MB


@dataclass(frozen=True)
class CachePopulationStats:
    """Statistics returned by ``populate_hash_cache()``.

    Attributes:
        hashed: Files newly hashed and written to the cache.
        already_cached: Files whose hash was already in the cache (rehash skipped).
        skipped_small: Files skipped because their size was below ``min_size_bytes``.
        errors: Files or directories that raised an exception during traversal or hashing
            (logged as warnings).
        total_bytes_hashed: Sum of bytes across all newly hashed files.
        total_duration: Wall-clock duration of the full run in seconds.
        avg_hashing_speed: Hashing throughput in bytes per second
            (``total_bytes_hashed / total_duration``; ``0.0`` if ``total_duration`` is zero).
    """

    hashed: int
    already_cached: int
    skipped_small: int
    errors: int
    total_bytes_hashed: int
    total_duration: float
    avg_hashing_speed: float
```

- [ ] **Step 1.4: Run tests — expect pass**

```bash
uv run pytest tests/test_hashing/test_cache_population.py::TestCachePopulationStats -v
```

Expected: 2 passed.

- [ ] **Step 1.5: Commit**

```bash
git add src/orcapod/hashing/cache_population.py tests/test_hashing/test_cache_population.py
git commit -m "feat(hashing): add CachePopulationStats dataclass (ITL-489)"
```

---

## Task 2: `populate_hash_cache()` — full implementation

**Files:**
- Modify: `src/orcapod/hashing/cache_population.py`
- Modify: `tests/test_hashing/test_cache_population.py`

All tests use `min_size_bytes=10` so small test files (≥ 10 bytes) qualify without creating real 500 MB files.

- [ ] **Step 2.1: Append all unit tests to the test file**

Append to `tests/test_hashing/test_cache_population.py`:

```python
# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_MIN = 10  # small threshold so test files qualify without being 500 MB


def _write(path, name: str, size: int):
    """Write a file of exactly ``size`` bytes under ``path``."""
    f = path / name
    f.write_bytes(b"x" * size)
    return f


# ---------------------------------------------------------------------------
# Traversal & filtering
# ---------------------------------------------------------------------------


class TestTraversal:
    def test_skips_files_below_threshold(self, tmp_path):
        db = tmp_path / "cache.db"
        _write(tmp_path, "small.bin", 5)  # < 10

        from orcapod.hashing.cache_population import populate_hash_cache

        stats = populate_hash_cache(tmp_path, min_size_bytes=_MIN, db_path=db)

        assert stats.skipped_small == 1
        assert stats.hashed == 0

    def test_hashes_file_at_exact_threshold(self, tmp_path):
        db = tmp_path / "cache.db"
        _write(tmp_path, "exact.bin", 10)  # == 10

        from orcapod.hashing.cache_population import populate_hash_cache

        stats = populate_hash_cache(tmp_path, min_size_bytes=_MIN, db_path=db)

        assert stats.hashed == 1
        assert stats.skipped_small == 0

    def test_recursive_traversal(self, tmp_path):
        db = tmp_path / "cache.db"
        sub = tmp_path / "a" / "b"
        sub.mkdir(parents=True)
        _write(sub, "deep.bin", 20)

        from orcapod.hashing.cache_population import populate_hash_cache

        stats = populate_hash_cache(tmp_path, min_size_bytes=_MIN, db_path=db)

        assert stats.hashed == 1

    def test_includes_hidden_files(self, tmp_path):
        db = tmp_path / "cache.db"
        _write(tmp_path, ".hidden.bin", 20)

        from orcapod.hashing.cache_population import populate_hash_cache

        stats = populate_hash_cache(tmp_path, min_size_bytes=_MIN, db_path=db)

        assert stats.hashed == 1

    def test_skips_symlinks(self, tmp_path):
        db = tmp_path / "cache.db"
        real = tmp_path / "real.bin"
        real.write_bytes(b"x" * 20)
        link = tmp_path / "link.bin"
        link.symlink_to(real)

        from orcapod.hashing.cache_population import populate_hash_cache

        stats = populate_hash_cache(tmp_path, min_size_bytes=_MIN, db_path=db)

        # Only the real file, not the symlink
        assert stats.hashed == 1


# ---------------------------------------------------------------------------
# Cache hit / miss
# ---------------------------------------------------------------------------


class TestCacheHitMiss:
    def test_cache_hit_skips_rehash(self, tmp_path):
        db = tmp_path / "cache.db"
        _write(tmp_path, "f.bin", 20)

        from orcapod.hashing.cache_population import populate_hash_cache

        first = populate_hash_cache(tmp_path, min_size_bytes=_MIN, db_path=db)
        second = populate_hash_cache(tmp_path, min_size_bytes=_MIN, db_path=db)

        assert first.hashed == 1
        assert second.already_cached == 1
        assert second.hashed == 0

    def test_total_bytes_hashed(self, tmp_path):
        db = tmp_path / "cache.db"
        _write(tmp_path, "a.bin", 20)
        _write(tmp_path, "b.bin", 30)

        from orcapod.hashing.cache_population import populate_hash_cache

        stats = populate_hash_cache(tmp_path, min_size_bytes=_MIN, db_path=db)

        assert stats.hashed == 2
        assert stats.total_bytes_hashed == 50


# ---------------------------------------------------------------------------
# Stats: timing & speed
# ---------------------------------------------------------------------------


class TestStats:
    def test_duration_positive(self, tmp_path):
        db = tmp_path / "cache.db"
        _write(tmp_path, "f.bin", 100)

        from orcapod.hashing.cache_population import populate_hash_cache

        stats = populate_hash_cache(tmp_path, min_size_bytes=_MIN, db_path=db)

        assert stats.total_duration > 0

    def test_speed_matches_bytes_over_duration(self, tmp_path):
        db = tmp_path / "cache.db"
        _write(tmp_path, "f.bin", 100)

        from orcapod.hashing.cache_population import populate_hash_cache

        stats = populate_hash_cache(tmp_path, min_size_bytes=_MIN, db_path=db)

        assert stats.avg_hashing_speed == pytest.approx(
            stats.total_bytes_hashed / stats.total_duration
        )

    def test_speed_zero_when_nothing_hashed(self, tmp_path):
        db = tmp_path / "cache.db"
        # Empty directory — nothing to hash

        from orcapod.hashing.cache_population import populate_hash_cache

        stats = populate_hash_cache(tmp_path, min_size_bytes=_MIN, db_path=db)

        assert stats.avg_hashing_speed == 0.0

    def test_db_path_none_uses_env_var(self, tmp_path, tmp_path_factory, monkeypatch):
        default_db = tmp_path_factory.mktemp("orcapod_db") / "file_hash_cache.db"
        monkeypatch.setenv("ORCAPOD_HASH_CACHE_DB", str(default_db))
        _write(tmp_path, "f.bin", 20)

        from orcapod.hashing.cache_population import populate_hash_cache

        stats = populate_hash_cache(tmp_path, min_size_bytes=_MIN, db_path=None)

        assert stats.hashed == 1
        assert default_db.exists()


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


class TestErrors:
    def test_file_error_increments_errors(self, tmp_path, monkeypatch):
        db = tmp_path / "cache.db"
        _write(tmp_path, "f.bin", 20)

        from orcapod.hashing import file_hashers

        def _raise(self, path):
            raise OSError("simulated hashing error")

        monkeypatch.setattr(file_hashers.FileHasher, "hash_file", _raise)

        from orcapod.hashing.cache_population import populate_hash_cache

        stats = populate_hash_cache(tmp_path, min_size_bytes=_MIN, db_path=db)

        assert stats.errors == 1
        assert stats.hashed == 0
```

- [ ] **Step 2.2: Run tests — expect failures**

```bash
uv run pytest tests/test_hashing/test_cache_population.py -v --tb=no -q
```

Expected: multiple FAILs with `ImportError` or `TypeError` (function not implemented yet).

- [ ] **Step 2.3: Implement `populate_hash_cache()` — append to `cache_population.py`**

Append the following to `src/orcapod/hashing/cache_population.py` (after the `CachePopulationStats` dataclass):

```python
def populate_hash_cache(
    path: PathLike,
    *,
    min_size_bytes: int = _DEFAULT_MIN_SIZE_BYTES,
    db_path: UPath | PathLike | None = None,
    algorithm: str = "sha256",
    buffer_size: int = 65536,
) -> CachePopulationStats:
    """Recursively hash and cache all files >= ``min_size_bytes`` under ``path``.

    Pre-populates the SQLite file-hash cache so that subsequent pipeline runs
    can skip expensive content hashing with a sub-millisecond DB lookup.

    All files under ``path`` are visited recursively, including hidden files.
    Symlinks are skipped. Per-file and per-directory errors are logged as warnings
    and counted in ``CachePopulationStats.errors``.

    Args:
        path: Root directory to scan recursively.
        min_size_bytes: Files strictly smaller than this threshold (in bytes) are
            skipped and not cached. Defaults to 500 MB (``500 * 1024 * 1024``).
        db_path: Path to the SQLite hash cache database. Only local paths are
            supported — SQLite cannot operate on remote filesystems. Defaults to
            the ``ORCAPOD_HASH_CACHE_DB`` environment variable, or
            ``~/.orcapod/file_hash_cache.db`` if unset.
        algorithm: Hash algorithm passed to ``FileHasher``. Defaults to ``"sha256"``.
        buffer_size: Read buffer size in bytes passed to ``FileHasher``.
            Defaults to 65536.

    Returns:
        ``CachePopulationStats`` with counts for hashed, cached, skipped, and
        errored files plus throughput metrics.
    """
    root = UPath(path)
    _db_path: Path | None = Path(db_path) if db_path is not None else None
    cacher = SqliteHashCacher(_db_path)
    hasher = FileHasher(algorithm=algorithm, buffer_size=buffer_size)

    hashed = 0
    already_cached = 0
    skipped_small = 0
    error_count = 0
    total_bytes_hashed = 0

    start = time.monotonic()

    # Explicit DFS stack — avoids recursion limits on deep trees.
    stack: list[UPath] = [root]
    while stack:
        current = stack.pop()
        try:
            entries = list(current.iterdir())
        except PermissionError:
            logger.warning(
                "Cannot access directory %s: permission denied", current
            )
            error_count += 1
            continue

        for entry in entries:
            # Never follow symlinks — they can cause cycles.
            if entry.is_symlink():
                continue
            if entry.is_dir():
                stack.append(entry)
                continue
            if not entry.is_file():
                continue

            try:
                resolved = entry.resolve()
                stat = resolved.stat()
                size = stat.st_size

                if size < min_size_bytes:
                    skipped_small += 1
                    continue

                key = FileHashKey(resolved, stat.st_mtime_ns, size)
                if cacher.get(key) is not None:
                    already_cached += 1
                    continue

                content_hash = hasher.hash_file(resolved)
                cacher.put(key, content_hash)
                hashed += 1
                total_bytes_hashed += size

            except Exception:
                logger.warning(
                    "Failed to process file %s", entry, exc_info=True
                )
                error_count += 1

    duration = time.monotonic() - start
    speed = total_bytes_hashed / duration if duration > 0 else 0.0

    return CachePopulationStats(
        hashed=hashed,
        already_cached=already_cached,
        skipped_small=skipped_small,
        errors=error_count,
        total_bytes_hashed=total_bytes_hashed,
        total_duration=duration,
        avg_hashing_speed=speed,
    )
```

- [ ] **Step 2.4: Run all tests — expect pass**

```bash
uv run pytest tests/test_hashing/test_cache_population.py -v
```

Expected: all tests pass.

- [ ] **Step 2.5: Commit**

```bash
git add src/orcapod/hashing/cache_population.py tests/test_hashing/test_cache_population.py
git commit -m "feat(hashing): implement populate_hash_cache() with size threshold and stats (ITL-489)"
```

---

## Task 3: Export from `orcapod.hashing`

**Files:**
- Modify: `src/orcapod/hashing/__init__.py`

- [ ] **Step 3.1: Write a failing import test**

Append to `tests/test_hashing/test_cache_population.py`:

```python
class TestPublicExports:
    def test_importable_from_orcapod_hashing(self):
        from orcapod.hashing import CachePopulationStats, populate_hash_cache

        assert callable(populate_hash_cache)
        assert CachePopulationStats.__dataclass_fields__  # is a dataclass
```

- [ ] **Step 3.2: Run test — expect fail**

```bash
uv run pytest tests/test_hashing/test_cache_population.py::TestPublicExports -v
```

Expected: FAIL — `ImportError: cannot import name 'CachePopulationStats' from 'orcapod.hashing'`

- [ ] **Step 3.3: Add exports to `src/orcapod/hashing/__init__.py`**

After the existing line:

```python
from orcapod.hashing.file_hashers import CachedFileHasher, FileHasher, FileHashKey
```

add:

```python
from orcapod.hashing.cache_population import CachePopulationStats, populate_hash_cache
```

In `__all__`, after the `"SqliteHashCacher"` entry, add:

```python
    "CachePopulationStats",
    "populate_hash_cache",
```

- [ ] **Step 3.4: Run test — expect pass**

```bash
uv run pytest tests/test_hashing/test_cache_population.py::TestPublicExports -v
```

Expected: PASS.

- [ ] **Step 3.5: Run full test suite for the module**

```bash
uv run pytest tests/test_hashing/ -v --tb=short -q
```

Expected: all existing + new tests pass.

- [ ] **Step 3.6: Commit**

```bash
git add src/orcapod/hashing/__init__.py tests/test_hashing/test_cache_population.py
git commit -m "feat(hashing): export CachePopulationStats and populate_hash_cache from orcapod.hashing (ITL-489)"
```

---

## Task 4: `typer` dependency + `[project.scripts]` entry point

**Files:**
- Modify: `pyproject.toml`

- [ ] **Step 4.1: Add `typer` to `[project.dependencies]` in `pyproject.toml`**

In `pyproject.toml`, find the `dependencies = [` list under `[project]` and append `"typer>=0.12"`:

```toml
dependencies = [
    ...existing entries...,
    "typer>=0.12",
]
```

- [ ] **Step 4.2: Add `[project.scripts]` section to `pyproject.toml`**

Add this new section immediately after the `[project.optional-dependencies]` block:

```toml
[project.scripts]
orcapod = "orcapod.cli:app"
```

- [ ] **Step 4.3: Sync dependencies**

```bash
uv sync
```

Expected: typer and its dependencies (click, etc.) installed with no errors.

- [ ] **Step 4.4: Verify typer is importable**

```bash
uv run python -c "import typer; print(typer.__version__)"
```

Expected: prints typer version (≥ 0.12).

- [ ] **Step 4.5: Commit**

```bash
git add pyproject.toml
git commit -m "chore(deps): add typer>=0.12 and register orcapod CLI entry point (ITL-489)"
```

---

## Task 5: `orcapod.cli` package and `warm-cache` subcommand

**Files:**
- Create: `src/orcapod/cli/__init__.py`
- Create: `src/orcapod/cli/warm_cache.py`
- Create: `tests/test_cli/__init__.py`
- Create: `tests/test_cli/test_warm_cache.py`

- [ ] **Step 5.1: Create the CLI test file with failing tests**

Create `tests/test_cli/__init__.py` (empty):

```python
```

Create `tests/test_cli/test_warm_cache.py`:

```python
"""CLI tests for ``orcapod warm-cache``."""

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
                "--min-size", "0.00002",   # ~20 bytes in MB, so 20-byte file qualifies
                "--db-path", str(db),
            ],
        )
        assert result.exit_code == 0, result.output
        assert "hashed" in result.output

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
        assert "already cached" in result.output

    def test_min_size_default_shown_in_help(self, runner):
        from orcapod.cli import app

        result = runner.invoke(app, ["warm-cache", "--help"])
        assert "500" in result.output  # default 500 MB should appear
```

- [ ] **Step 5.2: Run CLI tests — expect import failures**

```bash
uv run pytest tests/test_cli/ -v --tb=short -q
```

Expected: FAIL — `ModuleNotFoundError: No module named 'orcapod.cli'`

- [ ] **Step 5.3: Create `src/orcapod/cli/__init__.py`**

```python
"""Orcapod command-line interface.

Provides the ``orcapod`` entry point. Sub-commands are registered below.

Usage::

    orcapod warm-cache /data/recordings --min-size 500
"""

import typer

from orcapod.cli.warm_cache import warm_cache

app = typer.Typer(
    name="orcapod",
    help="Orcapod pipeline utilities.",
    no_args_is_help=True,
)

app.command("warm-cache")(warm_cache)
```

- [ ] **Step 5.4: Create `src/orcapod/cli/warm_cache.py`**

```python
"""``orcapod warm-cache`` subcommand.

Pre-populates the SQLite file-hash cache for large files under a target
directory so that subsequent pipeline runs skip expensive content hashing.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer

_DEFAULT_MIN_SIZE_MB: float = 500.0


def warm_cache(
    path: str = typer.Argument(..., help="Root directory to scan recursively."),
    min_size: float = typer.Option(
        _DEFAULT_MIN_SIZE_MB,
        "--min-size",
        help="Minimum file size in MB. Files smaller than this are skipped. Default: 500 MB.",
        show_default=True,
    ),
    db_path: Optional[str] = typer.Option(
        None,
        "--db-path",
        help=(
            "Path to the SQLite hash-cache database. "
            "Defaults to $ORCAPOD_HASH_CACHE_DB or ~/.orcapod/file_hash_cache.db."
        ),
    ),
    algorithm: str = typer.Option(
        "sha256",
        "--algorithm",
        help="Hash algorithm (sha256, xxh64, md5, …). Default: sha256.",
        show_default=True,
    ),
    buffer_size: int = typer.Option(
        65536,
        "--buffer-size",
        help="Read buffer size in bytes. Default: 65536.",
        show_default=True,
    ),
) -> None:
    """Pre-populate the file-hash cache for large files under PATH.

    Recursively scans PATH and hashes every file that is at least MIN_SIZE MB.
    Files already present in the cache are skipped. On completion, prints a
    summary with counts and throughput.
    """
    from orcapod.hashing.cache_population import populate_hash_cache

    min_size_bytes = int(min_size * 1024 * 1024)
    _db_path: Path | None = Path(db_path) if db_path is not None else None

    typer.echo(f"Scanning {path} ...")

    stats = populate_hash_cache(
        path,
        min_size_bytes=min_size_bytes,
        db_path=_db_path,
        algorithm=algorithm,
        buffer_size=buffer_size,
    )

    gb = stats.total_bytes_hashed / (1024**3)
    speed_gb = stats.avg_hashing_speed / (1024**3)
    min_size_display = f"{min_size:.4g} MB"

    typer.echo(
        f"Done in {stats.total_duration:.1f}s — "
        f"{stats.hashed} hashed ({gb:.2f} GB), "
        f"{stats.already_cached} already cached, "
        f"{stats.skipped_small} skipped (< {min_size_display}), "
        f"{stats.errors} errors."
    )
    if stats.hashed > 0:
        typer.echo(f"Average hashing speed: {speed_gb:.2f} GB/s")
```

- [ ] **Step 5.5: Run CLI tests — expect pass**

```bash
uv run pytest tests/test_cli/ -v
```

Expected: all 4 tests pass.

- [ ] **Step 5.6: Run full test suite**

```bash
uv run pytest tests/test_hashing/ tests/test_cli/ -v --tb=short -q
```

Expected: all tests pass.

- [ ] **Step 5.7: Commit**

```bash
git add src/orcapod/cli/__init__.py src/orcapod/cli/warm_cache.py \
        tests/test_cli/__init__.py tests/test_cli/test_warm_cache.py
git commit -m "feat(cli): add orcapod warm-cache subcommand via typer (ITL-489)"
```

---

## Task 6: End-to-end smoke test

Verify the installed script is reachable as `orcapod warm-cache` via `uv run`.

- [ ] **Step 6.1: Confirm entry point is registered**

```bash
uv run orcapod --help
```

Expected output contains:
```
Usage: orcapod [OPTIONS] COMMAND [ARGS]...
  Orcapod pipeline utilities.
Commands:
  warm-cache  Pre-populate the file-hash cache for large files under PATH.
```

- [ ] **Step 6.2: Run `warm-cache` on a temp directory**

```bash
TMPDIR=$(mktemp -d)
dd if=/dev/urandom of="$TMPDIR/test.bin" bs=1M count=1 2>/dev/null
uv run orcapod warm-cache "$TMPDIR" --min-size 0.5 --db-path "$TMPDIR/cache.db"
```

Expected: output like:
```
Scanning /tmp/tmpXXXXXX ...
Done in 0.0s — 1 hashed (0.00 GB), 0 already cached, 0 skipped (< 0.5 MB), 0 errors.
Average hashing speed: ...
```

- [ ] **Step 6.3: Run again — confirm cache hit**

```bash
uv run orcapod warm-cache "$TMPDIR" --min-size 0.5 --db-path "$TMPDIR/cache.db"
```

Expected: `1 already cached, 0 hashed`.

- [ ] **Step 6.4: Commit verification note**

No code changes needed. If Step 6.1–6.3 all pass, the feature is complete.

```bash
git log --oneline -5
```

Confirm the four feature commits are present:
1. `feat(hashing): add CachePopulationStats dataclass (ITL-489)`
2. `feat(hashing): implement populate_hash_cache() with size threshold and stats (ITL-489)`
3. `feat(hashing): export CachePopulationStats and populate_hash_cache from orcapod.hashing (ITL-489)`
4. `chore(deps): add typer>=0.12 and register orcapod CLI entry point (ITL-489)`
5. `feat(cli): add orcapod warm-cache subcommand via typer (ITL-489)`
