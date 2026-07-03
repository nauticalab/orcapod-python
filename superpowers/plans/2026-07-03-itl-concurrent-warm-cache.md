# Concurrent Warm Cache Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `max_workers: int = 4` to `populate_hash_cache()` and `--workers INT` to `orcapod warm-cache` so file hashing runs concurrently across a `ThreadPoolExecutor`, saturating NAS/HDD bandwidth.

**Architecture:** The DFS directory traversal stays single-threaded on the main thread; as each qualifying file is found it is submitted to a `ThreadPoolExecutor`. A new private `_hash_one()` function handles all per-file work (stat, size check, cache get, hash, cache put) and returns a `(category, bytes)` result tuple. The main thread collects results via `as_completed()` and accumulates stats counters. `SqliteHashCacher` is already thread-safe via `threading.local()` connections — no locking changes needed.

**Tech Stack:** Python stdlib `concurrent.futures` (no new dependencies), `upath.UPath`, existing `FileHasher` and `SqliteHashCacher`.

---

## Scene-setting

You are working in the `orcapod-python` repository. The relevant files are:

- `src/orcapod/hashing/cache_population.py` — contains `CachePopulationStats` and `populate_hash_cache()`
- `src/orcapod/cli/warm_cache.py` — contains the `warm_cache()` typer command
- `tests/test_hashing/test_cache_population.py` — existing tests for `populate_hash_cache()`
- `tests/test_cli/test_warm_cache.py` — existing CLI tests

Always run tests with `uv run pytest <path> -v`, never plain `pytest`.

The existing `populate_hash_cache()` processes each file inline inside a single-threaded DFS loop. The goal is to extract that per-file logic into `_hash_one()` and dispatch it to a thread pool.

---

## Task 1: Add concurrent hashing to `populate_hash_cache()`

**Files:**
- Modify: `src/orcapod/hashing/cache_population.py`
- Test: `tests/test_hashing/test_cache_population.py`

- [ ] **Step 1: Write the three failing tests**

Add a new `TestConcurrency` class at the bottom of `tests/test_hashing/test_cache_population.py` (before `TestPublicExports`):

```python
# ---------------------------------------------------------------------------
# Concurrency
# ---------------------------------------------------------------------------


class TestConcurrency:
    def test_concurrent_hashes_multiple_files(self, tmp_path):
        """ThreadPoolExecutor path hashes all qualifying files without losing any."""
        db = tmp_path / "cache.db"
        _write(tmp_path, "a.bin", 20)
        _write(tmp_path, "b.bin", 20)
        _write(tmp_path, "c.bin", 20)

        from orcapod.hashing.cache_population import populate_hash_cache

        stats = populate_hash_cache(
            tmp_path, min_size_bytes=_MIN, db_path=db, max_workers=4
        )

        assert stats.hashed == 3
        assert stats.errors == 0

    def test_max_workers_1_matches_serial(self, tmp_path):
        """max_workers=1 and max_workers=4 produce identical CachePopulationStats."""
        db1 = tmp_path / "cache1.db"
        db2 = tmp_path / "cache2.db"
        _write(tmp_path, "a.bin", 20)
        _write(tmp_path, "b.bin", 30)

        from orcapod.hashing.cache_population import populate_hash_cache

        serial = populate_hash_cache(
            tmp_path, min_size_bytes=_MIN, db_path=db1, max_workers=1
        )
        concurrent = populate_hash_cache(
            tmp_path, min_size_bytes=_MIN, db_path=db2, max_workers=4
        )

        assert serial.hashed == concurrent.hashed
        assert serial.already_cached == concurrent.already_cached
        assert serial.skipped_small == concurrent.skipped_small
        assert serial.errors == concurrent.errors
        assert serial.total_bytes_hashed == concurrent.total_bytes_hashed

    def test_workers_default_is_4(self):
        """The default value of max_workers is 4."""
        import inspect

        from orcapod.hashing.cache_population import populate_hash_cache

        sig = inspect.signature(populate_hash_cache)
        assert sig.parameters["max_workers"].default == 4
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
uv run pytest tests/test_hashing/test_cache_population.py::TestConcurrency -v
```

Expected: 3 failures — `populate_hash_cache` has no `max_workers` parameter yet.

```
FAILED tests/test_hashing/test_cache_population.py::TestConcurrency::test_concurrent_hashes_multiple_files
FAILED tests/test_hashing/test_cache_population.py::TestConcurrency::test_max_workers_1_matches_serial
FAILED tests/test_hashing/test_cache_population.py::TestConcurrency::test_workers_default_is_4
```

- [ ] **Step 3: Implement the new `cache_population.py`**

Replace the entire content of `src/orcapod/hashing/cache_population.py` with:

```python
"""File hash cache population utility.

Provides ``CachePopulationStats`` and ``populate_hash_cache()`` for pre-populating
the SQLite file-hash cache with hashes of large files before a pipeline run.
"""

from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path

from upath import UPath

from orcapod.hashing.file_hashers import FileHashKey, FileHasher
from orcapod.hashing.hash_cachers import SqliteHashCacher
from orcapod.types import PathLike

logger = logging.getLogger(__name__)

_DEFAULT_MIN_SIZE_BYTES: int = 500 * 1024 * 1024  # 500 MB
_DEFAULT_MAX_WORKERS: int = 4


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


def _hash_one(
    entry: UPath,
    cacher: SqliteHashCacher,
    hasher: FileHasher,
    min_size_bytes: int,
) -> tuple[str, int]:
    """Hash and cache a single file, returning a (category, bytes) result pair.

    Called from worker threads inside ``populate_hash_cache()``. ``cacher`` and
    ``hasher`` are shared across threads; both are safe because ``SqliteHashCacher``
    uses ``threading.local()`` connections and ``FileHasher`` is stateless.

    Args:
        entry: File path to process.
        cacher: Shared ``SqliteHashCacher`` instance.
        hasher: Shared ``FileHasher`` instance.
        min_size_bytes: Files strictly smaller than this threshold are skipped.

    Returns:
        A tuple ``(category, bytes_hashed)`` where ``category`` is one of
        ``"hashed"``, ``"cached"``, ``"skipped_small"``, or ``"error"``, and
        ``bytes_hashed`` is the file size for newly hashed files, ``0`` otherwise.
    """
    try:
        resolved = entry.resolve()
        stat = resolved.stat()
        if stat.st_size < min_size_bytes:
            return ("skipped_small", 0)
        key = FileHashKey(resolved, stat.st_mtime_ns, stat.st_size)
        if cacher.get(key) is not None:
            return ("cached", 0)
        content_hash = hasher.hash_file(resolved)
        cacher.put(key, content_hash)
        return ("hashed", stat.st_size)
    except Exception:
        logger.warning("Failed to process file %s", entry, exc_info=True)
        return ("error", 0)


def populate_hash_cache(
    path: PathLike,
    *,
    min_size_bytes: int = _DEFAULT_MIN_SIZE_BYTES,
    db_path: UPath | PathLike | None = None,
    algorithm: str = "sha256",
    buffer_size: int = 65536,
    max_workers: int = _DEFAULT_MAX_WORKERS,
) -> CachePopulationStats:
    """Recursively hash and cache all files >= ``min_size_bytes`` under ``path``.

    Pre-populates the SQLite file-hash cache so that subsequent pipeline runs
    can skip expensive content hashing with a sub-millisecond DB lookup.

    All files under ``path`` are visited recursively, including hidden files.
    Symlinks are skipped. Per-file and per-directory errors are logged as warnings
    and counted in ``CachePopulationStats.errors``.

    Directory traversal runs on the calling thread. Per-file work (stat, cache
    lookup, hashing, cache write) is dispatched to a ``ThreadPoolExecutor`` with
    ``max_workers`` threads. Pass ``max_workers=1`` to restore single-threaded
    behaviour.

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
        max_workers: Number of threads for concurrent per-file hashing. Defaults
            to 4, which is well-suited for NAS and HDD storage where more than 4
            parallel reads cause seek contention. Pass ``1`` for serial behaviour.

    Returns:
        ``CachePopulationStats`` with counts for hashed, cached, skipped, and
        errored files plus throughput metrics.
    """
    root = UPath(path)
    _db_path: Path | None = Path(db_path) if db_path is not None else None
    hasher = FileHasher(algorithm=algorithm, buffer_size=buffer_size)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        with SqliteHashCacher(_db_path) as cacher:
            # Collect the DB file and its SQLite journal/WAL siblings so they are
            # never treated as data files even if the DB lives inside the scan root.
            _db_resolved = cacher.db_path.resolve()
            _excluded: frozenset[Path] = frozenset(
                [
                    _db_resolved,
                    _db_resolved.with_suffix(_db_resolved.suffix + "-wal"),
                    _db_resolved.with_suffix(_db_resolved.suffix + "-shm"),
                ]
            )

            error_count = 0
            start = time.monotonic()

            # Explicit DFS stack — avoids recursion limits on deep trees.
            # DB-file exclusion happens here (main thread) so workers never
            # receive those paths.
            futures = []
            stack: list[UPath] = [root]
            while stack:
                current = stack.pop()
                try:
                    entries = list(current.iterdir())
                except OSError:
                    logger.warning(
                        "Cannot access directory %s", current, exc_info=True
                    )
                    error_count += 1
                    continue

                for entry in entries:
                    if entry.is_symlink():
                        continue
                    if entry.is_dir():
                        stack.append(entry)
                        continue
                    if not entry.is_file():
                        continue
                    if entry.resolve() in _excluded:
                        continue
                    futures.append(
                        executor.submit(_hash_one, entry, cacher, hasher, min_size_bytes)
                    )

            # Collect results as workers complete.
            hashed = 0
            already_cached = 0
            skipped_small = 0
            total_bytes_hashed = 0

            for future in as_completed(futures):
                category, nbytes = future.result()
                if category == "hashed":
                    hashed += 1
                    total_bytes_hashed += nbytes
                elif category == "cached":
                    already_cached += 1
                elif category == "skipped_small":
                    skipped_small += 1
                else:  # "error"
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

- [ ] **Step 4: Run the new concurrency tests**

```bash
uv run pytest tests/test_hashing/test_cache_population.py::TestConcurrency -v
```

Expected: all 3 pass.

```
PASSED tests/test_hashing/test_cache_population.py::TestConcurrency::test_concurrent_hashes_multiple_files
PASSED tests/test_hashing/test_cache_population.py::TestConcurrency::test_max_workers_1_matches_serial
PASSED tests/test_hashing/test_cache_population.py::TestConcurrency::test_workers_default_is_4
```

- [ ] **Step 5: Run the full existing test suite to confirm no regressions**

```bash
uv run pytest tests/test_hashing/test_cache_population.py -v
```

Expected: all tests pass (16 pre-existing + 3 new = 19 total).

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/hashing/cache_population.py tests/test_hashing/test_cache_population.py
git commit -m "feat(hashing): add concurrent hashing via ThreadPoolExecutor to populate_hash_cache"
```

---

## Task 2: Add `--workers` option to `orcapod warm-cache`

**Files:**
- Modify: `src/orcapod/cli/warm_cache.py`
- Test: `tests/test_cli/test_warm_cache.py`

- [ ] **Step 1: Write the failing CLI test**

Add this test to the `TestWarmCacheCLI` class in `tests/test_cli/test_warm_cache.py`:

```python
    def test_workers_option_accepted(self, runner, tmp_path):
        db = tmp_path / "cache.db"

        from orcapod.cli import app

        result = runner.invoke(
            app,
            [
                "warm-cache",
                str(tmp_path),
                "--min-size", "0",
                "--workers", "2",
                "--db-path", str(db),
            ],
        )
        assert result.exit_code == 0, result.output
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
uv run pytest tests/test_cli/test_warm_cache.py::TestWarmCacheCLI::test_workers_option_accepted -v
```

Expected: FAIL — `--workers` is not a recognised option yet.

```
FAILED tests/test_cli/test_warm_cache.py::TestWarmCacheCLI::test_workers_option_accepted
```

- [ ] **Step 3: Add `--workers` to `warm_cache.py`**

The current `warm_cache()` function signature ends at `buffer_size`. Add `max_workers` and pass it through. The full updated function:

```python
def warm_cache(
    path: str = typer.Argument(..., help="Root directory to scan recursively."),
    min_size: float = typer.Option(
        _DEFAULT_MIN_SIZE_MB,
        "--min-size",
        help="Minimum file size in MB. Files smaller than this are skipped. Default: 500 MB.",
        show_default=True,
    ),
    db_path: str | None = typer.Option(
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
    max_workers: int = typer.Option(
        4,
        "--workers",
        help="Number of threads for concurrent hashing. Default: 4.",
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

    root = Path(path)
    if not root.exists():
        typer.echo(f"Error: path does not exist: {path}", err=True)
        raise typer.Exit(code=1)
    if not root.is_dir():
        typer.echo(f"Error: path is not a directory: {path}", err=True)
        raise typer.Exit(code=1)

    typer.echo(f"Scanning {path} ...")

    stats = populate_hash_cache(
        path,
        min_size_bytes=min_size_bytes,
        db_path=_db_path,
        algorithm=algorithm,
        buffer_size=buffer_size,
        max_workers=max_workers,
    )

    gb = stats.total_bytes_hashed / (1024**3)
    speed_gb = stats.avg_hashing_speed / (1024**3)
    min_size_display = f"{min_size:g} MB"

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

- [ ] **Step 4: Run the new CLI test**

```bash
uv run pytest tests/test_cli/test_warm_cache.py::TestWarmCacheCLI::test_workers_option_accepted -v
```

Expected: PASS.

- [ ] **Step 5: Run the full CLI test suite**

```bash
uv run pytest tests/test_cli/ -v
```

Expected: all 6 tests pass (5 pre-existing + 1 new).

- [ ] **Step 6: Run the complete test suite**

```bash
uv run pytest tests/ -x -q
```

Expected: all tests pass.

- [ ] **Step 7: Commit**

```bash
git add src/orcapod/cli/warm_cache.py tests/test_cli/test_warm_cache.py
git commit -m "feat(cli): add --workers option to orcapod warm-cache"
```
