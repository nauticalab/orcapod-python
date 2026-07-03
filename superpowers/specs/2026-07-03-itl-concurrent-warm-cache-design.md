# Concurrent Warm Cache Design

**Date:** 2026-07-03
**Status:** Approved

---

## Overview

Add a `max_workers: int = 4` parameter to `populate_hash_cache()` and a matching
`--workers INT` flag to `orcapod warm-cache`. The function currently hashes files
serially; on NAS and HDD storage, parallelising the I/O-bound reads and hashes with
a `ThreadPoolExecutor` saturates available disk bandwidth and cuts wall-clock time
proportionally to the number of workers.

No new dependencies are introduced — `concurrent.futures` is stdlib.

---

## Design

### API change: `populate_hash_cache()`

Add one keyword-only parameter:

```python
def populate_hash_cache(
    path: PathLike,
    *,
    min_size_bytes: int = _DEFAULT_MIN_SIZE_BYTES,
    db_path: UPath | PathLike | None = None,
    algorithm: str = "sha256",
    buffer_size: int = 65536,
    max_workers: int = 4,              # NEW — default 4, pass 1 for serial behaviour
) -> CachePopulationStats:
```

`max_workers=1` reproduces the previous single-threaded behaviour exactly.
`max_workers=4` is the default, tuned for NAS/HDD where more than 4 parallel
reads tend to cause seek contention rather than additional throughput.

---

### Internal architecture

**Traversal (main thread):** The existing DFS stack loop is retained unchanged for
directory listing, symlink skipping, and `OSError` handling. The only change is that
instead of processing each file inline, the loop submits each qualifying file to the
executor. DB-file exclusion (`.db`, `-wal`, `-shm` sidecars) continues to be checked
in the main thread before dispatch, so workers never receive those paths.

**Per-file worker — `_hash_one()` (module-level private function):**

```python
def _hash_one(
    entry: UPath,
    cacher: SqliteHashCacher,
    hasher: FileHasher,
    min_size_bytes: int,
) -> tuple[str, int]:
    """Hash and cache a single file, returning a (category, bytes) result pair.

    Args:
        entry: File path to process.
        cacher: Shared SqliteHashCacher instance (thread-safe via threading.local).
        hasher: FileHasher instance (stateless, safe to share across threads).
        min_size_bytes: Files below this size are skipped.

    Returns:
        A tuple of (category, bytes_hashed) where category is one of
        "hashed", "cached", "skipped_small", or "error", and bytes_hashed
        is the file size for newly hashed files, 0 otherwise.
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
```

`FileHasher` is stateless (no mutable fields); sharing one instance across threads
is safe. `SqliteHashCacher` uses `threading.local()` internally — each worker thread
opens its own SQLite connection on first use, so the single shared instance requires
no additional locking.

**Execution structure:**

```python
from concurrent.futures import ThreadPoolExecutor, as_completed

futures = []
with ThreadPoolExecutor(max_workers=max_workers) as executor:
    with SqliteHashCacher(_db_path) as cacher:
        stack: list[UPath] = [root]
        while stack:
            current = stack.pop()
            try:
                entries = list(current.iterdir())
            except OSError:
                logger.warning("Cannot access directory %s", current, exc_info=True)
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
                if entry.resolve() in excluded:   # DB file exclusion — main thread
                    continue
                futures.append(
                    executor.submit(_hash_one, entry, cacher, hasher, min_size_bytes)
                )

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
```

**TOCTOU note:** Two workers could both see a cache miss for the same file and both
hash it. The write is idempotent (identical key → identical hash), and this race is
structurally impossible since the main thread traversal visits each file exactly once
before dispatching it — the same path is never submitted twice.

**WAL and concurrent writes:** SQLite WAL mode allows one writer at a time; with 4
workers the average write rate is low (one write per large file, which takes seconds
to hash), so contention is negligible in practice.

---

### CLI change: `orcapod warm-cache`

Add one option to `warm_cache()`:

```python
max_workers: int = typer.Option(
    4,
    "--workers",
    help="Number of threads for concurrent hashing. Default: 4.",
    show_default=True,
)
```

Pass directly to `populate_hash_cache(max_workers=max_workers)`. No output format
changes — `CachePopulationStats` is unchanged.

**Updated usage:**

```
orcapod warm-cache PATH
    [--min-size FLOAT]
    [--db-path PATH]
    [--algorithm TEXT]
    [--buffer-size INT]
    [--workers INT]        # default 4
```

---

## File Layout

```
src/orcapod/hashing/cache_population.py   MODIFY: add _hash_one(), max_workers param,
                                                   ThreadPoolExecutor dispatch
src/orcapod/cli/warm_cache.py             MODIFY: add --workers option
tests/test_hashing/test_cache_population.py  MODIFY: 3 new tests
tests/test_cli/test_warm_cache.py            MODIFY: 1 new test
```

No new files. No new dependencies.

---

## Tests

### `tests/test_hashing/test_cache_population.py`

| Test | What it checks |
|---|---|
| `test_concurrent_hashes_multiple_files` | 3 qualifying files + `max_workers=4` → `stats.hashed == 3`, no files lost |
| `test_max_workers_1_matches_serial` | Same directory, `max_workers=1` vs `max_workers=4` → identical `CachePopulationStats` |
| `test_workers_default_is_4` | `inspect.signature` confirms default is `4` |

### `tests/test_cli/test_warm_cache.py`

| Test | What it checks |
|---|---|
| `test_workers_option_accepted` | `--workers 2` flag accepted, exit code 0 |

No mocking needed — test files are a few bytes; real hashing is fast.

---

## Out of Scope

- Parallelising directory traversal (`iterdir()` on NAS is not the bottleneck)
- Async I/O (`asyncio` would require rewriting `SqliteHashCacher` and adds no benefit
  over threads for large sequential file reads)
- Dynamic worker scaling based on disk type detection
- Progress bars / per-file logging during concurrent runs
