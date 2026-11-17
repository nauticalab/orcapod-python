# Design: Improve `populate_hash_cache` Ergonomics (ITL-500)

**Date:** 2026-07-05
**Issue:** [ITL-500](https://linear.app/enigma-metamorphic/issue/ITL-500/improve-populate-hash-cache-ergonomics-progress-callback-dry-run-force)
**Status:** Draft

---

## Overview

`populate_hash_cache()` is a blocking function that walks a directory tree and
pre-populates the SQLite file-hash cache. As surfaced by the `hash-archive`
backfill tool (NPIPE-108), consumers currently cannot:

- observe progress during a long run (no callback, no streaming)
- preview what the function would do without actually hashing (no dry-run)
- force a re-hash of already-cached files without nuking the whole cache (no force flag)

This design adds all three capabilities while keeping the public API additive and
the internal structure clean via a visitor / accumulator split.

---

## Goals & Success Criteria

1. **Progress callback** — optional `progress_callback` parameter called once per
   qualifying file with its path, outcome, and a frozen running-stats snapshot.
2. **Dry-run mode** — `dry_run=True` performs the full DFS walk + stat + size filter
   + cache check but skips hashing and cache writes.
3. **Force re-hash** — `force=True` re-hashes files even if they already have a
   cache entry.
4. **Mutable internal stats** — replace the five loose counter variables with a
   private `_Stats` accumulator; `CachePopulationStats` remains frozen.
5. **CLI exposure** — `orcapod warm-cache` gains `--dry-run` and `--force` flags
   with appropriate output formatting.

---

## Scope & Boundaries

In scope:
- `src/orcapod/hashing/cache_population.py` — new types + revised implementation
- `src/orcapod/cli/warm_cache.py` — two new CLI flags
- `tests/test_hashing/test_cache_population.py` — new test classes

Out of scope:
- `CachePopulationStats` public fields — no renames; one new field added (`total_bytes_cached`)
- Async / `asyncio` execution model — remains `concurrent.futures` throughout
- Progress bar built into the CLI — callback API serves library consumers;
  CLI stays simple text output
- `engm://` multi-root iteration correctness — tracked separately as ITL-501

---

## Design

### New Types (all in `cache_population.py`)

```python
FileOutcome = Literal["hashed", "cached", "would_hash", "error"]
ProgressCallback = Callable[[Path, FileOutcome, CachePopulationStats], None]
```

`FileOutcome` labels what happened to each qualifying file:

| Value | When used |
|---|---|
| `"hashed"` | File was newly hashed and written to the cache (normal run) |
| `"cached"` | File was already in the cache; rehash skipped |
| `"would_hash"` | Dry-run: file is not in the cache and would be hashed |
| `"error"` | An exception was raised during stat, cache-check, or hashing |

`ProgressCallback` receives:
- `path: Path` — resolved file path just processed
- `outcome: FileOutcome` — what happened
- `stats: CachePopulationStats` — frozen snapshot of running totals at this moment

The callback is **not** called for files below `min_size_bytes` — those are
filtered cheaply on the main thread, and firing a callback for every tiny file
in large trees would swamp progress bars. The final `CachePopulationStats` still
carries an accurate `skipped_small` count.

---

### Internal Accumulator (`_Stats`, `_Accumulator`)

`_Stats` replaces the five loose local counter variables:

```python
@dataclass
class _Stats:
    hashed: int = 0
    already_cached: int = 0
    skipped_small: int = 0
    errors: int = 0
    total_bytes_hashed: int = 0
    total_bytes_cached: int = 0   # mirrors new CachePopulationStats field
    start: float = field(default_factory=time.monotonic)

    def snapshot(self) -> CachePopulationStats:
        duration = time.monotonic() - self.start
        return CachePopulationStats(
            hashed=self.hashed,
            already_cached=self.already_cached,
            skipped_small=self.skipped_small,
            errors=self.errors,
            total_bytes_hashed=self.total_bytes_hashed,
            total_bytes_cached=self.total_bytes_cached,
            total_duration=duration,
            avg_hashing_speed=(
                self.total_bytes_hashed / duration if duration > 0 else 0.0
            ),
        )
```

`_Accumulator` owns `_Stats` and the optional callback. It is the single place
where counters are updated and snapshots are created:

```python
class _Accumulator:
    def __init__(self, callback: ProgressCallback | None = None) -> None: ...

    def record(self, path: Path, outcome: FileOutcome, nbytes: int) -> None:
        # Updates counters; fires callback with a snapshot if one is set.
        # Called for every qualifying file (hashed, cached, would_hash, or file-level error).

    def record_skipped_small(self) -> None:
        # Increments skipped_small only — no callback fired.

    def record_directory_error(self) -> None:
        # Increments errors for a directory access failure — no callback fired.
        # Directory errors are not qualifying-file events; the callback is per-file only.

    def finalize(self) -> CachePopulationStats:
        # Returns the final frozen stats snapshot.
```

`"hashed"` and `"would_hash"` both increment `_stats.hashed` and `_stats.total_bytes_hashed`.
`"cached"` increments `_stats.already_cached` and `_stats.total_bytes_cached` — the `nbytes`
argument for cached outcomes is `file_stat.st_size` (visitors always return the file size,
not `0`, so the accumulator can track cached bytes).
Dry-run `hashed` means "would-hash count"; callers that passed `dry_run=True` interpret it
accordingly (documented on the parameter).

---

### Visitors (`_HashVisitor`, `_DryRunVisitor`)

Visitors encapsulate *what to do per qualifying file*. Both are callables with
the signature `(resolved: Path, file_stat: os.stat_result) -> tuple[FileOutcome, int]`
so the thread pool can invoke either without special casing.

**`_HashVisitor`** — normal hashing path; runs on worker threads:

```python
class _HashVisitor:
    def __init__(self, cacher, hasher, *, force: bool = False): ...
    def __call__(self, resolved, file_stat) -> tuple[FileOutcome, int]:
        key = FileHashKey(resolved, file_stat.st_mtime_ns, file_stat.st_size)
        if not self._force and self._cacher.get(key) is not None:
            return ("cached", file_stat.st_size)   # size returned for total_bytes_cached
        content_hash = self._hasher.hash_file(resolved)
        self._cacher.put(key, content_hash)
        return ("hashed", file_stat.st_size)
        # exceptions → ("error", 0) with logger.warning
```

**`_DryRunVisitor`** — cache-check only; runs on the traversal thread (no executor):

```python
class _DryRunVisitor:
    def __init__(self, cacher, *, force: bool = False): ...
    def __call__(self, resolved, file_stat) -> tuple[FileOutcome, int]:
        if self._force:
            return ("would_hash", file_stat.st_size)
        key = FileHashKey(resolved, file_stat.st_mtime_ns, file_stat.st_size)
        if self._cacher.get(key) is not None:
            return ("cached", file_stat.st_size)    # size returned for total_bytes_cached
        return ("would_hash", file_stat.st_size)
        # exceptions → ("error", 0) with logger.warning
```

`force=True, dry_run=True`: all qualifying files are `"would_hash"` — represents
"what would a force re-hash do?" without touching the cache.

Dry-run does not use the thread pool; cache lookups are fast SQLite reads and
parallelising them adds unnecessary complexity.

---

### Updated `populate_hash_cache` Signature

Three new keyword-only parameters appended (all additive, all with safe defaults):

```python
def populate_hash_cache(
    path: PathLike | UPath,
    *,
    min_size_bytes: int = _DEFAULT_MIN_SIZE_BYTES,
    db_path: UPath | PathLike | None = None,
    algorithm: str = "sha256",
    buffer_size: int = 65536,
    max_workers: int = _DEFAULT_MAX_WORKERS,
    progress_callback: ProgressCallback | None = None,   # new
    dry_run: bool = False,                               # new
    force: bool = False,                                 # new
) -> CachePopulationStats:
```

The `if dry_run` branch appears exactly once — at visitor construction time.
The traversal loop body is uniform for both paths.

---

### Traversal Loop Changes

**Pending collection** changes from `set[Future]` to `dict[Future, Path]`.
This allows the drain step to retrieve the file path from a completed future
in O(1) for the progress callback:

```python
pending: dict[Future[tuple[FileOutcome, int]], UPath] = {}

# Submit:
future = executor.submit(visitor, resolved, file_stat)
pending[future] = resolved

# Drain (backpressure):
done, _ = wait(pending.keys(), return_when=FIRST_COMPLETED)
for fut in done:
    path = pending.pop(fut)
    outcome, nbytes = fut.result()
    accumulator.record(path, outcome, nbytes)
```

All raw counter increments (`hashed += 1`, etc.) are replaced by
`accumulator.record(...)` and `accumulator.record_skipped_small()`.

The final return value changes from a manual `CachePopulationStats(...)` constructor
call to `return accumulator.finalize()`.

---

### CLI Changes (`warm_cache.py`)

Two new Typer options:

```
--dry-run    Scan and check cache without hashing. Prints what would be done.
--force      Re-hash files even if already cached.
```

Output format differs for dry-run:

- Normal: `"Done in Xs — N hashed (X.XX GB), Y already cached (X.XX GB), Z skipped, E errors."`
- Dry-run: `"Dry run complete in Xs — N would be hashed (X.XX GB), Y already cached (X.XX GB), Z skipped, E errors."`

No built-in progress bar is added to the CLI. The `progress_callback` API
is the extension point for library consumers.

---

## Testing Plan

New test classes in `tests/test_hashing/test_cache_population.py`:

| Class | Coverage |
|---|---|
| `TestForce` | `force=True` re-hashes already-cached files; `force=False` skips them |
| `TestDryRun` | No cache writes occur; `hashed` equals would-hash count; `dry_run+force` counts all qualifying files as `would_hash` |
| `TestProgressCallback` | Callback fires once per qualifying file; receives correct path, outcome, and snapshot; `skipped_small` files do not trigger callback; running totals are accurate at each call |
| `TestVisitors` | `_HashVisitor` and `_DryRunVisitor` unit-tested directly with a real `SqliteHashCacher`, independent of traversal |
| `TestCLI` | `--dry-run` and `--force` wire through correctly; dry-run output text differs from normal output |

All existing test classes (`TestTraversal`, `TestCacheHitMiss`, `TestStats`,
`TestErrors`, `TestConcurrency`, `TestPublicExports`) pass unchanged — the new
parameters are additive with safe defaults.

---

## Non-Goals / Deferred

- **Built-in tqdm progress bar in CLI** — the `progress_callback` API is the
  right extension point; adding a direct tqdm dependency to the CLI is YAGNI.
- **`scan_for_population()` sibling function** — `dry_run=True` is simpler and
  avoids duplicating traversal logic.
- **Async execution model** — out of scope; `concurrent.futures` is sufficient
  for IO-bound hashing.
