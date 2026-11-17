# populate_hash_cache Ergonomics Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `progress_callback`, `dry_run`, `force`, and `total_bytes_cached` to `populate_hash_cache` using a clean visitor/accumulator split.

**Architecture:** A private `_Accumulator` owns a mutable `_Stats` object and the optional callback; two visitor classes (`_HashVisitor`, `_DryRunVisitor`) replace the old `_hash_one` function; `populate_hash_cache` constructs the right visitor and delegates all per-file logic to it. The `pending` set becomes a `dict[Future, Path]` so callbacks receive the file path.

**Tech Stack:** Python 3.12, `concurrent.futures`, `uv run pytest`

**Spec:** `superpowers/specs/2026-07-05-itl-500-populate-hash-cache-ergonomics-design.md`

**Branch:** `eywalker/itl-500-improve-populate_hash_cache-ergonomics-progress-callback-dry`

---

## Task 0: Create and check out the feature branch

**Files:** none

- [ ] **Step 0.1: Create the branch from `main`**

```bash
git checkout main
git checkout -b eywalker/itl-500-improve-populate_hash_cache-ergonomics-progress-callback-dry
git branch --show-current
```

Expected output: `eywalker/itl-500-improve-populate_hash_cache-ergonomics-progress-callback-dry`

---

## Task 1: Add `total_bytes_cached` to `CachePopulationStats`

**Files:**
- Modify: `src/orcapod/hashing/cache_population.py`
- Modify: `tests/test_hashing/test_cache_population.py`

- [ ] **Step 1.1: Write failing tests**

Add to `tests/test_hashing/test_cache_population.py`:

```python
# Update TestCachePopulationStats.test_instantiation — add total_bytes_cached=50:
class TestCachePopulationStats:
    def test_instantiation(self):
        from orcapod.hashing.cache_population import CachePopulationStats

        stats = CachePopulationStats(
            hashed=1,
            already_cached=2,
            skipped_small=3,
            errors=0,
            total_bytes_hashed=100,
            total_bytes_cached=50,
            total_duration=1.0,
            avg_hashing_speed=100.0,
        )
        assert stats.hashed == 1
        assert stats.already_cached == 2
        assert stats.skipped_small == 3
        assert stats.errors == 0
        assert stats.total_bytes_hashed == 100
        assert stats.total_bytes_cached == 50
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
            total_bytes_cached=0,
            total_duration=0.0,
            avg_hashing_speed=0.0,
        )
        with pytest.raises((dataclasses.FrozenInstanceError, AttributeError)):
            stats.hashed = 1  # type: ignore[misc]
```

Add new class at end of file:

```python
class TestCachedBytes:
    def test_total_bytes_cached_zero_on_first_run(self, tmp_path):
        """First run has nothing cached yet — total_bytes_cached must be zero."""
        db = tmp_path / "cache.db"
        _write(tmp_path, "f.bin", 20)

        from orcapod.hashing.cache_population import populate_hash_cache

        stats = populate_hash_cache(tmp_path, min_size_bytes=_MIN, db_path=db)
        assert stats.total_bytes_cached == 0

    def test_total_bytes_cached_on_second_run(self, tmp_path):
        """Second run finds the file cached — total_bytes_cached equals file size."""
        db = tmp_path / "cache.db"
        _write(tmp_path, "f.bin", 20)

        from orcapod.hashing.cache_population import populate_hash_cache

        populate_hash_cache(tmp_path, min_size_bytes=_MIN, db_path=db)
        second = populate_hash_cache(tmp_path, min_size_bytes=_MIN, db_path=db)

        assert second.total_bytes_cached == 20
        assert second.total_bytes_hashed == 0

    def test_total_bytes_cached_multiple_files(self, tmp_path):
        """total_bytes_cached sums across all already-cached files."""
        db = tmp_path / "cache.db"
        _write(tmp_path, "a.bin", 20)
        _write(tmp_path, "b.bin", 30)

        from orcapod.hashing.cache_population import populate_hash_cache

        populate_hash_cache(tmp_path, min_size_bytes=_MIN, db_path=db)
        second = populate_hash_cache(tmp_path, min_size_bytes=_MIN, db_path=db)

        assert second.total_bytes_cached == 50
        assert second.total_bytes_hashed == 0
```

Also update `TestCacheHitMiss.test_cache_hit_skips_rehash` to assert `total_bytes_cached`:

```python
def test_cache_hit_skips_rehash(self, tmp_path):
    db = tmp_path / "cache.db"
    _write(tmp_path, "f.bin", 20)

    from orcapod.hashing.cache_population import populate_hash_cache

    first = populate_hash_cache(tmp_path, min_size_bytes=_MIN, db_path=db)
    second = populate_hash_cache(tmp_path, min_size_bytes=_MIN, db_path=db)

    assert first.hashed == 1
    assert first.total_bytes_cached == 0
    assert second.already_cached == 1
    assert second.hashed == 0
    assert second.total_bytes_cached == 20
```

- [ ] **Step 1.2: Run tests to confirm they fail**

```bash
cd /path/to/orcapod-python
uv run pytest tests/test_hashing/test_cache_population.py -q 2>&1 | tail -15
```

Expected: failures on `test_instantiation`, `test_is_frozen`, `test_cache_hit_skips_rehash`, and all `TestCachedBytes` tests (missing `total_bytes_cached` field).

- [ ] **Step 1.3: Add `total_bytes_cached` to `CachePopulationStats`**

In `src/orcapod/hashing/cache_population.py`, update the dataclass:

```python
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
        total_bytes_cached: Sum of bytes across all already-cached files.
        total_duration: Wall-clock duration of the full run in seconds.
        avg_hashing_speed: Hashing throughput in bytes per second
            (``total_bytes_hashed / total_duration``; ``0.0`` if ``total_duration`` is zero).
    """

    hashed: int
    already_cached: int
    skipped_small: int
    errors: int
    total_bytes_hashed: int
    total_bytes_cached: int
    total_duration: float
    avg_hashing_speed: float
```

- [ ] **Step 1.4: Update `_hash_one` to return file size for cached hits**

In `_hash_one`, change the cached return from `("cached", 0)` to `("cached", file_stat.st_size)`:

```python
def _hash_one(
    resolved: UPath,
    file_stat: os.stat_result,
    cacher: SqliteHashCacher,
    hasher: FileHasher,
) -> tuple[Literal["hashed", "cached", "error"], int]:
    """Hash and cache a single qualifying file, returning a (category, bytes) result pair.
    ...
    """
    try:
        key = FileHashKey(resolved, file_stat.st_mtime_ns, file_stat.st_size)
        if cacher.get(key) is not None:
            return ("cached", file_stat.st_size)
        content_hash = hasher.hash_file(resolved)
        cacher.put(key, content_hash)
        return ("hashed", file_stat.st_size)
    except Exception:
        logger.warning("Failed to process file %s", resolved, exc_info=True)
        return ("error", 0)
```

- [ ] **Step 1.5: Add `already_cached_bytes` accumulator and `total_bytes_cached` to the return in `populate_hash_cache`**

In `populate_hash_cache`, inside the `with SqliteHashCacher(_db_path) as cacher:` block, add `already_cached_bytes = 0` alongside the other counter variables:

```python
            error_count = 0
            hashed = 0
            already_cached = 0
            skipped_small = 0
            total_bytes_hashed = 0
            already_cached_bytes = 0
            start = time.monotonic()
```

In both drain locations (the backpressure drain and the final drain), update the `"cached"` branch to accumulate bytes. Replace both occurrences of:

```python
                            elif cat == "cached":
                                already_cached += 1
```

With:

```python
                            elif cat == "cached":
                                already_cached += 1
                                already_cached_bytes += nbytes
```

Update the `return` statement at the bottom:

```python
            return CachePopulationStats(
                hashed=hashed,
                already_cached=already_cached,
                skipped_small=skipped_small,
                errors=error_count,
                total_bytes_hashed=total_bytes_hashed,
                total_bytes_cached=already_cached_bytes,
                total_duration=duration,
                avg_hashing_speed=speed,
            )
```

- [ ] **Step 1.6: Run tests to confirm they pass**

```bash
uv run pytest tests/test_hashing/test_cache_population.py -q 2>&1 | tail -10
```

Expected: `25 passed` (22 original + 3 new `TestCachedBytes` tests).

- [ ] **Step 1.7: Commit**

```bash
git add src/orcapod/hashing/cache_population.py tests/test_hashing/test_cache_population.py
git commit -m "feat(hashing): add total_bytes_cached to CachePopulationStats (ITL-500)"
```

---

## Task 2: Introduce `_Stats`, `_Accumulator`, `_HashVisitor` — pure refactoring

No new behavior. All 25 tests must still pass after this task.

**Files:**
- Modify: `src/orcapod/hashing/cache_population.py`

- [ ] **Step 2.1: Run existing tests to establish baseline**

```bash
uv run pytest tests/test_hashing/test_cache_population.py -q 2>&1 | tail -5
```

Expected: `25 passed`.

- [ ] **Step 2.2: Add type aliases and `_Stats` dataclass**

After the `CachePopulationStats` dataclass and before `_hash_one`, add:

```python
FileOutcome = Literal["hashed", "cached", "would_hash", "error"]


@dataclass
class _Stats:
    """Private mutable accumulator for per-run counters.

    Replaces the five loose local variables previously used inside
    ``populate_hash_cache``.
    """

    hashed: int = 0
    already_cached: int = 0
    skipped_small: int = 0
    errors: int = 0
    total_bytes_hashed: int = 0
    total_bytes_cached: int = 0
    start: float = field(default_factory=time.monotonic)

    def snapshot(self) -> CachePopulationStats:
        """Return a frozen ``CachePopulationStats`` snapshot of current totals."""
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

Also add `field` to the dataclasses import at the top of the file:

```python
from dataclasses import dataclass, field
```

- [ ] **Step 2.3: Add `_Accumulator` class**

After `_Stats`, add:

```python
from typing import Callable
from pathlib import Path


class _Accumulator:
    """Owns a ``_Stats`` object and an optional progress callback.

    All counter updates flow through this class so there is one place
    where snapshots are created and the callback is fired.
    """

    def __init__(self, callback: "ProgressCallback | None" = None) -> None:
        self._stats = _Stats()
        self._callback = callback

    def record(self, path: Path, outcome: FileOutcome, nbytes: int) -> None:
        """Update counters for a qualifying file and fire the callback if set.

        Args:
            path: Resolved file path that was just processed.
            outcome: What happened to the file.
            nbytes: File size in bytes (for hashed/would_hash/cached outcomes);
                ``0`` for errors.
        """
        if outcome in ("hashed", "would_hash"):
            self._stats.hashed += 1
            self._stats.total_bytes_hashed += nbytes
        elif outcome == "cached":
            self._stats.already_cached += 1
            self._stats.total_bytes_cached += nbytes
        else:  # "error"
            self._stats.errors += 1
        if self._callback is not None:
            self._callback(path, outcome, self._stats.snapshot())

    def record_skipped_small(self) -> None:
        """Increment ``skipped_small`` without firing the callback."""
        self._stats.skipped_small += 1

    def record_directory_error(self) -> None:
        """Increment ``errors`` for a directory access failure without firing the callback."""
        self._stats.errors += 1

    def finalize(self) -> CachePopulationStats:
        """Return the final frozen ``CachePopulationStats`` snapshot."""
        return self._stats.snapshot()
```

Note: `ProgressCallback` is defined in Task 5; use a forward reference string for now.

- [ ] **Step 2.4: Add `_HashVisitor` and remove `_hash_one`**

Replace the entire `_hash_one` function with `_HashVisitor`:

```python
class _HashVisitor:
    """Per-file visitor that checks the cache and hashes on miss.

    Callable with signature ``(resolved, file_stat) -> (FileOutcome, int)``.
    Thread-safe: ``SqliteHashCacher`` uses ``threading.local()`` connections
    and ``FileHasher`` is stateless.

    Args:
        cacher: Shared ``SqliteHashCacher`` instance.
        hasher: Shared ``FileHasher`` instance.
        force: If ``True``, skip the cache-get and always re-hash.
    """

    def __init__(
        self,
        cacher: SqliteHashCacher,
        hasher: FileHasher,
        *,
        force: bool = False,
    ) -> None:
        self._cacher = cacher
        self._hasher = hasher
        self._force = force

    def __call__(
        self, resolved: UPath, file_stat: os.stat_result
    ) -> tuple[FileOutcome, int]:
        try:
            key = FileHashKey(resolved, file_stat.st_mtime_ns, file_stat.st_size)
            if not self._force and self._cacher.get(key) is not None:
                return ("cached", file_stat.st_size)
            content_hash = self._hasher.hash_file(resolved)
            self._cacher.put(key, content_hash)
            return ("hashed", file_stat.st_size)
        except Exception:
            logger.warning("Failed to process file %s", resolved, exc_info=True)
            return ("error", 0)
```

- [ ] **Step 2.5: Refactor `populate_hash_cache` to use `_Accumulator`, `_HashVisitor`, and `dict[Future, Path]`**

Replace the body of `populate_hash_cache` (everything inside `with ThreadPoolExecutor` and `with SqliteHashCacher`). The new body:

```python
    root = UPath(path)
    _db_path: Path | None = Path(db_path) if db_path is not None else None
    hasher = FileHasher(algorithm=algorithm, buffer_size=buffer_size)

    with SqliteHashCacher(_db_path) as cacher:
        _db_resolved = cacher.db_path.resolve()
        _db_str = str(_db_resolved)
        _excluded: frozenset[UPath] = frozenset(
            [
                UPath(_db_resolved),
                UPath(_db_str + "-wal"),
                UPath(_db_str + "-shm"),
                UPath(_db_str + "-journal"),
            ]
        )

        visitor = _HashVisitor(cacher, hasher)
        accumulator = _Accumulator()
        _max_pending = max_workers * _PENDING_FACTOR
        pending: dict[Future[tuple[FileOutcome, int]], UPath] = {}

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            stack: list[UPath] = [root]
            while stack:
                current = stack.pop()
                try:
                    entries = list(current.iterdir())
                except OSError:
                    logger.warning(
                        "Cannot access directory %s", current, exc_info=True
                    )
                    accumulator.record_directory_error()
                    continue

                for entry in entries:
                    if entry.is_symlink():
                        continue
                    if entry.is_dir():
                        stack.append(entry)
                        continue
                    if not entry.is_file():
                        continue

                    try:
                        resolved = entry.resolve()
                        if resolved in _excluded:
                            continue
                        file_stat = resolved.stat()
                    except OSError:
                        logger.warning(
                            "Cannot stat file %s", entry, exc_info=True
                        )
                        accumulator.record(entry, "error", 0)
                        continue

                    if file_stat.st_size < min_size_bytes:
                        accumulator.record_skipped_small()
                        continue

                    if len(pending) >= _max_pending:
                        done_futures, _ = wait(pending.keys(), return_when=FIRST_COMPLETED)
                        for fut in done_futures:
                            p = pending.pop(fut)
                            outcome, nbytes = fut.result()
                            accumulator.record(p, outcome, nbytes)

                    future = executor.submit(visitor, resolved, file_stat)
                    pending[future] = resolved

            for fut in as_completed(pending):
                p = pending[fut]
                outcome, nbytes = fut.result()
                accumulator.record(p, outcome, nbytes)

    return accumulator.finalize()
```

Also remove the old local variable declarations (`error_count = 0`, `hashed = 0`, etc.) and the old `duration`/`speed`/`return` at the bottom — all now inside `accumulator.finalize()`.

- [ ] **Step 2.6: Run all tests to confirm refactoring preserves behavior**

```bash
uv run pytest tests/test_hashing/test_cache_population.py -q 2>&1 | tail -10
```

Expected: `25 passed` — no regressions.

- [ ] **Step 2.7: Commit**

```bash
git add src/orcapod/hashing/cache_population.py
git commit -m "refactor(hashing): introduce _Stats, _Accumulator, _HashVisitor (ITL-500)"
```

---

## Task 3: Add `force` parameter

**Files:**
- Modify: `src/orcapod/hashing/cache_population.py`
- Modify: `tests/test_hashing/test_cache_population.py`

- [ ] **Step 3.1: Write failing tests**

Add to `tests/test_hashing/test_cache_population.py`:

```python
class TestForce:
    def test_force_rehashes_cached_file(self, tmp_path):
        """force=True re-hashes a file even if already in the cache."""
        db = tmp_path / "cache.db"
        _write(tmp_path, "f.bin", 20)

        from orcapod.hashing.cache_population import populate_hash_cache

        first = populate_hash_cache(tmp_path, min_size_bytes=_MIN, db_path=db)
        assert first.hashed == 1

        second = populate_hash_cache(
            tmp_path, min_size_bytes=_MIN, db_path=db, force=True
        )
        assert second.hashed == 1
        assert second.already_cached == 0

    def test_force_false_skips_cached_file(self, tmp_path):
        """force=False (default) does not re-hash an already-cached file."""
        db = tmp_path / "cache.db"
        _write(tmp_path, "f.bin", 20)

        from orcapod.hashing.cache_population import populate_hash_cache

        populate_hash_cache(tmp_path, min_size_bytes=_MIN, db_path=db)
        second = populate_hash_cache(
            tmp_path, min_size_bytes=_MIN, db_path=db, force=False
        )
        assert second.already_cached == 1
        assert second.hashed == 0

    def test_force_bytes_hashed(self, tmp_path):
        """force=True: total_bytes_hashed counts re-hashed bytes; total_bytes_cached is zero."""
        db = tmp_path / "cache.db"
        _write(tmp_path, "f.bin", 20)

        from orcapod.hashing.cache_population import populate_hash_cache

        populate_hash_cache(tmp_path, min_size_bytes=_MIN, db_path=db)
        second = populate_hash_cache(
            tmp_path, min_size_bytes=_MIN, db_path=db, force=True
        )
        assert second.total_bytes_hashed == 20
        assert second.total_bytes_cached == 0
```

- [ ] **Step 3.2: Run tests to confirm they fail**

```bash
uv run pytest tests/test_hashing/test_cache_population.py::TestForce -q 2>&1 | tail -10
```

Expected: `TypeError: populate_hash_cache() got an unexpected keyword argument 'force'`

- [ ] **Step 3.3: Add `force` parameter to `populate_hash_cache` and pass it to `_HashVisitor`**

Add `force: bool = False` to the signature:

```python
def populate_hash_cache(
    path: PathLike | UPath,
    *,
    min_size_bytes: int = _DEFAULT_MIN_SIZE_BYTES,
    db_path: UPath | PathLike | None = None,
    algorithm: str = "sha256",
    buffer_size: int = 65536,
    max_workers: int = _DEFAULT_MAX_WORKERS,
    force: bool = False,
) -> CachePopulationStats:
```

Update the docstring to document `force`:

```
        force: If ``True``, re-hash files even if they already have a cache entry.
            Defaults to ``False``.
```

Pass `force` to `_HashVisitor` where the visitor is constructed:

```python
        visitor = _HashVisitor(cacher, hasher, force=force)
```

- [ ] **Step 3.4: Run tests to confirm they pass**

```bash
uv run pytest tests/test_hashing/test_cache_population.py -q 2>&1 | tail -10
```

Expected: `28 passed`.

- [ ] **Step 3.5: Commit**

```bash
git add src/orcapod/hashing/cache_population.py tests/test_hashing/test_cache_population.py
git commit -m "feat(hashing): add force parameter to populate_hash_cache (ITL-500)"
```

---

## Task 4: Add `_DryRunVisitor` and `dry_run` parameter

**Files:**
- Modify: `src/orcapod/hashing/cache_population.py`
- Modify: `tests/test_hashing/test_cache_population.py`

- [ ] **Step 4.1: Write failing tests**

Add to `tests/test_hashing/test_cache_population.py`:

```python
class TestVisitors:
    def test_dry_run_visitor_miss_returns_would_hash(self, tmp_path):
        """_DryRunVisitor returns would_hash for a file not in the cache."""
        import os
        from orcapod.hashing.cache_population import _DryRunVisitor
        from orcapod.hashing.hash_cachers import SqliteHashCacher

        db = tmp_path / "cache.db"
        f = _write(tmp_path, "f.bin", 20)
        resolved = f.resolve()
        file_stat = resolved.stat()

        with SqliteHashCacher(db) as cacher:
            visitor = _DryRunVisitor(cacher)
            outcome, nbytes = visitor(resolved, file_stat)

        assert outcome == "would_hash"
        assert nbytes == 20

    def test_dry_run_visitor_hit_returns_cached(self, tmp_path):
        """_DryRunVisitor returns cached for a file already in the cache."""
        import os
        from orcapod.hashing.cache_population import _DryRunVisitor, populate_hash_cache
        from orcapod.hashing.hash_cachers import SqliteHashCacher

        db = tmp_path / "cache.db"
        f = _write(tmp_path, "f.bin", 20)
        populate_hash_cache(tmp_path, min_size_bytes=_MIN, db_path=db)

        resolved = f.resolve()
        file_stat = resolved.stat()

        with SqliteHashCacher(db) as cacher:
            visitor = _DryRunVisitor(cacher)
            outcome, nbytes = visitor(resolved, file_stat)

        assert outcome == "cached"
        assert nbytes == 20

    def test_dry_run_visitor_force_always_would_hash(self, tmp_path):
        """_DryRunVisitor with force=True always returns would_hash regardless of cache."""
        from orcapod.hashing.cache_population import _DryRunVisitor, populate_hash_cache
        from orcapod.hashing.hash_cachers import SqliteHashCacher

        db = tmp_path / "cache.db"
        f = _write(tmp_path, "f.bin", 20)
        populate_hash_cache(tmp_path, min_size_bytes=_MIN, db_path=db)

        resolved = f.resolve()
        file_stat = resolved.stat()

        with SqliteHashCacher(db) as cacher:
            visitor = _DryRunVisitor(cacher, force=True)
            outcome, nbytes = visitor(resolved, file_stat)

        assert outcome == "would_hash"
        assert nbytes == 20


class TestDryRun:
    def test_dry_run_no_cache_writes(self, tmp_path):
        """dry_run=True must not write any entries to the cache."""
        db = tmp_path / "cache.db"
        _write(tmp_path, "f.bin", 20)

        from orcapod.hashing.cache_population import populate_hash_cache

        populate_hash_cache(tmp_path, min_size_bytes=_MIN, db_path=db, dry_run=True)

        # Second run with dry_run=False should hash (not find cache entries).
        second = populate_hash_cache(tmp_path, min_size_bytes=_MIN, db_path=db)
        assert second.hashed == 1
        assert second.already_cached == 0

    def test_dry_run_hashed_equals_would_hash_count(self, tmp_path):
        """In dry-run mode, stats.hashed counts files that would be hashed."""
        db = tmp_path / "cache.db"
        _write(tmp_path, "a.bin", 20)
        _write(tmp_path, "b.bin", 30)

        from orcapod.hashing.cache_population import populate_hash_cache

        stats = populate_hash_cache(tmp_path, min_size_bytes=_MIN, db_path=db, dry_run=True)
        assert stats.hashed == 2
        assert stats.total_bytes_hashed == 50

    def test_dry_run_already_cached_counted(self, tmp_path):
        """dry_run=True counts already-cached files and their bytes correctly."""
        db = tmp_path / "cache.db"
        _write(tmp_path, "f.bin", 20)

        from orcapod.hashing.cache_population import populate_hash_cache

        populate_hash_cache(tmp_path, min_size_bytes=_MIN, db_path=db)
        stats = populate_hash_cache(tmp_path, min_size_bytes=_MIN, db_path=db, dry_run=True)

        assert stats.already_cached == 1
        assert stats.total_bytes_cached == 20
        assert stats.hashed == 0

    def test_dry_run_force_all_would_hash(self, tmp_path):
        """dry_run=True, force=True: all qualifying files are would_hash regardless of cache."""
        db = tmp_path / "cache.db"
        _write(tmp_path, "f.bin", 20)

        from orcapod.hashing.cache_population import populate_hash_cache

        populate_hash_cache(tmp_path, min_size_bytes=_MIN, db_path=db)
        stats = populate_hash_cache(
            tmp_path, min_size_bytes=_MIN, db_path=db, dry_run=True, force=True
        )

        assert stats.hashed == 1
        assert stats.already_cached == 0
```

- [ ] **Step 4.2: Run tests to confirm they fail**

```bash
uv run pytest tests/test_hashing/test_cache_population.py::TestDryRun tests/test_hashing/test_cache_population.py::TestVisitors -q 2>&1 | tail -10
```

Expected: `ImportError` or `TypeError` — `_DryRunVisitor` and `dry_run` don't exist yet.

- [ ] **Step 4.3: Add `_DryRunVisitor` to `cache_population.py`**

After `_HashVisitor`, add:

```python
class _DryRunVisitor:
    """Per-file visitor that checks the cache without hashing or writing.

    Callable with signature ``(resolved, file_stat) -> (FileOutcome, int)``.
    Runs on the traversal thread — no thread pool needed for cache lookups.

    Args:
        cacher: Shared ``SqliteHashCacher`` instance.
        force: If ``True``, skip the cache check and always return ``"would_hash"``.
    """

    def __init__(
        self,
        cacher: SqliteHashCacher,
        *,
        force: bool = False,
    ) -> None:
        self._cacher = cacher
        self._force = force

    def __call__(
        self, resolved: UPath, file_stat: os.stat_result
    ) -> tuple[FileOutcome, int]:
        try:
            if self._force:
                return ("would_hash", file_stat.st_size)
            key = FileHashKey(resolved, file_stat.st_mtime_ns, file_stat.st_size)
            if self._cacher.get(key) is not None:
                return ("cached", file_stat.st_size)
            return ("would_hash", file_stat.st_size)
        except Exception:
            logger.warning("Failed to check file %s", resolved, exc_info=True)
            return ("error", 0)
```

- [ ] **Step 4.4: Add `dry_run` parameter and serial path to `populate_hash_cache`**

Add `dry_run: bool = False` to the signature:

```python
def populate_hash_cache(
    path: PathLike | UPath,
    *,
    min_size_bytes: int = _DEFAULT_MIN_SIZE_BYTES,
    db_path: UPath | PathLike | None = None,
    algorithm: str = "sha256",
    buffer_size: int = 65536,
    max_workers: int = _DEFAULT_MAX_WORKERS,
    force: bool = False,
    dry_run: bool = False,
) -> CachePopulationStats:
```

Add to the docstring:

```
        dry_run: If ``True``, perform the full walk, stat, size filter, and cache check
            but skip hashing and cache writes. ``stats.hashed`` reports how many files
            *would* be hashed. Defaults to ``False``.
        force: If ``True``, re-hash files even if they already have a cache entry.
            Defaults to ``False``.
```

After the `_excluded` block and before the visitor/accumulator construction, replace the single `visitor = _HashVisitor(...)` line with a branch:

```python
        if dry_run:
            visitor: _HashVisitor | _DryRunVisitor = _DryRunVisitor(cacher, force=force)
        else:
            visitor = _HashVisitor(cacher, hasher, force=force)
        accumulator = _Accumulator()
```

After the `accumulator = _Accumulator()` line, replace the entire `with ThreadPoolExecutor` block with a branch:

```python
        if dry_run:
            # Serial path — cache lookups are fast; no thread pool needed.
            stack: list[UPath] = [root]
            while stack:
                current = stack.pop()
                try:
                    entries = list(current.iterdir())
                except OSError:
                    logger.warning(
                        "Cannot access directory %s", current, exc_info=True
                    )
                    accumulator.record_directory_error()
                    continue

                for entry in entries:
                    if entry.is_symlink():
                        continue
                    if entry.is_dir():
                        stack.append(entry)
                        continue
                    if not entry.is_file():
                        continue

                    try:
                        resolved = entry.resolve()
                        if resolved in _excluded:
                            continue
                        file_stat = resolved.stat()
                    except OSError:
                        logger.warning(
                            "Cannot stat file %s", entry, exc_info=True
                        )
                        accumulator.record(entry, "error", 0)
                        continue

                    if file_stat.st_size < min_size_bytes:
                        accumulator.record_skipped_small()
                        continue

                    outcome, nbytes = visitor(resolved, file_stat)
                    accumulator.record(resolved, outcome, nbytes)
        else:
            _max_pending = max_workers * _PENDING_FACTOR
            pending: dict[Future[tuple[FileOutcome, int]], UPath] = {}

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                stack = [root]
                while stack:
                    current = stack.pop()
                    try:
                        entries = list(current.iterdir())
                    except OSError:
                        logger.warning(
                            "Cannot access directory %s", current, exc_info=True
                        )
                        accumulator.record_directory_error()
                        continue

                    for entry in entries:
                        if entry.is_symlink():
                            continue
                        if entry.is_dir():
                            stack.append(entry)
                            continue
                        if not entry.is_file():
                            continue

                        try:
                            resolved = entry.resolve()
                            if resolved in _excluded:
                                continue
                            file_stat = resolved.stat()
                        except OSError:
                            logger.warning(
                                "Cannot stat file %s", entry, exc_info=True
                            )
                            accumulator.record(entry, "error", 0)
                            continue

                        if file_stat.st_size < min_size_bytes:
                            accumulator.record_skipped_small()
                            continue

                        if len(pending) >= _max_pending:
                            done_futures, _ = wait(pending.keys(), return_when=FIRST_COMPLETED)
                            for fut in done_futures:
                                p = pending.pop(fut)
                                outcome, nbytes = fut.result()
                                accumulator.record(p, outcome, nbytes)

                        future = executor.submit(visitor, resolved, file_stat)
                        pending[future] = resolved

                for fut in as_completed(pending):
                    p = pending[fut]
                    outcome, nbytes = fut.result()
                    accumulator.record(p, outcome, nbytes)

    return accumulator.finalize()
```

The `return accumulator.finalize()` line sits at the same indentation level as the
`with SqliteHashCacher(...)` block — i.e., it is *inside* `populate_hash_cache` but
*outside* the cacher context manager. `accumulator` is still in scope because Python
local variables outlive the `with` block that created them.

- [ ] **Step 4.5: Run all tests**

```bash
uv run pytest tests/test_hashing/test_cache_population.py -q 2>&1 | tail -10
```

Expected: `35 passed`.

- [ ] **Step 4.6: Commit**

```bash
git add src/orcapod/hashing/cache_population.py tests/test_hashing/test_cache_population.py
git commit -m "feat(hashing): add dry_run parameter and _DryRunVisitor (ITL-500)"
```

---

## Task 5: Add `progress_callback` parameter

**Files:**
- Modify: `src/orcapod/hashing/cache_population.py`
- Modify: `tests/test_hashing/test_cache_population.py`

- [ ] **Step 5.1: Write failing tests**

Add to `tests/test_hashing/test_cache_population.py`:

```python
class TestProgressCallback:
    def test_callback_fires_once_per_qualifying_file(self, tmp_path):
        """Callback is called exactly once for each file that passes the size filter."""
        db = tmp_path / "cache.db"
        _write(tmp_path, "a.bin", 20)
        _write(tmp_path, "b.bin", 20)

        from orcapod.hashing.cache_population import populate_hash_cache

        calls = []
        populate_hash_cache(
            tmp_path,
            min_size_bytes=_MIN,
            db_path=db,
            progress_callback=lambda path, outcome, stats: calls.append(outcome),
        )

        assert len(calls) == 2
        assert all(o == "hashed" for o in calls)

    def test_callback_receives_correct_path(self, tmp_path):
        """Callback path argument is the resolved path of the file just processed."""
        db = tmp_path / "cache.db"
        f = _write(tmp_path, "f.bin", 20)

        from orcapod.hashing.cache_population import populate_hash_cache

        paths_seen = []
        populate_hash_cache(
            tmp_path,
            min_size_bytes=_MIN,
            db_path=db,
            progress_callback=lambda path, outcome, stats: paths_seen.append(path),
        )

        assert len(paths_seen) == 1
        assert paths_seen[0] == f.resolve()

    def test_callback_not_fired_for_skipped_small(self, tmp_path):
        """Files below min_size_bytes do not trigger the callback."""
        db = tmp_path / "cache.db"
        _write(tmp_path, "small.bin", 5)   # below _MIN=10
        _write(tmp_path, "big.bin", 20)

        from orcapod.hashing.cache_population import populate_hash_cache

        calls = []
        populate_hash_cache(
            tmp_path,
            min_size_bytes=_MIN,
            db_path=db,
            progress_callback=lambda path, outcome, stats: calls.append(outcome),
        )

        assert len(calls) == 1
        assert calls[0] == "hashed"

    def test_callback_running_totals_are_accurate(self, tmp_path):
        """Each callback invocation receives a snapshot with totals correct up to that point."""
        db = tmp_path / "cache.db"
        _write(tmp_path, "a.bin", 20)
        _write(tmp_path, "b.bin", 30)

        from orcapod.hashing.cache_population import populate_hash_cache

        snapshots = []
        populate_hash_cache(
            tmp_path,
            min_size_bytes=_MIN,
            db_path=db,
            progress_callback=lambda path, outcome, stats: snapshots.append(stats),
        )

        assert len(snapshots) == 2
        # After both files are processed the last snapshot has hashed=2 and correct total bytes.
        last = snapshots[-1]
        assert last.hashed == 2
        assert last.total_bytes_hashed == 50

    def test_callback_outcome_cached(self, tmp_path):
        """On second run, callback receives 'cached' outcome."""
        db = tmp_path / "cache.db"
        _write(tmp_path, "f.bin", 20)

        from orcapod.hashing.cache_population import populate_hash_cache

        populate_hash_cache(tmp_path, min_size_bytes=_MIN, db_path=db)

        outcomes = []
        populate_hash_cache(
            tmp_path,
            min_size_bytes=_MIN,
            db_path=db,
            progress_callback=lambda path, outcome, stats: outcomes.append(outcome),
        )

        assert outcomes == ["cached"]

    def test_callback_outcome_would_hash_in_dry_run(self, tmp_path):
        """In dry-run mode, callback receives 'would_hash' for uncached files."""
        db = tmp_path / "cache.db"
        _write(tmp_path, "f.bin", 20)

        from orcapod.hashing.cache_population import populate_hash_cache

        outcomes = []
        populate_hash_cache(
            tmp_path,
            min_size_bytes=_MIN,
            db_path=db,
            dry_run=True,
            progress_callback=lambda path, outcome, stats: outcomes.append(outcome),
        )

        assert outcomes == ["would_hash"]
```

- [ ] **Step 5.2: Run tests to confirm they fail**

```bash
uv run pytest tests/test_hashing/test_cache_population.py::TestProgressCallback -q 2>&1 | tail -10
```

Expected: `TypeError: populate_hash_cache() got an unexpected keyword argument 'progress_callback'`

- [ ] **Step 5.3: Add `ProgressCallback` type alias to `cache_population.py`**

After the `FileOutcome` line, add:

```python
ProgressCallback = Callable[["Path", FileOutcome, CachePopulationStats], None]
```

Also ensure `Callable` and `Path` are imported. Replace the forward reference in `_Accumulator.__init__` with the real type now that `ProgressCallback` is defined before `_Accumulator`.

At the top of the file, update the imports:

```python
from pathlib import Path
from typing import Callable, Literal
```

Update `_Accumulator.__init__` signature to use the real type:

```python
    def __init__(self, callback: ProgressCallback | None = None) -> None:
```

- [ ] **Step 5.4: Add `progress_callback` parameter to `populate_hash_cache`**

Add to the signature:

```python
def populate_hash_cache(
    path: PathLike | UPath,
    *,
    min_size_bytes: int = _DEFAULT_MIN_SIZE_BYTES,
    db_path: UPath | PathLike | None = None,
    algorithm: str = "sha256",
    buffer_size: int = 65536,
    max_workers: int = _DEFAULT_MAX_WORKERS,
    force: bool = False,
    dry_run: bool = False,
    progress_callback: ProgressCallback | None = None,
) -> CachePopulationStats:
```

Add to the docstring:

```
        progress_callback: Optional callable invoked once per qualifying file
            (i.e. files that pass the size filter). Receives the resolved
            ``Path``, a ``FileOutcome`` string, and a frozen
            ``CachePopulationStats`` snapshot of running totals at that moment.
            Not called for files below ``min_size_bytes`` or for directory
            access errors. Defaults to ``None``.
```

Pass the callback to `_Accumulator`:

```python
        accumulator = _Accumulator(progress_callback)
```

- [ ] **Step 5.5: Run all tests**

```bash
uv run pytest tests/test_hashing/test_cache_population.py -q 2>&1 | tail -10
```

Expected: `41 passed`.

- [ ] **Step 5.6: Commit**

```bash
git add src/orcapod/hashing/cache_population.py tests/test_hashing/test_cache_population.py
git commit -m "feat(hashing): add progress_callback parameter to populate_hash_cache (ITL-500)"
```

---

## Task 6: Update CLI with `--dry-run`, `--force`, and cached-bytes output

**Files:**
- Modify: `src/orcapod/cli/warm_cache.py`
- Modify: `tests/test_hashing/test_cache_population.py`

- [ ] **Step 6.1: Write failing CLI tests**

Add to `tests/test_hashing/test_cache_population.py`. First, add the import at the top of the file:

```python
from typer.testing import CliRunner
```

Then add the test class:

```python
class TestCLI:
    def _app(self):
        import typer
        from orcapod.cli.warm_cache import warm_cache
        app = typer.Typer()
        app.command()(warm_cache)
        return app

    def test_force_flag_wires_through(self, tmp_path):
        """--force causes already-cached files to be re-hashed."""
        runner = CliRunner()
        db = tmp_path / "cache.db"
        _write(tmp_path, "f.bin", 20)

        app = self._app()
        # First run to populate cache.
        runner.invoke(app, [str(tmp_path), "--min-size", "0", "--db-path", str(db)])
        # Second run with --force.
        result = runner.invoke(
            app, [str(tmp_path), "--min-size", "0", "--db-path", str(db), "--force"]
        )
        assert result.exit_code == 0
        assert "1 hashed" in result.output
        assert "0 already cached" in result.output

    def test_dry_run_flag_wires_through(self, tmp_path):
        """--dry-run prints 'would be hashed' and makes no cache writes."""
        runner = CliRunner()
        db = tmp_path / "cache.db"
        _write(tmp_path, "f.bin", 20)

        app = self._app()
        result = runner.invoke(
            app, [str(tmp_path), "--min-size", "0", "--db-path", str(db), "--dry-run"]
        )
        assert result.exit_code == 0
        assert "would be hashed" in result.output
        assert "Dry run" in result.output

        # Cache must be empty after dry-run.
        real_run = runner.invoke(
            app, [str(tmp_path), "--min-size", "0", "--db-path", str(db)]
        )
        assert "1 hashed" in real_run.output

    def test_normal_output_includes_cached_gb(self, tmp_path):
        """Normal run output shows cached GB on the second run."""
        runner = CliRunner()
        db = tmp_path / "cache.db"
        _write(tmp_path, "f.bin", 20)

        app = self._app()
        runner.invoke(app, [str(tmp_path), "--min-size", "0", "--db-path", str(db)])
        result = runner.invoke(
            app, [str(tmp_path), "--min-size", "0", "--db-path", str(db)]
        )
        assert result.exit_code == 0
        # "1 already cached (0.00 GB)" should appear.
        assert "already cached" in result.output
        assert "GB" in result.output
```

- [ ] **Step 6.2: Run tests to confirm they fail**

```bash
uv run pytest tests/test_hashing/test_cache_population.py::TestCLI -q 2>&1 | tail -10
```

Expected: failures — `--force` and `--dry-run` flags don't exist yet.

- [ ] **Step 6.3: Update `warm_cache.py`**

Replace the entire contents of `src/orcapod/cli/warm_cache.py` with:

```python
"""``orcapod warm-cache`` subcommand.

Pre-populates the SQLite file-hash cache for large files under a target
directory so that subsequent pipeline runs skip expensive content hashing.
"""

from __future__ import annotations

from pathlib import Path

import typer
from upath import UPath

_DEFAULT_MIN_SIZE_MB: float = 500.0


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
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Scan and check cache without hashing. Prints what would be done.",
    ),
    force: bool = typer.Option(
        False,
        "--force",
        help="Re-hash files even if already cached.",
    ),
) -> None:
    """Pre-populate the file-hash cache for large files under PATH.

    Recursively scans PATH and hashes every file that is at least MIN_SIZE MB.
    Files already present in the cache are skipped unless --force is given.
    Use --dry-run to preview what would be done without writing to the cache.
    On completion, prints a summary with counts and throughput.
    """
    from orcapod.hashing.cache_population import populate_hash_cache

    min_size_bytes = int(min_size * 1024 * 1024)
    _db_path: Path | None = Path(db_path) if db_path is not None else None

    if max_workers < 1:
        typer.echo("Error: --workers must be at least 1", err=True)
        raise typer.Exit(code=1)

    root = UPath(path)
    if not root.exists():
        typer.echo(f"Error: path does not exist: {path}", err=True)
        raise typer.Exit(code=1)
    if not root.is_dir():
        typer.echo(f"Error: path is not a directory: {path}", err=True)
        raise typer.Exit(code=1)

    if dry_run:
        typer.echo(f"Dry-run scan of {path} ...")
    else:
        typer.echo(f"Scanning {path} ...")

    stats = populate_hash_cache(
        path,
        min_size_bytes=min_size_bytes,
        db_path=_db_path,
        algorithm=algorithm,
        buffer_size=buffer_size,
        max_workers=max_workers,
        dry_run=dry_run,
        force=force,
    )

    gb_hashed = stats.total_bytes_hashed / (1024**3)
    gb_cached = stats.total_bytes_cached / (1024**3)
    speed_gb = stats.avg_hashing_speed / (1024**3)
    min_size_display = f"{min_size:g} MB"

    if dry_run:
        typer.echo(
            f"Dry run complete in {stats.total_duration:.1f}s — "
            f"{stats.hashed} would be hashed ({gb_hashed:.2f} GB), "
            f"{stats.already_cached} already cached ({gb_cached:.2f} GB), "
            f"{stats.skipped_small} skipped (< {min_size_display}), "
            f"{stats.errors} errors."
        )
    else:
        typer.echo(
            f"Done in {stats.total_duration:.1f}s — "
            f"{stats.hashed} hashed ({gb_hashed:.2f} GB), "
            f"{stats.already_cached} already cached ({gb_cached:.2f} GB), "
            f"{stats.skipped_small} skipped (< {min_size_display}), "
            f"{stats.errors} errors."
        )
        if stats.hashed > 0:
            typer.echo(f"Average hashing speed: {speed_gb:.2f} GB/s")
```

- [ ] **Step 6.4: Run all tests**

```bash
uv run pytest tests/test_hashing/test_cache_population.py -q 2>&1 | tail -10
```

Expected: `44 passed`.

- [ ] **Step 6.5: Run the full test suite to check for regressions**

```bash
uv run pytest -q 2>&1 | tail -15
```

Expected: all tests pass.

- [ ] **Step 6.6: Commit**

```bash
git add src/orcapod/cli/warm_cache.py tests/test_hashing/test_cache_population.py
git commit -m "feat(cli): add --dry-run and --force to orcapod warm-cache (ITL-500)"
```

---

## Task 7: Expose new types in public API and push branch

**Files:**
- Modify: `src/orcapod/hashing/__init__.py`

- [ ] **Step 7.1: Export `FileOutcome` and `ProgressCallback` from `orcapod.hashing`**

In `src/orcapod/hashing/__init__.py`, update the import from `cache_population`:

```python
from orcapod.hashing.cache_population import (
    CachePopulationStats,
    FileOutcome,
    ProgressCallback,
    populate_hash_cache,
)
```

Add `"FileOutcome"` and `"ProgressCallback"` to `__all__`:

```python
    "CachePopulationStats",
    "FileOutcome",
    "ProgressCallback",
    "populate_hash_cache",
```

- [ ] **Step 7.2: Verify the new names are importable**

```bash
uv run python -c "from orcapod.hashing import FileOutcome, ProgressCallback, CachePopulationStats, populate_hash_cache; print('OK')"
```

Expected: `OK`

- [ ] **Step 7.3: Run the full test suite one final time**

```bash
uv run pytest -q 2>&1 | tail -10
```

Expected: all tests pass.

- [ ] **Step 7.4: Commit**

```bash
git add src/orcapod/hashing/__init__.py
git commit -m "feat(hashing): export FileOutcome and ProgressCallback from orcapod.hashing (ITL-500)"
```

- [ ] **Step 7.5: Push the feature branch**

```bash
git push -u origin eywalker/itl-500-improve-populate_hash_cache-ergonomics-progress-callback-dry
```
