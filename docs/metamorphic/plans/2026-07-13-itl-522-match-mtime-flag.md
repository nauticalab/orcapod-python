# ITL-522: `match_mtime` Flag Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `match_mtime: bool = True` to all three hash cacher backends so that cache lookups can optionally ignore `mtime_ns` and match on path + size only, returning the entry with the highest `mtime_ns` on multi-hit.

**Architecture:** The flag lives on each cacher (`InMemoryHashCacher`, `SqliteHashCacher`, `PostgresHashCacher`) and affects only `get()` — the write path (`put()`) is unchanged. `enable_file_hash_caching()` in `contexts/__init__.py` is updated to accept and forward the flag. All tests use TDD (write test first, run to see it fail, implement, run again to see it pass).

**Tech Stack:** Python 3.11+, SQLite3 (stdlib), psycopg3, pytest, uv run

---

## File Map

| Action | File | What changes |
|--------|------|-------------|
| Modify | `src/orcapod/hashing/hash_cachers.py` | `InMemoryHashCacher` and `SqliteHashCacher`: constructor + `get()` + `__repr__` |
| Modify | `src/orcapod/hashing/postgres_hash_cacher.py` | `PostgresHashCacher`: constructor + `get()` + `__repr__` |
| Modify | `src/orcapod/contexts/__init__.py` | `enable_file_hash_caching()`: new `match_mtime` kwarg + forwarding |
| Create | `tests/test_hashing/test_hash_cacher_match_mtime.py` | All `match_mtime` unit + integration tests |
| Modify | `tests/test_hashing/test_hash_cachers.py` | Add one test to `TestEnableFileHashCaching` |
| Modify | `tests/test_hashing/test_postgres_hash_cacher.py` | Add `match_mtime` tests to postgres suite |
| Modify | `docs/concepts/file-hash-caching.md` | New "Ignoring mtime in cache lookups" section |

---

## Task 1: `InMemoryHashCacher` — `match_mtime` flag

**Files:**
- Create: `tests/test_hashing/test_hash_cacher_match_mtime.py`
- Modify: `src/orcapod/hashing/hash_cachers.py`

- [ ] **Step 1: Create the test file with `InMemoryHashCacher` tests**

Create `tests/test_hashing/test_hash_cacher_match_mtime.py` with this content:

```python
"""Tests for hash cacher match_mtime flag (ITL-522).

Covers InMemoryHashCacher and SqliteHashCacher for the match_mtime flag,
plus CachedFileHasher integration with real files.
"""
from __future__ import annotations

import os

import pytest
from upath import UPath

from orcapod.hashing.file_hashers import CachedFileHasher, FileHashKey, FileHasher
from orcapod.hashing.hash_cachers import InMemoryHashCacher, SqliteHashCacher
from orcapod.types import ContentHash


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_key(
    path: str = "/a/b.txt", mtime_ns: int = 1000, size: int = 100
) -> FileHashKey:
    return FileHashKey(path=UPath(path), mtime_ns=mtime_ns, size=size)


def make_hash(digest: bytes = b"\xab" * 32) -> ContentHash:
    return ContentHash(method="sha256", digest=digest)


# ---------------------------------------------------------------------------
# InMemoryHashCacher — match_mtime
# ---------------------------------------------------------------------------


class TestInMemoryHashCacherMatchMtime:
    def test_default_true_mtime_change_causes_miss(self):
        """Default (match_mtime=True): different mtime → cache miss."""
        cacher = InMemoryHashCacher()
        cacher.put(make_key(mtime_ns=1000, size=100), make_hash())
        assert cacher.get(make_key(mtime_ns=2000, size=100)) is None

    def test_false_mtime_change_is_hit(self):
        """match_mtime=False: different mtime, same path+size → cache hit."""
        cacher = InMemoryHashCacher(match_mtime=False)
        value = make_hash()
        cacher.put(make_key(mtime_ns=1000, size=100), value)
        assert cacher.get(make_key(mtime_ns=2000, size=100)) == value

    def test_false_size_change_is_miss(self):
        """match_mtime=False: different size → cache miss (size guard still applies)."""
        cacher = InMemoryHashCacher(match_mtime=False)
        cacher.put(make_key(mtime_ns=1000, size=100), make_hash())
        assert cacher.get(make_key(mtime_ns=2000, size=200)) is None

    def test_false_different_path_is_miss(self):
        """match_mtime=False: different path → cache miss."""
        cacher = InMemoryHashCacher(match_mtime=False)
        cacher.put(make_key(path="/a/b.txt", size=100), make_hash())
        assert cacher.get(make_key(path="/a/c.txt", size=100)) is None

    def test_false_returns_latest_mtime_entry(self):
        """match_mtime=False: multiple entries for same path+size → returns highest mtime_ns."""
        cacher = InMemoryHashCacher(match_mtime=False)
        hash_old = make_hash(b"\xaa" * 32)
        hash_new = make_hash(b"\xbb" * 32)
        cacher.put(make_key(mtime_ns=1000, size=100), hash_old)
        cacher.put(make_key(mtime_ns=2000, size=100), hash_new)
        result = cacher.get(make_key(mtime_ns=3000, size=100))
        assert result == hash_new

    def test_false_no_entries_returns_none(self):
        """match_mtime=False: no matching path+size → None."""
        cacher = InMemoryHashCacher(match_mtime=False)
        assert cacher.get(make_key()) is None

    def test_repr_shows_match_mtime_true(self):
        assert "match_mtime=True" in repr(InMemoryHashCacher())

    def test_repr_shows_match_mtime_false(self):
        assert "match_mtime=False" in repr(InMemoryHashCacher(match_mtime=False))
```

- [ ] **Step 2: Run to verify tests fail**

```bash
uv run pytest tests/test_hashing/test_hash_cacher_match_mtime.py::TestInMemoryHashCacherMatchMtime -v
```

Expected: errors like `TypeError: __init__() got an unexpected keyword argument 'match_mtime'`

- [ ] **Step 3: Add `match_mtime` to `InMemoryHashCacher`**

In `src/orcapod/hashing/hash_cachers.py`, update `InMemoryHashCacher.__init__`:

```python
def __init__(
    self,
    *,
    read_only: bool = False,
    min_cache_size_bytes: int | None = None,
    match_mtime: bool = True,
) -> None:
    if min_cache_size_bytes is not None and min_cache_size_bytes < 0:
        raise ValueError(
            f"min_cache_size_bytes must be None or a non-negative integer, "
            f"got {min_cache_size_bytes!r}"
        )
    self._cache: dict[FileHashKey, ContentHash] = {}
    self._read_only = read_only
    self._min_cache_size_bytes = min_cache_size_bytes
    self._match_mtime = match_mtime
```

Replace `InMemoryHashCacher.get()`:

```python
def get(self, key: FileHashKey) -> ContentHash | None:
    """Return the cached ``ContentHash`` for ``key``, or ``None`` on miss.

    When ``match_mtime=True`` (default), all three key fields must match.
    When ``match_mtime=False``, only ``path`` and ``size`` are compared;
    among all matching entries the one with the highest ``mtime_ns`` is
    returned.

    Args:
        key: File hash cache key.

    Returns:
        Cached ``ContentHash``, or ``None`` if not found.
    """
    if self._match_mtime:
        return self._cache.get(key)
    best_key: FileHashKey | None = None
    best_value: ContentHash | None = None
    for cached_key, value in self._cache.items():
        if cached_key.path == key.path and cached_key.size == key.size:
            if best_key is None or cached_key.mtime_ns > best_key.mtime_ns:
                best_key = cached_key
                best_value = value
    return best_value
```

Replace `InMemoryHashCacher.__repr__`:

```python
def __repr__(self) -> str:
    return (
        f"InMemoryHashCacher("
        f"read_only={self._read_only!r}, "
        f"min_cache_size_bytes={self._min_cache_size_bytes!r}, "
        f"match_mtime={self._match_mtime!r})"
    )
```

Also update the class docstring to document the new arg:

```python
class InMemoryHashCacher:
    """Dict-backed file hash cacher for testing and ephemeral in-process use.

    No persistence, no thread-safety guarantees beyond the GIL, no eviction.
    Use ``SqliteHashCacher`` for production workloads.

    Args:
        read_only: When ``True``, all ``put()`` calls are silent no-ops.
            ``get()`` still works normally. Defaults to ``False``.
        min_cache_size_bytes: When set to a positive integer, files whose
            ``key.size`` is strictly below this threshold are not inserted.
            ``None`` and ``0`` disable the threshold (default behaviour).
            Negative values raise ``ValueError``. Defaults to ``None``.
        match_mtime: When ``True`` (default), cache hits require
            ``path``, ``mtime_ns``, and ``size`` to all match. When
            ``False``, only ``path`` and ``size`` are compared; among
            multiple matching entries the one with the highest ``mtime_ns``
            is returned. The write path is unaffected — ``mtime_ns`` is
            always stored.
    """
```

- [ ] **Step 4: Run tests and verify they pass**

```bash
uv run pytest tests/test_hashing/test_hash_cacher_match_mtime.py::TestInMemoryHashCacherMatchMtime -v
```

Expected: all 8 tests PASS

- [ ] **Step 5: Verify no regressions in existing cacher tests**

```bash
uv run pytest tests/test_hashing/test_hash_cachers.py tests/test_hashing/test_hash_cacher_write_guards.py -v
```

Expected: all pass

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/hashing/hash_cachers.py tests/test_hashing/test_hash_cacher_match_mtime.py
git commit -m "feat(hashing): add match_mtime flag to InMemoryHashCacher (ITL-522)"
```

---

## Task 2: `SqliteHashCacher` — `match_mtime` flag

**Files:**
- Modify: `tests/test_hashing/test_hash_cacher_match_mtime.py`
- Modify: `src/orcapod/hashing/hash_cachers.py`

- [ ] **Step 1: Add `SqliteHashCacher` tests to the test file**

Append this class to `tests/test_hashing/test_hash_cacher_match_mtime.py`:

```python
# ---------------------------------------------------------------------------
# SqliteHashCacher — match_mtime
# ---------------------------------------------------------------------------


class TestSqliteHashCacherMatchMtime:
    def test_default_true_mtime_change_causes_miss(self, tmp_path):
        """Default (match_mtime=True): different mtime → cache miss."""
        cacher = SqliteHashCacher(tmp_path / "cache.db")
        cacher.put(make_key(mtime_ns=1000, size=100), make_hash())
        assert cacher.get(make_key(mtime_ns=2000, size=100)) is None

    def test_false_mtime_change_is_hit(self, tmp_path):
        """match_mtime=False: different mtime, same path+size → cache hit."""
        cacher = SqliteHashCacher(tmp_path / "cache.db", match_mtime=False)
        value = make_hash()
        cacher.put(make_key(mtime_ns=1000, size=100), value)
        result = cacher.get(make_key(mtime_ns=2000, size=100))
        assert result is not None
        assert result.digest == value.digest

    def test_false_size_change_is_miss(self, tmp_path):
        """match_mtime=False: different size → cache miss."""
        cacher = SqliteHashCacher(tmp_path / "cache.db", match_mtime=False)
        cacher.put(make_key(mtime_ns=1000, size=100), make_hash())
        assert cacher.get(make_key(mtime_ns=2000, size=200)) is None

    def test_false_different_path_is_miss(self, tmp_path):
        """match_mtime=False: different path → cache miss."""
        cacher = SqliteHashCacher(tmp_path / "cache.db", match_mtime=False)
        cacher.put(make_key(path="/a/b.txt", size=100), make_hash())
        assert cacher.get(make_key(path="/a/c.txt", size=100)) is None

    def test_false_returns_latest_mtime_entry(self, tmp_path):
        """match_mtime=False: multiple entries for same path+size → returns highest mtime_ns."""
        cacher = SqliteHashCacher(tmp_path / "cache.db", match_mtime=False)
        hash_old = make_hash(b"\xaa" * 32)
        hash_new = make_hash(b"\xbb" * 32)
        cacher.put(make_key(mtime_ns=1000, size=100), hash_old)
        cacher.put(make_key(mtime_ns=2000, size=100), hash_new)
        result = cacher.get(make_key(mtime_ns=3000, size=100))
        assert result is not None
        assert result.digest == hash_new.digest

    def test_false_no_entries_returns_none(self, tmp_path):
        """match_mtime=False: no matching path+size → None."""
        cacher = SqliteHashCacher(tmp_path / "cache.db", match_mtime=False)
        assert cacher.get(make_key()) is None

    def test_repr_shows_match_mtime_true(self, tmp_path):
        cacher = SqliteHashCacher(tmp_path / "cache.db")
        assert "match_mtime=True" in repr(cacher)

    def test_repr_shows_match_mtime_false(self, tmp_path):
        cacher = SqliteHashCacher(tmp_path / "cache.db", match_mtime=False)
        assert "match_mtime=False" in repr(cacher)
```

- [ ] **Step 2: Run to verify tests fail**

```bash
uv run pytest tests/test_hashing/test_hash_cacher_match_mtime.py::TestSqliteHashCacherMatchMtime -v
```

Expected: `TypeError: __init__() got an unexpected keyword argument 'match_mtime'`

- [ ] **Step 3: Add `match_mtime` to `SqliteHashCacher`**

In `src/orcapod/hashing/hash_cachers.py`, update `SqliteHashCacher.__init__`:

```python
def __init__(
    self,
    db_path: Path | None = None,
    *,
    read_only: bool = False,
    min_cache_size_bytes: int | None = None,
    match_mtime: bool = True,
) -> None:
    if min_cache_size_bytes is not None and min_cache_size_bytes < 0:
        raise ValueError(
            f"min_cache_size_bytes must be None or a non-negative integer, "
            f"got {min_cache_size_bytes!r}"
        )
    self.db_path = Path(
        db_path
        or os.environ.get("ORCAPOD_HASH_CACHE_DB")
        or self.DEFAULT_DB_PATH
    )
    self._read_only = read_only
    self._min_cache_size_bytes = min_cache_size_bytes
    self._match_mtime = match_mtime
    self._local = threading.local()
    self._ensure_schema()
```

Replace `SqliteHashCacher.get()`:

```python
def get(self, key: FileHashKey) -> ContentHash | None:
    """Return the cached ``ContentHash`` for ``key``, or ``None`` on miss.

    When ``match_mtime=True`` (default), the query filters on
    ``path``, ``mtime_ns``, and ``size``. When ``match_mtime=False``,
    only ``path`` and ``size`` are filtered and results are ordered
    by ``mtime_ns DESC`` so the most recent entry is returned.

    Args:
        key: File hash cache key.

    Returns:
        Cached ``ContentHash``, or ``None`` if not found.
    """
    conn = self._connection()
    if self._match_mtime:
        cursor = conn.execute(
            "SELECT hash FROM file_hash_cache WHERE path=? AND mtime_ns=? AND size=?",
            (str(key.path), key.mtime_ns, key.size),
        )
    else:
        cursor = conn.execute(
            "SELECT hash FROM file_hash_cache "
            "WHERE path=? AND size=? ORDER BY mtime_ns DESC LIMIT 1",
            (str(key.path), key.size),
        )
    row = cursor.fetchone()
    if row is None:
        return None
    blob: bytes = row[0]
    method_bytes, digest = blob.split(b":", 1)
    return ContentHash(method=method_bytes.decode("ascii"), digest=digest)
```

Replace `SqliteHashCacher.__repr__`:

```python
def __repr__(self) -> str:
    return (
        f"SqliteHashCacher("
        f"db_path={str(self.db_path)!r}, "
        f"read_only={self._read_only!r}, "
        f"min_cache_size_bytes={self._min_cache_size_bytes!r}, "
        f"match_mtime={self._match_mtime!r})"
    )
```

Also add `match_mtime` to the `SqliteHashCacher` class docstring Args section:

```
        match_mtime: When ``True`` (default), cache hits require
            ``path``, ``mtime_ns``, and ``size`` to all match. When
            ``False``, only ``path`` and ``size`` are compared; among
            multiple matching entries the one with the highest ``mtime_ns``
            is returned. The write path is unaffected — ``mtime_ns`` is
            always stored.
```

- [ ] **Step 4: Run tests and verify they pass**

```bash
uv run pytest tests/test_hashing/test_hash_cacher_match_mtime.py::TestSqliteHashCacherMatchMtime -v
```

Expected: all 8 tests PASS

- [ ] **Step 5: Verify no regressions**

```bash
uv run pytest tests/test_hashing/test_hash_cachers.py tests/test_hashing/test_hash_cacher_write_guards.py tests/test_hashing/test_sqlite_cacher.py -v
```

Expected: all pass

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/hashing/hash_cachers.py tests/test_hashing/test_hash_cacher_match_mtime.py
git commit -m "feat(hashing): add match_mtime flag to SqliteHashCacher (ITL-522)"
```

---

## Task 3: `PostgresHashCacher` — `match_mtime` flag

**Files:**
- Modify: `tests/test_hashing/test_postgres_hash_cacher.py`
- Modify: `src/orcapod/hashing/postgres_hash_cacher.py`

- [ ] **Step 1: Add `match_mtime` tests to the Postgres test file**

In `tests/test_hashing/test_postgres_hash_cacher.py`, add a new test class after the existing `TestPostgresHashCacherWriteGuards` class:

```python
@pytest.mark.postgres
class TestPostgresHashCacherMatchMtime:
    def test_default_true_mtime_change_causes_miss(self, pg_conninfo):
        """Default (match_mtime=True): different mtime → cache miss."""
        cacher = PostgresHashCacher(pg_conninfo)
        cacher.clear()
        cacher.put(make_key(mtime_ns=1000, size=100), make_hash(b"\xaa" * 32))
        assert cacher.get(make_key(mtime_ns=2000, size=100)) is None
        cacher.close()

    def test_false_mtime_change_is_hit(self, pg_conninfo):
        """match_mtime=False: different mtime, same path+size → cache hit."""
        cacher = PostgresHashCacher(pg_conninfo, match_mtime=False)
        cacher.clear()
        value = make_hash(b"\xbb" * 32)
        cacher.put(make_key(mtime_ns=1000, size=100), value)
        result = cacher.get(make_key(mtime_ns=2000, size=100))
        assert result is not None
        assert result.digest == value.digest
        cacher.close()

    def test_false_size_change_is_miss(self, pg_conninfo):
        """match_mtime=False: different size → cache miss."""
        cacher = PostgresHashCacher(pg_conninfo, match_mtime=False)
        cacher.clear()
        cacher.put(make_key(mtime_ns=1000, size=100), make_hash())
        assert cacher.get(make_key(mtime_ns=2000, size=200)) is None
        cacher.close()

    def test_false_returns_latest_mtime_entry(self, pg_conninfo):
        """match_mtime=False: multiple entries for same path+size → returns highest mtime_ns."""
        cacher = PostgresHashCacher(pg_conninfo, match_mtime=False)
        cacher.clear()
        hash_old = make_hash(b"\xaa" * 32)
        hash_new = make_hash(b"\xbb" * 32)
        cacher.put(make_key(mtime_ns=1000, size=100), hash_old)
        cacher.put(make_key(mtime_ns=2000, size=100), hash_new)
        result = cacher.get(make_key(mtime_ns=3000, size=100))
        assert result is not None
        assert result.digest == hash_new.digest
        cacher.close()

    def test_repr_shows_match_mtime_true(self, pg_conninfo):
        """__repr__ includes match_mtime=True by default."""
        cacher = PostgresHashCacher(pg_conninfo)
        assert "match_mtime=True" in repr(cacher)
        cacher.close()

    def test_repr_shows_match_mtime_false(self, pg_conninfo):
        """__repr__ includes match_mtime=False."""
        cacher = PostgresHashCacher(pg_conninfo, match_mtime=False)
        assert "match_mtime=False" in repr(cacher)
        cacher.close()
```

- [ ] **Step 2: Add `match_mtime` to `PostgresHashCacher`**

In `src/orcapod/hashing/postgres_hash_cacher.py`, update `PostgresHashCacher.__init__`:

```python
def __init__(
    self,
    conninfo: str,
    *,
    read_only: bool = False,
    min_cache_size_bytes: int | None = None,
    match_mtime: bool = True,
) -> None:
    if min_cache_size_bytes is not None and min_cache_size_bytes < 0:
        raise ValueError(
            f"min_cache_size_bytes must be None or a non-negative integer, "
            f"got {min_cache_size_bytes!r}"
        )
    self._conninfo = conninfo
    self._read_only = read_only
    self._min_cache_size_bytes = min_cache_size_bytes
    self._match_mtime = match_mtime
    self._local = threading.local()
    self._ensure_schema()
```

Replace `PostgresHashCacher.get()`:

```python
def get(self, key: FileHashKey) -> ContentHash | None:
    """Return the cached ``ContentHash`` for ``key``, or ``None`` on miss.

    When ``match_mtime=True`` (default), the query filters on
    ``path``, ``mtime_ns``, and ``size``. When ``match_mtime=False``,
    only ``path`` and ``size`` are filtered and results are ordered
    by ``mtime_ns DESC`` so the most recent entry is returned.

    Args:
        key: File hash cache key.

    Returns:
        Cached ``ContentHash``, or ``None`` if not found.
    """
    conn = self._connection()
    if self._match_mtime:
        row = conn.execute(
            "SELECT hash FROM file_hash_cache "
            "WHERE path=%s AND mtime_ns=%s AND size=%s",
            (str(key.path), key.mtime_ns, key.size),
        ).fetchone()
    else:
        row = conn.execute(
            "SELECT hash FROM file_hash_cache "
            "WHERE path=%s AND size=%s ORDER BY mtime_ns DESC LIMIT 1",
            (str(key.path), key.size),
        ).fetchone()
    if row is None:
        return None
    blob: bytes = bytes(row[0])
    method_bytes, digest = blob.split(b":", 1)
    return ContentHash(method=method_bytes.decode("ascii"), digest=digest)
```

Replace `PostgresHashCacher.__repr__`:

```python
def __repr__(self) -> str:
    return (
        f"PostgresHashCacher("
        f"conninfo={_redact_conninfo(self._conninfo)!r}, "
        f"read_only={self._read_only!r}, "
        f"min_cache_size_bytes={self._min_cache_size_bytes!r}, "
        f"match_mtime={self._match_mtime!r})"
    )
```

Also add `match_mtime` to the `PostgresHashCacher` class docstring Args section:

```
        match_mtime: When ``True`` (default), cache hits require
            ``path``, ``mtime_ns``, and ``size`` to all match. When
            ``False``, only ``path`` and ``size`` are compared; among
            multiple matching entries the one with the highest ``mtime_ns``
            is returned. The write path is unaffected — ``mtime_ns`` is
            always stored.
```

- [ ] **Step 3: Run non-postgres tests to verify no regressions**

```bash
uv run pytest tests/test_hashing/ -v --ignore=tests/test_hashing/test_postgres_hash_cacher.py
```

Expected: all pass (the postgres tests require `--postgres` flag and a Docker daemon)

- [ ] **Step 4: Commit**

```bash
git add src/orcapod/hashing/postgres_hash_cacher.py tests/test_hashing/test_postgres_hash_cacher.py
git commit -m "feat(hashing): add match_mtime flag to PostgresHashCacher (ITL-522)"
```

---

## Task 4: `CachedFileHasher` integration + `enable_file_hash_caching()`

**Files:**
- Modify: `tests/test_hashing/test_hash_cacher_match_mtime.py`
- Modify: `tests/test_hashing/test_hash_cachers.py`
- Modify: `src/orcapod/contexts/__init__.py`

- [ ] **Step 1: Add `CachedFileHasher` integration tests**

Append this class to `tests/test_hashing/test_hash_cacher_match_mtime.py`:

```python
# ---------------------------------------------------------------------------
# CachedFileHasher integration — match_mtime with real files
# ---------------------------------------------------------------------------


class TestCachedFileHasherMatchMtime:
    def test_t1_default_mtime_change_causes_miss(self, tmp_path):
        """T1: Default (match_mtime=True), mtime changed, size unchanged → cache miss."""
        f = tmp_path / "file.bin"
        f.write_bytes(b"x" * 50)

        cacher = InMemoryHashCacher(match_mtime=True)
        cached = CachedFileHasher(file_hasher=FileHasher(), cacher=cacher)
        cached.hash_file(f)

        stat = f.stat()
        os.utime(f, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1_000_000_000))

        path = UPath(f).resolve()
        new_stat = path.stat()
        new_key = FileHashKey(path, new_stat.st_mtime_ns, new_stat.st_size)
        assert cacher.get(new_key) is None

    def test_t2_match_mtime_false_mtime_change_is_hit(self, tmp_path):
        """T2: match_mtime=False, mtime changed, size unchanged → cache hit."""
        f = tmp_path / "file.bin"
        f.write_bytes(b"x" * 50)

        cacher = InMemoryHashCacher(match_mtime=False)
        cached = CachedFileHasher(file_hasher=FileHasher(), cacher=cacher)
        first_hash = cached.hash_file(f)

        stat = f.stat()
        os.utime(f, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1_000_000_000))

        second_hash = cached.hash_file(f)
        assert second_hash == first_hash

    def test_t3_match_mtime_false_size_change_is_miss(self, tmp_path):
        """T3: match_mtime=False, content and size changed → cache miss."""
        f = tmp_path / "file.bin"
        f.write_bytes(b"hello")  # 5 bytes

        cacher = InMemoryHashCacher(match_mtime=False)
        cached = CachedFileHasher(file_hasher=FileHasher(), cacher=cacher)
        first_hash = cached.hash_file(f)

        f.write_bytes(b"hello world")  # 11 bytes — different size

        second_hash = cached.hash_file(f)
        assert second_hash != first_hash

    def test_t4_match_mtime_false_same_size_content_flip_is_hit(self, tmp_path):
        """T4: match_mtime=False, content changed but size preserved → cache hit (known trade-off)."""
        f = tmp_path / "file.bin"
        f.write_bytes(b"aaaaa")  # 5 bytes

        cacher = InMemoryHashCacher(match_mtime=False)
        cached = CachedFileHasher(file_hasher=FileHasher(), cacher=cacher)
        first_hash = cached.hash_file(f)

        f.write_bytes(b"bbbbb")  # 5 bytes — same size, different content

        second_hash = cached.hash_file(f)
        assert second_hash == first_hash  # stale hit — known trade-off

    def test_t5_unchanged_file_both_modes_agree(self, tmp_path):
        """T5: Unchanged file — match_mtime=True and match_mtime=False return the same hash."""
        f = tmp_path / "file.bin"
        f.write_bytes(b"stable content")

        inner = FileHasher()
        hash_strict = CachedFileHasher(
            file_hasher=inner,
            cacher=InMemoryHashCacher(match_mtime=True),
        ).hash_file(f)
        hash_relaxed = CachedFileHasher(
            file_hasher=inner,
            cacher=InMemoryHashCacher(match_mtime=False),
        ).hash_file(f)
        assert hash_strict == hash_relaxed
```

- [ ] **Step 2: Run integration tests to verify they pass**

```bash
uv run pytest tests/test_hashing/test_hash_cacher_match_mtime.py::TestCachedFileHasherMatchMtime -v
```

Expected: all 5 tests PASS (these exercise already-implemented cacher code)

- [ ] **Step 3: Add `enable_file_hash_caching` test**

In `tests/test_hashing/test_hash_cachers.py`, inside the `TestEnableFileHashCaching` class, add this test method after `test_min_cache_size_bytes_kwarg_passes_through_to_cacher`:

```python
    def test_match_mtime_kwarg_passes_through_to_cacher(
        self, restore_default_file_handler, tmp_path
    ):
        from orcapod.contexts import enable_file_hash_caching, get_default_context
        from orcapod.extension_types.file_type import File
        from orcapod.hashing.hash_cachers import SqliteHashCacher

        enable_file_hash_caching(db_path=tmp_path / "cache.db", match_mtime=False)

        context = get_default_context()
        registry = context.semantic_hasher.type_handler_registry
        handler = registry.get_handler_for_type(File)
        cacher = handler.file_hasher.cacher

        assert isinstance(cacher, SqliteHashCacher)
        assert cacher._match_mtime is False
```

- [ ] **Step 4: Run to verify test fails**

```bash
uv run pytest tests/test_hashing/test_hash_cachers.py::TestEnableFileHashCaching::test_match_mtime_kwarg_passes_through_to_cacher -v
```

Expected: `TypeError: enable_file_hash_caching() got an unexpected keyword argument 'match_mtime'`

- [ ] **Step 5: Add `match_mtime` to `enable_file_hash_caching()`**

In `src/orcapod/contexts/__init__.py`, update the function signature:

```python
def enable_file_hash_caching(
    *,
    db_path: "Path | None" = None,
    conninfo: str | None = None,
    read_only: bool = False,
    min_cache_size_bytes: int | None = None,
    match_mtime: bool = True,
) -> None:
```

Update the docstring Args section to add:

```
        match_mtime: When ``False``, the cacher matches cache entries by
            ``path`` and ``size`` only, ignoring ``mtime_ns``. The entry
            with the highest ``mtime_ns`` is returned on multi-hit. Use in
            environments where mtime is unreliable (rsync, container
            remounts, restore-from-backup). Defaults to ``True`` (strict
            path + mtime_ns + size matching).
```

Update the `SqliteHashCacher` construction in the function body:

```python
    cacher = SqliteHashCacher(
        db_path,
        read_only=read_only,
        min_cache_size_bytes=min_cache_size_bytes,
        match_mtime=match_mtime,
    )
```

Update the `PostgresHashCacher` construction in the function body:

```python
        cacher = PostgresHashCacher(
            conninfo,
            read_only=read_only,
            min_cache_size_bytes=min_cache_size_bytes,
            match_mtime=match_mtime,
        )
```

- [ ] **Step 6: Run to verify test passes**

```bash
uv run pytest tests/test_hashing/test_hash_cachers.py::TestEnableFileHashCaching::test_match_mtime_kwarg_passes_through_to_cacher -v
```

Expected: PASS

- [ ] **Step 7: Run full hashing test suite**

```bash
uv run pytest tests/test_hashing/ -v --ignore=tests/test_hashing/test_postgres_hash_cacher.py
```

Expected: all pass

- [ ] **Step 8: Commit**

```bash
git add tests/test_hashing/test_hash_cacher_match_mtime.py tests/test_hashing/test_hash_cachers.py src/orcapod/contexts/__init__.py
git commit -m "feat(hashing): add match_mtime to enable_file_hash_caching; integration tests (ITL-522)"
```

---

## Task 5: Documentation

**Files:**
- Modify: `docs/concepts/file-hash-caching.md`

- [ ] **Step 1: Add the new section to the docs**

In `docs/concepts/file-hash-caching.md`, insert a new section **"Ignoring mtime in cache lookups"** immediately after the closing of the "Controlling when the cache is written" section (after the "Combining both" subsection, before "## Directory hashing (op.Directory)").

The section to insert:

```markdown
## Ignoring mtime in cache lookups

By default, a cache hit requires **path, mtime_ns, and size** to all match the stored
entry. In several common deployment scenarios, mtime is unreliable — it changes even
when file content has not — causing spurious cache misses and unnecessary re-hashing:

- **rsync / file transfer tools** — rsync and similar tools do not preserve mtime by
  default. Even with `--times`, sub-second precision is often lost on destination
  filesystems, producing a different `mtime_ns` for otherwise identical files.
- **Restore from backup** — backup and restore pipelines (tar, restic, Borg, cloud
  storage sync) frequently reset mtime to the restore timestamp rather than the
  original file timestamp.
- **Container bind mounts and volume remounts** — remounting a volume or restarting a
  container can reset or truncate mtime precision depending on the host filesystem and
  container runtime (Docker, Podman, Kubernetes).
- **CI `touch` / build system side-effects** — build scripts, test harnesses, and CI
  pipelines sometimes call `touch` on input files to force rebuilds, or copy files in
  ways that update mtime without changing content.
- **Network filesystems (NFS, CIFS/SMB)** — clock skew between the client and server,
  or coarse mtime granularity on older NFS versions, can produce stale or shifted
  timestamps that differ from the values recorded in the cache.

Set `match_mtime=False` to drop mtime from the lookup criterion. A cache hit then
requires only **path and size** to match:

```python
import orcapod as op

op.enable_file_hash_caching(match_mtime=False)
```

When multiple cache entries share the same path and size (recorded at different
mtimes), Orcapod returns the hash from the entry with the most recent `mtime_ns`.

**The write path is unchanged.** mtime is always recorded when a new entry is
inserted. Switching `match_mtime=False` on a cache that was already populated under
the default `match_mtime=True` still produces hits — no cache rebuild is needed.

### Known trade-off

With `match_mtime=False`, a file modification that preserves the file's byte count
will **not** be detected by the cache. The stored hash from the previous version will
be returned silently. This is rare in practice (most writes change file size), but
you should be aware of the trade-off before enabling this mode.

Use `match_mtime=False` only in environments where mtime changes are known to be
unreliable. For most local-disk or NFS deployments the default (`match_mtime=True`)
is the right choice.
```

- [ ] **Step 2: Verify the docs file renders correctly (spot-check)**

```bash
grep -n "Ignoring mtime" docs/concepts/file-hash-caching.md
```

Expected: line number printed, confirming the section was inserted.

- [ ] **Step 3: Run full test suite one final time**

```bash
uv run pytest tests/test_hashing/ -v --ignore=tests/test_hashing/test_postgres_hash_cacher.py
```

Expected: all pass

- [ ] **Step 4: Commit**

```bash
git add docs/concepts/file-hash-caching.md
git commit -m "docs(hashing): document match_mtime flag in file-hash-caching guide (ITL-522)"
```
