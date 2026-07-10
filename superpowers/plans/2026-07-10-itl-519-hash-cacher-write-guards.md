# Hash Cacher Write Guards (ITL-519) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `read_only` and `min_cache_size_bytes` write guards to `InMemoryHashCacher` and `SqliteHashCacher`, expose them through `enable_file_hash_caching()`, and document them.

**Architecture:** Both knobs live on the cachers and are enforced in `put()`. `get()` is unchanged. `CachedFileHasher` itself is not modified — it already calls `cacher.put()`, which the guard intercepts. `enable_file_hash_caching()` forwards the new kwargs directly to `SqliteHashCacher`.

**Tech Stack:** Python 3.12, `uv run pytest`, `sqlite3`, `upath`

**Spec:** `superpowers/specs/2026-07-10-itl-519-hash-cacher-write-guards-design.md`

**Branch:** `eywalker/itl-519-filehasher-cache-add-read-only-mode-minimum-file-size`

---

## File Map

| File | Action | Responsibility |
|------|--------|---------------|
| `src/orcapod/hashing/hash_cachers.py` | Modify | Add `read_only`, `min_cache_size_bytes`, `__repr__` to both cachers |
| `src/orcapod/contexts/__init__.py` | Modify | Expose new kwargs in `enable_file_hash_caching()` |
| `tests/test_hashing/test_hash_cacher_write_guards.py` | Create | All write-guard tests for both cachers + `CachedFileHasher` integration |
| `tests/test_hashing/test_hash_cachers.py` | Modify | Add `enable_file_hash_caching()` pass-through tests |
| `docs/concepts/file-hash-caching.md` | Modify | Document both knobs with activation snippets |

---

## Task 0: Check out the feature branch

**Files:** none

- [ ] **Step 0.1: Check out the branch**

```bash
cd orcapod-python
git checkout main
git pull
git checkout -b eywalker/itl-519-filehasher-cache-add-read-only-mode-minimum-file-size
git branch --show-current
```

Expected output: `eywalker/itl-519-filehasher-cache-add-read-only-mode-minimum-file-size`

---

## Task 1: Write failing tests for `InMemoryHashCacher` write guards

Write the tests before touching any implementation. They will fail because
`InMemoryHashCacher` does not yet accept `read_only` or `min_cache_size_bytes`.

**Files:**
- Create: `tests/test_hashing/test_hash_cacher_write_guards.py`

- [ ] **Step 1.1: Create the test file**

Create `tests/test_hashing/test_hash_cacher_write_guards.py` with this content:

```python
"""Tests for hash cacher write guards: read_only and min_cache_size_bytes.

Covers InMemoryHashCacher, SqliteHashCacher, CachedFileHasher integration,
and enable_file_hash_caching() pass-through — added by ITL-519.
"""

from __future__ import annotations

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
# InMemoryHashCacher — read_only
# ---------------------------------------------------------------------------


class TestInMemoryHashCacherReadOnly:
    def test_put_is_noop_when_read_only(self):
        cacher = InMemoryHashCacher(read_only=True)
        key = make_key()
        cacher.put(key, make_hash())
        assert cacher.get(key) is None

    def test_get_returns_none_on_empty_read_only_cacher(self):
        cacher = InMemoryHashCacher(read_only=True)
        assert cacher.get(make_key()) is None

    def test_put_works_when_not_read_only(self):
        cacher = InMemoryHashCacher(read_only=False)
        key = make_key()
        value = make_hash()
        cacher.put(key, value)
        assert cacher.get(key) == value

    def test_default_is_not_read_only(self):
        cacher = InMemoryHashCacher()
        key = make_key()
        value = make_hash()
        cacher.put(key, value)
        assert cacher.get(key) == value


# ---------------------------------------------------------------------------
# InMemoryHashCacher — min_cache_size_bytes
# ---------------------------------------------------------------------------


class TestInMemoryHashCacherThreshold:
    def test_small_file_not_cached(self):
        cacher = InMemoryHashCacher(min_cache_size_bytes=100)
        key = make_key(size=50)
        cacher.put(key, make_hash())
        assert cacher.get(key) is None

    def test_file_at_threshold_is_cached(self):
        # Boundary is inclusive: size >= threshold → stored
        cacher = InMemoryHashCacher(min_cache_size_bytes=100)
        key = make_key(size=100)
        value = make_hash()
        cacher.put(key, value)
        assert cacher.get(key) == value

    def test_large_file_is_cached(self):
        cacher = InMemoryHashCacher(min_cache_size_bytes=100)
        key = make_key(size=200)
        value = make_hash()
        cacher.put(key, value)
        assert cacher.get(key) == value

    def test_none_threshold_caches_all(self):
        cacher = InMemoryHashCacher(min_cache_size_bytes=None)
        key = make_key(size=1)
        value = make_hash()
        cacher.put(key, value)
        assert cacher.get(key) == value

    def test_zero_threshold_caches_all(self):
        # 0 is falsy → treated as "no threshold"
        cacher = InMemoryHashCacher(min_cache_size_bytes=0)
        key = make_key(size=1)
        value = make_hash()
        cacher.put(key, value)
        assert cacher.get(key) == value


# ---------------------------------------------------------------------------
# InMemoryHashCacher — combined
# ---------------------------------------------------------------------------


class TestInMemoryHashCacherCombined:
    def test_read_only_wins_over_large_file(self):
        cacher = InMemoryHashCacher(read_only=True, min_cache_size_bytes=100)
        key = make_key(size=9999)
        cacher.put(key, make_hash())
        assert cacher.get(key) is None

    def test_writable_large_file_is_cached(self):
        cacher = InMemoryHashCacher(read_only=False, min_cache_size_bytes=100)
        key = make_key(size=200)
        value = make_hash()
        cacher.put(key, value)
        assert cacher.get(key) == value

    def test_writable_small_file_not_cached(self):
        cacher = InMemoryHashCacher(read_only=False, min_cache_size_bytes=100)
        key = make_key(size=50)
        cacher.put(key, make_hash())
        assert cacher.get(key) is None


# ---------------------------------------------------------------------------
# InMemoryHashCacher — __repr__
# ---------------------------------------------------------------------------


class TestInMemoryHashCacherRepr:
    def test_repr_shows_read_only_false(self):
        cacher = InMemoryHashCacher()
        assert "read_only=False" in repr(cacher)

    def test_repr_shows_read_only_true(self):
        cacher = InMemoryHashCacher(read_only=True)
        assert "read_only=True" in repr(cacher)

    def test_repr_shows_min_cache_size_bytes_none(self):
        cacher = InMemoryHashCacher()
        assert "min_cache_size_bytes=None" in repr(cacher)

    def test_repr_shows_min_cache_size_bytes_value(self):
        cacher = InMemoryHashCacher(min_cache_size_bytes=1024)
        assert "min_cache_size_bytes=1024" in repr(cacher)


# ---------------------------------------------------------------------------
# SqliteHashCacher — read_only
# ---------------------------------------------------------------------------


class TestSqliteHashCacherReadOnly:
    def test_put_is_noop_when_read_only(self, tmp_path):
        cacher = SqliteHashCacher(tmp_path / "cache.db", read_only=True)
        key = make_key()
        cacher.put(key, make_hash())
        assert cacher.get(key) is None

    def test_read_only_cacher_can_read_preexisting_entries(self, tmp_path):
        """A read-only SqliteHashCacher can still read entries written by a writable instance."""
        db = tmp_path / "cache.db"
        key = make_key()
        value = make_hash()
        # Pre-populate with a writable cacher
        writable = SqliteHashCacher(db)
        writable.put(key, value)
        writable.close()
        # Read-only cacher must see the pre-existing entry
        read_only = SqliteHashCacher(db, read_only=True)
        result = read_only.get(key)
        assert result is not None
        assert result.digest == value.digest

    def test_put_works_when_not_read_only(self, tmp_path):
        cacher = SqliteHashCacher(tmp_path / "cache.db", read_only=False)
        key = make_key()
        value = make_hash()
        cacher.put(key, value)
        assert cacher.get(key) is not None

    def test_default_is_not_read_only(self, tmp_path):
        cacher = SqliteHashCacher(tmp_path / "cache.db")
        key = make_key()
        value = make_hash()
        cacher.put(key, value)
        assert cacher.get(key) is not None


# ---------------------------------------------------------------------------
# SqliteHashCacher — min_cache_size_bytes
# ---------------------------------------------------------------------------


class TestSqliteHashCacherThreshold:
    def test_small_file_not_cached(self, tmp_path):
        cacher = SqliteHashCacher(tmp_path / "cache.db", min_cache_size_bytes=100)
        key = make_key(size=50)
        cacher.put(key, make_hash())
        assert cacher.get(key) is None

    def test_file_at_threshold_is_cached(self, tmp_path):
        cacher = SqliteHashCacher(tmp_path / "cache.db", min_cache_size_bytes=100)
        key = make_key(size=100)
        value = make_hash()
        cacher.put(key, value)
        result = cacher.get(key)
        assert result is not None
        assert result.digest == value.digest

    def test_large_file_is_cached(self, tmp_path):
        cacher = SqliteHashCacher(tmp_path / "cache.db", min_cache_size_bytes=100)
        key = make_key(size=200)
        value = make_hash()
        cacher.put(key, value)
        assert cacher.get(key) is not None

    def test_none_threshold_caches_all(self, tmp_path):
        cacher = SqliteHashCacher(tmp_path / "cache.db", min_cache_size_bytes=None)
        key = make_key(size=1)
        value = make_hash()
        cacher.put(key, value)
        assert cacher.get(key) is not None

    def test_zero_threshold_caches_all(self, tmp_path):
        cacher = SqliteHashCacher(tmp_path / "cache.db", min_cache_size_bytes=0)
        key = make_key(size=1)
        value = make_hash()
        cacher.put(key, value)
        assert cacher.get(key) is not None


# ---------------------------------------------------------------------------
# SqliteHashCacher — combined
# ---------------------------------------------------------------------------


class TestSqliteHashCacherCombined:
    def test_read_only_wins_over_large_file(self, tmp_path):
        cacher = SqliteHashCacher(
            tmp_path / "cache.db", read_only=True, min_cache_size_bytes=100
        )
        key = make_key(size=9999)
        cacher.put(key, make_hash())
        assert cacher.get(key) is None

    def test_writable_large_file_is_cached(self, tmp_path):
        cacher = SqliteHashCacher(
            tmp_path / "cache.db", read_only=False, min_cache_size_bytes=100
        )
        key = make_key(size=200)
        value = make_hash()
        cacher.put(key, value)
        assert cacher.get(key) is not None

    def test_writable_small_file_not_cached(self, tmp_path):
        cacher = SqliteHashCacher(
            tmp_path / "cache.db", read_only=False, min_cache_size_bytes=100
        )
        key = make_key(size=50)
        cacher.put(key, make_hash())
        assert cacher.get(key) is None


# ---------------------------------------------------------------------------
# SqliteHashCacher — __repr__
# ---------------------------------------------------------------------------


class TestSqliteHashCacherRepr:
    def test_repr_includes_db_path(self, tmp_path):
        db = tmp_path / "cache.db"
        cacher = SqliteHashCacher(db)
        assert str(db) in repr(cacher)

    def test_repr_shows_read_only_false(self, tmp_path):
        cacher = SqliteHashCacher(tmp_path / "cache.db")
        assert "read_only=False" in repr(cacher)

    def test_repr_shows_read_only_true(self, tmp_path):
        cacher = SqliteHashCacher(tmp_path / "cache.db", read_only=True)
        assert "read_only=True" in repr(cacher)

    def test_repr_shows_min_cache_size_bytes(self, tmp_path):
        cacher = SqliteHashCacher(tmp_path / "cache.db", min_cache_size_bytes=1024)
        assert "min_cache_size_bytes=1024" in repr(cacher)


# ---------------------------------------------------------------------------
# CachedFileHasher integration — read_only cacher (real files)
# ---------------------------------------------------------------------------


class TestCachedFileHasherWithReadOnlyCacher:
    def test_hash_file_returns_correct_hash_despite_read_only(self, tmp_path):
        """hash_file() computes and returns the hash even when the cacher is read-only."""
        f = tmp_path / "file.txt"
        f.write_text("hello world")

        inner = FileHasher(algorithm="sha256")
        cacher = InMemoryHashCacher(read_only=True)
        cached = CachedFileHasher(file_hasher=inner, cacher=cacher)

        result = cached.hash_file(f)
        expected = inner.hash_file(f)

        assert result.method == expected.method
        assert result.digest == expected.digest

    def test_hash_file_does_not_store_result_in_read_only_cacher(self, tmp_path):
        """hash_file() returns the correct hash but stores nothing in the cacher."""
        f = tmp_path / "file.txt"
        f.write_text("hello world")

        inner = FileHasher(algorithm="sha256")
        cacher = InMemoryHashCacher(read_only=True)
        cached = CachedFileHasher(file_hasher=inner, cacher=cacher)

        cached.hash_file(f)

        path = UPath(f).resolve()
        stat = path.stat()
        key = FileHashKey(path, stat.st_mtime_ns, stat.st_size)
        assert cacher.get(key) is None


# ---------------------------------------------------------------------------
# CachedFileHasher integration — threshold cacher (real files)
# ---------------------------------------------------------------------------


class TestCachedFileHasherWithThresholdCacher:
    def test_small_file_hashed_and_returned_but_not_stored(self, tmp_path):
        """Files below the threshold: hash is computed and returned, but not stored."""
        f = tmp_path / "small.bin"
        f.write_bytes(b"x" * 10)  # 10 bytes — below threshold of 100

        inner = FileHasher(algorithm="sha256")
        cacher = InMemoryHashCacher(min_cache_size_bytes=100)
        cached = CachedFileHasher(file_hasher=inner, cacher=cacher)

        result = cached.hash_file(f)
        expected = inner.hash_file(f)

        assert result.digest == expected.digest  # correct hash returned

        path = UPath(f).resolve()
        stat = path.stat()
        key = FileHashKey(path, stat.st_mtime_ns, stat.st_size)
        assert cacher.get(key) is None  # nothing stored

    def test_large_file_hashed_and_stored(self, tmp_path):
        """Files at or above the threshold: hash is computed, returned, and stored."""
        f = tmp_path / "large.bin"
        f.write_bytes(b"x" * 200)  # 200 bytes — at/above threshold of 100

        inner = FileHasher(algorithm="sha256")
        cacher = InMemoryHashCacher(min_cache_size_bytes=100)
        cached = CachedFileHasher(file_hasher=inner, cacher=cacher)

        result = cached.hash_file(f)

        path = UPath(f).resolve()
        stat = path.stat()
        key = FileHashKey(path, stat.st_mtime_ns, stat.st_size)
        stored = cacher.get(key)
        assert stored is not None
        assert stored.digest == result.digest
```

- [ ] **Step 1.2: Run the new tests to confirm they all fail**

```bash
uv run pytest tests/test_hashing/test_hash_cacher_write_guards.py -v 2>&1 | head -60
```

Expected: Many `TypeError: __init__() got an unexpected keyword argument 'read_only'` errors. Every test should fail — if any pass, investigate before continuing.

---

## Task 2: Implement `InMemoryHashCacher` write guards + `__repr__`

**Files:**
- Modify: `src/orcapod/hashing/hash_cachers.py`

- [ ] **Step 2.1: Update `InMemoryHashCacher`**

Open `src/orcapod/hashing/hash_cachers.py`. Replace the entire `InMemoryHashCacher` class with:

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
            Defaults to ``None``.
    """

    def __init__(
        self,
        *,
        read_only: bool = False,
        min_cache_size_bytes: int | None = None,
    ) -> None:
        self._cache: dict[FileHashKey, ContentHash] = {}
        self._read_only = read_only
        self._min_cache_size_bytes = min_cache_size_bytes

    def get(self, key: FileHashKey) -> ContentHash | None:
        """Return the cached ``ContentHash`` for ``key``, or ``None`` on miss.

        Args:
            key: File hash cache key.

        Returns:
            Cached ``ContentHash``, or ``None`` if not found.
        """
        return self._cache.get(key)

    def put(self, key: FileHashKey, value: ContentHash) -> None:
        """Store ``value`` under ``key``.

        No-ops silently when ``read_only=True`` or when ``key.size`` is below
        ``min_cache_size_bytes``.

        Args:
            key: File hash cache key.
            value: ``ContentHash`` to store.
        """
        if self._read_only:
            return
        if self._min_cache_size_bytes and key.size < self._min_cache_size_bytes:
            return
        self._cache[key] = value

    def clear(self) -> None:
        """Remove all entries from the cache."""
        self._cache.clear()

    def __repr__(self) -> str:
        return (
            f"InMemoryHashCacher("
            f"read_only={self._read_only!r}, "
            f"min_cache_size_bytes={self._min_cache_size_bytes!r})"
        )
```

- [ ] **Step 2.2: Run only the `InMemoryHashCacher` tests**

```bash
uv run pytest tests/test_hashing/test_hash_cacher_write_guards.py -k "InMemory" -v
```

Expected: All `TestInMemoryHashCacher*` tests pass. The `TestSqliteHashCacher*` and `TestCachedFileHasher*` tests still fail — that is expected at this point.

- [ ] **Step 2.3: Run existing InMemoryHashCacher tests to confirm no regression**

```bash
uv run pytest tests/test_hashing/test_hash_cachers.py::TestInMemoryHashCacher -v
```

Expected: All existing tests pass.

- [ ] **Step 2.4: Commit**

```bash
git add src/orcapod/hashing/hash_cachers.py tests/test_hashing/test_hash_cacher_write_guards.py
git commit -m "feat(hashing): add read_only and min_cache_size_bytes guards to InMemoryHashCacher"
```

---

## Task 3: Implement `SqliteHashCacher` write guards + `__repr__`

**Files:**
- Modify: `src/orcapod/hashing/hash_cachers.py`

- [ ] **Step 3.1: Update `SqliteHashCacher.__init__`**

In `src/orcapod/hashing/hash_cachers.py`, replace the `SqliteHashCacher.__init__` signature and body with:

```python
    def __init__(
        self,
        db_path: Path | None = None,
        *,
        read_only: bool = False,
        min_cache_size_bytes: int | None = None,
    ) -> None:
        self.db_path = Path(
            db_path
            or os.environ.get("ORCAPOD_HASH_CACHE_DB")
            or self.DEFAULT_DB_PATH
        )
        self._read_only = read_only
        self._min_cache_size_bytes = min_cache_size_bytes
        self._local = threading.local()
        self._ensure_schema()
```

- [ ] **Step 3.2: Update `SqliteHashCacher.put`**

Replace the existing `put` method with:

```python
    def put(self, key: FileHashKey, value: ContentHash) -> None:
        """Store ``value`` under ``key``.

        No-ops silently when ``read_only=True`` or when ``key.size`` is below
        ``min_cache_size_bytes``. Uses ``INSERT OR REPLACE`` so writes are
        idempotent when they do proceed.

        Args:
            key: File hash cache key.
            value: ``ContentHash`` to store.
        """
        if self._read_only:
            return
        if self._min_cache_size_bytes and key.size < self._min_cache_size_bytes:
            return
        conn = self._connection()
        conn.execute(
            """
            INSERT OR REPLACE INTO file_hash_cache (path, mtime_ns, size, hash)
            VALUES (?, ?, ?, ?)
            """,
            (str(key.path), key.mtime_ns, key.size, value.to_prefixed_digest()),
        )
        conn.commit()
```

- [ ] **Step 3.3: Add `__repr__` to `SqliteHashCacher`**

Add this method to `SqliteHashCacher` (before `__enter__`):

```python
    def __repr__(self) -> str:
        return (
            f"SqliteHashCacher("
            f"db_path={str(self.db_path)!r}, "
            f"read_only={self._read_only!r}, "
            f"min_cache_size_bytes={self._min_cache_size_bytes!r})"
        )
```

Also update the class docstring to document the new args. Replace the existing docstring with:

```python
    """SQLite-backed file hash cacher.

    Stores file hashes keyed on ``(path, mtime_ns, size)`` in a local
    SQLite database. Uses WAL mode for single-writer/multi-reader
    concurrency and thread-local connections for thread safety.

    The hash is stored as a BLOB in ``{method}:{raw_digest}`` format via
    ``ContentHash.to_prefixed_digest()``.

    Args:
        db_path: Path to the SQLite database file. Defaults to
            ``~/.orcapod/file_hash_cache.db`` or the
            ``ORCAPOD_HASH_CACHE_DB`` environment variable.
        read_only: When ``True``, all ``put()`` calls are silent no-ops.
            ``get()`` still works normally. Defaults to ``False``.
        min_cache_size_bytes: When set to a positive integer, files whose
            ``key.size`` is strictly below this threshold are not inserted.
            ``None`` and ``0`` disable the threshold (default behaviour).
            Defaults to ``None``.

    Note:
        Heavy multi-writer scenarios are a known SQLite limitation. A Turso
        / libSQL migration is planned as a follow-up issue.
    """
```

- [ ] **Step 3.4: Run all write-guard tests**

```bash
uv run pytest tests/test_hashing/test_hash_cacher_write_guards.py -v
```

Expected: All tests pass.

- [ ] **Step 3.5: Run existing SqliteHashCacher tests to confirm no regression**

```bash
uv run pytest tests/test_hashing/test_hash_cachers.py::TestSqliteHashCacher -v
```

Expected: All existing tests pass.

- [ ] **Step 3.6: Commit**

```bash
git add src/orcapod/hashing/hash_cachers.py
git commit -m "feat(hashing): add read_only and min_cache_size_bytes guards to SqliteHashCacher"
```

---

## Task 4: Add `enable_file_hash_caching()` pass-through tests and implementation

**Files:**
- Modify: `tests/test_hashing/test_hash_cachers.py`
- Modify: `src/orcapod/contexts/__init__.py`

- [ ] **Step 4.1: Add tests to `TestEnableFileHashCaching`**

Open `tests/test_hashing/test_hash_cachers.py`. Inside the `TestEnableFileHashCaching` class, add these two methods at the end:

```python
    def test_read_only_kwarg_passes_through_to_cacher(
        self, restore_default_file_handler, tmp_path
    ):
        from orcapod.contexts import enable_file_hash_caching, get_default_context
        from orcapod.extension_types.file_type import File
        from orcapod.hashing.hash_cachers import SqliteHashCacher

        enable_file_hash_caching(db_path=tmp_path / "cache.db", read_only=True)

        context = get_default_context()
        registry = context.semantic_hasher.type_handler_registry
        handler = registry.get_handler_for_type(File)
        cacher = handler.file_hasher.cacher

        assert isinstance(cacher, SqliteHashCacher)
        assert cacher._read_only is True

    def test_min_cache_size_bytes_kwarg_passes_through_to_cacher(
        self, restore_default_file_handler, tmp_path
    ):
        from orcapod.contexts import enable_file_hash_caching, get_default_context
        from orcapod.extension_types.file_type import File
        from orcapod.hashing.hash_cachers import SqliteHashCacher

        enable_file_hash_caching(
            db_path=tmp_path / "cache.db", min_cache_size_bytes=1024
        )

        context = get_default_context()
        registry = context.semantic_hasher.type_handler_registry
        handler = registry.get_handler_for_type(File)
        cacher = handler.file_hasher.cacher

        assert isinstance(cacher, SqliteHashCacher)
        assert cacher._min_cache_size_bytes == 1024
```

- [ ] **Step 4.2: Run the new tests to confirm they fail**

```bash
uv run pytest tests/test_hashing/test_hash_cachers.py::TestEnableFileHashCaching::test_read_only_kwarg_passes_through_to_cacher tests/test_hashing/test_hash_cachers.py::TestEnableFileHashCaching::test_min_cache_size_bytes_kwarg_passes_through_to_cacher -v
```

Expected: Both tests fail with `TypeError: enable_file_hash_caching() got an unexpected keyword argument 'read_only'`.

- [ ] **Step 4.3: Update `enable_file_hash_caching()` signature and body**

Open `src/orcapod/contexts/__init__.py`. Replace the function signature line:

```python
def enable_file_hash_caching(db_path: "Path | None" = None) -> None:
```

with:

```python
def enable_file_hash_caching(
    db_path: "Path | None" = None,
    read_only: bool = False,
    min_cache_size_bytes: int | None = None,
) -> None:
```

Also update the docstring to document the new args. After the `db_path` arg in the `Args:` section, add:

```
        read_only: When ``True``, the underlying ``SqliteHashCacher`` will
            not insert new entries. Lookups still work normally. Defaults
            to ``False``.
        min_cache_size_bytes: When set, files smaller than this byte count
            are not inserted into the cache. ``None`` and ``0`` disable the
            threshold. Defaults to ``None``.
```

Then find the `SqliteHashCacher(db_path)` call inside the function body and replace it with:

```python
    cached_file_hasher = CachedFileHasher(
        file_hasher=base_hasher,
        cacher=SqliteHashCacher(
            db_path,
            read_only=read_only,
            min_cache_size_bytes=min_cache_size_bytes,
        ),
    )
```

- [ ] **Step 4.4: Run the pass-through tests**

```bash
uv run pytest tests/test_hashing/test_hash_cachers.py::TestEnableFileHashCaching -v
```

Expected: All `TestEnableFileHashCaching` tests pass, including the two new ones.

- [ ] **Step 4.5: Run the full test suite for hash cachers**

```bash
uv run pytest tests/test_hashing/ -v
```

Expected: All tests pass.

- [ ] **Step 4.6: Commit**

```bash
git add tests/test_hashing/test_hash_cachers.py src/orcapod/contexts/__init__.py
git commit -m "feat(contexts): expose read_only and min_cache_size_bytes in enable_file_hash_caching"
```

---

## Task 5: Update documentation

**Files:**
- Modify: `docs/concepts/file-hash-caching.md`

- [ ] **Step 5.1: Add the "Controlling when the cache is written" section**

Open `docs/concepts/file-hash-caching.md`. Find the line:

```markdown
## Directory hashing (op.Directory)
```

Insert the following new section immediately before it (with a blank line above):

```markdown
## Controlling when the cache is written

By default, every file that passes through ``CachedFileHasher`` is inserted
into the cache on a miss. Two optional knobs let you restrict this.

### Read-only mode

Use ``read_only=True`` when you want lookups from a shared or authoritative
cache but must not add new entries to it — for example, when consuming a
cache pre-populated by ``populate_hash_cache()`` without polluting it with
ad-hoc entries.

```python
import orcapod as op

op.enable_file_hash_caching(db_path="/shared/cache.db", read_only=True)
```

Cache hits still work normally. On a miss, the file is hashed directly and
the result is returned to the caller — but it is never written to the cache.

### Minimum file size threshold

Use ``min_cache_size_bytes`` to skip the cache write overhead for small
files. For small files, the disk I/O bottleneck does not apply, so the
cache lookup and write add latency without meaningful savings.

```python
import orcapod as op

# Skip caching for files smaller than 1 MB
op.enable_file_hash_caching(min_cache_size_bytes=1_048_576)
```

Files smaller than the threshold are still hashed and the hash is returned
to the caller — they are simply not inserted into the cache. Files at or
above the threshold behave normally. Set to ``None`` or ``0`` to disable
the threshold (the default).

### Combining both

The two knobs compose independently. ``read_only=True`` takes precedence:
when enabled, no entry is ever written regardless of file size.
``min_cache_size_bytes`` is an additional guard that applies only when the
cacher is writable.

```python
import orcapod as op

# Read-only + skip files below 512 KB (threshold is moot when read-only,
# but harmless and documents intent)
op.enable_file_hash_caching(
    db_path="/shared/cache.db",
    read_only=True,
    min_cache_size_bytes=524_288,
)
```

```

- [ ] **Step 5.2: Update the "When caching helps" section**

In the same file, find the bullet that begins:

```markdown
- Files are small.
```

Replace it with:

```markdown
- Files are small. Disk I/O is not the bottleneck for small files, so the
  cache lookup adds overhead without meaningful savings. Use
  ``min_cache_size_bytes`` to skip caching small files automatically.
```

If the bullet does not already exist, add it to the "Caching does not help much when" list.

- [ ] **Step 5.3: Verify the docs file reads cleanly**

```bash
cat docs/concepts/file-hash-caching.md
```

Scan for any broken Markdown (unclosed code fences, stray backticks). Fix any formatting issues.

- [ ] **Step 5.4: Commit**

```bash
git add docs/concepts/file-hash-caching.md
git commit -m "docs(hashing): document read_only and min_cache_size_bytes knobs"
```

---

## Task 6: Final check — run the complete hashing test suite

- [ ] **Step 6.1: Run all hashing tests**

```bash
uv run pytest tests/test_hashing/ -v
```

Expected: All tests pass with no warnings about unexpected kwargs or missing attributes.

- [ ] **Step 6.2: Push the branch**

```bash
git push -u origin eywalker/itl-519-filehasher-cache-add-read-only-mode-minimum-file-size
```

Expected: Branch pushed, no errors.
