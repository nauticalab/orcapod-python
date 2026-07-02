# File Hash Caching Implementation Plan (ITL-472)

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `CacherProtocol[K, V]`, `FileHashKey`, `SqliteHashCacher`, and `enable_file_hash_caching()` so Orcapod can cache file hashes in SQLite and reduce repeat hashing to a sub-millisecond lookup.

**Architecture:** `CacherProtocol[K, V]` is a generic two-method protocol (`get`/`put`). `CachedFileHasher` wraps any `FileContentHasherProtocol` with any `CacherProtocol[FileHashKey, ContentHash]`. `SqliteHashCacher` is the production backend using stdlib `sqlite3` with WAL mode and a purpose-built schema. `enable_file_hash_caching()` wires caching into the default context by re-registering the live `FileHandler` with a `CachedFileHasher`.

**Tech Stack:** Python stdlib `sqlite3`, `upath.UPath`, `threading.local`, `pytest`, `dataclasses`.

---

## File Map

| Action | File | What changes |
|---|---|---|
| Modify | `src/orcapod/protocols/hashing_protocols.py` | Add `CacherProtocol[K, V]` |
| Modify | `src/orcapod/hashing/file_hashers.py` | Add `FileHashKey`; rename `BasicFileHasher` → `FileHasher`; update `CachedFileHasher` to use `CacherProtocol` + UPath |
| **Create** | `src/orcapod/hashing/hash_cachers.py` | `InMemoryHashCacher`, `SqliteHashCacher` |
| Modify | `src/orcapod/hashing/__init__.py` | Add new exports, remove `BasicFileHasher` |
| Modify | `src/orcapod/hashing/semantic_hashing/builtin_handlers.py` | Update fallback import `BasicFileHasher` → `FileHasher` |
| Modify | `src/orcapod/contexts/data/v0.1.json` | Update `_class` to `FileHasher` |
| Modify | `src/orcapod/contexts/__init__.py` | Add `enable_file_hash_caching()` |
| Modify | `tests/test_hashing/test_file_hashers.py` | Rename `BasicFileHasher` → `FileHasher`; update `CachedFileHasher` tests |
| **Create** | `tests/test_hashing/test_hash_cachers.py` | `InMemoryHashCacher`, `SqliteHashCacher`, `enable_file_hash_caching()` tests |
| **Create** | `bench/bench_file_hasher_cache.py` | Timing benchmark |

---

## Task 1: Add `CacherProtocol[K, V]`

**Files:**
- Modify: `src/orcapod/protocols/hashing_protocols.py`

- [ ] **Step 1: Add TypeVars and `CacherProtocol` to `hashing_protocols.py`**

Open `src/orcapod/protocols/hashing_protocols.py`. At the top of the file, `from __future__ import annotations` may or may not be present — check first. Add the TypeVars directly before the `StringCacherProtocol` class, and add `CacherProtocol` right after it:

```python
# Add near the top imports:
from typing import TypeVar

# ... existing code ...

# Add before StringCacherProtocol:
K = TypeVar("K")
V = TypeVar("V")


class CacherProtocol(Protocol[K, V]):
    """Generic get/put caching protocol.

    A two-method protocol for caches keyed and valued by arbitrary types.
    Use typed specializations (e.g. ``CacherProtocol[FileHashKey, ContentHash]``)
    to constrain implementations.

    Type Parameters:
        K: The cache key type.
        V: The cached value type.
    """

    def get(self, key: K) -> V | None:
        """Return the cached value for ``key``, or ``None`` on miss."""
        ...

    def put(self, key: K, value: V) -> None:
        """Store ``value`` under ``key``."""
        ...
```

- [ ] **Step 2: Verify it imports cleanly**

```bash
cd /home/kurouto/kurouto-jobs/e01df523-d89f-44e2-8417-5a50eb2a506a/orcapod-python
uv run python -c "from orcapod.protocols.hashing_protocols import CacherProtocol; print('ok')"
```

Expected output: `ok`

- [ ] **Step 3: Commit**

```bash
git add src/orcapod/protocols/hashing_protocols.py
git commit -m "feat(hashing): add generic CacherProtocol[K, V] to hashing_protocols"
```

---

## Task 2: Add `FileHashKey` and rename `BasicFileHasher` → `FileHasher`

**Files:**
- Modify: `src/orcapod/hashing/file_hashers.py`
- Modify: `src/orcapod/hashing/__init__.py`
- Modify: `src/orcapod/hashing/semantic_hashing/builtin_handlers.py`
- Modify: `src/orcapod/contexts/data/v0.1.json`
- Modify: `tests/test_hashing/test_file_hashers.py`
- Modify: `tests/test_hashing/test_file_handler.py`

- [ ] **Step 1: Rewrite `src/orcapod/hashing/file_hashers.py`**

Replace the entire file contents with:

```python
import os
from dataclasses import dataclass
from pathlib import Path

from upath import UPath

from orcapod.hashing.hash_utils import hash_file
from orcapod.protocols.hashing_protocols import (
    CacherProtocol,
    FileContentHasherProtocol,
)
from orcapod.types import ContentHash, PathLike


@dataclass(frozen=True)
class FileHashKey:
    """Cache lookup key for a file hash.

    The key captures the three file attributes that together identify a
    file's content without reading it: its absolute path, last-modified
    time in nanoseconds, and byte size.

    Attributes:
        path: Absolute, resolved ``UPath``. Any ``PathLike`` input is
            normalised to ``UPath`` before constructing this key.
        mtime_ns: Last-modified time in nanoseconds (``os.stat().st_mtime_ns``).
        size: File size in bytes (``os.stat().st_size``).
    """

    path: UPath
    mtime_ns: int
    size: int


class FileHasher:
    """Hash file content using a configurable algorithm.

    Args:
        algorithm: Hashing algorithm to use. Defaults to ``"sha256"``.
        buffer_size: Read buffer size in bytes. Defaults to 65536.
    """

    def __init__(
        self,
        algorithm: str = "sha256",
        buffer_size: int = 65536,
    ):
        self.algorithm = algorithm
        self.buffer_size = buffer_size

    def hash_file(self, file_path: PathLike) -> ContentHash:
        """Hash the file at ``file_path`` and return its ``ContentHash``.

        Args:
            file_path: Path to the file to hash.

        Returns:
            ContentHash of the file's content.
        """
        return hash_file(
            file_path, algorithm=self.algorithm, buffer_size=self.buffer_size
        )


class CachedFileHasher:
    """File hasher that caches results to avoid redundant I/O.

    Wraps any ``FileContentHasherProtocol`` with a
    ``CacherProtocol[FileHashKey, ContentHash]``. On each call to
    ``hash_file``:

    1. Resolve the path to an absolute ``UPath`` and stat the file.
    2. Look up ``(path, mtime_ns, size)`` in the cacher.
    3. On hit: return the cached ``ContentHash`` directly.
    4. On miss: delegate to the inner hasher, store the result, return it.

    Both ``FileHasher`` and ``CachedFileHasher`` satisfy
    ``FileContentHasherProtocol`` — callers do not need to know which they
    have.

    Args:
        file_hasher: Inner hasher that performs the actual content hashing.
        cacher: Cache backend implementing
            ``CacherProtocol[FileHashKey, ContentHash]``.
    """

    def __init__(
        self,
        file_hasher: FileContentHasherProtocol,
        cacher: "CacherProtocol[FileHashKey, ContentHash]",
    ) -> None:
        self.file_hasher = file_hasher
        self.cacher = cacher

    def hash_file(self, file_path: PathLike) -> ContentHash:
        """Return the ``ContentHash`` for ``file_path``, using the cache.

        Args:
            file_path: Path to the file to hash.

        Returns:
            ContentHash of the file's content (from cache or computed).
        """
        path = file_path if isinstance(file_path, UPath) else UPath(file_path)
        path = path.resolve()
        stat = path.stat()
        key = FileHashKey(path, stat.st_mtime_ns, stat.st_size)

        hit = self.cacher.get(key)
        if hit is not None:
            return hit

        result = self.file_hasher.hash_file(file_path)
        self.cacher.put(key, result)
        return result
```

- [ ] **Step 2: Update `src/orcapod/hashing/__init__.py`**

Find and replace these lines:

```python
# OLD:
from orcapod.hashing.file_hashers import BasicFileHasher, CachedFileHasher
```
```python
# NEW:
from orcapod.hashing.file_hashers import CachedFileHasher, FileHasher, FileHashKey
```

In the `__all__` list, replace `"BasicFileHasher"` with `"FileHasher"` and add `"FileHashKey"`:

```python
# OLD:
    "BasicFileHasher",
    "CachedFileHasher",
```
```python
# NEW:
    "FileHasher",
    "FileHashKey",
    "CachedFileHasher",
```

- [ ] **Step 3: Update `src/orcapod/hashing/semantic_hashing/builtin_handlers.py`**

Find lines 453–454 (the fallback import inside `register_builtin_python_type_handlers`):

```python
# OLD:
        from orcapod.hashing.file_hashers import BasicFileHasher
        file_hasher = BasicFileHasher(algorithm="sha256")
```
```python
# NEW:
        from orcapod.hashing.file_hashers import FileHasher
        file_hasher = FileHasher(algorithm="sha256")
```

Also update the docstring on line 444 from `BasicFileHasher(sha256)` to `FileHasher(sha256)`.

- [ ] **Step 4: Update `src/orcapod/contexts/data/v0.1.json`**

```json
// OLD:
"_class": "orcapod.hashing.file_hashers.BasicFileHasher",
```
```json
// NEW:
"_class": "orcapod.hashing.file_hashers.FileHasher",
```

- [ ] **Step 5: Update `tests/test_hashing/test_file_hashers.py`**

Replace the import line:
```python
# OLD:
from orcapod.hashing.file_hashers import BasicFileHasher, CachedFileHasher
from orcapod.hashing.string_cachers import InMemoryCacher
```
```python
# NEW:
from orcapod.hashing.file_hashers import CachedFileHasher, FileHasher, FileHashKey
from orcapod.hashing.hash_cachers import InMemoryHashCacher
```

Replace every occurrence of `BasicFileHasher` with `FileHasher` (class name, fixture, docstrings, comments).

Replace every occurrence of `string_cacher=cacher` with `cacher=cacher` and `InMemoryCacher()` with `InMemoryHashCacher()`.

Replace the fixture:
```python
# OLD:
@pytest.fixture
def file_hasher():
    return BasicFileHasher(algorithm="sha256")

@pytest.fixture
def cached_file_hasher(file_hasher):
    cacher = InMemoryCacher()
    return CachedFileHasher(file_hasher=file_hasher, string_cacher=cacher)
```
```python
# NEW:
@pytest.fixture
def file_hasher():
    return FileHasher(algorithm="sha256")

@pytest.fixture
def cached_file_hasher(file_hasher):
    cacher = InMemoryHashCacher()
    return CachedFileHasher(file_hasher=file_hasher, cacher=cacher)
```

Replace `TestBasicFileHasherReturnType` class name with `TestFileHasherReturnType` and update its docstring.

Replace `test_cache_stores_to_string_format` with a version that inspects the typed cache directly:

```python
def test_cache_stores_content_hash_directly(sample_file):
    """The cacher stores ContentHash objects keyed by FileHashKey."""
    from upath import UPath
    inner = FileHasher(algorithm="sha256")
    cacher = InMemoryHashCacher()
    cached = CachedFileHasher(file_hasher=inner, cacher=cacher)

    result = cached.hash_file(sample_file)

    path = UPath(sample_file).resolve()
    stat = path.stat()
    key = FileHashKey(path, stat.st_mtime_ns, stat.st_size)
    cached_value = cacher.get(key)

    assert cached_value is not None
    assert cached_value == result
    assert cached_value.method == "sha256"
    assert isinstance(cached_value.digest, bytes)
```

Update `test_different_algorithms_share_cache_key`:
```python
def test_different_algorithms_share_cache_key(tmp_path):
    """Two CachedFileHashers with different algorithms but same path+mtime+size
    share the same cache entry — second gets the first's result.

    This is a known limitation: the cache key does not include the algorithm.
    """
    f = tmp_path / "file.txt"
    f.write_text("test content")

    cacher = InMemoryHashCacher()

    sha_inner = FileHasher(algorithm="sha256")
    sha_cached = CachedFileHasher(file_hasher=sha_inner, cacher=cacher)
    sha_result = sha_cached.hash_file(f)

    md5_inner = FileHasher(algorithm="md5")
    md5_cached = CachedFileHasher(file_hasher=md5_inner, cacher=cacher)
    md5_result = md5_cached.hash_file(f)

    assert md5_result.method == "sha256"
    assert md5_result.digest == sha_result.digest
```

- [ ] **Step 6: Update `tests/test_hashing/test_file_handler.py`**

Replace:
```python
# OLD:
from orcapod.hashing.file_hashers import BasicFileHasher
```
```python
# NEW:
from orcapod.hashing.file_hashers import FileHasher
```

Replace every `BasicFileHasher(` with `FileHasher(` in that file.

- [ ] **Step 7: Run the existing test suite (it will fail — `InMemoryHashCacher` not yet defined)**

```bash
uv run pytest tests/test_hashing/test_file_hashers.py tests/test_hashing/test_file_handler.py -x -q 2>&1 | head -30
```

Expected: fails with `ModuleNotFoundError` or `ImportError` because `InMemoryHashCacher` doesn't exist yet. That is correct — we implement it in Task 3.

- [ ] **Step 8: Commit the rename (tests broken — that's expected)**

```bash
git add src/orcapod/hashing/file_hashers.py \
        src/orcapod/hashing/__init__.py \
        src/orcapod/hashing/semantic_hashing/builtin_handlers.py \
        src/orcapod/contexts/data/v0.1.json \
        tests/test_hashing/test_file_hashers.py \
        tests/test_hashing/test_file_handler.py
git commit -m "refactor(hashing): rename BasicFileHasher to FileHasher; add FileHashKey; update CachedFileHasher to CacherProtocol"
```

---

## Task 3: Implement `InMemoryHashCacher` and `SqliteHashCacher`

**Files:**
- Create: `src/orcapod/hashing/hash_cachers.py`
- Create: `tests/test_hashing/test_hash_cachers.py`

- [ ] **Step 1: Write failing tests in `tests/test_hashing/test_hash_cachers.py`**

Create the file with this content:

```python
"""Tests for InMemoryHashCacher and SqliteHashCacher."""

import sqlite3
import threading

import pytest
from upath import UPath

from orcapod.hashing.file_hashers import FileHashKey
from orcapod.types import ContentHash

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_key(path_str: str, mtime_ns: int = 1000, size: int = 100) -> FileHashKey:
    return FileHashKey(path=UPath(path_str), mtime_ns=mtime_ns, size=size)


def make_hash(method: str = "sha256", digest: bytes = b"\xab" * 32) -> ContentHash:
    return ContentHash(method=method, digest=digest)


# ---------------------------------------------------------------------------
# InMemoryHashCacher
# ---------------------------------------------------------------------------


class TestInMemoryHashCacher:
    def test_miss_returns_none(self):
        from orcapod.hashing.hash_cachers import InMemoryHashCacher
        cacher = InMemoryHashCacher()
        assert cacher.get(make_key("/a/b.txt")) is None

    def test_put_then_get_returns_hit(self):
        from orcapod.hashing.hash_cachers import InMemoryHashCacher
        cacher = InMemoryHashCacher()
        key = make_key("/a/b.txt")
        value = make_hash()
        cacher.put(key, value)
        assert cacher.get(key) == value

    def test_different_mtime_is_different_key(self):
        from orcapod.hashing.hash_cachers import InMemoryHashCacher
        cacher = InMemoryHashCacher()
        key1 = make_key("/a/b.txt", mtime_ns=1000)
        key2 = make_key("/a/b.txt", mtime_ns=2000)
        cacher.put(key1, make_hash(digest=b"\xaa" * 32))
        assert cacher.get(key2) is None

    def test_different_size_is_different_key(self):
        from orcapod.hashing.hash_cachers import InMemoryHashCacher
        cacher = InMemoryHashCacher()
        key1 = make_key("/a/b.txt", size=100)
        key2 = make_key("/a/b.txt", size=200)
        cacher.put(key1, make_hash(digest=b"\xaa" * 32))
        assert cacher.get(key2) is None

    def test_clear_removes_all_entries(self):
        from orcapod.hashing.hash_cachers import InMemoryHashCacher
        cacher = InMemoryHashCacher()
        key = make_key("/a/b.txt")
        cacher.put(key, make_hash())
        cacher.clear()
        assert cacher.get(key) is None


# ---------------------------------------------------------------------------
# SqliteHashCacher
# ---------------------------------------------------------------------------


class TestSqliteHashCacher:
    def test_miss_returns_none(self, tmp_path):
        from orcapod.hashing.hash_cachers import SqliteHashCacher
        cacher = SqliteHashCacher(tmp_path / "cache.db")
        assert cacher.get(make_key("/a/b.txt")) is None

    def test_put_then_get_returns_hit(self, tmp_path):
        from orcapod.hashing.hash_cachers import SqliteHashCacher
        cacher = SqliteHashCacher(tmp_path / "cache.db")
        key = make_key("/a/b.txt")
        value = make_hash()
        cacher.put(key, value)
        result = cacher.get(key)
        assert result is not None
        assert result.method == value.method
        assert result.digest == value.digest

    def test_different_mtime_is_different_key(self, tmp_path):
        from orcapod.hashing.hash_cachers import SqliteHashCacher
        cacher = SqliteHashCacher(tmp_path / "cache.db")
        key1 = make_key("/a/b.txt", mtime_ns=1000)
        key2 = make_key("/a/b.txt", mtime_ns=2000)
        cacher.put(key1, make_hash(digest=b"\xaa" * 32))
        assert cacher.get(key2) is None

    def test_different_size_is_different_key(self, tmp_path):
        from orcapod.hashing.hash_cachers import SqliteHashCacher
        cacher = SqliteHashCacher(tmp_path / "cache.db")
        key1 = make_key("/a/b.txt", size=100)
        key2 = make_key("/a/b.txt", size=200)
        cacher.put(key1, make_hash(digest=b"\xaa" * 32))
        assert cacher.get(key2) is None

    def test_persistence_across_instances(self, tmp_path):
        from orcapod.hashing.hash_cachers import SqliteHashCacher
        db = tmp_path / "cache.db"
        key = make_key("/a/b.txt")
        value = make_hash()

        cacher1 = SqliteHashCacher(db)
        cacher1.put(key, value)
        cacher1.close()

        cacher2 = SqliteHashCacher(db)
        result = cacher2.get(key)
        assert result is not None
        assert result.method == value.method
        assert result.digest == value.digest

    def test_wal_mode_enabled(self, tmp_path):
        from orcapod.hashing.hash_cachers import SqliteHashCacher
        db = tmp_path / "cache.db"
        SqliteHashCacher(db)
        with sqlite3.connect(db) as conn:
            cursor = conn.execute("PRAGMA journal_mode")
            mode = cursor.fetchone()[0]
        assert mode == "wal"

    def test_clear_empties_table(self, tmp_path):
        from orcapod.hashing.hash_cachers import SqliteHashCacher
        cacher = SqliteHashCacher(tmp_path / "cache.db")
        key = make_key("/a/b.txt")
        cacher.put(key, make_hash())
        cacher.clear()
        assert cacher.get(key) is None

    def test_env_var_path_override(self, tmp_path, monkeypatch):
        from orcapod.hashing.hash_cachers import SqliteHashCacher
        db = tmp_path / "env_cache.db"
        monkeypatch.setenv("ORCAPOD_HASH_CACHE_DB", str(db))
        cacher = SqliteHashCacher()
        assert cacher.db_path == db

    def test_context_manager_closes_connection(self, tmp_path):
        from orcapod.hashing.hash_cachers import SqliteHashCacher
        db = tmp_path / "cache.db"
        with SqliteHashCacher(db) as cacher:
            cacher.put(make_key("/a/b.txt"), make_hash())
        # After __exit__, thread-local conn should be None — verify by
        # reopening and checking persistence still works
        cacher2 = SqliteHashCacher(db)
        assert cacher2.get(make_key("/a/b.txt")) is not None

    def test_idempotent_put(self, tmp_path):
        from orcapod.hashing.hash_cachers import SqliteHashCacher
        cacher = SqliteHashCacher(tmp_path / "cache.db")
        key = make_key("/a/b.txt")
        v1 = make_hash(digest=b"\xaa" * 32)
        v2 = make_hash(digest=b"\xbb" * 32)
        cacher.put(key, v1)
        cacher.put(key, v2)  # INSERT OR REPLACE
        result = cacher.get(key)
        assert result.digest == v2.digest
```

- [ ] **Step 2: Run tests to verify they fail (module not found)**

```bash
uv run pytest tests/test_hashing/test_hash_cachers.py -x -q 2>&1 | head -20
```

Expected: `ModuleNotFoundError: No module named 'orcapod.hashing.hash_cachers'`

- [ ] **Step 3: Create `src/orcapod/hashing/hash_cachers.py`**

```python
"""File hash cacher implementations.

Provides ``InMemoryHashCacher`` (testing/ephemeral use) and
``SqliteHashCacher`` (persistent, production-grade) — both implementing
``CacherProtocol[FileHashKey, ContentHash]``.
"""

import logging
import os
import sqlite3
import threading
from pathlib import Path

from orcapod.hashing.file_hashers import FileHashKey
from orcapod.types import ContentHash

logger = logging.getLogger(__name__)


class InMemoryHashCacher:
    """Dict-backed file hash cacher for testing and ephemeral in-process use.

    No persistence, no thread-safety guarantees beyond the GIL, no eviction.
    Use ``SqliteHashCacher`` for production workloads.
    """

    def __init__(self) -> None:
        self._cache: dict[FileHashKey, ContentHash] = {}

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

        Args:
            key: File hash cache key.
            value: ``ContentHash`` to store.
        """
        self._cache[key] = value

    def clear(self) -> None:
        """Remove all entries from the cache."""
        self._cache.clear()


class SqliteHashCacher:
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

    Note:
        Heavy multi-writer scenarios are a known SQLite limitation. A Turso
        / libSQL migration is planned as a follow-up issue.
    """

    DEFAULT_DB_PATH = Path.home() / ".orcapod" / "file_hash_cache.db"

    def __init__(self, db_path: Path | None = None) -> None:
        self.db_path = Path(
            db_path
            or os.environ.get("ORCAPOD_HASH_CACHE_DB")
            or self.DEFAULT_DB_PATH
        )
        self._local = threading.local()
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        """Create the cache table and enable WAL mode.

        Uses a dedicated one-shot connection so schema setup happens once
        on construction, independent of the thread-local connection pool.
        """
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS file_hash_cache (
                    path      TEXT    NOT NULL,
                    mtime_ns  INTEGER NOT NULL,
                    size      INTEGER NOT NULL,
                    hash      BLOB    NOT NULL,
                    cached_at INTEGER NOT NULL DEFAULT (unixepoch()),
                    PRIMARY KEY (path, mtime_ns, size)
                ) WITHOUT ROWID
                """
            )
            conn.commit()

    def _connection(self) -> sqlite3.Connection:
        """Return this thread's connection, opening it on first use."""
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(self.db_path)
            conn.execute("PRAGMA journal_mode=WAL")
            self._local.conn = conn
        return conn

    def get(self, key: FileHashKey) -> ContentHash | None:
        """Return the cached ``ContentHash`` for ``key``, or ``None`` on miss.

        Args:
            key: File hash cache key.

        Returns:
            Cached ``ContentHash``, or ``None`` if not found.
        """
        conn = self._connection()
        cursor = conn.execute(
            "SELECT hash FROM file_hash_cache WHERE path=? AND mtime_ns=? AND size=?",
            (str(key.path), key.mtime_ns, key.size),
        )
        row = cursor.fetchone()
        if row is None:
            return None
        blob: bytes = row[0]
        method_bytes, digest = blob.split(b":", 1)
        return ContentHash(method=method_bytes.decode("ascii"), digest=digest)

    def put(self, key: FileHashKey, value: ContentHash) -> None:
        """Store ``value`` under ``key``.

        Uses ``INSERT OR REPLACE`` so writes are idempotent.

        Args:
            key: File hash cache key.
            value: ``ContentHash`` to store.
        """
        conn = self._connection()
        conn.execute(
            """
            INSERT OR REPLACE INTO file_hash_cache (path, mtime_ns, size, hash)
            VALUES (?, ?, ?, ?)
            """,
            (str(key.path), key.mtime_ns, key.size, value.to_prefixed_digest()),
        )
        conn.commit()

    def clear(self) -> None:
        """Delete all rows from the cache table."""
        conn = self._connection()
        conn.execute("DELETE FROM file_hash_cache")
        conn.commit()

    def close(self) -> None:
        """Close this thread's database connection."""
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            conn.close()
            self._local.conn = None

    def __enter__(self) -> "SqliteHashCacher":
        """Return self for use as a context manager."""
        return self

    def __exit__(self, *_: object) -> None:
        """Close the thread-local connection on exit."""
        self.close()
```

- [ ] **Step 4: Run the cacher tests**

```bash
uv run pytest tests/test_hashing/test_hash_cachers.py -v
```

Expected: all tests pass.

- [ ] **Step 5: Run the file hasher tests (now `InMemoryHashCacher` exists)**

```bash
uv run pytest tests/test_hashing/test_file_hashers.py tests/test_hashing/test_file_handler.py -v
```

Expected: all tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/hashing/hash_cachers.py \
        tests/test_hashing/test_hash_cachers.py
git commit -m "feat(hashing): add InMemoryHashCacher and SqliteHashCacher"
```

---

## Task 4: Update `hashing/__init__.py` exports

**Files:**
- Modify: `src/orcapod/hashing/__init__.py`

- [ ] **Step 1: Add new exports**

Add the following import after the existing `file_hashers` import line:

```python
from orcapod.hashing.hash_cachers import InMemoryHashCacher, SqliteHashCacher
```

Add to the `__all__` list (after `"CachedFileHasher"`):

```python
    "FileHashKey",
    "InMemoryHashCacher",
    "SqliteHashCacher",
```

Also add `"CacherProtocol"` to the imports from `hashing_protocols` and to `__all__`:

```python
# In the protocols imports block, add:
from orcapod.protocols.hashing_protocols import (
    ArrowHasherProtocol,
    CacherProtocol,          # <-- add this
    ContentIdentifiableProtocol,
    ...
)
```

```python
# In __all__, add:
    "CacherProtocol",
```

- [ ] **Step 2: Verify the public API imports cleanly**

```bash
uv run python -c "
from orcapod.hashing import (
    CacherProtocol, FileHashKey, FileHasher, CachedFileHasher,
    InMemoryHashCacher, SqliteHashCacher
)
print('all imports ok')
"
```

Expected: `all imports ok`

- [ ] **Step 3: Run full hashing test suite**

```bash
uv run pytest tests/test_hashing/ -q
```

Expected: all tests pass.

- [ ] **Step 4: Commit**

```bash
git add src/orcapod/hashing/__init__.py
git commit -m "feat(hashing): export CacherProtocol, FileHashKey, FileHasher, InMemoryHashCacher, SqliteHashCacher"
```

---

## Task 5: Add `enable_file_hash_caching()` to `contexts/__init__.py`

**Files:**
- Modify: `src/orcapod/contexts/__init__.py`
- Modify: `tests/test_hashing/test_hash_cachers.py`

- [ ] **Step 1: Write failing tests — append to `tests/test_hashing/test_hash_cachers.py`**

```python
# ---------------------------------------------------------------------------
# enable_file_hash_caching()
# ---------------------------------------------------------------------------


@pytest.fixture()
def restore_default_file_handler():
    """Restore the default FileHandler after each test to prevent cross-test pollution."""
    from orcapod.contexts import get_default_context
    from orcapod.extension_types.file_type import File

    context = get_default_context()
    registry = context.semantic_hasher.type_handler_registry
    original_handler = registry.get_handler_for_type(File)
    yield
    registry.register(File, original_handler)


class TestEnableFileHashCaching:
    def test_registers_cached_file_hasher(self, restore_default_file_handler, tmp_path):
        from orcapod.contexts import enable_file_hash_caching, get_default_context
        from orcapod.extension_types.file_type import File
        from orcapod.hashing.file_hashers import CachedFileHasher

        enable_file_hash_caching(db_path=tmp_path / "cache.db")

        context = get_default_context()
        registry = context.semantic_hasher.type_handler_registry
        handler = registry.get_handler_for_type(File)
        assert isinstance(handler.file_hasher, CachedFileHasher)

    def test_double_call_logs_warning_and_does_not_double_wrap(
        self, restore_default_file_handler, tmp_path, caplog
    ):
        import logging
        from orcapod.contexts import enable_file_hash_caching, get_default_context
        from orcapod.extension_types.file_type import File
        from orcapod.hashing.file_hashers import CachedFileHasher, FileHasher

        enable_file_hash_caching(db_path=tmp_path / "cache.db")

        with caplog.at_level(logging.WARNING):
            enable_file_hash_caching(db_path=tmp_path / "cache2.db")

        assert "already has a CachedFileHasher" in caplog.text

        context = get_default_context()
        registry = context.semantic_hasher.type_handler_registry
        handler = registry.get_handler_for_type(File)
        # Outer is CachedFileHasher
        assert isinstance(handler.file_hasher, CachedFileHasher)
        # Inner (the base hasher) must NOT be a CachedFileHasher
        assert isinstance(handler.file_hasher.file_hasher, FileHasher)

    def test_preserves_base_hasher_algorithm(
        self, restore_default_file_handler, tmp_path
    ):
        from orcapod.contexts import enable_file_hash_caching, get_default_context
        from orcapod.extension_types.file_type import File

        enable_file_hash_caching(db_path=tmp_path / "cache.db")

        context = get_default_context()
        registry = context.semantic_hasher.type_handler_registry
        handler = registry.get_handler_for_type(File)
        # The base FileHasher should still use sha256 (from v0.1.json)
        assert handler.file_hasher.file_hasher.algorithm == "sha256"
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/test_hashing/test_hash_cachers.py::TestEnableFileHashCaching -x -q 2>&1 | head -20
```

Expected: `ImportError: cannot import name 'enable_file_hash_caching'`

- [ ] **Step 3: Implement `enable_file_hash_caching()` in `src/orcapod/contexts/__init__.py`**

Add the following at the bottom of the imports section (before existing functions):

```python
import logging as _logging

_logger = _logging.getLogger(__name__)
```

Then add this function at the end of the file:

```python
def enable_file_hash_caching(db_path: "Path | None" = None) -> None:
    """Enable SQLite-backed file hash caching on the default Orcapod context.

    Wraps the existing ``FileHandler``'s hasher in a ``CachedFileHasher``
    backed by a ``SqliteHashCacher`` and re-registers it for ``orcapod.File``
    in the default context's semantic hasher registry.

    Call once at application startup before any file hashing occurs.

    If the handler already wraps a ``CachedFileHasher`` (i.e. this function
    was already called), a warning is logged, all existing caching layers are
    unwrapped to reach the original base hasher, and the new cacher is applied
    around that base hasher. This keeps the system in a well-defined state
    (exactly one caching layer) regardless of how many times this function
    is called.

    For intentional multi-layer caching (e.g. in-memory L1 + SQLite L2),
    construct a ``CachedFileHasher`` manually and register it directly via
    the context's ``type_handler_registry`` instead.

    Args:
        db_path: Path to the SQLite cache database. Defaults to
            ``~/.orcapod/file_hash_cache.db`` or the
            ``ORCAPOD_HASH_CACHE_DB`` environment variable.
    """
    from pathlib import Path

    from orcapod.extension_types.file_type import File
    from orcapod.hashing.file_hashers import CachedFileHasher
    from orcapod.hashing.hash_cachers import SqliteHashCacher
    from orcapod.hashing.semantic_hashing.builtin_handlers import FileHandler

    context = get_default_context()
    registry = context.semantic_hasher.type_handler_registry

    existing_handler = registry.get_handler_for_type(File)
    if existing_handler is None:
        raise RuntimeError(
            "enable_file_hash_caching(): no FileHandler registered for "
            "orcapod.File in the default context. This should not happen "
            "with the standard v0.1 context."
        )

    base_hasher = existing_handler.file_hasher

    if isinstance(base_hasher, CachedFileHasher):
        _logger.warning(
            "enable_file_hash_caching() called but the default FileHandler "
            "already has a CachedFileHasher. Unwrapping and replacing with "
            "the new cacher. If layered caching is intentional, construct a "
            "CachedFileHasher manually instead."
        )
        while isinstance(base_hasher, CachedFileHasher):
            base_hasher = base_hasher.file_hasher

    registry.register(
        File,
        FileHandler(
            CachedFileHasher(
                file_hasher=base_hasher,
                cacher=SqliteHashCacher(db_path),
            )
        ),
    )
```

- [ ] **Step 4: Run the new tests**

```bash
uv run pytest tests/test_hashing/test_hash_cachers.py::TestEnableFileHashCaching -v
```

Expected: all 3 tests pass.

- [ ] **Step 5: Run the full test suite**

```bash
uv run pytest tests/ -q --tb=short 2>&1 | tail -20
```

Expected: all tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/contexts/__init__.py \
        tests/test_hashing/test_hash_cachers.py
git commit -m "feat(contexts): add enable_file_hash_caching() to wire SqliteHashCacher into default context"
```

---

## Task 6: Write the bench script

**Files:**
- Create: `bench/bench_file_hasher_cache.py`

- [ ] **Step 1: Create `bench/` directory and write bench script**

```python
#!/usr/bin/env python
"""Benchmark: cached vs uncached file hashing.

Creates a ≥100 MB temp file, then times three scenarios:
  1. Uncached hash (baseline)
  2. First cached hash (miss — hash computed + stored)
  3. Second cached hash (hit — lookup only, should be sub-millisecond)

Run: uv run python bench/bench_file_hasher_cache.py
"""

import tempfile
import time
from pathlib import Path

from orcapod.hashing.file_hashers import CachedFileHasher, FileHasher
from orcapod.hashing.hash_cachers import SqliteHashCacher

FILE_SIZE_MB = 100
ITERATIONS = 3


def create_large_file(path: Path, size_mb: int) -> None:
    chunk = b"\x00" * (1024 * 1024)
    with open(path, "wb") as f:
        for _ in range(size_mb):
            f.write(chunk)


def time_call(fn, *args) -> tuple[object, float]:
    t0 = time.perf_counter()
    result = fn(*args)
    elapsed = time.perf_counter() - t0
    return result, elapsed


def main() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        large_file = tmp / "bench_input.bin"
        db_path = tmp / "bench_cache.db"

        print(f"Creating {FILE_SIZE_MB} MB temp file...")
        create_large_file(large_file, FILE_SIZE_MB)

        base_hasher = FileHasher(algorithm="sha256")

        # --- Uncached baseline ---
        _, uncached_time = time_call(base_hasher.hash_file, large_file)

        # --- Cached: miss ---
        cached_hasher = CachedFileHasher(
            file_hasher=base_hasher,
            cacher=SqliteHashCacher(db_path),
        )
        _, miss_time = time_call(cached_hasher.hash_file, large_file)

        # --- Cached: hit (multiple times) ---
        hit_times = []
        for _ in range(ITERATIONS):
            _, t = time_call(cached_hasher.hash_file, large_file)
            hit_times.append(t)
        avg_hit = sum(hit_times) / len(hit_times)

        print()
        print(f"{'Scenario':<30} {'Time (ms)':>12}")
        print("-" * 44)
        print(f"{'Uncached (baseline)':<30} {uncached_time * 1000:>12.2f}")
        print(f"{'Cached miss (1st call)':<30} {miss_time * 1000:>12.2f}")
        print(f"{'Cached hit (avg of 3)':<30} {avg_hit * 1000:>12.3f}")
        print()
        speedup = uncached_time / avg_hit if avg_hit > 0 else float("inf")
        print(f"Cache hit speedup: {speedup:.0f}x")
        if avg_hit < 0.001:
            print("✓ Sub-millisecond cache hit achieved")
        else:
            print(f"! Cache hit is {avg_hit * 1000:.2f} ms — expected < 1 ms")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Run the bench script**

```bash
uv run python bench/bench_file_hasher_cache.py
```

Expected output (timings will vary):
```
Creating 100 MB temp file...

Scenario                              Time (ms)
--------------------------------------------
Uncached (baseline)                     XXX.XX
Cached miss (1st call)                  XXX.XX
Cached hit (avg of 3)                     0.XXX

Cache hit speedup: XXXx
✓ Sub-millisecond cache hit achieved
```

- [ ] **Step 3: Run full test suite one final time**

```bash
uv run pytest tests/ -q --tb=short 2>&1 | tail -10
```

Expected: all tests pass, no failures.

- [ ] **Step 4: Commit**

```bash
git add bench/bench_file_hasher_cache.py
git commit -m "feat(bench): add file hasher cache benchmark script"
```

---

## Self-Review

**Spec coverage check:**

| Spec requirement | Covered by |
|---|---|
| `CacherProtocol[K, V]` protocol | Task 1 |
| `FileHashKey` frozen dataclass with `UPath` | Task 2 |
| `FileHasher` (rename of `BasicFileHasher`) | Task 2 |
| `CachedFileHasher` uses `CacherProtocol` + UPath normalization | Task 2 |
| `InMemoryHashCacher` | Task 3 |
| `SqliteHashCacher` with WAL, WITHOUT ROWID, BLOB, thread-local | Task 3 |
| `hashing/__init__.py` exports updated | Task 4 |
| `enable_file_hash_caching()` with warning on double-call | Task 5 |
| Tests: hit/miss/isolation/persistence/WAL/env-var | Task 3 |
| Tests: `enable_file_hash_caching()` double-call warning | Task 5 |
| Bench script on ≥100 MB file | Task 6 |
| All `BasicFileHasher` references updated | Task 2 |

**All spec requirements covered. No gaps.**
