# PostgreSQL Hash Cacher (ITL-520) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `PostgresHashCacher` — a Postgres-backed drop-in alternative to `SqliteHashCacher` — plus a `conninfo` parameter on `enable_file_hash_caching()` to activate it.

**Architecture:** `PostgresHashCacher` lives in its own module (`postgres_hash_cacher.py`), uses thread-local psycopg3 connections, and stores hashes in a `file_hash_cache` table with `INSERT ... ON CONFLICT DO NOTHING` for concurrency safety. It is exported from `orcapod.hashing` under a `try/except ImportError` guard so users without `psycopg` installed pay nothing. Tests use `testcontainers.postgres.PostgresContainer` against a real Postgres.

**Tech Stack:** Python 3.12, `psycopg[binary]>=3.0`, `testcontainers[postgres]>=4.0.0`, `uv run pytest`

**Spec:** `superpowers/specs/2026-07-10-itl-520-postgres-hash-cacher-design.md`

**Branch:** `eywalker/itl-520-filehasher-add-postgresql-backend-for-the-hash-cacher`

---

## File Map

| File | Action | Responsibility |
|------|--------|----------------|
| `src/orcapod/hashing/postgres_hash_cacher.py` | Create | `PostgresHashCacher` — full implementation |
| `tests/test_hashing/test_postgres_hash_cacher.py` | Create | All `PostgresHashCacher` tests via testcontainers |
| `src/orcapod/hashing/__init__.py` | Modify | Export `PostgresHashCacher` under `try/except ImportError` |
| `src/orcapod/contexts/__init__.py` | Modify | Add `conninfo` param to `enable_file_hash_caching()` |
| `tests/test_hashing/test_hash_cachers.py` | Modify | Add `ValueError` test for mutually-exclusive `conninfo`/`db_path` |
| `pyproject.toml` | Modify | Add `testcontainers[postgres]>=4.0.0` to dev deps |
| `docs/concepts/file-hash-caching.md` | Modify | Add "Choosing a backend: SQLite vs Postgres" section |

---

## Task 0: Check out the feature branch

**Files:** none

- [ ] **Step 0.1: Check out the branch**

```bash
git checkout main
git pull
git checkout -b eywalker/itl-520-filehasher-add-postgresql-backend-for-the-hash-cacher
git branch --show-current
```

Expected output: `eywalker/itl-520-filehasher-add-postgresql-backend-for-the-hash-cacher`

---

## Task 1: Add `testcontainers[postgres]` to dev deps

**Files:**
- Modify: `pyproject.toml`

- [ ] **Step 1.1: Add the dev dependency**

Open `pyproject.toml`. In the `[dependency-groups]` → `dev` list, add `testcontainers[postgres]` immediately after the existing `testcontainers[minio]` line:

```toml
    "testcontainers[minio]>=4.0.0",
    "testcontainers[postgres]>=4.0.0",
```

- [ ] **Step 1.2: Sync and verify**

```bash
uv sync
uv run python -c "from testcontainers.postgres import PostgresContainer; print('OK')"
```

Expected: `OK`

- [ ] **Step 1.3: Commit**

```bash
git add pyproject.toml uv.lock
git commit -m "chore(deps): add testcontainers[postgres] to dev deps (ITL-520)"
```

---

## Task 2: Write failing tests for `PostgresHashCacher` basic operations

Write the tests first. They will fail with `ImportError` / `ModuleNotFoundError` because `postgres_hash_cacher.py` does not exist yet.

**Files:**
- Create: `tests/test_hashing/test_postgres_hash_cacher.py`

- [ ] **Step 2.1: Create the test file**

Create `tests/test_hashing/test_postgres_hash_cacher.py` with this content:

```python
"""Tests for PostgresHashCacher using a real Postgres via testcontainers."""

from __future__ import annotations

import threading

import pytest
from upath import UPath

psycopg = pytest.importorskip("psycopg")

from testcontainers.postgres import PostgresContainer  # noqa: E402

from orcapod.hashing.file_hashers import FileHashKey  # noqa: E402
from orcapod.hashing.postgres_hash_cacher import PostgresHashCacher  # noqa: E402
from orcapod.types import ContentHash  # noqa: E402


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
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def pg_conninfo():
    """Start one Postgres container for the entire module; yield a conninfo string."""
    with PostgresContainer("postgres:16") as pg:
        yield (
            f"host={pg.get_container_host_ip()} "
            f"port={pg.get_exposed_port(5432)} "
            f"dbname={pg.dbname} "
            f"user={pg.username} "
            f"password={pg.password}"
        )


@pytest.fixture()
def cacher(pg_conninfo):
    """Fresh PostgresHashCacher with a cleared table for each test."""
    c = PostgresHashCacher(pg_conninfo)
    c.clear()
    yield c
    c.close()


# ---------------------------------------------------------------------------
# Basic operations
# ---------------------------------------------------------------------------


@pytest.mark.postgres
class TestPostgresHashCacherBasic:
    def test_miss_returns_none(self, cacher):
        assert cacher.get(make_key()) is None

    def test_put_then_get_returns_hit(self, cacher):
        key = make_key()
        value = make_hash()
        cacher.put(key, value)
        result = cacher.get(key)
        assert result is not None
        assert result.method == value.method
        assert result.digest == value.digest

    def test_different_path_is_miss(self, cacher):
        cacher.put(make_key("/a/b.txt"), make_hash(b"\xaa" * 32))
        assert cacher.get(make_key("/a/c.txt")) is None

    def test_different_mtime_ns_is_miss(self, cacher):
        cacher.put(make_key(mtime_ns=1000), make_hash(b"\xaa" * 32))
        assert cacher.get(make_key(mtime_ns=2000)) is None

    def test_different_size_is_miss(self, cacher):
        cacher.put(make_key(size=100), make_hash(b"\xaa" * 32))
        assert cacher.get(make_key(size=200)) is None

    def test_clear_empties_table(self, cacher):
        key = make_key()
        cacher.put(key, make_hash())
        cacher.clear()
        assert cacher.get(key) is None

    def test_persistence_across_instances(self, pg_conninfo):
        key = make_key("/persist.txt", mtime_ns=9999, size=512)
        value = make_hash(b"\xcc" * 32)

        cacher1 = PostgresHashCacher(pg_conninfo)
        cacher1.clear()
        cacher1.put(key, value)
        cacher1.close()

        cacher2 = PostgresHashCacher(pg_conninfo)
        result = cacher2.get(key)
        cacher2.close()

        assert result is not None
        assert result.method == value.method
        assert result.digest == value.digest

    def test_context_manager_closes_connection(self, pg_conninfo):
        key = make_key("/ctx.txt", mtime_ns=7777, size=64)
        value = make_hash(b"\xdd" * 32)

        with PostgresHashCacher(pg_conninfo) as c:
            c.clear()
            c.put(key, value)

        # Verify persistence (connection was closed but data committed)
        cacher2 = PostgresHashCacher(pg_conninfo)
        result = cacher2.get(key)
        cacher2.close()
        assert result is not None
        assert result.digest == value.digest


# ---------------------------------------------------------------------------
# Write guards
# ---------------------------------------------------------------------------


@pytest.mark.postgres
class TestPostgresHashCacherReadOnly:
    def test_put_is_noop_when_read_only(self, pg_conninfo):
        key = make_key("/ro.txt")
        value = make_hash()

        cacher = PostgresHashCacher(pg_conninfo, read_only=True)
        cacher.clear()
        cacher.put(key, value)
        assert cacher.get(key) is None
        cacher.close()

    def test_read_only_can_read_preexisting(self, pg_conninfo):
        key = make_key("/ro_pre.txt", mtime_ns=5555, size=256)
        value = make_hash(b"\xee" * 32)

        writable = PostgresHashCacher(pg_conninfo)
        writable.clear()
        writable.put(key, value)
        writable.close()

        read_only = PostgresHashCacher(pg_conninfo, read_only=True)
        result = read_only.get(key)
        read_only.close()

        assert result is not None
        assert result.digest == value.digest

    def test_default_is_not_read_only(self, cacher):
        key = make_key()
        value = make_hash()
        cacher.put(key, value)
        assert cacher.get(key) is not None


@pytest.mark.postgres
class TestPostgresHashCacherThreshold:
    def test_small_file_not_cached(self, pg_conninfo):
        cacher = PostgresHashCacher(pg_conninfo, min_cache_size_bytes=100)
        cacher.clear()
        key = make_key(size=50)
        cacher.put(key, make_hash())
        assert cacher.get(key) is None
        cacher.close()

    def test_file_at_threshold_is_cached(self, pg_conninfo):
        cacher = PostgresHashCacher(pg_conninfo, min_cache_size_bytes=100)
        cacher.clear()
        key = make_key(size=100)
        value = make_hash()
        cacher.put(key, value)
        result = cacher.get(key)
        cacher.close()
        assert result is not None
        assert result.digest == value.digest

    def test_large_file_is_cached(self, pg_conninfo):
        cacher = PostgresHashCacher(pg_conninfo, min_cache_size_bytes=100)
        cacher.clear()
        key = make_key(size=200)
        value = make_hash()
        cacher.put(key, value)
        assert cacher.get(key) is not None
        cacher.close()

    def test_none_threshold_caches_all(self, pg_conninfo):
        cacher = PostgresHashCacher(pg_conninfo, min_cache_size_bytes=None)
        cacher.clear()
        key = make_key(size=1)
        value = make_hash()
        cacher.put(key, value)
        assert cacher.get(key) is not None
        cacher.close()

    def test_zero_threshold_caches_all(self, pg_conninfo):
        cacher = PostgresHashCacher(pg_conninfo, min_cache_size_bytes=0)
        cacher.clear()
        key = make_key(size=1)
        value = make_hash()
        cacher.put(key, value)
        assert cacher.get(key) is not None
        cacher.close()


# ---------------------------------------------------------------------------
# Concurrency
# ---------------------------------------------------------------------------


@pytest.mark.postgres
class TestPostgresHashCacherConcurrency:
    def test_concurrent_insert_no_error_one_row(self, pg_conninfo):
        """Two threads inserting the same key simultaneously → no error, one row."""
        key = make_key("/concurrent.txt", mtime_ns=12345, size=999)
        value = make_hash(b"\xff" * 32)

        # Pre-clear using a dedicated cacher
        setup = PostgresHashCacher(pg_conninfo)
        setup.clear()
        setup.close()

        errors: list[Exception] = []

        def insert() -> None:
            try:
                c = PostgresHashCacher(pg_conninfo)
                c.put(key, value)
                c.close()
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=insert) for _ in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == [], f"Concurrent inserts raised errors: {errors}"

        # Verify exactly one row was written
        with psycopg.connect(pg_conninfo) as conn:
            count = conn.execute(
                "SELECT COUNT(*) FROM file_hash_cache "
                "WHERE path=%s AND mtime_ns=%s AND size=%s",
                (str(key.path), key.mtime_ns, key.size),
            ).fetchone()[0]
        assert count == 1


# ---------------------------------------------------------------------------
# Parity with SqliteHashCacher
# ---------------------------------------------------------------------------


@pytest.mark.postgres
class TestPostgresHashCacherParity:
    def test_parity_with_sqlite(self, pg_conninfo, tmp_path):
        """Same key/value → equivalent ContentHash from both backends."""
        from orcapod.hashing.hash_cachers import SqliteHashCacher

        key = make_key("/parity.txt", mtime_ns=111, size=222)
        value = make_hash(b"\x12" * 32)

        pg_cacher = PostgresHashCacher(pg_conninfo)
        pg_cacher.clear()
        pg_cacher.put(key, value)
        pg_result = pg_cacher.get(key)
        pg_cacher.close()

        sq_cacher = SqliteHashCacher(tmp_path / "parity.db")
        sq_cacher.put(key, value)
        sq_result = sq_cacher.get(key)

        assert pg_result is not None
        assert sq_result is not None
        assert pg_result.method == sq_result.method
        assert pg_result.digest == sq_result.digest


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


@pytest.mark.postgres
class TestPostgresHashCacherValidation:
    def test_negative_min_cache_size_bytes_raises(self, pg_conninfo):
        with pytest.raises(ValueError, match="non-negative"):
            PostgresHashCacher(pg_conninfo, min_cache_size_bytes=-1)

    def test_repr_includes_conninfo_and_guards(self, pg_conninfo):
        cacher = PostgresHashCacher(
            pg_conninfo, read_only=True, min_cache_size_bytes=1024
        )
        r = repr(cacher)
        assert "PostgresHashCacher" in r
        assert "read_only=True" in r
        assert "min_cache_size_bytes=1024" in r
        cacher.close()
```

- [ ] **Step 2.2: Run tests to verify they fail at import**

```bash
uv run pytest tests/test_hashing/test_postgres_hash_cacher.py -v 2>&1 | head -20
```

Expected: error containing `ModuleNotFoundError` or `ImportError: cannot import name 'PostgresHashCacher'`

---

## Task 3: Implement `PostgresHashCacher`

**Files:**
- Create: `src/orcapod/hashing/postgres_hash_cacher.py`

- [ ] **Step 3.1: Create the implementation file**

Create `src/orcapod/hashing/postgres_hash_cacher.py` with this content:

```python
"""PostgreSQL-backed file hash cacher.

Provides ``PostgresHashCacher`` — a network-accessible, concurrent-safe
implementation of ``CacherProtocol[FileHashKey, ContentHash]``. Requires
the optional ``psycopg`` driver (``pip install 'orcapod[postgresql]'``).
"""

from __future__ import annotations

import threading
from typing import TYPE_CHECKING

from orcapod.hashing.file_hashers import FileHashKey
from orcapod.types import ContentHash

if TYPE_CHECKING:
    import psycopg as _psycopg


class PostgresHashCacher:
    """PostgreSQL-backed file hash cacher.

    Stores file hashes keyed on ``(path, mtime_ns, size)`` in a shared
    PostgreSQL database. Uses thread-local connections for thread safety
    and ``INSERT ... ON CONFLICT DO NOTHING`` for concurrent-insert safety.

    The hash is stored as a BYTEA in ``{method}:{raw_digest}`` format via
    ``ContentHash.to_prefixed_digest()``.

    Requires ``psycopg[binary]>=3.0`` (install with
    ``pip install 'orcapod[postgresql]'``).

    Args:
        conninfo: psycopg3 connection string, e.g.
            ``"postgresql://user:pass@host:5432/dbname"`` or keyword DSN
            ``"host=myhost dbname=mydb user=myuser password=mypass"``.
        read_only: When ``True``, all ``put()`` calls are silent no-ops.
            ``get()`` still works normally. Defaults to ``False``.
        min_cache_size_bytes: When set to a positive integer, files whose
            ``key.size`` is strictly below this threshold are not inserted.
            ``None`` and ``0`` disable the threshold (default behaviour).
            Negative values raise ``ValueError``. Defaults to ``None``.
    """

    def __init__(
        self,
        conninfo: str,
        *,
        read_only: bool = False,
        min_cache_size_bytes: int | None = None,
    ) -> None:
        try:
            import psycopg  # noqa: F401
        except ImportError:
            raise ImportError(
                "PostgresHashCacher requires psycopg. "
                "Install it with: pip install 'orcapod[postgresql]'"
            ) from None
        if min_cache_size_bytes is not None and min_cache_size_bytes < 0:
            raise ValueError(
                f"min_cache_size_bytes must be None or a non-negative integer, "
                f"got {min_cache_size_bytes!r}"
            )
        self._conninfo = conninfo
        self._read_only = read_only
        self._min_cache_size_bytes = min_cache_size_bytes
        self._local = threading.local()
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        """Create the cache table if it does not exist.

        Uses a dedicated one-shot connection so schema setup happens once
        on construction, independent of the thread-local connection pool.
        """
        import psycopg

        with psycopg.connect(self._conninfo) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS file_hash_cache (
                    path      TEXT   NOT NULL,
                    mtime_ns  BIGINT NOT NULL,
                    size      BIGINT NOT NULL,
                    hash      BYTEA  NOT NULL,
                    cached_at BIGINT NOT NULL
                        DEFAULT EXTRACT(EPOCH FROM NOW())::BIGINT,
                    PRIMARY KEY (path, mtime_ns, size)
                )
                """
            )

    def _connection(self) -> "_psycopg.Connection[tuple[object, ...]]":
        """Return this thread's connection, opening it on first use."""
        import psycopg

        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = psycopg.connect(self._conninfo)
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
        row = conn.execute(
            "SELECT hash FROM file_hash_cache "
            "WHERE path=%s AND mtime_ns=%s AND size=%s",
            (str(key.path), key.mtime_ns, key.size),
        ).fetchone()
        if row is None:
            return None
        blob: bytes = bytes(row[0])
        method_bytes, digest = blob.split(b":", 1)
        return ContentHash(method=method_bytes.decode("ascii"), digest=digest)

    def put(self, key: FileHashKey, value: ContentHash) -> None:
        """Store ``value`` under ``key``.

        No-ops silently when ``read_only=True`` or when ``key.size`` is below
        ``min_cache_size_bytes``. Uses ``INSERT ... ON CONFLICT DO NOTHING``
        so concurrent inserts of the same key from multiple workers are safe.

        Args:
            key: File hash cache key.
            value: ``ContentHash`` to store.
        """
        if self._read_only:
            return
        if (
            self._min_cache_size_bytes is not None
            and self._min_cache_size_bytes > 0
            and key.size < self._min_cache_size_bytes
        ):
            return
        conn = self._connection()
        conn.execute(
            """
            INSERT INTO file_hash_cache (path, mtime_ns, size, hash)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (path, mtime_ns, size) DO NOTHING
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

    def __enter__(self) -> "PostgresHashCacher":
        """Return self for use as a context manager."""
        return self

    def __exit__(self, *_: object) -> None:
        """Close the thread-local connection on exit."""
        self.close()

    def __repr__(self) -> str:
        return (
            f"PostgresHashCacher("
            f"conninfo={self._conninfo!r}, "
            f"read_only={self._read_only!r}, "
            f"min_cache_size_bytes={self._min_cache_size_bytes!r})"
        )
```

- [ ] **Step 3.2: Run all Postgres tests**

```bash
uv run pytest tests/test_hashing/test_postgres_hash_cacher.py -v -m postgres
```

Expected: all tests PASS. (Container spin-up takes ~10–20 s on first run.)

- [ ] **Step 3.3: Commit**

```bash
git add src/orcapod/hashing/postgres_hash_cacher.py tests/test_hashing/test_postgres_hash_cacher.py
git commit -m "feat(hashing): add PostgresHashCacher with thread-local psycopg3 connections (ITL-520)"
```

---

## Task 4: Export `PostgresHashCacher` from `orcapod.hashing`

**Files:**
- Modify: `src/orcapod/hashing/__init__.py`

- [ ] **Step 4.1: Add the guarded import**

Open `src/orcapod/hashing/__init__.py`. Find the existing `try/except` block for `legacy_core` near the bottom of the import section (around line 86). Add a new `try/except` block immediately after the `from orcapod.hashing.hash_cachers import ...` line (around line 53):

```python
from orcapod.hashing.hash_cachers import InMemoryHashCacher, SqliteHashCacher

try:
    from orcapod.hashing.postgres_hash_cacher import PostgresHashCacher
except ImportError:
    PostgresHashCacher = None  # type: ignore[assignment,misc]
```

Then add `"PostgresHashCacher"` to the `__all__` list at the bottom of the file, after `"SqliteHashCacher"`:

```python
    "InMemoryHashCacher",
    "SqliteHashCacher",
    "PostgresHashCacher",
```

- [ ] **Step 4.2: Verify the export works**

```bash
uv run python -c "from orcapod.hashing import PostgresHashCacher; print(PostgresHashCacher)"
```

Expected: `<class 'orcapod.hashing.postgres_hash_cacher.PostgresHashCacher'>`

- [ ] **Step 4.3: Verify existing tests still pass**

```bash
uv run pytest tests/test_hashing/test_hash_cachers.py -v
```

Expected: all PASS (no regressions).

- [ ] **Step 4.4: Commit**

```bash
git add src/orcapod/hashing/__init__.py
git commit -m "feat(hashing): export PostgresHashCacher from orcapod.hashing (ITL-520)"
```

---

## Task 5: Extend `enable_file_hash_caching()` with `conninfo` parameter

**Files:**
- Modify: `src/orcapod/contexts/__init__.py`
- Modify: `tests/test_hashing/test_hash_cachers.py`

- [ ] **Step 5.1: Write the failing ValueError test first**

Open `tests/test_hashing/test_hash_cachers.py`. Add this new class at the end of the file (after `TestEnableFileHashCaching`):

```python
class TestEnableFileHashCachingConninfo:
    def test_conninfo_and_db_path_raises(self, restore_default_file_handler, tmp_path):
        """Providing both conninfo and db_path raises ValueError."""
        from orcapod.contexts import enable_file_hash_caching

        with pytest.raises(ValueError, match="not both"):
            enable_file_hash_caching(
                db_path=tmp_path / "x.db",
                conninfo="postgresql://unused",
            )
```

- [ ] **Step 5.2: Run the failing test**

```bash
uv run pytest tests/test_hashing/test_hash_cachers.py::TestEnableFileHashCachingConninfo -v
```

Expected: FAIL — `enable_file_hash_caching` does not yet accept `conninfo`.

- [ ] **Step 5.3: Update `enable_file_hash_caching()`**

Open `src/orcapod/contexts/__init__.py`. Find the `enable_file_hash_caching` function (around line 231). Replace the entire function signature and docstring with:

```python
def enable_file_hash_caching(
    db_path: "Path | None" = None,
    *,
    conninfo: str | None = None,
    read_only: bool = False,
    min_cache_size_bytes: int | None = None,
) -> None:
    """Enable file hash caching on the default Orcapod context.

    When ``conninfo`` is provided, uses ``PostgresHashCacher`` for a shared,
    network-accessible cache suitable for multi-machine pipelines. Otherwise
    uses ``SqliteHashCacher`` for a local, per-machine cache (default).

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

    Also patches ``DirectoryHandler`` for ``orcapod.Directory``, using the
    **same** ``CachedFileHasher`` instance. This means a file that was
    cached via a direct ``op.File`` hash is also a cache hit when the same
    file is encountered during directory traversal.

    Args:
        db_path: Path to the SQLite cache database. Ignored when ``conninfo``
            is provided. Defaults to ``~/.orcapod/file_hash_cache.db`` or
            the ``ORCAPOD_HASH_CACHE_DB`` environment variable.
        conninfo: psycopg3 connection string for a PostgreSQL cache database,
            e.g. ``"postgresql://user:pass@host:5432/db"``. When provided,
            ``PostgresHashCacher`` is used instead of ``SqliteHashCacher``.
            Mutually exclusive with ``db_path``.
        read_only: When ``True``, the underlying cacher will not insert new
            entries. Lookups still work normally. Defaults to ``False``.
        min_cache_size_bytes: When set, files smaller than this byte count
            are not inserted into the cache. ``None`` and ``0`` disable the
            threshold. Defaults to ``None``.

    Raises:
        ValueError: If both ``conninfo`` and ``db_path`` are provided.
    """
    if conninfo is not None and db_path is not None:
        raise ValueError(
            "enable_file_hash_caching(): provide conninfo or db_path, not both."
        )
```

Then, inside the function body, replace the block that constructs `cached_file_hasher` (find the lines that build `SqliteHashCacher` and `CachedFileHasher`):

```python
    if conninfo is not None:
        from orcapod.hashing.postgres_hash_cacher import PostgresHashCacher

        cacher = PostgresHashCacher(
            conninfo,
            read_only=read_only,
            min_cache_size_bytes=min_cache_size_bytes,
        )
    else:
        # SqliteHashCacher import was previously at the top of the function body;
        # it now lives inside this branch only.
        from orcapod.hashing.hash_cachers import SqliteHashCacher

        cacher = SqliteHashCacher(
            db_path,
            read_only=read_only,
            min_cache_size_bytes=min_cache_size_bytes,
        )

    cached_file_hasher = CachedFileHasher(
        file_hasher=base_hasher,
        cacher=cacher,
    )
```

The rest of the function body (registering `FileHandler` and `DirectoryHandler`) remains unchanged.

- [ ] **Step 5.4: Run the ValueError test — should now pass**

```bash
uv run pytest tests/test_hashing/test_hash_cachers.py::TestEnableFileHashCachingConninfo -v
```

Expected: PASS

- [ ] **Step 5.5: Verify no regressions in existing enable_file_hash_caching tests**

```bash
uv run pytest tests/test_hashing/test_hash_cachers.py -v
```

Expected: all PASS

- [ ] **Step 5.6: Add Postgres-specific enable_file_hash_caching tests to the Postgres test file**

Open `tests/test_hashing/test_postgres_hash_cacher.py`. Append this class at the end:

```python
# ---------------------------------------------------------------------------
# enable_file_hash_caching(conninfo=...) integration
# ---------------------------------------------------------------------------


@pytest.fixture()
def restore_default_file_handler():
    """Restore the default FileHandler and DirectoryHandler after each test."""
    from orcapod.contexts import get_default_context
    from orcapod.extension_types.directory_type import Directory
    from orcapod.extension_types.file_type import File

    context = get_default_context()
    registry = context.semantic_hasher.type_handler_registry
    original_file_handler = registry.get_handler_for_type(File)
    original_dir_handler = registry.get_handler_for_type(Directory)
    yield
    registry.register(File, original_file_handler)
    registry.register(Directory, original_dir_handler)


@pytest.mark.postgres
class TestEnableFileHashCachingPostgres:
    def test_conninfo_activates_postgres_cacher(
        self, restore_default_file_handler, pg_conninfo
    ):
        """enable_file_hash_caching(conninfo=...) wires up PostgresHashCacher."""
        from orcapod.contexts import enable_file_hash_caching, get_default_context
        from orcapod.extension_types.file_type import File

        enable_file_hash_caching(conninfo=pg_conninfo)

        context = get_default_context()
        registry = context.semantic_hasher.type_handler_registry
        handler = registry.get_handler_for_type(File)
        assert isinstance(handler.file_hasher.cacher, PostgresHashCacher)

    def test_conninfo_passes_read_only(
        self, restore_default_file_handler, pg_conninfo
    ):
        """read_only=True is forwarded to PostgresHashCacher."""
        from orcapod.contexts import enable_file_hash_caching, get_default_context
        from orcapod.extension_types.file_type import File

        enable_file_hash_caching(conninfo=pg_conninfo, read_only=True)

        context = get_default_context()
        registry = context.semantic_hasher.type_handler_registry
        handler = registry.get_handler_for_type(File)
        assert handler.file_hasher.cacher._read_only is True

    def test_conninfo_passes_min_cache_size_bytes(
        self, restore_default_file_handler, pg_conninfo
    ):
        """min_cache_size_bytes is forwarded to PostgresHashCacher."""
        from orcapod.contexts import enable_file_hash_caching, get_default_context
        from orcapod.extension_types.file_type import File

        enable_file_hash_caching(conninfo=pg_conninfo, min_cache_size_bytes=2048)

        context = get_default_context()
        registry = context.semantic_hasher.type_handler_registry
        handler = registry.get_handler_for_type(File)
        assert handler.file_hasher.cacher._min_cache_size_bytes == 2048
```

- [ ] **Step 5.7: Run the new integration tests**

```bash
uv run pytest tests/test_hashing/test_postgres_hash_cacher.py::TestEnableFileHashCachingPostgres -v -m postgres
```

Expected: all PASS

- [ ] **Step 5.8: Commit**

```bash
git add src/orcapod/contexts/__init__.py \
        tests/test_hashing/test_hash_cachers.py \
        tests/test_hashing/test_postgres_hash_cacher.py
git commit -m "feat(contexts): add conninfo param to enable_file_hash_caching() for Postgres backend (ITL-520)"
```

---

## Task 6: Update documentation

**Files:**
- Modify: `docs/concepts/file-hash-caching.md`

- [ ] **Step 6.1: Add the backend comparison section**

Open `docs/concepts/file-hash-caching.md`. Find the end of the file and append the following section. (If there is already a section about "Controlling when the cache is written" from ITL-519, add the new section after it.)

```markdown
## Choosing a backend: SQLite vs Postgres

Orcapod ships two hash cache backends. Pick based on your deployment:

| | SQLite | Postgres |
|---|---|---|
| Setup | Zero — file on disk | Requires a running Postgres server |
| Scope | Per-machine | Shared across machines and pipeline runs |
| Concurrency | Single-writer | Multi-writer safe (`ON CONFLICT DO NOTHING`) |
| Best for | Local development, single-machine pipelines | Distributed pipelines, shared caches |

### Using the SQLite backend (default)

```python
import orcapod as op

op.enable_file_hash_caching()  # uses ~/.orcapod/file_hash_cache.db
# or specify a path:
op.enable_file_hash_caching(db_path="/shared/nfs/cache.db")
```

### Using the Postgres backend

Requires `psycopg` (install with `pip install 'orcapod[postgresql]'`):

```python
import orcapod as op

op.enable_file_hash_caching(
    conninfo="postgresql://user:pass@db-host:5432/orcapod_cache"
)
```

You can also construct `PostgresHashCacher` directly and pass it to
`CachedFileHasher` for advanced configurations:

```python
from orcapod.hashing import CachedFileHasher, FileHasher, PostgresHashCacher

cacher = PostgresHashCacher(
    conninfo="postgresql://user:pass@db-host:5432/orcapod_cache",
    read_only=True,           # lookup only, no writes
    min_cache_size_bytes=1_048_576,  # skip files < 1 MB
)
hasher = CachedFileHasher(file_hasher=FileHasher(), cacher=cacher)
```

### Schema

`PostgresHashCacher` creates the following table on first use
(`CREATE TABLE IF NOT EXISTS`):

```sql
CREATE TABLE IF NOT EXISTS file_hash_cache (
    path      TEXT   NOT NULL,
    mtime_ns  BIGINT NOT NULL,
    size      BIGINT NOT NULL,
    hash      BYTEA  NOT NULL,
    cached_at BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM NOW())::BIGINT,
    PRIMARY KEY (path, mtime_ns, size)
)
```

Minimum supported Postgres version: **14**.
```

- [ ] **Step 6.2: Commit**

```bash
git add docs/concepts/file-hash-caching.md
git commit -m "docs(hashing): add Postgres backend section to file-hash-caching docs (ITL-520)"
```

---

## Task 7: Full test run and PR

**Files:** none

- [ ] **Step 7.1: Run the full hashing test suite**

```bash
uv run pytest tests/test_hashing/ -v
```

Expected: all tests PASS (Postgres tests require Docker; SQLite/in-memory tests run unconditionally).

- [ ] **Step 7.2: Run the broader test suite for regressions**

```bash
uv run pytest tests/ -v --ignore=tests/test_hashing/test_postgres_hash_cacher.py -q
```

Expected: all tests PASS, no regressions.

- [ ] **Step 7.3: Push and open PR**

```bash
git push -u origin eywalker/itl-520-filehasher-add-postgresql-backend-for-the-hash-cacher
```

Then open a PR targeting `main` with a description referencing `Closes ITL-520`.
