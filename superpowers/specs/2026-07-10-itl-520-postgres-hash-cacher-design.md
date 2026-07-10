# ITL-520: FileHasher — PostgreSQL Backend for the Hash Cacher

**Date:** 2026-07-10
**Issue:** [ITL-520](https://linear.app/enigma-metamorphic/issue/ITL-520)
**Status:** Approved

---

## Overview

Add `PostgresHashCacher` — a Postgres-backed implementation of the `CacherProtocol[FileHashKey, ContentHash]` that mirrors `SqliteHashCacher` in API and semantics but stores hashes in a shared, network-accessible Postgres database. Multiple machines and pipeline runs can consult and populate the cache concurrently without the per-machine limitation of a SQLite file.

---

## Goals & Success Criteria

- New `PostgresHashCacher` class implementing `CacherProtocol[FileHashKey, ContentHash]`.
- Constructor accepts a psycopg3 `conninfo` string (URL or keyword DSN).
- Idempotent schema creation on first use (`CREATE TABLE IF NOT EXISTS`).
- `INSERT ... ON CONFLICT DO NOTHING` for concurrency-safe writes.
- Respects `read_only` and `min_cache_size_bytes` write guards (same semantics as `SqliteHashCacher`).
- `enable_file_hash_caching()` extended with a `conninfo` parameter to activate Postgres caching.
- `PostgresHashCacher` exported from `orcapod.hashing` under a `try/except ImportError` guard.
- Tests against a real Postgres via `testcontainers` covering hit/miss, concurrency, read-only, threshold, and SQLite parity.
- `psycopg` remains an optional dependency (`orcapod[postgresql]`); users on SQLite pay nothing.

---

## Design

### 1. New module: `src/orcapod/hashing/postgres_hash_cacher.py`

`PostgresHashCacher` is a close structural mirror of `SqliteHashCacher`:

```python
class PostgresHashCacher:
    def __init__(
        self,
        conninfo: str,
        *,
        read_only: bool = False,
        min_cache_size_bytes: int | None = None,
    ) -> None:
```

- **`conninfo`** — psycopg3 connection string, e.g.
  `"postgresql://user:pass@host:5432/dbname"` or `"host=... dbname=... user=..."`.
- **`read_only`** / **`min_cache_size_bytes`** — identical semantics to `SqliteHashCacher`.
- Raises `ImportError` with a helpful install hint if `psycopg` is not available.
- Raises `ValueError` if `min_cache_size_bytes` is negative.
- Calls `_ensure_schema()` in `__init__` using a one-shot connection.

**Connection management:** `threading.local()` — each thread opens its own
`psycopg.connect(conninfo)` on first use, same pattern as `SqliteHashCacher`.

**Methods:** `get()`, `put()`, `clear()`, `close()`, `__enter__`/`__exit__`, `__repr__`.

### 2. PostgreSQL schema

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

- `PRIMARY KEY (path, mtime_ns, size)` creates a B-tree index — fast lookup with no
  separate `CREATE INDEX` needed.
- `BIGINT` covers all realistic `mtime_ns` and `size` values.
- `BYTEA` matches the `{method}:{raw_digest}` blob format used by `SqliteHashCacher`.
- Table name `file_hash_cache` is the same as the SQLite version (they live in separate
  databases, so there is no conflict).

### 3. Concurrency-safe insert

```sql
INSERT INTO file_hash_cache (path, mtime_ns, size, hash)
VALUES (%s, %s, %s, %s)
ON CONFLICT (path, mtime_ns, size) DO NOTHING
```

Two workers racing to insert the same key: exactly one row lands, the other's insert
silently completes with zero rows affected — no error, no duplicate. The miss → compute →
insert pattern is safe even if another worker inserts between the miss and the insert.

Note: unlike `SqliteHashCacher` which uses `INSERT OR REPLACE` (last-writer wins),
`PostgresHashCacher` uses `DO NOTHING` (first-writer wins). For a hash cache the value
for any given key is always the same deterministic hash, so either policy is correct.

### 4. `put()` write-guard order

Identical to `SqliteHashCacher`:

1. `read_only=True` → return immediately (no write, no error).
2. `min_cache_size_bytes` truthy and `key.size < min_cache_size_bytes` → return immediately.
3. Otherwise → execute `INSERT ... ON CONFLICT DO NOTHING`.

`get()` is unaffected by either guard.

### 5. `__repr__`

```python
def __repr__(self) -> str:
    return (
        f"PostgresHashCacher("
        f"conninfo={self._conninfo!r}, "
        f"read_only={self._read_only!r}, "
        f"min_cache_size_bytes={self._min_cache_size_bytes!r})"
    )
```

---

## Files Changed

| File | Change |
|---|---|
| `src/orcapod/hashing/postgres_hash_cacher.py` | **New** — `PostgresHashCacher` implementation |
| `tests/test_hashing/test_postgres_hash_cacher.py` | **New** — testcontainers-based tests |
| `src/orcapod/hashing/__init__.py` | Export `PostgresHashCacher` under `try/except ImportError` |
| `src/orcapod/contexts/__init__.py` | Add `conninfo` param to `enable_file_hash_caching()` |
| `pyproject.toml` | Add `testcontainers[postgres]>=4.0.0` to `[dependency-groups] dev` |

---

## `enable_file_hash_caching()` API change

```python
def enable_file_hash_caching(
    db_path: "Path | None" = None,
    *,
    conninfo: str | None = None,
    read_only: bool = False,
    min_cache_size_bytes: int | None = None,
) -> None:
```

- `conninfo` provided, `db_path` absent → use `PostgresHashCacher`.
- `db_path` provided (or both absent) and `conninfo` absent → use `SqliteHashCacher` as today
  (zero behaviour change for existing callers).
- Both `conninfo` and `db_path` provided → raise `ValueError("Provide conninfo or db_path, not both")`.

---

## `__init__.py` export

```python
try:
    from orcapod.hashing.postgres_hash_cacher import PostgresHashCacher
except ImportError:
    PostgresHashCacher = None  # type: ignore[assignment,misc]
```

`PostgresHashCacher` is added to `__all__`. Same pattern as the existing `legacy_core`
try/except block.

---

## Tests

**File:** `tests/test_hashing/test_postgres_hash_cacher.py`

**Infrastructure:**
- `pytest.importorskip("psycopg")` at module level — skips the whole file if psycopg not installed.
- `@pytest.mark.postgres` marker on each test class.
- Module-scoped `pg_conninfo` fixture starts a `PostgresContainer("postgres:16")` once per
  test session to avoid repeated container spin-up.
- Function-scoped `cacher` fixture creates a fresh `PostgresHashCacher` and calls `clear()`
  before each test to prevent cross-test row contamination.

```python
@pytest.fixture(scope="module")
def pg_conninfo():
    with PostgresContainer("postgres:16") as pg:
        # Build a raw psycopg3 conninfo string (not a SQLAlchemy URL)
        yield (
            f"host={pg.get_container_host_ip()} "
            f"port={pg.get_exposed_port(5432)} "
            f"dbname={pg.dbname} "
            f"user={pg.username} "
            f"password={pg.password}"
        )

@pytest.fixture()
def cacher(pg_conninfo):
    c = PostgresHashCacher(pg_conninfo)
    c.clear()
    yield c
    c.close()
```

**Test cases:**

| Test | What it verifies |
|---|---|
| `test_miss_returns_none` | `get()` on empty DB returns `None` |
| `test_put_then_get_returns_hit` | Round-trip: `put()` then `get()` returns same method and digest |
| `test_key_components_are_independent` | Different path / mtime_ns / size → miss |
| `test_persistence_across_instances` | Second `PostgresHashCacher(same_conninfo)` sees rows from first |
| `test_concurrent_insert_no_error_one_row` | Two threads insert same key simultaneously → no exception, exactly one DB row |
| `test_read_only_skips_writes` | `put()` with `read_only=True` → `get()` returns `None` |
| `test_read_only_can_read_preexisting` | Read-only instance `get()`s entries written by writable instance |
| `test_min_cache_size_bytes_small_file_not_cached` | File below threshold → not stored |
| `test_min_cache_size_bytes_boundary_is_inclusive` | File at threshold → stored |
| `test_parity_with_sqlite` | Same key/value → equivalent `ContentHash` from both backends |
| `test_clear_empties_table` | `clear()` → subsequent `get()` returns `None` |
| `test_context_manager_closes_connection` | `__exit__` closes thread-local connection |
| `test_negative_min_cache_size_bytes_raises` | `ValueError` on negative threshold |
| `test_repr_includes_conninfo_and_guards` | `__repr__` shows conninfo, read_only, min_cache_size_bytes |

---

## Dependency changes

**`pyproject.toml` — dev group only:**
```toml
"testcontainers[postgres]>=4.0.0",
```
(The `orcapod[postgresql]` optional extra already declares `psycopg[binary]>=3.0` — no change.)

---

## Documentation update

**File:** `docs/concepts/file-hash-caching.md`

Add a new section **"Choosing a backend: SQLite vs Postgres"** covering:

- **SQLite**: zero-setup, per-machine, single-writer. Use for local development and
  single-machine pipelines.
- **Postgres**: shared, network-accessible, concurrent multi-writer. Use when multiple
  machines or pipeline workers share a cache.
- Minimum supported Postgres version: **14** (for `ON CONFLICT` and `EXTRACT EPOCH` support).
- Example connection config:
  ```python
  op.enable_file_hash_caching(conninfo="postgresql://user:pass@db-host:5432/orcapod_cache")
  ```
- Schema DDL reference (same as the `CREATE TABLE` above).

---

## Scope & Boundaries

In scope:
- `PostgresHashCacher` implementation and tests.
- `enable_file_hash_caching(conninfo=...)` extension.
- `orcapod.hashing.__init__` export.
- `testcontainers[postgres]` dev dep.
- Docs section on backend choice.

Out of scope:
- Data migration between SQLite and Postgres.
- Multi-tenant / per-user table segmentation.
- TTL / eviction / size cap.
- Connection pooling (defer to follow-up).
- IAM / cloud auth schemes.
- Turso / other backends.

---

## Dependencies

- ITL-472 (FileHasher + `CacherProtocol` + `SqliteHashCacher`) — complete, merged.
- ITL-519 (read-only + min_cache_size_bytes write guards) — complete, merged.
