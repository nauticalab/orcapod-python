# SQLiteConnector Design Spec

**Issue:** PLT-1076 — Implement Database abstraction for SQLite
**Date:** 2026-03-23
**Status:** Approved

---

## Overview

Implement `SQLiteConnector`, a concrete implementation of `DBConnectorProtocol`
backed by Python's stdlib `sqlite3`. It enables `ConnectorArrowDatabase` (read/write)
and `DBTableSource` (read-only) to use SQLite without any extra dependencies.

Primary target use cases: local development, CI integration tests, and pipeline
prototyping with `:memory:` databases.

---

## Goals & Success Criteria

- `SQLiteConnector` satisfies `DBConnectorProtocol` (runtime-checkable `isinstance` check passes)
- Supports connection lifecycle: open at construction, close explicitly or via context manager
- Exposes schema introspection via `PRAGMA table_info`: table names, column info, PK columns
- Executes arbitrary SQL queries and streams results as Arrow `RecordBatch` objects
- Handles SQLite type affinity → Arrow type mapping for all five affinities
- Handles Arrow type → SQLite SQL type mapping for `CREATE TABLE`
- Implements upsert with `INSERT OR REPLACE` (overwrite) and `INSERT OR IGNORE` (skip-existing)
- Thread-safe via a single `threading.RLock` over one shared connection
- Unit tests use `:memory:` SQLite; no external service required
- Integration test: full `ConnectorArrowDatabase` round-trip over `:memory:`

---

## Scope & Boundaries

In scope:
- `SQLiteConnector` class in `src/orcapod/databases/sqlite_connector.py`
- Export from `src/orcapod/databases/__init__.py`
- Unit + integration tests in `tests/test_databases/test_sqlite_connector.py`

Out of scope:
- Connection pooling
- WAL mode configuration
- Schema migration / evolution
- Support for network filesystems (documented as unsupported)
- PostgreSQL, SpiralDB connectors (separate issues PLT-1075, PLT-1074)
- Connector factory registry / `from_config` dispatch (noted as follow-on in `ConnectorArrowDatabase.from_config`)

---

## File Layout

```
src/orcapod/databases/
    sqlite_connector.py      # new — SQLiteConnector implementation
    __init__.py              # updated — add SQLiteConnector import + __all__ entry

tests/test_databases/
    test_sqlite_connector.py # new — unit + integration tests
```

No new protocol files. `DBConnectorProtocol` and `ColumnInfo` already exist in
`src/orcapod/protocols/db_connector_protocol.py` and `src/orcapod/types.py`.

---

## Class Interface

```python
class SQLiteConnector:
    """DBConnectorProtocol implementation backed by stdlib sqlite3.

    Holds a single sqlite3.Connection opened at construction time.
    Thread-safe via an internal threading.RLock; not suitable for use
    on network filesystems (NFS, SMB) due to unreliable file locking.

    Args:
        db_path: Path to the SQLite database file, or ":memory:" for an
            in-process in-memory database. Defaults to ":memory:".
    """

    def __init__(self, db_path: str | os.PathLike = ":memory:") -> None: ...

    # Schema introspection
    def get_table_names(self) -> list[str]: ...
    def get_pk_columns(self, table_name: str) -> list[str]: ...
    def get_column_info(self, table_name: str) -> list[ColumnInfo]: ...

    # Reads
    def iter_batches(
        self,
        query: str,
        params: Any = None,
        batch_size: int = 1000,
    ) -> Iterator[pa.RecordBatch]: ...

    # Writes
    def create_table_if_not_exists(
        self,
        table_name: str,
        columns: list[ColumnInfo],
        pk_column: str,
    ) -> None: ...

    def upsert_records(
        self,
        table_name: str,
        records: pa.Table,
        id_column: str,
        skip_existing: bool = False,
    ) -> None: ...

    # Lifecycle
    def close(self) -> None: ...
    def __enter__(self) -> SQLiteConnector: ...
    def __exit__(self, *args: Any) -> None: ...

    # Serialization
    def to_config(self) -> dict[str, Any]: ...

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> SQLiteConnector: ...
```

---

## Type Affinity Mapping

### SQLite declared type → Arrow type

Follows SQLite's official type affinity rules
(https://www.sqlite.org/datatype3.html):

| Declared type contains… | Affinity | Arrow type |
|---|---|---|
| `INT` (anywhere) | INTEGER | `pa.int64()` |
| `CHAR`, `CLOB`, or `TEXT` | TEXT | `pa.large_string()` |
| `BLOB` or empty string | BLOB/NONE | `pa.large_binary()` |
| `REAL`, `FLOA`, or `DOUB` | REAL | `pa.float64()` |
| Anything else (incl. `NUMERIC`, `DECIMAL`) | NUMERIC | `pa.float64()` |

`PRAGMA table_info(t)` `notnull` column → `nullable = not bool(notnull)`.

`get_pk_columns()` filters rows where `pk > 0`, sorted ascending by `pk` value
(preserves multi-column PK declaration order).

### Arrow type → SQLite SQL type (for CREATE TABLE)

| Arrow type family | SQL type |
|---|---|
| int8, int16, int32, int64, uint8, uint16, uint32, uint64 | `INTEGER` |
| float32, float64 | `REAL` |
| utf8, large_utf8, string, large_string | `TEXT` |
| binary, large_binary | `BLOB` |
| bool | `INTEGER` |
| All others | `TEXT` (with `logging.warning`) |

---

## Key Implementation Details

### Connection management

```python
self._db_path = db_path
self._conn = sqlite3.connect(str(db_path), check_same_thread=False)
self._lock = threading.RLock()
```

`check_same_thread=False` disables Python's conservative single-thread guard;
the `RLock` provides correct synchronization above the SQLite layer.

### `get_table_names()`

```sql
SELECT name FROM sqlite_master WHERE type='table' ORDER BY name
```

### `get_pk_columns(table_name)`

```sql
PRAGMA table_info("{table_name}")
```

Filter rows where `pk > 0`, sort by `pk` ascending, return `[row["name"], ...]`.

### `get_column_info(table_name)`

```sql
PRAGMA table_info("{table_name}")
```

Map each row's `type` string through the affinity rules above.
Return `[ColumnInfo(name=row["name"], arrow_type=..., nullable=not row["notnull"]), ...]`.

### `iter_batches(query, params, batch_size)`

1. Execute `cursor.execute(query, params or [])`.
2. Read column names from `cursor.description`.
3. Infer Arrow types: if the query is `SELECT * FROM "table"`, reuse `get_column_info()`
   for exact types; otherwise fall back to affinity rules applied to the declared type
   from `cursor.description` (SQLite returns declared types there).
4. Loop `cursor.fetchmany(batch_size)` until exhausted.
5. For each chunk, build per-column `pa.array(values, type=arrow_type)` and yield
   `pa.RecordBatch.from_arrays(arrays, schema=schema)`.

### `create_table_if_not_exists(table_name, columns, pk_column)`

Emits:
```sql
CREATE TABLE IF NOT EXISTS "{table_name}" (
    "col1" TEXT,
    "col2" INTEGER NOT NULL,
    ...,
    PRIMARY KEY ("pk_col")
)
```

`NOT NULL` is added for columns where `nullable=False`.

### `upsert_records(table_name, records, id_column, skip_existing)`

- `skip_existing=False` → `INSERT OR REPLACE INTO "{table_name}" (...) VALUES (...)`
- `skip_existing=True`  → `INSERT OR IGNORE INTO  "{table_name}" (...) VALUES (...)`

Uses `cursor.executemany()` with rows from `records.to_pylist()`.

Values are converted from Arrow scalars to Python natives via `.as_py()` when
iterating; `pa.Table.to_pylist()` already does this.

### `close()`

```python
with self._lock:
    self._conn.close()
```

Idempotent: a second `close()` call on an already-closed SQLite connection raises
`ProgrammingError`; wrap with a try/except and swallow if already closed.

### `to_config()` / `from_config()`

```python
def to_config(self) -> dict[str, Any]:
    return {"connector_type": "sqlite", "db_path": str(self._db_path)}

@classmethod
def from_config(cls, config: dict[str, Any]) -> SQLiteConnector:
    return cls(db_path=config["db_path"])
```

---

## Test Plan

All tests use `SQLiteConnector(":memory:")`. No external service required.

| # | Section | What's tested |
|---|---|---|
| 1 | Protocol conformance | `isinstance(connector, DBConnectorProtocol)` passes |
| 2 | `get_table_names` | Empty DB → `[]`; after `CREATE TABLE` → name appears |
| 3 | `get_pk_columns` | Single PK; composite PK (order preserved); no PK → `[]` |
| 4 | `get_column_info` | All five SQLite affinities map to correct Arrow types; `nullable` from `notnull` |
| 5 | `iter_batches` | Empty table → no batches; single batch; multi-batch (batch_size=2); parameterized query |
| 6 | `create_table_if_not_exists` | Idempotent (second call is no-op); correct PK constraint; column types correct |
| 7 | `upsert_records` | `skip_existing=False` overwrites existing row; `skip_existing=True` ignores duplicate |
| 8 | Lifecycle | Context manager calls `close()`; double-close is safe |
| 9 | `to_config` / `from_config` | Round-trips `db_path`; `connector_type` key is `"sqlite"` |
| 10 | Integration with `ConnectorArrowDatabase` | `add_record → flush → get_record_by_id` round-trip using `:memory:` |

---

## Thread-Safety Note (for docstring)

`SQLiteConnector` is thread-safe for the common use case of multiple threads sharing
one connector instance. All operations are serialized through a `threading.RLock`.

`SQLiteConnector` is **not** suitable for use on network filesystems (NFS, SMB/CIFS).
SQLite's file-level locking relies on POSIX advisory locks, which network filesystems
frequently implement incorrectly, risking silent database corruption.
