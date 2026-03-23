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
- Handles SQLite type affinity → Arrow type mapping for all five affinities, plus `BOOLEAN`
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
- Connector factory registry / `from_config` dispatch (noted as follow-on in
  `ConnectorArrowDatabase.from_config`)

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

## Module Docstring Template

`sqlite_connector.py` opens with:

```python
"""SQLiteConnector — DBConnectorProtocol implementation backed by stdlib sqlite3.

Zero extra dependencies (sqlite3 is stdlib). Intended for local development,
CI integration tests, and pipeline prototyping. Not suitable for use on network
filesystems (NFS, SMB/CIFS) due to unreliable file locking.

Example::

    connector = SQLiteConnector(":memory:")
    db = ConnectorArrowDatabase(connector)
    db.add_record(("results", "my_fn"), record_id="abc", record=table)
    db.flush()
"""
```

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
    def __enter__(self) -> SQLiteConnector: ...  # covariant narrowing — passes protocol check
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
(https://www.sqlite.org/datatype3.html), with one explicit addition for
`BOOLEAN` to ensure round-trip fidelity:

| Declared type (uppercase, checked in order) | Arrow type |
|---|---|
| Exactly `BOOLEAN` | `pa.bool_()` |
| Contains `INT` | `pa.int64()` |
| Contains `CHAR`, `CLOB`, or `TEXT` | `pa.large_string()` |
| Contains `BLOB`, or is empty string | `pa.large_binary()` |
| Contains `REAL`, `FLOA`, or `DOUB` | `pa.float64()` |
| Anything else (incl. `NUMERIC`, `DECIMAL`) | `pa.float64()` |

**Why `BOOLEAN` is special:** SQLite assigns `NUMERIC` affinity to `BOOLEAN`
(none of the affinity keywords appear in it). Without explicit handling, an
Arrow `bool` column would write as `BOOLEAN` SQL type but read back as
`pa.float64()`, breaking `ConnectorArrowDatabase`'s schema-validation check on
the second flush. Checking for `BOOLEAN` before the general affinity waterfall
ensures `bool → BOOLEAN → bool` round-trips correctly.

`PRAGMA table_info(t)` fields used:
- `name` → `ColumnInfo.name`
- `type` → mapped via affinity rules above → `ColumnInfo.arrow_type`
- `notnull` → `ColumnInfo.nullable = not bool(notnull)`
- `pk` → used by `get_pk_columns()`: filter `pk > 0`, sort ascending by `pk`
  value, yield `[row["name"] for row in sorted_rows]`

### Arrow type → SQLite SQL type (for `CREATE TABLE`)

| Arrow type family | SQL type |
|---|---|
| int8, int16, int32, int64, uint8, uint16, uint32, uint64 | `INTEGER` |
| float32, float64 | `REAL` |
| utf8, large_utf8 / string, large_string | `TEXT` |
| binary, large_binary | `BLOB` |
| bool | `BOOLEAN` |
| All others | `TEXT` (with `logging.warning`) |

---

## Key Implementation Details

### Connection management

```python
self._db_path = db_path
self._conn: sqlite3.Connection | None = sqlite3.connect(
    str(db_path), check_same_thread=False, isolation_level=None  # autocommit
)
self._conn.row_factory = sqlite3.Row
self._lock = threading.RLock()
```

`check_same_thread=False` disables Python's conservative single-thread guard;
the `RLock` provides correct synchronization above the SQLite layer.
`isolation_level=None` enables SQLite autocommit mode — every DDL and DML
statement is committed immediately. This ensures writes are durable on
file-based databases without explicit `conn.commit()` calls in write methods.
`self._conn` is set to `None` on `close()` to prevent use-after-close.

### `get_table_names()`

```sql
SELECT name FROM sqlite_master WHERE type='table' ORDER BY name
```

Uses `type='table'` to exclude views and other objects.

### `get_pk_columns(table_name)`

```sql
PRAGMA table_info("table_name")
```

```python
rows = [r for r in cursor if r["pk"] > 0]
return [r["name"] for r in sorted(rows, key=lambda r: r["pk"])]
```

Uses `conn.row_factory = sqlite3.Row` so columns are accessible by name.

### `get_column_info(table_name)`

```sql
PRAGMA table_info("table_name")
```

```python
return [
    ColumnInfo(
        name=row["name"],
        arrow_type=_sqlite_type_to_arrow(row["type"]),
        nullable=not bool(row["notnull"]),
    )
    for row in cursor
]
```

Returns `[]` for an empty table (zero rows from PRAGMA) without raising.

### `iter_batches(query, params, batch_size)`

Arrow types for the result columns are resolved by calling `get_column_info()`
on the relevant table. Column names are read from `cursor.description[i][0]`
after execution. Type resolution strategy:

- Parse the table name from the query using a simple regex on the first
  `FROM "table_name"` occurrence.
- Call `get_column_info(table_name)` and build a `{col_name: arrow_type}` lookup.
- For each column in `cursor.description`, look up the Arrow type; fall back to
  `pa.large_string()` (with `logging.warning`) for columns not found in the
  lookup (e.g., computed columns in non-`SELECT *` queries).

**Note:** `cursor.description[i][1]` (the type_code field) is always `None` in
Python's `sqlite3` module — there are no declared types available from the
cursor itself for arbitrary queries. The `get_column_info()` lookup is the only
reliable type source.

Batching loop:

```python
cursor.execute(query, params or [])
# build schema from column name lookup …
while chunk := cursor.fetchmany(batch_size):
    arrays = [
        pa.array(_coerce_column(raw_col, arrow_type), type=arrow_type)
        for raw_col, arrow_type in zip(
            ([row[i] for row in chunk] for i in range(len(arrow_types))),
            arrow_types,
        )
    ]
    yield pa.RecordBatch.from_arrays(arrays, schema=schema)
```

**`BOOLEAN` coercion:** SQLite stores boolean values as integers (`1`/`0`).
Python's `sqlite3` returns them as `int`, and `pa.array([1, 0], type=pa.bool_())`
raises `ArrowInvalid`. The `_coerce_column` helper must convert `int` values to
`bool` when the target Arrow type is `pa.bool_()`:

```python
def _coerce_column(values: list, arrow_type: pa.DataType) -> list:
    if arrow_type == pa.bool_():
        return [bool(v) if v is not None else None for v in values]
    return values
```

`None` values in any column are handled correctly by `pa.array()` (they become
Arrow null values).

### `create_table_if_not_exists(table_name, columns, pk_column)`

All table and column identifiers are double-quoted throughout. Example output:

```sql
CREATE TABLE IF NOT EXISTS "my_table" (
    "__record_id" TEXT NOT NULL,
    "value" REAL,
    PRIMARY KEY ("__record_id")
)
```

`NOT NULL` is emitted for columns where `ColumnInfo.nullable = False`.

### `upsert_records(table_name, records, id_column, skip_existing)`

`id_column` is accepted to satisfy the protocol signature. Conflict detection is
handled at the SQL level by the table's `PRIMARY KEY` constraint (set to
`id_column` by `create_table_if_not_exists`), so `id_column` is not referenced
in the generated SQL body.

Named-parameter syntax (`:col_name`) is used so that rows from
`records.to_pylist()` (which produces `list[dict]`) bind correctly:

```python
cols = records.schema.names
placeholders = ", ".join(f":{c}" for c in cols)
col_list = ", ".join(f'"{c}"' for c in cols)
verb = "INSERT OR IGNORE" if skip_existing else "INSERT OR REPLACE"
sql = f'{verb} INTO "{table_name}" ({col_list}) VALUES ({placeholders})'
cursor.executemany(sql, records.to_pylist())
```

### `close()`

```python
def close(self) -> None:
    with self._lock:
        if self._conn is not None:
            self._conn.close()
            self._conn = None
```

Idempotent: the `self._conn is not None` guard makes a second `close()` call a
no-op. All other methods should check `self._conn is not None` and raise
`RuntimeError("SQLiteConnector is closed")` if called after `close()`.

### `to_config()` / `from_config()`

```python
def to_config(self) -> dict[str, Any]:
    return {"connector_type": "sqlite", "db_path": str(self._db_path)}

@classmethod
def from_config(cls, config: dict[str, Any]) -> SQLiteConnector:
    if config.get("connector_type") != "sqlite":
        raise ValueError(
            f"Expected connector_type 'sqlite', got {config.get('connector_type')!r}"
        )
    return cls(db_path=config["db_path"])
```

---

## Test Plan

All tests use `SQLiteConnector(":memory:")`. No external service required.

| # | Section | What's tested |
|---|---|---|
| 1 | Protocol conformance | `isinstance(connector, DBConnectorProtocol)` passes |
| 2 | `get_table_names` | Empty DB → `[]`; after `CREATE TABLE` → name present; views excluded |
| 3 | `get_pk_columns` | Single PK; composite PK (declaration order preserved); no PK → `[]` |
| 4 | `get_column_info` | All five SQLite affinities + `BOOLEAN` map to correct Arrow types; `nullable` from `notnull`; table with zero rows still returns full column metadata; non-existent table returns `[]` |
| 5 | `iter_batches` | Empty table → no batches; single batch; multi-batch (`batch_size=2`); parameterized query; `NULL` values in all column types; schema Arrow types match `get_column_info()` for all affinities |
| 6 | `create_table_if_not_exists` | Idempotent (second call is no-op); PK constraint present; column types match declared SQL types; column names are double-quoted |
| 7 | `upsert_records` | `skip_existing=False` overwrites existing row; `skip_existing=True` ignores duplicate; column order in Arrow table differs from SQL table order |
| 8 | Lifecycle | Context manager calls `close()`; double-close is safe; methods raise after `close()` |
| 9 | `to_config` / `from_config` | Round-trips `db_path`; `connector_type` key is `"sqlite"`; `from_config` raises on wrong `connector_type` |
| 10 | Integration with `ConnectorArrowDatabase` | Multiple `add_record` calls → `flush` → `get_record_by_id` all succeed; second flush to same table (exercises schema-validation path); `skip_duplicates=True` at the SQL level |

---

## Thread-Safety Note (for docstring)

`SQLiteConnector` is thread-safe for the common use case of multiple threads
sharing one connector instance. All operations are serialized through a
`threading.RLock`.

`SQLiteConnector` is **not** suitable for use on network filesystems (NFS,
SMB/CIFS). SQLite's file-level locking relies on POSIX advisory locks, which
network filesystems frequently implement incorrectly, risking silent database
corruption.
