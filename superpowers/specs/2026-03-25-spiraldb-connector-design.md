# SpiralDBConnector Design Spec

**Issue:** PLT-1074 — Implement Database abstraction for SpiralDB
**Date:** 2026-03-25
**Status:** Approved

---

## Overview

Implement `SpiralDBConnector`, a concrete implementation of `DBConnectorProtocol`
backed by [pyspiral](https://pypi.org/project/pyspiral/) (`spiral` module, `>=0.11.6`).

SpiralDB is Arrow-native and expression-based (no SQL). The connector bridges
`DBConnectorProtocol`'s SQL-style interface to Spiral's scan/write API, hiding
all `pyspiral` internals from `ConnectorArrowDatabase` and `DBTableSource`.

The connector is **dataset-scoped**: a fixed `(project_id, dataset)` pair is set
at construction time. All table names are plain strings within that dataset. A
follow-on issue (PLT-1167) covers a dataset-routing variant where the first
`record_path` component maps to the dataset.

---

## Goals & Success Criteria

- `SpiralDBConnector` satisfies `DBConnectorProtocol` (`isinstance` check passes)
- Supports connection lifecycle: `close()` and context manager
- Exposes schema introspection: `get_table_names()`, `get_pk_columns()`, `get_column_info()`
- Executes full-table scans via `iter_batches()`, yielding Arrow `RecordBatch` objects
- `create_table_if_not_exists()` creates a Spiral table with the correct key schema, idempotently
- `upsert_records()` writes via `table.write()` (always upsert-by-key); `skip_existing=True` uses a scan+filter approximation
- `to_config()` / `from_config()` round-trip `project_id`, `dataset`, `overrides`
- Unit tests use mocked `Project`/`Table` objects; no network access required
- Integration tests use dev project `test-orcapod-362211` and are skipped when credentials are absent

---

## Scope & Boundaries

In scope:
- `SpiralDBConnector` class in `src/orcapod/databases/spiraldb_connector.py`
- Export from `src/orcapod/databases/__init__.py`
- Unit tests in `tests/test_databases/test_spiraldb_connector.py`
- Integration tests in `tests/test_databases/test_spiraldb_connector_integration.py`
- `pyspiral>=0.11.6` already added as optional extra `orcapod[spiraldb]` in `pyproject.toml` (PLT-1163)

Out of scope:
- Dataset-routing connector (PLT-1167)
- `PostgreSQLConnector` (PLT-1075)
- Full SQL query support beyond `SELECT * FROM "table"` patterns
- Connector factory registry / `build_db_connector_from_config` (follow-on)
- Schema evolution / `ALTER TABLE` equivalent

---

## File Layout

```
src/orcapod/databases/
    spiraldb_connector.py      # new — SpiralDBConnector
    __init__.py                # updated — add SpiralDBConnector import + __all__ entry

tests/test_databases/
    test_spiraldb_connector.py              # new — unit tests (mocked)
    test_spiraldb_connector_integration.py  # new — integration tests (live dev project)
```

---

## Module Docstring

```python
"""SpiralDBConnector — DBConnectorProtocol implementation backed by SpiralDB (pyspiral).

Requires the ``spiraldb`` optional extra: ``pip install orcapod[spiraldb]``.
Authentication is handled externally via the ``spiral login`` CLI command,
which stores credentials in ``~/.config/pyspiral/auth.json``.

The connector is dataset-scoped: all tables are read from and written to
a single ``(project_id, dataset)`` pair.

Example::

    connector = SpiralDBConnector(project_id="my-project-123456", dataset="default")
    db = ConnectorArrowDatabase(connector)
    db.add_record(("results", "my_fn"), record_id="abc", record=table)
    db.flush()
"""
```

---

## Class Interface

```python
class SpiralDBConnector:
    """DBConnectorProtocol implementation backed by SpiralDB (pyspiral).

    Scoped to a single dataset within a SpiralDB project. Auth is handled
    externally — run ``spiral login`` once to store credentials in
    ``~/.config/pyspiral/auth.json``.

    Args:
        project_id: SpiralDB project identifier (e.g. ``"my-project-123456"``).
        dataset: Dataset within the project. Defaults to ``"default"``.
        overrides: Optional pyspiral client config overrides, e.g.
            ``{"server.url": "http://api.spiraldb.dev"}`` for the dev
            environment. See the pyspiral config docs for full options.
    """

    def __init__(
        self,
        project_id: str,
        dataset: str = "default",
        overrides: dict[str, str] | None = None,
    ) -> None: ...

    # Schema introspection
    def get_table_names(self) -> list[str]: ...
    def get_pk_columns(self, table_name: str) -> list[str]: ...
    def get_column_info(self, table_name: str) -> list[ColumnInfo]: ...

    # Read
    def iter_batches(
        self,
        query: str,
        params: Any = None,
        batch_size: int = 1000,
    ) -> Iterator[pa.RecordBatch]: ...

    # Write
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
    def __enter__(self) -> SpiralDBConnector: ...
    def __exit__(self, *args: Any) -> None: ...

    # Serialization
    def to_config(self) -> dict[str, Any]: ...

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> SpiralDBConnector: ...
```

---

## Key Implementation Details

### Imports and lazy loading

`spiral` is imported lazily via `LazyModule("spiral")` at module level (same
pattern as `pyarrow` elsewhere in the codebase), so `import orcapod` does not
fail when `pyspiral` is not installed. The real `import spiral` occurs only when
`SpiralDBConnector` is instantiated.

```python
if TYPE_CHECKING:
    import spiral as sp
else:
    sp = LazyModule("spiral")
```

### Connection lifecycle

```python
def __init__(self, project_id, dataset="default", overrides=None):
    self._project_id = project_id
    self._dataset = dataset
    self._overrides = overrides
    self._spiral = sp.Spiral(overrides=overrides)
    self._project = self._spiral.project(project_id)
    self._closed = False
```

`sp.Spiral()` is lazy — no network call occurs at construction. The first
network call happens on the first schema introspection or scan operation.

`close()` sets `self._closed = True`. There is no socket to tear down. It is
idempotent. All public methods begin with `self._require_open()`:

```python
def _require_open(self) -> None:
    if self._closed:
        raise RuntimeError("SpiralDBConnector is closed")
```

### Table identifier helper

All methods that address a Spiral table use the qualified `"dataset.table_name"`
identifier:

```python
def _table_id(self, table_name: str) -> str:
    return f"{self._dataset}.{table_name}"
```

### `get_table_names()`

```python
def get_table_names(self) -> list[str]:
    self._require_open()
    resources = self._project.list_tables()
    return sorted(r.table for r in resources if r.dataset == self._dataset)
```

`TableResource.id` (an opaque handle like `table_g4o78g`) is not exposed.

### `get_pk_columns(table_name)`

```python
def get_pk_columns(self, table_name: str) -> list[str]:
    self._require_open()
    return list(self._project.table(self._table_id(table_name)).key_schema.names)
```

Composite primary keys (up to 4 columns observed in production data) are
returned in declaration order. Returns `[]` if the table has no key schema.

### `get_column_info(table_name)`

```python
def get_column_info(self, table_name: str) -> list[ColumnInfo]:
    self._require_open()
    arrow_schema = self._project.table(self._table_id(table_name)).schema().to_arrow()
    return [
        ColumnInfo(name=field.name, arrow_type=field.type, nullable=field.nullable)
        for field in arrow_schema
    ]
```

**Timezone note:** SpiralDB silently drops timezone information from
`timestamp` columns (e.g., `timestamp[us, tz=UTC]` is returned as
`timestamp[us]`). This is a known SpiralDB limitation and is documented in the
method docstring. No workaround is applied.

### `iter_batches(query, params, batch_size)`

SpiralDB has no SQL engine. The connector parses the table name from the query
string using a regex (identical to `SQLiteConnector`'s approach), then
performs a full scan.

```python
def iter_batches(self, query, params=None, batch_size=1000):
    self._require_open()
    table_name = _parse_table_name(query)
    tbl = self._project.table(self._table_id(table_name))
    reader = self._spiral.scan(tbl.select()).to_record_batches(batch_size=batch_size)
    yield from reader
```

`_parse_table_name` is a module-level helper:

```python
def _parse_table_name(query: str) -> str:
    import re
    m = re.search(r'FROM\s+"([^"]+)"', query, re.IGNORECASE)
    if not m:
        m = re.search(r'FROM\s+(\w+)', query, re.IGNORECASE)
    if not m:
        raise ValueError(f"Cannot parse table name from query: {query!r}")
    return m.group(1)
```

`params` is accepted for protocol compliance but unused (Spiral has no
parameterised query interface). `batch_size` is passed to
`to_record_batches()`; the actual batch sizing is controlled by the Spiral
execution engine.

### `create_table_if_not_exists(table_name, columns, pk_column)`

Spiral requires a `key_schema` at table creation time. Only the `pk_column`
is placed in the key schema; value columns are inferred by Spiral from the
first write.

```python
def create_table_if_not_exists(self, table_name, columns, pk_column):
    self._require_open()
    col_names = [c.name for c in columns]
    if pk_column not in col_names:
        raise ValueError(
            f"pk_column {pk_column!r} not found in columns: {col_names}"
        )
    pk_arrow_type = next(c.arrow_type for c in columns if c.name == pk_column)
    self._project.create_table(
        self._table_id(table_name),
        key_schema=[(pk_column, pk_arrow_type)],
        exist_ok=True,
    )
```

`exist_ok=True` makes the call idempotent — a second call with the same table
name is a no-op (Spiral returns 409 Conflict without `exist_ok`).

### `upsert_records(table_name, records, id_column, skip_existing)`

**`skip_existing=False` (default, overwrite):**

```python
tbl = self._project.table(self._table_id(table_name))
tbl.write(records)
```

`table.write()` is always upsert-by-key in SpiralDB: rows whose key columns
match existing rows are overwritten; rows with novel keys are inserted.

**`skip_existing=True` (skip if key already exists):**

SpiralDB has no native skip-existing equivalent. The approximation:
1. Full scan to retrieve all existing rows (key-only scans are not supported).
2. Client-side filter to keep only rows with novel key tuples.
3. Write the filtered batch (no-op if empty).

```python
tbl = self._project.table(self._table_id(table_name))
pk_cols = list(tbl.key_schema.names)
existing = self._spiral.scan(tbl.select()).to_table()
existing_keys = {
    tuple(row[k] for k in pk_cols)
    for row in existing.to_pylist()
}
mask = pa.array([
    tuple(row[k] for k in pk_cols) not in existing_keys
    for row in records.to_pylist()
])
novel = records.filter(mask)
if len(novel) > 0:
    tbl.write(novel)
```

**Performance caveat (documented in docstring):** `skip_existing=True` reads
the entire table into memory to build the key set. Use `skip_existing=False`
(the default) for large tables.

### `to_config()` / `from_config()`

```python
def to_config(self) -> dict[str, Any]:
    return {
        "connector_type": "spiraldb",
        "project_id": self._project_id,
        "dataset": self._dataset,
        "overrides": self._overrides,
    }

@classmethod
def from_config(cls, config: dict[str, Any]) -> SpiralDBConnector:
    if config.get("connector_type") != "spiraldb":
        raise ValueError(
            f"Expected connector_type 'spiraldb', got {config.get('connector_type')!r}"
        )
    return cls(
        project_id=config["project_id"],
        dataset=config.get("dataset", "default"),
        overrides=config.get("overrides"),
    )
```

---

## Type Notes

SpiralDB is Arrow-native. `table.schema().to_arrow()` and scan results
(`scan.to_table()` / `scan.to_record_batches()`) return identical Arrow types.
No secondary type-mapping layer is needed.

| Vortex internal type                           | Arrow type from `schema().to_arrow()` |
|------------------------------------------------|---------------------------------------|
| `utf8?`                                        | `string`                              |
| `i16? / i32? / i64?`                           | `int16 / int32 / int64`               |
| `u16? / u32?`                                  | `uint16 / uint32`                     |
| `f32?`                                         | `float32`                             |
| `f64?`                                         | `float64`                             |
| `bool?`                                        | `bool_`                               |
| `list(T?)?`                                    | `list<item: T>`                       |
| `fixed_size_list(T?)[N]?`                      | `fixed_size_list<item: T>[N]`         |
| `vortex.timestamp[us, tz=America/Los_Angeles]` | `timestamp[us]` ⚠️ **tz dropped**    |
| struct                                         | Arrow struct                          |

---

## Test Plan

### Unit tests (`test_spiraldb_connector.py`)

All tests use a `MockProject` / `MockTable` defined inline. No network access.

| # | Section | What's tested |
|---|---|---|
| 1 | Protocol conformance | `isinstance(connector, DBConnectorProtocol)` passes |
| 2 | `get_table_names` | Returns sorted plain names; filters to correct dataset; excludes tables in other datasets |
| 3 | `get_pk_columns` | Single PK; composite PK; empty list for table with no key |
| 4 | `get_column_info` | Arrow types pass through unchanged; `nullable` from Arrow field; returns `[]` for non-existent table |
| 5 | `iter_batches` | Full table scan; double-quoted table name parsed correctly; unquoted fallback; `params` accepted but unused |
| 6 | `create_table_if_not_exists` | Idempotent (`exist_ok=True` always passed); correct `key_schema` tuple passed; raises `ValueError` if `pk_column` not in `columns` |
| 7 | `upsert_records(skip_existing=False)` | `table.write()` called with full records |
| 8 | `upsert_records(skip_existing=True)` | Existing keys filtered; only novel rows written; no-op write skipped when all rows exist |
| 9 | Lifecycle | Context manager calls `close()`; double-close is safe; all methods raise `RuntimeError` after `close()` |
| 10 | `to_config` / `from_config` | Round-trips `project_id`, `dataset`, `overrides`; `connector_type` is `"spiraldb"`; `from_config` raises on wrong `connector_type` |

### Integration tests (`test_spiraldb_connector_integration.py`)

Skipped unless `SPIRAL_INTEGRATION_TESTS=1` env var is set. Uses dev project
`test-orcapod-362211` (hits `api.spiraldb.dev`).

- Full round-trip: create table → write records → scan → verify values
- `skip_existing=True`: write once, write again with overlapping keys, verify no duplication
- `ConnectorArrowDatabase` round-trip: `add_record` → `flush` → `get_record_by_id`

---

## `__init__.py` changes

```python
# add to imports and __all__
from .spiraldb_connector import SpiralDBConnector

__all__ = [
    "ConnectorArrowDatabase",
    "DeltaTableDatabase",
    "InMemoryArrowDatabase",
    "NoOpArrowDatabase",
    "SpiralDBConnector",   # new
    "SQLiteConnector",
]
```

The inline comment in `__init__.py` pointing to PLT-1074 for `SpiralDBConnector`
is replaced with the actual import.
