# DBConnector: Shared Database Access Abstraction

**Issue:** PLT-1078
**Date:** 2026-03-21
**Status:** Approved

---

## Problem

With three `ArrowDatabaseProtocol` backends (SQLite, PostgreSQL, SpiralDB) and three corresponding `Source` implementations being built in parallel (PLT-1072–1077), each pair shares the same underlying DB technology. Without a shared abstraction, every contributor would need to implement:

- Connection lifecycle management
- DB-native → Arrow type mapping
- Schema introspection (list tables, get primary keys, get column metadata)
- Query execution

…twice: once for the `ArrowDatabaseProtocol` implementation, once for the `Source`.

This compounds design friction and creates diverging implementations of the same low-level logic.

---

## Design

### Layering

```
┌──────────────────────────────────────────────────────────┐
│                    User-facing layer                      │
│  ArrowDatabaseProtocol (read+write)   RootSource (read)  │
└──────────────────┬──────────────────────────┬────────────┘
                   │                          │
     ┌─────────────▼──────────────────────────▼──────────┐
     │   ConnectorArrowDatabase    DBTableSource          │
     │   (generic ArrowDB impl)    (generic Source impl)  │
     └─────────────────────────┬─────────────────────────┘
                               │ shared dependency
               ┌───────────────▼───────────────────┐
               │        DBConnectorProtocol          │
               │  (connection + type mapping +       │
               │   schema introspection + queries)   │
               └──────┬────────────┬────────────────┘
                      │            │            │
              SQLiteConnector  PostgreSQLConnector  SpiralDBConnector
              (PLT-1076)       (PLT-1075)           (PLT-1074)
```

**`DBConnectorProtocol`** — the minimal shared raw-access interface every DB technology must implement.

**`ConnectorArrowDatabase(connector)`** — generic `ArrowDatabaseProtocol` implementation on top of any `DBConnectorProtocol`. Owns all record-management logic: `record_path → table name` mapping, `__record_id` column convention, in-memory pending batch management, deduplication, upsert, schema evolution, flush semantics.

**`DBTableSource(connector, table_name)`** — generic read-only `RootSource` on top of any `DBConnectorProtocol`. Uses PK columns as default tag columns, delegates all data fetching and type mapping to the connector, feeds Arrow data into `SourceStreamBuilder`.

Contributors implementing PLT-1074/1075/1076 implement **one class** (`DBConnector`) and get both `ArrowDatabase` and `Source` support.

---

## `DBConnectorProtocol` Interface

```python
@dataclass(frozen=True)
class ColumnInfo:
    """A single column with its Arrow-mapped type. Type mapping is the connector's responsibility."""
    name: str
    arrow_type: pa.DataType
    nullable: bool = True


@runtime_checkable
class DBConnectorProtocol(Protocol):
    # ── Schema introspection (used by both Source and ArrowDatabase) ──────────
    def get_table_names(self) -> list[str]: ...
    def get_pk_columns(self, table_name: str) -> list[str]: ...
    def get_column_info(self, table_name: str) -> list[ColumnInfo]: ...

    # ── Read (used by both Source and ArrowDatabase) ──────────────────────────
    def iter_batches(
        self,
        query: str,
        params: Any = None,
        batch_size: int = 1000,
    ) -> Iterator[pa.RecordBatch]: ...

    # ── Write (used only by ConnectorArrowDatabase) ───────────────────────────
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

    # ── Lifecycle ─────────────────────────────────────────────────────────────
    def close(self) -> None: ...
    def __enter__(self) -> "DBConnectorProtocol": ...
    def __exit__(self, *args: Any) -> None: ...

    # ── Serialization ─────────────────────────────────────────────────────────
    def to_config(self) -> dict[str, Any]: ...
    @classmethod
    def from_config(cls, config: dict[str, Any]) -> "DBConnectorProtocol": ...
```

**Type mapping is fully owned by the connector.** `iter_batches()` returns Arrow-typed `RecordBatch`es. `get_column_info()` returns `ColumnInfo` with `arrow_type` already mapped. `upsert_records()` accepts an Arrow table and the connector handles conversion to DB-native types internally. `ConnectorArrowDatabase` and `DBTableSource` are completely DB-type-agnostic.

**Write methods** (`create_table_if_not_exists`, `upsert_records`) are on the protocol but are only called by `ConnectorArrowDatabase`. `DBTableSource` never calls them. This avoids the need for a separate `ReadableDBConnectorProtocol` split while keeping the interface intentionally minimal.

---

## `ConnectorArrowDatabase`

Implements `ArrowDatabaseProtocol` on top of any `DBConnectorProtocol`. Mirrors the pending-batch + flush semantics of `DeltaTableDatabase` and `InMemoryArrowDatabase`:

- **`record_path → table_name`**: `"__".join(record_path)` with validation (max depth, safe characters). Uses `__` as separator to avoid collisions with path components.
- **`__record_id` column**: Standard column added to every table, used as the primary key in the underlying DB table.
- **Pending batch**: Records are buffered in memory as Arrow tables, keyed by `record_path`. `flush()` commits all pending batches via `connector.upsert_records()`.
- **Deduplication**: Within-batch deduplication keeps the last occurrence per `__record_id`.
- **`skip_duplicates`**: Passes through to `connector.upsert_records(skip_existing=...)`.
- **Schema evolution**: `connector.create_table_if_not_exists()` is called at flush time if the table does not yet exist. For schema changes on existing tables, evolution is delegated to the connector.

### `record_path → table_name` mapping

```python
def _path_to_table_name(record_path: tuple[str, ...]) -> str:
    # Joins with '__' separator; sanitizes each component (replaces non-alphanumeric with '_')
    return "__".join(re.sub(r"[^a-zA-Z0-9_]", "_", part) for part in record_path)
```

---

## `DBTableSource`

Implements `RootSource` on top of any `DBConnectorProtocol`. Read-only.

```python
DBTableSource(
    connector: DBConnectorProtocol,
    table_name: str,
    tag_columns: Collection[str] | None = None,   # None → use PK columns
    record_id_column: str | None = None,
    source_id: str | None = None,                 # None → defaults to table_name
    **kwargs,  # passed to RootSource (label, data_context, config)
)
```

Construction flow:
1. Resolve `tag_columns`: if `None`, call `connector.get_pk_columns(table_name)`.
2. Fetch full table: `list(connector.iter_batches(f'SELECT * FROM "{table_name}"'))` → `pa.Table.from_batches(...)`.
3. Feed into `SourceStreamBuilder.build(table, tag_columns=..., source_id=..., record_id_column=...)`.
4. Store result stream, tag_columns, source_id.

`to_config()` serializes `connector.to_config()`, `table_name`, `tag_columns`, `record_id_column`, `source_id` plus identity fields from `_identity_config()`.

`from_config()` calls a `build_db_connector_from_config(config["connector"])` registry helper (to be implemented alongside connectors in PLT-1074/1075/1076).

---

## Module Layout

```
src/orcapod/
├── protocols/
│   ├── database_protocols.py          # existing ArrowDatabaseProtocol (unchanged)
│   └── db_connector_protocol.py       # NEW: ColumnInfo, DBConnectorProtocol
├── databases/
│   ├── __init__.py                    # updated: export ConnectorArrowDatabase
│   ├── connector_arrow_database.py    # NEW: ConnectorArrowDatabase
│   ├── delta_lake_databases.py        # existing (unchanged)
│   ├── in_memory_databases.py         # existing (unchanged)
│   └── noop_database.py               # existing (unchanged)
└── core/sources/
    ├── __init__.py                    # updated: export DBTableSource
    └── db_table_source.py             # NEW: DBTableSource
```

**Not in this spike** (belong to PLT-1074/1075/1076):
- `databases/sqlite_connector.py` — `SQLiteConnector`
- `databases/postgresql_connector.py` — `PostgreSQLConnector`
- `databases/spiraldb_connector.py` — `SpiralDBConnector`

---

## Tests

```
tests/
├── test_databases/
│   └── test_connector_arrow_database.py   # NEW: protocol conformance + behaviour via mock connector
└── test_core/sources/
    └── test_db_table_source.py            # NEW: DBTableSource via mock connector
```

Both test files use a `MockDBConnector` (defined in the test file) that holds data in-memory, enabling tests with zero external dependencies.

---

## Design Decisions Log

| Question | Decision |
|---|---|
| Protocol vs ABC? | `Protocol` — consistent with existing codebase, enables parallel dev without import coupling |
| Separate `ReadableDBConnectorProtocol`? | No — single `DBConnectorProtocol`; `Source` simply doesn't call write methods. Can split later if needed. |
| Generic Source vs per-DB subclasses? | Single `DBTableSource(connector, table_name)` — only the connector varies |
| Type mapping ownership? | Connector — `iter_batches` and `get_column_info` always return Arrow types |
| `record_path → table_name`? | `"__".join(sanitized_parts)` — double underscore separator avoids collisions |
| Upsert abstraction? | `upsert_records(table, id_column, skip_existing)` on connector — hides SQLite/PostgreSQL/SpiralDB dialect differences (INSERT OR IGNORE vs ON CONFLICT DO NOTHING, etc.) |
| Pending-batch location? | In `ConnectorArrowDatabase` (Python-side) — mirrors existing `DeltaTableDatabase`/`InMemoryArrowDatabase` pattern |
