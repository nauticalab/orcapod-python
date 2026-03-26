# Design: PostgreSQLTableSource (PLT-1072)

**Date:** 2026-03-26
**Issue:** [PLT-1072](https://linear.app/enigma-metamorphic/issue/PLT-1072/implement-source-based-on-postgresql-tables-with-pk-as-default-tag)
**Status:** Approved

---

## Summary

Implement `PostgreSQLTableSource`, a read-only OrcaPod `Source` backed by a PostgreSQL table. The table's primary key columns serve as default tag columns. Follows the same pattern as the already-merged `SQLiteTableSource`.

---

## Motivation

PostgreSQL is a primary production database at Metamorphic. Exposing tables as OrcaPod Sources allows pipelines to ingest structured relational data with content-addressable identity derived naturally from primary keys. The `PostgreSQLConnector` (PLT-1075) and the `DBTableSource` base class (PLT-1078) are already complete; this issue wires them together into a user-facing `Source`.

---

## Approach

**Thin subclass of `DBTableSource`, DSN-only construction.**

`PostgreSQLTableSource` is a minimal subclass of `DBTableSource`. All source logic (PK resolution, eager loading, Arrow conversion, stream building) lives in `DBTableSource`. This class only handles PostgreSQL-specific initialization: accept a DSN string, create a `PostgreSQLConnector`, delegate to the base class, then close the connector.

This mirrors `SQLiteTableSource` exactly — the only meaningful difference is that PostgreSQL has no ROWID fallback, so a table with no PK and no explicit `tag_columns` raises `ValueError` (already the default `DBTableSource` behaviour).

---

## Architecture

### New file

`src/orcapod/core/sources/postgresql_table_source.py`

### Class signature

```python
class PostgreSQLTableSource(DBTableSource):
    def __init__(
        self,
        dsn: str,
        table_name: str,
        tag_columns: Collection[str] | None = None,
        system_tag_columns: Collection[str] = (),
        record_id_column: str | None = None,
        source_id: str | None = None,
        label: str | None = None,
        data_context: str | contexts.DataContext | None = None,
        config: Config | None = None,
    ) -> None: ...

    def to_config(self) -> dict[str, Any]: ...

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> "PostgreSQLTableSource": ...
```

### Construction sequence

Unlike `SQLiteTableSource`, **no pre-`super()` introspection is needed** — there is no ROWID fallback to detect. The subclass creates the connector and delegates immediately to `DBTableSource`, which internally handles all table validation and PK resolution.

```
PostgreSQLTableSource.__init__(dsn, table_name)
  → PostgreSQLConnector(dsn)                   # open connection
  → DBTableSource.__init__(connector, ...)     # full source initialisation
      [inside DBTableSource]:
        → connector.get_table_names()          # validate table exists
        → connector.get_pk_columns(table)      # resolve default tag columns (if tag_columns=None)
        → connector.iter_batches(SELECT * FROM "table")
        → pa.Table.from_batches(...)           # assemble Arrow table
        → SourceStreamBuilder.build(...)       # attach tags, source-info, schema hash
  → connector.close()                          # release connection (finally block)
```

After construction the source holds all data in-memory as an `ArrowTableStream`. The PostgreSQL connection is fully released; subsequent `iter_packets()` / `as_table()` calls read from memory.

---

## Serialisation

`to_config()` returns:

```python
{
    "source_type": "postgresql_table",
    "dsn": "<connection string>",
    "table_name": "<name>",
    "tag_columns": [...],
    "system_tag_columns": [...],
    "record_id_column": ...,
    "source_id": ...,
    "content_hash": ...,
    "pipeline_hash": ...,
    "tag_schema": {...},
    "packet_schema": {...},
}
```

Notes:
- The generic `"connector"` key emitted by `DBTableSource.to_config()` is stripped (same technique as `SQLiteTableSource`).
- `"label"` is **not** included in `to_config()` output (consistent with `SQLiteTableSource` and `DBTableSource` — none of them serialise `label` today). `from_config()` accepts `config.get("label")` for forward-compatibility but round-trips will not preserve a label set at construction time.

`from_config()` reconstructs by passing `dsn`, `table_name`, and the stored optional fields directly to `__init__`. It does **not** go through `PostgreSQLConnector.from_config()` — the DSN is passed straight to `__init__`.

---

## Exports & Registration

Three places to update, identical to the `SQLiteTableSource` rollout:

1. **`src/orcapod/core/sources/__init__.py`** — import `PostgreSQLTableSource` and add `"PostgreSQLTableSource"` to `__all__`. The `__all__` list is explicit (not auto-generated), so both steps are required. Adding it to `__all__` is what makes the `orcapod.sources` re-export work automatically.
2. **`src/orcapod/pipeline/serialization.py`** — add `"postgresql_table": PostgreSQLTableSource` to `_build_source_registry()`
3. **`src/orcapod/sources/__init__.py`** — no direct change needed; the re-export via `from orcapod.core.sources import *` picks up whatever is in `__all__` (step 1 must be done first)

---

## Error Handling

| Situation | Behaviour |
|---|---|
| Table does not exist | `ValueError: Table 'x' not found in database` (from `DBTableSource`) |
| Table is empty | `ValueError: Table 'x' is empty` (from `DBTableSource`) |
| No PK and no `tag_columns` given | `ValueError: Table 'x' has no primary key columns. Provide explicit tag_columns.` (from `DBTableSource`) |
| NULL values in tag columns | Passed through as-is — Arrow supports nulls natively; PostgreSQL PK columns are always `NOT NULL` so this can only arise with an explicit `tag_columns` override |
| Connection failure | `psycopg` exception propagates naturally |
| Missing `"dsn"` key in `from_config` | `KeyError` from the `from_config` body |

---

## Testing

### Unit tests — no live database required

**File:** `tests/test_core/sources/test_postgresql_table_source.py`

Uses `unittest.mock.patch("psycopg.connect")` throughout, with mock cursors returning controlled data. Sections:

1. Import / export sanity (`from orcapod.core.sources import PostgreSQLTableSource`, present in `__all__`, importable from `orcapod.sources`)
2. Protocol conformance (`SourceProtocol`, `StreamProtocol`, `PipelineElementProtocol`)
3. PK as default tag columns — single PK, composite PK
4. Explicit `tag_columns` override
5. No-PK table raises `ValueError`
6. Missing / empty table raises `ValueError`
7. Stream behaviour (`iter_packets`, `output_schema`, `as_table`, `producer`, `upstreams`)
8. Deterministic hashing (`pipeline_hash`, `content_hash`)
9. `to_config` shape — has `source_type`, `dsn`, `table_name`, `tag_columns`, `source_id`, `content_hash`, `pipeline_hash`; does **not** have `connector` key or `label` key
10. `from_config` round-trip (reconstructs with matching hashes)
11. `resolve_source_from_config` dispatches to `PostgreSQLTableSource`

### Integration tests — requires live PostgreSQL

**File:** `tests/test_core/sources/test_postgresql_table_source_integration.py`
**Marker:** `@pytest.mark.postgres`
**Fixture:** per-test schema isolation (reuse pattern from `test_postgresql_connector_integration.py`)

- Single-PK table: source yields correct packets, tag column in tag schema
- Composite-PK table: both PK columns in tag schema
- Explicit `tag_columns` override: overrides PKs correctly
- Pipeline integration: `PostgreSQLTableSource` drives a full pipeline end-to-end, tag values and packet values are correct

---

## Out of Scope

- Lazy / streaming iteration (data is eagerly loaded into memory, same as all other `DBTableSource` subclasses)
- Async support
- Write path (this is a read-only Source)
- Connection pooling
