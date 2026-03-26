# Design Spec: SQLiteTableSource (PLT-1077)

**Date:** 2026-03-26
**Issue:** [PLT-1077](https://linear.app/enigma-metamorphic/issue/PLT-1077)
**Status:** Approved

---

## Summary

Implement `SQLiteTableSource`, a `RootSource` backed by a SQLite table. Primary-key columns serve as the default tag columns. For tables with no explicit primary key (ROWID-only tables), the implicit SQLite `rowid` is used automatically. Provides a working `from_config` round-trip — the gap that `DBTableSource` cannot fill today.

---

## Approach

`SQLiteTableSource` subclasses `DBTableSource` (Approach 2 of three considered). All stream-building logic stays in `DBTableSource`; `SQLiteTableSource` owns only SQLite-specific concerns: creating the `SQLiteConnector`, detecting ROWID-only tables, and serialization.

Three alternatives were evaluated:

| Approach | Summary | Why rejected |
|---|---|---|
| Standalone class | Full reimplementation, no parent | ~50 lines duplicated from `DBTableSource` |
| **Subclass + `_query` hook** | Minimal change to `DBTableSource`; ROWID detected before `super().__init__` | **Selected** |
| Connector adapter | Wrap connector to inject rowid; no changes to `DBTableSource` | Extra indirection; ROWID logic split across two objects |

---

## Files Changed

### New file

**`src/orcapod/core/sources/sqlite_table_source.py`**

```
SQLiteTableSource(DBTableSource)
  __init__(db_path, table_name, tag_columns=None,
           system_tag_columns=(), record_id_column=None,
           source_id=None, label=None, data_context=None, config=None)
  to_config() → dict        # source_type="sqlite_table", db_path, table_name, …
  from_config(config) → cls # reconstructs via db_path; fully working
```

### Modified files

| File | Change |
|---|---|
| `src/orcapod/databases/sqlite_connector.py` | `iter_batches`: map column named `"rowid"` to `pa.int64()` instead of fallback `pa.large_string()` |
| `src/orcapod/core/sources/db_table_source.py` | Add `_query: str \| None = None` parameter; used in place of default `SELECT *` when provided |
| `src/orcapod/core/sources/__init__.py` | Export `SQLiteTableSource`; add to `__all__` |
| `src/orcapod/sources/__init__.py` | Re-export via `from orcapod.core.sources import *` (no change needed if `__all__` is updated) |
| `src/orcapod/pipeline/serialization.py` | Register `"sqlite_table": SQLiteTableSource` in `_build_source_registry()` |

---

## Data Flow

```
SQLiteTableSource.__init__(db_path, table_name, tag_columns, ...)
│
├─ 1. SQLiteConnector(db_path)
│
├─ 2. Validate: table_name in connector.get_table_names()
│      └─ missing → ValueError("Table 'x' not found in database.")
│
├─ 3. Resolve tags
│      ├─ tag_columns provided → resolved_tags = list(tag_columns), _query = None
│      └─ tag_columns is None
│           ├─ pk_cols = connector.get_pk_columns(table_name)
│           ├─ pk_cols non-empty → resolved_tags = pk_cols, _query = None
│           └─ pk_cols empty (ROWID-only)
│                ├─ resolved_tags = ["rowid"]
│                └─ _query = 'SELECT rowid, * FROM "{table_name}"'
│
└─ 4. super().__init__(connector, table_name,
                       tag_columns=resolved_tags, _query=_query, ...)
       └─ DBTableSource: fetch batches → SourceStreamBuilder → stream
          (rowid column arrives typed as int64 via SQLiteConnector patch)

store self._db_path
```

`from_config` calls `cls(db_path=config["db_path"], table_name=config["table_name"], tag_columns=config["tag_columns"], ...)` — the connector is recreated from `db_path` with no connector factory registry needed.

---

## Error Handling

| Condition | Behaviour |
|---|---|
| Table not found | `ValueError: Table 'x' not found in database.` — raised by `DBTableSource` before ROWID logic |
| Table found, no PK, no explicit tags | ROWID fallback — no error, `"rowid"` used as tag column |
| Table found, no PK, explicit `tag_columns=[]` | `ValueError` from `DBTableSource` / `SourceStreamBuilder` (no tag columns) |
| Table exists but is empty | `ValueError: Table 'x' is empty.` — raised by `DBTableSource` |
| `db_path` points to non-existent file | `sqlite3.OperationalError` propagates from `SQLiteConnector.__init__` |
| `"` in table name | `ValueError` from `SQLiteConnector._validate_table_name` |

The ROWID fallback is silent (no warning log). The resolved `"rowid"` tag column appears in `to_config()` for auditability.

---

## Testing Plan

### Unit tests — `tests/test_core/sources/test_sqlite_table_source.py`

All use in-memory SQLite (`:memory:`), no mocks.

1. **Import / export sanity** — importable from `orcapod.core.sources` and `orcapod.sources`; in `__all__`
2. **Protocol conformance** — is `SourceProtocol`, `StreamProtocol`, `PipelineElementProtocol`
3. **PK as default tags** — single-column PK; composite PK; correct tag/packet schema split
4. **Explicit tag override** — `tag_columns=[...]` overrides PK detection
5. **ROWID fallback** — table with no explicit PK gets `"rowid"` tag; `rowid` values are `int64`; all rows returned
6. **Error cases** — missing table raises `ValueError`; empty table raises `ValueError`
7. **Stream behaviour** — `iter_packets` count, `as_table`, `output_schema`, `producer is None`, `upstreams == ()`
8. **Deterministic hashing** — `pipeline_hash` and `content_hash` stable across two identical constructions
9. **Config round-trip** — `to_config()` shape; `from_config(to_config())` reconstructs successfully; hashes match

### Integration test — same file, marked `@pytest.mark.integration`

Write rows into an in-memory SQLite table via `SQLiteConnector`, wrap with `SQLiteTableSource`, feed through a `FunctionPod`, collect output — verify tag columns flow through and pipeline completes.

### Regression tests

- `tests/test_databases/test_sqlite_connector.py` — `SELECT rowid, *` yields `rowid` as `int64`
- `tests/test_core/sources/test_db_table_source.py` — passing `_query` to `DBTableSource` selects correct rows

---

## Out of Scope

- `WITHOUT ROWID` tables (rare SQLite feature; raise clear error if rowid query returns no rowid column)
- Write-back / mutation via `SQLiteTableSource`
- Async SQLite access
- `PostgreSQLTableSource` / `SpiralDBTableSource` (separate issues PLT-1074/1075)
