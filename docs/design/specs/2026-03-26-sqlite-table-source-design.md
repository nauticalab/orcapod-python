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
| `src/orcapod/databases/sqlite_connector.py` | `iter_batches`: after `type_lookup` is populated from `get_column_info`, add `if "rowid" in col_names: type_lookup["rowid"] = _pa.int64()` before `arrow_types` is computed |
| `src/orcapod/core/sources/db_table_source.py` | Add `*, _query: str \| None = None` at the end of `__init__` (the `*` makes it keyword-only); replace the hardcoded `SELECT *` line (currently `batches = list(connector.iter_batches(f'SELECT * FROM "{table_name}"'))`) with two lines: `query = _query if _query is not None else f'SELECT * FROM "{table_name}"'` then `batches = list(connector.iter_batches(query))` |
| `src/orcapod/core/sources/__init__.py` | Import and export `SQLiteTableSource`; add to `__all__` |
| `src/orcapod/pipeline/serialization.py` | In `_build_source_registry()`: add `from orcapod.core.sources.sqlite_table_source import SQLiteTableSource` (following the established pattern of lazy local imports) and add `"sqlite_table": SQLiteTableSource` to the returned dict |

Note: `src/orcapod/sources/__init__.py` already does `from orcapod.core.sources import *` and re-exports `__all__` — updating `core/sources/__init__.py` is sufficient, no change needed there.

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
├─ 3. Resolve tags and query
│      ├─ tag_columns provided (non-None)
│      │    → resolved_tags = list(tag_columns)
│      └─ tag_columns is None
│           ├─ pk_cols = connector.get_pk_columns(table_name)
│           ├─ pk_cols non-empty → resolved_tags = pk_cols
│           └─ pk_cols empty   → resolved_tags = ["rowid"]
│
├─ 4. Determine query (handles both auto-detection AND from_config reconstruction)
│      normal_cols = {ci.name for ci in connector.get_column_info(table_name)}
│      ├─ "rowid" in resolved_tags AND "rowid" not in normal_cols
│      │    → _query = 'SELECT rowid, * FROM "{table_name}"'
│      └─ otherwise
│           → _query = None  (DBTableSource uses default SELECT *)
│
└─ 5. super().__init__(connector, table_name,
                       tag_columns=resolved_tags,   ← always non-None
                       _query=_query, ...)
       │
       │  NOTE: passing tag_columns as a non-None list bypasses
       │  DBTableSource's own PK-lookup-and-raise path, which only
       │  fires when tag_columns is None. This is intentional.
       │
       └─ DBTableSource: fetch batches (using _query) → SourceStreamBuilder → stream
          (rowid column arrives typed as int64 via SQLiteConnector patch)

store self._db_path
```

`from_config` calls `cls(db_path=config["db_path"], table_name=config["table_name"], tag_columns=config["tag_columns"], ...)` — the connector is recreated from `db_path`. Because `tag_columns` is passed explicitly (non-None), step 3 skips PK detection; step 4 then checks whether `"rowid"` is in the resolved tags but not in the table's normal columns and re-injects the rowid query if so. This means **ROWID-only tables also round-trip correctly** from config as long as the backing file exists.

**Known limitation:** `:memory:` sources cannot be reconstructed via `from_config`. The new in-memory database is empty and does not contain the original table, causing `ValueError: Table 'x' not found`. File-backed sources (including ROWID-only tables) round-trip correctly. The config round-trip test (test 9) must use a `tmp_path`-backed SQLite file.

---

## Error Handling

| Condition | Behaviour |
|---|---|
| Table not found | `ValueError: Table 'x' not found in database.` — raised in step 2, before ROWID logic |
| Table found, no PK, no explicit tags | ROWID fallback — no error; `"rowid"` used as tag column |
| Table found, no PK, explicit `tag_columns=[...]` | Works normally — ROWID detection skipped when `tag_columns` is provided |
| `tag_columns=[]` provided explicitly | Proceeds with empty tag schema — `SourceStreamBuilder` does not guard against empty tag lists; no `ValueError` is raised |
| Table exists but is empty | `ValueError: Table 'x' is empty.` — raised by `DBTableSource` |
| `db_path` points to non-existent file | `sqlite3.OperationalError` propagates from `SQLiteConnector.__init__` |
| `"` in table name | `ValueError` from `SQLiteConnector._validate_table_name` |

The ROWID fallback is silent (no warning log). The resolved `"rowid"` tag column appears in `to_config()` for auditability.

---

## Testing Plan

### Unit tests — `tests/test_core/sources/test_sqlite_table_source.py`

All use in-memory SQLite (`:memory:`), except the config round-trip test which requires a file-backed db.

1. **Import / export sanity** — importable from `orcapod.core.sources` and `orcapod.sources`; in `__all__`
2. **Protocol conformance** — is `SourceProtocol`, `StreamProtocol`, `PipelineElementProtocol`
3. **PK as default tags** — single-column PK; composite PK; correct tag/packet schema split
4. **Explicit tag override** — `tag_columns=[...]` overrides PK detection entirely
5. **ROWID fallback** — table with no explicit PK gets `"rowid"` tag; `rowid` column type is `int64`; all rows returned; rowid values are positive integers
6. **Error cases** — missing table raises `ValueError`; empty table raises `ValueError`
7. **Stream behaviour** — `iter_packets` count, `as_table`, `output_schema`, `producer is None`, `upstreams == ()`
8. **Deterministic hashing** — `pipeline_hash` and `content_hash` stable across two identical constructions (both in-memory)
9. **Config round-trip (PK table)** — uses file-backed `tmp_path` SQLite db; `to_config()` has `source_type="sqlite_table"`, `db_path`, `table_name`, `tag_columns`; `from_config(to_config())` reconstructs successfully; content/pipeline hashes match before and after
10. **Config round-trip (ROWID-only table)** — same as above but with a ROWID-only table; `tag_columns=["rowid"]` in config; `from_config(to_config())` reconstructs correctly and `rowid` remains the tag column

### Integration test — same file, marked `@pytest.mark.integration`

Write rows into an in-memory SQLite table via `SQLiteConnector`, wrap with `SQLiteTableSource`, feed through a `FunctionPod`, collect output — verify tag columns flow through and pipeline completes.

### Regression tests

- `tests/test_databases/test_sqlite_connector.py` — `SELECT rowid, * FROM "t"` yields `rowid` column typed as `int64`
- `tests/test_core/sources/test_db_table_source.py` — passing `_query` to `DBTableSource` overrides the default `SELECT *` and selects the correct rows

---

## Known Limitations

- **`:memory:` sources are not round-trip serializable.** `from_config` recreates `SQLiteConnector(db_path)`, which opens a fresh empty in-memory database. File-backed databases work correctly.
- **`WITHOUT ROWID` tables** (rare SQLite DDL option): the implicit rowid does not exist; `SELECT rowid, * FROM "t"` will raise a SQLite error which propagates as-is. No special handling is added for this edge case.

---

## Out of Scope

- Write-back / mutation via `SQLiteTableSource`
- Async SQLite access
- `PostgreSQLTableSource` / `SpiralDBTableSource` (separate issues PLT-1074/1075)
