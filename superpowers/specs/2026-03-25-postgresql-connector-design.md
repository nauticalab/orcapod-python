# PostgreSQLConnector Design

**Issue:** PLT-1075
**Date:** 2026-03-25
**Status:** Approved

---

## Summary

Implement `PostgreSQLConnector` — a `DBConnectorProtocol` implementation backed by `psycopg` (psycopg3) — enabling `ConnectorArrowDatabase` and `DBTableSource` to use PostgreSQL as a backend without exposing psycopg internals to callers.

---

## Context

PLT-1078 (done) established the three-layer architecture:

```
DBConnectorProtocol       ← what this ticket implements for PostgreSQL
        ↓
ConnectorArrowDatabase    ← generic, consumes any DBConnectorProtocol
        ↓
DBTableSource             ← read-only Source, also consumes DBConnectorProtocol
```

`SQLiteConnector` (PLT-1076) is the reference implementation. `PostgreSQLConnector` mirrors its structure exactly.

---

## Approach: psycopg3 server-side cursor (Option B)

`iter_batches` uses a **named server-side cursor** (`DECLARE … CURSOR FOR`), not a plain cursor. This means PostgreSQL streams rows incrementally rather than buffering the entire result set in server memory before sending anything.

For a 5M-row table:

| | Plain cursor (Option A) | Named cursor (Option B) |
|---|---|---|
| PG memory | 5M rows buffered | ~`batch_size` rows at a time |
| Python memory | all rows arrived, being sliced | one batch at a time |
| Caller stops early | all rows already sent | PG stops after `CLOSE` |

Named cursors require a transaction; psycopg3 opens one automatically with `autocommit=False` (the default).

---

## Module structure

**New file:** `src/orcapod/databases/postgresql_connector.py`

```
postgresql_connector.py
├── _pg_type_to_arrow(pg_type_name, udt_name) → pa.DataType   # module-level
├── _arrow_type_to_pg_sql(arrow_type) → str                    # module-level
├── _resolve_column_type_lookup(query, connector)
│       → dict[str, pa.DataType]                               # module-level
└── PostgreSQLConnector                                         # sync connector
    ├── __init__(dsn: str)
    ├── _conn: psycopg.Connection
    ├── _lock: threading.RLock
    ├── _cursor_seq: itertools.count            # per-call unique cursor names
    ├── _require_open() → psycopg.Connection
    ├── _validate_table_name(table_name) → None
    ├── get_table_names() → list[str]
    ├── get_pk_columns(table_name) → list[str]
    ├── get_column_info(table_name) → list[ColumnInfo]
    ├── iter_batches(query, params, batch_size) → Iterator[RecordBatch]
    ├── create_table_if_not_exists(table_name, columns, pk_column)
    ├── upsert_records(table_name, records, id_column, skip_existing)
    ├── close() / __enter__ / __exit__
    └── to_config() / from_config()
```

`PostgreSQLConnector` is exported from `src/orcapod/databases/__init__.py`.

Type-mapping helpers are **module-level functions** (not class methods) so a future `AsyncPostgreSQLConnector` can reuse them without subclassing.

`_resolve_column_type_lookup(query, connector)` extracts the FROM-clause table-name parsing and `get_column_info` lookup into a single helper. Signature:

```python
def _resolve_column_type_lookup(
    query: str,
    connector: "PostgreSQLConnector",
) -> dict[str, pa.DataType]:
    """Parse the FROM clause of query to find the source table, then return
    a name→Arrow-type dict from get_column_info. Returns {} if no table found."""
```

Falls back to an empty dict (all columns get `pa.large_string()`) for computed/aliased/multi-table queries.

---

## Type mapping

### PostgreSQL type name → Arrow type

| PostgreSQL type | Arrow type | Notes |
|---|---|---|
| `bool` | `pa.bool_()` | |
| `int2` | `pa.int32()` | Deliberate divergence from SQLiteConnector (see note) |
| `int4` | `pa.int32()` | Deliberate divergence from SQLiteConnector (see note) |
| `int8` | `pa.int64()` | |
| `float4` | `pa.float32()` | |
| `float8` | `pa.float64()` | |
| `numeric` / `decimal` | `pa.float64()` | Lossy; pragmatic |
| `text`, `varchar`, `char`, `name` | `pa.large_string()` | |
| `bytea` | `pa.large_binary()` | |
| `uuid` | `pa.large_string()` | TODO: revisit in PLT-1162 |
| `json` / `jsonb` | `pa.large_string()` | Serialised as JSON text |
| `date` | `pa.date32()` | |
| `timestamp` | `pa.timestamp('us')` | No timezone |
| `timestamptz` | `pa.timestamp('us', tz='UTC')` | |
| `time` / `timetz` | `pa.large_string()` | Known gap; warning logged |
| `_<elem>` (arrays) | `pa.large_list(<elem_type>)` | `udt_name` prefix `_` stripped |
| unknown | `pa.large_string()` | Warning logged |

**Note on `int2`/`int4` vs `SQLiteConnector`:** SQLiteConnector maps all integers to `pa.int64()`. PostgreSQL's richer integer type system allows more precise mapping (`int2`/`int4` → `pa.int32()`). Callers using `ConnectorArrowDatabase` with both backends should be aware that `SMALLINT`/`INTEGER` columns will produce `pa.int32()` from PostgreSQL vs `pa.int64()` from SQLite. Schema validation in `ConnectorArrowDatabase.flush()` will catch any mismatch at write time.

### Arrow type → PostgreSQL DDL

| Arrow type | PostgreSQL DDL |
|---|---|
| `pa.bool_()` | `BOOLEAN` |
| `pa.int32()` | `INTEGER` |
| `pa.int64()` | `BIGINT` |
| `pa.float32()` | `REAL` |
| `pa.float64()` | `DOUBLE PRECISION` |
| `pa.large_string()` / `pa.utf8()` | `TEXT` |
| `pa.large_binary()` / `pa.binary()` | `BYTEA` |
| `pa.date32()` | `DATE` |
| `pa.timestamp('us')` | `TIMESTAMP` |
| `pa.timestamp('us', tz=…)` | `TIMESTAMPTZ` |
| `pa.large_list(…)` | `ValueError` (not supported for CREATE TABLE) |
| unknown | `TEXT` + warning logged |

Arrays are read-only from the connector's perspective — `ConnectorArrowDatabase` never creates array-typed columns, so only `iter_batches` / `get_column_info` need to handle them.

---

## Schema introspection

All queries use `information_schema` (ANSI SQL, version-independent).

Table name validation: reject names containing `"` to prevent SQL injection via identifier interpolation. Implemented in `_validate_table_name(table_name)`, called at the top of every public method that accepts a table name (`get_pk_columns`, `get_column_info`, `create_table_if_not_exists`, `upsert_records`). `get_pk_columns` and `get_column_info` pass the table name as a parameterised `%s` placeholder (safe from injection), but still call `_validate_table_name` for consistency and defence-in-depth.

**`get_table_names()`**
```sql
SELECT table_name
FROM information_schema.tables
WHERE table_schema = current_schema()
  AND table_type = 'BASE TABLE'
ORDER BY table_name
```

**`get_pk_columns(table_name)`**
```sql
SELECT kcu.column_name
FROM information_schema.key_column_usage kcu
JOIN information_schema.table_constraints tc
  ON kcu.constraint_name = tc.constraint_name
 AND kcu.table_schema    = tc.table_schema
 AND kcu.table_name      = tc.table_name
WHERE tc.constraint_type = 'PRIMARY KEY'
  AND kcu.table_schema   = current_schema()
  AND kcu.table_name     = %s
ORDER BY kcu.ordinal_position
```

The join includes `kcu.table_name = tc.table_name` to prevent cross-table constraint-name collisions within the same schema.

Returns `[]` for non-existent tables or tables with no primary key.

**`get_column_info(table_name)`**
```sql
SELECT column_name, data_type, udt_name, is_nullable
FROM information_schema.columns
WHERE table_schema = current_schema()
  AND table_name   = %s
ORDER BY ordinal_position
```

`data_type = 'ARRAY'` → use `udt_name` (strip leading `_`) to get element type.
`is_nullable = 'YES'` → `ColumnInfo.nullable = True`.

Returns `[]` for non-existent tables (query returns zero rows, same contract as `SQLiteConnector`).

---

## `iter_batches` — server-side cursor

```python
def iter_batches(self, query, params=None, batch_size=1000):
    with self._lock:
        conn = self._require_open()
        cursor_name = f"orcapod_{next(self._cursor_seq)}"
        cur = conn.cursor(name=cursor_name)
    try:
        with self._lock:
            self._require_open()
            cur.execute(query, params)
            col_names = [d.name for d in cur.description]
            type_lookup = _resolve_column_type_lookup(query, self)
            arrow_types = [type_lookup.get(n, pa.large_string()) for n in col_names]
            schema = pa.schema([pa.field(n, t) for n, t in zip(col_names, arrow_types)])
            rows = cur.fetchmany(batch_size)
        while rows:
            arrays = [pa.array([r[i] for r in rows], type=t)
                      for i, t in enumerate(arrow_types)]
            yield pa.RecordBatch.from_arrays(arrays, schema=schema)
            with self._lock:
                self._require_open()
                rows = cur.fetchmany(batch_size)
    finally:
        cur.close()
```

Key points:

- **`cur.itersize` is psycopg2-only and must not be used.** psycopg3 does not expose this attribute on named cursors. Fetch page size is controlled exclusively via `fetchmany(batch_size)`.
- **Unique cursor names:** `self._cursor_seq = itertools.count()` is initialised in `__init__`. Each call to `iter_batches` draws the next value under the lock, producing names like `orcapod_0`, `orcapod_1`, etc. This ensures concurrent generators on the same connector each get a distinct server-side cursor name and do not collide.
- **`cur` is created before the `try` block** so its name is known, but `cur.execute` is called *inside* the `try` so that any execute failure still triggers `cur.close()` in the `finally` clause, preventing a leaked named cursor on the server.
- **`try/finally` guarantees `cur.close()`** is sent to the server on all exit paths: normal exhaustion, early generator abandonment (caller breaks/discards), and exceptions. `cur.close()` sends `CLOSE orcapod_N` to PostgreSQL, releasing the server-side cursor and its snapshot.
- Lock held only during cursor creation, `execute` + first fetch, and each subsequent `fetchmany` — not across yield points. This matches `SQLiteConnector`'s locking strategy.
- If `close()` is called from another thread while iteration is in progress, `_require_open()` inside the lock raises `RuntimeError`. This is the same behaviour as `SQLiteConnector` and is intentional — callers must not close a connector with active iterators.
- `_resolve_column_type_lookup` parses `FROM "table_name"` or `FROM table_name` from the query via regex, calls `get_column_info`, and returns a `dict[str, pa.DataType]`. Returns `{}` for multi-table, computed, or unresolvable queries; callers get `pa.large_string()` fallback.

---

## Write path

**`create_table_if_not_exists(table_name, columns, pk_column)`**

Raises `ValueError` if `pk_column` is not present in `columns` (same guard as `SQLiteConnector`).

Emits standard DDL:

```sql
CREATE TABLE IF NOT EXISTS "my_table" (
    "__record_id" TEXT NOT NULL PRIMARY KEY,
    "value"       DOUBLE PRECISION
)
```

**`upsert_records(table_name, records, id_column, skip_existing=False)`**

Uses PostgreSQL `ON CONFLICT`. The SET column list is derived from `records.column_names`, excluding the conflict-target column (`id_column`):

```sql
-- skip_existing=False (overwrite):
INSERT INTO "my_table" ("__record_id", "value")
VALUES (%s, %s)
ON CONFLICT ("__record_id") DO UPDATE
  SET "value" = EXCLUDED."value"

-- skip_existing=True (ignore):
INSERT INTO "my_table" ("__record_id", "value")
VALUES (%s, %s)
ON CONFLICT ("__record_id") DO NOTHING
```

Row serialisation: `records.to_pylist()` → `executemany`. psycopg3's `executemany` uses the extended query protocol internally.

psycopg3 handles `bool`, `int`, `float`, `str`, `bytes`, `datetime.date`, `datetime.datetime` (tz-aware for `timestamptz`) natively. Arrow nulls become Python `None` → SQL `NULL`.

**Transaction boundary:** The connector is opened with `autocommit=False`. `upsert_records` calls `conn.commit()` immediately after `executemany` completes successfully. `create_table_if_not_exists` also calls `conn.commit()` after executing the DDL. This means each write method is self-contained — callers (`ConnectorArrowDatabase.flush()`) do not need to manage commit/rollback. On error, the exception propagates to the caller; any uncommitted changes are rolled back when the connection is eventually closed.

---

## Lifecycle & serialisation

```python
PostgreSQLConnector(dsn: str)
```

DSN is a standard libpq connection string: `postgresql://user:pass@host:5432/dbname` or keyword form `host=localhost dbname=mydb user=alice`. Connection opened eagerly at construction with `autocommit=False`.

`_require_open()` returns the open connection or raises `RuntimeError("PostgreSQLConnector is closed")` — same guard pattern as `SQLiteConnector`.

`close()` is idempotent: checks `self._conn is not None`, calls `conn.close()`, sets to `None`. `__enter__` returns `self`, `__exit__` calls `close()`.

`to_config()` returns `{"connector_type": "postgresql", "dsn": "..."}`. The DSN may contain a password — callers must not log or persist the config dict in plaintext (documented in docstring).

`from_config()` raises `ValueError` if `config.get("connector_type") != "postgresql"` — this catches both wrong type and missing key (since `None != "postgresql"`), consistent with `SQLiteConnector`.

---

## Async path (future)

psycopg3 has a near-identical async API (`psycopg.AsyncConnection`, `await cur.execute()`, etc.). Because `_pg_type_to_arrow`, `_arrow_type_to_pg_sql`, and `_resolve_column_type_lookup` are module-level functions, an `AsyncPostgreSQLConnector` added later reuses all type-mapping and schema-resolution logic without subclassing. The protocol would require a parallel `AsyncDBConnectorProtocol` with `AsyncIterator[pa.RecordBatch]`.

---

## Testing

### Unit tests — `tests/test_databases/test_postgresql_connector.py`

No live database required. Uses `unittest.mock.patch` on `psycopg.connect`.

- `_pg_type_to_arrow`: full type name coverage including array variants and unknown fallback
- `_arrow_type_to_pg_sql`: Arrow type → DDL string
- `isinstance(connector, DBConnectorProtocol)` structural check (with mocked connection)
- `to_config` / `from_config` roundtrip
- `from_config` raises on wrong `connector_type`
- `from_config` raises on missing `connector_type` key
- `close()` idempotence (mock connection)
- `get_table_names()` with mocked cursor returning rows
- `get_pk_columns()` with mocked cursor — single PK, composite PK, no PK
- `_validate_table_name` raises on double-quote in name

### Integration tests — `tests/test_databases/test_postgresql_connector_integration.py`

Requires live PostgreSQL. Marked `@pytest.mark.postgres`. Uses `pytest-postgresql` process executor (spawns `initdb` + `pg_ctl` from locally-installed PostgreSQL binaries).

Fixture:
```python
@pytest.fixture(scope="session")
def pg_connector(postgresql):
    dsn = f"host={postgresql.info.host} port={postgresql.info.port} ..."
    c = PostgreSQLConnector(dsn)
    yield c
    c.close()
```

Test coverage:
- **Lifecycle:** open/close, context manager closes on exit, `_require_open` raises after close, `close()` idempotent
- **Schema introspection:** `get_table_names()` empty DB / with tables / excludes views; `get_pk_columns()` single/composite/none/nonexistent table; `get_column_info()` all mapped types + nullable flag + nonexistent table returns `[]`
- **Write:** `create_table_if_not_exists` idempotent, PK set correctly, NOT NULL respected, raises when `pk_column` not in columns, raises on invalid table name; `upsert_records` insert/replace/skip-existing, raises on invalid table name
- **Table name validation:** `get_pk_columns`, `get_column_info`, `create_table_if_not_exists`, and `upsert_records` all raise `ValueError` on a table name containing `"`
- **Read:** `iter_batches` all rows, correct types, batch size, empty result, early termination (generator abandoned mid-iteration still sends `CLOSE`)
- **Type roundtrip:** every mapped PG type written and read back with correct Arrow type (bool, int2, int4, int8, float4, float8, text, bytea, uuid, jsonb, date, timestamp, timestamptz, int4[], text[])
- **`ConnectorArrowDatabase` integration:** add/flush/get, skip_duplicates, schema mismatch raises

### pytest marker

```ini
[pytest]
markers =
    postgres: mark test as requiring a live PostgreSQL instance
```

Default suite: `pytest -m "not postgres"`. CI: separate step with `pytest -m postgres` against a `services: postgres:` container.
