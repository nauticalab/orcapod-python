# Async DB Connector Protocol Design

**Date:** 2026-05-23
**Linear issue:** PLT-1453
**Status:** Approved

---

## Overview

`DBConnectorProtocol` currently exposes only synchronous read and write methods. This spec
describes adding a parallel async interface — `AsyncDBConnectorProtocol` — and implementing
it across `PostgreSQLConnector`, `SQLiteConnector`, and `SpiralDBConnector`.

The async interface enables connectors to be used natively in async pipelines (e.g.,
`DynamicSourceProtocol` implementations) without forcing callers to wrap blocking I/O in
thread executors.

---

## Goals

- Define `AsyncDBConnectorProtocol` as a standalone protocol with 7 async methods.
- Implement all 7 methods in `PostgreSQLConnector` using psycopg3's native async API.
- Implement all 7 methods in `SQLiteConnector` using `asyncio.to_thread()` wrappers
  (stdlib `sqlite3` is synchronous).
- Provide unit test coverage for all async paths on both connectors.
- Provide async integration tests for `PostgreSQLConnector` (container fixture exists).

---

## Out of scope

- Async write methods (`create_table_if_not_exists`, `upsert_records`) — sync write path
  is sufficient for current use cases.
- MySQL connector — a dedicated follow-on issue.
- Removing or deprecating existing sync methods — the sync interface is unchanged.
- Changes to callers of `DBConnectorProtocol` (`DBTableSource`, `ConnectorArrowDatabase`).
- Connection pooling (`psycopg_pool.AsyncConnectionPool`) — not required for the current
  use cases and would add a dependency.
- `SpiralDBConnector` async implementation — deferred to PLT-1456. `SpiralDBConnector`
  has no internal thread synchronization, making `asyncio.to_thread()` wrappers unsafe
  under concurrent async use. PLT-1456 will first add a `threading.Lock` to
  `SpiralDBConnector` and then implement the async interface.

---

## Protocol design

### `AsyncDBConnectorProtocol` — standalone

A new `@runtime_checkable` Protocol defined in
`src/orcapod/protocols/async_db_connector_protocol.py`.

**It does not inherit from `DBConnectorProtocol`.** The two protocols are related but
independent structural contracts. A class satisfies both by implementing all their methods;
Python's structural typing handles this naturally. Callers that need both can check
`isinstance(obj, DBConnectorProtocol) and isinstance(obj, AsyncDBConnectorProtocol)`.

#### Methods

| Method | Return type | Notes |
|---|---|---|
| `__aenter__()` | `AsyncDBConnectorProtocol` | Opens async resources; returns `self` |
| `__aexit__(*args)` | `None` | Calls `async_close()` |
| `async_close()` | `None` | Full shutdown: closes async + sync connections |
| `async_get_table_names()` | `list[str]` | Async schema introspection |
| `async_get_pk_columns(table_name)` | `list[str]` | Async schema introspection |
| `async_get_column_info(table_name)` | `list[ColumnInfo]` | Async schema introspection |
| `async_iter_batches(query, params, batch_size)` | `AsyncIterator[pa.RecordBatch]` | Async read |

`async_iter_batches` signature mirrors `iter_batches` exactly:

```python
async def async_iter_batches(
    self,
    query: str,
    params: Any = None,
    batch_size: int = 1000,
) -> AsyncIterator[pa.RecordBatch]: ...
```

#### Lifecycle contract

The intended usage pattern is the `async with` context manager:

```python
async with PostgreSQLConnector(dsn) as connector:
    async for batch in connector.async_iter_batches('SELECT * FROM "t"'):
        process(batch)
```

`__aenter__` opens async resources. `__aexit__` calls `async_close()`, which performs a
full shutdown of both async and sync connections. After `__aexit__`, the connector must not
be used further.

#### Export surface

`AsyncDBConnectorProtocol` is exported from:
- `orcapod.protocols` (alongside `DBConnectorProtocol`)
- `orcapod.databases` (alongside the connector classes)

---

## `PostgreSQLConnector` implementation

### New private state

```python
self._async_conn: Any = None   # psycopg.AsyncConnection, set by __aenter__
```

`_async_conn` is set exactly once in `__aenter__` before any async method is callable, so
there is no initialization race. Concurrent async operations on `_async_conn` are serialized
by psycopg3's internal `asyncio.Lock` — no application-level lock is required.

### `__aenter__`

Opens a psycopg3 `AsyncConnection` and stores it in `_async_conn`:

```python
async def __aenter__(self) -> PostgreSQLConnector:
    self._async_conn = await psycopg.AsyncConnection.connect(
        self._dsn, autocommit=False
    )
    return self
```

### `_require_async_open()`

Private guard used by schema introspection methods:

```python
def _require_async_open(self) -> Any:
    if self._async_conn is None:
        raise RuntimeError(
            "PostgreSQLConnector: enter the async context manager before "
            "calling async methods"
        )
    return self._async_conn
```

### `async_close()`

Closes `_async_conn` (if open) then calls the sync `close()` for a full shutdown:

```python
async def async_close(self) -> None:
    if self._async_conn is not None:
        await self._async_conn.close()
        self._async_conn = None
    self.close()
```

### `__aexit__`

```python
async def __aexit__(self, *args: Any) -> None:
    await self.async_close()
```

### Schema introspection (`async_get_table_names`, `async_get_pk_columns`, `async_get_column_info`)

Use `_async_conn` (obtained via `_require_async_open()`). Structurally identical to their
sync counterparts — same SQL, same result mapping — but using `async with conn.cursor()`
and `await cur.execute()` / `await cur.fetchall()`.

No application-level lock is needed: psycopg3's `AsyncConnection` carries an internal
`asyncio.Lock` that serializes concurrent async operations on the same connection, so
concurrent calls to these methods (e.g. via `asyncio.gather`) are handled safely by the
driver itself.

### `async_iter_batches`

Opens a **dedicated** `AsyncConnection` per call, using psycopg3's `AsyncServerCursor`
for server-side streaming. This mirrors the sync `iter_batches` pattern exactly:
a dedicated connection prevents other operations on `_async_conn` from invalidating the
open cursor portal.

```
read_conn = await psycopg.AsyncConnection.connect(self._dsn, autocommit=False)
cursor_name = f"orcapod_{next(self._cursor_seq)}"
cur = read_conn.cursor(name=cursor_name)           # AsyncServerCursor
try:
    await cur.execute(query, params)
    ... fetch batches with await cur.fetchmany(batch_size) ...
    yield batch
finally:
    await cur.close()
    await read_conn.close()
```

A module-level coroutine `_async_resolve_column_type_lookup(query, connector)` mirrors the
sync `_resolve_column_type_lookup` but calls `await connector.async_get_column_info(table)`
for Arrow type mapping.

---

## `SQLiteConnector` implementation

stdlib `sqlite3` is synchronous. All async methods are thin `asyncio.to_thread()` wrappers
over the existing sync methods.

### `__aenter__` / `__aexit__`

```python
async def __aenter__(self) -> SQLiteConnector:
    return self   # sync connection already open in __init__

async def __aexit__(self, *args: Any) -> None:
    await self.async_close()
```

### `async_close`

```python
async def async_close(self) -> None:
    await asyncio.to_thread(self.close)
```

### Schema introspection

```python
async def async_get_table_names(self) -> list[str]:
    return await asyncio.to_thread(self.get_table_names)

async def async_get_pk_columns(self, table_name: str) -> list[str]:
    return await asyncio.to_thread(self.get_pk_columns, table_name)

async def async_get_column_info(self, table_name: str) -> list[ColumnInfo]:
    return await asyncio.to_thread(self.get_column_info, table_name)
```

### `async_iter_batches`

The entire sync iteration runs in the thread pool (blocking I/O stays off the event loop),
then batches are yielded lazily from the async generator:

```python
async def async_iter_batches(self, query, params=None, batch_size=1000):
    batches = await asyncio.to_thread(
        lambda: list(self.iter_batches(query, params, batch_size))
    )
    for batch in batches:
        yield batch
```

---

## Testing

### Unit tests

Added to each connector's existing test file as a new `TestAsyncMethods` class.

| Connector | Approach |
|---|---|
| `PostgreSQLConnector` | Mock `psycopg.AsyncConnection` and `AsyncServerCursor`; verify `__aenter__` sets `_async_conn`, `_require_async_open` guards, `async_iter_batches` opens a dedicated connection per call, `async_close` tears down async then sync |
| `SQLiteConnector` | Real in-memory `:memory:` DB (no mocks needed); verify each async method returns the same results as its sync counterpart |

All async test methods use `@pytest.mark.asyncio`.

### Integration tests

Added to `test_postgresql_connector_integration.py`, marked `@pytest.mark.postgres`:

- `TestAsyncLifecycle` — `__aenter__`/`__aexit__` open and close correctly; `async_close`
  is idempotent; `_require_async_open` raises outside context manager
- `TestAsyncSchemaIntrospection` — `async_get_table_names`, `async_get_pk_columns`,
  `async_get_column_info` return correct results against a live PostgreSQL instance
- `TestAsyncIterBatches` — correct rows, correct Arrow types, batch size respected,
  empty result, early generator abandonment closes server-side cursor

No SQLite integration tests (unit tests cover the full path with a real `:memory:` DB).
`SpiralDBConnector` async implementation and tests are deferred to PLT-1456.

---

## File changes summary

| File | Change |
|---|---|
| `src/orcapod/protocols/async_db_connector_protocol.py` | **New** — `AsyncDBConnectorProtocol` |
| `src/orcapod/protocols/__init__.py` | Export `AsyncDBConnectorProtocol` |
| `src/orcapod/databases/__init__.py` | Export `AsyncDBConnectorProtocol` |
| `src/orcapod/databases/postgresql_connector.py` | Add `_async_conn`, `__aenter__`, `__aexit__`, `async_close`, `_require_async_open`, async schema methods, `async_iter_batches`, `_async_resolve_column_type_lookup` |
| `src/orcapod/databases/sqlite_connector.py` | Add `__aenter__`, `__aexit__`, `async_close`, async schema methods, `async_iter_batches` |
| `src/orcapod/databases/spiraldb_connector.py` | No changes — deferred to PLT-1456 |
| `tests/test_databases/test_postgresql_connector.py` | Add `TestAsyncMethods` |
| `tests/test_databases/test_sqlite_connector.py` | Add `TestAsyncMethods` |
| `tests/test_databases/test_postgresql_connector_integration.py` | Add `TestAsyncLifecycle`, `TestAsyncSchemaIntrospection`, `TestAsyncIterBatches` |
