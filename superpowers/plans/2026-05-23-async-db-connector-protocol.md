# Async DB Connector Protocol Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `AsyncDBConnectorProtocol` and implement its 7 async methods on `PostgreSQLConnector` and `SQLiteConnector`.

**Architecture:** A standalone `@runtime_checkable` Protocol defines the async interface independently from `DBConnectorProtocol`. `PostgreSQLConnector` uses psycopg3's native async API (`AsyncConnection`, `AsyncServerCursor`); `SQLiteConnector` wraps its existing sync methods in `asyncio.to_thread()`. The async lifecycle is managed via `__aenter__`/`__aexit__`.

**Tech Stack:** Python 3.12, psycopg3 (`psycopg[binary]>=3.0`), stdlib `asyncio`, `pyarrow>=20`, `pytest-asyncio>=1.3.0`

---

## Prerequisites

- [ ] **Check out the feature branch**

```bash
git checkout -b eywalker/plt-1453-add-async-methods-to-dbconnectorprotocol-implement-across
```

---

## File map

| File | Action | Purpose |
|---|---|---|
| `src/orcapod/protocols/async_db_connector_protocol.py` | Create | `AsyncDBConnectorProtocol` definition |
| `src/orcapod/protocols/__init__.py` | Modify | Export `AsyncDBConnectorProtocol` |
| `src/orcapod/databases/__init__.py` | Modify | Export `AsyncDBConnectorProtocol` |
| `src/orcapod/databases/postgresql_connector.py` | Modify | Async lifecycle, schema, iter_batches |
| `src/orcapod/databases/sqlite_connector.py` | Modify | asyncio.to_thread() wrappers |
| `tests/test_databases/test_postgresql_connector.py` | Modify | `TestAsyncMethods` unit tests |
| `tests/test_databases/test_sqlite_connector.py` | Modify | `TestAsyncMethods` unit tests |
| `tests/test_databases/test_postgresql_connector_integration.py` | Modify | Async integration tests |

---

## Task 1: Define `AsyncDBConnectorProtocol` and wire up exports

**Files:**
- Create: `src/orcapod/protocols/async_db_connector_protocol.py`
- Modify: `src/orcapod/protocols/__init__.py`
- Modify: `src/orcapod/databases/__init__.py`

- [ ] **Step 1: Create the protocol file**

```python
# src/orcapod/protocols/async_db_connector_protocol.py
"""AsyncDBConnectorProtocol — async counterpart to DBConnectorProtocol.

Standalone protocol (does not inherit from DBConnectorProtocol). A class
satisfies both protocols by implementing all their methods.

Intended use::

    async with PostgreSQLConnector(dsn) as connector:
        tables = await connector.async_get_table_names()
        async for batch in connector.async_iter_batches('SELECT * FROM "t"'):
            process(batch)
"""
from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any, Protocol, TYPE_CHECKING, runtime_checkable

from orcapod.types import ColumnInfo

if TYPE_CHECKING:
    import pyarrow as pa

__all__ = ["AsyncDBConnectorProtocol"]


@runtime_checkable
class AsyncDBConnectorProtocol(Protocol):
    """Async interface for an external relational database backend.

    Standalone protocol — does not inherit from ``DBConnectorProtocol``. A
    connector class satisfies both protocols by implementing all their methods.

    Lifecycle: use ``async with connector:`` to open async resources.
    ``__aexit__`` calls ``async_close()``, which performs a full shutdown of
    both async and sync connections.

    Example::

        async with PostgreSQLConnector(dsn) as connector:
            tables = await connector.async_get_table_names()
            async for batch in connector.async_iter_batches('SELECT * FROM "t"'):
                process(batch)
    """

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def __aenter__(self) -> AsyncDBConnectorProtocol:
        """Open async resources and return self."""
        ...

    async def __aexit__(self, *args: Any) -> None:
        """Close all resources by calling ``async_close()``."""
        ...

    async def async_close(self) -> None:
        """Release all async and sync database resources. Idempotent."""
        ...

    # ── Schema introspection ──────────────────────────────────────────────────

    async def async_get_table_names(self) -> list[str]:
        """Return all available table names in this database."""
        ...

    async def async_get_pk_columns(self, table_name: str) -> list[str]:
        """Return primary-key column names for a table, in key-sequence order.

        Returns an empty list if the table has no primary key.
        """
        ...

    async def async_get_column_info(self, table_name: str) -> list[ColumnInfo]:
        """Return column metadata for a table, with types mapped to Arrow."""
        ...

    # ── Read ──────────────────────────────────────────────────────────────────

    def async_iter_batches(
        self,
        query: str,
        params: Any = None,
        batch_size: int = 1000,
    ) -> AsyncIterator[pa.RecordBatch]:
        """Execute a query and yield results as Arrow RecordBatches.

        Args:
            query: SQL query string. Table names should be double-quoted
                (``SELECT * FROM "my_table"``).
            params: Optional query parameters (connector-specific format).
            batch_size: Maximum rows per yielded batch.
        """
        ...
```

- [ ] **Step 2: Export from `orcapod.protocols`**

Add one import to `src/orcapod/protocols/__init__.py`:

```python
from orcapod.protocols.observability_protocols import (
    ExecutionObserverProtocol,
    DataExecutionLoggerProtocol,
)
from orcapod.protocols.async_db_connector_protocol import AsyncDBConnectorProtocol
```

- [ ] **Step 3: Export from `orcapod.databases`**

In `src/orcapod/databases/__init__.py`, add the import and update `__all__`:

```python
from .connector_arrow_database import ConnectorArrowDatabase
from .delta_lake_databases import DeltaTableDatabase
from .in_memory_databases import InMemoryArrowDatabase
from .noop_database import NoOpArrowDatabase
from .spiraldb_connector import SpiralDBConnector
from .sqlite_connector import SQLiteConnector
from .postgresql_connector import PostgreSQLConnector
from orcapod.protocols.async_db_connector_protocol import AsyncDBConnectorProtocol

__all__ = [
    "AsyncDBConnectorProtocol",
    "ConnectorArrowDatabase",
    "DeltaTableDatabase",
    "InMemoryArrowDatabase",
    "NoOpArrowDatabase",
    "SpiralDBConnector",
    "SQLiteConnector",
    "PostgreSQLConnector",
]

# Relational DB connector implementations satisfy DBConnectorProtocol
# (orcapod.protocols.db_connector_protocol) and can be passed to either
# ConnectorArrowDatabase (read+write ArrowDatabaseProtocol) or
# DBTableSource (read-only Source):
#
#   SQLiteConnector      -- PLT-1076 (stdlib sqlite3, zero extra deps)  ✓
#   PostgreSQLConnector  -- PLT-1075 (psycopg3)                          ✓
#   SpiralDBConnector    -- PLT-1074
#
# Connectors that also implement AsyncDBConnectorProtocol:
#
#   PostgreSQLConnector  -- native psycopg3 async (PLT-1453)              ✓
#   SQLiteConnector      -- asyncio.to_thread() wrappers (PLT-1453)       ✓
#   SpiralDBConnector    -- deferred to PLT-1456
#
# ArrowDatabaseProtocol backends (existing, not connector-based):
#
#   DeltaTableDatabase    -- Delta Lake (deltalake package)
#   InMemoryArrowDatabase -- pure in-memory, for tests
#   NoOpArrowDatabase     -- no-op, for dry-runs / benchmarks
```

- [ ] **Step 4: Verify imports work**

```bash
uv run python -c "
from orcapod.protocols.async_db_connector_protocol import AsyncDBConnectorProtocol
from orcapod.protocols import AsyncDBConnectorProtocol as P2
from orcapod.databases import AsyncDBConnectorProtocol as P3
print('OK', AsyncDBConnectorProtocol, P2, P3)
"
```

Expected: prints `OK` with the class three times. No import errors.

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/protocols/async_db_connector_protocol.py \
        src/orcapod/protocols/__init__.py \
        src/orcapod/databases/__init__.py
git commit -m "feat(protocols): add AsyncDBConnectorProtocol (PLT-1453)"
```

---

## Task 2: `PostgreSQLConnector` — async lifecycle

**Files:**
- Modify: `src/orcapod/databases/postgresql_connector.py`
- Modify: `tests/test_databases/test_postgresql_connector.py`

- [ ] **Step 1: Write the failing tests**

Add this class to the end of `tests/test_databases/test_postgresql_connector.py`.
Also add `AsyncMock` to the existing import line at the top:
```python
from unittest.mock import AsyncMock, MagicMock, patch
```

And add to the imports:
```python
from orcapod.protocols.async_db_connector_protocol import AsyncDBConnectorProtocol
```

Then add the test class:

```python
class TestAsyncLifecycle:
    """Unit tests for PostgreSQLConnector async lifecycle methods."""

    def _make_connector(self) -> PostgreSQLConnector:
        with patch("psycopg.connect") as mock_connect:
            mock_connect.return_value = MagicMock()
            return PostgreSQLConnector("postgresql://localhost/test")

    def test_isinstance_async_protocol(self) -> None:
        connector = self._make_connector()
        assert isinstance(connector, AsyncDBConnectorProtocol)
        connector._conn = None

    @pytest.mark.asyncio
    async def test_aenter_sets_async_conn(self) -> None:
        connector = self._make_connector()
        mock_async_conn = AsyncMock()
        with patch("psycopg.AsyncConnection.connect", new_callable=AsyncMock,
                   return_value=mock_async_conn):
            result = await connector.__aenter__()
        assert result is connector
        assert connector._async_conn is mock_async_conn
        connector._conn = None

    @pytest.mark.asyncio
    async def test_aexit_closes_async_and_sync(self) -> None:
        connector = self._make_connector()
        mock_async_conn = AsyncMock()
        connector._async_conn = mock_async_conn
        await connector.__aexit__(None, None, None)
        mock_async_conn.close.assert_called_once()
        assert connector._async_conn is None
        assert connector._conn is None  # sync also closed

    @pytest.mark.asyncio
    async def test_async_close_idempotent(self) -> None:
        connector = self._make_connector()
        mock_async_conn = AsyncMock()
        connector._async_conn = mock_async_conn
        await connector.async_close()
        await connector.async_close()  # must not raise
        mock_async_conn.close.assert_called_once()

    def test_require_async_open_raises_before_aenter(self) -> None:
        connector = self._make_connector()
        with pytest.raises(RuntimeError, match="async context manager"):
            connector._require_async_open()
        connector._conn = None

    def test_require_async_open_returns_conn_after_aenter(self) -> None:
        connector = self._make_connector()
        mock_async_conn = MagicMock()
        connector._async_conn = mock_async_conn
        result = connector._require_async_open()
        assert result is mock_async_conn
        connector._conn = None
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/test_databases/test_postgresql_connector.py::TestAsyncLifecycle -v
```

Expected: `ERRORS` or `FAILED` — `AsyncDBConnectorProtocol` import may pass but
`_require_async_open`, `_async_conn`, `__aenter__`, `__aexit__`, `async_close` don't
exist yet on `PostgreSQLConnector`.

- [ ] **Step 3: Implement async lifecycle in `PostgreSQLConnector`**

In `src/orcapod/databases/postgresql_connector.py`:

**a)** Add `_async_conn` to `__init__`. The existing `__init__` ends at line 233:

```python
def __init__(self, dsn: str) -> None:
    self._dsn = dsn
    self._conn: Any = psycopg.connect(dsn, autocommit=False)
    self._lock = threading.RLock()  # RLock required: iter_batches → _resolve_column_type_lookup → get_column_info re-enters the lock
    self._cursor_seq = itertools.count()
    self._async_conn: Any = None  # psycopg.AsyncConnection; set by __aenter__
```

**b)** Add `_require_async_open` after the existing `_require_open` method:

```python
def _require_async_open(self) -> Any:
    """Return the open async connection or raise RuntimeError if not entered.

    Raises:
        RuntimeError: If ``__aenter__`` has not been called.
    """
    if self._async_conn is None:
        raise RuntimeError(
            "PostgreSQLConnector: enter the async context manager before "
            "calling async methods"
        )
    return self._async_conn
```

**c)** Add async lifecycle methods in a new `# ── Async lifecycle ──` section,
placed after the existing `# ── Lifecycle ──` section (after `__exit__`):

```python
# ── Async lifecycle ───────────────────────────────────────────────────────

async def __aenter__(self) -> PostgreSQLConnector:
    """Open an async psycopg3 connection and return self.

    The async connection is used by ``async_get_table_names``,
    ``async_get_pk_columns``, and ``async_get_column_info``.
    ``async_iter_batches`` opens its own dedicated connection per call.
    """
    self._async_conn = await psycopg.AsyncConnection.connect(
        self._dsn, autocommit=False
    )
    return self

async def __aexit__(self, *args: Any) -> None:
    await self.async_close()

async def async_close(self) -> None:
    """Close async and sync connections. Idempotent."""
    if self._async_conn is not None:
        await self._async_conn.close()
        self._async_conn = None
    self.close()
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/test_databases/test_postgresql_connector.py::TestAsyncLifecycle -v
```

Expected: all 6 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/databases/postgresql_connector.py \
        tests/test_databases/test_postgresql_connector.py
git commit -m "feat(postgresql): add async lifecycle methods (PLT-1453)"
```

---

## Task 3: `PostgreSQLConnector` — async schema introspection

**Files:**
- Modify: `src/orcapod/databases/postgresql_connector.py`
- Modify: `tests/test_databases/test_postgresql_connector.py`

- [ ] **Step 1: Write failing tests**

Add this class to `tests/test_databases/test_postgresql_connector.py`:

```python
class TestAsyncSchemaIntrospectionUnit:
    """Unit tests for async schema introspection — mocked cursor."""

    def _make_connector_with_async_conn(self) -> PostgreSQLConnector:
        with patch("psycopg.connect") as mock_connect:
            mock_connect.return_value = MagicMock()
            connector = PostgreSQLConnector("postgresql://localhost/test")
        connector._async_conn = AsyncMock()
        return connector

    def _mock_cursor_returning(self, connector: PostgreSQLConnector,
                               rows: list) -> AsyncMock:
        """Wire up connector._async_conn.cursor() to return rows from fetchall."""
        mock_cursor = AsyncMock()
        mock_cursor.fetchall.return_value = rows
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_cursor)
        mock_cm.__aexit__ = AsyncMock(return_value=False)
        connector._async_conn.cursor.return_value = mock_cm
        return mock_cursor

    @pytest.mark.asyncio
    async def test_async_get_table_names(self) -> None:
        connector = self._make_connector_with_async_conn()
        self._mock_cursor_returning(connector, [("alpha",), ("beta",)])
        result = await connector.async_get_table_names()
        assert result == ["alpha", "beta"]
        connector._conn = None

    @pytest.mark.asyncio
    async def test_async_get_pk_columns_single(self) -> None:
        connector = self._make_connector_with_async_conn()
        self._mock_cursor_returning(connector, [("id",)])
        result = await connector.async_get_pk_columns("my_table")
        assert result == ["id"]
        connector._conn = None

    @pytest.mark.asyncio
    async def test_async_get_pk_columns_empty(self) -> None:
        connector = self._make_connector_with_async_conn()
        self._mock_cursor_returning(connector, [])
        result = await connector.async_get_pk_columns("my_table")
        assert result == []
        connector._conn = None

    @pytest.mark.asyncio
    async def test_async_get_pk_columns_raises_on_bad_name(self) -> None:
        connector = self._make_connector_with_async_conn()
        with pytest.raises(ValueError, match="double-quote"):
            await connector.async_get_pk_columns('bad"name')
        connector._conn = None

    @pytest.mark.asyncio
    async def test_async_get_column_info(self) -> None:
        connector = self._make_connector_with_async_conn()
        self._mock_cursor_returning(connector, [
            ("id", "text", "text", "NO"),
            ("val", "double precision", "float8", "YES"),
        ])
        result = await connector.async_get_column_info("my_table")
        assert len(result) == 2
        assert result[0].name == "id"
        assert result[0].arrow_type == pa.large_string()
        assert result[0].nullable is False
        assert result[1].name == "val"
        assert result[1].arrow_type == pa.float64()
        assert result[1].nullable is True
        connector._conn = None

    @pytest.mark.asyncio
    async def test_async_get_column_info_raises_on_bad_name(self) -> None:
        connector = self._make_connector_with_async_conn()
        with pytest.raises(ValueError, match="double-quote"):
            await connector.async_get_column_info('bad"name')
        connector._conn = None

    @pytest.mark.asyncio
    async def test_schema_methods_raise_without_aenter(self) -> None:
        with patch("psycopg.connect") as mock_connect:
            mock_connect.return_value = MagicMock()
            connector = PostgreSQLConnector("postgresql://localhost/test")
        # _async_conn is None — all methods should raise
        with pytest.raises(RuntimeError, match="async context manager"):
            await connector.async_get_table_names()
        connector._conn = None
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/test_databases/test_postgresql_connector.py::TestAsyncSchemaIntrospectionUnit -v
```

Expected: `FAILED` — methods `async_get_table_names`, `async_get_pk_columns`,
`async_get_column_info` don't exist yet.

- [ ] **Step 3: Implement async schema introspection in `postgresql_connector.py`**

Add a new `# ── Async schema introspection ──` section immediately after the
`# ── Schema introspection ──` section:

```python
# ── Async schema introspection ────────────────────────────────────────────

async def async_get_table_names(self) -> list[str]:
    """Return all user table names in this database (sorted, excludes views)."""
    conn = self._require_async_open()
    async with conn.cursor() as cur:
        await cur.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = current_schema()
              AND table_type = 'BASE TABLE'
            ORDER BY table_name
            """
        )
        return [row[0] for row in await cur.fetchall()]

async def async_get_pk_columns(self, table_name: str) -> list[str]:
    """Return primary-key column names in key-sequence order."""
    conn = self._require_async_open()
    self._validate_table_name(table_name)
    async with conn.cursor() as cur:
        await cur.execute(
            """
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
            """,
            (table_name,),
        )
        return [row[0] for row in await cur.fetchall()]

async def async_get_column_info(self, table_name: str) -> list[ColumnInfo]:
    """Return column metadata with Arrow-mapped types."""
    conn = self._require_async_open()
    self._validate_table_name(table_name)
    async with conn.cursor() as cur:
        await cur.execute(
            """
            SELECT column_name, data_type, udt_name, is_nullable
            FROM information_schema.columns
            WHERE table_schema = current_schema()
              AND table_name   = %s
            ORDER BY ordinal_position
            """,
            (table_name,),
        )
        return [
            ColumnInfo(
                name=row[0],
                arrow_type=_pg_type_to_arrow(row[1], row[2]),
                nullable=(row[3].upper() == "YES"),
            )
            for row in await cur.fetchall()
        ]
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/test_databases/test_postgresql_connector.py::TestAsyncSchemaIntrospectionUnit -v
```

Expected: all 7 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/databases/postgresql_connector.py \
        tests/test_databases/test_postgresql_connector.py
git commit -m "feat(postgresql): add async schema introspection methods (PLT-1453)"
```

---

## Task 4: `PostgreSQLConnector` — `async_iter_batches`

**Files:**
- Modify: `src/orcapod/databases/postgresql_connector.py`
- Modify: `tests/test_databases/test_postgresql_connector.py`

- [ ] **Step 1: Write failing tests**

Add this class to `tests/test_databases/test_postgresql_connector.py`:

```python
class TestAsyncIterBatchesUnit:
    """Unit tests for async_iter_batches — mocked psycopg async connection."""

    def _make_connector(self) -> PostgreSQLConnector:
        with patch("psycopg.connect") as mock_connect:
            mock_connect.return_value = MagicMock()
            connector = PostgreSQLConnector("postgresql://localhost/test")
        # Simulate entered context
        connector._async_conn = AsyncMock()
        return connector

    def _make_mock_read_conn(self, rows_per_call: list[list]) -> tuple[AsyncMock, AsyncMock]:
        """Return (mock_read_conn, mock_cursor).

        rows_per_call: list of row lists returned by successive fetchmany calls.
        The last entry must be an empty list to signal end-of-results.
        """
        mock_cursor = AsyncMock()
        col = MagicMock()
        col.name = "id"
        mock_cursor.description = [col]
        mock_cursor.fetchmany = AsyncMock(side_effect=rows_per_call)

        mock_read_conn = AsyncMock()
        mock_read_conn.cursor.return_value = mock_cursor

        return mock_read_conn, mock_cursor

    @pytest.mark.asyncio
    async def test_yields_correct_row_count(self) -> None:
        connector = self._make_connector()
        mock_read_conn, _ = self._make_mock_read_conn(
            [[(1,), (2,), (3,)], []]
        )
        with patch("psycopg.AsyncConnection.connect", new_callable=AsyncMock,
                   return_value=mock_read_conn):
            batches = [b async for b in connector.async_iter_batches(
                'SELECT * FROM "t"'
            )]
        assert sum(b.num_rows for b in batches) == 3
        connector._conn = None

    @pytest.mark.asyncio
    async def test_multiple_batches(self) -> None:
        connector = self._make_connector()
        mock_read_conn, _ = self._make_mock_read_conn(
            [[(1,), (2,)], [(3,)], []]
        )
        with patch("psycopg.AsyncConnection.connect", new_callable=AsyncMock,
                   return_value=mock_read_conn):
            batches = [b async for b in connector.async_iter_batches(
                'SELECT * FROM "t"', batch_size=2
            )]
        assert len(batches) == 2
        assert batches[0].num_rows == 2
        assert batches[1].num_rows == 1
        connector._conn = None

    @pytest.mark.asyncio
    async def test_empty_result(self) -> None:
        connector = self._make_connector()
        mock_cursor = AsyncMock()
        mock_cursor.description = None
        mock_read_conn = AsyncMock()
        mock_read_conn.cursor.return_value = mock_cursor
        with patch("psycopg.AsyncConnection.connect", new_callable=AsyncMock,
                   return_value=mock_read_conn):
            batches = [b async for b in connector.async_iter_batches(
                'SELECT * FROM "t"'
            )]
        assert batches == []
        connector._conn = None

    @pytest.mark.asyncio
    async def test_cursor_closed_on_completion(self) -> None:
        connector = self._make_connector()
        mock_read_conn, mock_cursor = self._make_mock_read_conn(
            [[(1,)], []]
        )
        with patch("psycopg.AsyncConnection.connect", new_callable=AsyncMock,
                   return_value=mock_read_conn):
            _ = [b async for b in connector.async_iter_batches('SELECT * FROM "t"')]
        mock_cursor.close.assert_called_once()
        mock_read_conn.close.assert_called_once()
        connector._conn = None

    @pytest.mark.asyncio
    async def test_cursor_closed_on_early_abandonment(self) -> None:
        connector = self._make_connector()
        mock_read_conn, mock_cursor = self._make_mock_read_conn(
            [[(1,)], [(2,)], []]
        )
        with patch("psycopg.AsyncConnection.connect", new_callable=AsyncMock,
                   return_value=mock_read_conn):
            gen = connector.async_iter_batches('SELECT * FROM "t"', batch_size=1)
            await gen.__anext__()  # consume one batch
            await gen.aclose()    # abandon mid-stream
        mock_cursor.close.assert_called_once()
        mock_read_conn.close.assert_called_once()
        connector._conn = None

    @pytest.mark.asyncio
    async def test_raises_without_aenter(self) -> None:
        with patch("psycopg.connect") as mock_connect:
            mock_connect.return_value = MagicMock()
            connector = PostgreSQLConnector("postgresql://localhost/test")
        # _async_conn is None
        with pytest.raises(RuntimeError, match="async context manager"):
            async for _ in connector.async_iter_batches('SELECT * FROM "t"'):
                pass
        connector._conn = None
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/test_databases/test_postgresql_connector.py::TestAsyncIterBatchesUnit -v
```

Expected: `FAILED` — `async_iter_batches` not defined yet.

- [ ] **Step 3: Implement `_async_resolve_column_type_lookup` and `async_iter_batches`**

**a)** Add the module-level coroutine after `_resolve_column_type_lookup` in
`src/orcapod/databases/postgresql_connector.py`:

```python
async def _async_resolve_column_type_lookup(
    query: str,
    connector: "PostgreSQLConnector",
) -> dict[str, pa.DataType]:
    """Async version of ``_resolve_column_type_lookup``.

    Parses the FROM clause of query to find the source table, then returns a
    column-name → Arrow-type dict by calling ``async_get_column_info``.

    Returns an empty dict if no single unambiguous table can be identified,
    causing ``async_iter_batches`` to fall back to ``pa.large_string()`` for
    all columns.

    Args:
        query: SQL query string.
        connector: The connector to call ``async_get_column_info`` on.

    Returns:
        Dict mapping column name to Arrow DataType.
    """
    if re.search(r"\bJOIN\b", query, re.IGNORECASE):
        return {}

    from_pattern = re.compile(
        r'\bFROM\b\s+(?:"([^"]+)"|(\w+))',
        re.IGNORECASE,
    )
    from_matches = list(from_pattern.finditer(query))
    if len(from_matches) != 1:
        return {}

    match = from_matches[0]
    table_name = match.group(1) or match.group(2)

    from_tail = query[match.end():]
    clause_boundary = re.search(
        r"\b(WHERE|GROUP\s+BY|ORDER\s+BY|LIMIT|OFFSET|HAVING|UNION|EXCEPT|INTERSECT)\b",
        from_tail,
        re.IGNORECASE,
    )
    if clause_boundary:
        from_tail = from_tail[: clause_boundary.start()]

    if "," in from_tail:
        return {}

    return {ci.name: ci.arrow_type for ci in await connector.async_get_column_info(table_name)}
```

**b)** Add `async_iter_batches` inside `PostgreSQLConnector` in the new
`# ── Async read ──` section, placed after the async schema introspection section:

```python
# ── Async read ────────────────────────────────────────────────────────────

async def async_iter_batches(
    self,
    query: str,
    params: Any = None,
    batch_size: int = 1000,
) -> AsyncIterator[pa.RecordBatch]:
    """Execute a query and yield results as Arrow RecordBatches.

    Opens a dedicated ``AsyncConnection`` per call so that the server-side
    cursor portal is isolated from operations on the shared ``_async_conn``.

    Args:
        query: SQL query string. Table names should be double-quoted.
        params: Optional query parameters.
        batch_size: Maximum rows per yielded batch.

    Raises:
        RuntimeError: If the async context manager has not been entered.
    """
    import pyarrow as _pa
    from collections.abc import AsyncIterator as _AsyncIterator  # noqa: F401

    self._require_async_open()  # guard: raise if context manager not entered

    # Briefly acquire the sync lock only to read self._dsn safely.
    with self._lock:
        self._require_open()  # guard: raise if sync connector is closed
        dsn = self._dsn

    read_conn = await psycopg.AsyncConnection.connect(dsn, autocommit=False)
    cursor_name = f"orcapod_{next(self._cursor_seq)}"
    cur = read_conn.cursor(name=cursor_name)

    try:
        await cur.execute(query, params)
        if cur.description is None:
            return
        col_names = [d.name for d in cur.description]
        type_lookup = await _async_resolve_column_type_lookup(query, self)
        arrow_types = [type_lookup.get(n, _pa.large_string()) for n in col_names]
        schema = _pa.schema(
            [_pa.field(n, t) for n, t in zip(col_names, arrow_types)]
        )
        rows = await cur.fetchmany(batch_size)

        while rows:
            arrays = [
                _pa.array([r[i] for r in rows], type=t)
                for i, t in enumerate(arrow_types)
            ]
            yield _pa.RecordBatch.from_arrays(arrays, schema=schema)
            rows = await cur.fetchmany(batch_size)
    finally:
        await cur.close()
        try:
            await read_conn.rollback()
        except Exception:
            pass
        await read_conn.close()
```

Also add `AsyncIterator` to the `collections.abc` import at the top of the file:

```python
from collections.abc import Iterator, AsyncIterator
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/test_databases/test_postgresql_connector.py::TestAsyncIterBatchesUnit -v
```

Expected: all 6 tests PASS.

- [ ] **Step 5: Run all PostgreSQL unit tests**

```bash
uv run pytest tests/test_databases/test_postgresql_connector.py -v
```

Expected: all tests PASS (existing + new).

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/databases/postgresql_connector.py \
        tests/test_databases/test_postgresql_connector.py
git commit -m "feat(postgresql): add async_iter_batches (PLT-1453)"
```

---

## Task 5: `SQLiteConnector` — all async methods

**Files:**
- Modify: `src/orcapod/databases/sqlite_connector.py`
- Modify: `tests/test_databases/test_sqlite_connector.py`

- [ ] **Step 1: Write failing tests**

Add the following class to the end of `tests/test_databases/test_sqlite_connector.py`.
Also add this import at the top of the file:

```python
from orcapod.protocols.async_db_connector_protocol import AsyncDBConnectorProtocol
```

Then the test class:

```python
class TestAsyncMethods:
    """Async method tests using a real in-memory SQLite DB — no mocks needed."""

    def _setup(self) -> SQLiteConnector:
        """Return a connector with a simple table pre-populated."""
        connector = SQLiteConnector(":memory:")
        connector._conn.execute(
            'CREATE TABLE "t" (id INTEGER PRIMARY KEY, val TEXT NOT NULL)'
        )
        connector._conn.execute(
            'INSERT INTO "t" VALUES (1, "a"), (2, "b"), (3, "c")'
        )
        return connector

    def test_isinstance_async_protocol(self) -> None:
        connector = SQLiteConnector(":memory:")
        assert isinstance(connector, AsyncDBConnectorProtocol)

    @pytest.mark.asyncio
    async def test_aenter_returns_self(self) -> None:
        connector = SQLiteConnector(":memory:")
        result = await connector.__aenter__()
        assert result is connector
        await connector.async_close()

    @pytest.mark.asyncio
    async def test_aexit_closes_connection(self) -> None:
        connector = SQLiteConnector(":memory:")
        await connector.__aenter__()
        await connector.__aexit__(None, None, None)
        with pytest.raises(RuntimeError, match="closed"):
            connector._require_open()

    @pytest.mark.asyncio
    async def test_async_close_idempotent(self) -> None:
        connector = SQLiteConnector(":memory:")
        await connector.async_close()
        await connector.async_close()  # must not raise

    @pytest.mark.asyncio
    async def test_async_context_manager(self) -> None:
        async with SQLiteConnector(":memory:") as connector:
            assert connector._conn is not None
        with pytest.raises(RuntimeError, match="closed"):
            connector._require_open()

    @pytest.mark.asyncio
    async def test_async_get_table_names_matches_sync(self) -> None:
        connector = self._setup()
        assert await connector.async_get_table_names() == connector.get_table_names()
        await connector.async_close()

    @pytest.mark.asyncio
    async def test_async_get_pk_columns_matches_sync(self) -> None:
        connector = self._setup()
        assert (
            await connector.async_get_pk_columns("t") == connector.get_pk_columns("t")
        )
        await connector.async_close()

    @pytest.mark.asyncio
    async def test_async_get_column_info_matches_sync(self) -> None:
        connector = self._setup()
        assert (
            await connector.async_get_column_info("t")
            == connector.get_column_info("t")
        )
        await connector.async_close()

    @pytest.mark.asyncio
    async def test_async_iter_batches_returns_correct_rows(self) -> None:
        connector = self._setup()
        batches = [b async for b in connector.async_iter_batches('SELECT * FROM "t"')]
        total_rows = sum(b.num_rows for b in batches)
        assert total_rows == 3
        await connector.async_close()

    @pytest.mark.asyncio
    async def test_async_iter_batches_matches_sync_data(self) -> None:
        import pyarrow as pa
        connector = self._setup()

        sync_table = pa.Table.from_batches(
            list(connector.iter_batches('SELECT * FROM "t"'))
        )
        async_table = pa.Table.from_batches(
            [b async for b in connector.async_iter_batches('SELECT * FROM "t"')]
        )
        assert async_table.equals(sync_table)
        await connector.async_close()

    @pytest.mark.asyncio
    async def test_async_iter_batches_empty_result(self) -> None:
        connector = self._setup()
        batches = [
            b async for b in connector.async_iter_batches(
                'SELECT * FROM "t" WHERE 1=0'
            )
        ]
        assert batches == []
        await connector.async_close()
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/test_databases/test_sqlite_connector.py::TestAsyncMethods -v
```

Expected: `FAILED` — `async_get_table_names`, `async_iter_batches`, etc. don't exist yet;
`__aenter__`/`__aexit__` don't exist either.

- [ ] **Step 3: Implement all async methods in `sqlite_connector.py`**

Add `import asyncio` to the imports at the top of `src/orcapod/databases/sqlite_connector.py`
(alongside the existing `import threading`):

```python
import asyncio
import logging
import os
import sqlite3
import threading
```

Also add `AsyncIterator` to the `collections.abc` import:

```python
from collections.abc import AsyncIterator, Iterator
```

Then add a new `# ── Async lifecycle ──` section after the existing `# ── Lifecycle ──`
section (after `__exit__`), and a `# ── Async read ──` section after `# ── Write ──`:

```python
# ── Async lifecycle ───────────────────────────────────────────────────────

async def __aenter__(self) -> SQLiteConnector:
    """Return self; the sync connection is already open from ``__init__``."""
    return self

async def __aexit__(self, *args: Any) -> None:
    await self.async_close()

async def async_close(self) -> None:
    """Close the database connection. Idempotent."""
    await asyncio.to_thread(self.close)

# ── Async schema introspection ────────────────────────────────────────────

async def async_get_table_names(self) -> list[str]:
    """Return all user table names asynchronously."""
    return await asyncio.to_thread(self.get_table_names)

async def async_get_pk_columns(self, table_name: str) -> list[str]:
    """Return primary-key column names asynchronously."""
    return await asyncio.to_thread(self.get_pk_columns, table_name)

async def async_get_column_info(self, table_name: str) -> list[ColumnInfo]:
    """Return column metadata asynchronously."""
    return await asyncio.to_thread(self.get_column_info, table_name)

# ── Async read ────────────────────────────────────────────────────────────

async def async_iter_batches(
    self,
    query: str,
    params: Any = None,
    batch_size: int = 1000,
) -> AsyncIterator[pa.RecordBatch]:
    """Execute a query and yield results as Arrow RecordBatches.

    Runs the full synchronous iteration in a thread-pool worker (so blocking
    I/O does not stall the event loop), then yields each collected batch.

    Args:
        query: SQL query string. Table names should be double-quoted.
        params: Optional query parameters.
        batch_size: Maximum rows per yielded batch.
    """
    batches = await asyncio.to_thread(
        lambda: list(self.iter_batches(query, params, batch_size))
    )
    for batch in batches:
        yield batch
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/test_databases/test_sqlite_connector.py::TestAsyncMethods -v
```

Expected: all 11 tests PASS.

- [ ] **Step 5: Run all SQLite unit tests**

```bash
uv run pytest tests/test_databases/test_sqlite_connector.py -v
```

Expected: all tests PASS (existing + new).

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/databases/sqlite_connector.py \
        tests/test_databases/test_sqlite_connector.py
git commit -m "feat(sqlite): add async methods via asyncio.to_thread() (PLT-1453)"
```

---

## Task 6: PostgreSQL async integration tests

**Files:**
- Modify: `tests/test_databases/test_postgresql_connector_integration.py`

These tests require a live PostgreSQL instance. They are marked `@pytest.mark.postgres`
and skipped in normal test runs. Run with:
```bash
uv run pytest tests/test_databases/test_postgresql_connector_integration.py -m postgres -v
```

- [ ] **Step 1: Add async imports to the integration test file**

At the top of `tests/test_databases/test_postgresql_connector_integration.py`, add:

```python
import asyncio  # already stdlib, no new dep
```

(The file already imports `pytest`, `psycopg`, `pa`, `PostgreSQLConnector`, `ColumnInfo`.)

- [ ] **Step 2: Add `TestAsyncLifecycle`**

Append to the end of the file:

```python
# ---------------------------------------------------------------------------
# Async lifecycle
# ---------------------------------------------------------------------------


@pytest.mark.postgres
class TestAsyncLifecycle:
    @pytest.mark.asyncio
    async def test_aenter_opens_async_conn(self, connector: PostgreSQLConnector) -> None:
        assert connector._async_conn is None
        async with connector:
            assert connector._async_conn is not None

    @pytest.mark.asyncio
    async def test_async_close_idempotent(self, connector: PostgreSQLConnector) -> None:
        async with connector:
            pass
        await connector.async_close()  # second call must not raise

    def test_require_async_open_raises_outside_context(
        self, connector: PostgreSQLConnector
    ) -> None:
        with pytest.raises(RuntimeError, match="async context manager"):
            connector._require_async_open()
```

- [ ] **Step 3: Add `TestAsyncSchemaIntrospection`**

```python
# ---------------------------------------------------------------------------
# Async schema introspection
# ---------------------------------------------------------------------------


@pytest.mark.postgres
class TestAsyncSchemaIntrospection:
    @pytest.mark.asyncio
    async def test_async_get_table_names_matches_sync(
        self, connector: PostgreSQLConnector
    ) -> None:
        with connector._conn.cursor() as cur:
            cur.execute('CREATE TABLE "t1" (id INTEGER PRIMARY KEY)')
            cur.execute('CREATE TABLE "t2" (id INTEGER PRIMARY KEY)')
        connector._conn.commit()
        async with connector:
            async_result = await connector.async_get_table_names()
        sync_result = connector.get_table_names()
        assert async_result == sync_result

    @pytest.mark.asyncio
    async def test_async_get_pk_columns_matches_sync(
        self, connector: PostgreSQLConnector
    ) -> None:
        with connector._conn.cursor() as cur:
            cur.execute('CREATE TABLE "t" (id TEXT PRIMARY KEY, val REAL)')
        connector._conn.commit()
        async with connector:
            async_result = await connector.async_get_pk_columns("t")
        sync_result = connector.get_pk_columns("t")
        assert async_result == sync_result

    @pytest.mark.asyncio
    async def test_async_get_column_info_matches_sync(
        self, connector: PostgreSQLConnector
    ) -> None:
        with connector._conn.cursor() as cur:
            cur.execute('CREATE TABLE "t" (id TEXT NOT NULL, val REAL)')
        connector._conn.commit()
        async with connector:
            async_result = await connector.async_get_column_info("t")
        sync_result = connector.get_column_info("t")
        assert async_result == sync_result
```

- [ ] **Step 4: Add `TestAsyncIterBatches`**

```python
# ---------------------------------------------------------------------------
# Async iter_batches
# ---------------------------------------------------------------------------


@pytest.mark.postgres
class TestAsyncIterBatches:
    def _setup_data(self, connector: PostgreSQLConnector) -> None:
        cols = [
            ColumnInfo("id", pa.large_string(), nullable=False),
            ColumnInfo("score", pa.float64(), nullable=True),
        ]
        connector.create_table_if_not_exists("data", cols, "id")
        records = pa.table({
            "id": pa.array(["a", "b", "c"], type=pa.large_string()),
            "score": pa.array([1.5, 2.5, 3.5], type=pa.float64()),
        })
        connector.upsert_records("data", records, "id")

    @pytest.mark.asyncio
    async def test_returns_all_rows(self, connector: PostgreSQLConnector) -> None:
        self._setup_data(connector)
        async with connector:
            batches = [
                b async for b in connector.async_iter_batches('SELECT * FROM "data"')
            ]
        assert sum(b.num_rows for b in batches) == 3

    @pytest.mark.asyncio
    async def test_correct_arrow_types(self, connector: PostgreSQLConnector) -> None:
        self._setup_data(connector)
        async with connector:
            batches = [
                b async for b in connector.async_iter_batches('SELECT * FROM "data"')
            ]
        table = pa.Table.from_batches(batches)
        assert table.schema.field("id").type == pa.large_string()
        assert table.schema.field("score").type == pa.float64()

    @pytest.mark.asyncio
    async def test_batch_size_respected(self, connector: PostgreSQLConnector) -> None:
        self._setup_data(connector)
        async with connector:
            batches = [
                b async for b in connector.async_iter_batches(
                    'SELECT * FROM "data"', batch_size=2
                )
            ]
        assert len(batches) == 2
        assert batches[0].num_rows == 2
        assert batches[1].num_rows == 1

    @pytest.mark.asyncio
    async def test_empty_result(self, connector: PostgreSQLConnector) -> None:
        self._setup_data(connector)
        async with connector:
            batches = [
                b async for b in connector.async_iter_batches(
                    'SELECT * FROM "data" WHERE 1=0'
                )
            ]
        assert batches == []

    @pytest.mark.asyncio
    async def test_early_abandonment_closes_cursor(
        self, connector: PostgreSQLConnector
    ) -> None:
        self._setup_data(connector)
        async with connector:
            gen = connector.async_iter_batches('SELECT * FROM "data"', batch_size=1)
            await gen.__anext__()  # consume one batch
            await gen.aclose()    # abandon — must not leak server-side cursor
            # Subsequent queries must still work
            batches = [
                b async for b in connector.async_iter_batches('SELECT * FROM "data"')
            ]
        assert sum(b.num_rows for b in batches) == 3
```

- [ ] **Step 5: Run integration tests (requires live PostgreSQL)**

```bash
uv run pytest tests/test_databases/test_postgresql_connector_integration.py -m postgres -v
```

Expected: all async test classes PASS against the live database.
If no live PostgreSQL is available locally, these run automatically in CI
(the GitHub Actions workflow starts a PostgreSQL service container).

- [ ] **Step 6: Commit**

```bash
git add tests/test_databases/test_postgresql_connector_integration.py
git commit -m "test(postgresql): add async integration tests (PLT-1453)"
```

---

## Task 7: Final verification and PR

- [ ] **Step 1: Run all connector unit tests**

```bash
uv run pytest tests/test_databases/ -v --ignore=tests/test_databases/test_postgresql_connector_integration.py --ignore=tests/test_databases/test_sqlite_connector_integration.py --ignore=tests/test_databases/test_spiraldb_connector_integration.py
```

Expected: all tests PASS.

- [ ] **Step 2: Run the full test suite**

```bash
uv run pytest tests/ -v --ignore=tests/test_databases/test_postgresql_connector_integration.py --ignore=tests/test_databases/test_sqlite_connector_integration.py --ignore=tests/test_databases/test_spiraldb_connector_integration.py
```

Expected: all tests PASS. No regressions.

- [ ] **Step 3: Open PR against `main`**

```bash
gh pr create \
  --title "feat(connectors): add AsyncDBConnectorProtocol; implement on PostgreSQL and SQLite" \
  --base main \
  --body "$(cat <<'EOF'
## Summary

- Adds `AsyncDBConnectorProtocol` — a standalone `@runtime_checkable` Protocol with 7 async methods (`__aenter__`, `__aexit__`, `async_close`, `async_get_table_names`, `async_get_pk_columns`, `async_get_column_info`, `async_iter_batches`)
- `PostgreSQLConnector`: native async via psycopg3 `AsyncConnection` / `AsyncServerCursor`; dedicated connection per `async_iter_batches` call for portal isolation
- `SQLiteConnector`: `asyncio.to_thread()` wrappers over existing sync methods (thread-safe: `check_same_thread=False` + `threading.RLock` already in place)
- `SpiralDBConnector` deferred to PLT-1456 (lacks thread safety prerequisite)
- Unit tests for both connectors; async integration tests for PostgreSQL

## Test plan

- [ ] `uv run pytest tests/test_databases/test_postgresql_connector.py -v` — PASS
- [ ] `uv run pytest tests/test_databases/test_sqlite_connector.py -v` — PASS
- [ ] `uv run pytest tests/test_databases/test_postgresql_connector_integration.py -m postgres -v` — PASS (CI)

Closes PLT-1453
EOF
)"
```
