# PostgreSQLConnector Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement `PostgreSQLConnector` — a `DBConnectorProtocol` implementation backed by psycopg3 that gives `ConnectorArrowDatabase` and `DBTableSource` a PostgreSQL backend.

**Architecture:** Mirror `SQLiteConnector` exactly in structure. Module-level type-mapping helpers (`_pg_type_to_arrow`, `_arrow_type_to_pg_sql`, `_resolve_column_type_lookup`) are pure functions kept separate from the class so a future `AsyncPostgreSQLConnector` can reuse them. Schema introspection uses `information_schema`. Reads use a psycopg3 named server-side cursor for true streaming. Writes use `ON CONFLICT` upsert and commit immediately.

**Tech Stack:** Python 3.11+, `psycopg` (psycopg3, already installed), `pyarrow`, `pytest`, `pytest-postgresql` (already installed)

---

## File Map

| Action | Path | Responsibility |
|--------|------|----------------|
| Create | `src/orcapod/databases/postgresql_connector.py` | Full `PostgreSQLConnector` implementation + module-level helpers |
| Create | `tests/test_databases/test_postgresql_connector.py` | Unit tests (no live DB, mocked psycopg) |
| Create | `tests/test_databases/test_postgresql_connector_integration.py` | Integration tests (live PG via pytest-postgresql) |
| Modify | `src/orcapod/databases/__init__.py` | Export `PostgreSQLConnector` |
| Modify | `pytest.ini` | Register `postgres` marker |

---

## Reference

Before starting, read these files:

- **Protocol to implement:** `src/orcapod/protocols/db_connector_protocol.py`
- **Reference implementation:** `src/orcapod/databases/sqlite_connector.py`
- **Generic DB layer that consumes this connector:** `src/orcapod/databases/connector_arrow_database.py`
- **Type used throughout:** `src/orcapod/types.py` — look for `ColumnInfo`
- **Approved spec:** `superpowers/specs/2026-03-25-postgresql-connector-design.md`

The SQLite connector is your closest guide — match its patterns for locking, error messages, `_require_open`, `_validate_table_name`, `to_config`/`from_config`, and test structure.

---

## Task 1: Register `postgres` pytest marker

**Files:**
- Modify: `pytest.ini`

- [ ] **Step 1: Add the marker**

Open `pytest.ini` and add `markers` to the existing `[pytest]` section:

```ini
[pytest]
testpaths = tests
python_files = test_*.py
python_classes = Test*
python_functions = test_*
addopts = -v
markers =
    postgres: mark test as requiring a live PostgreSQL instance (skipped if unavailable)
```

- [ ] **Step 2: Verify pytest recognises it**

```bash
pytest --markers | grep postgres
```

Expected output contains: `postgres: mark test as requiring a live PostgreSQL instance`

- [ ] **Step 3: Commit**

```bash
git add pytest.ini
git commit -m "test: register postgres pytest marker (PLT-1075)"
```

---

## Task 2: Type-mapping helpers — unit tests first

**Files:**
- Create: `tests/test_databases/test_postgresql_connector.py`

Write just the type-mapping test classes first. The implementation file doesn't exist yet — that's fine, the tests will fail with `ModuleNotFoundError`.

- [ ] **Step 1: Create the unit test file with type-mapping tests**

```python
# tests/test_databases/test_postgresql_connector.py
"""Unit tests for PostgreSQLConnector — no live database required."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from orcapod.databases.postgresql_connector import (
    PostgreSQLConnector,
    _arrow_type_to_pg_sql,
    _pg_type_to_arrow,
)
from orcapod.protocols.db_connector_protocol import DBConnectorProtocol
from orcapod.types import ColumnInfo


class TestPgTypeToArrow:
    def test_bool(self):
        assert _pg_type_to_arrow("bool", "bool") == pa.bool_()

    def test_int2(self):
        assert _pg_type_to_arrow("int2", "int2") == pa.int32()

    def test_int4(self):
        assert _pg_type_to_arrow("int4", "int4") == pa.int32()

    def test_int8(self):
        assert _pg_type_to_arrow("int8", "int8") == pa.int64()

    def test_float4(self):
        assert _pg_type_to_arrow("float4", "float4") == pa.float32()

    def test_float8(self):
        assert _pg_type_to_arrow("float8", "float8") == pa.float64()

    def test_numeric(self):
        assert _pg_type_to_arrow("numeric", "numeric") == pa.float64()

    def test_text(self):
        assert _pg_type_to_arrow("text", "text") == pa.large_string()

    def test_varchar(self):
        assert _pg_type_to_arrow("character varying", "varchar") == pa.large_string()

    def test_char(self):
        assert _pg_type_to_arrow("character", "bpchar") == pa.large_string()

    def test_name(self):
        assert _pg_type_to_arrow("name", "name") == pa.large_string()

    def test_bytea(self):
        assert _pg_type_to_arrow("bytea", "bytea") == pa.large_binary()

    def test_uuid(self):
        assert _pg_type_to_arrow("uuid", "uuid") == pa.large_string()

    def test_json(self):
        assert _pg_type_to_arrow("json", "json") == pa.large_string()

    def test_jsonb(self):
        assert _pg_type_to_arrow("jsonb", "jsonb") == pa.large_string()

    def test_date(self):
        assert _pg_type_to_arrow("date", "date") == pa.date32()

    def test_timestamp(self):
        assert _pg_type_to_arrow("timestamp without time zone", "timestamp") == pa.timestamp("us")

    def test_timestamptz(self):
        assert _pg_type_to_arrow("timestamp with time zone", "timestamptz") == pa.timestamp("us", tz="UTC")

    def test_time_is_string(self):
        result = _pg_type_to_arrow("time without time zone", "time")
        assert result == pa.large_string()

    def test_timetz_is_string(self):
        result = _pg_type_to_arrow("time with time zone", "timetz")
        assert result == pa.large_string()

    def test_int4_array(self):
        result = _pg_type_to_arrow("ARRAY", "_int4")
        assert result == pa.large_list(pa.int32())

    def test_text_array(self):
        result = _pg_type_to_arrow("ARRAY", "_text")
        assert result == pa.large_list(pa.large_string())

    def test_unknown_falls_back_to_large_string(self):
        result = _pg_type_to_arrow("weird_type", "weird_type")
        assert result == pa.large_string()


class TestArrowTypeToPgSql:
    def test_bool(self):
        assert _arrow_type_to_pg_sql(pa.bool_()) == "BOOLEAN"

    def test_int32(self):
        assert _arrow_type_to_pg_sql(pa.int32()) == "INTEGER"

    def test_int64(self):
        assert _arrow_type_to_pg_sql(pa.int64()) == "BIGINT"

    def test_float32(self):
        assert _arrow_type_to_pg_sql(pa.float32()) == "REAL"

    def test_float64(self):
        assert _arrow_type_to_pg_sql(pa.float64()) == "DOUBLE PRECISION"

    def test_large_string(self):
        assert _arrow_type_to_pg_sql(pa.large_string()) == "TEXT"

    def test_string(self):
        assert _arrow_type_to_pg_sql(pa.utf8()) == "TEXT"

    def test_large_binary(self):
        assert _arrow_type_to_pg_sql(pa.large_binary()) == "BYTEA"

    def test_binary(self):
        assert _arrow_type_to_pg_sql(pa.binary()) == "BYTEA"

    def test_date32(self):
        assert _arrow_type_to_pg_sql(pa.date32()) == "DATE"

    def test_timestamp_no_tz(self):
        assert _arrow_type_to_pg_sql(pa.timestamp("us")) == "TIMESTAMP"

    def test_timestamp_with_tz(self):
        assert _arrow_type_to_pg_sql(pa.timestamp("us", tz="UTC")) == "TIMESTAMPTZ"

    def test_large_list_raises(self):
        with pytest.raises(ValueError, match="not supported"):
            _arrow_type_to_pg_sql(pa.large_list(pa.int32()))

    def test_unknown_falls_back_to_text(self):
        # pa.null() is an unusual type not in the mapping
        result = _arrow_type_to_pg_sql(pa.null())
        assert result == "TEXT"
```

- [ ] **Step 2: Run to confirm failure**

```bash
pytest tests/test_databases/test_postgresql_connector.py::TestPgTypeToArrow -v 2>&1 | head -20
```

Expected: `ModuleNotFoundError: No module named 'orcapod.databases.postgresql_connector'`

---

## Task 3: Implement type-mapping helpers

**Files:**
- Create: `src/orcapod/databases/postgresql_connector.py`

- [ ] **Step 1: Create the implementation file with type helpers only**

```python
"""PostgreSQLConnector — DBConnectorProtocol implementation backed by psycopg3.

Uses a named server-side cursor for iter_batches so PostgreSQL streams results
incrementally rather than buffering the full result set in memory.

Example::

    connector = PostgreSQLConnector("postgresql://user:pass@localhost:5432/mydb")
    db = ConnectorArrowDatabase(connector)
    db.add_record(("results", "my_fn"), record_id="abc", record=table)
    db.flush()
"""
from __future__ import annotations

import itertools
import logging
import re
import threading
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

import psycopg

from orcapod.types import ColumnInfo
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Module-level helpers (pure functions, no I/O)
# These are kept module-level so AsyncPostgreSQLConnector (future) can reuse
# them without subclassing.
# ---------------------------------------------------------------------------


def _pg_type_to_arrow(pg_type_name: str, udt_name: str) -> pa.DataType:
    """Map a PostgreSQL type name to an Arrow DataType.

    Args:
        pg_type_name: The ``data_type`` value from ``information_schema.columns``
            (e.g. ``"integer"``, ``"text"``, ``"ARRAY"``).
        udt_name: The ``udt_name`` value from ``information_schema.columns``
            (e.g. ``"int4"``, ``"text"``, ``"_int4"`` for int4 arrays).

    Returns:
        The corresponding Arrow DataType. Falls back to ``pa.large_string()``
        for unknown types (with a logged warning).
    """
    import pyarrow as _pa

    # Array type: udt_name starts with "_", element type follows
    if pg_type_name.upper() == "ARRAY":
        elem_udt = udt_name.lstrip("_")
        elem_type = _pg_type_to_arrow(elem_udt, elem_udt)
        return _pa.large_list(elem_type)

    t = udt_name.lower()

    if t == "bool":
        return _pa.bool_()
    if t in ("int2", "smallint"):
        return _pa.int32()
    if t in ("int4", "integer", "int"):
        return _pa.int32()
    if t in ("int8", "bigint"):
        return _pa.int64()
    if t in ("float4", "real"):
        return _pa.float32()
    if t in ("float8", "double precision", "numeric", "decimal"):
        return _pa.float64()
    if t in ("text", "varchar", "character varying", "char", "bpchar",
             "name", "uuid", "json", "jsonb", "time", "timetz"):
        if t in ("time", "timetz"):
            logger.warning("PostgreSQL type %r mapped to pa.large_string() (known gap)", t)
        if t == "uuid":
            # TODO: revisit mapping once PLT-1162 decides on a canonical UUID Arrow type
            pass
        return _pa.large_string()
    if t == "bytea":
        return _pa.large_binary()
    if t == "date":
        return _pa.date32()
    if t in ("timestamp", "timestamp without time zone"):
        return _pa.timestamp("us")
    if t in ("timestamptz", "timestamp with time zone"):
        return _pa.timestamp("us", tz="UTC")

    # Fallback: also handle pg_type_name-based matches for information_schema values
    p = pg_type_name.lower()
    if p == "timestamp without time zone":
        return _pa.timestamp("us")
    if p == "timestamp with time zone":
        return _pa.timestamp("us", tz="UTC")
    if p in ("character varying", "character"):
        return _pa.large_string()
    if p in ("time without time zone", "time with time zone"):
        logger.warning("PostgreSQL type %r mapped to pa.large_string() (known gap)", p)
        return _pa.large_string()

    logger.warning("Unknown PostgreSQL type %r (udt_name=%r); mapping to pa.large_string()", pg_type_name, udt_name)
    return _pa.large_string()


def _arrow_type_to_pg_sql(arrow_type: pa.DataType) -> str:
    """Map an Arrow DataType to a PostgreSQL SQL type string for CREATE TABLE.

    Args:
        arrow_type: The Arrow DataType to convert.

    Returns:
        A PostgreSQL SQL type string (e.g. ``"TEXT"``, ``"BIGINT"``).

    Raises:
        ValueError: If ``arrow_type`` is ``pa.large_list(...)`` — array columns
            are not supported in CREATE TABLE (they are read-only from existing tables).
    """
    import pyarrow as _pa

    if arrow_type == _pa.bool_():
        return "BOOLEAN"
    if _pa.types.is_integer(arrow_type):
        if arrow_type in (_pa.int8(), _pa.int16(), _pa.int32(),
                          _pa.uint8(), _pa.uint16()):
            return "INTEGER"
        return "BIGINT"
    if arrow_type == _pa.float32():
        return "REAL"
    if _pa.types.is_floating(arrow_type):
        return "DOUBLE PRECISION"
    if _pa.types.is_string(arrow_type) or _pa.types.is_large_string(arrow_type):
        return "TEXT"
    if _pa.types.is_binary(arrow_type) or _pa.types.is_large_binary(arrow_type):
        return "BYTEA"
    if arrow_type == _pa.date32():
        return "DATE"
    if _pa.types.is_timestamp(arrow_type):
        return "TIMESTAMPTZ" if arrow_type.tz is not None else "TIMESTAMP"
    if _pa.types.is_large_list(arrow_type) or _pa.types.is_list(arrow_type):
        raise ValueError(
            f"Arrow type {arrow_type!r} (list/array) is not supported for "
            "CREATE TABLE. Array columns can only be read from existing tables."
        )
    logger.warning("Unsupported Arrow type %r; mapping to TEXT", arrow_type)
    return "TEXT"


def _resolve_column_type_lookup(
    query: str,
    connector: "PostgreSQLConnector",
) -> dict[str, pa.DataType]:
    """Parse the FROM clause of query to find the source table, then return
    a column-name → Arrow-type dict from get_column_info.

    Returns an empty dict if no single unambiguous table can be identified,
    causing iter_batches to fall back to pa.large_string() for all columns.

    Args:
        query: SQL query string.
        connector: The connector to call get_column_info on.

    Returns:
        Dict mapping column name to Arrow DataType.
    """
    # Match FROM "table_name" or FROM table_name (case-insensitive)
    match = re.search(r'FROM\s+"([^"]+)"', query, re.IGNORECASE)
    if not match:
        match = re.search(r'FROM\s+(\w+)', query, re.IGNORECASE)
    if not match:
        return {}
    table_name = match.group(1)
    return {ci.name: ci.arrow_type for ci in connector.get_column_info(table_name)}
```

- [ ] **Step 2: Run type-mapping tests**

```bash
pytest tests/test_databases/test_postgresql_connector.py::TestPgTypeToArrow tests/test_databases/test_postgresql_connector.py::TestArrowTypeToPgSql -v
```

Expected: all pass.

- [ ] **Step 3: Commit**

```bash
git add src/orcapod/databases/postgresql_connector.py tests/test_databases/test_postgresql_connector.py
git commit -m "feat(PLT-1075): add PostgreSQL type-mapping helpers + unit tests"
```

---

## Task 4: Connector scaffold — unit tests first

**Files:**
- Modify: `tests/test_databases/test_postgresql_connector.py`
- Modify: `src/orcapod/databases/postgresql_connector.py`

- [ ] **Step 1: Add scaffold unit tests to the test file**

Append these classes to `tests/test_databases/test_postgresql_connector.py`:

```python
class TestPostgreSQLConnectorScaffold:
    def test_isinstance_dbconnector_protocol(self) -> None:
        with patch("psycopg.connect") as mock_connect:
            mock_connect.return_value = MagicMock()
            connector = PostgreSQLConnector("postgresql://localhost/test")
            assert isinstance(connector, DBConnectorProtocol)
            connector._conn = None  # skip actual close


class TestConfig:
    def test_to_config(self) -> None:
        with patch("psycopg.connect") as mock_connect:
            mock_connect.return_value = MagicMock()
            connector = PostgreSQLConnector("postgresql://user:pass@localhost:5432/mydb")
            config = connector.to_config()
            assert config["connector_type"] == "postgresql"
            assert config["dsn"] == "postgresql://user:pass@localhost:5432/mydb"
            connector._conn = None

    def test_from_config_roundtrip(self) -> None:
        with patch("psycopg.connect") as mock_connect:
            mock_connect.return_value = MagicMock()
            config = {"connector_type": "postgresql", "dsn": "postgresql://localhost/test"}
            connector = PostgreSQLConnector.from_config(config)
            assert isinstance(connector, PostgreSQLConnector)
            connector._conn = None

    def test_from_config_wrong_type_raises(self) -> None:
        with pytest.raises(ValueError, match="postgresql"):
            PostgreSQLConnector.from_config({"connector_type": "sqlite", "dsn": "x"})

    def test_from_config_missing_type_raises(self) -> None:
        with pytest.raises(ValueError, match="postgresql"):
            PostgreSQLConnector.from_config({"dsn": "postgresql://localhost/test"})


class TestLifecycleUnit:
    def test_close_is_idempotent(self) -> None:
        with patch("psycopg.connect") as mock_connect:
            mock_conn = MagicMock()
            mock_connect.return_value = mock_conn
            connector = PostgreSQLConnector("postgresql://localhost/test")
            connector.close()
            connector.close()  # must not raise
            assert mock_conn.close.call_count == 1

    def test_require_open_raises_after_close(self) -> None:
        with patch("psycopg.connect") as mock_connect:
            mock_connect.return_value = MagicMock()
            connector = PostgreSQLConnector("postgresql://localhost/test")
            connector.close()
            with pytest.raises(RuntimeError, match="closed"):
                connector._require_open()

    def test_context_manager(self) -> None:
        with patch("psycopg.connect") as mock_connect:
            mock_conn = MagicMock()
            mock_connect.return_value = mock_conn
            with PostgreSQLConnector("postgresql://localhost/test") as c:
                c._require_open()  # must not raise
            with pytest.raises(RuntimeError, match="closed"):
                c._require_open()


class TestValidateTableName:
    def test_raises_on_double_quote(self) -> None:
        with patch("psycopg.connect") as mock_connect:
            mock_connect.return_value = MagicMock()
            connector = PostgreSQLConnector("postgresql://localhost/test")
            with pytest.raises(ValueError, match="double-quote"):
                connector._validate_table_name('bad"name')
            connector._conn = None

    def test_valid_name_does_not_raise(self) -> None:
        with patch("psycopg.connect") as mock_connect:
            mock_connect.return_value = MagicMock()
            connector = PostgreSQLConnector("postgresql://localhost/test")
            connector._validate_table_name("valid_table_name")  # must not raise
            connector._conn = None


class TestSchemaIntrospectionUnit:
    """Unit tests for get_table_names and get_pk_columns using mocked cursor."""

    def _make_connector(self) -> PostgreSQLConnector:
        with patch("psycopg.connect") as mock_connect:
            mock_connect.return_value = MagicMock()
            return PostgreSQLConnector("postgresql://localhost/test")

    def test_get_table_names_returns_sorted_list(self) -> None:
        connector = self._make_connector()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = lambda s: s
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_cursor.fetchall.return_value = [("alpha",), ("beta",)]  # ORDER BY in SQL
        connector._conn.cursor.return_value = mock_cursor
        result = connector.get_table_names()
        assert result == ["alpha", "beta"]
        connector._conn = None

    def test_get_pk_columns_single_pk(self) -> None:
        connector = self._make_connector()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = lambda s: s
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_cursor.fetchall.return_value = [("id",)]
        connector._conn.cursor.return_value = mock_cursor
        result = connector.get_pk_columns("my_table")
        assert result == ["id"]
        connector._conn = None

    def test_get_pk_columns_no_pk(self) -> None:
        connector = self._make_connector()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = lambda s: s
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_cursor.fetchall.return_value = []
        connector._conn.cursor.return_value = mock_cursor
        result = connector.get_pk_columns("my_table")
        assert result == []
        connector._conn = None
```

- [ ] **Step 2: Run to confirm failures**

```bash
pytest tests/test_databases/test_postgresql_connector.py::TestPostgreSQLConnectorScaffold tests/test_databases/test_postgresql_connector.py::TestConfig -v 2>&1 | head -30
```

Expected: failures because `PostgreSQLConnector` class doesn't exist yet.

---

## Task 5: Implement connector scaffold

**Files:**
- Modify: `src/orcapod/databases/postgresql_connector.py`

Append the full class skeleton to the existing file (after the module-level helpers):

- [ ] **Step 1: Add the PostgreSQLConnector class**

Append to `src/orcapod/databases/postgresql_connector.py`:

```python
# ---------------------------------------------------------------------------
# PostgreSQLConnector
# ---------------------------------------------------------------------------


class PostgreSQLConnector:
    """DBConnectorProtocol implementation backed by psycopg3 (psycopg).

    Holds a single psycopg.Connection opened at construction time.
    Thread-safe via an internal threading.RLock.

    Uses named server-side cursors in iter_batches so PostgreSQL streams
    results row-by-row rather than buffering the full result set.

    Args:
        dsn: libpq connection string.
            URI form: ``"postgresql://user:pass@host:5432/dbname"``
            Keyword form: ``"host=localhost dbname=mydb user=alice"``
    """

    def __init__(self, dsn: str) -> None:
        self._dsn = dsn
        self._conn: psycopg.Connection | None = psycopg.connect(dsn, autocommit=False)
        self._lock = threading.RLock()
        self._cursor_seq = itertools.count()

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _require_open(self) -> psycopg.Connection:
        """Return the open connection or raise RuntimeError if closed."""
        if self._conn is None:
            raise RuntimeError("PostgreSQLConnector is closed")
        return self._conn

    @staticmethod
    def _validate_table_name(table_name: str) -> None:
        """Raise ValueError if table_name contains a double-quote character.

        Double-quoted identifiers are used in SQL DDL/DML. Allowing a
        double-quote in the name would break the quoting and risk SQL injection.

        Args:
            table_name: The table name to validate.

        Raises:
            ValueError: If the table name contains a double-quote character.
        """
        if '"' in table_name:
            raise ValueError(
                f"Table name {table_name!r} contains an invalid double-quote character."
            )

    # ── Schema introspection ──────────────────────────────────────────────────

    def get_table_names(self) -> list[str]:
        """Return all user table names in this database (sorted, excludes views)."""
        with self._lock:
            conn = self._require_open()
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = current_schema()
                      AND table_type = 'BASE TABLE'
                    ORDER BY table_name
                    """
                )
                return [row[0] for row in cur.fetchall()]

    def get_pk_columns(self, table_name: str) -> list[str]:
        """Return primary-key column names in key-sequence order.

        Returns an empty list if the table has no primary key or does not exist.
        """
        self._validate_table_name(table_name)
        with self._lock:
            conn = self._require_open()
            with conn.cursor() as cur:
                cur.execute(
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
                return [row[0] for row in cur.fetchall()]

    def get_column_info(self, table_name: str) -> list[ColumnInfo]:
        """Return column metadata with Arrow-mapped types.

        Returns an empty list if the table does not exist.
        """
        self._validate_table_name(table_name)
        with self._lock:
            conn = self._require_open()
            with conn.cursor() as cur:
                cur.execute(
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
                    for row in cur.fetchall()
                ]

    # ── Read ──────────────────────────────────────────────────────────────────

    def iter_batches(
        self,
        query: str,
        params: Any = None,
        batch_size: int = 1000,
    ) -> Iterator[pa.RecordBatch]:
        """Execute a query and yield results as Arrow RecordBatches.

        Uses a named server-side cursor so PostgreSQL streams rows in pages
        rather than buffering the full result set.

        Args:
            query: SQL query string. Table names should be double-quoted.
            params: Optional query parameters.
            batch_size: Maximum rows per yielded batch.
        """
        import pyarrow as _pa

        with self._lock:
            conn = self._require_open()
            cursor_name = f"orcapod_{next(self._cursor_seq)}"
            cur = conn.cursor(name=cursor_name)

        try:
            with self._lock:
                self._require_open()
                cur.execute(query, params)
                if cur.description is None:
                    return
                col_names = [d.name for d in cur.description]
                type_lookup = _resolve_column_type_lookup(query, self)
                arrow_types = [type_lookup.get(n, _pa.large_string()) for n in col_names]
                schema = _pa.schema(
                    [_pa.field(n, t) for n, t in zip(col_names, arrow_types)]
                )
                rows = cur.fetchmany(batch_size)

            while rows:
                arrays = [
                    _pa.array([r[i] for r in rows], type=t)
                    for i, t in enumerate(arrow_types)
                ]
                yield _pa.RecordBatch.from_arrays(arrays, schema=schema)
                with self._lock:
                    self._require_open()
                    rows = cur.fetchmany(batch_size)
        finally:
            cur.close()

    # ── Write ─────────────────────────────────────────────────────────────────

    def create_table_if_not_exists(
        self,
        table_name: str,
        columns: list[ColumnInfo],
        pk_column: str,
    ) -> None:
        """Create a table with the given columns if it does not already exist.

        Raises:
            ValueError: If ``pk_column`` is not in ``columns``.
        """
        col_names = [col.name for col in columns]
        if pk_column not in col_names:
            raise ValueError(
                f"pk_column {pk_column!r} not found in columns: {col_names}"
            )
        self._validate_table_name(table_name)
        with self._lock:
            conn = self._require_open()
            col_defs = []
            for col in columns:
                pg_type = _arrow_type_to_pg_sql(col.arrow_type)
                not_null = " NOT NULL" if not col.nullable else ""
                pk = " PRIMARY KEY" if col.name == pk_column else ""
                escaped = col.name.replace('"', '""')
                col_defs.append(f'    "{escaped}" {pg_type}{not_null}{pk}')
            ddl = f'CREATE TABLE IF NOT EXISTS "{table_name}" (\n'
            ddl += ",\n".join(col_defs)
            ddl += "\n)"
            with conn.cursor() as cur:
                cur.execute(ddl)
            conn.commit()

    def upsert_records(
        self,
        table_name: str,
        records: pa.Table,
        id_column: str,
        skip_existing: bool = False,
    ) -> None:
        """Write records to a table using upsert semantics.

        Args:
            table_name: Target table (must already exist).
            records: Arrow table of records to write.
            id_column: Column used as the unique row identifier.
            skip_existing: If True, skip rows whose id already exists.
                If False, overwrite existing rows.
        """
        self._validate_table_name(table_name)
        with self._lock:
            conn = self._require_open()
            cols = list(records.column_names)
            col_list = ", ".join('"' + c.replace('"', '""') + '"' for c in cols)
            placeholders = ", ".join("%s" for _ in cols)

            if skip_existing:
                conflict_clause = f'ON CONFLICT ("{id_column}") DO NOTHING'
            else:
                non_pk_cols = [c for c in cols if c != id_column]
                if non_pk_cols:
                    set_clause = ", ".join(
                        f'"{c}" = EXCLUDED."{c}"' for c in non_pk_cols
                    )
                    conflict_clause = (
                        f'ON CONFLICT ("{id_column}") DO UPDATE SET {set_clause}'
                    )
                else:
                    conflict_clause = f'ON CONFLICT ("{id_column}") DO NOTHING'

            sql = (
                f'INSERT INTO "{table_name}" ({col_list}) VALUES ({placeholders}) '
                f'{conflict_clause}'
            )
            rows = [tuple(row[c] for c in cols) for row in records.to_pylist()]
            with conn.cursor() as cur:
                cur.executemany(sql, rows)
            conn.commit()

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def close(self) -> None:
        """Close the database connection. Idempotent."""
        with self._lock:
            if self._conn is not None:
                self._conn.close()
                self._conn = None

    def __enter__(self) -> PostgreSQLConnector:
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    # ── Serialization ─────────────────────────────────────────────────────────

    def to_config(self) -> dict[str, Any]:
        """Serialize connection configuration to a JSON-compatible dict.

        Warning: The returned dict includes the DSN which may contain a
        password. Do not log or persist this dict in plaintext.

        Returns:
            Dict with ``connector_type`` and ``dsn`` keys.
        """
        return {
            "connector_type": "postgresql",
            "dsn": self._dsn,
        }

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> PostgreSQLConnector:
        """Reconstruct a PostgreSQLConnector from a config dict.

        Args:
            config: Dict with ``connector_type`` and ``dsn`` keys.

        Returns:
            A new PostgreSQLConnector instance.

        Raises:
            ValueError: If ``connector_type`` is not ``"postgresql"``.
        """
        if config.get("connector_type") != "postgresql":
            raise ValueError(
                f"Expected connector_type 'postgresql', got "
                f"{config.get('connector_type')!r}"
            )
        return cls(dsn=config["dsn"])
```

- [ ] **Step 2: Run all unit tests**

```bash
pytest tests/test_databases/test_postgresql_connector.py -v
```

Expected: all pass.

- [ ] **Step 3: Commit**

```bash
git add src/orcapod/databases/postgresql_connector.py tests/test_databases/test_postgresql_connector.py
git commit -m "feat(PLT-1075): implement PostgreSQLConnector with full DBConnectorProtocol"
```

---

## Task 6: Update `__init__.py` export

**Files:**
- Modify: `src/orcapod/databases/__init__.py`

- [ ] **Step 1: Add the import and export**

In `src/orcapod/databases/__init__.py`, add `PostgreSQLConnector` alongside `SQLiteConnector`:

```python
from .postgresql_connector import PostgreSQLConnector
```

And add `"PostgreSQLConnector"` to `__all__`. Also update the comment block:

```python
#   SQLiteConnector      -- PLT-1076 (stdlib sqlite3, zero extra deps)  ✓
#   PostgreSQLConnector  -- PLT-1075 (psycopg3)                          ✓
#   SpiralDBConnector    -- PLT-1074
```

- [ ] **Step 2: Verify import works**

```bash
python -c "from orcapod.databases import PostgreSQLConnector; print('ok')"
```

Expected: `ok`

- [ ] **Step 3: Commit**

```bash
git add src/orcapod/databases/__init__.py
git commit -m "feat(PLT-1075): export PostgreSQLConnector from orcapod.databases"
```

---

## Task 7: Integration tests

**Files:**
- Create: `tests/test_databases/test_postgresql_connector_integration.py`

Integration tests require a live PostgreSQL instance. `pytest-postgresql` is already installed. These tests are marked `@pytest.mark.postgres` and are excluded from the default test run.

- [ ] **Step 1: Create the integration test file**

```python
# tests/test_databases/test_postgresql_connector_integration.py
"""Integration tests for PostgreSQLConnector — requires a live PostgreSQL instance.

Run with: pytest -m postgres tests/test_databases/test_postgresql_connector_integration.py

Uses pytest-postgresql to spawn a temporary PostgreSQL process. Requires
PostgreSQL binaries (pg_ctl, initdb) to be installed:
    sudo apt-get install postgresql
"""
from __future__ import annotations

from collections.abc import Iterator

import pyarrow as pa
import pytest

from orcapod.databases import ConnectorArrowDatabase, PostgreSQLConnector
from orcapod.protocols.db_connector_protocol import DBConnectorProtocol
from orcapod.types import ColumnInfo


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def pg_connector(postgresql) -> Iterator[PostgreSQLConnector]:
    """Session-scoped PostgreSQLConnector backed by a pytest-postgresql instance."""
    info = postgresql.info
    dsn = (
        f"host={info.host} port={info.port} "
        f"dbname={info.dbname} user={info.user}"
    )
    c = PostgreSQLConnector(dsn)
    yield c
    c.close()


@pytest.fixture
def connector(postgresql) -> Iterator[PostgreSQLConnector]:
    """Function-scoped connector — fresh DB per test."""
    info = postgresql.info
    dsn = (
        f"host={info.host} port={info.port} "
        f"dbname={info.dbname} user={info.user}"
    )
    c = PostgreSQLConnector(dsn)
    yield c
    c.close()


def _make_columns() -> list[ColumnInfo]:
    return [
        ColumnInfo("__record_id", pa.large_string(), nullable=False),
        ColumnInfo("value", pa.float64(), nullable=True),
    ]


def _make_table(ids: list[str], values: list[float | None]) -> pa.Table:
    return pa.table({
        "__record_id": pa.array(ids, type=pa.large_string()),
        "value": pa.array(values, type=pa.float64()),
    })


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------


@pytest.mark.postgres
class TestLifecycle:
    def test_open_and_close(self, connector: PostgreSQLConnector) -> None:
        connector._require_open()  # must not raise
        connector.close()
        with pytest.raises(RuntimeError, match="closed"):
            connector._require_open()

    def test_close_is_idempotent(self, connector: PostgreSQLConnector) -> None:
        connector.close()
        connector.close()  # must not raise

    def test_context_manager(self, postgresql) -> None:
        info = postgresql.info
        dsn = f"host={info.host} port={info.port} dbname={info.dbname} user={info.user}"
        with PostgreSQLConnector(dsn) as c:
            c._require_open()
        with pytest.raises(RuntimeError, match="closed"):
            c._require_open()


# ---------------------------------------------------------------------------
# Schema introspection
# ---------------------------------------------------------------------------


@pytest.mark.postgres
class TestGetTableNames:
    def test_empty_database(self, connector: PostgreSQLConnector) -> None:
        assert connector.get_table_names() == []

    def test_returns_table_names(self, connector: PostgreSQLConnector) -> None:
        with connector._conn.cursor() as cur:
            cur.execute('CREATE TABLE "foo" (id INTEGER PRIMARY KEY)')
            cur.execute('CREATE TABLE "bar" (id INTEGER PRIMARY KEY)')
        connector._conn.commit()
        assert connector.get_table_names() == ["bar", "foo"]

    def test_excludes_views(self, connector: PostgreSQLConnector) -> None:
        with connector._conn.cursor() as cur:
            cur.execute('CREATE TABLE "base_tbl" (id INTEGER PRIMARY KEY)')
            cur.execute('CREATE VIEW "v_base" AS SELECT * FROM "base_tbl"')
        connector._conn.commit()
        assert "v_base" not in connector.get_table_names()
        assert "base_tbl" in connector.get_table_names()


@pytest.mark.postgres
class TestGetPkColumns:
    def test_single_pk(self, connector: PostgreSQLConnector) -> None:
        with connector._conn.cursor() as cur:
            cur.execute('CREATE TABLE "t" (id TEXT PRIMARY KEY, val REAL)')
        connector._conn.commit()
        assert connector.get_pk_columns("t") == ["id"]

    def test_composite_pk_order_preserved(self, connector: PostgreSQLConnector) -> None:
        with connector._conn.cursor() as cur:
            cur.execute(
                'CREATE TABLE "t" (a TEXT, b INTEGER, c REAL, PRIMARY KEY (a, b))'
            )
        connector._conn.commit()
        assert connector.get_pk_columns("t") == ["a", "b"]

    def test_no_pk_returns_empty(self, connector: PostgreSQLConnector) -> None:
        with connector._conn.cursor() as cur:
            cur.execute('CREATE TABLE "t" (val TEXT)')
        connector._conn.commit()
        assert connector.get_pk_columns("t") == []

    def test_nonexistent_table_returns_empty(self, connector: PostgreSQLConnector) -> None:
        assert connector.get_pk_columns("no_such_table") == []

    def test_raises_on_invalid_table_name(self, connector: PostgreSQLConnector) -> None:
        with pytest.raises(ValueError, match="double-quote"):
            connector.get_pk_columns('table"name')


@pytest.mark.postgres
class TestGetColumnInfo:
    def test_all_pg_types(self, connector: PostgreSQLConnector) -> None:
        with connector._conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE "t" (
                    a BOOLEAN,
                    b SMALLINT,
                    c INTEGER,
                    d BIGINT,
                    e REAL,
                    f DOUBLE PRECISION,
                    g TEXT,
                    h BYTEA,
                    i UUID,
                    j JSONB,
                    k DATE,
                    l TIMESTAMP,
                    m TIMESTAMPTZ
                )
            """)
        connector._conn.commit()
        infos = {ci.name: ci for ci in connector.get_column_info("t")}
        assert infos["a"].arrow_type == pa.bool_()
        assert infos["b"].arrow_type == pa.int32()
        assert infos["c"].arrow_type == pa.int32()
        assert infos["d"].arrow_type == pa.int64()
        assert infos["e"].arrow_type == pa.float32()
        assert infos["f"].arrow_type == pa.float64()
        assert infos["g"].arrow_type == pa.large_string()
        assert infos["h"].arrow_type == pa.large_binary()
        assert infos["i"].arrow_type == pa.large_string()  # TODO: PLT-1162
        assert infos["j"].arrow_type == pa.large_string()
        assert infos["k"].arrow_type == pa.date32()
        assert infos["l"].arrow_type == pa.timestamp("us")
        assert infos["m"].arrow_type == pa.timestamp("us", tz="UTC")

    def test_nullable_flag(self, connector: PostgreSQLConnector) -> None:
        with connector._conn.cursor() as cur:
            cur.execute('CREATE TABLE "t" (a TEXT NOT NULL, b TEXT)')
        connector._conn.commit()
        infos = {ci.name: ci for ci in connector.get_column_info("t")}
        assert infos["a"].nullable is False
        assert infos["b"].nullable is True

    def test_nonexistent_table_returns_empty(self, connector: PostgreSQLConnector) -> None:
        assert connector.get_column_info("no_such_table") == []

    def test_raises_on_invalid_table_name(self, connector: PostgreSQLConnector) -> None:
        with pytest.raises(ValueError, match="double-quote"):
            connector.get_column_info('bad"name')

    def test_array_column(self, connector: PostgreSQLConnector) -> None:
        with connector._conn.cursor() as cur:
            cur.execute('CREATE TABLE "t" (ids INTEGER[])')
        connector._conn.commit()
        infos = {ci.name: ci for ci in connector.get_column_info("t")}
        assert infos["ids"].arrow_type == pa.large_list(pa.int32())


# ---------------------------------------------------------------------------
# Write path
# ---------------------------------------------------------------------------


@pytest.mark.postgres
class TestCreateTableIfNotExists:
    def test_creates_table(self, connector: PostgreSQLConnector) -> None:
        connector.create_table_if_not_exists("tbl", _make_columns(), "__record_id")
        assert "tbl" in connector.get_table_names()

    def test_idempotent(self, connector: PostgreSQLConnector) -> None:
        cols = _make_columns()
        connector.create_table_if_not_exists("tbl", cols, "__record_id")
        connector.create_table_if_not_exists("tbl", cols, "__record_id")  # must not raise

    def test_pk_column_set(self, connector: PostgreSQLConnector) -> None:
        connector.create_table_if_not_exists("tbl", _make_columns(), "__record_id")
        assert connector.get_pk_columns("tbl") == ["__record_id"]

    def test_not_null_respected(self, connector: PostgreSQLConnector) -> None:
        connector.create_table_if_not_exists("tbl", _make_columns(), "__record_id")
        infos = {ci.name: ci for ci in connector.get_column_info("tbl")}
        assert infos["__record_id"].nullable is False
        assert infos["value"].nullable is True

    def test_raises_when_pk_column_not_in_columns(self, connector: PostgreSQLConnector) -> None:
        with pytest.raises(ValueError, match="not found in columns"):
            connector.create_table_if_not_exists("tbl", _make_columns(), "missing_pk")

    def test_raises_on_invalid_table_name(self, connector: PostgreSQLConnector) -> None:
        with pytest.raises(ValueError, match="double-quote"):
            connector.create_table_if_not_exists('bad"name', _make_columns(), "__record_id")


@pytest.mark.postgres
class TestUpsertRecords:
    def setup_table(self, connector: PostgreSQLConnector) -> None:
        connector.create_table_if_not_exists("tbl", _make_columns(), "__record_id")

    def test_insert_new_records(self, connector: PostgreSQLConnector) -> None:
        self.setup_table(connector)
        connector.upsert_records("tbl", _make_table(["a", "b"], [1.0, 2.0]), "__record_id")
        batches = list(connector.iter_batches('SELECT * FROM "tbl"'))
        assert sum(b.num_rows for b in batches) == 2

    def test_replace_existing(self, connector: PostgreSQLConnector) -> None:
        self.setup_table(connector)
        connector.upsert_records("tbl", _make_table(["a"], [1.0]), "__record_id")
        connector.upsert_records("tbl", _make_table(["a"], [99.0]), "__record_id", skip_existing=False)
        batches = list(connector.iter_batches('SELECT * FROM "tbl" WHERE "__record_id" = \'a\''))
        table = pa.Table.from_batches(batches)
        assert table.column("value")[0].as_py() == 99.0

    def test_skip_existing_keeps_original(self, connector: PostgreSQLConnector) -> None:
        self.setup_table(connector)
        connector.upsert_records("tbl", _make_table(["a"], [1.0]), "__record_id")
        connector.upsert_records("tbl", _make_table(["a"], [99.0]), "__record_id", skip_existing=True)
        batches = list(connector.iter_batches('SELECT * FROM "tbl" WHERE "__record_id" = \'a\''))
        table = pa.Table.from_batches(batches)
        assert table.column("value")[0].as_py() == 1.0

    def test_raises_on_invalid_table_name(self, connector: PostgreSQLConnector) -> None:
        with pytest.raises(ValueError, match="double-quote"):
            connector.upsert_records('bad"name', _make_table(["a"], [1.0]), "__record_id")


# ---------------------------------------------------------------------------
# iter_batches
# ---------------------------------------------------------------------------


@pytest.mark.postgres
class TestIterBatches:
    def setup_data(self, connector: PostgreSQLConnector) -> None:
        cols = [
            ColumnInfo("id", pa.large_string(), nullable=False),
            ColumnInfo("score", pa.float64(), nullable=True),
            ColumnInfo("count", pa.int64(), nullable=True),
            ColumnInfo("active", pa.bool_(), nullable=True),
        ]
        connector.create_table_if_not_exists("data", cols, "id")
        records = pa.table({
            "id": pa.array(["a", "b", "c"], type=pa.large_string()),
            "score": pa.array([1.5, 2.5, 3.5], type=pa.float64()),
            "count": pa.array([10, 20, 30], type=pa.int64()),
            "active": pa.array([True, False, True], type=pa.bool_()),
        })
        connector.upsert_records("data", records, "id")

    def test_returns_all_rows(self, connector: PostgreSQLConnector) -> None:
        self.setup_data(connector)
        batches = list(connector.iter_batches('SELECT * FROM "data"'))
        assert sum(b.num_rows for b in batches) == 3

    def test_correct_types_roundtrip(self, connector: PostgreSQLConnector) -> None:
        self.setup_data(connector)
        table = pa.Table.from_batches(connector.iter_batches('SELECT * FROM "data"'))
        schema = {f.name: f.type for f in table.schema}
        assert schema["score"] == pa.float64()
        assert schema["count"] == pa.int64()
        assert schema["active"] == pa.bool_()

    def test_batch_size(self, connector: PostgreSQLConnector) -> None:
        self.setup_data(connector)
        batches = list(connector.iter_batches('SELECT * FROM "data"', batch_size=2))
        assert len(batches) == 2
        assert batches[0].num_rows == 2
        assert batches[1].num_rows == 1

    def test_empty_result(self, connector: PostgreSQLConnector) -> None:
        self.setup_data(connector)
        batches = list(connector.iter_batches('SELECT * FROM "data" WHERE 1=0'))
        assert batches == []

    def test_early_termination_closes_cursor(self, connector: PostgreSQLConnector) -> None:
        """Abandoning the generator mid-iteration must not leak server-side cursors."""
        self.setup_data(connector)
        gen = connector.iter_batches('SELECT * FROM "data"', batch_size=1)
        next(gen)  # consume one batch
        gen.close()  # abandon — must send CLOSE to PG without raising
        # Subsequent queries must still work
        batches = list(connector.iter_batches('SELECT * FROM "data"'))
        assert sum(b.num_rows for b in batches) == 3


# ---------------------------------------------------------------------------
# Type roundtrip
# ---------------------------------------------------------------------------


@pytest.mark.postgres
class TestTypeRoundtrip:
    """Write each PG type and read it back; verify Arrow types are correct."""

    def test_bool_roundtrip(self, connector: PostgreSQLConnector) -> None:
        with connector._conn.cursor() as cur:
            cur.execute('CREATE TABLE "t" (id TEXT PRIMARY KEY, v BOOLEAN)')
            cur.execute('INSERT INTO "t" VALUES (%s, %s)', ("r1", True))
        connector._conn.commit()
        table = pa.Table.from_batches(connector.iter_batches('SELECT * FROM "t"'))
        assert table.schema.field("v").type == pa.bool_()
        assert table.column("v")[0].as_py() is True

    def test_int_types_roundtrip(self, connector: PostgreSQLConnector) -> None:
        with connector._conn.cursor() as cur:
            cur.execute('CREATE TABLE "t" (i2 SMALLINT, i4 INTEGER, i8 BIGINT)')
            cur.execute('INSERT INTO "t" VALUES (%s, %s, %s)', (1, 2, 3))
        connector._conn.commit()
        table = pa.Table.from_batches(connector.iter_batches('SELECT * FROM "t"'))
        assert table.schema.field("i2").type == pa.int32()
        assert table.schema.field("i4").type == pa.int32()
        assert table.schema.field("i8").type == pa.int64()

    def test_timestamptz_roundtrip(self, connector: PostgreSQLConnector) -> None:
        with connector._conn.cursor() as cur:
            cur.execute('CREATE TABLE "t" (id TEXT PRIMARY KEY, ts TIMESTAMPTZ)')
            cur.execute("INSERT INTO \"t\" VALUES ('r1', '2024-01-15 12:00:00+00')")
        connector._conn.commit()
        table = pa.Table.from_batches(connector.iter_batches('SELECT * FROM "t"'))
        assert table.schema.field("ts").type == pa.timestamp("us", tz="UTC")

    def test_int_array_roundtrip(self, connector: PostgreSQLConnector) -> None:
        with connector._conn.cursor() as cur:
            cur.execute('CREATE TABLE "t" (id TEXT PRIMARY KEY, vals INTEGER[])')
            cur.execute("INSERT INTO \"t\" VALUES ('r1', ARRAY[1,2,3])")
        connector._conn.commit()
        infos = {ci.name: ci for ci in connector.get_column_info("t")}
        assert infos["vals"].arrow_type == pa.large_list(pa.int32())


# ---------------------------------------------------------------------------
# ConnectorArrowDatabase integration
# ---------------------------------------------------------------------------


@pytest.mark.postgres
class TestConnectorArrowDatabaseWithPostgreSQL:
    @pytest.fixture
    def db(self, connector: PostgreSQLConnector) -> ConnectorArrowDatabase:
        return ConnectorArrowDatabase(connector)

    def test_add_and_get_record(self, db: ConnectorArrowDatabase) -> None:
        record = pa.table({"x": pa.array([1, 2, 3], type=pa.int64())})
        db.add_record(("fn", "test"), record_id="r1", record=record, flush=True)
        result = db.get_record_by_id(("fn", "test"), "r1")
        assert result is not None
        assert result.num_rows == 1
        assert result.column("x")[0].as_py() == 3  # last row kept (dedup)

    def test_add_multiple_records(self, db: ConnectorArrowDatabase) -> None:
        r1 = pa.table({"x": pa.array([1], type=pa.int64())})
        r2 = pa.table({"x": pa.array([2], type=pa.int64())})
        db.add_record(("fn", "multi"), record_id="r1", record=r1)
        db.add_record(("fn", "multi"), record_id="r2", record=r2)
        db.flush()
        result = db.get_all_records(("fn", "multi"))
        assert result is not None
        assert result.num_rows == 2

    def test_skip_duplicates(self, db: ConnectorArrowDatabase) -> None:
        record_v1 = pa.table({"x": pa.array([1], type=pa.int64())})
        record_v2 = pa.table({"x": pa.array([99], type=pa.int64())})
        db.add_record(("fn", "dup"), record_id="r1", record=record_v1, flush=True)
        db.add_record(("fn", "dup"), record_id="r1", record=record_v2, skip_duplicates=True, flush=True)
        result = db.get_record_by_id(("fn", "dup"), "r1")
        assert result is not None
        assert result.column("x")[0].as_py() == 1  # original preserved

    def test_schema_mismatch_raises(self, db: ConnectorArrowDatabase) -> None:
        r1 = pa.table({"x": pa.array([1], type=pa.int64())})
        db.add_record(("fn", "mismatch"), record_id="r1", record=r1, flush=True)
        r2 = pa.table({"x": pa.array([2.0], type=pa.float64())})
        db.add_record(("fn", "mismatch"), record_id="r2", record=r2)
        with pytest.raises(ValueError, match="Schema mismatch"):
            db.flush()
```

- [ ] **Step 2: Run to confirm tests are collected (but skip without live PG)**

```bash
pytest tests/test_databases/test_postgresql_connector_integration.py --collect-only -m postgres 2>&1 | tail -5
```

Expected: tests are collected, none run (no PG available yet).

- [ ] **Step 3: Commit**

```bash
git add tests/test_databases/test_postgresql_connector_integration.py
git commit -m "test(PLT-1075): add PostgreSQL connector integration test suite"
```

---

## Task 8: Run full test suite and verify nothing broken

- [ ] **Step 1: Run the default test suite (no live PG)**

```bash
pytest -m "not postgres" -v 2>&1 | tail -20
```

Expected: all existing tests pass; zero postgres-marked tests run.

- [ ] **Step 2: Run PostgreSQL unit tests specifically**

```bash
pytest tests/test_databases/test_postgresql_connector.py -v
```

Expected: all pass.

- [ ] **Step 3: Final commit if any stray changes**

```bash
git status
```

If clean, no commit needed. If there are any minor fixes, commit them:

```bash
git add -p
git commit -m "fix(PLT-1075): address issues found during test run"
```

---

## Task 9: Push branch and open PR

- [ ] **Step 1: Authenticate with GitHub**

```bash
gh-app-token-generator nauticalab | gh auth login --with-token
```

- [ ] **Step 2: Create branch and push**

```bash
git checkout -b feat/plt-1075-postgresql-connector
git push -u origin feat/plt-1075-postgresql-connector
```

- [ ] **Step 3: Update Linear issue status**

```
mcp__claude_ai_Linear__save_issue(id: "PLT-1075", state: "In Progress")
```

- [ ] **Step 4: Open PR against `dev`**

```bash
gh pr create \
  --base dev \
  --title "feat(PLT-1075): implement PostgreSQLConnector" \
  --body "$(cat <<'EOF'
## Summary

- Implements `PostgreSQLConnector` satisfying `DBConnectorProtocol`
- Uses psycopg3 named server-side cursors for streaming reads (`iter_batches`)
- Type mapping covers all common PostgreSQL types → Arrow (see spec for full table)
- Writes use `ON CONFLICT` upsert; each write method self-commits
- Unit tests (no live DB) + integration test suite (`@pytest.mark.postgres`)

## Issue

Closes PLT-1075

## Test plan
- [ ] `pytest tests/test_databases/test_postgresql_connector.py` — passes without PG
- [ ] `pytest -m postgres tests/test_databases/test_postgresql_connector_integration.py` — passes with PG installed

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step 5: Report PR URL**

Copy the PR URL from the output and include it in your completion message.
