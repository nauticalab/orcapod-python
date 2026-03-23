# SQLiteConnector Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement `SQLiteConnector` — a `DBConnectorProtocol` implementation backed by stdlib `sqlite3` — enabling `ConnectorArrowDatabase` and `DBTableSource` to use SQLite with zero extra dependencies.

**Architecture:** A single class `SQLiteConnector` in its own module, holding one persistent `sqlite3.Connection` (opened at construction, `isolation_level=None` for autocommit) behind a `threading.RLock`. Three private module-level helper functions handle type mapping and value coercion. The class is exported from `orcapod.databases`.

**Tech Stack:** Python stdlib `sqlite3`, PyArrow (lazy import via `LazyModule`), `threading.RLock`.

**Spec:** `superpowers/specs/2026-03-23-sqlite-connector-design.md`

---

## File Map

| Action | Path | Responsibility |
|---|---|---|
| Create | `src/orcapod/databases/sqlite_connector.py` | `SQLiteConnector` class + 3 private type helpers |
| Modify | `src/orcapod/databases/__init__.py` | Export `SQLiteConnector` |
| Create | `tests/test_databases/test_sqlite_connector.py` | All unit + integration tests |

> **Run all tests with:** `uv run pytest tests/test_databases/test_sqlite_connector.py -v`
>
> **Lint/type-check:** `uv run basedpyright src/orcapod/databases/sqlite_connector.py`

---

## Task 1: Scaffold + Type-Mapping Helpers

The three module-level helpers are pure functions with no I/O — ideal first target for TDD.

**Files:**
- Create: `src/orcapod/databases/sqlite_connector.py`
- Create: `tests/test_databases/test_sqlite_connector.py`

- [ ] **Step 1.1: Create the test file with type-helper tests**

Create `tests/test_databases/test_sqlite_connector.py`:

```python
"""Tests for SQLiteConnector — DBConnectorProtocol backed by sqlite3."""
from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.databases.sqlite_connector import (
    SQLiteConnector,
    _arrow_type_to_sqlite_sql,
    _coerce_column,
    _sqlite_type_to_arrow,
)
from orcapod.protocols.db_connector_protocol import DBConnectorProtocol
from orcapod.types import ColumnInfo


# ---------------------------------------------------------------------------
# Type helpers
# ---------------------------------------------------------------------------


class TestSqliteTypeToArrow:
    def test_boolean(self):
        assert _sqlite_type_to_arrow("BOOLEAN") == pa.bool_()
        assert _sqlite_type_to_arrow("boolean") == pa.bool_()

    def test_integer_affinity(self):
        for t in ("INTEGER", "INT", "BIGINT", "TINYINT", "MEDIUMINT"):
            assert _sqlite_type_to_arrow(t) == pa.int64(), t

    def test_text_affinity(self):
        for t in ("TEXT", "VARCHAR(255)", "NCHAR(10)", "CLOB"):
            assert _sqlite_type_to_arrow(t) == pa.large_string(), t

    def test_blob_affinity(self):
        assert _sqlite_type_to_arrow("BLOB") == pa.large_binary()
        assert _sqlite_type_to_arrow("") == pa.large_binary()

    def test_real_affinity(self):
        for t in ("REAL", "FLOAT", "DOUBLE", "DOUBLE PRECISION"):
            assert _sqlite_type_to_arrow(t) == pa.float64(), t

    def test_numeric_affinity(self):
        for t in ("NUMERIC", "DECIMAL(10,2)", "NUMBER"):
            assert _sqlite_type_to_arrow(t) == pa.float64(), t


class TestArrowTypeToSqliteSql:
    def test_integers(self):
        for t in (pa.int8(), pa.int16(), pa.int32(), pa.int64(),
                  pa.uint8(), pa.uint16(), pa.uint32(), pa.uint64()):
            assert _arrow_type_to_sqlite_sql(t) == "INTEGER", t

    def test_floats(self):
        assert _arrow_type_to_sqlite_sql(pa.float32()) == "REAL"
        assert _arrow_type_to_sqlite_sql(pa.float64()) == "REAL"

    def test_strings(self):
        assert _arrow_type_to_sqlite_sql(pa.utf8()) == "TEXT"
        assert _arrow_type_to_sqlite_sql(pa.large_utf8()) == "TEXT"

    def test_binary(self):
        assert _arrow_type_to_sqlite_sql(pa.binary()) == "BLOB"
        assert _arrow_type_to_sqlite_sql(pa.large_binary()) == "BLOB"

    def test_bool(self):
        assert _arrow_type_to_sqlite_sql(pa.bool_()) == "BOOLEAN"


class TestCoerceColumn:
    def test_bool_coercion(self):
        assert _coerce_column([1, 0, 1], pa.bool_()) == [True, False, True]

    def test_bool_with_none(self):
        assert _coerce_column([1, None, 0], pa.bool_()) == [True, None, False]

    def test_non_bool_passthrough(self):
        vals = [1, 2, 3]
        assert _coerce_column(vals, pa.int64()) is vals
```

- [ ] **Step 1.2: Run tests — expect ImportError (module doesn't exist yet)**

```bash
cd /path/to/orcapod-python && uv run pytest tests/test_databases/test_sqlite_connector.py -v 2>&1 | head -20
```

Expected: `ModuleNotFoundError: No module named 'orcapod.databases.sqlite_connector'`

- [ ] **Step 1.3: Create the scaffold + implement type helpers**

Create `src/orcapod/databases/sqlite_connector.py`:

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
from __future__ import annotations

import logging
import os
import re
import sqlite3
import threading
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

from orcapod.types import ColumnInfo

if TYPE_CHECKING:
    import pyarrow as pa
else:
    from orcapod.utils.lazy_module import LazyModule

    pa = LazyModule("pyarrow")

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Module-level helpers (pure functions, no I/O)
# ---------------------------------------------------------------------------


def _sqlite_type_to_arrow(declared_type: str) -> pa.DataType:
    """Map a SQLite declared column type to an Arrow DataType.

    Follows SQLite's type affinity rules with an explicit BOOLEAN
    special-case to preserve round-trip fidelity.

    Args:
        declared_type: The declared column type string from PRAGMA table_info.

    Returns:
        The corresponding Arrow DataType.
    """
    t = declared_type.upper().strip()
    if t == "BOOLEAN":
        return pa.bool_()
    if "INT" in t:
        return pa.int64()
    if any(k in t for k in ("CHAR", "CLOB", "TEXT")):
        return pa.large_string()
    if "BLOB" in t or t == "":
        return pa.large_binary()
    if any(k in t for k in ("REAL", "FLOA", "DOUB")):
        return pa.float64()
    # NUMERIC affinity (and anything else)
    return pa.float64()


def _arrow_type_to_sqlite_sql(arrow_type: pa.DataType) -> str:
    """Map an Arrow DataType to a SQLite SQL type string for CREATE TABLE.

    Args:
        arrow_type: The Arrow DataType to convert.

    Returns:
        A SQLite SQL type string (e.g. "INTEGER", "TEXT").
    """
    import pyarrow as _pa  # noqa: PLC0415 — needed at call time

    if _pa.types.is_integer(arrow_type):
        return "INTEGER"
    if _pa.types.is_floating(arrow_type):
        return "REAL"
    if _pa.types.is_string(arrow_type) or _pa.types.is_large_string(arrow_type):
        return "TEXT"
    if _pa.types.is_binary(arrow_type) or _pa.types.is_large_binary(arrow_type):
        return "BLOB"
    if arrow_type == _pa.bool_():
        return "BOOLEAN"
    logger.warning("Unsupported Arrow type %r; mapping to TEXT", arrow_type)
    return "TEXT"


def _coerce_column(values: list[Any], arrow_type: pa.DataType) -> list[Any]:
    """Coerce raw SQLite Python values to match the target Arrow type.

    SQLite returns BOOLEAN column values as Python int (1/0). PyArrow
    raises ArrowInvalid when constructing pa.bool_() arrays from int values,
    so this helper converts them first.

    Args:
        values: Raw Python values from sqlite3 cursor rows.
        arrow_type: The target Arrow type for this column.

    Returns:
        The same list if no coercion is needed, otherwise a new list
        with coerced values.
    """
    import pyarrow as _pa  # noqa: PLC0415

    if arrow_type == _pa.bool_():
        return [bool(v) if v is not None else None for v in values]
    return values


# ---------------------------------------------------------------------------
# SQLiteConnector
# ---------------------------------------------------------------------------


class SQLiteConnector:
    """DBConnectorProtocol implementation backed by stdlib sqlite3.

    Holds a single sqlite3.Connection opened at construction time.
    Thread-safe via an internal threading.RLock. Not suitable for use
    on network filesystems (NFS, SMB/CIFS) due to unreliable file locking.

    Args:
        db_path: Path to the SQLite database file, or ":memory:" for an
            in-process in-memory database. Defaults to ":memory:".
    """

    def __init__(self, db_path: str | os.PathLike = ":memory:") -> None:
        self._db_path = db_path
        self._conn: sqlite3.Connection | None = sqlite3.connect(
            str(db_path), check_same_thread=False, isolation_level=None
        )
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.RLock()

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _require_open(self) -> sqlite3.Connection:
        """Return the open connection or raise RuntimeError if closed."""
        if self._conn is None:
            raise RuntimeError("SQLiteConnector is closed")
        return self._conn

    # ── Schema introspection ──────────────────────────────────────────────────

    def get_table_names(self) -> list[str]:
        """Return all table names in this database (excludes views).

        Returns:
            Sorted list of table name strings.
        """
        raise NotImplementedError

    def get_pk_columns(self, table_name: str) -> list[str]:
        """Return primary-key column names in key-sequence order.

        Args:
            table_name: Name of the table to introspect.

        Returns:
            List of PK column names, empty if no primary key.
        """
        raise NotImplementedError

    def get_column_info(self, table_name: str) -> list[ColumnInfo]:
        """Return column metadata with Arrow-mapped types.

        Args:
            table_name: Name of the table to introspect.

        Returns:
            List of ColumnInfo objects; empty list if table doesn't exist.
        """
        raise NotImplementedError

    # ── Read ──────────────────────────────────────────────────────────────────

    def iter_batches(
        self,
        query: str,
        params: Any = None,
        batch_size: int = 1000,
    ) -> Iterator[pa.RecordBatch]:
        """Execute a query and yield results as Arrow RecordBatches.

        Args:
            query: SQL query string. Table names should be double-quoted.
            params: Optional query parameters.
            batch_size: Maximum rows per yielded batch.

        Yields:
            Arrow RecordBatch objects.
        """
        raise NotImplementedError

    # ── Write ─────────────────────────────────────────────────────────────────

    def create_table_if_not_exists(
        self,
        table_name: str,
        columns: list[ColumnInfo],
        pk_column: str,
    ) -> None:
        """Create a table with the given columns if it does not already exist.

        Args:
            table_name: Table to create.
            columns: Column definitions with Arrow-mapped types.
            pk_column: Name of the column to use as the primary key.
        """
        raise NotImplementedError

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
            id_column: Column used as the unique row identifier. Conflict
                detection is handled by the SQL PRIMARY KEY constraint;
                this argument is accepted for protocol compliance.
            skip_existing: If True, skip rows whose id already exists
                (INSERT OR IGNORE). If False, overwrite (INSERT OR REPLACE).
        """
        raise NotImplementedError

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def close(self) -> None:
        """Close the database connection. Idempotent."""
        raise NotImplementedError

    def __enter__(self) -> SQLiteConnector:
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    # ── Serialization ─────────────────────────────────────────────────────────

    def to_config(self) -> dict[str, Any]:
        """Serialize connection configuration to a JSON-compatible dict.

        Returns:
            Dict with ``connector_type`` and ``db_path`` keys.
        """
        raise NotImplementedError

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> SQLiteConnector:
        """Reconstruct a SQLiteConnector from a config dict.

        Args:
            config: Dict with ``connector_type`` and ``db_path`` keys.

        Returns:
            A new SQLiteConnector instance.

        Raises:
            ValueError: If ``connector_type`` is not ``"sqlite"``.
        """
        raise NotImplementedError
```

- [ ] **Step 1.4: Run the type-helper tests — expect PASS**

```bash
uv run pytest tests/test_databases/test_sqlite_connector.py -v -k "TypeToArrow or ArrowType or CoerceColumn"
```

Expected: All 3 test classes pass (15 tests).

- [ ] **Step 1.5: Commit**

```bash
git checkout -b eywalker/plt-1076-implement-database-abstraction-for-sqlite
git add src/orcapod/databases/sqlite_connector.py tests/test_databases/test_sqlite_connector.py
git commit -m "feat(databases): scaffold SQLiteConnector with type-mapping helpers (PLT-1076)"
```

---

## Task 2: Connection Lifecycle

**Files:**
- Modify: `src/orcapod/databases/sqlite_connector.py` — implement `close`, `__enter__`, `__exit__`
- Modify: `tests/test_databases/test_sqlite_connector.py` — add lifecycle tests

- [ ] **Step 2.1: Add lifecycle tests**

Add to `tests/test_databases/test_sqlite_connector.py`:

```python
# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def connector() -> Iterator[SQLiteConnector]:
    c = SQLiteConnector(":memory:")
    yield c
    # Safe even if test already closed it
    try:
        c.close()
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------


class TestLifecycle:
    def test_constructor_opens_connection(self, connector):
        # Should not raise — connection is open
        assert connector.get_table_names() == []

    def test_close_is_idempotent(self, connector):
        connector.close()
        connector.close()  # must not raise

    def test_context_manager_closes_on_exit(self):
        with SQLiteConnector(":memory:") as c:
            assert c.get_table_names() == []
        # After exit, connection is closed; any method should raise
        with pytest.raises(RuntimeError, match="closed"):
            c.get_table_names()

    def test_methods_raise_after_close(self, connector):
        connector.close()
        with pytest.raises(RuntimeError, match="closed"):
            connector.get_table_names()
```

- [ ] **Step 2.2: Run lifecycle tests — expect FAIL**

```bash
uv run pytest tests/test_databases/test_sqlite_connector.py::TestLifecycle -v
```

Expected: FAIL — `NotImplementedError` from `close()`.

- [ ] **Step 2.3: Implement `close()`**

Replace the `close` stub in `sqlite_connector.py`:

```python
def close(self) -> None:
    """Close the database connection. Idempotent."""
    with self._lock:
        if self._conn is not None:
            self._conn.close()
            self._conn = None
```

- [ ] **Step 2.4: Run lifecycle tests — expect PASS**

```bash
uv run pytest tests/test_databases/test_sqlite_connector.py::TestLifecycle -v
```

Expected: All 4 tests pass.

- [ ] **Step 2.5: Commit**

```bash
git add src/orcapod/databases/sqlite_connector.py tests/test_databases/test_sqlite_connector.py
git commit -m "feat(databases): implement SQLiteConnector connection lifecycle (PLT-1076)"
```

---

## Task 3: Schema Introspection — `get_table_names`

**Files:**
- Modify: `src/orcapod/databases/sqlite_connector.py`
- Modify: `tests/test_databases/test_sqlite_connector.py`

- [ ] **Step 3.1: Add `get_table_names` tests**

Add to the test file:

```python
# ---------------------------------------------------------------------------
# get_table_names
# ---------------------------------------------------------------------------


class TestGetTableNames:
    def test_empty_database(self, connector):
        assert connector.get_table_names() == []

    def test_returns_table_names(self, connector):
        connector._conn.execute('CREATE TABLE "foo" (id INTEGER PRIMARY KEY)')
        connector._conn.execute('CREATE TABLE "bar" (id INTEGER PRIMARY KEY)')
        assert connector.get_table_names() == ["bar", "foo"]  # sorted

    def test_excludes_views(self, connector):
        connector._conn.execute('CREATE TABLE "base" (id INTEGER PRIMARY KEY)')
        connector._conn.execute('CREATE VIEW "v_base" AS SELECT * FROM "base"')
        assert connector.get_table_names() == ["base"]
```

- [ ] **Step 3.2: Run — expect FAIL**

```bash
uv run pytest tests/test_databases/test_sqlite_connector.py::TestGetTableNames -v
```

Expected: FAIL — `NotImplementedError`.

- [ ] **Step 3.3: Implement `get_table_names`**

```python
def get_table_names(self) -> list[str]:
    """Return all table names in this database (excludes views).

    Returns:
        Sorted list of table name strings.
    """
    with self._lock:
        conn = self._require_open()
        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )
        return [row["name"] for row in cursor]
```

- [ ] **Step 3.4: Run — expect PASS**

```bash
uv run pytest tests/test_databases/test_sqlite_connector.py::TestGetTableNames -v
```

- [ ] **Step 3.5: Commit**

```bash
git add src/orcapod/databases/sqlite_connector.py tests/test_databases/test_sqlite_connector.py
git commit -m "feat(databases): implement SQLiteConnector.get_table_names (PLT-1076)"
```

---

## Task 4: Schema Introspection — `get_pk_columns` + `get_column_info`

**Files:**
- Modify: `src/orcapod/databases/sqlite_connector.py`
- Modify: `tests/test_databases/test_sqlite_connector.py`

- [ ] **Step 4.1: Add tests**

```python
# ---------------------------------------------------------------------------
# get_pk_columns
# ---------------------------------------------------------------------------


class TestGetPkColumns:
    def test_single_pk(self, connector):
        connector._conn.execute(
            'CREATE TABLE "t" (id TEXT PRIMARY KEY, val REAL)'
        )
        assert connector.get_pk_columns("t") == ["id"]

    def test_composite_pk_order_preserved(self, connector):
        connector._conn.execute(
            'CREATE TABLE "t" (a TEXT, b INTEGER, c REAL, PRIMARY KEY (a, b))'
        )
        assert connector.get_pk_columns("t") == ["a", "b"]

    def test_no_pk_returns_empty(self, connector):
        connector._conn.execute('CREATE TABLE "t" (val TEXT)')
        assert connector.get_pk_columns("t") == []


# ---------------------------------------------------------------------------
# get_column_info
# ---------------------------------------------------------------------------


class TestGetColumnInfo:
    def test_all_affinities(self, connector):
        connector._conn.execute(
            """CREATE TABLE "t" (
                a TEXT,
                b INTEGER,
                c REAL,
                d BLOB,
                e NUMERIC,
                f BOOLEAN
            )"""
        )
        infos = {ci.name: ci for ci in connector.get_column_info("t")}
        assert infos["a"].arrow_type == pa.large_string()
        assert infos["b"].arrow_type == pa.int64()
        assert infos["c"].arrow_type == pa.float64()
        assert infos["d"].arrow_type == pa.large_binary()
        assert infos["e"].arrow_type == pa.float64()
        assert infos["f"].arrow_type == pa.bool_()

    def test_nullable_from_notnull(self, connector):
        connector._conn.execute(
            'CREATE TABLE "t" (a TEXT NOT NULL, b TEXT)'
        )
        infos = {ci.name: ci for ci in connector.get_column_info("t")}
        assert infos["a"].nullable is False
        assert infos["b"].nullable is True

    def test_table_with_zero_rows_returns_column_metadata(self, connector):
        connector._conn.execute(
            'CREATE TABLE "t" (id INTEGER PRIMARY KEY, val TEXT)'
        )
        # Table exists but has no rows — must still return columns
        infos = connector.get_column_info("t")
        assert len(infos) == 2
        assert infos[0].name == "id"

    def test_nonexistent_table_returns_empty(self, connector):
        assert connector.get_column_info("no_such_table") == []
```

- [ ] **Step 4.2: Run — expect FAIL**

```bash
uv run pytest tests/test_databases/test_sqlite_connector.py::TestGetPkColumns tests/test_databases/test_sqlite_connector.py::TestGetColumnInfo -v
```

- [ ] **Step 4.3: Implement `get_pk_columns` and `get_column_info`**

```python
def get_pk_columns(self, table_name: str) -> list[str]:
    """Return primary-key column names in key-sequence order.

    Args:
        table_name: Name of the table to introspect.

    Returns:
        List of PK column names, empty if no primary key.
    """
    with self._lock:
        conn = self._require_open()
        cursor = conn.execute(f'PRAGMA table_info("{table_name}")')
        rows = [row for row in cursor if row["pk"] > 0]
        return [row["name"] for row in sorted(rows, key=lambda r: r["pk"])]

def get_column_info(self, table_name: str) -> list[ColumnInfo]:
    """Return column metadata with Arrow-mapped types.

    Args:
        table_name: Name of the table to introspect.

    Returns:
        List of ColumnInfo objects; empty list if table doesn't exist.
    """
    with self._lock:
        conn = self._require_open()
        cursor = conn.execute(f'PRAGMA table_info("{table_name}")')
        return [
            ColumnInfo(
                name=row["name"],
                arrow_type=_sqlite_type_to_arrow(row["type"]),
                nullable=not bool(row["notnull"]),
            )
            for row in cursor
        ]
```

- [ ] **Step 4.4: Run — expect PASS**

```bash
uv run pytest tests/test_databases/test_sqlite_connector.py::TestGetPkColumns tests/test_databases/test_sqlite_connector.py::TestGetColumnInfo -v
```

- [ ] **Step 4.5: Commit**

```bash
git add src/orcapod/databases/sqlite_connector.py tests/test_databases/test_sqlite_connector.py
git commit -m "feat(databases): implement get_pk_columns and get_column_info (PLT-1076)"
```

---

## Task 5: `create_table_if_not_exists`

**Files:**
- Modify: `src/orcapod/databases/sqlite_connector.py`
- Modify: `tests/test_databases/test_sqlite_connector.py`

- [ ] **Step 5.1: Add tests**

```python
# ---------------------------------------------------------------------------
# create_table_if_not_exists
# ---------------------------------------------------------------------------


class TestCreateTableIfNotExists:
    def _make_columns(self) -> list[ColumnInfo]:
        return [
            ColumnInfo("__record_id", pa.large_string(), nullable=False),
            ColumnInfo("value", pa.float64(), nullable=True),
        ]

    def test_creates_table(self, connector):
        connector.create_table_if_not_exists("my_table", self._make_columns(), "__record_id")
        assert "my_table" in connector.get_table_names()

    def test_idempotent(self, connector):
        cols = self._make_columns()
        connector.create_table_if_not_exists("my_table", cols, "__record_id")
        connector.create_table_if_not_exists("my_table", cols, "__record_id")  # must not raise

    def test_pk_column_set(self, connector):
        connector.create_table_if_not_exists("t", self._make_columns(), "__record_id")
        assert connector.get_pk_columns("t") == ["__record_id"]

    def test_column_types_match(self, connector):
        connector.create_table_if_not_exists("t", self._make_columns(), "__record_id")
        infos = {ci.name: ci for ci in connector.get_column_info("t")}
        assert infos["__record_id"].arrow_type == pa.large_string()
        assert infos["value"].arrow_type == pa.float64()

    def test_not_null_respected(self, connector):
        connector.create_table_if_not_exists("t", self._make_columns(), "__record_id")
        infos = {ci.name: ci for ci in connector.get_column_info("t")}
        assert infos["__record_id"].nullable is False
        assert infos["value"].nullable is True
```

- [ ] **Step 5.2: Run — expect FAIL**

```bash
uv run pytest tests/test_databases/test_sqlite_connector.py::TestCreateTableIfNotExists -v
```

- [ ] **Step 5.3: Implement `create_table_if_not_exists`**

```python
def create_table_if_not_exists(
    self,
    table_name: str,
    columns: list[ColumnInfo],
    pk_column: str,
) -> None:
    """Create a table with the given columns if it does not already exist.

    Args:
        table_name: Table to create.
        columns: Column definitions with Arrow-mapped types.
        pk_column: Name of the column to use as the primary key.
    """
    with self._lock:
        conn = self._require_open()
        col_defs = []
        for col in columns:
            sql_type = _arrow_type_to_sqlite_sql(col.arrow_type)
            not_null = " NOT NULL" if not col.nullable else ""
            col_defs.append(f'    "{col.name}" {sql_type}{not_null}')
        col_defs.append(f'    PRIMARY KEY ("{pk_column}")')
        cols_str = ",\n".join(col_defs)
        conn.execute(
            f'CREATE TABLE IF NOT EXISTS "{table_name}" (\n{cols_str}\n)'
        )
```

- [ ] **Step 5.4: Run — expect PASS**

```bash
uv run pytest tests/test_databases/test_sqlite_connector.py::TestCreateTableIfNotExists -v
```

- [ ] **Step 5.5: Commit**

```bash
git add src/orcapod/databases/sqlite_connector.py tests/test_databases/test_sqlite_connector.py
git commit -m "feat(databases): implement create_table_if_not_exists (PLT-1076)"
```

---

## Task 6: `upsert_records`

**Files:**
- Modify: `src/orcapod/databases/sqlite_connector.py`
- Modify: `tests/test_databases/test_sqlite_connector.py`

- [ ] **Step 6.1: Add tests**

```python
# ---------------------------------------------------------------------------
# upsert_records helpers
# ---------------------------------------------------------------------------


def _setup_table(connector: SQLiteConnector) -> None:
    """Create a simple test table."""
    cols = [
        ColumnInfo("id", pa.large_string(), nullable=False),
        ColumnInfo("score", pa.float64(), nullable=True),
    ]
    connector.create_table_if_not_exists("scores", cols, pk_column="id")


def _read_all(connector: SQLiteConnector) -> list[dict]:
    """Read all rows from 'scores' as a list of dicts."""
    cursor = connector._conn.execute('SELECT id, score FROM "scores" ORDER BY id')
    return [dict(row) for row in cursor]


# ---------------------------------------------------------------------------
# upsert_records
# ---------------------------------------------------------------------------


class TestUpsertRecords:
    def test_basic_insert(self, connector):
        _setup_table(connector)
        records = pa.table({"id": ["a", "b"], "score": [1.0, 2.0]})
        connector.upsert_records("scores", records, id_column="id")
        rows = _read_all(connector)
        assert len(rows) == 2
        assert rows[0]["id"] == "a"

    def test_skip_existing_false_overwrites(self, connector):
        _setup_table(connector)
        records = pa.table({"id": ["a"], "score": [1.0]})
        connector.upsert_records("scores", records, id_column="id")
        updated = pa.table({"id": ["a"], "score": [99.0]})
        connector.upsert_records("scores", updated, id_column="id", skip_existing=False)
        rows = _read_all(connector)
        assert rows[0]["score"] == 99.0

    def test_skip_existing_true_ignores_duplicate(self, connector):
        _setup_table(connector)
        records = pa.table({"id": ["a"], "score": [1.0]})
        connector.upsert_records("scores", records, id_column="id")
        duplicate = pa.table({"id": ["a"], "score": [99.0]})
        connector.upsert_records("scores", duplicate, id_column="id", skip_existing=True)
        rows = _read_all(connector)
        assert rows[0]["score"] == 1.0  # original preserved

    def test_column_order_differs_from_sql_table(self, connector):
        """Arrow table with columns in different order than the SQL table."""
        _setup_table(connector)
        # Arrow table has columns in reverse order vs. SQL: (score, id)
        records = pa.table({"score": [5.0], "id": ["z"]})
        connector.upsert_records("scores", records, id_column="id")
        rows = _read_all(connector)
        assert rows[0]["id"] == "z"
        assert rows[0]["score"] == 5.0
```

- [ ] **Step 6.2: Run — expect FAIL**

```bash
uv run pytest tests/test_databases/test_sqlite_connector.py::TestUpsertRecords -v
```

- [ ] **Step 6.3: Implement `upsert_records`**

```python
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
        id_column: Column used as the unique row identifier. Conflict
            detection is handled by the SQL PRIMARY KEY constraint;
            this argument is accepted for protocol compliance.
        skip_existing: If True, skip rows whose id already exists
            (INSERT OR IGNORE). If False, overwrite (INSERT OR REPLACE).
    """
    with self._lock:
        conn = self._require_open()
        cols = records.schema.names
        col_list = ", ".join(f'"{c}"' for c in cols)
        placeholders = ", ".join(f":{c}" for c in cols)
        verb = "INSERT OR IGNORE" if skip_existing else "INSERT OR REPLACE"
        sql = f'{verb} INTO "{table_name}" ({col_list}) VALUES ({placeholders})'
        conn.executemany(sql, records.to_pylist())
```

- [ ] **Step 6.4: Run — expect PASS**

```bash
uv run pytest tests/test_databases/test_sqlite_connector.py::TestUpsertRecords -v
```

- [ ] **Step 6.5: Commit**

```bash
git add src/orcapod/databases/sqlite_connector.py tests/test_databases/test_sqlite_connector.py
git commit -m "feat(databases): implement upsert_records (PLT-1076)"
```

---

## Task 7: `iter_batches`

**Files:**
- Modify: `src/orcapod/databases/sqlite_connector.py`
- Modify: `tests/test_databases/test_sqlite_connector.py`

- [ ] **Step 7.1: Add tests**

```python
# ---------------------------------------------------------------------------
# iter_batches
# ---------------------------------------------------------------------------


class TestIterBatches:
    def _make_table(self, connector: SQLiteConnector) -> None:
        connector._conn.execute(
            """CREATE TABLE "data" (
                id TEXT PRIMARY KEY,
                name TEXT,
                score REAL,
                count INTEGER,
                raw BLOB,
                flag BOOLEAN
            )"""
        )

    def test_empty_table_yields_nothing(self, connector):
        self._make_table(connector)
        batches = list(connector.iter_batches('SELECT * FROM "data"'))
        assert batches == []

    def test_single_batch(self, connector):
        self._make_table(connector)
        connector._conn.execute(
            'INSERT INTO "data" VALUES ("a", "Alice", 1.5, 10, NULL, 1)'
        )
        batches = list(connector.iter_batches('SELECT * FROM "data"'))
        assert len(batches) == 1
        assert batches[0].num_rows == 1
        assert batches[0]["id"][0].as_py() == "a"

    def test_multi_batch(self, connector):
        self._make_table(connector)
        for i in range(5):
            connector._conn.execute(
                f'INSERT INTO "data" VALUES ("id{i}", "name{i}", {float(i)}, {i}, NULL, 0)'
            )
        batches = list(connector.iter_batches('SELECT * FROM "data"', batch_size=2))
        assert len(batches) == 3  # 2 + 2 + 1
        total_rows = sum(b.num_rows for b in batches)
        assert total_rows == 5

    def test_parameterized_query(self, connector):
        self._make_table(connector)
        connector._conn.execute(
            'INSERT INTO "data" VALUES ("a", "Alice", 1.5, 1, NULL, 1)'
        )
        connector._conn.execute(
            'INSERT INTO "data" VALUES ("b", "Bob", 2.5, 2, NULL, 0)'
        )
        batches = list(connector.iter_batches(
            'SELECT * FROM "data" WHERE id = ?', params=["a"]
        ))
        assert batches[0].num_rows == 1
        assert batches[0]["id"][0].as_py() == "a"

    def test_null_values_in_all_types(self, connector):
        self._make_table(connector)
        connector._conn.execute(
            'INSERT INTO "data" VALUES ("x", NULL, NULL, NULL, NULL, NULL)'
        )
        batches = list(connector.iter_batches('SELECT * FROM "data"'))
        row = batches[0]
        assert row["name"][0].as_py() is None
        assert row["score"][0].as_py() is None
        assert row["count"][0].as_py() is None

    def test_schema_arrow_types_match_column_info(self, connector):
        self._make_table(connector)
        connector._conn.execute(
            'INSERT INTO "data" VALUES ("a", "Alice", 1.5, 10, b"raw", 1)'
        )
        batches = list(connector.iter_batches('SELECT * FROM "data"'))
        schema = batches[0].schema
        assert schema.field("id").type == pa.large_string()
        assert schema.field("name").type == pa.large_string()
        assert schema.field("score").type == pa.float64()
        assert schema.field("count").type == pa.int64()
        assert schema.field("raw").type == pa.large_binary()
        assert schema.field("flag").type == pa.bool_()

    def test_boolean_values_coerced_correctly(self, connector):
        self._make_table(connector)
        connector._conn.execute(
            'INSERT INTO "data" VALUES ("t", NULL, NULL, NULL, NULL, 1)'
        )
        connector._conn.execute(
            'INSERT INTO "data" VALUES ("f", NULL, NULL, NULL, NULL, 0)'
        )
        batches = list(connector.iter_batches('SELECT * FROM "data" ORDER BY id'))
        flags = [batches[0]["flag"][i].as_py() for i in range(2)]
        assert flags == [False, True]  # 'f' then 't' after ORDER BY
```

- [ ] **Step 7.2: Run — expect FAIL**

```bash
uv run pytest tests/test_databases/test_sqlite_connector.py::TestIterBatches -v
```

- [ ] **Step 7.3: Implement `iter_batches`**

```python
def iter_batches(
    self,
    query: str,
    params: Any = None,
    batch_size: int = 1000,
) -> Iterator[pa.RecordBatch]:
    """Execute a query and yield results as Arrow RecordBatches.

    Args:
        query: SQL query string. Table names should be double-quoted.
        params: Optional query parameters.
        batch_size: Maximum rows per yielded batch.

    Yields:
        Arrow RecordBatch objects.
    """
    import pyarrow as _pa  # noqa: PLC0415

    with self._lock:
        conn = self._require_open()
        cursor = conn.execute(query, params or [])
        if cursor.description is None:
            return
        col_names = [d[0] for d in cursor.description]

        # Resolve Arrow types via get_column_info on the first table referenced
        match = re.search(r'FROM\s+"([^"]+)"', query, re.IGNORECASE)
        type_lookup: dict[str, _pa.DataType] = {}
        if match:
            type_lookup = {
                c.name: c.arrow_type
                for c in self.get_column_info(match.group(1))
            }

        arrow_types: list[_pa.DataType] = []
        for name in col_names:
            if name in type_lookup:
                arrow_types.append(type_lookup[name])
            else:
                logger.warning(
                    "Column %r not found in table info; defaulting to large_string",
                    name,
                )
                arrow_types.append(_pa.large_string())

        schema = _pa.schema(
            [_pa.field(name, t) for name, t in zip(col_names, arrow_types)]
        )

        while True:
            chunk = cursor.fetchmany(batch_size)
            if not chunk:
                break
            arrays = [
                _pa.array(
                    _coerce_column([row[i] for row in chunk], arrow_types[i]),
                    type=arrow_types[i],
                )
                for i in range(len(col_names))
            ]
            yield _pa.RecordBatch.from_arrays(arrays, schema=schema)
```

- [ ] **Step 7.4: Run — expect PASS**

```bash
uv run pytest tests/test_databases/test_sqlite_connector.py::TestIterBatches -v
```

- [ ] **Step 7.5: Commit**

```bash
git add src/orcapod/databases/sqlite_connector.py tests/test_databases/test_sqlite_connector.py
git commit -m "feat(databases): implement iter_batches (PLT-1076)"
```

---

## Task 8: Config Serialization + Export + Protocol Conformance

**Files:**
- Modify: `src/orcapod/databases/sqlite_connector.py`
- Modify: `src/orcapod/databases/__init__.py`
- Modify: `tests/test_databases/test_sqlite_connector.py`

- [ ] **Step 8.1: Add config + protocol conformance tests**

```python
# ---------------------------------------------------------------------------
# Config serialization
# ---------------------------------------------------------------------------


class TestConfig:
    def test_to_config_shape(self, connector):
        cfg = connector.to_config()
        assert cfg["connector_type"] == "sqlite"
        assert cfg["db_path"] == ":memory:"

    def test_from_config_round_trip(self):
        original = SQLiteConnector(":memory:")
        cfg = original.to_config()
        restored = SQLiteConnector.from_config(cfg)
        assert restored.to_config() == cfg
        original.close()
        restored.close()

    def test_from_config_wrong_type_raises(self):
        with pytest.raises(ValueError, match="connector_type"):
            SQLiteConnector.from_config({"connector_type": "postgresql", "db_path": ":memory:"})


# ---------------------------------------------------------------------------
# Protocol conformance
# ---------------------------------------------------------------------------


class TestProtocolConformance:
    def test_isinstance_check(self, connector):
        assert isinstance(connector, DBConnectorProtocol)
```

- [ ] **Step 8.2: Run — expect FAIL**

```bash
uv run pytest tests/test_databases/test_sqlite_connector.py::TestConfig tests/test_databases/test_sqlite_connector.py::TestProtocolConformance -v
```

- [ ] **Step 8.3: Implement `to_config` and `from_config`**

Replace the stubs in `sqlite_connector.py`:

```python
def to_config(self) -> dict[str, Any]:
    """Serialize connection configuration to a JSON-compatible dict.

    Returns:
        Dict with ``connector_type`` and ``db_path`` keys.
    """
    return {"connector_type": "sqlite", "db_path": str(self._db_path)}

@classmethod
def from_config(cls, config: dict[str, Any]) -> SQLiteConnector:
    """Reconstruct a SQLiteConnector from a config dict.

    Args:
        config: Dict with ``connector_type`` and ``db_path`` keys.

    Returns:
        A new SQLiteConnector instance.

    Raises:
        ValueError: If ``connector_type`` is not ``"sqlite"``.
    """
    if config.get("connector_type") != "sqlite":
        raise ValueError(
            f"Expected connector_type 'sqlite', got {config.get('connector_type')!r}"
        )
    return cls(db_path=config["db_path"])
```

- [ ] **Step 8.4: Run — expect PASS**

```bash
uv run pytest tests/test_databases/test_sqlite_connector.py::TestConfig tests/test_databases/test_sqlite_connector.py::TestProtocolConformance -v
```

- [ ] **Step 8.5: Export `SQLiteConnector` from `databases/__init__.py`**

In `src/orcapod/databases/__init__.py`, add the import and `__all__` entry:

```python
from .connector_arrow_database import ConnectorArrowDatabase
from .delta_lake_databases import DeltaTableDatabase
from .in_memory_databases import InMemoryArrowDatabase
from .noop_database import NoOpArrowDatabase
from .sqlite_connector import SQLiteConnector  # add this line

__all__ = [
    "ConnectorArrowDatabase",
    "DeltaTableDatabase",
    "InMemoryArrowDatabase",
    "NoOpArrowDatabase",
    "SQLiteConnector",  # add this entry
]
```

Also update the comment block at the bottom to reflect that `SQLiteConnector` is now implemented (remove `-- PLT-1076 (stdlib sqlite3, zero extra deps)` placeholder text):

```python
# Relational DB connector implementations satisfy DBConnectorProtocol
# (orcapod.protocols.db_connector_protocol) and can be passed to either
# ConnectorArrowDatabase (read+write ArrowDatabaseProtocol) or
# DBTableSource (read-only Source):
#
#   SQLiteConnector      -- stdlib sqlite3, zero extra deps
#   PostgreSQLConnector  -- PLT-1075 (psycopg3)
#   SpiralDBConnector    -- PLT-1074
```

- [ ] **Step 8.6: Verify export works**

```bash
uv run python -c "from orcapod.databases import SQLiteConnector; print(SQLiteConnector)"
```

Expected: `<class 'orcapod.databases.sqlite_connector.SQLiteConnector'>`

- [ ] **Step 8.7: Commit**

```bash
git add src/orcapod/databases/sqlite_connector.py src/orcapod/databases/__init__.py tests/test_databases/test_sqlite_connector.py
git commit -m "feat(databases): implement config serialization, export SQLiteConnector (PLT-1076)"
```

---

## Task 9: Integration Test with `ConnectorArrowDatabase`

This task validates the full stack: `SQLiteConnector` → `ConnectorArrowDatabase` → read back.

**Files:**
- Modify: `tests/test_databases/test_sqlite_connector.py`

- [ ] **Step 9.1: Add integration tests**

```python
# ---------------------------------------------------------------------------
# Integration: SQLiteConnector + ConnectorArrowDatabase
# ---------------------------------------------------------------------------


class TestConnectorArrowDatabaseIntegration:
    """End-to-end tests: SQLiteConnector used as the backend for ConnectorArrowDatabase."""

    def test_add_record_flush_get_by_id(self):
        connector = SQLiteConnector(":memory:")
        db = ConnectorArrowDatabase(connector)

        record = pa.table({"value": [42.0], "label": ["hello"]})
        db.add_record(("results", "fn1"), record_id="rec-1", record=record)
        db.flush()

        result = db.get_record_by_id(("results", "fn1"), record_id="rec-1")
        assert result is not None
        assert result["value"][0].as_py() == 42.0
        assert result["label"][0].as_py() == "hello"
        connector.close()

    def test_multiple_records_flush_get_all(self):
        connector = SQLiteConnector(":memory:")
        db = ConnectorArrowDatabase(connector)

        for i in range(3):
            record = pa.table({"x": [float(i)]})
            db.add_record(("data",), record_id=f"r{i}", record=record)
        db.flush()

        all_records = db.get_all_records(("data",))
        assert all_records is not None
        assert all_records.num_rows == 3
        connector.close()

    def test_second_flush_same_table_passes_schema_validation(self):
        """Second flush to an existing table must not raise schema mismatch."""
        connector = SQLiteConnector(":memory:")
        db = ConnectorArrowDatabase(connector)

        record = pa.table({"value": [1.0]})
        db.add_record(("t",), record_id="r1", record=record)
        db.flush()

        record2 = pa.table({"value": [2.0]})
        db.add_record(("t",), record_id="r2", record=record2)
        db.flush()  # must not raise ValueError("Schema mismatch")

        all_records = db.get_all_records(("t",))
        assert all_records is not None
        assert all_records.num_rows == 2
        connector.close()

    def test_skip_duplicates_true_at_sql_level(self):
        """skip_duplicates=True must not overwrite existing records."""
        connector = SQLiteConnector(":memory:")
        db = ConnectorArrowDatabase(connector)

        record = pa.table({"score": [1.0]})
        db.add_record(("scores",), record_id="r1", record=record, skip_duplicates=False)
        db.flush()

        duplicate = pa.table({"score": [99.0]})
        db.add_record(("scores",), record_id="r1", record=duplicate, skip_duplicates=True)
        db.flush()

        result = db.get_record_by_id(("scores",), record_id="r1")
        assert result is not None
        assert result["score"][0].as_py() == 1.0  # original preserved
        connector.close()
```

- [ ] **Step 9.2: Run — expect PASS (no new implementation needed)**

```bash
uv run pytest tests/test_databases/test_sqlite_connector.py::TestConnectorArrowDatabaseIntegration -v
```

Expected: All 4 tests pass.

- [ ] **Step 9.3: Run the full test file to confirm nothing broken**

```bash
uv run pytest tests/test_databases/test_sqlite_connector.py -v
```

Expected: All tests pass.

- [ ] **Step 9.4: Commit**

```bash
git add tests/test_databases/test_sqlite_connector.py
git commit -m "test(databases): add ConnectorArrowDatabase integration tests for SQLiteConnector (PLT-1076)"
```

---

## Final Verification

- [ ] **Run the full test suite to check for regressions**

```bash
uv run pytest tests/ -x -q
```

Expected: All existing tests pass, plus the new SQLiteConnector tests.

- [ ] **Update Linear issue status**

Update PLT-1076 status to **In Review** once the PR is open.
