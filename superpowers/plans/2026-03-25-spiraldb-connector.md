# SpiralDBConnector Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement `SpiralDBConnector` — a `DBConnectorProtocol` backed by SpiralDB (pyspiral), dataset-scoped, Arrow-native, enabling `ConnectorArrowDatabase` (read/write) and `DBTableSource` (read-only) for SpiralDB.

**Architecture:** `SpiralDBConnector` bridges the SQL-style `DBConnectorProtocol` interface to SpiralDB's expression-based scan/write API. It is dataset-scoped — a `(project_id, dataset)` pair is fixed at construction. All other layers (`ConnectorArrowDatabase`, `DBTableSource`) are unmodified; only the connector varies. `pyspiral` (`spiral` module) is lazy-loaded so `import orcapod` does not fail without it.

**Tech Stack:** Python ≥ 3.10, `pyspiral >= 0.11.6` (optional extra `orcapod[spiraldb]`), `pyarrow`, `orcapod.utils.lazy_module.LazyModule`, `pytest`, `unittest.mock`

---

## File Structure

| File | Status | Responsibility |
|---|---|---|
| `src/orcapod/databases/spiraldb_connector.py` | **Create** | `SpiralDBConnector` class + `_parse_table_name` helper |
| `src/orcapod/databases/__init__.py` | **Modify** | Add `SpiralDBConnector` import and `__all__` entry |
| `tests/test_databases/test_spiraldb_connector.py` | **Create** | Unit tests (mocked spiral, no network access) |
| `tests/test_databases/test_spiraldb_connector_integration.py` | **Create** | Integration tests (live dev project, skipped without env var) |

## Reference Reading

Before starting, read these files:
- `src/orcapod/databases/sqlite_connector.py` — reference implementation to follow patterns from
- `src/orcapod/protocols/db_connector_protocol.py` — protocol definition (what must be satisfied)
- `src/orcapod/types.py` — `ColumnInfo` dataclass definition
- `superpowers/specs/2026-03-25-spiraldb-connector-design.md` — authoritative spec with all implementation decisions

---

## Shared Mock Fixture Reference

All unit tests patch `orcapod.databases.spiraldb_connector.sp` (the module-level lazy `spiral` reference). This prevents any real network calls. Every test class gets access to `mock_sp`, `mock_project`, and `connector` fixtures:

```python
from unittest.mock import MagicMock, patch

@pytest.fixture()
def mock_sp():
    with patch("orcapod.databases.spiraldb_connector.sp") as mock:
        yield mock

@pytest.fixture()
def mock_project(mock_sp):
    project = MagicMock(name="MockProject")
    mock_sp.Spiral.return_value.project.return_value = project
    return project

@pytest.fixture()
def connector(mock_sp, mock_project):
    return SpiralDBConnector(project_id="test-project-123", dataset="default")
```

Why this works: `SpiralDBConnector.__init__` calls `sp.Spiral(overrides=overrides)` which becomes `mock_sp.Spiral(overrides=None)` → `mock_sp.Spiral.return_value`. Then `.project(project_id)` on that → `mock_project`. `self._spiral` = `mock_sp.Spiral.return_value`, which is used by `iter_batches` and `upsert_records(skip_existing=True)`.

---

## Task 1: Module scaffold + `_parse_table_name` helper

**Files:**
- Create: `src/orcapod/databases/spiraldb_connector.py`
- Create: `tests/test_databases/test_spiraldb_connector.py`

- [ ] **Step 1: Create the test file with `_parse_table_name` tests**

Create `tests/test_databases/test_spiraldb_connector.py`:

```python
"""Tests for SpiralDBConnector — DBConnectorProtocol backed by SpiralDB."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from orcapod.databases.spiraldb_connector import SpiralDBConnector, _parse_table_name
from orcapod.protocols.db_connector_protocol import DBConnectorProtocol
from orcapod.types import ColumnInfo


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def mock_sp():
    with patch("orcapod.databases.spiraldb_connector.sp") as mock:
        yield mock


@pytest.fixture()
def mock_project(mock_sp):
    project = MagicMock(name="MockProject")
    mock_sp.Spiral.return_value.project.return_value = project
    return project


@pytest.fixture()
def connector(mock_sp, mock_project):
    return SpiralDBConnector(project_id="test-project-123", dataset="default")


# ---------------------------------------------------------------------------
# _parse_table_name helper
# ---------------------------------------------------------------------------


class TestParseTableName:
    def test_double_quoted_identifier(self):
        assert _parse_table_name('SELECT * FROM "my_table"') == "my_table"

    def test_unquoted_fallback(self):
        assert _parse_table_name("SELECT * FROM my_table") == "my_table"

    def test_case_insensitive_from(self):
        assert _parse_table_name('select * from "signals"') == "signals"

    def test_raises_on_no_table_name(self):
        with pytest.raises(ValueError, match="Cannot parse table name"):
            _parse_table_name("SELECT 1")
```

- [ ] **Step 2: Create the source file with module structure and `_parse_table_name`**

Create `src/orcapod/databases/spiraldb_connector.py`:

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
from __future__ import annotations

import logging
import re
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

from orcapod.types import ColumnInfo

if TYPE_CHECKING:
    import pyarrow as pa
    import spiral as sp
else:
    from orcapod.utils.lazy_module import LazyModule

    pa = LazyModule("pyarrow")
    sp = LazyModule("spiral")

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------


def _parse_table_name(query: str) -> str:
    """Extract the plain table name from a SELECT * FROM ... query string.

    Supports double-quoted identifiers (``"table_name"``) and unquoted bare
    identifiers (``table_name``). Always returns the plain table name — not
    a ``dataset.table`` qualified form.

    Args:
        query: SQL-style query string, e.g. ``'SELECT * FROM "my_table"'``.

    Returns:
        Plain table name string.

    Raises:
        ValueError: If no table name can be parsed from the query.
    """
    m = re.search(r'FROM\s+"([^"]+)"', query, re.IGNORECASE)
    if not m:
        m = re.search(r"FROM\s+(\w+)", query, re.IGNORECASE)
    if not m:
        raise ValueError(f"Cannot parse table name from query: {query!r}")
    return m.group(1)


# ---------------------------------------------------------------------------
# SpiralDBConnector (stub — methods implemented task by task)
# ---------------------------------------------------------------------------


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
    ) -> None:
        raise NotImplementedError

    def _require_open(self) -> None:
        raise NotImplementedError

    def _table_id(self, table_name: str) -> str:
        raise NotImplementedError

    def get_table_names(self) -> list[str]:
        raise NotImplementedError

    def get_pk_columns(self, table_name: str) -> list[str]:
        raise NotImplementedError

    def get_column_info(self, table_name: str) -> list[ColumnInfo]:
        raise NotImplementedError

    def iter_batches(
        self,
        query: str,
        params: Any = None,
        batch_size: int = 1000,
    ) -> Iterator[pa.RecordBatch]:
        raise NotImplementedError
        yield  # make this a generator function for type-checking

    def create_table_if_not_exists(
        self,
        table_name: str,
        columns: list[ColumnInfo],
        pk_column: str,
    ) -> None:
        raise NotImplementedError

    def upsert_records(
        self,
        table_name: str,
        records: pa.Table,
        id_column: str,
        skip_existing: bool = False,
    ) -> None:
        raise NotImplementedError

    def close(self) -> None:
        raise NotImplementedError

    def __enter__(self) -> SpiralDBConnector:
        raise NotImplementedError

    def __exit__(self, *args: Any) -> None:
        raise NotImplementedError

    def to_config(self) -> dict[str, Any]:
        raise NotImplementedError

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> SpiralDBConnector:
        raise NotImplementedError
```

- [ ] **Step 3: Run `_parse_table_name` tests — expect PASS**

```bash
cd /home/kurouto/kurouto-jobs/2b13224a-4411-4797-8809-051d41cda0ae/orcapod-python
uv run pytest tests/test_databases/test_spiraldb_connector.py::TestParseTableName -v
```

Expected: 4 tests PASS (these are pure functions, no mocking needed)

- [ ] **Step 4: Commit**

```bash
git add src/orcapod/databases/spiraldb_connector.py tests/test_databases/test_spiraldb_connector.py
git commit -m "feat(spiraldb): scaffold SpiralDBConnector + _parse_table_name helper"
```

---

## Task 2: Lifecycle (`__init__`, `_require_open`, `_table_id`, `close`, context manager)

**Files:**
- Modify: `src/orcapod/databases/spiraldb_connector.py`
- Modify: `tests/test_databases/test_spiraldb_connector.py`

- [ ] **Step 1: Add lifecycle and protocol conformance tests**

Append to `tests/test_databases/test_spiraldb_connector.py`:

```python
# ---------------------------------------------------------------------------
# Protocol conformance
# ---------------------------------------------------------------------------


class TestProtocolConformance:
    def test_isinstance_check_passes(self, connector):
        assert isinstance(connector, DBConnectorProtocol)


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------


class TestLifecycle:
    def test_close_sets_closed_flag(self, connector):
        assert not connector._closed
        connector.close()
        assert connector._closed

    def test_double_close_is_safe(self, connector):
        connector.close()
        connector.close()  # must not raise

    def test_context_manager_closes_on_exit(self, mock_sp, mock_project):
        c = SpiralDBConnector(project_id="p", dataset="default")
        with c:
            assert not c._closed
        assert c._closed

    def test_context_manager_does_not_suppress_exceptions(self, mock_sp, mock_project):
        c = SpiralDBConnector(project_id="p", dataset="default")
        with pytest.raises(RuntimeError, match="boom"):
            with c:
                raise RuntimeError("boom")
        assert c._closed  # still closed even when exception raised

    def test_methods_raise_after_close(self, connector, mock_project):
        connector.close()
        with pytest.raises(RuntimeError, match="closed"):
            connector.get_table_names()
        with pytest.raises(RuntimeError, match="closed"):
            connector.get_pk_columns("t")
        with pytest.raises(RuntimeError, match="closed"):
            connector.get_column_info("t")
        with pytest.raises(RuntimeError, match="closed"):
            list(connector.iter_batches('SELECT * FROM "t"'))
        with pytest.raises(RuntimeError, match="closed"):
            connector.create_table_if_not_exists("t", [], "id")
        with pytest.raises(RuntimeError, match="closed"):
            connector.upsert_records("t", pa.table({"id": pa.array([], type=pa.string())}), "id")
        with pytest.raises(RuntimeError, match="closed"):
            connector.to_config()
```

- [ ] **Step 2: Run — expect FAIL**

```bash
uv run pytest tests/test_databases/test_spiraldb_connector.py::TestProtocolConformance tests/test_databases/test_spiraldb_connector.py::TestLifecycle -v
```

Expected: FAIL (NotImplementedError from `__init__`)

- [ ] **Step 3: Replace stub lifecycle methods in `spiraldb_connector.py`**

Replace the stub `__init__`, `_require_open`, `_table_id`, `close`, `__enter__`, `__exit__` with:

```python
def __init__(
    self,
    project_id: str,
    dataset: str = "default",
    overrides: dict[str, str] | None = None,
) -> None:
    self._project_id = project_id
    self._dataset = dataset
    self._overrides = overrides
    self._spiral = sp.Spiral(overrides=overrides)
    self._project = self._spiral.project(project_id)
    self._closed = False

def _require_open(self) -> None:
    """Raise RuntimeError if this connector has been closed."""
    if self._closed:
        raise RuntimeError("SpiralDBConnector is closed")

def _table_id(self, table_name: str) -> str:
    """Return the dataset-qualified table identifier for a plain table name."""
    return f"{self._dataset}.{table_name}"

def close(self) -> None:
    """Mark this connector as closed. Idempotent. No network teardown needed."""
    self._closed = True

def __enter__(self) -> SpiralDBConnector:
    return self

def __exit__(self, *args: Any) -> None:
    self.close()
```

- [ ] **Step 4: Run — expect PASS**

```bash
uv run pytest tests/test_databases/test_spiraldb_connector.py::TestProtocolConformance tests/test_databases/test_spiraldb_connector.py::TestLifecycle -v
```

Expected: all PASS

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/databases/spiraldb_connector.py tests/test_databases/test_spiraldb_connector.py
git commit -m "feat(spiraldb): implement lifecycle (__init__, close, context manager)"
```

---

## Task 3: Schema introspection (`get_table_names`, `get_pk_columns`, `get_column_info`)

**Files:**
- Modify: `src/orcapod/databases/spiraldb_connector.py`
- Modify: `tests/test_databases/test_spiraldb_connector.py`

- [ ] **Step 1: Add schema introspection tests**

Append to `tests/test_databases/test_spiraldb_connector.py`:

```python
# ---------------------------------------------------------------------------
# Schema introspection
# ---------------------------------------------------------------------------


class TestGetTableNames:
    def test_returns_sorted_plain_names_for_dataset(self, connector, mock_project):
        r1 = MagicMock(dataset="default", table="signals")
        r2 = MagicMock(dataset="other", table="signals")  # excluded — wrong dataset
        r3 = MagicMock(dataset="default", table="events")
        mock_project.list_tables.return_value = [r1, r2, r3]
        assert connector.get_table_names() == ["events", "signals"]

    def test_empty_project_returns_empty_list(self, connector, mock_project):
        mock_project.list_tables.return_value = []
        assert connector.get_table_names() == []

    def test_all_tables_in_other_dataset_returns_empty(self, connector, mock_project):
        r1 = MagicMock(dataset="other", table="foo")
        mock_project.list_tables.return_value = [r1]
        assert connector.get_table_names() == []


class TestGetPkColumns:
    def test_single_pk(self, connector, mock_project):
        mock_table = MagicMock()
        mock_table.key_schema.names = ["id"]
        mock_project.table.return_value = mock_table
        assert connector.get_pk_columns("my_table") == ["id"]
        mock_project.table.assert_called_once_with("default.my_table")

    def test_composite_pk_preserves_order(self, connector, mock_project):
        mock_table = MagicMock()
        mock_table.key_schema.names = ["session_id", "timestamp", "probe_id"]
        mock_project.table.return_value = mock_table
        assert connector.get_pk_columns("spike_data") == [
            "session_id", "timestamp", "probe_id"
        ]

    def test_empty_key_schema_returns_empty_list(self, connector, mock_project):
        mock_table = MagicMock()
        mock_table.key_schema.names = []
        mock_project.table.return_value = mock_table
        assert connector.get_pk_columns("no_key_table") == []


class TestGetColumnInfo:
    def test_arrow_types_pass_through_unchanged(self, connector, mock_project):
        arrow_schema = pa.schema([
            pa.field("id", pa.string(), nullable=False),
            pa.field("value", pa.float64(), nullable=True),
            pa.field("count", pa.int64(), nullable=True),
        ])
        mock_table = MagicMock()
        mock_table.schema.return_value.to_arrow.return_value = arrow_schema
        mock_project.table.return_value = mock_table
        result = connector.get_column_info("my_table")
        assert result == [
            ColumnInfo("id", pa.string(), nullable=False),
            ColumnInfo("value", pa.float64(), nullable=True),
            ColumnInfo("count", pa.int64(), nullable=True),
        ]
        mock_project.table.assert_called_once_with("default.my_table")

    def test_nonexistent_table_propagates_pyspiral_exception(self, connector, mock_project):
        mock_project.table.side_effect = RuntimeError("table not found")
        with pytest.raises(RuntimeError, match="table not found"):
            connector.get_column_info("nonexistent")
```

- [ ] **Step 2: Run — expect FAIL**

```bash
uv run pytest tests/test_databases/test_spiraldb_connector.py::TestGetTableNames tests/test_databases/test_spiraldb_connector.py::TestGetPkColumns tests/test_databases/test_spiraldb_connector.py::TestGetColumnInfo -v
```

Expected: FAIL (NotImplementedError)

- [ ] **Step 3: Implement schema introspection in `spiraldb_connector.py`**

Replace stub methods:

```python
def get_table_names(self) -> list[str]:
    """Return all table names in this connector's dataset, sorted alphabetically.

    Filters ``project.list_tables()`` to the connector's dataset. Only the
    plain ``.table`` field is returned; the opaque ``.id`` handle is never
    exposed.

    Returns:
        Sorted list of plain table name strings (no ``dataset.`` prefix).
    """
    self._require_open()
    resources = self._project.list_tables()
    return sorted(r.table for r in resources if r.dataset == self._dataset)

def get_pk_columns(self, table_name: str) -> list[str]:
    """Return primary-key column names in declaration order.

    Args:
        table_name: Plain table name (no dataset prefix).

    Returns:
        List of PK column names; empty list if the table has no key schema.
    """
    self._require_open()
    return list(self._project.table(self._table_id(table_name)).key_schema.names)

def get_column_info(self, table_name: str) -> list[ColumnInfo]:
    """Return column metadata with Arrow types.

    SpiralDB is Arrow-native; no type mapping layer is needed. Types are
    taken directly from the table's Arrow schema. Note: SpiralDB silently
    drops timezone information from timestamp columns at storage time —
    ``timestamp[us, tz=UTC]`` is returned as ``timestamp[us]``.

    Args:
        table_name: Plain table name (no dataset prefix).

    Returns:
        List of ColumnInfo objects. Propagates pyspiral exception if the
        table does not exist (no empty-list fallback).
    """
    self._require_open()
    arrow_schema = (
        self._project.table(self._table_id(table_name)).schema().to_arrow()
    )
    return [
        ColumnInfo(name=field.name, arrow_type=field.type, nullable=field.nullable)
        for field in arrow_schema
    ]
```

- [ ] **Step 4: Run — expect PASS**

```bash
uv run pytest tests/test_databases/test_spiraldb_connector.py::TestGetTableNames tests/test_databases/test_spiraldb_connector.py::TestGetPkColumns tests/test_databases/test_spiraldb_connector.py::TestGetColumnInfo -v
```

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/databases/spiraldb_connector.py tests/test_databases/test_spiraldb_connector.py
git commit -m "feat(spiraldb): implement schema introspection (get_table_names, get_pk_columns, get_column_info)"
```

---

## Task 4: `iter_batches`

**Files:**
- Modify: `src/orcapod/databases/spiraldb_connector.py`
- Modify: `tests/test_databases/test_spiraldb_connector.py`

- [ ] **Step 1: Add `iter_batches` tests**

Append to `tests/test_databases/test_spiraldb_connector.py`:

```python
# ---------------------------------------------------------------------------
# iter_batches
# ---------------------------------------------------------------------------


class TestIterBatches:
    def _wire_scan(self, mock_sp, mock_project, batches: list) -> None:
        """Wire mock_sp so that scan().to_record_batches() returns ``batches``."""
        mock_project.table.return_value.select.return_value = MagicMock()
        mock_sp.Spiral.return_value.scan.return_value.to_record_batches.return_value = iter(
            batches
        )

    def test_full_scan_yields_batches(self, connector, mock_sp, mock_project):
        batch = pa.record_batch(
            {"id": pa.array(["a", "b"]), "v": pa.array([1, 2])}
        )
        self._wire_scan(mock_sp, mock_project, [batch])
        result = list(connector.iter_batches('SELECT * FROM "my_table"'))
        assert len(result) == 1
        assert result[0] == batch
        mock_project.table.assert_called_once_with("default.my_table")

    def test_empty_table_yields_no_batches(self, connector, mock_sp, mock_project):
        self._wire_scan(mock_sp, mock_project, [])
        result = list(connector.iter_batches('SELECT * FROM "empty_table"'))
        assert result == []

    def test_multiple_batches_all_yielded(self, connector, mock_sp, mock_project):
        b1 = pa.record_batch({"id": pa.array(["a"])})
        b2 = pa.record_batch({"id": pa.array(["b"])})
        self._wire_scan(mock_sp, mock_project, [b1, b2])
        result = list(connector.iter_batches('SELECT * FROM "t"'))
        assert len(result) == 2

    def test_params_not_none_emits_warning(self, connector, mock_sp, mock_project, caplog):
        import logging
        self._wire_scan(mock_sp, mock_project, [])
        with caplog.at_level(logging.WARNING):
            list(connector.iter_batches('SELECT * FROM "t"', params={"x": 1}))
        assert any("params" in msg.lower() for msg in caplog.messages)

    def test_batch_size_passed_to_spiral(self, connector, mock_sp, mock_project):
        self._wire_scan(mock_sp, mock_project, [])
        list(connector.iter_batches('SELECT * FROM "t"', batch_size=500))
        mock_sp.Spiral.return_value.scan.return_value.to_record_batches.assert_called_with(
            batch_size=500
        )

    def test_nonexistent_table_propagates_exception(self, connector, mock_sp, mock_project):
        mock_project.table.side_effect = RuntimeError("table not found")
        with pytest.raises(RuntimeError, match="table not found"):
            list(connector.iter_batches('SELECT * FROM "missing"'))

    def test_tbl_select_called_for_full_scan(self, connector, mock_sp, mock_project):
        self._wire_scan(mock_sp, mock_project, [])
        list(connector.iter_batches('SELECT * FROM "t"'))
        mock_project.table.return_value.select.assert_called_once_with()
```

- [ ] **Step 2: Run — expect FAIL**

```bash
uv run pytest tests/test_databases/test_spiraldb_connector.py::TestIterBatches -v
```

Expected: FAIL (NotImplementedError)

- [ ] **Step 3: Implement `iter_batches` in `spiraldb_connector.py`**

Replace the stub:

```python
def iter_batches(
    self,
    query: str,
    params: Any = None,
    batch_size: int = 1000,
) -> Iterator[pa.RecordBatch]:
    """Execute a full-table scan and yield results as Arrow RecordBatches.

    SpiralDB has no SQL engine. The table name is parsed from the query's
    FROM clause (double-quoted or unquoted). Only ``SELECT * FROM "table"``
    patterns are supported; WHERE clauses and projections are silently ignored.

    The query must use a plain (non-qualified) table name — not
    ``"dataset.table"``. Passing a qualified name would cause a pyspiral
    lookup failure.

    Args:
        query: SQL-style query. Must contain ``FROM "table_name"``.
        params: Accepted for protocol compliance. SpiralDB has no parameterised
            interface — if not ``None``, a warning is logged and params are
            not used.
        batch_size: Passed to ``Scan.to_record_batches()``; actual batch
            sizing is controlled by the Spiral execution engine.

    Yields:
        Arrow RecordBatch objects. Yields nothing for an empty table.
    """
    self._require_open()
    if params is not None:
        logger.warning(
            "SpiralDBConnector does not support query parameters; "
            "ignoring params=%r",
            params,
        )
    table_name = _parse_table_name(query)
    tbl = self._project.table(self._table_id(table_name))
    reader = self._spiral.scan(tbl.select()).to_record_batches(batch_size=batch_size)
    yield from reader
```

- [ ] **Step 4: Run — expect PASS**

```bash
uv run pytest tests/test_databases/test_spiraldb_connector.py::TestIterBatches -v
```

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/databases/spiraldb_connector.py tests/test_databases/test_spiraldb_connector.py
git commit -m "feat(spiraldb): implement iter_batches"
```

---

## Task 5: `create_table_if_not_exists`

**Files:**
- Modify: `src/orcapod/databases/spiraldb_connector.py`
- Modify: `tests/test_databases/test_spiraldb_connector.py`

- [ ] **Step 1: Add tests**

Append to `tests/test_databases/test_spiraldb_connector.py`:

```python
# ---------------------------------------------------------------------------
# create_table_if_not_exists
# ---------------------------------------------------------------------------


class TestCreateTableIfNotExists:
    def test_creates_table_with_pk_key_schema(self, connector, mock_project):
        columns = [
            ColumnInfo("id", pa.string(), nullable=False),
            ColumnInfo("value", pa.float64(), nullable=True),
        ]
        connector.create_table_if_not_exists("my_table", columns, pk_column="id")
        mock_project.create_table.assert_called_once_with(
            "default.my_table",
            key_schema=[("id", pa.string())],
            exist_ok=True,
        )

    def test_exist_ok_always_true(self, connector, mock_project):
        columns = [ColumnInfo("id", pa.int64(), nullable=False)]
        connector.create_table_if_not_exists("t", columns, pk_column="id")
        connector.create_table_if_not_exists("t", columns, pk_column="id")
        assert mock_project.create_table.call_count == 2
        for call_args in mock_project.create_table.call_args_list:
            assert call_args.kwargs.get("exist_ok") is True

    def test_raises_if_pk_column_not_in_columns(self, connector, mock_project):
        columns = [ColumnInfo("id", pa.string(), nullable=False)]
        with pytest.raises(ValueError, match="pk_column"):
            connector.create_table_if_not_exists("t", columns, pk_column="missing_col")

    def test_raises_if_columns_empty(self, connector, mock_project):
        with pytest.raises(ValueError, match="pk_column"):
            connector.create_table_if_not_exists("t", columns=[], pk_column="id")

    def test_non_pk_columns_not_included_in_key_schema(self, connector, mock_project):
        columns = [
            ColumnInfo("id", pa.string(), nullable=False),
            ColumnInfo("value", pa.float64()),
            ColumnInfo("label", pa.string()),
        ]
        connector.create_table_if_not_exists("t", columns, pk_column="id")
        call_kwargs = mock_project.create_table.call_args.kwargs
        assert call_kwargs["key_schema"] == [("id", pa.string())]
```

- [ ] **Step 2: Run — expect FAIL**

```bash
uv run pytest tests/test_databases/test_spiraldb_connector.py::TestCreateTableIfNotExists -v
```

- [ ] **Step 3: Implement `create_table_if_not_exists` in `spiraldb_connector.py`**

Replace the stub:

```python
def create_table_if_not_exists(
    self,
    table_name: str,
    columns: list[ColumnInfo],
    pk_column: str,
) -> None:
    """Create a SpiralDB table with a single-column key schema, idempotently.

    Only the primary-key column's type is registered with Spiral at creation
    time (via ``key_schema``). Non-PK columns in ``columns`` are ignored —
    Spiral infers value column schemas from the first write. A second call
    with the same table name is a no-op (``exist_ok=True``).

    Only single-column primary keys are supported via this method. Tables
    with composite primary keys must be created outside this connector but
    can be read and written through it.

    Args:
        table_name: Plain table name (no dataset prefix).
        columns: Full column list; must include ``pk_column``. Non-PK entries
            are accepted but not used at creation time.
        pk_column: Name of the single primary-key column.

    Raises:
        ValueError: If ``pk_column`` is not found in ``columns`` (including
            when ``columns`` is empty).
    """
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

- [ ] **Step 4: Run — expect PASS**

```bash
uv run pytest tests/test_databases/test_spiraldb_connector.py::TestCreateTableIfNotExists -v
```

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/databases/spiraldb_connector.py tests/test_databases/test_spiraldb_connector.py
git commit -m "feat(spiraldb): implement create_table_if_not_exists"
```

---

## Task 6: `upsert_records`

**Files:**
- Modify: `src/orcapod/databases/spiraldb_connector.py`
- Modify: `tests/test_databases/test_spiraldb_connector.py`

- [ ] **Step 1: Add tests**

Append to `tests/test_databases/test_spiraldb_connector.py`:

```python
# ---------------------------------------------------------------------------
# upsert_records
# ---------------------------------------------------------------------------


class TestUpsertRecords:
    def _make_mock_table(self, mock_project, pk_cols: list[str]) -> MagicMock:
        mock_table = MagicMock()
        mock_table.key_schema.names = pk_cols
        mock_project.table.return_value = mock_table
        return mock_table

    # --- skip_existing=False (default: always upsert-by-key) ----------------

    def test_write_called_with_full_records(self, connector, mock_project):
        mock_table = self._make_mock_table(mock_project, ["id"])
        records = pa.table({"id": ["a", "b"], "value": [1.0, 2.0]})
        connector.upsert_records("my_table", records, id_column="id")
        mock_table.write.assert_called_once_with(records)

    def test_raises_if_id_column_not_in_key_schema(self, connector, mock_project):
        self._make_mock_table(mock_project, ["id"])
        records = pa.table({"id": ["a"], "value": [1.0]})
        with pytest.raises(ValueError, match="id_column"):
            connector.upsert_records("my_table", records, id_column="wrong_col")

    def test_raises_if_pk_column_missing_from_records(self, connector, mock_project):
        self._make_mock_table(mock_project, ["id", "session_id"])
        records = pa.table({"id": ["a"]})  # missing session_id
        with pytest.raises(ValueError, match="missing key column"):
            connector.upsert_records("my_table", records, id_column="id")

    def test_raises_if_empty_key_schema(self, connector, mock_project):
        # id_column can never be `in []`, so this always raises ValueError
        self._make_mock_table(mock_project, [])
        records = pa.table({"id": ["a"]})
        with pytest.raises(ValueError, match="id_column"):
            connector.upsert_records("my_table", records, id_column="id")

    # --- skip_existing=True (scan + filter + write novel rows only) ----------

    def test_skip_existing_writes_only_novel_rows(self, connector, mock_sp, mock_project):
        mock_table = self._make_mock_table(mock_project, ["id"])
        existing = pa.table({"id": ["a"], "value": [1.0]})
        new_records = pa.table({"id": ["a", "b"], "value": [1.0, 2.0]})
        mock_sp.Spiral.return_value.scan.return_value.to_table.return_value = existing

        connector.upsert_records("my_table", new_records, id_column="id", skip_existing=True)

        written = mock_table.write.call_args[0][0]
        assert written.column("id").to_pylist() == ["b"]

    def test_skip_existing_noop_when_all_rows_exist(self, connector, mock_sp, mock_project):
        mock_table = self._make_mock_table(mock_project, ["id"])
        existing = pa.table({"id": ["a", "b"], "value": [1.0, 2.0]})
        new_records = pa.table({"id": ["a", "b"], "value": [10.0, 20.0]})
        mock_sp.Spiral.return_value.scan.return_value.to_table.return_value = existing

        connector.upsert_records("my_table", new_records, id_column="id", skip_existing=True)

        mock_table.write.assert_not_called()

    def test_skip_existing_composite_pk(self, connector, mock_sp, mock_project):
        mock_table = self._make_mock_table(mock_project, ["session_id", "ts"])
        existing = pa.table({
            "session_id": pa.array(["s1"]),
            "ts": pa.array([1], type=pa.int64()),
            "v": pa.array([10.0]),
        })
        new_records = pa.table({
            "session_id": pa.array(["s1", "s1"]),
            "ts": pa.array([1, 2], type=pa.int64()),  # (s1,1) exists; (s1,2) is novel
            "v": pa.array([99.0, 5.0]),
        })
        mock_sp.Spiral.return_value.scan.return_value.to_table.return_value = existing

        connector.upsert_records("my_table", new_records, id_column="session_id", skip_existing=True)

        written = mock_table.write.call_args[0][0]
        assert written.column("ts").to_pylist() == [2]

    def test_skip_existing_raises_if_id_column_not_in_key_schema(self, connector, mock_project):
        self._make_mock_table(mock_project, ["id"])
        records = pa.table({"id": ["a"]})
        with pytest.raises(ValueError, match="id_column"):
            connector.upsert_records("t", records, id_column="wrong", skip_existing=True)

    def test_skip_existing_raises_if_pk_missing_from_records(self, connector, mock_project):
        self._make_mock_table(mock_project, ["id", "ts"])
        records = pa.table({"id": ["a"]})  # missing ts
        with pytest.raises(ValueError, match="missing key column"):
            connector.upsert_records("t", records, id_column="id", skip_existing=True)
```

- [ ] **Step 2: Run — expect FAIL**

```bash
uv run pytest tests/test_databases/test_spiraldb_connector.py::TestUpsertRecords -v
```

- [ ] **Step 3: Implement `upsert_records` in `spiraldb_connector.py`**

Replace the stub:

```python
def upsert_records(
    self,
    table_name: str,
    records: pa.Table,
    id_column: str,
    skip_existing: bool = False,
) -> None:
    """Write records to a SpiralDB table.

    SpiralDB uses the table's key schema for conflict detection. ``id_column``
    is accepted for protocol compliance; it must appear in the table's key
    schema (``tbl.key_schema.names``), but SpiralDB always uses the full
    composite key schema — not just ``id_column`` — for conflict resolution.

    When ``skip_existing=False`` (default), all rows are written and existing
    rows with matching keys are overwritten (upsert-by-key). When
    ``skip_existing=True``, the entire table is scanned first and only rows
    whose composite key is absent from the existing table are written. This
    is O(n) in table size.

    Args:
        table_name: Plain table name (no dataset prefix).
        records: Arrow table to write.
        id_column: Must be one of the table's key-schema columns. Validated
            for correctness; the full composite key schema is used for
            conflict detection.
        skip_existing: If False (default), overwrite on key conflict. If
            True, skip rows with already-existing composite keys (requires
            full table scan).

    Raises:
        ValueError: If ``id_column`` is not in the table key schema (including
            empty key schema), or if any key-schema column is absent from
            ``records``.
    """
    self._require_open()
    tbl = self._project.table(self._table_id(table_name))
    pk_cols = list(tbl.key_schema.names)

    # Guard 1: id_column must appear in the table's key schema.
    # Also handles empty-key-schema tables: nothing is ever `in []`.
    if id_column not in pk_cols:
        raise ValueError(
            f"id_column {id_column!r} is not in the table key schema {pk_cols}. "
            "SpiralDB uses the table key schema for conflict detection."
        )

    # Guard 2: all PK columns must be present in the records table.
    missing = [k for k in pk_cols if k not in records.schema.names]
    if missing:
        raise ValueError(
            f"records is missing key column(s) {missing} required by the "
            f"table key schema {pk_cols}."
        )

    if not skip_existing:
        # Always upsert-by-key: existing rows overwritten, novel rows inserted.
        tbl.write(records)
        return

    # skip_existing=True: full scan → client-side key filter → write novel rows.
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

- [ ] **Step 4: Run — expect PASS**

```bash
uv run pytest tests/test_databases/test_spiraldb_connector.py::TestUpsertRecords -v
```

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/databases/spiraldb_connector.py tests/test_databases/test_spiraldb_connector.py
git commit -m "feat(spiraldb): implement upsert_records"
```

---

## Task 7: `to_config` / `from_config`

**Files:**
- Modify: `src/orcapod/databases/spiraldb_connector.py`
- Modify: `tests/test_databases/test_spiraldb_connector.py`

- [ ] **Step 1: Add tests**

Append to `tests/test_databases/test_spiraldb_connector.py`:

```python
# ---------------------------------------------------------------------------
# to_config / from_config
# ---------------------------------------------------------------------------


class TestConfigSerialization:
    def test_to_config_round_trip(self, mock_sp, mock_project):
        c = SpiralDBConnector(
            project_id="my-project-123",
            dataset="prod",
            overrides={"server.url": "http://api.spiraldb.dev"},
        )
        cfg = c.to_config()
        assert cfg == {
            "connector_type": "spiraldb",
            "project_id": "my-project-123",
            "dataset": "prod",
            "overrides": {"server.url": "http://api.spiraldb.dev"},
        }

    def test_to_config_none_overrides(self, connector):
        cfg = connector.to_config()
        assert cfg["overrides"] is None

    def test_from_config_constructs_connector(self, mock_sp, mock_project):
        cfg = {
            "connector_type": "spiraldb",
            "project_id": "my-project-123",
            "dataset": "prod",
            "overrides": None,
        }
        c = SpiralDBConnector.from_config(cfg)
        assert c._project_id == "my-project-123"
        assert c._dataset == "prod"
        assert c._overrides is None

    def test_from_config_default_dataset_when_absent(self, mock_sp, mock_project):
        cfg = {"connector_type": "spiraldb", "project_id": "p"}
        c = SpiralDBConnector.from_config(cfg)
        assert c._dataset == "default"

    def test_from_config_raises_on_wrong_connector_type(self):
        with pytest.raises(ValueError, match="connector_type"):
            SpiralDBConnector.from_config(
                {"connector_type": "sqlite", "project_id": "p"}
            )

    def test_to_config_raises_after_close(self, connector):
        connector.close()
        with pytest.raises(RuntimeError, match="closed"):
            connector.to_config()
```

- [ ] **Step 2: Run — expect FAIL**

```bash
uv run pytest tests/test_databases/test_spiraldb_connector.py::TestConfigSerialization -v
```

- [ ] **Step 3: Implement `to_config` / `from_config` in `spiraldb_connector.py`**

Replace the stubs:

```python
def to_config(self) -> dict[str, Any]:
    """Serialize connection configuration to a JSON-compatible dict.

    Returns:
        Dict with ``connector_type``, ``project_id``, ``dataset``, and
        ``overrides`` keys.

    Raises:
        RuntimeError: If this connector has been closed.
    """
    self._require_open()
    return {
        "connector_type": "spiraldb",
        "project_id": self._project_id,
        "dataset": self._dataset,
        "overrides": self._overrides,
    }

@classmethod
def from_config(cls, config: dict[str, Any]) -> SpiralDBConnector:
    """Reconstruct a SpiralDBConnector from a config dict.

    Args:
        config: Dict with ``connector_type``, ``project_id``, and optionally
            ``dataset`` and ``overrides`` keys.

    Returns:
        A new, open SpiralDBConnector instance.

    Raises:
        ValueError: If ``connector_type`` is not ``"spiraldb"``.
    """
    if config.get("connector_type") != "spiraldb":
        raise ValueError(
            f"Expected connector_type 'spiraldb', "
            f"got {config.get('connector_type')!r}"
        )
    return cls(
        project_id=config["project_id"],
        dataset=config.get("dataset", "default"),
        overrides=config.get("overrides"),
    )
```

- [ ] **Step 4: Run — expect PASS**

```bash
uv run pytest tests/test_databases/test_spiraldb_connector.py::TestConfigSerialization -v
```

- [ ] **Step 5: Run the full unit test suite — everything should pass**

```bash
uv run pytest tests/test_databases/test_spiraldb_connector.py -v
```

Expected: all tests PASS

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/databases/spiraldb_connector.py tests/test_databases/test_spiraldb_connector.py
git commit -m "feat(spiraldb): implement to_config/from_config"
```

---

## Task 8: Export from `__init__.py`

**Files:**
- Modify: `src/orcapod/databases/__init__.py`

- [ ] **Step 1: Update `__init__.py`**

The current file has a placeholder comment `#   SpiralDBConnector    -- PLT-1074`. Replace the entire file with:

```python
from .connector_arrow_database import ConnectorArrowDatabase
from .delta_lake_databases import DeltaTableDatabase
from .in_memory_databases import InMemoryArrowDatabase
from .noop_database import NoOpArrowDatabase
from .spiraldb_connector import SpiralDBConnector
from .sqlite_connector import SQLiteConnector

__all__ = [
    "ConnectorArrowDatabase",
    "DeltaTableDatabase",
    "InMemoryArrowDatabase",
    "NoOpArrowDatabase",
    "SpiralDBConnector",
    "SQLiteConnector",
]

# Relational DB connector implementations satisfy DBConnectorProtocol
# (orcapod.protocols.db_connector_protocol) and can be passed to either
# ConnectorArrowDatabase (read+write ArrowDatabaseProtocol) or
# DBTableSource (read-only Source):
#
#   SQLiteConnector      -- PLT-1076 (stdlib sqlite3, zero extra deps)
#   PostgreSQLConnector  -- PLT-1075 (psycopg3)
#   SpiralDBConnector    -- PLT-1074
#
# ArrowDatabaseProtocol backends (existing, not connector-based):
#
#   DeltaTableDatabase    -- Delta Lake (deltalake package)
#   InMemoryArrowDatabase -- pure in-memory, for tests
#   NoOpArrowDatabase     -- no-op, for dry-runs / benchmarks
```

- [ ] **Step 2: Verify import works**

```bash
uv run python -c "from orcapod.databases import SpiralDBConnector; print('import OK')"
```

Expected: `import OK`

- [ ] **Step 3: Run the full `test_databases/` suite to confirm no regressions**

```bash
uv run pytest tests/test_databases/ -v
```

Expected: all pass

- [ ] **Step 4: Commit**

```bash
git add src/orcapod/databases/__init__.py
git commit -m "feat(spiraldb): export SpiralDBConnector from databases __init__"
```

---

## Task 9: Integration tests

**Files:**
- Create: `tests/test_databases/test_spiraldb_connector_integration.py`

Integration tests are **skipped by default** to avoid requiring live credentials in CI.

To run them:
1. Authenticate: `uv run pyspiral login` (device-code OAuth; stores token in `~/.config/pyspiral/auth.json`)
2. Run: `SPIRAL_INTEGRATION_TESTS=1 uv run pytest tests/test_databases/test_spiraldb_connector_integration.py -v`
3. Uses dev project `test-orcapod-362211` (hits `api.spiraldb.dev`)

Each test uses a `uuid`-suffixed table name to avoid cross-test interference.

- [ ] **Step 1: Create the integration test file**

Create `tests/test_databases/test_spiraldb_connector_integration.py`:

```python
"""Integration tests for SpiralDBConnector against the live dev project.

These tests are skipped unless ``SPIRAL_INTEGRATION_TESTS=1`` env var is set
AND valid credentials are present in ``~/.config/pyspiral/auth.json``
(obtained via ``spiral login``). If the env var is set but credentials are
absent, the tests fail rather than skip — the operator is expected to ensure
auth is in place when enabling integration tests.

Dev project: ``test-orcapod-362211``  (api.spiraldb.dev)
"""
from __future__ import annotations

import os
import uuid

import pyarrow as pa
import pytest

from orcapod.databases import ConnectorArrowDatabase, SpiralDBConnector
from orcapod.types import ColumnInfo

INTEGRATION = os.environ.get("SPIRAL_INTEGRATION_TESTS") == "1"
DEV_PROJECT = "test-orcapod-362211"
DEV_OVERRIDES = {"server.url": "http://api.spiraldb.dev"}

pytestmark = pytest.mark.skipif(
    not INTEGRATION,
    reason="Set SPIRAL_INTEGRATION_TESTS=1 to run SpiralDB integration tests",
)


def _unique_table(prefix: str = "test") -> str:
    """Return a unique table name to avoid cross-test interference."""
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


@pytest.fixture()
def connector():
    c = SpiralDBConnector(
        project_id=DEV_PROJECT,
        dataset="default",
        overrides=DEV_OVERRIDES,
    )
    yield c
    c.close()


class TestSpiralDBConnectorIntegration:
    def test_full_round_trip(self, connector):
        """Create table → write records → scan → verify values."""
        table_name = _unique_table("roundtrip")
        connector.create_table_if_not_exists(
            table_name,
            columns=[
                ColumnInfo("__record_id", pa.string(), nullable=False),
                ColumnInfo("value", pa.float64()),
            ],
            pk_column="__record_id",
        )
        records = pa.table({
            "__record_id": pa.array(["r1", "r2"], type=pa.string()),
            "value": pa.array([10.0, 20.0], type=pa.float64()),
        })
        connector.upsert_records(table_name, records, id_column="__record_id")

        result = pa.Table.from_batches(
            list(connector.iter_batches(f'SELECT * FROM "{table_name}"'))
        )
        assert result.num_rows == 2
        assert sorted(result.column("__record_id").to_pylist()) == ["r1", "r2"]

    def test_skip_existing_true(self, connector):
        """Write once, write again with overlapping keys — verify no duplication."""
        table_name = _unique_table("skip")
        connector.create_table_if_not_exists(
            table_name,
            columns=[
                ColumnInfo("id", pa.string(), nullable=False),
                ColumnInfo("val", pa.int64()),
            ],
            pk_column="id",
        )
        first_write = pa.table({"id": ["a", "b"], "val": pa.array([1, 2], type=pa.int64())})
        connector.upsert_records(table_name, first_write, id_column="id")

        second_write = pa.table({"id": ["a", "c"], "val": pa.array([99, 3], type=pa.int64())})
        connector.upsert_records(
            table_name, second_write, id_column="id", skip_existing=True
        )

        result = pa.Table.from_batches(
            list(connector.iter_batches(f'SELECT * FROM "{table_name}"'))
        )
        ids = sorted(result.column("id").to_pylist())
        assert ids == ["a", "b", "c"]

        # "a" must not have been overwritten (skip_existing=True)
        a_rows = result.filter(
            pa.chunked_array([result.column("id")]).combine_chunks()
            == pa.scalar("a")
        )
        assert a_rows.column("val")[0].as_py() == 1

    def test_connector_arrow_database_round_trip(self, connector):
        """ConnectorArrowDatabase: add_record → flush → get_record_by_id."""
        db = ConnectorArrowDatabase(connector)
        record = pa.table({"x": pa.array([42], type=pa.int64())})
        db.add_record(
            ("spiraldb", "integration"),
            record_id="test_r1",
            record=record,
            flush=True,
        )
        result = db.get_record_by_id(("spiraldb", "integration"), "test_r1")
        assert result is not None
        assert result.column("x")[0].as_py() == 42
```

- [ ] **Step 2: Verify tests are skipped by default**

```bash
uv run pytest tests/test_databases/test_spiraldb_connector_integration.py -v
```

Expected: 3 tests SKIPPED with `"Set SPIRAL_INTEGRATION_TESTS=1 to run SpiralDB integration tests"`

- [ ] **Step 3: Commit**

```bash
git add tests/test_databases/test_spiraldb_connector_integration.py
git commit -m "feat(spiraldb): add integration tests (skipped without SPIRAL_INTEGRATION_TESTS=1)"
```

---

## Final verification

- [ ] **Run the full unit test suite**

```bash
uv run pytest tests/test_databases/test_spiraldb_connector.py tests/test_databases/ -v
```

Expected: all pass (including SQLite tests — no regressions)

- [ ] **Verify protocol conformance**

```bash
uv run python -c "
from unittest.mock import MagicMock, patch
with patch('orcapod.databases.spiraldb_connector.sp') as mock:
    mock.Spiral.return_value.project.return_value = MagicMock()
    from orcapod.databases import SpiralDBConnector
    from orcapod.protocols.db_connector_protocol import DBConnectorProtocol
    c = SpiralDBConnector('test-project')
    print('isinstance:', isinstance(c, DBConnectorProtocol))
"
```

Expected: `isinstance: True`

- [ ] **Force-add plan to git (it's in gitignored `superpowers/`)**

```bash
git add -f superpowers/plans/2026-03-25-spiraldb-connector.md
git commit -m "docs: add SpiralDBConnector implementation plan"
```
