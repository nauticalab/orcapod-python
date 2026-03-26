# SQLiteTableSource Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement `SQLiteTableSource`, a `RootSource` backed by a SQLite table that uses primary-key columns (or `rowid` for ROWID-only tables) as default tag columns, with a working `from_config` round-trip.

**Architecture:** `SQLiteTableSource` subclasses `DBTableSource`. Two small patches are made to existing files first (`SQLiteConnector.iter_batches` for `rowid` typing; `DBTableSource.__init__` for a `_query` hook), then the new class and its tests are added, followed by wiring into exports and the source registry.

**Tech Stack:** Python 3.x, stdlib `sqlite3`, `pyarrow`, `pytest`

---

## File Map

| Action | Path | Responsibility |
|---|---|---|
| Modify | `src/orcapod/databases/sqlite_connector.py` | Patch `iter_batches` to type `rowid` as `int64` |
| Modify | `src/orcapod/core/sources/db_table_source.py` | Add `*, _query` keyword-only parameter |
| **Create** | `src/orcapod/core/sources/sqlite_table_source.py` | `SQLiteTableSource` class |
| Modify | `src/orcapod/core/sources/__init__.py` | Export `SQLiteTableSource` |
| Modify | `src/orcapod/pipeline/serialization.py` | Register `"sqlite_table"` in source registry |
| Modify | `pytest.ini` | Register `integration` marker |
| Modify | `tests/test_databases/test_sqlite_connector.py` | Regression: `rowid` typed as `int64` |
| Modify | `tests/test_core/sources/test_db_table_source.py` | Regression: `_query` parameter works |
| **Create** | `tests/test_core/sources/test_sqlite_table_source.py` | Full unit + integration tests |

---

## Task 1: Patch `SQLiteConnector.iter_batches` — type `rowid` as `int64`

**Background:** When `SELECT rowid, * FROM "t"` is executed, `rowid` is not in `PRAGMA table_info`, so `iter_batches` falls back to `pa.large_string()`. We need it to be `pa.int64()`.

**Files:**
- Modify: `src/orcapod/databases/sqlite_connector.py` (lines 268–270)
- Test: `tests/test_databases/test_sqlite_connector.py`

- [ ] **Step 1.1: Write the failing regression test**

Open `tests/test_databases/test_sqlite_connector.py` and add this test class at the end of the file:

```python
class TestRowidTyping:
    def test_rowid_column_typed_as_int64(self, connector):
        """SELECT rowid, * should yield rowid as int64, not large_string."""
        conn = connector._conn
        conn.execute("CREATE TABLE nokey (val TEXT)")
        conn.execute("INSERT INTO nokey VALUES ('hello')")

        batches = list(connector.iter_batches('SELECT rowid, * FROM "nokey"'))
        assert len(batches) == 1
        batch = batches[0]
        assert "rowid" in batch.schema.names
        assert batch.schema.field("rowid").type == pa.int64()
        assert batch.column("rowid")[0].as_py() == 1  # first rowid is always 1
```

- [ ] **Step 1.2: Run to confirm it fails**

```bash
cd /path/to/orcapod-python
uv run pytest tests/test_databases/test_sqlite_connector.py::TestRowidTyping::test_rowid_column_typed_as_int64 -v
```

Expected: FAIL — `AssertionError` because `rowid` is typed as `large_string`.

- [ ] **Step 1.3: Apply the patch**

In `src/orcapod/databases/sqlite_connector.py`, find the block around line 268 that reads:

```python
            if table_match:
                queried_table = table_match.group(1)
                for ci in self.get_column_info(queried_table):
                    type_lookup[ci.name] = ci.arrow_type

            arrow_types = [type_lookup.get(name, _pa.large_string()) for name in col_names]
```

Replace it with:

```python
            if table_match:
                queried_table = table_match.group(1)
                for ci in self.get_column_info(queried_table):
                    type_lookup[ci.name] = ci.arrow_type

            # rowid is an implicit SQLite integer column; it does not appear in
            # PRAGMA table_info, so it would otherwise fall back to large_string.
            if "rowid" in col_names:
                type_lookup["rowid"] = _pa.int64()

            arrow_types = [type_lookup.get(name, _pa.large_string()) for name in col_names]
```

- [ ] **Step 1.4: Run the test to confirm it passes**

```bash
uv run pytest tests/test_databases/test_sqlite_connector.py::TestRowidTyping::test_rowid_column_typed_as_int64 -v
```

Expected: PASS.

- [ ] **Step 1.5: Run the full SQLiteConnector test suite to confirm no regressions**

```bash
uv run pytest tests/test_databases/test_sqlite_connector.py -v
```

Expected: all tests PASS.

- [ ] **Step 1.6: Commit**

```bash
git add src/orcapod/databases/sqlite_connector.py tests/test_databases/test_sqlite_connector.py
git commit -m "fix(databases): type rowid column as int64 in SQLiteConnector.iter_batches"
```

---

## Task 2: Patch `DBTableSource.__init__` — add `_query` keyword-only parameter

**Background:** `DBTableSource` hardcodes `SELECT * FROM "table"`. Adding a `_query` parameter lets `SQLiteTableSource` inject `SELECT rowid, * FROM "table"` for ROWID-only tables.

**Files:**
- Modify: `src/orcapod/core/sources/db_table_source.py` (lines 61–72 signature, line 103 body)
- Test: `tests/test_core/sources/test_db_table_source.py`

- [ ] **Step 2.1: Write the failing regression test**

Open `tests/test_core/sources/test_db_table_source.py` and add this test at the end of the `TestConfig` class (or as a standalone function after all existing tests):

```python
class TestQueryOverride:
    def test_custom_query_overrides_default_select(self, measurements_table):
        """Passing _query to DBTableSource uses that query instead of SELECT *."""
        # Build a connector where iter_batches respects custom queries
        import re

        class CustomQueryConnector(MockDBConnector):
            def iter_batches(self, query, params=None, batch_size=1000):
                # Return only rows where trial > 1 when a custom query is given
                match = re.search(r"trial > 1", query)
                if match:
                    filtered = measurements_table.filter(
                        measurements_table.column("trial") > pa.scalar(1, pa.int64())
                    )
                    for batch in filtered.to_batches():
                        yield batch
                else:
                    yield from super().iter_batches(query, params, batch_size)

        connector = CustomQueryConnector(
            tables={"measurements": measurements_table},
            pk_columns={"measurements": ["session_id"]},
        )
        src = DBTableSource(
            connector,
            "measurements",
            _query='SELECT * FROM "measurements" WHERE trial > 1',
        )
        assert src.as_table().num_rows == 2  # only s2, s3 have trial > 1
```

- [ ] **Step 2.2: Run to confirm it fails**

```bash
uv run pytest tests/test_core/sources/test_db_table_source.py::TestQueryOverride::test_custom_query_overrides_default_select -v
```

Expected: FAIL — `TypeError: __init__() got an unexpected keyword argument '_query'`.

- [ ] **Step 2.3: Apply the patch to `DBTableSource.__init__`**

In `src/orcapod/core/sources/db_table_source.py`, update the `__init__` signature from:

```python
    def __init__(
        self,
        connector: DBConnectorProtocol,
        table_name: str,
        tag_columns: Collection[str] | None = None,
        system_tag_columns: Collection[str] = (),
        record_id_column: str | None = None,
        source_id: str | None = None,
        label: str | None = None,
        data_context: str | contexts.DataContext | None = None,
        config: Config | None = None,
    ) -> None:
```

to:

```python
    def __init__(
        self,
        connector: DBConnectorProtocol,
        table_name: str,
        tag_columns: Collection[str] | None = None,
        system_tag_columns: Collection[str] = (),
        record_id_column: str | None = None,
        source_id: str | None = None,
        label: str | None = None,
        data_context: str | contexts.DataContext | None = None,
        config: Config | None = None,
        *,
        _query: str | None = None,
    ) -> None:
```

Then replace line 103:

```python
        # Step 3: Fetch the full table as Arrow
        batches = list(connector.iter_batches(f'SELECT * FROM "{table_name}"'))
```

with:

```python
        # Step 3: Fetch the full table as Arrow.
        # _query allows subclasses (e.g. SQLiteTableSource) to inject a custom
        # SELECT (e.g. SELECT rowid, * FROM "t") without duplicating the rest of init.
        query = _query if _query is not None else f'SELECT * FROM "{table_name}"'
        batches = list(connector.iter_batches(query))
```

- [ ] **Step 2.4: Run the regression test to confirm it passes**

```bash
uv run pytest tests/test_core/sources/test_db_table_source.py::TestQueryOverride::test_custom_query_overrides_default_select -v
```

Expected: PASS.

- [ ] **Step 2.5: Run the full DBTableSource test suite**

```bash
uv run pytest tests/test_core/sources/test_db_table_source.py -v
```

Expected: all tests PASS.

- [ ] **Step 2.6: Commit**

```bash
git add src/orcapod/core/sources/db_table_source.py tests/test_core/sources/test_db_table_source.py
git commit -m "feat(sources): add keyword-only _query parameter to DBTableSource"
```

---

## Task 3: Implement `SQLiteTableSource` — core class + unit tests (PK, explicit tags, errors, stream, hashing)

**Files:**
- Create: `src/orcapod/core/sources/sqlite_table_source.py`
- Create: `tests/test_core/sources/test_sqlite_table_source.py`

- [ ] **Step 3.1: Create the test file with fixtures and the first failing test group**

Create `tests/test_core/sources/test_sqlite_table_source.py`:

```python
"""Tests for SQLiteTableSource.

Test sections:
 1. Import / export sanity
 2. Protocol conformance
 3. PK as default tag columns (single and composite)
 4. Explicit tag column override
 5. ROWID fallback (no explicit PK)
 6. Error cases (missing table, empty table)
 7. Stream behaviour
 8. Deterministic hashing
 9. Config round-trip — PK table (file-backed db)
10. Config round-trip — ROWID-only table (file-backed db)
11. Integration: SQLiteTableSource in a pipeline
"""
from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Iterator

import pyarrow as pa
import pytest

from orcapod.databases.sqlite_connector import SQLiteConnector


# ---------------------------------------------------------------------------
# Helpers: create in-memory SQLite tables via raw sqlite3
# ---------------------------------------------------------------------------


def _create_table_with_pk(conn: sqlite3.Connection) -> None:
    """Create 'measurements' with single-column PK and insert 3 rows."""
    conn.execute(
        "CREATE TABLE measurements (session_id TEXT PRIMARY KEY, trial INTEGER, response REAL)"
    )
    conn.executemany(
        "INSERT INTO measurements VALUES (?, ?, ?)",
        [("s1", 1, 0.1), ("s2", 2, 0.2), ("s3", 3, 0.3)],
    )


def _create_table_with_composite_pk(conn: sqlite3.Connection) -> None:
    """Create 'events' with a composite PK and insert 2 rows."""
    conn.execute(
        "CREATE TABLE events (user_id TEXT, event_id INTEGER, payload TEXT, "
        "PRIMARY KEY (user_id, event_id))"
    )
    conn.executemany(
        "INSERT INTO events VALUES (?, ?, ?)",
        [("u1", 1, "click"), ("u1", 2, "scroll")],
    )


def _create_table_without_pk(conn: sqlite3.Connection) -> None:
    """Create 'logs' with no explicit PK (ROWID-only) and insert 3 rows."""
    conn.execute("CREATE TABLE logs (message TEXT, level TEXT)")
    conn.executemany(
        "INSERT INTO logs VALUES (?, ?)",
        [("boot", "INFO"), ("error occurred", "ERROR"), ("shutdown", "INFO")],
    )


def _create_empty_table(conn: sqlite3.Connection) -> None:
    """Create 'empty_tbl' with a PK but no rows."""
    conn.execute("CREATE TABLE empty_tbl (id TEXT PRIMARY KEY, val INTEGER)")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def memory_connector() -> Iterator[SQLiteConnector]:
    c = SQLiteConnector(":memory:")
    yield c
    c.close()


@pytest.fixture
def pk_connector(memory_connector: SQLiteConnector) -> SQLiteConnector:
    _create_table_with_pk(memory_connector._conn)
    return memory_connector


@pytest.fixture
def composite_pk_connector(memory_connector: SQLiteConnector) -> SQLiteConnector:
    _create_table_with_composite_pk(memory_connector._conn)
    return memory_connector


@pytest.fixture
def rowid_connector(memory_connector: SQLiteConnector) -> SQLiteConnector:
    _create_table_without_pk(memory_connector._conn)
    return memory_connector


@pytest.fixture
def empty_connector(memory_connector: SQLiteConnector) -> SQLiteConnector:
    _create_empty_table(memory_connector._conn)
    return memory_connector


# ===========================================================================
# 1. Import / export sanity
# ===========================================================================


def test_import_from_core_sources():
    from orcapod.core.sources import SQLiteTableSource
    assert SQLiteTableSource is not None


def test_import_from_orcapod_sources():
    from orcapod.sources import SQLiteTableSource
    assert SQLiteTableSource is not None


def test_in_core_sources_all():
    import orcapod.core.sources as m
    assert "SQLiteTableSource" in m.__all__


# ===========================================================================
# 2. Protocol conformance
# ===========================================================================


class TestProtocolConformance:
    def test_is_source_protocol(self, pk_connector):
        from orcapod.core.sources import SQLiteTableSource
        from orcapod.protocols.core_protocols import SourceProtocol
        src = SQLiteTableSource(pk_connector._db_path, "measurements")
        assert isinstance(src, SourceProtocol)

    def test_is_stream_protocol(self, pk_connector):
        from orcapod.core.sources import SQLiteTableSource
        from orcapod.protocols.core_protocols import StreamProtocol
        src = SQLiteTableSource(pk_connector._db_path, "measurements")
        assert isinstance(src, StreamProtocol)

    def test_is_pipeline_element_protocol(self, pk_connector):
        from orcapod.core.sources import SQLiteTableSource
        from orcapod.protocols.hashing_protocols import PipelineElementProtocol
        src = SQLiteTableSource(pk_connector._db_path, "measurements")
        assert isinstance(src, PipelineElementProtocol)
```

- [ ] **Step 3.2: Run the import tests to confirm they fail**

```bash
uv run pytest tests/test_core/sources/test_sqlite_table_source.py::test_import_from_core_sources -v
```

Expected: FAIL — `ImportError: cannot import name 'SQLiteTableSource'`.

- [ ] **Step 3.3: Create the `SQLiteTableSource` implementation**

Create `src/orcapod/core/sources/sqlite_table_source.py`:

```python
"""SQLiteTableSource — a read-only RootSource backed by a SQLite table.

Wraps a SQLite table as an OrcaPod Source. Primary-key columns are used
as tag columns by default. For tables with no explicit primary key
(ROWID-only tables), the implicit ``rowid`` integer column is used
automatically.

Example::

    # File-backed (supports from_config round-trip)
    source = SQLiteTableSource("/path/to/my.db", "measurements")

    # In-memory (for tests / throwaway pipelines; cannot round-trip)
    source = SQLiteTableSource(":memory:", "events", tag_columns=["session_id"])

Note:
    ``:memory:`` sources cannot be reconstructed via ``from_config`` because
    each new ``SQLiteConnector(":memory:")`` opens a fresh empty database.
    File-backed sources (including ROWID-only tables) round-trip correctly.
"""
from __future__ import annotations

import os
from collections.abc import Collection
from typing import TYPE_CHECKING, Any

from orcapod.core.sources.db_table_source import DBTableSource
from orcapod.databases.sqlite_connector import SQLiteConnector
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    from orcapod import contexts
    from orcapod.config import Config


class SQLiteTableSource(DBTableSource):
    """A read-only Source backed by a table in a SQLite database.

    At construction time the source:
    1. Opens a ``SQLiteConnector`` for *db_path*.
    2. Validates the table exists.
    3. Resolves tag columns:
       - If *tag_columns* is provided, uses them as-is.
       - Otherwise uses the table's primary-key columns.
       - If the table has no explicit PK (ROWID-only), falls back to the
         implicit ``rowid`` integer column.
    4. Determines the fetch query: injects ``SELECT rowid, *`` when
       ``"rowid"`` is a resolved tag column and not a normal table column
       (handles both auto-detection and ``from_config`` reconstruction).
    5. Delegates to ``DBTableSource.__init__`` for fetching and stream building.

    Args:
        db_path: Path to the SQLite database file, or ``":memory:"`` for an
            in-process in-memory database.
        table_name: Name of the table to expose as a source.
        tag_columns: Columns to use as tag columns. If ``None`` (default),
            the table's primary-key columns are used; ROWID-only tables fall
            back to ``["rowid"]``.
        system_tag_columns: Additional system-level tag columns.
        record_id_column: Column for stable per-row record IDs in provenance.
        source_id: Canonical source name. Defaults to *table_name*.
        label: Human-readable label for this source node.
        data_context: Data context governing type conversion and hashing.
        config: OrcaPod configuration.

    Raises:
        ValueError: If the table is not found or is empty.
        sqlite3.OperationalError: If *db_path* cannot be opened.
    """

    def __init__(
        self,
        db_path: str | os.PathLike,
        table_name: str,
        tag_columns: Collection[str] | None = None,
        system_tag_columns: Collection[str] = (),
        record_id_column: str | None = None,
        source_id: str | None = None,
        label: str | None = None,
        data_context: str | contexts.DataContext | None = None,
        config: Config | None = None,
    ) -> None:
        self._db_path = db_path
        connector = SQLiteConnector(db_path)

        # Step 3: Resolve tag columns.
        if tag_columns is None:
            pk_cols = connector.get_pk_columns(table_name)
            resolved_tags: list[str] = pk_cols if pk_cols else ["rowid"]
        else:
            resolved_tags = list(tag_columns)

        # Step 4: Determine the fetch query.
        # If "rowid" is in resolved_tags but not a real column, we need
        # SELECT rowid, * to include it.  This also handles from_config
        # reconstruction where tag_columns=["rowid"] is passed explicitly.
        normal_cols = {ci.name for ci in connector.get_column_info(table_name)}
        if "rowid" in resolved_tags and "rowid" not in normal_cols:
            _query: str | None = f'SELECT rowid, * FROM "{table_name}"'
        else:
            _query = None

        super().__init__(
            connector,
            table_name,
            tag_columns=resolved_tags,
            system_tag_columns=system_tag_columns,
            record_id_column=record_id_column,
            source_id=source_id,
            label=label,
            data_context=data_context,
            config=config,
            _query=_query,
        )

    def to_config(self) -> dict[str, Any]:
        """Serialize source configuration to a JSON-compatible dict."""
        base = super().to_config()
        # Override source_type and replace connector dict with db_path.
        return {
            **base,
            "source_type": "sqlite_table",
            "db_path": str(self._db_path),
        }

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> "SQLiteTableSource":
        """Reconstruct a SQLiteTableSource from a config dict.

        Args:
            config: Dict as produced by ``to_config()``.

        Returns:
            A new ``SQLiteTableSource`` instance.

        Note:
            ``:memory:`` sources cannot be reconstructed — the new in-memory
            database will be empty and ``ValueError`` will be raised.
        """
        return cls(
            db_path=config["db_path"],
            table_name=config["table_name"],
            tag_columns=config.get("tag_columns"),
            system_tag_columns=config.get("system_tag_columns", ()),
            record_id_column=config.get("record_id_column"),
            source_id=config.get("source_id"),
        )
```

- [ ] **Step 3.4: Run the protocol conformance tests**

At this stage `from orcapod.core.sources import SQLiteTableSource` (used in `TestProtocolConformance`) will fail because `__init__.py` has not been updated yet. That happens in Task 4. However, the class itself is already importable via its direct path. Temporarily update the three imports inside `TestProtocolConformance` to use the direct path for this verification step:

```python
from orcapod.core.sources.sqlite_table_source import SQLiteTableSource
```

Then run:

```bash
uv run pytest tests/test_core/sources/test_sqlite_table_source.py::TestProtocolConformance -v
```

Expected: PASS. Revert the temporary import change (back to `from orcapod.core.sources import SQLiteTableSource`) before committing — Task 4 will wire the `__init__.py` export and make the standard import work.

- [ ] **Step 3.5: Add PK, explicit tag, error-case, stream, and hashing test groups to the test file**

Append to `tests/test_core/sources/test_sqlite_table_source.py`:

```python
# ===========================================================================
# 3. PK as default tag columns
# ===========================================================================


class TestPKAsDefaultTags:
    def test_single_pk_is_tag_column(self, pk_connector):
        from orcapod.core.sources import SQLiteTableSource
        src = SQLiteTableSource(pk_connector._db_path, "measurements")
        tag_schema, _ = src.output_schema()
        assert "session_id" in tag_schema

    def test_pk_not_in_packet_schema(self, pk_connector):
        from orcapod.core.sources import SQLiteTableSource
        src = SQLiteTableSource(pk_connector._db_path, "measurements")
        _, packet_schema = src.output_schema()
        assert "session_id" not in packet_schema

    def test_non_pk_columns_in_packet_schema(self, pk_connector):
        from orcapod.core.sources import SQLiteTableSource
        src = SQLiteTableSource(pk_connector._db_path, "measurements")
        _, packet_schema = src.output_schema()
        assert "trial" in packet_schema
        assert "response" in packet_schema

    def test_composite_pk_all_columns_are_tags(self, composite_pk_connector):
        from orcapod.core.sources import SQLiteTableSource
        src = SQLiteTableSource(composite_pk_connector._db_path, "events")
        tag_schema, _ = src.output_schema()
        assert "user_id" in tag_schema
        assert "event_id" in tag_schema

    def test_default_source_id_is_table_name(self, pk_connector):
        from orcapod.core.sources import SQLiteTableSource
        src = SQLiteTableSource(pk_connector._db_path, "measurements")
        assert src.source_id == "measurements"

    def test_explicit_source_id_overrides_default(self, pk_connector):
        from orcapod.core.sources import SQLiteTableSource
        src = SQLiteTableSource(pk_connector._db_path, "measurements", source_id="meas")
        assert src.source_id == "meas"


# ===========================================================================
# 4. Explicit tag column override
# ===========================================================================


class TestExplicitTagOverride:
    def test_explicit_tag_columns_override_pk(self, pk_connector):
        from orcapod.core.sources import SQLiteTableSource
        src = SQLiteTableSource(
            pk_connector._db_path, "measurements", tag_columns=["trial"]
        )
        tag_schema, _ = src.output_schema()
        assert "trial" in tag_schema
        assert "session_id" not in tag_schema

    def test_multiple_explicit_tag_columns(self, pk_connector):
        from orcapod.core.sources import SQLiteTableSource
        src = SQLiteTableSource(
            pk_connector._db_path,
            "measurements",
            tag_columns=["session_id", "trial"],
        )
        tag_schema, _ = src.output_schema()
        assert "session_id" in tag_schema
        assert "trial" in tag_schema


# ===========================================================================
# 5. ROWID fallback
# ===========================================================================


class TestRowidFallback:
    def test_rowid_only_table_uses_rowid_as_tag(self, rowid_connector):
        from orcapod.core.sources import SQLiteTableSource
        src = SQLiteTableSource(rowid_connector._db_path, "logs")
        tag_schema, _ = src.output_schema()
        assert "rowid" in tag_schema

    def test_rowid_is_not_in_packet_schema(self, rowid_connector):
        from orcapod.core.sources import SQLiteTableSource
        src = SQLiteTableSource(rowid_connector._db_path, "logs")
        _, packet_schema = src.output_schema()
        assert "rowid" not in packet_schema

    def test_rowid_values_are_positive_integers(self, rowid_connector):
        from orcapod.core.sources import SQLiteTableSource
        src = SQLiteTableSource(rowid_connector._db_path, "logs")
        for tags, _ in src.iter_packets():
            assert isinstance(tags["rowid"], int)
            assert tags["rowid"] > 0

    def test_rowid_type_is_int64(self, rowid_connector):
        """Verify rowid is actually typed as int64, not large_string."""
        from orcapod.core.sources import SQLiteTableSource
        src = SQLiteTableSource(rowid_connector._db_path, "logs")
        # The raw stream table (before tag/packet split) holds all columns.
        # We can verify the Arrow type via the internal stream table.
        raw = src._stream._table  # ArrowTableStream stores the enriched table
        assert "rowid" in raw.schema.names
        assert raw.schema.field("rowid").type == pa.int64()

    def test_all_rows_returned_for_rowid_table(self, rowid_connector):
        from orcapod.core.sources import SQLiteTableSource
        src = SQLiteTableSource(rowid_connector._db_path, "logs")
        packets = list(src.iter_packets())
        assert len(packets) == 3


# ===========================================================================
# 6. Error cases
# ===========================================================================


class TestErrorCases:
    def test_missing_table_raises_value_error(self, memory_connector):
        from orcapod.core.sources import SQLiteTableSource
        with pytest.raises(ValueError, match="not found in database"):
            SQLiteTableSource(memory_connector._db_path, "nonexistent")

    def test_empty_table_raises_value_error(self, empty_connector):
        from orcapod.core.sources import SQLiteTableSource
        with pytest.raises(ValueError, match="is empty"):
            SQLiteTableSource(empty_connector._db_path, "empty_tbl")


# ===========================================================================
# 7. Stream behaviour
# ===========================================================================


class TestStreamBehaviour:
    def test_producer_is_none(self, pk_connector):
        from orcapod.core.sources import SQLiteTableSource
        src = SQLiteTableSource(pk_connector._db_path, "measurements")
        assert src.producer is None

    def test_upstreams_is_empty(self, pk_connector):
        from orcapod.core.sources import SQLiteTableSource
        src = SQLiteTableSource(pk_connector._db_path, "measurements")
        assert src.upstreams == ()

    def test_iter_packets_yields_one_per_row(self, pk_connector):
        from orcapod.core.sources import SQLiteTableSource
        src = SQLiteTableSource(pk_connector._db_path, "measurements")
        packets = list(src.iter_packets())
        assert len(packets) == 3

    def test_iter_packets_tags_contain_pk(self, pk_connector):
        from orcapod.core.sources import SQLiteTableSource
        src = SQLiteTableSource(pk_connector._db_path, "measurements")
        for tags, _ in src.iter_packets():
            assert "session_id" in tags

    def test_output_schema_returns_two_schemas(self, pk_connector):
        from orcapod.core.sources import SQLiteTableSource
        src = SQLiteTableSource(pk_connector._db_path, "measurements")
        result = src.output_schema()
        assert len(result) == 2

    def test_as_table_returns_pyarrow_table(self, pk_connector):
        from orcapod.core.sources import SQLiteTableSource
        src = SQLiteTableSource(pk_connector._db_path, "measurements")
        t = src.as_table()
        assert isinstance(t, pa.Table)

    def test_as_table_row_count_matches_source(self, pk_connector):
        from orcapod.core.sources import SQLiteTableSource
        src = SQLiteTableSource(pk_connector._db_path, "measurements")
        assert src.as_table().num_rows == 3


# ===========================================================================
# 8. Deterministic hashing
# ===========================================================================


class TestDeterministicHashing:
    def test_pipeline_hash_is_deterministic(self, pk_connector):
        from orcapod.core.sources import SQLiteTableSource
        src1 = SQLiteTableSource(pk_connector._db_path, "measurements")
        src2 = SQLiteTableSource(pk_connector._db_path, "measurements")
        assert src1.pipeline_hash() == src2.pipeline_hash()

    def test_content_hash_is_deterministic(self, pk_connector):
        from orcapod.core.sources import SQLiteTableSource
        src1 = SQLiteTableSource(pk_connector._db_path, "measurements")
        src2 = SQLiteTableSource(pk_connector._db_path, "measurements")
        assert src1.content_hash() == src2.content_hash()

    def test_different_tag_columns_yields_different_pipeline_hash(self, pk_connector):
        from orcapod.core.sources import SQLiteTableSource
        src1 = SQLiteTableSource(pk_connector._db_path, "measurements")
        src2 = SQLiteTableSource(
            pk_connector._db_path, "measurements", tag_columns=["trial"]
        )
        assert src1.pipeline_hash() != src2.pipeline_hash()
```

- [ ] **Step 3.6: Run the new test groups**

```bash
uv run pytest tests/test_core/sources/test_sqlite_table_source.py -k "not Config and not integration" -v
```

Expected: all PASS (excluding the config round-trip and integration tests not yet written).

- [ ] **Step 3.7: Commit the core class and unit tests**

```bash
git add src/orcapod/core/sources/sqlite_table_source.py tests/test_core/sources/test_sqlite_table_source.py
git commit -m "feat(sources): implement SQLiteTableSource with ROWID fallback"
```

---

## Task 4: Wire exports and source registry

**Files:**
- Modify: `src/orcapod/core/sources/__init__.py`
- Modify: `src/orcapod/pipeline/serialization.py`
- Modify: `pytest.ini`

- [ ] **Step 4.1: Update `src/orcapod/core/sources/__init__.py`**

Add the import and `__all__` entry. The file currently ends with `"GLOBAL_SOURCE_REGISTRY",` — add `SQLiteTableSource` right after `DBTableSource`:

```python
from .sqlite_table_source import SQLiteTableSource
```

And add `"SQLiteTableSource"` to `__all__`.

The full updated `__init__.py` should be:

```python
from .base import RootSource
from .arrow_table_source import ArrowTableSource
from .cached_source import CachedSource
from .csv_source import CSVSource
from .data_frame_source import DataFrameSource
from .db_table_source import DBTableSource
from .delta_table_source import DeltaTableSource
from .derived_source import DerivedSource
from .dict_source import DictSource
from .list_source import ListSource
from .sqlite_table_source import SQLiteTableSource
from .source_registry import GLOBAL_SOURCE_REGISTRY, SourceRegistry
from .source_proxy import SourceProxy

__all__ = [
    "RootSource",
    "ArrowTableSource",
    "CachedSource",
    "CSVSource",
    "DataFrameSource",
    "DBTableSource",
    "DeltaTableSource",
    "DerivedSource",
    "DictSource",
    "ListSource",
    "SQLiteTableSource",
    "SourceRegistry",
    "SourceProxy",
    "GLOBAL_SOURCE_REGISTRY",
]
```

- [ ] **Step 4.2: Update `src/orcapod/pipeline/serialization.py`**

In `_build_source_registry()`, add the lazy local import and registry entry following the same pattern as all other entries. Find the return dict and add:

```python
    from orcapod.core.sources.sqlite_table_source import SQLiteTableSource
```

And add `"sqlite_table": SQLiteTableSource` to the returned dict:

```python
    return {
        "csv": CSVSource,
        "delta_table": DeltaTableSource,
        "dict": DictSource,
        "list": ListSource,
        "data_frame": DataFrameSource,
        "arrow_table": ArrowTableSource,
        "cached": CachedSource,
        "sqlite_table": SQLiteTableSource,
    }
```

- [ ] **Step 4.3: Register the `integration` marker in `pytest.ini`**

`pytest.ini` currently only has a `postgres` marker. Add `integration`:

```ini
[pytest]
testpaths = tests
python_files = test_*.py
python_classes = Test*
python_functions = test_*
addopts = -v
markers =
    postgres: mark test as requiring a live PostgreSQL instance
    integration: mark test as an integration test (may be slower)
```

- [ ] **Step 4.4: Run the import/export sanity tests**

```bash
uv run pytest tests/test_core/sources/test_sqlite_table_source.py::test_import_from_core_sources tests/test_core/sources/test_sqlite_table_source.py::test_import_from_orcapod_sources tests/test_core/sources/test_sqlite_table_source.py::test_in_core_sources_all -v
```

Expected: all PASS.

- [ ] **Step 4.5: Run the full unit test suite for the new source**

```bash
uv run pytest tests/test_core/sources/test_sqlite_table_source.py -k "not integration" -v
```

Expected: all PASS.

- [ ] **Step 4.6: Commit the wiring**

```bash
git add src/orcapod/core/sources/__init__.py src/orcapod/pipeline/serialization.py pytest.ini
git commit -m "feat(sources): export SQLiteTableSource and register in source registry"
```

---

## Task 5: Config round-trip tests

These tests require a file-backed SQLite database (via pytest's `tmp_path` fixture) because `:memory:` databases cannot be reconstructed.

**Files:**
- Modify: `tests/test_core/sources/test_sqlite_table_source.py`

- [ ] **Step 5.1: Add the config round-trip test groups**

Append to `tests/test_core/sources/test_sqlite_table_source.py`:

```python
# ===========================================================================
# 9. Config round-trip — PK table
# ===========================================================================


class TestConfigRoundTripPKTable:
    @pytest.fixture
    def file_db_path(self, tmp_path: Path) -> str:
        db_path = str(tmp_path / "test.db")
        conn = sqlite3.connect(db_path)
        _create_table_with_pk(conn)
        conn.close()
        return db_path

    def test_to_config_has_source_type(self, file_db_path):
        from orcapod.core.sources import SQLiteTableSource
        src = SQLiteTableSource(file_db_path, "measurements")
        assert src.to_config()["source_type"] == "sqlite_table"

    def test_to_config_has_db_path(self, file_db_path):
        from orcapod.core.sources import SQLiteTableSource
        src = SQLiteTableSource(file_db_path, "measurements")
        assert src.to_config()["db_path"] == file_db_path

    def test_to_config_has_table_name(self, file_db_path):
        from orcapod.core.sources import SQLiteTableSource
        src = SQLiteTableSource(file_db_path, "measurements")
        assert src.to_config()["table_name"] == "measurements"

    def test_to_config_has_tag_columns(self, file_db_path):
        from orcapod.core.sources import SQLiteTableSource
        src = SQLiteTableSource(file_db_path, "measurements")
        assert "session_id" in src.to_config()["tag_columns"]

    def test_to_config_has_identity_fields(self, file_db_path):
        from orcapod.core.sources import SQLiteTableSource
        src = SQLiteTableSource(file_db_path, "measurements")
        config = src.to_config()
        assert "content_hash" in config
        assert "pipeline_hash" in config

    def test_from_config_reconstructs_successfully(self, file_db_path):
        from orcapod.core.sources import SQLiteTableSource
        src = SQLiteTableSource(file_db_path, "measurements")
        config = src.to_config()
        src2 = SQLiteTableSource.from_config(config)
        assert src2.source_id == src.source_id

    def test_from_config_hashes_match(self, file_db_path):
        from orcapod.core.sources import SQLiteTableSource
        src = SQLiteTableSource(file_db_path, "measurements")
        config = src.to_config()
        src2 = SQLiteTableSource.from_config(config)
        assert src2.content_hash() == src.content_hash()
        assert src2.pipeline_hash() == src.pipeline_hash()

    def test_resolve_source_from_config_works(self, file_db_path):
        from orcapod.core.sources import SQLiteTableSource
        from orcapod.pipeline.serialization import resolve_source_from_config
        src = SQLiteTableSource(file_db_path, "measurements")
        config = src.to_config()
        src2 = resolve_source_from_config(config)
        assert isinstance(src2, SQLiteTableSource)


# ===========================================================================
# 10. Config round-trip — ROWID-only table
# ===========================================================================


class TestConfigRoundTripRowidTable:
    @pytest.fixture
    def rowid_file_db_path(self, tmp_path: Path) -> str:
        db_path = str(tmp_path / "rowid_test.db")
        conn = sqlite3.connect(db_path)
        _create_table_without_pk(conn)
        conn.close()
        return db_path

    def test_to_config_has_rowid_as_tag_column(self, rowid_file_db_path):
        from orcapod.core.sources import SQLiteTableSource
        src = SQLiteTableSource(rowid_file_db_path, "logs")
        assert src.to_config()["tag_columns"] == ["rowid"]

    def test_from_config_reconstructs_rowid_table(self, rowid_file_db_path):
        from orcapod.core.sources import SQLiteTableSource
        src = SQLiteTableSource(rowid_file_db_path, "logs")
        config = src.to_config()
        src2 = SQLiteTableSource.from_config(config)
        tag_schema, _ = src2.output_schema()
        assert "rowid" in tag_schema

    def test_from_config_rowid_hashes_match(self, rowid_file_db_path):
        from orcapod.core.sources import SQLiteTableSource
        src = SQLiteTableSource(rowid_file_db_path, "logs")
        config = src.to_config()
        src2 = SQLiteTableSource.from_config(config)
        assert src2.content_hash() == src.content_hash()
        assert src2.pipeline_hash() == src.pipeline_hash()
```

- [ ] **Step 5.2: Run the config round-trip tests**

```bash
uv run pytest tests/test_core/sources/test_sqlite_table_source.py::TestConfigRoundTripPKTable tests/test_core/sources/test_sqlite_table_source.py::TestConfigRoundTripRowidTable -v
```

Expected: all PASS.

- [ ] **Step 5.3: Commit**

```bash
git add tests/test_core/sources/test_sqlite_table_source.py
git commit -m "test(sources): add config round-trip tests for SQLiteTableSource"
```

---

## Task 6: Integration test — `SQLiteTableSource` in a pipeline

**Background:** `Pipeline` wires sources into a node graph. A `FunctionPod` consumes packets from the source. We verify end-to-end that tag columns flow through and the pipeline produces the expected results.

**Files:**
- Modify: `tests/test_core/sources/test_sqlite_table_source.py`

- [ ] **Step 6.1: Add the integration test**

Append to `tests/test_core/sources/test_sqlite_table_source.py`:

```python
# ===========================================================================
# 11. Integration: SQLiteTableSource in a pipeline
# ===========================================================================


@pytest.mark.integration
class TestPipelineIntegration:
    def test_sqlite_source_in_pipeline(self, pk_connector):
        """Verify SQLiteTableSource drives a full pipeline end-to-end."""
        from orcapod.core.function_pod import FunctionPod
        from orcapod.core.packet_function import PythonPacketFunction
        from orcapod.core.sources import SQLiteTableSource
        from orcapod.databases import InMemoryArrowDatabase
        from orcapod.pipeline import Pipeline
        from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator

        def double_response(response: float) -> float:
            return response * 2.0

        src = SQLiteTableSource(pk_connector._db_path, "measurements")
        pf = PythonPacketFunction(double_response, output_keys="doubled")
        pod = FunctionPod(pf)

        pipeline = Pipeline(
            name="sqlite_integration", pipeline_database=InMemoryArrowDatabase()
        )
        with pipeline:
            pod(src, label="doubler")

        orch = SyncPipelineOrchestrator()
        result = orch.run(pipeline._node_graph)

        fn_outputs = [
            v for k, v in result.node_outputs.items() if k.node_type == "function"
        ]
        assert len(fn_outputs) == 1
        assert len(fn_outputs[0]) == 3

        # Verify tag column (session_id) flows through and results are correct
        doubled_values = sorted(
            [pkt.as_dict()["doubled"] for _, pkt in fn_outputs[0]]
        )
        assert doubled_values == pytest.approx([0.2, 0.4, 0.6])

        # Verify tag values are present
        tag_values = sorted([tags["session_id"] for tags, _ in fn_outputs[0]])
        assert tag_values == ["s1", "s2", "s3"]
```

- [ ] **Step 6.2: Run the integration test**

```bash
uv run pytest tests/test_core/sources/test_sqlite_table_source.py::TestPipelineIntegration -v
```

Expected: PASS.

- [ ] **Step 6.3: Run the complete test file**

```bash
uv run pytest tests/test_core/sources/test_sqlite_table_source.py -v
```

Expected: all tests PASS.

- [ ] **Step 6.4: Commit**

```bash
git add tests/test_core/sources/test_sqlite_table_source.py
git commit -m "test(sources): add pipeline integration test for SQLiteTableSource"
```

---

## Task 7: Full regression run and PR

- [ ] **Step 7.1: Run the complete test suite**

```bash
uv run pytest --ignore=tests/test_databases/test_postgresql_connector_integration.py --ignore=tests/test_databases/test_spiraldb_connector.py -v
```

(Skip the live-DB integration tests that require external services.)

Expected: all tests PASS.

- [ ] **Step 7.2: Re-authenticate with GitHub (tokens expire after 1 hour)**

```bash
gh-app-token-generator nauticalab | gh auth login --with-token
```

- [ ] **Step 7.3: Create the feature branch and push**

```bash
git checkout -b eywalker/plt-1077-implement-source-based-on-sqlite-tables-with-pk-as-default-tag
git push -u origin eywalker/plt-1077-implement-source-based-on-sqlite-tables-with-pk-as-default-tag
```

- [ ] **Step 7.4: Open the PR against `dev`**

```bash
gh pr create \
  --base dev \
  --title "feat(sources): implement SQLiteTableSource with ROWID fallback (PLT-1077)" \
  --body "$(cat <<'EOF'
## Summary

- Implements `SQLiteTableSource` — a `RootSource` backed by a SQLite table
- Primary-key columns are used as tag columns by default
- ROWID-only tables (no explicit PK) automatically fall back to the implicit `rowid` integer column
- `from_config` round-trip works for file-backed databases (unlike `DBTableSource`)
- Registered as `\"sqlite_table\"` in the source registry

## Changes

- `src/orcapod/databases/sqlite_connector.py` — patch `iter_batches` to type `rowid` as `int64`
- `src/orcapod/core/sources/db_table_source.py` — add keyword-only `_query` parameter
- `src/orcapod/core/sources/sqlite_table_source.py` — new `SQLiteTableSource` class
- `src/orcapod/core/sources/__init__.py` — export `SQLiteTableSource`
- `src/orcapod/pipeline/serialization.py` — register `\"sqlite_table\"` source type

## Test plan

- [ ] Unit tests: import/export, protocol conformance, PK defaults, composite PK, ROWID fallback, error cases, stream behaviour, deterministic hashing, config round-trip (PK + ROWID tables)
- [ ] Integration test: `SQLiteTableSource` drives a `FunctionPod` pipeline end-to-end
- [ ] Regression tests for `SQLiteConnector.iter_batches` rowid typing and `DBTableSource._query`

Closes PLT-1077

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step 7.5: Update the Linear issue status to In Progress**

```python
mcp__claude_ai_Linear__save_issue(id="PLT-1077", state="In Progress")
```

---

## Quick Reference

| Command | Purpose |
|---|---|
| `uv run pytest tests/test_core/sources/test_sqlite_table_source.py -v` | Run all new tests |
| `uv run pytest tests/test_databases/test_sqlite_connector.py -v` | Run SQLiteConnector tests |
| `uv run pytest tests/test_core/sources/test_db_table_source.py -v` | Run DBTableSource tests |
| `uv run pytest -k "sqlite" -v` | Run all SQLite-related tests |
| `uv run pytest --ignore=tests/test_databases/test_postgresql_connector_integration.py --ignore=tests/test_databases/test_spiraldb_connector.py -v` | Full suite (skip live-DB tests) |
