# PostgreSQLTableSource Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement `PostgreSQLTableSource`, a read-only OrcaPod `Source` backed by a PostgreSQL table, using PK columns as default tag columns.

**Architecture:** Thin subclass of `DBTableSource` (which handles all source logic). `PostgreSQLTableSource.__init__` stores the DSN, creates a `PostgreSQLConnector`, delegates entirely to `DBTableSource.__init__` (which eagerly loads all data into memory), then closes the connector. No ROWID fallback needed — PostgreSQL PKs are always `NOT NULL`.

**Tech Stack:** Python, PyArrow, psycopg3 (psycopg), unittest.mock (unit tests), pytest with `@pytest.mark.postgres` marker (integration tests).

---

**Spec:** `docs/specs/2026-03-26-postgresql-table-source-design.md`

---

## File Map

| Action | Path | Responsibility |
|---|---|---|
| Create | `src/orcapod/core/sources/postgresql_table_source.py` | `PostgreSQLTableSource` class |
| Create | `tests/test_core/sources/test_postgresql_table_source.py` | Unit tests (no live DB) |
| Create | `tests/test_core/sources/test_postgresql_table_source_integration.py` | Integration tests (live DB, `@pytest.mark.postgres`) |
| Modify | `src/orcapod/core/sources/__init__.py` | Import + `__all__` entry |
| Modify | `src/orcapod/pipeline/serialization.py` | Register in `_build_source_registry()` |

---

## Task 1: Stub class + import/export wiring

Wire the new source into the package so imports work before any real logic exists.

**Files:**
- Create: `src/orcapod/core/sources/postgresql_table_source.py`
- Modify: `src/orcapod/core/sources/__init__.py`
- Test: `tests/test_core/sources/test_postgresql_table_source.py`

- [ ] **Step 1.1: Write failing import/export tests**

Create `tests/test_core/sources/test_postgresql_table_source.py` with:

```python
"""Unit tests for PostgreSQLTableSource."""
from __future__ import annotations


# ===========================================================================
# 1. Import / export sanity
# ===========================================================================


def test_import_from_core_sources():
    from orcapod.core.sources import PostgreSQLTableSource
    assert PostgreSQLTableSource is not None


def test_import_from_orcapod_sources():
    from orcapod.sources import PostgreSQLTableSource
    assert PostgreSQLTableSource is not None


def test_in_core_sources_all():
    import orcapod.core.sources as m
    assert "PostgreSQLTableSource" in m.__all__
```

- [ ] **Step 1.2: Run tests to confirm they fail**

```bash
cd /path/to/orcapod-python
pytest tests/test_core/sources/test_postgresql_table_source.py -v
```

Expected: `ImportError` or `AssertionError` — `PostgreSQLTableSource` does not exist yet.

- [ ] **Step 1.3: Create the stub source file**

Create `src/orcapod/core/sources/postgresql_table_source.py`:

```python
"""PostgreSQLTableSource — a read-only RootSource backed by a PostgreSQL table.

Wraps a PostgreSQL table as an OrcaPod Source. Primary-key columns are used
as tag columns by default.

Example::

    source = PostgreSQLTableSource(
        "postgresql://user:pass@localhost:5432/mydb", "measurements"
    )
"""
from __future__ import annotations

from collections.abc import Collection
from typing import TYPE_CHECKING, Any

from orcapod.core.sources.db_table_source import DBTableSource
from orcapod.databases.postgresql_connector import PostgreSQLConnector

if TYPE_CHECKING:
    from orcapod import contexts
    from orcapod.config import Config


class PostgreSQLTableSource(DBTableSource):
    """A read-only Source backed by a table in a PostgreSQL database.

    At construction time the source:
    1. Stores the DSN for serialisation.
    2. Opens a ``PostgreSQLConnector`` for *dsn*.
    3. Delegates to ``DBTableSource.__init__``, which validates the table,
       resolves tag columns (defaults to PK columns), fetches all rows as
       Arrow batches, and builds the stream.
    4. Closes the connector — all data is eagerly loaded into memory, so the
       connection is released immediately.

    PostgreSQL PK columns are always ``NOT NULL``, so NULL tag values can
    only arise when *tag_columns* is overridden to point at a nullable
    column. Such NULLs are passed through as-is (Arrow supports nulls).

    Args:
        dsn: libpq connection string.
            URI form: ``"postgresql://user:pass@host:5432/dbname"``
            Keyword form: ``"host=localhost dbname=mydb user=alice"``
        table_name: Name of the table to expose as a source.
        tag_columns: Columns to use as tag columns. If ``None`` (default),
            the table's primary-key columns are used. Raises ``ValueError``
            if the table has no primary key and no explicit columns are given.
        system_tag_columns: Additional system-level tag columns.
        record_id_column: Column for stable per-row record IDs in provenance.
        source_id: Canonical source name. Defaults to *table_name*.
        label: Human-readable label for this source node.
        data_context: Data context governing type conversion and hashing.
        config: OrcaPod configuration.

    Raises:
        ValueError: If the table is not found, is empty, or has no PK and
            no *tag_columns* are given.
        psycopg.OperationalError: If the DSN is invalid or connection fails.
    """

    def __init__(
        self,
        dsn: str,
        table_name: str,
        tag_columns: Collection[str] | None = None,
        system_tag_columns: Collection[str] = (),
        record_id_column: str | None = None,
        source_id: str | None = None,
        label: str | None = None,
        data_context: "str | contexts.DataContext | None" = None,
        config: "Config | None" = None,
    ) -> None:
        raise NotImplementedError("TODO: implement in Task 2")

    def to_config(self) -> dict[str, Any]:
        raise NotImplementedError("TODO: implement in Task 4")

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> "PostgreSQLTableSource":
        raise NotImplementedError("TODO: implement in Task 4")
```

- [ ] **Step 1.4: Wire into `src/orcapod/core/sources/__init__.py`**

Add the import and `__all__` entry. Open the file and make two changes:

Add this import line after the `sqlite_table_source` import:
```python
from .postgresql_table_source import PostgreSQLTableSource
```

Add `"PostgreSQLTableSource"` to the `__all__` list (alphabetical order is not required; add it after `"SQLiteTableSource"`):
```python
    "PostgreSQLTableSource",
```

- [ ] **Step 1.5: Run the import tests to confirm they pass**

```bash
pytest tests/test_core/sources/test_postgresql_table_source.py -v
```

Expected: All 3 tests PASS.

- [ ] **Step 1.6: Commit**

```bash
git add src/orcapod/core/sources/postgresql_table_source.py \
        src/orcapod/core/sources/__init__.py \
        tests/test_core/sources/test_postgresql_table_source.py
git commit -m "feat(sources): stub PostgreSQLTableSource with import/export wiring (PLT-1072)"
```

---

## Task 2: Core `__init__` — construction, PK tags, error cases

Implement the actual `__init__` and verify the main behaviours.

**Files:**
- Modify: `src/orcapod/core/sources/postgresql_table_source.py`
- Modify: `tests/test_core/sources/test_postgresql_table_source.py`

**How mocking works in these tests:** `PostgreSQLConnector` is patched at the point it is imported in the source module (`orcapod.core.sources.postgresql_table_source.PostgreSQLConnector`). The patch replaces the class constructor so `PostgreSQLConnector(dsn)` returns a `MagicMock` instance. That mock's methods (`get_table_names`, `get_pk_columns`, `iter_batches`) are configured to return controlled Arrow data — no real database required.

- [ ] **Step 2.1: Add shared test helpers and protocol conformance tests**

Append to `tests/test_core/sources/test_postgresql_table_source.py`:

```python
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

DSN = "postgresql://user:pass@localhost:5432/testdb"
_PATCH = "orcapod.core.sources.postgresql_table_source.PostgreSQLConnector"


def _make_mock_connector(
    table_names: list[str] | None = None,
    pk_columns: list[str] | None = None,
    batches: list[pa.RecordBatch] | None = None,
) -> MagicMock:
    """Build a MagicMock shaped like a PostgreSQLConnector.

    Defaults: table 'measurements' with PK 'session_id' and 3 rows.
    """
    mock = MagicMock()

    if table_names is None:
        table_names = ["measurements"]
    if pk_columns is None:
        pk_columns = ["session_id"]
    if batches is None:
        schema = pa.schema([
            pa.field("session_id", pa.large_string()),
            pa.field("trial", pa.int64()),
            pa.field("response", pa.float64()),
        ])
        batches = [
            pa.RecordBatch.from_arrays(
                [
                    pa.array(["s1", "s2", "s3"], type=pa.large_string()),
                    pa.array([1, 2, 3], type=pa.int64()),
                    pa.array([0.1, 0.2, 0.3], type=pa.float64()),
                ],
                schema=schema,
            )
        ]

    mock.get_table_names.return_value = table_names
    mock.get_pk_columns.return_value = pk_columns
    mock.iter_batches.return_value = iter(batches)
    return mock


# ===========================================================================
# 2. Protocol conformance
# ===========================================================================


class TestProtocolConformance:
    def test_is_source_protocol(self):
        from orcapod.core.sources import PostgreSQLTableSource
        from orcapod.protocols.core_protocols import SourceProtocol

        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src = PostgreSQLTableSource(DSN, "measurements")
        assert isinstance(src, SourceProtocol)

    def test_is_stream_protocol(self):
        from orcapod.core.sources import PostgreSQLTableSource
        from orcapod.protocols.core_protocols import StreamProtocol

        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src = PostgreSQLTableSource(DSN, "measurements")
        assert isinstance(src, StreamProtocol)

    def test_is_pipeline_element_protocol(self):
        from orcapod.core.sources import PostgreSQLTableSource
        from orcapod.protocols.hashing_protocols import PipelineElementProtocol

        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src = PostgreSQLTableSource(DSN, "measurements")
        assert isinstance(src, PipelineElementProtocol)


# ===========================================================================
# 3. PK as default tag columns
# ===========================================================================


class TestPKAsDefaultTags:
    def test_single_pk_is_tag_column(self):
        from orcapod.core.sources import PostgreSQLTableSource

        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src = PostgreSQLTableSource(DSN, "measurements")
        tag_schema, _ = src.output_schema()
        assert "session_id" in tag_schema

    def test_pk_not_in_packet_schema(self):
        from orcapod.core.sources import PostgreSQLTableSource

        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src = PostgreSQLTableSource(DSN, "measurements")
        _, packet_schema = src.output_schema()
        assert "session_id" not in packet_schema

    def test_non_pk_columns_in_packet_schema(self):
        from orcapod.core.sources import PostgreSQLTableSource

        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src = PostgreSQLTableSource(DSN, "measurements")
        _, packet_schema = src.output_schema()
        assert "trial" in packet_schema
        assert "response" in packet_schema

    def test_composite_pk_all_columns_are_tags(self):
        from orcapod.core.sources import PostgreSQLTableSource

        schema = pa.schema([
            pa.field("user_id", pa.large_string()),
            pa.field("event_id", pa.int64()),
            pa.field("payload", pa.large_string()),
        ])
        batch = pa.RecordBatch.from_arrays(
            [
                pa.array(["u1", "u1"], type=pa.large_string()),
                pa.array([1, 2], type=pa.int64()),
                pa.array(["click", "scroll"], type=pa.large_string()),
            ],
            schema=schema,
        )
        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector(
                table_names=["events"],
                pk_columns=["user_id", "event_id"],
                batches=[batch],
            )
            src = PostgreSQLTableSource(DSN, "events")
        tag_schema, _ = src.output_schema()
        assert "user_id" in tag_schema
        assert "event_id" in tag_schema

    def test_default_source_id_is_table_name(self):
        from orcapod.core.sources import PostgreSQLTableSource

        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src = PostgreSQLTableSource(DSN, "measurements")
        assert src.source_id == "measurements"

    def test_explicit_source_id_overrides_default(self):
        from orcapod.core.sources import PostgreSQLTableSource

        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src = PostgreSQLTableSource(DSN, "measurements", source_id="meas")
        assert src.source_id == "meas"


# ===========================================================================
# 4. Explicit tag_columns override
# ===========================================================================


class TestExplicitTagOverride:
    def test_explicit_tag_columns_override_pk(self):
        from orcapod.core.sources import PostgreSQLTableSource

        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src = PostgreSQLTableSource(DSN, "measurements", tag_columns=["trial"])
        tag_schema, _ = src.output_schema()
        assert "trial" in tag_schema
        assert "session_id" not in tag_schema

    def test_multiple_explicit_tag_columns(self):
        from orcapod.core.sources import PostgreSQLTableSource

        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src = PostgreSQLTableSource(
                DSN, "measurements", tag_columns=["session_id", "trial"]
            )
        tag_schema, _ = src.output_schema()
        assert "session_id" in tag_schema
        assert "trial" in tag_schema


# ===========================================================================
# 5. No-PK error
# ===========================================================================


class TestNoPKError:
    def test_no_pk_and_no_tag_columns_raises(self):
        from orcapod.core.sources import PostgreSQLTableSource

        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector(pk_columns=[])
            with pytest.raises(ValueError, match="has no primary key columns"):
                PostgreSQLTableSource(DSN, "measurements")


# ===========================================================================
# 6. Missing / empty table errors
# ===========================================================================


class TestTableErrors:
    def test_missing_table_raises_value_error(self):
        from orcapod.core.sources import PostgreSQLTableSource

        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector(table_names=[])
            with pytest.raises(ValueError, match="not found in database"):
                PostgreSQLTableSource(DSN, "nonexistent")

    def test_empty_table_raises_value_error(self):
        from orcapod.core.sources import PostgreSQLTableSource

        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector(batches=[])
            with pytest.raises(ValueError, match="is empty"):
                PostgreSQLTableSource(DSN, "measurements")


# ===========================================================================
# 7. Stream behaviour
# ===========================================================================


class TestStreamBehaviour:
    def test_producer_is_none(self):
        from orcapod.core.sources import PostgreSQLTableSource

        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src = PostgreSQLTableSource(DSN, "measurements")
        assert src.producer is None

    def test_upstreams_is_empty(self):
        from orcapod.core.sources import PostgreSQLTableSource

        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src = PostgreSQLTableSource(DSN, "measurements")
        assert src.upstreams == ()

    def test_iter_packets_yields_one_per_row(self):
        from orcapod.core.sources import PostgreSQLTableSource

        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src = PostgreSQLTableSource(DSN, "measurements")
        assert len(list(src.iter_packets())) == 3

    def test_iter_packets_tags_contain_pk(self):
        from orcapod.core.sources import PostgreSQLTableSource

        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src = PostgreSQLTableSource(DSN, "measurements")
        for tags, _ in src.iter_packets():
            assert "session_id" in tags

    def test_output_schema_returns_two_schemas(self):
        from orcapod.core.sources import PostgreSQLTableSource

        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src = PostgreSQLTableSource(DSN, "measurements")
        assert len(src.output_schema()) == 2

    def test_as_table_returns_pyarrow_table(self):
        from orcapod.core.sources import PostgreSQLTableSource

        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src = PostgreSQLTableSource(DSN, "measurements")
        assert isinstance(src.as_table(), pa.Table)

    def test_as_table_row_count_matches_source(self):
        from orcapod.core.sources import PostgreSQLTableSource

        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src = PostgreSQLTableSource(DSN, "measurements")
        assert src.as_table().num_rows == 3


# ===========================================================================
# 8. Deterministic hashing
# ===========================================================================


class TestDeterministicHashing:
    def test_pipeline_hash_is_deterministic(self):
        from orcapod.core.sources import PostgreSQLTableSource

        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src1 = PostgreSQLTableSource(DSN, "measurements")
        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src2 = PostgreSQLTableSource(DSN, "measurements")
        assert src1.pipeline_hash() == src2.pipeline_hash()

    def test_content_hash_is_deterministic(self):
        from orcapod.core.sources import PostgreSQLTableSource

        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src1 = PostgreSQLTableSource(DSN, "measurements")
        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src2 = PostgreSQLTableSource(DSN, "measurements")
        assert src1.content_hash() == src2.content_hash()

    def test_different_tag_columns_yields_different_pipeline_hash(self):
        from orcapod.core.sources import PostgreSQLTableSource

        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src1 = PostgreSQLTableSource(DSN, "measurements")
        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src2 = PostgreSQLTableSource(DSN, "measurements", tag_columns=["trial"])
        assert src1.pipeline_hash() != src2.pipeline_hash()
```

- [ ] **Step 2.2: Run tests to confirm they fail**

```bash
pytest tests/test_core/sources/test_postgresql_table_source.py -v -k "not test_import"
```

Expected: All new tests FAIL with `NotImplementedError`.

- [ ] **Step 2.3: Implement `__init__` in the source file**

Replace the `__init__` stub in `src/orcapod/core/sources/postgresql_table_source.py`:

```python
    def __init__(
        self,
        dsn: str,
        table_name: str,
        tag_columns: Collection[str] | None = None,
        system_tag_columns: Collection[str] = (),
        record_id_column: str | None = None,
        source_id: str | None = None,
        label: str | None = None,
        data_context: "str | contexts.DataContext | None" = None,
        config: "Config | None" = None,
    ) -> None:
        self._dsn = dsn  # store before try — needed by to_config even if super() raises
        connector = PostgreSQLConnector(dsn)  # outside try — if this raises, finally never runs
        try:
            super().__init__(
                connector,
                table_name,
                tag_columns=tag_columns,
                system_tag_columns=system_tag_columns,
                record_id_column=record_id_column,
                source_id=source_id,
                label=label,
                data_context=data_context,
                config=config,
            )
        finally:
            try:
                connector.close()
            except Exception:
                pass  # suppress close errors; don't mask original __init__ failure
```

- [ ] **Step 2.4: Run all unit tests and confirm they pass**

```bash
pytest tests/test_core/sources/test_postgresql_table_source.py -v
```

Expected: All tests PASS (the `to_config`/`from_config` tests haven't been written yet).

- [ ] **Step 2.5: Confirm connector is closed after construction**

Add this test to `TestStreamBehaviour` (verifies the connector close is called):

```python
    def test_connector_is_closed_after_construction(self):
        from orcapod.core.sources import PostgreSQLTableSource

        with patch(_PATCH) as mock_cls:
            mock_connector = _make_mock_connector()
            mock_cls.return_value = mock_connector
            PostgreSQLTableSource(DSN, "measurements")
        mock_connector.close.assert_called_once()
```

Run the full test file again:

```bash
pytest tests/test_core/sources/test_postgresql_table_source.py -v
```

Expected: PASS.

- [ ] **Step 2.6: Commit**

```bash
git add src/orcapod/core/sources/postgresql_table_source.py \
        tests/test_core/sources/test_postgresql_table_source.py
git commit -m "feat(sources): implement PostgreSQLTableSource.__init__ with PK tag resolution (PLT-1072)"
```

---

## Task 3: `to_config` / `from_config` serialisation

Implement and test the round-trip serialisation.

**Files:**
- Modify: `src/orcapod/core/sources/postgresql_table_source.py`
- Modify: `tests/test_core/sources/test_postgresql_table_source.py`

- [ ] **Step 3.1: Write failing serialisation tests**

Append to `tests/test_core/sources/test_postgresql_table_source.py`:

```python
# ===========================================================================
# 9. to_config shape
# ===========================================================================


class TestToConfig:
    def _make_src(self):
        from orcapod.core.sources import PostgreSQLTableSource

        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            return PostgreSQLTableSource(DSN, "measurements")

    def test_has_source_type_postgresql_table(self):
        assert self._make_src().to_config()["source_type"] == "postgresql_table"

    def test_has_dsn(self):
        assert self._make_src().to_config()["dsn"] == DSN

    def test_has_table_name(self):
        assert self._make_src().to_config()["table_name"] == "measurements"

    def test_has_tag_columns(self):
        assert "session_id" in self._make_src().to_config()["tag_columns"]

    def test_has_source_id(self):
        assert self._make_src().to_config()["source_id"] == "measurements"

    def test_has_content_hash(self):
        assert "content_hash" in self._make_src().to_config()

    def test_has_pipeline_hash(self):
        assert "pipeline_hash" in self._make_src().to_config()

    def test_no_connector_key(self):
        assert "connector" not in self._make_src().to_config()

    def test_no_label_key(self):
        # label is not serialised (consistent with SQLiteTableSource)
        assert "label" not in self._make_src().to_config()


# ===========================================================================
# 10. from_config round-trip
# ===========================================================================


class TestFromConfig:
    def _make_src(self):
        from orcapod.core.sources import PostgreSQLTableSource

        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            return PostgreSQLTableSource(DSN, "measurements")

    def test_from_config_reconstructs_source_id(self):
        from orcapod.core.sources import PostgreSQLTableSource

        src = self._make_src()
        config = src.to_config()
        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src2 = PostgreSQLTableSource.from_config(config)
        assert src2.source_id == src.source_id

    def test_from_config_hashes_match(self):
        from orcapod.core.sources import PostgreSQLTableSource

        src = self._make_src()
        config = src.to_config()
        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src2 = PostgreSQLTableSource.from_config(config)
        assert src2.content_hash() == src.content_hash()
        assert src2.pipeline_hash() == src.pipeline_hash()

    def test_from_config_with_explicit_tag_columns(self):
        from orcapod.core.sources import PostgreSQLTableSource

        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src = PostgreSQLTableSource(DSN, "measurements", tag_columns=["trial"])
        config = src.to_config()
        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src2 = PostgreSQLTableSource.from_config(config)
        tag_schema, _ = src2.output_schema()
        assert "trial" in tag_schema

    def test_from_config_missing_dsn_raises(self):
        from orcapod.core.sources import PostgreSQLTableSource

        with pytest.raises(KeyError):
            PostgreSQLTableSource.from_config({"table_name": "measurements"})
```

- [ ] **Step 3.2: Run tests to confirm they fail**

```bash
pytest tests/test_core/sources/test_postgresql_table_source.py::TestToConfig \
       tests/test_core/sources/test_postgresql_table_source.py::TestFromConfig -v
```

Expected: FAIL with `NotImplementedError`.

- [ ] **Step 3.3: Implement `to_config` and `from_config`**

Replace the stubs in `src/orcapod/core/sources/postgresql_table_source.py`:

```python
    def to_config(self) -> dict[str, Any]:
        """Serialize source configuration to a JSON-compatible dict."""
        base = super().to_config()
        base.pop("connector", None)
        return {**base, "source_type": "postgresql_table", "dsn": self._dsn}

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> "PostgreSQLTableSource":
        """Reconstruct a PostgreSQLTableSource from a config dict.

        Args:
            config: Dict as produced by ``to_config()``.

        Returns:
            A new ``PostgreSQLTableSource`` instance.
        """
        return cls(
            dsn=config["dsn"],
            table_name=config["table_name"],
            tag_columns=config.get("tag_columns"),
            system_tag_columns=config.get("system_tag_columns", ()),
            record_id_column=config.get("record_id_column"),
            source_id=config.get("source_id"),
            label=config.get("label"),
            data_context=config.get("data_context"),
        )
```

- [ ] **Step 3.4: Run all unit tests**

```bash
pytest tests/test_core/sources/test_postgresql_table_source.py -v
```

Expected: All tests PASS.

- [ ] **Step 3.5: Commit**

```bash
git add src/orcapod/core/sources/postgresql_table_source.py \
        tests/test_core/sources/test_postgresql_table_source.py
git commit -m "feat(sources): implement PostgreSQLTableSource.to_config and from_config (PLT-1072)"
```

---

## Task 4: Source registry registration

Register `PostgreSQLTableSource` so `resolve_source_from_config` can deserialise it.

**Files:**
- Modify: `src/orcapod/pipeline/serialization.py`
- Modify: `tests/test_core/sources/test_postgresql_table_source.py`

- [ ] **Step 4.1: Write failing registry test**

Append to `tests/test_core/sources/test_postgresql_table_source.py`:

```python
# ===========================================================================
# 11. resolve_source_from_config dispatch
# ===========================================================================


def test_resolve_source_from_config_dispatches_to_postgresql_table_source():
    from orcapod.core.sources import PostgreSQLTableSource
    from orcapod.pipeline.serialization import resolve_source_from_config

    with patch(_PATCH) as mock_cls:
        mock_cls.return_value = _make_mock_connector()
        src = PostgreSQLTableSource(DSN, "measurements")
    config = src.to_config()

    with patch(_PATCH) as mock_cls:
        mock_cls.return_value = _make_mock_connector()
        src2 = resolve_source_from_config(config)

    assert isinstance(src2, PostgreSQLTableSource)
```

- [ ] **Step 4.2: Run test to confirm it fails**

```bash
pytest tests/test_core/sources/test_postgresql_table_source.py::test_resolve_source_from_config_dispatches_to_postgresql_table_source -v
```

Expected: FAIL — `ValueError: Unknown source type: 'postgresql_table'`.

- [ ] **Step 4.3: Register in `_build_source_registry()`**

Open `src/orcapod/pipeline/serialization.py`. Find `_build_source_registry()` (around line 54). Add a deferred local import and registry entry inside the function body, after the `sqlite_table_source` import:

```python
    from orcapod.core.sources.postgresql_table_source import PostgreSQLTableSource
```

Add to the returned dict:
```python
        "postgresql_table": PostgreSQLTableSource,
```

The full function after the change should look like:

```python
def _build_source_registry() -> dict[str, type]:
    from orcapod.core.sources.arrow_table_source import ArrowTableSource
    from orcapod.core.sources.cached_source import CachedSource
    from orcapod.core.sources.csv_source import CSVSource
    from orcapod.core.sources.data_frame_source import DataFrameSource
    from orcapod.core.sources.delta_table_source import DeltaTableSource
    from orcapod.core.sources.dict_source import DictSource
    from orcapod.core.sources.list_source import ListSource
    from orcapod.core.sources.sqlite_table_source import SQLiteTableSource
    from orcapod.core.sources.postgresql_table_source import PostgreSQLTableSource

    return {
        "csv": CSVSource,
        "delta_table": DeltaTableSource,
        "dict": DictSource,
        "list": ListSource,
        "data_frame": DataFrameSource,
        "arrow_table": ArrowTableSource,
        "cached": CachedSource,
        "sqlite_table": SQLiteTableSource,
        "postgresql_table": PostgreSQLTableSource,
    }
```

- [ ] **Step 4.4: Run all unit tests**

```bash
pytest tests/test_core/sources/test_postgresql_table_source.py -v
```

Expected: All tests PASS.

- [ ] **Step 4.5: Run the full test suite to check for regressions**

```bash
pytest tests/ -v --ignore=tests/test_databases/test_postgresql_connector_integration.py \
                  --ignore=tests/test_databases/test_spiraldb_connector.py \
                  --ignore=tests/test_databases/test_spiraldb_connector_integration.py \
                  -m "not postgres"
```

Expected: All existing tests continue to PASS.

- [ ] **Step 4.6: Commit**

```bash
git add src/orcapod/pipeline/serialization.py \
        tests/test_core/sources/test_postgresql_table_source.py
git commit -m "feat(sources): register PostgreSQLTableSource in source registry (PLT-1072)"
```

---

## Task 5: Integration tests

Write integration tests that run against a live PostgreSQL instance. These are marked `@pytest.mark.postgres` and are skipped in normal CI unless a PostgreSQL service is available.

**Files:**
- Create: `tests/test_core/sources/test_postgresql_table_source_integration.py`

Note: These tests follow the per-test schema isolation pattern from `tests/test_databases/test_postgresql_connector_integration.py`. Each test gets a fresh PostgreSQL schema created before the test and dropped after.

- [ ] **Step 5.1: Create the integration test file**

Create `tests/test_core/sources/test_postgresql_table_source_integration.py`:

```python
"""Integration tests for PostgreSQLTableSource — requires a live PostgreSQL instance.

Run with:
    pytest -m postgres tests/test_core/sources/test_postgresql_table_source_integration.py

Connects via standard PG* environment variables:

    PGHOST      (default: localhost)
    PGPORT      (default: 5432)
    PGDATABASE  (default: testdb)
    PGUSER      (default: postgres)
    PGPASSWORD  (default: postgres)

Each test gets an isolated schema created fresh and dropped on teardown.
"""
from __future__ import annotations

import os
import uuid
from collections.abc import Iterator

import psycopg
import pyarrow as pa
import pytest


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _base_dsn() -> str:
    host = os.environ.get("PGHOST", "localhost")
    port = os.environ.get("PGPORT", "5432")
    dbname = os.environ.get("PGDATABASE", "testdb")
    user = os.environ.get("PGUSER", "postgres")
    password = os.environ.get("PGPASSWORD", "postgres")
    return f"host={host} port={port} dbname={dbname} user={user} password={password}"


@pytest.fixture
def pg_schema() -> Iterator[str]:
    """Create a fresh per-test schema; drop it on teardown."""
    base_dsn = _base_dsn()
    schema = f"test_{uuid.uuid4().hex[:12]}"
    with psycopg.connect(base_dsn, autocommit=True) as admin:
        admin.execute(f"CREATE SCHEMA {schema}")
    yield schema
    with psycopg.connect(base_dsn, autocommit=True) as admin:
        admin.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")


@pytest.fixture
def schema_dsn(pg_schema: str) -> str:
    """DSN that pins the search_path to the test schema."""
    return f"{_base_dsn()} options=-csearch_path={pg_schema}"


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.postgres
class TestSinglePKTable:
    """Source backed by a table with a single-column PK."""

    def test_pk_column_is_tag(self, schema_dsn: str) -> None:
        from orcapod.core.sources import PostgreSQLTableSource

        with psycopg.connect(schema_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    'CREATE TABLE "measurements" '
                    '(session_id TEXT PRIMARY KEY, trial INTEGER, response REAL)'
                )
                cur.executemany(
                    'INSERT INTO "measurements" VALUES (%s, %s, %s)',
                    [("s1", 1, 0.1), ("s2", 2, 0.2), ("s3", 3, 0.3)],
                )
            conn.commit()

        src = PostgreSQLTableSource(schema_dsn, "measurements")
        tag_schema, _ = src.output_schema()
        assert "session_id" in tag_schema

    def test_non_pk_columns_in_packet_schema(self, schema_dsn: str) -> None:
        from orcapod.core.sources import PostgreSQLTableSource

        with psycopg.connect(schema_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    'CREATE TABLE "measurements" '
                    '(session_id TEXT PRIMARY KEY, trial INTEGER, response REAL)'
                )
                cur.executemany(
                    'INSERT INTO "measurements" VALUES (%s, %s, %s)',
                    [("s1", 1, 0.1)],
                )
            conn.commit()

        src = PostgreSQLTableSource(schema_dsn, "measurements")
        _, packet_schema = src.output_schema()
        assert "trial" in packet_schema
        assert "response" in packet_schema

    def test_iter_packets_count_matches_rows(self, schema_dsn: str) -> None:
        from orcapod.core.sources import PostgreSQLTableSource

        with psycopg.connect(schema_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    'CREATE TABLE "measurements" '
                    '(session_id TEXT PRIMARY KEY, trial INTEGER, response REAL)'
                )
                cur.executemany(
                    'INSERT INTO "measurements" VALUES (%s, %s, %s)',
                    [("s1", 1, 0.1), ("s2", 2, 0.2), ("s3", 3, 0.3)],
                )
            conn.commit()

        src = PostgreSQLTableSource(schema_dsn, "measurements")
        assert len(list(src.iter_packets())) == 3

    def test_tag_values_are_correct(self, schema_dsn: str) -> None:
        from orcapod.core.sources import PostgreSQLTableSource

        with psycopg.connect(schema_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    'CREATE TABLE "measurements" '
                    '(session_id TEXT PRIMARY KEY, trial INTEGER, response REAL)'
                )
                cur.executemany(
                    'INSERT INTO "measurements" VALUES (%s, %s, %s)',
                    [("s1", 1, 0.1), ("s2", 2, 0.2), ("s3", 3, 0.3)],
                )
            conn.commit()

        src = PostgreSQLTableSource(schema_dsn, "measurements")
        tag_values = sorted([tags["session_id"] for tags, _ in src.iter_packets()])
        assert tag_values == ["s1", "s2", "s3"]


@pytest.mark.postgres
class TestCompositePKTable:
    """Source backed by a table with a composite PK."""

    def test_both_pk_columns_are_tags(self, schema_dsn: str) -> None:
        from orcapod.core.sources import PostgreSQLTableSource

        with psycopg.connect(schema_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    'CREATE TABLE "events" '
                    '(user_id TEXT, event_id INTEGER, payload TEXT, '
                    'PRIMARY KEY (user_id, event_id))'
                )
                cur.executemany(
                    'INSERT INTO "events" VALUES (%s, %s, %s)',
                    [("u1", 1, "click"), ("u1", 2, "scroll")],
                )
            conn.commit()

        src = PostgreSQLTableSource(schema_dsn, "events")
        tag_schema, _ = src.output_schema()
        assert "user_id" in tag_schema
        assert "event_id" in tag_schema


@pytest.mark.postgres
class TestExplicitTagOverride:
    """tag_columns override overrides the PK."""

    def test_explicit_tag_columns_override_pk(self, schema_dsn: str) -> None:
        from orcapod.core.sources import PostgreSQLTableSource

        with psycopg.connect(schema_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    'CREATE TABLE "measurements" '
                    '(session_id TEXT PRIMARY KEY, trial INTEGER, response REAL)'
                )
                cur.executemany(
                    'INSERT INTO "measurements" VALUES (%s, %s, %s)',
                    [("s1", 1, 0.1)],
                )
            conn.commit()

        src = PostgreSQLTableSource(
            schema_dsn, "measurements", tag_columns=["trial"]
        )
        tag_schema, _ = src.output_schema()
        assert "trial" in tag_schema
        assert "session_id" not in tag_schema


@pytest.mark.postgres
@pytest.mark.integration
class TestPipelineIntegration:
    """PostgreSQLTableSource drives a full pipeline end-to-end."""

    def test_postgresql_source_in_pipeline(self, schema_dsn: str) -> None:
        from orcapod.core.function_pod import FunctionPod
        from orcapod.core.packet_function import PythonPacketFunction
        from orcapod.core.sources import PostgreSQLTableSource
        from orcapod.databases import InMemoryArrowDatabase
        from orcapod.pipeline import Pipeline
        from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator

        with psycopg.connect(schema_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    'CREATE TABLE "measurements" '
                    '(session_id TEXT PRIMARY KEY, trial INTEGER, response REAL)'
                )
                cur.executemany(
                    'INSERT INTO "measurements" VALUES (%s, %s, %s)',
                    [("s1", 1, 0.1), ("s2", 2, 0.2), ("s3", 3, 0.3)],
                )
            conn.commit()

        def double_response(trial: int, response: float) -> float:
            return response * 2.0

        src = PostgreSQLTableSource(schema_dsn, "measurements")
        pf = PythonPacketFunction(double_response, output_keys="doubled")
        pod = FunctionPod(pf)

        pipeline = Pipeline(
            name="pg_integration", pipeline_database=InMemoryArrowDatabase()
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

        doubled_values = sorted([pkt.as_dict()["doubled"] for _, pkt in fn_outputs[0]])
        assert doubled_values == pytest.approx([0.2, 0.4, 0.6])

        tag_values = sorted([tags["session_id"] for tags, _ in fn_outputs[0]])
        assert tag_values == ["s1", "s2", "s3"]
```

- [ ] **Step 5.2: Verify the integration test file is syntactically correct (dry run)**

```bash
python -c "import ast; ast.parse(open('tests/test_core/sources/test_postgresql_table_source_integration.py').read()); print('OK')"
```

Expected: `OK`

- [ ] **Step 5.3: Commit**

```bash
git add tests/test_core/sources/test_postgresql_table_source_integration.py
git commit -m "test(sources): add PostgreSQLTableSource integration tests (PLT-1072)"
```

---

## Task 6: Final verification

Run the full unit test suite one last time to confirm everything is clean.

- [ ] **Step 6.1: Run all unit tests (excluding postgres/spiraldb integration)**

```bash
pytest tests/ -v \
  --ignore=tests/test_databases/test_postgresql_connector_integration.py \
  --ignore=tests/test_databases/test_spiraldb_connector.py \
  --ignore=tests/test_databases/test_spiraldb_connector_integration.py \
  -m "not postgres"
```

Expected: All tests PASS, no failures.

- [ ] **Step 6.2: Verify the new source file looks clean**

```bash
python -c "from orcapod.core.sources import PostgreSQLTableSource; print(PostgreSQLTableSource)"
```

Expected: `<class 'orcapod.core.sources.postgresql_table_source.PostgreSQLTableSource'>`

- [ ] **Step 6.3: Update the Linear issue status to In Review**

```python
# Using the MCP tool (not a shell command):
# mcp__claude_ai_Linear__save_issue(id="PLT-1072", state="In Review")
```

- [ ] **Step 6.4: Create a PR against the `dev` branch**

```bash
gh pr create \
  --base dev \
  --title "feat(sources): implement PostgreSQLTableSource (PLT-1072)" \
  --body "$(cat <<'EOF'
## Summary

- Implements `PostgreSQLTableSource` as a thin subclass of `DBTableSource`
- PK columns used as default tag columns; explicit `tag_columns` override supported
- Connector opened and closed eagerly at construction time (all data loaded into memory)
- `to_config` / `from_config` round-trip serialisation
- Registered in `_build_source_registry()` under `"postgresql_table"`
- 35+ unit tests (no live DB required) + integration tests (`@pytest.mark.postgres`)

Closes PLT-1072

## Test plan
- [ ] All unit tests pass: `pytest tests/test_core/sources/test_postgresql_table_source.py -v`
- [ ] Full suite passes (no regressions): `pytest tests/ -m "not postgres" --ignore=...`
- [ ] Integration tests pass against live PG: `pytest -m postgres tests/test_core/sources/test_postgresql_table_source_integration.py`

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```
