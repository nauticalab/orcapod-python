# `tag_columns` Bare-String Normalization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a shared `_normalize_column_list()` helper and apply it in every user-facing source constructor so that `tag_columns="session_id"` is treated identically to `tag_columns=["session_id"]`.

**Architecture:** A private helper in `schema_utils.py` normalizes bare strings to single-element lists and rejects non-string, non-iterable inputs with a `TypeError`. Normalization is applied only at user-facing source `__init__` methods — `SourceStreamBuilder.build()` is an internal contract and is not changed.

**Tech Stack:** Python 3.11+, PyArrow, Polars (DataFrameSource only), pytest, uv

---

## File Map

| Action | File | Change |
|---|---|---|
| Modify | `src/orcapod/utils/schema_utils.py` | Add `_normalize_column_list()` |
| Modify | `src/orcapod/core/sources/data_frame_source.py` | Replace inline str-check with helper |
| Modify | `src/orcapod/core/sources/arrow_table_source.py` | Add normalization + update type hint |
| Modify | `src/orcapod/core/sources/csv_source.py` | Add normalization + update type hint |
| Modify | `src/orcapod/core/sources/dict_source.py` | Add normalization + update type hint |
| Modify | `src/orcapod/core/sources/delta_table_source.py` | Add normalization + update type hint |
| Modify | `src/orcapod/core/sources/db_table_source.py` | Replace `list(tag_columns)` + update type hint |
| Modify | `src/orcapod/core/sources/sqlite_table_source.py` | Replace `list(tag_columns)` + update type hint |
| Modify | `src/orcapod/core/sources/spiraldb_table_source.py` | Replace `list(tag_columns)` + update type hint |
| Modify | `src/orcapod/core/sources/postgresql_table_source.py` | Update type hint only |
| Create | `tests/test_core/sources/test_tag_columns_normalization.py` | All normalization tests |

---

## Task 1: `_normalize_column_list` helper

**Files:**
- Modify: `src/orcapod/utils/schema_utils.py`
- Test: `tests/test_core/sources/test_tag_columns_normalization.py`

- [ ] **Step 1: Create the test file with failing helper unit tests**

Create `tests/test_core/sources/test_tag_columns_normalization.py` with this content:

```python
"""Tests for tag_columns bare-string normalization (ENG-571).

Covers:
- _normalize_column_list: bare string, list, tuple, empty, invalid inputs
- Per-source integration: all user-facing sources accept bare string identically
  to a single-element list.
"""
from __future__ import annotations

import re
import sqlite3
from collections.abc import Iterator
from typing import Any

import pyarrow as pa
import pytest

from orcapod.utils.schema_utils import _normalize_column_list


# ---------------------------------------------------------------------------
# Helper unit tests
# ---------------------------------------------------------------------------


class TestNormalizeColumnList:
    def test_bare_string_returns_single_element_list(self):
        assert _normalize_column_list("session_id") == ["session_id"]

    def test_single_element_list_unchanged(self):
        assert _normalize_column_list(["session_id"]) == ["session_id"]

    def test_multi_element_list_unchanged(self):
        assert _normalize_column_list(["a", "b", "c"]) == ["a", "b", "c"]

    def test_tuple_returns_list(self):
        assert _normalize_column_list(("a", "b")) == ["a", "b"]

    def test_empty_list_returns_empty_list(self):
        assert _normalize_column_list([]) == []

    def test_integer_raises_type_error(self):
        with pytest.raises(TypeError, match="tag_columns must be a string or iterable"):
            _normalize_column_list(42)

    def test_float_raises_type_error(self):
        with pytest.raises(TypeError, match="tag_columns must be a string or iterable"):
            _normalize_column_list(3.14)

    def test_list_with_non_string_element_raises_type_error(self):
        with pytest.raises(TypeError, match="All tag_columns elements must be strings"):
            _normalize_column_list([1, "b"])

    def test_list_with_all_non_string_elements_raises_type_error(self):
        with pytest.raises(TypeError, match="All tag_columns elements must be strings"):
            _normalize_column_list([1, 2, 3])
```

- [ ] **Step 2: Run helper tests — expect ImportError / AttributeError (function not yet defined)**

```bash
cd orcapod-python && uv run pytest tests/test_core/sources/test_tag_columns_normalization.py::TestNormalizeColumnList -v
```

Expected: all 9 tests fail with `ImportError: cannot import name '_normalize_column_list'`.

- [ ] **Step 3: Add `_normalize_column_list` to `schema_utils.py`**

At the bottom of `src/orcapod/utils/schema_utils.py`, append:

```python
def _normalize_column_list(value: Any) -> list[str]:
    """Normalize a column-list argument to a plain list of strings.

    Accepts a bare string (wraps it in a list), any iterable of strings
    (converts to list), or raises ``TypeError`` for non-string non-iterable
    inputs or iterables that contain non-string elements.

    Args:
        value: A single column name (``str``) or an iterable of column names.

    Returns:
        A list of column name strings.

    Raises:
        TypeError: If ``value`` is not a ``str`` or iterable, or if any
            element of the iterable is not a ``str``.
    """
    if isinstance(value, str):
        return [value]
    try:
        result = list(value)
    except TypeError:
        raise TypeError(
            f"tag_columns must be a string or iterable of strings, "
            f"got {type(value).__name__!r}"
        )
    bad = [x for x in result if not isinstance(x, str)]
    if bad:
        raise TypeError(
            f"All tag_columns elements must be strings; "
            f"got {[type(x).__name__ for x in bad]!r}"
        )
    return result
```

(`Any` is already imported at the top of `schema_utils.py` — no new import needed.)

- [ ] **Step 4: Run helper tests — expect all to pass**

```bash
uv run pytest tests/test_core/sources/test_tag_columns_normalization.py::TestNormalizeColumnList -v
```

Expected: 9 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/utils/schema_utils.py tests/test_core/sources/test_tag_columns_normalization.py
git commit -m "feat(sources): add _normalize_column_list helper for tag_columns normalization"
```

---

## Task 2: Integration tests for all source types

**Files:**
- Modify: `tests/test_core/sources/test_tag_columns_normalization.py`

Add the integration tests below to the existing test file. These test that each user-facing source accepts `tag_columns="col_name"` identically to `tag_columns=["col_name"]`. Most will **fail** until the source fixes land in Tasks 3–5 — that is expected.

- [ ] **Step 1: Append integration tests to `test_tag_columns_normalization.py`**

Add after the `TestNormalizeColumnList` class:

```python
# ---------------------------------------------------------------------------
# Shared Arrow table fixture
# ---------------------------------------------------------------------------


def _make_table() -> pa.Table:
    """Two-column table: session_id (tag candidate) + value (data)."""
    return pa.table(
        {
            "session_id": pa.array(["s1", "s2"], type=pa.large_string()),
            "value": pa.array([10, 20], type=pa.int64()),
        }
    )


# ---------------------------------------------------------------------------
# ArrowTableSource
# ---------------------------------------------------------------------------


class TestArrowTableSourceTagColumns:
    def test_bare_string_same_as_list(self):
        from orcapod.core.sources import ArrowTableSource

        t = _make_table()
        src_str = ArrowTableSource(table=t, tag_columns="session_id", infer_nullable=True)
        src_list = ArrowTableSource(table=t, tag_columns=["session_id"], infer_nullable=True)
        tag_keys_str, _ = src_str.keys()
        tag_keys_list, _ = src_list.keys()
        assert set(tag_keys_str) == set(tag_keys_list) == {"session_id"}

    def test_tuple_accepted(self):
        from orcapod.core.sources import ArrowTableSource

        t = _make_table()
        src = ArrowTableSource(table=t, tag_columns=("session_id",), infer_nullable=True)
        tag_keys, _ = src.keys()
        assert "session_id" in tag_keys

    def test_invalid_type_raises(self):
        from orcapod.core.sources import ArrowTableSource

        with pytest.raises(TypeError):
            ArrowTableSource(table=_make_table(), tag_columns=42, infer_nullable=True)


# ---------------------------------------------------------------------------
# DataFrameSource
# ---------------------------------------------------------------------------


class TestDataFrameSourceTagColumns:
    def test_bare_string_same_as_list(self):
        from orcapod.core.sources import DataFrameSource

        data = {"session_id": ["s1", "s2"], "value": [10, 20]}
        src_str = DataFrameSource(data=data, tag_columns="session_id")
        src_list = DataFrameSource(data=data, tag_columns=["session_id"])
        tag_keys_str, _ = src_str.keys()
        tag_keys_list, _ = src_list.keys()
        assert set(tag_keys_str) == set(tag_keys_list) == {"session_id"}

    def test_tuple_accepted(self):
        from orcapod.core.sources import DataFrameSource

        src = DataFrameSource(
            data={"session_id": ["s1"], "value": [10]},
            tag_columns=("session_id",),
        )
        tag_keys, _ = src.keys()
        assert "session_id" in tag_keys

    def test_invalid_type_raises(self):
        from orcapod.core.sources import DataFrameSource

        with pytest.raises(TypeError):
            DataFrameSource(data={"session_id": ["s1"], "value": [1]}, tag_columns=42)


# ---------------------------------------------------------------------------
# CSVSource
# ---------------------------------------------------------------------------


class TestCSVSourceTagColumns:
    def test_bare_string_same_as_list(self, tmp_path):
        from orcapod.core.sources import CSVSource

        p = tmp_path / "data.csv"
        p.write_text("session_id,value\ns1,10\ns2,20\n")
        src_str = CSVSource(file_path=str(p), tag_columns="session_id")
        src_list = CSVSource(file_path=str(p), tag_columns=["session_id"])
        tag_keys_str, _ = src_str.keys()
        tag_keys_list, _ = src_list.keys()
        assert set(tag_keys_str) == set(tag_keys_list) == {"session_id"}

    def test_tuple_accepted(self, tmp_path):
        from orcapod.core.sources import CSVSource

        p = tmp_path / "data.csv"
        p.write_text("session_id,value\ns1,10\n")
        src = CSVSource(file_path=str(p), tag_columns=("session_id",))
        tag_keys, _ = src.keys()
        assert "session_id" in tag_keys

    def test_invalid_type_raises(self, tmp_path):
        from orcapod.core.sources import CSVSource

        p = tmp_path / "data.csv"
        p.write_text("session_id,value\ns1,10\n")
        with pytest.raises(TypeError):
            CSVSource(file_path=str(p), tag_columns=42)


# ---------------------------------------------------------------------------
# DictSource
# ---------------------------------------------------------------------------


class TestDictSourceTagColumns:
    _DATA = [{"session_id": "s1", "value": 10}, {"session_id": "s2", "value": 20}]

    def test_bare_string_same_as_list(self):
        from orcapod.core.sources import DictSource

        src_str = DictSource(data=self._DATA, tag_columns="session_id")
        src_list = DictSource(data=self._DATA, tag_columns=["session_id"])
        tag_keys_str, _ = src_str.keys()
        tag_keys_list, _ = src_list.keys()
        assert set(tag_keys_str) == set(tag_keys_list) == {"session_id"}

    def test_tuple_accepted(self):
        from orcapod.core.sources import DictSource

        src = DictSource(data=self._DATA, tag_columns=("session_id",))
        tag_keys, _ = src.keys()
        assert "session_id" in tag_keys

    def test_invalid_type_raises(self):
        from orcapod.core.sources import DictSource

        with pytest.raises(TypeError):
            DictSource(data=self._DATA, tag_columns=42)


# ---------------------------------------------------------------------------
# DeltaTableSource (skipped when deltalake not installed)
# ---------------------------------------------------------------------------


class TestDeltaTableSourceTagColumns:
    @pytest.fixture
    def delta_path(self, tmp_path):
        deltalake = pytest.importorskip("deltalake")
        t = pa.table(
            {
                "session_id": pa.array(["s1", "s2"], type=pa.large_string()),
                "value": pa.array([10, 20], type=pa.int64()),
            }
        )
        dest = tmp_path / "delta"
        deltalake.write_deltalake(str(dest), t)
        return dest

    def test_bare_string_same_as_list(self, delta_path):
        from orcapod.core.sources import DeltaTableSource

        src_str = DeltaTableSource(delta_table_path=delta_path, tag_columns="session_id")
        src_list = DeltaTableSource(delta_table_path=delta_path, tag_columns=["session_id"])
        tag_keys_str, _ = src_str.keys()
        tag_keys_list, _ = src_list.keys()
        assert set(tag_keys_str) == set(tag_keys_list) == {"session_id"}

    def test_invalid_type_raises(self, delta_path):
        from orcapod.core.sources import DeltaTableSource

        with pytest.raises(TypeError):
            DeltaTableSource(delta_table_path=delta_path, tag_columns=42)


# ---------------------------------------------------------------------------
# DBTableSource (via MockDBConnector — no external DB required)
# ---------------------------------------------------------------------------


class _MockConnector:
    """Minimal in-memory DBConnectorProtocol for normalization tests."""

    _TABLE = pa.table(
        {
            "session_id": pa.array(["s1", "s2"], type=pa.large_string()),
            "value": pa.array([10, 20], type=pa.int64()),
        }
    )

    def get_table_names(self) -> list[str]:
        return ["events"]

    def get_pk_columns(self, table_name: str) -> list[str]:
        return ["session_id"]

    def get_column_info(self, table_name: str):
        from orcapod.types import ColumnInfo
        return [ColumnInfo(name=f.name, arrow_type=f.type) for f in self._TABLE.schema]

    def iter_batches(self, query: str, params: Any = None, batch_size: int = 1000) -> Iterator[pa.RecordBatch]:
        m = re.search(r'FROM\s+"?(\w+)"?', query, re.IGNORECASE)
        if m and m.group(1) == "events":
            yield from self._TABLE.to_batches()

    def create_table_if_not_exists(self, *a: Any, **kw: Any) -> None:
        pass

    def upsert_records(self, *a: Any, **kw: Any) -> None:
        pass

    def close(self) -> None:
        pass


class TestDBTableSourceTagColumns:
    def test_bare_string_same_as_list(self):
        from orcapod.core.sources import DBTableSource

        src_str = DBTableSource(
            connector=_MockConnector(), table_name="events", tag_columns="session_id"
        )
        src_list = DBTableSource(
            connector=_MockConnector(), table_name="events", tag_columns=["session_id"]
        )
        tag_keys_str, _ = src_str.keys()
        tag_keys_list, _ = src_list.keys()
        assert set(tag_keys_str) == set(tag_keys_list) == {"session_id"}

    def test_tuple_accepted(self):
        from orcapod.core.sources import DBTableSource

        src = DBTableSource(
            connector=_MockConnector(), table_name="events", tag_columns=("session_id",)
        )
        tag_keys, _ = src.keys()
        assert "session_id" in tag_keys

    def test_invalid_type_raises(self):
        from orcapod.core.sources import DBTableSource

        with pytest.raises(TypeError):
            DBTableSource(connector=_MockConnector(), table_name="events", tag_columns=42)


# ---------------------------------------------------------------------------
# SQLiteTableSource
# ---------------------------------------------------------------------------


@pytest.fixture
def sqlite_db_path(tmp_path):
    """SQLite DB with an 'events' table; session_id is the primary key."""
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "CREATE TABLE events (session_id TEXT PRIMARY KEY, value INTEGER NOT NULL)"
    )
    conn.executemany("INSERT INTO events VALUES (?, ?)", [("s1", 10), ("s2", 20)])
    conn.commit()
    conn.close()
    return str(db_path)


class TestSQLiteTableSourceTagColumns:
    def test_bare_string_same_as_list(self, sqlite_db_path):
        from orcapod.core.sources import SQLiteTableSource

        src_str = SQLiteTableSource(
            db_path=sqlite_db_path, table_name="events", tag_columns="session_id"
        )
        src_list = SQLiteTableSource(
            db_path=sqlite_db_path, table_name="events", tag_columns=["session_id"]
        )
        tag_keys_str, _ = src_str.keys()
        tag_keys_list, _ = src_list.keys()
        assert set(tag_keys_str) == set(tag_keys_list) == {"session_id"}

    def test_tuple_accepted(self, sqlite_db_path):
        from orcapod.core.sources import SQLiteTableSource

        src = SQLiteTableSource(
            db_path=sqlite_db_path, table_name="events", tag_columns=("session_id",)
        )
        tag_keys, _ = src.keys()
        assert "session_id" in tag_keys

    def test_invalid_type_raises(self, sqlite_db_path):
        from orcapod.core.sources import SQLiteTableSource

        with pytest.raises(TypeError):
            SQLiteTableSource(
                db_path=sqlite_db_path, table_name="events", tag_columns=42
            )
```

- [ ] **Step 2: Run integration tests — expect DataFrameSource to pass, all others to fail**

```bash
uv run pytest tests/test_core/sources/test_tag_columns_normalization.py -v
```

Expected:
- `TestNormalizeColumnList` — all 9 PASS (from Task 1)
- `TestDataFrameSourceTagColumns` — all 3 PASS (already has inline fix)
- All other `Test*TagColumns` classes — FAIL (bare string iterates characters)

- [ ] **Step 3: Commit the test file**

```bash
git add tests/test_core/sources/test_tag_columns_normalization.py
git commit -m "test(sources): add tag_columns normalization integration tests (red)"
```

---

## Task 3: Fix `DataFrameSource` — replace inline check with helper

**Files:**
- Modify: `src/orcapod/core/sources/data_frame_source.py`

The current inline check (lines 54–56) works but duplicates logic we now have in the helper. This task refactors it.

- [ ] **Step 1: Add import and replace inline check in `data_frame_source.py`**

At the top of `src/orcapod/core/sources/data_frame_source.py`, the existing imports include:

```python
from orcapod.utils import arrow_utils, polars_data_utils
```

Add the helper import on a new line after that:

```python
from orcapod.utils.schema_utils import _normalize_column_list
```

Then replace lines 54–56 (the inline str check):

```python
        if isinstance(tag_columns, str):
            tag_columns = [tag_columns]
        tag_columns = list(tag_columns)
```

with:

```python
        tag_columns = _normalize_column_list(tag_columns)
```

The `tag_columns` parameter type annotation in the `__init__` signature is already `str | Collection[str]` — no change needed there.

- [ ] **Step 2: Run DataFrameSource tests — expect all to pass**

```bash
uv run pytest tests/test_core/sources/test_tag_columns_normalization.py::TestDataFrameSourceTagColumns -v
```

Expected: 3 PASS (behavior unchanged — this is a pure refactor).

- [ ] **Step 3: Run broader DataFrameSource tests to check for regressions**

```bash
uv run pytest tests/test_core/sources/test_sources_comprehensive.py -v -k "DataFrameSource"
```

Expected: all existing DataFrameSource tests PASS.

- [ ] **Step 4: Commit**

```bash
git add src/orcapod/core/sources/data_frame_source.py
git commit -m "refactor(sources): use _normalize_column_list in DataFrameSource"
```

---

## Task 4: Fix direct-pass sources — `ArrowTableSource`, `CSVSource`, `DictSource`, `DeltaTableSource`

These sources pass `tag_columns` directly to `builder.build()` without any `list()` call. The fix is to normalize at the top of `__init__`.

**Files:**
- Modify: `src/orcapod/core/sources/arrow_table_source.py`
- Modify: `src/orcapod/core/sources/csv_source.py`
- Modify: `src/orcapod/core/sources/dict_source.py`
- Modify: `src/orcapod/core/sources/delta_table_source.py`

### `ArrowTableSource`

- [ ] **Step 1: Update `arrow_table_source.py`**

Add import after the existing `from orcapod.utils import arrow_utils` line:

```python
from orcapod.utils.schema_utils import _normalize_column_list
```

Update the `tag_columns` parameter type annotation in `__init__` from `Collection[str]` to `str | Collection[str]`:

```python
    def __init__(
        self,
        table: "pa.Table",
        tag_columns: str | Collection[str] = (),
        system_tag_columns: Collection[str] = (),
        record_id_column: str | None = None,
        infer_nullable: bool = False,
        **kwargs: Any,
    ) -> None:
```

Add normalization immediately after `super().__init__(**kwargs)`, before the `if infer_nullable:` block:

```python
        tag_columns = _normalize_column_list(tag_columns)
```

### `CSVSource`

- [ ] **Step 2: Update `csv_source.py`**

Add import after the existing `from orcapod.utils import arrow_utils` line:

```python
from orcapod.utils.schema_utils import _normalize_column_list
```

Update the `tag_columns` type annotation:

```python
    def __init__(
        self,
        file_path: str,
        tag_columns: str | Collection[str] = (),
        system_tag_columns: Collection[str] = (),
        record_id_column: str | None = None,
        source_id: str | None = None,
        schema: pa.Schema | None = None,
        **kwargs: Any,
    ) -> None:
```

Add normalization as the first line after `super().__init__(...)`:

```python
        tag_columns = _normalize_column_list(tag_columns)
```

(Place it right after the `super().__init__(source_id=source_id, **kwargs)` call, before the `self._file_path = file_path` line.)

### `DictSource`

- [ ] **Step 3: Update `dict_source.py`**

Add import after the existing `from orcapod.utils import arrow_utils` line:

```python
from orcapod.utils.schema_utils import _normalize_column_list
```

Update the `tag_columns` type annotation:

```python
    def __init__(
        self,
        data: Collection[Mapping[str, DataValue]],
        tag_columns: str | Collection[str] = (),
        system_tag_columns: Collection[str] = (),
        data_schema: SchemaLike | pa.Schema | None = None,
        source_id: str | None = None,
        **kwargs: Any,
    ) -> None:
```

Add normalization immediately after `super().__init__(source_id=source_id, **kwargs)`, before the `if isinstance(data_schema, pa.Schema):` block:

```python
        tag_columns = _normalize_column_list(tag_columns)
```

### `DeltaTableSource`

- [ ] **Step 4: Update `delta_table_source.py`**

Add a new import for the helper. The file currently has no `orcapod.utils` imports. Add after the last `from orcapod...` import line:

```python
from orcapod.utils.schema_utils import _normalize_column_list
```

Update the `tag_columns` type annotation:

```python
    def __init__(
        self,
        delta_table_path: PathLike,
        tag_columns: str | Collection[str] = (),
        system_tag_columns: Collection[str] = (),
        record_id_column: str | None = None,
        source_id: str | None = None,
        **kwargs: Any,
    ) -> None:
```

Add normalization as the first line after the `super().__init__(source_id=source_id, **kwargs)` call:

```python
        tag_columns = _normalize_column_list(tag_columns)
```

(Place it right before the `self._delta_table_path = resolved` assignment.)

- [ ] **Step 5: Run integration tests for these four sources — expect all to pass now**

```bash
uv run pytest tests/test_core/sources/test_tag_columns_normalization.py::TestArrowTableSourceTagColumns tests/test_core/sources/test_tag_columns_normalization.py::TestCSVSourceTagColumns tests/test_core/sources/test_tag_columns_normalization.py::TestDictSourceTagColumns tests/test_core/sources/test_tag_columns_normalization.py::TestDeltaTableSourceTagColumns -v
```

Expected: all tests PASS (DeltaTableSource tests skip if `deltalake` not installed — that's fine).

- [ ] **Step 6: Run existing source tests to check for regressions**

```bash
uv run pytest tests/test_core/sources/test_sources_comprehensive.py tests/test_core/sources/test_sources.py -v
```

Expected: all existing tests PASS.

- [ ] **Step 7: Commit**

```bash
git add src/orcapod/core/sources/arrow_table_source.py \
        src/orcapod/core/sources/csv_source.py \
        src/orcapod/core/sources/dict_source.py \
        src/orcapod/core/sources/delta_table_source.py
git commit -m "feat(sources): normalize tag_columns in ArrowTableSource, CSVSource, DictSource, DeltaTableSource"
```

---

## Task 5: Fix pre-processed sources — `DBTableSource`, `PostgreSQLTableSource`, `SQLiteTableSource`, `SpiralDBTableSource`

These sources call `list(tag_columns)` *before* delegating to the builder. Replace each with `_normalize_column_list(tag_columns)`.

**Files:**
- Modify: `src/orcapod/core/sources/db_table_source.py`
- Modify: `src/orcapod/core/sources/postgresql_table_source.py`
- Modify: `src/orcapod/core/sources/sqlite_table_source.py`
- Modify: `src/orcapod/core/sources/spiraldb_table_source.py`

### `DBTableSource`

- [ ] **Step 1: Update `db_table_source.py`**

Add import after `from orcapod.utils import arrow_utils`:

```python
from orcapod.utils.schema_utils import _normalize_column_list
```

Update the `tag_columns` type annotation in `__init__`:

```python
    def __init__(
        self,
        connector: DBConnectorProtocol,
        table_name: str,
        tag_columns: str | Collection[str] | None = None,
        system_tag_columns: Collection[str] = (),
        record_id_column: str | None = None,
        source_id: str | None = None,
        label: str | None = None,
        data_context: str | contexts.DataContext | None = None,
        config: OrcapodConfig | None = None,
        *,
        _query: str | None = None,
    ) -> None:
```

In Step 2 of `__init__` (resolving tag columns), replace:

```python
        else:
            resolved_tag_columns = list(tag_columns)
```

with:

```python
        else:
            resolved_tag_columns = _normalize_column_list(tag_columns)
```

### `PostgreSQLTableSource`

- [ ] **Step 2: Update `postgresql_table_source.py` — type hint only**

Update the `tag_columns` type annotation in `__init__`. No logic change:

```python
    def __init__(
        self,
        dsn: str,
        table_name: str,
        tag_columns: str | Collection[str] | None = None,
        system_tag_columns: Collection[str] = (),
        record_id_column: str | None = None,
        source_id: str | None = None,
        label: str | None = None,
        data_context: str | contexts.DataContext | None = None,
        config: OrcapodConfig | None = None,
    ) -> None:
```

Also update the `tag_columns` type in the docstring `Args` section from `Collection[str] | None` to `str | Collection[str] | None`.

### `SQLiteTableSource`

- [ ] **Step 3: Update `sqlite_table_source.py`**

Add import. The file currently has no `orcapod.utils` imports. Add after the last `from orcapod...` import line:

```python
from orcapod.utils.schema_utils import _normalize_column_list
```

Update the `tag_columns` type annotation:

```python
    def __init__(
        self,
        db_path: str | os.PathLike,
        table_name: str,
        tag_columns: str | Collection[str] | None = None,
        system_tag_columns: Collection[str] = (),
        record_id_column: str | None = None,
        source_id: str | None = None,
        label: str | None = None,
        data_context: str | contexts.DataContext | None = None,
        config: "OrcapodConfig | None" = None,
    ) -> None:
```

In Step 3 (resolving tag columns), replace:

```python
            else:
                resolved_tags = list(tag_columns)
```

with:

```python
            else:
                resolved_tags = _normalize_column_list(tag_columns)
```

### `SpiralDBTableSource`

- [ ] **Step 4: Update `spiraldb_table_source.py`**

Add import. The file has no `orcapod.utils` imports. Add after the last `from orcapod...` import line:

```python
from orcapod.utils.schema_utils import _normalize_column_list
```

Update the `tag_columns` type annotation:

```python
    def __init__(
        self,
        project_id: str,
        table_name: str,
        dataset: str = "default",
        tag_columns: str | Collection[str] | None = None,
        system_tag_columns: Collection[str] = (),
        record_id_column: str | None = None,
        source_id: str | None = None,
        label: str | None = None,
        data_context: "str | contexts.DataContext | None" = None,
        config: "OrcapodConfig | None" = None,
        overrides: dict[str, str] | None = None,
    ) -> None:
```

Replace the `resolved_tags` assignment:

```python
            resolved_tags: list[str] | None = (
                list(tag_columns) if tag_columns is not None else None
            )
```

with:

```python
            resolved_tags: list[str] | None = (
                _normalize_column_list(tag_columns) if tag_columns is not None else None
            )
```

- [ ] **Step 5: Run DB source integration tests — expect all to pass**

```bash
uv run pytest tests/test_core/sources/test_tag_columns_normalization.py::TestDBTableSourceTagColumns tests/test_core/sources/test_tag_columns_normalization.py::TestSQLiteTableSourceTagColumns -v
```

Expected: all 6 tests PASS.

- [ ] **Step 6: Run existing DB source tests to check for regressions**

```bash
uv run pytest tests/test_core/sources/test_db_table_source.py tests/test_core/sources/test_sqlite_table_source.py -v
```

Expected: all existing tests PASS.

- [ ] **Step 7: Commit**

```bash
git add src/orcapod/core/sources/db_table_source.py \
        src/orcapod/core/sources/postgresql_table_source.py \
        src/orcapod/core/sources/sqlite_table_source.py \
        src/orcapod/core/sources/spiraldb_table_source.py
git commit -m "feat(sources): normalize tag_columns in DBTableSource, PostgreSQLTableSource, SQLiteTableSource, SpiralDBTableSource"
```

---

## Task 6: Full test suite verification

- [ ] **Step 1: Run the complete test suite (excluding PostgreSQL integration tests)**

```bash
uv run pytest tests/ -v --ignore=tests/test_core/sources/test_postgresql_table_source_integration.py -m "not postgres"
```

Expected: all tests PASS. No regressions anywhere.

- [ ] **Step 2: Run the full normalization test file to confirm all tests green**

```bash
uv run pytest tests/test_core/sources/test_tag_columns_normalization.py -v
```

Expected: all tests PASS (DeltaTableSource tests may skip if `deltalake` not installed — that is acceptable).
