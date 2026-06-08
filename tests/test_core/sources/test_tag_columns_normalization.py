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
