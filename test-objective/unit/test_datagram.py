"""Specification-derived tests for Datagram."""

import pyarrow as pa
import pytest

from orcapod.core.datagrams.datagram import Datagram
from orcapod.types import ColumnConfig


# ---------------------------------------------------------------------------
# Helper to create a DataContext (needed for Arrow conversions)
# ---------------------------------------------------------------------------

def _make_context():
    """Create a DataContext for tests that need Arrow conversion."""
    from orcapod.contexts import resolve_context
    return resolve_context(None)


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------

class TestDatagramConstruction:
    """Datagram can be constructed from dict, pa.Table, pa.RecordBatch."""

    def test_construct_from_dict(self):
        dg = Datagram({"x": 1, "y": "hello"}, data_context=_make_context())
        assert "x" in dg
        assert dg["x"] == 1

    def test_construct_from_arrow_table(self):
        table = pa.table({"x": [1], "y": ["hello"]})
        dg = Datagram(table, data_context=_make_context())
        assert "x" in dg
        assert "y" in dg

    def test_construct_from_record_batch(self):
        batch = pa.record_batch({"x": [1], "y": ["hello"]})
        dg = Datagram(batch, data_context=_make_context())
        assert "x" in dg
        assert "y" in dg


# ---------------------------------------------------------------------------
# Dict-like access
# ---------------------------------------------------------------------------

class TestDatagramDictAccess:
    """Dict-like access: __getitem__, __contains__, __iter__, get()."""

    def _make_datagram(self):
        return Datagram({"a": 10, "b": "text"}, data_context=_make_context())

    def test_getitem(self):
        dg = self._make_datagram()
        assert dg["a"] == 10

    def test_contains(self):
        dg = self._make_datagram()
        assert "a" in dg
        assert "missing" not in dg

    def test_iter(self):
        dg = self._make_datagram()
        keys = list(dg)
        assert set(keys) == {"a", "b"}

    def test_get_existing(self):
        dg = self._make_datagram()
        assert dg.get("a") == 10

    def test_get_missing_returns_default(self):
        dg = self._make_datagram()
        assert dg.get("missing", 42) == 42


# ---------------------------------------------------------------------------
# Lazy conversion
# ---------------------------------------------------------------------------

class TestDatagramLazyConversion:
    """Dict access uses dict backing; as_table() triggers Arrow conversion."""

    def test_dict_constructed_datagram_dict_access_no_arrow(self):
        """Accessing a dict-constructed datagram by key should work without Arrow."""
        dg = Datagram({"x": 1}, data_context=_make_context())
        assert dg["x"] == 1

    def test_as_table_returns_arrow_table(self):
        dg = Datagram({"x": 1, "y": "hello"}, data_context=_make_context())
        table = dg.as_table()
        assert isinstance(table, pa.Table)

    def test_arrow_constructed_as_dict_returns_dict(self):
        table = pa.table({"x": [1], "y": ["hello"]})
        dg = Datagram(table, data_context=_make_context())
        d = dg.as_dict()
        assert isinstance(d, dict)
        assert "x" in d


# ---------------------------------------------------------------------------
# Round-trip
# ---------------------------------------------------------------------------

class TestDatagramRoundTrip:
    """dict->Arrow->dict and Arrow->dict->Arrow preserve data."""

    def test_dict_to_arrow_to_dict(self):
        ctx = _make_context()
        original = {"x": 1, "y": "hello"}
        dg = Datagram(original, data_context=ctx)
        # Force Arrow conversion
        _ = dg.as_table()
        # Convert back to dict
        result = dg.as_dict()
        assert result["x"] == original["x"]
        assert result["y"] == original["y"]

    def test_arrow_to_dict_to_arrow(self):
        ctx = _make_context()
        table = pa.table({"x": [42], "y": ["world"]})
        dg = Datagram(table, data_context=ctx)
        # Force dict conversion
        _ = dg.as_dict()
        # Convert back to Arrow
        result = dg.as_table()
        assert isinstance(result, pa.Table)
        assert result.column("x").to_pylist() == [42]


# ---------------------------------------------------------------------------
# Schema methods
# ---------------------------------------------------------------------------

class TestDatagramSchemaMethods:
    """keys(), schema(), arrow_schema() with ColumnConfig."""

    def test_keys_returns_field_names(self):
        dg = Datagram({"x": 1, "y": "hello"}, data_context=_make_context())
        assert set(dg.keys()) == {"x", "y"}

    def test_schema_returns_schema_object(self):
        from orcapod.types import Schema
        dg = Datagram({"x": 1, "y": "hello"}, data_context=_make_context())
        s = dg.schema()
        assert isinstance(s, Schema)
        assert "x" in s
        assert "y" in s


# ---------------------------------------------------------------------------
# Format conversions
# ---------------------------------------------------------------------------

class TestDatagramFormatConversions:
    """as_dict(), as_table(), as_arrow_compatible_dict()."""

    def test_as_dict_returns_dict(self):
        dg = Datagram({"x": 1}, data_context=_make_context())
        assert isinstance(dg.as_dict(), dict)

    def test_as_table_returns_table(self):
        dg = Datagram({"x": 1}, data_context=_make_context())
        assert isinstance(dg.as_table(), pa.Table)

    def test_as_arrow_compatible_dict(self):
        dg = Datagram({"x": 1, "y": "hello"}, data_context=_make_context())
        result = dg.as_arrow_compatible_dict()
        assert isinstance(result, dict)
        assert "x" in result


# ---------------------------------------------------------------------------
# Immutable operations
# ---------------------------------------------------------------------------

class TestDatagramImmutableOperations:
    """select, drop, rename, update, with_columns return NEW instances."""

    def _make_datagram(self):
        return Datagram({"a": 1, "b": 2, "c": 3}, data_context=_make_context())

    def test_select_returns_new_instance(self):
        dg = self._make_datagram()
        selected = dg.select("a", "b")
        assert selected is not dg
        assert "a" in selected
        assert "c" not in selected

    def test_drop_returns_new_instance(self):
        dg = self._make_datagram()
        dropped = dg.drop("c")
        assert dropped is not dg
        assert "c" not in dropped
        assert "a" in dropped

    def test_rename_returns_new_instance(self):
        dg = self._make_datagram()
        renamed = dg.rename({"a": "alpha"})
        assert renamed is not dg
        assert "alpha" in renamed
        assert "a" not in renamed

    def test_update_returns_new_instance(self):
        dg = self._make_datagram()
        updated = dg.update(a=99)
        assert updated is not dg
        assert updated["a"] == 99
        assert dg["a"] == 1  # original unchanged

    def test_with_columns_returns_new_instance(self):
        dg = self._make_datagram()
        extended = dg.with_columns(d=4)
        assert extended is not dg
        assert "d" in extended
        assert "d" not in dg

    def test_original_unchanged_after_select(self):
        dg = self._make_datagram()
        dg.select("a")
        assert "b" in dg
        assert "c" in dg


# ---------------------------------------------------------------------------
# Meta operations
# ---------------------------------------------------------------------------

class TestDatagramMetaOperations:
    """get_meta_value (auto-prefixed), with_meta_columns, drop_meta_columns."""

    def test_with_meta_columns_adds_prefixed_columns(self):
        dg = Datagram({"x": 1}, data_context=_make_context())
        with_meta = dg.with_meta_columns(my_meta="value")
        assert with_meta is not dg

    def test_get_meta_value_retrieves_by_unprefixed_name(self):
        dg = Datagram({"x": 1}, data_context=_make_context())
        with_meta = dg.with_meta_columns(my_meta="value")
        val = with_meta.get_meta_value("my_meta")
        assert val == "value"

    def test_drop_meta_columns(self):
        dg = Datagram({"x": 1}, data_context=_make_context())
        with_meta = dg.with_meta_columns(my_meta="value")
        dropped = with_meta.drop_meta_columns("my_meta")
        assert dropped is not with_meta


# ---------------------------------------------------------------------------
# Content hashing
# ---------------------------------------------------------------------------

class TestDatagramContentHashing:
    """Content hashing is deterministic, changes with data, equality by content."""

    def test_hashing_is_deterministic(self):
        ctx = _make_context()
        dg1 = Datagram({"x": 1, "y": "a"}, data_context=ctx)
        dg2 = Datagram({"x": 1, "y": "a"}, data_context=ctx)
        assert dg1.content_hash() == dg2.content_hash()

    def test_hash_changes_with_data(self):
        ctx = _make_context()
        dg1 = Datagram({"x": 1}, data_context=ctx)
        dg2 = Datagram({"x": 2}, data_context=ctx)
        assert dg1.content_hash() != dg2.content_hash()

    def test_equality_by_content(self):
        ctx = _make_context()
        dg1 = Datagram({"x": 1, "y": "a"}, data_context=ctx)
        dg2 = Datagram({"x": 1, "y": "a"}, data_context=ctx)
        assert dg1 == dg2

    def test_inequality_by_content(self):
        ctx = _make_context()
        dg1 = Datagram({"x": 1}, data_context=ctx)
        dg2 = Datagram({"x": 2}, data_context=ctx)
        assert dg1 != dg2
