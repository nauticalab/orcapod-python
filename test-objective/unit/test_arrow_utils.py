"""Tests for Arrow utility functions.

Specification-derived tests covering schema selection/dropping, type
normalization, row/column conversion, table stacking, schema compatibility
checking, and column group splitting.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.utils.arrow_utils import (
    check_arrow_schema_compatibility,
    hstack_tables,
    normalize_to_large_types,
    pydict_to_pylist,
    pylist_to_pydict,
    schema_drop,
    schema_select,
    split_by_column_groups,
)


# ===========================================================================
# schema_select
# ===========================================================================


class TestSchemaSelect:
    """Selects subset; KeyError for missing columns."""

    def test_select_subset(self) -> None:
        schema = pa.schema(
            [
                pa.field("a", pa.int64()),
                pa.field("b", pa.string()),
                pa.field("c", pa.float64()),
            ]
        )
        result = schema_select(schema, ["a", "c"])
        assert result.names == ["a", "c"]
        assert result.field("a").type == pa.int64()
        assert result.field("c").type == pa.float64()

    def test_select_all(self) -> None:
        schema = pa.schema([pa.field("x", pa.int64()), pa.field("y", pa.string())])
        result = schema_select(schema, ["x", "y"])
        assert result.names == ["x", "y"]

    def test_select_missing_column_raises(self) -> None:
        schema = pa.schema([pa.field("a", pa.int64())])
        with pytest.raises(KeyError, match="Missing columns"):
            schema_select(schema, ["a", "nonexistent"])

    def test_select_missing_with_ignore(self) -> None:
        schema = pa.schema([pa.field("a", pa.int64()), pa.field("b", pa.string())])
        result = schema_select(schema, ["a", "nonexistent"], ignore_missing=True)
        assert result.names == ["a"]


# ===========================================================================
# schema_drop
# ===========================================================================


class TestSchemaDrop:
    """Drops specified columns; KeyError if missing and not ignore_missing."""

    def test_drop_columns(self) -> None:
        schema = pa.schema(
            [
                pa.field("a", pa.int64()),
                pa.field("b", pa.string()),
                pa.field("c", pa.float64()),
            ]
        )
        result = schema_drop(schema, ["b"])
        assert result.names == ["a", "c"]

    def test_drop_missing_raises(self) -> None:
        schema = pa.schema([pa.field("a", pa.int64())])
        with pytest.raises(KeyError, match="Missing columns"):
            schema_drop(schema, ["nonexistent"])

    def test_drop_missing_with_ignore(self) -> None:
        schema = pa.schema([pa.field("a", pa.int64()), pa.field("b", pa.string())])
        result = schema_drop(schema, ["nonexistent"], ignore_missing=True)
        assert result.names == ["a", "b"]


# ===========================================================================
# normalize_to_large_types
# ===========================================================================


class TestNormalizeToLargeTypes:
    """string -> large_string, binary -> large_binary, list -> large_list."""

    def test_string_to_large_string(self) -> None:
        assert normalize_to_large_types(pa.string()) == pa.large_string()

    def test_binary_to_large_binary(self) -> None:
        assert normalize_to_large_types(pa.binary()) == pa.large_binary()

    def test_list_to_large_list(self) -> None:
        result = normalize_to_large_types(pa.list_(pa.string()))
        assert pa.types.is_large_list(result)
        # Inner type should also be normalized.
        assert result.value_type == pa.large_string()

    def test_large_string_unchanged(self) -> None:
        assert normalize_to_large_types(pa.large_string()) == pa.large_string()

    def test_int64_unchanged(self) -> None:
        assert normalize_to_large_types(pa.int64()) == pa.int64()

    def test_float64_unchanged(self) -> None:
        assert normalize_to_large_types(pa.float64()) == pa.float64()

    def test_nested_struct_normalized(self) -> None:
        struct_type = pa.struct([pa.field("name", pa.string())])
        result = normalize_to_large_types(struct_type)
        assert pa.types.is_struct(result)
        assert result[0].type == pa.large_string()

    def test_null_to_large_string(self) -> None:
        assert normalize_to_large_types(pa.null()) == pa.large_string()


# ===========================================================================
# pylist_to_pydict
# ===========================================================================


class TestPylistToPydict:
    """Row-oriented -> column-oriented conversion."""

    def test_basic_conversion(self) -> None:
        rows = [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
        result = pylist_to_pydict(rows)
        assert result == {"a": [1, 3], "b": [2, 4]}

    def test_missing_keys_filled_with_none(self) -> None:
        rows = [{"a": 1, "b": 2}, {"a": 3, "c": 4}]
        result = pylist_to_pydict(rows)
        assert result["a"] == [1, 3]
        assert result["b"] == [2, None]
        assert result["c"] == [None, 4]

    def test_empty_list(self) -> None:
        result = pylist_to_pydict([])
        assert result == {}

    def test_single_row(self) -> None:
        result = pylist_to_pydict([{"x": 10}])
        assert result == {"x": [10]}


# ===========================================================================
# pydict_to_pylist
# ===========================================================================


class TestPydictToPylist:
    """Column-oriented -> row-oriented; ValueError on inconsistent lengths."""

    def test_basic_conversion(self) -> None:
        data = {"a": [1, 3], "b": [2, 4]}
        result = pydict_to_pylist(data)
        assert result == [{"a": 1, "b": 2}, {"a": 3, "b": 4}]

    def test_empty_dict(self) -> None:
        result = pydict_to_pylist({})
        assert result == []

    def test_inconsistent_lengths_raises(self) -> None:
        data = {"a": [1, 2], "b": [3]}
        with pytest.raises(ValueError, match="Inconsistent"):
            pydict_to_pylist(data)

    def test_single_column(self) -> None:
        result = pydict_to_pylist({"x": [10, 20]})
        assert result == [{"x": 10}, {"x": 20}]


# ===========================================================================
# hstack_tables
# ===========================================================================


class TestHstackTables:
    """Horizontal concat; ValueError for different row counts or duplicate columns."""

    def test_basic_hstack(self) -> None:
        t1 = pa.table({"a": [1, 2]})
        t2 = pa.table({"b": ["x", "y"]})
        result = hstack_tables(t1, t2)
        assert result.column_names == ["a", "b"]
        assert result.num_rows == 2

    def test_single_table(self) -> None:
        t1 = pa.table({"a": [1]})
        result = hstack_tables(t1)
        assert result.column_names == ["a"]

    def test_different_row_counts_raises(self) -> None:
        t1 = pa.table({"a": [1, 2]})
        t2 = pa.table({"b": [3]})
        with pytest.raises(ValueError, match="same number of rows"):
            hstack_tables(t1, t2)

    def test_duplicate_columns_raises(self) -> None:
        t1 = pa.table({"a": [1]})
        t2 = pa.table({"a": [2]})
        with pytest.raises(ValueError, match="Duplicate column name"):
            hstack_tables(t1, t2)

    def test_no_tables_raises(self) -> None:
        with pytest.raises(ValueError, match="At least one table"):
            hstack_tables()

    def test_three_tables(self) -> None:
        t1 = pa.table({"a": [1]})
        t2 = pa.table({"b": [2]})
        t3 = pa.table({"c": [3]})
        result = hstack_tables(t1, t2, t3)
        assert result.column_names == ["a", "b", "c"]
        assert result.num_rows == 1


# ===========================================================================
# check_arrow_schema_compatibility
# ===========================================================================


class TestCheckArrowSchemaCompatibility:
    """Returns (is_compatible, errors)."""

    def test_compatible_schemas(self) -> None:
        s1 = pa.schema([pa.field("a", pa.int64()), pa.field("b", pa.string())])
        s2 = pa.schema([pa.field("a", pa.int64()), pa.field("b", pa.string())])
        is_compat, errors = check_arrow_schema_compatibility(s1, s2)
        assert is_compat is True
        assert errors == []

    def test_missing_field_incompatible(self) -> None:
        incoming = pa.schema([pa.field("a", pa.int64())])
        target = pa.schema([pa.field("a", pa.int64()), pa.field("b", pa.string())])
        is_compat, errors = check_arrow_schema_compatibility(incoming, target)
        assert is_compat is False
        assert any("Missing field" in e for e in errors)

    def test_type_mismatch(self) -> None:
        incoming = pa.schema([pa.field("a", pa.string())])
        target = pa.schema([pa.field("a", pa.int64())])
        is_compat, errors = check_arrow_schema_compatibility(incoming, target)
        assert is_compat is False
        assert any("Type mismatch" in e for e in errors)

    def test_extra_fields_allowed_non_strict(self) -> None:
        incoming = pa.schema(
            [pa.field("a", pa.int64()), pa.field("extra", pa.string())]
        )
        target = pa.schema([pa.field("a", pa.int64())])
        is_compat, errors = check_arrow_schema_compatibility(incoming, target)
        assert is_compat is True
        assert errors == []

    def test_extra_fields_rejected_strict(self) -> None:
        incoming = pa.schema(
            [pa.field("a", pa.int64()), pa.field("extra", pa.string())]
        )
        target = pa.schema([pa.field("a", pa.int64())])
        is_compat, errors = check_arrow_schema_compatibility(
            incoming, target, strict=True
        )
        assert is_compat is False
        assert any("Unexpected field" in e for e in errors)


# ===========================================================================
# split_by_column_groups
# ===========================================================================


class TestSplitByColumnGroups:
    """Splits table by column groups."""

    def test_basic_split(self) -> None:
        table = pa.table({"a": [1], "b": [2], "c": [3], "d": [4]})
        result = split_by_column_groups(table, ["a", "b"], ["c"])
        # result[0] = remaining (d), result[1] = group1 (a,b), result[2] = group2 (c)
        assert result[0] is not None
        assert result[0].column_names == ["d"]
        assert result[1] is not None
        assert set(result[1].column_names) == {"a", "b"}
        assert result[2] is not None
        assert result[2].column_names == ["c"]

    def test_no_groups_returns_full_table(self) -> None:
        table = pa.table({"a": [1], "b": [2]})
        result = split_by_column_groups(table)
        assert len(result) == 1
        assert result[0].column_names == ["a", "b"]

    def test_all_columns_in_groups(self) -> None:
        table = pa.table({"a": [1], "b": [2]})
        result = split_by_column_groups(table, ["a"], ["b"])
        # remaining should be None
        assert result[0] is None
        assert result[1] is not None
        assert result[2] is not None

    def test_empty_group_returns_none(self) -> None:
        table = pa.table({"a": [1], "b": [2]})
        result = split_by_column_groups(table, ["nonexistent"])
        # Group with nonexistent columns returns None
        assert result[1] is None
        # Remaining should have both columns
        assert result[0] is not None
        assert set(result[0].column_names) == {"a", "b"}
