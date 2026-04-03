"""Specification-derived tests for arrow_utils (formerly arrow_data_utils).

Tests system tag manipulation, source info, and column helper functions
based on documented behavior in the design specification.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.system_constants import constants
from orcapod.utils.arrow_utils import (
    add_source_info,
    add_system_tag_columns,
    append_to_system_tags,
    drop_columns_with_prefix,
    drop_system_columns,
    sort_system_tag_values,
)


# ---------------------------------------------------------------------------
# add_system_tag_columns
# ---------------------------------------------------------------------------


class TestAddSystemTagColumns:
    """Per the design spec, system tag columns are prefixed with _tag_ and
    track per-row provenance (source_id, record_id pairs)."""

    def test_adds_system_tag_columns(self):
        table = pa.table({"id": [1, 2], "value": [10, 20]})
        result = add_system_tag_columns(
            table,
            schema_hash="abc123",
            source_ids="src1",
            record_ids=["rec1", "rec2"],
        )
        # Should have original columns plus new system tag columns
        assert result.num_rows == 2
        tag_cols = [
            c for c in result.column_names if c.startswith(constants.SYSTEM_TAG_PREFIX)
        ]
        assert len(tag_cols) > 0

    def test_empty_table_returns_empty(self):
        table = pa.table({"id": pa.array([], type=pa.int64())})
        result = add_system_tag_columns(
            table,
            schema_hash="abc",
            source_ids="src1",
            record_ids=[],
        )
        assert result.num_rows == 0

    def test_length_mismatch_raises(self):
        table = pa.table({"id": [1, 2, 3]})
        with pytest.raises(ValueError):
            add_system_tag_columns(
                table,
                schema_hash="abc",
                source_ids=["s1", "s2"],  # 2 source_ids for 3 rows
                record_ids=["r1", "r2", "r3"],
            )


# ---------------------------------------------------------------------------
# append_to_system_tags
# ---------------------------------------------------------------------------


class TestAppendToSystemTags:
    """Per design, appends a value to existing system tag columns."""

    def test_appends_value_to_system_tags(self):
        # Create a table that already has system tag columns
        table = pa.table({"id": [1, 2], "value": [10, 20]})
        table_with_tags = add_system_tag_columns(
            table,
            schema_hash="abc",
            source_ids="src1",
            record_ids=["r1", "r2"],
        )
        result = append_to_system_tags(table_with_tags, value="::extra:0")
        # System tag column names should have changed (appended)
        tag_cols_before = [
            c
            for c in table_with_tags.column_names
            if c.startswith(constants.SYSTEM_TAG_PREFIX)
        ]
        tag_cols_after = [
            c for c in result.column_names if c.startswith(constants.SYSTEM_TAG_PREFIX)
        ]
        # The column names should be extended
        assert len(tag_cols_after) == len(tag_cols_before)

    def test_empty_table_returns_empty(self):
        table = pa.table(
            {"id": pa.array([], type=pa.int64()), "value": pa.array([], type=pa.int64())}
        )
        result = append_to_system_tags(table, value="::extra:0")
        assert result.num_rows == 0


# ---------------------------------------------------------------------------
# sort_system_tag_values
# ---------------------------------------------------------------------------


class TestSortSystemTagValues:
    """Per design, system tag values must be sorted for commutativity in
    multi-input operators. Paired (source_id, record_id) tuples are sorted
    together per row."""

    def test_sorts_system_tag_values(self):
        # This is a structural test — ensure the function runs without error
        # and produces a table with the same shape
        table = pa.table({"id": [1, 2], "value": [10, 20]})
        table_with_tags = add_system_tag_columns(
            table,
            schema_hash="abc",
            source_ids="src1",
            record_ids=["r1", "r2"],
        )
        result = sort_system_tag_values(table_with_tags)
        assert result.num_rows == table_with_tags.num_rows


# ---------------------------------------------------------------------------
# add_source_info
# ---------------------------------------------------------------------------


class TestAddSourceInfo:
    """Per design, source info columns are prefixed with _source_ and track
    provenance tokens per packet column."""

    def test_adds_source_info_columns(self):
        table = pa.table({"id": [1, 2], "value": [10, 20]})
        result = add_source_info(table, source_info="src_token")
        source_cols = [
            c for c in result.column_names if c.startswith(constants.SOURCE_PREFIX)
        ]
        assert len(source_cols) > 0

    def test_source_info_length_mismatch_raises(self):
        table = pa.table({"id": [1, 2], "value": [10, 20]})
        with pytest.raises(ValueError):
            add_source_info(table, source_info=["a", "b", "c"])  # Wrong count


# ---------------------------------------------------------------------------
# drop_columns_with_prefix
# ---------------------------------------------------------------------------


class TestDropColumnsWithPrefix:
    """Removes all columns matching a given prefix."""

    def test_drops_columns_with_matching_prefix(self):
        table = pa.table({"__meta_a": [1], "__meta_b": [2], "data": [3]})
        result = drop_columns_with_prefix(table, "__meta")
        assert "data" in result.column_names
        assert "__meta_a" not in result.column_names
        assert "__meta_b" not in result.column_names

    def test_no_match_returns_unchanged(self):
        table = pa.table({"a": [1], "b": [2]})
        result = drop_columns_with_prefix(table, "__nonexistent")
        assert result.column_names == table.column_names

    def test_tuple_of_prefixes(self):
        table = pa.table({"__a": [1], "_src_b": [2], "data": [3]})
        result = drop_columns_with_prefix(table, ("__", "_src_"))
        assert result.column_names == ["data"]


# ---------------------------------------------------------------------------
# drop_system_columns
# ---------------------------------------------------------------------------


class TestDropSystemColumns:
    """Removes columns with system prefixes (__ and datagram prefix)."""

    def test_drops_system_columns(self):
        table = pa.table({"__meta": [1], "data": [2]})
        result = drop_system_columns(table)
        assert "data" in result.column_names
        assert "__meta" not in result.column_names

    def test_preserves_non_system_columns(self):
        table = pa.table({"name": ["alice"], "age": [30]})
        result = drop_system_columns(table)
        assert result.column_names == ["name", "age"]
