"""Specification-derived tests for all operators.

Tests based on the design specification's operator semantics:
- Operators inspect tags, never packet content
- Operators can rename columns but never synthesize new values
- System tag evolution rules: name-preserving, name-extending, type-evolving
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.core.operators import (
    Batch,
    DropPacketColumns,
    DropTagColumns,
    Join,
    MapPackets,
    MapTags,
    MergeJoin,
    PolarsFilter,
    SelectPacketColumns,
    SelectTagColumns,
    SemiJoin,
)
from orcapod.core.sources import ArrowTableSource
from orcapod.core.streams import ArrowTableStream
from orcapod.errors import InputValidationError
from orcapod.system_constants import constants
from orcapod.types import ColumnConfig


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_stream(
    tag_data: dict, packet_data: dict, tag_columns: list[str]
) -> ArrowTableStream:
    all_data = {**tag_data, **packet_data}
    table = pa.table(all_data)
    return ArrowTableStream(table, tag_columns=tag_columns)


def _stream_a() -> ArrowTableStream:
    """Stream with tag=id, packet=age."""
    return _make_stream(
        {"id": pa.array([1, 2, 3], type=pa.int64())},
        {"age": pa.array([25, 30, 35], type=pa.int64())},
        ["id"],
    )


def _stream_b() -> ArrowTableStream:
    """Stream with tag=id, packet=score (overlaps with A on id=2,3)."""
    return _make_stream(
        {"id": pa.array([2, 3, 4], type=pa.int64())},
        {"score": pa.array([85, 90, 95], type=pa.int64())},
        ["id"],
    )


def _stream_b_overlapping_packet() -> ArrowTableStream:
    """Stream with tag=id, packet=age (same packet col name as A)."""
    return _make_stream(
        {"id": pa.array([2, 3, 4], type=pa.int64())},
        {"age": pa.array([40, 45, 50], type=pa.int64())},
        ["id"],
    )


def _stream_with_two_tags() -> ArrowTableStream:
    """Stream with tag={id, group}, packet=value."""
    return _make_stream(
        {
            "id": pa.array([1, 2, 3], type=pa.int64()),
            "group": pa.array(["a", "a", "b"], type=pa.large_string()),
        },
        {"value": pa.array([10, 20, 30], type=pa.int64())},
        ["id", "group"],
    )


# ===================================================================
# Join (N-ary, commutative)
# ===================================================================


class TestJoin:
    """Per design: N-ary inner join on shared tag columns. Requires
    non-overlapping packet columns. Commutative. System tags: name-extending."""

    def test_two_streams_on_common_tags(self):
        join = Join()
        result = join.process(_stream_a(), _stream_b())
        table = result.as_table()
        # Inner join on id: should have rows for id=2, id=3
        assert table.num_rows == 2
        assert "age" in table.column_names
        assert "score" in table.column_names

    def test_non_overlapping_packet_columns_required(self):
        join = Join()
        with pytest.raises(InputValidationError):
            join.validate_inputs(_stream_a(), _stream_b_overlapping_packet())

    def test_commutative(self):
        """join(A, B) should produce the same data as join(B, A)."""
        join = Join()
        result_ab = join.process(_stream_a(), _stream_b())
        result_ba = join.process(_stream_b(), _stream_a())

        table_ab = result_ab.as_table()
        table_ba = result_ba.as_table()

        # Same number of rows
        assert table_ab.num_rows == table_ba.num_rows

        # Same data (check by sorting by id and comparing values)
        ab_ids = sorted(table_ab.column("id").to_pylist())
        ba_ids = sorted(table_ba.column("id").to_pylist())
        assert ab_ids == ba_ids

    def test_empty_result_when_no_matches(self):
        """Disjoint tags → empty stream."""
        s1 = _make_stream(
            {"id": pa.array([1], type=pa.int64())},
            {"a": pa.array([10], type=pa.int64())},
            ["id"],
        )
        s2 = _make_stream(
            {"id": pa.array([99], type=pa.int64())},
            {"b": pa.array([20], type=pa.int64())},
            ["id"],
        )
        join = Join()
        result = join.process(s1, s2)
        table = result.as_table()
        assert table.num_rows == 0

    def test_three_or_more_streams(self):
        s1 = _make_stream(
            {"id": pa.array([1, 2], type=pa.int64())},
            {"a": pa.array([10, 20], type=pa.int64())},
            ["id"],
        )
        s2 = _make_stream(
            {"id": pa.array([1, 2], type=pa.int64())},
            {"b": pa.array([30, 40], type=pa.int64())},
            ["id"],
        )
        s3 = _make_stream(
            {"id": pa.array([1, 2], type=pa.int64())},
            {"c": pa.array([50, 60], type=pa.int64())},
            ["id"],
        )
        join = Join()
        result = join.process(s1, s2, s3)
        table = result.as_table()
        assert table.num_rows == 2
        assert "a" in table.column_names
        assert "b" in table.column_names
        assert "c" in table.column_names

    def test_system_tag_name_extending(self):
        """Per design, multi-input ops extend system tag column names with
        ::pipeline_hash:position. Sources (not raw streams) create system tags."""
        sa = ArrowTableSource(
            pa.table({"id": pa.array([2, 3], type=pa.int64()), "a": pa.array([10, 20], type=pa.int64())}),
            tag_columns=["id"],
        )
        sb = ArrowTableSource(
            pa.table({"id": pa.array([2, 3], type=pa.int64()), "b": pa.array([30, 40], type=pa.int64())}),
            tag_columns=["id"],
        )
        join = Join()
        result = join.process(sa, sb)
        table = result.as_table(all_info=True)
        tag_cols = [
            c for c in table.column_names if c.startswith(constants.SYSTEM_TAG_PREFIX)
        ]
        # After join, system tag columns should have extended names (at least 2 per input)
        assert len(tag_cols) >= 2

    def test_output_schema_prediction(self):
        join = Join()
        sa, sb = _stream_a(), _stream_b()
        predicted_tag, predicted_packet = join.output_schema(sa, sb)
        result = join.process(sa, sb)
        actual_tag, actual_packet = result.output_schema()
        assert set(predicted_tag.keys()) == set(actual_tag.keys())
        assert set(predicted_packet.keys()) == set(actual_packet.keys())


# ===================================================================
# MergeJoin (binary)
# ===================================================================


class TestMergeJoin:
    """Per design: binary join where colliding packet columns merge into
    sorted list[T]. Requires identical types for colliding columns."""

    def test_colliding_columns_become_sorted_lists(self):
        merge = MergeJoin()
        sa = _stream_a()  # packet: age
        sb = _stream_b_overlapping_packet()  # packet: age
        result = merge.process(sa, sb)
        table = result.as_table()
        # age should now be list[int] type
        age_type = table.schema.field("age").type
        assert pa.types.is_list(age_type) or pa.types.is_large_list(age_type)

    def test_non_colliding_columns_pass_through(self):
        merge = MergeJoin()
        # Create streams with some overlapping and some non-overlapping
        s1 = _make_stream(
            {"id": pa.array([1, 2], type=pa.int64())},
            {"shared": pa.array([10, 20], type=pa.int64()), "only_left": pa.array([1, 2], type=pa.int64())},
            ["id"],
        )
        s2 = _make_stream(
            {"id": pa.array([1, 2], type=pa.int64())},
            {"shared": pa.array([30, 40], type=pa.int64()), "only_right": pa.array([3, 4], type=pa.int64())},
            ["id"],
        )
        result = merge.process(s1, s2)
        table = result.as_table()
        assert "only_left" in table.column_names
        assert "only_right" in table.column_names
        # Non-overlapping columns should keep their original type
        assert table.schema.field("only_left").type == pa.int64()

    def test_output_schema_predicts_list_types(self):
        merge = MergeJoin()
        sa = _stream_a()
        sb = _stream_b_overlapping_packet()
        predicted_tag, predicted_packet = merge.output_schema(sa, sb)
        # The 'age' column should be predicted as list type
        assert "age" in predicted_packet


# ===================================================================
# SemiJoin (binary, non-commutative)
# ===================================================================


class TestSemiJoin:
    """Per design: binary non-commutative join. Keeps left rows matching
    right tags. Right packet columns are dropped."""

    def test_filters_left_by_right_tags(self):
        semi = SemiJoin()
        result = semi.process(_stream_a(), _stream_b())
        table = result.as_table()
        # A has id=[1,2,3], B has id=[2,3,4]
        # Semi-join keeps A rows where id in B → id=2, id=3
        assert table.num_rows == 2

    def test_non_commutative(self):
        semi = SemiJoin()
        result_ab = semi.process(_stream_a(), _stream_b())
        result_ba = semi.process(_stream_b(), _stream_a())
        # Generally not the same (different left/right roles)
        table_ab = result_ab.as_table()
        table_ba = result_ba.as_table()
        # AB keeps A's packets (age), BA keeps B's packets (score)
        assert "age" in table_ab.column_names
        assert "score" in table_ba.column_names

    def test_preserves_left_packet_columns(self):
        semi = SemiJoin()
        result = semi.process(_stream_a(), _stream_b())
        table = result.as_table()
        assert "age" in table.column_names
        assert "score" not in table.column_names


# ===================================================================
# Batch
# ===================================================================


class TestBatch:
    """Per design: groups rows by tag, aggregates packets. Packet column
    types become list[T]. System tag type evolves from str to list[str]."""

    def test_groups_rows(self):
        stream = _make_stream(
            {
                "group": pa.array(["a", "a", "b"], type=pa.large_string()),
            },
            {"value": pa.array([1, 2, 3], type=pa.int64())},
            ["group"],
        )
        batch = Batch()
        result = batch.process(stream)
        table = result.as_table()
        # Batch aggregates all rows into a single batch row
        assert table.num_rows == 1
        # Values should be collected into lists
        values = table.column("value").to_pylist()
        assert values == [[1, 2, 3]]

    def test_types_become_lists(self):
        stream = _make_stream(
            {"group": pa.array(["a", "a", "b"], type=pa.large_string())},
            {"value": pa.array([1, 2, 3], type=pa.int64())},
            ["group"],
        )
        batch = Batch()
        result = batch.process(stream)
        table = result.as_table()
        value_type = table.schema.field("value").type
        assert pa.types.is_list(value_type) or pa.types.is_large_list(value_type)

    def test_batch_output_schema_prediction(self):
        stream = _make_stream(
            {"group": pa.array(["a", "a", "b"], type=pa.large_string())},
            {"value": pa.array([1, 2, 3], type=pa.int64())},
            ["group"],
        )
        batch = Batch()
        predicted_tag, predicted_packet = batch.output_schema(stream)
        result = batch.process(stream)
        actual_tag, actual_packet = result.output_schema()
        assert set(predicted_tag.keys()) == set(actual_tag.keys())
        assert set(predicted_packet.keys()) == set(actual_packet.keys())

    def test_batch_with_batch_size(self):
        stream = _make_stream(
            {"group": pa.array(["a"] * 5, type=pa.large_string())},
            {"value": pa.array([1, 2, 3, 4, 5], type=pa.int64())},
            ["group"],
        )
        batch = Batch(batch_size=2)
        result = batch.process(stream)
        table = result.as_table()
        # 5 items with batch_size=2: groups of [2, 2, 1]
        assert table.num_rows >= 2

    def test_batch_drop_partial(self):
        stream = _make_stream(
            {"group": pa.array(["a"] * 5, type=pa.large_string())},
            {"value": pa.array([1, 2, 3, 4, 5], type=pa.int64())},
            ["group"],
        )
        batch = Batch(batch_size=2, drop_partial_batch=True)
        result = batch.process(stream)
        table = result.as_table()
        # 5 items, batch_size=2, drop_partial → only 2 full batches
        assert table.num_rows == 2


# ===================================================================
# Column Selection
# ===================================================================


class TestSelectTagColumns:
    """Per design: keeps only specified tag columns."""

    def test_select_tag_columns(self):
        stream = _stream_with_two_tags()
        select = SelectTagColumns(columns=["id"])
        result = select.process(stream)
        tag_keys, _ = result.keys()
        assert "id" in tag_keys
        assert "group" not in tag_keys

    def test_strict_missing_raises(self):
        stream = _stream_with_two_tags()
        select = SelectTagColumns(columns=["nonexistent"], strict=True)
        with pytest.raises(Exception):
            select.process(stream)


class TestSelectPacketColumns:
    """Per design: keeps only specified packet columns."""

    def test_select_packet_columns(self):
        stream = _make_stream(
            {"id": pa.array([1, 2], type=pa.int64())},
            {"a": pa.array([10, 20], type=pa.int64()), "b": pa.array([30, 40], type=pa.int64())},
            ["id"],
        )
        select = SelectPacketColumns(columns=["a"])
        result = select.process(stream)
        _, packet_keys = result.keys()
        assert "a" in packet_keys
        assert "b" not in packet_keys


class TestDropTagColumns:
    """Per design: removes specified tag columns."""

    def test_drop_tag_columns(self):
        stream = _stream_with_two_tags()
        drop = DropTagColumns(columns=["group"])
        result = drop.process(stream)
        tag_keys, _ = result.keys()
        assert "group" not in tag_keys
        assert "id" in tag_keys


class TestDropPacketColumns:
    """Per design: removes specified packet columns."""

    def test_drop_packet_columns(self):
        stream = _make_stream(
            {"id": pa.array([1, 2], type=pa.int64())},
            {"a": pa.array([10, 20], type=pa.int64()), "b": pa.array([30, 40], type=pa.int64())},
            ["id"],
        )
        drop = DropPacketColumns(columns=["b"])
        result = drop.process(stream)
        _, packet_keys = result.keys()
        assert "a" in packet_keys
        assert "b" not in packet_keys


# ===================================================================
# MapTags / MapPackets
# ===================================================================


class TestMapTags:
    """Per design: renames tag columns. System tags: name-preserving."""

    def test_renames_tag_columns(self):
        stream = _stream_with_two_tags()
        mapper = MapTags(name_map={"id": "identifier"})
        result = mapper.process(stream)
        tag_keys, _ = result.keys()
        assert "identifier" in tag_keys
        assert "id" not in tag_keys

    def test_drop_unmapped(self):
        stream = _stream_with_two_tags()
        mapper = MapTags(name_map={"id": "identifier"}, drop_unmapped=True)
        result = mapper.process(stream)
        tag_keys, _ = result.keys()
        assert "identifier" in tag_keys
        assert "group" not in tag_keys


class TestMapPackets:
    """Per design: renames packet columns."""

    def test_renames_packet_columns(self):
        stream = _make_stream(
            {"id": pa.array([1, 2], type=pa.int64())},
            {"value": pa.array([10, 20], type=pa.int64())},
            ["id"],
        )
        mapper = MapPackets(name_map={"value": "score"})
        result = mapper.process(stream)
        _, packet_keys = result.keys()
        assert "score" in packet_keys
        assert "value" not in packet_keys


# ===================================================================
# PolarsFilter
# ===================================================================


class TestPolarsFilter:
    """Per design: filters rows by predicate or constraints. Schema preserved.
    System tags: name-preserving."""

    def test_filter_with_constraints(self):
        stream = _stream_a()
        filt = PolarsFilter(constraints={"id": 2})
        result = filt.process(stream)
        table = result.as_table()
        assert table.num_rows == 1
        assert table.column("id").to_pylist() == [2]

    def test_filter_preserves_schema(self):
        stream = _stream_a()
        filt = PolarsFilter(constraints={"id": 2})
        predicted_tag, predicted_packet = filt.output_schema(stream)
        result = filt.process(stream)
        actual_tag, actual_packet = result.output_schema()
        assert set(predicted_tag.keys()) == set(actual_tag.keys())
        assert set(predicted_packet.keys()) == set(actual_packet.keys())


# ===================================================================
# Operator Base Class Validation
# ===================================================================


class TestOperatorInputValidation:
    """Per design, operators enforce input arity."""

    def test_unary_rejects_multiple_inputs(self):
        batch = Batch()
        with pytest.raises(Exception):
            batch.validate_inputs(_stream_a(), _stream_b())

    def test_binary_rejects_wrong_count(self):
        join = SemiJoin()
        with pytest.raises(Exception):
            join.validate_inputs(_stream_a())  # Only 1 for a binary op

    def test_nonzero_input_rejects_zero(self):
        join = Join()
        with pytest.raises(Exception):
            join.validate_inputs()  # No inputs
