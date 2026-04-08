"""
Tests for StreamBase convenience methods.

These methods wrap operators and are defined on StreamBase, so they're
available on ArrowTableStream, ArrowTableSource, and all derived streams.
"""

from __future__ import annotations

import pyarrow as pa

from orcapod.core.sources import ArrowTableSource
from orcapod.core.streams import ArrowTableStream


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_stream(tag_col: str, packet_cols: dict, tag_data: list) -> ArrowTableStream:
    """Build an ArrowTableStream from column specs."""
    columns = {tag_col: pa.array(tag_data, type=pa.large_string())}
    for name, values in packet_cols.items():
        columns[name] = pa.array(values, type=pa.int64())
    return ArrowTableStream(pa.table(columns), tag_columns=[tag_col])


def _make_source(tag_col: str, packet_cols: dict, tag_data: list) -> ArrowTableSource:
    """Build an ArrowTableSource from column specs."""
    columns = {tag_col: pa.array(tag_data, type=pa.large_string())}
    for name, values in packet_cols.items():
        columns[name] = pa.array(values, type=pa.int64())
    return ArrowTableSource(pa.table(columns), tag_columns=[tag_col], infer_nullable=True)


# ---------------------------------------------------------------------------
# Tests: join
# ---------------------------------------------------------------------------


class TestJoinConvenience:
    def test_join_returns_stream(self):
        s1 = _make_stream("k", {"a": [1, 2]}, ["x", "y"])
        s2 = _make_stream("k", {"b": [10, 20]}, ["x", "y"])
        result = s1.join(s2)
        table = result.as_table()
        assert table.num_rows == 2
        assert "a" in table.column_names
        assert "b" in table.column_names

    def test_join_with_label(self):
        s1 = _make_stream("k", {"a": [1]}, ["x"])
        s2 = _make_stream("k", {"b": [10]}, ["x"])
        result = s1.join(s2, label="my_join")
        assert result.label == "my_join"
        assert result.has_assigned_label

    def test_join_on_source(self):
        src1 = _make_source("k", {"a": [1, 2]}, ["x", "y"])
        src2 = _make_source("k", {"b": [10, 20]}, ["x", "y"])
        result = src1.join(src2)
        table = result.as_table()
        assert table.num_rows == 2
        assert "a" in table.column_names
        assert "b" in table.column_names

    def test_join_values_correct(self):
        s1 = _make_stream("k", {"val": [1, 2]}, ["a", "b"])
        s2 = _make_stream("k", {"score": [10, 20]}, ["a", "b"])
        result = s1.join(s2)
        table = result.as_table()
        vals = sorted(
            zip(table.column("k").to_pylist(), table.column("val").to_pylist())
        )
        assert vals == [("a", 1), ("b", 2)]


# ---------------------------------------------------------------------------
# Tests: semi_join
# ---------------------------------------------------------------------------


class TestSemiJoinConvenience:
    def test_semi_join_filters_to_matching(self):
        s1 = _make_stream("k", {"a": [1, 2, 3]}, ["x", "y", "z"])
        s2 = _make_stream("k", {"b": [10, 20]}, ["x", "z"])
        result = s1.semi_join(s2)
        table = result.as_table()
        assert table.num_rows == 2
        assert sorted(table.column("k").to_pylist()) == ["x", "z"]

    def test_semi_join_with_label(self):
        s1 = _make_stream("k", {"a": [1]}, ["x"])
        s2 = _make_stream("k", {"b": [10]}, ["x"])
        result = s1.semi_join(s2, label="my_semi")
        assert result.label == "my_semi"
        assert result.has_assigned_label


# ---------------------------------------------------------------------------
# Tests: map_tags
# ---------------------------------------------------------------------------


class TestMapTagsConvenience:
    def test_map_tags_renames(self):
        s = _make_stream("k", {"a": [1, 2]}, ["x", "y"])
        result = s.map_tags({"k": "key"})
        tag_keys, _ = result.keys()
        assert "key" in tag_keys
        assert "k" not in tag_keys

    def test_map_tags_with_label(self):
        s = _make_stream("k", {"a": [1]}, ["x"])
        result = s.map_tags({"k": "key"}, label="rename_tag")
        assert result.label == "rename_tag"
        assert result.has_assigned_label


# ---------------------------------------------------------------------------
# Tests: map_packets
# ---------------------------------------------------------------------------


class TestMapPacketsConvenience:
    def test_map_packets_renames(self):
        s = _make_stream("k", {"a": [1, 2]}, ["x", "y"])
        result = s.map_packets({"a": "alpha"})
        _, packet_keys = result.keys()
        assert "alpha" in packet_keys
        assert "a" not in packet_keys

    def test_map_packets_drop_unmapped_false(self):
        s = _make_stream("k", {"a": [1], "b": [2]}, ["x"])
        result = s.map_packets({"a": "alpha"}, drop_unmapped=False)
        _, packet_keys = result.keys()
        assert "alpha" in packet_keys
        assert "b" in packet_keys

    def test_map_packets_with_label(self):
        s = _make_stream("k", {"a": [1]}, ["x"])
        result = s.map_packets({"a": "alpha"}, label="rename_pkt")
        assert result.label == "rename_pkt"
        assert result.has_assigned_label


# ---------------------------------------------------------------------------
# Tests: select_tag_columns / select_packet_columns
# ---------------------------------------------------------------------------


class TestSelectColumnsConvenience:
    def test_select_tag_columns(self):
        table = pa.table(
            {
                "k1": pa.array(["a"], type=pa.large_string()),
                "k2": pa.array(["b"], type=pa.large_string()),
                "v": pa.array([1], type=pa.int64()),
            }
        )
        s = ArrowTableStream(table, tag_columns=["k1", "k2"])
        result = s.select_tag_columns(["k1"])
        tag_keys, _ = result.keys()
        assert tag_keys == ("k1",)

    def test_select_packet_columns(self):
        table = pa.table(
            {
                "k": pa.array(["a"], type=pa.large_string()),
                "v1": pa.array([1], type=pa.int64()),
                "v2": pa.array([2], type=pa.int64()),
            }
        )
        s = ArrowTableStream(table, tag_columns=["k"])
        result = s.select_packet_columns(["v1"])
        _, packet_keys = result.keys()
        assert packet_keys == ("v1",)

    def test_select_tag_columns_with_label(self):
        table = pa.table(
            {
                "k1": pa.array(["a"], type=pa.large_string()),
                "k2": pa.array(["b"], type=pa.large_string()),
                "v": pa.array([1], type=pa.int64()),
            }
        )
        s = ArrowTableStream(table, tag_columns=["k1", "k2"])
        result = s.select_tag_columns(["k1"], label="sel_tag")
        assert result.label == "sel_tag"
        assert result.has_assigned_label


# ---------------------------------------------------------------------------
# Tests: drop_tag_columns / drop_packet_columns
# ---------------------------------------------------------------------------


class TestDropColumnsConvenience:
    def test_drop_tag_columns(self):
        table = pa.table(
            {
                "k1": pa.array(["a"], type=pa.large_string()),
                "k2": pa.array(["b"], type=pa.large_string()),
                "v": pa.array([1], type=pa.int64()),
            }
        )
        s = ArrowTableStream(table, tag_columns=["k1", "k2"])
        result = s.drop_tag_columns(["k2"])
        tag_keys, _ = result.keys()
        assert "k1" in tag_keys
        assert "k2" not in tag_keys

    def test_drop_packet_columns(self):
        table = pa.table(
            {
                "k": pa.array(["a"], type=pa.large_string()),
                "v1": pa.array([1], type=pa.int64()),
                "v2": pa.array([2], type=pa.int64()),
            }
        )
        s = ArrowTableStream(table, tag_columns=["k"])
        result = s.drop_packet_columns(["v2"])
        _, packet_keys = result.keys()
        assert "v1" in packet_keys
        assert "v2" not in packet_keys


# ---------------------------------------------------------------------------
# Tests: batch
# ---------------------------------------------------------------------------


class TestBatchConvenience:
    def test_batch_groups_rows(self):
        s = _make_stream("k", {"a": [1, 2, 3, 4]}, ["w", "x", "y", "z"])
        result = s.batch(batch_size=2)
        table = result.as_table()
        # 4 rows / batch_size 2 = 2 batched rows
        assert table.num_rows == 2

    def test_batch_with_label(self):
        s = _make_stream("k", {"a": [1, 2]}, ["x", "y"])
        result = s.batch(batch_size=2, label="my_batch")
        assert result.label == "my_batch"
        assert result.has_assigned_label


# ---------------------------------------------------------------------------
# Tests: polars_filter
# ---------------------------------------------------------------------------


class TestPolarsFilterConvenience:
    def test_polars_filter_with_constraints(self):
        s = _make_stream("k", {"a": [1, 2, 3]}, ["x", "y", "z"])
        result = s.polars_filter(k="x")
        table = result.as_table()
        assert table.num_rows == 1
        assert table.column("k").to_pylist() == ["x"]

    def test_polars_filter_with_label(self):
        s = _make_stream("k", {"a": [1]}, ["x"])
        result = s.polars_filter(k="x", label="my_filter")
        assert result.label == "my_filter"
        assert result.has_assigned_label


# ---------------------------------------------------------------------------
# Tests: chaining convenience methods
# ---------------------------------------------------------------------------


class TestChaining:
    def test_join_then_select(self):
        s1 = _make_stream("k", {"a": [1, 2]}, ["x", "y"])
        s2 = _make_stream("k", {"b": [10, 20]}, ["x", "y"])
        result = s1.join(s2).select_packet_columns(["a"])
        _, packet_keys = result.keys()
        assert packet_keys == ("a",)
        assert result.as_table().num_rows == 2

    def test_map_then_filter(self):
        s = _make_stream("k", {"val": [1, 2, 3]}, ["a", "b", "c"])
        result = s.map_packets({"val": "value"}).polars_filter(k="b")
        table = result.as_table()
        assert table.num_rows == 1
        assert "value" in table.column_names

    def test_source_join_then_map(self):
        src1 = _make_source("k", {"a": [1, 2]}, ["x", "y"])
        src2 = _make_source("k", {"b": [10, 20]}, ["x", "y"])
        result = src1.join(src2).map_packets({"a": "alpha", "b": "beta"})
        _, packet_keys = result.keys()
        assert "alpha" in packet_keys
        assert "beta" in packet_keys
