"""
Protocol conformance and comprehensive functionality tests for all source implementations.

Every concrete source (ArrowTableSource, DictSource, ListSource, DataFrameSource)
must satisfy both StreamProtocol and PipelineElementProtocol — i.e. it is a pure
stream that is the root of the computational graph.

Tests are structured in two layers:
1. Protocol conformance  — isinstance checks against StreamProtocol,
                           PipelineElementProtocol, RootSource
2. StreamProtocol-side behaviour — source (None), upstreams (empty), keys,
                                   output_schema, iter_packets, as_table
"""

from __future__ import annotations

import pyarrow as pa
import pytest
import polars as pl

from orcapod.core.sources import (
    ArrowTableSource,
    DataFrameSource,
    DictSource,
    ListSource,
    RootSource,
)
from orcapod.protocols.core_protocols import SourceProtocol, StreamProtocol
from orcapod.protocols.hashing_protocols import PipelineElementProtocol
from orcapod.types import Schema


# ---------------------------------------------------------------------------
# Fixtures — one instance of each concrete source
# ---------------------------------------------------------------------------


@pytest.fixture
def arrow_src():
    table = pa.table(
        {
            "id": pa.array([1, 2, 3], type=pa.int64()),
            "value": pa.array(["a", "b", "c"], type=pa.large_string()),
        }
    )
    return ArrowTableSource(table=table, tag_columns=["id"])


@pytest.fixture
def arrow_src_with_record_id():
    table = pa.table(
        {
            "id": pa.array([1, 2, 3], type=pa.int64()),
            "value": pa.array(["a", "b", "c"], type=pa.large_string()),
        }
    )
    return ArrowTableSource(
        table=table,
        tag_columns=["id"],
        record_id_column="id",
        source_id="arrow_with_rid",
    )


@pytest.fixture
def dict_src():
    return DictSource(
        data=[
            {"id": 1, "value": "a"},
            {"id": 2, "value": "b"},
            {"id": 3, "value": "c"},
        ],
        tag_columns=["id"],
    )


@pytest.fixture
def list_src():
    return ListSource(name="item", data=["x", "y", "z"])


@pytest.fixture
def df_src():
    df = pl.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})
    return DataFrameSource(data=df, tag_columns="id")


ALL_SOURCE_FIXTURES = ["arrow_src", "dict_src", "list_src", "df_src"]


# ---------------------------------------------------------------------------
# 1. Protocol conformance
# ---------------------------------------------------------------------------


class TestProtocolConformance:
    """Every source must satisfy StreamProtocol, PipelineElementProtocol, and SourceProtocol."""

    @pytest.mark.parametrize("src_fixture", ALL_SOURCE_FIXTURES)
    def test_is_stream(self, src_fixture, request):
        src = request.getfixturevalue(src_fixture)
        assert isinstance(src, StreamProtocol), (
            f"{type(src).__name__} does not satisfy StreamProtocol"
        )

    @pytest.mark.parametrize("src_fixture", ALL_SOURCE_FIXTURES)
    def test_is_pipeline_element(self, src_fixture, request):
        src = request.getfixturevalue(src_fixture)
        assert isinstance(src, PipelineElementProtocol), (
            f"{type(src).__name__} does not satisfy PipelineElementProtocol"
        )

    @pytest.mark.parametrize("src_fixture", ALL_SOURCE_FIXTURES)
    def test_is_root_source(self, src_fixture, request):
        src = request.getfixturevalue(src_fixture)
        assert isinstance(src, RootSource)

    @pytest.mark.parametrize("src_fixture", ALL_SOURCE_FIXTURES)
    def test_is_source_protocol(self, src_fixture, request):
        """Every source must satisfy SourceProtocol (source_id + resolve_field)."""
        src = request.getfixturevalue(src_fixture)
        assert isinstance(src, SourceProtocol), (
            f"{type(src).__name__} does not satisfy SourceProtocol"
        )


# ---------------------------------------------------------------------------
# 2. output_schema
# ---------------------------------------------------------------------------


class TestSourceOutputSchema:
    @pytest.mark.parametrize("src_fixture", ALL_SOURCE_FIXTURES)
    def test_returns_two_schemas(self, src_fixture, request):
        src = request.getfixturevalue(src_fixture)
        result = src.output_schema()
        assert isinstance(result, tuple)
        assert len(result) == 2

    @pytest.mark.parametrize("src_fixture", ALL_SOURCE_FIXTURES)
    def test_schemas_are_schema_instances(self, src_fixture, request):
        src = request.getfixturevalue(src_fixture)
        tag_schema, packet_schema = src.output_schema()
        assert isinstance(tag_schema, Schema)
        assert isinstance(packet_schema, Schema)

    def test_arrow_src_tag_schema_has_id(self, arrow_src):
        tag_schema, _ = arrow_src.output_schema()
        assert "id" in tag_schema

    def test_arrow_src_packet_schema_has_value(self, arrow_src):
        _, packet_schema = arrow_src.output_schema()
        assert "value" in packet_schema

    def test_dict_src_tag_schema_has_id(self, dict_src):
        tag_schema, _ = dict_src.output_schema()
        assert "id" in tag_schema

    def test_list_src_packet_schema_has_item(self, list_src):
        _, packet_schema = list_src.output_schema()
        assert "item" in packet_schema

    def test_list_src_tag_schema_has_element_index(self, list_src):
        tag_schema, _ = list_src.output_schema()
        assert "element_index" in tag_schema

    def test_df_src_tag_schema_has_id(self, df_src):
        tag_schema, _ = df_src.output_schema()
        assert "id" in tag_schema


# ---------------------------------------------------------------------------
# 4. StreamProtocol-side behaviour
# ---------------------------------------------------------------------------


class TestStreamSource:
    @pytest.mark.parametrize("src_fixture", ALL_SOURCE_FIXTURES)
    def test_producer_is_none(self, src_fixture, request):
        """RootSource is a pure stream — source returns None."""
        src = request.getfixturevalue(src_fixture)
        assert src.producer is None

    @pytest.mark.parametrize("src_fixture", ALL_SOURCE_FIXTURES)
    def test_upstreams_is_empty_tuple(self, src_fixture, request):
        src = request.getfixturevalue(src_fixture)
        assert src.upstreams == ()


class TestStreamKeys:
    @pytest.mark.parametrize("src_fixture", ALL_SOURCE_FIXTURES)
    def test_returns_two_tuples(self, src_fixture, request):
        src = request.getfixturevalue(src_fixture)
        tag_keys, packet_keys = src.keys()
        assert isinstance(tag_keys, tuple)
        assert isinstance(packet_keys, tuple)

    @pytest.mark.parametrize("src_fixture", ALL_SOURCE_FIXTURES)
    def test_no_overlap_between_tag_and_packet_keys(self, src_fixture, request):
        src = request.getfixturevalue(src_fixture)
        tag_keys, packet_keys = src.keys()
        assert set(tag_keys).isdisjoint(set(packet_keys))

    def test_arrow_src_keys(self, arrow_src):
        tag_keys, packet_keys = arrow_src.keys()
        assert "id" in tag_keys
        assert "value" in packet_keys

    def test_list_src_keys(self, list_src):
        tag_keys, packet_keys = list_src.keys()
        assert "element_index" in tag_keys
        assert "item" in packet_keys

    def test_dict_src_keys(self, dict_src):
        tag_keys, packet_keys = dict_src.keys()
        assert "id" in tag_keys
        assert "value" in packet_keys


class TestStreamOutputSchema:
    """StreamProtocol-protocol output_schema (no positional args)."""

    @pytest.mark.parametrize("src_fixture", ALL_SOURCE_FIXTURES)
    def test_returns_two_schemas(self, src_fixture, request):
        src = request.getfixturevalue(src_fixture)
        tag_schema, packet_schema = src.output_schema()
        assert isinstance(tag_schema, Schema)
        assert isinstance(packet_schema, Schema)

    @pytest.mark.parametrize("src_fixture", ALL_SOURCE_FIXTURES)
    def test_consistent_with_keys(self, src_fixture, request):
        src = request.getfixturevalue(src_fixture)
        tag_keys, packet_keys = src.keys()
        tag_schema, packet_schema = src.output_schema()
        assert set(tag_keys) == set(tag_schema.keys())
        assert set(packet_keys) == set(packet_schema.keys())


class TestStreamIterPackets:
    @pytest.mark.parametrize("src_fixture", ALL_SOURCE_FIXTURES)
    def test_yields_tag_packet_pairs(self, src_fixture, request):
        src = request.getfixturevalue(src_fixture)
        pairs = list(src.iter_packets())
        assert len(pairs) > 0
        for tag, packet in pairs:
            assert tag is not None
            assert packet is not None

    @pytest.mark.parametrize("src_fixture", ALL_SOURCE_FIXTURES)
    def test_correct_row_count(self, src_fixture, request):
        src = request.getfixturevalue(src_fixture)
        assert len(list(src.iter_packets())) == 3

    def test_arrow_src_packet_values(self, arrow_src):
        packets = [pkt for _, pkt in arrow_src.iter_packets()]
        values = {pkt["value"] for pkt in packets}
        assert values == {"a", "b", "c"}

    def test_arrow_src_tag_values(self, arrow_src):
        tags = [tag for tag, _ in arrow_src.iter_packets()]
        ids = {tag["id"] for tag in tags}
        assert ids == {1, 2, 3}

    def test_list_src_packet_values(self, list_src):
        packets = [pkt for _, pkt in list_src.iter_packets()]
        items = {pkt["item"] for pkt in packets}
        assert items == {"x", "y", "z"}

    def test_dict_src_tag_and_packet_values(self, dict_src):
        pairs = list(dict_src.iter_packets())
        assert len(pairs) == 3
        values = {pkt["value"] for _, pkt in pairs}
        assert values == {"a", "b", "c"}

    def test_df_src_values(self, df_src):
        packets = [pkt for _, pkt in df_src.iter_packets()]
        values = {pkt["value"] for pkt in packets}
        assert values == {"a", "b", "c"}

    @pytest.mark.parametrize("src_fixture", ALL_SOURCE_FIXTURES)
    def test_iter_packets_is_repeatable(self, src_fixture, request):
        src = request.getfixturevalue(src_fixture)
        first = list(src.iter_packets())
        second = list(src.iter_packets())
        assert len(first) == len(second)


class TestStreamAsTable:
    @pytest.mark.parametrize("src_fixture", ALL_SOURCE_FIXTURES)
    def test_returns_pyarrow_table(self, src_fixture, request):
        src = request.getfixturevalue(src_fixture)
        table = src.as_table()
        assert isinstance(table, pa.Table)

    @pytest.mark.parametrize("src_fixture", ALL_SOURCE_FIXTURES)
    def test_correct_row_count(self, src_fixture, request):
        src = request.getfixturevalue(src_fixture)
        assert src.as_table().num_rows == 3

    @pytest.mark.parametrize("src_fixture", ALL_SOURCE_FIXTURES)
    def test_default_no_system_columns(self, src_fixture, request):
        src = request.getfixturevalue(src_fixture)
        table = src.as_table()
        assert not any(c.startswith("_tag::") for c in table.column_names)

    @pytest.mark.parametrize("src_fixture", ALL_SOURCE_FIXTURES)
    def test_all_info_adds_source_columns(self, src_fixture, request):
        src = request.getfixturevalue(src_fixture)
        table = src.as_table(all_info=True)
        source_cols = [c for c in table.column_names if c.startswith("_source_")]
        assert len(source_cols) > 0

    def test_arrow_src_data_columns_present(self, arrow_src):
        table = arrow_src.as_table()
        assert "id" in table.column_names
        assert "value" in table.column_names

    def test_list_src_data_columns_present(self, list_src):
        table = list_src.as_table()
        assert "element_index" in table.column_names
        assert "item" in table.column_names


# ---------------------------------------------------------------------------
# 5. source_id property
# ---------------------------------------------------------------------------


class TestSourceId:
    @pytest.mark.parametrize("src_fixture", ALL_SOURCE_FIXTURES)
    def test_source_id_is_string(self, src_fixture, request):
        src = request.getfixturevalue(src_fixture)
        assert isinstance(src.source_id, str)
        assert len(src.source_id) > 0

    def test_explicit_source_id_honoured(self):
        table = pa.table({"x": pa.array([1, 2], type=pa.int64())})
        src = ArrowTableSource(table=table, source_id="my_explicit_id")
        assert src.source_id == "my_explicit_id"

    def test_source_id_in_provenance_tokens(self, arrow_src):
        table = arrow_src.as_table(all_info=True)
        source_cols = [c for c in table.column_names if c.startswith("_source_")]
        assert source_cols
        token = table.column(source_cols[0])[0].as_py()
        assert token.startswith(arrow_src.source_id)


# ---------------------------------------------------------------------------
# 6. Content hash and identity
# ---------------------------------------------------------------------------


class TestContentHash:
    @pytest.mark.parametrize("src_fixture", ALL_SOURCE_FIXTURES)
    def test_content_hash_is_stable(self, src_fixture, request):
        src = request.getfixturevalue(src_fixture)
        assert src.content_hash() == src.content_hash()

    def test_same_data_same_content_hash(self):
        table = pa.table({"x": pa.array([1, 2, 3], type=pa.int64())})
        src1 = ArrowTableSource(table=table)
        src2 = ArrowTableSource(table=table)
        assert src1.content_hash() == src2.content_hash()

    def test_different_data_different_content_hash(self):
        src1 = ArrowTableSource(table=pa.table({"x": pa.array([1], type=pa.int64())}))
        src2 = ArrowTableSource(table=pa.table({"x": pa.array([2], type=pa.int64())}))
        assert src1.content_hash() != src2.content_hash()


# ---------------------------------------------------------------------------
# 7. Pipeline hash (PipelineElementProtocol)
# ---------------------------------------------------------------------------


class TestPipelineHash:
    @pytest.mark.parametrize("src_fixture", ALL_SOURCE_FIXTURES)
    def test_pipeline_hash_is_stable(self, src_fixture, request):
        src = request.getfixturevalue(src_fixture)
        assert src.pipeline_hash() == src.pipeline_hash()

    def test_same_schema_same_pipeline_hash(self):
        table = pa.table({"x": pa.array([1, 2, 3], type=pa.int64())})
        src1 = ArrowTableSource(table=table)
        src2 = ArrowTableSource(
            table=pa.table({"x": pa.array([99, 100, 101], type=pa.int64())})
        )
        # Same schema → same pipeline hash
        assert src1.pipeline_hash() == src2.pipeline_hash()

    def test_different_schema_different_pipeline_hash(self):
        src1 = ArrowTableSource(
            table=pa.table({"x": pa.array([1, 2], type=pa.int64())})
        )
        src2 = ArrowTableSource(
            table=pa.table({"y": pa.array([1, 2], type=pa.int64())})
        )
        assert src1.pipeline_hash() != src2.pipeline_hash()


# ---------------------------------------------------------------------------
# 8. Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    def test_arrow_source_no_tag_columns(self):
        """A source with no tag columns is valid; all columns are packet columns."""
        table = pa.table({"a": pa.array([1, 2], type=pa.int64())})
        src = ArrowTableSource(table=table)
        tag_keys, packet_keys = src.keys()
        assert "a" in packet_keys
        assert tag_keys == ()

    def test_dict_source_multiple_tag_columns(self):
        data = [
            {"a": 1, "b": 2, "val": "x"},
            {"a": 3, "b": 4, "val": "y"},
        ]
        src = DictSource(data=data, tag_columns=["a", "b"])
        tag_keys, packet_keys = src.keys()
        assert set(tag_keys) == {"a", "b"}
        assert "val" in packet_keys

    def test_list_source_custom_tag_function(self):
        def tag_fn(element, idx):
            return {"label": f"item_{idx}"}

        src = ListSource(
            name="val",
            data=[10, 20, 30],
            tag_function=tag_fn,
            expected_tag_keys=["label"],
        )
        tag_keys, packet_keys = src.keys()
        assert "label" in tag_keys
        assert "val" in packet_keys
        pairs = list(src.iter_packets())
        labels = {tag["label"] for tag, _ in pairs}
        assert labels == {"item_0", "item_1", "item_2"}

    def test_df_source_missing_tag_column_raises(self):
        df = pl.DataFrame({"x": [1, 2, 3]})
        with pytest.raises(ValueError, match="not found"):
            DataFrameSource(data=df, tag_columns="nonexistent")

    def test_arrow_source_strips_system_columns_from_input(self):
        """System columns in the input table are silently dropped."""
        table = pa.table(
            {
                "x": pa.array([1, 2], type=pa.int64()),
                "_tag::something": pa.array(["a", "b"], type=pa.large_string()),
            }
        )
        src = ArrowTableSource(table=table)
        # system columns should not appear in data keys
        tag_keys, packet_keys = src.keys()
        assert "_tag::something" not in tag_keys
        assert "_tag::something" not in packet_keys
