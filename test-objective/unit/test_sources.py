"""Specification-derived tests for all source types.

Tests documented behaviors of ArrowTableSource, DictSource, ListSource,
and DerivedSource from orcapod.core.sources.
"""

from unittest.mock import MagicMock

import pyarrow as pa
import pytest

from orcapod.core.datagrams import Packet, Tag
from orcapod.core.sources import ArrowTableSource
from orcapod.core.sources.derived_source import DerivedSource
from orcapod.core.sources.dict_source import DictSource
from orcapod.core.sources.list_source import ListSource
from orcapod.errors import FieldNotResolvableError
from orcapod.types import ColumnConfig, Schema


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _simple_table(n_rows: int = 3) -> pa.Table:
    return pa.table(
        {
            "name": pa.array([f"n{i}" for i in range(n_rows)], type=pa.large_string()),
            "age": pa.array([20 + i for i in range(n_rows)], type=pa.int64()),
        }
    )


# ===========================================================================
# ArrowTableSource
# ===========================================================================


class TestArrowTableSourceConstruction:
    """ArrowTableSource construction behaviors."""

    def test_normal_construction(self):
        """A valid table with tag columns constructs successfully."""
        source = ArrowTableSource(_simple_table(), tag_columns=["name"])
        assert source is not None

    def test_empty_table_raises(self):
        """An empty table raises an error during construction."""
        empty = pa.table(
            {
                "name": pa.array([], type=pa.large_string()),
                "age": pa.array([], type=pa.int64()),
            }
        )
        with pytest.raises(Exception):
            ArrowTableSource(empty, tag_columns=["name"])

    def test_missing_tag_columns_raises_value_error(self):
        """Specifying tag columns not in the table raises ValueError."""
        table = _simple_table()
        with pytest.raises(ValueError, match="tag_columns"):
            ArrowTableSource(table, tag_columns=["nonexistent"])

    def test_adds_system_tag_column(self):
        """The source auto-adds system tag columns to the underlying table."""
        source = ArrowTableSource(_simple_table(), tag_columns=["name"])
        table = source.as_table(all_info=True)
        system_tag_cols = [c for c in table.column_names if c.startswith("_tag_")]
        assert len(system_tag_cols) > 0

    def test_adds_source_info_columns(self):
        """The source adds source info columns (prefixed with _source_)."""
        source = ArrowTableSource(_simple_table(), tag_columns=["name"])
        table = source.as_table(columns=ColumnConfig(source=True))
        source_cols = [c for c in table.column_names if c.startswith("_source_")]
        assert len(source_cols) > 0

    def test_source_id_populated(self):
        """source_id property is populated (defaults to table hash)."""
        source = ArrowTableSource(_simple_table(), tag_columns=["name"])
        assert source.source_id is not None
        assert len(source.source_id) > 0

    def test_source_id_explicit(self):
        """Explicit source_id is preserved."""
        source = ArrowTableSource(
            _simple_table(),
            tag_columns=["name"],
            source_id="my_source",
        )
        assert source.source_id == "my_source"

    def test_producer_is_none(self):
        """Root sources have producer == None."""
        source = ArrowTableSource(_simple_table(), tag_columns=["name"])
        assert source.producer is None

    def test_upstreams_is_empty(self):
        """Root sources have empty upstreams tuple."""
        source = ArrowTableSource(_simple_table(), tag_columns=["name"])
        assert source.upstreams == ()

    def test_no_tag_columns_valid(self):
        """Construction with no tag columns is valid (all columns are packets)."""
        source = ArrowTableSource(_simple_table(), tag_columns=[])
        tag_keys, packet_keys = source.keys()
        assert tag_keys == ()
        assert "name" in packet_keys
        assert "age" in packet_keys


class TestArrowTableSourceResolveField:
    """ArrowTableSource.resolve_field() behaviors.

    NOTE: resolve_field is currently not implemented on ArrowTableSource
    (raises NotImplementedError from RootSource base). These tests are
    marked xfail until the implementation is restored.
    """

    NOT_IMPLEMENTED = pytest.mark.xfail(
        reason="resolve_field not yet re-implemented after source refactor",
        raises=NotImplementedError,
        strict=True,
    )

    @NOT_IMPLEMENTED
    def test_resolve_field_valid_record_id(self):
        """resolve_field works with valid positional record_id."""
        source = ArrowTableSource(_simple_table(3), tag_columns=["name"])
        value = source.resolve_field("row_0", "age")
        assert value == 20

    @NOT_IMPLEMENTED
    def test_resolve_field_second_row(self):
        """resolve_field returns data from the correct row."""
        source = ArrowTableSource(_simple_table(3), tag_columns=["name"])
        value = source.resolve_field("row_1", "age")
        assert value == 21

    @NOT_IMPLEMENTED
    def test_resolve_field_with_record_id_column(self):
        """resolve_field works with named record_id column."""
        source = ArrowTableSource(
            _simple_table(3),
            tag_columns=["name"],
            record_id_column="name",
        )
        value = source.resolve_field("name=n1", "age")
        assert value == 21

    @NOT_IMPLEMENTED
    def test_resolve_field_missing_record_raises(self):
        """resolve_field raises FieldNotResolvableError for missing records."""
        source = ArrowTableSource(_simple_table(3), tag_columns=["name"])
        with pytest.raises(FieldNotResolvableError):
            source.resolve_field("row_999", "age")

    @NOT_IMPLEMENTED
    def test_resolve_field_missing_field_raises(self):
        """resolve_field raises FieldNotResolvableError for missing field names."""
        source = ArrowTableSource(_simple_table(3), tag_columns=["name"])
        with pytest.raises(FieldNotResolvableError):
            source.resolve_field("row_0", "nonexistent_field")

    @NOT_IMPLEMENTED
    def test_resolve_field_invalid_record_id_format(self):
        """resolve_field raises FieldNotResolvableError for invalid record_id format."""
        source = ArrowTableSource(_simple_table(3), tag_columns=["name"])
        with pytest.raises(FieldNotResolvableError):
            source.resolve_field("invalid_format", "age")

    @NOT_IMPLEMENTED
    def test_resolve_field_tag_column(self):
        """resolve_field can resolve tag column values too."""
        source = ArrowTableSource(_simple_table(3), tag_columns=["name"])
        value = source.resolve_field("row_0", "name")
        assert value == "n0"


class TestArrowTableSourceSchema:
    """ArrowTableSource schema and identity behaviors."""

    def test_pipeline_identity_structure_returns_schemas(self):
        """pipeline_identity_structure returns (tag_schema, packet_schema)."""
        source = ArrowTableSource(_simple_table(), tag_columns=["name"])
        result = source.pipeline_identity_structure()
        assert isinstance(result, tuple)
        assert len(result) == 2
        tag_schema, packet_schema = result
        assert isinstance(tag_schema, Schema)
        assert isinstance(packet_schema, Schema)

    def test_output_schema_returns_schemas(self):
        source = ArrowTableSource(_simple_table(), tag_columns=["name"])
        tag_schema, packet_schema = source.output_schema()
        assert "name" in tag_schema
        assert "age" in packet_schema

    def test_output_schema_types(self):
        """output_schema types match column data types."""
        source = ArrowTableSource(_simple_table(), tag_columns=["name"])
        tag_schema, packet_schema = source.output_schema()
        assert tag_schema["name"] is str
        assert packet_schema["age"] is int

    def test_keys_returns_correct_split(self):
        """keys() correctly separates tag and packet columns."""
        source = ArrowTableSource(_simple_table(), tag_columns=["name"])
        tag_keys, packet_keys = source.keys()
        assert "name" in tag_keys
        assert "age" in packet_keys
        assert "name" not in packet_keys


class TestArrowTableSourceIteration:
    """ArrowTableSource iter_packets and as_table behaviors."""

    def test_iter_packets_yields_tag_packet_pairs(self):
        source = ArrowTableSource(_simple_table(3), tag_columns=["name"])
        pairs = list(source.iter_packets())
        assert len(pairs) == 3
        for tag, packet in pairs:
            assert isinstance(tag, Tag)
            assert isinstance(packet, Packet)

    def test_as_table_has_expected_columns(self):
        source = ArrowTableSource(_simple_table(), tag_columns=["name"])
        table = source.as_table()
        assert "name" in table.column_names
        assert "age" in table.column_names

    def test_as_table_row_count(self):
        """as_table row count matches input table row count."""
        source = ArrowTableSource(_simple_table(5), tag_columns=["name"])
        table = source.as_table()
        assert table.num_rows == 5

    def test_as_table_all_info_has_more_columns(self):
        """as_table(all_info=True) has more columns than default."""
        source = ArrowTableSource(_simple_table(), tag_columns=["name"])
        table_default = source.as_table()
        table_all = source.as_table(all_info=True)
        assert table_all.num_columns > table_default.num_columns

    def test_iter_packets_count_matches_as_table_rows(self):
        """iter_packets count equals as_table row count."""
        source = ArrowTableSource(_simple_table(4), tag_columns=["name"])
        pairs = list(source.iter_packets())
        table = source.as_table()
        assert len(pairs) == table.num_rows


# ===========================================================================
# DictSource
# ===========================================================================


class TestDictSource:
    """DictSource construction and delegation behaviors."""

    def test_construction_from_list_of_dicts(self):
        """DictSource can be constructed from a collection of dicts."""
        data = [{"x": 1, "y": "a"}, {"x": 2, "y": "b"}]
        source = DictSource(data=data, tag_columns=["x"])
        assert source is not None

    def test_delegates_to_arrow_table_source(self):
        """DictSource produces valid iter_packets output."""
        data = [{"x": 1, "y": "a"}, {"x": 2, "y": "b"}]
        source = DictSource(data=data, tag_columns=["x"])
        pairs = list(source.iter_packets())
        assert len(pairs) == 2

    def test_keys_correct(self):
        data = [{"x": 1, "y": "a"}]
        source = DictSource(data=data, tag_columns=["x"])
        tag_keys, packet_keys = source.keys()
        assert "x" in tag_keys
        assert "y" in packet_keys

    def test_source_id_populated(self):
        data = [{"x": 1, "y": "a"}]
        source = DictSource(data=data, tag_columns=["x"])
        assert source.source_id is not None
        assert len(source.source_id) > 0

    def test_producer_is_none(self):
        data = [{"x": 1, "y": "a"}]
        source = DictSource(data=data, tag_columns=["x"])
        assert source.producer is None

    def test_upstreams_is_empty(self):
        data = [{"x": 1, "y": "a"}]
        source = DictSource(data=data, tag_columns=["x"])
        assert source.upstreams == ()

    def test_output_schema(self):
        """DictSource output_schema delegates correctly."""
        data = [{"x": 1, "y": "a"}]
        source = DictSource(data=data, tag_columns=["x"])
        tag_schema, packet_schema = source.output_schema()
        assert "x" in tag_schema
        assert "y" in packet_schema

    def test_as_table_has_correct_rows(self):
        """DictSource as_table returns correct number of rows."""
        data = [{"x": 1, "y": "a"}, {"x": 2, "y": "b"}, {"x": 3, "y": "c"}]
        source = DictSource(data=data, tag_columns=["x"])
        table = source.as_table()
        assert table.num_rows == 3

    def test_iter_packets_yields_tag_packet_pairs(self):
        """DictSource iter_packets yields proper types."""
        data = [{"x": 1, "y": "a"}]
        source = DictSource(data=data, tag_columns=["x"])
        pairs = list(source.iter_packets())
        assert len(pairs) == 1
        tag, packet = pairs[0]
        assert isinstance(tag, Tag)
        assert isinstance(packet, Packet)

    def test_multiple_packet_columns(self):
        """DictSource handles multiple packet columns."""
        data = [{"tag": 1, "a": "x", "b": 10}]
        source = DictSource(data=data, tag_columns=["tag"])
        _, packet_keys = source.keys()
        assert "a" in packet_keys
        assert "b" in packet_keys


# ===========================================================================
# ListSource
# ===========================================================================


class TestListSource:
    """ListSource construction and behaviors."""

    def test_construction_from_list(self):
        """ListSource can be constructed from a list of elements."""
        source = ListSource(name="item", data=["a", "b", "c"])
        assert source is not None

    def test_iter_packets_yields_correct_count(self):
        source = ListSource(name="item", data=["a", "b", "c"])
        pairs = list(source.iter_packets())
        assert len(pairs) == 3

    def test_default_tag_is_element_index(self):
        """Default tag function produces element_index tag."""
        source = ListSource(name="item", data=["a", "b"])
        tag_keys, _ = source.keys()
        assert "element_index" in tag_keys

    def test_empty_list_raises_value_error(self):
        """An empty list raises ValueError (empty table)."""
        with pytest.raises(ValueError):
            ListSource(name="item", data=[])

    def test_custom_tag_function(self):
        """Custom tag_function is used for tag generation."""
        source = ListSource(
            name="item",
            data=["a", "b"],
            tag_function=lambda el, idx: {"pos": idx * 10},
            expected_tag_keys=["pos"],
        )
        tag_keys, _ = source.keys()
        assert "pos" in tag_keys

    def test_packet_column_name_matches(self):
        """The packet column is named after the 'name' parameter."""
        source = ListSource(name="my_data", data=[1, 2, 3])
        _, packet_keys = source.keys()
        assert "my_data" in packet_keys

    def test_source_id_populated(self):
        """ListSource has a populated source_id."""
        source = ListSource(name="item", data=["a"])
        assert source.source_id is not None
        assert len(source.source_id) > 0

    def test_as_table_correct_row_count(self):
        """ListSource as_table returns correct number of rows."""
        source = ListSource(name="item", data=["a", "b", "c", "d"])
        table = source.as_table()
        assert table.num_rows == 4

    def test_producer_is_none(self):
        """ListSource has no producer (root source)."""
        source = ListSource(name="item", data=["a"])
        assert source.producer is None

    def test_upstreams_is_empty(self):
        """ListSource has empty upstreams."""
        source = ListSource(name="item", data=["a"])
        assert source.upstreams == ()

    def test_integer_elements(self):
        """ListSource works with integer elements."""
        source = ListSource(name="num", data=[10, 20, 30])
        pairs = list(source.iter_packets())
        assert len(pairs) == 3

    def test_output_schema(self):
        """ListSource output_schema has tag and packet fields."""
        source = ListSource(name="item", data=["a", "b"])
        tag_schema, packet_schema = source.output_schema()
        assert "element_index" in tag_schema
        assert "item" in packet_schema


# ===========================================================================
# DerivedSource
# ===========================================================================


class TestDerivedSource:
    """DerivedSource behaviors before and after origin run."""

    def _make_mock_origin(self, records=None):
        """Create a mock origin node for DerivedSource testing."""
        mock_origin = MagicMock()
        mock_origin.content_hash.return_value = MagicMock(
            to_string=MagicMock(return_value="abcdef1234567890")
        )
        mock_origin.output_schema.return_value = (
            Schema({"tag_col": str}),
            Schema({"data_col": int}),
        )
        mock_origin.keys.return_value = (("tag_col",), ("data_col",))
        mock_origin.get_all_records.return_value = records
        return mock_origin

    def test_before_run_empty_stream(self):
        """Before run(), DerivedSource presents an empty stream (zero rows)."""
        mock_origin = self._make_mock_origin(records=None)
        source = DerivedSource(origin=mock_origin)
        table = source.as_table()
        assert table.num_rows == 0

    def test_before_run_correct_schema(self):
        """Before run(), the empty stream has the correct schema columns."""
        mock_origin = self._make_mock_origin(records=None)
        source = DerivedSource(origin=mock_origin)
        table = source.as_table()
        assert "tag_col" in table.column_names
        assert "data_col" in table.column_names

    def test_source_id_derived_prefix(self):
        """DerivedSource auto-generates a source_id with 'derived:' prefix."""
        mock_origin = self._make_mock_origin(records=None)
        source = DerivedSource(origin=mock_origin)
        assert source.source_id.startswith("derived:")

    def test_explicit_source_id(self):
        """Explicit source_id overrides the auto-generated one."""
        mock_origin = self._make_mock_origin(records=None)
        source = DerivedSource(origin=mock_origin, source_id="custom_id")
        assert source.source_id == "custom_id"

    def test_output_schema_delegates_to_origin(self):
        """output_schema delegates to origin node."""
        mock_origin = self._make_mock_origin(records=None)
        source = DerivedSource(origin=mock_origin)
        tag_schema, packet_schema = source.output_schema()
        assert "tag_col" in tag_schema
        assert "data_col" in packet_schema

    def test_keys_delegates_to_origin(self):
        """keys() delegates to origin node."""
        mock_origin = self._make_mock_origin(records=None)
        source = DerivedSource(origin=mock_origin)
        tag_keys, packet_keys = source.keys()
        assert "tag_col" in tag_keys
        assert "data_col" in packet_keys

    def test_after_run_with_records(self):
        """After run(), DerivedSource presents the computed records."""
        records_table = pa.table(
            {
                "tag_col": pa.array(["a", "b"], type=pa.large_string()),
                "data_col": pa.array([1, 2], type=pa.int64()),
            }
        )
        mock_origin = self._make_mock_origin(records=records_table)
        source = DerivedSource(origin=mock_origin)
        table = source.as_table()
        assert table.num_rows == 2
