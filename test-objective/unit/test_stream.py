"""Specification-derived tests for ArrowTableStream.

Tests documented behaviors of ArrowTableStream construction, immutability,
schema/key introspection, iteration, table output, ColumnConfig filtering,
and format conversions.
"""

import pyarrow as pa
import pytest

from orcapod.core.datagrams import Packet, Tag
from orcapod.core.streams import ArrowTableStream
from orcapod.types import ColumnConfig, Schema


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _simple_table(n_rows: int = 3) -> pa.Table:
    """A table with one tag-eligible column and one packet column."""
    schema = pa.schema([
        pa.field("id", pa.int64(), nullable=False),
        pa.field("value", pa.large_string(), nullable=False),
    ])
    return pa.table(
        {
            "id": pa.array(list(range(n_rows)), type=pa.int64()),
            "value": pa.array([f"v{i}" for i in range(n_rows)], type=pa.large_string()),
        },
        schema=schema,
    )


def _multi_packet_table(n_rows: int = 3) -> pa.Table:
    """A table with one tag column and two packet columns."""
    schema = pa.schema([
        pa.field("id", pa.int64(), nullable=False),
        pa.field("x", pa.int64(), nullable=False),
        pa.field("y", pa.large_string(), nullable=False),
    ])
    return pa.table(
        {
            "id": pa.array(list(range(n_rows)), type=pa.int64()),
            "x": pa.array([i * 10 for i in range(n_rows)], type=pa.int64()),
            "y": pa.array([f"y{i}" for i in range(n_rows)], type=pa.large_string()),
        },
        schema=schema,
    )


def _make_stream(
    tag_columns: list[str] | None = None,
    n_rows: int = 3,
    **kwargs,
) -> ArrowTableStream:
    tag_columns = tag_columns if tag_columns is not None else ["id"]
    return ArrowTableStream(_simple_table(n_rows), tag_columns=tag_columns, **kwargs)


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


class TestConstruction:
    """ArrowTableStream construction from a pa.Table."""

    def test_basic_construction(self):
        """Stream can be created from a pa.Table with tag_columns."""
        stream = _make_stream()
        assert stream is not None

    def test_construction_with_system_tag_columns(self):
        """Stream accepts system_tag_columns parameter."""
        table = pa.table(
            {
                "id": pa.array([1, 2], type=pa.int64()),
                "value": pa.array(["a", "b"], type=pa.large_string()),
                "sys": pa.array(["s1", "s2"], type=pa.large_string()),
            }
        )
        stream = ArrowTableStream(
            table, tag_columns=["id"], system_tag_columns=["sys"]
        )
        assert stream is not None

    def test_construction_with_source_info(self):
        """Stream accepts source_info dict parameter."""
        stream = ArrowTableStream(
            _simple_table(),
            tag_columns=["id"],
            source_info={"value": "test_source::row_0"},
        )
        assert stream is not None

    def test_construction_with_producer_and_upstreams(self):
        """Stream accepts producer and upstreams parameters."""
        upstream = _make_stream()
        # producer=None is the default; just verify upstreams tuple is stored
        stream = ArrowTableStream(
            _simple_table(), tag_columns=["id"], upstreams=(upstream,)
        )
        assert stream.upstreams == (upstream,)
        assert stream.producer is None

    def test_no_packet_columns_raises_value_error(self):
        """Stream requires at least one packet column; ValueError if none."""
        table = pa.table({"id": pa.array([1, 2, 3], type=pa.int64())})
        with pytest.raises(ValueError):
            ArrowTableStream(table, tag_columns=["id"])

    def test_no_tag_columns_is_valid(self):
        """All columns may be packet columns (no tags)."""
        table = pa.table({"value": pa.array(["a", "b"], type=pa.large_string())})
        stream = ArrowTableStream(table, tag_columns=[])
        tag_keys, packet_keys = stream.keys()
        assert tag_keys == ()
        assert "value" in packet_keys

    def test_multiple_tag_columns(self):
        """Stream supports multiple tag columns."""
        table = pa.table(
            {
                "t1": pa.array([1, 2], type=pa.int64()),
                "t2": pa.array(["a", "b"], type=pa.large_string()),
                "val": pa.array([10.0, 20.0], type=pa.float64()),
            }
        )
        stream = ArrowTableStream(table, tag_columns=["t1", "t2"])
        tag_keys, packet_keys = stream.keys()
        assert set(tag_keys) == {"t1", "t2"}
        assert packet_keys == ("val",)

    def test_multiple_packet_columns(self):
        """Stream supports multiple packet columns."""
        stream = ArrowTableStream(
            _multi_packet_table(), tag_columns=["id"]
        )
        _, packet_keys = stream.keys()
        assert set(packet_keys) == {"x", "y"}


# ---------------------------------------------------------------------------
# keys()
# ---------------------------------------------------------------------------


class TestKeys:
    """keys() returns (tag_keys, packet_keys) tuples."""

    def test_keys_returns_tuple_of_tuples(self):
        stream = _make_stream()
        result = stream.keys()
        assert isinstance(result, tuple)
        assert len(result) == 2
        tag_keys, packet_keys = result
        assert isinstance(tag_keys, tuple)
        assert isinstance(packet_keys, tuple)

    def test_keys_correct_split(self):
        stream = _make_stream(tag_columns=["id"])
        tag_keys, packet_keys = stream.keys()
        assert "id" in tag_keys
        assert "value" in packet_keys
        assert "id" not in packet_keys
        assert "value" not in tag_keys

    def test_keys_with_column_config_system_tags(self):
        """When system_tags=True, system tag columns appear in tag_keys."""
        table = pa.table(
            {
                "id": pa.array([1], type=pa.int64()),
                "value": pa.array(["a"], type=pa.large_string()),
                "sys_col": pa.array(["s"], type=pa.large_string()),
            }
        )
        stream = ArrowTableStream(
            table, tag_columns=["id"], system_tag_columns=["sys_col"]
        )
        tag_keys_default, _ = stream.keys()
        tag_keys_all, _ = stream.keys(columns=ColumnConfig(system_tags=True))
        # Default: system tags excluded from keys
        assert len(tag_keys_all) > len(tag_keys_default)

    def test_keys_with_all_info(self):
        """all_info=True includes system tags in tag_keys."""
        table = pa.table(
            {
                "id": pa.array([1], type=pa.int64()),
                "value": pa.array(["a"], type=pa.large_string()),
                "sys_col": pa.array(["s"], type=pa.large_string()),
            }
        )
        stream = ArrowTableStream(
            table, tag_columns=["id"], system_tag_columns=["sys_col"]
        )
        tag_keys_all, _ = stream.keys(all_info=True)
        assert len(tag_keys_all) > 1  # id + system tag(s)

    def test_keys_no_tag_columns(self):
        """With no tag columns, tag_keys is empty."""
        table = pa.table(
            {"a": pa.array([1], type=pa.int64()), "b": pa.array([2], type=pa.int64())}
        )
        stream = ArrowTableStream(table, tag_columns=[])
        tag_keys, packet_keys = stream.keys()
        assert tag_keys == ()
        assert set(packet_keys) == {"a", "b"}


# ---------------------------------------------------------------------------
# output_schema()
# ---------------------------------------------------------------------------


class TestOutputSchema:
    """output_schema() returns (tag_schema, packet_schema) as Schema objects."""

    def test_returns_tuple_of_schemas(self):
        stream = _make_stream()
        tag_schema, packet_schema = stream.output_schema()
        assert isinstance(tag_schema, Schema)
        assert isinstance(packet_schema, Schema)

    def test_schema_field_names_match_keys(self):
        stream = _make_stream(tag_columns=["id"])
        tag_schema, packet_schema = stream.output_schema()
        tag_keys, packet_keys = stream.keys()
        assert set(tag_schema.keys()) == set(tag_keys)
        assert set(packet_schema.keys()) == set(packet_keys)

    def test_schema_types_match_table_column_types(self):
        """output_schema types must be consistent with the actual data in as_table."""
        stream = _make_stream(tag_columns=["id"])
        tag_schema, packet_schema = stream.output_schema()
        # tag schema type for "id" should be int
        assert tag_schema["id"] is int
        # packet schema type for "value" should be str
        assert packet_schema["value"] is str

    def test_schema_with_multiple_types(self):
        """Schema correctly reflects different column types."""
        schema = pa.schema([
            pa.field("tag", pa.int64(), nullable=False),
            pa.field("col_int", pa.int64(), nullable=False),
            pa.field("col_str", pa.large_string(), nullable=False),
            pa.field("col_float", pa.float64(), nullable=False),
        ])
        table = pa.table(
            {
                "tag": pa.array([1], type=pa.int64()),
                "col_int": pa.array([42], type=pa.int64()),
                "col_str": pa.array(["hello"], type=pa.large_string()),
                "col_float": pa.array([3.14], type=pa.float64()),
            },
            schema=schema,
        )
        stream = ArrowTableStream(table, tag_columns=["tag"])
        tag_schema, packet_schema = stream.output_schema()
        assert tag_schema["tag"] is int
        assert packet_schema["col_int"] is int
        assert packet_schema["col_str"] is str
        assert packet_schema["col_float"] is float

    def test_schema_with_system_tags_config(self):
        """output_schema with system_tags=True includes system tag fields."""
        table = pa.table(
            {
                "id": pa.array([1], type=pa.int64()),
                "value": pa.array(["a"], type=pa.large_string()),
                "sys": pa.array(["s"], type=pa.large_string()),
            }
        )
        stream = ArrowTableStream(
            table, tag_columns=["id"], system_tag_columns=["sys"]
        )
        tag_schema_default, _ = stream.output_schema()
        tag_schema_with_sys, _ = stream.output_schema(
            columns=ColumnConfig(system_tags=True)
        )
        assert len(tag_schema_with_sys) > len(tag_schema_default)


# ---------------------------------------------------------------------------
# iter_packets()
# ---------------------------------------------------------------------------


class TestIterPackets:
    """iter_packets() yields (Tag, Packet) pairs."""

    def test_yields_tag_packet_pairs(self):
        stream = _make_stream(n_rows=2)
        pairs = list(stream.iter_packets())
        assert len(pairs) == 2
        for tag, packet in pairs:
            assert isinstance(tag, Tag)
            assert isinstance(packet, Packet)

    def test_count_matches_row_count(self):
        for n in [1, 5, 10]:
            stream = _make_stream(n_rows=n)
            pairs = list(stream.iter_packets())
            assert len(pairs) == n

    def test_iter_packets_idempotent(self):
        """Iterating twice produces the same number of results (cached)."""
        stream = _make_stream(n_rows=3)
        first = list(stream.iter_packets())
        second = list(stream.iter_packets())
        assert len(first) == len(second)

    def test_single_row(self):
        """iter_packets works with a single-row table."""
        stream = _make_stream(n_rows=1)
        pairs = list(stream.iter_packets())
        assert len(pairs) == 1
        tag, packet = pairs[0]
        assert isinstance(tag, Tag)
        assert isinstance(packet, Packet)

    def test_no_tag_columns_still_yields_packets(self):
        """iter_packets works when there are no tag columns."""
        table = pa.table({"value": pa.array(["a", "b"], type=pa.large_string())})
        stream = ArrowTableStream(table, tag_columns=[])
        pairs = list(stream.iter_packets())
        assert len(pairs) == 2


# ---------------------------------------------------------------------------
# as_table() consistency with iter_packets()
# ---------------------------------------------------------------------------


class TestAsTable:
    """as_table() returns a pa.Table consistent with iter_packets."""

    def test_as_table_returns_arrow_table(self):
        stream = _make_stream()
        table = stream.as_table()
        assert isinstance(table, pa.Table)

    def test_as_table_row_count_matches_iter_packets(self):
        stream = _make_stream(n_rows=4)
        table = stream.as_table()
        pairs = list(stream.iter_packets())
        assert table.num_rows == len(pairs)

    def test_as_table_contains_tag_and_packet_columns(self):
        stream = _make_stream(tag_columns=["id"])
        table = stream.as_table()
        assert "id" in table.column_names
        assert "value" in table.column_names

    def test_as_table_column_count_matches_keys(self):
        """Default as_table columns match keys() tag + packet columns."""
        stream = _make_stream(tag_columns=["id"])
        table = stream.as_table()
        tag_keys, packet_keys = stream.keys()
        expected_cols = set(tag_keys) | set(packet_keys)
        assert set(table.column_names) == expected_cols

    def test_as_table_data_values_consistent(self):
        """The data in as_table matches the original input data."""
        table_in = _simple_table(3)
        stream = ArrowTableStream(table_in, tag_columns=["id"])
        table_out = stream.as_table()
        assert table_out.column("id").to_pylist() == [0, 1, 2]
        assert table_out.column("value").to_pylist() == ["v0", "v1", "v2"]


# ---------------------------------------------------------------------------
# ColumnConfig filtering
# ---------------------------------------------------------------------------


class TestColumnConfigFiltering:
    """ColumnConfig controls which columns appear in keys/schema/table."""

    def test_default_excludes_system_tags(self):
        """Default ColumnConfig excludes system tag columns."""
        table = pa.table(
            {
                "id": pa.array([1], type=pa.int64()),
                "val": pa.array(["x"], type=pa.large_string()),
                "stag": pa.array(["t"], type=pa.large_string()),
            }
        )
        stream = ArrowTableStream(
            table, tag_columns=["id"], system_tag_columns=["stag"]
        )
        tag_keys, _ = stream.keys()
        # System tag columns are prefixed with _tag_ internally
        assert all(not k.startswith("_tag_") for k in tag_keys)

    def test_all_info_includes_everything(self):
        """all_info=True should include source, context, system_tags columns."""
        stream = _make_stream()
        table_default = stream.as_table()
        table_all = stream.as_table(all_info=True)
        assert table_all.num_columns >= table_default.num_columns

    def test_source_column_config(self):
        """source=True includes source info columns in as_table."""
        stream = _make_stream()
        table_no_source = stream.as_table()
        table_with_source = stream.as_table(
            columns=ColumnConfig(source=True)
        )
        assert table_with_source.num_columns >= table_no_source.num_columns

    def test_context_column_config(self):
        """context=True includes context columns in as_table."""
        stream = _make_stream()
        table_no_ctx = stream.as_table()
        table_with_ctx = stream.as_table(columns=ColumnConfig(context=True))
        assert table_with_ctx.num_columns >= table_no_ctx.num_columns

    def test_system_tags_in_as_table(self):
        """system_tags=True includes system tag columns in the output table."""
        table = pa.table(
            {
                "id": pa.array([1], type=pa.int64()),
                "val": pa.array(["x"], type=pa.large_string()),
                "stag": pa.array(["t"], type=pa.large_string()),
            }
        )
        stream = ArrowTableStream(
            table, tag_columns=["id"], system_tag_columns=["stag"]
        )
        table_default = stream.as_table()
        table_with_sys = stream.as_table(columns=ColumnConfig(system_tags=True))
        assert table_with_sys.num_columns > table_default.num_columns

    def test_column_config_as_dict(self):
        """ColumnConfig can be passed as a dict."""
        stream = _make_stream()
        table = stream.as_table(columns={"source": True})
        assert isinstance(table, pa.Table)

    def test_keys_schema_table_consistency_with_config(self):
        """keys(), output_schema(), and as_table() agree under the same ColumnConfig."""
        stream = _make_stream(tag_columns=["id"])
        tag_keys, packet_keys = stream.keys()
        tag_schema, packet_schema = stream.output_schema()
        table = stream.as_table()

        assert set(tag_schema.keys()) == set(tag_keys)
        assert set(packet_schema.keys()) == set(packet_keys)
        expected_cols = set(tag_keys) | set(packet_keys)
        assert set(table.column_names) == expected_cols


# ---------------------------------------------------------------------------
# Format conversions
# ---------------------------------------------------------------------------


class TestFormatConversions:
    """as_polars_df(), as_pandas_df(), as_lazy_frame() produce expected types."""

    def test_as_polars_df(self):
        import polars as pl

        stream = _make_stream()
        df = stream.as_polars_df()
        assert isinstance(df, pl.DataFrame)
        assert df.shape[0] == 3

    def test_as_pandas_df(self):
        import pandas as pd

        stream = _make_stream()
        df = stream.as_pandas_df()
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3

    def test_as_lazy_frame(self):
        import polars as pl

        stream = _make_stream()
        lf = stream.as_lazy_frame()
        assert isinstance(lf, pl.LazyFrame)

    def test_as_polars_df_preserves_columns(self):
        """Polars DataFrame has the same columns as as_table."""
        stream = _make_stream(tag_columns=["id"])
        table = stream.as_table()
        df = stream.as_polars_df()
        assert set(df.columns) == set(table.column_names)

    def test_as_pandas_df_preserves_row_count(self):
        """Pandas DataFrame has the same row count."""
        stream = _make_stream(n_rows=5)
        df = stream.as_pandas_df()
        assert len(df) == 5

    def test_as_lazy_frame_collects_to_correct_shape(self):
        """LazyFrame collects to the correct shape."""
        import polars as pl

        stream = _make_stream(n_rows=4)
        lf = stream.as_lazy_frame()
        df = lf.collect()
        assert isinstance(df, pl.DataFrame)
        assert df.shape[0] == 4

    def test_format_conversions_with_column_config(self):
        """Format conversions respect ColumnConfig."""
        import polars as pl

        stream = _make_stream()
        df_default = stream.as_polars_df()
        df_all = stream.as_polars_df(all_info=True)
        assert df_all.shape[1] >= df_default.shape[1]


# ---------------------------------------------------------------------------
# Immutability
# ---------------------------------------------------------------------------


class TestImmutability:
    """ArrowTableStream is immutable -- no public mutation methods."""

    def test_as_table_returns_consistent_data(self):
        """Repeated as_table calls return the same data."""
        stream = _make_stream(n_rows=3)
        t1 = stream.as_table()
        t2 = stream.as_table()
        assert t1.equals(t2)

    def test_producer_is_none_for_standalone_stream(self):
        """A stream created without a producer has producer == None."""
        stream = _make_stream()
        assert stream.producer is None

    def test_upstreams_is_empty_for_standalone_stream(self):
        """A stream created without upstreams has upstreams == ()."""
        stream = _make_stream()
        assert stream.upstreams == ()

    def test_iter_packets_same_on_repeated_calls(self):
        """Iterating multiple times yields consistent data."""
        stream = _make_stream(n_rows=3)
        first = list(stream.iter_packets())
        second = list(stream.iter_packets())
        assert len(first) == len(second) == 3

    def test_output_schema_stable(self):
        """output_schema() returns the same result on repeated calls."""
        stream = _make_stream()
        s1 = stream.output_schema()
        s2 = stream.output_schema()
        assert s1 == s2

    def test_keys_stable(self):
        """keys() returns the same result on repeated calls."""
        stream = _make_stream()
        k1 = stream.keys()
        k2 = stream.keys()
        assert k1 == k2
