"""Specification-derived tests for ArrowTableStream.

Tests documented behaviors of ArrowTableStream construction, immutability,
schema/key introspection, iteration, table output, ColumnConfig filtering,
and format conversions.
"""

import pyarrow as pa
import pytest

from orcapod.core.datagrams import Data, Key
from orcapod.core.streams import ArrowTableStream
from orcapod.types import ColumnConfig, Schema


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _simple_table(n_rows: int = 3) -> pa.Table:
    """A table with one key-eligible column and one data column."""
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


def _multi_data_table(n_rows: int = 3) -> pa.Table:
    """A table with one key column and two data columns."""
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
    key_columns: list[str] | None = None,
    n_rows: int = 3,
    **kwargs,
) -> ArrowTableStream:
    key_columns = key_columns if key_columns is not None else ["id"]
    return ArrowTableStream(_simple_table(n_rows), key_columns=key_columns, **kwargs)


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


class TestConstruction:
    """ArrowTableStream construction from a pa.Table."""

    def test_basic_construction(self):
        """Stream can be created from a pa.Table with key_columns."""
        stream = _make_stream()
        assert stream is not None

    def test_construction_with_system_key_columns(self):
        """Stream accepts system_key_columns parameter."""
        table = pa.table(
            {
                "id": pa.array([1, 2], type=pa.int64()),
                "value": pa.array(["a", "b"], type=pa.large_string()),
                "sys": pa.array(["s1", "s2"], type=pa.large_string()),
            }
        )
        stream = ArrowTableStream(
            table, key_columns=["id"], system_key_columns=["sys"]
        )
        assert stream is not None

    def test_construction_with_source_info(self):
        """Stream accepts source_info dict parameter."""
        stream = ArrowTableStream(
            _simple_table(),
            key_columns=["id"],
            source_info={"value": "test_source::row_0"},
        )
        assert stream is not None

    def test_construction_with_producer_and_upstreams(self):
        """Stream accepts producer and upstreams parameters."""
        upstream = _make_stream()
        # producer=None is the default; just verify upstreams tuple is stored
        stream = ArrowTableStream(
            _simple_table(), key_columns=["id"], upstreams=(upstream,)
        )
        assert stream.upstreams == (upstream,)
        assert stream.producer is None

    def test_no_data_columns_raises_value_error(self):
        """Stream requires at least one data column; ValueError if none."""
        table = pa.table({"id": pa.array([1, 2, 3], type=pa.int64())})
        with pytest.raises(ValueError):
            ArrowTableStream(table, key_columns=["id"])

    def test_no_key_columns_is_valid(self):
        """All columns may be data columns (no keys)."""
        table = pa.table({"value": pa.array(["a", "b"], type=pa.large_string())})
        stream = ArrowTableStream(table, key_columns=[])
        key_keys, data_keys = stream.keys()
        assert key_keys == ()
        assert "value" in data_keys

    def test_multiple_key_columns(self):
        """Stream supports multiple key columns."""
        table = pa.table(
            {
                "t1": pa.array([1, 2], type=pa.int64()),
                "t2": pa.array(["a", "b"], type=pa.large_string()),
                "val": pa.array([10.0, 20.0], type=pa.float64()),
            }
        )
        stream = ArrowTableStream(table, key_columns=["t1", "t2"])
        key_keys, data_keys = stream.keys()
        assert set(key_keys) == {"t1", "t2"}
        assert data_keys == ("val",)

    def test_multiple_data_columns(self):
        """Stream supports multiple data columns."""
        stream = ArrowTableStream(
            _multi_data_table(), key_columns=["id"]
        )
        _, data_keys = stream.keys()
        assert set(data_keys) == {"x", "y"}


# ---------------------------------------------------------------------------
# keys()
# ---------------------------------------------------------------------------


class TestKeys:
    """keys() returns (key_keys, data_keys) tuples."""

    def test_keys_returns_tuple_of_tuples(self):
        stream = _make_stream()
        result = stream.keys()
        assert isinstance(result, tuple)
        assert len(result) == 2
        key_keys, data_keys = result
        assert isinstance(key_keys, tuple)
        assert isinstance(data_keys, tuple)

    def test_keys_correct_split(self):
        stream = _make_stream(key_columns=["id"])
        key_keys, data_keys = stream.keys()
        assert "id" in key_keys
        assert "value" in data_keys
        assert "id" not in data_keys
        assert "value" not in key_keys

    def test_keys_with_column_config_system_keys(self):
        """When system_keys=True, system key columns appear in key_keys."""
        table = pa.table(
            {
                "id": pa.array([1], type=pa.int64()),
                "value": pa.array(["a"], type=pa.large_string()),
                "sys_col": pa.array(["s"], type=pa.large_string()),
            }
        )
        stream = ArrowTableStream(
            table, key_columns=["id"], system_key_columns=["sys_col"]
        )
        key_keys_default, _ = stream.keys()
        key_keys_all, _ = stream.keys(columns=ColumnConfig(system_keys=True))
        # Default: system keys excluded from keys
        assert len(key_keys_all) > len(key_keys_default)

    def test_keys_with_all_info(self):
        """all_info=True includes system keys in key_keys."""
        table = pa.table(
            {
                "id": pa.array([1], type=pa.int64()),
                "value": pa.array(["a"], type=pa.large_string()),
                "sys_col": pa.array(["s"], type=pa.large_string()),
            }
        )
        stream = ArrowTableStream(
            table, key_columns=["id"], system_key_columns=["sys_col"]
        )
        key_keys_all, _ = stream.keys(all_info=True)
        assert len(key_keys_all) > 1  # id + system key(s)

    def test_keys_no_key_columns(self):
        """With no key columns, key_keys is empty."""
        table = pa.table(
            {"a": pa.array([1], type=pa.int64()), "b": pa.array([2], type=pa.int64())}
        )
        stream = ArrowTableStream(table, key_columns=[])
        key_keys, data_keys = stream.keys()
        assert key_keys == ()
        assert set(data_keys) == {"a", "b"}


# ---------------------------------------------------------------------------
# output_schema()
# ---------------------------------------------------------------------------


class TestOutputSchema:
    """output_schema() returns (key_schema, data_schema) as Schema objects."""

    def test_returns_tuple_of_schemas(self):
        stream = _make_stream()
        key_schema, data_schema = stream.output_schema()
        assert isinstance(key_schema, Schema)
        assert isinstance(data_schema, Schema)

    def test_schema_field_names_match_keys(self):
        stream = _make_stream(key_columns=["id"])
        key_schema, data_schema = stream.output_schema()
        key_keys, data_keys = stream.keys()
        assert set(key_schema.keys()) == set(key_keys)
        assert set(data_schema.keys()) == set(data_keys)

    def test_schema_types_match_table_column_types(self):
        """output_schema types must be consistent with the actual data in as_table."""
        stream = _make_stream(key_columns=["id"])
        key_schema, data_schema = stream.output_schema()
        # key schema type for "id" should be int
        assert key_schema["id"] is int
        # data schema type for "value" should be str
        assert data_schema["value"] is str

    def test_schema_with_multiple_types(self):
        """Schema correctly reflects different column types."""
        schema = pa.schema([
            pa.field("key", pa.int64(), nullable=False),
            pa.field("col_int", pa.int64(), nullable=False),
            pa.field("col_str", pa.large_string(), nullable=False),
            pa.field("col_float", pa.float64(), nullable=False),
        ])
        table = pa.table(
            {
                "key": pa.array([1], type=pa.int64()),
                "col_int": pa.array([42], type=pa.int64()),
                "col_str": pa.array(["hello"], type=pa.large_string()),
                "col_float": pa.array([3.14], type=pa.float64()),
            },
            schema=schema,
        )
        stream = ArrowTableStream(table, key_columns=["key"])
        key_schema, data_schema = stream.output_schema()
        assert key_schema["key"] is int
        assert data_schema["col_int"] is int
        assert data_schema["col_str"] is str
        assert data_schema["col_float"] is float

    def test_schema_with_system_keys_config(self):
        """output_schema with system_keys=True includes system key fields."""
        table = pa.table(
            {
                "id": pa.array([1], type=pa.int64()),
                "value": pa.array(["a"], type=pa.large_string()),
                "sys": pa.array(["s"], type=pa.large_string()),
            }
        )
        stream = ArrowTableStream(
            table, key_columns=["id"], system_key_columns=["sys"]
        )
        key_schema_default, _ = stream.output_schema()
        key_schema_with_sys, _ = stream.output_schema(
            columns=ColumnConfig(system_keys=True)
        )
        assert len(key_schema_with_sys) > len(key_schema_default)


# ---------------------------------------------------------------------------
# iter_data()
# ---------------------------------------------------------------------------


class TestIterDatas:
    """iter_data() yields (Key, Data) pairs."""

    def test_yields_key_data_pairs(self):
        stream = _make_stream(n_rows=2)
        pairs = list(stream.iter_data())
        assert len(pairs) == 2
        for key, data in pairs:
            assert isinstance(key, Key)
            assert isinstance(data, Data)

    def test_count_matches_row_count(self):
        for n in [1, 5, 10]:
            stream = _make_stream(n_rows=n)
            pairs = list(stream.iter_data())
            assert len(pairs) == n

    def test_iter_data_idempotent(self):
        """Iterating twice produces the same number of results (cached)."""
        stream = _make_stream(n_rows=3)
        first = list(stream.iter_data())
        second = list(stream.iter_data())
        assert len(first) == len(second)

    def test_single_row(self):
        """iter_data works with a single-row table."""
        stream = _make_stream(n_rows=1)
        pairs = list(stream.iter_data())
        assert len(pairs) == 1
        key, data = pairs[0]
        assert isinstance(key, Key)
        assert isinstance(data, Data)

    def test_no_key_columns_still_yields_data(self):
        """iter_data works when there are no key columns."""
        table = pa.table({"value": pa.array(["a", "b"], type=pa.large_string())})
        stream = ArrowTableStream(table, key_columns=[])
        pairs = list(stream.iter_data())
        assert len(pairs) == 2


# ---------------------------------------------------------------------------
# as_table() consistency with iter_data()
# ---------------------------------------------------------------------------


class TestAsTable:
    """as_table() returns a pa.Table consistent with iter_data."""

    def test_as_table_returns_arrow_table(self):
        stream = _make_stream()
        table = stream.as_table()
        assert isinstance(table, pa.Table)

    def test_as_table_row_count_matches_iter_data(self):
        stream = _make_stream(n_rows=4)
        table = stream.as_table()
        pairs = list(stream.iter_data())
        assert table.num_rows == len(pairs)

    def test_as_table_contains_key_and_data_columns(self):
        stream = _make_stream(key_columns=["id"])
        table = stream.as_table()
        assert "id" in table.column_names
        assert "value" in table.column_names

    def test_as_table_column_count_matches_keys(self):
        """Default as_table columns match keys() key + data columns."""
        stream = _make_stream(key_columns=["id"])
        table = stream.as_table()
        key_keys, data_keys = stream.keys()
        expected_cols = set(key_keys) | set(data_keys)
        assert set(table.column_names) == expected_cols

    def test_as_table_data_values_consistent(self):
        """The data in as_table matches the original input data."""
        table_in = _simple_table(3)
        stream = ArrowTableStream(table_in, key_columns=["id"])
        table_out = stream.as_table()
        assert table_out.column("id").to_pylist() == [0, 1, 2]
        assert table_out.column("value").to_pylist() == ["v0", "v1", "v2"]


# ---------------------------------------------------------------------------
# ColumnConfig filtering
# ---------------------------------------------------------------------------


class TestColumnConfigFiltering:
    """ColumnConfig controls which columns appear in keys/schema/table."""

    def test_default_excludes_system_keys(self):
        """Default ColumnConfig excludes system key columns."""
        table = pa.table(
            {
                "id": pa.array([1], type=pa.int64()),
                "val": pa.array(["x"], type=pa.large_string()),
                "stag": pa.array(["t"], type=pa.large_string()),
            }
        )
        stream = ArrowTableStream(
            table, key_columns=["id"], system_key_columns=["stag"]
        )
        key_keys, _ = stream.keys()
        # System key columns are prefixed with _key_ internally
        assert all(not k.startswith("_key_") for k in key_keys)

    def test_all_info_includes_everything(self):
        """all_info=True should include source, context, system_keys columns."""
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

    def test_system_keys_in_as_table(self):
        """system_keys=True includes system key columns in the output table."""
        table = pa.table(
            {
                "id": pa.array([1], type=pa.int64()),
                "val": pa.array(["x"], type=pa.large_string()),
                "stag": pa.array(["t"], type=pa.large_string()),
            }
        )
        stream = ArrowTableStream(
            table, key_columns=["id"], system_key_columns=["stag"]
        )
        table_default = stream.as_table()
        table_with_sys = stream.as_table(columns=ColumnConfig(system_keys=True))
        assert table_with_sys.num_columns > table_default.num_columns

    def test_column_config_as_dict(self):
        """ColumnConfig can be passed as a dict."""
        stream = _make_stream()
        table = stream.as_table(columns={"source": True})
        assert isinstance(table, pa.Table)

    def test_keys_schema_table_consistency_with_config(self):
        """keys(), output_schema(), and as_table() agree under the same ColumnConfig."""
        stream = _make_stream(key_columns=["id"])
        key_keys, data_keys = stream.keys()
        key_schema, data_schema = stream.output_schema()
        table = stream.as_table()

        assert set(key_schema.keys()) == set(key_keys)
        assert set(data_schema.keys()) == set(data_keys)
        expected_cols = set(key_keys) | set(data_keys)
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
        stream = _make_stream(key_columns=["id"])
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

    def test_iter_data_same_on_repeated_calls(self):
        """Iterating multiple times yields consistent data."""
        stream = _make_stream(n_rows=3)
        first = list(stream.iter_data())
        second = list(stream.iter_data())
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
