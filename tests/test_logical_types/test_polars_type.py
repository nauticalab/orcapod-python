"""Tests for ``LogicalPolarsDataFrame`` (ITL-578).

The round trip is type strict: every case asserts that the Polars schema read back equals
the one written, not only that the values compare equal.
"""

from __future__ import annotations

import datetime as dt
import decimal
import io
from collections.abc import Callable

import numpy as np
import pandas as pd
import polars as pl
import pyarrow as pa
import pytest
from polars.testing import assert_frame_equal, assert_series_equal

from orcapod.logical_types import (
    LogicalNumpyArray,
    LogicalPolarsDataFrame,
    LogicalPolarsSeries,
)
from orcapod.logical_types.protocols import LogicalTypeProtocol


def _round_trip(df: pl.DataFrame) -> pl.DataFrame:
    """Return ``df`` after writing it to storage and reading it back."""
    lt = LogicalPolarsDataFrame()
    return lt.storage_to_python(lt.python_to_storage(df))


def _assert_exact(back: pl.DataFrame, df: pl.DataFrame) -> None:
    """Assert ``back`` equals ``df`` in schema and in values."""
    assert back.schema == df.schema
    assert_frame_equal(back, df)


# One column per case. Each builder returns a Series whose dtype is the one under test.
_COLUMN_CASES: dict[str, Callable[[], pl.Series]] = {
    "int8_null": lambda: pl.Series([1, None, -3], dtype=pl.Int8),
    "int16": lambda: pl.Series([1, 2, 3], dtype=pl.Int16),
    "int32": lambda: pl.Series([1, 2, 3], dtype=pl.Int32),
    "int64": lambda: pl.Series([1, 2, 3], dtype=pl.Int64),
    "uint8": lambda: pl.Series([1, 2, 255], dtype=pl.UInt8),
    "uint16": lambda: pl.Series([1, 2, 3], dtype=pl.UInt16),
    "uint32": lambda: pl.Series([1, 2, 3], dtype=pl.UInt32),
    "uint64": lambda: pl.Series([1, 2, 2**64 - 1], dtype=pl.UInt64),
    "float16": lambda: pl.Series([1.0, 2.5, None], dtype=pl.Float16),
    "float32": lambda: pl.Series([1.0, None, 3.5], dtype=pl.Float32),
    "float64": lambda: pl.Series([1.0, float("nan"), None], dtype=pl.Float64),
    "string_null": lambda: pl.Series(["a", None, ""], dtype=pl.String),
    "binary_null": lambda: pl.Series([b"x", None, b""], dtype=pl.Binary),
    "boolean": lambda: pl.Series([True, None, False], dtype=pl.Boolean),
    "categorical": lambda: pl.Series(["b", "a", None], dtype=pl.Categorical),
    "enum": lambda: pl.Series(["x", "y", None], dtype=pl.Enum(["y", "x", "z"])),
    "decimal_10_2": lambda: pl.Series(
        [decimal.Decimal("1.23"), None, decimal.Decimal("-9.99")],
        dtype=pl.Decimal(10, 2),
    ),
    "decimal_38_2": lambda: pl.Series(
        [decimal.Decimal("1.23"), None, decimal.Decimal("0.01")],
        dtype=pl.Decimal(38, 2),
    ),
    "datetime_ms_tz": lambda: pl.Series(
        [dt.datetime(2020, 1, 1, 12), None, dt.datetime(2021, 6, 1)],
        dtype=pl.Datetime("ms", "America/New_York"),
    ),
    "datetime_ns": lambda: pl.Series(
        [dt.datetime(2020, 1, 1), None, dt.datetime(1970, 1, 1)],
        dtype=pl.Datetime("ns"),
    ),
    "datetime_us_utc": lambda: pl.Series(
        [dt.datetime(2020, 1, 1), None, dt.datetime(1999, 12, 31)],
        dtype=pl.Datetime("us", "UTC"),
    ),
    "date": lambda: pl.Series([dt.date(2020, 1, 1), None, dt.date(1900, 1, 1)]),
    "time": lambda: pl.Series([dt.time(1, 2, 3), None, dt.time(23, 59)]),
    "duration_ms": lambda: pl.Series(
        [dt.timedelta(seconds=1), None, dt.timedelta(days=-1)],
        dtype=pl.Duration("ms"),
    ),
    "duration_ns": lambda: pl.Series(
        [dt.timedelta(microseconds=1), None, dt.timedelta(0)],
        dtype=pl.Duration("ns"),
    ),
    "list_int_null": lambda: pl.Series([[1, 2], None, []], dtype=pl.List(pl.Int64)),
    "list_string": lambda: pl.Series([["a"], [], [None]], dtype=pl.List(pl.String)),
    "list_categorical": lambda: pl.Series(
        [["a"], [], ["b", "a"]], dtype=pl.List(pl.Categorical)
    ),
    "list_list_uint8": lambda: pl.Series(
        [[[1], []], None, [[2, 3]]], dtype=pl.List(pl.List(pl.UInt8))
    ),
    "array_int16": lambda: pl.Series(
        [[1, 2], [3, 4], None], dtype=pl.Array(pl.Int16, 2)
    ),
    "array_2d": lambda: pl.Series(
        [[[1, 2]], [[3, 4]], [[5, 6]]], dtype=pl.Array(pl.Int16, (1, 2))
    ),
    "struct_null": lambda: pl.Series(
        [{"a": 1, "b": "x"}, None, {"a": None, "b": "z"}],
        dtype=pl.Struct({"a": pl.Int64, "b": pl.String}),
    ),
    "struct_field_order": lambda: pl.Series(
        [{"z": 1, "a": 2}, {"z": 3, "a": 4}, None],
        dtype=pl.Struct({"z": pl.Int32, "a": pl.Int32}),
    ),
    "struct_enum": lambda: pl.Series(
        [{"e": "x"}, None, {"e": "y"}],
        dtype=pl.Struct({"e": pl.Enum(["x", "y"])}),
    ),
    "struct_list_datetime": lambda: pl.Series(
        [{"t": [dt.datetime(2020, 1, 1)]}, {"t": []}, None],
        dtype=pl.Struct({"t": pl.List(pl.Datetime("us", "Europe/Berlin"))}),
    ),
    "null": lambda: pl.Series([None, None, None], dtype=pl.Null),
}


class TestLogicalPolarsDataFrameProtocol:
    """Names and types ``LogicalPolarsDataFrame`` reports through the protocol."""

    def test_satisfies_protocol(self) -> None:
        """A ``LogicalPolarsDataFrame`` is a ``LogicalTypeProtocol``."""
        assert isinstance(LogicalPolarsDataFrame(), LogicalTypeProtocol)

    def test_logical_type_name(self) -> None:
        """The logical type name is ``orcapod.polars.dataframe``."""
        assert LogicalPolarsDataFrame().logical_type_name == "orcapod.polars.dataframe"

    def test_python_type(self) -> None:
        """The Python type is ``pl.DataFrame``."""
        assert LogicalPolarsDataFrame().python_type is pl.DataFrame

    def test_arrow_extension_name(self) -> None:
        """The Arrow extension type is named ``orcapod.polars.dataframe``."""
        ext = LogicalPolarsDataFrame().get_arrow_extension_type()
        assert ext.extension_name == "orcapod.polars.dataframe"

    def test_arrow_storage_type_is_large_binary(self) -> None:
        """The Arrow extension type stores ``large_binary``."""
        ext = LogicalPolarsDataFrame().get_arrow_extension_type()
        assert ext.storage_type == pa.large_binary()

    def test_polars_extension_name(self) -> None:
        """The Polars extension type is named ``orcapod.polars.dataframe``."""
        ext = LogicalPolarsDataFrame().get_polars_extension_type()
        assert ext.ext_name() == "orcapod.polars.dataframe"

    def test_extension_types_are_cached(self) -> None:
        """Two instances share one Arrow extension type and equal Polars ones."""
        a, b = LogicalPolarsDataFrame(), LogicalPolarsDataFrame()
        assert a.get_arrow_extension_type() is b.get_arrow_extension_type()
        assert a.get_polars_extension_type() == b.get_polars_extension_type()


class TestLogicalPolarsDataFrameStorage:
    """The bytes a frame is stored as, and what is refused at write time."""

    def test_storage_is_polars_own_ipc_file(self) -> None:
        """The stored cell is what ``DataFrame.write_ipc`` writes with Polars' defaults."""
        df = pl.DataFrame({"x": [1, 2], "s": ["a", None]})
        stored = LogicalPolarsDataFrame().python_to_storage(df)
        assert isinstance(stored, bytes)
        assert stored.startswith(b"ARROW1")  # the Arrow IPC file format, not a stream
        native = io.BytesIO()
        df.write_ipc(native)
        assert stored == native.getvalue()

    def test_stored_bytes_are_read_by_polars_read_ipc(self) -> None:
        """``pl.read_ipc`` reads the stored bytes back as the exact frame."""
        df = pl.DataFrame({"x": pl.Series([1, 2], dtype=pl.Int8)})
        stored = LogicalPolarsDataFrame().python_to_storage(df)
        back = pl.read_ipc(io.BytesIO(stored), memory_map=False)
        _assert_exact(back, df)

    def test_chunked_frame_reads_back_with_equal_content(self) -> None:
        """Chunking is not stored: the frame comes back equal, in one chunk."""
        df = pl.concat(
            [pl.DataFrame({"x": [1]}), pl.DataFrame({"x": [2]})], rechunk=False
        )
        assert df.n_chunks() == 2
        back = _round_trip(df)
        _assert_exact(back, df)
        assert back.n_chunks() == 1

    def test_non_dataframe_raises_type_error(self) -> None:
        """Storing a ``pl.Series`` or a pandas frame raises ``TypeError``."""
        lt = LogicalPolarsDataFrame()
        with pytest.raises(TypeError, match="pl.DataFrame"):
            lt.python_to_storage(pl.Series("x", [1]))
        with pytest.raises(TypeError, match="pl.DataFrame"):
            lt.python_to_storage(pd.DataFrame({"x": [1]}))


class TestLogicalPolarsDataFrameRoundTrip:
    """Frames read back from storage with the exact schema and values."""

    @pytest.mark.parametrize("name", list(_COLUMN_CASES))
    def test_single_column_round_trip_is_exact(self, name: str) -> None:
        """A one-column frame of each dtype case reads back exactly."""
        df = pl.DataFrame({name: _COLUMN_CASES[name]()})
        _assert_exact(_round_trip(df), df)

    def test_wide_frame_keeps_column_order_and_names(self) -> None:
        """A frame of every dtype case keeps its unsorted column order and names."""
        # Column names are deliberately not in sorted order.
        names = list(reversed(_COLUMN_CASES))
        df = pl.DataFrame({name: _COLUMN_CASES[name]() for name in names})
        back = _round_trip(df)
        assert back.columns == names
        _assert_exact(back, df)

    def test_enum_category_order_is_kept(self) -> None:
        """An ``Enum`` column keeps its categories in their declared order."""
        df = pl.DataFrame({"e": pl.Series(["a"], dtype=pl.Enum(["c", "a", "b"]))})
        back = _round_trip(df)
        assert back.schema["e"] == pl.Enum(["c", "a", "b"])
        assert back.schema["e"].categories.to_list() == ["c", "a", "b"]  # pyright: ignore[reportAttributeAccessIssue] # Enum dtype has categories

    def test_empty_frame_keeps_schema(self) -> None:
        """A zero-row frame reads back with its full schema."""
        df = pl.DataFrame(
            schema={
                "i": pl.Int8,
                "s": pl.Struct({"e": pl.Enum(["p", "q"])}),
                "a": pl.Array(pl.Float32, 3),
                "t": pl.Datetime("ms", "Asia/Tokyo"),
            }
        )
        back = _round_trip(df)
        assert back.height == 0
        _assert_exact(back, df)

    def test_zero_column_frame(self) -> None:
        """A frame with no columns reads back with shape ``(0, 0)``."""
        df = pl.DataFrame()
        back = _round_trip(df)
        assert back.shape == (0, 0)

    def test_empty_column_name_is_kept(self) -> None:
        """A column named ``""`` keeps its name (``pl.from_arrow`` would rename it)."""
        df = pl.DataFrame({"": [1, 2], "x": ["a", "b"]})
        _assert_exact(_round_trip(df), df)

    def test_empty_name_next_to_column_0(self) -> None:
        """Columns named ``""`` and ``column_0`` both keep their names."""
        df = pl.DataFrame({"": [1], "column_0": [2]})
        _assert_exact(_round_trip(df), df)

    def test_chunked_frame_round_trips(self) -> None:
        """A frame in two chunks reads back exactly."""
        df = pl.concat(
            [pl.DataFrame({"x": [1, None]}), pl.DataFrame({"x": [3]})], rechunk=False
        )
        _assert_exact(_round_trip(df), df)

    def test_sliced_frame_round_trips(self) -> None:
        """A slice of a frame reads back exactly."""
        df = pl.DataFrame({"s": ["a", "b", "c", "d"], "l": [[1], [2], [3], [4]]})
        sliced = df.slice(1, 2)
        _assert_exact(_round_trip(sliced), sliced)


class TestLogicalPolarsDataFrameRejectsLossyDtypes:
    """Dtypes that cannot round-trip exactly are rejected at write time."""

    @pytest.mark.parametrize(
        ("series", "dtype_text"),
        [
            (pl.Series("col", [object(), object()], dtype=pl.Object), "Object"),
            (pl.Series("col", [1, 2], dtype=pl.Int128), "Int128"),
            (pl.Series("col", [1, 2], dtype=pl.UInt128), "UInt128"),
        ],
    )
    def test_top_level_unsupported_dtype(
        self, series: pl.Series, dtype_text: str
    ) -> None:
        """An ``Object``, ``Int128`` or ``UInt128`` column raises, naming the column."""
        df = pl.DataFrame([pl.Series("ok", [1, 2]), series])
        with pytest.raises(ValueError, match=rf"'col'.*{dtype_text}"):
            LogicalPolarsDataFrame().python_to_storage(df)

    # Polars 1.41 cannot build an Object nested in a List or Struct, so nested rejection is
    # exercised with Int128, which it can nest.
    def test_int128_nested_in_list(self) -> None:
        """``Int128`` inside a list of arrays raises, naming the column."""
        df = pl.DataFrame(
            [pl.Series("nested", [[[1]]], dtype=pl.List(pl.Array(pl.Int128, 1)))]
        )
        with pytest.raises(ValueError, match=r"'nested'.*contains Int128"):
            LogicalPolarsDataFrame().python_to_storage(df)

    def test_int128_nested_in_struct(self) -> None:
        """``Int128`` in a struct field raises, naming the column."""
        df = pl.DataFrame(
            [pl.Series("st", [{"v": 1}], dtype=pl.Struct({"v": pl.Int128}))]
        )
        with pytest.raises(ValueError, match=r"'st'.*Int128"):
            LogicalPolarsDataFrame().python_to_storage(df)

    def test_polars_extension_dtype(self) -> None:
        """A Polars extension dtype column raises, naming the column and extension."""
        logical_type = LogicalNumpyArray()
        ext = logical_type.get_polars_extension_type()
        # The stored row of one array, in the storage type of ``orcapod.numpy.ndarray``.
        storage = pa.array(
            [logical_type.python_to_storage(np.zeros(2))],
            type=logical_type.get_arrow_extension_type().storage_type,
        )
        stored = pl.from_arrow(storage)
        assert isinstance(stored, pl.Series)
        df = pl.DataFrame([stored.alias("arr").ext.to(ext)])
        with pytest.raises(ValueError, match=r"'arr'.*orcapod\.numpy\.ndarray"):
            LogicalPolarsDataFrame().python_to_storage(df)


class TestLogicalPolarsSeries:
    """``LogicalPolarsSeries`` names, exact round trip and refusals."""

    def test_protocol_and_names(self) -> None:
        """The Series logical type satisfies the protocol and reports its names."""
        lt = LogicalPolarsSeries()
        assert isinstance(lt, LogicalTypeProtocol)
        assert lt.logical_type_name == "orcapod.polars.series"
        assert lt.python_type is pl.Series
        ext = lt.get_arrow_extension_type()
        assert ext.extension_name == "orcapod.polars.series"
        assert ext.storage_type == pa.large_binary()
        assert lt.get_polars_extension_type().ext_name() == "orcapod.polars.series"

    @pytest.mark.parametrize("name", list(_COLUMN_CASES))
    def test_round_trip_is_exact(self, name: str) -> None:
        """A Series of each dtype case reads back with its name, dtype and values."""
        lt = LogicalPolarsSeries()
        series = _COLUMN_CASES[name]().alias(name)
        back = lt.storage_to_python(lt.python_to_storage(series))
        assert back.name == name
        assert back.dtype == series.dtype
        assert_series_equal(back, series)

    @pytest.mark.parametrize("name", ["", "metric", "with space"])
    def test_name_is_kept(self, name: str) -> None:
        """A Series keeps its name, the empty name included."""
        lt = LogicalPolarsSeries()
        series = pl.Series(name, [1, 2], dtype=pl.UInt8)
        assert lt.storage_to_python(lt.python_to_storage(series)).name == name

    def test_empty_series(self) -> None:
        """An empty ``Enum`` Series reads back with its dtype."""
        lt = LogicalPolarsSeries()
        series = pl.Series("e", [], dtype=pl.Enum(["a", "b"]))
        back = lt.storage_to_python(lt.python_to_storage(series))
        assert_series_equal(back, series)
        assert back.dtype == pl.Enum(["a", "b"])

    def test_unsupported_dtype_is_rejected(self) -> None:
        """An ``Int128`` Series raises ``ValueError`` naming the Series."""
        with pytest.raises(ValueError, match=r"'big'.*Int128"):
            LogicalPolarsSeries().python_to_storage(
                pl.Series("big", [1], dtype=pl.Int128)
            )

    def test_non_series_raises_type_error(self) -> None:
        """Storing a ``pl.DataFrame`` as a Series raises ``TypeError``."""
        with pytest.raises(TypeError, match="pl.Series"):
            LogicalPolarsSeries().python_to_storage(pl.DataFrame({"x": [1]}))
