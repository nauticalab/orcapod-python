"""Tests for LogicalPandasDataFrame and LogicalPandasSeries."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

from orcapod.logical_types.protocols import LogicalTypeProtocol


class TestLogicalPandasDataFrameProtocol:
    def test_isinstance_logical_type(self):
        from orcapod.logical_types.pandas_type import LogicalPandasDataFrame

        assert isinstance(LogicalPandasDataFrame(), LogicalTypeProtocol)

    def test_logical_type_name(self):
        from orcapod.logical_types.pandas_type import LogicalPandasDataFrame

        assert LogicalPandasDataFrame().logical_type_name == "orcapod.pandas.dataframe"

    def test_python_type(self):
        from orcapod.logical_types.pandas_type import LogicalPandasDataFrame

        assert LogicalPandasDataFrame().python_type is pd.DataFrame

    def test_arrow_ext_name(self):
        from orcapod.logical_types.pandas_type import LogicalPandasDataFrame

        assert (
            LogicalPandasDataFrame().get_arrow_extension_type().extension_name
            == "orcapod.pandas.dataframe"
        )

    def test_arrow_ext_storage_type(self):
        from orcapod.logical_types.pandas_type import LogicalPandasDataFrame

        assert (
            LogicalPandasDataFrame().get_arrow_extension_type().storage_type
            == pa.large_binary()
        )

    def test_arrow_ext_is_cached(self):
        from orcapod.logical_types.pandas_type import LogicalPandasDataFrame

        lt = LogicalPandasDataFrame()
        assert lt.get_arrow_extension_type() is lt.get_arrow_extension_type()

    def test_polars_ext_is_the_same_type_every_time(self):
        from orcapod.logical_types.pandas_type import LogicalPandasDataFrame

        lt = LogicalPandasDataFrame()
        assert lt.get_polars_extension_type() == lt.get_polars_extension_type()


class TestLogicalPandasDataFrameStorage:
    def test_python_to_storage_returns_bytes(self):
        from orcapod.logical_types.pandas_type import LogicalPandasDataFrame

        df = pd.DataFrame({"a": [1.0, 2.0], "b": [3, 4]})
        result = LogicalPandasDataFrame().python_to_storage(df)
        assert isinstance(result, bytes)

    def test_non_arrow_column_raises_value_error(self):
        from orcapod.logical_types.pandas_type import LogicalPandasDataFrame

        class _Opaque:
            pass

        df = pd.DataFrame({"bad": [_Opaque(), _Opaque()]})
        with pytest.raises(ValueError, match="cannot convert"):
            LogicalPandasDataFrame().python_to_storage(df)


class TestNamesThatCollideAsText:
    """Storage writes column labels and index names as text; a collision is refused.

    Arrow would write two fields of one name, and the frame read back would have lost its
    index (the levels come back as columns). So the write raises.
    """

    @pytest.mark.parametrize(
        "frame",
        [
            pd.DataFrame(
                {"a": [5]},
                index=pd.MultiIndex.from_tuples([(0, 9)], names=[1, 2]),
            ),
            pd.DataFrame(
                {"a": [5]},
                index=pd.MultiIndex.from_tuples([(0, 9)], names=[1, "1"]),
            ),
            pd.DataFrame({"1": [5]}, index=pd.Index([0], name=1)),
        ],
        ids=["integer level names", "level names 1 and '1'", "column '1', index 1"],
    )
    def test_frame_is_refused(self, frame: pd.DataFrame):
        """Index level names, or an index name and a column label, with one text."""
        from orcapod.logical_types.pandas_type import LogicalPandasDataFrame

        with pytest.raises(ValueError, match="'1'"):
            LogicalPandasDataFrame().python_to_storage(frame)

    def test_labels_read_back_as_duplicates_are_refused(self):
        """The labels ``True`` and ``False`` are written as text and both read back ``True``."""
        from orcapod.logical_types.pandas_type import LogicalPandasDataFrame

        frame = pd.DataFrame({True: [1], False: [2]})
        with pytest.raises(ValueError, match="True"):
            LogicalPandasDataFrame().python_to_storage(frame)

    def test_series_is_refused(self):
        """A Series named ``"1"`` whose index is named ``1``: one field name twice."""
        from orcapod.logical_types.pandas_type import LogicalPandasSeries

        series = pd.Series([5], name="1", index=pd.Index([0], name=1))
        with pytest.raises(ValueError, match="'1'"):
            LogicalPandasSeries().python_to_storage(series)

    def test_index_named_like_a_column_is_stored(self):
        """Not a collision: Arrow gives the index its own field name."""
        from orcapod.logical_types.pandas_type import LogicalPandasDataFrame

        logical = LogicalPandasDataFrame()
        frame = pd.DataFrame({"a": [5]}, index=pd.Index([0], name="a"))
        back = logical.storage_to_python(logical.python_to_storage(frame))
        pd.testing.assert_frame_equal(back, frame)


class TestLogicalPandasDataFrameRoundTrip:
    def _rt(self, df: pd.DataFrame) -> pd.DataFrame:
        from orcapod.logical_types.pandas_type import LogicalPandasDataFrame

        lt = LogicalPandasDataFrame()
        return lt.storage_to_python(lt.python_to_storage(df))

    def test_default_range_index(self):
        df = pd.DataFrame({"x": [1.0, 2.0, 3.0], "y": [4, 5, 6]})
        pd.testing.assert_frame_equal(self._rt(df), df)

    def test_named_index(self):
        df = pd.DataFrame({"x": [10, 20]}, index=pd.Index([100, 200], name="id"))
        pd.testing.assert_frame_equal(self._rt(df), df)

    def test_multiindex(self):
        idx = pd.MultiIndex.from_tuples([("a", 1), ("b", 2)], names=["letter", "num"])
        df = pd.DataFrame({"val": [0.5, 1.5]}, index=idx)
        pd.testing.assert_frame_equal(self._rt(df), df)

    def test_float64_column(self):
        df = pd.DataFrame({"f": np.array([1.1, 2.2, 3.3], dtype=np.float64)})
        pd.testing.assert_frame_equal(self._rt(df), df)

    def test_int32_column(self):
        df = pd.DataFrame({"i": np.array([1, 2, 3], dtype=np.int32)})
        pd.testing.assert_frame_equal(self._rt(df), df)

    def test_uint8_column(self):
        df = pd.DataFrame({"u": np.array([0, 128, 255], dtype=np.uint8)})
        pd.testing.assert_frame_equal(self._rt(df), df)

    def test_string_object_column(self):
        df = pd.DataFrame({"s": ["hello", "world", None]})
        pd.testing.assert_frame_equal(self._rt(df), df)

    def test_nullable_int_column(self):
        df = pd.DataFrame({"n": pd.array([1, 2, None], dtype=pd.Int64Dtype())})
        pd.testing.assert_frame_equal(self._rt(df), df)

    def test_boolean_column(self):
        df = pd.DataFrame({"b": [True, False, True]})
        pd.testing.assert_frame_equal(self._rt(df), df)

    def test_datetime_column(self):
        df = pd.DataFrame({"t": pd.to_datetime(["2024-01-01", "2024-06-01"])})
        pd.testing.assert_frame_equal(self._rt(df), df)

    def test_categorical_column(self):
        df = pd.DataFrame({"c": pd.Categorical(["a", "b", "a"])})
        pd.testing.assert_frame_equal(self._rt(df), df)

    def test_empty_dataframe(self):
        df = pd.DataFrame({"x": pd.Series([], dtype=float)})
        pd.testing.assert_frame_equal(self._rt(df), df)


class TestLogicalPandasSeriesProtocol:
    def test_isinstance_logical_type(self):
        from orcapod.logical_types.pandas_type import LogicalPandasSeries

        assert isinstance(LogicalPandasSeries(), LogicalTypeProtocol)

    def test_logical_type_name(self):
        from orcapod.logical_types.pandas_type import LogicalPandasSeries

        assert LogicalPandasSeries().logical_type_name == "orcapod.pandas.series"

    def test_python_type(self):
        from orcapod.logical_types.pandas_type import LogicalPandasSeries

        assert LogicalPandasSeries().python_type is pd.Series

    def test_arrow_ext_name(self):
        from orcapod.logical_types.pandas_type import LogicalPandasSeries

        assert (
            LogicalPandasSeries().get_arrow_extension_type().extension_name
            == "orcapod.pandas.series"
        )

    def test_arrow_ext_storage_type(self):
        from orcapod.logical_types.pandas_type import LogicalPandasSeries

        assert (
            LogicalPandasSeries().get_arrow_extension_type().storage_type
            == pa.large_binary()
        )

    def test_arrow_ext_is_cached(self):
        from orcapod.logical_types.pandas_type import LogicalPandasSeries

        lt = LogicalPandasSeries()
        assert lt.get_arrow_extension_type() is lt.get_arrow_extension_type()

    def test_polars_ext_is_the_same_type_every_time(self):
        from orcapod.logical_types.pandas_type import LogicalPandasSeries

        lt = LogicalPandasSeries()
        assert lt.get_polars_extension_type() == lt.get_polars_extension_type()


class TestLogicalPandasSeriesRoundTrip:
    def _rt(self, s: pd.Series) -> pd.Series:
        from orcapod.logical_types.pandas_type import LogicalPandasSeries

        lt = LogicalPandasSeries()
        return lt.storage_to_python(lt.python_to_storage(s))

    def test_unnamed_series_name_is_none(self):
        s = pd.Series([1.0, 2.0, 3.0])
        result = self._rt(s)
        pd.testing.assert_series_equal(result, s)
        assert result.name is None

    def test_named_series(self):
        s = pd.Series([10, 20, 30], name="count")
        result = self._rt(s)
        pd.testing.assert_series_equal(result, s)
        assert result.name == "count"

    def test_named_index_preserved(self):
        s = pd.Series([1.0, 2.0], index=pd.Index([10, 20], name="row_id"), name="val")
        pd.testing.assert_series_equal(self._rt(s), s)

    def test_float64_values(self):
        s = pd.Series(np.array([1.1, 2.2], dtype=np.float64), name="f")
        pd.testing.assert_series_equal(self._rt(s), s)

    def test_int32_values(self):
        s = pd.Series(np.array([1, 2, 3], dtype=np.int32), name="i")
        pd.testing.assert_series_equal(self._rt(s), s)

    def test_string_values(self):
        s = pd.Series(["a", "b", None], name="words")
        pd.testing.assert_series_equal(self._rt(s), s)

    def test_bool_values(self):
        s = pd.Series([True, False, True], name="flags")
        pd.testing.assert_series_equal(self._rt(s), s)

    def test_datetime_values(self):
        s = pd.Series(pd.to_datetime(["2024-01-01", "2024-06-01"]), name="ts")
        pd.testing.assert_series_equal(self._rt(s), s)

    def test_empty_series(self):
        s = pd.Series([], dtype=float, name="empty")
        pd.testing.assert_series_equal(self._rt(s), s)

    def test_reserved_name_raises_value_error(self):
        from orcapod.logical_types.pandas_type import LogicalPandasSeries

        s = pd.Series([1.0, 2.0], name="__pandas_series_unnamed__")
        with pytest.raises(ValueError, match="reserved"):
            LogicalPandasSeries().python_to_storage(s)
