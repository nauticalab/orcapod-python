"""Columns whose restored Arrow type is null hash, and hash alike after storage (ITL-897).

An all-null or empty ``pd.ArrowDtype`` decimal column, and an all-null or empty ``object``
column, have the Arrow type ``null`` once restored: storage cannot keep the decimal type of
a column with no value. Starfix refuses a null column, so the hash digests such a column as
an all-null ``large_string`` column of the same length. These tests use the strict round
trip (no write or read failure is tolerated).
"""

from __future__ import annotations

from typing import Any

import pandas as pd
import pyarrow as pa
import pytest

from orcapod.contexts import get_default_context
from orcapod.logical_types.pandas_type import (
    LogicalPandasDataFrame,
    LogicalPandasSeries,
)


def _hash(value: Any) -> str:
    return get_default_context().semantic_hasher.hash_object(value).to_string()


def _through_storage(value: Any) -> Any:
    logical = (
        LogicalPandasSeries()
        if isinstance(value, pd.Series)
        else LogicalPandasDataFrame()
    )
    return logical.storage_to_python(logical.python_to_storage(value))


def _decimal(values: list[None]) -> pd.Series:
    return pd.Series(pd.array(values, dtype=pd.ArrowDtype(pa.decimal128(5, 2))))


def _object(values: list[None]) -> pd.Series:
    return pd.Series(values, dtype=object)


SHAPES = {
    "decimal all null": lambda: _decimal([None, None]),
    "decimal empty": lambda: _decimal([]),
    "object all null": lambda: _object([None, None]),
    "object empty": lambda: _object([]),
}


@pytest.mark.parametrize("shape", SHAPES)
class TestNullColumnRoundTrip:
    """``hash(read(write(v))) == hash(v)`` for each shape, as a Series and as a column."""

    def test_series(self, shape: str) -> None:
        value = SHAPES[shape]()
        assert _hash(value) == _hash(_through_storage(value))

    def test_dataframe_column(self, shape: str) -> None:
        value = pd.DataFrame(
            {"c": SHAPES[shape](), "d": SHAPES[shape]().astype(object)}
        )
        assert _hash(value) == _hash(_through_storage(value))


class TestNullColumnIdentity:
    """What the rule makes equal and what it keeps apart."""

    @pytest.mark.parametrize("rows", [0, 2])
    def test_decimal_and_object_null_columns_hash_equal(self, rows: int) -> None:
        """Both are ``object`` once restored, so storage cannot tell them apart."""
        decimal = _decimal([None] * rows)
        plain = _object([None] * rows)
        assert _hash(decimal) == _hash(plain)
        assert _hash(decimal.to_frame("c")) == _hash(plain.to_frame("c"))

    def test_row_count_and_dtype_still_count(self) -> None:
        """The digest keeps the height, and a real ``string`` column stays distinct."""
        assert _hash(_object([None])) != _hash(_object([None, None]))
        assert _hash(_object([None, None])) != _hash(
            pd.Series([None, None], dtype="string")
        )


def _arrow(arrow_type: pa.DataType, rows: int) -> pd.Series:
    return pd.Series(pd.array([None] * rows, dtype=pd.ArrowDtype(arrow_type)))


@pytest.mark.parametrize("rows", [0, 2])
class TestArrowNullDtypeStaysDistinct:
    """``pd.ArrowDtype(pa.null())`` is not ``pd.ArrowDtype(pa.large_string())``."""

    def test_series_hash_differently(self, rows: int) -> None:
        """Both dtypes survive storage, so both count in the hash."""
        assert _hash(_arrow(pa.null(), rows)) != _hash(_arrow(pa.large_string(), rows))

    def test_dataframe_columns_hash_differently(self, rows: int) -> None:
        null = _arrow(pa.null(), rows).to_frame("c")
        text = _arrow(pa.large_string(), rows).to_frame("c")
        assert _hash(null) != _hash(text)

    @pytest.mark.parametrize("arrow_type", [pa.null(), pa.large_string()])
    def test_round_trip(self, rows: int, arrow_type: pa.DataType) -> None:
        series = _arrow(arrow_type, rows)
        frame = series.to_frame("c")
        assert _hash(series) == _hash(_through_storage(series))
        assert _hash(frame) == _hash(_through_storage(frame))
