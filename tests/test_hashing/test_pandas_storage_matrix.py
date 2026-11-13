"""A pandas value hashes the same before and after storage, over a matrix of values (ITL-897).

``hash(read(write(v))) == hash(v)`` is the rule that makes a stored pandas value a cache hit.
It fails whenever storage changes a property the hash counts, and such properties are easy to
miss one at a time: an ``Int64`` index reads back ``int64``, mixed column labels read back as
text. So this module checks the rule for every index kind, column axis and column dtype it
lists, each on its own, and over the products of representatives of each, for frames and
Series.

A value storage does not accept (the write or the read raises) is outside the rule and is
left out; each test asserts that most of its matrix was accepted, so it cannot pass empty.
"""

from __future__ import annotations

import datetime as dt
import decimal
import itertools
import warnings
import zoneinfo
from typing import Any

import numpy as np
import pandas as pd
import pyarrow as pa

from orcapod.contexts import get_default_context
from orcapod.logical_types.pandas_type import (
    LogicalPandasDataFrame,
    LogicalPandasSeries,
)

# Index kinds: two rows each.
INDEXES: dict[str, pd.Index] = {
    "range": pd.RangeIndex(2),
    "int64": pd.Index([5, 6], dtype="int64"),
    "int32 named": pd.Index([5, 6], dtype="int32", name="id"),
    "object ints": pd.Index([5, 6], dtype=object),
    "float64": pd.Index([0.5, 1.5]),
    "object strings": pd.Index(["p", "q"], dtype=object),
    "string": pd.Index(["p", "q"], dtype="string"),
    "string[pyarrow]": pd.Index(["p", "q"], dtype="string[pyarrow]"),
    "Int64": pd.Index([5, 6], dtype="Int64"),
    "Int64 with NA": pd.Index(pd.array([5, None], dtype="Int64")),
    "Float64": pd.Index([0.5, 1.5], dtype="Float64"),
    "boolean": pd.Index([True, False], dtype="boolean"),
    "bool": pd.Index([True, False]),
    "arrow int64": pd.Index(pd.array([5, 6], dtype="int64[pyarrow]")),
    "categorical": pd.CategoricalIndex(["p", "q"]),
    "categorical, object int categories": pd.CategoricalIndex(
        [1, 2], categories=pd.Index([1, 2], dtype=object)
    ),
    "categorical, string dtype categories": pd.CategoricalIndex(
        ["p", "q"], categories=pd.Index(["p", "q"], dtype="string")
    ),
    "datetime": pd.DatetimeIndex(["2020-01-01", "2020-01-02"]),
    "datetime s": pd.DatetimeIndex(["2020-01-01", "2020-01-02"]).as_unit("s"),
    "datetime UTC": pd.DatetimeIndex(["2020-01-01", "2020-01-02"], tz="UTC"),
    "datetime fixed offset": pd.DatetimeIndex(
        ["2020-01-01", "2020-01-02"], tz=dt.timezone(dt.timedelta(hours=1))
    ),
    "datetime zoneinfo": pd.DatetimeIndex(
        ["2020-01-01", "2020-01-02"], tz=zoneinfo.ZoneInfo("Europe/Berlin")
    ),
    "arrow string": pd.Index(pd.array(["p", "q"], dtype=pd.ArrowDtype(pa.string()))),
    "arrow timestamp": pd.Index(
        pd.array(
            pd.to_datetime(["2020-01-01", "2020-01-02"]),
            dtype=pd.ArrowDtype(pa.timestamp("us")),
        )
    ),
    "UInt8": pd.Index([5, 6], dtype="UInt8"),
    "float32": pd.Index([0.5, 1.5], dtype="float32"),
    "uint16": pd.Index([5, 6], dtype="uint16"),
    "object mixed": pd.Index([5, "q"], dtype=object),
    "object dates": pd.Index([dt.date(2020, 1, 1), dt.date(2020, 1, 2)], dtype=object),
    "object None and str": pd.Index([None, "q"], dtype=object),
    "float with nan": pd.Index([0.5, np.nan]),
    "timedelta": pd.TimedeltaIndex(["1s", "2s"]),
    "period": pd.period_range("2020-01", periods=2, freq="M"),
    "interval": pd.interval_range(0, 2),
    "int name": pd.Index([5, 6], name=7),
    "float name": pd.Index([5, 6], name=1.5),
    "tuple name": pd.Index([5, 6], name=("a", 1)),
    "timestamp name": pd.Index([5, 6], name=pd.Timestamp("2020-01-01")),
    "multi": pd.MultiIndex.from_tuples([(0, "p"), (1, "q")], names=["i", "j"]),
    "multi unnamed": pd.MultiIndex.from_tuples([(0, "p"), (1, "q")]),
    "multi Int64": pd.MultiIndex.from_arrays(
        [pd.array([5, 6], dtype="Int64"), pd.array(["p", "q"], dtype="string")]
    ),
    "multi mixed names": pd.MultiIndex.from_tuples(
        [(0, "p"), (1, "q")], names=[1.5, "j"]
    ),
}

# Column axes: two labels each.
COLUMN_AXES: dict[str, pd.Index] = {
    "strings": pd.Index(["a", "b"], dtype=object),
    "string dtype": pd.Index(["a", "b"], dtype="string"),
    "ints": pd.Index([0, 1]),
    "int32": pd.Index([0, 1], dtype="int32"),
    "object ints": pd.Index([0, 1], dtype=object),
    "floats": pd.Index([0.5, 1.5]),
    "arrow int8": pd.Index([0, 1], dtype="int8[pyarrow]"),
    "arrow double": pd.Index([0.5, 1.5], dtype="double[pyarrow]"),
    "mixed int and str": pd.Index([0, "b"], dtype=object),
    "mixed int and float": pd.Index([1, 2.5], dtype=object),
    "None and str": pd.Index([None, "b"], dtype=object),
    "bools": pd.Index([True, False]),
    "timestamps": pd.DatetimeIndex(["2020-01-01", "2020-01-02"]),
    "timestamps s": pd.DatetimeIndex(["2020-01-01", "2020-01-02"]).as_unit("s"),
    "timestamps UTC": pd.DatetimeIndex(["2020-01-01", "2020-01-02"], tz="UTC"),
    "timestamps fixed offset": pd.DatetimeIndex(
        ["2020-01-01", "2020-01-02"], tz=dt.timezone(dt.timedelta(hours=1))
    ),
    "arrow string": pd.Index(pd.array(["a", "b"], dtype=pd.ArrowDtype(pa.string()))),
    "Int64": pd.Index([0, 1], dtype="Int64"),
    "uint8": pd.Index([0, 1], dtype="uint8"),
    "categorical": pd.CategoricalIndex(["a", "b"]),
    "timedeltas": pd.TimedeltaIndex(["1s", "2s"]),
    "periods": pd.period_range("2020-01", periods=2, freq="M"),
    "named": pd.Index(["a", "b"], dtype=object, name="features"),
    "int name": pd.Index(["a", "b"], dtype=object, name=3),
    "multi": pd.MultiIndex.from_tuples([("a", 1), ("a", 2)]),
    "multi named": pd.MultiIndex.from_tuples([("a", 1), ("b", 2)], names=["x", 2]),
    "multi int32": pd.MultiIndex.from_arrays(
        [pd.Index([1, 2], dtype="int32"), pd.Index(["x", "y"], dtype=object)]
    ),
}

# Column values: two rows each.
COLUMN_VALUES: dict[str, Any] = {
    "int64": np.array([1, 2], dtype="int64"),
    "int8": np.array([1, 2], dtype="int8"),
    "float64 with nan": np.array([1.5, np.nan]),
    "bool": np.array([True, False]),
    "Int64 with NA": pd.array([1, None], dtype="Int64"),
    "boolean with NA": pd.array([True, None], dtype="boolean"),
    "string with NA": pd.array(["x", None], dtype="string"),
    "string[pyarrow]": pd.array(["x", "y"], dtype="string[pyarrow]"),
    "object strings": pd.Series(["x", None], dtype=object),
    "object ints": pd.Series([1, 2], dtype=object),
    "object ints with None": pd.Series([1, None], dtype=object),
    "object bools with None": pd.Series([True, None], dtype=object),
    "object bytes": pd.Series([b"x", b"y"], dtype=object),
    "object dates": pd.Series([dt.date(2020, 1, 1), None], dtype=object),
    "object datetimes": pd.Series([dt.datetime(2020, 1, 1), None], dtype=object),
    "object lists": pd.Series([[1, None], [2]], dtype=object),
    "object nested lists": pd.Series([[[1, None]], [[2]]], dtype=object),
    "object dicts": pd.Series([{"u": 1, "v": [1, None]}, {"u": 2, "v": []}]),
    "categorical": pd.Categorical(["x", "y"], categories=["y", "x", "z"]),
    "ordered categorical": pd.Categorical(["x", "y"], ordered=True),
    "categorical, object int categories": pd.Categorical(
        [1, 2], categories=pd.Index([2, 1, 3], dtype=object)
    ),
    "categorical, int32 categories": pd.Categorical(
        [1, 2], categories=pd.Index([1, 2], dtype="int32")
    ),
    "categorical, float categories": pd.Categorical([0.5, None], categories=[0.5, 1.5]),
    "categorical, object bool categories": pd.Categorical(
        [True, False], categories=pd.Index([True, False], dtype=object)
    ),
    "categorical, string dtype categories": pd.Categorical(
        ["x", "y"], categories=pd.Index(["x", "y"], dtype="string")
    ),
    "categorical, datetime categories": pd.Categorical(
        pd.to_datetime(["2020-01-01", "2020-01-02"])
    ),
    "categorical, Int64 categories": pd.Categorical(
        [1, 2], categories=pd.Index([1, 2], dtype="Int64")
    ),
    "datetime": pd.to_datetime(["2020-01-01", None]),
    "datetime UTC": pd.to_datetime(["2020-01-01", "2020-01-02"]).tz_localize("UTC"),
    "timedelta": pd.to_timedelta(["1s", None]),
    "period": pd.period_range("2020-01", periods=2, freq="M"),
    "interval": pd.arrays.IntervalArray.from_breaks([0, 1, 2]),
    "arrow int64": pd.array([1, None], dtype="int64[pyarrow]"),
    "arrow string": pd.array(["x", None], dtype=pd.ArrowDtype(pa.string())),
    "arrow large_string": pd.array(["x", "y"], dtype=pd.ArrowDtype(pa.large_string())),
    "arrow bool": pd.array([True, None], dtype="bool[pyarrow]"),
    "arrow double": pd.array([0.5, None], dtype="double[pyarrow]"),
    "arrow timestamp": pd.array(
        pd.to_datetime(["2020-01-01", None]),
        dtype=pd.ArrowDtype(pa.timestamp("us")),
    ),
    "arrow timestamp UTC": pd.array(
        pd.to_datetime(["2020-01-01", None]).tz_localize("UTC"),
        dtype=pd.ArrowDtype(pa.timestamp("ns", tz="UTC")),
    ),
    "arrow list": pd.Series([[1, 2], None], dtype=pd.ArrowDtype(pa.list_(pa.int64()))),
    "arrow binary": pd.array([b"x", None], dtype=pd.ArrowDtype(pa.binary())),
    "arrow decimal": pd.array(
        [decimal.Decimal("1.50"), None], dtype=pd.ArrowDtype(pa.decimal128(5, 2))
    ),
    "arrow date32": pd.array(
        [dt.date(2020, 1, 1), None], dtype=pd.ArrowDtype(pa.date32())
    ),
    "arrow duration": pd.array(
        [dt.timedelta(seconds=1), None], dtype=pd.ArrowDtype(pa.duration("s"))
    ),
    "arrow dictionary": pd.Series(
        ["x", "y"], dtype=pd.ArrowDtype(pa.dictionary(pa.int8(), pa.string()))
    ),
    "UInt8 with NA": pd.array([1, None], dtype="UInt8"),
    "Float32 with NA": pd.array([0.5, None], dtype="Float32"),
    "float32": np.array([0.5, 1.5], dtype="float32"),
    "uint16": np.array([1, 2], dtype="uint16"),
    "complex128": np.array([1 + 2j, 3 - 1j]),
    "object decimals": pd.Series([decimal.Decimal("1.5"), None], dtype=object),
    "arrow decimal all null": pd.array(
        [None, None], dtype=pd.ArrowDtype(pa.decimal128(5, 2))
    ),
    "object all null": pd.Series([None, None], dtype=object),
    "arrow null all null": pd.array([None, None], dtype=pd.ArrowDtype(pa.null())),
    "object timedeltas": pd.Series([dt.timedelta(days=1), None], dtype=object),
    "object timestamps UTC": pd.Series(
        [pd.Timestamp("2020-01-01", tz="UTC"), None], dtype=object
    ),
    "object mixed numbers": pd.Series([1, 2.5], dtype=object),
    "object tuples": pd.Series([(1, 2), (3, 4)], dtype=object),
    "object sets": pd.Series([{1, 2}, {3}], dtype=object),
    "object arrays": pd.Series([np.array([1, 2]), np.array([3])], dtype=object),
    "datetime s": pd.to_datetime(["2020-01-01", None]).as_unit("s"),
    "datetime fixed offset": pd.to_datetime(["2020-01-01", "2020-01-02"]).tz_localize(
        dt.timezone(dt.timedelta(hours=1))
    ),
    "datetime timezone.utc": pd.to_datetime(["2020-01-01", "2020-01-02"]).tz_localize(
        dt.timezone.utc
    ),
    "datetime zoneinfo": pd.to_datetime(["2020-01-01", "2020-01-02"]).tz_localize(
        zoneinfo.ZoneInfo("Europe/Berlin")
    ),
    "datetime tz name": pd.to_datetime(["2020-01-01", "2020-01-02"]).tz_localize(
        "America/New_York"
    ),
    "datetime pytz fixed": pd.to_datetime(["2020-01-01", "2020-01-02"]).tz_localize(
        "+05:30"
    ),
    "timedelta s": pd.to_timedelta(["1s", "2s"]).as_unit("s"),
    "period D": pd.period_range("2020-01-01", periods=2, freq="D"),
    "interval float left": pd.arrays.IntervalArray.from_breaks(
        [0.0, 1.0, 2.0], closed="left"
    ),
    "interval datetime": pd.arrays.IntervalArray.from_breaks(
        pd.to_datetime(["2020-01-01", "2020-01-02", "2020-01-03"])
    ),
    "sparse": pd.arrays.SparseArray([0, 1]),
    "arrow struct": pd.Series(
        pa.array([{"u": 1, "v": "x"}, None]),
        dtype=pd.ArrowDtype(pa.struct([("u", pa.int64()), ("v", pa.string())])),
    ),
}

SERIES_NAMES: dict[str, Any] = {
    "none": None,
    "str": "s",
    "int": 3,
    "float": 1.5,
    "tuple": ("a", 1),
    "timestamp": pd.Timestamp("2020-01-01"),
}

ATTRS: dict[str, dict[Any, Any]] = {
    "empty": {},
    "json": {"unit": "mV", "n": [1, 2]},
    "int key and tuple": {1: "x", "t": (1, 2)},
}


def _hash(value: Any) -> str:
    """The semantic hash of ``value`` under the default data context, as text."""
    return get_default_context().semantic_hasher.hash_object(value).to_string()


def _stored_and_read(value: Any) -> Any:
    """``value`` after the storage round trip of its logical type."""
    logical = (
        LogicalPandasSeries()
        if isinstance(value, pd.Series)
        else LogicalPandasDataFrame()
    )
    return logical.storage_to_python(logical.python_to_storage(value))


def _unstable(values: dict[str, Any]) -> tuple[list[str], int]:
    """Check the round-trip rule on every value storage accepts.

    Returns:
        The names of the values whose hash changed through storage (or changed again on a
        second round trip), and how many values storage accepted.
    """
    unstable: list[str] = []
    accepted = 0
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        for name, value in values.items():
            try:
                back = _stored_and_read(value)
                again = _stored_and_read(back)
            except (ValueError, TypeError, NotImplementedError, pa.ArrowException):
                continue  # Storage does not accept this value.
            accepted += 1
            if not (_hash(value) == _hash(back) == _hash(again)):
                unstable.append(name)
    return unstable, accepted


def _frame(index: pd.Index, columns: pd.Index, values: Any) -> pd.DataFrame:
    """A two-row, two-column frame: ``values`` and a plain ``int64`` column."""
    frame = pd.DataFrame(
        {"first": values, "second": np.array([1, 2], dtype="int64")}
    ).set_axis(index, axis=0)
    return frame.set_axis(columns, axis=1)


# Representatives for the cross products: one of each way storage treats a kind (kept,
# restored in another form, restored from a dictionary, written as text, several levels).
# Every kind is also checked once on its own.
INDEX_REPRESENTATIVES = (
    "range",
    "object ints",
    "string",
    "Int64 with NA",
    "categorical, object int categories",
    "datetime fixed offset",
    "arrow string",
    "period",
    "int name",
    "tuple name",
    "multi Int64",
    "multi mixed names",
)
COLUMN_AXIS_REPRESENTATIVES = (
    "strings",
    "string dtype",
    "ints",
    "object ints",
    "mixed int and str",
    "timestamps fixed offset",
    "arrow int8",
    "int name",
    "multi named",
)
COLUMN_VALUE_REPRESENTATIVES = (
    "int64",
    "Int64 with NA",
    "string[pyarrow]",
    "object strings",
    "object ints with None",
    "object lists",
    "object dicts",
    "categorical, object int categories",
    "datetime fixed offset",
    "arrow string",
    "arrow decimal",
    "arrow decimal all null",
    "object all null",
    "period",
    "interval",
)


def _chosen(kinds: dict[str, Any], names: tuple[str, ...]) -> dict[str, Any]:
    """The entries of ``kinds`` with the given names."""
    return {name: kinds[name] for name in names}


def _assert_stable(values: dict[str, Any], *, accepted_at_least: int) -> None:
    """Assert the round-trip rule on ``values`` and that storage accepted enough of them."""
    unstable, accepted = _unstable(values)
    assert unstable == []
    assert accepted >= accepted_at_least


class TestFrameHashSurvivesStorage:
    """Frames over index kinds, column axes and column dtypes."""

    def test_every_index_kind(self) -> None:
        """Each index kind, on a plain frame."""
        values = {
            f"index {i}": _frame(index, COLUMN_AXES["strings"], COLUMN_VALUES["int64"])
            for i, index in INDEXES.items()
        }
        _assert_stable(values, accepted_at_least=len(values) - 2)

    def test_every_column_axis(self) -> None:
        """Each column axis, on a plain frame."""
        values = {
            f"columns {c}": _frame(INDEXES["range"], columns, COLUMN_VALUES["int64"])
            for c, columns in COLUMN_AXES.items()
        }
        _assert_stable(values, accepted_at_least=len(values) - 6)

    def test_every_column_dtype(self) -> None:
        """Each column dtype, on a plain frame."""
        values = {
            f"values {v}": _frame(INDEXES["range"], COLUMN_AXES["strings"], column)
            for v, column in COLUMN_VALUES.items()
        }
        _assert_stable(values, accepted_at_least=len(values) - 8)

    def test_index_kinds_by_column_axes(self) -> None:
        """Representative index kinds with representative column axes."""
        values = {
            f"index {i} / columns {c}": _frame(index, columns, COLUMN_VALUES["int64"])
            for (i, index), (c, columns) in itertools.product(
                _chosen(INDEXES, INDEX_REPRESENTATIVES).items(),
                _chosen(COLUMN_AXES, COLUMN_AXIS_REPRESENTATIVES).items(),
            )
        }
        _assert_stable(values, accepted_at_least=len(values) * 3 // 4)

    def test_index_kinds_by_column_dtypes(self) -> None:
        """Representative index kinds with representative column dtypes."""
        values = {
            f"index {i} / values {v}": _frame(index, COLUMN_AXES["strings"], column)
            for (i, index), (v, column) in itertools.product(
                _chosen(INDEXES, INDEX_REPRESENTATIVES).items(),
                _chosen(COLUMN_VALUES, COLUMN_VALUE_REPRESENTATIVES).items(),
            )
        }
        _assert_stable(values, accepted_at_least=len(values) * 3 // 4)

    def test_column_axes_by_column_dtypes(self) -> None:
        """Representative column axes with representative column dtypes."""
        values = {
            f"columns {c} / values {v}": _frame(INDEXES["range"], columns, column)
            for (c, columns), (v, column) in itertools.product(
                _chosen(COLUMN_AXES, COLUMN_AXIS_REPRESENTATIVES).items(),
                _chosen(COLUMN_VALUES, COLUMN_VALUE_REPRESENTATIVES).items(),
            )
        }
        _assert_stable(values, accepted_at_least=len(values) * 3 // 4)

    def test_attrs(self) -> None:
        """Frames and Series with ``attrs`` storage keeps as JSON."""
        values: dict[str, Any] = {}
        for name, attrs in ATTRS.items():
            frame = _frame(INDEXES["range"], COLUMN_AXES["strings"], [1, 2])
            frame.attrs = attrs
            series = pd.Series([1, 2], name="s")
            series.attrs = attrs
            values[f"frame attrs {name}"] = frame
            values[f"series attrs {name}"] = series
        _assert_stable(values, accepted_at_least=len(values))


class TestSeriesHashSurvivesStorage:
    """Series over index kinds, names and dtypes."""

    def test_every_index_kind(self) -> None:
        """Each index kind, on a plain Series."""
        values = {
            f"index {i}": pd.Series([1, 2], index=index, name="s")
            for i, index in INDEXES.items()
        }
        _assert_stable(values, accepted_at_least=len(values) - 2)

    def test_every_dtype(self) -> None:
        """Each dtype, on a plain Series."""
        values = {
            f"values {v}": pd.Series(column, name="s")
            for v, column in COLUMN_VALUES.items()
        }
        _assert_stable(values, accepted_at_least=len(values) - 8)

    def test_index_kinds_by_names(self) -> None:
        """Representative index kinds with every kind of Series name."""
        values = {
            f"index {i} / name {n}": pd.Series([1, 2], index=index, name=series_name)
            for (i, index), (n, series_name) in itertools.product(
                _chosen(INDEXES, INDEX_REPRESENTATIVES).items(), SERIES_NAMES.items()
            )
        }
        _assert_stable(values, accepted_at_least=len(values) * 3 // 4)

    def test_index_kinds_by_dtypes(self) -> None:
        """Representative index kinds with representative dtypes."""
        values = {
            f"index {i} / values {v}": pd.Series(column, name="s").set_axis(index)
            for (i, index), (v, column) in itertools.product(
                _chosen(INDEXES, INDEX_REPRESENTATIVES).items(),
                _chosen(COLUMN_VALUES, COLUMN_VALUE_REPRESENTATIVES).items(),
            )
        }
        _assert_stable(values, accepted_at_least=len(values) * 3 // 4)

    def test_names_by_dtypes(self) -> None:
        """Every kind of Series name with representative dtypes."""
        values = {
            f"name {n} / values {v}": pd.Series(column, name=series_name)
            for (n, series_name), (v, column) in itertools.product(
                SERIES_NAMES.items(),
                _chosen(COLUMN_VALUES, COLUMN_VALUE_REPRESENTATIVES).items(),
            )
        }
        _assert_stable(values, accepted_at_least=len(values) * 3 // 4)
