"""A pandas value's hash identity follows pandas' own equality (ITL-897, ITL-887).

Developer decisions, November 11, 2026: order is identity wherever the type's own equality
depends on order, and ``pandas_dataframe_v1`` / ``pandas_series_v1`` settle in one hash move
every difference pandas' equality recognises. The oracle is ``assert_frame_equal`` /
``assert_series_equal`` with default arguments, with two exceptions: ``flags`` (storage drops it)
and ``-0.0`` vs ``0.0`` (bytes differ; an extra computation, never a wrong result).

Hashes are taken through the data context's semantic hasher, the entry point every stored key
goes through.
"""

from __future__ import annotations

import datetime as dt
import warnings
from collections.abc import Callable
from typing import Any

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest
from pandas.testing import assert_frame_equal, assert_series_equal

from orcapod.contexts import get_default_context
from orcapod.logical_types import LogicalPandasDataFrame, LogicalPandasSeries


def _content_hash(value: Any) -> Any:
    """The semantic hash of ``value`` under the default data context."""
    return get_default_context().semantic_hasher.hash_object(value)


def _hash(value: Any) -> str:
    """The semantic hash of ``value`` as text (label and hex digest)."""
    return _content_hash(value).to_string()


def _frame() -> pd.DataFrame:
    """Columns ``a`` (ints) and ``b`` (strings), two rows, default index."""
    return pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})


def _multi_index() -> pd.MultiIndex:
    """A two-level index named ``i``, ``j``."""
    return pd.MultiIndex.from_tuples([(0, "p"), (1, "q")], names=["i", "j"])


def _with_columns_name(df: pd.DataFrame, name: str) -> pd.DataFrame:
    """A copy of ``df`` whose column axis is named ``name``."""
    out = df.copy()
    out.columns = out.columns.rename(name)
    return out


def _with_attrs(value: Any, attrs: dict[Any, Any] | None = None) -> Any:
    """A copy of ``value`` with ``attrs`` (default ``{'unit': 'mV'}``)."""
    out = value.copy()
    out.attrs = {"unit": "mV"} if attrs is None else attrs
    return out


def _daily(with_freq: bool) -> pd.DataFrame:
    """Three daily rows; the index carries ``freq`` or not."""
    days = ["2020-01-01", "2020-01-02", "2020-01-03"]
    index = pd.DatetimeIndex(days, freq="D") if with_freq else pd.DatetimeIndex(days)
    return pd.DataFrame({"a": [1, 2, 3]}, index=index)


def _categorical(categories: list[str], ordered: bool = False) -> pd.DataFrame:
    """One categorical column with the given categories and ordered flag."""
    values = pd.Categorical(["x", "y"], categories=categories, ordered=ordered)
    return pd.DataFrame({"a": values})


def _timestamp_labels(tz: str | None) -> pd.DataFrame:
    """A frame whose column labels are timestamps, naive or in ``tz``."""
    labels = pd.DatetimeIndex(pd.to_datetime(["2020-01-01", "2020-01-02"]))
    return pd.DataFrame(
        [[1, 2]], columns=labels if tz is None else labels.tz_localize(tz)
    )


# Pairs that assert_frame_equal / assert_series_equal (defaults) tell apart: must hash apart.
DISTINCT: dict[str, tuple[Any, Any]] = {
    # orders
    "column order": (_frame(), _frame()[["b", "a"]]),
    "MultiIndex level order": (
        _frame().set_index(_multi_index()),
        _frame().set_index(_multi_index()).swaplevel(),
    ),
    # labels and names
    "column label type 0 vs '0'": (
        pd.DataFrame({0: [1, 2]}),
        pd.DataFrame({"0": [1, 2]}),
    ),
    "MultiIndex columns vs flat labels": (
        pd.DataFrame(
            [[1, 2]], columns=pd.MultiIndex.from_tuples([("a", "x"), ("a", "y")])
        ),
        pd.DataFrame([[1, 2]], columns=pd.Index(["('a', 'x')", "('a', 'y')"])),
    ),
    "columns.name": (_frame(), _with_columns_name(_frame(), "features")),
    "column axis dtype int64 vs object": (
        pd.DataFrame([[1, 2]], columns=pd.Index([1, 2], dtype="int64")),
        pd.DataFrame([[1, 2]], columns=pd.Index([1, 2], dtype=object)),
    ),
    "column axis dtype int64 vs int32": (
        pd.DataFrame([[1, 2]], columns=pd.Index([1, 2], dtype="int64")),
        pd.DataFrame([[1, 2]], columns=pd.Index([1, 2], dtype="int32")),
    ),
    "column axis dtype object vs string": (
        pd.DataFrame([[1, 2]], columns=pd.Index(["a", "b"], dtype=object)),
        pd.DataFrame([[1, 2]], columns=pd.Index(["a", "b"], dtype="string")),
    ),
    "column axis Arrow dtype int8 vs int64": (
        pd.DataFrame([[1, 2]], columns=pd.Index([1, 2], dtype="int8[pyarrow]")),
        pd.DataFrame([[1, 2]], columns=pd.Index([1, 2], dtype="int64[pyarrow]")),
    ),
    "column axis level dtype int64 vs int32": (
        pd.DataFrame(
            [[1, 2]],
            columns=pd.MultiIndex.from_arrays(
                [pd.Index([1, 2], dtype="int64"), pd.Index(["x", "y"], dtype=object)]
            ),
        ),
        pd.DataFrame(
            [[1, 2]],
            columns=pd.MultiIndex.from_arrays(
                [pd.Index([1, 2], dtype="int32"), pd.Index(["x", "y"], dtype=object)]
            ),
        ),
    ),
    "index name": (_frame(), _frame().rename_axis("id")),
    "MultiIndex level names": (
        _frame().set_index(pd.MultiIndex.from_tuples([(0, "p"), (1, "q")])),
        _frame().set_index(_multi_index()),
    ),
    "index dtype int64 vs float64": (
        pd.DataFrame({"a": [1, 2]}, index=pd.Index([0, 1], dtype="int64")),
        pd.DataFrame({"a": [1, 2]}, index=pd.Index([0.0, 1.0], dtype="float64")),
    ),
    # dtypes (ITL-887 among them)
    "categorical vs object string": (
        pd.DataFrame({"a": ["x", "y"]}),
        _categorical(["x", "y"]),
    ),
    "categorical category order": (_categorical(["x", "y"]), _categorical(["y", "x"])),
    "categorical unused category": (
        _categorical(["x", "y"]),
        _categorical(["x", "y", "z"]),
    ),
    "categorical ordered flag": (
        _categorical(["x", "y"]),
        _categorical(["x", "y"], ordered=True),
    ),
    "Int64 vs int64": (
        pd.DataFrame({"a": pd.array([1, 2], dtype="int64")}),
        pd.DataFrame({"a": pd.array([1, 2], dtype="Int64")}),
    ),
    "boolean vs bool": (
        pd.DataFrame({"a": [True, False]}),
        pd.DataFrame({"a": pd.array([True, False], dtype="boolean")}),
    ),
    "string vs object": (
        pd.DataFrame({"a": ["x", "y"]}),
        pd.DataFrame({"a": pd.array(["x", "y"], dtype="string")}),
    ),
    "int64 vs int32": (
        pd.DataFrame({"a": np.array([1, 2], dtype="int64")}),
        pd.DataFrame({"a": np.array([1, 2], dtype="int32")}),
    ),
    "row order": (_frame(), _frame().iloc[::-1]),
    "index values": (_frame(), _frame().set_axis([10, 11], axis="index")),
    "values": (_frame(), pd.DataFrame({"a": [1, 3], "b": ["x", "y"]})),
    # labels outside str/int that work on v0.2.0
    "Timestamp labels naive vs UTC": (
        _timestamp_labels(None),
        _timestamp_labels("UTC"),
    ),
    "Timestamp vs str labels": (
        _timestamp_labels(None),
        pd.DataFrame(
            [[1, 2]], columns=pd.Index(["2020-01-01 00:00:00", "2020-01-02 00:00:00"])
        ),
    ),
    "period column freq": (
        pd.DataFrame({"p": pd.period_range("2020-01", periods=2, freq="M")}),
        pd.DataFrame({"p": pd.period_range("2020Q1", periods=2, freq="Q")}),
    ),
    "interval column closed": (
        pd.DataFrame({"i": pd.interval_range(0, 2, closed="right")}),
        pd.DataFrame({"i": pd.interval_range(0, 2, closed="left")}),
    ),
    # Series
    "Series MultiIndex level order": (
        pd.Series([1, 2], index=_multi_index(), name="v"),
        pd.Series([1, 2], index=_multi_index(), name="v").swaplevel(),
    ),
    "Series name": (pd.Series([1, 2], name="a"), pd.Series([1, 2], name="b")),
    "Series name None vs 'x'": (pd.Series([1, 2]), pd.Series([1, 2], name="x")),
    "Series name 0 vs '0'": (pd.Series([1, 2], name=0), pd.Series([1, 2], name="0")),
    "Series dtype int64 vs Int64": (
        pd.Series([1, 2], dtype="int64"),
        pd.Series([1, 2], dtype="Int64"),
    ),
    "Series categorical vs object": (
        pd.Series(["x", "y"]),
        pd.Series(pd.Categorical(["x", "y"])),
    ),
}

# Pairs that assert_frame_equal / assert_series_equal (defaults) call equal: must hash equal.
EQUAL: dict[str, tuple[Any, Any]] = {
    "copy": (_frame(), _frame().copy()),
    "reordered back": (_frame(), _frame()[["b", "a"]][["a", "b"]]),
    "RangeIndex vs Index[int64]": (
        _frame(),
        pd.DataFrame(
            {"a": [1, 2], "b": ["x", "y"]}, index=pd.Index([0, 1], dtype="int64")
        ),
    ),
    "attrs key order": (
        _with_attrs(_frame(), {"a": 1, "b": 2}),
        _with_attrs(_frame(), {"b": 2, "a": 1}),
    ),
    "unused MultiIndex levels": (
        _frame().set_index(_multi_index()),
        pd.concat([_frame(), _frame()])
        .set_index(
            pd.MultiIndex.from_tuples(
                [(0, "p"), (1, "q"), (2, "r"), (3, "s")], names=["i", "j"]
            )
        )
        .iloc[:2],
    ),
    "dict key order in an object column": (
        pd.DataFrame({"s": [{"u": 1, "v": "x"}]}),
        pd.DataFrame({"s": [{"v": "x", "u": 1}]}),
    ),
    "chunked copy": (_frame(), pd.concat([_frame().iloc[:1], _frame().iloc[1:]])),
}


# Where the hash deliberately departs from pandas' assert functions: (pair, hashes equal).
EXCEPTIONS: dict[str, tuple[tuple[Any, Any], bool]] = {
    # Storage does not restore these, so they never count (pandas tells the pair apart).
    "flags": ((_frame(), _frame().set_flags(allows_duplicate_labels=False)), True),
    "index freq": ((_daily(True), _daily(False)), True),
    "string storage": (
        (
            pd.DataFrame({"a": pd.array(["x", None], dtype="string[python]")}),
            pd.DataFrame({"a": pd.array(["x", None], dtype="string[pyarrow]")}),
        ),
        True,
    ),
    "object ints vs int64": (
        (
            pd.DataFrame({"a": [1, 2]}),
            pd.DataFrame({"a": pd.Series([1, 2], dtype=object)}),
        ),
        True,
    ),
    "object ints with None vs float64": (
        (
            pd.DataFrame({"a": [1.0, np.nan]}),
            pd.DataFrame({"a": pd.Series([1, None], dtype=object)}),
        ),
        True,
    ),
    "object int index vs int64 index": (
        (
            pd.DataFrame({"a": [1, 2]}, index=pd.Index([5, 6], dtype="int64")),
            pd.DataFrame({"a": [1, 2]}, index=pd.Index([5, 6], dtype=object)),
        ),
        True,
    ),
    # Storage writes an index name as text, so 1 and "1" are one name once read back.
    "index name 1 vs '1'": (
        (_frame().rename_axis(1), _frame().rename_axis("1")),
        True,
    ),
    "Series index name 1 vs '1'": (
        (pd.Series([1, 2]).rename_axis(1), pd.Series([1, 2]).rename_axis("1")),
        True,
    ),
    # Storage writes column labels of mixed type as text.
    "mixed labels 0 vs '0'": (
        (pd.DataFrame({0: [1], "b": [2]}), pd.DataFrame({"0": [1], "b": [2]})),
        True,
    ),
    # Storage restores an index level as NumPy.
    "Int64 index vs int64 index": (
        (
            pd.DataFrame({"a": [1, 2]}, index=pd.Index([5, 6], dtype="Int64")),
            pd.DataFrame({"a": [1, 2]}, index=pd.Index([5, 6], dtype="int64")),
        ),
        True,
    ),
    "string index vs object index": (
        (
            pd.DataFrame({"a": [1, 2]}, index=pd.Index(["p", "q"], dtype="string")),
            pd.DataFrame({"a": [1, 2]}, index=pd.Index(["p", "q"], dtype=object)),
        ),
        True,
    ),
    # The hash is strict where pandas calls the pair equal.
    "-0.0 vs 0.0": ((pd.DataFrame({"a": [0.0]}), pd.DataFrame({"a": [-0.0]})), False),
    "attrs": ((_frame(), _with_attrs(_frame())), False),
    "Series attrs": ((pd.Series([1, 2]), _with_attrs(pd.Series([1, 2]))), False),
}

# The identity property (``PANDAS_IDENTITY_PROPERTIES``) each pair above exercises.
PROPERTY_OF_PAIR: dict[str, str] = {
    "column order": "column_order",
    "MultiIndex level order": "index_level_order",
    "column label type 0 vs '0'": "label_type",
    "MultiIndex columns vs flat labels": "label_type",
    "columns.name": "columns_names",
    "column axis dtype int64 vs object": "columns_dtype",
    "column axis dtype int64 vs int32": "columns_dtype",
    "column axis dtype object vs string": "columns_dtype",
    "column axis level dtype int64 vs int32": "columns_dtype",
    "column axis Arrow dtype int8 vs int64": "columns_dtype",
    "index name 1 vs '1'": "index_name_type",
    "mixed labels 0 vs '0'": "mixed_label_type",
    "Int64 index vs int64 index": "index_extension_dtype",
    "string index vs object index": "index_extension_dtype",
    "Series index name 1 vs '1'": "index_name_type",
    "index name": "index_names",
    "MultiIndex level names": "index_names",
    "index dtype int64 vs float64": "index_dtype",
    "categorical vs object string": "categorical_dtype",
    "categorical category order": "categorical_categories",
    "categorical unused category": "categorical_categories",
    "categorical ordered flag": "categorical_ordered",
    "Int64 vs int64": "pandas_dtype",
    "boolean vs bool": "pandas_dtype",
    "string vs object": "pandas_dtype",
    "int64 vs int32": "arrow_type",
    "row order": "values",
    "values": "values",
    "Timestamp labels naive vs UTC": "label_type",
    "Timestamp vs str labels": "label_type",
    "period column freq": "pandas_dtype",
    "interval column closed": "pandas_dtype",
    "Series MultiIndex level order": "index_level_order",
    "Series name": "series_name",
    "Series name None vs 'x'": "series_name",
    "Series name 0 vs '0'": "series_name",
    "Series dtype int64 vs Int64": "pandas_dtype",
    "Series categorical vs object": "categorical_dtype",
    "index values": "index_values",
    "frame vs its one-column Series": "value_kind",
    "RangeIndex vs Index[int64]": "index_kind",
    "attrs": "attrs",
    "Series attrs": "attrs",
    "index freq": "index_freq",
    "unused MultiIndex levels": "index_unused_levels",
    "string storage": "string_storage",
    "object ints vs int64": "object_dtype",
    "object ints with None vs float64": "object_dtype",
    "object int index vs int64 index": "object_dtype",
    "dict key order in an object column": "dict_key_order",
    "flags": "flags",
    "-0.0 vs 0.0": "signed_zero",
}


def _assert_fn(value: Any) -> Callable[[Any, Any], None]:
    """pandas' assert function for the kind of ``value``."""
    return assert_series_equal if isinstance(value, pd.Series) else assert_frame_equal


class TestFollowsPandasEquality:
    """Values pandas tells apart hash apart; values it calls equal hash equal."""

    @pytest.mark.parametrize("name", list(DISTINCT))
    def test_pandas_distinguishes_the_pair(self, name: str) -> None:
        """Precondition: pandas' assert function says the two values differ."""
        first, second = DISTINCT[name]
        with pytest.raises(AssertionError):
            _assert_fn(first)(first, second)

    @pytest.mark.parametrize("name", list(DISTINCT))
    def test_distinct_values_hash_differently(self, name: str) -> None:
        """The two values of a ``DISTINCT`` pair hash differently."""
        first, second = DISTINCT[name]
        assert _hash(first) != _hash(second)

    @pytest.mark.parametrize("name", list(EQUAL))
    def test_pandas_calls_the_pair_equal(self, name: str) -> None:
        """Precondition: pandas' assert function accepts the pair."""
        first, second = EQUAL[name]
        _assert_fn(first)(first, second)

    @pytest.mark.parametrize("name", list(EQUAL))
    def test_equal_values_hash_equal(self, name: str) -> None:
        """The two values of an ``EQUAL`` pair hash equal."""
        first, second = EQUAL[name]
        assert _hash(first) == _hash(second)


class TestIdentityProperties:
    """``PANDAS_IDENTITY_PROPERTIES`` is the decision; every entry is checked on a pair."""

    def _pairs(self) -> dict[str, tuple[Any, Any]]:
        """Every pair of this module, by name."""
        pairs = {**DISTINCT, **EQUAL}
        pairs.update({name: pair for name, (pair, _) in EXCEPTIONS.items()})
        pairs["frame vs its one-column Series"] = (
            pd.DataFrame({"a": [1, 2]}),
            pd.Series([1, 2], name="a"),
        )
        return pairs

    def test_every_property_is_exercised_and_agrees(self) -> None:
        """Each declared property has a pair, and the pair's hashes agree with it."""
        from orcapod.hashing.pandas_value_hash import PANDAS_IDENTITY_PROPERTIES

        declared = {p.name: p.identity for p in PANDAS_IDENTITY_PROPERTIES}
        assert len(declared) == len(PANDAS_IDENTITY_PROPERTIES)
        assert set(PROPERTY_OF_PAIR.values()) == set(declared)
        for name, (first, second) in self._pairs().items():
            if (
                name not in PROPERTY_OF_PAIR
            ):  # "copy", "chunked copy", "attrs key order"
                continue
            differs = _hash(first) != _hash(second)
            assert differs == declared[PROPERTY_OF_PAIR[name]], name

    @pytest.mark.parametrize("name", list(EXCEPTIONS))
    def test_exceptions_to_pandas_equality(self, name: str) -> None:
        """What storage does not restore hashes equal though pandas differs; ``-0.0`` and
        ``attrs`` hash apart though pandas calls the values equal.
        """
        (first, second), hashes_equal = EXCEPTIONS[name]
        assert (_hash(first) == _hash(second)) == hashes_equal
        if hashes_equal:
            with pytest.raises(AssertionError):
                _assert_fn(first)(first, second)
        else:
            _assert_fn(first)(first, second)

    def test_only_identity_properties_are_options(self) -> None:
        """An ignore-list option is always an identity property."""
        from orcapod.hashing.pandas_value_hash import (
            PANDAS_DATAFRAME_OPTIONS,
            PANDAS_IDENTITY_PROPERTIES,
            PANDAS_SERIES_OPTIONS,
        )

        options = {p.name for p in PANDAS_IDENTITY_PROPERTIES if p.option}
        assert options == {"column_order", "series_name", "attrs"}
        assert options == PANDAS_DATAFRAME_OPTIONS | PANDAS_SERIES_OPTIONS
        assert all(p.identity for p in PANDAS_IDENTITY_PROPERTIES if p.option)

    def test_carrier_is_stated_exactly_for_identity_properties(self) -> None:
        """An identity property names what carries it; any other names nothing."""
        from orcapod.hashing.pandas_value_hash import PANDAS_IDENTITY_PROPERTIES

        for prop in PANDAS_IDENTITY_PROPERTIES:
            assert bool(prop.carried_by) == prop.identity, prop.name


class TestLabels:
    """The hash labels of the two pandas types."""

    def test_dataframe_label(self) -> None:
        """A frame's hash has the DataFrame label."""
        assert _content_hash(_frame()).method == "pandas_dataframe_v1"

    def test_series_label(self) -> None:
        """A Series' hash has the Series label."""
        assert _content_hash(pd.Series([1, 2])).method == "pandas_series_v1"

    def test_frame_and_its_one_column_series_differ(self) -> None:
        """A frame and its one-column Series are different values."""
        assert _hash(pd.DataFrame({"a": [1, 2]})) != _hash(pd.Series([1, 2], name="a"))


class TestOutsideTheGrammar:
    """Labels and dtypes the descriptor grammar does not cover."""

    def test_unsupported_label_type_raises(self) -> None:
        """A label or name of an arbitrary class raises, naming the class."""

        class Label:
            """A hashable class the grammar does not know."""

            def __hash__(self) -> int:
                """Any constant: the class only needs to be hashable."""
                return 1

        with pytest.raises(ValueError, match="Label"):
            _hash(pd.DataFrame({Label(): [1]}))
        # Also where storage would write the label or name as text.
        with pytest.raises(ValueError, match="Label"):
            _hash(pd.DataFrame({Label(): [1], "b": [2]}))
        with pytest.raises(ValueError, match="Label"):
            _hash(pd.DataFrame({"a": [1]}, index=pd.Index([0], name=Label())))
        with pytest.raises(ValueError, match="Label"):
            _hash(pd.Series([1], name="s", index=pd.Index([0], name=Label())))

    def test_unsupported_dtype_raises_naming_the_column(self) -> None:
        """A dtype outside the grammar raises, naming its column."""
        frame = pd.DataFrame({"s": pd.arrays.SparseArray([0, 1])})
        with pytest.raises(ValueError, match="'s'"):
            _hash(frame)

    @pytest.mark.parametrize(
        "value",
        [_with_attrs(_frame(), {"unit": "mV", "o": {1, 2}}), pd.Series([1, 2])],
        ids=["frame", "series"],
    )
    def test_attrs_that_are_not_json_are_refused(self, value: Any) -> None:
        """``attrs`` storage cannot write as JSON raise, naming the key."""
        value = _with_attrs(value, {"unit": "mV", "o": {1, 2}})
        with pytest.raises(ValueError, match=r"attrs\['o'\]"):
            _hash(value)
        logical = (
            LogicalPandasSeries()
            if isinstance(value, pd.Series)
            else LogicalPandasDataFrame()
        )
        with warnings.catch_warnings():
            # Refused by Orcapod, before pyarrow sees the value: pyarrow 23 would warn
            # and write {}, pyarrow 22 would raise TypeError.
            warnings.simplefilter("error")
            with pytest.raises(ValueError, match=r"attrs\['o'\]"):
                logical.python_to_storage(value)

    def test_attrs_key_that_is_not_a_json_key_is_refused(self) -> None:
        """A key JSON cannot write (a tuple) raises, naming the key."""
        with pytest.raises(ValueError, match=r"attrs\[\(1, 2\)\]"):
            _hash(_with_attrs(_frame(), {(1, 2): "x"}))

    def test_attrs_with_a_key_that_is_not_text_are_accepted(self) -> None:
        """``{1: "x"}`` is written as ``{"1": "x"}`` and hashed in that form."""
        assert _hash(_with_attrs(_frame(), {1: "x"})) == _hash(
            _with_attrs(_frame(), {"1": "x"})
        )

    def test_attrs_that_are_not_json_do_not_matter_when_attrs_are_ignored(self) -> None:
        """With ``attrs`` in the ignore list the hash does not read them."""
        from orcapod.hashing.pandas_value_hash import pandas_frame_descriptor

        frame = _with_attrs(_frame(), {"o": {1, 2}})
        assert pandas_frame_descriptor(frame, frozenset({"attrs"}))["attrs"] == {}
        # Also with column order ignored, which probes the schema on its own.
        both = frozenset({"attrs", "column_order"})
        assert pandas_frame_descriptor(frame, both)["attrs"] == {}

    def test_ignored_attrs_are_never_copied(self) -> None:
        """With ``attrs`` ignored, ``attrs`` that cannot even be copied do not matter.

        pandas deep-copies ``attrs`` in almost every operation, and a lock cannot be
        copied; the hash must not touch them, for a frame or a Series, also with column
        order ignored.
        """
        import threading

        from orcapod.hashing.pandas_value_hash import (
            pandas_frame_descriptor,
            pandas_series_descriptor,
        )

        frame = _frame()
        frame.attrs = {"lock": threading.Lock()}
        for ignore in ({"attrs"}, {"attrs", "column_order"}):
            assert pandas_frame_descriptor(frame, frozenset(ignore))["attrs"] == {}
        series = pd.Series([1, 2], name="s")
        series.attrs = {"lock": threading.Lock()}
        assert pandas_series_descriptor(series, frozenset({"attrs"}))["attrs"] == {}
        # Not ignored: refused like any other attrs that are not JSON.
        with pytest.raises(ValueError, match=r"attrs\['lock'\]"):
            _hash(frame)
        with pytest.raises(ValueError, match=r"attrs\['lock'\]"):
            _hash(series)
        assert list(frame.attrs) == ["lock"]

    def test_date_label_is_in_the_grammar(self) -> None:
        """A ``datetime.date`` label is in the grammar."""
        from orcapod.hashing.pandas_value_hash import pandas_label_descriptor

        assert pandas_label_descriptor(dt.date(2020, 1, 1)) == {"date": "2020-01-01"}

    def test_date_column_axis_is_refused(self) -> None:
        """A column axis of dates cannot be read back from storage, so it is refused (ITL-1054)."""
        with pytest.raises(ValueError, match="column axis.*cannot be read back"):
            _hash(pd.DataFrame([[1]], columns=pd.Index([dt.date(2020, 1, 1)])))


STORABLE_FRAMES: dict[str, Any] = {
    "ab": _frame(),
    "ba": _frame()[["b", "a"]],
    "swapped levels": _frame().set_index(_multi_index()).swaplevel(),
    "categorical": _categorical(["y", "x", "z"], ordered=True),
    "nullable": pd.DataFrame({"a": pd.array([1, None], dtype="Int64")}),
    "columns.name": _with_columns_name(_frame(), "features"),
    "int labels": pd.DataFrame({0: [1], 1: ["x"]}),
    "Timestamp labels": _timestamp_labels("UTC"),
    "period": pd.DataFrame({"p": pd.period_range("2020-01", periods=2, freq="M")}),
    "interval": pd.DataFrame({"i": pd.interval_range(0, 2)}),
}


class TestStorage:
    """A storable value hashes the same after the storage round trip."""

    @pytest.mark.parametrize("name", list(STORABLE_FRAMES))
    def test_frame_hash_survives_storage(self, name: str) -> None:
        """A frame read back from storage keeps its cache key."""
        value = STORABLE_FRAMES[name]
        logical = LogicalPandasDataFrame()
        back = logical.storage_to_python(logical.python_to_storage(value))
        assert_frame_equal(back, value)
        assert _hash(back) == _hash(value)

    @pytest.mark.parametrize(
        "value",
        [
            pd.Series([1, 2], index=_multi_index(), name="v").swaplevel(),
            pd.Series([1, 2]),
            pd.Series(pd.Categorical(["x", "y"]), name=0),
        ],
        ids=["swapped levels", "unnamed", "categorical int name"],
    )
    def test_series_hash_survives_storage(self, value: pd.Series) -> None:
        """A storable Series hashes the same after storage."""
        logical = LogicalPandasSeries()
        back = logical.storage_to_python(logical.python_to_storage(value))
        assert_series_equal(back, value)
        assert _hash(back) == _hash(value)


def _object(values: list[Any]) -> pd.Series:
    """``values`` as an ``object`` Series."""
    return pd.Series(values, dtype=object)


# Values storage reads back in another form: the hash is the hash of that form.
CHANGED_BY_STORAGE: dict[str, Any] = {
    "string[pyarrow]": pd.DataFrame(
        {"a": pd.array(["x", None], dtype="string[pyarrow]")}
    ),
    "object ints": pd.DataFrame({"a": _object([1, 2])}),
    "object ints with None": pd.DataFrame({"a": _object([1, None])}),
    "object floats": pd.DataFrame({"a": _object([1.5, None])}),
    "object bools": pd.DataFrame({"a": _object([True, False])}),
    "object datetimes": pd.DataFrame({"a": _object([dt.datetime(2020, 1, 1)])}),
    "object timedeltas": pd.DataFrame({"a": _object([dt.timedelta(days=1)])}),
    "object int index": pd.DataFrame(
        {"a": [1, 2]}, index=pd.Index([5, 6], dtype=object)
    ),
    "index name int": _frame().rename_axis(1),
    "index name tuple": _frame().rename_axis([("a", 1)]),
    "MultiIndex level name float": _frame().set_index(
        pd.MultiIndex.from_tuples([(0, "p"), (1, "q")], names=[1.5, "j"])
    ),
    "RangeIndex with an int name": pd.DataFrame(
        {"a": [1, 2]}, index=pd.RangeIndex(2, name=7)
    ),
    "list of ints with None": pd.DataFrame({"a": _object([[1, None], [2]])}),
    "list of lists of ints with None": pd.DataFrame({"a": _object([[[1, None]]])}),
    "dict holding a list of ints with None": pd.DataFrame(
        {"a": _object([{"x": [1, None]}])}
    ),
    "RangeIndex with a step": pd.DataFrame({"a": [1, 2]}, index=pd.RangeIndex(5, 9, 2)),
    "index freq": _daily(True),
    "flags": _frame().set_flags(allows_duplicate_labels=False),
    "unused MultiIndex levels": EQUAL["unused MultiIndex levels"][1],
    "attrs read back as JSON": _with_attrs(_frame(), {1: "x", "t": (1, 2)}),
    "Series string[pyarrow]": pd.Series(
        pd.array(["x"], dtype="string[pyarrow]"), name="s"
    ),
    "Series object ints with None": pd.Series([1, None], dtype=object, name="s"),
    "Series attrs read back as JSON": _with_attrs(pd.Series([1, 2]), {1: "x"}),
    "Series index name int": pd.Series([1, 2], name="s").rename_axis(1),
    "unnamed Series, index name int": pd.Series([1, 2]).rename_axis(0),
    "Series list of ints with None": pd.Series([[1, None]], dtype=object, name="s"),
}

# ``object`` columns that read back as ``object``: hashed as they are.
OBJECT_KEPT_BY_STORAGE: dict[str, pd.DataFrame] = {
    "strings with None": pd.DataFrame({"a": _object(["x", None])}),
    "bools with None": pd.DataFrame({"a": _object([True, None])}),
    "bytes": pd.DataFrame({"a": _object([b"x", b"y"])}),
    "dates": pd.DataFrame({"a": _object([dt.date(2020, 1, 1)])}),
    "dicts": pd.DataFrame({"a": _object([{"v": 1, "u": 2}])}),
    "lists": pd.DataFrame({"a": _object([[1, 2], [3]])}),
}


def _stored_and_read(value: Any) -> Any:
    """``value`` after the storage round trip of its logical type (public methods)."""
    logical = (
        LogicalPandasSeries()
        if isinstance(value, pd.Series)
        else LogicalPandasDataFrame()
    )
    with warnings.catch_warnings():
        # pyarrow warns that attrs it cannot write as JSON are dropped.
        warnings.simplefilter("ignore", UserWarning)
        return logical.storage_to_python(logical.python_to_storage(value))


class TestHashOfWhatStorageRestores:
    """``hash(read(write(v))) == hash(v)``, also where storage changes the value."""

    @pytest.mark.parametrize("name", list(CHANGED_BY_STORAGE))
    def test_value_changed_by_storage_keeps_its_hash(self, name: str) -> None:
        """A value storage changes hashes like the value read back."""
        value = CHANGED_BY_STORAGE[name]
        back = _stored_and_read(value)
        # Precondition: the round trip does change something pandas can see.
        with pytest.raises(AssertionError):
            _assert_fn(value)(back, value)
            assert back.attrs == value.attrs
            assert back.flags == value.flags
            assert type(back.index) is type(value.index)
            if isinstance(value.index, pd.MultiIndex):
                assert list(back.index.levels[0]) == list(value.index.levels[0])
        assert _hash(back) == _hash(value)
        # And the form storage restores is stable under a second round trip.
        assert _hash(_stored_and_read(back)) == _hash(back)

    @pytest.mark.parametrize("name", list(OBJECT_KEPT_BY_STORAGE))
    def test_object_column_kept_by_storage_keeps_its_hash(self, name: str) -> None:
        """An ``object`` column that reads back ``object`` keeps its hash."""
        value = OBJECT_KEPT_BY_STORAGE[name]
        back = _stored_and_read(value)
        assert back["a"].dtype == object
        assert _hash(back) == _hash(value)

    @pytest.mark.parametrize(
        "columns",
        [
            pd.Index([1, 2], dtype="int64"),
            pd.Index([1, 2], dtype="int32"),
            pd.Index([1, 2], dtype=object),
            pd.Index([1.5, 2.5], dtype="float32"),
            pd.Index(["a", "b"], dtype=object),
            pd.Index(["a", "b"], dtype="string"),
            pd.DatetimeIndex(["2020-01-01", "2020-01-02"]).as_unit("s"),
            pd.DatetimeIndex(["2020-01-01", "2020-01-02"], tz="UTC"),
            pd.MultiIndex.from_arrays([pd.Index([1, 2], dtype="int32"), ["x", "y"]]),
            pd.Index([], dtype=object),
            pd.Index([], dtype="int64"),
        ],
        ids=lambda columns: f"{type(columns).__name__}[{columns.dtype}]x{len(columns)}",
    )
    def test_column_axis_dtype_survives_storage(self, columns: pd.Index) -> None:
        """The column-axis dtype in the hash is the one storage restores."""
        value = pd.DataFrame([[1] * len(columns)], columns=columns)
        back = _stored_and_read(value)
        assert _hash(back) == _hash(value)
        assert _hash(_stored_and_read(back)) == _hash(back)

    def test_object_strings_still_differ_from_string_dtype(self) -> None:
        """Both read back as they are, so the dtype stays part of the hash."""
        assert _hash(pd.DataFrame({"a": ["x"]})) != _hash(
            pd.DataFrame({"a": pd.array(["x"], dtype="string")})
        )


class TestDescriptorGrammar:
    """Pins of the descriptor grammar.

    Uses the non-public descriptor functions because the test is about the grammar itself: a
    change here is a new hash definition and needs a new label.
    """

    def test_frame_descriptor(self) -> None:
        """Pin of a frame's descriptor."""
        from orcapod.hashing.pandas_value_hash import pandas_frame_descriptor

        frame = pd.DataFrame(
            {
                0: pd.array([1, None], dtype="Int64"),
                # The category dtype is stated: pandas 3 infers a string dtype here.
                "c": pd.Categorical(
                    ["x", "y"], categories=pd.Index(["x", "y"], dtype=object)
                ),
            },
            index=pd.Index([5, 6], name="id"),
        )
        frame.columns = frame.columns.rename("features")
        assert pandas_frame_descriptor(frame) == {
            "kind": "orcapod.pandas.dataframe",
            "height": 2,
            "attrs": {},
            "columns": [
                # Labels of mixed type: storage writes them as text.
                ["0", {"masked": "Int64"}],
                [
                    "c",
                    {
                        "categorical": {
                            "categories": [{"numpy": "object"}, ["x", "y"]],
                            "ordered": False,
                        }
                    },
                ],
            ],
            "columns_names": ["features"],
            "columns_dtypes": [{"numpy": "object"}],
            "index": [["id", {"numpy": "int64"}]],
        }

    def test_frame_descriptor_of_a_column_multiindex(self) -> None:
        """The column axis has one dtype term per level."""
        from orcapod.hashing.pandas_value_hash import pandas_frame_descriptor

        columns = pd.MultiIndex.from_arrays(
            [pd.Index([1, 2], dtype="int32"), pd.Index(["x", "y"], dtype=object)]
        )
        descriptor = pandas_frame_descriptor(pd.DataFrame([[1, 2]], columns=columns))
        assert descriptor["columns_dtypes"] == [{"numpy": "int32"}, {"numpy": "object"}]

    def test_frame_descriptor_of_an_arrow_column_axis(self) -> None:
        """A ``pd.ArrowDtype`` column axis is described with its Arrow type."""
        from orcapod.hashing.pandas_value_hash import pandas_frame_descriptor

        columns = pd.Index([1, 2], dtype="int8[pyarrow]")
        descriptor = pandas_frame_descriptor(pd.DataFrame([[1, 2]], columns=columns))
        assert descriptor["columns_dtypes"] == [{"arrow": "int8"}]

    def test_index_names_are_described_as_storage_restores_them(self) -> None:
        """An index name is its text; a Series name stays typed."""
        from orcapod.hashing.pandas_value_hash import (
            pandas_frame_descriptor,
            pandas_series_descriptor,
        )

        frame = _frame().rename_axis(1)
        assert pandas_frame_descriptor(frame)["index"] == [["1", {"numpy": "int64"}]]
        series = pd.Series([1, 2], name=2).rename_axis(1)
        descriptor = pandas_series_descriptor(series)
        assert descriptor["index"] == [["1", {"numpy": "int64"}]]
        # A Series name is restored typed, so it stays typed.
        assert descriptor["name"] == {"int": 2}

    def test_series_descriptor(self) -> None:
        """Pin of a Series' descriptor."""
        from orcapod.hashing.pandas_value_hash import pandas_series_descriptor

        series = pd.Series(
            pd.array(["x", None], dtype="string"),
            index=pd.MultiIndex.from_tuples([(0, 1.5), (1, 2.5)], names=["i", None]),
            name=("a", 1),
        )
        assert pandas_series_descriptor(series) == {
            "kind": "orcapod.pandas.series",
            "height": 2,
            "attrs": {},
            "name": {"tuple": ["a", {"int": 1}]},
            "dtype": "string",
            "index": [
                ["i", {"numpy": "int64"}],
                [None, {"numpy": "float64"}],
            ],
        }

    def test_label_grammar(self) -> None:
        """Pin of the label terms."""
        from orcapod.hashing.pandas_value_hash import pandas_label_descriptor

        assert pandas_label_descriptor("a") == "a"
        assert pandas_label_descriptor(None) is None
        assert pandas_label_descriptor(True) == {"bool": True}
        assert pandas_label_descriptor(np.int8(3)) == {"int": 3}
        assert pandas_label_descriptor(0.5) == {"float": "0x1.0000000000000p-1"}
        # A timestamp or duration is the integer count in its own unit, with the unit.
        second = pd.Timestamp("2020-01-01", tz="UTC").as_unit("s")
        assert pandas_label_descriptor(second) == {
            "timestamp": [1577836800, "s", "UTC"]
        }
        assert pandas_label_descriptor(second.as_unit("ns")) == {
            "timestamp": [1577836800000000000, "ns", "UTC"]
        }
        # Outside the nanosecond range: still a label.
        assert pandas_label_descriptor(pd.Timestamp("2500-01-01").as_unit("s")) == {
            "timestamp": [16725225600, "s", None]
        }
        assert pandas_label_descriptor(dt.datetime(2020, 1, 1)) == {
            "timestamp": [1577836800000000, "us", None]
        }
        assert pandas_label_descriptor(np.datetime64("2020-01-01", "s")) == {
            "timestamp": [1577836800, "s", None]
        }
        assert pandas_label_descriptor(np.timedelta64(5, "s")) == {
            "timedelta": [5, "s"]
        }
        assert pandas_label_descriptor(dt.timedelta(seconds=5)) == {
            "timedelta": [5000000, "us"]
        }
        assert pandas_label_descriptor(np.timedelta64("NaT")) == {"nat": "timedelta"}
        assert pandas_label_descriptor(dt.date(2020, 1, 2)) == {"date": "2020-01-02"}
        assert pandas_label_descriptor(pd.Timedelta(seconds=1).as_unit("ns")) == {
            "timedelta": [1000000000, "ns"]
        }
        assert pandas_label_descriptor(pd.Period("2020-01", freq="M")) == {
            "period": [600, "M"]
        }
        assert pandas_label_descriptor(pd.NaT) == {"nat": "datetime"}

    def test_dtype_grammar(self) -> None:
        """Pin of the dtype terms."""
        from orcapod.hashing.pandas_value_hash import pandas_dtype_descriptor

        assert pandas_dtype_descriptor(np.dtype("datetime64[ns]")) == {
            "numpy": "datetime64[ns]"
        }
        assert pandas_dtype_descriptor(pd.DatetimeTZDtype("ns", "UTC")) == {
            "datetimetz": ["ns", "UTC"]
        }
        # A timezone is the text storage writes, whatever class holds it.
        plus_one = dt.timezone(dt.timedelta(hours=1))
        assert pandas_dtype_descriptor(pd.DatetimeTZDtype("ns", plus_one)) == {
            "datetimetz": ["ns", "+01:00"]
        }
        assert pandas_dtype_descriptor(pd.DatetimeTZDtype("ns", dt.timezone.utc)) == {
            "datetimetz": ["ns", "UTC"]
        }
        # The storage of a string dtype is not restored by storage, so it is not a term.
        assert pandas_dtype_descriptor(pd.StringDtype("pyarrow")) == "string"
        assert pandas_dtype_descriptor(pd.StringDtype("python")) == "string"
        assert pandas_dtype_descriptor(pd.BooleanDtype()) == {"masked": "boolean"}
        assert pandas_dtype_descriptor(pd.PeriodDtype("M")) == {"period": "M"}
        assert pandas_dtype_descriptor(pd.PeriodDtype("Q")) == {"period": "Q-DEC"}
        assert pandas_dtype_descriptor(pd.IntervalDtype("int64", "left")) == {
            "interval": [{"numpy": "int64"}, "left"]
        }


def _arrow_series(values: list[Any], arrow_type: pa.DataType) -> pd.Series:
    """A named ``pd.ArrowDtype`` Series of ``values`` typed ``arrow_type``."""
    return pd.Series(values, dtype=pd.ArrowDtype(arrow_type), name="s")


class TestArrowDtypeLeafTypeIsIdentity:
    """A ``pd.ArrowDtype`` of a non-nested Arrow type keeps its exact type in the identity.

    Storage restores the dtype with its exact Arrow type (``binary`` and ``large_binary`` come
    back as themselves), so the type is part of the hash, although the Arrow value digest
    unifies large and non-large variants (ITL-897, review of PR #339).
    """

    PAIRS: dict[str, tuple[pa.DataType, pa.DataType]] = {
        "binary": (pa.binary(), pa.large_binary()),
        "timestamp_tz": (
            pa.timestamp("ns", tz="UTC"),
            pa.timestamp("ns", tz="Europe/Berlin"),
        ),
    }

    @staticmethod
    def _values(name: str) -> list[Any]:
        """The values of pair ``name`` in a form both of its types accept."""
        if name == "timestamp_tz":
            return [pd.Timestamp("2020-01-01", tz="UTC"), None]
        return [b"x", None]

    @pytest.mark.parametrize("name", list(PAIRS))
    @pytest.mark.parametrize("kind", ["series", "frame"])
    def test_the_two_types_hash_differently_and_round_trip_equal(
        self, name: str, kind: str
    ) -> None:
        """Two exact Arrow types give two hashes; each hashes the same after storage."""
        first, second = self.PAIRS[name]
        values = self._values(name)
        built = [_arrow_series(values, t) for t in (first, second)]
        if kind == "frame":
            built = [s.to_frame() for s in built]
        assert _hash(built[0]) != _hash(built[1])
        for value in built:
            assert _hash(_stored_and_read(value)) == _hash(value)

    @pytest.mark.parametrize("kind", ["series", "frame"])
    def test_decimal_precision_is_not_identity(self, kind: str) -> None:
        """Storage reads a decimal column back as ``object``, so its precision is not kept.

        By the strict rule (only what storage keeps counts) two precisions hash equal, and the
        term is the restored ``object`` dtype's, not an Arrow type text.
        """
        import decimal

        values = [decimal.Decimal("1.50"), None]
        built = [
            _arrow_series(values, pa.decimal128(precision, 2)) for precision in (10, 12)
        ]
        if kind == "frame":
            built = [s.to_frame() for s in built]
        assert _hash(built[0]) == _hash(built[1])
        assert _hash(_stored_and_read(built[0])) == _hash(built[0])

    def test_nested_arrow_dtypes_are_refused(self) -> None:
        """A nested or dictionary ``pd.ArrowDtype`` is refused by the hash (ITL-1054).

        Storage cannot read such a column back, so no hash describes one: the hash refuses it
        with storage's own message, and a ``list`` and a ``large_list`` column are never
        confused.
        """
        for nested in (
            pa.list_(pa.int64()),
            pa.large_list(pa.int64()),
            pa.struct([("s", pa.string())]),
            pa.map_(pa.string(), pa.int64()),
            pa.dictionary(pa.int8(), pa.string()),
        ):
            empty = pa.array([], nested)
            with pytest.raises(ValueError, match="cannot be read back"):
                _hash(pd.Series(empty, dtype=pd.ArrowDtype(nested), name="s"))

    def test_leaf_terms(self) -> None:
        """The term of a leaf type is its Arrow type text (``null`` included)."""
        from orcapod.hashing.pandas_value_hash import pandas_dtype_descriptor

        # Non-public descriptor function: the test is about the grammar itself.
        for leaf, text in (
            (pa.binary(), "binary"),
            (pa.large_binary(), "large_binary"),
            (pa.null(), "null"),
            (pa.decimal128(10, 2), "decimal128(10, 2)"),
            (pa.timestamp("ns", tz="UTC"), "timestamp[ns, tz=UTC]"),
        ):
            assert pandas_dtype_descriptor(pd.ArrowDtype(leaf)) == {"arrow": text}
