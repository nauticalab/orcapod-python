"""A pandas value storage would not restore is refused when it is written (ITL-1054).

Storage writes a pandas value with pyarrow and reads it back with ``to_pandas()``. Two kinds of
value used to be written although storage could not restore them:

- **Changed without an error.** pyarrow fetches index level ``i`` with
  ``index.get_level_values(i)``, which pandas resolves as a level *name* first. A MultiIndex whose
  level names equal other levels' positions (``names=[1, 0]``) was written with its levels in
  another order, read back changed, and hashed like the reordered frame.
- **Not readable.** A nested, dictionary or fixed-size ``pd.ArrowDtype`` column or index level, and
  a categorical, period, timedelta, interval, date or ``pd.ArrowDtype`` column axis, were written,
  and the read raised. In a pipeline the computed result stayed stored and unreadable.

Both are now refused, with a ``ValueError`` naming the column or level, by ``python_to_storage``
and by the value hash alike (one shared check), on every supported pyarrow version.
"""

from __future__ import annotations

import datetime as dt
import decimal
import warnings
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pytest

from orcapod import PipelineJob, function_pod
from orcapod.contexts import get_default_context
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.logical_types.pandas_type import (
    LogicalPandasDataFrame,
    LogicalPandasSeries,
)
from orcapod.sources import DictSource


def _arrow_values(array: pa.Array[Any]) -> pd.Series:
    """A Series named ``"s"`` of ``pd.ArrowDtype`` over ``array``."""
    return pd.Series(pd.arrays.ArrowExtensionArray(pa.chunked_array([array])), name="s")


def _multi(names: list[Any]) -> pd.MultiIndex:
    """A two-row, two-level index with the given level names."""
    return pd.MultiIndex.from_tuples([("p", 7), ("q", 8)], names=names)


# pd.ArrowDtype values storage writes but cannot read back (as a column and as an index level).
UNREADABLE_ARROW: dict[str, pa.Array[Any]] = {
    "list": pa.array([[1, 2], None], pa.list_(pa.int64())),
    "large_list": pa.array([[1, 2], None], pa.large_list(pa.int64())),
    "fixed_size_list": pa.array([[1, 2], None], pa.list_(pa.int64(), 2)),
    "struct": pa.array([{"u": 1}, None], pa.struct([("u", pa.int64())])),
    "map": pa.array([[("k", 1)], None], pa.map_(pa.string(), pa.int64())),
    "dictionary": pa.array(["x", "y"]).dictionary_encode(),
    "fixed_size_binary": pa.array([b"ab", None], pa.binary(2)),
}
# Readable as an index level? No for these either; readable as a column but refused at write
# by Arrow already for an index level.
UNREADABLE_ARROW_COLUMN_ONLY: dict[str, pa.Array[Any]] = {
    "list_view": pa.array([[1, 2], None], pa.list_view(pa.int64())),
    "run_end_encoded": pa.RunEndEncodedArray.from_arrays(
        pa.array([2], pa.int32()), pa.array([1])
    ),
    "dense_union": pa.UnionArray.from_dense(
        pa.array([0, 1], pa.int8()),
        pa.array([0, 0], pa.int32()),
        [pa.array([1]), pa.array(["x"])],
    ),
}

# Column axes storage writes but cannot read back.
UNREADABLE_COLUMN_AXES: dict[str, pd.Index] = {
    "categorical": pd.CategoricalIndex(["a", "b"]),
    "period": pd.period_range("2020-01", periods=2, freq="M"),
    "timedelta": pd.TimedeltaIndex(["1s", "2s"]),
    "timedelta s": pd.TimedeltaIndex(["1s", "2s"]).as_unit("s"),
    "interval": pd.interval_range(0, 2),
    "dates": pd.Index([dt.date(2020, 1, 1), dt.date(2020, 1, 2)], dtype=object),
    "arrow timestamp": pd.Index(
        pd.array(
            pd.to_datetime(["2020-01-01", "2020-01-02"]),
            dtype=pd.ArrowDtype(pa.timestamp("us")),
        )
    ),
    "arrow duration": pd.Index(
        pd.array(
            [dt.timedelta(seconds=1), dt.timedelta(seconds=2)],
            dtype=pd.ArrowDtype(pa.duration("s")),
        )
    ),
    "arrow bool": pd.Index(pd.array([True, False], dtype="bool[pyarrow]")),
    "arrow date32": pd.Index(
        pd.array(
            [dt.date(2020, 1, 1), dt.date(2020, 1, 2)], dtype=pd.ArrowDtype(pa.date32())
        )
    ),
    "arrow decimal": pd.Index(
        pd.array(
            [decimal.Decimal("1.5"), decimal.Decimal("2.5")],
            dtype=pd.ArrowDtype(pa.decimal128(5, 2)),
        )
    ),
    "multi, categorical level": pd.MultiIndex.from_arrays(
        [pd.CategoricalIndex(["a", "b"]), pd.Index([1, 2])]
    ),
}


def _refused() -> dict[str, tuple[pd.DataFrame | pd.Series, str]]:
    """Every refused value, with a pattern its message must match (the part it names)."""
    cases: dict[str, tuple[pd.DataFrame | pd.Series, str]] = {}
    for names in ([1, 0], [1.0, 0], [True, 0], [0, 2, 1], [1, 0, "x"]):
        tuples = [("p", 7, 1.5)[: len(names)], ("q", 8, 2.5)[: len(names)]]
        index = pd.MultiIndex.from_tuples(tuples, names=names)
        cases[f"frame, level names {names}"] = (
            pd.DataFrame({"a": [10, 20]}, index=index),
            r"index level \d+ .*another order",
        )
        cases[f"series, level names {names}"] = (
            pd.Series([10, 20], name="s", index=index),
            r"index level \d+ .*another order",
        )
    for kind, array in {**UNREADABLE_ARROW, **UNREADABLE_ARROW_COLUMN_ONLY}.items():
        values = _arrow_values(array)
        cases[f"frame, arrow {kind} column"] = (
            values.to_frame(name="col"),
            r"column 'col'.*(cannot be read back|cannot convert)",
        )
        cases[f"series, arrow {kind} values"] = (
            values,
            r"column 's'.*(cannot be read back|cannot convert)",
        )
    for kind, array in UNREADABLE_ARROW.items():
        cases[f"frame, arrow {kind} index level"] = (
            pd.DataFrame({"a": [1, 2]}, index=pd.Index(_arrow_values(array).array)),
            r"index level 0.*cannot be read back",
        )
    cases["frame, arrow float16 index level"] = (
        pd.DataFrame(
            {"a": [1, 2]},
            index=pd.Index(_arrow_values(pa.array([1.5, 2.5], pa.float16())).array),
        ),
        r"index level 0.*cannot be read back",
    )
    for kind, axis in UNREADABLE_COLUMN_AXES.items():
        cases[f"frame, {kind} column axis"] = (
            pd.DataFrame([[1, 2], [3, 4]], columns=axis),
            r"column axis.*cannot be read back",
        )
    tuples = pd.Categorical([(1, 2), (3, 4)])
    cases["frame, tuple categories"] = (
        pd.DataFrame({"a": tuples}),
        r"column 'a'.*cannot be read back",
    )
    cases["series, tuple categories"] = (
        pd.Series(tuples, name="s"),
        r"column 's'.*cannot be read back",
    )
    cases["frame, tuple categories index"] = (
        pd.DataFrame({"a": [1, 2]}, index=pd.CategoricalIndex(tuples)),
        r"index level 0.*cannot be read back",
    )
    cases["frame, column label False"] = (
        pd.DataFrame([[7]], columns=pd.Index([False])),
        r"column axis: the label False would read back from storage as True",
    )
    cases["series, name False"] = (
        pd.Series([7], name=False),
        r"column axis: the label False would read back from storage as True",
    )
    cases["frame, column level holding False"] = (
        pd.DataFrame([[7]], columns=pd.MultiIndex.from_tuples([("a", False)])),
        r"column axis level 1: the label .*False.* would read back from storage as "
        r".*True",
    )
    cases["frame, arrow float16 column axis"] = (
        pd.DataFrame(
            [[1, 2]],
            columns=pd.Index(_arrow_values(pa.array([0.5, 1.5], pa.float16())).array),
        ),
        r"column axis.*cannot convert",
    )
    cases["frame, arrow uuid (extension type) column"] = (
        _arrow_values(pa.array([b"0123456789abcdef"] * 2, pa.uuid())).to_frame("col"),
        r"column 'col'.*cannot be read back",
    )
    cases["frame, interval categories"] = (
        pd.DataFrame({"a": pd.Categorical(pd.interval_range(0, 2))}),
        r"column 'a'.*cannot be read back",
    )
    tuple_level = pd.MultiIndex.from_tuples(
        [((1, 2), 7), ((3, 4), 8)], names=["x", "y"]
    )
    cases["frame, MultiIndex level holding tuples"] = (
        pd.DataFrame({"a": [1, 2]}, index=tuple_level),
        r"index level 0 \(name 'x', dtype object\).*cannot be read back",
    )
    cases["series, MultiIndex level holding tuples"] = (
        pd.Series([1, 2], name="s", index=tuple_level),
        r"index level 0 \(name 'x', dtype object\).*cannot be read back",
    )
    sub_microsecond = pd.Index(
        pd.arrays.ArrowExtensionArray(
            pa.array([1, 2000], pa.int64()).view(pa.time64("ns"))
        )
    )
    cases["frame, time64[ns] index with sub-microsecond values"] = (
        pd.DataFrame({"a": [1, 2]}, index=sub_microsecond),
        r"index level 0.*cannot be read back.*non-zero nanoseconds",
    )
    cases["series, time64[ns] index with sub-microsecond values"] = (
        pd.Series([1, 2], name="s", index=sub_microsecond),
        r"index level 0.*cannot be read back.*non-zero nanoseconds",
    )
    cases["frame, index named like a generated field"] = (
        pd.DataFrame({"a": [1]}, index=pd.Index([7], name="__index_level_0__")),
        r"index level 0 \(name '__index_level_0__'\): the name would read back from "
        r"storage as None",
    )
    cases["frame, MultiIndex level named like a generated field"] = (
        pd.DataFrame(
            {"a": [1]},
            index=pd.MultiIndex.from_tuples([(7, 8)], names=["x", "__index_level_1__"]),
        ),
        r"index level 1 \(name '__index_level_1__'\): the name would read back",
    )
    cases["series, index named like a generated field"] = (
        pd.Series([1], name="s", index=pd.Index([7], name="__index_level_0__")),
        r"index level 0 \(name '__index_level_0__'\): the name would read back",
    )
    return cases


REFUSED = _refused()

# Neighbours of the refused values that storage restores: they stay accepted.
ACCEPTED: dict[str, pd.DataFrame | pd.Series] = {
    "level names [0, 1] (in order)": pd.DataFrame({"a": [1, 2]}, index=_multi([0, 1])),
    "level names [5, 7]": pd.DataFrame({"a": [1, 2]}, index=_multi([5, 7])),
    "level names ['1', '0'] (text)": pd.DataFrame(
        {"a": [1, 2]}, index=_multi(["1", "0"])
    ),
    "level names [0, 'x']": pd.Series([1, 2], name="s", index=_multi([0, "x"])),
    "column axis level names [1, 0]": pd.DataFrame(
        [[1, 2]], columns=pd.MultiIndex.from_tuples([("a", 1), ("b", 2)], names=[1, 0])
    ),
    "arrow string column": _arrow_values(pa.array(["x", None])).to_frame(),
    "arrow large_binary values": _arrow_values(
        pa.array([b"x", None], pa.large_binary())
    ),
    "arrow timestamp tz index": pd.DataFrame(
        {"a": [1, 2]},
        index=pd.Index(
            _arrow_values(
                pa.array(
                    [dt.datetime(2020, 1, 1), dt.datetime(2020, 1, 2)],
                    pa.timestamp("s", tz="UTC"),
                )
            ).array
        ),
    ),
    "Int64 column axis": pd.DataFrame(
        [[1, 2]], columns=pd.Index([0, 1], dtype="Int64")
    ),
    "datetime column axis": pd.DataFrame(
        [[1, 2]], columns=pd.DatetimeIndex(["2020-01-01", "2020-01-02"], tz="UTC")
    ),
    "column label True": pd.DataFrame([[7]], columns=pd.Index([True])),
    "series named True": pd.Series([7], name=True),
    "text and int labels": pd.DataFrame(
        [[1, 2, 3]], columns=pd.Index(["a", "False", 0])
    ),
    "int column axis": pd.DataFrame([[1, 2]], columns=pd.Index([0, 1])),
    "string categories": pd.DataFrame({"a": pd.Categorical(["x", "y"])}),
    "time64[ns] index, whole microseconds": pd.DataFrame(
        {"a": [1, 2]},
        index=pd.Index(
            pd.arrays.ArrowExtensionArray(
                pa.array([1000, 2000], pa.int64()).view(pa.time64("ns"))
            )
        ),
    ),
    "int, tuple and timestamp index names": pd.DataFrame(
        {"a": [1]},
        index=pd.MultiIndex.from_tuples(
            [(7, 8, 9)], names=[5, ("a", 1), pd.Timestamp("2020-01-01")]
        ),
    ),
    "MultiIndex levels of bytes and Decimals": pd.DataFrame(
        {"a": [1, 2]},
        index=pd.MultiIndex.from_arrays(
            [
                pd.Index([b"x", b"y"]),
                pd.Index([decimal.Decimal("1.5"), decimal.Decimal("2.5")]),
            ]
        ),
    ),
    "index named like a field, but not generated": pd.DataFrame(
        {"a": [1]}, index=pd.Index([7], name="__index_level__")
    ),
}


def _logical(
    value: pd.DataFrame | pd.Series,
) -> LogicalPandasDataFrame | LogicalPandasSeries:
    """The logical type that stores ``value``."""
    if isinstance(value, pd.Series):
        return LogicalPandasSeries()
    return LogicalPandasDataFrame()


def _name_text(name: Any) -> Any:
    """An index level name as storage writes it: its text, a tuple's parts each as text."""
    if name is None:
        return None
    if isinstance(name, tuple):
        return str(tuple(_name_text(part) for part in name))
    return str(name)


def _hash(value: Any) -> Any:
    """The semantic hash of ``value`` under the default data context."""
    return get_default_context().semantic_hasher.hash_object(value)


@pytest.fixture(autouse=True)
def _quiet() -> Any:
    """pyarrow warns about non-text index names; not what these tests are about."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        yield


@pytest.mark.parametrize("name", list(REFUSED))
def test_python_to_storage_refuses(name: str) -> None:
    """``python_to_storage`` raises ``ValueError`` naming the column or level."""
    value, pattern = REFUSED[name]
    with pytest.raises(ValueError, match=pattern):
        _logical(value).python_to_storage(value)


@pytest.mark.parametrize("name", list(REFUSED))
def test_hash_refuses_with_the_same_error(name: str) -> None:
    """The value hash refuses the value with the same message as storage."""
    value, _ = REFUSED[name]
    with pytest.raises(ValueError) as stored:
        _logical(value).python_to_storage(value)
    with pytest.raises(ValueError) as hashed:
        _hash(value)
    assert str(hashed.value) == str(stored.value)


@pytest.mark.parametrize("name", list(ACCEPTED))
def test_restorable_neighbours_are_accepted(name: str) -> None:
    """A neighbour storage restores is written and read back, in order, with the same hash."""
    value = ACCEPTED[name]
    logical = _logical(value)
    back = logical.storage_to_python(logical.python_to_storage(value))
    assert list(back.index) == list(value.index)
    assert [_name_text(n) for n in back.index.names] == [
        _name_text(n) for n in value.index.names
    ]
    assert _hash(back) == _hash(value)


def test_level_names_that_are_positions_name_the_level() -> None:
    """The message names the level, its name, and the level its name resolves to."""
    value = pd.DataFrame({"a": [10, 20]}, index=_multi([1, 0]))
    with pytest.raises(ValueError, match=r"index level 0 \(name 1\)") as raised:
        LogicalPandasDataFrame().python_to_storage(value)
    assert "level 1" in str(raised.value)


@function_pod(output_keys="df")
def _make_reordered_frame(n: int) -> pd.DataFrame:
    """A frame whose MultiIndex level names are the other level's position."""
    return pd.DataFrame({"a": [n, n + 1]}, index=_multi([1, 0]))


@function_pod(output_keys="df")
def _make_unreadable_frame(n: int) -> pd.DataFrame:
    """A frame with a ``pd.ArrowDtype`` list column."""
    return _arrow_values(pa.array([[n], None], pa.list_(pa.int64()))).to_frame("col")


@pytest.mark.parametrize("store_name", ["in_memory", "delta"])
@pytest.mark.parametrize("pod", [_make_reordered_frame, _make_unreadable_frame])
def test_pipeline_stores_nothing_for_a_refused_frame(
    store_name: str, pod: Any, tmp_path: Path
) -> None:
    """In a pipeline the row fails at write: nothing is stored, nothing reads back changed."""
    store = (
        InMemoryArrowDatabase()
        if store_name == "in_memory"
        else DeltaTableDatabase(tmp_path / "delta")
    )
    source = DictSource([{"k": "x", "n": 1}], tag_columns=["k"], tag_only_columns=True)
    job = PipelineJob(name="refused", database=store)
    with job:
        pod.pod(source, label="make")
    with pytest.warns(Warning, match="1 of 1 row failed"):
        job.run()
    assert list(job.nodes["make"].iter_data()) == []
    stored_rows = 0
    for path in store.list_table_paths():
        table = store.get_all_records(path)
        stored_rows += 0 if table is None else table.num_rows
    assert stored_rows == 0
