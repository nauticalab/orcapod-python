"""ITL-897 claim (a): what pandas equality depends on vs what Orcapod's hash depends on.

For each property, builds two values that differ only in that property and reports:

* ``equals``  -- ``DataFrame.equals`` / ``Series.equals`` says the two are equal
* ``assert``  -- ``assert_frame_equal`` / ``assert_series_equal`` with default arguments passes
* ``hash==``  -- Orcapod's semantic hash (default data context) is the same for both
* ``stored``  -- the stored round trip (``LogicalPandasDataFrame`` / ``LogicalPandasSeries``,
  Arrow IPC) gives back a value that keeps the property of the second value

A row is a MISMATCH when pandas (``equals`` or the default ``assert``) says "different" but
the hash is equal (a cache collision), or pandas says "equal" on both checks but the hash
differs (a spurious cache miss).

Run: ``uv run python superpowers/scripts/itl_897/claim_a_equality_vs_hash.py``
"""

from __future__ import annotations

import warnings
from collections.abc import Callable
from typing import Any

import numpy as np
import pandas as pd
import pyarrow as pa
from pandas.testing import assert_frame_equal, assert_series_equal

from orcapod.contexts import get_default_context
from orcapod.logical_types.pandas_type import (
    LogicalPandasDataFrame,
    LogicalPandasSeries,
)

warnings.simplefilter("ignore")
HASHER = get_default_context().semantic_hasher
LT_DF = LogicalPandasDataFrame()
LT_S = LogicalPandasSeries()


def _df(**kw: Any) -> pd.DataFrame:
    return pd.DataFrame({"a": [1, 2], "b": ["x", "y"]}, **kw)


def _with_attrs(df: Any, attrs: dict[str, Any]) -> Any:
    out = df.copy()
    out.attrs = attrs
    return out


def _with_columns_name(df: pd.DataFrame, name: str) -> pd.DataFrame:
    out = df.copy()
    out.columns = out.columns.rename(name)
    return out


def _arrow_struct_frame(order: list[str]) -> pd.DataFrame:
    types = {"u": pa.int64(), "v": pa.large_string()}
    struct = pa.struct([(name, types[name]) for name in order])
    array = pa.array([{"u": 1, "v": "x"}], struct)
    return pd.DataFrame({"s": pd.Series(array, dtype=pd.ArrowDtype(struct))})


def _no_dup_flag(df: pd.DataFrame) -> pd.DataFrame:
    return df.set_flags(allows_duplicate_labels=False)


# (property, value_1, value_2, keeps(roundtripped_2) -> bool)
Keeps = Callable[[Any], bool]
FRAME_CASES: list[tuple[str, Any, Any, Keeps]] = [
    (
        "column order",
        _df(),
        _df()[["b", "a"]],
        lambda r: list(r.columns) == ["b", "a"],
    ),
    (
        "row order",
        _df(),
        _df().iloc[::-1],
        lambda r: list(r["a"]) == [2, 1],
    ),
    (
        "index values",
        _df(),
        _df(index=[10, 20]),
        lambda r: list(r.index) == [10, 20],
    ),
    (
        "index name",
        _df(),
        _df(index=pd.RangeIndex(2, name="id")),
        lambda r: r.index.name == "id",
    ),
    (
        "index type: RangeIndex vs Index[int64], same values",
        _df(),
        _df(index=pd.Index([0, 1], dtype="int64")),
        lambda r: type(r.index) is pd.Index and r.index.dtype == "int64",
    ),
    (
        "index dtype: int64 vs float64, same values",
        _df(index=pd.Index([0, 1], dtype="int64")),
        _df(index=pd.Index([0.0, 1.0], dtype="float64")),
        lambda r: r.index.dtype == "float64",
    ),
    (
        "MultiIndex level names",
        _df(index=pd.MultiIndex.from_tuples([(0, "p"), (1, "q")])),
        _df(index=pd.MultiIndex.from_tuples([(0, "p"), (1, "q")], names=["i", "j"])),
        lambda r: list(r.index.names) == ["i", "j"],
    ),
    (
        "MultiIndex level order (swaplevel)",
        _df(index=pd.MultiIndex.from_tuples([(0, "p"), (1, "q")], names=["i", "j"])),
        _df(
            index=pd.MultiIndex.from_tuples([(0, "p"), (1, "q")], names=["i", "j"])
        ).swaplevel(),
        lambda r: list(r.index.names) == ["j", "i"],
    ),
    (
        "columns.name",
        _df(),
        _with_columns_name(_df(), "features"),
        lambda r: r.columns.name == "features",
    ),
    (
        "column label type: 0 vs '0'",
        pd.DataFrame({0: [1, 2]}),
        pd.DataFrame({"0": [1, 2]}),
        lambda r: list(r.columns) == ["0"],
    ),
    (
        "MultiIndex columns vs flat tuple-ish labels",
        pd.DataFrame(
            [[1, 2]], columns=pd.MultiIndex.from_tuples([("a", "x"), ("a", "y")])
        ),
        pd.DataFrame([[1, 2]], columns=pd.Index(["('a', 'x')", "('a', 'y')"])),
        lambda r: not isinstance(r.columns, pd.MultiIndex),
    ),
    (
        "column dtype: int64 vs float64",
        pd.DataFrame({"a": [1, 2]}),
        pd.DataFrame({"a": [1.0, 2.0]}),
        lambda r: r["a"].dtype == "float64",
    ),
    (
        "column dtype: int64 vs int32",
        pd.DataFrame({"a": np.array([1, 2], dtype="int64")}),
        pd.DataFrame({"a": np.array([1, 2], dtype="int32")}),
        lambda r: r["a"].dtype == "int32",
    ),
    (
        "categorical vs object string (ITL-887)",
        pd.DataFrame({"a": ["x", "y"]}),
        pd.DataFrame({"a": pd.Categorical(["x", "y"])}),
        lambda r: isinstance(r["a"].dtype, pd.CategoricalDtype),
    ),
    (
        "categorical: category order",
        pd.DataFrame({"a": pd.Categorical(["x", "y"], categories=["x", "y"])}),
        pd.DataFrame({"a": pd.Categorical(["x", "y"], categories=["y", "x"])}),
        lambda r: list(r["a"].cat.categories) == ["y", "x"],
    ),
    (
        "categorical: unused category",
        pd.DataFrame({"a": pd.Categorical(["x", "y"], categories=["x", "y"])}),
        pd.DataFrame({"a": pd.Categorical(["x", "y"], categories=["x", "y", "z"])}),
        lambda r: list(r["a"].cat.categories) == ["x", "y", "z"],
    ),
    (
        "categorical: ordered flag",
        pd.DataFrame({"a": pd.Categorical(["x", "y"], ordered=False)}),
        pd.DataFrame({"a": pd.Categorical(["x", "y"], ordered=True)}),
        lambda r: bool(r["a"].cat.ordered),
    ),
    (
        "nullable Int64 vs int64 (no nulls)",
        pd.DataFrame({"a": pd.array([1, 2], dtype="int64")}),
        pd.DataFrame({"a": pd.array([1, 2], dtype="Int64")}),
        lambda r: str(r["a"].dtype) == "Int64",
    ),
    (
        "nullable boolean vs bool (no nulls)",
        pd.DataFrame({"a": [True, False]}),
        pd.DataFrame({"a": pd.array([True, False], dtype="boolean")}),
        lambda r: str(r["a"].dtype) == "boolean",
    ),
    (
        "string[python] vs object str",
        pd.DataFrame({"a": ["x", "y"]}),
        pd.DataFrame({"a": pd.array(["x", "y"], dtype="string")}),
        lambda r: str(r["a"].dtype) == "string",
    ),
    (
        "Int64 <NA> vs float64 NaN",
        pd.DataFrame({"a": [1.0, np.nan]}),
        pd.DataFrame({"a": pd.array([1, None], dtype="Int64")}),
        lambda r: str(r["a"].dtype) == "Int64",
    ),
    (
        "datetime64 unit ns vs us",
        pd.DataFrame({"t": pd.to_datetime(["2020-01-01"]).astype("datetime64[ns]")}),
        pd.DataFrame({"t": pd.to_datetime(["2020-01-01"]).astype("datetime64[us]")}),
        lambda r: str(r["t"].dtype) == "datetime64[us]",
    ),
    (
        "datetime tz: UTC vs naive (same wall clock)",
        pd.DataFrame({"t": pd.to_datetime(["2020-01-01"])}),
        pd.DataFrame({"t": pd.to_datetime(["2020-01-01"]).tz_localize("UTC")}),
        lambda r: str(r["t"].dtype) == "datetime64[ns, UTC]",
    ),
    (
        "float -0.0 vs 0.0",
        pd.DataFrame({"a": [0.0]}),
        pd.DataFrame({"a": [-0.0]}),
        lambda r: bool(np.signbit(r["a"].iloc[0])),
    ),
    (
        "struct field order (ArrowDtype struct column)",
        _arrow_struct_frame(["u", "v"]),
        _arrow_struct_frame(["v", "u"]),
        lambda r: [f.name for f in r["s"].dtype.pyarrow_dtype] == ["v", "u"],
    ),
    (
        "dict key order (object column of dicts)",
        pd.DataFrame({"s": [{"u": 1, "v": "x"}]}),
        pd.DataFrame({"s": [{"v": "x", "u": 1}]}),
        lambda r: list(r["s"].iloc[0]) == ["v", "u"],
    ),
    (
        "attrs",
        _df(),
        _with_attrs(_df(), {"unit": "mV"}),
        lambda r: r.attrs == {"unit": "mV"},
    ),
    (
        "flags.allows_duplicate_labels",
        _df(),
        _no_dup_flag(_df()),
        lambda r: r.flags.allows_duplicate_labels is False,
    ),
]

SERIES_CASES: list[tuple[str, Any, Any, Keeps]] = [
    (
        "Series name",
        pd.Series([1, 2], name="a"),
        pd.Series([1, 2], name="b"),
        lambda r: r.name == "b",
    ),
    (
        "Series name None vs 'x'",
        pd.Series([1, 2]),
        pd.Series([1, 2], name="x"),
        lambda r: r.name == "x",
    ),
    (
        "Series name 0 vs '0'",
        pd.Series([1, 2], name=0),
        pd.Series([1, 2], name="0"),
        lambda r: r.name == "0" and isinstance(r.name, str),
    ),
    (
        "Series MultiIndex level order (swaplevel)",
        pd.Series(
            [1, 2],
            index=pd.MultiIndex.from_tuples([(0, "p"), (1, "q")], names=["i", "j"]),
            name="v",
        ),
        pd.Series(
            [1, 2],
            index=pd.MultiIndex.from_tuples([(0, "p"), (1, "q")], names=["i", "j"]),
            name="v",
        ).swaplevel(),
        lambda r: list(r.index.names) == ["j", "i"],
    ),
    (
        "Series index name",
        pd.Series([1, 2]),
        pd.Series([1, 2], index=pd.RangeIndex(2, name="id")),
        lambda r: r.index.name == "id",
    ),
    (
        "Series dtype int64 vs Int64",
        pd.Series([1, 2], dtype="int64"),
        pd.Series([1, 2], dtype="Int64"),
        lambda r: str(r.dtype) == "Int64",
    ),
    (
        "Series categorical vs object",
        pd.Series(["x", "y"]),
        pd.Series(pd.Categorical(["x", "y"])),
        lambda r: isinstance(r.dtype, pd.CategoricalDtype),
    ),
    (
        "Series attrs",
        pd.Series([1, 2]),
        _with_attrs(pd.Series([1, 2]), {"u": 1}),
        lambda r: r.attrs == {"u": 1},
    ),
]


def _passes(check: Callable[[Any, Any], None], a: Any, b: Any) -> bool:
    try:
        check(a, b)
    except AssertionError:
        return False
    return True


def _hash(value: Any) -> str:
    try:
        return HASHER.hash_object(value).to_string()
    except Exception as exc:  # noqa: BLE001 - report, do not stop the survey
        return f"ERROR {type(exc).__name__}: {exc}"


def _roundtrip_keeps(lt: Any, value: Any, keeps: Keeps) -> str:
    try:
        back = lt.storage_to_python(lt.python_to_storage(value))
    except Exception as exc:  # noqa: BLE001
        return f"ERROR {type(exc).__name__}"
    return "yes" if keeps(back) else "NO"


def _report(
    kind: str,
    cases: list[tuple[str, Any, Any, Keeps]],
    assert_fn: Callable[[Any, Any], None],
    lt: Any,
) -> None:
    print(f"\n## {kind}\n")
    print(
        "| property | equals | assert (defaults) | hash equal | stored keeps it | verdict |"
    )
    print("|---|---|---|---|---|---|")
    for name, v1, v2, keeps in cases:
        eq = bool(v1.equals(v2))
        asserted = _passes(assert_fn, v1, v2)
        h1, h2 = _hash(v1), _hash(v2)
        if h1.startswith("ERROR") or h2.startswith("ERROR"):
            hash_eq = h1 if h1.startswith("ERROR") else h2
            verdict = "hash error"
        else:
            same = h1 == h2
            hash_eq = "yes" if same else "no"
            pandas_differs = (not eq) or (not asserted)
            pandas_same = eq and asserted
            if pandas_differs and same:
                verdict = "MISMATCH: collision"
            elif pandas_same and not same:
                verdict = "MISMATCH: spurious miss"
            elif eq != asserted and not same:
                verdict = "pandas split; hash distinguishes"
            else:
                verdict = "consistent"
        stored = _roundtrip_keeps(lt, v2, keeps)
        print(
            f"| {name} | {'equal' if eq else 'differ'} | "
            f"{'equal' if asserted else 'differ'} | {hash_eq} | {stored} | {verdict} |"
        )


EDGE_FRAMES: list[tuple[str, pd.DataFrame]] = [
    (
        "duplicate column labels ['a', 'a']",
        pd.DataFrame([[1, 2]], columns=pd.Index(["a", "a"])),
    ),
    (
        "labels 1 and '1' in one frame",
        pd.DataFrame([[1, 2]], columns=pd.Index([1, "1"])),
    ),
    ("int label 0", pd.DataFrame({0: [1, 2]})),
    ("zero columns, two rows", pd.DataFrame(index=pd.Index([0, 1]))),
]


def _edges() -> None:
    print("\n## Edge values: hashable? stored round trip?\n")
    print("| value | hash | stored round trip columns | stored equals original |")
    print("|---|---|---|---|")
    for name, df in EDGE_FRAMES:
        h = _hash(df)
        try:
            back = LT_DF.storage_to_python(LT_DF.python_to_storage(df))
            cols, same = repr(list(back.columns)), str(bool(df.equals(back)))
        except Exception as exc:  # noqa: BLE001
            cols, same = f"ERROR {type(exc).__name__}", "-"
        print(f"| {name} | {h if h.startswith('ERROR') else 'ok'} | {cols} | {same} |")


if __name__ == "__main__":
    print(f"pandas {pd.__version__}")
    _report("DataFrame", FRAME_CASES, assert_frame_equal, LT_DF)
    _report("Series", SERIES_CASES, assert_series_equal, LT_S)
    _edges()
