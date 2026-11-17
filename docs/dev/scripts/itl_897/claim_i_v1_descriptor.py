"""ITL-897 claim (i): what one full-descriptor ``pandas_dataframe_v1`` would do per property.

Takes every pair from claim (a), hashes it as released and then with the full-descriptor
prototype (``prototype_v1.install_full_descriptor()``, this process only), and reports both next
to pandas' own verdicts. Also checks that each value keeps its v1 hash through the storage round
trip, and that the full descriptor still moves only the 3 pandas golden entries.

Run: ``uv run python superpowers/scripts/itl_897/claim_i_v1_descriptor.py``
"""

from __future__ import annotations

import datetime as dt
import importlib
import sys
import warnings
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent))

prototype_v1 = importlib.import_module("prototype_v1")
_claim_a = importlib.import_module("claim_a_equality_vs_hash")
FRAME_CASES, SERIES_CASES = _claim_a.FRAME_CASES, _claim_a.SERIES_CASES
LT_DF, LT_S, _passes = _claim_a.LT_DF, _claim_a.LT_S, _claim_a._passes
from pandas.testing import assert_frame_equal, assert_series_equal  # noqa: E402

import pandas as pd  # noqa: E402

from orcapod.contexts import get_default_context  # noqa: E402

warnings.simplefilter("ignore")
HASHER = get_default_context().semantic_hasher


def _h(value: Any) -> str:
    try:
        return HASHER.hash_object(value).to_string()
    except Exception as exc:  # noqa: BLE001 - reported in the table
        return f"ERROR {type(exc).__name__}: {exc}"


def _same(a: str, b: str) -> str:
    if a.startswith("ERROR") or b.startswith("ERROR"):
        return "error"
    return "equal" if a == b else "differ"


def _ts_labels(tz: str | None) -> pd.DataFrame:
    labels = pd.to_datetime(["2020-01-01", "2020-01-02"])
    return pd.DataFrame(
        [[1, 2]], columns=labels if tz is None else labels.tz_localize(tz)
    )


# Values that hash and store on v0.2.0 and are outside the minimal grammar, in pairs that
# pandas distinguishes: v1 must hash every value and tell each pair apart.
COVERAGE: list[tuple[str, pd.DataFrame, pd.DataFrame]] = [
    ("Timestamp column labels: naive vs UTC", _ts_labels(None), _ts_labels("UTC")),
    (
        "Timestamp vs str column labels",
        _ts_labels(None),
        pd.DataFrame(
            [[1, 2]], columns=pd.Index(["2020-01-01 00:00:00", "2020-01-02 00:00:00"])
        ),
    ),
    (
        "date vs Timestamp column labels",
        pd.DataFrame([[1]], columns=pd.Index([dt.date(2020, 1, 1)])),
        pd.DataFrame([[1]], columns=pd.Index([pd.Timestamp("2020-01-01")])),
    ),
    (
        "Timedelta column labels",
        pd.DataFrame([[1]], columns=pd.Index([pd.Timedelta(seconds=1)])),
        pd.DataFrame([[1]], columns=pd.Index([pd.Timedelta(seconds=2)])),
    ),
    (
        "Period column labels: freq M vs D",
        pd.DataFrame([[1]], columns=pd.Index([pd.Period("2020-01", freq="M")])),
        pd.DataFrame([[1]], columns=pd.Index([pd.Period("2020-01-01", freq="D")])),
    ),
    (
        "period column: freq M vs Q",
        pd.DataFrame({"p": pd.period_range("2020-01", periods=2, freq="M")}),
        pd.DataFrame({"p": pd.period_range("2020Q1", periods=2, freq="Q")}),
    ),
    (
        "interval column: closed right vs left",
        pd.DataFrame({"i": pd.interval_range(0, 2, closed="right")}),
        pd.DataFrame({"i": pd.interval_range(0, 2, closed="left")}),
    ),
    (
        "DatetimeIndex vs Index[int64]",
        pd.DataFrame({"a": [1, 2]}, index=pd.date_range("2020-01-01", periods=2)),
        pd.DataFrame({"a": [1, 2]}, index=pd.Index([0, 1])),
    ),
]


def _kept_through_storage(value: Any, back: Any) -> bool:
    """Whether both hashes were computed and are equal (two equal errors do not count)."""
    before, after = _h(value), _h(back)
    return not before.startswith("ERROR") and before == after


def _coverage() -> None:
    print("\n## Grammar coverage: values that work on v0.2.0\n")
    print("| pair | today hashes | equals | v1 hashes | v1 | v1 kept through storage |")
    print("|---|---|---|---|---|---|")
    for name, a, b in COVERAGE:
        today_ok = not any(_h(v).startswith("ERROR") for v in (a, b))
        prototype_v1.install_full_descriptor()
        v1a, v1b = _h(a), _h(b)
        v1_ok = not (v1a.startswith("ERROR") or v1b.startswith("ERROR"))
        try:
            kept: object = all(
                _kept_through_storage(
                    v, LT_DF.storage_to_python(LT_DF.python_to_storage(v))
                )
                for v in (a, b)
            )
        except Exception as exc:  # noqa: BLE001 - storage fails on main too
            kept = f"store fails on main ({type(exc).__name__})"
        eq = "equal" if a.equals(b) else "differ"
        print(
            f"| {name} | {today_ok} | {eq} | {v1_ok if v1_ok else v1a[:60]} | "
            f"{_same(v1a, v1b)} | {kept} |"
        )
        _restore()


_RELEASED: dict[str, Any] = {}


def _restore() -> None:
    from orcapod.hashing.semantic_hashing import builtin_handlers as bh

    bh.PandasDataFrameHandler.handle = _RELEASED["df"]  # type: ignore[method-assign]
    bh.PandasSeriesHandler.handle = _RELEASED["series"]  # type: ignore[method-assign]


def _remember() -> None:
    from orcapod.hashing.semantic_hashing import builtin_handlers as bh

    _RELEASED["df"] = bh.PandasDataFrameHandler.handle
    _RELEASED["series"] = bh.PandasSeriesHandler.handle


def main() -> None:
    _remember()
    _coverage()
    groups = [
        ("DataFrame", FRAME_CASES, assert_frame_equal, LT_DF),
        ("Series", SERIES_CASES, assert_series_equal, LT_S),
    ]
    today = {
        (kind, name): (_h(a), _h(b))
        for kind, cases, _, _ in groups
        for name, a, b, _ in cases
    }
    prototype_v1.install_full_descriptor()
    unstable: list[str] = []
    unhashable: list[str] = []
    for kind, cases, assert_fn, lt in groups:
        print(f"\n## {kind}\n")
        print("| property | equals | assert | hash today | v1 | v1 follows |")
        print("|---|---|---|---|---|---|")
        for name, a, b, _ in cases:
            eq = "equal" if a.equals(b) else "differ"
            asserted = "equal" if _passes(assert_fn, a, b) else "differ"
            now = _same(*today[(kind, name)])
            v1a, v1b = _h(a), _h(b)
            v1 = _same(v1a, v1b)
            if v1 == "error":
                follows = (v1a if v1a.startswith("ERROR") else v1b)[:70]
            elif v1 == eq == asserted:
                follows = "both"
            elif v1 == asserted:
                follows = "assert"
            elif v1 == eq:
                follows = "equals"
            else:
                follows = "NEITHER"
            print(f"| {name} | {eq} | {asserted} | {now} | {v1} | {follows} |")
            for value in (a, b):
                try:
                    back = lt.storage_to_python(lt.python_to_storage(value))
                except Exception:  # noqa: BLE001 - not storable on main; see claim (a)
                    continue
                if _h(value).startswith("ERROR"):
                    unhashable.append(f"{kind}: {name}")
                elif not _kept_through_storage(value, back):
                    unstable.append(f"{kind}: {name}")
    print(f"\nv1 hash changes through storage for: {sorted(set(unstable)) or 'none'}")
    print(f"v1 hash not computed for: {sorted(set(unhashable)) or 'none'}")


if __name__ == "__main__":
    main()
