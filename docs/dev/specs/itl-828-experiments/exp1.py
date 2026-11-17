"""Experiment: candidate identity representations of Polars expressions."""

import hashlib
import json
import sys
from typing import Any
import warnings
from datetime import date, datetime

import polars as pl


def exprs():
    return {
        "gt0": pl.col("x") > 0,
        "gt0_method": pl.col("x").gt(0),
        "gt0_lit": pl.col("x") > pl.lit(0),
        "gt0_rev": 0 < pl.col("x"),
        "gt0_float": pl.col("x") > 0.0,
        "gt1": pl.col("x") > 1,
        "y_gt0": pl.col("y") > 0,
        "gt0_int8lit": pl.col("x") > pl.lit(0, dtype=pl.Int8),
        "gt_big": pl.col("x") > 2**40,
        "gt_huge": pl.col("x") > 2**70,
        "date": pl.col("d") > date(2024, 1, 1),
        "datetime": pl.col("d") > datetime(2024, 1, 1, 12),
        "and": (pl.col("x") > 0) & (pl.col("y") < 5),
        "and_swapped": (pl.col("y") < 5) & (pl.col("x") > 0),
        "sum_gt": (pl.col("x") + pl.col("y")) > 3,
        "is_in": pl.col("x").is_in([1, 2, 3]),
        "between": pl.col("x").is_between(1, 5),
        "str_eq": pl.col("s") == "abc",
        "str_contains": pl.col("s").str.contains("a"),
        "is_null": pl.col("x").is_null(),
        "cast": pl.col("x").cast(pl.Int64) > 0,
        "nan": pl.col("x") > float("nan"),
        "lit_true": pl.lit(True),
        "map_elements": pl.col("x").map_elements(
            lambda v: v > 0, return_dtype=pl.Boolean
        ),
    }


def main():
    out: dict[str, Any] = {"version": pl.__version__}
    for name, e in exprs().items():
        rec: dict[str, Any] = {"str": str(e)}
        try:
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                b = e.meta.serialize(format="binary")
            rec["bin_sha"] = hashlib.sha256(b).hexdigest()[:16]
            rec["bin_head"] = b[:24].hex()
            rec["bin_warn"] = [str(x.message)[:80] for x in w]
        except Exception as exc:  # noqa: BLE001
            rec["bin_err"] = f"{type(exc).__name__}: {str(exc)[:120]}"
        try:
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                j = e.meta.serialize(format="json")
            rec["json"] = j if isinstance(j, str) else j.decode()
            rec["json_warn"] = [str(x.message)[:80] for x in w]
        except Exception as exc:  # noqa: BLE001
            rec["json_err"] = f"{type(exc).__name__}: {str(exc)[:120]}"
        out[name] = rec
    json.dump(out, sys.stdout, indent=1)


main()
