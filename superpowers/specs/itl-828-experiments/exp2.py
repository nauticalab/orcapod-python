"""Experiment 2: more node kinds, UDFs, float edge cases."""

import hashlib
import json
import sys
from typing import Any
from datetime import date, timedelta

import polars as pl
import polars.selectors as cs


def exprs():
    """Return the expressions the experiment serializes, keyed by name."""
    return {
        "cols_multi": pl.col("x", "y") > 0,
        "all": pl.all() > 0,
        "regex": pl.col("^x.*$") > 0,
        "exclude": pl.exclude("s") > 0,
        "selector": cs.numeric() > 0,
        "dtype_col": pl.col(pl.Int64) > 0,
        "all_horizontal": pl.all_horizontal(pl.col("x") > 0, pl.col("y") > 0),
        "not": ~(pl.col("x") > 0),
        "is_not_null": pl.col("x").is_not_null(),
        "when": pl.when(pl.col("x") > 0).then(True).otherwise(False),
        "alias": (pl.col("x") > 0).alias("keep"),
        "struct_field": pl.col("st").struct.field("a") > 0,
        "dt_year": pl.col("d").dt.year() == 2024,
        "list_contains": pl.col("l").list.contains(1),
        "duration": pl.col("d") - pl.col("e") > timedelta(days=1),
        "float_01": pl.col("x") > 0.1,
        "float_inf": pl.col("x") > float("inf"),
        "float_ninf": pl.col("x") > float("-inf"),
        "float_negzero": pl.col("x") > -0.0,
        "lit_null": pl.col("x") == None,  # noqa: E711
        "lit_f32": pl.col("x") > pl.lit(0.5, dtype=pl.Float32),
        "lit_u8": pl.col("x") > pl.lit(1, dtype=pl.UInt8),
        "lit_bool": pl.col("b") == True,  # noqa: E712
        "is_in_str": pl.col("s").is_in(["a", "b"]),
        "is_in_dates": pl.col("d").is_in([date(2024, 1, 1)]),
        "str_starts": pl.col("s").str.starts_with("a"),
        "mean_cmp": pl.col("x") > pl.col("x").mean(),
        "map_elements": pl.col("x").map_elements(
            lambda v: v > 0, return_dtype=pl.Boolean
        ),
        "map_batches": pl.col("x").map_batches(
            lambda s: s > 0, return_dtype=pl.Boolean
        ),
    }


def main():
    """Serialize each expression in several ways and print the results as JSON."""
    out: dict[str, Any] = {"version": pl.__version__}
    for name, e in exprs().items():
        rec: dict[str, Any] = {"str": str(e)}
        try:
            b = e.meta.serialize(format="binary")
            rec["bin_sha"] = hashlib.sha256(b).hexdigest()[:16]
        except Exception as exc:  # noqa: BLE001
            rec["bin_err"] = f"{type(exc).__name__}: {str(exc)[:150]}"
        try:
            j = e.meta.serialize(format="json")
            rec["json"] = (j if isinstance(j, str) else j.decode())[:400]
        except Exception as exc:  # noqa: BLE001
            rec["json_err"] = f"{type(exc).__name__}: {str(exc)[:150]}"
        out[name] = rec
    json.dump(out, sys.stdout, indent=1)


main()
