"""Identity of a Polars expression: the ``polars_expr_v1`` canonical encoding (ITL-828).

The canonical tree is Orcapod's own grammar, not Polars' serialization, so the pinned trees in
``CORPUS`` must never change: a change here is a hash change. If a Polars upgrade fails these
tests with a parse error, teach the parser the new layout and map it onto the same tree; never
edit an expected tree to match.
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import textwrap
from collections.abc import Callable
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from typing import Any

import polars as pl
import polars.selectors as cs
import pytest

from orcapod.contexts import get_default_context
from orcapod.hashing.semantic_hashing.polars_expr import (
    POLARS_EXPR_HASH_LABEL,
    polars_expr_canonical,
)
from orcapod.types import ContentHash


def _hash(expr: pl.Expr) -> ContentHash:
    return get_default_context().semantic_hasher.hash_object(expr)


def _gt_x(literal: dict[str, Any]) -> dict[str, Any]:
    return {"binary": ["gt", {"col": "x"}, {"lit": literal}]}


# (name, expression factory, pinned canonical tree). DO NOT EDIT an expected tree.
CORPUS: list[tuple[str, Callable[[], pl.Expr], Any]] = [
    ("column", lambda: pl.col("flag"), {"col": "flag"}),
    ("gt_dyn_int", lambda: pl.col("x") > 0, _gt_x({"dyn_int": "0"})),
    ("gt_dyn_int_huge", lambda: pl.col("x") > 2**70, _gt_x({"dyn_int": str(2**70)})),
    ("gt_dyn_float", lambda: pl.col("x") > 0.0, _gt_x({"dyn_float": "0x0.0p+0"})),
    (
        "gt_dyn_float_neg0",
        lambda: pl.col("x") > -0.0,
        _gt_x({"dyn_float": "-0x0.0p+0"}),
    ),
    ("gt_dyn_float_01", lambda: pl.col("x") > 0.1, _gt_x({"dyn_float": (0.1).hex()})),
    ("gt_int8", lambda: pl.col("x") > pl.lit(0, dtype=pl.Int8), _gt_x({"int8": "0"})),
    (
        "gt_uint64",
        lambda: pl.col("x") > pl.lit(7, dtype=pl.UInt64),
        _gt_x({"uint64": "7"}),
    ),
    (
        "gt_float32",
        lambda: pl.col("x") > pl.lit(0.1, dtype=pl.Float32),
        _gt_x({"float32": "0x1.99999a0000000p-4"}),
    ),
    (
        "gt_float64",
        lambda: pl.col("x") > pl.lit(0.1, dtype=pl.Float64),
        _gt_x({"float64": "0x1.999999999999ap-4"}),
    ),
    (
        "eq_string",
        lambda: pl.col("x") == "abc",
        {"binary": ["eq", {"col": "x"}, {"lit": {"string": "abc"}}]},
    ),
    (
        "eq_bool",
        lambda: pl.col("x") == True,  # noqa: E712 - builds an Expr, not a bool
        {"binary": ["eq", {"col": "x"}, {"lit": {"boolean": True}}]},
    ),
    (
        "eq_null",
        lambda: pl.col("x") == None,  # noqa: E711 - builds an Expr, not a bool
        {"binary": ["eq", {"col": "x"}, {"lit": {"null": None}}]},
    ),
    ("gt_date", lambda: pl.col("x") > date(2024, 1, 1), _gt_x({"date": 19723})),
    ("gt_time", lambda: pl.col("x") > time(12, 0), _gt_x({"time": 43200000000000})),
    (
        "gt_datetime",
        lambda: pl.col("x") > datetime(2024, 1, 1, 12),
        _gt_x({"datetime": [1704110400000000, "us", None]}),
    ),
    (
        "gt_datetime_utc",
        lambda: pl.col("x") > datetime(2024, 1, 1, tzinfo=timezone.utc),
        _gt_x({"datetime": [1704067200000000, "us", "UTC"]}),
    ),
    (
        "gt_duration",
        lambda: pl.col("x") > timedelta(hours=1),
        _gt_x({"duration": [3600000000, "us"]}),
    ),
    (
        "gt_decimal",
        lambda: pl.col("x") > Decimal("1.50"),
        _gt_x({"decimal": ["150", 38, 2]}),
    ),
    (
        "eq_binary",
        lambda: pl.col("x") == b"ab",
        {"binary": ["eq", {"col": "x"}, {"lit": {"binary": "6162"}}]},
    ),
    (
        "ops",
        lambda: ((pl.col("a") + 1) * 2 - pl.col("b") / 3) // 4 % 5 != 0,
        {
            "binary": [
                "ne",
                {
                    "binary": [
                        "mod",
                        {
                            "binary": [
                                "floordiv",
                                {
                                    "binary": [
                                        "sub",
                                        {
                                            "binary": [
                                                "mul",
                                                {
                                                    "binary": [
                                                        "add",
                                                        {"col": "a"},
                                                        {"lit": {"dyn_int": "1"}},
                                                    ]
                                                },
                                                {"lit": {"dyn_int": "2"}},
                                            ]
                                        },
                                        {
                                            "binary": [
                                                "truediv",
                                                {"col": "b"},
                                                {"lit": {"dyn_int": "3"}},
                                            ]
                                        },
                                    ]
                                },
                                {"lit": {"dyn_int": "4"}},
                            ]
                        },
                        {"lit": {"dyn_int": "5"}},
                    ]
                },
                {"lit": {"dyn_int": "0"}},
            ]
        },
    ),
    (
        "comparisons",
        lambda: (pl.col("a") <= 1) | (pl.col("b") >= 2) ^ (pl.col("c") < 3),
        {
            "binary": [
                "or",
                {"binary": ["le", {"col": "a"}, {"lit": {"dyn_int": "1"}}]},
                {
                    "binary": [
                        "xor",
                        {"binary": ["ge", {"col": "b"}, {"lit": {"dyn_int": "2"}}]},
                        {"binary": ["lt", {"col": "c"}, {"lit": {"dyn_int": "3"}}]},
                    ]
                },
            ]
        },
    ),
    (
        "and_two_columns",
        lambda: (pl.col("x") > 0) & (pl.col("y") < 5),
        {
            "binary": [
                "and",
                _gt_x({"dyn_int": "0"}),
                {"binary": ["lt", {"col": "y"}, {"lit": {"dyn_int": "5"}}]},
            ]
        },
    ),
    (
        "eq_missing",
        lambda: pl.col("x").eq_missing(1) & pl.col("y").ne_missing(2),
        {
            "binary": [
                "and",
                {"binary": ["eq_missing", {"col": "x"}, {"lit": {"dyn_int": "1"}}]},
                {"binary": ["ne_missing", {"col": "y"}, {"lit": {"dyn_int": "2"}}]},
            ]
        },
    ),
    ("not", lambda: ~pl.col("b"), {"fn": ["not", {}, [{"col": "b"}]]}),
    ("is_null", lambda: pl.col("x").is_null(), {"fn": ["is_null", {}, [{"col": "x"}]]}),
    (
        "is_not_null",
        lambda: pl.col("x").is_not_null(),
        {"fn": ["is_not_null", {}, [{"col": "x"}]]},
    ),
    ("is_nan", lambda: pl.col("x").is_nan(), {"fn": ["is_nan", {}, [{"col": "x"}]]}),
    (
        "is_not_nan",
        lambda: pl.col("x").is_not_nan(),
        {"fn": ["is_not_nan", {}, [{"col": "x"}]]},
    ),
    (
        "negate",
        lambda: -pl.col("x") > 0,
        {
            "binary": [
                "gt",
                {"fn": ["negate", {}, [{"col": "x"}]]},
                {"lit": {"dyn_int": "0"}},
            ]
        },
    ),
    (
        "is_in_ints",
        lambda: pl.col("x").is_in([1, 2, 3]),
        {
            "fn": [
                "is_in",
                {"nulls_equal": False},
                [{"col": "x"}, {"lit": {"list": ["int64", ["1", "2", "3"]]}}],
            ]
        },
    ),
    (
        "is_in_nulls_equal",
        lambda: pl.col("x").is_in([1.5, None], nulls_equal=True),
        {
            "fn": [
                "is_in",
                {"nulls_equal": True},
                [{"col": "x"}, {"lit": {"list": ["float64", [(1.5).hex(), None]]}}],
            ]
        },
    ),
    (
        "is_in_strings",
        lambda: pl.col("s").is_in(["a", "b"]),
        {
            "fn": [
                "is_in",
                {"nulls_equal": False},
                [{"col": "s"}, {"lit": {"list": ["string", ["a", "b"]]}}],
            ]
        },
    ),
    (
        "is_in_dates",
        lambda: pl.col("d").is_in([date(2024, 1, 1)]),
        {
            "fn": [
                "is_in",
                {"nulls_equal": False},
                [{"col": "d"}, {"lit": {"list": ["date", [19723]]}}],
            ]
        },
    ),
    (
        "is_between",
        lambda: pl.col("x").is_between(1, 5, closed="left"),
        {
            "fn": [
                "is_between",
                {"closed": "left"},
                [{"col": "x"}, {"lit": {"dyn_int": "1"}}, {"lit": {"dyn_int": "5"}}],
            ]
        },
    ),
    (
        "all_horizontal",
        lambda: pl.all_horizontal(pl.col("x") > 0, pl.col("b")),
        {"fn": ["all_horizontal", {}, [_gt_x({"dyn_int": "0"}), {"col": "b"}]]},
    ),
    (
        "any_horizontal",
        lambda: pl.any_horizontal(pl.col("a"), pl.col("b")),
        {"fn": ["any_horizontal", {}, [{"col": "a"}, {"col": "b"}]]},
    ),
    (
        "str_contains",
        lambda: pl.col("s").str.contains("a", literal=True),
        {
            "fn": [
                "str.contains",
                {"literal": True, "strict": False},
                [{"col": "s"}, {"lit": {"string": "a"}}],
            ]
        },
    ),
    (
        "str_contains_regex",
        lambda: pl.col("s").str.contains("^a"),
        {
            "fn": [
                "str.contains",
                {"literal": False, "strict": True},
                [{"col": "s"}, {"lit": {"string": "^a"}}],
            ]
        },
    ),
    (
        "str_starts_with",
        lambda: pl.col("s").str.starts_with("a"),
        {"fn": ["str.starts_with", {}, [{"col": "s"}, {"lit": {"string": "a"}}]]},
    ),
    (
        "str_ends_with",
        lambda: pl.col("s").str.ends_with("a"),
        {"fn": ["str.ends_with", {}, [{"col": "s"}, {"lit": {"string": "a"}}]]},
    ),
    (
        "cast_strict",
        lambda: pl.col("x").cast(pl.Int64) > 0,
        {
            "binary": [
                "gt",
                {"cast": ["int64", "strict", {"col": "x"}]},
                {"lit": {"dyn_int": "0"}},
            ]
        },
    ),
    (
        "cast_non_strict_datetime",
        lambda: (
            pl.col("x").cast(pl.Datetime("ns", "Europe/Berlin"), strict=False).is_null()
        ),
        {
            "fn": [
                "is_null",
                {},
                [
                    {
                        "cast": [
                            {"datetime": ["ns", "Europe/Berlin"]},
                            "non_strict",
                            {"col": "x"},
                        ]
                    }
                ],
            ]
        },
    ),
    (
        "cast_list_decimal",
        lambda: pl.col("x").cast(pl.List(pl.Decimal(10, 2))).is_null(),
        {
            "fn": [
                "is_null",
                {},
                [{"cast": [{"list": {"decimal": [10, 2]}}, "strict", {"col": "x"}]}],
            ]
        },
    ),
    (
        "cast_duration",
        lambda: pl.col("x").cast(pl.Duration("ms")).is_null(),
        {
            "fn": [
                "is_null",
                {},
                [{"cast": [{"duration": "ms"}, "strict", {"col": "x"}]}],
            ]
        },
    ),
]


class TestCanonicalCorpus:
    @pytest.mark.parametrize(
        ("factory", "expected"),
        [pytest.param(f, e, id=n) for n, f, e in CORPUS],
    )
    def test_canonical_tree_is_pinned(self, factory, expected):
        assert polars_expr_canonical(factory()) == expected

    def test_corpus_trees_are_distinct(self):
        trees = [json.dumps(e, sort_keys=True) for _, _, e in CORPUS]
        assert len(set(trees)) == len(trees)


class TestHash:
    def test_hash_is_labelled_sha256_of_canonical_json(self):
        expr = pl.col("x") > 0
        canonical = json.dumps(
            polars_expr_canonical(expr),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
        h = _hash(expr)
        assert POLARS_EXPR_HASH_LABEL == "polars_expr_v1"
        assert h.method == "polars_expr_v1"
        assert h.digest == hashlib.sha256(canonical).digest()

    @pytest.mark.parametrize(
        "same",
        [
            lambda: pl.col("x").gt(0),
            lambda: pl.col("x") > pl.lit(0),
            lambda: 0 < pl.col("x"),
        ],
    )
    def test_equal_expressions_written_differently_hash_equal(self, same):
        assert _hash(same()) == _hash(pl.col("x") > 0)

    @pytest.mark.parametrize(
        "other",
        [
            lambda: pl.col("x") > 0.0,
            lambda: pl.col("x") > pl.lit(0, dtype=pl.Int8),
            lambda: pl.col("x") > pl.lit(0, dtype=pl.Int64),
            lambda: pl.col("x") > 1,
            lambda: pl.col("y") > 0,
            lambda: pl.col("x") >= 0,
        ],
    )
    def test_different_expressions_hash_differently(self, other):
        assert _hash(other()) != _hash(pl.col("x") > 0)

    def test_operand_order_is_part_of_the_identity(self):
        a, b = pl.col("x") > 0, pl.col("y") < 5
        assert _hash(a & b) != _hash(b & a)


class TestRejected:
    @pytest.mark.parametrize(
        ("factory", "fragment"),
        [
            pytest.param(
                lambda: pl.col("x").map_elements(
                    lambda v: v > 0, return_dtype=pl.Boolean
                ),
                "Python function",
                id="map_elements",
            ),
            pytest.param(
                lambda: pl.col("x").map_batches(
                    lambda s: s > 0, return_dtype=pl.Boolean
                ),
                "Python function",
                id="map_batches",
            ),
            pytest.param(
                lambda: pl.col("x", "y") > 0, "several columns", id="multi_col"
            ),
            pytest.param(lambda: pl.all() > 0, "several columns", id="all"),
            pytest.param(lambda: pl.col("^x.*$") > 0, "several columns", id="regex"),
            pytest.param(lambda: pl.exclude("s") > 0, "several columns", id="exclude"),
            pytest.param(lambda: cs.numeric() > 0, "several columns", id="selector"),
            pytest.param(
                lambda: pl.col(pl.Int64) > 0, "several columns", id="dtype_col"
            ),
            pytest.param(
                lambda: pl.col("x") > pl.col("x").mean(), "not supported", id="agg"
            ),
            pytest.param(
                lambda: (pl.col("x") > 0).alias("k"), "not supported", id="alias"
            ),
            pytest.param(
                lambda: pl.when(pl.col("x") > 0).then(True).otherwise(False),
                "not supported",
                id="when",
            ),
            pytest.param(lambda: pl.col("x") > float("nan"), "non-finite", id="nan"),
            pytest.param(lambda: pl.col("x") > float("inf"), "non-finite", id="inf"),
            pytest.param(
                lambda: pl.col("x").is_in(pl.Series([1, 2])),
                "not supported",
                id="series_literal",
            ),
            pytest.param(
                lambda: pl.col("x").is_in([b"a", b"b"]),
                "list literals of dtype Binary",
                id="bytes_list",
            ),
            pytest.param(
                lambda: pl.col("x").is_in([Decimal("1.5")]),
                "list literals of dtype Decimal",
                id="decimal_list",
            ),
            pytest.param(
                lambda: pl.col("x").dt.year() == 2024, "not supported", id="dt_year"
            ),
            pytest.param(
                lambda: pl.col("x").cast(pl.Categorical).is_null(),
                "not supported",
                id="cast_cat",
            ),
        ],
    )
    def test_unsupported_expression_raises_naming_it(self, factory, fragment):
        expr = factory()
        with pytest.raises(ValueError) as excinfo:
            _hash(expr)
        message = str(excinfo.value)
        assert str(expr) in message
        assert fragment in message
        # The message tells the user what to do instead.
        assert "keyword constraints" in message
        assert "function pod" in message


_SUBPROCESS_SCRIPT = textwrap.dedent("""\
    import json
    import polars as pl
    from orcapod.contexts import get_default_context

    hasher = get_default_context().semantic_hasher
    exprs = {
        "gt": pl.col("x") > 0,
        "and": (pl.col("x") > 0) & (pl.col("y").is_in(["a", "b"])),
        "date": pl.col("d") > pl.lit(0.5, dtype=pl.Float32),
    }
    print(json.dumps({k: hasher.hash_object(e).to_string() for k, e in exprs.items()}))
""")


def test_hash_is_identical_across_processes_and_hash_seeds():
    results = []
    for seed in ("0", "12345"):
        env = os.environ.copy()
        env["PYTHONHASHSEED"] = seed
        proc = subprocess.run(
            [sys.executable, "-c", _SUBPROCESS_SCRIPT],
            capture_output=True,
            text=True,
            env=env,
            check=False,
        )
        assert proc.returncode == 0, proc.stderr
        results.append(json.loads(proc.stdout))
    assert results[0] == results[1]
    hasher = get_default_context().semantic_hasher
    assert results[0]["gt"] == hasher.hash_object(pl.col("x") > 0).to_string()
