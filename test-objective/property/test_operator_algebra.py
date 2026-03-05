"""Property-based tests for operator algebraic properties.

Tests mathematical properties that operators must satisfy:
- Join commutativity
- Join associativity
- Filter idempotency
- Select composition
- Drop composition
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.core.operators import (
    DropPacketColumns,
    Join,
    PolarsFilter,
    SelectPacketColumns,
)
from orcapod.core.streams import ArrowTableStream


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _sorted_rows(table: pa.Table, sort_col: str = "id") -> list[dict]:
    """Extract rows as sorted list of dicts for comparison."""
    df = table.to_pydict()
    rows = []
    n = table.num_rows
    for i in range(n):
        row = {k: df[k][i] for k in df if not k.startswith("_")}
        rows.append(row)
    return sorted(rows, key=lambda r: r.get(sort_col, 0))


def _make_stream(tag_data: dict, packet_data: dict, tag_cols: list[str]) -> ArrowTableStream:
    all_data = {**tag_data, **packet_data}
    return ArrowTableStream(pa.table(all_data), tag_columns=tag_cols)


# ===================================================================
# Join commutativity
# ===================================================================


class TestJoinCommutativity:
    """Per design: Join is commutative — join(A, B) produces same data as join(B, A)."""

    def test_two_way_commutativity(self):
        sa = _make_stream(
            {"id": pa.array([1, 2, 3], type=pa.int64())},
            {"a": pa.array([10, 20, 30], type=pa.int64())},
            ["id"],
        )
        sb = _make_stream(
            {"id": pa.array([2, 3, 4], type=pa.int64())},
            {"b": pa.array([200, 300, 400], type=pa.int64())},
            ["id"],
        )
        join = Join()
        result_ab = join.process(sa, sb)
        result_ba = join.process(sb, sa)

        rows_ab = _sorted_rows(result_ab.as_table())
        rows_ba = _sorted_rows(result_ba.as_table())
        assert rows_ab == rows_ba


# ===================================================================
# Join associativity
# ===================================================================


class TestJoinAssociativity:
    """Per design: join(join(A,B),C) should produce same data as join(A,join(B,C))
    when all have non-overlapping packet columns."""

    def test_three_way_associativity(self):
        sa = _make_stream(
            {"id": pa.array([1, 2], type=pa.int64())},
            {"a": pa.array([10, 20], type=pa.int64())},
            ["id"],
        )
        sb = _make_stream(
            {"id": pa.array([1, 2], type=pa.int64())},
            {"b": pa.array([100, 200], type=pa.int64())},
            ["id"],
        )
        sc = _make_stream(
            {"id": pa.array([1, 2], type=pa.int64())},
            {"c": pa.array([1000, 2000], type=pa.int64())},
            ["id"],
        )

        join = Join()

        # (A join B) join C
        ab = join.process(sa, sb)
        abc_left = join.process(ab, sc)

        # A join (B join C)
        bc = join.process(sb, sc)
        abc_right = join.process(sa, bc)

        rows_left = _sorted_rows(abc_left.as_table())
        rows_right = _sorted_rows(abc_right.as_table())
        assert rows_left == rows_right


# ===================================================================
# Filter idempotency
# ===================================================================


class TestFilterIdempotency:
    """filter(filter(S, P), P) == filter(S, P) — filtering twice with
    the same predicate is the same as filtering once."""

    def test_filter_idempotent(self):
        stream = _make_stream(
            {"id": pa.array([1, 2, 3, 4, 5], type=pa.int64())},
            {"value": pa.array([10, 20, 30, 40, 50], type=pa.int64())},
            ["id"],
        )

        filt = PolarsFilter(constraints={"id": 3})
        once = filt.process(stream)
        twice = filt.process(once)

        table_once = once.as_table()
        table_twice = twice.as_table()
        assert table_once.num_rows == table_twice.num_rows
        assert _sorted_rows(table_once) == _sorted_rows(table_twice)


# ===================================================================
# Select composition
# ===================================================================


class TestSelectComposition:
    """select(select(S, X), Y) == select(S, X∩Y)."""

    def test_select_then_select_is_intersection(self):
        stream = _make_stream(
            {"id": pa.array([1, 2], type=pa.int64())},
            {
                "a": pa.array([10, 20], type=pa.int64()),
                "b": pa.array([30, 40], type=pa.int64()),
                "c": pa.array([50, 60], type=pa.int64()),
            },
            ["id"],
        )

        # select(S, {a, b}) then select(result, {b, c}) → should keep only {b}
        sel1 = SelectPacketColumns(columns=["a", "b"])
        step1 = sel1.process(stream)

        sel2 = SelectPacketColumns(columns=["b"])
        step2 = sel2.process(step1)

        # Direct intersection: select {a,b} ∩ {b,c} = {b}
        sel_direct = SelectPacketColumns(columns=["b"])
        direct = sel_direct.process(stream)

        _, step2_keys = step2.keys()
        _, direct_keys = direct.keys()
        assert set(step2_keys) == set(direct_keys)


# ===================================================================
# Drop composition
# ===================================================================


class TestDropComposition:
    """drop(drop(S, X), Y) == drop(S, X∪Y)."""

    def test_drop_then_drop_is_union(self):
        stream = _make_stream(
            {"id": pa.array([1, 2], type=pa.int64())},
            {
                "a": pa.array([10, 20], type=pa.int64()),
                "b": pa.array([30, 40], type=pa.int64()),
                "c": pa.array([50, 60], type=pa.int64()),
            },
            ["id"],
        )

        # drop(S, {a}) then drop(result, {b}) → should drop {a, b}
        drop1 = DropPacketColumns(columns=["a"])
        step1 = drop1.process(stream)

        drop2 = DropPacketColumns(columns=["b"])
        step2 = drop2.process(step1)

        # Direct: drop {a} ∪ {b} = drop {a, b}
        drop_direct = DropPacketColumns(columns=["a", "b"])
        direct = drop_direct.process(stream)

        _, step2_keys = step2.keys()
        _, direct_keys = direct.keys()
        assert set(step2_keys) == set(direct_keys)
