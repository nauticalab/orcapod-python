"""Joins over a shared tag key that differs only in nullability (ITL-829, second case).

Developer decision, November 13, 2026: ``Join``, ``MergeJoin`` and ``SemiJoin`` accept a
shared tag key that is ``T`` in one input and ``T | None`` in another, because a null key
matches nothing. ``Join`` and ``MergeJoin`` give the key ``T`` when any input has it
non-nullable, and ``T | None`` only when every input has it nullable. ``SemiJoin``'s
output is the left input's schema as it is. Data columns are untouched: a data column
that differs in nullability behaves as before.
"""

from __future__ import annotations

from collections.abc import Callable

import pyarrow as pa
import pytest

from orcapod.core.operators import Join, MergeJoin, SemiJoin
from orcapod.core.operators.static_output_pod import StaticOutputOperatorPod
from orcapod.core.sources import ArrowTableSource
from orcapod.errors import InputValidationError, SemiJoinIgnoredColumnWarning
from orcapod.protocols.core_protocols import StreamProtocol
from tests.test_core.operators.test_async_sync_equivalence import (
    _source,  # pyright: ignore[reportPrivateUsage]  # shared test helper
    assert_equivalent,
    assert_same_provenance,
    async_rows,
    sync_rows,
)

SYSTEM_TAGS = {"system_tags": True}


def keyed(
    source_id: str,
    data: str,
    nullable: tuple[str, ...] = (),
    b: tuple[int | None, ...] = (1, 5, 1, 2, 2),
) -> ArrowTableSource:
    """Tags ``a`` and ``b`` (int64) and one int64 data column named ``data``.

    A tag listed in ``nullable`` is declared nullable; ``b`` may hold a null.
    """
    return _source(
        {
            "a": pa.array([1, 1, 2, 2, 3], pa.int64()),
            "b": pa.array(list(b), pa.int64()),
            data: pa.array([10, 20, 30, 40, 50], pa.int64()),
        },
        tags=("a", "b"),
        source_id=source_id,
        nullable=nullable,
    )


def with_null(source_id: str, data: str, nullable: tuple[str, ...]) -> ArrowTableSource:
    """Like ``keyed``, with a null in ``b`` when ``b`` is nullable."""
    b = (1, None, 1, 2, 2) if "b" in nullable else (1, 5, 1, 2, 2)
    return keyed(source_id, data, nullable, b)


def tag_types(stream: StreamProtocol) -> dict[str, object]:
    """The stream's predicted tag types."""
    return dict(stream.output_schema()[0])


def arrow_nullable(stream: StreamProtocol, name: str) -> bool:
    """Whether the computed table's field ``name`` is nullable."""
    return stream.as_table().schema.field(name).nullable


# ── Join and MergeJoin: T if any input has T ───────────────────────────────

JOINS: list[Callable[[], StaticOutputOperatorPod]] = [Join, MergeJoin]
JOIN_IDS = ["join", "merge_join"]


@pytest.mark.parametrize("make_op", JOINS, ids=JOIN_IDS)
@pytest.mark.parametrize(
    ("left_null", "right_null", "expected_b"),
    [
        pytest.param(("b",), (), int, id="left_nullable"),
        pytest.param((), ("b",), int, id="right_nullable"),
        pytest.param(("b",), ("b",), int | None, id="both_nullable"),
        pytest.param((), (), int, id="neither_nullable"),
    ],
)
def test_join_key_is_nullable_only_when_every_input_has_it_nullable(
    make_op: Callable[[], StaticOutputOperatorPod],
    left_null: tuple[str, ...],
    right_null: tuple[str, ...],
    expected_b: object,
) -> None:
    """Prediction and computed Arrow field agree, in both operand orders."""
    left, right = with_null("l", "x", left_null), with_null("r", "z", right_null)
    for result in (make_op()(left, right), make_op()(right, left)):
        assert tag_types(result) == {"a": int, "b": expected_b}
        assert arrow_nullable(result, "b") == (expected_b is not int)
        assert not arrow_nullable(result, "a")
        # A null key matches nothing: the row with b null finds no partner.
        matched = [10, 30, 40, 50] if left_null or right_null else [10, 20, 30, 40, 50]
        assert result.as_table().sort_by("x").column("x").to_pylist() == matched


@pytest.mark.parametrize("make_op", JOINS, ids=JOIN_IDS)
@pytest.mark.parametrize(
    ("left_null", "right_null"),
    [
        pytest.param(("b",), (), id="left_nullable"),
        pytest.param((), ("b",), id="right_nullable"),
        pytest.param(("a", "b"), ("b",), id="composite"),
    ],
)
def test_join_prediction_matches_computed_schema(
    make_op: Callable[[], StaticOutputOperatorPod],
    left_null: tuple[str, ...],
    right_null: tuple[str, ...],
) -> None:
    """``output_schema()`` without computing equals the computed stream's schema."""
    left, right = with_null("l", "x", left_null), with_null("r", "z", right_null)
    op = make_op()
    predicted = op(left, right).output_schema(columns=SYSTEM_TAGS)
    # Schema-prediction check: compared against the computed stream (CLAUDE.md).
    computed = op.static_process(left, right).output_schema(columns=SYSTEM_TAGS)
    assert predicted == computed


@pytest.mark.parametrize(
    ("nullables", "expected_b"),
    [
        pytest.param((("b",), ("b",), ()), int, id="one_non_nullable"),
        pytest.param(((), ("b",), ("b",)), int, id="first_non_nullable"),
        pytest.param((("b",), ("b",), ("b",)), int | None, id="all_nullable"),
    ],
)
def test_nary_join_key_is_nullable_only_when_every_input_has_it_nullable(
    nullables: tuple[tuple[str, ...], ...], expected_b: object
) -> None:
    """Across three inputs the key is ``T`` as soon as one input has it ``T``."""
    sources = [
        with_null(f"s{i}", f"d{i}", nullable) for i, nullable in enumerate(nullables)
    ]
    result = Join()(*sources)
    assert tag_types(result)["b"] == expected_b
    assert arrow_nullable(result, "b") == (expected_b is not int)
    # Schema-prediction check: compared against the computed stream (CLAUDE.md).
    computed = Join().static_process(*sources).output_schema(columns=SYSTEM_TAGS)
    assert result.output_schema(columns=SYSTEM_TAGS) == computed
    expected = sync_rows(Join(), *sources)
    assert expected
    assert_equivalent(expected, async_rows(Join(), *sources))


# ── SemiJoin: the left input's schema as it is ─────────────────────────────


@pytest.mark.parametrize(
    ("left_null", "right_null"),
    [
        pytest.param(("b",), (), id="left_nullable"),
        pytest.param((), ("b",), id="right_nullable"),
        pytest.param(("b",), ("b",), id="both_nullable"),
        pytest.param(("b",), ("a",), id="composite_crossed"),
    ],
)
def test_semijoin_keeps_the_left_schema(
    left_null: tuple[str, ...], right_null: tuple[str, ...]
) -> None:
    """The result has the left input's schema exactly; a null key matches nothing."""
    left, right = with_null("l", "x", left_null), with_null("r", "z", right_null)
    result = SemiJoin()(left, right)
    assert result.output_schema() == left.output_schema()
    assert result.as_table().schema == left.as_table().schema
    assert result.as_table().column("x").to_pylist() == [10, 30, 40, 50]
    # Schema-prediction check: compared against the computed stream (CLAUDE.md).
    computed = SemiJoin().static_process(left, right).output_schema(columns=SYSTEM_TAGS)
    assert result.output_schema(columns=SYSTEM_TAGS) == computed


# ── every operator: async equals sync ──────────────────────────────────────

OPERATORS: list[Callable[[], StaticOutputOperatorPod]] = [Join, MergeJoin, SemiJoin]
OPERATOR_IDS = ["join", "merge_join", "semijoin"]


@pytest.mark.parametrize("make_op", OPERATORS, ids=OPERATOR_IDS)
@pytest.mark.parametrize(
    ("left_null", "right_null"),
    [
        pytest.param(("b",), (), id="left_nullable"),
        pytest.param((), ("b",), id="right_nullable"),
        pytest.param(("b",), ("a",), id="composite_crossed"),
    ],
)
def test_async_rows_equal_sync_rows(
    make_op: Callable[[], StaticOutputOperatorPod],
    left_null: tuple[str, ...],
    right_null: tuple[str, ...],
) -> None:
    """The async path yields the sync rows: types, nullability, tags, hashes, sources."""
    left, right = with_null("l", "x", left_null), with_null("r", "z", right_null)
    expected = sync_rows(make_op(), left, right)
    actual = async_rows(make_op(), left, right)
    assert expected
    assert_equivalent(expected, actual)
    assert_same_provenance(expected, actual)


# ── what does not change ───────────────────────────────────────────────────


@pytest.mark.parametrize("make_op", OPERATORS, ids=OPERATOR_IDS)
def test_other_key_type_difference_still_raises(
    make_op: Callable[[], StaticOutputOperatorPod],
) -> None:
    """Only ``None`` is reconciled: ``int`` against ``str | None`` still raises."""
    left = _source(
        {"a": pa.array([1, 2], pa.int64()), "x": pa.array([1, 2], pa.int64())},
        tags=("a",),
        source_id="l",
    )
    right = _source(
        {
            "a": pa.array(["1", None], pa.large_string()),
            "z": pa.array([1, 2], pa.int64()),
        },
        tags=("a",),
        source_id="r",
        nullable=("a",),
    )
    with pytest.raises(InputValidationError, match="not compatible|Type conflict"):
        _ = make_op()(left, right)


def _data_pair() -> tuple[ArrowTableSource, ArrowTableSource]:
    """Two inputs sharing tag ``a`` and data column ``y``: ``int`` left, ``int | None`` right."""
    left = _source(
        {"a": pa.array([1, 2], pa.int64()), "y": pa.array([1, 2], pa.int64())},
        tags=("a",),
        source_id="l",
    )
    right = _source(
        {"a": pa.array([1, 2], pa.int64()), "y": pa.array([1, None], pa.int64())},
        tags=("a",),
        source_id="r",
        nullable=("y",),
    )
    return left, right


@pytest.mark.parametrize(
    ("make_op", "message"),
    [
        pytest.param(Join, "Type conflict for key .y.", id="join"),
        pytest.param(MergeJoin, "incompatible types", id="merge_join"),
    ],
)
def test_data_column_nullability_difference_behaves_as_before(
    make_op: Callable[[], StaticOutputOperatorPod], message: str
) -> None:
    """A data column is never reconciled: ``Join`` and ``MergeJoin`` raise as they did."""
    left, right = _data_pair()
    with pytest.raises(InputValidationError, match=message):
        _ = make_op()(left, right)


def test_semijoin_ignores_a_data_column_that_differs_in_nullability() -> None:
    """``SemiJoin`` keys on shared tags only (ITL-1088), so a shared data column is
    neither a key nor checked: the result is every left row whose tag matches."""
    left, right = _data_pair()
    with pytest.warns(SemiJoinIgnoredColumnWarning, match="'y'"):
        result = SemiJoin()(left, right)
    assert result.output_schema() == left.output_schema()
    assert result.as_table().column("y").to_pylist() == [1, 2]
