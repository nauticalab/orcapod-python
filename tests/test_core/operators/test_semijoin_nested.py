"""SemiJoin over nested and logical columns, and over string vs large_string keys.

ITL-847: the sync path used pyarrow's Acero hash join, which rejects any nested
non-key column ("Data type list<item: int8> is not supported in join non-key
field"), so a list, large_list, struct or struct-backed logical column on either
input failed. ITL-829 (first case): Acero also rejected a key that is ``string``
on one side and ``large_string`` on the other ("Incompatible data types for
corresponding join field keys"), e.g. a ``CSVSource`` against a ``DictSource``; since
ITL-1122 every stream presents the large variant, so that meeting no longer occurs.

The result is the left input's rows that match a right row, in left order, with
the left input's Arrow schema in its large variants (ITL-1122: ``string`` is
``large_string``, ``list`` is ``large_list``; top-level and nested nullability and
logical types exactly as they were). The sync and async paths yield the same rows.
"""

from __future__ import annotations

import asyncio
import dataclasses
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa
import pytest

from orcapod.channels import Channel
from orcapod.core.operators import SemiJoin
from orcapod.core.sources import ArrowTableSource, DictSource
from orcapod.core.streams import ArrowTableStream
from orcapod.errors import InputValidationError
from orcapod.protocols.core_protocols import StreamProtocol
from orcapod.utils import arrow_utils
from tests.test_core.operators.test_async_sync_equivalence import (
    STRUCT,
    Column,
    Row,
    _logical_column,  # pyright: ignore[reportPrivateUsage]  # shared test helper
    _source,  # pyright: ignore[reportPrivateUsage]  # shared test helper
    assert_equivalent,
    assert_same_provenance,
    sync_rows,
)

ALL_COLUMNS = {"source": True, "system_tags": True}


@dataclasses.dataclass
class Point:
    """A dataclass column: a logical type whose storage is a struct."""

    x: int
    label: str


def nested_column(kind: str, n_rows: int) -> Column:
    """A non-nullable column of ``kind`` with ``n_rows`` distinct values."""
    rows = range(n_rows)
    if kind == "list":
        return pa.array([[i, i + 1] for i in rows], pa.list_(pa.int8()))
    if kind == "large_list":
        return pa.array([[i * 0.5] for i in rows], pa.large_list(pa.float32()))
    if kind == "struct":
        return pa.array([{"x": i, "y": f"s{i}"} for i in rows], STRUCT)
    if kind == "dataclass":
        return _logical_column([Point(i, f"p{i}") for i in rows], Point)
    if kind == "path":
        return _logical_column([Path(f"/data/{i}") for i in rows], Path)
    if kind == "list_of_path":
        return _logical_column(
            [[Path(f"/d/{i}"), Path("/e")] for i in rows], list[Path]
        )
    if kind == "numpy":
        return _logical_column(
            [np.arange(i + 1, dtype=np.int8).reshape(1, -1) for i in rows], np.ndarray
        )
    raise ValueError(kind)


NESTED_KINDS = [
    "list",
    "large_list",
    "struct",
    "dataclass",
    "path",
    "list_of_path",
    "numpy",
]


def left_with(kind: str) -> ArrowTableSource:
    """Tag ``k: int16`` in the order 4, 1, 3, 2; data ``v: int8`` and ``n`` of ``kind``."""
    return _source(
        {
            "k": pa.array([4, 1, 3, 2], pa.int16()),
            "v": pa.array([40, 10, 30, 20], pa.int8()),
            "n": nested_column(kind, 4),
        },
        tags=("k",),
        source_id=f"left_{kind}",
    )


def right_with(kind: str) -> ArrowTableSource:
    """Tag ``k: int16`` in the order 2, 9, 4; data ``w: int8`` and ``m`` of ``kind``."""
    return _source(
        {
            "k": pa.array([2, 9, 4], pa.int16()),
            "w": pa.array([1, 2, 3], pa.int8()),
            "m": nested_column(kind, 3),
        },
        tags=("k",),
        source_id=f"right_{kind}",
    )


def scalar_left() -> ArrowTableSource:
    """Tag ``k: int16`` in the order 4, 1, 3, 2 and a scalar ``v: int8``."""
    return _source(
        {
            "k": pa.array([4, 1, 3, 2], pa.int16()),
            "v": pa.array([4, 1, 3, 2], pa.int8()),
        },
        tags=("k",),
        source_id="scalar_left",
    )


def scalar_right() -> ArrowTableSource:
    """Tag ``k: int16`` in the order 2, 9, 4 and a scalar ``w: int8``."""
    return _source(
        {"k": pa.array([2, 9, 4], pa.int16()), "w": pa.array([1, 2, 3], pa.int8())},
        tags=("k",),
        source_id="scalar_right",
    )


def expected_rows(
    left: StreamProtocol, keep: Callable[[dict[str, Any]], bool]
) -> pa.Table:
    """The left input's full table (source info, system tags) filtered by ``keep``."""
    table = left.as_table(columns=ALL_COLUMNS)
    mask = pa.array([keep(row) for row in table.to_pylist()], pa.bool_())
    return table.filter(mask)


def assert_semijoin_result(
    left: StreamProtocol, right: StreamProtocol, expected: pa.Table
) -> None:
    """The sync result equals ``expected`` in its large variants (ITL-1122), and async
    yields the same rows."""
    expected = arrow_utils.normalize_table_to_large_types(expected, keep_null=True)
    result = SemiJoin()(left, right)
    table = result.as_table(columns=ALL_COLUMNS)
    assert table.schema == expected.schema
    assert table.equals(expected)
    assert result.keys() == left.keys()
    assert result.output_schema(all_info=True) == left.output_schema(all_info=True)
    expected_sync = sync_rows(SemiJoin(), left, right)
    actual_async = async_semijoin_rows(left, right)
    assert len(actual_async) == expected.num_rows
    assert_equivalent(expected_sync, actual_async)
    assert_same_provenance(expected_sync, actual_async)
    # The async path streams left rows, so it keeps left order too.
    assert [t.content_hash() for t, _ in actual_async] == [
        t.content_hash() for t, _ in expected_sync
    ]


async def _run_async(sources: Sequence[StreamProtocol]) -> list[Row]:
    """Run SemiJoin's async path over channels fed from ``sources``."""
    op = SemiJoin()
    # A pipeline validates its inputs before async execution; SemiJoin's async
    # path reads the common keys that validation records (empty-right case).
    op.validate_inputs(*sources)
    channels: list[Channel[Row]] = [Channel(buffer_size=64) for _ in sources]
    output: Channel[Row] = Channel(buffer_size=64)
    for stream, channel in zip(sources, channels):
        for row in stream.iter_data():
            await channel.writer.send(row)
        await channel.writer.close()
    await op.async_execute(
        [channel.reader for channel in channels],
        output.writer,
        input_pipeline_hashes=[s.pipeline_hash() for s in sources],
    )
    return await output.reader.collect()


def async_semijoin_rows(left: StreamProtocol, right: StreamProtocol) -> list[Row]:
    """The rows of SemiJoin's async path over channels fed from ``left`` and ``right``."""
    return asyncio.run(_run_async((left, right)))


def zero_rows(stream: StreamProtocol) -> ArrowTableStream:
    """``stream`` with no rows, keeping its schema, source info and system tags."""
    table = stream.as_table(columns=ALL_COLUMNS)
    return ArrowTableStream(table.slice(0, 0), tag_columns=stream.keys()[0])


# ── nested and logical non-key columns ──────────────────────────────────────


@pytest.mark.parametrize("kind", NESTED_KINDS)
def test_nested_column_on_kept_side(kind: str) -> None:
    """A nested or logical column on the left passes through, in left order."""
    left = left_with(kind)
    expected = expected_rows(left, lambda row: row["k"] in (2, 4))
    assert expected.column("k").to_pylist() == [4, 2]  # left order
    assert_semijoin_result(left, scalar_right(), expected)


@pytest.mark.parametrize("kind", NESTED_KINDS)
def test_nested_column_on_filtering_side(kind: str) -> None:
    """A nested or logical column on the right does not block the join."""
    left = scalar_left()
    assert_semijoin_result(
        left, right_with(kind), expected_rows(left, lambda row: row["k"] in (2, 4))
    )


@pytest.mark.parametrize("kind", NESTED_KINDS)
def test_nested_columns_on_both_sides(kind: str) -> None:
    """Nested or logical columns on both inputs."""
    left = left_with(kind)
    assert_semijoin_result(
        left, right_with(kind), expected_rows(left, lambda row: row["k"] in (2, 4))
    )


@pytest.mark.parametrize("kind", NESTED_KINDS)
def test_zero_row_left(kind: str) -> None:
    """A zero-row left input gives a zero-row result with the left schema."""
    left = zero_rows(left_with(kind))
    assert_semijoin_result(left, right_with(kind), expected_rows(left, lambda _: True))


@pytest.mark.parametrize("kind", NESTED_KINDS)
def test_zero_row_right(kind: str) -> None:
    """A zero-row right input gives a zero-row result with the left schema."""
    left = left_with(kind)
    assert_semijoin_result(
        left, zero_rows(right_with(kind)), expected_rows(left, lambda _: False)
    )


def test_nested_non_nullable_fields_keep_their_flag() -> None:
    """Non-nullable list items and struct children keep their flag on the kept side
    (a Polars round-trip makes every nested field nullable)."""
    items = pa.list_(pa.field("item", pa.int8(), nullable=False))
    strict_struct = pa.struct([pa.field("x", pa.int8(), nullable=False)])
    left = _source(
        {
            "k": pa.array([1, 2], pa.int16()),
            "l": pa.array([[1], [2, 3]], items),
            "s": pa.array([None, "b"], pa.string()),
            "st": pa.array([{"x": 1}, {"x": 2}], strict_struct),
        },
        tags=("k",),
        source_id="nn",
        nullable=("s",),
    )
    result = SemiJoin()(left, scalar_right()).as_table(columns=ALL_COLUMNS)
    # The large variants (ITL-1122); the item keeps its non-null flag.
    assert result.schema.field("l").type == pa.large_list(
        pa.field("item", pa.int8(), nullable=False)
    )
    assert result.schema.field("s").type == pa.large_string()
    assert result.schema.field("st").type == strict_struct
    assert result.column("k").to_pylist() == [2]


# ── nested and logical keys ─────────────────────────────────────────────────


def keyed(name: str, key_column: Column, tag: Sequence[int]) -> ArrowTableSource:
    """A source with tags ``t_<name>`` and the shared nullable ``key``, data ``d_<name>``.

    ``key`` is the only tag both inputs share, so it is the semi join's match key
    (ITL-1088: the keys are the shared tag columns).
    """
    return _source(
        {
            f"t_{name}": pa.array(tag, pa.int16()),
            "key": key_column,
            f"d_{name}": pa.array(tag, pa.int16()),
        },
        tags=(f"t_{name}", "key"),
        source_id=name,
        nullable=("key",),
    )


def test_struct_key() -> None:
    """Struct keys match by value; a null inside the struct compares equal, a null
    struct never matches."""
    left = keyed(
        "l",
        pa.array(
            [{"x": 1, "y": None}, {"x": 2, "y": "b"}, None, {"x": 3, "y": "c"}], STRUCT
        ),
        [0, 1, 2, 3],
    )
    right = keyed(
        "r",
        pa.array([{"x": 3, "y": "c"}, None, {"x": 1, "y": None}], STRUCT),
        [7, 8, 9],
    )
    expected = expected_rows(left, lambda row: row["t_l"] in (0, 3))
    assert_semijoin_result(left, right, expected)


def test_struct_key_string_vs_large_string_child() -> None:
    """A struct key whose string child is ``string`` on one side and
    ``large_string`` on the other matches; the left keeps its own child type."""
    large = pa.struct([("x", pa.int8()), ("y", pa.large_string())])
    left = keyed(
        "l", pa.array([{"x": 1, "y": "a"}, {"x": 2, "y": "b"}], STRUCT), [0, 1]
    )
    right = keyed("r", pa.array([{"x": 2, "y": "b"}], large), [5])
    assert_semijoin_result(
        left, right, expected_rows(left, lambda row: row["t_l"] == 1)
    )


def test_list_key() -> None:
    """List keys match element-wise, including null items; a null list never matches."""
    list_type = pa.list_(pa.int8())
    left = keyed("l", pa.array([[1, None], [2], None, []], list_type), [0, 1, 2, 3])
    right = keyed(
        "r", pa.array([[], None, [1, None]], pa.large_list(pa.int8())), [5, 6, 7]
    )
    assert_semijoin_result(
        left, right, expected_rows(left, lambda row: row["t_l"] in (0, 3))
    )


def test_path_key() -> None:
    """A ``Path`` (logical, string storage) key matches by value."""
    left = keyed(
        "l", _logical_column([Path("/a"), Path("/b"), Path("/c")], Path), [0, 1, 2]
    )
    right = keyed("r", _logical_column([Path("/c"), Path("/a")], Path), [5, 6])
    assert_semijoin_result(
        left, right, expected_rows(left, lambda row: row["t_l"] in (0, 2))
    )


def test_null_scalar_key_never_matches() -> None:
    """A null scalar key matches nothing, not even a null, on both paths."""
    left = keyed("l", pa.array([1, None, 2], pa.int16()), [0, 1, 2])
    right = keyed("r", pa.array([None, 2], pa.int16()), [5, 6])
    assert_semijoin_result(
        left, right, expected_rows(left, lambda row: row["t_l"] == 2)
    )


# ── key types ───────────────────────────────────────────────────────────────


def test_string_vs_large_string_key_user_vs_dict() -> None:
    """ITL-829 (first case): a user's ``string`` key against a DictSource's key, in
    both directions.

    The ``string`` / ``large_string`` meeting no longer reaches an operator: every
    stream presents the large variant (ITL-1122), so the user's ``string`` key is
    ``large_string`` at its source already.
    """
    user_source = _source(
        {
            "id": pa.array(["c", "a", "b"], pa.string()),
            "x": pa.array([3, 1, 2], pa.int64()),
        },
        tags=("id",),
        source_id="user",
    )
    dict_source = DictSource(
        data=[{"id": "a", "y": 1}, {"id": "c", "y": 3}],
        tag_columns=["id"],
        tag_only_columns=True,
    )
    assert user_source.as_table().schema.field("id").type == pa.large_string()
    assert dict_source.as_table().schema.field("id").type == pa.large_string()

    assert_semijoin_result(
        user_source,
        dict_source,
        expected_rows(user_source, lambda row: row["id"] in ("a", "c")),
    )
    assert_semijoin_result(
        dict_source, user_source, expected_rows(dict_source, lambda _: True)
    )
    # The stream convenience method takes the same path.
    assert user_source.semi_join(dict_source).as_table().column("id").to_pylist() == [
        "c",
        "a",
    ]


def mismatched_key_sources() -> tuple[ArrowTableSource, ArrowTableSource]:
    """Two sources whose shared tag ``k`` is ``int8`` on one side and ``int16`` on the other."""
    left = _source(
        {"k": pa.array([1, 2], pa.int8()), "v": pa.array([1, 2], pa.int8())},
        tags=("k",),
        source_id="k8",
    )
    right = _source(
        {"k": pa.array([2], pa.int16()), "w": pa.array([1], pa.int8())},
        tags=("k",),
        source_id="k16",
    )
    return left, right


MISMATCH = r"SemiJoin inputs disagree on the Arrow type of column 'k': int8 .* int16"


def test_key_type_mismatch_raises_sync() -> None:
    """Keys whose types differ beyond large-normalization are an error, never unified."""
    left, right = mismatched_key_sources()
    with pytest.raises(InputValidationError, match=MISMATCH):
        _ = SemiJoin()(left, right).as_table()


def test_key_type_mismatch_raises_async() -> None:
    """The async path raises the same error as the sync path."""
    left, right = mismatched_key_sources()
    with pytest.raises(InputValidationError, match=MISMATCH):
        _ = async_semijoin_rows(left, right)


# ── schema prediction ───────────────────────────────────────────────────────


@pytest.mark.parametrize("kind", NESTED_KINDS)
def test_predicted_schema_equals_materialized(kind: str) -> None:
    """The predicted output schema equals the materialized one."""
    left, right = left_with(kind), right_with(kind)
    op = SemiJoin()
    for all_info in (False, True):
        # Schema predictions are checked against the static result (CLAUDE.md).
        materialized = op.binary_static_process(left, right).output_schema(
            all_info=all_info
        )
        assert op.output_schema(left, right, all_info=all_info) == materialized
