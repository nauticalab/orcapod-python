"""The row order an ``ArrowTableStream`` hashes for its identity never changes (ITL-1053).

``identity_structure`` of a stream with no producer hashes its table, so the order of its
rows is a hash input. ITL-1053 gave ``as_table``, ``head`` and the preview a new
deterministic sort; the identity keeps the ordering code it had before. This test holds a
literal copy of that code and compares it with the stream's identity table over a battery
of shapes: tag and system tag columns, system tags given in either order, nulls, tied
tags and list tags.
"""

from __future__ import annotations

import itertools
import random
from typing import Any

import pyarrow as pa
import pytest

from orcapod.core.streams import ArrowTableStream
from orcapod.types import ColumnConfig


def main_ordering(stream: ArrowTableStream, table: pa.Table) -> pa.Table:
    """The sort block of ``ArrowTableStream._assemble_table`` on origin/main.

    Copied literally from origin/main at commit 89160c47abf149c8160496de2cdfebe7d651e448
    (``src/orcapod/core/streams/arrow_table_stream.py``, last changed in f51ce67c), with
    ``columns_config.system_tags`` set to ``True``, which is what ``identity_structure``
    asked for, and ``columns_config.sort_by_tags`` true. Do not edit it.
    """
    # --- literal copy below; ``self`` is ``stream``, ``columns_config.system_tags`` is True
    self = stream
    try:
        target_tags = self._all_tag_columns  # pyright: ignore[reportPrivateUsage]
        return table.sort_by([(column, "ascending") for column in target_tags])
    except pa.ArrowTypeError:
        # If sorting fails, fall back to unsorted table
        return table


def shapes() -> list[tuple[Any, ...]]:
    """Every shape of the battery: tag columns, system tag order, nulls, ties, list tags."""
    return list(
        itertools.product(
            [(), ("k",), ("k", "j")],
            [("s1", "s2"), ("s2", "s1"), ("s1",), ()],
            [False, True],
            [False, True],
            [False, True],
        )
    )


def build(shape: tuple[Any, ...]) -> ArrowTableStream:
    """A six-row stream of the given shape; random but seeded values."""
    tags, system, nulls, ties, list_tag = shape
    rng = random.Random(7)
    n = 6
    k: list[Any] = [rng.choice([0, 1, 2]) if ties else i for i in range(n)]
    j: list[Any] = [rng.choice(["a", "b"]) for _ in range(n)]
    if nulls:
        k[1] = None
        j[3] = None
    columns: dict[str, pa.Array[Any]] = {
        "k": pa.array(
            [None if v is None else [v] for v in k], pa.large_list(pa.int64())
        )
        if list_tag
        else pa.array(k, pa.int64()),
        "j": pa.array(j, pa.string()),
        "s1": pa.array([rng.choice(["p", "q", "r"]) for _ in range(n)], pa.string()),
        "s2": pa.array([rng.choice(["u", "v"]) for _ in range(n)], pa.string()),
        "x": pa.array(range(n), pa.int64()),
    }
    names = [c for c in columns if c in tags or c in system or c == "x"]
    table = pa.table([columns[c] for c in names], names=names)
    return ArrowTableStream(table, tag_columns=tags, system_tag_columns=system)


@pytest.mark.parametrize("shape", shapes())
def test_the_identity_table_has_the_row_order_main_gave_it(
    shape: tuple[Any, ...],
) -> None:
    """``identity_structure``'s table is main's ordering of the same columns, or both raise."""
    stream = build(shape)
    unsorted = stream.as_table(
        columns=ColumnConfig(meta=True, context=True, source=True, system_tags=True)
    )
    try:
        expected = main_ordering(stream, unsorted)
    except pa.ArrowInvalid:
        # Main raises when there is nothing to sort by: so does the identity.
        with pytest.raises(pa.ArrowInvalid):
            stream.identity_structure()
        return
    name, table, tag_columns = stream.identity_structure()
    assert name == "ArrowTableStream"
    assert table.equals(expected)
    assert tag_columns == tuple(c for c in shape[0])
