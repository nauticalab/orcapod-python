"""Regression tests for ITL-816.

``ArrowTableStream.iter_data`` must yield every row, with the tag paired to the
right data and the right source info, whatever the chunk layout of the tag
columns, the data columns and the source-info columns of the backing table.
"""

from __future__ import annotations

from typing import Any

import pyarrow as pa
import pytest

from orcapod.core.streams import ArrowTableStream
from orcapod.system_constants import constants
from orcapod.types import ColumnConfig

SOURCE_X = f"{constants.SOURCE_PREFIX}x"
SYSTEM_TAG = f"{constants.SYSTEM_TAG_PREFIX}source:abc123"
WITH_SYSTEM_TAGS = ColumnConfig(system_tags=True)


def _chunked(
    values: list[Any], sizes: list[int], arrow_type: pa.DataType
) -> pa.ChunkedArray[Any]:
    """Cut ``values`` into a ``ChunkedArray`` with chunks of the given sizes."""
    assert sum(sizes) == len(values)
    chunks: list[pa.Array[Any]] = []
    start = 0
    for size in sizes:
        chunks.append(pa.array(values[start : start + size], type=arrow_type))
        start += size
    return pa.chunked_array(chunks)


def _chunked_stream(
    tag_sizes: list[int],
    data_sizes: list[int],
    source_sizes: list[int] | None = None,
    with_tags: bool = True,
) -> ArrowTableStream:
    """Build a stream whose tag, data and source-info columns are chunked independently.

    Row ``i`` holds ``id=i``, ``grp="g{i}"``, ``x=i*10``, ``y="v{i}"`` and (when
    ``source_sizes`` is given) ``_source_x="src::{i}"``, so a mis-paired row is
    always detectable.
    """
    n = sum(data_sizes)
    columns: dict[str, pa.ChunkedArray[Any]] = {}
    if with_tags:
        columns["id"] = _chunked(list(range(n)), tag_sizes, pa.int64())
        columns["grp"] = _chunked(
            [f"g{i}" for i in range(n)], tag_sizes, pa.large_string()
        )
    columns["x"] = _chunked([i * 10 for i in range(n)], data_sizes, pa.int64())
    columns["y"] = _chunked([f"v{i}" for i in range(n)], data_sizes, pa.large_string())
    if source_sizes is not None:
        columns[SOURCE_X] = _chunked(
            [f"src::{i}" for i in range(n)], source_sizes, pa.large_string()
        )
    table = pa.Table.from_pydict(columns)
    return ArrowTableStream(table, tag_columns=["id", "grp"] if with_tags else [])


LAYOUTS = [
    pytest.param([6], [1, 1, 1, 1, 1, 1], id="tag-single-data-many"),
    pytest.param([2, 2, 2], [6], id="tag-many-data-single"),
    pytest.param([4, 2], [1, 5], id="unequal-boundaries-same-count"),
    pytest.param([3, 3], [2, 2, 2], id="fewer-tag-chunks"),
    pytest.param([1, 2, 3], [5, 1], id="fewer-data-chunks"),
    pytest.param([4, 2], [2, 2, 1, 1], id="reporter-shape"),
    pytest.param([0, 6], [3, 0, 3], id="empty-chunks"),
    # Result-cache hits return data whose columns carry zero-length chunks
    # (layouts like [1, 0, 0, ...]) while the tag side has one chunk per row.
    pytest.param(
        [1] * 6, [1, 0, 0, 1, 0, 1, 0, 0, 1, 1, 0, 1], id="zero-length-data-chunks"
    ),
    pytest.param(
        [1, 0, 0, 1, 0, 1, 0, 0, 1, 1, 0, 1], [1] * 6, id="zero-length-tag-chunks"
    ),
    pytest.param(
        [0, 1, 0, 2, 0, 3], [3, 0, 0, 2, 1, 0], id="zero-length-chunks-both-sides"
    ),
    pytest.param([2, 4], [2, 4], id="same-layout-control"),
]


@pytest.mark.parametrize("tag_sizes,data_sizes", LAYOUTS)
class TestIterDataChunkLayouts:
    """``iter_data`` over tag and data columns that are chunked differently."""

    def test_layout_is_actually_as_described(self, tag_sizes, data_sizes):
        """Guard the fixture: the stream's backing table really is chunked unevenly."""
        stream = _chunked_stream(tag_sizes, data_sizes)
        assert [len(c) for c in stream._table.column("id").chunks] == tag_sizes
        assert [len(c) for c in stream._table.column("x").chunks] == data_sizes

    def test_yields_every_row(self, tag_sizes, data_sizes):
        """``iter_data`` yields as many pairs as the table has rows."""
        stream = _chunked_stream(tag_sizes, data_sizes)
        n = sum(data_sizes)
        assert stream.as_table().num_rows == n
        assert len(list(stream.iter_data())) == stream.as_table().num_rows

    def test_tag_pairs_with_its_own_data(self, tag_sizes, data_sizes):
        """Rows come out in table order, each tag next to its own data."""
        stream = _chunked_stream(tag_sizes, data_sizes)
        pairs = list(stream.iter_data())
        assert [tag["id"] for tag, _ in pairs] == list(range(sum(data_sizes)))
        for row, (tag, data) in enumerate(pairs):
            assert tag["grp"] == f"g{row}"
            assert data["x"] == row * 10
            assert data["y"] == f"v{row}"

    def test_iteration_matches_as_table(self, tag_sizes, data_sizes):
        """The iterated pairs, flattened, equal the rows of ``as_table()``."""
        stream = _chunked_stream(tag_sizes, data_sizes)
        expected = stream.as_table().to_pylist()
        actual = [
            {**tag.as_dict(), **data.as_dict()} for tag, data in stream.iter_data()
        ]
        assert actual == expected

    def test_cached_second_iteration_is_identical(self, tag_sizes, data_sizes):
        """A second pass, served from the element cache, repeats the first."""
        stream = _chunked_stream(tag_sizes, data_sizes)
        first = [(t.as_dict(), d.as_dict()) for t, d in stream.iter_data()]
        second = [(t.as_dict(), d.as_dict()) for t, d in stream.iter_data()]
        assert len(first) == sum(data_sizes)
        assert first == second

    def test_content_hash_column_has_one_hash_per_row(self, tag_sizes, data_sizes):
        """``as_table(content_hash=True)`` appends one hash per iterated row."""
        stream = _chunked_stream(tag_sizes, data_sizes)
        table = stream.as_table(columns={"content_hash": True})
        assert table.num_rows == sum(data_sizes)


SOURCE_LAYOUTS = [
    pytest.param([6], [6], [2, 2, 2], id="source-many-rest-single"),
    pytest.param([2, 2, 2], [2, 2, 2], [6], id="source-single-rest-many"),
    pytest.param([4, 2], [1, 5], [3, 3], id="all-three-differ"),
    pytest.param([3, 3], [3, 3], [3, 3], id="same-layout-multi-chunk"),
]


@pytest.mark.parametrize("tag_sizes,data_sizes,source_sizes", SOURCE_LAYOUTS)
def test_source_info_pairs_with_its_own_row(tag_sizes, data_sizes, source_sizes):
    """Per-row source info follows the row, not the position inside a batch."""
    stream = _chunked_stream(tag_sizes, data_sizes, source_sizes)
    pairs = list(stream.iter_data())
    assert len(pairs) == sum(data_sizes)
    for row, (tag, data) in enumerate(pairs):
        assert tag["id"] == row
        assert data["x"] == row * 10
        assert data.source_info()["x"] == f"src::{row}"


@pytest.mark.parametrize("data_sizes", [[6], [1, 5], [2, 2, 2]])
def test_stream_without_tag_columns_yields_every_row(data_sizes):
    """With no tag column of any kind, every row pairs with an empty tag."""
    n = sum(data_sizes)
    table = pa.Table.from_pydict(
        {
            "x": _chunked([i * 10 for i in range(n)], data_sizes, pa.int64()),
            "y": _chunked(
                [f"v{i}" for i in range(n)], data_sizes[::-1], pa.large_string()
            ),
        }
    )
    stream = ArrowTableStream(table)
    pairs = list(stream.iter_data())
    assert len(pairs) == stream.as_table().num_rows == n
    assert [(d["x"], d["y"]) for _, d in pairs] == [(i * 10, f"v{i}") for i in range(n)]
    assert all(t.as_dict() == {} for t, _ in pairs)
    # Each row gets its own empty tag rather than one shared instance.
    assert len({id(t) for t, _ in pairs}) == n


def test_empty_stream_yields_nothing():
    """A zero-row stream iterates to nothing rather than raising."""
    table = pa.table(
        {
            "id": pa.array([], type=pa.int64()),
            "x": pa.array([], type=pa.int64()),
        }
    )
    stream = ArrowTableStream(table, tag_columns=["id"])
    assert list(stream.iter_data()) == []
    assert stream.as_table().num_rows == 0


def _system_tag_stream(
    system_tag_sizes: list[int],
    data_sizes: list[int],
    tag_sizes: list[int] | None = None,
) -> ArrowTableStream:
    """Build a stream with a system tag column chunked independently of the rest.

    Row ``i`` holds the system tag ``"rec::{i}"``, ``x=i*10``, ``y="v{i}"`` and,
    when ``tag_sizes`` is given, the ordinary tag ``id=i``. Without
    ``tag_sizes`` the stream has no ordinary tag column at all.
    """
    n = sum(data_sizes)
    columns: dict[str, pa.ChunkedArray[Any]] = {}
    if tag_sizes is not None:
        columns["id"] = _chunked(list(range(n)), tag_sizes, pa.int64())
    columns[SYSTEM_TAG] = _chunked(
        [f"rec::{i}" for i in range(n)], system_tag_sizes, pa.large_string()
    )
    columns["x"] = _chunked([i * 10 for i in range(n)], data_sizes, pa.int64())
    columns["y"] = _chunked([f"v{i}" for i in range(n)], data_sizes, pa.large_string())
    return ArrowTableStream(
        pa.Table.from_pydict(columns),
        tag_columns=["id"] if tag_sizes is not None else [],
    )


@pytest.mark.parametrize("system_tag_sizes,data_sizes", LAYOUTS)
def test_system_tags_survive_without_ordinary_tag_columns(system_tag_sizes, data_sizes):
    """A stream with system tags but no ordinary tags keeps each row's system tag.

    The tag of every pair is empty by default, yet still carries the system tag
    of its own row, whatever the chunk layout of the system tag column.
    """
    stream = _system_tag_stream(system_tag_sizes, data_sizes)
    n = sum(data_sizes)
    assert [len(c) for c in stream._table.column(SYSTEM_TAG).chunks] == system_tag_sizes
    assert stream.keys() == ((), ("x", "y"))

    pairs = list(stream.iter_data())
    assert len(pairs) == n
    for row, (tag, data) in enumerate(pairs):
        assert tag.as_dict() == {}
        assert tag.as_dict(columns=WITH_SYSTEM_TAGS) == {SYSTEM_TAG: f"rec::{row}"}
        assert tag.system_tags() == {SYSTEM_TAG: f"rec::{row}"}
        assert data["x"] == row * 10
        assert data["y"] == f"v{row}"

    table = stream.as_table(columns=WITH_SYSTEM_TAGS)
    assert table.column(SYSTEM_TAG).to_pylist() == [f"rec::{i}" for i in range(n)]


SYSTEM_TAG_LAYOUTS = [
    pytest.param([6], [2, 2, 2], [1, 5], id="tag-single-rest-differ"),
    pytest.param([4, 2], [6], [1, 1, 1, 1, 1, 1], id="system-tag-single"),
    pytest.param([2, 2, 2], [3, 3], [6], id="data-single"),
    pytest.param([4, 2], [1, 5], [3, 3], id="all-three-differ"),
    pytest.param([0, 6], [3, 0, 3], [1, 0, 0, 2, 0, 3], id="zero-length-chunks"),
    pytest.param(
        [1] * 6,
        [0, 1, 0, 2, 0, 3],
        [1, 0, 0, 1, 0, 1, 0, 0, 1, 1, 0, 1],
        id="zero-length-system-tag-and-data-chunks",
    ),
    pytest.param([2, 4], [2, 4], [2, 4], id="same-layout-control"),
]


@pytest.mark.parametrize("tag_sizes,system_tag_sizes,data_sizes", SYSTEM_TAG_LAYOUTS)
def test_system_tags_pair_with_ordinary_tags_and_data(
    tag_sizes, system_tag_sizes, data_sizes
):
    """Ordinary tag, system tag and data of one row stay together.

    The three column groups are chunked independently, so a system tag taken
    from the wrong row shows up as a mismatch against the ordinary tag ``id``.
    """
    stream = _system_tag_stream(system_tag_sizes, data_sizes, tag_sizes)
    n = sum(data_sizes)
    assert [len(c) for c in stream._table.column("id").chunks] == tag_sizes
    assert [len(c) for c in stream._table.column(SYSTEM_TAG).chunks] == system_tag_sizes

    pairs = list(stream.iter_data())
    assert [tag["id"] for tag, _ in pairs] == list(range(n))
    for row, (tag, data) in enumerate(pairs):
        assert tag.as_dict() == {"id": row}
        assert tag.as_dict(columns=WITH_SYSTEM_TAGS) == {
            "id": row,
            SYSTEM_TAG: f"rec::{row}",
        }
        assert data["x"] == row * 10
        assert data["y"] == f"v{row}"

    expected = stream.as_table(columns=WITH_SYSTEM_TAGS).to_pylist()
    actual = [
        {**tag.as_dict(columns=WITH_SYSTEM_TAGS), **data.as_dict()}
        for tag, data in pairs
    ]
    assert actual == expected


NESTED_SOURCE_TYPE = pa.large_list(pa.large_list(pa.large_string()))


@pytest.mark.parametrize("tag_sizes,data_sizes,source_sizes", SOURCE_LAYOUTS)
def test_nested_source_info_pairs_with_its_own_row(
    tag_sizes: list[int], data_sizes: list[int], source_sizes: list[int]
) -> None:
    """A list-of-lists source-info column (a second aggregation) follows its row.

    ``iter_data`` hands each row's source info to ``Data`` as Arrow columns, so
    the nested type is kept rather than re-inferred (ITL-849).
    """
    n = sum(data_sizes)
    tokens = [[[f"src::{i}::a"], [f"src::{i}::b", f"src::{i}::c"]] for i in range(n)]
    table = pa.Table.from_pydict(
        {
            "id": _chunked(list(range(n)), tag_sizes, pa.int64()),
            "x": _chunked([i * 10 for i in range(n)], data_sizes, pa.int64()),
            SOURCE_X: _chunked(tokens, source_sizes, NESTED_SOURCE_TYPE),
        }
    )
    stream = ArrowTableStream(table, tag_columns=["id"])
    pairs = list(stream.iter_data())
    assert len(pairs) == n
    for row, (tag, data) in enumerate(pairs):
        assert tag["id"] == row
        assert data["x"] == row * 10
        assert data.source_info()["x"] == tokens[row]
        source_schema = data.as_table(columns={"source": True}).schema
        assert source_schema.field(SOURCE_X).type == NESTED_SOURCE_TYPE


def test_iter_data_keeps_the_source_info_arrow_type() -> None:
    """``Data`` receives the stream's source-info column, not a re-inferred type.

    The first element of this token is an empty list, which says nothing about
    the depth below it. ``iter_data`` passes the Arrow column through
    (ITL-849), and ``Data`` serializes the token at its full depth.
    """
    source_type = pa.large_list(NESTED_SOURCE_TYPE)
    tokens = [[[], [["a"]]], [[["b"]], [["c"], []]]]
    table = pa.table(
        {
            "id": pa.array([0, 1], type=pa.int64()),
            "x": pa.array([0, 10], type=pa.int64()),
            SOURCE_X: pa.array(tokens, type=source_type),
        }
    )
    stream = ArrowTableStream(table, tag_columns=["id"])
    rows = list(stream.iter_data())
    assert [data.source_info()["x"] for _, data in rows] == tokens
    for _, data in rows:
        source_table = data.as_table(columns={"source": True})
        assert source_table.schema.field(SOURCE_X).type == source_type
