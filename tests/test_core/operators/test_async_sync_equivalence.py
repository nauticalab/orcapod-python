"""Every operator's async path yields exactly the rows of its sync path (ITL-837).

For the same inputs, ``async_execute`` must produce rows whose Arrow types,
nullability, column order, system tags, source info and Tag/Data content hashes
equal those of ``op(...).iter_data()``. A result must not depend on the path
the data took: ``Data.content_hash()`` is the input hash a downstream function
node stores, so any difference breaks cache hits between sync and async runs.

The inputs cover narrow integers, ``float32``, ``string`` and ``large_string``,
``binary``, ``list`` and ``large_list`` (with nullable and non-nullable items), a
struct, a logical type (``Path``), and nullable and non-nullable columns.
"""

from __future__ import annotations

import asyncio
import re
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TypeAlias, cast

import pyarrow as pa
import pytest

from orcapod.channels import Channel
from orcapod.contexts import get_default_context, get_default_type_converter
from orcapod.core.cached_function_pod import CachedFunctionPod
from orcapod.core.data_function import PythonDataFunction
from orcapod.core.function_pod import FunctionPod
from orcapod.core.operators import (
    Batch,
    DropDataColumns,
    DropTagColumns,
    GroupBy,
    Index,
    Join,
    MapData,
    MapTags,
    MergeJoin,
    Pick,
    PolarsFilter,
    SelectDataColumns,
    SelectTagColumns,
    SemiJoin,
)
from orcapod.core.operators.static_output_pod import StaticOutputOperatorPod
from orcapod.core.sources import ArrowTableSource
from orcapod.core.streams import ArrowTableStream
from orcapod.databases import InMemoryArrowDatabase
from orcapod.errors import InputValidationError
from orcapod.protocols.core_protocols import (
    DataProtocol,
    StreamProtocol,
    TagProtocol,
)
from orcapod.utils import arrow_utils

Row: TypeAlias = tuple[TagProtocol, DataProtocol]
Column: TypeAlias = "pa.Array[Any] | pa.ChunkedArray[Any]"

STRUCT = pa.struct([("x", pa.int8()), ("y", pa.string())])
NON_NULL_ITEMS = pa.list_(pa.field("item", pa.int8(), nullable=False))


def _dict_type(value_type: pa.DataType) -> pa.DataType:
    """The ``list<struct<key, value>>`` encoding orcapod uses for ``dict[str, V]``."""
    return pa.large_list(
        pa.struct(
            [
                pa.field("key", pa.large_string(), nullable=False),
                pa.field("value", value_type),
            ]
        )
    )


def _logical_column(values: list[Any], python_type: Any) -> pa.ChunkedArray[Any]:
    """Build a column of ``values`` in orcapod's logical Arrow type for ``python_type``."""
    return (
        get_default_type_converter()
        .python_dicts_to_arrow_table(
            [{"v": v} for v in values], python_schema={"v": python_type}
        )
        .column("v")
    )


def _source(
    columns: dict[str, Column],
    tags: Sequence[str],
    source_id: str,
    nullable: Sequence[str] = (),
) -> ArrowTableSource:
    """Build a source; every column not listed in ``nullable`` is non-nullable."""
    table = pa.table(list(columns.values()), names=list(columns))
    schema = pa.schema([f.with_nullable(f.name in nullable) for f in table.schema])
    return ArrowTableSource(
        table.cast(schema), tag_columns=list(tags), source_id=source_id
    )


def source_a() -> ArrowTableSource:
    """Tags ``k: int16`` (non-nullable) and ``ta: int8``; many narrow data columns."""
    return _source(
        {
            "k": pa.array([1, 2, 3], pa.int16()),
            "ta": pa.array([5, 5, 6], pa.int8()),
            "i8": pa.array([1, 2, 3], pa.int8()),
            "s": pa.array(["a", "b", "c"], pa.string()),
            "bn": pa.array([b"x", b"y", b"z"], pa.binary()),
            "st": pa.array(
                [{"x": 1, "y": "a"}, {"x": 2, "y": "b"}, {"x": 3, "y": "c"}], STRUCT
            ),
            "p": _logical_column([Path("/a"), Path("/b"), Path("/c")], Path),
            "l8": pa.array([[1, 2], [3], [4, 5]], pa.list_(pa.int8())),
            "d": pa.array(
                [[{"key": "q", "value": v}] for v in (1, 2, 3)], _dict_type(pa.int16())
            ),
            "nul": pa.array([7, None, 9], pa.int16()),
        },
        tags=("k", "ta"),
        source_id="a",
        nullable=("nul", "s"),
    )


def source_b(nested: bool = True) -> ArrowTableSource:
    """Tag ``k: int16``; scalar data columns, plus list columns when ``nested``."""
    columns: dict[str, Column] = {
        "k": pa.array([1, 2, 3], pa.int16()),
        "i32": pa.array([10, 20, 30], pa.int32()),
        "u8": pa.array([1, 2, 3], pa.uint8()),
        "f32": pa.array([0.5, 1.5, None], pa.float32()),
        "ls": pa.array(["p", "q", "r"], pa.large_string()),
    }
    if nested:
        columns["ll"] = pa.array([[0.5], [1.5, 2.5], []], pa.large_list(pa.float32()))
        columns["lnn"] = pa.array([[1], [2], [3]], NON_NULL_ITEMS)
    return _source(columns, tags=("k",), source_id="b", nullable=("f32",))


def source_c() -> ArrowTableSource:
    """Tags ``k: int16`` and nullable ``tc: uint8``; a third input for the staggered path."""
    return _source(
        {
            "k": pa.array([1, 2, 3], pa.int16()),
            "tc": pa.array([4, 4, 4], pa.uint8()),
            "i16": pa.array([100, 200, 300], pa.int16()),
            "c8": pa.array([-1, -2, None], pa.int8()),
        },
        tags=("k", "tc"),
        source_id="c",
        nullable=("tc", "c8"),
    )


def merge_source(source_id: str, offset: int) -> ArrowTableSource:
    """A ``MergeJoin`` input; two of them collide on every data column but one."""
    only = f"only_{source_id}"
    return _source(
        {
            "k": pa.array([1, 2, 3], pa.int16()),
            "x8": pa.array([offset, offset + 1, offset + 2], pa.int8()),
            "xf": pa.array([0.5 + offset, 1.5, 2.5], pa.float32()),
            "xs": pa.array([f"{source_id}{i}" for i in range(3)], pa.string()),
            "xp": _logical_column([Path(f"/{source_id}{i}") for i in range(3)], Path),
            only: pa.array([1, 2, 3], pa.uint8()),
        },
        tags=("k",),
        source_id=source_id,
        nullable=("xs",),
    )


# ── running both paths ─────────────────────────────────────────────────────


def sync_rows(op: StaticOutputOperatorPod, *sources: StreamProtocol) -> list[Row]:
    """The rows of the sync path, ``op(...)``."""
    return list(op(*sources).iter_data())


async def _run_async(
    op: StaticOutputOperatorPod, sources: Sequence[StreamProtocol]
) -> list[Row]:
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


def async_rows(op: StaticOutputOperatorPod, *sources: StreamProtocol) -> list[Row]:
    """The rows of the async path, ``op.async_execute`` over channels."""
    return asyncio.run(_run_async(op, sources))


def _row_key(tag: TagProtocol) -> str:
    return repr(sorted(tag.as_dict().items()))


def _row_schema(tag: TagProtocol, data: DataProtocol) -> pa.Schema:
    """The row's Arrow schema: user and system tags, then data."""
    return arrow_utils.join_arrow_schemas(
        tag.arrow_schema(columns={"system_tags": True}), data.arrow_schema()
    )


def _paired(expected: list[Row], actual: list[Row]) -> list[tuple[Row, Row]]:
    """Pair each async row with the sync row that has the same user tags."""
    by_key = {_row_key(tag): (tag, data) for tag, data in expected}
    assert len(by_key) == len(expected), "test inputs must give unique tag rows"
    assert sorted(_row_key(tag) for tag, _ in actual) == sorted(by_key)
    return [((tag, data), by_key[_row_key(tag)]) for tag, data in actual]


def assert_equivalent(expected: list[Row], actual: list[Row]) -> None:
    """Assert the async rows equal the sync rows in types, order, tags and hashes."""
    context = get_default_context()
    for (tag, data), (sync_tag, sync_data) in _paired(expected, actual):
        # Arrow types AND nullability, in column order.
        assert tag.arrow_schema(columns={"system_tags": True}) == sync_tag.arrow_schema(
            columns={"system_tags": True}
        )
        assert data.arrow_schema() == sync_data.arrow_schema()
        assert list(tag.keys()) == list(sync_tag.keys())
        assert list(data.keys()) == list(sync_data.keys())
        # System tags: names, order and values.
        assert list(tag.system_tags().items()) == list(sync_tag.system_tags().items())
        # Content hashes.
        assert tag.content_hash() == sync_tag.content_hash()
        assert data.content_hash() == sync_data.content_hash()
        # Schema hashes. A zero-row table's Arrow hash commits to its schema only.
        assert context.arrow_hasher.hash_table(
            _row_schema(tag, data).empty_table()
        ) == context.arrow_hasher.hash_table(
            _row_schema(sync_tag, sync_data).empty_table()
        )
        assert context.semantic_hasher.hash_object(
            (tag.schema(all_info=True), data.schema(all_info=True))
        ) == context.semantic_hasher.hash_object(
            (sync_tag.schema(all_info=True), sync_data.schema(all_info=True))
        )


def assert_same_provenance(expected: list[Row], actual: list[Row]) -> None:
    """Assert the async rows carry the sync rows' source info, column for column."""
    for (_, data), (_, sync_data) in _paired(expected, actual):
        assert data.source_info() == sync_data.source_info()
        assert data.arrow_schema(columns={"source": True}) == sync_data.arrow_schema(
            columns={"source": True}
        )


# ── the matrix ──────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Case:
    """An operator and the sources it runs on."""

    label: str
    make_op: Callable[[], StaticOutputOperatorPod]
    make_sources: Callable[[], tuple[StreamProtocol, ...]]


CASES = [
    Case("join_2", Join, lambda: (source_a(), source_b())),
    Case("join_2_reversed", Join, lambda: (source_b(), source_a())),
    Case("join_3", Join, lambda: (source_a(), source_b(), source_c())),
    Case("join_3_rotated", Join, lambda: (source_c(), source_a(), source_b())),
    Case(
        "merge_join",
        MergeJoin,
        lambda: (merge_source("m", 0), merge_source("n", 10)),
    ),
    Case("semijoin", SemiJoin, lambda: (source_c(), source_b(nested=False))),
    Case("semijoin_nested", SemiJoin, lambda: (source_a(), source_b())),
    Case("batch_2", lambda: Batch(batch_size=2), lambda: (source_a(),)),
    Case("batch_all", Batch, lambda: (source_a(),)),
    Case("group_by", lambda: GroupBy(["ta"]), lambda: (source_a(),)),
    Case(
        "polars_filter",
        lambda: PolarsFilter(constraints={"ta": 5}),
        lambda: (source_a(),),
    ),
    Case("map_tags", lambda: MapTags({"ta": "tb"}), lambda: (source_a(),)),
    Case("map_data", lambda: MapData({"i8": "j8"}), lambda: (source_a(),)),
    Case(
        "select_tag_columns",
        lambda: SelectTagColumns(["k"]),
        lambda: (source_a(),),
    ),
    Case(
        "select_data_columns",
        lambda: SelectDataColumns(["i8", "st", "p"]),
        lambda: (source_a(),),
    ),
    Case("drop_tag_columns", lambda: DropTagColumns(["ta"]), lambda: (source_a(),)),
    Case(
        "drop_data_columns",
        lambda: DropDataColumns(["s", "bn"]),
        lambda: (source_a(),),
    ),
    Case("index", lambda: Index("l8", 0), lambda: (source_a(),)),
    Case("pick", lambda: Pick("d", "q"), lambda: (source_a(),)),
]
CASE = pytest.mark.parametrize("case", CASES, ids=[c.label for c in CASES])


@CASE
def test_async_rows_equal_sync_rows(case: Case) -> None:
    """The async path yields the sync path's types, order, system tags and hashes."""
    sources = case.make_sources()
    assert_equivalent(
        sync_rows(case.make_op(), *sources), async_rows(case.make_op(), *sources)
    )


# Pre-existing provenance defects, not type re-inference: out of scope for
# ITL-837 and tracked elsewhere. Strict, so a fix flags them.
_DROPPED_SOURCE_INFO = (
    "ITL-838: Data.select / Data.drop keep the dropped columns' source info, so the "
    "async row's source-info schema lists columns it no longer has"
)
PROVENANCE_XFAIL = {
    "select_data_columns": _DROPPED_SOURCE_INFO,
    "drop_data_columns": _DROPPED_SOURCE_INFO,
}
PROVENANCE_CASES = [
    pytest.param(
        c,
        id=c.label,
        marks=[pytest.mark.xfail(strict=True, reason=PROVENANCE_XFAIL[c.label])]
        if c.label in PROVENANCE_XFAIL
        else [],
    )
    for c in CASES
]


def test_group_by_keeps_non_key_tags_as_tags_on_both_paths() -> None:
    """Neither path moves a non-key tag into the data (ITL-955).

    ``source_a`` has tags ``k`` and ``ta``; grouping by ``ta`` leaves ``k`` a
    list-valued tag, and the data of every row holds only the input's data
    columns.
    """
    _, data_keys = source_a().keys()
    for rows in (
        sync_rows(GroupBy(["ta"]), source_a()),
        async_rows(GroupBy(["ta"]), source_a()),
    ):
        members = sorted(tuple(cast(list[int], tag.as_dict()["k"])) for tag, _ in rows)
        assert members == [(1, 2), (3,)]
        for tag, data in rows:
            assert set(tag.keys()) == {"k", "ta"}
            assert set(data.keys()) == set(data_keys)
            assert tag.as_table().schema.field("k").type == pa.large_list(pa.int16())


@pytest.mark.parametrize("case", PROVENANCE_CASES)
def test_async_provenance_equals_sync(case: Case) -> None:
    """The async path carries the sync path's per-row source info."""
    sources = case.make_sources()
    assert_same_provenance(
        sync_rows(case.make_op(), *sources), async_rows(case.make_op(), *sources)
    )


def test_semijoin_with_nested_columns() -> None:
    """SemiJoin over inputs with struct and list data columns, both paths."""
    sources = (source_a(), source_b())
    assert_equivalent(sync_rows(SemiJoin(), *sources), async_rows(SemiJoin(), *sources))


@pytest.mark.xfail(
    strict=True,
    raises=TypeError,
    reason=(
        "ITL-846: ArrowHasher.hash_schema passes live extension types to starfix, "
        "which cannot hash them (hash_table normalizes them first)."
    ),
)
def test_arrow_hasher_hash_schema_with_extension_type() -> None:
    """``hash_schema`` hashes a schema with a logical (extension) column."""
    schema = pa.schema([pa.field("p", _logical_column([Path("/a")], Path).type)])
    _ = get_default_context().arrow_hasher.hash_schema(schema)  # pyright: ignore[reportAttributeAccessIssue]  # concrete hasher method, not on the protocol


# ── Data meta columns (ITL-854) ─────────────────────────────────────────────


def _meta_table(data: DataProtocol) -> pa.Table:
    """The data's meta (``__``-prefixed) columns alone, in their stored order."""
    return data.as_table(columns={"meta": True}).select(list(data.meta_columns))


def assert_same_meta(expected: list[Row], actual: list[Row]) -> None:
    """Assert the async rows carry the sync rows' meta columns.

    Names, order, Arrow types, nullability and values must all match.
    """
    for (_, data), (_, sync_data) in _paired(expected, actual):
        assert list(data.meta_columns) == list(sync_data.meta_columns)
        assert data.arrow_schema(columns={"meta": True}) == sync_data.arrow_schema(
            columns={"meta": True}
        )
        assert _meta_table(data).schema == _meta_table(sync_data).schema
        assert _meta_table(data).to_pylist() == _meta_table(sync_data).to_pylist()


def _with_meta(source: StreamProtocol, meta: dict[str, Column]) -> ArrowTableStream:
    """``source`` as a stream whose every ``Data`` also carries ``meta`` columns."""
    table = source.as_table(columns={"source": True, "system_tags": True})
    for name, column in meta.items():
        table = table.append_column(name, column)
    tags = source.keys()[0]
    return ArrowTableStream(table, tag_columns=tags)


def _meta_columns(prefix: str, n_rows: int) -> dict[str, Column]:
    """Meta columns named after ``prefix``: non-null ``int8``, nullable ``string``
    and nullable ``list<int8>``."""
    columns: dict[str, Column] = {
        f"__{prefix}_i8": pa.array(range(n_rows), pa.int8()),
        f"__{prefix}_s": pa.array(
            [None if i == 1 else f"{prefix}{i}" for i in range(n_rows)], pa.string()
        ),
        f"__{prefix}_l": pa.array([[i] for i in range(n_rows)], pa.list_(pa.int8())),
    }
    table = pa.table(list(columns.values()), names=list(columns))
    schema = pa.schema(
        [f.with_nullable(f.name != f"__{prefix}_i8") for f in table.schema]
    )
    table = table.cast(schema)
    return {name: table.column(name) for name in table.column_names}


def _meta_case(case: Case) -> Case:
    """``case`` with each source carrying its own, distinctly named meta columns."""

    def make_sources() -> tuple[StreamProtocol, ...]:
        sources = case.make_sources()
        return tuple(
            _with_meta(s, _meta_columns(f"m{i}", s.as_table().num_rows))
            for i, s in enumerate(sources)
        )

    return Case(case.label, case.make_op, make_sources)


# Operators whose paths disagree on meta columns in ways that are not a local
# Join-style fix. Strict, so a fix flags them.
_MATERIALIZE_DROPS_META = (
    "ITL-846: barrier-mode async_execute rebuilds its input with "
    "_materialize_to_stream, which drops Data meta columns; the rebuilt stream's "
    "pipeline hash then differs, so system-tag names and canonical order differ too"
)
_SYNC_DROPS_META = (
    "ITL-846: the sync path drops Data meta columns that the async path keeps (a raw "
    "ArrowTableStream lists its __ columns as data columns, so the sync selection "
    "or _materialize_to_stream discards them)"
)
META_XFAIL: dict[str, str] = {
    "merge_join": _MATERIALIZE_DROPS_META,
    "batch_2": _MATERIALIZE_DROPS_META,
    "batch_all": _MATERIALIZE_DROPS_META,
    "group_by": _MATERIALIZE_DROPS_META,
    "polars_filter": _MATERIALIZE_DROPS_META,
    "select_data_columns": _SYNC_DROPS_META,
    "index": _SYNC_DROPS_META,
    "pick": _SYNC_DROPS_META,
}
META_CASES = [
    pytest.param(
        _meta_case(c),
        id=c.label,
        marks=[pytest.mark.xfail(strict=True, reason=META_XFAIL[c.label])]
        if c.label in META_XFAIL
        else [],
    )
    for c in CASES
]


@pytest.mark.parametrize("case", META_CASES)
def test_async_meta_equals_sync(case: Case) -> None:
    """The async path's Data meta columns equal the sync path's (Join drops them on both)."""
    sources = case.make_sources()
    expected = sync_rows(case.make_op(), *sources)
    actual = async_rows(case.make_op(), *sources)
    assert_equivalent(expected, actual)
    assert_same_meta(expected, actual)


def meta_stream(
    source_id: str, value_column: str, meta: Sequence[str]
) -> ArrowTableStream:
    """A two-row stream on tag ``k: int16`` with ``int8`` meta columns ``meta``."""
    columns: dict[str, Column] = {
        "k": pa.array([1, 2], pa.int16()),
        value_column: pa.array([3, 4], pa.int8()),
    }
    source = _source(columns, tags=("k",), source_id=source_id)
    offset = sum(map(ord, source_id))
    return _with_meta(
        source,
        {
            name: pa.array([offset + i, offset + i + 1], pa.int8())
            for i, name in enumerate(meta)
        },
    )


def double_a(a: int) -> int:
    return a * 2


def double_b(b: int) -> int:
    return b * 2


def double_c(c: int) -> int:
    return c * 2


def cached_pod_stream(column: str, function: Callable[[int], int]) -> StreamProtocol:
    """A cached function pod's output on tag ``k: int16``.

    Every row's ``Data`` carries the pod's ``__computed`` meta column, which the
    stream's ``output_schema()`` does not list, so two such streams collide on it
    and still pass ``Join``'s input validation.
    """
    source = _source(
        {
            "k": pa.array([1, 2], pa.int16()),
            column: pa.array([3, 4], pa.int8()),
        },
        tags=("k",),
        source_id=column,
    )
    function_pod = FunctionPod(
        PythonDataFunction(function, output_keys=f"out_{column}")  # pyright: ignore[reportArgumentType]  # PythonDataFunction is not yet typed as matching DataFunctionProtocol (baselined in other tests); it works at runtime
    )
    pod = CachedFunctionPod(function_pod, result_database=InMemoryArrowDatabase())
    return pod(source)


# ── the issue's own example, pinned ────────────────────────────────────────


def _narrow_source(column: str, source_id: str) -> ArrowTableSource:
    table = pa.table(
        {"k": pa.array([1, 2], pa.int16()), column: pa.array([3, 4], pa.int8())}
    )
    return ArrowTableSource(table, tag_columns=["k"], source_id=source_id)


# Recorded from the sync path on main (ab544982) before the ITL-837 fix. The sync
# path must not change; the async path must now produce the same values.
PINNED_ROW_HASHES = {
    1: (
        "arrow_v0.1:000001515f1eb3674534e0226bbe2aada98adb5137a0f8829ee815fdb994809ac001fa",
        "arrow_v0.1:00000119d2a6630f08dd30ef49c4a298862b7d129d2169d84963ea4f801eb8248e99e4",
    ),
    2: (
        "arrow_v0.1:000001ea3d721d986f7382996ef41ded3d1a43baf41a543fde471b20c71190b45dbf98",
        "arrow_v0.1:000001fa7bf20e72872ffc6860a21d75ad93c38bd54753fa9eb325502acc783b693d43",
    ),
}
PINNED_STREAM_CONTENT_HASH = (
    "semantic_v0.1:ac7c70563c06d9ba54adb890bb52a886e6005937bd3d5df265cd3f6817e988bd"
)
PINNED_STREAM_PIPELINE_HASH = (
    "semantic_v0.1:929efa4256930fda2d02ad359106db279fc888ff155d9500d47cef194137e744"
)


def test_issue_example_types_and_pinned_hashes() -> None:
    """ITL-837's example: ``k: int16``, ``a``/``b: int8`` keep their types on both paths."""
    left, right = _narrow_source("a", "l"), _narrow_source("b", "r")
    stream = Join()(left, right)
    assert stream.content_hash().to_string() == PINNED_STREAM_CONTENT_HASH
    assert stream.pipeline_hash().to_string() == PINNED_STREAM_PIPELINE_HASH
    for rows in (sync_rows(Join(), left, right), async_rows(Join(), left, right)):
        assert len(rows) == 2
        for tag, data in rows:
            assert tag.arrow_schema().field("k").type == pa.int16()
            assert data.arrow_schema().field("a").type == pa.int8()
            assert data.arrow_schema().field("b").type == pa.int8()
            tag_hash, data_hash = PINNED_ROW_HASHES[cast(int, tag["k"])]
            assert tag.content_hash().to_string() == tag_hash
            assert data.content_hash().to_string() == data_hash


# ── shared join keys whose Arrow types disagree ─────────────────────────────


def _key_source(key: Column, column: str, source_id: str) -> ArrowTableSource:
    table = pa.Table.from_arrays(
        [key, pa.array([1, 2], pa.int8())], names=["k", column]
    )
    return ArrowTableSource(table, tag_columns=["k"], source_id=source_id)


DISAGREEING_KEYS = [
    pytest.param(pa.int16(), pa.int32(), id="int16-int32"),
    pytest.param(pa.int8(), pa.uint8(), id="int8-uint8"),
    pytest.param(pa.float32(), pa.float64(), id="float32-double"),
]
KEYS = pytest.mark.parametrize(("left_type", "right_type"), DISAGREEING_KEYS)
# The message names the column and both Arrow types, in either order.
_KEY_MESSAGE = r"column 'k': (\w+) in one input and (\w+) in the other"


def _disagreeing_sources(
    left_type: pa.DataType, right_type: pa.DataType, matching: bool = True
) -> tuple[ArrowTableSource, ArrowTableSource]:
    """Two sources whose ``k`` types disagree; with ``matching=False`` no key matches."""
    right_values = [1, 2] if matching else [10, 20]
    return (
        _key_source(pa.array([1, 2], left_type), "a", "l"),
        _key_source(pa.array(right_values, right_type), "b", "r"),
    )


# The error must not depend on whether any key values match.
MATCHING = pytest.mark.parametrize("matching", [True, False], ids=["match", "disjoint"])


@KEYS
@MATCHING
def test_sync_join_rejects_disagreeing_key_types(
    left_type: pa.DataType, right_type: pa.DataType, matching: bool
) -> None:
    """A shared tag key with different Arrow types is an error, never a conversion."""
    left, right = _disagreeing_sources(left_type, right_type, matching)
    for operands in ((left, right), (right, left)):
        with pytest.raises(InputValidationError, match=_KEY_MESSAGE) as excinfo:
            _ = Join()(*operands).as_table()
        found = re.search(_KEY_MESSAGE, str(excinfo.value))
        assert found is not None
        assert set(found.groups()) == {str(left_type), str(right_type)}


@KEYS
@MATCHING
def test_async_join_rejects_disagreeing_key_types(
    left_type: pa.DataType, right_type: pa.DataType, matching: bool
) -> None:
    """The async path rejects the same inputs with the same error."""
    left, right = _disagreeing_sources(left_type, right_type, matching)
    for operands in ((left, right), (right, left)):
        # The merge runs inside the join's task group, which wraps the error.
        with pytest.raises(ExceptionGroup) as excinfo:
            _ = async_rows(Join(), *operands)
        assert excinfo.group_contains(InputValidationError, match=_KEY_MESSAGE)


@MATCHING
def test_join_rejects_disagreeing_key_types_three_inputs(matching: bool) -> None:
    """The check also applies to the staggered (three-input) path, sync and async."""
    left, right = _disagreeing_sources(pa.int16(), pa.int32(), matching)
    third = _key_source(pa.array([1, 2], pa.int16()), "c", "t")
    with pytest.raises(InputValidationError, match=_KEY_MESSAGE):
        _ = Join()(left, right, third).as_table()
    with pytest.raises(ExceptionGroup) as excinfo:
        _ = async_rows(Join(), left, right, third)
    assert excinfo.group_contains(InputValidationError, match=_KEY_MESSAGE, depth=None)


@pytest.mark.parametrize(
    ("left_type", "right_type"),
    [
        pytest.param(pa.string(), pa.large_string(), id="string"),
        pytest.param(pa.binary(), pa.large_binary(), id="binary"),
    ],
)
def test_join_accepts_layout_only_key_difference(
    left_type: pa.DataType, right_type: pa.DataType
) -> None:
    """Keys that differ only in layout normalize and join, identically on both paths."""
    values = ["x", "y"] if pa.types.is_string(left_type) else [b"x", b"y"]
    left = _key_source(pa.array(values, left_type), "a", "l")
    right = _key_source(pa.array(values, right_type), "b", "r")
    for operands in ((left, right), (right, left)):
        expected = sync_rows(Join(), *operands)
        assert len(expected) == 2
        assert_equivalent(expected, async_rows(Join(), *operands))
