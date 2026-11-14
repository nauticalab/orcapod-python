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
    DropColumns,
    DropDataColumns,
    DropNulls,
    DropTagColumns,
    GroupBy,
    Index,
    Join,
    MapData,
    MapTags,
    MergeJoin,
    Pick,
    PolarsFilter,
    SelectColumns,
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
    # Tag and data columns in one call (ITL-1084).
    Case(
        "drop_columns",
        lambda: DropColumns(["ta", "s", "bn"]),
        lambda: (source_a(),),
    ),
    Case(
        "select_columns",
        lambda: SelectColumns(["k", "i8", "st", "p", "nul"]),
        lambda: (source_a(),),
    ),
    Case("index", lambda: Index("l8", 0), lambda: (source_a(),)),
    Case("pick", lambda: Pick("d", "q"), lambda: (source_a(),)),
    Case("drop_nulls", lambda: DropNulls("nul"), lambda: (source_a(),)),
    Case(
        "drop_nulls_many",
        # A nullable column with a null, one without, a tag, a non-nullable list.
        lambda: DropNulls(["nul", "s", "ta", "l8"]),
        lambda: (source_a(),),
    ),
    # No names: every tag and data column, among them struct, logical and list ones.
    Case("drop_nulls_all", lambda: DropNulls(), lambda: (source_a(),)),
]
CASE = pytest.mark.parametrize("case", CASES, ids=[c.label for c in CASES])


@CASE
def test_async_rows_equal_sync_rows(case: Case) -> None:
    """The async path yields the sync path's types, order, system tags and hashes."""
    sources = case.make_sources()
    assert_equivalent(
        sync_rows(case.make_op(), *sources), async_rows(case.make_op(), *sources)
    )


# ── inputs that are not root sources (ITL-906) ──────────────────────────────


def after_operator(source: StreamProtocol) -> StreamProtocol:
    """``source`` behind an operator that keeps every row: ``DropNulls`` on tag ``k``.

    Every source of the matrix has the non-nullable tag ``k``, so the rows and columns
    are those of ``source``; only the pipeline hash differs, as it does after any
    operator or function pod.
    """
    stream = DropNulls("k")(source)
    assert stream.pipeline_hash() != source.pipeline_hash()
    return stream


def _after_operator_case(case: Case) -> Case:
    """``case`` with every input behind ``after_operator``."""

    def make_sources() -> tuple[StreamProtocol, ...]:
        return tuple(after_operator(s) for s in case.make_sources())

    return Case(case.label, case.make_op, make_sources)


def _first_after_operator_case(case: Case) -> Case:
    """``case`` with only its first input behind ``after_operator``."""

    def make_sources() -> tuple[StreamProtocol, ...]:
        first, *rest = case.make_sources()
        return (after_operator(first), *rest)

    return Case(case.label, case.make_op, make_sources)


AFTER_OPERATOR_CASES = [
    pytest.param(_after_operator_case(c), id=f"{c.label}-all") for c in CASES
] + [
    pytest.param(_first_after_operator_case(c), id=f"{c.label}-first")
    for c in CASES
    if len(c.make_sources()) > 1
]


@pytest.mark.parametrize("case", AFTER_OPERATOR_CASES)
def test_async_rows_equal_sync_rows_after_an_operator(case: Case) -> None:
    """Every operator's async path equals its sync path when an input is not a source.

    System-tag column names of ``MergeJoin`` (name-extending), ``Batch`` and ``GroupBy``
    (reducing) end in the input's pipeline hash; the async path must use the hash of the
    stream the rows came from, not that of the rows rebuilt into a table.
    """
    sources = case.make_sources()
    assert_equivalent(
        sync_rows(case.make_op(), *sources), async_rows(case.make_op(), *sources)
    )


def function_pod_stream(source_id: str, offset: int) -> StreamProtocol:
    """A function pod's output on tag ``k: int16``: ``x = a + offset`` as ``int``."""
    source = _source(
        {"k": pa.array([1, 2, 3], pa.int16()), "a": pa.array([1, 2, 3], pa.int8())},
        tags=("k",),
        source_id=source_id,
    )

    def shift(a: int) -> int:
        return a + offset

    pod = FunctionPod(
        PythonDataFunction(shift, output_keys="x", function_name=f"shift_{source_id}")  # pyright: ignore[reportArgumentType]  # PythonDataFunction is not yet typed as matching DataFunctionProtocol (baselined in other tests); it works at runtime
    )
    return pod(source)


FUNCTION_POD_CASES = [
    Case(
        "merge_join",
        MergeJoin,
        lambda: (function_pod_stream("p", 0), function_pod_stream("q", 10)),
    ),
    Case(
        "merge_join_with_source",
        MergeJoin,
        lambda: (
            function_pod_stream("p", 0),
            _source(
                {
                    "k": pa.array([1, 2, 3], pa.int16()),
                    "x": pa.array([7, 8, 9], pa.int64()),
                },
                tags=("k",),
                source_id="r",
            ),
        ),
    ),
    Case("join", Join, lambda: (function_pod_stream("p", 0), source_b())),
    Case("semijoin", SemiJoin, lambda: (function_pod_stream("p", 0), source_b())),
    Case(
        "batch_2", lambda: Batch(batch_size=2), lambda: (function_pod_stream("p", 0),)
    ),
    Case("batch_all", Batch, lambda: (function_pod_stream("p", 0),)),
    Case("group_by", lambda: GroupBy(["k"]), lambda: (function_pod_stream("p", 0),)),
    Case(
        "polars_filter",
        lambda: PolarsFilter(constraints={"k": 1}),
        lambda: (function_pod_stream("p", 0),),
    ),
]


@pytest.mark.parametrize(
    "case", FUNCTION_POD_CASES, ids=[c.label for c in FUNCTION_POD_CASES]
)
def test_async_rows_equal_sync_rows_after_a_function_pod(case: Case) -> None:
    """The async path equals the sync path when an input is a function pod's output."""
    sources = case.make_sources()
    assert_equivalent(
        sync_rows(case.make_op(), *sources), async_rows(case.make_op(), *sources)
    )


@pytest.mark.parametrize(
    "make_op",
    [lambda: Batch(batch_size=2), Batch, lambda: GroupBy(["ta"])],
    ids=["batch_2", "batch_all", "group_by"],
)
def test_reducing_operator_names_system_tags_after_its_input(
    make_op: Callable[[], StaticOutputOperatorPod],
) -> None:
    """A folded system-tag column name ends in the input's pipeline hash on both paths.

    System tag rule 3: ``Batch`` and ``GroupBy`` append ``::{pipeline_hash}`` of their
    input. The input here is an operator's output, whose pipeline hash is not the
    schema-only hash of its rows.
    """
    op = make_op()
    stream = after_operator(source_a())
    suffix = stream.pipeline_hash().to_hex(op.orcapod_config.hashing.system_tag_n_char)
    for rows in (sync_rows(make_op(), stream), async_rows(make_op(), stream)):
        for tag, _ in rows:
            names = list(tag.system_tags())
            assert names
            assert all(name.endswith(f"::{suffix}") for name in names), names


@CASE
def test_async_execute_requires_input_pipeline_hashes(case: Case) -> None:
    """Every operator's ``async_execute`` takes the input pipeline hashes, always.

    Without them a barrier path would name system tags after a schema-only hash,
    unlike the sync path (ITL-906), so there is no default to fall back on.
    """
    channels: list[Channel[Row]] = [Channel() for _ in case.make_sources()]
    output: Channel[Row] = Channel()
    with pytest.raises(TypeError, match="input_pipeline_hashes"):
        case.make_op().async_execute(  # pyright: ignore[reportCallIssue]  # the missing keyword is the point
            [channel.reader for channel in channels], output.writer
        )


@pytest.mark.parametrize(
    "case",
    [
        c
        for c in CASES
        if c.label
        in (
            "join_2",
            "join_3",
            "merge_join",
            "batch_2",
            "batch_all",
            "group_by",
            "polars_filter",
        )
    ],
    ids=lambda c: c.label,
)
def test_barrier_async_rejects_wrong_number_of_pipeline_hashes(case: Case) -> None:
    """An ``async_execute`` that uses the hashes needs one per input channel."""
    sources = case.make_sources()
    n_inputs = len(sources)

    async def run() -> None:
        channels: list[Channel[Row]] = [Channel(buffer_size=64) for _ in sources]
        for stream, channel in zip(sources, channels):
            for row in stream.iter_data():
                await channel.writer.send(row)
            await channel.writer.close()
        output: Channel[Row] = Channel(buffer_size=64)
        await case.make_op().async_execute(
            [channel.reader for channel in channels],
            output.writer,
            input_pipeline_hashes=[s.pipeline_hash() for s in sources] * 2,
        )

    message = rf"length \({2 * n_inputs}\) must match inputs length \({n_inputs}\)"
    with pytest.raises(ValueError, match=message):
        asyncio.run(run())


# ── inputs that deliver no rows (ITL-865) ───────────────────────────────────


def no_rows(source: StreamProtocol) -> StreamProtocol:
    """``source`` behind a filter that matches no row (every source has ``k`` in 1..3)."""
    stream = PolarsFilter(constraints={"k": 0})(source)
    assert list(stream.iter_data()) == []
    return stream


def _zero_row_cases() -> list[Any]:
    """Every case with all inputs empty, and each multi-input case with one input empty."""
    params: list[Any] = []
    for case in CASES:
        n_inputs = len(case.make_sources())
        empty_sets = [tuple(range(n_inputs))]
        if n_inputs > 1:
            empty_sets += [(i,) for i in range(n_inputs)]
        for empty in empty_sets:

            def make_sources(
                case: Case = case, empty: tuple[int, ...] = empty
            ) -> tuple[StreamProtocol, ...]:
                return tuple(
                    no_rows(s) if i in empty else s
                    for i, s in enumerate(case.make_sources())
                )

            suffix = "all" if len(empty) == n_inputs else f"input_{empty[0]}"
            params.append(
                pytest.param(
                    Case(case.label, case.make_op, make_sources),
                    id=f"{case.label}-empty_{suffix}",
                )
            )
    return params


@pytest.mark.parametrize("case", _zero_row_cases())
def test_async_rows_equal_sync_rows_when_an_input_has_no_rows(case: Case) -> None:
    """An input with no rows gives the sync path's rows on the async path, never an error.

    The operator is applied to its inputs first, ``op(...)``, as an operator node does when
    the graph is built: ``SemiJoin`` with an empty right input reads the common keys that
    application recorded, because a channel with no rows says nothing about its columns
    (ITL-858).
    """
    sources = case.make_sources()
    expected = sync_rows(case.make_op(), *sources)
    op = case.make_op()
    _ = op(*sources)
    actual = async_rows(op, *sources)
    assert len(actual) == len(expected)
    assert_equivalent(expected, actual)


def test_drop_nulls_without_tag_columns() -> None:
    """A stream without tag columns: the async path drops the same rows, in order.

    Outside the matrix, whose pairing needs unique tag rows.
    """
    source = _source(
        {
            "x": pa.array([1, None, 3], pa.int8()),
            "s": pa.array(["a", "b", "c"], pa.string()),
        },
        tags=(),
        source_id="tagless",
        nullable=("x", "s"),
    )
    expected = sync_rows(DropNulls("x"), source)
    actual = async_rows(DropNulls("x"), source)
    assert [data.as_dict() for _, data in actual] == [
        {"x": 1, "s": "a"},
        {"x": 3, "s": "c"},
    ]
    assert len(actual) == len(expected)
    for (tag, data), (sync_tag, sync_data) in zip(actual, expected):
        assert data.arrow_schema() == sync_data.arrow_schema()
        assert data.content_hash() == sync_data.content_hash()
        assert data.source_info() == sync_data.source_info()
        assert tag.arrow_schema(columns={"system_tags": True}) == sync_tag.arrow_schema(
            columns={"system_tags": True}
        )
        assert list(tag.system_tags().items()) == list(sync_tag.system_tags().items())

    # No names: both data columns are tested and narrowed on both paths.
    source = _source(
        {
            "x": pa.array([1, None, 3], pa.int8()),
            "s": pa.array(["a", "b", None], pa.string()),
        },
        tags=(),
        source_id="tagless",
        nullable=("x", "s"),
    )
    expected = sync_rows(DropNulls(), source)
    actual = async_rows(DropNulls(), source)
    assert [data.as_dict() for _, data in actual] == [{"x": 1, "s": "a"}]
    assert len(actual) == len(expected) == 1
    for (_, data), (_, sync_data) in zip(actual, expected):
        assert data.arrow_schema() == sync_data.arrow_schema()
        assert [f.nullable for f in data.arrow_schema()] == [False, False]
        assert data.content_hash() == sync_data.content_hash()
        assert data.source_info() == sync_data.source_info()


# Every case: the async path carries the sync path's source info (ITL-838 / ITL-1040 fixed
# the select and drop cases, whose dropped columns' source info used to survive).
PROVENANCE_CASES = [pytest.param(c, id=c.label) for c in CASES]


def test_group_by_keeps_only_key_tags_on_both_paths() -> None:
    """Both paths drop the non-key tags and keep the data lists (ITL-1070).

    ``source_a`` has tags ``k`` and ``ta``; grouping by ``ta`` drops ``k``, and
    the data of every row holds only the input's data columns, ordered by ``k``.
    """
    _, data_keys = source_a().keys()
    results = []
    for rows in (
        sync_rows(GroupBy(["ta"]), source_a()),
        async_rows(GroupBy(["ta"]), source_a()),
    ):
        for tag, data in rows:
            assert set(tag.keys()) == {"ta"}
            assert set(data.keys()) == set(data_keys)
        results.append(
            sorted(
                (tag.as_dict()["ta"], data.as_table().to_pylist()[0])
                for tag, data in rows
            )
        )
    assert results[0] == results[1]


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


def test_arrow_hasher_hash_schema_with_extension_type() -> None:
    """``hash_schema`` hashes a logical (extension) column with its identity (ITL-846).

    The extension name and metadata are part of the hash, so the schema does not hash like
    one holding the extension's storage type.
    """
    ext_type = _logical_column([Path("/a")], Path).type
    assert isinstance(ext_type, pa.ExtensionType)
    hasher = get_default_context().arrow_hasher
    ext_hash = hasher.hash_schema(pa.schema([pa.field("p", ext_type)]))  # pyright: ignore[reportAttributeAccessIssue]  # concrete hasher method, not on the protocol
    storage_hash = hasher.hash_schema(pa.schema([pa.field("p", ext_type.storage_type)]))  # pyright: ignore[reportAttributeAccessIssue]  # concrete hasher method, not on the protocol
    assert ext_hash != storage_hash


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
    "_materialize_to_stream, which drops Data meta columns"
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
# path must not change; the async path must now produce the same values. Row hashes
# re-recorded in starfix hash format 2 (arrow_v1, ITL-910); the stream hashes did not move.
PINNED_ROW_HASHES = {
    1: (
        "arrow_v1:0000027e1a4693c531d4411fafa68a1ac623a0ed3f04a6402de0fb8f040f0aba717ae8",
        "arrow_v1:0000025c4232f42a6012b104827b2c4731e143af6044e010e9a37ae5ffa82fe3ae7153",
    ),
    2: (
        "arrow_v1:000002e3b4cd9f773e0263ba270d251b90058702fa282e129feebdbc428d832080752e",
        "arrow_v1:00000218de95b54e8987999a339fe1726957da66219b529c4699be853334833e71f60e",
    ),
}
PINNED_STREAM_CONTENT_HASH = (
    "semantic_v1:1ff5a58dbf661279f4fe767477adcc7d95f6f4f6ee47c5ccb53a6a7ab1a2306f"
)
PINNED_STREAM_PIPELINE_HASH = (
    "semantic_v1:8f8d1f2e44624b0e42a55ad513a38626d54012d783e1f2c7cdcd706e22ea709a"
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


# ── shared tags whose values are lists, structs or null (ITL-1044) ─────────

_NAN = float("nan")


def _tag_source(
    key_type: pa.DataType,
    keys: list[Any],
    id_name: str,
    data_name: str,
    source_id: str,
) -> ArrowTableSource:
    """A source with tags (``<id_name>``, ``k``) and one data column.

    The tag columns are not in alphabetical order and the rows are not sorted
    by ``k``: an input already in tag order once hid a defect.
    """
    ids = list(range(len(keys), 0, -1))
    table = pa.Table.from_arrays(
        [
            pa.array(ids, pa.int64()),
            pa.array(keys, key_type),
            pa.array(ids, pa.int16()),
        ],
        names=[id_name, "k", data_name],
    )
    return ArrowTableSource(table, tag_columns=[id_name, "k"], source_id=source_id)


_LIST_INT = pa.large_list(pa.int64())
_NESTED = pa.large_list(pa.large_list(pa.int64()))
_STRUCT = pa.struct([("x", pa.int64()), ("y", pa.large_list(pa.int64()))])

KEY_CASES = [
    pytest.param(
        _LIST_INT,
        [[3, 1], [1, 2], [2, 1], [1, 2]],
        [[1, 2], [2, 1], [9], [1, 2]],
        id="list_int_many_to_many",
    ),
    pytest.param(
        pa.large_list(pa.large_string()),
        [["b", "a"], ["a"], ["a", "b"]],
        [["a", "b"], ["a"], ["b", "a"], ["a"]],
        id="list_string",
    ),
    pytest.param(
        _NESTED,
        [[[1], [2, 3]], [[2, 3], [1]], [[]], [[1], None]],
        [[[1], [2, 3]], [[]], [[1], None], []],
        id="nested_list",
    ),
    pytest.param(
        _STRUCT,
        [{"x": 2, "y": [1]}, {"x": 1, "y": None}, {"x": None, "y": None}],
        [{"x": 1, "y": None}, {"x": 2, "y": [1]}, {"x": None, "y": None}],
        id="struct",
    ),
    pytest.param(
        _LIST_INT,
        [[None, 1], [1, None], [None], [1]],
        [[1, None], [None], [None, 1], [None]],
        id="null_element",
    ),
    pytest.param(
        _LIST_INT,
        [None, [1], None, []],
        [[1], None, [], None],
        id="null_list_and_empty_list",
    ),
    pytest.param(
        _STRUCT,
        [None, {"x": None, "y": None}, {"x": 1, "y": []}],
        [{"x": None, "y": None}, None, {"x": 1, "y": []}],
        id="null_struct_and_struct_of_nulls",
    ),
    pytest.param(
        pa.large_list(pa.float64()),
        [[_NAN, 1.0], [0.0], [1.0, _NAN]],
        [[_NAN, 1.0], [-0.0], [_NAN]],
        id="nan_in_list",
    ),
    pytest.param(pa.float64(), [_NAN, 1.0, 0.0], [_NAN, 2.0, -0.0], id="nan"),
    pytest.param(pa.int64(), [3, None, 1], [None, 1, 2], id="null_primitive"),
    pytest.param(
        pa.large_string(), [None, "a", "b"], [None, "b", "a"], id="null_string"
    ),
]
JOIN_KEYS = pytest.mark.parametrize(("key_type", "left_keys", "right_keys"), KEY_CASES)


def _key_sources(
    key_type: pa.DataType, left_keys: list[Any], right_keys: list[Any]
) -> tuple[ArrowTableSource, ArrowTableSource]:
    return (
        _tag_source(key_type, left_keys, "zl", "a", "left"),
        _tag_source(key_type, right_keys, "zr", "b", "right"),
    )


@JOIN_KEYS
def test_async_join_matches_shared_tags_as_the_sync_join_does(
    key_type: pa.DataType, left_keys: list[Any], right_keys: list[Any]
) -> None:
    """Shared tags of any type match on both paths by the same equality (ITL-1044)."""
    left, right = _key_sources(key_type, left_keys, right_keys)
    for operands in ((left, right), (right, left)):
        expected = sync_rows(Join(), *operands)
        assert_equivalent(expected, async_rows(Join(), *operands))


@JOIN_KEYS
def test_async_semijoin_matches_shared_tags_as_the_sync_semijoin_does(
    key_type: pa.DataType, left_keys: list[Any], right_keys: list[Any]
) -> None:
    """``SemiJoin`` keeps the same left rows on both paths for these keys."""
    left, right = _key_sources(key_type, left_keys, right_keys)
    # ``SemiJoin`` is ordered: both operand orders.
    for operands in ((left, right), (right, left)):
        expected = sync_rows(SemiJoin(), *operands)
        assert_equivalent(expected, async_rows(SemiJoin(), *operands))


def test_async_join_matches_a_shared_list_tag_across_three_inputs() -> None:
    """The staggered N-ary join matches list-valued tags like the sync join."""
    left, right = _key_sources(
        _LIST_INT, [[3, 1], [1, 2], [2, 1], [1, 2]], [[1, 2], [2, 1], [9], [1, 2]]
    )
    third = _tag_source(_LIST_INT, [[2, 1], [1, 2], None], "zt", "c", "third")
    for operands in ((left, right, third), (third, left, right)):
        expected = sync_rows(Join(), *operands)
        assert expected
        assert_equivalent(expected, async_rows(Join(), *operands))


# ── null join keys (ITL-1050) ───────────────────────────────────────────────

NULL_KEY_CASES = [
    pytest.param(["a", None, "b"], [None, "a", "b"], id="null_on_both_sides"),
    pytest.param(["a", None, "b"], ["b", "a", "c"], id="null_on_one_side"),
    pytest.param([None, None, None], [None, None], id="all_null_key_column"),
    pytest.param([None, None], ["b", "a"], id="all_null_on_one_side"),
]
NULL_KEYS = pytest.mark.parametrize(("left_keys", "right_keys"), NULL_KEY_CASES)


def _null_key_source(
    keys: list[str | None], id_name: str, data_name: str, source_id: str
) -> ArrowTableSource:
    """Tags (``<id_name>``, nullable ``g``) out of tag order, one data column."""
    ids = list(range(len(keys), 0, -1))
    return _tag_source(
        pa.large_string(), keys, id_name, data_name, source_id
    ).__class__(
        pa.Table.from_arrays(
            [
                pa.array(ids, pa.int64()),
                pa.array(keys, pa.large_string()),
                pa.array(ids, pa.int16()),
            ],
            names=[id_name, "g", data_name],
        ),
        tag_columns=[id_name, "g"],
        source_id=source_id,
    )


@NULL_KEYS
@pytest.mark.parametrize("operator", [Join, SemiJoin], ids=["join", "semijoin"])
def test_async_null_key_matches_nothing_as_the_sync_path_does(
    operator: Callable[[], StaticOutputOperatorPod],
    left_keys: list[str | None],
    right_keys: list[str | None],
) -> None:
    """A null in a shared tag matches nothing, on both paths, in both operand orders."""
    left = _null_key_source(left_keys, "zl", "a", "left")
    right = _null_key_source(right_keys, "zr", "b", "right")
    for operands in ((left, right), (right, left)):
        expected = sync_rows(operator(), *operands)
        assert_equivalent(expected, async_rows(operator(), *operands))


@NULL_KEYS
def test_async_null_key_in_a_three_input_join_matches_nothing(
    left_keys: list[str | None], right_keys: list[str | None]
) -> None:
    """The staggered N-ary join also never matches a null key."""
    left = _null_key_source(left_keys, "zl", "a", "left")
    right = _null_key_source(right_keys, "zr", "b", "right")
    third = _null_key_source([None, "a", "b"], "zt", "c", "third")
    for operands in ((left, right, third), (third, right, left)):
        assert_equivalent(sync_rows(Join(), *operands), async_rows(Join(), *operands))


def _merge_null_key_source(
    keys: list[str | None], source_id: str, offset: int
) -> ArrowTableSource:
    """A ``MergeJoin`` input: nullable tag ``g``, colliding data column ``x``."""
    ids = list(range(len(keys), 0, -1))
    return ArrowTableSource(
        pa.Table.from_arrays(
            [
                pa.array(ids, pa.int64()),
                pa.array(keys, pa.large_string()),
                pa.array([i + offset for i in ids], pa.int16()),
            ],
            names=[f"z{source_id}", "g", "x"],
        ),
        tag_columns=[f"z{source_id}", "g"],
        source_id=source_id,
    )


@NULL_KEYS
def test_async_merge_join_with_null_tags_equals_sync(
    left_keys: list[str | None], right_keys: list[str | None]
) -> None:
    """``MergeJoin`` agrees on both paths when a tag is null."""
    left = _merge_null_key_source(left_keys, "l", 0)
    right = _merge_null_key_source(right_keys, "r", 10)
    for operands in ((left, right), (right, left)):
        assert_equivalent(
            sync_rows(MergeJoin(), *operands), async_rows(MergeJoin(), *operands)
        )


# A name that is a tag in one input and a data column in another (ITL-940): both paths raise
# the same InputValidationError, never a KeyError or rows whose tag and data values differ.
def _tag_data_sources() -> tuple[ArrowTableSource, ArrowTableSource]:
    tag_side = _source(
        {"x": pa.array([1, 2], pa.int64()), "a": pa.array([3, 4], pa.int64())},
        tags=("x",),
        source_id="tag_side",
    )
    data_side = _source(
        {
            "k": pa.array([1, 2], pa.int64()),
            "x": pa.array([1, 2], pa.int64()),
            "b": pa.array([5, 6], pa.int64()),
        },
        tags=("k",),
        source_id="data_side",
    )
    return tag_side, data_side


_TAG_DATA_MESSAGE = r"column 'x' is a tag in the (first|second) input and a data column"


@pytest.mark.parametrize("operator", [Join, MergeJoin], ids=["join", "merge_join"])
def test_join_paths_reject_a_tag_and_a_data_column_of_one_name(
    operator: type[Join] | type[MergeJoin],
) -> None:
    tag_side, data_side = _tag_data_sources()
    for operands in ((tag_side, data_side), (data_side, tag_side)):
        with pytest.raises(InputValidationError, match=_TAG_DATA_MESSAGE):
            _ = operator()(*operands)
        with pytest.raises(BaseException) as excinfo:
            _ = async_rows(operator(), *operands)
        # Join's merge runs inside a task group, which wraps the error.
        errors = (
            excinfo.value.exceptions
            if isinstance(excinfo.value, BaseExceptionGroup)
            else (excinfo.value,)
        )
        assert any(
            isinstance(e, InputValidationError) and re.search(_TAG_DATA_MESSAGE, str(e))
            for e in errors
        ), errors


# ── keys that differ below one microsecond (CodeRabbit, PR #351) ────────────

_NS_TYPES = [
    pytest.param(pa.time64("ns"), id="time64_ns"),
    pytest.param(pa.timestamp("ns"), id="timestamp_ns"),
    pytest.param(pa.duration("ns"), id="duration_ns"),
]


@pytest.mark.parametrize("key_type", _NS_TYPES)
@pytest.mark.parametrize("nested", [False, True], ids=["top_level", "in_list"])
@pytest.mark.parametrize("operator", [Join, SemiJoin], ids=["join", "semijoin"])
def test_async_keys_differing_below_a_microsecond_stay_apart(
    operator: Callable[[], StaticOutputOperatorPod],
    nested: bool,
    key_type: pa.DataType,
) -> None:
    """Nanosecond keys that ``as_py()`` would merge do not match, as in the sync path."""
    left_keys: list[Any] = [3, 1, 2]
    right_keys: list[Any] = [2, 1, 4]
    arrow_type: pa.DataType = key_type
    if nested:
        left_keys = [[k] for k in left_keys]
        right_keys = [[k] for k in right_keys]
        arrow_type = pa.large_list(key_type)
    left, right = _key_sources(arrow_type, left_keys, right_keys)
    for operands in ((left, right), (right, left)):
        expected = sync_rows(operator(), *operands)
        assert_equivalent(expected, async_rows(operator(), *operands))
        assert len(expected) == 2 or operator is SemiJoin


# ── null logical keys and custom-named maps (Copilot, CodeRabbit; PR #351) ──


def test_async_null_logical_key_matches_nothing_as_the_sync_path_does() -> None:
    """A nullable ``Path`` tag: null rows match nothing on either path, no crash."""
    keys: list[Path | None] = [Path("/b"), None, Path("/a")]
    other: list[Path | None] = [None, Path("/a"), Path("/c")]
    sources: list[ArrowTableSource] = []
    for name, values, source_id in (("a", keys, "left"), ("b", other, "right")):
        column = _logical_column(values, Path | None)
        table = pa.Table.from_arrays(
            [pa.array([3, 2, 1], pa.int64()), column, pa.array([3, 2, 1], pa.int16())],
            names=["z" + name, "k", name],
        )
        sources.append(
            ArrowTableSource(table, tag_columns=["z" + name, "k"], source_id=source_id)
        )
    left, right = sources
    for operator in (Join, SemiJoin):
        for operands in ((left, right), (right, left)):
            expected = sync_rows(operator(), *operands)
            assert_equivalent(expected, async_rows(operator(), *operands))
