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
    MapColumns,
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

# The ``semijoin_shared_data_column`` case shares a data column name on purpose; SemiJoin
# warns that it is not a match key (ITL-1088), which is tested in test_semijoin_tag_keys.
pytestmark = pytest.mark.filterwarnings(
    "ignore::orcapod.errors.SemiJoinIgnoredColumnWarning"
)

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
        table.cast(schema),
        tag_columns=list(tags),
        source_id=source_id,
        tag_only_columns=True,
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


def source_nullable_k() -> ArrowTableSource:
    """Tag ``k: int16``, nullable with a null; for a join against a non-nullable ``k``."""
    return _source(
        {
            "k": pa.array([1, None, 3, 4], pa.int16()),
            "kn": pa.array([1, 2, 3, 4], pa.int8()),
            "ks": pa.array(["a", "b", "c", "d"], pa.string()),
        },
        tags=("k",),
        source_id="kn",
        nullable=("k",),
    )


def source_shared_data() -> ArrowTableSource:
    """Tag ``k: int16``; data ``i32`` and ``ls`` named as ``source_b``'s, other values and types."""
    return _source(
        {
            "k": pa.array([3, 1], pa.int16()),
            "i32": pa.array([-1, -2], pa.int64()),
            "ls": pa.array([None, "x"], pa.string()),
        },
        tags=("k",),
        source_id="shared_data",
        nullable=("ls",),
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
    # The tag's Arrow values, not ``as_dict()``: a null nested in a container tag
    # (``[[1], None]``) cannot be read back to Python yet and raises (ITL-1144).
    [row] = tag.as_table().to_pylist()
    return repr(sorted(row.items()))


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
    # A shared tag key nullable in one input only (ITL-829).
    Case("join_nullable_key", Join, lambda: (source_nullable_k(), source_b())),
    Case(
        "join_3_nullable_key",
        Join,
        lambda: (source_c(), source_nullable_k(), source_b()),
    ),
    Case(
        "merge_join_nullable_key",
        MergeJoin,
        lambda: (source_b(nested=False), source_nullable_k()),
    ),
    Case(
        "merge_join",
        MergeJoin,
        lambda: (merge_source("m", 0), merge_source("n", 10)),
    ),
    Case("semijoin", SemiJoin, lambda: (source_c(), source_b(nested=False))),
    Case("semijoin_nested", SemiJoin, lambda: (source_a(), source_b())),
    # A shared tag key nullable in one input only (ITL-829).
    Case(
        "semijoin_left_nullable_key",
        SemiJoin,
        lambda: (source_nullable_k(), source_b(nested=False)),
    ),
    Case(
        "semijoin_right_nullable_key",
        SemiJoin,
        lambda: (source_c(), source_nullable_k()),
    ),
    # A shared data column name is not a key (ITL-1088): only ``k`` decides.
    Case(
        "semijoin_shared_data_column",
        SemiJoin,
        lambda: (source_b(nested=False), source_shared_data()),
    ),
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
    # Tag and data columns in one call (ITL-1160); a logical column among them.
    Case(
        "map_columns",
        lambda: MapColumns({"ta": "tb", "i8": "j8", "p": "q"}),
        lambda: (source_a(),),
    ),
    Case(
        "map_columns_drop_unmapped",
        lambda: MapColumns({"k": "kk", "st": "su", "nul": "n"}, drop_unmapped=True),
        lambda: (source_a(),),
    ),
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


# Every operator, with each input carrying its own meta columns. No operator keeps
# them, on either path (ITL-1080; see ``test_no_operator_carries_raw_stream_meta``).
META_CASES = [pytest.param(_meta_case(c), id=c.label) for c in CASES]


@pytest.mark.parametrize("case", META_CASES)
def test_async_meta_equals_sync(case: Case) -> None:
    """The async path's Data meta columns equal the sync path's: none, on both (ITL-1080)."""
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
    return ArrowTableSource(
        table, tag_columns=["k"], source_id=source_id, tag_only_columns=True
    )


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
    "semantic_v1:1a1287b527e0b08f2bc4af1084a9da9e1e7c1177b73f757ca5c6d742dc832067"
)
PINNED_STREAM_PIPELINE_HASH = (
    "semantic_v1:45e033fc620875ff4704cc3ca60aee3c5f219a664c15f482428cda95bae2588e"
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
    return ArrowTableSource(
        table, tag_columns=["k"], source_id=source_id, tag_only_columns=True
    )


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
    return ArrowTableSource(
        table,
        tag_columns=[id_name, "k"],
        source_id=source_id,
        tag_only_columns=True,
    )


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
        tag_only_columns=True,
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
            ArrowTableSource(
                table,
                tag_columns=["z" + name, "k"],
                source_id=source_id,
                tag_only_columns=True,
            )
        )
    left, right = sources
    for operator in (Join, SemiJoin):
        for operands in ((left, right), (right, left)):
            expected = sync_rows(operator(), *operands)
            assert_equivalent(expected, async_rows(operator(), *operands))


# ── no operator carries its inputs' Data meta columns (ITL-1080, rule B) ────


def _assert_no_input_meta(rows: list[Row]) -> None:
    """No output row carries a Data meta column."""
    for _, data in rows:
        assert data.meta_columns == ()
        assert not [
            name
            for name in data.as_table(all_info=True).column_names
            if "__m" in name or "__computed" in name
        ]


@pytest.mark.parametrize(
    "case", [pytest.param(_meta_case(c), id=c.label) for c in CASES]
)
def test_no_operator_carries_raw_stream_meta(case: Case) -> None:
    """A raw ``ArrowTableStream``'s ``__`` columns reach no operator output, on either path.

    Join's rule (ITL-854/857), generalized to every operator: meta columns describe how
    an input row was produced; an operator's output row was produced by the operator.
    The prediction leaves the ``__`` names out as well.
    """
    sources = case.make_sources()
    expected = sync_rows(case.make_op(), *sources)
    actual = async_rows(case.make_op(), *sources)
    _assert_no_input_meta(expected)
    _assert_no_input_meta(actual)
    _, data_schema = case.make_op().output_schema(*sources)
    assert not [name for name in data_schema if name.startswith("__")]


def cached_meta_source(
    source_id: str, offset: int = 0, mergeable: bool = False
) -> StreamProtocol:
    """A cached function pod's output whose every row's ``Data`` carries ``__computed``.

    Tags ``k: int16`` and ``t: int8``; data ``y: int``, ``yl: list[int]``,
    ``yd: dict[str, int]`` and nullable ``yn: int | None``; only ``y`` and ``yl`` when
    ``mergeable``, the columns ``MergeJoin`` can merge (it cannot sort dicts or nulls).
    """
    source = _source(
        {
            "k": pa.array([1, 2, 3], pa.int16()),
            "t": pa.array([5, 5, 6], pa.int8()),
            "a": pa.array([1, 2, 3], pa.int8()),
        },
        tags=("k", "t"),
        source_id=source_id,
    )

    def outputs(a: int) -> tuple[int, list[int], dict[str, int], int | None]:
        return a + offset, [a, a + 1], {"q": a}, None if a == 2 else a

    def merged_outputs(a: int) -> tuple[int, list[int]]:
        return a + offset, [a, a + 1]

    function_pod = FunctionPod(
        PythonDataFunction(  # pyright: ignore[reportArgumentType]  # PythonDataFunction is not yet typed as matching DataFunctionProtocol (baselined in other tests); it works at runtime
            merged_outputs if mergeable else outputs,
            output_keys=("y", "yl") if mergeable else ("y", "yl", "yd", "yn"),
            function_name=f"outputs_{source_id}",
        )
    )
    pod = CachedFunctionPod(function_pod, result_database=InMemoryArrowDatabase())
    stream = pod(source)
    assert all(data.meta_columns for _, data in stream.iter_data())
    return stream


CACHED_META_CASES = [
    Case("join", Join, lambda: (cached_meta_source("p"), source_b(nested=False))),
    Case(
        "merge_join",
        MergeJoin,
        lambda: (
            cached_meta_source("p", mergeable=True),
            cached_meta_source("q", 10, mergeable=True),
        ),
    ),
    Case("semijoin", SemiJoin, lambda: (cached_meta_source("p"), source_b())),
    Case("batch_2", lambda: Batch(batch_size=2), lambda: (cached_meta_source("p"),)),
    Case("batch_all", Batch, lambda: (cached_meta_source("p"),)),
    Case("group_by", lambda: GroupBy(["t"]), lambda: (cached_meta_source("p"),)),
    Case(
        "polars_filter",
        lambda: PolarsFilter(constraints={"t": 5}),
        lambda: (cached_meta_source("p"),),
    ),
    Case("map_tags", lambda: MapTags({"t": "tb"}), lambda: (cached_meta_source("p"),)),
    Case("map_data", lambda: MapData({"y": "w"}), lambda: (cached_meta_source("p"),)),
    Case(
        "select_tag_columns",
        lambda: SelectTagColumns(["k"]),
        lambda: (cached_meta_source("p"),),
    ),
    Case(
        "select_data_columns",
        lambda: SelectDataColumns(["y"]),
        lambda: (cached_meta_source("p"),),
    ),
    Case(
        "drop_tag_columns",
        lambda: DropTagColumns(["t"]),
        lambda: (cached_meta_source("p"),),
    ),
    Case(
        "drop_data_columns",
        lambda: DropDataColumns(["yl"]),
        lambda: (cached_meta_source("p"),),
    ),
    Case("index", lambda: Index("yl", 0), lambda: (cached_meta_source("p"),)),
    Case("pick", lambda: Pick("yd", "q"), lambda: (cached_meta_source("p"),)),
    Case("drop_nulls", lambda: DropNulls("yn"), lambda: (cached_meta_source("p"),)),
    Case("drop_nulls_all", DropNulls, lambda: (cached_meta_source("p"),)),
]


@pytest.mark.parametrize(
    "case", CACHED_META_CASES, ids=[c.label for c in CACHED_META_CASES]
)
def test_no_operator_carries_cached_pod_meta(case: Case) -> None:
    """A cached pod's ``__computed`` reaches no operator output, on either path.

    The stream hides the column in ``as_table()`` but every row's ``Data`` carries it,
    so a per-row async path used to keep it while the sync path lost it.
    """
    sources = case.make_sources()
    expected = sync_rows(case.make_op(), *sources)
    actual = async_rows(case.make_op(), *sources)
    assert expected
    _assert_no_input_meta(expected)
    _assert_no_input_meta(actual)
    assert_equivalent(expected, actual)
    assert_same_meta(expected, actual)


# ── a selection that would leave no data column (ITL-1080) ──────────────────

NO_DATA_COLUMN_OPS = {
    "drop_every_data_column": lambda: DropDataColumns(["i32", "u8", "f32", "ls"]),
    "drop_every_data_column_not_strict": lambda: DropDataColumns(
        ["i32", "u8", "f32", "ls", "absent"], strict=False
    ),
    "select_no_data_column_not_strict": lambda: SelectDataColumns(
        ["absent"], strict=False
    ),
}


@pytest.mark.parametrize("make_op", NO_DATA_COLUMN_OPS.values(), ids=NO_DATA_COLUMN_OPS)
def test_selection_leaving_no_data_column_raises_alike_on_both_paths(
    make_op: Callable[[], StaticOutputOperatorPod],
) -> None:
    """A stream needs a data column: both paths refuse a result without one.

    The sync path raises when the operator is applied, ``op(stream)``; the async
    path on the first row, with the same error. Before, the sync path raised only
    when the rows were read and the async path emitted rows with empty ``Data``.
    """
    source = source_b(nested=False)
    with pytest.raises(InputValidationError, match="no data column") as sync_error:
        _ = make_op()(source)
    with pytest.raises(InputValidationError, match="no data column") as async_error:
        _ = async_rows(make_op(), source)
    assert str(async_error.value) == str(sync_error.value)


def test_selection_leaving_only_meta_columns_raises() -> None:
    """A raw stream's ``__`` columns are not data: they are not carried (ITL-1080)."""
    source = _with_meta(source_b(nested=False), _meta_columns("m", 3))
    op = DropDataColumns(["i32", "u8", "f32", "ls"])
    with pytest.raises(InputValidationError, match="no data column"):
        _ = op(source)
    with pytest.raises(InputValidationError, match="no data column"):
        _ = async_rows(DropDataColumns(["i32", "u8", "f32", "ls"]), source)


# ── a tag named with the meta prefix (ITL-1080) ──────────────────────────────
#
# A ``__`` name is a meta column: a datagram keeps it apart from its tags, so a tag with
# such a name would stop being a tag on every path that rebuilds rows. No route may
# produce one.


def _table_with_meta_named_column() -> pa.Table:
    return pa.table(
        {
            "k": pa.array([1, 2], pa.int16()),
            "__g": pa.array([1, 1], pa.int8()),
            "x": pa.array([3, 4], pa.int8()),
        }
    )


def test_a_stream_refuses_a_tag_named_with_the_meta_prefix() -> None:
    with pytest.raises(InputValidationError, match="'__g'.*reserved for meta"):
        _ = ArrowTableStream(_table_with_meta_named_column(), tag_columns=["k", "__g"])


def test_a_source_refuses_a_tag_named_with_the_meta_prefix() -> None:
    """Not "tag column not found": the source would drop the meta column first."""
    with pytest.raises(InputValidationError, match="'__g'.*reserved for meta"):
        _ = ArrowTableSource(
            _table_with_meta_named_column(),
            tag_columns=["k", "__g"],
            source_id="s",
            tag_only_columns=True,
        )


def test_map_tags_refuses_a_meta_named_target() -> None:
    """Refused when built, so neither path nor the prediction can produce one."""
    with pytest.raises(InputValidationError, match="'__t'.*reserved for meta"):
        _ = MapTags({"ta": "__t"})


def test_join_keeps_a_tag_and_drops_the_data_meta_columns() -> None:
    """Join drops only data meta columns; a tag is never one (it once dropped by prefix).

    A stream with a ``__`` tag cannot be built, so the one case left is data meta
    next to ordinary tags: the tags all survive, the meta columns do not.
    """
    left = _with_meta(source_a(), _meta_columns("m", 3))
    stream = Join()(left, source_b(nested=False))
    tag_keys, data_keys = stream.keys()
    assert set(tag_keys) == {"k", "ta"}
    assert not [name for name in data_keys if name.startswith("__")]
    assert not [
        name for name in stream.as_table(all_info=True).column_names if "__m" in name
    ]


def test_map_data_does_not_rename_a_meta_column_into_data() -> None:
    """A raw stream's ``__`` column is meta: ``MapData`` renames it on neither path.

    The sync path used to build its renames from ``keys()``, which on a raw stream
    lists ``__`` columns, so the meta value reached a data column (CodeRabbit, #369).
    """
    source = _with_meta(source_a(), _meta_columns("m", 3))
    expected = sync_rows(MapData({"__m_i8": "renamed"}, strict=False), source)
    actual = async_rows(MapData({"__m_i8": "renamed"}, strict=False), source)
    for rows in (expected, actual):
        for _, data in rows:
            assert "renamed" not in data.keys()
    assert_equivalent(expected, actual)
    op = MapData({"__m_i8": "renamed"}, strict=False)
    predicted = op.output_schema(source)
    assert "renamed" not in predicted[1]
    # Non-public on purpose: a prediction is verified against the static result.
    assert predicted == op.static_process(source).output_schema()


def test_map_data_meta_source_does_not_collide() -> None:
    """A meta source is not a rename source, so it cannot collide with a data column."""
    source = _with_meta(source_b(nested=False), _meta_columns("m", 3))
    op = MapData({"__m_i8": "i32", "u8": "v8"}, strict=False)
    expected = sync_rows(op, source)
    actual = async_rows(MapData({"__m_i8": "i32", "u8": "v8"}, strict=False), source)
    for _, data in expected:
        assert set(data.keys()) == {"i32", "v8", "f32", "ls"}
    assert_equivalent(expected, actual)


@pytest.mark.parametrize(
    "name_map",
    [{"absent": "a"}, {"__m_i8": "renamed"}],
    ids=["absent_source", "meta_source"],
)
def test_map_data_dropping_every_data_column_raises_alike(
    name_map: dict[str, str],
) -> None:
    """``drop_unmapped`` with no applicable source would leave no data column.

    Before, both paths kept every data column while the prediction listed none.
    """
    source = _with_meta(source_b(nested=False), _meta_columns("m", 3))
    with pytest.raises(InputValidationError, match="no data column") as sync_error:
        _ = MapData(name_map, drop_unmapped=True, strict=False)(source)
    with pytest.raises(InputValidationError, match="no data column") as async_error:
        _ = async_rows(MapData(name_map, drop_unmapped=True, strict=False), source)
    assert str(async_error.value) == str(sync_error.value)


def test_map_tags_dropping_every_tag_agrees_on_both_paths() -> None:
    """``MapTags`` with ``drop_unmapped`` and no applicable source drops every tag.

    A stream may have no tags, so this is not an error. Before, the async path kept
    every tag while the sync path and the prediction dropped them.
    """
    source = source_a()
    op = MapTags({"absent": "a"}, drop_unmapped=True, strict=False)
    expected = sync_rows(op, source)
    actual = async_rows(
        MapTags({"absent": "a"}, drop_unmapped=True, strict=False), source
    )
    assert op.output_schema(source)[0] == {}
    for rows in (expected, actual):
        assert len(rows) == 3
        for tag, _ in rows:
            assert tag.keys() == ()
    for (tag, data), (sync_tag, sync_data) in zip(actual, expected):
        assert data.content_hash() == sync_data.content_hash()
        assert list(tag.system_tags().items()) == list(sync_tag.system_tags().items())


def test_map_data_refuses_a_meta_named_target() -> None:
    """A data column never gets a meta column's name: refused when built."""
    with pytest.raises(InputValidationError, match="'__x'.*reserved for meta"):
        _ = MapData({"i8": "__x"})


def test_semijoin_does_not_match_on_a_meta_column() -> None:
    """A ``__`` column both raw inputs carry is not a match key, on either path."""
    left = _with_meta(source_c(), {"__shared": pa.array([1, 2, 3], pa.int8())})
    right = _with_meta(
        source_b(nested=False), {"__shared": pa.array([9, 9, 9], pa.int8())}
    )
    expected = sync_rows(SemiJoin(), left, right)
    op = SemiJoin()
    _ = op(left, right)  # records the common keys, as an operator node does
    actual = async_rows(op, left, right)
    assert len(expected) == len(actual) == 3
    assert_equivalent(expected, actual)


def test_without_meta_fields_keeps_optional_fields() -> None:
    """Dropping the meta fields leaves the optionality of the others (CodeRabbit, #369)."""
    from orcapod.core.operators.meta_columns import without_meta_fields
    from orcapod.types import Schema

    schema = Schema(
        {"x": int, "y": int, "__note": str}, optional_fields={"x", "__note"}
    )
    kept = without_meta_fields(schema)
    assert dict(kept) == {"x": int, "y": int}
    assert kept.optional_fields == frozenset({"x"})


@pytest.mark.parametrize(
    "make_op",
    [
        lambda: SelectDataColumns(["i32", "__note"]),
        lambda: DropDataColumns(["__note"]),
    ],
    ids=["select", "drop"],
)
def test_strict_selection_naming_a_meta_column_raises_alike(
    make_op: Callable[[], StaticOutputOperatorPod],
) -> None:
    """A raw stream's ``__`` column is not a data column for a strict selection.

    The sync path found it in ``keys()``; the async path, whose ``Data`` keeps it as
    meta, reported it missing (CodeRabbit, #369). Both now report it missing.
    """
    source = _with_meta(
        source_b(nested=False), {"__note": pa.array(["a", "b", "c"], pa.string())}
    )
    with pytest.raises(
        InputValidationError, match="Missing data columns"
    ) as sync_error:
        _ = make_op()(source)
    with pytest.raises(
        InputValidationError, match="Missing data columns"
    ) as async_error:
        _ = async_rows(make_op(), source)
    assert str(async_error.value) == str(sync_error.value)
