"""Index and Pick must preserve the input's Arrow types (ITL-834).

Whatever Arrow types the inputs have are what is respected: every column the
operator does not touch keeps its exact input Arrow type (tags and data, empty
and non-empty results, every miss policy), and the extracted column's Arrow
type is derived from the input column's Arrow type instead of being inferred
from Python values.

Layout-only normalization (``string`` -> ``large_string``, ``binary`` ->
``large_binary``, ``list`` -> ``large_list``) happens for every operator when a
row becomes a datagram and does not change content hashes; it is out of scope
for ITL-834, so expected types are the large-normalized input types.
"""

from __future__ import annotations

import inspect
import typing
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, TypeAlias

import pyarrow as pa
import pytest

from orcapod import contexts
from orcapod.channels import Channel
from orcapod.contexts import DataContext, get_default_type_converter
from orcapod.core.operators import (
    DropDataColumns,
    DropTagColumns,
    Index,
    MapData,
    MapTags,
    Pick,
    SelectDataColumns,
    SelectTagColumns,
)
from orcapod.core.operators.base import UnaryOperator
from orcapod.core.sources import ArrowTableSource
from orcapod.protocols.core_protocols import DataProtocol, StreamProtocol, TagProtocol
from orcapod.types import DataType
from orcapod.utils.arrow_utils import normalize_schema_to_large_types

TAG = "tg"
TAG8 = "tg8"
TAGS = (TAG, TAG8)

Column: TypeAlias = "pa.Array[Any] | pa.ChunkedArray[Any]"
Row = tuple[TagProtocol, DataProtocol]


def _path_column(n: int) -> pa.ChunkedArray[Any]:
    """Build a column of ``n`` ``Path`` values in orcapod's logical Arrow type."""
    converter = get_default_type_converter()
    return converter.python_dicts_to_arrow_table(
        [{"p": Path(f"/{i}")} for i in range(n)], python_schema={"p": Path}
    ).column("p")


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


# The struct's string field is already large: normalizing a plain ``string``
# field yields a distinct dynamic TypedDict, a layout-only effect out of scope here.
STRUCT = pa.struct([("x", pa.int8()), ("y", pa.large_string())])


def _base_columns() -> dict[str, Column]:
    """Two rows of every column type the operators must pass through untouched."""
    return {
        TAG: pa.array([1, 2], pa.int16()),
        TAG8: pa.array([5, 6], pa.int8()),
        "i8": pa.array([1, 2], pa.int8()),
        "i16": pa.array([3, 4], pa.int16()),
        "i32": pa.array([5, 6], pa.int32()),
        "u8": pa.array([7, 8], pa.uint8()),
        "f32": pa.array([1.5, 2.5], pa.float32()),
        "s": pa.array(["a", "b"], pa.string()),
        "ls": pa.array(["a", "b"], pa.large_string()),
        "bn": pa.array([b"x", b"y"], pa.binary()),
        "l8": pa.array([[1], [2, 3]], pa.list_(pa.int8())),
        "st": pa.array([{"x": 1, "y": "a"}, {"x": 2, "y": "b"}], STRUCT),
        "p": _path_column(2),
    }


UNTOUCHED_DATA = tuple(c for c in _base_columns() if c not in TAGS)


def _source(**targets: Column) -> ArrowTableSource:
    """Build a non-nullable source of the base columns plus ``targets``."""
    columns = {**_base_columns(), **targets}
    table = pa.table(list(columns.values()), names=list(columns))
    table = table.cast(pa.schema([f.with_nullable(False) for f in table.schema]))
    return ArrowTableSource(table, tag_columns=list(TAGS), tag_only_columns=True)


def _index_source(hit: bool = True) -> ArrowTableSource:
    """Build an ``Index`` source whose ``lst`` rows hit or miss index 0."""
    values = [[7, 8], [9, 10]] if hit else [[], []]
    return _source(lst=pa.array(values, pa.list_(pa.int8())))


def _pick_source(hit: bool = True) -> ArrowTableSource:
    """Build a ``Pick`` source whose ``d`` rows hit or miss key ``k``."""
    key = "k" if hit else "other"
    rows = [[{"key": key, "value": 1}], [{"key": key, "value": 2}]]
    return _source(d=pa.array(rows, _dict_type(pa.int16())))


def _types(table: pa.Table) -> dict[str, pa.DataType]:
    """Map each column of ``table`` to its Arrow type."""
    return {f.name: f.type for f in table.schema}


def _input_types(stream: StreamProtocol) -> dict[str, pa.DataType]:
    """Map each input column to its large-normalized Arrow type."""
    schema = normalize_schema_to_large_types(stream.as_table().schema)
    return {f.name: f.type for f in schema}


def _large(t: pa.DataType) -> pa.DataType:
    return (
        normalize_schema_to_large_types(pa.schema([pa.field("v", t)])).field("v").type
    )


@dataclass(frozen=True)
class OpCase:
    """A projection operator under test and the source shaped for it."""

    label: str
    make_op: Callable[..., UnaryOperator]
    make_source: Callable[[bool], ArrowTableSource]
    target: str
    extracted: pa.DataType


OP_CASES = [
    OpCase(
        "index", lambda **kw: Index("lst", 0, **kw), _index_source, "lst", pa.int8()
    ),
    OpCase("pick", lambda **kw: Pick("d", "k", **kw), _pick_source, "d", pa.int16()),
]
OP = pytest.mark.parametrize("case", OP_CASES, ids=[c.label for c in OP_CASES])
OUT = pytest.mark.parametrize("out", [None, "x"], ids=["in_place", "out"])
MISS = pytest.mark.parametrize("fail_on_miss", [False, True], ids=["skip", "fail"])
COLUMN = pytest.mark.parametrize("column", [*TAGS, *UNTOUCHED_DATA])


def _rows_by_tag(stream: StreamProtocol) -> dict[Any, Row]:
    """Index the stream's rows by their ``TAG`` value."""
    return {tag[TAG]: (tag, data) for tag, data in stream.iter_data()}


# ── non-empty output ────────────────────────────────────────────────────────


@OP
@OUT
@MISS
@COLUMN
def test_untouched_column_keeps_arrow_type(
    case: OpCase, out: str | None, fail_on_miss: bool, column: str
) -> None:
    """Every untouched tag and data column keeps exactly its input Arrow type."""
    source = case.make_source(True)
    result = case.make_op(out=out, fail_on_miss=fail_on_miss)(source).as_table()
    assert _types(result)[column] == _input_types(source)[column]


@OP
@MISS
def test_original_column_kept_by_out_form_keeps_arrow_type(
    case: OpCase, fail_on_miss: bool
) -> None:
    """With ``out=``, the projected-from column itself passes through untouched."""
    source = case.make_source(True)
    result = case.make_op(out="x", fail_on_miss=fail_on_miss)(source).as_table()
    assert _types(result)[case.target] == _input_types(source)[case.target]


@OP
@OUT
def test_extracted_column_type_derived_from_input(
    case: OpCase, out: str | None
) -> None:
    """The extracted column takes the input's element / value Arrow type."""
    result = case.make_op(out=out)(case.make_source(True)).as_table()
    assert result.schema.field(out or case.target).type == case.extracted


@OP
@OUT
@MISS
def test_untouched_data_content_hash_unchanged(
    case: OpCase, out: str | None, fail_on_miss: bool
) -> None:
    """Per row, the content hash of the untouched data columns equals the input's."""
    source = case.make_source(True)
    before = _rows_by_tag(source)
    after = _rows_by_tag(case.make_op(out=out, fail_on_miss=fail_on_miss)(source))
    assert before.keys() == after.keys()
    keep = [*UNTOUCHED_DATA, *([case.target] if out is not None else [])]
    for key, (_, data) in after.items():
        in_data = before[key][1]
        assert data.select(*keep).content_hash() == in_data.select(*keep).content_hash()


@OP
@OUT
def test_materialized_schema_matches_prediction(case: OpCase, out: str | None) -> None:
    """The materialized schema (names, order, Python types) equals ``output_schema()``."""
    source = case.make_source(True)
    op = case.make_op(out=out)
    op.validate_inputs(source)
    predicted = op.output_schema(source)
    materialized = op.unary_static_process(source)
    assert materialized.output_schema() == predicted
    table = materialized.as_table()
    assert table.column_names == [*predicted[0], *predicted[1]]
    converter = get_default_type_converter()
    data_table = table.drop_columns(list(predicted[0]))
    assert dict(converter.arrow_schema_to_python_schema(data_table.schema)) == dict(
        predicted[1]
    )


# ── extracted element types ─────────────────────────────────────────────────


@dataclass(frozen=True)
class ElementCase:
    """An element / value Arrow type and two values of it."""

    arrow_type: pa.DataType
    values: tuple[Any, Any]


ELEMENT_CASES = [
    ElementCase(pa.int8(), (1, 2)),
    ElementCase(pa.int16(), (1, 2)),
    ElementCase(pa.int32(), (1, 2)),
    ElementCase(pa.uint8(), (1, 2)),
    ElementCase(pa.float32(), (1.5, 2.5)),
    ElementCase(pa.string(), ("a", "b")),
    ElementCase(pa.binary(), (b"a", b"b")),
    ElementCase(pa.list_(pa.int8()), ([1], [2])),
]
ELEMENT = pytest.mark.parametrize(
    "element", ELEMENT_CASES, ids=[str(c.arrow_type) for c in ELEMENT_CASES]
)


@ELEMENT
@OUT
def test_index_element_arrow_type(element: ElementCase, out: str | None) -> None:
    """Index of ``list<T>`` yields a column of type ``T`` for every element type."""
    a, b = element.values
    source = _source(lst=pa.array([[a, b], [b]], pa.list_(element.arrow_type)))
    result = Index("lst", 0, out=out)(source).as_table()
    assert result.schema.field(out or "lst").type == _large(element.arrow_type)
    assert result.column(out or "lst").to_pylist() == [a, b]


def test_index_logical_element_arrow_type() -> None:
    """Index of ``list[Path]`` yields the ``Path`` logical (extension) type."""
    converter = get_default_type_converter()
    lists = converter.python_dicts_to_arrow_table(
        [{"lp": [Path("/a"), Path("/b")]}, {"lp": [Path("/c")]}],
        python_schema={"lp": list[Path]},
    ).column("lp")
    source = _source(lp=lists)
    result = Index("lp", 0)(source).as_table()
    assert result.schema.field("lp").type == _input_types(source)["p"]
    rows = [data["lp"] for _, data in Index("lp", 0)(source).iter_data()]
    assert rows == [Path("/a"), Path("/c")]


@ELEMENT
@OUT
def test_pick_value_arrow_type(element: ElementCase, out: str | None) -> None:
    """Pick from ``dict[str, V]`` yields a column of type ``V`` for every value type."""
    a, b = element.values
    rows = [[{"key": "k", "value": a}], [{"key": "k", "value": b}]]
    source = _source(d=pa.array(rows, _dict_type(element.arrow_type)))
    result = Pick("d", "k", out=out)(source).as_table()
    assert result.schema.field(out or "d").type == _large(element.arrow_type)
    assert result.column(out or "d").to_pylist() == [a, b]


# ── all rows skipped ────────────────────────────────────────────────────────


@OP
@OUT
@COLUMN
def test_empty_result_keeps_arrow_types(
    case: OpCase, out: str | None, column: str
) -> None:
    """When every row misses, the empty result still carries the input's types."""
    source = case.make_source(False)
    result = case.make_op(out=out)(source).as_table()
    assert result.num_rows == 0
    assert _types(result)[column] == _input_types(source)[column]


@OP
@OUT
def test_empty_result_extracted_type_and_prediction(
    case: OpCase, out: str | None
) -> None:
    """The empty result has the derived extracted type and matches ``output_schema()``."""
    source = case.make_source(False)
    op = case.make_op(out=out)
    op.validate_inputs(source)
    predicted = op.output_schema(source)
    materialized = op.unary_static_process(source)
    table = materialized.as_table()
    assert table.schema.field(out or case.target).type == case.extracted
    assert materialized.output_schema() == predicted
    assert table.column_names == [*predicted[0], *predicted[1]]


@OP
@OUT
def test_empty_and_non_empty_results_share_schema(
    case: OpCase, out: str | None
) -> None:
    """An all-skipped result has the same tag and data Arrow types as a non-empty one."""
    hit = case.make_op(out=out)(case.make_source(True)).as_table()
    miss = case.make_op(out=out)(case.make_source(False)).as_table()
    assert _types(miss) == _types(hit)
    assert miss.column_names == hit.column_names


@OP
@OUT
def test_fail_on_miss_raises(case: OpCase, out: str | None) -> None:
    """``fail_on_miss=True`` raises instead of producing an empty result."""
    with pytest.raises(RuntimeError, match="fail_on_miss=True"):
        _ = case.make_op(out=out, fail_on_miss=True)(case.make_source(False)).as_table()


# ── extracted nullability: stable per column, equal to the prediction ──────


def _index_null_source(null_first: bool) -> ArrowTableSource:
    """Build an ``Index`` source whose element 0 is ``None`` in one row only."""
    values = [[None, 1], [5]] if null_first else [[5], [None]]
    return _source(lst=pa.array(values, pa.list_(pa.int8())))


def _pick_null_source(null_first: bool) -> ArrowTableSource:
    """Build a ``Pick`` source whose value at key ``k`` is ``None`` in one row only."""
    rows: list[list[dict[str, Any]]] = [
        [{"key": "k", "value": None}],
        [{"key": "k", "value": 2}],
    ]
    if not null_first:
        rows.reverse()
    return _source(d=pa.array(rows, _dict_type(pa.int16())))


@dataclass(frozen=True)
class NullCase:
    """An operator, a source with one ``None`` extraction, and the expected error."""

    label: str
    make_op: Callable[..., UnaryOperator]
    make_source: Callable[[bool], ArrowTableSource]
    message: str


NULL_CASES = [
    NullCase(
        "index",
        lambda **kw: Index("lst", 0, **kw),
        _index_null_source,
        "Index: element 0 of column 'lst' is None",
    ),
    NullCase(
        "pick",
        lambda **kw: Pick("d", "k", **kw),
        _pick_null_source,
        "Pick: value of key 'k' in column 'd' is None",
    ),
]
NULL_OP = pytest.mark.parametrize("case", NULL_CASES, ids=[c.label for c in NULL_CASES])
NULL_FIRST = pytest.mark.parametrize(
    "null_first", [True, False], ids=["null_first", "null_last"]
)


def _null_row_pattern(case: NullCase, null_first: bool) -> str:
    """Regex for the error: operator, column, and the tag of the ``None`` row."""
    tag_value = 1 if null_first else 2
    return rf"{case.message} in the row with tag \{{.*'{TAG}': {tag_value}"


@NULL_OP
@OUT
@MISS
@NULL_FIRST
def test_none_extraction_raises_value_error(
    case: NullCase, out: str | None, fail_on_miss: bool, null_first: bool
) -> None:
    """A ``None`` element / value raises a clear ``ValueError``, not ``ArrowInvalid``.

    Mixing a ``None`` row with a non-null row used to give the rows different
    nullability, so materializing failed in ``pa.concat_tables``.
    """
    op = case.make_op(out=out, fail_on_miss=fail_on_miss)
    with pytest.raises(ValueError, match=_null_row_pattern(case, null_first)):
        _ = op(case.make_source(null_first)).as_table()


@NULL_OP
@OUT
@pytest.mark.asyncio
async def test_none_extraction_raises_value_error_streaming(
    case: NullCase, out: str | None
) -> None:
    """The streaming path raises the same ``ValueError``."""
    with pytest.raises(ValueError, match=_null_row_pattern(case, True)):
        _ = await _run_async(case.make_op(out=out), case.make_source(True))


@OP
@OUT
def test_extracted_column_nullability_matches_prediction(
    case: OpCase, out: str | None
) -> None:
    """Every result marks the extracted column ``NOT NULL``, as ``int`` predicts.

    The predicted type of an element / value is never Optional, so the column
    is non-nullable in the non-empty result, in each row, and in the
    all-skipped result alike.
    """
    target = out or case.target
    op = case.make_op(out=out)
    source = case.make_source(True)
    assert type(None) not in typing.get_args(op.output_schema(source)[1][target])
    hit = op(source).as_table()
    miss = case.make_op(out=out)(case.make_source(False)).as_table()
    assert not hit.schema.field(target).nullable
    assert not miss.schema.field(target).nullable
    for _, data in op(source).iter_data():
        assert not data.arrow_schema().field(target).nullable


# ── streaming path and a generic guard for row-wise operators ──────────────


async def _run_async(op: UnaryOperator, source: StreamProtocol) -> list[Row]:
    """Run ``op`` over ``source`` through the async channel path."""
    op.validate_inputs(source)
    input_ch: Channel[Row] = Channel(buffer_size=16)
    output_ch: Channel[Row] = Channel(buffer_size=16)
    for tag, data in source.iter_data():
        await input_ch.writer.send((tag, data))
    await input_ch.writer.close()
    await op.async_execute(
        [input_ch.reader],
        output_ch.writer,
        input_pipeline_hashes=[source.pipeline_hash()],
    )
    return await output_ch.reader.collect()


@OP
@OUT
@pytest.mark.asyncio
async def test_streaming_path_preserves_types(case: OpCase, out: str | None) -> None:
    """``async_execute`` keeps untouched types and derives the extracted type."""
    source = case.make_source(True)
    expected = _input_types(source)
    rows = await _run_async(case.make_op(out=out), source)
    assert len(rows) == 2
    for tag, data in rows:
        tag_schema = tag.arrow_schema()
        for name in TAGS:
            assert tag_schema.field(name).type == expected[name], name
        data_schema = data.arrow_schema()
        for name in UNTOUCHED_DATA:
            assert data_schema.field(name).type == expected[name], name
        assert data_schema.field(out or case.target).type == case.extracted


@dataclass(frozen=True)
class RowWiseCase:
    """A row-wise operator, its source, and the columns it changes or renames."""

    label: str
    make_op: Callable[[], UnaryOperator]
    make_source: Callable[[], ArrowTableSource]
    changed: frozenset[str] = frozenset()
    renames: dict[str, str] = field(default_factory=dict[str, str])


# Every row-wise operator whose per-row path mutates datagrams. Adding an
# operator here covers it automatically.
ROW_WISE_CASES = [
    RowWiseCase("index", lambda: Index("lst", 0), _index_source, frozenset({"lst"})),
    RowWiseCase("index_out", lambda: Index("lst", 0, out="x"), _index_source),
    RowWiseCase("pick", lambda: Pick("d", "k"), _pick_source, frozenset({"d"})),
    RowWiseCase("pick_out", lambda: Pick("d", "k", out="x"), _pick_source),
    RowWiseCase(
        "map_data",
        lambda: MapData({"i8": "j8"}),
        _index_source,
        renames={"i8": "j8"},
    ),
    RowWiseCase(
        "map_tags",
        lambda: MapTags({TAG8: "t8"}),
        _index_source,
        renames={TAG8: "t8"},
    ),
    RowWiseCase(
        "select_data",
        lambda: SelectDataColumns(["i8", "f32", "st", "p"]),
        _index_source,
    ),
    RowWiseCase("drop_data", lambda: DropDataColumns(["i16"]), _index_source),
    RowWiseCase("select_tags", lambda: SelectTagColumns([TAG8]), _index_source),
    RowWiseCase("drop_tags", lambda: DropTagColumns([TAG]), _index_source),
]
ROW_WISE = pytest.mark.parametrize(
    "case", ROW_WISE_CASES, ids=[c.label for c in ROW_WISE_CASES]
)


def _assert_pass_through(
    case: RowWiseCase,
    expected: dict[str, pa.DataType],
    actual: dict[str, pa.DataType],
) -> None:
    """Assert every untouched column keeps its input type (width, not layout)."""
    # Width, not layout: Arrow-native operators keep ``string`` where the
    # datagram path normalizes to ``large_string``; both hash the same.
    checked = 0
    for name, in_type in expected.items():
        out_name = case.renames.get(name, name)
        if name in case.changed or out_name not in actual:
            continue
        assert _large(actual[out_name]) == in_type, name
        checked += 1
    assert checked > 0


@ROW_WISE
def test_row_wise_operator_pass_through_types(case: RowWiseCase) -> None:
    """A row-wise operator keeps the Arrow type of every column it passes through."""
    source = case.make_source()
    result = case.make_op()(source).as_table()
    _assert_pass_through(case, _input_types(source), _types(result))


@ROW_WISE
@pytest.mark.asyncio
async def test_row_wise_operator_streaming_pass_through_types(
    case: RowWiseCase,
) -> None:
    """The streaming path of a row-wise operator keeps pass-through Arrow types."""
    source = case.make_source()
    expected = _input_types(source)
    rows = await _run_async(case.make_op(), source)
    assert rows
    for tag, data in rows:
        actual = {
            **{f.name: f.type for f in tag.arrow_schema()},
            **{f.name: f.type for f in data.arrow_schema()},
        }
        _assert_pass_through(case, expected, actual)


# ── context use: the stream's context is read once, at validation ──────────
# ``async_execute`` has no stream, so Index and Pick resolve the fallback
# element Arrow type (needed only for logical input columns) from the stream's
# context in ``validate_unary_input`` and keep only that resolved type.


def _list_path_source() -> ArrowTableSource:
    """Build a source whose ``lp`` column is ``list[Path]`` (a logical type)."""
    lists = get_default_type_converter().python_dicts_to_arrow_table(
        [{"lp": [Path("/a"), Path("/b")]}, {"lp": [Path("/c")]}],
        python_schema={"lp": list[Path]},
    )
    return _source(lp=lists.column("lp"))


@pytest.mark.asyncio
async def test_index_logical_element_arrow_type_streaming() -> None:
    """The streaming path of Index on ``list[Path]`` yields the ``Path`` logical type."""
    source = _list_path_source()
    rows = await _run_async(Index("lp", 0), source)
    expected = _input_types(source)["p"]
    assert [data["lp"] for _, data in rows] == [Path("/a"), Path("/c")]
    for _, data in rows:
        assert data.arrow_schema().field("lp").type == expected


CONTEXT_CASES = pytest.mark.parametrize(
    ("make_op", "make_source"),
    [
        (lambda: Index("lst", 0), _index_source),
        (lambda: Index("lp", 0), _list_path_source),
        (lambda: Pick("d", "k"), _pick_source),
    ],
    ids=["index", "index-logical", "pick"],
)


@CONTEXT_CASES
def test_no_converter_or_context_held_after_validation(
    make_op: Callable[[], UnaryOperator],
    make_source: Callable[[], ArrowTableSource],
) -> None:
    """Validation keeps resolved types only, never a converter or a context.

    The only context-like attribute is ``_data_context``, which every operator
    inherits from its base class; Index and Pick add none of their own.
    """
    op = make_op()
    op.validate_inputs(make_source())
    attributes: dict[str, object] = vars(op)
    held = [
        name
        for name, value in attributes.items()
        if isinstance(value, DataContext) or hasattr(value, "python_type_to_arrow_type")
    ]
    assert held == ["_data_context"]


_OPERATOR_MODULES = frozenset({Index.__module__, Pick.__module__})


def _forbid_row_time_context(
    op: UnaryOperator, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Make any context read by ``op``'s own code fail from now on.

    Reading the operator's own context, or resolving one from the Index or
    Pick modules, raises ``AssertionError``. Datagrams and streams may still
    use their own contexts.
    """

    def forbidden(_self: object) -> object:
        raise AssertionError(f"{type(op).__name__} read its own context at row time")

    monkeypatch.setattr(type(op), "data_context", property(forbidden))
    monkeypatch.setattr(type(op), "data_context_key", property(forbidden))
    original = contexts.resolve_context

    def guarded(
        context_info: str | DataContext | None = None,
    ) -> DataContext:
        caller = inspect.currentframe()
        module = (
            caller.f_back.f_globals.get("__name__")
            if caller and caller.f_back
            else None
        )
        if module in _OPERATOR_MODULES:
            raise AssertionError(f"{module} resolved a context at row time")
        return original(context_info)

    monkeypatch.setattr(contexts, "resolve_context", guarded)


@CONTEXT_CASES
@pytest.mark.asyncio
async def test_row_time_code_never_reads_a_context(
    make_op: Callable[[], UnaryOperator],
    make_source: Callable[[], ArrowTableSource],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """After validation, sync and streaming rows use only the types resolved there."""
    source = make_source()
    op = make_op()
    op.validate_inputs(source)
    _forbid_row_time_context(op, monkeypatch)
    sync_rows = list(op.unary_static_process(source).iter_data())
    input_ch: Channel[Row] = Channel(buffer_size=16)
    output_ch: Channel[Row] = Channel(buffer_size=16)
    for row in source.iter_data():
        await input_ch.writer.send(row)
    await input_ch.writer.close()
    await op.async_execute(
        [input_ch.reader],
        output_ch.writer,
        input_pipeline_hashes=[source.pipeline_hash()],
    )
    async_rows = await output_ch.reader.collect()
    assert len(sync_rows) == len(async_rows) == 2


def test_pick_fallback_never_reads_a_context(monkeypatch: pytest.MonkeyPatch) -> None:
    """Pick's fallback value type, unreachable through sources, reads no context."""
    op = Pick("d", "k")
    op.validate_inputs(_pick_source())
    _forbid_row_time_context(op, monkeypatch)
    struct_type = pa.struct([("k", pa.int16())])
    assert op._value_arrow_type(struct_type) == pa.int64()  # pyright: ignore[reportPrivateUsage]  # fallback is unreachable publicly


def _validate_with_unmappable(
    op: UnaryOperator,
    source: ArrowTableSource,
    unmappable: DataType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Validate ``op`` while the stream's converter cannot map ``unmappable``."""
    converter = source.data_context.type_converter
    original = converter.python_type_to_arrow_type

    def fake(python_type: DataType) -> pa.DataType:
        if python_type is unmappable:
            raise ValueError(f"Unsupported Python type: {python_type!r}")
        return original(python_type)

    monkeypatch.setattr(converter, "python_type_to_arrow_type", fake)
    op.validate_inputs(source)
    monkeypatch.undo()


def test_unmappable_fallback_is_not_raised_when_not_needed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An unmappable output type raises neither at validation nor on Arrow-typed rows."""
    source = _index_source()
    op = Index("lst", 0)
    _validate_with_unmappable(op, source, int, monkeypatch)
    result = op.unary_static_process(source)
    assert [data["lst"] for _, data in result.iter_data()] == [7, 9]


def test_unmappable_fallback_raises_where_needed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A logical column whose element type is unmappable raises a clear error at use."""
    source = _list_path_source()
    op = Index("lp", 0)
    _validate_with_unmappable(op, source, Path, monkeypatch)
    with pytest.raises(ValueError, match=r"Index: .*'lp'.*Path"):
        _ = op.unary_static_process(source)


def test_pick_fallback_value_type_resolved_at_validation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Pick's fallback value type comes from validation; an unmappable one raises at use.

    No registered logical type implements ``pick_field`` yet, so the fallback is
    reached here through the helper directly.
    """
    struct_type = pa.struct([("k", pa.int16())])
    op = Pick("d", "k")
    op.validate_inputs(_pick_source())
    assert op._value_arrow_type(struct_type) == pa.int64()  # pyright: ignore[reportPrivateUsage]  # fallback is unreachable publicly

    unmappable = Pick("d", "k")
    _validate_with_unmappable(unmappable, _pick_source(), int, monkeypatch)
    with pytest.raises(ValueError, match=r"Pick: .*'d'.*int"):
        _ = unmappable._value_arrow_type(struct_type)  # pyright: ignore[reportPrivateUsage]  # fallback is unreachable publicly
