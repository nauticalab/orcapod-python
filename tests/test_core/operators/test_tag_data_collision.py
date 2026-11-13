"""A name is a tag or a data column, never both (ITL-940).

An input that has a name as a tag and another that has it as a data column cannot be joined: the
result would hold two columns of one name. Each case is rejected when the graph is built, with an
``InputValidationError`` that names the column, the roles and the remedy, instead of a raw
``KeyError`` when the result is read (sync) or rows with disagreeing values (async).
"""

from __future__ import annotations

import asyncio
from typing import TypeAlias

import pyarrow as pa
import pytest

from orcapod import function_pod
from orcapod.channels import Channel
from orcapod.core.function_pod import FunctionPod
from orcapod.core.operators import Index, Join, MergeJoin, Pick, SemiJoin
from orcapod.core.sources import ArrowTableSource
from orcapod.errors import InputValidationError
from orcapod.protocols.core_protocols import DataProtocol, TagProtocol

from .test_async_sync_equivalence import async_rows

DICT = pa.list_(
    pa.field(
        "item",
        pa.struct(
            [
                pa.field("key", pa.string(), nullable=False),
                pa.field("value", pa.int64(), nullable=False),
            ]
        ),
        nullable=False,
    )
)
Row: TypeAlias = tuple[TagProtocol, DataProtocol]
MESSAGE = r"column 'x'.*tag.*data column.*(map_tags|map_data)"


def _source(
    columns: dict[str, list[int]], tags: list[str], source_id: str
) -> ArrowTableSource:
    table = pa.table({name: pa.array(v, pa.int64()) for name, v in columns.items()})
    return ArrowTableSource(table, tag_columns=tags, source_id=source_id)


def _flatten(error: BaseException) -> list[BaseException]:
    """The leaf exceptions of a (possibly nested) exception group."""
    if isinstance(error, BaseExceptionGroup):
        return [leaf for inner in error.exceptions for leaf in _flatten(inner)]
    return [error]


def tag_x() -> ArrowTableSource:
    return _source({"x": [1, 2], "a": [10, 20]}, ["x"], "tag_x")


def data_x() -> ArrowTableSource:
    return _source({"k": [1, 2], "x": [1, 2], "b": [5, 6]}, ["k"], "data_x")


def other_data() -> ArrowTableSource:
    return _source({"x": [1, 2], "u": [1, 2], "v": [3, 4]}, ["u"], "other")


@pytest.mark.parametrize("operator", [Join, MergeJoin], ids=["join", "merge_join"])
@pytest.mark.parametrize("reverse", [False, True], ids=["tag_first", "data_first"])
def test_sync_rejects_a_tag_and_a_data_column_of_one_name(
    operator: type[Join] | type[MergeJoin], reverse: bool
) -> None:
    inputs = (data_x(), tag_x()) if reverse else (tag_x(), data_x())
    with pytest.raises(InputValidationError, match=MESSAGE):
        _ = operator()(*inputs)


@pytest.mark.parametrize("operator", [Join, MergeJoin], ids=["join", "merge_join"])
def test_output_schema_rejects_the_collision_too(
    operator: type[Join] | type[MergeJoin],
) -> None:
    with pytest.raises(InputValidationError, match=MESSAGE):
        _ = operator().output_schema(tag_x(), data_x())


def test_join_rejects_the_collision_between_any_pair_of_inputs() -> None:
    third = _source({"k": [1, 2], "c": [0, 0]}, ["k"], "third")
    with pytest.raises(InputValidationError, match=MESSAGE):
        _ = Join()(third, data_x(), tag_x())


@pytest.mark.parametrize("operator", [Join, MergeJoin], ids=["join", "merge_join"])
def test_async_rejects_a_tag_and_a_data_column_of_one_name(
    operator: type[Join] | type[MergeJoin],
) -> None:
    with pytest.raises(BaseException) as excinfo:
        _ = async_rows(operator(), tag_x(), data_x())
    errors = _flatten(excinfo.value)
    assert any(
        isinstance(e, InputValidationError) and "tag" in str(e) and "'x'" in str(e)
        for e in errors
    ), errors


def test_async_join_rejects_the_collision_in_a_three_input_join() -> None:
    third = _source({"k": [1, 2], "c": [0, 0]}, ["k"], "third")
    with pytest.raises(BaseException) as excinfo:
        _ = async_rows(Join(), third, data_x(), tag_x())
    errors = _flatten(excinfo.value)
    assert any(isinstance(e, InputValidationError) for e in errors), errors


def test_two_data_columns_of_one_name_still_merge() -> None:
    left = _source({"x": [1, 2], "u": [1, 2]}, ["u"], "l")
    result = MergeJoin()(left, other_data())
    assert result.as_table().column("x").to_pylist() == [[1, 1], [2, 2]]


def test_valid_join_still_works() -> None:
    left = _source({"k": [1, 2], "a": [10, 20]}, ["k"], "l")
    result = Join()(left, data_x())
    assert result.keys() == (("k",), ("a", "x", "b"))


def test_semijoin_is_not_affected() -> None:
    """SemiJoin keys on every common column, tag or data, and returns the left stream's columns."""
    result = SemiJoin()(tag_x(), data_x())
    table = result.as_table()
    assert table.column_names == ["x", "a"]
    assert table.column("x").to_pylist() == [1, 2]


@pytest.mark.parametrize("operator", [Index, Pick])
def test_index_and_pick_reject_out_equal_to_a_tag(
    operator: type[Index] | type[Pick],
) -> None:
    if operator is Index:
        source = ArrowTableSource(
            pa.table(
                {"x": pa.array([1], pa.int64()), "vals": pa.array([[1, 2]])},
                schema=pa.schema(
                    [
                        pa.field("x", pa.int64()),
                        pa.field(
                            "vals",
                            pa.list_(pa.field("item", pa.int64(), nullable=False)),
                            nullable=False,
                        ),
                    ]
                ),
            ),
            tag_columns=["x"],
            source_id="idx",
        )
        op = Index("vals", 0, out="x")
    else:
        source = ArrowTableSource(
            pa.table(
                {
                    "x": pa.array([1], pa.int64()),
                    "d": pa.array([[{"key": "a", "value": 1}]], DICT),
                },
                schema=pa.schema(
                    [pa.field("x", pa.int64()), pa.field("d", DICT, nullable=False)]
                ),
            ),
            tag_columns=["x"],
            source_id="pk",
        )
        op = Pick("d", "a", out="x")
    with pytest.raises(InputValidationError, match=r"out column 'x'.*tag"):
        _ = op(source)


def _make_x_pod() -> FunctionPod:
    @function_pod(output_keys="x")
    def make_x(b: int | None) -> int:
        return (b or 0) * 2

    return make_x.pod  # pyright: ignore[reportReturnType]  # the decorator's pod is a FunctionPod


def test_function_pod_rejects_an_output_key_equal_to_an_input_tag() -> None:
    source = _source({"x": [1, 2], "b": [5, 6]}, ["x"], "podsrc")
    with pytest.raises(InputValidationError, match=r"output key 'x'.*tag"):
        _ = _make_x_pod()(source)


def test_function_pod_call_rejects_the_collision_after_its_automatic_join() -> None:
    left = _source({"x": [1, 2], "a": [10, 20]}, ["x"], "podl")
    right = _source({"x": [1, 2], "b": [5, 6]}, ["x"], "podr")
    with pytest.raises(InputValidationError, match=r"output key 'x'.*tag"):
        _ = _make_x_pod()(left, right)


def test_function_pod_process_rejects_the_collision() -> None:
    source = _source({"x": [1, 2], "b": [5, 6]}, ["x"], "podsrc2")
    with pytest.raises(InputValidationError, match=r"output key 'x'.*tag"):
        _ = _make_x_pod().process(source)


def test_function_pod_async_rejects_the_collision_instead_of_skipping_rows() -> None:
    source = _source({"x": [1, 2], "b": [5, 6]}, ["x"], "podasync")
    pod = _make_x_pod()
    with pytest.raises(BaseException) as excinfo:  # a task group wraps the error
        _ = _run_pod_async(pod, source)
    assert any(
        isinstance(e, InputValidationError) and "output key 'x'" in str(e)
        for e in _flatten(excinfo.value)
    )


def _run_pod_async(pod: FunctionPod, source: ArrowTableSource) -> list[Row]:
    async def run() -> list[Row]:
        inp: Channel[Row] = Channel(buffer_size=8)
        out: Channel[Row] = Channel(buffer_size=8)
        for row in source.iter_data():
            await inp.writer.send(row)
        await inp.writer.close()
        await pod.async_execute([inp.reader], out.writer)
        return await out.reader.collect()

    return asyncio.run(run())


def test_function_node_rejects_the_collision() -> None:
    from orcapod.core.nodes import FunctionNode

    source = _source({"x": [1, 2], "b": [5, 6]}, ["x"], "podnode")
    with pytest.raises(InputValidationError, match=r"output key 'x'.*tag"):
        _ = FunctionNode(_make_x_pod(), source)  # pyright: ignore[reportArgumentType]  # pod base class vs protocol, baselined elsewhere
