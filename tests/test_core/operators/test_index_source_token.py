"""Index after an aggregation takes the element's own provenance token (ITL-1046).

After ``Batch`` or ``GroupBy`` a column's source-info token is a list, one token per
member. ``Index`` must give the extracted element the token of that member, not the text
of the whole list followed by ``[i]``. A scalar token keeps the ``<token>[i]`` form.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pyarrow as pa

from orcapod.channels import Channel
from orcapod.core.operators import Batch, GroupBy, Index
from orcapod.core.sources import ArrowTableSource
from orcapod.protocols.core_protocols import StreamProtocol

SRC = "src"


def _source() -> StreamProtocol:
    """Five rows, deliberately not in tag order (ids 3, 1, 2, 5, 4)."""
    table = pa.table(
        {
            "k": pa.array(["b", "a", "b", "a", "b"]),
            "id": pa.array([3, 1, 2, 5, 4], pa.int64()),
            "x": pa.array([30, 10, 20, 50, 40], pa.int64()),
        }
    )
    return ArrowTableSource(
        table, tag_columns=["k", "id"], source_id=SRC, tag_only_columns=True
    )


def _tokens(stream: StreamProtocol, column: str) -> list[Any]:
    table = stream.as_table(columns={"source": True})
    return table.column(f"_source_{column}").to_pylist()


def _token_type(stream: StreamProtocol, column: str) -> pa.DataType:
    return (
        stream.as_table(columns={"source": True}).schema.field(f"_source_{column}").type
    )


def test_batch_then_index_takes_the_members_own_token() -> None:
    batched = Batch(2)(_source())
    members = _tokens(batched, "x")
    result = Index("x", 0, out="first")(batched)

    assert _tokens(result, "first") == [m[0] for m in members]
    assert all(isinstance(t, str) and "[" not in t for t in _tokens(result, "first"))
    assert _token_type(result, "first") == pa.large_string()
    # the untouched column keeps its list token
    assert _tokens(result, "x") == members


def test_batch_then_index_negative_index() -> None:
    batched = Batch(2)(_source())
    members = _tokens(batched, "x")
    result = Index("x", -1, out="last")(batched)
    assert _tokens(result, "last") == [m[-1] for m in members]


def test_batch_then_index_in_place_replaces_the_token() -> None:
    batched = Batch(2)(_source())
    members = _tokens(batched, "x")
    result = Index("x", 1)(batched.select_data_columns("x"))
    # row 3 is a batch of one: index 1 is out of range, so the row is skipped
    assert _tokens(result, "x") == [m[1] for m in members if len(m) > 1]


def test_out_of_range_index_skips_the_row() -> None:
    batched = Batch(2)(_source())
    result = Index("x", 1, out="second")(batched)
    assert result.as_table().num_rows == 2
    assert all(isinstance(t, str) for t in _tokens(result, "second"))


def test_group_by_then_index_takes_the_members_own_token() -> None:
    grouped = GroupBy(["k"])(_source())
    members = _tokens(grouped, "x")
    result = Index("x", 0, out="first")(grouped)

    assert _tokens(result, "first") == [m[0] for m in members]
    assert _token_type(result, "first") == pa.large_string()


def test_two_aggregations_then_index_gives_the_inner_list() -> None:
    twice = Batch(2)(GroupBy(["k"])(_source()))
    members = _tokens(twice, "x")  # list of lists of tokens
    assert all(isinstance(m[0], list) for m in members)
    result = Index("x", 0, out="first")(twice)

    assert _tokens(result, "first") == [m[0] for m in members]
    assert _token_type(result, "first") == pa.large_list(pa.large_string())


def test_zero_row_result_has_the_same_token_type_as_a_non_empty_one() -> None:
    batched = Batch(2)(_source())
    non_empty = Index("x", 0, out="first")(batched)
    empty = Index("x", 5, out="first")(batched)
    assert empty.as_table().num_rows == 0
    assert _token_type(empty, "first") == _token_type(non_empty, "first")


def test_scalar_token_keeps_the_indexed_form() -> None:
    table = pa.table(
        {
            "id": pa.array([2, 1], pa.int64()),
            "v": pa.array([[1, 2], [3, 4]], pa.list_(pa.int64())),
        },
        schema=pa.schema(
            [
                pa.field("id", pa.int64(), nullable=False),
                pa.field("v", pa.list_(pa.int64()), nullable=False),
            ]
        ),
    )
    source = ArrowTableSource(
        table, tag_columns=["id"], source_id=SRC, tag_only_columns=True
    )
    before = _tokens(source, "v")
    result = Index("v", 1, out="second")(source)
    assert _tokens(result, "second") == [f"{t}[1]" for t in before]


def test_async_path_yields_the_same_tokens_as_the_sync_path() -> None:
    batched = Batch(2)(_source())
    op = Index("x", 0, out="first")
    sync_tokens = _tokens(op(batched), "first")

    async def run() -> list[Any]:
        op.validate_inputs(batched)
        source = Channel(buffer_size=16)
        out = Channel(buffer_size=16)
        for tag, data in batched.iter_data():
            await source.writer.send((tag, data))
        await source.writer.close()
        await op.async_execute(
            [source.reader], out.writer, input_pipeline_hashes=[batched.pipeline_hash()]
        )
        return [data.source_info()["first"] for _, data in await out.reader.collect()]

    assert asyncio.run(run()) == sync_tokens


def test_zero_row_result_has_every_token_type_of_a_non_empty_one() -> None:
    # Two aggregations: the projected token is one list level, the others keep theirs.
    twice = Batch(2)(GroupBy(["k"])(_source()))
    for out in ("first", None):
        non_empty = Index("x", 0, out=out)(twice)
        empty = Index("x", 9, out=out)(twice)
        assert empty.as_table().num_rows == 0
        empty_schema = empty.as_table(columns={"source": True}).schema
        full_schema = non_empty.as_table(columns={"source": True}).schema
        assert empty_schema == full_schema
