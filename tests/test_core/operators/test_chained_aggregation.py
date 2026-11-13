"""Aggregating an already-aggregated stream (ITL-849).

``GroupBy`` and ``Batch`` applied to a stream whose data and source-info columns
are already list-valued (the output of ``MergeJoin``, ``Batch`` or ``GroupBy``)
must produce a list of the incoming list type: ``large_list<large_list<...>>``
with element types exactly the large-normalized input types, and a source-info
column that is ``large_list`` of its incoming source-info type, one list per
row. Before the fix every such chain raised ``ArrowTypeError: Expected bytes,
got a 'list' object`` as soon as its rows were materialized, because a second
level of list nesting in a source-info token was typed ``list<large_string>``.

``Batch`` followed by ``GroupBy`` stays rejected: ``Batch`` turns every tag
into a list, and ``GroupBy`` keys only on scalar tags.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast

import pyarrow as pa
import pytest

from orcapod.channels import Channel
from orcapod.contexts import get_default_type_converter
from orcapod.core.nodes.function_node import _build_record_id_preimage
from orcapod.core.operators import Batch, GroupBy, MergeJoin
from orcapod.core.sources.arrow_table_source import ArrowTableSource
from orcapod.errors import InputValidationError
from orcapod.protocols.core_protocols import DataProtocol, StreamProtocol, TagProtocol
from orcapod.system_constants import constants

CONVERTER = get_default_type_converter()
PATH_TYPE = CONVERTER.python_type_to_arrow_type(Path)
LIST_PATH = CONVERTER.python_type_to_arrow_type(list[Path])
LIST_LIST_PATH = CONVERTER.python_type_to_arrow_type(list[list[Path]])
SOURCE = constants.SOURCE_PREFIX
WITH_SOURCE = {"source": True}
WITH_SOURCE_AND_SYSTEM_TAGS = {"source": True, "system_tags": True}

Rows = list[tuple[TagProtocol, DataProtocol]]


def LL(element: pa.DataType) -> pa.DataType:
    """Shorthand for ``pa.large_list(element)``."""
    return pa.large_list(element)


def _source(
    columns: dict[str, tuple[pa.DataType, list[Any]]], tags: tuple[str, ...]
) -> ArrowTableSource:
    """Build a non-nullable source; a ``Path`` column is built over its storage."""
    arrays: list[pa.Array[Any]] = []
    for arrow_type, values in columns.values():
        if arrow_type == PATH_TYPE:
            assert isinstance(PATH_TYPE, pa.ExtensionType)
            storage = pa.array(values, type=PATH_TYPE.storage_type)
            arrays.append(pa.ExtensionArray.from_storage(PATH_TYPE, storage))
        else:
            arrays.append(pa.array(values, type=arrow_type))
    schema = pa.schema(
        [pa.field(name, t, nullable=False) for name, (t, _) in columns.items()]
    )
    return ArrowTableSource(
        pa.Table.from_arrays(arrays, schema=schema), tag_columns=list(tags)
    )


def source_a() -> ArrowTableSource:
    """Narrow types, both string layouts, a list column and a logical column."""
    return _source(
        {
            "id": (pa.int64(), [1, 2, 3, 4]),
            "g": (pa.int64(), [0, 0, 1, 1]),
            "x": (pa.int8(), [1, 2, 3, 4]),
            "y": (pa.float32(), [1.5, 2.5, 3.5, 4.5]),
            "name": (pa.string(), ["a", "b", "c", "d"]),
            "lname": (pa.large_string(), ["e", "f", "g", "h"]),
            "lst": (pa.list_(pa.int8()), [[1], [2, 3], [], [4]]),
            "p": (PATH_TYPE, ["/a", "/b", "/c", "/d"]),
        },
        ("id", "g"),
    )


def source_c(ids: tuple[int, ...] = (1, 2, 3, 4)) -> ArrowTableSource:
    """Collides with ``source_a`` on ``x`` and ``p``; disjoint ``ids`` give zero rows."""
    return _source(
        {
            "id": (pa.int64(), list(ids)),
            "g": (pa.int64(), [0, 0, 1, 1][: len(ids)]),
            "x": (pa.int8(), [10, 20, 30, 40][: len(ids)]),
            "z": (pa.large_string(), ["p", "q", "r", "s"][: len(ids)]),
            "p": (PATH_TYPE, ["/w", "/x", "/y", "/z"][: len(ids)]),
        },
        ("id", "g"),
    )


def merge_join_ac() -> StreamProtocol:
    """``MergeJoin`` of A and C: ``x`` and ``p`` become lists, one per source."""
    return MergeJoin()(source_a(), source_c())


def merge_join_zero() -> StreamProtocol:
    """``MergeJoin`` of A and C with no shared tag tuple: zero rows."""
    return MergeJoin()(source_a(), source_c(ids=(7, 8, 9, 10)))


# Expected Arrow type of every user tag and data column of each chain's output.
# Each upstream list type gains exactly one ``large_list`` level; element types
# are the large-normalized input types (``int8`` and ``float32`` keep their
# width, ``string`` becomes ``large_string``) and ``Path`` stays logical.
MERGE_JOIN_THEN_AGGREGATE = {
    "x": LL(LL(pa.int8())),
    "y": LL(pa.float32()),
    "name": LL(pa.large_string()),
    "lname": LL(pa.large_string()),
    "lst": LL(LL(pa.int8())),
    "z": LL(pa.large_string()),
    "p": LIST_LIST_PATH,
}
AGGREGATE_THEN_AGGREGATE = {
    "id": LL(LL(pa.int64())),
    "x": LL(LL(pa.int8())),
    "y": LL(LL(pa.float32())),
    "name": LL(LL(pa.large_string())),
    "lname": LL(LL(pa.large_string())),
    "lst": LL(LL(LL(pa.int8()))),
    "p": LIST_LIST_PATH,
}
# ``GroupBy(["g"])`` drops the non-key tag ``id`` (ITL-1070), so a chain whose
# upstream is a ``GroupBy`` has no ``id`` column to aggregate again.
GROUP_BY_THEN_AGGREGATE = {
    k: v for k, v in AGGREGATE_THEN_AGGREGATE.items() if k != "id"
}


def _batch_members(upstream: Rows, row: int, size: int = 2) -> list[int]:
    """Upstream row indices of output row ``row`` of ``Batch(size)``."""
    return list(range(row * size, min((row + 1) * size, len(upstream))))


def _group_members(upstream: Rows, key: Any) -> list[int]:
    """Upstream row indices of the group ``g == key``, in ``GroupBy`` member order."""
    members = [i for i, (tag, _) in enumerate(upstream) if tag["g"] == key]
    if "id" in upstream[0][0].keys():
        members.sort(key=lambda i: cast(int, upstream[i][0]["id"]))
    return members


CHAINS: dict[str, dict[str, Any]] = {
    "mergejoin_group_by": {
        "upstream": merge_join_ac,
        "op": lambda: GroupBy(["g"]),
        "types": {"g": pa.int64(), **MERGE_JOIN_THEN_AGGREGATE},
        "members": lambda up, r, tag: _group_members(up, tag["g"]),
    },
    "mergejoin_batch": {
        "upstream": merge_join_ac,
        "op": lambda: Batch(batch_size=2),
        "types": {
            "id": LL(pa.int64()),
            "g": LL(pa.int64()),
            **MERGE_JOIN_THEN_AGGREGATE,
        },
        "members": lambda up, r, tag: _batch_members(up, r),
    },
    "batch_batch": {
        "upstream": lambda: Batch(batch_size=2)(source_a()),
        "op": lambda: Batch(batch_size=2),
        "types": {"g": LL(LL(pa.int64())), **AGGREGATE_THEN_AGGREGATE},
        "members": lambda up, r, tag: _batch_members(up, r),
    },
    "group_by_batch": {
        "upstream": lambda: GroupBy(["g"])(source_a()),
        "op": lambda: Batch(batch_size=2),
        "types": {"g": LL(pa.int64()), **GROUP_BY_THEN_AGGREGATE},
        "members": lambda up, r, tag: _batch_members(up, r),
    },
    "group_by_group_by": {
        "upstream": lambda: GroupBy(["g"])(source_a()),
        "op": lambda: GroupBy(["g"]),
        "types": {"g": pa.int64(), **GROUP_BY_THEN_AGGREGATE},
        "members": lambda up, r, tag: _group_members(up, tag["g"]),
    },
}


class Chain:
    """One chained aggregation: upstream stream, operator, and its output stream."""

    def __init__(self, name: str) -> None:
        spec = CHAINS[name]
        self.name = name
        self.upstream: StreamProtocol = spec["upstream"]()
        self.op = spec["op"]()
        self.types: dict[str, pa.DataType] = spec["types"]
        self.members: Callable[[Rows, int, TagProtocol], list[int]] = spec["members"]
        self.result = self.op(self.upstream)


@pytest.fixture(params=sorted(CHAINS))
def chain(request: pytest.FixtureRequest) -> Chain:
    """Every allowed chain of an aggregating operator over a list-valued stream."""
    return Chain(request.param)


def _run_async(op: Any, upstream: StreamProtocol) -> Rows:
    """Feed ``upstream`` into a channel and run ``op.async_execute`` over it."""

    async def run() -> Rows:
        source, sink = Channel(buffer_size=64), Channel(buffer_size=64)
        for row in upstream.iter_data():
            await source.writer.send(row)
        await source.writer.close()
        await op.async_execute(
            [source.reader],
            sink.writer,
            input_pipeline_hashes=[upstream.pipeline_hash()],
        )
        return await sink.reader.collect()

    return asyncio.run(run())


def test_group_by_after_merge_join_materializes() -> None:
    """The ITL-849 reproduction: ``as_table(all_info=True)`` no longer raises."""
    table = GroupBy(["id"])(merge_join_ac()).as_table(all_info=True)
    assert table.num_rows == 4
    assert table.schema.field("x").type == LL(LL(pa.int8()))
    assert table.column("x").to_pylist() == [[[1, 10]], [[2, 20]], [[3, 30]], [[4, 40]]]
    assert table.schema.field(f"{SOURCE}x").type == LL(LL(pa.large_string()))


class TestSyncChains:
    """The synchronous result of each chain is well typed and fully materializable."""

    def test_column_types_are_one_list_level_deeper(self, chain: Chain) -> None:
        """Each user column is ``large_list`` of its exact incoming type, NOT NULL.

        A ``GroupBy`` key stays scalar, and NOT NULL like its input.
        """
        schema = chain.result.as_table(all_info=True).schema
        user_columns = {f.name: f for f in schema if not f.name.startswith("_")}
        assert {name: f.type for name, f in user_columns.items()} == chain.types
        for name, field in user_columns.items():
            assert not field.nullable, name

    def test_source_info_columns_nest_their_incoming_type(self, chain: Chain) -> None:
        """``_source_<col>`` is ``large_list`` of the upstream ``_source_<col>`` type."""
        upstream = chain.upstream.as_table(columns=WITH_SOURCE).schema
        result = chain.result.as_table(columns=WITH_SOURCE).schema
        source_columns = [n for n in upstream.names if n.startswith(SOURCE)]
        assert source_columns
        for name in source_columns:
            incoming = upstream.field(name).type
            assert result.field(name).type == LL(incoming), name
            assert not result.field(name).nullable, name

    @pytest.mark.parametrize("columns", [{}, {"system_tags": True}], ids=str)
    def test_prediction_matches_static_process(
        self, chain: Chain, columns: dict[str, bool]
    ) -> None:
        """``output_schema()`` predicts exactly what ``static_process`` produces."""
        # Not ``chain.result``: ``op(stream)`` returns a stream whose
        # ``output_schema()`` delegates back to this prediction, so comparing
        # against it would be circular (CLAUDE.md, "Important implementation
        # details"), and ``as_table()`` legitimately carries columns the schema
        # omits.
        static: StreamProtocol = chain.op.unary_static_process(chain.upstream)
        assert chain.op.output_schema(
            chain.upstream, columns=columns
        ) == static.output_schema(columns=columns)

    def test_values_are_the_members_values(self, chain: Chain) -> None:
        """Each output value lists its members' upstream values, in member order."""
        upstream = list(chain.upstream.iter_data())
        rows = list(chain.result.iter_data())
        assert rows
        data_columns = set(upstream[0][1].keys())
        for r, (tag, data) in enumerate(rows):
            members = chain.members(upstream, r, tag)
            assert members
            for column in data_columns:
                assert data[column] == [upstream[m][1][column] for m in members], column

    def test_path_values_round_trip(self, chain: Chain) -> None:
        """The logical ``Path`` column comes back as nested ``Path`` objects."""
        for _, data in chain.result.iter_data():
            for member in data["p"]:  # type: ignore[union-attr]
                assert all(isinstance(p, Path) for p in member)

    def test_source_info_is_a_list_per_row_of_its_members_tokens(
        self, chain: Chain
    ) -> None:
        """Every row's provenance lists its own members' upstream tokens.

        Only data columns carry provenance: a tag has no source-info column
        (ITL-955).
        """
        upstream = list(chain.upstream.iter_data())
        rows = list(chain.result.iter_data())
        table = chain.result.as_table(columns=WITH_SOURCE)
        assert any(v is not None for v in upstream[0][1].source_info().values())
        for r, (tag, data) in enumerate(rows):
            members = chain.members(upstream, r, tag)
            source_info = data.source_info()
            for column in upstream[0][1].source_info():
                expected = [upstream[m][1].source_info()[column] for m in members]
                assert source_info[column] == expected, column
                assert table.column(f"{SOURCE}{column}")[r].as_py() == expected

    def test_hashes_are_computable(self, chain: Chain) -> None:
        """Data, tag, record-ID and stream hashes all compute."""
        hasher = chain.result.data_context.arrow_hasher
        for tag, data in chain.result.iter_data():
            assert data.content_hash() is not None
            assert tag.content_hash() is not None
            assert hasher.hash_table(_build_record_id_preimage(tag, data)) is not None
        assert chain.result.content_hash() is not None
        assert chain.result.pipeline_hash() is not None
        assert chain.result.as_table(all_info=True).num_rows > 0


class TestAsyncChains:
    """The barrier-mode async path agrees with the sync path."""

    def test_rows_match_sync(self, chain: Chain) -> None:
        """Same rows, values, Arrow types and data/tag content hashes as sync."""
        sync_rows = list(chain.result.iter_data())
        async_rows = _run_async(chain.op, chain.upstream)
        assert len(async_rows) == len(sync_rows)
        for (a_tag, a_data), (s_tag, s_data) in zip(async_rows, sync_rows):
            assert a_data.as_dict() == s_data.as_dict()
            assert a_data.content_hash() == s_data.content_hash()
            assert a_tag.content_hash() == s_tag.content_hash()
            assert (
                a_data.as_table(columns=WITH_SOURCE).schema
                == s_data.as_table(columns=WITH_SOURCE).schema
            )

    def test_source_info_matches_sync(self, chain: Chain) -> None:
        """Per-row provenance on the async path equals the sync path's."""
        sync_rows = list(chain.result.iter_data())
        async_rows = _run_async(chain.op, chain.upstream)
        assert [d.source_info() for _, d in async_rows] == [
            d.source_info() for _, d in sync_rows
        ]

    def test_three_level_chain_matches_sync(self) -> None:
        """A third aggregation hands nested Python source info to the barrier path."""
        upstream = Batch(batch_size=2)(GroupBy(["g"])(source_a()))
        op = Batch(batch_size=1)
        sync_rows = list(op(upstream).iter_data())
        async_rows = _run_async(op, upstream)
        assert len(async_rows) == len(sync_rows) == 1
        (a_tag, a_data), (s_tag, s_data) = async_rows[0], sync_rows[0]
        assert a_data.as_dict() == s_data.as_dict()
        assert a_data.content_hash() == s_data.content_hash()
        assert a_tag.content_hash() == s_tag.content_hash()
        schema = a_data.as_table(columns=WITH_SOURCE).schema
        assert schema == s_data.as_table(columns=WITH_SOURCE).schema
        assert schema.field("x").type == LL(LL(LL(pa.int8())))
        assert schema.field(f"{SOURCE}x").type == LL(LL(LL(pa.large_string())))


ZERO_ROW_CHAINS: dict[str, Callable[[StreamProtocol], StreamProtocol]] = {
    "group_by": lambda s: GroupBy(["g"])(s),
    "batch": lambda s: Batch(batch_size=2)(s),
    "batch_batch": lambda s: Batch(batch_size=2)(Batch(batch_size=2)(s)),
}


@pytest.mark.parametrize("name", sorted(ZERO_ROW_CHAINS))
def test_zero_row_chain_has_the_non_empty_types(name: str) -> None:
    """A zero-row input yields zero rows typed exactly like a non-empty result."""
    build = ZERO_ROW_CHAINS[name]
    empty = build(merge_join_zero())
    full = build(merge_join_ac())
    empty_table = empty.as_table(columns=WITH_SOURCE_AND_SYSTEM_TAGS)
    full_table = full.as_table(columns=WITH_SOURCE_AND_SYSTEM_TAGS)
    assert empty_table.num_rows == 0
    assert full_table.num_rows > 0
    assert empty_table.schema == full_table.schema
    assert empty.output_schema(all_info=True) == full.output_schema(all_info=True)


def test_group_by_after_batch_is_rejected() -> None:
    """``Batch`` makes every tag a list, so ``GroupBy`` has no scalar key."""
    with pytest.raises(InputValidationError, match="list-valued tag columns"):
        GroupBy(["g"])(Batch(batch_size=2)(source_a()))
