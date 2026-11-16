"""Every materialized row keeps its own source info (ITL-845).

``StaticOutputOperatorPod._materialize_to_stream`` and
``SyncPipelineOrchestrator._materialize_as_stream`` turn collected (Tag, Data)
pairs back into a stream. They used to read row 0's source info and stamp it on
every row, so rows from index 1 onward were attributed to row 0. These tests
send rows with distinct source-info tokens through every path that materializes
rows and check that each output row keeps its own source-info tokens.

A source-info token is the provenance value a row holds for one data column: the
value of its ``_source_<column>`` column, as ``Data.source_info()`` returns it
(for example ``<source>::row_0::x``, or a list of such tokens after ``Batch``,
``GroupBy`` or ``MergeJoin``). "Token" below always means a source-info token.

Expected source-info tokens always come from the input stream itself or from the
sync operator path (which never materializes rows), so no hash is hard-coded
except in the pinned-identity tests at the bottom.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from orcapod.channels import Channel
from orcapod.core.nodes.operator_node import OperatorJobNode
from orcapod.core.nodes.source_node import SourceJobNode
from orcapod.core.operators import (
    Batch,
    MergeJoin,
    PolarsFilter,
    SelectDataColumns,
)
from orcapod.core.operators.group_by import GroupBy
from orcapod.core.operators.index import Index
from orcapod.core.operators.pick import Pick
from orcapod.core.operators.static_output_pod import StaticOutputOperatorPod
from orcapod.core.sources import ArrowTableSource, DictSource
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator
from orcapod.protocols.database_protocols import ArrowDatabaseProtocol
from orcapod.protocols.core_protocols import DataProtocol, StreamProtocol, TagProtocol
from orcapod.system_constants import constants
from orcapod.types import CacheMode
from orcapod.system_constants import OPERATOR_DB_SCHEMA_VERSION

Row = tuple[TagProtocol, DataProtocol]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _non_null(table: pa.Table) -> pa.Table:
    """Return ``table`` with every top-level field declared non-nullable."""
    return table.cast(pa.schema([f.with_nullable(False) for f in table.schema]))


def _source(n: int = 4, offset: int = 0) -> ArrowTableSource:
    """Tags ``id`` (large_string) and ``g`` (int64, ``i % 2``); data ``x`` and ``y``."""
    table = pa.table(
        {
            "id": pa.array([f"k{i}" for i in range(n)], pa.large_string()),
            "g": pa.array([i % 2 for i in range(n)], pa.int64()),
            "x": pa.array([offset + i * 10 for i in range(n)], pa.int64()),
            "y": pa.array([offset + i * 10 + 1 for i in range(n)], pa.int64()),
        }
    )
    return ArrowTableSource(
        _non_null(table), tag_columns=["id", "g"], tag_only_columns=True
    )


def _list_source(n: int = 3) -> ArrowTableSource:
    """Build a source whose data column ``x`` holds non-null ``large_list<int64>`` values."""
    table = pa.table(
        {
            "id": pa.array([f"k{i}" for i in range(n)], pa.large_string()),
            "x": pa.array([[i, i + 1] for i in range(n)], pa.large_list(pa.int64())),
        }
    )
    return ArrowTableSource(_non_null(table), tag_columns=["id"], tag_only_columns=True)


def _source_info_tokens(rows: Sequence[Row], column: str = "x") -> list[Any]:
    """Return each row's source-info token for ``column``, in row order."""
    return [data.source_info()[column] for _, data in rows]


def _keyed(rows: Sequence[Row]) -> list[tuple[str, dict[str, Any], dict[str, Any]]]:
    """Rows as sortable (tag, data, source info) triples, ignoring emission order."""
    return sorted(
        (
            (repr(sorted(tag.as_dict().items())), data.as_dict(), data.source_info())
            for tag, data in rows
        ),
        key=lambda item: item[0],
    )


async def _feed(stream: StreamProtocol) -> Channel[Row]:
    """Return a closed channel holding every (Tag, Data) pair of ``stream``."""
    channel: Channel[Row] = Channel(buffer_size=64)
    for item in stream.iter_data():
        await channel.writer.send(item)
    await channel.writer.close()
    return channel


def _run_async(op: StaticOutputOperatorPod, *streams: StreamProtocol) -> list[Row]:
    """Run ``op.async_execute`` over ``streams`` and collect what it emits."""

    async def go() -> list[Row]:
        """Drive the coroutine under test."""
        inputs = [(await _feed(s)).reader for s in streams]
        output: Channel[Row] = Channel(buffer_size=64)
        await op.async_execute(
            inputs,
            output.writer,
            input_pipeline_hashes=[s.pipeline_hash() for s in streams],
        )
        return await output.reader.collect()

    return asyncio.run(go())


def _materialize(rows: Sequence[Row]) -> StreamProtocol:
    """Call ``StaticOutputOperatorPod._materialize_to_stream``, the helper under test."""
    # Non-public on purpose: these tests are about this helper itself.
    return StaticOutputOperatorPod._materialize_to_stream(rows)  # pyright: ignore[reportPrivateUsage]


def _materialize_buffer(buf: Sequence[Row], node: Any) -> StreamProtocol:
    """Call ``SyncPipelineOrchestrator._materialize_as_stream``, the helper under test."""
    # Non-public on purpose: these tests are about this helper itself.
    return SyncPipelineOrchestrator._materialize_as_stream(list(buf), node)  # pyright: ignore[reportPrivateUsage]


def _broadcast_row_zero(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make every materialization stamp row 0's source info, as before ITL-845."""
    # Non-public on purpose: re-creating the pre-fix behaviour has no public hook.
    original = StaticOutputOperatorPod._rows_to_table  # pyright: ignore[reportPrivateUsage]

    def broadcasting(
        rows: Sequence[Row],
    ) -> tuple[pa.Table, tuple[str, ...]]:
        """Stamp row 0's source info on every row, then build the table."""
        row_zero = rows[0][1].source_info()
        return original(
            [(tag, data.with_source_info(**row_zero)) for tag, data in rows]
        )

    monkeypatch.setattr(
        StaticOutputOperatorPod, "_rows_to_table", staticmethod(broadcasting)
    )


def _stored_source(node: OperatorJobNode, column: str = "x") -> list[Any]:
    """Return the node's stored ``_source_<column>`` values, sorted by tag."""
    records = node.get_all_records(columns={"source": True})
    assert records is not None
    rows = sorted(records.to_pylist(), key=lambda r: repr(r["id"]))
    return [r[f"{constants.SOURCE_PREFIX}{column}"] for r in rows]


def _source_fields(table: pa.Table) -> dict[str, pa.Field[pa.DataType]]:
    """Return the ``_source_*`` fields of ``table``'s schema, keyed by name."""
    return {
        f.name: f for f in table.schema if f.name.startswith(constants.SOURCE_PREFIX)
    }


# ---------------------------------------------------------------------------
# _materialize_to_stream itself
# ---------------------------------------------------------------------------


class TestMaterializeToStream:
    def test_scalar_source_info_tokens_kept_per_row(self):
        """Scalar source-info tokens of every data column stay with their own row."""
        rows = list(_source().iter_data())
        expected = _source_info_tokens(rows)
        assert len(set(expected)) == len(
            expected
        )  # distinct, so the test is not vacuous

        stream = _materialize(rows)

        assert _source_info_tokens(list(stream.iter_data())) == expected
        assert _source_info_tokens(
            list(stream.iter_data()), "y"
        ) == _source_info_tokens(rows, "y")

    def test_list_source_info_tokens_kept_per_row(self):
        """List-valued source-info tokens (after ``Batch``) stay with their own row."""
        rows = list(Batch(2)(_source(5)).iter_data())  # batches of 2, 2, 1
        expected = _source_info_tokens(rows)
        assert [len(t) for t in expected] == [2, 2, 1]

        stream = _materialize(rows)

        assert _source_info_tokens(list(stream.iter_data())) == expected

    def test_single_row(self):
        """A single row materializes with its own source-info tokens."""
        rows = list(_source(1).iter_data())
        stream = _materialize(rows)
        assert _source_info_tokens(list(stream.iter_data())) == _source_info_tokens(
            rows
        )

    def test_zero_rows_raises(self):
        """Materializing zero rows still raises ``ValueError``."""
        with pytest.raises(ValueError, match="empty list of rows"):
            _ = _materialize([])

    def test_none_source_info_rows(self):
        """A ``None`` source-info token on row 0 does not spread to the other rows."""
        rows = list(_source(3).iter_data())
        # Row 0 has no source-info token, the others keep theirs.
        rows[0] = (rows[0][0], rows[0][1].with_source_info(x=None))
        expected = _source_info_tokens(rows)
        assert expected[0] is None and None not in expected[1:]

        stream = _materialize(rows)

        assert _source_info_tokens(list(stream.iter_data())) == expected

    def test_none_source_info_after_row_zero(self):
        """A ``None`` source-info token after row 0 is kept rather than overwritten."""
        rows = list(_source(3).iter_data())
        rows[2] = (rows[2][0], rows[2][1].with_source_info(x=None))
        stream = _materialize(rows)
        assert _source_info_tokens(list(stream.iter_data())) == _source_info_tokens(
            rows
        )

    def test_all_none_source_info(self):
        """Rows with no source-info tokens materialize with ``None`` for each one."""
        rows = [(t, d.with_source_info(x=None)) for t, d in _source(3).iter_data()]
        stream = _materialize(rows)
        assert _source_info_tokens(list(stream.iter_data())) == [None, None, None]

    def test_data_columns_and_types_unchanged(self):
        """Data values and the output schema are those of the input."""
        src = _source()
        rows = list(src.iter_data())
        stream = _materialize(rows)
        assert stream.as_table().equals(src.as_table())
        assert stream.output_schema() == src.output_schema()


class TestSourceInfoArrowFields:
    """The ``_source_*`` fields keep the Arrow type and nullability they had before.

    Before ITL-845 a concrete (broadcast) source-info token made the field
    non-nullable, and a row-0 ``None`` passed the column through as nullable. The
    fix declares a field nullable exactly when some row's source-info token is
    ``None``, which matches the old flag whenever the old values were right; the
    stored schema, and so the operator node's per-row record hash, depend on it.
    """

    def test_scalar_source_info_tokens_non_nullable_large_string(self):
        """Scalar source-info token fields stay non-nullable ``large_string``."""
        stream = _materialize(list(_source().iter_data()))
        fields = _source_fields(stream.as_table(columns={"source": True}))
        assert fields == {
            "_source_x": pa.field("_source_x", pa.large_string(), nullable=False),
            "_source_y": pa.field("_source_y", pa.large_string(), nullable=False),
        }

    def test_list_source_info_tokens_non_nullable_large_list(self):
        """List source-info fields stay non-nullable ``large_list<large_string>``."""
        rows = list(Batch(2)(_source(4)).iter_data())
        stream = _materialize(rows)
        field = _source_fields(stream.as_table(columns={"source": True}))["_source_x"]
        assert field == pa.field(
            "_source_x", pa.large_list(pa.large_string()), nullable=False
        )
        list_type = field.type
        assert isinstance(list_type, pa.LargeListType)
        assert list_type.value_field.nullable is True

    @pytest.mark.parametrize("none_row", [0, 2])
    def test_field_nullable_when_a_source_info_token_is_none(self, none_row: int):
        """A ``_source_*`` field is nullable iff a row's source-info token is None."""
        rows = list(_source(3).iter_data())
        rows[none_row] = (rows[none_row][0], rows[none_row][1].with_source_info(x=None))
        stream = _materialize(rows)
        fields = _source_fields(stream.as_table(columns={"source": True}))
        assert fields["_source_x"] == pa.field(
            "_source_x", pa.large_string(), nullable=True
        )
        assert fields["_source_y"].nullable is False


# ---------------------------------------------------------------------------
# Async (barrier and streaming) operator paths: async must equal sync
# ---------------------------------------------------------------------------


class TestAsyncOperatorsKeepPerRowSourceInfo:
    @pytest.mark.parametrize(
        "make_op",
        [
            pytest.param(lambda: SelectDataColumns(["x"]), id="select"),
            pytest.param(lambda: PolarsFilter(), id="polars_filter"),
            pytest.param(lambda: PolarsFilter(constraints={"g": 1}), id="filter_g1"),
            pytest.param(lambda: Batch(2), id="batch2"),
            pytest.param(lambda: Batch(3), id="batch3_partial"),
            pytest.param(lambda: Batch(0), id="batch_all"),
            pytest.param(lambda: GroupBy(by=["g"]), id="group_by"),
        ],
    )
    def test_unary_async_matches_sync(
        self, make_op: Callable[[], StaticOutputOperatorPod]
    ):
        """The async path emits the same rows and source-info tokens as sync."""
        src = _source(5)
        sync_rows = list(make_op()(src).iter_data())
        async_rows = _run_async(make_op(), src)
        assert _keyed(async_rows) == _keyed(sync_rows)

    def test_polars_filter_source_info_tokens_follow_rows(self):
        """Async ``PolarsFilter`` keeps each row's own source-info token."""
        src = _source(4)
        out = _run_async(PolarsFilter(), src)
        assert _source_info_tokens(out) == _source_info_tokens(list(src.iter_data()))

    def test_batch_lists_hold_member_source_info_tokens(self):
        """An async ``Batch(2)`` list holds its members' source-info tokens in order."""
        src = _source(5)
        inputs = _source_info_tokens(list(src.iter_data()))
        out = _run_async(Batch(2), src)
        assert _source_info_tokens(out) == [inputs[0:2], inputs[2:4], inputs[4:5]]

    def test_batch_all_holds_member_source_info_tokens(self):
        """Async ``Batch(0)`` holds every input source-info token in order."""
        src = _source(4)
        out = _run_async(Batch(0), src)
        assert _source_info_tokens(out) == [_source_info_tokens(list(src.iter_data()))]

    def test_group_by_lists_hold_member_source_info_tokens(self):
        """Each async ``GroupBy`` list holds its members' source-info tokens."""
        src = _source(4)
        inputs = _source_info_tokens(list(src.iter_data()))
        out = _run_async(GroupBy(by=["g"]), src)
        by_group = {tag.as_dict()["g"]: data.source_info()["x"] for tag, data in out}
        assert by_group == {0: [inputs[0], inputs[2]], 1: [inputs[1], inputs[3]]}

    def test_merge_join_async_matches_sync(self):
        """Async ``MergeJoin`` matches sync; rows pair their own source-info tokens."""
        left, right = _source(3), _source(3, offset=1)
        sync_rows = list(MergeJoin()(left, right).iter_data())
        async_rows = _run_async(MergeJoin(), left, right)
        assert _keyed(async_rows) == _keyed(sync_rows)
        # Each output row holds its own row's source-info token from each side.
        left_tokens = _source_info_tokens(list(left.iter_data()))
        right_tokens = _source_info_tokens(list(right.iter_data()))
        for i, token_list in enumerate(
            _source_info_tokens(
                sorted(async_rows, key=lambda r: str(r[0].as_dict()["id"]))
            )
        ):
            assert sorted(token_list) == sorted([left_tokens[i], right_tokens[i]])

    def test_list_source_info_through_async_batch_of_batch(self):
        """A list-valued ``_source_*`` column (after Batch) materializes per row too."""
        batched = Batch(2)(_source(4))
        sync_rows = list(PolarsFilter()(batched).iter_data())
        async_rows = _run_async(PolarsFilter(), batched)
        assert _keyed(async_rows) == _keyed(sync_rows)

    @pytest.mark.parametrize(
        "make_op",
        [
            pytest.param(lambda: Batch(2), id="batch2"),
            pytest.param(lambda: Batch(0), id="batch_all"),
        ],
    )
    def test_zero_rows_emit_nothing(
        self, make_op: Callable[[], StaticOutputOperatorPod]
    ):
        """Async ``Batch`` over an empty channel emits nothing."""

        async def go() -> list[Row]:
            """Drive the coroutine under test."""
            empty: Channel[Row] = Channel(buffer_size=4)
            await empty.writer.close()
            output: Channel[Row] = Channel(buffer_size=4)
            # An input that delivers no rows still has a pipeline hash.
            await make_op().async_execute(
                [empty.reader],
                output.writer,
                input_pipeline_hashes=[_source().pipeline_hash()],
            )
            return await output.reader.collect()

        # The generic barrier path (e.g. ``PolarsFilter``) still raises on zero rows,
        # unchanged by ITL-845; reported separately rather than pinned here.
        assert asyncio.run(go()) == []


# ---------------------------------------------------------------------------
# Sync Index and Pick (build rows one by one, then materialize)
# ---------------------------------------------------------------------------


class TestIndexPickKeepPerRowSourceInfo:
    def test_index(self):
        """``Index`` derives each row's source-info token from that row's own."""
        src = _list_source(3)
        inputs = _source_info_tokens(list(src.iter_data()))
        out = list(Index(column="x", i=0)(src).iter_data())
        assert _source_info_tokens(out) == [f"{t}[0]" for t in inputs]

    def test_pick(self):
        """``Pick`` derives each row's source-info token from that row's own."""
        src = DictSource(
            [
                {"animal": "cat", "scores": {"speed": 8}},
                {"animal": "dog", "scores": {"speed": 7}},
                {"animal": "bird", "scores": {"speed": 10}},
            ],
            tag_columns=["animal"],
            tag_only_columns=True,
            data_schema={"animal": str, "scores": dict[str, int]},
        )
        inputs = {
            t.as_dict()["animal"]: d.source_info()["scores"] for t, d in src.iter_data()
        }
        out = list(Pick("scores", "speed")(src).iter_data())
        assert {t.as_dict()["animal"]: d.source_info()["scores"] for t, d in out} == {
            animal: f"{token}['speed']" for animal, token in inputs.items()
        }

    def test_index_single_row(self):
        """``Index`` on a single row keeps that row's source-info token."""
        src = _list_source(1)
        out = list(Index(column="x", i=1)(src).iter_data())
        assert _source_info_tokens(out) == [
            f"{_source_info_tokens(list(src.iter_data()))[0]}[1]"
        ]


# ---------------------------------------------------------------------------
# Operator nodes (stored rows) and the sync orchestrator
# ---------------------------------------------------------------------------


def _run_node(node: OperatorJobNode, src: StreamProtocol, mode: str) -> None:
    """Run ``node`` over ``src`` with ``execute`` or ``async_execute``."""
    if mode == "execute":
        _ = node.execute(src)
        return

    async def go() -> None:
        """Drive the coroutine under test."""
        output: Channel[Row] = Channel(buffer_size=64)
        await node.async_execute([(await _feed(src)).reader], output.writer)
        _ = await output.reader.collect()

    asyncio.run(go())


@pytest.mark.parametrize("mode", ["execute", "async_execute"])
class TestOperatorNodeStoresPerRowSourceInfo:
    def test_select(self, mode: str):
        """An operator node stores each row's own source-info token."""
        src = _source(3)
        node = OperatorJobNode(
            SelectDataColumns(["x"]),
            [src],
            pipeline_database=InMemoryArrowDatabase(),
            cache_mode=CacheMode.LOG,
        )
        _run_node(node, src, mode)
        assert _stored_source(node) == _source_info_tokens(list(src.iter_data()))

    def test_batch(self, mode: str):
        """An operator node stores one source-info token per batch member."""
        src = _source(3)
        node = OperatorJobNode(
            Batch(2),
            [src],
            pipeline_database=InMemoryArrowDatabase(),
            cache_mode=CacheMode.LOG,
        )
        _run_node(node, src, mode)
        inputs = _source_info_tokens(list(src.iter_data()))
        # One source-info token per batch member: the one-member batch holds one.
        assert _stored_source(node) == [inputs[0:2], inputs[2:3]]


class TestSyncOrchestratorMaterialization:
    def _source_node(self, src: ArrowTableSource) -> SourceJobNode:
        """Wrap ``src`` in a ``SourceJobNode``."""
        tag_schema, data_schema = src.output_schema()
        return SourceJobNode(
            name="src",
            tag_schema=tag_schema,
            data_schema=data_schema,
            bound_source=src,
        )

    def test_buffer_keeps_per_row_source_info_tokens(self):
        """A source node's buffer keeps per-row source-info tokens and pipeline hash."""
        node = self._source_node(_source(3))
        buf = list(node.iter_data())
        stream = _materialize_buffer(buf, node)
        assert _source_info_tokens(list(stream.iter_data())) == _source_info_tokens(buf)
        assert stream.pipeline_hash() == node.pipeline_hash()

    def test_buffer_keeps_list_source_info_tokens(self):
        """An operator node's buffer keeps list-valued source-info tokens and hashes."""
        src = _source(4)
        node = OperatorJobNode(Batch(2), [src])
        node.run()
        buf = list(node.iter_data())
        stream = _materialize_buffer(buf, node)
        assert _source_info_tokens(list(stream.iter_data())) == _source_info_tokens(buf)
        assert stream.pipeline_hash() == node.pipeline_hash()
        assert stream.content_hash() == node.content_hash()
        fields = _source_fields(stream.as_table(columns={"source": True}))
        assert fields["_source_x"] == pa.field(
            "_source_x", pa.large_list(pa.large_string()), nullable=False
        )

    def test_empty_buffer(self):
        """An empty buffer still builds an empty stream."""
        node = self._source_node(_source(3))
        stream = _materialize_buffer([], node)
        assert stream.as_table().num_rows == 0


# ---------------------------------------------------------------------------
# Identity is unchanged; stores written before the fix
# ---------------------------------------------------------------------------


def _pin_source() -> ArrowTableSource:
    """Build the fixed three-row source whose node hashes are pinned below."""
    table = pa.table(
        {
            "id": pa.array(["a", "b", "c"], pa.large_string()),
            "x": pa.array([1, 2, 3], pa.int64()),
            "y": pa.array([4, 5, 6], pa.int64()),
        }
    )
    return ArrowTableSource(_non_null(table), tag_columns=["id"], tag_only_columns=True)


def _record_ids(node: OperatorJobNode) -> dict[str, bytes]:
    """Return the stored ``_record_hash`` of each row, keyed by the ``id`` tag."""
    # Non-public on purpose: no public view exposes the stored record hash.
    database = node._pipeline_database  # pyright: ignore[reportPrivateUsage]
    assert database is not None
    records = database.get_all_records(
        node.node_identity_path + (OPERATOR_DB_SCHEMA_VERSION,),
        record_id_column=node.HASH_COLUMN_NAME,
    )
    assert records is not None
    return {
        r["id"]: r[node.HASH_COLUMN_NAME]
        for r in records.select(["id", node.HASH_COLUMN_NAME]).to_pylist()
    }


class TestIdentityPinned:
    """Values recorded on ``main`` at ab544982, before the fix.

    Re-recorded under starfix hash format 2 (``arrow_v1``, ITL-910): the node content
    hash and the record hashes are Arrow-derived; the pipeline hash is not.

    Re-recorded for ITL-957: the pinned source has no given ``source_id``, so its default
    id (now ``arrow_table:<label>:<hex>``) moved, and with it the node content hash and
    the record hashes; the pipeline hash did not.

    The node's identity, the output data and the row-0 record hash (whose
    source-info token was already right) are unchanged; only rows whose stored
    source-info token was wrong get a new record hash.
    """

    def test_operator_node_identity_and_row_zero_record_hash(self):
        """Node hashes, data, schema and row 0's record hash match ``ab544982``."""
        src = _pin_source()
        node = OperatorJobNode(
            SelectDataColumns(["x"]),
            [src],
            pipeline_database=InMemoryArrowDatabase(),
            cache_mode=CacheMode.LOG,
        )
        _ = node.execute(src)

        assert node.pipeline_hash().to_string() == (
            "semantic_v1:62bd560f1fa945b5cac83f3eb99b6f8bb0fea607ce7e9503c987df4c36134da4"
        )
        assert node.content_hash().to_string() == (
            "semantic_v1:2a73f6156d2ad32c6daa75c400ed265df967d2a8fdddb825a383320b2a0ee7a8"
        )
        ids = _record_ids(node)
        assert ids["a"] == ROW_ZERO_RECORD_ID
        assert (
            ids["b"] not in PRE_FIX_RECORD_IDS
        )  # pre-fix rows b, c held row a's source-info token
        assert ids["c"] not in PRE_FIX_RECORD_IDS

        records = node.get_all_records(columns={"source": True})
        assert records is not None
        assert records.select(["id", "x"]).to_pylist() == [
            {"id": "a", "x": 1},
            {"id": "b", "x": 2},
            {"id": "c", "x": 3},
        ]
        assert _source_fields(records) == {
            "_source_x": pa.field("_source_x", pa.large_string(), nullable=False)
        }


# ``_record_hash`` of rows a, b, c stored by the pinned node before the fix (ab544982).
# Row a's source-info token was already right, so the fix leaves its record hash.
# Re-recorded in starfix hash format 2 (ITL-910) and again as labelled digests
# (``b"arrow_v1:" + digest``, ITL-945) by reproducing the pre-fix broadcast with
# ``_broadcast_row_zero``. Re-recorded once more for ITL-957: the pinned source has no
# given ``source_id``, and its default id (now ``arrow_table:<label>:<hex>``) is part of
# every source-info token these record hashes cover.
ROW_ZERO_RECORD_ID = b"arrow_v1:" + bytes.fromhex(
    "000002a5cac2713a6f95bb9fc0ffbc253dd6889a4b57584653be52c1c374f92516c047"
)
PRE_FIX_RECORD_IDS = frozenset(
    {
        ROW_ZERO_RECORD_ID,
        b"arrow_v1:"
        + bytes.fromhex(
            "000002a8e0ed9f26c79c30b169b8084f771d183b5de166128dd360ce58e996fd68f49a"
        ),
        b"arrow_v1:"
        + bytes.fromhex(
            "000002ca41256b75c5e733eb7488bd8a71eca066db16103796ec7463cd4227dda4f2ce"
        ),
    }
)


class TestStoreWrittenBeforeFix:
    """What a user sees when a database written by pre-fix ``main`` is re-run.

    Operator-node tables written by ``main`` from c398e7d4 (ITL-539) up to the fix
    hold row 0's source-info tokens on every row. The fixed code gives the corrected
    rows new record hashes, so a LOG re-run adds them next to the old ones, and a
    REPLAY node then emits both. Pinned as it behaves today; the cleanup guidance
    lives in the CHANGELOG.
    """

    def _node(
        self, db: ArrowDatabaseProtocol, mode: CacheMode = CacheMode.LOG
    ) -> OperatorJobNode:
        """Build the pinned ``SelectDataColumns`` operator node on ``db``."""
        return OperatorJobNode(
            SelectDataColumns(["x"]),
            [_pin_source()],
            pipeline_database=db,
            cache_mode=mode,
        )

    def test_rerun_after_pre_fix_store(self, monkeypatch: pytest.MonkeyPatch):
        """A LOG re-run adds corrected rows next to the old ones; REPLAY sees both."""
        db = InMemoryArrowDatabase()
        old = self._node(db)
        with monkeypatch.context() as patch:
            _broadcast_row_zero(patch)
            _ = old.execute(_pin_source())
        tokens = _source_info_tokens(list(_pin_source().iter_data()))
        assert _stored_source(old) == [tokens[0]] * 3

        new = self._node(db)
        out = new.execute(_pin_source())

        # The run itself emits the corrected rows.
        assert _source_info_tokens(out) == tokens
        # The table keeps the old rows: row a deduplicates (its ID did not change),
        # rows b and c are now stored twice.
        stored = sorted(zip(*[_stored_rows(new, c) for c in ("id", "_source_x")]))
        assert stored == sorted(
            [
                ("a", tokens[0]),
                ("b", tokens[0]),
                ("c", tokens[0]),
                ("b", tokens[1]),
                ("c", tokens[2]),
            ]
        )

        replay = self._node(db, CacheMode.REPLAY)
        _ = replay.run()  # a node shows rows only after a run (ITL-1095)
        assert len(list(replay.iter_data())) == 5

    def test_v014_store_is_not_read(self, tmp_path: Path):
        """A table written by v0.1.x is invisible to the run filter, so no duplicates.

        v0.1.x stored the run column as ``_node_content_hash`` (``large_string``);
        v0.2 filters on ``__node_content_hash``, so old rows never match.
        """
        db = DeltaTableDatabase(tmp_path)
        old = self._node(db)
        table = SelectDataColumns(["x"])(_pin_source()).as_table(
            columns={"source": True, "system_tags": True}
        )
        # Old rows as v0.1.x stored them: row 0's source-info token on every row.
        position = table.schema.get_field_index("_source_x")
        row_zero = table.column("_source_x")[0]
        table = table.set_column(
            position,
            table.schema.field(position),
            pa.array([row_zero.as_py()] * table.num_rows, row_zero.type),
        )
        table = table.append_column(
            "_node_content_hash",
            pa.array(
                [old.content_hash().to_string()] * table.num_rows, pa.large_string()
            ),
        )
        table = table.add_column(
            0,
            old.HASH_COLUMN_NAME,
            pa.array(
                [f"v014-{i}".encode() for i in range(table.num_rows)], pa.large_binary()
            ),
        )
        db.add_records(
            old.node_identity_path, table, record_id_column=old.HASH_COLUMN_NAME
        )
        db.flush()

        new = self._node(db)
        _ = new.execute(_pin_source())
        db.flush()
        tokens = _source_info_tokens(list(_pin_source().iter_data()))
        assert _stored_source(new) == tokens
        replay = self._node(db, CacheMode.REPLAY)
        _ = replay.run()  # a node shows rows only after a run (ITL-1095)
        assert _source_info_tokens(list(replay.iter_data())) == tokens


def _stored_rows(node: OperatorJobNode, column: str) -> list[Any]:
    """Return the node's stored values of ``column``, in storage order."""
    records = node.get_all_records(columns={"source": True})
    assert records is not None
    return records.column(column).to_pylist()
