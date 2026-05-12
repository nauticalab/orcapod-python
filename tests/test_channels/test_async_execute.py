"""
Comprehensive tests for async_execute on operators and FunctionPod.

Covers:
- StaticOutputPod._materialize_to_stream round-trip
- UnaryOperator barrier-mode async_execute (Select, Drop, Map, Filter, Batch)
- BinaryOperator barrier-mode async_execute (MergeJoin, SemiJoin)
- NonZeroInputOperator barrier-mode async_execute (Join)
- FunctionPod streaming async_execute
- FunctionPod concurrency control (max_concurrency)
- PythonDataFunction.direct_async_call via run_in_executor
- End-to-end multi-stage async pipeline wiring
- Error propagation through channels
- NodeConfig / PipelineConfig integration with FunctionPod
"""

from __future__ import annotations

import asyncio
from typing import cast

import pyarrow as pa
import pytest

from orcapod.channels import Channel
from orcapod.core.datagrams import Data
from orcapod.core.function_pod import FunctionPod
from orcapod.core.operators import (
    Batch,
    DropDataColumns,
    DropTagColumns,
    Join,
    MapData,
    MapTags,
    MergeJoin,
    PolarsFilter,
    SelectDataColumns,
    SelectTagColumns,
    SemiJoin,
)
from orcapod.core.operators.static_output_pod import StaticOutputOperatorPod
from orcapod.core.data_function import PythonDataFunction
from orcapod.core.streams.arrow_table_stream import ArrowTableStream
from orcapod.types import NodeConfig, PipelineConfig

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_stream(n: int = 3) -> ArrowTableStream:
    """Stream with tag=id, data=x (ints). Uses nullable=False schema."""
    schema = pa.schema(
        [pa.field("id", pa.int64(), nullable=False), pa.field("x", pa.int64(), nullable=False)]
    )
    table = pa.table(
        {"id": pa.array(list(range(n)), type=pa.int64()), "x": pa.array(list(range(n)), type=pa.int64())},
        schema=schema,
    )
    return ArrowTableStream(table, tag_columns=["id"])


def make_two_col_stream(n: int = 3) -> ArrowTableStream:
    """Stream with tag=id, data={x, y}. Uses nullable=False schema."""
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("x", pa.int64(), nullable=False),
            pa.field("y", pa.int64(), nullable=False),
        ]
    )
    table = pa.table(
        {
            "id": pa.array(list(range(n)), type=pa.int64()),
            "x": pa.array(list(range(n)), type=pa.int64()),
            "y": pa.array([i * 10 for i in range(n)], type=pa.int64()),
        },
        schema=schema,
    )
    return ArrowTableStream(table, tag_columns=["id"])


def make_name_stream() -> ArrowTableStream:
    """Stream with tag=id, data=name (str). Uses nullable=False schema."""
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("name", pa.large_string(), nullable=False),
        ]
    )
    table = pa.table(
        {
            "id": pa.array([0, 1, 2], type=pa.int64()),
            "name": pa.array(["alice", "bob", "carol"], type=pa.large_string()),
        },
        schema=schema,
    )
    return ArrowTableStream(table, tag_columns=["id"])


async def feed_stream_to_channel(stream: ArrowTableStream, ch: Channel) -> None:
    """Push all (tag, data) pairs from a stream into a channel, then close."""
    for tag, data in stream.iter_data():
        await ch.writer.send((tag, data))
    await ch.writer.close()


async def collect_output(ch: Channel) -> list[tuple]:
    """Collect all (tag, data) pairs from a channel's reader."""
    return await ch.reader.collect()


# ---------------------------------------------------------------------------
# 1. _materialize_to_stream round-trip
# ---------------------------------------------------------------------------


class TestMaterializeToStream:
    def test_round_trip_preserves_data(self):
        stream = make_stream(5)
        rows = list(stream.iter_data())
        rebuilt = StaticOutputOperatorPod._materialize_to_stream(rows)

        original_table = stream.as_table()
        rebuilt_table = rebuilt.as_table()

        assert (
            original_table.column("id").to_pylist()
            == rebuilt_table.column("id").to_pylist()
        )
        assert (
            original_table.column("x").to_pylist()
            == rebuilt_table.column("x").to_pylist()
        )

    def test_round_trip_preserves_schema(self):
        stream = make_stream(3)
        rows = list(stream.iter_data())
        rebuilt = StaticOutputOperatorPod._materialize_to_stream(rows)

        orig_tag, orig_pkt = stream.output_schema()
        rebuilt_tag, rebuilt_pkt = rebuilt.output_schema()
        assert dict(orig_tag) == dict(rebuilt_tag)
        assert dict(orig_pkt) == dict(rebuilt_pkt)

    def test_empty_rows_raises(self):
        with pytest.raises(ValueError, match="empty"):
            StaticOutputOperatorPod._materialize_to_stream([])

    def test_round_trip_two_col_stream(self):
        stream = make_two_col_stream(4)
        rows = list(stream.iter_data())
        rebuilt = StaticOutputOperatorPod._materialize_to_stream(rows)

        original = stream.as_table()
        rebuilt_t = rebuilt.as_table()
        assert original.column("x").to_pylist() == rebuilt_t.column("x").to_pylist()
        assert original.column("y").to_pylist() == rebuilt_t.column("y").to_pylist()


# ---------------------------------------------------------------------------
# 3. PythonDataFunction.direct_async_call
# ---------------------------------------------------------------------------


class TestDirectAsyncCall:
    @pytest.mark.asyncio
    async def test_direct_async_call_returns_correct_result(self):
        def add(x: int, y: int) -> int:
            return x + y

        pf = PythonDataFunction(add, output_keys="result")
        data = Data({"x": 3, "y": 5})
        result = await pf.direct_async_call(data)
        assert result is not None
        assert result.as_dict()["result"] == 8

    @pytest.mark.asyncio
    async def test_async_call_multiple_data(self):
        def double(x: int) -> int:
            return x * 2

        pf = PythonDataFunction(double, output_keys="result")
        results = await asyncio.gather(
            pf.async_call(Data({"x": 1})),
            pf.async_call(Data({"x": 2})),
            pf.async_call(Data({"x": 3})),
        )
        assert all(r is not None for r in results)
        values = [r.as_dict()["result"] for r in results if r is not None]
        assert values == [2, 4, 6]

    @pytest.mark.asyncio
    async def test_async_call_runs_in_thread(self):
        """Verify the function actually runs (proves run_in_executor works)."""
        import threading

        call_threads = []

        def record_thread(x: int) -> int:
            call_threads.append(threading.current_thread().name)
            return x

        pf = PythonDataFunction(record_thread, output_keys="result")
        result = await pf.direct_async_call(Data({"x": 42}))
        assert result is not None
        assert len(call_threads) == 1


# ---------------------------------------------------------------------------
# 4. UnaryOperator barrier-mode async_execute
# ---------------------------------------------------------------------------


class TestUnaryOperatorAsyncExecute:
    @pytest.mark.asyncio
    async def test_select_tag_columns(self):
        stream = make_two_col_stream(3)
        op = SelectTagColumns(["id"])

        input_ch = Channel(buffer_size=16)
        output_ch = Channel(buffer_size=16)

        await feed_stream_to_channel(stream, input_ch)
        await op.async_execute([input_ch.reader], output_ch.writer)

        results = await output_ch.reader.collect()
        assert len(results) == 3
        for tag, data in results:
            assert "id" in tag.keys()

    @pytest.mark.asyncio
    async def test_select_data_columns(self):
        stream = make_two_col_stream(3)
        op = SelectDataColumns(["x"])

        input_ch = Channel(buffer_size=16)
        output_ch = Channel(buffer_size=16)

        await feed_stream_to_channel(stream, input_ch)
        await op.async_execute([input_ch.reader], output_ch.writer)

        results = await output_ch.reader.collect()
        assert len(results) == 3
        for _, data in results:
            pkt_dict = data.as_dict()
            assert "x" in pkt_dict
            assert "y" not in pkt_dict

    @pytest.mark.asyncio
    async def test_drop_data_columns(self):
        stream = make_two_col_stream(3)
        op = DropDataColumns(["y"])

        input_ch = Channel(buffer_size=16)
        output_ch = Channel(buffer_size=16)

        await feed_stream_to_channel(stream, input_ch)
        await op.async_execute([input_ch.reader], output_ch.writer)

        results = await output_ch.reader.collect()
        assert len(results) == 3
        for _, data in results:
            pkt_dict = data.as_dict()
            assert "x" in pkt_dict
            assert "y" not in pkt_dict

    @pytest.mark.asyncio
    async def test_drop_tag_columns(self):
        # Need multi-tag stream
        table = pa.table(
            {
                "a": pa.array([1, 2], type=pa.int64()),
                "b": pa.array([10, 20], type=pa.int64()),
                "x": pa.array([100, 200], type=pa.int64()),
            }
        )
        stream = ArrowTableStream(table, tag_columns=["a", "b"])
        op = DropTagColumns(["b"])

        input_ch = Channel(buffer_size=16)
        output_ch = Channel(buffer_size=16)

        await feed_stream_to_channel(stream, input_ch)
        await op.async_execute([input_ch.reader], output_ch.writer)

        results = await output_ch.reader.collect()
        assert len(results) == 2
        for tag, _ in results:
            tag_keys = tag.keys()
            assert "a" in tag_keys
            assert "b" not in tag_keys

    @pytest.mark.asyncio
    async def test_map_tags(self):
        stream = make_stream(3)
        op = MapTags({"id": "row_id"}, drop_unmapped=True)

        input_ch = Channel(buffer_size=16)
        output_ch = Channel(buffer_size=16)

        await feed_stream_to_channel(stream, input_ch)
        await op.async_execute([input_ch.reader], output_ch.writer)

        results = await output_ch.reader.collect()
        assert len(results) == 3
        for tag, _ in results:
            assert "row_id" in tag.keys()
            assert "id" not in tag.keys()

    @pytest.mark.asyncio
    async def test_map_data(self):
        stream = make_stream(3)
        op = MapData({"x": "value"}, drop_unmapped=True)

        input_ch = Channel(buffer_size=16)
        output_ch = Channel(buffer_size=16)

        await feed_stream_to_channel(stream, input_ch)
        await op.async_execute([input_ch.reader], output_ch.writer)

        results = await output_ch.reader.collect()
        assert len(results) == 3
        for _, data in results:
            pkt_dict = data.as_dict()
            assert "value" in pkt_dict
            assert "x" not in pkt_dict

    @pytest.mark.asyncio
    async def test_polars_filter(self):
        stream = make_stream(5)
        op = PolarsFilter(constraints={"id": 2})

        input_ch = Channel(buffer_size=16)
        output_ch = Channel(buffer_size=16)

        await feed_stream_to_channel(stream, input_ch)
        await op.async_execute([input_ch.reader], output_ch.writer)

        results = await output_ch.reader.collect()
        assert len(results) == 1
        tag, data = results[0]
        assert tag.as_dict()["id"] == 2
        assert data.as_dict()["x"] == 2

    @pytest.mark.asyncio
    async def test_batch_operator(self):
        stream = make_stream(6)
        op = Batch(batch_size=2)

        input_ch = Channel(buffer_size=16)
        output_ch = Channel(buffer_size=16)

        await feed_stream_to_channel(stream, input_ch)
        await op.async_execute([input_ch.reader], output_ch.writer)

        results = await output_ch.reader.collect()
        assert len(results) == 3  # 6 rows / batch_size=2


# ---------------------------------------------------------------------------
# 5. BinaryOperator barrier-mode async_execute
# ---------------------------------------------------------------------------


class TestBinaryOperatorAsyncExecute:
    @pytest.mark.asyncio
    async def test_semi_join(self):
        left = make_stream(5)
        right_table = pa.table(
            {
                "id": pa.array([1, 3], type=pa.int64()),
                "z": pa.array([100, 300], type=pa.int64()),
            }
        )
        right = ArrowTableStream(right_table, tag_columns=["id"])

        op = SemiJoin()

        left_ch = Channel(buffer_size=16)
        right_ch = Channel(buffer_size=16)
        output_ch = Channel(buffer_size=16)

        await feed_stream_to_channel(left, left_ch)
        await feed_stream_to_channel(right, right_ch)

        await op.async_execute([left_ch.reader, right_ch.reader], output_ch.writer)

        results = await output_ch.reader.collect()
        ids = sorted(tag.as_dict()["id"] for tag, _ in results)
        assert ids == [1, 3]

    @pytest.mark.asyncio
    async def test_merge_join(self):
        left_table = pa.table(
            {
                "id": pa.array([0, 1], type=pa.int64()),
                "val": pa.array([10, 20], type=pa.int64()),
            }
        )
        right_table = pa.table(
            {
                "id": pa.array([0, 1], type=pa.int64()),
                "val": pa.array([100, 200], type=pa.int64()),
            }
        )
        left = ArrowTableStream(left_table, tag_columns=["id"])
        right = ArrowTableStream(right_table, tag_columns=["id"])

        op = MergeJoin()

        left_ch = Channel(buffer_size=16)
        right_ch = Channel(buffer_size=16)
        output_ch = Channel(buffer_size=16)

        await feed_stream_to_channel(left, left_ch)
        await feed_stream_to_channel(right, right_ch)

        await op.async_execute([left_ch.reader, right_ch.reader], output_ch.writer)

        results = await output_ch.reader.collect()
        assert len(results) == 2


# ---------------------------------------------------------------------------
# 6. NonZeroInputOperator barrier-mode async_execute (Join)
# ---------------------------------------------------------------------------


class TestJoinAsyncExecute:
    @pytest.mark.asyncio
    async def test_two_way_join(self):
        left_table = pa.table(
            {
                "id": pa.array([0, 1, 2], type=pa.int64()),
                "x": pa.array([10, 20, 30], type=pa.int64()),
            }
        )
        right_table = pa.table(
            {
                "id": pa.array([0, 1, 2], type=pa.int64()),
                "y": pa.array([100, 200, 300], type=pa.int64()),
            }
        )
        left = ArrowTableStream(left_table, tag_columns=["id"])
        right = ArrowTableStream(right_table, tag_columns=["id"])

        op = Join()

        left_ch = Channel(buffer_size=16)
        right_ch = Channel(buffer_size=16)
        output_ch = Channel(buffer_size=16)

        await feed_stream_to_channel(left, left_ch)
        await feed_stream_to_channel(right, right_ch)

        await op.async_execute([left_ch.reader, right_ch.reader], output_ch.writer)

        results = await output_ch.reader.collect()
        assert len(results) == 3

        # Verify all tag values present
        ids = sorted(tag.as_dict()["id"] for tag, _ in results)
        assert ids == [0, 1, 2]

        # Verify both data columns present
        for _, data in results:
            pkt = data.as_dict()
            assert "x" in pkt
            assert "y" in pkt


# ---------------------------------------------------------------------------
# 7. FunctionPod streaming async_execute
# ---------------------------------------------------------------------------


class TestFunctionPodAsyncExecute:
    @pytest.mark.asyncio
    async def test_basic_streaming(self):
        def double(x: int) -> int:
            return x * 2

        pf = PythonDataFunction(double, output_keys="result")
        pod = FunctionPod(pf)

        stream = make_stream(5)
        input_ch = Channel(buffer_size=16)
        output_ch = Channel(buffer_size=16)

        await feed_stream_to_channel(stream, input_ch)
        await pod.async_execute([input_ch.reader], output_ch.writer)

        results = await output_ch.reader.collect()
        assert len(results) == 5

        values = sorted(pkt.as_dict()["result"] for _, pkt in results)
        assert values == [0, 2, 4, 6, 8]

    @pytest.mark.asyncio
    async def test_two_input_keys(self):
        def add(x: int, y: int) -> int:
            return x + y

        pf = PythonDataFunction(add, output_keys="result")
        pod = FunctionPod(pf)

        stream = make_two_col_stream(3)
        input_ch = Channel(buffer_size=16)
        output_ch = Channel(buffer_size=16)

        await feed_stream_to_channel(stream, input_ch)
        await pod.async_execute([input_ch.reader], output_ch.writer)

        results = await output_ch.reader.collect()
        assert len(results) == 3
        values = sorted(pkt.as_dict()["result"] for _, pkt in results)
        assert values == [0, 11, 22]

    @pytest.mark.asyncio
    async def test_tags_pass_through(self):
        """FunctionPod should preserve the input tag for each output."""

        def noop(x: int) -> int:
            return x

        pf = PythonDataFunction(noop, output_keys="result")
        pod = FunctionPod(pf)

        stream = make_stream(3)
        input_ch = Channel(buffer_size=16)
        output_ch = Channel(buffer_size=16)

        await feed_stream_to_channel(stream, input_ch)
        await pod.async_execute([input_ch.reader], output_ch.writer)

        results = await output_ch.reader.collect()
        ids = sorted(tag.as_dict()["id"] for tag, _ in results)
        assert ids == [0, 1, 2]

    @pytest.mark.asyncio
    async def test_empty_input(self):
        """No items in → no items out, channel closed cleanly."""

        def double(x: int) -> int:
            return x * 2

        pf = PythonDataFunction(double, output_keys="result")
        pod = FunctionPod(pf)

        input_ch = Channel(buffer_size=4)
        output_ch = Channel(buffer_size=4)

        await input_ch.writer.close()
        await pod.async_execute([input_ch.reader], output_ch.writer)

        results = await output_ch.reader.collect()
        assert results == []


# ---------------------------------------------------------------------------
# 8. FunctionPod concurrency control
# ---------------------------------------------------------------------------


class TestFunctionPodConcurrency:
    @pytest.mark.asyncio
    async def test_max_concurrency_limits_in_flight(self):
        """With max_concurrency=1, data should be processed sequentially."""
        processing_order = []

        def record_order(x: int) -> int:
            processing_order.append(x)
            return x

        pf = PythonDataFunction(record_order, output_keys="result")
        pod = FunctionPod(pf, node_config=NodeConfig(max_concurrency=1))

        stream = make_stream(5)
        input_ch = Channel(buffer_size=16)
        output_ch = Channel(buffer_size=16)

        await feed_stream_to_channel(stream, input_ch)
        await pod.async_execute([input_ch.reader], output_ch.writer)

        results = await output_ch.reader.collect()
        assert len(results) == 5

    @pytest.mark.asyncio
    async def test_unlimited_concurrency(self):
        """With max_concurrency=None, all data run concurrently."""

        def double(x: int) -> int:
            return x * 2

        pf = PythonDataFunction(double, output_keys="result")
        pod = FunctionPod(pf, node_config=NodeConfig(max_concurrency=None))
        pipeline_cfg = PipelineConfig(default_max_concurrency=None)

        stream = make_stream(10)
        input_ch = Channel(buffer_size=32)
        output_ch = Channel(buffer_size=32)

        await feed_stream_to_channel(stream, input_ch)
        await pod.async_execute(
            [input_ch.reader], output_ch.writer, pipeline_config=pipeline_cfg
        )

        results = await output_ch.reader.collect()
        assert len(results) == 10
        values = sorted(pkt.as_dict()["result"] for _, pkt in results)
        assert values == [i * 2 for i in range(10)]

    @pytest.mark.asyncio
    async def test_pipeline_config_concurrency_fallback(self):
        """NodeConfig inherits from PipelineConfig when not overridden."""

        def double(x: int) -> int:
            return x * 2

        pf = PythonDataFunction(double, output_keys="result")
        pod = FunctionPod(pf)  # NodeConfig default (None)
        pipeline_cfg = PipelineConfig(default_max_concurrency=2)

        stream = make_stream(4)
        input_ch = Channel(buffer_size=16)
        output_ch = Channel(buffer_size=16)

        await feed_stream_to_channel(stream, input_ch)
        await pod.async_execute(
            [input_ch.reader], output_ch.writer, pipeline_config=pipeline_cfg
        )

        results = await output_ch.reader.collect()
        assert len(results) == 4


# ---------------------------------------------------------------------------
# 9. End-to-end multi-stage async pipeline
# ---------------------------------------------------------------------------


class TestEndToEndPipeline:
    @pytest.mark.asyncio
    async def test_source_filter_function_chain(self):
        """Source → Filter → FunctionPod, wired with channels."""
        import polars as pl

        # Setup
        stream = make_stream(10)
        filter_op = PolarsFilter(predicates=(pl.col("id").is_in([1, 3, 5, 7]),))

        def triple(x: int) -> int:
            return x * 3

        func_pod = FunctionPod(PythonDataFunction(triple, output_keys="result"))

        # Channels
        ch1 = Channel(buffer_size=16)
        ch2 = Channel(buffer_size=16)
        ch3 = Channel(buffer_size=16)

        # Wire
        async def source():
            for tag, data in stream.iter_data():
                await ch1.writer.send((tag, data))
            await ch1.writer.close()

        async with asyncio.TaskGroup() as tg:
            tg.create_task(source())
            tg.create_task(filter_op.async_execute([ch1.reader], ch2.writer))
            tg.create_task(func_pod.async_execute([ch2.reader], ch3.writer))

        results = await ch3.reader.collect()
        assert len(results) == 4

        result_map = {
            tag.as_dict()["id"]: pkt.as_dict()["result"] for tag, pkt in results
        }
        assert result_map[1] == 3
        assert result_map[3] == 9
        assert result_map[5] == 15
        assert result_map[7] == 21

    @pytest.mark.asyncio
    async def test_source_join_function_chain(self):
        """Two sources → Join → FunctionPod, wired with channels."""
        left_table = pa.table(
            {
                "id": pa.array([0, 1, 2], type=pa.int64()),
                "x": pa.array([10, 20, 30], type=pa.int64()),
            }
        )
        right_table = pa.table(
            {
                "id": pa.array([0, 1, 2], type=pa.int64()),
                "y": pa.array([1, 2, 3], type=pa.int64()),
            }
        )
        left_stream = ArrowTableStream(left_table, tag_columns=["id"])
        right_stream = ArrowTableStream(right_table, tag_columns=["id"])

        def add(x: int, y: int) -> int:
            return x + y

        join_op = Join()
        func_pod = FunctionPod(PythonDataFunction(add, output_keys="result"))

        ch_left = Channel(buffer_size=16)
        ch_right = Channel(buffer_size=16)
        ch_joined = Channel(buffer_size=16)
        ch_out = Channel(buffer_size=16)

        async def push(stream, ch):
            for tag, data in stream.iter_data():
                await ch.writer.send((tag, data))
            await ch.writer.close()

        async with asyncio.TaskGroup() as tg:
            tg.create_task(push(left_stream, ch_left))
            tg.create_task(push(right_stream, ch_right))
            tg.create_task(
                join_op.async_execute(
                    [ch_left.reader, ch_right.reader], ch_joined.writer
                )
            )
            tg.create_task(func_pod.async_execute([ch_joined.reader], ch_out.writer))

        results = await ch_out.reader.collect()
        assert len(results) == 3

        result_map = {
            tag.as_dict()["id"]: pkt.as_dict()["result"] for tag, pkt in results
        }
        assert result_map[0] == 11  # 10 + 1
        assert result_map[1] == 22  # 20 + 2
        assert result_map[2] == 33  # 30 + 3


# ---------------------------------------------------------------------------
# 10. Error propagation
# ---------------------------------------------------------------------------


class TestErrorPropagation:
    @pytest.mark.asyncio
    async def test_function_exception_returns_none(self):
        """An exception in the data function is caught by process_data — no raise."""

        def failing(x: int) -> int:
            if x == 2:
                raise ValueError("boom")
            return x

        pf = PythonDataFunction(failing, output_keys="result")
        pod = FunctionPod(pf)

        stream = make_stream(5)
        input_ch = Channel(buffer_size=16)
        output_ch = Channel(buffer_size=16)

        await feed_stream_to_channel(stream, input_ch)
        await pod.async_execute([input_ch.reader], output_ch.writer)

        results = await output_ch.reader.collect()
        # The failing data (x=2) is silently dropped; 4 of 5 succeed
        assert len(results) == 4
        values = sorted(pkt.as_dict()["result"] for _, pkt in results)
        assert values == [0, 1, 3, 4]

    @pytest.mark.asyncio
    async def test_direct_async_call_raises_on_failure(self):
        """direct_async_call re-raises the exception on error."""

        def failing(x: int) -> int:
            raise ValueError("boom")

        pf = PythonDataFunction(failing, output_keys="result")
        with pytest.raises(ValueError, match="boom"):
            await pf.direct_async_call(Data({"x": 1}))


# ---------------------------------------------------------------------------
# 11. Sync behavior unchanged
# ---------------------------------------------------------------------------


class TestSyncBehaviorUnchanged:
    """Verify that adding async_execute doesn't break the existing sync path."""

    def test_function_pod_sync_process_still_works(self):
        def double(x: int) -> int:
            return x * 2

        pf = PythonDataFunction(double, output_keys="result")
        pod = FunctionPod(pf)

        stream = make_stream(3)
        output = pod.process(stream)
        results = list(output.iter_data())
        assert len(results) == 3
        values = [pkt.as_dict()["result"] for _, pkt in results]
        assert values == [0, 2, 4]

    def test_operator_sync_process_still_works(self):
        import polars as pl

        stream = make_stream(5)
        op = PolarsFilter(predicates=(pl.col("id").is_in([1, 3]),))
        output = op.process(stream)
        results = list(output.iter_data())
        ids = sorted(cast(int, tag.as_dict()["id"]) for tag, _ in results)
        assert ids == [1, 3]

    def test_join_sync_process_still_works(self):
        left_table = pa.table(
            {
                "id": pa.array([0, 1], type=pa.int64()),
                "x": pa.array([10, 20], type=pa.int64()),
            }
        )
        right_table = pa.table(
            {
                "id": pa.array([0, 1], type=pa.int64()),
                "y": pa.array([100, 200], type=pa.int64()),
            }
        )
        left = ArrowTableStream(left_table, tag_columns=["id"])
        right = ArrowTableStream(right_table, tag_columns=["id"])

        join = Join()
        output = join.process(left, right)
        results = list(output.iter_data())
        assert len(results) == 2

    def test_function_pod_with_node_config_sync_still_works(self):
        """NodeConfig should be ignored in sync mode."""

        def add(x: int, y: int) -> int:
            return x + y

        pf = PythonDataFunction(add, output_keys="result")
        pod = FunctionPod(pf, node_config=NodeConfig(max_concurrency=2))

        stream = make_two_col_stream(3)
        output = pod.process(stream)
        results = list(output.iter_data())
        assert len(results) == 3
        values = sorted(cast(int, pkt.as_dict()["result"]) for _, pkt in results)
        assert values == [0, 11, 22]
