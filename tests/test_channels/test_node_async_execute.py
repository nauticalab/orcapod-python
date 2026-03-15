"""
Tests for async_execute on Node classes.

Covers:
- CachedPacketFunction.async_call with cache support
- FunctionNode.async_execute basic streaming
- FunctionNode.async_execute two-phase logic
- OperatorNode.async_execute delegation
- OperatorNode.async_execute with cache modes
- execute_packet / async_process_packet routing
"""

from __future__ import annotations

import asyncio

import pyarrow as pa
import pytest

from orcapod.channels import Channel
from orcapod.core.datagrams import Packet
from orcapod.core.function_pod import FunctionPod
from orcapod.core.nodes import (
    FunctionNode,
    OperatorNode,
)
from orcapod.core.operators import SelectPacketColumns
from orcapod.core.operators.join import Join
from orcapod.core.operators.semijoin import SemiJoin
from orcapod.core.packet_function import CachedPacketFunction, PythonPacketFunction
from orcapod.core.streams import ArrowTableStream
from orcapod.databases import InMemoryArrowDatabase
from orcapod.types import CacheMode, NodeConfig


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_stream(n: int = 5) -> ArrowTableStream:
    table = pa.table(
        {
            "id": pa.array(list(range(n)), type=pa.int64()),
            "x": pa.array(list(range(n)), type=pa.int64()),
        }
    )
    return ArrowTableStream(table, tag_columns=["id"])


def make_two_col_stream(n: int = 3) -> ArrowTableStream:
    table = pa.table(
        {
            "id": pa.array(list(range(n)), type=pa.int64()),
            "x": pa.array(list(range(n)), type=pa.int64()),
            "y": pa.array([i * 10 + i for i in range(n)], type=pa.int64()),
        }
    )
    return ArrowTableStream(table, tag_columns=["id"])


async def feed_stream_to_channel(stream: ArrowTableStream, ch: Channel) -> None:
    """Push all (tag, packet) pairs from a stream into a channel, then close."""
    for tag, packet in stream.iter_packets():
        await ch.writer.send((tag, packet))
    await ch.writer.close()


def make_double_pod() -> tuple[PythonPacketFunction, FunctionPod]:
    def double(x: int) -> int:
        return x * 2

    pf = PythonPacketFunction(double, output_keys="result")
    pod = FunctionPod(pf)
    return pf, pod


# ---------------------------------------------------------------------------
# 1. CachedPacketFunction.async_call
# ---------------------------------------------------------------------------


class TestCachedPacketFunctionAsync:
    @pytest.mark.asyncio
    async def test_async_call_cache_miss_computes_and_records(self):
        def double(x: int) -> int:
            return x * 2

        pf = PythonPacketFunction(double, output_keys="result")
        db = InMemoryArrowDatabase()
        cpf = CachedPacketFunction(pf, result_database=db)

        packet = Packet({"x": 5})
        result = await cpf.async_call(packet)

        assert result is not None
        assert result.as_dict()["result"] == 10
        # Check that result was recorded in DB
        cached = cpf.get_cached_output_for_packet(packet)
        assert cached is not None
        assert cached.as_dict()["result"] == 10

    @pytest.mark.asyncio
    async def test_async_call_cache_hit_returns_cached(self):
        def double(x: int) -> int:
            return x * 2

        pf = PythonPacketFunction(double, output_keys="result")
        db = InMemoryArrowDatabase()
        cpf = CachedPacketFunction(pf, result_database=db)

        packet = Packet({"x": 5})
        # First call — computes
        result1 = await cpf.async_call(packet)
        assert result1 is not None
        # Has RESULT_COMPUTED_FLAG
        assert result1.get_meta_value(cpf.RESULT_COMPUTED_FLAG, False) is True

        # Second call — should hit cache (no RESULT_COMPUTED_FLAG set to True)
        result2 = await cpf.async_call(packet)
        assert result2 is not None
        assert result2.as_dict()["result"] == 10
        # Cache hit should NOT have RESULT_COMPUTED_FLAG=True
        # (the flag is only set on freshly computed results)
        assert result2.get_meta_value(cpf.RESULT_COMPUTED_FLAG, None) is not True

    @pytest.mark.asyncio
    async def test_async_call_skip_cache_lookup(self):
        call_count = 0

        def counting_double(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x * 2

        pf = PythonPacketFunction(counting_double, output_keys="result")
        db = InMemoryArrowDatabase()
        cpf = CachedPacketFunction(pf, result_database=db)

        packet = Packet({"x": 5})
        await cpf.async_call(packet)
        assert call_count == 1

        # With skip_cache_lookup, should recompute
        await cpf.async_call(packet, skip_cache_lookup=True)
        assert call_count == 2

    @pytest.mark.asyncio
    async def test_async_call_skip_cache_insert(self):
        def double(x: int) -> int:
            return x * 2

        pf = PythonPacketFunction(double, output_keys="result")
        db = InMemoryArrowDatabase()
        cpf = CachedPacketFunction(pf, result_database=db)

        packet = Packet({"x": 5})
        result = await cpf.async_call(packet, skip_cache_insert=True)
        assert result is not None
        assert result.as_dict()["result"] == 10

        # Should NOT be cached
        cached = cpf.get_cached_output_for_packet(packet)
        assert cached is None


# ---------------------------------------------------------------------------
# 3. FunctionNode.async_execute
# ---------------------------------------------------------------------------


class TestFunctionNodeAsyncExecute:
    @pytest.mark.asyncio
    async def test_basic_streaming_matches_sync(self):
        _, pod = make_double_pod()
        stream = make_stream(5)

        # Sync results
        node_sync = FunctionNode(pod, stream)
        sync_results = list(node_sync.iter_packets())
        sync_values = sorted(pkt.as_dict()["result"] for _, pkt in sync_results)

        # Async results
        node_async = FunctionNode(pod, make_stream(5))
        input_ch = Channel(buffer_size=16)
        output_ch = Channel(buffer_size=16)

        await feed_stream_to_channel(make_stream(5), input_ch)
        await node_async.async_execute(input_ch.reader, output_ch.writer)

        async_results = await output_ch.reader.collect()
        async_values = sorted(pkt.as_dict()["result"] for _, pkt in async_results)
        assert async_values == sync_values

    @pytest.mark.asyncio
    async def test_empty_input_closes_cleanly(self):
        _, pod = make_double_pod()
        node = FunctionNode(pod, make_stream(1))

        input_ch = Channel(buffer_size=4)
        output_ch = Channel(buffer_size=4)

        await input_ch.writer.close()
        await node.async_execute(input_ch.reader, output_ch.writer)

        results = await output_ch.reader.collect()
        assert results == []

    @pytest.mark.asyncio
    async def test_tags_preserved(self):
        """Tags should pass through unchanged."""
        _, pod = make_double_pod()
        node = FunctionNode(pod, make_stream(3))

        input_ch = Channel(buffer_size=16)
        output_ch = Channel(buffer_size=16)

        await feed_stream_to_channel(make_stream(3), input_ch)
        await node.async_execute(input_ch.reader, output_ch.writer)

        results = await output_ch.reader.collect()
        ids = sorted(tag.as_dict()["id"] for tag, _ in results)
        assert ids == [0, 1, 2]


# ---------------------------------------------------------------------------
# 4. FunctionNode.async_execute
# ---------------------------------------------------------------------------


class TestFunctionNodeAsyncExecute:
    @pytest.mark.asyncio
    async def test_no_cache_processes_all_inputs(self):
        """With an empty DB, all inputs should be computed."""
        pf, pod = make_double_pod()
        db = InMemoryArrowDatabase()
        stream = make_stream(3)
        node = FunctionNode(pod, stream, pipeline_database=db)

        input_ch = Channel(buffer_size=16)
        output_ch = Channel(buffer_size=16)

        await feed_stream_to_channel(make_stream(3), input_ch)
        await node.async_execute(input_ch.reader, output_ch.writer)

        results = await output_ch.reader.collect()
        assert len(results) == 3
        values = sorted(pkt.as_dict()["result"] for _, pkt in results)
        assert values == [0, 2, 4]

    @pytest.mark.asyncio
    async def test_sync_run_then_async_emits_from_cache(self):
        """After sync run() populates DB, async should emit cached results."""
        pf, pod = make_double_pod()
        db = InMemoryArrowDatabase()
        stream = make_stream(3)

        # Sync run to populate DB
        node1 = FunctionNode(pod, stream, pipeline_database=db)
        node1.run()

        # New node with same DB — Phase 1 should emit cached
        node2 = FunctionNode(pod, make_stream(3), pipeline_database=db)

        input_ch = Channel(buffer_size=16)
        output_ch = Channel(buffer_size=16)

        # Close input immediately — no new packets
        await input_ch.writer.close()
        await node2.async_execute(input_ch.reader, output_ch.writer)

        results = await output_ch.reader.collect()
        assert len(results) == 3
        values = sorted(pkt.as_dict()["result"] for _, pkt in results)
        assert values == [0, 2, 4]

    @pytest.mark.asyncio
    async def test_two_phase_cached_and_new(self):
        """Phase 1 emits cached; Phase 2 computes new."""
        pf, pod = make_double_pod()
        db = InMemoryArrowDatabase()

        # Sync run with 3 items to populate DB
        stream = make_stream(3)
        node1 = FunctionNode(pod, stream, pipeline_database=db)
        node1.run()

        # Now run async with 5 items (3 cached + 2 new)
        node2 = FunctionNode(pod, make_stream(5), pipeline_database=db)
        input_ch = Channel(buffer_size=16)
        output_ch = Channel(buffer_size=16)

        await feed_stream_to_channel(make_stream(5), input_ch)
        await node2.async_execute(input_ch.reader, output_ch.writer)

        results = await output_ch.reader.collect()
        values = sorted(pkt.as_dict()["result"] for _, pkt in results)
        # 3 from cache + 2 new = 5 total
        assert values == [0, 2, 4, 6, 8]

    @pytest.mark.asyncio
    async def test_concurrent_execution_with_async_function(self):
        """Async packets should run concurrently when max_concurrency > 1."""
        import time

        async def slow_double(x: int) -> int:
            await asyncio.sleep(0.2)
            return x * 2

        pf = PythonPacketFunction(slow_double, output_keys="result")
        pod = FunctionPod(pf, node_config=NodeConfig(max_concurrency=5))
        db = InMemoryArrowDatabase()
        stream = make_stream(5)
        node = FunctionNode(pod, stream, pipeline_database=db)

        input_ch = Channel(buffer_size=16)
        output_ch = Channel(buffer_size=16)

        await feed_stream_to_channel(make_stream(5), input_ch)

        t0 = time.perf_counter()
        await node.async_execute(input_ch.reader, output_ch.writer)
        elapsed = time.perf_counter() - t0

        results = await output_ch.reader.collect()
        assert len(results) == 5
        values = sorted(pkt.as_dict()["result"] for _, pkt in results)
        assert values == [0, 2, 4, 6, 8]

        # Concurrency limiting removed in PLT-922 (deferred to PLT-930).
        # Packets are now processed sequentially.
        assert elapsed >= 0.9, f"Expected sequential execution but took {elapsed:.2f}s"

    @pytest.mark.asyncio
    async def test_db_records_created(self):
        """Async execute should create pipeline records in the DB."""
        pf, pod = make_double_pod()
        db = InMemoryArrowDatabase()
        stream = make_stream(3)
        node = FunctionNode(pod, stream, pipeline_database=db)

        input_ch = Channel(buffer_size=16)
        output_ch = Channel(buffer_size=16)

        await feed_stream_to_channel(make_stream(3), input_ch)
        await node.async_execute(input_ch.reader, output_ch.writer)
        await output_ch.reader.collect()

        # Verify records in DB
        records = node.get_all_records()
        assert records is not None
        assert records.num_rows == 3


# ---------------------------------------------------------------------------
# 5. OperatorNode.async_execute
# ---------------------------------------------------------------------------


class TestOperatorNodeAsyncExecute:
    @pytest.mark.asyncio
    async def test_unary_op_delegation(self):
        stream = make_two_col_stream(3)
        op = SelectPacketColumns(["x"])
        node = OperatorNode(op, [stream])

        input_ch = Channel(buffer_size=16)
        output_ch = Channel(buffer_size=16)

        await feed_stream_to_channel(make_two_col_stream(3), input_ch)
        await node.async_execute([input_ch.reader], output_ch.writer)

        results = await output_ch.reader.collect()
        assert len(results) == 3
        for _, packet in results:
            pkt_dict = packet.as_dict()
            assert "x" in pkt_dict
            assert "y" not in pkt_dict

    @pytest.mark.asyncio
    async def test_binary_op_delegation(self):
        left = make_stream(5)
        right_table = pa.table(
            {
                "id": pa.array([1, 3], type=pa.int64()),
                "z": pa.array([100, 300], type=pa.int64()),
            }
        )
        right = ArrowTableStream(right_table, tag_columns=["id"])

        op = SemiJoin()
        node = OperatorNode(op, [left, right])

        left_ch = Channel(buffer_size=16)
        right_ch = Channel(buffer_size=16)
        output_ch = Channel(buffer_size=16)

        await feed_stream_to_channel(make_stream(5), left_ch)
        await feed_stream_to_channel(
            ArrowTableStream(right_table, tag_columns=["id"]), right_ch
        )
        await node.async_execute([left_ch.reader, right_ch.reader], output_ch.writer)

        results = await output_ch.reader.collect()
        ids = sorted(tag.as_dict()["id"] for tag, _ in results)
        assert ids == [1, 3]

    @pytest.mark.asyncio
    async def test_nary_op_delegation(self):
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
        node = OperatorNode(op, [left, right])

        left_ch = Channel(buffer_size=16)
        right_ch = Channel(buffer_size=16)
        output_ch = Channel(buffer_size=16)

        await feed_stream_to_channel(
            ArrowTableStream(left_table, tag_columns=["id"]), left_ch
        )
        await feed_stream_to_channel(
            ArrowTableStream(right_table, tag_columns=["id"]), right_ch
        )
        await node.async_execute([left_ch.reader, right_ch.reader], output_ch.writer)

        results = await output_ch.reader.collect()
        assert len(results) == 3
        ids = sorted(tag.as_dict()["id"] for tag, _ in results)
        assert ids == [0, 1, 2]

    @pytest.mark.asyncio
    async def test_results_match_sync(self):
        stream = make_two_col_stream(4)
        op = SelectPacketColumns(["x"])

        # Sync
        node_sync = OperatorNode(op, [stream])
        node_sync.run()
        sync_table = node_sync.as_table()
        sync_x = sorted(sync_table.column("x").to_pylist())

        # Async
        node_async = OperatorNode(op, [make_two_col_stream(4)])
        input_ch = Channel(buffer_size=16)
        output_ch = Channel(buffer_size=16)

        await feed_stream_to_channel(make_two_col_stream(4), input_ch)
        await node_async.async_execute([input_ch.reader], output_ch.writer)

        results = await output_ch.reader.collect()
        async_x = sorted(pkt.as_dict()["x"] for _, pkt in results)
        assert async_x == sync_x


# ---------------------------------------------------------------------------
# 6. OperatorNode.async_execute
# ---------------------------------------------------------------------------


class TestOperatorNodeAsyncExecute:
    @pytest.mark.asyncio
    async def test_off_mode_no_db_write(self):
        stream = make_two_col_stream(3)
        op = SelectPacketColumns(["x"])
        db = InMemoryArrowDatabase()
        node = OperatorNode(
            op, [stream], pipeline_database=db, cache_mode=CacheMode.OFF
        )

        input_ch = Channel(buffer_size=16)
        output_ch = Channel(buffer_size=16)

        await feed_stream_to_channel(make_two_col_stream(3), input_ch)
        await node.async_execute([input_ch.reader], output_ch.writer)

        results = await output_ch.reader.collect()
        assert len(results) == 3

        # DB should be empty (OFF mode)
        records = node.get_all_records()
        assert records is None

    @pytest.mark.asyncio
    async def test_log_mode_stores_results(self):
        stream = make_two_col_stream(3)
        op = SelectPacketColumns(["x"])
        db = InMemoryArrowDatabase()
        node = OperatorNode(
            op, [stream], pipeline_database=db, cache_mode=CacheMode.LOG
        )

        input_ch = Channel(buffer_size=16)
        output_ch = Channel(buffer_size=16)

        await feed_stream_to_channel(make_two_col_stream(3), input_ch)
        await node.async_execute([input_ch.reader], output_ch.writer)

        results = await output_ch.reader.collect()
        assert len(results) == 3

        # DB should have records (LOG mode)
        records = node.get_all_records()
        assert records is not None
        assert records.num_rows == 3

    @pytest.mark.asyncio
    async def test_replay_mode_emits_from_db(self):
        stream = make_two_col_stream(3)
        op = SelectPacketColumns(["x"])
        db = InMemoryArrowDatabase()

        # First: sync LOG to populate DB
        node1 = OperatorNode(
            op, [stream], pipeline_database=db, cache_mode=CacheMode.LOG
        )
        node1.run()

        # Second: async REPLAY from DB
        node2 = OperatorNode(
            op,
            [make_two_col_stream(3)],
            pipeline_database=db,
            cache_mode=CacheMode.REPLAY,
        )

        # No input needed for REPLAY — close input immediately
        input_ch = Channel(buffer_size=4)
        output_ch = Channel(buffer_size=16)

        await input_ch.writer.close()
        await node2.async_execute([input_ch.reader], output_ch.writer)

        results = await output_ch.reader.collect()
        assert len(results) == 3
        values = sorted(pkt.as_dict()["x"] for _, pkt in results)
        assert values == [0, 1, 2]

    @pytest.mark.asyncio
    async def test_replay_empty_db_returns_empty(self):
        stream = make_two_col_stream(3)
        op = SelectPacketColumns(["x"])
        db = InMemoryArrowDatabase()

        node = OperatorNode(
            op,
            [stream],
            pipeline_database=db,
            cache_mode=CacheMode.REPLAY,
        )

        input_ch = Channel(buffer_size=4)
        output_ch = Channel(buffer_size=16)

        await input_ch.writer.close()
        await node.async_execute([input_ch.reader], output_ch.writer)

        results = await output_ch.reader.collect()
        assert len(results) == 0


# ---------------------------------------------------------------------------
# 7. execute_packet routing verification
# ---------------------------------------------------------------------------


class TestExecutePacketRouting:
    def test_function_node_sequential_uses_execute_packet(self):
        """Verify FunctionNode routes through execute_packet (not raw pf.call)."""
        call_log = []

        _, pod = make_double_pod()
        stream = make_stream(3)
        node = FunctionNode(pod, stream)

        # Monkey-patch to verify routing through internal path
        original = node._process_packet_internal

        def patched(tag, packet):
            call_log.append("_process_packet_internal")
            return original(tag, packet)

        node._process_packet_internal = patched

        results = list(node.iter_packets())
        assert len(results) == 3
        assert len(call_log) == 3

    @pytest.mark.asyncio
    async def test_function_node_async_uses_async_process_packet_internal(self):
        """Verify FunctionNode.async_execute routes through _async_process_packet_internal."""
        call_log = []

        _, pod = make_double_pod()
        stream = make_stream(3)
        node = FunctionNode(pod, stream)

        original = node._async_process_packet_internal

        async def patched(tag, packet):
            call_log.append("_async_process_packet_internal")
            return await original(tag, packet)

        node._async_process_packet_internal = patched

        input_ch = Channel(buffer_size=16)
        output_ch = Channel(buffer_size=16)

        await feed_stream_to_channel(make_stream(3), input_ch)
        await node.async_execute(input_ch.reader, output_ch.writer)
        await output_ch.reader.collect()

        assert len(call_log) == 3


# ---------------------------------------------------------------------------
# 8. End-to-end async pipeline with nodes
# ---------------------------------------------------------------------------


class TestEndToEnd:
    @pytest.mark.asyncio
    async def test_source_to_function_node_pipeline(self):
        """Source → FunctionNode async pipeline."""

        def triple(x: int) -> int:
            return x * 3

        pf = PythonPacketFunction(triple, output_keys="result")
        pod = FunctionPod(pf)
        stream = make_stream(4)
        node = FunctionNode(pod, stream)

        ch1 = Channel(buffer_size=16)
        ch2 = Channel(buffer_size=16)

        async def source():
            for tag, packet in make_stream(4).iter_packets():
                await ch1.writer.send((tag, packet))
            await ch1.writer.close()

        async with asyncio.TaskGroup() as tg:
            tg.create_task(source())
            tg.create_task(node.async_execute(ch1.reader, ch2.writer))

        results = await ch2.reader.collect()
        assert len(results) == 4
        values = sorted(pkt.as_dict()["result"] for _, pkt in results)
        assert values == [0, 3, 6, 9]

    @pytest.mark.asyncio
    async def test_source_to_operator_node_pipeline(self):
        """Source → OperatorNode (SelectPacketColumns) async pipeline."""
        stream = make_two_col_stream(3)
        op = SelectPacketColumns(["x"])
        node = OperatorNode(op, [stream])

        ch1 = Channel(buffer_size=16)
        ch2 = Channel(buffer_size=16)

        async def source():
            for tag, packet in make_two_col_stream(3).iter_packets():
                await ch1.writer.send((tag, packet))
            await ch1.writer.close()

        async with asyncio.TaskGroup() as tg:
            tg.create_task(source())
            tg.create_task(node.async_execute([ch1.reader], ch2.writer))

        results = await ch2.reader.collect()
        assert len(results) == 3
        for _, packet in results:
            pkt_dict = packet.as_dict()
            assert "x" in pkt_dict
            assert "y" not in pkt_dict


# ---------------------------------------------------------------------------
# 9. Async pipeline → synchronous DB retrieval (concrete example)
# ---------------------------------------------------------------------------


class TestAsyncPipelineThenSyncRetrieval:
    """Demonstrates the full workflow: run an async pipeline, then retrieve
    results synchronously from the database.

    This is the primary use-case for persistent nodes: async streaming
    execution populates the DB, and later callers can retrieve results
    without re-running the pipeline.
    """

    @pytest.mark.asyncio
    async def test_persistent_function_node_async_then_sync_db_retrieval(self):
        """FunctionNode: async execute → sync get_all_records."""

        # --- Setup ---
        def double(x: int) -> int:
            return x * 2

        pf = PythonPacketFunction(double, output_keys="result")
        pod = FunctionPod(pf)
        db = InMemoryArrowDatabase()
        stream = make_stream(5)  # ids 0..4, x values 0..4

        node = FunctionNode(pod, stream, pipeline_database=db)

        # --- Async pipeline execution ---
        input_ch = Channel(buffer_size=16)
        output_ch = Channel(buffer_size=16)

        async def source_producer():
            for tag, packet in make_stream(5).iter_packets():
                await input_ch.writer.send((tag, packet))
            await input_ch.writer.close()

        async with asyncio.TaskGroup() as tg:
            tg.create_task(source_producer())
            tg.create_task(node.async_execute(input_ch.reader, output_ch.writer))

        async_results = await output_ch.reader.collect()
        async_values = sorted(pkt.as_dict()["result"] for _, pkt in async_results)
        assert async_values == [0, 2, 4, 6, 8]

        # --- Synchronous DB retrieval (no re-computation) ---
        records = node.get_all_records()
        assert records is not None
        assert records.num_rows == 5

        # The DB contains the same result values that were streamed async
        result_col = records.column("result").to_pylist()
        assert sorted(result_col) == [0, 2, 4, 6, 8]

        # A *new* node sharing the same DB can also read these records
        node2 = FunctionNode(pod, make_stream(5), pipeline_database=db)
        records2 = node2.get_all_records()
        assert records2 is not None
        assert records2.num_rows == 5
        assert sorted(records2.column("result").to_pylist()) == [0, 2, 4, 6, 8]

    @pytest.mark.asyncio
    async def test_persistent_operator_node_log_then_sync_db_retrieval(self):
        """OperatorNode (LOG): async execute → sync get_all_records."""
        # --- Setup ---
        stream = make_two_col_stream(4)  # ids 0..3, x 0..3, y 0,11,22,33
        op = SelectPacketColumns(["x"])
        db = InMemoryArrowDatabase()

        node = OperatorNode(
            op, [stream], pipeline_database=db, cache_mode=CacheMode.LOG
        )

        # --- Async pipeline execution ---
        input_ch = Channel(buffer_size=16)
        output_ch = Channel(buffer_size=16)

        async def source_producer():
            for tag, packet in make_two_col_stream(4).iter_packets():
                await input_ch.writer.send((tag, packet))
            await input_ch.writer.close()

        async with asyncio.TaskGroup() as tg:
            tg.create_task(source_producer())
            tg.create_task(node.async_execute([input_ch.reader], output_ch.writer))

        async_results = await output_ch.reader.collect()
        assert len(async_results) == 4
        async_x = sorted(pkt.as_dict()["x"] for _, pkt in async_results)
        assert async_x == [0, 1, 2, 3]

        # --- Synchronous DB retrieval ---
        records = node.get_all_records()
        assert records is not None
        assert records.num_rows == 4
        assert sorted(records.column("x").to_pylist()) == [0, 1, 2, 3]
        # 'y' column should NOT be present (was dropped by SelectPacketColumns)
        assert "y" not in records.column_names

        # --- REPLAY from DB via a new node (no computation) ---
        replay_node = OperatorNode(
            op,
            [make_two_col_stream(4)],
            pipeline_database=db,
            cache_mode=CacheMode.REPLAY,
        )
        replay_node.run()
        replay_table = replay_node.as_table()
        assert replay_table.num_rows == 4
        assert sorted(replay_table.column("x").to_pylist()) == [0, 1, 2, 3]

    @pytest.mark.asyncio
    async def test_multi_stage_async_pipeline_with_db_retrieval(self):
        """Two-stage async pipeline: Source → FunctionNode → OperatorNode.

        Both nodes are persistent. After async execution, results from each
        stage can be retrieved synchronously from the database.
        """

        # --- Setup stage 1: double(x) ---
        def double(x: int) -> int:
            return x * 2

        pf = PythonPacketFunction(double, output_keys="result")
        pod = FunctionPod(pf)
        fn_db = InMemoryArrowDatabase()
        stream = make_stream(3)  # ids 0..2, x 0..2

        fn_node = FunctionNode(pod, stream, pipeline_database=fn_db)

        # --- Setup stage 2: select only "result" column ---
        # Build a placeholder stream for schema purposes (OperatorNode needs
        # to validate inputs at construction time)
        stage1_table = pa.table(
            {
                "id": pa.array([0, 1, 2], type=pa.int64()),
                "result": pa.array([0, 2, 4], type=pa.int64()),
            }
        )
        stage1_stream = ArrowTableStream(stage1_table, tag_columns=["id"])
        op = SelectPacketColumns(["result"])
        op_db = InMemoryArrowDatabase()
        op_node = OperatorNode(
            op, [stage1_stream], pipeline_database=op_db, cache_mode=CacheMode.LOG
        )

        # --- Async pipeline execution ---
        ch_source = Channel(buffer_size=16)
        ch_mid = Channel(buffer_size=16)
        ch_out = Channel(buffer_size=16)

        async def source_producer():
            for tag, packet in make_stream(3).iter_packets():
                await ch_source.writer.send((tag, packet))
            await ch_source.writer.close()

        async with asyncio.TaskGroup() as tg:
            tg.create_task(source_producer())
            tg.create_task(fn_node.async_execute(ch_source.reader, ch_mid.writer))
            tg.create_task(op_node.async_execute([ch_mid.reader], ch_out.writer))

        final_results = await ch_out.reader.collect()
        assert len(final_results) == 3
        final_values = sorted(pkt.as_dict()["result"] for _, pkt in final_results)
        assert final_values == [0, 2, 4]

        # --- Sync retrieval from stage 1 DB ---
        fn_records = fn_node.get_all_records()
        assert fn_records is not None
        assert fn_records.num_rows == 3
        assert sorted(fn_records.column("result").to_pylist()) == [0, 2, 4]

        # --- Sync retrieval from stage 2 DB ---
        op_records = op_node.get_all_records()
        assert op_records is not None
        assert op_records.num_rows == 3
        assert sorted(op_records.column("result").to_pylist()) == [0, 2, 4]
