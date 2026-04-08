"""
Comprehensive tests for the async channel primitives.

Covers:
- Channel basic send/receive
- Channel close semantics and ChannelClosed exception
- Backpressure (bounded buffer)
- Async iteration (__aiter__ / __anext__)
- collect() draining
- Multiple readers seeing sentinel
- Writer send-after-close
- BroadcastChannel fan-out to multiple readers
- BroadcastChannel close semantics
- Protocol conformance (ReadableChannel / WritableChannel)
- Empty channel collect
- Concurrent producer/consumer patterns
- Edge cases (zero-buffer, single item, large burst)
"""

from __future__ import annotations

import asyncio

import pytest

from orcapod.channels import (
    BroadcastChannel,
    Channel,
    ChannelClosed,
    ReadableChannel,
    WritableChannel,
)


# ---------------------------------------------------------------------------
# 1. Basic send/receive
# ---------------------------------------------------------------------------


class TestBasicSendReceive:
    @pytest.mark.asyncio
    async def test_send_and_receive_single_item(self):
        ch = Channel[int](buffer_size=8)
        await ch.writer.send(42)
        result = await ch.reader.receive()
        assert result == 42

    @pytest.mark.asyncio
    async def test_send_and_receive_multiple_items(self):
        ch = Channel[str](buffer_size=8)
        items = ["a", "b", "c"]
        for item in items:
            await ch.writer.send(item)

        received = []
        for _ in range(3):
            received.append(await ch.reader.receive())
        assert received == items

    @pytest.mark.asyncio
    async def test_fifo_ordering(self):
        ch = Channel[int](buffer_size=16)
        for i in range(10):
            await ch.writer.send(i)
        await ch.writer.close()

        result = await ch.reader.collect()
        assert result == list(range(10))

    @pytest.mark.asyncio
    async def test_send_receive_complex_types(self):
        ch = Channel[tuple[str, int]](buffer_size=4)
        await ch.writer.send(("hello", 1))
        await ch.writer.send(("world", 2))
        assert await ch.reader.receive() == ("hello", 1)
        assert await ch.reader.receive() == ("world", 2)


# ---------------------------------------------------------------------------
# 2. Close semantics
# ---------------------------------------------------------------------------


class TestCloseSemantics:
    @pytest.mark.asyncio
    async def test_receive_after_close_raises_channel_closed(self):
        ch = Channel[int](buffer_size=4)
        await ch.writer.close()
        with pytest.raises(ChannelClosed):
            await ch.reader.receive()

    @pytest.mark.asyncio
    async def test_receive_drains_then_raises(self):
        ch = Channel[int](buffer_size=4)
        await ch.writer.send(1)
        await ch.writer.send(2)
        await ch.writer.close()

        assert await ch.reader.receive() == 1
        assert await ch.reader.receive() == 2
        with pytest.raises(ChannelClosed):
            await ch.reader.receive()

    @pytest.mark.asyncio
    async def test_send_after_close_raises(self):
        ch = Channel[int](buffer_size=4)
        await ch.writer.close()
        with pytest.raises(ChannelClosed, match="Cannot send to a closed channel"):
            await ch.writer.send(99)

    @pytest.mark.asyncio
    async def test_double_close_is_idempotent(self):
        ch = Channel[int](buffer_size=4)
        await ch.writer.close()
        await ch.writer.close()  # Should not raise

    @pytest.mark.asyncio
    async def test_repeated_receive_after_close_raises_channel_closed(self):
        """After close, repeated receive() calls all raise ChannelClosed.

        Implementation uses a channel-level _drained flag together with
        re-enqueuing the _CLOSED sentinel to avoid deadlock and wake
        concurrent waiters when buffer_size is small.
        """
        ch = Channel[int](buffer_size=4)
        await ch.writer.close()
        for _ in range(3):
            with pytest.raises(ChannelClosed):
                await ch.reader.receive()


# ---------------------------------------------------------------------------
# 3. Backpressure
# ---------------------------------------------------------------------------


class TestBackpressure:
    @pytest.mark.asyncio
    async def test_send_blocks_when_buffer_full(self):
        ch = Channel[int](buffer_size=2)
        await ch.writer.send(1)
        await ch.writer.send(2)

        # Buffer is full — a third send should not complete immediately
        send_completed = False

        async def try_send():
            nonlocal send_completed
            await ch.writer.send(3)
            send_completed = True

        task = asyncio.create_task(try_send())
        await asyncio.sleep(0.05)  # Give event loop a tick
        assert not send_completed, "Send should block when buffer is full"

        # Drain one item to unblock
        await ch.reader.receive()
        await asyncio.sleep(0.05)
        assert send_completed, "Send should complete after buffer has space"
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    @pytest.mark.asyncio
    async def test_receive_blocks_when_buffer_empty(self):
        ch = Channel[int](buffer_size=4)
        received = None

        async def try_receive():
            nonlocal received
            received = await ch.reader.receive()

        task = asyncio.create_task(try_receive())
        await asyncio.sleep(0.05)
        assert received is None, "Receive should block when buffer is empty"

        await ch.writer.send(42)
        await asyncio.sleep(0.05)
        assert received == 42
        await task


# ---------------------------------------------------------------------------
# 4. Async iteration
# ---------------------------------------------------------------------------


class TestAsyncIteration:
    @pytest.mark.asyncio
    async def test_async_for_yields_all_items(self):
        ch = Channel[int](buffer_size=8)
        expected = [10, 20, 30]
        for item in expected:
            await ch.writer.send(item)
        await ch.writer.close()

        result = []
        async for item in ch.reader:
            result.append(item)
        assert result == expected

    @pytest.mark.asyncio
    async def test_async_for_on_empty_closed_channel(self):
        ch = Channel[int](buffer_size=4)
        await ch.writer.close()
        result = []
        async for item in ch.reader:
            result.append(item)
        assert result == []

    @pytest.mark.asyncio
    async def test_async_iteration_with_concurrent_producer(self):
        ch = Channel[int](buffer_size=4)

        async def producer():
            for i in range(5):
                await ch.writer.send(i)
            await ch.writer.close()

        async def consumer():
            items = []
            async for item in ch.reader:
                items.append(item)
            return items

        _, result = await asyncio.gather(producer(), consumer())
        assert result == [0, 1, 2, 3, 4]


# ---------------------------------------------------------------------------
# 5. collect()
# ---------------------------------------------------------------------------


class TestCollect:
    @pytest.mark.asyncio
    async def test_collect_returns_all_items(self):
        ch = Channel[int](buffer_size=16)
        for i in range(5):
            await ch.writer.send(i)
        await ch.writer.close()

        result = await ch.reader.collect()
        assert result == [0, 1, 2, 3, 4]

    @pytest.mark.asyncio
    async def test_collect_on_empty_closed_channel(self):
        ch = Channel[int](buffer_size=4)
        await ch.writer.close()
        result = await ch.reader.collect()
        assert result == []

    @pytest.mark.asyncio
    async def test_collect_with_concurrent_producer(self):
        ch = Channel[int](buffer_size=2)

        async def producer():
            for i in range(10):
                await ch.writer.send(i)
            await ch.writer.close()

        task = asyncio.create_task(producer())
        result = await ch.reader.collect()
        await task
        assert result == list(range(10))


# ---------------------------------------------------------------------------
# 6. BroadcastChannel
# ---------------------------------------------------------------------------


class TestBroadcastChannel:
    @pytest.mark.asyncio
    async def test_broadcast_sends_to_all_readers(self):
        bc = BroadcastChannel[int](buffer_size=8)
        r1 = bc.add_reader()
        r2 = bc.add_reader()

        await bc.writer.send(1)
        await bc.writer.send(2)
        await bc.writer.close()

        result1 = await r1.collect()
        result2 = await r2.collect()
        assert result1 == [1, 2]
        assert result2 == [1, 2]

    @pytest.mark.asyncio
    async def test_broadcast_close_signals_all_readers(self):
        bc = BroadcastChannel[str](buffer_size=4)
        r1 = bc.add_reader()
        r2 = bc.add_reader()
        r3 = bc.add_reader()
        await bc.writer.close()

        for reader in [r1, r2, r3]:
            with pytest.raises(ChannelClosed):
                await reader.receive()

    @pytest.mark.asyncio
    async def test_broadcast_readers_independent_pace(self):
        bc = BroadcastChannel[int](buffer_size=8)
        r1 = bc.add_reader()
        r2 = bc.add_reader()

        await bc.writer.send(10)
        await bc.writer.send(20)
        await bc.writer.close()

        # Reader 1 drains all
        result1 = await r1.collect()

        # Reader 2 also gets everything
        result2 = await r2.collect()

        assert result1 == [10, 20]
        assert result2 == [10, 20]

    @pytest.mark.asyncio
    async def test_broadcast_send_after_close_raises(self):
        bc = BroadcastChannel[int](buffer_size=4)
        bc.add_reader()
        await bc.writer.close()
        with pytest.raises(ChannelClosed):
            await bc.writer.send(1)

    @pytest.mark.asyncio
    async def test_broadcast_double_close_idempotent(self):
        bc = BroadcastChannel[int](buffer_size=4)
        bc.add_reader()
        await bc.writer.close()
        await bc.writer.close()  # Should not raise

    @pytest.mark.asyncio
    async def test_broadcast_no_readers(self):
        """Broadcast with no readers should still work (items are dropped)."""
        bc = BroadcastChannel[int](buffer_size=4)
        await bc.writer.send(1)
        await bc.writer.close()

    @pytest.mark.asyncio
    async def test_broadcast_repeated_receive_after_close(self):
        bc = BroadcastChannel[int](buffer_size=4)
        r = bc.add_reader()
        await bc.writer.close()
        for _ in range(3):
            with pytest.raises(ChannelClosed):
                await r.receive()


# ---------------------------------------------------------------------------
# 7. Protocol conformance
# ---------------------------------------------------------------------------


class TestProtocolConformance:
    def test_channel_reader_is_readable(self):
        ch = Channel[int](buffer_size=4)
        assert isinstance(ch.reader, ReadableChannel)

    def test_channel_writer_is_writable(self):
        ch = Channel[int](buffer_size=4)
        assert isinstance(ch.writer, WritableChannel)

    def test_broadcast_reader_is_readable(self):
        bc = BroadcastChannel[int](buffer_size=4)
        r = bc.add_reader()
        assert isinstance(r, ReadableChannel)

    def test_broadcast_writer_is_writable(self):
        bc = BroadcastChannel[int](buffer_size=4)
        assert isinstance(bc.writer, WritableChannel)


# ---------------------------------------------------------------------------
# 8. Concurrent producer/consumer patterns
# ---------------------------------------------------------------------------


class TestConcurrentPatterns:
    @pytest.mark.asyncio
    async def test_multiple_producers_single_consumer(self):
        """Multiple tasks sending to the same channel."""
        ch = Channel[int](buffer_size=8)

        async def produce(start: int, count: int):
            for i in range(start, start + count):
                await ch.writer.send(i)

        async def run():
            async with asyncio.TaskGroup() as tg:
                tg.create_task(produce(0, 5))
                tg.create_task(produce(100, 5))

            await ch.writer.close()

        task = asyncio.create_task(run())
        result = await ch.reader.collect()
        await task

        assert sorted(result) == [0, 1, 2, 3, 4, 100, 101, 102, 103, 104]

    @pytest.mark.asyncio
    async def test_pipeline_two_stages(self):
        """Simple two-stage pipeline: producer -> transformer -> consumer."""
        ch1 = Channel[int](buffer_size=4)
        ch2 = Channel[int](buffer_size=4)

        async def producer():
            for i in range(5):
                await ch1.writer.send(i)
            await ch1.writer.close()

        async def transformer():
            async for item in ch1.reader:
                await ch2.writer.send(item * 2)
            await ch2.writer.close()

        async def consumer():
            return await ch2.reader.collect()

        _, _, result = await asyncio.gather(producer(), transformer(), consumer())
        assert result == [0, 2, 4, 6, 8]

    @pytest.mark.asyncio
    async def test_pipeline_three_stages(self):
        """Three-stage pipeline: source -> add1 -> double -> sink."""
        ch1 = Channel[int](buffer_size=4)
        ch2 = Channel[int](buffer_size=4)
        ch3 = Channel[int](buffer_size=4)

        async def source():
            for i in range(3):
                await ch1.writer.send(i)
            await ch1.writer.close()

        async def add_one():
            async for item in ch1.reader:
                await ch2.writer.send(item + 1)
            await ch2.writer.close()

        async def double():
            async for item in ch2.reader:
                await ch3.writer.send(item * 2)
            await ch3.writer.close()

        _, _, _, result = await asyncio.gather(
            source(), add_one(), double(), ch3.reader.collect()
        )
        assert result == [2, 4, 6]

    @pytest.mark.asyncio
    async def test_fan_out_fan_in(self):
        """Broadcast to two consumers, each processing independently."""
        bc = BroadcastChannel[int](buffer_size=8)
        r1 = bc.add_reader()
        r2 = bc.add_reader()

        out = Channel[int](buffer_size=16)

        async def producer():
            for i in range(3):
                await bc.writer.send(i)
            await bc.writer.close()

        async def worker(reader, multiplier):
            async for item in reader:
                await out.writer.send(item * multiplier)

        async def run():
            async with asyncio.TaskGroup() as tg:
                tg.create_task(producer())
                tg.create_task(worker(r1, 10))
                tg.create_task(worker(r2, 100))
            await out.writer.close()

        task = asyncio.create_task(run())
        result = await out.reader.collect()
        await task

        assert sorted(result) == [0, 0, 10, 20, 100, 200]


# ---------------------------------------------------------------------------
# 9. Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    @pytest.mark.asyncio
    async def test_buffer_size_one(self):
        ch = Channel[int](buffer_size=1)

        async def producer():
            for i in range(5):
                await ch.writer.send(i)
            await ch.writer.close()

        task = asyncio.create_task(producer())
        result = await ch.reader.collect()
        await task
        assert result == [0, 1, 2, 3, 4]

    @pytest.mark.asyncio
    async def test_large_burst(self):
        ch = Channel[int](buffer_size=4)
        n = 100

        async def producer():
            for i in range(n):
                await ch.writer.send(i)
            await ch.writer.close()

        task = asyncio.create_task(producer())
        result = await ch.reader.collect()
        await task
        assert result == list(range(n))

    @pytest.mark.asyncio
    async def test_none_as_item(self):
        """None is a valid item — it should not be confused with sentinel."""
        ch = Channel[int | None](buffer_size=4)
        await ch.writer.send(None)
        await ch.writer.send(1)
        await ch.writer.send(None)
        await ch.writer.close()

        result = await ch.reader.collect()
        assert result == [None, 1, None]

    @pytest.mark.asyncio
    async def test_channel_default_buffer_size(self):
        ch = Channel[int]()
        assert ch.buffer_size == 64


# ---------------------------------------------------------------------------
# 10. Config types
# ---------------------------------------------------------------------------


class TestConfigTypes:
    def test_executor_type_enum(self):
        from orcapod.types import ExecutorType

        assert ExecutorType.SYNCHRONOUS.value == "synchronous"
        assert ExecutorType.ASYNC_CHANNELS.value == "async_channels"

    def test_pipeline_config_defaults(self):
        from orcapod.types import ExecutorType, PipelineConfig

        cfg = PipelineConfig()
        assert cfg.executor == ExecutorType.SYNCHRONOUS
        assert cfg.channel_buffer_size == 64
        assert cfg.default_max_concurrency is None

    def test_pipeline_config_custom(self):
        from orcapod.types import ExecutorType, PipelineConfig

        cfg = PipelineConfig(
            executor=ExecutorType.ASYNC_CHANNELS,
            channel_buffer_size=128,
            default_max_concurrency=4,
        )
        assert cfg.executor == ExecutorType.ASYNC_CHANNELS
        assert cfg.channel_buffer_size == 128
        assert cfg.default_max_concurrency == 4

    def test_node_config_defaults(self):
        from orcapod.types import NodeConfig

        cfg = NodeConfig()
        assert cfg.max_concurrency is None

    def test_resolve_concurrency_node_overrides_pipeline(self):
        from orcapod.types import NodeConfig, PipelineConfig, resolve_concurrency

        node = NodeConfig(max_concurrency=2)
        pipeline = PipelineConfig(default_max_concurrency=8)
        assert resolve_concurrency(node, pipeline) == 2

    def test_resolve_concurrency_falls_back_to_pipeline(self):
        from orcapod.types import NodeConfig, PipelineConfig, resolve_concurrency

        node = NodeConfig()
        pipeline = PipelineConfig(default_max_concurrency=8)
        assert resolve_concurrency(node, pipeline) == 8

    def test_resolve_concurrency_both_none(self):
        from orcapod.types import NodeConfig, PipelineConfig, resolve_concurrency

        node = NodeConfig()
        pipeline = PipelineConfig()
        assert resolve_concurrency(node, pipeline) is None
