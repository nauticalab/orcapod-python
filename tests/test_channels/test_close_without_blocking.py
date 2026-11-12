"""Closing a channel never blocks, so a failed async run cannot hang (ITL-932).

When a node fails, the run's task group cancels every task, including the readers of
the channels other nodes write to. Those nodes close their output in a ``finally``
block; ``close`` used to wait for room in a full queue that no reader would ever
drain again, so the run hung instead of raising. These tests pin the new contract:

- ``close`` on a full queue returns at once;
- a reader still receives every queued item, in order, exactly once, and then
  ``ChannelClosed``;
- ``close`` on a queue with room behaves as before;
- a second ``close`` does nothing; ``send`` after ``close`` raises as before;
- a ``BroadcastChannel`` closes without blocking on a slow or cancelled reader.
"""

from __future__ import annotations

import asyncio

import pytest

from orcapod.channels import BroadcastChannel, Channel, ChannelClosed, WritableChannel

# A close that blocks fails the test after this many seconds instead of hanging it.
NO_BLOCK = 1.0


async def _fill(writer: WritableChannel[int], items: list[int]) -> None:
    """Send every item without waiting for a reader (the buffer must hold them)."""
    for item in items:
        await asyncio.wait_for(writer.send(item), NO_BLOCK)


class TestChannelCloseOnFullQueue:
    @pytest.mark.asyncio
    async def test_close_returns_without_blocking(self):
        ch = Channel[int](buffer_size=3)
        await _fill(ch.writer, [1, 2, 3])
        await asyncio.wait_for(ch.writer.close(), NO_BLOCK)

    @pytest.mark.asyncio
    async def test_reader_drains_every_item_then_sees_closed(self):
        ch = Channel[int](buffer_size=3)
        await _fill(ch.writer, [1, 2, 3])
        await asyncio.wait_for(ch.writer.close(), NO_BLOCK)
        assert await asyncio.wait_for(ch.reader.collect(), NO_BLOCK) == [1, 2, 3]
        with pytest.raises(ChannelClosed):
            await asyncio.wait_for(ch.reader.receive(), NO_BLOCK)

    @pytest.mark.asyncio
    async def test_items_received_one_by_one_are_not_lost_or_repeated(self):
        ch = Channel[int](buffer_size=2)
        await _fill(ch.writer, [7, 8])
        await asyncio.wait_for(ch.writer.close(), NO_BLOCK)
        reader = ch.reader
        assert await reader.receive() == 7
        assert await reader.receive() == 8
        for _ in range(2):
            with pytest.raises(ChannelClosed):
                await asyncio.wait_for(reader.receive(), NO_BLOCK)

    @pytest.mark.asyncio
    async def test_double_close_and_send_after_close(self):
        ch = Channel[int](buffer_size=1)
        await _fill(ch.writer, [1])
        await asyncio.wait_for(ch.writer.close(), NO_BLOCK)
        await asyncio.wait_for(ch.writer.close(), NO_BLOCK)
        with pytest.raises(ChannelClosed):
            await ch.writer.send(2)
        assert await asyncio.wait_for(ch.reader.collect(), NO_BLOCK) == [1]

    @pytest.mark.asyncio
    async def test_close_with_room_behaves_as_before(self):
        ch = Channel[int](buffer_size=4)
        await _fill(ch.writer, [1, 2])
        await asyncio.wait_for(ch.writer.close(), NO_BLOCK)
        assert await asyncio.wait_for(ch.reader.collect(), NO_BLOCK) == [1, 2]

    @pytest.mark.asyncio
    async def test_close_wakes_a_waiting_reader(self):
        ch = Channel[int](buffer_size=2)
        waiting = asyncio.create_task(ch.reader.receive())
        await asyncio.sleep(0)
        await asyncio.wait_for(ch.writer.close(), NO_BLOCK)
        with pytest.raises(ChannelClosed):
            await asyncio.wait_for(waiting, NO_BLOCK)


class TestBroadcastCloseWithSlowOrCancelledReader:
    @pytest.mark.asyncio
    async def test_close_does_not_wait_for_a_full_reader(self):
        bch = BroadcastChannel[int](buffer_size=2)
        slow, fast = bch.add_reader(), bch.add_reader()
        fast_items = asyncio.create_task(fast.collect())
        await _fill(bch.writer, [1, 2])
        await asyncio.wait_for(bch.writer.close(), NO_BLOCK)
        assert await asyncio.wait_for(fast_items, NO_BLOCK) == [1, 2]
        # The slow reader still gets every item, then the end of the stream.
        assert await asyncio.wait_for(slow.collect(), NO_BLOCK) == [1, 2]
        with pytest.raises(ChannelClosed):
            await asyncio.wait_for(slow.receive(), NO_BLOCK)

    @pytest.mark.asyncio
    async def test_close_after_a_reader_was_cancelled(self):
        bch = BroadcastChannel[int](buffer_size=1)
        gone, alive = bch.add_reader(), bch.add_reader()
        alive_items = asyncio.create_task(alive.collect())
        consumer = asyncio.create_task(gone.collect())
        await _fill(bch.writer, [1])
        await asyncio.sleep(0)
        consumer.cancel()  # the consumer of ``gone`` stops reading, as on a failure
        with pytest.raises(asyncio.CancelledError):
            await consumer
        await _fill(bch.writer, [2])  # ``gone``'s queue is now full
        await asyncio.wait_for(bch.writer.close(), NO_BLOCK)
        assert await asyncio.wait_for(alive_items, NO_BLOCK) == [1, 2]

    @pytest.mark.asyncio
    async def test_send_after_close_and_double_close(self):
        bch = BroadcastChannel[int](buffer_size=1)
        reader = bch.add_reader()
        await _fill(bch.writer, [5])
        await asyncio.wait_for(bch.writer.close(), NO_BLOCK)
        await asyncio.wait_for(bch.writer.close(), NO_BLOCK)
        with pytest.raises(ChannelClosed):
            await bch.writer.send(6)
        assert await asyncio.wait_for(reader.collect(), NO_BLOCK) == [5]
        with pytest.raises(ChannelClosed):
            await asyncio.wait_for(reader.receive(), NO_BLOCK)
