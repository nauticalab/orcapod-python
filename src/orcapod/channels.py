"""Async channel primitives for push-based pipeline execution.

Provides bounded async channels with close/done signaling, backpressure,
and fan-out (broadcast) support.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import Generic, Protocol, TypeVar, runtime_checkable

T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)
T_contra = TypeVar("T_contra", contravariant=True)


# ---------------------------------------------------------------------------
# Sentinel & exception
# ---------------------------------------------------------------------------


class _Sentinel:
    """Internal marker signaling channel closure."""

    __slots__ = ()

    def __repr__(self) -> str:
        return "<channel-closed>"


_CLOSED = _Sentinel()


class ChannelClosed(Exception):
    """Raised when ``receive()`` is called on a closed, drained channel."""


# ---------------------------------------------------------------------------
# Protocol types
# ---------------------------------------------------------------------------


@runtime_checkable
class ReadableChannel(Protocol[T_co]):
    """Consumer side of a channel."""

    async def receive(self) -> T_co:
        """Receive next item. Raises ``ChannelClosed`` when done."""
        ...

    def __aiter__(self) -> AsyncIterator[T_co]: ...

    async def __anext__(self) -> T_co: ...

    async def collect(self) -> list[T_co]:
        """Drain all remaining items into a list."""
        ...


@runtime_checkable
class WritableChannel(Protocol[T_contra]):
    """Producer side of a channel."""

    async def send(self, item: T_contra) -> None:
        """Send an item. Blocks if channel buffer is full (backpressure)."""
        ...

    async def close(self) -> None:
        """Signal that no more items will be sent."""
        ...


# ---------------------------------------------------------------------------
# Concrete reader / writer views
# ---------------------------------------------------------------------------


class _ChannelReader(Generic[T]):
    """Concrete ReadableChannel backed by a Channel.

    ``Channel`` is intentionally single-consumer.  Fan-out (multiple readers)
    is handled by ``BroadcastChannel``, which gives every reader its own
    independent queue.  To avoid deadlocks with small queue buffers we avoid
    *awaiting* any sentinel re-enqueue: once the closure sentinel has been
    consumed the channel-level ``_drained`` flag is set, and a best-effort
    non-blocking re-enqueue of the sentinel is used only to wake any other
    coroutines already suspended in ``queue.get()``.  After ``_drained`` is
    set, any subsequent ``receive()`` call (even from a freshly created
    reader) raises ``ChannelClosed`` immediately without touching the queue.
    """

    __slots__ = ("_channel",)

    def __init__(self, channel: Channel[T]) -> None:
        self._channel = channel

    async def receive(self) -> T:
        if self._channel._drained:
            raise ChannelClosed()
        if self._channel._close_pending and self._channel._queue.empty():
            # Closed while the queue was full: every item is received; the end now.
            self._channel._drained = True
            raise ChannelClosed()
        item = await self._channel._queue.get()
        if isinstance(item, _Sentinel):
            self._channel._drained = True
            # Wake any other coroutine already suspended in queue.get().
            # put_nowait succeeds immediately because the queue just lost an
            # item; if the queue is somehow full (shouldn't happen once closed)
            # we silently skip — the _drained flag will cover those callers.
            try:
                self._channel._queue.put_nowait(_CLOSED)
            except asyncio.QueueFull:
                pass
            raise ChannelClosed()
        return item  # type: ignore[return-value]

    def __aiter__(self) -> AsyncIterator[T]:
        return self

    async def __anext__(self) -> T:
        try:
            return await self.receive()
        except ChannelClosed:
            raise StopAsyncIteration

    async def collect(self) -> list[T]:
        items: list[T] = []
        async for item in self:
            items.append(item)
        return items


class _ChannelWriter(Generic[T]):
    """Concrete WritableChannel backed by a Channel."""

    __slots__ = ("_channel",)

    def __init__(self, channel: Channel[T]) -> None:
        self._channel = channel

    async def send(self, item: T) -> None:
        if self._channel._closed.is_set():
            raise ChannelClosed("Cannot send to a closed channel")
        await self._channel._queue.put(item)

    async def close(self) -> None:
        """Signal that no more items will be sent, without waiting.

        Never blocks (ITL-932): on a failed run every reader is cancelled while
        writers close their outputs in ``finally`` blocks, and a close that waited
        for room in a full queue would hang the run. When the queue is full, the
        close is recorded instead of queued; readers receive every queued item and
        then ``ChannelClosed``, exactly as with a queued close.
        """
        if not self._channel._closed.is_set():
            self._channel._closed.set()
            try:
                self._channel._queue.put_nowait(_CLOSED)
            except asyncio.QueueFull:
                self._channel._close_pending = True


# ---------------------------------------------------------------------------
# Channel
# ---------------------------------------------------------------------------


@dataclass
class Channel(Generic[T]):
    """Bounded async channel with close/done signaling.

    Args:
        buffer_size: Maximum number of items that can be buffered.
            Defaults to 64.
    """

    buffer_size: int = 64
    _queue: asyncio.Queue[T | _Sentinel] = field(init=False)
    _closed: asyncio.Event = field(init=False, default_factory=asyncio.Event)
    _drained: bool = field(init=False, default=False)
    # Closed while the queue was full: the close is not queued (ITL-932).
    _close_pending: bool = field(init=False, default=False)

    def __post_init__(self) -> None:
        self._queue = asyncio.Queue(maxsize=self.buffer_size)

    @property
    def reader(self) -> _ChannelReader[T]:
        """Return a readable view of this channel."""
        return _ChannelReader(self)

    @property
    def writer(self) -> _ChannelWriter[T]:
        """Return a writable view of this channel."""
        return _ChannelWriter(self)


# ---------------------------------------------------------------------------
# Broadcast channel (fan-out)
# ---------------------------------------------------------------------------


class _BroadcastReader(Generic[T]):
    """A reader that receives items broadcast from a shared source.

    Each broadcast reader maintains its own independent queue so that
    multiple downstream consumers can read at their own pace.
    """

    __slots__ = ("_closed", "_queue")

    def __init__(self, buffer_size: int) -> None:
        self._queue: asyncio.Queue[T | _Sentinel] = asyncio.Queue(maxsize=buffer_size)
        # Set once this reader's stream has ended, or by a close that found its
        # queue full (ITL-932): then the end comes once the queue is drained.
        self._closed = False

    async def receive(self) -> T:
        if self._closed and self._queue.empty():
            raise ChannelClosed()
        item = await self._queue.get()
        if isinstance(item, _Sentinel):
            # Remember the end so repeated receive() calls also raise.
            self._closed = True
            raise ChannelClosed()
        return item  # type: ignore[return-value]

    def __aiter__(self) -> AsyncIterator[T]:
        return self

    async def __anext__(self) -> T:
        try:
            return await self.receive()
        except ChannelClosed:
            raise StopAsyncIteration

    async def collect(self) -> list[T]:
        items: list[T] = []
        async for item in self:
            items.append(item)
        return items


class BroadcastChannel(Generic[T]):
    """A channel whose output is broadcast to multiple readers.

    Each call to ``add_reader()`` creates an independent reader queue.
    Items sent via the writer are copied to every reader's queue.

    Args:
        buffer_size: Per-reader buffer size. Defaults to 64.
    """

    def __init__(self, buffer_size: int = 64) -> None:
        self._buffer_size = buffer_size
        self._readers: list[_BroadcastReader[T]] = []
        self._closed = False

    def add_reader(self) -> _BroadcastReader[T]:
        """Create and return a new reader for this broadcast channel."""
        reader = _BroadcastReader[T](self._buffer_size)
        self._readers.append(reader)
        return reader

    @property
    def writer(self) -> _BroadcastWriter[T]:
        """Return a writable view of this broadcast channel."""
        return _BroadcastWriter(self)


class _BroadcastWriter(Generic[T]):
    """Writer that fans out items to all broadcast readers."""

    __slots__ = ("_broadcast",)

    def __init__(self, broadcast: BroadcastChannel[T]) -> None:
        self._broadcast = broadcast

    async def send(self, item: T) -> None:
        if self._broadcast._closed:
            raise ChannelClosed("Cannot send to a closed channel")
        for reader in self._broadcast._readers:
            await reader._queue.put(item)

    async def close(self) -> None:
        """Signal every reader that no more items will be sent, without waiting.

        Never blocks on a slow or cancelled reader (ITL-932); see
        ``_ChannelWriter.close``.
        """
        if not self._broadcast._closed:
            self._broadcast._closed = True
            for reader in self._broadcast._readers:
                try:
                    reader._queue.put_nowait(_CLOSED)
                except asyncio.QueueFull:
                    reader._closed = True
