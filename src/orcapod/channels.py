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
    independent queue.  Sentinel re-enqueuing is avoided because it can
    deadlock when the queue buffer is small.  Instead the channel-level
    ``_drained`` flag is set once the sentinel has been consumed, so any
    subsequent ``receive()`` call (even from a freshly created reader) raises
    ``ChannelClosed`` immediately without touching the queue.
    """

    __slots__ = ("_channel",)

    def __init__(self, channel: Channel[T]) -> None:
        self._channel = channel

    async def receive(self) -> T:
        if self._channel._drained:
            raise ChannelClosed()
        item = await self._channel._queue.get()
        if isinstance(item, _Sentinel):
            self._channel._drained = True
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
        if not self._channel._closed.is_set():
            self._channel._closed.set()
            await self._channel._queue.put(_CLOSED)


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

    __slots__ = ("_queue",)

    def __init__(self, buffer_size: int) -> None:
        self._queue: asyncio.Queue[T | _Sentinel] = asyncio.Queue(maxsize=buffer_size)

    async def receive(self) -> T:
        item = await self._queue.get()
        if isinstance(item, _Sentinel):
            # Re-enqueue so repeated receive() calls also raise
            await self._queue.put(item)
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
        if not self._broadcast._closed:
            self._broadcast._closed = True
            for reader in self._broadcast._readers:
                await reader._queue.put(_CLOSED)
