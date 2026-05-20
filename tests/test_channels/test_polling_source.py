"""Tests for PollingSource and DynamicSourceProtocol."""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

import pyarrow as pa
import pytest

from orcapod.errors import CursorInvalidatedError
from orcapod.types import Cursor, PollingConfig


# ===========================================================================
# Task 1: Type tests
# ===========================================================================


class TestCursor:
    def test_cursor_stores_value(self):
        c = Cursor(value=42)
        assert c.value == 42

    def test_cursor_modified_at_defaults_to_none(self):
        c = Cursor(value="tok")
        assert c.modified_at is None

    def test_cursor_accepts_datetime_modified_at(self):
        ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
        c = Cursor(value=ts, modified_at=ts)
        assert c.modified_at == ts

    def test_cursor_generic_int(self):
        c: Cursor[int] = Cursor(value=7)
        assert c.value == 7

    def test_cursor_generic_str(self):
        c: Cursor[str] = Cursor(value="page_token")
        assert c.value == "page_token"


class TestPollingConfig:
    def test_defaults(self):
        cfg = PollingConfig()
        assert cfg.interval == 1.0
        assert cfg.duration == 0.0
        assert cfg.max_missed_intervals == 5
        assert cfg.max_consecutive_errors == 3
        assert cfg.error_backoff_base == 1.0

    def test_custom_values(self):
        cfg = PollingConfig(interval=0.5, duration=10.0, max_missed_intervals=2)
        assert cfg.interval == 0.5
        assert cfg.duration == 10.0
        assert cfg.max_missed_intervals == 2

    def test_is_frozen(self):
        cfg = PollingConfig()
        with pytest.raises((AttributeError, TypeError)):
            cfg.interval = 99.0  # type: ignore[misc]


class TestCursorInvalidatedError:
    def test_is_exception(self):
        e = CursorInvalidatedError("state lost")
        assert isinstance(e, Exception)
        assert "state lost" in str(e)


# ===========================================================================
# Task 2: DynamicSourceProtocol conformance
# ===========================================================================


from orcapod.protocols.core_protocols.sources import DynamicSourceProtocol


class _MinimalImpl:
    """Minimal protocol-conformant implementation for isinstance checks."""

    async def poll(self, cursor: Cursor[int] | None = None) -> bool:
        return False

    async def fetch(self, cursor: Cursor[int] | None = None) -> tuple[Cursor[int], dict]:
        return Cursor(value=0), {}

    async def close(self) -> None:
        pass


class TestDynamicSourceProtocol:
    def test_minimal_impl_satisfies_protocol(self):
        impl = _MinimalImpl()
        assert isinstance(impl, DynamicSourceProtocol)

    def test_missing_poll_fails_isinstance(self):
        class NoPoll:
            async def fetch(self, cursor=None):
                return Cursor(value=0), {}

            async def close(self):
                pass

        assert not isinstance(NoPoll(), DynamicSourceProtocol)

    def test_missing_fetch_fails_isinstance(self):
        class NoFetch:
            async def poll(self, cursor=None):
                return False

            async def close(self):
                pass

        assert not isinstance(NoFetch(), DynamicSourceProtocol)

    def test_missing_close_fails_isinstance(self):
        class NoClose:
            async def poll(self, cursor=None):
                return False

            async def fetch(self, cursor=None):
                return Cursor(value=0), {}

        assert not isinstance(NoClose(), DynamicSourceProtocol)


# ===========================================================================
# Task 3: StreamBase.async_iter_data default + SourceNode.async_execute
# ===========================================================================


from orcapod.channels import Channel
from orcapod.core.nodes.source_node import SourceNode
from orcapod.core.streams.arrow_table_stream import ArrowTableStream


def _make_arrow_stream(n: int = 3) -> ArrowTableStream:
    """Build a minimal ArrowTableStream with `id` tag and `val` data column."""
    schema = pa.schema([
        pa.field("id", pa.int64(), nullable=False),
        pa.field("val", pa.int64(), nullable=False),
    ])
    table = pa.table(
        {"id": pa.array(list(range(n)), type=pa.int64()),
         "val": pa.array([i * 10 for i in range(n)], type=pa.int64())},
        schema=schema,
    )
    return ArrowTableStream(table, tag_columns=["id"])


class TestAsyncIterDataDefault:
    @pytest.mark.asyncio
    async def test_default_yields_same_items_as_iter_data(self):
        stream = _make_arrow_stream(3)
        sync_items = list(stream.iter_data())

        async_items = []
        async for item in stream.async_iter_data():
            async_items.append(item)

        assert len(async_items) == 3
        assert len(async_items) == len(sync_items)
        assert async_items == sync_items

    @pytest.mark.asyncio
    async def test_source_node_async_execute_uses_async_iter(self):
        """SourceNode.async_execute routes through async_iter_data — no regression."""
        stream = _make_arrow_stream(2)
        node = SourceNode(stream)

        ch = Channel(buffer_size=8)
        await node.async_execute(ch.writer)

        rows = await ch.reader.collect()
        assert len(rows) == 2

    @pytest.mark.asyncio
    async def test_source_node_closes_channel_after_exhaustion(self):
        stream = _make_arrow_stream(1)
        node = SourceNode(stream)

        ch = Channel(buffer_size=4)
        await node.async_execute(ch.writer)

        # collect() only returns after channel is closed
        rows = await ch.reader.collect()
        assert len(rows) == 1
