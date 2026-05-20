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


# ===========================================================================
# Shared test fixture — FakeDynamicSource
# ===========================================================================


class FakeDynamicSource:
    """Hand-written test double for DynamicSourceProtocol.

    Serves pre-loaded batches sequentially. ``poll()`` returns ``True``
    while un-fetched batches remain; ``False`` once all are consumed.
    """

    def __init__(
        self,
        batches: list[Any],
        *,
        poll_always_false: bool = False,
        poll_raises: Exception | None = None,
        fetch_raises: Exception | None = None,
        fetch_delay: float = 0.0,
        cursor_modified_at: datetime | None = None,
    ) -> None:
        self._batches = batches
        self._poll_always_false = poll_always_false
        self._poll_raises = poll_raises
        self._fetch_raises = fetch_raises
        self._fetch_delay = fetch_delay
        self._cursor_modified_at = cursor_modified_at
        self.close_called = False
        self.poll_cursors: list[Cursor[int] | None] = []
        self.fetch_cursors: list[Cursor[int] | None] = []

    async def poll(self, cursor: Cursor[int] | None = None) -> bool:
        self.poll_cursors.append(cursor)
        if self._poll_raises is not None:
            raise self._poll_raises
        if self._poll_always_false:
            return False
        current_idx = cursor.value if cursor is not None else 0
        return current_idx < len(self._batches)

    async def fetch(
        self, cursor: Cursor[int] | None = None
    ) -> tuple[Cursor[int], Any]:
        self.fetch_cursors.append(cursor)
        if self._fetch_raises is not None:
            raise self._fetch_raises
        if self._fetch_delay > 0:
            await asyncio.sleep(self._fetch_delay)
        current_idx = cursor.value if cursor is not None else 0
        if current_idx >= len(self._batches):
            return Cursor(value=current_idx, modified_at=self._cursor_modified_at), {}
        data = self._batches[current_idx]
        return (
            Cursor(value=current_idx + 1, modified_at=self._cursor_modified_at),
            data,
        )

    async def close(self) -> None:
        self.close_called = True


# ===========================================================================
# Task 4: PollingSource sync mode tests
# ===========================================================================


from orcapod.core.sources.polling_source import PollingSource


def _batch(id_: int, val: int) -> dict:
    return {"id": pa.array([id_], type=pa.int64()), "val": pa.array([val], type=pa.int64())}


class TestPollingSourceSyncMode:
    def test_sync_snapshot_fetches_on_first_iter_data(self):
        """First iter_data() triggers fetch(cursor=None) and returns rows."""
        fake = FakeDynamicSource(batches=[_batch(1, 10)])
        src = PollingSource(fake, tag_columns="id", polling_config=PollingConfig(interval=1.0))

        rows = list(src.iter_data())
        assert len(rows) == 1
        # First fetch always receives cursor=None
        assert fake.fetch_cursors == [None]

    def test_sync_snapshot_fetch_called_once_on_repeated_access(self):
        """Second iter_data() polls first; if poll returns False, cache is served."""
        fake = FakeDynamicSource(batches=[_batch(1, 10)], poll_always_false=True)
        src = PollingSource(fake, tag_columns="id", polling_config=PollingConfig(interval=1.0))

        list(src.iter_data())  # first call — fetches
        list(src.iter_data())  # second call — polls (False) → serves cache

        # fetch called once (first access), poll called once (second access)
        assert len(fake.fetch_cursors) == 1
        assert len(fake.poll_cursors) == 1

    def test_sync_cache_miss_refetches_and_combines(self):
        """Second iter_data() re-fetches when poll returns True."""
        fake = FakeDynamicSource(
            batches=[_batch(1, 10), _batch(2, 20)]
        )
        src = PollingSource(fake, tag_columns="id", polling_config=PollingConfig(interval=1.0))

        rows1 = list(src.iter_data())   # fetches batch 0
        rows2 = list(src.iter_data())   # polls → True, fetches batch 1 → combines

        assert len(rows1) == 1
        assert len(rows2) == 2   # accumulated: batch 0 + batch 1

    def test_cursor_threading_fetch_receives_previous_fetch_cursor(self):
        """fetch() receives the cursor returned by the previous fetch call."""
        fake = FakeDynamicSource(batches=[_batch(1, 10), _batch(2, 20)])
        src = PollingSource(fake, tag_columns="id", polling_config=PollingConfig(interval=1.0))

        list(src.iter_data())   # first fetch → cursor returned is Cursor(value=1)
        list(src.iter_data())   # poll(Cursor(1)), then fetch(Cursor(1))

        # poll should receive the cursor from the first fetch
        assert fake.poll_cursors[0] is not None
        assert fake.poll_cursors[0].value == 1
        # second fetch should receive same cursor
        assert fake.fetch_cursors[1] is not None
        assert fake.fetch_cursors[1].value == 1

    def test_last_modified_updated_from_cursor_modified_at(self):
        """cursor.modified_at updates last_modified; None falls back to wall clock."""
        ts = datetime(2026, 3, 15, 12, 0, 0, tzinfo=timezone.utc)
        fake = FakeDynamicSource(batches=[_batch(1, 10)], cursor_modified_at=ts)
        src = PollingSource(fake, tag_columns="id", polling_config=PollingConfig(interval=1.0))

        list(src.iter_data())
        assert src.last_modified == ts

    def test_last_modified_falls_back_to_wall_clock_when_none(self):
        """When cursor.modified_at is None, last_modified is updated to wall clock."""
        fake = FakeDynamicSource(batches=[_batch(1, 10)], cursor_modified_at=None)
        src = PollingSource(fake, tag_columns="id", polling_config=PollingConfig(interval=1.0))

        before = datetime.now(timezone.utc)
        list(src.iter_data())
        after = datetime.now(timezone.utc)

        assert src.last_modified is not None
        assert before <= src.last_modified <= after

    def test_output_schema_triggers_fetch(self):
        """output_schema() lazily triggers fetch on first access."""
        fake = FakeDynamicSource(batches=[_batch(1, 10)])
        src = PollingSource(fake, tag_columns="id", polling_config=PollingConfig(interval=1.0))

        tag_schema, data_schema = src.output_schema()
        assert "id" in tag_schema
        assert "val" in data_schema

    def test_keys_triggers_fetch(self):
        fake = FakeDynamicSource(batches=[_batch(1, 10)])
        src = PollingSource(fake, tag_columns="id", polling_config=PollingConfig(interval=1.0))

        tag_keys, data_keys = src.keys()
        assert "id" in tag_keys
        assert "val" in data_keys

    def test_identity_structure_schema_independent(self):
        """identity_structure() does not require a fetch (schema-independent)."""
        fake = FakeDynamicSource(batches=[])
        src = PollingSource(
            fake, tag_columns="id", polling_config=PollingConfig(interval=1.0),
            source_id="test_source"
        )
        # Should not raise even though no fetch has occurred
        ident = src.identity_structure()
        assert "test_source" in str(ident)

    def test_to_config_returns_non_reconstructable_descriptor(self):
        fake = FakeDynamicSource(batches=[])
        src = PollingSource(
            fake, tag_columns="id", polling_config=PollingConfig(),
            source_id="my_source"
        )
        cfg = src.to_config()
        assert cfg["source_type"] == "polling_source"
        assert cfg["source_id"] == "my_source"

    def test_from_config_raises(self):
        with pytest.raises(NotImplementedError):
            PollingSource.from_config({"source_type": "polling_source"})


# ===========================================================================
# Task 5: PollingSource async mode tests (basic)
# ===========================================================================


class TestPollingSourceAsyncMode:
    @pytest.mark.asyncio
    async def test_async_streaming_emits_rows(self):
        """Positive polls trigger fetch; rows are emitted to async_iter_data."""
        fake = FakeDynamicSource(batches=[_batch(1, 10), _batch(2, 20)])
        src = PollingSource(
            fake,
            tag_columns="id",
            # Use a wider interval and generous overrun budget so stream-building
            # latency (Arrow schema inference, SourceStreamBuilder) never trips
            # the overrun guard in CI environments.
            polling_config=PollingConfig(interval=0.05, duration=0.5, max_missed_intervals=50),
        )

        items = []
        async for tag, data in src.async_iter_data():
            items.append((tag, data))

        assert len(items) == 2
        assert fake.close_called

    @pytest.mark.asyncio
    async def test_negative_poll_emits_nothing(self):
        """poll() returning False emits nothing; fetch is never called."""
        fake = FakeDynamicSource(batches=[_batch(1, 10)], poll_always_false=True)
        src = PollingSource(
            fake,
            tag_columns="id",
            polling_config=PollingConfig(interval=0.01, duration=0.05),
        )

        items = []
        async for tag, data in src.async_iter_data():
            items.append((tag, data))

        assert len(items) == 0
        assert len(fake.fetch_cursors) == 0

    @pytest.mark.asyncio
    async def test_pre_seeding_yields_cached_rows_first(self):
        """async_iter_data() yields cached rows before entering the polling loop."""
        fake = FakeDynamicSource(batches=[_batch(1, 10)])
        src = PollingSource(
            fake,
            tag_columns="id",
            polling_config=PollingConfig(interval=1.0),
        )

        # Seed the cache synchronously first
        list(src.iter_data())

        # Now supply a second fake with no new data
        fake2 = FakeDynamicSource(batches=[], poll_always_false=True)
        src._impl = fake2

        # async_iter_data should yield the 1 cached row, then stop after duration
        items = []
        src._polling_config = PollingConfig(interval=0.01, duration=0.05)
        async for tag, data in src.async_iter_data():
            items.append((tag, data))

        assert len(items) == 1  # pre-seeded row
        assert fake2.close_called

    @pytest.mark.asyncio
    async def test_cache_combining_accumulates_rows(self):
        """After two delta fetches, async stream contains rows from both batches."""
        fake = FakeDynamicSource(batches=[_batch(1, 10), _batch(2, 20)])
        src = PollingSource(
            fake,
            tag_columns="id",
            # Wide interval + generous overrun budget — same rationale as
            # test_async_streaming_emits_rows.
            polling_config=PollingConfig(interval=0.05, duration=0.5, max_missed_intervals=50),
        )

        items = []
        async for tag, data in src.async_iter_data():
            items.append((tag, data))

        assert len(items) == 2
        # Internal cache should hold both rows
        assert src._schema_stream is not None
        cached_rows = list(src._schema_stream.iter_data())
        assert len(cached_rows) == 2

    @pytest.mark.asyncio
    async def test_clean_shutdown_on_cancellation(self):
        """CancelledError causes close() to be awaited before terminating."""
        fake = FakeDynamicSource(batches=[], poll_always_false=True)
        src = PollingSource(
            fake,
            tag_columns="id",
            polling_config=PollingConfig(interval=0.01, duration=0.0),
        )

        async def run():
            async for _ in src.async_iter_data():
                pass

        task = asyncio.create_task(run())
        await asyncio.sleep(0.05)
        assert not task.done()  # confirm task is still in the polling loop
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        assert fake.close_called

    @pytest.mark.asyncio
    async def test_duration_limit_terminates_source(self):
        """Source stops naturally after config.duration seconds."""
        fake = FakeDynamicSource(batches=[], poll_always_false=True)
        src = PollingSource(
            fake,
            tag_columns="id",
            polling_config=PollingConfig(interval=0.01, duration=0.08),
        )

        items = []
        async for tag, data in src.async_iter_data():
            items.append((tag, data))

        assert fake.close_called

    @pytest.mark.asyncio
    async def test_indefinite_mode_runs_until_cancelled(self):
        """duration=0 runs until explicitly cancelled."""
        fake = FakeDynamicSource(batches=[], poll_always_false=True)
        src = PollingSource(
            fake,
            tag_columns="id",
            polling_config=PollingConfig(interval=0.01, duration=0.0),
        )

        async def run():
            async for _ in src.async_iter_data():
                pass

        task = asyncio.create_task(run())
        # Task should still be running after a short sleep
        await asyncio.sleep(0.05)
        assert not task.done()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        assert fake.close_called

    @pytest.mark.asyncio
    async def test_cursor_threaded_through_async_fetches(self):
        """In async mode fetch() receives the cursor from the previous fetch."""
        fake = FakeDynamicSource(batches=[_batch(1, 10), _batch(2, 20)])
        src = PollingSource(
            fake,
            tag_columns="id",
            # Wide interval + generous overrun budget — same rationale as
            # test_async_streaming_emits_rows.
            polling_config=PollingConfig(interval=0.05, duration=0.5, max_missed_intervals=50),
        )

        async for _ in src.async_iter_data():
            pass

        # First fetch: cursor=None; second fetch: cursor=Cursor(value=1)
        assert len(fake.fetch_cursors) == 2
        assert fake.fetch_cursors[0] is None
        assert fake.fetch_cursors[1] is not None
        assert fake.fetch_cursors[1].value == 1
