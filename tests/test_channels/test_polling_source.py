"""Tests for PollingSource and DynamicSourceProtocol."""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

import pyarrow as pa
import pytest

from orcapod.channels import Channel
from orcapod.core.nodes.source_node import SourceJobNode
from orcapod.core.sources.polling_source import PollingSource
from orcapod.core.streams.arrow_table_stream import ArrowTableStream
from orcapod.errors import CursorInvalidatedError
from orcapod.protocols.core_protocols.sources import DynamicSourceProtocol
from orcapod.types import Cursor, PollingConfig, Schema


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

    def test_invalid_interval(self):
        with pytest.raises(ValueError, match="interval"):
            PollingConfig(interval=0.0)
        with pytest.raises(ValueError, match="interval"):
            PollingConfig(interval=-1.0)

    def test_invalid_duration(self):
        with pytest.raises(ValueError, match="duration"):
            PollingConfig(duration=-1.0)

    def test_invalid_max_missed_intervals(self):
        with pytest.raises(ValueError, match="max_missed_intervals"):
            PollingConfig(max_missed_intervals=0)

    def test_invalid_max_consecutive_errors(self):
        with pytest.raises(ValueError, match="max_consecutive_errors"):
            PollingConfig(max_consecutive_errors=0)

    def test_invalid_error_backoff_base(self):
        with pytest.raises(ValueError, match="error_backoff_base"):
            PollingConfig(error_backoff_base=0.0)
        with pytest.raises(ValueError, match="error_backoff_base"):
            PollingConfig(error_backoff_base=-0.5)


class TestCursorInvalidatedError:
    def test_is_exception(self):
        e = CursorInvalidatedError("state lost")
        assert isinstance(e, Exception)
        assert "state lost" in str(e)


# ===========================================================================
# Task 2: DynamicSourceProtocol conformance
# ===========================================================================


class _MinimalImpl:
    """Minimal protocol-conformant implementation for isinstance checks."""

    def identity(self) -> Any:
        return "_MinimalImpl"

    def to_config(self) -> dict[str, Any] | None:
        return None

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> _MinimalImpl:
        return cls()

    def schema(self):
        return None

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
            def schema(self): return None
            async def fetch(self, cursor=None): return Cursor(value=0), {}
            async def close(self): pass

        assert not isinstance(NoPoll(), DynamicSourceProtocol)

    def test_missing_fetch_fails_isinstance(self):
        class NoFetch:
            def schema(self): return None
            async def poll(self, cursor=None): return False
            async def close(self): pass

        assert not isinstance(NoFetch(), DynamicSourceProtocol)

    def test_missing_close_fails_isinstance(self):
        class NoClose:
            def schema(self): return None
            async def poll(self, cursor=None): return False
            async def fetch(self, cursor=None): return Cursor(value=0), {}

        assert not isinstance(NoClose(), DynamicSourceProtocol)

    def test_missing_schema_fails_isinstance(self):
        """schema() is now a required protocol method."""
        class NoSchema:
            def identity(self): return "NoSchema"
            def to_config(self): return None
            @classmethod
            def from_config(cls, config): return cls()
            async def poll(self, cursor=None): return False
            async def fetch(self, cursor=None): return Cursor(value=0), {}
            async def close(self): pass

        assert not isinstance(NoSchema(), DynamicSourceProtocol)


# ===========================================================================
# Task 3: StreamBase.async_iter_data default + SourceNode.async_execute
# ===========================================================================


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
        """SourceJobNode.async_execute routes through async_iter_data — no regression."""
        from orcapod.types import Schema

        stream = _make_arrow_stream(2)
        node = SourceJobNode(
            name="test",
            tag_schema=Schema({"id": int}),
            data_schema=Schema({"val": int}),
            bound_source=stream,
        )

        ch = Channel(buffer_size=8)
        await node.async_execute(ch.writer)

        rows = await ch.reader.collect()
        assert len(rows) == 2

    @pytest.mark.asyncio
    async def test_source_node_closes_channel_after_exhaustion(self):
        from orcapod.types import Schema

        stream = _make_arrow_stream(1)
        node = SourceJobNode(
            name="test",
            tag_schema=Schema({"id": int}),
            data_schema=Schema({"val": int}),
            bound_source=stream,
        )

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

    Pass ``schema_override`` to declare a known schema upfront (used by
    schema-validation tests). Omit it (``None``) to exercise the
    fetch-inference fallback path.
    """

    def __init__(
        self,
        batches: list[Any],
        *,
        schema_override: Any | None = None,
        poll_always_false: bool = False,
        poll_raises: Exception | None = None,
        fetch_raises: Exception | None = None,
        fetch_delay: float = 0.0,
        cursor_modified_at: datetime | None = None,
    ) -> None:
        self._batches = batches
        self._schema_override = schema_override
        self._poll_always_false = poll_always_false
        self._poll_raises = poll_raises
        self._fetch_raises = fetch_raises
        self._fetch_delay = fetch_delay
        self._cursor_modified_at = cursor_modified_at
        self.close_called = False
        self.poll_cursors: list[Cursor[int] | None] = []
        self.fetch_cursors: list[Cursor[int] | None] = []

    def identity(self) -> Any:
        return "FakeDynamicSource"

    def to_config(self) -> dict[str, Any] | None:
        return None

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> FakeDynamicSource:
        raise NotImplementedError("FakeDynamicSource is not serializable")

    def schema(self) -> Any:
        return self._schema_override

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


class NoArgSource:
    """Minimal ``DynamicSourceProtocol`` impl with a no-argument constructor.

    Used to test ``PollingSource.from_config`` reconstruction: the impl must
    be importable and instantiable without arguments.
    """

    def identity(self) -> Any:
        return "NoArgSource"

    def to_config(self) -> dict[str, Any] | None:
        return None

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> NoArgSource:
        return cls()

    def schema(self) -> None:
        return None

    async def poll(self, cursor: Cursor[int] | None = None) -> bool:
        return False

    async def fetch(
        self, cursor: Cursor[int] | None = None
    ) -> tuple[Cursor[int], Any]:
        return Cursor(value=0), []

    async def close(self) -> None:
        pass


# ===========================================================================
# Task 4: PollingSource sync mode tests
# ===========================================================================


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
        )
        # Should not raise even though no fetch has occurred
        ident = src.identity_structure()
        # Identity encodes the impl class location and tag columns
        assert "FakeDynamicSource" in str(ident)
        assert "id" in str(ident)

    def test_identity_differs_for_different_impl_classes(self):
        """Different impl classes produce different identity structures."""
        fake1 = FakeDynamicSource(batches=[])
        fake2 = NoArgSource()
        src1 = PollingSource(fake1, tag_columns="id", polling_config=PollingConfig(interval=1.0))
        src2 = PollingSource(fake2, tag_columns="id", polling_config=PollingConfig(interval=1.0))
        assert src1.identity_structure() != src2.identity_structure()

    def test_to_config_includes_impl_location(self):
        """to_config() serializes impl class module/qualname and polling config."""
        fake = FakeDynamicSource(batches=[])
        src = PollingSource(
            fake, tag_columns="id", polling_config=PollingConfig(),
            source_id="my_source"
        )
        cfg = src.to_config()
        assert cfg["source_type"] == "polling_source"
        assert cfg["source_id"] == "my_source"
        assert cfg["impl_module"] == FakeDynamicSource.__module__
        assert cfg["impl_class"] == FakeDynamicSource.__qualname__
        assert cfg["tag_columns"] == ["id"]
        assert "polling_config" in cfg
        assert cfg["polling_config"]["interval"] == 1.0

    def test_from_config_reconstructs_no_arg_impl(self):
        """from_config() reconstructs a PollingSource when impl has a no-arg constructor."""
        src = PollingSource(
            NoArgSource(), tag_columns="id", polling_config=PollingConfig(interval=2.0),
            source_id="recon_source"
        )
        cfg = src.to_config()
        reconstructed = PollingSource.from_config(cfg)
        assert isinstance(reconstructed, PollingSource)
        assert reconstructed._tag_columns == ("id",)
        assert reconstructed._polling_config.interval == 2.0
        assert isinstance(reconstructed._impl, NoArgSource)

    def test_from_config_raises_on_missing_impl_module(self):
        """from_config() raises KeyError when impl_module is absent."""
        with pytest.raises(KeyError):
            PollingSource.from_config({"source_type": "polling_source", "tag_columns": ["id"]})


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
        assert len(src._batches) == 2
        cached_rows = list(src._get_combined_stream().iter_data())
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
        """Source stops naturally after config.duration seconds; emits zero rows when no batches."""
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
        assert len(items) == 0  # poll_always_false → nothing committed

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

    @pytest.mark.asyncio
    async def test_last_batch_yielded_before_duration_exit(self):
        """A batch committed in the same iteration that hits the duration limit must be yielded.

        Regression test for the bug where ``return`` (now ``break``) skipped the
        drain step, losing any batch committed in the final iteration.  The repro
        uses ``fetch_delay > duration`` so the single fetch outlives the duration
        budget: commit and duration-check land in the same iteration, and without
        the final drain the generator exits with ``len(rows) == 0`` while
        ``len(src._batches) == 1``.
        """
        fake = FakeDynamicSource(batches=[_batch(1, 10)], fetch_delay=0.05)
        src = PollingSource(
            fake,
            tag_columns="id",
            polling_config=PollingConfig(interval=0.01, duration=0.01, max_missed_intervals=1000),
        )

        rows = [item async for item in src.async_iter_data()]
        assert len(rows) == 1   # must not be 0


# ===========================================================================
# Task 6: Error handling, overrun, schema drift tests
# ===========================================================================


class TestPollingSourceErrorHandling:
    @pytest.mark.asyncio
    async def test_error_backoff_retries_and_uses_exponential_wait(self):
        """Exception in poll() increments error counter; errors below threshold → retry."""

        class CountingError(Exception):
            pass

        call_count = 0

        class TransientImpl:
            def identity(self):
                return "TransientImpl"

            def schema(self): return None

            async def poll(self, cursor=None) -> bool:
                nonlocal call_count
                call_count += 1
                if call_count <= 2:
                    raise CountingError("transient")
                return False  # succeed on 3rd attempt

            async def fetch(self, cursor=None):
                return Cursor(value=0), {}

            async def close(self):
                pass

        src = PollingSource(
            TransientImpl(),
            tag_columns="id",
            polling_config=PollingConfig(
                interval=0.01,
                duration=0.5,
                max_consecutive_errors=5,
                error_backoff_base=0.01,  # very short backoff for tests
            ),
        )

        items = []
        async for tag, data in src.async_iter_data():
            items.append((tag, data))

        # Completed without raising; errors were below threshold
        assert call_count >= 3

    @pytest.mark.asyncio
    async def test_max_consecutive_errors_closes_cleanly(self):
        """Reaching max_consecutive_errors closes the channel without crashing."""
        fake = FakeDynamicSource(
            batches=[],
            poll_raises=RuntimeError("persistent error"),
        )
        src = PollingSource(
            fake,
            tag_columns="id",
            polling_config=PollingConfig(
                interval=0.01,
                max_consecutive_errors=3,
                error_backoff_base=0.01,
            ),
        )

        items = []
        async for tag, data in src.async_iter_data():
            items.append((tag, data))

        assert len(items) == 0
        assert fake.close_called

    @pytest.mark.asyncio
    async def test_cursor_invalidated_error_propagates_to_caller(self):
        """CursorInvalidatedError is re-raised to the caller; no retry, close() called."""
        fake = FakeDynamicSource(
            batches=[],
            poll_raises=CursorInvalidatedError("state lost"),
        )
        src = PollingSource(
            fake,
            tag_columns="id",
            polling_config=PollingConfig(interval=0.01),
        )

        with pytest.raises(CursorInvalidatedError, match="state lost"):
            async for _ in src.async_iter_data():
                pass

        # Only one poll attempt — no retry for invalidation
        assert len(fake.poll_cursors) == 1
        assert fake.close_called

    @pytest.mark.asyncio
    async def test_overrun_terminates_after_threshold(self):
        """Slow fetches that consistently overrun max_missed_intervals terminate the source."""
        fake = FakeDynamicSource(
            batches=[_batch(i, i * 10) for i in range(20)],
            fetch_delay=0.05,  # each fetch takes 50 ms
        )
        src = PollingSource(
            fake,
            tag_columns="id",
            polling_config=PollingConfig(
                interval=0.01,           # 10 ms tick — fetch will always overrun
                max_missed_intervals=3,  # terminate after 3 cumulative misses
                duration=0.0,            # run indefinitely (until overrun)
            ),
        )

        items = []
        async for tag, data in src.async_iter_data():
            items.append((tag, data))

        # Terminated due to overrun; close() must have been called.
        # At least the first committed batch must be yielded — rows committed
        # in the final iteration before the overrun break must not be lost.
        assert fake.close_called
        assert len(items) > 0

    @pytest.mark.asyncio
    async def test_schema_mismatch_raises_on_column_change(self):
        """A second fetch with a different column set raises SchemaInconsistencyError."""
        from orcapod.errors import SchemaInconsistencyError

        class DriftingImpl:
            _call = 0

            def identity(self):
                return "DriftingImpl"

            def schema(self): return None

            async def poll(self, cursor=None) -> bool:
                return self._call < 2

            async def fetch(self, cursor=None):
                self._call += 1
                if self._call == 1:
                    return (
                        Cursor(value=1),
                        {"id": pa.array([1], type=pa.int64()),
                         "val": pa.array([10], type=pa.int64())},
                    )
                # Second fetch has an extra column — schema mismatch
                return (
                    Cursor(value=2),
                    {"id": pa.array([2], type=pa.int64()),
                     "val": pa.array([20], type=pa.int64()),
                     "extra": pa.array(["x"], type=pa.large_string())},
                )

            async def close(self):
                pass

        src = PollingSource(
            DriftingImpl(),
            tag_columns="id",
            polling_config=PollingConfig(interval=0.05, duration=0.5, max_missed_intervals=50),
        )

        with pytest.raises(SchemaInconsistencyError, match="schema mismatch"):
            async for _ in src.async_iter_data():
                pass


# ===========================================================================
# Task 7: Schema validation tests
# ===========================================================================


class TestCursorNow:
    def test_now_sets_modified_at_to_current_time(self):
        before = datetime.now(timezone.utc)
        c = Cursor.now(value=42)
        after = datetime.now(timezone.utc)

        assert c.value == 42
        assert c.modified_at is not None
        assert before <= c.modified_at <= after

    def test_now_modified_at_is_timezone_aware(self):
        c = Cursor.now(value="tok")
        assert c.modified_at is not None
        assert c.modified_at.tzinfo is not None


class TestPollingSourceSchemaValidation:
    # -----------------------------------------------------------------------
    # Declared-schema short-circuit (via impl.schema())
    # -----------------------------------------------------------------------

    def test_output_schema_returns_declared_without_fetch(self):
        """output_schema() uses impl.schema() before any data is fetched."""
        declared = Schema({"id": int, "val": int})
        fake = FakeDynamicSource(batches=[_batch(1, 10)], schema_override=declared)
        src = PollingSource(
            fake,
            tag_columns="id",
            polling_config=PollingConfig(interval=1.0),
        )

        # No fetch should have occurred
        assert len(fake.fetch_cursors) == 0
        ts, ds = src.output_schema()
        assert ts == Schema({"id": int})
        assert ds == Schema({"val": int})
        # Still no fetch triggered
        assert len(fake.fetch_cursors) == 0

    def test_keys_returns_declared_without_fetch(self):
        """keys() uses impl.schema() before any data is fetched."""
        declared = Schema({"id": int, "val": int})
        fake = FakeDynamicSource(batches=[_batch(1, 10)], schema_override=declared)
        src = PollingSource(
            fake,
            tag_columns="id",
            polling_config=PollingConfig(interval=1.0),
        )

        assert len(fake.fetch_cursors) == 0
        tag_keys, data_keys = src.keys()
        assert "id" in tag_keys
        assert "val" in data_keys
        assert len(fake.fetch_cursors) == 0

    # -----------------------------------------------------------------------
    # Declared-schema compatibility checking (first fetch)
    # -----------------------------------------------------------------------

    def test_compatible_declared_schema_passes_silently(self):
        """Fetched data matching the impl-declared schema succeeds without error."""
        declared = Schema({"id": int, "val": int})
        fake = FakeDynamicSource(batches=[_batch(1, 10)], schema_override=declared)
        src = PollingSource(
            fake,
            tag_columns="id",
            polling_config=PollingConfig(interval=1.0),
        )
        rows = list(src.iter_data())
        assert len(rows) == 1

    def test_declared_tag_schema_type_mismatch_raises(self):
        """Declared tag column type differing from fetched type raises SchemaInconsistencyError."""
        from orcapod.errors import SchemaInconsistencyError

        # Declare id as str, but fetch produces id as int64 — tag schema mismatch
        declared = Schema({"id": str, "val": int})
        fake = FakeDynamicSource(batches=[_batch(1, 10)], schema_override=declared)
        src = PollingSource(
            fake,
            tag_columns="id",
            polling_config=PollingConfig(interval=1.0),
        )

        with pytest.raises(SchemaInconsistencyError, match="tag schema incompatible"):
            list(src.iter_data())

    def test_declared_data_schema_type_mismatch_raises(self):
        """Declared data column type differing from fetched type raises SchemaInconsistencyError."""
        from orcapod.errors import SchemaInconsistencyError

        # Declare val as str, but fetch produces val as int64 — data schema mismatch
        declared = Schema({"id": int, "val": str})
        fake = FakeDynamicSource(batches=[_batch(1, 10)], schema_override=declared)
        src = PollingSource(
            fake,
            tag_columns="id",
            polling_config=PollingConfig(interval=1.0),
        )

        with pytest.raises(SchemaInconsistencyError, match="data schema incompatible"):
            list(src.iter_data())

    def test_declared_schema_missing_data_column_raises(self):
        """Declared schema referencing a column absent from fetched data raises SchemaInconsistencyError."""
        from orcapod.errors import SchemaInconsistencyError

        # Declare missing_col as a data column — won't appear in fetched data
        declared = Schema({"id": int, "val": int, "missing_col": str})
        fake = FakeDynamicSource(batches=[_batch(1, 10)], schema_override=declared)
        src = PollingSource(
            fake,
            tag_columns="id",
            polling_config=PollingConfig(interval=1.0),
        )

        with pytest.raises(SchemaInconsistencyError, match="data schema incompatible"):
            list(src.iter_data())

    def test_schema_missing_tag_column_raises_at_construction(self):
        """impl.schema() omitting a declared tag column raises ValueError at construction."""
        # id is in tag_columns but not in the schema
        declared = Schema({"val": int})
        fake = FakeDynamicSource(batches=[_batch(1, 10)], schema_override=declared)

        with pytest.raises(ValueError, match="missing tag columns"):
            PollingSource(fake, tag_columns="id", polling_config=PollingConfig(interval=1.0))

    # -----------------------------------------------------------------------
    # Combining-schema validation
    # -----------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_combining_type_mismatch_raises(self):
        """A type change between batches raises SchemaInconsistencyError on combine."""
        from orcapod.errors import SchemaInconsistencyError

        class TypeDriftImpl:
            _call = 0

            def identity(self):
                return "TypeDriftImpl"

            def schema(self): return None

            async def poll(self, cursor=None) -> bool:
                return self._call < 2

            async def fetch(self, cursor=None):
                self._call += 1
                if self._call == 1:
                    return Cursor(value=1), {
                        "id": pa.array([1], type=pa.int64()),
                        "val": pa.array([10], type=pa.int64()),
                    }
                # Second fetch: val changes from int to string
                return Cursor(value=2), {
                    "id": pa.array([2], type=pa.int64()),
                    "val": pa.array(["twenty"], type=pa.large_string()),
                }

            async def close(self):
                pass

        src = PollingSource(
            TypeDriftImpl(),
            tag_columns="id",
            polling_config=PollingConfig(interval=0.05, duration=0.5, max_missed_intervals=50),
        )

        with pytest.raises(SchemaInconsistencyError, match="schema"):
            async for _ in src.async_iter_data():
                pass

    @pytest.mark.asyncio
    async def test_combining_column_set_mismatch_raises(self):
        """Column set changes between batches raise SchemaInconsistencyError."""
        from orcapod.errors import SchemaInconsistencyError

        class ColumnDriftImpl:
            _call = 0

            def identity(self):
                return "ColumnDriftImpl"

            def schema(self): return None

            async def poll(self, cursor=None) -> bool:
                return self._call < 2

            async def fetch(self, cursor=None):
                self._call += 1
                if self._call == 1:
                    return Cursor(value=1), {
                        "id": pa.array([1], type=pa.int64()),
                        "val": pa.array([10], type=pa.int64()),
                    }
                # Second fetch removes val and adds extra
                return Cursor(value=2), {
                    "id": pa.array([2], type=pa.int64()),
                    "extra": pa.array([99], type=pa.int64()),
                }

            async def close(self):
                pass

        src = PollingSource(
            ColumnDriftImpl(),
            tag_columns="id",
            polling_config=PollingConfig(interval=0.05, duration=0.5, max_missed_intervals=50),
        )

        with pytest.raises(SchemaInconsistencyError, match="schema mismatch"):
            async for _ in src.async_iter_data():
                pass

    def test_sync_three_fetches_no_content_hash_leak(self):
        """After 3 accumulating fetches (2 combines), data schema is stable and
        contains no _content_hash column, and all rows are present.

        Regression test for ITL-616: _combine called as_table(all_info=True),
        which injected the synthetic _content_hash column into the stored stream.
        The second combine then raised SchemaInconsistencyError.
        """
        fake = FakeDynamicSource(
            batches=[_batch(1, 10), _batch(2, 20), _batch(3, 30)]
        )
        src = PollingSource(
            fake, tag_columns="id", polling_config=PollingConfig(interval=1.0)
        )

        rows1 = list(src.iter_data())   # fetch 1 — builds initial stream
        rows2 = list(src.iter_data())   # fetch 2 — first combine
        rows3 = list(src.iter_data())   # fetch 3 — second combine (crashed before fix)

        assert len(rows1) == 1
        assert len(rows2) == 2
        assert len(rows3) == 3

        _, data_keys = src.keys()
        assert "_content_hash" not in data_keys

        _, data_schema = src.output_schema()
        assert "_content_hash" not in data_schema

    @pytest.mark.asyncio
    async def test_async_three_fetches_no_content_hash_leak(self):
        """After 3 async batches (2 combines), data schema is stable and contains
        no _content_hash column, and all rows are accumulated.

        Regression test for ITL-616: same root cause as the sync path.
        """
        fake = FakeDynamicSource(
            batches=[_batch(1, 10), _batch(2, 20), _batch(3, 30)]
        )
        src = PollingSource(
            fake,
            tag_columns="id",
            polling_config=PollingConfig(
                interval=0.05, duration=0.5, max_missed_intervals=50
            ),
        )

        items = []
        async for tag, data in src.async_iter_data():
            items.append((tag, data))

        assert len(items) == 3

        _, data_keys = src.keys()
        assert "_content_hash" not in data_keys

        _, data_schema = src.output_schema()
        assert "_content_hash" not in data_schema


async def _drain_async(agen):
    """Consume all items from an async generator."""
    async for _ in agen:
        pass


# ===========================================================================
# ITL-617: Concurrent sync access during async run must not lose rows
# ===========================================================================


class TestPollingSourceSyncAccessDuringAsyncRun:
    """Regression tests for ITL-617.

    A concurrent sync call (``iter_data``, ``as_table``, ``output_schema``,
    ``keys``) must not advance ``_cursor`` in a way that causes the async
    polling loop to skip rows.
    """

    @pytest.mark.asyncio
    async def test_iter_data_and_as_table_concurrent_with_async_run_lose_no_rows(self):
        """iter_data() and as_table() called concurrently with async_iter_data()
        must not cause any rows to be skipped by the async iterator."""
        fake = FakeDynamicSource(
            batches=[_batch(1, 10), _batch(2, 20), _batch(3, 30)],
            schema_override=None,
        )
        src = PollingSource(
            fake,
            tag_columns="id",
            polling_config=PollingConfig(interval=0.02, duration=1.0, max_missed_intervals=100),
        )

        rows_from_async: list = []
        stop_bg = asyncio.Event()

        async def background_sync_calls():
            # Wait until at least one batch is committed, then hammer the
            # sync API from a thread pool (iter_data and as_table both call
            # _sync_poll_and_commit which can race with the async loop).
            while not src._batches:
                await asyncio.sleep(0.005)
            while not stop_bg.is_set():
                await asyncio.to_thread(lambda: list(src.iter_data()))
                await asyncio.to_thread(lambda: src.as_table())
                await asyncio.sleep(0.005)

        bg_task = asyncio.create_task(background_sync_calls())

        async for tag, data in src.async_iter_data():
            rows_from_async.append((tag, data))

        stop_bg.set()
        try:
            await asyncio.wait_for(bg_task, timeout=1.0)
        except asyncio.TimeoutError:
            bg_task.cancel()

        # The async iterator must deliver ALL three rows, regardless of
        # how many times the sync caller raced in.
        assert len(rows_from_async) == 3

    @pytest.mark.asyncio
    async def test_output_schema_and_keys_concurrent_with_async_run_lose_no_rows(self):
        """output_schema() and keys() called concurrently with async_iter_data()
        must not trigger a fetch that advances the cursor past the async loop."""
        fake = FakeDynamicSource(
            batches=[_batch(1, 10), _batch(2, 20), _batch(3, 30)],
            schema_override=None,  # no declared schema → would fall through to fetch
        )
        src = PollingSource(
            fake,
            tag_columns="id",
            polling_config=PollingConfig(interval=0.02, duration=1.0, max_missed_intervals=100),
        )

        rows_from_async: list = []
        stop_bg = asyncio.Event()

        async def background_introspection():
            # Wait until first batch is available (so _batches is non-empty)
            # then hammer output_schema / keys.
            while not src._batches:
                await asyncio.sleep(0.005)
            while not stop_bg.is_set():
                src.output_schema()
                src.keys()
                src.output_schema(columns={"system_tags": True})
                src.keys(columns={"system_tags": True})
                await asyncio.sleep(0.005)

        bg_task = asyncio.create_task(background_introspection())

        async for tag, data in src.async_iter_data():
            rows_from_async.append((tag, data))

        stop_bg.set()
        try:
            await asyncio.wait_for(bg_task, timeout=1.0)
        except asyncio.TimeoutError:
            bg_task.cancel()

        # All 3 rows must be delivered by the async iterator.
        assert len(rows_from_async) == 3
        # output_schema / keys must not have triggered any fetches —
        # they should use _batches[0] bypass once the first batch exists.
        # Exactly 3 fetches: one per batch, all by the async loop.
        assert len(fake.fetch_cursors) == 3


# ===========================================================================
# Zero-row batch regression (ENG-952)
# ===========================================================================


class TestPollingSourceZeroRowBatch:
    """Regression tests for ENG-952.

    PollingSource must not crash when a poll returns a zero-row batch after
    a batch that contained a null value in some column.  The root cause was
    per-batch nullability re-inference: a zero-row table has null_count == 0
    for every column, so every field was inferred non-nullable, conflicting
    with the accumulated stream's nullable schema.

    The fix establishes a canonical Arrow schema once (from impl.schema() when
    declared, or from the first batch otherwise) and casts every subsequent
    batch to it by column name.
    """

    # ------------------------------------------------------------------
    # Shared impl used by tests 1 and 2 (infer-once path, no declared schema)
    # ------------------------------------------------------------------

    @staticmethod
    def _make_emit_once_then_empty_impl():
        """Return a DynamicSourceProtocol impl that emits one nullable row then empty frames."""

        class EmitOnceThenEmpty:
            """Emits one row with a null 'note' on fetch 1, then zero-row frames."""

            def __init__(self):
                self.n = 0

            def identity(self):
                return ("EmitOnceThenEmpty",)

            def schema(self):
                return None  # no declared schema — exercises the infer-once path

            async def poll(self, cursor=None):
                return True  # always claims new data

            async def fetch(self, cursor=None):
                self.n += 1
                if self.n == 1:
                    # First fetch: one row, nullable 'note' column contains None.
                    data = {
                        "id": pa.array([1], type=pa.int64()),
                        "val": pa.array([1.0], type=pa.float64()),
                        "note": pa.array([None], type=pa.large_utf8()),
                    }
                else:
                    # Subsequent fetches: zero-row frame, same columns.
                    data = {
                        "id": pa.array([], type=pa.int64()),
                        "val": pa.array([], type=pa.float64()),
                        "note": pa.array([], type=pa.large_utf8()),
                    }
                return Cursor.now(self.n), data

            async def close(self):
                return None

        return EmitOnceThenEmpty()

    # ------------------------------------------------------------------
    # Test 1: core regression — source must not crash
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_zero_row_batch_after_nullable_column_streams_cleanly(self):
        """Zero-row poll after a nullable column must not raise SchemaInconsistencyError.

        This is the exact scenario from ENG-952: fetch 1 returns a row with a
        null in 'note' (inferred nullable=True); fetches 2+ return zero-row frames
        (would be inferred nullable=False without the fix).  The source must stream
        the single real row and then terminate cleanly after the duration expires.
        """
        src = PollingSource(
            self._make_emit_once_then_empty_impl(),
            tag_columns="id",
            polling_config=PollingConfig(interval=0.05, duration=0.5, max_missed_intervals=50),
        )

        rows = []
        async for tag, data in src.async_iter_data():
            rows.append((tag, data))

        assert len(rows) == 1

    # ------------------------------------------------------------------
    # Test 2: zero-row batches must not accumulate
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_zero_row_batch_is_not_accumulated(self):
        """After zero-row polls, the internal accumulated stream must hold only the real rows."""
        src = PollingSource(
            self._make_emit_once_then_empty_impl(),
            tag_columns="id",
            polling_config=PollingConfig(interval=0.05, duration=0.5, max_missed_intervals=50),
        )

        async for _ in src.async_iter_data():
            pass

        assert len(src._batches) == 1, "_batches must contain exactly one batch after iteration"
        cached = list(src._batches[0].iter_data())
        assert len(cached) == 1

    # ------------------------------------------------------------------
    # Test 3: declared-schema path — zero-row polls, no warning
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_declared_schema_zero_row_batch_no_warning(self, caplog):
        """Declared-schema path: zero-row polls stream cleanly and emit no WARNING.

        When impl.schema() returns a Schema, nullability is derived from the
        Python type annotations (str | None → nullable=True) without inference.
        No warning must be logged even when the first batch carries a null.
        """

        class DeclaredNullableImpl:
            def __init__(self):
                self.n = 0

            def identity(self):
                return ("DeclaredNullableImpl",)

            def schema(self):
                # note is declared nullable via str | None
                return Schema({"id": int, "val": float, "note": str | None})

            async def poll(self, cursor=None):
                return True

            async def fetch(self, cursor=None):
                self.n += 1
                if self.n == 1:
                    data = {
                        "id": pa.array([1], type=pa.int64()),
                        "val": pa.array([1.0], type=pa.float64()),
                        "note": pa.array([None], type=pa.large_utf8()),
                    }
                else:
                    data = {
                        "id": pa.array([], type=pa.int64()),
                        "val": pa.array([], type=pa.float64()),
                        "note": pa.array([], type=pa.large_utf8()),
                    }
                return Cursor.now(self.n), data

            async def close(self):
                return None

        src = PollingSource(
            DeclaredNullableImpl(),
            tag_columns="id",
            polling_config=PollingConfig(interval=0.05, duration=0.5, max_missed_intervals=50),
        )

        with caplog.at_level(logging.WARNING, logger="orcapod.core.sources.polling_source"):
            rows = []
            async for tag, data in src.async_iter_data():
                rows.append((tag, data))

        assert len(rows) == 1
        inference_warnings = [
            r for r in caplog.records if "inferring nullability" in r.message
        ]
        assert len(inference_warnings) == 0, (
            "Declared-schema path must not emit an inference warning"
        )

    # ------------------------------------------------------------------
    # Test 4: infer-once path emits exactly one WARNING
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_infer_once_emits_exactly_one_warning(self, caplog):
        """When impl.schema() returns None, exactly one WARNING is logged for schema inference.

        The warning must be emitted on the first batch only — not on every subsequent
        poll — because _canonical_arrow_schema is set after the first call.
        """
        # Two batches so _build_stream_from_df is called twice; warning fires only once.
        fake = FakeDynamicSource(batches=[_batch(1, 10), _batch(2, 20)])
        src = PollingSource(
            fake,
            tag_columns="id",
            polling_config=PollingConfig(interval=0.05, duration=0.5, max_missed_intervals=50),
        )

        with caplog.at_level(logging.WARNING, logger="orcapod.core.sources.polling_source"):
            async for _ in src.async_iter_data():
                pass

        inference_warnings = [
            r for r in caplog.records if "inferring nullability" in r.message
        ]
        assert len(inference_warnings) == 1, (
            f"Expected exactly one inference warning, got {len(inference_warnings)}"
        )

    # ------------------------------------------------------------------
    # Test 5: infer-once path — zero-row first batch must not lock in nullable=False
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_zero_row_first_batch_then_nullable_column_streams_cleanly(self):
        """Infer-once path: zero-row first batch must not lock in nullable=False.

        If the first batch is zero-row, _canonical_arrow_schema must remain None
        until a non-empty batch arrives.  A subsequent batch with a null value
        must stream cleanly — not raise SchemaInconsistencyError.
        """

        class ZeroFirstImpl:
            def __init__(self):
                self.n = 0

            def identity(self):
                return ("ZeroFirstImpl",)

            def schema(self):
                return None  # infer-once path

            async def poll(self, cursor=None):
                return True

            async def fetch(self, cursor=None):
                self.n += 1
                if self.n == 1:
                    # First fetch: zero-row frame.
                    data = {
                        "id": pa.array([], type=pa.int64()),
                        "val": pa.array([], type=pa.float64()),
                        "note": pa.array([], type=pa.large_utf8()),
                    }
                elif self.n == 2:
                    # Second fetch: one row with a null 'note'.
                    data = {
                        "id": pa.array([1], type=pa.int64()),
                        "val": pa.array([1.0], type=pa.float64()),
                        "note": pa.array([None], type=pa.large_utf8()),
                    }
                else:
                    # Subsequent fetches: zero-row frames.
                    data = {
                        "id": pa.array([], type=pa.int64()),
                        "val": pa.array([], type=pa.float64()),
                        "note": pa.array([], type=pa.large_utf8()),
                    }
                return Cursor.now(self.n), data

            async def close(self):
                return None

        src = PollingSource(
            ZeroFirstImpl(),
            tag_columns="id",
            polling_config=PollingConfig(interval=0.05, duration=0.5, max_missed_intervals=50),
        )

        rows = []
        async for tag, data in src.async_iter_data():
            rows.append((tag, data))

        assert len(rows) == 1
