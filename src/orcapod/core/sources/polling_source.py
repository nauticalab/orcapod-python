"""Protocol-based polling source for async pipelines.

Provides ``PollingSource``, a ``RootSource`` that wraps a
``DynamicSourceProtocol`` implementation. The framework handles scheduling,
cursor tracking, cache management, error handling, and shutdown; the
implementation only supplies ``poll``, ``fetch``, and ``close``.
"""
from __future__ import annotations

import asyncio
import logging
from collections.abc import Collection
from math import floor
from typing import TYPE_CHECKING, Any, Generic

from orcapod.core.sources.base import RootSource
from orcapod.core.sources.stream_builder import SourceStreamBuilder
from orcapod.errors import CursorInvalidatedError
from orcapod.types import ColumnConfig, Cursor, PollingConfig, T
from orcapod.utils import arrow_utils, polars_data_utils
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import polars as pl
    import pyarrow as pa
    from polars._typing import FrameInitTypes

    from orcapod.core.streams.arrow_table_stream import ArrowTableStream
    from orcapod.protocols.core_protocols.sources import DynamicSourceProtocol
    from orcapod.types import Schema
else:
    pa = LazyModule("pyarrow")
    pl = LazyModule("polars")

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Module-level sync executor (mirrors data_function.py pattern)
# ---------------------------------------------------------------------------

_sync_executor = None


def _get_sync_executor():
    global _sync_executor
    if _sync_executor is None:
        from concurrent.futures import ThreadPoolExecutor

        _sync_executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="polling_source_sync"
        )
    return _sync_executor


def _run_sync(async_fn, *args, **kwargs):
    """Run ``async_fn(*args, **kwargs)`` synchronously.

    Safe to call from within a running event loop — uses a thread-based
    executor in that case (same pattern as ``data_function.py``). The
    coroutine is created inside the executor thread so it is always owned by
    the loop that runs it.

    Args:
        async_fn: An async callable (coroutine function).
        *args: Positional arguments forwarded to *async_fn*.
        **kwargs: Keyword arguments forwarded to *async_fn*.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(async_fn(*args, **kwargs))
    else:
        return _get_sync_executor().submit(
            lambda: asyncio.run(async_fn(*args, **kwargs))
        ).result()


# ---------------------------------------------------------------------------
# PollingSource
# ---------------------------------------------------------------------------


class PollingSource(RootSource, Generic[T]):
    """A root source that continuously emits data via a polling loop.

    Wraps a ``DynamicSourceProtocol`` implementation. Under async execution
    (``async_iter_data``), the framework
    polls on a fixed interval and yields new rows as they arrive. Under sync
    execution (``iter_data``), a single poll+fetch cycle is performed on each
    access and results are served from an accumulated in-memory cache.

    Args:
        impl: User-supplied protocol implementation.
        tag_columns: Column name(s) that form the tag (key) for each row.
            All other columns are data columns.
        polling_config: Scheduling and error configuration.
        source_id: Optional stable identifier for this source.
        label: Optional human-readable label.
        data_context: Optional data context key or instance.
        config: Optional Orcapod framework config.

    Note:
        Sync mode calls ``asyncio.run()`` (or a thread executor when inside a
        running loop). This fails without a shim in Jupyter notebooks. Async
        mode has no such restriction.

    Note:
        The accumulated cache is unbounded for true-delta implementations.
        No eviction policy is implemented in this version.
    """

    def __init__(
        self,
        impl: DynamicSourceProtocol[T],
        tag_columns: str | Collection[str],
        polling_config: PollingConfig = PollingConfig(),
        source_id: str | None = None,
        label: str | None = None,
        data_context: str | Any | None = None,
        config: Any | None = None,
    ) -> None:
        super().__init__(
            source_id=source_id,
            label=label,
            data_context=data_context,
            config=config,
        )
        self._impl: DynamicSourceProtocol[T] = impl
        if isinstance(tag_columns, str):
            self._tag_columns: tuple[str, ...] = (tag_columns,)
        else:
            self._tag_columns = tuple(tag_columns)
        if polling_config.interval <= 0:
            raise ValueError(
                f"PollingConfig.interval must be > 0, got {polling_config.interval}"
            )
        if polling_config.duration < 0:
            raise ValueError(
                f"PollingConfig.duration must be >= 0, got {polling_config.duration}"
            )
        if polling_config.max_missed_intervals < 1:
            raise ValueError(
                f"PollingConfig.max_missed_intervals must be >= 1, "
                f"got {polling_config.max_missed_intervals}"
            )
        if polling_config.max_consecutive_errors < 1:
            raise ValueError(
                f"PollingConfig.max_consecutive_errors must be >= 1, "
                f"got {polling_config.max_consecutive_errors}"
            )
        if polling_config.error_backoff_base <= 0:
            raise ValueError(
                f"PollingConfig.error_backoff_base must be > 0, "
                f"got {polling_config.error_backoff_base}"
            )
        self._polling_config = polling_config
        self._cursor: Cursor[T] | None = None
        self._schema_stream: ArrowTableStream | None = None

    # -------------------------------------------------------------------------
    # Identity
    # -------------------------------------------------------------------------

    def identity_structure(self) -> Any:
        """Schema-independent identity (schema is unknown until first fetch)."""
        return (self.__class__.__name__, self._tag_columns, self._source_id or "")

    # -------------------------------------------------------------------------
    # Sync stream delegation — all route through _get_latest_stream()
    # -------------------------------------------------------------------------

    def output_schema(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        """Return the output schema, triggering a fetch on first access."""
        return self._get_latest_stream().output_schema(columns=columns, all_info=all_info)

    def keys(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        """Return tag and data column keys, triggering a fetch on first access."""
        return self._get_latest_stream().keys(columns=columns, all_info=all_info)

    def iter_data(self):
        """Iterate over (tag, data) pairs from the current snapshot."""
        return self._get_latest_stream().iter_data()

    def as_table(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> pa.Table:
        """Return the accumulated rows as a PyArrow table."""
        return self._get_latest_stream().as_table(columns=columns, all_info=all_info)

    # -------------------------------------------------------------------------
    # Serialization
    # -------------------------------------------------------------------------

    def to_config(self, db_registry: Any = None) -> dict[str, Any]:
        """Return a non-reconstructable descriptor for this source."""
        return {
            "source_type": "polling_source",
            "tag_columns": list(self._tag_columns),
            "source_id": self._source_id,
        }

    @classmethod
    def from_config(cls, config: dict[str, Any], db_registry: Any = None) -> PollingSource:
        """Not supported — ``PollingSource`` cannot be reconstructed from config.

        Raises:
            NotImplementedError: Always.
        """
        raise NotImplementedError(
            "PollingSource cannot be reconstructed from config — "
            "the DynamicSourceProtocol implementation is not serializable."
        )

    # -------------------------------------------------------------------------
    # Internal sync helpers
    # -------------------------------------------------------------------------

    def _get_latest_stream(self) -> ArrowTableStream:
        """Return the current accumulated stream, fetching/polling as needed."""
        if self._schema_stream is None:
            # First access — no cache yet; fetch immediately
            logger.debug("PollingSource %r: first sync access — fetching", self._source_id)
            new_cursor, data = _run_sync(self._impl.fetch, cursor=None)
            new_stream = self._try_build_stream(data)
            if new_stream is not None:
                self._schema_stream = new_stream
            self._update_last_modified_from_cursor(new_cursor)
            self._cursor = new_cursor
        else:
            # Have cache — poll for updates
            has_new = _run_sync(self._impl.poll, cursor=self._cursor)
            if has_new:
                logger.debug("PollingSource %r: sync poll found new data — fetching", self._source_id)
                new_cursor, data = _run_sync(self._impl.fetch, cursor=self._cursor)
                new_stream = self._try_build_stream(data)
                if new_stream is not None:
                    self._schema_stream = self._combine(self._schema_stream, new_stream)
                self._update_last_modified_from_cursor(new_cursor)
                self._cursor = new_cursor
            else:
                logger.debug("PollingSource %r: sync poll — cache still valid", self._source_id)

        if self._schema_stream is None:
            raise ValueError(
                "PollingSource: no data available yet — first fetch returned empty data."
            )
        return self._schema_stream

    def _try_build_stream(self, data: FrameInitTypes) -> ArrowTableStream | None:
        """Build an ``ArrowTableStream`` from raw data, returning ``None`` for empty data.

        Returns:
            ``ArrowTableStream`` if data has rows and columns, ``None`` otherwise.
        """
        df = pl.DataFrame(data)
        if len(df.columns) == 0:
            logger.debug(
                "PollingSource %r: fetch returned data with no columns — skipping stream build",
                self._source_id,
            )
            return None
        return self._build_stream_from_df(df)

    def _build_stream_from_df(self, df: pl.DataFrame) -> ArrowTableStream:
        """Build an ``ArrowTableStream`` from a Polars DataFrame."""
        from orcapod.core.streams.arrow_table_stream import ArrowTableStream

        # Handle Object-dtype columns (same pattern as DataFrameSource)
        object_columns = [c for c in df.columns if df[c].dtype == pl.Object]
        if object_columns:
            sub_table = self.data_context.type_converter.python_dicts_to_arrow_table(
                df.select(object_columns).to_dicts()
            )
            df = df.with_columns([pl.from_arrow(c) for c in sub_table])

        df = polars_data_utils.drop_system_columns(df)

        arrow_table = df.to_arrow()
        arrow_table = arrow_table.cast(arrow_utils.infer_schema_nullable(arrow_table))

        builder = SourceStreamBuilder(self.data_context, self.orcapod_config)
        result = builder.build(
            arrow_table,
            tag_columns=self._tag_columns,
            source_id=self._source_id,
        )

        if self._source_id is None:
            self._source_id = result.source_id

        return result.stream

    def _combine(
        self, existing: ArrowTableStream, new_stream: ArrowTableStream
    ) -> ArrowTableStream:
        """Append *new_stream* rows to *existing*, warning on schema drift."""
        from orcapod.core.streams.arrow_table_stream import ArrowTableStream

        # Schema drift check (user-facing columns only)
        old_tag_keys, old_data_keys = existing.keys()
        new_tag_keys, new_data_keys = new_stream.keys()
        old_cols = set(old_tag_keys) | set(old_data_keys)
        new_cols = set(new_tag_keys) | set(new_data_keys)
        if old_cols != new_cols:
            logger.warning(
                "PollingSource %r: schema drift detected — added: %r, removed: %r",
                self._source_id,
                sorted(new_cols - old_cols),
                sorted(old_cols - new_cols),
            )

        combined = pa.concat_tables(
            [
                existing.as_table(all_info=True),
                new_stream.as_table(all_info=True),
            ],
            promote_options="default",
        )
        return ArrowTableStream(table=combined, tag_columns=self._tag_columns)

    def _update_last_modified_from_cursor(self, cursor: Cursor[T]) -> None:
        """Update ``last_modified`` from cursor or fall back to wall clock."""
        if cursor.modified_at is not None:
            self._set_modified_time(cursor.modified_at)
        else:
            self._update_modified_time()

    # -------------------------------------------------------------------------
    # Async mode — full polling loop
    # -------------------------------------------------------------------------

    async def async_iter_data(self):
        """Async generator that continuously emits (tag, data) pairs.

        Pre-seeds from the cached stream (if any) before entering the polling
        loop. The loop runs until: the configured duration elapses, the maximum
        consecutive error or overrun threshold is exceeded,
        ``CursorInvalidatedError`` is raised, or the task is cancelled.

        ``impl.close()`` is always awaited before returning.
        """
        # Pre-seed from cache (yield first, count after — avoids double iteration)
        if self._schema_stream is not None:
            pre_seed_count = 0
            for item in self._schema_stream.iter_data():
                yield item
                pre_seed_count += 1
            logger.debug(
                "PollingSource %r: pre-seeded %d row(s)", self._source_id, pre_seed_count
            )

        cfg = self._polling_config
        loop = asyncio.get_running_loop()
        start_time = loop.time()
        next_tick = start_time
        consecutive_misses = 0
        consecutive_errors = 0

        logger.info(
            "PollingSource %r starting (interval=%.2fs, duration=%.1fs)",
            self._source_id,
            cfg.interval,
            cfg.duration,
        )

        try:
            while True:
                # 1. Sleep until next scheduled tick
                now = loop.time()
                if next_tick > now:
                    await asyncio.sleep(next_tick - now)

                # 2. Poll + fetch
                try:
                    has_new = await self._impl.poll(cursor=self._cursor)

                    if has_new:
                        logger.debug(
                            "PollingSource %r: new data detected, fetching",
                            self._source_id,
                        )
                        new_cursor, data = await self._impl.fetch(cursor=self._cursor)
                        new_stream = self._try_build_stream(data)
                        self._cursor = new_cursor
                        self._update_last_modified_from_cursor(new_cursor)
                        if new_stream is not None:
                            if self._schema_stream is None:
                                self._schema_stream = new_stream
                            else:
                                self._schema_stream = self._combine(self._schema_stream, new_stream)

                            # Emit new rows from just this fetch
                            rows = list(new_stream.iter_data())
                            logger.debug(
                                "PollingSource %r: emitting %d row(s)",
                                self._source_id,
                                len(rows),
                            )
                            for item in rows:
                                yield item
                    else:
                        logger.debug(
                            "PollingSource %r: poll returned no new data",
                            self._source_id,
                        )

                    consecutive_errors = 0

                except asyncio.CancelledError:
                    raise

                except CursorInvalidatedError:
                    logger.error(
                        "PollingSource %r: cursor invalidated — previous state cannot "
                        "be reconciled with already-emitted rows. Terminating source.",
                        self._source_id,
                    )
                    return

                except Exception as e:
                    consecutive_errors += 1
                    backoff = cfg.error_backoff_base * 2 ** (consecutive_errors - 1)
                    logger.error(
                        "PollingSource %r: poll/fetch error (consecutive=%d, "
                        "backoff=%.1fs): %s",
                        self._source_id,
                        consecutive_errors,
                        backoff,
                        e,
                    )
                    if consecutive_errors >= cfg.max_consecutive_errors:
                        logger.error(
                            "PollingSource %r: max consecutive errors (%d) reached. "
                            "Terminating source.",
                            self._source_id,
                            cfg.max_consecutive_errors,
                        )
                        return
                    await asyncio.sleep(backoff)
                    continue  # retry — do not advance next_tick

                # 3. Tick advancement (start-to-start)
                now = loop.time()
                intervals_consumed = floor((now - next_tick) / cfg.interval)
                if intervals_consumed > 0:
                    consecutive_misses += intervals_consumed
                    logger.warning(
                        "PollingSource %r: tick overrun — consumed %d interval(s) "
                        "(consecutive_misses=%d/%d)",
                        self._source_id,
                        intervals_consumed,
                        consecutive_misses,
                        cfg.max_missed_intervals,
                    )
                    if consecutive_misses >= cfg.max_missed_intervals:
                        logger.error(
                            "PollingSource %r: overrun threshold exceeded. "
                            "Terminating source.",
                            self._source_id,
                        )
                        return
                else:
                    consecutive_misses = 0
                next_tick += (intervals_consumed + 1) * cfg.interval

                # 4. Duration check
                if cfg.duration > 0 and (loop.time() - start_time) >= cfg.duration:
                    logger.info(
                        "PollingSource %r: duration limit (%.1fs) reached. "
                        "Terminating source.",
                        self._source_id,
                        cfg.duration,
                    )
                    return

        except asyncio.CancelledError:
            logger.info(
                "PollingSource %r: cancelled — shutting down cleanly.",
                self._source_id,
            )

        finally:
            logger.debug("PollingSource %r: calling impl.close()", self._source_id)
            await self._impl.close()
            logger.info("PollingSource %r: closed.", self._source_id)
