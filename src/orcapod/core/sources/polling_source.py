"""Protocol-based polling source for async pipelines.

Provides ``PollingSource``, a ``RootSource`` that wraps a
``DynamicSourceProtocol`` implementation. The framework handles scheduling,
cursor tracking, cache management, error handling, and shutdown; the
implementation only supplies ``identity``, ``to_config``, ``from_config``,
``poll``, ``fetch``, and ``close``.
"""
from __future__ import annotations

import asyncio
import dataclasses
import logging
from collections.abc import Collection
from math import floor
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from orcapod.core.sources.base import RootSource
from orcapod.core.sources.stream_builder import SourceStreamBuilder
from orcapod.errors import CursorInvalidatedError, InputValidationError
from orcapod.types import ColumnConfig, Cursor, PollingConfig
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

T = TypeVar("T")

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


def _assert_schema_match(
    schema_kind: str,
    declared: Schema,
    actual: Schema,
    source_id: str | None,
) -> None:
    """Raise ``InputValidationError`` when *actual* is incompatible with *declared*.

    Checks that every field in *declared* is present in *actual* with the same
    type. Extra fields in *actual* are allowed.

    Args:
        schema_kind: Human-readable label (``"tag"`` or ``"data"``) used in
            the error message.
        declared: The expected schema (e.g. from ``tag_schema`` / ``data_schema``
            constructor arguments, or the accumulated stream's schema).
        actual: The observed schema from the most recently fetched data.
        source_id: Source identifier for error messages.

    Raises:
        InputValidationError: If any declared field is absent from *actual* or
            has a mismatched type.
    """
    mismatches: list[str] = []
    for field, expected_type in declared.items():
        if field not in actual:
            mismatches.append(f"{field!r} missing from fetched data")
        elif actual[field] != expected_type:
            mismatches.append(
                f"{field!r}: declared {expected_type!r}, got {actual[field]!r}"
            )
    if mismatches:
        raise InputValidationError(
            f"PollingSource {source_id!r}: {schema_kind} schema incompatible — "
            + "; ".join(mismatches)
        )


# ---------------------------------------------------------------------------
# PollingSource
# ---------------------------------------------------------------------------


class PollingSource(RootSource, Generic[T]):
    """A root source that continuously emits data via a polling loop.

    Wraps a ``DynamicSourceProtocol`` implementation. Under async execution
    (``async_iter_data``), the framework polls on a fixed interval and yields
    new rows as they arrive. Under sync execution (``iter_data``), a single
    poll+fetch cycle is performed on each access and results are served from
    an accumulated in-memory cache.

    Args:
        impl: User-supplied ``DynamicSourceProtocol`` implementation that
            provides ``identity``, ``to_config``, ``from_config``, ``poll``,
            ``fetch``, and ``close`` methods.
        tag_columns: Column name(s) that form the tag (join key) for each
            row. All other columns become data columns.
        polling_config: Scheduling and error-handling configuration.
            Defaults to ``PollingConfig()`` which polls every 1 second,
            runs indefinitely (``duration=0``), tolerates up to 5
            consecutive missed intervals, retries up to 3 consecutive
            errors with 1-second exponential backoff base. All five fields
            are validated at construction time — ``ValueError`` is raised
            for out-of-range values.
        tag_schema: Optional expected tag schema. When provided, ``output_schema``
            and ``keys`` can answer without triggering a fetch, and each fetched
            batch is validated against this schema — ``InputValidationError`` is
            raised on any mismatch.
        data_schema: Optional expected data schema. Same behaviour as
            *tag_schema* above.
        source_id: Optional stable string identifier for provenance tracking.
            Defaults to ``str(impl.identity())`` when omitted.
        label: Optional human-readable label shown in pipeline diagrams.
        data_context: Optional data context key or instance for type
            conversion and hashing.
        config: Optional Orcapod framework config.

    Note:
        Sync mode calls ``asyncio.run()`` (or a ``ThreadPoolExecutor``
        when called from within a running event loop). This can fail in
        Jupyter notebooks without a nest-asyncio shim. Async mode has no
        such restriction.

    Note:
        The accumulated in-memory cache is unbounded for true-delta
        implementations. No eviction policy is implemented.
    """

    def __init__(
        self,
        impl: DynamicSourceProtocol[T],
        tag_columns: str | Collection[str],
        polling_config: PollingConfig = PollingConfig(),
        tag_schema: Schema | None = None,
        data_schema: Schema | None = None,
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
        self._polling_config = polling_config
        self._tag_schema = tag_schema
        self._data_schema = data_schema
        self._cursor: Cursor[T] | None = None
        self._accumulated_stream: ArrowTableStream | None = None
        # Derive source_id from impl identity if not explicitly provided
        if self._source_id is None:
            self._source_id = str(self._impl.identity())

    # -------------------------------------------------------------------------
    # Identity
    # -------------------------------------------------------------------------

    def identity_structure(self) -> Any:
        """Identity derived from the impl's own identity and tag columns.

        Delegates to ``impl.identity()`` so that the implementer controls
        what makes two ``PollingSource`` instances distinct.
        """
        return (
            self.__class__.__name__,
            self._impl.identity(),
            self._tag_columns,
        )

    # -------------------------------------------------------------------------
    # Sync stream delegation — all route through _get_latest_stream()
    # -------------------------------------------------------------------------

    def output_schema(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        """Return the output schema.

        When both ``tag_schema`` and ``data_schema`` were provided at
        construction and no data has been fetched yet, returns them directly
        without triggering a fetch. Otherwise triggers a fetch on first access.
        """
        if (
            self._tag_schema is not None
            and self._data_schema is not None
            and self._accumulated_stream is None
            and columns is None
            and not all_info
        ):
            return self._tag_schema, self._data_schema
        return self._get_latest_stream().output_schema(columns=columns, all_info=all_info)

    def keys(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        """Return tag and data column keys.

        When both ``tag_schema`` and ``data_schema`` were provided at
        construction and no data has been fetched yet, derives keys from them
        directly without triggering a fetch.
        """
        if (
            self._tag_schema is not None
            and self._data_schema is not None
            and self._accumulated_stream is None
            and columns is None
            and not all_info
        ):
            return tuple(self._tag_schema.keys()), tuple(self._data_schema.keys())
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
        """Serialize this source to a JSON-compatible config dict.

        The config stores the impl class's module path and qualified name for
        reconstruction, plus the output of ``impl.to_config()`` under the
        ``"impl_config"`` key (``None`` if the impl does not support
        serialization). ``PollingSource.from_config`` calls
        ``impl_class.from_config(impl_config)`` when ``impl_config`` is
        non-``None``, and falls back to a no-argument constructor otherwise.

        Args:
            db_registry: Unused; present for protocol compatibility.

        Returns:
            A dict suitable for passing to ``from_config``.
        """
        impl_type = type(self._impl)
        return {
            "source_type": "polling_source",
            "impl_module": impl_type.__module__,
            "impl_class": impl_type.__qualname__,
            "tag_columns": list(self._tag_columns),
            "polling_config": dataclasses.asdict(self._polling_config),
            "source_id": self._source_id,
            "impl_config": self._impl.to_config(),
        }

    @classmethod
    def from_config(cls, config: dict[str, Any], db_registry: Any = None) -> PollingSource:
        """Reconstruct a ``PollingSource`` by importing the impl class.

        When ``config["impl_config"]`` is non-``None``, the impl is
        reconstructed via ``impl_class.from_config(impl_config)``. Otherwise
        the impl class is instantiated with no arguments.

        Args:
            config: A dict as produced by ``to_config``.
            db_registry: Unused; present for protocol compatibility.

        Returns:
            A new ``PollingSource`` wrapping the imported impl.

        Raises:
            KeyError: If ``config`` is missing a required key.
            ImportError: If the impl module cannot be imported.
            AttributeError: If the impl class does not exist in the module.
            TypeError: If the impl class cannot be instantiated.
        """
        import importlib

        module = importlib.import_module(config["impl_module"])
        # Walk dotted qualname to support nested classes (e.g. "Outer.Inner")
        impl_class: Any = module
        for part in config["impl_class"].split("."):
            impl_class = getattr(impl_class, part)

        impl_config = config.get("impl_config")
        if impl_config is None:
            impl = impl_class()
        else:
            impl = impl_class.from_config(impl_config)

        polling_config = PollingConfig(**config.get("polling_config", {}))
        return cls(
            impl=impl,
            tag_columns=config["tag_columns"],
            polling_config=polling_config,
            source_id=config.get("source_id"),
        )

    # -------------------------------------------------------------------------
    # Internal sync helpers
    # -------------------------------------------------------------------------

    def _get_latest_stream(self) -> ArrowTableStream:
        """Return the current accumulated stream, fetching/polling as needed."""
        if self._accumulated_stream is None:
            # First access — no cache yet; fetch immediately
            logger.debug("PollingSource %r: first sync access — fetching", self._source_id)
            new_cursor, data = _run_sync(self._impl.fetch, cursor=None)
            new_stream = self._try_build_stream(data)
            if new_stream is not None:
                if self._tag_schema is not None or self._data_schema is not None:
                    self._validate_against_declared_schemas(new_stream)
                self._accumulated_stream = new_stream
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
                    if self._tag_schema is not None or self._data_schema is not None:
                        self._validate_against_declared_schemas(new_stream)
                    self._accumulated_stream = self._combine(self._accumulated_stream, new_stream)
                self._update_last_modified_from_cursor(new_cursor)
                self._cursor = new_cursor
            else:
                logger.debug("PollingSource %r: sync poll — cache still valid", self._source_id)

        if self._accumulated_stream is None:
            raise ValueError(
                "PollingSource: no data available yet — first fetch returned empty data."
            )
        return self._accumulated_stream

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
        return result.stream

    def _validate_against_declared_schemas(self, stream: ArrowTableStream) -> None:
        """Validate *stream*'s schema against the declared ``tag_schema`` / ``data_schema``.

        Called whenever a new batch is fetched and declared schemas were provided
        at construction. Raises if the fetched data is incompatible.

        Args:
            stream: The newly built stream whose schema is to be validated.

        Raises:
            InputValidationError: If the stream's tag or data schema is
                incompatible with the declared schemas.
        """
        actual_tag_schema, actual_data_schema = stream.output_schema()
        if self._tag_schema is not None:
            _assert_schema_match("tag", self._tag_schema, actual_tag_schema, self._source_id)
        if self._data_schema is not None:
            _assert_schema_match("data", self._data_schema, actual_data_schema, self._source_id)

    def _validate_combining_schemas(
        self, existing: ArrowTableStream, new_stream: ArrowTableStream
    ) -> None:
        """Validate that *new_stream*'s schema is compatible with *existing*.

        Checks that both streams have identical user-facing column sets and
        that all shared column types match exactly.

        Args:
            existing: The currently accumulated stream.
            new_stream: The newly fetched stream to be appended.

        Raises:
            InputValidationError: If column sets differ or any column type has
                changed between batches.
        """
        old_tag_keys, old_data_keys = existing.keys()
        new_tag_keys, new_data_keys = new_stream.keys()

        old_cols = set(old_tag_keys) | set(old_data_keys)
        new_cols = set(new_tag_keys) | set(new_data_keys)

        if old_cols != new_cols:
            added = sorted(new_cols - old_cols)
            removed = sorted(old_cols - new_cols)
            raise InputValidationError(
                f"PollingSource {self._source_id!r}: schema mismatch between batches — "
                f"added: {added!r}, removed: {removed!r}"
            )

        old_tag_schema, old_data_schema = existing.output_schema()
        new_tag_schema, new_data_schema = new_stream.output_schema()
        _assert_schema_match("tag", old_tag_schema, new_tag_schema, self._source_id)
        _assert_schema_match("data", old_data_schema, new_data_schema, self._source_id)

    def _combine(
        self, existing: ArrowTableStream, new_stream: ArrowTableStream
    ) -> ArrowTableStream:
        """Validate schemas then append *new_stream* rows to *existing*.

        Args:
            existing: The currently accumulated stream.
            new_stream: The newly fetched stream to be appended.

        Returns:
            A new ``ArrowTableStream`` containing rows from both streams.

        Raises:
            InputValidationError: If the schemas are incompatible (see
                ``_validate_combining_schemas``).
        """
        from orcapod.core.streams.arrow_table_stream import ArrowTableStream

        self._validate_combining_schemas(existing, new_stream)

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
        # Pre-seed from cache
        if self._accumulated_stream is not None:
            pre_seed_count = 0
            for pre_seed_count, item in enumerate(
                self._accumulated_stream.iter_data(), start=1
            ):
                yield item
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
                            if self._tag_schema is not None or self._data_schema is not None:
                                self._validate_against_declared_schemas(new_stream)
                            if self._accumulated_stream is None:
                                self._accumulated_stream = new_stream
                            else:
                                self._accumulated_stream = self._combine(
                                    self._accumulated_stream, new_stream
                                )

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

                except InputValidationError:
                    # Schema mismatches are not transient — propagate immediately.
                    raise

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
