"""CachedFunctionPod — pod-level caching wrapper that intercepts process_data()."""

from __future__ import annotations

import dataclasses
import logging
from datetime import datetime, timezone
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

from orcapod.core.datagrams import Datagram
from orcapod.core.function_pod import FunctionPodStream, WrappedFunctionPod
from orcapod.core.streams.head import StreamHead
from orcapod.system_constants import constants
from orcapod.types import ColumnConfig
from orcapod.contexts.stored import StaleTables
from orcapod.core.result_cache import ResultCache
from orcapod.hooks import InvocationStatus
from orcapod.protocols.core_protocols import (
    FunctionPodProtocol,
    DataProtocol,
    StreamProtocol,
    TagProtocol,
)
from orcapod.protocols.database_protocols import ArrowDatabaseProtocol
from orcapod.protocols.observability_protocols import DataExecutionLoggerProtocol

if TYPE_CHECKING:
    import pyarrow as pa

module_logger = logging.getLogger(__name__)


class CachedFunctionPod(WrappedFunctionPod):
    """Pod-level caching wrapper that intercepts ``process_data()``.

    Caches at the ``process_data(tag, data)`` level using only the
    **input data content hash** as the cache key — the output of a
    data function depends solely on the data, not the tag.

    Tag-level provenance tracking (tag + system tags + data hash) is
    handled separately by ``FunctionNode.add_pipeline_record``.

    Uses a shared ``ResultCache`` for lookup/store/conflict-resolution
    logic (same mechanism as ``CachedDataFunction``).
    """

    # Expose RESULT_COMPUTED_FLAG from the shared ResultCache
    RESULT_COMPUTED_FLAG = ResultCache.RESULT_COMPUTED_FLAG

    def __init__(
        self,
        function_pod: FunctionPodProtocol,
        result_database: ArrowDatabaseProtocol,
        auto_flush: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(function_pod, **kwargs)
        self._cache = ResultCache(
            result_database=result_database,
            record_path=self.uri,  # no prefix; db is pre-scoped
            auto_flush=auto_flush,
            # A pod that takes the invocation context performs an external action.
            delivers_side_effects=function_pod.ctx_arg_name is not None,
            # The uri holds no hash up to the function's name, which comes after
            # "side_effect_function" for a pod that takes the invocation context.
            name_path=self.uri[: 2 if function_pod.ctx_arg_name is not None else 1],
        )

    @property
    def result_database(self) -> ArrowDatabaseProtocol:
        """The underlying result database."""
        return self._cache.result_database

    @property
    def record_path(self) -> tuple[str, ...]:
        """Return the path to the cached records in the result store (versioned)."""
        return self._cache.record_path

    def stale_result_tables(self, include_current: bool = True) -> StaleTables:
        """Find stored results of other generations that this pod will not use.

        Args:
            include_current: Whether to read the current result table and count
                its rows of another data context. ``False`` leaves the search to
                tables at an older layout, which reads no current table.

        Returns:
            Result tables at an older layout (at this pod's path, or anywhere
            under its function's name), and, for a pod that takes the invocation
            context, rows of another data context in the current result table.
        """
        return self._cache.stale_tables(
            self.data_context_key, include_current=include_current
        )

    def set_auto_flush(self, on: bool = True) -> None:
        """Set whether each stored result is committed (flushed) right away.

        Args:
            on: ``True`` (the default) commits after every stored result;
                ``False`` leaves the commit to the caller.
        """
        self._cache.set_auto_flush(on)

    def stale_result_tables_accounted_for(self) -> None:
        """Record that the caller accounted for what ``stale_result_tables`` found.

        The pod then does not report or refuse those tables again on its first
        lookup.
        """
        self._cache.stale_tables_accounted_for()

    def lookup_cached_data(self, data: DataProtocol) -> DataProtocol | None:
        """Look up a cached result for ``data`` without triggering computation.

        Uses the input data content hash as the cache key (same as
        ``process_data``). When a cached result is found, it is tagged with
        ``RESULT_COMPUTED_FLAG=False`` before being returned, exactly as
        ``process_data`` does on a cache hit.

        This method is the correct entry point for callers that need a
        cache-only probe — for example, when processing an ``EmptyData``
        token where computation cannot proceed because the upstream result
        is missing.

        Args:
            data: The input data whose content hash is used as the cache key.

        Returns:
            The cached output ``Data`` (with ``RESULT_COMPUTED_FLAG=False``
            appended), or ``None`` if no cached result exists for this input.
        """
        cached = self._cache.lookup(data, data_context_key=self.data_context_key)
        if cached is None:
            return None
        return cached.with_meta_columns(**{self.RESULT_COMPUTED_FLAG: False})

    def process_data(
        self,
        tag: TagProtocol,
        data: DataProtocol,
        *,
        logger: DataExecutionLoggerProtocol | None = None,
        run_id: str | None = None,
    ) -> tuple[TagProtocol, DataProtocol | None]:
        """Process a data with pod-level caching.

        The cache key is the input data content hash only — the function
        output depends solely on the data, not the tag.  The output
        data carries a ``RESULT_COMPUTED_FLAG`` meta value: ``True`` if
        freshly computed, ``False`` if retrieved from cache.

        Args:
            tag: The tag associated with the data.
            data: The input data to process.
            logger: Optional data execution logger.
            run_id: Pipeline run identifier forwarded to the inner pod's
                ``process_data``. Used to populate ``InvocationContext.pipeline_run_id``
                for ctx-aware pods.

        Returns:
            A ``(tag, output_data)`` tuple; output_data is ``None``
            if the inner function filters the data out.
        """
        cached = self._cache.lookup(data, data_context_key=self.data_context_key)
        if cached is not None:
            module_logger.info("Pod-level cache hit")
            cached = cached.with_meta_columns(**{self.RESULT_COMPUTED_FLAG: False})
            return tag, cached

        tag, output = self._function_pod.process_data(
            tag, data, logger=logger, run_id=run_id
        )
        if output is not None:
            pf = self._function_pod.data_function
            var_dg = Datagram(
                pf.get_function_variation_data(),
                python_schema=pf.get_function_variation_data_schema(),
                data_context=pf.data_context,
            )
            exec_dg = Datagram(
                pf.get_execution_data(),
                python_schema=pf.get_execution_data_schema(),
                data_context=pf.data_context,
            )
            self._cache.store(
                data, output, var_dg, exec_dg, data_context_key=self.data_context_key
            )
            output = output.with_meta_columns(**{self.RESULT_COMPUTED_FLAG: True})
        return tag, output

    async def async_process_data(
        self,
        tag: TagProtocol,
        data: DataProtocol,
        *,
        logger: DataExecutionLoggerProtocol | None = None,
        run_id: str | None = None,
    ) -> tuple[TagProtocol, DataProtocol | None]:
        """Async counterpart of ``process_data``.

        DB lookup and store are synchronous (DB protocol is sync), but the
        actual computation uses the inner pod's ``async_process_data``
        for true async execution.

        Args:
            tag: The tag associated with the data.
            data: The input data to process.
            logger: Optional data execution logger.
            run_id: Pipeline run identifier forwarded to the inner pod's
                ``async_process_data``. Used to populate ``InvocationContext.pipeline_run_id``
                for ctx-aware pods.

        Returns:
            A ``(tag, output_data)`` tuple; output_data is ``None``
            if the inner function filters the data out.
        """
        cached = self._cache.lookup(data, data_context_key=self.data_context_key)
        if cached is not None:
            module_logger.info("Pod-level cache hit")
            cached = cached.with_meta_columns(**{self.RESULT_COMPUTED_FLAG: False})
            return tag, cached

        tag, output = await self._function_pod.async_process_data(
            tag, data, logger=logger, run_id=run_id
        )
        if output is not None:
            pf = self._function_pod.data_function
            var_dg = Datagram(
                pf.get_function_variation_data(),
                python_schema=pf.get_function_variation_data_schema(),
                data_context=pf.data_context,
            )
            exec_dg = Datagram(
                pf.get_execution_data(),
                python_schema=pf.get_execution_data_schema(),
                data_context=pf.data_context,
            )
            self._cache.store(
                data, output, var_dg, exec_dg, data_context_key=self.data_context_key
            )
            output = output.with_meta_columns(**{self.RESULT_COMPUTED_FLAG: True})
        return tag, output

    def _invoke_with_hooks(
        self,
        tag: TagProtocol,
        data: DataProtocol,
        *,
        logger: DataExecutionLoggerProtocol | None = None,
        run_id: str | None = None,
    ) -> tuple[TagProtocol, DataProtocol | None]:
        """Override to detect cache hit status from ``RESULT_COMPUTED_FLAG`` meta.

        When ``_post_run_hooks`` is empty, delegates directly to
        ``process_data`` with zero overhead. Otherwise calls
        ``self.process_data()`` (which owns all cache lookup and store logic),
        reads ``RESULT_COMPUTED_FLAG`` from the output data meta to determine
        ``InvocationStatus.HIT`` vs ``InvocationStatus.COMPUTED``, and fires
        registered hooks.

        Args:
            tag: The tag associated with the data.
            data: The input data to process.
            logger: Optional data execution logger.
            run_id: Pipeline run identifier forwarded to
                ``PostRunPayload.invocation_context``.

        Returns:
            A ``(tag, output_data)`` tuple.
        """
        if not self._post_run_hooks:
            return self.process_data(tag, data, logger=logger, run_id=run_id)

        started_at = datetime.now(timezone.utc)
        out_tag = tag
        output_data: DataProtocol | None = None

        try:
            out_tag, output_data = self.process_data(
                tag, data, logger=logger, run_id=run_id
            )
            if output_data is not None:
                status = (
                    InvocationStatus.HIT
                    if output_data.get_meta_value(self.RESULT_COMPUTED_FLAG) is False
                    else InvocationStatus.COMPUTED
                )
            else:
                status = InvocationStatus.COMPUTED
        except Exception as exc:
            finished_at = datetime.now(timezone.utc)
            self._fire_post_run_hooks(
                self._build_post_run_payload(
                    tag,
                    data,
                    None,
                    started_at,
                    finished_at,
                    InvocationStatus.ERROR,
                    exc,
                    run_id=run_id,
                )
            )
            raise  # bare raise — preserves the original traceback exactly

        finished_at = datetime.now(timezone.utc)
        self._fire_post_run_hooks(
            self._build_post_run_payload(
                tag,
                data,
                output_data,
                started_at,
                finished_at,
                status,
                None,
                run_id=run_id,
            )
        )
        return out_tag, output_data

    async def _async_invoke_with_hooks(
        self,
        tag: TagProtocol,
        data: DataProtocol,
        *,
        logger: DataExecutionLoggerProtocol | None = None,
        run_id: str | None = None,
    ) -> tuple[TagProtocol, DataProtocol | None]:
        """Async counterpart of ``_invoke_with_hooks`` for ``CachedFunctionPod``.

        When ``_post_run_hooks`` is empty, delegates directly to
        ``async_process_data`` with zero overhead.

        Args:
            tag: The tag associated with the data.
            data: The input data to process.
            logger: Optional data execution logger.
            run_id: Pipeline run identifier forwarded to
                ``PostRunPayload.invocation_context``.

        Returns:
            A ``(tag, output_data)`` tuple.
        """
        if not self._post_run_hooks:
            return await self.async_process_data(
                tag, data, logger=logger, run_id=run_id
            )

        started_at = datetime.now(timezone.utc)
        out_tag = tag
        output_data: DataProtocol | None = None

        try:
            out_tag, output_data = await self.async_process_data(
                tag, data, logger=logger, run_id=run_id
            )
            if output_data is not None:
                status = (
                    InvocationStatus.HIT
                    if output_data.get_meta_value(self.RESULT_COMPUTED_FLAG) is False
                    else InvocationStatus.COMPUTED
                )
            else:
                status = InvocationStatus.COMPUTED
        except Exception as exc:
            finished_at = datetime.now(timezone.utc)
            self._fire_post_run_hooks(
                self._build_post_run_payload(
                    tag,
                    data,
                    None,
                    started_at,
                    finished_at,
                    InvocationStatus.ERROR,
                    exc,
                    run_id=run_id,
                )
            )
            raise  # bare raise — preserves the original traceback exactly

        finished_at = datetime.now(timezone.utc)
        self._fire_post_run_hooks(
            self._build_post_run_payload(
                tag,
                data,
                output_data,
                started_at,
                finished_at,
                status,
                None,
                run_id=run_id,
            )
        )
        return out_tag, output_data

    def get_all_cached_outputs(
        self, include_system_columns: bool = False
    ) -> "pa.Table | None":
        """Return all cached records from the result store for this pod."""
        return self._cache.get_all_records(
            self.data_context_key,
            include_system_columns=include_system_columns,
        )

    def to_config(self) -> dict[str, Any]:
        """Serialize this pod as its inner function pod.

        A saved job describes the function, not where its results are cached, so a
        pod-cached pod is saved exactly as the uncached pod would be. The pod cache
        database is not part of the saved job, and a loaded job does not use it.

        Returns:
            The inner function pod's config.
        """
        return self._function_pod.to_config()

    def process(
        self, *streams: StreamProtocol, label: str | None = None
    ) -> StreamProtocol:
        """Invoke the inner pod but with pod-level caching on process_data.

        The stream returned uses *this* pod's ``process_data`` (which
        includes caching) rather than the inner pod's. Like ``FunctionPod.process``
        it joins the inputs once, validates them and records the invocation with the
        active trackers (so a ``PipelineJob`` records this pod).
        """
        input_stream = self.handle_input_streams(*streams)
        tag_schema, data_schema = input_stream.output_schema()
        self._validate_input_schema(data_schema)
        self._validate_output_keys(tag_schema.keys())
        # The pod has no ``from_config`` (it cannot be rebuilt without its cache
        # database), the one member of FunctionPodProtocol it lacks; the tracker uses
        # none of it.
        self.tracker_manager.record_function_pod_invocation(
            self,  # pyright: ignore[reportArgumentType]
            input_stream,
            label=label,
        )
        return _CachedFunctionPodStream(
            function_pod=self,
            input_stream=input_stream,
            label=label,
        )


class _CachedFunctionPodStream(FunctionPodStream):
    """The stream of a ``CachedFunctionPod``, with the same columns in every cache state.

    A row computed in this process carries only the ``__computed`` meta column; a row
    served from the cache also carries the function variation and execution columns it
    was stored with. This stream keeps ``__computed`` and drops the others, and its
    table has no meta columns unless asked for, as an uncached pod's table.
    """

    def iter_data(self) -> Iterator[tuple[TagProtocol, DataProtocol]]:
        for tag, data in super().iter_data():
            yield tag, _without_stored_meta(data)

    def _computed_prefix(
        self, limit: int
    ) -> tuple[list[tuple[TagProtocol, DataProtocol]], int]:
        shown, total = super()._computed_prefix(limit)
        return [(tag, _without_stored_meta(data)) for tag, data in shown], total

    def head(
        self,
        n: int = 10,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> StreamHead:
        """Return the first computed rows, with the columns ``as_table`` gives.

        The pod's result store is not looked up: that needs the input rows' hashes,
        i.e. reading the input. See ``FunctionPodStream.head``.
        """
        head = super().head(n, columns=columns, all_info=all_info)
        column_config = ColumnConfig.handle_config(columns, all_info=all_info)
        if column_config.meta or column_config.all_info:
            return head
        table = head.table.drop_columns(
            [n for n in head.table.column_names if n.startswith(constants.META_PREFIX)]
        )
        return dataclasses.replace(head, table=table)

    def as_table(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> "pa.Table":
        table = super().as_table(columns=columns, all_info=all_info)
        column_config = ColumnConfig.handle_config(columns, all_info=all_info)
        if column_config.meta or column_config.all_info:
            return table
        return table.drop_columns(
            [n for n in table.column_names if n.startswith(constants.META_PREFIX)]
        )


def _without_stored_meta(data: DataProtocol) -> DataProtocol:
    """Return ``data`` with only the ``__computed`` meta column of its stored ones."""
    keep = CachedFunctionPod.RESULT_COMPUTED_FLAG
    stored = [name for name in data.meta_columns if name != keep]
    return data.drop_meta_columns(*stored) if stored else data
