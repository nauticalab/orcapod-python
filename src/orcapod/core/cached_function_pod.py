"""CachedFunctionPod — pod-level caching wrapper that intercepts process_data()."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from orcapod.core.datagrams import Datagram
from orcapod.core.function_pod import WrappedFunctionPod
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
        )

    @property
    def result_database(self) -> ArrowDatabaseProtocol:
        """The underlying result database."""
        return self._cache.result_database

    @property
    def record_path(self) -> tuple[str, ...]:
        """Return the path to the cached records in the result store."""
        return self._cache.record_path

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
        cached = self._cache.lookup(data)
        if cached is not None:
            module_logger.info("Pod-level cache hit")
            cached = cached.with_meta_columns(**{self.RESULT_COMPUTED_FLAG: False})
            return tag, cached

        tag, output = self._function_pod.process_data(tag, data, logger=logger, run_id=run_id)
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
            self._cache.store(data, output, var_dg, exec_dg)
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
        cached = self._cache.lookup(data)
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
            self._cache.store(data, output, var_dg, exec_dg)
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
            run_id: Pipeline run identifier (unused in cached path).

        Returns:
            A ``(tag, output_data)`` tuple.
        """
        if not self._post_run_hooks:
            return self.process_data(tag, data, logger=logger, run_id=run_id)

        started_at = datetime.now(timezone.utc)
        out_tag = tag
        output_data: DataProtocol | None = None

        try:
            out_tag, output_data = self.process_data(tag, data, logger=logger, run_id=run_id)
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
                    tag, data, None, started_at, finished_at,
                    InvocationStatus.ERROR, exc,
                )
            )
            raise  # bare raise — preserves the original traceback exactly

        finished_at = datetime.now(timezone.utc)
        self._fire_post_run_hooks(
            self._build_post_run_payload(
                tag, data, output_data, started_at, finished_at, status, None,
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
            run_id: Pipeline run identifier (unused in cached path).

        Returns:
            A ``(tag, output_data)`` tuple.
        """
        if not self._post_run_hooks:
            return await self.async_process_data(tag, data, logger=logger, run_id=run_id)

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
                    tag, data, None, started_at, finished_at,
                    InvocationStatus.ERROR, exc,
                )
            )
            raise  # bare raise — preserves the original traceback exactly

        finished_at = datetime.now(timezone.utc)
        self._fire_post_run_hooks(
            self._build_post_run_payload(
                tag, data, output_data, started_at, finished_at, status, None,
            )
        )
        return out_tag, output_data

    def get_all_cached_outputs(
        self, include_system_columns: bool = False
    ) -> "pa.Table | None":
        """Return all cached records from the result store for this pod."""
        return self._cache.get_all_records(
            include_system_columns=include_system_columns
        )

    def process(
        self, *streams: StreamProtocol, label: str | None = None
    ) -> StreamProtocol:
        """Invoke the inner pod but with pod-level caching on process_data.

        The stream returned uses *this* pod's ``process_data`` (which
        includes caching) rather than the inner pod's.
        """
        from orcapod.core.function_pod import FunctionPodStream

        # Validate and prepare the input stream
        input_stream = self._function_pod.handle_input_streams(*streams)
        self._function_pod.validate_inputs(*streams)

        return FunctionPodStream(
            function_pod=self,
            input_stream=input_stream,
            label=label,
        )
