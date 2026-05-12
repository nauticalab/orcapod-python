"""CachedFunctionPod — pod-level caching wrapper that intercepts process_data()."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from orcapod.core.datagrams import Datagram
from orcapod.core.function_pod import WrappedFunctionPod
from orcapod.core.result_cache import ResultCache
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
    def _result_database(self) -> ArrowDatabaseProtocol:
        """The underlying result database (for FunctionNode access)."""
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

        Returns:
            A ``(tag, output_data)`` tuple; output_data is ``None``
            if the inner function filters the data out.
        """
        cached = self._cache.lookup(data)
        if cached is not None:
            module_logger.info("Pod-level cache hit")
            cached = cached.with_meta_columns(**{self.RESULT_COMPUTED_FLAG: False})
            return tag, cached

        tag, output = self._function_pod.process_data(tag, data, logger=logger)
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
    ) -> tuple[TagProtocol, DataProtocol | None]:
        """Async counterpart of ``process_data``.

        DB lookup and store are synchronous (DB protocol is sync), but the
        actual computation uses the inner pod's ``async_process_data``
        for true async execution.
        """
        cached = self._cache.lookup(data)
        if cached is not None:
            module_logger.info("Pod-level cache hit")
            cached = cached.with_meta_columns(**{self.RESULT_COMPUTED_FLAG: False})
            return tag, cached

        tag, output = await self._function_pod.async_process_data(
            tag, data, logger=logger
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
