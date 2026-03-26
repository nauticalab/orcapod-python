from __future__ import annotations

import asyncio
import atexit
import inspect
import traceback
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from orcapod.core.executors.base import PythonFunctionExecutorBase

if TYPE_CHECKING:
    from orcapod.protocols.observability_protocols import PacketExecutionLoggerProtocol


class LocalExecutor(PythonFunctionExecutorBase):
    """Default executor -- runs the packet function directly in the current process.

    Supports all packet function types (``supported_function_type_ids``
    returns an empty set).
    """

    @property
    def executor_type_id(self) -> str:
        return "local"

    def supported_function_type_ids(self) -> frozenset[str]:
        return frozenset()

    def execute_callable(
        self,
        fn: Callable[..., Any],
        kwargs: dict[str, Any],
        executor_options: dict[str, Any] | None = None,
        *,
        logger: PacketExecutionLoggerProtocol | None = None,
    ) -> Any:
        from orcapod.pipeline.logging_capture import LocalCaptureContext

        ctx = LocalCaptureContext()
        with ctx:
            try:
                if inspect.iscoroutinefunction(fn):
                    raw_result = self._run_async_sync(fn, kwargs)
                else:
                    raw_result = fn(**kwargs)
            except Exception:
                tb = traceback.format_exc()
                captured = ctx.get_captured(success=False, tb=tb)
                if logger is not None:
                    logger.record(**captured.as_dict())
                raise
        captured = ctx.get_captured(success=True)
        if logger is not None:
            logger.record(**captured.as_dict())
        return raw_result

    # Shared single-thread pool used when an event loop is already running and
    # we need to bridge a sync call to an async function (nested-loop scenario).
    # Reusing a pool avoids per-call thread creation/teardown overhead.
    _async_bridge_pool: Any = None  # concurrent.futures.ThreadPoolExecutor | None

    @classmethod
    def _get_async_bridge_pool(cls) -> Any:
        """Return (creating if needed) the shared async-bridge thread pool."""
        if cls._async_bridge_pool is None:
            from concurrent.futures import ThreadPoolExecutor

            cls._async_bridge_pool = ThreadPoolExecutor(
                1, thread_name_prefix="orcapod-async-bridge"
            )
            atexit.register(cls._shutdown_async_bridge_pool)
        return cls._async_bridge_pool

    @classmethod
    def _shutdown_async_bridge_pool(cls) -> None:
        """Shut down the shared async-bridge thread pool at interpreter exit."""
        pool = cls._async_bridge_pool
        if pool is not None:
            pool.shutdown(wait=False)
            cls._async_bridge_pool = None

    @classmethod
    def _run_async_sync(cls, fn: Callable[..., Any], kwargs: dict[str, Any]) -> Any:
        """Run an async function synchronously, handling nested event loops."""
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(fn(**kwargs))
        else:
            pool = cls._get_async_bridge_pool()
            return pool.submit(lambda: asyncio.run(fn(**kwargs))).result()

    async def async_execute_callable(
        self,
        fn: Callable[..., Any],
        kwargs: dict[str, Any],
        executor_options: dict[str, Any] | None = None,
        *,
        logger: PacketExecutionLoggerProtocol | None = None,
    ) -> Any:
        from orcapod.pipeline.logging_capture import LocalCaptureContext

        ctx = LocalCaptureContext()
        with ctx:
            try:
                if inspect.iscoroutinefunction(fn):
                    raw_result = await fn(**kwargs)
                else:
                    import contextvars
                    import functools

                    loop = asyncio.get_running_loop()
                    task_ctx = contextvars.copy_context()
                    raw_result = await loop.run_in_executor(
                        None,
                        functools.partial(task_ctx.run, fn, **kwargs),
                    )
            except Exception:
                tb = traceback.format_exc()
                captured = ctx.get_captured(success=False, tb=tb)
                if logger is not None:
                    logger.record(**captured.as_dict())
                raise
        captured = ctx.get_captured(success=True)
        if logger is not None:
            logger.record(**captured.as_dict())
        return raw_result

    def with_options(self, **opts: Any) -> LocalExecutor:
        """Return a new ``LocalExecutor``.

        ``LocalExecutor`` carries no state, so options are ignored.
        """
        return LocalExecutor()
