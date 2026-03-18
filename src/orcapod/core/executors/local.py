from __future__ import annotations

import asyncio
import inspect
import traceback as _traceback_module
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from orcapod.core.executors.base import PacketFunctionExecutorBase

if TYPE_CHECKING:
    from orcapod.protocols.core_protocols import PacketFunctionProtocol, PacketProtocol
    from orcapod.protocols.observability_protocols import PacketExecutionLoggerProtocol


class LocalExecutor(PacketFunctionExecutorBase):
    """Default executor -- runs the packet function directly in the current process.

    Supports all packet function types (``supported_function_type_ids``
    returns an empty set).
    """

    @property
    def executor_type_id(self) -> str:
        return "local"

    def supported_function_type_ids(self) -> frozenset[str]:
        return frozenset()

    def execute(
        self,
        packet_function: "PacketFunctionProtocol",
        packet: "PacketProtocol",
        *,
        logger: "PacketExecutionLoggerProtocol | None" = None,
    ) -> "PacketProtocol | None":
        return packet_function.direct_call(packet)

    async def async_execute(
        self,
        packet_function: "PacketFunctionProtocol",
        packet: "PacketProtocol",
        *,
        logger: "PacketExecutionLoggerProtocol | None" = None,
    ) -> "PacketProtocol | None":
        return await packet_function.direct_async_call(packet)

    # -- PythonFunctionExecutorProtocol --

    def execute_callable(
        self,
        fn: Callable[..., Any],
        kwargs: dict[str, Any],
        executor_options: dict[str, Any] | None = None,
        *,
        logger: "PacketExecutionLoggerProtocol | None" = None,
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
                tb = _traceback_module.format_exc()
                captured = ctx.get_captured(success=False, tb=tb)
                if logger is not None:
                    logger.record(captured)
                raise
        captured = ctx.get_captured(success=True)
        if logger is not None:
            logger.record(captured)
        return raw_result

    @staticmethod
    def _run_async_sync(fn: Callable[..., Any], kwargs: dict[str, Any]) -> Any:
        """Run an async function synchronously, handling nested event loops."""
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(fn(**kwargs))
        else:
            from concurrent.futures import ThreadPoolExecutor

            with ThreadPoolExecutor(1) as pool:
                return pool.submit(lambda: asyncio.run(fn(**kwargs))).result()

    async def async_execute_callable(
        self,
        fn: Callable[..., Any],
        kwargs: dict[str, Any],
        executor_options: dict[str, Any] | None = None,
        *,
        logger: "PacketExecutionLoggerProtocol | None" = None,
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
                tb = _traceback_module.format_exc()
                captured = ctx.get_captured(success=False, tb=tb)
                if logger is not None:
                    logger.record(captured)
                raise
        captured = ctx.get_captured(success=True)
        if logger is not None:
            logger.record(captured)
        return raw_result

    def with_options(self, **opts: Any) -> "LocalExecutor":
        """Return a new ``LocalExecutor``.

        ``LocalExecutor`` carries no state, so options are ignored.
        """
        return LocalExecutor()
