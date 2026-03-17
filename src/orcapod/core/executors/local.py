from __future__ import annotations

import asyncio
import inspect
import traceback as _traceback_module
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from orcapod.core.executors.base import PacketFunctionExecutorBase

if TYPE_CHECKING:
    from orcapod.pipeline.logging_capture import CapturedLogs
    from orcapod.protocols.core_protocols import PacketFunctionProtocol, PacketProtocol


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
        packet_function: PacketFunctionProtocol,
        packet: PacketProtocol,
    ) -> "tuple[PacketProtocol | None, CapturedLogs]":
        return packet_function.direct_call(packet)

    async def async_execute(
        self,
        packet_function: PacketFunctionProtocol,
        packet: PacketProtocol,
    ) -> "tuple[PacketProtocol | None, CapturedLogs]":
        return await packet_function.direct_async_call(packet)

    # -- PythonFunctionExecutorProtocol --

    def execute_callable(
        self,
        fn: Callable[..., Any],
        kwargs: dict[str, Any],
        executor_options: dict[str, Any] | None = None,
    ) -> "tuple[Any, CapturedLogs]":
        from orcapod.pipeline.logging_capture import CapturedLogs, LocalCaptureContext

        ctx = LocalCaptureContext()
        raw_result = None
        success = True
        tb: str | None = None
        with ctx:
            try:
                if inspect.iscoroutinefunction(fn):
                    raw_result = self._run_async_sync(fn, kwargs)
                else:
                    raw_result = fn(**kwargs)
            except Exception:
                success = False
                tb = _traceback_module.format_exc()
        return raw_result, ctx.get_captured(success=success, tb=tb)

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
    ) -> "tuple[Any, CapturedLogs]":
        from orcapod.pipeline.logging_capture import CapturedLogs, LocalCaptureContext

        ctx = LocalCaptureContext()
        raw_result = None
        success = True
        tb: str | None = None
        with ctx:
            try:
                if inspect.iscoroutinefunction(fn):
                    raw_result = await fn(**kwargs)
                else:
                    loop = asyncio.get_running_loop()
                    raw_result = await loop.run_in_executor(None, lambda: fn(**kwargs))
            except Exception:
                success = False
                tb = _traceback_module.format_exc()
        return raw_result, ctx.get_captured(success=success, tb=tb)

    def with_options(self, **opts: Any) -> "LocalExecutor":
        """Return a new ``LocalExecutor``.

        ``LocalExecutor`` carries no state, so options are ignored.
        """
        return LocalExecutor()
