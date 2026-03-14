from __future__ import annotations

import asyncio
import inspect
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from orcapod.core.executors.base import PacketFunctionExecutorBase

if TYPE_CHECKING:
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
    ) -> PacketProtocol | None:
        return packet_function.direct_call(packet)

    async def async_execute(
        self,
        packet_function: PacketFunctionProtocol,
        packet: PacketProtocol,
    ) -> PacketProtocol | None:
        return await packet_function.direct_async_call(packet)

    # -- PythonFunctionExecutorProtocol --

    def execute_callable(
        self,
        fn: Callable[..., Any],
        kwargs: dict[str, Any],
        executor_options: dict[str, Any] | None = None,
    ) -> Any:
        if inspect.iscoroutinefunction(fn):
            return asyncio.run(fn(**kwargs))
        return fn(**kwargs)

    async def async_execute_callable(
        self,
        fn: Callable[..., Any],
        kwargs: dict[str, Any],
        executor_options: dict[str, Any] | None = None,
    ) -> Any:
        if inspect.iscoroutinefunction(fn):
            return await fn(**kwargs)
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, lambda: fn(**kwargs))

    def with_options(self, **opts: Any) -> "LocalExecutor":
        """Return a new ``LocalExecutor``.

        ``LocalExecutor`` carries no state, so options are ignored.
        """
        return LocalExecutor()
