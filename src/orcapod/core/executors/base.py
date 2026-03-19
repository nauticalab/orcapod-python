from __future__ import annotations

import copy
from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from orcapod.protocols.observability_protocols import PacketExecutionLoggerProtocol


class PythonFunctionExecutorBase(ABC):
    """Abstract base class for executors that run PythonPacketFunction callables.

    An executor defines *where* and *how* a PythonPacketFunction's computation
    runs (e.g. in-process, on a Ray cluster, in a container).

    Subclasses must implement ``execute_callable`` and optionally
    ``async_execute_callable``.
    """

    @property
    @abstractmethod
    def executor_type_id(self) -> str:
        """Unique identifier for this executor type, e.g. ``'local'``, ``'ray.v0'``."""
        ...

    @abstractmethod
    def supported_function_type_ids(self) -> frozenset[str]:
        """Return the set of ``packet_function_type_id`` values this executor can run.

        Return an empty ``frozenset`` to indicate support for *all* types.
        """
        ...

    def supports(self, packet_function_type_id: str) -> bool:
        """Return ``True`` if this executor can handle the given function type.

        Default implementation checks membership in
        ``supported_function_type_ids()``; an empty set means "supports all".
        """
        ids = self.supported_function_type_ids()
        return len(ids) == 0 or packet_function_type_id in ids

    @property
    def supports_concurrent_execution(self) -> bool:
        """Whether this executor can run multiple packets concurrently.

        Default is ``False``.  Subclasses that support truly concurrent
        execution (e.g. via a remote cluster) should override to ``True``.
        """
        return False

    def with_options(self, **opts: Any) -> PythonFunctionExecutorBase:
        """Return a **new** executor instance configured with the given options.

        The default implementation returns a shallow copy of *self*.
        Subclasses that carry mutable state (e.g. ``RayExecutor``) should
        override to produce a properly configured new instance.
        """
        return copy.copy(self)

    # ------------------------------------------------------------------
    # Callable-level execution (PythonFunctionExecutorProtocol)
    # ------------------------------------------------------------------

    @abstractmethod
    def execute_callable(
        self,
        fn: Callable[..., Any],
        kwargs: dict[str, Any],
        executor_options: dict[str, Any] | None = None,
        *,
        logger: PacketExecutionLoggerProtocol | None = None,
    ) -> Any:
        """Synchronously execute *fn* with *kwargs*, returning the raw result.

        Subclasses must implement this to provide I/O capture and logger
        recording.  On failure, the original exception must be re-raised
        after recording to the logger.

        Args:
            fn: The Python callable to execute.
            kwargs: Keyword arguments to pass to *fn*.
            executor_options: Optional per-call options.
            logger: Optional logger to record captured I/O.

        Returns:
            The raw return value of *fn*.
        """
        ...

    async def async_execute_callable(
        self,
        fn: Callable[..., Any],
        kwargs: dict[str, Any],
        executor_options: dict[str, Any] | None = None,
        *,
        logger: PacketExecutionLoggerProtocol | None = None,
    ) -> Any:
        """Asynchronously execute *fn* with *kwargs*, returning the raw result.

        Default implementation delegates to ``execute_callable``
        synchronously.  Subclasses should override for truly async execution.
        """
        return self.execute_callable(fn, kwargs, executor_options, logger=logger)

    def get_execution_data(self) -> dict[str, Any]:
        """Return metadata describing the execution environment.

        Recorded alongside results for observability but does not affect
        content or pipeline hashes.  The default returns the executor type id.
        """
        return {"executor_type": self.executor_type_id}
