from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Protocol, Self, runtime_checkable

from orcapod.types import Schema

if TYPE_CHECKING:
    from orcapod.protocols.observability_protocols import PacketExecutionLoggerProtocol


@runtime_checkable
class PacketFunctionExecutorProtocol(Protocol):
    """Base executor protocol — defines identity, compatibility, and lifecycle.

    Executors decouple *what* a packet function computes from *where/how* it
    runs.  Each executor declares which ``packet_function_type_id`` values it
    supports.  Subtype protocols (e.g. ``PythonFunctionExecutorProtocol``)
    add execution methods specific to the function type.
    """

    @property
    def executor_type_id(self) -> str:
        """Unique identifier for this executor type, e.g. ``'local'``, ``'ray.v0'``."""
        ...

    def supported_function_type_ids(self) -> frozenset[str]:
        """Return the set of ``packet_function_type_id`` values this executor can handle.

        Return an empty frozenset to indicate support for *all* function types.
        """
        ...

    def supports(self, packet_function_type_id: str) -> bool:
        """Return ``True`` if this executor can run functions of the given type."""
        ...

    @property
    def supports_concurrent_execution(self) -> bool:
        """Whether this executor can meaningfully run multiple packets concurrently.

        When ``True``, iteration machinery may submit all packets via
        async execution concurrently and collect results before yielding.
        """
        ...

    def with_options(self, **opts: Any) -> Self:
        """Return a **new** executor instance configured with the given per-node options.

        Used by the pipeline to produce node-specific executor instances
        (e.g. with different CPU/GPU allocations) from a shared base executor.
        Implementations must always return a new instance, even when no
        options change, so that executors are effectively immutable value
        objects after construction.
        """
        ...

    def get_executor_data(self) -> dict[str, Any]:
        """Return metadata describing the execution environment.

        Stored alongside results for observability/provenance but does not
        affect content or pipeline hashes.
        """
        ...

    def get_executor_data_schema(self) -> Schema:
        """Return schema for the data returned by ``get_executor_data``."""
        ...


@runtime_checkable
class PythonFunctionExecutorProtocol(PacketFunctionExecutorProtocol, Protocol):
    """Executor protocol for Python callable-based packet functions.

    Extends ``PacketFunctionExecutorProtocol`` with callable-level
    execution methods.  The executor receives the raw Python function
    and its keyword arguments directly — the packet function handles
    packet construction/deconstruction around the executor call.
    """

    def execute_callable(
        self,
        fn: Callable[..., Any],
        kwargs: dict[str, Any],
        executor_options: dict[str, Any] | None = None,
        *,
        logger: PacketExecutionLoggerProtocol | None = None,
    ) -> Any:
        """Synchronously execute *fn* with *kwargs*, capturing I/O.

        Args:
            fn: The Python callable to execute.
            kwargs: Keyword arguments to pass to *fn*.
            executor_options: Optional per-call options (e.g. resource
                overrides).
            logger: Optional logger to record captured I/O via
                ``logger.record(**captured_fields)``.

        Returns:
            The raw return value of *fn* (or ``None`` on failure).
            On failure, the executor re-raises the original exception
            after recording captured logs.
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
        """Asynchronously execute *fn* with *kwargs*, capturing I/O.

        Args:
            fn: The Python callable to execute.
            kwargs: Keyword arguments to pass to *fn*.
            executor_options: Optional per-call options.
            logger: Optional logger to record captured I/O.

        Returns:
            The raw return value of *fn*.
        """
        ...
