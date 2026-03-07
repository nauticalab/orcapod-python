from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from orcapod.protocols.core_protocols import PacketFunctionProtocol, PacketProtocol


class PacketFunctionExecutorBase(ABC):
    """Abstract base class for packet function executors.

    An executor defines *where* and *how* a packet function's computation
    runs (e.g. in-process, on a Ray cluster, in a container).  Executors
    are type-specific: each declares the ``packet_function_type_id`` values
    it supports.

    Subclasses must implement ``execute`` and optionally ``async_execute``.
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

    @abstractmethod
    def execute(
        self,
        packet_function: PacketFunctionProtocol,
        packet: PacketProtocol,
    ) -> PacketProtocol | None:
        """Synchronously execute *packet_function* on *packet*.

        Implementations should call ``packet_function.direct_call(packet)``
        to invoke the function's native computation, bypassing executor
        routing.
        """
        ...

    async def async_execute(
        self,
        packet_function: PacketFunctionProtocol,
        packet: PacketProtocol,
    ) -> PacketProtocol | None:
        """Asynchronous counterpart of ``execute``.

        The default implementation delegates to ``execute`` synchronously.
        Subclasses should override for truly async execution.
        """
        return self.execute(packet_function, packet)

    @property
    def supports_concurrent_execution(self) -> bool:
        """Whether this executor can run multiple packets concurrently.

        Default is ``False``.  Subclasses that support truly concurrent
        execution (e.g. via a remote cluster) should override to ``True``.
        """
        return False

    def with_options(self, **opts: Any) -> "PacketFunctionExecutorBase":
        """Return an executor configured with the given per-node options.

        The default implementation ignores *opts* and returns *self*.
        Subclasses that support resource options (e.g. ``RayExecutor``)
        should override to return a new instance with the merged options.
        """
        return self

    def get_execution_data(self) -> dict[str, Any]:
        """Return metadata describing the execution environment.

        Recorded alongside results for observability but does not affect
        content or pipeline hashes.  The default returns the executor type id.
        """
        return {"executor_type": self.executor_type_id}
