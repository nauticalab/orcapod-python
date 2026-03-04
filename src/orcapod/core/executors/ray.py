from __future__ import annotations

from typing import TYPE_CHECKING, Any

from orcapod.core.executors.base import PacketFunctionExecutorBase

if TYPE_CHECKING:
    from orcapod.protocols.core_protocols import PacketFunctionProtocol, PacketProtocol


class RayExecutor(PacketFunctionExecutorBase):
    """Executor that dispatches Python packet functions to a Ray cluster.

    Only supports ``packet_function_type_id == "python.function.v0"``.

    Note:
        ``ray`` is an optional dependency.  Import errors surface at
        construction time so callers get a clear message.
    """

    SUPPORTED_TYPES: frozenset[str] = frozenset({"python.function.v0"})

    def __init__(
        self,
        *,
        ray_address: str | None = None,
        num_cpus: int | None = None,
        num_gpus: int | None = None,
        resources: dict[str, float] | None = None,
    ) -> None:
        try:
            import ray  # noqa: F401
        except ImportError as exc:
            raise ImportError(
                "RayExecutor requires the 'ray' package. "
                "Install it with: pip install ray"
            ) from exc

        self._ray_address = ray_address
        self._num_cpus = num_cpus
        self._num_gpus = num_gpus
        self._resources = resources

    @property
    def executor_type_id(self) -> str:
        return "ray.v0"

    def supported_function_type_ids(self) -> frozenset[str]:
        return self.SUPPORTED_TYPES

    @property
    def supports_concurrent_execution(self) -> bool:
        return True

    def execute(
        self,
        packet_function: PacketFunctionProtocol,
        packet: PacketProtocol,
    ) -> PacketProtocol | None:
        import ray

        remote_opts: dict[str, Any] = {}
        if self._num_cpus is not None:
            remote_opts["num_cpus"] = self._num_cpus
        if self._num_gpus is not None:
            remote_opts["num_gpus"] = self._num_gpus
        if self._resources is not None:
            remote_opts["resources"] = self._resources

        @ray.remote(**remote_opts)
        def _run(pf: Any, pkt: Any) -> Any:
            return pf.direct_call(pkt)

        ref = _run.remote(packet_function, packet)
        return ray.get(ref)

    async def async_execute(
        self,
        packet_function: PacketFunctionProtocol,
        packet: PacketProtocol,
    ) -> PacketProtocol | None:
        import ray

        remote_opts: dict[str, Any] = {}
        if self._num_cpus is not None:
            remote_opts["num_cpus"] = self._num_cpus
        if self._num_gpus is not None:
            remote_opts["num_gpus"] = self._num_gpus
        if self._resources is not None:
            remote_opts["resources"] = self._resources

        @ray.remote(**remote_opts)
        def _run(pf: Any, pkt: Any) -> Any:
            return pf.direct_call(pkt)

        ref = _run.remote(packet_function, packet)
        return await ref

    def get_execution_data(self) -> dict[str, Any]:
        data: dict[str, Any] = {
            "executor_type": self.executor_type_id,
            "ray_address": self._ray_address or "auto",
        }
        if self._num_cpus is not None:
            data["num_cpus"] = self._num_cpus
        if self._num_gpus is not None:
            data["num_gpus"] = self._num_gpus
        return data
