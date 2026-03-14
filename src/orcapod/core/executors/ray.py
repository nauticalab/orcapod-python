from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from orcapod.core.executors.base import PacketFunctionExecutorBase

if TYPE_CHECKING:
    from orcapod.core.packet_function import PythonPacketFunction
    from orcapod.protocols.core_protocols import PacketFunctionProtocol, PacketProtocol


class RayExecutor(PacketFunctionExecutorBase):
    """Executor that dispatches Python packet functions to a Ray cluster.

    Only supports ``packet_function_type_id == "python.function.v0"``.

    The caller is responsible for calling ``ray.init(...)`` before using
    this executor.  If ``ray_address`` is provided and Ray has not been
    initialized yet, this executor will call ``ray.init(address=...)``
    lazily on first use.

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
        **ray_remote_opts: Any,
    ) -> None:
        """Create a RayExecutor.

        Args:
            ray_address: Address of the Ray cluster to connect to (e.g.
                ``"ray://host:10001"``).  If ``None`` and Ray is not yet
                initialised, ``ray.init()`` is called without an address,
                which starts a local cluster.
            num_cpus: Number of CPUs to request per remote task.  Passed
                directly to ``ray.remote(num_cpus=...)``.
            num_gpus: Number of GPUs to request per remote task.  Passed
                directly to ``ray.remote(num_gpus=...)``.
            resources: Custom resource requirements dict forwarded to
                ``ray.remote(resources=...)``.
            **ray_remote_opts: Any additional keyword arguments accepted by
                ``ray.remote()`` (e.g. ``memory``, ``max_calls``,
                ``runtime_env``, ``accelerator_type``).
        """
        try:
            import ray  # noqa: F401
        except ImportError as exc:
            raise ImportError(
                "RayExecutor requires the 'ray' package. "
                "Install it with: pip install ray"
            ) from exc

        self._ray_address = ray_address

        # Collect all remote opts into a single dict so that arbitrary Ray
        # options (memory, max_calls, accelerator_type, label_selector, …)
        # can be passed through without hardcoding each one.
        self._remote_opts: dict[str, Any] = {}
        if num_cpus is not None:
            self._remote_opts["num_cpus"] = num_cpus
        if num_gpus is not None:
            self._remote_opts["num_gpus"] = num_gpus
        if resources is not None:
            self._remote_opts["resources"] = resources
        self._remote_opts.update(ray_remote_opts)

    def _ensure_ray_initialized(self) -> None:
        """Initialize Ray if it has not been initialized yet."""
        import ray

        if not ray.is_initialized():
            if self._ray_address is not None:
                ray.init(address=self._ray_address)
            else:
                ray.init()

    @property
    def executor_type_id(self) -> str:
        return "ray.v0"

    def supported_function_type_ids(self) -> frozenset[str]:
        return self.SUPPORTED_TYPES

    @property
    def supports_concurrent_execution(self) -> bool:
        return True

    def _build_remote_opts(self) -> dict[str, Any]:
        """Return a copy of the Ray remote options dict."""
        return dict(self._remote_opts)

    def _as_python_packet_function(
        self, packet_function: PacketFunctionProtocol
    ) -> "PythonPacketFunction":
        """Return *packet_function* cast to ``PythonPacketFunction``, or raise.

        Raises:
            TypeError: If *packet_function* is not a ``PythonPacketFunction``
                instance and therefore does not expose the attributes required
                for remote execution.
        """
        from orcapod.core.packet_function import PythonPacketFunction

        if not isinstance(packet_function, PythonPacketFunction):
            raise TypeError(
                f"RayExecutor only supports PythonPacketFunction, "
                f"got {type(packet_function).__name__}"
            )
        return packet_function

    def execute(
        self,
        packet_function: PacketFunctionProtocol,
        packet: PacketProtocol,
    ) -> PacketProtocol | None:
        import ray

        pf = self._as_python_packet_function(packet_function)
        if not pf.is_active():
            return None

        self._ensure_ray_initialized()

        kwargs = packet.as_dict()
        remote_fn = ray.remote(**self._build_remote_opts())(pf._function)
        ref = remote_fn.remote(**kwargs)
        raw_result = ray.get(ref)
        return pf._build_output_packet(raw_result)

    async def async_execute(
        self,
        packet_function: PacketFunctionProtocol,
        packet: PacketProtocol,
    ) -> PacketProtocol | None:
        import ray

        pf = self._as_python_packet_function(packet_function)
        if not pf.is_active():
            return None

        self._ensure_ray_initialized()

        kwargs = packet.as_dict()
        remote_fn = ray.remote(**self._build_remote_opts())(pf._function)
        ref = remote_fn.remote(**kwargs)
        raw_result = await asyncio.wrap_future(ref.future())
        return pf._build_output_packet(raw_result)

    # -- PythonFunctionExecutorProtocol --

    def execute_callable(
        self,
        fn: Callable[..., Any],
        kwargs: dict[str, Any],
        executor_options: dict[str, Any] | None = None,
    ) -> Any:
        import ray

        self._ensure_ray_initialized()
        remote_fn = ray.remote(**self._build_remote_opts())(fn)
        ref = remote_fn.remote(**kwargs)
        return ray.get(ref)

    async def async_execute_callable(
        self,
        fn: Callable[..., Any],
        kwargs: dict[str, Any],
        executor_options: dict[str, Any] | None = None,
    ) -> Any:
        import ray

        self._ensure_ray_initialized()
        remote_fn = ray.remote(**self._build_remote_opts())(fn)
        ref = remote_fn.remote(**kwargs)
        return await asyncio.wrap_future(ref.future())

    def with_options(self, **opts: Any) -> "RayExecutor":
        """Return a new ``RayExecutor`` with the given options merged in.

        The returned executor shares the same ``ray_address``.  All opts are
        passed through to ``ray.remote()``/``.options()`` as-is — no keys are
        hardcoded, so any option Ray supports (``num_cpus``, ``num_gpus``,
        ``memory``, ``max_calls``, ``accelerator_type``, ``label_selector``,
        ``runtime_env``, …) can be used.  Node-level opts override
        pipeline-level defaults.
        """
        merged = {**self._remote_opts, **opts}
        return RayExecutor(ray_address=self._ray_address, **merged)

    def get_execution_data(self) -> dict[str, Any]:
        return {
            "executor_type": self.executor_type_id,
            "ray_address": self._ray_address or "auto",
            **self._remote_opts,
        }
