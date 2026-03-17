from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from orcapod.core.executors.base import PacketFunctionExecutorBase

if TYPE_CHECKING:
    from orcapod.core.packet_function import PythonPacketFunction
    from orcapod.pipeline.logging_capture import CapturedLogs
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
        """Initialize Ray if it has not been initialized yet.

        Also registers a cloudpickle dispatch for ``logging.Logger`` so that
        user functions referencing loggers can be sent to Ray workers that
        do not have orcapod installed.

        By default cloudpickle serializes Logger instances by value, which
        traverses the parent chain to the root logger.  After
        ``install_capture_streams()`` the root logger has a
        ``ContextVarLoggingHandler`` from ``orcapod``.  Workers without
        orcapod cannot deserialize that class.

        Registering loggers as ``(logging.getLogger, (name,))`` is the
        correct semantic — loggers are name-keyed singletons — and produces
        no orcapod dependency in the pickled bytes.
        """
        import logging
        import ray

        try:
            import cloudpickle

            def _pickle_logger(l: logging.Logger) -> tuple:
                # Root logger has name "root" but must be fetched as ""
                name = "" if isinstance(l, logging.RootLogger) else l.name
                return logging.getLogger, (name,)

            cloudpickle.CloudPickler.dispatch[logging.Logger] = _pickle_logger
            cloudpickle.CloudPickler.dispatch[logging.RootLogger] = _pickle_logger
        except Exception:
            pass  # cloudpickle not available or API changed — best effort

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
    ) -> "tuple[PacketProtocol | None, CapturedLogs]":
        from orcapod.pipeline.logging_capture import CapturedLogs

        pf = self._as_python_packet_function(packet_function)
        if not pf.is_active():
            return None, CapturedLogs(success=True)

        raw, captured = self.execute_callable(pf._function, packet.as_dict())
        if not captured.success:
            return None, captured
        return pf._build_output_packet(raw), captured

    async def async_execute(
        self,
        packet_function: PacketFunctionProtocol,
        packet: PacketProtocol,
    ) -> "tuple[PacketProtocol | None, CapturedLogs]":
        from orcapod.pipeline.logging_capture import CapturedLogs

        pf = self._as_python_packet_function(packet_function)
        if not pf.is_active():
            return None, CapturedLogs(success=True)

        raw, captured = await self.async_execute_callable(pf._function, packet.as_dict())
        if not captured.success:
            return None, captured
        return pf._build_output_packet(raw), captured

    # -- PythonFunctionExecutorProtocol --

    @staticmethod
    def _make_capture_wrapper() -> Callable[..., Any]:
        """Return an inline capture wrapper suitable for Ray remote execution.

        The wrapper is defined as a closure (not a module-level import) so that
        cloudpickle serializes it by bytecode rather than by module reference.
        This means the Ray cluster workers do **not** need ``orcapod`` installed
        — only the standard library is required on the worker side.

        The wrapper returns a plain 6-tuple ``(raw_result, stdout, stderr,
        python_logs, traceback_str, success)`` so no orcapod types cross the
        Ray object store; the driver reconstructs :class:`CapturedLogs` from
        the tuple.
        """
        def _capture(fn: Any, kwargs: dict) -> tuple:
            import io
            import logging
            import os
            import sys
            import tempfile
            import traceback as _tb

            stdout_tmp = tempfile.TemporaryFile()
            stderr_tmp = tempfile.TemporaryFile()
            orig_stdout_fd = os.dup(1)
            orig_stderr_fd = os.dup(2)
            orig_sys_stdout = sys.stdout
            orig_sys_stderr = sys.stderr
            sys_stdout_buf = io.StringIO()
            sys_stderr_buf = io.StringIO()
            log_records: list = []

            fmt = logging.Formatter("%(levelname)s:%(name)s:%(message)s")

            class _H(logging.Handler):
                def emit(self, record: logging.LogRecord) -> None:
                    log_records.append(fmt.format(record))

            handler = _H()
            root_logger = logging.getLogger()
            orig_level = root_logger.level
            root_logger.setLevel(logging.DEBUG)
            root_logger.addHandler(handler)

            raw_result = None
            success = True
            tb_str = None
            try:
                sys.stdout.flush()
                sys.stderr.flush()
                os.dup2(stdout_tmp.fileno(), 1)
                os.dup2(stderr_tmp.fileno(), 2)
                sys.stdout = sys_stdout_buf
                sys.stderr = sys_stderr_buf
                try:
                    raw_result = fn(**kwargs)
                except Exception:
                    success = False
                    tb_str = _tb.format_exc()
            finally:
                sys.stdout = orig_sys_stdout
                sys.stderr = orig_sys_stderr
                os.dup2(orig_stdout_fd, 1)
                os.dup2(orig_stderr_fd, 2)
                os.close(orig_stdout_fd)
                os.close(orig_stderr_fd)
                root_logger.removeHandler(handler)
                root_logger.setLevel(orig_level)
                stdout_tmp.seek(0)
                stderr_tmp.seek(0)
                cap_stdout = (
                    stdout_tmp.read().decode("utf-8", errors="replace")
                    + sys_stdout_buf.getvalue()
                )
                cap_stderr = (
                    stderr_tmp.read().decode("utf-8", errors="replace")
                    + sys_stderr_buf.getvalue()
                )
                stdout_tmp.close()
                stderr_tmp.close()

            return raw_result, cap_stdout, cap_stderr, "\n".join(log_records), tb_str, success

        return _capture

    def execute_callable(
        self,
        fn: Callable[..., Any],
        kwargs: dict[str, Any],
        executor_options: dict[str, Any] | None = None,
    ) -> "tuple[Any, CapturedLogs]":
        """Execute *fn* on the Ray cluster with fd-level I/O capture.

        The capture wrapper is serialized by bytecode (not module reference) so
        the Ray cluster workers do not need ``orcapod`` installed.
        """
        import ray

        from orcapod.pipeline.logging_capture import CapturedLogs

        self._ensure_ray_initialized()
        wrapper = self._make_capture_wrapper()
        wrapper.__name__ = fn.__name__
        wrapper.__qualname__ = fn.__qualname__
        remote_fn = ray.remote(**self._build_remote_opts())(wrapper)
        ref = remote_fn.remote(fn, kwargs)
        raw, stdout, stderr, python_logs, tb, success = ray.get(ref)
        return raw, CapturedLogs(
            stdout=stdout, stderr=stderr, python_logs=python_logs,
            traceback=tb, success=success,
        )

    async def async_execute_callable(
        self,
        fn: Callable[..., Any],
        kwargs: dict[str, Any],
        executor_options: dict[str, Any] | None = None,
    ) -> "tuple[Any, CapturedLogs]":
        """Async counterpart of :meth:`execute_callable`."""
        import ray

        from orcapod.pipeline.logging_capture import CapturedLogs

        self._ensure_ray_initialized()
        wrapper = self._make_capture_wrapper()
        wrapper.__name__ = fn.__name__
        wrapper.__qualname__ = fn.__qualname__
        remote_fn = ray.remote(**self._build_remote_opts())(wrapper)
        ref = remote_fn.remote(fn, kwargs)
        raw, stdout, stderr, python_logs, tb, success = await asyncio.wrap_future(
            ref.future()
        )
        return raw, CapturedLogs(
            stdout=stdout, stderr=stderr, python_logs=python_logs,
            traceback=tb, success=success,
        )

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
