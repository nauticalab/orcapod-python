from __future__ import annotations

import asyncio
import threading
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from orcapod.core.executors.base import PythonFunctionExecutorBase
from orcapod.core.executors.capture_wrapper import make_capture_wrapper

if TYPE_CHECKING:
    from orcapod.protocols.observability_protocols import PacketExecutionLoggerProtocol

# Guard flag: cloudpickle Logger dispatch is registered at most once per process.
_cloudpickle_dispatch_registered = False


class RayExecutor(PythonFunctionExecutorBase):
    """Executor that dispatches Python packet functions to a Ray cluster.

    Only supports ``packet_function_type_id == "python.function.v0"``.

    If Ray has not been initialized yet, this executor will call
    ``ray.init()`` lazily on first use, passing ``ray_address`` and
    ``runtime_env`` if provided.  Callers can also call ``ray.init()``
    themselves before creating the executor — in that case the executor
    will detect the existing session and skip initialization.

    Note:
        ``ray`` is an optional dependency.  Import errors surface at
        construction time so callers get a clear message.
    """

    SUPPORTED_TYPES: frozenset[str] = frozenset({"python.function.v0"})

    def __init__(
        self,
        *,
        ray_address: str | None = None,
        runtime_env: dict[str, Any] | None = None,
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
            runtime_env: Runtime environment configuration passed to
                ``ray.init()``.  Supports ``py_modules`` (as module
                objects or paths), ``pip``, ``working_dir``, ``env_vars``,
                etc.  Applied cluster-wide at init time — not per-task —
                so that dependencies are set up once rather than on every
                remote call.
            num_cpus: Number of CPUs to request per remote task.  Passed
                directly to ``ray.remote(num_cpus=...)``.
            num_gpus: Number of GPUs to request per remote task.  Passed
                directly to ``ray.remote(num_gpus=...)``.
            resources: Custom resource requirements dict forwarded to
                ``ray.remote(resources=...)``.
            **ray_remote_opts: Any additional keyword arguments accepted by
                ``ray.remote()`` (e.g. ``memory``, ``max_calls``,
                ``accelerator_type``).
        """
        try:
            import ray  # noqa: F401
        except ImportError as exc:
            raise ImportError(
                "RayExecutor requires the 'ray' package. "
                "Install it with: pip install ray"
            ) from exc

        self._ray_address = ray_address
        self._runtime_env = runtime_env

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

        # Cache for Ray remote-function objects keyed by normalised remote opts.
        # The capture wrapper closure has identical bytecode for every call, so
        # it only needs to be remotized once per distinct option set.
        self._remote_fn_cache: dict[Any, Any] = {}
        # Lock guards cache population under concurrent execute_callable() calls
        # (supports_concurrent_execution = True, so callers may use threads).
        self._remote_fn_cache_lock = threading.Lock()

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

        global _cloudpickle_dispatch_registered
        if not _cloudpickle_dispatch_registered:
            try:
                import cloudpickle

                def _pickle_logger(l: logging.Logger) -> tuple:
                    # Root logger has name "root" but must be fetched as ""
                    name = "" if isinstance(l, logging.RootLogger) else l.name
                    return logging.getLogger, (name,)

                if logging.Logger not in cloudpickle.CloudPickler.dispatch:
                    cloudpickle.CloudPickler.dispatch[logging.Logger] = _pickle_logger
                if logging.RootLogger not in cloudpickle.CloudPickler.dispatch:
                    cloudpickle.CloudPickler.dispatch[logging.RootLogger] = _pickle_logger
                _cloudpickle_dispatch_registered = True
            except (ImportError, AttributeError, KeyError):
                pass  # cloudpickle not available or API changed — best effort

        if not ray.is_initialized():
            init_kwargs: dict[str, Any] = {}
            if self._ray_address is not None:
                init_kwargs["address"] = self._ray_address
            if self._runtime_env is not None:
                init_kwargs["runtime_env"] = self._runtime_env
            ray.init(**init_kwargs)

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

    def execute_callable(
        self,
        fn: Callable[..., Any],
        kwargs: dict[str, Any],
        executor_options: dict[str, Any] | None = None,
        *,
        logger: PacketExecutionLoggerProtocol | None = None,
    ) -> Any:
        """Execute *fn* on the Ray cluster with fd-level I/O capture.

        The capture wrapper is serialized by bytecode (not module reference) so
        the Ray cluster workers do not need ``orcapod`` installed.

        On success, returns the raw result and records captured I/O to *logger*.
        On failure, the worker raises a ``_CapturedTaskError`` which Ray
        propagates via ``RayTaskError``.  The driver extracts the captured I/O,
        records it to *logger*, and re-raises the **original** user exception
        so that Ray's error handling (retries, etc.) is not disrupted.
        """
        import ray

        self._ensure_ray_initialized()
        remote_fn = self._get_remote_fn(ray)
        ref = remote_fn.options(name=fn.__name__).remote(fn, kwargs)

        try:
            raw, stdout_log, stderr_log, python_logs = ray.get(ref)
        except Exception as exc:
            handled = self._handle_worker_error(exc, logger)
            if handled is exc:
                raise
            raise handled from exc

        self._record_success(stdout_log, stderr_log, python_logs, logger)
        return raw

    async def async_execute_callable(
        self,
        fn: Callable[..., Any],
        kwargs: dict[str, Any],
        executor_options: dict[str, Any] | None = None,
        *,
        logger: PacketExecutionLoggerProtocol | None = None,
    ) -> Any:
        """Async counterpart of `execute_callable`."""
        import ray

        self._ensure_ray_initialized()
        remote_fn = self._get_remote_fn(ray)
        ref = remote_fn.options(name=fn.__name__).remote(fn, kwargs)

        try:
            # In Ray client mode (ray://) ClientObjectRef.future() returns a
            # concurrent.futures.Future which cannot be directly awaited.
            # asyncio.wrap_future() handles both Future types correctly.
            raw, stdout_log, stderr_log, python_logs = await asyncio.wrap_future(
                ref.future()
            )
        except Exception as exc:
            handled = self._handle_worker_error(exc, logger)
            if handled is exc:
                raise
            raise handled from exc

        self._record_success(stdout_log, stderr_log, python_logs, logger)
        return raw

    def _get_remote_fn(self, ray: Any) -> Any:
        """Return a cached Ray remote wrapper for the capture closure.

        The capture wrapper's bytecode is identical on every invocation, so
        it only needs to be remotized once per distinct set of remote options.
        Caching avoids the non-trivial overhead of ``ray.remote()`` on every
        packet.

        A ``threading.Lock`` guards population so that concurrent calls
        (``supports_concurrent_execution = True``) never redundantly call
        ``ray.remote()`` for the same option set.
        """
        opts = self._build_remote_opts()
        cache_key = self._normalize_opts(opts)
        if cache_key not in self._remote_fn_cache:
            with self._remote_fn_cache_lock:
                # Double-checked: another thread may have filled the slot
                # while we waited for the lock.
                if cache_key not in self._remote_fn_cache:
                    wrapper = make_capture_wrapper()
                    self._remote_fn_cache[cache_key] = ray.remote(**opts)(wrapper)
        return self._remote_fn_cache[cache_key]

    @staticmethod
    def _normalize_opts(value: Any) -> Any:
        """Recursively normalize option values to produce stable cache keys.

        Dicts with different insertion orders but identical contents
        (common for ``resources`` / ``runtime_env``) produce the same key.
        """
        if isinstance(value, dict):
            return tuple(
                (k, RayExecutor._normalize_opts(v))
                for k, v in sorted(value.items())
            )
        if isinstance(value, (list, tuple)):
            return tuple(RayExecutor._normalize_opts(v) for v in value)
        if isinstance(value, set):
            return tuple(sorted((RayExecutor._normalize_opts(v) for v in value), key=repr))
        return value

    @staticmethod
    def _record_success(
        stdout_log: str,
        stderr_log: str,
        python_logs: str,
        logger: PacketExecutionLoggerProtocol | None,
    ) -> None:
        """Record a successful execution to the logger."""
        if logger is None:
            return
        logger.record(
            stdout_log=stdout_log, stderr_log=stderr_log, python_logs=python_logs,
            traceback=None, success=True,
        )

    @staticmethod
    def _handle_worker_error(
        exc: Exception,
        logger: PacketExecutionLoggerProtocol | None,
    ) -> Exception:
        """Extract captured I/O from a worker error, record it, and return the exception to raise.

        The worker's ``_CapturedTaskError`` is wrapped by Ray in a
        ``RayTaskError``.  We reach through to get the captured output
        and the original user exception, then return it so the caller
        can raise with proper chaining.

        If the exception is not from our wrapper (e.g. a Ray system error),
        it is returned as-is.
        """
        # Ray wraps worker exceptions: exc.cause is the _CapturedTaskError
        task_err = getattr(exc, "cause", None)
        if task_err is not None and hasattr(task_err, "captured_stdout_log"):
            if logger is not None:
                logger.record(
                    stdout_log=task_err.captured_stdout_log,
                    stderr_log=task_err.captured_stderr_log,
                    python_logs=task_err.captured_python_logs,
                    traceback=task_err.captured_traceback,
                    success=False,
                )
            return task_err.cause
        # Not our wrapper error (Ray system error, etc.) — return as-is
        return exc

    def with_options(self, **opts: Any) -> RayExecutor:
        """Return a new ``RayExecutor`` with the given options merged in.

        The returned executor shares the same ``ray_address`` and
        ``runtime_env``.  All opts are passed through to
        ``ray.remote()``/``.options()`` as-is — no keys are hardcoded,
        so any option Ray supports (``num_cpus``, ``num_gpus``,
        ``memory``, ``max_calls``, ``accelerator_type``,
        ``label_selector``, …) can be used.  Node-level opts override
        pipeline-level defaults.
        """
        merged = {**self._remote_opts, **opts}
        # Pop constructor-level keys so they don't collide with the explicit
        # keyword arguments below.
        ray_address = merged.pop("ray_address", self._ray_address)
        runtime_env = merged.pop("runtime_env", self._runtime_env)
        return RayExecutor(
            ray_address=ray_address,
            runtime_env=runtime_env,
            **merged,
        )

    def get_execution_data(self) -> dict[str, Any]:
        data: dict[str, Any] = {
            "executor_type": self.executor_type_id,
            "ray_address": self._ray_address or "auto",
            **self._remote_opts,
        }
        if self._runtime_env is not None:
            data["runtime_env"] = True  # flag presence without dumping contents
        return data
