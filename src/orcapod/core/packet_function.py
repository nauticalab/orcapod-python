from __future__ import annotations

import inspect
import logging
import re
import sys
from abc import abstractmethod
from collections.abc import Callable, Iterable, Sequence
from datetime import datetime, timezone
import typing
from typing import TYPE_CHECKING, Any, ClassVar, Generic, Literal, TypeVar

from uuid_utils import uuid7

from orcapod.config import Config
from orcapod.contexts import DataContext
from orcapod.core.base import TraceableBase
from orcapod.core.datagrams import Packet
from orcapod.hashing.hash_utils import (
    get_function_components,
    get_function_signature,
)
from orcapod.core.result_cache import ResultCache
from orcapod.protocols.core_protocols import PacketFunctionProtocol, PacketProtocol
from orcapod.protocols.core_protocols.executor import (
    PacketFunctionExecutorProtocol,
    PythonFunctionExecutorProtocol,
)
from orcapod.protocols.database_protocols import ArrowDatabaseProtocol
from orcapod.system_constants import constants
from orcapod.types import DataValue, Schema, SchemaLike
from orcapod.utils import schema_utils
from orcapod.utils.git_utils import get_git_info_for_python_object
from orcapod.utils.lazy_module import LazyModule

from orcapod.protocols.observability_protocols import PacketExecutionLoggerProtocol

if TYPE_CHECKING:
    import pyarrow as pa
    import pyarrow.compute as pc
else:
    pa = LazyModule("pyarrow")
    pc = LazyModule("pyarrow.compute")

module_logger = logging.getLogger(__name__)

error_handling_options = Literal["raise", "ignore", "warn"]

# ---------------------------------------------------------------------------
# Shared executor for running async functions synchronously from within
# an event loop (see PythonPacketFunction._call_async_function_sync).
# ---------------------------------------------------------------------------

_sync_executor = None


def _get_sync_executor():
    """Return a shared single-thread executor for sync fallback of async fns."""
    global _sync_executor
    if _sync_executor is None:
        from concurrent.futures import ThreadPoolExecutor

        _sync_executor = ThreadPoolExecutor(1)
    return _sync_executor


def parse_function_outputs(
    output_keys: Sequence[str], values: Any
) -> dict[str, DataValue]:
    """
    Map raw function return values to a keyed output dict.

    Rules:
        - ``output_keys = []``: return value is ignored; empty dict returned.
        - ``output_keys = ["result"]``: any value (including iterables) is stored as-is
          under the single key.
        - ``output_keys = ["a", "b", ...]``: ``values`` must be iterable and its length
          must match the number of keys.

    Args:
        output_keys: Ordered list of output key names.
        values: Raw return value from the function.

    Returns:
        Dict mapping each output key to its corresponding value.

    Raises:
        ValueError: If ``values`` is not iterable when multiple keys are given, or if
            the number of values does not match the number of keys.
    """
    if len(output_keys) == 0:
        output_values: list[Any] = []
    elif len(output_keys) == 1:
        output_values = [values]
    elif isinstance(values, Iterable):
        output_values = list(values)
    else:
        raise ValueError(
            "Values returned by function must be sequence-like if multiple output keys are specified"
        )

    if len(output_values) != len(output_keys):
        raise ValueError(
            f"Number of output keys {len(output_keys)}:{output_keys} does not match "
            f"number of values returned by function {len(output_values)}"
        )

    return dict(zip(output_keys, output_values))


E = TypeVar("E", bound=PacketFunctionExecutorProtocol)


class PacketFunctionBase(TraceableBase, Generic[E]):
    """Abstract base class for PacketFunctionProtocol.

    Type-parameterized with the executor protocol ``E``.  Concrete
    subclasses that bind ``E`` (e.g. ``class Foo(PacketFunctionBase[SomeProto])``)
    get automatic ``isinstance`` validation in ``set_executor`` at class
    definition time via ``__init_subclass__``.
    """

    _resolved_executor_protocol: ClassVar[type | None] = None

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        for base in getattr(cls, "__orig_bases__", ()):
            origin = typing.get_origin(base)
            if origin is PacketFunctionBase:
                args = typing.get_args(base)
                if args and not isinstance(args[0], TypeVar):
                    cls._resolved_executor_protocol = args[0]
                    return

    def __init__(
        self,
        version: str = "v0.0",
        label: str | None = None,
        data_context: str | DataContext | None = None,
        config: Config | None = None,
        executor: PacketFunctionExecutorProtocol | None = None,
    ):
        super().__init__(label=label, data_context=data_context, config=config)
        self._active = True
        self._version = version
        self._executor: E | None = None

        # Parse version string to extract major and minor versions
        # 0.5.2 -> 0 and 5.2, 1.3rc -> 1 and 3rc
        match = re.match(r"\D*(\d+)\.(.*)", version)
        if match:
            self._major_version = int(match.group(1))
            self._minor_version = match.group(2)
        else:
            raise ValueError(
                f"Version string {version} does not contain a valid version number"
            )

        self._output_packet_schema_hash = None

        # Validate and set via the property setter.  This works because
        # concrete subclasses define packet_function_type_id as a simple
        # constant property that does not depend on instance state set
        # *after* super().__init__().
        if executor is not None:
            self.executor = executor

    def computed_label(self) -> str | None:
        """Return the canonical function name as the label if no explicit label is given."""
        return self.canonical_function_name

    @property
    def output_packet_schema_hash(self) -> str:
        """Return the hash of the output packet schema as a string.

        The hash is computed lazily on first access and cached for subsequent calls.

        Returns:
            The hash string of the output packet schema.
        """
        if self._output_packet_schema_hash is None:
            self._output_packet_schema_hash = (
                self.data_context.semantic_hasher.hash_object(
                    self.output_packet_schema
                ).to_string()
            )
        return self._output_packet_schema_hash

    @property
    def uri(self) -> tuple[str, ...]:
        return (
            self.canonical_function_name,
            self.output_packet_schema_hash,
            f"v{self.major_version}",
            self.packet_function_type_id,
        )

    def identity_structure(self) -> Any:
        return self.uri

    def pipeline_identity_structure(self) -> Any:
        return self.uri

    @property
    def major_version(self) -> int:
        return self._major_version

    @property
    def minor_version_string(self) -> str:
        return self._minor_version

    @property
    @abstractmethod
    def packet_function_type_id(self) -> str:
        """Unique function type identifier (e.g. ``"python.function.v1"``)."""
        ...

    @property
    @abstractmethod
    def canonical_function_name(self) -> str:
        """Human-readable function identifier."""
        ...

    @property
    @abstractmethod
    def input_packet_schema(self) -> Schema:
        """Schema describing the input packets this function accepts."""
        ...

    @property
    @abstractmethod
    def output_packet_schema(self) -> Schema:
        """Schema describing the output packets this function produces."""
        ...

    @abstractmethod
    def get_function_variation_data(self) -> dict[str, Any]:
        """Raw data defining function variation"""
        ...

    @abstractmethod
    def get_execution_data(self) -> dict[str, Any]:
        """Raw data defining execution context"""
        ...

    # ==================== Executor ====================

    @property
    def executor(self) -> E | None:
        """Return the executor used to run this packet function, or ``None`` for direct execution."""
        return self._executor

    @executor.setter
    def executor(self, executor: E | None) -> None:
        """Set or clear the executor for this packet function.

        Delegates to ``set_executor`` for validation.
        """
        self.set_executor(executor)

    def set_executor(self, executor: PacketFunctionExecutorProtocol | None) -> None:
        """Set or clear the executor, validating type compatibility.

        Performs two checks:
        1. The executor supports this function's ``packet_function_type_id``.
        2. If the subclass bound ``E`` via ``Generic[E]``, the executor is an
           instance of the resolved protocol (checked once at assignment time,
           not in the hot path).

        Raises:
            TypeError: If *executor* fails either compatibility check.
        """
        if executor is not None:
            if not executor.supports(self.packet_function_type_id):
                raise TypeError(
                    f"Executor {executor.executor_type_id!r} does not support "
                    f"packet function type {self.packet_function_type_id!r}. "
                    f"Supported types: {executor.supported_function_type_ids()}"
                )
            proto = getattr(type(self), "_resolved_executor_protocol", None)
            if proto is not None and not isinstance(executor, proto):
                raise TypeError(
                    f"{type(self).__name__} requires an executor implementing "
                    f"{proto.__name__}, got {type(executor).__name__}"
                )
        self._executor = executor

    # ==================== Execution ====================

    def call(
        self,
        packet: PacketProtocol,
        *,
        logger: PacketExecutionLoggerProtocol | None = None,
    ) -> PacketProtocol | None:
        """Process a single packet.

        Base implementation calls ``direct_call``.  Subclasses that support
        executors (e.g. ``PythonPacketFunction``) override to route through
        the executor's ``execute_callable``.
        """
        if logger is not None and self._executor is None:
            import warnings

            warnings.warn(
                "A logger was passed but no executor is set — "
                "capture will not occur. Set an executor to enable logging.",
                stacklevel=2,
            )
        return self.direct_call(packet)

    async def async_call(
        self,
        packet: PacketProtocol,
        *,
        logger: PacketExecutionLoggerProtocol | None = None,
    ) -> PacketProtocol | None:
        """Asynchronously process a single packet.

        Base implementation calls ``direct_async_call``.  Subclasses that
        support executors override to route through the executor.
        """
        if logger is not None and self._executor is None:
            import warnings

            warnings.warn(
                "A logger was passed but no executor is set — "
                "capture will not occur. Set an executor to enable logging.",
                stacklevel=2,
            )
        return await self.direct_async_call(packet)

    @abstractmethod
    def direct_call(
        self, packet: PacketProtocol
    ) -> PacketProtocol | None:
        """Execute the function's native computation on *packet*.

        This is the method executors invoke.  It bypasses executor routing
        and runs the computation directly.  On user-function failure the
        exception is re-raised.  Subclasses must implement this.
        """
        ...

    @abstractmethod
    async def direct_async_call(
        self, packet: PacketProtocol
    ) -> PacketProtocol | None:
        """Asynchronous counterpart of ``direct_call``."""
        ...


class PythonPacketFunction(PacketFunctionBase[PythonFunctionExecutorProtocol]):
    @property
    def packet_function_type_id(self) -> str:
        """Unique function type identifier."""
        return "python.function.v0"

    def __init__(
        self,
        function: Callable[..., Any],
        output_keys: str | Sequence[str] | None = None,
        function_name: str | None = None,
        version: str = "v0.0",
        input_schema: SchemaLike | None = None,
        output_schema: SchemaLike | Sequence[type] | None = None,
        label: str | None = None,
        **kwargs,
    ) -> None:
        self._function = function
        self._is_async = inspect.iscoroutinefunction(function)

        # Reject functions with variadic parameters -- PythonPacketFunction maps
        # packet keys to named parameters, so the full parameter set must be fixed.
        _sig = inspect.signature(function)
        _variadic = [
            name
            for name, param in _sig.parameters.items()
            if param.kind
            in (
                inspect.Parameter.VAR_POSITIONAL,
                inspect.Parameter.VAR_KEYWORD,
            )
        ]
        if _variadic:
            raise ValueError(
                f"PythonPacketFunction does not support functions with variadic "
                f"parameters (*args / **kwargs). "
                f"Offending parameters: {_variadic!r}."
            )

        if output_keys is None:
            output_keys = []
        if isinstance(output_keys, str):
            output_keys = [output_keys]
        self._output_keys = output_keys
        if function_name is None:
            if hasattr(self._function, "__name__"):
                function_name = getattr(self._function, "__name__")
            else:
                raise ValueError(
                    "function_name must be provided if function has no __name__"
                )

        assert function_name is not None
        self._function_name = function_name

        super().__init__(label=label, version=version, **kwargs)

        # extract input and output schema from the function signature
        self._input_schema, self._output_schema = schema_utils.extract_function_schemas(
            self._function,
            self._output_keys,
            input_typespec=input_schema,
            output_typespec=output_schema,
        )

        # get git info for the function
        # TODO: turn this into optional addition
        env_info = get_git_info_for_python_object(self._function)
        if env_info is None:
            git_hash = "unknown"
        else:
            git_hash = env_info.get("git_commit_hash", "unknown")
            if env_info.get("git_repo_status") == "dirty":
                git_hash += "-dirty"
        self._git_hash = git_hash

        semantic_hasher = self.data_context.semantic_hasher
        self._function_signature_hash = semantic_hasher.hash_object(
            get_function_signature(function)
        ).to_string()
        self._function_content_hash = semantic_hasher.hash_object(
            get_function_components(self._function)
        ).to_string()
        self._output_schema_hash = semantic_hasher.hash_object(
            self.output_packet_schema
        ).to_string()

    @property
    def canonical_function_name(self) -> str:
        """Human-readable function identifier."""
        return self._function_name

    def get_function_variation_data(self) -> dict[str, Any]:
        """Raw data defining function variation - system computes hash"""
        return {
            "function_name": self._function_name,
            "function_signature_hash": self._function_signature_hash,
            "function_content_hash": self._function_content_hash,
            "git_hash": self._git_hash,
        }

    def get_execution_data(self) -> dict[str, Any]:
        """Raw data defining execution context - system computes hash"""
        python_version_info = sys.version_info
        python_version_str = f"{python_version_info.major}.{python_version_info.minor}.{python_version_info.micro}"
        return {"python_version": python_version_str, "execution_context": "local"}

    @property
    def input_packet_schema(self) -> Schema:
        """Schema describing the input packets this function accepts."""
        return self._input_schema

    @property
    def output_packet_schema(self) -> Schema:
        """Schema describing the output packets this function produces."""
        return self._output_schema

    def is_active(self) -> bool:
        """Return whether the function is active (will process packets)."""
        return self._active

    def set_active(self, active: bool = True) -> None:
        """Set the active state. If False, ``call`` returns None for every packet."""
        self._active = active

    @property
    def is_async(self) -> bool:
        """Return whether the wrapped function is an async coroutine function."""
        return self._is_async

    def _build_output_packet(self, values: Any) -> PacketProtocol:
        """Build an output Packet from raw function return values.

        Args:
            values: Raw return value from the wrapped function.

        Returns:
            A Packet containing the parsed outputs with source info.
        """
        output_data = parse_function_outputs(self._output_keys, values)

        def combine(*components: tuple[str, ...]) -> str:
            inner_parsed = [":".join(component) for component in components]
            return "::".join(inner_parsed)

        record_id = str(uuid7())
        source_info = {k: combine(self.uri, (record_id,), (k,)) for k in output_data}

        return Packet(
            output_data,
            source_info=source_info,
            record_id=record_id,
            python_schema=self.output_packet_schema,
            data_context=self.data_context,
        )

    def _call_async_function_sync(self, packet: PacketProtocol) -> Any:
        """Run the wrapped async function synchronously.

        Uses ``asyncio.run()`` when no event loop is running. When called
        from within a running loop, offloads to a new thread to avoid
        nested event loop errors.

        The coroutine is constructed inside the executor thread (not in the
        caller thread) to avoid unawaited-coroutine warnings if submission
        fails.

        Args:
            packet: The input packet whose dict form is passed to the function.

        Returns:
            The raw return value of the async function.
        """
        import asyncio

        kwargs = packet.as_dict()
        fn = self._function
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            # No running loop — safe to use asyncio.run()
            return asyncio.run(fn(**kwargs))
        else:
            # Already in a loop — run in a separate thread with its own loop.
            # The lambda ensures the coroutine is created inside the executor
            # thread, avoiding unawaited-coroutine warnings on submission failure.
            return (
                _get_sync_executor().submit(lambda: asyncio.run(fn(**kwargs))).result()
            )

    def call(
        self,
        packet: PacketProtocol,
        *,
        logger: PacketExecutionLoggerProtocol | None = None,
    ) -> PacketProtocol | None:
        """Process a single packet, routing through the executor if one is set.

        When an executor implementing ``PythonFunctionExecutorProtocol`` is
        set, the raw callable and kwargs are handed to ``execute_callable``
        which captures I/O and records to the logger.
        """
        if self._executor is not None:
            if not self._active:
                return None
            raw = self._executor.execute_callable(
                self._function, packet.as_dict(), logger=logger
            )
            return self._build_output_packet(raw)
        if logger is not None:
            import warnings

            warnings.warn(
                "A logger was passed but no executor is set — "
                "capture will not occur. Set an executor to enable logging.",
                stacklevel=2,
            )
        return self.direct_call(packet)

    async def async_call(
        self,
        packet: PacketProtocol,
        *,
        logger: PacketExecutionLoggerProtocol | None = None,
    ) -> PacketProtocol | None:
        """Async counterpart of ``call``."""
        if self._executor is not None:
            if not self._active:
                return None
            raw = await self._executor.async_execute_callable(
                self._function, packet.as_dict(), logger=logger
            )
            return self._build_output_packet(raw)
        if logger is not None:
            import warnings

            warnings.warn(
                "A logger was passed but no executor is set — "
                "capture will not occur. Set an executor to enable logging.",
                stacklevel=2,
            )
        return await self.direct_async_call(packet)

    def direct_call(
        self, packet: PacketProtocol
    ) -> PacketProtocol | None:
        """Execute the function on *packet* synchronously (no executor path).

        On user-function failure the exception is re-raised.
        For async functions, the coroutine is driven to completion via
        ``asyncio.run()`` (or a helper thread when already inside an event loop).
        """
        if not self._active:
            return None

        if self._is_async:
            raw_result = self._call_async_function_sync(packet)
        else:
            raw_result = self._function(**packet.as_dict())
        return self._build_output_packet(raw_result)

    async def direct_async_call(
        self, packet: PacketProtocol
    ) -> PacketProtocol | None:
        """Execute the function on *packet* asynchronously (no executor path).

        Async functions are ``await``-ed directly. Sync functions are
        offloaded to a thread pool via ``run_in_executor``.  On failure,
        the exception is re-raised.
        """
        import asyncio

        if not self._active:
            return None

        if self._is_async:
            raw_result = await self._function(**packet.as_dict())
        else:
            import contextvars
            import functools

            loop = asyncio.get_running_loop()
            task_ctx = contextvars.copy_context()
            raw_result = await loop.run_in_executor(
                None,
                functools.partial(
                    task_ctx.run,
                    self._function,
                    **packet.as_dict(),
                ),
            )
        return self._build_output_packet(raw_result)

    def to_config(self) -> dict[str, Any]:
        """Serialize this packet function to a JSON-compatible config dict.

        Returns:
            A dict with ``packet_function_type_id`` and a nested ``config``
            containing enough information to reconstruct this instance via
            :meth:`from_config`.
        """
        return {
            "packet_function_type_id": self.packet_function_type_id,
            "uri": list(self.uri),
            "config": {
                "module_path": self._function.__module__,
                "callable_name": self._function_name,
                "version": self._version,
                "input_packet_schema": {
                    k: str(v) for k, v in self.input_packet_schema.items()
                },
                "output_packet_schema": {
                    k: str(v) for k, v in self.output_packet_schema.items()
                },
                "output_keys": list(self._output_keys) if self._output_keys else None,
            },
        }

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> "PythonPacketFunction":
        """Reconstruct a PythonPacketFunction by importing the callable.

        Args:
            config: A dict as produced by :meth:`to_config`.

        Returns:
            A new ``PythonPacketFunction`` wrapping the imported callable.

        Raises:
            ImportError: If the module specified in *config* cannot be imported.
            AttributeError: If the callable name does not exist in the module.
        """
        import importlib

        inner = config.get("config", config)
        module = importlib.import_module(inner["module_path"])
        func = getattr(module, inner["callable_name"])
        return cls(
            function=func,
            output_keys=inner.get("output_keys"),
            version=inner.get("version", "v0.0"),
        )


class PacketFunctionWrapper(PacketFunctionBase[E]):
    """Wrapper around a PacketFunctionProtocol to modify or extend its behavior.

    Remains generic over ``E`` — the executor protocol is not bound here
    so that wrappers inherit the executor type constraint of the wrapped
    function.
    """

    def __init__(self, packet_function: PacketFunctionProtocol, **kwargs) -> None:
        self._packet_function = packet_function
        super().__init__(**kwargs)

    def computed_label(self) -> str | None:
        return self._packet_function.label

    @property
    def major_version(self) -> int:
        return self._packet_function.major_version

    @property
    def minor_version_string(self) -> str:
        return self._packet_function.minor_version_string

    @property
    def packet_function_type_id(self) -> str:
        return self._packet_function.packet_function_type_id

    @property
    def canonical_function_name(self) -> str:
        return self._packet_function.canonical_function_name

    @property
    def input_packet_schema(self) -> Schema:
        return self._packet_function.input_packet_schema

    @property
    def output_packet_schema(self) -> Schema:
        return self._packet_function.output_packet_schema

    def get_function_variation_data(self) -> dict[str, Any]:
        return self._packet_function.get_function_variation_data()

    def get_execution_data(self) -> dict[str, Any]:
        return self._packet_function.get_execution_data()

    def to_config(self) -> dict[str, Any]:
        """Delegate serialization to the wrapped packet function."""
        return self._packet_function.to_config()

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> "PacketFunctionWrapper":
        """Reconstruct by delegating to the wrapped function type.

        Args:
            config: A dict as produced by :meth:`to_config`.

        Returns:
            A new instance reconstructed from *config*.
        """
        return cls._packet_function_class_for_config(config).from_config(config)

    @staticmethod
    def _packet_function_class_for_config(config: dict[str, Any]) -> type:
        """Return the concrete class to use when reconstructing from *config*.

        Currently only ``PythonPacketFunction`` is supported. Subclasses may
        override this to handle additional types.

        Args:
            config: A config dict as produced by :meth:`to_config`.

        Returns:
            The class to call ``from_config`` on.

        Raises:
            ValueError: If the ``packet_function_type_id`` is not recognized.
        """
        type_id = config.get("packet_function_type_id")
        if type_id == "python.function.v0":
            return PythonPacketFunction
        raise ValueError(f"Unrecognized packet_function_type_id: {type_id!r}")

    # -- Executor delegation: setting/getting the executor on a wrapper
    #    transparently targets the wrapped (leaf) packet function.

    @property
    def executor(self) -> PacketFunctionExecutorProtocol | None:
        return self._packet_function.executor

    @executor.setter
    def executor(self, executor: PacketFunctionExecutorProtocol | None) -> None:
        self._packet_function.executor = executor

    # -- Execution: call/async_call delegate to the wrapped function's
    #    call/async_call which handles executor routing.  direct_call /
    #    direct_async_call bypass executor routing as their names imply.

    def call(
        self,
        packet: PacketProtocol,
        *,
        logger: PacketExecutionLoggerProtocol | None = None,
    ) -> PacketProtocol | None:
        return self._packet_function.call(packet, logger=logger)

    async def async_call(
        self,
        packet: PacketProtocol,
        *,
        logger: PacketExecutionLoggerProtocol | None = None,
    ) -> PacketProtocol | None:
        return await self._packet_function.async_call(packet, logger=logger)

    def direct_call(
        self, packet: PacketProtocol
    ) -> PacketProtocol | None:
        return self._packet_function.direct_call(packet)

    async def direct_async_call(
        self, packet: PacketProtocol
    ) -> PacketProtocol | None:
        return await self._packet_function.direct_async_call(packet)


class CachedPacketFunction(PacketFunctionWrapper):
    """Wrapper around a PacketFunctionProtocol that caches results for identical input packets.

    Uses a shared ``ResultCache`` for lookup/store/conflict-resolution
    logic (same mechanism as ``CachedFunctionPod``).
    """

    # Expose RESULT_COMPUTED_FLAG from the shared ResultCache
    RESULT_COMPUTED_FLAG = ResultCache.RESULT_COMPUTED_FLAG

    def __init__(
        self,
        packet_function: PacketFunctionProtocol,
        result_database: ArrowDatabaseProtocol,
        record_path_prefix: tuple[str, ...] = (),
        **kwargs,
    ) -> None:
        super().__init__(packet_function, **kwargs)
        self._result_database = result_database
        self._record_path_prefix = record_path_prefix
        self._cache = ResultCache(
            result_database=result_database,
            record_path=record_path_prefix + self.uri,
            auto_flush=True,
        )

    def set_auto_flush(self, on: bool = True) -> None:
        """Set auto-flush behavior. If True, the database flushes after each record."""
        self._cache.set_auto_flush(on)

    @property
    def record_path(self) -> tuple[str, ...]:
        """Return the path to the record in the result store."""
        return self._cache.record_path

    def call(
        self,
        packet: PacketProtocol,
        *,
        logger: PacketExecutionLoggerProtocol | None = None,
        skip_cache_lookup: bool = False,
        skip_cache_insert: bool = False,
    ) -> PacketProtocol | None:
        output_packet = None
        if not skip_cache_lookup:
            module_logger.info("Checking for cache...")
            output_packet = self._cache.lookup(packet)
            if output_packet is not None:
                module_logger.info(f"Cache hit for {packet}!")
                return output_packet
        output_packet = self._packet_function.call(packet, logger=logger)
        if output_packet is not None:
            if not skip_cache_insert:
                self._cache.store(
                    packet,
                    output_packet,
                    variation_data=self.get_function_variation_data(),
                    execution_data=self.get_execution_data(),
                )
            output_packet = output_packet.with_meta_columns(
                **{self.RESULT_COMPUTED_FLAG: True}
            )
        return output_packet

    async def async_call(
        self,
        packet: PacketProtocol,
        *,
        logger: PacketExecutionLoggerProtocol | None = None,
        skip_cache_lookup: bool = False,
        skip_cache_insert: bool = False,
    ) -> PacketProtocol | None:
        """Async counterpart of ``call`` with cache check and recording."""
        output_packet = None
        if not skip_cache_lookup:
            module_logger.info("Checking for cache...")
            output_packet = self._cache.lookup(packet)
            if output_packet is not None:
                module_logger.info(f"Cache hit for {packet}!")
                return output_packet
        output_packet = await self._packet_function.async_call(packet, logger=logger)
        if output_packet is not None:
            if not skip_cache_insert:
                self._cache.store(
                    packet,
                    output_packet,
                    variation_data=self.get_function_variation_data(),
                    execution_data=self.get_execution_data(),
                )
            output_packet = output_packet.with_meta_columns(
                **{self.RESULT_COMPUTED_FLAG: True}
            )
        return output_packet

    def get_cached_output_for_packet(
        self, input_packet: PacketProtocol
    ) -> PacketProtocol | None:
        """Retrieve the cached output packet for *input_packet*.

        If multiple cached entries exist, the most recent (by timestamp) wins.

        Returns:
            The cached output packet, or ``None`` if no entry was found.
        """
        return self._cache.lookup(input_packet)

    def record_packet(
        self,
        input_packet: PacketProtocol,
        output_packet: PacketProtocol,
        skip_duplicates: bool = False,
    ) -> PacketProtocol:
        """Record the output packet against the input packet in the result store."""
        self._cache.store(
            input_packet,
            output_packet,
            variation_data=self.get_function_variation_data(),
            execution_data=self.get_execution_data(),
            skip_duplicates=skip_duplicates,
        )
        return output_packet

    def get_all_cached_outputs(
        self, include_system_columns: bool = False
    ) -> "pa.Table | None":
        """Return all cached records from the result store for this function.

        Args:
            include_system_columns: If True, include system columns
                (e.g. record_id) in the result.

        Returns:
            A PyArrow table of cached results, or ``None`` if empty.
        """
        return self._cache.get_all_records(
            include_system_columns=include_system_columns
        )
