from __future__ import annotations

import asyncio
import logging
from abc import abstractmethod
from collections.abc import Callable, Collection, Iterator, Sequence
from functools import update_wrapper, wraps
from typing import TYPE_CHECKING, Any, Protocol, cast

from orcapod import contexts
from orcapod.channels import ReadableChannel, WritableChannel
from orcapod.config import OrcapodConfig
from orcapod.core.base import TraceableBase
from orcapod.core.data_function import CachedDataFunction, PythonDataFunction
from orcapod.core.streams.base import StreamBase
from orcapod.core.tracker import DEFAULT_TRACKER_MANAGER
from orcapod.protocols.core_protocols import (
    ArgumentGroup,
    FunctionPodProtocol,
    DataFunctionExecutorProtocol,
    DataFunctionProtocol,
    DataProtocol,
    PodProtocol,
    StreamProtocol,
    TagProtocol,
    TrackerManagerProtocol,
)
from orcapod.protocols.database_protocols import ArrowDatabaseProtocol
from orcapod.protocols.observability_protocols import DataExecutionLoggerProtocol
from orcapod.system_constants import constants
from orcapod.types import (
    ColumnConfig,
    PipelineConfig,
    PodConfig,
    Schema,
    resolve_concurrency,
)
from orcapod.utils import arrow_utils, schema_utils
from orcapod.utils.lazy_module import LazyModule

from datetime import datetime, timezone

from orcapod.hooks import (
    HookConfig,
    InvocationStatus,
    PodContext,
    PostRunHook,
    PostRunPayload,
    RunStats,
)

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    import polars as pl
    import pyarrow as pa
    from orcapod.protocols.observability_protocols import ExecutionObserverProtocol
else:
    pa = LazyModule("pyarrow")
    pl = LazyModule("polars")


def _executor_supports_concurrent(
    data_function: DataFunctionProtocol,
) -> bool:
    """Return True if the data function's executor supports concurrent execution."""
    executor = data_function.executor
    return executor is not None and executor.supports_concurrent_execution



class _FunctionPodBase(TraceableBase):
    """Base pod that applies a data function to each input data."""

    def __init__(
        self,
        data_function: DataFunctionProtocol,
        tracker_manager: TrackerManagerProtocol | None = None,
        label: str | None = None,
        data_context: str | contexts.DataContext | None = None,
        config: OrcapodConfig | None = None,
        ctx_arg_name: str | None = None,
    ) -> None:
        super().__init__(
            label=label,
            data_context=data_context,
            config=config,
        )
        self.tracker_manager = tracker_manager or DEFAULT_TRACKER_MANAGER
        self._data_function = data_function
        self._post_run_hooks: list[PostRunHook] = []
        # ctx-injection support: ctx_arg_name is excluded from the pod's
        # exposed input schema and auto-injected per row at process_data time.
        if ctx_arg_name is not None and ctx_arg_name not in data_function.input_data_schema:
            raise ValueError(
                f"ctx_arg_name={ctx_arg_name!r} is not a parameter of the data function "
                f"(schema keys: {list(data_function.input_data_schema.keys())})"
            )
        self._ctx_arg_name: str | None = ctx_arg_name
        # Register types for schema validation, excluding ctx (not a data column).
        self.data_context.type_converter.ensure_types_registered_for_schemas(
            self.input_data_schema,
            data_function.output_data_schema,
        )

    def computed_label(self) -> str | None:
        """Use the data function's canonical name as the default label."""
        return self._data_function.canonical_function_name

    @property
    def canonical_function_name(self) -> str:
        """Canonical function name from the underlying data function."""
        return self._data_function.canonical_function_name

    @property
    def ctx_arg_name(self) -> str | None:
        """Parameter name auto-injected with ``InvocationContext``, or ``None``."""
        return self._ctx_arg_name

    @property
    def input_data_schema(self) -> "Schema":
        """Input schema as exposed to callers of this pod.

        When ``ctx_arg_name`` is set, the corresponding parameter is excluded
        from the schema — it is auto-provided by the pod and is not a data
        column that upstream streams need to supply.
        """
        schema = self._data_function.input_data_schema
        if self._ctx_arg_name is None or self._ctx_arg_name not in schema:
            return schema
        from orcapod.types import Schema
        items = {k: v for k, v in schema.items() if k != self._ctx_arg_name}
        opt = schema.optional_fields - {self._ctx_arg_name}
        return Schema(items, optional_fields=opt)

    @property
    def data_function(self) -> DataFunctionProtocol:
        return self._data_function

    @property
    def executor(self) -> DataFunctionExecutorProtocol | None:
        """The executor set on the underlying data function, or ``None``."""
        return self._data_function.executor

    @executor.setter
    def executor(self, executor: DataFunctionExecutorProtocol | None) -> None:
        """Set or clear the executor on the underlying data function."""
        self._data_function.executor = executor

    @property
    def pod_config(self) -> PodConfig:
        """Per-pod executor configuration. Defaults to no concurrency limits."""
        return PodConfig()

    def identity_structure(self) -> Any:
        if self._ctx_arg_name is not None:
            return (self.data_function, self._ctx_arg_name)
        return self.data_function

    def pipeline_identity_structure(self) -> Any:
        return self.data_function

    def _build_invocation_context(
        self,
        tag: TagProtocol,
        data: DataProtocol,
        *,
        run_id: str | None = None,
    ) -> Any:
        """Build a per-row ``InvocationContext`` for ctx-aware pods.

        Uses the same preimage as ``SideEffectPod._execute_side_effect_row`` and
        ``FunctionJobNode._build_entry_id_preimage``: system-tag columns +
        ``INPUT_DATA_HASH_COL`` (``large_binary``) + recomputation index 0.
        ``NODE_CONTENT_HASH_COL`` is intentionally excluded — it is redundant,
        fully determined by the table path (``pipeline_hash()``) plus system tags.

        Args:
            tag: The input tag for this row.
            data: The input data for this row.
            run_id: Pipeline run identifier, or ``None`` in standalone mode.

        Returns:
            An ``InvocationContext`` instance.
        """
        import pyarrow as pa
        from orcapod.core.nodes.function_node import _build_record_id_preimage
        from orcapod.invocation import InvocationContext, InvocationHashConfig
        from orcapod.side_effects import _SIDE_EFFECT_RECOMPUTATION_INDEX_COL

        preimage = _build_record_id_preimage(tag, data).append_column(
            _SIDE_EFFECT_RECOMPUTATION_INDEX_COL,
            pa.array([0], type=pa.int32()),
        )
        record_id_hash = self.data_context.arrow_hasher.hash_table(preimage)

        return InvocationContext(
            pod_name=self.label,
            pipeline_run_id=run_id,
            _pipeline_hash_ch=self.pipeline_hash(),
            _record_id_hash_ch=record_id_hash,
            _hash_config=InvocationHashConfig(),
            _track_completion=True,
        )

    @property
    def uri(self) -> tuple[str, ...]:
        """Canonical URI, prefixed with ``"side_effect_function"`` when ctx is set."""
        base = self.data_function.uri
        if self._ctx_arg_name is not None:
            return ("side_effect_function",) + base
        return base

    def multi_stream_handler(self) -> PodProtocol:
        from orcapod.core.operators import Join

        return Join()

    def validate_inputs(self, *streams: StreamProtocol) -> None:
        """Validate input streams, raising exceptions if invalid.

        Args:
            *streams: Input streams to validate.

        Raises:
            ValueError: If inputs are incompatible with the data function schema.
        """
        input_stream = self.handle_input_streams(*streams)
        _, incoming_data_schema = input_stream.output_schema()
        self._validate_input_schema(incoming_data_schema)

    def _validate_input_schema(self, input_schema: Schema) -> None:
        expected_data_schema = self.input_data_schema
        # When expected_data_schema contains a union type (e.g. str | Path),
        # check_schema_compatibility uses beartype.door.is_subhint which accepts
        # any concrete branch: is_subhint(str, str | Path) → True,
        # is_subhint(int, str | Path) → False.
        if not schema_utils.check_schema_compatibility(
            input_schema, expected_data_schema
        ):
            # TODO: use custom exception type for better error handling
            raise ValueError(
                f"Incoming data data type {input_schema} is not compatible with expected input schema {expected_data_schema}"
            )

    def process_data(
        self,
        tag: TagProtocol,
        data: DataProtocol,
        *,
        logger: DataExecutionLoggerProtocol | None = None,
        run_id: str | None = None,
    ) -> tuple[TagProtocol, DataProtocol | None]:
        """Process a single data using the pod's data function.

        When ``ctx_arg_name`` is set, builds a per-row ``InvocationContext``
        and injects it as an extra kwarg to the original function.

        Args:
            tag: The tag associated with the data.
            data: The input data to process.
            logger: Optional ``DataExecutionLoggerProtocol`` for I/O capture.
            run_id: Pipeline run identifier forwarded to ``InvocationContext``.
                Only used when ``ctx_arg_name`` is set.

        Returns:
            A ``(tag, output_data)`` tuple; ``output_data`` is ``None`` if
            the function filters the data out.
        """
        extra: dict[str, Any] = {}
        if self._ctx_arg_name is not None:
            extra[self._ctx_arg_name] = self._build_invocation_context(tag, data, run_id=run_id)
        return tag, self.data_function.call(data, logger=logger, **extra)

    async def async_process_data(
        self,
        tag: TagProtocol,
        data: DataProtocol,
        *,
        logger: DataExecutionLoggerProtocol | None = None,
        run_id: str | None = None,
    ) -> tuple[TagProtocol, DataProtocol | None]:
        """Async counterpart of ``process_data``.

        When ``ctx_arg_name`` is set, builds a per-row ``InvocationContext``
        and injects it as an extra kwarg to the original function.

        Args:
            tag: The tag associated with the data.
            data: The input data to process.
            logger: Optional ``DataExecutionLoggerProtocol`` for I/O capture.
            run_id: Pipeline run identifier forwarded to ``InvocationContext``.
                Only used when ``ctx_arg_name`` is set.

        Returns:
            A ``(tag, output_data)`` tuple; ``output_data`` is ``None`` if
            the function filters the data out.
        """
        extra: dict[str, Any] = {}
        if self._ctx_arg_name is not None:
            extra[self._ctx_arg_name] = self._build_invocation_context(tag, data, run_id=run_id)
        return tag, await self.data_function.async_call(data, logger=logger, **extra)

    def add_post_run_hook(self, hook: PostRunHook) -> None:
        """Register a post-run hook on this pod.

        Hooks fire after every invocation (computed, cache hit, or error), in
        registration order, before the result is emitted downstream.

        A plain callable defaults to fail-loud (exceptions propagate, stopping
        the pod run). Wrap in ``HookConfig(fn=..., on_error="log")`` to log and
        continue on hook failure.

        Args:
            hook: A callable ``(PostRunPayload) -> None``, or a ``HookConfig``
                wrapping such a callable with explicit error handling.
        """
        self._post_run_hooks.append(hook)

    def _fire_post_run_hooks(self, payload: PostRunPayload) -> None:
        """Fire all registered hooks with payload in registration order.

        Args:
            payload: The post-run payload to pass to each hook.
        """
        for hook in self._post_run_hooks:
            fn = hook.fn if isinstance(hook, HookConfig) else hook
            on_error = hook.on_error if isinstance(hook, HookConfig) else "raise"
            try:
                fn(payload)
            except Exception as exc:
                if on_error == "raise":
                    raise
                logger.warning(
                    "Post-run hook %r raised and was suppressed: %s",
                    fn,
                    exc,
                    exc_info=True,
                )

    def _build_post_run_payload(
        self,
        tag: TagProtocol,
        data: DataProtocol,
        output_data: DataProtocol | None,
        started_at: datetime,
        finished_at: datetime,
        status: InvocationStatus,
        exc: Exception | None,
        run_id: str | None = None,
    ) -> PostRunPayload:
        """Build a ``PostRunPayload`` from invocation results.

        Args:
            tag: The input tag.
            data: The input data.
            output_data: The output data, or ``None`` if filtered or errored.
            started_at: UTC timestamp when the invocation started.
            finished_at: UTC timestamp when compute-or-lookup completed.
            status: Invocation status (``COMPUTED``, ``HIT``, or ``ERROR``).
            exc: The exception raised, if ``status == ERROR``; ``None`` otherwise.
            run_id: Pipeline run identifier, or ``None`` in standalone mode.
                Forwarded to ``InvocationContext.pipeline_run_id``.

        Returns:
            A ``PostRunPayload`` ready to pass to registered hooks.
        """
        record_id = (
            str(output_data.datagram_uuid) if output_data is not None else None
        )
        invocation_context = self._build_invocation_context(tag, data, run_id=run_id)
        return PostRunPayload(
            record_id_hash=record_id,
            tag=tag,
            input=data,
            output=output_data,
            stats=RunStats(
                duration_ms=(finished_at - started_at).total_seconds() * 1000,
                status=status,
                started_at=started_at,
                finished_at=finished_at,
                error=exc,
            ),
            pod=PodContext(
                label=self.label,
                pod_hash=self.content_hash().to_string(),
            ),
            invocation_context=invocation_context,
        )

    def _invoke_with_hooks(
        self,
        tag: TagProtocol,
        data: DataProtocol,
        *,
        logger: DataExecutionLoggerProtocol | None = None,
        run_id: str | None = None,
    ) -> tuple[TagProtocol, DataProtocol | None]:
        """Call ``process_data``, time it, and fire post-run hooks.

        When ``_post_run_hooks`` is empty, delegates directly to
        ``process_data`` with zero overhead. Override in subclasses (e.g.
        ``CachedFunctionPod``) to supply a different ``InvocationStatus``.

        Args:
            tag: The tag associated with the data.
            data: The input data to process.
            logger: Optional data execution logger forwarded to ``process_data``.
            run_id: Pipeline run identifier forwarded to ``process_data`` and
                ``PostRunPayload.invocation_context``.

        Returns:
            A ``(tag, output_data)`` tuple.
        """
        if not self._post_run_hooks:
            return self.process_data(tag, data, logger=logger, run_id=run_id)

        started_at = datetime.now(timezone.utc)
        out_tag = tag
        output_data: DataProtocol | None = None

        try:
            out_tag, output_data = self.process_data(tag, data, logger=logger, run_id=run_id)
        except Exception as exc:
            finished_at = datetime.now(timezone.utc)
            self._fire_post_run_hooks(
                self._build_post_run_payload(
                    tag, data, None, started_at, finished_at,
                    InvocationStatus.ERROR, exc, run_id=run_id,
                )
            )
            raise  # bare raise — preserves the original traceback exactly

        finished_at = datetime.now(timezone.utc)
        self._fire_post_run_hooks(
            self._build_post_run_payload(
                tag, data, output_data, started_at, finished_at,
                InvocationStatus.COMPUTED, None, run_id=run_id,
            )
        )
        return out_tag, output_data

    async def _async_invoke_with_hooks(
        self,
        tag: TagProtocol,
        data: DataProtocol,
        *,
        logger: DataExecutionLoggerProtocol | None = None,
        run_id: str | None = None,
    ) -> tuple[TagProtocol, DataProtocol | None]:
        """Async counterpart of ``_invoke_with_hooks``.

        When ``_post_run_hooks`` is empty, delegates directly to
        ``async_process_data`` with zero overhead.

        Args:
            tag: The tag associated with the data.
            data: The input data to process.
            logger: Optional data execution logger forwarded to
                ``async_process_data``.
            run_id: Pipeline run identifier forwarded to ``async_process_data``
                and ``PostRunPayload.invocation_context``.

        Returns:
            A ``(tag, output_data)`` tuple.
        """
        if not self._post_run_hooks:
            return await self.async_process_data(tag, data, logger=logger, run_id=run_id)

        started_at = datetime.now(timezone.utc)
        out_tag = tag
        output_data: DataProtocol | None = None

        try:
            out_tag, output_data = await self.async_process_data(
                tag, data, logger=logger, run_id=run_id
            )
        except Exception as exc:
            finished_at = datetime.now(timezone.utc)
            self._fire_post_run_hooks(
                self._build_post_run_payload(
                    tag, data, None, started_at, finished_at,
                    InvocationStatus.ERROR, exc, run_id=run_id,
                )
            )
            raise  # bare raise — preserves the original traceback exactly

        finished_at = datetime.now(timezone.utc)
        self._fire_post_run_hooks(
            self._build_post_run_payload(
                tag, data, output_data, started_at, finished_at,
                InvocationStatus.COMPUTED, None, run_id=run_id,
            )
        )
        return out_tag, output_data

    def handle_input_streams(self, *streams: StreamProtocol) -> StreamProtocol:
        """Handle multiple input streams by joining them if necessary.

        Args:
            *streams: Input streams to handle.
        """
        # handle multiple input streams
        if len(streams) == 0:
            raise ValueError("At least one input stream is required")
        elif len(streams) > 1:
            # TODO: simplify the multi-stream handling logic
            multi_stream_handler = self.multi_stream_handler()
            joined_stream = multi_stream_handler.process(*streams)
            return joined_stream
        return streams[0]

    async def async_execute(
        self,
        inputs: Sequence[ReadableChannel[tuple[TagProtocol, DataProtocol]]],
        output: WritableChannel[tuple[TagProtocol, DataProtocol]],
        pipeline_config: PipelineConfig | None = None,
        *,
        observer: ExecutionObserverProtocol | None = None,
        run_id: str | None = None,
    ) -> None:
        """Streaming async execution with per-data concurrency control.

        Each input (tag, data) is dispatched as an independent async task.
        A semaphore limits how many tasks are in-flight concurrently.
        Observer hooks fire per item: ``on_data_start`` before processing,
        ``on_data_end(cached=False)`` on success, ``on_data_crash`` on error.

        Args:
            inputs: Single-element sequence containing the input channel.
            output: Writable channel for output (tag, data) pairs.
            pipeline_config: Optional pipeline-level concurrency config.
            observer: Optional observer for per-item lifecycle hooks.
            run_id: Pipeline run identifier forwarded to per-row processing.
        """
        from orcapod.pipeline.observer import NoOpObserver

        try:
            pipeline_config = pipeline_config or PipelineConfig()
            max_concurrency = resolve_concurrency(self.pod_config, pipeline_config)
            obs = observer if observer is not None else NoOpObserver()
            pod_label = self.label

            sem = (
                asyncio.Semaphore(max_concurrency)
                if max_concurrency is not None
                else None
            )

            async def process_one(tag: TagProtocol, data: DataProtocol) -> None:
                obs.on_data_start(pod_label, tag, data)
                pkt_logger = obs.create_data_logger(tag, data)
                try:
                    out_tag, result_data = await self._async_invoke_with_hooks(
                        tag, data, logger=pkt_logger, run_id=run_id
                    )
                except Exception as exc:
                    logger.debug(
                        "Data processing failed, skipping: %s", exc, exc_info=True
                    )
                    obs.on_data_crash(pod_label, tag, data, exc)
                else:
                    obs.on_data_end(pod_label, tag, data, result_data, cached=False)
                    if result_data is not None:
                        await output.send((out_tag, result_data))
                finally:
                    if sem is not None:
                        sem.release()

            async with asyncio.TaskGroup() as tg:
                async for tag, data in inputs[0]:
                    if sem is not None:
                        await sem.acquire()
                    tg.create_task(process_one(tag, data))
        finally:
            await output.close()

    @abstractmethod
    def process(
        self, *streams: StreamProtocol, label: str | None = None
    ) -> StreamProtocol:
        """Invoke the data processor on the input stream(s).

        If multiple streams are passed in, they are joined before processing.

        Args:
            *streams: Input streams to process.
            label: Optional label for tracking.

        Returns:
            The resulting output stream.
        """
        ...

    def __call__(
        self, *streams: StreamProtocol, label: str | None = None
    ) -> StreamProtocol:
        """Convenience alias for ``process``."""
        logger.debug(f"Invoking pod {self} on streams through __call__: {streams}")
        # perform input stream validation
        return self.process(*streams, label=label)

    def argument_symmetry(self, streams: Collection[StreamProtocol]) -> ArgumentGroup:
        return self.multi_stream_handler().argument_symmetry(streams)

    def output_schema(
        self,
        *streams: StreamProtocol,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        tag_schema, incoming_data_schema = self.multi_stream_handler().output_schema(
            *streams, columns=columns, all_info=all_info
        )
        # validate that incoming_data_schema is valid
        self._validate_input_schema(incoming_data_schema)
        # The output schema of the FunctionPodProtocol is determined by the data function
        # TODO: handle and extend to include additional columns
        # Namely, the source columns
        return tag_schema, self.data_function.output_data_schema


class FunctionPod(_FunctionPodBase):
    def __init__(
        self,
        data_function: DataFunctionProtocol,
        pod_config: PodConfig | None = None,
        ctx_arg_name: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(
            data_function,
            ctx_arg_name=ctx_arg_name,
            **kwargs,
        )
        self._pod_config = pod_config or PodConfig()

    @classmethod
    def from_fn(
        cls,
        fn: Callable,
        output_keys: list[str] | str,
        *,
        ctx_arg_name: str | None = None,
        name: str | None = None,
        version: str = "v1.0",
        pod_config: PodConfig | None = None,
        label: str | None = None,
        **kwargs,
    ) -> "FunctionPod":
        """Construct a ``FunctionPod`` directly from a callable.

        When ``ctx_arg_name`` is set, the named parameter is excluded from the
        pod's exposed ``input_data_schema`` and auto-injected with a per-row
        ``InvocationContext`` at ``process_data`` time.  The full function
        signature (including ``ctx_arg_name``) is retained in the underlying
        ``PythonDataFunction`` for correct identity hashing.

        Args:
            fn: The user function.
            output_keys: Output column key(s).
            ctx_arg_name: If set, exclude this parameter from the exposed input
                schema and inject an ``InvocationContext`` per row under this name.
            name: Optional canonical function name override.
            version: Version string (default ``"v1.0"``).
            pod_config: Optional per-pod config.
            label: Optional display label.
            **kwargs: Forwarded to ``_FunctionPodBase.__init__``.

        Returns:
            A new ``FunctionPod``.
        """
        data_function = PythonDataFunction(
            fn,
            output_keys=output_keys,
            function_name=name or getattr(fn, "__name__", "unknown"),
            version=version,
            label=label,
        )
        return cls(
            data_function=data_function,
            pod_config=pod_config,
            ctx_arg_name=ctx_arg_name,
            label=label,
            **kwargs,
        )

    @property
    def pod_config(self) -> PodConfig:
        """Per-pod executor configuration."""
        return self._pod_config

    def process(
        self, *streams: StreamProtocol, label: str | None = None
    ) -> FunctionPodStream:
        """Invoke the data processor on the input stream(s).

        Args:
            *streams: Input streams to process.
            label: Optional label for tracking.

        Returns:
            A ``FunctionPodStream`` wrapping the computation.
        """
        logger.debug(f"Invoking kernel {self} on streams: {streams}")

        input_stream = self.handle_input_streams(*streams)

        # perform input stream schema validation
        self._validate_input_schema(input_stream.output_schema()[1])
        self.tracker_manager.record_function_pod_invocation(
            self, input_stream, label=label
        )
        output_stream = FunctionPodStream(
            function_pod=self,
            input_stream=input_stream,
            label=label,
        )
        return output_stream

    def __call__(
        self, *streams: StreamProtocol, label: str | None = None
    ) -> FunctionPodStream:
        """Convenience alias for ``process``."""
        logger.debug(f"Invoking pod {self} on streams through __call__: {streams}")
        # perform input stream validation
        return self.process(*streams, label=label)

    def to_config(self) -> dict[str, Any]:
        """Serialize this function pod to a JSON-compatible config dict.

        Returns:
            A JSON-serializable dict containing the URI, data function config,
            and pod config for this function pod.
        """
        config: dict[str, Any] = {
            "uri": list(self.uri),
            "data_function": self.data_function.to_config(),
            "pod_config": None,
            "ctx_arg_name": self.ctx_arg_name,
        }
        if self._pod_config.max_concurrency is not None:
            config["pod_config"] = {
                "max_concurrency": self._pod_config.max_concurrency,
            }
        return config

    @classmethod
    def from_config(
        cls,
        config: dict[str, Any],
        *,
        fallback_to_proxy: bool = False,
    ) -> "FunctionPod":
        """Reconstruct a ``FunctionPod`` from a config dict.

        Args:
            config: A dict as produced by ``to_config``.
            fallback_to_proxy: If ``True`` and the data function cannot be
                resolved, use a ``DataFunctionProxy`` instead of raising.

        Returns:
            A new ``FunctionPod`` instance.

        Note:
            Ctx-aware pods (where ``ctx_arg_name`` is set in config) are reconstructed
            from the serialized data function config. If the underlying function cannot
            be resolved, a ``DataFunctionProxy`` is used and calling ``process_data``
            will raise ``RuntimeError``. These stubs can only serve cached results via
            ``FunctionJobNode``.
        """
        from orcapod.pipeline.serialization import resolve_data_function_from_config

        pf_config = config["data_function"]
        data_function = resolve_data_function_from_config(
            pf_config, fallback_to_proxy=fallback_to_proxy
        )

        pod_config = None
        if config.get("pod_config") is not None:
            pod_config = PodConfig(**config["pod_config"])

        return cls(data_function=data_function, pod_config=pod_config, ctx_arg_name=config.get("ctx_arg_name"))


class FunctionPodStream(StreamBase):
    """Recomputable stream wrapping a data function."""

    def __init__(
        self, function_pod: FunctionPodProtocol, input_stream: StreamProtocol, **kwargs
    ) -> None:
        self._function_pod = function_pod
        self._input_stream = input_stream
        super().__init__(**kwargs)

        # Iterator acquired lazily on first use to avoid triggering upstream
        # computation during construction.
        self._cached_input_iterator: (
            Iterator[tuple[TagProtocol, DataProtocol]] | None
        ) = None
        self._needs_iterator = True

        # DataProtocol-level caching (for the output data)
        self._cached_output_datas: dict[
            int, tuple[TagProtocol, DataProtocol | None]
        ] = {}
        self._cached_output_table: pa.Table | None = None
        self._cached_content_hash_column: pa.Array | None = None

    @property
    def producer(self) -> PodProtocol:
        return self._function_pod

    @property
    def executor(self) -> DataFunctionExecutorProtocol | None:
        """The executor set on the underlying data function."""
        return self._function_pod.data_function.executor

    @executor.setter
    def executor(self, executor: DataFunctionExecutorProtocol | None) -> None:
        """Set or clear the executor on the underlying data function."""
        self._function_pod.data_function.executor = executor

    @property
    def upstreams(self) -> tuple[StreamProtocol, ...]:
        return (self._input_stream,)

    def identity_structure(self) -> Any:
        return (
            self._function_pod,
            self._function_pod.argument_symmetry((self._input_stream,)),
        )

    def pipeline_identity_structure(self) -> Any:
        return self.identity_structure()

    def keys(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        tag_schema, data_schema = self.output_schema(
            columns=columns, all_info=all_info
        )

        return tuple(tag_schema.keys()), tuple(data_schema.keys())

    def output_schema(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        return self._function_pod.output_schema(
            self._input_stream, columns=columns, all_info=all_info
        )

    def _ensure_iterator(self) -> None:
        """Lazily acquire the upstream iterator on first use."""
        if self._needs_iterator:
            self._cached_input_iterator = self._input_stream.iter_data()
            self._needs_iterator = False
            self._update_modified_time()

    def clear_cache(self) -> None:
        """Discard all in-memory cached state."""
        self._cached_input_iterator = None
        self._needs_iterator = True
        self._cached_output_datas.clear()
        self._cached_output_table = None
        self._cached_content_hash_column = None
        self._update_modified_time()

    def __iter__(self) -> Iterator[tuple[TagProtocol, DataProtocol]]:
        return self.iter_data()

    def iter_data(self) -> Iterator[tuple[TagProtocol, DataProtocol]]:
        if self.is_stale:
            self.clear_cache()
        self._ensure_iterator()
        if self._cached_input_iterator is not None:
            if _executor_supports_concurrent(self._function_pod.data_function):
                yield from self._iter_data_concurrent()
            else:
                yield from self._iter_data_sequential()
        else:
            # Yield from snapshot of complete cache
            for i in range(len(self._cached_output_datas)):
                tag, data = self._cached_output_datas[i]
                if data is not None:
                    yield tag, data

    def _iter_data_sequential(
        self,
    ) -> Iterator[tuple[TagProtocol, DataProtocol]]:
        input_iter = self._cached_input_iterator
        assert input_iter is not None
        for i, (tag, data) in enumerate(input_iter):
            if i in self._cached_output_datas:
                # Use cached result
                tag, data = self._cached_output_datas[i]
                if data is not None:
                    yield tag, data
            else:
                # Process data
                tag, output_data = self._function_pod._invoke_with_hooks(tag, data)
                self._cached_output_datas[i] = (tag, output_data)
                if output_data is not None:
                    yield tag, output_data

        # Mark completion by releasing the iterator
        self._cached_input_iterator = None

    def _iter_data_concurrent(
        self,
    ) -> Iterator[tuple[TagProtocol, DataProtocol]]:
        """Collect remaining inputs, execute concurrently, and yield results in order."""
        input_iter = self._cached_input_iterator
        assert input_iter is not None

        # Materialise remaining inputs and separate cached from uncached.
        all_inputs: list[tuple[int, TagProtocol, DataProtocol]] = []
        to_compute: list[tuple[int, TagProtocol, DataProtocol]] = []
        for i, (tag, data) in enumerate(input_iter):
            all_inputs.append((i, tag, data))
            if i not in self._cached_output_datas:
                to_compute.append((i, tag, data))
        self._cached_input_iterator = None

        # Submit uncached data concurrently via async_process_data.
        if to_compute:
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = None

            if loop is not None:
                # Already in event loop — fall back to sequential sync
                results = [
                    self._function_pod._invoke_with_hooks(tag, pkt)
                    for _, tag, pkt in to_compute
                ]
            else:

                async def _gather() -> list[tuple[TagProtocol, DataProtocol | None]]:
                    return list(
                        await asyncio.gather(
                            *[
                                self._function_pod._async_invoke_with_hooks(tag, pkt)
                                for _, tag, pkt in to_compute
                            ]
                        )
                    )

                results = asyncio.run(_gather())

            for (i, _, _), (tag, output_data) in zip(to_compute, results):
                self._cached_output_datas[i] = (tag, output_data)

        # Yield everything in original order.
        for i, *_ in all_inputs:
            tag, data = self._cached_output_datas[i]
            if data is not None:
                yield tag, data

    def as_table(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> pa.Table:
        if self._cached_output_table is None:
            all_tags = []
            all_data = []
            tag_schema, data_schema = None, None
            for tag, data in self.iter_data():
                if tag_schema is None:
                    tag_schema = tag.arrow_schema(all_info=True)
                if data_schema is None:
                    data_schema = data.arrow_schema(all_info=True)
                # TODO: make use of arrow_compat dict
                all_tags.append(tag.as_dict(all_info=True))
                all_data.append(data.as_dict(all_info=True))

            # TODO: re-verify the implemetation of this conversion
            converter = self.data_context.type_converter

            struct_data = converter.python_dicts_to_struct_dicts(all_data)
            all_tags_as_tables: pa.Table = pa.Table.from_pylist(
                all_tags, schema=tag_schema
            )
            # drop context key column from tags table (guard: column absent on empty stream)
            if constants.CONTEXT_KEY in all_tags_as_tables.column_names:
                all_tags_as_tables = all_tags_as_tables.drop([constants.CONTEXT_KEY])
            all_data_as_tables: pa.Table = pa.Table.from_pylist(
                struct_data, schema=data_schema
            )

            self._cached_output_table = arrow_utils.hstack_tables(
                all_tags_as_tables, all_data_as_tables
            )
        assert self._cached_output_table is not None, (
            "_cached_output_table should not be None here."
        )

        column_config = ColumnConfig.handle_config(columns, all_info=all_info)

        drop_columns = []
        if not column_config.system_tags:
            # TODO: get system tags more effiicently
            drop_columns.extend(
                [
                    c
                    for c in self._cached_output_table.column_names
                    if c.startswith(constants.SYSTEM_TAG_PREFIX)
                ]
            )
        if not column_config.source:
            drop_columns.extend(f"{constants.SOURCE_PREFIX}{c}" for c in self.keys()[1])
        if not column_config.context:
            drop_columns.append(constants.CONTEXT_KEY)

        output_table = self._cached_output_table.drop(
            [c for c in drop_columns if c in self._cached_output_table.column_names]
        )

        # lazily prepare content hash column if requested
        if column_config.content_hash:
            if self._cached_content_hash_column is None:
                content_hashes = []
                # TODO: verify that order will be preserved
                for tag, data in self.iter_data():
                    content_hashes.append(data.content_hash().to_string())
                self._cached_content_hash_column = pa.array(
                    content_hashes, type=pa.large_string()
                )
            assert self._cached_content_hash_column is not None, (
                "_cached_content_hash_column should not be None here."
            )
            hash_column_name = (
                "_content_hash"
                if column_config.content_hash is True
                else column_config.content_hash
            )
            output_table = output_table.append_column(
                hash_column_name, self._cached_content_hash_column
            )

        if column_config.sort_by_tags:
            # TODO: reimplement using polars natively
            output_table_schema = output_table.schema
            output_table = (
                pl.DataFrame(output_table)
                .sort(by=self.keys()[0], descending=False)
                .to_arrow()
            )
            output_table = arrow_utils.restore_schema_nullability(output_table, output_table_schema)
            # output_table = output_table.sort_by(
            #     [(column, "ascending") for column in self.keys()[0]]
            # )
        return output_table


class CallableWithPodProtocol(Protocol):
    @property
    def pod(self) -> _FunctionPodBase:
        """Return the associated function pod."""
        ...

    def __call__(self, *args, **kwargs):
        """Call the underlying function."""
        ...


def function_pod(
    output_keys: str | Sequence[str] | None = None,
    function_name: str | None = None,
    version: str = "v0.0",
    label: str | None = None,
    ctx_arg: str | None = None,
    result_database: ArrowDatabaseProtocol | None = None,
    pod_cache_database: ArrowDatabaseProtocol | None = None,
    executor: DataFunctionExecutorProtocol | None = None,
    post_run_hooks: Sequence[PostRunHook] | None = None,
    **kwargs,
) -> Callable[..., CallableWithPodProtocol]:
    """Decorator that attaches a ``FunctionPod`` as a ``pod`` attribute.

    Args:
        output_keys: Keys for the function output(s).
        function_name: Name of the function pod; defaults to ``func.__name__``.
        version: Version string for the data function.
        label: Optional label for tracking.
        result_database: Optional database for data-level caching
            (wraps the data function in ``CachedDataFunction``).
        pod_cache_database: Optional database for pod-level caching
            (wraps the pod in ``CachedFunctionPod``, which caches at the
            ``process_data`` level using input data content hash).
        executor: Optional executor for running the data function.
        post_run_hooks: Optional list of post-run hooks to register on the
            pod after construction. Each entry is either a plain callable
            ``(PostRunPayload) -> None`` or a ``HookConfig``.
        **kwargs: Forwarded to ``PythonDataFunction``.

    Returns:
        A decorator that adds a ``pod`` attribute to the wrapped function.
    """

    def decorator(func: Callable) -> CallableWithPodProtocol:
        if func.__name__ == "<lambda>":
            raise ValueError("Lambda functions cannot be used with function_pod")

        data_function = PythonDataFunction(
            func,
            output_keys=output_keys,
            function_name=function_name or func.__name__,
            version=version,
            label=label,
            executor=executor,
            **kwargs,
        )

        # if database is provided, wrap in CachedDataFunction
        if result_database is not None:
            data_function = CachedDataFunction(
                data_function,
                result_database=result_database,
            )

        # Create a simple typed function pod
        pod: _FunctionPodBase = FunctionPod(
            data_function=data_function,
            ctx_arg_name=ctx_arg,
        )

        # if pod_cache_database is provided, wrap in CachedFunctionPod
        if pod_cache_database is not None:
            from orcapod.core.cached_function_pod import CachedFunctionPod

            pod = CachedFunctionPod(
                function_pod=pod,
                result_database=pod_cache_database,
            )

        if post_run_hooks:
            for hook in post_run_hooks:
                pod.add_post_run_hook(hook)

        @wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        setattr(wrapper, "pod", pod)
        return cast(CallableWithPodProtocol, wrapper)

    return decorator


class WrappedFunctionPod(_FunctionPodBase):
    """Wrapper for a function pod, delegating call logic to the inner pod."""

    def __init__(
        self,
        function_pod: FunctionPodProtocol,
        data_context: str | contexts.DataContext | None = None,
        **kwargs,
    ) -> None:
        # if data_context is not explicitly given, use that of the contained pod
        if data_context is None:
            data_context = function_pod.data_context_key
        super().__init__(
            data_function=function_pod.data_function,
            data_context=data_context,
            # Propagate ctx_arg_name so input_data_schema filters ctx correctly.
            ctx_arg_name=function_pod.ctx_arg_name,
            **kwargs,
        )
        self._function_pod = function_pod

    def computed_label(self) -> str | None:
        return self._function_pod.label

    @property
    def pod_config(self) -> PodConfig:
        """Delegate to the inner pod's config so CachedFunctionPod respects limits."""
        return self._function_pod.pod_config

    @property
    def uri(self) -> tuple[str, ...]:
        return self._function_pod.uri

    def validate_inputs(self, *streams: StreamProtocol) -> None:
        self._function_pod.validate_inputs(*streams)

    def argument_symmetry(self, streams: Collection[StreamProtocol]) -> ArgumentGroup:
        return self._function_pod.argument_symmetry(streams)

    def output_schema(
        self,
        *streams: StreamProtocol,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        return self._function_pod.output_schema(
            *streams, columns=columns, all_info=all_info
        )

    # TODO: reconsider whether to return FunctionPodStream here in the signature
    def process(
        self, *streams: StreamProtocol, label: str | None = None
    ) -> StreamProtocol:
        return self._function_pod.process(*streams, label=label)


def side_effect_function_pod(
    fn: Callable | None = None,
    *,
    output_keys: list[str] | str,
    ctx_arg_name: str = "ctx",
    name: str | None = None,
    version: str = "v1.0",
    pod_config: PodConfig | None = None,
) -> "FunctionPod | Callable":
    """Decorator wrapping a callable as a ctx-aware ``FunctionPod``.

    Note:
        This decorator is superseded by ``@function_pod(ctx_arg=<arg_name>)``,
        which is now the preferred way to author side-effect pods. The two
        forms are equivalent in computational behaviour but differ in the type
        of the decorated object: ``@function_pod(...)`` returns a plain callable
        with a ``.pod`` attribute, whereas this decorator returns the
        ``FunctionPod`` directly.

        Preferred form (use this instead)::

            @function_pod(output_keys=["result"], ctx_arg="ctx")
            def my_fn(value: int, ctx: InvocationContext) -> str:
                ...

            assert callable(my_fn)          # still a plain callable
            assert isinstance(my_fn.pod, FunctionPod)

        Legacy form (this decorator)::

            @side_effect_function_pod(output_keys=["result"])
            def my_fn(value: int, ctx: InvocationContext) -> str:
                ...

            assert isinstance(my_fn, FunctionPod)  # decorated object IS the pod

        Full removal of this decorator is tracked separately.

    Equivalent to ``FunctionPod.from_fn(fn, output_keys=..., ctx_arg_name=...)``.
    The decorated object is the ``FunctionPod`` itself (not a wrapper function),
    so it can be called directly as a pod.

    Args:
        fn: Optional function — if provided, decorates immediately.
        output_keys: Output column key(s).
        ctx_arg_name: Name of the ``InvocationContext`` parameter (default ``"ctx"``).
        name: Optional canonical function name override.
        version: Version string for the data function (default ``"v1.0"``).
        pod_config: Optional per-pod configuration.

    Returns:
        A ``FunctionPod`` with ``ctx_arg_name`` set, or a decorator if ``fn``
        is not provided.

    Raises:
        ValueError: If ``ctx_arg_name`` is not in ``fn``'s signature.
    """
    def decorator(func: Callable) -> FunctionPod:
        pod = FunctionPod.from_fn(
            func,
            output_keys=output_keys,
            ctx_arg_name=ctx_arg_name,
            name=name,
            version=version,
            pod_config=pod_config,
        )
        update_wrapper(pod, func)
        return pod

    if fn is not None:
        return decorator(fn)
    return decorator
