from __future__ import annotations

import asyncio
import itertools
import dataclasses
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
from orcapod.core.streams.base import (
    StreamBase,
    check_head_size,
    schema_requires_io,
)
from orcapod.core.streams.head import (
    SCHEMA_NEEDS_POLL_NOTE,
    STALE_NOTE,
    HeadStatus,
    StreamHead,
)
from orcapod.core.tracker import DEFAULT_TRACKER_MANAGER
from orcapod.errors import StoreError, TypeIdentityError
from orcapod.utils.store_errors import keep_store_error, note_callback_failure
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
from orcapod.semantic_types import infer_python_schema_from_pylist_data
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
        if (
            ctx_arg_name is not None
            and ctx_arg_name not in data_function.input_data_schema
        ):
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
        incoming_tag_schema, incoming_data_schema = input_stream.output_schema()
        self._validate_input_schema(incoming_data_schema)
        self._validate_output_keys(incoming_tag_schema.keys())

    def _validate_output_keys(self, tag_names: Collection[str]) -> None:
        """Reject an output key that is also a tag column of the input.

        Args:
            tag_names: The tag column names of the pod's (joined) input.

        Raises:
            InputValidationError: If an output key of the function is a tag name.
        """
        schema_utils.require_output_keys_not_tags(
            tag_names, self.data_function.output_data_schema.keys()
        )

    def _validate_input_schema(self, input_schema: Schema) -> None:
        expected_data_schema = self.input_data_schema
        # When expected_data_schema contains a union type (e.g. str | Path),
        # check_schema_compatibility uses beartype.door.is_subhint which accepts
        # any concrete branch: is_subhint(str, str | Path) → True,
        # is_subhint(int, str | Path) → False.
        reason = schema_utils.schema_incompatibility(input_schema, expected_data_schema)
        if reason is not None:
            # TODO: use custom exception type for better error handling
            raise ValueError(
                f"Incoming data schema {input_schema} is not compatible with "
                f"expected input schema {expected_data_schema}: {reason}"
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
            extra[self._ctx_arg_name] = self._build_invocation_context(
                tag, data, run_id=run_id
            )
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
            extra[self._ctx_arg_name] = self._build_invocation_context(
                tag, data, run_id=run_id
            )
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

        When the invocation failed with a ``StoreError`` or a ``TypeIdentityError``
        (both fail the run under every error policy), a fail-loud hook that
        raises does not replace it: its failure is logged and noted on the store
        error, the remaining hooks are skipped as for any fail-loud failure, and the
        caller re-raises the store error.

        Args:
            payload: The post-run payload to pass to each hook.
        """
        error = payload.stats.error
        store_error = (
            error if isinstance(error, (StoreError, TypeIdentityError)) else None
        )
        for hook in self._post_run_hooks:
            fn = hook.fn if isinstance(hook, HookConfig) else hook
            on_error = hook.on_error if isinstance(hook, HookConfig) else "raise"
            try:
                fn(payload)
            except Exception as exc:
                if on_error == "raise":
                    if store_error is not None:
                        note_callback_failure(store_error, f"Post-run hook {fn!r}", exc)
                        return
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
        record_id = str(output_data.datagram_uuid) if output_data is not None else None
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
            out_tag, output_data = self.process_data(
                tag, data, logger=logger, run_id=run_id
            )
        except Exception as exc:
            finished_at = datetime.now(timezone.utc)
            self._fire_post_run_hooks(
                self._build_post_run_payload(
                    tag,
                    data,
                    None,
                    started_at,
                    finished_at,
                    InvocationStatus.ERROR,
                    exc,
                    run_id=run_id,
                )
            )
            raise  # bare raise — preserves the original traceback exactly

        finished_at = datetime.now(timezone.utc)
        self._fire_post_run_hooks(
            self._build_post_run_payload(
                tag,
                data,
                output_data,
                started_at,
                finished_at,
                InvocationStatus.COMPUTED,
                None,
                run_id=run_id,
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
            return await self.async_process_data(
                tag, data, logger=logger, run_id=run_id
            )

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
                    tag,
                    data,
                    None,
                    started_at,
                    finished_at,
                    InvocationStatus.ERROR,
                    exc,
                    run_id=run_id,
                )
            )
            raise  # bare raise — preserves the original traceback exactly

        finished_at = datetime.now(timezone.utc)
        self._fire_post_run_hooks(
            self._build_post_run_payload(
                tag,
                data,
                output_data,
                started_at,
                finished_at,
                InvocationStatus.COMPUTED,
                None,
                run_id=run_id,
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
                except StoreError as exc:
                    # A failed store read or write fails the run; only rows whose
                    # function raised are skipped.
                    exc.add_context(tag=tag.as_dict())
                    with keep_store_error(exc, "Observer on_data_crash"):
                        obs.on_data_crash(pod_label, tag, data, exc)
                    raise
                except TypeIdentityError as exc:
                    # A stored row that cannot be decoded as its class is never
                    # skipped as a row failure (ITL-881).
                    with keep_store_error(exc, "Observer on_data_crash"):
                        obs.on_data_crash(pod_label, tag, data, exc)
                    raise
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
                first_row = True
                async for tag, data in inputs[0]:
                    if first_row:
                        # The sync path checks the input's schema; here the first
                        # tag is the schema. Raised outside process_one, whose
                        # handler would skip the row silently.
                        first_row = False
                        self._validate_output_keys(tag.keys())
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
        self,
        *streams: StreamProtocol,
        label: str | None = None,
        strict: bool = False,
    ) -> StreamProtocol:
        """Apply the pod, dropping the data columns its function does not take.

        This is the convenient form of ``process``. The inputs are joined as
        ``process`` joins them. When the joined stream carries data columns that
        are not parameters of the function, one ``SelectDataColumns`` keeping the
        parameter columns is applied to it, and the result is handed to
        ``process``. The select is an ordinary operator step: the pipeline is the
        one in which the caller wrote that select by hand. When nothing would be
        dropped no step is added, and the call is ``process``. Tag columns are
        never dropped.

        Args:
            *streams: Input streams to process.
            label: Optional label for tracking.
            strict: If ``True``, an extra data column is an error, as in
                ``process``. It decides only that; it is part of no identity.

        Returns:
            The resulting output stream.

        Raises:
            ValueError: If a parameter without a default has no column, a column's
                type is incompatible with its parameter, or (``strict=True``) the
                input carries a data column that is not a parameter.
        """
        logger.debug(f"Invoking pod {self} on streams through __call__: {streams}")
        # A function that accepts arbitrary keywords takes every column, so
        # nothing is unused and no select is added (developer, November 12, 2026,
        # ITL-807). No data function kind accepts them today.
        if strict or self.data_function.accepts_arbitrary_keywords:
            return self.process(*streams, label=label)

        input_stream = self.handle_input_streams(*streams)
        _, data_schema = input_stream.output_schema()
        parameters = self.input_data_schema
        dropped = [name for name in data_schema if name not in parameters]
        kept = [name for name in data_schema if name in parameters]
        # With no parameter column left there is nothing to select: a stream has
        # at least one data column. ``process`` then raises its own error.
        if dropped and kept:
            # A missing column or a type mismatch is raised before a step is added.
            self._validate_input_schema(
                Schema({name: data_schema[name] for name in kept})
            )
            from orcapod.core.operators import SelectDataColumns

            logger.info(
                "Function pod %r: dropped data columns %s, which are not "
                "parameters of the function",
                self.label,
                dropped,
            )
            input_stream = SelectDataColumns(columns=kept)(input_stream)
        return self.process(input_stream, label=label)

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
        self._validate_output_keys(tag_schema.keys())
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
        version: str = "v0.0",
        pod_config: PodConfig | None = None,
        label: str | None = None,
        **kwargs: Any,
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
            version: Version string (default ``"v0.0"``, as for every entry point).
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
        tag_schema, data_schema = input_stream.output_schema()
        self._validate_input_schema(data_schema)
        self._validate_output_keys(tag_schema.keys())
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
        self,
        *streams: StreamProtocol,
        label: str | None = None,
        strict: bool = False,
    ) -> FunctionPodStream:
        """Apply the pod, dropping the data columns its function does not take.

        See ``_FunctionPodBase.__call__``; this override only narrows the return
        type to the ``FunctionPodStream`` that ``process`` returns.

        Args:
            *streams: Input streams to process.
            label: Optional label for tracking.
            strict: If ``True``, an extra data column is an error, as in ``process``.

        Returns:
            A ``FunctionPodStream`` wrapping the computation.
        """
        return cast(
            FunctionPodStream, super().__call__(*streams, label=label, strict=strict)
        )

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

        return cls(
            data_function=data_function,
            pod_config=pod_config,
            ctx_arg_name=config.get("ctx_arg_name"),
        )


class FunctionPodStream(StreamBase):
    """Recomputable stream wrapping a data function."""

    def __init__(
        self, function_pod: FunctionPodProtocol, input_stream: StreamProtocol, **kwargs
    ) -> None:
        self._function_pod = function_pod
        self._input_stream = input_stream
        super().__init__(**kwargs)

        # Iterator acquired lazily on first use to avoid triggering upstream
        # computation during construction. Each input row carries its index, attached
        # once when the input is first read: an iteration that resumes the input after
        # a partial one keeps counting from where the input is, never from 0 (ITL-1039).
        self._cached_input_iterator: (
            Iterator[tuple[int, tuple[TagProtocol, DataProtocol]]] | None
        ) = None
        self._needs_iterator = True
        # The input raised while it was being read: its iterator is closed, so the next
        # read opens the input again and skips the rows already cached.
        self._input_interrupted = False

        # DataProtocol-level caching (for the output data)
        self._cached_output_datas: dict[
            int, tuple[TagProtocol, DataProtocol | None]
        ] = {}
        self._cached_output_table: pa.Table | None = None
        self._cached_content_hash_column: pa.Array | None = None

        # Staleness bookkeeping, in memory only (never hashed, saved or stored). The
        # stream stamps its modified time when its input is read to the end, if its
        # input did not change while it was read (ITL-1042, ITL-1052): every reset
        # bumps the generation, and the state of the input taken when the first row
        # arrived is compared at the end.
        self._reset_generation = 0
        self._input_state: tuple[object, ...] | None = None

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
        tag_schema, data_schema = self.output_schema(columns=columns, all_info=all_info)

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
        """Lazily acquire the upstream iterator on first use, or again after it raised.

        An input opened again after it raised yields its rows from the first, with the
        same indices; the rows this stream already cached are skipped. Opening it again
        is not a change of this stream's content, so its modified time stays.
        """
        if self._needs_iterator:
            self._cached_input_iterator = enumerate(self._input_stream.iter_data())
            self._needs_iterator = False
        elif self._input_interrupted:
            self._cached_input_iterator = enumerate(self._input_stream.iter_data())
            self._input_interrupted = False

    def _input_raised(self) -> None:
        """Record that the input raised while being read: it is opened again next read."""
        self._cached_input_iterator = None
        self._input_interrupted = True

    @property
    def last_modified(self) -> datetime | None:
        """When this stream's results were completed; ``None`` while they are not current.

        ``None`` (modification time unknown) while the stream is stale, that is while its
        input or its pod changed after its results were completed, or while those results
        may mix two states of its input. A stream reading this one then sees an unknown
        time and is stale as well, however far down the chain it is.
        """
        own_time = self._modified_time
        if own_time is None:
            return None
        for upstream_time in (
            self._input_stream.last_modified,
            self._function_pod.last_modified,
        ):
            if upstream_time is None or upstream_time > own_time:
                return None
        return own_time

    def _current_input_state(self) -> tuple[object, ...]:
        """What tells a change of the input: its reset generation, else its modified time.

        A pod stream's modified time moves when it finishes its own read, which happens
        while this stream reads it; only its resets are changes. Any other input
        (operator stream, source, node) is computed or fixed by the time it yields.
        """
        upstream = self._input_stream
        if isinstance(upstream, FunctionPodStream):
            return (upstream._reset_generation,)
        return (upstream.last_modified,)

    def _note_input_state(self) -> None:
        """Remember the input's state when its first row (or its end) arrives."""
        if self._input_state is None:
            self._input_state = self._current_input_state()

    def _input_read_to_end(self) -> None:
        """Stamp the completed result, or mark it stale if the input changed during the read.

        A stream stamps its modified time when its results are complete, before its
        reader sees the end, so it is newer than its input and older than its readers.
        If the input was reset while it was read, or is itself not current (no modified
        time), the result may mix two states of the input: the modified time is cleared
        instead, which makes this stream, and every stream reading it, stale.
        """
        self._note_input_state()
        changed = self._input_state != self._current_input_state()
        if changed or self._input_stream.last_modified is None:
            self._set_modified_time(None)
        else:
            self._update_modified_time()
        # The read is complete: from now on the stream is judged by its modified time.
        self._input_state = None

    @property
    def is_stale(self) -> bool:
        """Whether the stream's results must be discarded before it is read again.

        Results in progress (a read has noted its input's state and not reached the
        input's end) stay current while that state is unchanged, the input is not
        stale itself and the pod did not change: an upstream completing or computing for
        the first time while the stream is read is not a change of its input (ITL-1065).
        A pod-stream input is asked whether it is stale because its noted state, the
        reset generation, does not move when a stream further up is reset; any other
        input's modified time already reflects its upstreams. Otherwise the stream is
        stale when it, an upstream or its pod has no modified time or an upstream or its
        pod is newer (``StreamBase.is_stale``).
        """
        if self._input_state is None:
            return super().is_stale
        current = self._current_input_state()
        if current != self._input_state or None in current:
            return True
        upstream = self._input_stream
        if isinstance(upstream, FunctionPodStream) and upstream.is_stale:
            return True
        own_time = self._modified_time
        pod_time = self._function_pod.last_modified
        return own_time is None or pod_time is None or pod_time > own_time

    def clear_cache(self) -> None:
        """Discard all in-memory cached state."""
        self._reset_generation += 1
        self._input_state = None
        self._cached_input_iterator = None
        self._needs_iterator = True
        self._input_interrupted = False
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

    def _pod_base(self) -> "_FunctionPodBase":
        """The stream's pod, with its row hooks: every function pod is a ``_FunctionPodBase``."""
        pod = self._function_pod
        assert isinstance(pod, _FunctionPodBase), type(pod)
        return pod

    def _iter_data_sequential(
        self,
    ) -> Iterator[tuple[TagProtocol, DataProtocol]]:
        """Yield every row in input order, computing each uncached one as it is reached.

        Rows an earlier (possibly partial, possibly interleaved) iteration computed are
        yielded from the cache under their own input index; the next uncached index is
        always the next row of the shared input, which is computed, cached under its own
        index, and yielded.
        """
        index = 0
        while True:
            if index in self._cached_output_datas:
                tag, data = self._cached_output_datas[index]
                if data is not None:
                    yield tag, data
                index += 1
                continue
            if self._cached_input_iterator is None and self._input_interrupted:
                # Another iteration saw the input raise: open it again.
                self._ensure_iterator()
            input_iter = self._cached_input_iterator
            if input_iter is None:
                # The input was read to its end, by this or another iteration.
                return
            try:
                input_index, (tag, data) = next(input_iter)
            except StopIteration:
                # Mark completion by releasing the iterator, and stamp the result
                # before the reader sees the end.
                self._cached_input_iterator = None
                self._input_read_to_end()
                return
            except BaseException:
                # The input itself raised; its iterator is closed. Open it again on
                # the next read instead of taking it as exhausted.
                self._input_raised()
                raise
            self._note_input_state()
            if input_index in self._cached_output_datas:
                # An input opened again yields the rows this stream already cached.
                continue
            try:
                out_tag, output_data = self._pod_base()._invoke_with_hooks(tag, data)
            except BaseException:
                # The row was taken from the input but not computed: put it back, so
                # the next read computes it instead of skipping it.
                self._cached_input_iterator = itertools.chain(
                    [(input_index, (tag, data))], input_iter
                )
                raise
            tag = out_tag
            self._cached_output_datas[input_index] = (tag, output_data)
            if output_data is not None:
                yield tag, output_data
            index = input_index + 1

    def _iter_data_concurrent(
        self,
    ) -> Iterator[tuple[TagProtocol, DataProtocol]]:
        """Collect remaining inputs, execute concurrently, and yield every row in order."""
        input_iter = self._cached_input_iterator
        assert input_iter is not None

        # Materialise the rest of the input, each row with its own input index; the rows
        # before it were cached by an earlier iteration.
        try:
            to_compute: list[tuple[int, TagProtocol, DataProtocol]] = [
                (i, tag, data)
                for i, (tag, data) in input_iter
                if i not in self._cached_output_datas
            ]
        except BaseException:
            # The input itself raised; its iterator is closed. The rows read so far are
            # not computed yet, so nothing is lost: the next read opens the input again
            # and reads them anew (an upstream stream yields its cached rows).
            self._input_raised()
            raise
        # The input has delivered its rows (after its own refresh, if it had one): note
        # its state now, before computing, so a reset during the computation is seen.
        self._note_input_state()
        # Submit uncached data concurrently via async_process_data. Every row that
        # returns is cached; a row whose function raised stays in the input, so the
        # next read computes it again, and the first error is raised after the others
        # are cached.
        try:
            if to_compute:
                try:
                    loop = asyncio.get_running_loop()
                except RuntimeError:
                    loop = None

                if loop is not None:
                    # Already in event loop — fall back to sequential sync
                    for i, tag, pkt in to_compute:
                        self._cached_output_datas[i] = (
                            self._pod_base()._invoke_with_hooks(tag, pkt)
                        )
                else:

                    async def _gather() -> list[
                        tuple[TagProtocol, DataProtocol | None] | BaseException
                    ]:
                        return list(
                            await asyncio.gather(
                                *[
                                    self._pod_base()._async_invoke_with_hooks(tag, pkt)
                                    for _, tag, pkt in to_compute
                                ],
                                return_exceptions=True,
                            )
                        )

                    first_error: BaseException | None = None
                    for (i, _, _), result in zip(
                        to_compute, asyncio.run(_gather()), strict=True
                    ):
                        if isinstance(result, BaseException):
                            first_error = first_error or result
                        else:
                            self._cached_output_datas[i] = result
                    if first_error is not None:
                        raise first_error
        finally:
            remaining = [
                (i, (tag, pkt))
                for i, tag, pkt in to_compute
                if i not in self._cached_output_datas
            ]
            self._cached_input_iterator = iter(remaining) if remaining else None
        # Every row is computed: stamp the result before the reader sees any row.
        self._input_read_to_end()

        # Yield every row in input order, those an earlier iteration computed included.
        for i in range(len(self._cached_output_datas)):
            tag, data = self._cached_output_datas[i]
            if data is not None:
                yield tag, data

    def _table_from_pairs(
        self, pairs: list[tuple[TagProtocol, DataProtocol]]
    ) -> pa.Table:
        """Return the output rows ``pairs`` as one table with every column group.

        With no rows the table has the declared columns (tags, system tags and data),
        typed from the declared schema, as the rest of a head without rows is.
        """
        if not pairs:
            tag_schema, data_schema = self.output_schema(all_info=True)
            return arrow_utils.make_empty_table(
                {**tag_schema, **data_schema}, self.data_context.type_converter
            )
        tag_tables: list[pa.Table] = []
        all_data = []
        data_schema = None
        for tag, data in pairs:
            if data_schema is None:
                data_schema = data.arrow_schema(all_info=True)
            # Tags are passed through untouched: each row's own Arrow table keeps
            # the input's tag types, a logical type included (ITL-928).
            tag_tables.append(tag.as_table(columns={"system_tags": True}))
            all_data.append(data.as_dict(all_info=True))

        # TODO: re-verify the implemetation of this conversion
        converter = self.data_context.type_converter

        # The data columns have declared types: convert each value as its column's
        # type, never as the type the value happens to have. A subclass instance
        # returned for a base-class column is then refused instead of being stored
        # as the base without its own fields (ITL-972). Only the remaining columns
        # (meta, source info) are typed from their values.
        python_schema = dict(infer_python_schema_from_pylist_data(all_data))
        python_schema.update(self.output_schema()[1])
        struct_data = converter.python_dicts_to_struct_dicts(
            all_data, python_schema=python_schema
        )
        all_tags_as_tables = arrow_utils.concat_tables_by_name(tag_tables)
        all_data_as_tables: pa.Table = pa.Table.from_pylist(
            struct_data, schema=data_schema
        )
        return arrow_utils.hstack_tables(all_tags_as_tables, all_data_as_tables)

    def as_table(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> pa.Table:
        # A stale cache is cleared before any of it is used: the table and the content
        # hashes are then built from the same rows, and they are the current ones
        # (ITL-1045). ``iter_data`` clears a stale cache too, but only after the
        # cached table would already have been read.
        if self.is_stale:
            self.clear_cache()
        if self._cached_output_table is None:
            self._cached_output_table = self._table_from_pairs(list(self.iter_data()))
        assert self._cached_output_table is not None, (
            "_cached_output_table should not be None here."
        )

        column_config = ColumnConfig.handle_config(columns, all_info=all_info)

        def all_content_hashes() -> pa.Array[Any]:
            if self._cached_content_hash_column is None:
                # TODO: verify that order will be preserved
                self._cached_content_hash_column = pa.array(
                    [data.content_hash().to_string() for _, data in self.iter_data()],
                    type=pa.large_string(),
                )
            return self._cached_content_hash_column

        return self._shape_table(
            self._cached_output_table, column_config, all_content_hashes
        )

    def head(
        self,
        n: int = 10,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> StreamHead:
        """Return the first rows already computed; never calls the function.

        After a complete computation (``as_table``, or iteration to the end) the rows are
        the first of the result. After an iteration that stopped part way they are the
        rows computed so far (``PARTIAL``). Before any computation, or once an input
        changed, there are none. A ``CachedFunctionPod``'s result store is not looked
        up: that needs the input rows' hashes, i.e. reading the input. With
        ``sort_by_tags`` (or ``all_info``) the rows held are sorted by tag and the first
        ``n`` of those returned: after a complete computation that is
        ``as_table(...).slice(0, n)``; after a partial one, the rows computed so far in
        tag order ("the rows it has", not the first ``n`` of the whole result in tag
        order). See ``StreamBase.head``.
        """
        check_head_size(n)
        config = ColumnConfig.handle_config(columns, all_info=all_info)
        # Sorting needs every row held, not only the first n: read them all (in memory).
        wanted = len(self._cached_output_datas) if config.sort_by_tags else n
        has_results = bool(self._cached_output_datas) or (
            self._cached_output_table is not None
        )
        if has_results and self.is_stale:
            return self._head_without_rows(
                config, HeadStatus.NOT_COMPUTED, note=STALE_NOTE
            )
        if has_results and schema_requires_io(self, config):
            # The input gave no rows, so its columns are still known only by polling
            # it: show the result as computed, without columns.
            finished = (
                not self._needs_iterator
                and not self._input_interrupted
                and self._cached_input_iterator is None
            )
            return dataclasses.replace(
                StreamHead.without_rows(
                    self,
                    HeadStatus.ROWS if finished else HeadStatus.PARTIAL,
                    pa.table({}),
                    tag_columns=(),
                    note=SCHEMA_NEEDS_POLL_NOTE,
                ),
                total_rows=0,
                origin="computed",
            )
        tag_columns = self.keys(columns=config)[0] if has_results else ()
        if self._cached_output_table is not None:
            # A complete computation: its table holds the rows; only the shown rows'
            # data are read, and only when their content hashes are asked for.
            def shown_content_hashes() -> pa.Array[Any]:
                shown, _ = self._computed_prefix(
                    wanted, contiguous=not config.sort_by_tags
                )
                return pa.array(
                    [data.content_hash().to_string() for _, data in shown],
                    type=pa.large_string(),
                )

            return StreamHead.of_rows(
                self,
                self._shape_table(
                    self._cached_output_table
                    if config.sort_by_tags
                    else self._cached_output_table.slice(0, n),
                    config,
                    shown_content_hashes,
                ).slice(0, n),
                tag_columns=tag_columns,
                total_rows=self._cached_output_table.num_rows,
                origin="computed",
            )
        finished = (
            not self._needs_iterator
            and not self._input_interrupted
            and self._cached_input_iterator is None
        )
        if not finished and not self._cached_output_datas:
            return self._head_without_rows(config, HeadStatus.NOT_COMPUTED)
        shown, total = self._computed_prefix(wanted, contiguous=not config.sort_by_tags)
        status = HeadStatus.ROWS if finished else HeadStatus.PARTIAL
        if not shown:
            # Nothing to show (an empty result, inputs that all gave no output so far,
            # or n == 0): the declared columns.
            table = self._shape_table(
                self._table_from_pairs([]),
                config,
                lambda: pa.array([], type=pa.large_string()),
            )
            return StreamHead.of_rows(
                self,
                table,
                tag_columns=tag_columns,
                total_rows=total,
                status=status,
                origin="computed",
            )
        return StreamHead.of_rows(
            self,
            self._shape_table(
                self._table_from_pairs(shown),
                config,
                lambda: pa.array(
                    [data.content_hash().to_string() for _, data in shown],
                    type=pa.large_string(),
                ),
            ).slice(0, n),
            tag_columns=tag_columns,
            total_rows=total,
            status=status,
            origin="computed",
        )

    def _computed_prefix(
        self, limit: int, contiguous: bool = True
    ) -> tuple[list[tuple[TagProtocol, DataProtocol]], int]:
        """Return the first ``limit`` output rows computed so far, and how many there are.

        By default the rows are the contiguous run of computed inputs from the first, in
        the order iteration yields them; an input whose output is ``None`` (filtered out)
        is skipped, as iteration skips it. Only the first ``limit`` rows are collected.
        With ``contiguous=False`` the rows are every computed output, in input order,
        also those after an input that has no output (a failure): a sorted head sorts the
        rows it has.

        Args:
            limit: The largest number of rows to return.
            contiguous: Stop at the first input with no cached output.

        Returns:
            The rows, and the number of output rows counted.
        """
        shown: list[tuple[TagProtocol, DataProtocol]] = []
        total = 0
        indexes = (
            iter(range(len(self._cached_output_datas) + 1))
            if contiguous
            else iter(sorted(self._cached_output_datas))
        )
        for index in indexes:
            if index not in self._cached_output_datas:
                break
            tag, data = self._cached_output_datas[index]
            if data is not None:
                if total < limit:
                    shown.append((tag, data))
                total += 1
        return shown, total

    def _shape_table(
        self,
        table: pa.Table,
        column_config: ColumnConfig,
        content_hashes: Callable[[], pa.Array[Any]],
    ) -> pa.Table:
        """Return ``table`` with the column groups of ``column_config``.

        Args:
            table: Output rows with every column group (``_table_from_pairs``).
            column_config: The column groups to keep, and whether to sort by tags.
            content_hashes: Returns the content hash of each row of ``table``; called
                only when ``column_config`` asks for them.
        """

        def shape(carried: pa.Table) -> pa.Table:
            if column_config.sort_by_tags and self.keys()[0]:
                # Before the system tags are dropped: they break ties between equal tags.
                carried = arrow_utils.sort_by_tag_columns(carried, self.keys()[0])
            drop_columns = []
            if not column_config.system_tags:
                drop_columns.extend(
                    c
                    for c in carried.column_names
                    if c.startswith(constants.SYSTEM_TAG_PREFIX)
                )
            if not column_config.source:
                drop_columns.extend(
                    f"{constants.SOURCE_PREFIX}{c}" for c in self.keys()[1]
                )
            if not column_config.context:
                drop_columns.append(constants.CONTEXT_KEY)
            return carried.drop([c for c in drop_columns if c in carried.column_names])

        return arrow_utils.with_content_hash_column(
            table, column_config.content_hash, content_hashes, shape
        )


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
    **kwargs: Any,
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
    version: str = "v0.0",
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
        version: Version string for the data function (default ``"v0.0"``, as for every entry point).
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
