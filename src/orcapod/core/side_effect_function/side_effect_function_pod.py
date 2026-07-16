"""SideEffectFunctionPod — hybrid of FunctionPod and SideEffectPod."""
from __future__ import annotations

import asyncio
import functools
import inspect
import logging
import sys
import uuid
from collections.abc import Callable, Collection, Iterator, Sequence
from typing import TYPE_CHECKING, Any

from uuid_utils import uuid7

from orcapod.core.base import TraceableBase
from orcapod.core.streams.base import StreamBase
from orcapod.core.tracker import DEFAULT_TRACKER_MANAGER
from orcapod.errors import PipelineJobRequiredError
from orcapod.side_effects import (
    InvocationContext,
    SideEffectPodConfig,
    _SIDE_EFFECT_RECOMPUTATION_INDEX_COL,
    _write_invocation_row,
)
from orcapod.utils.lazy_module import LazyModule
from orcapod.utils.schema_utils import extract_function_schemas

if TYPE_CHECKING:
    import pyarrow as pa
    from orcapod.channels import ReadableChannel, WritableChannel
    from orcapod.protocols.core_protocols import (
        DataProtocol,
        StreamProtocol,
        TagProtocol,
        TrackerManagerProtocol,
    )
    from orcapod.protocols.database_protocols import ArrowDatabaseProtocol
    from orcapod.types import ColumnConfig, ContentHash, Schema
else:
    pa = LazyModule("pyarrow")

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# _strip_ctx_from_fn — remove ctx param from signature for schema inference
# ---------------------------------------------------------------------------


def _strip_ctx_from_fn(fn: Callable, ctx_arg_name: str) -> Callable:
    """Return a wrapper of ``fn`` with ``ctx_arg_name`` removed from signature.

    The wrapper is passed to ``extract_function_schemas`` so the context
    parameter is transparent to schema inference. The original ``fn`` is used
    for actual calls and content hashing.

    Args:
        fn: The original user function.
        ctx_arg_name: Name of the parameter receiving ``InvocationContext``.

    Returns:
        A wrapper whose ``__signature__`` and ``__annotations__`` exclude
        ``ctx_arg_name``.

    Raises:
        ValueError: If ``ctx_arg_name`` is not in ``fn``'s signature.
    """
    sig = inspect.signature(fn)
    if ctx_arg_name not in sig.parameters:
        raise ValueError(
            f"ctx_arg_name {ctx_arg_name!r} not found in function signature "
            f"{fn.__name__!r}. Available parameters: {list(sig.parameters)}"
        )
    new_params = [p for n, p in sig.parameters.items() if n != ctx_arg_name]
    new_sig = sig.replace(parameters=new_params)

    @functools.wraps(fn)
    def wrapper(*args, **kwargs):  # pragma: no cover
        return fn(*args, **kwargs)

    wrapper.__signature__ = new_sig  # type: ignore[attr-defined]
    wrapper.__annotations__ = {
        k: v for k, v in fn.__annotations__.items() if k != ctx_arg_name
    }
    return wrapper


# ---------------------------------------------------------------------------
# _build_ctx_and_record_id — shared preimage helper
# ---------------------------------------------------------------------------


def _build_ctx_and_record_id(
    *,
    pod: "SideEffectFunctionPod",
    tag: "TagProtocol",
    data: "DataProtocol",
    pipeline_hash_ch: "ContentHash",
    run_id: str | None,
) -> "tuple[InvocationContext, ContentHash, bytes]":
    """Build InvocationContext + record ID for one (tag, data) row.

    Uses the same preimage as ``_execute_side_effect_row`` in
    ``side_effects.py``: system-tag columns + INPUT_DATA_HASH_COL +
    NODE_CONTENT_HASH_COL + recomputation index 0.

    Args:
        pod: The invoking pod.
        tag: Tag for this row.
        data: Data for this row.
        pipeline_hash_ch: Pipeline hash of the node.
        run_id: Pipeline run identifier, or ``None`` in standalone mode.

    Returns:
        ``(ctx, record_id_hash, record_id)`` where ``record_id`` is the
        prefixed digest of ``record_id_hash``.
    """
    from orcapod.system_constants import constants

    preimage = (
        tag.as_table(columns={"system_tags": True})
        .append_column(
            constants.INPUT_DATA_HASH_COL,
            pa.array([data.content_hash().to_string()], type=pa.large_string()),
        )
        .append_column(
            constants.NODE_CONTENT_HASH_COL,
            pa.array([pod.content_hash().to_string()], type=pa.large_string()),
        )
        .append_column(
            _SIDE_EFFECT_RECOMPUTATION_INDEX_COL,
            pa.array([0], type=pa.int32()),
        )
    )
    record_id_hash: ContentHash = pod.data_context.arrow_hasher.hash_table(preimage)
    record_id: bytes = record_id_hash.to_prefixed_digest()

    ctx = InvocationContext(
        pod_name=pod.label,
        pipeline_run_id=run_id,
        _pipeline_hash_ch=pipeline_hash_ch,
        _record_id_hash_ch=record_id_hash,
        _hash_config=pod.pod_config.hash_config,
        _track_completion=pod.pod_config.track_completion,
    )
    return ctx, record_id_hash, record_id


def _build_invocation_context(
    *,
    pod: "SideEffectFunctionPod",
    tag: "TagProtocol",
    data: "DataProtocol",
    pipeline_hash_ch: "ContentHash",
    run_id: str | None,
) -> InvocationContext:
    """Convenience wrapper — returns only the ``InvocationContext``.

    Used by ``SideEffectFunctionPodStream.iter_data()`` where the record ID
    is not needed.
    """
    ctx, _, _ = _build_ctx_and_record_id(
        pod=pod, tag=tag, data=data,
        pipeline_hash_ch=pipeline_hash_ch, run_id=run_id,
    )
    return ctx


# ---------------------------------------------------------------------------
# SideEffectFunctionPod
# ---------------------------------------------------------------------------


class SideEffectFunctionPod(TraceableBase):
    """Function pod that receives an ``InvocationContext`` per row.

    Wraps a callable ``(arg1: T1, ..., ctx: InvocationContext) -> OutputData``.
    The ``ctx`` parameter is stripped from schema inference; data fields are
    passed as keyword arguments. The pod produces a downstream data stream
    like ``FunctionPod``.

    Args:
        fn: Callable whose signature includes ``ctx_arg_name`` plus data args.
        output_keys: Key(s) mapping the return value(s) to output columns.
            A bare string is wrapped in a list.
        ctx_arg_name: Name of the context parameter (default ``"ctx"``).
        config: Pod-level ``SideEffectPodConfig``. Defaults to
            ``SideEffectPodConfig()``.
        name: Optional canonical function name override.
        version: Version integer in the URI (default ``1``).
        label: Optional display label.
        data_context: Optional data context override.

    Raises:
        ValueError: If ``ctx_arg_name`` is not in ``fn``'s signature.
    """

    def __init__(
        self,
        fn: Callable,
        output_keys: list[str] | str,
        ctx_arg_name: str = "ctx",
        config: SideEffectPodConfig | None = None,
        name: str | None = None,
        version: int = 1,
        label: str | None = None,
        data_context: Any = None,
    ) -> None:
        super().__init__(label=label, data_context=data_context)
        self._fn = fn
        self._ctx_arg_name = ctx_arg_name
        self._pod_config = config or SideEffectPodConfig()
        self._version = version
        self._name: str = name if name is not None else getattr(fn, "__name__", "unknown")
        self._output_keys: list[str] = (
            [output_keys] if isinstance(output_keys, str) else list(output_keys)
        )
        self.tracker_manager: "TrackerManagerProtocol" = DEFAULT_TRACKER_MANAGER
        self._is_async = inspect.iscoroutinefunction(fn)

        # Strip ctx for schema inference (raises ValueError if ctx_arg_name missing)
        stripped = _strip_ctx_from_fn(fn, ctx_arg_name)

        # Extract schemas from the stripped wrapper
        self.input_data_schema, self.output_data_schema = extract_function_schemas(
            stripped, output_keys=self._output_keys
        )

        # Pre-compute hashes for URI and result-cache variation data
        from orcapod.hashing.hash_utils import get_function_components, get_function_signature

        semantic_hasher = self.data_context.semantic_hasher
        self._function_signature_hash = semantic_hasher.hash_object(
            get_function_signature(fn)
        ).to_string()
        self._function_content_hash = semantic_hasher.hash_object(
            get_function_components(fn)
        ).to_string()
        self._output_schema_hash = semantic_hasher.hash_object(
            self.output_data_schema
        ).to_string()
        self._git_hash: str = ""  # stable empty string; populated by CI

        # Register Arrow types
        self.data_context.type_converter.ensure_types_registered_for_schemas(
            self.input_data_schema,
            self.output_data_schema,
        )

    # ------------------------------------------------------------------
    # Identity
    # ------------------------------------------------------------------

    @property
    def uri(self) -> tuple[str, ...]:
        """Canonical URI: ``("side_effect_function", name, schema_hash, "vN", "python_side_effect_function")``."""
        return (
            "side_effect_function",
            self.canonical_function_name,
            self._output_schema_hash,
            f"v{self._version}",
            "python_side_effect_function",
        )

    def identity_structure(self) -> Any:
        # Include ctx_arg_name so renaming the context parameter changes the hash,
        # consistent with SideEffectPod.identity_structure().
        return (self.uri, self._ctx_arg_name)

    def pipeline_identity_structure(self) -> Any:
        return self.identity_structure()

    # ------------------------------------------------------------------
    # Pod API
    # ------------------------------------------------------------------

    @property
    def pod_config(self) -> SideEffectPodConfig:
        """Pod-level configuration."""
        return self._pod_config

    @property
    def canonical_function_name(self) -> str:
        """Human-readable function identifier."""
        return self._name

    def computed_label(self) -> str | None:
        """Use the callable's ``__name__`` as the default label."""
        return getattr(self._fn, "__name__", None)

    def argument_symmetry(self, streams: Collection["StreamProtocol"]) -> Any:
        """Single ordered input — return as an ordered tuple."""
        return tuple(streams)

    def output_schema(
        self,
        *streams: "StreamProtocol",
        columns: "ColumnConfig | dict[str, Any] | None" = None,
        all_info: bool = False,
    ) -> "tuple[Schema, Schema]":
        """Return ``(tag_schema, output_data_schema)`` for the given input streams.

        Args:
            *streams: Exactly one input stream.
            columns: Optional column config.
            all_info: Include all metadata columns.

        Returns:
            ``(tag_schema, output_data_schema)`` — tags pass through unchanged.

        Raises:
            ValueError: If ``streams`` does not contain exactly one stream.
        """
        if len(streams) != 1:
            raise ValueError(
                f"SideEffectFunctionPod expects exactly 1 input stream; got {len(streams)}."
            )
        tag_schema, _ = streams[0].output_schema(columns=columns, all_info=all_info)
        return tag_schema, self.output_data_schema

    def process(
        self, *streams: "StreamProtocol", label: str | None = None
    ) -> "SideEffectFunctionPodStream":
        """Invoke the pod on the input stream.

        Records a ``SideEffectFunctionInvocation`` when inside a pipeline
        recording block, then returns a ``SideEffectFunctionPodStream``.

        Args:
            *streams: Exactly one input stream.
            label: Optional display label.

        Returns:
            A ``SideEffectFunctionPodStream``.

        Raises:
            ValueError: If ``streams`` does not contain exactly one stream.
        """
        if len(streams) != 1:
            raise ValueError(
                f"SideEffectFunctionPod.process() expects exactly 1 stream; got {len(streams)}."
            )
        input_stream = streams[0]
        self.tracker_manager.record_side_effect_function_pod_invocation(
            self, input_stream, label=label
        )
        return SideEffectFunctionPodStream(pod=self, input_stream=input_stream, label=label)

    def __call__(
        self, *streams: "StreamProtocol", label: str | None = None
    ) -> "SideEffectFunctionPodStream":
        """Convenience alias for ``process``."""
        return self.process(*streams, label=label)

    # ------------------------------------------------------------------
    # Internal execution helpers
    # ------------------------------------------------------------------

    def _call_with_ctx(self, data: "DataProtocol", ctx: InvocationContext) -> Any:
        """Call the user function with data kwargs and InvocationContext.

        Args:
            data: Input data row.
            ctx: Per-row ``InvocationContext``.

        Returns:
            Raw function return value.

        Raises:
            ValueError: If ``ctx_arg_name`` collides with a data column name.
        """
        data_dict = data.as_dict()
        if self._ctx_arg_name in data_dict:
            raise ValueError(
                f"ctx_arg_name {self._ctx_arg_name!r} collides with a data column of the "
                f"same name. Choose a different ctx_arg_name or rename the data column."
            )
        kwargs = {self._ctx_arg_name: ctx, **data_dict}
        if self._is_async:
            return self._call_async_sync(kwargs)
        return self._fn(**kwargs)

    def _call_async_sync(self, kwargs: dict[str, Any]) -> Any:
        """Run the async user function synchronously.

        Args:
            kwargs: Keyword arguments to pass to ``self._fn``.

        Returns:
            The coroutine's return value.
        """
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(self._fn(**kwargs))

        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(1) as executor:
            future = executor.submit(asyncio.run, self._fn(**kwargs))
            return future.result()

    def _build_output_data(self, raw_output: Any) -> "DataProtocol":
        """Wrap raw function return value in a ``Data`` object with source info.

        Args:
            raw_output: Raw return value from the user function.

        Returns:
            A ``Data`` with source info and a new UUID.
        """
        from orcapod.core.datagrams import Data
        from orcapod.core.data_function import parse_function_outputs

        output_dict = parse_function_outputs(self._output_keys, raw_output)
        new_uuid = uuid.UUID(bytes=uuid7().bytes)
        source_info = {
            k: f"{':'.join(self.uri)}::{new_uuid.hex}::{k}" for k in output_dict
        }
        return Data(
            output_dict,
            source_info=source_info,
            record_uuid=new_uuid,
            python_schema=self.output_data_schema,
            data_context=self.data_context,
        )

    # ------------------------------------------------------------------
    # Result cache metadata (mirrors PythonDataFunction)
    # ------------------------------------------------------------------

    def get_function_variation_data(self) -> dict[str, Any]:
        """Data defining function variation for ``ResultCache.store()``."""
        return {
            "function_name": self.canonical_function_name,
            "function_signature_hash": self._function_signature_hash,
            "function_content_hash": self._function_content_hash,
            "git_hash": self._git_hash,
        }

    def get_function_variation_data_schema(self) -> "Schema":
        """Schema for ``get_function_variation_data``."""
        from orcapod.types import Schema
        return Schema({
            "function_name": str,
            "function_signature_hash": str,
            "function_content_hash": str,
            "git_hash": str,
        })

    def get_execution_data(self) -> dict[str, Any]:
        """Data defining execution context for ``ResultCache.store()``."""
        vi = sys.version_info
        return {
            "executor_type": "none",
            "executor_info": {},
            "python_version": f"{vi.major}.{vi.minor}.{vi.micro}",
            "extra_info": {},
        }

    def get_execution_data_schema(self) -> "Schema":
        """Schema for ``get_execution_data``."""
        from orcapod.types import Schema
        return Schema({
            "executor_type": str,
            "executor_info": dict[str, str],
            "python_version": str,
            "extra_info": dict[str, str],
        })


# ---------------------------------------------------------------------------
# SideEffectFunctionPodStream — standalone execution (no DB)
# ---------------------------------------------------------------------------


class SideEffectFunctionPodStream(StreamBase):
    """Lazy stream returned by ``SideEffectFunctionPod.process()`` in standalone mode.

    Iterates the upstream stream, builds a per-row ``InvocationContext``,
    calls the user function, and yields ``(tag, output_data)`` pairs.
    No invocation log is written in standalone mode (``run_id=None``).

    Args:
        pod: The ``SideEffectFunctionPod`` this stream wraps.
        input_stream: The upstream stream.
    """

    node_type = "side_effect_function"

    def __init__(
        self,
        pod: SideEffectFunctionPod,
        input_stream: "StreamProtocol",
        **kwargs: Any,
    ) -> None:
        self._pod = pod
        self._input_stream = input_stream
        super().__init__(**kwargs)

    @property
    def producer(self) -> SideEffectFunctionPod:
        return self._pod

    @property
    def upstreams(self) -> "tuple[StreamProtocol, ...]":
        return (self._input_stream,)

    def identity_structure(self) -> Any:
        return (self._pod, self._pod.argument_symmetry((self._input_stream,)))

    def pipeline_identity_structure(self) -> Any:
        return self.identity_structure()

    def output_schema(
        self,
        *,
        columns: "ColumnConfig | dict[str, Any] | None" = None,
        all_info: bool = False,
    ) -> "tuple[Schema, Schema]":
        tag_schema, _ = self._input_stream.output_schema(columns=columns, all_info=all_info)
        return tag_schema, self._pod.output_data_schema

    def keys(
        self,
        *,
        columns: "ColumnConfig | dict[str, Any] | None" = None,
        all_info: bool = False,
    ) -> "tuple[tuple[str, ...], tuple[str, ...]]":
        tag_schema, data_schema = self.output_schema(columns=columns, all_info=all_info)
        return tuple(tag_schema.keys()), tuple(data_schema.keys())

    def iter_data(self) -> "Iterator[tuple[TagProtocol, DataProtocol]]":
        """Iterate the input stream, calling the pod function per row.

        Exceptions from the user function always propagate.
        """
        for tag, data in self._input_stream.iter_data():
            ctx = _build_invocation_context(
                pod=self._pod,
                tag=tag,
                data=data,
                pipeline_hash_ch=self.pipeline_hash(),
                run_id=None,
            )
            raw = self._pod._call_with_ctx(data, ctx)
            output_data = self._pod._build_output_data(raw)
            yield tag, output_data

    def as_table(
        self,
        *,
        columns: "ColumnConfig | dict[str, Any] | None" = None,
        all_info: bool = False,
    ) -> "pa.Table":
        """Collect all rows from ``iter_data()`` into an Arrow table."""
        from orcapod.types import ColumnConfig as _ColumnConfig
        from orcapod.utils import arrow_utils

        column_config = _ColumnConfig.handle_config(columns, all_info=all_info)
        tag_tables = []
        data_tables = []
        for tag, data in self.iter_data():
            tag_tables.append(tag.as_table(columns=column_config))
            data_tables.append(data.as_table(columns=column_config))
        if not tag_tables:
            tag_schema, data_schema = self.output_schema(columns=column_config)
            tc = self._pod.data_context.type_converter
            fields = {
                name: pa.array([], type=tc.python_type_to_arrow_type(py_type))
                for name, py_type in {**tag_schema, **data_schema}.items()
            }
            return pa.table(fields)
        return arrow_utils.hstack_tables(
            pa.concat_tables(tag_tables),
            pa.concat_tables(data_tables),
        )


# ---------------------------------------------------------------------------
# SideEffectFunctionNode — blueprint (raises on iter_data)
# ---------------------------------------------------------------------------


class SideEffectFunctionNode(StreamBase):
    """Lightweight blueprint node for side-effect function pods.

    Used by ``Pipeline`` to represent a ``SideEffectFunctionPod`` invocation
    without any DB attachment or execution logic. ``iter_data()`` raises
    ``PipelineJobRequiredError`` — use a ``PipelineJob`` to execute.

    Args:
        pod: The ``SideEffectFunctionPod`` this node wraps.
        input_stream: The upstream stream at compile time.
        label: Optional display label.
    """

    node_type = "side_effect_function"

    def __init__(
        self,
        pod: SideEffectFunctionPod,
        input_stream: "StreamProtocol",
        label: str | None = None,
    ) -> None:
        self._pod = pod
        self._input_stream = input_stream
        super().__init__(label=label)

    @property
    def producer(self) -> SideEffectFunctionPod:
        return self._pod

    @property
    def upstreams(self) -> "tuple[StreamProtocol, ...]":
        return (self._input_stream,)

    def identity_structure(self) -> Any:
        return (self._pod, self._pod.argument_symmetry((self._input_stream,)))

    def pipeline_identity_structure(self) -> Any:
        return self.identity_structure()

    def computed_label(self) -> str | None:
        return self._pod.label

    def output_schema(
        self,
        *,
        columns: "ColumnConfig | dict[str, Any] | None" = None,
        all_info: bool = False,
    ) -> "tuple[Schema, Schema]":
        tag_schema, _ = self._input_stream.output_schema(columns=columns, all_info=all_info)
        return tag_schema, self._pod.output_data_schema

    def keys(
        self,
        *,
        columns: "ColumnConfig | dict[str, Any] | None" = None,
        all_info: bool = False,
    ) -> "tuple[tuple[str, ...], tuple[str, ...]]":
        tag_schema, data_schema = self.output_schema(columns=columns, all_info=all_info)
        return tuple(tag_schema.keys()), tuple(data_schema.keys())

    def iter_data(self) -> "Iterator[tuple[TagProtocol, DataProtocol]]":
        raise PipelineJobRequiredError(
            "SideEffectFunctionNode.iter_data() requires a PipelineJob. "
            "Use pod.process(stream).iter_data() for standalone execution, "
            "or run via a PipelineJob."
        )

    def as_table(
        self,
        *,
        columns: "ColumnConfig | dict[str, Any] | None" = None,
        all_info: bool = False,
    ) -> "pa.Table":
        raise PipelineJobRequiredError(
            "SideEffectFunctionNode.as_table() requires a PipelineJob."
        )

    @property
    def node_uri(self) -> tuple[str, ...]:
        """Canonical URI identifying this node — same as ``pod.uri``."""
        return self._pod.uri

    def set_ephemeral_store(self, store: "ArrowDatabaseProtocol | None") -> None:
        """No-op for blueprint nodes."""


# ---------------------------------------------------------------------------
# SideEffectFunctionJobNode — DB-backed execution
# ---------------------------------------------------------------------------


class SideEffectFunctionJobNode(SideEffectFunctionNode):
    """DB-backed execution node for side-effect function pods.

    Created at pipeline compile time by ``PipelineJob``. Receives databases
    via ``attach_databases()``. On each ``execute()`` call:

    1. Cache hit check (if ``track_completion=True``).
    2. Build ``InvocationContext`` per row.
    3. Call user function — exceptions always propagate.
    4. Wrap output in ``Data`` and cache in result DB.
    5. Write invocation log row to pipeline DB.

    Args:
        pod: The ``SideEffectFunctionPod`` this node wraps.
        input_stream: The upstream stream at compile time.
        label: Optional display label.
    """

    def __init__(
        self,
        pod: SideEffectFunctionPod,
        input_stream: "StreamProtocol",
        label: str | None = None,
    ) -> None:
        super().__init__(pod=pod, input_stream=input_stream, label=label)
        self._pipeline_database: "ArrowDatabaseProtocol | None" = None
        self._result_cache: Any = None
        self._table_path: tuple[str, ...] | None = None

    def attach_databases(
        self,
        pipeline_database: "ArrowDatabaseProtocol | None" = None,
        result_database: "ArrowDatabaseProtocol | None" = None,
    ) -> None:
        """Attach pipeline and result databases.

        Sets up the result cache and the invocation log table path.
        Called by ``PipelineJob._distribute_databases()``.

        Args:
            pipeline_database: Pre-scoped pipeline DB for invocation logging.
            result_database: DB for output caching via ``ResultCache``.
        """
        from orcapod.core.result_cache import ResultCache

        self._pipeline_database = pipeline_database
        self._result_cache = (
            ResultCache(result_database, record_path=self.node_uri)
            if result_database is not None
            else None
        )
        if pipeline_database is not None:
            self._table_path = self.node_uri + (
                f"schema:{self.pipeline_hash().to_string()}",
            )
        else:
            self._table_path = None

    def execute(
        self,
        input_stream: "StreamProtocol",
        *,
        observer: Any = None,
        run_id: str | None = None,
    ) -> "list[tuple[TagProtocol, DataProtocol]]":
        """Execute side-effect function delivery for all rows in ``input_stream``.

        Args:
            input_stream: Stream of ``(tag, data)`` pairs to process.
            observer: Optional execution observer (currently unused).
            run_id: Pipeline run identifier from the orchestrator.

        Returns:
            List of ``(tag, output_data)`` tuples.
        """
        from orcapod.core.datagrams import Datagram

        results: list[tuple[TagProtocol, DataProtocol]] = []

        for tag, data in input_stream.iter_data():
            # 1. Cache hit check
            if (
                self._pod.pod_config.track_completion
                and self._result_cache is not None
            ):
                cached = self._result_cache.lookup(data)
                if cached is not None:
                    results.append((tag, cached))
                    continue

            # 2. Build InvocationContext + record_id (single preimage computation)
            ctx, record_id_hash, record_id = _build_ctx_and_record_id(
                pod=self._pod,
                tag=tag,
                data=data,
                pipeline_hash_ch=self.pipeline_hash(),
                run_id=run_id,
            )

            # 3. Call user function — always re-raise (no silent row suppression).
            # Unlike SideEffectPod, dropping a row would break the downstream
            # stream that consumers depend on. on_error="log" only controls whether
            # the exception is logged before propagating.
            try:
                raw = self._pod._call_with_ctx(data, ctx)
            except Exception as exc:
                if self._pod.pod_config.on_error == "log":
                    logger.warning(
                        "SideEffectFunctionPod %r failed on row: %s",
                        self._pod.label,
                        exc,
                        exc_info=True,
                    )
                raise  # always re-raise

            # 4. Wrap output and cache it
            output_data = self._pod._build_output_data(raw)
            if self._result_cache is not None:
                var_dg = Datagram(
                    self._pod.get_function_variation_data(),
                    python_schema=self._pod.get_function_variation_data_schema(),
                    data_context=self._pod.data_context,
                )
                exec_dg = Datagram(
                    self._pod.get_execution_data(),
                    python_schema=self._pod.get_execution_data_schema(),
                    data_context=self._pod.data_context,
                )
                self._result_cache.store(data, output_data, var_dg, exec_dg)

            # 5. Log invocation to pipeline database
            if self._pipeline_database is not None and self._table_path is not None:
                _write_invocation_row(
                    pipeline_database=self._pipeline_database,
                    table_path=self._table_path,
                    record_id=record_id,
                    record_id_hash_str=record_id_hash.to_string(),
                    run_id=run_id,
                )

            results.append((tag, output_data))

        return results

    async def async_execute(
        self,
        inputs: "Sequence[Any]",
        output: Any,
        *,
        observer: Any = None,
        run_id: str | None = None,
    ) -> None:
        """Async execution with semaphore-bounded concurrency.

        Reads from ``inputs[0]``, dispatches each row as an independent
        async task via ``asyncio.TaskGroup``. A semaphore bounds in-flight
        tasks at ``pod_config.max_concurrency`` (default 16). Always closes
        ``output`` in a ``finally`` block.

        Args:
            inputs: Single-element sequence with the input channel.
            output: Writable channel for output ``(tag, output_data)`` pairs.
            observer: Optional execution observer (currently unused).
            run_id: Pipeline run identifier from the orchestrator.

        Raises:
            ValueError: If ``inputs`` does not contain exactly one channel.
        """
        if len(inputs) != 1:
            raise ValueError(
                f"SideEffectFunctionJobNode.async_execute expects exactly 1 "
                f"input channel; got {len(inputs)}."
            )

        max_concurrency = self._pod.pod_config.max_concurrency
        sem = asyncio.Semaphore(max_concurrency) if max_concurrency is not None else None

        try:
            async def process_one(tag: "TagProtocol", data: "DataProtocol") -> None:
                try:
                    from orcapod.core.datagrams import Datagram

                    # Cache hit check
                    if (
                        self._pod.pod_config.track_completion
                        and self._result_cache is not None
                    ):
                        cached = self._result_cache.lookup(data)
                        if cached is not None:
                            await output.send((tag, cached))
                            return

                    ctx, record_id_hash, record_id = _build_ctx_and_record_id(
                        pod=self._pod,
                        tag=tag,
                        data=data,
                        pipeline_hash_ch=self.pipeline_hash(),
                        run_id=run_id,
                    )

                    # Always re-raise — on_error="log" only controls logging.
                    try:
                        raw = self._pod._call_with_ctx(data, ctx)
                    except Exception as exc:
                        if self._pod.pod_config.on_error == "log":
                            logger.warning(
                                "SideEffectFunctionPod %r async failed: %s",
                                self._pod.label,
                                exc,
                                exc_info=True,
                            )
                        raise

                    output_data = self._pod._build_output_data(raw)

                    if self._result_cache is not None:
                        var_dg = Datagram(
                            self._pod.get_function_variation_data(),
                            python_schema=self._pod.get_function_variation_data_schema(),
                            data_context=self._pod.data_context,
                        )
                        exec_dg = Datagram(
                            self._pod.get_execution_data(),
                            python_schema=self._pod.get_execution_data_schema(),
                            data_context=self._pod.data_context,
                        )
                        self._result_cache.store(data, output_data, var_dg, exec_dg)

                    if self._pipeline_database is not None and self._table_path is not None:
                        _write_invocation_row(
                            pipeline_database=self._pipeline_database,
                            table_path=self._table_path,
                            record_id=record_id,
                            record_id_hash_str=record_id_hash.to_string(),
                            run_id=run_id,
                        )

                    await output.send((tag, output_data))
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

    def set_ephemeral_store(self, store: "ArrowDatabaseProtocol | None") -> None:
        """No-op for this node type."""


# ---------------------------------------------------------------------------
# Decorator
# ---------------------------------------------------------------------------


def side_effect_function_pod(
    fn: Callable | None = None,
    *,
    output_keys: list[str] | str,
    ctx_arg_name: str = "ctx",
    config: SideEffectPodConfig | None = None,
    name: str | None = None,
    version: int = 1,
) -> "SideEffectFunctionPod | Callable":
    """Decorator wrapping a callable as a ``SideEffectFunctionPod``.

    Parameterised usage only — ``output_keys`` is always required:

    .. code-block:: python

        @side_effect_function_pod(output_keys=["artifact_path"])
        def write_artifact(value: int, ctx: InvocationContext) -> str:
            return f"out/{ctx.invocation_hash}.bin"

    The decorated name is replaced with a ``SideEffectFunctionPod`` instance.
    The pod is callable (via ``__call__`` -> ``process``).

    Args:
        fn: Internal — not for direct caller use.
        output_keys: Key(s) mapping the return value(s) to output columns.
        ctx_arg_name: Name of the context parameter (default ``"ctx"``).
        config: Optional ``SideEffectPodConfig``.
        name: Optional canonical function name override.
        version: Version integer for the URI (default ``1``).

    Returns:
        A ``SideEffectFunctionPod`` (or a one-argument decorator).
    """
    def _wrap(f: Callable) -> SideEffectFunctionPod:
        return SideEffectFunctionPod(
            f,
            output_keys=output_keys,
            ctx_arg_name=ctx_arg_name,
            config=config,
            name=name,
            version=version,
        )

    if fn is not None:
        return _wrap(fn)
    return _wrap
