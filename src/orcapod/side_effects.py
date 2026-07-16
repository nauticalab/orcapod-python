# src/orcapod/side_effects.py
"""SideEffectPod — pass-through pipeline node for side effects.

Provides ``SideEffectPod``, ``SideEffectPodStream``, ``SideEffectJobNode``,
``InvocationContext``, ``InvocationHashConfig``, ``SideEffectPodConfig``,
and the ``side_effect_pod``, ``sink_pod``, ``tap_pod`` decorators.
"""
from __future__ import annotations

import asyncio
import base64
import dataclasses
import datetime
import logging
from collections.abc import Callable, Collection, Iterator, Sequence
from typing import TYPE_CHECKING, Any, Literal

from orcapod.core.base import TraceableBase
from orcapod.core.streams.base import StreamBase
from orcapod.core.tracker import DEFAULT_TRACKER_MANAGER
from orcapod.utils.lazy_module import LazyModule

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
    from orcapod.protocols.observability_protocols import ExecutionObserverProtocol
    from orcapod.types import ColumnConfig, ContentHash, Schema
else:
    pa = LazyModule("pyarrow")

logger = logging.getLogger(__name__)

# Column name used in the record-ID preimage table (same semantic as
# ``_PIPELINE_RECOMPUTATION_INDEX_COL`` in ``function_node.py``).
_SIDE_EFFECT_RECOMPUTATION_INDEX_COL = "__pipeline_recomputation_index"


# ---------------------------------------------------------------------------
# InvocationHashConfig
# ---------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class InvocationHashConfig:
    """Controls how ``InvocationContext.invocation_hash`` is serialized.

    Args:
        encoding: Output encoding — ``"hex"`` (default) or ``"base64"``.
        component_length: Bytes of raw digest to use per component. ``None``
            means full digest length. Applied identically to every
            ``::``-separated component.
    """

    encoding: Literal["hex", "base64"] = "hex"
    component_length: int | None = None


def _serialize_component(content_hash: ContentHash, config: InvocationHashConfig) -> str:
    """Serialize one ``ContentHash`` component per ``InvocationHashConfig``.

    Args:
        content_hash: The hash to serialize.
        config: Encoding and truncation config.

    Returns:
        A string representation of the (optionally truncated) digest.
    """
    raw = content_hash.digest
    if config.component_length is not None:
        raw = raw[:config.component_length]
    if config.encoding == "base64":
        return base64.b64encode(raw).decode("ascii")
    return raw.hex()


# ---------------------------------------------------------------------------
# SideEffectPodConfig
# ---------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class SideEffectPodConfig:
    """Configuration for a ``SideEffectPod``.

    Args:
        track_completion: If ``True`` (default), skip re-delivery for inputs
            that previously completed successfully.
        drop_on_failure: If ``True`` (default), drop rows whose delivery
            raised an exception from the downstream output.
        on_error: ``"raise"`` (default) re-raises delivery exceptions;
            ``"log"`` logs at WARNING and continues.
        hash_config: Controls encoding of ``InvocationContext.invocation_hash``.
        max_concurrency: Maximum number of in-flight async delivery tasks when
            running under the async orchestrator. ``None`` means unlimited.
            Defaults to ``16`` to prevent unbounded task accumulation on large
            streams.
    """

    track_completion: bool = True
    drop_on_failure: bool = True
    on_error: Literal["raise", "log"] = "raise"
    hash_config: InvocationHashConfig = dataclasses.field(
        default_factory=InvocationHashConfig
    )
    max_concurrency: int | None = 16


# ---------------------------------------------------------------------------
# InvocationContext
# ---------------------------------------------------------------------------


class InvocationContext:
    """Per-invocation context passed to every side-effect pod function.

    Carries a deterministic ``invocation_hash`` and metadata about the
    current delivery. ``invocation_hash`` is a computed property that
    delegates to ``format_id()`` with the pod's default
    ``InvocationHashConfig``. ``format_id()`` can be called with a custom
    config to re-serialize without recomputation.

    Public fields are read-only by convention (no public setters).

    Args:
        pod_name: ``pod.label`` of the invoking pod.
        pipeline_run_id: The current pipeline run identifier, or ``None``
            for standalone / lazy pipelines.
    """

    def __init__(
        self,
        pod_name: str,
        pipeline_run_id: str | None,
        _pipeline_hash_ch: ContentHash,
        _record_id_hash_ch: ContentHash,
        _hash_config: InvocationHashConfig,
        _track_completion: bool,
    ) -> None:
        self.pod_name = pod_name
        self.pipeline_run_id = pipeline_run_id
        self._pipeline_hash_ch = _pipeline_hash_ch
        self._record_id_hash_ch = _record_id_hash_ch
        self._hash_config = _hash_config
        self._track_completion = _track_completion

    @property
    def invocation_hash(self) -> str:
        """Serialized invocation hash — delegates to ``format_id()``."""
        return self.format_id()

    def format_id(self, config: InvocationHashConfig | None = None) -> str:
        """Return ``'orcapod-{hash}'`` with an optional format override.

        Serializes the stored ``ContentHash`` components. Uses ``config``
        if supplied, otherwise the pod's own ``InvocationHashConfig``.

        Args:
            config: Optional encoding/truncation override.

        Returns:
            A string of the form ``"orcapod-{component1}::{component2}"``
            (two components when ``track_completion=True``) or
            ``"orcapod-{c1}::{c2}::{run_id}"`` (three components when
            ``track_completion=False`` and ``pipeline_run_id`` is not ``None``).
        """
        cfg = config or self._hash_config
        c1 = _serialize_component(self._pipeline_hash_ch, cfg)
        c2 = _serialize_component(self._record_id_hash_ch, cfg)
        if not self._track_completion and self.pipeline_run_id is not None:
            parts = f"{c1}::{c2}::{self.pipeline_run_id}"
        else:
            parts = f"{c1}::{c2}"
        return f"orcapod-{parts}"


# ---------------------------------------------------------------------------
# SideEffectPodStream
# ---------------------------------------------------------------------------


class SideEffectPodStream(StreamBase):
    """Pass-through stream returned by ``SideEffectPod.process()`` in standalone mode.

    Iterates the upstream stream and calls the side-effect function per row.
    No invocation log is written in standalone mode (``pipeline_run_id=None``).
    """

    def __init__(
        self,
        side_effect_pod: SideEffectPod,
        input_stream: StreamProtocol,
        **kwargs: Any,
    ) -> None:
        self._pod = side_effect_pod
        self._input_stream = input_stream
        super().__init__(**kwargs)

    @property
    def producer(self) -> SideEffectPod:  # type: ignore[override]
        return self._pod

    @property
    def upstreams(self) -> tuple[StreamProtocol, ...]:
        return (self._input_stream,)

    def identity_structure(self) -> Any:
        return (self._pod, self._pod.argument_symmetry((self._input_stream,)))

    def pipeline_identity_structure(self) -> Any:
        return self.identity_structure()

    def output_schema(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        return self._input_stream.output_schema(columns=columns, all_info=all_info)

    def keys(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        return self._input_stream.keys(columns=columns, all_info=all_info)

    def iter_data(self) -> Iterator[tuple[TagProtocol, DataProtocol]]:
        for tag, data in self._input_stream.iter_data():
            result = _execute_side_effect_row(
                fn=self._pod._fn,
                tag=tag,
                data=data,
                pod_config=self._pod.pod_config,
                pipeline_hash_ch=self.pipeline_hash(),
                node_content_hash_str=self._pod.content_hash().to_string(),
                pod_name=self._pod.label,
                run_id=None,
                arrow_hasher=self._pod.data_context.arrow_hasher,
            )
            if result is not None:
                yield result

    def as_table(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> pa.Table:
        from orcapod.types import ColumnConfig as _ColumnConfig
        from orcapod.utils import arrow_utils

        column_config = _ColumnConfig.handle_config(columns, all_info=all_info)
        tag_tables = []
        data_tables = []
        for tag, data in self.iter_data():
            tag_tables.append(tag.as_table(columns=column_config))
            data_tables.append(data.as_table(columns=column_config))
        if not tag_tables:
            # Return an empty table with the correct schema
            tag_schema, data_schema = self.output_schema(
                columns=column_config
            )
            tc = self._pod.data_context.type_converter
            fields = {}
            for name, py_type in {**tag_schema, **data_schema}.items():
                fields[name] = pa.array(
                    [], type=tc.python_type_to_arrow_type(py_type)
                )
            return pa.table(fields)
        combined_tags = pa.concat_tables(tag_tables)
        combined_data = pa.concat_tables(data_tables)
        return arrow_utils.hstack_tables(combined_tags, combined_data)


# ---------------------------------------------------------------------------
# Shared row execution helper
# ---------------------------------------------------------------------------


def _execute_side_effect_row(
    *,
    fn: Callable,
    tag: TagProtocol,
    data: DataProtocol,
    pod_config: SideEffectPodConfig,
    pipeline_hash_ch: ContentHash,
    node_content_hash_str: str,
    pod_name: str,
    run_id: str | None,
    arrow_hasher: Any,
    pipeline_database: ArrowDatabaseProtocol | None = None,
    table_path: tuple[str, ...] | None = None,
) -> tuple[TagProtocol, DataProtocol] | None:
    """Execute delivery for one (tag, data) row.

    Computes a deterministic ``record_id`` from a unified preimage (matching
    ``FunctionNode._build_entry_id_preimage`` plus a recomputation index of
    ``0``).  The preimage covers:

    * tag system-tag columns,
    * input data values and source-info provenance columns,
    * ``NODE_CONTENT_HASH_COL`` (the pod's own content hash), and
    * a recomputation index of ``0`` (side effects never recompute).

    The invocation hash is ``pipeline_hash :: record_id_hash``, where
    ``pipeline_hash`` uniquely identifies the table path and ``record_id_hash``
    uniquely identifies the entry within that table.

    Args:
        fn: The side-effect callable ``(data, ctx) -> None``.
        tag: Tag for this row.
        data: Data for this row.
        pod_config: Pod-level configuration.
        pipeline_hash_ch: Pipeline hash of the node (for invocation_hash c1).
        node_content_hash_str: ``pod.content_hash().to_string()`` — included in
            the preimage as ``NODE_CONTENT_HASH_COL`` to scope the record ID
            to this specific pod version.
        pod_name: Label of the pod.
        run_id: Pipeline run identifier (or ``None`` in standalone mode).
        arrow_hasher: The ``arrow_hasher`` from the pod's data context.
        pipeline_database: Attached DB (or ``None`` for standalone mode).
        table_path: Path tuple for the invocation log table.

    Returns:
        ``(tag, data)`` to emit downstream, or ``None`` to drop the row.
    """
    from orcapod.system_constants import constants
    from orcapod.utils import arrow_utils

    # 1. Build the unified preimage (identical structure to FunctionNode).
    preimage = arrow_utils.hstack_tables(
        tag.as_table(columns={"system_tags": True}),
        data.as_table(columns={"source": True}),
        pa.table(
            {
                constants.NODE_CONTENT_HASH_COL: pa.array(
                    [node_content_hash_str], type=pa.large_string()
                )
            }
        ),
        pa.table(
            {
                _SIDE_EFFECT_RECOMPUTATION_INDEX_COL: pa.array([0], type=pa.int32())
            }
        ),
    )
    record_id_hash: ContentHash = arrow_hasher.hash_table(preimage)
    record_id: bytes = record_id_hash.to_prefixed_digest()

    # 2. Completion check — look up by deterministic record_id.
    if pod_config.track_completion and pipeline_database is not None and table_path is not None:
        prior = pipeline_database.get_record_by_id(table_path, record_id)
        if prior is not None:
            return (tag, data)  # already completed — re-emit without re-delivery

    # 3. Build InvocationContext (invocation_hash derived lazily via format_id()).
    ctx = InvocationContext(
        pod_name=pod_name,
        pipeline_run_id=run_id,
        _pipeline_hash_ch=pipeline_hash_ch,
        _record_id_hash_ch=record_id_hash,
        _hash_config=pod_config.hash_config,
        _track_completion=pod_config.track_completion,
    )

    # 5. Call user function.
    try:
        fn(data, ctx)
        if pipeline_database is not None and table_path is not None:
            _write_invocation_row(
                pipeline_database=pipeline_database,
                table_path=table_path,
                record_id=record_id,
                record_id_hash_str=record_id_hash.to_string(),
                run_id=run_id,
            )
        return (tag, data)
    except Exception as exc:
        if pod_config.on_error == "raise":
            raise
        logger.warning(
            "SideEffectPod %r delivery failed: %s", pod_name, exc, exc_info=True
        )
        if pod_config.drop_on_failure:
            return None
        return (tag, data)


def _write_invocation_row(
    *,
    pipeline_database: ArrowDatabaseProtocol,
    table_path: tuple[str, ...],
    record_id: bytes,
    record_id_hash_str: str,
    run_id: str | None,
) -> None:
    """Write one success row to the side-effect invocation log table.

    Only called on successful delivery. Uses a deterministic ``record_id``
    (the prefixed digest of the unified preimage hash) so that
    ``get_record_by_id`` can look up prior completions without a column scan.

    Args:
        pipeline_database: The attached pipeline database.
        table_path: Path tuple for the invocation log table.
        record_id: Deterministic bytes key for this ``(input, pod version)``
            pair — the prefixed digest of the unified preimage hash.
        record_id_hash_str: String form of the record-ID hash (stored for
            human inspection).
        run_id: Pipeline run identifier (or ``None`` for standalone mode).
    """
    executed_at = datetime.datetime.now(datetime.timezone.utc)
    record = pa.table(
        {
            "record_id_hash": pa.array(
                [record_id_hash_str], type=pa.large_string()
            ),
            "pipeline_run_id": pa.array(
                [run_id], type=pa.large_string()
            ),
            "executed_at": pa.array(
                [executed_at],
                type=pa.timestamp("us", tz="UTC"),
            ),
        }
    )
    pipeline_database.add_record(
        table_path,
        record_id,
        record,
        skip_duplicates=True,
    )


# ---------------------------------------------------------------------------
# SideEffectPod
# ---------------------------------------------------------------------------


class SideEffectPod(TraceableBase):
    """A pipeline node whose primary purpose is a side effect.

    Wraps a ``(data: T, ctx: InvocationContext) -> None`` callable.
    ``InvocationContext`` is always constructed and passed — it is part of
    the function contract. Callees that do not need it may ignore it (name
    the parameter ``_ctx`` by convention).

    Returns a pass-through stream. When ``drop_on_failure=True``, only
    successfully-delivered rows flow downstream.

    In standalone mode (no ``PipelineJob``), executes row-by-row via
    ``SideEffectPodStream`` with no invocation logging.

    In pipeline mode, promoted to ``SideEffectJobNode`` at compile time,
    which adds DB-backed invocation logging and completion tracking.

    Args:
        fn: A callable ``(data, ctx: InvocationContext) -> None``.
        config: Pod-level configuration. Defaults to ``SideEffectPodConfig()``.
        tracker_manager: Optional tracker manager override.
        name: Optional canonical function name used in ``uri`` and the
            invocation-log table path. Defaults to ``fn.__name__``.
        label: Optional display label (separate from ``name``).
        data_context: Optional data context override.
    """

    def __init__(
        self,
        fn: Callable,
        config: SideEffectPodConfig | None = None,
        tracker_manager: TrackerManagerProtocol | None = None,
        name: str | None = None,
        label: str | None = None,
        data_context: Any = None,
    ) -> None:
        super().__init__(label=label, data_context=data_context)
        self._fn = fn
        self._name: str = name if name is not None else getattr(fn, "__name__", "unknown")
        self._pod_config = config or SideEffectPodConfig()
        self.tracker_manager = tracker_manager or DEFAULT_TRACKER_MANAGER

    @property
    def pod_config(self) -> SideEffectPodConfig:
        """Pod-level configuration."""
        return self._pod_config

    @property
    def canonical_function_name(self) -> str:
        """Human-readable function identifier, defaults to ``fn.__name__``."""
        return self._name

    def computed_label(self) -> str | None:
        """Use the callable's ``__name__`` as the default label."""
        return getattr(self._fn, "__name__", None)

    def identity_structure(self) -> Any:
        return (self.uri, self._pod_config.track_completion, self._pod_config.drop_on_failure)

    def pipeline_identity_structure(self) -> Any:
        return self.identity_structure()

    @property
    def uri(self) -> tuple[str, ...]:
        """Canonical URI for this pod: ``("side_effects", canonical_function_name)``."""
        return ("side_effects", self.canonical_function_name)

    def argument_symmetry(self, streams: Collection[StreamProtocol]) -> Any:
        """Single ordered input — return as an ordered tuple."""
        return tuple(streams)

    def output_schema(
        self,
        *streams: StreamProtocol,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        """Return the input stream's schema unchanged (pass-through).

        Args:
            *streams: Exactly one input stream.
            columns: Optional column config.
            all_info: Include all metadata columns.

        Returns:
            The input stream's ``(tag_schema, data_schema)`` unchanged.
        """
        if len(streams) != 1:
            raise ValueError(
                f"SideEffectPod expects exactly 1 input stream; got {len(streams)}."
            )
        return streams[0].output_schema(columns=columns, all_info=all_info)

    def process(
        self, *streams: StreamProtocol, label: str | None = None
    ) -> SideEffectPodStream:
        """Invoke the side-effect pod on the input stream.

        Registers a ``SideEffectInvocation`` with the tracker manager (if
        inside a ``with PipelineJob():`` block), then returns a
        ``SideEffectPodStream`` for standalone / lazy execution.

        Args:
            *streams: Exactly one input stream.
            label: Optional label for the compiled node.

        Returns:
            A ``SideEffectPodStream``.
        """
        if len(streams) != 1:
            raise ValueError(
                f"SideEffectPod.process() expects exactly 1 stream; got {len(streams)}."
            )
        input_stream = streams[0]
        self.tracker_manager.record_side_effect_pod_invocation(
            self, input_stream, label=label
        )
        return SideEffectPodStream(
            side_effect_pod=self,
            input_stream=input_stream,
            label=label,
        )

    def __call__(
        self, *streams: StreamProtocol, label: str | None = None
    ) -> SideEffectPodStream:
        """Convenience alias for ``process``."""
        return self.process(*streams, label=label)


# ---------------------------------------------------------------------------
# Decorators
# ---------------------------------------------------------------------------


def side_effect_pod(
    fn: Callable | None = None,
    *,
    config: SideEffectPodConfig | None = None,
    name: str | None = None,
) -> SideEffectPod | Callable[[Callable], SideEffectPod]:
    """Decorator that wraps a callable as a ``SideEffectPod``.

    Supports both bare (``@side_effect_pod``) and parameterised
    (``@side_effect_pod(config=...)``) usage.

    Args:
        fn: The callable to wrap (when used as a bare decorator).
        config: Optional ``SideEffectPodConfig`` to apply.
        name: Optional canonical function name override (see ``SideEffectPod``).

    Returns:
        A ``SideEffectPod`` (bare usage) or a decorator (parameterised usage).
    """
    def _wrap(f: Callable) -> SideEffectPod:
        return SideEffectPod(f, config=config, name=name)

    if fn is not None:
        return _wrap(fn)
    return _wrap


def sink_pod(
    fn: Callable | None = None,
    *,
    config: SideEffectPodConfig | None = None,
    name: str | None = None,
) -> SideEffectPod | Callable[[Callable], SideEffectPod]:
    """Decorator preset: ``track_completion=True``, ``drop_on_failure=True``.

    Caller-supplied ``config`` fields override the presets. Supports both
    bare (``@sink_pod``) and parameterised (``@sink_pod(config=...)``) usage.

    Args:
        fn: The callable to wrap (bare usage).
        config: Optional config override.
        name: Optional canonical function name override (see ``SideEffectPod``).

    Returns:
        A ``SideEffectPod`` or decorator.
    """
    preset = SideEffectPodConfig(track_completion=True, drop_on_failure=True)
    effective_config = _merge_config(preset, config)

    def _wrap(f: Callable) -> SideEffectPod:
        return SideEffectPod(f, config=effective_config, name=name)

    if fn is not None:
        return _wrap(fn)
    return _wrap


def tap_pod(
    fn: Callable | None = None,
    *,
    config: SideEffectPodConfig | None = None,
    name: str | None = None,
) -> SideEffectPod | Callable[[Callable], SideEffectPod]:
    """Decorator preset: ``track_completion=False``, ``drop_on_failure=False``.

    Caller-supplied ``config`` fields override the presets. Supports both
    bare (``@tap_pod``) and parameterised (``@tap_pod(config=...)``) usage.

    Args:
        fn: The callable to wrap (bare usage).
        config: Optional config override.
        name: Optional canonical function name override (see ``SideEffectPod``).

    Returns:
        A ``SideEffectPod`` or decorator.
    """
    preset = SideEffectPodConfig(track_completion=False, drop_on_failure=False)
    effective_config = _merge_config(preset, config)

    def _wrap(f: Callable) -> SideEffectPod:
        return SideEffectPod(f, config=effective_config, name=name)

    if fn is not None:
        return _wrap(fn)
    return _wrap


def _merge_config(
    preset: SideEffectPodConfig,
    override: SideEffectPodConfig | None,
) -> SideEffectPodConfig:
    """Merge *preset* with caller-supplied *override*.

    Non-default fields in *override* win over the preset.

    Args:
        preset: The decorator's pre-configured defaults.
        override: Optional caller-supplied config.

    Returns:
        A merged ``SideEffectPodConfig``.
    """
    if override is None:
        return preset
    default = SideEffectPodConfig()
    return SideEffectPodConfig(
        track_completion=(
            override.track_completion
            if override.track_completion != default.track_completion
            else preset.track_completion
        ),
        drop_on_failure=(
            override.drop_on_failure
            if override.drop_on_failure != default.drop_on_failure
            else preset.drop_on_failure
        ),
        on_error=(
            override.on_error
            if override.on_error != default.on_error
            else preset.on_error
        ),
        hash_config=(
            override.hash_config
            if override.hash_config != default.hash_config
            else preset.hash_config
        ),
        max_concurrency=(
            override.max_concurrency
            if override.max_concurrency != default.max_concurrency
            else preset.max_concurrency
        ),
    )


# ---------------------------------------------------------------------------
# SideEffectNode — lightweight blueprint node (no DB)
# ---------------------------------------------------------------------------


class SideEffectNode(StreamBase):
    """Lightweight blueprint node for side-effect pods.

    Used by ``Pipeline`` (the blueprint) to represent a side-effect pod
    invocation without any DB attachment or execution logic. Analogous to
    ``FunctionNode`` in the function pod hierarchy.

    Args:
        side_effect_pod: The ``SideEffectPod`` this node wraps.
        input_stream: The upstream stream at compile time.
        label: Optional display label.
    """

    node_type = "side_effect"

    def __init__(
        self,
        side_effect_pod: SideEffectPod,
        input_stream: StreamProtocol,
        label: str | None = None,
    ) -> None:
        self._pod = side_effect_pod
        self._input_stream = input_stream
        super().__init__(label=label)

    # ------------------------------------------------------------------
    # StreamBase interface
    # ------------------------------------------------------------------

    @property
    def producer(self) -> SideEffectPod:  # type: ignore[override]
        return self._pod

    @property
    def upstreams(self) -> tuple[StreamProtocol, ...]:
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
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        return self._input_stream.output_schema(columns=columns, all_info=all_info)

    def keys(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        return self._input_stream.keys(columns=columns, all_info=all_info)

    def iter_data(self) -> Iterator[tuple[TagProtocol, DataProtocol]]:
        """Standalone iteration — executes delivery with no DB logging."""
        for tag, data in self._input_stream.iter_data():
            result = _execute_side_effect_row(
                fn=self._pod._fn,
                tag=tag,
                data=data,
                pod_config=self._pod.pod_config,
                pipeline_hash_ch=self.pipeline_hash(),
                node_content_hash_str=self._pod.content_hash().to_string(),
                pod_name=self._pod.label,
                run_id=None,
                arrow_hasher=self._pod.data_context.arrow_hasher,
                pipeline_database=None,
                table_path=None,
            )
            if result is not None:
                yield result

    def as_table(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> pa.Table:
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
            fields = {}
            for name, py_type in {**tag_schema, **data_schema}.items():
                fields[name] = pa.array(
                    [], type=tc.python_type_to_arrow_type(py_type)
                )
            return pa.table(fields)
        return arrow_utils.hstack_tables(
            pa.concat_tables(tag_tables),
            pa.concat_tables(data_tables),
        )

    @property
    def node_uri(self) -> tuple[str, ...]:
        """Canonical URI tuple identifying this side-effect node.

        Identical to ``side_effect_pod.uri`` at runtime, following the same
        convention as ``FunctionNode.node_uri`` and ``OperatorNode.node_uri``.
        """
        return self._pod.uri


# ---------------------------------------------------------------------------
# SideEffectJobNode — DB-backed execution node
# ---------------------------------------------------------------------------


class SideEffectJobNode(SideEffectNode):
    """DB-backed execution node for side-effect pods.

    Created at pipeline compile time by ``PipelineJob``. Receives a
    ``pipeline_database`` via ``attach_databases()``. ``run_id`` is passed
    as a call-time keyword argument from the orchestrator.

    Extends ``SideEffectNode`` with DB attachment and orchestrated execution
    methods. Analogous to ``FunctionJobNode`` in the function pod hierarchy.

    Args:
        side_effect_pod: The ``SideEffectPod`` this node wraps.
        input_stream: The upstream stream at compile time.
        label: Optional display label.
    """

    def __init__(
        self,
        side_effect_pod: SideEffectPod,
        input_stream: StreamProtocol,
        label: str | None = None,
    ) -> None:
        super().__init__(side_effect_pod=side_effect_pod, input_stream=input_stream, label=label)
        self._pipeline_database: ArrowDatabaseProtocol | None = None
        self._table_path: tuple[str, ...] | None = None

    # ------------------------------------------------------------------
    # DB attachment
    # ------------------------------------------------------------------

    def attach_databases(
        self,
        pipeline_database: ArrowDatabaseProtocol | None = None,
    ) -> None:
        """Attach or detach the pipeline database.

        Called by ``PipelineJob._distribute_databases()``. The table path is
        ``self.node_uri + (f"schema:{self.pipeline_hash().to_string()}",)`` —
        the same scoping convention used by ``FunctionNode`` and
        ``OperatorNode``.

        Args:
            pipeline_database: Pre-scoped pipeline DB (at pipeline root),
                or ``None`` to detach.
        """
        self._pipeline_database = pipeline_database
        if pipeline_database is not None:
            self._table_path = self.node_uri + (
                f"schema:{self.pipeline_hash().to_string()}",
            )
        else:
            self._table_path = None

    # ------------------------------------------------------------------
    # Sync execution
    # ------------------------------------------------------------------

    def execute(
        self,
        input_stream: StreamProtocol,
        *,
        observer: ExecutionObserverProtocol | None = None,
        run_id: str | None = None,
    ) -> list[tuple[TagProtocol, DataProtocol]]:
        """Execute side-effect delivery for all rows in ``input_stream``.

        Args:
            input_stream: Stream of ``(tag, data)`` pairs to process.
            observer: Optional execution observer (currently unused).
            run_id: Pipeline run identifier forwarded from the orchestrator.

        Returns:
            List of ``(tag, data)`` tuples — the pass-through rows.
        """
        results = []
        for tag, data in input_stream.iter_data():
            result = _execute_side_effect_row(
                fn=self._pod._fn,
                tag=tag,
                data=data,
                pod_config=self._pod.pod_config,
                pipeline_hash_ch=self.pipeline_hash(),
                node_content_hash_str=self._pod.content_hash().to_string(),
                pod_name=self._pod.label,
                run_id=run_id,
                arrow_hasher=self._pod.data_context.arrow_hasher,
                pipeline_database=self._pipeline_database,
                table_path=self._table_path,
            )
            if result is not None:
                results.append(result)
        return results

    # ------------------------------------------------------------------
    # Async execution
    # ------------------------------------------------------------------

    async def async_execute(
        self,
        inputs: Sequence[ReadableChannel[tuple[TagProtocol, DataProtocol]]],
        output: WritableChannel[tuple[TagProtocol, DataProtocol]],
        *,
        observer: ExecutionObserverProtocol | None = None,
        run_id: str | None = None,
    ) -> None:
        """Async side-effect delivery with semaphore-bounded concurrency.

        Reads from ``inputs[0]``, dispatches each row as an independent
        async task via ``asyncio.TaskGroup``. A semaphore caps in-flight
        tasks at ``pod_config.max_concurrency`` (default 16) to prevent
        unbounded task accumulation on large streams. Pass
        ``max_concurrency=None`` in the pod config for unlimited concurrency.
        Emits non-``None`` results to ``output``. Always closes ``output``
        in a ``finally`` block.

        Args:
            inputs: Single-element sequence containing the input channel.
            output: Writable channel for pass-through ``(tag, data)`` pairs.
            observer: Optional execution observer (currently unused).
            run_id: Pipeline run identifier from the orchestrator.

        Raises:
            ValueError: If ``inputs`` does not contain exactly one channel.
        """
        if len(inputs) != 1:
            raise ValueError(
                f"SideEffectJobNode.async_execute expects exactly 1 input channel; "
                f"got {len(inputs)}."
            )
        max_concurrency = self._pod.pod_config.max_concurrency
        sem = asyncio.Semaphore(max_concurrency) if max_concurrency is not None else None

        try:
            async def process_one(tag: TagProtocol, data: DataProtocol) -> None:
                try:
                    result = _execute_side_effect_row(
                        fn=self._pod._fn,
                        tag=tag,
                        data=data,
                        pod_config=self._pod.pod_config,
                        pipeline_hash_ch=self.pipeline_hash(),
                        node_content_hash_str=self._pod.content_hash().to_string(),
                        pod_name=self._pod.label,
                        run_id=run_id,
                        arrow_hasher=self._pod.data_context.arrow_hasher,
                        pipeline_database=self._pipeline_database,
                        table_path=self._table_path,
                    )
                    if result is not None:
                        await output.send(result)
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
