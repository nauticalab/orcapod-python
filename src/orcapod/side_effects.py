# src/orcapod/side_effects.py
"""SideEffectPod — pass-through pipeline node for side effects.

Provides ``SideEffectPod``, ``SideEffectPodStream``, ``SideEffectJobNode``,
``SideEffectPodConfig``, and the ``side_effect_pod``, ``sink_pod``, ``tap_pod``
decorators.
"""

from __future__ import annotations

import asyncio
import dataclasses
import datetime
import logging
from collections.abc import Callable, Collection, Iterator, Sequence
from typing import TYPE_CHECKING, Any, Literal

from orcapod.core.base import TraceableBase
from orcapod.core.nodes.function_node import _build_record_id_preimage
from orcapod.core.streams.base import (
    StreamBase,
    check_head_size,
)
from orcapod.core.streams.view import (
    SIDE_EFFECTS_NOTE,
    ViewStatus,
    StreamView,
)
from orcapod.core.tracker import DEFAULT_TRACKER_MANAGER
from orcapod.errors import (
    StoreError,
    StoreReadError,
    StoreWriteError,
)
from orcapod.contexts.stored import (
    StaleTables,
    account_for_stale_tables,
    find_stale_tables,
    require_same_data_context,
    with_data_context,
)
from orcapod.invocation import (
    InvocationContext,
    InvocationHashConfig,
)
from orcapod.system_constants import (
    OLD_TRACKING_DB_SCHEMA_VERSIONS,
    TRACKING_DB_SCHEMA_VERSION,
)
from orcapod.types import ColumnConfig
from orcapod.utils import arrow_utils
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
    from orcapod.types import ContentHash, Schema
else:
    pa = LazyModule("pyarrow")

logger = logging.getLogger(__name__)

# Column name used in the record-ID preimage table (same semantic as
# ``_PIPELINE_RECOMPUTATION_INDEX_COL`` in ``function_node.py``).
_SIDE_EFFECT_RECOMPUTATION_INDEX_COL = "__pipeline_recomputation_index"


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

    def head(
        self,
        n: int = 10,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> StreamView:
        """Return no rows: showing this stream must never deliver its side effects."""
        check_head_size(n)
        config = ColumnConfig.handle_config(columns, all_info=all_info)
        return self._head_without_rows(
            config, ViewStatus.NOT_COMPUTED, note=SIDE_EFFECTS_NOTE
        )

    # The rows a run() view shows were passed on while delivering, not computed.
    _RUN_ORIGIN: str = "delivered"

    def run(self) -> StreamView:
        """Deliver the side effects for every row and return a ``StreamView`` of the rows.

        Does exactly what ``as_table()`` does: every call delivers every side effect
        again (no invocation log is written), because the stream keeps no rows. The view holds the rows that call
        passed on. ``head()`` and the notebook preview never deliver, so they keep showing
        no rows.

        Returns:
            A ``StreamView`` of the rows this call delivered for.
        """
        return super().run()

    def iter_data(self) -> Iterator[tuple[TagProtocol, DataProtocol]]:
        for tag, data in self._input_stream.iter_data():
            result = _execute_side_effect_row(
                fn=self._pod._fn,
                tag=tag,
                data=data,
                pod_config=self._pod.pod_config,
                pipeline_hash_ch=self.pipeline_hash(),
                pod_name=self._pod.label,
                run_id=None,
                arrow_hasher=self._pod.data_context.arrow_hasher,
                data_context_key=self._pod.data_context_key,
                ctx_arg_name=self._pod.ctx_arg_name,
            )
            if result is not None:
                yield result

    def as_table(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> pa.Table:
        column_config = ColumnConfig.handle_config(columns, all_info=all_info)
        return _table_of_rows(
            self.iter_data(),
            column_config,
            tag_keys=lambda: self.keys()[0],
            declared_schema=lambda: self.output_schema(all_info=True),
            type_converter=self._pod.data_context.type_converter,
        )


def _table_of_rows(
    rows: Iterator[tuple[TagProtocol, DataProtocol]],
    column_config: ColumnConfig,
    *,
    tag_keys: Callable[[], tuple[str, ...]],
    declared_schema: Callable[[], tuple[Schema, Schema]],
    type_converter: Any,
) -> pa.Table:
    """Return the delivered rows as one table with the column groups of ``column_config``.

    The one place the side-effect stream and node build their table (ITL-1066). Every
    requested column group is collected once per row (the tag's system tags, the data's
    source info, meta and context) and the shared shaping helpers then choose the groups, sort
    and attach the content hash, as the function pod streams do. A row's content hash is
    that of its own data, and it stays with its row when the rows are sorted.

    Args:
        rows: The (tag, data) pairs to tabulate.
        column_config: The column groups to keep, and whether to sort by tags.
        tag_keys: Returns the names of the regular tag columns. Called after the rows were
            read: asking a source for its keys can poll it.
        declared_schema: Returns the declared (tag, data) schemas with every group. Called
            only for the table of no rows: asking a source for its schema can poll it.
        type_converter: The data context's type converter, to type the table of no rows.
    """
    tag_tables: list[pa.Table] = []
    data_tables: list[pa.Table] = []
    hashes: list[str] = []
    for tag, data in rows:
        tag_tables.append(tag.as_table(columns={"system_tags": True}))
        # Only the requested auxiliary groups, so a group that is not shown cannot
        # make the rows' tables differ; the hash is that of the data, whatever is shown.
        data_tables.append(
            data.as_table(
                columns={
                    "source": column_config.source,
                    "meta": column_config.meta,
                    "context": column_config.context,
                }
            )
        )
        hashes.append(data.content_hash().to_string())
    if tag_tables:
        full = arrow_utils.hstack_tables(
            pa.concat_tables(tag_tables), pa.concat_tables(data_tables)
        )
    else:
        # No rows: the declared columns, with the declared field nullability.
        tag_schema, data_schema = declared_schema()
        full = arrow_utils.make_empty_table(
            {**tag_schema, **data_schema}, type_converter
        )

    def shape(carried: pa.Table) -> pa.Table:
        return arrow_utils.apply_column_config(carried, column_config, tag_keys())

    return arrow_utils.with_content_hash_column(
        full,
        column_config.content_hash,
        lambda: pa.array(hashes, type=pa.large_string()),
        shape,
    )


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
    pod_name: str,
    run_id: str | None,
    arrow_hasher: Any,
    data_context_key: str,
    ctx_arg_name: str = "ctx",
    pipeline_database: ArrowDatabaseProtocol | None = None,
    table_path: tuple[str, ...] | None = None,
) -> tuple[TagProtocol, DataProtocol] | None:
    """Execute delivery for one (tag, data) row.

    Computes a deterministic ``record_id`` from a preimage built by
    ``_build_record_id_preimage(tag, data)`` plus a fixed recomputation index
    of ``0``.  The preimage covers:

    * tag system-tag columns,
    * ``INPUT_DATA_HASH_COL`` (the raw digest of ``data.content_hash()``, no label), and
    * ``_SIDE_EFFECT_RECOMPUTATION_INDEX_COL`` fixed at ``0`` (side effects
      never recompute).

    The invocation hash is ``pipeline_hash :: record_id_hash``, where
    ``pipeline_hash`` uniquely identifies the table path and ``record_id_hash``
    uniquely identifies the entry within that table.

    Args:
        fn: The side-effect callable receiving data fields as kwargs plus ``InvocationContext``.
        tag: Tag for this row.
        data: Data for this row.
        pod_config: Pod-level configuration.
        pipeline_hash_ch: Pipeline hash of the node (for invocation_hash c1).
        pod_name: Label of the pod.
        run_id: Pipeline run identifier (or ``None`` in standalone mode).
        arrow_hasher: The ``arrow_hasher`` from the pod's data context.
        data_context_key: Key of the pod's data context, stored on the log row.
        pipeline_database: Attached DB (or ``None`` for standalone mode).
        table_path: Path tuple for the invocation log table.

    Returns:
        ``(tag, data)`` to emit downstream, or ``None`` to drop the row.
    """
    require_same_data_context(data.data_context_key, data_context_key)

    # 1. Build the preimage — delegates to the shared helper used by FunctionJobNode.
    #    Appends the recomputation index (fixed at 0; side effects never recompute).
    preimage = _build_record_id_preimage(tag, data).append_column(
        _SIDE_EFFECT_RECOMPUTATION_INDEX_COL,
        pa.array([0], type=pa.int32()),
    )
    record_id_hash: ContentHash = arrow_hasher.hash_table(preimage)
    record_id: bytes = record_id_hash.to_prefixed_digest()

    # 2. Completion check — look up by deterministic record_id.
    if (
        pod_config.track_completion
        and pipeline_database is not None
        and table_path is not None
    ):
        try:
            prior = pipeline_database.get_record_by_id(table_path, record_id)
        except StoreError as exc:
            # A failed lookup fails the run like a failed write: name pod and row.
            exc.add_context(node_label=pod_name, tag=tag.as_dict())
            raise
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
    data_dict = data.as_dict()
    if ctx_arg_name in data_dict:
        raise ValueError(
            f"ctx_arg_name {ctx_arg_name!r} collides with a data column of the same name. "
            f"Choose a different ctx_arg_name or rename the data column."
        )
    try:
        fn(**{ctx_arg_name: ctx, **data_dict})
        if pipeline_database is not None and table_path is not None:
            _write_invocation_row(
                pipeline_database=pipeline_database,
                table_path=table_path,
                record_id=record_id,
                record_id_hash_bytes=record_id_hash.to_prefixed_digest(),
                run_id=run_id,
                data_context_key=data_context_key,
            )
        return (tag, data)
    except StoreError as exc:
        # A failed store read or write fails the run, even under on_error="log".
        exc.add_context(node_label=pod_name, tag=tag.as_dict())
        raise
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
    record_id_hash_bytes: bytes,
    run_id: str | None,
    data_context_key: str,
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
        record_id_hash_bytes: Binary prefixed digest of the record-ID hash
            (stored for human inspection via ``ContentHash.from_prefixed_digest``).
        run_id: Pipeline run identifier (or ``None`` for standalone mode).
        data_context_key: Key of the data context whose hasher produced
            ``record_id``; stored on the row as ``__data_context``.
    """
    executed_at = datetime.datetime.now(datetime.timezone.utc)
    record = pa.table(
        {
            "record_id_hash": pa.array([record_id_hash_bytes], type=pa.large_binary()),
            "pipeline_run_id": pa.array([run_id], type=pa.large_string()),
            "executed_at": pa.array(
                [executed_at],
                type=pa.timestamp("us", tz="UTC"),
            ),
        }
    )
    try:
        pipeline_database.add_record(
            table_path,
            record_id,
            with_data_context(record, data_context_key),
            skip_duplicates=True,
        )
    except StoreReadError:
        # The stored table could not be read: report the read failure itself.
        raise
    except Exception as exc:
        raise StoreWriteError(table_path) from exc


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
        ctx_arg_name: str = "ctx",
    ) -> None:
        super().__init__(label=label, data_context=data_context)
        self._fn = fn
        self._name: str = (
            name if name is not None else getattr(fn, "__name__", "unknown")
        )
        self._pod_config = config or SideEffectPodConfig()
        self.tracker_manager = tracker_manager or DEFAULT_TRACKER_MANAGER
        self._ctx_arg_name: str = ctx_arg_name

    @property
    def ctx_arg_name(self) -> str:
        """Parameter name injected with ``InvocationContext`` on each call."""
        return self._ctx_arg_name

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
        return (
            self.uri,
            self._ctx_arg_name,
            self._pod_config.track_completion,
            self._pod_config.drop_on_failure,
        )

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
    ctx_arg_name: str = "ctx",
) -> SideEffectPod | Callable[[Callable], SideEffectPod]:
    """Decorator that wraps a callable as a ``SideEffectPod``.

    Supports both bare (``@side_effect_pod``) and parameterised
    (``@side_effect_pod(config=...)``) usage.

    Args:
        fn: The callable to wrap (when used as a bare decorator).
        config: Optional ``SideEffectPodConfig`` to apply.
        name: Optional canonical function name override (see ``SideEffectPod``).
        ctx_arg_name: Name of the parameter that receives ``InvocationContext``.
            Defaults to ``"ctx"``.

    Returns:
        A ``SideEffectPod`` (bare usage) or a decorator (parameterised usage).
    """

    def _wrap(f: Callable) -> SideEffectPod:
        return SideEffectPod(f, config=config, name=name, ctx_arg_name=ctx_arg_name)

    if fn is not None:
        return _wrap(fn)
    return _wrap


def sink_pod(
    fn: Callable | None = None,
    *,
    config: SideEffectPodConfig | None = None,
    name: str | None = None,
    ctx_arg_name: str = "ctx",
) -> SideEffectPod | Callable[[Callable], SideEffectPod]:
    """Decorator preset: ``track_completion=True``, ``drop_on_failure=True``.

    Caller-supplied ``config`` fields override the presets. Supports both
    bare (``@sink_pod``) and parameterised (``@sink_pod(config=...)``) usage.

    Args:
        fn: The callable to wrap (bare usage).
        config: Optional config override.
        name: Optional canonical function name override (see ``SideEffectPod``).
        ctx_arg_name: Name of the parameter that receives ``InvocationContext``.
            Defaults to ``"ctx"``.

    Returns:
        A ``SideEffectPod`` or decorator.
    """
    preset = SideEffectPodConfig(track_completion=True, drop_on_failure=True)
    effective_config = _merge_config(preset, config)

    def _wrap(f: Callable) -> SideEffectPod:
        return SideEffectPod(
            f, config=effective_config, name=name, ctx_arg_name=ctx_arg_name
        )

    if fn is not None:
        return _wrap(fn)
    return _wrap


def tap_pod(
    fn: Callable | None = None,
    *,
    config: SideEffectPodConfig | None = None,
    name: str | None = None,
    ctx_arg_name: str = "ctx",
) -> SideEffectPod | Callable[[Callable], SideEffectPod]:
    """Decorator preset: ``track_completion=False``, ``drop_on_failure=False``.

    Caller-supplied ``config`` fields override the presets. Supports both
    bare (``@tap_pod``) and parameterised (``@tap_pod(config=...)``) usage.

    Args:
        fn: The callable to wrap (bare usage).
        config: Optional config override.
        name: Optional canonical function name override (see ``SideEffectPod``).
        ctx_arg_name: Name of the parameter that receives ``InvocationContext``.
            Defaults to ``"ctx"``.

    Returns:
        A ``SideEffectPod`` or decorator.
    """
    preset = SideEffectPodConfig(track_completion=False, drop_on_failure=False)
    effective_config = _merge_config(preset, config)

    def _wrap(f: Callable) -> SideEffectPod:
        return SideEffectPod(
            f, config=effective_config, name=name, ctx_arg_name=ctx_arg_name
        )

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

    def head(
        self,
        n: int = 10,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> StreamView:
        """Return no rows: showing this stream must never deliver its side effects."""
        check_head_size(n)
        config = ColumnConfig.handle_config(columns, all_info=all_info)
        return self._head_without_rows(
            config, ViewStatus.NOT_COMPUTED, note=SIDE_EFFECTS_NOTE
        )

    # The rows a run() view shows were passed on while delivering, not computed.
    _RUN_ORIGIN: str = "delivered"

    def run(self) -> StreamView:
        """Deliver the side effects for every row and return a ``StreamView`` of the rows.

        Does exactly what ``as_table()`` does: every call delivers every side effect
        again, standalone, with no invocation log, also on a ``SideEffectJobNode`` (a job run logs its deliveries through ``execute``), because the stream keeps no rows. The view holds the rows that call
        passed on. ``head()`` and the notebook preview never deliver, so they keep showing
        no rows.

        Returns:
            A ``StreamView`` of the rows this call delivered for.
        """
        return super().run()

    def iter_data(self) -> Iterator[tuple[TagProtocol, DataProtocol]]:
        """Standalone iteration — executes delivery with no DB logging."""
        for tag, data in self._input_stream.iter_data():
            result = _execute_side_effect_row(
                fn=self._pod._fn,
                tag=tag,
                data=data,
                pod_config=self._pod.pod_config,
                pipeline_hash_ch=self.pipeline_hash(),
                pod_name=self._pod.label,
                run_id=None,
                arrow_hasher=self._pod.data_context.arrow_hasher,
                data_context_key=self._pod.data_context_key,
                ctx_arg_name=self._pod.ctx_arg_name,
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
        column_config = ColumnConfig.handle_config(columns, all_info=all_info)
        return _table_of_rows(
            self.iter_data(),
            column_config,
            tag_keys=lambda: self.keys()[0],
            declared_schema=lambda: self.output_schema(all_info=True),
            type_converter=self._pod.data_context.type_converter,
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
        super().__init__(
            side_effect_pod=side_effect_pod, input_stream=input_stream, label=label
        )
        self._pipeline_database: ArrowDatabaseProtocol | None = None
        self._table_path: tuple[str, ...] | None = None
        self._unlisted_store_reported = False
        self._current_log_checked = False

    def _refuse_log_of_another_data_context(self) -> None:
        """Refuse an invocation log holding deliveries of another data context.

        A delivery already made is found by a record id hashed under this node's
        data context, so deliveries logged under another one would not be found and
        every side effect would be delivered again (ITL-944). Two cases: a log at an
        older table layout (written by Orcapod up to v0.2), and rows of another data
        context in the current log. An older log is looked for at this node's own
        path and, because the hash in the path can differ between versions, anywhere
        under the pod's name in the store (ITL-954). Checked at the start of every
        run, so a log that appears between two runs of one node object is found too.
        The current log is read and counted by data context the first time only;
        the later runs list the store, which reads no table.

        Raises:
            SideEffectLogDataContextError: If such deliveries are found; nothing has
                been delivered or written then.
        """
        if self._pipeline_database is None or self._table_path is None:
            return
        current = self._pod.data_context_key
        found = find_stale_tables(
            self._pipeline_database,
            tuple(
                self._table_path[:-1] + (layout,)
                for layout in OLD_TRACKING_DB_SCHEMA_VERSIONS
            ),
            current=None if self._current_log_checked else (self._table_path, current),
            # ("side_effects", <pod name>): the part of the path that holds no hash.
            name_path=self.node_uri,
        )
        if found.unlisted:
            # The check runs before every run; that the store cannot list its tables
            # is reported once per node object and attached database.
            if self._unlisted_store_reported:
                found = StaleTables(tables=found.tables)
            self._unlisted_store_reported = True
        try:
            account_for_stale_tables(
                found, delivers_side_effects=True, current_context=current
            )
        except StoreError as exc:
            exc.add_context(node_label=self.label)
            raise
        self._current_log_checked = True

    # ------------------------------------------------------------------
    # DB attachment
    # ------------------------------------------------------------------

    def attach_databases(
        self,
        pipeline_database: ArrowDatabaseProtocol | None = None,
    ) -> None:
        """Attach or detach the pipeline database.

        Called by ``PipelineJob._distribute_databases()``. The table path is
        ``self.node_uri + (f"schema:{self.pipeline_hash().to_hex()}", TRACKING_DB_SCHEMA_VERSION)``
        — scoped by pipeline hash and tdb schema version.

        Args:
            pipeline_database: Pre-scoped pipeline DB (at pipeline root),
                or ``None`` to detach.
        """
        self._pipeline_database = pipeline_database
        self._unlisted_store_reported = False
        self._current_log_checked = False
        if pipeline_database is not None:
            self._table_path = self.node_uri + (
                f"schema:{self.pipeline_hash().to_hex()}",
                TRACKING_DB_SCHEMA_VERSION,
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

        Raises:
            SideEffectLogDataContextError: If the invocation log holds deliveries
                logged under another data context; nothing is delivered then.
        """
        self._refuse_log_of_another_data_context()
        results = []
        for tag, data in input_stream.iter_data():
            result = _execute_side_effect_row(
                fn=self._pod._fn,
                tag=tag,
                data=data,
                pod_config=self._pod.pod_config,
                pipeline_hash_ch=self.pipeline_hash(),
                pod_name=self._pod.label,
                run_id=run_id,
                arrow_hasher=self._pod.data_context.arrow_hasher,
                data_context_key=self._pod.data_context_key,
                ctx_arg_name=self._pod.ctx_arg_name,
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
        sem = (
            asyncio.Semaphore(max_concurrency) if max_concurrency is not None else None
        )

        try:
            self._refuse_log_of_another_data_context()

            async def process_one(tag: TagProtocol, data: DataProtocol) -> None:
                try:
                    result = _execute_side_effect_row(
                        fn=self._pod._fn,
                        tag=tag,
                        data=data,
                        pod_config=self._pod.pod_config,
                        pipeline_hash_ch=self.pipeline_hash(),
                        pod_name=self._pod.label,
                        run_id=run_id,
                        arrow_hasher=self._pod.data_context.arrow_hasher,
                        data_context_key=self._pod.data_context_key,
                        ctx_arg_name=self._pod.ctx_arg_name,
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
