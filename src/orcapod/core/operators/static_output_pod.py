from __future__ import annotations

import asyncio
import logging
from abc import abstractmethod
from collections.abc import Collection, Iterator, Sequence
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, ClassVar, cast

from orcapod.channels import ReadableChannel, WritableChannel
from orcapod import contexts
from orcapod.config import OrcapodConfig
from orcapod.contexts import DataContext
from orcapod.core.base import TraceableBase
from orcapod.core.function_pod import FunctionPodStream
from orcapod.core.streams.arrow_table_stream import ArrowTableStream
from orcapod.core.streams.base import (
    ZERO_ROW_INPUT_COLUMNS,
    StreamBase,
    check_head_size,
    short_stream_repr,
    typed_zero_row_input,
    typed_zero_row_table,
)
from orcapod.core.streams.view import (
    STALE_NOTE,
    ViewStatus,
    StreamView,
    head_of,
)
from orcapod.core.tracker import DEFAULT_TRACKER_MANAGER
from orcapod.protocols.core_protocols import (
    ArgumentGroup,
    DataProtocol,
    PodProtocol,
    StreamProtocol,
    TagProtocol,
    TrackerManagerProtocol,
)
from orcapod.types import ColumnConfig, ContentHash, Schema
from orcapod.utils.lazy_module import LazyModule

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    import pyarrow as pa

    from orcapod.protocols.hashing_protocols import SemanticHasherProtocol
else:
    pa = LazyModule("pyarrow")


class StaticOutputOperatorPod(TraceableBase):
    """Abstract base class for pods whose core logic yields a static output stream.

    The static output stream is wrapped in ``DynamicPodStream`` which re-executes
    the pod as necessary to keep the output up-to-date.  Pod invocations are
    tracked by the tracker manager.
    """

    OUTPUT_SCHEMA_IS_INPUT_SCHEMA: ClassVar[bool] = False
    """Whether the output has exactly its single input's columns and Arrow types.

    Declared by an operator that only chooses which rows exist (``PolarsFilter``). Its
    zero-row table is then its input's (``zero_row_result``), got without evaluating a
    predicate, so no user code runs to learn the column types (ITL-912).
    """

    def __init__(
        self, tracker_manager: TrackerManagerProtocol | None = None, **kwargs
    ) -> None:
        self.tracker_manager = tracker_manager or DEFAULT_TRACKER_MANAGER
        super().__init__(**kwargs)

    def pipeline_identity_structure(self) -> Any:
        """Return the pipeline identity, which defaults to content identity for operators."""
        return self.identity_structure()

    @property
    def uri(self) -> tuple[str, ...]:
        """Return a unique resource identifier for the pod."""
        return (
            f"{self.__class__.__name__}",
            self.content_hash().to_hex(),
        )

    @abstractmethod
    def validate_inputs(self, *streams: StreamProtocol) -> None:
        """Validate input streams, raising exceptions if invalid.

        Args:
            *streams: Input streams to validate.

        Raises:
            PodInputValidationError: If inputs are invalid.
        """
        ...

    @abstractmethod
    def argument_symmetry(self, streams: Collection[StreamProtocol]) -> ArgumentGroup:
        """Describe symmetry/ordering constraints on input arguments.

        Returns a structure encoding which arguments can be reordered:
        - ``frozenset``: Arguments commute (order doesn't matter).
        - ``tuple``: Arguments have fixed positions.
        - Nesting expresses partial symmetry.

        Examples:
            Full symmetry (Join)::

                return frozenset([a, b, c])

            No symmetry (Concatenate)::

                return (a, b, c)

            Partial symmetry::

                return (frozenset([a, b]), c)
        """
        ...

    @abstractmethod
    def output_schema(
        self,
        *streams: StreamProtocol,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        """Determine output (tag, data) schemas without triggering computation.

        Args:
            *streams: Input streams to analyze.
            columns: Column configuration for included column groups.
            all_info: If True, include all info columns.

        Returns:
            A ``(tag_schema, data_schema)`` tuple.

        Raises:
            ValidationError: If input types are incompatible.
        """
        ...

    @abstractmethod
    def static_process(self, *streams: StreamProtocol) -> StreamProtocol:
        """Execute the pod on the input streams and return a static output stream.

        Args:
            *streams: Input streams to process.

        Returns:
            The resulting output stream.
        """
        ...

    def process(
        self, *streams: StreamProtocol, label: str | None = None
    ) -> DynamicPodStream:
        """Invoke the pod on input streams and return a ``DynamicPodStream``.

        Args:
            *streams: Input streams to process.
            label: Optional label for tracking.

        Returns:
            A ``DynamicPodStream`` wrapping the computation.
        """
        logger.debug("Invoking kernel %s on streams: %s", self, streams)

        # perform input stream validation
        self.validate_inputs(*streams)
        self.tracker_manager.record_operator_pod_invocation(
            self, upstreams=streams, label=label
        )
        output_stream = DynamicPodStream(
            pod=self,
            upstreams=streams,
            label=label,
        )
        return output_stream

    def to_config(self) -> dict[str, Any]:
        """Serialize this operator to a JSON-compatible config dict.

        Subclasses with constructor parameters should override this to include
        their specific config in the ``"config"`` key.

        Returns:
            A dict with ``class_name``, ``module_path``, and ``config`` keys.
        """
        return {
            "class_name": self.__class__.__name__,
            "module_path": self.__class__.__module__,
            "config": {},
        }

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> "StaticOutputOperatorPod":
        """Reconstruct an operator from a config dict.

        Args:
            config: Dict as returned by ``to_config()``.

        Returns:
            A new instance of this operator class.
        """
        return cls(**config.get("config", {}))

    def __call__(self, *streams: StreamProtocol, **kwargs) -> DynamicPodStream:
        """Convenience alias for ``process``."""
        logger.debug("Invoking pod %s on streams through __call__: %s", self, streams)
        # perform input stream validation
        return self.process(*streams, **kwargs)

    # ------------------------------------------------------------------
    # Async channel execution (default barrier mode)
    # ------------------------------------------------------------------

    @staticmethod
    def _rows_to_table(
        rows: Sequence[tuple[TagProtocol, DataProtocol]],
    ) -> tuple[pa.Table, tuple[str, ...]]:
        """Concatenate (Tag, Data) pairs into one table, keeping per-row source info.

        Each row contributes its own system-tag and ``_source_*`` columns, so
        every row keeps its own provenance tokens (a token embeds a per-row
        identifier, so rows never share one). A ``_source_*`` field is declared
        non-nullable unless some row's token is ``None``; ``Data.as_table``
        declares every source-info field nullable, and this keeps the field
        flags (which the operator node's per-row record hash covers) what they
        were when a broadcast token made them non-nullable.

        Args:
            rows: Non-empty sequence of (Tag, Data) pairs.

        Returns:
            The concatenated table and the user tag column names.

        Raises:
            ValueError: If ``rows`` is empty.
        """
        from orcapod.utils import arrow_utils

        if not rows:
            raise ValueError("Cannot materialize an empty list of rows into a stream")

        tag_tables = [tag.as_table(columns={"system_tags": True}) for tag, _ in rows]
        data_tables = [data.as_table(columns={"source": True}) for _, data in rows]

        # Rows may list the same columns in a different order (cached vs
        # freshly computed), so match columns by name.
        combined_tags = arrow_utils.concat_tables_by_name(tag_tables)
        combined_data = arrow_utils.concat_tables_by_name(data_tables)

        combined_data = StaticOutputOperatorPod._flag_source_info_nullability(
            combined_data
        )

        full_table = arrow_utils.hstack_tables(combined_tags, combined_data)
        return full_table, tuple(rows[0][0].keys())

    @staticmethod
    def _in_row_order(table: pa.Table, tag_keys: Sequence[str]) -> pa.Table:
        """Return ``table`` with its columns in the order of a table built from rows.

        The one statement of that order (ITL-1131): user tags, system tags, data,
        source info, as ``_rows_to_table`` builds it (each row's tag table with its
        system tags, then its data table with its source info). Data columns keep their
        order in ``table``. Every table a node or an operator input holds with no rows
        takes this order, so it equals the table a run with rows gives.

        Args:
            table: A table of user tags, system tags, data and source info.
            tag_keys: The user tag columns, in order.

        Returns:
            The same columns, reordered; no column is added, dropped or changed.
        """
        from orcapod.system_constants import constants

        names = table.column_names
        system_tags = [n for n in names if n.startswith(constants.SYSTEM_TAG_PREFIX)]
        source_info = [n for n in names if n.startswith(constants.SOURCE_PREFIX)]
        placed = {*tag_keys, *system_tags, *source_info}
        data = [n for n in names if n not in placed]
        return table.select([*tag_keys, *system_tags, *data, *source_info])

    @staticmethod
    def _as_row_built_table(table: pa.Table, tag_keys: Sequence[str]) -> pa.Table:
        """Return ``table`` as ``_rows_to_table`` would build it from the same rows.

        Its columns in row order (``_in_row_order``) and each source-info field flagged
        nullable only when some row's token is ``None``
        (``_flag_source_info_nullability``). For a table with no rows: what a result
        built from rows has, with no row (ITL-1131).

        Args:
            table: A table of user tags, system tags, data and source info.
            tag_keys: The user tag columns, in order.

        Returns:
            The same arrays, reordered, under the new source-info field flags.
        """
        return StaticOutputOperatorPod._flag_source_info_nullability(
            StaticOutputOperatorPod._in_row_order(table, tag_keys)
        )

    @staticmethod
    def _zero_rows_in_row_order(result: StreamProtocol) -> StreamProtocol:
        """Return an operator's result with no rows as a run with rows would keep it.

        The zero-row counterpart of ``_materialize_to_stream``: ``result``'s zero-row
        table (user tags, system tags, data and source info, with its Arrow types and
        nullability) in the order of a table built from rows (``_in_row_order``). An
        operator's own table may order its columns otherwise (a join lists its data
        before its system tags), so a node that keeps the result of a run with no rows
        keeps this, and reads with the columns of a run with rows (ITL-1131).

        Args:
            result: An operator's result that has no rows.

        Returns:
            A stream over the reordered zero-row table, in ``result``'s data context.
        """
        tag_keys = result.keys()[0]
        table = StaticOutputOperatorPod._in_row_order(
            result.as_table(columns=ZERO_ROW_INPUT_COLUMNS), tag_keys
        )
        return ArrowTableStream(
            table, tag_columns=tag_keys, data_context=result.data_context_key
        )

    @staticmethod
    def _kept_output(
        result: StreamProtocol,
        rows: Sequence[tuple[TagProtocol, DataProtocol]],
    ) -> StreamProtocol:
        """Return what an operator node keeps, and stores, of a run of its operator.

        The rows of the run as ``_materialize_to_stream`` builds them, or, with no row,
        the result with no rows in the same order (``_zero_rows_in_row_order``). Every
        way a node runs its operator (a job run, sync or async, and ``node.run()`` on
        its own) keeps this, so a node has one column order, the row order, in memory
        and in its stored table (ITL-1133).

        Args:
            result: The operator's result of the run.
            rows: ``result``'s rows, in order.

        Returns:
            A stream over the rows, in row order.
        """
        if rows:
            return StaticOutputOperatorPod._materialize_to_stream(rows)
        return StaticOutputOperatorPod._zero_rows_in_row_order(result)

    @staticmethod
    def _flag_source_info_nullability(table: pa.Table) -> pa.Table:
        """Declare each ``_source_*`` field nullable only if some row's token is ``None``.

        The rule ``_rows_to_table`` applies to the rows it concatenates, and that a
        zero-row input (``_zero_row_input``) applies to no row: there, no token is
        ``None``, so every source-info field is non-nullable.

        Args:
            table: A table whose ``_source_*`` columns hold source-info tokens.

        Returns:
            The same arrays under the new field flags.
        """
        from orcapod.system_constants import constants

        schema = table.schema
        for i, field in enumerate(schema):
            if field.name.startswith(constants.SOURCE_PREFIX):
                nullable = table.column(i).null_count > 0
                schema = schema.set(i, field.with_nullable(nullable))  # pyright: ignore[reportUnknownMemberType]  # stubs leave Field unparametrized
        # Same arrays, new field flags: no cast, so no value or type can change.
        return pa.Table.from_arrays(table.columns, schema=schema)

    @staticmethod
    def _zero_row_input(upstream: StreamProtocol) -> StreamProtocol | None:
        """Return ``upstream`` as an operator input with no rows, typed as its rows are.

        The zero-row form of what ``_materialize_upstream`` builds from an upstream's
        rows: the upstream's zero-row table (``typed_zero_row_table``: user tags,
        system tags, data and source info, with the Arrow types its rows have) in the
        column order and with the source-info flags ``_rows_to_table`` gives, as the
        upstream's stream (``_as_upstream_stream``), so the stream's identity,
        pipeline hash and system-tag column names are the upstream's (ITL-912).

        Args:
            upstream: The node or stream an operator reads.

        Returns:
            The zero-row stream, or ``None`` when ``upstream`` cannot say its Arrow
            types without rows; the caller then builds its input from the Python
            output schema.
        """
        table = typed_zero_row_input(upstream)
        if table is None:
            return None
        tag_keys = upstream.keys()[0]
        return StaticOutputOperatorPod._as_upstream_stream(
            StaticOutputOperatorPod._as_row_built_table(table, tag_keys),
            tag_keys,
            upstream,
        )

    def zero_row_result(self, *inputs: StreamProtocol) -> StreamProtocol:
        """This operator's result over zero-row inputs, for its zero-row table (ITL-912).

        ``static_process`` over the inputs, or, for an operator whose output schema is
        its input's (``OUTPUT_SCHEMA_IS_INPUT_SCHEMA``), the input's columns in the
        large-normalized form a run gives them, so no predicate or other user code is
        evaluated. A run never comes here: it calls
        ``static_process`` (or ``process``) whether its inputs have rows or not.

        Args:
            inputs: The inputs as zero-row streams.

        Returns:
            A stream with no rows and the columns and Arrow types of the result.
        """
        if self.OUTPUT_SCHEMA_IS_INPUT_SCHEMA:
            from orcapod.utils import arrow_utils

            (only,) = inputs
            # The input's columns, each in the large-normalized form a run gives it.
            schema = only.as_table(columns=ZERO_ROW_INPUT_COLUMNS).schema
            normalized = pa.schema(
                [
                    field.with_type(
                        arrow_utils.normalize_to_large_types(field.type, keep_null=True)
                    )
                    for field in schema
                ]
            )
            return ArrowTableStream(
                normalized.empty_table(),
                tag_columns=only.keys()[0],
                data_context=only.data_context_key,
            )
        return self.static_process(*inputs)

    @staticmethod
    def _materialize_to_stream(
        rows: Sequence[tuple[TagProtocol, DataProtocol]],
    ) -> StreamProtocol:
        """Materialize a list of (Tag, Data) pairs into an ArrowTableStream.

        Holds an operator's or a node's OUTPUT rows, whose stream identity is
        given by whoever holds them (the node, the ``DynamicPodStream``), so the
        stream has no producer and its own pipeline hash is never read. Rows
        that stand for an operator's INPUT go through
        ``_materialize_input_stream``. Every row keeps its own source info (see
        ``_rows_to_table``).

        Args:
            rows: Non-empty sequence of (Tag, Data) pairs.

        Returns:
            A stream over the rows, in order.

        Raises:
            ValueError: If ``rows`` is empty.
        """
        table, tag_keys = StaticOutputOperatorPod._rows_to_table(rows)
        # No ``source_info`` argument: the table's own ``_source_*`` columns
        # pass through, one value per row.
        return ArrowTableStream(table, tag_columns=tag_keys)

    @staticmethod
    def _materialize_upstream(
        rows: Sequence[tuple[TagProtocol, DataProtocol]],
        upstream: StreamProtocol,
        *,
        zero_rows: bool = False,
    ) -> StreamProtocol:
        """Rebuild the rows an upstream node delivered as that node's stream.

        This is how an operator node's input is built in a job: the sync
        orchestrator rebuilds every upstream's buffered rows with it, and an
        async operator node rebuilds its inputs with it when its run produced
        no row (``zero_rows``). The stream takes the upstream's producer and
        upstreams, so its identity and pipeline hash are the upstream's and
        system-tag column names come out as they would on the upstream itself.

        With rows, the table is ``_rows_to_table(rows)``: every row keeps its
        own system tags and source info. Without rows, it is the upstream's
        zero-row table (``_zero_row_input``), with the Arrow types its rows have
        (ITL-912). Only an upstream that cannot say its Arrow types without rows
        (see ``typed_zero_row_table``) is built from its Python output schema
        (with system tags and source info), a known limitation (ITL-1089).

        Args:
            rows: The rows the upstream delivered; may be empty.
            upstream: The upstream node or stream the rows came from.
            zero_rows: Keep the table's columns and types but none of its rows.

        Returns:
            A stream over the rows (none when ``zero_rows``), in order.
        """
        from orcapod.utils import arrow_utils

        if rows:
            table, tag_keys = StaticOutputOperatorPod._rows_to_table(rows)
            if zero_rows:
                table = table.slice(0, 0)
            return StaticOutputOperatorPod._as_upstream_stream(
                table, tag_keys, upstream
            )
        typed = StaticOutputOperatorPod._zero_row_input(upstream)
        if typed is not None:
            return typed
        # Build an empty table preserving declared field nullability.
        tag_schema, data_schema = upstream.output_schema(
            columns={"system_tags": True, "source": True}
        )
        table = arrow_utils.make_empty_table(
            {**tag_schema, **data_schema},
            StaticOutputOperatorPod._upstream_context(upstream).type_converter,
        )
        return StaticOutputOperatorPod._as_upstream_stream(
            table, upstream.keys()[0], upstream
        )

    @staticmethod
    def _upstream_context(upstream: StreamProtocol) -> DataContext:
        """The data context of ``upstream``, which its rebuilt stream is in."""
        from orcapod.core.base import DataContextMixin

        if isinstance(upstream, DataContextMixin):
            return upstream.data_context
        return contexts.resolve_context(upstream.data_context_key)

    @staticmethod
    def _as_upstream_stream(
        table: pa.Table, tag_keys: Sequence[str], upstream: StreamProtocol
    ) -> StreamProtocol:
        """Wrap a table of ``upstream``'s rows (or none) as ``upstream``'s stream.

        Args:
            table: User tags, system tags, data and source info, without the
                context column.
            tag_keys: The user tag columns.
            upstream: The node or stream the table stands for.

        Returns:
            A stream with ``upstream``'s producer, upstreams and data context.
        """
        from orcapod.system_constants import constants

        # The rebuilt stream is in the upstream's data context, as its rows are.
        context = StaticOutputOperatorPod._upstream_context(upstream)
        # The context column names the upstream's context on every row; without
        # it, the stream would fill in the default context's key.
        table = table.append_column(
            pa.field(constants.CONTEXT_KEY, pa.large_string(), nullable=False),
            pa.array([context.context_key] * table.num_rows, pa.large_string()),
        )
        # No ``source_info`` argument: the table's own ``_source_*`` columns
        # pass through, one value per row.
        return ArrowTableStream(
            table,
            tag_columns=tag_keys,
            producer=upstream.producer,
            upstreams=upstream.upstreams,
            data_context=context,
        )

    @staticmethod
    def _materialize_input_stream(
        rows: Sequence[tuple[TagProtocol, DataProtocol]],
        pipeline_hash: ContentHash,
    ) -> StreamProtocol:
        """Rebuild an input stream from the rows a barrier ``async_execute`` collected.

        The rebuilt stream reports ``pipeline_hash`` as its own, so an operator
        that names system-tag columns after its input's pipeline hash
        (``MergeJoin``, ``Batch``, ``GroupBy``) names them as the sync path
        does (ITL-906). Every row keeps its own source info (see
        ``_rows_to_table``).

        Args:
            rows: Non-empty sequence of (Tag, Data) pairs.
            pipeline_hash: The pipeline hash of the stream the rows came from.

        Returns:
            A stream over the rows, in order.

        Raises:
            ValueError: If ``rows`` is empty.
        """
        table, tag_keys = StaticOutputOperatorPod._rows_to_table(rows)
        return _RowsOfStream(table, tag_columns=tag_keys, pipeline_hash=pipeline_hash)

    async def async_execute(
        self,
        inputs: Sequence[ReadableChannel[tuple[TagProtocol, DataProtocol]]],
        output: WritableChannel[tuple[TagProtocol, DataProtocol]],
        *,
        input_pipeline_hashes: Sequence[ContentHash],
    ) -> None:
        """Default barrier-mode async execution.

        Collects all inputs, runs ``static_process``, emits results.
        Subclasses override for streaming or incremental strategies. When an
        input has no rows, nothing is emitted (ITL-865): an operator whose
        result is not empty then must override this method.

        Every operator's ``async_execute`` takes ``input_pipeline_hashes``, with
        no default: the operator node passes it for every operator alike.

        Args:
            inputs: Readable channels, one per upstream node.
            output: Writable channel for downstream consumption.
            input_pipeline_hashes: Pipeline hash for each input stream,
                positionally matching ``inputs``. Each rebuilt input stream
                reports its hash, so system-tag column names equal the sync
                path's (ITL-906).

        Raises:
            ValueError: If ``input_pipeline_hashes`` does not have one hash per input.
        """
        try:
            if len(input_pipeline_hashes) != len(inputs):
                raise ValueError(
                    f"input_pipeline_hashes length ({len(input_pipeline_hashes)}) "
                    f"must match inputs length ({len(inputs)})"
                )
            all_rows = await asyncio.gather(*(ch.collect() for ch in inputs))
            if not all(all_rows):
                # Every barrier operator that exists gives no rows when an input has
                # none. The typed empty stream the sync path returns needs a schema
                # the channels do not carry (ITL-858), so nothing is emitted (ITL-865).
                return
            streams = [
                self._materialize_input_stream(rows, pipeline_hash)
                for rows, pipeline_hash in zip(all_rows, input_pipeline_hashes)
            ]
            result = self.static_process(*streams)
            for tag, data in result.iter_data():
                await output.send((tag, data))
        finally:
            await output.close()


class _RowsOfStream(ArrowTableStream):
    """Rows collected from a channel, standing in for the stream they came from.

    The barrier-mode ``async_execute`` rebuilds each input from its rows. A
    rebuilt ``ArrowTableStream`` has no producer, so its own pipeline hash is
    the schema-only hash of a root; this stream reports the pipeline hash of
    the stream the rows came from instead, as the operator node passes it
    (``input_pipeline_hashes``). Its content hash is that of its table, as for
    any ``ArrowTableStream`` without a producer.
    """

    def __init__(
        self,
        table: pa.Table,
        *,
        tag_columns: Collection[str],
        pipeline_hash: ContentHash,
    ) -> None:
        super().__init__(table, tag_columns=tag_columns)
        self._input_pipeline_hash: ContentHash = pipeline_hash

    def pipeline_hash(
        self, hasher: SemanticHasherProtocol | None = None
    ) -> ContentHash:
        """The pipeline hash of the stream the rows came from.

        It was computed by the operator node with the hasher of its data
        context, so it is returned whatever ``hasher`` is given.

        Args:
            hasher: Not used.

        Returns:
            The input pipeline hash the operator node passed.
        """
        return self._input_pipeline_hash


class DynamicPodStream(StreamBase):
    """Recomputable stream wrapping a ``StaticOutputOperatorPod`` invocation."""

    def __init__(
        self,
        pod: StaticOutputOperatorPod,
        upstreams: tuple[StreamProtocol, ...] = (),
        label: str | None = None,
        data_context: DataContext | None = None,
        config: OrcapodConfig | None = None,
    ) -> None:
        self._pod = pod
        self._upstreams = upstreams

        super().__init__(label=label, data_context=data_context, config=config)
        self._set_modified_time(None)
        self._cached_time: datetime | None = None
        self._cached_stream: StreamProtocol | None = None
        # Staleness bookkeeping, in memory only (never hashed, saved or stored): the
        # upstreams' states when the kept result was made (``_upstream_state``), and a
        # count of the kept results discarded, which tells a downstream operator stream
        # that this one changed (ITL-1083).
        self._kept_states: tuple[tuple[str, object], ...] | None = None
        self._reset_generation: int = 0

    def identity_structure(self) -> Any:
        structure = (self._pod,)
        if self._upstreams:
            structure += (self._pod.argument_symmetry(self._upstreams),)
        return structure

    def pipeline_identity_structure(self) -> Any:
        structure = (self._pod,)
        if self._upstreams:
            structure += (self._pod.argument_symmetry(self._upstreams),)
        return structure

    @property
    def producer(self) -> PodProtocol:
        return self._pod

    @property
    def upstreams(self) -> tuple[StreamProtocol, ...]:
        return self._upstreams

    def clear_cache(self) -> None:
        """Clear the cached stream, forcing recomputation on next access."""
        self._cached_stream = None
        self._cached_time = None
        self._kept_states = None
        self._reset_generation += 1

    def _typed_zero_row_table(self) -> pa.Table | None:
        """The pod's result over its upstreams' zero-row tables (ITL-912).

        The rule an operator node follows (``OperatorJobNode._typed_zero_row_table``),
        with each upstream given as the stream it is, as ``static_process`` reads
        it: no row is computed or read, and nothing this stream holds is consulted.
        """
        inputs: list[StreamProtocol] = []
        for upstream in self._upstreams:
            # The upstream itself with no row: the pod reads the upstream stream,
            # not rows rebuilt from it, so its columns stay in their own order.
            table = typed_zero_row_table(upstream)
            if table is None:
                return None
            inputs.append(
                ArrowTableStream(
                    table,
                    tag_columns=upstream.keys()[0],
                    producer=upstream.producer,
                    upstreams=upstream.upstreams,
                    data_context=StaticOutputOperatorPod._upstream_context(upstream),
                )
            )
        pod = cast(StaticOutputOperatorPod, self._pod)
        return pod.zero_row_result(*inputs).as_table(columns=ZERO_ROW_INPUT_COLUMNS)

    def keys(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        """Return the (tag_keys, data_keys) column names for this stream."""
        tag_schema, data_schema = self.output_schema(columns=columns, all_info=all_info)
        return tuple(tag_schema.keys()), tuple(data_schema.keys())

    def output_schema(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        """Return the (tag_schema, data_schema) for this stream."""
        # Memoized (ITL-1111). The prediction depends on the operator's configuration
        # and on the upstreams' schemas and pipeline hashes, all fixed for the life of
        # this stream; never on rows, so recomputing the rows when stale leaves it valid.
        return self._memoized_output_schema(
            columns,
            all_info,
            lambda: self._pod.output_schema(
                *self.upstreams, columns=columns, all_info=all_info
            ),
        )

    @property
    def last_modified(self) -> datetime | None:
        """Returns the last modified time of the stream."""
        self._update_cache_status()
        return self._cached_time

    def _update_cache_status(self) -> None:
        """Discard the kept result if the operator or an upstream changed since it was made.

        A pod stream or an operator stream upstream is changed when its state
        (``_upstream_state``) differs from the one noted with the kept result: its modified
        time moves when it completes its first computation, which happens after the result
        was kept when the operator returned that stream unread (a pass-through), and that is
        not a change (ITL-1083, the rule ITL-1065 set for pod streams). Any other upstream,
        and the operator, are changed when their modified time is unknown or newer.
        """
        if self._cached_time is None:
            return

        states = _upstream_states(self.upstreams)
        times = [value for kind, value in states if kind == "time"]
        times.append(self._pod.last_modified)

        if any(t is None for t in times):
            self._discard_result()
            self._cached_time = None
            return

        newest = max(cast(list[datetime], times))
        if states != self._kept_states or newest > self._cached_time:
            self._discard_result()
            # Noted so that the next check finds a change only if there is a new one.
            self._kept_states = states
            self._cached_time = datetime.now(timezone.utc)

    @property
    def is_stale(self) -> bool:
        """Whether the next read must run the operator: no kept result, or an outdated one.

        Judged as ``_update_cache_status`` judges the kept result (which it discards if
        outdated), not by comparing modified times: an upstream that completed its first
        computation after the result was kept is newer, but not changed (ITL-1083).
        """
        self._update_cache_status()
        return self._cached_stream is None

    def _discard_result(self) -> None:
        """Drop the kept result; a downstream operator stream sees the change."""
        self._cached_stream = None
        self._reset_generation += 1

    def _refresh(self) -> None:
        """Run the operator unless its kept result is current: the one computation
        path of ``as_table``, ``iter_data`` and so ``run``."""
        self._update_cache_status()

        # recompute if cache is invalid
        if self._cached_time is None or self._cached_stream is None:
            # Noted before the operator reads its inputs, so that a change made while it
            # reads them is seen by the next check.
            kept_states = _upstream_states(self.upstreams)
            self._cached_stream = self._pod.static_process(
                *self.upstreams,
            )
            self._cached_time = datetime.now(timezone.utc)
            self._kept_states = kept_states

    def as_table(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> "pa.Table":
        self._refresh()
        assert self._cached_stream is not None, (
            "StreamProtocol has not been updated or is empty."
        )
        return self._cached_stream.as_table(columns=columns, all_info=all_info)

    def iter_data(
        self,
    ) -> Iterator[tuple[TagProtocol, DataProtocol]]:
        self._refresh()
        assert self._cached_stream is not None, (
            "StreamProtocol has not been updated or is empty."
        )
        return self._cached_stream.iter_data()

    def head(
        self,
        n: int = 10,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> StreamView:
        """Return the first rows of the operator's kept result; never runs it over rows.

        The head of an operator is not the operator over the heads of its inputs (a
        group over ten rows has the wrong members), so before the result is computed,
        or after an input changed, there are no rows. See ``StreamBase.head``.
        """
        check_head_size(n)
        config = ColumnConfig.handle_config(columns, all_info=all_info)
        had_result = self._cached_stream is not None
        self._update_cache_status()
        if self._cached_stream is not None and self._cached_time is not None:
            return head_of(
                self._cached_stream.head(n, columns=config), self, origin="computed"
            )
        return self._head_without_rows(
            config,
            ViewStatus.NOT_COMPUTED,
            note=STALE_NOTE if had_result else None,
        )

    def __repr__(self) -> str:
        # Upstreams by class and label only: their own repr would nest every path (ITL-1111).
        upstreams = ", ".join(
            short_stream_repr(upstream) for upstream in self.upstreams
        )
        return (
            f"{self.__class__.__name__}(label={self.label!r}, kernel={self.producer}, "
            f"upstreams=({upstreams}))"
        )


def _upstream_state(stream: StreamProtocol) -> tuple[str, object]:
    """What tells a change of an upstream of an operator stream.

    A pod stream or an operator stream computes lazily, so its modified time moves when it
    completes its first computation, which is not a change of its content: its reset
    generation tells a change instead (ITL-1083, as ``FunctionPodStream`` judges a pod-stream
    input, ITL-1065). A stale pod stream counts the reset its next read makes. An operator
    stream is first asked for its modified time, which discards its own kept result if that
    is stale. Any other upstream (source, table stream, node) is told by its modified time.

    Args:
        stream: The upstream.

    Returns:
        ``("generation", n)`` or ``("time", modified time or None)``.
    """
    if isinstance(stream, FunctionPodStream):
        generation = stream._reset_generation  # pyright: ignore[reportPrivateUsage]  # staleness bookkeeping shared by the stream kinds, never public
        return ("generation", generation + int(stream.is_stale))
    if isinstance(stream, DynamicPodStream):
        _ = stream.last_modified
        return ("generation", stream._reset_generation)  # pyright: ignore[reportPrivateUsage]  # same class
    return ("time", stream.last_modified)


def _upstream_states(
    upstreams: tuple[StreamProtocol, ...],
) -> tuple[tuple[str, object], ...]:
    """The state of each upstream (``_upstream_state``), in order."""
    return tuple(_upstream_state(stream) for stream in upstreams)
