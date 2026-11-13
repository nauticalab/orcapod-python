from __future__ import annotations

import asyncio
import logging
from abc import abstractmethod
from collections.abc import Collection, Iterator, Sequence
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, cast

from orcapod.channels import ReadableChannel, WritableChannel
from orcapod.config import OrcapodConfig
from orcapod.contexts import DataContext
from orcapod.core.base import TraceableBase
from orcapod.core.streams.arrow_table_stream import ArrowTableStream
from orcapod.core.streams.base import (
    StreamBase,
    check_head_size,
    head_column_config,
)
from orcapod.core.streams.head import (
    STALE_NOTE,
    HeadStatus,
    StreamHead,
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
        logger.debug(f"Invoking kernel {self} on streams: {streams}")

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
        logger.debug(f"Invoking pod {self} on streams through __call__: {streams}")
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
        from orcapod.system_constants import constants
        from orcapod.utils import arrow_utils

        if not rows:
            raise ValueError("Cannot materialize an empty list of rows into a stream")

        tag_tables = [tag.as_table(columns={"system_tags": True}) for tag, _ in rows]
        data_tables = [data.as_table(columns={"source": True}) for _, data in rows]

        # Rows may list the same columns in a different order (cached vs
        # freshly computed), so match columns by name.
        combined_tags = arrow_utils.concat_tables_by_name(tag_tables)
        combined_data = arrow_utils.concat_tables_by_name(data_tables)

        data_schema = combined_data.schema
        for i, field in enumerate(data_schema):
            if field.name.startswith(constants.SOURCE_PREFIX):
                nullable = combined_data.column(i).null_count > 0
                data_schema = data_schema.set(i, field.with_nullable(nullable))  # pyright: ignore[reportUnknownMemberType]  # stubs leave Field unparametrized
        # Same arrays, new field flags: no cast, so no value or type can change.
        combined_data = pa.Table.from_arrays(combined_data.columns, schema=data_schema)

        full_table = arrow_utils.hstack_tables(combined_tags, combined_data)
        return full_table, tuple(rows[0][0].keys())

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

    def keys(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        """Return the (tag_keys, data_keys) column names for this stream."""
        tag_schema, data_schema = self._pod.output_schema(
            *self.upstreams,
            columns=columns,
            all_info=all_info,
        )
        return tuple(tag_schema.keys()), tuple(data_schema.keys())

    def output_schema(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        """Return the (tag_schema, data_schema) for this stream."""
        return self._pod.output_schema(
            *self.upstreams,
            columns=columns,
            all_info=all_info,
        )

    @property
    def last_modified(self) -> datetime | None:
        """Returns the last modified time of the stream."""
        self._update_cache_status()
        return self._cached_time

    def _update_cache_status(self) -> None:
        if self._cached_time is None:
            return

        upstream_times = [stream.last_modified for stream in self.upstreams]
        upstream_times.append(self._pod.last_modified)

        if any(t is None for t in upstream_times):
            self._cached_stream = None
            self._cached_time = None
            return

        # Get the maximum upstream time
        max_upstream_time = max(cast(list[datetime], upstream_times))

        # Invalidate cache if upstream is newer and update the cache time
        if max_upstream_time > self._cached_time:
            self._cached_stream = None
            self._cached_time = max_upstream_time

    def run(self, *args: Any, **kwargs: Any) -> None:
        self._update_cache_status()

        # recompute if cache is invalid
        if self._cached_time is None or self._cached_stream is None:
            self._cached_stream = self._pod.static_process(
                *self.upstreams,
            )
            self._cached_time = datetime.now(timezone.utc)

    def as_table(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> "pa.Table":
        self.run()
        assert self._cached_stream is not None, (
            "StreamProtocol has not been updated or is empty."
        )
        return self._cached_stream.as_table(columns=columns, all_info=all_info)

    def iter_data(
        self,
    ) -> Iterator[tuple[TagProtocol, DataProtocol]]:
        self.run()
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
    ) -> StreamHead:
        """Return the first rows of the operator's kept result; never runs the operator.

        The head of an operator is not the operator over the heads of its inputs (a
        group over ten rows has the wrong members), so before the result is computed,
        or after an input changed, there are no rows. See ``StreamBase.head``.
        """
        check_head_size(n)
        config = head_column_config(columns, all_info)
        had_result = self._cached_stream is not None
        self._update_cache_status()
        if self._cached_stream is not None and self._cached_time is not None:
            return head_of(
                self._cached_stream.head(n, columns=config), self, origin="computed"
            )
        return self._head_without_rows(
            config,
            HeadStatus.NOT_COMPUTED,
            note=STALE_NOTE if had_result else None,
        )

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(kernel={self.producer}, upstreams={self.upstreams})"
