from __future__ import annotations

import asyncio
import logging
from abc import abstractmethod
from collections.abc import Collection, Iterator, Sequence
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, cast

from orcapod.channels import ReadableChannel, WritableChannel
from orcapod.config import Config
from orcapod.contexts import DataContext
from orcapod.core.base import TraceableBase
from orcapod.core.streams.base import StreamBase
from orcapod.core.tracker import DEFAULT_TRACKER_MANAGER
from orcapod.protocols.core_protocols import (
    ArgumentGroup,
    PacketProtocol,
    PodProtocol,
    StreamProtocol,
    TagProtocol,
    TrackerManagerProtocol,
)
from orcapod.types import ColumnConfig, Schema
from orcapod.utils.lazy_module import LazyModule

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")


class StaticOutputPod(TraceableBase):
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
        """Determine output (tag, packet) schemas without triggering computation.

        Args:
            *streams: Input streams to analyze.
            columns: Column configuration for included column groups.
            all_info: If True, include all info columns.

        Returns:
            A ``(tag_schema, packet_schema)`` tuple.

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

    def __call__(self, *streams: StreamProtocol, **kwargs) -> DynamicPodStream:
        """Convenience alias for ``process``."""
        logger.debug(f"Invoking pod {self} on streams through __call__: {streams}")
        # perform input stream validation
        return self.process(*streams, **kwargs)

    # ------------------------------------------------------------------
    # Async channel execution (default barrier mode)
    # ------------------------------------------------------------------

    @staticmethod
    def _materialize_to_stream(
        rows: list[tuple[TagProtocol, PacketProtocol]],
    ) -> StreamProtocol:
        """Materialize a list of (Tag, Packet) pairs into an ArrowTableStream.

        Used by the barrier-mode ``async_execute`` to convert collected
        channel items back into a stream suitable for ``static_process``.
        """
        from orcapod.core.datagrams import Tag
        from orcapod.core.streams.arrow_table_stream import ArrowTableStream
        from orcapod.utils import arrow_utils

        if not rows:
            raise ValueError("Cannot materialize an empty list of rows into a stream")

        tag_tables = []
        packet_tables = []
        source_info_dicts: list[dict[str, str | None]] = []

        for tag, packet in rows:
            tag_tables.append(tag.as_table(columns={"system_tags": True}))
            packet_tables.append(packet.as_table())
            source_info_dicts.append(packet.source_info())

        combined_tags = pa.concat_tables(tag_tables)
        combined_packets = pa.concat_tables(packet_tables)

        # Determine which columns are user tags vs system tags
        first_tag = rows[0][0]
        if isinstance(first_tag, Tag):
            user_tag_keys = tuple(first_tag.keys())
        else:
            user_tag_keys = tuple(first_tag.keys())

        # Build source_info: for each packet column, use the source info
        # from the first row (all rows should have the same packet columns)
        source_info: dict[str, str | None] = {}
        if source_info_dicts:
            for key in source_info_dicts[0]:
                source_info[key] = None

        full_table = arrow_utils.hstack_tables(combined_tags, combined_packets)

        return ArrowTableStream(
            full_table,
            tag_columns=user_tag_keys,
            source_info=source_info,
        )

    async def async_execute(
        self,
        inputs: Sequence[ReadableChannel[tuple[TagProtocol, PacketProtocol]]],
        output: WritableChannel[tuple[TagProtocol, PacketProtocol]],
    ) -> None:
        """Default barrier-mode async execution.

        Collects all inputs, runs ``static_process``, emits results.
        Subclasses override for streaming or incremental strategies.
        """
        try:
            all_rows = await asyncio.gather(*(ch.collect() for ch in inputs))
            streams = [self._materialize_to_stream(rows) for rows in all_rows]
            result = self.static_process(*streams)
            for tag, packet in result.iter_packets():
                await output.send((tag, packet))
        finally:
            await output.close()


class DynamicPodStream(StreamBase):
    """Recomputable stream wrapping a ``StaticOutputPod`` invocation."""

    def __init__(
        self,
        pod: StaticOutputPod,
        upstreams: tuple[StreamProtocol, ...] = (),
        label: str | None = None,
        data_context: DataContext | None = None,
        config: Config | None = None,
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
        """Return the (tag_keys, packet_keys) column names for this stream."""
        tag_schema, packet_schema = self._pod.output_schema(
            *self.upstreams,
            columns=columns,
            all_info=all_info,
        )
        return tuple(tag_schema.keys()), tuple(packet_schema.keys())

    def output_schema(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        """Return the (tag_schema, packet_schema) for this stream."""
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
            self._cached_results = None
            self._cached_time = None
            return

        # Get the maximum upstream time
        max_upstream_time = max(cast(list[datetime], upstream_times))

        # Invalidate cache if upstream is newer and update the cache time
        if max_upstream_time > self._cached_time:
            self._cached_results = None
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

    def iter_packets(
        self,
    ) -> Iterator[tuple[TagProtocol, PacketProtocol]]:
        self.run()
        assert self._cached_stream is not None, (
            "StreamProtocol has not been updated or is empty."
        )
        return self._cached_stream.iter_packets()

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(kernel={self.producer}, upstreams={self.upstreams})"
