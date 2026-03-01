from __future__ import annotations

import logging
from abc import abstractmethod
from collections.abc import Collection, Iterator
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, cast

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
    """
    Abstract Base class for pods with core logic that yields static output stream.
    The static output stream will be wrapped in DynamicPodStream which will re-execute
    the pod as necessary to ensure that the output stream is up-to-date.

    Furthermore, the invocation of the pod will be tracked by the tracker manager, registering
    the pod as a general pod invocation.
    """

    def __init__(
        self, tracker_manager: TrackerManagerProtocol | None = None, **kwargs
    ) -> None:
        self.tracker_manager = tracker_manager or DEFAULT_TRACKER_MANAGER
        super().__init__(**kwargs)

    def pipeline_identity_structure(self) -> Any:
        """
        Pipeline identity for operators defaults to their content identity structure.
        Operators are stateless — their pipeline identity IS their content identity.
        """
        return self.identity_structure()

    @property
    def uri(self) -> tuple[str, ...]:
        """
        Returns a unique resource identifier for the pod.
        The pod URI must uniquely determine the schema for the pod
        """
        return (
            f"{self.__class__.__name__}",
            self.content_hash().to_hex(),
        )

    @abstractmethod
    def validate_inputs(self, *streams: StreamProtocol) -> None:
        """
        Validate input streams, raising exceptions if invalid.

        Should check:
        - Number of input streams
        - StreamProtocol types and schemas
        - Kernel-specific requirements
        - Business logic constraints

        Args:
            *streams: Input streams to validate

        Raises:
            PodInputValidationError: If inputs are invalid
        """
        ...

    @abstractmethod
    def argument_symmetry(self, streams: Collection[StreamProtocol]) -> ArgumentGroup:
        """
        Describe symmetry/ordering constraints on input arguments.

        Returns a structure encoding which arguments can be reordered:
        - SymmetricGroup (frozenset): Arguments commute (order doesn't matter)
        - OrderedGroup (tuple): Arguments have fixed positions
        - Nesting expresses partial symmetry

        Examples:
            Full symmetry (Join):
                return frozenset([a, b, c])

            No symmetry (Concatenate):
                return (a, b, c)

            Partial symmetry:
                return (frozenset([a, b]), c)
                # a,b are interchangeable, c has fixed position
        """
        ...

    @abstractmethod
    def output_schema(
        self,
        *streams: StreamProtocol,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        """
        Determine output types without triggering computation.

        This method performs type inference based on input stream types,
        enabling efficient type checking and stream property queries.
        It should be fast and not trigger any expensive computation.

        Used for:
        - Pre-execution type validation
        - Query planning and optimization
        - Schema inference in complex pipelines
        - IDE support and developer tooling

        Args:
            *streams: Input streams to analyze

        Returns:
            tuple[Schema, Schema]: (tag_types, packet_types) for output

        Raises:
            ValidationError: If input types are incompatible
            TypeError: If stream types cannot be processed
        """
        ...

    @abstractmethod
    def static_process(self, *streams: StreamProtocol) -> StreamProtocol:
        """
        Executes the pod on the input streams, returning a new static output stream.
        The output of execute is expected to be a static stream and thus only represent
        instantaneous computation of the pod on the input streams.

        Concrete subclass implementing a PodProtocol should override this method to provide
        the pod's unique processing logic.

        Args:
            *streams: Input streams to process

        Returns:
            cp.StreamProtocol: The resulting output stream
        """
        ...

    def process(
        self, *streams: StreamProtocol, label: str | None = None
    ) -> DynamicPodStream:
        """
        Invoke the pod on a collection of streams, returning a KernelStream
        that represents the computation.

        Args:
            *streams: Input streams to process

        Returns:
            cp.StreamProtocol: The resulting output stream
        """
        logger.debug(f"Invoking kernel {self} on streams: {streams}")

        # perform input stream validation
        self.validate_inputs(*streams)
        self.tracker_manager.record_pod_invocation(self, upstreams=streams, label=label)
        output_stream = DynamicPodStream(
            pod=self,
            upstreams=streams,
        )
        return output_stream

    def __call__(self, *streams: StreamProtocol, **kwargs) -> DynamicPodStream:
        """
        Convenience method to invoke the pod process on a collection of streams,
        """
        logger.debug(f"Invoking pod {self} on streams through __call__: {streams}")
        # perform input stream validation
        return self.process(*streams, **kwargs)


class DynamicPodStream(StreamBase):
    """
    Recomputable stream wrapping a StaticOutputPod

    This stream is used to represent the output of a StaticOutputPod invocation.

    """

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
        """
        Clears the cached stream.
        This is useful for re-processing the stream with the same pod.
        """
        self._cached_stream = None
        self._cached_time = None

    def keys(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        """
        Returns the keys of the tag and packet columns in the stream.
        """
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
        """
        Returns the schemas of the tag and packet columns in the stream.
        """
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
