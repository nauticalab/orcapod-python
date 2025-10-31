from collections.abc import Collection, Iterator, Mapping
from datetime import datetime
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from orcapod.protocols.core_protocols.base import ExecutionEngine, Labelable
from orcapod.protocols.core_protocols.datagrams import Packet, Tag
from orcapod.protocols.hashing_protocols import ContentIdentifiable
from orcapod.types import PythonSchema

if TYPE_CHECKING:
    import polars as pl
    import pyarrow as pa
    import pandas as pd
    from orcapod.protocols.core_protocols.kernel import Kernel


@runtime_checkable
class Stream(ContentIdentifiable, Labelable, Protocol):
    """
    Base protocol for all streams in Orcapod.

    Streams represent sequences of (Tag, Packet) pairs flowing through the
    computational graph. They are the fundamental data structure connecting
    kernels and carrying both data and metadata.

    Streams can be either:
    - Static: Immutable snapshots created at a specific point in time
    - Live: Dynamic streams that stay current with upstream dependencies

    All streams provide:
    - Iteration over (tag, packet) pairs
    - Type information and schema access
    - Lineage information (source kernel and upstream streams)
    - Basic caching and freshness tracking
    - Conversion to common formats (tables, dictionaries)
    """

    @property
    def substream_identities(self) -> tuple[str, ...]:
        """
        Unique identifiers for sub-streams within this stream.

        This property provides a way to identify and differentiate
        sub-streams that may be part of a larger stream. It is useful
        for tracking and managing complex data flows.

        Returns:
            tuple[str, ...]: Unique identifiers for each sub-stream
        """
        ...

    @property
    def execution_engine(self) -> ExecutionEngine | None:
        """
        The execution engine attached to this stream. By default, the stream
        will use this execution engine whenever it needs to perform computation.
        None means the stream is not attached to any execution engine and will default
        to running natively.
        """

    @execution_engine.setter
    def execution_engine(self, engine: ExecutionEngine | None) -> None:
        """
        Set the execution engine for this stream.

        This allows the stream to use a specific execution engine for
        computation, enabling optimized execution strategies and resource
        management.

        Args:
            engine: The execution engine to attach to this stream
        """
        ...

    def get_substream(self, substream_id: str) -> "Stream":
        """
        Retrieve a specific sub-stream by its identifier.

        This method allows access to individual sub-streams within the
        main stream, enabling focused operations on specific data segments.

        Args:
            substream_id: Unique identifier for the desired sub-stream.

        Returns:
            Stream: The requested sub-stream if it exists
        """
        ...

    @property
    def source(self) -> "Kernel | None":
        """
        The kernel that produced this stream.

        This provides lineage information for tracking data flow through
        the computational graph. Root streams (like file sources) may
        have no source kernel.

        Returns:
            Kernel: The source kernel that created this stream
            None: This is a root stream with no source kernel
        """
        ...

    @property
    def upstreams(self) -> tuple["Stream", ...]:
        """
        Input streams used to produce this stream.

        These are the streams that were provided as input to the source
        kernel when this stream was created. Used for dependency tracking
        and cache invalidation.

        Returns:
            tuple[Stream, ...]: Upstream dependency streams (empty for sources)
        """
        ...

    def keys(
        self, include_system_tags: bool = False
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        """
        Available keys/fields in the stream content.

        Returns the field names present in both tags and packets.
        This provides schema information without requiring type details,
        useful for:
        - Schema inspection and exploration
        - Query planning and optimization
        - Field validation and mapping

        Returns:
            tuple[tuple[str, ...], tuple[str, ...]]: (tag_keys, packet_keys)
        """
        ...

    def tag_keys(self, include_system_tags: bool = False) -> tuple[str, ...]:
        """
        Return the keys used for the tag in the pipeline run records.
        This is used to store the run-associated tag info.
        """
        ...

    def packet_keys(self) -> tuple[str, ...]:
        """
        Return the keys used for the packet in the pipeline run records.
        This is used to store the run-associated packet info.
        """
        ...

    def types(
        self, include_system_tags: bool = False
    ) -> tuple[PythonSchema, PythonSchema]:
        """
        Type specifications for the stream content.

        Returns the type schema for both tags and packets in this stream.
        This information is used for:
        - Type checking and validation
        - Schema inference and planning
        - Compatibility checking between kernels

        Returns:
            tuple[TypeSpec, TypeSpec]: (tag_types, packet_types)
        """
        ...

    def tag_types(self, include_system_tags: bool = False) -> PythonSchema:
        """
        Type specifications for the stream content.

        Returns the type schema for both tags and packets in this stream.
        This information is used for:
        - Type checking and validation
        - Schema inference and planning
        - Compatibility checking between kernels

        Returns:
            tuple[TypeSpec, TypeSpec]: (tag_types, packet_types)
        """
        ...

    def packet_types(self) -> PythonSchema: ...

    @property
    def last_modified(self) -> datetime | None:
        """
        When the stream's content was last modified.

        This property is crucial for caching decisions and dependency tracking:
        - datetime: Content was last modified at this time (cacheable)
        - None: Content is never stable, always recompute (some dynamic streams)

        Both static and live streams typically return datetime values, but
        live streams update this timestamp whenever their content changes.

        Returns:
            datetime: Timestamp of last modification for most streams
            None: Stream content is never stable (some special dynamic streams)
        """
        ...

    @property
    def is_current(self) -> bool:
        """
        Whether the stream is up-to-date with its dependencies.

        A stream is current if its content reflects the latest state of its
        source kernel and upstream streams. This is used for cache validation
        and determining when refresh is needed.

        For live streams, this should always return True since they stay
        current automatically. For static streams, this indicates whether
        the cached content is still valid.

        Returns:
            bool: True if stream is up-to-date, False if refresh needed
        """
        ...

    def __iter__(self) -> Iterator[tuple[Tag, Packet]]:
        """
        Iterate over (tag, packet) pairs in the stream.

        This is the primary way to access stream data. The behavior depends
        on the stream type:
        - Static streams: Return cached/precomputed data
        - Live streams: May trigger computation and always reflect current state

        Yields:
            tuple[Tag, Packet]: Sequential (tag, packet) pairs
        """
        ...

    def iter_packets(
        self,
        execution_engine: ExecutionEngine | None = None,
        execution_engine_opts: dict[str, Any] | None = None,
    ) -> Iterator[tuple[Tag, Packet]]:
        """
        Alias for __iter__ for explicit packet iteration.

        Provides a more explicit method name when the intent is to iterate
        over packets specifically, improving code readability.

        This method must return an immutable iterator -- that is, the returned iterator
        should not change and must consistently return identical tag,packet pairs across
        multiple iterations of the iterator.

        Note that this is NOT to mean that multiple invocation of `iter_packets` must always
        return an identical iterator. The iterator returned by `iter_packets` may change
        between invocations, but the iterator itself must not change. Consequently, it should be understood
        that the returned iterators may be a burden on memory if the stream is large or infinite.

        Yields:
            tuple[Tag, Packet]: Sequential (tag, packet) pairs
        """
        ...

    def run(
        self,
        *args: Any,
        execution_engine: ExecutionEngine | None = None,
        execution_engine_opts: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        """
        Execute the stream using the provided execution engine.

        This method triggers computation of the stream content based on its
        source kernel and upstream streams. It returns a new stream instance
        containing the computed (tag, packet) pairs.

        Args:
            execution_engine: The execution engine to use for computation

        """
        ...

    async def run_async(
        self,
        *args: Any,
        execution_engine: ExecutionEngine | None = None,
        execution_engine_opts: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        """
        Asynchronously execute the stream using the provided execution engine.

        This method triggers computation of the stream content based on its
        source kernel and upstream streams. It returns a new stream instance
        containing the computed (tag, packet) pairs.

        Args:
            execution_engine: The execution engine to use for computation

        """
        ...

    def as_df(
        self,
        include_data_context: bool = False,
        include_source: bool = False,
        include_system_tags: bool = False,
        include_content_hash: bool | str = False,
        sort_by_tags: bool = True,
        execution_engine: ExecutionEngine | None = None,
        execution_engine_opts: dict[str, Any] | None = None,
    ) -> "pl.DataFrame":
        """
        Convert the entire stream to a Polars DataFrame.
        """
        ...

    def as_lazy_frame(
        self,
        include_data_context: bool = False,
        include_source: bool = False,
        include_system_tags: bool = False,
        include_content_hash: bool | str = False,
        sort_by_tags: bool = True,
        execution_engine: ExecutionEngine | None = None,
        execution_engine_opts: dict[str, Any] | None = None,
    ) -> "pl.LazyFrame":
        """
        Load the entire stream to a Polars LazyFrame.
        """
        ...

    def as_polars_df(
        self,
        include_data_context: bool = False,
        include_source: bool = False,
        include_system_tags: bool = False,
        include_content_hash: bool | str = False,
        sort_by_tags: bool = True,
        execution_engine: ExecutionEngine | None = None,
        execution_engine_opts: dict[str, Any] | None = None,
    ) -> "pl.DataFrame": ...

    def as_pandas_df(
        self,
        include_data_context: bool = False,
        include_source: bool = False,
        include_system_tags: bool = False,
        include_content_hash: bool | str = False,
        sort_by_tags: bool = True,
        index_by_tags: bool = True,
        execution_engine: ExecutionEngine | None = None,
        execution_engine_opts: dict[str, Any] | None = None,
    ) -> "pd.DataFrame": ...

    def as_table(
        self,
        include_data_context: bool = False,
        include_source: bool = False,
        include_system_tags: bool = False,
        include_content_hash: bool | str = False,
        sort_by_tags: bool = True,
        execution_engine: ExecutionEngine | None = None,
        execution_engine_opts: dict[str, Any] | None = None,
    ) -> "pa.Table":
        """
        Convert the entire stream to a PyArrow Table.

        Materializes all (tag, packet) pairs into a single table for
        analysis and processing. This operation may be expensive for
        large streams or live streams that need computation.

        If include_content_hash is True, an additional column called "_content_hash"
        containing the content hash of each packet is included. If include_content_hash
        is a string, it is used as the name of the content hash column.

        Returns:
            pa.Table: Complete stream data as a PyArrow Table
        """
        ...

    def flow(
        self,
        execution_engine: ExecutionEngine | None = None,
        execution_engine_opts: dict[str, Any] | None = None,
    ) -> Collection[tuple[Tag, Packet]]:
        """
        Return the entire stream as a collection of (tag, packet) pairs.

        This method materializes the stream content into a list or similar
        collection type. It is useful for small streams or when you need
        to process all data at once.

        Args:
            execution_engine: Optional execution engine to use for computation.
                If None, the stream will use its default execution engine.
        """
        ...

    def join(self, other_stream: "Stream", label: str | None = None) -> "Stream":
        """
        Join this stream with another stream.

        Combines two streams into a single stream by merging their content.
        The resulting stream contains all (tag, packet) pairs from both
        streams, preserving their order.

        Args:
            other_stream: The other stream to join with this one.

        Returns:
            Self: New stream containing combined content from both streams.
        """
        ...

    def semi_join(self, other_stream: "Stream", label: str | None = None) -> "Stream":
        """
        Perform a semi-join with another stream.

        This operation filters this stream to only include packets that have
        corresponding tags in the other stream. The resulting stream contains
        all (tag, packet) pairs from this stream that match tags in the other.

        Args:
            other_stream: The other stream to semi-join with this one.

        Returns:
            Self: New stream containing filtered content based on the semi-join.
        """
        ...

    def map_tags(
        self,
        name_map: Mapping[str, str],
        drop_unmapped: bool = True,
        label: str | None = None,
    ) -> "Stream":
        """
        Map tag names in this stream to new names based on the provided mapping.
        """
        ...

    def map_packets(
        self,
        name_map: Mapping[str, str],
        drop_unmapped: bool = True,
        label: str | None = None,
    ) -> "Stream":
        """
        Map packet names in this stream to new names based on the provided mapping.
        """
        ...

    def polars_filter(
        self,
        *predicates: Any,
        constraint_map: Mapping[str, Any] | None = None,
        label: str | None = None,
        **constraints: Any,
    ) -> "Stream": ...

    def select_tag_columns(
        self,
        tag_columns: str | Collection[str],
        strict: bool = True,
        label: str | None = None,
    ) -> "Stream":
        """
        Select the specified tag columns from the stream. A ValueError is raised
        if one or more specified tag columns do not exist in the stream unless strict = False.
        """
        ...

    def select_packet_columns(
        self,
        packet_columns: str | Collection[str],
        strict: bool = True,
        label: str | None = None,
    ) -> "Stream":
        """
        Select the specified tag columns from the stream. A ValueError is raised
        if one or more specified tag columns do not exist in the stream unless strict = False.
        """
        ...

    def drop_tag_columns(
        self,
        tag_columns: str | Collection[str],
        strict: bool = True,
        label: str | None = None,
    ) -> "Stream":
        """
        Drop the specified tag columns from the stream. A ValueError is raised
        if one or more specified tag columns do not exist in the stream unless strict = False.
        """
        ...

    # TODO: check to make sure source columns are also dropped
    def drop_packet_columns(
        self,
        packet_columns: str | Collection[str],
        strict: bool = True,
        label: str | None = None,
    ) -> "Stream":
        """
        Drop the specified packet columns from the stream. A ValueError is raised
        if one or more specified packet columns do not exist in the stream unless strict = False.
        """
        ...

    def batch(
        self,
        batch_size: int = 0,
        drop_partial_batch: bool = False,
        label: str | None = None,
    ) -> "Stream":
        """
        Batch the stream into groups of the specified size.

        This operation groups (tag, packet) pairs into batches for more
        efficient processing. Each batch is represented as a single (tag, packet)
        pair where the tag is a list of tags and the packet is a list of packets.

        Args:
            batch_size: Number of (tag, packet) pairs per batch. If 0, all
                        pairs are included in a single batch.
            drop_partial_batch: If True, drop the last batch if it has fewer
                             than batch_size pairs.

        Returns:
            Self: New stream containing batched (tag, packet) pairs.
        """
        ...


@runtime_checkable
class LiveStream(Stream, Protocol):
    """
    A stream that automatically stays up-to-date with its upstream dependencies.

    LiveStream extends the base Stream protocol with capabilities for "up-to-date"
    data flow and reactive computation. Unlike static streams which represent
    snapshots, LiveStreams provide the guarantee that their content always
    reflects the current state of their dependencies.

    Key characteristics:
    - Automatically refresh the stream if changes in the upstreams are detected
    - Track last_modified timestamp when content changes
    - Support manual refresh triggering and invalidation
    - By design, LiveStream would return True for is_current except when auto-update fails.

    LiveStreams are always returned by Kernel.__call__() methods, ensuring
    that normal kernel usage produces live, up-to-date results.

    Caching behavior:
    - last_modified updates whenever content changes
    - Can be cached based on dependency timestamps
    - Invalidation happens automatically when upstreams change

    Use cases:
    - Real-time data processing pipelines
    - Reactive user interfaces
    - Monitoring and alerting systems
    - Dynamic dashboard updates
    - Any scenario requiring current data
    """

    def refresh(self, force: bool = False) -> bool:
        """
        Manually trigger a refresh of this stream's content.

        Forces the stream to check its upstream dependencies and update
        its content if necessary. This is useful when:
        - You want to ensure the latest data before a critical operation
        - You need to force computation at a specific time
        - You're debugging data flow issues
        - You want to pre-compute results for performance
        Args:
            force: If True, always refresh even if the stream is current.
                   If False, only refresh if the stream is not current.

        Returns:
            bool: True if the stream was refreshed, False if it was already current.
        Note: LiveStream refreshes automatically on access, so this
        method may be a no-op for some implementations. However, it's
        always safe to call if you need to control when the cache is refreshed.
        """
        ...

    def invalidate(self) -> None:
        """
        Mark this stream as invalid, forcing a refresh on next access.

        This method is typically called when:
        - Upstream dependencies have changed
        - The source kernel has been modified
        - External data sources have been updated
        - Manual cache invalidation is needed

        The stream will automatically refresh its content the next time
        it's accessed (via iteration, as_table(), etc.).

        This is more efficient than immediate refresh when you know the
        data will be accessed later.
        """
        ...
