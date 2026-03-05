from collections.abc import AsyncIterator, Collection, Iterator, Mapping
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from orcapod.protocols.core_protocols.datagrams import PacketProtocol, TagProtocol
from orcapod.protocols.core_protocols.traceable import TraceableProtocol
from orcapod.protocols.hashing_protocols import PipelineElementProtocol
from orcapod.types import ColumnConfig, Schema

if TYPE_CHECKING:
    import pandas as pd
    import polars as pl
    import pyarrow as pa

    from orcapod.protocols.core_protocols.pod import PodProtocol


@runtime_checkable
class StreamProtocol(TraceableProtocol, PipelineElementProtocol, Protocol):
    """
    Base protocol for all streams in Orcapod.

    Streams represent sequences of (TagProtocol, PacketProtocol) pairs flowing through the
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

    # TODO: add substream system

    @property
    def producer(self) -> "PodProtocol | None":
        """
        The pod that produced this stream, if any.

        This provides lineage information for tracking data flow through
        the computational graph. Root streams (like file sources) may
        have no source pod.

        Returns:
            PodProtocol: The source pod that created this stream
            None: This is a root stream with no source pod
        """
        ...

    @property
    def upstreams(self) -> tuple["StreamProtocol", ...]:
        """
        Input streams used to produce this stream.

        These are the streams that were provided as input to the source
        pod when this stream was created. Used for dependency tracking
        and cache invalidation. Note that `source` must be checked for
        upstreams to be meaningfully inspected.

        Returns:
            tuple[StreamProtocol, ...]: Upstream dependency streams (empty for sources)
        """
        ...

    def keys(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
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

    def output_schema(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        """
        Type specifications for the stream content.

        Returns the type schema for both tags and packets in this stream.
        This information is used for:
        - Type checking and validation
        - Schema inference and planning
        - Compatibility checking between kernels

        Returns:
            tuple[Schema, Schema]: (tag_types, packet_types)
        """
        ...

    def iter_packets(self) -> Iterator[tuple[TagProtocol, PacketProtocol]]:
        """
        Generates explicit iterator over (tag, packet) pairs in the stream.

        Note that multiple invocation of `iter_packets` may not always
        return an identical iterator.

        Yields:
            tuple[TagProtocol, PacketProtocol]: Sequential (tag, packet) pairs
        """
        ...

    def async_iter_packets(self) -> AsyncIterator[tuple[TagProtocol, PacketProtocol]]:
        """
        Generates asynchronous iterator over (tag, packet) pairs in the stream.

        Note that multiple invocation of `async_iter_packets` may not always
        return an identical iterator.

        Yields:
            tuple[tagProtocol, PacketProtcol]: Asynchrnous sequential (tag, packet) pairs

        """
        ...

    def as_table(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
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


class StreamWithOperationsProtocol(StreamProtocol, Protocol):
    def as_df(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> "pl.DataFrame":
        """
        Convert the entire stream to a Polars DataFrame.
        """
        ...

    def as_lazy_frame(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> "pl.LazyFrame":
        """
        Load the entire stream to a Polars LazyFrame.
        """
        ...

    def as_polars_df(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> "pl.DataFrame":
        """
        Convert the entire stream to a Polars DataFrame.
        """
        ...

    def as_pandas_df(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> "pd.DataFrame":
        """
        Convert the entire stream to a Pandas DataFrame.
        """
        ...

    def flow(
        self,
    ) -> Collection[tuple[TagProtocol, PacketProtocol]]:
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

    def join(
        self, other_stream: "StreamProtocol", label: str | None = None
    ) -> "StreamProtocol":
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

    def semi_join(
        self, other_stream: "StreamProtocol", label: str | None = None
    ) -> "StreamProtocol":
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
    ) -> "StreamProtocol":
        """
        Map tag names in this stream to new names based on the provided mapping.
        """
        ...

    def map_packets(
        self,
        name_map: Mapping[str, str],
        drop_unmapped: bool = True,
        label: str | None = None,
    ) -> "StreamProtocol":
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
    ) -> "StreamProtocol": ...

    def select_tag_columns(
        self,
        tag_columns: str | Collection[str],
        strict: bool = True,
        label: str | None = None,
    ) -> "StreamProtocol":
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
    ) -> "StreamProtocol":
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
    ) -> "StreamProtocol":
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
    ) -> "StreamProtocol":
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
    ) -> "StreamProtocol":
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
