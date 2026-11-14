from __future__ import annotations

from collections.abc import AsyncIterator, Collection, Iterator, Mapping, Sequence
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from orcapod.protocols.core_protocols.datagrams import DataProtocol, TagProtocol
from orcapod.protocols.core_protocols.traceable import TraceableProtocol
from orcapod.protocols.hashing_protocols import PipelineElementProtocol
from orcapod.types import ColumnConfig, Schema

if TYPE_CHECKING:
    import pandas as pd
    import polars as pl
    import pyarrow as pa

    from orcapod.protocols.core_protocols.pod import PodProtocol


class StreamViewProtocol(Protocol):
    """Rows of a stream, as ``head`` and ``run`` return them.

    ``orcapod.core.streams.view.StreamView`` is the implementation.
    """

    @property
    def table(self) -> "pa.Table":
        """The rows, with the exact Arrow types the stream gives them."""
        ...

    @property
    def status(self) -> str:
        """Where the rows came from, or why there are none (a ``ViewStatus``)."""
        ...

    @property
    def total_rows(self) -> int | None:
        """The stream's row count when it is known without computing."""
        ...

    def header(self) -> str:
        """The one-line description shown above the rows."""
        ...

    def as_table(self) -> "pa.Table":
        """The rows as a PyArrow table."""
        ...

    def as_polars_df(self) -> "pl.DataFrame":
        """The rows as a Polars DataFrame."""
        ...

    def __len__(self) -> int:
        """The number of rows in ``table``."""
        ...

    def head(self, n: int = 10) -> "StreamViewProtocol":
        """A view of the first ``n`` rows of this view; never reads or computes."""
        ...

    def tail(self, n: int = 10) -> "StreamViewProtocol":
        """A view of the last ``n`` rows of this view; never reads or computes."""
        ...

    def sort_by_tags(self) -> "StreamViewProtocol":
        """A view of this view's rows in tag order; never reads or computes."""
        ...

    def _repr_html_(self) -> str:
        """The notebook preview of the rows."""
        ...


@runtime_checkable
class StreamProtocol(TraceableProtocol, PipelineElementProtocol, Protocol):
    """
    Base protocol for all streams in Orcapod.

    Streams represent sequences of (TagProtocol, DataProtocol) pairs flowing through the
    computational graph. They are the fundamental data structure connecting
    kernels and carrying both data and metadata.

    Streams can be either:
    - Static: Immutable snapshots created at a specific point in time
    - Live: Dynamic streams that stay current with upstream dependencies

    All streams provide:
    - Iteration over (tag, data) pairs
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

        Returns the field names present in both tags and data.
        This provides schema information without requiring type details,
        useful for:
        - Schema inspection and exploration
        - Query planning and optimization
        - Field validation and mapping

        Returns:
            tuple[tuple[str, ...], tuple[str, ...]]: (tag_keys, data_keys)
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

        Returns the type schema for both tags and data in this stream.
        This information is used for:
        - Type checking and validation
        - Schema inference and planning
        - Compatibility checking between kernels

        Returns:
            tuple[Schema, Schema]: (tag_types, data_types)
        """
        ...

    def iter_data(self) -> Iterator[tuple[TagProtocol, DataProtocol]]:
        """
        Generates explicit iterator over (tag, data) pairs in the stream.

        Note that multiple invocation of `iter_data` may not always
        return an identical iterator.

        Yields:
            tuple[TagProtocol, DataProtocol]: Sequential (tag, data) pairs
        """
        ...

    def async_iter_data(self) -> AsyncIterator[tuple[TagProtocol, DataProtocol]]:
        """
        Generates asynchronous iterator over (tag, data) pairs in the stream.

        Note that multiple invocation of `async_iter_data` may not always
        return an identical iterator.

        Yields:
            tuple[tagProtocol, DataProtcol]: Asynchrnous sequential (tag, data) pairs

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

        Materializes all (tag, data) pairs into a single table for
        analysis and processing. This operation may be expensive for
        large streams or live streams that need computation.

        If include_content_hash is True, an additional column called "_content_hash"
        containing the content hash of each data is included. If include_content_hash
        is a string, it is used as the name of the content hash column.

        Returns:
            pa.Table: Complete stream data as a PyArrow Table
        """
        ...

    def run(self) -> "StreamViewProtocol":
        """Compute the whole stream and return a view of all its rows.

        Does the work ``as_table()`` does, through the same path and cache; a job node
        runs as it does in a job. Every stream has it, with no argument: it is the one
        way to compute a stream outside a job (ITL-1081). ``head()`` and the notebook
        preview never compute.

        Returns:
            StreamView: Every computed row, with the default column configuration.
        """
        ...

    def head(
        self,
        n: int = 10,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> "StreamViewProtocol":
        """Return at most ``n`` readily available rows, without computing.

        The rows are ones the stream already holds, has cached from an earlier
        computation, has stored, or can read with a bounded read. It never calls a
        function, runs an operator, delivers a side effect, polls or writes.

        Returns:
            StreamView: The rows, and a status that tells an empty result from one
            that is not computed.
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
    ) -> Sequence[tuple[TagProtocol, DataProtocol]]:
        """
        Return the entire stream as a collection of (tag, data) pairs.

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
        The resulting stream contains all (tag, data) pairs from both
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

        This operation filters this stream to only include data that have
        corresponding tags in the other stream. The resulting stream contains
        all (tag, data) pairs from this stream that match tags in the other.

        Args:
            other_stream: The other stream to semi-join with this one.

        Returns:
            Self: New stream containing filtered content based on the semi-join.
        """
        ...

    def map_tags(
        self,
        name_map: Mapping[str, str],
        drop_unmapped: bool = False,
        label: str | None = None,
    ) -> "StreamProtocol":
        """
        Map tag names in this stream to new names based on the provided mapping.
        Tags not in the mapping are kept unless ``drop_unmapped`` is True.
        """
        ...

    def map_data(
        self,
        name_map: Mapping[str, str],
        drop_unmapped: bool = False,
        label: str | None = None,
    ) -> "StreamProtocol":
        """
        Map data names in this stream to new names based on the provided mapping.
        Data columns not in the mapping are kept unless ``drop_unmapped`` is True.
        """
        ...

    def polars_filter(
        self,
        *predicates: Any,
        constraint_map: Mapping[str, Any] | None = None,
        label: str | None = None,
        **constraints: Any,
    ) -> "StreamProtocol": ...

    def drop_nulls(
        self,
        columns: str | Collection[str] | None = None,
        label: str | None = None,
    ) -> "StreamProtocol":
        """
        Keep the rows where every named column (tag or data) is non-null, and
        narrow each named column from ``T | None`` to ``T``. Without names,
        every tag and data column is tested and narrowed.
        """
        ...

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

    def select_data_columns(
        self,
        data_columns: str | Collection[str],
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
    def drop_data_columns(
        self,
        data_columns: str | Collection[str],
        strict: bool = True,
        label: str | None = None,
    ) -> "StreamProtocol":
        """
        Drop the specified data columns from the stream. A ValueError is raised
        if one or more specified data columns do not exist in the stream unless strict = False.
        """
        ...

    def drop_columns(
        self,
        *columns: str | Collection[str],
        strict: bool = True,
        label: str | None = None,
    ) -> "StreamProtocol":
        """Drop the named columns, whether each is a tag or a data column.

        Names are given as one or more strings, or as one collection of strings
        as the only positional argument.
        """
        ...

    def select_columns(
        self,
        *columns: str | Collection[str],
        strict: bool = True,
        label: str | None = None,
    ) -> "StreamProtocol":
        """Keep exactly the named columns, tag or data; drop every other one.

        Names are given as one or more strings, or as one collection of strings
        as the only positional argument.
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

        This operation groups (tag, data) pairs into batches for more
        efficient processing. Each batch is represented as a single (tag, data)
        pair where the tag is a list of tags and the data is a list of data.

        Args:
            batch_size: Number of (tag, data) pairs per batch. If 0, all
                        pairs are included in a single batch.
            drop_partial_batch: If True, drop the last batch if it has fewer
                             than batch_size pairs.

        Returns:
            Self: New stream containing batched (tag, data) pairs.
        """
        ...
