import logging
from abc import abstractmethod
from collections.abc import Collection, Iterator, Mapping
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from orcapod import contexts
from orcapod.core.base import LabeledContentIdentifiableBase
from orcapod.protocols import core_protocols as cp
from orcapod.types import PythonSchema
from orcapod.utils.lazy_module import LazyModule


if TYPE_CHECKING:
    import pyarrow as pa
    import pyarrow.compute as pc
    import polars as pl
    import pandas as pd
else:
    pa = LazyModule("pyarrow")
    pc = LazyModule("pyarrow.compute")
    pl = LazyModule("polars")
    pd = LazyModule("pandas")


# TODO: consider using this instead of making copy of dicts
# from types import MappingProxyType

logger = logging.getLogger(__name__)


class OperatorStreamBaseMixin:
    def join(self, other_stream: cp.Stream, label: str | None = None) -> cp.Stream:
        """
        Joins this stream with another stream, returning a new stream that contains
        the combined data from both streams.
        """
        from orcapod.core.operators import Join

        return Join()(self, other_stream, label=label)  # type: ignore

    def semi_join(
        self,
        other_stream: cp.Stream,
        label: str | None = None,
    ) -> cp.Stream:
        """
        Performs a semi-join with another stream, returning a new stream that contains
        only the packets from this stream that have matching tags in the other stream.
        """
        from orcapod.core.operators import SemiJoin

        return SemiJoin()(self, other_stream, label=label)  # type: ignore

    def map_tags(
        self,
        name_map: Mapping[str, str],
        drop_unmapped: bool = True,
        label: str | None = None,
    ) -> cp.Stream:
        """
        Maps the tags in this stream according to the provided name_map.
        If drop_unmapped is True, any tags that are not in the name_map will be dropped.
        """
        from orcapod.core.operators import MapTags

        return MapTags(name_map, drop_unmapped)(self, label=label)  # type: ignore

    def map_packets(
        self,
        name_map: Mapping[str, str],
        drop_unmapped: bool = True,
        label: str | None = None,
    ) -> cp.Stream:
        """
        Maps the packets in this stream according to the provided packet_map.
        If drop_unmapped is True, any packets that are not in the packet_map will be dropped.
        """
        from orcapod.core.operators import MapPackets

        return MapPackets(name_map, drop_unmapped)(self, label=label)  # type: ignore

    def batch(
        self: cp.Stream,
        batch_size: int = 0,
        drop_partial_batch: bool = False,
        label: str | None = None,
    ) -> cp.Stream:
        """
        Batch stream into fixed-size chunks, each of size batch_size.
        If drop_last is True, any remaining elements that don't fit into a full batch will be dropped.
        """
        from orcapod.core.operators import Batch

        return Batch(batch_size=batch_size, drop_partial_batch=drop_partial_batch)(
            self, label=label
        )  # type: ignore

    def polars_filter(
        self: cp.Stream,
        *predicates: Any,
        constraint_map: Mapping[str, Any] | None = None,
        label: str | None = None,
        **constraints: Any,
    ) -> cp.Stream:
        from orcapod.core.operators import PolarsFilter

        total_constraints = dict(constraint_map) if constraint_map is not None else {}

        total_constraints.update(constraints)

        return PolarsFilter(predicates=predicates, constraints=total_constraints)(
            self, label=label
        )

    def select_tag_columns(
        self: cp.Stream,
        tag_columns: str | Collection[str],
        strict: bool = True,
        label: str | None = None,
    ) -> cp.Stream:
        """
        Select the specified tag columns from the stream. A ValueError is raised
        if one or more specified tag columns do not exist in the stream unless strict = False.
        """
        from orcapod.core.operators import SelectTagColumns

        return SelectTagColumns(tag_columns, strict=strict)(self, label=label)

    def select_packet_columns(
        self: cp.Stream,
        packet_columns: str | Collection[str],
        strict: bool = True,
        label: str | None = None,
    ) -> cp.Stream:
        """
        Select the specified packet columns from the stream. A ValueError is raised
        if one or more specified packet columns do not exist in the stream unless strict = False.
        """
        from orcapod.core.operators import SelectPacketColumns

        return SelectPacketColumns(packet_columns, strict=strict)(self, label=label)

    def drop_tag_columns(
        self: cp.Stream,
        tag_columns: str | Collection[str],
        strict: bool = True,
        label: str | None = None,
    ) -> cp.Stream:
        from orcapod.core.operators import DropTagColumns

        return DropTagColumns(tag_columns, strict=strict)(self, label=label)

    def drop_packet_columns(
        self: cp.Stream,
        packet_columns: str | Collection[str],
        strict: bool = True,
        label: str | None = None,
    ) -> cp.Stream:
        from orcapod.core.operators import DropPacketColumns

        return DropPacketColumns(packet_columns, strict=strict)(self, label=label)


class StatefulStreamBase(OperatorStreamBaseMixin, LabeledContentIdentifiableBase):
    """
    A stream that has a unique identity within the pipeline.
    """

    def pop(self) -> cp.Stream:
        return self

    def __init__(
        self,
        execution_engine: cp.ExecutionEngine | None = None,
        execution_engine_opts: dict[str, Any] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self._last_modified: datetime | None = None
        self._set_modified_time()
        # note that this is not necessary for Stream protocol, but is provided
        # for convenience to resolve semantic types and other context-specific information
        self._execution_engine = execution_engine
        self._execution_engine_opts = execution_engine_opts

    @property
    def substream_identities(self) -> tuple[str, ...]:
        """
        Returns the identities of the substreams that this stream is composed of.
        This is used to identify the substreams in the computational graph.
        """
        return (self.content_hash().to_hex(),)

    @property
    def execution_engine(self) -> cp.ExecutionEngine | None:
        """
        Returns the execution engine that is used to execute this stream.
        This is typically used to track the execution context of the stream.
        """
        return self._execution_engine

    @execution_engine.setter
    def execution_engine(self, engine: cp.ExecutionEngine | None) -> None:
        """
        Sets the execution engine for the stream.
        This is typically used to track the execution context of the stream.
        """
        self._execution_engine = engine

    # TODO: add getter/setter for execution engine opts

    def get_substream(self, substream_id: str) -> cp.Stream:
        """
        Returns the substream with the given substream_id.
        This is used to retrieve a specific substream from the stream.
        """
        if substream_id == self.substream_identities[0]:
            return self
        else:
            raise ValueError(f"Substream with ID {substream_id} not found.")

    @property
    @abstractmethod
    def source(self) -> cp.Kernel | None:
        """
        The source of the stream, which is the kernel that generated the stream.
        This is typically used to track the origin of the stream in the computational graph.
        """
        ...

    @property
    @abstractmethod
    def upstreams(self) -> tuple[cp.Stream, ...]:
        """
        The upstream streams that are used to generate this stream.
        This is typically used to track the origin of the stream in the computational graph.
        """
        ...

    def computed_label(self) -> str | None:
        if self.source is not None:
            # use the invocation operation label
            return self.source.label
        return None

    @abstractmethod
    def keys(
        self, include_system_tags: bool = False
    ) -> tuple[tuple[str, ...], tuple[str, ...]]: ...

    def tag_keys(self, include_system_tags: bool = False) -> tuple[str, ...]:
        return self.keys(include_system_tags=include_system_tags)[0]

    def packet_keys(self) -> tuple[str, ...]:
        return self.keys()[1]

    @abstractmethod
    def types(
        self, include_system_tags: bool = False
    ) -> tuple[PythonSchema, PythonSchema]: ...

    def tag_types(self, include_system_tags: bool = False) -> PythonSchema:
        return self.types(include_system_tags=include_system_tags)[0]

    def packet_types(self) -> PythonSchema:
        return self.types()[1]

    @property
    def last_modified(self) -> datetime | None:
        """
        Returns when the stream's content was last modified.
        This is used to track the time when the stream was last accessed.
        Returns None if the stream has not been accessed yet.
        """
        return self._last_modified

    @property
    def is_current(self) -> bool:
        """
        Returns whether the stream is current.
        A stream is current if the content is up-to-date with respect to its source.
        This can be used to determine if a stream with non-None last_modified is up-to-date.
        Note that for asynchronous streams, this status is not applicable and always returns False.
        """
        if self.last_modified is None:
            # If there is no last_modified timestamp, we cannot determine if the stream is current
            return False

        # check if the source kernel has been modified
        if self.source is not None and (
            self.source.last_modified is None
            or self.source.last_modified > self.last_modified
        ):
            return False

        # check if all upstreams are current
        for upstream in self.upstreams:
            if (
                not upstream.is_current
                or upstream.last_modified is None
                or upstream.last_modified > self.last_modified
            ):
                return False
        return True

    def _set_modified_time(
        self, timestamp: datetime | None = None, invalidate: bool = False
    ) -> None:
        if invalidate:
            self._last_modified = None
            return

        if timestamp is not None:
            self._last_modified = timestamp
        else:
            self._last_modified = datetime.now(timezone.utc)

    def __iter__(
        self,
    ) -> Iterator[tuple[cp.Tag, cp.Packet]]:
        return self.iter_packets()

    @abstractmethod
    def iter_packets(
        self,
        execution_engine: cp.ExecutionEngine | None = None,
        execution_engine_opts: dict[str, Any] | None = None,
    ) -> Iterator[tuple[cp.Tag, cp.Packet]]: ...

    @abstractmethod
    def run(
        self,
        *args: Any,
        execution_engine: cp.ExecutionEngine | None = None,
        execution_engine_opts: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None: ...

    @abstractmethod
    async def run_async(
        self,
        *args: Any,
        execution_engine: cp.ExecutionEngine | None = None,
        execution_engine_opts: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None: ...

    @abstractmethod
    def as_table(
        self,
        include_data_context: bool = False,
        include_source: bool = False,
        include_system_tags: bool = False,
        include_content_hash: bool | str = False,
        sort_by_tags: bool = True,
        execution_engine: cp.ExecutionEngine | None = None,
        execution_engine_opts: dict[str, Any] | None = None,
    ) -> "pa.Table": ...

    def as_polars_df(
        self,
        include_data_context: bool = False,
        include_source: bool = False,
        include_system_tags: bool = False,
        include_content_hash: bool | str = False,
        sort_by_tags: bool = True,
        execution_engine: cp.ExecutionEngine | None = None,
        execution_engine_opts: dict[str, Any] | None = None,
    ) -> "pl.DataFrame":
        """
        Convert the entire stream to a Polars DataFrame.
        """
        return pl.DataFrame(
            self.as_table(
                include_data_context=include_data_context,
                include_source=include_source,
                include_system_tags=include_system_tags,
                include_content_hash=include_content_hash,
                sort_by_tags=sort_by_tags,
                execution_engine=execution_engine,
                execution_engine_opts=execution_engine_opts,
            )
        )

    def as_df(
        self,
        include_data_context: bool = False,
        include_source: bool = False,
        include_system_tags: bool = False,
        include_content_hash: bool | str = False,
        sort_by_tags: bool = True,
        execution_engine: cp.ExecutionEngine | None = None,
        execution_engine_opts: dict[str, Any] | None = None,
    ) -> "pl.DataFrame":
        """
        Convert the entire stream to a Polars DataFrame.
        """
        return self.as_polars_df(
            include_data_context=include_data_context,
            include_source=include_source,
            include_system_tags=include_system_tags,
            include_content_hash=include_content_hash,
            sort_by_tags=sort_by_tags,
            execution_engine=execution_engine,
            execution_engine_opts=execution_engine_opts,
        )

    def as_lazy_frame(
        self,
        include_data_context: bool = False,
        include_source: bool = False,
        include_system_tags: bool = False,
        include_content_hash: bool | str = False,
        sort_by_tags: bool = True,
        execution_engine: cp.ExecutionEngine | None = None,
        execution_engine_opts: dict[str, Any] | None = None,
    ) -> "pl.LazyFrame":
        """
        Convert the entire stream to a Polars LazyFrame.
        """
        df = self.as_polars_df(
            include_data_context=include_data_context,
            include_source=include_source,
            include_system_tags=include_system_tags,
            include_content_hash=include_content_hash,
            sort_by_tags=sort_by_tags,
            execution_engine=execution_engine,
            execution_engine_opts=execution_engine_opts,
        )
        return df.lazy()

    def as_pandas_df(
        self,
        include_data_context: bool = False,
        include_source: bool = False,
        include_system_tags: bool = False,
        include_content_hash: bool | str = False,
        sort_by_tags: bool = True,
        index_by_tags: bool = True,
        execution_engine: cp.ExecutionEngine | None = None,
        execution_engine_opts: dict[str, Any] | None = None,
    ) -> "pd.DataFrame":
        df = self.as_polars_df(
            include_data_context=include_data_context,
            include_source=include_source,
            include_system_tags=include_system_tags,
            include_content_hash=include_content_hash,
            sort_by_tags=sort_by_tags,
            execution_engine=execution_engine,
            execution_engine_opts=execution_engine_opts,
        )
        tag_keys, _ = self.keys()
        pdf = df.to_pandas()
        if index_by_tags:
            pdf = pdf.set_index(list(tag_keys))
        return pdf

    def flow(
        self,
        execution_engine: cp.ExecutionEngine | None = None,
        execution_engine_opts: dict[str, Any] | None = None,
    ) -> Collection[tuple[cp.Tag, cp.Packet]]:
        """
        Flow everything through the stream, returning the entire collection of
        (Tag, Packet) as a collection. This will tigger any upstream computation of the stream.
        """
        return [
            e
            for e in self.iter_packets(
                execution_engine=execution_engine,
                execution_engine_opts=execution_engine_opts,
            )
        ]

    def _repr_html_(self) -> str:
        df = self.as_polars_df()
        tag_map = {t: f"*{t}" for t in self.tag_keys()}
        # TODO: construct repr html better
        df = df.rename(tag_map)
        return f"{self.__class__.__name__}[{self.label}]\n" + df._repr_html_()

    def view(
        self,
        include_data_context: bool = False,
        include_source: bool = False,
        include_system_tags: bool = False,
        include_content_hash: bool | str = False,
        sort_by_tags: bool = True,
        execution_engine: cp.ExecutionEngine | None = None,
        execution_engine_opts: dict[str, Any] | None = None,
    ) -> "StreamView":
        df = self.as_polars_df(
            include_data_context=include_data_context,
            include_source=include_source,
            include_system_tags=include_system_tags,
            include_content_hash=include_content_hash,
            sort_by_tags=sort_by_tags,
            execution_engine=execution_engine,
            execution_engine_opts=execution_engine_opts,
        )
        tag_map = {t: f"*{t}" for t in self.tag_keys()}
        # TODO: construct repr html better
        df = df.rename(tag_map)
        return StreamView(self, df)


class StreamView:
    def __init__(self, stream: StatefulStreamBase, view_df: "pl.DataFrame") -> None:
        self._stream = stream
        self._view_df = view_df

    def _repr_html_(self) -> str:
        return (
            f"{self._stream.__class__.__name__}[{self._stream.label}]\n"
            + self._view_df._repr_html_()
        )

    # def identity_structure(self) -> Any:
    #     """
    #     Identity structure of a stream is deferred to the identity structure
    #     of the associated invocation, if present.
    #     A bare stream without invocation has no well-defined identity structure.
    #     Specialized stream subclasses should override this method to provide more meaningful identity structure
    #     """
    #     ...


class StreamBase(StatefulStreamBase):
    """
    A stream is a collection of tagged-packets that are generated by an operation.
    The stream is iterable and can be used to access the packets in the stream.

    A stream has property `invocation` that is an instance of Invocation that generated the stream.
    This may be None if the stream is not generated by a kernel (i.e. directly instantiated by a user).
    """

    def __init__(
        self,
        source: cp.Kernel | None = None,
        upstreams: tuple[cp.Stream, ...] = (),
        data_context: str | contexts.DataContext | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self._source = source
        self._upstreams = upstreams

        # if data context is not provided, use that of the source kernel
        if data_context is None and source is not None:
            # if source is provided, use its data context
            data_context = source.data_context_key
        super().__init__(data_context=data_context, **kwargs)

    @property
    def source(self) -> cp.Kernel | None:
        """
        The source of the stream, which is the kernel that generated the stream.
        This is typically used to track the origin of the stream in the computational graph.
        """
        return self._source

    @property
    def upstreams(self) -> tuple[cp.Stream, ...]:
        """
        The upstream streams that are used to generate this stream.
        This is typically used to track the origin of the stream in the computational graph.
        """
        return self._upstreams

    def computed_label(self) -> str | None:
        if self.source is not None:
            # use the invocation operation label
            return self.source.label
        return None

    # @abstractmethod
    # def iter_packets(
    #     self,
    #     execution_engine: dp.ExecutionEngine | None = None,
    # ) -> Iterator[tuple[dp.Tag, dp.Packet]]: ...

    # @abstractmethod
    # def run(
    #     self,
    #     execution_engine: dp.ExecutionEngine | None = None,
    # ) -> None: ...

    # @abstractmethod
    # async def run_async(
    #     self,
    #     execution_engine: dp.ExecutionEngine | None = None,
    # ) -> None: ...

    # @abstractmethod
    # def as_table(
    #     self,
    #     include_data_context: bool = False,
    #     include_source: bool = False,
    #     include_system_tags: bool = False,
    #     include_content_hash: bool | str = False,
    #     sort_by_tags: bool = True,
    #     execution_engine: dp.ExecutionEngine | None = None,
    # ) -> "pa.Table": ...

    def identity_structure(self) -> Any:
        """
        Identity structure of a stream is deferred to the identity structure
        of the associated invocation, if present.
        A bare stream without invocation has no well-defined identity structure.
        Specialized stream subclasses should override this method to provide more meaningful identity structure
        """
        if self.source is not None:
            # if the stream is generated by an operation, use the identity structure from the invocation
            return self.source.identity_structure(self.upstreams)
        return super().identity_structure()


class ImmutableStream(StreamBase):
    """
    A class of stream that is constructed from immutable/constant data and does not change over time.
    Consequently, the identity of an unsourced stream should be based on the content of the stream itself.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._data_content_identity = None

    @abstractmethod
    def data_content_identity_structure(self) -> Any:
        """
        Returns a hash of the content of the stream.
        This is used to identify the content of the stream.
        """
        ...

    def identity_structure(self) -> Any:
        if self.source is not None:
            # if the stream is generated by an operation, use the identity structure from the invocation
            return self.source.identity_structure(self.upstreams)
        # otherwise, use the content of the stream as the identity structure
        if self._data_content_identity is None:
            self._data_content_identity = self.data_content_identity_structure()
        return self._data_content_identity
