import logging
from abc import abstractmethod
from collections.abc import Collection, Iterator, Mapping
from typing import TYPE_CHECKING, Any

from orcapod.core.base import OrcapodBase
from orcapod.protocols.core_protocols import ColumnConfig, Packet, Pod, Stream, Tag
from orcapod.types import PythonSchema
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pandas as pd
    import polars as pl
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")
    pl = LazyModule("polars")
    pd = LazyModule("pandas")


# TODO: consider using this instead of making copy of dicts
# from types import MappingProxyType

logger = logging.getLogger(__name__)


class StreamBase(OrcapodBase):
    @property
    @abstractmethod
    def source(self) -> Pod | None: ...

    @property
    @abstractmethod
    def upstreams(self) -> tuple[Stream, ...]: ...

    def computed_label(self) -> str | None:
        if self.source is not None:
            # use the invocation operation label
            return self.source.label
        return None

    def identity_structure(self) -> Any:
        # Identity of a PodStream is determined by the pod and its upstreams
        if self.source is None:
            raise ValueError("Stream has no source pod for identity structure.")
        return (
            self.source,
            self.source.argument_symmetry(self.upstreams),
        )

    def join(self, other_stream: Stream, label: str | None = None) -> Stream:
        """
        Joins this stream with another stream, returning a new stream that contains
        the combined data from both streams.
        """
        from orcapod.core.operators import Join

        return Join()(self, other_stream, label=label)  # type: ignore

    def semi_join(
        self,
        other_stream: Stream,
        label: str | None = None,
    ) -> Stream:
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
    ) -> Stream:
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
    ) -> Stream:
        """
        Maps the packets in this stream according to the provided packet_map.
        If drop_unmapped is True, any packets that are not in the packet_map will be dropped.
        """
        from orcapod.core.operators import MapPackets

        return MapPackets(name_map, drop_unmapped)(self, label=label)  # type: ignore

    def batch(
        self,
        batch_size: int = 0,
        drop_partial_batch: bool = False,
        label: str | None = None,
    ) -> Stream:
        """
        Batch stream into fixed-size chunks, each of size batch_size.
        If drop_last is True, any remaining elements that don't fit into a full batch will be dropped.
        """
        from orcapod.core.operators import Batch

        return Batch(batch_size=batch_size, drop_partial_batch=drop_partial_batch)(
            self, label=label
        )  # type: ignore

    def polars_filter(
        self,
        *predicates: Any,
        constraint_map: Mapping[str, Any] | None = None,
        label: str | None = None,
        **constraints: Any,
    ) -> Stream:
        from orcapod.core.operators import PolarsFilter

        total_constraints = dict(constraint_map) if constraint_map is not None else {}

        total_constraints.update(constraints)

        return PolarsFilter(predicates=predicates, constraints=total_constraints)(
            self, label=label
        )

    def select_tag_columns(
        self,
        tag_columns: str | Collection[str],
        strict: bool = True,
        label: str | None = None,
    ) -> Stream:
        """
        Select the specified tag columns from the stream. A ValueError is raised
        if one or more specified tag columns do not exist in the stream unless strict = False.
        """
        from orcapod.core.operators import SelectTagColumns

        return SelectTagColumns(tag_columns, strict=strict)(self, label=label)

    def select_packet_columns(
        self,
        packet_columns: str | Collection[str],
        strict: bool = True,
        label: str | None = None,
    ) -> Stream:
        """
        Select the specified packet columns from the stream. A ValueError is raised
        if one or more specified packet columns do not exist in the stream unless strict = False.
        """
        from orcapod.core.operators import SelectPacketColumns

        return SelectPacketColumns(packet_columns, strict=strict)(self, label=label)

    def drop_tag_columns(
        self,
        tag_columns: str | Collection[str],
        strict: bool = True,
        label: str | None = None,
    ) -> Stream:
        from orcapod.core.operators import DropTagColumns

        return DropTagColumns(tag_columns, strict=strict)(self, label=label)

    def drop_packet_columns(
        self,
        packet_columns: str | Collection[str],
        strict: bool = True,
        label: str | None = None,
    ) -> Stream:
        from orcapod.core.operators import DropPacketColumns

        return DropPacketColumns(packet_columns, strict=strict)(self, label=label)

    @abstractmethod
    def keys(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[tuple[str, ...], tuple[str, ...]]: ...

    @abstractmethod
    def output_schema(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[PythonSchema, PythonSchema]: ...

    def __iter__(
        self,
    ) -> Iterator[tuple[Tag, Packet]]:
        return self.iter_packets()

    @abstractmethod
    def iter_packets(
        self,
    ) -> Iterator[tuple[Tag, Packet]]: ...

    @abstractmethod
    def as_table(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> "pa.Table": ...

    def as_polars_df(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> "pl.DataFrame":
        """
        Convert the entire stream to a Polars DataFrame.
        """
        return pl.DataFrame(
            self.as_table(
                columns=columns,
                all_info=all_info,
            )
        )

    def as_df(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> "pl.DataFrame":
        """
        Convert the entire stream to a Polars DataFrame.
        """
        return self.as_polars_df(
            columns=columns,
            all_info=all_info,
        )

    def as_lazy_frame(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> "pl.LazyFrame":
        """
        Convert the entire stream to a Polars LazyFrame.
        """
        df = self.as_polars_df(
            columns=columns,
            all_info=all_info,
        )
        return df.lazy()

    def as_pandas_df(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        index_by_tags: bool = False,
        all_info: bool = False,
    ) -> "pd.DataFrame":
        df = self.as_polars_df(
            columns=columns,
            all_info=all_info,
        )
        tag_keys, _ = self.keys()
        pdf = df.to_pandas()
        if index_by_tags:
            pdf = pdf.set_index(list(tag_keys))
        return pdf

    def flow(
        self,
    ) -> Collection[tuple[Tag, Packet]]:
        """
        Flow everything through the stream, returning the entire collection of
        (Tag, Packet) as a collection. This will tigger any upstream computation of the stream.
        """
        return [e for e in self.iter_packets()]

    def _repr_html_(self) -> str:
        df = self.as_polars_df()
        # reorder columns
        new_column_order = [c for c in df.columns if c in self.keys()[0]] + [
            c for c in df.columns if c not in self.keys()[0]
        ]
        df = df[new_column_order]
        tag_map = {t: f"*{t}" for t in self.keys()[0]}
        # TODO: construct repr html better
        df = df.rename(tag_map)
        return f"{self.__class__.__name__}[{self.label}]\n" + df._repr_html_()

    def view(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> "StreamView":
        df = self.as_polars_df(
            columns=columns,
            all_info=all_info,
        )
        tag_map = {t: f"*{t}" for t in self.keys()[0]}
        # TODO: construct repr html better
        df = df.rename(tag_map)
        return StreamView(self, df)


class StreamView:
    def __init__(self, stream: StreamBase, view_df: "pl.DataFrame") -> None:
        self._stream = stream
        self._view_df = view_df

    def _repr_html_(self) -> str:
        return (
            f"{self._stream.__class__.__name__}[{self._stream.label}]\n"
            + self._view_df._repr_html_()
        )
