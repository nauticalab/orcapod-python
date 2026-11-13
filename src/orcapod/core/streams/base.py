from __future__ import annotations

import dataclasses
import logging
from abc import abstractmethod
from collections.abc import AsyncIterator, Collection, Iterator, Mapping
from datetime import datetime
from typing import TYPE_CHECKING, Any

from orcapod.core.base import TraceableBase
from orcapod.core.streams.head import (
    SCHEMA_NEEDS_POLL_NOTE,
    HeadStatus,
    StreamHead,
    default_note,
    render_error_html,
)
from orcapod.protocols.core_protocols import (
    DataProtocol,
    PodProtocol,
    StreamProtocol,
    TagProtocol,
)
from orcapod.types import ColumnConfig, Schema
from orcapod.utils import arrow_utils
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


def schema_requires_io(stream: object, config: ColumnConfig) -> bool:
    """Whether reading ``stream``'s schema with ``config`` would read or poll a source.

    A stream that does not say (``_schema_requires_io``) is taken not to.

    Args:
        stream: The stream whose schema would be read.
        config: The column groups the schema would be read with: source info, meta,
            context and content hashes may be known only from rows.
    """
    check = getattr(stream, "_schema_requires_io", None)
    return bool(check(config)) if callable(check) else False


def check_head_size(n: int) -> None:
    """Check the row count given to ``head``.

    Raises:
        TypeError: If ``n`` is not an ``int``.
        ValueError: If ``n`` is negative.
    """
    if isinstance(n, bool) or not isinstance(n, int):
        raise TypeError(f"head() takes an int number of rows, got {type(n).__name__}")
    if n < 0:
        raise ValueError(f"head() takes a non-negative number of rows n, got {n}")


def head_column_config(
    columns: ColumnConfig | dict[str, Any] | None, all_info: bool
) -> ColumnConfig:
    """Return the column configuration ``head`` uses for ``columns`` and ``all_info``.

    The column groups mean what they mean for ``as_table``. ``head`` returns rows in
    stream order, so ``all_info`` does not sort, and asking for ``sort_by_tags`` raises.

    Raises:
        ValueError: If ``columns`` asks for ``sort_by_tags``.
    """
    if isinstance(columns, ColumnConfig):
        asked_to_sort = bool(columns.sort_by_tags)
    elif isinstance(columns, Mapping):
        asked_to_sort = bool(columns.get("sort_by_tags", False))
    else:
        asked_to_sort = False
    if asked_to_sort:
        raise ValueError(
            "head() returns the first rows in stream order and cannot sort them by "
            "tags; sort the result of as_table() instead"
        )
    config = ColumnConfig.handle_config(columns, all_info=all_info)
    return dataclasses.replace(config, sort_by_tags=False)


class StreamBase(TraceableBase):
    @property
    @abstractmethod
    def producer(self) -> PodProtocol | None: ...

    @property
    @abstractmethod
    def upstreams(self) -> tuple[StreamProtocol, ...]: ...

    def identity_structure(self) -> Any:
        if self.producer is not None:
            return (self.producer, self.producer.argument_symmetry(self.upstreams))

        raise NotImplementedError("StreamBase.identity_structure")

    def pipeline_identity_structure(self) -> Any:
        return self.identity_structure()

    @property
    def is_stale(self) -> bool:
        """
        True if any upstream stream or the source pod has a ``last_modified``
        timestamp strictly newer than this stream's own ``last_modified``,
        indicating that any in-memory cached content should be discarded and
        repopulated.

        Semantics:
        - A ``None`` timestamp on *this* stream means "content not yet
          established" → always stale.
        - A ``None`` timestamp on an upstream or source means "modification
          time unknown" → conservatively treat as stale.
        - Immutable streams with no upstreams and no source (e.g.
          ``ArrowTableStream``) always return ``False``.
        """
        own_time: datetime | None = self.last_modified
        if own_time is None:
            return True
        candidates: list[datetime | None] = [s.last_modified for s in self.upstreams]
        if self.producer is not None:
            candidates.append(self.producer.last_modified)
        return any(t is None or t > own_time for t in candidates)

    def computed_label(self) -> str | None:
        if self.producer is not None:
            # use the invocation operation label
            return self.producer.label
        return None

    def join(
        self, other_stream: StreamProtocol, label: str | None = None
    ) -> StreamBase:
        """
        Joins this stream with another stream, returning a new stream that contains
        the combined data from both streams.
        """
        from orcapod.core.operators import Join

        return Join()(self, other_stream, label=label)

    def semi_join(
        self,
        other_stream: StreamProtocol,
        label: str | None = None,
    ) -> StreamBase:
        """
        Performs a semi-join with another stream, returning a new stream that contains
        only the data from this stream that have matching tags in the other stream.
        """
        from orcapod.core.operators import SemiJoin

        return SemiJoin()(self, other_stream, label=label)

    def map_tags(
        self,
        name_map: Mapping[str, str],
        drop_unmapped: bool = False,
        label: str | None = None,
    ) -> StreamBase:
        """
        Maps the tags in this stream according to the provided name_map.
        Tags not in the name_map are kept unless drop_unmapped is True.
        """
        from orcapod.core.operators import MapTags

        return MapTags(name_map, drop_unmapped)(self, label=label)

    def map_data(
        self,
        name_map: Mapping[str, str],
        drop_unmapped: bool = False,
        label: str | None = None,
    ) -> StreamBase:
        """
        Maps the data in this stream according to the provided data_map.
        Data columns not in the name_map are kept unless drop_unmapped is True.
        """
        from orcapod.core.operators import MapData

        return MapData(name_map, drop_unmapped)(self, label=label)

    def batch(
        self,
        batch_size: int = 0,
        drop_partial_batch: bool = False,
        label: str | None = None,
    ) -> StreamBase:
        """
        Batch stream into fixed-size chunks, each of size batch_size.
        If drop_last is True, any remaining elements that don't fit into a full batch will be dropped.
        """
        from orcapod.core.operators import Batch

        return Batch(batch_size=batch_size, drop_partial_batch=drop_partial_batch)(
            self, label=label
        )

    def group_by(
        self,
        by: Collection[str],
        label: str | None = None,
    ) -> StreamBase:
        """Reduce rows sharing a tag tuple into one packet per group.

        Group-key columns stay scalar tags; every other column becomes
        list-valued and stays in its group: a non-key tag stays a tag, a data
        column stays data.  See ``orcapod.core.operators.GroupBy``.

        Args:
            by: Tag column names to group on.
            label: Optional node label for the pipeline graph.

        Returns:
            A stream with one row per distinct group-key tuple.
        """
        from orcapod.core.operators import GroupBy

        return GroupBy(by=by)(self, label=label)

    def polars_filter(
        self,
        *predicates: Any,
        constraint_map: Mapping[str, Any] | None = None,
        label: str | None = None,
        **constraints: Any,
    ) -> StreamBase:
        from orcapod.core.operators import PolarsFilter

        total_constraints = dict(constraint_map) if constraint_map is not None else {}

        total_constraints.update(constraints)

        return PolarsFilter(predicates=predicates, constraints=total_constraints)(
            self, label=label
        )

    def drop_nulls(
        self,
        columns: str | Collection[str] | None = None,
        label: str | None = None,
    ) -> StreamBase:
        """Keep the rows where every named column is non-null; narrow those columns to ``T``.

        Each named column, tag or data, is non-nullable afterwards (``T``
        instead of ``T | None``), so a pod whose parameter is a plain ``T``
        can consume it. Without names, every tag and data column of this
        stream is tested and narrowed: a row with a null in any of them is
        dropped. See ``orcapod.core.operators.DropNulls``.

        Args:
            columns: One column name, a collection of names, or ``None`` (the
                default) for every tag and data column. An empty collection
                raises ``ValueError``.
            label: Optional node label for the pipeline graph.

        Returns:
            A stream over the surviving rows, in input order.
        """
        from orcapod.core.operators import DropNulls

        return DropNulls(columns)(self, label=label)

    def select_tag_columns(
        self,
        tag_columns: str | Collection[str],
        strict: bool = True,
        label: str | None = None,
    ) -> StreamBase:
        """
        Select the specified tag columns from the stream. A ValueError is raised
        if one or more specified tag columns do not exist in the stream unless strict = False.
        """
        from orcapod.core.operators import SelectTagColumns

        return SelectTagColumns(tag_columns, strict=strict)(self, label=label)

    def select_data_columns(
        self,
        data_columns: str | Collection[str],
        strict: bool = True,
        label: str | None = None,
    ) -> StreamBase:
        """
        Select the specified data columns from the stream. A ValueError is raised
        if one or more specified data columns do not exist in the stream unless strict = False.
        """
        from orcapod.core.operators import SelectDataColumns

        return SelectDataColumns(data_columns, strict=strict)(self, label=label)

    def drop_tag_columns(
        self,
        tag_columns: str | Collection[str],
        strict: bool = True,
        label: str | None = None,
    ) -> StreamBase:
        from orcapod.core.operators import DropTagColumns

        return DropTagColumns(tag_columns, strict=strict)(self, label=label)

    def drop_data_columns(
        self,
        data_columns: str | Collection[str],
        strict: bool = True,
        label: str | None = None,
    ) -> StreamBase:
        from orcapod.core.operators import DropDataColumns

        return DropDataColumns(data_columns, strict=strict)(self, label=label)

    def pick(
        self,
        column: str,
        key: str,
        out: str | None = None,
        fail_on_miss: bool = False,
        label: str | None = None,
    ) -> StreamBase:
        """Extract a value from a struct- or dict-typed data column by key.

        For ``dict[K, V]`` columns the lookup is per-packet; if the key is absent
        the packet is skipped (or an error is raised when ``fail_on_miss=True``).

        Args:
            column: Name of the data column to project into.
            key: Dict key or struct field name to extract.
            out: Output column name. ``None`` (default) replaces ``column``
                in-place; a string adds a new column alongside the original.
            fail_on_miss: If ``True``, raise ``RuntimeError`` when the key is
                absent in a packet instead of skipping.
            label: Optional label for the operator node.

        Returns:
            A new stream with the projected column.
        """
        from orcapod.core.operators import Pick

        return Pick(column, key, out=out, fail_on_miss=fail_on_miss)(self, label=label)

    def index(
        self,
        column: str,
        i: int,
        out: str | None = None,
        fail_on_miss: bool = False,
        label: str | None = None,
    ) -> StreamBase:
        """Extract an element from a list-typed data column by position.

        Out-of-bounds access causes the packet to be skipped (or an error
        when ``fail_on_miss=True``). Negative indices follow Python semantics
        (``-1`` is the last element).

        Args:
            column: Name of the data column to project into.
            i: Position to extract. Negative indices follow Python semantics.
            out: Output column name. ``None`` (default) replaces ``column``
                in-place; a string adds a new column alongside the original.
            fail_on_miss: If ``True``, raise ``RuntimeError`` on out-of-bounds
                instead of skipping.
            label: Optional label for the operator node.

        Returns:
            A new stream with the projected column.
        """
        from orcapod.core.operators import Index

        return Index(column, i, out=out, fail_on_miss=fail_on_miss)(self, label=label)

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
    ) -> tuple[Schema, Schema]: ...

    def __iter__(
        self,
    ) -> Iterator[tuple[TagProtocol, DataProtocol]]:
        return self.iter_data()

    @abstractmethod
    def iter_data(
        self,
    ) -> Iterator[tuple[TagProtocol, DataProtocol]]: ...

    async def async_iter_data(
        self,
    ) -> AsyncIterator[tuple[TagProtocol, DataProtocol]]:
        """Async iterator over (tag, data) pairs.

        Default implementation wraps ``iter_data`` as an async generator.
        Subclasses override this to provide true async streaming behaviour
        (e.g. ``PollingSource``).
        """
        for item in self.iter_data():
            yield item

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
    ) -> list[tuple[TagProtocol, DataProtocol]]:
        """Materialize the stream into a concrete collection of
        ``(TagProtocol, DataProtocol)`` pairs.

        This is implemented by iterating over ``iter_data``. Depending on
        the concrete stream implementation, iterating may trigger computation or
        upstream work, or it may simply materialize already-computed results.
        """
        return [e for e in self.iter_data()]

    def head(
        self,
        n: int = 10,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> StreamHead:
        """Return at most ``n`` rows that are readily available, without computing.

        The rows are ones the stream already holds, has cached from an earlier
        computation, has stored, or can read with a bounded read of its table. ``head``
        never calls a function, runs an operator, delivers a side effect, polls a source
        or writes anything. When rows are shown, they are the first ``n`` rows that
        ``as_table()`` would return. The notebook preview of a stream is its ``head()``.

        This default shows no rows: a stream class that can serve rows without computing
        overrides it. The zero-row table then has the stream's columns, typed from its
        declared (Python) schema, so a column's Arrow width can differ from the one the
        computed stream will have (``int`` shows as ``int64``).

        Args:
            n: The largest number of rows to return.
            columns: Column groups to include, as for ``as_table``. ``sort_by_tags`` is
                not accepted: the rows come in stream order.
            all_info: Include every column group, as for ``as_table`` (without sorting).

        Returns:
            A ``StreamHead`` with the rows and a ``status`` that tells an empty result
            from one that is not computed.

        Raises:
            TypeError: If ``n`` is not an ``int``.
            ValueError: If ``n`` is negative or ``columns`` asks for ``sort_by_tags``.
        """
        check_head_size(n)
        config = head_column_config(columns, all_info)
        return self._head_without_rows(config, HeadStatus.NOT_COMPUTED)

    def _head_without_rows(
        self,
        config: ColumnConfig,
        status: HeadStatus,
        note: str | None = None,
    ) -> StreamHead:
        """Return a head with no rows: the stream's columns from its declared schema.

        When reading the schema would poll a source (``schema_requires_io``), the head
        has no columns either, and the note says so.
        """
        if schema_requires_io(self, config):
            return StreamHead.without_rows(
                self,
                status,
                pa.table({}),
                tag_columns=(),
                note=f"{note or default_note(status)}; {SCHEMA_NEEDS_POLL_NOTE}",
            )
        tag_schema, data_schema = self.output_schema(columns=config)
        table = arrow_utils.make_empty_table(
            {**tag_schema, **data_schema}, self.data_context.type_converter
        )
        return StreamHead.without_rows(
            self, status, table, tag_columns=tuple(tag_schema.keys()), note=note
        )

    def _display_name(self) -> str:
        """The name a preview header shows: the label. Never used as an identity."""
        return self.label

    def _schema_requires_io(self, config: ColumnConfig) -> bool:
        """Whether reading this stream's schema with ``config`` would read or poll a source.

        True when an upstream's would: a ``PollingSource`` that has not polled yet
        answers its schema only by polling when it declared no schema, or when
        ``config`` asks for a column group known only from rows.
        """
        return any(schema_requires_io(upstream, config) for upstream in self.upstreams)

    def _repr_html_(self) -> str:
        """Render ``head()`` for a notebook; never computes."""
        try:
            head = self.head()
        except Exception as error:  # noqa: BLE001 - a display hook must not raise
            return render_error_html(self, error)
        return head._repr_html_()
