from __future__ import annotations

import logging
from abc import abstractmethod
from collections.abc import AsyncIterator, Callable, Collection, Iterator, Mapping
from contextvars import ContextVar
from datetime import datetime
from typing import TYPE_CHECKING, Any, cast

from orcapod.core.base import TraceableBase
from orcapod.core.streams.view import (
    SCHEMA_NEEDS_POLL_NOTE,
    ViewStatus,
    StreamView,
    default_note,
    preview_html,
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


def short_stream_repr(stream: object) -> str:
    """Name a stream by its class and label only, as a derived stream's ``repr`` lists it.

    A derived stream's ``repr`` names its upstreams this way, never by their own
    ``repr``: that one names their upstreams in turn, so a graph whose stages split
    and join again would print every path, 2 ** stages long (ITL-1111).

    Args:
        stream: An upstream stream.

    Returns:
        ``<ClassName 'label'>``, or ``<ClassName>`` when the stream has no label.
    """
    label = getattr(stream, "label", None)
    name = type(stream).__name__
    return f"<{name} {label!r}>" if label is not None else f"<{name}>"


ZERO_ROW_INPUT_COLUMNS = ColumnConfig(system_tags=True, source=True)
"""The column groups of a zero-row table: what a node's rows carry downstream."""


_ZERO_ROW_MEMO: ContextVar[dict[int, tuple[object, pa.Table | None]] | None] = (
    ContextVar("_ZERO_ROW_MEMO", default=None)
)
"""The zero-row tables derived during one outermost ``typed_zero_row_table`` call."""


def typed_zero_row_table(stream: object) -> pa.Table | None:
    """Return ``stream``'s table with no rows and the Arrow types its rows would have.

    The table has the user tags, system tags, data and source-info columns
    (``ZERO_ROW_INPUT_COLUMNS``; never a meta column) in the order the stream's own table has them, with
    the Arrow types, nullability and logical types the stream's rows have (ITL-912). Getting it never computes, reads or polls a row:
    an in-memory table is sliced, a source answers from the table or schema it already
    holds, and an operator node applies its operator to its inputs' zero-row tables, as
    the sync path applies it to zero-row inputs.

    A stream that does not say (``_typed_zero_row_table``) cannot know its Arrow types
    without rows. The caller then builds the table from the Python output schema.

    Within one outermost call each stream is derived once, so an upstream that several
    inputs share is not derived again per path; the memo is discarded when that call
    returns and is never shared between threads or tasks.

    Args:
        stream: The stream, node or source whose zero-row table is wanted.

    Returns:
        The zero-row table, or ``None`` when ``stream`` cannot type it without rows:
        a function pod's stream or node (ITL-985), an unbound input slot, a source
        that reads, polls or derives its rows later, a pod stream not yet computed.
    """
    memo = _ZERO_ROW_MEMO.get()
    if memo is None:
        # The outermost call: a memo for this derivation only, so an upstream that
        # several inputs share (a diamond) is derived once. ``reset`` discards it, and
        # a ContextVar keeps it apart from other threads and tasks.
        token = _ZERO_ROW_MEMO.set({})
        try:
            return typed_zero_row_table(stream)
        finally:
            _ZERO_ROW_MEMO.reset(token)
    known = memo.get(id(stream))
    if known is not None and known[0] is stream:
        return known[1]
    table = _derive_zero_row_table(stream)
    # The stream is held with its table, so its id is not reused during the call.
    memo[id(stream)] = (stream, table)
    return table


def typed_zero_row_input(stream: object) -> pa.Table | None:
    """Return ``stream``'s zero-row table as a pod reads it: in the large-normalized form.

    What a function pod or an operator takes from an input's zero-row table
    (``typed_zero_row_table``): the input's types in the form its rows have downstream
    (``arrow_utils.normalize_to_large_types`` with ``keep_null=True``: ``string`` to
    ``large_string``, ``binary`` to ``large_binary``, ``list`` to ``large_list``,
    recursively), as the populated path normalizes them (ITL-912). Only layouts change:
    width, nullability, field names and logical types are kept, and no value is cast
    (there is none). A source's own table is not changed: ``typed_zero_row_table`` still
    gives it as it holds it.

    Args:
        stream: The input whose zero-row table a pod reads.

    Returns:
        The normalized zero-row table, or ``None`` when ``stream`` cannot type it
        without rows.
    """
    table = typed_zero_row_table(stream)
    if table is None:
        return None
    return table.cast(
        pa.schema(
            [
                field.with_type(
                    arrow_utils.normalize_to_large_types(field.type, keep_null=True)
                )
                for field in table.schema
            ]
        ).with_metadata(table.schema.metadata or {})
    )


def _derive_zero_row_table(stream: object) -> pa.Table | None:
    """Ask ``stream`` for its zero-row table and drop the meta columns; no memo."""
    method = getattr(stream, "_typed_zero_row_table", None)
    table = cast("pa.Table | None", method()) if callable(method) else None
    if table is None:
        return None
    # A row passed downstream carries no meta column (``_rows_to_table`` reads a row's
    # data with source info only), so neither does its zero-row table: a cached
    # function pod's ``__computed`` stays in that stream's own table.
    from orcapod.core.operators.meta_columns import without_meta_columns

    tag_columns = cast(StreamProtocol, stream).keys()[0]
    return without_meta_columns(table, tag_columns)


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


def _column_names(columns: tuple[str | Collection[str], ...]) -> tuple[str, ...]:
    """Normalize the positional names of ``drop_columns`` and ``select_columns``.

    The names are given as one or more strings, or as one collection of strings
    that is the only positional argument.

    Args:
        columns: The positional arguments as received.

    Returns:
        The names, in the order given.

    Raises:
        TypeError: When no name is given, a collection is mixed with further
            positional arguments, or a name is not a string.
        ValueError: When the one collection given is empty.
    """
    if not columns:
        raise TypeError("expected at least one column name")
    first = columns[0]
    if isinstance(first, str):
        names: tuple[object, ...] = columns
    elif len(columns) > 1:
        raise TypeError(
            "give the column names either as strings or as one collection of "
            "strings, not both"
        )
    elif isinstance(first, Collection):
        names = tuple(first)
        if not names:
            raise ValueError(
                "expected at least one column name, got an empty collection"
            )
    else:
        raise TypeError(f"expected column names as strings, got {type(first).__name__}")
    not_strings = [name for name in names if not isinstance(name, str)]
    if not_strings:
        raise TypeError(f"column names must be strings, got {not_strings!r}")
    return tuple(str(name) for name in names)


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
        """Reduce rows sharing a key into one row per group.

        Group-key columns stay scalar tags and are the only tags of the output;
        every other tag is dropped, and every data column becomes list-valued.
        A dropped tag's values are not carried: make the column data before
        grouping if a later step needs it.  See
        ``orcapod.core.operators.GroupBy``.

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

    def drop_columns(
        self,
        *columns: str | Collection[str],
        strict: bool = True,
        label: str | None = None,
    ) -> StreamBase:
        """Drop the named columns, whether each is a tag or a data column.

        ``s.drop_columns("a", "b")`` equals ``s.drop_columns(["a", "b"])``. A
        dropped data column's source-info column goes with it; system tags are
        kept. See ``orcapod.core.operators.DropColumns``.

        Args:
            *columns: One or more column names, or one collection of names as the
                only positional argument.
            strict: When ``True`` (the default), a name that is neither a tag nor
                a data column raises ``InputValidationError``; when ``False`` it
                is ignored.
            label: Optional node label for the pipeline graph.

        Returns:
            The stream without the named columns.

        Raises:
            TypeError: When no name is given, a collection is mixed with further
                positional arguments, or a name is not a string.
            ValueError: When the one collection given is empty.
            InputValidationError: When a ``strict`` name is missing, or when no
                data column would remain.
        """
        from orcapod.core.operators import DropColumns

        return DropColumns(_column_names(columns), strict=strict)(self, label=label)

    def select_columns(
        self,
        *columns: str | Collection[str],
        strict: bool = True,
        label: str | None = None,
    ) -> StreamBase:
        """Keep exactly the named columns, tag or data; drop every other one.

        ``s.select_columns("a", "b")`` equals ``s.select_columns(["a", "b"])``.
        Every user tag and data column not named is dropped, with the source-info
        column of each dropped data column; system tags are kept. Naming no tag
        is allowed. See ``orcapod.core.operators.SelectColumns``.

        Args:
            *columns: One or more column names, or one collection of names as the
                only positional argument.
            strict: When ``True`` (the default), a name that is neither a tag nor
                a data column raises ``InputValidationError``; when ``False`` it
                is ignored.
            label: Optional node label for the pipeline graph.

        Returns:
            The stream with only the named columns.

        Raises:
            TypeError: When no name is given, a collection is mixed with further
                positional arguments, or a name is not a string.
            ValueError: When the one collection given is empty.
            InputValidationError: When a ``strict`` name is missing, or when no
                data column is named.
        """
        from orcapod.core.operators import SelectColumns

        return SelectColumns(_column_names(columns), strict=strict)(self, label=label)

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

    # Predicted output schemas by normalized column config; see
    # ``_memoized_output_schema``. Set on first use, so a subclass that asks for its
    # schema before ``StreamBase.__init__`` runs still works.
    _output_schema_memo: dict[ColumnConfig, tuple[Schema, Schema]] | None = None

    def _memoized_output_schema(
        self,
        columns: ColumnConfig | dict[str, Any] | None,
        all_info: bool,
        predict: Callable[[], tuple[Schema, Schema]],
    ) -> tuple[Schema, Schema]:
        """Return ``predict()``, computed once per normalized column config (ITL-1111).

        A stream derived from upstreams predicts its schema by asking them for
        theirs. Without a memo, a graph in which one stream feeds two branches that
        join again asks the shared upstream once per path, so stacked
        split-and-join stages cost 4x per stage. With it, every graph walk that
        asks for schemas is linear in the nodes.

        Only the prediction is kept: a ``(tag_schema, data_schema)`` pair of
        immutable ``Schema`` objects, never rows or a table. The caller states what
        its prediction depends on, and clears the memo
        (``_forget_output_schema``) when one of those things changes. A config
        that cannot be a dict key (``meta`` given as a list of names) is predicted
        every time.

        Args:
            columns: The column config as the caller received it.
            all_info: The ``all_info`` flag as the caller received it.
            predict: Computes the schema from ``columns`` and ``all_info``.

        Returns:
            The predicted ``(tag_schema, data_schema)``.
        """
        config = ColumnConfig.handle_config(columns, all_info=all_info)
        try:
            hash(config)
        except TypeError:
            return predict()
        if self._output_schema_memo is None:
            self._output_schema_memo = {}
        memo = self._output_schema_memo
        if config not in memo:
            memo[config] = predict()
        return memo[config]

    def _forget_output_schema(self) -> None:
        """Drop the memoized output schemas: an input of the prediction changed."""
        self._output_schema_memo = None

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

    # The origin a ``run()`` view gives its rows: computed by this stream.
    _RUN_ORIGIN: str = "computed"

    def run(self) -> StreamView:
        """Compute the whole stream and return a ``StreamView`` of all its rows.

        The one way to compute any stream outside a job (developer, November 13, 2026,
        ITL-1081): ``head()`` and the notebook preview never compute a row (see
        ``head``). ``run()`` does
        exactly the work ``as_table()`` does, through the same code path and cache, so
        a stream already computed is not computed again, and one whose inputs changed is
        computed again. The view holds every row with the default column configuration;
        its ``head``, ``tail`` and ``sort_by_tags`` choose what is shown. For other
        column groups, use ``as_table(columns=...)`` or ``head(columns=...)`` after
        ``run()``. On a stream that keeps its rows (a pod's or operator's output stream,
        a job node, a job's side-effect node included, an in-memory source) they read the
        rows ``run()`` gave and deliver no side effect; a stream that keeps none does its
        ``as_table()`` work again (a side-effect stream outside a job delivers again, a
        polling source polls again).

        Returns:
            A ``StreamView`` of all the rows ``as_table()`` returns, with status
            ``ROWS``.
        """
        return self._view_of_rows(self.as_table(), self._RUN_ORIGIN)

    def _view_of_rows(self, table: "pa.Table", origin: str) -> StreamView:
        """Return the ``run()`` view of ``table``, every row of this stream."""
        return StreamView.of_rows(
            self,
            table,
            tag_columns=self.keys()[0],
            total_rows=table.num_rows,
            origin=origin,
            display_all_rows=False,
        )

    def head(
        self,
        n: int = 10,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> StreamView:
        """Return at most ``n`` rows that are readily available, without computing.

        The rows are ones the stream already holds, has cached from an earlier
        computation, has stored, or can read with a bounded read of its table. ``head``
        never computes, polls or delivers a row (it reads only what is held or stored,
        or makes a bounded table read), never calls user code (a
        function, a predicate, a side effect) and never writes. With no rows to show, it
        learns the column types without data (ITL-912): each operator of the stream is
        applied to zero-row inputs, which processes no data, so a chain of ``N``
        operators applies up to ``N`` operators to empty tables (about 60 ms for 20); an
        operator whose output schema is its input's (``OUTPUT_SCHEMA_IS_INPUT_SCHEMA``,
        such as ``PolarsFilter``) is not applied at all. A user-defined operator's
        ``static_process`` is applied to zero-row tables and must not have effects. So a pipeline whose inputs
        disagree on a column's Arrow type raises from ``head`` the
        ``InputValidationError`` its run raises, and a filter with an ill-typed
        predicate shows its input's columns: the error appears at the run. ``head`` takes the column configuration of ``as_table``,
        sorting included, and applies it to the rows the stream has, then takes the first
        ``n``. The default configuration does not sort, as ``as_table()`` does not, so
        the rows are the first ``n`` that ``as_table()`` would return. With
        ``sort_by_tags`` (or ``all_info``) a stream that has all of its rows (a node's
        stored slice, a computed stream, an in-memory source) returns the first ``n``
        of ``as_table`` with that configuration. A stream that has read or computed only
        part of its rows (a source's bounded read, a partly computed stream) sorts those
        rows: the rows read, in tag order, not the first ``n`` in tag order. The
        notebook preview of a stream is its ``head()`` asked for ``sort_by_tags``.

        This default shows no rows: a stream class that can serve rows without computing
        overrides it. The zero-row table then has the stream's columns, typed from its
        declared (Python) schema, so a column's Arrow width can differ from the one the
        computed stream will have (``int`` shows as ``int64``). That zero-row table has
        the declared columns only: never a ``_source_*``, ``_context_key`` or
        content-hash column, whatever ``columns`` asks for, because whether such a column
        exists and, for ``_source_*``, its type are known only from rows.

        Args:
            n: The largest number of rows to return.
            columns: Column groups to include and whether to sort by tags, exactly as
                for ``as_table``.
            all_info: Include every column group and sort by tags, as for ``as_table``.

        Returns:
            A ``StreamView`` with the rows and a ``status`` that tells an empty result
            from one that is not computed.

        Raises:
            TypeError: If ``n`` is not an ``int``.
            ValueError: If ``n`` is negative.
        """
        check_head_size(n)
        config = ColumnConfig.handle_config(columns, all_info=all_info)
        return self._head_without_rows(config, ViewStatus.NOT_COMPUTED)

    def _head_without_rows(
        self,
        config: ColumnConfig,
        status: ViewStatus,
        note: str | None = None,
    ) -> StreamView:
        """Return a head with no rows: the stream's columns, typed as its rows are.

        The one place a head's table of no rows is built (ITL-912): the stream's
        ``_zero_row_table(config)``, which has the columns and Arrow types
        ``as_table(columns=config)`` gives the stream with no rows; getting it
        computes, reads, polls and delivers nothing. Only a stream that cannot say its
        Arrow types without rows (see ``typed_zero_row_table``; ITL-1089) gets a table
        built from its declared Python schema. When reading the schema would poll a source
        (``schema_requires_io``), the head has no columns either, and the note says so.
        """
        if schema_requires_io(self, config):
            return StreamView.without_rows(
                self,
                status,
                pa.table({}),
                tag_columns=(),
                note=f"{note or default_note(status)}; {SCHEMA_NEEDS_POLL_NOTE}",
            )
        tag_schema, data_schema = self.output_schema(columns=config)
        table = self._zero_row_table(config)
        if table is None:
            table = arrow_utils.make_empty_table(
                {**tag_schema, **data_schema}, self.data_context.type_converter
            )
        return StreamView.without_rows(
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

        Each upstream is visited once, however many paths lead to it (ITL-1111): a
        stream that answers like this one is passed through to its own upstreams,
        any other is asked. The answer is not kept, because a source's changes once
        it has polled.
        """
        seen: set[int] = {id(self)}
        pending: list[StreamProtocol] = list(self.upstreams)
        while pending:
            stream = pending.pop()
            if id(stream) in seen:
                continue
            seen.add(id(stream))
            hook = getattr(type(stream), "_schema_requires_io", None)
            if hook is StreamBase._schema_requires_io:
                pending.extend(stream.upstreams)
            elif schema_requires_io(stream, config):
                return True
        return False

    def _zero_row_table(self, config: ColumnConfig) -> pa.Table | None:
        """This stream's table with no rows and the column groups of ``config``.

        What ``as_table(columns=config)`` gives this stream when it has no rows: its
        zero-row table (``typed_zero_row_table``) shaped as an ``ArrowTableStream``
        shapes its table. A kind whose ``as_table`` shapes its table another way
        overrides this with that shaping. Never computes, reads, polls or delivers.

        Returns:
            The table, or ``None`` when the stream cannot type it without rows.
        """
        from orcapod.core.streams.arrow_table_stream import ArrowTableStream

        typed = typed_zero_row_table(self)
        if typed is None:
            return None
        return ArrowTableStream(
            typed, tag_columns=self.keys()[0], data_context=self.data_context_key
        ).as_table(columns=config)

    def _typed_zero_row_table(self) -> pa.Table | None:
        """This stream's zero-row table; ``None``: unknown without rows.

        See ``typed_zero_row_table``. A stream whose Arrow types are known only from
        computed rows (a pod's stream) keeps this default.
        """
        return None

    def _repr_html_(self) -> str:
        """Render ``head()`` in tag order for a notebook; never computes."""
        return preview_html(self)
