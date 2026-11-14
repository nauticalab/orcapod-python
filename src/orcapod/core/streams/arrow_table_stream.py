from __future__ import annotations

import itertools
import logging
from collections.abc import Collection, Iterator
from typing import TYPE_CHECKING, Any

from orcapod import contexts
from orcapod.core.datagrams import Data, Tag
from orcapod.core.streams.base import (
    StreamBase,
    check_head_size,
)
from orcapod.core.streams.view import StreamView
from orcapod.protocols.core_protocols import PodProtocol, StreamProtocol, TagProtocol
from orcapod.protocols.hashing_protocols import PipelineElementProtocol
from orcapod.system_constants import constants
from orcapod.types import ColumnConfig, Schema, SourceInfoValue
from orcapod.utils import arrow_utils, schema_utils
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")


logger = logging.getLogger(__name__)


class ArrowTableStream(StreamBase):
    """
    An immutable stream based on a PyArrow Table.
    This stream is designed to be used with data that is already in a tabular format,
    such as data loaded from a file or database. The columns to be treated as tags are
    specified at initialization, and the rest of the columns are treated as data.
    The stream is immutable, meaning that once it is created, it cannot be modified.
    This is useful for ensuring that the data in the stream remains consistent and unchanging.

    The types of the tag and data columns are inferred from the PyArrow Table schema.
    """

    # The table's rows are held, not computed: a run() view says "all N rows", as
    # head() does.
    _RUN_ORIGIN: str = "held"

    def __init__(
        self,
        table: "pa.Table",
        tag_columns: Collection[str] = (),
        system_tag_columns: Collection[str] = (),
        source_info: dict[str, SourceInfoValue] | None = None,
        producer: PodProtocol | None = None,
        upstreams: tuple[StreamProtocol, ...] = (),
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self._producer = producer
        self._upstreams = upstreams

        data_table, data_context_table = arrow_utils.split_by_column_groups(
            table, [constants.CONTEXT_KEY]
        )
        if data_table is None:
            # TODO: provide better error message
            raise ValueError(
                "Table must contain at least one column to be used as a stream."
            )
        table = data_table

        if data_context_table is None:
            data_context_table = pa.table(
                {
                    constants.CONTEXT_KEY: pa.array(
                        [contexts.get_default_context_key()] * len(table),
                        pa.large_string(),
                    )
                },
                schema=pa.schema(
                    [pa.field(constants.CONTEXT_KEY, pa.large_string(), nullable=False)]
                ),
            )

        prefix_info = {constants.SOURCE_PREFIX: source_info}

        # A tag never has a meta column's name (ITL-1080).
        schema_utils.require_names_not_meta(tag_columns)
        # determine tag columns first and then exclude any source info
        self._tag_columns = tuple(c for c in tag_columns if c in table.column_names)
        self._system_tag_columns = tuple(
            c for c in table.column_names if c.startswith(constants.SYSTEM_TAG_PREFIX)
        )
        if len(system_tag_columns) > 0:
            # rename system_tag_columns
            column_name_map = {
                c: f"{constants.SYSTEM_TAG_PREFIX}{c}" for c in system_tag_columns
            }
            table = table.rename_columns(
                [column_name_map.get(c, c) for c in table.column_names]
            )

            self._system_tag_columns += tuple(
                f"{constants.SYSTEM_TAG_PREFIX}{c}" for c in system_tag_columns
            )

        self._all_tag_columns = self._tag_columns + self._system_tag_columns
        if delta := set(tag_columns) - set(self._tag_columns):
            raise ValueError(
                f"Specified tag columns {delta} are not present in the table."
            )
        table, prefix_tables = arrow_utils.prepare_prefixed_columns(
            table,
            prefix_info,
            exclude_columns=self._all_tag_columns,
        )
        # now table should only contain tag columns and data columns
        self._data_columns = tuple(
            c for c in table.column_names if c not in self._all_tag_columns
        )
        self._table = table
        self._source_info_table = prefix_tables[constants.SOURCE_PREFIX]
        self._data_context_table = data_context_table

        if len(self._data_columns) == 0:
            raise ValueError(
                "No data columns found in the table. At least one data column is required."
            )

        # Respect Arrow nullable flags as-is (after optional inference above):
        # nullable=True → T | None, nullable=False → T.
        tag_schema = pa.schema(
            f for f in self._table.schema if f.name in self._tag_columns
        )
        system_tag_schema = pa.schema(
            f for f in self._table.schema if f.name in self._system_tag_columns
        )
        all_tag_schema = arrow_utils.join_arrow_schemas(tag_schema, system_tag_schema)
        data_schema = pa.schema(
            f for f in self._table.schema if f.name in self._data_columns
        )

        self._tag_schema = tag_schema
        self._system_tag_schema = system_tag_schema
        self._all_tag_schema = all_tag_schema
        self._data_schema = data_schema

        self._cached_elements: list[tuple[TagProtocol, Data]] | None = None
        self._update_modified_time()  # set modified time to now

    def identity_structure(self) -> Any:
        if self._producer is not None:
            return super().identity_structure()
        # Every column group except the per-row content hash: that column is derived
        # from the data columns already in the table, and its values carry the Arrow
        # hash label, which must never enter a hash input.
        return (
            self.__class__.__name__,
            self._identity_table(),
            self._tag_columns,
        )

    def _identity_table(self) -> "pa.Table":
        """Return the rows in the order and columns that ``identity_structure`` hashes.

        THIS ORDER IS A HASH INPUT AND MUST NOT CHANGE: it is the ordering that every
        stream's content hash has been built from since before ITL-1053. It is kept apart
        from ``as_table``'s sort on purpose; the sort in the body is the code
        ``as_table(sort_by_tags=True)`` had on origin/main, unchanged: the tags and
        system tags in this stream's own priority, a type Arrow cannot order leaving the
        rows in stream order, and a stream with nothing to sort by raising.
        """
        table = self._assemble_table(
            ColumnConfig(meta=True, context=True, source=True, system_tags=True),
            limit=None,
        )
        try:
            return table.sort_by(
                [(column, "ascending") for column in self._all_tag_columns]
            )
        except pa.ArrowTypeError:
            # If sorting fails, fall back to unsorted table
            return table

    def pipeline_identity_structure(self) -> Any:
        if self._producer is None or not isinstance(
            self._producer, PipelineElementProtocol
        ):
            tag_schema, data_schema = self.output_schema()
            return (tag_schema, data_schema)
        return super().pipeline_identity_structure()

    @property
    def producer(self) -> PodProtocol | None:
        return self._producer

    @property
    def upstreams(self) -> tuple[StreamProtocol, ...]:
        return self._upstreams

    def keys(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        """
        Returns the keys of the tag and data columns in the stream.
        This is useful for accessing the columns in the stream.
        """
        tag_columns = self._tag_columns
        columns_config = ColumnConfig.handle_config(columns, all_info=all_info)
        # TODO: add standard parsing of columns
        if columns_config.system_tags:
            tag_columns += self._system_tag_columns
        return tag_columns, self._data_columns

    def output_schema(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        """
        Returns the types of the tag and data columns in the stream.
        This is useful for accessing the types of the columns in the stream.
        """
        # normalize column config
        columns_config = ColumnConfig.handle_config(columns, all_info=all_info)
        # TODO: consider using MappingProxyType to avoid copying the dicts
        converter = self.data_context.type_converter
        if columns_config.system_tags:
            tag_schema = self._all_tag_schema
        else:
            tag_schema = self._tag_schema
        return (
            converter.arrow_schema_to_python_schema(tag_schema),
            converter.arrow_schema_to_python_schema(self._data_schema),
        )

    def as_table(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> "pa.Table":
        """
        Returns the underlying table representation of the stream.
        This is useful for converting the stream to a table format.
        """
        columns_config = ColumnConfig.handle_config(columns, all_info=all_info)
        return self._assemble_table(columns_config, limit=None)

    def head(
        self,
        n: int = 10,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> StreamView:
        """Return the first ``n`` rows of the table this stream holds.

        Without sorting only those rows are assembled: content hashes, when asked for,
        are computed for them alone. With ``sort_by_tags`` (or ``all_info``) the rows are
        sorted first, as ``as_table`` sorts them, and the first ``n`` of those are
        returned, so the head is always ``as_table(...)`` cut to ``n`` rows. See
        ``StreamBase.head``.
        """
        check_head_size(n)
        config = ColumnConfig.handle_config(columns, all_info=all_info)
        if config.sort_by_tags:
            table = self._assemble_table(config, limit=None).slice(0, n)
        else:
            table = self._assemble_table(config, limit=n)
        return StreamView.of_rows(
            self,
            table,
            tag_columns=self.keys(columns=config)[0],
            total_rows=self._table.num_rows,
        )

    def _assemble_table(
        self, columns_config: ColumnConfig, limit: int | None
    ) -> "pa.Table":
        """Return the stream's rows with the column groups of ``columns_config``.

        Args:
            columns_config: The column groups to include, and whether to sort by tags.
            limit: Assemble only the first ``limit`` rows; ``None`` for all of them.
        """
        output_table = self._table
        data_context_table = self._data_context_table
        source_info_table = self._source_info_table
        if limit is not None:
            output_table = output_table.slice(0, limit)
            data_context_table = data_context_table.slice(0, limit)
            source_info_table = source_info_table.slice(0, limit)

        def hashes() -> pa.Array[Any]:
            rows = self.iter_data()
            if limit is not None:
                rows = itertools.islice(rows, limit)
            return pa.array(
                [str(data.content_hash()) for _, data in rows], type=pa.large_string()
            )

        def shape(carried: "pa.Table") -> "pa.Table":
            table_stack = (carried,)
            if columns_config.context:
                table_stack += (data_context_table,)
            if columns_config.source:
                table_stack += (source_info_table,)
            table = arrow_utils.hstack_tables(*table_stack)
            if columns_config.sort_by_tags and (
                self._tag_columns or columns_config.system_tags
            ):
                # The presentation sort (not the identity order, see
                # ``_identity_table``). A stream with no tag column is sorted only when
                # the system tags are asked for. The system tags stay in the table
                # through the sort: they break ties between equal tags.
                table = arrow_utils.sort_by_tag_columns(table, tuple(self._tag_columns))
            if not columns_config.system_tags:
                table = table.drop_columns(list(self._system_tag_columns))
            return table

        return arrow_utils.with_content_hash_column(
            output_table,
            columns_config.content_hash,
            hashes,
            shape,
            nullable=False,
        )

    def clear_cache(self) -> None:
        """
        Resets the cached elements of the stream.
        This is useful for re-iterating over the stream.
        """
        self._cached_elements = None

    def iter_data(self) -> Iterator[tuple[TagProtocol, Data]]:
        """Iterates over the rows of the stream as ``(Tag, Data)`` pairs.

        Every row of the backing table is yielded exactly once, in table order,
        regardless of how the tag, data and source-info columns are chunked.

        Yields:
            A ``(Tag, Data)`` tuple for each row.
        """
        # TODO: make it work with table batch stream
        if self._cached_elements is None:
            cached_elements = []
            tag_columns = list(self._all_tag_columns)
            # Each row's source info goes to ``Data`` as Arrow columns, so it
            # keeps the type it has here: re-typing it from Python values
            # (``to_pylist``) would infer a type the stream already knows.
            source_columns = list(self._source_info_table.column_names)
            data_columns = list(self._data_columns) + source_columns

            # The source-info table has its own chunk layout, so append its
            # columns to the backing table and batch that once.
            # ``Table.to_batches`` cuts at the union of the chunk boundaries of
            # every column, so the tag, data and source-info columns of a batch
            # always cover the same rows. Batching any of them separately and
            # zipping the results drops or mis-pairs rows whenever they are
            # chunked differently.
            batching_table = self._table
            for name in source_columns:
                batching_table = batching_table.append_column(
                    self._source_info_table.schema.field(name),
                    self._source_info_table.column(name),
                )
            for batch in batching_table.to_batches():
                tag_batch = batch.select(tag_columns)
                data_batch = batch.select(data_columns)
                for i in range(batch.num_rows):
                    if tag_columns:
                        tag = Tag(
                            tag_batch.slice(i, 1),  # type: ignore
                            data_context=self.data_context,
                        )
                    else:
                        # A fresh empty tag per row, so rows never share one object.
                        tag = Tag({})

                    data = Data(
                        data_batch.slice(i, 1),  # type: ignore
                        data_context=self.data_context,
                    )

                    yield tag, data

                    cached_elements.append((tag, data))
            self._cached_elements = cached_elements
        else:
            yield from self._cached_elements

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(table={self._table.column_names}, "
            f"tag_columns={self._tag_columns})"
        )
