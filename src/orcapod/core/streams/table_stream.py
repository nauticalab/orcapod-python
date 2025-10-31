import logging
from collections.abc import Collection, Iterator
from itertools import repeat
from typing import TYPE_CHECKING, Any, cast

from orcapod import contexts
from orcapod.core.datagrams import (
    ArrowPacket,
    ArrowTag,
    DictTag,
)
from orcapod.core.system_constants import constants
from orcapod.protocols import core_protocols as cp
from orcapod.types import PythonSchema
from orcapod.utils import arrow_utils
from orcapod.utils.lazy_module import LazyModule
from orcapod.core.streams.base import ImmutableStream

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

logger = logging.getLogger(__name__)


class TableStream(ImmutableStream):
    """
    An immutable stream based on a PyArrow Table.
    This stream is designed to be used with data that is already in a tabular format,
    such as data loaded from a file or database. The columns to be treated as tags are
    specified at initialization, and the rest of the columns are treated as packets.
    The stream is immutable, meaning that once it is created, it cannot be modified.
    This is useful for ensuring that the data in the stream remains consistent and unchanging.

    The types of the tag and packet columns are inferred from the PyArrow Table schema.
    """

    def __init__(
        self,
        table: "pa.Table",
        tag_columns: Collection[str] = (),
        system_tag_columns: Collection[str] = (),
        source_info: dict[str, str | None] | None = None,
        source: cp.Kernel | None = None,
        upstreams: tuple[cp.Stream, ...] = (),
        **kwargs,
    ) -> None:
        super().__init__(source=source, upstreams=upstreams, **kwargs)

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
                }
            )

        prefix_info = {constants.SOURCE_PREFIX: source_info}

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
        # now table should only contain tag columns and packet columns
        self._packet_columns = tuple(
            c for c in table.column_names if c not in self._all_tag_columns
        )
        self._table = table
        self._source_info_table = prefix_tables[constants.SOURCE_PREFIX]
        self._data_context_table = data_context_table

        if len(self._packet_columns) == 0:
            raise ValueError(
                "No packet columns found in the table. At least one packet column is required."
            )

        tag_schema = pa.schema(
            f for f in self._table.schema if f.name in self._tag_columns
        )
        system_tag_schema = pa.schema(
            f for f in self._table.schema if f.name in self._system_tag_columns
        )
        all_tag_schema = arrow_utils.join_arrow_schemas(tag_schema, system_tag_schema)
        packet_schema = pa.schema(
            f for f in self._table.schema if f.name in self._packet_columns
        )

        self._tag_schema = tag_schema
        self._system_tag_schema = system_tag_schema
        self._all_tag_schema = all_tag_schema
        self._packet_schema = packet_schema
        # self._tag_converter = SemanticConverter.from_semantic_schema(
        #     schemas.SemanticSchema.from_arrow_schema(
        #         tag_schema, self._data_context.semantic_type_registry
        #     )
        # )
        # self._packet_converter = SemanticConverter.from_semantic_schema(
        #     schemas.SemanticSchema.from_arrow_schema(
        #         packet_schema, self._data_context.semantic_type_registry
        #     )
        # )

        self._cached_elements: list[tuple[cp.Tag, ArrowPacket]] | None = None
        self._set_modified_time()  # set modified time to now

    def data_content_identity_structure(self) -> Any:
        """
        Returns a hash of the content of the stream.
        This is used to identify the content of the stream.
        """
        table_hash = self.data_context.arrow_hasher.hash_table(
            self.as_table(
                include_data_context=True, include_source=True, include_system_tags=True
            ),
        )
        return (
            self.__class__.__name__,
            table_hash,
            self._tag_columns,
        )

    def keys(
        self, include_system_tags: bool = False
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        """
        Returns the keys of the tag and packet columns in the stream.
        This is useful for accessing the columns in the stream.
        """
        tag_columns = self._tag_columns
        if include_system_tags:
            tag_columns += self._system_tag_columns
        return tag_columns, self._packet_columns

    def types(
        self, include_system_tags: bool = False
    ) -> tuple[PythonSchema, PythonSchema]:
        """
        Returns the types of the tag and packet columns in the stream.
        This is useful for accessing the types of the columns in the stream.
        """
        # TODO: consider using MappingProxyType to avoid copying the dicts
        converter = self.data_context.type_converter
        if include_system_tags:
            tag_schema = self._all_tag_schema
        else:
            tag_schema = self._tag_schema
        return (
            converter.arrow_schema_to_python_schema(tag_schema),
            converter.arrow_schema_to_python_schema(self._packet_schema),
        )

    def as_table(
        self,
        include_data_context: bool = False,
        include_source: bool = False,
        include_system_tags: bool = False,
        include_content_hash: bool | str = False,
        sort_by_tags: bool = True,
        execution_engine: cp.ExecutionEngine | None = None,
        execution_engine_opts: dict[str, Any] | None = None,
    ) -> "pa.Table":
        """
        Returns the underlying table representation of the stream.
        This is useful for converting the stream to a table format.
        """
        output_table = self._table
        if include_content_hash:
            hash_column_name = (
                "_content_hash"
                if include_content_hash is True
                else include_content_hash
            )
            content_hashes = [
                str(packet.content_hash()) for _, packet in self.iter_packets()
            ]
            output_table = output_table.append_column(
                hash_column_name, pa.array(content_hashes, type=pa.large_string())
            )
        if not include_system_tags:
            # Check in original implementation
            output_table = output_table.drop_columns(list(self._system_tag_columns))
        table_stack = (output_table,)
        if include_data_context:
            table_stack += (self._data_context_table,)
        if include_source:
            table_stack += (self._source_info_table,)

        table = arrow_utils.hstack_tables(*table_stack)

        if sort_by_tags:
            # TODO: cleanup the sorting tag selection logic
            try:
                target_tags = (
                    self._all_tag_columns if include_system_tags else self._tag_columns
                )
                return table.sort_by([(column, "ascending") for column in target_tags])
            except pa.ArrowTypeError:
                # If sorting fails, fall back to unsorted table
                return table

        return table

    def clear_cache(self) -> None:
        """
        Resets the cached elements of the stream.
        This is useful for re-iterating over the stream.
        """
        self._cached_elements = None

    def iter_packets(
        self,
        execution_engine: cp.ExecutionEngine | None = None,
        execution_engine_opts: dict[str, Any] | None = None,
    ) -> Iterator[tuple[cp.Tag, ArrowPacket]]:
        """
        Iterates over the packets in the stream.
        Each packet is represented as a tuple of (Tag, Packet).
        """
        # TODO: make it work with table batch stream
        if self._cached_elements is None:
            cached_elements = []
            tag_present = len(self._all_tag_columns) > 0
            if tag_present:
                tags = self._table.select(self._all_tag_columns)
                tag_batches = tags.to_batches()
            else:
                tag_batches = repeat(DictTag({}))

            # TODO: come back and clean up this logic

            packets = self._table.select(self._packet_columns)

            for tag_batch, packet_batch in zip(tag_batches, packets.to_batches()):
                for i in range(len(packet_batch)):
                    if tag_present:
                        tag = ArrowTag(
                            tag_batch.slice(i, 1),  # type: ignore
                            data_context=self.data_context,
                        )

                    else:
                        tag = cast(DictTag, tag_batch)

                    packet = ArrowPacket(
                        packet_batch.slice(i, 1),
                        source_info=self._source_info_table.slice(i, 1).to_pylist()[0],
                        data_context=self.data_context,
                    )

                    yield tag, packet

                    cached_elements.append((tag, packet))
            self._cached_elements = cached_elements
        else:
            yield from self._cached_elements

    def run(
        self,
        *args: Any,
        execution_engine: cp.ExecutionEngine | None = None,
        execution_engine_opts: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        """
        Runs the stream, which in this case is a no-op since the stream is immutable.
        This is typically used to trigger any upstream computation of the stream.
        """
        # No-op for immutable streams
        pass

    async def run_async(
        self,
        *args: Any,
        execution_engine: cp.ExecutionEngine | None = None,
        execution_engine_opts: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        """
        Runs the stream asynchronously, which in this case is a no-op since the stream is immutable.
        This is typically used to trigger any upstream computation of the stream.
        """
        # No-op for immutable streams
        pass

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(table={self._table.column_names}, "
            f"tag_columns={self._tag_columns})"
        )
