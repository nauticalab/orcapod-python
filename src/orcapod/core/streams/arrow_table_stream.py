from __future__ import annotations

import logging
from collections.abc import Collection, Iterator
from itertools import repeat
from typing import TYPE_CHECKING, Any, cast

from orcapod import contexts
from orcapod.core.datagrams import Packet, Tag
from orcapod.core.streams.base import StreamBase
from orcapod.protocols.core_protocols import PodProtocol, StreamProtocol, TagProtocol
from orcapod.protocols.hashing_protocols import PipelineElementProtocol
from orcapod.system_constants import constants
from orcapod.types import ColumnConfig, Schema
from orcapod.utils import arrow_utils
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

        self._cached_elements: list[tuple[TagProtocol, Packet]] | None = None
        self._update_modified_time()  # set modified time to now

    def identity_structure(self) -> Any:
        if self._producer is not None:
            return super().identity_structure()
        return (
            self.__class__.__name__,
            self.as_table(all_info=True),
            self._tag_columns,
        )

    def pipeline_identity_structure(self) -> Any:
        if self._producer is None or not isinstance(
            self._producer, PipelineElementProtocol
        ):
            tag_schema, packet_schema = self.output_schema()
            return (tag_schema, packet_schema)
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
        Returns the keys of the tag and packet columns in the stream.
        This is useful for accessing the columns in the stream.
        """
        tag_columns = self._tag_columns
        columns_config = ColumnConfig.handle_config(columns, all_info=all_info)
        # TODO: add standard parsing of columns
        if columns_config.system_tags:
            tag_columns += self._system_tag_columns
        return tag_columns, self._packet_columns

    def output_schema(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        """
        Returns the types of the tag and packet columns in the stream.
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
            converter.arrow_schema_to_python_schema(self._packet_schema),
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
        output_table = self._table
        if columns_config.content_hash:
            hash_column_name = (
                "_content_hash"
                if columns_config.content_hash is True
                else columns_config.content_hash
            )
            content_hashes = [
                str(packet.content_hash()) for _, packet in self.iter_packets()
            ]
            output_table = output_table.append_column(
                hash_column_name, pa.array(content_hashes, type=pa.large_string())
            )
        if not columns_config.system_tags:
            # Check in original implementation
            output_table = output_table.drop_columns(list(self._system_tag_columns))
        table_stack = (output_table,)
        if columns_config.context:
            table_stack += (self._data_context_table,)
        if columns_config.source:
            table_stack += (self._source_info_table,)

        table = arrow_utils.hstack_tables(*table_stack)

        if columns_config.sort_by_tags:
            # TODO: cleanup the sorting tag selection logic
            try:
                target_tags = (
                    self._all_tag_columns
                    if columns_config.system_tags
                    else self._tag_columns
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

    def iter_packets(self) -> Iterator[tuple[TagProtocol, Packet]]:
        """
        Iterates over the packets in the stream.
        Each packet is represented as a tuple of (TagProtocol, PacketProtocol).
        """
        # TODO: make it work with table batch stream
        if self._cached_elements is None:
            cached_elements = []
            tag_present = len(self._all_tag_columns) > 0
            if tag_present:
                tags = self._table.select(self._all_tag_columns)
                tag_batches = tags.to_batches()
            else:
                tag_batches = repeat(Tag({}))

            # TODO: come back and clean up this logic

            packets = self._table.select(self._packet_columns)

            for tag_batch, packet_batch in zip(tag_batches, packets.to_batches()):
                for i in range(len(packet_batch)):
                    if tag_present:
                        tag = Tag(
                            tag_batch.slice(i, 1),  # type: ignore
                            data_context=self.data_context,
                        )

                    else:
                        tag = cast(Tag, tag_batch)

                    packet = Packet(
                        packet_batch.slice(i, 1),
                        source_info=self._source_info_table.slice(i, 1).to_pylist()[0],
                        data_context=self.data_context,
                    )

                    yield tag, packet

                    cached_elements.append((tag, packet))
            self._cached_elements = cached_elements
        else:
            yield from self._cached_elements

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(table={self._table.column_names}, "
            f"tag_columns={self._tag_columns})"
        )
