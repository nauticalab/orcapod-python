import logging
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Self

from orcapod import contexts
from .arrow_datagram import ArrowDatagram
from orcapod.semantic_types import infer_python_schema_from_pylist_data
from orcapod.system_constants import constants
from orcapod.types import ColumnConfig, DataValue, Schema
from orcapod.utils import arrow_utils
from orcapod.utils.lazy_module import LazyModule

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")


class ArrowTag(ArrowDatagram):
    """
    A tag implementation using Arrow table backend.

    Represents a single-row Arrow table that can be converted to Python
    dictionary representation while caching computed values for efficiency.

    Initialize with an Arrow table.

        Args:
            table: Single-row Arrow table representing the tag

        Raises:
            ValueError: If table doesn't contain exactly one row
    """

    def __init__(
        self,
        table: "pa.Table",
        system_tags: Mapping[str, DataValue] | None = None,
        data_context: str | contexts.DataContext | None = None,
        record_id: str | None = None,
        **kwargs,
    ) -> None:
        if len(table) != 1:
            raise ValueError(
                "ArrowTag should only contain a single row, "
                "as it represents a single tag."
            )
        super().__init__(
            table=table,
            data_context=data_context,
            record_id=record_id,
            **kwargs,
        )
        extracted_system_tag_columns = [
            c
            for c in self._data_table.column_names
            if c.startswith(constants.SYSTEM_TAG_PREFIX)
        ]
        self._system_tags_dict: dict[str, DataValue] = (
            self._data_context.type_converter.arrow_table_to_python_dicts(
                self._data_table.select(extracted_system_tag_columns)
            )[0]
        )
        self._system_tags_dict.update(system_tags or {})
        self._system_tags_python_schema = infer_python_schema_from_pylist_data(
            [self._system_tags_dict]
        )
        self._system_tags_table = (
            self._data_context.type_converter.python_dicts_to_arrow_table(
                [self._system_tags_dict], python_schema=self._system_tags_python_schema
            )
        )

        self._data_table = self._data_table.drop_columns(extracted_system_tag_columns)

    def keys(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[str, ...]:
        keys = super().keys(
            columns=columns,
            all_info=all_info,
        )
        column_config = ColumnConfig.handle_config(columns, all_info=all_info)
        if column_config.system_tags:
            keys += tuple(self._system_tags_dict.keys())
        return keys

    def schema(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> Schema:
        """Return copy of the Python schema."""
        schema = super().schema(
            columns=columns,
            all_info=all_info,
        )
        column_config = ColumnConfig.handle_config(columns, all_info=all_info)
        if column_config.system_tags:
            schema.update(self._system_tags_python_schema)
        return schema

    def arrow_schema(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> "pa.Schema":
        """
        Return the PyArrow schema for this datagram.

        Args:
            include_data_context: Whether to include data context column in the schema
            include_source: Whether to include source info columns in the schema

        Returns:
            PyArrow schema representing the datagram's structure
        """
        schema = super().arrow_schema(
            columns=columns,
            all_info=all_info,
        )
        column_config = ColumnConfig.handle_config(columns, all_info=all_info)
        if column_config.system_tags:
            return arrow_utils.join_arrow_schemas(
                schema, self._system_tags_table.schema
            )
        return schema

    def as_dict(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> dict[str, DataValue]:
        """
        Convert to dictionary representation.

        Args:
            include_source: Whether to include source info fields

        Returns:
            Dictionary representation of the packet
        """
        return_dict = super().as_dict(
            columns=columns,
            all_info=all_info,
        )
        column_config = ColumnConfig.handle_config(columns, all_info=all_info)
        if column_config.system_tags:
            return_dict.update(self._system_tags_dict)
        return return_dict

    def as_table(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> "pa.Table":
        table = super().as_table(
            columns=columns,
            all_info=all_info,
        )
        column_config = ColumnConfig.handle_config(columns, all_info=all_info)
        if column_config.system_tags and self._system_tags_table.num_columns > 0:
            # add system_tags only if there are actual system tag columns
            table = arrow_utils.hstack_tables(table, self._system_tags_table)
        return table

    def as_datagram(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> ArrowDatagram:
        table = self.as_table(
            columns=columns,
            all_info=all_info,
        )
        return ArrowDatagram(
            table,
            data_context=self.data_context,
        )

    def system_tags(self) -> dict[str, DataValue | None]:
        """
        Return system tags for all keys.

        Returns:
            Copy of the dictionary mapping field names to their source info
        """
        return self._system_tags_dict.copy()

    # 8. Utility Operations
    def copy(self, include_cache: bool = True, preserve_id: bool = True) -> Self:
        """Return a copy of the datagram."""
        new_tag = super().copy(include_cache=include_cache, preserve_id=preserve_id)

        new_tag._system_tags_dict = self._system_tags_dict.copy()
        new_tag._system_tags_python_schema = self._system_tags_python_schema.copy()
        new_tag._system_tags_table = self._system_tags_table

        return new_tag


class ArrowPacket(ArrowDatagram):
    """
    Arrow table-based packet implementation with comprehensive features.

    A packet implementation that uses Arrow tables as the primary storage format,
    providing efficient memory usage and columnar data operations while supporting
    source information tracking and content hashing.


    Initialize ArrowPacket with Arrow table and configuration.

        Args:
            table: Single-row Arrow table representing the packet
            source_info: Optional source information mapping
            semantic_converter: Optional semantic converter
            semantic_type_registry: Registry for semantic types
            finger_print: Optional fingerprint for tracking
            arrow_hasher: Optional Arrow hasher
            post_hash_callback: Optional callback after hash calculation
            skip_source_info_extraction: Whether to skip source info processing

        Raises:
            ValueError: If table doesn't contain exactly one row
    """

    def __init__(
        self,
        table: "pa.Table | pa.RecordBatch",
        meta_info: Mapping[str, DataValue] | None = None,
        source_info: Mapping[str, str | None] | None = None,
        data_context: str | contexts.DataContext | None = None,
        record_id: str | None = None,
        **kwargs,
    ) -> None:
        if len(table) != 1:
            raise ValueError(
                "ArrowPacket should only contain a single row, "
                "as it represents a single packet."
            )
        if source_info is None:
            source_info = {}
        else:
            # normalize by removing any existing prefixes
            source_info = {
                (
                    k.removeprefix(constants.SOURCE_PREFIX)
                    if k.startswith(constants.SOURCE_PREFIX)
                    else k
                ): v
                for k, v in source_info.items()
            }

        # normalize the table to ensure it has the expected source_info columns
        # TODO: use simpler function to ensure source_info columns
        data_table, prefixed_tables = arrow_utils.prepare_prefixed_columns(
            table,
            {constants.SOURCE_PREFIX: source_info},
            exclude_columns=[constants.CONTEXT_KEY],
            exclude_prefixes=[constants.META_PREFIX],
        )

        super().__init__(
            data_table,
            meta_info=meta_info,
            data_context=data_context,
            record_id=record_id,
            **kwargs,
        )
        self._source_info_table = prefixed_tables[constants.SOURCE_PREFIX]

        self._cached_source_info: dict[str, str | None] | None = None
        self._cached_python_schema: Schema | None = None

    def keys(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[str, ...]:
        keys = super().keys(
            columns=columns,
            all_info=all_info,
        )
        column_config = ColumnConfig.handle_config(columns, all_info=all_info)
        if column_config.source:
            keys += tuple(f"{constants.SOURCE_PREFIX}{k}" for k in self.keys())
        return keys

    def schema(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> Schema:
        """Return copy of the Python schema."""
        schema = super().schema(
            columns=columns,
            all_info=all_info,
        )
        column_config = ColumnConfig.handle_config(columns, all_info=all_info)
        if column_config.source:
            for key in self.keys():
                schema[f"{constants.SOURCE_PREFIX}{key}"] = str
        return schema

    def arrow_schema(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> "pa.Schema":
        """
        Return the PyArrow schema for this datagram.

        Args:
            include_data_context: Whether to include data context column in the schema
            include_source: Whether to include source info columns in the schema

        Returns:
            PyArrow schema representing the datagram's structure
        """
        schema = super().arrow_schema(columns=columns, all_info=all_info)
        column_config = ColumnConfig.handle_config(columns, all_info=all_info)
        if column_config.source:
            return arrow_utils.join_arrow_schemas(
                schema, self._source_info_table.schema
            )
        return schema

    def as_dict(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> dict[str, DataValue]:
        """
        Convert to dictionary representation.

        Args:
            include_source: Whether to include source info fields

        Returns:
            Dictionary representation of the packet
        """
        return_dict = super().as_dict(columns=columns, all_info=all_info)
        column_config = ColumnConfig.handle_config(columns, all_info=all_info)
        if column_config.source:
            return_dict.update(
                {
                    f"{constants.SOURCE_PREFIX}{k}": v
                    for k, v in self.source_info().items()
                }
            )
        return return_dict

    def as_table(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> "pa.Table":
        table = super().as_table(columns=columns, all_info=all_info)
        column_config = ColumnConfig.handle_config(columns, all_info=all_info)
        if column_config.source:
            # add source_info only if there are columns and the table has meaningful data
            if (
                self._source_info_table.num_columns > 0
                and self._source_info_table.num_rows > 0
            ):
                table = arrow_utils.hstack_tables(table, self._source_info_table)
        return table

    def as_datagram(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> ArrowDatagram:
        table = self.as_table(columns=columns, all_info=all_info)
        return ArrowDatagram(
            table,
            data_context=self._data_context,
        )

    def source_info(self) -> dict[str, str | None]:
        """
        Return source information for all keys.

        Returns:
            Copy of the dictionary mapping field names to their source info
        """
        if self._cached_source_info is None:
            self._cached_source_info = {
                k.removeprefix(constants.SOURCE_PREFIX): v
                for k, v in self._source_info_table.to_pylist()[0].items()
            }
        return self._cached_source_info.copy()

    def with_source_info(self, **source_info: str | None) -> Self:
        """
        Create a copy of the packet with updated source information.

        Args:
            source_info: New source information mapping

        Returns:
            New ArrowPacket instance with updated source info
        """
        new_packet = self.copy(include_cache=False)

        existing_source_info_with_prefix = self._source_info_table.to_pylist()[0]
        for key, value in source_info.items():
            if not key.startswith(constants.SOURCE_PREFIX):
                # Ensure the key is prefixed correctly
                key = f"{constants.SOURCE_PREFIX}{key}"
            if key in existing_source_info_with_prefix:
                existing_source_info_with_prefix[key] = value

        new_packet._source_info_table = pa.Table.from_pylist(
            [existing_source_info_with_prefix]
        )
        return new_packet

    def rename(self, column_mapping: Mapping[str, str]) -> Self:
        """
        Create a new ArrowDatagram with data columns renamed.
        Maintains immutability by returning a new instance.

        Args:
            column_mapping: Mapping from old column names to new column names

        Returns:
            New ArrowDatagram instance with renamed data columns
        """
        # Create new schema with renamed fields, preserving original types

        if not column_mapping:
            return self

        new_names = [column_mapping.get(k, k) for k in self._data_table.column_names]

        new_source_info_names = [
            f"{constants.SOURCE_PREFIX}{column_mapping.get(k.removeprefix(constants.SOURCE_PREFIX), k.removeprefix(constants.SOURCE_PREFIX))}"
            for k in self._source_info_table.column_names
        ]

        new_datagram = self.copy(include_cache=False)
        new_datagram._data_table = new_datagram._data_table.rename_columns(new_names)
        new_datagram._source_info_table = (
            new_datagram._source_info_table.rename_columns(new_source_info_names)
        )

        return new_datagram

    def with_columns(
        self,
        column_types: Mapping[str, type] | None = None,
        **updates: DataValue,
    ) -> Self:
        """
        Create a new ArrowPacket with new data columns added.
        Maintains immutability by returning a new instance.
        Also adds corresponding empty source info columns for new columns.

        Args:
            column_types: Optional type specifications for new columns
            **updates: New data columns as keyword arguments

        Returns:
            New ArrowPacket instance with new data columns and corresponding source info columns

        Raises:
            ValueError: If any column already exists (use update() instead)
        """
        if not updates:
            return self

        # First call parent method to add the data columns
        new_packet = super().with_columns(column_types=column_types, **updates)

        # Now add corresponding empty source info columns for the new columns
        source_info_updates = {}
        for column_name in updates.keys():
            source_key = f"{constants.SOURCE_PREFIX}{column_name}"
            source_info_updates[source_key] = None  # Empty source info

        # Add new source info columns to the source info table
        if source_info_updates:
            # Get existing source info
            schema = new_packet._source_info_table.schema
            existing_source_info = new_packet._source_info_table.to_pylist()[0]

            # Add the new empty source info columns
            existing_source_info.update(source_info_updates)
            schema_columns = list(schema)
            schema_columns.extend(
                [
                    pa.field(name, pa.large_string())
                    for name in source_info_updates.keys()
                ]
            )
            new_schema = pa.schema(schema_columns)

            # Update the source info table
            new_packet._source_info_table = pa.Table.from_pylist(
                [existing_source_info], new_schema
            )

        return new_packet

    # 8. Utility Operations
    def copy(self, include_cache: bool = True, preserve_id: bool = True) -> Self:
        """Return a copy of the datagram."""
        new_packet = super().copy(include_cache=include_cache, preserve_id=preserve_id)
        new_packet._source_info_table = self._source_info_table

        if include_cache:
            new_packet._cached_source_info = self._cached_source_info
        else:
            new_packet._cached_source_info = None

        return new_packet
