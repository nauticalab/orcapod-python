import logging
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Self

from orcapod import contexts
from orcapod.core.datagrams.dict_datagram import DictDatagram
from orcapod.protocols.core_protocols import ColumnConfig
from orcapod.semantic_types import infer_python_schema_from_pylist_data
from orcapod.system_constants import constants
from orcapod.types import DataValue, Schema, PythonSchemaLike
from orcapod.utils import arrow_utils
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")

logger = logging.getLogger(__name__)


class DictTag(DictDatagram):
    """
    A simple tag implementation using Python dictionary.

    Represents a tag (metadata) as a dictionary that can be converted
    to different representations like Arrow tables.
    """

    def __init__(
        self,
        data: Mapping[str, DataValue],
        system_tags: Mapping[str, DataValue] | None = None,
        meta_info: Mapping[str, DataValue] | None = None,
        python_schema: dict[str, type] | None = None,
        data_context: str | contexts.DataContext | None = None,
        record_id: str | None = None,
        **kwargs,
    ) -> None:
        """
        Initialize the tag with data.

        Args:
            data: Dictionary containing tag data
        """
        # normalize the data content and remove any source info keys
        data_only = {
            k: v
            for k, v in data.items()
            if not k.startswith(constants.SYSTEM_TAG_PREFIX)
        }
        extracted_system_tags = {
            k: v for k, v in data.items() if k.startswith(constants.SYSTEM_TAG_PREFIX)
        }

        super().__init__(
            data_only,
            python_schema=python_schema,
            meta_info=meta_info,
            data_context=data_context,
            record_id=record_id,
            **kwargs,
        )

        self._system_tags = {**extracted_system_tags, **(system_tags or {})}
        self._system_tags_python_schema: Schema = infer_python_schema_from_pylist_data(
            [self._system_tags]
        )
        self._cached_system_tags_table: pa.Table | None = None
        self._cached_system_tags_schema: pa.Schema | None = None

    def _get_total_dict(self) -> dict[str, DataValue]:
        """Return the total dictionary representation including system tags."""
        total_dict = super()._get_total_dict()
        total_dict.update(self._system_tags)
        return total_dict

    def as_table(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> "pa.Table":
        """Convert the packet to an Arrow table."""
        table = super().as_table(columns=columns, all_info=all_info)
        column_config = ColumnConfig.handle_config(columns, all_info=all_info)

        if column_config.system_tags:
            # Only create and stack system tags table if there are actually system tags
            if self._system_tags:  # Check if system tags dict is not empty
                if self._cached_system_tags_table is None:
                    self._cached_system_tags_table = (
                        self._data_context.type_converter.python_dicts_to_arrow_table(
                            [self._system_tags],
                            python_schema=self._system_tags_python_schema,
                        )
                    )
                table = arrow_utils.hstack_tables(table, self._cached_system_tags_table)
        return table

    def as_dict(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> dict[str, DataValue]:
        """
        Return dictionary representation.

        Args:
            include_source: Whether to include source info fields

        Returns:
            Dictionary representation of the packet
        """
        dict_copy = super().as_dict(columns=columns, all_info=all_info)
        column_config = ColumnConfig.handle_config(columns, all_info=all_info)

        if column_config.system_tags:
            dict_copy.update(self._system_tags)
        return dict_copy

    def keys(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[str, ...]:
        """Return keys of the Python schema."""
        keys = super().keys(columns=columns, all_info=all_info)
        column_config = ColumnConfig.handle_config(columns, all_info=all_info)
        if column_config.system_tags:
            keys += tuple(self._system_tags.keys())
        return keys

    def schema(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> Schema:
        """Return copy of the Python schema."""
        schema = super().schema(columns=columns, all_info=all_info)
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
        schema = super().arrow_schema(columns=columns, all_info=all_info)
        column_config = ColumnConfig.handle_config(columns, all_info=all_info)
        if column_config.system_tags:
            if self._cached_system_tags_schema is None:
                self._cached_system_tags_schema = (
                    self._data_context.type_converter.python_schema_to_arrow_schema(
                        self._system_tags_python_schema
                    )
                )
            return arrow_utils.join_arrow_schemas(
                schema, self._cached_system_tags_schema
            )
        return schema

    def as_datagram(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> DictDatagram:
        """
        Convert the packet to a DictDatagram.

        Args:
            include_source: Whether to include source info fields

        Returns:
            DictDatagram representation of the packet
        """

        data = self.as_dict(columns=columns, all_info=all_info)
        python_schema = self.schema(columns=columns, all_info=all_info)
        return DictDatagram(
            data,
            python_schema=python_schema,
            data_context=self._data_context,
        )

    def system_tags(self) -> dict[str, DataValue]:
        """
        Return source information for all keys.

        Returns:
            Dictionary mapping field names to their source info
        """
        return dict(self._system_tags)

    def copy(self, include_cache: bool = True, preserve_id: bool = False) -> Self:
        """Return a shallow copy of the packet."""
        instance = super().copy(include_cache=include_cache)
        instance._system_tags = self._system_tags.copy()
        if include_cache:
            instance._cached_system_tags_table = self._cached_system_tags_table
            instance._cached_system_tags_schema = self._cached_system_tags_schema
        else:
            instance._cached_system_tags_table = None
            instance._cached_system_tags_schema = None

        if preserve_id:
            instance._datagram_id = self._datagram_id

        return instance


class DictPacket(DictDatagram):
    """
    Enhanced packet implementation with source information support.

    Extends DictDatagram to include source information tracking and
    enhanced table conversion capabilities that can include or exclude
    source metadata.

    Initialize packet with data and optional source information.

    Args:
        data: Primary data content
        source_info: Optional mapping of field names to source information
        typespec: Optional type specification
        semantic_converter: Optional semantic converter
        semantic_type_registry: Registry for semantic types. Defaults to system default registry.
        arrow_hasher: Optional Arrow hasher. Defaults to system default arrow hasher.
    """

    def __init__(
        self,
        data: Mapping[str, DataValue],
        meta_info: Mapping[str, DataValue] | None = None,
        source_info: Mapping[str, str | None] | None = None,
        python_schema: PythonSchemaLike | None = None,
        data_context: str | contexts.DataContext | None = None,
        record_id: str | None = None,
        **kwargs,
    ) -> None:
        # normalize the data content and remove any source info keys
        data_only = {
            k: v for k, v in data.items() if not k.startswith(constants.SOURCE_PREFIX)
        }
        contained_source_info = {
            k.removeprefix(constants.SOURCE_PREFIX): v
            for k, v in data.items()
            if k.startswith(constants.SOURCE_PREFIX)
        }

        super().__init__(
            data_only,
            python_schema=python_schema,
            meta_info=meta_info,
            data_context=data_context,
            record_id=record_id,
            **kwargs,
        )

        self._source_info = {**contained_source_info, **(source_info or {})}
        self._cached_source_info_table: pa.Table | None = None
        self._cached_source_info_schema: pa.Schema | None = None

    @property
    def _source_info_arrow_schema(self) -> "pa.Schema":
        if self._cached_source_info_schema is None:
            self._cached_source_info_schema = (
                self.converter.python_schema_to_arrow_schema(
                    self._source_info_python_schema
                )
            )

        return self._cached_source_info_schema

    @property
    def _source_info_python_schema(self) -> dict[str, type]:
        """Return the Python schema for source info."""
        return {f"{constants.SOURCE_PREFIX}{k}": str for k in self.keys()}

    def as_table(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> "pa.Table":
        """Convert the packet to an Arrow table."""
        table = super().as_table(columns=columns, all_info=all_info)
        column_config = ColumnConfig.handle_config(columns, all_info=all_info)
        if column_config.source:
            if self._cached_source_info_table is None:
                source_info_data = {
                    f"{constants.SOURCE_PREFIX}{k}": v
                    for k, v in self.source_info().items()
                }
                self._cached_source_info_table = pa.Table.from_pylist(
                    [source_info_data], schema=self._source_info_arrow_schema
                )
            assert self._cached_source_info_table is not None, (
                "Cached source info table should not be None"
            )
            # subselect the corresponding _source_info as the columns present in the data table
            source_info_table = self._cached_source_info_table.select(
                [
                    f"{constants.SOURCE_PREFIX}{k}"
                    for k in table.column_names
                    if k in self.keys()
                ]
            )
            table = arrow_utils.hstack_tables(table, source_info_table)
        return table

    def as_dict(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> dict[str, DataValue]:
        """
        Return dictionary representation.

        Args:
            include_source: Whether to include source info fields

        Returns:
            Dictionary representation of the packet
        """
        dict_copy = super().as_dict(columns=columns, all_info=all_info)
        column_config = ColumnConfig.handle_config(columns, all_info=all_info)
        if column_config.source:
            for key, value in self.source_info().items():
                dict_copy[f"{constants.SOURCE_PREFIX}{key}"] = value
        return dict_copy

    def keys(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[str, ...]:
        """Return keys of the Python schema."""
        keys = super().keys(columns=columns, all_info=all_info)
        column_config = ColumnConfig.handle_config(columns, all_info=all_info)
        if column_config.source:
            keys += tuple(f"{constants.SOURCE_PREFIX}{key}" for key in super().keys())
        return keys

    def schema(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> Schema:
        """Return copy of the Python schema."""
        schema = super().schema(columns=columns, all_info=all_info)
        column_config = ColumnConfig.handle_config(columns, all_info=all_info)
        if column_config.source:
            for key in self.keys():
                schema[f"{constants.SOURCE_PREFIX}{key}"] = str
        return schema

    def rename(self, column_mapping: Mapping[str, str]) -> Self:
        """
        Create a new DictDatagram with data columns renamed.
        Maintains immutability by returning a new instance.

        Args:
            column_mapping: Mapping from old column names to new column names

        Returns:
            New DictDatagram instance with renamed data columns
        """
        # Rename data columns according to mapping, preserving original types

        new_data = {column_mapping.get(k, k): v for k, v in self._data.items()}

        new_source_info = {
            column_mapping.get(k, k): v for k, v in self._source_info.items()
        }

        # Handle python_schema updates for renamed columns
        new_python_schema = {
            column_mapping.get(k, k): v for k, v in self._data_python_schema.items()
        }

        return self.__class__(
            data=new_data,
            meta_info=self._meta_data,
            source_info=new_source_info,
            python_schema=new_python_schema,
            data_context=self._data_context,
        )

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
                schema, self._source_info_arrow_schema
            )
        return schema

    def as_datagram(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> DictDatagram:
        """
        Convert the packet to a DictDatagram.

        Args:
            include_source: Whether to include source info fields

        Returns:
            DictDatagram representation of the packet
        """

        data = self.as_dict(columns=columns, all_info=all_info)
        python_schema = self.schema(columns=columns, all_info=all_info)
        return DictDatagram(
            data=data,
            python_schema=python_schema,
            data_context=self._data_context,
        )

    def source_info(self) -> dict[str, str | None]:
        """
        Return source information for all keys.

        Returns:
            Dictionary mapping field names to their source info
        """
        return {key: self._source_info.get(key, None) for key in self.keys()}

    def with_source_info(self, **source_info: str | None) -> Self:
        """
        Create a new packet with updated source information.

        Args:
            **kwargs: Key-value pairs to update source information

        Returns:
            New DictPacket instance with updated source info
        """
        current_source_info = self._source_info.copy()

        for key, value in source_info.items():
            # Remove prefix if it exists, since _source_info stores unprefixed keys
            if key.startswith(constants.SOURCE_PREFIX):
                key = key.removeprefix(constants.SOURCE_PREFIX)
            current_source_info[key] = value

        new_packet = self.copy(include_cache=False)
        new_packet._source_info = current_source_info

        return new_packet

    def copy(self, include_cache: bool = True, preserve_id: bool = True) -> Self:
        """Return a shallow copy of the packet."""
        instance = super().copy(include_cache=include_cache, preserve_id=preserve_id)
        instance._source_info = self._source_info.copy()
        if include_cache:
            instance._cached_source_info_table = self._cached_source_info_table
            instance._cached_source_info_schema = self._cached_source_info_schema

        else:
            instance._cached_source_info_table = None
            instance._cached_source_info_schema = None

        return instance
