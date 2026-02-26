import logging
from collections.abc import Collection, Iterator, Mapping
from typing import TYPE_CHECKING, Any, Self

from orcapod import contexts
from orcapod.core.datagrams.base import BaseDatagram
from orcapod.protocols.hashing_protocols import ContentHash
from orcapod.system_constants import constants
from orcapod.types import ColumnConfig, DataValue, Schema
from orcapod.utils import arrow_utils
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")

logger = logging.getLogger(__name__)
DEBUG = False


class ArrowDatagram(BaseDatagram):
    """
    Immutable datagram implementation using PyArrow Table as storage backend.

    This implementation provides high-performance columnar data operations while
    maintaining the datagram interface. It efficiently handles type conversions,
    semantic processing, and interoperability with Arrow-based tools.

    The underlying table is split into separate components:
    - Data table: Primary business data columns
    - Meta table: Internal system metadata with {orcapod.META_PREFIX} ('__') prefixes
    - Context table: Data context information with {orcapod.CONTEXT_KEY}

    Future Packet subclass will also handle:
    - Source info: Data provenance with {orcapod.SOURCE_PREFIX} ('_source_') prefixes

    When exposing to external tools, semantic types are encoded as
    `_{semantic_type}_` prefixes (_path_config_file, _id_user_name).

    All operations return new instances, preserving immutability.

    Example:
        >>> table = pa.Table.from_pydict({
        ...     "user_id": [123],
        ...     "name": ["Alice"],
        ...     "__pipeline_version": ["v2.1.0"],
        ...     "{orcapod.CONTEXT_KEY}": ["financial_v1"]
        ... })
        >>> datagram = ArrowDatagram(table)
        >>> updated = datagram.update(name="Alice Smith")
    """

    def __init__(
        self,
        table: "pa.Table",
        meta_info: Mapping[str, DataValue] | None = None,
        data_context: str | contexts.DataContext | None = None,
        record_id: str | None = None,
        **kwargs,
    ) -> None:
        """
        Initialize ArrowDatagram from PyArrow Table.

        Args:
            table: PyArrow Table containing the data. Must have exactly one row.
            semantic_converter: Optional converter for semantic type handling.
                If None, will be created based on the data context and table schema.
            data_context: Context key string or DataContext object.
                If None and table contains context column, will extract from table.

        Raises:
            ValueError: If table doesn't contain exactly one row.

        Note:
            The input table is automatically split into data, meta, and context
            components based on column naming conventions.
        """

        # Validate table has exactly one row for datagram
        if len(table) != 1:
            raise ValueError(
                "Table must contain exactly one row to be a valid datagram."
            )

        # normalize the table to large data types (for Polars compatibility)
        table = arrow_utils.normalize_table_to_large_types(table)

        # Split table into data, meta, and context components
        context_columns = (
            [constants.CONTEXT_KEY]
            if constants.CONTEXT_KEY in table.column_names
            else []
        )

        # Extract context table from passed in table if present
        # TODO: revisit the logic here
        if constants.CONTEXT_KEY in table.column_names and data_context is None:
            context_table = table.select([constants.CONTEXT_KEY])
            data_context = context_table[constants.CONTEXT_KEY].to_pylist()[0]

        # Initialize base class with data context
        super().__init__(data_context=data_context, datagram_id=record_id, **kwargs)

        meta_columns = [
            col for col in table.column_names if col.startswith(constants.META_PREFIX)
        ]
        # Split table into components
        self._data_table = table.drop_columns(context_columns + meta_columns)
        self._meta_table = table.select(meta_columns) if meta_columns else None

        if len(self._data_table.column_names) == 0:
            raise ValueError("Data table must contain at least one data column.")

        # process supplemented meta info if provided
        if meta_info is not None:
            # make sure it has the expected prefixes
            meta_info = {
                (
                    f"{constants.META_PREFIX}{k}"
                    if not k.startswith(constants.META_PREFIX)
                    else k
                ): v
                for k, v in meta_info.items()
            }
            new_meta_table = (
                self._data_context.type_converter.python_dicts_to_arrow_table(
                    [meta_info],
                )
            )

            if self._meta_table is None:
                self._meta_table = new_meta_table
            else:
                # drop any column that will be overwritten by the new meta table
                keep_meta_columns = [
                    c
                    for c in self._meta_table.column_names
                    if c not in new_meta_table.column_names
                ]
                self._meta_table = arrow_utils.hstack_tables(
                    self._meta_table.select(keep_meta_columns), new_meta_table
                )

        # Create data context table
        data_context_schema = pa.schema({constants.CONTEXT_KEY: pa.large_string()})
        self._data_context_table = pa.Table.from_pylist(
            [{constants.CONTEXT_KEY: self._data_context.context_key}],
            schema=data_context_schema,
        )

        # Initialize caches
        self._cached_python_schema: Schema | None = None
        self._cached_python_dict: dict[str, DataValue] | None = None
        self._cached_meta_python_schema: Schema | None = None
        self._cached_content_hash: ContentHash | None = None

    # 1. Core Properties (Identity & Structure)
    @property
    def meta_columns(self) -> tuple[str, ...]:
        """Return tuple of meta column names."""
        if self._meta_table is None:
            return ()
        return tuple(self._meta_table.column_names)

    # 2. Dict-like Interface (Data Access)
    def __getitem__(self, key: str) -> DataValue:
        """Get data column value by key."""
        if key not in self._data_table.column_names:
            raise KeyError(f"Data column '{key}' not found")

        return self.as_dict()[key]

    def __contains__(self, key: str) -> bool:
        """Check if data column exists."""
        return key in self._data_table.column_names

    def __iter__(self) -> Iterator[str]:
        """Iterate over data column names."""
        return iter(self._data_table.column_names)

    def get(self, key: str, default: DataValue = None) -> DataValue:
        """Get data column value with default."""
        if key in self._data_table.column_names:
            return self.as_dict()[key]
        return default

    # 3. Structural Information
    def keys(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[str, ...]:
        """Return tuple of column names."""
        # Start with data columns
        column_config = ColumnConfig.handle_config(columns, all_info=all_info)
        include_meta_columns = column_config.meta
        include_context = column_config.context

        result_keys = list(self._data_table.column_names)

        # Add context if requested
        if include_context:
            result_keys.append(constants.CONTEXT_KEY)

        # Add meta columns if requested
        if include_meta_columns:
            if include_meta_columns is True:
                result_keys.extend(self.meta_columns)
            elif isinstance(include_meta_columns, Collection):
                # Filter meta columns by prefix matching
                filtered_meta_cols = [
                    col
                    for col in self.meta_columns
                    if any(col.startswith(prefix) for prefix in include_meta_columns)
                ]
                result_keys.extend(filtered_meta_cols)

        return tuple(result_keys)

    def schema(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> Schema:
        """
        Return Python schema for the datagram.

        Args:
            include_meta_columns: Whether to include meta column types.
                - True: include all meta column types
                - Collection[str]: include meta column types matching these prefixes
                - False: exclude meta column types
            include_context: Whether to include context type

        Returns:
            Python schema
        """
        column_config = ColumnConfig.handle_config(columns, all_info=all_info)
        include_meta_columns = column_config.meta
        include_context = column_config.context

        # Get data schema (cached)
        if self._cached_python_schema is None:
            self._cached_python_schema = (
                self._data_context.type_converter.arrow_schema_to_python_schema(
                    self._data_table.schema
                )
            )

        schema = dict(self._cached_python_schema)

        # Add context if requested
        if include_context:
            schema[constants.CONTEXT_KEY] = str

        # Add meta schema if requested
        if include_meta_columns and self._meta_table is not None:
            if self._cached_meta_python_schema is None:
                self._cached_meta_python_schema = (
                    self._data_context.type_converter.arrow_schema_to_python_schema(
                        self._meta_table.schema
                    )
                )
            meta_schema = dict(self._cached_meta_python_schema)
            if include_meta_columns is True:
                schema.update(meta_schema)
            elif isinstance(include_meta_columns, Collection):
                filtered_meta_schema = {
                    k: v
                    for k, v in meta_schema.items()
                    if any(k.startswith(prefix) for prefix in include_meta_columns)
                }
                schema.update(filtered_meta_schema)

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
            include_meta_columns: Whether to include meta columns in the schema.
                - True: include all meta columns
                - Collection[str]: include meta columns matching these prefixes
                - False: exclude meta columns
            include_context: Whether to include context column in the schema

        Returns:
            PyArrow schema representing the datagram's structure
        """
        # order matters
        column_config = ColumnConfig.handle_config(columns, all_info=all_info)
        include_meta_columns = column_config.meta
        include_context = column_config.context

        all_schemas = [self._data_table.schema]

        # Add context schema if requested
        if include_context:
            # TODO: reassess the efficiency of this approach
            all_schemas.append(self._data_context_table.schema)

        # Add meta schema if requested
        if include_meta_columns and self._meta_table is not None:
            if include_meta_columns is True:
                meta_schema = self._meta_table.schema
            elif isinstance(include_meta_columns, Collection):
                # Filter meta schema by prefix matching
                matched_fields = [
                    field
                    for field in self._meta_table.schema
                    if any(
                        field.name.startswith(prefix) for prefix in include_meta_columns
                    )
                ]
                if matched_fields:
                    meta_schema = pa.schema(matched_fields)
                else:
                    meta_schema = None
            else:
                meta_schema = None

            if meta_schema is not None:
                all_schemas.append(meta_schema)

        return arrow_utils.join_arrow_schemas(*all_schemas)

    def content_hash(self) -> ContentHash:
        """
        Calculate and return content hash of the datagram.
        Only includes data columns, not meta columns or context.

        Returns:
            Hash string of the datagram content
        """
        if self._cached_content_hash is None:
            self._cached_content_hash = self._data_context.arrow_hasher.hash_table(
                self._data_table,
            )
        return self._cached_content_hash

    # 4. Format Conversions (Export)
    def as_dict(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> dict[str, DataValue]:
        """
        Return dictionary representation of the datagram.

        Args:
            include_meta_columns: Whether to include meta columns.
                - True: include all meta columns
                - Collection[str]: include meta columns matching these prefixes
                - False: exclude meta columns
            include_context: Whether to include context key

        Returns:
            Dictionary representation
        """
        column_config = ColumnConfig.handle_config(columns, all_info=all_info)
        include_meta_columns = column_config.meta
        include_context = column_config.context

        # Get data dict (cached)
        if self._cached_python_dict is None:
            self._cached_python_dict = (
                self._data_context.type_converter.arrow_table_to_python_dicts(
                    self._data_table
                )[0]
            )

        result_dict = dict(self._cached_python_dict)

        # Add context if requested
        if include_context:
            result_dict[constants.CONTEXT_KEY] = self._data_context.context_key

        # Add meta data if requested
        if include_meta_columns and self._meta_table is not None:
            meta_dict = None
            if include_meta_columns is True:
                meta_dict = self._meta_table.to_pylist()[0]
            elif isinstance(include_meta_columns, Collection):
                meta_dict = self._meta_table.to_pylist()[0]
                # Include only meta columns matching prefixes
                meta_dict = {
                    k: v
                    for k, v in meta_dict.items()
                    if any(k.startswith(prefix) for prefix in include_meta_columns)
                }
            if meta_dict is not None:
                result_dict.update(meta_dict)

        return result_dict

    def as_table(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> "pa.Table":
        """
        Convert the datagram to an Arrow table.

        Args:
            include_meta_columns: Whether to include meta columns.
                - True: include all meta columns
                - Collection[str]: include meta columns matching these prefixes
                - False: exclude meta columns
            include_context: Whether to include the context column

        Returns:
            Arrow table representation
        """
        column_config = ColumnConfig.handle_config(columns, all_info=all_info)
        include_meta_columns = column_config.meta
        include_context = column_config.context

        all_tables = [self._data_table]

        # Add context if requested
        if include_context:
            all_tables.append(self._data_context_table)

        # Add meta columns if requested
        if include_meta_columns and self._meta_table is not None:
            meta_table = None
            if include_meta_columns is True:
                meta_table = self._meta_table
            elif isinstance(include_meta_columns, Collection):
                # Filter meta columns by prefix matching
                # ensure all given prefixes start with the meta prefix
                prefixes = (
                    f"{constants.META_PREFIX}{prefix}"
                    if not prefix.startswith(constants.META_PREFIX)
                    else prefix
                    for prefix in include_meta_columns
                )

                matched_cols = [
                    col
                    for col in self._meta_table.column_names
                    if any(col.startswith(prefix) for prefix in prefixes)
                ]
                if matched_cols:
                    meta_table = self._meta_table.select(matched_cols)
                else:
                    meta_table = None

            if meta_table is not None:
                all_tables.append(meta_table)

        return arrow_utils.hstack_tables(*all_tables)

    def as_arrow_compatible_dict(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> dict[str, DataValue]:
        """
        Return dictionary representation compatible with Arrow.

        Args:
            include_meta_columns: Whether to include meta columns.
                - True: include all meta columns
                - Collection[str]: include meta columns matching these prefixes
                - False: exclude meta columns
            include_context: Whether to include context key

        Returns:
            Dictionary representation compatible with Arrow
        """
        return self.as_table(columns=columns, all_info=all_info).to_pylist()[0]

    # 5. Meta Column Operations
    def get_meta_value(self, key: str, default: DataValue = None) -> DataValue:
        """
        Get a meta column value.

        Args:
            key: Meta column key (with or without {orcapod.META_PREFIX} ('__') prefix)
            default: Default value if not found

        Returns:
            Meta column value
        """
        if self._meta_table is None:
            return default

        # Handle both prefixed and unprefixed keys
        if not key.startswith(constants.META_PREFIX):
            key = constants.META_PREFIX + key

        if key not in self._meta_table.column_names:
            return default

        return self._meta_table[key].to_pylist()[0]

    def with_meta_columns(self, **meta_updates: DataValue) -> Self:
        """
        Create a new ArrowDatagram with updated meta columns.
        Maintains immutability by returning a new instance.

        Args:
            **meta_updates: Meta column updates (keys will be prefixed with {orcapod.META_PREFIX} ('__') if needed)

        Returns:
            New ArrowDatagram instance
        """
        # Prefix the keys and prepare updates
        prefixed_updates = {}
        for k, v in meta_updates.items():
            if not k.startswith(constants.META_PREFIX):
                k = constants.META_PREFIX + k
            prefixed_updates[k] = v

        new_datagram = self.copy(include_cache=False)

        # Start with existing meta data
        meta_dict = {}
        if self._meta_table is not None:
            meta_dict = self._meta_table.to_pylist()[0]

        # Apply updates
        meta_dict.update(prefixed_updates)

        # TODO: properly handle case where meta data is None (it'll get inferred as NoneType)

        # Create new meta table
        new_datagram._meta_table = (
            self._data_context.type_converter.python_dicts_to_arrow_table([meta_dict])
            if meta_dict
            else None
        )
        return new_datagram

    def drop_meta_columns(self, *keys: str, ignore_missing: bool = False) -> Self:
        """
        Create a new ArrowDatagram with specified meta columns dropped.
        Maintains immutability by returning a new instance.

        Args:
            *keys: Meta column keys to drop (with or without {orcapod.META_PREFIX} ('__') prefix)

        Returns:
            New ArrowDatagram instance without specified meta columns
        """
        if self._meta_table is None:
            return self  # No meta columns to drop

        # Normalize keys to have prefixes
        prefixed_keys = set()
        for key in keys:
            if not key.startswith(constants.META_PREFIX):
                key = constants.META_PREFIX + key
            prefixed_keys.add(key)

        missing_keys = prefixed_keys - set(self._meta_table.column_names)
        if missing_keys and not ignore_missing:
            raise KeyError(
                f"Following meta columns do not exist and cannot be dropped: {sorted(missing_keys)}"
            )

        # Only drop columns that actually exist
        existing_keys = prefixed_keys - missing_keys

        new_datagram = self.copy(include_cache=False)
        if existing_keys:  # Only drop if there are existing columns to drop
            new_datagram._meta_table = self._meta_table.drop_columns(
                list(existing_keys)
            )

        return new_datagram

    # 6. Data Column Operations
    def select(self, *column_names: str) -> Self:
        """
        Create a new ArrowDatagram with only specified data columns.
        Maintains immutability by returning a new instance.

        Args:
            *column_names: Data column names to keep

        Returns:
            New ArrowDatagram instance with only specified data columns
        """
        # Validate columns exist
        missing_cols = set(column_names) - set(self._data_table.column_names)
        if missing_cols:
            raise ValueError(f"Columns not found: {missing_cols}")

        new_datagram = self.copy(include_cache=False)
        new_datagram._data_table = new_datagram._data_table.select(column_names)

        return new_datagram

    def drop(self, *column_names: str, ignore_missing: bool = False) -> Self:
        """
        Create a new ArrowDatagram with specified data columns dropped.
        Maintains immutability by returning a new instance.

        Args:
            *column_names: Data column names to drop

        Returns:
            New ArrowDatagram instance without specified data columns
        """

        # Filter out specified data columns
        missing = set(column_names) - set(self._data_table.column_names)
        if missing and not ignore_missing:
            raise KeyError(
                f"Following columns do not exist and cannot be dropped: {sorted(missing)}"
            )
        # Only keep columns that actually exist
        existing_columns = tuple(
            c for c in column_names if c in self._data_table.column_names
        )

        new_datagram = self.copy(include_cache=False)
        if existing_columns:  # Only drop if there are existing columns to drop
            new_datagram._data_table = self._data_table.drop_columns(
                list(existing_columns)
            )
        # TODO: consider dropping extra semantic columns if they are no longer needed
        return new_datagram

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

        new_datagram = self.copy(include_cache=False)
        new_datagram._data_table = new_datagram._data_table.rename_columns(new_names)

        return new_datagram

    def update(self, **updates: DataValue) -> Self:
        """
        Create a new ArrowDatagram with specific column values updated.

        Args:
            **updates: Column names and their new values

        Returns:
            New ArrowDatagram instance with updated values

        Raises:
            KeyError: If any specified column doesn't exist

        Example:
            # Convert relative path to absolute path
            updated = datagram.update(file_path="/absolute/path/to/file.txt")

            # Update multiple values
            updated = datagram.update(status="processed", file_path="/new/path")
        """
        # Only update if there are columns to update
        if not updates:
            return self

        # Validate all columns exist
        missing_cols = set(updates.keys()) - set(self._data_table.column_names)
        if missing_cols:
            raise KeyError(
                f"Only existing columns can be updated. Following columns were not found: {sorted(missing_cols)}"
            )

        new_datagram = self.copy(include_cache=False)

        # use existing schema
        sub_schema = arrow_utils.schema_select(
            new_datagram._data_table.schema, list(updates.keys())
        )

        update_table = self._data_context.type_converter.python_dicts_to_arrow_table(
            [updates], arrow_schema=sub_schema
        )

        new_datagram._data_table = arrow_utils.hstack_tables(
            self._data_table.drop_columns(list(updates.keys())), update_table
        ).select(self._data_table.column_names)  # adjsut the order to match original

        return new_datagram

    def with_columns(
        self,
        column_types: Mapping[str, type] | None = None,
        **updates: DataValue,
    ) -> Self:
        """
        Create a new ArrowDatagram with new data columns added.
        Maintains immutability by returning a new instance.

        Args:
            column_updates: New data columns as a mapping
            column_types: Optional type specifications for new columns
            **kwargs: New data columns as keyword arguments

        Returns:
            New ArrowDatagram instance with new data columns added

        Raises:
            ValueError: If any column already exists (use update() instead)
        """
        # Combine explicit updates with kwargs

        if not updates:
            return self

        # Error if any of the columns already exists
        existing_overlaps = set(updates.keys()) & set(self._data_table.column_names)
        if existing_overlaps:
            raise ValueError(
                f"Columns already exist: {sorted(existing_overlaps)}. "
                f"Use update() to modify existing columns."
            )

        # create a copy and perform in-place updates
        new_datagram = self.copy()

        # TODO: consider simplifying this conversion logic

        # TODO: cleanup the handling of typespec python schema and various conversion points
        new_data_table = self._data_context.type_converter.python_dicts_to_arrow_table(
            [updates], python_schema=dict(column_types) if column_types else None
        )

        # perform in-place update
        new_datagram._data_table = arrow_utils.hstack_tables(
            new_datagram._data_table, new_data_table
        )

        return new_datagram

    # 7. Context Operations
    def with_context_key(self, new_context_key: str) -> Self:
        """
        Create a new ArrowDatagram with a different data context key.
        Maintains immutability by returning a new instance.

        Args:
            new_context_key: New data context key string

        Returns:
            New ArrowDatagram instance with new context
        """
        # TODO: consider if there is a more efficient way to handle context
        # Combine all tables for reconstruction

        new_datagram = self.copy(include_cache=False)
        new_datagram._data_context = contexts.resolve_context(new_context_key)
        return new_datagram

    # 8. Utility Operations
    def copy(self, include_cache: bool = True, preserve_id: bool = True) -> Self:
        """Return a copy of the datagram."""
        new_datagram = super().copy(
            include_cache=include_cache, preserve_id=preserve_id
        )

        new_datagram._data_table = self._data_table
        new_datagram._meta_table = self._meta_table
        new_datagram._data_context = self._data_context

        if include_cache:
            new_datagram._cached_python_schema = self._cached_python_schema
            new_datagram._cached_python_dict = self._cached_python_dict
            new_datagram._cached_content_hash = self._cached_content_hash
            new_datagram._cached_meta_python_schema = self._cached_meta_python_schema
        else:
            new_datagram._cached_python_schema = None
            new_datagram._cached_python_dict = None
            new_datagram._cached_content_hash = None
            new_datagram._cached_meta_python_schema = None

        return new_datagram

    # 9. String Representations
    def __str__(self) -> str:
        """
        Return user-friendly string representation.

        Shows the datagram as a simple dictionary for user-facing output,
        messages, and logging. Only includes data columns for clean output.

        Returns:
            Dictionary-style string representation of data columns only.

        Example:
            >>> str(datagram)
            "{'user_id': 123, 'name': 'Alice'}"
            >>> print(datagram)
            {'user_id': 123, 'name': 'Alice'}
        """
        return str(self.as_dict())

    def __repr__(self) -> str:
        """
        Return detailed string representation for debugging.

        Shows the datagram type and comprehensive information including
        data columns, meta columns count, and context for debugging purposes.

        Returns:
            Detailed representation with type and metadata information.

        Example:
            >>> repr(datagram)
            "ArrowDatagram(data={'user_id': 123, 'name': 'Alice'}, meta_columns=2, context='std:v1.0.0:abc123')"
        """
        if DEBUG:
            data_dict = self.as_dict()
            meta_count = len(self.meta_columns)
            context_key = self.data_context_key

            return (
                f"{self.__class__.__name__}("
                f"data={data_dict}, "
                f"meta_columns={meta_count}, "
                f"context='{context_key}'"
                f")"
            )
        else:
            return str(self.as_dict())
