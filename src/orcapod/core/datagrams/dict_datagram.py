import logging
from collections.abc import Collection, Iterator, Mapping
from typing import TYPE_CHECKING, Any, Self, cast

from orcapod import contexts
from orcapod.core.datagrams.base import BaseDatagram
from orcapod.system_constants import constants
from orcapod.protocols.core_protocols import ColumnConfig
from orcapod.protocols.hashing_protocols import ContentHash
from orcapod.semantic_types import infer_python_schema_from_pylist_data
from orcapod.types import DataValue, PythonSchema, PythonSchemaLike
from orcapod.utils import arrow_utils
from orcapod.utils.lazy_module import LazyModule

logger = logging.getLogger(__name__)

# FIXME: make this configurable!
DEBUG = False

if TYPE_CHECKING:
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")


class DictDatagram(BaseDatagram):
    """
    Immutable datagram implementation using dictionary as storage backend.

    This implementation uses composition (not inheritance from Mapping) to maintain
    control over the interface while leveraging dictionary efficiency for data access.
    Provides clean separation between data, meta, and context components.

    The underlying data is split into separate components:
    - Data dict: Primary business data columns
    - Meta dict: Internal system metadata with {orcapod.META_PREFIX} ('__') prefixes
    - Context: Data context information with {orcapod.CONTEXT_KEY}

    Future Packet subclass will also handle:
    - Source info: Data provenance with {orcapod.SOURCE_PREFIX} ('_source_') prefixes

    When exposing to external tools, semantic types are encoded as
    `_{semantic_type}_` prefixes (_path_config_file, _id_user_name).

    All operations return new instances, preserving immutability.

    Example:
        >>> data = {{
        ...     "user_id": 123,
        ...     "name": "Alice",
        ...     "__pipeline_version": "v2.1.0",
        ...     "{orcapod.CONTEXT_KEY}": "financial_v1"
        ... }}
        >>> datagram = DictDatagram(data)
        >>> updated = datagram.update(name="Alice Smith")
    """

    def __init__(
        self,
        data: Mapping[str, DataValue],
        python_schema: PythonSchemaLike | None = None,
        meta_info: Mapping[str, DataValue] | None = None,
        data_context: str | contexts.DataContext | None = None,
        record_id: str | None = None,
        **kwargs,
    ) -> None:
        """
        Initialize DictDatagram from dictionary data.

        Args:
            data: Source data mapping containing all column data.
            typespec: Optional type specification for fields.
            semantic_converter: Optional converter for semantic type handling.
                If None, will be created based on data context and inferred types.
            data_context: Data context for semantic type resolution.
                If None and data contains context column, will extract from data.

        Note:
            The input data is automatically split into data, meta, and context
            components based on column naming conventions.
        """
        # Parse through data and extract different column types
        data_columns = {}
        meta_columns = {}
        extracted_context = None

        for k, v in data.items():
            if k == constants.CONTEXT_KEY:
                # Extract data context but keep it separate from meta data
                if data_context is None:
                    extracted_context = v
                # Don't store context in meta_data - it's managed separately
            elif k.startswith(constants.META_PREFIX):
                # Double underscore = meta metadata
                meta_columns[k] = v
            else:
                # Everything else = user data (including _source_ and semantic types)
                data_columns[k] = v

        # Initialize base class with data context
        final_context = data_context or cast(str, extracted_context)
        super().__init__(data_context=final_context, record_id=record_id, **kwargs)

        # Store data and meta components separately (immutable)
        self._data = dict(data_columns)
        if meta_info is not None:
            meta_columns.update(meta_info)
        self._meta_data = meta_columns

        # Combine provided typespec info with inferred typespec from content
        # If the column value is None and no type spec is provided, defaults to str.
        inferred_schema = infer_python_schema_from_pylist_data(
            [self._data], default_type=str
        )

        self._data_python_schema = (
            {k: python_schema.get(k, v) for k, v in inferred_schema.items()}
            if python_schema
            else inferred_schema
        )

        # Create schema for meta data
        inferred_meta_schema = infer_python_schema_from_pylist_data(
            [self._meta_data], default_type=str
        )
        self._meta_python_schema = (
            {k: python_schema.get(k, v) for k, v in inferred_meta_schema.items()}
            if python_schema
            else inferred_meta_schema
        )

        # Initialize caches
        self._cached_data_table: pa.Table | None = None
        self._cached_meta_table: pa.Table | None = None
        self._cached_content_hash: ContentHash | None = None
        self._cached_data_arrow_schema: pa.Schema | None = None
        self._cached_meta_arrow_schema: pa.Schema | None = None

    def _get_total_dict(self) -> dict[str, DataValue]:
        """
        Return the total dictionary representation including meta and context.

        This is used for content hashing and exporting to Arrow.
        """
        total_dict = dict(self._data)
        total_dict.update(self._meta_data)
        total_dict[constants.CONTEXT_KEY] = self._data_context
        return total_dict

    # 1. Core Properties (Identity & Structure)
    @property
    def meta_columns(self) -> tuple[str, ...]:
        """Return tuple of meta column names."""
        return tuple(self._meta_data.keys())

    def get_meta_info(self) -> dict[str, DataValue]:
        """
        Get meta column information.

        Returns:
            Dictionary of meta column names and their values.
        """
        return dict(self._meta_data)

    # 2. Dict-like Interface (Data Access)
    def __getitem__(self, key: str) -> DataValue:
        """Get data column value by key."""
        if key not in self._data:
            raise KeyError(f"Data column '{key}' not found")
        return self._data[key]

    def __contains__(self, key: str) -> bool:
        """Check if data column exists."""
        return key in self._data

    def __iter__(self) -> Iterator[str]:
        """Iterate over data column names."""
        return iter(self._data)

    def get(self, key: str, default: DataValue = None) -> DataValue:
        """Get data column value with default."""
        return self._data.get(key, default)

    # 3. Structural Information
    def keys(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[str, ...]:
        """Return tuple of column names."""
        column_config = ColumnConfig.handle_config(columns, all_info=all_info)
        include_meta_columns = column_config.meta
        include_context = column_config.context
        # Start with data columns
        result_keys = list(self._data.keys())

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
    ) -> PythonSchema:
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

        # Start with data schema
        schema = dict(self._data_python_schema)

        # Add context if requested
        if include_context:
            schema[constants.CONTEXT_KEY] = str

        # Add meta schema if requested
        if include_meta_columns and self._meta_data:
            if include_meta_columns is True:
                schema.update(self._meta_python_schema)
            elif isinstance(include_meta_columns, Collection):
                filtered_meta_schema = {
                    k: v
                    for k, v in self._meta_python_schema.items()
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
        column_config = ColumnConfig.handle_config(columns, all_info=all_info)
        include_meta_columns = column_config.meta
        include_context = column_config.context

        # Build data schema (cached)
        if self._cached_data_arrow_schema is None:
            self._cached_data_arrow_schema = (
                self._data_context.type_converter.python_schema_to_arrow_schema(
                    self._data_python_schema
                )
            )

        all_schemas = [self._cached_data_arrow_schema]

        # Add context schema if requested
        if include_context:
            context_schema = self.converter.python_schema_to_arrow_schema(
                {constants.CONTEXT_KEY: str}
            )
            all_schemas.append(context_schema)

        # Add meta schema if requested
        if include_meta_columns and self._meta_data:
            if include_meta_columns is True:
                meta_schema = self._get_meta_arrow_schema()
            elif isinstance(include_meta_columns, Collection):
                # Filter meta schema by prefix matching
                meta_schema = (
                    arrow_utils.select_schema_columns_with_prefixes(
                        self._get_meta_arrow_schema(),
                        include_meta_columns,
                    )
                    or None
                )
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
                self.as_table(columns={"meta": False, "context": False}),
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

        result_dict = dict(self._data)  # Start with user data

        # Add context if requested
        if include_context:
            result_dict[constants.CONTEXT_KEY] = self._data_context.context_key

        # Add meta columns if requested
        if include_meta_columns and self._meta_data:
            if include_meta_columns is True:
                # Include all meta columns
                result_dict.update(self._meta_data)
            elif isinstance(include_meta_columns, Collection):
                # Include only meta columns matching prefixes
                filtered_meta_data = {
                    k: v
                    for k, v in self._meta_data.items()
                    if any(k.startswith(prefix) for prefix in include_meta_columns)
                }
                result_dict.update(filtered_meta_data)

        return result_dict

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
        # FIXME: this is a super inefficient implementation!
        python_dict = self.as_dict(columns=columns, all_info=all_info)
        python_schema = self.schema(columns=columns, all_info=all_info)

        return self._data_context.type_converter.python_dicts_to_struct_dicts(
            [python_dict], python_schema=python_schema
        )[0]

    def _get_meta_arrow_table(self) -> "pa.Table":
        if self._cached_meta_table is None:
            arrow_schema = self._get_meta_arrow_schema()
            self._cached_meta_table = pa.Table.from_pylist(
                [self._meta_data],
                schema=arrow_schema,
            )
        assert self._cached_meta_table is not None, (
            "Meta Arrow table should be initialized by now"
        )
        return self._cached_meta_table

    def _get_meta_arrow_schema(self) -> "pa.Schema":
        if self._cached_meta_arrow_schema is None:
            self._cached_meta_arrow_schema = (
                self._data_context.type_converter.python_schema_to_arrow_schema(
                    self._meta_python_schema
                )
            )

        assert self._cached_meta_arrow_schema is not None, (
            "Meta Arrow schema should be initialized by now"
        )
        return self._cached_meta_arrow_schema

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

        # Build data table (cached)
        if self._cached_data_table is None:
            self._cached_data_table = (
                self._data_context.type_converter.python_dicts_to_arrow_table(
                    [self._data],
                    self._data_python_schema,
                )
            )
        assert self._cached_data_table is not None, (
            "Data Arrow table should be initialized by now"
        )
        result_table = self._cached_data_table

        # Add context if requested
        # TODO: consider using type converter for consistency
        if include_context:
            result_table = result_table.append_column(
                constants.CONTEXT_KEY,
                pa.array([self._data_context.context_key], type=pa.large_string()),
            )

        # Add meta columns if requested
        meta_table = None
        if include_meta_columns and self._meta_data:
            meta_table = self._get_meta_arrow_table()
            # Select appropriate meta columns
            if isinstance(include_meta_columns, Collection):
                # Filter meta columns by prefix matching
                meta_table = arrow_utils.select_table_columns_with_prefixes(
                    meta_table, include_meta_columns
                )

            # Combine tables if we have meta columns to add
            if meta_table:
                result_table = arrow_utils.hstack_tables(result_table, meta_table)

        return result_table

    # 5. Meta Column Operations
    def get_meta_value(self, key: str, default: DataValue = None) -> DataValue:
        """
        Get meta column value with optional default.

        Args:
            key: Meta column key (with or without {orcapod.META_PREFIX} ('__') prefix).
            default: Value to return if meta column doesn't exist.

        Returns:
            Meta column value if exists, otherwise the default value.
        """
        # Handle both prefixed and unprefixed keys
        if not key.startswith(constants.META_PREFIX):
            key = constants.META_PREFIX + key

        return self._meta_data.get(key, default)

    def with_meta_columns(self, **meta_updates: DataValue) -> Self:
        """
        Create a new DictDatagram with updated meta columns.
        Maintains immutability by returning a new instance.

        Args:
            **meta_updates: Meta column updates (keys will be prefixed with {orcapod.META_PREFIX} ('__') if needed)

        Returns:
            New DictDatagram instance
        """
        # Prefix the keys and prepare updates
        prefixed_updates = {}
        for k, v in meta_updates.items():
            if not k.startswith(constants.META_PREFIX):
                k = constants.META_PREFIX + k
            prefixed_updates[k] = v

        # Start with existing meta data
        new_meta_data = dict(self._meta_data)
        new_meta_data.update(prefixed_updates)

        # Reconstruct full data dict for new instance
        full_data = dict(self._data)  # User data
        full_data.update(new_meta_data)  # Meta data

        new_datagram = self.__class__(
            data=full_data,
            data_context=self._data_context,
        )

        # TODO: use copy instead
        new_datagram._record_id = self._record_id

        return new_datagram

    def drop_meta_columns(self, *keys: str, ignore_missing: bool = False) -> Self:
        """
        Create a new DictDatagram with specified meta columns dropped.
        Maintains immutability by returning a new instance.

        Args:
            *keys: Meta column keys to drop (with or without {orcapod.META_PREFIX} ('__') prefix)
            ignore_missing: If True, ignore missing meta columns without raising an error.

        Raises:
            KeyError: If any specified meta column to drop doesn't exist and ignore_missing=False.

        Returns:
            New DictDatagram instance without specified meta columns
        """
        # Normalize keys to have prefixes
        prefixed_keys = set()
        for key in keys:
            if not key.startswith(constants.META_PREFIX):
                key = constants.META_PREFIX + key
            prefixed_keys.add(key)

        missing_keys = prefixed_keys - set(self._meta_data.keys())
        if missing_keys and not ignore_missing:
            raise KeyError(
                f"Following meta columns do not exist and cannot be dropped: {sorted(missing_keys)}"
            )

        # Filter out specified meta columns
        new_meta_data = {
            k: v for k, v in self._meta_data.items() if k not in prefixed_keys
        }

        # Reconstruct full data dict for new instance
        full_data = dict(self._data)  # User data
        full_data.update(new_meta_data)  # Filtered meta data

        return self.__class__(
            data=full_data,
            data_context=self._data_context,
        )

    # 6. Data Column Operations
    def select(self, *column_names: str) -> Self:
        """
        Create a new DictDatagram with only specified data columns.
        Maintains immutability by returning a new instance.

        Args:
            *column_names: Data column names to keep

        Returns:
            New DictDatagram instance with only specified data columns
        """
        # Validate columns exist
        missing_cols = set(column_names) - set(self._data.keys())
        if missing_cols:
            raise KeyError(f"Columns not found: {missing_cols}")

        # Keep only specified data columns
        new_data = {k: v for k, v in self._data.items() if k in column_names}

        # Reconstruct full data dict for new instance
        full_data = new_data  # Selected user data
        full_data.update(self._meta_data)  # Keep existing meta data

        return self.__class__(
            data=full_data,
            data_context=self._data_context,
        )

    def drop(self, *column_names: str, ignore_missing: bool = False) -> Self:
        """
        Create a new DictDatagram with specified data columns dropped.
        Maintains immutability by returning a new instance.

        Args:
            *column_names: Data column names to drop

        Returns:
            New DictDatagram instance without specified data columns
        """
        # Filter out specified data columns
        missing = set(column_names) - set(self._data.keys())
        if missing and not ignore_missing:
            raise KeyError(
                f"Following columns do not exist and cannot be dropped: {sorted(missing)}"
            )

        new_data = {k: v for k, v in self._data.items() if k not in column_names}

        if not new_data:
            raise ValueError("Cannot drop all data columns")

        new_datagram = self.copy(include_cache=False)
        new_datagram._data = new_data
        return new_datagram

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
        new_data = {}
        for old_name, value in self._data.items():
            new_name = column_mapping.get(old_name, old_name)
            new_data[new_name] = value

        # Handle python_schema updates for renamed columns
        new_python_schema = None
        if self._data_python_schema:
            existing_python_schema = dict(self._data_python_schema)

            # Rename types according to column mapping
            renamed_python_schema = {}
            for old_name, old_type in existing_python_schema.items():
                new_name = column_mapping.get(old_name, old_name)
                renamed_python_schema[new_name] = old_type

            new_python_schema = renamed_python_schema

        # Reconstruct full data dict for new instance
        full_data = new_data  # Renamed user data
        full_data.update(self._meta_data)  # Keep existing meta data

        return self.__class__(
            data=full_data,
            python_schema=new_python_schema,
            data_context=self._data_context,
        )

    def update(self, **updates: DataValue) -> Self:
        """
        Create a new DictDatagram with existing column values updated.
        Maintains immutability by returning a new instance if any values are changed.

        Args:
            **updates: Column names and their new values (columns must exist)

        Returns:
            New DictDatagram instance with updated values

        Raises:
            KeyError: If any column doesn't exist (use with_columns() to add new columns)
        """
        if not updates:
            return self

        # Error if any column doesn't exist
        missing_columns = set(updates.keys()) - set(self._data.keys())
        if missing_columns:
            raise KeyError(
                f"Columns not found: {sorted(missing_columns)}. "
                f"Use with_columns() to add new columns."
            )

        # Update existing columns
        new_data = dict(self._data)
        new_data.update(updates)

        new_datagram = self.copy(include_cache=False)
        new_datagram._data = new_data
        return new_datagram

    def with_columns(
        self,
        column_types: Mapping[str, type] | None = None,
        **updates: DataValue,
    ) -> Self:
        """
        Create a new DictDatagram with new data columns added.
        Maintains immutability by returning a new instance.

        Args:
            column_updates: New data columns as a mapping
            column_types: Optional type specifications for new columns
            **kwargs: New data columns as keyword arguments

        Returns:
            New DictDatagram instance with new data columns added

        Raises:
            ValueError: If any column already exists (use update() instead)
        """
        # Combine explicit updates with kwargs

        if not updates:
            return self

        # Error if any column already exists
        existing_overlaps = set(updates.keys()) & set(self._data.keys())
        if existing_overlaps:
            raise ValueError(
                f"Columns already exist: {sorted(existing_overlaps)}. "
                f"Use update() to modify existing columns."
            )

        # Update user data with new columns
        new_data = dict(self._data)
        new_data.update(updates)

        # Create updated python schema - handle None values by defaulting to str
        python_schema = self.schema()
        if column_types is not None:
            python_schema.update(column_types)

        new_python_schema = infer_python_schema_from_pylist_data([new_data])
        new_python_schema = {
            k: python_schema.get(k, v) for k, v in new_python_schema.items()
        }

        new_datagram = self.copy(include_cache=False)
        new_datagram._data = new_data
        new_datagram._data_python_schema = new_python_schema

        return new_datagram

    # 8. Utility Operations
    def copy(self, include_cache: bool = True, preserve_record_id: bool = True) -> Self:
        """
        Create a shallow copy of the datagram.

        Returns a new datagram instance with the same data and cached values.
        This is more efficient than reconstructing from scratch when you need
        an identical datagram instance.

        Returns:
            New DictDatagram instance with copied data and caches.
        """
        new_datagram = super().copy(
            include_cache=include_cache, preserve_record_id=preserve_record_id
        )
        new_datagram._data = self._data.copy()
        new_datagram._meta_data = self._meta_data.copy()
        new_datagram._data_python_schema = self._data_python_schema.copy()
        new_datagram._meta_python_schema = self._meta_python_schema.copy()

        if include_cache:
            new_datagram._cached_data_table = self._cached_data_table
            new_datagram._cached_meta_table = self._cached_meta_table
            new_datagram._cached_content_hash = self._cached_content_hash
            new_datagram._cached_data_arrow_schema = self._cached_data_arrow_schema
            new_datagram._cached_meta_arrow_schema = self._cached_meta_arrow_schema
        else:
            new_datagram._cached_data_table = None
            new_datagram._cached_meta_table = None
            new_datagram._cached_content_hash = None
            new_datagram._cached_data_arrow_schema = None
            new_datagram._cached_meta_arrow_schema = None

        return new_datagram

    # 9. String Representations
    def __str__(self) -> str:
        """
        Return user-friendly string representation.

        Shows the datagram as a simple dictionary for user-facing output,
        messages, and logging. Only includes data columns for clean output.

        Returns:
            Dictionary-style string representation of data columns only.
        """
        return str(self._data)

    def __repr__(self) -> str:
        """
        Return detailed string representation for debugging.

        Shows the datagram type and comprehensive information including
        data columns, meta columns count, and context for debugging purposes.

        Returns:
            Detailed representation with type and metadata information.
        """
        if DEBUG:
            meta_count = len(self.meta_columns)
            context_key = self.data_context_key

            return (
                f"{self.__class__.__name__}("
                f"data={self._data}, "
                f"meta_columns={meta_count}, "
                f"context='{context_key}'"
                f")"
            )
        else:
            return str(self._data)
