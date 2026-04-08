from __future__ import annotations

from collections.abc import Iterator, Mapping
from typing import (
    TYPE_CHECKING,
    Any,
    Protocol,
    Self,
    runtime_checkable,
)

from orcapod.protocols.hashing_protocols import (
    ContentIdentifiableProtocol,
    DataContextAwareProtocol,
)
from orcapod.types import ColumnConfig, DataValue, Schema

if TYPE_CHECKING:
    import pyarrow as pa


@runtime_checkable
class DatagramProtocol(ContentIdentifiableProtocol, DataContextAwareProtocol, Protocol):
    """
    Protocol for immutable datagram containers in Orcapod.

    Datagrams are the fundamental units of data that flow through the system.
    They provide a unified interface for data access, conversion, and manipulation,
    ensuring consistent behavior across different storage backends (dict, Arrow table, etc.).

    Each datagram contains:
    - **Data columns**: The primary business data (user_id, name, etc.)
    - **Meta columns**: Internal system metadata with {constants.META_PREFIX} (typically '__') prefixes (e.g. __processed_at, etc.)
    - **Context column**: Data context information ({constants.CONTEXT_KEY})

    Derivative of datagram (such as PacketProtocol or TagProtocol) will also include some specific columns pertinent to the function of the specialized datagram:
    - **Source info columns**: Data provenance with {constants.SOURCE_PREFIX} ('_source_') prefixes (_source_user_id, etc.) used in PacketProtocol
    - **System tags**: Internal tags for system use, typically prefixed with {constants.SYSTEM_TAG_PREFIX} ('_system_') (_system_created_at, etc.) used in TagProtocol

    All operations are by design immutable - methods return new datagram instances rather than modifying existing ones.

    Example:
        >>> datagram = DictDatagram({"user_id": 123, "name": "Alice"})
        >>> updated = datagram.update(name="Alice Smith")
        >>> filtered = datagram.select("user_id", "name")
        >>> table = datagram.as_table()
    """

    @property
    def datagram_id(self) -> str:
        """
        Return the UUID of this datagram.

        Returns:
            UUID: The unique identifier for this instance of datagram.
        """
        ...

    @property
    def meta_columns(self) -> tuple[str, ...]:
        """Return tuple of meta column names (with {constants.META_PREFIX} ('__') prefix)."""
        ...

    # 2. Dict-like Interface (Data Access)
    def __getitem__(self, key: str) -> DataValue:
        """
        Get data column value by key.

        Provides dict-like access to data columns only. Meta columns
        are not accessible through this method (use `get_meta_value()` instead).

        Args:
            key: Data column name.

        Returns:
            The value stored in the specified data column.

        Raises:
            KeyError: If the column doesn't exist in data columns.

        Example:
            >>> datagram["user_id"]
            123
            >>> datagram["name"]
            'Alice'
        """
        ...

    def __contains__(self, key: str) -> bool:
        """
        Check if data column exists.

        Args:
            key: Column name to check.

        Returns:
            True if column exists in data columns, False otherwise.

        Example:
            >>> "user_id" in datagram
            True
            >>> "nonexistent" in datagram
            False
        """
        ...

    def __iter__(self) -> Iterator[str]:
        """
        Iterate over data column names.

        Provides for-loop support over column names, enabling natural iteration
        patterns without requiring conversion to dict.

        Yields:
            Data column names in no particular order.

        Example:
            >>> for column in datagram:
            ...     value = datagram[column]
            ...     print(f"{column}: {value}")
        """
        ...

    def get(self, key: str, default: DataValue = None) -> DataValue:
        """
        Get data column value with default fallback.

        Args:
            key: Data column name.
            default: Value to return if column doesn't exist.

        Returns:
            Column value if exists, otherwise the default value.

        Example:
            >>> datagram.get("user_id")
            123
            >>> datagram.get("missing", "default")
            'default'
        """
        ...

    # 3. Structural Information
    def keys(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[str, ...]:
        """
        Return tuple of column names.

        Provides access to column names with filtering options for different
        column types. Default returns only data column names.

        Args:
            include_meta_columns: Controls meta column inclusion.
                - False: Return only data column names (default)
                - True: Include all meta column names
                - Collection[str]: Include meta columns matching these prefixes. If absent,
                    {orcapod.META_PREFIX} ('__') prefix is prepended to each key.
            include_context: Whether to include context column.

        Returns:
            Tuple of column names based on inclusion criteria.

        Example:
            >>> datagram.keys()  # Data columns only
            ('user_id', 'name', 'email')
            >>> datagram.keys(include_meta_columns=True)
            ('user_id', 'name', 'email', f'{orcapod.META_PREFIX}processed_at', f'{orcapod.META_PREFIX}pipeline_version')
            >>> datagram.keys(include_meta_columns=["pipeline"])
            ('user_id', 'name', 'email',f'{orcapod.META_PREFIX}pipeline_version')
            >>> datagram.keys(include_context=True)
            ('user_id', 'name', 'email', f'{orcapod.CONTEXT_KEY}')
        """
        ...

    def schema(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> Schema:
        """
        Return type specification mapping field names to Python types.

        The TypeSpec enables type checking and validation throughout the system.

        Args:
            include_meta_columns: Controls meta column type inclusion.
                - False: Exclude meta column types (default)
                - True: Include all meta column types
                - Collection[str]: Include meta column types matching these prefixes. If absent,
                    {orcapod.META_PREFIX} ('__') prefix is prepended to each key.
            include_context: Whether to include context type.

        Returns:
            TypeSpec mapping field names to their Python types.

        Example:
            >>> datagram.types()
            {'user_id': <class 'int'>, 'name': <class 'str'>}
        """
        ...

    def arrow_schema(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> pa.Schema:
        """
        Return PyArrow schema representation.

        The schema provides structured field and type information for efficient
        serialization and deserialization with PyArrow.

        Args:
            include_meta_columns: Controls meta column schema inclusion.
                - False: Exclude meta columns (default)
                - True: Include all meta columns
                - Collection[str]: Include meta columns matching these prefixes. If absent,
                    {orcapod.META_PREFIX} ('__') prefix is prepended to each key.
            include_context: Whether to include context column.

        Returns:
            PyArrow Schema describing the datagram structure.

        Example:
            >>> schema = datagram.arrow_schema()
            >>> schema.names
            ['user_id', 'name']
        """
        ...

    # 4. Format Conversions (Export)
    def as_dict(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> dict[str, DataValue]:
        """
        Convert datagram to dictionary format.

        Provides a simple key-value representation useful for debugging,
        serialization, and interop with dict-based APIs.

        Args:
            include_meta_columns: Controls meta column inclusion.
                - False: Exclude all meta columns (default)
                - True: Include all meta columns
                - Collection[str]: Include meta columns matching these prefixes. If absent,
                    {orcapod.META_PREFIX} ('__') prefix is prepended to each key.
            include_context: Whether to include the context key.
            include_all_info: If True, include all available information. This option supersedes all other inclusion options.


        Returns:
            Dictionary with requested columns as key-value pairs.

        Example:
            >>> data = datagram.as_dict()  # {'user_id': 123, 'name': 'Alice'}
            >>> full_data = datagram.as_dict(
            ...     include_meta_columns=True,
            ...     include_context=True
            ... )
        """
        ...

    def as_table(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> "pa.Table":
        """
        Convert datagram to PyArrow Table format.

        Provides a standardized columnar representation suitable for analysis,
        processing, and interoperability with Arrow-based tools.

        Args:
            include_meta_columns: Controls meta column inclusion.
                - False: Exclude all meta columns (default)
                - True: Include all meta columns
                - Collection[str]: Include meta columns matching these prefixes. If absent,
                    {orcapod.META_PREFIX} ('__') prefix is prepended to each key.
            include_context: Whether to include the context column.
            include_all_info: If True, include all available information. This option supersedes all other inclusion options.

        Returns:
            PyArrow Table with requested columns.

        Example:
            >>> table = datagram.as_table()  # Data columns only
            >>> full_table = datagram.as_table(
            ...     include_meta_columns=True,
            ...     include_context=True
            ... )
            >>> filtered = datagram.as_table(include_meta_columns=["pipeline"])   # same as passing f"{orcapod.META_PREFIX}pipeline"
        """
        ...

    def as_arrow_compatible_dict(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> dict[str, Any]:
        """
        Return a dictionary with values optimized for Arrow table conversion.

        This method returns a dictionary where values are in a form that can be
        efficiently converted to Arrow format using pa.Table.from_pylist().

        The key insight is that this avoids the expensive as_table() → concat pattern
        by providing values that are "Arrow-ready" while remaining in dict format
        for efficient batching.

        Implementation note: This may involve format conversions (e.g., Path objects
        to strings, datetime objects to ISO strings, etc.) to ensure compatibility
        with Arrow's expected input formats.

        Arrow table that results from pa.Table.from_pylist on the output of this should be accompanied
        with arrow_schema(...) with the same argument options to ensure that the schema matches the table.

        Args:
            include_all_info: Include all available information
            include_meta_columns: Controls meta column inclusion
            include_context: Whether to include context key

        Returns:
            A dictionary with values optimized for Arrow table conversion.

        Example:
            # Efficient batch conversion pattern
            arrow_dicts = [datagram.as_arrow_compatible_dict() for datagram in datagrams]
            schema = datagrams[0].arrow_schema()
            table = pa.Table.from_pylist(arrow_dicts, schema=schema)
        """
        ...

    # 5. Meta Column Operations
    def get_meta_value(self, key: str, default: DataValue = None) -> DataValue:
        """
        Get meta column value with optional default.

        Meta columns store operational metadata and use {orcapod.META_PREFIX} ('__') prefixes.
        This method handles both prefixed and unprefixed key formats.

        Args:
            key: Meta column key (with or without {orcapod.META_PREFIX} ('__') prefix).
            default: Value to return if meta column doesn't exist.

        Returns:
            Meta column value if exists, otherwise the default value.

        Example:
            >>> datagram.get_meta_value("pipeline_version")  # Auto-prefixed
            'v2.1.0'
            >>> datagram.get_meta_value("__pipeline_version")  # Already prefixed
            'v2.1.0'
            >>> datagram.get_meta_value("missing", "default")
            'default'
        """
        ...

    def with_meta_columns(self, **updates: DataValue) -> Self:
        """
        Create a new datagram with updated meta columns.

        Adds or updates operational metadata while preserving all data columns.
        Keys are automatically prefixed with {orcapod.META_PREFIX} ('__') if needed.

        Args:
            **updates: Meta column updates as keyword arguments.

        Returns:
            A new datagram instance with updated meta columns.

        Example:
            >>> tracked = datagram.with_meta_columns(
            ...     processed_by="pipeline_v2",
            ...     timestamp="2024-01-15T10:30:00Z"
            ... )
        """
        ...

    def drop_meta_columns(self, *keys: str, ignore_missing: bool = False) -> Self:
        """
        Create a new datagram with specified meta columns removed.

        Args:
            *keys: Meta column keys to remove (prefixes optional).
            ignore_missing: If True, ignore missing columns without raising an error.


        Returns:
            A new datagram instance without specified meta columns.

        Raises:
            KeyError: If any specified meta column to drop doesn't exist and ignore_missing=False.

        Example:
            >>> cleaned = datagram.drop_meta_columns("old_source", "temp_debug")
        """
        ...

    # 6. Data Column Operations
    def select(self, *column_names: str) -> Self:
        """
        Create a new datagram with only specified data columns.

        Args:
            *column_names: Data column names to keep.


        Returns:
            New datagram instance with only specified data columns. All other columns including
            meta columns and context are preserved.

        Raises:
            KeyError: If any specified column doesn't exist.

        Example:
            >>> subset = datagram.select("user_id", "name", "email")
        """
        ...

    def drop(self, *column_names: str, ignore_missing: bool = False) -> Self:
        """
        Create a new datagram with specified data columns removed. Note that this does not
        remove meta columns or context column. Refer to `drop_meta_columns()` for dropping
        specific meta columns. Context key column can never be dropped but a modified copy
        can be created with a different context key using `with_data_context()`.

        Args:
            *column_names: Data column names to remove.
            ignore_missing: If True, ignore missing columns without raising an error.

        Returns:
            New datagram instance without specified data columns.

        Raises:
            KeryError: If any specified column to drop doesn't exist and ignore_missing=False.

        Example:
            >>> filtered = datagram.drop("temp_field", "debug_info")
        """
        ...

    def rename(
        self,
        column_mapping: Mapping[str, str],
    ) -> Self:
        """
        Create a new datagram with data columns renamed.

        Args:
            column_mapping: Mapping from old names to new names.

        Returns:
            New datagram instance with renamed data columns.

        Example:
            >>> renamed = datagram.rename(
            ...     {"old_id": "user_id", "old_name": "full_name"},
            ...     column_types={"user_id": int}
            ... )
        """
        ...

    def update(self, **updates: DataValue) -> Self:
        """
        Create a new datagram with existing column values updated.

        Updates values in existing data columns. Will error if any specified
        column doesn't exist - use with_columns() to add new columns.

        Args:
            **updates: Column names and their new values.

        Returns:
            New datagram instance with updated values.

        Raises:
            KeyError: If any specified column doesn't exist.

        Example:
            >>> updated = datagram.update(
            ...     file_path="/new/absolute/path.txt",
            ...     status="processed"
            ... )
        """
        ...

    def with_columns(
        self,
        column_types: Mapping[str, type] | None = None,
        **updates: DataValue,
    ) -> Self:
        """
        Create a new datagram with additional data columns.

        Adds new data columns to the datagram. Will error if any specified
        column already exists - use update() to modify existing columns.

        Args:
            column_types: Optional type specifications for new columns. If not provided, the column type is
                inferred from the provided values. If value is None, the column type defaults to `str`.
            **kwargs: New columns as keyword arguments.

        Returns:
            New datagram instance with additional data columns.

        Raises:
            ValueError: If any specified column already exists.

        Example:
            >>> expanded = datagram.with_columns(
            ...     status="active",
            ...     score=95.5,
            ...     column_types={"score": float}
            ... )
        """
        ...

    # 7. Context Operations
    def with_context_key(self, new_context_key: str) -> Self:
        """
        Create new datagram with a different context key.

        Changes the semantic interpretation context while preserving all data.
        The context key affects how columns are processed and converted.

        Args:
            new_context_key: New context key string.

        Returns:
            New datagram instance with updated context key.

        Note:
            How the context is interpreted depends on the datagram implementation.
            Semantic processing may be rebuilt for the new context.

        Example:
            >>> financial_datagram = datagram.with_context_key("financial_v1")
        """
        ...

    # 8. Utility Operations
    def copy(self) -> Self:
        """
        Create a shallow copy of the datagram.

        Returns a new datagram instance with the same data and cached values.
        This is more efficient than reconstructing from scratch when you need
        an identical datagram instance.

        Returns:
            New datagram instance with copied data and caches.

        Example:
            >>> copied = datagram.copy()
            >>> copied is datagram  # False - different instance
            False
        """
        ...

    # 9. String Representations
    def __str__(self) -> str:
        """
        Return user-friendly string representation.

        Shows the datagram as a simple dictionary for user-facing output,
        messages, and logging. Only includes data columns for clean output.

        Returns:
            Dictionary-style string representation of data columns only.
        """
        ...

    def __repr__(self) -> str:
        """
        Return detailed string representation for debugging.

        Shows the datagram type and comprehensive information for debugging.

        Returns:
            A detailed representation with type and metadata information.
        """
        ...


@runtime_checkable
class TagProtocol(DatagramProtocol, Protocol):
    """
    Metadata associated with each data item in a stream.

    Tags carry contextual information about data packets as they flow through
    the computational graph. They are immutable and provide metadata that
    helps with:
    - Data lineage tracking
    - Grouping and aggregation operations
    - TemporalProtocol information (timestamps)
    - Source identification
    - Processing context

    Common examples include:
    - Timestamps indicating when data was created/processed
    - Source identifiers showing data origin
    - Processing metadata like batch IDs or session information
    - Grouping keys for aggregation operations
    - Quality indicators or confidence scores
    """

    def system_tags(self) -> dict[str, DataValue]:
        """
        Return metadata about the packet's source/origin.

        Provides debugging and lineage information about where the packet
        originated. May include information like:
        - File paths for file-based sources
        - Database connection strings
        - API endpoints
        - Processing pipeline information

        Returns:
            A dictionary with source information for each data column as key-value pairs.
        """
        ...


@runtime_checkable
class PacketProtocol(DatagramProtocol, Protocol):
    """
    The actual data payload in a stream.

    Packets represent the core data being processed through the computational
    graph. Unlike Tags (which are metadata), Packets contain the actual
    information that computations operate on.

    Packets extend DatagramProtocol with additional capabilities for:
    - Source tracking and lineage
    - Content-based hashing for caching
    - Metadata inclusion for debugging

    The distinction between TagProtocol and PacketProtocol is crucial for understanding
    data flow: Tags provide context, Packets provide content.
    """

    def source_info(self) -> dict[str, str | None]:
        """
        Return metadata about the packet's source/origin.

        Provides debugging and lineage information about where the packet
        originated. May include information like:
        - File paths for file-based sources
        - Database connection strings
        - API endpoints
        - Processing pipeline information

        Returns:
            dict[str, str | None]: Source information for each data column as key-value pairs.
        """
        ...

    def with_source_info(
        self,
        **source_info: str | None,
    ) -> Self:
        """
        Create a new packet with updated source information.

        Adds or updates source metadata for the packet. This is useful for
        tracking data provenance and lineage through the computational graph.

        Args:
            **source_info: Source metadata as keyword arguments.

        Returns:
            A new packet instance with updated source information.

        Example:
            >>> updated_packet = packet.with_source_info(
            ...     file_path="/new/path/to/file.txt",
            ...     source_id="source_123"
            ... )
        """
        ...
