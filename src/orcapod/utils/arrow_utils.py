from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Collection
from typing import Any


from typing import TYPE_CHECKING
from orcapod.utils.lazy_module import LazyModule
from orcapod.system_constants import constants

if TYPE_CHECKING:
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")


def schema_select(
    arrow_schema: "pa.Schema",
    column_names: Collection[str],
    ignore_missing: bool = False,
) -> "pa.Schema":
    """
    Select a subset of columns from a PyArrow schema.

    Args:
        arrow_schema: The original PyArrow schema.
        column_names: A collection of column names to select.

    Returns:
        A new PyArrow schema containing only the selected columns.
    """
    if not ignore_missing:
        existing_columns = set(field.name for field in arrow_schema)
        missing_columns = set(column_names) - existing_columns
        if missing_columns:
            raise KeyError(f"Missing columns in Arrow schema: {missing_columns}")
    selected_fields = [field for field in arrow_schema if field.name in column_names]
    return pa.schema(selected_fields)


def schema_drop(
    arrow_schema: "pa.Schema",
    column_names: Collection[str],
    ignore_missing: bool = False,
) -> "pa.Schema":
    """
    Drop a subset of columns from a PyArrow schema.

    Args:
        arrow_schema: The original PyArrow schema.
        column_names: A collection of column names to drop.

    Returns:
        A new PyArrow schema containing only the remaining columns.
    """
    if not ignore_missing:
        existing_columns = set(field.name for field in arrow_schema)
        missing_columns = set(column_names) - existing_columns
        if missing_columns:
            raise KeyError(f"Missing columns in Arrow schema: {missing_columns}")
    remaining_fields = [
        field for field in arrow_schema if field.name not in column_names
    ]
    return pa.schema(remaining_fields)


def normalize_to_large_types(arrow_type: "pa.DataType") -> "pa.DataType":
    """
    Recursively convert Arrow types to their large variants where available.

    This ensures consistent schema representation regardless of the original
    type choices (e.g., string vs large_string, binary vs large_binary).

    Args:
        arrow_type: Arrow data type to normalize

    Returns:
        Arrow data type with large variants substituted

    Examples:
        >>> normalize_to_large_types(pa.string())
        large_string

        >>> normalize_to_large_types(pa.list_(pa.string()))
        large_list<large_string>

        >>> normalize_to_large_types(pa.struct([pa.field("name", pa.string())]))
        struct<name: large_string>
    """
    # Handle primitive types that have large variants
    if pa.types.is_null(arrow_type):
        # TODO: make this configurable
        return pa.large_string()
    if pa.types.is_string(arrow_type):
        return pa.large_string()
    elif pa.types.is_binary(arrow_type):
        return pa.large_binary()
    elif pa.types.is_list(arrow_type):
        # Regular list -> large_list with normalized element type
        element_type = normalize_to_large_types(arrow_type.value_type)
        return pa.large_list(element_type)

    # Large variants and fixed-size lists stay as-is (already normalized or no large variant)
    elif pa.types.is_large_string(arrow_type) or pa.types.is_large_binary(arrow_type):
        return arrow_type
    elif pa.types.is_large_list(arrow_type):
        # Still need to normalize the element type
        element_type = normalize_to_large_types(arrow_type.value_type)
        return pa.large_list(element_type)
    elif pa.types.is_fixed_size_list(arrow_type):
        # Fixed-size lists don't have large variants, but normalize element type
        element_type = normalize_to_large_types(arrow_type.value_type)
        return pa.list_(element_type, arrow_type.list_size)

    # Handle struct types recursively
    elif pa.types.is_struct(arrow_type):
        normalized_fields = []
        for field in arrow_type:
            normalized_field_type = normalize_to_large_types(field.type)
            normalized_fields.append(
                pa.field(
                    field.name,
                    normalized_field_type,
                    nullable=field.nullable,
                    metadata=field.metadata,
                )
            )
        return pa.struct(normalized_fields)

    # Handle map types (key and value types)
    elif pa.types.is_map(arrow_type):
        normalized_key_type = normalize_to_large_types(arrow_type.key_type)
        normalized_value_type = normalize_to_large_types(arrow_type.item_type)
        return pa.map_(normalized_key_type, normalized_value_type)

    # Handle union types
    elif pa.types.is_union(arrow_type):
        # Union types contain multiple child types
        normalized_child_types = []
        for i in range(arrow_type.num_fields):
            child_field = arrow_type[i]
            normalized_child_type = normalize_to_large_types(child_field.type)
            normalized_child_types.append(
                pa.field(child_field.name, normalized_child_type)
            )

        # Reconstruct union with normalized child types
        if isinstance(arrow_type, pa.SparseUnionType):
            return pa.sparse_union(normalized_child_types)
        else:  # dense union
            return pa.dense_union(normalized_child_types)

    # Handle dictionary types
    elif pa.types.is_dictionary(arrow_type):
        # Normalize the value type (dictionary values), keep index type as-is
        normalized_value_type = normalize_to_large_types(arrow_type.value_type)
        return pa.dictionary(arrow_type.index_type, normalized_value_type)  # type: ignore

    # All other types (int, float, bool, date, timestamp, etc.) don't have large variants
    else:
        return arrow_type


def normalize_schema_to_large_types(schema: "pa.Schema") -> "pa.Schema":
    """
    Convert a schema to use large variants of data types.

    This normalizes schemas so that string -> large_string, binary -> large_binary,
    list -> large_list, etc., handling nested structures recursively.

    Args:
        schema: Arrow schema to normalize

    Returns:
        New schema with large type variants, or same schema if no changes needed

    Examples:
        >>> schema = pa.schema([
        ...     pa.field("name", pa.string()),
        ...     pa.field("files", pa.list_(pa.string())),
        ... ])
        >>> normalize_schema_to_large_types(schema)
        name: large_string
        files: large_list<large_string>
    """
    normalized_fields = []
    schema_changed = False

    for field in schema:
        normalized_type = normalize_to_large_types(field.type)

        # Check if the type actually changed
        if normalized_type != field.type:
            schema_changed = True

        normalized_field = pa.field(
            field.name,
            normalized_type,
            nullable=field.nullable,
            metadata=field.metadata,
        )
        normalized_fields.append(normalized_field)

    # Only create new schema if something actually changed
    if schema_changed:
        return pa.schema(normalized_fields, metadata=schema.metadata)  # type: ignore
    else:
        return schema


def normalize_table_to_large_types(table: "pa.Table") -> "pa.Table":
    """
    Normalize table schema to use large type variants.

    Uses cast() which should be zero-copy for large variant conversions
    since they have identical binary representations, but ensures proper
    type validation and handles any edge cases safely.

    Args:
        table: Arrow table to normalize

    Returns:
        Table with normalized schema, or same table if no changes needed

    Examples:
        >>> table = pa.table({"name": ["Alice", "Bob"], "age": [25, 30]})
        >>> normalized = normalize_table_to_large_types(table)
        >>> normalized.schema
        name: large_string
        age: int64
    """
    normalized_schema = normalize_schema_to_large_types(table.schema)

    # If schema didn't change, return original table
    if normalized_schema is table.schema:
        return table

    # Use cast() for safety - should be zero-copy for large variant conversions
    # but handles Arrow's internal type validation and any edge cases properly
    return table.cast(normalized_schema)


def pylist_to_pydict(pylist: list[dict]) -> dict:
    """
    Convert a list of dictionaries to a dictionary of lists (columnar format).

    This function transforms row-based data (list of dicts) to column-based data
    (dict of lists), similar to converting from records format to columnar format.
    Missing keys in individual dictionaries are filled with None values.

    Args:
        pylist: List of dictionaries representing rows of data

    Returns:
        Dictionary where keys are column names and values are lists of column data

    Example:
        >>> data = [{'a': 1, 'b': 2}, {'a': 3, 'c': 4}]
        >>> pylist_to_pydict(data)
        {'a': [1, 3], 'b': [2, None], 'c': [None, 4]}
    """
    result = {}
    known_keys = set()
    for i, d in enumerate(pylist):
        known_keys.update(d.keys())
        for k in known_keys:
            result.setdefault(k, [None] * i).append(d.get(k, None))
    return result


def pydict_to_pylist(pydict: dict) -> list[dict]:
    """
    Convert a dictionary of lists (columnar format) to a list of dictionaries.

    This function transforms column-based data (dict of lists) to row-based data
    (list of dicts), similar to converting from columnar format to records format.
    All arrays in the input dictionary must have the same length.

    Args:
        pydict: Dictionary where keys are column names and values are lists of column data

    Returns:
        List of dictionaries representing rows of data

    Raises:
        ValueError: If arrays in the dictionary have inconsistent lengths

    Example:
        >>> data = {'a': [1, 3], 'b': [2, None], 'c': [None, 4]}
        >>> pydict_to_pylist(data)
        [{'a': 1, 'b': 2, 'c': None}, {'a': 3, 'b': None, 'c': 4}]
    """
    if not pydict:
        return []

    # Check all arrays have same length
    lengths = [len(v) for v in pydict.values()]
    if not all(length == lengths[0] for length in lengths):
        raise ValueError(
            f"Inconsistent array lengths: {dict(zip(pydict.keys(), lengths))}"
        )

    num_rows = lengths[0]
    if num_rows == 0:
        return []

    result = []
    keys = pydict.keys()
    for i in range(num_rows):
        row = {k: pydict[k][i] for k in keys}
        result.append(row)
    return result


def join_arrow_schemas(*schemas: "pa.Schema") -> "pa.Schema":
    """Join multiple Arrow schemas into a single schema, ensuring compatibility of fields. In particular,
    no field names should collide."""
    merged_fields = []
    for schema in schemas:
        merged_fields.extend(schema)
    return pa.schema(merged_fields)


def select_table_columns_with_prefixes(
    table: "pa.Table", prefix: str | Collection[str]
) -> "pa.Table":
    """
    Select columns from a PyArrow table that start with a specific prefix.

    Args:
        table (pa.Table): The original table.
        prefix (str): The prefix to filter column names.

    Returns:
        pa.Table: New table containing only the columns with the specified prefix.
    """
    if isinstance(prefix, str):
        prefix = [prefix]
    selected_columns = [
        col for col in table.column_names if any(col.startswith(p) for p in prefix)
    ]
    return table.select(selected_columns)


def select_schema_columns_with_prefixes(
    schema: "pa.Schema", prefix: str | Collection[str]
) -> "pa.Schema":
    """
    Select columns from an Arrow schema that start with a specific prefix.

    Args:
        schema (pa.Schema): The original schema.
        prefix (str): The prefix to filter column names.

    Returns:
        pa.Schema: New schema containing only the columns with the specified prefix.
    """
    if isinstance(prefix, str):
        prefix = [prefix]
    selected_fields = [
        field for field in schema if any(field.name.startswith(p) for p in prefix)
    ]
    return pa.schema(selected_fields)


def select_arrow_schema(schema: "pa.Schema", columns: Collection[str]) -> "pa.Schema":
    """
    Select specific columns from an Arrow schema.

    Args:
        schema (pa.Schema): The original schema.
        columns (Collection[str]): List of column names to select.

    Returns:
        pa.Schema: New schema containing only the specified columns.
    """
    selected_fields = [field for field in schema if field.name in columns]
    return pa.schema(selected_fields)


def hstack_tables(*tables: "pa.Table") -> "pa.Table":
    """
    Horizontally stack multiple PyArrow tables by concatenating their columns.

    All input tables must have the same number of rows and unique column names.

    Args:
        *tables: Variable number of PyArrow tables to stack horizontally

    Returns:
        Combined PyArrow table with all columns from input tables

    Raises:
        ValueError: If no tables provided, tables have different row counts,
                   or duplicate column names are found
    """
    if len(tables) == 0:
        raise ValueError("At least one table is required for horizontal stacking.")
    if len(tables) == 1:
        return tables[0]

    N = len(tables[0])
    for table in tables[1:]:
        if len(table) != N:
            raise ValueError(
                "All tables must have the same number of rows for horizontal stacking."
            )

    # create combined schema
    all_fields = []
    all_names = set()
    for table in tables:
        for field in table.schema:
            if field.name in all_names:
                raise ValueError(
                    f"Duplicate column name '{field.name}' found in input tables."
                )
            all_fields.append(field)
            all_names.add(field.name)
    combined_schmea = pa.schema(all_fields)

    # create combined columns
    all_columns = []
    for table in tables:
        all_columns += table.columns

    return pa.Table.from_arrays(all_columns, schema=combined_schmea)


def check_arrow_schema_compatibility(
    incoming_schema: "pa.Schema", target_schema: "pa.Schema", strict: bool = False
) -> tuple[bool, list[str]]:
    # TODO: add strict comparison
    """
    Check if incoming schema is compatible with current schema.

    Args:
        incoming_schema: Schema to validate
        target_schema: Expected schema to match against
        strict: If True, requires exact match of field names and types. If False (default),
            incoming_schema can have additional fields or different types as long as they are compatible.

    Returns:
        Tuple of (is_compatible, list_of_errors)
    """
    errors = []

    # Create lookup dictionaries for efficient access
    incoming_fields = {field.name: field for field in incoming_schema}
    target_fields = {field.name: field for field in target_schema}

    # Check each field in target_schema
    for field_name, target_field in target_fields.items():
        if field_name not in incoming_fields:
            errors.append(f"Missing field '{field_name}' in incoming schema")
            continue

        incoming_field = incoming_fields[field_name]

        # Check data type compatibility
        if not target_field.type.equals(incoming_field.type):
            # TODO: if not strict, allow type coercion
            errors.append(
                f"Type mismatch for field '{field_name}': "
                f"expected {target_field.type}, got {incoming_field.type}"
            )

        # Check semantic_type metadata if present in current schema
        current_metadata = target_field.metadata or {}
        incoming_metadata = incoming_field.metadata or {}

        if b"semantic_type" in current_metadata:
            expected_semantic_type = current_metadata[b"semantic_type"]

            if b"semantic_type" not in incoming_metadata:
                errors.append(
                    f"Missing 'semantic_type' metadata for field '{field_name}'"
                )
            elif incoming_metadata[b"semantic_type"] != expected_semantic_type:
                errors.append(
                    f"Semantic type mismatch for field '{field_name}': "
                    f"expected {expected_semantic_type.decode()}, "
                    f"got {incoming_metadata[b'semantic_type'].decode()}"
                )
        elif b"semantic_type" in incoming_metadata:
            errors.append(
                f"Unexpected 'semantic_type' metadata for field '{field_name}': "
                f"{incoming_metadata[b'semantic_type'].decode()}"
            )

    # If strict mode, check for additional fields in incoming schema
    if strict:
        for field_name in incoming_fields:
            if field_name not in target_fields:
                errors.append(f"Unexpected field '{field_name}' in incoming schema")

    return len(errors) == 0, errors


def split_by_column_groups(
    table,
    *column_groups: Collection[str],
) -> tuple["pa.Table | None", ...]:
    """
    Split the table into multiple tables based on the provided column groups.
    Each group is a collection of column names that should be included in the same table.
    The remaining columns that are not part of any group will be returned as the first table/None.
    """
    if not column_groups:
        return (table,)

    tables = []
    remaining_columns = set(table.column_names)

    for group in column_groups:
        group_columns = [col for col in group if col in remaining_columns]
        if group_columns:
            tables.append(table.select(group_columns))
            remaining_columns.difference_update(group_columns)
        else:
            tables.append(None)

    remaining_table = None
    if remaining_columns:
        ordered_remaining_columns = [
            col for col in table.column_names if col in remaining_columns
        ]
        remaining_table = table.select(ordered_remaining_columns)
    return (remaining_table, *tables)


def prepare_prefixed_columns(
    table: "pa.Table | pa.RecordBatch",
    prefix_info: Collection[str]
    | Mapping[str, Any | None]
    | Mapping[str, Mapping[str, Any | None]],
    exclude_columns: Collection[str] = (),
    exclude_prefixes: Collection[str] = (),
) -> tuple["pa.Table", dict[str, "pa.Table"]]:
    """
    Split a table into non-prefixed columns and per-prefix tables.

    Columns whose names start with one of the configured prefixes are grouped
    into a separate output table for that prefix, with the matching prefix
    removed from each grouped column name. Columns that do not match any
    prefix remain in the returned data table.

    Args:
        table: The input ``pyarrow.Table`` or ``pyarrow.RecordBatch`` whose
            columns will be partitioned into a main data table and zero or more
            prefix-specific tables.
        prefix_info: Prefix configuration. This may be:

            * a collection of prefix strings, in which case each prefix is used
              with no additional per-prefix metadata; or
            * a mapping from prefix string to metadata/``None``; or
            * a mapping from prefix string to a mapping of per-column metadata.

            The exact metadata values are consumed by the implementation when
            constructing the per-prefix tables, but the keys define the set of
            prefixes that will be recognized.
        exclude_columns: Column names to omit from the constructed output.
        exclude_prefixes: Prefix groups to skip entirely even if matching input
            columns are present.

    Returns:
        A tuple ``(data_table, prefixed_tables)`` where ``data_table`` is a
        ``pyarrow.Table`` containing the columns from ``table`` that were not
        assigned to any prefix group, and ``prefixed_tables`` is a dictionary
        mapping each recognized prefix to its constructed ``pyarrow.Table``.

        Nullability is preserved from the source columns for columns copied
        directly from ``table``.
    """
    all_prefix_info = {}
    if isinstance(prefix_info, Mapping):
        for prefix, info in prefix_info.items():
            if isinstance(info, Mapping):
                all_prefix_info[prefix] = info
            else:
                all_prefix_info[prefix] = info
    elif isinstance(prefix_info, Collection):
        for prefix in prefix_info:
            all_prefix_info[prefix] = {}
    else:
        raise TypeError(
            "prefix_group must be a Collection of strings or a Mapping of string to string or None."
        )

    # split column into prefix groups
    data_column_names = []
    data_columns = []
    existing_prefixed_columns = defaultdict(list)

    for col_name in table.column_names:
        prefix_found = False
        for prefix in all_prefix_info:
            if col_name.startswith(prefix):
                # Remove the prefix from the column name
                base_name = col_name.removeprefix(prefix)
                existing_prefixed_columns[prefix].append(base_name)
                prefix_found = True
        if not prefix_found:
            # if no prefix found, consider this as a data column
            data_column_names.append(col_name)
            data_columns.append(table[col_name])

    # Create source_info columns for each regular column
    num_rows = table.num_rows

    prefixed_column_names = defaultdict(list)
    prefixed_columns = defaultdict(list)
    prefixed_column_nullables: dict[str, list[bool]] = defaultdict(list)

    target_column_names = [
        c
        for c in data_column_names
        if not any(c.startswith(prefix) for prefix in exclude_prefixes)
        and c not in exclude_columns
    ]

    for prefix, value_lut in all_prefix_info.items():
        target_prefixed_column_names = prefixed_column_names[prefix]
        target_prefixed_columns = prefixed_columns[prefix]
        target_nullables = prefixed_column_nullables[prefix]

        for col_name in target_column_names:
            prefixed_col_name = f"{prefix}{col_name}"
            existing_columns = existing_prefixed_columns[prefix]

            if isinstance(value_lut, Mapping):
                value = value_lut.get(col_name)
            else:
                value = value_lut

            if value is not None:
                # Concrete value supplied — column is never null.
                # TODO: clean up the logic here
                if not isinstance(value, str) and isinstance(value, Collection):
                    # TODO: this won't work other data types!!!
                    column_values = pa.array(
                        [value] * num_rows, type=pa.list_(pa.large_string())
                    )
                else:
                    column_values = pa.array([value] * num_rows, type=pa.large_string())
                col_nullable = False
            elif col_name in existing_columns:
                # Pass-through from the source table — inherit its nullable flag.
                existing_col = table[prefixed_col_name]
                if existing_col.type == pa.string():
                    # Convert to large_string
                    column_values = pa.compute.cast(existing_col, pa.large_string())  # type: ignore
                else:
                    column_values = existing_col
                col_nullable = table.schema.field(prefixed_col_name).nullable
            else:
                # No value available — column is genuinely all-null.
                column_values = pa.array([None] * num_rows, type=pa.large_string())
                col_nullable = True
            # Safety: never declare non-nullable when the data actually has nulls
            # (e.g. a pass-through column from a source that was non-nullable can
            # gain nulls after a join populates only one side of the result).
            col_nullable = col_nullable or bool(column_values.null_count > 0)
            target_prefixed_column_names.append(prefixed_col_name)
            target_prefixed_columns.append(column_values)
            target_nullables.append(col_nullable)

    # Step 3: Create the final table, preserving nullable flags from the source table
    data_fields = [table.schema.field(name) for name in data_column_names]
    data_schema = pa.schema(data_fields, metadata=table.schema.metadata)
    data_table: pa.Table = pa.Table.from_arrays(data_columns, schema=data_schema)
    result_tables = {}
    for prefix in all_prefix_info:
        schema = pa.schema([
            pa.field(name, col.type, nullable=nullable)
            for name, col, nullable in zip(
                prefixed_column_names[prefix],
                prefixed_columns[prefix],
                prefixed_column_nullables[prefix],
            )
        ])
        prefix_table = pa.Table.from_arrays(prefixed_columns[prefix], schema=schema)
        result_tables[prefix] = normalize_table_to_large_types(prefix_table)

    return data_table, result_tables


def drop_schema_columns(schema: "pa.Schema", columns: Collection[str]) -> "pa.Schema":
    """
    Drop specified columns from a PyArrow schema.

    Args:
        schema (pa.Schema): The original schema.
        columns (list[str]): List of column names to drop.

    Returns:
        pa.Schema: New schema with specified columns removed.
    """
    return pa.schema([field for field in schema if field.name not in columns])


# Test function to demonstrate usage
def test_schema_normalization():
    """Test the schema normalization functions."""
    print("=== Testing Arrow Schema Normalization ===\n")

    # Test basic types
    print("1. Basic type normalization:")
    basic_types = [
        pa.string(),
        pa.binary(),
        pa.list_(pa.string()),
        pa.large_string(),  # Should stay the same
        pa.int64(),  # Should stay the same
    ]

    for arrow_type in basic_types:
        normalized = normalize_to_large_types(arrow_type)
        print(f"  {arrow_type} -> {normalized}")

    print("\n2. Complex nested type normalization:")
    complex_types = [
        pa.struct(
            [pa.field("name", pa.string()), pa.field("files", pa.list_(pa.binary()))]
        ),
        pa.map_(pa.string(), pa.list_(pa.string())),
        pa.large_list(
            pa.struct([pa.field("id", pa.int64()), pa.field("path", pa.string())])
        ),
    ]

    for arrow_type in complex_types:
        normalized = normalize_to_large_types(arrow_type)
        print(f"  {arrow_type}")
        print(f"  -> {normalized}")
        print()

    print("3. Schema normalization:")
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
            pa.field("files", pa.list_(pa.string())),
            pa.field(
                "metadata",
                pa.struct(
                    [pa.field("created", pa.string()), pa.field("size", pa.int64())]
                ),
            ),
        ]
    )

    print("Original schema:")
    print(schema)

    normalized_schema = normalize_schema_to_large_types(schema)
    print("\nNormalized schema:")
    print(normalized_schema)

    print("\n4. Table normalization:")
    table_data = {
        "name": ["Alice", "Bob", "Charlie"],
        "files": [
            ["file1.txt", "file2.txt"],
            ["data.csv"],
            ["config.json", "output.log"],
        ],
    }

    table = pa.table(table_data)
    print("Original table schema:")
    print(table.schema)

    normalized_table = normalize_table_to_large_types(table)
    print("\nNormalized table schema:")
    print(normalized_table.schema)

    # Verify data is preserved
    print(f"\nData preserved: {table.to_pydict() == normalized_table.to_pydict()}")


def make_schema_non_nullable(schema: "pa.Schema") -> "pa.Schema":
    """Return a copy of *schema* with every field's ``nullable`` flag set to False.

    Arrow tables default all fields to ``nullable=True`` as a storage convention.
    When recovering Python types from a raw Arrow schema via
    ``arrow_schema_to_python_schema``, callers should normalise the schema first
    to prevent plain fields from being reconstructed as ``T | None``.

    Args:
        schema: The Arrow schema to normalise.

    Returns:
        A new ``pa.Schema`` with the same fields and types but ``nullable=False``
        on every field.
    """
    return pa.schema(
        [pa.field(f.name, f.type, nullable=False, metadata=f.metadata) for f in schema],
        metadata=schema.metadata,
    )


def infer_schema_nullable(table: "pa.Table") -> "pa.Schema":
    """Return a schema where each field's nullable flag is inferred from actual data.

    A field is marked ``nullable=True`` if its column contains any null values,
    ``nullable=False`` otherwise.  This is the right heuristic when receiving
    a raw Arrow table whose schema has not been set deliberately — Arrow and
    Polars default all fields to ``nullable=True`` regardless of content, so
    inferring from the data avoids spurious ``T | None`` types for columns
    that never contain nulls.

    Args:
        table: Arrow table to inspect.

    Returns:
        A new schema with nullable flags derived from actual null counts.
    """
    return pa.schema(
        [
            pa.field(
                field.name,
                field.type,
                nullable=table.column(i).null_count > 0,
                metadata=field.metadata,
            )
            for i, field in enumerate(table.schema)
        ],
        metadata=table.schema.metadata,
    )


def restore_schema_nullability(
    table: "pa.Table",
    *reference_schemas: "pa.Schema",
) -> "pa.Table":
    """Restore nullable flags lost during an Arrow → Polars → Arrow round-trip.

    Polars converts all Arrow fields to ``nullable=True`` when producing its
    Arrow output.  This function repairs the resulting table by looking up each
    field name in the supplied reference schemas and reinstating the original
    ``nullable`` flag (and type) from those references.

    Fields that are **not** found in any reference schema are left exactly as
    Polars produced them (``nullable=True``), which is safe for internally
    generated sentinel columns (e.g. ``_exists``, ``__pipeline_entry_id``).

    When the same field name appears in multiple reference schemas the
    **last-supplied** schema wins, so callers can pass ``(left_schema,
    right_schema)`` and get right-side nullability for join-key columns that
    appear in both.

    Args:
        table: Arrow table produced by a Polars round-trip (all fields
            ``nullable=True``).
        *reference_schemas: One or more Arrow schemas carrying the original
            nullable flags.  Pass the schemas of every table that participated
            in the Polars join/sort so that all columns are covered.

    Returns:
        A new Arrow table cast to the restored schema.  Data is unchanged;
        only the schema metadata (nullability, field type, field metadata)
        is corrected.

    Raises:
        pyarrow.ArrowInvalid: If a restored field type is incompatible with
            the actual column data (should not happen for well-formed
            round-trips, but surfaced to the caller rather than silently
            discarded).

    Example::

        taginfo_schema = taginfo.schema
        results_schema = results.schema
        joined = (
            pl.DataFrame(taginfo)
            .join(pl.DataFrame(results), on="record_id", how="inner")
            .to_arrow()
        )
        joined = arrow_utils.restore_schema_nullability(
            joined, taginfo_schema, results_schema
        )
    """
    # Build a name → Field lookup; later schemas override earlier ones.
    field_lookup: dict[str, "pa.Field"] = {}
    for schema in reference_schemas:
        for field in schema:
            field_lookup[field.name] = field

    restored_fields = []
    for field in table.schema:
        ref = field_lookup.get(field.name)
        if ref is not None:
            restored_fields.append(
                pa.field(field.name, ref.type, nullable=ref.nullable, metadata=ref.metadata)
            )
        else:
            restored_fields.append(field)

    restored_schema = pa.schema(restored_fields, metadata=table.schema.metadata)
    return table.cast(restored_schema)


def drop_columns_with_prefix(
    table: "pa.Table",
    prefix: str | tuple[str, ...],
    exclude_columns: Collection[str] = (),
) -> "pa.Table":
    """Drop columns with a specific prefix from an Arrow table."""
    columns_to_drop = [
        col
        for col in table.column_names
        if col.startswith(prefix) and col not in exclude_columns
    ]
    return table.drop(columns=columns_to_drop)


def drop_system_columns(
    table: "pa.Table",
    system_column_prefix: tuple[str, ...] = (
        constants.META_PREFIX,
        constants.DATAGRAM_PREFIX,
    ),
) -> "pa.Table":
    return drop_columns_with_prefix(table, system_column_prefix)


def get_system_columns(table: "pa.Table") -> "pa.Table":
    """Get system columns from an Arrow table."""
    return table.select(
        [
            col
            for col in table.column_names
            if col.startswith(constants.SYSTEM_TAG_PREFIX)
        ]
    )


def add_system_tag_columns(
    table: "pa.Table",
    schema_hash: str,
    source_ids: str | Collection[str],
    record_ids: Collection[str],
) -> "pa.Table":
    """Add paired source_id and record_id system tag columns to an Arrow table."""
    if not table.column_names:
        raise ValueError("Table is empty")

    # Normalize source_ids
    if isinstance(source_ids, str):
        source_ids = [source_ids] * table.num_rows
    else:
        source_ids = list(source_ids)
        if len(source_ids) != table.num_rows:
            raise ValueError(
                "Length of source_ids must match number of rows in the table."
            )

    record_ids = list(record_ids)
    if len(record_ids) != table.num_rows:
        raise ValueError("Length of record_ids must match number of rows in the table.")

    source_id_col_name = f"{constants.SYSTEM_TAG_SOURCE_ID_PREFIX}{constants.BLOCK_SEPARATOR}{schema_hash}"
    record_id_col_name = f"{constants.SYSTEM_TAG_RECORD_ID_PREFIX}{constants.BLOCK_SEPARATOR}{schema_hash}"

    source_id_array = pa.array(source_ids, type=pa.large_string())
    record_id_array = pa.array(record_ids, type=pa.large_string())

    # System tag columns are always computed, never null — declare nullable=False
    # explicitly so the schema intent is not lost in Polars round-trips.
    table = table.append_column(
        pa.field(source_id_col_name, pa.large_string(), nullable=False), source_id_array
    )
    table = table.append_column(
        pa.field(record_id_col_name, pa.large_string(), nullable=False), record_id_array
    )
    return table


def append_to_system_tags(table: "pa.Table", value: str) -> "pa.Table":
    """Append a value to the system tags column in an Arrow table."""
    if not table.column_names:
        raise ValueError("Table is empty")

    column_name_map = {
        c: f"{c}{constants.BLOCK_SEPARATOR}{value}"
        if c.startswith(constants.SYSTEM_TAG_PREFIX)
        else c
        for c in table.column_names
    }
    return table.rename_columns(column_name_map)


def _parse_system_tag_column(
    col_name: str,
) -> tuple[str, str, str] | None:
    """Parse a system tag column name into (field_type, provenance_path, position).

    For example:
        _tag_source_id::abc123::def456:0
        → field_type="source_id", provenance_path="abc123::def456", position="0"

        _tag_record_id::abc123::def456:0
        → field_type="record_id", provenance_path="abc123::def456", position="0"

    Returns None if the column doesn't end with a :position suffix.
    """
    # Strip the trailing :position
    base, sep, position = col_name.rpartition(constants.FIELD_SEPARATOR)
    if not sep or not position.isdigit():
        return None

    # Determine field type by checking known prefixes
    prefix = constants.SYSTEM_TAG_PREFIX
    if not base.startswith(prefix):
        return None

    after_prefix = base[len(prefix) :]  # e.g. "source_id::abc123::def456"

    # Extract field_type and provenance_path
    # field_type is everything before the first BLOCK_SEPARATOR
    field_type, block_sep, provenance_path = after_prefix.partition(
        constants.BLOCK_SEPARATOR
    )
    if not block_sep:
        return None

    return field_type, provenance_path, position


def sort_system_tag_values(table: "pa.Table") -> "pa.Table":
    """Sort paired system tag values for columns that share the same provenance path.

    System tag columns come in (source_id, record_id) pairs. Columns that differ
    only by their canonical position (the final :N) represent streams with the same
    pipeline_hash that were joined. For commutativity, paired (source_id, record_id)
    tuples must be sorted together per row so that the result is independent of
    input order.

    Algorithm:
    1. Parse each system tag column into (field_type, provenance_path, position)
    2. Group by provenance_path — source_id and record_id at the same path+position
       are paired
    3. For each group with >1 position, sort per-row by (source_id, record_id) tuples
    4. Assign sorted values back to both columns at each position
    """
    sys_tag_cols = [
        c for c in table.column_names if c.startswith(constants.SYSTEM_TAG_PREFIX)
    ]

    if not sys_tag_cols:
        return table

    # Parse all system tag columns and group by provenance_path
    # groups[provenance_path][position] = {field_type: col_name}
    groups: dict[str, dict[str, dict[str, str]]] = {}
    for col in sys_tag_cols:
        parsed = _parse_system_tag_column(col)
        if parsed is None:
            continue
        field_type, provenance_path, position = parsed
        groups.setdefault(provenance_path, {}).setdefault(position, {})[field_type] = (
            col
        )

    source_id_field = constants.SYSTEM_TAG_SOURCE_ID_PREFIX[
        len(constants.SYSTEM_TAG_PREFIX) :
    ]
    record_id_field = constants.SYSTEM_TAG_RECORD_ID_PREFIX[
        len(constants.SYSTEM_TAG_PREFIX) :
    ]

    # For each provenance_path group with >1 position, sort paired tuples per row
    for provenance_path, positions in groups.items():
        if len(positions) <= 1:
            continue

        # Sort positions numerically
        sorted_positions = sorted(positions.keys(), key=int)

        # Collect paired column names for each position
        paired_cols: list[tuple[str | None, str | None]] = []
        for pos in sorted_positions:
            field_map = positions[pos]
            sid_col = field_map.get(source_id_field)
            rid_col = field_map.get(record_id_field)
            paired_cols.append((sid_col, rid_col))

        # Get values for all paired columns
        sid_values = []
        rid_values = []
        for sid_col, rid_col in paired_cols:
            sid_values.append(
                table.column(sid_col).to_pylist()
                if sid_col
                else [None] * table.num_rows
            )
            rid_values.append(
                table.column(rid_col).to_pylist()
                if rid_col
                else [None] * table.num_rows
            )

        # Sort per row by (source_id, record_id) tuples
        n_positions = len(sorted_positions)
        sorted_sid: list[list] = [[] for _ in range(n_positions)]
        sorted_rid: list[list] = [[] for _ in range(n_positions)]

        for row_idx in range(table.num_rows):
            row_tuples = [
                (sid_values[pos_idx][row_idx], rid_values[pos_idx][row_idx])
                for pos_idx in range(n_positions)
            ]
            row_tuples.sort()
            for pos_idx, (sid_val, rid_val) in enumerate(row_tuples):
                sorted_sid[pos_idx].append(sid_val)
                sorted_rid[pos_idx].append(rid_val)

        # Replace columns with sorted values
        for pos_idx, (sid_col, rid_col) in enumerate(paired_cols):
            if sid_col:
                orig_type = table.column(sid_col).type
                tbl_idx = table.column_names.index(sid_col)
                table = table.drop(sid_col)
                table = table.add_column(
                    tbl_idx,
                    sid_col,
                    pa.array(sorted_sid[pos_idx], type=orig_type),
                )
            if rid_col:
                orig_type = table.column(rid_col).type
                tbl_idx = table.column_names.index(rid_col)
                table = table.drop(rid_col)
                table = table.add_column(
                    tbl_idx,
                    rid_col,
                    pa.array(sorted_rid[pos_idx], type=orig_type),
                )

    return table


def add_source_info(
    table: "pa.Table",
    source_info: str | Collection[str] | None,
    exclude_prefixes: Collection[str] = (
        constants.META_PREFIX,
        constants.DATAGRAM_PREFIX,
    ),
    exclude_columns: Collection[str] = (),
) -> "pa.Table":
    """Add source information to an Arrow table."""
    # Create a base list of per-row source tokens; one entry per row.
    if source_info is None or isinstance(source_info, str):
        base_source = [source_info] * table.num_rows
    elif isinstance(source_info, Collection):
        if len(source_info) != table.num_rows:
            raise ValueError(
                "Length of source_info collection must match number of rows in the table."
            )
        base_source = list(source_info)
    else:
        raise TypeError(
            f"source_info must be a str, a sized Collection[str], or None; "
            f"got {type(source_info).__name__}"
        )

    # For each data column, build an independent _source_<col> column from the
    # base tokens.  We must NOT re-use the array produced for a previous column
    # as input for the next one — doing so would accumulate column names
    # (e.g. "src::col1::col2" instead of "src::col2").
    for col in table.column_names:
        if col.startswith(tuple(exclude_prefixes)) or col in exclude_columns:
            continue
        col_source = pa.array(
            [f"{source_val}::{col}" for source_val in base_source],
            type=pa.large_string(),
        )
        # Source info columns are always computed strings, never null.
        table = table.append_column(
            pa.field(f"{constants.SOURCE_PREFIX}{col}", pa.large_string(), nullable=False),
            col_source,
        )

    return table


if __name__ == "__main__":
    test_schema_normalization()
