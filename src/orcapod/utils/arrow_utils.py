# TODO: move this to a separate module
from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Collection
from typing import Any


from typing import TYPE_CHECKING
from orcapod.utils.lazy_module import LazyModule

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
    """ """
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

    target_column_names = [
        c
        for c in data_column_names
        if not any(c.startswith(prefix) for prefix in exclude_prefixes)
        and c not in exclude_columns
    ]

    for prefix, value_lut in all_prefix_info.items():
        target_prefixed_column_names = prefixed_column_names[prefix]
        target_prefixed_columns = prefixed_columns[prefix]

        for col_name in target_column_names:
            prefixed_col_name = f"{prefix}{col_name}"
            existing_columns = existing_prefixed_columns[prefix]

            if isinstance(value_lut, Mapping):
                value = value_lut.get(col_name)
            else:
                value = value_lut

            if value is not None:
                # Use value from source_info dictionary
                # TODO: clean up the logic here
                if not isinstance(value, str) and isinstance(value, Collection):
                    # TODO: this won't work other data types!!!
                    column_values = pa.array(
                        [value] * num_rows, type=pa.list_(pa.large_string())
                    )
                else:
                    column_values = pa.array([value] * num_rows, type=pa.large_string())
            # if col_name is in existing_source_info, use that column
            elif col_name in existing_columns:
                # Use existing prefixed column, but convert to large_string
                existing_col = table[prefixed_col_name]

                if existing_col.type == pa.string():
                    # Convert to large_string
                    column_values = pa.compute.cast(existing_col, pa.large_string())  # type: ignore
                else:
                    column_values = existing_col
            else:
                # Use null values
                column_values = pa.array([None] * num_rows, type=pa.large_string())
            target_prefixed_column_names.append(prefixed_col_name)
            target_prefixed_columns.append(column_values)

    # Step 3: Create the final table
    data_table: pa.Table = pa.Table.from_arrays(data_columns, names=data_column_names)
    result_tables = {}
    for prefix in all_prefix_info:
        prefix_table = pa.Table.from_arrays(
            prefixed_columns[prefix], names=prefixed_column_names[prefix]
        )
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


if __name__ == "__main__":
    test_schema_normalization()
