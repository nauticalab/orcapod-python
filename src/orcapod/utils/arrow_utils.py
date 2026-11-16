from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from collections.abc import Callable, Mapping, Collection, Sequence
from typing import Any, TYPE_CHECKING, cast

from orcapod.system_constants import constants
from orcapod.types import ColumnConfig, SourceInfoValue
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa
    from orcapod.protocols.semantic_types_protocols import TypeConverterProtocol
else:
    pa = LazyModule("pyarrow")


def make_empty_table(
    python_schema: "Mapping[str, Any]", type_converter: "TypeConverterProtocol"
) -> "pa.Table":
    """Return a zero-row PyArrow table whose field nullability matches ``python_schema``.

    Uses ``python_schema_to_arrow_schema`` so that plain types (``str``, ``int``, …)
    produce ``nullable=False`` fields and Optional types (``str | None``) produce
    ``nullable=True`` fields. This preserves the bidirectional round-trip through
    ``ArrowTableStream.output_schema()``.

    Args:
        python_schema: Mapping of field name to Python type annotation.
        type_converter: A ``UniversalTypeConverter`` instance.

    Returns:
        A zero-row ``pa.Table`` with the correct Arrow schema.
    """
    arrow_schema = type_converter.python_schema_to_arrow_schema(python_schema)
    return pa.Table.from_batches([], schema=arrow_schema)


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


def normalize_to_large_types(
    arrow_type: "pa.DataType", *, keep_null: bool = False
) -> "pa.DataType":
    """
    Recursively convert Arrow types to their large variants where available.

    This ensures consistent schema representation regardless of the original
    type choices (e.g., string vs large_string, binary vs large_binary). Only
    layouts change: every nested field keeps its name, nullability and
    metadata, and widths are untouched, so the Arrow hasher gives the
    normalized type the same schema and value hashes.

    Args:
        arrow_type: Arrow data type to normalize
        keep_null: Keep ``null`` types (at any depth) as ``null``. By default
            ``null`` becomes ``large_string``, which ``Data`` relies on; that is
            a type change, not a layout change, so code that must change
            layouts only (operators, the type converter) passes ``True``.

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
        return arrow_type if keep_null else pa.large_string()
    if pa.types.is_string(arrow_type) or pa.types.is_string_view(arrow_type):
        # string_view has no comparison/sort kernels in PyArrow (>=23, <=24), so
        # normalizing it to large_string is required for filtering, not just for
        # consistency. See design_issues/ D8 / ENG-601.
        return pa.large_string()
    elif pa.types.is_binary(arrow_type) or pa.types.is_binary_view(arrow_type):
        return pa.large_binary()
    elif pa.types.is_large_string(arrow_type) or pa.types.is_large_binary(arrow_type):
        return arrow_type
    # Nested types: every nested field is rebuilt with ``Field.with_type`` so
    # its name, nullability and metadata are kept. Dropping a non-null flag
    # would change the type's Arrow schema hash.
    elif pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
        return pa.large_list(
            _normalize_field_to_large_types(arrow_type.value_field, keep_null)
        )
    elif pa.types.is_fixed_size_list(arrow_type):
        # Fixed-size lists have no large variant; normalize the element only.
        return pa.list_(
            _normalize_field_to_large_types(arrow_type.value_field, keep_null),
            arrow_type.list_size,
        )
    elif pa.types.is_struct(arrow_type):
        return pa.struct(
            [_normalize_field_to_large_types(f, keep_null) for f in arrow_type]
        )
    elif pa.types.is_map(arrow_type):
        # pyarrow accepts key and item Fields here; its stubs only list types.
        return pa.map_(  # pyright: ignore[reportCallIssue]
            _normalize_field_to_large_types(arrow_type.key_field, keep_null),
            _normalize_field_to_large_types(arrow_type.item_field, keep_null),
            keys_sorted=arrow_type.keys_sorted,
        )
    elif pa.types.is_union(arrow_type):
        children = [
            _normalize_field_to_large_types(arrow_type.field(i), keep_null)
            for i in range(arrow_type.num_fields)
        ]
        if isinstance(arrow_type, pa.SparseUnionType):
            return pa.sparse_union(children, arrow_type.type_codes)
        return pa.dense_union(children, arrow_type.type_codes)
    elif pa.types.is_dictionary(arrow_type):
        # Normalize the value type (dictionary values), keep index type as-is
        return pa.dictionary(
            arrow_type.index_type,
            # The stubs restrict value_type to a few basic types; any type works.
            normalize_to_large_types(arrow_type.value_type, keep_null=keep_null),  # pyright: ignore[reportArgumentType]
            ordered=arrow_type.ordered,
        )

    # All other types (int, float, bool, date, timestamp, etc.) don't have large variants
    else:
        return arrow_type


def _normalize_field_to_large_types(
    field: "pa.Field[Any]", keep_null: bool
) -> "pa.Field[Any]":
    """Return ``field`` with its type normalized, keeping name, nullability and metadata.

    Args:
        field: A nested field (list value, struct child, map key or item, ...).
        keep_null: Passed through to ``normalize_to_large_types``.

    Returns:
        The same field with ``normalize_to_large_types`` applied to its type.
    """
    return field.with_type(normalize_to_large_types(field.type, keep_null=keep_null))


def normalize_schema_to_large_types(
    schema: "pa.Schema", *, keep_null: bool = False
) -> "pa.Schema":
    """
    Convert a schema to use large variants of data types.

    This normalizes schemas so that string -> large_string, binary -> large_binary,
    list -> large_list, etc., handling nested structures recursively.

    Args:
        schema: Arrow schema to normalize
        keep_null: Passed to ``normalize_to_large_types``. ``True`` keeps a
            ``null`` column ``null``, as operators and sources do; the default
            ``False`` maps it to ``large_string``, as ``Data`` does.

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
        normalized_type = normalize_to_large_types(field.type, keep_null=keep_null)

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


def shared_arrow_type(
    left_type: "pa.DataType", right_type: "pa.DataType"
) -> "pa.DataType | None":
    """Return the Arrow type two types of one column share, or ``None``.

    This is the single place the rule lives for an operator that combines one
    column from two inputs (``MergeJoin``'s colliding data and source-info
    columns, a shared tag key in ``Join`` and ``MergeJoin``).

    Both types are normalized to their large variants
    (``normalize_to_large_types`` with ``keep_null=True``: ``string`` to
    ``large_string``, ``binary`` to ``large_binary``, ``list`` to
    ``large_list``, recursively), which the Arrow hasher treats as identical.
    If the normalized types are equal, that is the shared type. Any other
    difference (integer or float width, timestamp unit, decimal precision,
    struct fields, nullability of a nested field, logical type) means there is
    no shared type: nothing is widened, promoted or picked. The result does
    not depend on argument order.

    Args:
        left_type: The column's Arrow type in one input.
        right_type: The column's Arrow type in the other input.

    Returns:
        The large-normalized type both inputs share, or ``None`` when the types
        still differ after normalization.
    """
    normalized_left = normalize_to_large_types(left_type, keep_null=True)
    normalized_right = normalize_to_large_types(right_type, keep_null=True)
    if normalized_left == normalized_right:
        return normalized_left
    return None


def normalize_table_to_large_types(
    table: "pa.Table", *, keep_null: bool = False
) -> "pa.Table":
    """
    Normalize table schema to use large type variants.

    Uses cast() which should be zero-copy for large variant conversions
    since they have identical binary representations, but ensures proper
    type validation and handles any edge cases safely. Only layouts change:
    width, nullability, field metadata and logical (extension) types are kept.

    Args:
        table: Arrow table to normalize
        keep_null: Passed to ``normalize_to_large_types``. ``True`` keeps a
            ``null`` column ``null``, as operators and sources do; the default
            ``False`` maps it to ``large_string``, as ``Data`` does.

    Returns:
        Table with normalized schema, or same table if no changes needed

    Examples:
        >>> table = pa.table({"name": ["Alice", "Bob"], "age": [25, 30]})
        >>> normalized = normalize_table_to_large_types(table)
        >>> normalized.schema
        name: large_string
        age: int64
    """
    # ``table.schema`` makes a new Schema object on every access: read it once,
    # so the identity check below can find an unchanged schema.
    schema = table.schema
    normalized_schema = normalize_schema_to_large_types(schema, keep_null=keep_null)

    # If schema didn't change, return original table
    if normalized_schema is schema:
        return table

    # Use cast() for safety - should be zero-copy for large variant conversions
    # but handles Arrow's internal type validation and any edge cases properly
    return table.cast(normalized_schema)


def normalize_view_types(arrow_type: "pa.DataType") -> "pa.DataType":
    """Recursively convert Arrow *view* types to their large variants.

    Maps ``string_view`` -> ``large_string`` and ``binary_view`` ->
    ``large_binary``, recursing into nested types; all other types are returned
    unchanged. PyArrow (>=23, <=24) has no comparison/sort kernels for view
    types, so they must be converted before any grouping, filtering, or storage
    or those operations raise ``ArrowNotImplementedError`` (ENG-601). Unlike
    ``normalize_to_large_types`` this leaves ``string`` / ``binary`` / ``list``
    as-is.
    """
    if pa.types.is_string_view(arrow_type):
        return pa.large_string()
    if pa.types.is_binary_view(arrow_type):
        return pa.large_binary()
    # For nested types, preserve the child fields' name/nullability/metadata
    # (and keys_sorted for maps) and only swap out the view type underneath.
    if pa.types.is_list(arrow_type):
        vf = arrow_type.value_field
        return pa.list_(vf.with_type(normalize_view_types(vf.type)))
    if pa.types.is_large_list(arrow_type):
        vf = arrow_type.value_field
        return pa.large_list(vf.with_type(normalize_view_types(vf.type)))
    if pa.types.is_fixed_size_list(arrow_type):
        vf = arrow_type.value_field
        return pa.list_(
            vf.with_type(normalize_view_types(vf.type)), arrow_type.list_size
        )
    if pa.types.is_struct(arrow_type):
        return pa.struct(
            [f.with_type(normalize_view_types(f.type)) for f in arrow_type]
        )
    if pa.types.is_map(arrow_type):
        kf, itf = arrow_type.key_field, arrow_type.item_field
        return pa.map_(
            kf.with_type(normalize_view_types(kf.type)),
            itf.with_type(normalize_view_types(itf.type)),
            keys_sorted=arrow_type.keys_sorted,
        )
    return arrow_type


def normalize_extension_columns(table: "pa.Table") -> "pa.Table":
    """Return a copy of ``table`` with all extension-typed columns converted to
    their IPC/Parquet storage representation.

    For each top-level column whose type is a ``pa.ExtensionType``, the column
    data is replaced with the underlying storage array (via
    ``ExtensionArray.storage`` — no Python-level materialization) and the field
    gains ``ARROW:extension:name`` and ``ARROW:extension:metadata`` keys in its
    metadata, exactly matching the on-disk Arrow IPC/Parquet format.

    Non-extension columns are returned unchanged.  Schema-level metadata and
    existing per-field metadata are preserved; the two ``ARROW:extension:*``
    keys are merged in (or added) without touching any other metadata already
    on the field.

    This is a fast path: for tables with no extension columns the original
    table object is returned immediately.  For tables that do have extension
    columns a new table is constructed; chunking is preserved and the column
    data itself is not copied — each chunk's ``ExtensionArray.storage``
    property returns a zero-copy view of the underlying buffers.

    Note: only **top-level** extension columns are handled.  Extension types
    nested inside struct fields or list element types are not supported by the
    orcapod type system (see ET1 in design_issues/) and are left unchanged.

    Args:
        table: Input Arrow table, may contain extension-typed columns.

    Returns:
        A ``pa.Table`` where every top-level extension-typed column has been
        replaced by its storage-typed equivalent with extension identity
        preserved in field metadata.
    """
    if not any(isinstance(field.type, pa.ExtensionType) for field in table.schema):
        return table

    new_columns: list[pa.ChunkedArray] = []
    new_fields: list[pa.Field] = []
    for i, field in enumerate(table.schema):
        if isinstance(field.type, pa.ExtensionType):
            ext_type = field.type
            # Preserve chunking: convert each ExtensionArray chunk to its
            # .storage chunk (zero-copy view of the underlying buffers) and
            # rebuild a ChunkedArray.  Calling combine_chunks() first would
            # allocate new buffers for multi-chunk columns, defeating the
            # zero-copy guarantee.
            col = table.column(i)
            storage_arr = pa.chunked_array(
                [chunk.storage for chunk in col.chunks],
                type=ext_type.storage_type,
            )
            serialized = ext_type.__arrow_ext_serialize__()
            # Merge extension identity into existing field metadata (if any)
            # so that non-extension keys already on the field are preserved.
            existing_meta = dict(field.metadata) if field.metadata else {}
            existing_meta[b"ARROW:extension:name"] = ext_type.extension_name.encode(
                "utf-8"
            )
            existing_meta[b"ARROW:extension:metadata"] = serialized
            new_fields.append(
                pa.field(
                    field.name,
                    ext_type.storage_type,
                    nullable=field.nullable,
                    metadata=existing_meta,
                )
            )
            new_columns.append(storage_arr)
        else:
            new_columns.append(table.column(i))
            new_fields.append(field)

    return pa.table(
        new_columns,
        schema=pa.schema(new_fields, metadata=table.schema.metadata),
    )


def normalize_table_view_types(table: "pa.Table") -> "pa.Table":
    """Cast a table's view-typed columns to their large variants.

    Returns the table unchanged if it has no view types. PyArrow lacks
    comparison/sort kernels for ``string_view`` / ``binary_view``, so a table
    carrying them breaks group_by, filter, and Delta predicate pushdown until
    converted (ENG-601).
    """
    new_types = [normalize_view_types(f.type) for f in table.schema]
    if all(nt == f.type for nt, f in zip(new_types, table.schema)):
        return table
    new_schema = pa.schema(
        [
            pa.field(f.name, nt, nullable=f.nullable, metadata=f.metadata)
            for f, nt in zip(table.schema, new_types)
        ],
        metadata=table.schema.metadata,
    )
    return table.cast(new_schema)


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


def column_arrow_type(schema: pa.Schema, name: str) -> pa.DataType:
    """Return the Arrow type of the column ``name`` in ``schema``.

    Reads ``schema.types`` by index rather than ``schema.field(name).type``,
    which the pyarrow stubs leave untyped.

    Args:
        schema: The schema to look the column up in.
        name: The column name; it must occur exactly once in ``schema``.

    Returns:
        The column's Arrow type, exactly as declared in ``schema``.

    Raises:
        KeyError: If ``name`` is absent from ``schema`` or occurs more than once.
    """
    index = schema.get_field_index(name)
    if index < 0:
        raise KeyError(f"Column {name!r} is absent from the schema or not unique")
    return schema.types[index]


def concat_tables_by_name(tables: "Sequence[pa.Table]") -> "pa.Table":
    """Vertically concatenate tables, matching columns by name rather than position.

    ``pa.concat_tables`` requires every table to list its columns in the same
    order. Per-row tables collected from a node's output do not always agree:
    a ``FunctionJobNode`` serves cached rows with the tag column order recorded
    at compile time and fresh rows with the order its upstream emitted. Each
    table is therefore reordered to the first table's column order before
    concatenating.

    Only the column order is reconciled. Tables whose column name sets differ
    are passed through unchanged so that ``pa.concat_tables`` raises as usual.

    Args:
        tables: Tables to concatenate, in row order.

    Returns:
        The concatenated table, with the first table's column order.

    Raises:
        ValueError: If ``tables`` is empty.
    """
    if not tables:
        raise ValueError("Cannot concatenate an empty sequence of tables")
    names = tables[0].column_names
    name_set = set(names)
    aligned = [
        table.select(names)
        if table.column_names != names
        and len(table.column_names) == len(names)
        and set(table.column_names) == name_set
        else table
        for table in tables
    ]
    return pa.concat_tables(aligned)


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


# TODO(NPIPE-204): this only understands scalars and ``list[T]``. Structured
# source-info values (a struct/dataclass token, or a mapping) will need their own
# branches, and the recursion should dispatch on a logical type rather than on
# ``isinstance(value, list)``. It also exists only because ``Data`` stores its
# source info as Python values; a ``Data`` that kept the Arrow source-info table
# it was built from would not need to re-type anything. Raised in review on
# PR #250 and PR #281.
def source_info_arrow_type(value: SourceInfoValue) -> "pa.DataType":
    """Return the Arrow type of one source-info provenance token.

    This is the single definition of the source-info type rule, used wherever a
    token arrives as a Python value with no Arrow type attached (``Data``
    serializing its source info, ``prepare_prefixed_columns`` broadcasting a
    supplied token). A token is a string, ``None``, or a list of tokens, one per
    member of a many->one operator; aggregating an already aggregated stream
    nests the list again (ITL-849). Scalars and ``None`` map to
    ``large_string``; a list maps to ``large_list`` of its element type.

    ``None`` and an empty list say nothing about the levels below them, so each
    list level takes its element type from the first element that does (a
    string, or a list holding one). ``[[], [["a"]]]`` is therefore three list
    levels deep, not two. A list with no such element falls back to its first
    element's type, and an empty list to ``large_list<large_string>``.

    Args:
        value: The provenance token.

    Returns:
        The Arrow type of the token.
    """
    return _source_info_arrow_type_and_depth_known(value)[0]


def _source_info_arrow_type_and_depth_known(
    value: SourceInfoValue,
) -> tuple["pa.DataType", bool]:
    """Type a token, reporting whether the token itself determined its depth.

    Args:
        value: The provenance token.

    Returns:
        ``(arrow_type, depth_known)``. ``depth_known`` is ``False`` for ``None``
        and for a list whose elements are all ``None`` or empty lists.
    """
    if not isinstance(value, list):
        return pa.large_string(), value is not None
    if not value:
        return pa.large_list(pa.large_string()), False
    element_types = (_source_info_arrow_type_and_depth_known(v) for v in value)
    fallback: "pa.DataType | None" = None
    for element_type, depth_known in element_types:
        if depth_known:
            return pa.large_list(element_type), True
        if fallback is None:
            fallback = element_type
    assert fallback is not None
    return pa.large_list(fallback), False


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
                # Concrete token supplied as a Python value — column is never
                # null. No Arrow type is attached, so the source-info rule
                # types it (a list token, nested or not, keeps its depth).
                # Every caller passes source-info tokens here.
                token = cast(SourceInfoValue, value)
                column_values = pa.array(
                    [token] * num_rows, type=source_info_arrow_type(token)
                )
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
        schema = pa.schema(
            [
                pa.field(name, col.type, nullable=nullable)
                for name, col, nullable in zip(
                    prefixed_column_names[prefix],
                    prefixed_columns[prefix],
                    prefixed_column_nullables[prefix],
                )
            ]
        )
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
                pa.field(
                    field.name, ref.type, nullable=ref.nullable, metadata=ref.metadata
                )
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


def system_tag_column_names(schema_hash: str) -> tuple[str, str]:
    """Return the (source_id_col_name, record_id_col_name) system-tag column names.

    These are the two column names that ``add_system_tag_columns`` adds to every
    source table, and that ``ArrowTableStream`` exposes via ``keys(system_tags=True)``
    and ``output_schema(system_tags=True)``.

    Args:
        schema_hash: Hex schema hash produced by ``compute_schema_hash()``.

    Returns:
        Tuple of ``(source_id_column_name, record_id_column_name)``.
        Both start with ``constants.SYSTEM_TAG_PREFIX``. Both columns are
        ``large_string``: the source id is the source's name and the record id the
        row identifier, the type each keeps everywhere: through a ``Tag``, through
        every operator and in every stored table (ITL-1154).
    """
    source_id_col = f"{constants.SYSTEM_TAG_SOURCE_ID_PREFIX}{constants.BLOCK_SEPARATOR}{schema_hash}"
    record_id_col = f"{constants.SYSTEM_TAG_RECORD_ID_PREFIX}{constants.BLOCK_SEPARATOR}{schema_hash}"
    return source_id_col, record_id_col


def add_system_tag_columns(
    table: "pa.Table",
    schema_hash: str,
    source_ids: str | Collection[str],
    record_ids: Collection[str],
) -> "pa.Table":
    """Add paired source_id and record_id system tag columns to an Arrow table.

    Both are text (``large_string``): the source id is the source's name and the record
    id is the row identifier, ``row_<n>`` or the key's canonical JSON (ITL-1154).
    """
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

    source_id_col_name, record_id_col_name = system_tag_column_names(schema_hash)

    source_id_array = pa.array(source_ids, type=pa.large_string())
    record_id_array = pa.array(record_ids, type=pa.large_string())

    # System tag columns are always computed, never null — declare nullable=False
    # explicitly so the schema intent is not lost in Polars round-trips.
    table = table.append_column(
        pa.field(source_id_col_name, pa.large_string(), nullable=False), source_id_array
    )
    table = table.append_column(
        pa.field(record_id_col_name, pa.large_string(), nullable=False),
        record_id_array,
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


def fold_system_tag_values(values: Sequence[Any], *, label: str) -> str:
    """Fold a group's system-tag values into one scalar: ``"<label>:<hex>"``.

    The one fold helper (ITL-1154). Many->one operators must emit scalar system tags,
    because ``_build_record_id_preimage`` (``core/nodes/function_node.py``) hashes
    those columns directly to derive a record's identity. Each column folds
    independently over its own ordered member values, the source-id column and the
    record-id column alike; both are text.

    The hex is the SHA-256 of the canonical JSON array of the members in order (a null
    member is ``null``, never ``""``), so the preimage decodes back to the ordered
    member list and no two member lists share a fold (ITL-942 H). The digest is stable
    across processes: never substitute ``hash()`` or a set-based construction, since
    the result is a cache key. The label names the operation that applied the fold;
    each operation defines its own constant beside its use of this helper
    (``GROUP_BY_FOLD_LABEL`` in ``group_by.py``, ``BATCH_FOLD_LABEL`` in
    ``batch.py``), so an operation's procedure can change on its own.

    Member order is significant: it matches the order of the list-valued data columns
    the folded tag accompanies.

    Args:
        values: The group's member values, in member order.
        label: The operation's fold label.

    Returns:
        ``"<label>:<64 hex digits>"``.
    """
    members = json.dumps(
        [None if v is None else str(v) for v in values],
        separators=(",", ":"),
        ensure_ascii=False,
    )
    # A folded value is a hash stored as a value: it carries its label.
    return f"{label}:{hashlib.sha256(members.encode('utf-8')).hexdigest()}"


def aggregated_list_type(
    element_type: pa.DataType,
    type_converter: TypeConverterProtocol,
    *,
    column: str,
) -> pa.DataType:
    """Return the list type for a column that aggregates ``element_type`` values.

    This is the one construction every aggregating operator (``Batch``,
    ``GroupBy``, ``MergeJoin``) uses for its ``list[T]`` columns. Nothing is
    inferred from Python values: the element is the input column's Arrow type
    normalized to its large variants (``normalize_to_large_types``: ``string``
    becomes ``large_string``, ``list`` becomes ``large_list``, recursively,
    which the Arrow hasher treats as identical), keeping integer and float
    width, struct fields and nullability (``int8`` stays ``int8``). The outer
    list is ``large_list``, the variant orcapod's type converter gives
    ``list[T]``.

    Arrow cannot embed an extension type inside a list value field, so for a
    logical (extension) element the result is the logical list type of that
    element: the ``list[<element>]`` extension type supplied by
    ``ListLogicalType`` through the type converter, which keeps the inner
    logical type. The element is never stripped to its storage type.

    Args:
        element_type: Arrow type of the input column being aggregated.
        type_converter: The stream's type converter, used to resolve a logical
            element's Python type and its logical list type.
        column: Name of the column being aggregated, for the error message.

    Returns:
        ``pa.large_list(normalize_to_large_types(element_type))``, or the
        ``list[...]`` extension type for a logical element.

    Raises:
        ValueError: If ``element_type`` is an extension type for which the type
            converter supplies no logical list type.
    """
    # BaseExtensionType also covers PyArrow's canonical extensions (arrow.uuid,
    # arrow.json, ...), which are not ``pa.ExtensionType`` subclasses.
    if isinstance(element_type, pa.BaseExtensionType):
        element_python_type = type_converter.arrow_type_to_python_type(element_type)
        list_type = type_converter.python_type_to_arrow_type(list[element_python_type])
        if not isinstance(list_type, pa.BaseExtensionType):
            raise ValueError(
                f"Column {column!r} has logical Arrow type {element_type} "
                f"(extension {element_type.extension_name!r}), and the type "
                f"converter supplies no logical list type for it (it returned "
                f"{list_type} for list[{element_python_type}]). Register the "
                f"logical type with the data context's type converter."
            )
        return list_type
    return pa.large_list(normalize_to_large_types(element_type, keep_null=True))


def list_array(values: Sequence[Any], list_type: pa.DataType) -> pa.Array[Any]:
    """Build a list array of exactly ``list_type`` from Python values.

    The type never depends on ``values``, so an empty array has the same type
    as a non-empty one. An extension ``list_type`` is built over its storage
    type and then wrapped.

    Args:
        values: One Python list per row, holding storage values for an
            extension element.
        list_type: The target type, normally from ``aggregated_list_type``.

    Returns:
        The list array.
    """
    if isinstance(list_type, pa.BaseExtensionType):
        storage = pa.array(values, type=list_type.storage_type)
        return pa.ExtensionArray.from_storage(list_type, storage)
    return pa.array(values, type=list_type)


def restore_column_types(
    table: pa.Table, reference_types: Mapping[str, pa.DataType]
) -> pa.Table:
    """Give columns back the Arrow types they had before a Polars round-trip.

    Polars stores strings, binaries and lists in their large layouts and has
    no ``fixed_size_binary``, so ``string`` comes back as ``large_string``,
    ``list`` as ``large_list`` and so on. This casts every column named in
    ``reference_types`` back to its reference type. Nullability and columns
    not in ``reference_types`` are left exactly as they are; use
    ``restore_schema_nullability`` to restore both.

    Args:
        table: The table produced by the round-trip.
        reference_types: Column name to the Arrow type to restore.

    Returns:
        The table cast to the restored types.
    """
    fields = [
        field.with_type(reference_types[field.name])
        if field.name in reference_types
        else field
        for field in table.schema
    ]
    restored = table.cast(pa.schema(fields))
    return restored.replace_schema_metadata(table.schema.metadata)


def build_aggregated_table(
    rows: "Sequence[Mapping[str, Any]]",
    input_schema: "pa.Schema",
    member_columns: "Collection[str]",
    type_converter: Any,
) -> "pa.Table":
    """Build a many->one operator's output table, list-wrapping member columns.

    Columns named in ``member_columns`` become list-valued, one element per
    group member; every other column keeps its input field unchanged (that is
    how scalar group keys and folded system tags pass through).

    Logical element types are preserved. Arrow cannot embed an extension type
    inside a list value field, so ``pa.list_(extension_type)`` raises
    ``ArrowNotImplementedError`` (see ``design_issues/`` ET1/ET2). For such a
    column the list is built over the element's *storage* type and then wrapped
    in the outer ``list[<element>]`` extension type supplied by
    ``ListLogicalType``. A pod annotated ``-> Path`` therefore groups into
    ``list[orcapod.path]`` rather than losing the type or failing.

    Args:
        rows: One mapping per output row, values already aggregated into lists
            for every member column.
        input_schema: Schema of the operator's input table, used to derive
            element types and to pass non-member fields through unchanged.
        member_columns: Names of the columns to list-wrap.
        type_converter: The stream's type converter, used to resolve an
            extension type's Python element type and the matching outer list
            extension type.

    Returns:
        The aggregated ``pa.Table``.
    """
    member_columns = set(member_columns)
    fields: list[pa.Field] = []
    # Extension columns are built as plain storage lists, then re-wrapped.
    extension_overrides: dict[str, Any] = {}

    for field in input_schema:
        if field.name not in member_columns:
            fields.append(field)
            continue
        list_type = aggregated_list_type(field.type, type_converter, column=field.name)
        if isinstance(list_type, pa.BaseExtensionType):
            extension_overrides[field.name] = list_type
            fields.append(pa.field(field.name, list_type.storage_type, nullable=False))
        else:
            fields.append(pa.field(field.name, list_type, nullable=False))

    table = pa.Table.from_pylist(list(rows), schema=pa.schema(fields))

    for name, list_type in extension_overrides.items():
        index = table.schema.get_field_index(name)
        storage = table.column(name)
        wrapped = pa.chunked_array(
            [
                pa.ExtensionArray.from_storage(list_type, chunk)
                for chunk in storage.chunks
            ],
            type=list_type,
        )
        table = table.set_column(
            index, pa.field(name, list_type, nullable=False), wrapped
        )

    return table


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
            pa.field(
                f"{constants.SOURCE_PREFIX}{col}", pa.large_string(), nullable=False
            ),
            col_source,
        )

    return table


def apply_column_config(
    table: "pa.Table",
    column_config: ColumnConfig,
    tag_keys: tuple[str, ...],
) -> "pa.Table":
    """Apply ``ColumnConfig`` column filtering and optional tag-sort to a table.

    Data columns are derived automatically from the table's column names by
    excluding the known tag columns and all system-managed column groups
    (system tags, source-info, context, meta).  This means callers only need to
    supply ``tag_keys``; there is no need to thread ``data_keys`` through the
    call site.

    Args:
        table: A fully-materialized PyArrow table (all columns present).
        column_config: Resolved column configuration.
        tag_keys: Names of the regular (non-system) tag columns.

    Returns:
        A new table with the appropriate columns dropped and optionally
        sorted by tag columns.
    """
    tag_key_set = set(tag_keys)
    # Derive data column names: anything that is not a tag, not a system-prefix
    # column, and not the context key.
    data_keys = tuple(
        c
        for c in table.column_names
        if c not in tag_key_set
        and not c.startswith(constants.SYSTEM_TAG_PREFIX)
        and not c.startswith(constants.META_PREFIX)
        and not c.startswith(constants.SOURCE_PREFIX)
        and c != constants.CONTEXT_KEY
    )
    drop_columns = []
    if not column_config.system_tags:
        drop_columns.extend(
            c for c in table.column_names if c.startswith(constants.SYSTEM_TAG_PREFIX)
        )
    if not column_config.source:
        drop_columns.extend(f"{constants.SOURCE_PREFIX}{c}" for c in data_keys)
    if not column_config.context:
        drop_columns.append(constants.CONTEXT_KEY)
    if not column_config.meta:
        drop_columns.extend(
            c for c in table.column_names if c.startswith(constants.META_PREFIX)
        )
    elif not isinstance(column_config.meta, bool):
        # Normalize: prepend META_PREFIX to any user-supplied prefix that doesn't
        # already carry it.  This matches ColumnConfig's documented behaviour that
        # the '__' prefix is added automatically when not present.
        normalized_meta_prefixes = [
            p if p.startswith(constants.META_PREFIX) else f"{constants.META_PREFIX}{p}"
            for p in column_config.meta
        ]
        drop_columns.extend(
            c
            for c in table.column_names
            if c.startswith(constants.META_PREFIX)
            and not any(c.startswith(p) for p in normalized_meta_prefixes)
        )
    if column_config.sort_by_tags and tag_keys:
        # Before the system tags are dropped: they break ties between equal tags.
        table = sort_by_tag_columns(table, tag_keys)
    return table.drop([c for c in drop_columns if c in table.column_names])


def unused_column_name(table: "pa.Table", base: str) -> str:
    """Return ``base``, with underscores added until no column of ``table`` has that name."""
    name = base
    while name in table.column_names:
        name += "_"
    return name


def with_content_hash_column(
    table: "pa.Table",
    content_hash: bool | str,
    hashes: "Callable[[], pa.Array[Any]]",
    shape: "Callable[[pa.Table], pa.Table]",
    nullable: bool = True,
) -> "pa.Table":
    """Return ``shape(table)`` with the requested content-hash column attached.

    The ONE mechanism by which a requested content-hash column is added while rows are
    sorted or columns are filtered (ITL-1029, ITL-1053). The hash column is attached to
    ``table`` before ``shape`` runs, so a sort moves every hash with its own row, and it
    travels under a reserved temporary name that belongs to no column group, so that
    neither the sort (a name starting with the system tag prefix is not a tie-break key)
    nor the filtering (a name equal to ``_context_key`` or ``_source_y``) can mistake or
    drop it. It gets its requested name last.

    Args:
        table: The rows, with every column ``shape`` needs.
        content_hash: The column configuration's ``content_hash``: ``False`` for none,
            ``True`` for ``_content_hash``, or the name of the column.
        hashes: Returns the content hash of each row of ``table``, in its order, as a
            ``large_string`` array. Called only when a content hash is requested.
        shape: Sorts and filters the table (it must keep every column it does not drop
            by its own rules, and must not read the hash column).
        nullable: Whether the hash column's field is nullable.

    Returns:
        The shaped table, the content-hash column last under its requested name.
    """
    if not content_hash:
        return shape(table)
    name = content_hash if isinstance(content_hash, str) else "_content_hash"
    carrier = unused_column_name(table, "row content hash")
    carried = table.append_column(
        pa.field(carrier, pa.large_string(), nullable=nullable),
        hashes(),
    )
    shaped = shape(carried)
    return shaped.rename_columns(
        [name if column == carrier else column for column in shaped.column_names]
    )


def sort_by_tag_columns(table: "pa.Table", tag_keys: tuple[str, ...]) -> "pa.Table":
    """Return ``table`` with its rows in tag order: the sort every presentation uses.

    This is the order of ``as_table(sort_by_tags=True)``, ``head`` and the notebook
    preview. It is not a hash input: a stream's identity keeps its own ordering
    (``ArrowTableStream._identity_table``).

    The rows are ordered ascending by the tag columns ``tag_keys``, nulls last. Rows with
    equal tags are then ordered by the system tag columns ``table`` has (they hold the
    record ids), in column order, and rows still equal keep their input order, so the
    result does not depend on the order the rows arrived in whenever the table has system
    tags. A table with neither is returned as it is. A caller that must keep the order of
    a stream with no regular tag column does not call this for it. A tag type Arrow cannot
    order (a list) is ordered with Polars, nulls last.

    Whole rows move together: a column added to ``table`` before the sort (a content hash)
    stays with its row. Column types and nullability are those of ``table``. Sort before
    dropping the system tag columns, or the tiebreak is lost.

    Args:
        table: The table to sort.
        tag_keys: Names of the regular (non-system) tag columns, in sort priority.

    Returns:
        A new table with the same columns, rows in tag order.
    """
    system_tags = (
        c for c in table.column_names if c.startswith(constants.SYSTEM_TAG_PREFIX)
    )
    keys = list(dict.fromkeys([*tag_keys, *system_tags]))
    if not keys:
        return table
    try:
        return table.sort_by([(key, "ascending") for key in keys])
    except (pa.ArrowNotImplementedError, pa.ArrowTypeError):
        import polars as pl

        sorted_table = (
            pl.DataFrame(table)
            .sort(by=keys, descending=False, nulls_last=True, maintain_order=True)
            .to_arrow()
        )
        return restore_schema_nullability(sorted_table, table.schema)


if __name__ == "__main__":
    test_schema_normalization()
