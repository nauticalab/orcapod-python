import pyarrow as pa
import pyarrow.compute as pc
from io import BytesIO
import struct
from typing import Any
import hashlib


def bool_sequence_to_byte(sequence: list[bool]) -> bytes:
    """Convert a sequence of booleans to a byte array."""
    if len(sequence) > 8:
        raise ValueError("Sequence length exceeds 8 bits, cannot fit in a byte.")
    mask = 1
    flags = 0
    for value in sequence:
        if value:
            flags |= mask
        mask <<= 1
    return struct.pack("<B", flags)


class OrderOptions:
    """Configuration for order-independent serialization."""

    def __init__(
        self, ignore_column_order: bool = True, ignore_row_order: bool = False
    ):
        self.ignore_column_order = ignore_column_order
        self.ignore_row_order = ignore_row_order

    def to_bytes(self) -> bytes:
        """Serialize order options to bytes for inclusion in format."""
        flags = 0
        if self.ignore_column_order:
            flags |= 1
        if self.ignore_row_order:
            flags |= 2
        return struct.pack("<B", flags)

    @classmethod
    def from_bytes(cls, data: bytes) -> "OrderOptions":
        """Deserialize order options from bytes."""
        flags = struct.unpack("<B", data)[0]
        return cls(
            ignore_column_order=bool(flags & 1), ignore_row_order=bool(flags & 2)
        )

    def __str__(self):
        return f"OrderOptions(ignore_column_order={self.ignore_column_order}, ignore_row_order={self.ignore_row_order})"


def _convert_array_to_string_for_sorting(array: pa.Array) -> pa.Array:
    """
    Convert any Arrow array to string representation for sorting purposes.
    Handles all data types including complex ones.
    """
    if pa.types.is_string(array.type) or pa.types.is_large_string(array.type):
        # Already string
        return array

    elif pa.types.is_binary(array.type) or pa.types.is_large_binary(array.type):
        # Convert binary to base64 string representation for deterministic sorting
        try:
            # Use Arrow's base64 encoding if available
            import base64

            str_values = []
            # Get null mask
            null_mask = pc.is_null(array)  # type: ignore
            for i in range(len(array)):
                if null_mask[i].as_py():
                    str_values.append(None)  # Will be handled by fill_null later
                else:
                    binary_val = array[i].as_py()
                    if binary_val is not None:
                        str_values.append(base64.b64encode(binary_val).decode("ascii"))
                    else:
                        str_values.append(None)
            return pa.array(str_values, type=pa.string())
        except Exception:
            # Fallback: convert to hex string
            str_values = []
            try:
                null_mask = pc.is_null(array)  # type: ignore
                for i in range(len(array)):
                    if null_mask[i].as_py():
                        str_values.append(None)
                    else:
                        try:
                            binary_val = array[i].as_py()
                            if binary_val is not None:
                                str_values.append(binary_val.hex())
                            else:
                                str_values.append(None)
                        except Exception:
                            str_values.append(f"BINARY_{i}")
            except Exception:
                # If null checking fails, just convert all values
                for i in range(len(array)):
                    try:
                        binary_val = array[i].as_py()
                        if binary_val is not None:
                            str_values.append(binary_val.hex())
                        else:
                            str_values.append(None)
                    except Exception:
                        str_values.append(f"BINARY_{i}")
            return pa.array(str_values, type=pa.string())

    elif _is_primitive_type(array.type):
        # Convert primitive types to string
        try:
            return pc.cast(array, pa.string())
        except Exception:
            # Manual conversion for types that don't cast well
            str_values = []
            try:
                null_mask = pc.is_null(array)  # type: ignore
                for i in range(len(array)):
                    if null_mask[i].as_py():
                        str_values.append(None)
                    else:
                        try:
                            value = array[i].as_py()
                            str_values.append(str(value))
                        except Exception:
                            str_values.append(f"PRIMITIVE_{i}")
            except Exception:
                # If null checking fails, just convert all values
                for i in range(len(array)):
                    try:
                        value = array[i].as_py()
                        if value is not None:
                            str_values.append(str(value))
                        else:
                            str_values.append(None)
                    except Exception:
                        str_values.append(f"PRIMITIVE_{i}")
            return pa.array(str_values, type=pa.string())

    elif pa.types.is_list(array.type) or pa.types.is_large_list(array.type):
        # Convert list to string representation
        str_values = []
        try:
            null_mask = pc.is_null(array)  # type: ignore
            for i in range(len(array)):
                if null_mask[i].as_py():
                    str_values.append(None)
                else:
                    try:
                        value = array[i].as_py()
                        # Sort list elements for consistent representation
                        if value is not None:
                            sorted_value = sorted(
                                value, key=lambda x: (x is None, str(x))
                            )
                            str_values.append(str(sorted_value))
                        else:
                            str_values.append(None)
                    except Exception:
                        str_values.append(f"LIST_{i}")
        except Exception:
            # If null checking fails, just convert all values
            for i in range(len(array)):
                try:
                    value = array[i].as_py()
                    if value is not None:
                        sorted_value = sorted(value, key=lambda x: (x is None, str(x)))
                        str_values.append(str(sorted_value))
                    else:
                        str_values.append(None)
                except Exception:
                    str_values.append(f"LIST_{i}")
        return pa.array(str_values, type=pa.string())

    elif pa.types.is_struct(array.type):
        # Convert struct to string representation
        str_values = []
        try:
            null_mask = pc.is_null(array)  # type: ignore
            for i in range(len(array)):
                if null_mask[i].as_py():
                    str_values.append(None)
                else:
                    try:
                        value = array[i].as_py()
                        if value is not None:
                            # Sort dict keys for consistent representation
                            if isinstance(value, dict):
                                sorted_items = sorted(
                                    value.items(), key=lambda x: str(x[0])
                                )
                                str_values.append(str(dict(sorted_items)))
                            else:
                                str_values.append(str(value))
                        else:
                            str_values.append(None)
                    except Exception:
                        str_values.append(f"STRUCT_{i}")
        except Exception:
            # If null checking fails, just convert all values
            for i in range(len(array)):
                try:
                    value = array[i].as_py()
                    if value is not None:
                        if isinstance(value, dict):
                            sorted_items = sorted(
                                value.items(), key=lambda x: str(x[0])
                            )
                            str_values.append(str(dict(sorted_items)))
                        else:
                            str_values.append(str(value))
                    else:
                        str_values.append(None)
                except Exception:
                    str_values.append(f"STRUCT_{i}")
        return pa.array(str_values, type=pa.string())

    elif pa.types.is_dictionary(array.type):
        # Convert dictionary to string representation using the decoded values
        str_values = []
        try:
            null_mask = pc.is_null(array)  # type: ignore
            for i in range(len(array)):
                if null_mask[i].as_py():
                    str_values.append(None)
                else:
                    try:
                        value = array[i].as_py()
                        str_values.append(str(value))
                    except Exception:
                        str_values.append(f"DICT_{i}")
        except Exception:
            # If null checking fails, just convert all values
            for i in range(len(array)):
                try:
                    value = array[i].as_py()
                    if value is not None:
                        str_values.append(str(value))
                    else:
                        str_values.append(None)
                except Exception:
                    str_values.append(f"DICT_{i}")
        return pa.array(str_values, type=pa.string())

    else:
        # Generic fallback for any other types
        try:
            return pc.cast(array, pa.string())
        except Exception:
            # Manual conversion as last resort
            str_values = []
            try:
                null_mask = pc.is_null(array)  # type: ignore
                for i in range(len(array)):
                    if null_mask[i].as_py():
                        str_values.append(None)
                    else:
                        try:
                            value = array[i].as_py()
                            str_values.append(str(value))
                        except Exception:
                            str_values.append(f"UNKNOWN_{array.type}_{i}")
            except Exception:
                # If null checking fails, just convert all values
                for i in range(len(array)):
                    try:
                        value = array[i].as_py()
                        if value is not None:
                            str_values.append(str(value))
                        else:
                            str_values.append(None)
                    except Exception:
                        str_values.append(f"UNKNOWN_{array.type}_{i}")
            return pa.array(str_values, type=pa.string())


def _create_row_sort_key(table: pa.Table) -> pa.Array:
    """
    Create a deterministic sort key for rows by combining all column values.
    This ensures consistent row ordering regardless of input order.
    """
    if table.num_rows == 0:
        return pa.array([], type=pa.string())

    # Convert each column to string representation for sorting
    sort_components = []

    for i in range(table.num_columns):
        column = table.column(i)
        field = table.schema.field(i)

        # Combine all chunks into a single array
        if column.num_chunks > 1:
            combined_array = pa.concat_arrays(column.chunks)
        elif column.num_chunks == 1:
            combined_array = column.chunk(0)
        else:
            combined_array = pa.array([], type=field.type)

        # Convert to string representation for sorting
        str_array = _convert_array_to_string_for_sorting(combined_array)

        # Handle nulls by replacing with a consistent null representation
        str_array = pc.fill_null(str_array, "NULL")
        sort_components.append(str_array)

    # Combine all columns into a single sort key
    if len(sort_components) == 1:
        return sort_components[0]
    else:
        # Concatenate all string representations with separators
        separator = pa.scalar("||")
        combined = sort_components[0]
        for component in sort_components[1:]:
            combined = pc.binary_join_element_wise(combined, separator, component)  # type: ignore
        return combined


def _sort_table_by_content(table: pa.Table) -> pa.Table:
    """Sort table rows based on content for deterministic ordering."""
    if table.num_rows <= 1:
        return table

    # Create sort key
    sort_key = _create_row_sort_key(table)

    # Get sort indices
    sort_indices = pc.sort_indices(sort_key)  # type: ignore

    # Apply sort to table
    return pc.take(table, sort_indices)


def _sort_table_columns_by_name(table: pa.Table) -> pa.Table:
    """Sort table columns alphabetically by name for deterministic ordering."""
    if table.num_columns <= 1:
        return table

    # Get column names and sort them
    column_names = [field.name for field in table.schema]
    sorted_names = sorted(column_names)

    # If already sorted, return as-is
    if column_names == sorted_names:
        return table

    # Reorder columns
    return table.select(sorted_names)


def serialize_table_logical(
    table: pa.Table, order_options: OrderOptions | None = None
) -> bytes:
    """
    Serialize table using column-wise processing with direct binary data access.

    This implementation works directly with Arrow's underlying binary buffers
    without converting to Python objects, making it much faster and more
    memory efficient while maintaining high repeatability.

    Args:
        table: PyArrow table to serialize
        order_options: Options for handling column and row order independence
    """
    if order_options is None:
        order_options = OrderOptions()

    buffer = BytesIO()

    # Write format version
    buffer.write(b"ARROW_BINARY_V1")  # Updated version to include order options

    # Write order options
    buffer.write(order_options.to_bytes())

    # Apply ordering transformations if requested
    processed_table = table

    if order_options.ignore_column_order:
        processed_table = _sort_table_columns_by_name(processed_table)

    if order_options.ignore_row_order:
        processed_table = _sort_table_by_content(processed_table)

    # Serialize schema deterministically
    _serialize_schema_deterministic(buffer, processed_table.schema)

    # Process each column using direct binary access
    column_digests = []
    for i in range(processed_table.num_columns):
        column = processed_table.column(i)
        field = processed_table.schema.field(i)
        column_digest = _serialize_column_binary(column, field)
        column_digests.append(column_digest)

    # Combine column digests
    for digest in column_digests:
        buffer.write(digest)

    return buffer.getvalue()


def _serialize_schema_deterministic(buffer: BytesIO, schema: pa.Schema) -> None:
    """Serialize schema information deterministically."""
    buffer.write(struct.pack("<Q", len(schema)))

    for field in schema:
        # Field name as UTF-8
        name_bytes = field.name.encode("utf-8")
        buffer.write(struct.pack("<Q", len(name_bytes)))
        buffer.write(name_bytes)

        # Nullable flag
        buffer.write(struct.pack("<?", field.nullable))

        # Serialize data type deterministically
        _serialize_datatype_deterministic(buffer, field.type)


def _serialize_datatype_deterministic(buffer: BytesIO, data_type: pa.DataType) -> None:
    """Serialize Arrow data type deterministically."""
    type_id = data_type.id
    buffer.write(struct.pack("<i", type_id))

    if pa.types.is_integer(data_type):
        buffer.write(struct.pack("<i", data_type.bit_width))
        buffer.write(struct.pack("<?", pa.types.is_signed_integer(data_type)))

    elif pa.types.is_floating(data_type):
        buffer.write(struct.pack("<i", data_type.bit_width))

    elif pa.types.is_decimal(data_type):
        buffer.write(struct.pack("<i", data_type.bit_width))
        buffer.write(struct.pack("<i", data_type.precision))
        buffer.write(struct.pack("<i", data_type.scale))

    elif pa.types.is_timestamp(data_type):
        unit_map = {"s": 0, "ms": 1, "us": 2, "ns": 3}
        buffer.write(struct.pack("<i", unit_map.get(data_type.unit, 0)))
        if data_type.tz:
            tz_bytes = data_type.tz.encode("utf-8")
            buffer.write(struct.pack("<Q", len(tz_bytes)))
            buffer.write(tz_bytes)
        else:
            buffer.write(struct.pack("<Q", 0))

    elif pa.types.is_date(data_type):
        unit_map = {"day": 0, "ms": 1}
        buffer.write(struct.pack("<i", unit_map.get(data_type.unit, 0)))

    elif pa.types.is_time(data_type):
        unit_map = {"s": 0, "ms": 1, "us": 2, "ns": 3}
        buffer.write(struct.pack("<i", unit_map.get(data_type.unit, 0)))
        buffer.write(struct.pack("<i", data_type.bit_width))

    elif pa.types.is_list(data_type) or pa.types.is_large_list(data_type):
        _serialize_datatype_deterministic(buffer, data_type.value_type)

    elif pa.types.is_struct(data_type):
        buffer.write(struct.pack("<i", len(data_type)))
        for field in data_type:
            field_name_bytes = field.name.encode("utf-8")
            buffer.write(struct.pack("<Q", len(field_name_bytes)))
            buffer.write(field_name_bytes)
            _serialize_datatype_deterministic(buffer, field.type)

    elif pa.types.is_dictionary(data_type):
        _serialize_datatype_deterministic(buffer, data_type.index_type)
        _serialize_datatype_deterministic(buffer, data_type.value_type)


def _serialize_column_binary(column: pa.ChunkedArray, field: pa.Field) -> bytes:
    """
    Serialize column using direct binary buffer access.

    To ensure chunking independence, we combine chunks into a single array
    before processing. This ensures identical output regardless of chunk boundaries.
    """
    buffer = BytesIO()

    # Combine all chunks into a single array for consistent processing
    if column.num_chunks > 1:
        # Multiple chunks - combine them using pa.concat_arrays
        combined_array = pa.concat_arrays(column.chunks)
    elif column.num_chunks == 1:
        # Single chunk - use directly
        combined_array = column.chunk(0)
    else:
        # No chunks - create empty array
        combined_array = pa.array([], type=field.type)

    # Process the combined array
    chunk_result = _serialize_array_binary(combined_array, field.type)
    buffer.write(chunk_result)

    return buffer.getvalue()


def _serialize_array_binary(array: pa.Array, data_type: pa.DataType) -> bytes:
    """Serialize array using direct access to Arrow's binary buffers."""
    buffer = BytesIO()

    # Get validity buffer (null bitmap) if it exists
    validity_buffer = None
    if array.buffers()[0] is not None:
        validity_buffer = array.buffers()[0]

    # Process based on Arrow type, accessing buffers directly
    try:
        if _is_primitive_type(data_type):
            _serialize_primitive_array_binary(buffer, array, data_type, validity_buffer)

        elif pa.types.is_string(data_type) or pa.types.is_large_string(data_type):
            _serialize_string_array_binary(buffer, array, data_type, validity_buffer)

        elif pa.types.is_binary(data_type) or pa.types.is_large_binary(data_type):
            _serialize_binary_array_binary(buffer, array, data_type, validity_buffer)

        elif pa.types.is_list(data_type) or pa.types.is_large_list(data_type):
            _serialize_list_array_binary(buffer, array, data_type, validity_buffer)

        elif pa.types.is_struct(data_type):
            _serialize_struct_array_binary(buffer, array, data_type, validity_buffer)

        elif pa.types.is_dictionary(data_type):
            _serialize_dictionary_array_binary(
                buffer, array, data_type, validity_buffer
            )

        else:
            # Fallback to element-wise processing for complex types
            _serialize_array_fallback(buffer, array, data_type, validity_buffer)

    except Exception as e:
        # If binary serialization fails, fall back to element-wise processing
        print(
            f"Warning: Binary serialization failed for {data_type}, falling back to element-wise: {e}"
        )
        buffer = BytesIO()  # Reset buffer
        _serialize_array_fallback(buffer, array, data_type, validity_buffer)

    return buffer.getvalue()


def _is_primitive_type(data_type: pa.DataType) -> bool:
    """Check if type can be processed as primitive (fixed-size) data."""
    return (
        pa.types.is_integer(data_type)
        or pa.types.is_floating(data_type)
        or pa.types.is_boolean(data_type)
        or pa.types.is_date(data_type)
        or pa.types.is_time(data_type)
        or pa.types.is_timestamp(data_type)
    )


def _serialize_primitive_array_binary(
    buffer: BytesIO, array: pa.Array, data_type: pa.DataType, validity_buffer
):
    """Serialize primitive arrays by directly copying binary data."""
    # Write validity bitmap
    _serialize_validity_buffer(buffer, validity_buffer)

    # Get data buffer (buffer[1] for primitive types)
    data_buffer = array.buffers()[1]
    if data_buffer is not None:
        # For primitive types, copy the buffer directly
        if pa.types.is_boolean(data_type):
            # Boolean needs the length for bit interpretation
            buffer.write(struct.pack("<Q", len(array)))
        buffer.write(data_buffer.to_pybytes())


def _serialize_string_array_binary(
    buffer: BytesIO, array: pa.Array, data_type: pa.DataType, validity_buffer
):
    """Serialize string arrays using offset + data buffers."""
    # Write validity bitmap
    _serialize_validity_buffer(buffer, validity_buffer)

    # Get offset and data buffers
    offset_buffer = array.buffers()[1]  # int32 or int64 offsets
    data_buffer = array.buffers()[2]  # UTF-8 string data

    # Write offset buffer
    if offset_buffer is not None:
        buffer.write(offset_buffer.to_pybytes())

    # Write data buffer
    if data_buffer is not None:
        buffer.write(data_buffer.to_pybytes())


def _serialize_binary_array_binary(
    buffer: BytesIO, array: pa.Array, data_type: pa.DataType, validity_buffer
):
    """Serialize binary arrays using offset + data buffers."""
    # Same structure as string arrays
    _serialize_string_array_binary(buffer, array, data_type, validity_buffer)


def _serialize_list_array_binary(
    buffer: BytesIO, array: pa.Array, data_type: pa.DataType, validity_buffer
):
    """Serialize list arrays using offset buffer + child array."""
    # Write validity bitmap
    _serialize_validity_buffer(buffer, validity_buffer)

    # Write offset buffer
    offset_buffer = array.buffers()[1]
    if offset_buffer is not None:
        buffer.write(offset_buffer.to_pybytes())

    # Get child array - handle different access patterns
    child_array = None

    # Method 1: Try .values (most common for ListArray)
    if hasattr(array, "values") and array.values is not None:
        child_array = array.values

    # Method 2: Try .children (some array types)
    elif hasattr(array, "children") and array.children and len(array.children) > 0:
        child_array = array.children[0]

    # Method 3: Try accessing via flatten() for some list types
    elif hasattr(array, "flatten"):
        try:
            child_array = array.flatten()
        except Exception:
            pass

    # Recursively serialize child array
    if child_array is not None:
        child_data = _serialize_array_binary(child_array, data_type.value_type)
        buffer.write(child_data)
    else:
        # If we can't access child arrays directly, fall back to element-wise processing
        _serialize_array_fallback(buffer, array, data_type, validity_buffer)


def _serialize_struct_array_binary(
    buffer: BytesIO, array: pa.Array, data_type: pa.DataType, validity_buffer
):
    """Serialize struct arrays by processing child arrays."""
    # Write validity bitmap
    _serialize_validity_buffer(buffer, validity_buffer)

    # Get child arrays - handle different access patterns for StructArray
    child_arrays = []
    if hasattr(array, "field"):
        # StructArray uses .field(i) to access child arrays
        for i in range(len(data_type)):
            child_arrays.append(array.field(i))
    elif hasattr(array, "children") and array.children:
        # Some array types use .children
        child_arrays = array.children
    else:
        # Fallback: try to access fields by iterating
        try:
            for i in range(len(data_type)):
                child_arrays.append(array.field(i))
        except (AttributeError, IndexError):
            # If all else fails, use element-wise processing
            _serialize_array_fallback(buffer, array, data_type, validity_buffer)
            return

    # Serialize each child field
    for i, child_array in enumerate(child_arrays):
        field_type = data_type[i].type
        child_data = _serialize_array_binary(child_array, field_type)
        buffer.write(child_data)


def _serialize_dictionary_array_binary(
    buffer: BytesIO, array: pa.Array, data_type: pa.DataType, validity_buffer
):
    """Serialize dictionary arrays using indices + dictionary."""
    # Write validity bitmap
    _serialize_validity_buffer(buffer, validity_buffer)

    # Serialize indices array
    indices_data = _serialize_array_binary(array.indices, data_type.index_type)
    buffer.write(indices_data)

    # Serialize dictionary array
    dict_data = _serialize_array_binary(array.dictionary, data_type.value_type)
    buffer.write(dict_data)


def _serialize_validity_buffer(buffer: BytesIO, validity_buffer):
    """Serialize validity (null) bitmap."""
    if validity_buffer is not None:
        # Copy validity bitmap directly
        buffer.write(validity_buffer.to_pybytes())
    # If no validity buffer, there are no nulls (implicit)


def _serialize_array_fallback(
    buffer: BytesIO, array: pa.Array, data_type: pa.DataType, validity_buffer
):
    """Fallback to element-wise processing for complex types."""
    # Write validity bitmap
    _serialize_validity_buffer(buffer, validity_buffer)

    # Process element by element
    for i in range(len(array)):
        try:
            null_mask = pc.is_null(array)  # type: ignore
            is_null = null_mask[i].as_py()
        except:
            # Fallback null check
            try:
                value = array[i].as_py()
                is_null = value is None
            except:
                is_null = False

        if is_null:
            buffer.write(b"\x00")
        else:
            buffer.write(b"\x01")

            # For complex nested types, convert to Python and serialize
            try:
                value = array[i].as_py()
                _serialize_complex_value(buffer, value, data_type)
            except Exception:
                # If .as_py() fails, try alternative approaches
                try:
                    # For some array types, we can access scalar values differently
                    scalar = array[i]
                    if hasattr(scalar, "value"):
                        value = scalar.value
                    else:
                        value = str(scalar)  # Convert to string as last resort
                    _serialize_complex_value(buffer, value, data_type)
                except Exception:
                    # Absolute fallback - serialize type name and index
                    fallback_str = f"{data_type}[{i}]"
                    fallback_bytes = fallback_str.encode("utf-8")
                    buffer.write(struct.pack("<Q", len(fallback_bytes)))
                    buffer.write(fallback_bytes)


def _serialize_complex_value(buffer: BytesIO, value: Any, data_type: pa.DataType):
    """Serialize complex values that can't be handled by direct buffer access."""

    if pa.types.is_decimal(data_type):
        # Decimal as string for deterministic representation
        decimal_str = str(value).encode("utf-8")
        buffer.write(struct.pack("<Q", len(decimal_str)))
        buffer.write(decimal_str)

    elif pa.types.is_list(data_type) or pa.types.is_large_list(data_type):
        # List: length + elements
        if value is None:
            buffer.write(struct.pack("<Q", 0))
        else:
            buffer.write(struct.pack("<Q", len(value)))
            for item in value:
                if item is not None:
                    buffer.write(b"\x01")
                    _serialize_complex_value(buffer, item, data_type.value_type)
                else:
                    buffer.write(b"\x00")

    elif pa.types.is_struct(data_type):
        # Struct: serialize fields in order
        if value is None:
            buffer.write(struct.pack("<Q", 0))
        else:
            buffer.write(struct.pack("<Q", len(data_type)))
            # Handle both dict-like and tuple-like struct values
            if isinstance(value, dict):
                # Dict-like access
                for i, field in enumerate(data_type):
                    field_value = value.get(field.name)
                    if field_value is not None:
                        buffer.write(b"\x01")
                        _serialize_complex_value(buffer, field_value, field.type)
                    else:
                        buffer.write(b"\x00")
            else:
                # Tuple/list-like access
                for i, field in enumerate(data_type):
                    field_value = value[i] if i < len(value) else None
                    if field_value is not None:
                        buffer.write(b"\x01")
                        _serialize_complex_value(buffer, field_value, field.type)
                    else:
                        buffer.write(b"\x00")

    elif pa.types.is_dictionary(data_type):
        # Dictionary: serialize the actual decoded value
        _serialize_complex_value(buffer, value, data_type.value_type)

    elif isinstance(value, (list, tuple)):
        # Generic list/tuple handling
        buffer.write(struct.pack("<Q", len(value)))
        for item in value:
            item_str = str(item).encode("utf-8")
            buffer.write(struct.pack("<Q", len(item_str)))
            buffer.write(item_str)

    elif isinstance(value, dict):
        # Generic dict handling - sort keys for determinism
        sorted_items = sorted(value.items())
        buffer.write(struct.pack("<Q", len(sorted_items)))
        for key, val in sorted_items:
            key_str = str(key).encode("utf-8")
            val_str = str(val).encode("utf-8")
            buffer.write(struct.pack("<Q", len(key_str)))
            buffer.write(key_str)
            buffer.write(struct.pack("<Q", len(val_str)))
            buffer.write(val_str)

    else:
        # Generic fallback: convert to string representation
        str_repr = str(value).encode("utf-8")
        buffer.write(struct.pack("<Q", len(str_repr)))
        buffer.write(str_repr)


def serialize_table_logical_hash(
    table: pa.Table,
    algorithm: str = "sha256",
    order_options: OrderOptions | None = None,
) -> str:
    """Create deterministic hash using binary serialization."""
    serialized = serialize_table_logical(table, order_options)

    if algorithm == "sha256":
        hasher = hashlib.sha256()
    elif algorithm == "sha3_256":
        hasher = hashlib.sha3_256()
    elif algorithm == "blake2b":
        hasher = hashlib.blake2b()
    else:
        raise ValueError(f"Unsupported hash algorithm: {algorithm}")

    hasher.update(serialized)
    return hasher.hexdigest()


def serialize_table_logical_streaming(
    table: pa.Table, order_options: OrderOptions | None = None
) -> str:
    """
    Memory-efficient streaming version that produces the same hash as serialize_table_logical_hash.

    This version processes data in streaming fashion but maintains the same logical structure
    as the non-streaming version to ensure identical hashes and chunking independence.
    """
    if order_options is None:
        order_options = OrderOptions()

    hasher = hashlib.sha256()

    # Hash format version (same as non-streaming)
    hasher.update(b"ARROW_BINARY_V1")

    # Hash order options
    hasher.update(order_options.to_bytes())

    # Apply ordering transformations if requested
    processed_table = table

    if order_options.ignore_column_order:
        processed_table = _sort_table_columns_by_name(processed_table)

    if order_options.ignore_row_order:
        processed_table = _sort_table_by_content(processed_table)

    # Hash schema (same as non-streaming)
    schema_buffer = BytesIO()
    _serialize_schema_deterministic(schema_buffer, processed_table.schema)
    hasher.update(schema_buffer.getvalue())

    # Process each column using the same logic as non-streaming
    for i in range(processed_table.num_columns):
        column = processed_table.column(i)
        field = processed_table.schema.field(i)

        # Use the same column serialization logic for chunking independence
        column_data = _serialize_column_binary(column, field)

        # Hash the column data
        hasher.update(column_data)

    return hasher.hexdigest()


# IPC serialization for comparison (updated to include order options for fair comparison)
def serialize_table_ipc(
    table: pa.Table, order_options: OrderOptions | None = None
) -> bytes:
    """Serialize table using Arrow IPC format for comparison."""
    from io import BytesIO
    import pyarrow.ipc as ipc

    if order_options is None:
        order_options = OrderOptions()

    buffer = BytesIO()

    # Add format version for consistency with logical serialization
    buffer.write(b"ARROW_IPC_V2")

    # Add order options
    buffer.write(order_options.to_bytes())

    # Apply ordering transformations if requested
    processed_table = table

    if order_options.ignore_column_order:
        processed_table = _sort_table_columns_by_name(processed_table)

    if order_options.ignore_row_order:
        processed_table = _sort_table_by_content(processed_table)

    # Standard IPC serialization
    ipc_buffer = BytesIO()
    with ipc.new_stream(ipc_buffer, processed_table.schema) as writer:
        writer.write_table(processed_table)

    # Append IPC data
    buffer.write(ipc_buffer.getvalue())

    return buffer.getvalue()


# Test utilities (updated to test order independence)
def create_test_table_1():
    """Create a basic test table with various data types."""
    return pa.table(
        {
            "int32_col": pa.array([1, 2, None, 4, 5], type=pa.int32()),
            "float64_col": pa.array([1.1, 2.2, 3.3, None, 5.5], type=pa.float64()),
            "string_col": pa.array(["hello", "world", None, "arrow", "fast"]),
            "bool_col": pa.array([True, False, None, True, False]),
            "binary_col": pa.array([b"data1", b"data2", None, b"data4", b"data5"]),
        }
    )


def create_test_table_reordered_columns():
    """Same data as test_table_1 but with different column order."""
    return pa.table(
        {
            "string_col": pa.array(["hello", "world", None, "arrow", "fast"]),
            "bool_col": pa.array([True, False, None, True, False]),
            "int32_col": pa.array([1, 2, None, 4, 5], type=pa.int32()),
            "binary_col": pa.array([b"data1", b"data2", None, b"data4", b"data5"]),
            "float64_col": pa.array([1.1, 2.2, 3.3, None, 5.5], type=pa.float64()),
        }
    )


def create_test_table_reordered_rows():
    """Same data as test_table_1 but with different row order."""
    return pa.table(
        {
            "int32_col": pa.array([5, 4, None, 2, 1], type=pa.int32()),
            "float64_col": pa.array([5.5, None, 3.3, 2.2, 1.1], type=pa.float64()),
            "string_col": pa.array(["fast", "arrow", None, "world", "hello"]),
            "bool_col": pa.array([False, True, None, False, True]),
            "binary_col": pa.array([b"data5", b"data4", None, b"data2", b"data1"]),
        }
    )


def create_test_table_different_types():
    """Same logical data but with different Arrow types where possible."""
    return pa.table(
        {
            "int32_col": pa.array(
                [1, 2, None, 4, 5], type=pa.int64()
            ),  # int64 instead of int32
            "float64_col": pa.array(
                [1.1, 2.2, 3.3, None, 5.5], type=pa.float32()
            ),  # float32 instead of float64
            "string_col": pa.array(["hello", "world", None, "arrow", "fast"]),
            "bool_col": pa.array([True, False, None, True, False]),
            "binary_col": pa.array([b"data1", b"data2", None, b"data4", b"data5"]),
        }
    )


def create_test_table_different_chunking():
    """Same data as test_table_1 but with different chunking."""
    # Create arrays with explicit chunking
    int_chunks = [
        pa.array([1, 2], type=pa.int32()),
        pa.array([None, 4, 5], type=pa.int32()),
    ]
    float_chunks = [
        pa.array([1.1], type=pa.float64()),
        pa.array([2.2, 3.3, None, 5.5], type=pa.float64()),
    ]
    string_chunks = [pa.array(["hello", "world"]), pa.array([None, "arrow", "fast"])]
    bool_chunks = [pa.array([True, False, None]), pa.array([True, False])]
    binary_chunks = [
        pa.array([b"data1"]),
        pa.array([b"data2", None, b"data4", b"data5"]),
    ]

    return pa.table(
        {
            "int32_col": pa.chunked_array(int_chunks),
            "float64_col": pa.chunked_array(float_chunks),
            "string_col": pa.chunked_array(string_chunks),
            "bool_col": pa.chunked_array(bool_chunks),
            "binary_col": pa.chunked_array(binary_chunks),
        }
    )


def create_test_table_complex_types():
    """Create a table with complex nested types."""
    return pa.table(
        {
            "list_col": pa.array(
                [[1, 2], [3, 4, 5], None, [], [6]], type=pa.list_(pa.int32())
            ),
            "struct_col": pa.array(
                [
                    {"a": 1, "b": "x"},
                    {"a": 2, "b": "y"},
                    None,
                    {"a": 3, "b": "z"},
                    {"a": 4, "b": "w"},
                ],
                type=pa.struct([("a", pa.int32()), ("b", pa.string())]),
            ),
            "dict_col": pa.array(
                ["apple", "banana", "apple", None, "cherry"]
            ).dictionary_encode(),
        }
    )


def run_comprehensive_tests():
    """Run comprehensive test suite for serialization with order independence."""
    import time

    print("=" * 70)
    print("COMPREHENSIVE ARROW SERIALIZATION TEST SUITE (WITH ORDER OPTIONS)")
    print("=" * 70)

    # Test cases
    test_cases = [
        ("Basic table", create_test_table_1),
        ("Reordered columns", create_test_table_reordered_columns),
        ("Reordered rows", create_test_table_reordered_rows),
        ("Different types", create_test_table_different_types),
        ("Different chunking", create_test_table_different_chunking),
        ("Complex types", create_test_table_complex_types),
    ]

    # Order option combinations to test
    order_configs = [
        ("Default (order-sensitive)", OrderOptions(False, False)),
        ("Column-order independent", OrderOptions(True, False)),
        ("Row-order independent", OrderOptions(False, True)),
        ("Fully order-independent", OrderOptions(True, True)),
    ]

    print("\n1. ORDER INDEPENDENCE TESTS")
    print("-" * 50)

    base_table = create_test_table_1()
    reordered_cols = create_test_table_reordered_columns()
    reordered_rows = create_test_table_reordered_rows()

    for config_name, order_opts in order_configs:
        print(f"\n{config_name}:")
        print(f"  Config: {order_opts}")

        # Test with base table
        base_hash = serialize_table_logical_hash(base_table, order_options=order_opts)
        cols_hash = serialize_table_logical_hash(
            reordered_cols, order_options=order_opts
        )
        rows_hash = serialize_table_logical_hash(
            reordered_rows, order_options=order_opts
        )

        # Test streaming consistency
        base_stream = serialize_table_logical_streaming(
            base_table, order_options=order_opts
        )

        print(f"  Base table:        {base_hash[:12]}...")
        print(f"  Reordered columns: {cols_hash[:12]}...")
        print(f"  Reordered rows:    {rows_hash[:12]}...")
        print(f"  Streaming matches: {base_hash == base_stream}")

        # Check expected behavior
        cols_should_match = order_opts.ignore_column_order
        rows_should_match = order_opts.ignore_row_order

        cols_match = base_hash == cols_hash
        rows_match = base_hash == rows_hash

        cols_status = "✓" if cols_match == cols_should_match else "✗"
        rows_status = "✓" if rows_match == rows_should_match else "✗"

        print(
            f"  {cols_status} Column order independence: {cols_match} (expected: {cols_should_match})"
        )
        print(
            f"  {rows_status} Row order independence: {rows_match} (expected: {rows_should_match})"
        )

    print("\n2. CHUNKING INDEPENDENCE WITH ORDER OPTIONS")
    print("-" * 50)

    original = create_test_table_1()
    combined = original.combine_chunks()
    different_chunking = create_test_table_different_chunking()

    for config_name, order_opts in order_configs:
        orig_hash = serialize_table_logical_hash(original, order_options=order_opts)
        comb_hash = serialize_table_logical_hash(combined, order_options=order_opts)
        diff_hash = serialize_table_logical_hash(
            different_chunking, order_options=order_opts
        )

        chunking_independent = orig_hash == comb_hash == diff_hash
        status = "✓" if chunking_independent else "✗"

        print(
            f"{status} {config_name:25} | Chunking independent: {chunking_independent}"
        )

    print("\n3. FORMAT VERSION COMPATIBILITY")
    print("-" * 50)

    # Test that different order options produce different hashes when they should
    test_table = create_test_table_1()

    hashes = {}
    for config_name, order_opts in order_configs:
        hash_value = serialize_table_logical_hash(test_table, order_options=order_opts)
        hashes[config_name] = hash_value
        print(f"{config_name:25} | {hash_value[:16]}...")

    # Verify that order-sensitive vs order-independent produce different hashes
    default_hash = hashes["Default (order-sensitive)"]
    col_indep_hash = hashes["Column-order independent"]
    row_indep_hash = hashes["Row-order independent"]
    full_indep_hash = hashes["Fully order-independent"]

    print("\nHash uniqueness:")
    print(f"  Default != Col-independent:  {default_hash != col_indep_hash}")
    print(f"  Default != Row-independent:  {default_hash != row_indep_hash}")
    print(f"  Default != Fully independent: {default_hash != full_indep_hash}")

    print("\n4. CONTENT EQUIVALENCE TEST")
    print("-" * 50)

    # Create tables with same content but different presentation
    table_a = pa.table({"col1": pa.array([1, 2, 3]), "col2": pa.array(["a", "b", "c"])})

    table_b = pa.table(
        {
            "col2": pa.array(["a", "b", "c"]),  # Different column order
            "col1": pa.array([1, 2, 3]),
        }
    )

    table_c = pa.table(
        {
            "col1": pa.array([3, 1, 2]),  # Different row order
            "col2": pa.array(["c", "a", "b"]),
        }
    )

    table_d = pa.table(
        {
            "col2": pa.array(["c", "a", "b"]),  # Both different
            "col1": pa.array([3, 1, 2]),
        }
    )

    full_indep_opts = OrderOptions(True, True)

    hash_a = serialize_table_logical_hash(table_a, order_options=full_indep_opts)
    hash_b = serialize_table_logical_hash(table_b, order_options=full_indep_opts)
    hash_c = serialize_table_logical_hash(table_c, order_options=full_indep_opts)
    hash_d = serialize_table_logical_hash(table_d, order_options=full_indep_opts)

    all_match = hash_a == hash_b == hash_c == hash_d
    status = "✓" if all_match else "✗"

    print(f"{status} Content equivalence test:")
    print(f"  Table A (original):     {hash_a[:12]}...")
    print(f"  Table B (reord cols):   {hash_b[:12]}...")
    print(f"  Table C (reord rows):   {hash_c[:12]}...")
    print(f"  Table D (both reord):   {hash_d[:12]}...")
    print(f"  All hashes match: {all_match}")

    print("\n5. PERFORMANCE COMPARISON")
    print("-" * 50)

    # Create larger table for performance testing
    large_size = 10000
    large_table = pa.table(
        {
            "int_col": pa.array(list(range(large_size)), type=pa.int32()),
            "float_col": pa.array(
                [i * 1.5 for i in range(large_size)], type=pa.float64()
            ),
            "string_col": pa.array([f"item_{i}" for i in range(large_size)]),
            "bool_col": pa.array([i % 2 == 0 for i in range(large_size)]),
        }
    )

    # Time each method with different order options
    for config_name, order_opts in order_configs:
        times = []
        for _ in range(3):  # Run 3 times for average
            start = time.time()
            hash_result = serialize_table_logical_hash(
                large_table, order_options=order_opts
            )
            end = time.time()
            times.append(end - start)

        avg_time = sum(times) / len(times)
        throughput = (large_size * 4) / avg_time  # 4 columns

        print(
            f"{config_name:25} | {avg_time * 1000:6.1f}ms | {throughput:8.0f} values/sec"
        )

    print(f"\n{'=' * 70}")
    print("ORDER-INDEPENDENT SERIALIZATION TEST SUITE COMPLETE")
    print(f"{'=' * 70}")


# Main execution
if __name__ == "__main__":
    # Run the comprehensive test suite
    test_results = run_comprehensive_tests()
