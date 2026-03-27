from __future__ import annotations

import base64
import hashlib
import json
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")


def serialize_pyarrow_table_schema(table: "pa.Table") -> str:
    """
    Serialize PyArrow table schema to JSON with Python type names and filtered metadata.

    Args:
        table: PyArrow table

    Returns:
        JSON string representation of schema
    """
    schema_info = []

    for field in table.schema:
        field_info = {
            "name": field.name,
            "type": _arrow_type_to_python_type(field.type),
            "metadata": _extract_semantic_metadata(field.metadata),
        }
        schema_info.append(field_info)

    return json.dumps(schema_info, separators=(",", ":"), sort_keys=True)


def serialize_pyarrow_table(table: "pa.Table") -> str:
    """
    Serialize a PyArrow table to a stable JSON string with both schema and data.

    Args:
        table: PyArrow table to serialize

    Returns:
        JSON string representation with schema and data sections
    """
    # Convert table to dictionary of lists using to_pylist()
    data_dict = {}

    for column_name in table.column_names:
        column = table.column(column_name)
        # Convert Arrow column to Python list, which visits all elements
        column_values = column.to_pylist()

        # Handle special types that need encoding for JSON
        data_dict[column_name] = [
            _serialize_value_for_json(val) for val in column_values
        ]

    # Serialize schema
    schema_info = []
    for field in table.schema:
        field_info = {
            "name": field.name,
            "type": _arrow_type_to_python_type(field.type),
            "metadata": _extract_semantic_metadata(field.metadata),
        }
        schema_info.append(field_info)

    # Combine schema and data
    serialized_table = {"schema": schema_info, "data": data_dict}

    # Serialize to JSON with sorted keys and no whitespace
    return json.dumps(
        serialized_table,
        separators=(",", ":"),
        sort_keys=True,
        default=_json_serializer,
    )


def get_pyarrow_table_hash(table: "pa.Table") -> str:
    """
    Get a stable SHA-256 hash of the table content.

    Args:
        table: PyArrow table

    Returns:
        SHA-256 hash of the serialized table
    """
    serialized = serialize_pyarrow_table(table)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def deserialize_to_pyarrow_table(serialized_str: str) -> "pa.Table":
    """
    Deserialize JSON string back to a PyArrow table.

    Args:
        serialized_str: JSON string from serialize_pyarrow_table

    Returns:
        Reconstructed PyArrow table
    """
    parsed_data = json.loads(serialized_str)

    # Handle both old format (dict of lists) and new format (schema + data)
    if "data" in parsed_data and "schema" in parsed_data:
        # New format with schema and data
        data_dict = parsed_data["data"]
        schema_info = parsed_data["schema"]
    else:
        # Old format - just data dict
        data_dict = parsed_data
        schema_info = None

    if not data_dict:
        return pa.table([])

    # Deserialize each column
    arrays = []
    names = []

    for column_name in sorted(data_dict.keys()):  # Sort for consistency
        column_values = [_deserialize_value(val) for val in data_dict[column_name]]
        arrays.append(pa.array(column_values))
        names.append(column_name)

    return pa.table(arrays, names=names)


def _arrow_type_to_python_type(arrow_type: pa.DataType) -> str:
    """
    Convert PyArrow data type to standard Python type name.

    Args:
        arrow_type: PyArrow data type

    Returns:
        Python type name as string
    """
    if pa.types.is_boolean(arrow_type):
        return "bool"
    elif pa.types.is_integer(arrow_type):
        return "int"
    elif pa.types.is_floating(arrow_type):
        return "float"
    elif pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
        return "str"
    elif pa.types.is_binary(arrow_type) or pa.types.is_large_binary(arrow_type):
        return "bytes"
    elif pa.types.is_date(arrow_type):
        return "date"
    elif pa.types.is_timestamp(arrow_type):
        return "datetime"
    elif pa.types.is_time(arrow_type):
        return "time"
    elif pa.types.is_decimal(arrow_type):
        return "decimal"
    elif pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
        element_type = _arrow_type_to_python_type(arrow_type.value_type)
        return f"list[{element_type}]"
    elif pa.types.is_struct(arrow_type):
        return "dict"
    elif pa.types.is_dictionary(arrow_type):
        value_type = _arrow_type_to_python_type(arrow_type.value_type)
        return value_type  # Dictionary encoding is transparent
    elif pa.types.is_null(arrow_type):
        return "NoneType"
    else:
        # Fallback for other types
        return str(arrow_type).lower()


def _extract_semantic_metadata(field_metadata) -> dict[str, str]:
    """
    Extract only 'semantic_type' metadata from field metadata.

    Args:
        field_metadata: PyArrow field metadata (can be None)

    Returns:
        Dictionary containing only semantic_type if present, empty dict otherwise
    """
    if field_metadata is None:
        return {}

    metadata_dict = dict(field_metadata)

    # Only keep semantic_type if it exists
    if "semantic_type" in metadata_dict:
        return {
            "semantic_type": metadata_dict["semantic_type"].decode("utf-8")
            if isinstance(metadata_dict["semantic_type"], bytes)
            else metadata_dict["semantic_type"]
        }
    else:
        return {}


def _serialize_value_for_json(value: Any) -> Any:
    """
    Prepare a Python value for JSON serialization.

    Args:
        value: Python value from to_pylist()

    Returns:
        JSON-serializable value
    """
    if value is None:
        return None
    elif isinstance(value, bytes):
        return {
            "__type__": "bytes",
            "__value__": base64.b64encode(value).decode("ascii"),
        }
    elif isinstance(value, Decimal):
        return {"__type__": "decimal", "__value__": str(value)}
    elif hasattr(value, "date") and hasattr(value, "time"):  # datetime objects
        return {"__type__": "datetime", "__value__": value.isoformat()}
    elif hasattr(value, "isoformat") and not hasattr(
        value, "time"
    ):  # date objects (no time component)
        return {"__type__": "date", "__value__": value.isoformat()}
    elif isinstance(value, (list, tuple)):
        return [_serialize_value_for_json(item) for item in value]
    elif isinstance(value, dict):
        return {k: _serialize_value_for_json(v) for k, v in sorted(value.items())}
    else:
        return value


def _deserialize_value(value: Any) -> Any:
    """
    Deserialize a value from the JSON representation.

    Args:
        value: Value from JSON

    Returns:
        Python value suitable for PyArrow
    """
    if value is None:
        return None
    elif isinstance(value, dict) and "__type__" in value:
        type_name = value["__type__"]
        val = value["__value__"]

        if type_name == "bytes":
            return base64.b64decode(val.encode("ascii"))
        elif type_name == "decimal":
            return Decimal(val)
        elif type_name == "datetime":
            from datetime import datetime

            return datetime.fromisoformat(val)
        elif type_name == "date":
            from datetime import date

            return date.fromisoformat(val)
        else:
            return val
    elif isinstance(value, list):
        return [_deserialize_value(item) for item in value]
    elif isinstance(value, dict):
        return {k: _deserialize_value(v) for k, v in value.items()}
    else:
        return value


def _json_serializer(obj):
    """Custom JSON serializer for edge cases."""
    if hasattr(obj, "date") and hasattr(obj, "time"):  # datetime objects
        return {"__type__": "datetime", "__value__": obj.isoformat()}
    elif hasattr(obj, "isoformat") and not hasattr(obj, "time"):  # date objects
        return {"__type__": "date", "__value__": obj.isoformat()}
    elif isinstance(obj, bytes):
        return {"__type__": "bytes", "__value__": base64.b64encode(obj).decode("ascii")}
    elif isinstance(obj, Decimal):
        return {"__type__": "decimal", "__value__": str(obj)}
    else:
        return str(obj)  # Fallback to string representation


# Example usage and testing
if __name__ == "__main__":
    import datetime

    # Create a sample PyArrow table with various types
    data = {
        "integers": [1, 2, 3, 4, 5],
        "floats": [1.1, 2.2, 3.3, 4.4, 5.5],
        "strings": ["a", "b", "c", "d", "e"],
        "booleans": [True, False, True, False, True],
        "nulls": [1, None, 3, None, 5],
        "dates": [
            datetime.date(2023, 1, 1),
            datetime.date(2023, 1, 2),
            None,
            datetime.date(2023, 1, 4),
            datetime.date(2023, 1, 5),
        ],
    }

    table = pa.table(data)
    print("Original table:")
    print(table)
    print()

    # Serialize the table
    serialized = serialize_pyarrow_table(table)
    print("Serialized JSON (first 200 chars):")
    print(serialized[:200] + "..." if len(serialized) > 200 else serialized)
    print()

    # Get hash
    table_hash = get_pyarrow_table_hash(table)
    print(f"Table hash: {table_hash}")
    print()

    # Test stability
    serialized2 = serialize_pyarrow_table(table)
    hash2 = get_pyarrow_table_hash(table)

    print(f"Serialization is stable: {serialized == serialized2}")
    print(f"Hash is stable: {table_hash == hash2}")
    print()

    # Test with different column order
    print("--- Testing column order stability ---")
    data_reordered = {
        "strings": ["a", "b", "c", "d", "e"],
        "integers": [1, 2, 3, 4, 5],
        "nulls": [1, None, 3, None, 5],
        "floats": [1.1, 2.2, 3.3, 4.4, 5.5],
        "booleans": [True, False, True, False, True],
        "dates": [
            datetime.date(2023, 1, 1),
            datetime.date(2023, 1, 2),
            None,
            datetime.date(2023, 1, 4),
            datetime.date(2023, 1, 5),
        ],
    }

    table_reordered = pa.table(data_reordered)
    serialized_reordered = serialize_pyarrow_table(table_reordered)
    hash_reordered = get_pyarrow_table_hash(table_reordered)

    print(
        f"Same content, different column order produces same serialization: {serialized == serialized_reordered}"
    )
    print(
        f"Same content, different column order produces same hash: {table_hash == hash_reordered}"
    )
    print()

    # Test schema serialization
    print("\n--- Testing schema serialization ---")

    # Create table with metadata
    schema = pa.schema(
        [
            pa.field(
                "integers",
                pa.int64(),
                metadata={"semantic_type": "id", "other_meta": "ignored"},
            ),
            pa.field("floats", pa.float64(), metadata={"semantic_type": "measurement"}),
            pa.field("strings", pa.string()),  # No metadata
            pa.field(
                "booleans", pa.bool_(), metadata={"other_meta": "ignored"}
            ),  # No semantic_type
            pa.field("dates", pa.date32(), metadata={"semantic_type": "event_date"}),
        ]
    )

    table_with_schema = pa.table(data, schema=schema)
    schema_json = serialize_pyarrow_table_schema(table_with_schema)
    print(f"Schema JSON: {schema_json}")

    # Parse and display nicely
    import json as json_module

    schema_parsed = json_module.loads(schema_json)
    print("\nParsed schema:")
    for field in schema_parsed:
        print(f"  {field['name']}: {field['type']} (metadata: {field['metadata']})")

    # Test deserialization
    reconstructed = deserialize_to_pyarrow_table(serialized)
    print("Reconstructed table:")
    print(reconstructed)
    print()

    # Verify round-trip
    reconstructed_hash = get_pyarrow_table_hash(reconstructed)
    print(f"Round-trip hash matches: {table_hash == reconstructed_hash}")

    # Show actual JSON structure for small example
    print("\n--- Small example JSON structure ---")
    small_table = pa.table(
        {"numbers": [1, 2, None], "text": ["hello", "world", "test"]}
    )
    small_json = serialize_pyarrow_table(small_table)
    print(f"Small table JSON: {small_json}")
