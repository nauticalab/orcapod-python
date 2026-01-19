"""
Arrow Schema BSON Serialization

Implements the Arrow Schema Canonical Serialization Specification v2.0.0
for deterministic, cross-language schema hashing.

Requirements:
    pip install pyarrow pymongo

Usage:
    import pyarrow as pa
    from arrow_schema_bson import serialize_schema, deserialize_schema

    schema = pa.schema([
        pa.field("id", pa.int64(), nullable=False),
        pa.field("name", pa.utf8(), nullable=True),
    ])

    bson_bytes = serialize_schema(schema)
    reconstructed = deserialize_schema(bson_bytes)
"""

from collections import OrderedDict
from typing import Any

import bson
import pyarrow as pa


def sort_keys_recursive(obj: Any) -> Any:
    """Recursively sort all dictionary keys alphabetically."""
    if isinstance(obj, dict):
        return OrderedDict((k, sort_keys_recursive(v)) for k, v in sorted(obj.items()))
    elif isinstance(obj, list):
        return [sort_keys_recursive(x) for x in obj]
    return obj


def serialize_type(arrow_type: pa.DataType) -> dict[str, Any]:
    """Convert an Arrow DataType to a canonical type descriptor."""

    # Null
    if pa.types.is_null(arrow_type):
        return {"name": "null"}

    # Boolean
    if pa.types.is_boolean(arrow_type):
        return {"name": "bool"}

    # Integers
    if pa.types.is_integer(arrow_type):
        return {
            "bitWidth": arrow_type.bit_width,
            "isSigned": pa.types.is_signed_integer(arrow_type),
            "name": "int",
        }

    # Floating point
    if pa.types.is_floating(arrow_type):
        precision_map = {16: "HALF", 32: "SINGLE", 64: "DOUBLE"}
        return {
            "name": "floatingpoint",
            "precision": precision_map[arrow_type.bit_width],
        }

    # Decimal
    if pa.types.is_decimal(arrow_type):
        return {
            "bitWidth": arrow_type.bit_width,
            "name": "decimal",
            "precision": arrow_type.precision,
            "scale": arrow_type.scale,
        }

    # Date
    if pa.types.is_date(arrow_type):
        if pa.types.is_date32(arrow_type):
            return {"name": "date", "unit": "DAY"}
        else:  # date64
            return {"name": "date", "unit": "MILLISECOND"}

    # Time
    if pa.types.is_time(arrow_type):
        unit_map = {
            "s": "SECOND",
            "ms": "MILLISECOND",
            "us": "MICROSECOND",
            "ns": "NANOSECOND",
        }
        return {
            "bitWidth": arrow_type.bit_width,
            "name": "time",
            "unit": unit_map[str(arrow_type.unit)],
        }

    # Timestamp
    if pa.types.is_timestamp(arrow_type):
        unit_map = {
            "s": "SECOND",
            "ms": "MILLISECOND",
            "us": "MICROSECOND",
            "ns": "NANOSECOND",
        }
        return {
            "name": "timestamp",
            "timezone": arrow_type.tz,  # None if no timezone
            "unit": unit_map[str(arrow_type.unit)],
        }

    # Duration
    if pa.types.is_duration(arrow_type):
        unit_map = {
            "s": "SECOND",
            "ms": "MILLISECOND",
            "us": "MICROSECOND",
            "ns": "NANOSECOND",
        }
        return {
            "name": "duration",
            "unit": unit_map[str(arrow_type.unit)],
        }

    # Interval
    if pa.types.is_interval(arrow_type):
        if arrow_type == pa.month_day_nano_interval():
            unit = "MONTH_DAY_NANO"
        elif arrow_type == pa.day_time_interval():
            unit = "DAY_TIME"
        else:
            unit = "YEAR_MONTH"
        return {"name": "interval", "unit": unit}

    # Binary types
    if pa.types.is_fixed_size_binary(arrow_type):
        return {
            "byteWidth": arrow_type.byte_width,
            "name": "fixedsizebinary",
        }

    if pa.types.is_large_binary(arrow_type):
        return {"name": "largebinary"}

    if pa.types.is_binary(arrow_type):
        return {"name": "binary"}

    # String types - check by comparing to type instances
    if arrow_type == pa.utf8() or arrow_type == pa.string():
        return {"name": "utf8"}

    if arrow_type == pa.large_utf8() or arrow_type == pa.large_string():
        return {"name": "largeutf8"}

    # List types
    if pa.types.is_list(arrow_type):
        return {
            "children": [serialize_field(arrow_type.value_field)],
            "name": "list",
        }

    if pa.types.is_large_list(arrow_type):
        return {
            "children": [serialize_field(arrow_type.value_field)],
            "name": "largelist",
        }

    if pa.types.is_fixed_size_list(arrow_type):
        return {
            "children": [serialize_field(arrow_type.value_field)],
            "listSize": arrow_type.list_size,
            "name": "fixedsizelist",
        }

    # Struct
    if pa.types.is_struct(arrow_type):
        children = {}
        for i in range(arrow_type.num_fields):
            field = arrow_type.field(i)
            children[field.name] = serialize_field(field)
        return {
            "children": children,
            "name": "struct",
        }

    # Map
    if pa.types.is_map(arrow_type):
        return {
            "children": [
                serialize_field(arrow_type.key_field),
                serialize_field(arrow_type.item_field),
            ],
            "keysSorted": arrow_type.keys_sorted,
            "name": "map",
        }

    # Union
    if pa.types.is_union(arrow_type):
        mode = "SPARSE" if arrow_type.mode == "sparse" else "DENSE"
        children = []
        for i in range(arrow_type.num_fields):
            children.append(serialize_field(arrow_type.field(i)))
        return {
            "children": children,
            "mode": mode,
            "name": "union",
            "typeIds": list(arrow_type.type_codes),
        }

    # Dictionary
    if pa.types.is_dictionary(arrow_type):
        return {
            "indexType": serialize_type(arrow_type.index_type),
            "name": "dictionary",
            "valueType": serialize_type(arrow_type.value_type),
        }

    raise ValueError(f"Unsupported Arrow type: {arrow_type}")


def serialize_field(field: pa.Field) -> dict:
    """Convert an Arrow Field to a canonical field descriptor."""
    return {
        "nullable": field.nullable,
        "type": serialize_type(field.type),
    }


def serialize_schema(schema: pa.Schema) -> bytes:
    """
    Serialize an Arrow Schema to canonical BSON bytes.

    The output is deterministic: identical schemas always produce
    identical byte sequences, regardless of field definition order.
    """
    doc = {}
    for i in range(len(schema)):
        field = schema.field(i)
        doc[field.name] = serialize_field(field)

    sorted_doc = sort_keys_recursive(doc)
    return bson.encode(sorted_doc)


def serialize_schema_to_hex(schema: pa.Schema) -> str:
    """Serialize schema and return hex string for debugging."""
    return serialize_schema(schema).hex()


# -----------------------------------------------------------------------------
# Deserialization
# -----------------------------------------------------------------------------


def deserialize_type(type_desc: dict) -> pa.DataType:
    """Convert a type descriptor back to an Arrow DataType."""
    name = type_desc["name"]

    if name == "null":
        return pa.null()

    if name == "bool":
        return pa.bool_()

    if name == "int":
        bit_width = type_desc["bitWidth"]
        signed = type_desc["isSigned"]
        type_map = {
            (8, True): pa.int8(),
            (8, False): pa.uint8(),
            (16, True): pa.int16(),
            (16, False): pa.uint16(),
            (32, True): pa.int32(),
            (32, False): pa.uint32(),
            (64, True): pa.int64(),
            (64, False): pa.uint64(),
        }
        return type_map[(bit_width, signed)]

    if name == "floatingpoint":
        precision_map = {
            "HALF": pa.float16(),
            "SINGLE": pa.float32(),
            "DOUBLE": pa.float64(),
        }
        return precision_map[type_desc["precision"]]

    if name == "decimal":
        bit_width = type_desc["bitWidth"]
        precision = type_desc["precision"]
        scale = type_desc["scale"]
        if bit_width == 128:
            return pa.decimal128(precision, scale)
        elif bit_width == 256:
            return pa.decimal256(precision, scale)
        else:
            raise ValueError(f"Unsupported decimal bit width: {bit_width}")

    if name == "date":
        if type_desc["unit"] == "DAY":
            return pa.date32()
        else:
            return pa.date64()

    if name == "time":
        unit_map = {
            "SECOND": "s",
            "MILLISECOND": "ms",
            "MICROSECOND": "us",
            "NANOSECOND": "ns",
        }
        unit = unit_map[type_desc["unit"]]
        bit_width = type_desc["bitWidth"]
        if bit_width == 32:
            return pa.time32(unit)
        else:
            return pa.time64(unit)

    if name == "timestamp":
        unit_map = {
            "SECOND": "s",
            "MILLISECOND": "ms",
            "MICROSECOND": "us",
            "NANOSECOND": "ns",
        }
        unit = unit_map[type_desc["unit"]]
        tz = type_desc.get("timezone")
        return pa.timestamp(unit, tz=tz)

    if name == "duration":
        unit_map = {
            "SECOND": "s",
            "MILLISECOND": "ms",
            "MICROSECOND": "us",
            "NANOSECOND": "ns",
        }
        return pa.duration(unit_map[type_desc["unit"]])

    if name == "interval":
        unit = type_desc["unit"]
        if unit == "YEAR_MONTH":
            return pa.month_day_nano_interval()  # PyArrow limitation
        elif unit == "DAY_TIME":
            return pa.day_time_interval()
        else:
            return pa.month_day_nano_interval()

    if name == "binary":
        return pa.binary()

    if name == "largebinary":
        return pa.large_binary()

    if name == "fixedsizebinary":
        return pa.binary(type_desc["byteWidth"])

    if name == "utf8":
        return pa.utf8()

    if name == "largeutf8":
        return pa.large_utf8()

    if name == "list":
        child_field = deserialize_field("item", type_desc["children"][0])
        return pa.list_(child_field)

    if name == "largelist":
        child_field = deserialize_field("item", type_desc["children"][0])
        return pa.large_list(child_field)

    if name == "fixedsizelist":
        child_field = deserialize_field("item", type_desc["children"][0])
        return pa.list_(child_field, type_desc["listSize"])

    if name == "struct":
        fields = []
        children = type_desc["children"]
        for field_name in sorted(children.keys()):
            fields.append(deserialize_field(field_name, children[field_name]))
        return pa.struct(fields)

    if name == "map":
        key_field = deserialize_field("key", type_desc["children"][0])
        value_field = deserialize_field("value", type_desc["children"][1])
        return pa.map_(
            key_field.type, value_field.type, keys_sorted=type_desc["keysSorted"]
        )

    if name == "union":
        fields = []
        for i, child in enumerate(type_desc["children"]):
            fields.append(deserialize_field(f"field_{i}", child))
        type_ids = type_desc["typeIds"]
        mode = type_desc["mode"].lower()
        return pa.union(fields, mode=mode, type_codes=type_ids)

    if name == "dictionary":
        index_type = deserialize_type(type_desc["indexType"])
        value_type = deserialize_type(type_desc["valueType"])
        return pa.dictionary(index_type, value_type)

    raise ValueError(f"Unknown type name: {name}")


def deserialize_field(name: str, field_desc: dict) -> pa.Field:
    """Convert a field descriptor back to an Arrow Field."""
    return pa.field(
        name,
        deserialize_type(field_desc["type"]),
        nullable=field_desc["nullable"],
    )


def deserialize_schema(bson_bytes: bytes) -> pa.Schema:
    """
    Deserialize BSON bytes back to an Arrow Schema.

    Fields are reconstructed in alphabetical order by name.
    """
    doc = bson.decode(bson_bytes)
    fields = []
    for field_name in sorted(doc.keys()):
        fields.append(deserialize_field(field_name, doc[field_name]))
    return pa.schema(fields)


# -----------------------------------------------------------------------------
# Testing / Verification
# -----------------------------------------------------------------------------


def verify_roundtrip(schema: pa.Schema) -> bool:
    """Verify that a schema survives serialization roundtrip."""
    bson_bytes = serialize_schema(schema)
    reconstructed = deserialize_schema(bson_bytes)
    return schema.equals(reconstructed)


def print_debug(schema: pa.Schema) -> None:
    """Print debug information about schema serialization."""
    import json

    print("Original Schema:")
    print(schema)
    print()

    # Build the document (before BSON encoding)
    doc = {}
    for i in range(len(schema)):
        field = schema.field(i)
        doc[field.name] = serialize_field(field)
    sorted_doc = sort_keys_recursive(doc)

    print("Canonical JSON representation:")
    print(json.dumps(sorted_doc, indent=2))
    print()

    bson_bytes = bson.encode(sorted_doc)
    print(f"BSON bytes ({len(bson_bytes)} bytes):")
    print(bson_bytes.hex())
    print()

    # Verify roundtrip
    reconstructed = deserialize_schema(bson_bytes)
    print("Reconstructed Schema:")
    print(reconstructed)
    print()
    print("Roundtrip successful:", schema.equals(reconstructed))


# -----------------------------------------------------------------------------
# Example usage
# -----------------------------------------------------------------------------

if __name__ == "__main__":
    # Example 1: Simple schema
    schema1 = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("name", pa.utf8(), nullable=True),
            pa.field("score", pa.float64(), nullable=False),
        ]
    )
    print("=" * 60)
    print("Example 1: Simple schema")
    print("=" * 60)
    print_debug(schema1)

    # Example 2: Schema with nested struct
    schema2 = pa.schema(
        [
            pa.field("user_id", pa.int64(), nullable=False),
            pa.field(
                "profile",
                pa.struct(
                    [
                        pa.field("email", pa.utf8(), nullable=False),
                        pa.field("age", pa.int32(), nullable=True),
                    ]
                ),
                nullable=True,
            ),
        ]
    )
    print("\n" + "=" * 60)
    print("Example 2: Nested struct")
    print("=" * 60)
    print_debug(schema2)

    # Example 3: Schema with list and timestamp
    schema3 = pa.schema(
        [
            pa.field("event_time", pa.timestamp("us", tz="UTC"), nullable=False),
            pa.field(
                "tags",
                pa.list_(pa.field("item", pa.utf8(), nullable=True)),
                nullable=True,
            ),
        ]
    )
    print("\n" + "=" * 60)
    print("Example 3: List and timestamp")
    print("=" * 60)
    print_debug(schema3)

    # Example 4: Demonstrate field order independence
    schema4a = pa.schema(
        [
            pa.field("b", pa.int32()),
            pa.field("a", pa.int32()),
        ]
    )
    schema4b = pa.schema(
        [
            pa.field("a", pa.int32()),
            pa.field("b", pa.int32()),
        ]
    )
    print("\n" + "=" * 60)
    print("Example 4: Field order independence")
    print("=" * 60)
    bytes_a = serialize_schema(schema4a)
    bytes_b = serialize_schema(schema4b)
    print(f"Schema [b, a] -> {bytes_a.hex()}")
    print(f"Schema [a, b] -> {bytes_b.hex()}")
    print(f"Identical bytes: {bytes_a == bytes_b}")
