"""Tests for schema_walker — recursive Arrow extension type discovery."""

from __future__ import annotations

import re
import uuid

import pyarrow as pa
import pytest

from orcapod.extension_types.schema_walker import (
    ExtensionTypeInfo,
    walk_field,
    walk_schema,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _unique_name() -> str:
    """Return a unique extension name to avoid cross-test collisions."""
    return f"test.walker.{uuid.uuid4().hex[:8]}"


def _make_reg_field(
    field_name: str,
    ext_name: str,
    storage: pa.DataType | None = None,
    metadata: bytes = b"test.cat",
) -> pa.Field:
    """Create a ``pa.Field`` with an in-memory ``pa.ExtensionType`` (registered channel).

    The extension type is NOT registered in PyArrow's global registry — this
    is intentional. ``pa.types.is_extension(field.type)`` returns ``True``
    for any ``pa.ExtensionType`` instance regardless of global registration.
    """
    _n = ext_name
    _s = storage if storage is not None else pa.large_utf8()
    _m = metadata
    ExtType = type(
        f"_RegExt_{re.sub(r'[^A-Za-z0-9]', '_', ext_name)}",
        (pa.ExtensionType,),
        {
            "__init__": lambda self: pa.ExtensionType.__init__(self, _s, _n),
            "__arrow_ext_serialize__": lambda self: _m,
            "__arrow_ext_deserialize__": classmethod(lambda cls, st, se: cls()),
        },
    )
    return pa.field(field_name, ExtType())


def _make_unreg_field(
    field_name: str,
    ext_name: str,
    storage: pa.DataType | None = None,
    metadata: bytes = b"test.cat",
) -> pa.Field:
    """Create a ``pa.Field`` with raw Arrow extension metadata (unregistered channel)."""
    _s = storage if storage is not None else pa.large_utf8()
    return pa.field(
        field_name,
        _s,
        metadata={
            b"ARROW:extension:name": ext_name.encode(),
            b"ARROW:extension:metadata": metadata,
        },
    )


# ---------------------------------------------------------------------------
# Task 1 tests: top-level detection and deduplication
# ---------------------------------------------------------------------------


def test_empty_schema():
    result = walk_schema(pa.schema([]))
    assert result == []


def test_no_extension_types():
    schema = pa.schema([
        pa.field("x", pa.int64()),
        pa.field("y", pa.large_utf8()),
    ])
    assert walk_schema(schema) == []


def test_top_level_registered():
    name = _unique_name()
    schema = pa.schema([_make_reg_field("col", name, metadata=b"my.cat")])
    result = walk_schema(schema)
    assert len(result) == 1
    assert result[0].extension_name == name
    assert result[0].extension_metadata == b"my.cat"
    assert result[0].storage_type == pa.large_utf8()


def test_top_level_unregistered():
    name = _unique_name()
    schema = pa.schema([_make_unreg_field("col", name, metadata=b"my.cat")])
    result = walk_schema(schema)
    assert len(result) == 1
    assert result[0].extension_name == name
    assert result[0].extension_metadata == b"my.cat"
    assert result[0].storage_type == pa.large_utf8()


def test_empty_metadata_normalised_to_none_registered():
    """b'' from __arrow_ext_serialize__ is normalised to None."""
    name = _unique_name()
    _n, _s = name, pa.large_utf8()
    ExtType = type(
        "_EmptyMetaExt",
        (pa.ExtensionType,),
        {
            "__init__": lambda self: pa.ExtensionType.__init__(self, _s, _n),
            "__arrow_ext_serialize__": lambda self: b"",
            "__arrow_ext_deserialize__": classmethod(lambda cls, st, se: cls()),
        },
    )
    result = walk_field(pa.field("col", ExtType()))
    assert len(result) == 1
    assert result[0].extension_metadata is None


def test_empty_metadata_normalised_to_none_unregistered():
    """b'' ARROW:extension:metadata value is normalised to None."""
    name = _unique_name()
    field = pa.field(
        "col",
        pa.large_utf8(),
        metadata={
            b"ARROW:extension:name": name.encode(),
            b"ARROW:extension:metadata": b"",
        },
    )
    result = walk_field(field)
    assert len(result) == 1
    assert result[0].extension_metadata is None


def test_walk_field_returns_single_field_result():
    name = _unique_name()
    field = _make_reg_field("col", name, metadata=b"cat")
    result = walk_field(field)
    assert len(result) == 1
    assert result[0].extension_name == name


def test_deduplication():
    """Same (extension_name, extension_metadata) in two columns → one result."""
    name = _unique_name()
    meta = b"test.cat"
    schema = pa.schema([
        _make_reg_field("col_a", name, metadata=meta),
        _make_reg_field("col_b", name, metadata=meta),
    ])
    result = walk_schema(schema)
    assert len(result) == 1
    assert result[0].extension_name == name
    assert result[0].extension_metadata == meta


# ---------------------------------------------------------------------------
# Task 2 tests: container recursion
# ---------------------------------------------------------------------------


def test_list_of_registered():
    """Registered extension type as the value field of a list."""
    name = _unique_name()
    value_field = _make_reg_field("item", name, metadata=b"my.cat")
    list_field = pa.field("col", pa.list_(value_field))
    result = walk_schema(pa.schema([list_field]))
    assert len(result) == 1
    assert result[0].extension_name == name
    assert result[0].extension_metadata == b"my.cat"


def test_list_of_unregistered():
    """Unregistered extension type as the value field of a list."""
    name = _unique_name()
    value_field = _make_unreg_field("item", name, metadata=b"my.cat")
    list_field = pa.field("col", pa.list_(value_field))
    result = walk_schema(pa.schema([list_field]))
    assert len(result) == 1
    assert result[0].extension_name == name
    assert result[0].extension_metadata == b"my.cat"


def test_struct_containing_registered():
    """Registered extension type as a field inside a struct."""
    name = _unique_name()
    struct_field = pa.field(
        "col",
        pa.struct([
            _make_reg_field("a", name, metadata=b"my.cat"),
            pa.field("b", pa.int64()),
        ]),
    )
    result = walk_schema(pa.schema([struct_field]))
    assert len(result) == 1
    assert result[0].extension_name == name
    assert result[0].extension_metadata == b"my.cat"


def test_struct_containing_unregistered():
    """Unregistered extension type as a field inside a struct."""
    name = _unique_name()
    struct_field = pa.field(
        "col",
        pa.struct([
            _make_unreg_field("a", name, metadata=b"my.cat"),
            pa.field("b", pa.int64()),
        ]),
    )
    result = walk_schema(pa.schema([struct_field]))
    assert len(result) == 1
    assert result[0].extension_name == name
    assert result[0].extension_metadata == b"my.cat"


def test_nested_list_struct():
    """Registered extension type nested inside list<struct<...>>."""
    name = _unique_name()
    struct_type = pa.struct([
        _make_reg_field("x", name, metadata=b"deep.cat"),
        pa.field("y", pa.int32()),
    ])
    value_field = pa.field("item", struct_type)
    col = pa.field("col", pa.list_(value_field))
    result = walk_schema(pa.schema([col]))
    assert len(result) == 1
    assert result[0].extension_name == name
    assert result[0].extension_metadata == b"deep.cat"


def test_map_type():
    """Extension type as the item type of a map (registered channel)."""
    name = _unique_name()
    _n, _m, _s = name, b"map.cat", pa.large_utf8()
    # Build a pa.ExtensionType instance — it IS a pa.DataType and can be
    # passed directly to pa.map_() as the item type.
    ExtType = type(
        "_MapItemExt",
        (pa.ExtensionType,),
        {
            "__init__": lambda self: pa.ExtensionType.__init__(self, _s, _n),
            "__arrow_ext_serialize__": lambda self: _m,
            "__arrow_ext_deserialize__": classmethod(lambda cls, st, se: cls()),
        },
    )
    map_field = pa.field("col", pa.map_(pa.large_utf8(), ExtType()))
    result = walk_schema(pa.schema([map_field]))
    # _collect uses getattr(t, "item_field") to retrieve the item pa.Field.
    # pa.types.is_extension(item_field.type) will be True for the ExtType above.
    assert len(result) == 1
    assert result[0].extension_name == name
