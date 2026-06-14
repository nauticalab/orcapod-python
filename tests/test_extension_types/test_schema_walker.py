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
    _n, _m, _s = name, meta, pa.large_utf8()
    ExtType = type(
        "_DupExt",
        (pa.ExtensionType,),
        {
            "__init__": lambda self: pa.ExtensionType.__init__(self, _s, _n),
            "__arrow_ext_serialize__": lambda self: _m,
            "__arrow_ext_deserialize__": classmethod(lambda cls, st, se: cls()),
        },
    )
    schema = pa.schema([
        pa.field("col_a", ExtType()),
        pa.field("col_b", ExtType()),
    ])
    result = walk_schema(schema)
    assert len(result) == 1
    assert result[0].extension_name == name
    assert result[0].extension_metadata == meta
