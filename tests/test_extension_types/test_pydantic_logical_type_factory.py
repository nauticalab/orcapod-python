"""Tests for PydanticLogicalType and PydanticLogicalTypeFactory."""

from __future__ import annotations

import uuid as _uuid_module
from typing import Any

import pyarrow as pa
import pytest
from pydantic import BaseModel, PrivateAttr


# ── Helpers ──────────────────────────────────────────────────────────────────

class _StubConverter:
    """Minimal converter stub for PydanticLogicalType tests."""

    def python_to_storage(self, value, annotation):
        if annotation is str:
            return str(value)
        if annotation is int:
            return int(value)
        return value

    def storage_to_python(self, storage_value, annotation):
        if annotation is str:
            return str(storage_value)
        if annotation is int:
            return int(storage_value)
        return storage_value

    def register_python_class(self, annotation):
        if annotation is str:
            return pa.large_string()
        if annotation is int:
            return pa.int64()
        raise ValueError(f"No mapping for {annotation}")


# ── PydanticLogicalType tests ────────────────────────────────────────────────

def test_pydantic_logical_type_is_importable():
    from orcapod.extension_types.pydantic_logical_type_factory import PydanticLogicalType
    assert PydanticLogicalType is not None


def test_pydantic_logical_type_protocol_conformance():
    from orcapod.extension_types.pydantic_logical_type_factory import PydanticLogicalType
    from orcapod.extension_types.protocols import LogicalTypeProtocol

    class _MyModel(BaseModel):
        name: str
        count: int

    storage = pa.struct([pa.field("name", pa.large_string()), pa.field("count", pa.int64())])
    lt = PydanticLogicalType(
        logical_name="tests._MyModel",
        python_type=_MyModel,
        storage_type=storage,
        field_annotations=[("name", str), ("count", int)],
    )
    assert isinstance(lt, LogicalTypeProtocol)


def test_pydantic_logical_type_python_to_storage():
    from orcapod.extension_types.pydantic_logical_type_factory import PydanticLogicalType

    class _Point(BaseModel):
        x: int
        y: int

    storage = pa.struct([pa.field("x", pa.int64()), pa.field("y", pa.int64())])
    lt = PydanticLogicalType("tests._Point", _Point, storage, [("x", int), ("y", int)])
    converter = _StubConverter()

    result = lt.python_to_storage(_Point(x=3, y=7), converter)
    assert result == {"x": 3, "y": 7}


def test_pydantic_logical_type_storage_to_python():
    from orcapod.extension_types.pydantic_logical_type_factory import PydanticLogicalType

    class _Point(BaseModel):
        x: int
        y: int

    storage = pa.struct([pa.field("x", pa.int64()), pa.field("y", pa.int64())])
    lt = PydanticLogicalType("tests._Point2", _Point, storage, [("x", int), ("y", int)])
    converter = _StubConverter()

    result = lt.storage_to_python({"x": 3, "y": 7}, converter)
    assert isinstance(result, _Point)
    assert result.x == 3
    assert result.y == 7


def test_pydantic_logical_type_logical_type_name():
    from orcapod.extension_types.pydantic_logical_type_factory import PydanticLogicalType

    class _Foo(BaseModel):
        val: str

    storage = pa.struct([pa.field("val", pa.large_string())])
    lt = PydanticLogicalType("mymod.Foo", _Foo, storage, [("val", str)])
    assert lt.logical_type_name == "mymod.Foo"


def test_pydantic_logical_type_python_type():
    from orcapod.extension_types.pydantic_logical_type_factory import PydanticLogicalType

    class _Bar(BaseModel):
        val: str

    storage = pa.struct([pa.field("val", pa.large_string())])
    lt = PydanticLogicalType("mymod.Bar", _Bar, storage, [("val", str)])
    assert lt.python_type is _Bar


def test_python_to_storage_raises_when_converter_none():
    from orcapod.extension_types.pydantic_logical_type_factory import PydanticLogicalType

    class _DC(BaseModel):
        x: int

    storage = pa.struct([pa.field("x", pa.int64())])
    lt = PydanticLogicalType("mymod._DC", _DC, storage, [("x", int)])
    with pytest.raises(ValueError, match="converter"):
        lt.python_to_storage(_DC(x=1), None)


def test_storage_to_python_raises_when_converter_none():
    from orcapod.extension_types.pydantic_logical_type_factory import PydanticLogicalType

    class _DC2(BaseModel):
        x: int

    storage = pa.struct([pa.field("x", pa.int64())])
    lt = PydanticLogicalType("mymod._DC2", _DC2, storage, [("x", int)])
    with pytest.raises(ValueError, match="converter"):
        lt.storage_to_python({"x": 1}, None)
