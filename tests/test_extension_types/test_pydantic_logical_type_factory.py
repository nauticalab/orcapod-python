"""Tests for PydanticLogicalType and PydanticLogicalTypeFactory."""

from __future__ import annotations

from typing import Any

import uuid as _uuid_module

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


# ── Module-level models for factory tests ────────────────────────────────────
# Must be at module scope (not inside functions) so FQCN reconstruction works.

class _FlatModel(BaseModel):
    name: str
    count: int


class _ModelWithUUID(BaseModel):
    id: _uuid_module.UUID
    label: str


class _ModelWithList(BaseModel):
    tags: list[str]
    count: int


class _ModelWithDict(BaseModel):
    meta: dict[str, int]


class _InnerModel(BaseModel):
    value: int


class _OuterModel(BaseModel):
    inner: _InnerModel
    label: str


class _ModelWithPrivateAttr(BaseModel):
    name: str
    _cache: str = PrivateAttr(default="")


# ── Factory helper ────────────────────────────────────────────────────────────

def _make_full_converter():
    """Make a UniversalTypeConverter with builtin types + PydanticLogicalTypeFactory."""
    from pydantic import BaseModel as _BaseModel
    from orcapod.extension_types.builtin_logical_types import LogicalPath, LogicalUUID, LogicalUPath
    from orcapod.extension_types.registry import LogicalTypeRegistry
    from orcapod.extension_types.pydantic_logical_type_factory import PydanticLogicalTypeFactory, PYDANTIC_CATEGORY
    from orcapod.semantic_types.universal_converter import UniversalTypeConverter

    registry = LogicalTypeRegistry(logical_types=[LogicalPath(), LogicalUUID(), LogicalUPath()])
    factory = PydanticLogicalTypeFactory()
    registry.register_logical_type_factory(factory, category=PYDANTIC_CATEGORY, python_bases=[_BaseModel])
    return UniversalTypeConverter(logical_type_registry=registry)


# ── PydanticLogicalTypeFactory write-path tests ───────────────────────────────

def test_factory_supports_class_pydantic_model():
    from orcapod.extension_types.pydantic_logical_type_factory import PydanticLogicalTypeFactory

    factory = PydanticLogicalTypeFactory()
    assert factory.supports_class(_FlatModel) is True


def test_factory_supports_class_non_pydantic():
    from orcapod.extension_types.pydantic_logical_type_factory import PydanticLogicalTypeFactory

    import dataclasses

    @dataclasses.dataclass
    class _DC:
        x: int

    factory = PydanticLogicalTypeFactory()
    assert factory.supports_class(str) is False
    assert factory.supports_class(int) is False
    assert factory.supports_class(_DC) is False


def test_factory_create_flat_model():
    from orcapod.extension_types.pydantic_logical_type_factory import PydanticLogicalTypeFactory, PydanticLogicalType

    factory = PydanticLogicalTypeFactory()
    converter = _make_full_converter()
    lt = factory.create_for_python_type(_FlatModel, converter=converter)

    assert isinstance(lt, PydanticLogicalType)
    storage = lt.get_arrow_extension_type().storage_type
    assert pa.types.is_struct(storage)
    assert storage.field("name").type == pa.large_string()
    assert storage.field("count").type == pa.int64()


def test_factory_create_model_with_uuid_field():
    """UUID field → plain storage type (large_binary) in the struct, not extension type (ET1)."""
    from orcapod.extension_types.pydantic_logical_type_factory import PydanticLogicalTypeFactory

    factory = PydanticLogicalTypeFactory()
    converter = _make_full_converter()
    lt = factory.create_for_python_type(_ModelWithUUID, converter=converter)

    storage = lt.get_arrow_extension_type().storage_type
    id_field_type = storage.field("id").type
    assert id_field_type == pa.large_binary()
    assert not isinstance(id_field_type, pa.ExtensionType)


def test_factory_create_model_with_list_field():
    from orcapod.extension_types.pydantic_logical_type_factory import PydanticLogicalTypeFactory

    factory = PydanticLogicalTypeFactory()
    converter = _make_full_converter()
    lt = factory.create_for_python_type(_ModelWithList, converter=converter)

    storage = lt.get_arrow_extension_type().storage_type
    assert pa.types.is_large_list(storage.field("tags").type)
    assert storage.field("tags").type.value_type == pa.large_string()


def test_factory_create_model_with_dict_field():
    from orcapod.extension_types.pydantic_logical_type_factory import PydanticLogicalTypeFactory

    factory = PydanticLogicalTypeFactory()
    converter = _make_full_converter()
    lt = factory.create_for_python_type(_ModelWithDict, converter=converter)

    storage = lt.get_arrow_extension_type().storage_type
    meta_type = storage.field("meta").type
    assert pa.types.is_large_list(meta_type)
    assert pa.types.is_struct(meta_type.value_type)
    field_names = {meta_type.value_type.field(i).name for i in range(meta_type.value_type.num_fields)}
    assert field_names == {"key", "value"}


def test_factory_rejects_local_class():
    from orcapod.extension_types.pydantic_logical_type_factory import PydanticLogicalTypeFactory

    def _make_local():
        class _Local(BaseModel):
            x: int
        return _Local

    LocalModel = _make_local()
    factory = PydanticLogicalTypeFactory()
    converter = _make_full_converter()
    with pytest.raises(ValueError, match="local"):
        factory.create_for_python_type(LocalModel, converter=converter)


def test_private_fields_not_stored():
    """Private attributes (PrivateAttr) must not appear in the Arrow struct."""
    from orcapod.extension_types.pydantic_logical_type_factory import PydanticLogicalTypeFactory

    factory = PydanticLogicalTypeFactory()
    converter = _make_full_converter()
    lt = factory.create_for_python_type(_ModelWithPrivateAttr, converter=converter)

    storage = lt.get_arrow_extension_type().storage_type
    field_names = {storage.field(i).name for i in range(storage.num_fields)}
    assert "name" in field_names
    assert "_cache" not in field_names
    assert storage.num_fields == 1
