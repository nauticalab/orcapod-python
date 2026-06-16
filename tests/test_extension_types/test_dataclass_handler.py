"""Tests for DataclassLogicalType and DataclassHandlerFactory."""

from __future__ import annotations

import dataclasses
import uuid as _uuid_module
from typing import Any

import pyarrow as pa
import pytest


# ── Helpers ─────────────────────────────────────────────────────────────────

class _StubConverter:
    """Minimal converter stub for DataclassLogicalType tests."""

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


# ── DataclassLogicalType tests ───────────────────────────────────────────────

def test_dataclass_logical_type_is_importable():
    from orcapod.extension_types.dataclass_handler import DataclassLogicalType
    assert DataclassLogicalType is not None


def test_dataclass_logical_type_protocol_conformance():
    from orcapod.extension_types.dataclass_handler import DataclassLogicalType
    from orcapod.extension_types.protocols import LogicalTypeProtocol

    @dataclasses.dataclass
    class _MyDC:
        name: str
        count: int

    storage = pa.struct([pa.field("name", pa.large_string()), pa.field("count", pa.int64())])
    field_annotations = [("name", str), ("count", int)]
    lt = DataclassLogicalType(
        logical_name="tests.MyDC",
        python_type=_MyDC,
        storage_type=storage,
        field_annotations=field_annotations,
    )
    assert isinstance(lt, LogicalTypeProtocol)


def test_dataclass_logical_type_python_to_storage():
    from orcapod.extension_types.dataclass_handler import DataclassLogicalType

    @dataclasses.dataclass
    class _Point:
        x: int
        y: int

    storage = pa.struct([pa.field("x", pa.int64()), pa.field("y", pa.int64())])
    lt = DataclassLogicalType("tests.Point", _Point, storage, [("x", int), ("y", int)])
    converter = _StubConverter()

    result = lt.python_to_storage(_Point(x=3, y=7), converter)
    assert result == {"x": 3, "y": 7}


def test_dataclass_logical_type_storage_to_python():
    from orcapod.extension_types.dataclass_handler import DataclassLogicalType

    @dataclasses.dataclass
    class _Point:
        x: int
        y: int

    storage = pa.struct([pa.field("x", pa.int64()), pa.field("y", pa.int64())])
    lt = DataclassLogicalType("tests.Point", _Point, storage, [("x", int), ("y", int)])
    converter = _StubConverter()

    result = lt.storage_to_python({"x": 3, "y": 7}, converter)
    assert isinstance(result, _Point)
    assert result.x == 3
    assert result.y == 7


def test_dataclass_logical_type_logical_type_name():
    from orcapod.extension_types.dataclass_handler import DataclassLogicalType

    @dataclasses.dataclass
    class _Foo:
        val: str

    storage = pa.struct([pa.field("val", pa.large_string())])
    lt = DataclassLogicalType("mymod.Foo", _Foo, storage, [("val", str)])
    assert lt.logical_type_name == "mymod.Foo"


def test_dataclass_logical_type_python_type():
    from orcapod.extension_types.dataclass_handler import DataclassLogicalType

    @dataclasses.dataclass
    class _Bar:
        val: str

    storage = pa.struct([pa.field("val", pa.large_string())])
    lt = DataclassLogicalType("mymod.Bar", _Bar, storage, [("val", str)])
    assert lt.python_type is _Bar


# ── DataclassHandlerFactory helpers ──────────────────────────────────────────

def _make_full_converter():
    """Make a UniversalTypeConverter with builtin types + DataclassHandlerFactory."""
    from orcapod.extension_types.builtin_logical_types import LogicalPath, LogicalUUID, LogicalUPath
    from orcapod.extension_types.registry import LogicalTypeRegistry
    from orcapod.extension_types.dataclass_handler import DataclassHandlerFactory, DATACLASS_CATEGORY
    from orcapod.semantic_types.universal_converter import UniversalTypeConverter

    registry = LogicalTypeRegistry(logical_types=[LogicalPath(), LogicalUUID(), LogicalUPath()])
    factory = DataclassHandlerFactory()
    registry.register_logical_type_factory(factory, category=DATACLASS_CATEGORY, python_bases=[object])
    return UniversalTypeConverter(logical_type_registry=registry)


# ── DataclassHandlerFactory write-path tests ─────────────────────────────────

def test_factory_supports_class_dataclass():
    from orcapod.extension_types.dataclass_handler import DataclassHandlerFactory

    @dataclasses.dataclass
    class _Dummy:
        x: int

    factory = DataclassHandlerFactory()
    assert factory.supports_class(_Dummy) is True


def test_factory_supports_class_non_dataclass():
    from orcapod.extension_types.dataclass_handler import DataclassHandlerFactory

    factory = DataclassHandlerFactory()
    assert factory.supports_class(str) is False
    assert factory.supports_class(int) is False


@dataclasses.dataclass
class _Flat:
    name: str
    count: int


@dataclasses.dataclass
class _WithUUID:
    id: _uuid_module.UUID
    label: str


@dataclasses.dataclass
class _WithList:
    tags: list[str]
    count: int


@dataclasses.dataclass
class _WithDict:
    meta: dict[str, int]


def test_factory_create_flat_dataclass():
    from orcapod.extension_types.dataclass_handler import DataclassHandlerFactory, DataclassLogicalType

    factory = DataclassHandlerFactory()
    converter = _make_full_converter()
    lt = factory.create_for_python_type(_Flat, converter=converter)

    assert isinstance(lt, DataclassLogicalType)
    storage = lt.get_arrow_extension_type().storage_type
    assert pa.types.is_struct(storage)
    assert storage.field("name").type == pa.large_string()
    assert storage.field("count").type == pa.int64()


def test_factory_create_dataclass_with_uuid_field():
    """UUID field → orcapod.uuid extension type in storage struct."""
    from orcapod.extension_types.dataclass_handler import DataclassHandlerFactory

    factory = DataclassHandlerFactory()
    converter = _make_full_converter()
    lt = factory.create_for_python_type(_WithUUID, converter=converter)

    storage = lt.get_arrow_extension_type().storage_type
    id_field_type = storage.field("id").type
    assert isinstance(id_field_type, pa.ExtensionType)
    assert id_field_type.extension_name == "orcapod.uuid"


def test_factory_create_dataclass_with_list_field():
    from orcapod.extension_types.dataclass_handler import DataclassHandlerFactory

    factory = DataclassHandlerFactory()
    converter = _make_full_converter()
    lt = factory.create_for_python_type(_WithList, converter=converter)

    storage = lt.get_arrow_extension_type().storage_type
    assert pa.types.is_large_list(storage.field("tags").type)
    assert storage.field("tags").type.value_type == pa.large_string()


def test_factory_create_dataclass_with_dict_field():
    from orcapod.extension_types.dataclass_handler import DataclassHandlerFactory

    factory = DataclassHandlerFactory()
    converter = _make_full_converter()
    lt = factory.create_for_python_type(_WithDict, converter=converter)

    storage = lt.get_arrow_extension_type().storage_type
    meta_type = storage.field("meta").type
    assert pa.types.is_large_list(meta_type)
    assert pa.types.is_struct(meta_type.value_type)
    field_names = {meta_type.value_type.field(i).name for i in range(meta_type.value_type.num_fields)}
    assert field_names == {"key", "value"}


def test_factory_rejects_local_class():
    from orcapod.extension_types.dataclass_handler import DataclassHandlerFactory

    def _make_local():
        @dataclasses.dataclass
        class _Local:
            x: int
        return _Local

    LocalClass = _make_local()
    factory = DataclassHandlerFactory()
    converter = _make_full_converter()
    with pytest.raises(ValueError, match="local"):
        factory.create_for_python_type(LocalClass, converter=converter)


def test_register_python_class_dispatches_to_dataclass_factory():
    """register_python_class on a dataclass triggers DataclassHandlerFactory."""
    converter = _make_full_converter()

    # For this test, use UUID as a proxy (already registered as built-in).
    result = converter.register_python_class(_uuid_module.UUID)
    assert isinstance(result, pa.ExtensionType)
    assert result.extension_name == "orcapod.uuid"


# ── Module-level dataclasses for round-trip tests ────────────────────────────

@dataclasses.dataclass
class _RoundTripPoint:
    """Module-level dataclass for round-trip testing."""
    x: int
    y: int


@dataclasses.dataclass
class _RoundTripRecord:
    """Module-level dataclass with a UUID field."""
    record_id: _uuid_module.UUID
    label: str


# ── Read-path tests ───────────────────────────────────────────────────────────

def test_factory_reconstruct_from_arrow():
    """reconstruct_from_arrow rebuilds the logical type from the Arrow struct."""
    from orcapod.extension_types.dataclass_handler import DataclassHandlerFactory, DataclassLogicalType

    storage = pa.struct([pa.field("x", pa.int64()), pa.field("y", pa.int64())])
    metadata = {"category": "orcapod.dataclass"}
    fqcn = f"{_RoundTripPoint.__module__}.{_RoundTripPoint.__qualname__}"

    factory = DataclassHandlerFactory()
    converter = _make_full_converter()
    lt = factory.reconstruct_from_arrow(fqcn, storage, metadata, converter=converter)

    assert isinstance(lt, DataclassLogicalType)
    assert lt.python_type is _RoundTripPoint
    assert lt.logical_type_name == fqcn


def test_factory_reconstruct_from_arrow_invalid_fqcn():
    """ImportError if the FQCN cannot be resolved."""
    from orcapod.extension_types.dataclass_handler import DataclassHandlerFactory

    storage = pa.struct([pa.field("x", pa.int64())])
    factory = DataclassHandlerFactory()
    converter = _make_full_converter()

    with pytest.raises(ImportError):
        factory.reconstruct_from_arrow(
            "nonexistent.module.NoSuchClass", storage, {"category": "orcapod.dataclass"}, converter
        )


def test_dataclass_python_to_storage_round_trip():
    """python_to_storage → storage_to_python returns an equivalent dataclass."""
    converter = _make_full_converter()

    from orcapod.extension_types.dataclass_handler import DataclassHandlerFactory
    factory = DataclassHandlerFactory()
    lt = factory.create_for_python_type(_RoundTripPoint, converter=converter)
    converter.register_logical_type(lt)

    point = _RoundTripPoint(x=10, y=20)
    storage_value = lt.python_to_storage(point, converter)
    assert storage_value == {"x": 10, "y": 20}

    reconstructed = lt.storage_to_python(storage_value, converter)
    assert isinstance(reconstructed, _RoundTripPoint)
    assert reconstructed.x == 10
    assert reconstructed.y == 20


def test_dataclass_with_uuid_round_trip():
    """Round-trip a dataclass with a UUID field through python_to_storage / storage_to_python."""
    from orcapod.extension_types.dataclass_handler import DataclassHandlerFactory

    converter = _make_full_converter()
    factory = DataclassHandlerFactory()
    lt = factory.create_for_python_type(_RoundTripRecord, converter=converter)
    converter.register_logical_type(lt)

    u = _uuid_module.UUID("12345678-1234-5678-1234-567812345678")
    record = _RoundTripRecord(record_id=u, label="hello")

    storage_value = lt.python_to_storage(record, converter)
    assert storage_value["label"] == "hello"
    # UUID stored as bytes
    assert storage_value["record_id"] == u.bytes

    reconstructed = lt.storage_to_python(storage_value, converter)
    assert isinstance(reconstructed, _RoundTripRecord)
    assert reconstructed.record_id == u
    assert reconstructed.label == "hello"
