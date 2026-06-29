"""Tests for PydanticLogicalType and PydanticLogicalTypeFactory."""

from __future__ import annotations

from typing import Any, Literal

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


class _LiteralStrModel(BaseModel):
    method: Literal["a", "b"]


class _LiteralIntModel(BaseModel):
    count: Literal[1, 2, 3]


class _LiteralNoneModel(BaseModel):
    status: Literal["active", None]


class _LiteralNoneOnlyModel(BaseModel):
    x: Literal[None]


class _MixedLiteralModel(BaseModel):
    val: Literal["a", 1]  # type: ignore[assignment]


class _LiteralRoundTripModel(BaseModel):
    method: Literal["a", "b"]
    count: int


# ── Module-level models for read-path and round-trip tests ───────────────────

class _RoundTripPoint(BaseModel):
    x: int
    y: int


class _RoundTripRecord(BaseModel):
    record_id: _uuid_module.UUID
    label: str


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


# ── PydanticLogicalTypeFactory read-path tests ────────────────────────────────

def test_factory_reconstruct_from_arrow():
    """reconstruct_from_arrow rebuilds the logical type from the Arrow struct."""
    from orcapod.extension_types.pydantic_logical_type_factory import PydanticLogicalTypeFactory, PydanticLogicalType

    storage = pa.struct([pa.field("x", pa.int64()), pa.field("y", pa.int64())])
    metadata = {"category": "orcapod.pydantic"}
    fqcn = f"{_RoundTripPoint.__module__}.{_RoundTripPoint.__qualname__}"

    factory = PydanticLogicalTypeFactory()
    converter = _make_full_converter()
    lt = factory.reconstruct_from_arrow(fqcn, storage, metadata, converter=converter)

    assert isinstance(lt, PydanticLogicalType)
    assert lt.python_type is _RoundTripPoint
    assert lt.logical_type_name == fqcn


def test_factory_reconstruct_from_arrow_invalid_fqcn():
    """ImportError if the FQCN cannot be resolved."""
    from orcapod.extension_types.pydantic_logical_type_factory import PydanticLogicalTypeFactory

    storage = pa.struct([pa.field("x", pa.int64())])
    factory = PydanticLogicalTypeFactory()
    converter = _make_full_converter()

    with pytest.raises(ImportError):
        factory.reconstruct_from_arrow(
            "nonexistent.module.NoSuchModel", storage, {"category": "orcapod.pydantic"}, converter
        )


def test_reconstruct_from_arrow_registers_nested_types():
    """reconstruct_from_arrow for Outer must register Inner as a side effect."""
    from orcapod.extension_types.pydantic_logical_type_factory import PydanticLogicalTypeFactory

    inner_storage = pa.struct([pa.field("value", pa.int64())])
    outer_storage = pa.struct([
        pa.field("inner", inner_storage),
        pa.field("label", pa.large_string()),
    ])
    outer_fqcn = f"{_OuterModel.__module__}.{_OuterModel.__qualname__}"

    factory = PydanticLogicalTypeFactory()
    converter = _make_full_converter()

    # Inner is NOT pre-registered
    assert converter._logical_type_registry.get_by_python_type(_InnerModel) is None

    factory.reconstruct_from_arrow(outer_fqcn, outer_storage, {"category": "orcapod.pydantic"}, converter)

    # Inner must now be registered as a side effect
    assert converter._logical_type_registry.get_by_python_type(_InnerModel) is not None


# ── Value round-trip tests ────────────────────────────────────────────────────

def test_pydantic_python_to_storage_round_trip():
    """python_to_storage → storage_to_python returns an equivalent model."""
    from orcapod.extension_types.pydantic_logical_type_factory import PydanticLogicalTypeFactory

    converter = _make_full_converter()
    factory = PydanticLogicalTypeFactory()
    lt = factory.create_for_python_type(_RoundTripPoint, converter=converter)
    converter.register_logical_type(lt)

    point = _RoundTripPoint(x=10, y=20)
    storage_value = lt.python_to_storage(point, converter)
    assert storage_value == {"x": 10, "y": 20}

    reconstructed = lt.storage_to_python(storage_value, converter)
    assert isinstance(reconstructed, _RoundTripPoint)
    assert reconstructed.x == 10
    assert reconstructed.y == 20


def test_pydantic_with_uuid_round_trip():
    """Round-trip a pydantic model with a UUID field."""
    from orcapod.extension_types.pydantic_logical_type_factory import PydanticLogicalTypeFactory

    converter = _make_full_converter()
    factory = PydanticLogicalTypeFactory()
    lt = factory.create_for_python_type(_RoundTripRecord, converter=converter)
    converter.register_logical_type(lt)

    u = _uuid_module.UUID("12345678-1234-5678-1234-567812345678")
    record = _RoundTripRecord(record_id=u, label="hello")

    storage_value = lt.python_to_storage(record, converter)
    assert storage_value["label"] == "hello"
    assert storage_value["record_id"] == u.bytes

    reconstructed = lt.storage_to_python(storage_value, converter)
    assert isinstance(reconstructed, _RoundTripRecord)
    assert reconstructed.record_id == u
    assert reconstructed.label == "hello"


# ── Parquet integration test ──────────────────────────────────────────────────

def test_nested_pydantic_model_parquet_roundtrip(tmp_path):
    """Fresh-process Parquet round-trip for a two-level nested pydantic model.

    Verifies that register_discovered_extensions triggers the chain:
      register_arrow_extension("Outer") -> reconstruct_from_arrow
        -> register_python_class(Inner) -> registers Inner
    so that storage_to_python can reconstruct the full nested object.
    """
    import pyarrow.parquet as pq
    from orcapod.extension_types.database_hooks import register_discovered_extensions, apply_extension_types

    # ── Write path ───────────────────────────────────────────────────────────
    write_converter = _make_full_converter()

    inner = _InnerModel(value=42)
    outer = _OuterModel(inner=inner, label="hello")

    write_converter.register_python_class(_OuterModel)

    arrow_schema = write_converter.python_schema_to_arrow_schema({"item": _OuterModel})
    rows = [{"item": outer}]
    table = write_converter.python_dicts_to_arrow_table(rows, arrow_schema=arrow_schema)

    parquet_path = tmp_path / "nested_pydantic.parquet"
    pq.write_table(table, parquet_path)

    # ── Read path (fresh converter — neither Inner nor Outer pre-registered) ──
    read_converter = _make_full_converter()
    read_table = pq.read_table(parquet_path)

    register_discovered_extensions(read_converter, read_table.schema)
    read_table = apply_extension_types(read_table, read_converter._logical_type_registry)

    assert read_converter._logical_type_registry.get_by_python_type(_OuterModel) is not None
    assert read_converter._logical_type_registry.get_by_python_type(_InnerModel) is not None

    rows_out = read_converter.arrow_table_to_python_dicts(read_table)
    assert len(rows_out) == 1
    reconstructed = rows_out[0]["item"]
    assert isinstance(reconstructed, _OuterModel)
    assert isinstance(reconstructed.inner, _InnerModel)
    assert reconstructed.inner.value == 42
    assert reconstructed.label == "hello"


# ── typing.Literal support tests (ITL-442) ───────────────────────────────────


def test_factory_create_model_with_literal_str_field():
    """Literal["a", "b"] field → large_string in the Arrow struct."""
    from orcapod.extension_types.pydantic_logical_type_factory import PydanticLogicalTypeFactory

    factory = PydanticLogicalTypeFactory()
    converter = _make_full_converter()
    lt = factory.create_for_python_type(_LiteralStrModel, converter=converter)

    storage = lt.get_arrow_extension_type().storage_type
    assert storage.field("method").type == pa.large_string()


def test_factory_create_model_with_literal_int_field():
    """Literal[1, 2, 3] field → int64 in the Arrow struct."""
    from orcapod.extension_types.pydantic_logical_type_factory import PydanticLogicalTypeFactory

    factory = PydanticLogicalTypeFactory()
    converter = _make_full_converter()
    lt = factory.create_for_python_type(_LiteralIntModel, converter=converter)

    storage = lt.get_arrow_extension_type().storage_type
    assert storage.field("count").type == pa.int64()


def test_factory_create_model_with_literal_none_field():
    """Literal["active", None] strips None → resolves to large_string."""
    from orcapod.extension_types.pydantic_logical_type_factory import PydanticLogicalTypeFactory

    factory = PydanticLogicalTypeFactory()
    converter = _make_full_converter()
    lt = factory.create_for_python_type(_LiteralNoneModel, converter=converter)

    storage = lt.get_arrow_extension_type().storage_type
    assert storage.field("status").type == pa.large_string()


def test_factory_rejects_literal_none_only():
    """Literal[None] has no concrete value type — raises ValueError."""
    from orcapod.extension_types.pydantic_logical_type_factory import PydanticLogicalTypeFactory

    factory = PydanticLogicalTypeFactory()
    converter = _make_full_converter()
    with pytest.raises(ValueError, match="Literal\\[None\\]"):
        factory.create_for_python_type(_LiteralNoneOnlyModel, converter=converter)


def test_factory_rejects_mixed_literal():
    """Literal["a", 1] mixes str and int — raises ValueError."""
    from orcapod.extension_types.pydantic_logical_type_factory import PydanticLogicalTypeFactory

    factory = PydanticLogicalTypeFactory()
    converter = _make_full_converter()
    with pytest.raises(ValueError, match="Mixed-type Literal"):
        factory.create_for_python_type(_MixedLiteralModel, converter=converter)


def test_literal_model_round_trip():
    """python_to_storage → storage_to_python round-trip for a model with Literal fields."""
    from orcapod.extension_types.pydantic_logical_type_factory import PydanticLogicalTypeFactory

    factory = PydanticLogicalTypeFactory()
    converter = _make_full_converter()
    lt = factory.create_for_python_type(_LiteralRoundTripModel, converter=converter)
    converter.register_logical_type(lt)

    instance = _LiteralRoundTripModel(method="a", count=42)
    storage_value = lt.python_to_storage(instance, converter)
    assert storage_value == {"method": "a", "count": 42}

    reconstructed = lt.storage_to_python(storage_value, converter)
    assert isinstance(reconstructed, _LiteralRoundTripModel)
    assert reconstructed.method == "a"
    assert reconstructed.count == 42
