"""Tests for DataclassLogicalType and DataclassLogicalTypeFactory."""

from __future__ import annotations

import dataclasses
import uuid as _uuid_module

import pyarrow as pa
import pytest


def _identity(cls: type) -> str:
    """Return the identity string (logical type name) of a user class (ITL-881)."""
    from orcapod.contexts import get_default_context
    from orcapod.types import Schema

    converter = get_default_context().type_converter
    converter.ensure_types_registered_for_schemas(Schema({"value": cls}))
    lt = converter.get_logical_type(cls)
    assert lt is not None
    return lt.logical_type_name


def _metadata(cls: type) -> dict[str, object]:
    """Return the parsed Arrow extension metadata of a user class (ITL-881)."""
    import json

    from orcapod.contexts import get_default_context
    from orcapod.types import Schema

    converter = get_default_context().type_converter
    converter.ensure_types_registered_for_schemas(Schema({"value": cls}))
    lt = converter.get_logical_type(cls)
    assert lt is not None
    return json.loads(lt.get_arrow_extension_type().__arrow_ext_serialize__())


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
    from orcapod.logical_types.dataclass_logical_type_factory import (
        DataclassLogicalType,
    )

    assert DataclassLogicalType is not None


def test_dataclass_logical_type_protocol_conformance():
    from orcapod.logical_types.dataclass_logical_type_factory import (
        DataclassLogicalType,
    )
    from orcapod.logical_types.protocols import LogicalTypeProtocol

    @dataclasses.dataclass
    class _MyDC:
        name: str
        count: int

    storage = pa.struct(
        [pa.field("name", pa.large_string()), pa.field("count", pa.int64())]
    )
    field_annotations = [("name", str), ("count", int)]
    lt = DataclassLogicalType(
        logical_name="tests.MyDC",
        python_type=_MyDC,
        storage_type=storage,
        field_annotations=field_annotations,
    )
    assert isinstance(lt, LogicalTypeProtocol)


def test_dataclass_logical_type_python_to_storage():
    from orcapod.logical_types.dataclass_logical_type_factory import (
        DataclassLogicalType,
    )

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
    from orcapod.logical_types.dataclass_logical_type_factory import (
        DataclassLogicalType,
    )

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
    from orcapod.logical_types.dataclass_logical_type_factory import (
        DataclassLogicalType,
    )

    @dataclasses.dataclass
    class _Foo:
        val: str

    storage = pa.struct([pa.field("val", pa.large_string())])
    lt = DataclassLogicalType("mymod.Foo", _Foo, storage, [("val", str)])
    assert lt.logical_type_name == "mymod.Foo"


def test_dataclass_logical_type_python_type():
    from orcapod.logical_types.dataclass_logical_type_factory import (
        DataclassLogicalType,
    )

    @dataclasses.dataclass
    class _Bar:
        val: str

    storage = pa.struct([pa.field("val", pa.large_string())])
    lt = DataclassLogicalType("mymod.Bar", _Bar, storage, [("val", str)])
    assert lt.python_type is _Bar


# ── DataclassLogicalTypeFactory helpers ──────────────────────────────────────────


def _make_full_converter():
    """Make a UniversalTypeConverter with builtin types + DataclassLogicalTypeFactory."""
    from orcapod.logical_types.builtin_logical_types import (
        LogicalPath,
        LogicalUUID,
        LogicalUPath,
    )
    from orcapod.logical_types.registry import LogicalTypeRegistry
    from orcapod.logical_types.dataclass_logical_type_factory import (
        DataclassLogicalTypeFactory,
        DATACLASS_CATEGORY,
    )
    from orcapod.semantic_types.universal_converter import UniversalTypeConverter

    registry = LogicalTypeRegistry(
        logical_types=[LogicalPath(), LogicalUUID(), LogicalUPath()]
    )
    factory = DataclassLogicalTypeFactory()
    registry.register_logical_type_factory(
        factory, category=DATACLASS_CATEGORY, python_bases=[object]
    )
    return UniversalTypeConverter(logical_type_registry=registry)


# ── DataclassLogicalTypeFactory write-path tests ─────────────────────────────────


def test_factory_supports_class_dataclass():
    from orcapod.logical_types.dataclass_logical_type_factory import (
        DataclassLogicalTypeFactory,
    )

    @dataclasses.dataclass
    class _Dummy:
        x: int

    factory = DataclassLogicalTypeFactory()
    assert factory.supports_class(_Dummy) is True


def test_factory_supports_class_non_dataclass():
    from orcapod.logical_types.dataclass_logical_type_factory import (
        DataclassLogicalTypeFactory,
    )

    factory = DataclassLogicalTypeFactory()
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


@dataclasses.dataclass
class _InnerForRegistrationTest:
    """Module-level inner dataclass for registration completeness test."""

    value: int


@dataclasses.dataclass
class _OuterForRegistrationTest:
    """Module-level outer dataclass for registration completeness test."""

    inner: _InnerForRegistrationTest
    label: str


# ── Module-level dataclasses for list[dataclass[dataclass]] round-trip test ──


@dataclasses.dataclass
class _ListItemDC:
    """Inner dataclass used as element type in list[_ListItemDC] field."""

    x: int
    y: int


@dataclasses.dataclass
class _ListContainerDC:
    """Outer dataclass with a list[_ListItemDC] field."""

    items: list[_ListItemDC]
    label: str


def test_factory_create_flat_dataclass():
    from orcapod.logical_types.dataclass_logical_type_factory import (
        DataclassLogicalTypeFactory,
        DataclassLogicalType,
    )

    factory = DataclassLogicalTypeFactory()
    converter = _make_full_converter()
    lt = factory.create_for_python_type(_Flat, converter=converter)

    assert isinstance(lt, DataclassLogicalType)
    storage = lt.get_arrow_extension_type().storage_type
    assert pa.types.is_struct(storage)
    assert storage.field("name").type == pa.large_string()
    assert storage.field("count").type == pa.int64()


def test_factory_create_dataclass_with_uuid_field():
    """UUID field → plain storage type (large_binary) in the struct, not extension type.

    ``pa.Table.from_pylist`` (and Polars dtype inference) cannot handle a struct
    whose fields are ``pa.ExtensionType`` nodes.  ``DataclassLogicalTypeFactory`` strips
    extension types from struct field types so that Arrow array construction works.
    The UUID's extension type (``orcapod.uuid``) is still registered and used for
    value conversion; only the struct field schema uses the stripped storage type.
    """
    from orcapod.logical_types.dataclass_logical_type_factory import (
        DataclassLogicalTypeFactory,
    )

    factory = DataclassLogicalTypeFactory()
    converter = _make_full_converter()
    lt = factory.create_for_python_type(_WithUUID, converter=converter)

    storage = lt.get_arrow_extension_type().storage_type
    id_field_type = storage.field("id").type
    # Stripped to plain storage type — NOT an extension type in the struct.
    assert id_field_type == pa.large_binary()
    assert not isinstance(id_field_type, pa.ExtensionType)


def test_factory_create_dataclass_with_list_field():
    from orcapod.logical_types.dataclass_logical_type_factory import (
        DataclassLogicalTypeFactory,
    )

    factory = DataclassLogicalTypeFactory()
    converter = _make_full_converter()
    lt = factory.create_for_python_type(_WithList, converter=converter)

    storage = lt.get_arrow_extension_type().storage_type
    assert pa.types.is_large_list(storage.field("tags").type)
    assert storage.field("tags").type.value_type == pa.large_string()


def test_factory_create_dataclass_with_dict_field():
    from orcapod.logical_types.dataclass_logical_type_factory import (
        DataclassLogicalTypeFactory,
    )

    factory = DataclassLogicalTypeFactory()
    converter = _make_full_converter()
    lt = factory.create_for_python_type(_WithDict, converter=converter)

    storage = lt.get_arrow_extension_type().storage_type
    meta_type = storage.field("meta").type
    assert pa.types.is_large_list(meta_type)
    assert pa.types.is_struct(meta_type.value_type)
    field_names = {
        meta_type.value_type.field(i).name
        for i in range(meta_type.value_type.num_fields)
    }
    assert field_names == {"key", "value"}


def test_factory_rejects_local_class():
    from orcapod.logical_types.dataclass_logical_type_factory import (
        DataclassLogicalTypeFactory,
    )

    def _make_local():
        @dataclasses.dataclass
        class _Local:
            x: int

        return _Local

    LocalClass = _make_local()
    factory = DataclassLogicalTypeFactory()
    converter = _make_full_converter()
    with pytest.raises(ValueError, match="local"):
        factory.create_for_python_type(LocalClass, converter=converter)


def test_register_python_class_dispatches_to_dataclass_factory():
    """register_python_class on a dataclass triggers DataclassLogicalTypeFactory."""
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
    from orcapod.logical_types.dataclass_logical_type_factory import (
        DataclassLogicalTypeFactory,
        DataclassLogicalType,
    )

    storage = pa.struct([pa.field("x", pa.int64()), pa.field("y", pa.int64())])
    metadata = _metadata(_RoundTripPoint)
    type_name = _identity(_RoundTripPoint)

    factory = DataclassLogicalTypeFactory()
    converter = _make_full_converter()
    lt = factory.reconstruct_from_arrow(
        type_name, storage, metadata, converter=converter
    )

    assert isinstance(lt, DataclassLogicalType)
    assert lt.python_type is _RoundTripPoint
    assert lt.logical_type_name == type_name


def test_factory_reconstruct_from_arrow_invalid_fqcn():
    """ImportError if the FQCN cannot be resolved."""
    from orcapod.logical_types.dataclass_logical_type_factory import (
        DataclassLogicalTypeFactory,
    )

    storage = pa.struct([pa.field("x", pa.int64())])
    factory = DataclassLogicalTypeFactory()
    converter = _make_full_converter()

    import orcapod.logical_types.user_class_identity as uci

    record = uci.identity_record(
        "dataclass", "nonexistent.module.NoSuchClass", [("x", "int")]
    )
    with pytest.raises(ImportError):
        factory.reconstruct_from_arrow(
            uci.identity_string(record),
            storage,
            {"category": "orcapod.dataclass", "identity": record},
            converter,
        )


def test_reconstruct_from_arrow_registers_nested_types():
    """reconstruct_from_arrow for Outer must register Inner as a side effect."""
    from orcapod.logical_types.dataclass_logical_type_factory import (
        DataclassLogicalTypeFactory,
    )

    # Build the storage type for _OuterForRegistrationTest manually (as it would come
    # from Parquet): outer struct with an inner struct field (Inner is stored as a struct,
    # NOT as an extension type inside the struct field — that's the ET1 constraint).
    inner_storage = pa.struct([pa.field("value", pa.int64())])
    outer_storage = pa.struct(
        [
            pa.field("inner", inner_storage),
            pa.field("label", pa.large_string()),
        ]
    )
    outer_type_name = _identity(_OuterForRegistrationTest)

    factory = DataclassLogicalTypeFactory()
    converter = _make_full_converter()

    # Inner is NOT pre-registered
    assert (
        converter._logical_type_registry.get_by_python_type(_InnerForRegistrationTest)
        is None
    )

    # reconstruct_from_arrow for Outer should trigger registration of Inner as a side effect
    factory.reconstruct_from_arrow(
        outer_type_name, outer_storage, _metadata(_OuterForRegistrationTest), converter
    )

    # Inner must now be registered
    assert (
        converter._logical_type_registry.get_by_python_type(_InnerForRegistrationTest)
        is not None
    )


def test_dataclass_python_to_storage_round_trip():
    """python_to_storage → storage_to_python returns an equivalent dataclass."""
    converter = _make_full_converter()

    from orcapod.logical_types.dataclass_logical_type_factory import (
        DataclassLogicalTypeFactory,
    )

    factory = DataclassLogicalTypeFactory()
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
    from orcapod.logical_types.dataclass_logical_type_factory import (
        DataclassLogicalTypeFactory,
    )

    converter = _make_full_converter()
    factory = DataclassLogicalTypeFactory()
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


# ── nested class lookup on read ───────────────────────────────────────────────


@dataclasses.dataclass
class _OuterForNestedTest:
    """Module-level outer class for testing the read-side lookup of a nested class."""

    @dataclasses.dataclass
    class Inner:
        x: int
        y: str


def test_reconstruct_finds_class_nested_in_a_class():
    """The read path imports a class nested in another class by its dotted name."""
    from orcapod.logical_types.dataclass_logical_type_factory import (
        DataclassLogicalTypeFactory,
    )

    import orcapod.logical_types.user_class_identity as uci

    inner = _OuterForNestedTest.Inner
    # Build the stored identity by hand, so that the class is not registered and the
    # read can only find it by importing its dotted name.
    name = f"{inner.__module__}.{inner.__qualname__}"
    record = uci.identity_record("dataclass", name, [("x", "int"), ("y", "str")])
    storage = pa.struct([pa.field("x", pa.int64()), pa.field("y", pa.large_string())])
    lt = DataclassLogicalTypeFactory().reconstruct_from_arrow(
        uci.identity_string(record),
        storage,
        {"category": "orcapod.dataclass", "identity": record},
        _make_full_converter(),
    )
    assert lt.python_type is inner


def test_python_to_storage_raises_when_converter_none():
    """DataclassLogicalType.python_to_storage raises ValueError when converter is None."""
    from orcapod.logical_types.dataclass_logical_type_factory import (
        DataclassLogicalType,
    )

    @dataclasses.dataclass
    class _DC:
        x: int

    storage = pa.struct([pa.field("x", pa.int64())])
    lt = DataclassLogicalType("mymod._DC", _DC, storage, [("x", int)])
    with pytest.raises(ValueError, match="converter"):
        lt.python_to_storage(_DC(x=1), None)


def test_storage_to_python_raises_when_converter_none():
    """DataclassLogicalType.storage_to_python raises ValueError when converter is None."""
    from orcapod.logical_types.dataclass_logical_type_factory import (
        DataclassLogicalType,
    )

    @dataclasses.dataclass
    class _DC:
        x: int

    storage = pa.struct([pa.field("x", pa.int64())])
    lt = DataclassLogicalType("mymod._DC2", _DC, storage, [("x", int)])
    with pytest.raises(ValueError, match="converter"):
        lt.storage_to_python({"x": 1}, None)


def test_nested_dataclass_parquet_roundtrip(tmp_path):
    """Fresh-process Parquet round-trip for a two-level nested dataclass.

    Verifies that register_discovered_logical_types triggers the chain:
      register_logical_type_from_arrow_metadata("Outer") -> reconstruct_from_arrow
        -> register_python_class(Inner) -> registers Inner
    so that storage_to_python can reconstruct the full nested object.
    """
    import pyarrow.parquet as pq
    from orcapod.logical_types.database_hooks import (
        register_discovered_logical_types,
        apply_logical_types,
    )

    # ── Write path ───────────────────────────────────────────────────────────
    write_converter = _make_full_converter()

    inner = _InnerForRegistrationTest(value=42)
    outer = _OuterForRegistrationTest(inner=inner, label="hello")

    # Register Outer (which also registers Inner via create_for_python_type)
    write_converter.register_python_class(_OuterForRegistrationTest)

    # Serialise: python_schema_to_arrow_schema gives the column-level Arrow schema
    # (with extension types at the top level); python_dicts_to_arrow_table converts rows.
    arrow_schema = write_converter.python_schema_to_arrow_schema(
        {"item": _OuterForRegistrationTest}
    )
    rows = [{"item": outer}]
    table = write_converter.python_dicts_to_arrow_table(rows, arrow_schema=arrow_schema)

    parquet_path = tmp_path / "nested.parquet"
    pq.write_table(table, parquet_path)

    # ── Read path (fresh converter — neither Inner nor Outer pre-registered) ──
    read_converter = _make_full_converter()
    read_table = pq.read_table(parquet_path)

    # register_discovered_logical_types triggers: Outer -> reconstruct_from_arrow
    # -> register_python_class(Inner) -> registers Inner
    register_discovered_logical_types(read_converter, read_table.schema)
    read_table = apply_logical_types(read_table, read_converter._logical_type_registry)

    # Both types must now be registered
    assert (
        read_converter._logical_type_registry.get_by_python_type(
            _OuterForRegistrationTest
        )
        is not None
    )
    assert (
        read_converter._logical_type_registry.get_by_python_type(
            _InnerForRegistrationTest
        )
        is not None
    )

    # Convert back to Python and verify full nested object
    rows_out = read_converter.arrow_table_to_python_dicts(read_table)
    assert len(rows_out) == 1
    reconstructed = rows_out[0]["item"]
    assert isinstance(reconstructed, _OuterForRegistrationTest)
    assert isinstance(reconstructed.inner, _InnerForRegistrationTest)
    assert reconstructed.inner.value == 42
    assert reconstructed.label == "hello"


def test_list_of_nested_dataclass_parquet_roundtrip(tmp_path):
    """Parquet round-trip for a dataclass whose field is list[AnotherDataclass].

    Verifies that registering a dataclass that contains a list[T] field where T is
    itself a logical type (a dataclass) correctly creates a ListLogicalType and
    round-trips through Parquet without data loss.
    """
    import pyarrow.parquet as pq
    from orcapod.logical_types.database_hooks import (
        register_discovered_logical_types,
        apply_logical_types,
    )

    # ── Write path ───────────────────────────────────────────────────────────
    write_converter = _make_full_converter()

    items = [_ListItemDC(x=1, y=2), _ListItemDC(x=3, y=4)]
    container = _ListContainerDC(items=items, label="test")

    # This raises ValueError currently: list[_ListItemDC] contains a logical type
    # (_ListItemDC is a dataclass → extension type) in a list value field position.
    write_converter.register_python_class(_ListContainerDC)

    arrow_schema = write_converter.python_schema_to_arrow_schema(
        {"record": _ListContainerDC}
    )
    rows = [{"record": container}]
    table = write_converter.python_dicts_to_arrow_table(rows, arrow_schema=arrow_schema)

    parquet_path = tmp_path / "list_nested.parquet"
    pq.write_table(table, parquet_path)

    # ── Read path (fresh converter) ──────────────────────────────────────────
    read_converter = _make_full_converter()
    read_table = pq.read_table(parquet_path)
    register_discovered_logical_types(read_converter, read_table.schema)
    read_table = apply_logical_types(read_table, read_converter._logical_type_registry)

    rows_out = read_converter.arrow_table_to_python_dicts(read_table)
    assert len(rows_out) == 1
    reconstructed = rows_out[0]["record"]
    assert isinstance(reconstructed, _ListContainerDC)
    assert len(reconstructed.items) == 2
    assert isinstance(reconstructed.items[0], _ListItemDC)
    assert reconstructed.items[0].x == 1
    assert reconstructed.items[1].y == 4
    assert reconstructed.label == "test"


def test_reconstruct_rejects_a_record_that_does_not_match_the_extension_name():
    """A stored record must hash to the stored extension name (ITL-881)."""
    from orcapod.errors import TypeIdentityError
    from orcapod.logical_types.dataclass_logical_type_factory import (
        DataclassLogicalTypeFactory,
    )

    # This test is about the factory's read entry point: an inconsistent stored
    # column cannot be produced through the public interface.
    metadata = _metadata(_Flat)
    stored = metadata["identity"]
    assert isinstance(stored, dict)
    record = {**stored, "fields": [["name", "str"], ["count", "str"]]}
    storage = pa.struct(
        [pa.field("name", pa.large_string()), pa.field("count", pa.int64())]
    )
    with pytest.raises(TypeIdentityError, match="invalid identity record"):
        DataclassLogicalTypeFactory().reconstruct_from_arrow(
            _identity(_Flat),
            storage,
            {"category": metadata["category"], "identity": record},
            _make_full_converter(),
        )
