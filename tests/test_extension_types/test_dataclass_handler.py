"""Tests for DataclassLogicalType and DataclassHandlerFactory (PLT-1657)."""

from __future__ import annotations

import dataclasses
import json
import uuid

import pyarrow as pa
import polars as pl
import pytest

from orcapod.extension_types.protocols import LogicalTypeProtocol, ResolutionContext


# ---------------------------------------------------------------------------
# Shared dataclass fixtures (module-level so Arrow extension names are stable)
# ---------------------------------------------------------------------------

@dataclasses.dataclass
class Flat:
    x: int
    y: str


@dataclasses.dataclass
class AllPrimitives:
    i: int
    f: float
    s: str
    b: bool
    by: bytes


@dataclasses.dataclass
class WithList:
    items: list[int]


@dataclasses.dataclass
class Inner:
    a: int


@dataclasses.dataclass
class Outer:
    inner: Inner
    z: str


# Cyclic fixtures — must be module-level so get_type_hints resolves the string
# annotations ('_SelfRef', '_IndirectB', '_IndirectA') in module globals.

@dataclasses.dataclass
class _SelfRef:
    value: int
    child: _SelfRef  # type: ignore[name-defined]  # PEP 563 → string; resolved at get_type_hints time


@dataclasses.dataclass
class _IndirectA:
    value: int
    b: _IndirectB  # type: ignore[name-defined]  # forward ref; _IndirectB defined below


@dataclasses.dataclass
class _IndirectB:
    a: _IndirectA


# ---------------------------------------------------------------------------
# DataclassLogicalType — unit tests
# ---------------------------------------------------------------------------

def _make_flat_lt():
    """Construct a DataclassLogicalType for Flat without using the full factory."""
    from orcapod.extension_types.dataclass_handler import DataclassLogicalType
    storage = pa.struct([pa.field("x", pa.int64()), pa.field("y", pa.large_string())])
    identity = lambda v: v
    field_converters = [("x", identity, identity), ("y", identity, identity)]
    fqcn = f"{Flat.__module__}.{Flat.__qualname__}"
    return DataclassLogicalType(fqcn, Flat, storage, field_converters)


def test_dataclass_logical_type_satisfies_protocol():
    lt = _make_flat_lt()
    assert isinstance(lt, LogicalTypeProtocol)


def test_dataclass_logical_type_logical_name():
    lt = _make_flat_lt()
    expected = f"{Flat.__module__}.{Flat.__qualname__}"
    assert lt.logical_type_name == expected


def test_dataclass_logical_type_python_type():
    lt = _make_flat_lt()
    assert lt.python_type is Flat


def test_dataclass_logical_type_get_arrow_extension_type():
    lt = _make_flat_lt()
    ext = lt.get_arrow_extension_type()
    assert isinstance(ext, pa.ExtensionType)
    assert ext.extension_name == lt.logical_type_name
    expected_storage = pa.struct([pa.field("x", pa.int64()), pa.field("y", pa.large_string())])
    assert ext.storage_type == expected_storage


def test_dataclass_logical_type_get_arrow_extension_type_cached():
    lt = _make_flat_lt()
    ext1 = lt.get_arrow_extension_type()
    ext2 = lt.get_arrow_extension_type()
    assert ext1 is ext2


def test_dataclass_logical_type_get_polars_extension_type():
    lt = _make_flat_lt()
    polars_ext = lt.get_polars_extension_type()
    assert isinstance(polars_ext, pl.BaseExtension)


def test_dataclass_logical_type_get_polars_extension_type_cached():
    lt = _make_flat_lt()
    p1 = lt.get_polars_extension_type()
    p2 = lt.get_polars_extension_type()
    assert p1 is p2


def test_dataclass_logical_type_arrow_metadata_contains_category():
    lt = _make_flat_lt()
    ext = lt.get_arrow_extension_type()
    meta = json.loads(ext.__arrow_ext_serialize__().decode("utf-8"))
    assert meta["category"] == "orcapod.dataclass"


def test_dataclass_logical_type_python_to_storage():
    lt = _make_flat_lt()
    result = lt.python_to_storage(Flat(x=7, y="hello"))
    assert result == {"x": 7, "y": "hello"}


def test_dataclass_logical_type_storage_to_python():
    lt = _make_flat_lt()
    result = lt.storage_to_python({"x": 7, "y": "hello"})
    assert result == Flat(x=7, y="hello")
    assert isinstance(result, Flat)


# ---------------------------------------------------------------------------
# DataclassHandlerFactory — protocol conformance
# ---------------------------------------------------------------------------

def test_handler_factory_satisfies_protocol():
    from orcapod.extension_types.protocols import LogicalTypeFactoryProtocol
    from orcapod.extension_types.dataclass_handler import DataclassHandlerFactory
    assert isinstance(DataclassHandlerFactory(), LogicalTypeFactoryProtocol)


def test_handler_factory_supports_class_true_for_dataclass():
    from orcapod.extension_types.dataclass_handler import DataclassHandlerFactory
    factory = DataclassHandlerFactory()
    assert factory.supports_class(Flat) is True


def test_handler_factory_supports_class_false_for_plain_class():
    from orcapod.extension_types.dataclass_handler import DataclassHandlerFactory
    class _Plain:
        pass
    factory = DataclassHandlerFactory()
    assert factory.supports_class(_Plain) is False


# ---------------------------------------------------------------------------
# Write path — flat dataclass with primitives
# ---------------------------------------------------------------------------

def test_create_for_python_type_logical_name():
    from orcapod.extension_types.dataclass_handler import DataclassHandlerFactory
    factory = DataclassHandlerFactory()
    lt = factory.create_for_python_type(Flat)
    assert lt.logical_type_name == f"{Flat.__module__}.{Flat.__qualname__}"


def test_create_for_python_type_python_type():
    from orcapod.extension_types.dataclass_handler import DataclassHandlerFactory
    factory = DataclassHandlerFactory()
    lt = factory.create_for_python_type(Flat)
    assert lt.python_type is Flat


def test_create_for_python_type_arrow_struct_layout():
    from orcapod.extension_types.dataclass_handler import DataclassHandlerFactory
    factory = DataclassHandlerFactory()
    lt = factory.create_for_python_type(Flat)
    expected = pa.struct([pa.field("x", pa.int64()), pa.field("y", pa.large_string())])
    assert lt.get_arrow_extension_type().storage_type == expected


def test_create_for_python_type_not_dataclass_raises():
    from orcapod.extension_types.dataclass_handler import DataclassHandlerFactory
    factory = DataclassHandlerFactory()
    with pytest.raises(ValueError, match="not a dataclass"):
        factory.create_for_python_type(str)


def test_all_primitives_round_trip():
    from orcapod.extension_types.dataclass_handler import DataclassHandlerFactory
    factory = DataclassHandlerFactory()
    lt = factory.create_for_python_type(AllPrimitives)
    original = AllPrimitives(i=1, f=2.5, s="hi", b=True, by=b"\x00\x01")
    storage = lt.python_to_storage(original)
    assert storage == {"i": 1, "f": 2.5, "s": "hi", "b": True, "by": b"\x00\x01"}
    reconstructed = lt.storage_to_python(storage)
    assert reconstructed == original


def test_all_primitives_arrow_types():
    from orcapod.extension_types.dataclass_handler import DataclassHandlerFactory
    factory = DataclassHandlerFactory()
    lt = factory.create_for_python_type(AllPrimitives)
    ext = lt.get_arrow_extension_type()
    struct_type = ext.storage_type
    assert struct_type.field("i").type == pa.int64()
    assert struct_type.field("f").type == pa.float64()
    assert struct_type.field("s").type == pa.large_string()
    assert struct_type.field("b").type == pa.bool_()
    assert struct_type.field("by").type == pa.large_binary()


# ---------------------------------------------------------------------------
# Write path — list[T] fields
# ---------------------------------------------------------------------------

def test_list_int_field_arrow_type():
    from orcapod.extension_types.dataclass_handler import DataclassHandlerFactory
    factory = DataclassHandlerFactory()
    lt = factory.create_for_python_type(WithList)
    struct_type = lt.get_arrow_extension_type().storage_type
    assert struct_type.field("items").type == pa.list_(pa.int64())


def test_list_int_field_round_trip():
    from orcapod.extension_types.dataclass_handler import DataclassHandlerFactory
    factory = DataclassHandlerFactory()
    lt = factory.create_for_python_type(WithList)
    original = WithList(items=[1, 2, 3])
    storage = lt.python_to_storage(original)
    assert storage == {"items": [1, 2, 3]}
    reconstructed = lt.storage_to_python(storage)
    assert reconstructed == original


# ---------------------------------------------------------------------------
# Write path — nested dataclass
# ---------------------------------------------------------------------------

def test_nested_dataclass_arrow_type():
    from orcapod.extension_types.dataclass_handler import DataclassHandlerFactory
    factory = DataclassHandlerFactory()
    lt = factory.create_for_python_type(Outer)
    struct_type = lt.get_arrow_extension_type().storage_type
    # inner field should be a plain struct, not an extension type
    inner_field_type = struct_type.field("inner").type
    assert inner_field_type == pa.struct([pa.field("a", pa.int64())])
    assert struct_type.field("z").type == pa.large_string()


def test_nested_dataclass_round_trip():
    from orcapod.extension_types.dataclass_handler import DataclassHandlerFactory
    factory = DataclassHandlerFactory()
    lt = factory.create_for_python_type(Outer)
    original = Outer(inner=Inner(a=42), z="world")
    storage = lt.python_to_storage(original)
    assert storage == {"inner": {"a": 42}, "z": "world"}
    reconstructed = lt.storage_to_python(storage)
    assert reconstructed == original


def test_nested_dataclass_registers_inner_in_registry():
    from orcapod.extension_types.dataclass_handler import DataclassHandlerFactory
    from orcapod.extension_types.registry import LogicalTypeRegistry
    factory = DataclassHandlerFactory()
    registry = LogicalTypeRegistry()
    lt = factory.create_for_python_type(Outer, registry=registry)
    registry.register_logical_type(lt)
    # Inner was registered as a side effect
    inner_lt = registry.get_by_python_type(Inner)
    assert inner_lt is not None
    assert inner_lt.python_type is Inner


# ---------------------------------------------------------------------------
# Cycle detection (write path)
# ---------------------------------------------------------------------------

def test_self_referential_dataclass_raises_type_error():
    """A dataclass with a self-referential field raises TypeError."""
    from orcapod.extension_types.dataclass_handler import DataclassHandlerFactory
    factory = DataclassHandlerFactory()
    with pytest.raises(TypeError, match="[Cc]ircular"):
        factory.create_for_python_type(_SelfRef)


def test_indirect_cycle_raises_type_error():
    """An A → B → A cycle raises TypeError."""
    from orcapod.extension_types.dataclass_handler import DataclassHandlerFactory
    factory = DataclassHandlerFactory()
    with pytest.raises(TypeError, match="[Cc]ircular"):
        factory.create_for_python_type(_IndirectA)


# ---------------------------------------------------------------------------
# Unsupported field types
# ---------------------------------------------------------------------------

# NOTE: We use uuid.UUID here because `uuid` is imported at module level.
# With `from __future__ import annotations`, `u: uuid.UUID` becomes the string
# `'uuid.UUID'`, which get_type_hints resolves via the module's globals where
# `uuid` IS present. Using pathlib.Path would fail unless pathlib is also
# imported at module level.

def test_unsupported_field_type_raises_type_error():
    """A field annotated with an unsupported type (uuid.UUID) raises TypeError."""
    from orcapod.extension_types.dataclass_handler import DataclassHandlerFactory

    @dataclasses.dataclass
    class _Bad:
        u: uuid.UUID

    factory = DataclassHandlerFactory()
    with pytest.raises(TypeError, match="[Uu]nsupported"):
        factory.create_for_python_type(_Bad)


def test_unsupported_field_type_error_mentions_annotation():
    """TypeError message names the unsupported annotation."""
    from orcapod.extension_types.dataclass_handler import DataclassHandlerFactory

    @dataclasses.dataclass
    class _Bad:
        u: uuid.UUID

    factory = DataclassHandlerFactory()
    with pytest.raises(TypeError) as exc_info:
        factory.create_for_python_type(_Bad)
    assert "UUID" in str(exc_info.value)


# ---------------------------------------------------------------------------
# Read path — reconstruct_from_arrow
# ---------------------------------------------------------------------------

def test_reconstruct_from_arrow_returns_dataclass_logical_type():
    from orcapod.extension_types.dataclass_handler import DataclassHandlerFactory, DataclassLogicalType
    factory = DataclassHandlerFactory()
    fqcn = f"{Flat.__module__}.{Flat.__qualname__}"
    storage = pa.struct([pa.field("x", pa.int64()), pa.field("y", pa.large_string())])
    lt = factory.reconstruct_from_arrow(fqcn, storage, {"category": "orcapod.dataclass"})
    assert isinstance(lt, DataclassLogicalType)
    assert lt.python_type is Flat
    assert lt.logical_type_name == fqcn


def test_reconstruct_from_arrow_converters_work():
    from orcapod.extension_types.dataclass_handler import DataclassHandlerFactory
    factory = DataclassHandlerFactory()
    fqcn = f"{Flat.__module__}.{Flat.__qualname__}"
    storage = pa.struct([pa.field("x", pa.int64()), pa.field("y", pa.large_string())])
    lt = factory.reconstruct_from_arrow(fqcn, storage, {"category": "orcapod.dataclass"})
    original = Flat(x=5, y="test")
    assert lt.storage_to_python(lt.python_to_storage(original)) == original


def test_reconstruct_from_arrow_uses_schema_storage_type():
    """reconstruct_from_arrow uses the storage_type from the schema, not re-derived."""
    from orcapod.extension_types.dataclass_handler import DataclassHandlerFactory
    factory = DataclassHandlerFactory()
    fqcn = f"{Flat.__module__}.{Flat.__qualname__}"
    # Intentionally use storage_type from the schema
    storage = pa.struct([pa.field("x", pa.int64()), pa.field("y", pa.large_string())])
    lt = factory.reconstruct_from_arrow(fqcn, storage, {"category": "orcapod.dataclass"})
    assert lt.get_arrow_extension_type().storage_type == storage


def test_reconstruct_from_arrow_bad_module_raises_value_error():
    from orcapod.extension_types.dataclass_handler import DataclassHandlerFactory
    factory = DataclassHandlerFactory()
    storage = pa.struct([pa.field("x", pa.int64())])
    with pytest.raises(ValueError, match="[Cc]annot import"):
        factory.reconstruct_from_arrow(
            "no.such.module.Foo", storage, {"category": "orcapod.dataclass"}
        )


def test_reconstruct_from_arrow_bad_class_name_raises_value_error():
    from orcapod.extension_types.dataclass_handler import DataclassHandlerFactory
    # Use a valid module but nonexistent class
    factory = DataclassHandlerFactory()
    storage = pa.struct([pa.field("x", pa.int64())])
    with pytest.raises(ValueError, match="[Cc]annot find"):
        factory.reconstruct_from_arrow(
            "builtins.NoSuchClass", storage, {"category": "orcapod.dataclass"}
        )


def test_reconstruct_from_arrow_non_dataclass_raises_value_error():
    from orcapod.extension_types.dataclass_handler import DataclassHandlerFactory
    factory = DataclassHandlerFactory()
    # str is a builtin — definitely not a dataclass
    storage = pa.struct([pa.field("x", pa.int64())])
    with pytest.raises(ValueError, match="not a.*dataclass"):
        factory.reconstruct_from_arrow(
            "builtins.str", storage, {"category": "orcapod.dataclass"}
        )


def test_reconstruct_from_arrow_no_dot_in_fqcn_raises_value_error():
    from orcapod.extension_types.dataclass_handler import DataclassHandlerFactory
    factory = DataclassHandlerFactory()
    storage = pa.struct([])
    with pytest.raises(ValueError, match="no module separator"):
        factory.reconstruct_from_arrow(
            "NoDotInName", storage, {"category": "orcapod.dataclass"}
        )


def test_reconstruct_from_arrow_cycle_detection():
    from orcapod.extension_types.dataclass_handler import DataclassHandlerFactory
    factory = DataclassHandlerFactory()
    fqcn = f"{Flat.__module__}.{Flat.__qualname__}"
    storage = pa.struct([pa.field("x", pa.int64()), pa.field("y", pa.large_string())])
    ctx = ResolutionContext(visited_arrow_names=frozenset({fqcn}))
    with pytest.raises(ValueError, match="[Cc]ircular"):
        factory.reconstruct_from_arrow(
            fqcn, storage, {"category": "orcapod.dataclass"}, context=ctx
        )
