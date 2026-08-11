"""Tests for ListLogicalType and ListLogicalTypeFactory."""
from __future__ import annotations

import uuid as uuid_module

import pyarrow as pa
import pytest


# ── Helpers ───────────────────────────────────────────────────────────────────


def _uuid_ext_type() -> pa.ExtensionType:
    """Return the registered orcapod.uuid extension type."""
    from orcapod.logical_types.builtin_logical_types import LogicalUUID
    return LogicalUUID().get_arrow_extension_type()


def _logical_uuid():
    """Return a ``LogicalUUID`` instance."""
    from orcapod.logical_types.builtin_logical_types import LogicalUUID
    return LogicalUUID()


class _StubConverter:
    """Minimal converter stub delegating UUID and list[UUID] conversions."""

    def python_to_storage(self, value, annotation):
        if annotation is uuid_module.UUID:
            return value.bytes
        if hasattr(annotation, "__origin__") and annotation.__origin__ is list:
            import typing
            args = typing.get_args(annotation)
            return [self.python_to_storage(item, args[0]) for item in value]
        return value

    def storage_to_python(self, storage_value, annotation):
        if annotation is uuid_module.UUID:
            return uuid_module.UUID(bytes=bytes(storage_value))
        if hasattr(annotation, "__origin__") and annotation.__origin__ is list:
            import typing
            args = typing.get_args(annotation)
            return [self.storage_to_python(item, args[0]) for item in storage_value]
        return storage_value

    def register_python_class(self, annotation):
        if annotation is uuid_module.UUID:
            return _uuid_ext_type()
        if annotation is str:
            return pa.large_string()
        if annotation is int:
            return pa.int64()
        raise ValueError(f"Unsupported annotation: {annotation}")

    def register_logical_type_from_arrow_metadata(self, ext_name, metadata_bytes, storage_type):
        if ext_name == "orcapod.uuid":
            return _uuid_ext_type()
        raise ValueError(f"Unknown extension: {ext_name}")

    def arrow_type_to_python_type(self, arrow_type):
        if hasattr(arrow_type, "extension_name"):
            if arrow_type.extension_name == "orcapod.uuid":
                return uuid_module.UUID
        return type(None)

    def get_logical_type_by_arrow_name(self, arrow_ext_name):
        if arrow_ext_name == "orcapod.uuid":
            return _logical_uuid()
        return None


# ── ListLogicalType unit tests ────────────────────────────────────────────────


def test_list_logical_type_importable():
    from orcapod.logical_types.list_logical_type_factory import ListLogicalType
    assert ListLogicalType is not None


def test_list_logical_type_name():
    from orcapod.logical_types.list_logical_type_factory import ListLogicalType
    lt = ListLogicalType(_logical_uuid(), is_set=False)
    assert lt.logical_type_name == "list[orcapod.uuid]"


def test_set_logical_type_name():
    from orcapod.logical_types.list_logical_type_factory import ListLogicalType
    lt = ListLogicalType(_logical_uuid(), is_set=True)
    assert lt.logical_type_name == "set[orcapod.uuid]"


def test_list_logical_type_python_type():
    from orcapod.logical_types.list_logical_type_factory import ListLogicalType
    lt = ListLogicalType(_logical_uuid(), is_set=False)
    assert lt.python_type == list[uuid_module.UUID]


def test_set_logical_type_python_type():
    from orcapod.logical_types.list_logical_type_factory import ListLogicalType
    lt = ListLogicalType(_logical_uuid(), is_set=True)
    assert lt.python_type == set[uuid_module.UUID]


def test_list_logical_type_arrow_extension_type_name():
    from orcapod.logical_types.list_logical_type_factory import ListLogicalType
    lt = ListLogicalType(_logical_uuid(), is_set=False)
    ext = lt.get_arrow_extension_type()
    assert hasattr(ext, "extension_name")
    assert ext.extension_name == "list[orcapod.uuid]"


def test_list_logical_type_storage_is_large_list_of_large_binary():
    from orcapod.logical_types.list_logical_type_factory import ListLogicalType
    lt = ListLogicalType(_logical_uuid(), is_set=False)
    ext = lt.get_arrow_extension_type()
    assert pa.types.is_large_list(ext.storage_type)
    assert ext.storage_type.value_type == pa.large_binary()


def test_list_logical_type_storage_is_et1_safe():
    """Value type of list storage must NOT be a pa.ExtensionType (ET1 invariant)."""
    from orcapod.logical_types.list_logical_type_factory import ListLogicalType
    lt = ListLogicalType(_logical_uuid(), is_set=False)
    ext = lt.get_arrow_extension_type()
    assert not isinstance(ext.storage_type.value_type, pa.ExtensionType), (
        "ET1 violation: list value type must not be an ExtensionType"
    )


def test_list_logical_type_arrow_extension_type_cached():
    from orcapod.logical_types.list_logical_type_factory import ListLogicalType
    lt = ListLogicalType(_logical_uuid(), is_set=False)
    assert lt.get_arrow_extension_type() is lt.get_arrow_extension_type()


def test_list_logical_type_index_element():
    from orcapod.logical_types.list_logical_type_factory import ListLogicalType
    lt = ListLogicalType(_logical_uuid(), is_set=False)
    assert lt.index_element() == uuid_module.UUID


def test_list_logical_type_python_to_storage():
    from orcapod.logical_types.list_logical_type_factory import ListLogicalType
    lt = ListLogicalType(_logical_uuid(), is_set=False)
    u1 = uuid_module.UUID("12345678-1234-5678-1234-567812345678")
    u2 = uuid_module.UUID("87654321-4321-8765-4321-876543218765")
    result = lt.python_to_storage([u1, u2], _StubConverter())
    assert result == [u1.bytes, u2.bytes]


def test_list_logical_type_python_to_storage_none():
    from orcapod.logical_types.list_logical_type_factory import ListLogicalType
    lt = ListLogicalType(_logical_uuid(), is_set=False)
    assert lt.python_to_storage(None, _StubConverter()) == []


def test_list_logical_type_storage_to_python():
    from orcapod.logical_types.list_logical_type_factory import ListLogicalType
    lt = ListLogicalType(_logical_uuid(), is_set=False)
    u1 = uuid_module.UUID("12345678-1234-5678-1234-567812345678")
    result = lt.storage_to_python([u1.bytes], _StubConverter())
    assert result == [u1]
    assert isinstance(result, list)


def test_set_logical_type_storage_to_python_returns_set():
    from orcapod.logical_types.list_logical_type_factory import ListLogicalType
    lt = ListLogicalType(_logical_uuid(), is_set=True)
    u1 = uuid_module.UUID("12345678-1234-5678-1234-567812345678")
    result = lt.storage_to_python([u1.bytes], _StubConverter())
    assert isinstance(result, set)
    assert result == {u1}


def test_set_logical_type_python_to_storage_is_sorted():
    """python_to_storage for set[UUID] must produce a deterministically sorted list.

    Python sets have nondeterministic iteration order. The storage representation
    must always be sorted so that two sets with identical elements produce identical
    Arrow storage bytes (required for content-hash stability).
    """
    from orcapod.logical_types.list_logical_type_factory import ListLogicalType
    lt = ListLogicalType(_logical_uuid(), is_set=True)
    u1 = uuid_module.UUID("12345678-1234-5678-1234-567812345678")
    u2 = uuid_module.UUID("87654321-4321-8765-4321-876543218765")
    u3 = uuid_module.UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
    # Pass as a set — iteration order is nondeterministic; output must be sorted.
    result = lt.python_to_storage({u3, u1, u2}, _StubConverter())
    assert len(result) == 3
    assert result == sorted(result), "storage values must be in sorted order for determinism"


def test_list_logical_type_metadata_contains_element_ext_name():
    """Extension metadata must contain element_ext_name for reconstruction."""
    import json
    from orcapod.logical_types.list_logical_type_factory import ListLogicalType, LIST_CATEGORY
    lt = ListLogicalType(_logical_uuid(), is_set=False)
    ext = lt.get_arrow_extension_type()
    meta = json.loads(ext.__arrow_ext_serialize__().decode("utf-8"))
    assert meta["category"] == LIST_CATEGORY
    assert meta["element_ext_name"] == "orcapod.uuid"
    assert "element_ext_metadata" in meta


def test_list_logical_type_protocol_conformance():
    from orcapod.logical_types.list_logical_type_factory import ListLogicalType
    from orcapod.logical_types.protocols import LogicalTypeProtocol
    lt = ListLogicalType(_logical_uuid(), is_set=False)
    assert isinstance(lt, LogicalTypeProtocol)


# ── ListLogicalTypeFactory tests ──────────────────────────────────────────────


def test_list_logical_type_factory_importable():
    from orcapod.logical_types.list_logical_type_factory import ListLogicalTypeFactory
    assert ListLogicalTypeFactory is not None


def test_list_logical_type_factory_reconstruct_list_of_uuid():
    """reconstruct_from_arrow produces ListLogicalType(uuid.UUID, …, is_set=False)."""
    from orcapod.logical_types.list_logical_type_factory import (
        ListLogicalTypeFactory,
        LIST_CATEGORY,
    )
    factory = ListLogicalTypeFactory()
    storage_type = pa.large_list(pa.large_binary())
    metadata = {
        "category": LIST_CATEGORY,
        "element_ext_name": "orcapod.uuid",
        "element_ext_metadata": None,
    }
    lt = factory.reconstruct_from_arrow(
        "list[orcapod.uuid]", storage_type, metadata, _StubConverter()
    )
    assert lt.logical_type_name == "list[orcapod.uuid]"
    assert lt.python_type == list[uuid_module.UUID]


def test_list_logical_type_factory_reconstruct_set_of_uuid():
    """reconstruct_from_arrow produces ListLogicalType(uuid.UUID, …, is_set=True)."""
    from orcapod.logical_types.list_logical_type_factory import (
        ListLogicalTypeFactory,
        SET_CATEGORY,
    )
    factory = ListLogicalTypeFactory()
    storage_type = pa.large_list(pa.large_binary())
    metadata = {
        "category": SET_CATEGORY,
        "element_ext_name": "orcapod.uuid",
        "element_ext_metadata": None,
    }
    lt = factory.reconstruct_from_arrow(
        "set[orcapod.uuid]", storage_type, metadata, _StubConverter()
    )
    assert lt.logical_type_name == "set[orcapod.uuid]"
    assert lt.python_type == set[uuid_module.UUID]


def test_list_logical_type_factory_reconstruct_raises_on_non_list_storage():
    from orcapod.logical_types.list_logical_type_factory import (
        ListLogicalTypeFactory,
        LIST_CATEGORY,
    )
    factory = ListLogicalTypeFactory()
    metadata = {"category": LIST_CATEGORY, "element_ext_name": "orcapod.uuid", "element_ext_metadata": None}
    with pytest.raises(ValueError, match="list storage"):
        factory.reconstruct_from_arrow(
            "list[orcapod.uuid]", pa.large_binary(), metadata, _StubConverter()
        )


def test_list_logical_type_factory_reconstruct_raises_on_missing_element_ext_name():
    from orcapod.logical_types.list_logical_type_factory import (
        ListLogicalTypeFactory,
        LIST_CATEGORY,
    )
    factory = ListLogicalTypeFactory()
    metadata = {"category": LIST_CATEGORY}
    with pytest.raises(ValueError, match="element_ext_name"):
        factory.reconstruct_from_arrow(
            "list[orcapod.uuid]", pa.large_list(pa.large_binary()), metadata, _StubConverter()
        )
