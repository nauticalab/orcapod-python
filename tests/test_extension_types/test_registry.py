"""Tests for ExtensionTypeRegistry."""

from __future__ import annotations

import uuid

import pyarrow as pa
import pytest

from orcapod.extension_types.protocols import ExtensionTypeConverter
from orcapod.extension_types.registry import ExtensionTypeRegistry


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _unique_name() -> str:
    """Unique extension name to avoid cross-test global-registry collisions."""
    return f"test.registry.{uuid.uuid4().hex[:8]}"


def _make_stub(
    name: str | None = None,
    storage: pa.DataType | None = None,
    metadata: bytes | None = b"test.category",
    py_type: type = str,
) -> ExtensionTypeConverter:
    """Factory for minimal ExtensionTypeConverter conforming stubs."""
    _name = name or _unique_name()
    _storage = storage if storage is not None else pa.large_utf8()
    _metadata = metadata
    _py_type = py_type

    class _Stub:
        @property
        def extension_name(self) -> str:
            return _name

        @property
        def extension_metadata(self) -> bytes | None:
            return _metadata

        @property
        def storage_type(self) -> pa.DataType:
            return _storage

        @property
        def python_type(self) -> type:
            return _py_type

        def python_to_storage(self, value):
            return str(value)

        def storage_to_python(self, storage_value):
            return storage_value

    return _Stub()


# ---------------------------------------------------------------------------
# Pure-Python registry tests (no PA/Polars global state required)
# ---------------------------------------------------------------------------

def test_register_stores_converter():
    registry = ExtensionTypeRegistry()
    conv = _make_stub()
    registry.register(conv)
    assert registry.get_converter_for_name(conv.extension_name) is conv


def test_register_duplicate_raises():
    registry = ExtensionTypeRegistry()
    name = _unique_name()
    registry.register(_make_stub(name=name))
    with pytest.raises(ValueError, match=name):
        registry.register(_make_stub(name=name))


def test_get_converter_for_name_miss():
    registry = ExtensionTypeRegistry()
    assert registry.get_converter_for_name("does.not.exist") is None


def test_get_converter_for_python_type_exact():
    registry = ExtensionTypeRegistry()
    conv = _make_stub(py_type=bytes)
    registry.register(conv)
    assert registry.get_converter_for_python_type(bytes) is conv


def test_get_converter_for_python_type_subclass():
    class _Base:
        pass

    class _Child(_Base):
        pass

    registry = ExtensionTypeRegistry()
    conv = _make_stub(py_type=_Base)
    registry.register(conv)
    assert registry.get_converter_for_python_type(_Child) is conv


def test_get_converter_for_python_type_miss():
    registry = ExtensionTypeRegistry()
    assert registry.get_converter_for_python_type(int) is None


def test_has_extension_name():
    registry = ExtensionTypeRegistry()
    conv = _make_stub()
    assert not registry.has_extension_name(conv.extension_name)
    registry.register(conv)
    assert registry.has_extension_name(conv.extension_name)


def test_has_python_type():
    registry = ExtensionTypeRegistry()
    conv = _make_stub(py_type=float)
    assert not registry.has_python_type(float)
    registry.register(conv)
    assert registry.has_python_type(float)


def test_list_extension_names():
    registry = ExtensionTypeRegistry()
    a = _make_stub()
    b = _make_stub()
    registry.register(a)
    registry.register(b)
    assert registry.list_extension_names() == [a.extension_name, b.extension_name]


def test_list_python_types():
    registry = ExtensionTypeRegistry()
    a = _make_stub(py_type=bytes)
    b = _make_stub(py_type=float)
    registry.register(a)
    registry.register(b)
    assert registry.list_python_types() == [bytes, float]
