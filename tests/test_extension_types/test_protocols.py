"""Tests for ExtensionTypeConverter protocol."""

from __future__ import annotations

import pyarrow as pa

from orcapod.extension_types.protocols import ExtensionTypeConverter


class _StubConverter:
    """Minimal conforming implementation of ExtensionTypeConverter for use in tests."""

    @property
    def extension_name(self) -> str:
        return "test.module.MyType"

    @property
    def extension_metadata(self) -> bytes | None:
        return b"test.category"

    @property
    def storage_type(self) -> pa.DataType:
        return pa.large_string()

    @property
    def python_type(self) -> type:
        return str

    def python_to_storage(self, value):
        return str(value)

    def storage_to_python(self, storage_value):
        return storage_value


def test_protocol_is_importable():
    """ExtensionTypeConverter can be imported from extension_types.protocols."""
    assert ExtensionTypeConverter is not None


def test_protocol_defines_required_members():
    """A conforming class is recognized as an ExtensionTypeConverter instance."""
    assert isinstance(_StubConverter(), ExtensionTypeConverter)


def test_conforming_class_satisfies_protocol():
    """A class implementing all required members works correctly via the protocol interface."""
    converter: ExtensionTypeConverter = _StubConverter()
    assert converter.extension_name == "test.module.MyType"
    assert converter.extension_metadata == b"test.category"
    assert converter.storage_type == pa.large_string()
    assert converter.python_type is str
    assert converter.python_to_storage(42) == "42"
    assert converter.storage_to_python("hello") == "hello"


