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
    """Protocol defines all six required members."""
    required = {
        "extension_name",
        "extension_metadata",
        "storage_type",
        "python_type",
        "python_to_storage",
        "storage_to_python",
    }
    for member in required:
        assert hasattr(ExtensionTypeConverter, member), f"Protocol missing member: {member}"


def test_conforming_class_satisfies_protocol():
    """A class implementing all required members works correctly via the protocol interface."""
    converter: ExtensionTypeConverter = _StubConverter()
    assert converter.extension_name == "test.module.MyType"
    assert converter.extension_metadata == b"test.category"
    assert converter.storage_type == pa.large_string()
    assert converter.python_type is str
    assert converter.python_to_storage(42) == "42"
    assert converter.storage_to_python("hello") == "hello"


def test_extension_metadata_can_be_none():
    """extension_metadata is allowed to be None — it is bytes | None."""

    class NullMetadataConverter:
        @property
        def extension_name(self) -> str:
            return "test.NullMeta"

        @property
        def extension_metadata(self) -> bytes | None:
            return None

        @property
        def storage_type(self) -> pa.DataType:
            return pa.binary(16)

        @property
        def python_type(self) -> type:
            return bytes

        def python_to_storage(self, value):
            return value

        def storage_to_python(self, storage_value):
            return storage_value

    converter: ExtensionTypeConverter = NullMetadataConverter()
    assert converter.extension_metadata is None


def test_storage_type_not_constrained_to_struct():
    """storage_type accepts any pa.DataType — primitive types are valid, not only struct."""

    class BinaryConverter:
        @property
        def extension_name(self) -> str:
            return "uuid.UUID"

        @property
        def extension_metadata(self) -> bytes | None:
            return b"orcapod.builtin"

        @property
        def storage_type(self) -> pa.DataType:
            return pa.binary(16)

        @property
        def python_type(self) -> type:
            return bytes

        def python_to_storage(self, value):
            return value

        def storage_to_python(self, storage_value):
            return bytes(storage_value)

    converter: ExtensionTypeConverter = BinaryConverter()
    assert converter.storage_type == pa.binary(16)
    assert not pa.types.is_struct(converter.storage_type)


def test_protocol_does_not_include_old_members():
    """The new protocol must not define hashing or struct-dispatch members from the old protocol."""
    excluded = {
        "hash_struct_dict",
        "hasher_id",
        "can_handle_python_type",
        "can_handle_struct_type",
        "arrow_struct_type",
    }
    for member in excluded:
        assert not hasattr(ExtensionTypeConverter, member), (
            f"ExtensionTypeConverter must not define '{member}' — "
            "hashing and struct-shape dispatch are not part of the new protocol"
        )
