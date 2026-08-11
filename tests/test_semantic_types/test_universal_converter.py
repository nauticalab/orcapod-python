"""Tests for UniversalTypeConverter list[T] / set[T] extension type support."""
from __future__ import annotations


def test_register_python_class_list_of_uuid_returns_extension_type():
    """register_python_class(list[uuid.UUID]) must return a pa.ExtensionType, not raise."""
    import uuid
    import pyarrow as pa
    from orcapod.contexts import create_registry

    converter = create_registry().get_context().type_converter
    result = converter.register_python_class(list[uuid.UUID])

    assert isinstance(result, pa.ExtensionType), (
        f"Expected pa.ExtensionType, got {type(result)}: {result!r}"
    )
    assert result.extension_name == "list[orcapod.uuid]"


def test_register_python_class_set_of_uuid_returns_extension_type():
    """register_python_class(set[uuid.UUID]) must return a pa.ExtensionType."""
    import uuid
    import pyarrow as pa
    from orcapod.contexts import create_registry

    converter = create_registry().get_context().type_converter
    result = converter.register_python_class(set[uuid.UUID])

    assert isinstance(result, pa.ExtensionType)
    assert result.extension_name == "set[orcapod.uuid]"


def test_register_python_class_list_of_int_unchanged():
    """register_python_class(list[int]) must still return plain large_list(int64)."""
    import pyarrow as pa
    from orcapod.contexts import create_registry

    converter = create_registry().get_context().type_converter
    result = converter.register_python_class(list[int])

    assert not isinstance(result, pa.ExtensionType)
    assert pa.types.is_large_list(result)
    assert result.value_type == pa.int64()


def test_register_python_class_list_of_uuid_idempotent():
    """Calling register_python_class(list[uuid.UUID]) twice returns same ext type name."""
    import uuid
    from orcapod.contexts import create_registry

    converter = create_registry().get_context().type_converter
    result1 = converter.register_python_class(list[uuid.UUID])
    result2 = converter.register_python_class(list[uuid.UUID])
    assert result1.extension_name == result2.extension_name


def test_python_type_to_arrow_type_list_of_uuid_returns_extension_type():
    """python_type_to_arrow_type(list[UUID]) must return the list[orcapod.uuid] ext type."""
    import uuid
    import pyarrow as pa
    from orcapod.contexts import create_registry

    converter = create_registry().get_context().type_converter
    # Pre-register so the type is in the registry before python_type_to_arrow_type is called.
    converter.register_python_class(list[uuid.UUID])
    result = converter.python_type_to_arrow_type(list[uuid.UUID])

    assert isinstance(result, pa.ExtensionType), (
        f"Expected pa.ExtensionType, got {type(result)}: {result!r}"
    )
    assert result.extension_name == "list[orcapod.uuid]"


def test_python_type_to_arrow_type_list_of_uuid_without_prior_registration():
    """python_type_to_arrow_type(list[UUID]) must work even without prior register_python_class."""
    import uuid
    import pyarrow as pa
    from orcapod.contexts import create_registry

    # Fresh converter — ListLogicalType not yet registered
    converter = create_registry().get_context().type_converter
    result = converter.python_type_to_arrow_type(list[uuid.UUID])

    assert isinstance(result, pa.ExtensionType)
    assert result.extension_name == "list[orcapod.uuid]"


def test_arrow_schema_to_python_schema_round_trip_list_of_uuid():
    """Schema round-trip: list[UUID] → Arrow ext → python schema → Arrow ext (same type)."""
    import uuid
    import pyarrow as pa
    from orcapod.contexts import create_registry

    converter = create_registry().get_context().type_converter
    python_schema = {"ids": list[uuid.UUID]}
    arrow_schema = converter.python_schema_to_arrow_schema(python_schema)

    # Arrow schema has list[orcapod.uuid] extension type
    assert arrow_schema.field("ids").type.extension_name == "list[orcapod.uuid]"

    # Recover Python schema
    recovered = converter.arrow_schema_to_python_schema(arrow_schema)
    assert recovered["ids"] == list[uuid.UUID]

    # Re-derive Arrow schema — must be identical
    arrow_schema2 = converter.python_schema_to_arrow_schema(recovered)
    assert arrow_schema2.field("ids").type.extension_name == "list[orcapod.uuid]"


def test_value_converter_list_of_uuid_produces_bytes_list():
    """get_python_to_arrow_converter(list[UUID]) converts [uuid, uuid] → [bytes, bytes]."""
    import uuid
    from orcapod.contexts import create_registry

    converter = create_registry().get_context().type_converter
    converter.register_python_class(list[uuid.UUID])

    conv_fn = converter.get_python_to_arrow_converter(list[uuid.UUID])
    u1 = uuid.UUID("12345678-1234-5678-1234-567812345678")
    u2 = uuid.UUID("87654321-4321-8765-4321-876543218765")
    result = conv_fn([u1, u2])

    assert result == [u1.bytes, u2.bytes]


def test_value_converter_set_of_uuid_produces_bytes_list():
    """get_python_to_arrow_converter(set[UUID]) converts {uuid} → [bytes]."""
    import uuid
    from orcapod.contexts import create_registry

    converter = create_registry().get_context().type_converter
    converter.register_python_class(set[uuid.UUID])

    conv_fn = converter.get_python_to_arrow_converter(set[uuid.UUID])
    u1 = uuid.UUID("12345678-1234-5678-1234-567812345678")
    result = conv_fn({u1})

    assert result == [u1.bytes]
