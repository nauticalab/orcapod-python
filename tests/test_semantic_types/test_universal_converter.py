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
