from unittest.mock import Mock

import pytest

from orcapod.semantic_types import semantic_registry


def test_registry_initialization():
    registry = semantic_registry.SemanticTypeRegistry()
    assert registry.list_semantic_types() == []
    assert registry.list_python_types() == []
    assert registry.list_struct_signatures() == []


def test_register_and_retrieve_converter():
    registry = semantic_registry.SemanticTypeRegistry()
    python_type = Mock(name="PythonType")
    struct_type = Mock(name="StructType")
    converter = Mock()
    converter.python_type = python_type
    converter.arrow_struct_type = struct_type
    registry.register_converter("mock_type", converter)
    # Retrieve by semantic type name
    assert registry.get_converter_for_semantic_type("mock_type") is converter
    # Retrieve by python type
    assert registry.get_converter_for_python_type(python_type) is converter
    # Retrieve by struct signature
    assert registry.get_converter_for_struct_signature(struct_type) is converter


def test_register_duplicate_semantic_type_raises():
    registry = semantic_registry.SemanticTypeRegistry()
    python_type = Mock(name="PythonType")
    struct_type = Mock(name="StructType")
    converter1 = Mock()
    converter1.python_type = python_type
    converter1.arrow_struct_type = struct_type
    registry.register_converter("mock_type", converter1)
    converter2 = Mock()
    converter2.python_type = python_type
    converter2.arrow_struct_type = struct_type
    with pytest.raises(ValueError):
        registry.register_converter("mock_type", converter2)


def test_register_conflicting_python_type_raises():
    registry = semantic_registry.SemanticTypeRegistry()
    python_type = Mock(name="PythonType")
    struct_type1 = Mock(name="StructType1")
    struct_type2 = Mock(name="StructType2")
    converter1 = Mock()
    converter1.python_type = python_type
    converter1.arrow_struct_type = struct_type1
    registry.register_converter("mock_type1", converter1)
    converter2 = Mock()
    converter2.python_type = python_type
    converter2.arrow_struct_type = struct_type2
    with pytest.raises(ValueError):
        registry.register_converter("mock_type2", converter2)


def test_register_conflicting_struct_signature_raises():
    registry = semantic_registry.SemanticTypeRegistry()
    python_type1 = Mock(name="PythonType1")
    python_type2 = Mock(name="PythonType2")
    struct_type = Mock(name="StructType")
    converter1 = Mock()
    converter1.python_type = python_type1
    converter1.arrow_struct_type = struct_type
    registry.register_converter("mock_type1", converter1)
    converter2 = Mock()
    converter2.python_type = python_type2
    converter2.arrow_struct_type = struct_type
    with pytest.raises(ValueError):
        registry.register_converter("mock_type2", converter2)


def test_get_nonexistent_returns_none():
    registry = semantic_registry.SemanticTypeRegistry()
    python_type = Mock(name="PythonType")
    struct_type = Mock(name="StructType")
    assert registry.get_converter_for_semantic_type("not_present") is None
    assert registry.get_converter_for_python_type(python_type) is None
    assert registry.get_converter_for_struct_signature(struct_type) is None


def test_list_registered_types():
    registry = semantic_registry.SemanticTypeRegistry()
    python_type1 = Mock(name="PythonType1")
    struct_type1 = Mock(name="StructType1")
    converter1 = Mock()
    converter1.python_type = python_type1
    converter1.arrow_struct_type = struct_type1
    registry.register_converter("mock_type1", converter1)

    python_type2 = Mock(name="PythonType2")
    struct_type2 = Mock(name="StructType2")
    converter2 = Mock()
    converter2.python_type = python_type2
    converter2.arrow_struct_type = struct_type2
    registry.register_converter("mock_type2", converter2)

    assert set(registry.list_semantic_types()) == {"mock_type1", "mock_type2"}
    assert set(registry.list_python_types()) == {python_type1, python_type2}
    assert set(registry.list_struct_signatures()) == {struct_type1, struct_type2}


def test_has_methods():
    registry = semantic_registry.SemanticTypeRegistry()
    python_type = Mock(name="PythonType")
    struct_type = Mock(name="StructType")
    converter = Mock()
    converter.python_type = python_type
    converter.arrow_struct_type = struct_type
    registry.register_converter("mock_type", converter)
    assert registry.has_semantic_type("mock_type")
    assert registry.has_python_type(python_type)
    assert registry.has_semantic_struct_signature(struct_type)


def test_integration_with_converter():
    registry = semantic_registry.SemanticTypeRegistry()
    python_type = Mock(name="PythonType")
    struct_type = Mock(name="StructType")
    converter = Mock()
    converter.python_type = python_type
    converter.arrow_struct_type = struct_type
    registry.register_converter("mock_type", converter)
    retrieved = registry.get_converter_for_semantic_type("mock_type")
    assert retrieved is converter


# Comprehensive unregister tests for future implementation
# Uncomment when unregister methods are implemented
#
# def test_unregister_by_semantic_type_name():
#     registry = semantic_registry.SemanticTypeRegistry()
#     python_type = Mock(name="PythonType")
#     struct_type = Mock(name="StructType")
#     converter = Mock()
#     converter.python_type = python_type
#     converter.arrow_struct_type = struct_type
#     registry.register_converter("mock_type", converter)
#     result = registry.unregister_by_semantic_type_name("mock_type")
#     assert result == {"mock_type": converter}
#     assert not registry.has_semantic_type("mock_type")
#     assert not registry.has_python_type(python_type)
#     assert not registry.has_semantic_struct_signature(struct_type)
#     assert registry.get_converter_for_semantic_type("mock_type") is None
#     assert registry.get_converter_for_python_type(python_type) is None
#     assert registry.get_converter_for_struct_signature(struct_type) is None
#
# def test_unregister_by_converter():
#     registry = semantic_registry.SemanticTypeRegistry()
#     python_type = Mock(name="PythonType")
#     struct_type = Mock(name="StructType")
#     converter = Mock()
#     converter.python_type = python_type
#     converter.arrow_struct_type = struct_type
#     registry.register_converter("mock_type", converter)
#     result = registry.unregister_by_converter(converter)
#     assert result == {"mock_type": converter}
#     assert not registry.has_semantic_type("mock_type")
#     assert not registry.has_python_type(python_type)
#     assert not registry.has_semantic_struct_signature(struct_type)
#     assert registry.get_converter_for_semantic_type("mock_type") is None
#     assert registry.get_converter_for_python_type(python_type) is None
#     assert registry.get_converter_for_struct_signature(struct_type) is None
#
# def test_unregister_by_python_type():
#     registry = semantic_registry.SemanticTypeRegistry()
#     python_type = Mock(name="PythonType")
#     struct_type = Mock(name="StructType")
#     converter = Mock()
#     converter.python_type = python_type
#     converter.arrow_struct_type = struct_type
#     registry.register_converter("mock_type", converter)
#     result = registry.unregister_by_python_type(python_type)
#     assert result == {"mock_type": converter}
#     assert not registry.has_semantic_type("mock_type")
#     assert not registry.has_python_type(python_type)
#     assert not registry.has_semantic_struct_signature(struct_type)
#     assert registry.get_converter_for_semantic_type("mock_type") is None
#     assert registry.get_converter_for_python_type(python_type) is None
#     assert registry.get_converter_for_struct_signature(struct_type) is None
#
# def test_unregister_by_struct_signature():
#     registry = semantic_registry.SemanticTypeRegistry()
#     python_type = Mock(name="PythonType")
#     struct_type = Mock(name="StructType")
#     converter = Mock()
#     converter.python_type = python_type
#     converter.arrow_struct_type = struct_type
#     registry.register_converter("mock_type", converter)
#     result = registry.unregister_by_struct_signature(struct_type)
#     assert result == {"mock_type": converter}
#     assert not registry.has_semantic_type("mock_type")
#     assert not registry.has_python_type(python_type)
#     assert not registry.has_semantic_struct_signature(struct_type)
#     assert registry.get_converter_for_semantic_type("mock_type") is None
#     assert registry.get_converter_for_python_type(python_type) is None
#     assert registry.get_converter_for_struct_signature(struct_type) is None
