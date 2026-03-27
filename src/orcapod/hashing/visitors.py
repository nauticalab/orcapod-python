"""
SUGGESTED FILE: src/orcapod/hashing/visitors.py

Generic visitor pattern for traversing Arrow types and data simultaneously.

This provides a base visitor class that can be extended for various processing needs
like semantic hashing, validation, data cleaning, etc.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from orcapod.semantic_types.semantic_registry import SemanticTypeRegistry
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")


class ArrowTypeDataVisitor(ABC):
    """
    Base visitor for traversing Arrow types and data simultaneously.

    This enables processing that needs to transform both the Arrow schema
    and the corresponding data in a single pass.
    """

    @abstractmethod
    def visit_struct(
        self, struct_type: "pa.StructType", data: dict | None
    ) -> tuple["pa.DataType", Any]:
        """Visit a struct type with its data"""
        pass

    @abstractmethod
    def visit_list(
        self, list_type: "pa.ListType", data: list | None
    ) -> tuple["pa.DataType", Any]:
        """Visit a list type with its data"""
        pass

    @abstractmethod
    def visit_map(
        self, map_type: "pa.MapType", data: dict | None
    ) -> tuple["pa.DataType", Any]:
        """Visit a map type with its data"""
        pass

    @abstractmethod
    def visit_primitive(
        self, primitive_type: "pa.DataType", data: Any
    ) -> tuple["pa.DataType", Any]:
        """Visit a primitive type with its data"""
        pass

    def visit(self, arrow_type: "pa.DataType", data: Any) -> tuple["pa.DataType", Any]:
        """
        Main dispatch method that routes to appropriate visit method.

        Args:
            arrow_type: Arrow data type to process
            data: Corresponding data value

        Returns:
            Tuple of (new_arrow_type, new_data)
        """
        if pa.types.is_struct(arrow_type):
            return self.visit_struct(arrow_type, data)
        elif pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
            return self.visit_list(arrow_type, data)
        elif pa.types.is_fixed_size_list(arrow_type):
            # Treat fixed-size lists like regular lists for processing
            return self.visit_list(arrow_type, data)
        elif pa.types.is_map(arrow_type):
            return self.visit_map(arrow_type, data)
        else:
            return self.visit_primitive(arrow_type, data)

    def _visit_struct_fields(
        self, struct_type: "pa.StructType", data: dict | None
    ) -> tuple["pa.StructType", dict]:
        """
        Helper method to recursively process struct fields.

        This is the default behavior for regular (non-semantic) structs.
        """
        if data is None:
            return struct_type, None

        new_fields = []
        new_data = {}

        for field in struct_type:
            field_data = data.get(field.name)
            new_field_type, new_field_data = self.visit(field.type, field_data)

            new_fields.append(pa.field(field.name, new_field_type))
            new_data[field.name] = new_field_data

        return pa.struct(new_fields), new_data

    def _visit_list_elements(
        self, list_type: "pa.ListType", data: list | None
    ) -> tuple["pa.DataType", list]:
        """
        Helper method to recursively process list elements.

        This is the default behavior for lists.
        """
        if data is None:
            return list_type, None

        element_type = list_type.value_type
        processed_elements = []
        new_element_type = None

        for item in data:
            current_element_type, processed_item = self.visit(element_type, item)
            processed_elements.append(processed_item)

            # Use the first non-None element to determine new element type
            if new_element_type is None:
                new_element_type = current_element_type

        # If list was empty or all None, keep original element type
        if new_element_type is None:
            new_element_type = element_type

        # Create appropriate list type based on original type
        if pa.types.is_large_list(list_type):
            return pa.large_list(new_element_type), processed_elements
        elif pa.types.is_fixed_size_list(list_type):
            return pa.list_(new_element_type, list_type.list_size), processed_elements
        else:
            return pa.list_(new_element_type), processed_elements


class SemanticHashingError(Exception):
    """Exception raised when semantic hashing fails"""

    pass


class SemanticHashingVisitor(ArrowTypeDataVisitor):
    """
    Visitor that replaces semantic types with their hash strings.

    This visitor traverses Arrow type structures and data simultaneously,
    identifying semantic types by their struct signatures and replacing
    them with hash strings computed by their respective converters.
    """

    def __init__(self, semantic_registry: SemanticTypeRegistry):
        """
        Initialize the semantic hashing visitor.

        Args:
            semantic_registry: Registry containing semantic type converters
        """
        self.registry = semantic_registry
        self._current_field_path: list[str] = []

    def visit_struct(
        self, struct_type: "pa.StructType", data: dict | None
    ) -> tuple["pa.DataType", Any]:
        """
        Visit a struct type, checking if it's a semantic type.

        If the struct is a semantic type (recognized by signature), replace it
        with a hash string. Otherwise, recursively process its fields.
        """
        if data is None:
            return struct_type, None

        # Check if this struct IS a semantic type by signature recognition
        converter = self.registry.get_converter_for_struct_signature(struct_type)
        if converter:
            # This is a semantic type - hash it
            try:
                hash_string = converter.hash_struct_dict(data, add_prefix=True)
                return pa.large_string(), hash_string
            except Exception as e:
                field_path = (
                    ".".join(self._current_field_path)
                    if self._current_field_path
                    else "<root>"
                )
                converter_name = getattr(
                    converter, "semantic_type_name", str(type(converter).__name__)
                )
                raise SemanticHashingError(
                    f"Failed to hash semantic type '{converter_name}' at field path '{field_path}': {str(e)}"
                ) from e
        else:
            # Regular struct - recursively process fields
            return self._visit_struct_fields(struct_type, data)

    def visit_list(
        self, list_type: "pa.ListType", data: list | None
    ) -> tuple["pa.DataType", Any]:
        """
        Visit a list type, recursively processing elements.

        Elements that are semantic types will be replaced with hash strings.
        """
        if data is None:
            return list_type, None

        # Add list indicator to field path for error context
        self._current_field_path.append("[*]")
        try:
            return self._visit_list_elements(list_type, data)
        finally:
            self._current_field_path.pop()

    def visit_map(
        self, map_type: "pa.MapType", data: dict | None
    ) -> tuple["pa.DataType", Any]:
        """
        Visit a map type.

        For now, we treat maps as pass-through since they're less common.
        TODO: Implement proper map traversal if needed for semantic types in keys/values.
        """
        return map_type, data

    def visit_primitive(
        self, primitive_type: "pa.DataType", data: Any
    ) -> tuple["pa.DataType", Any]:
        """
        Visit a primitive type - pass through unchanged.

        Primitive types cannot be semantic types (which are always structs).
        """
        return primitive_type, data

    def _visit_struct_fields(
        self, struct_type: "pa.StructType", data: dict | None
    ) -> tuple["pa.StructType", dict]:
        """Override to add field path tracking for better error messages"""
        if data is None:
            return struct_type, None

        new_fields = []
        new_data = {}

        for field in struct_type:
            # Add field name to path for error context
            self._current_field_path.append(field.name)
            try:
                field_data = data.get(field.name)
                new_field_type, new_field_data = self.visit(field.type, field_data)

                new_fields.append(pa.field(field.name, new_field_type))
                new_data[field.name] = new_field_data
            finally:
                self._current_field_path.pop()

        return pa.struct(new_fields), new_data
