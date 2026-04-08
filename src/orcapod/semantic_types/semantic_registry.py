from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from orcapod.protocols.semantic_types_protocols import SemanticStructConverterProtocol
from orcapod.semantic_types import pydata_utils

# from orcapod.semantic_types.type_inference import infer_python_schema_from_pylist_data
from orcapod.types import DataType, Schema
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")


class SemanticTypeRegistry:
    """
    Registry that manages semantic type converters using struct signature recognition.

    This registry maps Python types to PyArrow struct signatures, enabling
    automatic detection and conversion of semantic types based on their
    struct schema alone.
    """

    @staticmethod
    def infer_python_schema_from_pylist(data: list[dict[str, Any]]) -> Schema:
        """
        Infer Python schema from a list of dictionaries (pylist)
        """
        return pydata_utils.infer_python_schema_from_pylist_data(data)

    @staticmethod
    def infer_python_schema_from_pydict(data: dict[str, list[Any]]) -> Schema:
        # TODO: consider which data type is more efficient and use that pylist or pydict
        return pydata_utils.infer_python_schema_from_pylist_data(
            pydata_utils.pydict_to_pylist(data)
        )

    def __init__(
        self, converters: Mapping[str, SemanticStructConverterProtocol] | None = None
    ):
        # Bidirectional mappings between Python types and struct signatures
        self._python_to_struct: dict[DataType, "pa.StructType"] = {}
        self._struct_to_python: dict["pa.StructType", DataType] = {}
        self._struct_to_converter: dict[
            "pa.StructType", SemanticStructConverterProtocol
        ] = {}

        # Name mapping for convenience
        self._name_to_converter: dict[str, SemanticStructConverterProtocol] = {}
        self._struct_to_name: dict["pa.StructType", str] = {}

        # If initialized with a list of converters, register them
        if converters:
            for semantic_type_name, converter in converters.items():
                self.register_converter(semantic_type_name, converter)

    def register_converter(
        self, semantic_type_name: str, converter: SemanticStructConverterProtocol
    ) -> None:
        """
        Register a semantic type converter.

        This creates bidirectional mappings between:
        - Python type ↔ Arrow struct signature
        - Arrow struct signature ↔ converter instance

        Optionally, a semantic type name can be provided.
        """
        python_type = converter.python_type
        struct_signature = converter.arrow_struct_type

        # Check for conflicts
        if python_type in self._python_to_struct:
            existing_struct = self._python_to_struct[python_type]
            if existing_struct != struct_signature:
                raise ValueError(
                    f"Python type {python_type} already registered with different struct signature. "
                    f"Existing: {existing_struct}, New: {struct_signature}"
                )

        if struct_signature in self._struct_to_python:
            existing_python = self._struct_to_python[struct_signature]
            if existing_python != python_type:
                raise ValueError(
                    f"Struct signature {struct_signature} already registered with different Python type. "
                    f"Existing: {existing_python}, New: {python_type}"
                )

        # catch case where a different converter is already registered with the semantic type name
        if existing_converter := self.get_converter_for_semantic_type(
            semantic_type_name
        ):
            if existing_converter != converter:
                raise ValueError(
                    f"Semantic type name '{semantic_type_name}' is already registered to {existing_converter}"
                )

        # Register bidirectional mappings
        self._python_to_struct[python_type] = struct_signature
        self._struct_to_python[struct_signature] = python_type
        self._struct_to_converter[struct_signature] = converter

        self._name_to_converter[semantic_type_name] = converter
        self._struct_to_name[struct_signature] = semantic_type_name

    def get_converter_for_python_type(
        self, python_type: DataType
    ) -> SemanticStructConverterProtocol | None:
        """Get converter registered to the Python type."""
        # Direct lookup first
        struct_signature = self._python_to_struct.get(python_type)
        if struct_signature:
            return self._struct_to_converter[struct_signature]

        # Handle subclass relationships - add safety check
        for registered_type, struct_signature in self._python_to_struct.items():
            try:
                if (
                    isinstance(registered_type, type)
                    and isinstance(python_type, type)
                    and issubclass(python_type, registered_type)
                ):
                    return self._struct_to_converter[struct_signature]
            except TypeError:
                # Handle cases where issubclass fails (e.g., with generic types)
                continue

        return None

    def get_converter_for_semantic_type(
        self, semantic_type_name: str
    ) -> SemanticStructConverterProtocol | None:
        """Get converter registered to the semantic type name."""
        return self._name_to_converter.get(semantic_type_name)

    def get_converter_for_struct_signature(
        self, struct_signature: "pa.StructType"
    ) -> SemanticStructConverterProtocol | None:
        """
        Get converter registered to the Arrow struct signature.
        """
        return self._struct_to_converter.get(struct_signature)

    def get_python_type_for_semantic_struct_signature(
        self, struct_signature: "pa.StructType"
    ) -> DataType | None:
        """
        Get Python type registered to the Arrow struct signature.
        """
        return self._struct_to_python.get(struct_signature)

    def get_semantic_struct_signature_for_python_type(
        self, python_type: type
    ) -> "pa.StructType | None":
        """Get Arrow struct signature registered to the Python type."""
        return self._python_to_struct.get(python_type)

    def has_semantic_type(self, semantic_type_name: str) -> bool:
        """Check if the semantic type name is registered."""
        return semantic_type_name in self._name_to_converter

    def has_python_type(self, python_type: type) -> bool:
        """Check if the Python type is registered."""
        return python_type in self._python_to_struct

    def has_semantic_struct_signature(self, struct_signature: "pa.StructType") -> bool:
        """Check if the struct signature is registered."""
        return struct_signature in self._struct_to_python

    def list_semantic_types(self) -> list[str]:
        """Get all registered semantic type names."""
        return list(self._name_to_converter.keys())

    def list_python_types(self) -> list[DataType]:
        """Get all registered Python types."""
        return list(self._python_to_struct.keys())

    def list_struct_signatures(self) -> list["pa.StructType"]:
        """Get all registered struct signatures."""
        return list(self._struct_to_python.keys())

    def find_semantic_fields_in_schema(self, schema: "pa.Schema") -> dict[str, str]:
        """
        Find all semantic type fields in a schema by struct signature recognition.

        Args:
            schema: PyArrow schema to examine

        Returns:
            Dictionary mapping field names to semantic type names

        Example:
            schema with fields:
            - name: string
            - file_path: struct<path: large_string>
            - location: struct<latitude: double, longitude: double>

            Returns: {"file_path": "path", "location": "geolocation"}
        """
        semantic_fields = {}
        for field in schema:
            if pa.types.is_struct(field.type) and field.type in self._struct_to_name:
                semantic_fields[field.name] = self._struct_to_name[field.type]
        return semantic_fields

    def get_semantic_field_info(self, schema: "pa.Schema") -> dict[str, dict[str, Any]]:
        """
        Get detailed information about semantic fields in a schema.

        Returns:
            Dictionary with field names as keys and info dictionaries as values.
            Each info dict contains: semantic_type, python_type, struct_signature
        """
        semantic_info = {}
        for field in schema:
            if pa.types.is_struct(field.type):
                converter = self.get_converter_for_struct_signature(field.type)
                if converter:
                    semantic_info[field.name] = {
                        "python_type": converter.python_type,
                        "struct_signature": field.type,
                        "converter": converter,
                    }
        return semantic_info

    def validate_struct_signature(
        self, struct_signature: "pa.StructType", expected_python_type: type
    ) -> bool:
        """
        Validate that a struct signature matches the expected Python type.

        Args:
            struct_signature: Arrow struct type to validate
            expected_python_type: Expected Python type

        Returns:
            True if the struct signature is registered for the Python type
        """
        registered_type = self.get_python_type_for_semantic_struct_signature(
            struct_signature
        )
        return registered_type == expected_python_type
