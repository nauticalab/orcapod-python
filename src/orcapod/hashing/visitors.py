"""
Generic visitor pattern for traversing Arrow types and data simultaneously.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
import typing
from typing import TYPE_CHECKING, Any

from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa
    from orcapod.semantic_types.universal_converter import UniversalTypeConverter
    from orcapod.protocols.hashing_protocols import SemanticHasherProtocol
else:
    pa = LazyModule("pyarrow")


class ArrowTypeDataVisitor(ABC):
    """Base visitor for traversing Arrow types and data simultaneously."""

    @abstractmethod
    def visit_struct(
        self, struct_type: "pa.StructType", data: dict | None
    ) -> tuple["pa.DataType", Any]:
        """Visit a struct type with its data."""
        pass

    @abstractmethod
    def visit_list(
        self, list_type: "pa.ListType", data: list | None
    ) -> tuple["pa.DataType", Any]:
        """Visit a list type with its data."""
        pass

    @abstractmethod
    def visit_map(
        self, map_type: "pa.MapType", data: dict | None
    ) -> tuple["pa.DataType", Any]:
        """Visit a map type with its data."""
        pass

    @abstractmethod
    def visit_primitive(
        self, primitive_type: "pa.DataType", data: Any
    ) -> tuple["pa.DataType", Any]:
        """Visit a primitive type with its data."""
        pass

    def visit_extension(
        self,
        extension_type: "pa.ExtensionType",
        storage_value: Any,
    ) -> tuple["pa.DataType", Any]:
        """Handle an Arrow extension type.

        Default implementation: passthrough — preserves the extension type and its
        storage value unchanged so that the downstream ``StarfixArrowHasher`` /
        ``ArrowDigester`` sees the full extension metadata when it receives the
        pre-processed table.

        Subclasses may override to convert recognised extension types to a hashed
        ``pa.large_binary()`` value.

        Args:
            extension_type: The Arrow extension type.
            storage_value: The storage-level value (result of ``to_pylist()`` on the column).

        Returns:
            Tuple of ``(new_arrow_type, new_data)``.
        """
        return extension_type, storage_value

    def visit(self, arrow_type: "pa.DataType", data: Any) -> tuple["pa.DataType", Any]:
        """Main dispatch method that routes to the appropriate visit method.

        Extension types are checked **first** — before the struct check — because
        extension types with struct storage would otherwise be incorrectly routed
        into ``visit_struct``.  After ``visit_extension``, the result is re-visited
        only if the type changed AND is no longer an extension type (enables
        composability, avoids infinite recursion).

        Args:
            arrow_type: Arrow data type to process.
            data: Corresponding data value.

        Returns:
            Tuple of ``(new_arrow_type, new_data)``.
        """
        if isinstance(arrow_type, pa.ExtensionType):
            new_type, new_data = self.visit_extension(arrow_type, data)
            if new_type is not arrow_type and not isinstance(new_type, pa.ExtensionType):
                return self.visit(new_type, new_data)
            return new_type, new_data

        if pa.types.is_struct(arrow_type):
            return self.visit_struct(arrow_type, data)
        elif pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
            return self.visit_list(arrow_type, data)
        elif pa.types.is_fixed_size_list(arrow_type):
            return self.visit_list(arrow_type, data)
        elif pa.types.is_map(arrow_type):
            return self.visit_map(arrow_type, data)
        else:
            return self.visit_primitive(arrow_type, data)

    def _visit_struct_fields(
        self, struct_type: "pa.StructType", data: dict | None
    ) -> tuple["pa.StructType", dict | None]:
        """Recursively process struct fields. Default behavior for regular structs."""
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
    ) -> tuple["pa.DataType", list | None]:
        """Recursively process list elements."""
        if data is None:
            return list_type, None

        element_type = list_type.value_type
        processed_elements = []
        new_element_type = None

        for item in data:
            current_element_type, processed_item = self.visit(element_type, item)
            processed_elements.append(processed_item)
            if new_element_type is None and processed_item is not None:
                new_element_type = current_element_type

        if new_element_type is None:
            new_element_type = element_type

        if pa.types.is_large_list(list_type):
            return pa.large_list(new_element_type), processed_elements
        elif pa.types.is_fixed_size_list(list_type):
            return pa.list_(new_element_type, list_type.list_size), processed_elements
        else:
            return pa.list_(new_element_type), processed_elements


class SemanticHashingError(Exception):
    """Exception raised when semantic hashing fails."""
    pass


class SemanticHashingVisitor(ArrowTypeDataVisitor):
    """Visitor that replaces extension-typed columns with their content hashes.

    For each Arrow column whose type is a ``pa.ExtensionType``:

    1. Look up the corresponding Python type via ``type_converter``.
    2. If the Python type has a semantic hasher registered in ``python_hasher``,
       convert the storage value to a Python object and hash it, replacing the
       column with a ``pa.large_binary()`` value of the form::

           <type_name_bytes> + b"::" + content_hash.to_prefixed_digest()

       where ``type_name`` is the extension name with dots replaced by colons
       (e.g. ``"orcapod.path"`` → ``"orcapod:path"``), and
       ``to_prefixed_digest()`` = ``method_bytes + b":" + digest``.
    3. If no hasher is registered (or the converter doesn't know the type),
       return the extension type and storage value unchanged. The downstream
       ``StarfixArrowHasher`` / ``ArrowDigester`` will see the full extension
       metadata intact and hash it in a type-aware way.

    Args:
        type_converter: The active ``UniversalTypeConverter`` for resolving
            extension type → Python type and storage → Python conversion.
        python_hasher: The active ``SemanticHasherProtocol`` for hashing
            Python objects.
    """

    def __init__(
        self,
        type_converter: "UniversalTypeConverter",
        python_hasher: "SemanticHasherProtocol",
    ) -> None:
        self._type_converter = type_converter
        self._python_hasher = python_hasher
        self._current_field_path: list[str] = []

    def visit_extension(
        self,
        extension_type: "pa.ExtensionType",
        storage_value: Any,
    ) -> tuple["pa.DataType", Any]:
        """Hash an extension type value to pa.large_binary(), or passthrough."""
        if storage_value is None:
            return extension_type, None

        # Resolve extension type → Python type.
        python_type = self._type_converter.arrow_type_to_python_type(extension_type)

        # If the converter couldn't resolve to a concrete class, passthrough.
        if python_type is typing.Any or not isinstance(python_type, type):
            return extension_type, storage_value

        # Only hash if a semantic hasher is registered for this Python type.
        if not self._python_hasher.type_handler_registry.has_handler(
            python_type
        ):
            return extension_type, storage_value

        # Convert storage value → Python object and hash it.
        python_obj = self._type_converter.storage_to_python(storage_value, python_type)
        content_hash = self._python_hasher.hash_object(python_obj)

        # Encode as binary: "<type_name>::<method>:<digest>"
        # Dots in the extension name → colons (e.g. "orcapod.path" → "orcapod:path").
        # The "::" separator is unambiguous because to_prefixed_digest() uses only ":".
        type_name = extension_type.extension_name.replace(".", ":")
        hash_bytes = (
            type_name.encode("utf-8")
            + b"::"
            + content_hash.to_prefixed_digest()
        )
        return pa.large_binary(), hash_bytes

    def visit_struct(
        self, struct_type: "pa.StructType", data: dict | None
    ) -> tuple["pa.DataType", Any]:
        """Regular struct (no extension identity) — recurse into fields."""
        if data is None:
            return struct_type, None
        return self._visit_struct_fields(struct_type, data)

    def visit_list(
        self, list_type: "pa.ListType", data: list | None
    ) -> tuple["pa.DataType", Any]:
        """Recurse into list elements."""
        if data is None:
            return list_type, None
        self._current_field_path.append("[*]")
        try:
            return self._visit_list_elements(list_type, data)
        finally:
            self._current_field_path.pop()

    def visit_map(
        self, map_type: "pa.MapType", data: dict | None
    ) -> tuple["pa.DataType", Any]:
        """Pass map types through unchanged."""
        return map_type, data

    def visit_primitive(
        self, primitive_type: "pa.DataType", data: Any
    ) -> tuple["pa.DataType", Any]:
        """Pass primitive types through unchanged."""
        return primitive_type, data

    def _visit_struct_fields(
        self, struct_type: "pa.StructType", data: dict | None
    ) -> tuple["pa.StructType", dict | None]:
        """Override to add field path tracking for better error messages."""
        if data is None:
            return struct_type, None

        new_fields = []
        new_data = {}

        for field in struct_type:
            self._current_field_path.append(field.name)
            try:
                field_data = data.get(field.name)
                new_field_type, new_field_data = self.visit(field.type, field_data)
                new_fields.append(pa.field(field.name, new_field_type))
                new_data[field.name] = new_field_data
            finally:
                self._current_field_path.pop()

        return pa.struct(new_fields), new_data
