from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Protocol

from orcapod.types import Schema, SchemaLike

if TYPE_CHECKING:
    import pyarrow as pa


class TypeConverter(Protocol):
    def python_type_to_arrow_type(self, python_type: type) -> "pa.DataType": ...

    def python_schema_to_arrow_schema(
        self, python_schema: SchemaLike
    ) -> "pa.Schema": ...

    def arrow_type_to_python_type(self, arrow_type: "pa.DataType") -> type: ...

    def arrow_schema_to_python_schema(self, arrow_schema: "pa.Schema") -> Schema: ...

    def python_dicts_to_struct_dicts(
        self,
        python_dicts: list[dict[str, Any]],
        python_schema: SchemaLike | None = None,
    ) -> list[dict[str, Any]]: ...

    def struct_dicts_to_python_dicts(
        self,
        struct_dict: list[dict[str, Any]],
        arrow_schema: "pa.Schema",
    ) -> list[dict[str, Any]]: ...

    def python_dicts_to_arrow_table(
        self,
        python_dicts: list[dict[str, Any]],
        python_schema: SchemaLike | None = None,
        arrow_schema: "pa.Schema | None" = None,
    ) -> "pa.Table": ...

    def arrow_table_to_python_dicts(
        self, arrow_table: "pa.Table"
    ) -> list[dict[str, Any]]: ...

    def get_python_to_arrow_converter(
        self, python_type: type
    ) -> "Callable[[Any], Any]": ...

    def get_arrow_to_python_converter(
        self, arrow_type: "pa.DataType"
    ) -> "Callable[[Any], Any]": ...


# Core protocols
class SemanticStructConverter(Protocol):
    """Protocol for converting between Python objects and semantic structs."""

    @property
    def python_type(self) -> type:
        """The Python type this converter can handle."""
        ...

    @property
    def arrow_struct_type(self) -> "pa.StructType":
        """The Arrow struct type this converter produces."""
        ...

    def python_to_struct_dict(self, value: Any) -> dict[str, Any]:
        """Convert Python value to struct dictionary."""
        ...

    def struct_dict_to_python(self, struct_dict: dict[str, Any]) -> Any:
        """Convert struct dictionary back to Python value."""
        ...

    def can_handle_python_type(self, python_type: type) -> bool:
        """Check if this converter can handle the given Python type."""
        ...

    def can_handle_struct_type(self, struct_type: "pa.StructType") -> bool:
        """Check if this converter can handle the given struct type."""
        ...

    def hash_struct_dict(
        self, struct_dict: dict[str, Any], add_prefix: bool = False
    ) -> str:
        """
        Compute hash of the semantic type from its struct dictionary representation.

        Args:
            struct_dict: Arrow struct dictionary representation
            add_prefix: If True, prefix with semantic type and algorithm info

        Returns:
            Hash string, optionally prefixed like "path:sha256:abc123..."

        Raises:
            Exception: If hashing fails (e.g., file not found for path types)
        """
        ...

    @property
    def hasher_id(self) -> str:
        """Identifier for this hasher (for debugging/versioning)"""
        ...
