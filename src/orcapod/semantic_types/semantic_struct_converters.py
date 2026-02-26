"""
Struct-based semantic type system for OrcaPod.

This replaces the metadata-based approach with explicit struct fields,
making semantic types visible in schemas and preserved through operations.
"""

from pathlib import Path
from typing import TYPE_CHECKING, Any

from orcapod.types import ContentHash
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")


class SemanticStructConverterBase:
    """
    Base class providing common functionality for semantic struct converters.

    Subclasses only need to implement the abstract methods and can use
    the common hashing infrastructure.
    """

    def __init__(self, semantic_type_name: str):
        self._semantic_type_name = semantic_type_name
        self._hasher_id = f"{self.semantic_type_name}_content_sha256"

    @property
    def semantic_type_name(self) -> str:
        """The name of the semantic type this converter handles."""
        return self._semantic_type_name

    @property
    def hasher_id(self) -> str:
        """Default hasher ID based on semantic type name"""
        return self._hasher_id

    def _format_hash_string(self, hash_bytes: bytes, add_prefix: bool = False) -> str:
        """
        Format hash bytes into the standard hash string format.

        Args:
            hash_bytes: Raw hash bytes
            add_prefix: Whether to add semantic type and algorithm prefix

        Returns:
            Formatted hash string
        """
        hash_hex = hash_bytes.hex()
        if add_prefix:
            return f"{self.semantic_type_name}:sha256:{hash_hex}"
        else:
            return hash_hex

    def _compute_content_hash(self, content: bytes) -> ContentHash:
        """
        Compute SHA-256 hash of content bytes.

        Args:
            content: Content to hash

        Returns:
            SHA-256 hash bytes
        """
        import hashlib

        digest = hashlib.sha256(content).digest()
        return ContentHash(method=f"{self.semantic_type_name}:sha256", digest=digest)


# Path-specific implementation
class PathStructConverter(SemanticStructConverterBase):
    """Converter for pathlib.Path objects to/from semantic structs of form { path: "/value/of/path"}"""

    def __init__(self):
        super().__init__("path")
        self._python_type = Path

        # Define the Arrow struct type for paths
        self._arrow_struct_type = pa.struct(
            [
                pa.field("path", pa.large_string()),
            ]
        )

    @property
    def python_type(self) -> type:
        return self._python_type

    @property
    def arrow_struct_type(self) -> pa.StructType:
        return self._arrow_struct_type

    def python_to_struct_dict(self, value: Path) -> dict[str, Any]:
        """Convert Path to struct dictionary."""
        if not isinstance(value, Path):
            raise TypeError(f"Expected Path, got {type(value)}")

        return {
            "path": str(value),
        }

    def struct_dict_to_python(self, struct_dict: dict[str, Any]) -> Path:
        """Convert struct dictionary back to Path."""
        path_str = struct_dict.get("path")
        if path_str is None:
            raise ValueError("Missing 'path' field in struct")

        return Path(path_str)

    def can_handle_python_type(self, python_type: type) -> bool:
        """Check if this converter can handle the given Python type."""
        return issubclass(python_type, Path)

    def can_handle_struct_type(self, struct_type: pa.StructType) -> bool:
        """Check if this converter can handle the given struct type."""
        # Check if struct has the expected fields
        for field in self._arrow_struct_type:
            if (
                field.name not in struct_type.names
                or struct_type[field.name].type != field.type
            ):
                return False

        return True

    def is_semantic_struct(self, struct_dict: dict[str, Any]) -> bool:
        """Check if a struct dictionary represents this semantic type."""
        # TODO: infer this check based on identified struct type as defined in the __init__
        return set(struct_dict.keys()) == {"path"} and isinstance(
            struct_dict["path"], str
        )
