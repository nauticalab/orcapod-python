"""
Struct-based semantic type system for OrcaPod.

This replaces the metadata-based approach with explicit struct fields,
making semantic types visible in schemas and preserved through operations.
"""

from typing import Any, TYPE_CHECKING
from pathlib import Path
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

    def _compute_content_hash(self, content: bytes) -> bytes:
        """
        Compute SHA-256 hash of content bytes.

        Args:
            content: Content to hash

        Returns:
            SHA-256 hash bytes
        """
        import hashlib

        return hashlib.sha256(content).digest()


# Path-specific implementation
class PathStructConverter(SemanticStructConverterBase):
    """Converter for pathlib.Path objects to/from semantic structs."""

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
        field_names = [field.name for field in struct_type]
        expected_fields = {"path"}

        if set(field_names) != expected_fields:
            return False

        # Check field types
        field_types = {field.name: field.type for field in struct_type}

        return field_types["path"] == pa.large_string()

    def is_semantic_struct(self, struct_dict: dict[str, Any]) -> bool:
        """Check if a struct dictionary represents this semantic type."""
        return set(struct_dict.keys()) == {"path"} and isinstance(
            struct_dict["path"], str
        )

    def hash_struct_dict(
        self, struct_dict: dict[str, Any], add_prefix: bool = False
    ) -> str:
        """
        Compute hash of the file content pointed to by the path.

        Args:
            struct_dict: Arrow struct dictionary with 'path' field
            add_prefix: If True, prefix with semantic type and algorithm info

        Returns:
            Hash string of the file content, optionally prefixed

        Raises:
            FileNotFoundError: If the file doesn't exist
            PermissionError: If the file can't be read
            OSError: For other file system errors
        """
        path_str = struct_dict.get("path")
        if path_str is None:
            raise ValueError("Missing 'path' field in struct")

        path = Path(path_str)

        try:
            # TODO: replace with FileHasher implementation
            # Read file content and compute hash
            content = path.read_bytes()
            hash_bytes = self._compute_content_hash(content)
            return self._format_hash_string(hash_bytes, add_prefix)

        except FileNotFoundError:
            raise FileNotFoundError(f"File not found: {path}")
        except PermissionError:
            raise PermissionError(f"Permission denied reading file: {path}")
        except IsADirectoryError:
            raise ValueError(f"Path is a directory, not a file: {path}")
        except OSError as e:
            raise OSError(f"Error reading file {path}: {e}")


from upath import UPath

class UPathStructConverter(SemanticStructConverterBase):
    """Converter for universal_pathlib.UPath objects to/from semantic structs."""
    
    def __init__(self):
        super().__init__("upath")
        self._python_type = UPath
        # Define the Arrow struct type for upaths
        self._arrow_struct_type = pa.struct(
            [
                pa.field("upath", pa.large_string()),
            ]
        )
    
    @property
    def python_type(self) -> type:
        return self._python_type
    
    @property
    def arrow_struct_type(self) -> pa.StructType:
        return self._arrow_struct_type
    
    def python_to_struct_dict(self, value: UPath) -> dict[str, Any]:
        """Convert UPath to struct dictionary."""
        if not isinstance(value, UPath):
            raise TypeError(f"Expected UPath, got {type(value)}")
        return {
            "upath": str(value),
        }
    
    def struct_dict_to_python(self, struct_dict: dict[str, Any]) -> UPath:
        """Convert struct dictionary back to UPath."""
        path_str = struct_dict.get("upath")
        if path_str is None:
            raise ValueError("Missing 'upath' field in struct")
        return UPath(path_str)
    
    def can_handle_python_type(self, python_type: type) -> bool:
        """Check if this converter can handle the given Python type."""
        return issubclass(python_type, UPath)
    
    def can_handle_struct_type(self, struct_type: pa.StructType) -> bool:
        """Check if this converter can handle the given struct type."""
        # Check if struct has the expected fields
        field_names = [field.name for field in struct_type]
        expected_fields = {"upath"}
        
        if set(field_names) != expected_fields:
            return False
        
        # Check field types
        field_types = {field.name: field.type for field in struct_type}
        return field_types["upath"] == pa.large_string()
    
    def is_semantic_struct(self, struct_dict: dict[str, Any]) -> bool:
        """Check if a struct dictionary represents this semantic type."""
        return set(struct_dict.keys()) == {"upath"} and isinstance(
            struct_dict["upath"], str
        )
    
    def hash_struct_dict(
        self, struct_dict: dict[str, Any], add_prefix: bool = False
    ) -> str:
        """
        Compute hash of the file content pointed to by the path.
        
        Args:
            struct_dict: Arrow struct dictionary with 'upath' field
            add_prefix: If True, prefix with semantic type and algorithm info
        
        Returns:
            Hash string of the file content, optionally prefixed
        
        Raises:
            FileNotFoundError: If the file doesn't exist
            PermissionError: If the file can't be read
            OSError: For other file system errors
        """
        path_str = struct_dict.get("upath")
        if path_str is None:
            raise ValueError("Missing 'upath' field in struct")
        
        path = UPath(path_str)
        
        try:
            # Read file content and compute hash
            content = path.read_bytes()
            hash_bytes = self._compute_content_hash(content)
            return self._format_hash_string(hash_bytes, add_prefix)
        except FileNotFoundError:
            raise FileNotFoundError(f"File not found: {path}")
        except PermissionError:
            raise PermissionError(f"Permission denied reading file: {path}")
        except IsADirectoryError:
            raise ValueError(f"Path is a directory, not a file: {path}")
        except OSError as e:
            raise OSError(f"Error reading file {path}: {e}")