"""
Struct-based semantic type system for OrcaPod.

This replaces the metadata-based approach with explicit struct fields,
making semantic types visible in schemas and preserved through operations.
"""

from __future__ import annotations

import uuid as _uuid_module
from abc import ABC, abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING, Any

from upath import UPath

from orcapod.types import ContentHash
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa

    from orcapod.protocols.hashing_protocols import FileContentHasherProtocol
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

    def _compute_content_hash(self, content: bytes) -> ContentHash:
        """Compute SHA-256 hash of content bytes.

        Args:
            content: Content to hash.

        Returns:
            ``ContentHash`` with ``method="sha256"`` and the raw digest.
        """
        import hashlib

        digest = hashlib.sha256(content).digest()
        return ContentHash(method="sha256", digest=digest)

    def _format_semantic_hash(self, content_hash: ContentHash, add_prefix: bool) -> str:
        """Format a ``ContentHash`` into the standard semantic hash string.

        When ``add_prefix`` is ``True`` the result is
        ``"{semantic_type_name}:{method}:{hex}"``, e.g. ``"uuid:sha256:abc123"``.
        When ``False`` only the hex digest is returned.

        Args:
            content_hash: Hash to format.
            add_prefix: Whether to prepend the semantic type name and algorithm.

        Returns:
            Formatted hash string.
        """
        if add_prefix:
            return f"{self.semantic_type_name}:{content_hash.to_string(prefix_method=True)}"
        return content_hash.to_hex()


class PathStructConverterBase(SemanticStructConverterBase, ABC):
    """Base converter for file path types (Path and UPath).

    Extracts the shared conversion logic since Path and UPath have
    identical APIs for the operations we need (str conversion,
    construction from string, ``read_bytes``).
    """

    def __init__(
        self,
        name: str,
        path_type: type,
        file_hasher: "FileContentHasherProtocol",
    ):
        super().__init__(name)
        self._python_type = path_type
        self._field_name = name
        self._file_hasher = file_hasher
        self._arrow_struct_type = pa.struct([
            pa.field(name, pa.large_string()),
        ])

    @property
    def python_type(self) -> type:
        return self._python_type

    @property
    def arrow_struct_type(self) -> "pa.StructType":
        return self._arrow_struct_type

    @abstractmethod
    def _make_path(self, path_str: str) -> Any:
        """Construct the appropriate path object from a string."""
        ...

    def python_to_struct_dict(self, value: Any) -> dict[str, Any]:
        """Convert path object to struct dictionary."""
        if not isinstance(value, self._python_type):
            raise TypeError(f"Expected {self._python_type.__name__}, got {type(value)}")
        return {self._field_name: str(value)}

    def struct_dict_to_python(self, struct_dict: dict[str, Any]) -> Any:
        """Convert struct dictionary back to path object."""
        path_str = struct_dict.get(self._field_name)
        if path_str is None:
            raise ValueError(f"Missing '{self._field_name}' field in struct")
        return self._make_path(path_str)

    def can_handle_python_type(self, python_type: type) -> bool:
        """Check if this converter can handle the given Python type."""
        return issubclass(python_type, self._python_type)

    def can_handle_struct_type(self, struct_type: "pa.StructType") -> bool:
        """Check if this converter can handle the given struct type."""
        for field in self._arrow_struct_type:
            if (
                field.name not in struct_type.names
                or struct_type[field.name].type != field.type
            ):
                return False
        return True

    def is_semantic_struct(self, struct_dict: dict[str, Any]) -> bool:
        """Check if a struct dictionary represents this semantic type."""
        return (
            set(struct_dict.keys()) == {self._field_name}
            and isinstance(struct_dict[self._field_name], str)
        )

    def hash_struct_dict(
        self, struct_dict: dict[str, Any], add_prefix: bool = False
    ) -> str:
        """Compute hash of a path semantic type by hashing the file content.

        Args:
            struct_dict: Dict with the path field containing a file path string.
            add_prefix: If True, prefix with semantic type and algorithm info.

        Returns:
            Hash string of the file content.

        Raises:
            FileNotFoundError: If the path does not exist.
            IsADirectoryError: If the path is a directory.
        """
        path_str = struct_dict.get(self._field_name)
        if path_str is None:
            raise ValueError(f"Missing '{self._field_name}' field in struct dict")

        path = self._make_path(path_str)
        if not path.exists():
            raise FileNotFoundError(f"Path does not exist: {path}")
        if path.is_dir():
            raise IsADirectoryError(f"Path is a directory: {path}")

        file_hash = self._file_hasher.hash_file(path)
        return self._format_semantic_hash(file_hash, add_prefix)


class PythonPathStructConverter(PathStructConverterBase):
    """Converter for pathlib.Path objects to/from semantic structs.

    Rejects ``UPath`` instances to avoid ambiguity with
    ``UPathStructConverter``, since ``UPath`` is a ``Path`` subclass.
    """

    def __init__(self, file_hasher: "FileContentHasherProtocol"):
        super().__init__("path", Path, file_hasher)

    def _make_path(self, path_str: str) -> Path:
        return Path(path_str)

    def python_to_struct_dict(self, value: Any) -> dict[str, Any]:
        """Convert Path to struct dictionary, rejecting UPath instances."""
        if isinstance(value, UPath):
            raise TypeError(
                f"Expected Path (not UPath), got {type(value)}. "
                "Use UPathStructConverter for UPath instances."
            )
        return super().python_to_struct_dict(value)

    def can_handle_python_type(self, python_type: type) -> bool:
        """Check if this converter can handle the given Python type.

        Returns False for UPath (and its subclasses) to avoid ambiguity.
        """
        if issubclass(python_type, UPath):
            return False
        return issubclass(python_type, Path)


class UPathStructConverter(PathStructConverterBase):
    """Converter for universal_pathlib.UPath objects to/from semantic structs."""

    def __init__(self, file_hasher: "FileContentHasherProtocol"):
        super().__init__("upath", UPath, file_hasher)

    def _make_path(self, path_str: str) -> UPath:
        return UPath(path_str)


class UUIDStructConverter(SemanticStructConverterBase):
    """Converter for ``uuid.UUID`` objects to/from Arrow semantic structs.

    Stores UUIDs as fixed 16-byte binary values inside a single-field struct,
    following the same pattern as ``PythonPathStructConverter`` and
    ``UPathStructConverter``.

    Note:
        ``uuid_utils.UUID`` objects (e.g. from ``uuid7()``) are accepted via
        duck typing because they expose a ``.bytes`` attribute but do not
        inherit from ``uuid.UUID``.
    """

    def __init__(self) -> None:
        super().__init__("uuid")
        self._python_type = _uuid_module.UUID
        self._arrow_struct_type = pa.struct([pa.field("uuid", pa.binary(16))])

    @property
    def python_type(self) -> type:
        """The Python type this converter handles (``uuid.UUID``)."""
        return self._python_type

    @property
    def arrow_struct_type(self) -> "pa.StructType":
        """The Arrow struct type used for serialisation."""
        return self._arrow_struct_type

    def python_to_struct_dict(self, value: Any) -> dict[str, bytes]:
        """Convert a UUID to a struct dictionary with a single ``uuid`` field.

        Accepts both ``uuid.UUID`` instances and duck-typed UUID-compatible
        objects (e.g. ``uuid_utils.UUID``) that expose a ``.bytes`` attribute
        returning 16 raw bytes.

        Args:
            value: A ``uuid.UUID`` instance or compatible UUID-like object.

        Returns:
            A dict with a single key ``"uuid"`` whose value is 16 raw bytes.

        Raises:
            TypeError: If ``value`` is not a ``uuid.UUID`` instance or
                compatible duck-typed UUID object.
        """
        if isinstance(value, _uuid_module.UUID):
            return {"uuid": value.bytes}
        # Accept uuid_utils.UUID and other duck-typed UUID objects
        raw = getattr(value, "bytes", None)
        if isinstance(raw, bytes) and len(raw) == 16:
            return {"uuid": raw}
        raise TypeError(
            f"Expected uuid.UUID or compatible UUID object, got {type(value)}"
        )

    def struct_dict_to_python(self, struct_dict: dict[str, Any]) -> _uuid_module.UUID:
        """Convert a struct dictionary back to a ``uuid.UUID`` instance.

        Args:
            struct_dict: Dict with a ``"uuid"`` key containing 16 raw bytes
                (``bytes`` or ``bytearray``).

        Returns:
            A ``uuid.UUID`` constructed from the raw bytes.

        Raises:
            ValueError: If the ``"uuid"`` key is absent from ``struct_dict``.
        """
        raw = struct_dict.get("uuid")
        if raw is None:
            raise ValueError("Missing 'uuid' field in struct dict")
        return _uuid_module.UUID(bytes=bytes(raw))

    def can_handle_python_type(self, python_type: type) -> bool:
        """Check if this converter can handle the given Python type.

        Args:
            python_type: The Python type to check.

        Returns:
            ``True`` if ``python_type`` is ``uuid.UUID`` or a subclass of it.
        """
        return issubclass(python_type, self._python_type)

    def can_handle_struct_type(self, struct_type: "pa.StructType") -> bool:
        """Check if this converter can handle the given Arrow struct type.

        Args:
            struct_type: The Arrow struct type to check.

        Returns:
            ``True`` if ``struct_type`` equals the UUID Arrow struct type.
        """
        return struct_type == self._arrow_struct_type

    def hash_struct_dict(
        self, struct_dict: dict[str, Any], add_prefix: bool = False
    ) -> str:
        """Compute a SHA-256 hash of the UUID from its struct dictionary representation.

        Hashes the raw 16 UUID bytes directly.

        Args:
            struct_dict: Dict with a ``"uuid"`` key containing 16 raw bytes.
            add_prefix: If ``True``, prefix the hash with semantic type and
                algorithm info (e.g. ``"uuid:sha256:<hex>"``).

        Returns:
            Hash string, optionally prefixed.

        Raises:
            ValueError: If the ``"uuid"`` key is absent from ``struct_dict``.
        """
        raw = struct_dict.get("uuid")
        if raw is None:
            raise ValueError("Missing 'uuid' field in struct dict")
        content_hash = self._compute_content_hash(bytes(raw))
        return self._format_semantic_hash(content_hash, add_prefix)
