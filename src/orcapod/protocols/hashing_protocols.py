"""Hash strategy protocols for dependency injection."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from orcapod.types import ContentHash, PathLike, Schema

if TYPE_CHECKING:
    import pyarrow as pa
    from orcapod.hashing.semantic_hashing.type_handler_registry import PythonTypeSemanticHasherRegistry


@runtime_checkable
class DataContextAwareProtocol(Protocol):
    """Protocol for objects aware of their data context."""

    @property
    def data_context_key(self) -> str:
        """Return the data context key associated with this object."""
        ...


@runtime_checkable
class PipelineElementProtocol(Protocol):
    """Protocol for objects that have a stable identity as an element in a pipeline graph."""

    def pipeline_identity_structure(self) -> Any:
        """Return a structure representing this element's pipeline identity."""
        ...

    def pipeline_hash(self, hasher=None) -> ContentHash:
        """Return the pipeline-level hash of this element."""
        ...


@runtime_checkable
class ContentIdentifiableProtocol(Protocol):
    """Protocol for objects that can express their semantic identity as a plain Python structure."""

    def identity_structure(self) -> Any:
        """Return a structure that represents the semantic identity of this object."""
        ...

    def content_hash(self, hasher: "SemanticHasherProtocol | None" = None) -> ContentHash:
        """Returns the content hash."""
        ...


class PythonTypeSemanticHasherProtocol(Protocol):
    """Protocol for type-specific semantic hashers used by SemanticAwarePythonHasher.

    A ``PythonTypeSemanticHasherProtocol`` hashes a specific Python type to a
    ``ContentHash``. Implementations are registered with a
    ``PythonTypeSemanticHasherRegistry`` and looked up via MRO-aware resolution.

    Each implementation receives the full ``SemanticAwarePythonHasher`` so it can
    delegate hashing of sub-values back to the outer hasher without coupling to a
    specific hasher instance.
    """

    def hash(self, obj: Any, hasher: "SemanticAwarePythonHasher") -> ContentHash:
        """Hash *obj* to a ContentHash.

        Args:
            obj:    The object to hash. Always matches the registered type.
            hasher: The active ``SemanticAwarePythonHasher``. Use
                    ``hasher.hash_object(sub_value)`` to hash sub-values.

        Returns:
            ContentHash: The content-addressed hash of *obj*.
        """
        ...


class SemanticHasherProtocol(Protocol):
    """Protocol for the semantic content-based hasher."""

    def hash_object(
        self,
        obj: Any,
        resolver: Callable[[Any], ContentHash] | None = None,
    ) -> ContentHash:
        """Hash *obj* based on its semantic content."""
        ...

    @property
    def hasher_id(self) -> str:
        """Returns a unique identifier/name for this hasher instance."""
        ...

    @property
    def type_semantic_hasher_registry(self) -> "PythonTypeSemanticHasherRegistry":
        """Return the PythonTypeSemanticHasherRegistry used by this hasher."""
        ...


class FileContentHasherProtocol(Protocol):
    """Protocol for file-related hashing."""

    def hash_file(self, file_path: PathLike) -> ContentHash: ...


@runtime_checkable
class ArrowHasherProtocol(Protocol):
    """Protocol for hashing arrow data."""

    @property
    def hasher_id(self) -> str: ...

    def hash_table(self, table: "pa.Table | pa.RecordBatch") -> ContentHash: ...


class StringCacherProtocol(Protocol):
    """Protocol for caching string key value pairs."""

    def get_cached(self, cache_key: str) -> str | None: ...
    def set_cached(self, cache_key: str, value: str) -> None: ...
    def clear_cache(self) -> None: ...


class FunctionInfoExtractorProtocol(Protocol):
    """Protocol for extracting function information."""

    def extract_function_info(
        self,
        func: Callable[..., Any],
        function_name: str | None = None,
        input_typespec: Schema | None = None,
        output_typespec: Schema | None = None,
        exclude_function_signature: bool = False,
        exclude_function_body: bool = False,
    ) -> dict[str, Any]: ...


class SemanticTypeHasherProtocol(Protocol):
    """Abstract base class for semantic type-specific hashers."""

    @property
    def hasher_id(self) -> str:
        """Unique identifier for this semantic type hasher."""
        ...

    def hash_column(self, column: "pa.Array") -> "pa.Array":
        """Hash a column with this semantic type and return the hash bytes as an array."""
        ...

    def set_cacher(self, cacher: StringCacherProtocol) -> None:
        """Add a string cacher for caching hash values."""
        ...
