"""Hash strategy protocols for dependency injection."""

from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from orcapod.types import ContentHash, PathLike, Schema

if TYPE_CHECKING:
    import pyarrow as pa


@runtime_checkable
class DataContextAwareProtocol(Protocol):
    """Protocol for objects aware of their data context."""

    @property
    def data_context_key(self) -> str:
        """
        Return the data context key associated with this object.

        Returns:
            str: The data context key
        """
        ...


@runtime_checkable
class ContentIdentifiableProtocol(Protocol):
    """
    Protocol for objects that can express their semantic identity as a plain
    Python structure.

    This is the only method a class needs to implement to participate in the
    content-based hashing system. The returned structure is recursively
    resolved by the SemanticHasherProtocol -- any nested ContentIdentifiableProtocol objects
    within the structure will themselves be expanded and hashed, producing a
    Merkle-tree-like composition of hashes.

    The method should return a deterministic structure whose value depends
    only on the semantic content of the object -- not on memory addresses,
    object IDs, or other incidental runtime state.
    """

    def identity_structure(self) -> Any:
        """
        Return a structure that represents the semantic identity of this object.

        The returned value may be any Python object:
          - Primitives (str, int, float, bool, None) are used as-is.
          - Collections (list, dict, set, tuple) are recursively traversed.
          - Nested ContentIdentifiableProtocol objects are recursively resolved by
            the SemanticHasherProtocol: their identity structure is hashed to a
            ContentHash hex token, which is then embedded in place of the
            object in the parent structure.
          - Any type that has a registered TypeHandlerProtocol in the
            SemanticHasherProtocol's registry is handled by that handler.

        Returns:
            Any: A structure representing this object's semantic content.
                 Should be deterministic and include all identity-relevant data.
        """
        ...

    def content_hash(self) -> ContentHash:
        """
        Returns the content hash. Note that the context and algorithm used for computing
        the hash is dependent on the object implementing this. If you'd prefer to use
        your own algorithm, hash the identity_structure instead.
        """
        ...


class TypeHandlerProtocol(Protocol):
    """
    Protocol for type-specific serialization handlers used by SemanticHasherProtocol.

    A TypeHandlerProtocol converts a specific Python type into a value that
    ``hash_object`` can process.  Handlers are registered with a
    TypeHandlerRegistry and looked up via MRO-aware resolution.

    The returned value is passed directly back to ``hash_object``, so it may
    be anything that ``hash_object`` understands:

      - A primitive (None, bool, int, float, str) -- hashed directly.
      - A structure (list, tuple, dict, set, frozenset) -- expanded and hashed.
      - A ContentHash -- treated as a terminal; returned as-is without
        re-hashing.  Use this when the handler has already computed the
        definitive hash of the object (e.g. hashing a file's content).
      - A ContentIdentifiableProtocol -- its identity_structure() will be called.
      - Another registered type -- dispatched through the registry.
    """

    def handle(self, obj: Any, hasher: "SemanticHasherProtocol") -> Any:
        """
        Convert *obj* into a value that ``hash_object`` can process.

        Args:
            obj:    The object to handle.
            hasher: The SemanticHasherProtocol, available if the handler needs to
                    hash sub-objects explicitly via ``hasher.hash_object()``.

        Returns:
            Any value accepted by ``hash_object``: a primitive, structure,
            ContentHash, ContentIdentifiableProtocol, or another registered type.
        """
        ...


class SemanticHasherProtocol(Protocol):
    """
    Protocol for the semantic content-based hasher.

    ``hash_object(obj)`` is the single recursive entry point.  It produces a
    ContentHash for any Python object using the following dispatch:

      - ContentHash        → terminal; returned as-is
      - Primitive          → JSON-serialised and hashed directly
      - Structure          → structurally expanded (type-tagged), then hashed
      - Handler match      → handler.handle() returns a new value; recurse
      - ContentIdentifiableProtocol→ identity_structure() returns a value; recurse
      - Unknown            → TypeError (strict) or best-effort string (lenient)

    Containers are type-tagged before hashing so that list, tuple, dict, set,
    and namedtuple produce distinct hashes even when their elements are equal.

    Unknown types raise TypeError by default (strict mode).  Set
    strict=False on construction to fall back to a best-effort string
    representation with a warning instead.
    """

    def hash_object(self, obj: Any) -> ContentHash:
        """
        Hash *obj* based on its semantic content.

        Args:
            obj: The object to hash.

        Returns:
            ContentHash: Stable, content-based hash of the object.
        """
        ...

    @property
    def hasher_id(self) -> str:
        """
        Returns a unique identifier/name for this hasher instance.

        The hasher_id is embedded in every ContentHash produced by this
        hasher, allowing hashes from different versions or configurations
        to be distinguished.
        """
        ...


class FileContentHasherProtocol(Protocol):
    """Protocol for file-related hashing."""

    def hash_file(self, file_path: PathLike) -> ContentHash: ...


class ArrowHasherProtocol(Protocol):
    """Protocol for hashing arrow packets."""

    def get_hasher_id(self) -> str: ...

    def hash_table(
        self, table: "pa.Table | pa.RecordBatch", prefix_hasher_id: bool = True
    ) -> ContentHash: ...


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

    def hash_column(
        self,
        column: "pa.Array",
    ) -> "pa.Array":
        """Hash a column with this semantic type and return the hash bytes an an array"""
        ...

    def set_cacher(self, cacher: StringCacherProtocol) -> None:
        """Add a string cacher for caching hash values."""
        ...
