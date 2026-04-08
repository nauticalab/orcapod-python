"""
OrcaPod hashing package.

Public API
----------
  BaseSemanticHasher           -- content-based recursive object hasher (concrete)
  SemanticHasherProtocol       -- protocol for semantic hashers
  TypeHandlerRegistry          -- registry mapping types to TypeHandlerProtocol instances
  get_default_semantic_hasher  -- global default SemanticHasherProtocol factory
  get_default_type_handler_registry -- global default TypeHandlerRegistry factory
  ContentIdentifiableMixin     -- convenience mixin for content-identifiable objects

Built-in handlers (importable for custom registry setup):
  PathContentHandler
  UUIDHandler
  BytesHandler
  FunctionHandler
  TypeObjectHandler
  register_builtin_handlers

Legacy names (kept for backward compatibility):
  HashableMixin             -- legacy mixin from legacy_core (deprecated)

Utility:
  FileContentHasherProtocol
  StringCacherProtocol
  FunctionInfoExtractorProtocol
  ArrowHasherProtocol
"""

# ---------------------------------------------------------------------------
# New API -- SemanticHasherProtocol, registry, mixin
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Default hasher factories
# ---------------------------------------------------------------------------
from orcapod.hashing.defaults import (
    get_default_arrow_hasher,
    get_default_semantic_hasher,
    get_default_type_handler_registry,
)

# ---------------------------------------------------------------------------
# File hashing utilities
# ---------------------------------------------------------------------------
from orcapod.hashing.file_hashers import BasicFileHasher, CachedFileHasher
from orcapod.hashing.hash_utils import hash_file
from orcapod.hashing.semantic_hashing.builtin_handlers import (
    BytesHandler,
    FunctionHandler,
    PathContentHandler,
    TypeObjectHandler,
    UUIDHandler,
    register_builtin_handlers,
)
from orcapod.hashing.semantic_hashing.content_identifiable_mixin import (
    ContentIdentifiableMixin,
)

# ---------------------------------------------------------------------------
# Legacy API (deprecated -- kept for backward compatibility)
# These imports are guarded because legacy_core.py has pre-existing import
# issues (e.g. references to removed types) that should not block the new API.
# ---------------------------------------------------------------------------
try:
    from orcapod.hashing.legacy_core import (
        HashableMixin,
        function_content_hash,
        get_function_signature,
        hash_function,
        hash_packet,
        hash_pathset,
        hash_to_hex,
        hash_to_int,
        hash_to_uuid,
    )
except ImportError:
    HashableMixin = None  # type: ignore[assignment,misc]
    function_content_hash = None  # type: ignore[assignment]
    get_function_signature = None  # type: ignore[assignment]
    hash_function = None  # type: ignore[assignment]
    hash_packet = None  # type: ignore[assignment]
    hash_pathset = None  # type: ignore[assignment]
    hash_to_hex = None  # type: ignore[assignment]
    hash_to_int = None  # type: ignore[assignment]
    hash_to_uuid = None  # type: ignore[assignment]
from orcapod.hashing.semantic_hashing.semantic_hasher import BaseSemanticHasher
from orcapod.hashing.semantic_hashing.type_handler_registry import (
    BuiltinTypeHandlerRegistry,
    TypeHandlerRegistry,
)

# ---------------------------------------------------------------------------
# Protocols (re-exported for convenience)
# ---------------------------------------------------------------------------
from orcapod.protocols.hashing_protocols import (
    ArrowHasherProtocol,
    ContentIdentifiableProtocol,
    FileContentHasherProtocol,
    FunctionInfoExtractorProtocol,
    SemanticHasherProtocol,
    SemanticTypeHasherProtocol,
    StringCacherProtocol,
    TypeHandlerProtocol,
)

# ---------------------------------------------------------------------------
# __all__ -- defines the public surface of this package
# ---------------------------------------------------------------------------

__all__ = [
    # ---- New API: concrete implementation ----
    "BaseSemanticHasher",
    "TypeHandlerRegistry",
    "BuiltinTypeHandlerRegistry",
    "get_default_type_handler_registry",
    "get_default_semantic_hasher",
    "ContentIdentifiableMixin",
    # Built-in handlers
    "PathContentHandler",
    "UUIDHandler",
    "BytesHandler",
    "FunctionHandler",
    "TypeObjectHandler",
    "register_builtin_handlers",
    # ---- Protocols ----
    "SemanticHasherProtocol",
    "ContentIdentifiableProtocol",
    "TypeHandlerProtocol",
    "FileContentHasherProtocol",
    "ArrowHasherProtocol",
    "StringCacherProtocol",
    "FunctionInfoExtractorProtocol",
    "SemanticTypeHasherProtocol",
    # ---- File hashing ----
    "BasicFileHasher",
    "CachedFileHasher",
    "hash_file",
    # ---- Legacy / backward-compatible ----
    # TODO: remove legacy section
    "get_default_arrow_hasher",
    "HashableMixin",
    "hash_to_hex",
    "hash_to_int",
    "hash_to_uuid",
    "hash_function",
    "get_function_signature",
    "function_content_hash",
    "hash_pathset",
    "hash_packet",
]
