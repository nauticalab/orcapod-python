"""
OrcaPod hashing package.

Public API
----------
  SemanticAwarePythonHasher            -- content-based recursive object hasher
  SemanticHasherProtocol               -- protocol for semantic hashers
  PythonTypeSemanticHasherRegistry     -- registry mapping types to PythonTypeSemanticHasherProtocol instances
  get_default_semantic_hasher          -- global default SemanticHasherProtocol factory
  get_default_python_type_semantic_hasher_registry -- global default registry factory
  ContentIdentifiableMixin             -- convenience mixin for content-identifiable objects

Built-in hashers (importable for custom registry setup):
  PathSemanticHasher
  UUIDSemanticHasher
  BytesSemanticHasher
  FunctionSemanticHasher
  TypeObjectSemanticHasher
  register_builtin_python_type_semantic_hashers

Utility:
  FileContentHasherProtocol
  StringCacherProtocol
  FunctionInfoExtractorProtocol
  ArrowHasherProtocol
"""

from orcapod.hashing.defaults import (
    get_default_arrow_hasher,
    get_default_python_type_semantic_hasher_registry,
    get_default_semantic_hasher,
)
from orcapod.hashing.file_hashers import BasicFileHasher, CachedFileHasher
from orcapod.hashing.hash_utils import hash_file
from orcapod.hashing.semantic_hashing.builtin_handlers import (
    BytesSemanticHasher,
    FunctionSemanticHasher,
    PathSemanticHasher,
    TypeObjectSemanticHasher,
    UUIDSemanticHasher,
    register_builtin_python_type_semantic_hashers,
)
from orcapod.hashing.semantic_hashing.content_identifiable_mixin import (
    ContentIdentifiableMixin,
)
from orcapod.hashing.semantic_hashing.semantic_hasher import SemanticAwarePythonHasher
from orcapod.hashing.semantic_hashing.type_handler_registry import (
    BuiltinPythonTypeSemanticHasherRegistry,
    PythonTypeSemanticHasherRegistry,
)
from orcapod.protocols.hashing_protocols import (
    ArrowHasherProtocol,
    ContentIdentifiableProtocol,
    FileContentHasherProtocol,
    FunctionInfoExtractorProtocol,
    PythonTypeSemanticHasherProtocol,
    SemanticHasherProtocol,
    SemanticTypeHasherProtocol,
    StringCacherProtocol,
)

try:
    from orcapod.hashing.legacy_core import (
        HashableMixin,
        function_content_hash,
        get_function_signature,
        hash_function,
        hash_data,
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
    hash_data = None  # type: ignore[assignment]
    hash_pathset = None  # type: ignore[assignment]
    hash_to_hex = None  # type: ignore[assignment]
    hash_to_int = None  # type: ignore[assignment]
    hash_to_uuid = None  # type: ignore[assignment]

__all__ = [
    "SemanticAwarePythonHasher",
    "PythonTypeSemanticHasherRegistry",
    "BuiltinPythonTypeSemanticHasherRegistry",
    "get_default_python_type_semantic_hasher_registry",
    "get_default_semantic_hasher",
    "ContentIdentifiableMixin",
    "PathSemanticHasher",
    "UUIDSemanticHasher",
    "BytesSemanticHasher",
    "FunctionSemanticHasher",
    "TypeObjectSemanticHasher",
    "register_builtin_python_type_semantic_hashers",
    "SemanticHasherProtocol",
    "ContentIdentifiableProtocol",
    "PythonTypeSemanticHasherProtocol",
    "FileContentHasherProtocol",
    "ArrowHasherProtocol",
    "StringCacherProtocol",
    "FunctionInfoExtractorProtocol",
    "SemanticTypeHasherProtocol",
    "BasicFileHasher",
    "CachedFileHasher",
    "hash_file",
    "get_default_arrow_hasher",
    "HashableMixin",
    "hash_to_hex",
    "hash_to_int",
    "hash_to_uuid",
    "hash_function",
    "get_function_signature",
    "function_content_hash",
    "hash_pathset",
    "hash_data",
]
