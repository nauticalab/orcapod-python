"""
OrcaPod hashing package.

Public API
----------
  SemanticAwarePythonHasher            -- content-based recursive object hasher
  SemanticHasherProtocol               -- protocol for semantic hashers
  PythonTypeHandlerRegistry            -- registry mapping types to PythonTypeHandlerProtocol instances
  get_default_semantic_hasher          -- global default SemanticHasherProtocol factory
  get_default_python_type_handler_registry -- global default registry factory
  ContentIdentifiableMixin             -- convenience mixin for content-identifiable objects

Built-in hashers (importable for custom registry setup):
  UUIDHandler
  BytesHandler
  FunctionHandler
  TypeObjectHandler
  FileHandler
  register_builtin_python_type_handlers

Utility:
  FileContentHasherProtocol
  StringCacherProtocol
  FunctionInfoExtractorProtocol
  ArrowHasherProtocol
"""

from orcapod.hashing.defaults import (
    get_default_arrow_hasher,
    get_default_python_type_handler_registry,
    get_default_semantic_hasher,
)
from orcapod.hashing.file_hashers import BasicFileHasher, CachedFileHasher
from orcapod.hashing.hash_utils import hash_file
from orcapod.hashing.semantic_hashing.builtin_handlers import (
    BytesHandler,
    FileHandler,
    FunctionHandler,
    TypeObjectHandler,
    UUIDHandler,
    register_builtin_python_type_handlers,
)
from orcapod.hashing.semantic_hashing.content_identifiable_mixin import (
    ContentIdentifiableMixin,
)
from orcapod.hashing.semantic_hashing.semantic_hasher import SemanticAwarePythonHasher
from orcapod.hashing.semantic_hashing.type_handler_registry import (
    BuiltinPythonTypeHandlerRegistry,
    PythonTypeHandlerRegistry,
)
from orcapod.protocols.hashing_protocols import (
    ArrowHasherProtocol,
    ContentIdentifiableProtocol,
    FileContentHasherProtocol,
    FunctionInfoExtractorProtocol,
    PythonTypeHandlerProtocol,
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
    "PythonTypeHandlerRegistry",
    "BuiltinPythonTypeHandlerRegistry",
    "get_default_python_type_handler_registry",
    "get_default_semantic_hasher",
    "ContentIdentifiableMixin",
    "UUIDHandler",
    "BytesHandler",
    "FunctionHandler",
    "TypeObjectHandler",
    "FileHandler",
    "register_builtin_python_type_handlers",
    "SemanticHasherProtocol",
    "ContentIdentifiableProtocol",
    "PythonTypeHandlerProtocol",
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
