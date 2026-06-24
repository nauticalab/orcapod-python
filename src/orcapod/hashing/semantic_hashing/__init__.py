"""
orcapod.hashing.semantic_hashing
=================================
  SemanticAwarePythonHasher           -- content-based recursive object hasher
  PythonTypeSemanticHasherRegistry    -- MRO-aware registry mapping types → PythonTypeSemanticHasherProtocol
  BuiltinPythonTypeSemanticHasherRegistry  -- pre-populated registry with built-in hashers
  ContentIdentifiableMixin            -- convenience mixin for content-identifiable objects

Built-in PythonTypeSemanticHasherProtocol implementations:
  PathSemanticHasher          -- pathlib.Path  → file-content hash
  UUIDSemanticHasher          -- uuid.UUID     → canonical bytes
  BytesSemanticHasher         -- bytes/bytearray → hex string
  FunctionSemanticHasher      -- callable      → via FunctionInfoExtractorProtocol
  TypeObjectSemanticHasher    -- type objects  → "type:<module>.<qualname>"
  register_builtin_python_type_semantic_hashers -- populate a registry with all of the above

Function info extractors (used by FunctionSemanticHasher):
  FunctionNameExtractor
  FunctionSignatureExtractor
  FunctionInfoExtractorFactory
"""

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
from orcapod.hashing.semantic_hashing.function_info_extractors import (
    FunctionInfoExtractorFactory,
    FunctionNameExtractor,
    FunctionSignatureExtractor,
)
from orcapod.hashing.semantic_hashing.semantic_hasher import SemanticAwarePythonHasher
from orcapod.hashing.semantic_hashing.type_handler_registry import (
    BuiltinPythonTypeSemanticHasherRegistry,
    PythonTypeSemanticHasherRegistry,
)

__all__ = [
    "SemanticAwarePythonHasher",
    "PythonTypeSemanticHasherRegistry",
    "BuiltinPythonTypeSemanticHasherRegistry",
    "ContentIdentifiableMixin",
    "PathSemanticHasher",
    "UUIDSemanticHasher",
    "BytesSemanticHasher",
    "FunctionSemanticHasher",
    "TypeObjectSemanticHasher",
    "register_builtin_python_type_semantic_hashers",
    "FunctionNameExtractor",
    "FunctionSignatureExtractor",
    "FunctionInfoExtractorFactory",
]
