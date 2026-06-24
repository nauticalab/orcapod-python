"""
orcapod.hashing.semantic_hashing
=================================
  SemanticAwarePythonHasher           -- content-based recursive object hasher
  PythonTypeHandlerRegistry    -- MRO-aware registry mapping types → PythonTypeHandler
  BuiltinPythonTypeHandlerRegistry  -- pre-populated registry with built-in hashers
  ContentIdentifiableMixin            -- convenience mixin for content-identifiable objects

Built-in PythonTypeHandler implementations:
  PathHandler          -- pathlib.Path  → file-content hash
  UUIDHandler          -- uuid.UUID     → canonical bytes
  BytesHandler         -- bytes/bytearray → hex string
  FunctionHandler      -- callable      → via FunctionInfoExtractorProtocol
  TypeObjectHandler    -- type objects  → "type:<module>.<qualname>"
  register_builtin_python_type_handlers -- populate a registry with all of the above

Function info extractors (used by FunctionHandler):
  FunctionNameExtractor
  FunctionSignatureExtractor
  FunctionInfoExtractorFactory
"""

from orcapod.hashing.semantic_hashing.builtin_handlers import (
    BytesHandler,
    FunctionHandler,
    PathHandler,
    TypeObjectHandler,
    UUIDHandler,
    register_builtin_python_type_handlers,
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
    BuiltinPythonTypeHandlerRegistry,
    PythonTypeHandlerRegistry,
)

__all__ = [
    "SemanticAwarePythonHasher",
    "PythonTypeHandlerRegistry",
    "BuiltinPythonTypeHandlerRegistry",
    "ContentIdentifiableMixin",
    "PathHandler",
    "UUIDHandler",
    "BytesHandler",
    "FunctionHandler",
    "TypeObjectHandler",
    "register_builtin_python_type_handlers",
    "FunctionNameExtractor",
    "FunctionSignatureExtractor",
    "FunctionInfoExtractorFactory",
]
