"""
orcapod.hashing.semantic_hashing
=================================
Sub-package containing all components of the semantic hashing system:

  BaseSemanticHasher          -- content-based recursive object hasher
  TypeHandlerRegistry         -- MRO-aware registry mapping types → TypeHandlerProtocol
  BuiltinTypeHandlerRegistry  -- pre-populated registry with built-in handlers
  ContentIdentifiableMixin    -- convenience mixin for content-identifiable objects

Built-in TypeHandlerProtocol implementations:
  PathContentHandler          -- pathlib.Path  → file-content hash
  UUIDHandler                 -- uuid.UUID     → canonical string
  BytesHandler                -- bytes/bytearray → hex string
  FunctionHandler             -- callable      → via FunctionInfoExtractorProtocol
  TypeObjectHandler           -- type objects  → "type:<module>.<qualname>"
  register_builtin_handlers   -- populate a registry with all of the above

Function info extractors (used by FunctionHandler):
  FunctionNameExtractor
  FunctionSignatureExtractor
  FunctionInfoExtractorFactory
"""

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
from orcapod.hashing.semantic_hashing.function_info_extractors import (
    FunctionInfoExtractorFactory,
    FunctionNameExtractor,
    FunctionSignatureExtractor,
)
from orcapod.hashing.semantic_hashing.semantic_hasher import BaseSemanticHasher
from orcapod.hashing.semantic_hashing.type_handler_registry import (
    BuiltinTypeHandlerRegistry,
    TypeHandlerRegistry,
)

__all__ = [
    # Core hasher
    "BaseSemanticHasher",
    # Registry
    "TypeHandlerRegistry",
    "BuiltinTypeHandlerRegistry",
    # Mixin
    "ContentIdentifiableMixin",
    # Built-in handlers
    "PathContentHandler",
    "UUIDHandler",
    "BytesHandler",
    "FunctionHandler",
    "TypeObjectHandler",
    "register_builtin_handlers",
    # Function info extractors
    "FunctionNameExtractor",
    "FunctionSignatureExtractor",
    "FunctionInfoExtractorFactory",
]
