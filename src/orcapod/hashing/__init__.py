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
  DirectoryHandler                     -- built-in handler for orcapod.Directory
  register_builtin_python_type_handlers

File hashing:
  FileHasher                           -- content hasher for individual files
  FileHashKey                          -- frozen dataclass cache key (path, mtime_ns, size)
  CachedFileHasher                     -- caching decorator around FileContentHasherProtocol
  InMemoryHashCacher                   -- dict-backed cacher for testing
  SqliteHashCacher                     -- SQLite-backed persistent cacher
  CachePopulationStats                 -- stats returned by populate_hash_cache()
  populate_hash_cache                  -- pre-populate the SQLite hash cache for large files

Utility:
  CacherProtocol                       -- generic get/put caching protocol [K, V]
  FileContentHasherProtocol
  StringCacherProtocol
  FunctionInfoExtractorProtocol
  ArrowHasherProtocol
  BasicDirectoryHasher                 -- recursive Merkle tree directory hasher
  DirectoryHasherProtocol              -- protocol for directory hashers
"""

from orcapod.hashing.defaults import (
    get_default_arrow_hasher,
    get_default_python_type_handler_registry,
    get_default_semantic_hasher,
)
from orcapod.hashing.file_hashers import CachedFileHasher, FileHasher, FileHashKey
from orcapod.hashing.cache_population import (
    CachePopulationStats,
    FileOutcome,
    ProgressCallback,
    populate_hash_cache,
)
from orcapod.hashing.hash_cachers import InMemoryHashCacher, SqliteHashCacher

try:
    from orcapod.hashing.postgres_hash_cacher import PostgresHashCacher
except ImportError:  # pragma: no cover
    PostgresHashCacher = None  # type: ignore[assignment,misc]

from orcapod.hashing.directory_hashers import BasicDirectoryHasher
from orcapod.hashing.hash_utils import hash_file
from orcapod.hashing.semantic_hashing.builtin_handlers import (
    BytesHandler,
    DirectoryHandler,
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
    CacherProtocol,
    ContentIdentifiableProtocol,
    DirectoryHasherProtocol,
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
    "DirectoryHandler",
    "register_builtin_python_type_handlers",
    "SemanticHasherProtocol",
    "ContentIdentifiableProtocol",
    "PythonTypeHandlerProtocol",
    "FileContentHasherProtocol",
    "ArrowHasherProtocol",
    "CacherProtocol",
    "StringCacherProtocol",
    "FunctionInfoExtractorProtocol",
    "SemanticTypeHasherProtocol",
    "FileHasher",
    "FileHashKey",
    "CachedFileHasher",
    "InMemoryHashCacher",
    "SqliteHashCacher",
    "PostgresHashCacher",
    "CachePopulationStats",
    "FileOutcome",
    "ProgressCallback",
    "populate_hash_cache",
    "BasicDirectoryHasher",
    "DirectoryHasherProtocol",
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
