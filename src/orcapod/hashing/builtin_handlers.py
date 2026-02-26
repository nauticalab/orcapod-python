"""
Built-in TypeHandler implementations for the SemanticHasher system.

This module provides handlers for all Python types that the SemanticHasher
knows how to process out of the box:

  - PathContentHandler    -- pathlib.Path: returns ContentHash of file content
  - UUIDHandler           -- uuid.UUID: canonical string representation
  - BytesHandler          -- bytes / bytearray: hex string representation
  - FunctionHandler       -- callable with __code__: via FunctionInfoExtractor
  - TypeObjectHandler     -- type objects (classes): stable "type:<name>" string

Note: ContentHash requires no handler -- it is recognised as a terminal by
``hash_object`` and returned as-is.

The module also exposes ``register_builtin_handlers(registry)`` which is
called automatically when the global default registry is first accessed.

Extending the system
--------------------
To add a handler for a third-party type, create a class that implements the
TypeHandler protocol (a single ``handle(obj, hasher)`` method) and register
it:

    from orcapod.hashing.type_handler_registry import get_default_type_handler_registry
    get_default_type_handler_registry().register(MyType, MyTypeHandler())
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any
from uuid import UUID

from orcapod.types import ContentHash

if TYPE_CHECKING:
    from orcapod.hashing.type_handler_registry import TypeHandlerRegistry
    from orcapod.protocols.hashing_protocols import SemanticHasher

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Individual handlers
# ---------------------------------------------------------------------------


class PathContentHandler:
    """
    Handler for pathlib.Path objects.

    Hashes the *content* of the file at the given path using the injected
    FileContentHasher, producing a stable content-addressed identifier.
    The resulting bytes are stored as a hex string embedded in the resolved
    structure.

    The path must refer to an existing, readable file.  Directories and
    missing paths are not supported and will raise an error -- if you need
    a path-as-string handler, register a separate handler for that use case
    or return a ``str`` from ``identity_structure()`` instead of a ``Path``.

    Args:
        file_hasher: Any object with a ``hash_file(path) -> ContentHash``
                     method (satisfies the FileContentHasher protocol).
    """

    def __init__(self, file_hasher: Any) -> None:
        self.file_hasher = file_hasher

    def handle(self, obj: Any, hasher: "SemanticHasher") -> Any:
        path: Path = obj if isinstance(obj, Path) else Path(obj)

        if not path.exists():
            raise FileNotFoundError(
                f"PathContentHandler: path does not exist: {path!r}. "
                "Paths must refer to existing files for content-based hashing. "
                "If you intended to hash the path string, return str(path) from "
                "identity_structure() instead of a Path object."
            )

        if path.is_dir():
            raise IsADirectoryError(
                f"PathContentHandler: path is a directory: {path!r}. "
                "Only regular files are supported for content-based hashing."
            )

        logger.debug("PathContentHandler: hashing file content at %s", path)
        result = self.file_hasher.hash_file(path)
        # hash_file returns a ContentHash. SemanticHasher treats ContentHash
        # as a terminal -- so returning it directly means no re-hashing occurs.
        if isinstance(result, ContentHash):
            return result
        # Legacy file hashers may return raw bytes; wrap in a ContentHash.
        if isinstance(result, (bytes, bytearray)):
            return ContentHash("file-sha256", bytes(result))
        # Fallback: wrap unknown return types as a string-method ContentHash.
        return ContentHash("file-unknown", str(result).encode())


class UUIDHandler:
    """
    Handler for uuid.UUID objects.

    Converts the UUID to its canonical hyphenated string representation
    (e.g. ``"550e8400-e29b-41d4-a716-446655440000"``), which is stable,
    human-readable, and unambiguous.
    """

    def handle(self, obj: Any, hasher: "SemanticHasher") -> Any:
        return str(obj)


class BytesHandler:
    """
    Handler for bytes and bytearray objects.

    Converts binary data to its lowercase hex string representation.  This
    avoids JSON serialisation issues with raw bytes while preserving the
    exact byte sequence in the hash input.
    """

    def handle(self, obj: Any, hasher: "SemanticHasher") -> Any:
        if isinstance(obj, (bytes, bytearray)):
            return obj.hex()
        raise TypeError(f"BytesHandler: expected bytes or bytearray, got {type(obj)!r}")


class FunctionHandler:
    """
    Handler for Python functions / callables that carry a ``__code__`` attribute.

    Delegates to a FunctionInfoExtractor to produce a stable, serialisable
    dict representation of the function.  The extractor is responsible for
    deciding which parts of the function (name, signature, source body, etc.)
    are included.

    Args:
        function_info_extractor: Any object with an
            ``extract_function_info(func) -> dict`` method (satisfies the
            FunctionInfoExtractor protocol).
    """

    def __init__(self, function_info_extractor: Any) -> None:
        self.function_info_extractor = function_info_extractor

    def handle(self, obj: Any, hasher: "SemanticHasher") -> Any:
        if not (callable(obj) and hasattr(obj, "__code__")):
            raise TypeError(
                f"FunctionHandler: expected a callable with __code__, got {type(obj)!r}"
            )
        func_name = getattr(obj, "__name__", repr(obj))
        logger.debug("FunctionHandler: extracting info for function %r", func_name)
        info: dict[str, Any] = self.function_info_extractor.extract_function_info(obj)
        return info


class TypeObjectHandler:
    """
    Handler for type objects (i.e. classes passed as values).

    Returns a stable string of the form ``"type:<module>.<qualname>"`` so
    that different classes always produce different hash inputs and the
    result is human-readable.
    """

    def handle(self, obj: Any, hasher: "SemanticHasher") -> Any:
        if not isinstance(obj, type):
            raise TypeError(
                f"TypeObjectHandler: expected a type/class, got {type(obj)!r}"
            )
        module: str = obj.__module__ or "<unknown>"
        qualname: str = obj.__qualname__
        return f"type:{module}.{qualname}"


# ---------------------------------------------------------------------------
# Registration helper
# ---------------------------------------------------------------------------


def register_builtin_handlers(
    registry: "TypeHandlerRegistry",
    file_hasher: Any = None,
    function_info_extractor: Any = None,
) -> None:
    """
    Register all built-in TypeHandlers into *registry*.

    This function is called automatically when the global default registry is
    first accessed via ``get_default_type_handler_registry()``.  It can also
    be called manually to populate a custom registry.

    Path and function handling require auxiliary objects (a FileContentHasher
    and a FunctionInfoExtractor respectively).  When these are not supplied,
    sensible defaults are constructed:

      - ``BasicFileHasher`` (SHA-256, 64 KiB buffer) for Path handling.
      - ``FunctionSignatureExtractor`` for function handling.

    Args:
        registry:
            The TypeHandlerRegistry to populate.
        file_hasher:
            Optional object satisfying FileContentHasher (i.e. has a
            ``hash_file(path) -> ContentHash`` method).  Defaults to a
            ``BasicFileHasher`` configured with SHA-256.
        function_info_extractor:
            Optional object satisfying FunctionInfoExtractor (i.e. has an
            ``extract_function_info(func) -> dict`` method).  Defaults to
            ``FunctionSignatureExtractor``.
    """
    # Resolve defaults for auxiliary objects ----------------------------
    if file_hasher is None:
        from orcapod.hashing.file_hashers import BasicFileHasher

        file_hasher = BasicFileHasher(algorithm="sha256")

    if function_info_extractor is None:
        from orcapod.hashing.function_info_extractors import FunctionSignatureExtractor

        function_info_extractor = FunctionSignatureExtractor(
            include_module=True,
            include_defaults=True,
        )

    # Register handlers -------------------------------------------------

    # bytes / bytearray
    bytes_handler = BytesHandler()
    registry.register(bytes, bytes_handler)
    registry.register(bytearray, bytes_handler)

    # pathlib.Path (and subclasses such as PosixPath / WindowsPath)
    registry.register(Path, PathContentHandler(file_hasher))

    # uuid.UUID
    registry.register(UUID, UUIDHandler())

    # Note: ContentHash needs no handler -- SemanticHasher treats it as
    # a terminal in hash_object() and returns it as-is.

    # Functions -- register types.FunctionType so MRO lookup works for
    # plain ``def`` functions, plus built-in functions and bound methods.
    import types as _types

    function_handler = FunctionHandler(function_info_extractor)
    registry.register(_types.FunctionType, function_handler)
    registry.register(_types.BuiltinFunctionType, function_handler)
    registry.register(_types.MethodType, function_handler)

    # type objects (classes used as values, e.g. passed in a dict)
    registry.register(type, TypeObjectHandler())

    logger.debug(
        "register_builtin_handlers: registered %d built-in handlers",
        len(registry),
    )
