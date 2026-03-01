"""
Built-in TypeHandlerProtocol implementations for the SemanticHasherProtocol system.

This module provides handlers for all Python types that the SemanticHasherProtocol
knows how to process out of the box:

  - PathContentHandler    -- pathlib.Path: returns ContentHash of file content
  - UUIDHandler           -- uuid.UUID: canonical string representation
  - BytesHandler          -- bytes / bytearray: hex string representation
  - FunctionHandler       -- callable with __code__: via FunctionInfoExtractorProtocol
  - TypeObjectHandler     -- type objects (classes): stable "type:<name>" string

Note: ContentHash requires no handler -- it is recognised as a terminal by
``hash_object`` and returned as-is.

The module also exposes ``register_builtin_handlers(registry)`` which is
called automatically when the global default registry is first accessed.

Extending the system
--------------------
To add a handler for a third-party type, create a class that implements the
TypeHandlerProtocol protocol (a single ``handle(obj, hasher)`` method) and register
it:

    from orcapod.hashing.semantic_hashing.type_handler_registry import get_default_type_handler_registry
    get_default_type_handler_registry().register(MyType, MyTypeHandler())
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any
from uuid import UUID

from orcapod.types import PathLike, Schema

if TYPE_CHECKING:
    from orcapod.hashing.semantic_hashing.type_handler_registry import (
        TypeHandlerRegistry,
    )
    from orcapod.protocols.hashing_protocols import (
        ArrowHasherProtocol,
        FileContentHasherProtocol,
        SemanticHasherProtocol,
    )

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Individual handlers
# ---------------------------------------------------------------------------


class PathContentHandler:
    """
    Handler for pathlib.Path objects.

    Hashes the *content* of the file at the given path using the injected
    FileContentHasherProtocol, producing a stable content-addressed identifier.
    The resulting bytes are stored as a hex string embedded in the resolved
    structure.

    The path must refer to an existing, readable file.  Directories and
    missing paths are not supported and will raise an error -- if you need
    a path-as-string handler, register a separate handler for that use case
    or return a ``str`` from ``identity_structure()`` instead of a ``Path``.

    Args:
        file_hasher: Any object with a ``hash_file(path) -> ContentHash``
                     method (satisfies the FileContentHasherProtocol protocol).
    """

    def __init__(self, file_hasher: FileContentHasherProtocol) -> None:
        self.file_hasher = file_hasher

    def handle(self, obj: PathLike, hasher: "SemanticHasherProtocol") -> Any:
        path: Path = Path(obj)

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
        return self.file_hasher.hash_file(path)


class UUIDHandler:
    """
    Handler for uuid.UUID objects.

    Converts the UUID to its canonical hyphenated string representation
    (e.g. ``"550e8400-e29b-41d4-a716-446655440000"``), which is stable,
    human-readable, and unambiguous.
    """

    def handle(self, obj: Any, hasher: "SemanticHasherProtocol") -> Any:
        return str(obj)


class BytesHandler:
    """
    Handler for bytes and bytearray objects.

    Converts binary data to its lowercase hex string representation.  This
    avoids JSON serialisation issues with raw bytes while preserving the
    exact byte sequence in the hash input.
    """

    def handle(self, obj: Any, hasher: "SemanticHasherProtocol") -> Any:
        if isinstance(obj, (bytes, bytearray)):
            return obj.hex()
        raise TypeError(f"BytesHandler: expected bytes or bytearray, got {type(obj)!r}")


class FunctionHandler:
    """
    Handler for Python functions / callables that carry a ``__code__`` attribute.

    Delegates to a FunctionInfoExtractorProtocol to produce a stable, serialisable
    dict representation of the function.  The extractor is responsible for
    deciding which parts of the function (name, signature, source body, etc.)
    are included.

    Args:
        function_info_extractor: Any object with an
            ``extract_function_info(func) -> dict`` method (satisfies the
            FunctionInfoExtractorProtocol protocol).
    """

    def __init__(self, function_info_extractor: Any) -> None:
        self.function_info_extractor = function_info_extractor

    def handle(self, obj: Any, hasher: "SemanticHasherProtocol") -> Any:
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

    def handle(self, obj: Any, hasher: "SemanticHasherProtocol") -> Any:
        if not isinstance(obj, type):
            raise TypeError(
                f"TypeObjectHandler: expected a type/class, got {type(obj)!r}"
            )
        module: str = obj.__module__ or "<unknown>"
        qualname: str = obj.__qualname__
        return f"type:{module}.{qualname}"


class ArrowTableHandler:
    """
    Handler for ``pa.Table`` and ``pa.RecordBatch`` objects.

    Delegates to the injected ``ArrowHasherProtocol`` to produce a stable,
    content-addressed ``ContentHash`` of the Arrow table data.  The returned
    ``ContentHash`` is recognised as a terminal by ``hash_object`` and
    returned as-is — no further recursion occurs.

    Args:
        arrow_hasher: Any object satisfying ArrowHasherProtocol (i.e. has a
                      ``hash_table(table) -> ContentHash`` method).
    """

    def __init__(self, arrow_hasher: ArrowHasherProtocol) -> None:
        self.arrow_hasher = arrow_hasher

    def handle(self, obj: Any, hasher: "SemanticHasherProtocol") -> Any:
        import pyarrow as _pa

        if isinstance(obj, _pa.RecordBatch):
            obj = _pa.Table.from_batches([obj])
        if not isinstance(obj, _pa.Table):
            raise TypeError(
                f"ArrowTableHandler: expected pa.Table or pa.RecordBatch, got {type(obj)!r}"
            )
        return self.arrow_hasher.hash_table(obj)


class SchemaHandler:
    """
    Handler for :class:`~orcapod.types.Schema` objects.

    Produces a stable dict containing both the field-type mapping and the
    sorted list of optional field names, so that two schemas differing only
    in which fields are optional produce different hashes.
    """

    def handle(self, obj: Any, hasher: "SemanticHasherProtocol") -> Any:
        if not isinstance(obj, Schema):
            raise TypeError(f"SchemaHandler: expected a Schema, got {type(obj)!r}")
        # schema handler is not implemented yet
        raise NotImplementedError()
        # visited: frozenset[int] = frozenset()

        # return {
        #     "fields": {k: hasher._expand_element(v, visited) for k, v in obj.items()},
        #     "optional_fields": sorted(obj.optional_fields),
        # }


# ---------------------------------------------------------------------------
# Registration helper
# ---------------------------------------------------------------------------


def register_builtin_handlers(
    registry: "TypeHandlerRegistry",
    file_hasher: Any = None,
    function_info_extractor: Any = None,
    arrow_hasher: "ArrowHasherProtocol | None" = None,
) -> None:
    """
    Register all built-in TypeHandlers into *registry*.

    This function is called automatically when the global default registry is
    first accessed via ``get_default_type_handler_registry()``.  It can also
    be called manually to populate a custom registry.

    Path, function, and Arrow table handling require auxiliary objects.
    When these are not supplied, sensible defaults are constructed:

      - ``BasicFileHasher`` (SHA-256, 64 KiB buffer) for Path handling.
      - ``FunctionSignatureExtractor`` for function handling.
      - ``SemanticArrowHasher`` (SHA-256, logical serialisation) for Arrow table handling.

    Args:
        registry:
            The TypeHandlerRegistry to populate.
        file_hasher:
            Optional object satisfying FileContentHasherProtocol (i.e. has a
            ``hash_file(path) -> ContentHash`` method).  Defaults to a
            ``BasicFileHasher`` configured with SHA-256.
        function_info_extractor:
            Optional object satisfying FunctionInfoExtractorProtocol (i.e. has an
            ``extract_function_info(func) -> dict`` method).  Defaults to
            ``FunctionSignatureExtractor``.
        arrow_hasher:
            Optional object satisfying ArrowHasherProtocol (i.e. has a
            ``hash_table(table) -> ContentHash`` method).  Defaults to a
            ``SemanticArrowHasher`` configured with SHA-256 and logical serialisation.
            Should be the data context's arrow hasher when called from a versioned
            context so that hashing is consistent across all components.
    """
    # Resolve defaults for auxiliary objects ----------------------------
    if file_hasher is None:
        from orcapod.hashing.file_hashers import BasicFileHasher  # stays in hashing/

        file_hasher = BasicFileHasher(algorithm="sha256")

    if function_info_extractor is None:
        from orcapod.hashing.semantic_hashing.function_info_extractors import (
            FunctionSignatureExtractor,
        )

        function_info_extractor = FunctionSignatureExtractor(
            include_module=True,
            include_defaults=True,
        )

    if arrow_hasher is None:
        from orcapod.hashing.arrow_hashers import SemanticArrowHasher
        from orcapod.semantic_types.semantic_registry import SemanticTypeRegistry

        arrow_hasher = SemanticArrowHasher(
            semantic_registry=SemanticTypeRegistry(),
            hasher_id="arrow_v0.1",
            hash_algorithm="sha256",
            serialization_method="logical",
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

    # Note: ContentHash needs no handler -- SemanticHasherProtocol treats it as
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

    # Schema objects -- must come after type handler so Schema is matched
    # specifically rather than falling through to the Mapping expansion path
    registry.register(Schema, SchemaHandler())

    # Arrow tables and record batches -- delegate to the injected arrow hasher
    import pyarrow as _pa

    arrow_table_handler = ArrowTableHandler(arrow_hasher)
    registry.register(_pa.Table, arrow_table_handler)
    registry.register(_pa.RecordBatch, arrow_table_handler)

    logger.debug(
        "register_builtin_handlers: registered %d built-in handlers",
        len(registry),
    )
