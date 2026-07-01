"""
Built-in PythonTypeHandlerProtocol implementations.

  UUIDHandler       -- uuid.UUID: 16-byte binary representation
  BytesHandler      -- bytes/bytearray: hex string representation
  FunctionHandler   -- callable with __code__: via FunctionInfoExtractorProtocol
  TypeObjectHandler -- type objects: stable "type:<module>.<qualname>" string
  SpecialFormHandler    -- typing._SpecialForm
  GenericAliasHandler   -- generic alias type annotations
  UnionTypeHandler      -- types.UnionType (Python 3.10+ X | Y syntax)
  ArrowTableHandler     -- pa.Table / pa.RecordBatch
  SchemaHandler         -- Schema objects
  FileHandler       -- orcapod.File: file content hash
  DirectoryHandler  -- orcapod.Directory: recursive Merkle tree hash

``register_builtin_python_type_handlers(registry)`` populates a registry
with all of the above.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any
from uuid import UUID

from orcapod.types import ContentHash, Schema

if TYPE_CHECKING:
    from orcapod.protocols.hashing_protocols import (
        ArrowHasherProtocol,
        DirectoryHasherProtocol,
        FileContentHasherProtocol,
        HandlerRegistryProtocol,
        SemanticHasherProtocol,
    )

logger = logging.getLogger(__name__)


class UUIDHandler:
    """Hasher for ``uuid.UUID`` objects — returns the raw 16-byte binary representation."""

    def handle(self, obj: Any, hasher: "SemanticHasherProtocol") -> Any:
        return obj.bytes


class BytesHandler:
    """Hasher for bytes and bytearray objects — returns the lowercase hex string."""

    def handle(self, obj: Any, hasher: "SemanticHasherProtocol") -> Any:
        if isinstance(obj, (bytes, bytearray)):
            return obj.hex()
        raise TypeError(
            f"BytesHandler: expected bytes or bytearray, got {type(obj)!r}"
        )


class FunctionHandler:
    """Hasher for Python functions/callables with a ``__code__`` attribute.

    Args:
        function_info_extractor: Any object with an
            ``extract_function_info(func) -> dict`` method.
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
    """Hasher for type objects (classes passed as values).

    Returns a stable string of the form ``"type:<module>.<qualname>"``.
    """

    def handle(self, obj: Any, hasher: "SemanticHasherProtocol") -> Any:
        if not isinstance(obj, type):
            raise TypeError(
                f"TypeObjectHandler: expected a type/class, got {type(obj)!r}"
            )
        module: str = obj.__module__ or "<unknown>"
        qualname: str = obj.__qualname__
        return f"type:{module}.{qualname}"


class SpecialFormHandler:
    """Hasher for ``typing._SpecialForm`` objects such as ``typing.Union``."""

    def handle(self, obj: Any, hasher: "SemanticHasherProtocol") -> Any:
        name = getattr(obj, "_name", None) or repr(obj)
        return f"special_form:typing.{name}"


class GenericAliasHandler:
    """Hasher for generic alias type annotations (``dict[int, str]``, ``Optional[X]``, etc.)."""

    def handle(self, obj: Any, hasher: "SemanticHasherProtocol") -> Any:
        import typing

        origin = getattr(obj, "__origin__", None)
        args = getattr(obj, "__args__", None) or ()
        if origin is None:
            return f"generic_alias:{obj!r}"
        if origin is typing.Union:
            hashed_args = sorted(hasher.hash_object(arg).to_string() for arg in args)
            return {"__type__": "union", "args": hashed_args}
        return {
            "__type__": "generic_alias",
            "origin": hasher.hash_object(origin).to_string(),
            "args": [hasher.hash_object(arg).to_string() for arg in args],
        }


class UnionTypeHandler:
    """Hasher for ``types.UnionType`` objects (Python 3.10+ ``X | Y`` syntax)."""

    def handle(self, obj: Any, hasher: "SemanticHasherProtocol") -> Any:
        args = getattr(obj, "__args__", None) or ()
        hashed_args = sorted(hasher.hash_object(arg).to_string() for arg in args)
        return {"__type__": "union", "args": hashed_args}


class ArrowTableHandler:
    """Hasher for ``pa.Table`` and ``pa.RecordBatch`` objects.

    Args:
        arrow_hasher: Any object satisfying ``ArrowHasherProtocol``.  When
            ``None``, the default data context's ``arrow_hasher`` is resolved
            lazily at call time (breaking the circular dependency that would
            arise if the registry were constructed before the arrow hasher).
    """

    def __init__(self, arrow_hasher: "ArrowHasherProtocol | None" = None) -> None:
        self._arrow_hasher = arrow_hasher

    def _get_arrow_hasher(self) -> "ArrowHasherProtocol":
        if self._arrow_hasher is not None:
            return self._arrow_hasher
        from orcapod.contexts import get_default_context
        return get_default_context().arrow_hasher  # type: ignore[return-value]

    def handle(self, obj: Any, hasher: "SemanticHasherProtocol") -> ContentHash:
        import pyarrow as _pa

        if isinstance(obj, _pa.RecordBatch):
            obj = _pa.Table.from_batches([obj])
        if not isinstance(obj, _pa.Table):
            raise TypeError(
                f"ArrowTableHandler: expected pa.Table or pa.RecordBatch, got {type(obj)!r}"
            )
        return self._get_arrow_hasher().hash_table(obj)


class SchemaHandler:
    """Hasher for ``Schema`` objects."""

    def handle(self, obj: Any, hasher: "SemanticHasherProtocol") -> Any:
        if not isinstance(obj, Schema):
            raise TypeError(
                f"SchemaHandler: expected a Schema, got {type(obj)!r}"
            )
        raise NotImplementedError("SchemaHandler is not yet implemented.")


class FileHandler:
    """Hasher for ``orcapod.File`` objects — hashes file *content*.

    By the time ``handle`` is called, ``File``'s constructor has already validated
    that the path exists and is a non-directory file (and is not a symlink when
    ``follow_symlinks=False``). The hash is produced by reading file bytes through
    the wrapped ``UPath``, which follows symlinks by default.

    Args:
        file_hasher: Any object with a ``hash_file(path) -> ContentHash`` method.
    """

    def __init__(self, file_hasher: "FileContentHasherProtocol") -> None:
        self.file_hasher = file_hasher

    def handle(self, obj: Any, hasher: "SemanticHasherProtocol") -> ContentHash:
        # Deferred import breaks the circular dependency between this module and
        # file_type.py — the same pattern used by ArrowTableHandler.
        from orcapod.extension_types.file_type import File
        if not isinstance(obj, File):
            raise TypeError(
                f"FileHandler: expected an orcapod.File, got {type(obj)!r}"
            )
        wrapped = getattr(obj, "__wrapped__")
        logger.debug("FileHandler: hashing file content at %s", wrapped)
        return self.file_hasher.hash_file(wrapped)


class DirectoryHandler:
    """Hasher for ``orcapod.Directory`` objects — hashes directory *content* via Merkle tree.

    When a ``Directory`` is created via its normal constructor, existence and
    traversability are validated at construction time. Derived ``Directory`` instances
    created by path-navigation operations (e.g. ``.parent``, ``/`` operator) bypass that
    validation via ``_from_upath`` — so existence is not guaranteed at ``handle`` time.
    The hash is produced by ``BasicDirectoryHasher`` using a recursive Merkle scheme.

    Args:
        directory_hasher: Any object with a
            ``hash_directory(path, ignore) -> ContentHash`` method.
    """

    def __init__(self, directory_hasher: "DirectoryHasherProtocol") -> None:
        self.directory_hasher = directory_hasher

    def handle(self, obj: Any, hasher: "SemanticHasherProtocol") -> ContentHash:
        # Deferred import breaks the circular dependency between this module and
        # directory_type.py — the same pattern used by FileHandler.
        from orcapod.extension_types.directory_type import Directory
        if not isinstance(obj, Directory):
            raise TypeError(
                f"DirectoryHandler: expected an orcapod.Directory, got {type(obj)!r}"
            )
        wrapped = getattr(obj, "__wrapped__")
        ignore = getattr(obj, "_ignore", None)
        logger.debug("DirectoryHandler: hashing directory content at %s", wrapped)
        return self.directory_hasher.hash_directory(wrapped, ignore=ignore)


class NumpyArrayHandler:
    """Hasher for ``numpy.ndarray`` — content hash via numpy's ``.npy`` binary format.

    Returns the raw ``.npy`` bytes produced by ``np.save``, which encode dtype
    (including structured/record field names), shape, byte order, and data in
    numpy's stable on-disk format. This is identical to what ``LogicalNumpyArray``
    stores in Arrow, so the hash input and storage bytes are always consistent.

    Object-dtype arrays are rejected with ``ValueError`` immediately (before
    ``np.save`` is called). Structured dtypes containing object-typed fields
    are caught by ``allow_pickle=False`` in ``np.save``.
    """

    def handle(self, obj: Any, hasher: "SemanticHasherProtocol") -> bytes:
        """Return the ``.npy`` bytes for ``obj``.

        Args:
            obj: A ``numpy.ndarray`` instance.
            hasher: Ignored. Present for protocol conformance.

        Returns:
            Raw ``.npy`` bytes encoding dtype, shape, byte order, and data.

        Raises:
            TypeError: If ``obj`` is not a ``numpy.ndarray``.
            ValueError: If ``obj`` has ``dtype.kind == "O"`` (object dtype).
        """
        import io
        import numpy as np
        if not isinstance(obj, np.ndarray):
            raise TypeError(
                f"NumpyArrayHandler: expected numpy.ndarray, got {type(obj)!r}"
            )
        if obj.dtype.kind == "O":
            raise ValueError(
                f"NumpyArrayHandler does not support object-dtype arrays "
                f"(dtype={obj.dtype!r}). Object arrays require pickling, "
                "which is disabled for security."
            )
        buf = io.BytesIO()
        np.save(buf, obj, allow_pickle=False)
        return buf.getvalue()


def register_builtin_python_type_handlers(
    registry: "HandlerRegistryProtocol",
    file_hasher: Any = None,
    function_info_extractor: Any = None,
    arrow_hasher: "ArrowHasherProtocol | None" = None,
    directory_hasher: Any = None,
) -> None:
    """Register all built-in semantic hashers into *registry*.

    ``pa.Table`` and ``pa.RecordBatch`` are always registered via
    ``ArrowTableHandler``. When ``arrow_hasher`` is provided it is
    passed through for immediate use; when ``None``, ``ArrowTableHandler``
    resolves the active arrow hasher lazily via ``get_default_context()`` at
    hash time, breaking the construction-time circular dependency.

    ``orcapod.File`` is registered via ``FileHandler`` for content-based file
    hashing. ``orcapod.Directory`` is registered via ``DirectoryHandler`` for
    recursive Merkle tree directory hashing. ``pathlib.Path`` and ``upath.UPath``
    are NOT registered here. When these types appear in pipeline columns they are
    handled at the Arrow level through their ``LogicalPath`` / ``LogicalUPath``
    extension types, which store the path string in ``large_string()`` storage.
    The Arrow hasher then operates directly on that string storage — no Python-level
    roundtrip and no file I/O occurs. Passing a raw ``Path`` or ``UPath`` directly to
    the Python semantic hasher raises ``TypeError`` in strict mode (the default).

    Args:
        registry: The ``HandlerRegistryProtocol`` instance to populate.
        file_hasher: Optional ``FileContentHasherProtocol`` for file content hashing.
            Defaults to ``BasicFileHasher(sha256)``.
        function_info_extractor: Optional ``FunctionInfoExtractorProtocol``.
            Defaults to ``FunctionSignatureExtractor``.
        arrow_hasher: Optional ``ArrowHasherProtocol`` for nested table hashing.
            When ``None``, lazy resolution via the default context is used.
        directory_hasher: Optional ``DirectoryHasherProtocol`` for directory tree hashing.
            Defaults to ``BasicDirectoryHasher(sha256)``.
    """
    if file_hasher is None:
        from orcapod.hashing.file_hashers import BasicFileHasher
        file_hasher = BasicFileHasher(algorithm="sha256")

    if directory_hasher is None:
        from orcapod.hashing.directory_hashers import BasicDirectoryHasher
        directory_hasher = BasicDirectoryHasher(algorithm="sha256")

    if function_info_extractor is None:
        from orcapod.hashing.semantic_hashing.function_info_extractors import (
            FunctionSignatureExtractor,
        )
        function_info_extractor = FunctionSignatureExtractor(
            include_module=True,
            include_defaults=True,
        )

    bytes_hasher = BytesHandler()
    registry.register(bytes, bytes_hasher)
    registry.register(bytearray, bytes_hasher)

    registry.register(UUID, UUIDHandler())

    from orcapod.extension_types.file_type import File
    registry.register(File, FileHandler(file_hasher))

    from orcapod.extension_types.directory_type import Directory
    registry.register(Directory, DirectoryHandler(directory_hasher))

    import types as _types

    function_hasher = FunctionHandler(function_info_extractor)
    registry.register(_types.FunctionType, function_hasher)
    registry.register(_types.BuiltinFunctionType, function_hasher)
    registry.register(_types.MethodType, function_hasher)

    registry.register(type, TypeObjectHandler())
    registry.register(_types.UnionType, UnionTypeHandler())

    generic_alias_hasher = GenericAliasHandler()
    registry.register(_types.GenericAlias, generic_alias_hasher)
    try:
        import typing as _typing
        registry.register(_typing._GenericAlias, generic_alias_hasher)  # type: ignore[attr-defined]
        registry.register(_typing._SpecialForm, SpecialFormHandler())  # type: ignore[attr-defined]
    except AttributeError:
        pass

    registry.register(Schema, SchemaHandler())

    import pyarrow as _pa
    arrow_table_hasher = ArrowTableHandler(arrow_hasher)
    registry.register(_pa.Table, arrow_table_hasher)
    registry.register(_pa.RecordBatch, arrow_table_hasher)

    logger.debug(
        "register_builtin_python_type_handlers: registered %d hashers",
        len(registry),
    )
