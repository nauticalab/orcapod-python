"""
Built-in PythonTypeHandler implementations.

  PathSemanticHasher       -- pathlib.Path: file content hash
  UPathSemanticHasher      -- upath.UPath: file content hash (remote-aware)
  UUIDSemanticHasher       -- uuid.UUID: 16-byte binary representation
  BytesSemanticHasher      -- bytes/bytearray: hex string representation
  FunctionSemanticHasher   -- callable with __code__: via FunctionInfoExtractorProtocol
  TypeObjectSemanticHasher -- type objects: stable "type:<module>.<qualname>" string
  SpecialFormSemanticHasher    -- typing._SpecialForm
  GenericAliasSemanticHasher   -- generic alias type annotations
  UnionTypeSemanticHasher      -- types.UnionType (Python 3.10+ X | Y syntax)
  ArrowTableSemanticHasher     -- pa.Table / pa.RecordBatch
  SchemaSemanticHasher         -- Schema objects

``register_builtin_python_type_semantic_hashers(registry)`` populates a registry
with all of the above.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any
from uuid import UUID

from upath import UPath

from orcapod.types import ContentHash, PathLike, Schema

if TYPE_CHECKING:
    from orcapod.hashing.semantic_hashing.type_handler_registry import (
        PythonTypeSemanticHasherRegistry,
    )
    from orcapod.hashing.semantic_hashing.semantic_hasher import SemanticAwarePythonHasher
    from orcapod.protocols.hashing_protocols import (
        ArrowHasherProtocol,
        FileContentHasherProtocol,
    )

logger = logging.getLogger(__name__)


class PathSemanticHasher:
    """Hasher for pathlib.Path objects — hashes file *content*.

    Args:
        file_hasher: Any object with a ``hash_file(path) -> ContentHash`` method.
    """

    def __init__(self, file_hasher: "FileContentHasherProtocol") -> None:
        self.file_hasher = file_hasher

    def handle(self, obj: PathLike, hasher: "SemanticAwarePythonHasher") -> ContentHash:
        path: Path = Path(obj)
        if not path.exists():
            raise FileNotFoundError(
                f"PathSemanticHasher: path does not exist: {path!r}. "
                "Paths must refer to existing files for content-based hashing."
            )
        if path.is_dir():
            raise IsADirectoryError(
                f"PathSemanticHasher: path is a directory: {path!r}. "
                "Only regular files are supported for content-based hashing."
            )
        logger.debug("PathSemanticHasher: hashing file content at %s", path)
        return self.file_hasher.hash_file(path)


class UPathSemanticHasher:
    """Hasher for universal_pathlib.UPath objects — hashes file content.

    Args:
        file_hasher: Any object with a ``hash_file(path) -> ContentHash`` method.
    """

    def __init__(self, file_hasher: "FileContentHasherProtocol") -> None:
        self.file_hasher = file_hasher

    def handle(self, obj: Any, hasher: "SemanticAwarePythonHasher") -> ContentHash:
        if not isinstance(obj, UPath):
            raise TypeError(
                f"UPathSemanticHasher: expected a UPath, got {type(obj)!r}."
            )
        if not obj.exists():
            raise FileNotFoundError(
                f"UPathSemanticHasher: path does not exist: {obj!r}."
            )
        if obj.is_dir():
            raise IsADirectoryError(
                f"UPathSemanticHasher: path is a directory: {obj!r}."
            )
        logger.debug("UPathSemanticHasher: hashing file content at %s", obj)
        return self.file_hasher.hash_file(obj)


class UUIDSemanticHasher:
    """Hasher for ``uuid.UUID`` objects — returns the raw 16-byte binary representation."""

    def handle(self, obj: Any, hasher: "SemanticAwarePythonHasher") -> Any:
        return obj.bytes


class BytesSemanticHasher:
    """Hasher for bytes and bytearray objects — returns the lowercase hex string."""

    def handle(self, obj: Any, hasher: "SemanticAwarePythonHasher") -> Any:
        if isinstance(obj, (bytes, bytearray)):
            return obj.hex()
        raise TypeError(
            f"BytesSemanticHasher: expected bytes or bytearray, got {type(obj)!r}"
        )


class FunctionSemanticHasher:
    """Hasher for Python functions/callables with a ``__code__`` attribute.

    Args:
        function_info_extractor: Any object with an
            ``extract_function_info(func) -> dict`` method.
    """

    def __init__(self, function_info_extractor: Any) -> None:
        self.function_info_extractor = function_info_extractor

    def handle(self, obj: Any, hasher: "SemanticAwarePythonHasher") -> Any:
        if not (callable(obj) and hasattr(obj, "__code__")):
            raise TypeError(
                f"FunctionSemanticHasher: expected a callable with __code__, got {type(obj)!r}"
            )
        func_name = getattr(obj, "__name__", repr(obj))
        logger.debug("FunctionSemanticHasher: extracting info for function %r", func_name)
        info: dict[str, Any] = self.function_info_extractor.extract_function_info(obj)
        return info


class TypeObjectSemanticHasher:
    """Hasher for type objects (classes passed as values).

    Returns a stable string of the form ``"type:<module>.<qualname>"``.
    """

    def handle(self, obj: Any, hasher: "SemanticAwarePythonHasher") -> Any:
        if not isinstance(obj, type):
            raise TypeError(
                f"TypeObjectSemanticHasher: expected a type/class, got {type(obj)!r}"
            )
        module: str = obj.__module__ or "<unknown>"
        qualname: str = obj.__qualname__
        return f"type:{module}.{qualname}"


class SpecialFormSemanticHasher:
    """Hasher for ``typing._SpecialForm`` objects such as ``typing.Union``."""

    def handle(self, obj: Any, hasher: "SemanticAwarePythonHasher") -> Any:
        name = getattr(obj, "_name", None) or repr(obj)
        return f"special_form:typing.{name}"


class GenericAliasSemanticHasher:
    """Hasher for generic alias type annotations (``dict[int, str]``, ``Optional[X]``, etc.)."""

    def handle(self, obj: Any, hasher: "SemanticAwarePythonHasher") -> Any:
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


class UnionTypeSemanticHasher:
    """Hasher for ``types.UnionType`` objects (Python 3.10+ ``X | Y`` syntax)."""

    def handle(self, obj: Any, hasher: "SemanticAwarePythonHasher") -> Any:
        args = getattr(obj, "__args__", None) or ()
        hashed_args = sorted(hasher.hash_object(arg).to_string() for arg in args)
        return {"__type__": "union", "args": hashed_args}


class ArrowTableSemanticHasher:
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

    def handle(self, obj: Any, hasher: "SemanticAwarePythonHasher") -> ContentHash:
        import pyarrow as _pa

        if isinstance(obj, _pa.RecordBatch):
            obj = _pa.Table.from_batches([obj])
        if not isinstance(obj, _pa.Table):
            raise TypeError(
                f"ArrowTableSemanticHasher: expected pa.Table or pa.RecordBatch, got {type(obj)!r}"
            )
        return self._get_arrow_hasher().hash_table(obj)


class SchemaSemanticHasher:
    """Hasher for ``Schema`` objects."""

    def handle(self, obj: Any, hasher: "SemanticAwarePythonHasher") -> Any:
        if not isinstance(obj, Schema):
            raise TypeError(
                f"SchemaSemanticHasher: expected a Schema, got {type(obj)!r}"
            )
        raise NotImplementedError("SchemaSemanticHasher is not yet implemented.")


def register_builtin_python_type_semantic_hashers(
    registry: "PythonTypeSemanticHasherRegistry",
    file_hasher: Any = None,
    function_info_extractor: Any = None,
    arrow_hasher: "ArrowHasherProtocol | None" = None,
) -> None:
    """Register all built-in semantic hashers into *registry*.

    ``pa.Table`` and ``pa.RecordBatch`` are always registered via
    ``ArrowTableSemanticHasher``. When ``arrow_hasher`` is provided it is
    passed through for immediate use; when ``None``, ``ArrowTableSemanticHasher``
    resolves the active arrow hasher lazily via ``get_default_context()`` at
    hash time, breaking the construction-time circular dependency.

    Args:
        registry: The ``PythonTypeSemanticHasherRegistry`` to populate.
        file_hasher: Optional ``FileContentHasherProtocol`` for path hashing.
            Defaults to ``BasicFileHasher(sha256)``.
        function_info_extractor: Optional ``FunctionInfoExtractorProtocol``.
            Defaults to ``FunctionSignatureExtractor``.
        arrow_hasher: Optional ``ArrowHasherProtocol`` for nested table hashing.
            When ``None``, lazy resolution via the default context is used.
    """
    if file_hasher is None:
        from orcapod.hashing.file_hashers import BasicFileHasher
        file_hasher = BasicFileHasher(algorithm="sha256")

    if function_info_extractor is None:
        from orcapod.hashing.semantic_hashing.function_info_extractors import (
            FunctionSignatureExtractor,
        )
        function_info_extractor = FunctionSignatureExtractor(
            include_module=True,
            include_defaults=True,
        )

    bytes_hasher = BytesSemanticHasher()
    registry.register(bytes, bytes_hasher)
    registry.register(bytearray, bytes_hasher)

    registry.register(Path, PathSemanticHasher(file_hasher))
    registry.register(UPath, UPathSemanticHasher(file_hasher))
    registry.register(UUID, UUIDSemanticHasher())

    import types as _types

    function_hasher = FunctionSemanticHasher(function_info_extractor)
    registry.register(_types.FunctionType, function_hasher)
    registry.register(_types.BuiltinFunctionType, function_hasher)
    registry.register(_types.MethodType, function_hasher)

    registry.register(type, TypeObjectSemanticHasher())
    registry.register(_types.UnionType, UnionTypeSemanticHasher())

    generic_alias_hasher = GenericAliasSemanticHasher()
    registry.register(_types.GenericAlias, generic_alias_hasher)
    try:
        import typing as _typing
        registry.register(_typing._GenericAlias, generic_alias_hasher)  # type: ignore[attr-defined]
        registry.register(_typing._SpecialForm, SpecialFormSemanticHasher())  # type: ignore[attr-defined]
    except AttributeError:
        pass

    registry.register(Schema, SchemaSemanticHasher())

    import pyarrow as _pa
    arrow_table_hasher = ArrowTableSemanticHasher(arrow_hasher)
    registry.register(_pa.Table, arrow_table_hasher)
    registry.register(_pa.RecordBatch, arrow_table_hasher)

    logger.debug(
        "register_builtin_python_type_semantic_hashers: registered %d hashers",
        len(registry),
    )
