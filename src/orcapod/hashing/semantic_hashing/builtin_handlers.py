"""
Built-in PythonTypeHandlerProtocol implementations.

  UUIDHandler           -- uuid.UUID: 16-byte binary representation
  BytesHandler          -- bytes/bytearray: hex string representation
  FunctionHandler       -- callable with __code__: via FunctionInfoExtractorProtocol
  TypeObjectHandler     -- type objects: logical type identifier, else "type:<module>.<qualname>"
  SpecialFormHandler    -- typing._SpecialForm
  GenericAliasHandler   -- generic alias type annotations
  UnionTypeHandler      -- types.UnionType (Python 3.10+ X | Y syntax)
  ArrowTableHandler     -- pa.Table / pa.RecordBatch
  SchemaHandler         -- Schema objects
  FileHandler           -- orcapod.File: file content hash
  DirectoryHandler      -- orcapod.Directory: recursive Merkle tree hash
  NumpyArrayHandler     -- numpy.ndarray: ndarray_v1 value digest (array_digest_v1)
  PandasDataFrameHandler -- pd.DataFrame: pandas_dataframe_v1 (descriptor + ordered Arrow digest)
  PandasSeriesHandler   -- pd.Series: pandas_series_v1 (descriptor + ordered Arrow digest)
  PolarsExprHandler     -- pl.Expr: polars_expr_v1 canonical expression tree (ITL-828)

``register_builtin_python_type_handlers(registry)`` populates a registry
with all of the above handlers.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any
from uuid import UUID

from typing_extensions import override

from orcapod.hashing.pandas_value_hash import (
    PANDAS_DATAFRAME_HASH_LABEL,
    PANDAS_DATAFRAME_OPTIONS,
    PANDAS_SERIES_HASH_LABEL,
    PANDAS_SERIES_OPTIONS,
)
from orcapod.hashing.semantic_hashing.encoded import Encoded
from orcapod.hashing.semantic_hashing.function_info_extractors import ParameterDefault
from orcapod.logical_types.array_format import NDARRAY_EXTENSION_NAME, NDARRAY_OPTIONS
from orcapod.logical_types.identifier import logical_type_identifier
from orcapod.logical_types.ignore_list import IgnoreListHandler
from orcapod.logical_types.pandas_type import (
    LogicalPandasDataFrame,
    LogicalPandasSeries,
)
from orcapod.logical_types.polars_type import (
    POLARS_DATAFRAME_EXTENSION_NAME,
    POLARS_SERIES_EXTENSION_NAME,
)
from orcapod.types import ContentHash, Schema
from orcapod.utils.canonical_type_names import canonical_type_name

if TYPE_CHECKING:
    from orcapod.protocols.hashing_protocols import (
        ArrowHasherProtocol,
        DirectoryHasherProtocol,
        FileContentHasherProtocol,
        HandlerRegistryProtocol,
        SemanticHasherProtocol,
    )
    from orcapod.protocols.semantic_types_protocols import TypeConverterProtocol

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
        raise TypeError(f"BytesHandler: expected bytes or bytearray, got {type(obj)!r}")


def hash_default_value(value: Any, hasher: "SemanticHasherProtocol") -> ContentHash:
    """Hash one parameter default.

    The single place that decides how a default value is hashed: like every other
    hashed value, by the semantic hasher, never by its ``repr()``.

    Args:
        value: The default value.
        hasher: The semantic hasher hashing the function.

    Returns:
        The hash of the value.

    Raises:
        TypeError: If the hasher has no way to hash the value.
    """
    return hasher.hash_object(value)


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
        parameters = info.get("parameters")
        if not parameters:
            return info
        return {
            **info,
            "parameters": [
                self._with_hashed_default(entry, obj, hasher) for entry in parameters
            ],
        }

    @staticmethod
    def _with_hashed_default(
        entry: dict[str, Any], func: Any, hasher: "SemanticHasherProtocol"
    ) -> dict[str, Any]:
        """Return a parameter entry with its default replaced by the default's hash.

        Args:
            entry: One parameter entry of the extracted function description.
            func: The function the parameter belongs to.
            hasher: The semantic hasher hashing the function.

        Returns:
            ``entry`` unchanged when it has no ``ParameterDefault``; otherwise a
            copy whose ``"default"`` is the hash of the default value.

        Raises:
            TypeError: If the default value cannot be hashed. The message names the
                parameter and the function; the hasher's error is its cause.
        """
        default = entry.get("default")
        if not isinstance(default, ParameterDefault):
            return entry
        try:
            default_hash = hash_default_value(default.value, hasher)
        except TypeError as exc:
            module = getattr(func, "__module__", None)
            qualname = getattr(func, "__qualname__", None) or repr(func)
            function_name = f"{module}.{qualname}" if module else qualname
            raise TypeError(
                f"Cannot hash the default value of parameter {entry['name']!r} of "
                f"function {function_name}: {exc} A default value is part of the "
                "function's hash: use a default the hasher can hash, or remove the "
                "default and supply the value as an input."
            ) from exc
        return {**entry, "default": default_hash}


class TypeObjectHandler:
    """Hasher for type objects (classes passed as values).

    Resolves a type to its logical type in the ``LogicalTypeRegistry`` exposed by
    *type_converter* and hashes its logical type identifier, the Arrow extension
    name and extension metadata, written as ``LogicalTypeIdentifier.text``
    (``"type:orcapod.file;7b2276657273696f6e223a317d"`` for ``op.File``:
    the name, ``;``, then the metadata ``{"version":1}`` in hex). A change of
    either part of the identifier therefore changes every type, schema,
    function and pipeline hash the type takes part in.

    A class not registered yet that a registered factory covers (a dataclass,
    an ``orcapod.dataclass``, a pydantic model) is registered first, through
    ``type_converter.get_logical_type_for_python_type``. So a type hashes the
    same before and after anything else registers it (ITL-1137): hashing a
    schema before the source or function pod that registers its class gives
    the registered value, the one every stored path and golden value holds.

    Falls back to ``"type:<module>.<qualname>"`` for a type with no logical
    type (``int``, a class no factory covers) or when no ``type_converter`` is
    provided. The module is the canonical one from ``canonical_type_name``, so
    standard-library types that moved into a private submodule
    (``pathlib._local`` on Python 3.13) hash identically on every Python
    version.

    Args:
        type_converter: Optional ``TypeConverterProtocol``.  When provided,
            ``type_converter.get_logical_type_for_python_type(obj)`` resolves the
            type to its logical type, registering it when a factory covers it.
            When ``None`` (the default), the fallback
            ``"type:<module>.<qualname>"`` serialisation is always used.
    """

    def __init__(self, type_converter: "TypeConverterProtocol | None" = None) -> None:
        self._type_converter = type_converter

    def handle(self, obj: Any, hasher: "SemanticHasherProtocol") -> Any:
        if not isinstance(obj, type):
            raise TypeError(
                f"TypeObjectHandler: expected a type/class, got {type(obj)!r}"
            )
        if self._type_converter is not None:
            lt = self._type_converter.get_logical_type_for_python_type(obj)
            if lt is not None:
                return f"type:{logical_type_identifier(lt).text}"
        return f"type:{canonical_type_name(obj)}"


class SpecialFormHandler:
    """Hasher for ``typing._SpecialForm`` objects such as ``typing.Union``."""

    def handle(self, obj: Any, hasher: "SemanticHasherProtocol") -> Any:
        name = getattr(obj, "_name", None) or repr(obj)
        return f"special_form:typing.{name}"


def _sorted_hashes(
    hasher: "SemanticHasherProtocol", args: tuple[Any, ...]
) -> list[ContentHash]:
    """Hash each of ``args`` and return the hashes ordered by digest.

    The hashes are returned as ``ContentHash`` objects: the structure hasher feeds only
    their digests into the enclosing hash, never their labels.

    Args:
        hasher: The structure hasher in use.
        args: The members of a union.

    Returns:
        One hash per member, sorted by hex digest so that member order does not matter.
    """
    return sorted((hasher.hash_object(arg) for arg in args), key=ContentHash.to_hex)


class GenericAliasHandler:
    """Hasher for generic alias type annotations (``dict[int, str]``, ``Optional[X]``, etc.)."""

    def handle(self, obj: Any, hasher: "SemanticHasherProtocol") -> Any:
        import typing

        origin = getattr(obj, "__origin__", None)
        args = getattr(obj, "__args__", None) or ()
        if origin is None:
            return f"generic_alias:{obj!r}"
        if origin is typing.Union:
            return Encoded("union", {"args": _sorted_hashes(hasher, args)})
        return Encoded(
            "generic_alias",
            {
                "origin": hasher.hash_object(origin),
                "args": [hasher.hash_object(arg) for arg in args],
            },
        )


class UnionTypeHandler:
    """Hasher for ``types.UnionType`` objects (Python 3.10+ ``X | Y`` syntax)."""

    def handle(self, obj: Any, hasher: "SemanticHasherProtocol") -> Any:
        args = getattr(obj, "__args__", None) or ()
        return Encoded("union", {"args": _sorted_hashes(hasher, args)})


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
    """Hasher for ``Schema`` objects.

    Canonical, explicit path for schema hashing. For each field, hashes the Python
    type via ``hasher.hash_object`` (which dispatches to ``TypeObjectHandler``, using
    the logical type identifier, Arrow extension name and metadata, for registered types
    and the stable ``builtins.*`` / stdlib path for native types). Field names are sorted so the
    hash is deterministic regardless of insertion order.

    This produces the same hash as the previous accidental path through ``_expand_mapping``
    (Schema is a Mapping), preserving all existing hash values.

    ``Schema.optional_fields`` is intentionally excluded from the hash. Two schemas with
    the same field names and types but different optionality are hash-equivalent.
    Optionality is a Python-level execution contract (which parameters have defaults),
    not part of the structural identity used for caching or pipeline routing.

    When ``type_converter`` is provided, every field type is verified to be
    Arrow-translatable. A type that cannot be converted raises ``TypeError`` with a
    diagnostic message, catching unsupported types at hash time rather than silently
    falling back to a Python module path. The check runs after the field type is
    hashed, because hashing a type registers a class a factory covers
    (``TypeObjectHandler``), and the check reads only registered types.

    Args:
        type_converter: Optional ``TypeConverterProtocol``. When provided, validates
            Arrow-translatability per field. When ``None`` (e.g. in tests without a
            full ``DataContext``), validation is skipped.
    """

    def __init__(self, type_converter: "TypeConverterProtocol | None" = None) -> None:
        self._type_converter = type_converter

    def handle(self, obj: Any, hasher: "SemanticHasherProtocol") -> Any:
        """Hash ``obj`` by hashing each field type individually and sorting by name.

        Args:
            obj: A ``Schema`` instance.
            hasher: The calling ``SemanticHasherProtocol`` — used to hash each field's
                Python type.

        Returns:
            A ``dict[str, ContentHash]`` mapping field name to the hash of the field's
            type, sorted by field name for determinism. The structure hasher feeds
            only the digests into the schema hash.

        Raises:
            TypeError: If ``obj`` is not a ``Schema``, or if ``type_converter`` is
                provided and a field type is not Arrow-translatable.
        """
        if not isinstance(obj, Schema):
            raise TypeError(f"SchemaHandler: expected a Schema, got {type(obj)!r}")
        result: dict[str, ContentHash] = {}
        for field_name, python_type in obj.items():
            # Hashing the type registers a class a factory covers, so it comes first.
            type_hash = hasher.hash_object(python_type)
            if self._type_converter is not None:
                try:
                    self._type_converter.python_type_to_arrow_type(python_type)
                except (TypeError, ValueError) as exc:
                    raise TypeError(
                        f"SchemaHandler: field {field_name!r} has type "
                        f"{python_type!r} that is not Arrow-translatable. "
                        f"Every type in a schema must be Arrow-convertible — "
                        f"register it as an orcapod logical type or use a "
                        f"supported native type (int, str, float, bool, bytes, "
                        f"datetime, date)."
                    ) from exc
            result[field_name] = type_hash
        # Sort by field name for determinism — matches _expand_mapping's sort_keys.
        return dict(sorted(result.items()))


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
        from orcapod.logical_types.file_type import File

        if not isinstance(obj, File):
            raise TypeError(f"FileHandler: expected an orcapod.File, got {type(obj)!r}")
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
        from orcapod.logical_types.directory_type import Directory

        if not isinstance(obj, Directory):
            raise TypeError(
                f"DirectoryHandler: expected an orcapod.Directory, got {type(obj)!r}"
            )
        wrapped = getattr(obj, "__wrapped__")
        ignore = getattr(obj, "_ignore", None)
        logger.debug("DirectoryHandler: hashing directory content at %s", wrapped)
        return self.directory_hasher.hash_directory(wrapped, ignore=ignore)


class NumpyArrayHandler(IgnoreListHandler):
    """Hasher for ``numpy.ndarray``: the strict value digest under ``ndarray_v1``.

    The digest is ``numpy_type.array_value_digest``: the element digest of the
    array (dtype, shape and element bits) and, on top of it, memory order, byte
    order and structured layout, each of which the configured ignore list can
    take out. What storage does not keep (strides beyond C or Fortran
    contiguity, subclass, flags) never changes the hash.

    The label ``ndarray_v1`` names this algorithm; the ignore list is part of
    the logical type identifier, not of the label or the preimage.

    Args:
        ignore: Names from ``array_format.NDARRAY_OPTIONS`` ignored when a bare
            array is hashed (the data context's list for the type).
    """

    EXTENSION_NAME = NDARRAY_EXTENSION_NAME
    OPTIONS = NDARRAY_OPTIONS
    HASH_LABEL = "ndarray_v1"
    """``ContentHash.method`` of an ndarray hash."""

    def digest(self, obj: Any, ignore: frozenset[str]) -> bytes:
        """Return the value digest of ``obj``, ignoring the named properties.

        Args:
            obj: A ``numpy.ndarray`` instance (subclasses are hashed as plain arrays).
            ignore: Names from ``OPTIONS`` to ignore.

        Returns:
            ``numpy_type.array_value_digest(obj, ignore)``.

        Raises:
            TypeError: If ``obj`` is not a ``numpy.ndarray``.
            ValueError: If the array cannot be stored (masked, object,
                ``StringDType``, ``longdouble``, ...).
        """
        from orcapod.logical_types.numpy_type import array_value_digest

        return array_value_digest(obj, ignore)


class PandasDataFrameHandler(IgnoreListHandler):
    """Hasher for ``pd.DataFrame``: Orcapod's pandas descriptor plus an ordered Arrow digest.

    Delegates to ``pandas_frame_content_hash`` in ``orcapod.hashing.pandas_value_hash``, whose
    module docstring defines the ``pandas_dataframe_v1`` scheme. The hash is strict: column
    order, index levels and their order, label types, ``columns.names``, pandas dtypes,
    categorical categories, ``attrs`` and the values are all part of it. Only what storage
    does not restore (``flags``, for example) is left out. A data context may configure
    the hash to ignore the properties named in ``OPTIONS``.

    Args:
        ignore: The property names ignored when a bare frame is hashed.
        arrow_hasher: An object satisfying ``ArrowHasherProtocol``; its ``hash_table_ordered``
            digests the values. When ``None``, the default data context's ``arrow_hasher`` is
            resolved lazily at call time (breaking the circular dependency that would arise if
            the registry were constructed before the arrow hasher).
    """

    EXTENSION_NAME = LogicalPandasDataFrame.EXTENSION_NAME
    OPTIONS = PANDAS_DATAFRAME_OPTIONS
    HASH_LABEL = PANDAS_DATAFRAME_HASH_LABEL

    def __init__(
        self,
        ignore: Iterable[str] = (),
        arrow_hasher: "ArrowHasherProtocol | None" = None,
    ) -> None:
        """Create the handler.

        Args:
            ignore: The property names the hash ignores; must be options of the type.
            arrow_hasher: The Arrow hasher that digests the values table; the default
                data context's when ``None``.
        """
        super().__init__(ignore)
        self._arrow_hasher = arrow_hasher

    def _get_arrow_hasher(self) -> "ArrowHasherProtocol":
        """Return the Arrow hasher given at construction, or the default data context's."""
        if self._arrow_hasher is not None:
            return self._arrow_hasher
        from orcapod.contexts import get_default_context

        return get_default_context().arrow_hasher  # type: ignore[return-value]

    @override
    def digest(self, obj: Any, ignore: frozenset[str]) -> bytes:
        """Return the ``pandas_dataframe_v1`` digest of ``obj``.

        Args:
            obj: A ``pd.DataFrame`` instance.
            ignore: The property names to ignore; a subset of ``OPTIONS``.

        Returns:
            The 32-byte digest.

        Raises:
            TypeError: If ``obj`` is not a ``pd.DataFrame``.
            ValueError: If a label or dtype is outside the descriptor grammar, or a column
                cannot be converted to Arrow.
        """
        import pandas as _pd

        from orcapod.hashing.pandas_value_hash import pandas_frame_content_hash

        if not isinstance(obj, _pd.DataFrame):
            raise TypeError(
                f"PandasDataFrameHandler: expected pd.DataFrame, got {type(obj)!r}"
            )
        return pandas_frame_content_hash(obj, self._get_arrow_hasher(), ignore).digest


class PandasSeriesHandler(IgnoreListHandler):
    """Hasher for ``pd.Series``: Orcapod's pandas descriptor plus an ordered Arrow digest.

    Delegates to ``pandas_series_content_hash`` in ``orcapod.hashing.pandas_value_hash``
    (``pandas_series_v1``). The name is part of the hash, typed (``0`` and ``"0"`` differ).
    A data context may configure the hash to ignore the properties named in ``OPTIONS``.

    Args:
        ignore: The property names ignored when a bare Series is hashed.
        arrow_hasher: An object satisfying ``ArrowHasherProtocol``; its ``hash_table_ordered``
            digests the values. When ``None``, the default data context's ``arrow_hasher`` is
            resolved lazily at call time (breaking the circular dependency that would arise if
            the registry were constructed before the arrow hasher).
    """

    EXTENSION_NAME = LogicalPandasSeries.EXTENSION_NAME
    OPTIONS = PANDAS_SERIES_OPTIONS
    HASH_LABEL = PANDAS_SERIES_HASH_LABEL

    def __init__(
        self,
        ignore: Iterable[str] = (),
        arrow_hasher: "ArrowHasherProtocol | None" = None,
    ) -> None:
        """Create the handler.

        Args:
            ignore: The property names the hash ignores; must be options of the type.
            arrow_hasher: The Arrow hasher that digests the values table; the default
                data context's when ``None``.
        """
        super().__init__(ignore)
        self._arrow_hasher = arrow_hasher

    def _get_arrow_hasher(self) -> "ArrowHasherProtocol":
        """Return the Arrow hasher given at construction, or the default data context's."""
        if self._arrow_hasher is not None:
            return self._arrow_hasher
        from orcapod.contexts import get_default_context

        return get_default_context().arrow_hasher  # type: ignore[return-value]

    @override
    def digest(self, obj: Any, ignore: frozenset[str]) -> bytes:
        """Return the ``pandas_series_v1`` digest of ``obj``.

        Args:
            obj: A ``pd.Series`` instance.
            ignore: The property names to ignore; a subset of ``OPTIONS``.

        Returns:
            The 32-byte digest.

        Raises:
            TypeError: If ``obj`` is not a ``pd.Series``.
            ValueError: If a label or the dtype is outside the descriptor grammar, or the values
                cannot be converted to Arrow.
        """
        import pandas as _pd

        from orcapod.hashing.pandas_value_hash import pandas_series_content_hash

        if not isinstance(obj, _pd.Series):
            raise TypeError(
                f"PandasSeriesHandler: expected pd.Series, got {type(obj)!r}"
            )
        return pandas_series_content_hash(obj, self._get_arrow_hasher(), ignore).digest


class PolarsExprHandler:
    """Hasher for ``pl.Expr``: the ``polars_expr_v1`` canonical expression tree.

    Delegates to ``polars_expr_content_hash`` in
    ``orcapod.hashing.semantic_hashing.polars_expr``, whose module docstring defines the
    grammar. Expressions outside the grammar raise ``ValueError`` naming the expression.
    """

    def handle(self, obj: Any, hasher: "SemanticHasherProtocol") -> ContentHash:
        """Return the content hash of ``obj``.

        Args:
            obj: A ``pl.Expr`` instance.
            hasher: Ignored. Present for protocol conformance.

        Returns:
            A ``ContentHash`` with method ``"polars_expr_v1"``.

        Raises:
            TypeError: If ``obj`` is not a ``pl.Expr``.
            ValueError: If the expression is outside the ``polars_expr_v1`` grammar.
        """
        import polars as _pl

        from orcapod.hashing.semantic_hashing.polars_expr import (
            polars_expr_content_hash,
        )

        if not isinstance(obj, _pl.Expr):
            raise TypeError(f"PolarsExprHandler: expected pl.Expr, got {type(obj)!r}")
        return polars_expr_content_hash(obj)


class PolarsDataFrameHandler(IgnoreListHandler):
    """Hasher for ``pl.DataFrame``: the ``polars_dataframe_v1`` value digest.

    The algorithm is ``polars_frame_digest`` in ``orcapod.logical_types.polars_type``,
    whose module docstring defines it. By default the hash is strict: it ignores chunking,
    slice offsets and the bytes under null slots, which are not properties of the value,
    and distinguishes column order, struct field order, a null struct from a struct of
    nulls, and every dtype parameter. The ignore list can leave out ``column_order`` and
    ``struct_field_order``.
    """

    EXTENSION_NAME = POLARS_DATAFRAME_EXTENSION_NAME
    OPTIONS = frozenset({"column_order", "struct_field_order"})
    HASH_LABEL = "polars_dataframe_v1"
    """``ContentHash.method`` of a ``pl.DataFrame`` hash."""

    def digest(self, obj: Any, ignore: frozenset[str]) -> bytes:
        """Return the value digest of ``obj``, ignoring the named properties.

        Args:
            obj: A ``pl.DataFrame`` instance.
            ignore: A subset of ``OPTIONS``.

        Returns:
            The 32 digest bytes.

        Raises:
            TypeError: If ``obj`` is not a ``pl.DataFrame``.
            ValueError: If a column's dtype is outside the descriptor grammar.
        """
        import polars as _pl

        from orcapod.logical_types.polars_type import polars_frame_digest

        if not isinstance(obj, _pl.DataFrame):
            raise TypeError(
                f"PolarsDataFrameHandler: expected pl.DataFrame, got {type(obj)!r}"
            )
        return polars_frame_digest(
            obj,
            ignore_column_order="column_order" in ignore,
            ignore_struct_field_order="struct_field_order" in ignore,
        )


class PolarsSeriesHandler(IgnoreListHandler):
    """Hasher for ``pl.Series``: the ``polars_series_v1`` value digest.

    The algorithm is ``polars_series_digest`` in ``orcapod.logical_types.polars_type``:
    the Series' name, dtype and values. The ignore list can leave out ``series_name`` and
    ``struct_field_order``.
    """

    EXTENSION_NAME = POLARS_SERIES_EXTENSION_NAME
    OPTIONS = frozenset({"series_name", "struct_field_order"})
    HASH_LABEL = "polars_series_v1"
    """``ContentHash.method`` of a ``pl.Series`` hash."""

    def digest(self, obj: Any, ignore: frozenset[str]) -> bytes:
        """Return the value digest of ``obj``, ignoring the named properties.

        Args:
            obj: A ``pl.Series`` instance.
            ignore: A subset of ``OPTIONS``.

        Returns:
            The 32 digest bytes.

        Raises:
            TypeError: If ``obj`` is not a ``pl.Series``.
            ValueError: If the dtype is outside the descriptor grammar.
        """
        import polars as _pl

        from orcapod.logical_types.polars_type import polars_series_digest

        if not isinstance(obj, _pl.Series):
            raise TypeError(
                f"PolarsSeriesHandler: expected pl.Series, got {type(obj)!r}"
            )
        return polars_series_digest(
            obj,
            ignore_name="series_name" in ignore,
            ignore_struct_field_order="struct_field_order" in ignore,
        )


def register_builtin_python_type_handlers(
    registry: "HandlerRegistryProtocol",
    file_hasher: Any = None,
    function_info_extractor: Any = None,
    arrow_hasher: "ArrowHasherProtocol | None" = None,
    directory_hasher: Any = None,
    type_converter: "TypeConverterProtocol | None" = None,
) -> None:
    """Register all built-in semantic hashers into *registry*.

    ``pa.Table`` and ``pa.RecordBatch`` are registered via ``ArrowTableHandler``.
    ``pd.DataFrame`` and ``pd.Series`` are registered via ``PandasDataFrameHandler``
    and ``PandasSeriesHandler`` (``pandas_dataframe_v1`` / ``pandas_series_v1``, values
    digested by the Arrow hasher's ``hash_table_ordered``). When ``arrow_hasher`` is
    provided it is passed through for immediate use; when ``None``, these handlers
    resolve the active arrow hasher lazily via ``get_default_context()`` at hash time,
    breaking the construction-time circular dependency.

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
            Defaults to ``FileHasher(sha256)``.
        function_info_extractor: Optional ``FunctionInfoExtractorProtocol``.
            Defaults to ``FunctionSignatureExtractor``.
        arrow_hasher: Optional ``ArrowHasherProtocol`` for nested table hashing.
            When ``None``, lazy resolution via the default context is used.
        directory_hasher: Optional ``DirectoryHasherProtocol`` for directory tree hashing.
            Defaults to ``BasicDirectoryHasher(sha256)``.
        type_converter: Optional ``TypeConverterProtocol`` forwarded to
            ``TypeObjectHandler``, ``FunctionSignatureExtractor``, and
            ``SchemaHandler`` for stable canonical type-name resolution via
            ``get_logical_type_for_python_type()`` and Arrow-translatability
            validation.
            When ``None`` (the default), all three handlers fall back to the raw
            ``"type:<module>.<qualname>"`` serialisation and skip Arrow validation.
    """
    if file_hasher is None:
        from orcapod.hashing.file_hashers import FileHasher

        file_hasher = FileHasher(algorithm="sha256")

    if directory_hasher is None:
        from orcapod.hashing.directory_hashers import BasicDirectoryHasher

        directory_hasher = BasicDirectoryHasher(
            file_hasher=file_hasher,
            algorithm="sha256",
        )

    if function_info_extractor is None:
        from orcapod.hashing.semantic_hashing.function_info_extractors import (
            FunctionSignatureExtractor,
        )

        function_info_extractor = FunctionSignatureExtractor(
            include_module=True,
            include_defaults=True,
            type_converter=type_converter,
        )

    bytes_hasher = BytesHandler()
    registry.register(bytes, bytes_hasher)
    registry.register(bytearray, bytes_hasher)

    registry.register(UUID, UUIDHandler())

    from orcapod.logical_types.file_type import File

    registry.register(File, FileHandler(file_hasher))

    from orcapod.logical_types.directory_type import Directory

    registry.register(Directory, DirectoryHandler(directory_hasher))

    import types as _types

    function_hasher = FunctionHandler(function_info_extractor)
    registry.register(_types.FunctionType, function_hasher)
    registry.register(_types.BuiltinFunctionType, function_hasher)
    registry.register(_types.MethodType, function_hasher)

    registry.register(type, TypeObjectHandler(type_converter=type_converter))
    registry.register(_types.UnionType, UnionTypeHandler())

    generic_alias_hasher = GenericAliasHandler()
    registry.register(_types.GenericAlias, generic_alias_hasher)
    try:
        import typing as _typing

        registry.register(_typing._GenericAlias, generic_alias_hasher)  # type: ignore[attr-defined]
        registry.register(_typing._SpecialForm, SpecialFormHandler())  # type: ignore[attr-defined]
    except AttributeError:
        pass

    registry.register(Schema, SchemaHandler(type_converter=type_converter))

    import pyarrow as _pa

    arrow_table_hasher = ArrowTableHandler(arrow_hasher)
    registry.register(_pa.Table, arrow_table_hasher)
    registry.register(_pa.RecordBatch, arrow_table_hasher)

    import numpy as _np

    registry.register(_np.ndarray, NumpyArrayHandler())

    import pandas as _pd

    registry.register(_pd.DataFrame, PandasDataFrameHandler(arrow_hasher=arrow_hasher))
    registry.register(_pd.Series, PandasSeriesHandler(arrow_hasher=arrow_hasher))

    import polars as _pl

    registry.register(_pl.Expr, PolarsExprHandler())
    registry.register(_pl.DataFrame, PolarsDataFrameHandler())
    registry.register(_pl.Series, PolarsSeriesHandler())

    logger.debug(
        "register_builtin_python_type_handlers: registered %d hashers",
        len(registry),
    )
