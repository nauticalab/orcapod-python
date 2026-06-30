"""orcapod.Directory — content-identified, existence-validated directory path.

``Directory`` wraps a ``upath.UPath`` and validates that the path points to a readable,
traversable directory at construction time. Use ``pathlib.Path`` / ``upath.UPath`` for
paths that may not yet exist.

``LogicalDirectory`` is the Arrow extension type that serialises ``Directory`` instances as
``large_string`` columns tagged with the ``"orcapod.directory"`` extension name. The stored
value is always a JSON object containing the path and, if set, the ignore parameter.
"""

from __future__ import annotations

import importlib
import json
import warnings
from collections.abc import Callable, Iterable
from typing import TYPE_CHECKING, Any, Self

import polars as pl
import pyarrow as pa
from upath import UPath
from upath.extensions import ProxyUPath

from orcapod.extension_types.base_logical_type import BaseLogicalType
from orcapod.extension_types.registry import make_arrow_extension_type, make_polars_extension_type

if TYPE_CHECKING:
    from orcapod.extension_types.protocols import TypeConverterProtocol


class Directory(ProxyUPath):
    """A content-identified, existence-validated directory path.

    Wraps a ``UPath`` and validates that the path refers to a readable, traversable
    directory at construction time. Works across local, S3, GCS, and any other
    fsspec-backed backend supported by ``upath``.

    Note:
        ``isinstance(directory_instance, UPath)`` returns ``False`` because
        ``ProxyUPath`` does not inherit from ``UPath``. Use
        ``isinstance(x, Directory)`` to type-check.

    Args:
        *args: Positional path arguments forwarded to ``UPath``.
        ignore: Optional filter for excluding entries from the content hash.
            Accepts an iterable of glob patterns matched against entry names (via
            ``fnmatch``), or a callable ``(UPath) -> bool`` returning ``True`` to
            exclude an entry. Applied at every level of recursion during hashing.
            Defaults to ``None`` (all entries included).
        **kwargs: Keyword arguments forwarded to ``UPath``.

    Raises:
        FileNotFoundError: If the path does not exist.
        NotADirectoryError: If the path is not a directory.
        PermissionError: If the directory cannot be traversed.

    Example:
        >>> d = Directory("/tmp/mydata")
        >>> str(d)
        '/tmp/mydata'
        >>> Directory("/tmp/nonexistent")
        FileNotFoundError: ...
    """

    def __init__(
        self,
        *args: Any,
        ignore: Callable[[UPath], bool] | Iterable[str] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        if not self.__wrapped__.exists():
            raise FileNotFoundError(
                f"Directory: path does not exist: {self.__wrapped__!r}"
            )
        if not self.__wrapped__.is_dir():
            raise NotADirectoryError(
                f"Directory: path is not a directory: {self.__wrapped__!r}"
            )
        try:
            next(iter(self.__wrapped__.iterdir()), None)
        except PermissionError as exc:
            raise PermissionError(
                f"Directory: path is not traversable: {self.__wrapped__!r}"
            ) from exc
        self._ignore = ignore

    @classmethod
    def _from_upath(cls, upath: UPath, /) -> Self:
        """Create a ``Directory`` from an existing ``UPath`` without validation.

        Used internally by ``ProxyUPath`` for derived paths (e.g. ``.parent``,
        ``/`` operator). Validation is intentionally skipped — derived paths from
        navigation may not exist yet. ``ignore`` defaults to ``None`` on all derived
        instances.
        """
        obj = object.__new__(cls)
        object.__setattr__(obj, "__wrapped__", upath)
        object.__setattr__(obj, "_ignore", None)
        return obj


def _try_import_callable(full_name: str) -> Callable[..., Any] | None:
    """Attempt to import a callable by its ``"module:qualname"`` serialised form.

    Args:
        full_name: A string of the form ``"module.path:QualifiedName"`` produced by
            ``LogicalDirectory.python_to_storage`` for named callables.

    Returns:
        The recovered callable, or ``None`` with a ``UserWarning`` if recovery fails.
    """
    if ":" not in full_name:
        warnings.warn(
            f"Directory: cannot recover ignore callable from {full_name!r} "
            "(expected 'module:qualname' format). Falling back to ignore=None.",
            UserWarning,
            stacklevel=2,
        )
        return None
    module_path, qualname = full_name.split(":", 1)
    try:
        mod = importlib.import_module(module_path)
        obj: Any = mod
        for attr in qualname.split("."):
            obj = getattr(obj, attr)
    except (ImportError, AttributeError) as exc:
        warnings.warn(
            f"Directory: cannot recover ignore callable {full_name!r}: {exc}. "
            "Falling back to ignore=None.",
            UserWarning,
            stacklevel=2,
        )
        return None
    if not callable(obj):
        warnings.warn(
            f"Directory: recovered attribute {full_name!r} is not callable "
            f"(got {type(obj)!r}). Falling back to ignore=None.",
            UserWarning,
            stacklevel=2,
        )
        return None
    return obj  # type: ignore[return-value]


class LogicalDirectory(BaseLogicalType):
    """Logical type for ``orcapod.Directory``.

    Stores ``Directory`` instances as Arrow large strings using the custom extension
    type ``"orcapod.directory"``. The stored value is always a JSON object containing
    the ``"path"`` key, and optionally ``"ignore"`` (glob pattern list) or
    ``"ignore_callable"`` (``"module:qualname"`` string for named callables).

    On read (``storage_to_python``), the path is used to reconstruct a ``Directory``
    instance, which re-validates existence. Reading an Arrow table with
    ``"orcapod.directory"`` columns will raise ``FileNotFoundError`` if the directory
    has been moved or deleted.

    Example:
        >>> import tempfile
        >>> lt = LogicalDirectory()
        >>> with tempfile.TemporaryDirectory() as tmp:
        ...     d = Directory(tmp)
        ...     lt.storage_to_python(lt.python_to_storage(d)) == d
        True
    """

    _arrow_ext_class = make_arrow_extension_type("orcapod.directory", pa.large_string())
    _arrow_ext: pa.ExtensionType | None = None
    _polars_ext_class = make_polars_extension_type("orcapod.directory", pa.large_string())
    _polars_ext: pl.BaseExtension | None = None

    logical_type_name: str = "orcapod.directory"
    python_type: type = Directory

    def get_arrow_extension_type(self) -> pa.ExtensionType:
        """Return the Arrow extension type for ``Directory``.

        Returns:
            A cached ``pa.ExtensionType`` with extension name ``"orcapod.directory"``
            and storage type ``pa.large_string()``.
        """
        if LogicalDirectory._arrow_ext is None:
            LogicalDirectory._arrow_ext = LogicalDirectory._arrow_ext_class()
        return LogicalDirectory._arrow_ext

    def get_polars_extension_type(self) -> pl.BaseExtension:
        """Return the Polars extension type for ``Directory``.

        Returns:
            A cached ``pl.BaseExtension`` registered under ``"orcapod.directory"``.
        """
        if LogicalDirectory._polars_ext is None:
            LogicalDirectory._polars_ext = LogicalDirectory._polars_ext_class()
        return LogicalDirectory._polars_ext

    def python_to_storage(
        self, value: Any, converter: TypeConverterProtocol | None = None
    ) -> str:
        """Convert a ``Directory`` to its JSON storage representation.

        The ``ignore`` parameter is serialised as follows:

        * ``None`` → ``{"path": "..."}``
        * Any non-callable iterable (``list``, ``tuple``, etc.) → ``{"path": "...", "ignore": [...]}`` (patterns sorted)
        * Named callable → ``{"path": "...", "ignore_callable": "module:qualname"}``
        * Lambda / closure / built-in → ``{"path": "..."}`` + ``UserWarning``

        Args:
            value: A ``Directory`` instance.
            converter: Ignored. Present for protocol conformance.

        Returns:
            A JSON string.
        """
        path_str = str(value)
        ignore = getattr(value, "_ignore", None)

        if ignore is None:
            return json.dumps({"path": path_str})

        if not callable(ignore):
            # Any non-callable iterable (list, tuple, set, …) — pattern spec.
            # Sort by base name (strip leading glob chars) for deterministic storage order;
            # secondary sort by the full pattern ensures canonical output when base names collide.
            return json.dumps({"path": path_str, "ignore": sorted(ignore, key=lambda x: (x.lstrip("*."), x))})

        # Callable — attempt best-effort serialisation via module:qualname.
        qualname = getattr(ignore, "__qualname__", "")
        module = getattr(ignore, "__module__", "")
        if module and qualname and "<" not in qualname:
            full_name = f"{module}:{qualname}"
            return json.dumps({"path": path_str, "ignore_callable": full_name})

        warnings.warn(
            f"Directory.ignore is a callable ({ignore!r}) that cannot be serialised "
            "(lambda, closure, or built-in). The ignore filter will be lost on "
            "roundtrip. Use a list of glob patterns for a lossless roundtrip.",
            UserWarning,
            stacklevel=2,
        )
        return json.dumps({"path": path_str})

    def storage_to_python(
        self, storage_value: Any, converter: TypeConverterProtocol | None = None
    ) -> Directory:
        """Reconstruct a ``Directory`` from its stored JSON string.

        Re-validates existence on read — raises ``FileNotFoundError`` if the directory
        no longer exists at the stored path.

        Args:
            storage_value: A JSON string as stored in Arrow.
            converter: Ignored. Present for protocol conformance.

        Returns:
            A ``Directory`` instance.

        Raises:
            ValueError: If ``storage_value`` is not valid JSON or lacks the
                ``"path"`` key.
            FileNotFoundError: If the path no longer exists.
            NotADirectoryError: If the path is now a non-directory.
        """
        try:
            data = json.loads(storage_value)
            path = data["path"]
        except (json.JSONDecodeError, KeyError, TypeError) as exc:
            raise ValueError(
                f"LogicalDirectory: cannot deserialise storage value {storage_value!r}; "
                'expected a JSON object with a "path" key, '
                'e.g. {"path": "/some/dir"}.'
            ) from exc

        if "ignore_callable" in data:
            fn = _try_import_callable(data["ignore_callable"])
            return Directory(path, ignore=fn)

        if "ignore" in data:
            return Directory(path, ignore=data["ignore"])

        return Directory(path)
