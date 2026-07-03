"""orcapod.File — content-identified, existence-validated file path.

``File`` wraps a ``upath.UPath`` and validates that the path points to a readable,
non-directory file at construction time. Use ``pathlib.Path`` / ``upath.UPath`` for
paths that may not yet exist.

``LogicalFile`` is the Arrow extension type that serialises ``File`` instances as
``large_string`` columns tagged with the ``"orcapod.file"`` extension name.
"""

from __future__ import annotations

import json
import os
from typing import TYPE_CHECKING, Any, Self

import polars as pl
import pyarrow as pa
from upath import UPath
from upath.extensions import ProxyUPath

from orcapod.extension_types.base_logical_type import BaseLogicalType
from orcapod.extension_types.registry import make_arrow_extension_type, make_polars_extension_type

if TYPE_CHECKING:
    from orcapod.extension_types.protocols import TypeConverterProtocol


class File(ProxyUPath):
    """A content-identified, existence-validated file path.

    Wraps a ``UPath`` and validates that the path refers to a readable, non-directory
    file at construction time. Works across local, S3, GCS, and any other
    fsspec-backed backend supported by ``upath``.

    Note:
        ``isinstance(file_instance, UPath)`` returns ``False`` because ``ProxyUPath``
        does not inherit from ``UPath``. Use ``isinstance(x, File)`` to type-check.

    Args:
        *args: Positional path arguments forwarded to ``UPath``.
        follow_symlinks: If ``True`` (default), symlinks are followed and the
            resolved target is validated and hashed. If ``False``, raises
            ``ValueError`` on construction when the path is a symlink.
        **kwargs: Keyword arguments forwarded to ``UPath``.

    Raises:
        FileNotFoundError: If the path does not exist.
        IsADirectoryError: If the path is a directory (or a symlink to one when
            ``follow_symlinks=True``).
        ValueError: If the path is a symlink and ``follow_symlinks=False``, or if
            the path exists but is not a regular file.

    Example:
        >>> f = File("/tmp/data.csv")
        >>> str(f)
        '/tmp/data.csv'
        >>> File("/tmp/nonexistent.csv")
        FileNotFoundError: ...
    """

    def __init__(self, *args: Any, follow_symlinks: bool = True, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        if not follow_symlinks and self.__wrapped__.is_symlink():
            raise ValueError(
                f"File: path is a symlink and follow_symlinks=False: {self.__wrapped__!r}"
            )
        if not self.__wrapped__.exists():
            raise FileNotFoundError(
                f"File: path does not exist: {self.__wrapped__!r}"
            )
        if self.__wrapped__.is_dir():
            raise IsADirectoryError(
                f"File: path is a directory: {self.__wrapped__!r}"
            )
        if not self.__wrapped__.is_file():
            raise ValueError(
                f"File: path is not a regular file: {self.__wrapped__!r}"
            )

    def __fspath__(self) -> str:
        """Return the file system path representation of the underlying ``UPath``.

        Succeeds for local-backed paths (``PosixUPath``, ``FilePath``) and returns
        the local path string. Raises ``TypeError`` for remote-backed paths (S3, GCS,
        engm, …), consistent with how ``UPath`` itself behaves for those backends.

        Returns:
            The local filesystem path as a string.

        Raises:
            TypeError: If the underlying path is remote-backed (e.g. S3, GCS) and does
                not support local filesystem operations.
        """
        return os.fspath(self.__wrapped__)

    @classmethod
    def _from_upath(cls, upath: UPath, /) -> Self:
        """Create a ``File`` from an existing ``UPath`` without validation.

        Used internally by ``ProxyUPath`` for derived paths (e.g. ``.parent``,
        ``/`` operator). Validation is intentionally skipped — derived paths from
        navigation may not exist yet. ``follow_symlinks`` defaults to ``True`` on
        all derived instances.
        """
        obj = object.__new__(cls)
        object.__setattr__(obj, "__wrapped__", upath)
        return obj


class LogicalFile(BaseLogicalType):
    """Logical type for ``orcapod.File``.

    Stores ``File`` instances as Arrow large strings using the custom extension
    type ``"orcapod.file"``. The stored value is a JSON string of the form
    ``{"path": "/tmp/data.csv"}`` encoding the file path.

    On read (``storage_to_python``), the path is used to reconstruct a ``File``
    instance, which re-validates existence. Reading an Arrow table with
    ``"orcapod.file"`` columns will raise ``FileNotFoundError`` if the underlying
    files have been moved or deleted — this is the correct semantic for a
    content-identified type.

    Example:
        >>> import tempfile
        >>> lt = LogicalFile()
        >>> with tempfile.NamedTemporaryFile(delete=False) as f:
        ...     _ = f.write(b"hello")
        ...     tmp = f.name
        >>> file = File(tmp)
        >>> lt.storage_to_python(lt.python_to_storage(file)) == file
        True
    """

    _arrow_ext_class = make_arrow_extension_type("orcapod.file", pa.large_string())
    _arrow_ext: pa.ExtensionType | None = None
    _polars_ext_class = make_polars_extension_type("orcapod.file", pa.large_string())
    _polars_ext: pl.BaseExtension | None = None

    logical_type_name: str = "orcapod.file"
    python_type: type = File

    def get_arrow_extension_type(self) -> pa.ExtensionType:
        """Return the Arrow extension type for ``File``.

        Returns:
            A cached ``pa.ExtensionType`` with extension name ``"orcapod.file"``
            and storage type ``pa.large_string()``.
        """
        if LogicalFile._arrow_ext is None:
            LogicalFile._arrow_ext = LogicalFile._arrow_ext_class()
        return LogicalFile._arrow_ext

    def get_polars_extension_type(self) -> pl.BaseExtension:
        """Return the Polars extension type for ``File``.

        Returns:
            A cached ``pl.BaseExtension`` registered under ``"orcapod.file"``.
        """
        if LogicalFile._polars_ext is None:
            LogicalFile._polars_ext = LogicalFile._polars_ext_class()
        return LogicalFile._polars_ext

    def python_to_storage(self, value: Any, converter: TypeConverterProtocol | None = None) -> str:
        """Convert a ``File`` to its JSON storage representation.

        Args:
            value: A ``File`` instance.
            converter: Ignored. Present for protocol conformance.

        Returns:
            A JSON string ``{"path": "<path>"}`` encoding the file path.
        """
        return json.dumps({"path": str(value)})

    def storage_to_python(self, storage_value: Any, converter: TypeConverterProtocol | None = None) -> File:
        """Reconstruct a ``File`` from its stored JSON string.

        Re-validates existence on read — raises ``FileNotFoundError`` if the file
        no longer exists at the stored path.

        Args:
            storage_value: A JSON string as stored in Arrow.
            converter: Ignored. Present for protocol conformance.

        Returns:
            A ``File`` instance.

        Raises:
            ValueError: If ``storage_value`` is not valid JSON or lacks the
                ``"path"`` key.
            FileNotFoundError: If the path no longer exists.
            IsADirectoryError: If the path is now a directory.
        """
        try:
            path = json.loads(storage_value)["path"]
        except (json.JSONDecodeError, KeyError, TypeError) as exc:
            raise ValueError(
                f"LogicalFile: cannot deserialise storage value {storage_value!r}; "
                'expected a JSON object with a "path" key, '
                'e.g. {"path": "/some/file.csv"}.'
            ) from exc
        return File(path)
