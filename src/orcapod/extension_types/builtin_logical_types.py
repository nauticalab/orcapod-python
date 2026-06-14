"""Built-in LogicalType implementations for orcapod.

Provides three built-in logical types registered into the default
``DataContext.logical_type_registry`` via ``contexts/data/v0.1.json``:

- ``LogicalPath``: maps ``pathlib.Path`` ↔ Arrow large_string extension "pathlib.Path"
- ``LogicalUPath``: maps ``upath.UPath`` ↔ Arrow large_string extension "upath.UPath"
- ``LogicalUUID``: maps ``uuid.UUID`` ↔ PyArrow built-in ``pa.uuid()`` ("arrow.uuid")

Note:
    All imports from orcapod.extension_types use direct submodule paths
    (e.g. ``from orcapod.extension_types.registry import ...``) rather than
    the package ``__init__`` to avoid circular imports when the context system
    loads this module at startup.
"""

from __future__ import annotations

import pathlib
import uuid as _uuid_module
from typing import TYPE_CHECKING, Any

from upath import UPath

from orcapod.extension_types.registry import make_arrow_extension_type, make_polars_extension_type
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import polars as pl
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")
    pl = LazyModule("polars")


class LogicalPath:
    """Logical type for ``pathlib.Path``.

    Stores paths as Arrow large strings using the custom extension type
    ``"pathlib.Path"`` with metadata ``b"orcapod.builtin"``.

    Example:
        >>> lt = LogicalPath()
        >>> lt.python_to_storage(pathlib.Path("/tmp/foo"))
        '/tmp/foo'
        >>> lt.storage_to_python('/tmp/foo')
        PosixPath('/tmp/foo')
    """

    _arrow_ext_class = make_arrow_extension_type(
        "pathlib.Path", pa.large_string(), b"orcapod.builtin"
    )
    _arrow_ext: pa.ExtensionType | None = None
    _polars_ext_class = make_polars_extension_type(
        "pathlib.Path", pa.large_string(), "orcapod.builtin"
    )
    _polars_ext: pl.BaseExtension | None = None

    logical_type_name: str = "pathlib.Path"
    python_type: type = pathlib.Path

    def get_arrow_extension_type(self) -> pa.ExtensionType:
        """Return the Arrow extension type for ``pathlib.Path``.

        Returns:
            A cached ``pa.ExtensionType`` instance with extension name
            ``"pathlib.Path"`` and storage type ``pa.large_string()``.
        """
        if LogicalPath._arrow_ext is None:
            LogicalPath._arrow_ext = LogicalPath._arrow_ext_class()
        return LogicalPath._arrow_ext

    def get_polars_extension_type(self) -> pl.BaseExtension:
        """Return the Polars extension type for ``pathlib.Path``.

        Returns:
            A cached ``pl.BaseExtension`` instance registered under
            ``"pathlib.Path"``.
        """
        if LogicalPath._polars_ext is None:
            LogicalPath._polars_ext = LogicalPath._polars_ext_class()
        return LogicalPath._polars_ext

    def python_to_storage(self, value: Any) -> str:
        """Convert a ``pathlib.Path`` to its string representation.

        Args:
            value: A ``pathlib.Path`` instance.

        Returns:
            The string form of the path (e.g. ``"/tmp/foo"``).
        """
        return str(value)

    def storage_to_python(self, storage_value: Any) -> pathlib.Path:
        """Reconstruct a ``pathlib.Path`` from its string representation.

        Args:
            storage_value: A string path as stored in Arrow.

        Returns:
            A ``pathlib.Path`` instance.
        """
        return pathlib.Path(storage_value)


class LogicalUPath:
    """Logical type for ``upath.UPath``.

    Stores paths as Arrow large strings using the custom extension type
    ``"upath.UPath"`` with metadata ``b"orcapod.builtin"``.

    Example:
        >>> lt = LogicalUPath()
        >>> lt.python_to_storage(UPath("s3://bucket/key"))
        's3://bucket/key'
        >>> lt.storage_to_python("s3://bucket/key")
        UPath('s3://bucket/key')
    """

    _arrow_ext_class = make_arrow_extension_type(
        "upath.UPath", pa.large_string(), b"orcapod.builtin"
    )
    _arrow_ext: pa.ExtensionType | None = None
    _polars_ext_class = make_polars_extension_type(
        "upath.UPath", pa.large_string(), "orcapod.builtin"
    )
    _polars_ext: pl.BaseExtension | None = None

    logical_type_name: str = "upath.UPath"
    python_type: type = UPath

    def get_arrow_extension_type(self) -> pa.ExtensionType:
        """Return the Arrow extension type for ``upath.UPath``.

        Returns:
            A cached ``pa.ExtensionType`` instance with extension name
            ``"upath.UPath"`` and storage type ``pa.large_string()``.
        """
        if LogicalUPath._arrow_ext is None:
            LogicalUPath._arrow_ext = LogicalUPath._arrow_ext_class()
        return LogicalUPath._arrow_ext

    def get_polars_extension_type(self) -> pl.BaseExtension:
        """Return the Polars extension type for ``upath.UPath``.

        Returns:
            A cached ``pl.BaseExtension`` instance registered under
            ``"upath.UPath"``.
        """
        if LogicalUPath._polars_ext is None:
            LogicalUPath._polars_ext = LogicalUPath._polars_ext_class()
        return LogicalUPath._polars_ext

    def python_to_storage(self, value: Any) -> str:
        """Convert a ``upath.UPath`` to its string representation.

        Args:
            value: A ``upath.UPath`` instance.

        Returns:
            The string form of the path (e.g. ``"s3://bucket/key"``).
        """
        return str(value)

    def storage_to_python(self, storage_value: Any) -> UPath:
        """Reconstruct a ``upath.UPath`` from its string representation.

        Args:
            storage_value: A string path as stored in Arrow.

        Returns:
            A ``upath.UPath`` instance.
        """
        return UPath(storage_value)
