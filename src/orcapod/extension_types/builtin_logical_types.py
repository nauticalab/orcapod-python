"""Built-in LogicalType implementations for orcapod.

Provides three built-in logical types registered into the default
``DataContext.logical_type_registry`` via ``contexts/data/v0.1.json``:

- ``LogicalPath``: maps ``pathlib.Path`` ↔ Arrow large_string extension ``"orcapod.path"``
- ``LogicalUPath``: maps ``upath.UPath`` ↔ Arrow large_string extension ``"orcapod.upath"``
- ``LogicalUUID``: maps ``uuid.UUID`` ↔ Arrow large_binary extension ``"orcapod.uuid"``

All three types use the ``orcapod.*`` extension name namespace rather than the upstream
module-qualified names (``"pathlib.Path"``, etc.). This gives Orcapod stable ownership of
the on-disk extension identity: even if the upstream library is renamed or restructured,
data written with these extension names continues to be readable without modification.

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

import polars as pl
import pyarrow as pa
from upath import UPath

from orcapod.extension_types.base_logical_type import BaseLogicalType
from orcapod.extension_types.registry import make_arrow_extension_type, make_polars_extension_type

if TYPE_CHECKING:
    from orcapod.extension_types.protocols import TypeConverterProtocol


class LogicalPath(BaseLogicalType):
    """Logical type for ``pathlib.Path``.

    Stores paths as Arrow large strings using the custom extension type
    ``"orcapod.path"``.

    The extension name ``"orcapod.path"`` is Orcapod-owned and stable; it does not
    depend on the upstream ``pathlib`` module path. Use ``orcapod.Path`` (a top-level
    alias for ``pathlib.Path``) as the preferred way to reference this type in user code.

    Example:
        >>> lt = LogicalPath()
        >>> lt.python_to_storage(pathlib.Path("/tmp/foo"))
        '/tmp/foo'
        >>> lt.storage_to_python('/tmp/foo')
        PosixPath('/tmp/foo')
    """

    _arrow_ext_class = make_arrow_extension_type("orcapod.path", pa.large_string())
    _arrow_ext: pa.ExtensionType | None = None
    _polars_ext_class = make_polars_extension_type("orcapod.path", pa.large_string())
    _polars_ext: pl.BaseExtension | None = None

    logical_type_name: str = "orcapod.path"
    python_type: type = pathlib.Path

    def get_arrow_extension_type(self) -> pa.ExtensionType:
        """Return the Arrow extension type for ``pathlib.Path``.

        Returns:
            A cached ``pa.ExtensionType`` instance with extension name
            ``"orcapod.path"`` and storage type ``pa.large_string()``.
        """
        if LogicalPath._arrow_ext is None:
            LogicalPath._arrow_ext = LogicalPath._arrow_ext_class()
        return LogicalPath._arrow_ext

    def get_polars_extension_type(self) -> pl.BaseExtension:
        """Return the Polars extension type for ``pathlib.Path``.

        Returns:
            A cached ``pl.BaseExtension`` instance registered under
            ``"orcapod.path"``.
        """
        if LogicalPath._polars_ext is None:
            LogicalPath._polars_ext = LogicalPath._polars_ext_class()
        return LogicalPath._polars_ext

    def python_to_storage(self, value: Any, converter: TypeConverterProtocol | None = None) -> str:
        """Convert a ``pathlib.Path`` to its string representation.

        Args:
            value: A ``pathlib.Path`` instance.
            converter: Ignored. Present for protocol conformance.

        Returns:
            The string form of the path (e.g. ``"/tmp/foo"``).
        """
        return str(value)

    def storage_to_python(self, storage_value: Any, converter: TypeConverterProtocol | None = None) -> pathlib.Path:
        """Reconstruct a ``pathlib.Path`` from its string representation.

        Args:
            storage_value: A string path as stored in Arrow.
            converter: Ignored. Present for protocol conformance.

        Returns:
            A ``pathlib.Path`` instance.
        """
        return pathlib.Path(storage_value)


class LogicalUPath(BaseLogicalType):
    """Logical type for ``upath.UPath``.

    Stores paths as Arrow large strings using the custom extension type
    ``"orcapod.upath"``.

    The extension name ``"orcapod.upath"`` is Orcapod-owned and stable; it does not
    depend on the upstream ``upath`` module path. Use ``orcapod.UPath`` (a top-level
    alias for ``upath.UPath``) as the preferred way to reference this type in user code.

    Example:
        >>> lt = LogicalUPath()
        >>> lt.python_to_storage(UPath("s3://bucket/key"))
        's3://bucket/key'
        >>> lt.storage_to_python("s3://bucket/key")
        UPath('s3://bucket/key')
    """

    _arrow_ext_class = make_arrow_extension_type("orcapod.upath", pa.large_string())
    _arrow_ext: pa.ExtensionType | None = None
    _polars_ext_class = make_polars_extension_type("orcapod.upath", pa.large_string())
    _polars_ext: pl.BaseExtension | None = None

    logical_type_name: str = "orcapod.upath"
    python_type: type = UPath

    def get_arrow_extension_type(self) -> pa.ExtensionType:
        """Return the Arrow extension type for ``upath.UPath``.

        Returns:
            A cached ``pa.ExtensionType`` instance with extension name
            ``"orcapod.upath"`` and storage type ``pa.large_string()``.
        """
        if LogicalUPath._arrow_ext is None:
            LogicalUPath._arrow_ext = LogicalUPath._arrow_ext_class()
        return LogicalUPath._arrow_ext

    def get_polars_extension_type(self) -> pl.BaseExtension:
        """Return the Polars extension type for ``upath.UPath``.

        Returns:
            A cached ``pl.BaseExtension`` instance registered under
            ``"orcapod.upath"``.
        """
        if LogicalUPath._polars_ext is None:
            LogicalUPath._polars_ext = LogicalUPath._polars_ext_class()
        return LogicalUPath._polars_ext

    def python_to_storage(self, value: Any, converter: TypeConverterProtocol | None = None) -> str:
        """Convert a ``upath.UPath`` to its string representation.

        Args:
            value: A ``upath.UPath`` instance.
            converter: Ignored. Present for protocol conformance.

        Returns:
            The string form of the path (e.g. ``"s3://bucket/key"``).
        """
        return str(value)

    def storage_to_python(self, storage_value: Any, converter: TypeConverterProtocol | None = None) -> UPath:
        """Reconstruct a ``upath.UPath`` from its string representation.

        Args:
            storage_value: A string path as stored in Arrow.
            converter: Ignored. Present for protocol conformance.

        Returns:
            A ``upath.UPath`` instance.
        """
        return UPath(storage_value)


class LogicalUUID(BaseLogicalType):
    """Logical type for ``uuid.UUID``.

    Stores UUIDs as Arrow binary (16 bytes) using the custom extension type
    ``"orcapod.uuid"``. Both the Arrow extension name and ``logical_type_name``
    are ``"orcapod.uuid"``, consistent with ``LogicalPath`` and ``LogicalUPath``.

    The extension name ``"orcapod.uuid"`` is Orcapod-owned and stable, replacing
    the previous ``"uuid.UUID"`` name that mirrored PyArrow's ``"arrow.uuid"``
    territory. Use ``orcapod.UUID`` (a top-level alias for ``uuid.UUID``) as the
    preferred way to reference this type in user code.

    The storage type is ``pa.large_binary()`` (variable-length binary), using
    big-endian byte order as returned by ``uuid.UUID.bytes``. ``large_binary``
    is used rather than ``pa.binary(16)`` (fixed-size) because Polars maps
    fixed-size binary to variable-length on the round-trip, which would
    conflict with the deserializer's storage type check.

    Example:
        >>> import uuid
        >>> lt = LogicalUUID()
        >>> u = uuid.uuid4()
        >>> lt.storage_to_python(lt.python_to_storage(u)) == u
        True
    """

    _arrow_ext_class = make_arrow_extension_type("orcapod.uuid", pa.large_binary())
    _arrow_ext: pa.ExtensionType | None = None
    _polars_ext_class = make_polars_extension_type("orcapod.uuid", pa.large_binary())
    _polars_ext: pl.BaseExtension | None = None

    logical_type_name: str = "orcapod.uuid"
    python_type: type = _uuid_module.UUID

    def get_arrow_extension_type(self) -> pa.ExtensionType:
        """Return the Arrow extension type for ``uuid.UUID``.

        Returns:
            A cached ``pa.ExtensionType`` instance with extension name
            ``"orcapod.uuid"`` and storage type ``pa.large_binary()``.
        """
        if LogicalUUID._arrow_ext is None:
            LogicalUUID._arrow_ext = LogicalUUID._arrow_ext_class()
        return LogicalUUID._arrow_ext

    def get_polars_extension_type(self) -> pl.BaseExtension:
        """Return the Polars extension type for ``uuid.UUID``.

        Returns:
            A cached ``pl.BaseExtension`` instance registered under
            ``"orcapod.uuid"``.
        """
        if LogicalUUID._polars_ext is None:
            LogicalUUID._polars_ext = LogicalUUID._polars_ext_class()
        return LogicalUUID._polars_ext

    def python_to_storage(self, value: Any, converter: TypeConverterProtocol | None = None) -> bytes:
        """Convert a ``uuid.UUID`` to its 16-byte binary representation.

        Args:
            value: A ``uuid.UUID`` instance.
            converter: Ignored. Present for protocol conformance.

        Returns:
            A 16-byte ``bytes`` object (big-endian byte order, as per
            ``uuid.UUID.bytes``).
        """
        return value.bytes

    def storage_to_python(self, storage_value: Any, converter: TypeConverterProtocol | None = None) -> _uuid_module.UUID:
        """Reconstruct a ``uuid.UUID`` from its 16-byte binary representation.

        Args:
            storage_value: A bytes-like object of length 16.
            converter: Ignored. Present for protocol conformance.

        Returns:
            A ``uuid.UUID`` instance.
        """
        return _uuid_module.UUID(bytes=bytes(storage_value))
