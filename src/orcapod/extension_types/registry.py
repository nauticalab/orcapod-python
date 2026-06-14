"""Registry for ExtensionTypeConverter instances.

Registering a converter automatically registers the corresponding
extension type in both PyArrow's and Polars' global registries.
"""

from __future__ import annotations

import re

import pyarrow as pa
import polars as pl

from orcapod.extension_types.protocols import ExtensionTypeConverter

# ---------------------------------------------------------------------------
# Shadow dicts — track what *we* have registered in the global registries.
# These are module-level singletons shared across all ExtensionTypeRegistry
# instances. We use our own dicts rather than querying library internals
# because neither PyArrow nor Polars exposes a stable public API for looking
# up a previously registered extension type by name.
#
# Limitation: types registered externally (directly via
# pa.register_extension_type / pl.register_extension_type, bypassing this
# module) will not appear here. A subsequent register() call for the same
# name will detect the conflict via the library-level error and raise,
# because without knowing what was registered externally we cannot guarantee
# the same extension name maps to the same Python class and underlying
# storage type — silently proceeding risks data corruption or misrouted
# conversions at read time.
# ---------------------------------------------------------------------------

_ARROW_REGISTRY: dict[str, tuple[pa.DataType, bytes]] = {}
# extension_name -> (storage_type, metadata_bytes)

_POLARS_REGISTRY: dict[str, tuple[pl.DataType, str | None]] = {}
# extension_name -> (pl_storage_dtype, metadata_str)


def _sanitize(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9]", "_", name)


def _register_arrow_ext_type(converter: ExtensionTypeConverter) -> None:
    """Register a ``pa.ExtensionType`` subclass for *converter* in PyArrow's global registry."""
    name = converter.extension_name
    metadata = converter.extension_metadata or b""
    storage = converter.storage_type

    if name in _ARROW_REGISTRY:
        existing_storage, existing_metadata = _ARROW_REGISTRY[name]
        if existing_storage == storage and existing_metadata == metadata:
            return  # idempotent — safe for module reload and test-suite reuse
        raise ValueError(
            f"Extension type '{name}' is already registered in the PyArrow global registry "
            f"with different parameters.\n"
            f"  Registered: storage_type={existing_storage!r}, metadata={existing_metadata!r}\n"
            f"  Attempted:  storage_type={storage!r}, metadata={metadata!r}"
        )

    _name, _storage, _metadata = name, storage, metadata
    ArrowExtType = type(
        f"_ArrowExt_{_sanitize(name)}",
        (pa.ExtensionType,),
        {
            "__init__": lambda self: pa.ExtensionType.__init__(self, _storage, _name),
            "__arrow_ext_serialize__": lambda self: _metadata,
            "__arrow_ext_deserialize__": classmethod(lambda cls, st, se: cls()),
        },
    )

    try:
        pa.register_extension_type(ArrowExtType())
    except pa.lib.ArrowKeyError:
        raise ValueError(
            f"Extension type '{name}' is already registered in the PyArrow global registry "
            f"by an external source. Cannot verify equivalence; orcapod requires exclusive "
            f"ownership of extension type registrations to prevent data corruption or "
            f"misrouted conversions. See PLT-1665 for future interop support."
        ) from None

    _ARROW_REGISTRY[name] = (storage, metadata)


def _register_polars_ext_type(converter: ExtensionTypeConverter) -> None:
    """Register a ``pl.BaseExtension`` subclass for *converter* in Polars' global registry."""
    name = converter.extension_name
    metadata = converter.extension_metadata
    metadata_str = metadata.decode("utf-8") if metadata else None
    pl_storage = pl.from_arrow(pa.array([], type=converter.storage_type)).dtype

    if name in _POLARS_REGISTRY:
        existing_storage, existing_meta = _POLARS_REGISTRY[name]
        if existing_storage == pl_storage and existing_meta == metadata_str:
            return  # idempotent
        raise ValueError(
            f"Extension type '{name}' is already registered in the Polars global registry "
            f"with different parameters.\n"
            f"  Registered: storage_dtype={existing_storage!r}, metadata={existing_meta!r}\n"
            f"  Attempted:  storage_dtype={pl_storage!r}, metadata={metadata_str!r}"
        )

    _name, _pl_storage, _meta_str = name, pl_storage, metadata_str
    PolarsExtType = type(
        f"_PolarsExt_{_sanitize(name)}",
        (pl.BaseExtension,),
        {
            "__init__": lambda self: pl.BaseExtension.__init__(self, _name, _pl_storage, _meta_str),
            "ext_from_params": classmethod(lambda cls, n, s, m: cls()),
        },
    )

    try:
        pl.register_extension_type(name, PolarsExtType)
    except ValueError as exc:
        raise ValueError(
            f"Extension type '{name}' is already registered in the Polars global registry "
            f"by an external source. Cannot verify equivalence; orcapod requires exclusive "
            f"ownership of extension type registrations to prevent data corruption or "
            f"misrouted conversions. See PLT-1665 for future interop support."
        ) from exc

    _POLARS_REGISTRY[name] = (pl_storage, metadata_str)


class ExtensionTypeRegistry:
    """Registry for ``ExtensionTypeConverter`` instances.

    Registering a converter automatically registers the corresponding
    extension type in both PyArrow's and Polars' global registries.

    The primary lookup key is ``extension_name``; a secondary lookup by
    ``python_type`` is provided for the write path.

    Example:
        >>> registry = ExtensionTypeRegistry()
        >>> registry.register(my_converter)
        >>> conv = registry.get_converter_for_name("my.Type")
    """

    def __init__(self) -> None:
        self._by_name: dict[str, ExtensionTypeConverter] = {}
        self._by_python_type: dict[type, ExtensionTypeConverter] = {}

    def register(self, converter: ExtensionTypeConverter) -> None:
        """Register *converter* and its PyArrow/Polars extension types.

        Args:
            converter: An ``ExtensionTypeConverter`` instance to register.

        Raises:
            ValueError: If ``converter.extension_name`` is already registered
                in this registry instance.
            ValueError: If the extension name is already in the PA or Polars
                global registry with different parameters.
            ValueError: If the extension name is already in the PA or Polars
                global registry from an external source (equivalence cannot
                be verified).
        """
        name = converter.extension_name
        if name in self._by_name:
            raise ValueError(
                f"Extension type '{name}' is already registered in this registry."
            )
        self._by_name[name] = converter
        self._by_python_type[converter.python_type] = converter
        _register_arrow_ext_type(converter)
        _register_polars_ext_type(converter)

    def get_converter_for_name(self, name: str) -> ExtensionTypeConverter | None:
        """Return the converter registered under *name*, or ``None``."""
        return self._by_name.get(name)

    def get_converter_for_python_type(self, python_type: type) -> ExtensionTypeConverter | None:
        """Return the converter for *python_type*, or ``None``.

        Checks exact match first, then falls back to an ``issubclass`` scan.
        When multiple registered types are superclasses of *python_type*, the
        one registered first wins (insertion-order dict, Python 3.7+).
        """
        converter = self._by_python_type.get(python_type)
        if converter is not None:
            return converter
        for registered_type, conv in self._by_python_type.items():
            if issubclass(python_type, registered_type):
                return conv
        return None

    def has_extension_name(self, name: str) -> bool:
        """Return ``True`` if *name* is registered."""
        return name in self._by_name

    def has_python_type(self, python_type: type) -> bool:
        """Return ``True`` if *python_type* (or a subclass) is registered."""
        return self.get_converter_for_python_type(python_type) is not None

    def list_extension_names(self) -> list[str]:
        """Return all registered extension names in insertion order."""
        return list(self._by_name.keys())

    def list_python_types(self) -> list[type]:
        """Return all registered Python types in insertion order."""
        return list(self._by_python_type.keys())
