"""Registry for LogicalType instances.

Registering a logical type automatically registers the corresponding
extension type in both PyArrow's and Polars' global registries.
"""

from __future__ import annotations

import re

import polars as pl
import pyarrow as pa

from orcapod.extension_types.protocols import LogicalType


def _sanitize(name: str) -> str:
    """Replace non-alphanumeric characters with underscores.

    Used to produce a valid Python identifier for the dynamically created
    ``pa.ExtensionType`` subclass name.
    """
    return re.sub(r"[^A-Za-z0-9]", "_", name)


def make_arrow_extension_type(
    extension_name: str,
    storage_type: pa.DataType,
    metadata: bytes | None = None,
) -> type[pa.ExtensionType]:
    """Synthesise and return a ``pa.ExtensionType`` subclass.

    Returns the *class*, not an instance — callers instantiate it inside their
    ``get_arrow_extension_type()`` implementation. Returning the class preserves
    the option to create multiple instances or future parameterised variants from
    the same class.

    This is a low-level building block. The full pattern for binding a Python
    type to a specific Arrow/Polars representation — the extension type factory —
    is the responsibility of each ``LogicalType`` implementation. See PLT-1656
    for the built-in implementations (``Path``, ``UPath``, ``UUID``).

    Args:
        extension_name: The Arrow extension name (``ARROW:extension:name``).
        storage_type: The underlying Arrow storage type.
        metadata: Optional bytes stored as ``ARROW:extension:metadata``.
            Defaults to ``None`` (serialised as empty bytes).

    Returns:
        A ``pa.ExtensionType`` subclass. Call it with no arguments to obtain
        an instance suitable for passing to ``pa.register_extension_type`` or
        returning from ``get_arrow_extension_type()``.
    """
    _name, _storage, _metadata = extension_name, storage_type, metadata or b""
    return type(
        f"_ArrowExt_{_sanitize(extension_name)}",
        (pa.ExtensionType,),
        {
            "__init__": lambda self: pa.ExtensionType.__init__(self, _storage, _name),
            "__arrow_ext_serialize__": lambda self: _metadata,
            # __arrow_ext_deserialize__ reconstructs the type descriptor from schema
            # metadata (called once per IPC/Parquet read, not per value). The storage
            # type and metadata are baked into the constructor via closure, so
            # arguments are intentionally ignored.
            "__arrow_ext_deserialize__": classmethod(
                lambda cls, storage_type, serialized: cls()
            ),
        },
    )


class LogicalTypeRegistry:
    """Registry for ``LogicalType`` instances.

    Maintains a three-way binding: ``(logical_type_name, arrow_extension_name,
    python_type)`` → ``LogicalType``. Each key participates in at most one
    binding within a registry instance.

    Registering a logical type side-effect-registers the corresponding extension
    type in PyArrow's and Polars' global registries. Pre-existing types (those
    already registered externally, e.g. PyArrow's built-in ``"arrow.uuid"``) are
    accepted silently — the binding is stored without error.

    The process-global ``default_logical_type_registry`` instance provides
    effective process-wide uniqueness for normal use. Thread-safety is deferred.

    Example:
        >>> registry = LogicalTypeRegistry()
        >>> registry.register(my_logical_type)
        >>> lt = registry.get_by_logical_name("uuid.UUID")
    """

    def __init__(self) -> None:
        self._by_logical_name: dict[str, LogicalType] = {}
        self._by_arrow_name: dict[str, LogicalType] = {}
        self._by_python_type: dict[type, LogicalType] = {}

    def register(self, logical_type: LogicalType) -> None:
        """Register *logical_type* and its PyArrow/Polars extension types.

        Args:
            logical_type: A ``LogicalType`` instance to register.

        Raises:
            ValueError: If any of the three keys (``logical_type_name``,
                Arrow extension name, ``python_type``) is already bound to a
                *different* ``LogicalType`` in this registry.
        """
        arrow_ext_name = logical_type.get_arrow_extension_type().extension_name
        py_type = logical_type.python_type
        logical_name = logical_type.logical_type_name

        existing_by_logical = self._by_logical_name.get(logical_name)
        existing_by_arrow = self._by_arrow_name.get(arrow_ext_name)
        existing_by_python = self._by_python_type.get(py_type)

        # Triplet conflict check: raise if any key is bound to a different instance.
        for existing, label, key in [
            (existing_by_logical, "logical_type_name", logical_name),
            (existing_by_arrow, "arrow_extension_name", arrow_ext_name),
            (existing_by_python, "python_type", py_type.__qualname__),
        ]:
            if existing is not None and existing is not logical_type:
                raise ValueError(
                    f"Cannot register logical type '{logical_name}': "
                    f"{label} {key!r} is already bound to "
                    f"'{existing.logical_type_name}'."
                )

        # Idempotent check: all three keys already bound to this same instance.
        if (
            existing_by_logical is logical_type
            and existing_by_arrow is logical_type
            and existing_by_python is logical_type
        ):
            return

        # Register Arrow extension type. ArrowKeyError means the name is already
        # in PyArrow's global registry (pre-existing type or another registry
        # instance). Accept silently — PLT-1669 adds post-error validation.
        try:
            pa.register_extension_type(logical_type.get_arrow_extension_type())
        except pa.lib.ArrowKeyError:
            pass

        # Register Polars extension type. ValueError or ComputeError means already registered.
        # Polars raises ValueError via its Python-level guard (_REGISTRY dict check), but
        # raises polars.exceptions.ComputeError when the lower-level Rust registry detects
        # the duplicate (e.g. when the Polars Python dict was already cleared or bypassed).
        # Both errors mean "already registered" — accept silently.
        polars_ext_class = type(logical_type.get_polars_extension_type())
        try:
            pl.register_extension_type(arrow_ext_name, polars_ext_class)
        except (ValueError, pl.exceptions.ComputeError):
            pass

        # Store three-way binding.
        self._by_logical_name[logical_name] = logical_type
        self._by_arrow_name[arrow_ext_name] = logical_type
        self._by_python_type[py_type] = logical_type

    def get_by_logical_name(self, name: str) -> LogicalType | None:
        """Return the logical type registered under *name*, or ``None``."""
        return self._by_logical_name.get(name)

    def get_by_python_type(self, python_type: type) -> LogicalType | None:
        """Return the logical type for *python_type*, or ``None``.

        Checks exact match first, then falls back to an ``issubclass`` scan.
        When multiple registered types are superclasses of *python_type*, the
        one registered first wins (insertion-order dict, Python 3.7+).
        """
        result = self._by_python_type.get(python_type)
        if result is not None:
            return result
        for registered_type, registered_lt in self._by_python_type.items():
            try:
                if issubclass(python_type, registered_type):
                    return registered_lt
            except TypeError:
                continue
        return None

    def get_by_arrow_extension_name(self, arrow_name: str) -> LogicalType | None:
        """Return the logical type registered under *arrow_name*, or ``None``."""
        return self._by_arrow_name.get(arrow_name)
