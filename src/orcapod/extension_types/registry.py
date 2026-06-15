"""Registry for LogicalType instances.

Registering a logical type automatically registers the corresponding
extension type in both PyArrow's and Polars' global registries.
"""

from __future__ import annotations

import json
import logging
import re
from typing import TYPE_CHECKING

from orcapod.extension_types.protocols import LogicalType, LogicalTypeFactory
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import polars as pl
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")
    pl = LazyModule("polars")

logger = logging.getLogger(__name__)


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

    This is a low-level building block. Each ``LogicalType`` implementation acts
    as a factory: it creates and owns the ``pa.ExtensionType`` instance it requires
    and exposes it via ``get_arrow_extension_type()``. See PLT-1656 for the
    built-in implementations (``Path``, ``UPath``, ``UUID``).

    Args:
        extension_name: The Arrow extension name (``ARROW:extension:name``).
        storage_type: The underlying Arrow storage type.
        metadata: Optional bytes stored as ``ARROW:extension:metadata``.
            Defaults to ``None`` (serialised as empty bytes).

            ``metadata`` can optionally encode a **LogicalType category** as a
            UTF-8 JSON object with at least a ``"category"`` key
            (e.g. ``b'{"category": "Dataclass"}'``,
            ``b'{"category": "Pydantic", "pydantic_version": 2}'``).
            A ``LogicalTypeFactory`` (see ``LogicalTypeFactory.create_logical_type``)
            dispatches on the ``"category"`` value when reading schemas from IPC or
            Parquet files and uses it to auto-generate the correct ``LogicalType``
            for the specific Python class within that category, without requiring
            explicit prior registration.

    Returns:
        A ``pa.ExtensionType`` subclass. Call it with no arguments to obtain
        an instance suitable for passing to ``pa.register_extension_type`` or
        returning from ``get_arrow_extension_type()``.
    """
    _name, _storage, _metadata = extension_name, storage_type, metadata or b""

    def _deserialize(cls, storage_type: pa.DataType, serialized: bytes) -> pa.ExtensionType:
        # __arrow_ext_deserialize__ reconstructs the type descriptor from schema
        # metadata (called once per IPC/Parquet read, not per value). Validate the
        # incoming storage_type and serialized bytes against the expected values so
        # that reading a file where the same extension name was written with different
        # parameters raises immediately rather than silently producing wrong data.
        if storage_type != _storage:
            raise ValueError(
                f"Arrow extension type '{_name}': expected storage_type "
                f"{_storage!r} but got {storage_type!r}."
            )
        if serialized != _metadata:
            raise ValueError(
                f"Arrow extension type '{_name}': expected metadata "
                f"{_metadata!r} but got {serialized!r}."
            )
        return cls()

    return type(
        f"_ArrowExt_{_sanitize(extension_name)}",
        (pa.ExtensionType,),
        {
            "__init__": lambda self: pa.ExtensionType.__init__(self, _storage, _name),
            "__arrow_ext_serialize__": lambda self: _metadata,
            "__arrow_ext_deserialize__": classmethod(_deserialize),
        },
    )


def make_polars_extension_type(
    extension_name: str,
    arrow_storage_type: pa.DataType,
    metadata: str | None = None,
) -> type[pl.BaseExtension]:
    """Synthesise and return a ``pl.BaseExtension`` subclass.

    Derives the Polars storage dtype from *arrow_storage_type* via
    ``pl.from_arrow``. Returns the *class*; callers instantiate it inside
    ``get_polars_extension_type()``.

    The returned class uses the Arrow extension name as its registration name
    (the same name passed to ``pl.register_extension_type``), so that Polars
    correctly maps Arrow extension columns on read.

    Args:
        extension_name: The extension type name used for Polars registration.
            Must match the Arrow extension name so Polars can round-trip the
            type through Arrow IPC.
        arrow_storage_type: The Arrow storage type. Converted once to the
            corresponding Polars dtype via ``pl.from_arrow``.
        metadata: Optional metadata string stored as ``metadata_str`` in the
            Polars extension. Defaults to ``None``.

    Returns:
        A ``pl.BaseExtension`` subclass. Call it with no arguments to obtain
        an instance suitable for passing to ``pl.register_extension_type`` or
        returning from ``get_polars_extension_type()``.
    """
    _name = extension_name
    _polars_dtype = pl.from_arrow(pa.array([], type=arrow_storage_type)).dtype
    _metadata = metadata

    def __init__(self: pl.BaseExtension) -> None:
        pl.BaseExtension.__init__(self, _name, _polars_dtype, _metadata)

    @classmethod  # type: ignore[misc]
    def ext_from_params(
        cls: type[pl.BaseExtension],
        ext_name: str,
        storage_dtype: pl.PolarsDataType,
        metadata_str: str | None,
    ) -> pl.BaseExtension:
        return cls()

    return type(
        f"_PolarsExt_{_sanitize(extension_name)}",
        (pl.BaseExtension,),
        {
            "__init__": __init__,
            "ext_from_params": ext_from_params,
        },
    )


class LogicalTypeRegistry:
    """Registry for ``LogicalType`` instances.

    Maintains a three-way binding: ``(logical_type_name, arrow_extension_name,
    python_type)`` → ``LogicalType``. Each key participates in at most one
    binding within a registry instance.

    Registering a logical type side-effect-registers the corresponding extension
    type in PyArrow's and Polars' global registries. Pre-existing types (those
    already registered externally in the global Arrow or Polars registries) are
    accepted silently — the binding is stored without error.

    The standard access path for the default registry is
    ``get_default_context().logical_type_registry`` or the convenience function
    ``get_default_logical_type_registry()`` from ``orcapod.contexts``.
    Thread-safety is deferred.

    An optional ``logical_types`` list can be passed at construction time to
    pre-register one or more ``LogicalType`` instances immediately, following
    the same pattern as ``SemanticTypeRegistry``'s ``converters`` constructor
    argument.

    Example:
        >>> registry = LogicalTypeRegistry()
        >>> registry.register(my_logical_type)
        >>> lt = registry.get_by_logical_name("uuid.UUID")

        >>> # Pre-register types at construction:
        >>> registry = LogicalTypeRegistry(logical_types=[path_lt, uuid_lt])
    """

    def __init__(self, logical_types: list[LogicalType] | None = None) -> None:
        self._by_logical_name: dict[str, LogicalType] = {}
        self._by_arrow_name: dict[str, LogicalType] = {}
        self._by_python_type: dict[type, LogicalType] = {}
        self._factories: dict[str, LogicalTypeFactory] = {}
        for lt in (logical_types or []):
            self.register(lt)

    def register(self, logical_type: LogicalType) -> None:
        """Register *logical_type* and its PyArrow/Polars extension types.

        Args:
            logical_type: A ``LogicalType`` instance to register.

        Raises:
            ValueError: If any of the three keys (``logical_type_name``,
                Arrow extension name, ``python_type``) is already bound to a
                *different* ``LogicalType`` in this registry.
        """
        arrow_ext = logical_type.get_arrow_extension_type()
        arrow_ext_name = arrow_ext.extension_name
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
            pa.register_extension_type(arrow_ext)
        except pa.lib.ArrowKeyError:
            pass

        # Register Polars extension type. ValueError or ComputeError means already registered.
        # Polars raises ValueError via its Python-level guard (_REGISTRY dict check), but
        # raises polars.exceptions.ComputeError when the lower-level Rust registry detects
        # the duplicate (e.g. when the Polars Python dict was already cleared or bypassed).
        # Both errors mean "already registered" — accept silently.
        polars_ext = logical_type.get_polars_extension_type()
        polars_ext_class = type(polars_ext)
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

    def register_logical_type_factory(
        self,
        category: str,
        factory: LogicalTypeFactory,
    ) -> None:
        """Register a factory for the given metadata category string.

        When ``prepare_extension_type`` encounters an Arrow extension type whose
        ``extension_metadata`` JSON contains ``{"category": "<category>", ...}``,
        it calls ``factory.create_logical_type(arrow_extension_name, storage_type,
        metadata_dict)`` to construct the logical type and then registers it.

        Args:
            category: The ``"category"`` value from the extension metadata JSON that
                identifies this category (e.g. ``"Dataclass"``).
            factory: A ``LogicalTypeFactory`` instance responsible for constructing
                logical types for this category.

        Raises:
            ValueError: If ``category`` is already registered to a different factory.
        """
        existing = self._factories.get(category)
        if existing is not None and existing is not factory:
            raise ValueError(
                f"Cannot register factory for category {category!r}: "
                f"a different factory is already registered for this category."
            )
        if existing is factory:
            return
        self._factories[category] = factory
        logger.debug(
            "registered LogicalTypeFactory for category %r: %r", category, factory
        )

    def prepare_extension_type(
        self,
        arrow_extension_name: str,
        extension_metadata: bytes | None,
        storage_type: pa.DataType,
    ) -> None:
        """Ensure the Arrow extension type identified by ``arrow_extension_name``
        is registered as a ``LogicalType``.

        This is the single entry point called by ``ensure_extensions_registered``
        in ``database_hooks``. The registry owns all dispatch logic.

        Args:
            arrow_extension_name: Arrow extension type name (``ARROW:extension:name``).
            extension_metadata: Raw metadata bytes (``ARROW:extension:metadata``),
                expected to be UTF-8 JSON containing at least a ``"category"`` key.
                ``None`` if absent.
            storage_type: Underlying Arrow storage type for this extension field.

        Raises:
            ValueError: If ``extension_metadata`` is ``None`` and the type is not
                already registered.
            ValueError: If ``extension_metadata`` is not valid UTF-8 JSON.
            ValueError: If the parsed JSON has no ``"category"`` key.
            ValueError: If no factory is registered for the ``"category"`` value.
            ValueError: Propagated from the factory if it cannot construct a type.
        """
        # Step 1: per-process cache hit — no-op regardless of metadata content.
        if self.get_by_arrow_extension_name(arrow_extension_name) is not None:
            logger.debug(
                "prepare_extension_type: %r already registered, skipping",
                arrow_extension_name,
            )
            return

        # Step 2: None metadata — cannot auto-register; must be pre-registered.
        if extension_metadata is None:
            raise ValueError(
                f"Extension type {arrow_extension_name!r} has no extension metadata "
                f"(metadata is None).\n"
                f"Types without a metadata category tag cannot be auto-registered via "
                f"a factory — they must be pre-registered explicitly via "
                f"registry.register(logical_type) on the registry instance used for reads."
            )

        # Step 3: Parse JSON.
        try:
            metadata_dict = json.loads(extension_metadata.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError(
                f"Extension type {arrow_extension_name!r} has extension metadata that "
                f"is not valid UTF-8 JSON: {extension_metadata!r}. "
                f"Parse error: {exc}.\n"
                f'Extension metadata must be a JSON object with at least a "category" '
                f'key, e.g. {{"category": "Dataclass"}}.'
            ) from exc

        # Guard: JSON must decode to a dict (object), not a list, scalar, etc.
        if not isinstance(metadata_dict, dict):
            raise ValueError(
                f"Extension type {arrow_extension_name!r} has extension metadata that "
                f"decoded to a non-object JSON value: {metadata_dict!r}. "
                f'Extension metadata must be a JSON object with at least a "category" '
                f'key, e.g. {{"category": "Dataclass"}}.'
            )

        # Step 4: Require "category" key.
        if "category" not in metadata_dict:
            raise ValueError(
                f"Extension type {arrow_extension_name!r} has extension metadata JSON "
                f'with no "category" key: {metadata_dict}. Extension metadata must be '
                f'a JSON object with at least a "category" key, e.g. '
                f'{{"category": "Dataclass"}}.'
            )

        category = metadata_dict["category"]

        # Guard: "category" value must be a string (used as dict key for factory lookup).
        if not isinstance(category, str):
            raise ValueError(
                f"Extension type {arrow_extension_name!r} has extension metadata JSON "
                f'where "category" is not a string: {category!r}. '
                f'The "category" value must be a plain string, e.g. "Dataclass".'
            )

        # Step 5: Look up factory.
        factory = self._factories.get(category)
        if factory is None:
            raise ValueError(
                f"No LogicalTypeFactory is registered for category {category!r}.\n"
                f"Cannot prepare extension type {arrow_extension_name!r} for "
                f"registration.\n"
                f"Register a factory on the registry instance used for reads via "
                f"register_logical_type_factory({category!r}, factory)."
            )

        # Step 6: Construct logical type via factory.
        logger.debug(
            "prepare_extension_type: %r not registered — dispatching to category %r factory",
            arrow_extension_name,
            category,
        )
        logical_type = factory.create_logical_type(
            arrow_extension_name, storage_type, metadata_dict
        )

        # Step 7: Register in all three bindings + PA/Polars global registries.
        self.register(logical_type)
        logger.debug(
            "prepare_extension_type: successfully registered %r via factory for category %r",
            arrow_extension_name,
            category,
        )


# Module-level singleton — per-process registry used by database_hooks and
# application code. Defined here (not in __init__.py) to avoid the circular
# import that would arise if database_hooks imported from the package __init__.
default_logical_type_registry = LogicalTypeRegistry()
