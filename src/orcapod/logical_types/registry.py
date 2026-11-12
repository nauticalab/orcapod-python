"""Registry for LogicalType instances.

Registering a logical type automatically registers the corresponding
extension type in both PyArrow's and Polars' global registries.
"""

from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, Iterable

from orcapod.logical_types.identifier import LogicalTypeIdentifierError
from orcapod.logical_types.identifier import LogicalTypeIdentifier
from orcapod.logical_types.protocols import (
    LogicalTypeProtocol,
    LogicalTypeFactoryProtocol,
)
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
    also_reads: tuple[bytes, ...] = (),
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

            ``metadata`` can optionally encode a **LogicalTypeProtocol category** as a
            UTF-8 JSON object with at least a ``"category"`` key
            (e.g. ``b'{"category": "Dataclass"}'``,
            ``b'{"category": "Pydantic", "pydantic_version": 2}'``).
            A ``LogicalTypeFactoryProtocol`` (see
            ``LogicalTypeFactoryProtocol.reconstruct_from_arrow``) dispatches on the
            ``"category"`` value when reading schemas from IPC or Parquet files and
            uses it to auto-generate the correct ``LogicalTypeProtocol`` implementation
            for the specific Python class within that category, without requiring
            explicit prior registration.
        also_reads: Further stored metadata values the type can read, beside
            ``metadata``: identifiers of older versions whose stored layout this
            version still decodes. Data stored under one of them is given the
            current type on read. The returned class exposes the full tuple as
            ``readable_metadata``, which ``BaseLogicalType`` uses as the default
            for ``readable_extension_metadata``.

    Returns:
        A ``pa.ExtensionType`` subclass. Call it with no arguments to obtain
        an instance suitable for passing to ``pa.register_extension_type`` or
        returning from ``get_arrow_extension_type()``.
    """
    _name, _storage, _metadata = extension_name, storage_type, metadata or b""
    _readable = (_metadata, *(m for m in also_reads if m != _metadata))

    def _deserialize(
        cls, storage_type: pa.DataType, serialized: bytes
    ) -> pa.ExtensionType:
        # __arrow_ext_deserialize__ reconstructs the type descriptor from schema
        # metadata (called once per IPC/Parquet read, not per value). Validate the
        # incoming storage_type and serialized bytes against the expected values so
        # that reading a file where the same extension name was written with different
        # parameters raises immediately rather than silently producing wrong data.
        #
        # Storage-family tolerance (ITL-602): when _storage is the canonical large
        # variant (large_string or large_binary), accept any physically-equivalent
        # member of the same family (string/string_view → large_string,
        # binary/binary_view → large_binary).  Delta Lake compaction tools (e.g.
        # delta-rs optimize.compact()) normalise large_string columns to plain
        # string in rewritten Parquet files.  Returning cls() (whose storage is
        # _storage) causes PyArrow to widen the physical data to the canonical
        # large variant automatically — verified empirically with pyarrow 25.0.0.
        #
        # When _storage is NOT the canonical large variant, strict equality is
        # required: (a) an extension registered with plain string/binary must still
        # read its own data, and (b) large_* → non-large narrowing is never
        # silently accepted (it could silently overflow for values > 2 GB).
        #
        # Cross-family mismatches (string where large_binary was registered, or
        # vice-versa) are genuine semantic errors and always raise.
        from orcapod.utils.arrow_utils import normalize_to_large_types

        if normalize_to_large_types(_storage, keep_null=True) == _storage:
            # _storage is the canonical large form — accept any same-family variant,
            # at any nesting depth (a struct field rewritten from large_string to
            # string by Delta compaction, ITL-881).
            storage_ok = (
                normalize_to_large_types(storage_type, keep_null=True) == _storage
            )
        else:
            # Non-canonical _storage — require exact match.
            storage_ok = storage_type == _storage
        from orcapod.errors import TypeIdentityError

        if not storage_ok:
            raise TypeIdentityError(
                f"Arrow extension type '{_name}': expected storage_type "
                f"{_storage!r} but got {storage_type!r}."
            )
        if serialized not in _readable:
            raise LogicalTypeIdentifierError(
                LogicalTypeIdentifier(_name, serialized), _readable
            )
        return cls()

    return type(
        f"_ArrowExt_{_sanitize(extension_name)}",
        (pa.ExtensionType,),
        {
            "__init__": lambda self: pa.ExtensionType.__init__(self, _storage, _name),
            "__arrow_ext_serialize__": lambda self: _metadata,
            "__arrow_ext_deserialize__": classmethod(_deserialize),
            "readable_metadata": _readable,
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

    **Limitation — nested extension types not supported:** ``arrow_storage_type``
    must not contain any ``pa.ExtensionType`` nodes (e.g. as struct fields or
    list element types). Polars's Arrow IPC bridge can handle a top-level
    extension type via ``pl.BaseExtension``, but raises
    ``ArrowNotImplementedError: extension`` when it encounters an extension type
    nested inside a struct or list during dtype inference. Callers must ensure
    ``arrow_storage_type`` is storage-safe (no nested extension type nodes) before
    passing it here. Types produced by ``register_python_class`` and
    ``register_storage_type`` satisfy this invariant, but arbitrary
    ``pa.DataType`` values do not. This is tracked as design issue ET1 in
    ``design_issues/``.

    Args:
        extension_name: The extension type name used for Polars registration.
            Must match the Arrow extension name so Polars can round-trip the
            type through Arrow IPC.
        arrow_storage_type: The Arrow storage type. Must not contain nested
            ``pa.ExtensionType`` nodes; see limitation note above. Converted
            once to the corresponding Polars dtype via ``pl.from_arrow``.
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


def _same_arrow_extension(a: pa.ExtensionType, b: pa.ExtensionType) -> bool:
    """Return True if two extension types have the same name, storage and metadata."""
    return (
        a.extension_name == b.extension_name
        and a.storage_type == b.storage_type
        and a.__arrow_ext_serialize__() == b.__arrow_ext_serialize__()
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
    pre-register one or more ``LogicalTypeProtocol`` instances immediately, following
    the same pattern as the ``logical_types`` constructor argument used by
    other registries in this package.

    An optional ``factories`` list can also be passed to pre-register
    ``LogicalTypeFactoryProtocol`` instances at construction time. Each entry is a
    dict with keys ``factory`` (the factory instance), ``category`` (optional str),
    and ``python_bases`` (optional list of types).

    Example:
        >>> registry = LogicalTypeRegistry()
        >>> registry.register_logical_type(my_logical_type)
        >>> lt = registry.get_by_logical_name("orcapod.uuid")

        >>> # Pre-register types at construction:
        >>> registry = LogicalTypeRegistry(logical_types=[path_lt, uuid_lt])
    """

    def __init__(
        self,
        logical_types: list[LogicalTypeProtocol] | None = None,
        factories: list[dict] | None = None,
    ) -> None:
        self._by_logical_name: dict[str, LogicalTypeProtocol] = {}
        self._by_arrow_name: dict[str, LogicalTypeProtocol] = {}
        self._by_python_type: dict[type, LogicalTypeProtocol] = {}
        self._category_factories: dict[str, LogicalTypeFactoryProtocol] = {}
        self._python_class_factories: dict[type, LogicalTypeFactoryProtocol] = {}
        for lt in logical_types or []:
            self.register_logical_type(lt)
        for entry in factories or []:
            self.register_logical_type_factory(
                entry["factory"],
                category=entry.get("category"),
                python_bases=entry.get("python_bases", []),
            )

    def register_logical_type(self, logical_type: LogicalTypeProtocol) -> None:
        """Register *logical_type* and its PyArrow/Polars extension types.

        Args:
            logical_type: A ``LogicalTypeProtocol`` instance to register.

        Raises:
            ValueError: If any of the three keys (``logical_type_name``,
                Arrow extension name, ``python_type``) is already bound to a
                *different* ``LogicalTypeProtocol`` in this registry, or if the
                Arrow extension name contains ``;`` (the separator between name
                and metadata in the identifier text that is hashed).
        """
        arrow_ext = logical_type.get_arrow_extension_type()
        arrow_ext_name = arrow_ext.extension_name
        if ";" in arrow_ext_name:
            raise ValueError(
                f"Cannot register logical type with Arrow extension name "
                f"{arrow_ext_name!r}: an extension name must not contain ';'."
            )
        py_type = logical_type.python_type
        logical_name = logical_type.logical_type_name

        existing_by_logical = self._by_logical_name.get(logical_name)
        existing_by_arrow = self._by_arrow_name.get(arrow_ext_name)
        existing_by_python = self._by_python_type.get(py_type)

        if (
            existing_by_python is None
            and existing_by_logical is not None
            and existing_by_logical is existing_by_arrow
            and existing_by_logical is not logical_type
            and getattr(logical_type, "exact_python_type_only", False)
            and _same_arrow_extension(
                existing_by_logical.get_arrow_extension_type(), arrow_ext
            )
        ):
            self._rebind(existing_by_logical, logical_type)
            return

        # Triplet conflict check: raise if any key is bound to a different instance.
        for existing, label, key in [
            (existing_by_logical, "logical_type_name", logical_name),
            (existing_by_arrow, "arrow_extension_name", arrow_ext_name),
            (
                existing_by_python,
                "python_type",
                getattr(py_type, "__qualname__", repr(py_type)),
            ),
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

    def _rebind(
        self, previous: LogicalTypeProtocol, logical_type: LogicalTypeProtocol
    ) -> None:
        """Rebind an identity to a logical type built for a new class object.

        Used when a user class with an unchanged identity is defined again (a notebook
        cell re-run, a module reload, a second structural class with the same name and
        fields). It makes the newest class the one stored values are read as; it does
        not make two exact classes one type. The logical-name and Arrow-name bindings move to
        the new logical type; the previous class object keeps its binding, so instances
        created before the re-definition still convert. Arrow's and Polars' global
        registrations are reused: name, storage type and metadata are identical.

        Args:
            previous: The logical type currently bound to the identity.
            logical_type: The logical type for the new class object.
        """
        self._move_bindings(previous, logical_type)

    def _move_bindings(
        self, previous: LogicalTypeProtocol, logical_type: LogicalTypeProtocol
    ) -> None:
        """Bind an identity's names to ``logical_type`` and rebuild its containers.

        The logical-name and Arrow-name bindings move to ``logical_type`` and its
        Python type is bound to it; the previous Python type keeps its binding. Each
        registered list or set logical type whose element is ``previous`` is rebuilt
        over ``logical_type`` and rebound the same way, at any nesting depth, so
        ``list[Point]`` names the class ``Point`` is now bound to.

        Args:
            previous: The logical type the names were bound to.
            logical_type: The logical type to bind them to, with the same names.
        """
        containers = [
            registered
            for registered in self._by_logical_name.values()
            if getattr(registered, "element_logical_type", None) is previous
        ]
        name = logical_type.logical_type_name
        self._by_logical_name[name] = logical_type
        self._by_arrow_name[logical_type.get_arrow_extension_type().extension_name] = (
            logical_type
        )
        self._by_python_type[logical_type.python_type] = logical_type
        for container in containers:
            with_element = getattr(container, "with_element", None)
            if with_element is not None:
                self._move_bindings(container, with_element(logical_type))

    def get_by_logical_name(self, name: str) -> LogicalTypeProtocol | None:
        """Return the logical type registered under *name*, or ``None``."""
        return self._by_logical_name.get(name)

    def get_by_python_type(self, python_type: type) -> LogicalTypeProtocol | None:
        """Return the logical type for *python_type*, or ``None``.

        The most specific registered type wins: *python_type* itself, then each
        class of its MRO in order, so a subclass registered with its own logical
        type is used for itself and its subclasses whatever the registration
        order (an ``np.ndarray`` subclass without one is an array). A logical type
        with ``exact_python_type_only`` (a dataclass or pydantic model) is never
        returned for a subclass. Types that are
        subclasses only through ``__subclasshook__``/ABC registration fall back
        to an ``issubclass`` scan, where the first registered match wins.
        """
        result = self._by_python_type.get(python_type)
        if result is not None:
            return result
        for base in getattr(python_type, "__mro__", ())[1:]:
            result = self._by_python_type.get(base)
            # A user class matches exactly: its subclass is a different class with
            # its own fields and identity, never stored as the base (ITL-881).
            if result is not None and not getattr(
                result, "exact_python_type_only", False
            ):
                return result
        for registered_type, registered_lt in self._by_python_type.items():
            if getattr(registered_lt, "exact_python_type_only", False):
                continue
            try:
                if issubclass(python_type, registered_type):
                    return registered_lt
            except TypeError:
                continue
        return None

    def get_by_arrow_extension_name(
        self, arrow_name: str
    ) -> LogicalTypeProtocol | None:
        """Return the logical type registered under *arrow_name*, or ``None``."""
        return self._by_arrow_name.get(arrow_name)

    def register_logical_type_factory(
        self,
        factory: LogicalTypeFactoryProtocol,
        *,
        category: str | None = None,
        python_bases: Iterable[type] = (),
    ) -> None:
        """Register a factory on one or both dispatch axes.

        A single factory instance can be registered for multiple ``python_bases``
        at once — pass a list with all the base classes it should handle.

        Args:
            factory: The factory to register.
            category: If given, registers factory as the read-side handler for Arrow
                extension types whose metadata contains this category string. Raises
                ``ValueError`` if a different factory is already registered for this
                category.
            python_bases: Zero or more Python base classes. Registers factory as the
                write-side handler for each base. A single factory may cover any
                number of bases. Raises ``ValueError`` if a *different* factory is
                already registered for a given base.

        Raises:
            ValueError: If neither ``category`` nor ``python_bases`` is provided.
            ValueError: If a different factory is already registered for a given key.
        """
        python_bases_list = list(python_bases)
        if category is None and not python_bases_list:
            raise ValueError(
                "At least one of 'category' or 'python_bases' must be provided."
            )
        if category is not None:
            existing = self._category_factories.get(category)
            if existing is not None and existing is not factory:
                raise ValueError(
                    f"Cannot register factory for category {category!r}: "
                    f"a different factory is already registered for this category."
                )
            # Skip registration if this exact factory object is already bound to the category.
            if existing is not factory:
                self._category_factories[category] = factory
                logger.debug(
                    "registered LogicalTypeFactory for category %r: %r",
                    category,
                    factory,
                )
        # Validate all bases before writing any (prevents partial mutation on error).
        for base in python_bases_list:
            existing = self._python_class_factories.get(base)
            if existing is not None and existing is not factory:
                raise ValueError(
                    f"Cannot register factory for python base {base!r}: "
                    f"a different factory is already registered for this base."
                )
        for base in python_bases_list:
            # Skip if this exact factory object is already bound to the base class
            # (idempotent re-registration of the same factory is always a no-op).
            if self._python_class_factories.get(base) is not factory:
                self._python_class_factories[base] = factory
                logger.debug(
                    "registered LogicalTypeFactory for python base %r: %r",
                    base,
                    factory,
                )
