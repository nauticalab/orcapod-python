"""Handler factory for Python dataclasses.

Implements ``DataclassHandlerFactory`` — a ``LogicalTypeFactoryProtocol`` that
constructs ``DataclassLogicalType`` instances for any Python dataclass on both
the write path (annotation-driven) and read path (Arrow schema metadata).

Registration example::

    factory = DataclassHandlerFactory()
    registry.register_logical_type_factory(
        factory,
        category="orcapod.dataclass",
        python_bases=[object],
    )

Note:
    Nested dataclasses are stored as plain Arrow sub-structs (not extension
    types). Only the outermost column is self-describing via the extension
    type metadata. Supporting nested extension types inside struct sub-fields
    is tracked as a v0.2 issue (PLT-1700).

    Registered logical types (e.g. ``pathlib.Path``, ``uuid.UUID``) used as
    dataclass field annotations are not supported in this version and will
    raise ``TypeError``. A follow-up issue will add registry-lookup support.
"""

from __future__ import annotations

import dataclasses
import functools
import importlib
from typing import TYPE_CHECKING, Any, Callable

from orcapod.extension_types.protocols import ResolutionContext
from orcapod.extension_types.registry import make_arrow_extension_type, make_polars_extension_type
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import polars as pl
    import pyarrow as pa
    from orcapod.extension_types.protocols import LogicalTypeProtocol
    from orcapod.extension_types.registry import LogicalTypeRegistry
else:
    pa = LazyModule("pyarrow")
    pl = LazyModule("polars")

_DATACLASS_CATEGORY_METADATA = b'{"category": "orcapod.dataclass"}'


@functools.lru_cache(maxsize=1)
def _primitive_arrow_map() -> dict[type, Any]:
    """Return the primitive-type → Arrow-type lookup table (built once, cached)."""
    return {
        int: pa.int64(),
        float: pa.float64(),
        str: pa.large_string(),
        bool: pa.bool_(),
        bytes: pa.large_binary(),
    }


class DataclassLogicalType:
    """Concrete ``LogicalTypeProtocol`` for a specific Python dataclass.

    Constructed once by ``DataclassHandlerFactory``; holds no registry reference —
    all conversion logic is baked in at construction time via pre-built field
    converters.

    Args:
        logical_name: Fully qualified class name (e.g. ``"my.module.Data1"``).
        python_type: The dataclass class.
        storage_type: ``pa.struct([...])`` describing the Arrow layout.
        field_converters: Ordered list of ``(field_name, to_storage_fn,
            from_storage_fn)`` tuples. Primitive fields use identity functions;
            nested dataclass fields use their own ``python_to_storage`` /
            ``storage_to_python`` methods; ``list[T]`` fields use element-wise
            converters.
    """

    def __init__(
        self,
        logical_name: str,
        python_type: type,
        storage_type: pa.DataType,
        field_converters: list[tuple[str, Callable[..., Any], Callable[..., Any]]],
    ) -> None:
        self._logical_name = logical_name
        self._python_type = python_type
        self._storage_type = storage_type
        self._field_converters = field_converters
        self._arrow_ext_class = make_arrow_extension_type(
            logical_name, storage_type, _DATACLASS_CATEGORY_METADATA
        )
        self._polars_ext_class = make_polars_extension_type(logical_name, storage_type)
        self._arrow_ext: pa.ExtensionType | None = None
        self._polars_ext: pl.BaseExtension | None = None

    @property
    def logical_type_name(self) -> str:
        """Fully qualified class name used as the Arrow extension name."""
        return self._logical_name

    @property
    def python_type(self) -> type:
        """The dataclass class this logical type represents."""
        return self._python_type

    def get_arrow_extension_type(self) -> pa.ExtensionType:
        """Return a cached Arrow extension type instance for this dataclass.

        Returns:
            A ``pa.ExtensionType`` with extension name equal to the FQCN and
            storage type ``pa.struct([...])``. Metadata bytes encode
            ``{"category": "orcapod.dataclass"}`` for read-path dispatch.
        """
        if self._arrow_ext is None:
            self._arrow_ext = self._arrow_ext_class()
        return self._arrow_ext

    def get_polars_extension_type(self) -> pl.BaseExtension:
        """Return a cached Polars extension type instance for this dataclass.

        Returns:
            A ``pl.BaseExtension`` registered under the FQCN.
        """
        if self._polars_ext is None:
            self._polars_ext = self._polars_ext_class()
        return self._polars_ext

    def python_to_storage(self, value: Any) -> dict[str, Any]:
        """Convert a dataclass instance to a Python dict for Arrow struct storage.

        Args:
            value: A Python instance of ``python_type``.

        Returns:
            A dict mapping field names to their storage-converted values.
        """
        return {
            name: to_fn(getattr(value, name))
            for name, to_fn, _ in self._field_converters
        }

    def storage_to_python(self, storage_value: Any) -> Any:
        """Reconstruct the dataclass from an Arrow struct ``.as_py()`` dict.

        Args:
            storage_value: A Python dict as returned by ``scalar.as_py()`` for
                a struct-storage Arrow scalar.

        Returns:
            A fully reconstructed instance of ``python_type``.
        """
        return self._python_type(**{
            name: from_fn(storage_value[name])
            for name, _, from_fn in self._field_converters
        })


class DataclassHandlerFactory:
    """Factory that synthesizes ``DataclassLogicalType`` instances for Python dataclasses.

    Stateless — holds no registry reference. Registers against ``object`` in the
    ``LogicalTypeRegistry`` write-side dispatch, with ``supports_class`` as the gate
    that confirms the Python type is actually a dataclass.

    Registration::

        factory = DataclassHandlerFactory()
        registry.register_logical_type_factory(
            factory,
            category="orcapod.dataclass",
            python_bases=[object],
        )
    """

    def supports_class(self, python_type: type) -> bool:
        """Return ``True`` if *python_type* is a Python dataclass.

        Called by the registry during the MRO walk after hitting ``object``.
        Not called on the read path.

        Args:
            python_type: The Python class to test.

        Returns:
            ``True`` if ``dataclasses.is_dataclass(python_type)`` is ``True``.
        """
        return dataclasses.is_dataclass(python_type)

    def _resolve_field(
        self,
        annotation: Any,
        registry: LogicalTypeRegistry | None,
        context: ResolutionContext,
    ) -> tuple[pa.DataType, Callable[[Any], Any], Callable[[Any], Any]]:
        """Resolve one field annotation to an Arrow type and a pair of converters.

        Args:
            annotation: The Python type annotation for a dataclass field.
            registry: Optional registry for side-effect registration of nested types.
            context: Current cycle-detection context (outer class already added).

        Returns:
            A ``(arrow_type, to_storage, from_storage)`` triple.

        Raises:
            TypeError: If the annotation is not a supported type.
        """
        import typing

        primitive_map = _primitive_arrow_map()

        # Primitive types
        if annotation in primitive_map:
            arrow_type = primitive_map[annotation]
            identity: Callable[[Any], Any] = lambda v: v
            return arrow_type, identity, identity

        # list[T]
        origin = typing.get_origin(annotation)
        if origin is list:
            args = typing.get_args(annotation)
            if not args:
                raise TypeError(
                    f"Unsupported field annotation: bare 'list' with no type argument. "
                    f"Use list[T] with a concrete element type."
                )
            elem_arrow, elem_to, elem_from = self._resolve_field(args[0], registry, context)
            arrow_type = pa.list_(elem_arrow)

            def to_storage_list(val: Any, _to: Callable[[Any], Any] = elem_to) -> list[Any]:
                return [_to(x) for x in val]

            def from_storage_list(val: Any, _from: Callable[[Any], Any] = elem_from) -> list[Any]:
                return [_from(x) for x in val]

            return arrow_type, to_storage_list, from_storage_list

        # Nested dataclass
        if isinstance(annotation, type) and dataclasses.is_dataclass(annotation):
            nested_lt = self.create_for_python_type(annotation, registry, context)
            if registry is not None:
                registry.register_logical_type(nested_lt)
            # Use raw struct storage type, NOT the extension type — nested structs
            # are plain sub-structs to avoid unsupported nested extension types (PLT-1700).
            nested_storage = nested_lt.get_arrow_extension_type().storage_type
            return nested_storage, nested_lt.python_to_storage, nested_lt.storage_to_python

        raise TypeError(
            f"Unsupported field type annotation: {annotation!r}. "
            f"Supported types: int, float, str, bool, bytes, list[T], and nested "
            f"dataclasses. Registered logical types (e.g. pathlib.Path, uuid.UUID) "
            f"as field types are not yet supported — see follow-up issue."
        )

    def create_for_python_type(
        self,
        python_type: type,
        registry: LogicalTypeRegistry | None = None,
        context: ResolutionContext = ResolutionContext(),
    ) -> DataclassLogicalType:
        """Synthesize a ``DataclassLogicalType`` for *python_type* (write path).

        Derives the Arrow struct layout and field converters from the class
        annotations. Registers nested dataclass types in *registry* as a side
        effect so they are available for subsequent lookups.

        Args:
            python_type: A Python dataclass class.
            registry: Optional registry; if provided, nested dataclass types are
                registered as a side effect.
            context: Cycle-detection context. Any class already in
                ``context.visited_types`` will trigger a ``TypeError``.

        Returns:
            A fully constructed ``DataclassLogicalType``.

        Raises:
            ValueError: If *python_type* is not a dataclass.
            TypeError: If a circular reference is detected via *context*.
            TypeError: If a field uses an unsupported annotation.
        """
        from typing import get_type_hints

        if not dataclasses.is_dataclass(python_type):
            raise ValueError(
                f"{python_type!r} is not a dataclass. "
                f"DataclassHandlerFactory only handles @dataclass-decorated classes."
            )

        if python_type in context.visited_types:
            raise TypeError(
                f"Circular reference detected: {python_type!r} is already being "
                f"resolved. Dataclass fields cannot form circular references because "
                f"Arrow struct storage sizes must be finite."
            )

        # Update context BEFORE resolving fields so nested calls see this type.
        context = dataclasses.replace(
            context,
            visited_types=context.visited_types | {python_type},
        )

        hints = get_type_hints(python_type)
        struct_fields: list[pa.Field] = []
        field_converters: list[tuple[str, Callable[[Any], Any], Callable[[Any], Any]]] = []

        for field in dataclasses.fields(python_type):
            if not field.init:
                continue
            annotation = hints[field.name]
            arrow_type, to_fn, from_fn = self._resolve_field(annotation, registry, context)
            struct_fields.append(pa.field(field.name, arrow_type))
            field_converters.append((field.name, to_fn, from_fn))

        storage_type = pa.struct(struct_fields)
        fqcn = f"{python_type.__module__}.{python_type.__qualname__}"
        return DataclassLogicalType(fqcn, python_type, storage_type, field_converters)

    def reconstruct_from_arrow(
        self,
        arrow_extension_name: str,
        storage_type: pa.DataType,
        metadata: dict[str, Any],
        registry: LogicalTypeRegistry | None = None,
        context: ResolutionContext = ResolutionContext(),
    ) -> DataclassLogicalType:
        """Reconstruct a ``DataclassLogicalType`` from Arrow schema metadata (read path).

        Imports the dataclass class by its FQCN (the Arrow extension name), then
        builds field converters from the class annotations. The *storage_type* from
        the schema is used as-is — it is not re-derived from the class.

        Args:
            arrow_extension_name: FQCN of the dataclass (e.g. ``"my.module.MyClass"``).
            storage_type: Arrow struct storage type from the schema.
            metadata: Full parsed metadata JSON dict (must contain ``"category"``).
            registry: Optional registry; nested types are registered as a side effect.
            context: Cycle-detection context.

        Returns:
            A fully constructed ``DataclassLogicalType``.

        Raises:
            ValueError: If the FQCN cannot be imported or is not a dataclass.
            ValueError: If a circular reference is detected via *context*.
        """
        from typing import get_type_hints

        if arrow_extension_name in context.visited_arrow_names:
            raise ValueError(
                f"Circular reference detected: {arrow_extension_name!r} is already "
                f"being resolved on the read path."
            )

        context = dataclasses.replace(
            context,
            visited_arrow_names=context.visited_arrow_names | {arrow_extension_name},
        )

        # Import the class by FQCN (split on last dot).
        last_dot = arrow_extension_name.rfind(".")
        if last_dot == -1:
            raise ValueError(
                f"Cannot import class from FQCN {arrow_extension_name!r}: "
                f"no module separator (dot) found. "
                f"Expected a fully qualified name such as 'my.module.MyClass'."
            )
        module_path = arrow_extension_name[:last_dot]
        class_name = arrow_extension_name[last_dot + 1:]

        try:
            module = importlib.import_module(module_path)
        except ImportError as exc:
            raise ValueError(
                f"Cannot import module {module_path!r} to reconstruct "
                f"{arrow_extension_name!r}: {exc}"
            ) from exc

        try:
            imported_class = getattr(module, class_name)
        except AttributeError as exc:
            raise ValueError(
                f"Cannot find class {class_name!r} in module {module_path!r} "
                f"to reconstruct {arrow_extension_name!r}: {exc}"
            ) from exc

        if not dataclasses.is_dataclass(imported_class):
            raise ValueError(
                f"Imported class {arrow_extension_name!r} is not a Python dataclass. "
                f"Only @dataclass-decorated classes can be reconstructed by "
                f"DataclassHandlerFactory."
            )

        hints = get_type_hints(imported_class)
        field_converters: list[tuple[str, Callable[[Any], Any], Callable[[Any], Any]]] = []

        # Build converters from annotations; use storage_type from schema as-is.
        # Pass the write-path context (visited_types) via a fresh ResolutionContext
        # so that nested type resolution also participates in cycle detection.
        write_context = ResolutionContext(visited_types=frozenset({imported_class}))

        for field in dataclasses.fields(imported_class):
            if not field.init:
                continue
            annotation = hints[field.name]
            _, to_fn, from_fn = self._resolve_field(annotation, registry, write_context)
            field_converters.append((field.name, to_fn, from_fn))

        return DataclassLogicalType(
            arrow_extension_name, imported_class, storage_type, field_converters
        )
