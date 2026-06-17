"""DataclassLogicalType and DataclassLogicalTypeFactory.

Provides the ``DataclassLogicalType`` logical type implementation and the
``DataclassLogicalTypeFactory`` that synthesises and reconstructs ``DataclassLogicalType``
instances for Python dataclasses.

Write path (``create_for_python_type``):
    Iterates dataclass fields, delegates field Arrow-type resolution to the converter
    via ``register_python_class``, and returns a ``DataclassLogicalType`` backed by
    a ``pa.struct`` extension type.

Read path (``reconstruct_from_arrow``):
    Imports the dataclass by fully-qualified class name, resolves field annotations
    against the (already bottom-up resolved) storage type, and returns a
    ``DataclassLogicalType``.

Category tag: ``"orcapod.dataclass"``
"""

from __future__ import annotations

import dataclasses
import importlib
import json
import logging
from typing import TYPE_CHECKING, Any

from orcapod.extension_types.registry import make_arrow_extension_type, make_polars_extension_type
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import polars as pl
    import pyarrow as pa
    from orcapod.extension_types.protocols import TypeConverterProtocol
else:
    pa = LazyModule("pyarrow")
    pl = LazyModule("polars")

logger = logging.getLogger(__name__)

#: Category tag embedded in Arrow extension metadata. Used as the factory dispatch key.
DATACLASS_CATEGORY = "orcapod.dataclass"


class DataclassLogicalType:
    """Logical type binding a Python dataclass to its Arrow extension type representation.

    Stores the dataclass's fully-qualified class name as the Arrow extension name
    and a ``pa.struct`` of the dataclass fields as the storage type.

    No Arrow-type reasoning lives here — all field-type resolution is owned by the
    converter and completed before this object is constructed.

    Args:
        logical_name: Fully-qualified class name (e.g. ``"mymodule.sub.MyData"``).
            Used as both the logical type name and the Arrow extension name.
        python_type: The Python dataclass ``type`` object.
        storage_type: The Arrow ``pa.StructType`` for the dataclass fields.
        field_annotations: Ordered list of ``(field_name, python_annotation)`` pairs
            matching the fields in ``storage_type``.

    Example:
        >>> lt = DataclassLogicalType(
        ...     "mymod.Point", Point,
        ...     pa.struct([pa.field("x", pa.int64()), pa.field("y", pa.int64())]),
        ...     [("x", int), ("y", int)],
        ... )
        >>> lt.python_to_storage(Point(1, 2), converter)
        {"x": 1, "y": 2}
    """

    def __init__(
        self,
        logical_name: str,
        python_type: type,
        storage_type: pa.StructType,
        field_annotations: list[tuple[str, Any]],
    ) -> None:
        self._logical_name = logical_name
        self._python_type = python_type
        self._storage_type = storage_type
        self._field_annotations = field_annotations

        _metadata = json.dumps({"category": DATACLASS_CATEGORY}).encode("utf-8")
        self._arrow_ext_class = make_arrow_extension_type(
            logical_name, storage_type, metadata=_metadata
        )
        self._arrow_ext: pa.ExtensionType | None = None
        # ``storage_type`` must not contain nested extension types (ET1 in DESIGN_ISSUES.md).
        # ``DataclassLogicalTypeFactory.create_for_python_type`` and ``reconstruct_from_arrow``
        # both guarantee this by stripping any top-level extension type from each field's
        # Arrow type before inserting it into the struct.
        self._polars_ext_class = make_polars_extension_type(logical_name, storage_type)
        self._polars_ext: pl.BaseExtension | None = None

    @property
    def logical_type_name(self) -> str:
        """Fully-qualified class name used as the logical type identifier."""
        return self._logical_name

    @property
    def python_type(self) -> type:
        """The Python dataclass type this logical type represents."""
        return self._python_type

    def get_arrow_extension_type(self) -> pa.ExtensionType:
        """Return the Arrow extension type for this dataclass.

        Returns:
            A cached ``pa.ExtensionType`` instance with ``extension_name`` equal to
            the fully-qualified class name and ``storage_type`` equal to the struct
            of the dataclass fields.
        """
        if self._arrow_ext is None:
            self._arrow_ext = self._arrow_ext_class()
        return self._arrow_ext

    def get_polars_extension_type(self) -> pl.BaseExtension:
        """Return the Polars extension type for this dataclass.

        Returns:
            A cached ``pl.BaseExtension`` instance.
        """
        if self._polars_ext is None:
            self._polars_ext = self._polars_ext_class()
        return self._polars_ext

    def python_to_storage(self, value: Any, converter: TypeConverterProtocol | None) -> dict[str, Any]:
        """Convert a dataclass instance to an Arrow-compatible struct dict.

        Iterates ``_field_annotations`` and delegates each field's conversion to
        ``converter.python_to_storage``.

        Args:
            value: A dataclass instance of type ``python_type``.
            converter: The active converter for per-field delegation. Must not be ``None``.

        Returns:
            A dict mapping field names to their Arrow storage values.

        Raises:
            ValueError: If ``converter`` is ``None``.
        """
        if converter is None:
            raise ValueError(
                "DataclassLogicalType.python_to_storage requires a converter — "
                "pass a TypeConverterProtocol instance for field-level conversion."
            )
        return {
            name: converter.python_to_storage(getattr(value, name), annotation)
            for name, annotation in self._field_annotations
        }

    def storage_to_python(self, storage_value: Any, converter: TypeConverterProtocol | None) -> Any:
        """Reconstruct a dataclass instance from an Arrow struct dict.

        Args:
            storage_value: A dict mapping field names to Arrow storage values.
            converter: The active converter for per-field delegation. Must not be ``None``.

        Returns:
            A dataclass instance of type ``python_type``.

        Raises:
            ValueError: If ``converter`` is ``None``.
        """
        if converter is None:
            raise ValueError(
                "DataclassLogicalType.storage_to_python requires a converter — "
                "pass a TypeConverterProtocol instance for field-level conversion."
            )
        kwargs = {
            name: converter.storage_to_python(storage_value[name], annotation)
            for name, annotation in self._field_annotations
        }
        return self._python_type(**kwargs)


class DataclassLogicalTypeFactory:
    """Stateless factory that synthesises and reconstructs ``DataclassLogicalType`` instances.

    **Write path** (``create_for_python_type``): derives Arrow struct type from the
    dataclass fields by delegating to ``converter.register_python_class`` per field.

    **Read path** (``reconstruct_from_arrow``): imports the dataclass by FQCN, matches
    fields against the already-resolved ``storage_type``, and returns a
    ``DataclassLogicalType``.

    Category tag: ``"orcapod.dataclass"``

    Register with::

        converter.register_logical_type_factory(
            DataclassLogicalTypeFactory(),
            category="orcapod.dataclass",
            python_bases=[object],
        )

    Example:
        >>> factory = DataclassLogicalTypeFactory()
        >>> factory.supports_class(MyDataclass)
        True
        >>> factory.supports_class(str)
        False
    """

    def supports_class(self, python_type: type) -> bool:
        """Return True if ``python_type`` is a dataclass.

        Args:
            python_type: Any Python type.

        Returns:
            True if ``dataclasses.is_dataclass(python_type)`` is True.
        """
        return dataclasses.is_dataclass(python_type) and isinstance(python_type, type)

    def create_for_python_type(
        self,
        python_type: type,
        converter: TypeConverterProtocol,
    ) -> DataclassLogicalType:
        """Synthesise a ``DataclassLogicalType`` for a Python dataclass (write path).

        Derives the FQCN, obtains type hints, and resolves each field's Arrow type
        via ``converter.register_python_class``. Rejects local / unnamed classes.

        Args:
            python_type: A Python dataclass type.
            converter: The active converter for field-type resolution.

        Returns:
            A ``DataclassLogicalType`` ready for registration.

        Raises:
            ValueError: If ``python_type`` is a local class (``__qualname__`` contains
                ``"<locals>"``).
        """
        import typing

        fqcn = f"{python_type.__module__}.{python_type.__qualname__}"
        if "<locals>" in fqcn:
            raise ValueError(
                f"Cannot register local class {python_type!r} as a DataclassLogicalType — "
                f"local classes have no stable fully-qualified class name and cannot be "
                f"reconstructed on read. Define the dataclass at module level."
            )

        try:
            hints = typing.get_type_hints(python_type)
        except Exception as exc:
            raise ValueError(
                f"Cannot get type hints for {python_type!r}: {exc}"
            ) from exc

        arrow_fields = []
        field_annotations = []
        for field in dataclasses.fields(python_type):
            if not field.init:
                continue
            annotation = hints.get(field.name, Any)
            arrow_type = converter.register_python_class(annotation)
            # register_python_class returns a storage-safe type: may be extension at the
            # top level, but struct fields are always plain. Strip the top-level extension
            # type here before inserting into the struct (ET1; see DESIGN_ISSUES.md).
            if isinstance(arrow_type, pa.ExtensionType):
                arrow_type = arrow_type.storage_type
            arrow_fields.append(pa.field(field.name, arrow_type))
            field_annotations.append((field.name, annotation))

        storage_type = pa.struct(arrow_fields)
        logger.debug("DataclassLogicalTypeFactory: synthesised %r for %r", fqcn, python_type)
        return DataclassLogicalType(fqcn, python_type, storage_type, field_annotations)

    def reconstruct_from_arrow(
        self,
        arrow_extension_name: str,
        storage_type: pa.DataType,
        metadata: dict[str, Any],
        converter: TypeConverterProtocol,
    ) -> DataclassLogicalType:
        """Reconstruct a ``DataclassLogicalType`` from Arrow schema metadata (read path).

        Imports the dataclass from its FQCN (``arrow_extension_name``), then matches
        the dataclass field annotations against the fields in ``storage_type``.
        ``storage_type`` is already bottom-up resolved by ``register_storage_type``
        before this method is called.

        Args:
            arrow_extension_name: FQCN of the dataclass (Arrow extension name).
            storage_type: Already-resolved ``pa.StructType`` for the dataclass fields.
            metadata: Full parsed metadata JSON dict (always contains ``"category"``).
            converter: The active converter (not needed here but required by protocol).

        Returns:
            A ``DataclassLogicalType`` ready for registration.

        Raises:
            ImportError: If the class cannot be imported from ``arrow_extension_name``.
            ValueError: If ``storage_type`` is not a struct type.
        """
        import typing

        if not pa.types.is_struct(storage_type):
            raise ValueError(
                f"DataclassLogicalTypeFactory.reconstruct_from_arrow: expected a struct "
                f"storage type for {arrow_extension_name!r}, got {storage_type!r}."
            )

        # Import class from FQCN using longest-prefix module walk
        cls = _import_from_fqcn(arrow_extension_name)

        try:
            hints = typing.get_type_hints(cls)
        except Exception as exc:
            raise ValueError(
                f"Cannot get type hints for {cls!r}: {exc}"
            ) from exc

        field_annotations = []
        for field in dataclasses.fields(cls):
            if not field.init:
                continue
            annotation = hints.get(field.name, Any)
            # Register any logical type the field annotation maps to (registration
            # completeness invariant: all nested logical types must be registered when
            # the outer type is registered). The return value is discarded; only the
            # side effect of registration matters here.
            converter.register_python_class(annotation)
            field_annotations.append((field.name, annotation))

        logger.debug(
            "DataclassLogicalTypeFactory: reconstructed %r from Arrow", arrow_extension_name
        )
        return DataclassLogicalType(
            arrow_extension_name, cls, storage_type, field_annotations
        )


def _import_from_fqcn(fqcn: str) -> type:
    """Import a class from its fully-qualified class name.

    Tries module prefixes from longest to shortest, then walks the remaining
    parts as attribute access. For example:

    - ``"mypackage.sub.MyClass"`` → import ``mypackage.sub``, then
      ``getattr(module, "MyClass")``.
    - ``"mypackage.sub.Outer.Inner"`` → import ``mypackage.sub``, then
      ``getattr(module, "Outer")``, then ``getattr(Outer, "Inner")``.

    Args:
        fqcn: Fully-qualified class name, e.g. ``"mypackage.sub.MyClass"``.

    Returns:
        The imported dataclass type.

    Raises:
        ImportError: If no valid module+attribute split can be found, or if the
            resolved object is not a dataclass type.
    """
    parts = fqcn.split(".")
    if len(parts) < 2:
        raise ImportError(f"Cannot import from FQCN {fqcn!r}: no module separator found.")

    # Try module paths from longest to shortest prefix
    for i in range(len(parts) - 1, 0, -1):
        module_path = ".".join(parts[:i])
        attr_parts = parts[i:]
        try:
            module = importlib.import_module(module_path)
        except (ImportError, ModuleNotFoundError):
            continue
        # Walk the remaining attribute chain (handles nested classes)
        obj: Any = module
        try:
            for attr in attr_parts:
                obj = getattr(obj, attr)
        except AttributeError:
            continue
        if not dataclasses.is_dataclass(obj) or not isinstance(obj, type):
            raise ImportError(
                f"{'.'.join(attr_parts)!r} in {module_path!r} is not a dataclass type."
            )
        return obj

    raise ImportError(
        f"Cannot import dataclass from FQCN {fqcn!r}: no valid module+attribute path found."
    )
