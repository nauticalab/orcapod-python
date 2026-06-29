"""PydanticLogicalType and PydanticLogicalTypeFactory.

Provides the ``PydanticLogicalType`` logical type implementation and the
``PydanticLogicalTypeFactory`` that synthesises and reconstructs
``PydanticLogicalType`` instances for pydantic v2 ``BaseModel`` subclasses.

Write path (``create_for_python_type``):
    Iterates model fields via ``model_fields`` (pydantic v2 API), delegates
    field Arrow-type resolution to the converter via ``register_python_class``,
    and returns a ``PydanticLogicalType`` backed by a ``pa.struct`` extension
    type.

Read path (``reconstruct_from_arrow``):
    Imports the model by fully-qualified class name, resolves field annotations
    against the (already bottom-up resolved) storage type, and returns a
    ``PydanticLogicalType``.

Category tag: ``"orcapod.pydantic"``
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any

from orcapod.extension_types.base_logical_type import BaseLogicalType
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
PYDANTIC_CATEGORY = "orcapod.pydantic"


class PydanticLogicalType(BaseLogicalType):
    """Logical type binding a pydantic ``BaseModel`` subclass to its Arrow extension type.

    Stores the model's fully-qualified class name as the Arrow extension name
    and a ``pa.struct`` of the model fields as the storage type.

    No Arrow-type reasoning lives here — all field-type resolution is owned by
    the converter and completed before this object is constructed.

    Args:
        logical_name: Fully-qualified class name (e.g. ``"mymodule.sub.MyModel"``).
            Used as both the logical type name and the Arrow extension name.
        python_type: The pydantic ``BaseModel`` subclass.
        storage_type: The Arrow ``pa.StructType`` for the model fields.
        field_annotations: Ordered list of ``(field_name, python_annotation)``
            pairs matching the fields in ``storage_type``.

    Example:
        >>> lt = PydanticLogicalType(
        ...     "mymod.Point", Point,
        ...     pa.struct([pa.field("x", pa.int64()), pa.field("y", pa.int64())]),
        ...     [("x", int), ("y", int)],
        ... )
        >>> lt.python_to_storage(Point(x=1, y=2), converter)
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

        _metadata = json.dumps({"category": PYDANTIC_CATEGORY}).encode("utf-8")
        self._arrow_ext_class = make_arrow_extension_type(
            logical_name, storage_type, metadata=_metadata
        )
        self._arrow_ext: pa.ExtensionType | None = None
        # ``storage_type`` must not contain nested extension types (ET1 in DESIGN_ISSUES.md).
        # On the write path, ``PydanticLogicalTypeFactory.create_for_python_type`` strips any
        # top-level extension type from each field's Arrow type before inserting it into the
        # struct. On the read path, ``reconstruct_from_arrow`` receives a ``storage_type``
        # already guaranteed storage-safe by ``register_storage_type``.
        self._polars_ext_class = make_polars_extension_type(
            logical_name,
            storage_type,
            metadata=json.dumps({"category": PYDANTIC_CATEGORY}),
        )
        self._polars_ext: pl.BaseExtension | None = None

    @property
    def logical_type_name(self) -> str:
        """Fully-qualified class name used as the logical type identifier."""
        return self._logical_name

    @property
    def python_type(self) -> type:
        """The pydantic ``BaseModel`` subclass this logical type represents."""
        return self._python_type

    def get_arrow_extension_type(self) -> pa.ExtensionType:
        """Return the Arrow extension type for this model.

        Returns:
            A cached ``pa.ExtensionType`` instance with ``extension_name`` equal to
            the fully-qualified class name and ``storage_type`` equal to the struct
            of the model fields.
        """
        if self._arrow_ext is None:
            self._arrow_ext = self._arrow_ext_class()
        return self._arrow_ext

    def get_polars_extension_type(self) -> pl.BaseExtension:
        """Return the Polars extension type for this model.

        Returns:
            A cached ``pl.BaseExtension`` instance.
        """
        if self._polars_ext is None:
            self._polars_ext = self._polars_ext_class()
        return self._polars_ext

    def python_to_storage(self, value: Any, converter: TypeConverterProtocol | None) -> dict[str, Any]:
        """Convert a pydantic model instance to an Arrow-compatible struct dict.

        Iterates ``_field_annotations`` and delegates each field's conversion to
        ``converter.python_to_storage``.

        Args:
            value: A pydantic model instance of type ``python_type``.
            converter: The active converter for per-field delegation. Must not be ``None``.

        Returns:
            A dict mapping field names to their Arrow storage values.

        Raises:
            ValueError: If ``converter`` is ``None``.
        """
        if converter is None:
            raise ValueError(
                "PydanticLogicalType.python_to_storage requires a converter — "
                "pass a TypeConverterProtocol instance for field-level conversion."
            )
        return {
            name: converter.python_to_storage(getattr(value, name), annotation)
            for name, annotation in self._field_annotations
        }

    def storage_to_python(self, storage_value: Any, converter: TypeConverterProtocol | None) -> Any:
        """Reconstruct a pydantic model instance from an Arrow struct dict.

        Args:
            storage_value: A dict mapping field names to Arrow storage values.
            converter: The active converter for per-field delegation. Must not be ``None``.

        Returns:
            A pydantic model instance of type ``python_type``. Pydantic validation
            runs on construction, ensuring the model is always in a valid state.

        Raises:
            ValueError: If ``converter`` is ``None``.
        """
        if converter is None:
            raise ValueError(
                "PydanticLogicalType.storage_to_python requires a converter — "
                "pass a TypeConverterProtocol instance for field-level conversion."
            )
        kwargs = {
            name: converter.storage_to_python(storage_value[name], annotation)
            for name, annotation in self._field_annotations
        }
        return self._python_type(**kwargs)


class PydanticLogicalTypeFactory:
    """Stateless factory that synthesises and reconstructs ``PydanticLogicalType`` instances.

    **Write path** (``create_for_python_type``): derives Arrow struct type from the
    model fields by delegating to ``converter.register_python_class`` per field.
    Only fields in ``model_fields`` are stored — computed fields and private
    attributes are excluded.

    **Read path** (``reconstruct_from_arrow``): imports the model by FQCN, matches
    fields against the already-resolved ``storage_type``, and returns a
    ``PydanticLogicalType``.

    Category tag: ``"orcapod.pydantic"``

    Register with::

        from pydantic import BaseModel
        converter.register_logical_type_factory(
            PydanticLogicalTypeFactory(),
            category="orcapod.pydantic",
            python_bases=[BaseModel],
        )

    Example:
        >>> factory = PydanticLogicalTypeFactory()
        >>> factory.supports_class(MyModel)
        True
        >>> factory.supports_class(str)
        False
    """

    def supports_class(self, python_type: type) -> bool:
        """Return True if ``python_type`` is a pydantic ``BaseModel`` subclass.

        Args:
            python_type: Any Python type.

        Returns:
            True if ``python_type`` is a ``BaseModel`` subclass.
        """
        from pydantic import BaseModel
        return isinstance(python_type, type) and issubclass(python_type, BaseModel)

    def create_for_python_type(
        self,
        python_type: type,
        converter: TypeConverterProtocol,
    ) -> PydanticLogicalType:
        """Synthesise a ``PydanticLogicalType`` for a pydantic model (write path).

        Derives the FQCN, obtains type hints, and resolves each field's Arrow type
        via ``converter.register_python_class``. Only fields present in
        ``model_fields`` are stored — computed fields and private attributes are
        excluded. Rejects local / unnamed classes.

        Args:
            python_type: A pydantic ``BaseModel`` subclass.
            converter: The active converter for field-type resolution.

        Returns:
            A ``PydanticLogicalType`` ready for registration.

        Raises:
            ValueError: If ``python_type`` is a local class (``__qualname__`` contains
                ``"<locals>"``).
        """
        import typing

        fqcn = f"{python_type.__module__}.{python_type.__qualname__}"
        if "<locals>" in fqcn:
            raise ValueError(
                f"Cannot register local class {python_type!r} as a PydanticLogicalType — "
                f"local classes have no stable fully-qualified class name and cannot be "
                f"reconstructed on read. Define the model at module level."
            )

        try:
            hints = typing.get_type_hints(python_type)
        except Exception as exc:
            raise ValueError(
                f"Cannot get type hints for {python_type!r}: {exc}"
            ) from exc

        arrow_fields = []
        field_annotations = []
        for field_name in python_type.model_fields:
            annotation = hints.get(field_name, Any)
            arrow_type = converter.register_python_class(annotation)
            # Strip top-level extension type before inserting into the struct (ET1;
            # see DESIGN_ISSUES.md): Arrow cannot represent extension types inside
            # struct field types.
            if isinstance(arrow_type, pa.ExtensionType):
                arrow_type = arrow_type.storage_type
            arrow_fields.append(pa.field(field_name, arrow_type))
            field_annotations.append((field_name, annotation))

        storage_type = pa.struct(arrow_fields)
        logger.debug("PydanticLogicalTypeFactory: synthesised %r for %r", fqcn, python_type)
        return PydanticLogicalType(fqcn, python_type, storage_type, field_annotations)

    def reconstruct_from_arrow(
        self,
        arrow_extension_name: str,
        storage_type: pa.DataType,
        metadata: dict[str, Any],
        converter: TypeConverterProtocol,
    ) -> PydanticLogicalType:
        """Reconstruct a ``PydanticLogicalType`` from Arrow schema metadata (read path).

        Imports the model from its FQCN (``arrow_extension_name``), then matches
        the model field annotations against the fields in ``storage_type``.
        ``storage_type`` is already bottom-up resolved by ``register_storage_type``
        before this method is called.

        Args:
            arrow_extension_name: FQCN of the pydantic model (Arrow extension name).
            storage_type: Already-resolved ``pa.StructType`` for the model fields.
            metadata: Full parsed metadata JSON dict (always contains ``"category"``).
            converter: The active converter (used for registration completeness invariant).

        Returns:
            A ``PydanticLogicalType`` ready for registration.

        Raises:
            ImportError: If the class cannot be imported from ``arrow_extension_name``.
            ValueError: If ``storage_type`` is not a struct type.
        """
        import typing

        if not pa.types.is_struct(storage_type):
            raise ValueError(
                f"PydanticLogicalTypeFactory.reconstruct_from_arrow: expected a struct "
                f"storage type for {arrow_extension_name!r}, got {storage_type!r}."
            )

        cls = _import_pydantic_model_from_fqcn(arrow_extension_name)

        try:
            hints = typing.get_type_hints(cls)
        except Exception as exc:
            raise ValueError(
                f"Cannot get type hints for {cls!r}: {exc}"
            ) from exc

        field_annotations = []
        for field_name in cls.model_fields:
            annotation = hints.get(field_name, Any)
            # Register any logical type the field annotation maps to (registration
            # completeness invariant: all nested logical types must be registered when
            # the outer type is registered). The return value is discarded.
            converter.register_python_class(annotation)
            field_annotations.append((field_name, annotation))

        logger.debug(
            "PydanticLogicalTypeFactory: reconstructed %r from Arrow", arrow_extension_name
        )
        return PydanticLogicalType(
            arrow_extension_name, cls, storage_type, field_annotations
        )


def _import_pydantic_model_from_fqcn(fqcn: str) -> type:
    """Import a pydantic ``BaseModel`` subclass from its fully-qualified class name.

    Delegates the module-prefix walk to ``type_utils._walk_fqcn``, then
    validates the resolved object is a ``BaseModel`` subclass.

    Args:
        fqcn: Fully-qualified class name, e.g. ``"mypackage.sub.MyModel"``.

    Returns:
        The imported ``BaseModel`` subclass.

    Raises:
        ImportError: If no valid module+attribute split can be found, or if the
            resolved object is not a ``BaseModel`` subclass.
    """
    from pydantic import BaseModel
    from orcapod.extension_types.type_utils import _walk_fqcn

    obj: Any = _walk_fqcn(fqcn)
    if not (isinstance(obj, type) and issubclass(obj, BaseModel)):
        raise ImportError(
            f"{fqcn!r} does not resolve to a pydantic BaseModel subclass."
        )
    return obj
