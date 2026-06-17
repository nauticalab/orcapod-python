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


class PydanticLogicalType:
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
        self._polars_ext_class = make_polars_extension_type(logical_name, storage_type)
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
