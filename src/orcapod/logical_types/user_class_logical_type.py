"""Shared logical type and factory for user classes (dataclasses and pydantic models).

A user class's logical type name and Arrow extension name are its *identity string*
(``user_class_identity.identity_string``), and its Arrow extension metadata carries the
full identity record. Together they are the class's logical type identifier.

Exact classes (ordinary dataclasses, pydantic models; ITL-972) are bound to the class
object: a value is written only if its class is that object, and stored values are read
by finding the class at its module path and comparing its record with the stored one.

Write path (``create_for_python_type``):
    Describes the class (name, kind, fields in declared order) and resolves each stored
    field's Arrow type through the converter.

Read path (``reconstruct_from_arrow``):
    Reads the identity record from the metadata, finds the class for it
    (``class_for_stored_record``) and returns that class's logical type.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from orcapod.errors import RecursiveTypeError, TypeIdentityError
import orcapod.logical_types.user_class_identity as uci
from orcapod.logical_types.base_logical_type import BaseLogicalType
from orcapod.logical_types.structural_class import class_for_record
from orcapod.logical_types.registry import (
    make_arrow_extension_type,
    make_polars_extension_type,
)
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import polars as pl
    import pyarrow as pa

    from orcapod.logical_types.protocols import TypeConverterProtocol
else:
    pa = LazyModule("pyarrow")
    pl = LazyModule("polars")

logger = logging.getLogger(__name__)


# Extension type classes per (identity string, storage type, metadata). Two class objects
# with one identity (a class defined again, two structural classes of one type) must
# give Arrow types that compare equal, and pyarrow compares the type's Python class.
_EXTENSION_CLASSES: dict[tuple[str, str, str], tuple[type, type]] = {}


def _extension_classes(
    logical_name: str, storage_type: pa.DataType, metadata_json: str
) -> tuple[type[pa.ExtensionType], type[pl.BaseExtension]]:
    """Return the Arrow and Polars extension type classes of an identifier.

    Args:
        logical_name: The identity string, used as the extension name.
        storage_type: The Arrow storage struct.
        metadata_json: The extension metadata as canonical JSON.

    Returns:
        ``(arrow class, polars class)``, the same pair for the same arguments.
    """
    key = (logical_name, str(storage_type), metadata_json)
    found = _EXTENSION_CLASSES.get(key)
    if found is None:
        found = (
            make_arrow_extension_type(
                logical_name, storage_type, metadata=metadata_json.encode("utf-8")
            ),
            make_polars_extension_type(
                logical_name, storage_type, metadata=metadata_json
            ),
        )
        _EXTENSION_CLASSES[key] = found
    return found


class UserClassLogicalType(BaseLogicalType):
    """Logical type binding a user class to its Arrow extension type.

    The logical type name and Arrow extension name are the class's identity string;
    the storage type is a ``pa.struct`` of the stored fields in declared order.
    Subclasses set ``category``.

    Args:
        logical_name: The identity string (e.g. ``"pkg.models.Point@76d961e4..."``).
        python_type: The class.
        storage_type: The Arrow ``pa.StructType`` of the stored fields.
        field_annotations: ``(field_name, python_annotation)`` pairs matching the
            fields in ``storage_type``.
        identity_record: The identity record, embedded in the Arrow extension metadata.
            ``None`` embeds only the category (for low-level tests).
    """

    category: str = ""

    #: A subclass of the class is a different class with its own identity, so the
    #: registry must not match it to this logical type by ``issubclass``.
    exact_python_type_only: bool = True

    def __init__(
        self,
        logical_name: str,
        python_type: type,
        storage_type: pa.StructType,
        field_annotations: list[tuple[str, Any]],
        identity_record: dict[str, Any] | None = None,
    ) -> None:
        self._logical_name = logical_name
        self._python_type = python_type
        self._storage_type = storage_type
        self._field_annotations = field_annotations
        self._identity_record = identity_record

        metadata: dict[str, Any] = {"category": self.category}
        if identity_record is not None:
            metadata["identity"] = identity_record
        metadata_json = uci.canonical_json(metadata)
        # ``storage_type`` never contains nested extension types (ET1 in design_issues/):
        # the factory strips each field's top-level extension type before building it.
        self._arrow_ext_class, self._polars_ext_class = _extension_classes(
            logical_name, storage_type, metadata_json
        )
        self._arrow_ext: pa.ExtensionType | None = None
        self._polars_ext: pl.BaseExtension | None = None

    @property
    def logical_type_name(self) -> str:
        """The class's identity string."""
        return self._logical_name

    @property
    def python_type(self) -> type:
        """The class this logical type represents."""
        return self._python_type

    @property
    def identity_record(self) -> dict[str, Any] | None:
        """The identity record, or ``None`` for a logical type built without one."""
        return self._identity_record

    @property
    def storage_type(self) -> pa.StructType:
        """The Arrow struct type of the stored fields."""
        return self._storage_type

    def get_arrow_extension_type(self) -> pa.ExtensionType:
        """Return the cached Arrow extension type (name = identity string)."""
        if self._arrow_ext is None:
            self._arrow_ext = self._arrow_ext_class()  # pyright: ignore[reportCallIssue]  # synthesized class takes no arguments
        return self._arrow_ext

    def get_polars_extension_type(self) -> pl.BaseExtension:
        """Return the cached Polars extension type."""
        if self._polars_ext is None:
            self._polars_ext = self._polars_ext_class()  # pyright: ignore[reportCallIssue]  # synthesized class takes no arguments
        return self._polars_ext

    def python_to_storage(
        self, value: Any, converter: TypeConverterProtocol | None
    ) -> dict[str, Any]:
        """Convert an instance to an Arrow-compatible struct dict.

        Args:
            value: An instance of ``python_type``.
            converter: The active converter for per-field conversion.

        Returns:
            A dict mapping field names to their Arrow storage values.

        Raises:
            ValueError: If ``converter`` is ``None``.
            TypeError: If ``value`` is not an instance of this type's class
                (``accepts_instance``).
        """
        if converter is None:
            raise ValueError(
                f"{type(self).__name__}.python_to_storage requires a converter — "
                "pass a TypeConverterProtocol instance for field-level conversion."
            )
        if not self.accepts_instance(value, converter):
            raise TypeError(
                f"Cannot store a value of class {uci.class_path(type(value))} as "
                f"{uci.class_path(self._python_type)}: "
                f"{self.instance_requirement}"
            )
        return {
            name: converter.python_to_storage(getattr(value, name), annotation)
            for name, annotation in self._field_annotations
        }

    #: What ``accepts_instance`` requires, for the error message.
    instance_requirement: str = (
        "an ordinary dataclass or pydantic model is stored only from instances of "
        "that exact class. Build an instance of the class, or annotate the value "
        "with its own class."
    )

    def accepts_instance(self, value: Any, converter: TypeConverterProtocol) -> bool:
        """Return True if ``value`` may be stored as this type.

        Args:
            value: The value offered.
            converter: The active converter.

        Returns:
            For an exact class: whether ``type(value)`` is the class object. A subclass
            instance is refused, because its own fields would be lost.
        """
        return type(value) is self._python_type

    def storage_to_python(
        self, storage_value: Any, converter: TypeConverterProtocol | None
    ) -> Any:
        """Build an instance from an Arrow struct dict.

        Args:
            storage_value: A dict mapping field names to Arrow storage values, or
                ``None`` for a null value (an optional field of this class that
                holds ``None``).
            converter: The active converter for per-field conversion.

        Returns:
            An instance of ``python_type``, or ``None`` for a null value.

        Raises:
            ValueError: If ``converter`` is ``None``.
        """
        if converter is None:
            raise ValueError(
                f"{type(self).__name__}.storage_to_python requires a converter — "
                "pass a TypeConverterProtocol instance for field-level conversion."
            )
        if storage_value is None:
            return None
        kwargs = {
            name: converter.storage_to_python(storage_value[name], annotation)
            for name, annotation in self._field_annotations
        }
        return self._python_type(**kwargs)


class UserClassLogicalTypeFactory:
    """Base factory for user-class logical types.

    Subclasses set ``kind``, ``category`` and ``logical_type_class``.
    """

    kind: str = ""
    category: str = ""
    logical_type_class: type[UserClassLogicalType] = UserClassLogicalType

    def supports_class(self, python_type: type) -> bool:
        """Return True if this factory handles ``python_type``."""
        return uci.class_kind(python_type) == self.kind

    # ------------------------------------------------------------------
    # Write path
    # ------------------------------------------------------------------

    def create_for_python_type(
        self,
        python_type: type,
        converter: TypeConverterProtocol,
    ) -> UserClassLogicalType:
        """Build the logical type of a user class (write path).

        Args:
            python_type: The class.
            converter: The active converter for field-type resolution.

        Returns:
            A logical type whose name is the class's identity string.

        Raises:
            ValueError: If an exact class is local or type hints cannot be resolved.
            TypeError: If the class refers back to itself (directly or through other
                classes), naming the field path, or a field type cannot be stored.
        """
        record = uci.class_record(python_type, converter)
        storage_type, field_annotations = self._storage(python_type, converter)
        identity = uci.identity_string(record)
        logger.debug("%s: built %r for %r", type(self).__name__, identity, python_type)
        return self.logical_type_class(
            identity, python_type, storage_type, field_annotations, record
        )

    def _storage(
        self, python_type: type, converter: TypeConverterProtocol
    ) -> tuple[pa.StructType, list[tuple[str, Any]]]:
        """Return the storage struct and field annotations of a class.

        Registers the field types (their Arrow types are needed), but not
        ``python_type`` itself.

        Args:
            python_type: The class.
            converter: The active converter for field-type resolution.

        Returns:
            ``(storage_type, field_annotations)``, fields in declared order: the order
            is part of the identity, so one identity always has one struct.

        Raises:
            ValueError: If the type hints cannot be resolved.
            RecursiveTypeError: If the class refers back to itself, naming the path.
        """
        hints = uci.type_hints(python_type)
        arrow_fields = []
        field_annotations: list[tuple[str, Any]] = []
        for field_name in uci.stored_field_names(python_type):
            annotation = hints.get(field_name, Any)
            try:
                arrow_type = converter.register_python_class(annotation)
            except TypeError as exc:
                if isinstance(exc, RecursiveTypeError):
                    raise RecursiveTypeError(
                        f"{python_type.__qualname__}.{field_name} -> {exc}"
                    ) from exc
                raise
            # Struct fields cannot hold extension types (ET1; see design_issues/).
            if isinstance(arrow_type, pa.ExtensionType):
                arrow_type = arrow_type.storage_type
            arrow_fields.append(pa.field(field_name, arrow_type))
            field_annotations.append((field_name, annotation))
        return pa.struct(arrow_fields), field_annotations

    # ------------------------------------------------------------------
    # Read path
    # ------------------------------------------------------------------

    def reconstruct_from_arrow(
        self,
        arrow_extension_name: str,
        storage_type: pa.DataType,
        metadata: dict[str, Any],
        converter: TypeConverterProtocol,
    ) -> UserClassLogicalType:
        """Find the class of a stored column and return its logical type (read path).

        Args:
            arrow_extension_name: The stored identity string.
            storage_type: The stored struct type (already resolved).
            metadata: The parsed extension metadata, with ``category`` and ``identity``.
            converter: The active converter.

        Returns:
            The logical type of the class the stored record describes.

        Raises:
            ImportError: If an exact class cannot be found at its module path.
            TypeIdentityError: If the column has no identity record (written before
                Orcapod 0.3), the record is invalid, the class found is defined with
                other fields, or its storage type differs from the stored one.
        """
        record = metadata.get("identity")
        if not isinstance(record, dict):
            raise TypeIdentityError(
                f"The stored column type {arrow_extension_name!r} was written by an "
                f"Orcapod version before 0.3, which stored no description of the "
                f"class. Orcapod 0.3 does not read such columns; re-running the "
                f"pipeline computes the results into new tables."
            )
        if (
            record.get("kind") != self.kind
            or not isinstance(record.get("name"), str)
            or uci.identity_string(record) != arrow_extension_name
        ):
            raise TypeIdentityError(
                f"Stored column type {arrow_extension_name!r} has an invalid identity "
                f"record for category {self.category!r}: {record!r}."
            )
        try:
            cls = class_for_record(record, converter)
        except ImportError:
            raise
        except (TypeError, ValueError) as exc:
            raise TypeIdentityError(str(exc)) from exc
        _ = converter.register_python_class(cls)
        lt = converter.get_logical_type_for_python_type(cls)
        if not isinstance(lt, UserClassLogicalType):
            raise TypeIdentityError(
                f"{uci.class_path(cls)} is not registered as a user class."
            )
        _check_storage(lt, storage_type, record)
        return lt


def _check_storage(
    lt: UserClassLogicalType, storage_type: pa.DataType, record: dict[str, Any]
) -> None:
    """Raise ``TypeIdentityError`` if stored and registered storage differ beyond layout."""
    from orcapod.utils.arrow_utils import normalize_to_large_types

    if normalize_to_large_types(
        lt.storage_type, keep_null=True
    ) != normalize_to_large_types(storage_type, keep_null=True):
        raise TypeIdentityError(
            f"Stored values of {uci.describe_record(record)} have Arrow storage type "
            f"{storage_type}, but the registered class stores {lt.storage_type}."
        )
