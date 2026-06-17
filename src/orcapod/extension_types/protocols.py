"""Protocol definitions for the Arrow/Polars extension type system.

This module defines ``TypeConverterProtocol``, ``LogicalTypeProtocol``, and
``LogicalTypeFactoryProtocol`` — the contracts for the converter, for logical
type implementations that bind a Python class to its Arrow and Polars extension
type representation, and for factories that auto-construct such implementations
from Arrow schema metadata.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    import polars as pl
    import pyarrow as pa


@runtime_checkable
class TypeConverterProtocol(Protocol):
    """Minimal protocol exposing what factories and logical types need from the converter.

    Placed in ``extension_types/protocols.py`` to avoid circular imports.
    ``UniversalTypeConverter`` is the canonical implementation.
    """

    def register_python_class(self, annotation: Any) -> "pa.DataType":
        """Traverse a Python annotation, register any logical types found, and return
        the storage-safe Arrow type.

        The returned type may be a ``pa.ExtensionType`` at the top level for registered
        classes (e.g. ``UUID`` → ``orcapod.uuid`` extension type), but struct fields and
        list value types at any depth are always plain (non-extension) Arrow types.

        Args:
            annotation: A Python type or generic alias (e.g. ``list[str]``,
                ``Optional[uuid.UUID]``, a dataclass type).

        Returns:
            A storage-safe ``pa.DataType``. May be ``pa.ExtensionType`` at the top level;
            never contains nested extension types in struct/list fields.
        """
        ...

    def register_storage_type(self, arrow_type: "pa.DataType") -> "pa.DataType":
        """Traverse an Arrow type bottom-up, registering extension types, and return a
        storage-safe type.

        The returned type may be a ``pa.ExtensionType`` at the top level, but struct fields
        and list value types at any depth are always plain (non-extension) Arrow types.
        This invariant makes the return value safe to use as a struct field or list element
        type without further stripping.

        Args:
            arrow_type: An Arrow type to traverse and register.

        Returns:
            A storage-safe ``pa.DataType``.
        """
        ...

    def python_to_storage(self, value: Any, annotation: Any) -> Any:
        """Convert a Python value to its Arrow storage representation."""
        ...

    def storage_to_python(self, storage_value: Any, annotation: Any) -> Any:
        """Convert an Arrow storage value back to a Python object."""
        ...

    def apply_extension_types(self, table: "pa.Table") -> "pa.Table":
        """Re-wrap table columns into their registered Arrow extension types."""
        ...

    def register_arrow_extension(
        self,
        arrow_extension_name: str,
        extension_metadata: "bytes | None",
        storage_type: "pa.DataType",
    ) -> "pa.DataType":
        """Register an extension type from (name, metadata, storage_type) and return the Arrow type."""
        ...


@runtime_checkable
class LogicalTypeProtocol(Protocol):
    """Protocol for Arrow/Polars extension-type-backed logical types.

    A ``LogicalTypeProtocol`` is a three-way binding between a unique logical type name
    (orcapod's identifier), a Python class, and Arrow/Polars extension types.
    Each implementation *owns* its Arrow and Polars extension types by providing
    them directly via ``get_arrow_extension_type`` and ``get_polars_extension_type``.

    This protocol is Arrow I/O only — hashing is not a logical type responsibility.
    """

    @property
    def logical_type_name(self) -> str:
        """Unique orcapod identifier for this logical type (e.g. ``"orcapod.uuid"``)."""
        ...

    @property
    def python_type(self) -> type:
        """The Python class this logical type represents."""
        ...

    def get_arrow_extension_type(self) -> "pa.ExtensionType":
        """Return the Arrow extension type for this logical type."""
        ...

    def get_polars_extension_type(self) -> "pl.BaseExtension":
        """Return an instance of the Polars extension type for this logical type."""
        ...

    def python_to_storage(self, value: Any, converter: "TypeConverterProtocol | None") -> Any:
        """Convert a Python value to its Arrow storage representation.

        Args:
            value: A Python object of type ``python_type``.
            converter: The active ``TypeConverterProtocol`` for recursive delegation.

        Returns:
            A value suitable for Arrow storage.
        """
        ...

    def storage_to_python(self, storage_value: Any, converter: "TypeConverterProtocol | None") -> Any:
        """Convert an Arrow storage value back to a Python object.

        Args:
            storage_value: A scalar or array element from the Arrow storage array.
            converter: The active ``TypeConverterProtocol`` for recursive delegation.

        Returns:
            A Python object of type ``python_type``.
        """
        ...


@runtime_checkable
class LogicalTypeFactoryProtocol(Protocol):
    """Protocol for factories that synthesize or reconstruct ``LogicalTypeProtocol`` instances.

    Bridges two directions: the write path (``create_for_python_type``) and the read
    path (``reconstruct_from_arrow``). Both methods receive ``converter`` instead of
    ``registry`` so all traversal flows through the converter.
    """

    def supports_class(self, python_type: type) -> bool:
        """Return True if this factory can synthesize a LogicalType for ``python_type``.

        Used as a probe during write-side MRO dispatch in ``register_python_class``.

        Args:
            python_type: The Python class to probe.

        Returns:
            True if this factory handles ``python_type``.
        """
        ...

    def create_for_python_type(
        self,
        python_type: type,
        converter: "TypeConverterProtocol",
    ) -> LogicalTypeProtocol:
        """Synthesize a LogicalType for the given Python class (write path).

        Args:
            python_type: The concrete Python class to synthesize a LogicalType for.
            converter: The active converter for recursive field-type resolution.

        Returns:
            A fully constructed ``LogicalTypeProtocol`` ready for registration.

        Raises:
            ValueError: If this factory cannot construct a type for the given class.
        """
        ...

    def reconstruct_from_arrow(
        self,
        arrow_extension_name: str,
        storage_type: "pa.DataType",
        metadata: dict[str, Any],
        converter: "TypeConverterProtocol",
    ) -> LogicalTypeProtocol:
        """Reconstruct a LogicalType from Arrow schema metadata (read path).

        Args:
            arrow_extension_name: The Arrow extension type name from the schema.
            storage_type: The underlying Arrow storage type (already resolved bottom-up).
            metadata: Full parsed metadata JSON dict. Always contains ``"category"``.
            converter: The active converter for recursive field-type resolution.

        Returns:
            A fully constructed ``LogicalTypeProtocol`` ready for registration.

        Raises:
            ValueError: If this factory cannot reconstruct a type for the given name.
        """
        ...
