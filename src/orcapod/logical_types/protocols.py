"""Protocol definitions for the Arrow/Polars extension type system.

This module defines ``TypeConverterProtocol``, ``LogicalTypeProtocol``, and
``LogicalTypeFactoryProtocol`` — the contracts for the converter, for logical
type implementations that bind a Python class to its Arrow and Polars extension
type representation, and for factories that auto-construct such implementations
from Arrow schema metadata.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from orcapod.types import DataType

if TYPE_CHECKING:
    import polars as pl
    import pyarrow as pa


@runtime_checkable
class TypeConverterProtocol(Protocol):
    """Minimal protocol exposing what factories and logical types need from the converter.

    Placed in ``logical_types/protocols.py`` to avoid circular imports.
    ``UniversalTypeConverter`` is the canonical implementation.
    """

    def register_python_class(self, annotation: Any) -> "pa.DataType":
        """Ensure a ``LogicalType`` is registered for ``annotation`` and return the Arrow column data type.

        Traverses generic annotations recursively. For each concrete class found,
        either returns from the primitive map or registry (cache hit), or
        synthesises via factory and registers the result.

        The returned ``pa.DataType`` describes what the column looks like in Arrow
        storage: a ``pa.ExtensionType`` when the class has a registered ``LogicalType``
        (e.g. ``uuid.UUID`` → ``orcapod.uuid``), or a plain Arrow type otherwise
        (e.g. ``str`` → ``pa.large_string()``). Struct fields and list value types
        are always plain (non-extension) Arrow types, regardless of what the element
        logical type is — this is the ET1 constraint.

        Args:
            annotation: A Python type or generic alias (e.g. ``list[str]``,
                ``Optional[uuid.UUID]``, a dataclass type).

        Returns:
            A ``pa.DataType`` for the Arrow column. May be ``pa.ExtensionType`` at the
            top level; never contains nested extension types in struct/list fields.
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

    def apply_logical_types(self, table: "pa.Table") -> "pa.Table":
        """Re-wrap table columns into their registered Arrow extension types."""
        ...

    def register_discovered_logical_types(self, schema: "pa.Schema") -> None:
        """Register any extension types found in ``schema`` that are not yet known."""
        ...

    def load_logical_types(self, table: "pa.Table") -> "pa.Table":
        """Register and apply extension types for *table* in one step."""
        ...

    def register_logical_type_from_arrow_metadata(
        self,
        arrow_extension_name: str,
        extension_metadata: "bytes | None",
        storage_type: "pa.DataType",
    ) -> "pa.DataType":
        """Reconstruct and register a ``LogicalType`` from Arrow schema metadata.

        Called during the read path when an Arrow extension type is discovered in a
        Parquet or IPC schema. Parses the JSON ``extension_metadata`` (from the
        ``ARROW:extension:metadata`` field) to find the ``"category"`` key, then
        delegates to the matching ``LogicalTypeFactoryProtocol.reconstruct_from_arrow``
        to re-create the ``LogicalType`` and register it.

        The ``storage_type`` must already be resolved (nested extension types
        registered bottom-up) before calling this method.

        Args:
            arrow_extension_name: Arrow extension name (``ARROW:extension:name``).
            extension_metadata: Raw metadata bytes, expected to be UTF-8 JSON with
                at least a ``"category"`` key. ``None`` or empty bytes if absent.
            storage_type: Underlying Arrow storage type (already bottom-up resolved).

        Returns:
            The Arrow extension type after registration.

        Raises:
            ValueError: If metadata is missing, malformed, lacks ``"category"``, or
                no factory is registered for the category.
        """
        ...

    def arrow_type_to_python_type(self, arrow_type: "pa.DataType") -> "DataType":
        """Convert an Arrow type to its Python type hint.

        Args:
            arrow_type: An Arrow type (may be a ``pa.ExtensionType``).

        Returns:
            The Python type hint corresponding to ``arrow_type``.
        """
        ...

    def get_logical_type_by_arrow_extension_name(
        self, arrow_extension_name: str
    ) -> "LogicalTypeProtocol | None":
        """Return the registered logical type for *arrow_extension_name*, or ``None``.

        Used by ``ListLogicalTypeFactory.reconstruct_from_arrow`` to retrieve the
        element logical type after ``register_logical_type_from_arrow_metadata`` has registered it.

        Args:
            arrow_extension_name: Arrow extension name to look up (e.g. ``"orcapod.uuid"``).

        Returns:
            The registered ``LogicalTypeProtocol``, or ``None`` if not found.
        """
        ...

    def get_logical_type_for_python_type(
        self, annotation: "Any"
    ) -> "LogicalTypeProtocol | None":
        """Return the ``LogicalType`` for *annotation*, registering it first if needed.

        Combines registration and registry lookup into a single call, providing a
        direct path from a Python type annotation to its ``LogicalType`` without
        going through the intermediate Arrow type representation.

        Args:
            annotation: A Python type or generic alias (e.g. ``uuid.UUID``,
                ``list[uuid.UUID]``). Primitives like ``str`` and ``int`` have no
                ``LogicalType`` and return ``None``.

        Returns:
            The registered ``LogicalTypeProtocol`` for *annotation*, or ``None`` if
            the type has no associated ``LogicalType``.
        """
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

    def pick_field(self, key: str) -> DataType:
        """Return the Python type of field ``key`` in this structured logical type.

        Args:
            key: Name of the field to project into.

        Returns:
            The Python ``DataType`` of the requested field.

        Raises:
            InputValidationError: If the field does not exist in the type's schema.
            NotImplementedError: If this logical type does not support keyed access.
        """
        ...

    def index_element(self) -> DataType:
        """Return the Python element type for positional list access.

        Returns:
            The Python ``DataType`` of elements in this list-like logical type.

        Raises:
            NotImplementedError: If this logical type does not support positional access.
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
