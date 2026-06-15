"""Protocol definitions for the Arrow/Polars extension type system.

This module defines ``LogicalTypeProtocol`` and ``LogicalTypeFactoryProtocol`` —
the contracts for implementations that bind a Python class to its Arrow and Polars
extension type representation, and for factories that auto-construct such
implementations from Arrow schema metadata.

Note:
    This module is part of the parallel-build phase. The old
    ``SemanticStructConverterProtocol`` in ``protocols/semantic_types_protocols.py``
    is untouched; it is removed in PLT-1660.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    import polars as pl
    import pyarrow as pa


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
        """Unique orcapod identifier for this logical type.

        By convention the Python fully qualified name (e.g. ``"uuid.UUID"``), but any unique
        string is valid. Does NOT need to match the Arrow extension type name.
        """
        ...

    @property
    def python_type(self) -> type:
        """The Python class this logical type represents."""
        ...

    def get_arrow_extension_type(self) -> pa.ExtensionType:
        """Return the Arrow extension type for this logical type.

        ``storage_type``, ``extension_name``, and serialised metadata are
        encapsulated inside the returned type; they are no longer top-level
        properties on ``LogicalType``.

        For custom types: create and return an instance of a new
        ``pa.ExtensionType`` subclass (e.g. via ``make_arrow_extension_type``).
        For pre-existing types: return the existing instance directly
        (e.g. ``pa.uuid()``).
        """
        ...

    def get_polars_extension_type(self) -> pl.BaseExtension:
        """Return an instance of the Polars extension type for this logical type.

        The registry calls ``type(instance)`` to obtain the class passed to
        ``pl.register_extension_type``.
        """
        ...

    def python_to_storage(self, value: Any) -> Any:
        """Convert a Python value to its Arrow storage representation.

        Args:
            value: A Python object of type ``python_type``.

        Returns:
            A value suitable for use as an Arrow scalar or array element
            matching the storage type of ``get_arrow_extension_type()``.
        """
        ...

    def storage_to_python(self, storage_value: Any) -> Any:
        """Convert an Arrow storage value back to a Python object.

        Args:
            storage_value: A scalar or array element from the Arrow storage array.

        Returns:
            A Python object of type ``python_type``.
        """
        ...


@runtime_checkable
class LogicalTypeFactoryProtocol(Protocol):
    """Protocol for factories that auto-construct ``LogicalTypeProtocol`` instances from Arrow schema metadata.

    A ``LogicalTypeFactoryProtocol`` constructs a ``LogicalTypeProtocol`` from the
    Arrow extension type name, its underlying storage type, and the full parsed JSON
    metadata dict. The dispatch key (``"category"`` value from the metadata JSON) that
    routes to this factory is declared at registration time via
    ``LogicalTypeRegistry.register_logical_type_factory``; the factory itself has no
    knowledge of its dispatch key but receives the full metadata dict so it can read
    additional hints beyond ``"category"``.

    This protocol is ``@runtime_checkable``, consistent with ``LogicalTypeProtocol``.
    """

    def create_logical_type(
        self,
        arrow_extension_name: str,
        storage_type: pa.DataType,
        metadata: dict[str, Any],
    ) -> LogicalTypeProtocol:
        """Construct a ``LogicalTypeProtocol`` for the given Arrow extension name and storage type.

        Args:
            arrow_extension_name: The Arrow extension type name extracted from the
                schema (i.e. the value of ``ARROW:extension:name`` field metadata).
            storage_type: The underlying Arrow storage type for this extension field.
            metadata: The full parsed JSON metadata dict (``dict[str, Any]``). Always contains at least a
                ``"category"`` key. May contain additional keys the factory uses (e.g.
                ``"protocol"``, ``"pydantic_version"``).

        Returns:
            A fully constructed ``LogicalTypeProtocol`` ready to be passed to
            ``LogicalTypeRegistry.register_logical_type()``.

        Raises:
            ValueError: If this factory cannot construct a logical type for the given
                extension name (e.g. the Python class cannot be resolved by name).
        """
        ...
