"""Protocol definitions for the Arrow/Polars extension type system.

This module defines ``LogicalType`` — the contract for all implementations
that bind a Python class to its Arrow and Polars extension type representation.

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
class LogicalType(Protocol):
    """Protocol for Arrow/Polars extension-type-backed logical types.

    A ``LogicalType`` is a three-way binding between a unique logical type name
    (orcapod's identifier), a Python class, and Arrow/Polars extension types.
    Each implementation *owns* its Arrow and Polars extension types by providing
    them directly via ``get_arrow_extension_type`` and ``get_polars_extension_type``.

    This protocol is Arrow I/O only — hashing is not a logical type responsibility.
    """

    @property
    def logical_type_name(self) -> str:
        """Unique orcapod identifier for this logical type.

        By convention the Python FQCN (e.g. ``"uuid.UUID"``), but any unique
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
