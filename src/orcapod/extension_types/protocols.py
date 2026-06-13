"""Protocol definitions for the Arrow/Polars extension type system.

This module defines ``ExtensionTypeConverter`` — the contract for all
converters that map between Python objects and their Arrow extension type
storage representation.

Note:
    This module is part of the parallel-build phase. The old
    ``SemanticStructConverterProtocol`` in ``protocols/semantic_types_protocols.py``
    is untouched; it is removed in PLT-1660.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    import pyarrow as pa


@runtime_checkable
class ExtensionTypeConverter(Protocol):
    """Protocol for Arrow/Polars extension-type-backed converters.

    Declares the full contract for a converter that maps between Python
    objects and their Arrow extension type storage representation. This
    protocol is Arrow I/O only — hashing is not a converter responsibility.

    Attributes:
        extension_name: Fully-qualified Python class name used as the
            ``ARROW:extension:name`` metadata value (e.g. ``"pathlib.Path"``).
            Must be unique across all registered converters. By convention
            equals the FQCN, but any unique string is valid.
        extension_metadata: Category tag encoded as ``ARROW:extension:metadata``
            (e.g. ``b"orcapod.dataclass"``). Used by the registry to locate
            the right category handler at read time. May be ``None``.
        storage_type: The underlying Arrow ``pa.DataType`` used for physical
            storage (e.g. ``pa.large_string()``, ``pa.binary(16)``,
            ``pa.struct(...)``). Not used as an identity signal — identity
            is determined solely by ``extension_name``.
        python_type: The Python class this converter handles.
    """

    @property
    def extension_name(self) -> str:
        """Fully-qualified Python class name; stored as ``ARROW:extension:name``."""
        ...

    @property
    def extension_metadata(self) -> bytes | None:
        """Category tag; stored as ``ARROW:extension:metadata``. May be ``None``."""
        ...

    @property
    def storage_type(self) -> pa.DataType:
        """Underlying Arrow storage type. Any ``pa.DataType`` is valid."""
        ...

    @property
    def python_type(self) -> type:
        """The Python class this converter handles."""
        ...

    def python_to_storage(self, value: Any) -> Any:
        """Convert a Python value to its Arrow storage representation.

        Args:
            value: A Python object of type ``python_type``.

        Returns:
            A value suitable for use as an Arrow scalar or array element
            of type ``storage_type``.
        """
        ...

    def storage_to_python(self, storage_value: Any) -> Any:
        """Convert an Arrow storage value back to a Python object.

        Args:
            storage_value: A scalar or array element of type ``storage_type``.

        Returns:
            A Python object of type ``python_type``.
        """
        ...
