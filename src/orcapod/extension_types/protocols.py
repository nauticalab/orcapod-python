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

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    import polars as pl
    import pyarrow as pa

    from orcapod.extension_types.registry import LogicalTypeRegistry


@dataclass(frozen=True)
class ResolutionContext:
    """Immutable context for cycle detection during ``LogicalType`` resolution.

    Passed through the factory call chain so that circular references are
    detected across factory boundaries (e.g. a dataclass ``A`` containing a
    field of type ``B`` which itself contains a field of type ``A``).

    Updates always produce new instances via ``dataclasses.replace(...)``.

    Attributes:
        visited_types: Python types currently being resolved on the call stack.
        visited_arrow_names: Arrow extension names currently being resolved
            on the call stack.
    """

    visited_types: frozenset[type] = frozenset()
    visited_arrow_names: frozenset[str] = frozenset()


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

        For built-in types, use an ``orcapod.*`` prefix (e.g. ``"orcapod.uuid"``). Any unique
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
    """Protocol for factories that synthesize or reconstruct ``LogicalTypeProtocol`` instances.

    Bridges two directions: the write path (``create_for_python_type`` — synthesizes a
    ``LogicalTypeProtocol`` from a Python class) and the read path
    (``reconstruct_from_arrow`` — reconstructs a ``LogicalTypeProtocol`` from Arrow schema
    metadata).

    ``supports_class`` is the intended write-side gate for the registry's MRO walk — the
    registry consults it to confirm a factory handles a given Python type before committing.
    Read-side dispatch (via ``"category"`` metadata) bypasses ``supports_class`` entirely —
    the category string is fully definitive.

    ``registry`` and ``context`` are optional on both factory methods so that simple
    factories that don't recurse can ignore them. Factories that handle recursive types
    (e.g. nested dataclasses) can use ``registry`` to register sub-types as a side effect and
    ``context`` to propagate cycle detection across factory boundaries.

    This protocol is ``@runtime_checkable``, consistent with ``LogicalTypeProtocol``.
    """

    def supports_class(self, python_type: type) -> bool:
        """Return ``True`` if this factory handles *python_type* (write-side gate).

        Intended to be called by the registry during the MRO walk after a base
        class registered for this factory is found in the target type's MRO.
        The first factory (in registration order) that returns ``True`` wins.

        Read-side dispatch via ``"category"`` metadata does NOT call this method.

        Args:
            python_type: The concrete Python class being resolved.

        Returns:
            ``True`` if this factory can synthesize a ``LogicalTypeProtocol``
            for *python_type*; ``False`` otherwise.
        """
        ...

    def reconstruct_from_arrow(
        self,
        arrow_extension_name: str,
        storage_type: pa.DataType,
        metadata: dict[str, Any],
        registry: LogicalTypeRegistry | None = None,
        context: ResolutionContext = ResolutionContext(),
    ) -> LogicalTypeProtocol:
        """Reconstruct a LogicalType from Arrow schema metadata (read path).

        Called by the registry when a schema walk encounters an extension type
        whose metadata ``"category"`` value matches this factory's registered
        category. All Arrow schema information is already known.

        Args:
            arrow_extension_name: The Arrow extension type name from the schema.
            storage_type: The underlying Arrow storage type.
            metadata: Full parsed metadata JSON dict. Always contains ``"category"``.
            registry: The ``LogicalTypeRegistry`` to register sub-types into as a
                side effect. ``None`` if the caller has no registry.
            context: Immutable cycle-detection context. Updated with the current
                Arrow extension name before recursing.

        Returns:
            A fully constructed ``LogicalTypeProtocol`` ready for registration.

        Raises:
            ValueError: If this factory cannot reconstruct a type for the given name.
        """
        ...

    def create_for_python_type(
        self,
        python_type: type,
        registry: LogicalTypeRegistry | None = None,
        context: ResolutionContext = ResolutionContext(),
    ) -> LogicalTypeProtocol:
        """Synthesize a LogicalType for the given Python class (write path).

        Called by the registry when pod declaration encounters an unregistered
        class whose MRO intersects a base registered for this factory and
        ``supports_class`` returned ``True``. The factory derives all Arrow
        metadata (extension name, storage type, metadata dict) from the
        Python class itself.

        Args:
            python_type: The concrete Python class to synthesize a LogicalType for.
            registry: The ``LogicalTypeRegistry`` to register sub-types into as a
                side effect. ``None`` if the caller has no registry.
            context: Immutable cycle-detection context. Updated with *python_type*
                before recursing into field resolution.

        Returns:
            A fully constructed ``LogicalTypeProtocol`` ready for registration.

        Raises:
            ValueError: If this factory cannot construct a type for the given class.
        """
        ...
