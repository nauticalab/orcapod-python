"""Base class for all orcapod logical types."""

from __future__ import annotations

from typing import TYPE_CHECKING

from orcapod.types import DataType

if TYPE_CHECKING:
    import pyarrow as pa


class BaseLogicalType:
    """Shared base for all logical types.

    Provides default ``NotImplementedError`` implementations for structural
    projection methods. Logical types that support ``pick`` or ``index``
    override these methods.

    Subclasses implement ``get_arrow_extension_type``; its extension name and
    serialized metadata are the type's identifier.
    """

    def get_arrow_extension_type(self) -> "pa.ExtensionType":
        """Return the Arrow extension type for this logical type.

        Its extension name and serialized metadata are the type's identifier.

        Raises:
            NotImplementedError: Always; every logical type implements this.
        """
        raise NotImplementedError(
            f"{type(self).__name__} must implement get_arrow_extension_type."
        )

    def readable_extension_metadata(self) -> tuple[bytes, ...]:
        """Return every stored extension metadata value this type can read.

        The default is the type's own metadata, plus the ``also_reads`` values
        when the Arrow extension type was built by ``make_arrow_extension_type``.
        Override to declare older identifiers whose stored layout this version
        still decodes.

        Returns:
            The readable metadata values, the current one first.
        """
        extension_type = self.get_arrow_extension_type()
        declared = getattr(extension_type, "readable_metadata", None)
        if declared is not None:
            return tuple(declared)
        return (extension_type.__arrow_ext_serialize__(),)

    def pick_field(self, key: str) -> DataType:
        """Return the Python type of field ``key``.

        Args:
            key: Name of the field to project into.

        Returns:
            The Python type of the requested field.

        Raises:
            NotImplementedError: If this logical type does not support keyed access.
        """
        raise NotImplementedError(
            f"{type(self).__name__} does not support pick (keyed field access)."
        )

    def index_element(self) -> DataType:
        """Return the Python element type for positional list access.

        Returns:
            The Python type of elements in this list-like logical type.

        Raises:
            NotImplementedError: If this logical type does not support positional access.
        """
        raise NotImplementedError(
            f"{type(self).__name__} does not support index (positional access)."
        )
