"""Base class for all orcapod logical types."""
from __future__ import annotations

from orcapod.types import DataType


class BaseLogicalType:
    """Shared base for all logical types.

    Provides default ``NotImplementedError`` implementations for structural
    projection methods. Logical types that support ``pick`` or ``index``
    override these methods.
    """

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
