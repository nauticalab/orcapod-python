"""Base class for all orcapod logical types."""
from __future__ import annotations


class BaseLogicalType:
    """Shared base for all logical types.

    Provides default ``NotImplementedError`` implementations for structural
    projection methods. Logical types that support ``pick`` or ``index``
    override these methods.
    """

    def pick_field(self, key: str) -> type:
        """Return the Python type of field ``key``.

        Args:
            key: Name of the field to project into.

        Returns:
            The Python type of the requested field.

        Raises:
            NotImplementedError: Until implemented for this logical type.
        """
        raise NotImplementedError(
            f"{type(self).__name__} does not yet support pick (keyed field access). "
            "Support for this extension type is planned for a future issue."
        )

    def index_element(self) -> type:
        """Return the Python element type for positional list access.

        Returns:
            The Python type of elements in this list-like logical type.

        Raises:
            NotImplementedError: Until implemented for this logical type.
        """
        raise NotImplementedError(
            f"{type(self).__name__} does not yet support index (positional access). "
            "Support for this extension type is planned for a future issue."
        )
