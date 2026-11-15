"""Which None at a nested position a read restores (ITL-1144).

A None inside a container column (a list or set element, a dict value, a user-class
field) is stored faithfully as an Arrow null. Reading the column back to Python restores
it only where the position's type is a primitive or a user class: a container read as an
empty container and any other logical type failed. Until the read restores every such
None (the ITL-1144 follow-up), a declared type with such a position is refused, and a
stored null at such a position raises when it is read, so no wrong value gets out.
"""

from __future__ import annotations

import types
import typing
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, get_args, get_origin

if TYPE_CHECKING:
    import pyarrow as pa

IsUserClass = Callable[[str], bool]
"""Whether an Arrow extension name is the logical type of a user class (a dataclass,
an ``orcapod.dataclass`` or a pydantic model). The caller supplies it, so this module
does not import the user-class logical types, which import it."""

_FOLLOW_UP = "ITL-1144"


class NestedNoneNotRestorableError(ValueError):
    """A None at a nested position cannot be read back faithfully (ITL-1144)."""


def none_is_restorable(arrow_type: pa.DataType, is_user_class: IsUserClass) -> bool:
    """Whether a read restores a null at a nested position of ``arrow_type`` as None.

    A user class (dataclass, ``orcapod.dataclass``, pydantic model), every scalar type
    and an Arrow map (its decoder passes a null through; a Python ``dict`` is stored as
    a list of key/value structs, never as a map) are restored; a list or struct reads
    as an empty value, and any other logical type fails.

    Args:
        arrow_type: The Arrow type of the nested position.
        is_user_class: Whether an extension name is a user class's logical type.

    Returns:
        True when a null there is read back as None.
    """
    import pyarrow as pa

    if isinstance(arrow_type, pa.ExtensionType):
        return is_user_class(arrow_type.extension_name)
    return not (
        pa.types.is_list(arrow_type)
        or pa.types.is_large_list(arrow_type)
        or pa.types.is_fixed_size_list(arrow_type)
        or pa.types.is_list_view(arrow_type)
        or pa.types.is_large_list_view(arrow_type)
        or pa.types.is_struct(arrow_type)
    )


def _is_optional(annotation: Any) -> bool:
    """Whether ``annotation`` is a union that includes None."""
    origin = get_origin(annotation)
    return (origin is typing.Union or origin is types.UnionType) and type(
        None
    ) in get_args(annotation)


def require_restorable_none(
    annotation: Any, arrow_type: pa.DataType, is_user_class: IsUserClass
) -> None:
    """Refuse a nested position declared ``X | None`` whose None a read cannot restore.

    Args:
        annotation: The declared type of the nested position (a list or set element, a
            dict value, a user-class field).
        arrow_type: The Arrow type ``annotation`` registers as.
        is_user_class: Whether an extension name is a user class's logical type.

    Raises:
        NestedNoneNotRestorableError: If ``annotation`` admits None and a stored None
            there would not be read back as None.
    """
    if _is_optional(annotation) and not none_is_restorable(arrow_type, is_user_class):
        raise NestedNoneNotRestorableError(
            f"{annotation} is not supported inside a container or as a class field yet: "
            f"a None there is stored but not read back faithfully (it would read as an "
            f"empty value or fail). Declare it without '| None', or hold the value in a "
            f"dataclass and make that optional (e.g. list[MyData | None]). Follow-up: "
            f"{_FOLLOW_UP}."
        )


def unrestorable_none_error(arrow_type: pa.DataType) -> NestedNoneNotRestorableError:
    """The error for a stored null read at a nested position it cannot be restored at.

    Args:
        arrow_type: The Arrow type of the nested position holding the null.

    Returns:
        The error to raise.
    """
    return NestedNoneNotRestorableError(
        f"a stored None inside a container, at a position of Arrow type {arrow_type}, "
        f"cannot be read back faithfully yet (it would read as an empty value or fail). "
        f"Remove the None before the value is put in a container (e.g. DropNulls on "
        f"the column before GroupBy or Batch), or hold the value in a dataclass. "
        f"Follow-up: {_FOLLOW_UP}."
    )


def with_context(
    context: str, error: NestedNoneNotRestorableError
) -> NestedNoneNotRestorableError:
    """The same refusal with the column or field it concerns put first.

    Args:
        context: What the caller knows the refusal concerns, e.g. ``"Column 'v'"``.
        error: The refusal raised below the caller.

    Returns:
        A new error whose message starts with ``context``.
    """
    return NestedNoneNotRestorableError(f"{context}: {error}")
