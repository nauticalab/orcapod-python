"""Utility helpers for Python type annotation inspection.

Used by the write-side registration trigger to extract leaf Python classes from
complex generic annotations like ``list[dict[A, list[B]]]``.
"""

from __future__ import annotations

import typing
from typing import Any, Iterator


def _extract_leaf_classes(annotation: Any) -> Iterator[type]:
    """Recursively yield all concrete leaf Python classes from a type annotation.

    Unwraps generic aliases (``list[T]``, ``dict[K, V]``, ``Optional[T]``,
    ``Union[A, B]``, etc.) using ``typing.get_origin`` and ``typing.get_args``
    and yields every non-generic leaf found. ``NoneType`` that appears as a
    generic argument (from ``Optional`` and ``Union[..., None]``) is skipped —
    callers see only the concrete types. When ``type(None)`` is passed directly
    as the annotation, it is yielded as-is.

    Non-type, non-generic values (e.g. unresolved string annotations) are
    silently skipped.

    Args:
        annotation: A Python type or generic alias to inspect.

    Yields:
        Concrete Python ``type`` objects found at leaf positions.

    Examples:
        >>> list(_extract_leaf_classes(list[int]))
        [<class 'int'>]
        >>> set(_extract_leaf_classes(dict[str, list[MyClass]]))
        {<class 'str'>, <class 'MyClass'>}
    """
    origin = typing.get_origin(annotation)

    if origin is None:
        # Not a generic alias. Yield only if it is a plain type.
        if isinstance(annotation, type):
            yield annotation
        return

    # Generic alias — recurse into every type argument, skipping NoneType.
    for arg in typing.get_args(annotation):
        if arg is type(None):
            continue
        yield from _extract_leaf_classes(arg)
