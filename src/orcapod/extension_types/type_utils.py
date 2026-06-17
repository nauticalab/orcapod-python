"""Utility helpers for Python type annotation inspection and FQCN import.

Used by the write-side registration trigger to extract leaf Python classes from
complex generic annotations like ``list[dict[A, list[B]]]``, and by logical type
factories to import classes from fully-qualified class names.
"""

from __future__ import annotations

import importlib
import typing
from typing import Any, Iterator


def _extract_leaf_classes(annotation: Any) -> Iterator[type]:
    """Recursively yield all concrete leaf Python classes from a type annotation.

    Unwraps generic aliases (``list[T]``, ``dict[K, V]``, ``Optional[T]``,
    ``Union[A, B]``, ``A | B``, etc.) using ``typing.get_origin`` and
    ``typing.get_args`` and yields every non-generic leaf found. ``NoneType``
    that appears as a generic argument (from ``Optional`` and
    ``Union[..., None]`` / ``T | None``) is skipped — callers see only the
    concrete types. When ``type(None)`` is passed directly as the annotation,
    it is yielded as-is.

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


def _walk_fqcn(fqcn: str) -> Any:
    """Walk a fully-qualified class name and return the resolved object.

    Tries module prefixes from longest to shortest, then walks the remaining
    parts as attribute accesses. For example:

    - ``"mypackage.sub.MyClass"`` → import ``mypackage.sub``, then
      ``getattr(module, "MyClass")``.
    - ``"mypackage.sub.Outer.Inner"`` → import ``mypackage.sub``, then
      ``getattr(module, "Outer")``, then ``getattr(Outer, "Inner")``.

    Does **not** validate the type of the resolved object — callers are
    responsible for checking that the result is the expected kind of object
    (e.g. a dataclass, a ``BaseModel`` subclass).

    Args:
        fqcn: Fully-qualified name, e.g. ``"mypackage.sub.MyClass"``.

    Returns:
        The resolved Python object.

    Raises:
        ImportError: If no valid module+attribute split can be found.
    """
    parts = fqcn.split(".")
    if len(parts) < 2:
        raise ImportError(f"Cannot import from FQCN {fqcn!r}: no module separator found.")

    for i in range(len(parts) - 1, 0, -1):
        module_path = ".".join(parts[:i])
        attr_parts = parts[i:]
        try:
            module = importlib.import_module(module_path)
        except ImportError:
            continue
        obj: Any = module
        try:
            for attr in attr_parts:
                obj = getattr(obj, attr)
        except AttributeError:
            continue
        return obj

    raise ImportError(
        f"Cannot import from FQCN {fqcn!r}: no valid module+attribute path found."
    )
