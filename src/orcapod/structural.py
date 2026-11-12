"""``orcapod.dataclass``: dataclasses identified by name and fields (ITL-972).

An ordinary ``@dataclass`` is identified by its exact class: a pod annotated with it
accepts that class object and no other. A class made with ``@orcapod.dataclass`` is
**structural**: Orcapod identifies it by its name and its fields (names, types and
order). Two such classes with the same name and fields are one type: a pod annotated
with one accepts a column of the other, their instances compare equal when their field
values are equal, and stored values can be read back without the class being defined.

Orcapod never converts a value from one class to another. A function annotated with a
structural class receives the object that arrives, which may be an instance of another
class of the same type: a structural class promises its fields, not its methods.
``from_instance`` is the explicit conversion.

See ``docs/concepts/your-own-data-types.md``.
"""

from __future__ import annotations

import dataclasses
import typing
from collections.abc import Callable
from typing import Any, TypeVar, overload

import orcapod.logical_types.structural_class as _structural
import orcapod.logical_types.user_class_identity as _uci

T = TypeVar("T")

__all__ = ["dataclass"]


@overload
def dataclass(cls: type[T], /) -> type[T]: ...


@overload
def dataclass(
    *, name: str | None = None, **options: Any
) -> Callable[[type[T]], type[T]]: ...


@typing.dataclass_transform()
def dataclass(
    cls: type[T] | None = None, /, *, name: str | None = None, **options: Any
) -> type[T] | Callable[[type[T]], type[T]]:
    """Define a dataclass that Orcapod identifies by its name and fields.

    Use it in place of ``dataclasses.dataclass``, bare or with keywords. The result is
    a real dataclass (``dataclasses.fields``, ``asdict`` and ``replace`` work) with two
    differences. ``==`` compares field values across classes of the same type: an
    instance equals an instance of another ``orcapod.dataclass`` with the same name
    and fields when their field values are equal (unless ``eq=False`` is passed or the
    class defines ``__eq__`` itself). Equality and hashing always cover all stored
    fields: ``field(compare=False)`` and ``field(hash=...)`` are not accepted. And the class has ``from_instance``, which builds
    an instance from an instance of another dataclass with the same fields.

    Args:
        cls: The class, when used as a bare decorator.
        name: The name the type is identified by. Default: the class name. It must not
            be empty, contain ``@``, ``;`` or whitespace, or start with ``orcapod.``.
        **options: Keyword options of ``dataclasses.dataclass`` (``frozen``,
            ``slots``, ``kw_only``, ``order``, ...). ``init=False`` is not allowed.

    Returns:
        The dataclass, or a decorator that returns it.

    Raises:
        TypeError: If the class is already a dataclass or a pydantic model,
            ``init=False`` is passed, or a field sets ``compare=`` or ``hash=`` so
            that equality or hashing would not cover exactly the stored fields.
        ValueError: If the name is invalid.

    Example:
        >>> import orcapod as op
        >>> @op.dataclass
        ... class Point:
        ...     x: int
        ...     y: float
    """
    if options.get("init", True) is not True:
        raise TypeError(
            "orcapod.dataclass does not accept init=False: Orcapod builds instances "
            "through the generated __init__."
        )

    def decorate(target: type[T]) -> type[T]:
        """Make ``target`` a dataclass and mark it as a structural class."""
        if not isinstance(target, type):
            raise TypeError(f"orcapod.dataclass decorates a class, got {target!r}.")
        if "__dataclass_fields__" in vars(target):
            raise TypeError(
                f"{target.__qualname__} is already a dataclass. Use @orcapod.dataclass "
                f"in place of @dataclasses.dataclass, not on top of it."
            )
        if _uci.class_kind(target) == _uci.KIND_PYDANTIC:
            raise TypeError(
                f"{target.__qualname__} is a pydantic model. orcapod.dataclass makes "
                f"dataclasses only."
            )
        declared = _uci.validate_name(name if name is not None else target.__name__)
        own_eq = "__eq__" in vars(target)
        made = dataclasses.dataclass(target, **options)
        _require_structural_comparison(made)
        _structural.finish_structural_class(
            made, declared, structural_equality=options.get("eq", True) and not own_eq
        )
        return made

    if cls is None:
        return decorate
    return decorate(cls)


def _require_structural_comparison(cls: type) -> None:
    """Raise unless equality and hashing of a new dataclass cover its stored fields.

    Two classes of one structural type must compare and hash alike, including a class
    built from stored metadata, which knows the fields and nothing else. So a stored
    field (``init=True``) always takes part, a field that is not stored never does, and
    no field chooses its own ``hash=``.

    Args:
        cls: The dataclass ``orcapod.dataclass`` has just created.

    Raises:
        TypeError: Naming the first field that breaks the rule, and why.
    """
    for field in dataclasses.fields(cls):
        if field.hash is not None:
            problem = f"sets hash={field.hash!r}"
        elif field.init and not field.compare:
            problem = "sets compare=False"
        elif not field.init and field.compare:
            problem = (
                "has init=False, so it is not stored, but takes part in comparison"
            )
        else:
            continue
        raise TypeError(
            f"Field {field.name!r} of the orcapod.dataclass {cls.__qualname__} "
            f"{problem}. An orcapod.dataclass is identified by its fields, and two "
            f"classes with the same name and fields must compare and hash alike, so "
            f"equality and hashing cover exactly the stored fields. Remove the "
            f"compare= or hash= argument (declare an init=False field with "
            f"compare=False), or use an ordinary dataclass."
        )
