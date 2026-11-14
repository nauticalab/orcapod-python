"""A shared tag key that differs only in nullability between join inputs (ITL-829).

Developer decision, November 13, 2026: ``Join``, ``MergeJoin`` and ``SemiJoin`` accept
a shared tag key that is ``T`` in one input and ``T | None`` in another. A null key
matches nothing, so no joined row can hold a null there: the key is nullable in a join's
output only when it is nullable in every input. ``SemiJoin``'s output is the left input's
schema as it is. Only tag schemas go through this module; data columns never do.
"""

from __future__ import annotations

from orcapod.core.operators.drop_nulls import DropNulls
from orcapod.types import Schema

# DropNulls' own rule for removing ``None`` from a type, reused, never copied.
_narrow_schema = DropNulls._narrow_schema  # pyright: ignore[reportPrivateUsage]  # shared on purpose with DropNulls


def key_nullable(*nullable: bool) -> bool:
    """Return whether a shared tag key is nullable in a join's output.

    Args:
        *nullable: The key's nullable flag in each input that has it.

    Returns:
        True only when the key is nullable in every input.
    """
    return all(nullable)


def reconcile_key_nullability(first: Schema, second: Schema) -> tuple[Schema, Schema]:
    """Give a shared tag key that differs only in nullability its non-nullable type.

    For each name in both tag schemas whose types differ but are equal once
    ``None`` is removed, both schemas get that type without ``None``. Every other
    pair is left as it is, so a real type difference still raises where the
    schemas are combined, with the message it has always had.

    Args:
        first: One input's tag schema.
        second: The other input's tag schema.

    Returns:
        The two tag schemas, with those keys non-nullable in both.
    """
    shared = [name for name in first if name in second]
    first_narrow = _narrow_schema(first, shared)
    second_narrow = _narrow_schema(second, shared)
    names = [
        name
        for name in shared
        if first[name] != second[name] and first_narrow[name] == second_narrow[name]
    ]
    if not names:
        return first, second
    return _narrow_schema(first, names), _narrow_schema(second, names)
