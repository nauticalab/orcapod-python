"""Canonical order for the unordered containers ``dict`` and ``set``.

A ``dict[K, V]`` value is stored as ``large_list<struct<key, value>>`` and a plain
``set[T]`` as ``large_list<T>``. A dict and a set are order independent, so the order
they are stored in must not come from insertion or iteration order (ITL-905). Orcapod
puts them in canonical order where a Python value becomes Arrow: in the converter, and
in a datagram built from Python values. An Arrow table is always hashed exactly as it
is stored; a ``list<struct<key, value>>`` that arrives already in Arrow form is an
ordered list at the Arrow level, and its order is part of its hash.

The order is defined on storage values (what the converter produces):

* ``None`` sorts first.
* ``bool``, ``int``, ``float``, ``Decimal``: numeric. A NaN of any numeric type has
  no order and is rejected. ``-0.0`` and ``0.0`` compare equal, so within one dict or
  set they collide (and raise, see below); a dict keyed by ``-0.0`` and one keyed by
  ``0.0`` store different bytes and hash differently, like any float value.
* ``str``: Unicode code point (never locale). ``bytes``: unsigned bytewise.
* ``date``, ``datetime`` (by instant), ``time``, ``timedelta``: natural order.

Keys or elements that cannot be ordered against each other, and two keys or elements
with the same storage value, raise ``CanonicalOrderError``.
"""

from __future__ import annotations

import math
import numbers
import types
import typing
from collections.abc import Callable
from decimal import Decimal
from itertools import pairwise
from typing import Any, TypeVar, cast, get_args, get_origin

from orcapod.errors import CanonicalOrderError

_T = TypeVar("_T")

# A sort key: ``(0,)`` for None, ``(1, value)`` otherwise.
_Rank = tuple[Any, ...]


def _key_rank(key: object) -> _Rank:
    """Return the sort key of one storage key or set element.

    Raises:
        CanonicalOrderError: If the key is a NaN of any numeric type (Python ``float``,
            NumPy floating scalars, ``Decimal``).
    """
    if key is None:
        return (0,)
    if _is_nan(key):
        raise CanonicalOrderError("a NaN key or set element has no canonical order")
    return (1, key)


def _is_nan(value: object) -> bool:
    """Return whether ``value`` is a NaN of any numeric type.

    Integers are never NaN and are not converted (a large ``int`` does not fit a float).
    """
    if isinstance(value, Decimal):
        return value.is_nan()
    if isinstance(value, numbers.Real) and not isinstance(value, numbers.Integral):
        return math.isnan(value)
    return False


def _sorted_by_rank(items: list[tuple[_Rank, _T]]) -> list[_T]:
    """Sort ``(rank, payload)`` pairs by rank and return the payloads.

    Raises:
        CanonicalOrderError: If two ranks cannot be compared or are equal.
    """
    try:
        items = sorted(items, key=lambda item: item[0])
    except TypeError as exc:
        raise CanonicalOrderError(f"values cannot be ordered: {exc}") from exc
    for (previous, _), (current, payload) in pairwise(items):
        if previous == current:
            raise CanonicalOrderError(f"duplicate key or set element: {payload!r}")
    return [payload for _, payload in items]


def sort_dict_entries(entries: list[dict[str, _T]]) -> list[dict[str, _T]]:
    """Return converted entries ``{"key": ..., "value": ...}`` in canonical key order.

    Args:
        entries: The entries of one dict value, as the converter produces them.

    Returns:
        A new list holding the same entries, sorted by key.

    Raises:
        CanonicalOrderError: For a NaN key, keys that cannot be ordered against each
            other, or two keys with the same storage value.
    """
    return _sorted_by_rank([(_key_rank(entry["key"]), entry) for entry in entries])


def sort_set_elements(elements: list[_T]) -> list[_T]:
    """Return the storage elements of one set value in canonical order.

    Args:
        elements: The converted elements of a set.

    Returns:
        A new list holding the same elements, sorted.

    Raises:
        CanonicalOrderError: For a NaN element, elements that cannot be ordered against
            each other, or two elements with the same storage value.
    """
    return _sorted_by_rank([(_key_rank(element), element) for element in elements])


def canonical_python_value(
    value: _T,
    python_type: object,
    key_converter: Callable[[Any], Callable[[Any], object]],
) -> _T:
    """Return ``value`` with every dict in it in canonical key order.

    Walks ``python_type`` through ``dict``, ``list`` and ``Optional``. A dict whose order
    changes, or whose values change, is returned as a new dict; everything else is
    returned as given. The input is never mutated. Values of other types (including
    dataclass and pydantic objects) are returned unchanged.

    Args:
        value: A Python value of type ``python_type``.
        python_type: The value's declared Python type.
        key_converter: Returns the Python-to-storage converter for a key type, so keys
            are ordered by their storage value, as the converter stores them.

    Returns:
        ``value`` itself when nothing changes, otherwise a canonically ordered copy.

    Raises:
        CanonicalOrderError: For a key that has no canonical order (NaN), keys that
            cannot be ordered, or two keys that convert to the same storage key.
    """
    if value is None:
        return value
    origin = cast(object, get_origin(python_type))
    args = cast(tuple[object, ...], get_args(python_type))
    if origin is typing.Union or origin is types.UnionType:  # pyright: ignore[reportDeprecated]  -- detects Optional[T]
        non_none = [a for a in args if a is not type(None)]
        if len(non_none) != 1:
            return value
        return canonical_python_value(value, non_none[0], key_converter)
    if origin is dict and len(args) == 2 and isinstance(value, dict):
        original = cast(dict[object, object], value)
        to_storage = key_converter(args[0])
        items = [
            (
                _key_rank(to_storage(k)),
                (k, canonical_python_value(v, args[1], key_converter)),
            )
            for k, v in original.items()
        ]
        ordered = _sorted_by_rank(items)
        unchanged = all(
            new_k is old_k and new_v is old_v
            for (new_k, new_v), (old_k, old_v) in zip(ordered, original.items())
        )
        return value if unchanged else cast(_T, dict(ordered))
    if origin is list and len(args) == 1 and isinstance(value, list):
        original_items = cast(list[object], value)
        new_items = [
            canonical_python_value(v, args[0], key_converter) for v in original_items
        ]
        if all(new is old for new, old in zip(new_items, original_items)):
            return value
        return cast(_T, new_items)
    return value
