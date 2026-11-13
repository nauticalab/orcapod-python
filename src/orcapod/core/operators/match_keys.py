"""Hashable match keys for the async join operators (ITL-1044).

The sync ``Join`` and ``SemiJoin`` match rows with a Polars join on the key
columns. The async paths index rows in a dictionary, so each key value must be
hashable and equal exactly when Polars treats two values as equal. Measured on
the Polars join:

* a list matches a list of the same elements in the same order; an empty list
  is not a null list; nulls inside a list or struct compare equal;
* a struct matches field by field; a null struct is not a struct of nulls;
* NaN equals NaN (also inside a list); ``0.0`` equals ``-0.0``;
* a null never matches, whatever its type (the sync join keys on the column
  together with a constant column, which makes a null list or struct unequal
  to another one too).

Values are read from the row's Arrow column (the storage of a logical column),
never from ``as_dict()``, and never through ``repr`` or ``str``. Temporal values
(time, timestamp, duration, date) are frozen from their integer storage value,
because ``as_py()`` drops nanoseconds.
"""

from __future__ import annotations

import math
from typing import TYPE_CHECKING, Any

from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")

# The frozen form of a NaN value: equal to itself, as NaN is in Polars.
_NAN = object()


def _freeze_scalar(scalar: Any) -> object:  # pyright: ignore[reportAny, reportExplicitAny]  # pyarrow's scalar stubs lack the nested accessors
    """Return an Arrow scalar, nested values included, in a hashable form.

    Structs become tuples of (field name, value), lists tuples, a map a tuple
    of (key, value) pairs, NaN one shared sentinel and a null ``None``; a
    temporal value is its integer storage value.

    Args:
        scalar: A scalar of a key column, or of a list item or struct field.

    Returns:
        The hashable form of the scalar.
    """
    if not scalar.is_valid:  # pyright: ignore[reportAny]
        return None
    if isinstance(scalar, pa.ExtensionScalar):
        # A null extension scalar has no storage scalar, so validity comes first.
        return _freeze_scalar(scalar.value)  # pyright: ignore[reportAny]
    arrow_type = scalar.type
    if pa.types.is_struct(arrow_type):
        return tuple(
            (field.name, _freeze_scalar(scalar[field.name])) for field in arrow_type
        )
    if pa.types.is_map(arrow_type):
        return tuple(
            # Entry fields may carry any names: read them by position.
            (_freeze_scalar(item[0]), _freeze_scalar(item[1]))
            for item in scalar.values
        )
    if (
        pa.types.is_list(arrow_type)
        or pa.types.is_large_list(arrow_type)
        or pa.types.is_fixed_size_list(arrow_type)
    ):
        return tuple(_freeze_scalar(item) for item in scalar.values)
    if pa.types.is_temporal(arrow_type) and not pa.types.is_interval(arrow_type):
        return scalar.value
    value = scalar.as_py()
    if isinstance(value, float) and math.isnan(value):
        return _NAN
    return value


def frozen_scalar(scalar: "pa.Scalar[Any]") -> object | None:  # pyright: ignore[reportExplicitAny]
    """Return an Arrow scalar's match key, as the sync ``Join`` equality has it.

    Args:
        scalar: One value of a key column.

    Returns:
        ``None`` for a null (a null primitive, list or struct never matches,
        because the sync join also keys on a constant column), otherwise the
        frozen value.
    """
    return _freeze_scalar(scalar)
