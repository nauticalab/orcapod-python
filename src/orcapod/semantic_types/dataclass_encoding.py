# src/orcapod/semantic_types/dataclass_encoding.py
"""
Dataclass <-> Arrow struct encoding for Orcapod.

Encodes Python dataclasses as Arrow structs with a `__type` sentinel field
carrying the fully-qualified class name. Decoding uses a three-tier fallback:
import -> registry -> synthesize.
"""

from __future__ import annotations

import dataclasses
import logging
import re
import typing
from typing import TYPE_CHECKING, Any

from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")

logger = logging.getLogger(__name__)

DATACLASS_TYPE_FIELD = "__type"
DATACLASS_TYPE_PREFIX = "dataclass:"

# Validates fully-qualified class names like "my_module.sub.MyClass".
# Used by struct_dict_to_dataclass (tier-1 import path).
_FQCN_RE = re.compile(r"^[A-Za-z_]\w*(\.[A-Za-z_]\w*)+$")

# Process-global registry for tier-2 reconstruction.
# Populated via register_dataclass(); persists for the process lifetime.
_DATACLASS_REGISTRY: dict[str, type] = {}


def register_dataclass(cls: type) -> type:
    """Register a dataclass for tier-2 reconstruction by fully-qualified name.

    Can be used as a class decorator or called directly. Returns ``cls``
    unchanged so it works transparently as a decorator.

    Args:
        cls: A Python dataclass type to register.

    Returns:
        The same ``cls`` that was passed in.

    Raises:
        TypeError: If ``cls`` is not a dataclass type.
    """
    if not dataclasses.is_dataclass(cls) or not isinstance(cls, type):
        raise TypeError(f"{cls!r} is not a dataclass type")
    key = f"{cls.__module__}.{cls.__qualname__}"
    _DATACLASS_REGISTRY[key] = cls
    return cls


def has_dataclass_type_sentinel(arrow_type: pa.DataType) -> bool:
    """Return `True` if `arrow_type` is a struct with a `__type` string field.

    Accepts both `pa.large_string()` and `pa.string()` for compatibility
    with data written by older Arrow versions.

    Args:
        arrow_type: Any PyArrow data type.

    Returns:
        True if `arrow_type` is a struct containing a `__type: (large_)string`
        field.
    """
    if not pa.types.is_struct(arrow_type):
        return False
    for i in range(arrow_type.num_fields):
        field = arrow_type.field(i)
        if field.name == DATACLASS_TYPE_FIELD:
            return pa.types.is_large_string(field.type) or pa.types.is_string(field.type)
    return False


def dataclass_to_arrow_struct_type(
    cls: type,
    converter: Any,
) -> pa.StructType:
    """Derive the Arrow struct type for a dataclass class.

    The resulting struct has `__type: large_string` as its first field,
    followed by one field per dataclass field. Field types are resolved via
    `converter` (a `UniversalTypeConverter`), so nested dataclasses
    produce nested structs automatically once the converter has the dataclass
    branch wired in.

    Args:
        cls: A Python dataclass type.
        converter: A `UniversalTypeConverter` instance used for field type
            resolution.

    Returns:
        A `pa.StructType` with `__type` as the first field.

    Raises:
        TypeError: If `cls` is not a dataclass type.
    """
    if not dataclasses.is_dataclass(cls) or not isinstance(cls, type):
        raise TypeError(f"{cls!r} is not a dataclass type")

    hints = typing.get_type_hints(cls)
    fields: list[pa.Field] = [pa.field(DATACLASS_TYPE_FIELD, pa.large_string())]
    for f in dataclasses.fields(cls):
        arrow_type = converter.python_type_to_arrow_type(hints[f.name])
        fields.append(pa.field(f.name, arrow_type))
    return pa.struct(fields)
