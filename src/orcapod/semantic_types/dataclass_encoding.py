# src/orcapod/semantic_types/dataclass_encoding.py
"""
Dataclass <-> Arrow struct encoding for Orcapod.

Encodes Python dataclasses as Arrow structs with a ``__type`` sentinel field
carrying the fully-qualified class name. Decoding uses a three-tier fallback:
import -> registry -> synthesize.
"""

from __future__ import annotations

import dataclasses
import importlib
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

# Validates fully-qualified class names like "my_module.sub.MyClass"
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
    """
    key = f"{cls.__module__}.{cls.__qualname__}"
    _DATACLASS_REGISTRY[key] = cls
    return cls
