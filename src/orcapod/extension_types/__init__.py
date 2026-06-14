"""Arrow/Polars extension type system for orcapod.

This subpackage provides the registry and protocol for logical types that map
between Python objects and their Arrow/Polars extension type representation.

The module-level ``default_logical_type_registry`` instance is the process default.
Built-in registrations (``Path``, ``UPath``, ``UUID``) are added by PLT-1656.
``DataContext`` wiring is added by PLT-1660.
"""

from __future__ import annotations

from .protocols import LogicalType
from .registry import LogicalTypeRegistry, make_arrow_extension_type, make_polars_extension_type
from .schema_walker import ExtensionTypeInfo, walk_field, walk_schema

default_logical_type_registry = LogicalTypeRegistry()

__all__ = [
    "LogicalType",
    "LogicalTypeRegistry",
    "make_arrow_extension_type",
    "make_polars_extension_type",
    "default_logical_type_registry",
    # PLT-1654
    "ExtensionTypeInfo",
    "walk_schema",
    "walk_field",
]
