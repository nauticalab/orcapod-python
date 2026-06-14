"""Arrow/Polars extension type system for orcapod.

This subpackage provides the registry and protocol for converters that map
between Python objects and their Arrow extension type storage representation.

The module-level `default_extension_type_registry` instance is the process default.
Built-in registrations (`Path`, `UPath`, `UUID`) are added by PLT-1656.
`DataContext` wiring is added by PLT-1660.
"""

from __future__ import annotations

from .protocols import ExtensionTypeConverter
from .registry import ExtensionTypeRegistry

default_extension_type_registry = ExtensionTypeRegistry()

__all__ = [
    "ExtensionTypeConverter",
    "ExtensionTypeRegistry",
    "default_extension_type_registry",
]
