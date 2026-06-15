"""Arrow/Polars extension type system for orcapod.

This subpackage provides the registry and protocol for logical types that map
between Python objects and their Arrow/Polars extension type representation.

Built-in registrations (``LogicalPath``, ``LogicalUPath``, ``LogicalUUID``) are
wired into ``DataContext`` via ``contexts/data/v0.1.json``. The primary access
paths for the default registry are:

- ``get_default_context().logical_type_registry``
- ``get_default_logical_type_registry()`` (from ``orcapod.contexts``)
"""

from __future__ import annotations

from .protocols import LogicalTypeProtocol, LogicalTypeFactoryProtocol
from .registry import LogicalTypeRegistry, make_arrow_extension_type, make_polars_extension_type
from .schema_walker import ExtensionTypeInfo, walk_field, walk_schema
from .database_hooks import apply_extension_types, register_discovered_extensions
from .type_utils import _extract_leaf_classes

__all__ = [
    "LogicalTypeProtocol",
    "LogicalTypeFactoryProtocol",
    "LogicalTypeRegistry",
    "make_arrow_extension_type",
    "make_polars_extension_type",
    # PLT-1654
    "ExtensionTypeInfo",
    "walk_schema",
    "walk_field",
    # PLT-1655
    "register_discovered_extensions",
    "apply_extension_types",
    # PLT-1672
    "_extract_leaf_classes",
]
