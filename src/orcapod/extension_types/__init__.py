"""Arrow/Polars extension type system for orcapod.

This subpackage provides the registry and protocol for logical types that map
between Python objects and their Arrow/Polars extension type representation.

Built-in registrations (``LogicalPath``, ``LogicalUPath``, ``LogicalUUID``) are
wired into ``DataContext`` via ``contexts/data/v0.1.json``. The logical type
registry is accessible via ``get_default_context().type_converter._logical_type_registry``.

``DataclassHandlerFactory`` provides automatic registration for Python dataclasses:
register it with a ``LogicalTypeRegistry`` and any dataclass used in a ``FunctionPod``
will be auto-registered on pod declaration.
"""

from __future__ import annotations

from .protocols import LogicalTypeProtocol, LogicalTypeFactoryProtocol
from .registry import LogicalTypeRegistry, make_arrow_extension_type, make_polars_extension_type
from .schema_walker import ExtensionTypeInfo, walk_field, walk_schema
from .database_hooks import apply_extension_types, register_discovered_extensions
from .dataclass_handler import DATACLASS_CATEGORY, DataclassLogicalType, DataclassHandlerFactory

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
    # PLT-1705
    "DATACLASS_CATEGORY",
    "DataclassLogicalType",
    "DataclassHandlerFactory",
]
