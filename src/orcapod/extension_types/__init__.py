"""Arrow/Polars extension type system for orcapod.

This subpackage provides the registry and protocol for logical types that map
between Python objects and their Arrow/Polars extension type representation.

Built-in registrations (``LogicalPath``, ``LogicalUPath``, ``LogicalUUID``) are
wired into ``DataContext`` via ``contexts/data/v0.1.json``. Use
``get_default_context().type_converter.register_python_class()`` to register new
types, ``register_logical_type_factory()`` to add factories, and
``apply_extension_types()`` to re-wrap Arrow tables with their registered extension types.

``DataclassLogicalTypeFactory`` provides automatic registration for Python dataclasses:
register it with a ``LogicalTypeRegistry`` and any dataclass used in a ``FunctionPod``
will be auto-registered on pod declaration.

``PydanticLogicalTypeFactory`` provides automatic registration for pydantic v2
``BaseModel`` subclasses. Requires the optional ``pydantic`` extra.
"""

from __future__ import annotations

from .protocols import LogicalTypeProtocol, LogicalTypeFactoryProtocol
from .registry import LogicalTypeRegistry, make_arrow_extension_type, make_polars_extension_type
from .schema_walker import ExtensionTypeInfo, walk_field, walk_schema
from .database_hooks import apply_extension_types, register_discovered_extensions
from .dataclass_logical_type_factory import DATACLASS_CATEGORY, DataclassLogicalType, DataclassLogicalTypeFactory
from .pydantic_logical_type_factory import PYDANTIC_CATEGORY, PydanticLogicalType, PydanticLogicalTypeFactory
from .file_type import LogicalFile  # ITL-450
from .directory_type import LogicalDirectory  # ITL-451
from .numpy_type import LogicalNumpyArray  # ITL-460
from .pandas_type import LogicalPandasDataFrame, LogicalPandasSeries  # PLT-1869

# ITL-459, ITL-468, ITL-470 — SpikeInterface support (optional; requires pip install orcapod[spikeinterface])
try:
    from .spikeinterface_types import (
        LogicalSIRecording,
        SIRecordingHandler,
        LogicalSISorting,
        SISortingHandler,
        LogicalSIMotion,
        SIMotionHandler,
        register_spikeinterface_types,
    )
    _SI_AVAILABLE = True
except ImportError:
    _SI_AVAILABLE = False

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
    "DataclassLogicalTypeFactory",
    # PLT-1731
    "PYDANTIC_CATEGORY",
    "PydanticLogicalType",
    "PydanticLogicalTypeFactory",
    # ITL-450
    "LogicalFile",
    # ITL-451
    "LogicalDirectory",
    # ITL-460
    "LogicalNumpyArray",
    # ITL-459, ITL-468, ITL-470 (conditional — only present when spikeinterface is installed)
    *(
        [
            "LogicalSIRecording", "SIRecordingHandler",
            "LogicalSISorting", "SISortingHandler",
            "LogicalSIMotion", "SIMotionHandler",
            "register_spikeinterface_types",
        ] if _SI_AVAILABLE else []
    ),
    # PLT-1869
    "LogicalPandasDataFrame",
    "LogicalPandasSeries",
]
