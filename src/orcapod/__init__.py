from .config import (
    DEFAULT_CONFIG,
    DisplayConfig,
    HashingConfig,
    OrcapodConfig,
    load_config,
)
from .core.function_pod import (
    FunctionPod,
    function_pod,
)
from .core.nodes.source_node import SourceNode
from .pipeline import Pipeline, PipelineJob
from .semantic_types.dataclass_encoding import register_dataclass

# Subpackage re-exports for clean public API
from . import databases  # noqa: F401
from . import nodes  # noqa: F401
from . import operators  # noqa: F401
from . import sources  # noqa: F401
from . import streams  # noqa: F401
from . import types  # noqa: F401

__all__ = [
    "DEFAULT_CONFIG",
    "DisplayConfig",
    "HashingConfig",
    "OrcapodConfig",
    "load_config",
    "FunctionPod",
    "function_pod",
    "Pipeline",
    "PipelineJob",
    "SourceNode",
    "register_dataclass",
    "UUID_ARROW_TYPE",
    "UUID_STRUCT_ARROW_TYPE",
    "databases",
    "nodes",
    "operators",
    "sources",
    "streams",
    "types",
]


def __getattr__(name: str) -> object:
    """Lazy resolution for module-level constants that depend on pyarrow.

    Delegates UUID Arrow type constants to ``orcapod.types`` so that importing
    ``orcapod`` does not eagerly load the pyarrow C extension.
    """
    if name in ("UUID_ARROW_TYPE", "UUID_STRUCT_ARROW_TYPE"):
        from . import types as _types

        value = getattr(_types, name)
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
