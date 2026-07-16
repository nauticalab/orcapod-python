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
from .side_effects import (
    InvocationContext,
    InvocationHashConfig,
    SideEffectPod,
    SideEffectPodConfig,
    side_effect_pod,
    sink_pod,
    tap_pod,
)
from .core.side_effect_function import (
    SideEffectFunctionPod,
    SideEffectFunctionNode,
    SideEffectFunctionJobNode,
    side_effect_function_pod,
)
from .core.nodes.source_node import SourceNode
from .pipeline import Pipeline, PipelineJob
# Subpackage re-exports for clean public API
from . import databases  # noqa: F401
from . import nodes  # noqa: F401
from . import operators  # noqa: F401
from . import sources  # noqa: F401
from . import streams  # noqa: F401
from . import types  # noqa: F401

# Stable type aliases — preferred over importing directly from pathlib/upath/uuid.
#
# These aliases are the recommended way to reference these types in orcapod user code.
# Even if an upstream library is renamed or restructured, these symbols remain stable
# at ``orcapod.Path``, ``orcapod.UPath``, ``orcapod.UUID``, ``orcapod.File``, and
# ``orcapod.Directory``. Their Arrow extension types are registered under the
# ``orcapod.*`` namespace (``"orcapod.path"``, ``"orcapod.upath"``, ``"orcapod.uuid"``,
# ``"orcapod.file"``, ``"orcapod.directory"``), so on-disk identity is also decoupled
# from upstream module paths.
from pathlib import Path
from upath import UPath
from uuid import UUID
from orcapod.extension_types.file_type import File
from orcapod.extension_types.directory_type import Directory

# Extension registration API (ITL-473)
from .extensions import OrcapodExtension, register_extension

# Post-run hook types (ITL-523)
from .hooks import (
    HookConfig,
    InvocationStatus,
    PodContext,
    PostRunHook,
    PostRunHookFn,
    PostRunPayload,
    RunStats,
)

__all__ = [
    "DEFAULT_CONFIG",
    "DisplayConfig",
    "HashingConfig",
    "OrcapodConfig",
    "load_config",
    "FunctionPod",
    "function_pod",
    "InvocationContext",
    "InvocationHashConfig",
    "SideEffectPod",
    "SideEffectPodConfig",
    "side_effect_pod",
    "sink_pod",
    "tap_pod",
    "SideEffectFunctionPod",
    "SideEffectFunctionNode",
    "SideEffectFunctionJobNode",
    "side_effect_function_pod",
    "Pipeline",
    "PipelineJob",
    "SourceNode",
    "databases",
    "nodes",
    "operators",
    "sources",
    "streams",
    "types",
    # Stable type aliases
    "Directory",
    "File",
    "Path",
    "UPath",
    "UUID",
    # Extension registration API (ITL-473)
    "OrcapodExtension",
    "register_extension",
    # Post-run hook types (ITL-523)
    "HookConfig",
    "InvocationStatus",
    "PodContext",
    "PostRunHook",
    "PostRunHookFn",
    "PostRunPayload",
    "RunStats",
]


