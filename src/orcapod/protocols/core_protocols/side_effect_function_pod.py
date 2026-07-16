"""Protocol for SideEffectFunctionPod."""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from orcapod.protocols.hashing_protocols import PipelineElementProtocol

if TYPE_CHECKING:
    from orcapod.side_effects import SideEffectPodConfig
    from orcapod.protocols.core_protocols.streams import StreamProtocol
    from orcapod.types import Schema


@runtime_checkable
class SideEffectFunctionPodProtocol(PipelineElementProtocol, Protocol):
    """Protocol for side-effect function pods.

    Hybrid of ``FunctionPodProtocol`` and ``SideEffectPodProtocol``:
    receives per-row ``InvocationContext``, produces a downstream data stream.
    """

    @property
    def pod_config(self) -> "SideEffectPodConfig": ...

    @property
    def input_data_schema(self) -> "Schema": ...

    @property
    def output_data_schema(self) -> "Schema": ...

    def process(self, *streams: "StreamProtocol", label: str | None = None) -> Any: ...

    def output_schema(
        self,
        *streams: "StreamProtocol",
        columns: Any = None,
        all_info: bool = False,
    ) -> "tuple[Schema, Schema]": ...

    def argument_symmetry(self, streams: Any) -> Any: ...
