# src/orcapod/protocols/core_protocols/side_effect_pod.py
"""SideEffectPodProtocol — protocol for side-effect pods."""
from __future__ import annotations

from collections.abc import Collection
from typing import TYPE_CHECKING, Protocol, runtime_checkable

from orcapod.protocols.hashing_protocols import PipelineElementProtocol

if TYPE_CHECKING:
    from orcapod.protocols.core_protocols.pod import ArgumentGroup, PodProtocol
    from orcapod.protocols.core_protocols.streams import StreamProtocol
    from orcapod.side_effects import SideEffectPodConfig, SideEffectPodStream
    from orcapod.types import ColumnConfig, Schema


@runtime_checkable
class SideEffectPodProtocol(PipelineElementProtocol, Protocol):
    """Protocol for side-effect pods.

    A side-effect pod wraps a ``(data: T, ctx: InvocationContext) -> None``
    callable. Its ``process()`` returns a pass-through stream. Output schema
    equals input schema.
    """

    @property
    def pod_config(self) -> "SideEffectPodConfig":
        """Pod-level configuration."""
        ...

    def process(
        self, *streams: "StreamProtocol", label: str | None = None
    ) -> "SideEffectPodStream":
        """Invoke the pod on input streams, returning a pass-through stream."""
        ...

    def output_schema(
        self,
        *streams: "StreamProtocol",
        columns: "ColumnConfig | dict[str, object] | None" = None,
        all_info: bool = False,
    ) -> "tuple[Schema, Schema]":
        """Return the input stream's schema unchanged (pass-through)."""
        ...

    def argument_symmetry(
        self, streams: "Collection[StreamProtocol]"
    ) -> "ArgumentGroup": ...
