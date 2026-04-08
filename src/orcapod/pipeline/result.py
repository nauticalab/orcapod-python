"""Result type returned by pipeline orchestrators."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from orcapod.protocols.core_protocols import PacketProtocol, TagProtocol


@dataclass
class OrchestratorResult:
    """Result of an orchestrator run.

    Attributes:
        node_outputs: Mapping from graph node to its computed (tag, packet)
            pairs. Empty when ``materialize_results=False``.
    """

    node_outputs: dict[Any, list[tuple["TagProtocol", "PacketProtocol"]]] = field(
        default_factory=dict
    )
