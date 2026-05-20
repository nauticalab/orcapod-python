"""ExecutionContext — stub type for pipeline execution configuration.

Full definition (PipelineConfig integration, distributed execution) is
deferred to a follow-up issue.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from orcapod.types import PipelineConfig


@dataclass(frozen=True)
class ExecutionContext:
    """Minimal placeholder for pipeline execution configuration.

    Full definition including ``PipelineConfig`` integration is deferred
    to a follow-up issue.

    Args:
        config: Optional pipeline-level execution configuration.
    """

    config: "PipelineConfig | None" = None
