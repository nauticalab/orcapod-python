# src/orcapod/hooks.py
"""Post-run hook types for function pods.

Defines the payload, status, and hook configuration types used by the
post-run hook mechanism on function pods. Import these when writing or
registering hooks.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Callable
from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from orcapod.protocols.core_protocols import DataProtocol, TagProtocol


class InvocationStatus(str, Enum):
    """Status of a single function pod invocation.

    Attributes:
        COMPUTED: The function was invoked and produced a fresh result.
        HIT: The result was served from a pod-level database cache
            (``CachedFunctionPod``).
        ERROR: The function raised an exception.
    """

    COMPUTED = "computed"
    HIT = "hit"
    ERROR = "error"


@dataclasses.dataclass(frozen=True)
class RunStats:
    """Timing and status information for a single pod invocation.

    Attributes:
        duration_ms: Wall-clock milliseconds elapsed during the compute-or-lookup
            step only. Hook execution time is not included.
        status: Whether the result was freshly computed, a cache hit, or an error.
        started_at: UTC timestamp when the invocation started.
        finished_at: UTC timestamp when the compute-or-lookup step completed,
            before hooks fire. Together with ``started_at`` it gives the raw
            compute time, independent of hook overhead.
        error: The exception raised, if ``status == ERROR``; ``None`` otherwise.
    """

    duration_ms: float
    status: InvocationStatus
    started_at: datetime
    finished_at: datetime
    error: Exception | None = None


@dataclasses.dataclass(frozen=True)
class PodContext:
    """Identity information about the pod that produced a result.

    Attributes:
        label: Human-readable pod label (``pod.label``); ``None`` if not set.
        pod_hash: Hex-string content hash of the pod (``pod.content_hash().to_string()``).
            Changes when the underlying function code or version changes.
    """

    label: str | None
    pod_hash: str


@dataclasses.dataclass(frozen=True)
class PostRunPayload:
    """Payload passed to every post-run hook after a pod invocation.

    Attributes:
        record_id_hash: String form of ``output.datagram_uuid`` — the same UUID
            used as the primary key when the result is stored in a backing database.
            ``None`` when ``output`` is ``None`` (filtered row or error).
        tag: The input tag for this invocation. Treat as read-only.
        input: The input data for this invocation. Treat as read-only.
        output: The output data; ``None`` if the function filtered the row out or raised.
        stats: Timing and status bundle.
        pod: Identity of the pod that produced this result.
    """

    record_id_hash: str | None
    tag: TagProtocol
    input: DataProtocol
    output: DataProtocol | None
    stats: RunStats
    pod: PodContext


PostRunHookFn = Callable[["PostRunPayload"], None]
"""A plain hook callable: ``(PostRunPayload) -> None``.

Defaults to fail-loud on error (exceptions propagate).
"""


@dataclasses.dataclass(frozen=True)
class HookConfig:
    """Hook callable with explicit error-handling behaviour.

    Use this instead of a plain callable when you want the hook to log and
    continue on failure rather than propagating the exception.

    Attributes:
        fn: The hook callable.
        on_error: ``"raise"`` (default) propagates exceptions; ``"log"`` logs at
            WARNING level and continues.

    Example:
        pod.add_post_run_hook(HookConfig(fn=my_hook, on_error="log"))
    """

    fn: PostRunHookFn
    on_error: Literal["raise", "log"] = "raise"


PostRunHook = PostRunHookFn | HookConfig
"""A hook is either a plain callable (fail-loud) or a ``HookConfig`` wrapper."""
