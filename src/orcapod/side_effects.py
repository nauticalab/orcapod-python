# src/orcapod/side_effects.py
"""SideEffectPod — pass-through pipeline node for side effects.

Provides ``SideEffectPod``, ``SideEffectPodStream``, ``SideEffectJobNode``,
``InvocationContext``, ``InvocationHashConfig``, ``SideEffectPodConfig``,
and the ``side_effect_pod``, ``sink_pod``, ``tap_pod`` decorators.
"""
from __future__ import annotations

import asyncio
import base64
import dataclasses
import datetime
import logging
from collections.abc import Callable, Collection, Iterator, Sequence
from typing import TYPE_CHECKING, Any, Literal

from orcapod.core.base import TraceableBase
from orcapod.core.streams.base import StreamBase
from orcapod.core.tracker import DEFAULT_TRACKER_MANAGER
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa
    from orcapod.channels import ReadableChannel, WritableChannel
    from orcapod.protocols.core_protocols import (
        DataProtocol,
        StreamProtocol,
        TagProtocol,
        TrackerManagerProtocol,
    )
    from orcapod.protocols.database_protocols import ArrowDatabaseProtocol
    from orcapod.protocols.observability_protocols import ExecutionObserverProtocol
    from orcapod.types import ColumnConfig, ContentHash, Schema
else:
    pa = LazyModule("pyarrow")

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# InvocationHashConfig
# ---------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class InvocationHashConfig:
    """Controls how ``InvocationContext.invocation_hash`` is serialized.

    Args:
        encoding: Output encoding — ``"hex"`` (default), ``"base64"``, or
            ``"binary"`` (falls back to hex in string contexts).
        component_length: Bytes of raw digest to use per component. ``None``
            means full digest length. Applied identically to every
            ``::``-separated component.
    """

    encoding: Literal["hex", "base64", "binary"] = "hex"
    component_length: int | None = None


def _serialize_component(content_hash: ContentHash, config: InvocationHashConfig) -> str:
    """Serialize one ``ContentHash`` component per ``InvocationHashConfig``.

    Args:
        content_hash: The hash to serialize.
        config: Encoding and truncation config.

    Returns:
        A string representation of the (optionally truncated) digest.
    """
    raw: bytes = content_hash.digest
    if config.component_length is not None:
        raw = raw[: config.component_length]
    if config.encoding == "base64":
        return base64.b64encode(raw).decode("ascii")
    # "hex" and "binary" both produce hex strings in string contexts
    return raw.hex()


# ---------------------------------------------------------------------------
# SideEffectPodConfig
# ---------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class SideEffectPodConfig:
    """Configuration for a ``SideEffectPod``.

    Args:
        track_completion: If ``True`` (default), skip re-delivery for inputs
            that previously completed successfully.
        drop_on_failure: If ``True`` (default), drop rows whose delivery
            raised an exception from the downstream output.
        on_error: ``"raise"`` (default) re-raises delivery exceptions;
            ``"log"`` logs at WARNING and continues.
        hash_config: Controls encoding of ``InvocationContext.invocation_hash``.
    """

    track_completion: bool = True
    drop_on_failure: bool = True
    on_error: Literal["raise", "log"] = "raise"
    hash_config: InvocationHashConfig = dataclasses.field(
        default_factory=InvocationHashConfig
    )


# ---------------------------------------------------------------------------
# InvocationContext
# ---------------------------------------------------------------------------


class InvocationContext:
    """Per-invocation context passed to every side-effect pod function.

    Carries the deterministic ``invocation_hash`` string and metadata about
    the current delivery. ``format_id()`` re-serializes the hash with a
    caller-supplied ``InvocationHashConfig`` without recomputing.

    Public fields are read-only by convention (no public setters).

    Args:
        invocation_hash: Serialized compound hash string.
        pod_name: ``pod.label`` of the invoking pod.
        pod_content_hash: ``pod.content_hash().to_string()``.
        pipeline_run_id: The current pipeline run identifier, or ``None``
            for standalone / lazy pipelines.
    """

    def __init__(
        self,
        invocation_hash: str,
        pod_name: str,
        pod_content_hash: str,
        pipeline_run_id: str | None,
        _pipeline_hash_ch: ContentHash,
        _full_input_packet_hash_ch: ContentHash,
        _hash_config: InvocationHashConfig,
        _track_completion: bool,
    ) -> None:
        self.invocation_hash = invocation_hash
        self.pod_name = pod_name
        self.pod_content_hash = pod_content_hash
        self.pipeline_run_id = pipeline_run_id
        self._pipeline_hash_ch = _pipeline_hash_ch
        self._full_input_packet_hash_ch = _full_input_packet_hash_ch
        self._hash_config = _hash_config
        self._track_completion = _track_completion

    def format_id(self, config: InvocationHashConfig | None = None) -> str:
        """Return ``'orcapod-{hash}'`` with an optional format override.

        Re-serializes from the stored raw ``ContentHash`` components — no
        recomputation. Uses ``config`` if supplied, otherwise the pod's own
        ``InvocationHashConfig``.

        Args:
            config: Optional encoding/truncation override.

        Returns:
            A string of the form ``"orcapod-{component1}::{component2}"``
            (two components when ``track_completion=True``) or
            ``"orcapod-{c1}::{c2}::{run_id}"`` (three components when
            ``track_completion=False`` and ``pipeline_run_id`` is not ``None``).
        """
        cfg = config or self._hash_config
        c1 = _serialize_component(self._pipeline_hash_ch, cfg)
        c2 = _serialize_component(self._full_input_packet_hash_ch, cfg)
        if not self._track_completion and self.pipeline_run_id is not None:
            parts = f"{c1}::{c2}::{self.pipeline_run_id}"
        else:
            parts = f"{c1}::{c2}"
        return f"orcapod-{parts}"
