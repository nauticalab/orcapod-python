# src/orcapod/invocation.py
"""Invocation identity types for orcapod pipeline elements.

Provides ``InvocationHashConfig``, ``InvocationContext``, and the internal
``_serialize_component`` helper. These types are shared between
``side_effects.py`` and ``hooks.py`` and are extracted here to avoid
circular imports.
"""

from __future__ import annotations

import base64
import dataclasses
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from orcapod.types import ContentHash


@dataclasses.dataclass(frozen=True)
class InvocationHashConfig:
    """Controls how ``InvocationContext.invocation_hash`` is serialized.

    Args:
        encoding: Output encoding — ``"hex"`` (default) or ``"base64"``.
        component_length: Bytes of raw digest to use per component. ``None``
            means full digest length. Applied identically to every
            ``::``-separated component.
    """

    encoding: Literal["hex", "base64"] = "hex"
    component_length: int | None = None


def _serialize_component(content_hash: ContentHash, config: InvocationHashConfig) -> str:
    """Serialize one ``ContentHash`` component per ``InvocationHashConfig``.

    The method name is always included as a prefix (e.g. ``"arrow_v2.1:abcd1234"``).
    Only the digest bytes are subject to truncation via ``component_length``.

    Args:
        content_hash: The hash to serialize.
        config: Encoding and truncation config.

    Returns:
        A string of the form ``"{method}:{encoded_digest}"`` where the digest
        is optionally truncated then encoded as hex or base64.
    """
    raw = content_hash.digest
    if config.component_length is not None:
        raw = raw[:config.component_length]
    if config.encoding == "base64":
        encoded = base64.b64encode(raw).decode("ascii")
    else:
        encoded = raw.hex()
    return f"{content_hash.method}:{encoded}"


class InvocationContext:
    """Per-invocation context describing a single pod call.

    Carries a deterministic ``invocation_hash`` and metadata about the
    current delivery. ``invocation_hash`` is a computed property that
    delegates to ``format_id()`` with the pod's default
    ``InvocationHashConfig``. ``format_id()`` can be called with a custom
    config to re-serialize without recomputation.

    Public fields are read-only by convention (no public setters).

    Available on:
    - Side-effect pod functions (injected as ``ctx`` argument).
    - Function pod post-run hooks (via ``PostRunPayload.invocation_context``).

    Args:
        pod_name: ``pod.label`` of the invoking pod.
        pipeline_run_id: The current pipeline run identifier, or ``None``
            for standalone / lazy pipelines.
    """

    def __init__(
        self,
        pod_name: str,
        pipeline_run_id: str | None,
        _pipeline_hash_ch: ContentHash,
        _record_id_hash_ch: ContentHash,
        _hash_config: InvocationHashConfig,
        _track_completion: bool,
    ) -> None:
        self.pod_name = pod_name
        self.pipeline_run_id = pipeline_run_id
        self._pipeline_hash_ch = _pipeline_hash_ch
        self._record_id_hash_ch = _record_id_hash_ch
        self._hash_config = _hash_config
        self._track_completion = _track_completion

    @property
    def invocation_hash(self) -> str:
        """Serialized invocation hash — delegates to ``format_id()``."""
        return self.format_id()

    def format_id(self, config: InvocationHashConfig | None = None) -> str:
        """Return the invocation hash string with an optional format override.

        Serializes the stored ``ContentHash`` components. Uses ``config``
        if supplied, otherwise the pod's own ``InvocationHashConfig``.

        Args:
            config: Optional encoding/truncation override.

        Returns:
            A string of the form ``"{component1}::{component2}"``
            (two components when ``track_completion=True``) or
            ``"{c1}::{c2}::{run_id}"`` (three components when
            ``track_completion=False`` and ``pipeline_run_id`` is not ``None``).
            Each component is ``"{method}:{encoded_digest}"``.
        """
        cfg = config or self._hash_config
        c1 = _serialize_component(self._pipeline_hash_ch, cfg)
        c2 = _serialize_component(self._record_id_hash_ch, cfg)
        if not self._track_completion and self.pipeline_run_id is not None:
            return f"{c1}::{c2}::{self.pipeline_run_id}"
        return f"{c1}::{c2}"
