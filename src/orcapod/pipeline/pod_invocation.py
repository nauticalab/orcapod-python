"""PodInvocation — minimal recording primitives for pipeline invocations.

These lightweight value objects capture a pod-plus-streams invocation at
recording time.  Their ``content_hash()`` is structurally identical to the
corresponding compiled node's ``content_hash()``, which means the same hash
key can be used in both ``_invocation_lut`` and ``_persistent_node_map``.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any

from orcapod.core.base import ContentIdentifiableBase

if TYPE_CHECKING:
    from orcapod.protocols.core_protocols import (
        FunctionPodProtocol,
        OperatorPodProtocol,
        SideEffectPodProtocol,
        StreamProtocol,
    )


class PodInvocation(ContentIdentifiableBase):
    """Abstract recording primitive for a pod applied to input streams.

    ``PodInvocation.identity_structure()`` returns
    ``(pod, pod.argument_symmetry(input_streams))``, which mirrors
    ``StreamBase.identity_structure()`` and therefore guarantees that the
    hash of a ``PodInvocation`` equals the hash of the corresponding
    compiled node (``FunctionNode`` / ``OperatorNode``).

    Args:
        pod: The pod being invoked (function or operator).
        input_streams: Tuple of upstream streams passed to the pod.
        label: Optional display label for the resulting compiled node.
    """

    def __init__(
        self,
        pod: Any,
        input_streams: "tuple[StreamProtocol, ...]",
        label: str | None = None,
    ) -> None:
        super().__init__()
        self._pod = pod
        self._input_streams = tuple(input_streams)
        self._label = label

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def pod(self) -> Any:
        """The pod being invoked."""
        return self._pod

    @property
    def input_streams(self) -> "tuple[StreamProtocol, ...]":
        """Upstream streams passed to this invocation."""
        return self._input_streams

    @property
    def label(self) -> str | None:
        """Optional display label for the compiled node."""
        return self._label

    # ------------------------------------------------------------------
    # Identity — mirrors StreamBase.identity_structure()
    # ------------------------------------------------------------------

    def identity_structure(self) -> Any:
        """Return ``(pod, pod.argument_symmetry(input_streams))``.

        This matches the identity structure of the corresponding compiled
        node, ensuring hash equality between invocation and node.
        """
        return (self._pod, self._pod.argument_symmetry(self._input_streams))

    def pipeline_identity_structure(self) -> Any:
        """Return same as ``identity_structure()``."""
        return self.identity_structure()


class FunctionInvocation(PodInvocation):
    """Invocation of a function pod against a single input stream.

    Args:
        pod: A ``FunctionPodProtocol`` instance.
        input_streams: Tuple with exactly one stream.
        label: Optional display label.

    Raises:
        ValueError: If ``input_streams`` does not contain exactly one element.
    """

    def __init__(
        self,
        pod: "FunctionPodProtocol",
        input_streams: "tuple[StreamProtocol, ...]",
        label: str | None = None,
    ) -> None:
        if len(input_streams) != 1:
            raise ValueError(
                f"FunctionInvocation requires exactly 1 input stream; "
                f"got {len(input_streams)}."
            )
        super().__init__(pod=pod, input_streams=input_streams, label=label)


class OperatorInvocation(PodInvocation):
    """Invocation of an operator pod against one or more input streams.

    Args:
        pod: An ``OperatorPodProtocol`` instance.
        input_streams: Tuple with one or more streams.
        label: Optional display label.

    Raises:
        ValueError: If ``input_streams`` is empty.
    """

    def __init__(
        self,
        pod: "OperatorPodProtocol",
        input_streams: "tuple[StreamProtocol, ...]",
        label: str | None = None,
    ) -> None:
        if len(input_streams) == 0:
            raise ValueError("OperatorInvocation requires at least 1 input stream.")
        super().__init__(pod=pod, input_streams=input_streams, label=label)


class SideEffectInvocation(PodInvocation):
    """Invocation of a side-effect pod against exactly one input stream.

    Args:
        pod: A ``SideEffectPodProtocol`` instance.
        input_streams: Tuple with exactly one stream.
        label: Optional display label.

    Raises:
        ValueError: If ``input_streams`` does not contain exactly one element.
    """

    def __init__(
        self,
        pod: "SideEffectPodProtocol",
        input_streams: "tuple[StreamProtocol, ...]",
        label: str | None = None,
    ) -> None:
        if len(input_streams) != 1:
            raise ValueError(
                f"SideEffectInvocation requires exactly 1 input stream; "
                f"got {len(input_streams)}."
            )
        super().__init__(pod=pod, input_streams=input_streams, label=label)
