from typing import Any, Protocol, runtime_checkable

from orcapod.protocols.core_protocols.pod import PodProtocol


@runtime_checkable
class OperatorPodProtocol(PodProtocol, Protocol):
    """
    PodProtocol that performs operations on streams.

    This is a base protocol for pods that perform operations on streams.
    TODO: add a method to map out source relationship
    """

    def to_config(self) -> dict[str, Any]:
        """Serialize this operator to a JSON-compatible config dict."""
        ...

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> "OperatorPodProtocol":
        """Reconstruct an operator from a config dict."""
        ...
