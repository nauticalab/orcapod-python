from typing import Any, Protocol, runtime_checkable

from orcapod.protocols.core_protocols.streams import StreamProtocol


@runtime_checkable
class SourceProtocol(StreamProtocol, Protocol):
    """
    Protocol for root sources — streams with no upstream dependencies that
    expose provenance identity and optional field resolution.

    A SourceProtocol is a StreamProtocol where:
    - ``source`` is always ``None`` (no upstream pod)
    - ``upstreams`` is always empty
    - ``source_id`` provides a canonical name for registry and provenance
    - ``resolve_field`` enables lookup of individual field values by record id
    """

    @property
    def source_id(self) -> str: ...

    def resolve_field(self, record_id: str, field_name: str) -> Any: ...

    def to_config(self) -> dict[str, Any]:
        """Serialize source configuration to a JSON-compatible dict."""
        ...

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> "SourceProtocol":
        """Reconstruct a source instance from a config dict."""
        ...
