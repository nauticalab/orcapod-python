from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from orcapod.protocols.core_protocols.streams import StreamProtocol

if TYPE_CHECKING:
    from orcapod.protocols.database_protocols import DatabaseRegistryProtocol


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

    def to_config(
        self, db_registry: DatabaseRegistryProtocol | None = None
    ) -> dict[str, Any]:
        """Serialize source configuration to a JSON-compatible dict.

        Args:
            db_registry: Optional registry for deduplicating embedded database
                configs at save time.  Sources that do not embed database
                references ignore this parameter.
        """
        ...

    @classmethod
    def from_config(
        cls,
        config: dict[str, Any],
        db_registry: DatabaseRegistryProtocol | None = None,
    ) -> "SourceProtocol":
        """Reconstruct a source instance from a config dict.

        Args:
            config: Dict as produced by ``to_config``.
            db_registry: Optional registry for resolving embedded database
                config keys at load time.  Sources that do not embed database
                references ignore this parameter.
        """
        ...
