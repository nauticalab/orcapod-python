from __future__ import annotations

from collections.abc import Collection, Mapping
from typing import Any, Protocol, TYPE_CHECKING, runtime_checkable

from orcapod.protocols.db_connector_protocol import ColumnInfo, DBConnectorProtocol

if TYPE_CHECKING:
    import pyarrow as pa


@runtime_checkable
class ArrowDatabaseProtocol(Protocol):
    def add_record(
        self,
        record_path: tuple[str, ...],
        record_id: str,
        record: pa.Table,
        skip_duplicates: bool = False,
        flush: bool = False,
    ) -> None: ...

    def add_records(
        self,
        record_path: tuple[str, ...],
        records: pa.Table,
        record_id_column: str | None = None,
        skip_duplicates: bool = False,
        flush: bool = False,
    ) -> None: ...

    def get_record_by_id(
        self,
        record_path: tuple[str, ...],
        record_id: str,
        record_id_column: str | None = None,
        flush: bool = False,
    ) -> pa.Table | None: ...

    def get_all_records(
        self,
        record_path: tuple[str, ...],
        record_id_column: str | None = None,
    ) -> pa.Table | None:
        """Retrieve all records for a given path as a stream."""
        ...

    def get_records_by_ids(
        self,
        record_path: tuple[str, ...],
        record_ids: Collection[str],
        record_id_column: str | None = None,
        flush: bool = False,
    ) -> pa.Table | None: ...

    def get_records_with_column_value(
        self,
        record_path: tuple[str, ...],
        column_values: Collection[tuple[str, Any]] | Mapping[str, Any],
        record_id_column: str | None = None,
        flush: bool = False,
    ) -> pa.Table | None: ...

    def flush(self) -> None:
        """Flush any buffered writes to the underlying storage."""
        ...

    def to_config(self) -> dict[str, Any]:
        """Serialize database configuration to a JSON-compatible dict.

        The returned dict must include a ``"type"`` key identifying the
        database implementation (e.g., ``"delta_table"``, ``"in_memory"``).
        """
        ...

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> ArrowDatabaseProtocol:
        """Reconstruct a database instance from a config dict."""
        ...


class MetadataCapableProtocol(Protocol):
    def set_metadata(
        self,
        record_path: tuple[str, ...],
        metadata: Mapping[str, Any],
        merge: bool = True,
    ) -> None: ...

    def get_metadata(
        self,
        record_path: tuple[str, ...],
    ) -> Mapping[str, Any]: ...

    def get_supported_metadata_schema(self) -> Mapping[str, type]: ...

    def validate_metadata(
        self,
        metadata: Mapping[str, Any],
    ) -> Collection[str]: ...


class ArrowDatabaseWithMetadataProtocol(
    ArrowDatabaseProtocol, MetadataCapableProtocol, Protocol
):
    """A protocol that combines ArrowDatabaseProtocol with metadata capabilities."""

    pass


@runtime_checkable
class DatabaseRegistryProtocol(Protocol):
    """Protocol for a database config registry used during pipeline save/load.

    At **save time** (``to_config``), sources that embed database references
    call ``register()`` to deduplicate configs and get a stable key string.

    At **load time** (``from_config``), sources call ``resolve()`` with the
    key string to retrieve the original config dict for reconstruction.

    The existing ``DatabaseRegistry`` class satisfies this protocol.
    """

    def register(self, config: dict[str, Any]) -> str:
        """Register a database config and return a stable registry key."""
        ...

    def resolve(self, key: str) -> dict[str, Any]:
        """Return the config dict for a previously registered key."""
        ...


__all__ = [
    "ArrowDatabaseProtocol",
    "ArrowDatabaseWithMetadataProtocol",
    "ColumnInfo",
    "DBConnectorProtocol",
    "DatabaseRegistryProtocol",
    "MetadataCapableProtocol",
]
