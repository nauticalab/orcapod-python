from typing import Any, Protocol, TYPE_CHECKING, runtime_checkable
from collections.abc import Collection, Mapping

if TYPE_CHECKING:
    import pyarrow as pa


@runtime_checkable
class ArrowDatabaseProtocol(Protocol):
    def add_record(
        self,
        record_path: tuple[str, ...],
        record_id: str,
        record: "pa.Table",
        skip_duplicates: bool = False,
        flush: bool = False,
    ) -> None: ...

    def add_records(
        self,
        record_path: tuple[str, ...],
        records: "pa.Table",
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
    ) -> "pa.Table | None": ...

    def get_all_records(
        self,
        record_path: tuple[str, ...],
        record_id_column: str | None = None,
    ) -> "pa.Table | None":
        """Retrieve all records for a given path as a stream."""
        ...

    def get_records_by_ids(
        self,
        record_path: tuple[str, ...],
        record_ids: Collection[str],
        record_id_column: str | None = None,
        flush: bool = False,
    ) -> "pa.Table | None": ...

    def get_records_with_column_value(
        self,
        record_path: tuple[str, ...],
        column_values: Collection[tuple[str, Any]] | Mapping[str, Any],
        record_id_column: str | None = None,
        flush: bool = False,
    ) -> "pa.Table | None": ...

    def flush(self) -> None:
        """Flush any buffered writes to the underlying storage."""
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
