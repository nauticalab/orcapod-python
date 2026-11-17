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
        record_id: bytes,
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
        record_id: bytes,
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
        record_ids: Collection[bytes],
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

    @property
    def base_path(self) -> tuple[str, ...]:
        """The current relative root of this database view.

        Always () for a root (non-scoped) instance. Extended by at().
        The absolute storage root (filesystem URI, SQL connector, etc.)
        is a separate, backend-specific implementation detail.
        """
        ...

    def at(self, *path_components: str) -> "ArrowDatabaseProtocol":
        """Return a new database scoped to the given sub-path.

        All reads and writes on the returned database are relative to
        this database's base_path extended by path_components. The
        original is unmodified.

        Calling at() with no arguments returns a new view equivalent
        to the current one (same base_path, fresh or shared state
        depending on the backend).

        For backends with shared pending state (InMemoryArrowDatabase,
        ConnectorArrowDatabase), calling flush() on any view drains
        the entire shared pending queue — not just the caller's prefix.
        This is intentional: all views share the same underlying database.

        Args:
            *path_components: Zero or more path components to append.

        Returns:
            A new database instance with
            base_path == self.base_path + path_components.
        """
        ...

    def table_exists(self, record_path: tuple[str, ...]) -> bool:
        """Return ``True`` if a table exists at ``record_path``, ``False`` otherwise.

        This is a cheap existence check that does NOT load any records.
        Used by schema detection logic to determine whether a versioned table
        path already exists before checking for legacy paths.

        Args:
            record_path: Path components identifying the table, relative to
                ``self.base_path``.

        Returns:
            ``True`` if a table exists at the given path, ``False`` otherwise.
        """
        ...

    def list_table_paths(self, prefix: tuple[str, ...] = ()) -> list[tuple[str, ...]]:
        """List the paths of the tables stored under ``prefix``.

        A table is listed when its path starts with every component of ``prefix``
        (a table at ``prefix`` itself included). Tables stored below another table
        are listed too. Paths are relative to ``self.base_path``, like the
        ``record_path`` of every other method, and each begins with ``prefix``.
        The listing opens no table and loads no record.

        Orcapod uses it to find tables written by an earlier version, whose paths
        it cannot compute because the hashes inside them changed (ITL-954).

        What each database of Orcapod can list:

        - ``InMemoryArrowDatabase``: every table, flushed or not, exactly.
        - ``DeltaTableDatabase`` on a local directory: every flushed table, exactly
          (a directory walk; a directory holding ``_delta_log`` is a table).
        - ``DeltaTableDatabase`` on ``s3://`` or ``s3a://``: the same walk through
          ``s3fs``, one list request per directory. On ``gs://``, ``az://`` and
          ``abfs://`` only when the matching ``fsspec`` package is installed and
          can reach the database with its default credentials. A cloud database that
          cannot be listed raises ``TableListingNotSupportedError``.
        - ``ConnectorArrowDatabase`` (SQLite, PostgreSQL, SpiralDB): the tables
          whose name in the backend starts with the name ``prefix`` maps to. A
          table name does not keep the path it was made from, so the components
          after ``prefix`` come back as the backend stores them: every character
          other than letters, digits and ``_`` is ``_``, and a component that
          holds ``__`` comes back as two. Such a path still names the same
          table. The last component of a path is exact unless it holds ``__``.
        - ``ExtensionAwareDatabase``: what the database it wraps lists.
        - ``NoOpArrowDatabase``: nothing; it stores no table.

        Args:
            prefix: Leading path components, relative to ``self.base_path``. The
                empty prefix lists every table of this database view.

        Returns:
            The table paths, sorted. Empty when there is none.

        Raises:
            TableListingNotSupportedError: If this database cannot enumerate its
                tables.
            OrcapodDatabaseReadError: If the database can list its tables but the listing
                failed.
        """
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


class TableCommittingDatabaseProtocol(Protocol):
    """A database that commits table by table (ITL-919).

    Such a database (``DeltaTableDatabase``) can commit one table's pending writes
    on its own, and commits a table by itself once that table holds ``batch_size``
    pending rows. A function node uses ``flush_batch`` to commit its results before
    its pipeline records; ``PipelineJob.run`` keeps ``commit_every_rows`` below
    ``batch_size``. A wrapper satisfies this protocol only when the database it
    wraps does. Check with ``orcapod.databases.utils.as_table_committing``.
    """

    @property
    def batch_size(self) -> int:
        """Pending rows of one table at which the database commits that table."""
        ...

    def flush_batch(self, record_path: tuple[str, ...]) -> None:
        """Commit the pending writes of the table at ``record_path`` only."""
        ...


class ColumnCheckingDatabaseProtocol(Protocol):
    """A database that states which columns it can store (ITL-1162).

    Such a database (``ConnectorArrowDatabase``) refuses a table with a column its
    backend cannot store faithfully, before anything of it is written. A job node
    asks it about its stored tables when the database is attached, so a pipeline
    the database cannot hold is refused before anything runs. Check with
    ``orcapod.contexts.stored.require_storable``.
    """

    def require_storable(self, schema: pa.Schema) -> None:
        """Raise ``UnstorableColumnError`` for a column of ``schema`` it cannot store."""
        ...


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
