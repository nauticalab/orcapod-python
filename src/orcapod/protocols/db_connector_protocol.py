"""DBConnectorProtocol — minimal shared interface for external relational DB backends.

Each DB technology (SQLite, PostgreSQL, SpiralDB) implements this once.
Both ``ConnectorArrowDatabase`` (read+write) and ``DBTableSource`` (read-only)
depend on it, eliminating duplicated connection management and type-mapping logic.

Planned implementations:
    SQLiteConnector     -- PLT-1076 (stdlib sqlite3, zero extra deps)
    PostgreSQLConnector -- PLT-1075 (psycopg3)
    SpiralDBConnector   -- PLT-1074
"""
from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any, Protocol, TYPE_CHECKING, runtime_checkable

if TYPE_CHECKING:
    import pyarrow as pa


@dataclass(frozen=True)
class ColumnInfo:
    """Metadata for a single database column with its Arrow-mapped type.

    Type mapping (DB-native → Arrow) is the connector's responsibility.
    Consumers of ``DBConnectorProtocol`` always see Arrow types.

    Args:
        name: Column name.
        arrow_type: Arrow data type (already mapped from the DB-native type).
        nullable: Whether the column accepts NULL values.
    """

    name: str
    arrow_type: "pa.DataType"
    nullable: bool = True


@runtime_checkable
class DBConnectorProtocol(Protocol):
    """Minimal interface for an external relational database backend.

    Implementations encapsulate:
    - Connection lifecycle
    - DB-native ↔ Arrow type mapping
    - Schema introspection
    - Query execution (reads) and record management (writes)

    Read methods are used by both ``ConnectorArrowDatabase`` and ``DBTableSource``.
    Write methods (``create_table_if_not_exists``, ``upsert_records``) are used
    only by ``ConnectorArrowDatabase``.

    All query results are returned as Arrow types; connectors handle all
    DB-native type conversion internally.

    Planned implementations: ``SQLiteConnector`` (PLT-1076),
    ``PostgreSQLConnector`` (PLT-1075), ``SpiralDBConnector`` (PLT-1074).
    """

    # ── Schema introspection ──────────────────────────────────────────────────

    def get_table_names(self) -> list[str]:
        """Return all available table names in this database."""
        ...

    def get_pk_columns(self, table_name: str) -> list[str]:
        """Return primary-key column names for a table, in key-sequence order.

        Returns an empty list if the table has no primary key.
        """
        ...

    def get_column_info(self, table_name: str) -> list[ColumnInfo]:
        """Return column metadata for a table, with types mapped to Arrow."""
        ...

    # ── Read ──────────────────────────────────────────────────────────────────

    def iter_batches(
        self,
        query: str,
        params: Any = None,
        batch_size: int = 1000,
    ) -> Iterator["pa.RecordBatch"]:
        """Execute a query and yield results as Arrow RecordBatches.

        Args:
            query: SQL query string. Table names should be double-quoted
                (``SELECT * FROM "my_table"``); all connectors must support
                ANSI-standard double-quoted identifiers.
            params: Optional query parameters (connector-specific format).
            batch_size: Maximum rows per yielded batch.
        """
        ...

    # ── Write ─────────────────────────────────────────────────────────────────

    def create_table_if_not_exists(
        self,
        table_name: str,
        columns: list[ColumnInfo],
        pk_column: str,
    ) -> None:
        """Create a table with the given columns if it does not already exist.

        Args:
            table_name: Table to create.
            columns: Column definitions with Arrow-mapped types.
            pk_column: Name of the column to use as the primary key.
        """
        ...

    def upsert_records(
        self,
        table_name: str,
        records: "pa.Table",
        id_column: str,
        skip_existing: bool = False,
    ) -> None:
        """Write records to a table using upsert semantics.

        Args:
            table_name: Target table (must already exist).
            records: Arrow table of records to write.
            id_column: Column used as the unique row identifier.
            skip_existing: If ``True``, skip records whose ``id_column`` value
                already exists in the table (INSERT OR IGNORE).
                If ``False``, overwrite existing records (INSERT OR REPLACE).
        """
        ...

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def close(self) -> None:
        """Release the database connection and any associated resources."""
        ...

    def __enter__(self) -> "DBConnectorProtocol":
        ...

    def __exit__(self, *args: Any) -> None:
        ...

    # ── Serialization ─────────────────────────────────────────────────────────

    def to_config(self) -> dict[str, Any]:
        """Serialize connection configuration to a JSON-compatible dict.

        The returned dict must include a ``"connector_type"`` key
        (e.g., ``"sqlite"``, ``"postgresql"``, ``"spiraldb"``) so that
        a registry helper can dispatch to the correct ``from_config``
        classmethod when deserializing.
        """
        ...

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> "DBConnectorProtocol":
        """Reconstruct a connector instance from a config dict."""
        ...
