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
from typing import Any, Protocol, TYPE_CHECKING, runtime_checkable

from orcapod.types import ColumnInfo

if TYPE_CHECKING:
    import pyarrow as pa

# Re-export so existing ``from orcapod.protocols.db_connector_protocol import ColumnInfo``
# imports continue to work while the canonical definition lives in orcapod.types.
__all__ = ["ColumnInfo", "DBConnectorProtocol"]


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
    ) -> Iterator[pa.RecordBatch]:
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
        records: pa.Table,
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

    def unstorable_column_reason(
        self, name: str, arrow_type: pa.DataType, *, primary_key: bool
    ) -> str | None:
        """Say why this connector cannot store a column, or ``None`` when it can.

        The one place a connector states which columns it stores (ITL-1162). A
        connector stores a column only when it reads it back as written: the same
        name, and the same Arrow type in its large variant (``large_string``,
        ``large_binary``). A type the backend would widen, turn into text or fail to
        bind is refused, and so is a name the backend reserves.
        ``ConnectorArrowDatabase.require_storable`` asks this for every column of
        a table before anything of it is written.

        Args:
            name: The column's name.
            arrow_type: The column's Arrow type.
            primary_key: Whether the column is the table's primary key.

        Returns:
            A short reason, or ``None`` when the connector stores the column.
        """
        ...

    def unstorable_table_name_reason(self, table_name: str) -> str | None:
        """Say why this connector cannot hold a table of this name, or ``None``.

        The one place a connector states which table names it holds (ITL-1162): a
        name the backend would change (PostgreSQL truncates past 63 bytes) is
        refused, so two tables can never collide under one stored name.

        Args:
            table_name: The table name.

        Returns:
            A short reason, or ``None`` when the connector holds the name.
        """
        ...

    def validate_records(self, records: pa.Table) -> None:
        """Validate that ``records`` are safe to write through this connector.

        Implementations should raise ``ValueError`` if ``records`` contain
        columns that this connector cannot round-trip faithfully (e.g. Arrow
        extension-typed columns on connectors that drop field metadata).

        A connector that fully preserves Arrow field metadata should implement
        this as a no-op.

        Args:
            records: Arrow table to validate before writing.

        Raises:
            ValueError: If any column is incompatible with this connector.
        """
        ...

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def close(self) -> None:
        """Release the database connection and any associated resources."""
        ...

    def __enter__(self) -> DBConnectorProtocol: ...

    def __exit__(self, *args: Any) -> None: ...

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
    def from_config(cls, config: dict[str, Any]) -> DBConnectorProtocol:
        """Reconstruct a connector instance from a config dict."""
        ...
