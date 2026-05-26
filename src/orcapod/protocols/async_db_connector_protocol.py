"""AsyncDBConnectorProtocol — async counterpart to DBConnectorProtocol.

Standalone protocol (does not inherit from DBConnectorProtocol). A class
satisfies both protocols by implementing all their methods.

Intended use::

    async with PostgreSQLConnector(dsn) as connector:
        tables = await connector.async_get_table_names()
        async for batch in connector.async_iter_batches('SELECT * FROM "t"'):
            process(batch)
"""
from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any, Protocol, TYPE_CHECKING, runtime_checkable

from orcapod.types import ColumnInfo

if TYPE_CHECKING:
    import pyarrow as pa

__all__ = ["AsyncDBConnectorProtocol"]


@runtime_checkable
class AsyncDBConnectorProtocol(Protocol):
    """Async interface for an external relational database backend.

    Standalone protocol — does not inherit from ``DBConnectorProtocol``. A
    connector class satisfies both protocols by implementing all their methods.

    Lifecycle: use ``async with connector:`` to open async resources.
    ``__aexit__`` calls ``async_close()``, which performs a full shutdown of
    both async and sync connections.

    Example::

        async with PostgreSQLConnector(dsn) as connector:
            tables = await connector.async_get_table_names()
            async for batch in connector.async_iter_batches('SELECT * FROM "t"'):
                process(batch)
    """

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def __aenter__(self) -> AsyncDBConnectorProtocol:
        """Open async resources and return self."""
        ...

    async def __aexit__(self, *args: Any) -> None:
        """Close all resources by calling ``async_close()``."""
        ...

    async def async_close(self) -> None:
        """Release all async and sync database resources. Idempotent."""
        ...

    # ── Schema introspection ──────────────────────────────────────────────────

    async def async_get_table_names(self) -> list[str]:
        """Return all available table names in this database."""
        ...

    async def async_get_pk_columns(self, table_name: str) -> list[str]:
        """Return primary-key column names for a table, in key-sequence order.

        Returns an empty list if the table has no primary key.
        """
        ...

    async def async_get_column_info(self, table_name: str) -> list[ColumnInfo]:
        """Return column metadata for a table, with types mapped to Arrow."""
        ...

    # ── Read ──────────────────────────────────────────────────────────────────

    async def async_iter_batches(
        self,
        query: str,
        params: Any = None,
        batch_size: int = 1000,
    ) -> AsyncIterator[pa.RecordBatch]:
        """Execute a query and yield results as Arrow RecordBatches.

        Args:
            query: SQL query string. Table names should be double-quoted
                (``SELECT * FROM "my_table"``).
            params: Optional query parameters (connector-specific format).
            batch_size: Maximum rows per yielded batch.
        """
        ...
