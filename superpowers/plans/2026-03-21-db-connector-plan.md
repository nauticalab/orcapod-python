# DBConnector Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Introduce `DBConnectorProtocol`, `ConnectorArrowDatabase`, and `DBTableSource` so that a single DB technology class (SQLiteConnector, PostgreSQLConnector, SpiralDBConnector) powers both the `ArrowDatabaseProtocol` and `Source` layers.

**Architecture:** A new `DBConnectorProtocol` captures the minimal raw-access interface (schema introspection + Arrow-typed reads + writes + lifecycle). `ConnectorArrowDatabase` wraps any connector and implements `ArrowDatabaseProtocol` with pending-batch/flush semantics. `DBTableSource` wraps any connector as a read-only `RootSource`, defaulting to PK columns as tag columns.

**Tech Stack:** Python 3.11+, PyArrow, `typing.Protocol`, existing `RootSource` / `SourceStreamBuilder` / `ArrowDatabaseProtocol` abstractions.

---

## File Map

| Action | Path | Responsibility |
|--------|------|---------------|
| Create | `src/orcapod/protocols/db_connector_protocol.py` | `ColumnInfo` dataclass + `DBConnectorProtocol` |
| Modify | `src/orcapod/protocols/database_protocols.py` | Re-export `ColumnInfo`, `DBConnectorProtocol` |
| Create | `src/orcapod/databases/connector_arrow_database.py` | `ConnectorArrowDatabase` (generic `ArrowDatabaseProtocol` impl) |
| Modify | `src/orcapod/databases/__init__.py` | Export `ConnectorArrowDatabase`; update backend comment |
| Create | `src/orcapod/core/sources/db_table_source.py` | `DBTableSource` (generic read-only `RootSource`) |
| Modify | `src/orcapod/core/sources/__init__.py` | Export `DBTableSource` |
| Create | `tests/test_databases/test_connector_arrow_database.py` | Protocol conformance + behaviour tests |
| Create | `tests/test_core/sources/test_db_table_source.py` | `DBTableSource` tests |

---

## Task 1: `ColumnInfo` and `DBConnectorProtocol`

**Files:**
- Create: `src/orcapod/protocols/db_connector_protocol.py`
- Modify: `src/orcapod/protocols/database_protocols.py`

- [ ] **Step 1: Write the failing import test**

```python
# tests/test_databases/test_connector_arrow_database.py  (stub for now)
def test_import_db_connector_protocol():
    from orcapod.protocols.db_connector_protocol import ColumnInfo, DBConnectorProtocol
    assert ColumnInfo is not None
    assert DBConnectorProtocol is not None
```

- [ ] **Step 2: Run test to confirm it fails**

```bash
cd /tmp/kurouto-jobs/b8d04a9f-a949-4b75-9ab4-332a63bc70e3/orcapod-python
uv run pytest tests/test_databases/test_connector_arrow_database.py::test_import_db_connector_protocol -v
```
Expected: `ModuleNotFoundError`

- [ ] **Step 3: Create `src/orcapod/protocols/db_connector_protocol.py`**

```python
"""DBConnectorProtocol — minimal shared interface for external relational DB backends.

Each DB technology (SQLite, PostgreSQL, SpiralDB) implements this once.
Both ``ConnectorArrowDatabase`` (read+write) and ``DBTableSource`` (read-only)
depend on it, eliminating duplicated connection management and type-mapping logic.
"""
from __future__ import annotations

import re
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
```

- [ ] **Step 4: Add re-export to `src/orcapod/protocols/database_protocols.py`**

Append to the end of the existing file:

```python
from orcapod.protocols.db_connector_protocol import ColumnInfo, DBConnectorProtocol

__all__ = [
    "ArrowDatabaseProtocol",
    "ArrowDatabaseWithMetadataProtocol",
    "ColumnInfo",
    "DBConnectorProtocol",
    "MetadataCapableProtocol",
]
```

- [ ] **Step 5: Run test to confirm it passes**

```bash
uv run pytest tests/test_databases/test_connector_arrow_database.py::test_import_db_connector_protocol -v
```
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/protocols/db_connector_protocol.py src/orcapod/protocols/database_protocols.py tests/test_databases/test_connector_arrow_database.py
git commit -m "feat(protocols): add ColumnInfo and DBConnectorProtocol for PLT-1078"
```

---

## Task 2: `ConnectorArrowDatabase`

**Files:**
- Create: `src/orcapod/databases/connector_arrow_database.py`
- Modify: `src/orcapod/databases/__init__.py`

- [ ] **Step 1: Write the failing protocol-conformance test**

Replace the stub test file content with:

```python
"""Tests for ConnectorArrowDatabase — protocol conformance and behaviour via MockDBConnector."""
from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pytest

from orcapod.protocols.db_connector_protocol import ColumnInfo, DBConnectorProtocol
from orcapod.protocols.database_protocols import ArrowDatabaseProtocol


# ---------------------------------------------------------------------------
# MockDBConnector — in-memory implementation of DBConnectorProtocol for tests
# ---------------------------------------------------------------------------


class MockDBConnector:
    """Minimal in-memory DBConnectorProtocol implementation for testing."""

    def __init__(
        self,
        tables: dict[str, pa.Table] | None = None,
        pk_columns: dict[str, list[str]] | None = None,
    ):
        self._tables: dict[str, pa.Table] = dict(tables or {})
        self._pk_columns: dict[str, list[str]] = dict(pk_columns or {})

    def get_table_names(self) -> list[str]:
        return list(self._tables.keys())

    def get_pk_columns(self, table_name: str) -> list[str]:
        return list(self._pk_columns.get(table_name, []))

    def get_column_info(self, table_name: str) -> list[ColumnInfo]:
        schema = self._tables[table_name].schema
        return [
            ColumnInfo(name=f.name, arrow_type=f.type, nullable=f.nullable)
            for f in schema
        ]

    def iter_batches(
        self, query: str, params: Any = None, batch_size: int = 1000
    ) -> Iterator[pa.RecordBatch]:
        import re
        match = re.search(r'FROM\s+"?(\w+)"?', query, re.IGNORECASE)
        if not match:
            return
        table_name = match.group(1)
        table = self._tables.get(table_name)
        if table is None or table.num_rows == 0:
            return
        for batch in table.to_batches(max_chunksize=batch_size):
            yield batch

    def create_table_if_not_exists(
        self, table_name: str, columns: list[ColumnInfo], pk_column: str
    ) -> None:
        if table_name not in self._tables:
            self._tables[table_name] = pa.table(
                {c.name: pa.array([], type=c.arrow_type) for c in columns}
            )
            self._pk_columns.setdefault(table_name, [pk_column])

    def upsert_records(
        self, table_name: str, records: pa.Table, id_column: str, skip_existing: bool = False
    ) -> None:
        existing = self._tables.get(table_name)
        if existing is None or existing.num_rows == 0:
            self._tables[table_name] = records
            return
        new_ids = set(records[id_column].to_pylist())
        if skip_existing:
            existing_ids = set(existing[id_column].to_pylist())
            mask = pc.invert(
                pc.is_in(records[id_column], pa.array(list(new_ids & existing_ids)))
            )
            to_add = records.filter(mask)
            if to_add.num_rows > 0:
                self._tables[table_name] = pa.concat_tables([existing, to_add])
        else:
            mask = pc.invert(pc.is_in(existing[id_column], pa.array(list(new_ids))))
            kept = existing.filter(mask)
            self._tables[table_name] = pa.concat_tables([kept, records])

    def close(self) -> None:
        pass

    def __enter__(self) -> "MockDBConnector":
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    def to_config(self) -> dict[str, Any]:
        return {"connector_type": "mock"}

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> "MockDBConnector":
        return cls()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def connector():
    return MockDBConnector()


@pytest.fixture
def db(connector):
    from orcapod.databases import ConnectorArrowDatabase
    return ConnectorArrowDatabase(connector)


def make_table(**columns: list) -> pa.Table:
    return pa.table({k: pa.array(v) for k, v in columns.items()})


# ---------------------------------------------------------------------------
# Protocol conformance
# ---------------------------------------------------------------------------


class TestProtocolConformance:
    def test_satisfies_arrow_database_protocol(self, db):
        assert isinstance(db, ArrowDatabaseProtocol)

    def test_mock_satisfies_db_connector_protocol(self, connector):
        assert isinstance(connector, DBConnectorProtocol)
```

- [ ] **Step 2: Run test to confirm it fails**

```bash
uv run pytest tests/test_databases/test_connector_arrow_database.py::TestProtocolConformance -v
```
Expected: `ImportError: cannot import name 'ConnectorArrowDatabase'`

- [ ] **Step 3: Create `src/orcapod/databases/connector_arrow_database.py`**

```python
"""ConnectorArrowDatabase — generic ArrowDatabaseProtocol backed by any DBConnectorProtocol.

Implements the full ArrowDatabaseProtocol on top of any DBConnectorProtocol,
owning all record-management logic: record_path → table name mapping,
``__record_id`` column convention, in-memory pending-batch management,
deduplication, upsert, and flush.

Connector implementations (SQLiteConnector, PostgreSQLConnector, SpiralDBConnector)
need only satisfy DBConnectorProtocol; they do not implement ArrowDatabaseProtocol.
"""
from __future__ import annotations

import re
from collections import defaultdict
from collections.abc import Collection, Mapping
from typing import TYPE_CHECKING, Any, cast

from orcapod.protocols.db_connector_protocol import ColumnInfo, DBConnectorProtocol
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa
    import pyarrow.compute as pc
else:
    pa = LazyModule("pyarrow")
    pc = LazyModule("pyarrow.compute")


def _arrow_schema_to_column_infos(schema: "pa.Schema") -> list[ColumnInfo]:
    """Convert a PyArrow schema to a list of ColumnInfo."""
    return [
        ColumnInfo(name=field.name, arrow_type=field.type, nullable=field.nullable)
        for field in schema
    ]


class ConnectorArrowDatabase:
    """Generic ``ArrowDatabaseProtocol`` implementation backed by a ``DBConnectorProtocol``.

    Records are buffered in memory (pending batch) and written to the connector
    on ``flush()``. The ``record_path`` tuple is mapped to a sanitized SQL table
    name using ``"__".join(sanitized_parts)``.

    Args:
        connector: A ``DBConnectorProtocol`` implementation providing the
            underlying DB access (connection, type mapping, queries, writes).
        max_hierarchy_depth: Maximum allowed length for ``record_path`` tuples.

    Example::

        connector = SQLiteConnector(":memory:")   # PLT-1076
        db = ConnectorArrowDatabase(connector)
        db.add_record(("results", "my_fn"), record_id="abc", record=table)
        db.flush()
    """

    RECORD_ID_COLUMN = "__record_id"
    _ROW_INDEX_COLUMN = "__row_index"

    def __init__(
        self,
        connector: DBConnectorProtocol,
        max_hierarchy_depth: int = 10,
    ) -> None:
        self._connector = connector
        self.max_hierarchy_depth = max_hierarchy_depth
        self._pending_batches: dict[str, pa.Table] = {}
        self._pending_record_ids: dict[str, set[str]] = defaultdict(set)

    # ── Path helpers ──────────────────────────────────────────────────────────

    def _get_record_key(self, record_path: tuple[str, ...]) -> str:
        return "/".join(record_path)

    def _path_to_table_name(self, record_path: tuple[str, ...]) -> str:
        """Map a record_path to a safe SQL table name.

        Each component is sanitized (non-alphanumeric chars → ``_``), then
        joined with ``__`` as separator. A ``t_`` prefix is added if the result
        starts with a digit to ensure a valid SQL identifier.
        """
        parts = [re.sub(r"[^a-zA-Z0-9_]", "_", part) for part in record_path]
        name = "__".join(parts)
        if name and name[0].isdigit():
            name = "t_" + name
        return name

    def _validate_record_path(self, record_path: tuple[str, ...]) -> None:
        if not record_path:
            raise ValueError("record_path cannot be empty")
        if len(record_path) > self.max_hierarchy_depth:
            raise ValueError(
                f"record_path depth {len(record_path)} exceeds maximum "
                f"{self.max_hierarchy_depth}"
            )
        for i, component in enumerate(record_path):
            if not component or not isinstance(component, str):
                raise ValueError(
                    f"record_path component {i} is invalid: {repr(component)}"
                )

    # ── Record-ID column helpers ──────────────────────────────────────────────

    def _ensure_record_id_column(
        self, arrow_data: "pa.Table", record_id: str
    ) -> "pa.Table":
        if self.RECORD_ID_COLUMN not in arrow_data.column_names:
            key_array = pa.array(
                [record_id] * len(arrow_data), type=pa.large_string()
            )
            arrow_data = arrow_data.add_column(0, self.RECORD_ID_COLUMN, key_array)
        return arrow_data

    def _remove_record_id_column(self, arrow_data: "pa.Table") -> "pa.Table":
        if self.RECORD_ID_COLUMN in arrow_data.column_names:
            arrow_data = arrow_data.drop([self.RECORD_ID_COLUMN])
        return arrow_data

    def _handle_record_id_column(
        self, arrow_data: "pa.Table", record_id_column: str | None
    ) -> "pa.Table":
        if not record_id_column:
            return self._remove_record_id_column(arrow_data)
        if self.RECORD_ID_COLUMN in arrow_data.column_names:
            new_names = [
                record_id_column if name == self.RECORD_ID_COLUMN else name
                for name in arrow_data.schema.names
            ]
            return arrow_data.rename_columns(new_names)
        raise ValueError(
            f"Record ID column '{self.RECORD_ID_COLUMN}' not found in table."
        )

    # ── Deduplication ─────────────────────────────────────────────────────────

    def _deduplicate_within_table(self, table: "pa.Table") -> "pa.Table":
        """Keep the last occurrence of each record ID within a single table."""
        if table.num_rows <= 1:
            return table
        indices = pa.array(range(table.num_rows))
        table_with_idx = table.add_column(0, self._ROW_INDEX_COLUMN, indices)
        grouped = table_with_idx.group_by([self.RECORD_ID_COLUMN]).aggregate(
            [(self._ROW_INDEX_COLUMN, "max")]
        )
        max_indices = grouped[f"{self._ROW_INDEX_COLUMN}_max"].to_pylist()
        mask = pc.is_in(indices, pa.array(max_indices))
        return table.filter(mask)

    # ── Committed data access ─────────────────────────────────────────────────

    def _get_committed_table(
        self, record_path: tuple[str, ...]
    ) -> "pa.Table | None":
        """Fetch all committed records for a path from the connector."""
        table_name = self._path_to_table_name(record_path)
        if table_name not in self._connector.get_table_names():
            return None
        batches = list(
            self._connector.iter_batches(f'SELECT * FROM "{table_name}"')
        )
        if not batches:
            return None
        return pa.Table.from_batches(batches)

    # ── Write methods ─────────────────────────────────────────────────────────

    def add_record(
        self,
        record_path: tuple[str, ...],
        record_id: str,
        record: "pa.Table",
        skip_duplicates: bool = False,
        flush: bool = False,
    ) -> None:
        """Add a single record identified by ``record_id``."""
        data_with_id = self._ensure_record_id_column(record, record_id)
        self.add_records(
            record_path=record_path,
            records=data_with_id,
            record_id_column=self.RECORD_ID_COLUMN,
            skip_duplicates=skip_duplicates,
            flush=flush,
        )

    def add_records(
        self,
        record_path: tuple[str, ...],
        records: "pa.Table",
        record_id_column: str | None = None,
        skip_duplicates: bool = False,
        flush: bool = False,
    ) -> None:
        """Add multiple records to the pending batch."""
        self._validate_record_path(record_path)
        if records.num_rows == 0:
            return

        if record_id_column is None:
            record_id_column = records.column_names[0]
        if record_id_column not in records.column_names:
            raise ValueError(
                f"record_id_column '{record_id_column}' not found in table columns: "
                f"{records.column_names}"
            )

        # Normalise to internal column name
        if record_id_column != self.RECORD_ID_COLUMN:
            rename_map = {record_id_column: self.RECORD_ID_COLUMN}
            records = records.rename_columns(
                [rename_map.get(c, c) for c in records.column_names]
            )

        records = self._deduplicate_within_table(records)
        record_key = self._get_record_key(record_path)
        input_ids = set(cast(list[str], records[self.RECORD_ID_COLUMN].to_pylist()))

        if skip_duplicates:
            committed = self._get_committed_table(record_path)
            committed_ids: set[str] = set()
            if committed is not None:
                committed_ids = set(
                    cast(list[str], committed[self.RECORD_ID_COLUMN].to_pylist())
                )
            all_existing = (input_ids & self._pending_record_ids[record_key]) | (
                input_ids & committed_ids
            )
            if all_existing:
                mask = pc.invert(
                    pc.is_in(
                        records[self.RECORD_ID_COLUMN], pa.array(list(all_existing))
                    )
                )
                records = records.filter(mask)
            if records.num_rows == 0:
                return
        else:
            conflicts = input_ids & self._pending_record_ids[record_key]
            if conflicts:
                raise ValueError(
                    f"Records with IDs {conflicts} already exist in the pending batch. "
                    "Use skip_duplicates=True to skip them."
                )

        # Buffer in pending batch
        existing_pending = self._pending_batches.get(record_key)
        if existing_pending is None:
            self._pending_batches[record_key] = records
        else:
            self._pending_batches[record_key] = pa.concat_tables(
                [existing_pending, records]
            )
        self._pending_record_ids[record_key].update(
            cast(list[str], records[self.RECORD_ID_COLUMN].to_pylist())
        )

        if flush:
            self.flush()

    # ── Flush ─────────────────────────────────────────────────────────────────

    def flush(self) -> None:
        """Commit all pending batches to the connector via upsert."""
        for record_key in list(self._pending_batches.keys()):
            record_path = tuple(record_key.split("/"))
            table_name = self._path_to_table_name(record_path)
            pending = self._pending_batches.pop(record_key)
            self._pending_record_ids.pop(record_key, None)

            columns = _arrow_schema_to_column_infos(pending.schema)
            self._connector.create_table_if_not_exists(
                table_name, columns, pk_column=self.RECORD_ID_COLUMN
            )
            self._connector.upsert_records(
                table_name,
                pending,
                id_column=self.RECORD_ID_COLUMN,
                skip_existing=False,
            )

    # ── Read methods ──────────────────────────────────────────────────────────

    def get_record_by_id(
        self,
        record_path: tuple[str, ...],
        record_id: str,
        record_id_column: str | None = None,
        flush: bool = False,
    ) -> "pa.Table | None":
        if flush:
            self.flush()
        record_key = self._get_record_key(record_path)

        # Check pending first
        if record_id in self._pending_record_ids.get(record_key, set()):
            pending = self._pending_batches[record_key]
            filtered = pending.filter(pc.field(self.RECORD_ID_COLUMN) == record_id)
            if filtered.num_rows > 0:
                return self._handle_record_id_column(filtered, record_id_column)

        # Check committed
        committed = self._get_committed_table(record_path)
        if committed is None:
            return None
        filtered = committed.filter(pc.field(self.RECORD_ID_COLUMN) == record_id)
        if filtered.num_rows == 0:
            return None
        return self._handle_record_id_column(filtered, record_id_column)

    def get_all_records(
        self,
        record_path: tuple[str, ...],
        record_id_column: str | None = None,
    ) -> "pa.Table | None":
        record_key = self._get_record_key(record_path)
        parts: list[pa.Table] = []

        committed = self._get_committed_table(record_path)
        if committed is not None and committed.num_rows > 0:
            parts.append(committed)
        pending = self._pending_batches.get(record_key)
        if pending is not None and pending.num_rows > 0:
            parts.append(pending)

        if not parts:
            return None
        table = parts[0] if len(parts) == 1 else pa.concat_tables(parts)
        return self._handle_record_id_column(table, record_id_column)

    def get_records_by_ids(
        self,
        record_path: tuple[str, ...],
        record_ids: Collection[str],
        record_id_column: str | None = None,
        flush: bool = False,
    ) -> "pa.Table | None":
        if flush:
            self.flush()
        ids_list = list(record_ids)
        if not ids_list:
            return None
        all_records = self.get_all_records(
            record_path, record_id_column=self.RECORD_ID_COLUMN
        )
        if all_records is None:
            return None
        filtered = all_records.filter(
            pc.is_in(all_records[self.RECORD_ID_COLUMN], pa.array(ids_list))
        )
        if filtered.num_rows == 0:
            return None
        return self._handle_record_id_column(filtered, record_id_column)

    def get_records_with_column_value(
        self,
        record_path: tuple[str, ...],
        column_values: Collection[tuple[str, Any]] | Mapping[str, Any],
        record_id_column: str | None = None,
        flush: bool = False,
    ) -> "pa.Table | None":
        if flush:
            self.flush()
        all_records = self.get_all_records(
            record_path, record_id_column=self.RECORD_ID_COLUMN
        )
        if all_records is None:
            return None

        if isinstance(column_values, Mapping):
            pairs = list(column_values.items())
        else:
            pairs = cast(list[tuple[str, Any]], list(column_values))

        expr = None
        for col, val in pairs:
            e = pc.field(col) == val
            expr = e if expr is None else expr & e  # type: ignore[assignment]

        filtered = all_records.filter(expr)
        if filtered.num_rows == 0:
            return None
        return self._handle_record_id_column(filtered, record_id_column)

    # ── Config ────────────────────────────────────────────────────────────────

    def to_config(self) -> dict[str, Any]:
        """Serialize configuration to a JSON-compatible dict."""
        return {
            "type": "connector_arrow_database",
            "connector": self._connector.to_config(),
            "max_hierarchy_depth": self.max_hierarchy_depth,
        }

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> "ConnectorArrowDatabase":
        """Reconstruct a ConnectorArrowDatabase from config.

        Raises:
            NotImplementedError: Always — requires a connector registry that
                maps ``connector_type`` keys to ``from_config`` classmethods.
                Implement alongside connector classes in PLT-1074/1075/1076.
        """
        raise NotImplementedError(
            "ConnectorArrowDatabase.from_config requires a registered connector "
            "factory (connector_type → class). Implement in PLT-1074/1075/1076."
        )
```

- [ ] **Step 4: Update `src/orcapod/databases/__init__.py`**

```python
from .connector_arrow_database import ConnectorArrowDatabase
from .delta_lake_databases import DeltaTableDatabase
from .in_memory_databases import InMemoryArrowDatabase
from .noop_database import NoOpArrowDatabase

__all__ = [
    "ConnectorArrowDatabase",
    "DeltaTableDatabase",
    "InMemoryArrowDatabase",
    "NoOpArrowDatabase",
]

# Relational DB connector implementations (satisfy DBConnectorProtocol from
# orcapod.protocols.db_connector_protocol) and can be passed to
# ConnectorArrowDatabase or DBTableSource:
#
#   SQLiteConnector      -- PLT-1076 (stdlib sqlite3, zero extra deps)
#   PostgreSQLConnector  -- PLT-1075 (psycopg3)
#   SpiralDBConnector    -- PLT-1074
#
# ArrowDatabaseProtocol backends (existing, not connector-based):
#
#   DeltaTableDatabase   -- Delta Lake (deltalake package)
#   InMemoryArrowDatabase -- pure in-memory, for tests
#   NoOpArrowDatabase    -- no-op, for dry-runs / benchmarks
```

- [ ] **Step 5: Run conformance test to verify it passes**

```bash
uv run pytest tests/test_databases/test_connector_arrow_database.py::TestProtocolConformance -v
```
Expected: PASS

- [ ] **Step 6: Add behaviour tests to the test file**

Append to `tests/test_databases/test_connector_arrow_database.py`:

```python
# ---------------------------------------------------------------------------
# Behaviour tests
# ---------------------------------------------------------------------------


class TestAddAndGetRecord:
    def test_add_record_and_get_by_id(self, db):
        record = make_table(value=[42])
        db.add_record(("test", "path"), record_id="r1", record=record, flush=True)
        result = db.get_record_by_id(("test", "path"), "r1")
        assert result is not None
        assert result["value"][0].as_py() == 42

    def test_get_record_not_found_returns_none(self, db):
        assert db.get_record_by_id(("missing",), "nope") is None

    def test_add_record_pending_visible_before_flush(self, db):
        record = make_table(value=[1])
        db.add_record(("p",), record_id="x", record=record)
        result = db.get_record_by_id(("p",), "x")
        assert result is not None

    def test_get_all_records_returns_pending_and_committed(self, db):
        db.add_record(("t",), "a", make_table(v=[1]), flush=True)
        db.add_record(("t",), "b", make_table(v=[2]))
        all_r = db.get_all_records(("t",))
        assert all_r is not None
        assert all_r.num_rows == 2

    def test_skip_duplicates_true_ignores_existing(self, db):
        db.add_record(("t",), "a", make_table(v=[1]), flush=True)
        db.add_record(("t",), "a", make_table(v=[99]), skip_duplicates=True, flush=True)
        result = db.get_record_by_id(("t",), "a")
        assert result["v"][0].as_py() == 1  # original value preserved

    def test_skip_duplicates_false_raises_on_pending_conflict(self, db):
        db.add_record(("t",), "a", make_table(v=[1]))
        with pytest.raises(ValueError, match="already exist in the pending batch"):
            db.add_record(("t",), "a", make_table(v=[2]))

    def test_flush_writes_to_connector(self, connector, db):
        db.add_record(("fn",), "h1", make_table(x=[10]))
        assert db.get_all_records(("fn",)) is not None  # in pending
        db.flush()
        # after flush the connector should have the table
        table_name = db._path_to_table_name(("fn",))
        assert table_name in connector.get_table_names()

    def test_empty_record_path_raises(self, db):
        with pytest.raises(ValueError, match="cannot be empty"):
            db.add_record((), "x", make_table(v=[1]))

    def test_get_records_by_ids(self, db):
        db.add_record(("t",), "a", make_table(v=[1]), flush=True)
        db.add_record(("t",), "b", make_table(v=[2]), flush=True)
        result = db.get_records_by_ids(("t",), ["a"])
        assert result is not None
        assert result.num_rows == 1

    def test_get_records_with_column_value(self, db):
        db.add_records(
            ("t",),
            pa.table({"__record_id": pa.array(["a", "b"]), "kind": pa.array(["x", "y"])}),
            record_id_column="__record_id",
            flush=True,
        )
        result = db.get_records_with_column_value(("t",), {"kind": "x"})
        assert result is not None
        assert result.num_rows == 1


class TestPathToTableName:
    def test_simple_path(self, db):
        assert db._path_to_table_name(("results", "my_fn")) == "results__my_fn"

    def test_special_chars_sanitized(self, db):
        name = db._path_to_table_name(("a:b", "c/d"))
        assert "__" in name
        assert ":" not in name
        assert "/" not in name

    def test_digit_prefix_gets_t_prefix(self, db):
        name = db._path_to_table_name(("1abc",))
        assert name.startswith("t_")
```

- [ ] **Step 7: Run all tests**

```bash
uv run pytest tests/test_databases/test_connector_arrow_database.py -v
```
Expected: All PASS

- [ ] **Step 8: Commit**

```bash
git add src/orcapod/databases/connector_arrow_database.py src/orcapod/databases/__init__.py tests/test_databases/test_connector_arrow_database.py
git commit -m "feat(databases): add ConnectorArrowDatabase for PLT-1078"
```

---

## Task 3: `DBTableSource`

**Files:**
- Create: `src/orcapod/core/sources/db_table_source.py`
- Modify: `src/orcapod/core/sources/__init__.py`

- [ ] **Step 1: Write the failing import test**

```python
# tests/test_core/sources/test_db_table_source.py
def test_import_db_table_source():
    from orcapod.core.sources import DBTableSource
    assert DBTableSource is not None
```

- [ ] **Step 2: Run to confirm failure**

```bash
uv run pytest tests/test_core/sources/test_db_table_source.py::test_import_db_table_source -v
```
Expected: `ImportError`

- [ ] **Step 3: Create `src/orcapod/core/sources/db_table_source.py`**

```python
"""DBTableSource — a read-only RootSource backed by any DBConnectorProtocol.

Uses the table's primary-key columns as tag columns by default.
Type mapping (DB-native → Arrow) is fully delegated to the connector.

Example::

    connector = SQLiteConnector(":memory:")   # PLT-1076
    source = DBTableSource(connector, "measurements")          # PKs → tags
    source = DBTableSource(connector, "events", tag_columns=["session_id"])
"""
from __future__ import annotations

from collections.abc import Collection
from typing import TYPE_CHECKING, Any

from orcapod.core.sources.base import RootSource
from orcapod.core.sources.stream_builder import SourceStreamBuilder
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa

    from orcapod.protocols.db_connector_protocol import DBConnectorProtocol
else:
    pa = LazyModule("pyarrow")


class DBTableSource(RootSource):
    """A read-only Source backed by a table in any DBConnectorProtocol database.

    At construction time the source:
    1. Resolves tag columns (defaults to the table's primary-key columns).
    2. Validates the table exists in the connector.
    3. Fetches all rows as Arrow batches and assembles a PyArrow table.
    4. Enriches via ``SourceStreamBuilder`` (source-info, schema-hash, system tags).

    Args:
        connector: A ``DBConnectorProtocol`` providing DB access.
        table_name: Name of the table to expose as a source.
        tag_columns: Columns to use as tag columns.  If ``None`` (default),
            the table's primary-key columns are used.  Raises ``ValueError``
            if the table has no primary key and no explicit columns are given.
        system_tag_columns: Additional system-level tag columns (passed through
            to ``SourceStreamBuilder``; mirrors ``DeltaTableSource`` API).
        record_id_column: Column for stable per-row record IDs in provenance
            strings.  If ``None``, row indices are used.
        source_id: Canonical source name for the registry and provenance tokens.
            Defaults to ``table_name``.
        **kwargs: Forwarded to ``RootSource`` (``label``, ``data_context``,
            ``config``).

    Raises:
        ValueError: If the table is not found, has no PK columns and none are
            provided, or is empty.
    """

    def __init__(
        self,
        connector: "DBConnectorProtocol",
        table_name: str,
        tag_columns: Collection[str] | None = None,
        system_tag_columns: Collection[str] = (),
        record_id_column: str | None = None,
        source_id: str | None = None,
        **kwargs: Any,
    ) -> None:
        if source_id is None:
            source_id = table_name
        super().__init__(source_id=source_id, **kwargs)

        self._connector = connector
        self._table_name = table_name
        self._record_id_column = record_id_column

        # Validate the table exists first so the error is always "not found"
        # rather than a misleading "no primary key" error for missing tables.
        if table_name not in connector.get_table_names():
            raise ValueError(f"Table {table_name!r} not found in database.")

        # Resolve tag columns — default to PK columns
        if tag_columns is None:
            resolved_tag_columns: list[str] = connector.get_pk_columns(table_name)
            if not resolved_tag_columns:
                raise ValueError(
                    f"Table {table_name!r} has no primary key columns. "
                    "Provide explicit tag_columns."
                )
        else:
            resolved_tag_columns = list(tag_columns)

        # Fetch the full table as Arrow
        batches = list(connector.iter_batches(f'SELECT * FROM "{table_name}"'))
        if not batches:
            raise ValueError(f"Table {table_name!r} is empty.")
        table: pa.Table = pa.Table.from_batches(batches)

        # Enrich via SourceStreamBuilder (same pipeline as all other RootSources)
        builder = SourceStreamBuilder(self.data_context, self.orcapod_config)
        result = builder.build(
            table,
            tag_columns=resolved_tag_columns,
            source_id=self._source_id,
            record_id_column=record_id_column,
            system_tag_columns=system_tag_columns,
        )

        self._stream = result.stream
        self._tag_columns = result.tag_columns
        self._system_tag_columns = result.system_tag_columns
        if self._source_id is None:
            self._source_id = result.source_id

    def to_config(self) -> dict[str, Any]:
        """Serialize source configuration to a JSON-compatible dict."""
        return {
            "source_type": "db_table",
            "connector": self._connector.to_config(),
            "table_name": self._table_name,
            "tag_columns": list(self._tag_columns),
            "system_tag_columns": list(self._system_tag_columns),
            "record_id_column": self._record_id_column,
            "source_id": self.source_id,
            **self._identity_config(),
        }

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> "DBTableSource":
        """Not yet implemented — requires a connector factory registry.

        Raises:
            NotImplementedError: Always, until connector implementations add
                a registry helper (``build_db_connector_from_config``) in
                PLT-1074/1075/1076.
        """
        raise NotImplementedError(
            "DBTableSource.from_config requires a registered connector factory. "
            "Implement build_db_connector_from_config in PLT-1074/1075/1076."
        )
```

- [ ] **Step 4: Update `src/orcapod/core/sources/__init__.py`**

Add `DBTableSource` to the imports and `__all__`:

```python
from .db_table_source import DBTableSource
```

Add `"DBTableSource"` to the `__all__` list.

- [ ] **Step 5: Run import test to confirm it passes**

```bash
uv run pytest tests/test_core/sources/test_db_table_source.py::test_import_db_table_source -v
```
Expected: PASS

- [ ] **Step 6: Add full test suite to `tests/test_core/sources/test_db_table_source.py`**

Replace the entire file with:

```python
"""Tests for DBTableSource using MockDBConnector (no external DB required)."""
from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pytest

from orcapod.core.sources import DBTableSource
from orcapod.protocols.core_protocols import SourceProtocol, StreamProtocol
from orcapod.protocols.db_connector_protocol import ColumnInfo, DBConnectorProtocol
from orcapod.protocols.hashing_protocols import PipelineElementProtocol


# ---------------------------------------------------------------------------
# MockDBConnector (same interface as in test_connector_arrow_database.py)
# ---------------------------------------------------------------------------


class MockDBConnector:
    """Minimal in-memory DBConnectorProtocol for testing DBTableSource."""

    def __init__(
        self,
        tables: dict[str, pa.Table] | None = None,
        pk_columns: dict[str, list[str]] | None = None,
    ):
        self._tables: dict[str, pa.Table] = dict(tables or {})
        self._pk_columns: dict[str, list[str]] = dict(pk_columns or {})

    def get_table_names(self) -> list[str]:
        return list(self._tables.keys())

    def get_pk_columns(self, table_name: str) -> list[str]:
        return list(self._pk_columns.get(table_name, []))

    def get_column_info(self, table_name: str) -> list[ColumnInfo]:
        schema = self._tables[table_name].schema
        return [ColumnInfo(name=f.name, arrow_type=f.type) for f in schema]

    def iter_batches(
        self, query: str, params: Any = None, batch_size: int = 1000
    ) -> Iterator[pa.RecordBatch]:
        import re
        match = re.search(r'FROM\s+"?(\w+)"?', query, re.IGNORECASE)
        if not match:
            return
        table_name = match.group(1)
        table = self._tables.get(table_name)
        if table is None or table.num_rows == 0:
            return
        for batch in table.to_batches(max_chunksize=batch_size):
            yield batch

    def create_table_if_not_exists(self, *args: Any, **kwargs: Any) -> None:
        pass

    def upsert_records(self, *args: Any, **kwargs: Any) -> None:
        pass

    def close(self) -> None:
        pass

    def __enter__(self) -> "MockDBConnector":
        return self

    def __exit__(self, *args: Any) -> None:
        pass

    def to_config(self) -> dict[str, Any]:
        return {"connector_type": "mock"}

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> "MockDBConnector":
        return cls()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def measurements_table() -> pa.Table:
    return pa.table(
        {
            "session_id": pa.array(["s1", "s2", "s3"], type=pa.large_string()),
            "trial": pa.array([1, 2, 3], type=pa.int64()),
            "response": pa.array([0.1, 0.2, 0.3], type=pa.float64()),
        }
    )


@pytest.fixture
def connector(measurements_table) -> MockDBConnector:
    return MockDBConnector(
        tables={"measurements": measurements_table},
        pk_columns={"measurements": ["session_id"]},
    )


@pytest.fixture
def source(connector) -> DBTableSource:
    return DBTableSource(connector, "measurements")


# ---------------------------------------------------------------------------
# Protocol conformance
# ---------------------------------------------------------------------------


class TestProtocolConformance:
    def test_is_source_protocol(self, source):
        assert isinstance(source, SourceProtocol)

    def test_is_stream_protocol(self, source):
        assert isinstance(source, StreamProtocol)

    def test_is_pipeline_element_protocol(self, source):
        assert isinstance(source, PipelineElementProtocol)

    def test_connector_satisfies_db_connector_protocol(self, connector):
        assert isinstance(connector, DBConnectorProtocol)


# ---------------------------------------------------------------------------
# Construction behaviour
# ---------------------------------------------------------------------------


class TestConstruction:
    def test_pk_columns_used_as_default_tag_columns(self, source):
        tag_schema, _ = source.output_schema()
        assert "session_id" in tag_schema

    def test_explicit_tag_columns_override_pk(self, connector):
        src = DBTableSource(connector, "measurements", tag_columns=["trial"])
        tag_schema, _ = src.output_schema()
        assert "trial" in tag_schema
        assert "session_id" not in tag_schema

    def test_default_source_id_is_table_name(self, source):
        assert source.source_id == "measurements"

    def test_explicit_source_id_is_used(self, connector):
        src = DBTableSource(connector, "measurements", source_id="my_source")
        assert src.source_id == "my_source"

    def test_missing_table_raises_value_error(self, connector):
        with pytest.raises(ValueError, match="not found in database"):
            DBTableSource(connector, "nonexistent")

    def test_no_pk_and_no_tag_columns_raises(self, measurements_table):
        connector = MockDBConnector(
            tables={"t": measurements_table},
            pk_columns={},  # no PKs
        )
        with pytest.raises(ValueError, match="no primary key"):
            DBTableSource(connector, "t")

    def test_empty_table_raises_value_error(self, connector):
        connector._tables["empty"] = pa.table(
            {"id": pa.array([], type=pa.large_string())}
        )
        connector._pk_columns["empty"] = ["id"]
        with pytest.raises(ValueError, match="is empty"):
            DBTableSource(connector, "empty")


# ---------------------------------------------------------------------------
# Stream behaviour
# ---------------------------------------------------------------------------


class TestStreamBehaviour:
    def test_no_upstream_producer(self, source):
        assert source.producer is None

    def test_empty_upstreams(self, source):
        assert source.upstreams == ()

    def test_iter_packets_yields_correct_count(self, source, measurements_table):
        packets = list(source.iter_packets())
        assert len(packets) == measurements_table.num_rows

    def test_output_schema_has_correct_columns(self, source):
        tag_schema, packet_schema = source.output_schema()
        assert "session_id" in tag_schema
        assert "trial" in packet_schema
        assert "response" in packet_schema

    def test_as_table_returns_arrow_table(self, source, measurements_table):
        t = source.as_table()
        assert t.num_rows == measurements_table.num_rows

    def test_pipeline_hash_is_deterministic(self, connector):
        src1 = DBTableSource(connector, "measurements")
        src2 = DBTableSource(connector, "measurements")
        assert src1.pipeline_hash() == src2.pipeline_hash()

    def test_content_hash_is_deterministic(self, connector):
        src1 = DBTableSource(connector, "measurements")
        src2 = DBTableSource(connector, "measurements")
        assert src1.content_hash() == src2.content_hash()


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------


class TestConfig:
    def test_to_config_has_required_keys(self, source):
        config = source.to_config()
        assert config["source_type"] == "db_table"
        assert config["table_name"] == "measurements"
        assert "tag_columns" in config
        assert "connector" in config
        assert config["connector"]["connector_type"] == "mock"

    def test_from_config_raises_not_implemented(self, source):
        config = source.to_config()
        with pytest.raises(NotImplementedError):
            DBTableSource.from_config(config)
```

- [ ] **Step 7: Run all tests**

```bash
uv run pytest tests/test_core/sources/test_db_table_source.py -v
```
Expected: All PASS

- [ ] **Step 8: Commit**

```bash
git add src/orcapod/core/sources/db_table_source.py src/orcapod/core/sources/__init__.py tests/test_core/sources/test_db_table_source.py
git commit -m "feat(sources): add DBTableSource for PLT-1078"
```

---

## Task 4: Run the Full Test Suite

- [ ] **Step 1: Run all existing tests to verify no regressions**

```bash
uv run pytest tests/ -v --tb=short
```
Expected: All previously-passing tests still PASS; new tests also PASS.

- [ ] **Step 2: If any failures, fix them before proceeding**

- [ ] **Step 3: Commit any fixes**

```bash
git add -A
git commit -m "fix: address regressions from PLT-1078 interface changes"
```

---

## Task 5: Post Design Note to Linear

- [ ] **Step 1: Post the design decisions as a comment on PLT-1078**

Post a comment summarising: three-layer architecture (DBConnectorProtocol → ConnectorArrowDatabase / DBTableSource), all six design decisions from the decisions log, the new file map, and pointers to PLT-1074/1075/1076 for next steps.

- [ ] **Step 2: Update PLT-1078 status to "In Review"** (or the equivalent completed status)

---

## Task 6: Open PR

- [ ] **Step 1: Push the branch**

```bash
gh-app-token-generator nauticalab | gh auth login --with-token
git push -u origin eywalker/plt-1078-design-spike-clean-up-databasesource-interface-to-streamline
```

- [ ] **Step 2: Open PR targeting `dev`**

```bash
gh pr create \
  --title "feat: DBConnectorProtocol + ConnectorArrowDatabase + DBTableSource (PLT-1078)" \
  --base dev \
  --body "$(cat <<'EOF'
## Summary

Closes PLT-1078 — design spike: clean up Database/Source interface.

Introduces a three-layer abstraction so each DB technology (SQLite, PostgreSQL, SpiralDB) only needs to implement **one class** (`DBConnectorProtocol`) to power both the `ArrowDatabaseProtocol` layer (read+write memoization) and the `Source` layer (read-only pipeline ingestion).

### New files
- `src/orcapod/protocols/db_connector_protocol.py` — `ColumnInfo`, `DBConnectorProtocol`
- `src/orcapod/databases/connector_arrow_database.py` — `ConnectorArrowDatabase`
- `src/orcapod/core/sources/db_table_source.py` — `DBTableSource`
- `tests/test_databases/test_connector_arrow_database.py`
- `tests/test_core/sources/test_db_table_source.py`

### Updated files
- `src/orcapod/protocols/database_protocols.py` — re-exports
- `src/orcapod/databases/__init__.py` — exports + backend comments
- `src/orcapod/core/sources/__init__.py` — exports

### Key decisions
| Question | Decision |
|---|---|
| Protocol vs ABC? | `Protocol` (structural subtyping, no import coupling) |
| Generic source vs per-DB subclasses? | Single `DBTableSource(connector, table_name)` |
| Type mapping ownership? | Connector — callers always see Arrow types |
| Upsert abstraction? | `upsert_records(skip_existing)` hides SQL dialect differences |
| Pending-batch location? | `ConnectorArrowDatabase` (Python-side, mirrors existing impls) |
| Schema evolution? | Out of scope for this spike; `ValueError` on mismatch |

### Unblocks
- PLT-1074 (SpiralDBConnector)
- PLT-1075 (PostgreSQLConnector)
- PLT-1076 (SQLiteConnector)
- PLT-1072, PLT-1073, PLT-1077 (DB-backed Sources — just `DBTableSource(connector, table_name)`)

## Test plan
- [x] `ConnectorArrowDatabase` satisfies `ArrowDatabaseProtocol` (isinstance check)
- [x] `MockDBConnector` satisfies `DBConnectorProtocol` (isinstance check)
- [x] Full add/get/flush/skip-duplicates behaviour via mock connector
- [x] `DBTableSource` satisfies `SourceProtocol`, `StreamProtocol`, `PipelineElementProtocol`
- [x] PK columns used as default tag columns; explicit override works
- [x] Missing table / empty table / no-PK errors raised correctly
- [x] `to_config` round-trip; `from_config` raises `NotImplementedError`
- [x] No regressions in existing test suite

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```
