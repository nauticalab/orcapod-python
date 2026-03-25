from .connector_arrow_database import ConnectorArrowDatabase
from .delta_lake_databases import DeltaTableDatabase
from .in_memory_databases import InMemoryArrowDatabase
from .noop_database import NoOpArrowDatabase
from .spiraldb_connector import SpiralDBConnector
from .sqlite_connector import SQLiteConnector
from .postgresql_connector import PostgreSQLConnector

__all__ = [
    "ConnectorArrowDatabase",
    "DeltaTableDatabase",
    "InMemoryArrowDatabase",
    "NoOpArrowDatabase",
    "SpiralDBConnector",
    "SQLiteConnector",
    "PostgreSQLConnector",
]

# Relational DB connector implementations satisfy DBConnectorProtocol
# (orcapod.protocols.db_connector_protocol) and can be passed to either
# ConnectorArrowDatabase (read+write ArrowDatabaseProtocol) or
# DBTableSource (read-only Source):
#
#   SQLiteConnector      -- PLT-1076 (stdlib sqlite3, zero extra deps)  ✓
#   PostgreSQLConnector  -- PLT-1075 (psycopg3)                          ✓
#   SpiralDBConnector    -- PLT-1074
#
# ArrowDatabaseProtocol backends (existing, not connector-based):
#
#   DeltaTableDatabase    -- Delta Lake (deltalake package)
#   InMemoryArrowDatabase -- pure in-memory, for tests
#   NoOpArrowDatabase     -- no-op, for dry-runs / benchmarks
