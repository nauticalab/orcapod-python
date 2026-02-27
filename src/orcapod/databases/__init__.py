from .delta_lake_databases import DeltaTableDatabase
from .in_memory_databases import InMemoryArrowDatabase
from .noop_database import NoOpArrowDatabase

__all__ = [
    "DeltaTableDatabase",
    "InMemoryArrowDatabase",
    "NoOpArrowDatabase",
]

# Future ArrowDatabaseProtocol backends to implement:
#
#   ParquetArrowDatabase    -- stores each record_path as a partitioned Parquet
#                              directory; simpler, no Delta Lake dependency,
#                              suitable for write-once / read-heavy workloads.
#
#   IcebergArrowDatabase    -- Apache Iceberg backend for cloud-native /
#                              object-store deployments.
#
# All backends must satisfy the ArrowDatabaseProtocol protocol defined in
# orcapod.protocols.database_protocols.
