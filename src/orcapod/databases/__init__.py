from .delta_lake_databases import DeltaTableDatabase

__all__ = [
    "DeltaTableDatabase",
]

# Future ArrowDatabase backends to implement:
#
#   ParquetArrowDatabase    -- stores each record_path as a partitioned Parquet
#                              directory; simpler, no Delta Lake dependency,
#                              suitable for write-once / read-heavy workloads.
#
#   InMemoryArrowDatabase   -- dict-backed, no filesystem I/O; intended for
#                              unit tests and ephemeral in-process use.
#
#   IcebergArrowDatabase    -- Apache Iceberg backend for cloud-native /
#                              object-store deployments.
#
# All backends must satisfy the ArrowDatabase protocol defined in
# orcapod.protocols.database_protocols.
