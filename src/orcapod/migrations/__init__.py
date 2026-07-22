"""Orcapod schema migration utilities.

Public API:
    ``migrate_pipeline_v0_to_v1`` — migrate a pipeline DB table from v0 to v1.
    ``migrate_result_v0_to_v1`` — migrate a result DB table from v0 to v1.
    ``migrate_node`` — convenience wrapper: migrate a ``FunctionJobNode`` in one call.
    ``MigrationResult`` — dataclass summarising a migration run.
"""
from orcapod.migrations.types import MigrationResult
from orcapod.migrations.result_db import migrate_result_v0_to_v1
from orcapod.migrations.pipeline_db import migrate_pipeline_v0_to_v1, migrate_node

__all__ = [
    "MigrationResult",
    "migrate_pipeline_v0_to_v1",
    "migrate_result_v0_to_v1",
    "migrate_node",
]
