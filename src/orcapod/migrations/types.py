"""Migration result types for Orcapod schema migrations."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass
class MigrationResult:
    """Summary of a completed schema migration run.

    Attributes:
        rows_total: Total rows found at the v0 path.
        rows_migrated: Rows successfully written to the v1 path.
        rows_skipped: Rows already present at the v1 path (idempotent re-run).
        rows_unresolvable: Rows whose result data was unreachable (e.g. ephemeral
            result expired); ``__output_data_hash`` written as ``None`` for these.
        elapsed_s: Wall-clock seconds elapsed during the migration.
        dry_run: ``True`` if the run was a dry run (no writes performed).
    """

    rows_total: int
    rows_migrated: int
    rows_skipped: int
    rows_unresolvable: int
    elapsed_s: float
    dry_run: bool
