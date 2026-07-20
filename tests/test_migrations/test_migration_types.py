"""Tests for MigrationResult dataclass."""
from __future__ import annotations

from orcapod.migrations.types import MigrationResult


class TestMigrationResult:
    def test_fields(self):
        r = MigrationResult(
            rows_total=100,
            rows_migrated=95,
            rows_skipped=4,
            rows_unresolvable=1,
            elapsed_s=3.14,
            dry_run=False,
        )
        assert r.rows_total == 100
        assert r.rows_migrated == 95
        assert r.rows_skipped == 4
        assert r.rows_unresolvable == 1
        assert r.elapsed_s == 3.14
        assert r.dry_run is False

    def test_dry_run_field(self):
        r = MigrationResult(
            rows_total=10,
            rows_migrated=0,
            rows_skipped=0,
            rows_unresolvable=0,
            elapsed_s=0.1,
            dry_run=True,
        )
        assert r.dry_run is True
