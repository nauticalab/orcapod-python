"""Unit tests for PostgreSQLTableSource."""
from __future__ import annotations


# ===========================================================================
# 1. Import / export sanity
# ===========================================================================


def test_import_from_core_sources():
    from orcapod.core.sources import PostgreSQLTableSource
    assert PostgreSQLTableSource is not None


def test_import_from_orcapod_sources():
    from orcapod.sources import PostgreSQLTableSource
    assert PostgreSQLTableSource is not None


def test_in_core_sources_all():
    import orcapod.core.sources as m
    assert "PostgreSQLTableSource" in m.__all__
