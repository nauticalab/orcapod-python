"""Integration tests for SpiralDBConnector against the live dev project.

These tests are skipped unless the ``SPIRAL_INTEGRATION_TESTS=1`` env var is set.
They require a valid auth mechanism, either:

- Local dev: ``~/.config/pyspiral/auth.json`` (obtained via ``spiral login``)
- CI (GitHub Actions): ``SPIRAL_WORKLOAD_ID=work_fbjjcy`` env var — workload
  ``work_fbjjcy`` (policy ``work_policy_p9cxfh``) has editor access on the dev
  project and is bound to ``nauticalab/orcapod-python`` via GitHub OIDC.

If the env var is set but credentials are absent, the tests fail rather than
skip — the operator is expected to ensure auth is in place when enabling
integration tests.

Project and server URL are configurable via env vars:

- ``SPIRAL_PROJECT_ID`` (default: ``test-orcapod-362211``)
- ``SPIRAL_SERVER_URL`` (default: ``http://api.spiraldb.dev``)
"""
from __future__ import annotations

import os
import uuid

import pyarrow as pa
import pyarrow.compute as pc
import pytest

from orcapod.databases import ConnectorArrowDatabase, SpiralDBConnector
from orcapod.types import ColumnInfo

INTEGRATION = os.environ.get("SPIRAL_INTEGRATION_TESTS") == "1"
DEV_PROJECT = os.environ.get("SPIRAL_PROJECT_ID", "test-orcapod-362211")
DEV_OVERRIDES = {"server.url": os.environ.get("SPIRAL_SERVER_URL", "http://api.spiraldb.dev")}

pytestmark = pytest.mark.skipif(
    not INTEGRATION,
    reason="Set SPIRAL_INTEGRATION_TESTS=1 to run SpiralDB integration tests",
)


def _unique_table(prefix: str = "test") -> str:
    """Return a unique table name to avoid cross-test interference."""
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


@pytest.fixture()
def connector():
    c = SpiralDBConnector(
        project_id=DEV_PROJECT,
        dataset="default",
        overrides=DEV_OVERRIDES,
    )
    yield c
    c.close()


class TestSpiralDBConnectorIntegration:
    def test_full_round_trip(self, connector):
        """Create table → write records → scan → verify values."""
        table_name = _unique_table("roundtrip")
        connector.create_table_if_not_exists(
            table_name,
            columns=[
                ColumnInfo("__record_id", pa.string(), nullable=False),
                ColumnInfo("value", pa.float64()),
            ],
            pk_column="__record_id",
        )
        records = pa.table({
            "__record_id": pa.array(["r1", "r2"], type=pa.string()),
            "value": pa.array([10.0, 20.0], type=pa.float64()),
        })
        connector.upsert_records(table_name, records, id_column="__record_id")

        result = pa.Table.from_batches(
            list(connector.iter_batches(f'SELECT * FROM "{table_name}"'))
        )
        assert result.num_rows == 2
        assert sorted(result.column("__record_id").to_pylist()) == ["r1", "r2"]

    def test_skip_existing_true(self, connector):
        """Write once, write again with overlapping keys — verify no duplication."""
        table_name = _unique_table("skip")
        connector.create_table_if_not_exists(
            table_name,
            columns=[
                ColumnInfo("id", pa.string(), nullable=False),
                ColumnInfo("val", pa.int64()),
            ],
            pk_column="id",
        )
        first_write = pa.table({"id": ["a", "b"], "val": pa.array([1, 2], type=pa.int64())})
        connector.upsert_records(table_name, first_write, id_column="id")

        second_write = pa.table({"id": ["a", "c"], "val": pa.array([99, 3], type=pa.int64())})
        connector.upsert_records(
            table_name, second_write, id_column="id", skip_existing=True
        )

        result = pa.Table.from_batches(
            list(connector.iter_batches(f'SELECT * FROM "{table_name}"'))
        )
        ids = sorted(result.column("id").to_pylist())
        assert ids == ["a", "b", "c"]

        # "a" must not have been overwritten (skip_existing=True preserved original val=1)
        a_mask = pc.equal(result.column("id"), pa.scalar("a"))
        a_val = result.filter(a_mask).column("val")[0].as_py()
        assert a_val == 1

    def test_connector_arrow_database_round_trip(self, connector):
        """ConnectorArrowDatabase: add_record → flush → get_record_by_id."""
        db = ConnectorArrowDatabase(connector)
        record = pa.table({"x": pa.array([42], type=pa.int64())})
        db.add_record(
            ("spiraldb", "integration"),
            record_id="test_r1",
            record=record,
            flush=True,
        )
        result = db.get_record_by_id(("spiraldb", "integration"), "test_r1")
        assert result is not None
        assert result.column("x")[0].as_py() == 42
