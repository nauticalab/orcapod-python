"""
Comprehensive tests for DBTableSource using MockDBConnector (no external DB required).

Test sections:
 1. Import / export sanity
 2. MockDBConnector satisfies DBConnectorProtocol
 3. DBTableSource protocol conformance (SourceProtocol, StreamProtocol, PipelineElementProtocol)
 4. Construction — default tag columns (PK), explicit tag columns, source_id
 5. Construction error cases — missing table, no PK columns, empty table
 6. Stream behaviour — iter_packets count, output_schema, as_table, producer/upstreams
 7. Deterministic hashing (pipeline_hash, content_hash)
 8. Config — to_config shape, from_config raises NotImplementedError
"""
from __future__ import annotations

import re
from collections.abc import Iterator
from typing import Any

import pyarrow as pa
import pytest

from orcapod.core.sources import DBTableSource
from orcapod.protocols.core_protocols import SourceProtocol, StreamProtocol
from orcapod.protocols.db_connector_protocol import DBConnectorProtocol
from orcapod.types import ColumnInfo
from orcapod.protocols.hashing_protocols import PipelineElementProtocol


# ---------------------------------------------------------------------------
# MockDBConnector — minimal in-memory DBConnectorProtocol for these tests
# ---------------------------------------------------------------------------


class MockDBConnector:
    """Read-only in-memory connector for DBTableSource tests.

    Write methods (create_table_if_not_exists, upsert_records) are no-ops
    because DBTableSource never calls them.
    """

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
        pass  # not used by DBTableSource

    def upsert_records(self, *args: Any, **kwargs: Any) -> None:
        pass  # not used by DBTableSource

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
# Standard fixtures
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


# ===========================================================================
# 1. Import / export sanity
# ===========================================================================


def test_import_db_table_source_from_core_sources():
    from orcapod.core.sources import DBTableSource as _DBTableSource
    assert _DBTableSource is not None


def test_db_table_source_is_in_all():
    import orcapod.core.sources as sources_module
    assert "DBTableSource" in sources_module.__all__


# ===========================================================================
# 2. MockDBConnector satisfies DBConnectorProtocol
# ===========================================================================


class TestMockConnectorProtocol:
    def test_satisfies_db_connector_protocol(self, connector):
        assert isinstance(connector, DBConnectorProtocol)

    def test_get_table_names_returns_list(self, connector):
        names = connector.get_table_names()
        assert isinstance(names, list)
        assert "measurements" in names

    def test_get_pk_columns_returns_list(self, connector):
        pks = connector.get_pk_columns("measurements")
        assert pks == ["session_id"]

    def test_iter_batches_yields_record_batches(self, connector):
        batches = list(connector.iter_batches('SELECT * FROM "measurements"'))
        assert len(batches) > 0
        assert all(isinstance(b, pa.RecordBatch) for b in batches)

    def test_iter_batches_total_rows_match(self, connector, measurements_table):
        batches = list(connector.iter_batches('SELECT * FROM "measurements"'))
        total = sum(b.num_rows for b in batches)
        assert total == measurements_table.num_rows

    def test_iter_batches_missing_table_yields_nothing(self, connector):
        batches = list(connector.iter_batches('SELECT * FROM "no_such_table"'))
        assert batches == []


# ===========================================================================
# 3. DBTableSource protocol conformance
# ===========================================================================


class TestProtocolConformance:
    def test_is_source_protocol(self, source):
        assert isinstance(source, SourceProtocol)

    def test_is_stream_protocol(self, source):
        assert isinstance(source, StreamProtocol)

    def test_is_pipeline_element_protocol(self, source):
        assert isinstance(source, PipelineElementProtocol)

    def test_has_iter_packets(self, source):
        assert callable(source.iter_packets)

    def test_has_output_schema(self, source):
        assert callable(source.output_schema)

    def test_has_as_table(self, source):
        assert callable(source.as_table)

    def test_has_to_config(self, source):
        assert callable(source.to_config)

    def test_has_from_config(self, source):
        assert callable(source.from_config)


# ===========================================================================
# 4. Construction — tag columns and source_id
# ===========================================================================


class TestConstruction:
    def test_pk_columns_used_as_default_tag_columns(self, source):
        tag_schema, _ = source.output_schema()
        assert "session_id" in tag_schema

    def test_pk_tag_column_not_in_packet_schema(self, source):
        tag_schema, packet_schema = source.output_schema()
        assert "session_id" in tag_schema
        assert "session_id" not in packet_schema

    def test_non_pk_columns_in_packet_schema(self, source):
        _, packet_schema = source.output_schema()
        assert "trial" in packet_schema
        assert "response" in packet_schema

    def test_explicit_tag_columns_override_pk(self, connector):
        src = DBTableSource(connector, "measurements", tag_columns=["trial"])
        tag_schema, packet_schema = src.output_schema()
        assert "trial" in tag_schema
        assert "session_id" not in tag_schema

    def test_multiple_explicit_tag_columns(self, connector):
        src = DBTableSource(
            connector, "measurements", tag_columns=["session_id", "trial"]
        )
        tag_schema, _ = src.output_schema()
        assert "session_id" in tag_schema
        assert "trial" in tag_schema

    def test_default_source_id_is_table_name(self, source):
        assert source.source_id == "measurements"

    def test_explicit_source_id_overrides_default(self, connector):
        src = DBTableSource(connector, "measurements", source_id="my_meas")
        assert src.source_id == "my_meas"

    def test_table_with_multiple_pk_columns(self, measurements_table):
        connector = MockDBConnector(
            tables={"t": measurements_table},
            pk_columns={"t": ["session_id", "trial"]},
        )
        src = DBTableSource(connector, "t")
        tag_schema, _ = src.output_schema()
        assert "session_id" in tag_schema
        assert "trial" in tag_schema


# ===========================================================================
# 5. Construction error cases
# ===========================================================================


class TestConstructionErrors:
    def test_missing_table_raises_value_error(self, connector):
        with pytest.raises(ValueError, match="not found in database"):
            DBTableSource(connector, "nonexistent")

    def test_missing_table_error_not_confused_with_no_pk(self, measurements_table):
        """A missing table should raise 'not found', not 'no primary key'."""
        connector = MockDBConnector(
            tables={"t": measurements_table},
            pk_columns={},  # no PKs registered
        )
        with pytest.raises(ValueError, match="not found in database"):
            DBTableSource(connector, "completely_missing")

    def test_no_pk_and_no_explicit_tags_raises_value_error(self, measurements_table):
        connector = MockDBConnector(
            tables={"t": measurements_table},
            pk_columns={},  # table exists but has no PK
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

    def test_empty_table_error_distinguishable_from_missing_table(self, connector):
        """The two error messages must be distinct."""
        connector._tables["empty"] = pa.table({"id": pa.array([], type=pa.large_string())})
        connector._pk_columns["empty"] = ["id"]
        empty_err: ValueError | None = None
        missing_err: ValueError | None = None
        try:
            DBTableSource(connector, "empty")
        except ValueError as exc:
            empty_err = exc
        try:
            DBTableSource(connector, "nonexistent")
        except ValueError as exc:
            missing_err = exc
        assert empty_err is not None
        assert missing_err is not None
        # They must be different messages
        assert "not found" not in str(empty_err)
        assert "is empty" not in str(missing_err)


# ===========================================================================
# 6. Stream behaviour
# ===========================================================================


class TestStreamBehaviour:
    def test_producer_is_none(self, source):
        """Root sources have no upstream producer."""
        assert source.producer is None

    def test_upstreams_is_empty(self, source):
        assert source.upstreams == ()

    def test_iter_packets_yields_one_packet_per_row(self, source, measurements_table):
        packets = list(source.iter_packets())
        assert len(packets) == measurements_table.num_rows

    def test_iter_packets_each_has_tag_and_packet(self, source):
        # Tag and Packet are named types (not plain dict) but support
        # dict-like access and containment checks.
        for tags, packet in source.iter_packets():
            assert "session_id" in tags
            assert "trial" in packet or "response" in packet

    def test_output_schema_returns_two_schemas(self, source):
        result = source.output_schema()
        assert len(result) == 2

    def test_output_schema_tag_schema_is_dict_like(self, source):
        tag_schema, _ = source.output_schema()
        assert "session_id" in tag_schema

    def test_output_schema_packet_schema_has_payload_columns(self, source):
        _, packet_schema = source.output_schema()
        assert "trial" in packet_schema
        assert "response" in packet_schema

    def test_as_table_returns_pyarrow_table(self, source):
        t = source.as_table()
        assert isinstance(t, pa.Table)

    def test_as_table_row_count_matches_source_data(self, source, measurements_table):
        t = source.as_table()
        assert t.num_rows == measurements_table.num_rows

    def test_source_with_explicit_tags_yields_correct_keys(self, connector):
        src = DBTableSource(connector, "measurements", tag_columns=["session_id"])
        for tags, _ in src.iter_packets():
            assert "session_id" in tags


# ===========================================================================
# 7. Deterministic hashing
# ===========================================================================


class TestDeterministicHashing:
    def test_pipeline_hash_is_deterministic(self, connector):
        src1 = DBTableSource(connector, "measurements")
        src2 = DBTableSource(connector, "measurements")
        assert src1.pipeline_hash() == src2.pipeline_hash()

    def test_content_hash_is_deterministic(self, connector):
        src1 = DBTableSource(connector, "measurements")
        src2 = DBTableSource(connector, "measurements")
        assert src1.content_hash() == src2.content_hash()

    def test_pipeline_hash_is_schema_only_not_source_id(self, connector):
        # pipeline_identity_structure() is (tag_schema, packet_schema) by design —
        # source_id is intentionally excluded so sources with identical schemas
        # share the same pipeline hash and therefore the same pipeline DB table.
        src1 = DBTableSource(connector, "measurements", source_id="a")
        src2 = DBTableSource(connector, "measurements", source_id="b")
        assert src1.pipeline_hash() == src2.pipeline_hash()

    def test_different_tag_columns_yields_different_pipeline_hash(self, connector):
        src1 = DBTableSource(connector, "measurements", tag_columns=["session_id"])
        src2 = DBTableSource(connector, "measurements", tag_columns=["trial"])
        assert src1.pipeline_hash() != src2.pipeline_hash()


# ===========================================================================
# 8. Config
# ===========================================================================


class TestConfig:
    def test_to_config_has_source_type(self, source):
        config = source.to_config()
        assert config.get("source_type") == "db_table"

    def test_to_config_has_table_name(self, source):
        config = source.to_config()
        assert config["table_name"] == "measurements"

    def test_to_config_has_tag_columns(self, source):
        config = source.to_config()
        assert "tag_columns" in config
        assert "session_id" in config["tag_columns"]

    def test_to_config_has_connector(self, source):
        config = source.to_config()
        assert "connector" in config
        assert config["connector"]["connector_type"] == "mock"

    def test_to_config_has_source_id(self, source):
        config = source.to_config()
        assert config["source_id"] == "measurements"

    def test_to_config_has_identity_fields(self, source):
        config = source.to_config()
        # identity_config() adds content_hash, pipeline_hash, tag_schema, packet_schema
        assert "content_hash" in config
        assert "pipeline_hash" in config

    def test_from_config_raises_not_implemented(self, source):
        config = source.to_config()
        with pytest.raises(NotImplementedError):
            DBTableSource.from_config(config)

    def test_to_config_explicit_source_id_preserved(self, connector):
        src = DBTableSource(connector, "measurements", source_id="custom_id")
        config = src.to_config()
        assert config["source_id"] == "custom_id"

    def test_to_config_system_tag_columns_preserved(self, connector):
        src = DBTableSource(
            connector, "measurements", system_tag_columns=["session_id"]
        )
        config = src.to_config()
        assert "system_tag_columns" in config
