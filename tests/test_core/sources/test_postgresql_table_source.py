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


from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

DSN = "postgresql://user:pass@localhost:5432/testdb"
_PATCH = "orcapod.core.sources.postgresql_table_source.PostgreSQLConnector"


def _make_mock_connector(
    table_names: list[str] | None = None,
    pk_columns: list[str] | None = None,
    batches: list[pa.RecordBatch] | None = None,
) -> MagicMock:
    """Build a MagicMock shaped like a PostgreSQLConnector.

    Defaults: table 'measurements' with PK 'session_id' and 3 rows.
    """
    mock = MagicMock()

    if table_names is None:
        table_names = ["measurements"]
    if pk_columns is None:
        pk_columns = ["session_id"]
    if batches is None:
        schema = pa.schema([
            pa.field("session_id", pa.large_string()),
            pa.field("trial", pa.int64()),
            pa.field("response", pa.float64()),
        ])
        batches = [
            pa.RecordBatch.from_arrays(
                [
                    pa.array(["s1", "s2", "s3"], type=pa.large_string()),
                    pa.array([1, 2, 3], type=pa.int64()),
                    pa.array([0.1, 0.2, 0.3], type=pa.float64()),
                ],
                schema=schema,
            )
        ]

    mock.get_table_names.return_value = table_names
    mock.get_pk_columns.return_value = pk_columns
    mock.iter_batches.return_value = iter(batches)
    return mock


# ===========================================================================
# 2. Protocol conformance
# ===========================================================================


class TestProtocolConformance:
    def test_is_source_protocol(self):
        from orcapod.core.sources import PostgreSQLTableSource
        from orcapod.protocols.core_protocols import SourceProtocol

        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src = PostgreSQLTableSource(DSN, "measurements")
        assert isinstance(src, SourceProtocol)

    def test_is_stream_protocol(self):
        from orcapod.core.sources import PostgreSQLTableSource
        from orcapod.protocols.core_protocols import StreamProtocol

        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src = PostgreSQLTableSource(DSN, "measurements")
        assert isinstance(src, StreamProtocol)

    def test_is_pipeline_element_protocol(self):
        from orcapod.core.sources import PostgreSQLTableSource
        from orcapod.protocols.hashing_protocols import PipelineElementProtocol

        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src = PostgreSQLTableSource(DSN, "measurements")
        assert isinstance(src, PipelineElementProtocol)


# ===========================================================================
# 3. PK as default tag columns
# ===========================================================================


class TestPKAsDefaultTags:
    def test_single_pk_is_tag_column(self):
        from orcapod.core.sources import PostgreSQLTableSource

        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src = PostgreSQLTableSource(DSN, "measurements")
        tag_schema, _ = src.output_schema()
        assert "session_id" in tag_schema

    def test_pk_not_in_packet_schema(self):
        from orcapod.core.sources import PostgreSQLTableSource

        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src = PostgreSQLTableSource(DSN, "measurements")
        _, packet_schema = src.output_schema()
        assert "session_id" not in packet_schema

    def test_non_pk_columns_in_packet_schema(self):
        from orcapod.core.sources import PostgreSQLTableSource

        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src = PostgreSQLTableSource(DSN, "measurements")
        _, packet_schema = src.output_schema()
        assert "trial" in packet_schema
        assert "response" in packet_schema

    def test_composite_pk_all_columns_are_tags(self):
        from orcapod.core.sources import PostgreSQLTableSource

        schema = pa.schema([
            pa.field("user_id", pa.large_string()),
            pa.field("event_id", pa.int64()),
            pa.field("payload", pa.large_string()),
        ])
        batch = pa.RecordBatch.from_arrays(
            [
                pa.array(["u1", "u1"], type=pa.large_string()),
                pa.array([1, 2], type=pa.int64()),
                pa.array(["click", "scroll"], type=pa.large_string()),
            ],
            schema=schema,
        )
        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector(
                table_names=["events"],
                pk_columns=["user_id", "event_id"],
                batches=[batch],
            )
            src = PostgreSQLTableSource(DSN, "events")
        tag_schema, _ = src.output_schema()
        assert "user_id" in tag_schema
        assert "event_id" in tag_schema

    def test_default_source_id_is_table_name(self):
        from orcapod.core.sources import PostgreSQLTableSource

        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src = PostgreSQLTableSource(DSN, "measurements")
        assert src.source_id == "measurements"

    def test_explicit_source_id_overrides_default(self):
        from orcapod.core.sources import PostgreSQLTableSource

        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src = PostgreSQLTableSource(DSN, "measurements", source_id="meas")
        assert src.source_id == "meas"


# ===========================================================================
# 4. Explicit tag_columns override
# ===========================================================================


class TestExplicitTagOverride:
    def test_explicit_tag_columns_override_pk(self):
        from orcapod.core.sources import PostgreSQLTableSource

        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src = PostgreSQLTableSource(DSN, "measurements", tag_columns=["trial"])
        tag_schema, _ = src.output_schema()
        assert "trial" in tag_schema
        assert "session_id" not in tag_schema

    def test_multiple_explicit_tag_columns(self):
        from orcapod.core.sources import PostgreSQLTableSource

        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src = PostgreSQLTableSource(
                DSN, "measurements", tag_columns=["session_id", "trial"]
            )
        tag_schema, _ = src.output_schema()
        assert "session_id" in tag_schema
        assert "trial" in tag_schema


# ===========================================================================
# 5. No-PK error
# ===========================================================================


class TestNoPKError:
    def test_no_pk_and_no_tag_columns_raises(self):
        from orcapod.core.sources import PostgreSQLTableSource

        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector(pk_columns=[])
            with pytest.raises(ValueError, match="has no primary key columns"):
                PostgreSQLTableSource(DSN, "measurements")


# ===========================================================================
# 6. Missing / empty table errors
# ===========================================================================


class TestTableErrors:
    def test_missing_table_raises_value_error(self):
        from orcapod.core.sources import PostgreSQLTableSource

        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector(table_names=[])
            with pytest.raises(ValueError, match="not found in database"):
                PostgreSQLTableSource(DSN, "nonexistent")

    def test_empty_table_raises_value_error(self):
        from orcapod.core.sources import PostgreSQLTableSource

        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector(batches=[])
            with pytest.raises(ValueError, match="is empty"):
                PostgreSQLTableSource(DSN, "measurements")


# ===========================================================================
# 7. Stream behaviour
# ===========================================================================


class TestStreamBehaviour:
    def test_producer_is_none(self):
        from orcapod.core.sources import PostgreSQLTableSource

        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src = PostgreSQLTableSource(DSN, "measurements")
        assert src.producer is None

    def test_upstreams_is_empty(self):
        from orcapod.core.sources import PostgreSQLTableSource

        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src = PostgreSQLTableSource(DSN, "measurements")
        assert src.upstreams == ()

    def test_iter_packets_yields_one_per_row(self):
        from orcapod.core.sources import PostgreSQLTableSource

        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src = PostgreSQLTableSource(DSN, "measurements")
        assert len(list(src.iter_packets())) == 3

    def test_iter_packets_tags_contain_pk(self):
        from orcapod.core.sources import PostgreSQLTableSource

        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src = PostgreSQLTableSource(DSN, "measurements")
        for tags, _ in src.iter_packets():
            assert "session_id" in tags

    def test_output_schema_returns_two_schemas(self):
        from orcapod.core.sources import PostgreSQLTableSource

        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src = PostgreSQLTableSource(DSN, "measurements")
        assert len(src.output_schema()) == 2

    def test_as_table_returns_pyarrow_table(self):
        from orcapod.core.sources import PostgreSQLTableSource

        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src = PostgreSQLTableSource(DSN, "measurements")
        assert isinstance(src.as_table(), pa.Table)

    def test_as_table_row_count_matches_source(self):
        from orcapod.core.sources import PostgreSQLTableSource

        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src = PostgreSQLTableSource(DSN, "measurements")
        assert src.as_table().num_rows == 3

    def test_connector_is_closed_after_construction(self):
        from orcapod.core.sources import PostgreSQLTableSource

        with patch(_PATCH) as mock_cls:
            mock_connector = _make_mock_connector()
            mock_cls.return_value = mock_connector
            PostgreSQLTableSource(DSN, "measurements")
        mock_connector.close.assert_called_once()

    def test_connector_is_closed_even_when_super_raises(self):
        from orcapod.core.sources import PostgreSQLTableSource

        with patch(_PATCH) as mock_cls:
            mock_connector = _make_mock_connector(table_names=[])
            mock_cls.return_value = mock_connector
            with pytest.raises(ValueError):
                PostgreSQLTableSource(DSN, "nonexistent")
        mock_connector.close.assert_called_once()


# ===========================================================================
# 8. Deterministic hashing
# ===========================================================================


class TestDeterministicHashing:
    def test_pipeline_hash_is_deterministic(self):
        from orcapod.core.sources import PostgreSQLTableSource

        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src1 = PostgreSQLTableSource(DSN, "measurements")
        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src2 = PostgreSQLTableSource(DSN, "measurements")
        assert src1.pipeline_hash() == src2.pipeline_hash()

    def test_content_hash_is_deterministic(self):
        from orcapod.core.sources import PostgreSQLTableSource

        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src1 = PostgreSQLTableSource(DSN, "measurements")
        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src2 = PostgreSQLTableSource(DSN, "measurements")
        assert src1.content_hash() == src2.content_hash()

    def test_different_tag_columns_yields_different_pipeline_hash(self):
        from orcapod.core.sources import PostgreSQLTableSource

        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src1 = PostgreSQLTableSource(DSN, "measurements")
        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src2 = PostgreSQLTableSource(DSN, "measurements", tag_columns=["trial"])
        assert src1.pipeline_hash() != src2.pipeline_hash()


# ===========================================================================
# 9. to_config shape
# ===========================================================================


class TestToConfig:
    def _make_src(self):
        from orcapod.core.sources import PostgreSQLTableSource

        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            return PostgreSQLTableSource(DSN, "measurements")

    def test_has_source_type_postgresql_table(self):
        assert self._make_src().to_config()["source_type"] == "postgresql_table"

    def test_has_dsn(self):
        assert self._make_src().to_config()["dsn"] == DSN

    def test_has_table_name(self):
        assert self._make_src().to_config()["table_name"] == "measurements"

    def test_has_tag_columns(self):
        assert "session_id" in self._make_src().to_config()["tag_columns"]

    def test_has_source_id(self):
        assert self._make_src().to_config()["source_id"] == "measurements"

    def test_has_content_hash(self):
        assert "content_hash" in self._make_src().to_config()

    def test_has_pipeline_hash(self):
        assert "pipeline_hash" in self._make_src().to_config()

    def test_no_connector_key(self):
        assert "connector" not in self._make_src().to_config()

    def test_no_label_key(self):
        # label is not serialised (consistent with SQLiteTableSource)
        assert "label" not in self._make_src().to_config()


# ===========================================================================
# 10. from_config round-trip
# ===========================================================================


class TestFromConfig:
    def _make_src(self):
        from orcapod.core.sources import PostgreSQLTableSource

        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            return PostgreSQLTableSource(DSN, "measurements")

    def test_from_config_reconstructs_source_id(self):
        from orcapod.core.sources import PostgreSQLTableSource

        src = self._make_src()
        config = src.to_config()
        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src2 = PostgreSQLTableSource.from_config(config)
        assert src2.source_id == src.source_id

    def test_from_config_hashes_match(self):
        from orcapod.core.sources import PostgreSQLTableSource

        src = self._make_src()
        config = src.to_config()
        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src2 = PostgreSQLTableSource.from_config(config)
        assert src2.content_hash() == src.content_hash()
        assert src2.pipeline_hash() == src.pipeline_hash()

    def test_from_config_with_explicit_tag_columns(self):
        from orcapod.core.sources import PostgreSQLTableSource

        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src = PostgreSQLTableSource(DSN, "measurements", tag_columns=["trial"])
        config = src.to_config()
        with patch(_PATCH) as mock_cls:
            mock_cls.return_value = _make_mock_connector()
            src2 = PostgreSQLTableSource.from_config(config)
        tag_schema, _ = src2.output_schema()
        assert "trial" in tag_schema

    def test_from_config_missing_dsn_raises(self):
        from orcapod.core.sources import PostgreSQLTableSource

        with pytest.raises(KeyError):
            PostgreSQLTableSource.from_config({"table_name": "measurements"})


# ===========================================================================
# 11. resolve_source_from_config dispatch
# ===========================================================================


def test_resolve_source_from_config_dispatches_to_postgresql_table_source():
    from orcapod.core.sources import PostgreSQLTableSource
    from orcapod.pipeline.serialization import resolve_source_from_config

    with patch(_PATCH) as mock_cls:
        mock_cls.return_value = _make_mock_connector()
        src = PostgreSQLTableSource(DSN, "measurements")
    config = src.to_config()

    with patch(_PATCH) as mock_cls:
        mock_cls.return_value = _make_mock_connector()
        src2 = resolve_source_from_config(config)

    assert isinstance(src2, PostgreSQLTableSource)
