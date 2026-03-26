"""Tests for SpiralDBTableSource.

SpiralDB requires network access and credentials, so all tests use
``unittest.mock`` to patch ``SpiralDBConnector`` at the import site inside
``spiraldb_table_source``.  No live SpiralDB instance is required.

Test sections:
 1. Import / export sanity
 2. Protocol conformance
 3. PK as default tag columns (single and composite)
 4. Explicit tag column override
 5. No key schema → ValueError (no ROWID fallback)
 6. Error cases (missing table, empty table)
 7. Stream behaviour
 8. Deterministic hashing
 9. Config round-trip (to_config / from_config)
10. Integration: SpiralDBTableSource in a simple pipeline
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest


# ---------------------------------------------------------------------------
# Helpers — build mock connector behaviour
# ---------------------------------------------------------------------------

_TABLE_NAME = "spike_data"
_PROJECT_ID = "test-project-123456"
_DATASET = "default"


def _make_mock_connector(
    table_names: list[str] | None = None,
    pk_columns: list[str] | None = None,
    batches: list[pa.RecordBatch] | None = None,
) -> MagicMock:
    """Return a MagicMock that satisfies DBConnectorProtocol for the given data.

    Args:
        table_names: Tables reported by ``get_table_names()``.
        pk_columns: PK columns returned by ``get_pk_columns()``.
        batches: Arrow batches returned by ``iter_batches()``.
    """
    if table_names is None:
        table_names = [_TABLE_NAME]
    if pk_columns is None:
        pk_columns = ["session_id"]
    if batches is None:
        batches = [
            pa.record_batch(
                {
                    "session_id": pa.array(["s1", "s2", "s3"], type=pa.large_string()),
                    "firing_rate": pa.array([0.1, 0.2, 0.3], type=pa.float64()),
                }
            )
        ]

    connector = MagicMock(name="MockSpiralDBConnector")
    connector.get_table_names.return_value = table_names
    connector.get_pk_columns.return_value = pk_columns
    connector.iter_batches.return_value = iter(batches)
    return connector


def _make_composite_pk_connector() -> MagicMock:
    """Mock connector with composite PK (session_id, probe_id) and data."""
    batches = [
        pa.record_batch(
            {
                "session_id": pa.array(["s1", "s1"], type=pa.large_string()),
                "probe_id": pa.array([1, 2], type=pa.int64()),
                "neuron_count": pa.array([100, 200], type=pa.int64()),
            }
        )
    ]
    return _make_mock_connector(
        pk_columns=["session_id", "probe_id"],
        batches=batches,
    )


# ---------------------------------------------------------------------------
# Shared fixture — patches SpiralDBConnector at the module level
# ---------------------------------------------------------------------------


def _patch_connector(mock_connector: MagicMock):
    """Context manager: patch SpiralDBConnector to return *mock_connector*."""
    return patch(
        "orcapod.core.sources.spiraldb_table_source.SpiralDBConnector",
        return_value=mock_connector,
    )


# ===========================================================================
# 1. Import / export sanity
# ===========================================================================


def test_import_from_core_sources():
    from orcapod.core.sources import SpiralDBTableSource

    assert SpiralDBTableSource is not None


def test_import_from_orcapod_sources():
    from orcapod.sources import SpiralDBTableSource

    assert SpiralDBTableSource is not None


def test_in_core_sources_all():
    import orcapod.core.sources as m

    assert "SpiralDBTableSource" in m.__all__


# ===========================================================================
# 2. Protocol conformance
# ===========================================================================


class TestProtocolConformance:
    def test_is_source_protocol(self):
        from orcapod.core.sources import SpiralDBTableSource
        from orcapod.protocols.core_protocols import SourceProtocol

        connector = _make_mock_connector()
        with _patch_connector(connector):
            src = SpiralDBTableSource(_PROJECT_ID, _TABLE_NAME)
        assert isinstance(src, SourceProtocol)

    def test_is_stream_protocol(self):
        from orcapod.core.sources import SpiralDBTableSource
        from orcapod.protocols.core_protocols import StreamProtocol

        connector = _make_mock_connector()
        with _patch_connector(connector):
            src = SpiralDBTableSource(_PROJECT_ID, _TABLE_NAME)
        assert isinstance(src, StreamProtocol)

    def test_is_pipeline_element_protocol(self):
        from orcapod.core.sources import SpiralDBTableSource
        from orcapod.protocols.hashing_protocols import PipelineElementProtocol

        connector = _make_mock_connector()
        with _patch_connector(connector):
            src = SpiralDBTableSource(_PROJECT_ID, _TABLE_NAME)
        assert isinstance(src, PipelineElementProtocol)


# ===========================================================================
# 3. PK as default tag columns
# ===========================================================================


class TestPKAsDefaultTags:
    def test_single_pk_is_tag_column(self):
        from orcapod.core.sources import SpiralDBTableSource

        connector = _make_mock_connector(pk_columns=["session_id"])
        with _patch_connector(connector):
            src = SpiralDBTableSource(_PROJECT_ID, _TABLE_NAME)
        tag_schema, _ = src.output_schema()
        assert "session_id" in tag_schema

    def test_pk_not_in_packet_schema(self):
        from orcapod.core.sources import SpiralDBTableSource

        connector = _make_mock_connector(pk_columns=["session_id"])
        with _patch_connector(connector):
            src = SpiralDBTableSource(_PROJECT_ID, _TABLE_NAME)
        _, packet_schema = src.output_schema()
        assert "session_id" not in packet_schema

    def test_non_pk_columns_in_packet_schema(self):
        from orcapod.core.sources import SpiralDBTableSource

        connector = _make_mock_connector(pk_columns=["session_id"])
        with _patch_connector(connector):
            src = SpiralDBTableSource(_PROJECT_ID, _TABLE_NAME)
        _, packet_schema = src.output_schema()
        assert "firing_rate" in packet_schema

    def test_composite_pk_all_columns_are_tags(self):
        from orcapod.core.sources import SpiralDBTableSource

        connector = _make_composite_pk_connector()
        with _patch_connector(connector):
            src = SpiralDBTableSource(_PROJECT_ID, _TABLE_NAME)
        tag_schema, _ = src.output_schema()
        assert "session_id" in tag_schema
        assert "probe_id" in tag_schema

    def test_composite_pk_data_column_in_packet(self):
        from orcapod.core.sources import SpiralDBTableSource

        connector = _make_composite_pk_connector()
        with _patch_connector(connector):
            src = SpiralDBTableSource(_PROJECT_ID, _TABLE_NAME)
        _, packet_schema = src.output_schema()
        assert "neuron_count" in packet_schema

    def test_default_source_id_is_table_name(self):
        from orcapod.core.sources import SpiralDBTableSource

        connector = _make_mock_connector()
        with _patch_connector(connector):
            src = SpiralDBTableSource(_PROJECT_ID, _TABLE_NAME)
        assert src.source_id == _TABLE_NAME

    def test_explicit_source_id_overrides_default(self):
        from orcapod.core.sources import SpiralDBTableSource

        connector = _make_mock_connector()
        with _patch_connector(connector):
            src = SpiralDBTableSource(_PROJECT_ID, _TABLE_NAME, source_id="my_src")
        assert src.source_id == "my_src"


# ===========================================================================
# 4. Explicit tag column override
# ===========================================================================


class TestExplicitTagOverride:
    def test_explicit_tag_columns_override_pk(self):
        from orcapod.core.sources import SpiralDBTableSource

        connector = _make_mock_connector(pk_columns=["session_id"])
        # Provide explicit tag_columns — PK should be ignored
        # Use "firing_rate" as tag by overriding, and "session_id" as packet
        batches = [
            pa.record_batch(
                {
                    "session_id": pa.array(["s1", "s2"], type=pa.large_string()),
                    "firing_rate": pa.array([0.1, 0.2], type=pa.float64()),
                    "amplitude": pa.array([1.0, 2.0], type=pa.float64()),
                }
            )
        ]
        connector.iter_batches.return_value = iter(batches)
        with _patch_connector(connector):
            src = SpiralDBTableSource(
                _PROJECT_ID, _TABLE_NAME, tag_columns=["firing_rate"]
            )
        tag_schema, _ = src.output_schema()
        assert "firing_rate" in tag_schema
        assert "session_id" not in tag_schema

    def test_multiple_explicit_tag_columns(self):
        from orcapod.core.sources import SpiralDBTableSource

        connector = _make_composite_pk_connector()
        with _patch_connector(connector):
            src = SpiralDBTableSource(
                _PROJECT_ID,
                _TABLE_NAME,
                tag_columns=["session_id", "probe_id"],
            )
        tag_schema, _ = src.output_schema()
        assert "session_id" in tag_schema
        assert "probe_id" in tag_schema


# ===========================================================================
# 5. No key schema → ValueError
# ===========================================================================


class TestNoPKRaisesError:
    def test_no_pk_columns_and_no_explicit_tags_raises(self):
        """SpiralDB has no ROWID fallback — raise ValueError when no PK."""
        from orcapod.core.sources import SpiralDBTableSource

        connector = _make_mock_connector(pk_columns=[])
        with _patch_connector(connector):
            with pytest.raises(ValueError, match="no primary key"):
                SpiralDBTableSource(_PROJECT_ID, _TABLE_NAME)

    def test_no_pk_columns_but_explicit_tags_succeeds(self):
        """Explicit tag_columns bypass the PK requirement."""
        from orcapod.core.sources import SpiralDBTableSource

        connector = _make_mock_connector(pk_columns=[])
        with _patch_connector(connector):
            src = SpiralDBTableSource(
                _PROJECT_ID, _TABLE_NAME, tag_columns=["session_id"]
            )
        tag_schema, _ = src.output_schema()
        assert "session_id" in tag_schema


# ===========================================================================
# 6. Error cases
# ===========================================================================


class TestErrorCases:
    def test_missing_table_raises_value_error(self):
        from orcapod.core.sources import SpiralDBTableSource

        connector = _make_mock_connector(table_names=[])
        with _patch_connector(connector):
            with pytest.raises(ValueError, match="not found in database"):
                SpiralDBTableSource(_PROJECT_ID, _TABLE_NAME)

    def test_empty_table_raises_value_error(self):
        from orcapod.core.sources import SpiralDBTableSource

        connector = _make_mock_connector(batches=[])
        with _patch_connector(connector):
            with pytest.raises(ValueError, match="is empty"):
                SpiralDBTableSource(_PROJECT_ID, _TABLE_NAME)

    def test_connector_closed_after_successful_init(self):
        """Connector must be closed even on success (eager load)."""
        from orcapod.core.sources import SpiralDBTableSource

        connector = _make_mock_connector()
        with _patch_connector(connector):
            SpiralDBTableSource(_PROJECT_ID, _TABLE_NAME)
        connector.close.assert_called()

    def test_connector_closed_after_failed_init(self):
        """Connector must be closed even when init raises."""
        from orcapod.core.sources import SpiralDBTableSource

        connector = _make_mock_connector(table_names=[])
        with _patch_connector(connector):
            with pytest.raises(ValueError):
                SpiralDBTableSource(_PROJECT_ID, _TABLE_NAME)
        connector.close.assert_called()

    def test_dataset_forwarded_to_connector(self):
        """dataset argument must be passed through to SpiralDBConnector."""
        from orcapod.core.sources import SpiralDBTableSource

        connector = _make_mock_connector()
        with patch(
            "orcapod.core.sources.spiraldb_table_source.SpiralDBConnector",
            return_value=connector,
        ) as mock_cls:
            SpiralDBTableSource(_PROJECT_ID, _TABLE_NAME, dataset="prod")
        mock_cls.assert_called_once_with(
            project_id=_PROJECT_ID, dataset="prod", overrides=None
        )

    def test_overrides_forwarded_to_connector(self):
        """overrides argument must be passed through to SpiralDBConnector."""
        from orcapod.core.sources import SpiralDBTableSource

        overrides = {"server.url": "http://api.spiraldb.dev"}
        connector = _make_mock_connector()
        with patch(
            "orcapod.core.sources.spiraldb_table_source.SpiralDBConnector",
            return_value=connector,
        ) as mock_cls:
            SpiralDBTableSource(_PROJECT_ID, _TABLE_NAME, overrides=overrides)
        mock_cls.assert_called_once_with(
            project_id=_PROJECT_ID, dataset="default", overrides=overrides
        )


# ===========================================================================
# 7. Stream behaviour
# ===========================================================================


class TestStreamBehaviour:
    def test_producer_is_none(self):
        from orcapod.core.sources import SpiralDBTableSource

        connector = _make_mock_connector()
        with _patch_connector(connector):
            src = SpiralDBTableSource(_PROJECT_ID, _TABLE_NAME)
        assert src.producer is None

    def test_upstreams_is_empty(self):
        from orcapod.core.sources import SpiralDBTableSource

        connector = _make_mock_connector()
        with _patch_connector(connector):
            src = SpiralDBTableSource(_PROJECT_ID, _TABLE_NAME)
        assert src.upstreams == ()

    def test_iter_packets_yields_one_per_row(self):
        from orcapod.core.sources import SpiralDBTableSource

        connector = _make_mock_connector()
        with _patch_connector(connector):
            src = SpiralDBTableSource(_PROJECT_ID, _TABLE_NAME)
        packets = list(src.iter_packets())
        assert len(packets) == 3

    def test_iter_packets_tags_contain_pk(self):
        from orcapod.core.sources import SpiralDBTableSource

        connector = _make_mock_connector()
        with _patch_connector(connector):
            src = SpiralDBTableSource(_PROJECT_ID, _TABLE_NAME)
        for tags, _ in src.iter_packets():
            assert "session_id" in tags

    def test_output_schema_returns_two_schemas(self):
        from orcapod.core.sources import SpiralDBTableSource

        connector = _make_mock_connector()
        with _patch_connector(connector):
            src = SpiralDBTableSource(_PROJECT_ID, _TABLE_NAME)
        result = src.output_schema()
        assert len(result) == 2

    def test_as_table_returns_pyarrow_table(self):
        from orcapod.core.sources import SpiralDBTableSource

        connector = _make_mock_connector()
        with _patch_connector(connector):
            src = SpiralDBTableSource(_PROJECT_ID, _TABLE_NAME)
        t = src.as_table()
        assert isinstance(t, pa.Table)

    def test_as_table_row_count_matches_source(self):
        from orcapod.core.sources import SpiralDBTableSource

        connector = _make_mock_connector()
        with _patch_connector(connector):
            src = SpiralDBTableSource(_PROJECT_ID, _TABLE_NAME)
        assert src.as_table().num_rows == 3

    def test_packet_values_are_correct(self):
        from orcapod.core.sources import SpiralDBTableSource

        connector = _make_mock_connector()
        with _patch_connector(connector):
            src = SpiralDBTableSource(_PROJECT_ID, _TABLE_NAME)
        firing_rates = sorted(pkt["firing_rate"] for _, pkt in src.iter_packets())
        assert firing_rates == pytest.approx([0.1, 0.2, 0.3])

    def test_tag_values_are_correct(self):
        from orcapod.core.sources import SpiralDBTableSource

        connector = _make_mock_connector()
        with _patch_connector(connector):
            src = SpiralDBTableSource(_PROJECT_ID, _TABLE_NAME)
        session_ids = sorted(tags["session_id"] for tags, _ in src.iter_packets())
        assert session_ids == ["s1", "s2", "s3"]


# ===========================================================================
# 8. Deterministic hashing
# ===========================================================================


class TestDeterministicHashing:
    def test_pipeline_hash_is_deterministic(self):
        from orcapod.core.sources import SpiralDBTableSource

        c1 = _make_mock_connector()
        c2 = _make_mock_connector()
        with _patch_connector(c1):
            src1 = SpiralDBTableSource(_PROJECT_ID, _TABLE_NAME)
        with _patch_connector(c2):
            src2 = SpiralDBTableSource(_PROJECT_ID, _TABLE_NAME)
        assert src1.pipeline_hash() == src2.pipeline_hash()

    def test_content_hash_is_deterministic(self):
        from orcapod.core.sources import SpiralDBTableSource

        c1 = _make_mock_connector()
        c2 = _make_mock_connector()
        with _patch_connector(c1):
            src1 = SpiralDBTableSource(_PROJECT_ID, _TABLE_NAME)
        with _patch_connector(c2):
            src2 = SpiralDBTableSource(_PROJECT_ID, _TABLE_NAME)
        assert src1.content_hash() == src2.content_hash()

    def test_different_tag_columns_yields_different_pipeline_hash(self):
        from orcapod.core.sources import SpiralDBTableSource

        # src1 uses PK (session_id) as tag; src2 uses firing_rate
        batches = [
            pa.record_batch(
                {
                    "session_id": pa.array(["s1", "s2"], type=pa.large_string()),
                    "firing_rate": pa.array([0.1, 0.2], type=pa.float64()),
                    "amplitude": pa.array([1.0, 2.0], type=pa.float64()),
                }
            )
        ]
        c1 = _make_mock_connector(batches=batches)
        c2 = _make_mock_connector(batches=batches)
        with _patch_connector(c1):
            src1 = SpiralDBTableSource(_PROJECT_ID, _TABLE_NAME)
        c2.iter_batches.return_value = iter(batches)
        with _patch_connector(c2):
            src2 = SpiralDBTableSource(
                _PROJECT_ID, _TABLE_NAME, tag_columns=["firing_rate"]
            )
        assert src1.pipeline_hash() != src2.pipeline_hash()

    def test_different_schemas_yield_different_pipeline_hash(self):
        """pipeline_hash is schema-only; different column schemas → different hash."""
        from orcapod.core.sources import SpiralDBTableSource

        # src1: tag=session_id (large_string), packet=firing_rate (float64)
        c1 = _make_mock_connector()
        # src2: tag=session_id (large_string), packet=neuron_count (int64)
        batches2 = [
            pa.record_batch(
                {
                    "session_id": pa.array(["s1", "s2"], type=pa.large_string()),
                    "neuron_count": pa.array([10, 20], type=pa.int64()),
                }
            )
        ]
        c2 = _make_mock_connector(batches=batches2)
        with _patch_connector(c1):
            src1 = SpiralDBTableSource(_PROJECT_ID, _TABLE_NAME)
        with _patch_connector(c2):
            src2 = SpiralDBTableSource(_PROJECT_ID, _TABLE_NAME)
        assert src1.pipeline_hash() != src2.pipeline_hash()


# ===========================================================================
# 9. Config round-trip (to_config / from_config)
# ===========================================================================


class TestConfigSerialization:
    def test_to_config_source_type(self):
        from orcapod.core.sources import SpiralDBTableSource

        connector = _make_mock_connector()
        with _patch_connector(connector):
            src = SpiralDBTableSource(_PROJECT_ID, _TABLE_NAME)
        assert src.to_config()["source_type"] == "spiraldb_table"

    def test_to_config_has_no_connector_key(self):
        from orcapod.core.sources import SpiralDBTableSource

        connector = _make_mock_connector()
        with _patch_connector(connector):
            src = SpiralDBTableSource(_PROJECT_ID, _TABLE_NAME)
        assert "connector" not in src.to_config()

    def test_to_config_has_project_id(self):
        from orcapod.core.sources import SpiralDBTableSource

        connector = _make_mock_connector()
        with _patch_connector(connector):
            src = SpiralDBTableSource(_PROJECT_ID, _TABLE_NAME)
        assert src.to_config()["project_id"] == _PROJECT_ID

    def test_to_config_has_dataset(self):
        from orcapod.core.sources import SpiralDBTableSource

        connector = _make_mock_connector()
        with _patch_connector(connector):
            src = SpiralDBTableSource(_PROJECT_ID, _TABLE_NAME, dataset="prod")
        assert src.to_config()["dataset"] == "prod"

    def test_to_config_has_table_name(self):
        from orcapod.core.sources import SpiralDBTableSource

        connector = _make_mock_connector()
        with _patch_connector(connector):
            src = SpiralDBTableSource(_PROJECT_ID, _TABLE_NAME)
        assert src.to_config()["table_name"] == _TABLE_NAME

    def test_to_config_has_tag_columns(self):
        from orcapod.core.sources import SpiralDBTableSource

        connector = _make_mock_connector(pk_columns=["session_id"])
        with _patch_connector(connector):
            src = SpiralDBTableSource(_PROJECT_ID, _TABLE_NAME)
        assert "session_id" in src.to_config()["tag_columns"]

    def test_to_config_has_overrides(self):
        from orcapod.core.sources import SpiralDBTableSource

        overrides = {"server.url": "http://api.spiraldb.dev"}
        connector = _make_mock_connector()
        with _patch_connector(connector):
            src = SpiralDBTableSource(_PROJECT_ID, _TABLE_NAME, overrides=overrides)
        assert src.to_config()["overrides"] == overrides

    def test_to_config_overrides_none_by_default(self):
        from orcapod.core.sources import SpiralDBTableSource

        connector = _make_mock_connector()
        with _patch_connector(connector):
            src = SpiralDBTableSource(_PROJECT_ID, _TABLE_NAME)
        assert src.to_config()["overrides"] is None

    def test_to_config_has_identity_fields(self):
        from orcapod.core.sources import SpiralDBTableSource

        connector = _make_mock_connector()
        with _patch_connector(connector):
            src = SpiralDBTableSource(_PROJECT_ID, _TABLE_NAME)
        cfg = src.to_config()
        assert "content_hash" in cfg
        assert "pipeline_hash" in cfg

    def test_from_config_reconstructs_successfully(self):
        from orcapod.core.sources import SpiralDBTableSource

        c1 = _make_mock_connector()
        with _patch_connector(c1):
            src = SpiralDBTableSource(_PROJECT_ID, _TABLE_NAME)
            cfg = src.to_config()

        c2 = _make_mock_connector()
        with _patch_connector(c2):
            src2 = SpiralDBTableSource.from_config(cfg)
        assert src2.source_id == src.source_id

    def test_from_config_hashes_match(self):
        from orcapod.core.sources import SpiralDBTableSource

        c1 = _make_mock_connector()
        with _patch_connector(c1):
            src = SpiralDBTableSource(_PROJECT_ID, _TABLE_NAME)
            cfg = src.to_config()

        c2 = _make_mock_connector()
        with _patch_connector(c2):
            src2 = SpiralDBTableSource.from_config(cfg)
        assert src2.content_hash() == src.content_hash()
        assert src2.pipeline_hash() == src.pipeline_hash()

    def test_from_config_forwards_dataset(self):
        """from_config must pass dataset back to the constructor."""
        from orcapod.core.sources import SpiralDBTableSource

        c1 = _make_mock_connector()
        with _patch_connector(c1):
            src = SpiralDBTableSource(_PROJECT_ID, _TABLE_NAME, dataset="prod")
            cfg = src.to_config()

        assert cfg["dataset"] == "prod"
        c2 = _make_mock_connector()
        with patch(
            "orcapod.core.sources.spiraldb_table_source.SpiralDBConnector",
            return_value=c2,
        ) as mock_cls:
            SpiralDBTableSource.from_config(cfg)
        mock_cls.assert_called_once_with(
            project_id=_PROJECT_ID, dataset="prod", overrides=None
        )

    def test_resolve_source_from_config_works(self):
        from orcapod.core.sources import SpiralDBTableSource
        from orcapod.pipeline.serialization import resolve_source_from_config

        c1 = _make_mock_connector()
        with _patch_connector(c1):
            src = SpiralDBTableSource(_PROJECT_ID, _TABLE_NAME)
            cfg = src.to_config()

        c2 = _make_mock_connector()
        with _patch_connector(c2):
            src2 = resolve_source_from_config(cfg)
        assert isinstance(src2, SpiralDBTableSource)


# ===========================================================================
# 10. Integration: SpiralDBTableSource in a simple pipeline
# ===========================================================================


@pytest.mark.integration
class TestPipelineIntegration:
    def test_spiraldb_source_in_pipeline(self):
        """Verify SpiralDBTableSource drives a full pipeline end-to-end."""
        from orcapod.core.function_pod import FunctionPod
        from orcapod.core.packet_function import PythonPacketFunction
        from orcapod.core.sources import SpiralDBTableSource
        from orcapod.databases import InMemoryArrowDatabase
        from orcapod.pipeline import Pipeline
        from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator

        def double_rate(firing_rate: float) -> float:
            return firing_rate * 2.0

        connector = _make_mock_connector()
        with _patch_connector(connector):
            src = SpiralDBTableSource(_PROJECT_ID, _TABLE_NAME)

        pf = PythonPacketFunction(double_rate, output_keys="doubled")
        pod = FunctionPod(pf)

        pipeline = Pipeline(
            name="spiraldb_integration", pipeline_database=InMemoryArrowDatabase()
        )
        with pipeline:
            pod(src, label="doubler")

        orch = SyncPipelineOrchestrator()
        result = orch.run(pipeline._node_graph)

        fn_outputs = [
            v for k, v in result.node_outputs.items() if k.node_type == "function"
        ]
        assert len(fn_outputs) == 1
        assert len(fn_outputs[0]) == 3

        doubled_values = sorted(
            [pkt.as_dict()["doubled"] for _, pkt in fn_outputs[0]]
        )
        assert doubled_values == pytest.approx([0.2, 0.4, 0.6])

        tag_values = sorted([tags["session_id"] for tags, _ in fn_outputs[0]])
        assert tag_values == ["s1", "s2", "s3"]
