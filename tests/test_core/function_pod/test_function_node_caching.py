"""Tests for FunctionNode caching: pipeline DB vs result DB interaction.

Covers:
- compute_pipeline_entry_id behavior
- Pipeline entry_id based Phase 2 skip (tag + system_tags + packet_hash)
- CachedFunctionPod result cache hit with novel pipeline entry_id
- Same packet data, different tags → 1 result record, N pipeline records
- System tag awareness in pipeline entry_id computation
- Phase 1 yields existing records, Phase 2 processes only novel entry_ids
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.core.cached_function_pod import CachedFunctionPod
from orcapod.core.datagrams import Packet, Tag
from orcapod.core.function_pod import FunctionPod
from orcapod.core.nodes import FunctionNode
from orcapod.core.packet_function import PythonPacketFunction
from orcapod.core.sources import ArrowTableSource
from orcapod.core.streams.arrow_table_stream import ArrowTableStream
from orcapod.databases import InMemoryArrowDatabase
from orcapod.system_constants import constants


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def double(x: int) -> int:
    return x * 2


def _make_pod():
    pf = PythonPacketFunction(double, output_keys="result")
    return FunctionPod(pf)


def _make_stream(
    rows: list[dict], tag_columns: list[str] | None = None
) -> ArrowTableStream:
    if tag_columns is None:
        tag_columns = ["id"]
    table = pa.table(
        {k: pa.array([r[k] for r in rows], type=pa.int64()) for k in rows[0]}
    )
    return ArrowTableStream(table, tag_columns=tag_columns)


def _make_source_stream(
    rows: list[dict], tag_columns: list[str] | None = None, source_id: str = "src_a"
) -> ArrowTableStream:
    """Create a stream from an ArrowTableSource so it has system tag columns."""
    if tag_columns is None:
        tag_columns = ["id"]
    table = pa.table(
        {k: pa.array([r[k] for r in rows], type=pa.int64()) for k in rows[0]}
    )
    source = ArrowTableSource(table, tag_columns=tag_columns, source_id=source_id)
    return source


def _make_node(stream, db=None):
    pod = _make_pod()
    if db is None:
        db = InMemoryArrowDatabase()
    return FunctionNode(
        function_pod=pod,
        input_stream=stream,
        pipeline_database=db,
        result_database=db,
    ), db


# ---------------------------------------------------------------------------
# compute_pipeline_entry_id
# ---------------------------------------------------------------------------


class TestComputePipelineEntryId:
    def test_returns_non_empty_string(self):
        stream = _make_stream([{"id": 0, "x": 10}])
        node, _ = _make_node(stream)
        tag = Tag({"id": 0})
        packet = Packet({"x": 10})
        entry_id = node.compute_pipeline_entry_id(tag, packet)
        assert isinstance(entry_id, str)
        assert len(entry_id) > 0

    def test_same_inputs_produce_same_id(self):
        stream = _make_stream([{"id": 0, "x": 10}])
        node, _ = _make_node(stream)
        tag = Tag({"id": 0})
        packet = Packet({"x": 10})
        id1 = node.compute_pipeline_entry_id(tag, packet)
        id2 = node.compute_pipeline_entry_id(tag, packet)
        assert id1 == id2

    def test_different_tags_produce_different_ids(self):
        stream = _make_stream([{"id": 0, "x": 10}])
        node, _ = _make_node(stream)
        packet = Packet({"x": 10})
        id_tag0 = node.compute_pipeline_entry_id(Tag({"id": 0}), packet)
        id_tag1 = node.compute_pipeline_entry_id(Tag({"id": 1}), packet)
        assert id_tag0 != id_tag1

    def test_different_packets_produce_different_ids(self):
        stream = _make_stream([{"id": 0, "x": 10}])
        node, _ = _make_node(stream)
        tag = Tag({"id": 0})
        id_x10 = node.compute_pipeline_entry_id(tag, Packet({"x": 10}))
        id_x99 = node.compute_pipeline_entry_id(tag, Packet({"x": 99}))
        assert id_x10 != id_x99


# ---------------------------------------------------------------------------
# System tag awareness in entry_id
# ---------------------------------------------------------------------------


class TestSystemTagAwareness:
    def test_same_tag_values_different_system_tags_produce_different_ids(self):
        """Two tags with identical user values but different system tags
        must produce different pipeline entry_ids."""
        stream = _make_stream([{"id": 0, "x": 10}])
        node, _ = _make_node(stream)
        packet = Packet({"x": 10})

        # Tags with same user value but different system tag columns
        tag_a = Tag(
            {"id": 0},
            system_tags={f"{constants.SYSTEM_TAG_PREFIX}source:abc": "row0"},
        )
        tag_b = Tag(
            {"id": 0},
            system_tags={f"{constants.SYSTEM_TAG_PREFIX}source:xyz": "row0"},
        )

        id_a = node.compute_pipeline_entry_id(tag_a, packet)
        id_b = node.compute_pipeline_entry_id(tag_b, packet)
        assert id_a != id_b


# ---------------------------------------------------------------------------
# Result DB vs Pipeline DB record counts
# ---------------------------------------------------------------------------


class TestResultVsPipelineRecordCounts:
    def test_same_packet_different_tags_one_result_two_pipeline_records(self):
        """Same packet data with different tags should produce:
        - 1 result record (CachedFunctionPod caches by packet hash only)
        - 2 pipeline records (different tag → different entry_id)
        """
        rows = [{"id": 0, "x": 10}, {"id": 1, "x": 10}]
        stream = _make_stream(rows)
        node, db = _make_node(stream)
        node.run()

        # Result DB: 1 record (same packet hash, second is cache hit)
        result_records = node._cached_function_pod._result_database.get_all_records(
            node._cached_function_pod.record_path
        )
        assert result_records is not None
        assert result_records.num_rows == 1

        # Pipeline DB: 2 records (different tags → different entry_ids)
        pipeline_records = db.get_all_records(node.pipeline_path)
        assert pipeline_records is not None
        assert pipeline_records.num_rows == 2

    def test_different_packets_same_tag_two_result_two_pipeline_records(self):
        """Different packet data with same tag should produce:
        - 2 result records (different packet hashes)
        - 2 pipeline records (different packet hash → different entry_id)
        """
        rows = [{"id": 0, "x": 10}, {"id": 0, "x": 20}]
        stream = _make_stream(rows)
        node, db = _make_node(stream)
        node.run()

        result_records = node._cached_function_pod._result_database.get_all_records(
            node._cached_function_pod.record_path
        )
        assert result_records is not None
        assert result_records.num_rows == 2

        pipeline_records = db.get_all_records(node.pipeline_path)
        assert pipeline_records is not None
        assert pipeline_records.num_rows == 2

    def test_identical_rows_one_result_one_pipeline_record(self):
        """Identical (tag, packet) → 1 result record, 1 pipeline record."""
        # A single row — process once
        stream = _make_stream([{"id": 0, "x": 10}])
        node, db = _make_node(stream)
        node.run()

        result_records = node._cached_function_pod._result_database.get_all_records(
            node._cached_function_pod.record_path
        )
        assert result_records is not None
        assert result_records.num_rows == 1

        pipeline_records = db.get_all_records(node.pipeline_path)
        assert pipeline_records is not None
        assert pipeline_records.num_rows == 1


# ---------------------------------------------------------------------------
# Phase 1/2 interaction with pipeline entry_ids
# ---------------------------------------------------------------------------


class TestPhase1Phase2PipelineEntryId:
    def test_phase1_yields_existing_results(self):
        """Phase 1 should yield all previously computed results from DB."""
        db = InMemoryArrowDatabase()
        stream1 = _make_stream([{"id": 0, "x": 10}, {"id": 1, "x": 20}])
        node1, _ = _make_node(stream1, db=db)
        node1.run()

        # Create a second node with the same stream and shared DB
        stream2 = _make_stream([{"id": 0, "x": 10}, {"id": 1, "x": 20}])
        node2, _ = _make_node(stream2, db=db)
        results = list(node2.iter_packets())

        # All results should come from Phase 1 (DB), not recomputed
        assert len(results) == 2

    def test_phase2_processes_novel_entry_ids_only(self):
        """Phase 2 should only process inputs whose pipeline entry_id
        is not yet in the pipeline DB."""
        db = InMemoryArrowDatabase()

        # First run: process 2 rows
        stream1 = _make_stream([{"id": 0, "x": 10}, {"id": 1, "x": 20}])
        node1, _ = _make_node(stream1, db=db)
        node1.run()

        # Second run: 3 rows, 2 existing + 1 new
        stream2 = _make_stream(
            [{"id": 0, "x": 10}, {"id": 1, "x": 20}, {"id": 2, "x": 30}]
        )
        node2, _ = _make_node(stream2, db=db)
        results = list(node2.iter_packets())

        # Should yield 3 total: 2 from Phase 1 + 1 from Phase 2
        assert len(results) == 3
        # The new result should have the correct value
        result_values = sorted(p.as_dict()["result"] for _, p in results)
        assert result_values == [20, 40, 60]

    def test_same_packet_new_tag_triggers_phase2(self):
        """Same packet data but new tag should trigger Phase 2 processing
        because the pipeline entry_id (tag+system_tags+packet) is novel,
        even though CachedFunctionPod has a cache hit for the packet."""
        db = InMemoryArrowDatabase()

        # First run: tag=0, x=10
        stream1 = _make_stream([{"id": 0, "x": 10}])
        node1, _ = _make_node(stream1, db=db)
        node1.run()

        pipeline_count_after_first = db.get_all_records(node1.pipeline_path).num_rows
        assert pipeline_count_after_first == 1

        # Second run: tag=1, x=10 (same packet, different tag)
        stream2 = _make_stream([{"id": 1, "x": 10}])
        node2, _ = _make_node(stream2, db=db)
        results = list(node2.iter_packets())

        # Should yield 1 result from Phase 1 (tag=0) + 1 from Phase 2 (tag=1)
        assert len(results) == 2

        # Pipeline DB should now have 2 records
        pipeline_records = db.get_all_records(node2.pipeline_path)
        assert pipeline_records is not None
        assert pipeline_records.num_rows == 2

        # Result DB should still have only 1 record (same packet hash)
        result_records = node2._cached_function_pod._result_database.get_all_records(
            node2._cached_function_pod.record_path
        )
        assert result_records is not None
        assert result_records.num_rows == 1

    def test_all_existing_entry_ids_skipped_in_phase2(self):
        """When all inputs already have pipeline records, Phase 2
        should not call execute_packet at all."""
        db = InMemoryArrowDatabase()

        stream1 = _make_stream([{"id": 0, "x": 10}, {"id": 1, "x": 20}])
        node1, _ = _make_node(stream1, db=db)
        node1.run()

        # Re-run with identical stream
        stream2 = _make_stream([{"id": 0, "x": 10}, {"id": 1, "x": 20}])
        node2, _ = _make_node(stream2, db=db)

        pipeline_count_before = db.get_all_records(node2.pipeline_path).num_rows

        results = list(node2.iter_packets())
        assert len(results) == 2

        # No new pipeline records should be added
        pipeline_count_after = db.get_all_records(node2.pipeline_path).num_rows
        assert pipeline_count_after == pipeline_count_before


# ---------------------------------------------------------------------------
# CachedFunctionPod cache hit + novel pipeline entry
# ---------------------------------------------------------------------------


class TestResultCacheHitPipelineNovel:
    def test_cached_result_reused_for_new_tag(self):
        """When CachedFunctionPod has a cache hit (same packet hash) but
        the pipeline entry_id is novel (different tag), the cached result
        should be reused and a new pipeline record created."""
        db = InMemoryArrowDatabase()

        # Process tag=0, x=10
        stream1 = _make_stream([{"id": 0, "x": 10}])
        node1, _ = _make_node(stream1, db=db)
        node1.run()

        # Process tag=1, x=10 — same packet, different tag
        stream2 = _make_stream([{"id": 1, "x": 10}])
        node2, _ = _make_node(stream2, db=db)
        results = list(node2.iter_packets())

        # Both tags should produce the same result value
        result_values = [p.as_dict()["result"] for _, p in results]
        assert all(v == 20 for v in result_values)

    def test_pipeline_records_reference_same_result_uuid(self):
        """Two pipeline records for the same packet (different tags)
        should reference the same output packet UUID in the result DB."""
        db = InMemoryArrowDatabase()

        stream = _make_stream([{"id": 0, "x": 10}, {"id": 1, "x": 10}])
        node, _ = _make_node(stream, db=db)
        node.run()

        pipeline_records = db.get_all_records(node.pipeline_path)
        assert pipeline_records is not None
        assert pipeline_records.num_rows == 2

        # Both pipeline records should reference the same PACKET_RECORD_ID
        record_ids = pipeline_records.column(constants.PACKET_RECORD_ID).to_pylist()
        assert record_ids[0] == record_ids[1]


# ---------------------------------------------------------------------------
# Source columns in pipeline records
# ---------------------------------------------------------------------------


class TestPipelineRecordSourceColumns:
    def test_pipeline_record_contains_source_columns(self):
        """Pipeline records should include source columns of the input packet."""
        stream = _make_source_stream([{"id": 0, "x": 10}], source_id="my_source")
        node, db = _make_node(stream)
        node.run()

        pipeline_records = db.get_all_records(node.pipeline_path)
        assert pipeline_records is not None

        source_cols = [
            c
            for c in pipeline_records.column_names
            if c.startswith(constants.SOURCE_PREFIX)
        ]
        assert len(source_cols) > 0

    def test_pipeline_record_excludes_data_columns_of_input(self):
        """Pipeline records should NOT include data columns of the input packet."""
        stream = _make_source_stream([{"id": 0, "x": 10}])
        node, db = _make_node(stream)
        node.run()

        pipeline_records = db.get_all_records(node.pipeline_path)
        assert pipeline_records is not None

        # "x" is the input packet data column — should not appear
        assert "x" not in pipeline_records.column_names
        assert "_input_x" not in pipeline_records.column_names
