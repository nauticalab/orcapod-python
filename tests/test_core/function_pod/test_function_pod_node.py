"""
Tests for FunctionNode covering:
- Construction, pipeline_path, uri
- output_schema and keys
- process_packet and add_pipeline_record
- iter_packets, run(), stream interface
- get_all_records: empty DB, correctness, ColumnConfig (meta/source/system_tags/all_info)
- pipeline_identity_structure and pipeline_hash
- pipeline_path_prefix
- result path conventions
"""

from __future__ import annotations

from collections.abc import Mapping

import pyarrow as pa
import pytest

from orcapod.core.datagrams import Packet, Tag
from orcapod.core.function_pod import FunctionPod
from orcapod.core.nodes import FunctionNode
from orcapod.core.packet_function import PythonPacketFunction
from orcapod.core.streams import ArrowTableStream
from orcapod.databases import InMemoryArrowDatabase
from orcapod.protocols.core_protocols import StreamProtocol
from orcapod.protocols.hashing_protocols import PipelineElementProtocol
from orcapod.system_constants import constants

from ..conftest import make_int_stream

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_node(
    pf: PythonPacketFunction,
    n: int = 3,
    db: InMemoryArrowDatabase | None = None,
) -> FunctionNode:
    if db is None:
        db = InMemoryArrowDatabase()
    return FunctionNode(
        function_pod=FunctionPod(packet_function=pf),
        input_stream=make_int_stream(n=n),
        pipeline_database=db,
    )


def _make_node_with_system_tags(
    pf: PythonPacketFunction,
    n: int = 3,
    db: InMemoryArrowDatabase | None = None,
) -> FunctionNode:
    """Build a node whose input stream has an explicit system-tag column ('run')."""
    if db is None:
        db = InMemoryArrowDatabase()
    table = pa.table(
        {
            "id": pa.array(list(range(n)), type=pa.int64()),
            "run": pa.array([f"r{i}" for i in range(n)]),
            "x": pa.array(list(range(n)), type=pa.int64()),
        }
    )
    stream = ArrowTableStream(table, tag_columns=["id"], system_tag_columns=["run"])
    return FunctionNode(
        function_pod=FunctionPod(packet_function=pf),
        input_stream=stream,
        pipeline_database=db,
    )


def _fill_node(node: FunctionNode) -> None:
    """Process all packets so the DB is populated."""
    node.run()


# ---------------------------------------------------------------------------
# 1. Construction
# ---------------------------------------------------------------------------


class TestFunctionNodeConstruction:
    @pytest.fixture
    def node(self, double_pf) -> FunctionNode:
        db = InMemoryArrowDatabase()
        stream = make_int_stream(n=3)
        return FunctionNode(
            function_pod=FunctionPod(packet_function=double_pf),
            input_stream=stream,
            pipeline_database=db,
        )

    def test_construction_succeeds(self, node):
        assert node is not None

    def test_pipeline_path_is_tuple_of_strings(self, node):
        path = node.pipeline_path
        assert isinstance(path, tuple)
        assert all(isinstance(p, str) for p in path)

    def test_node_hash_is_equal_to_wrapped_function_pod_output_stream(
        self, node: FunctionNode
    ) -> None:
        output_stream = node._function_pod.process(node._input_stream)
        node_hash = node.content_hash().to_string()
        stream_hash = output_stream.content_hash().to_string()
        assert node_hash == stream_hash

    def test_pipeline_path_ends_with_node_hash(self, node):
        path = node.pipeline_path
        assert path[-1].startswith("node:")

    def test_pipeline_path_contains_packet_function_uri(self, node):
        pf_uri = node._packet_function.uri
        for part in pf_uri:
            assert part in node.pipeline_path

    def test_pipeline_path_has_no_tag_schema_hash(self, node):
        path = node.pipeline_path
        assert not any(segment.startswith("tag:") for segment in path)

    def test_node_is_stream_protocol(self, node):
        assert isinstance(node, StreamProtocol)

    def test_node_is_pipeline_element_protocol(self, node):
        assert isinstance(node, PipelineElementProtocol)

    def test_producer_is_function_pod(self, node):
        assert isinstance(node.producer, FunctionPod)

    def test_upstreams_contains_input_stream(self, node):
        upstreams = node.upstreams
        assert isinstance(upstreams, tuple)
        assert len(upstreams) == 1
        assert isinstance(upstreams[0], StreamProtocol)

    def test_incompatible_stream_raises_on_construction(self, double_pf):
        db = InMemoryArrowDatabase()
        bad_stream = ArrowTableStream(
            pa.table(
                {
                    "id": pa.array([0, 1], type=pa.int64()),
                    "z": pa.array([0, 1], type=pa.int64()),
                }
            ),
            tag_columns=["id"],
        )
        with pytest.raises(ValueError):
            FunctionNode(
                function_pod=FunctionPod(packet_function=double_pf),
                input_stream=bad_stream,
                pipeline_database=db,
            )

    def test_result_database_defaults_to_pipeline_database(self, double_pf):
        db = InMemoryArrowDatabase()
        node = FunctionNode(
            function_pod=FunctionPod(packet_function=double_pf),
            input_stream=make_int_stream(n=2),
            pipeline_database=db,
        )
        assert node._pipeline_database is db

    def test_separate_result_database_accepted(self, double_pf):
        pipeline_db = InMemoryArrowDatabase()
        result_db = InMemoryArrowDatabase()
        node = FunctionNode(
            function_pod=FunctionPod(packet_function=double_pf),
            input_stream=make_int_stream(n=2),
            pipeline_database=pipeline_db,
            result_database=result_db,
        )
        assert node._pipeline_database is pipeline_db


# ---------------------------------------------------------------------------
# 2. output_schema
# ---------------------------------------------------------------------------


class TestFunctionNodeOutputSchema:
    @pytest.fixture
    def node(self, double_pf) -> FunctionNode:
        db = InMemoryArrowDatabase()
        return FunctionNode(
            function_pod=FunctionPod(packet_function=double_pf),
            input_stream=make_int_stream(n=3),
            pipeline_database=db,
        )

    def test_output_schema_returns_two_mappings(self, node: FunctionNode):
        tag_schema, packet_schema = node.output_schema()
        assert isinstance(tag_schema, Mapping)
        assert isinstance(packet_schema, Mapping)
        assert "id" in tag_schema
        assert len(tag_schema) == 1
        assert "result" in packet_schema
        assert len(packet_schema) == 1
        assert tag_schema["id"] is int
        assert packet_schema["result"] is int

    def test_packet_schema_matches_function_output(self, node, double_pf):
        _, packet_schema = node.output_schema()
        assert packet_schema == double_pf.output_packet_schema

    def test_tag_schema_matches_input_stream(self, node):
        tag_schema, _ = node.output_schema()
        assert "id" in tag_schema
        assert tag_schema["id"] is int


# ---------------------------------------------------------------------------
# 3. process_packet and add_pipeline_record
# ---------------------------------------------------------------------------


class TestFunctionNodeProcessPacket:
    @pytest.fixture
    def node(self, double_pf) -> FunctionNode:
        db = InMemoryArrowDatabase()
        return FunctionNode(
            function_pod=FunctionPod(packet_function=double_pf),
            input_stream=make_int_stream(n=3),
            pipeline_database=db,
        )

    def test_process_packet_returns_tag_and_packet(self, node):
        tag = Tag({"id": 0})
        packet = Packet({"x": 5})
        out_tag, out_packet = node.process_packet(tag, packet)
        assert out_tag is tag
        assert out_packet is not None

    def test_process_packet_value_correct(self, node):
        tag = Tag({"id": 0})
        packet = Packet({"x": 6})
        _, out_packet = node.process_packet(tag, packet)
        assert out_packet["result"] == 12  # 6 * 2

    def test_process_packet_adds_pipeline_record(self, node, double_pf):
        tag = Tag({"id": 0})
        packet = Packet({"x": 3})
        node.process_packet(tag, packet)
        db = node._pipeline_database
        db.flush()
        all_records = db.get_all_records(node.pipeline_path)
        assert all_records is not None
        assert all_records.num_rows >= 1

    def test_process_packet_second_call_same_input_deduplicates(self, node):
        tag = Tag({"id": 0})
        packet = Packet({"x": 3})
        node.process_packet(tag, packet)
        node.process_packet(tag, packet)
        db = node._pipeline_database
        db.flush()
        all_records = db.get_all_records(node.pipeline_path)
        assert all_records is not None
        assert all_records.num_rows == 1

    def test_process_two_packets_add_two_entries(self, node):
        tag = Tag({"id": 0})
        packet1 = Packet({"x": 3})
        packet2 = Packet({"x": 4})
        node.process_packet(tag, packet1)
        node.process_packet(tag, packet2)
        db = node._pipeline_database
        all_records = db.get_all_records(node.pipeline_path)
        assert all_records is not None
        assert all_records.num_rows == 2


# ---------------------------------------------------------------------------
# 4. iter_packets / run() stream interface
# ---------------------------------------------------------------------------


class TestFunctionNodeStreamInterface:
    @pytest.fixture
    def node(self, double_pf) -> FunctionNode:
        db = InMemoryArrowDatabase()
        return FunctionNode(
            function_pod=FunctionPod(packet_function=double_pf),
            input_stream=make_int_stream(n=3),
            pipeline_database=db,
        )

    def test_iter_packets_correct_values(self, node):
        assert [packet["result"] for _, packet in node.iter_packets()] == [0, 2, 4]

    def test_node_is_stream_protocol(self, node):
        assert isinstance(node, StreamProtocol)

    def test_dunder_iter_delegates_to_iter_packets(self, node):
        assert len(list(node)) == len(list(node.iter_packets()))

    def test_run_fills_database(self, node):
        node.run()
        records = node.get_all_records()
        assert records is not None
        assert records.num_rows == 3


# ---------------------------------------------------------------------------
# 5. pipeline identity
# ---------------------------------------------------------------------------


class TestFunctionNodePipelineIdentity:
    def test_pipeline_hash_same_schema_same_hash(self, double_pf):
        db = InMemoryArrowDatabase()
        node1 = FunctionNode(
            function_pod=FunctionPod(packet_function=double_pf),
            input_stream=make_int_stream(n=3),
            pipeline_database=db,
        )
        node2 = FunctionNode(
            function_pod=FunctionPod(packet_function=double_pf),
            input_stream=make_int_stream(n=5),  # different data, same schema
            pipeline_database=db,
        )
        assert node1.pipeline_hash() == node2.pipeline_hash()

    def test_pipeline_hash_different_data_same_hash(self, double_pf):
        db = InMemoryArrowDatabase()
        stream_a = make_int_stream(n=3)
        # Build a stream with same schema (id: int64, x: int64) but different values
        stream_b = ArrowTableStream(
            pa.table(
                {
                    "id": pa.array([10, 11, 12], type=pa.int64()),
                    "x": pa.array([100, 200, 300], type=pa.int64()),
                }
            ),
            tag_columns=["id"],
        )
        node_a = FunctionNode(
            function_pod=FunctionPod(packet_function=double_pf),
            input_stream=stream_a,
            pipeline_database=db,
        )
        node_b = FunctionNode(
            function_pod=FunctionPod(packet_function=double_pf),
            input_stream=stream_b,
            pipeline_database=db,
        )
        # Same schema → same pipeline hash
        assert node_a.pipeline_hash() == node_b.pipeline_hash()
        # Different data → different content hash
        assert node_a.content_hash() != node_b.content_hash()

    def test_pipeline_hash_is_consistent(self, double_pf):
        node = _make_node(double_pf, n=3)
        assert node.pipeline_hash() == node.pipeline_hash()

    def test_pipeline_node_hash_in_uri_is_schema_based(self, double_pf):
        """pipeline_node_hash in uri must be derived from pipeline_hash (schema-only),
        not content_hash (data-inclusive)."""
        db = InMemoryArrowDatabase()
        node1 = FunctionNode(
            function_pod=FunctionPod(packet_function=double_pf),
            input_stream=make_int_stream(n=3),
            pipeline_database=db,
        )
        node2 = FunctionNode(
            function_pod=FunctionPod(packet_function=double_pf),
            input_stream=make_int_stream(n=99),  # different data
            pipeline_database=db,
        )
        # Both nodes must have identical pipeline_paths since they share schema
        assert node1.pipeline_path == node2.pipeline_path


# ---------------------------------------------------------------------------
# 6. get_all_records — empty database
# ---------------------------------------------------------------------------


class TestGetAllRecordsEmpty:
    def test_returns_none_when_db_is_empty(self, double_pf):
        node = _make_node(double_pf, n=3)
        assert node.get_all_records() is None

    def test_returns_none_after_no_processing(self, double_pf):
        node = _make_node(double_pf, n=5)
        assert node.get_all_records(all_info=True) is None


# ---------------------------------------------------------------------------
# 7. get_all_records — basic correctness after population
# ---------------------------------------------------------------------------


class TestGetAllRecordsValues:
    @pytest.fixture
    def filled_node(self, double_pf) -> FunctionNode:
        node = _make_node(double_pf, n=4)
        _fill_node(node)
        return node

    def test_returns_pyarrow_table(self, filled_node):
        result = filled_node.get_all_records()
        assert isinstance(result, pa.Table)

    def test_row_count_matches_input(self, filled_node):
        result = filled_node.get_all_records()
        assert result is not None
        assert result.num_rows == 4

    def test_contains_tag_column(self, filled_node):
        result = filled_node.get_all_records()
        assert result is not None
        assert "id" in result.column_names

    def test_contains_output_packet_column(self, filled_node):
        result = filled_node.get_all_records()
        assert result is not None
        assert "result" in result.column_names

    def test_output_values_are_correct(self, filled_node):
        result = filled_node.get_all_records()
        assert result is not None
        assert sorted(result.column("result").to_pylist()) == [0, 2, 4, 6]

    def test_tag_values_are_correct(self, filled_node):
        result = filled_node.get_all_records()
        assert result is not None
        assert sorted(result.column("id").to_pylist()) == [0, 1, 2, 3]


# ---------------------------------------------------------------------------
# 8. get_all_records — ColumnConfig: meta columns
# ---------------------------------------------------------------------------


class TestGetAllRecordsMetaColumns:
    @pytest.fixture
    def filled_node(self, double_pf) -> FunctionNode:
        node = _make_node(double_pf, n=3)
        _fill_node(node)
        return node

    def test_default_excludes_meta_columns(self, filled_node):
        result = filled_node.get_all_records()
        assert result is not None
        meta_cols = [
            c for c in result.column_names if c.startswith(constants.META_PREFIX)
        ]
        assert meta_cols == [], f"Unexpected meta columns: {meta_cols}"

    def test_meta_true_includes_packet_record_id(self, filled_node):
        result = filled_node.get_all_records(columns={"meta": True})
        assert result is not None
        assert constants.PACKET_RECORD_ID in result.column_names

    def test_meta_true_includes_input_packet_hash(self, filled_node):
        result = filled_node.get_all_records(columns={"meta": True})
        assert result is not None
        assert constants.INPUT_PACKET_HASH_COL in result.column_names

    def test_meta_true_still_has_data_columns(self, filled_node):
        result = filled_node.get_all_records(columns={"meta": True})
        assert result is not None
        assert "id" in result.column_names
        assert "result" in result.column_names

    def test_input_packet_hash_values_are_non_empty_strings(self, filled_node):
        result = filled_node.get_all_records(columns={"meta": True})
        assert result is not None
        hashes = result.column(constants.INPUT_PACKET_HASH_COL).to_pylist()
        assert all(isinstance(h, str) and len(h) > 0 for h in hashes)

    def test_packet_record_id_values_are_non_empty_strings(self, filled_node):
        result = filled_node.get_all_records(columns={"meta": True})
        assert result is not None
        ids = result.column(constants.PACKET_RECORD_ID).to_pylist()
        assert all(isinstance(rid, str) and len(rid) > 0 for rid in ids)


# ---------------------------------------------------------------------------
# 9. get_all_records — ColumnConfig: source columns
# ---------------------------------------------------------------------------


class TestGetAllRecordsSourceColumns:
    @pytest.fixture
    def filled_node(self, double_pf) -> FunctionNode:
        node = _make_node(double_pf, n=3)
        _fill_node(node)
        return node

    def test_default_excludes_source_columns(self, filled_node):
        result = filled_node.get_all_records()
        assert result is not None
        source_cols = [
            c for c in result.column_names if c.startswith(constants.SOURCE_PREFIX)
        ]
        assert source_cols == [], f"Unexpected source columns: {source_cols}"

    def test_source_true_includes_source_columns(self, filled_node):
        result = filled_node.get_all_records(columns={"source": True})
        assert result is not None
        source_cols = [
            c for c in result.column_names if c.startswith(constants.SOURCE_PREFIX)
        ]
        assert len(source_cols) > 0

    def test_source_true_still_has_data_columns(self, filled_node):
        result = filled_node.get_all_records(columns={"source": True})
        assert result is not None
        assert "id" in result.column_names
        assert "result" in result.column_names


# ---------------------------------------------------------------------------
# 10. get_all_records — ColumnConfig: system_tags columns
# ---------------------------------------------------------------------------


class TestGetAllRecordsSystemTagColumns:
    @pytest.fixture
    def filled_node_with_sys_tags(self, double_pf) -> FunctionNode:
        node = _make_node_with_system_tags(double_pf, n=3)
        _fill_node(node)
        return node

    def test_default_excludes_system_tag_columns(self, filled_node_with_sys_tags):
        result = filled_node_with_sys_tags.get_all_records()
        assert result is not None
        sys_cols = [
            c for c in result.column_names if c.startswith(constants.SYSTEM_TAG_PREFIX)
        ]
        assert sys_cols == [], f"Unexpected system tag columns: {sys_cols}"

    def test_system_tags_true_includes_system_tag_columns(
        self, filled_node_with_sys_tags
    ):
        result = filled_node_with_sys_tags.get_all_records(
            columns={"system_tags": True}
        )
        assert result is not None
        sys_cols = [
            c for c in result.column_names if c.startswith(constants.SYSTEM_TAG_PREFIX)
        ]
        assert len(sys_cols) > 0

    def test_system_tags_true_still_has_data_columns(self, filled_node_with_sys_tags):
        result = filled_node_with_sys_tags.get_all_records(
            columns={"system_tags": True}
        )
        assert result is not None
        assert "id" in result.column_names
        assert "result" in result.column_names


# ---------------------------------------------------------------------------
# 11. get_all_records — all_info=True
# ---------------------------------------------------------------------------


class TestGetAllRecordsAllInfo:
    @pytest.fixture
    def filled_node(self, double_pf) -> FunctionNode:
        node = _make_node(double_pf, n=3)
        _fill_node(node)
        return node

    @pytest.fixture
    def filled_node_with_sys_tags(self, double_pf) -> FunctionNode:
        node = _make_node_with_system_tags(double_pf, n=3)
        _fill_node(node)
        return node

    def test_all_info_includes_meta_columns(self, filled_node):
        result = filled_node.get_all_records(all_info=True)
        assert result is not None
        meta_cols = [
            c for c in result.column_names if c.startswith(constants.META_PREFIX)
        ]
        assert len(meta_cols) > 0

    def test_all_info_includes_source_columns(self, filled_node):
        result = filled_node.get_all_records(all_info=True)
        assert result is not None
        source_cols = [
            c for c in result.column_names if c.startswith(constants.SOURCE_PREFIX)
        ]
        assert len(source_cols) > 0

    def test_all_info_includes_system_tag_columns(self, filled_node_with_sys_tags):
        result = filled_node_with_sys_tags.get_all_records(all_info=True)
        assert result is not None
        sys_cols = [
            c for c in result.column_names if c.startswith(constants.SYSTEM_TAG_PREFIX)
        ]
        assert len(sys_cols) > 0

    def test_all_info_has_more_columns_than_default(self, filled_node):
        default_result = filled_node.get_all_records()
        full_result = filled_node.get_all_records(all_info=True)
        assert full_result is not None
        assert default_result is not None
        assert full_result.num_columns > default_result.num_columns

    def test_all_info_data_columns_match_default(self, filled_node):
        default_result = filled_node.get_all_records()
        full_result = filled_node.get_all_records(all_info=True)
        assert default_result is not None
        assert full_result is not None
        assert sorted(default_result.column("id").to_pylist()) == sorted(
            full_result.column("id").to_pylist()
        )
        assert sorted(default_result.column("result").to_pylist()) == sorted(
            full_result.column("result").to_pylist()
        )


# ---------------------------------------------------------------------------
# 12. pipeline_path_prefix
# ---------------------------------------------------------------------------


class TestFunctionNodePipelinePathPrefix:
    def test_prefix_prepended_to_pipeline_path(self, double_pf):
        db = InMemoryArrowDatabase()
        prefix = ("my_pipeline", "stage_1")
        node = FunctionNode(
            function_pod=FunctionPod(packet_function=double_pf),
            input_stream=make_int_stream(n=2),
            pipeline_database=db,
            pipeline_path_prefix=prefix,
        )
        pipeline_path = node.pipeline_path
        assert pipeline_path[: len(prefix)] == prefix

    def test_no_prefix_pipeline_path_starts_with_pf_uri(self, double_pf):
        db = InMemoryArrowDatabase()
        node = FunctionNode(
            function_pod=FunctionPod(packet_function=double_pf),
            input_stream=make_int_stream(n=2),
            pipeline_database=db,
        )
        pf_uri = node._packet_function.uri
        assert node.pipeline_path[: len(pf_uri)] == pf_uri
        assert node.pipeline_path[-1].startswith("node:")


# ---------------------------------------------------------------------------
# 13. Result path conventions
# ---------------------------------------------------------------------------


class TestFunctionNodeResultPath:
    def test_result_records_stored_under_result_suffix_path(self, double_pf):
        db = InMemoryArrowDatabase()
        node = FunctionNode(
            function_pod=FunctionPod(packet_function=double_pf),
            input_stream=make_int_stream(n=2),
            pipeline_database=db,
        )
        tag = Tag({"id": 0})
        packet = Packet({"x": 5})
        node.process_packet(tag, packet)
        db.flush()

        result_path = node._packet_function.record_path
        assert result_path[-1] == "_result" or any(
            "_result" in part for part in result_path
        )
