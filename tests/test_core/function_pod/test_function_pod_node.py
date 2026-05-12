"""
Tests for FunctionNode covering:
- Construction, pipeline_path, uri
- output_schema and keys
- execute_data and add_pipeline_record
- iter_data, run(), stream interface
- get_all_records: empty DB, correctness, ColumnConfig (meta/source/system_tags/all_info)
- pipeline_identity_structure and pipeline_hash
- pipeline_path_prefix
- result path conventions
"""

from __future__ import annotations

from collections.abc import Mapping

import pyarrow as pa
import pytest

from orcapod.core.datagrams import Data, Tag
from orcapod.core.function_pod import FunctionPod
from orcapod.core.nodes import FunctionNode
from orcapod.core.data_function import PythonDataFunction
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
    pf: PythonDataFunction,
    n: int = 3,
    db: InMemoryArrowDatabase | None = None,
) -> FunctionNode:
    if db is None:
        db = InMemoryArrowDatabase()
    return FunctionNode(
        function_pod=FunctionPod(data_function=pf),
        input_stream=make_int_stream(n=n),
        pipeline_database=db,
    )


def _make_node_with_system_tags(
    pf: PythonDataFunction,
    n: int = 3,
    db: InMemoryArrowDatabase | None = None,
) -> FunctionNode:
    """Build a node whose input stream has an explicit system-tag column ('run')."""
    if db is None:
        db = InMemoryArrowDatabase()
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("run", pa.large_string(), nullable=False),
            pa.field("x", pa.int64(), nullable=False),
        ]
    )
    table = pa.table(
        {
            "id": list(range(n)),
            "run": [f"r{i}" for i in range(n)],
            "x": list(range(n)),
        },
        schema=schema,
    )
    stream = ArrowTableStream(table, tag_columns=["id"], system_tag_columns=["run"])
    return FunctionNode(
        function_pod=FunctionPod(data_function=pf),
        input_stream=stream,
        pipeline_database=db,
    )


def _fill_node(node: FunctionNode) -> None:
    """Process all data so the DB is populated."""
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
            function_pod=FunctionPod(data_function=double_pf),
            input_stream=stream,
            pipeline_database=db,
        )

    def test_construction_succeeds(self, node):
        assert node is not None

    def test_pipeline_path_is_tuple_of_strings(self, node):
        path = node.node_identity_path
        assert isinstance(path, tuple)
        assert all(isinstance(p, str) for p in path)

    def test_node_hash_is_equal_to_wrapped_function_pod_output_stream(
        self, node: FunctionNode
    ) -> None:
        output_stream = node._function_pod.process(node._input_stream)
        node_hash = node.content_hash().to_string()
        stream_hash = output_stream.content_hash().to_string()
        assert node_hash == stream_hash

    def test_pipeline_path_ends_with_schema_hash(self, node):
        path = node.node_identity_path
        # With pipeline_hash scope (default): path ends with schema:{hash} only
        assert path[-1].startswith("schema:")
        assert not any(seg.startswith("instance:") for seg in path)

    def test_pipeline_path_contains_data_function_uri(self, node):
        pf_uri = node._data_function.uri
        for part in pf_uri:
            assert part in node.node_identity_path

    def test_pipeline_path_has_no_tag_schema_hash(self, node):
        path = node.node_identity_path
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
                function_pod=FunctionPod(data_function=double_pf),
                input_stream=bad_stream,
                pipeline_database=db,
            )

    def test_result_database_defaults_to_pipeline_database(self, double_pf):
        db = InMemoryArrowDatabase()
        node = FunctionNode(
            function_pod=FunctionPod(data_function=double_pf),
            input_stream=make_int_stream(n=2),
            pipeline_database=db,
        )
        assert node._pipeline_database is db

    def test_separate_result_database_accepted(self, double_pf):
        pipeline_db = InMemoryArrowDatabase()
        result_db = InMemoryArrowDatabase()
        node = FunctionNode(
            function_pod=FunctionPod(data_function=double_pf),
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
            function_pod=FunctionPod(data_function=double_pf),
            input_stream=make_int_stream(n=3),
            pipeline_database=db,
        )

    def test_output_schema_returns_two_mappings(self, node: FunctionNode):
        tag_schema, data_schema = node.output_schema()
        assert isinstance(tag_schema, Mapping)
        assert isinstance(data_schema, Mapping)
        assert "id" in tag_schema
        assert len(tag_schema) == 1
        assert "result" in data_schema
        assert len(data_schema) == 1
        assert tag_schema["id"] is int
        assert data_schema["result"] is int

    def test_data_schema_matches_function_output(self, node, double_pf):
        _, data_schema = node.output_schema()
        assert data_schema == double_pf.output_data_schema

    def test_tag_schema_matches_input_stream(self, node):
        tag_schema, _ = node.output_schema()
        assert "id" in tag_schema
        assert tag_schema["id"] is int


# ---------------------------------------------------------------------------
# 3. execute_data and add_pipeline_record
# ---------------------------------------------------------------------------


class TestFunctionNodeExecuteData:
    @pytest.fixture
    def node(self, double_pf) -> FunctionNode:
        db = InMemoryArrowDatabase()
        return FunctionNode(
            function_pod=FunctionPod(data_function=double_pf),
            input_stream=make_int_stream(n=3),
            pipeline_database=db,
        )

    def test_execute_data_returns_tag_and_data(self, node):
        tag = Tag({"id": 0})
        data = Data({"x": 5})
        out_tag, out_data = node.execute_data(tag, data)
        assert out_tag is tag
        assert out_data is not None

    def test_execute_data_value_correct(self, node):
        tag = Tag({"id": 0})
        data = Data({"x": 6})
        _, out_data = node.execute_data(tag, data)
        assert out_data["result"] == 12  # 6 * 2

    def test_execute_data_adds_pipeline_record(self, node, double_pf):
        """execute_data writes pipeline records (compute + persist + cache)."""
        tag = Tag({"id": 0})
        data = Data({"x": 3})
        node.execute_data(tag, data)
        db = node._pipeline_database
        db.flush()
        all_records = db.get_all_records(node.node_identity_path)
        assert all_records is not None
        assert all_records.num_rows >= 1

    def test_execute_data_internal_adds_pipeline_record(self, node, double_pf):
        tag = Tag({"id": 0})
        data = Data({"x": 3})
        node._process_data_internal(tag, data)
        db = node._pipeline_database
        db.flush()
        all_records = db.get_all_records(node.node_identity_path)
        assert all_records is not None
        assert all_records.num_rows >= 1

    def test_execute_data_second_call_same_input_deduplicates(self, node):
        tag = Tag({"id": 0})
        data = Data({"x": 3})
        node._process_data_internal(tag, data)
        node._process_data_internal(tag, data)
        db = node._pipeline_database
        db.flush()
        all_records = db.get_all_records(node.node_identity_path)
        assert all_records is not None
        assert all_records.num_rows == 1

    def test_process_and_store_two_data_add_two_entries(self, node):
        tag = Tag({"id": 0})
        data1 = Data({"x": 3})
        data2 = Data({"x": 4})
        node._process_data_internal(tag, data1)
        node._process_data_internal(tag, data2)
        db = node._pipeline_database
        all_records = db.get_all_records(node.node_identity_path)
        assert all_records is not None
        assert all_records.num_rows == 2


# ---------------------------------------------------------------------------
# 4. iter_data / run() stream interface
# ---------------------------------------------------------------------------


class TestFunctionNodeStreamInterface:
    @pytest.fixture
    def node(self, double_pf) -> FunctionNode:
        db = InMemoryArrowDatabase()
        node = FunctionNode(
            function_pod=FunctionPod(data_function=double_pf),
            input_stream=make_int_stream(n=3),
            pipeline_database=db,
        )
        node.run()
        return node

    def test_iter_data_correct_values(self, node):
        assert [data["result"] for _, data in node.iter_data()] == [0, 2, 4]

    def test_node_is_stream_protocol(self, node):
        assert isinstance(node, StreamProtocol)

    def test_dunder_iter_delegates_to_iter_data(self, node):
        assert len(list(node)) == len(list(node.iter_data()))

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
            function_pod=FunctionPod(data_function=double_pf),
            input_stream=make_int_stream(n=3),
            pipeline_database=db,
        )
        node2 = FunctionNode(
            function_pod=FunctionPod(data_function=double_pf),
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
                    "id": [10, 11, 12],
                    "x": [100, 200, 300],
                },
                schema=pa.schema(
                    [pa.field("id", pa.int64(), nullable=False), pa.field("x", pa.int64(), nullable=False)]
                ),
            ),
            tag_columns=["id"],
        )
        node_a = FunctionNode(
            function_pod=FunctionPod(data_function=double_pf),
            input_stream=stream_a,
            pipeline_database=db,
        )
        node_b = FunctionNode(
            function_pod=FunctionPod(data_function=double_pf),
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
        """node_identity_path uses only schema:{pipeline_hash} (pipeline_hash scope default).
        Two nodes with same schema share the same full path; per-run isolation
        is achieved via the _node_content_hash row column."""
        db = InMemoryArrowDatabase()
        node1 = FunctionNode(
            function_pod=FunctionPod(data_function=double_pf),
            input_stream=make_int_stream(n=3),
            pipeline_database=db,
        )
        node2 = FunctionNode(
            function_pod=FunctionPod(data_function=double_pf),
            input_stream=make_int_stream(n=99),  # different data
            pipeline_database=db,
        )
        # Same schema → same pipeline_hash → SAME full path
        assert node1.node_identity_path == node2.node_identity_path
        assert node1.node_identity_path[-1].startswith("schema:")


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

    def test_contains_output_data_column(self, filled_node):
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

    def test_meta_true_includes_data_record_id(self, filled_node):
        result = filled_node.get_all_records(columns={"meta": True})
        assert result is not None
        assert constants.DATA_RECORD_ID in result.column_names

    def test_meta_true_includes_input_data_hash(self, filled_node):
        result = filled_node.get_all_records(columns={"meta": True})
        assert result is not None
        assert constants.INPUT_DATA_HASH_COL in result.column_names

    def test_meta_true_still_has_data_columns(self, filled_node):
        result = filled_node.get_all_records(columns={"meta": True})
        assert result is not None
        assert "id" in result.column_names
        assert "result" in result.column_names

    def test_input_data_hash_values_are_non_empty_strings(self, filled_node):
        result = filled_node.get_all_records(columns={"meta": True})
        assert result is not None
        hashes = result.column(constants.INPUT_DATA_HASH_COL).to_pylist()
        assert all(isinstance(h, str) and len(h) > 0 for h in hashes)

    def test_data_record_id_values_are_non_empty_strings(self, filled_node):
        result = filled_node.get_all_records(columns={"meta": True})
        assert result is not None
        ids = result.column(constants.DATA_RECORD_ID).to_pylist()
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
# 12. node_identity_path structure
# ---------------------------------------------------------------------------


class TestFunctionNodeIdentityPath:
    def test_node_identity_path_starts_with_pf_uri(self, double_pf):
        db = InMemoryArrowDatabase()
        node = FunctionNode(
            function_pod=FunctionPod(data_function=double_pf),
            input_stream=make_int_stream(n=2),
            pipeline_database=db,
        )
        pf_uri = node._data_function.uri
        assert node.node_identity_path[: len(pf_uri)] == pf_uri
        # With pipeline_hash scope (default): ends with schema:{hash} only
        assert node.node_identity_path[-1].startswith("schema:")
        assert not any(seg.startswith("instance:") for seg in node.node_identity_path)


# ---------------------------------------------------------------------------
# 13. Result path conventions
# ---------------------------------------------------------------------------


class TestFunctionNodeResultPath:
    def test_result_records_stored_under_pod_uri_path(self, double_pf):
        """Result records are stored under the pod's URI — no _result prefix
        since the database is pre-scoped at compile time (ENG-340/ENG-349)."""
        db = InMemoryArrowDatabase()
        pod = FunctionPod(data_function=double_pf)
        node = FunctionNode(
            function_pod=pod,
            input_stream=make_int_stream(n=2),
            pipeline_database=db,
        )
        tag = Tag({"id": 0})
        data = Data({"x": 5})
        node.execute_data(tag, data)
        db.flush()

        result_path = node._cached_function_pod.record_path
        assert result_path == pod.uri
