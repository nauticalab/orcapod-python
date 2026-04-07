"""Tests for the table_scope flag on FunctionNode and OperatorNode.

Covers the two modes explicitly:
  - "pipeline_hash" (default): nodes with the same structure share one DB table,
    distinguished at read-time by the ``_node_content_hash`` row column.
  - "content_hash" (legacy): every node gets its own DB table path
    (``instance:{content_hash}`` segment appended).
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.core.function_pod import FunctionPod
from orcapod.core.nodes import FunctionNode, OperatorNode
from orcapod.core.operators import Join
from orcapod.core.packet_function import PythonPacketFunction
from orcapod.core.sources import ArrowTableSource, DictSource
from orcapod.databases import InMemoryArrowDatabase
from orcapod.system_constants import SystemConstant
from orcapod.types import CacheMode


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_pod() -> FunctionPod:
    """Pod that doubles the 'y' packet column (tag column is 'x')."""

    def double(y: int) -> int:
        return y * 2

    pf = PythonPacketFunction(function=double, output_keys=["result"])
    return FunctionPod(packet_function=pf)


def _make_source(data: list[dict], source_id: str = "src") -> DictSource:
    """Source with tag column 'x' and packet column 'y'."""
    return DictSource(data=data, tag_columns=["x"], source_id=source_id)


def _make_join_streams(
    vals: list[int], source_id_suffix: str = "1"
) -> tuple[ArrowTableSource, ArrowTableSource]:
    """Two ArrowTableSources with key/val and key/score columns.

    Distinct ``source_id_suffix`` values guarantee different content_hash when
    the ArrowTableSource identity includes its source_id.
    """
    table_a = pa.table(
        {
            "key": pa.array([str(v) for v in vals], type=pa.large_string()),
            "val": pa.array(vals, type=pa.int64()),
        }
    )
    table_b = pa.table(
        {
            "key": pa.array([str(v) for v in vals], type=pa.large_string()),
            "score": pa.array(vals, type=pa.int64()),
        }
    )
    src_a = ArrowTableSource(
        table_a, tag_columns=["key"], source_id=f"src_a_{source_id_suffix}", infer_nullable=True
    )
    src_b = ArrowTableSource(
        table_b, tag_columns=["key"], source_id=f"src_b_{source_id_suffix}", infer_nullable=True
    )
    return src_a, src_b


# ---------------------------------------------------------------------------
# FunctionNode — table_scope="pipeline_hash" (default)
# ---------------------------------------------------------------------------


class TestFunctionNodePipelineHashScope:
    """Default table_scope="pipeline_hash": shared table, per-run isolation via content hash."""

    def test_default_scope_is_pipeline_hash(self):
        db = InMemoryArrowDatabase()
        pod = _make_pod()
        src = _make_source([{"x": 1, "y": 2}])
        node = FunctionNode(function_pod=pod, input_stream=src, pipeline_database=db)
        assert node._table_scope == "pipeline_hash"

    def test_node_identity_path_ends_with_schema_only(self):
        db = InMemoryArrowDatabase()
        pod = _make_pod()
        src = _make_source([{"x": 1, "y": 2}])
        node = FunctionNode(function_pod=pod, input_stream=src, pipeline_database=db)
        path = node.node_identity_path
        assert path[-1].startswith("schema:"), f"Expected schema:... got {path[-1]!r}"
        assert not any(seg.startswith("instance:") for seg in path)

    def test_two_nodes_same_function_same_schema_share_path(self):
        """Same function + same source schema → same pipeline_hash → same node_identity_path."""
        db = InMemoryArrowDatabase()
        pod = _make_pod()
        # Both use same source_id → same schema structure → same pipeline_hash
        src_a = _make_source([{"x": 1, "y": 10}], source_id="src")
        src_b = _make_source([{"x": 2, "y": 20}], source_id="src")
        node_a = FunctionNode(function_pod=pod, input_stream=src_a, pipeline_database=db)
        node_b = FunctionNode(function_pod=pod, input_stream=src_b, pipeline_database=db)

        assert node_a.node_identity_path == node_b.node_identity_path
        assert node_a.pipeline_hash() == node_b.pipeline_hash()

    def test_node_content_hash_col_in_pipeline_records(self):
        """After run(), pipeline records contain _node_content_hash column."""
        db = InMemoryArrowDatabase()
        pod = _make_pod()
        src = _make_source([{"x": 1, "y": 2}])
        node = FunctionNode(function_pod=pod, input_stream=src, pipeline_database=db)
        node.run()

        col_name = SystemConstant().NODE_CONTENT_HASH_COL
        pipeline_table = node._pipeline_database.get_all_records(node.node_identity_path)
        assert pipeline_table is not None
        assert col_name in pipeline_table.column_names

    def test_get_all_records_drops_node_content_hash_col(self):
        """Consumer-facing records must NOT expose _node_content_hash."""
        db = InMemoryArrowDatabase()
        pod = _make_pod()
        src = _make_source([{"x": 1, "y": 2}])
        node = FunctionNode(function_pod=pod, input_stream=src, pipeline_database=db)
        node.run()

        records = node.get_all_records()
        col_name = SystemConstant().NODE_CONTENT_HASH_COL
        assert col_name not in records.column_names

    def test_isolation_two_nodes_share_table_see_only_own_records(self):
        """Two nodes sharing a DB path each see only their own records via content_hash filter."""
        db = InMemoryArrowDatabase()
        pod = _make_pod()
        # Different source_ids → different content_hash → different _node_content_hash rows
        src_a = _make_source([{"x": 1, "y": 10}], source_id="source_a")
        src_b = _make_source([{"x": 2, "y": 20}], source_id="source_b")
        node_a = FunctionNode(function_pod=pod, input_stream=src_a, pipeline_database=db)
        node_b = FunctionNode(function_pod=pod, input_stream=src_b, pipeline_database=db)

        node_a.run()
        node_b.run()

        records_a = node_a.get_all_records()
        records_b = node_b.get_all_records()

        # Each node returns only its own 1 record
        assert records_a.num_rows == 1
        assert records_b.num_rows == 1

        # Results are correct per-node
        assert records_a.column("result")[0].as_py() == 20  # 10 * 2
        assert records_b.column("result")[0].as_py() == 40  # 20 * 2


# ---------------------------------------------------------------------------
# FunctionNode — table_scope="content_hash" (legacy)
# ---------------------------------------------------------------------------


class TestFunctionNodeContentHashScope:
    """Legacy table_scope="content_hash": per-run isolated tables."""

    def test_scope_set_to_content_hash(self):
        db = InMemoryArrowDatabase()
        pod = _make_pod()
        src = _make_source([{"x": 1, "y": 2}])
        node = FunctionNode(
            function_pod=pod, input_stream=src, pipeline_database=db, table_scope="content_hash"
        )
        assert node._table_scope == "content_hash"

    def test_node_identity_path_ends_with_schema_and_instance(self):
        db = InMemoryArrowDatabase()
        pod = _make_pod()
        src = _make_source([{"x": 1, "y": 2}])
        node = FunctionNode(
            function_pod=pod, input_stream=src, pipeline_database=db, table_scope="content_hash"
        )
        path = node.node_identity_path
        assert path[-2].startswith("schema:"), f"Expected schema:... got {path[-2]!r}"
        assert path[-1].startswith("instance:"), f"Expected instance:... got {path[-1]!r}"
        assert node.pipeline_hash().to_string() in path[-2]
        assert node.content_hash().to_string() in path[-1]

    def test_two_nodes_different_source_id_have_different_paths(self):
        """Different source_id → different content_hash → different node_identity_path."""
        db = InMemoryArrowDatabase()
        pod = _make_pod()
        src_a = _make_source([{"x": 1, "y": 10}], source_id="source_a")
        src_b = _make_source([{"x": 2, "y": 20}], source_id="source_b")
        node_a = FunctionNode(
            function_pod=pod, input_stream=src_a, pipeline_database=db, table_scope="content_hash"
        )
        node_b = FunctionNode(
            function_pod=pod, input_stream=src_b, pipeline_database=db, table_scope="content_hash"
        )
        assert node_a.content_hash() != node_b.content_hash()
        assert node_a.node_identity_path != node_b.node_identity_path

    def test_pipeline_hash_still_equal_across_content_hash_nodes(self):
        """Even in content_hash scope, pipeline_hash is the same for same function structure."""
        db = InMemoryArrowDatabase()
        pod = _make_pod()
        # Same source_id → same pipeline_hash
        src_a = _make_source([{"x": 1, "y": 10}], source_id="src")
        src_b = _make_source([{"x": 2, "y": 20}], source_id="src")
        node_a = FunctionNode(
            function_pod=pod, input_stream=src_a, pipeline_database=db, table_scope="content_hash"
        )
        node_b = FunctionNode(
            function_pod=pod, input_stream=src_b, pipeline_database=db, table_scope="content_hash"
        )
        # pipeline_hash same → same schema: segment
        assert node_a.pipeline_hash() == node_b.pipeline_hash()
        assert node_a.node_identity_path[-2] == node_b.node_identity_path[-2]


# ---------------------------------------------------------------------------
# FunctionNode — from_descriptor validation
# ---------------------------------------------------------------------------


class TestFunctionNodeDescriptorTableScope:
    def test_from_descriptor_missing_table_scope_raises(self):
        """Descriptor without table_scope must raise ValueError."""
        pod = _make_pod()
        src = _make_source([{"x": 1, "y": 2}])
        db = InMemoryArrowDatabase()
        node = FunctionNode(function_pod=pod, input_stream=src, pipeline_database=db)
        tag_schema, packet_schema = node.output_schema()
        descriptor = {
            "node_type": "function",
            "label": None,
            "content_hash": node.content_hash().to_string(),
            "pipeline_hash": node.pipeline_hash().to_string(),
            "data_context_key": node.data_context_key,
            # "table_scope" intentionally omitted
            "output_schema": {
                "tag": {k: str(v) for k, v in tag_schema.items()},
                "packet": {k: str(v) for k, v in packet_schema.items()},
            },
        }
        with pytest.raises(ValueError, match="table_scope"):
            FunctionNode.from_descriptor(
                descriptor=descriptor,
                function_pod=None,
                input_stream=None,
                databases={},
            )

    def test_from_descriptor_preserves_pipeline_hash_scope(self):
        pod = _make_pod()
        src = _make_source([{"x": 1, "y": 2}])
        db = InMemoryArrowDatabase()
        node = FunctionNode(function_pod=pod, input_stream=src, pipeline_database=db)
        tag_schema, packet_schema = node.output_schema()
        descriptor = {
            "node_type": "function",
            "label": None,
            "content_hash": node.content_hash().to_string(),
            "pipeline_hash": node.pipeline_hash().to_string(),
            "data_context_key": node.data_context_key,
            "table_scope": "pipeline_hash",
            "output_schema": {
                "tag": {k: str(v) for k, v in tag_schema.items()},
                "packet": {k: str(v) for k, v in packet_schema.items()},
            },
        }
        loaded = FunctionNode.from_descriptor(
            descriptor=descriptor,
            function_pod=None,
            input_stream=None,
            databases={},
        )
        assert loaded._table_scope == "pipeline_hash"

    def test_from_descriptor_preserves_content_hash_scope(self):
        pod = _make_pod()
        src = _make_source([{"x": 1, "y": 2}])
        db = InMemoryArrowDatabase()
        node = FunctionNode(
            function_pod=pod, input_stream=src, pipeline_database=db, table_scope="content_hash"
        )
        tag_schema, packet_schema = node.output_schema()
        descriptor = {
            "node_type": "function",
            "label": None,
            "content_hash": node.content_hash().to_string(),
            "pipeline_hash": node.pipeline_hash().to_string(),
            "data_context_key": node.data_context_key,
            "table_scope": "content_hash",
            "output_schema": {
                "tag": {k: str(v) for k, v in tag_schema.items()},
                "packet": {k: str(v) for k, v in packet_schema.items()},
            },
        }
        loaded = FunctionNode.from_descriptor(
            descriptor=descriptor,
            function_pod=None,
            input_stream=None,
            databases={},
        )
        assert loaded._table_scope == "content_hash"


# ---------------------------------------------------------------------------
# OperatorNode — table_scope="pipeline_hash" (default)
# ---------------------------------------------------------------------------


class TestOperatorNodePipelineHashScope:
    def test_default_scope_is_pipeline_hash(self):
        db = InMemoryArrowDatabase()
        src_a, src_b = _make_join_streams([1, 2], "x")
        node = OperatorNode(
            operator=Join(),
            input_streams=(src_a, src_b),
            pipeline_database=db,
        )
        assert node._table_scope == "pipeline_hash"

    def test_node_identity_path_ends_with_schema_only(self):
        db = InMemoryArrowDatabase()
        src_a, src_b = _make_join_streams([1, 2], "x")
        node = OperatorNode(
            operator=Join(),
            input_streams=(src_a, src_b),
            pipeline_database=db,
        )
        path = node.node_identity_path
        assert path[-1].startswith("schema:"), f"Expected schema:... got {path[-1]!r}"
        assert not any(seg.startswith("instance:") for seg in path)

    def test_two_nodes_same_operator_same_schema_share_path(self):
        """Same operator + same schema (same source_id prefix) → same pipeline_hash → same path."""
        db = InMemoryArrowDatabase()
        # Same source_id_suffix → same source_id → same pipeline_hash
        src_a1, src_b1 = _make_join_streams([1, 2], "x")
        src_a2, src_b2 = _make_join_streams([3, 4], "x")
        node1 = OperatorNode(
            operator=Join(),
            input_streams=(src_a1, src_b1),
            pipeline_database=db,
        )
        node2 = OperatorNode(
            operator=Join(),
            input_streams=(src_a2, src_b2),
            pipeline_database=db,
        )
        assert node1.node_identity_path == node2.node_identity_path
        assert node1.pipeline_hash() == node2.pipeline_hash()

    def test_two_nodes_different_source_ids_have_different_content_hash(self):
        """Different source_ids → different content_hash for OperatorNode."""
        db = InMemoryArrowDatabase()
        src_a1, src_b1 = _make_join_streams([1, 2], "run1")
        src_a2, src_b2 = _make_join_streams([1, 2], "run2")
        node1 = OperatorNode(
            operator=Join(),
            input_streams=(src_a1, src_b1),
            pipeline_database=db,
        )
        node2 = OperatorNode(
            operator=Join(),
            input_streams=(src_a2, src_b2),
            pipeline_database=db,
        )
        assert node1.content_hash() != node2.content_hash()

    def test_isolation_two_nodes_share_table_see_only_own_records(self):
        """Two OperatorNodes sharing a DB path each see only their own records."""
        db = InMemoryArrowDatabase()
        src_a1, src_b1 = _make_join_streams([1, 2], "run1")
        src_a2, src_b2 = _make_join_streams([3, 4], "run2")
        node1 = OperatorNode(
            operator=Join(),
            input_streams=(src_a1, src_b1),
            pipeline_database=db,
            cache_mode=CacheMode.LOG,
        )
        node2 = OperatorNode(
            operator=Join(),
            input_streams=(src_a2, src_b2),
            pipeline_database=db,
            cache_mode=CacheMode.LOG,
        )

        node1.run()
        node2.run()

        records1 = node1.get_all_records()
        records2 = node2.get_all_records()

        assert records1 is not None
        assert records2 is not None

        # Each has 2 join results (matching keys)
        assert records1.num_rows == 2
        assert records2.num_rows == 2

        # Values are correct per node
        keys1 = set(records1.column("key").to_pylist())
        keys2 = set(records2.column("key").to_pylist())
        assert keys1 == {"1", "2"}
        assert keys2 == {"3", "4"}


# ---------------------------------------------------------------------------
# OperatorNode — table_scope="content_hash" (legacy)
# ---------------------------------------------------------------------------


class TestOperatorNodeContentHashScope:
    def test_node_identity_path_ends_with_schema_and_instance(self):
        db = InMemoryArrowDatabase()
        src_a, src_b = _make_join_streams([1, 2], "x")
        node = OperatorNode(
            operator=Join(),
            input_streams=(src_a, src_b),
            pipeline_database=db,
            table_scope="content_hash",
        )
        path = node.node_identity_path
        assert path[-2].startswith("schema:"), f"Expected schema:... got {path[-2]!r}"
        assert path[-1].startswith("instance:"), f"Expected instance:... got {path[-1]!r}"
        assert node.pipeline_hash().to_string() in path[-2]
        assert node.content_hash().to_string() in path[-1]

    def test_two_nodes_different_source_ids_have_different_paths(self):
        db = InMemoryArrowDatabase()
        src_a1, src_b1 = _make_join_streams([1, 2], "run1")
        src_a2, src_b2 = _make_join_streams([1, 2], "run2")
        node1 = OperatorNode(
            operator=Join(),
            input_streams=(src_a1, src_b1),
            pipeline_database=db,
            table_scope="content_hash",
        )
        node2 = OperatorNode(
            operator=Join(),
            input_streams=(src_a2, src_b2),
            pipeline_database=db,
            table_scope="content_hash",
        )
        assert node1.content_hash() != node2.content_hash()
        assert node1.node_identity_path != node2.node_identity_path

    def test_pipeline_hash_same_across_content_hash_nodes_with_same_schema(self):
        """Even in content_hash scope, pipeline_hash is the same for same operator+schema."""
        db = InMemoryArrowDatabase()
        # Same source_id_suffix → same source_ids → same pipeline_hash AND same content_hash
        # (ArrowTableSource hashes by schema/source_id, not raw data values)
        src_a1, src_b1 = _make_join_streams([1, 2], "x")
        src_a2, src_b2 = _make_join_streams([3, 4], "x")
        node1 = OperatorNode(
            operator=Join(),
            input_streams=(src_a1, src_b1),
            pipeline_database=db,
            table_scope="content_hash",
        )
        node2 = OperatorNode(
            operator=Join(),
            input_streams=(src_a2, src_b2),
            pipeline_database=db,
            table_scope="content_hash",
        )
        assert node1.pipeline_hash() == node2.pipeline_hash()
        # schema: segments match
        assert node1.node_identity_path[-2] == node2.node_identity_path[-2]


# ---------------------------------------------------------------------------
# OperatorNode — from_descriptor validation
# ---------------------------------------------------------------------------


class TestOperatorNodeDescriptorTableScope:
    def test_from_descriptor_missing_table_scope_raises(self):
        from orcapod.core.nodes.operator_node import OperatorNode as ON

        db = InMemoryArrowDatabase()
        descriptor = {
            "node_type": "operator",
            "label": None,
            "content_hash": "fake_hash",
            "pipeline_hash": "fake_pipeline_hash",
            "data_context_key": "std:v0.1:default",
            # "table_scope" intentionally omitted
            "output_schema": {"tag": {"key": "large_string"}, "packet": {"val": "int64"}},
            "operator": {
                "class_name": "Join",
                "module_path": "orcapod.core.operators.join",
                "config": {},
            },
            "cache_mode": "OFF",
        }
        with pytest.raises(ValueError, match="table_scope"):
            ON.from_descriptor(
                descriptor=descriptor,
                operator=None,
                input_streams=(),
                databases={"pipeline": db},
            )

    def test_from_descriptor_preserves_pipeline_hash_scope(self):
        from orcapod.core.nodes.operator_node import OperatorNode as ON

        db = InMemoryArrowDatabase()
        descriptor = {
            "node_type": "operator",
            "label": None,
            "content_hash": "fake_hash",
            "pipeline_hash": "fake_pipeline_hash",
            "data_context_key": "std:v0.1:default",
            "table_scope": "pipeline_hash",
            "output_schema": {"tag": {"key": "large_string"}, "packet": {"val": "int64"}},
            "operator": {
                "class_name": "Join",
                "module_path": "orcapod.core.operators.join",
                "config": {},
            },
            "cache_mode": "OFF",
        }
        loaded = ON.from_descriptor(
            descriptor=descriptor,
            operator=None,
            input_streams=(),
            databases={"pipeline": db},
        )
        assert loaded._table_scope == "pipeline_hash"

    def test_from_descriptor_preserves_content_hash_scope(self):
        from orcapod.core.nodes.operator_node import OperatorNode as ON

        db = InMemoryArrowDatabase()
        descriptor = {
            "node_type": "operator",
            "label": None,
            "content_hash": "fake_hash",
            "pipeline_hash": "fake_pipeline_hash",
            "data_context_key": "std:v0.1:default",
            "table_scope": "content_hash",
            "output_schema": {"tag": {"key": "large_string"}, "packet": {"val": "int64"}},
            "operator": {
                "class_name": "Join",
                "module_path": "orcapod.core.operators.join",
                "config": {},
            },
            "cache_mode": "OFF",
        }
        loaded = ON.from_descriptor(
            descriptor=descriptor,
            operator=None,
            input_streams=(),
            databases={"pipeline": db},
        )
        assert loaded._table_scope == "content_hash"


# ---------------------------------------------------------------------------
# Cache invalidation — node_identity_path_cache cleared on clear_cache
# ---------------------------------------------------------------------------


class TestNodeIdentityPathCacheInvalidation:
    def test_function_node_cache_cleared_on_clear_cache(self):
        db = InMemoryArrowDatabase()
        pod = _make_pod()
        src = _make_source([{"x": 1, "y": 2}])
        node = FunctionNode(function_pod=pod, input_stream=src, pipeline_database=db)
        _ = node.node_identity_path  # populate cache
        assert node._node_identity_path_cache is not None
        node.clear_cache()
        assert node._node_identity_path_cache is None

    def test_operator_node_cache_cleared_on_clear_cache(self):
        db = InMemoryArrowDatabase()
        src_a, src_b = _make_join_streams([1], "x")
        node = OperatorNode(
            operator=Join(),
            input_streams=(src_a, src_b),
            pipeline_database=db,
        )
        _ = node.node_identity_path  # populate cache
        assert node._node_identity_path_cache is not None
        node.clear_cache()
        assert node._node_identity_path_cache is None
