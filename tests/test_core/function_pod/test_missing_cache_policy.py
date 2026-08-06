"""Tests for NodeConfig.missing_cache_policy — ITL-604."""
from __future__ import annotations

import logging

import pyarrow as pa
import pytest

from orcapod.core.data_function import PythonDataFunction
from orcapod.core.datagrams.tag_data import EmptyData
from orcapod.core.function_pod import FunctionPod
from orcapod.core.nodes.function_node import FunctionJobNode
from orcapod.core.streams.arrow_table_stream import ArrowTableStream
from orcapod.databases import InMemoryArrowDatabase
from orcapod.errors import CacheMissError, EphemeralResultMissingError
from orcapod.types import NodeConfig


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _double(x: int) -> int:
    return x * 2


def _make_stream(rows: list[dict]) -> ArrowTableStream:
    keys = list(rows[0].keys())
    table = pa.table(
        {k: pa.array([r[k] for r in rows], type=pa.int64()) for k in keys},
        schema=pa.schema([pa.field(k, pa.int64(), nullable=False) for k in keys]),
    )
    return ArrowTableStream(table, tag_columns=["id"])


def _make_node(
    stream: ArrowTableStream,
    pipeline_db: InMemoryArrowDatabase,
    result_db: InMemoryArrowDatabase,
    missing_cache_policy: str | None = None,
) -> FunctionJobNode:
    pf = PythonDataFunction(_double, output_keys="result")
    pod = FunctionPod(pf)
    node = FunctionJobNode(
        function_pod=pod,
        input_stream=stream,
        pipeline_database=pipeline_db,
        result_database=result_db,
    )
    if missing_cache_policy is not None:
        node.node_config = NodeConfig(missing_cache_policy=missing_cache_policy)
    return node


def _wipe_result_db(result_db: InMemoryArrowDatabase) -> None:
    """Simulate data loss by clearing the result store."""
    result_db._tables.clear()
    result_db._pending_batches.clear()
    result_db._pending_record_ids.clear()


# ---------------------------------------------------------------------------
# Task 1: NodeConfig + CacheMissError types
# ---------------------------------------------------------------------------

class TestNodeConfigMissingCachePolicy:
    def test_default_is_none(self):
        cfg = NodeConfig()
        assert cfg.missing_cache_policy is None

    def test_accepts_valid_values(self):
        assert NodeConfig(missing_cache_policy="recompute").missing_cache_policy == "recompute"
        assert NodeConfig(missing_cache_policy="as_empty").missing_cache_policy == "as_empty"
        assert NodeConfig(missing_cache_policy="strict").missing_cache_policy == "strict"

    def test_merge_none_inherits_from_self(self):
        base = NodeConfig(missing_cache_policy="strict")
        merged = base.merge(NodeConfig())
        assert merged.missing_cache_policy == "strict"

    def test_merge_non_none_other_wins(self):
        base = NodeConfig(missing_cache_policy="recompute")
        merged = base.merge(NodeConfig(missing_cache_policy="as_empty"))
        assert merged.missing_cache_policy == "as_empty"

    def test_merge_preserves_other_fields(self):
        base = NodeConfig(is_result_ephemeral=True, missing_cache_policy="strict")
        merged = base.merge(NodeConfig(missing_cache_policy="as_empty"))
        assert merged.is_result_ephemeral is True
        assert merged.missing_cache_policy == "as_empty"

    def test_cache_miss_error_is_importable(self):
        # Will fail if CacheMissError doesn't exist in errors.py yet
        from orcapod.errors import CacheMissError  # noqa: F401


class TestStrictPolicy:
    """missing_cache_policy="strict" raises CacheMissError on non-ephemeral miss."""

    def test_strict_raises_when_result_db_completely_empty(self):
        """Branch A: result DB returns None (never written to)."""
        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()
        result_db = InMemoryArrowDatabase()

        # Session 1: compute and store
        _make_node(stream, pipeline_db, result_db).execute(stream)

        # Wipe result DB completely
        _wipe_result_db(result_db)

        # Session 2: strict mode should raise
        node = _make_node(stream, pipeline_db, result_db, missing_cache_policy="strict")
        with pytest.raises(CacheMissError):
            node.execute(stream)

    def test_strict_raises_when_result_db_has_partial_gap(self):
        """Branch B: result DB has rows but the required row is absent."""
        rows = [{"id": 0, "x": 10}, {"id": 1, "x": 20}]
        stream = _make_stream(rows)
        pipeline_db = InMemoryArrowDatabase()
        result_db = InMemoryArrowDatabase()

        # Session 1: compute both rows
        _make_node(stream, result_db=result_db, pipeline_db=pipeline_db).execute(stream)

        # Remove only the first row's result to create a partial gap
        for table_path in list(result_db._tables.keys()):
            tbl = result_db._tables[table_path]
            # Keep only the second row
            result_db._tables[table_path] = tbl.slice(1)
        result_db._pending_batches.clear()

        node = _make_node(stream, pipeline_db, result_db, missing_cache_policy="strict")
        with pytest.raises(CacheMissError):
            node.execute(stream)

    def test_strict_does_not_raise_when_all_results_present(self):
        """strict mode is a no-op when the result DB is fully populated."""
        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()
        result_db = InMemoryArrowDatabase()

        node1 = _make_node(stream, pipeline_db, result_db)
        node1.execute(stream)

        node2 = _make_node(stream, pipeline_db, result_db, missing_cache_policy="strict")
        results = node2.execute(stream)
        assert len(results) == 1
        assert results[0][1].as_dict()["result"] == 20

    def test_strict_ephemeral_miss_does_not_raise(self):
        """strict mode never raises for ephemeral misses."""
        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()
        result_db = InMemoryArrowDatabase()
        ephemeral_db = InMemoryArrowDatabase()

        node1 = FunctionJobNode(
            function_pod=FunctionPod(PythonDataFunction(_double, output_keys="result")),
            input_stream=stream,
            pipeline_database=pipeline_db,
            result_database=result_db,
            ephemeral_database=ephemeral_db,
        )
        node1.node_config = NodeConfig(is_result_ephemeral=True, missing_cache_policy="strict")
        node1.execute(stream)

        # Wipe ephemeral store only
        ephemeral_db._tables.clear()
        ephemeral_db._pending_batches.clear()

        node2 = FunctionJobNode(
            function_pod=FunctionPod(PythonDataFunction(_double, output_keys="result")),
            input_stream=stream,
            pipeline_database=pipeline_db,
            result_database=result_db,
            ephemeral_database=ephemeral_db,
        )
        node2.node_config = NodeConfig(is_result_ephemeral=True, missing_cache_policy="strict")
        # Should NOT raise — ephemeral misses degrade gracefully
        results = node2.execute(stream)
        assert len(results) == 1  # recomputed
