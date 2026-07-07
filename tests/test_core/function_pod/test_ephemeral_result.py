"""Tests for FunctionJobNode ephemeral result store — ITL-507."""
from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.core.data_function import PythonDataFunction
from orcapod.core.datagrams import Data, Tag
from orcapod.core.function_pod import FunctionPod
from orcapod.core.nodes import FunctionNode
from orcapod.core.nodes.function_node import FunctionJobNode
from orcapod.core.sources import ArrowTableSource
from orcapod.core.streams.arrow_table_stream import ArrowTableStream
from orcapod.databases import InMemoryArrowDatabase
from orcapod.types import NodeConfig


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def double(x: int) -> int:
    return x * 2


def _make_pod(config: NodeConfig | None = None):
    pf = PythonDataFunction(double, output_keys="result")
    return FunctionPod(pf, config=config)


def _make_stream(rows: list[dict], tag_columns: list[str] | None = None) -> ArrowTableStream:
    if tag_columns is None:
        tag_columns = ["id"]
    keys = list(rows[0].keys())
    schema = pa.schema([pa.field(k, pa.int64(), nullable=False) for k in keys])
    table = pa.table(
        {k: pa.array([r[k] for r in rows], type=pa.int64()) for k in keys},
        schema=schema,
    )
    return ArrowTableStream(table, tag_columns=tag_columns)


def _make_source_stream(rows: list[dict], tag_columns: list[str] | None = None) -> ArrowTableStream:
    if tag_columns is None:
        tag_columns = ["id"]
    table = pa.table(
        {k: pa.array([r[k] for r in rows], type=pa.int64()) for k in rows[0]}
    )
    source = ArrowTableSource(table, tag_columns=tag_columns, source_id="test_src", infer_nullable=True)
    return source


def _make_node(stream, pipeline_db=None, result_db=None, ephemeral_result: bool = False):
    """Create a FunctionJobNode with given DB configuration."""
    cfg = NodeConfig(ephemeral_result=ephemeral_result)
    pod = _make_pod(config=cfg)
    if pipeline_db is None:
        pipeline_db = InMemoryArrowDatabase()
    return FunctionJobNode(
        function_pod=pod,
        input_stream=stream,
        pipeline_database=pipeline_db,
        result_database=result_db if result_db is not None else pipeline_db,
    ), pipeline_db


# ---------------------------------------------------------------------------
# Task 4 test: no-op set_ephemeral_store on blueprint node classes
# ---------------------------------------------------------------------------

class TestNoOpSetEphemeralStore:
    def test_function_node_has_set_ephemeral_store(self):
        """FunctionNode (blueprint) must have set_ephemeral_store as a no-op."""
        stream = _make_stream([{"id": 0, "x": 10}])
        pf = PythonDataFunction(double, output_keys="result")
        pod = FunctionPod(pf)
        node = FunctionNode(function_pod=pod, input_stream=stream)
        store = InMemoryArrowDatabase()
        # Must not raise
        node.set_ephemeral_store(store)
        node.set_ephemeral_store(None)

    def test_operator_node_has_set_ephemeral_store(self):
        """OperatorNode (blueprint) must have set_ephemeral_store as a no-op."""
        from orcapod.core.nodes import OperatorNode
        from orcapod.core.operators import Join

        stream_a = _make_stream([{"id": 0, "x": 10}])
        stream_b = _make_stream([{"id": 0, "y": 20}])
        op = Join()
        node = OperatorNode(operator=op, input_streams=(stream_a, stream_b))
        store = InMemoryArrowDatabase()
        # Must not raise
        node.set_ephemeral_store(store)
        node.set_ephemeral_store(None)


# ---------------------------------------------------------------------------
# Task 5 test: FunctionJobNode set_ephemeral_store real override
# ---------------------------------------------------------------------------

class TestSetEphemeralStore:
    def test_set_ephemeral_store_assigns_store(self):
        """set_ephemeral_store(store) assigns the ephemeral_result_store attribute."""
        stream = _make_stream([{"id": 0, "x": 10}])
        node, _ = _make_node(stream)
        store = InMemoryArrowDatabase()
        node.set_ephemeral_store(store)
        assert node.ephemeral_result_store is store

    def test_set_ephemeral_store_none_detaches(self):
        """set_ephemeral_store(None) removes the ephemeral store."""
        stream = _make_stream([{"id": 0, "x": 10}])
        node, _ = _make_node(stream)
        store = InMemoryArrowDatabase()
        node.set_ephemeral_store(store)
        node.set_ephemeral_store(None)
        assert node.ephemeral_result_store is None
