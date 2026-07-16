# tests/test_core/nodes/test_function_node_empty_data.py
"""Tests for EmptyData integration in FunctionJobNode."""
from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.core.data_function import PythonDataFunction
from orcapod.core.datagrams import Data
from orcapod.core.datagrams.tag_data import EmptyData
from orcapod.core.function_pod import FunctionPod
from orcapod.core.nodes.function_node import FunctionJobNode
from orcapod.core.sources import ArrowTableSource
from orcapod.databases import InMemoryArrowDatabase
from orcapod.system_constants import constants
from orcapod.types import NodeConfig


def double_value(value: int) -> int:
    return value * 2


@pytest.fixture
def persistent_node():
    """FunctionJobNode with persistent databases, two input rows."""
    table = pa.table({
        "key": pa.array(["a", "b"], type=pa.large_string()),
        "value": pa.array([1, 2], type=pa.int64()),
    })
    src = ArrowTableSource(table, tag_columns=["key"], infer_nullable=True)
    pod = FunctionPod(PythonDataFunction(double_value, output_keys="result"))
    return FunctionJobNode(
        pod, src,
        pipeline_database=InMemoryArrowDatabase(),
        result_database=InMemoryArrowDatabase(),
    )


class TestAddPipelineRecordStoresInputHash:
    def test_pipeline_record_contains_input_data_hash(self, persistent_node):
        """add_pipeline_record now stores INPUT_DATA_HASH_COL in the pipeline DB."""
        node = persistent_node
        for tag, data in node._input_stream.iter_data():
            node.execute_data(tag, data)

        all_records = node._pipeline_database.get_all_records(node.node_identity_path)
        assert all_records is not None
        assert constants.INPUT_DATA_HASH_COL in all_records.column_names

    def test_stored_hash_matches_input_content_hash(self, persistent_node):
        """The stored INPUT_DATA_HASH_COL value matches input_data.content_hash()."""
        node = persistent_node
        input_pairs = list(node._input_stream.iter_data())
        tag0, data0 = input_pairs[0]
        node.execute_data(tag0, data0)

        all_records = node._pipeline_database.get_all_records(node.node_identity_path)
        assert all_records is not None
        stored_hashes = all_records.column(constants.INPUT_DATA_HASH_COL).to_pylist()
        assert data0.content_hash().to_string() in stored_hashes


@pytest.fixture
def ephemeral_node():
    """FunctionJobNode with is_result_ephemeral=True and an ephemeral store."""
    table = pa.table({
        "key": pa.array(["a"], type=pa.large_string()),
        "value": pa.array([1], type=pa.int64()),
    })
    src = ArrowTableSource(table, tag_columns=["key"], infer_nullable=True)
    pod = FunctionPod(PythonDataFunction(double_value, output_keys="result"))
    node = FunctionJobNode(
        pod, src,
        pipeline_database=InMemoryArrowDatabase(),
        result_database=None,
    )
    node.node_config = NodeConfig(is_result_ephemeral=True)
    node.set_ephemeral_store(InMemoryArrowDatabase())
    return node


class TestFetchJoinedRecordsEmptyData:
    def test_returns_empty_data_tokens_field(self, ephemeral_node):
        """_JoinedRecords now has an empty_data_tokens field (even when empty)."""
        node = ephemeral_node
        # Execute so pipeline DB has a record
        for tag, data in node._input_stream.iter_data():
            node.execute_data(tag, data)
        result = node._fetch_joined_records()
        assert result is not None
        assert hasattr(result, "empty_data_tokens")
        assert hasattr(result, "empty_taginfo_rows")

    def test_ephemeral_miss_produces_empty_data_token(self, ephemeral_node):
        """When the ephemeral store is cleared, _fetch_joined_records emits EmptyData."""
        node = ephemeral_node
        for tag, data in node._input_stream.iter_data():
            node.execute_data(tag, data)

        # Simulate ephemeral result expiry by replacing the ephemeral store with empty one
        node.set_ephemeral_store(InMemoryArrowDatabase())

        result = node._fetch_joined_records()
        assert result is not None
        assert len(result.empty_data_tokens) == 1

        token = next(iter(result.empty_data_tokens.values()))
        assert isinstance(token, EmptyData)
        assert token.cached_content_hash is not None

    def test_empty_data_token_hash_matches_input(self, ephemeral_node):
        """The EmptyData token's cached hash matches the original input's content hash."""
        node = ephemeral_node
        input_pairs = list(node._input_stream.iter_data())
        tag0, data0 = input_pairs[0]
        node.execute_data(tag0, data0)

        # Clear ephemeral store
        node.set_ephemeral_store(InMemoryArrowDatabase())

        result = node._fetch_joined_records()
        assert result is not None
        token = next(iter(result.empty_data_tokens.values()))
        assert token.cached_content_hash.to_string() == data0.content_hash().to_string()
