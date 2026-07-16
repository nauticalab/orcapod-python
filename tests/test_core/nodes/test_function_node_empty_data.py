# tests/test_core/nodes/test_function_node_empty_data.py
"""Tests for EmptyData integration in FunctionJobNode."""
from __future__ import annotations

import pyarrow as pa
import pytest
import asyncio

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

    def test_empty_data_token_hash_matches_output(self, ephemeral_node):
        """The EmptyData token's cached hash matches the upstream OUTPUT's content hash.

        The downstream result cache is keyed by the INPUT to the downstream
        (= the OUTPUT of the ephemeral upstream node). So the EmptyData token
        must carry the upstream output hash, not the upstream input hash.
        """
        node = ephemeral_node
        input_pairs = list(node._input_stream.iter_data())
        tag0, data0 = input_pairs[0]
        _, output0 = node.execute_data(tag0, data0)

        # Clear ephemeral store
        node.set_ephemeral_store(InMemoryArrowDatabase())

        result = node._fetch_joined_records()
        assert result is not None
        token = next(iter(result.empty_data_tokens.values()))
        assert output0 is not None
        assert token.cached_content_hash.to_string() == output0.content_hash().to_string()


class TestLoadCachedEntriesEmptyData:
    def test_empty_data_token_appears_in_loaded_entries(self, ephemeral_node):
        """_load_cached_entries yields (tag, EmptyData) for ephemeral miss rows."""
        node = ephemeral_node
        for tag, data in node._input_stream.iter_data():
            node.execute_data(tag, data)

        # Simulate expiry by replacing ephemeral store
        node.set_ephemeral_store(InMemoryArrowDatabase())

        loaded = node._load_cached_entries()
        assert len(loaded) == 1
        base_eid = next(iter(loaded))
        tag_out, data_out = loaded[base_eid]
        assert isinstance(data_out, EmptyData)
        assert data_out.cached_content_hash is not None

    def test_normal_result_wins_over_empty_data_token(self, persistent_node):
        """Non-ephemeral rows produce real Data, not EmptyData."""
        node = persistent_node
        for tag, data in node._input_stream.iter_data():
            node.execute_data(tag, data)

        loaded = node._load_cached_entries()
        for base_eid, (tag, data) in loaded.items():
            assert not isinstance(data, EmptyData)
            assert isinstance(data, Data)


from orcapod.errors import EphemeralResultMissingError


class TestProcessDataInternalEmptyData:
    def test_cache_hit_returns_cached_result(self, ephemeral_node):
        """Downstream cache hit on EmptyData input returns the cached result."""
        node = ephemeral_node
        # Step 1: Run upstream to populate ephemeral store and pipeline DB
        for tag, data in node._input_stream.iter_data():
            node.execute_data(tag, data)

        # Step 2: Build a downstream node over the ephemeral node
        table = pa.table({
            "key": pa.array(["a"], type=pa.large_string()),
            "value": pa.array([1], type=pa.int64()),
        })
        src = ArrowTableSource(table, tag_columns=["key"], infer_nullable=True)

        def triple_value(result: int) -> int:
            return result * 3

        pod = FunctionPod(PythonDataFunction(triple_value, output_keys="final"))
        downstream = FunctionJobNode(
            pod, node,
            pipeline_database=InMemoryArrowDatabase(),
            result_database=InMemoryArrowDatabase(),
        )

        # Step 3: Run downstream with real data to populate its result cache
        for tag, data in node.iter_data():
            downstream.execute_data(tag, data)

        # Step 4: Simulate upstream ephemeral expiry
        node.set_ephemeral_store(InMemoryArrowDatabase())
        node._cached_output_datas.clear()

        # Step 5: Upstream now emits EmptyData
        upstream_items = list(node.iter_data())
        assert len(upstream_items) == 1
        tag_in, data_in = upstream_items[0]
        assert isinstance(data_in, EmptyData)

        # Step 6: Downstream processes EmptyData — should hit its result cache
        tag_out, result = downstream.execute_data(tag_in, data_in)
        assert result is not None
        assert result.as_dict()["final"] == 6  # double(1)=2, triple(2)=6

    def test_cache_miss_raises_ephemeral_result_missing_error(self, ephemeral_node):
        """Downstream cache miss on EmptyData input raises EphemeralResultMissingError."""
        node = ephemeral_node
        # Run upstream but NOT downstream
        for tag, data in node._input_stream.iter_data():
            node.execute_data(tag, data)

        # Simulate expiry
        node.set_ephemeral_store(InMemoryArrowDatabase())
        node._cached_output_datas.clear()

        # Build a downstream node that has NEVER run before
        table = pa.table({
            "key": pa.array(["a"], type=pa.large_string()),
            "value": pa.array([1], type=pa.int64()),
        })
        src = ArrowTableSource(table, tag_columns=["key"], infer_nullable=True)

        def triple_value(result: int) -> int:
            return result * 3

        pod = FunctionPod(PythonDataFunction(triple_value, output_keys="final"))
        downstream = FunctionJobNode(
            pod, node,
            pipeline_database=InMemoryArrowDatabase(),
            result_database=InMemoryArrowDatabase(),
        )

        upstream_items = list(node.iter_data())
        tag_in, data_in = upstream_items[0]
        assert isinstance(data_in, EmptyData)

        with pytest.raises(EphemeralResultMissingError) as exc_info:
            downstream.execute_data(tag_in, data_in)

        assert exc_info.value.cached_content_hash is not None
        assert exc_info.value.node_identity_path == downstream.node_identity_path


# ---------------------------------------------------------------------------
# Async variants — cover _async_process_data_internal
# ---------------------------------------------------------------------------


class TestAsyncProcessDataInternalEmptyData:
    @pytest.mark.asyncio
    async def test_async_empty_data_cache_hit_returns_cached_result(self, ephemeral_node):
        """Async path: downstream cache hit on EmptyData input returns the cached result.

        Covers ``_async_process_data_internal`` lines where the EmptyData guard
        finds a match in ``_cached_function_pod.lookup_cached_data()``.
        """
        node = ephemeral_node
        # Run upstream to populate ephemeral store and pipeline DB
        for tag, data in node._input_stream.iter_data():
            node.execute_data(tag, data)

        # Build a downstream node with a persistent result cache
        def triple_value(result: int) -> int:
            return result * 3

        pod = FunctionPod(PythonDataFunction(triple_value, output_keys="final"))
        downstream = FunctionJobNode(
            pod, node,
            pipeline_database=InMemoryArrowDatabase(),
            result_database=InMemoryArrowDatabase(),
        )

        # Populate downstream result cache with real data (sync)
        for tag, data in node.iter_data():
            downstream.execute_data(tag, data)

        # Simulate ephemeral expiry
        node.set_ephemeral_store(InMemoryArrowDatabase())
        node._cached_output_datas.clear()

        # Upstream now emits EmptyData
        upstream_items = list(node.iter_data())
        assert len(upstream_items) == 1
        tag_in, data_in = upstream_items[0]
        assert isinstance(data_in, EmptyData)

        # Call async internal directly — must hit downstream result cache
        tag_out, result = await downstream._async_process_data_internal(tag_in, data_in)
        assert result is not None
        assert result.as_dict()["final"] == 6  # double(1)=2, triple(2)=6

    @pytest.mark.asyncio
    async def test_async_empty_data_cache_miss_raises(self, ephemeral_node):
        """Async path: downstream cache miss on EmptyData raises EphemeralResultMissingError.

        Covers the ``raise EphemeralResultMissingError(...)`` branch of
        ``_async_process_data_internal`` when no cached result exists.
        """
        node = ephemeral_node
        for tag, data in node._input_stream.iter_data():
            node.execute_data(tag, data)

        # Simulate expiry
        node.set_ephemeral_store(InMemoryArrowDatabase())
        node._cached_output_datas.clear()

        # Build downstream that has NEVER run — empty result cache
        def triple_value(result: int) -> int:
            return result * 3

        pod = FunctionPod(PythonDataFunction(triple_value, output_keys="final"))
        downstream = FunctionJobNode(
            pod, node,
            pipeline_database=InMemoryArrowDatabase(),
            result_database=InMemoryArrowDatabase(),
        )

        upstream_items = list(node.iter_data())
        tag_in, data_in = upstream_items[0]
        assert isinstance(data_in, EmptyData)

        with pytest.raises(EphemeralResultMissingError) as exc_info:
            await downstream._async_process_data_internal(tag_in, data_in)

        assert exc_info.value.cached_content_hash is not None
        assert exc_info.value.node_identity_path == downstream.node_identity_path


class TestAsyncProcessDataInternalEphemeralWrite:
    @pytest.mark.asyncio
    async def test_async_ephemeral_path_writes_pipeline_record(self, ephemeral_node):
        """Async ephemeral path writes an IS_EPHEMERAL=True record to the pipeline DB.

        Covers the ``await self._ephemeral_cached_pod.async_process_data(...)``
        and ``self.add_pipeline_record(..., is_ephemeral=True, ...)`` block inside
        ``_async_process_data_internal``.
        """
        node = ephemeral_node
        tag, data = next(iter(node._input_stream.iter_data()))

        tag_out, output = await node._async_process_data_internal(tag, data)

        assert output is not None
        all_records = node._pipeline_database.get_all_records(node.node_identity_path)
        assert all_records is not None
        vals = all_records.column(constants.IS_EPHEMERAL_COL).to_pylist()
        assert all(v is True for v in vals)

    @pytest.mark.asyncio
    async def test_async_ephemeral_path_stores_output_data_hash(self, ephemeral_node):
        """Async ephemeral path stores OUTPUT_DATA_HASH_COL in the pipeline DB."""
        node = ephemeral_node
        tag, data = next(iter(node._input_stream.iter_data()))

        _, output = await node._async_process_data_internal(tag, data)

        assert output is not None
        all_records = node._pipeline_database.get_all_records(node.node_identity_path)
        assert all_records is not None
        assert constants.OUTPUT_DATA_HASH_COL in all_records.column_names
        stored_hash = all_records.column(constants.OUTPUT_DATA_HASH_COL).to_pylist()[0]
        assert stored_hash == output.content_hash().to_string()
