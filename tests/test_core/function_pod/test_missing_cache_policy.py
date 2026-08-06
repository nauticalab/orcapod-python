"""Tests for NodeConfig.missing_cache_policy — ITL-604."""
from __future__ import annotations

import asyncio
import logging

import pyarrow as pa
import pytest

from orcapod.channels import Channel
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


class TestAsEmptyPolicy:
    """missing_cache_policy="as_empty" emits EmptyData instead of recomputing."""

    def test_as_empty_nonephemeral_miss_emits_empty_data_not_recompute(self):
        call_count = {"n": 0}

        def counting_double(x: int) -> int:
            call_count["n"] += 1
            return x * 2

        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()
        result_db = InMemoryArrowDatabase()

        # Session 1: compute once (call_count = 1)
        pf = PythonDataFunction(counting_double, output_keys="result")
        node1 = FunctionJobNode(
            function_pod=FunctionPod(pf),
            input_stream=stream,
            pipeline_database=pipeline_db,
            result_database=result_db,
        )
        node1.execute(stream)
        assert call_count["n"] == 1

        _wipe_result_db(result_db)

        # Session 2: as_empty — function must NOT be called again
        pf2 = PythonDataFunction(counting_double, output_keys="result")
        node2 = FunctionJobNode(
            function_pod=FunctionPod(pf2),
            input_stream=stream,
            pipeline_database=pipeline_db,
            result_database=result_db,
        )
        node2.node_config = NodeConfig(missing_cache_policy="as_empty")
        results = node2.execute(stream)

        assert call_count["n"] == 1, "function must not be called again in as_empty mode"
        assert len(results) == 1
        assert isinstance(results[0][1], EmptyData)

    def test_as_empty_nonephemeral_miss_logs_warning(self, caplog):
        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()
        result_db = InMemoryArrowDatabase()

        _make_node(stream, pipeline_db, result_db).execute(stream)
        _wipe_result_db(result_db)

        node = _make_node(stream, pipeline_db, result_db, missing_cache_policy="as_empty")
        with caplog.at_level(logging.WARNING, logger="orcapod.core.nodes.function_node"):
            node.execute(stream)

        assert any("treating as Empty data" in msg for msg in caplog.messages)

    def test_as_empty_honoured_across_multiple_execute_calls(self):
        call_count = {"n": 0}

        def counting_double(x: int) -> int:
            call_count["n"] += 1
            return x * 2

        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()
        result_db = InMemoryArrowDatabase()

        pf = PythonDataFunction(counting_double, output_keys="result")
        node1 = FunctionJobNode(
            function_pod=FunctionPod(pf),
            input_stream=stream,
            pipeline_database=pipeline_db,
            result_database=result_db,
        )
        node1.execute(stream)
        assert call_count["n"] == 1

        _wipe_result_db(result_db)

        pf2 = PythonDataFunction(counting_double, output_keys="result")
        node2 = FunctionJobNode(
            function_pod=FunctionPod(pf2),
            input_stream=stream,
            pipeline_database=pipeline_db,
            result_database=result_db,
        )
        node2.node_config = NodeConfig(missing_cache_policy="as_empty")
        node2.execute(stream)
        node2._cached_output_datas.clear()
        node2.execute(stream)  # second call — must still emit EmptyData, not recompute

        assert call_count["n"] == 1, "policy must be honoured on every call, not just the first"


class TestEphemeralInfoLog:
    """Ephemeral misses log at INFO level (never WARNING)."""

    def test_ephemeral_miss_logs_info_not_warning(self, caplog):
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
        node1.node_config = NodeConfig(is_result_ephemeral=True)
        node1.execute(stream)

        # Wipe ephemeral store
        ephemeral_db._tables.clear()
        ephemeral_db._pending_batches.clear()
        ephemeral_db._pending_record_ids.clear()

        node2 = FunctionJobNode(
            function_pod=FunctionPod(PythonDataFunction(_double, output_keys="result")),
            input_stream=stream,
            pipeline_database=pipeline_db,
            result_database=result_db,
            ephemeral_database=ephemeral_db,
        )
        node2.node_config = NodeConfig(is_result_ephemeral=True)

        with caplog.at_level(logging.DEBUG, logger="orcapod.core.nodes.function_node"):
            node2.execute(stream)

        # Assert INFO log was emitted for ephemeral miss
        info_msgs = [
            r.message for r in caplog.records
            if r.levelno == logging.INFO and "ephemeral result DB" in r.message
        ]
        assert info_msgs, "expected INFO log for ephemeral miss"

        # Assert NO WARNING log about ephemeral miss
        warning_msgs = [
            r.message for r in caplog.records
            if r.levelno == logging.WARNING and "ephemeral result DB" in r.message
        ]
        assert not warning_msgs, f"unexpected WARNING for ephemeral miss: {warning_msgs}"


# ---------------------------------------------------------------------------
# Task 5: async_execute() route_inputs respects missing_cache_policy
# ---------------------------------------------------------------------------


async def _feed_stream(stream: ArrowTableStream, ch: Channel) -> None:
    """Feed all (tag, data) pairs from stream into channel, then close."""
    for tag, data in stream.iter_data():
        await ch.writer.send((tag, data))
    await ch.writer.close()


class TestAsyncExecutePolicy:
    """async_execute() route_inputs respects missing_cache_policy."""

    def test_async_recompute_mode_does_not_forward_empty_data(self):
        """In 'recompute' mode, an ephemeral miss in async_execute triggers recompute."""
        call_count = {"n": 0}

        def counting_double(x: int) -> int:
            call_count["n"] += 1
            return x * 2

        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()
        result_db = InMemoryArrowDatabase()
        ephemeral_db = InMemoryArrowDatabase()

        pf = PythonDataFunction(counting_double, output_keys="result")
        node1 = FunctionJobNode(
            function_pod=FunctionPod(pf),
            input_stream=stream,
            pipeline_database=pipeline_db,
            result_database=result_db,
            ephemeral_database=ephemeral_db,
        )
        node1.node_config = NodeConfig(is_result_ephemeral=True)
        node1.execute(stream)
        assert call_count["n"] == 1

        # Wipe ephemeral store
        ephemeral_db._tables.clear()
        ephemeral_db._pending_batches.clear()
        ephemeral_db._pending_record_ids.clear()

        pf2 = PythonDataFunction(counting_double, output_keys="result")
        node2 = FunctionJobNode(
            function_pod=FunctionPod(pf2),
            input_stream=stream,
            pipeline_database=pipeline_db,
            result_database=result_db,
            ephemeral_database=ephemeral_db,
        )
        node2.node_config = NodeConfig(is_result_ephemeral=True)  # recompute (default)

        async def run():
            input_ch: Channel = Channel(buffer_size=16)
            output_ch: Channel = Channel(buffer_size=16)
            await _feed_stream(stream, input_ch)
            await node2.async_execute(input_ch.reader, output_ch.writer)
            return await output_ch.reader.collect()

        results = asyncio.run(run())
        # In recompute mode, the function is called again
        assert call_count["n"] == 2
        assert len(results) == 1
        assert not isinstance(results[0][1], EmptyData)

    def test_async_as_empty_mode_forwards_empty_data_without_recompute(self):
        """In 'as_empty' mode, an ephemeral miss forwards EmptyData without recompute."""
        call_count = {"n": 0}

        def counting_double(x: int) -> int:
            call_count["n"] += 1
            return x * 2

        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()
        result_db = InMemoryArrowDatabase()
        ephemeral_db = InMemoryArrowDatabase()

        pf = PythonDataFunction(counting_double, output_keys="result")
        node1 = FunctionJobNode(
            function_pod=FunctionPod(pf),
            input_stream=stream,
            pipeline_database=pipeline_db,
            result_database=result_db,
            ephemeral_database=ephemeral_db,
        )
        node1.node_config = NodeConfig(is_result_ephemeral=True)
        node1.execute(stream)
        assert call_count["n"] == 1

        ephemeral_db._tables.clear()
        ephemeral_db._pending_batches.clear()
        ephemeral_db._pending_record_ids.clear()

        pf2 = PythonDataFunction(counting_double, output_keys="result")
        node2 = FunctionJobNode(
            function_pod=FunctionPod(pf2),
            input_stream=stream,
            pipeline_database=pipeline_db,
            result_database=result_db,
            ephemeral_database=ephemeral_db,
        )
        node2.node_config = NodeConfig(is_result_ephemeral=True, missing_cache_policy="as_empty")

        async def run():
            input_ch: Channel = Channel(buffer_size=16)
            output_ch: Channel = Channel(buffer_size=16)
            await _feed_stream(stream, input_ch)
            await node2.async_execute(input_ch.reader, output_ch.writer)
            return await output_ch.reader.collect()

        results = asyncio.run(run())
        assert call_count["n"] == 1, "function must not be called again in as_empty mode"
        assert len(results) == 1
        assert isinstance(results[0][1], EmptyData)
