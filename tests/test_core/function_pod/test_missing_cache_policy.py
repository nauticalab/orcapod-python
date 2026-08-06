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

    def test_async_strict_raises_on_nonephemeral_miss(self):
        """In 'strict' mode, async_execute raises CacheMissError on non-ephemeral miss."""
        import asyncio

        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()
        result_db = InMemoryArrowDatabase()

        # Session 1: compute normally
        _make_node(stream, pipeline_db, result_db).execute(stream)

        # Wipe result DB
        _wipe_result_db(result_db)

        # Session 2: strict mode in async_execute should raise CacheMissError
        node = _make_node(stream, pipeline_db, result_db, missing_cache_policy="strict")

        async def run():
            in_ch = Channel(buffer_size=16)
            out_ch = Channel(buffer_size=16)
            async def feed():
                for tag, data in stream.iter_data():
                    await in_ch.writer.send((tag, data))
                await in_ch.writer.close()
            await asyncio.gather(
                feed(),
                node.async_execute(in_ch.reader, out_ch.writer),
            )

        with pytest.raises(CacheMissError):
            asyncio.run(run())


# ---------------------------------------------------------------------------
# Task 6: CACHE_ONLY mode respects missing_cache_policy
# ---------------------------------------------------------------------------


class TestCacheOnlyPolicy:
    """CACHE_ONLY mode respects missing_cache_policy."""

    def _make_cache_only_node(
        self,
        stream: ArrowTableStream,
        pipeline_db: InMemoryArrowDatabase,
        result_db: InMemoryArrowDatabase,
        missing_cache_policy: str | None = None,
    ) -> FunctionJobNode:
        """Build a FunctionJobNode in CACHE_ONLY mode."""
        from orcapod.pipeline.serialization import LoadStatus

        pf = PythonDataFunction(_double, output_keys="result")
        pod = FunctionPod(pf)
        node = FunctionJobNode(
            function_pod=pod,
            input_stream=stream,
            pipeline_database=pipeline_db,
            result_database=result_db,
        )
        # Force CACHE_ONLY status — same as what from_descriptor() does
        # when the upstream stream has load_status == UNAVAILABLE.
        node._load_status = LoadStatus.CACHE_ONLY
        if missing_cache_policy is not None:
            node.node_config = NodeConfig(missing_cache_policy=missing_cache_policy)
        return node

    def test_strict_raises_in_cache_only_mode(self):
        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()
        result_db = InMemoryArrowDatabase()

        # Session 1: compute normally
        _make_node(stream, pipeline_db, result_db).execute(stream)
        _wipe_result_db(result_db)

        # CACHE_ONLY + strict → must raise on iter_data()
        node = self._make_cache_only_node(
            stream, pipeline_db, result_db, missing_cache_policy="strict"
        )
        with pytest.raises(CacheMissError):
            list(node.iter_data())

    def test_as_empty_forwards_empty_data_in_cache_only_mode(self):
        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()
        result_db = InMemoryArrowDatabase()

        _make_node(stream, pipeline_db, result_db).execute(stream)
        _wipe_result_db(result_db)

        node = self._make_cache_only_node(
            stream, pipeline_db, result_db, missing_cache_policy="as_empty"
        )
        results = list(node.iter_data())
        assert len(results) == 1
        assert isinstance(results[0][1], EmptyData)

    def test_recompute_omits_missing_entry_in_cache_only_mode(self):
        """Default 'recompute' mode: CACHE_ONLY can't recompute, so entry is just absent."""
        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()
        result_db = InMemoryArrowDatabase()

        _make_node(stream, pipeline_db, result_db).execute(stream)
        _wipe_result_db(result_db)

        node = self._make_cache_only_node(stream, pipeline_db, result_db)
        results = list(node.iter_data())
        assert results == [], "missing entry should be silently omitted in recompute+CACHE_ONLY"


# ---------------------------------------------------------------------------
# Task 7: End-to-end — "as_empty" downstream cache hit
# ---------------------------------------------------------------------------


class TestAsEmptyEndToEnd:
    """End-to-end: upstream as_empty miss -> downstream serves from its own cache."""

    def test_downstream_raises_ephemeral_result_missing_when_it_has_no_cache(self):
        """When EmptyData propagates to a node that never computed the result,
        EphemeralResultMissingError is raised (ITL-605 boundary).
        """
        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()
        result_db = InMemoryArrowDatabase()

        node = _make_node(stream, pipeline_db, result_db)
        node.execute(stream)
        _wipe_result_db(result_db)

        node2 = _make_node(stream, pipeline_db, result_db, missing_cache_policy="as_empty")
        results = node2.execute(stream)
        assert len(results) == 1
        assert isinstance(results[0][1], EmptyData)

        empty_tag, empty_data = results[0]

        # A downstream node that has never computed the result raises EphemeralResultMissingError.
        # Build a stream whose schema matches the downstream function's input (result: int).
        import pyarrow as pa

        downstream_stream = ArrowTableStream(
            pa.table(
                {"id": pa.array([0], type=pa.int64()), "result": pa.array([20], type=pa.int64())},
                schema=pa.schema([
                    pa.field("id", pa.int64(), nullable=False),
                    pa.field("result", pa.int64(), nullable=False),
                ]),
            ),
            tag_columns=["id"],
        )
        downstream_pipeline_db = InMemoryArrowDatabase()
        downstream_result_db = InMemoryArrowDatabase()

        def _increment(result: int) -> int:
            return result + 1

        pf_down = PythonDataFunction(_increment, output_keys="incremented")
        node_down = FunctionJobNode(
            function_pod=FunctionPod(pf_down),
            input_stream=downstream_stream,
            pipeline_database=downstream_pipeline_db,
            result_database=downstream_result_db,
        )

        with pytest.raises(EphemeralResultMissingError):
            node_down._process_data_internal(empty_tag, empty_data)

    def test_downstream_serves_from_cache_when_upstream_emits_empty_data(self):
        """Two-node pipeline: Node A (as_empty miss) -> Node B (persistent).

        Session 1: A computes x*2, B computes result*3.
        Session 2: A's result wiped, A emits EmptyData. B serves 60 from its own
        cache using EmptyData.cached_content_hash.
        """
        import pyarrow as pa

        def double(x: int) -> int:
            return x * 2

        def triple(result: int) -> int:
            return result * 3

        stream = _make_stream([{"id": 0, "x": 10}])

        pipeline_db_a = InMemoryArrowDatabase()
        result_db_a = InMemoryArrowDatabase()
        pipeline_db_b = InMemoryArrowDatabase()
        result_db_b = InMemoryArrowDatabase()

        # Session 1: compute A, then build B's stream from A's output, compute B
        pf_a = PythonDataFunction(double, output_keys="result")
        node_a1 = FunctionJobNode(
            function_pod=FunctionPod(pf_a),
            input_stream=stream,
            pipeline_database=pipeline_db_a,
            result_database=result_db_a,
        )
        results_a = node_a1.execute(stream)
        assert len(results_a) == 1
        assert results_a[0][1].as_dict()["result"] == 20

        # Build B's stream from A's output (result=20 row)
        b_table = pa.table(
            {"id": pa.array([0], type=pa.int64()), "result": pa.array([20], type=pa.int64())},
            schema=pa.schema([
                pa.field("id", pa.int64(), nullable=False),
                pa.field("result", pa.int64(), nullable=False),
            ]),
        )
        stream_b = ArrowTableStream(b_table, tag_columns=["id"])

        pf_b = PythonDataFunction(triple, output_keys="tripled")
        node_b1 = FunctionJobNode(
            function_pod=FunctionPod(pf_b),
            input_stream=stream_b,
            pipeline_database=pipeline_db_b,
            result_database=result_db_b,
        )
        results_b = node_b1.execute(stream_b)
        assert len(results_b) == 1
        assert results_b[0][1].as_dict()["tripled"] == 60

        # Session 2: wipe A's result; A emits EmptyData
        _wipe_result_db(result_db_a)

        pf_a2 = PythonDataFunction(double, output_keys="result")
        node_a2 = FunctionJobNode(
            function_pod=FunctionPod(pf_a2),
            input_stream=stream,
            pipeline_database=pipeline_db_a,
            result_database=result_db_a,
        )
        from orcapod.types import NodeConfig
        node_a2.node_config = NodeConfig(missing_cache_policy="as_empty")
        results_a2 = node_a2.execute(stream)
        assert len(results_a2) == 1
        empty_tag, empty_data = results_a2[0]
        assert isinstance(empty_data, EmptyData)

        # Verify EmptyData carries the correct hash (A's output hash = B's input hash)
        assert empty_data.cached_content_hash is not None, (
            "EmptyData must carry the upstream output hash so downstream can look up its cache"
        )

        # Feed EmptyData to B: B serves from its own cache via _process_data_internal.
        # EmptyData.content_hash() returns cached_content_hash (= A's output hash = B's input hash),
        # so CachedFunctionPod.lookup_cached_data(empty_data) finds B's cached result.
        pf_b2 = PythonDataFunction(triple, output_keys="tripled")
        node_b2 = FunctionJobNode(
            function_pod=FunctionPod(pf_b2),
            input_stream=stream_b,
            pipeline_database=pipeline_db_b,
            result_database=result_db_b,
        )
        b2_tag_out, b2_data_out = node_b2._process_data_internal(empty_tag, empty_data)
        assert b2_data_out is not None, "B should return a cached result"
        assert b2_data_out.as_dict()["tripled"] == 60, (
            "B should serve 60 from its cache given EmptyData with the correct hash"
        )


class TestRecomputeRegression:
    """Default 'recompute' mode preserves all existing behaviour exactly."""

    def test_default_nonephemeral_miss_still_recomputes(self):
        call_count = {"n": 0}

        def counting_double(x: int) -> int:
            call_count["n"] += 1
            return x * 2

        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()
        result_db = InMemoryArrowDatabase()

        pf = PythonDataFunction(counting_double, output_keys="result")
        node1 = FunctionJobNode(
            function_pod=FunctionPod(pf), input_stream=stream,
            pipeline_database=pipeline_db, result_database=result_db,
        )
        node1.execute(stream)
        assert call_count["n"] == 1

        _wipe_result_db(result_db)

        pf2 = PythonDataFunction(counting_double, output_keys="result")
        node2 = FunctionJobNode(
            function_pod=FunctionPod(pf2), input_stream=stream,
            pipeline_database=pipeline_db, result_database=result_db,
        )
        # No missing_cache_policy set — defaults to "recompute"
        results = node2.execute(stream)
        assert call_count["n"] == 2, "default mode must recompute on miss"
        assert len(results) == 1
        assert not isinstance(results[0][1], EmptyData)
        assert results[0][1].as_dict()["result"] == 20

    def test_explicit_recompute_policy_same_as_default(self):
        call_count = {"n": 0}

        def counting_double(x: int) -> int:
            call_count["n"] += 1
            return x * 2

        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()
        result_db = InMemoryArrowDatabase()

        pf = PythonDataFunction(counting_double, output_keys="result")
        node1 = FunctionJobNode(
            function_pod=FunctionPod(pf), input_stream=stream,
            pipeline_database=pipeline_db, result_database=result_db,
        )
        node1.execute(stream)
        _wipe_result_db(result_db)

        pf2 = PythonDataFunction(counting_double, output_keys="result")
        node2 = FunctionJobNode(
            function_pod=FunctionPod(pf2), input_stream=stream,
            pipeline_database=pipeline_db, result_database=result_db,
        )
        node2.node_config = NodeConfig(missing_cache_policy="recompute")
        results = node2.execute(stream)
        assert call_count["n"] == 2
        assert not isinstance(results[0][1], EmptyData)
