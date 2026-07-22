"""Tests for FunctionJobNode.async_execute() — unified async execution path."""

from __future__ import annotations

import asyncio

import pyarrow as pa
import pytest

from orcapod.channels import Channel
from orcapod.core.data_function import PythonDataFunction
from orcapod.core.function_pod import FunctionPod
from orcapod.core.nodes.function_node import FunctionJobNode
from orcapod.core.sources import ArrowTableSource
from orcapod.databases import InMemoryArrowDatabase
from orcapod.types import PodConfig


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _make_source(n: int = 3) -> ArrowTableSource:
    """ArrowTableSource with tag=key (large_string), data=value (int64)."""
    table = pa.table(
        {
            "key": pa.array([f"k{i}" for i in range(n)], type=pa.large_string()),
            "value": pa.array(list(range(n)), type=pa.int64()),
        }
    )
    return ArrowTableSource(table, tag_columns=["key"], infer_nullable=True)


def double_no_db(value: int) -> int:
    return value * 2


def _make_no_db_node(
    n: int = 3, max_concurrency: int | None = None
) -> FunctionJobNode:
    """FunctionJobNode with no DB attached."""
    src = _make_source(n)
    pod = FunctionPod(
        PythonDataFunction(double_no_db, output_keys="result"),
        pod_config=PodConfig(max_concurrency=max_concurrency),
    )
    return FunctionJobNode(pod, src)


async def _run_node(node: FunctionJobNode) -> list[tuple]:
    """Feed the node's own input stream through async_execute and collect results."""
    input_ch: Channel = Channel(buffer_size=32)
    output_ch: Channel = Channel(buffer_size=32)

    async def feed() -> None:
        for tag, data in node._input_stream.iter_data():
            await input_ch.writer.send((tag, data))
        await input_ch.writer.close()

    await asyncio.gather(
        feed(),
        node.async_execute(input_ch.reader, output_ch.writer),
    )
    return await output_ch.reader.collect()


# ---------------------------------------------------------------------------
# No-DB path
# ---------------------------------------------------------------------------


class TestFunctionJobNodeAsyncExecuteNoDB:
    @pytest.mark.asyncio
    async def test_all_items_processed_and_forwarded(self):
        """All input items reach the output channel."""
        node = _make_no_db_node(n=3)
        results = await _run_node(node)
        assert len(results) == 3
        values = sorted(data.as_dict()["result"] for _, data in results)
        assert values == [0, 2, 4]

    @pytest.mark.asyncio
    async def test_output_channel_closed_after_execution(self):
        """Output channel is closed even if execution completes without error."""
        node = _make_no_db_node(n=2)
        input_ch: Channel = Channel(buffer_size=8)
        output_ch: Channel = Channel(buffer_size=8)

        async def feed() -> None:
            for tag, data in node._input_stream.iter_data():
                await input_ch.writer.send((tag, data))
            await input_ch.writer.close()

        await asyncio.gather(
            feed(), node.async_execute(input_ch.reader, output_ch.writer)
        )
        # collect() should return immediately (channel closed), not hang.
        results = await output_ch.reader.collect()
        assert isinstance(results, list)

    @pytest.mark.asyncio
    async def test_max_concurrency_limits_tasks(self):
        """With max_concurrency=1, at most one task runs at a time."""
        concurrent_count = 0
        max_observed = 0

        def double_base(value: int) -> int:
            return value * 2

        pf = PythonDataFunction(double_base, output_keys="result")
        original_async_call = pf.async_call

        async def patched_async_call(data, **kwargs):
            nonlocal concurrent_count, max_observed
            concurrent_count += 1
            max_observed = max(max_observed, concurrent_count)
            try:
                await asyncio.sleep(0.01)
                return await original_async_call(data, **kwargs)
            finally:
                concurrent_count -= 1

        pf.async_call = patched_async_call  # type: ignore[method-assign]

        src = _make_source(5)
        pod = FunctionPod(pf, pod_config=PodConfig(max_concurrency=1))
        node = FunctionJobNode(pod, src)
        results = await _run_node(node)
        assert len(results) == 5
        assert max_observed <= 1


# ---------------------------------------------------------------------------
# Helpers — observer spy
# ---------------------------------------------------------------------------


class _SpyObserver:
    """Captures all observer hook invocations."""

    def __init__(self):
        self.node_starts: list[str] = []
        self.node_ends: list[str] = []
        self.data_starts: list[tuple] = []
        self.data_ends: list[tuple] = []    # (label, tag, input_data, output_data, cached)
        self.data_crashes: list[tuple] = []

    def contextualize(self, *path):
        return self

    def on_node_start(self, node_label, node_hash, *, tag_schema=None):
        self.node_starts.append(node_label)

    def on_node_end(self, node_label, node_hash):
        self.node_ends.append(node_label)

    def on_data_start(self, node_label, tag, data):
        self.data_starts.append((node_label, tag, data))

    def on_data_end(self, node_label, tag, input_data, output_data, *, cached):
        self.data_ends.append((node_label, tag, input_data, output_data, cached))

    def on_data_crash(self, node_label, tag, data, error):
        self.data_crashes.append((node_label, tag, data, error))

    def create_data_logger(self, tag, data):
        return None


# ---------------------------------------------------------------------------
# Helpers — DB node fixtures
# ---------------------------------------------------------------------------


def _make_db_node(
    n: int = 3,
) -> tuple[FunctionJobNode, InMemoryArrowDatabase, InMemoryArrowDatabase]:
    """FunctionJobNode with persistent pipeline + result databases."""

    def double(value: int) -> int:
        return value * 2

    src = _make_source(n)
    pod = FunctionPod(PythonDataFunction(double, output_keys="result"))
    pipeline_db = InMemoryArrowDatabase()
    result_db = InMemoryArrowDatabase()
    node = FunctionJobNode(
        pod, src, pipeline_database=pipeline_db, result_database=result_db
    )
    return node, pipeline_db, result_db


# ---------------------------------------------------------------------------
# DB path — basic flow
# ---------------------------------------------------------------------------


class TestFunctionJobNodeAsyncExecuteDB:
    @pytest.mark.asyncio
    async def test_all_items_processed_and_forwarded(self):
        """All input items are computed and forwarded via the DB path."""
        node, _, _ = _make_db_node(3)
        results = await _run_node(node)
        assert len(results) == 3
        values = sorted(data.as_dict()["result"] for _, data in results)
        assert values == [0, 2, 4]

    @pytest.mark.asyncio
    async def test_pipeline_records_written_for_each_item(self):
        """add_pipeline_record is called once per cache miss → one row per item."""
        node, pipeline_db, _ = _make_db_node(3)
        await _run_node(node)
        records = pipeline_db.get_all_records(node._versioned_pipeline_path)
        assert records is not None
        assert records.num_rows == 3

    @pytest.mark.asyncio
    async def test_correlation_key_absent_from_output_tags(self):
        """Output tags must not contain the internal _tag_node_input_ref column."""
        from orcapod.core.nodes.function_node import _TAG_NODE_INPUT_REF
        from orcapod.system_constants import constants as _constants

        node, _, _ = _make_db_node(3)
        results = await _run_node(node)
        full_key = f"{_constants.META_PREFIX}{_TAG_NODE_INPUT_REF}"
        for tag, _ in results:
            meta = tag.get_meta_info()
            assert full_key not in meta, (
                f"Output tag leaked internal correlation key {full_key!r}"
            )

    @pytest.mark.asyncio
    async def test_cache_hits_emitted_without_recomputation(self):
        """Items already in the pipeline DB are served from cache (no recomputation)."""
        node, _, _ = _make_db_node(3)

        # First run — populates both pipeline DB and result DB.
        first_results = await _run_node(node)
        assert len(first_results) == 3

        # Reset in-memory state so async_execute re-runs from scratch.
        node.clear_cache()

        call_count = 0
        # async_execute() calls data_function.async_call() (not .call()), so patch
        # async_call to reliably detect any recomputation via the async path.
        original_async_call = node._function_pod.data_function.async_call

        async def counted_async_call(data, **kw):
            nonlocal call_count
            call_count += 1
            return await original_async_call(data, **kw)

        node._function_pod.data_function.async_call = counted_async_call  # type: ignore[method-assign]

        second_results = await _run_node(node)
        assert len(second_results) == 3
        # All 3 were hits — data function should NOT have been called again.
        assert call_count == 0

    @pytest.mark.asyncio
    async def test_observer_cached_false_for_misses_cached_true_for_hits(self):
        """Cache hits emit on_data_end(cached=True); misses emit on_data_end(cached=False)."""
        node, _, _ = _make_db_node(2)

        # First run — both are misses. Just to populate the DB.
        await _run_node(node)
        node.clear_cache()

        spy = _SpyObserver()
        input_ch: Channel = Channel(buffer_size=8)
        output_ch: Channel = Channel(buffer_size=8)

        async def feed() -> None:
            for tag, data in node._input_stream.iter_data():
                await input_ch.writer.send((tag, data))
            await input_ch.writer.close()

        await asyncio.gather(
            feed(),
            node.async_execute(input_ch.reader, output_ch.writer, observer=spy),
        )
        await output_ch.reader.collect()

        # Second run was all hits.
        assert len(spy.data_ends) == 2
        assert all(cached is True for _, _, _, _, cached in spy.data_ends)

    @pytest.mark.asyncio
    async def test_correlation_key_absent_from_observer_callbacks(self):
        """Observer on_data_start/on_data_end must not see _tag_node_input_ref (first run = cache misses)."""
        from orcapod.core.nodes.function_node import _TAG_NODE_INPUT_REF
        from orcapod.system_constants import constants as _constants

        node, _, _ = _make_db_node(3)
        spy = _SpyObserver()

        # First run — all are cache misses, pod path is used with stamped tags.
        input_ch: Channel = Channel(buffer_size=8)
        output_ch: Channel = Channel(buffer_size=8)

        async def feed() -> None:
            for tag, data in node._input_stream.iter_data():
                await input_ch.writer.send((tag, data))
            await input_ch.writer.close()

        await asyncio.gather(
            feed(),
            node.async_execute(input_ch.reader, output_ch.writer, observer=spy),
        )
        await output_ch.reader.collect()

        full_key = f"{_constants.META_PREFIX}{_TAG_NODE_INPUT_REF}"
        for _, tag, _ in spy.data_starts:
            assert full_key not in tag.get_meta_info(), (
                f"on_data_start leaked correlation key {full_key!r}"
            )
        for _, tag, _, _, _ in spy.data_ends:
            assert full_key not in tag.get_meta_info(), (
                f"on_data_end leaked correlation key {full_key!r}"
            )

    @pytest.mark.asyncio
    async def test_semaphore_not_leaked_on_computation_crash(self):
        """When a data function raises, processing continues for remaining items."""
        call_count = 0

        def sometimes_fail(value: int) -> int:
            nonlocal call_count
            call_count += 1
            if value == 1:
                raise ValueError("deliberate failure")
            return value * 2

        src = _make_source(3)
        pod = FunctionPod(
            PythonDataFunction(sometimes_fail, output_keys="result"),
            pod_config=PodConfig(max_concurrency=1),
        )
        pipeline_db = InMemoryArrowDatabase()
        result_db = InMemoryArrowDatabase()
        node = FunctionJobNode(
            pod, src, pipeline_database=pipeline_db, result_database=result_db
        )

        results = await _run_node(node)
        # value=0 → 0, value=1 → crash (skipped), value=2 → 4
        assert len(results) == 2
        values = sorted(data.as_dict()["result"] for _, data in results)
        assert values == [0, 4]

    @pytest.mark.asyncio
    async def test_filtered_items_update_in_memory_cache(self):
        """Items filtered by the pod (output_data=None) must be stored in
        _cached_output_datas so the sync iter_data() path sees them.

        Regression: before the fix, _NodeLabelObserver.on_data_end with
        out=None never popped the input_store entry or updated
        _cached_output_datas, so filtered inputs were always recomputed and
        the input_store accumulated entries for the lifetime of the call.
        """
        node, _, _ = _make_db_node(3)

        # Deactivate the data function so every input is filtered (call → None).
        node._function_pod.data_function.set_active(False)

        results = await _run_node(node)
        # All items filtered — nothing emitted.
        assert len(results) == 0

        # _cached_output_datas must have one entry per input, with None data,
        # confirming that on_data_end cleaned up input_store and updated the cache.
        assert len(node._cached_output_datas) == 3
        for _tag, data in node._cached_output_datas.values():
            assert data is None

    @pytest.mark.asyncio
    async def test_crashed_items_cleaned_from_input_store(self):
        """Crashed items must not leave dangling entries in input_store.

        Regression: before the fix, _NodeLabelObserver.on_data_crash never
        popped the input_store entry, so the dict accumulated one entry per
        crash for the entire lifetime of the async_execute() call.

        Verification is indirect: if input_store cleanup works, no KeyError or
        deadlock occurs when the same inputs are re-run after a crash run.
        """

        def sometimes_fail(value: int) -> int:
            if value == 1:
                raise ValueError("deliberate failure")
            return value * 2

        src = _make_source(3)
        pod = FunctionPod(PythonDataFunction(sometimes_fail, output_keys="result"))
        pipeline_db = InMemoryArrowDatabase()
        result_db = InMemoryArrowDatabase()
        node = FunctionJobNode(
            pod, src, pipeline_database=pipeline_db, result_database=result_db
        )

        # First run — value=1 crashes.
        first = await _run_node(node)
        assert len(first) == 2

        # Second run — value=1 still crashes, others are cache hits.
        node.clear_cache()
        second = await _run_node(node)
        assert len(second) == 2
        values = sorted(data.as_dict()["result"] for _, data in second)
        assert values == [0, 4]


# ---------------------------------------------------------------------------
# DB path — ephemeral
# ---------------------------------------------------------------------------


class TestFunctionJobNodeAsyncExecuteEphemeral:
    @pytest.mark.asyncio
    async def test_ephemeral_pipeline_records_have_is_ephemeral_true(self):
        """Ephemeral execution writes pipeline records with is_ephemeral=True."""
        from orcapod.system_constants import constants
        from orcapod.types import NodeConfig

        def double(value: int) -> int:
            return value * 2

        src = _make_source(2)
        pod = FunctionPod(PythonDataFunction(double, output_keys="result"))
        pipeline_db = InMemoryArrowDatabase()
        result_db = InMemoryArrowDatabase()
        ephemeral_db = InMemoryArrowDatabase()
        node = FunctionJobNode(
            pod, src, pipeline_database=pipeline_db, result_database=result_db
        )
        node.node_config = NodeConfig(is_result_ephemeral=True)
        node.set_ephemeral_store(ephemeral_db)

        results = await _run_node(node)
        assert len(results) == 2

        records = pipeline_db.get_all_records(node._versioned_pipeline_path)
        assert records is not None
        assert records.num_rows == 2
        # All rows should have is_ephemeral=True
        is_eph_col = records.column(constants.IS_EPHEMERAL_COL).to_pylist()
        assert all(v is True for v in is_eph_col)

    @pytest.mark.asyncio
    async def test_ephemeral_missing_store_raises_runtime_error(self):
        """Raises RuntimeError when is_result_ephemeral=True but no store set."""
        from orcapod.types import NodeConfig

        def double(value: int) -> int:
            return value * 2

        src = _make_source(1)
        pod = FunctionPod(PythonDataFunction(double, output_keys="result"))
        pipeline_db = InMemoryArrowDatabase()
        result_db = InMemoryArrowDatabase()
        node = FunctionJobNode(
            pod, src, pipeline_database=pipeline_db, result_database=result_db
        )
        node.node_config = NodeConfig(is_result_ephemeral=True)
        # Deliberately do NOT call set_ephemeral_store()

        with pytest.raises(RuntimeError, match="ephemeral"):
            await _run_node(node)
