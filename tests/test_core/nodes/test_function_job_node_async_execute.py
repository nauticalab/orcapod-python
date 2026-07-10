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
            await asyncio.sleep(0.01)
            concurrent_count -= 1
            return await original_async_call(data, **kwargs)

        pf.async_call = patched_async_call  # type: ignore[method-assign]

        src = _make_source(5)
        pod = FunctionPod(pf, pod_config=PodConfig(max_concurrency=1))
        node = FunctionJobNode(pod, src)
        results = await _run_node(node)
        assert len(results) == 5
        assert max_observed <= 1
