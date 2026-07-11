"""Tests for _FunctionPodBase.async_execute() — observer hooks and backpressure."""

from __future__ import annotations

import asyncio

import pyarrow as pa
import pytest

from orcapod.channels import Channel
from orcapod.core.data_function import PythonDataFunction
from orcapod.core.function_pod import FunctionPod
from orcapod.core.streams.arrow_table_stream import ArrowTableStream
from orcapod.types import PodConfig


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_stream(n: int = 3) -> ArrowTableStream:
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("x", pa.int64(), nullable=False),
        ]
    )
    table = pa.table(
        {
            "id": pa.array(list(range(n)), type=pa.int64()),
            "x": pa.array(list(range(n)), type=pa.int64()),
        },
        schema=schema,
    )
    return ArrowTableStream(table, tag_columns=["id"])


async def _feed(stream: ArrowTableStream, ch: Channel) -> None:
    for tag, data in stream.iter_data():
        await ch.writer.send((tag, data))
    await ch.writer.close()


class _SpyObserver:
    def __init__(self):
        self.starts = []
        self.ends = []   # [(label, tag, input, output, cached)]
        self.crashes = []

    def contextualize(self, *path):
        return self

    def on_run_start(self, *a, **kw): pass
    def on_run_end(self, *a, **kw): pass
    def on_node_start(self, *a, **kw): pass
    def on_node_end(self, *a, **kw): pass

    def on_data_start(self, label, tag, data):
        self.starts.append((label, tag, data))

    def on_data_end(self, label, tag, inp, out, *, cached):
        self.ends.append((label, tag, inp, out, cached))

    def on_data_crash(self, label, tag, data, exc):
        self.crashes.append(exc)

    def create_data_logger(self, tag, data):
        return None


# ---------------------------------------------------------------------------
# Observer hooks
# ---------------------------------------------------------------------------


class TestFunctionPodBaseAsyncExecuteObserver:
    @pytest.mark.asyncio
    async def test_on_data_start_and_end_fire_per_item(self):
        """on_data_start fires before and on_data_end(cached=False) fires after each item."""

        def double(x: int) -> int:
            return x * 2

        pf = PythonDataFunction(double, output_keys="result")
        pod = FunctionPod(pf)
        stream = _make_stream(3)
        input_ch: Channel = Channel(buffer_size=16)
        output_ch: Channel = Channel(buffer_size=16)
        await _feed(stream, input_ch)

        spy = _SpyObserver()
        await pod.async_execute([input_ch.reader], output_ch.writer, observer=spy)
        results = await output_ch.reader.collect()

        assert len(results) == 3
        assert len(spy.starts) == 3
        assert len(spy.ends) == 3
        assert all(cached is False for _, _, _, _, cached in spy.ends)

    @pytest.mark.asyncio
    async def test_on_data_crash_fires_not_on_data_end(self):
        """on_data_crash fires (and on_data_end does not) when processing raises."""

        def boom(x: int) -> int:
            raise ValueError("bang")

        pf = PythonDataFunction(boom, output_keys="result")
        pod = FunctionPod(pf)
        stream = _make_stream(2)
        input_ch: Channel = Channel(buffer_size=16)
        output_ch: Channel = Channel(buffer_size=16)
        await _feed(stream, input_ch)

        spy = _SpyObserver()
        await pod.async_execute([input_ch.reader], output_ch.writer, observer=spy)
        results = await output_ch.reader.collect()

        assert len(results) == 0
        assert len(spy.crashes) == 2
        assert len(spy.ends) == 0


# ---------------------------------------------------------------------------
# Backpressure
# ---------------------------------------------------------------------------


class TestFunctionPodBaseAsyncExecuteBackpressure:
    @pytest.mark.asyncio
    async def test_max_concurrency_limits_concurrent_tasks(self):
        """With max_concurrency=1, at most one task runs concurrently."""
        concurrent_count = 0
        max_observed = 0

        def double(x: int) -> int:
            return x * 2

        pf = PythonDataFunction(double, output_keys="result")
        original_async_call = pf.async_call

        async def patched(data, **kw):
            nonlocal concurrent_count, max_observed
            concurrent_count += 1
            max_observed = max(max_observed, concurrent_count)
            try:
                await asyncio.sleep(0.01)
                return await original_async_call(data, **kw)
            finally:
                concurrent_count -= 1

        pf.async_call = patched  # type: ignore[method-assign]

        pod = FunctionPod(pf, pod_config=PodConfig(max_concurrency=1))
        stream = _make_stream(5)
        input_ch: Channel = Channel(buffer_size=32)
        output_ch: Channel = Channel(buffer_size=32)
        await _feed(stream, input_ch)
        await pod.async_execute([input_ch.reader], output_ch.writer)
        results = await output_ch.reader.collect()
        assert len(results) == 5
        assert max_observed <= 1
