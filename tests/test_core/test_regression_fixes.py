"""
Regression tests for bugs fixed in the packet-function-executor-system branch.

Covers:
1. async_execute output channel closed on exception (try/finally)
2. PacketFunctionWrapper.direct_call/direct_async_call bypass executor routing
3. Concurrent iteration falls back to sequential inside a running event loop
4. FunctionPod.async_execute backpressure bounds pending tasks
5. _materialize_to_stream preserves source_info provenance tokens
6. RayExecutor._ensure_ray_initialized uses ray_address
7. PacketFunctionExecutorProtocol uses PacketFunctionProtocol (not Any)
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from orcapod.channels import Channel, ChannelClosed
from orcapod.core.datagrams import Packet, Tag
from orcapod.core.executors import LocalExecutor, PacketFunctionExecutorBase
from orcapod.core.function_pod import FunctionPod, FunctionPodStream
from orcapod.core.operators import SelectPacketColumns
from orcapod.core.operators.join import Join
from orcapod.core.operators.static_output_pod import StaticOutputOperatorPod
from orcapod.core.packet_function import (
    PacketFunctionWrapper,
    PythonPacketFunction,
)
from orcapod.core.sources.dict_source import DictSource
from orcapod.core.streams.arrow_table_stream import ArrowTableStream
from orcapod.protocols.core_protocols import (
    PacketFunctionExecutorProtocol,
    PacketFunctionProtocol,
    PacketProtocol,
)
from orcapod.types import NodeConfig

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_stream(n: int = 3) -> ArrowTableStream:
    """Stream with tag=id, packet=x (ints)."""
    table = pa.table(
        {
            "id": pa.array(list(range(n)), type=pa.int64()),
            "x": pa.array(list(range(n)), type=pa.int64()),
        }
    )
    return ArrowTableStream(table, tag_columns=["id"])


async def feed_stream_to_channel(stream: ArrowTableStream, ch: Channel) -> None:
    for tag, packet in stream.iter_packets():
        await ch.writer.send((tag, packet))
    await ch.writer.close()


class SpyExecutor(PacketFunctionExecutorBase):
    """Records all calls and delegates to direct_call."""

    def __init__(self) -> None:
        self.calls: list[tuple[Any, Any]] = []

    @property
    def executor_type_id(self) -> str:
        return "spy"

    def supported_function_type_ids(self) -> frozenset[str]:
        return frozenset()

    def execute(
        self,
        packet_function: PacketFunctionProtocol,
        packet: PacketProtocol,
    ) -> PacketProtocol | None:
        self.calls.append((packet_function, packet))
        return packet_function.direct_call(packet)


# ===========================================================================
# 1. async_execute output channel closed on exception (try/finally)
# ===========================================================================


class TestAsyncExecuteChannelCloseOnError:
    """Output channel must be closed even when async_execute raises."""

    @pytest.mark.asyncio
    async def test_unary_operator_closes_channel_on_error(self):
        """SelectPacketColumns with a column that doesn't exist should fail,
        but the output channel must still be closed."""

        def failing(x: int) -> int:
            raise ValueError("boom")

        pf = PythonPacketFunction(failing, output_keys="result")
        pod = FunctionPod(pf)

        stream = make_stream(3)
        input_ch = Channel(buffer_size=16)
        output_ch = Channel(buffer_size=16)

        await feed_stream_to_channel(stream, input_ch)

        with pytest.raises(ExceptionGroup):
            await pod.async_execute([input_ch.reader], output_ch.writer)

        # The output channel should be closed despite the error.
        # Attempting to collect should return whatever was sent before error,
        # and not hang forever.
        results = await output_ch.reader.collect()
        # We don't assert content, just that it doesn't hang.
        assert isinstance(results, list)

    @pytest.mark.asyncio
    async def test_operator_closes_channel_on_static_process_error(self):
        """If static_process raises, output channel must still be closed."""
        stream = make_stream(3)

        # SelectPacketColumns with non-existent column will error during
        # static_process. Use a mock operator that raises.
        op = SelectPacketColumns(columns=["nonexistent_col"])

        input_ch = Channel(buffer_size=16)
        output_ch = Channel(buffer_size=16)

        await feed_stream_to_channel(stream, input_ch)

        with pytest.raises(Exception):
            await op.async_execute([input_ch.reader], output_ch.writer)

        # Channel should be closed — collect should not hang.
        results = await output_ch.reader.collect()
        assert isinstance(results, list)

    @pytest.mark.asyncio
    async def test_static_output_pod_closes_channel_on_empty_input(self):
        """Empty input should be handled gracefully with channel still closed.

        Streaming async_execute processes rows individually, so empty input
        simply means zero iterations and a clean close — no error raised.
        """
        op = SelectPacketColumns(columns=["x"])

        input_ch = Channel(buffer_size=4)
        output_ch = Channel(buffer_size=4)

        await input_ch.writer.close()  # empty input

        # Streaming async_execute handles empty input gracefully.
        await op.async_execute([input_ch.reader], output_ch.writer)

        # Channel should be closed and empty.
        results = await output_ch.reader.collect()
        assert results == []


# ===========================================================================
# 2. PacketFunctionWrapper.direct_call bypasses executor routing
# ===========================================================================


class TestWrapperDirectCallBypassesExecutor:
    """direct_call and direct_async_call on a wrapper must NOT go through
    the executor.  Before the fix, they delegated to call/async_call which
    re-entered executor routing."""

    @staticmethod
    def _make_add_pf_with_spy() -> tuple[
        PythonPacketFunction, SpyExecutor, PacketFunctionWrapper
    ]:
        def add(x: int, y: int) -> int:
            return x + y

        spy = SpyExecutor()
        inner_pf = PythonPacketFunction(add, output_keys="result")
        inner_pf.executor = spy
        wrapper = PacketFunctionWrapper(inner_pf, version="v0.0")
        return inner_pf, spy, wrapper

    def test_direct_call_does_not_invoke_executor(self):
        _, spy, wrapper = self._make_add_pf_with_spy()

        packet = Packet({"x": 3, "y": 4})
        result = wrapper.direct_call(packet)

        assert result is not None
        assert result.as_dict()["result"] == 7
        # Executor should NOT have been invoked.
        assert len(spy.calls) == 0

    @pytest.mark.asyncio
    async def test_direct_async_call_does_not_invoke_executor(self):
        _, spy, wrapper = self._make_add_pf_with_spy()

        packet = Packet({"x": 3, "y": 4})
        result = await wrapper.direct_async_call(packet)

        assert result is not None
        assert result.as_dict()["result"] == 7
        assert len(spy.calls) == 0

    def test_call_still_routes_through_executor(self):
        """Sanity check: regular call() should still route through executor."""
        _, spy, wrapper = self._make_add_pf_with_spy()

        packet = Packet({"x": 3, "y": 4})
        result = wrapper.call(packet)

        assert result is not None
        assert result.as_dict()["result"] == 7
        assert len(spy.calls) == 1


# ===========================================================================
# 3. Concurrent iteration falls back inside running event loop
# ===========================================================================


class TestConcurrentFallbackInRunningLoop:
    """_iter_packets_concurrent must not crash when called from inside
    an already-running asyncio event loop — should fall back to sequential
    process_packet calls."""

    @staticmethod
    def _make_concurrent_stream() -> tuple[FunctionPodStream, FunctionPod]:
        def double(x: int) -> int:
            return x * 2

        pf = PythonPacketFunction(double, output_keys="result")
        # Attach an executor that reports concurrent support
        executor = LocalExecutor()
        pf.executor = executor
        pod = FunctionPod(pf)

        table = pa.table(
            {
                "id": pa.array([0, 1, 2], type=pa.int64()),
                "x": pa.array([10, 20, 30], type=pa.int64()),
            }
        )
        from orcapod.core.streams.arrow_table_stream import ArrowTableStream

        stream = ArrowTableStream(table, tag_columns=["id"])
        return pod.process(stream), pod

    @pytest.mark.asyncio
    async def test_falls_back_to_sequential_in_async_context(self):
        """When called from async code, should fall back to sequential
        execution instead of raising RuntimeError."""
        pod_stream, _ = self._make_concurrent_stream()
        results = list(pod_stream.iter_packets())

        assert len(results) == 3
        values = sorted(pkt.as_dict()["result"] for _, pkt in results)
        assert values == [20, 40, 60]

    def test_uses_asyncio_run_when_no_loop(self):
        """When there is no running event loop, it should use asyncio.run
        (concurrent path)."""
        pod_stream, _ = self._make_concurrent_stream()
        results = list(pod_stream.iter_packets())

        assert len(results) == 3
        values = sorted(pkt.as_dict()["result"] for _, pkt in results)
        assert values == [20, 40, 60]


# ===========================================================================
# 4. FunctionPod.async_execute backpressure bounds pending tasks
# ===========================================================================


class TestAsyncExecuteBackpressure:
    """With max_concurrency set, pending tasks should be bounded."""

    @pytest.mark.asyncio
    async def test_semaphore_limits_concurrent_tasks(self):
        """With max_concurrency=1, at most one task should be running."""
        concurrent_count = 0
        max_concurrent = 0

        async def track_concurrency(x: int) -> int:
            nonlocal concurrent_count, max_concurrent
            concurrent_count += 1
            max_concurrent = max(max_concurrent, concurrent_count)
            await asyncio.sleep(0.01)  # simulate work
            concurrent_count -= 1
            return x * 2

        def double(x: int) -> int:
            return x * 2

        # Build a PythonPacketFunction that uses our async-aware tracker.
        # We override async_call to directly call our async function.
        pf = PythonPacketFunction(double, output_keys="result")

        # Patch async_call to use our concurrency-tracking function
        original_async_call = pf.async_call

        async def tracked_async_call(packet: PacketProtocol) -> PacketProtocol | None:
            nonlocal concurrent_count, max_concurrent
            concurrent_count += 1
            max_concurrent = max(max_concurrent, concurrent_count)
            await asyncio.sleep(0.01)
            concurrent_count -= 1
            return await original_async_call(packet)

        pf.async_call = tracked_async_call  # type: ignore

        pod = FunctionPod(pf, node_config=NodeConfig(max_concurrency=1))

        stream = make_stream(5)
        input_ch = Channel(buffer_size=32)
        output_ch = Channel(buffer_size=32)

        await feed_stream_to_channel(stream, input_ch)
        await pod.async_execute([input_ch.reader], output_ch.writer)

        results = await output_ch.reader.collect()
        assert len(results) == 5
        # With semaphore acquired before task creation and max_concurrency=1,
        # at most 1 should be in-flight at a time.
        assert max_concurrent <= 1


# ===========================================================================
# 5. _materialize_to_stream preserves source_info provenance
# ===========================================================================


class TestMaterializePreservesSourceInfo:
    """_materialize_to_stream must preserve source_info provenance tokens
    rather than replacing them with None."""

    def test_source_info_preserved_through_round_trip(self):
        """Packets with source_info should retain their provenance tokens
        after being materialized into a stream and back."""
        source = DictSource(
            data=[
                {"id": 0, "x": 10},
                {"id": 1, "x": 20},
            ],
            tag_columns=["id"],
        )

        rows = list(source.iter_packets())
        rebuilt = StaticOutputOperatorPod._materialize_to_stream(rows)

        # The original packets should have non-None source_info
        original_source_info = rows[0][1].source_info()
        assert any(v is not None for v in original_source_info.values()), (
            "Test setup: original packets should have source_info tokens"
        )

        # The rebuilt stream's packets should also have non-None source_info.
        rebuilt_rows = list(rebuilt.iter_packets())
        for _, rebuilt_pkt in rebuilt_rows:
            for key, val in rebuilt_pkt.source_info().items():
                orig_val = original_source_info.get(key)
                if orig_val is not None:
                    assert val is not None, (
                        f"source_info[{key!r}] was {orig_val!r} but became None "
                        f"after _materialize_to_stream round-trip"
                    )

    def test_materialize_source_columns_in_table(self):
        """The rebuilt stream's full table should contain source columns."""
        source = DictSource(
            data=[
                {"id": 0, "x": 10},
            ],
            tag_columns=["id"],
        )
        rows = list(source.iter_packets())

        rebuilt = StaticOutputOperatorPod._materialize_to_stream(rows)
        rebuilt_table = rebuilt.as_table(all_info=True)

        # Should have source info columns in the table
        source_cols = [
            c for c in rebuilt_table.column_names if c.startswith("_source_")
        ]
        assert len(source_cols) > 0, "Rebuilt stream should contain _source_ columns"


# ===========================================================================
# 6. RayExecutor._ensure_ray_initialized uses ray_address
# ===========================================================================


class TestRayExecutorInitialization:
    """RayExecutor should use ray_address when initializing Ray."""

    def test_ensure_ray_initialized_uses_address(self):
        """Mock ray to verify _ensure_ray_initialized calls ray.init
        with the stored address."""
        mock_ray = MagicMock()
        mock_ray.is_initialized.return_value = False

        with patch.dict("sys.modules", {"ray": mock_ray}):
            from orcapod.core.executors.ray import RayExecutor

            executor = RayExecutor.__new__(RayExecutor)
            executor._ray_address = "ray://my-cluster:10001"
            executor._num_cpus = None
            executor._num_gpus = None
            executor._resources = None

            executor._ensure_ray_initialized()

            mock_ray.init.assert_called_once_with(address="ray://my-cluster:10001")

    def test_ensure_ray_initialized_auto_when_no_address(self):
        """When ray_address is None, ray.init() is called without args."""
        mock_ray = MagicMock()
        mock_ray.is_initialized.return_value = False

        with patch.dict("sys.modules", {"ray": mock_ray}):
            from orcapod.core.executors.ray import RayExecutor

            executor = RayExecutor.__new__(RayExecutor)
            executor._ray_address = None
            executor._num_cpus = None
            executor._num_gpus = None
            executor._resources = None

            executor._ensure_ray_initialized()

            mock_ray.init.assert_called_once_with()

    def test_ensure_ray_initialized_skips_when_already_initialized(self):
        """When Ray is already initialized, don't call ray.init again."""
        mock_ray = MagicMock()
        mock_ray.is_initialized.return_value = True

        with patch.dict("sys.modules", {"ray": mock_ray}):
            from orcapod.core.executors.ray import RayExecutor

            executor = RayExecutor.__new__(RayExecutor)
            executor._ray_address = "ray://my-cluster:10001"
            executor._num_cpus = None
            executor._num_gpus = None
            executor._resources = None

            executor._ensure_ray_initialized()

            mock_ray.init.assert_not_called()

    def test_async_execute_uses_wrap_future(self):
        """async_execute should use ref.future() + asyncio.wrap_future,
        not bare 'await ref'."""
        import inspect

        from orcapod.core.executors.ray import RayExecutor

        source = inspect.getsource(RayExecutor.async_execute)
        assert "ref.future()" in source, (
            "async_execute should use ref.future() for asyncio compatibility"
        )
        assert "wrap_future" in source, "async_execute should use asyncio.wrap_future"
        # Should NOT do bare 'await ref'
        assert "return await ref\n" not in source, (
            "async_execute should not use bare 'await ref'"
        )


# ===========================================================================
# 7. PacketFunctionExecutorProtocol type safety
# ===========================================================================


class TestExecutorProtocolTypeSafety:
    """PacketFunctionExecutorProtocol.execute() and async_execute() should
    accept PacketFunctionProtocol, not Any."""

    def test_protocol_execute_annotation_is_typed(self):
        """The execute method's packet_function parameter should be
        annotated with PacketFunctionProtocol, not Any."""
        import inspect

        # With `from __future__ import annotations`, annotations are stored
        # as strings. Check the raw annotation string.
        raw_hints = inspect.get_annotations(
            PacketFunctionExecutorProtocol.execute, eval_str=False
        )
        pf_annotation = raw_hints.get("packet_function", "")
        assert "PacketFunctionProtocol" in str(pf_annotation), (
            f"execute() packet_function should reference PacketFunctionProtocol, "
            f"got {pf_annotation!r}"
        )

    def test_protocol_async_execute_annotation_is_typed(self):
        """The async_execute method's packet_function parameter should be
        annotated with PacketFunctionProtocol, not Any."""
        import inspect

        raw_hints = inspect.get_annotations(
            PacketFunctionExecutorProtocol.async_execute, eval_str=False
        )
        pf_annotation = raw_hints.get("packet_function", "")
        assert "PacketFunctionProtocol" in str(pf_annotation), (
            f"async_execute() packet_function should reference PacketFunctionProtocol, "
            f"got {pf_annotation!r}"
        )
