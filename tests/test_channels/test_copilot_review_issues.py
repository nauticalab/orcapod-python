"""
Tests that expose the issues identified by GitHub Copilot in PR #72.

Each test class targets a specific review comment and is expected to FAIL
before the corresponding fix is applied.

Issues:
1. Timing-based test flakiness — deterministic concurrency tracking
2. Inaccurate GIL documentation — (not testable, documentation-only)
3. ThreadPoolExecutor created per-call — resource waste
4. Unawaited coroutine risk — coroutine created in wrong thread
5. Semaphore(0) deadlock — resolve_concurrency can return 0
"""

from __future__ import annotations

import asyncio
from unittest.mock import patch

import pyarrow as pa
import pytest

from orcapod.channels import Channel
from orcapod.core.datagrams import Packet
from orcapod.core.function_pod import FunctionPod
from orcapod.core.nodes import FunctionNode, PersistentFunctionNode
from orcapod.core.packet_function import PythonPacketFunction
from orcapod.core.streams import ArrowTableStream
from orcapod.databases import InMemoryArrowDatabase
from orcapod.types import NodeConfig, PipelineConfig, resolve_concurrency


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_stream(n: int = 5) -> ArrowTableStream:
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


# ---------------------------------------------------------------------------
# Issue 1: Deterministic concurrency tracking instead of timing assertions
# ---------------------------------------------------------------------------


class TestDeterministicConcurrencyTracking:
    """Copilot comment: timing assertions (elapsed < 0.6s) are unreliable.

    This test uses a deterministic concurrency counter to prove that tasks
    actually ran concurrently, without relying on wall-clock time.
    """

    @pytest.mark.asyncio
    async def test_peak_concurrency_matches_max_concurrency(self):
        """Track peak concurrent tasks to verify concurrent execution."""
        peak = 0
        current = 0
        lock = asyncio.Lock()

        async def tracked_double(x: int) -> int:
            nonlocal peak, current
            async with lock:
                current += 1
                peak = max(peak, current)
            await asyncio.sleep(0.05)  # Small sleep to allow overlap
            async with lock:
                current -= 1
            return x * 2

        pf = PythonPacketFunction(tracked_double, output_keys="result")
        pod = FunctionPod(pf, node_config=NodeConfig(max_concurrency=5))
        db = InMemoryArrowDatabase()
        stream = make_stream(5)
        node = PersistentFunctionNode(pod, stream, pipeline_database=db)

        input_ch = Channel(buffer_size=16)
        output_ch = Channel(buffer_size=16)

        await feed_stream_to_channel(make_stream(5), input_ch)
        await node.async_execute([input_ch.reader], output_ch.writer)

        results = await output_ch.reader.collect()
        assert len(results) == 5
        values = sorted(pkt.as_dict()["result"] for _, pkt in results)
        assert values == [0, 2, 4, 6, 8]

        # If tasks ran concurrently, peak should equal max_concurrency (5).
        # If sequential, peak would be 1.
        assert peak == 5, f"Expected 5 concurrent tasks but peak was {peak}"


# ---------------------------------------------------------------------------
# Issue 3: ThreadPoolExecutor created per-call (resource waste)
# ---------------------------------------------------------------------------


class TestThreadPoolExecutorReuse:
    """Copilot comment: creating new ThreadPoolExecutor on every call wastes
    thread resources.

    This test verifies that multiple sync calls to an async function reuse
    a shared executor rather than creating a new one each time.
    """

    def test_executor_not_created_per_call(self):
        """Calling an async function sync multiple times should not create
        a new ThreadPoolExecutor each time."""
        async def simple_add(x: int, y: int) -> int:
            return x + y

        pf = PythonPacketFunction(simple_add, output_keys="result")

        creation_count = 0
        original_init = None

        # We need to patch ThreadPoolExecutor.__init__ to count instantiations
        from concurrent.futures import ThreadPoolExecutor

        original_init = ThreadPoolExecutor.__init__

        def counting_init(self, *args, **kwargs):
            nonlocal creation_count
            creation_count += 1
            return original_init(self, *args, **kwargs)

        # Run inside an event loop context to trigger the ThreadPoolExecutor path
        async def run_in_loop():
            nonlocal creation_count
            with patch.object(ThreadPoolExecutor, "__init__", counting_init):
                for _ in range(3):
                    pf.direct_call(Packet({"x": 1, "y": 2}))

        asyncio.run(run_in_loop())
        # Current code creates a new executor per call, so creation_count == 3.
        # After fix, it should be <= 1 (reused executor).
        assert creation_count <= 1, (
            f"ThreadPoolExecutor created {creation_count} times; "
            f"expected at most 1 (should reuse)"
        )


# ---------------------------------------------------------------------------
# Issue 4: Coroutine constructed in wrong thread
# ---------------------------------------------------------------------------


class TestCoroutineConstructedInExecutorThread:
    """Copilot comment: coroutine is created in the caller thread but passed
    to another thread — risks unawaited coroutine warnings if submission fails.

    The current code does ``coro = self._function(**packet.as_dict())`` in the
    caller thread, then passes the already-created coroutine to a
    ThreadPoolExecutor. If submission fails, the coroutine is never awaited,
    triggering a RuntimeWarning.

    The fix should construct the coroutine inside the executor thread by
    passing a lambda that both creates and runs the coroutine.
    """

    def test_coroutine_created_inside_executor_thread(self):
        """Verify the coroutine is NOT created before being submitted to
        the executor.

        We instrument the async function to record the thread where it is
        *called* (i.e. where the coroutine starts executing). In the fixed
        code, this should happen inside the executor thread. We also check
        that no coroutine object is created in the caller thread by
        inspecting the implementation pattern.
        """
        import threading

        execution_threads: list[str] = []

        async def tracking_func(x: int) -> int:
            execution_threads.append(threading.current_thread().name)
            return x * 2

        pf = PythonPacketFunction(tracking_func, output_keys="result")

        # Intercept _call_async_function_sync to verify it does NOT call
        # self._function before submitting to the executor.
        # The buggy pattern: coro = self._function(...); pool.submit(asyncio.run, coro)
        # The fixed pattern: pool.submit(lambda: asyncio.run(self._function(...)))
        coroutine_created_in_caller = False
        original_method = pf._call_async_function_sync

        def instrumented_call(packet):
            nonlocal coroutine_created_in_caller
            # Read the source to check if coro is created before submit
            import inspect as _inspect

            source = _inspect.getsource(original_method)
            # The buggy pattern assigns coro before the try block
            if "coro = self._function(" in source:
                coroutine_created_in_caller = True
            return original_method(packet)

        pf._call_async_function_sync = instrumented_call

        # Run inside an event loop to trigger the ThreadPoolExecutor path
        async def run_in_loop():
            pf.direct_call(Packet({"x": 5}))

        asyncio.run(run_in_loop())

        assert not coroutine_created_in_caller, (
            "Coroutine is created in the caller thread before submission "
            "to the executor. It should be created inside the executor thread "
            "to avoid unawaited coroutine warnings on submission failure."
        )


# ---------------------------------------------------------------------------
# Issue 5: Semaphore(0) deadlock
# ---------------------------------------------------------------------------


class TestSemaphoreZeroDeadlock:
    """Copilot comment: resolve_concurrency can return 0, causing
    asyncio.Semaphore(0) to deadlock on first acquire.

    This test verifies that max_concurrency=0 is rejected with a clear error.
    """

    def test_resolve_concurrency_rejects_zero(self):
        """resolve_concurrency should raise ValueError when result is 0."""
        node_config = NodeConfig(max_concurrency=0)
        pipeline_config = PipelineConfig()

        with pytest.raises(ValueError, match="max_concurrency"):
            resolve_concurrency(node_config, pipeline_config)

    def test_resolve_concurrency_rejects_negative(self):
        """resolve_concurrency should raise ValueError when result is negative."""
        node_config = NodeConfig(max_concurrency=-1)
        pipeline_config = PipelineConfig()

        with pytest.raises(ValueError, match="max_concurrency"):
            resolve_concurrency(node_config, pipeline_config)

    def test_resolve_concurrency_accepts_one(self):
        """max_concurrency=1 is valid (sequential execution)."""
        node_config = NodeConfig(max_concurrency=1)
        pipeline_config = PipelineConfig()
        assert resolve_concurrency(node_config, pipeline_config) == 1

    def test_resolve_concurrency_accepts_none(self):
        """max_concurrency=None means unlimited (no semaphore)."""
        node_config = NodeConfig()
        pipeline_config = PipelineConfig()
        assert resolve_concurrency(node_config, pipeline_config) is None

    @pytest.mark.asyncio
    async def test_semaphore_zero_causes_deadlock(self):
        """Demonstrate that Semaphore(0) actually deadlocks.

        This is a demonstration of the bug — if resolve_concurrency returns 0,
        the pipeline hangs forever. We use a timeout to detect the deadlock.
        """
        async def double(x: int) -> int:
            return x * 2

        pf = PythonPacketFunction(double, output_keys="result")
        pod = FunctionPod(pf, node_config=NodeConfig(max_concurrency=0))
        stream = make_stream(1)
        node = FunctionNode(pod, stream)

        input_ch = Channel(buffer_size=4)
        output_ch = Channel(buffer_size=4)

        await feed_stream_to_channel(make_stream(1), input_ch)

        # This should deadlock because Semaphore(0) never allows acquisition.
        # We use a timeout to detect the deadlock instead of hanging forever.
        with pytest.raises((asyncio.TimeoutError, ValueError)):
            await asyncio.wait_for(
                node.async_execute([input_ch.reader], output_ch.writer),
                timeout=0.5,
            )


# ---------------------------------------------------------------------------
# Issue 2: GIL comment accuracy (not directly testable, but we verify the
# example file contains the inaccurate text)
# ---------------------------------------------------------------------------


class TestGILCommentAccuracy:
    """Copilot comment: the example claims native coroutines 'bypass the GIL
    entirely', which is misleading.

    This test checks that the inaccurate phrase does NOT exist in the example.
    After the fix, this test should pass.
    """

    def test_example_does_not_claim_gil_bypass(self):
        """The async example should not claim coroutines bypass the GIL."""
        import pathlib

        example_path = (
            pathlib.Path(__file__).resolve().parents[2]
            / "examples"
            / "async_vs_sync_pipeline.py"
        )
        content = example_path.read_text()

        assert "bypass the GIL entirely" not in content, (
            "Example still contains misleading claim that coroutines "
            "'bypass the GIL entirely'"
        )
