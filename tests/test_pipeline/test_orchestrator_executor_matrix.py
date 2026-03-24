"""Orchestrator × Executor concurrency matrix tests.

Validates all four combinations of pipeline orchestrator and function executor
(sync vs async underlying function):

  ┌─────────────────────────────────────────────────────────────────────┐
  │             │  Sync function (def)  │  Async function (async def)   │
  ├─────────────┼───────────────────────┼───────────────────────────────┤
  │ Sync orch.  │  (1) default path     │  (2) async fn in sync context │
  │ Async orch. │  (3) thread-pool      │  (4) maximum concurrency      │
  └─────────────┴───────────────────────┴───────────────────────────────┘

Each cell is tested for:
  - **Correctness**: produces the expected output values.
  - **Graceful behaviour**: no deadlocks, no unhandled exceptions.

Additionally, cell (4) is verified to execute I/O-bound workloads
*measurably faster* than cell (1), confirming the concurrency benefit
restored by PR #99.

Terminology
-----------
* "sync orchestrator"  = ``SyncPipelineOrchestrator`` / ``ExecutorType.SYNCHRONOUS``
* "async orchestrator" = ``AsyncPipelineOrchestrator`` / ``ExecutorType.ASYNC_CHANNELS``
* "sync function"      = regular ``def`` (blocking, runs in executor thread
                         when called from async context)
* "async function"     = ``async def`` (native coroutine, awaited directly
                         when called from async context)
"""

from __future__ import annotations

import asyncio
import time
from concurrent.futures import ThreadPoolExecutor

import pyarrow as pa
import pytest

from orcapod.core.function_pod import FunctionPod
from orcapod.core.packet_function import PythonPacketFunction
from orcapod.core.sources import ArrowTableSource
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline import AsyncPipelineOrchestrator, Pipeline
from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator
from orcapod.types import ExecutorType, NodeConfig, PipelineConfig


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

_N_PACKETS = 5
_SLEEP_S = 0.2
_SEQUENTIAL_MIN = _N_PACKETS * _SLEEP_S * 0.8   # 80 % of serial time
_CONCURRENT_MAX = _SLEEP_S * 2.5                 # at most 2.5× single-step


def _make_source(n: int = _N_PACKETS) -> ArrowTableSource:
    """Simple source with ``id`` tag and ``x`` packet column."""
    table = pa.table(
        {
            "id": pa.array(list(range(n)), type=pa.int64()),
            "x": pa.array(list(range(n)), type=pa.int64()),
        }
    )
    return ArrowTableSource(table, tag_columns=["id"])


def _build_pipeline(
    fn: object,
    *,
    n: int = _N_PACKETS,
    max_concurrency: int | None = 5,
) -> Pipeline:
    """Construct an auto-compiled pipeline around *fn*."""
    pf = PythonPacketFunction(fn, output_keys="result")
    pod = FunctionPod(pf, node_config=NodeConfig(max_concurrency=max_concurrency))
    db = InMemoryArrowDatabase()
    pipeline = Pipeline(name="matrix_test", pipeline_database=db, auto_compile=True)
    with pipeline:
        pod(_make_source(n), label="node")
    return pipeline


def _run_sync(pipeline: Pipeline) -> tuple[list[int], float]:
    """Run with SyncPipelineOrchestrator, return (sorted results, elapsed)."""
    t0 = time.perf_counter()
    pipeline.run(config=PipelineConfig(executor=ExecutorType.SYNCHRONOUS))
    elapsed = time.perf_counter() - t0
    records = pipeline.node.get_all_records()
    assert records is not None
    return sorted(records.column("result").to_pylist()), elapsed


def _run_async(pipeline: Pipeline) -> tuple[list[int], float]:
    """Run with AsyncPipelineOrchestrator, return (sorted results, elapsed)."""
    t0 = time.perf_counter()
    pipeline.run(config=PipelineConfig(executor=ExecutorType.ASYNC_CHANNELS))
    elapsed = time.perf_counter() - t0
    records = pipeline.node.get_all_records()
    assert records is not None
    return sorted(records.column("result").to_pylist()), elapsed


# ---------------------------------------------------------------------------
# Shared functions used across tests
# ---------------------------------------------------------------------------

def sync_double(x: int) -> int:
    """Sync function: double x, simulating a fast computation."""
    return x * 2


async def async_double(x: int) -> int:
    """Async function: double x, simulating a fast async computation."""
    return x * 2


def slow_sync_double(x: int) -> int:
    """Sync I/O-bound function: blocks for SLEEP_S seconds."""
    import time as _time
    _time.sleep(_SLEEP_S)
    return x * 2


async def slow_async_double(x: int) -> int:
    """Async I/O-bound function: suspends for SLEEP_S seconds."""
    await asyncio.sleep(_SLEEP_S)
    return x * 2


_EXPECTED = [x * 2 for x in range(_N_PACKETS)]  # [0, 2, 4, 6, 8]

# ---------------------------------------------------------------------------
# Constants for thread-pool saturation test (async+sync vs async+async)
# ---------------------------------------------------------------------------

# Use a deliberately small thread pool so the sync path is forced into
# multiple rounds while the async path runs all coroutines concurrently.
_POOL_WORKERS = 2
_POOL_N_PACKETS = 6          # 3× the pool size → at least 3 rounds for sync
_POOL_SLEEP_S = 0.1          # per-packet sleep
# async+async: all 6 run at once → ~0.1 s
# async+sync:  2 threads × 3 rounds → ~0.3 s
_POOL_ASYNC_MAX = _POOL_SLEEP_S * 2.0    # generous upper bound for async path
_POOL_SYNC_MIN = _POOL_SLEEP_S * 2.5    # conservative lower bound for sync path


# ===========================================================================
# Combination 1 — Sync orchestrator + Sync function
# ===========================================================================


class TestSyncOrchestratorSyncFunction:
    """Cell (1): default sequential path — no concurrency overhead."""

    def test_correctness(self):
        """Sync orchestrator with a sync function produces correct results."""
        pipeline = _build_pipeline(sync_double)
        results, _ = _run_sync(pipeline)
        assert results == _EXPECTED

    def test_via_orchestrator_directly(self):
        """SyncPipelineOrchestrator.run() returns correct results directly."""
        pf = PythonPacketFunction(sync_double, output_keys="result")
        pod = FunctionPod(pf)
        pipeline = Pipeline(
            name="direct_orch",
            pipeline_database=InMemoryArrowDatabase(),
            auto_compile=True,
        )
        with pipeline:
            pod(_make_source(), label="node")

        orch = SyncPipelineOrchestrator()
        result = orch.run(pipeline._node_graph)
        fn_outputs = [
            v for k, v in result.node_outputs.items() if k.node_type == "function"
        ]
        assert len(fn_outputs) == 1
        values = sorted(pkt.as_dict()["result"] for _, pkt in fn_outputs[0])
        assert values == _EXPECTED

    def test_sequential_with_io_bound_work(self):
        """Sync orchestrator processes packets sequentially for I/O-bound work."""
        pipeline = _build_pipeline(slow_sync_double)
        results, elapsed = _run_sync(pipeline)
        assert results == _EXPECTED
        # 5 packets × 0.2 s each must take at least 80 % of serial time
        assert elapsed >= _SEQUENTIAL_MIN, (
            f"Expected sequential execution (>= {_SEQUENTIAL_MIN:.2f}s) "
            f"but took {elapsed:.2f}s"
        )


# ===========================================================================
# Combination 2 — Sync orchestrator + Async function
# ===========================================================================


class TestSyncOrchestratorAsyncFunction:
    """Cell (2): sync orchestrator handles ``async def`` functions gracefully.

    The sync orchestrator calls ``node.execute()`` which delegates to
    ``LocalExecutor.execute_callable``.  For async functions, that executor
    uses ``_run_async_sync`` (thread-pool bridge) so packets still complete
    without deadlock.  No *inter-packet* concurrency is expected, however.
    """

    def test_correctness(self):
        """Sync orchestrator with an async function produces correct results."""
        pipeline = _build_pipeline(async_double)
        results, _ = _run_sync(pipeline)
        assert results == _EXPECTED

    def test_graceful_no_deadlock_with_io_bound(self):
        """Sync orchestrator with a slow async function completes without deadlock."""
        pipeline = _build_pipeline(slow_async_double)
        results, elapsed = _run_sync(pipeline)
        assert results == _EXPECTED
        # Packets still run sequentially: async_execute is NOT called by the
        # sync orchestrator, so packets are bridged one-at-a-time via the
        # thread-pool bridge in LocalExecutor.execute_callable.
        assert elapsed >= _SEQUENTIAL_MIN, (
            f"Sync orchestrator should process packets sequentially "
            f"(>= {_SEQUENTIAL_MIN:.2f}s) but took {elapsed:.2f}s"
        )

    def test_matches_sync_function_results(self):
        """Sync orch + async fn produces the same values as sync orch + sync fn."""
        pipeline_sync_fn = _build_pipeline(sync_double)
        results_sync, _ = _run_sync(pipeline_sync_fn)

        pipeline_async_fn = _build_pipeline(async_double)
        results_async, _ = _run_sync(pipeline_async_fn)

        assert results_sync == results_async == _EXPECTED


# ===========================================================================
# Combination 3 — Async orchestrator + Sync function
# ===========================================================================


class TestAsyncOrchestratorSyncFunction:
    """Cell (3): async orchestrator + sync function.

    The async orchestrator calls ``node.async_execute()``, which spawns tasks
    via ``asyncio.TaskGroup``.  Each sync function is dispatched through
    ``LocalExecutor.async_execute_callable`` → ``loop.run_in_executor``
    (thread pool), so multiple packets can run concurrently as OS threads.
    """

    def test_correctness(self):
        """Async orchestrator with a sync function produces correct results."""
        pipeline = _build_pipeline(sync_double)
        results, _ = _run_async(pipeline)
        assert results == _EXPECTED

    def test_concurrent_with_io_bound_work(self):
        """Async orch + sync function shows concurrency via thread-pool execution."""
        pipeline = _build_pipeline(slow_sync_double, max_concurrency=5)
        results, elapsed = _run_async(pipeline)
        assert results == _EXPECTED
        # Should complete well under serial time thanks to run_in_executor
        assert elapsed < _CONCURRENT_MAX, (
            f"Expected concurrent execution (< {_CONCURRENT_MAX:.2f}s) "
            f"but took {elapsed:.2f}s"
        )

    def test_matches_sync_orchestrator_results(self):
        """Async orch + sync fn produces the same values as sync orch + sync fn."""
        p_sync = _build_pipeline(sync_double)
        results_sync, _ = _run_sync(p_sync)

        p_async = _build_pipeline(sync_double)
        results_async, _ = _run_async(p_async)

        assert results_sync == results_async == _EXPECTED


# ===========================================================================
# Combination 4 — Async orchestrator + Async function
# ===========================================================================


class TestAsyncOrchestratorAsyncFunction:
    """Cell (4): async orchestrator + async function — maximum concurrency.

    Both the orchestrator (via TaskGroup) and the function itself (native
    coroutines) are async, so multiple packets are awaited concurrently in
    the event loop.  I/O-bound workloads should complete in approximately
    a single ``sleep`` duration rather than N × ``sleep``.
    """

    def test_correctness(self):
        """Async orchestrator with an async function produces correct results."""
        pipeline = _build_pipeline(async_double)
        results, _ = _run_async(pipeline)
        assert results == _EXPECTED

    def test_concurrent_with_io_bound_work(self):
        """Async orch + async fn completes I/O-bound workload concurrently."""
        pipeline = _build_pipeline(slow_async_double, max_concurrency=5)
        results, elapsed = _run_async(pipeline)
        assert results == _EXPECTED
        assert elapsed < _CONCURRENT_MAX, (
            f"Expected concurrent execution (< {_CONCURRENT_MAX:.2f}s) "
            f"but took {elapsed:.2f}s"
        )

    def test_concurrency_limiting_respected(self):
        """max_concurrency=1 forces sequential execution even with async fn."""
        pipeline = _build_pipeline(slow_async_double, max_concurrency=1)
        results, elapsed = _run_async(pipeline)
        assert results == _EXPECTED
        assert elapsed >= _SEQUENTIAL_MIN, (
            f"max_concurrency=1 should force sequential execution "
            f"(>= {_SEQUENTIAL_MIN:.2f}s) but took {elapsed:.2f}s"
        )

    def test_matches_sync_orchestrator_results(self):
        """Async orch + async fn produces the same values as sync orch + sync fn."""
        p_sync = _build_pipeline(sync_double)
        results_sync, _ = _run_sync(p_sync)

        p_async = _build_pipeline(async_double)
        results_async, _ = _run_async(p_async)

        assert results_sync == results_async == _EXPECTED


# ===========================================================================
# Cross-combination: performance comparison
# ===========================================================================


class TestConcurrencyBenefitAcrossMatrix:
    """Verify the relative speed ordering promised by the matrix.

    For an I/O-bound workload (N × asyncio.sleep):
      - Cell (4) async+async must be measurably faster than cell (1) sync+sync.
      - Cell (3) async+sync must also be measurably faster than cell (1)
        (thread-pool concurrency beats true sequentiality).
    """

    def test_async_async_faster_than_sync_sync(self):
        """async+async should complete I/O workload much faster than sync+sync."""
        p_sync = _build_pipeline(slow_sync_double)
        _, elapsed_sync = _run_sync(p_sync)

        p_async = _build_pipeline(slow_async_double, max_concurrency=5)
        _, elapsed_async = _run_async(p_async)

        assert elapsed_async < elapsed_sync * 0.6, (
            f"async+async ({elapsed_async:.2f}s) should be at least 40 % faster "
            f"than sync+sync ({elapsed_sync:.2f}s)"
        )

    def test_async_sync_faster_than_sync_sync(self):
        """async orch + sync fn should outperform sync orch + sync fn for I/O work."""
        p_sync = _build_pipeline(slow_sync_double)
        _, elapsed_sync = _run_sync(p_sync)

        p_async = _build_pipeline(slow_sync_double, max_concurrency=5)
        _, elapsed_async = _run_async(p_async)

        assert elapsed_async < elapsed_sync * 0.6, (
            f"async+sync ({elapsed_async:.2f}s) should be at least 40 % faster "
            f"than sync+sync ({elapsed_sync:.2f}s)"
        )

    def test_unlimited_concurrency_no_deadlock(self):
        """max_concurrency=None (unlimited) should not deadlock."""
        pipeline = _build_pipeline(slow_async_double, max_concurrency=None)
        results, elapsed = _run_async(pipeline)
        assert results == _EXPECTED
        # Should run all packets concurrently with no semaphore overhead
        assert elapsed < _CONCURRENT_MAX, (
            f"Unlimited concurrency should be fast (< {_CONCURRENT_MAX:.2f}s) "
            f"but took {elapsed:.2f}s"
        )


# ===========================================================================
# Regression guard: async fn concurrency under async orchestrator
# ===========================================================================

# With a single-worker thread pool, sync functions are forced to run one at a
# time (only 1 thread). Async functions are unaffected — they don't use threads.
# This creates a clear performance split between the two function types under
# the *same* async orchestrator, directly mirroring the PLT-930 regression:
# if async_execute loses its TaskGroup concurrency, async functions would also
# become sequential and this test would fail.
_SINGLE_N_PACKETS = 5
_SINGLE_SLEEP_S = 0.15
_SINGLE_ASYNC_MAX = _SINGLE_SLEEP_S * 2.0       # async fn: 1 concurrent round
_SINGLE_SYNC_MIN = _SINGLE_N_PACKETS * _SINGLE_SLEEP_S * 0.8  # sync fn: serial


def _blocking_fn(x: int) -> int:
    """Sync fn: for-loop of time.sleep — holds its thread slot the whole time.

    One thread-pool slot occupied per packet for the full duration.  With only
    one worker available, packets are forced to run one-at-a-time.
    """
    for _ in range(5):
        time.sleep(_SINGLE_SLEEP_S / 5)
    return x * 2


async def _cooperative_fn(x: int) -> int:
    """Async fn: asyncio.sleep — suspends cooperatively, no thread needed.

    All N packets can be suspended simultaneously in the event loop regardless
    of thread pool size.
    """
    await asyncio.sleep(_SINGLE_SLEEP_S)
    return x * 2


class TestAsyncOrchestratorFunctionTypeDifference:
    """Async orchestrator: sync fn is sequential (GIL+pool), async fn is concurrent.

    Uses a single-worker thread pool so the sync function cannot run more than
    one packet at a time, while the async function is unaffected.  If
    async_execute ever loses its TaskGroup concurrency (the PLT-930 regression),
    the async function would also serialize and the fast-path assertion fails.
    """

    def test_async_fn_concurrent_sync_fn_sequential_under_single_worker(self):
        """async fn completes in ~1 sleep period; sync fn needs N sequential rounds."""
        expected = [x * 2 for x in range(_SINGLE_N_PACKETS)]

        # async+sync: one thread → packets run one-at-a-time
        p_sync = _build_pipeline(
            _blocking_fn, n=_SINGLE_N_PACKETS, max_concurrency=None
        )
        results_sync, elapsed_sync = asyncio.run(
            _run_async_with_limited_pool(p_sync, pool_workers=1)
        )
        assert results_sync == expected
        assert elapsed_sync >= _SINGLE_SYNC_MIN, (
            f"async+sync with 1 worker should be sequential "
            f"(>= {_SINGLE_SYNC_MIN:.2f}s) but took {elapsed_sync:.2f}s"
        )

        # async+async: asyncio.sleep, all packets concurrent regardless of pool
        p_async = _build_pipeline(
            _cooperative_fn, n=_SINGLE_N_PACKETS, max_concurrency=None
        )
        results_async, elapsed_async = asyncio.run(
            _run_async_with_limited_pool(p_async, pool_workers=1)
        )
        assert results_async == expected
        assert elapsed_async < _SINGLE_ASYNC_MAX, (
            f"async+async should be concurrent (< {_SINGLE_ASYNC_MAX:.2f}s) "
            f"but took {elapsed_async:.2f}s — "
            f"possible regression: async_execute may have lost TaskGroup concurrency"
        )


# ===========================================================================
# Thread-pool saturation: async+async vs async+sync
# ===========================================================================


def _loop_sleep_double(x: int) -> int:
    """Sync function: sleeps via a for-loop of time.sleep calls.

    A plain ``for`` loop of blocking sleeps makes it unambiguously clear this
    function has no way to cooperate with an asyncio event loop.  It occupies
    exactly one thread-pool slot for its entire duration.
    """
    steps = 5
    for _ in range(steps):
        time.sleep(_POOL_SLEEP_S / steps)
    return x * 2


async def _coro_sleep_double(x: int) -> int:
    """Async function: same total sleep, but via a single asyncio.sleep.

    Suspends the coroutine cooperatively so the event loop can run other
    coroutines during the wait — no thread needed.
    """
    await asyncio.sleep(_POOL_SLEEP_S)
    return x * 2


async def _run_async_with_limited_pool(
    pipeline: Pipeline, pool_workers: int
) -> tuple[list[int], float]:
    """Run pipeline async on an event loop whose thread pool is capped.

    Temporarily installs a ``ThreadPoolExecutor`` with *pool_workers* threads
    as the running loop's default executor, ensuring the sync path cannot
    exceed that concurrency even if the machine has many CPUs.
    """
    loop = asyncio.get_running_loop()
    limited_pool = ThreadPoolExecutor(max_workers=pool_workers)
    loop.set_default_executor(limited_pool)
    # asyncio.run() creates a fresh loop each call, so no need to restore.
    t0 = time.perf_counter()
    await AsyncPipelineOrchestrator().run_async(pipeline._node_graph)
    elapsed = time.perf_counter() - t0
    limited_pool.shutdown(wait=False)

    pipeline.flush()
    records = pipeline.node.get_all_records()
    assert records is not None
    return sorted(records.column("result").to_pylist()), elapsed


class TestAsyncAsyncVsAsyncSync:
    """Verify that async+async outperforms async+sync when the thread pool
    is smaller than the number of concurrent packets.

    The sync function sleeps via a ``for`` loop of ``time.sleep`` calls —
    unambiguously blocking with no event-loop cooperation.  With only
    ``_POOL_WORKERS`` thread slots available and ``_POOL_N_PACKETS`` packets,
    the async+sync path must serialise into multiple rounds.  The async+async
    path runs all coroutines concurrently regardless of the thread pool size.
    """

    def test_async_async_faster_than_async_sync_under_pool_saturation(self):
        """async+async finishes in ~1 sleep period; async+sync needs multiple rounds."""
        expected = [x * 2 for x in range(_POOL_N_PACKETS)]

        # async+sync: for-loop sleep, thread pool capped at _POOL_WORKERS
        p_sync = _build_pipeline(
            _loop_sleep_double, n=_POOL_N_PACKETS, max_concurrency=None
        )
        results_sync, elapsed_sync = asyncio.run(
            _run_async_with_limited_pool(p_sync, pool_workers=_POOL_WORKERS)
        )
        assert results_sync == expected

        # async+async: asyncio.sleep, all coroutines run concurrently
        p_async = _build_pipeline(
            _coro_sleep_double, n=_POOL_N_PACKETS, max_concurrency=None
        )
        results_async, elapsed_async = asyncio.run(
            _run_async_with_limited_pool(p_async, pool_workers=_POOL_WORKERS)
        )
        assert results_async == expected

        # async+async should complete in ~1 sleep period (all concurrent)
        assert elapsed_async < _POOL_ASYNC_MAX, (
            f"async+async ({elapsed_async:.2f}s) should complete in ~1 sleep "
            f"period (< {_POOL_ASYNC_MAX:.2f}s)"
        )

        # async+sync must take at least 2.5 sleep periods (pool saturated)
        assert elapsed_sync >= _POOL_SYNC_MIN, (
            f"async+sync ({elapsed_sync:.2f}s) should be throttled by the "
            f"thread pool to >= {_POOL_SYNC_MIN:.2f}s"
        )

        # and async+async must be clearly faster
        assert elapsed_async < elapsed_sync * 0.6, (
            f"async+async ({elapsed_async:.2f}s) should be >40% faster than "
            f"async+sync ({elapsed_sync:.2f}s) under pool saturation"
        )
