"""Async vs sync pipeline execution — 2x2 comparison matrix.

Demonstrates the interplay of two independent concurrency axes:

1. **Pipeline executor** — sync (sequential node execution) vs async
   (concurrent node execution via channels).

2. **Packet function** — sync (GIL-holding busy-wait) vs async
   (non-blocking ``asyncio.sleep``).

The sync function uses a pure-Python busy-wait loop that holds the GIL,
preventing thread-pool concurrency. This makes the difference between
async+sync and async+async clearly visible:

+---------------------+----------------------------+----------------------------+
|                     | sync function (holds GIL)  | async function             |
+---------------------+----------------------------+----------------------------+
| sync executor       | fully sequential           | sequential (async fn       |
|                     |                            | called via sync fallback)  |
+---------------------+----------------------------+----------------------------+
| async executor      | branches overlap, but      | branches overlap AND       |
|                     | packets serialize on GIL   | packets run concurrently   |
|                     | (thread pool can't help)   | (native coroutines)        |
+---------------------+----------------------------+----------------------------+

Usage:
    uv run python examples/async_vs_sync_pipeline.py
"""

from __future__ import annotations

import asyncio
import time

import pyarrow as pa

from orcapod import ArrowTableSource
from orcapod.core.function_pod import FunctionPod
from orcapod.core.packet_function import PythonPacketFunction
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline import Pipeline
from orcapod.types import ExecutorType, NodeConfig, PipelineConfig

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SLEEP_SECONDS = 0.1  # per-packet delay (kept short since busy-wait burns CPU)
NUM_PACKETS = 6

# ---------------------------------------------------------------------------
# Source data
# ---------------------------------------------------------------------------

SOURCE_TABLE = pa.table(
    {
        "id": pa.array(list(range(NUM_PACKETS)), type=pa.int64()),
        "x": pa.array(list(range(NUM_PACKETS)), type=pa.int64()),
    }
)

# ---------------------------------------------------------------------------
# Domain functions — async and sync variants
# ---------------------------------------------------------------------------


def _busy_wait(seconds: float) -> None:
    """Burn CPU in a pure-Python loop that never releases the GIL."""
    end = time.perf_counter() + seconds
    while time.perf_counter() < end:
        pass


async def async_slow_double(x: int) -> int:
    """Simulate an async I/O-bound operation (e.g. API call)."""
    await asyncio.sleep(SLEEP_SECONDS)
    return x * 2


def sync_slow_double(x: int) -> int:
    """Simulate a GIL-holding blocking operation (e.g. CPU-bound work)."""
    _busy_wait(SLEEP_SECONDS)
    return x * 2


# ---------------------------------------------------------------------------
# Pipeline builder
# ---------------------------------------------------------------------------


def build_pipeline(use_async_fn: bool) -> Pipeline:
    """Build a pipeline with two independent branches from the same source.

    Pipeline::

        source ──┬── slow_double (branch_a)
                 └── slow_double (branch_b)
    """
    db = InMemoryArrowDatabase()
    pipeline = Pipeline(name="demo", pipeline_database=db)

    fn = async_slow_double if use_async_fn else sync_slow_double
    with pipeline:
        source = ArrowTableSource(SOURCE_TABLE, tag_columns=["id"])
        pf_a = PythonPacketFunction(fn, output_keys="result", function_name="branch_a")
        pf_b = PythonPacketFunction(fn, output_keys="result", function_name="branch_b")
        FunctionPod(
            packet_function=pf_a,
            node_config=NodeConfig(max_concurrency=NUM_PACKETS),
        )(source, label="branch_a")
        FunctionPod(
            packet_function=pf_b,
            node_config=NodeConfig(max_concurrency=NUM_PACKETS),
        )(source, label="branch_b")

    return pipeline


def run_case(label: str, use_async_fn: bool, use_async_executor: bool) -> float:
    """Run a single combination and return elapsed time."""
    pipeline = build_pipeline(use_async_fn=use_async_fn)
    t0 = time.perf_counter()
    if use_async_executor:
        pipeline.run(config=PipelineConfig(executor=ExecutorType.ASYNC_CHANNELS))
    else:
        pipeline.run()
    elapsed = time.perf_counter() - t0

    a = pipeline.branch_a.get_all_records()
    b = pipeline.branch_b.get_all_records()
    assert a is not None and b is not None
    total_rows = a.num_rows + b.num_rows
    print(f"  {label:44s} {elapsed:5.2f}s  ({total_rows} rows)")
    return elapsed


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    total = NUM_PACKETS * 2  # packets per branch * 2 branches
    seq_time = SLEEP_SECONDS * total

    print("=" * 64)
    print("Pipeline execution: 2x2 comparison matrix")
    print("=" * 64)
    print(f"  {NUM_PACKETS} packets x 2 branches, {SLEEP_SECONDS}s sleep each")
    print(f"  Sequential baseline: {seq_time:.1f}s\n")

    print("  Pipeline topology:")
    print("    source ──┬── slow_double (branch_a)")
    print("             └── slow_double (branch_b)\n")

    t1 = run_case("sync executor  + sync function :", False, False)
    t2 = run_case("sync executor  + async function:", True, False)
    t3 = run_case("async executor + sync function :", False, True)
    t4 = run_case("async executor + async function:", True, True)

    print()
    print("  Analysis:")
    print(f"    sync+sync   {t1:.2f}s  — fully sequential ({NUM_PACKETS}x2 x {SLEEP_SECONDS}s)")
    print(f"    sync+async  {t2:.2f}s  — still sequential (sync executor runs nodes one by one)")
    print(f"    async+sync  {t3:.2f}s  — branches overlap, but GIL-holding busy-wait")
    print(f"                          serializes packets even across threads")
    print(f"    async+async {t4:.2f}s  — branches overlap AND packets overlap")
    print(f"                          (native coroutines yield at await points, enabling I/O overlap)")
    print()
    print(f"  Key insight: async+sync is much slower than async+async because")
    print(f"  the sync function holds the GIL, so run_in_executor threads")
    print(f"  cannot actually run in parallel. Native async coroutines yield")
    print(f"  control at each 'await', enabling cooperative I/O overlap.")
    print()
    print(f"  Speedup (sync+sync  vs async+async): {t1 / t4:.1f}x")
    print(f"  Speedup (async+sync vs async+async): {t3 / t4:.1f}x")


if __name__ == "__main__":
    main()
