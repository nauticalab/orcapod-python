"""Async vs sync pipeline execution — 2x2 comparison matrix.

Demonstrates the interplay of two independent concurrency axes:

1. **Pipeline executor** — sync (sequential node execution) vs async
   (concurrent node execution via channels).

2. **Packet function** — sync (blocking ``time.sleep``) vs async
   (non-blocking ``asyncio.sleep``).

The four combinations are:

+---------------------+----------------------------+----------------------------+
|                     | sync function              | async function             |
+---------------------+----------------------------+----------------------------+
| sync executor       | fully sequential           | sequential (async fn       |
|                     |                            | called in thread pool)     |
+---------------------+----------------------------+----------------------------+
| async executor      | branches overlap, but      | branches overlap AND       |
|                     | packets within each branch | packets within each branch |
|                     | are sequential (blocking)  | run concurrently           |
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

SLEEP_SECONDS = 0.3  # per-packet sleep duration
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


async def async_slow_double(x: int) -> int:
    """Simulate an async I/O-bound operation (e.g. API call)."""
    await asyncio.sleep(SLEEP_SECONDS)
    return x * 2


def sync_slow_double(x: int) -> int:
    """Simulate a blocking I/O-bound operation."""
    time.sleep(SLEEP_SECONDS)
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
    print(f"    async+sync  {t3:.2f}s  — branches overlap; sync fn runs in thread pool,")
    print(f"                          so packets also overlap via threads")
    print(f"    async+async {t4:.2f}s  — branches overlap; packets overlap via native await")
    print()
    print(f"  Note: async+sync ≈ async+async for I/O-bound work because")
    print(f"  run_in_executor (thread pool) overlaps blocking I/O similarly.")
    print(f"  Native async wins at scale (coroutines are lighter than threads).")
    print()
    print(f"  Speedup (sync+sync vs async+async): {t1 / t4:.1f}x")


if __name__ == "__main__":
    main()
