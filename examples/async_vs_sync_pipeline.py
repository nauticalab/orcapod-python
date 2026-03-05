"""Async vs sync pipeline execution comparison.

Demonstrates two levels of async benefit:

1. **Pipeline-level parallelism** (Pipeline API):
   Multiple independent branches execute concurrently under the async
   orchestrator, overlapping I/O across branches.

2. **Packet-level concurrency** (FunctionPod + channels):
   An async packet function processes all packets concurrently within a
   single stage, achieving near-constant latency regardless of packet count.

Usage:
    uv run python examples/async_vs_sync_pipeline.py
"""

from __future__ import annotations

import asyncio
import time

import pyarrow as pa

from orcapod import ArrowTableSource
from orcapod.channels import Channel
from orcapod.core.function_pod import FunctionPod
from orcapod.core.packet_function import PythonPacketFunction
from orcapod.core.streams.arrow_table_stream import ArrowTableStream
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


# ═══════════════════════════════════════════════════════════════════════════
# Part 1: Pipeline API — pipeline-level parallelism
# ═══════════════════════════════════════════════════════════════════════════


def build_pipeline_with_branches(use_async_fn: bool) -> Pipeline:
    """Build a pipeline with two independent branches from the same source.

    Pipeline::

        source ──┬── slow_double (branch_a)
                 └── slow_double (branch_b)

    The async orchestrator runs both branches concurrently; the sync
    executor runs them sequentially.
    """
    db = InMemoryArrowDatabase()
    pipeline = Pipeline(name="branch_demo", pipeline_database=db)

    fn = async_slow_double if use_async_fn else sync_slow_double
    with pipeline:
        source = ArrowTableSource(SOURCE_TABLE, tag_columns=["id"])
        pf_a = PythonPacketFunction(fn, output_keys="result", function_name="branch_a")
        pf_b = PythonPacketFunction(fn, output_keys="result", function_name="branch_b")
        FunctionPod(packet_function=pf_a)(source, label="branch_a")
        FunctionPod(packet_function=pf_b)(source, label="branch_b")

    return pipeline


def run_pipeline_demo() -> None:
    """Compare sync vs async pipeline execution on a two-branch pipeline."""
    print("=" * 60)
    print("Part 1: Pipeline API — pipeline-level parallelism")
    print("=" * 60)
    print(f"  {NUM_PACKETS} packets x 2 branches, {SLEEP_SECONDS}s sleep each")
    print(f"  Sync runs branches sequentially, async overlaps them\n")

    # -- Sync: pipeline.run() --
    sync_pipeline = build_pipeline_with_branches(use_async_fn=False)
    t0 = time.perf_counter()
    sync_pipeline.run()
    sync_time = time.perf_counter() - t0

    a = sync_pipeline.branch_a.get_all_records()
    b = sync_pipeline.branch_b.get_all_records()
    assert a is not None and b is not None
    print(f"  pipeline.run()              : {sync_time:.2f}s  ({a.num_rows + b.num_rows} rows)")

    # -- Async: pipeline.run(ASYNC_CHANNELS) --
    async_pipeline = build_pipeline_with_branches(use_async_fn=True)
    t0 = time.perf_counter()
    async_pipeline.run(config=PipelineConfig(executor=ExecutorType.ASYNC_CHANNELS))
    async_time = time.perf_counter() - t0

    a = async_pipeline.branch_a.get_all_records()
    b = async_pipeline.branch_b.get_all_records()
    assert a is not None and b is not None
    print(f"  pipeline.run(ASYNC_CHANNELS): {async_time:.2f}s  ({a.num_rows + b.num_rows} rows)")
    print(f"  Speedup: {sync_time / async_time:.1f}x")


# ═══════════════════════════════════════════════════════════════════════════
# Part 2: FunctionPod + channels — packet-level concurrency
# ═══════════════════════════════════════════════════════════════════════════


def run_sync_function_pod() -> list[dict]:
    """Process packets sequentially using sync FunctionPod."""
    pf = PythonPacketFunction(sync_slow_double, output_keys="result")
    pod = FunctionPod(packet_function=pf)
    stream = ArrowTableStream(SOURCE_TABLE, tag_columns=["id"])
    output = pod.process(stream)
    return [pkt.as_dict() for _, pkt in output.iter_packets()]


async def run_async_function_pod() -> list[dict]:
    """Process all packets concurrently using async FunctionPod + channels."""
    pf = PythonPacketFunction(async_slow_double, output_keys="result")
    pod = FunctionPod(
        packet_function=pf,
        node_config=NodeConfig(max_concurrency=NUM_PACKETS),
    )
    stream = ArrowTableStream(SOURCE_TABLE, tag_columns=["id"])

    buf = NUM_PACKETS + 2
    input_ch = Channel(buffer_size=buf)
    output_ch = Channel(buffer_size=buf)

    async def feed():
        for tag, packet in stream.iter_packets():
            await input_ch.writer.send((tag, packet))
        await input_ch.writer.close()

    async with asyncio.TaskGroup() as tg:
        tg.create_task(feed())
        tg.create_task(pod.async_execute([input_ch.reader], output_ch.writer))

    items = await output_ch.reader.collect()
    return [pkt.as_dict() for _, pkt in items]


def run_function_pod_demo() -> None:
    """Compare sync vs async FunctionPod on single-stage packet processing."""
    print("=" * 60)
    print("Part 2: FunctionPod — packet-level concurrency")
    print("=" * 60)
    print(f"  {NUM_PACKETS} packets, {SLEEP_SECONDS}s sleep each")
    print(f"  Sync processes one at a time, async overlaps all\n")

    # Sync
    t0 = time.perf_counter()
    sync_results = run_sync_function_pod()
    sync_time = time.perf_counter() - t0
    print(f"  Sync  FunctionPod: {sync_time:.2f}s  results={[r['result'] for r in sync_results]}")

    # Async
    t0 = time.perf_counter()
    async_results = asyncio.run(run_async_function_pod())
    async_time = time.perf_counter() - t0
    print(f"  Async FunctionPod: {async_time:.2f}s  results={sorted(r['result'] for r in async_results)}")
    print(f"  Speedup: {sync_time / async_time:.1f}x")

    # Verify correctness
    assert sorted(r["result"] for r in sync_results) == sorted(
        r["result"] for r in async_results
    )
    print("  Correctness verified.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    run_pipeline_demo()
    print()
    run_function_pod_demo()


if __name__ == "__main__":
    main()
