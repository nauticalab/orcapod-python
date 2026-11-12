"""Claim 2: the orcapod call chain from the function's return value to the stored value.

For every execution path, run a pod declared ``-> int`` that returns ``1.5`` with the
tracer installed, and print (a) each pyarrow call that received a non-int value for an
integer field, with the orcapod frames that led to it, and (b) what ends up stored /
yielded.

Run: uv run python superpowers/specs/itl-895-float-to-int/02_call_chain.py
"""

from __future__ import annotations

import importlib

import asyncio
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

_tracer = importlib.import_module("_tracer")  # the sibling helper, via sys.path

from orcapod import PipelineJob, function_pod  # noqa: E402
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase  # noqa: E402
from orcapod.pipeline import AsyncPipelineOrchestrator  # noqa: E402
from orcapod.sources import DictSource  # noqa: E402

_tracer.install()


def make_src() -> DictSource:
    return DictSource(data=[{"k": "a", "x": 1}], tag_columns=["k"], source_id="src")


def banner(title: str) -> None:
    print(f"\n=== {title} ===")
    _tracer.HITS.clear()


def show_iter(stream) -> None:
    for _tag, data in stream.iter_data():
        print(
            f"  iter_data: as_dict()={data.as_dict()}  as_table()={data.as_table().to_pylist()}"
            f"  arrow type={data.arrow_schema().field('y').type}"
        )


# --- A. standalone FunctionPod stream, sync function --------------------------------
@function_pod(output_keys="y")
def f_sync(x: int) -> int:
    return x + 0.5  # pyright: ignore[reportReturnType]  # deliberate wrong return type: reproduces ITL-895


banner("A1 standalone FunctionPod, iter_data()")
show_iter(f_sync.pod(make_src()))

banner("A2 standalone FunctionPod, as_table()")
t = f_sync.pod(make_src()).as_table()
print(f"  as_table: y={t['y'].to_pylist()} type={t.schema.field('y').type}")


# --- B. standalone FunctionPod stream, async function -------------------------------
@function_pod(output_keys="y")
async def f_async(x: int) -> int:
    return x + 0.5  # pyright: ignore[reportReturnType]  # deliberate wrong return type: reproduces ITL-895


banner("B1 standalone FunctionPod with an async function, iter_data()")
show_iter(f_async.pod(make_src()))

banner("B2 standalone FunctionPod with an async function, as_table()")
t = f_async.pod(make_src()).as_table()
print(f"  as_table: y={t['y'].to_pylist()} type={t.schema.field('y').type}")

# --- C. cached function pod: result_database (CachedDataFunction) -------------------
db_c = InMemoryArrowDatabase()


@function_pod(output_keys="y", result_database=db_c)
def f_cached(x: int) -> int:
    return x + 0.5  # pyright: ignore[reportReturnType]  # deliberate wrong return type: reproduces ITL-895


banner("C1 function_pod(result_database=...) -> CachedDataFunction, as_table()")
t = f_cached.pod(make_src()).as_table()
print(f"  as_table: y={t['y'].to_pylist()} type={t.schema.field('y').type}")
banner("C2 CachedDataFunction, second run (served from the store)")
show_iter(f_cached.pod(make_src()))

# --- D. cached function pod: pod_cache_database (CachedFunctionPod) -----------------
db_d = InMemoryArrowDatabase()


@function_pod(output_keys="y", pod_cache_database=db_d)
def f_podcached(x: int) -> int:
    return x + 0.5  # pyright: ignore[reportReturnType]  # deliberate wrong return type: reproduces ITL-895


banner("D1 function_pod(pod_cache_database=...) -> CachedFunctionPod, as_table()")
t = f_podcached.pod(make_src()).as_table()
print(f"  as_table: y={t['y'].to_pylist()} type={t.schema.field('y').type}")


# --- E. FunctionNode in a PipelineJob -----------------------------------------------
def run_job(store, orchestrator=None, fn=f_sync) -> None:
    job = PipelineJob(name="itl895", store=store)
    with job:
        fn.pod(make_src(), label="f")
    job.run(orchestrator=orchestrator)
    node = job.nodes["f"]
    t = node.as_table()
    print(f"  node.as_table: y={t['y'].to_pylist()} type={t.schema.field('y').type}")
    recs = node.get_all_records()
    if recs is not None:
        print(
            f"  stored record y={recs['y'].to_pylist()} type={recs.schema.field('y').type}"
        )


banner("E1 PipelineJob sync, InMemoryArrowDatabase")
run_job(InMemoryArrowDatabase())

with tempfile.TemporaryDirectory() as d:
    banner("E2 PipelineJob sync, DeltaTableDatabase")
    run_job(DeltaTableDatabase(Path(d) / "sync"))

banner("E3 PipelineJob async orchestrator, InMemoryArrowDatabase")
run_job(InMemoryArrowDatabase(), AsyncPipelineOrchestrator())

with tempfile.TemporaryDirectory() as d:
    banner("E4 PipelineJob async orchestrator, DeltaTableDatabase")
    run_job(DeltaTableDatabase(Path(d) / "async"), AsyncPipelineOrchestrator())

banner("E5 PipelineJob async orchestrator, async function, InMemoryArrowDatabase")
run_job(InMemoryArrowDatabase(), AsyncPipelineOrchestrator(), fn=f_async)

_ = asyncio  # imported for readers running parts interactively
