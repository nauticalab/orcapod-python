"""Comment 4: function -> side-effect job; run, clear only the side-effect node, run again."""

from typing import Any

from orcapod import PipelineJob, function_pod
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator
from orcapod.side_effects import tap_pod
from orcapod.sources import DictSource

DELIVERED: list[int] = []


@function_pod(output_keys="y")
def inc(x: int) -> int:
    return x + 1


def deliver(y: int, ctx: Any) -> None:
    DELIVERED.append(y)


for name, orch in [
    ("sync", SyncPipelineOrchestrator),
    ("async", AsyncPipelineOrchestrator),
]:
    for via in ("job.run", "node.run"):
        DELIVERED.clear()
        job = PipelineJob(name="j", database=InMemoryArrowDatabase())
        with job:
            rows = inc.pod(
                DictSource(
                    [{"k": i, "x": i} for i in range(3)],
                    tag_columns=["k"],
                    source_id="s",
                ),
                label="rowwise",
            )
            tap: Any = tap_pod(deliver)
            tap.process(rows, label="tap")
        job.run(orchestrator=orch())
        first = len(DELIVERED)
        job.nodes["tap"].clear_cache()
        DELIVERED.clear()
        if via == "job.run":
            summary = job.run(orchestrator=orch())
        else:
            if name == "async":
                continue  # node.run() is sync only
            job.nodes["tap"].run()
            summary = job.nodes["tap"].last_run
        print(
            f"--- {name} {via}: first run delivered {first}; second run delivered {len(DELIVERED)}"
        )
        print(repr(summary))

# The same with an operator node below the held function node instead of a side effect.
for name, orch in [
    ("sync", SyncPipelineOrchestrator),
    ("async", AsyncPipelineOrchestrator),
]:
    job = PipelineJob(name="j", database=InMemoryArrowDatabase())
    with job:
        rows = inc.pod(
            DictSource(
                [{"k": i, "x": i} for i in range(3)], tag_columns=["k"], source_id="s"
            ),
            label="rowwise",
        )
        rows_any: Any = rows
        rows_any.select_data_columns(["y"], label="op")
    job.run(orchestrator=orch())
    job.nodes["op"].clear_cache()
    summary = job.run(orchestrator=orch())
    print(
        f"--- {name} job.run, operator node cleared: op holds_current_run={job.nodes['op'].holds_current_run(load_only=False)}"
    )
    print(repr(summary))
