"""Comment 1: a function job node is stamped after on_node_end, so an observer that reads
the node in on_node_end finds it stale and clears it."""

from typing import Any

from orcapod import PipelineJob, function_pod
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator
from orcapod.pipeline.observer import NoOpObserver
from orcapod.sources import DictSource


@function_pod(output_keys="y")
def inc(x: int) -> int:
    return x + 1


class Reader(NoOpObserver):
    def __init__(self, job: Any) -> None:
        self.job = job
        self.seen: dict[str, int] = {}

    def contextualize(self, *identity_path: str) -> "Reader":
        return self

    def on_node_end(self, node_label: str, node_hash: str) -> None:
        node = self.job.nodes.get(node_label)
        if node is not None:
            self.seen[node_label] = node.as_table().num_rows


def build(db: Any) -> PipelineJob:
    job = PipelineJob(name="j", database=db)
    with job:
        inc.pod(
            DictSource(
                [{"k": i, "x": i} for i in range(3)], tag_columns=["k"], source_id="s"
            ),
            label="rowwise",
        )
    return job


for name, orch in [
    ("sync", SyncPipelineOrchestrator),
    ("async", AsyncPipelineOrchestrator),
]:
    for load_only in (False, True):
        db = InMemoryArrowDatabase()
        if load_only:
            build(db).run()  # store the results first
        job = build(db)
        obs = Reader(job)
        job.run(orchestrator=orch(), observer=obs, load_only=load_only)
        node = job.nodes["rowwise"]
        print(
            f"{name:5} load_only={load_only!s:5}: read in on_node_end={obs.seen.get('rowwise')}, "
            f"after run: rows={node.as_table().num_rows} holds_current_run={node.holds_current_run(load_only=load_only)}"
        )
