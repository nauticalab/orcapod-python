"""Observation (not changed): node.run() of a node that holds a current run returns at once
and makes no summary, so node.last_run stays the summary of the run before it."""

from orcapod import PipelineJob, function_pod
from orcapod.databases import InMemoryArrowDatabase
from orcapod.sources import DictSource


@function_pod(output_keys="y")
def inc(x: int) -> int:
    return x + 1


job = PipelineJob(name="j", database=InMemoryArrowDatabase())
with job:
    inc.pod(
        DictSource([{"k": 1, "x": 1}], tag_columns=["k"], source_id="s"),
        label="rowwise",
    )
node = job.nodes["rowwise"]
node.run()
first = node.last_run
node.run()  # held: runs nothing
print(f"second run() left last_run unchanged: {node.last_run is first}")
print(repr(node.last_run).splitlines()[0])
