"""Save a pipeline job to JSON, then load it back to read its stored results.

Run from any directory: the database and the saved job are written to the
current working directory.
"""

from orcapod import PipelineJob, function_pod
from orcapod.databases import DeltaTableDatabase
from orcapod.sources import DictSource

database = DeltaTableDatabase("./local_database")

source1 = DictSource(
    [{"id": 0, "x": 5}, {"id": 1, "x": 10}, {"id": 2, "x": 15}],
    tag_columns=["id"],
    source_id="source1",
)
source2 = DictSource(
    [{"id": 0, "y": 3}, {"id": 2, "y": 6}, {"id": 4, "y": 9}],
    tag_columns=["id"],
    source_id="source2",
)


@function_pod(output_keys="sum")
def take_sum(x: int, y: int) -> int:
    return x + y


# Record the pipeline: nothing is computed inside the ``with`` block.
job = PipelineJob(name="my_pipeline", store=database)
with job:
    take_sum.pod(source1.join(source2), label="sum")

job.run()
print(job.nodes["sum"].as_table().to_pandas())

# Save the job (graph, store location and source configuration) to JSON ...
path = "./pipeline.json"
job.save(path)

# ... and load it back, for example in a later session, to read the stored
# results. A loaded job is for reading: it cannot be run again, because function
# pods are not saved with it. Its function nodes serve the rows the run stored.
# In-memory sources such as DictSource are not saved either (loading logs this at
# INFO level); that does not affect reading the other nodes.
job2 = PipelineJob.load(path)
print(job2.nodes["sum"].as_table().sort_by("id").to_pandas())
