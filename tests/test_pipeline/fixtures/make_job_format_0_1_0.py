"""Produces ``job_format_0_1_0.json``: a job saved in format 0.1.0.

Not a test and not run in CI. It was run against the source of origin/main at
commit cdb4f160, which predates the ``orcapod_version`` stamp and the 0.2.0 bump:

    git archive cdb4f160 src | tar -x -C <dir>
    PYTHONPATH=<dir>/src uv run python make_job_format_0_1_0.py job_format_0_1_0.json

Running it on a later checkout writes a newer format; do not regenerate the fixture.

Its hashes were re-recorded once for the v1 hash definition (ITL-1154): this script
was run on origin/main (it reproduced every hash in the file) and on the ITL-1154
branch, and each hash in the file was replaced by the branch's value for the same
node, uri or source. The format, keys and layout of the file are unchanged.
"""

import sys
import pyarrow as pa
import orcapod
from orcapod.core.data_function import PythonDataFunction
from orcapod.core.function_pod import FunctionPod
from orcapod.core.operators import Join
from orcapod.core.sources import ArrowTableSource
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline.job import PipelineJob


def src(tag, col, d):
    t = pa.table(
        {
            tag: pa.array(d[tag], type=pa.large_string()),
            col: pa.array(d[col], type=pa.int64()),
        }
    )
    return ArrowTableSource(
        t, tag_columns=[tag], infer_nullable=True, source_id=f"fixture_{col}"
    )


def add_values(value: int, score: int) -> int:
    return value + score


a = src("key", "value", {"key": ["a", "b"], "value": [10, 20]})
b = src("key", "score", {"key": ["a", "b"], "score": [100, 200]})
job = PipelineJob(database=InMemoryArrowDatabase())
with job:
    pf = PythonDataFunction(add_values, output_keys="total")
    pod = FunctionPod(data_function=pf)  # pyright: ignore[reportArgumentType] -- protocol mismatch shared with other tests
    pod(Join()(a, b))
job.save(sys.argv[1])
print(orcapod.__file__)
