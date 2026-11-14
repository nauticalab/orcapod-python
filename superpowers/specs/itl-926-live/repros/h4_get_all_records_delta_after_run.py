"""H4 repro: with ``DeltaTableDatabase``, ``get_all_records()`` misses rows right after ``run()``.

Run from the repository root:

    uv run python superpowers/specs/itl-926-live/repros/h4_get_all_records_delta_after_run.py

No SpiralDB needed. Expected: ``get_all_records()`` returns the same 2 rows as ``as_table()``
for both stores. Observed on main (ebf2511b): ``InMemoryArrowDatabase`` returns 2 rows;
``DeltaTableDatabase`` returns fewer (1, and in one run ``None``). A later job over the same
store reads every row back, so the rows look unflushed when the same job reads them. Seen
first in the Spiral workshop path after a row was inserted.
"""

import tempfile

import pyarrow as pa

from orcapod import PipelineJob, function_pod
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.sources import ArrowTableSource


@function_pod(output_keys="y")
def double(x: int) -> int:
    """Double the value."""
    return 2 * x


schema = pa.schema(
    [pa.field("k", pa.large_string(), False), pa.field("x", pa.int64(), False)]
)
data = pa.table({"k": ["b", "c"], "x": [2, 3]}, schema=schema)

for make_store in (
    InMemoryArrowDatabase,
    lambda: DeltaTableDatabase(tempfile.mkdtemp()),
):
    for source_id in ("s", None):
        store = make_store()
        job = PipelineJob(name="h4", database=store)
        with job:
            double.pod(
                ArrowTableSource(data, tag_columns=["k"], source_id=source_id),
                label="double",
            )
        job.run()
        records = job.nodes["double"].get_all_records()
        print(
            f"{type(store).__name__:22} source_id={source_id!s:5} "
            f"as_table={job.nodes['double'].as_table().num_rows} "
            f"get_all_records={None if records is None else records.num_rows}"
        )
