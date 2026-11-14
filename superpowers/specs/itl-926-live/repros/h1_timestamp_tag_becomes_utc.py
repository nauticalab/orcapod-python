"""H1 repro: ``FunctionJobNode.as_table()`` turns a naive ``timestamp[us]`` tag into UTC.

Run from the repository root:

    uv run python superpowers/specs/itl-926-live/repros/h1_timestamp_tag_becomes_utc.py

No SpiralDB needed. Expected: the tag stays ``timestamp[us]`` on every read path. Observed on
main (ebf2511b): ``as_table()`` gives ``timestamp[us, tz=UTC]``; ``get_all_records()`` gives
``timestamp[us]``. The EDI sample keys include ``session_start_datetime`` (timestamp, µs).
"""

import datetime as dt
import tempfile

import pyarrow as pa

from orcapod import PipelineJob, function_pod
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.sources import ArrowTableSource

schema = pa.schema(
    [pa.field("start", pa.timestamp("us"), False), pa.field("x", pa.int64(), False)]
)
data = pa.table({"start": [dt.datetime(2026, 1, 1, 9)], "x": [1]}, schema=schema)


@function_pod(output_keys="y")
def inc(x: int) -> int:
    """Add one."""
    return x + 1


for store in (InMemoryArrowDatabase(), DeltaTableDatabase(tempfile.mkdtemp())):
    job = PipelineJob(name="h1", database=store)
    with job:
        inc.pod(
            ArrowTableSource(data, tag_columns=["start"], source_id="s"), label="inc"
        )
    job.run()
    node = job.nodes["inc"]
    print(
        f"{type(store).__name__:22} as_table: {node.as_table().schema.field('start').type}"
        f" | get_all_records: {node.get_all_records().schema.field('start').type}"
    )
