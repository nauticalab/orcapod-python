"""Build, run and save a PipelineJob covering every source kind (ITL-904).

Run as a script in a separate process, ``python save_job_script.py <directory>``, so
that the test loading the job starts from a fresh process: nothing the save created
(function pods, sources, the database object) is reachable from the loading process.

Writes into ``<directory>``:

* ``c.csv`` and ``dt/`` -- the CSV and Delta inputs;
* ``store/`` -- the Delta database the job ran against;
* ``job.json`` -- the saved job;
* ``expected/<label>.parquet`` and ``expected/<label>.all_info.parquet`` -- each function
  node's ``as_table()`` read in the saving process right after the run, the reference the
  loaded job must reproduce.
"""

from __future__ import annotations

import pathlib
import sys

import pyarrow as pa
import pyarrow.parquet as pq
from deltalake import write_deltalake

from orcapod import PipelineJob, function_pod
from orcapod.core.operators import Join
from orcapod.databases import DeltaTableDatabase
from orcapod.sources import (
    ArrowTableSource,
    CSVSource,
    DeltaTableSource,
    DictSource,
    ListSource,
)

FUNCTION_NODES = ("dbl", "inc", "half", "sq")


@function_pod(output_keys="dbl")
def double(x: int) -> int:
    """Double *x*."""
    return 2 * x


@function_pod(output_keys="cc")
def increment(c: int | None) -> int:
    """Add one to *c*, reading a missing value as zero."""
    return (c or 0) + 1


@function_pod(output_keys="dd")
def halve(d: float | None) -> float:
    """Halve *d*, reading a missing value as zero."""
    return (d or 0.0) / 2


@function_pod(output_keys="zz")
def square(z: int) -> int:
    """Square *z*."""
    return z * z


def main(root: pathlib.Path) -> None:
    """Build, run and save the job, writing everything into *root*."""
    root.mkdir(parents=True, exist_ok=True)
    _ = (root / "c.csv").write_text("id,c\n0,7\n1,8\n")
    write_deltalake(
        str(root / "dt"),
        pa.table({"id": pa.array([0, 1], pa.int64()), "d": pa.array([1.5, 2.5])}),
        mode="overwrite",
    )

    dict_source = DictSource(
        [{"id": 0, "x": 5}, {"id": 1, "x": 10}], tag_columns=["id"], source_id="dsrc"
    )
    arrow_source = ArrowTableSource(
        pa.table(
            {"id": [0, 1], "y": [3, 4]},
            schema=pa.schema(
                [
                    pa.field("id", pa.int64(), nullable=False),
                    pa.field("y", pa.int64(), nullable=False),
                ]
            ),
        ),
        tag_columns=["id"],
        source_id="asrc",
    )
    csv_source = CSVSource(str(root / "c.csv"), tag_columns=["id"], source_id="csrc")
    delta_source = DeltaTableSource(
        str(root / "dt"), tag_columns=["id"], source_id="tsrc"
    )
    list_source = ListSource("z", [1, 2], source_id="lsrc")

    job = PipelineJob(
        name="loaded_demo", database=DeltaTableDatabase(str(root / "store"))
    )
    with job:
        # A pod whose output feeds an operator: the shape that used to make
        # PipelineJob.load crash.
        _ = Join()(double.pod(dict_source, label="dbl"), arrow_source, label="joined")
        _ = increment.pod(csv_source, label="inc")
        _ = halve.pod(delta_source, label="half")
        _ = square.pod(list_source, label="sq")
    _ = job.run()

    expected = root / "expected"
    expected.mkdir()
    for label in FUNCTION_NODES:
        node = job.nodes[label]
        pq.write_table(node.as_table(), expected / f"{label}.parquet")
        pq.write_table(
            node.as_table(all_info=True), expected / f"{label}.all_info.parquet"
        )

    job.save(str(root / "job.json"))


if __name__ == "__main__":
    main(pathlib.Path(sys.argv[1]))
