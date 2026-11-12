"""Repro: two Spiral projects with the same table name feed the same pipeline.

Run from the repository root:

    uv run python superpowers/specs/itl-926-live/repros/source_id_collision_across_projects.py

Uses the pyspiral fake. Two projects each have a ``subject`` table with the same schema but
different rows; the same job (same name, same store) runs on project A, then on project B,
with the default ``source_id``. Prints each source's ``source_id`` and ``content_hash``, the
function calls each run makes, and what ``get_all_records()`` returns after the B run.
"""

import sys
import tempfile
from pathlib import Path

import pyarrow as pa

sys.path.insert(0, "tests/test_databases")
import fake_pyspiral  # noqa: E402  # pyright: ignore[reportMissingImports]  (path set above)

from orcapod import PipelineJob, function_pod  # noqa: E402
from orcapod.databases import DeltaTableDatabase  # noqa: E402
from orcapod.sources import SpiralDBTableSource  # noqa: E402

work = Path(tempfile.mkdtemp())
calls: list[str] = []


@function_pod(output_keys="weight_kg")
def to_kg(weight_g: float) -> float:
    """Convert grams to kilograms, counting the call."""
    calls.append("call")
    return weight_g / 1000


schema = pa.schema(
    [
        pa.field("subject_id", pa.string(), False),
        pa.field("weight_g", pa.float64(), False),
    ]
)
with fake_pyspiral.install(work / "spiral"):
    for project, weights in (
        ("edi-sample-a", [1000.0, 2000.0]),
        ("edi-sample-b", [3000.0, 4000.0]),
    ):
        table = (
            fake_pyspiral.FakeSpiral()
            .project(project)
            .create_table(
                "default.subject",
                key_schema=pa.schema([pa.field("subject_id", pa.string(), False)]),
            )
        )
        table.write(
            pa.table({"subject_id": ["m1", "m2"], "weight_g": weights}, schema=schema)
        )

    for project in ("edi-sample-a", "edi-sample-b"):
        calls.clear()
        source = SpiralDBTableSource(project, "subject")
        job = PipelineJob(name="workshop", store=DeltaTableDatabase(work / "store"))
        with job:
            to_kg.pod(source, label="kg")
        job.run()
        print(
            f"{project}: source_id={source.source_id!r} "
            f"content_hash={source.content_hash().to_string()[:28]}... calls={len(calls)}"
        )
        print("   as_table:", job.nodes["kg"].as_table().to_pylist())

    later = PipelineJob(name="workshop", store=DeltaTableDatabase(work / "store"))
    with later:
        to_kg.pod(SpiralDBTableSource("edi-sample-b", "subject"), label="kg")
    records = later.nodes["kg"].get_all_records()
    print(
        "project B job, get_all_records():",
        None if records is None else records.to_pylist(),
    )
