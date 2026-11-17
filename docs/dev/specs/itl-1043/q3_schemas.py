"""ITL-1043 Q3: the schemas hashed by the running and the loaded operator node.

Prints, for every node of a Join job with a nullable tag, the hashed schema of each
input source node (types, ``optional_fields``) and the identity structures, then the
saved ``output_schema`` / ``source_config`` blocks, before save and after load.

Run: uv run python superpowers/specs/itl-1043/q3_schemas.py
"""

import json
import tempfile
from pathlib import Path

import pyarrow as pa

from orcapod.core.operators import Join
from orcapod.core.sources import ArrowTableSource
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline import PipelineJob
from orcapod.types import CacheMode

NAME = "j"


def table(col: str, vals: list[int], nullable: bool) -> pa.Table:
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=nullable),
            pa.field(col, pa.int64(), nullable=False),
        ]
    )
    return pa.table({"id": [0, 1], col: vals}, schema=schema)


def describe(schema) -> str:
    fields = ", ".join(f"{k}: {v!r}" for k, v in schema.items())
    return (
        f"{{{fields}}} optional_fields={sorted(getattr(schema, 'optional_fields', ()))}"
    )


def show(label: str, node) -> None:
    tag, data = node.output_schema()
    print(f"  {label}: {type(node).__name__}")
    print(f"    tag  {describe(tag)}")
    print(f"    data {describe(data)}")
    print(f"    pipeline_hash {node.pipeline_hash().to_string()}")
    print(f"    content_hash  {node.content_hash().to_string()}")


store = InMemoryArrowDatabase()
job = PipelineJob(name=NAME, database=store)
with job:
    left = ArrowTableSource(table("a", [1, 2], True), tag_columns=["id"], source_id="a")
    right = ArrowTableSource(
        table("b", [3, 4], True), tag_columns=["id"], source_id="b"
    )
    Join()(left, right, label="joined")
job.nodes["joined"].attach_databases(
    pipeline_database=store.at(NAME), cache_mode=CacheMode.LOG
)
job.run()

print("== source table schema (Arrow) ==")
print(" ", left.as_table().schema.field("id"))
print("== running job ==")
for label, node in job.nodes.items():
    show(label, node)
print("== compiled (blueprint) pipeline, which save() writes ==")
for label, node in job.compiled_pipeline.nodes.items():
    show(label, node)

with tempfile.TemporaryDirectory() as d:
    saved = Path(d) / "job.json"
    job.save(saved)
    data = json.loads(saved.read_text())
    print("== saved descriptors ==")
    for desc in data["pipeline"]["nodes"].values():
        print(" ", desc["label"], "output_schema", desc["output_schema"])
        if "source_config" in desc:
            sc = desc["source_config"]
            print(
                "   source_config",
                {"tag_schema": sc["tag_schema"], "data_schema": sc["data_schema"]},
            )
        print(
            "   content_hash",
            desc["content_hash"],
            "pipeline_hash",
            desc["pipeline_hash"],
        )
    loaded = PipelineJob.load(str(saved), database=store)
    print("== loaded job ==")
    for label, node in loaded.nodes.items():
        show(label, node)
