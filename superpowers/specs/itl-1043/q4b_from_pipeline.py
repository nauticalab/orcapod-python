"""ITL-1043 Q4b: an operator-only pipeline loaded with ``Pipeline.load`` and run again.

``Pipeline.load`` rebuilds operators (not function pods), so ``from_pipeline`` with live
sources gives a runnable job. Does its operator node find the cache the first run wrote?

Run: uv run python superpowers/specs/itl-1043/q4b_from_pipeline.py
"""

import json
import tempfile
from pathlib import Path

import pyarrow as pa

from orcapod.core.operators import Join
from orcapod.core.sources import ArrowTableSource
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline import Pipeline, PipelineJob
from orcapod.types import CacheMode

NAME = "j"


def sources(tag_null: bool) -> tuple[ArrowTableSource, ArrowTableSource]:
    def one(col: str, vals: list[int], sid: str) -> ArrowTableSource:
        schema = pa.schema(
            [
                pa.field("id", pa.int64(), nullable=tag_null),
                pa.field(col, pa.int64(), nullable=False),
            ]
        )
        return ArrowTableSource(
            pa.table({"id": [0, 1], col: vals}, schema=schema),
            tag_columns=["id"],
            source_id=sid,
        )

    return one("x", [1, 2], "a"), one("y", [3, 4], "b")


for tag_null in (False, True):
    store = InMemoryArrowDatabase()
    a, b = sources(tag_null)
    job = PipelineJob(name=NAME, database=store)
    with job:
        Join()(a, b, label="joined")
    node = job.nodes["joined"]
    node.attach_databases(pipeline_database=store.at(NAME), cache_mode=CacheMode.LOG)
    job.run()
    with tempfile.TemporaryDirectory() as d:
        saved = Path(d) / "job.json"
        job.save(saved)
        pfile = Path(d) / "pipeline.json"
        pfile.write_text(json.dumps(json.loads(saved.read_text())["pipeline"]))
        a2, b2 = sources(tag_null)
        fp = PipelineJob.from_pipeline(
            Pipeline.load(pfile), database=store, sources={"a": a2, "b": b2}
        )
        fnode = fp.nodes["joined"]
        fnode.attach_databases(
            pipeline_database=store.at(NAME), cache_mode=CacheMode.REPLAY
        )
        print(f"tag nullable={tag_null}")
        print(
            "  run   path",
            node._stored_table_path[-2][:20],
            "content",
            node.content_hash().to_hex()[:8],
        )
        print(
            "  load  path",
            fnode._stored_table_path[-2][:20],
            "content",
            fnode.content_hash().to_hex()[:8],
        )
        print(
            "  slot schema",
            dict(fp.nodes["a"].output_schema()[0]),
            "bound source",
            dict(a2.output_schema()[0]),
        )
        try:
            print("  REPLAY rows:", fnode.as_table().num_rows)
        except Exception as exc:  # noqa: BLE001
            print("  REPLAY:", type(exc).__name__, str(exc)[:90])
