"""ITL-1043 Q1: reproduce reading a cached operator node from a loaded job.

Run: uv run python superpowers/specs/itl-1043/q1_repro.py
"""

import sys
import tempfile
from pathlib import Path

import pyarrow as pa

from orcapod.core.operators import Join
from orcapod.core.sources import ArrowTableSource
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.pipeline import PipelineJob
from orcapod.types import CacheMode

NAME = "j"


def tables(nullable: bool) -> tuple[pa.Table, pa.Table]:
    """Two 2-row tables joined on tag ``id``; ``id`` nullable or not (no nulls)."""

    def one(col: str, vals: list[int]) -> pa.Table:
        schema = pa.schema(
            [
                pa.field("id", pa.int64(), nullable=nullable),
                pa.field(col, pa.int64(), nullable=False),
            ]
        )
        return pa.table({"id": [0, 1], col: vals}, schema=schema)

    return one("a", [1, 2]), one("b", [3, 4])


def run(store_kind: str, nullable: bool, tmp: Path) -> None:
    store = (
        InMemoryArrowDatabase()
        if store_kind == "memory"
        else DeltaTableDatabase(str(tmp / "store"))
    )
    ta, tb = tables(nullable)
    job = PipelineJob(name=NAME, database=store)
    with job:
        left = ArrowTableSource(ta, tag_columns=["id"], source_id="a")
        right = ArrowTableSource(tb, tag_columns=["id"], source_id="b")
        Join()(left, right, label="joined")
    node = job.nodes["joined"]
    node.attach_databases(pipeline_database=store.at(NAME), cache_mode=CacheMode.LOG)
    job.run()
    print(f"--- store={store_kind} nullable_tag={nullable}")
    print("  run: path", "/".join(node._stored_table_path))
    print("  run: content_hash", node.content_hash().to_string())
    print("  run: rows before save", node.as_table().num_rows)
    saved = tmp / "job.json"
    job.save(saved)

    loaded = PipelineJob.load(str(saved), database=store)
    lnode = loaded.nodes["joined"]
    print(
        "  load: node type",
        type(lnode).__name__,
        "operator present:",
        lnode._operator is not None,
    )
    try:
        print("  (a) no re-attach rows:", lnode.as_table().num_rows)
    except Exception as exc:  # noqa: BLE001 - report every outcome
        print("  (a) no re-attach:", type(exc).__name__, str(exc)[:110])
    lnode.attach_databases(
        pipeline_database=store.at(NAME), cache_mode=CacheMode.REPLAY
    )
    print("  load: path", "/".join(lnode._stored_table_path))
    print("  load: content_hash", lnode.content_hash().to_string())
    try:
        print("  (b) REPLAY rows:", lnode.as_table().num_rows)
    except Exception as exc:  # noqa: BLE001
        print("  (b) REPLAY:", type(exc).__name__, str(exc)[:200])
    raw = store.at(NAME).get_all_records(
        node._stored_table_path, record_id_column="__record_id"
    )
    print("  stored rows at run path:", None if raw is None else raw.num_rows)


for kind in ("memory", "delta"):
    for nullable in (False, True):
        with tempfile.TemporaryDirectory() as d:
            run(kind, nullable, Path(d))
print(sys.version.split()[0])
