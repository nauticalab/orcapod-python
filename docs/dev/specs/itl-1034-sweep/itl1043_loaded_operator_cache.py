"""ITL-1043: a loaded job cannot read a cached operator node (repro, main 003d2e82).

Join of two 2-row ArrowTableSources, operator cache LOG, run, save, load, re-attach REPLAY.
Prints, for nullable and non-nullable tag columns, the pipeline hash (the `schema:` path
component) and the upstream pipeline identity structures of the live and the loaded node.
"""

import tempfile
from pathlib import Path

import pyarrow as pa

from orcapod import PipelineJob
from orcapod.core.operators import Join
from orcapod.databases import InMemoryArrowDatabase
from orcapod.sources import ArrowTableSource
from orcapod.types import CacheMode


def run(nullable: bool) -> None:
    def table(data_col: str) -> pa.Table:
        return pa.table(
            {"k": ["a", "b"], data_col: [1, 2]},
            schema=pa.schema(
                [
                    pa.field("k", pa.large_string(), nullable=nullable),
                    pa.field(data_col, pa.int64(), nullable=False),
                ]
            ),
        )

    store = InMemoryArrowDatabase()
    job = PipelineJob(name="j", database=store)
    with job:
        Join()(
            ArrowTableSource(table("x"), tag_columns=["k"], source_id="t"),
            ArrowTableSource(table("z"), tag_columns=["k"], source_id="u"),
            label="op",
        )
    job.nodes["op"].attach_databases(
        pipeline_database=store.at("j"), cache_mode=CacheMode.LOG
    )
    job.run()
    live = job.nodes["op"]
    print(f"--- nullable tag columns: {nullable}")
    print(
        "live   pipeline_hash",
        live.pipeline_hash().to_hex()[:16],
        "rows",
        live.as_table().num_rows,
    )
    path = Path(tempfile.mkdtemp()) / "j.json"
    job.save(path)
    loaded_job = PipelineJob.load(str(path), database=store)
    loaded = loaded_job.nodes["op"]
    loaded.attach_databases(
        pipeline_database=store.at("j"), cache_mode=CacheMode.REPLAY
    )
    print("loaded pipeline_hash", loaded.pipeline_hash().to_hex()[:16])
    print(
        "content_hash live",
        live.content_hash().to_hex()[:16],
        "loaded",
        loaded.content_hash().to_hex()[:16],
    )
    rec = loaded.get_all_records(all_info=True)
    print("loaded get_all_records rows:", None if rec is None else rec.num_rows)
    for i, (a, b) in enumerate(zip(live.upstreams, loaded.upstreams)):
        sa, sb = a.pipeline_identity_structure(), b.pipeline_identity_structure()
        print(f"upstream {i} live  ", type(a).__name__, sa)
        print(f"upstream {i} loaded", type(b).__name__, sb)
        print(
            f"upstream {i} pipeline hashes equal:",
            a.pipeline_hash() == b.pipeline_hash(),
        )
    try:
        print("loaded as_table rows:", loaded.as_table().num_rows)
    except Exception as exc:
        print("loaded as_table raises:", str(exc)[:160])


run(False)
run(True)


def identities(nullable: bool) -> None:
    """Content identity structures of the upstreams, live and loaded."""
    import pyarrow as pa

    t = pa.table(
        {"k": ["a"], "x": [1]},
        schema=pa.schema(
            [
                pa.field("k", pa.large_string(), nullable=nullable),
                pa.field("x", pa.int64(), nullable=False),
            ]
        ),
    )
    store = InMemoryArrowDatabase()
    job = PipelineJob(name="j", database=store)
    with job:
        Join()(
            ArrowTableSource(t, tag_columns=["k"], source_id="t"),
            ArrowTableSource(
                t.rename_columns(["k", "z"]), tag_columns=["k"], source_id="u"
            ),
            label="op",
        )
    job.run()
    path = Path(tempfile.mkdtemp()) / "j.json"
    job.save(path)
    loaded = PipelineJob.load(str(path), database=store).nodes["op"]
    live = job.nodes["op"]
    print("== identities, nullable", nullable)
    for a, b in zip(live.upstreams, loaded.upstreams):
        print(" live  ", a.identity_structure())
        print(" loaded", b.identity_structure())
    print(" op live  ", live.identity_structure())
    print(" op loaded", loaded.identity_structure())


identities(False)
