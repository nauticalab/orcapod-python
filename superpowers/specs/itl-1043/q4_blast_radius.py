"""ITL-1043 Q4: every node's hashes and paths before save and after load, and re-runs.

Job: sources ``a`` (tag ``id``, data ``x``) and ``b`` (tag ``id``, data ``y``); function
node ``inc`` on ``a``; operator node ``joined`` = Join(inc, b) in LOG mode. Tag and data
nullability vary. For each case it prints, per node, pipeline_hash / content_hash / stored
path for: the run; ``PipelineJob.load``; the same job rebuilt from code; and
``Pipeline.load`` + ``PipelineJob.from_pipeline(sources=...)``. Then it re-runs the rebuilt
job and reports what was computed vs read from the store.

Run: uv run python superpowers/specs/itl-1043/q4_blast_radius.py
"""

import json
import tempfile
from pathlib import Path

import pyarrow as pa

from orcapod import function_pod
from orcapod.core.operators import Join
from orcapod.core.sources import ArrowTableSource
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline import Pipeline, PipelineJob
from orcapod.types import CacheMode

NAME = "j"


@function_pod(output_keys="x1")
def inc(x: int) -> int:
    return x + 1


@function_pod(output_keys="x1")
def inc_opt(x: int | None) -> int | None:
    return None if x is None else x + 1


def sources(
    tag_null: bool, data_null: bool
) -> tuple[ArrowTableSource, ArrowTableSource]:
    def one(col: str, vals: list[int], sid: str) -> ArrowTableSource:
        schema = pa.schema(
            [
                pa.field("id", pa.int64(), nullable=tag_null),
                pa.field(col, pa.int64(), nullable=data_null),
            ]
        )
        return ArrowTableSource(
            pa.table({"id": [0, 1], col: vals}, schema=schema),
            tag_columns=["id"],
            source_id=sid,
        )

    return one("x", [1, 2], "a"), one("y", [3, 4], "b")


def build(store, tag_null: bool, data_null: bool, mode: CacheMode) -> PipelineJob:
    a, b = sources(tag_null, data_null)
    job = PipelineJob(name=NAME, store=store)
    with job:
        out = (inc_opt if data_null else inc).pod(a, label="inc")
        Join()(out, b, label="joined")
    job.nodes["joined"].attach_databases(
        pipeline_database=store.at(NAME), cache_mode=mode
    )
    return job


def path(node) -> str:
    if hasattr(node, "_stored_table_path"):
        return "/".join(node._stored_table_path)
    if hasattr(node, "_versioned_pipeline_path"):
        return "/".join(node._versioned_pipeline_path)
    return "-"


def snapshot(job) -> dict[str, tuple[str, str, str, str]]:
    out = {}
    for label, node in job.nodes.items():
        tag, data = node.output_schema()
        out[label] = (
            node.pipeline_hash().to_hex()[:8],
            node.content_hash().to_hex()[:8],
            path(node).split("/")[-2][:15] if path(node) != "-" else "-",
            f"tag={dict(tag)} data={dict(data)}",
        )
    return out


def compare(
    name: str,
    ref: dict[str, tuple[str, str, str, str]],
    other: dict[str, tuple[str, str, str, str]],
) -> None:
    for label in sorted(ref):
        r, o = ref[label], other.get(label)
        flags = (
            []
            if o is None
            else [
                k
                for k, i in (
                    ("pipeline", 0),
                    ("content", 1),
                    ("path", 2),
                    ("schema", 3),
                )
                if r[i] != o[i]
            ]
        )
        print(
            f"    {name:14s} {label:7s} {o[:3] if o else None} differs: {flags or 'none'}"
        )
        if o and "schema" in flags:
            print(f"        run    {r[3]}\n        {name:6s} {o[3]}")


for tag_null in (False, True):
    for data_null in (False, True):
        print(f"=== tag nullable={tag_null} data nullable={data_null}")
        store = InMemoryArrowDatabase()
        job = build(store, tag_null, data_null, CacheMode.LOG)
        job.run()
        ref = snapshot(job)
        for label, v in ref.items():
            print(f"    run            {label:7s} {v[:3]}")
        with tempfile.TemporaryDirectory() as d:
            saved = Path(d) / "job.json"
            job.save(saved)
            loaded = PipelineJob.load(str(saved), store=store)
            compare("load", ref, snapshot(loaded))
            print(
                "      load: inc rows",
                loaded.nodes["inc"].as_table().num_rows,
                end="; ",
            )
            lj = loaded.nodes["joined"]
            lj.attach_databases(
                pipeline_database=store.at(NAME), cache_mode=CacheMode.REPLAY
            )
            try:
                print("joined REPLAY rows", lj.as_table().num_rows)
            except RuntimeError as exc:
                print("joined REPLAY:", str(exc)[:60])

            # Rebuilt from code (definitions in hand), same store.
            rebuilt = build(store, tag_null, data_null, CacheMode.REPLAY)
            compare("rebuilt", ref, snapshot(rebuilt))
            summary = rebuilt.run()
            print(
                f"      rebuilt re-run: inc computed={summary.nodes['inc'].computed} "
                f"read={summary.nodes['inc'].reused}; joined REPLAY rows={rebuilt.nodes['joined'].as_table().num_rows}"
            )

            # Pipeline.load of the saved blueprint + from_pipeline with live sources.
            pfile = Path(d) / "pipeline.json"
            pfile.write_text(json.dumps(json.loads(saved.read_text())["pipeline"]))
            pl = Pipeline.load(pfile)
            a, b = sources(tag_null, data_null)
            try:
                fp = PipelineJob.from_pipeline(
                    pl, store=store, sources={"a": a, "b": b}
                )
                compare("from_pipeline", ref, snapshot(fp))
                try:
                    s = fp.run()
                    print(
                        "      from_pipeline run:",
                        {k: (v.computed, v.reused) for k, v in s.nodes.items()},
                    )
                except Exception as exc:  # noqa: BLE001
                    print(
                        "      from_pipeline run:", type(exc).__name__, str(exc)[:100]
                    )
            except Exception as exc:  # noqa: BLE001
                print("      from_pipeline:", type(exc).__name__, str(exc)[:160])
