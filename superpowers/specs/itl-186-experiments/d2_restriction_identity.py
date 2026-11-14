"""D2 experiment (ITL-186): a source's row restriction in or out of its content hash.

Run from the repository root:

    uv run python superpowers/specs/itl-186-experiments/d2_restriction_identity.py

No SpiralDB needed. ``Restricted`` stands for a Spiral source over one 10-row table: the
same source id and key-based record ids whatever rows it reads, with the restriction either
left out of ``identity_structure`` (OUT) or appended to it (IN). Two jobs with the same
pipeline run one after the other on one Delta store: job A reads a larger part of the
table, job B a smaller one.
"""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path
from typing import Any, cast

import pyarrow as pa
import pyarrow.compute as pc

from orcapod import PipelineJob, function_pod
from orcapod.databases import DeltaTableDatabase
from orcapod.pipeline.async_orchestrator import AsyncPipelineOrchestrator
from orcapod.sources import ArrowTableSource, DictSource

FULL = pa.table(
    {"k": pa.array(range(10), pa.int64()), "v": pa.array(range(10), pa.int64())}
).cast(pa.schema([pa.field("k", pa.int64(), False), pa.field("v", pa.int64(), False)]))
SOURCE_ID = "proj/default.t@table_X"


class Restricted(ArrowTableSource):
    """One table, read through a restriction that may or may not enter identity."""

    def __init__(self, keys: list[int], restriction: str, in_identity: bool) -> None:
        self._restriction = restriction
        self._in_identity = in_identity
        rows = FULL.filter(pc.is_in(FULL["k"], pa.array(keys, pa.int64())))
        super().__init__(
            rows, tag_columns=["k"], source_id=SOURCE_ID, record_id_column="k"
        )

    def identity_structure(self) -> Any:
        base = super().identity_structure()
        return (*base, self._restriction) if self._in_identity else base


def build(store: Path, source: Any, calls: list[int]) -> PipelineJob:
    @function_pod(output_keys="w")
    def double(v: int) -> int:
        calls.append(v)
        return 2 * v

    job = PipelineJob(name="job", database=DeltaTableDatabase(store))
    with job:
        double.pod(source, label="double")
    return job


def keys_of(table: pa.Table | None) -> list[int] | None:
    if table is None:
        return None
    return sorted(cast("list[int]", table.column("k").to_pylist()))


def report(name: str, job: PipelineJob, calls: list[int], store: Path) -> None:
    node = job.nodes["double"]
    print(f"  {name}: function calls this run = {sorted(calls)}")
    print(f"    node.as_table() keys        = {keys_of(node.as_table())}")
    print(f"    node.iter_data() rows       = {sum(1 for _ in node.iter_data())}")
    print(f"    node.get_all_records() keys = {keys_of(node.get_all_records())}")
    saved = store.parent / f"{store.name}-{name}.json"
    job.save(saved)
    loaded = PipelineJob.load(saved, database=DeltaTableDatabase(store)).nodes["double"]
    print(f"    loaded job get_all_records  = {keys_of(loaded.get_all_records())}")
    print(f"    node content hash           = {node.content_hash().to_hex()[:12]}")
    print(f"    node pipeline hash          = {node.pipeline_hash().to_hex()[:12]}")


def tables(store: Path) -> None:
    import deltalake

    for log in sorted(store.rglob("_delta_log")):
        table = deltalake.DeltaTable(str(log.parent)).to_pyarrow_table()
        extra = ""
        if "__node_content_hash" in table.column_names:
            distinct = len(set(table.column("__node_content_hash").to_pylist()))
            extra = f", {distinct} distinct node content hash(es)"
        print(
            f"    table .../{log.parent.name} ({'results' if '_result' in log.parts else 'pipeline records'}): {table.num_rows} rows{extra}"
        )


def scenario(title: str, make_a: Any, make_b: Any, orchestrator: Any) -> None:
    print(f"\n== {title} ({'async' if orchestrator else 'sync'})")
    store = Path(tempfile.mkdtemp()) / "store"
    for name, make in (("A", make_a), ("B", make_b)):
        calls: list[int] = []
        job = build(store, make(), calls)
        job.run(orchestrator=orchestrator() if orchestrator else None)
        report(name, job, calls, store)
    print("  stored tables:")
    tables(store)


LIMIT_A, LIMIT_B = [0, 1, 2, 3, 4], [0, 1]
WHERE_A, WHERE_B = [0, 2, 4, 6, 8], [0, 1, 2]
for orch in (None, AsyncPipelineOrchestrator):
    for in_identity in (False, True):
        tag = "IN the content hash" if in_identity else "OUT of the content hash"
        scenario(
            f"limit: A reads k=0..4, B reads k=0..1; restriction {tag}",
            lambda: Restricted(LIMIT_A, "limit=5", in_identity),
            lambda: Restricted(LIMIT_B, "limit=2", in_identity),
            orch,
        )
        scenario(
            f"where: A reads even k, B reads k<3; restriction {tag}",
            lambda: Restricted(WHERE_A, "where=even", in_identity),
            lambda: Restricted(WHERE_B, "where=k<3", in_identity),
            orch,
        )
    scenario(
        "today: two DictSources, same schema and source id, different rows (positional record ids)",
        lambda: DictSource(
            [{"k": k, "v": k} for k in LIMIT_A],
            tag_columns=["k"],
            source_id=SOURCE_ID,
        ),
        lambda: DictSource(
            [{"k": k, "v": k} for k in LIMIT_B],
            tag_columns=["k"],
            source_id=SOURCE_ID,
        ),
        orch,
    )
sys.stdout.flush()
