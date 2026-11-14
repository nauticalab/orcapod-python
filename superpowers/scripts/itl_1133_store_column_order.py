"""ITL-1133 reproduction (found during ITL-1131): one operator node table, two column orders.

Fails on main before the ITL-1133 fix (52d67858); prints OK for every case after it.

A job run keeps and stores an operator node's rows in row order (user tags, system tags,
data, source info: ``_rows_to_table``); a direct ``node.run()`` (``_compute_and_store``)
in ``CacheMode.LOG`` stores them in the operator's own order (a join: user tags, data,
system tags). Appending the rows of one to a table holding the other fails with
``StoreWriteError`` (schema mismatch) in the in-memory store, and in a Delta store while the
first write is a pending batch (when the store is not read in between, the in-memory store
fails on the next read instead). A Delta store flushed between the two writes appends by
name and does not fail. With identical rows nothing is appended
(``skip_duplicates``), so each step here has new rows under the same source ids (same
node path and content hash).

Run: ``uv run python superpowers/scripts/itl_1133_store_column_order.py``
"""

from __future__ import annotations

import tempfile

import pyarrow as pa

from orcapod.core.nodes.operator_node import CacheMode
from orcapod.core.operators import Join
from orcapod.core.sources import ArrowTableSource
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.pipeline.job import PipelineJob


def _source(name: str, column: str, ids: list[int]) -> ArrowTableSource:
    table = pa.table(
        {"id": pa.array(ids, pa.int64()), column: pa.array(ids, pa.int64())}
    )
    return ArrowTableSource(table, tag_columns=["id"], source_id=name)


def _job(ids: list[int]) -> PipelineJob:
    job = PipelineJob(name="itl1133", database=InMemoryArrowDatabase())
    with job:
        _ = Join()(_source("a", "x", ids), _source("b", "z", ids), label="joined")
    return job


def _attempt(store_kind: str, first: str, second: str) -> str:
    store = (
        InMemoryArrowDatabase()
        if store_kind == "in-memory"
        else DeltaTableDatabase(tempfile.mkdtemp())
    )
    try:
        for step, ids in ((first, [1, 2]), (second, [3, 4])):
            job = _job(ids)
            node = job.nodes["joined"]
            node.attach_databases(pipeline_database=store, cache_mode=CacheMode.LOG)
            if step == "job":
                _ = job.run()  # sync orchestrator -> execute(): row order
            else:
                _ = node.run()  # _compute_and_store: the operator's order
            _ = node.get_all_records()
        return "OK"
    except Exception as exc:  # noqa: BLE001  # a reproduction reports any failure
        return f"{type(exc).__name__}: {str(exc).splitlines()[0][:120]}"


if __name__ == "__main__":
    for kind in ("in-memory", "delta (no flush)"):
        for first, second in (("job", "node"), ("node", "job")):
            print(f"{kind:17s} {first} then {second}: {_attempt(kind, first, second)}")
