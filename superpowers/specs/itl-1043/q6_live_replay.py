"""ITL-1043 Q6: what a LIVE operator node in REPLAY does with no rows of its own.

(1) table absent from the store; (2) table present, holding only another run's rows.

Run: uv run python superpowers/specs/itl-1043/q6_live_replay.py
"""

import pyarrow as pa

from orcapod import PipelineJob
from orcapod.core.operators import Join
from orcapod.databases import InMemoryArrowDatabase
from orcapod.sources import ArrowTableSource
from orcapod.types import CacheMode


def src(col: str, ids: list[int], sid: str) -> ArrowTableSource:
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field(col, pa.int64(), nullable=False),
        ]
    )
    return ArrowTableSource(
        pa.table({"id": ids, col: [1] * len(ids)}, schema=schema),
        tag_columns=["id"],
        source_id=sid,
    )


def job(store, a_ids: list[int], mode: CacheMode) -> PipelineJob:
    j = PipelineJob(name="q6", store=store)
    with j:
        Join()(src("a", a_ids, f"a{a_ids}"), src("b", [0, 1], "b"), label="joined")
    j.nodes["joined"].attach_databases(
        pipeline_database=store.at("q6"), cache_mode=mode
    )
    return j


def show(case: str, node) -> None:
    try:
        t = node.as_table()
        print(f"{case}: as_table rows={t.num_rows} cols={t.column_names}")
    except Exception as exc:  # noqa: BLE001
        print(f"{case}: as_table raised {type(exc).__name__}: {exc}")
    try:
        print(f"{case}: iter_data rows={len(list(node.iter_data()))}")
    except Exception as exc:  # noqa: BLE001
        print(f"{case}: iter_data raised {type(exc).__name__}: {exc}")
    h = node.head()
    print(
        f"{case}: head status={h.status} rows={h.table.num_rows} header={h.header()!r}"
    )


# (1) absent table
show(
    "live REPLAY, table absent",
    job(InMemoryArrowDatabase(), [0, 1], CacheMode.REPLAY).nodes["joined"],
)
# (2) table with another run's rows only
store = InMemoryArrowDatabase()
job(store, [0, 1], CacheMode.LOG).run()
show(
    "live REPLAY, only other runs' rows",
    job(store, [1], CacheMode.REPLAY).nodes["joined"],
)
