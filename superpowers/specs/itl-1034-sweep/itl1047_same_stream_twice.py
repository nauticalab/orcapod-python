"""Suspect: async operator nodes pair input channels with upstream pipeline hashes by position."""

import pyarrow as pa
from orcapod import PipelineJob
from orcapod.core.operators import MergeJoin, SemiJoin
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator
from orcapod.sources import ArrowTableSource

t = pa.table(
    {"k": ["a", "b"], "x": [1, 2]},
    schema=pa.schema(
        [
            pa.field("k", pa.large_string(), nullable=False),
            pa.field("x", pa.int64(), nullable=False),
        ]
    ),
)
for op in (SemiJoin, MergeJoin):
    for orch in (SyncPipelineOrchestrator, AsyncPipelineOrchestrator):
        job = PipelineJob(name="j", database=InMemoryArrowDatabase())
        with job:
            src = ArrowTableSource(t, tag_columns=["k"], source_id="src")
            op()(src, src, label="op")
        try:
            job.run(orchestrator=orch())
            tab = job.nodes["op"].as_table(all_info=True)
            print(
                op.__name__,
                orch.__name__[:5],
                "rows",
                tab.num_rows,
                [c[:40] for c in tab.column_names if c.startswith("_tag")],
            )
        except BaseException as exc:
            inner = exc.exceptions[0] if isinstance(exc, BaseExceptionGroup) else exc
            print(
                op.__name__,
                orch.__name__[:5],
                "FAILS",
                type(inner).__name__,
                str(inner).splitlines()[0][:150],
            )
