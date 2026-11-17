"""ITL-1060 (and ITL-1042): a row read back from the store gets a fresh datagram id.

A pod over a source with an extra data column (so the automatic select, an operator node,
feeds it) runs on the async orchestrator: the function node is stale right after the run.
A second job on the same store reads every row back; each read-back row's
``datagram_uuid`` is neither the stored ``__data_id`` nor the uuid its own source-info
token names. Measured on main 003d2e82 and cdb4f160 (November 13, 2026).
"""

import pyarrow as pa
from orcapod import PipelineJob, function_pod
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator
from orcapod.sources import ArrowTableSource


@function_pod(output_keys="y")
def f(x: int) -> int:
    return 10 * x


t = pa.table(
    {"k": ["a", "b"], "x": [1, 2], "s": ["p", "q"]},
    schema=pa.schema(
        [
            pa.field("k", pa.large_string(), nullable=False),
            pa.field("x", pa.int64(), nullable=False),
            pa.field("s", pa.large_string(), nullable=False),
        ]
    ),
)


def job(store):
    j = PipelineJob(name="j", database=store)
    with j:
        f.pod(ArrowTableSource(t, tag_columns=["k"], source_id="src"), label="n")
    return j


store = InMemoryArrowDatabase()
first = job(store)
first.run(orchestrator=AsyncPipelineOrchestrator())
print("async run: node stale right after the run:", first.nodes["n"].is_stale)
second = job(store)
s = second.run(orchestrator=SyncPipelineOrchestrator())
node = second.nodes["n"]
rec = {
    r["k"]: r["__data_id"].hex()
    for r in node.get_all_records(all_info=True).to_pylist()
}
print("rerun (rows from store):", str(s).splitlines()[-1].strip())
print(
    "  read back: datagram_uuid == stored __data_id:",
    all(d.datagram_uuid.hex == rec[tg.as_dict()["k"]] for tg, d in node.iter_data()),
)
print(
    "  read back: datagram_uuid == its own token:",
    all(
        d.datagram_uuid.hex == d.source_info()["y"].split("::")[1]
        for _, d in node.iter_data()
    ),
)
