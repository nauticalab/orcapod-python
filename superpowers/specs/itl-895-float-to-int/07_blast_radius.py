"""Claim 7: blast radius for existing stores, and how to find affected rows.

1. A truncated stored row is byte-identical to a genuine one: the stored value, the
   output data hash and the record are those of ``1``; nothing records that the function
   returned ``1.5``. Stored data alone cannot tell the two apart.
2. Later runs reuse the stored row without calling the function (Phase 1 of
   ``FunctionNode``). A fix that rejects floats therefore does NOT touch rows already
   stored: they keep being served until the user removes them or bumps the pod version.
3. Recipe to find affected rows: for each pod whose declared output contains an integer
   type (or another type pyarrow coerces: ``date``/``datetime``, ``str``/``bytes``,
   ``float32``, lists/structs of these), call the raw function again on the node's stored
   inputs and flag rows whose returned value is not exactly the declared type.

Run: uv run python superpowers/specs/itl-895-float-to-int/07_blast_radius.py
"""

from __future__ import annotations

import logging

from orcapod import PipelineJob, function_pod
from orcapod.databases import InMemoryArrowDatabase
from orcapod.sources import DictSource

logging.disable(logging.CRITICAL)
calls: list[int] = []


def raw_half(x: int) -> int:
    calls.append(x)
    return (  # pyright: ignore[reportReturnType]  # deliberate wrong return type: reproduces ITL-895
        x + 0.5 if x % 2 else x
    )  # odd inputs return a float


half = function_pod(output_keys="y")(raw_half)

rows = [{"k": f"r{i}", "x": i} for i in range(4)]
db = InMemoryArrowDatabase()


def build() -> PipelineJob:
    job = PipelineJob(name="blast", store=db)
    with job:
        src = DictSource(data=rows, tag_columns=["k"], source_id="src")
        half.pod(src, label="half")
    return job


job = build()
job.run()
recs = job.nodes["half"].get_all_records(columns={"meta": True})
print("1. stored rows (odd x returned x + 0.5):")
for k, y in zip(recs["k"].to_pylist(), recs["y"].to_pylist()):
    x = int(k[1:])
    print(f"   {k}: x={x} stored y={y!r}")
print(
    f"   stored column type: {recs.schema.field('y').type}; meta columns: "
    f"{[c for c in recs.column_names if c.startswith('__')][:4]} ..."
)

calls.clear()
job2 = build()
job2.run()
print(
    f"\n2. second run over the same store: function calls={len(calls)}; "
    f"y={job2.nodes['half'].as_table()['y'].to_pylist()} (served from the store)"
)

print(
    "\n3. finding affected rows: re-call the raw function on the node's stored inputs"
)
src_stream = job2.nodes["src"]
affected = []
for tag, data in src_stream.iter_data():
    returned = raw_half(**data.as_dict())
    if type(returned) is not int:
        affected.append((tag.as_dict()["k"], returned))
print(f"   affected rows (tag, value the function returns): {affected}")
