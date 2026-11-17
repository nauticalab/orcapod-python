"""Claim 3: the output Data is internally inconsistent, and downstream pods see 1.5.

``PythonDataFunction._build_output_data`` builds ``Data({"y": 1.5}, python_schema={"y": int})``
without checking the value. The Data keeps the raw dict; its Arrow table (used for
``content_hash()`` and for storage) is built lazily and truncates to 1. So:

* ``data.as_dict()`` says 1.5, ``data.as_table()`` / the stored record / the hash say 1.
* A downstream pod consumes ``as_dict()``. Chained standalone it receives 1.5; in a
  PipelineJob it receives 1 on the sync orchestrator and 1.5 on the async one. The same
  graph gives z=15 or z=10 depending on how it runs. In the standalone chain the upstream
  Data is never converted to Arrow before the downstream function runs.

Run: uv run python superpowers/specs/itl-895-float-to-int/03_split_datagram.py
"""

from __future__ import annotations

from orcapod import PipelineJob, function_pod
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline import AsyncPipelineOrchestrator
from orcapod.sources import DictSource

seen: list[object] = []


@function_pod(output_keys="y")
def up(x: int) -> int:
    return x + 0.5  # pyright: ignore[reportReturnType]  # deliberate wrong return type: reproduces ITL-895


@function_pod(output_keys="z")
def down(y: int) -> int:
    seen.append(y)
    return y * 10


def src() -> DictSource:
    return DictSource(data=[{"k": "a", "x": 1}], tag_columns=["k"], source_id="src")


print("=== one output Data, two answers ===")
for _tag, data in up.pod(src()).iter_data():
    print(f"  as_dict()    -> {data.as_dict()}")
    print(f"  as_table()   -> {data.as_table().to_pylist()}")
    print(f"  python_schema-> {dict(data.schema())}")
    print(f"  content_hash -> {data.content_hash().to_hex()[:16]}...")
    from orcapod.core.datagrams import Data

    same_as_int = Data({"y": 1}, python_schema={"y": int}).content_hash().to_hex()[:16]
    print(
        f"  content_hash of Data({{'y': 1}}) -> {same_as_int}...  (identical: hash is of the truncated value)"
    )

print("\n=== standalone chain: what does the downstream function receive? ===")
seen.clear()
t = down.pod(up.pod(src())).as_table()
print(f"  down received y={seen!r}; z={t['z'].to_pylist()}")

for label, orch in [("sync", None), ("async", AsyncPipelineOrchestrator())]:
    seen.clear()
    job = PipelineJob(name=f"itl895_chain_{label}", database=InMemoryArrowDatabase())
    with job:
        down.pod(up.pod(src(), label="up"), label="down")
    job.run(orchestrator=orch)
    print(f"\n=== PipelineJob ({label}) ===")
    print(
        f"  down received y={seen!r}; z={job.nodes['down'].as_table()['z'].to_pylist()}; "
        f"up stored y={job.nodes['up'].as_table()['y'].to_pylist()}"
    )
