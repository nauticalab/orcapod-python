"""PR #388 CodeRabbit round 1: more ways a load-only run could read a delivering stream.

D: a job's source slot bound to a SideEffectPodStream; job.run(load_only=True).
H: a stream outside a job that wraps a SideEffectPodStream (an operator over it).
C: a standalone OperatorJobNode without a database, run(load_only=True): its status.
"""

from typing import Any

import pyarrow as pa

from orcapod import PipelineJob, function_pod
from orcapod.core.nodes.operator_node import OperatorJobNode
from orcapod.core.operators import MapData
from orcapod.core.streams import ArrowTableStream
from orcapod.databases import InMemoryArrowDatabase
from orcapod.side_effects import tap_pod
from orcapod.sources import DictSource
from orcapod.types import CacheMode

UP: list[int] = []


def deliver_up(x: int, ctx: Any) -> None:
    UP.append(x)


@function_pod(output_keys="w")
def twice(x: int) -> int:
    return 2 * x


schema = pa.schema(
    [
        pa.field("k", pa.int64(), nullable=False),
        pa.field("x", pa.int64(), nullable=False),
    ]
)


def base() -> Any:
    return ArrowTableStream(
        pa.table({"k": [1, 2], "x": [1, 2]}, schema=schema), tag_columns=["k"]
    )


def delivering() -> Any:
    tap: Any = tap_pod(deliver_up)
    return tap.process(base())


# D
db = InMemoryArrowDatabase()
job = PipelineJob(name="j", database=db)
with job:
    src = DictSource(
        [{"k": 1, "x": 1}, {"k": 2, "x": 2}], tag_columns=["k"], source_id="s"
    )
    twice.pod(src, label="tw")
job.run()
UP.clear()
try:
    job.bind(sources={"s": delivering()})
    job.run(load_only=True)
    print(
        f"D: bound source slot, job.run(load_only=True): upstream delivered {len(UP)}"
    )
except Exception as exc:
    print(f"D: {type(exc).__name__}: {exc}")

# H
UP.clear()
wrapped: Any = MapData({"x": "x2"})(delivering())  # an operator stream outside a job
print("H: wrapped input type:", type(wrapped).__name__)
node = OperatorJobNode(
    operator=MapData({"x2": "x3"}),
    input_streams=(wrapped,),
    pipeline_database=InMemoryArrowDatabase(),
    cache_mode=CacheMode.OFF,
)
try:
    node.run(load_only=True)
    print(
        f"H: operator node over the wrapped stream, run(load_only=True): upstream delivered {len(UP)}"
    )
except Exception as exc:
    print(f"H: {type(exc).__name__}: {exc}")

# C
op = OperatorJobNode(
    operator=MapData({"x": "y"}), input_streams=(base(),), pipeline_database=None
)
view = op.run(load_only=True)
print(
    f"C: no-database operator, run(load_only=True): status {view.status}; full run holds it: {op.holds_current_run(load_only=False)}"
)
