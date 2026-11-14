"""PR #388 CodeRabbit round 2 (review of 9fc663d7): the load-only input traversal.

A (4575874462): an ArrowTableStream holds its table and never reads its upstreams, yet a
  load-only run over one built with upstreams=(a function pod's stream,) is refused.
B (4575874471): a blueprint SideEffectNode outside a job delivers when it is read; a
  load-only run over it (a node's input, or bound to a source slot) is not refused.
"""

from typing import Any

import pyarrow as pa

from orcapod import PipelineJob, function_pod
from orcapod.core.nodes.function_node import FunctionJobNode
from orcapod.core.streams import ArrowTableStream
from orcapod.databases import InMemoryArrowDatabase
from orcapod.side_effects import SideEffectNode, tap_pod
from orcapod.sources import DictSource

UP: list[int] = []
CALLS: list[int] = []


def deliver_up(x: int, ctx: Any) -> None:
    UP.append(x)


@function_pod(output_keys="y")
def inc(x: int) -> int:
    CALLS.append(x)
    return x + 1


@function_pod(output_keys="w")
def twice(x: int) -> int:
    return 2 * x


inc_pod: Any = inc.pod
twice_pod: Any = twice.pod
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


def blueprint_tap() -> Any:
    tap: Any = tap_pod(deliver_up)
    return SideEffectNode(tap, base())


# A
held = ArrowTableStream(
    pa.table({"k": [1, 2], "x": [1, 2]}, schema=schema),
    tag_columns=["k"],
    upstreams=(inc_pod(base()),),
)
node = FunctionJobNode(twice_pod, held, pipeline_database=InMemoryArrowDatabase())
try:
    view = node.run(load_only=True)
    print(
        f"A: run(load_only=True) accepted: status {view.status}; function calls {len(CALLS)}"
    )
except ValueError as exc:
    print(f"A: refused: {exc}; function calls {len(CALLS)}")

# B, as a node's input
UP.clear()
node = FunctionJobNode(
    twice_pod, blueprint_tap(), pipeline_database=InMemoryArrowDatabase()
)
try:
    node.run(load_only=True)
    print(f"B input: run(load_only=True) ran: delivered {len(UP)}")
except ValueError as exc:
    print(f"B input: refused: {exc}; delivered {len(UP)}")

# B, bound to a source slot
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
    job.bind(sources={"s": blueprint_tap()})
    job.run(load_only=True)
    print(f"B slot: job.run(load_only=True) ran: delivered {len(UP)}")
except ValueError as exc:
    print(f"B slot: refused: {exc}; delivered {len(UP)}")
