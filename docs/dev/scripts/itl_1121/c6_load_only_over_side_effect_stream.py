"""Comment 6: a SideEffectJobNode over a standalone SideEffectPodStream, run with
load_only=True, delivers the upstream stream's side effects while reading it.

Compared with the general case: a FunctionJobNode over a FunctionPodStream (a stream
outside a job), run with load_only=True, computes the upstream function while reading it.
"""

from typing import Any

import pyarrow as pa

from orcapod import function_pod
from orcapod.core.nodes.function_node import FunctionJobNode
from orcapod.core.streams import ArrowTableStream
from orcapod.databases import InMemoryArrowDatabase
from orcapod.side_effects import SideEffectJobNode, tap_pod

UP: list[int] = []
OWN: list[int] = []
CALLS: list[int] = []


def deliver_up(x: int, ctx: Any) -> None:
    UP.append(x)


def deliver_own(x: int, ctx: Any) -> None:
    OWN.append(x)


@function_pod(output_keys="y")
def inc(x: int) -> int:
    CALLS.append(x)
    return x + 1


@function_pod(output_keys="z")
def dbl(y: int) -> int:
    return 2 * y


schema = pa.schema(
    [
        pa.field("k", pa.int64(), nullable=False),
        pa.field("x", pa.int64(), nullable=False),
    ]
)
base = ArrowTableStream(
    pa.table({"k": [1, 2, 3], "x": [1, 2, 3]}, schema=schema), tag_columns=["k"]
)

tap_up: Any = tap_pod(deliver_up)
upstream: Any = tap_up.process(base)  # a SideEffectPodStream, outside a job
print("input type:", type(upstream).__name__)
tap_own: Any = tap_pod(deliver_own)
node = SideEffectJobNode(tap_own, upstream)
# Before ITL-1121 this delivered every row of the upstream stream (3, then 6 after
# clear_cache()); since then the load-only run refuses the input before reading it.
try:
    view = node.run(load_only=True)
    print(f"side-effect node, run(load_only=True): rows {view.as_table().num_rows}")
except ValueError as exc:
    print(f"side-effect node, run(load_only=True) refused: {exc}")
print(f"  upstream delivered {len(UP)}, own delivered {len(OWN)}")

# A function pod's stream outside a job computes when read. Before the developer's decision
# (November 14, 2026, "Refuse is consistent") a load-only run read it and called the
# function; since then it is refused as well.
fn_upstream: Any = inc.pod(base)  # a FunctionPodStream, outside a job
dbl_pod: Any = dbl.pod
fnode = FunctionJobNode(dbl_pod, fn_upstream, pipeline_database=InMemoryArrowDatabase())
try:
    fnode.run(load_only=True)
    print(
        f"function node over a pod stream, run(load_only=True): upstream calls {len(CALLS)}"
    )
except ValueError as exc:
    print(f"function node over a pod stream, run(load_only=True) refused: {exc}")
print(f"  upstream function calls: {len(CALLS)}")
