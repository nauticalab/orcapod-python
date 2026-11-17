"""Comment 2: an operator job node's run() over a lazy output (no database, or
CacheMode.OFF) materializes it in _run_view(), outside the try that clears the node."""

import pyarrow as pa

from orcapod import function_pod
from orcapod.core.nodes.operator_node import OperatorJobNode
from orcapod.core.operators import MapData
from orcapod.core.streams import ArrowTableStream
from orcapod.databases import InMemoryArrowDatabase
from orcapod.types import CacheMode

FAIL = {"on": True}


@function_pod(output_keys="y")
def flaky(x: int) -> int:
    if FAIL["on"]:
        raise RuntimeError("boom")
    return x + 1


schema = pa.schema(
    [
        pa.field("id", pa.int64(), nullable=False),
        pa.field("x", pa.int64(), nullable=False),
    ]
)
base = ArrowTableStream(
    pa.table({"id": [1, 2], "x": [1, 2]}, schema=schema), tag_columns=["id"]
)

for label, db in [("no database", None), ("CacheMode.OFF", InMemoryArrowDatabase())]:
    FAIL["on"] = True
    upstream = flaky.pod(base)  # a pod stream outside a job: lazy, raises when read
    node = OperatorJobNode(
        operator=MapData({"y": "z"}),
        input_streams=(upstream,),
        pipeline_database=db,
        cache_mode=CacheMode.OFF,
    )
    try:
        node.run()
    except Exception as exc:
        print(f"{label}: run() raised {type(exc).__name__}: {exc}")
    print(
        f"  after the failed run: holds_current_run={node.holds_current_run(load_only=False)} is_stale={node.is_stale} "
        f"kept output installed={node._cached_output_stream is not None}"
    )  # private: what the comment is about
    try:
        print(f"  as_table() rows={node.as_table().num_rows}")
    except Exception as exc:
        print(f"  as_table() raised {type(exc).__name__}: {exc}")
    print(f"  head(): {node.head().status}")
    FAIL["on"] = False
    try:
        print(
            f"  second run() after the input recovers: rows={node.run().as_table().num_rows}"
        )
    except Exception as exc:
        print(f"  second run() raised {type(exc).__name__}: {exc}")
