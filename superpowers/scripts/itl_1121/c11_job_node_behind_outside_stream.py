"""CodeRabbit 4575098786 (out of scope, #380 behaviour, also on main e4b4319c): node.run()
does not schedule a job node reached through a stream outside a job; the run reads 0 rows."""

from typing import Any

import pyarrow as pa
from orcapod import function_pod
from orcapod.core.nodes.function_node import FunctionJobNode
from orcapod.core.nodes.operator_node import OperatorJobNode
from orcapod.core.operators import MapData
from orcapod.core.streams import ArrowTableStream
from orcapod.databases import InMemoryArrowDatabase


@function_pod(output_keys="y")
def inc(x: int) -> int:
    return x + 1


@function_pod(output_keys="w")
def dbl(z: int) -> int:
    return 2 * z


inc_pod: Any = inc.pod
dbl_pod: Any = dbl.pod

schema = pa.schema(
    [
        pa.field("id", pa.int64(), nullable=False),
        pa.field("x", pa.int64(), nullable=False),
    ]
)
base = ArrowTableStream(
    pa.table({"id": [1, 2, 3], "x": [1, 2, 3]}, schema=schema), tag_columns=["id"]
)
for via in ["function", "operator"]:
    first = FunctionJobNode(inc_pod, base, pipeline_database=InMemoryArrowDatabase())
    mid = MapData({"y": "z"})(
        first
    )  # an operator's stream outside a job over a job node
    if via == "function":
        second = FunctionJobNode(
            dbl_pod, mid, pipeline_database=InMemoryArrowDatabase()
        )
    else:
        second = OperatorJobNode(
            operator=MapData({"z": "q"}),
            input_streams=(mid,),
            pipeline_database=InMemoryArrowDatabase(),
        )
    print(type(mid).__name__, "first ran?", first.holds_current_run(load_only=False))
    try:
        view = second.run()
        print(
            f"{via}: second.run() rows={view.as_table().num_rows}; first holds a run: {first.holds_current_run(load_only=False)}; summary: {second.last_run}"
        )
    except Exception as exc:
        print(f"{via}: raised {type(exc).__name__}: {exc}")
