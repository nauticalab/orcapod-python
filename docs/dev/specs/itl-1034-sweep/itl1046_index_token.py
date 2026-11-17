"""Suspect: Index / Pick build the element token as f'{old_src}[{i}]' even when old_src is a list."""

import pyarrow as pa
from orcapod.core.operators import Batch, GroupBy, Index
from orcapod.sources import ArrowTableSource

t = pa.table(
    {"g": ["a", "a", "b"], "x": [1, 2, 3]},
    schema=pa.schema(
        [
            pa.field("g", pa.large_string(), nullable=False),
            pa.field("x", pa.int64(), nullable=False),
        ]
    ),
)
src = ArrowTableSource(t, tag_columns=["g"], source_id="src", record_id_column="x")
for name, stream in [
    ("Batch(2)", Batch(2)(src)),
    ("GroupBy(g)", GroupBy(by=["g"])(src)),
]:
    print(
        name,
        "input tokens:",
        stream.as_table(columns={"source": True}).column("_source_x").to_pylist(),
    )
    out = Index("x", 0, out="first")(stream)
    tab = out.as_table(columns={"source": True})
    print("  Index(x, 0, out='first'): first =", tab.column("first").to_pylist())
    print("  _source_first =", tab.column("_source_first").to_pylist())
    print(
        "  element's own token =",
        [
            tok[0] if tok else None
            for tok in stream.as_table(columns={"source": True})
            .column("_source_x")
            .to_pylist()
        ],
    )
