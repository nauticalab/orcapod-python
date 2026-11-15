"""Suspect: two iter_data passes in FunctionPodStream.as_table, upstream changed between them."""

import itertools
from typing import Any

import pyarrow as pa
from orcapod import function_pod
from orcapod.sources import ArrowTableSource

counter = itertools.count()


@function_pod(output_keys="y")
def stamp(x: int) -> int:
    """Not deterministic: a call counter (a timestamp or a random draw behaves alike)."""
    return 100 * next(counter) + x


@function_pod(output_keys="z")
def plus_one(y: int) -> int:
    return y + 1


t = pa.table(
    {"k": ["a", "b", "c"], "x": [1, 2, 3]},
    schema=pa.schema(
        [
            pa.field("k", pa.large_string(), nullable=False),
            pa.field("x", pa.int64(), nullable=False),
        ]
    ),
)
s1: Any = stamp.pod(ArrowTableSource(t, tag_columns=["k"], source_id="src"))
s2: Any = plus_one.pod(s1)
first = s2.as_table()  # table cached, content hashes not yet
print("z after first as_table:", first.column("z").to_pylist())
s1.clear_cache()  # public: the upstream is reset, s2 goes stale
print("s2 stale:", s2.is_stale)
second = s2.as_table(columns={"content_hash": True})
hasher = s2.data_context.arrow_hasher
print("z in the table:", second.column("z").to_pylist())
for i in range(second.num_rows):
    own = hasher.hash_table(second.slice(i, 1).select(["z"])).to_string()
    print(
        f"  row {i}: _content_hash is the row's own hash: {own == second.column('_content_hash')[i].as_py()}"
    )
print("z in a fresh read:", s2.as_table().column("z").to_pylist())
