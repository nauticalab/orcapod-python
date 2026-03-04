# Operators

Operators are structural transformers that reshape streams without synthesizing new packet
values. Every output value is traceable to a concrete input value. Operators handle joins,
filters, projections, renames, and batching.

## Operator Types

Operators are organized by input arity:

| Base Class | Arity | Operators |
|-----------|-------|-----------|
| `UnaryOperator` | Exactly 1 | Batch, Select/Drop columns, Map, PolarsFilter |
| `BinaryOperator` | Exactly 2 | MergeJoin, SemiJoin |
| `NonZeroInputOperator` | 1 or more | Join |

## Join

Variable-arity inner join on shared tag columns:

```python
from orcapod import DictSource
from orcapod.core.operators import Join

source_a = DictSource(
    data=[{"id": "a", "x": 1}, {"id": "b", "x": 2}, {"id": "c", "x": 3}],
    tag_columns=["id"],
)
source_b = DictSource(
    data=[{"id": "a", "y": 4}, {"id": "b", "y": 5}, {"id": "c", "y": 6}],
    tag_columns=["id"],
)

joined = Join()(source_a, source_b)
```

**Properties:**

- **Commutative** — `Join()(A, B)` produces the same result as `Join()(B, A)`.
- Non-overlapping packet columns are required — raises `InputValidationError` on collision.
- Tag schema is the union of all input tag schemas.
- Packet schema is the union of all input packet schemas.

## MergeJoin

Binary inner join that handles colliding packet columns by merging values into sorted lists:

<!--pytest-codeblocks:skip-->
```python
from orcapod.core.operators import MergeJoin

merged = MergeJoin()(stream_a, stream_b)
```

**Properties:**

- **Commutative** — commutativity achieved by sorting merged values.
- Colliding columns must have identical types.
- Colliding columns become `list[T]` in the output.
- Non-colliding columns remain as scalars.

**Example:**

```
Stream A: {id: "p1", score: 0.9}
Stream B: {id: "p1", score: 0.7}

MergeJoin result: {id: "p1", score: [0.7, 0.9]}  # sorted
```

## SemiJoin

Binary semi-join: returns entries from the left stream that match on overlapping columns
in the right stream:

<!--pytest-codeblocks:skip-->
```python
from orcapod.core.operators import SemiJoin

filtered = SemiJoin()(left_stream, right_stream)
```

**Properties:**

- **Non-commutative** — `SemiJoin(A, B) != SemiJoin(B, A)`.
- Output schema matches the left stream exactly.

## Batch

Groups rows into batches of configurable size:

<!--pytest-codeblocks:skip-->
```python
from orcapod.core.operators import Batch

batched = Batch(batch_size=3)(input_stream)

# With incomplete batch dropping
batched = Batch(batch_size=3, drop_incomplete=True)(input_stream)
```

## Column Selection

Keep or remove specific tag or packet columns:

<!--pytest-codeblocks:cont-->
```python
from orcapod.core.operators import (
    SelectTagColumns,
    SelectPacketColumns,
    DropTagColumns,
    DropPacketColumns,
)

stream = source_a  # Sources implement the stream protocol

# Keep only specified columns
selected = SelectPacketColumns(columns=["x"])(stream)

# Remove specified columns
dropped = DropPacketColumns(columns=["x"])(stream)
```

`SelectTagColumns` and `SelectPacketColumns` accept an optional `strict` parameter that
raises on missing columns.

`DropPacketColumns` automatically removes associated source-info columns.

## Column Renaming

Rename tag or packet columns via a mapping:

<!--pytest-codeblocks:cont-->
```python
from orcapod.core.operators import MapTags, MapPackets

renamed = MapPackets(mapping={"x": "new_x"})(stream)
renamed_tags = MapTags(mapping={"id": "new_id"})(stream)
```

`MapPackets` automatically renames associated source-info columns.

## PolarsFilter

Filter rows using Polars expressions:

<!--pytest-codeblocks:skip-->
```python
from orcapod.core.operators import PolarsFilter

# Filter by column value
filtered = PolarsFilter(age=30)(stream)

# Output schema is unchanged from input
```

## Convenience Methods

Streams and sources expose convenience methods for all operators:

<!--pytest-codeblocks:cont-->
```python
# Instead of explicit operator construction:
joined = source_a.join(source_b)
selected = joined.select_packet_columns(["x"])
renamed = selected.map_packets({"x": "patient_x"})
```

Inside a pipeline context, pass `label` to track each step:

<!--pytest-codeblocks:skip-->
```python
with pipeline:
    joined = source_a.join(source_b, label="join_data")
    selected = joined.select_packet_columns(["age"], label="select_age")
```

## OperatorNode

The `OperatorNode` is the database-backed counterpart for operators:

<!--pytest-codeblocks:skip-->
```python
from orcapod.core.operator_node import OperatorNode, PersistentOperatorNode
from orcapod.databases import InMemoryArrowDatabase
from orcapod.types import CacheMode

# Non-persistent
node = OperatorNode(
    operator=Join(),
    input_streams=[source_a, source_b],
)

# Persistent with cache logging
db = InMemoryArrowDatabase()
persistent = PersistentOperatorNode(
    operator=Join(),
    input_streams=[source_a, source_b],
    pipeline_database=db,
    cache_mode=CacheMode.LOG,
)
persistent.run()
```

See [Caching & Persistence](caching.md) for details on operator cache modes.
