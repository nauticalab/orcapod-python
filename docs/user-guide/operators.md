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
from orcapod.core.operators import Join

joined = Join()(source_a, source_b)
# Or with more inputs
joined = Join()(source_a, source_b, source_c)
```

**Properties:**

- **Commutative** — `Join()(A, B)` produces the same result as `Join()(B, A)`.
- Non-overlapping packet columns are required — raises `InputValidationError` on collision.
- Tag schema is the union of all input tag schemas.
- Packet schema is the union of all input packet schemas.

## MergeJoin

Binary inner join that handles colliding packet columns by merging values into sorted lists:

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

```python
from orcapod.core.operators import SemiJoin

filtered = SemiJoin()(left_stream, right_stream)
```

**Properties:**

- **Non-commutative** — `SemiJoin(A, B) != SemiJoin(B, A)`.
- Output schema matches the left stream exactly.

## Batch

Groups rows into batches of configurable size:

```python
from orcapod.core.operators import Batch

batched = Batch(batch_size=3)(input_stream)

# With incomplete batch dropping
batched = Batch(batch_size=3, drop_incomplete=True)(input_stream)
```

**Properties:**

- All column types become `list[T]` in the output.
- Optionally drops the final batch if it's smaller than `batch_size`.

## Column Selection

Keep or remove specific tag or packet columns:

```python
from orcapod.core.operators import (
    SelectTagColumns,
    SelectPacketColumns,
    DropTagColumns,
    DropPacketColumns,
)

# Keep only specified columns
selected = SelectPacketColumns(columns=["age", "score"])(stream)
selected_tags = SelectTagColumns(columns=["id"])(stream)

# Remove specified columns
dropped = DropPacketColumns(columns=["temp_col"])(stream)
dropped_tags = DropTagColumns(columns=["debug_tag"])(stream)
```

`SelectTagColumns` and `SelectPacketColumns` accept an optional `strict` parameter that
raises on missing columns.

`DropPacketColumns` automatically removes associated source-info columns.

## Column Renaming

Rename tag or packet columns via a mapping:

```python
from orcapod.core.operators import MapTags, MapPackets

renamed = MapPackets(mapping={"old_name": "new_name"})(stream)
renamed_tags = MapTags(mapping={"old_tag": "new_tag"})(stream)

# Drop unmapped columns
renamed = MapPackets(
    mapping={"keep_col": "renamed_col"},
    drop_unmapped=True,
)(stream)
```

`MapPackets` automatically renames associated source-info columns.

## PolarsFilter

Filter rows using Polars expressions:

```python
from orcapod.core.operators import PolarsFilter

# Filter by column value
filtered = PolarsFilter(age=30)(stream)

# Output schema is unchanged from input
```

## Convenience Methods

Streams and sources expose convenience methods for all operators:

```python
# Instead of:
from orcapod.core.operators import Join, SelectPacketColumns, MapPackets

joined = Join()(source_a, source_b)
selected = SelectPacketColumns(columns=["age"])(joined)
renamed = MapPackets(mapping={"age": "patient_age"})(selected)

# Write:
joined = source_a.join(source_b)
selected = joined.select_packet_columns(["age"])
renamed = selected.map_packets({"age": "patient_age"})
```

Inside a pipeline context, pass `label` to track each step:

```python
with pipeline:
    joined = source_a.join(source_b, label="join_data")
    selected = joined.select_packet_columns(["age"], label="select_age")
```

## OperatorNode

The `OperatorNode` is the database-backed counterpart for operators:

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
