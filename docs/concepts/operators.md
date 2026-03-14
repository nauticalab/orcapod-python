# Operators

Operators are structural transforms that reshape [streams](streams.md) without inspecting or
synthesizing packet values. They join, filter, batch, rename, and select columns -- operations
that affect the *structure* of the data (which rows exist, which columns are present, how
columns are named) but never compute new values from packet content. This is the key
distinction from [function pods](function-pods.md), which do the opposite: they transform
packet values but never touch tags or stream structure.

## The operator / function pod boundary

This separation is a core Orcapod design principle:

|  | Operator | Function Pod |
|---|---|---|
| Inspects packet content | Never | Yes |
| Inspects / uses tags | Yes | No |
| Can rename columns | Yes | No |
| Synthesizes new values | No | Yes |
| Stream arity | Configurable (1, 2, or N inputs) | Single in, single out |

This boundary ensures that structural operations (joins, filters) and value computations
(transformations, model inference) are cleanly separated, making pipelines easier to reason
about and optimize.

## Operator categories

### `UnaryOperator` -- single input

Takes one stream, produces one stream. Used for filtering, column selection, renaming, and
batching.

### `BinaryOperator` -- two inputs

Takes exactly two streams. Used for `MergeJoin` and `SemiJoin`.

### `NonZeroInputOperator` -- one or more inputs

Takes one or more streams. Used for `Join`, which performs an N-ary inner join.

## Available operators

### Join

N-ary inner join on shared tag columns. Requires that input streams have non-overlapping
packet columns (raises `InputValidationError` on collision). Join is **commutative** -- the
order of input streams does not affect the result.

```python
from orcapod.sources import DictSource
from orcapod.operators import Join

subjects = DictSource(
    data=[
        {"subject_id": "mouse_01", "age": 12},
        {"subject_id": "mouse_02", "age": 8},
    ],
    tag_columns=["subject_id"],
)

measurements = DictSource(
    data=[
        {"subject_id": "mouse_01", "weight": 25.3},
        {"subject_id": "mouse_02", "weight": 22.1},
    ],
    tag_columns=["subject_id"],
)

join = Join()
joined = join.process(subjects, measurements)
print(joined.as_table().to_pandas())
#   subject_id  age  weight
# 0   mouse_01   12    25.3
# 1   mouse_02    8    22.1
```

### MergeJoin

Binary join that handles colliding packet columns by merging their values into sorted
`list[T]`. Both inputs must have the same type for any colliding packet columns. MergeJoin
is **commutative** -- the order of the two input streams does not affect the result.

### SemiJoin

Binary join that filters the left stream to only include rows whose tags match the right
stream. The right stream's packet columns are discarded. SemiJoin is **not commutative** --
the order of inputs matters. The first stream is the one being filtered; the second stream
provides the set of matching tags.

### Batch

Groups all rows into a single row (or fixed-size batches), converting column types from `T`
to `list[T]`. Useful for aggregation-style processing.

```python
from orcapod.sources import DictSource
from orcapod.operators import Batch

source = DictSource(
    data=[
        {"subject_id": "mouse_01", "age": 12},
        {"subject_id": "mouse_02", "age": 8},
    ],
    tag_columns=["subject_id"],
)

batch = Batch()
batched = batch.process(source)
for tag, packet in batched.iter_packets():
    print("Tags:", tag.as_dict())
    # Tags: {'subject_id': ['mouse_01', 'mouse_02']}
    print("Packet:", packet.as_dict())
    # Packet: {'age': [12, 8]}
```

Pass `batch_size=N` to create fixed-size batches instead of grouping everything:

```python
batch = Batch(batch_size=10, drop_partial_batch=False)
```

### Column selection

Four operators for including or excluding columns:

- **`SelectTagColumns(columns=["col1", "col2"])`** -- keep only the specified tag columns
- **`SelectPacketColumns(columns=["col1", "col2"])`** -- keep only the specified packet columns
- **`DropTagColumns(columns=["col1"])`** -- remove the specified tag columns
- **`DropPacketColumns(columns=["col1"])`** -- remove the specified packet columns

```python
from orcapod.operators import SelectPacketColumns

select = SelectPacketColumns(columns=["weight"])
result = select.process(source)
print(result.keys()[1])  # ('weight',)
```

### Column renaming

- **`MapTags(mapping={"old_name": "new_name"})`** -- rename tag columns
- **`MapPackets(mapping={"old_name": "new_name"})`** -- rename packet columns

### PolarsFilter

Filter rows using Polars expressions:

```python
import polars as pl
from orcapod.operators import PolarsFilter

filt = PolarsFilter(predicates=[pl.col("age") > 10])
filtered = filt.process(source)
for tag, pkt in filtered.iter_packets():
    print(f"{tag.as_dict()} -> {pkt.as_dict()}")
# {'subject_id': 'mouse_01'} -> {'age': 12, 'weight': 25.3}
# {'subject_id': 'mouse_03'} -> {'age': 15, 'weight': 27.8}
```

You can also filter by exact values using `constraints`:

```python
filt = PolarsFilter(constraints={"subject_id": "mouse_01"})
```

## Using operators

All operators follow the same interface. Call `.process()` with one or more input streams:

```python
operator = Join()
result_stream = operator.process(stream_a, stream_b)
```

All standard operators also support `__call__` as a shorthand for `.process()`, so you can
write:

```python
result_stream = Join()(stream_a, stream_b)
```

The result is a new [stream](streams.md) that you can inspect, iterate, or pass to further
operators or function pods.

## How it connects to other concepts

- Operators consume and produce [Streams](streams.md)
- [Sources](sources.md) produce the initial streams that operators transform
- [Function Pods](function-pods.md) handle value-level transforms (the complement of operators)
- Operators participate in the [identity chain](identity.md) -- each operator's hash includes
  its own identity plus its upstream hashes
