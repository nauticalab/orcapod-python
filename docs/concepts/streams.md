# Streams

A stream is an immutable sequence of (Tag, Packet) pairs backed by a PyArrow Table. Streams
are the universal data currency in Orcapod -- every [source](sources.md) produces a stream,
every [operator](operators.md) consumes and produces streams, and every
[function pod](function-pods.md) transforms packets within a stream. Immutability guarantees
that once a stream is created, its data cannot change, which is essential for reproducible
pipelines.

## Tag columns vs Packet columns

Every stream divides its columns into two groups:

**Tag columns** are join keys and metadata. They identify *which* record you are looking at
(e.g., `subject_id`, `session_date`). Operators like [Join](operators.md) match rows across
streams using shared tag columns.

**Packet columns** are the data payload. They hold the actual values being processed
(e.g., `age`, `weight`, `spike_count`). [Function pods](function-pods.md) read packet
columns as function inputs and write new packet columns as outputs.

This separation is enforced throughout the framework:

- Operators inspect and restructure tags but never look inside packets
- Function pods inspect and transform packets but never look at tags

## Key classes

### `ArrowTableStream`

The primary stream implementation. Wraps a PyArrow Table with designated tag and packet
columns. Created internally by sources and operators -- you rarely construct one directly.

### `StreamBase`

Abstract base class providing the stream interface. Both `ArrowTableStream` and stream-like
objects (sources, function pod streams, nodes) inherit from it.

## Core methods

Every stream exposes four key methods:

### `output_schema()`

Returns the `(tag_schema, packet_schema)` tuple describing column names and their Python types:

```python
tag_schema, packet_schema = stream.output_schema()
print(dict(tag_schema))    # {'subject_id': <class 'str'>}
print(dict(packet_schema)) # {'age': <class 'int'>, 'weight': <class 'float'>}
```

### `keys()`

Returns column names as `(tag_keys, packet_keys)`:

```python
tag_keys, packet_keys = stream.keys()
# tag_keys = ('subject_id',)
# packet_keys = ('age', 'weight')
```

### `iter_packets()`

Iterates over (Tag, Packet) pairs. Each Tag and Packet is an immutable datagram that you can
inspect with `.as_dict()`:

```python
for tag, packet in stream.iter_packets():
    print(tag.as_dict())    # {'subject_id': 'mouse_01'}
    print(packet.as_dict()) # {'age': 12, 'weight': 25.3}
```

### `as_table()`

Returns the full stream as a PyArrow Table, which integrates with Pandas, Polars, and other
Arrow-compatible tools:

```python
table = stream.as_table()
df = table.to_pandas()
```

## Controlling column visibility with `ColumnConfig`

By default, streams only expose user-facing tag and packet columns. Orcapod also maintains
hidden columns for provenance tracking and metadata. Use `ColumnConfig` (or the `all_info`
shortcut) to control which column groups are included.

| Config field | What it reveals | Column prefix |
|---|---|---|
| `system_tags` | System tag columns (provenance tracking) | `_tag::` |
| `source` | Source-info columns (per-packet provenance tokens) | `_source_` |
| `context` | Data context column | `_context_key` |
| `content_hash` | Content hash column | `_content_hash` |
| `sort_by_tags` | Sort rows by tag columns | (ordering only) |

Pass config as a dict or a `ColumnConfig` object:

```python
from orcapod.sources import DictSource

source = DictSource(
    data=[
        {"subject_id": "mouse_01", "age": 12, "weight": 25.3},
        {"subject_id": "mouse_02", "age": 8, "weight": 22.1},
    ],
    tag_columns=["subject_id"],
)

# Default: user-facing columns only
table = source.as_table()
print(table.column_names)
# ['subject_id', 'age', 'weight']

# Include source-info columns
table = source.as_table(columns={"source": True})
print(table.column_names)
# ['subject_id', 'age', 'weight', '_source_age', '_source_weight']

# Include everything
table = source.as_table(all_info=True)
print(table.column_names)
# ['subject_id', 'age', 'weight', '_tag_source_id::...', '_tag_record_id::...',
#  '_content_hash', '_context_key', '_source_age', '_source_weight']
```

## Code example

Inspect a stream produced by a source:

```python
from orcapod.sources import DictSource

source = DictSource(
    data=[
        {"subject_id": "mouse_01", "age": 12, "weight": 25.3},
        {"subject_id": "mouse_02", "age": 8, "weight": 22.1},
        {"subject_id": "mouse_03", "age": 15, "weight": 27.8},
    ],
    tag_columns=["subject_id"],
)

# Schema inspection
tag_schema, packet_schema = source.output_schema()
print("Tag schema:", dict(tag_schema))
# Tag schema: {'subject_id': <class 'str'>}
print("Packet schema:", dict(packet_schema))
# Packet schema: {'age': <class 'int'>, 'weight': <class 'float'>}

# Iterate over (Tag, Packet) pairs
for tag, packet in source.iter_packets():
    print(f"  {tag.as_dict()} -> {packet.as_dict()}")
# {'subject_id': 'mouse_01'} -> {'age': 12, 'weight': 25.3}
# {'subject_id': 'mouse_02'} -> {'age': 8, 'weight': 22.1}
# {'subject_id': 'mouse_03'} -> {'age': 15, 'weight': 27.8}

# Convert to a PyArrow table (interops with Pandas)
table = source.as_table()
print(table.to_pandas())
#   subject_id  age  weight
# 0   mouse_01   12    25.3
# 1   mouse_02    8    22.1
# 2   mouse_03   15    27.8
```

## How it connects to other concepts

- [Sources](sources.md) produce streams from external data
- [Operators](operators.md) consume one or more streams and produce a new stream
- [Function Pods](function-pods.md) transform packet values within a stream
- Every stream carries [identity hashes](identity.md) for deduplication and DB-scoping
