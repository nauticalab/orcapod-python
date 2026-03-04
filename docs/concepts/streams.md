# Streams

Streams are orcapod's fundamental data-flow abstraction. A stream is an immutable sequence
of (Tag, Packet) pairs over a shared schema. Every source emits a stream, every operator
consumes and produces streams, and every function pod iterates over them.

## ArrowTableStream

The concrete stream implementation is `ArrowTableStream`, backed by an immutable PyArrow
Table with explicit tag/packet column assignment.

```python
import pyarrow as pa
from orcapod.core.streams import ArrowTableStream

table = pa.table({
    "id": pa.array(["a", "b", "c"], type=pa.large_string()),
    "value": pa.array([1, 2, 3], type=pa.int64()),
})

stream = ArrowTableStream(table, tag_columns=["id"])
```

## Schema Introspection

Every stream exposes its schema as a tuple of `(tag_schema, packet_schema)`:

```python
tag_schema, packet_schema = stream.output_schema()
print(tag_schema)    # Schema({'id': str})
print(packet_schema) # Schema({'value': int})
```

## Iterating

Streams provide lazy iteration over (Tag, Packet) pairs:

```python
for tag, packet in stream.iter_packets():
    print(f"Tag: {tag.as_dict()}, Packet: {packet.as_dict()}")
```

## Materialization

Materialize a stream as a PyArrow table:

```python
table = stream.as_table()
print(table.to_pandas())
```

Use `ColumnConfig` to control which metadata columns are included:

```python
from orcapod.types import ColumnConfig

# Data columns only (default)
table = stream.as_table()

# Include source-info provenance columns
table = stream.as_table(columns=ColumnConfig(source=True))

# Include everything
table = stream.as_table(columns=ColumnConfig.all())
```

## Key Methods

| Method | Description |
|--------|-------------|
| `output_schema()` | Returns `(tag_schema, packet_schema)` |
| `keys()` | Returns the tag column names |
| `iter_packets()` | Lazy iteration over `(Tag, Packet)` pairs |
| `as_table()` | Materialize as a PyArrow table |
| `content_hash()` | Data-inclusive identity hash |
| `pipeline_hash()` | Schema-and-topology-only identity hash |

## Stream Properties

- **Immutable** — once created, a stream's data never changes.
- **Lazy** — iteration and materialization are deferred until requested.
- **Self-describing** — streams carry their schema explicitly, not by reference to a registry.
- **At least one packet column** — a stream with only tag columns raises `ValueError`.
