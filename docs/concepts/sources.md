# Sources

Sources are the entry points for external data into an Orcapod pipeline. Every pipeline begins
with one or more sources that load raw data -- from Python dicts, lists, CSV files, Delta Lake
tables, or Pandas DataFrames -- and present it as an immutable
[stream](streams.md) of (Tag, Packet) pairs. Sources also attach provenance metadata
(source-info columns and system tag columns) so that every downstream value can be traced back
to its origin.

## Key classes

### `RootSource` (abstract base)

All sources inherit from `RootSource`. A root source is a pure stream with no upstream
dependencies -- it sits at the root of the computational graph. Key properties:

- `source.producer` returns `None` (no upstream pod)
- `source.upstreams` is always an empty tuple
- `source.source_id` is a canonical name used for provenance tracking and the source registry

### `ArrowTableSource`

The core source implementation. All other source types (CSV, dict, list, DataFrame, Delta)
delegate to `ArrowTableSource` internally. It accepts a PyArrow Table directly and wraps it
as a stream.

### Convenience wrappers

These sources convert common Python data formats into an `ArrowTableSource` under the hood:

| Source | Input type | Notes |
|---|---|---|
| `DictSource` | `list[dict]` | Each dict becomes one (Tag, Packet) pair |
| `ListSource` | `list[Any]` | Each element stored under a named packet column |
| `DataFrameSource` | Pandas `DataFrame` | Converts via Arrow |
| `CSVSource` | File path (string) | Reads CSV into Arrow |
| `DeltaTableSource` | File path (string) | Reads Delta Lake table |

### `DerivedSource`

A `DerivedSource` reads the computed results of a [FunctionNode or OperatorNode](function-pods.md)
from its database and presents them as a new source. This is useful for chaining pipeline
stages: run a node, then use its output as input to a new pipeline.

## How sources add provenance columns

When you create a source, Orcapod automatically adds two kinds of hidden columns to track
data lineage:

**Source-info columns** (prefix `_source_`) store a provenance token for each packet column.
For example, a packet column `weight` gets a companion `_source_weight` column. These tokens
identify which source originally produced each value.

**System tag columns** (prefix `_tag::`) track which source contributed each row. These
columns are used internally during [joins](operators.md) to maintain provenance through
multi-stream operations.

These columns are hidden by default. You can reveal them using `ColumnConfig`:

```python
# Show source-info columns
table = source.as_table(columns={"source": True})

# Show system tag columns
tag_schema, packet_schema = source.output_schema(columns={"system_tags": True})

# Show everything
table = source.as_table(all_info=True)
```

## Code example

Create a `DictSource`, inspect its schema, and iterate over its stream:

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

# Inspect the schema
tag_schema, packet_schema = source.output_schema()
print("Tag schema:", dict(tag_schema))
# Tag schema: {'subject_id': <class 'str'>}
print("Packet schema:", dict(packet_schema))
# Packet schema: {'age': <class 'int'>, 'weight': <class 'float'>}

# Get column names
tag_keys, packet_keys = source.keys()
print("Tag keys:", tag_keys)    # ('subject_id',)
print("Packet keys:", packet_keys)  # ('age', 'weight')

# Iterate over (Tag, Packet) pairs
for tag, packet in source.iter_packets():
    print(f"  Tag: {tag.as_dict()}, Packet: {packet.as_dict()}")
# Tag: {'subject_id': 'mouse_01'}, Packet: {'age': 12, 'weight': 25.3}
# Tag: {'subject_id': 'mouse_02'}, Packet: {'age': 8, 'weight': 22.1}
# Tag: {'subject_id': 'mouse_03'}, Packet: {'age': 15, 'weight': 27.8}

# Convert to a PyArrow table
table = source.as_table()
print(table.to_pandas())
#   subject_id  age  weight
# 0   mouse_01   12    25.3
# 1   mouse_02    8    22.1
# 2   mouse_03   15    27.8
```

## How it connects to other concepts

- Sources produce [Streams](streams.md) -- immutable sequences of (Tag, Packet) pairs
- Streams flow into [Operators](operators.md) for structural transforms (joins, filters,
  column selection)
- Streams flow into [Function Pods](function-pods.md) for value-level transforms
- Every source has a `content_hash()` and `pipeline_hash()` -- see
  [Identity & Hashing](identity.md) for how these work
