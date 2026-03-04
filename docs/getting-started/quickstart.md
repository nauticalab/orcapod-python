# Quickstart

This guide introduces orcapod's core concepts through a hands-on example. You'll create
sources, join them, apply a computation, and inspect the results — all with automatic
provenance tracking.

## Creating Sources

Sources are the entry points for data in orcapod. The simplest way to create one is from a
Python dictionary:

```python
from orcapod import DictSource

patients = DictSource(
    data={
        "patient_id": ["p1", "p2", "p3"],
        "age": [30, 45, 60],
    },
    tag_columns=["patient_id"],
)
```

The `tag_columns` parameter specifies which columns are **tags** (metadata used for joining
and routing) versus **packets** (the data payload). Here, `patient_id` is a tag and `age`
is a packet column.

orcapod supports many source types:

```python
import pyarrow as pa
from orcapod import ArrowTableSource, ListSource

# From a PyArrow table
arrow_src = ArrowTableSource(
    pa.table({"id": ["a", "b"], "value": [1, 2]}),
    tag_columns=["id"],
)

# From a list of dicts
list_src = ListSource(
    data=[{"id": "a", "score": 0.9}, {"id": "b", "score": 0.7}],
    tag_columns=["id"],
)
```

## Exploring Streams

Every source produces a **stream** — an immutable sequence of (Tag, Packet) pairs:

```python
stream = patients.as_stream()

# Check the schema
tag_schema, packet_schema = stream.output_schema()
print(f"Tags: {tag_schema}")      # Schema({'patient_id': str})
print(f"Packets: {packet_schema}") # Schema({'age': int})

# Iterate over entries
for tag, packet in stream.iter_packets():
    print(f"  {tag.as_dict()} → {packet.as_dict()}")
```

## Joining Streams

Use the **Join** operator to combine streams on their shared tag columns:

```python
from orcapod.core.operators import Join

labs = DictSource(
    data={
        "patient_id": ["p1", "p2", "p3"],
        "cholesterol": [180, 220, 260],
    },
    tag_columns=["patient_id"],
)

joined = Join()(patients, labs)

# The joined stream has both age and cholesterol as packet columns
tag_schema, packet_schema = joined.output_schema()
print(f"Packets: {packet_schema}")  # Schema({'age': int, 'cholesterol': int})
```

## Applying Computations

**Function pods** apply stateless computations to individual packets. Define a regular Python
function and wrap it:

```python
from orcapod import FunctionPod
from orcapod.core.packet_function import PythonPacketFunction

def risk_score(age: int, cholesterol: int) -> float:
    """Compute a simple risk score."""
    return age * 0.5 + cholesterol * 0.3

risk_fn = PythonPacketFunction(risk_score, output_keys="risk")
risk_pod = FunctionPod(packet_function=risk_fn)

# Apply the function pod to the joined stream
result = risk_pod(joined)

for tag, packet in result.iter_packets():
    print(f"  {tag.as_dict()} → {packet.as_dict()}")
# {'patient_id': 'p1'} → {'risk': 69.0}
# {'patient_id': 'p2'} → {'risk': 88.5}
# {'patient_id': 'p3'} → {'risk': 108.0}
```

You can also use the decorator syntax:

```python
from orcapod import function_pod

@function_pod(output_keys="risk")
def compute_risk(age: int, cholesterol: int) -> float:
    return age * 0.5 + cholesterol * 0.3

result = compute_risk.pod(joined)
```

## Materializing Results

Streams are lazy — data is only computed when you request it. Materialize a stream
as a PyArrow table:

```python
table = result.as_table()
print(table.to_pandas())
#   patient_id  risk
# 0         p1  69.0
# 1         p2  88.5
# 2         p3 108.0
```

## Inspecting Provenance

Every value in orcapod is traceable. Use `ColumnConfig` to inspect provenance metadata:

```python
from orcapod.types import ColumnConfig

# Include source-info columns
table = result.as_table(columns=ColumnConfig(source=True))
print(table.column_names)
# [..., '_source_risk', ...]

# Include system tags for full lineage
table = result.as_table(columns=ColumnConfig(system_tags=True))
print(table.column_names)
# [..., '_tag::source_id::...', '_tag::record_id::...', ...]
```

## Next Steps

- [Building Your First Pipeline](first-pipeline.md) — Learn how to orchestrate multi-step
  pipelines with persistence and incremental computation.
- [Concepts: Architecture Overview](../concepts/architecture.md) — Understand the design
  principles behind orcapod.
