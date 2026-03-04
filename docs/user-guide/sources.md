# Sources

Sources are the entry points for data in orcapod. They produce streams from external data
with no upstream dependencies and establish provenance by annotating each row with source
identity.

## Source Types

orcapod provides several source implementations. All delegate internally to `ArrowTableSource`.

### ArrowTableSource

The core source implementation. Backed by an in-memory PyArrow Table:

```python
import pyarrow as pa
from orcapod import ArrowTableSource

table = pa.table({
    "patient_id": pa.array(["p1", "p2", "p3"], type=pa.large_string()),
    "age": pa.array([30, 45, 60], type=pa.int64()),
    "cholesterol": pa.array([180, 220, 260], type=pa.int64()),
})

source = ArrowTableSource(
    table,
    tag_columns=["patient_id"],
)
```

### DictSource

From a Python dictionary:

```python
from orcapod import DictSource

source = DictSource(
    data=[
        {"id": "a", "value": 1},
        {"id": "b", "value": 2},
        {"id": "c", "value": 3},
    ],
    tag_columns=["id"],
)
```

### ListSource

From a list of arbitrary Python objects, with a tag function that extracts metadata:

<!--pytest-codeblocks:skip-->
```python
from orcapod import ListSource

source = ListSource(
    name="scores",
    data=[
        {"id": "a", "score": 0.9},
        {"id": "b", "score": 0.7},
        {"id": "c", "score": 0.5},
    ],
    tag_function=lambda item, idx: {"id": item["id"]},
    expected_tag_keys=["id"],
)
```

### DataFrameSource

From a pandas DataFrame:

```python
import pandas as pd
from orcapod import DataFrameSource

df = pd.DataFrame({
    "id": ["a", "b", "c"],
    "value": [1, 2, 3],
})

source = DataFrameSource(df, tag_columns=["id"])
```

### DeltaTableSource

From a Delta Lake table on disk:

<!--pytest-codeblocks:skip-->
```python
from pathlib import Path
from orcapod.core.sources import DeltaTableSource

source = DeltaTableSource(
    Path("./data/patients"),
    tag_columns=["patient_id"],
)
```

### CSVSource

From a CSV file:

<!--pytest-codeblocks:skip-->
```python
from orcapod.core.sources import CSVSource

source = CSVSource(
    "data/patients.csv",
    tag_columns=["patient_id"],
)
```

## Tag Columns vs. Packet Columns

The `tag_columns` parameter determines which columns are tags (metadata) and which are
packets (data):

- **Tag columns** — used for joining, filtering, and routing. They carry provenance
  metadata and are the basis for stream joins.
- **Packet columns** — the data payload. All columns not listed in `tag_columns` become
  packet columns.

```python
from orcapod import DictSource

source = DictSource(
    data=[
        {"id": "a", "x": 1, "y": 3},
        {"id": "b", "x": 2, "y": 4},
    ],
    tag_columns=["id"],
)

tag_schema, packet_schema = source.output_schema()
print(tag_schema)    # Schema({'id': str})
print(packet_schema) # Schema({'x': int, 'y': int})
```

## Source Identity

Every source has a `source_id` — a canonical name used for provenance tracking:

<!--pytest-codeblocks:skip-->
```python
# Named sources: source_id from the data origin
delta_src = DeltaTableSource(Path("./patients"), tag_columns=["id"])
print(delta_src.source_id)  # "patients" (directory name)

# Unnamed sources: source_id defaults to a truncated content hash
arrow_src = ArrowTableSource(table, tag_columns=["id"])
print(arrow_src.source_id)  # "a3f7b2c1..." (content hash)

# Explicit source_id
arrow_src = ArrowTableSource(table, tag_columns=["id"], source_id="my_source")
print(arrow_src.source_id)  # "my_source"
```

## DerivedSource

A `DerivedSource` wraps the computed output of a `FunctionNode` or `OperatorNode`, reading
from their pipeline database. It represents a materialization — an intermediate result given
durable identity:

<!--pytest-codeblocks:skip-->
```python
from orcapod import DerivedSource
from orcapod.core.operators import Join

# After running a pipeline node
derived = function_node.as_source()

# Use as input to a downstream pipeline
downstream_joined = Join()(derived, other_source)
```

Derived sources serve two purposes:

1. **Semantic materialization** — domain-meaningful intermediate results (e.g., a daily
   top-3 selection) get durable identity.
2. **Pipeline decoupling** — downstream pipelines evolve independently of upstream topology.

## Source Registration

Sources can be registered in a `SourceRegistry` for provenance resolution:

<!--pytest-codeblocks:skip-->
```python
from orcapod.core.sources import SourceRegistry

registry = SourceRegistry()
source = ArrowTableSource(table, tag_columns=["id"])
# Register for provenance resolution
registry.register(source)
```

## Validation Rules

- Tag columns must exist in the table — raises `ValueError` otherwise.
- The table must have at least one packet column — raises `ValueError` if all columns are tags.
- Empty tables raise `ValueError("Table is empty")`.
