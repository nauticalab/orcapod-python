# Building Your First Pipeline

This guide walks through building a persistent, incremental pipeline using orcapod's
`Pipeline` class. You'll learn how to:

- Define a multi-step computation graph
- Persist results to a database
- Re-run with incremental computation

## The Pipeline Class

A `Pipeline` wraps your computation graph with automatic persistence. Inside a `with pipeline:`
block, all source, operator, and function pod invocations are tracked and automatically
upgraded to their persistent variants when the context exits.

```python
import pyarrow as pa
from orcapod import ArrowTableSource, FunctionPod
from orcapod.core.packet_function import PythonPacketFunction
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline import Pipeline

# Define sources
patients = ArrowTableSource(
    pa.table({
        "patient_id": pa.array(["p1", "p2", "p3"], type=pa.large_string()),
        "age": pa.array([30, 45, 60], type=pa.int64()),
    }),
    tag_columns=["patient_id"],
)

labs = ArrowTableSource(
    pa.table({
        "patient_id": pa.array(["p1", "p2", "p3"], type=pa.large_string()),
        "cholesterol": pa.array([180, 220, 260], type=pa.int64()),
    }),
    tag_columns=["patient_id"],
)

# Define computation
def risk_score(age: int, cholesterol: int) -> float:
    return age * 0.5 + cholesterol * 0.3

risk_fn = PythonPacketFunction(risk_score, output_keys="risk")
risk_pod = FunctionPod(packet_function=risk_fn)

# Build the pipeline
db = InMemoryArrowDatabase()
pipeline = Pipeline(name="risk_pipeline", pipeline_database=db)

with pipeline:
    joined = patients.join(labs, label="join_data")
    risk_pod(joined, label="compute_risk")

pipeline.run()
```

## Labels and Node Access

Every operation inside a pipeline context can be given a `label`. After compilation and
execution, access nodes by label as attributes:

```python
# Access results by label
risk_table = pipeline.compute_risk.as_table()
print(risk_table.to_pandas()[["patient_id", "risk"]])
#   patient_id  risk
# 0         p1  69.0
# 1         p2  88.5
# 2         p3 108.0
```

## Convenience Methods

Streams and sources expose convenience methods for common operators, making pipeline
construction more fluent:

```python
with pipeline:
    joined = patients.join(labs, label="join_data")
    selected = joined.select_packet_columns(["age"], label="select_age")
    renamed = selected.map_packets({"age": "patient_age"}, label="rename")
    risk_pod(renamed, label="compute")
```

Available convenience methods:

| Method | Operator |
|--------|----------|
| `.join(other)` | `Join` |
| `.semi_join(other)` | `SemiJoin` |
| `.map_tags(mapping)` | `MapTags` |
| `.map_packets(mapping)` | `MapPackets` |
| `.select_tag_columns(cols)` | `SelectTagColumns` |
| `.select_packet_columns(cols)` | `SelectPacketColumns` |
| `.drop_tag_columns(cols)` | `DropTagColumns` |
| `.drop_packet_columns(cols)` | `DropPacketColumns` |
| `.batch(batch_size=N)` | `Batch` |
| `.polars_filter(col="val")` | `PolarsFilter` |

## Persistent Storage with Delta Lake

For durable persistence, use `DeltaTableDatabase` instead of `InMemoryArrowDatabase`:

```python
from pathlib import Path
from orcapod.databases import DeltaTableDatabase

db = DeltaTableDatabase(base_path=Path("./my_pipeline_db"))
pipeline = Pipeline(name="risk_pipeline", pipeline_database=db)

with pipeline:
    joined = patients.join(labs, label="join_data")
    risk_pod(joined, label="compute_risk")

pipeline.run()
```

Results are stored as Delta Lake tables on disk and survive across process restarts.

## Incremental Computation

When you re-run a pipeline with new data, only the new rows are computed:

```python
# First run: 3 patients
pipeline.run()  # computes all 3

# Add a new patient to the source
patients_v2 = ArrowTableSource(
    pa.table({
        "patient_id": pa.array(["p1", "p2", "p3", "p4"], type=pa.large_string()),
        "age": pa.array([30, 45, 60, 25], type=pa.int64()),
    }),
    tag_columns=["patient_id"],
)

# Rebuild pipeline with updated source
pipeline2 = Pipeline(name="risk_pipeline", pipeline_database=db)
with pipeline2:
    joined = patients_v2.join(labs_v2, label="join_data")
    risk_pod(joined, label="compute_risk")

pipeline2.run()  # only p4 is computed; p1-p3 come from cache
```

## Compiled Node Types

When a pipeline compiles, each node is replaced with its persistent variant:

| Original | Persistent Variant | Cache Scoping |
|----------|-------------------|---------------|
| Leaf stream | `PersistentSourceNode` | Content hash |
| Operator call | `PersistentOperatorNode` | Content hash |
| Function pod call | `PersistentFunctionNode` | Pipeline hash (schema + topology) |

## Separate Function Database

For isolating function pod result caches from the main pipeline database:

```python
pipeline_db = DeltaTableDatabase(base_path=Path("./pipeline"))
function_db = DeltaTableDatabase(base_path=Path("./functions"))

pipeline = Pipeline(
    name="risk_pipeline",
    pipeline_database=pipeline_db,
    function_database=function_db,
)
```

## Next Steps

- [User Guide: Pipelines](../user-guide/pipelines.md) — Advanced pipeline patterns
  and composition
- [User Guide: Caching & Persistence](../user-guide/caching.md) — Deep dive into
  orcapod's three-tier caching strategy
