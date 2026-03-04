# Pipelines

The `Pipeline` class orchestrates multi-step computation graphs with automatic persistence,
incremental computation, and graph tracking.

## Pipeline Lifecycle

A pipeline has three phases:

### 1. Recording Phase

Inside a `with pipeline:` block, all pod invocations are captured as non-persistent nodes:

```python
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline import Pipeline

db = InMemoryArrowDatabase()
pipeline = Pipeline(name="my_pipeline", pipeline_database=db)

with pipeline:
    joined = source_a.join(source_b, label="join_data")
    result = pod(joined, label="compute")
```

### 2. Compilation Phase

On context exit (if `auto_compile=True`, which is the default), `compile()` walks the
graph in topological order and replaces every node with its persistent variant:

| Non-persistent | Persistent | Scoped by |
|----------------|-----------|-----------|
| Leaf stream | `PersistentSourceNode` | Content hash |
| Operator call | `PersistentOperatorNode` | Content hash |
| Function pod call | `PersistentFunctionNode` | Pipeline hash |

### 3. Execution Phase

`pipeline.run()` executes all compiled nodes in topological order:

```python
pipeline.run()

# Access results by label
result_table = pipeline.compute.as_table()
```

## Labels

Every operation inside a pipeline context can be labeled. Labels become attributes on the
pipeline object:

```python
with pipeline:
    joined = source_a.join(source_b, label="join_data")
    risk_stream = risk_pod(joined, label="compute_risk")
    cat_pod(risk_stream, label="categorize")

pipeline.run()

# Access by label
pipeline.join_data       # -> PersistentOperatorNode
pipeline.compute_risk    # -> PersistentFunctionNode
pipeline.categorize      # -> PersistentFunctionNode
```

Labels are disambiguated by content hash on collision during incremental compilation.

## Inspecting the Graph

After compilation, inspect the compiled nodes:

```python
for name, node in pipeline.compiled_nodes.items():
    print(f"{name}: {type(node).__name__}")
```

## Graph Tracking

All pod invocations are automatically recorded by a global `BasicTrackerManager`. The user
writes normal imperative code, and the computation graph is captured behind the scenes:

```python
with pipeline:
    # These calls are transparently tracked
    joined = source_a.join(source_b)
    result = pod(joined)
    # The graph is automatically built:
    # source_a -> Join -> pod -> result
```

## Incremental Compilation

Compilation is incremental — re-entering the context, adding more operations, and compiling
again preserves existing persistent nodes:

```python
pipeline = Pipeline(name="my_pipeline", pipeline_database=db)

# First round
with pipeline:
    joined = source_a.join(source_b, label="join")
    pod_a(joined, label="step_a")

pipeline.run()

# Second round: adds step_b without recomputing step_a
with pipeline:
    joined = source_a.join(source_b, label="join")
    pod_a(joined, label="step_a")
    pod_b(pipeline.step_a, label="step_b")  # builds on previous

pipeline.run()  # only step_b is new
```

## Database Configuration

### Single Database

The simplest setup uses one database for everything:

```python
db = InMemoryArrowDatabase()
pipeline = Pipeline(name="my_pipeline", pipeline_database=db)
```

### Separate Function Database

Isolate function pod result caches from the main pipeline database:

```python
from orcapod.databases import DeltaTableDatabase

pipeline_db = DeltaTableDatabase(base_path="./pipeline")
function_db = DeltaTableDatabase(base_path="./functions")

pipeline = Pipeline(
    name="my_pipeline",
    pipeline_database=pipeline_db,
    function_database=function_db,
)
```

When `function_database=None`, function results are stored under
`{pipeline_name}/_results/` in the pipeline database.

## Pipeline Composition

Pipelines can be composed across boundaries:

### Cross-Pipeline References

Pipeline B can use Pipeline A's compiled nodes as input:

```python
pipeline_a.run()
# Use pipeline_a's output as input to pipeline_b
with pipeline_b:
    pod(pipeline_a.step_a, label="next_step")
```

### Chain Detachment with `.as_source()`

Create a `DerivedSource` from a persistent node, breaking the upstream Merkle chain:

```python
pipeline_a.run()
derived = pipeline_a.step_a.as_source()

# downstream pipeline is independent of pipeline_a's topology
with pipeline_b:
    pod(derived, label="process")
```

## Persistence Backends

| Backend | Durability | Use Case |
|---------|-----------|----------|
| `InMemoryArrowDatabase` | Process lifetime | Testing, exploration |
| `DeltaTableDatabase` | Disk (Delta Lake) | Production, reusable pipelines |
