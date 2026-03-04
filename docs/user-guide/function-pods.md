# Function Pods

Function pods wrap stateless **packet functions** that transform individual packets. They
are orcapod's mechanism for data synthesis — any operation that creates new computed values
goes through a function pod.

## Defining a Packet Function

A `PythonPacketFunction` wraps a regular Python function:

```python
from orcapod.core.packet_function import PythonPacketFunction

def risk_score(age: int, cholesterol: int) -> float:
    """Compute a simple risk score."""
    return age * 0.5 + cholesterol * 0.3

pf = PythonPacketFunction(
    risk_score,
    output_keys="risk",          # Name of the output column
    function_name="risk_score",  # Optional: canonical name for identity
    version="v1.0",              # Optional: version for identity
)
```

### Output Keys

`output_keys` declares the names of the output columns:

```python
# Single output
pf = PythonPacketFunction(compute_score, output_keys="score")

# Multiple outputs
def compute_stats(values: list[float]) -> tuple[float, float]:
    return sum(values) / len(values), max(values)

pf = PythonPacketFunction(compute_stats, output_keys=["mean", "max_val"])
```

### Input and Output Schemas

Packet functions introspect input parameters from the function signature and output types
from `output_keys`:

```python
print(pf.input_packet_schema)   # Schema({'age': int, 'cholesterol': int})
print(pf.output_packet_schema)  # Schema({'risk': float})
```

You can also provide explicit schemas:

```python
from orcapod.types import Schema

pf = PythonPacketFunction(
    risk_score,
    output_keys="risk",
    input_schema=Schema({"age": int, "cholesterol": int}),
    output_schema=Schema({"risk": float}),
)
```

## Creating a Function Pod

A `FunctionPod` wraps a packet function and applies it to a stream:

```python
from orcapod import FunctionPod

risk_fn = PythonPacketFunction(risk_score, output_keys="risk")
pod = FunctionPod(packet_function=risk_fn)

# Apply to a stream
result_stream = pod(input_stream)

# Or equivalently
result_stream = pod.process(input_stream)
```

## The `@function_pod` Decorator

For convenience, use the decorator to create a function pod in one step:

```python
from orcapod import function_pod

@function_pod(output_keys="risk")
def compute_risk(age: int, cholesterol: int) -> float:
    return age * 0.5 + cholesterol * 0.3

# The decorated function has a .pod attribute
result = compute_risk.pod(input_stream)
```

The decorator accepts the same parameters as `PythonPacketFunction`:

```python
@function_pod(
    output_keys=["mean", "std"],
    function_name="statistics",
    version="v2.0",
)
def compute_statistics(values: list[float]) -> tuple[float, float]:
    import statistics
    return statistics.mean(values), statistics.stdev(values)
```

## Execution Models

Function pods support two execution models:

### Lazy In-Memory (FunctionPodStream)

The default. The function pod processes each packet on demand:

```python
result = pod(input_stream)  # Returns FunctionPodStream

# Computation happens lazily during iteration
for tag, packet in result.iter_packets():
    print(packet.as_dict())
```

### Database-Backed (FunctionNode / PersistentFunctionNode)

For incremental computation with caching:

```python
from orcapod import FunctionNode, PersistentFunctionNode
from orcapod.databases import InMemoryArrowDatabase

# Non-persistent (in-memory tracking)
node = FunctionNode(
    function_pod=pod,
    input_stream=input_stream,
)

# Persistent (DB-backed with caching)
db = InMemoryArrowDatabase()
persistent_node = PersistentFunctionNode(
    function_pod=pod,
    input_stream=input_stream,
    pipeline_database=db,
)
persistent_node.run()
```

`PersistentFunctionNode` uses two-phase iteration:

1. **Phase 1** — yield cached results for inputs whose hashes are already in the database.
2. **Phase 2** — compute results for remaining inputs, store them, and yield.

## The Strict Boundary

Function pods **never** inspect or modify tags:

- They receive individual packets, not (Tag, Packet) pairs.
- Tags flow through automatically — the same tag is attached to the output.
- Function pods cannot rename columns — use operators for that.

This separation ensures that function pods are purely about data transformation, keeping
provenance clean.

## Multi-Stream Input

When given multiple input streams, a function pod joins them first (defaulting to `Join`)
before iterating:

```python
# These are equivalent:
result = pod(stream_a, stream_b)

# Explicit join + function pod
from orcapod.core.operators import Join
joined = Join()(stream_a, stream_b)
result = pod(joined)
```

## Concurrent Execution

When a packet function has an executor with `supports_concurrent_execution = True`,
`iter_packets()` materializes all remaining inputs and dispatches them concurrently:

```python
from orcapod.core.packet_function import PythonPacketFunction

pf = PythonPacketFunction(expensive_computation, output_keys="result")

# With a Ray executor for distributed computation
from orcapod.core.executors import RayExecutor
pf.executor = RayExecutor(num_cpus=4)

pod = FunctionPod(packet_function=pf)
result = pod(input_stream)  # Packets processed in parallel
```
