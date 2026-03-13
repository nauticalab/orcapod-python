# Function Pods

Function pods are packet-level transforms -- they take each packet in a
[stream](streams.md), apply a Python function to its values, and produce a new packet with the
function's outputs. Unlike [operators](operators.md), function pods never inspect or modify
tags. They are the primary mechanism for adding computation to an Orcapod pipeline: data
cleaning, feature extraction, model inference, or any transformation that produces new values
from existing ones.

## The `function_pod` decorator

The most common way to create a function pod is with the `@function_pod` decorator. It wraps
a plain Python function so it can be applied to streams:

```python
from orcapod import function_pod

@function_pod(output_keys="bmi")
def compute_bmi(weight: float, height: float) -> float:
    return weight / (height ** 2)
```

Key points:

- **`output_keys`** names the output packet column(s). A single string means one output
  column; a list of strings means the function returns multiple values.
- **Function parameters** must match the input stream's packet column names. Orcapod uses
  the function signature to determine which columns to read.
- **Type annotations** on parameters are used to validate schema compatibility.

The decorated function still works as a normal Python function. The pod is accessible via the
`.pod` attribute:

```python
# Call as a normal function
result = compute_bmi(weight=25.3, height=0.12)

# Access the pod for pipeline use
pod = compute_bmi.pod
```

## `FunctionPod` -- lazy in-memory execution

`FunctionPod` is the pod class created by the decorator. When you call `.process()` on it, it
returns a `FunctionPodStream` -- a lazy stream that applies the function to each packet on
demand:

```python
from orcapod import function_pod
from orcapod.sources import DictSource

@function_pod(output_keys="bmi")
def compute_bmi(weight: float, height: float) -> float:
    return weight / (height ** 2)

source = DictSource(
    data=[
        {"subject_id": "mouse_01", "weight": 25.3, "height": 0.12},
        {"subject_id": "mouse_02", "weight": 22.1, "height": 0.10},
    ],
    tag_columns=["subject_id"],
)

# Apply the function pod to the source stream
result = compute_bmi.pod.process(source)

# Inspect the output schema -- tags pass through, packets are replaced
tag_schema, packet_schema = result.output_schema()
print("Tag schema:", dict(tag_schema))
# Tag schema: {'subject_id': <class 'str'>}
print("Packet schema:", dict(packet_schema))
# Packet schema: {'bmi': <class 'float'>}

# Iterate over results
for tag, packet in result.iter_packets():
    print(f"  {tag.as_dict()} -> {packet.as_dict()}")
# {'subject_id': 'mouse_01'} -> {'bmi': 1756.9444444444446}
# {'subject_id': 'mouse_02'} -> {'bmi': 2209.9999999999995}
```

The function pod preserves tags and replaces packet columns with the function's output. If the
input stream has multiple packet columns but the function only needs some of them, Orcapod
extracts the matching columns by name.

## `FunctionNode` -- DB-backed cached execution

For persistent, cached execution, use `FunctionNode`. It wraps a function pod and an input
stream, storing results in a database. This enables two-phase iteration: on subsequent runs,
cached results are returned immediately, and only new inputs are computed.

```python
from orcapod import function_pod
from orcapod.sources import DictSource
from orcapod.nodes import FunctionNode
from orcapod.databases import InMemoryArrowDatabase

@function_pod(output_keys="bmi")
def compute_bmi(weight: float, height: float) -> float:
    return weight / (height ** 2)

source = DictSource(
    data=[
        {"subject_id": "mouse_01", "weight": 25.3, "height": 0.12},
        {"subject_id": "mouse_02", "weight": 22.1, "height": 0.10},
    ],
    tag_columns=["subject_id"],
)

db = InMemoryArrowDatabase()
node = FunctionNode(
    function_pod=compute_bmi.pod,
    input_stream=source,
    pipeline_database=db,
    result_database=db,
)

# Run the node -- computes and stores all results
node.run()

# Iterate over cached results
for tag, packet in node.iter_packets():
    print(f"  {tag.as_dict()} -> {packet.as_dict()}")
```

`FunctionNode` also provides:

- **`as_source()`** -- returns a `DerivedSource` backed by the node's stored results, which
  can be used as input to downstream pipelines
- **`get_all_records()`** -- returns the stored PyArrow Table directly

## Multiple input streams

If you pass multiple streams to a function pod, they are automatically joined (using
[Join](operators.md)) before the function is applied:

```python
result = compute_bmi.pod.process(weight_stream, height_stream)
```

The join happens on shared tag columns, and the merged packet columns are fed to the function.

## PacketFunction internals

Under the hood, the `function_pod` decorator creates a `PythonPacketFunction`, which wraps a
Python callable with input/output schema metadata. When a result database is provided, the
packet function is further wrapped in a `CachedPacketFunction` that checks the database before
calling the underlying function.

These are implementation details -- the `@function_pod` decorator and `FunctionNode` are the
primary user-facing APIs.

## How it connects to other concepts

- Function pods consume and produce [Streams](streams.md)
- [Sources](sources.md) produce the input streams that function pods transform
- [Operators](operators.md) handle structural transforms (the complement of function pods)
- Function pods participate in the [identity chain](identity.md) -- each pod's hash includes
  the function's identity and its upstream pipeline hashes
