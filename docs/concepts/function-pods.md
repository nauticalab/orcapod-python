# Function Pods

Function pods are data-level transforms -- they take each data in a
[stream](streams.md), apply a Python function to its values, and produce a new data with the
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

- **`output_keys`** names the output data column(s). A single string means one output
  column; a list of strings means the function returns multiple values.
- **Function parameters** must match the input stream's data columns exactly: one data
  column per parameter, with the same names. A parameter with a default value may be left out
  of the stream, and the default is used. See [Input columns must match exactly](#input-columns-must-match-exactly).
- **Parameter types** are used to validate schema compatibility. Give each parameter a type
  annotation, or supply its type through the decorator's `input_schema` mapping. A parameter
  with neither raises `ValueError` when the pod is created.

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
returns a `FunctionPodStream` -- a lazy stream that applies the function to each data on
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
result = compute_bmi.pod(source)  # shorthand for compute_bmi.pod.process(source)

# Inspect the output schema -- tags pass through, data are replaced
tag_schema, data_schema = result.output_schema()
print("Tag schema:", dict(tag_schema))
# Tag schema: {'subject_id': <class 'str'>}
print("Data schema:", dict(data_schema))
# Data schema: {'bmi': <class 'float'>}

# Iterate over results
for tag, data in result.iter_data():
    print(f"  {tag.as_dict()} -> {data.as_dict()}")
# {'subject_id': 'mouse_01'} -> {'bmi': 1756.9444444444446}
# {'subject_id': 'mouse_02'} -> {'bmi': 2209.9999999999995}
```

The function pod preserves tags and replaces data columns with the function's output.

!!! tip
    All standard pods support `__call__` as a shorthand for `.process()`, so
    `compute_bmi.pod(source)` is equivalent to `compute_bmi.pod.process(source)`.

## Input columns must match exactly

A function pod is strict about its input: the stream's data columns must be exactly the
function's parameters. Orcapod does **not** pick the columns it needs out of a wider stream.
A stream with an extra data column, a missing column for a parameter without a default, or a
column whose type is incompatible with its parameter is rejected with `ValueError` as soon as
the pod is applied. Types need to be compatible, not identical: a `str` column is accepted for
a `str | Path` parameter but rejected for an `int` one. (A parameter that has a default value
may be left out of the stream; the function then uses the default.)

```python
wide = DictSource(
    data=[
        {"subject_id": "mouse_01", "weight": 25.3, "height": 0.12, "age": 12},
        {"subject_id": "mouse_02", "weight": 22.1, "height": 0.10, "age": 8},
    ],
    tag_columns=["subject_id"],
)

try:
    compute_bmi.pod(wide)
except ValueError as err:
    print(type(err).__name__)
# ValueError
```

The error message names both schemas, for example `... Schema({'weight': float, 'height':
float, 'age': int}) is not compatible with expected input schema Schema({'weight': float,
'height': float})`.

Narrow the stream first with the `SelectDataColumns` [operator](operators.md) (or drop the
extras with `DropDataColumns`):

```python
from orcapod.operators import SelectDataColumns

narrow = SelectDataColumns(columns=["weight", "height"])(wide)
result = compute_bmi.pod(narrow)
for tag, data in result.iter_data():
    print(f"{tag.as_dict()} -> {data.as_dict()}")
# {'subject_id': 'mouse_01'} -> {'bmi': 1756.9444444444446}
# {'subject_id': 'mouse_02'} -> {'bmi': 2209.9999999999995}
```

The same step is available as a stream method: `wide.select_data_columns(["weight", "height"])`.
Tag columns are never affected by this rule -- a function pod passes all tags through
untouched.

## `PipelineJob` -- database-backed, cached execution

Applying a pod directly, as above, computes in memory and keeps nothing. To store results and
reuse them on later runs, record the same calls inside a `PipelineJob` that has a database
(`store`):

```python
from orcapod import PipelineJob, function_pod
from orcapod.databases import InMemoryArrowDatabase
from orcapod.sources import DictSource

calls = []

@function_pod(output_keys="bmi")
def compute_bmi(weight: float, height: float) -> float:
    calls.append(weight)  # only here to show when the function really runs
    return weight / (height ** 2)

mice = [
    {"subject_id": "mouse_01", "weight": 25.3, "height": 0.12},
    {"subject_id": "mouse_02", "weight": 22.1, "height": 0.10},
]

db = InMemoryArrowDatabase()

job = PipelineJob(name="bmi_demo", store=db)
with job:
    source = DictSource(data=mice, tag_columns=["subject_id"], source_id="mice")
    compute_bmi.pod(source, label="bmi")

job.run()

print(job.nodes["bmi"].as_table().to_pandas())
#   subject_id          bmi
# 0   mouse_01  1756.944444
# 1   mouse_02  2210.000000
print("function calls:", len(calls))
# function calls: 2
```

Inside the `with` block nothing is computed; the job only records which pods are applied to
which streams. `job.run()` executes the recorded graph and writes every result to the
database. Each recorded step becomes a node, available afterwards as `job.nodes[label]`
(pass `label=` when you apply the pod, otherwise the function's name is used; a source is
listed under its `source_id`). A node is a
stream, so `as_table()`, `iter_data()` and `output_schema()` all work on it.

`InMemoryArrowDatabase` lasts only as long as the Python process. Use
`DeltaTableDatabase("some/directory")` from `orcapod.databases` to keep results on disk
between sessions.

### Re-runs compute only what is new

Build the same job again over the same database, this time with one more row. Only the new
row is computed; the other two results come from the database:

```python
mice.append({"subject_id": "mouse_03", "weight": 27.8, "height": 0.11})

job = PipelineJob(name="bmi_demo", store=db)
with job:
    source = DictSource(data=mice, tag_columns=["subject_id"], source_id="mice")
    compute_bmi.pod(source, label="bmi")

job.run()

print(job.nodes["bmi"].as_table().to_pandas())
#   subject_id          bmi
# 0   mouse_01  1756.944444
# 1   mouse_02  2210.000000
# 2   mouse_03  2297.520661
print("function calls:", len(calls))
# function calls: 3
```

!!! warning "Editing a function does not invalidate its cached results"
    Stored results are looked up by the function's **name**, its **output schema** and its
    **major version** -- not by its source code. If you change the body of `compute_bmi` and
    run again, rows that were already computed are returned from the database with their old
    values. To force recomputation after changing what a function does, bump the major
    version, e.g. `@function_pod(output_keys="bmi", version="v1.0")` (the default is
    `"v0.0"`; a minor bump such as `"v0.1"` is not enough), or rename the function.

A node from a job that has run also provides:

- **`get_all_records()`** -- every record this step of the job has stored, as a PyArrow
  Table, including rows computed by earlier runs of the same job over the same source with
  other inputs. It is not an export of everything the function ever computed: results the
  function produced in a job with a different name, or from a source with a different
  `source_id`, are not included
- **`as_source()`** -- a `DerivedSource` backed by those stored records, usable as the input
  of another pipeline

## Multiple input streams

If you pass multiple streams to a function pod, they are automatically joined (using
[Join](operators.md)) before the function is applied:

```python
weights = DictSource(
    data=[
        {"subject_id": "mouse_01", "weight": 25.3},
        {"subject_id": "mouse_02", "weight": 22.1},
    ],
    tag_columns=["subject_id"],
)
heights = DictSource(
    data=[
        {"subject_id": "mouse_01", "height": 0.12},
        {"subject_id": "mouse_02", "height": 0.10},
    ],
    tag_columns=["subject_id"],
)

result = compute_bmi.pod(weights, heights)
for tag, data in result.iter_data():
    print(f"{tag.as_dict()} -> {data.as_dict()}")
# {'subject_id': 'mouse_01'} -> {'bmi': 1756.9444444444446}
# {'subject_id': 'mouse_02'} -> {'bmi': 2209.9999999999995}
```

The join happens on shared tag columns, and the merged data columns are fed to the function.
The [exact-match rule](#input-columns-must-match-exactly) applies to the joined stream.

## DataFunction internals

Under the hood, the `function_pod` decorator creates a `PythonDataFunction`, which wraps a
Python callable with input/output schema metadata. When the pod runs inside a `PipelineJob`,
the job wraps it in a database-backed node (`FunctionJobNode`) that checks the database
before calling the underlying function.

These are implementation details -- the `@function_pod` decorator and `PipelineJob` are the
primary user-facing APIs.

## How it connects to other concepts

- Function pods consume and produce [Streams](streams.md)
- [Sources](sources.md) produce the input streams that function pods transform
- [Operators](operators.md) handle structural transforms (the complement of function pods)
- Function pods participate in the [identity chain](identity.md) -- each pod's hash includes
  the function's identity and its upstream pipeline hashes
