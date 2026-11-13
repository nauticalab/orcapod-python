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
- **Function parameters** are matched to the input stream's data columns by name: one data
  column per parameter. A parameter with a default value may be left out of the stream, and
  the default is used. Data columns the function does not take are dropped when the pod is
  called. See [Input columns](#input-columns).
- **Default values** are part of the function's signature hash, so a default must be a value
  Orcapod can hash: `None`, numbers, strings, `bytes`, and lists, tuples, sets and dicts of
  them. A default of another type, such as a `Path`, an `Enum` member, a `datetime` or a
  dataclass instance, raises `TypeError` when the pod is created, naming the parameter. Use
  a hashable default and build the object inside the function, or supply the value as an
  input column. See [Function values and parameter defaults](../reference/hashing.md#1d-function-values-and-parameter-defaults).
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
result = compute_bmi.pod(source)

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
    Calling a function pod, `compute_bmi.pod(source)`, is the convenient form of
    `compute_bmi.pod.process(source)`: it first drops the data columns the function does not
    take (see [Input columns](#input-columns)). When the stream has exactly the function's
    columns, as here, the two are the same call.

## Input columns

A function takes one data column per parameter, matched by name. Calling the pod,
`compute_bmi.pod(stream)`, is the convenient form: data columns that are not parameters of
the function are dropped, and the function receives the rest. Tag columns are never dropped.

```python
wide = DictSource(
    data=[
        {"subject_id": "mouse_01", "weight": 25.3, "height": 0.12, "age": 12},
        {"subject_id": "mouse_02", "weight": 22.1, "height": 0.10, "age": 8},
    ],
    tag_columns=["subject_id"],
)

result = compute_bmi.pod(wide)  # 'age' is not a parameter: it is dropped
for tag, data in result.iter_data():
    print(f"{tag.as_dict()} -> {data.as_dict()}")
# {'subject_id': 'mouse_01'} -> {'bmi': 1756.9444444444446}
# {'subject_id': 'mouse_02'} -> {'bmi': 2209.9999999999995}
```

The drop is not hidden. Orcapod places one `SelectDataColumns` [operator](operators.md)
between the stream and the pod, exactly as if you had written it yourself with the stream's
`select_data_columns` method, so the two pipelines below are the same pipeline: same hashes,
same graph, same stored tables. `SelectDataColumns` is the name that appears in the graph.

```python
narrow = wide.select_data_columns(["weight", "height"])
by_hand = compute_bmi.pod(narrow)

print(result.pipeline_hash() == by_hand.pipeline_hash())
# True
print(result.content_hash() == by_hand.content_hash())
# True
```

Because the unused columns are cut before the function's input is hashed, a change in an
unused column does not cause a recomputation. When the stream already has exactly the
function's columns, no step is added.

Each call that drops columns logs one message at `INFO` level (logger
`orcapod.core.function_pod`) naming them. It is worth reading when a parameter has a default
value: a misspelt column (`Height` for a parameter `height=1.0`) is dropped like any other
unused column and the default is used, and that message is the only trace.

### Exact calls: `strict=True` and `.process()`

To have an extra column rejected instead of dropped, pass `strict=True`, or call
`.process()`, which is the exact primitive and never drops a column:

```python
try:
    compute_bmi.pod(wide, strict=True)
except ValueError as err:
    print(type(err).__name__)
# ValueError

try:
    compute_bmi.pod.process(wide)
except ValueError as err:
    print(type(err).__name__)
# ValueError
```

The error message names both schemas and the first column that does not fit, for example
`Incoming data schema Schema({'weight': float, 'height': float, 'age': int}) is not
compatible with expected input schema Schema({'weight': float, 'height': float}): column
'age' is not a parameter of the function`.

`strict` decides only whether an extra column is an error. It is not part of any hash: when
there is nothing to drop, `pod(stream)`, `pod(stream, strict=True)` and `pod.process(stream)`
build the same pipeline.

### What is an error in every form

A missing column for a parameter without a default, or a column whose type is incompatible
with its parameter, is rejected with `ValueError` as soon as the pod is applied, however it
is applied. Types need to be compatible, not identical: a `str` column is accepted for a
`str | Path` parameter but rejected for an `int` one. (A parameter that has a default value
may be left out of the stream; the function then uses the default.)

A stream must keep at least one data column, so a stream none of whose data columns is a
parameter is rejected as well.

The same step is available as a stream method: `wide.select_data_columns(["weight", "height"])`.
Tag columns are never affected by this rule -- a function pod passes all tags through
untouched.

## Return values must match the declared type

Each value the function returns is checked against its declared output type before it is
stored or hashed. A `float` returned for an output declared `int` is rejected; it is not
truncated. The `OutputTypeError` names the function, the output, the declared type and the
type of the value (never the value itself, which stays on the error's `value` attribute),
and says what to return instead:

```python
from orcapod.errors import OutputTypeError

@function_pod(output_keys="half_weight")
def halve(weight: float) -> int:
    return weight / 2  # a float, although the function says it returns an int

weights = DictSource(data=[{"subject_id": "mouse_01", "weight": 25.3}], tag_columns=["subject_id"])

try:
    list(halve.pod(weights).iter_data())
except OutputTypeError as err:
    print(err)
# Function 'halve' returned float for output 'half_weight', which is declared int. The output does not conform. Return an int instead: round(value), or int(value) to truncate.
```

Fix the function (`return round(weight / 2)`), or declare the type it really returns
(`-> float`).

`OutputTypeError` is a `TypeError` (and a `ValueError`). Inside a `PipelineJob` it counts as
a failure of the function for that row, like an exception the function raises itself. With
the default `error_policy="continue"` the row is skipped and reported to the observer, and
the other rows are computed and stored. With `error_policy="fail_fast"` the synchronous
orchestrator stops the run; the asynchronous orchestrator does not stop for row failures yet
(ITL-879, see below).

The function-output rule is: the returned value must be an instance of the declared type, or an exact numeric equivalent; a subclass stands in for its base. What is stored for a subclass instance is decided by the logical type of the declared class: a user class is stored as the base (the declared class's fields only), while a `torch.nn.Parameter` returned for `-> torch.Tensor` is stored, read back and hashed as an `nn.Parameter`.

| Declared | Returned | Result |
|---|---|---|
| `int` / `float` / `bool` | same-kind NumPy scalar, represented exactly | accepted silently |
| NumPy integer type | Python or NumPy integer within range | accepted silently; out of range rejected |
| `int` / `float` | value-preserving numeric subclass such as `IntEnum` | accepted silently |
| `float` | `numpy.float32`, including `numpy.float32(0.1)` | accepted: widening preserves the original float32 value exactly |
| `numpy.float32` | Python `float` or `numpy.float64`, even `0.5` | rejected: explicitly return `numpy.float32(value)` |
| `float` | NumPy floating scalar wider than 64 bits | rejected: explicitly return `float(value)` |
| `list[T]` | tuple, NumPy array, range, deque, dict view, or iterator | rejected: return `list(value)` |
| `list[T]` | set or frozenset | rejected: return `sorted(value)` for deterministic order |
| `dict[K, V]` | another mapping, such as `MappingProxyType` or `UserDict` | rejected: return `dict(value)` |
| `Literal[...]` | a value that is not a member, or equals one with another type (`True`, `1.0` or `numpy.int64(1)` for `Literal[1]`) | rejected: a `Literal` names exact values, so no numeric exception applies |
| `date` | `datetime` | accepted as a Python subclass; storage keeps the date only |
| `int` | `bool` | accepted as a Python subclass; stored as an integer |
| `int` | float, Decimal, Fraction | rejected; choose an explicit conversion |
| `bool` | `int` | rejected: return `bool(value)` explicitly |
| floating type | integer or bool | rejected: return a floating value explicitly |
| non-optional type | `None` | rejected; declare `T \| None` if null is intended |
| ordinary dataclass or pydantic model | subclass | accepted; storage keeps only declared base fields |
| ordinary dataclass or pydantic model | unrelated class | rejected |
| structural class | another structural class with the same identity | accepted; fields follow the same recursive checks |

Python subclassing takes precedence: `bool` for `int` is accepted, while `int` for
`bool` is rejected. Numeric exceptions otherwise require the same kind.
Nested list elements, dictionary keys and values, and stored user-class fields follow the
same policy. A mismatch raises `OutputTypeError` at the common function-output boundary,
before standalone iteration, table materialization, or sync/async storage can diverge.
Errors name the function, output, declared type and returned type; nested locations show
structure without including returned dictionary keys or converter messages.

Outputs are validated against the declared storage schema immediately, and accepted numeric
exceptions are normalized so downstream pods, hashes and stored rows see the same value.
Eager validation never replaces a compatible user-class object; stored reads keep their
existing registry binding. A `Sub` returned for `Base` reaches the next in-memory pod
as that same `Sub` object. The stored column is `Base`: subclass-only fields are not
stored and do not enter the row hash. Two such values differing only in those extra
fields therefore have the same stored row hash. Likewise, a `datetime` declared as
`date` is passed on unchanged in memory, but storage contains only its date.

A consequence to know: the declared type's logical type decides what is stored, so a
function should read only what its declared parameter type has. A function that reads a
field its declared parameter type does not have (a subclass-only field, or the time of a
`datetime` declared as `date`) gets results that depend on the execution path: standalone
streams and the asynchronous orchestrator pass the in-memory object, while the synchronous
orchestrator and any later read pass the stored base value. Such results can also be reused
across values, because two values that differ only in what is not stored have the same row
hash and the second is served the first one's stored result. This follows from Python
subclassing and is left as it is (ITL-978).

No warning-and-convert path remains for function outputs. Make container conversions
explicit:

```python
@function_pod(output_keys="weights")
def twice(weight: float) -> list[float]:
    return list((weight, weight * 2))

for tag, data in twice.pod(weights).iter_data():
    print(data.as_dict())
# {'weights': [25.3, 50.6]}
```

Previously stored rows are served unchanged; this check does not repair or recompute them.
Input-source conformance remains outside this change.

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

!!! note "When the store cannot be read"
    A table that does not exist yet reads as empty, and its rows are computed. In a
    `DeltaTableDatabase`, a table that exists but cannot be read (for example, a data file was
    deleted or damaged) is never treated as empty: the run stops with
    `orcapod.errors.StoreReadError`, which names the
    table, the node and row being processed when they are known, and the underlying error,
    whatever the orchestrator's `error_policy`. Its stored rows are not recomputed and the
    table is not overwritten, so you can repair the table or restore the file and run again.
    A write that fails because the stored table cannot be read raises `StoreReadError` too,
    not `StoreWriteError` (see *When the store cannot be written* below).

    In a `DeltaTableDatabase`, rows whose column types differ from the stored table's, as
    Delta stores them (`float64` values for a column stored as `int64`, say), are refused with
    `InputValidationError`, which names the table, the column and both types, and nothing is
    written to that table. They are not converted to the stored type. `string` and
    `large_string`, and the other large and non-large layouts of one type, count as the same
    type. A column of a type Delta Lake has no equivalent for (an unsigned integer, a
    timestamp in seconds, milliseconds or nanoseconds or with a zone other than UTC, `date64`,
    `time`, `duration`, at any depth) is written in a form Delta has, with its Arrow type
    recorded in the table, and every read gives it back with that type and its exact values.
    New columns and new struct fields pass this
    check; whether they are added follows
    the database's `allow_schema_evolution` setting. In a job run, a refused append fails the
    run as a `StoreWriteError` whose cause is the `InputValidationError`.

    When you use the database directly, both errors reach you as themselves, also from
    `flush()` and from `add_records(..., flush=True)`. When several pending tables fail in one flush, the
    `StoreReadError` (or else the first `InputValidationError`) is raised, and each other
    failure is attached to it as a note and logged. Failed rows stay queued, so a later
    flush tries them again. A flush that fails for any other reason raises `RuntimeError`,
    with the first failure as its cause.

### Running asynchronously, in a script or a notebook

`job.run()` uses the `SyncPipelineOrchestrator`, which runs one node after another. The
`AsyncPipelineOrchestrator` runs every node at once, passing rows between them as they are
ready, and runs your function on several rows concurrently. Choose it with the
`orchestrator` argument. The line is the same in a script and in a Jupyter notebook:

```python
from orcapod.pipeline import AsyncPipelineOrchestrator

job = PipelineJob(name="bmi_async", store=InMemoryArrowDatabase())
with job:
    source = DictSource(data=mice, tag_columns=["subject_id"], source_id="mice")
    compute_bmi.pod(source, label="bmi")

job.run(orchestrator=AsyncPipelineOrchestrator())

# The async run stores rows as they finish: sort them to print in a fixed order.
print(job.nodes["bmi"].as_table().sort_by("subject_id").to_pandas())
#   subject_id          bmi
# 0   mouse_01  1756.944444
# 1   mouse_02  2210.000000
# 2   mouse_03  2297.520661
```

Both orchestrators store the same rows, with the same record ids and hashes.

A notebook kernel already runs an event loop. There, `job.run(...)` runs the job on a
separate thread and waits for it, so the cell blocks until the run ends, as it does with the
sync orchestrator. The interrupt button cancels the run: rows already stored stay stored, and
the next run computes only the rest. A function that is in the middle of a row cannot be
interrupted, so the cell waits until it returns. If it does not return (a function that
loops forever, say), press the interrupt button again: the cell stops waiting at once, with a
warning that the function in that node has not returned yet. The run is already cancelled,
so nothing more is written to the store when the function does return. To stop the function
itself, restart the kernel.

An `async def` function must not block (with `time.sleep`, a busy loop or another blocking
call). It runs on the event loop's own thread, so while it blocks, the cancellation cannot
run. In that case the warning says so: rows already in progress may still be stored when
the function returns, before the run stops.

In async code, await the run instead, so the event loop stays free while it runs:
`await job.run_async()` (the async orchestrator is its default). This also works at the top
level of a notebook cell. In a script, start async code with `asyncio.run`:

```python
import asyncio

job = PipelineJob(name="bmi_awaited", store=InMemoryArrowDatabase())
with job:
    source = DictSource(data=mice, tag_columns=["subject_id"], source_id="mice")
    compute_bmi.pod(source, label="bmi")


async def main():
    await job.run_async()


asyncio.run(main())
print(job.nodes["bmi"].as_table().num_rows)
# 3
```

### Reading the run summary

`job.run()` (and `await job.run_async()`) returns a `RunSummary`: for each function node, how
many rows were computed, how many were read from the store, and how many failed. At the end
of a notebook cell it is shown as a table; `print` shows the same as text. The job keeps it as
`job.last_run`. The results themselves are read from `job.nodes`, as above.

Running the same job again computes nothing new, and the summary says so:

```python
job = PipelineJob(name="bmi_demo", store=db)
with job:
    source = DictSource(data=mice, tag_columns=["subject_id"], source_id="mice")
    compute_bmi.pod(source, label="bmi")

summary = job.run()
print(summary)
# Run of job 'bmi_demo': all rows succeeded.
#   bmi: 3 rows: 0 computed, 3 from store, 0 failed
```

When a function raises for a row, the run skips that row and goes on with the others (the
default `error_policy="continue"`). At the end it issues one `RowFailuresWarning` (from
`orcapod.errors`) naming each node with failed rows, and the summary shows the first error.
A run with no failed rows issues no warning:

```python
@function_pod(output_keys="bmi")
def careless_bmi(weight: float, height: float) -> float:
    return weight / heigth ** 2  # a typo: heigth


job = PipelineJob(name="bmi_typo", store=InMemoryArrowDatabase())
with job:
    source = DictSource(data=mice, tag_columns=["subject_id"], source_id="mice")
    careless_bmi.pod(source, label="bmi")

summary = job.run()
# RowFailuresWarning: 3 of 3 rows failed in node 'bmi' (first error: NameError: name 'heigth' is not defined). Full traceback: job.last_run.print_traceback("bmi").
print(summary)
# Run of job 'bmi_typo': 3 rows failed.
#   bmi: 3 rows: 0 computed, 0 from store, 3 failed
#     first error: NameError: name 'heigth' is not defined
#     Full traceback: job.last_run.print_traceback("bmi")
```

`job.last_run.print_traceback("bmi")` prints the full traceback of the node's first failed
row, ending in the line of your function that raised. Fix the function and run the job
again: the rows that succeeded before are read from the store, and only the failed ones
are computed. To stop a run at the first failed row instead, pass
`SyncPipelineOrchestrator(error_policy="fail_fast")` as the orchestrator.

The counts are also available one by one, for example in a test:

```python
node = summary.nodes["bmi"]
print(node.computed, node.reused, node.failed, type(node.first_error).__name__)
# 0 0 3 NameError
print(summary.ok, summary.failed_rows)
# False 3
```

### When results are committed

Inside `job.run()`, each function node commits its results and pipeline records to the
store in batches: every `commit_every_rows` rows (default 100), when the node finishes,
before a failed run raises, and when the job ends. Committing in batches keeps a run's time
linear in its rows; on a `DeltaTableDatabase` each commit is one Delta version per table, so
a 1,000-row node writes 10 versions per table instead of 1,000.

```python
job.run(commit_every_rows=500)  # fewer, larger commits
```

`commit_every_rows=1` commits after every row. A node or a cached pod used on its own,
outside `job.run()`, also commits after every row. On a `DeltaTableDatabase`,
`commit_every_rows` must be smaller than the store's `batch_size` (default 1000):
the store commits every pending table by itself once one table holds that many pending
rows, which would not keep the order described below, so `job.run()` refuses a larger value
with `ValueError`. Create the store with a larger `batch_size` to commit less often. For the same reason, `job.run()` first commits
whatever such a store already holds pending (writes made without a flush, or the batches of a
run that failed); if that fails, the run stops before computing anything.

**Memory.** A `DeltaTableDatabase` keeps the committed rows of every table it looks rows up
in (each function node's result and pipeline-record tables) in memory for as long as the
store object lives, so that a lookup never re-reads the table. These copies are held
together, so plan memory for all of them at once: for a function returning one `int`, about
1.6 KB per stored row per function node (result and pipeline-record tables together), more
for larger outputs. Before, each lookup read one whole table and released it. To release
the copies, use a new store object, for example one per job.

**What a hard crash loses.** If the process dies without raising (power loss, `kill -9`,
the machine restarts), a durable store such as `DeltaTableDatabase` keeps every batch
committed before the crash; at most the rows the running node processed since its last
commit are lost, and nothing stored is damaged. Run the job again: it computes only the rows
whose results were not committed. A node commits its results before their pipeline records,
so a crash between those two commits leaves results stored without pipeline records. Reading
the node (`as_table()`, `get_all_records()`) does not show those rows until the next run,
which finds their results in the store, computes nothing for them and writes their pipeline
records.

**Reads see the version this process loaded.** A `DeltaTableDatabase` keeps the committed
rows of each table it has read in memory and answers lookups from them, extending them with
its own commits. Rows another process commits to the same table are not seen by a store
object that has already loaded the table until its own next commit to that table reloads it;
a new `DeltaTableDatabase` object reads them at once. This was already the case before
batching; only the re-reading per lookup is gone.

A node from a job that has run also provides:

- **`get_all_records()`** -- every record this step of the job has stored, as a PyArrow
  Table, including rows computed by earlier runs of the same job over the same source with
  other inputs. It is not an export of everything the function ever computed: results the
  function produced in a job with a different name, or from a source with a different
  `source_id`, are not included
- **`as_source()`** -- a `DerivedSource` backed by those stored records, usable as the input
  of another pipeline

An operator node (a `Join`, `Batch`, column selection and so on, applied inside the job) is
read the same way: after `job.run()` its `as_table()` and `iter_data()` return the rows it
produced in the latest run, on both the synchronous and the asynchronous orchestrator. It
keeps that output in memory and, with the default `cache_mode=OFF`, stores nothing. Its
`get_all_records()` returns `None` when no matching records exist; records from an earlier
LOG run remain readable after switching to OFF. Calling `clear_cache()` on it discards the
output, and it then reads as a zero-row table, as before the first run, until the job runs again.

### When the store cannot be written

A failed write to the store always fails the run. If a node's results or records cannot be
stored -- the disk is full, the connection drops, or the database cannot hold one of the
output types -- `job.run()` raises `StoreWriteError` (from `orcapod.errors`) at the first
failed write, on both the synchronous and the asynchronous orchestrator. A row's write that
fails names the node, the row being stored and the table, and the database's own exception
is chained as its cause. When committing pending writes fails, the error has `flush=True`
and names the node and the store instead of a row and table: a commit holds a batch of rows
(see *When results are committed*), and every view of a store shares its pending writes, so
the write that failed may belong to any row of the batch or another table. A run that
returns normally has stored every result that could be turned into a record; a result
rejected before that is a row failure (see below).

With batched commits, a store failure that only committing reveals (an unreadable table, a
full disk) surfaces at the node's next commit point, not at the row that caused it: the rows
the node computed since its last commit are not stored and are computed again on the next
run. `job.run(commit_every_rows=1)` commits after every row, so such a failure surfaces at
its row, at the cost of one commit per row.

This holds whatever `error_policy` the orchestrator was given. `error_policy="continue"` (the
default) skips rows whose *function* raised and keeps going; it never skips a row because it
could not be stored.

`StoreWriteError` and `StoreReadError` both derive from `orcapod.errors.StoreError`, so
`except StoreError` handles either. The error you get is always one plain store error, never
an exception group and never another exception raised in its place. A post-run hook or
observer callback that fails while the store error is on its way out is logged and recorded
as a note on the error (`err.__notes__`). When several nodes fail at once on the asynchronous
orchestrator, read and write failures alike, you get the store error of the node that comes
first in the pipeline, the one the synchronous orchestrator would have stopped at; the other
failures are logged and noted on it. A node whose failure the asynchronous orchestrator
cancelled before it happened is not reported.

After the failure, rows stored before it are kept: every batch committed before the failure
stays, and `job.run()` tries once more to commit the writes still pending before it
re-raises (a failed commit's batch stays pending, so this is its one retry; if it fails
again, that is noted on the error), and nothing already stored is rewritten. Once the cause is
fixed, run the job again: stored function results are not recomputed, only the missing ones
are. An operator with `cache_mode=LOG` recomputes its output on every run, as it always does.
A side effect is different: if its delivery completed but the write to its invocation log
failed, there is no record that it completed, and `track_completion` delivers it again on the
re-run. Before re-running, check which side effects already completed, or make the side
effect safe to repeat.

A value your function returned for one row that cannot be turned into a record at all is a
different case: a naive `datetime`, a `str` from a function declared to return `int`, or an
`int` where a dataclass is declared. Such a value is rejected while the row's record is being
prepared, before anything is written, and it is a *row failure*: it is handled exactly like a
function that raised on that row. The observer gets `on_data_crash`, the failure is logged,
and `error_policy` decides -- `"continue"` skips the row, `"fail_fast"` raises the error
the conversion raised. It is not a `StoreWriteError`, because the store is healthy and every
re-run would fail at the same row. The asynchronous orchestrator currently logs row failures
at DEBUG and does not stop for `"fail_fast"` (ITL-879); this applies to these rows too.

This is the contract for now. An opt-in mode that tolerates store failures may be added
later.

### Saving and loading a job

`job.save(path)` writes the job to a JSON file: the graph, the store's location and how each
source was configured. `PipelineJob.load(path)` reads it back, for example in a later Python
session, so that you can read the results the job stored. The file also records
`orcapod_version`, the Orcapod release that wrote it (the same text as `orcapod.__version__`;
a development checkout gives a development version). It is informational: loading never
depends on it. Adding the field moved the file format version from `0.1.0` to `0.2.0`; a
`0.1.0` file, written by v0.2.0, still loads as before, and any other version is rejected.
The reverse does not hold: v0.2.0 cannot load a file saved by v0.3.0, because hashes differ
between the two releases.

For example:

```python
import tempfile
from pathlib import Path

from orcapod.databases import DeltaTableDatabase

workdir = Path(tempfile.mkdtemp())  # any directory you keep

job = PipelineJob(name="bmi_demo", store=DeltaTableDatabase(workdir / "store"))
with job:
    source = DictSource(data=mice, tag_columns=["subject_id"], source_id="mice")
    compute_bmi.pod(source, label="bmi")

job.run()
job.save(workdir / "bmi_job.json")

loaded = PipelineJob.load(workdir / "bmi_job.json")
print(loaded.nodes["bmi"].as_table().sort_by("subject_id").to_pandas())
#   subject_id          bmi
# 0   mouse_01  1756.944444
# 1   mouse_02  2210.000000
# 2   mouse_03  2297.520661
```

A loaded job is for reading stored results. It cannot be run again: function pods are not
saved with the job, so `loaded.run()` raises. To compute again, build the job from your code
as before.

- A function node's `as_table()`, `iter_data()` and `get_all_records()` return the rows the
  saved run stored, with the types they were stored with. Rows come in the order the store
  returns them; sort them if order matters.
- Operators (a join, a filter) store outputs only when caching is enabled, under `odb_v1`.
  A loaded operator without a readable cache raises from `as_table()` and `iter_data()`.
  Read its input nodes instead (an in-memory input source must be bound first, see below).
  Saved descriptors do not yet preserve all runtime identity and cache configuration needed
  for operator readback without reconstruction (follow-up ITL-977).
- Sources that read files, such as `CSVSource` and `DeltaTableSource`, are rebuilt from their
  paths. In-memory sources (`DictSource`, `ListSource`, `ArrowTableSource`,
  `DataFrameSource`) keep their data only in memory, so they are not saved; loading logs
  this at INFO level. This does not affect reading the other nodes. To read such a source
  node itself, bind a source to it: `loaded.bind(sources={"mice": source})`.
- The results are read from the store recorded in the file. Pass
  `PipelineJob.load(path, store=...)` if the store has moved. If the store you give lacks
  results the saved run stored, reading the node raises an error naming the store, instead
  of returning no rows.

Logical types in saved schema text carry their complete identifier, written as
`extension<name;hex-metadata>`, including inside nested containers. The loader checks the
identifier against the registered type. Known types such as `Path`, `File`, UUID and arrays
keep downstream operators available; a user class must be registered in the loading process.
An unknown type is reported with its node and column, and affected operators become read-only
stubs. An incompatible identifier raises `LogicalTypeIdentifierError` instead of using a stub.
Stored data independently validates its full identifier when values are read.

Older files containing name-only extension type text cannot be loaded by this reader: the
error says the file was saved by an earlier version. Use that version to read the old file,
and save a new job from the original pipeline under this version. Files with only supported
primitive schema types still load; results from pre-v0.3 table layouts remain excluded as
described below. This changes saved schema text only, not stored tables or hashes.

### A store written by an older version

Every row Orcapod stores names the **data context** that wrote it, in the column
`__data_context`: `std:v1:default` since v0.3. The data context is one key for the whole of
Orcapod's hashing, so rows that share it were keyed in exactly the same way (see
[The data context of a stored row](../reference/hashing.md#the-data-context-of-a-stored-row)).
Tables written before v0.3 have no such column and are stored at an older table layout
(`pdb_v1`, `rdb_v1`). Orcapod **does not read them as results**: it does not refuse the
store, and it does not reuse those rows.

- The first run of v0.3 on a store written by v0.2 computes every input again and stores the
  rows in its own tables (`pdb_v2`, `rdb_v2`).
- Reading a node (`as_table()`, `iter_data()`, `get_all_records()`) returns the rows of the
  current data context only: nothing before that first run, one row per input after it.
- The old tables are never changed or deleted. They stay readable with the version that
  wrote them and are kept for a later migration. To see which data contexts a table holds,
  use `orcapod.contexts.stored_data_contexts`.
- When Orcapod finds a table at an older layout, it logs one warning with the number of
  rows: `Found 3 rows in stored table '...' written by an earlier version of Orcapod (older
  table layout, data context std:v0.1:default). They are not used and are left unchanged;
  they can be migrated later.`
- The hashes in a table path can differ between versions, so older tables are looked for
  by name: Orcapod lists the tables stored under the job and function name and treats every
  table that is not at a current layout as written by an earlier version. Tables found this
  way are reported once per node, without a number of rows; a table of another function
  with the same name in the same job is included. A job run also reports one total of the
  older tables that belong to none of its function nodes (operator caches, cached sources,
  nodes that left the job).
- Nodes that perform side effects are the exception: side-effect pods, and function pods
  that take the invocation context (`ctx_arg`). A side effect is an external action, and
  completions stored by v0.2 cannot be matched with current inputs, so running the node
  would do everything a second time. Instead the node raises
  `SideEffectLogDataContextError` (from `orcapod.errors`, a `StoreError`), naming the
  tables, before it does anything. Every older table under the node's function or pod name
  in the job counts. Keep using the version that wrote them. A fresh store is not a safe
  way around the error: it holds no completions, so on it the node performs every side
  effect again; use one only when a repeated delivery is acceptable. If the store cannot
  list its tables, the node warns that it could not check, and runs.

Each result row also records the full version string of the function that produced it, in
the column `__pf_var_function_version` (for example `v3.7`); only the major part is in the
table path.

## Value types

A function pod's parameters and return values can be plain Python scalars (`int`, `float`,
`str`, `bool`, `bytes`), lists, dataclasses, pydantic models, and the built-in logical types
below. A logical type is stored as an Arrow extension type: the extension name is recorded
in the stored schema, so a value read back from a database comes back as the same Python
type.

| Python type | Extension name | Arrow storage |
|---|---|---|
| `pathlib.Path` | `orcapod.path` | `large_string` |
| `upath.UPath` | `orcapod.upath` | `large_string` |
| `uuid.UUID` | `orcapod.uuid` | `large_binary` |
| `orcapod.File` | `orcapod.file` | `large_string` |
| `orcapod.Directory` | `orcapod.directory` | `large_string` |
| `numpy.ndarray` | `orcapod.numpy.ndarray` | `large_binary` (`.npy` bytes, see [Arrays](arrays.md)) |
| `pandas.DataFrame` | `pandas.dataframe` | `large_binary` (Arrow IPC stream) |
| `pandas.Series` | `pandas.series` | `large_binary` (Arrow IPC stream) |
| `polars.DataFrame` | `orcapod.polars.dataframe` | `large_binary` (Polars IPC file, `write_ipc`) |
| `polars.Series` | `orcapod.polars.series` | `large_binary` (Polars IPC file, `write_ipc`) |

A `polars.DataFrame` comes back with exactly the schema it was stored with: column order and
names, integer and float widths, `Categorical`, `Enum` with its category order, `Decimal`
precision and scale, `Datetime` unit and time zone, `Duration` unit, `List`, `Array` shape,
`Struct` fields in their order and `Null`. A `polars.Series` keeps its name, dtype and values the same way.
Dtypes that cannot be stored and read back exactly (`Object`, `Int128`, `UInt128`, and Polars
extension dtypes) raise `ValueError` naming the column when the value is stored, so convert
such a column explicitly before returning the frame.

A Polars value's content hash, which is its cache key, covers the value's logical content: its
row count, column names and their order, dtypes (struct fields and their order included) and
values. So a pod that depends on column order, such as one returning `df.columns[0]`, gets a
separate cache entry for each order. Physical properties are not part of the hash: chunking
(`n_chunks()`) and sortedness flags (`Series.flags`, which are not stored either). A pod must
not depend on them, because equal frames that differ only there share a cache entry. See
[Polars DataFrame and Series values](../reference/hashing.md#1e-polars-dataframe-and-series-values)
for the exact scheme.

```python
import polars as pl

from orcapod import function_pod
from orcapod.sources import DictSource

@function_pod(output_keys="readings")
def load_readings(n_samples: int) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "sample": pl.Series(range(n_samples), dtype=pl.UInt16),
            "voltage": pl.Series([0.5] * n_samples, dtype=pl.Float32),
        }
    )

@function_pod(output_keys="n_rows")
def count_rows(readings: pl.DataFrame) -> int:
    return readings.height

recordings = DictSource(
    data=[{"session": "s1", "n_samples": 2}, {"session": "s2", "n_samples": 3}],
    tag_columns=["session"],
)
readings = load_readings.pod(recordings)
for tag, data in readings.iter_data():
    print(tag["session"], dict(data["readings"].schema))
# s1 {'sample': UInt16, 'voltage': Float32}
# s2 {'sample': UInt16, 'voltage': Float32}

for tag, data in count_rows.pod(readings).iter_data():
    print(tag["session"], data["n_rows"])
# s1 2
# s2 3
```

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
The [input column rules](#input-columns) apply to the joined stream: the join comes first,
then one automatic `SelectDataColumns` drops the joined columns the function does not take.
Written by hand, that is `a.join(b).select_data_columns([...])` passed to the pod.
Two consequences:

- Two inputs that carry the same data column are rejected by the join, whether or not the
  function takes that column. Drop it from one input yourself.
- A stream can be passed to restrict the rows by its tags: it is joined, and its data
  columns, which the function does not take, are then dropped.

```python
chosen = DictSource(
    data=[{"subject_id": "mouse_02", "cohort": "b"}],
    tag_columns=["subject_id"],
)

result = compute_bmi.pod(weights, heights, chosen)
for tag, data in result.iter_data():
    print(f"{tag.as_dict()} -> {data.as_dict()}")
# {'subject_id': 'mouse_02'} -> {'bmi': 2209.9999999999995}
```

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
- Function pods can take and return dataclasses and pydantic models; see
  [Your Own Data Types](your-own-data-types.md) for when two classes count as the same type
