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

The error message names both schemas and the first column that does not fit, for example
`Incoming data schema Schema({'weight': float, 'height': float, 'age': int}) is not
compatible with expected input schema Schema({'weight': float, 'height': float}): column
'age' is not a parameter of the function`.

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
    type, and so do types the Delta writer converts on the way in (`uint64` is stored as
    `int64`, every timestamp in microseconds). New columns and new struct fields pass this
    check; whether they are added follows
    the database's `allow_schema_evolution` setting. In a job run, a refused append fails the
    run as a `StoreWriteError` whose cause is the `InputValidationError`.

    When you use the database directly, both errors reach you as themselves, also from
    `flush()` and from `add_records(..., flush=True)`. When several pending tables fail in one flush, the
    `StoreReadError` (or else the first `InputValidationError`) is raised, and each other
    failure is attached to it as a note and logged. Failed rows stay queued, so a later
    flush tries them again. A flush that fails for any other reason raises `RuntimeError`,
    with the first failure as its cause.

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
- Function pods can take and return dataclasses and pydantic models; see
  [Your Own Data Types](your-own-data-types.md) for when two classes count as the same type
