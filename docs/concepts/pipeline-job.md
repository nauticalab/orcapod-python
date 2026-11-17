# Pipelines and Jobs

Applying a [function pod](function-pods.md) or an [operator](operators.md) to a
[stream](streams.md) directly computes in memory and keeps nothing. A **`PipelineJob`** runs
the same steps against a database: every function result is stored, and a later run computes
only what is not stored yet. Operator steps are not stored by default; they run again on
every run. A **`Pipeline`** is the description of the steps on its own, without
data or a database, from which several jobs can be made.

For a guided introduction see the tutorial, [Part 5](../tutorial/05-pipeline-job.md) and
[Part 6](../tutorial/06-reruns-and-caching.md). This page gives an overview of jobs and
blueprints. How a run behaves in detail (the run summary, failed rows, load-only runs, what
a second run runs, commits, saving and loading) is in the
[`PipelineJob` section of Function Pods](function-pods.md#pipelinejob-database-backed-cached-execution).

## A job: record, then run

```python
from orcapod import PipelineJob, function_pod
from orcapod.databases import InMemoryArrowDatabase
from orcapod.sources import DictSource


@function_pod(output_keys="bmi")
def compute_bmi(weight: float, height: float) -> float:
    return weight / height**2


def mice(rows):
    return DictSource(data=rows, tag_columns=["subject_id"], source_id="mice")


cohort_a = [
    {"subject_id": "mouse_01", "weight": 25.3, "height": 0.12},
    {"subject_id": "mouse_02", "weight": 22.1, "height": 0.10},
]

database = InMemoryArrowDatabase()

job = PipelineJob(name="bmi", database=database)
with job:
    compute_bmi.pod(mice(cohort_a), label="bmi")
job.run()

print(job.nodes["bmi"].as_table().sort_by("subject_id").to_pylist())
# [{'subject_id': 'mouse_01', 'bmi': 1756.9444444444446}, {'subject_id': 'mouse_02', 'bmi': 2209.9999999999995}]
```

The three stages:

1. **Create** the job with a `name` and a `database`.
2. **Record** inside the `with` block. Pods and operators applied there are not executed;
   the job notes which step is applied to which stream. Sources may be created inside or
   outside the block.
3. **Run** with `job.run()`. The recorded steps execute in dependency order and each
   function result is written to the database. `run()` returns a `RunSummary` (also kept as
   `job.last_run`) that counts, per function node, the rows computed, read from the database
   and failed; see
   [Reading the run summary](function-pods.md#reading-the-run-summary).

Among `job.run()`'s optional arguments, the tutorial's
[Part 9](../tutorial/09-when-things-go-wrong.md) demonstrates `orchestrator=` (for example
`SyncPipelineOrchestrator(error_policy="fail_fast")` to stop at the first failing row; the
default policy skips failed rows and continues) and `observer=` (`StatusObserver`,
`LoggingObserver`) to record what happened during the run. `load_only=True` fills the nodes
from what is already stored and computes nothing; see
[Loading without computing](function-pods.md#loading-without-computing-runload_onlytrue).

## Nodes

Each recorded step, and each source, is a **node**. `job.nodes` maps a node's name to the
node:

| Kind of step | Name in `job.nodes` |
|---|---|
| Source | Its `source_id` |
| Function pod | The `label=` passed when it was applied; otherwise the function's name |
| Operator | The operator's class name (`Join`, `GroupBy`), numbered when there are several (`SelectDataColumns_1`) |

```python
print(sorted(job.nodes))
# ['bmi', 'mice']
```

The order in which `job.nodes` lists its names is not meaningful and can differ between
Python sessions; look nodes up by name.

A node is a stream: `output_schema()`, `keys()`, `iter_data()`, `as_table()` and
`as_pandas_df()` work on it, and show the rows of the job's **current run** in this Python
process. Read nodes after `run()`: before it, every node other than a source is empty
(`as_table()` returns no rows; `head()` and the notebook preview say the node has not run).
A run fills a node also when every result is read from the database and nothing is computed.

Three more things a job node offers:

- **`get_all_records()`** returns every record this step of the job has stored, including
  rows computed by earlier runs of the same job over the same source with other inputs.
  Pass `columns={"meta": True}` to include the record of each call (function name and
  version, time, Python version).
- **`as_source()`** returns a `DerivedSource` over the same stored records, whether or not
  the node has run in this process, to use as the input of another pipeline.
- **`upstreams`** and **`label`** let you walk the pipeline backwards.

## The database

Any Orcapod database can hold a job's results. Two are used most:

- **`InMemoryArrowDatabase()`** keeps results for the lifetime of the Python process. Use it
  for experiments and tests.
- **`DeltaTableDatabase("some/directory")`** writes [Delta Lake](https://delta.io/) tables
  under a directory, and is what makes results survive between sessions.

On disk the layout is `<directory>/<job name>/<function name>/...` for each function, plus
`<directory>/<job name>/_result/...`. The job's `name` is the top level, so jobs with
different names over one directory do not see each other's results. Deleting the directory,
or a job's subdirectory, is safe: the next run of a newly built job, for example in a new
Python session, recomputes what is missing. A job object that has already run holds its
run and computes nothing on a second `run()`.

A job created without a database can be recorded but not run. Give it one later with
`job.bind(database=...)`; `run()` without a database raises `ValueError`.

### Connector databases

A `ConnectorArrowDatabase` stores a column only when its connector can store it as written.
Each connector states which columns those are, and anything else is refused with
`UnstorableColumnError` before anything of the table is written.

The SQL connectors store a column only when they read it back with the same Arrow type (text
and binary columns come back as `large_string` and `large_binary`, the types Orcapod uses):

| Connector | Column types it stores |
|---|---|
| `SQLiteConnector` | `bool`, `int64`, `double`, `large_string`, `large_binary` |
| `PostgreSQLConnector` | `bool`, `int32`, `int64`, `float`, `double`, `large_string`, `large_binary`, `date32`, `timestamp[us]`, `timestamp[us, tz=UTC]` |

Any other type (lists, structs, dates or timestamps on SQLite, narrower integers and floats,
logical types) is refused. PostgreSQL also refuses a column name or a table name longer than
63 bytes, which it would truncate (`UnstorableTableNameError` for a table). `SpiralDBConnector` stores other types as written, except a
timestamp with a time zone (SpiralDB drops the zone), and SpiralDB reserves the `__` prefix on
value columns (the `__record_id` key is allowed) and `.` in any column name.

Orcapod's own stored tables hold such columns. A function node's result table holds
list-valued executor columns and `__`-prefixed columns. A side-effect node's log holds a UTC
timestamp and `__data_context`, and an operator cache and a cached source hold
`__data_context` too. So a job refuses these nodes when it is built, before anything runs:

- every function node, on all three connectors;
- every side-effect node, on SQLite and SpiralDB;
- every operator cache (an operator node with caching on) and cached source, on SpiralDB.

An operator cache or cached source on a SQL connector is refused when it is written, before
anything is stored, if one of its columns has a type or name the connector does not store. On
PostgreSQL that is every operator cache with caching on: its system-tag column names
(`_tag_source_id::` and a 64-character hash) are longer than 63 bytes.
A table's name is its Orcapod path joined by `__`, and every node table and cached source path
holds a 64-character hash, so on PostgreSQL only tables you write through `ConnectorArrowDatabase`
yourself under short paths fit; reading an existing table with `PostgreSQLTableSource` is
unaffected. Use
`InMemoryArrowDatabase` or `DeltaTableDatabase` for such jobs. Storing these columns on
connector databases is planned for a later release.

```python
from orcapod.databases import ConnectorArrowDatabase, SQLiteConnector
from orcapod.errors import UnstorableColumnError

sqlite_database = ConnectorArrowDatabase(SQLiteConnector(":memory:"))
sql_job = PipelineJob(name="bmi", database=sqlite_database)
try:
    with sql_job:
        compute_bmi.pod(mice(cohort_a), label="bmi")
except UnstorableColumnError as exc:
    print(exc)
# node 'bmi': a SQLiteConnector database cannot store column '__pf_exec_executor_info' of Arrow type large_list<...>: SQLite has no column type for it. Use an InMemoryArrowDatabase or a DeltaTableDatabase instead.
```

## What a re-run computes

A stored result is reused when the function's identity (its name, its output schema and its
**major** version), the input values of the row, and the job's name and database are all
unchanged. Which source, and which steps before the function, the values came through does
not matter.

```python
cohort_a.append({"subject_id": "mouse_03", "weight": 27.8, "height": 0.11})

calls = []


@function_pod(output_keys="bmi")
def compute_bmi(weight: float, height: float) -> float:
    calls.append(weight)
    return weight / height**2


job = PipelineJob(name="bmi", database=database)
with job:
    compute_bmi.pod(mice(cohort_a), label="bmi")
job.run()

print(job.nodes["bmi"].as_table().num_rows, len(calls))
# 3 1
```

Three rows in the result, one call: the two rows stored by the first job were reused, even
though `compute_bmi` was defined again. The function's source code is not part of its
identity. **Editing a function body does not recompute its results**; raise the major
version (`@function_pod(..., version="v1.0")`; the default is `"v0.0"`) when you change what
a function computes. See [Identity & Hashing](identity.md#change-detection).

Running the same job object again does even less. A node that already holds a current run
is not run again, so a second `job.run()` computes, reads and writes nothing:

```python
print(job.run())
# Run of job 'bmi': nothing to do; every node holds a current run.
#   bmi: 3 rows: held from the previous run
```

Each run first asks every source it reads whether its data changed (a `CSVSource` checks
its file, for example), and the nodes below a source that changed run again. An in-memory
source such as `DictSource` cannot see its data change: build a new source object and bind
it with `job.bind(sources=...)`. See
[Running again: what a run runs](function-pods.md#running-again-what-a-run-runs) and
[What a source does at a run](sources.md#what-a-source-does-at-a-run).

## `Pipeline`: a blueprint without data

A `Pipeline` records steps the same way, but has no database and never runs. It is a
**blueprint**. Its sources become named slots, and a job is made from it by filling them:

```python
from orcapod import Pipeline

pipeline = Pipeline("bmi")
with pipeline:
    compute_bmi.pod(mice(cohort_a), label="bmi")

cohort_b = [{"subject_id": "mouse_11", "weight": 24.0, "height": 0.11}]

job_a = PipelineJob.from_pipeline(pipeline, database=database, sources={"mice": mice(cohort_a)})
job_b = PipelineJob.from_pipeline(pipeline, database=database, sources={"mice": mice(cohort_b)})
job_a.run()
job_b.run()

print(job_a.nodes["bmi"].as_table().num_rows, job_b.nodes["bmi"].as_table().num_rows)
# 3 1
```

`sources` maps a source slot's name (the `source_id` used when the blueprint was recorded)
to the source to use. A source bound to a slot must have the slot's tag and data columns
with the same Python types (a column the slot declares `T | None` also accepts `T`); any
other source raises `SourceSpecMismatchError`, and an unknown slot name raises `ValueError`.
Binding a source never changes a node's schema or where its results are stored.

A job made without sources has **unbound** slots. It can still be run: nodes that depend on
an unbound source are skipped, and `job.unbound_sources` lists what is missing. Bind later
with `job.bind(...)`:

```python
job_c = PipelineJob.from_pipeline(pipeline, database=database)
print(job_c.unbound_sources)
# ['mice']

job_c.bind(sources={"mice": mice(cohort_b)})
print(job_c.unbound_sources)
# []
job_c.run()
print(job_c.nodes["bmi"].as_table().num_rows)
# 1
```

Every job also carries its blueprint as `job.pipeline`. That object has `show_graph()`,
which draws the pipeline and needs the Graphviz `dot` program to be installed.

## Saving and loading

`job.save(path)` writes the job to a JSON file: the graph, the database's location and how
each source was configured. `PipelineJob.load(path)` reads it back, for example in a later
Python session. An `InMemoryArrowDatabase` has no location to record, so here the database
is passed again; `PipelineJob.load(path, database=...)` is also how you point a loaded job
at a database that has moved.

```python
job_a.save("bmi_job.json")

loaded = PipelineJob.load("bmi_job.json", database=database)
print(loaded.nodes["bmi"].as_table().num_rows)  # not run in this process: empty
# 0
print(loaded.nodes["bmi"].get_all_records().num_rows)
# 4
```

A loaded job is for **reading stored results**. The file holds the names of your functions,
not their code, so a loaded job cannot compute:

- Its nodes have not run in this process, so they are empty. `get_all_records()` and
  `as_source()` read what the database holds for the node: here the three rows of cohort A
  and the one of cohort B, because both cohorts were read through a source named `"mice"`.
- `loaded.run()` raises `RuntimeError`. To compute, run the code that defines the pipeline.
- `loaded.run(load_only=True)` fills the nodes with the stored results for the sources'
  current rows, computing nothing. An in-memory source such as `DictSource` is not saved
  with the job, so bind it first:

```python
try:
    loaded.run()
except RuntimeError:
    print("a loaded job cannot compute")
# a loaded job cannot compute

loaded.bind(sources={"mice": mice(cohort_a)})
summary = loaded.run(load_only=True)
print(loaded.nodes["bmi"].as_table().num_rows, summary.nodes["bmi"].not_stored)
# 3 0
```

The full rules (which sources are rebuilt from the file, operators, side effects, a
database that holds other rows than the file expects) are in
[Saving and loading a job](function-pods.md#saving-and-loading-a-job).

`Pipeline.save(path)` and `Pipeline.load(path)` save and load a blueprint.

## How it connects to other concepts

- A job records applications of [Function Pods](function-pods.md) and
  [Operators](operators.md) to [Streams](streams.md), starting from [Sources](sources.md).
- What is reused and what is recomputed follows from [Identity & Hashing](identity.md).
- Files as pipeline inputs and outputs are covered in [File and Path Types](file-types.md).
