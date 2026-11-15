# Part 5: Keeping results

Everything in Parts 1 to 4 was computed in memory. Close Python and the results are gone;
run the script again and every function runs again. That is fine for 64 rows. It is not fine
when one function call takes a minute and you have ten thousand of them.

A **`PipelineJob`** runs the same steps against a database. Each result is stored together
with a record of the inputs it was computed from. The next time the pipeline runs, results
that are already stored are read back instead of being computed again.

You will:

- record a pipeline inside a `PipelineJob` and run it
- read results from the job's **nodes**
- keep the results on disk, and see a second run compute nothing

```python
from workshop_data import make_dataset
from orcapod import PipelineJob, function_pod
from orcapod.sources import CSVSource

data_dir = make_dataset()

sessions = CSVSource(
    str(data_dir / "sessions.csv"),
    tag_columns=["subject_id", "session"],
    source_id="sessions",
)
units = CSVSource(
    str(data_dir / "units.csv"),
    tag_columns=["subject_id", "session", "probe", "unit"],
    source_id="units",
)

calls = []  # every function call adds an entry, so we can see when functions really run


@function_pod(output_keys="rate_hz")
def firing_rate(n_spikes: int, duration_s: float) -> float:
    calls.append("firing_rate")
    return n_spikes / duration_s


@function_pod(output_keys="mean_rate_hz")
def mean_rate(rate_hz: list[float]) -> float:
    calls.append("mean_rate")
    return sum(rate_hz) / len(rate_hz)
```

These are the two functions from Part 4. The `calls` list is only there for teaching: it
lets us count how often each function was really called.

## Record, then run

A job needs a **database** to put its results in. Start with one that lives in memory. Then write the pipeline exactly as before, inside a `with` block:

```python
from orcapod.databases import InMemoryArrowDatabase

database = InMemoryArrowDatabase()

job = PipelineJob(name="workshop", database=database)
with job:
    rates = firing_rate.pod(
        units.select_data_columns(["n_spikes"]),
        sessions.select_data_columns(["duration_s"]),
        label="rates",
    )
    mean_rate.pod(
        rates.group_by(["subject_id", "session"]).select_data_columns(["rate_hz"]),
        label="session_means",
    )

print(len(calls))
# 0
```

Nothing has been computed. Inside the `with` block the job only **records** which operators
and pods are applied to which streams. The one new thing in the code is `label=`: a name for
the step, which you will use to find its results.

`run()` executes what was recorded and writes every result to the database:

```python
job.run()

print(len(calls))
# 71
print(calls.count("firing_rate"), calls.count("mean_rate"))
# 63 8
```

`mean_rate` ran 8 times, once per session. `firing_rate` ran 63 times for 64 units. That is
not a mistake: two units happen to have the same spike count and the same session duration.
Orcapod stores a result under the function and the *values* of its inputs, so the second of
those two units found its answer already stored.

## Nodes: where the results are

Each recorded step is a **node** of the job. `job.nodes` lists them by name:

```python
print(sorted(job.nodes))
# ['GroupBy', 'Join', 'SelectDataColumns_1', 'SelectDataColumns_2', 'SelectDataColumns_3', 'rates', 'session_means', 'sessions', 'units']
```

(`sorted` is only there to make this page reproducible: `list(job.nodes)` gives the same
names, in an order that can differ from one Python session to the next.)

- A source is listed under its `source_id`.
- A function pod is listed under the `label` you gave it. Without a label it gets the
  function's name.
- Operators are named after their kind, numbered when there are several.

A node is a stream. Everything from Part 1 works on it:

```python
means = job.nodes["session_means"]
print(means.output_schema())
# (Schema({'subject_id': str, 'session': str}), Schema({'mean_rate_hz': float}))
print(means.as_pandas_df().sort_values(["subject_id", "session"], ignore_index=True))
#   subject_id session  mean_rate_hz
# 0   mouse_01      s1      6.033333
# 1   mouse_01      s2      6.793750
# 2   mouse_02      s1      5.845833
# 3   mouse_02      s2      7.506250
# 4   mouse_03      s1      4.906944
# 5   mouse_03      s2      5.059375
# 6   mouse_04      s1      5.136458
# 7   mouse_04      s2      5.129167
print(job.nodes["rates"].as_table().num_rows)
# 64
```

!!! warning "Read nodes after `run()`"
    A node shows the rows of the job's run in this Python session. Before `run()`, every node
    except the sources gives you an empty table without complaint; only `head()` and the
    notebook preview say "not run". If a node looks empty, check that you ran the job.

## Run it again

Record the same pipeline in a new job over the same database. A small function saves retyping:

```python
def build_job(database):
    job = PipelineJob(name="workshop", database=database)
    with job:
        rates = firing_rate.pod(
            units.select_data_columns(["n_spikes"]),
            sessions.select_data_columns(["duration_s"]),
            label="rates",
        )
        mean_rate.pod(
            rates.group_by(["subject_id", "session"]).select_data_columns(["rate_hz"]),
            label="session_means",
        )
    return job


job = build_job(database)
job.run()

print(len(calls))
# 71
print(job.nodes["session_means"].as_table().num_rows)
# 8
```

Still 71. The second run called no function at all. Every result was found in the database,
and the node holds the same 8 rows.

## Keep results on disk

`InMemoryArrowDatabase` disappears with the Python process. `DeltaTableDatabase` writes to a
directory and is what you use for real work:

```python
import shutil

from orcapod.databases import DeltaTableDatabase

shutil.rmtree("workshop_db", ignore_errors=True)  # start empty, also when you rerun this page
job = build_job(DeltaTableDatabase("workshop_db"))
job.run()
print(len(calls))
# 142
```

This is a different, empty database, so everything was computed once more: 71 further calls.
(You will see many `WARN deltalake_core::writer::stats` lines while it writes. They are
harmless; see [Part 0](00-setup.md#messages-you-can-ignore).)

Now do what a new Python session would do: make a new database object for the same directory,
build the job, run it.

```python
job = build_job(DeltaTableDatabase("workshop_db"))
job.run()

print(len(calls))
# 142
means = job.nodes["session_means"].as_pandas_df()
print(means.sort_values(["subject_id", "session"], ignore_index=True).head(2))
#   subject_id session  mean_rate_hz
# 0   mouse_01      s1      6.033333
# 1   mouse_01      s2      6.793750
```

No calls. The results came from the files in `workshop_db/`. Rows read back from a database
are not guaranteed to come in the order they were first computed in, which is why the sample
sorts them before printing. Sort whenever the order matters to you.

```python
from pathlib import Path

print(sorted(p.name for p in Path("workshop_db").iterdir()))
# ['workshop']
print(sorted(p.name for p in Path("workshop_db/workshop").iterdir()))
# ['_result', 'firing_rate', 'mean_rate']
```

The first level is the job's `name`. Below it, `_result` holds each function's results, and
the directory named after each function records which inputs the job gave it. Two jobs with
different names over the same directory do not share results. You never need to read
these files yourself, and deleting the whole directory is safe: it only means the next run
computes everything again.

## What to remember

- `PipelineJob(name=..., database=...)`, a `with` block to record, `job.run()` to execute.
- Give pods a `label`; read results from `job.nodes[label]` after `run()`.
- Results are stored per function and per input values. A second run over the same
  database computes only what is missing.
- `InMemoryArrowDatabase` for trying things out; `DeltaTableDatabase("directory")` to keep
  results between sessions.

The concept page [Pipelines and jobs](../concepts/pipeline-job.md) covers what this part left
out: reusable pipeline blueprints, binding sources later, and saving a job to a file.

## Exercises

**1.** Run a pipeline, quit Python, and run it again. Put the script below in a file
`run_pipeline.py` and run `python run_pipeline.py` twice. What does each run print?

```python
from workshop_data import make_dataset
from orcapod import PipelineJob, function_pod
from orcapod.databases import DeltaTableDatabase
from orcapod.sources import CSVSource

data_dir = make_dataset()
n_calls = 0


@function_pod(output_keys="depth_mm")
def to_mm(depth_um: int) -> float:
    global n_calls
    n_calls += 1
    return depth_um / 1000


job = PipelineJob(name="depths", database=DeltaTableDatabase("exercise_db"))
with job:
    units = CSVSource(
        str(data_dir / "units.csv"),
        tag_columns=["subject_id", "session", "probe", "unit"],
        source_id="units",
    )
    to_mm.pod(units.select_data_columns(["depth_um"]), label="depth_mm")
job.run()

print("rows:", job.nodes["depth_mm"].as_table().num_rows)
# rows: 64
print("function calls in this run:", n_calls)
# function calls in this run: 54
```

??? success "Solution"
    The first run prints `rows: 64` and `function calls in this run: 54`. Fifty-four, not
    64, because some units share a depth and identical inputs are computed once.

    The second run prints `rows: 64` and `function calls in this run: 0`. It is a new Python
    process, with a new job object and a new database object. The only thing it shares with
    the first run is the `exercise_db` directory, and that is enough.

    Delete `exercise_db/` and run the script once more: 54 calls again.

**2.** In the main example, what would `list(job.nodes)` show if you removed both `label=`
arguments? Predict, then try it with a new job over an `InMemoryArrowDatabase`.

??? success "Solution"
    The two function nodes are listed under their function names.

    ```python
    unlabelled = PipelineJob(name="unlabelled", database=InMemoryArrowDatabase())
    with unlabelled:
        rates = firing_rate.pod(
            units.select_data_columns(["n_spikes"]),
            sessions.select_data_columns(["duration_s"]),
        )
        mean_rate.pod(
            rates.group_by(["subject_id", "session"]).select_data_columns(["rate_hz"])
        )
    print(sorted(unlabelled.nodes))
    # ['GroupBy', 'Join', 'SelectDataColumns_1', 'SelectDataColumns_2', 'SelectDataColumns_3', 'firing_rate', 'mean_rate', 'sessions', 'units']
    ```

    Labels become useful when the same function is applied in two places of one pipeline.

**3.** Create a job without a database, record one step, and call `run()`. Read the error and do
what it suggests.

??? success "Solution"
    ```python
    no_database = PipelineJob(name="no_database")
    with no_database:
        firing_rate.pod(
            units.select_data_columns(["n_spikes"]),
            sessions.select_data_columns(["duration_s"]),
            label="rates",
        )

    try:
        no_database.run()
    except ValueError as err:
        print(err)
    # PipelineJob.run() requires a database. Call job.bind(database=db) before run(). For transient (in-memory) storage use job.bind(database=InMemoryArrowDatabase()).

    no_database.bind(database=InMemoryArrowDatabase())
    no_database.run()
    print(no_database.nodes["rates"].as_table().num_rows)
    # 64
    ```

Next: [Part 6: Re-runs and caching](06-reruns-and-caching.md).
