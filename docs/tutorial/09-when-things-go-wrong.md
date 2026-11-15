# Part 9: When things go wrong

Two kinds of things go wrong. Some mistakes Orcapod catches before any data is touched: a
misspelt column, a function that does not fit its input. Others happen in the middle of a
run: one file out of a thousand is corrupt and your function raises. This part shows how
each kind appears and what to do about it.

You will:

- recognise the errors Orcapod raises while a pipeline is being built
- see what a job does when a function raises for some rows
- choose between "carry on" and "stop at the first failure"
- find out which rows failed, and why, after a run

```python
from workshop_data import make_dataset
from orcapod import PipelineJob, function_pod
from orcapod.databases import InMemoryArrowDatabase
from orcapod.sources import CSVSource

data_dir = make_dataset()

units = CSVSource(
    str(data_dir / "units.csv"),
    tag_columns=["subject_id", "session", "probe", "unit"],
    source_id="units",
)
sessions = CSVSource(
    str(data_dir / "sessions.csv"),
    tag_columns=["subject_id", "session"],
    source_id="sessions",
)
```

## Errors while building

Most mistakes are caught at the line that makes them, because Orcapod checks column names
and types as soon as you connect two things. You have met these already:

| You did | Raised | Where you saw it |
|---|---|---|
| Named a tag column that does not exist | `ValueError: tag_columns not found in table` | Part 1 |
| Made every column a tag | `ValueError: No data columns found in the table` | Part 1 |
| Left out a type annotation | `ValueError: Parameter '...' has no type annotation` | Part 2 |
| Gave a pod a stream with a missing or differently typed column | `ValueError: Incoming data schema ... is not compatible with expected input schema ...: ...` | Part 2 |
| Joined streams that share a data column | `InputValidationError: ... should not have overlapping keys` | Part 3 |
| Grouped by a data column | `InputValidationError: GroupBy: ... are not tag columns` | Part 4 |
| Ran a job without a database | `ValueError: PipelineJob.run() requires a database` | Part 5 |

The end of a pod's message names the problem, with the column and both types:
`required parameter 'duration_s' has no matching column`, or
`column 'duration_s' has type float, the function expects int`. Data columns the function
does not take are not a problem: the pod drops them.

The habit that makes these quick to fix: when a pod or operator refuses a stream, print
`stream.keys()` or `stream.output_schema()` for the stream you passed, and compare it with
what the step expects.

The [Troubleshooting](../troubleshooting.md) page lists every one of these with its cause
and fix, together with the problems that give **no** error, such as stale results after
editing a function (Part 6).

## A function that fails for some rows

Suppose the rate function refuses spike counts it considers implausible:

```python
calls = []


@function_pod(output_keys="rate_hz")
def firing_rate(n_spikes: int, duration_s: float) -> float:
    calls.append(n_spikes)
    if n_spikes > 1200:
        raise ValueError(f"implausible spike count: {n_spikes}")
    return n_spikes / duration_s


def build_job(database):
    job = PipelineJob(name="workshop", database=database)
    with job:
        firing_rate.pod(
            units.select_data_columns(["n_spikes"]),
            sessions.select_data_columns(["duration_s"]),
            label="rates",
        )
    return job
```

Three of the 64 units have more than 1200 spikes.

**Without a job**, the exception simply propagates, as it would from any Python function.
The stream cannot be produced:

```python
in_memory = firing_rate.pod(
    units.select_data_columns(["n_spikes"]),
    sessions.select_data_columns(["duration_s"]),
)
try:
    in_memory.as_table()
except ValueError as err:
    print(err)
# implausible spike count: 1221
```

**Inside a job**, the default is different. The run carries on:

```python
database = InMemoryArrowDatabase()
calls.clear()

job = build_job(database)
summary = job.run()
# RowFailuresWarning: 3 of 64 rows failed in node 'rates' (first error: ValueError: implausible spike count: 1221). Full traceback: job.last_run.print_traceback("rates").
print(summary)
# Run of job 'workshop': 3 rows failed.
#   rates: 64 rows: 60 computed, 1 from store, 3 failed
#     first error: ValueError: implausible spike count: 1221
#     Full traceback: job.last_run.print_traceback("rates")
print(job.nodes["rates"].as_table().num_rows)
# 61
```

`run()` did not raise. It skipped the three failing rows, computed and stored the other 61,
and at the end issued one `RowFailuresWarning` and returned a **run summary** (the job also
keeps it as `job.last_run`). `job.last_run.print_traceback("rates")` prints the full
traceback of the first failure. (Why "1 from store" in a first run? Two units have the same
spike count and the same session duration. Their inputs are identical, so the second one
reuses the result computed for the first.)

This default suits long runs: one bad file should not throw away a night of computation.
It also means that **a job that finishes is not a job in which everything succeeded**.
Read the warning and the summary; in a script, check `summary.ok`, which is `False` when
any row failed.

A row also fails when the function returns a value of a different type than it declares,
for example a `float` from a function annotated `-> int`. That raises an `OutputTypeError`
(from `orcapod.errors`) naming the function, the output and both types; Orcapod does not
convert the value for you. Inside a job it is skipped and reported like any other failure.

Failures are not stored. Run again and the failed rows are tried again, and only those (they
fail again, so the warning appears again):

```python
calls.clear()
job = build_job(database)
job.run()
print(len(calls))
# 3
```

## Stop at the first failure

While developing, you usually want the opposite: stop immediately and show me the error.
Pass an orchestrator with `error_policy="fail_fast"`:

```python
from orcapod.pipeline import SyncPipelineOrchestrator

job = build_job(InMemoryArrowDatabase())
try:
    job.run(orchestrator=SyncPipelineOrchestrator(error_policy="fail_fast"))
except ValueError as err:
    print(err)
# implausible spike count: 1221
```

The **orchestrator** is the part of Orcapod that walks through the nodes of a job and
executes them. You have been using the default one all along. `"fail_fast"` re-raises the
first exception a function raises; `"continue"`, the default, skips the row.

## Which rows failed, and why?

Reading tracebacks off the screen does not scale. An **observer** records what happens
during a run in a table you can query afterwards. `StatusObserver` records the state of
every row; `LoggingObserver` records the traceback and captured output of failures.
`CompositeObserver` lets you use both:

```python
import polars as pl

from orcapod.pipeline import CompositeObserver, LoggingObserver, StatusObserver

status = StatusObserver(status_database=InMemoryArrowDatabase())
logs = LoggingObserver(log_database=InMemoryArrowDatabase())

job = build_job(InMemoryArrowDatabase())
job.run(observer=CompositeObserver(status, logs))

status_table = pl.from_arrow(status.get_status())
print(status_table.group_by("_status_state").len().sort("_status_state").rows())
# [('FAILED', 3), ('RUNNING', 64), ('SUCCESS', 61)]
```

Every row is recorded as `RUNNING` when it starts, and then as `SUCCESS` or `FAILED`. (On a
re-run, rows read from the database are recorded as `CACHED`.) The failed rows, with their
tags and the error message:

```python
failed = status_table.filter(pl.col("_status_state") == "FAILED").sort(
    "subject_id", "session", "probe"
)
print(failed.select("subject_id", "session", "probe", "unit", "_status_error_summary").rows())
# [('mouse_01', 's2', 'imec0', '2', 'implausible spike count: 1221'), ('mouse_01', 's2', 'imec1', '2', 'implausible spike count: 1205'), ('mouse_02', 's2', 'imec1', '3', 'implausible spike count: 1233')]
```

Tag values are stored as text in these tables, which is why `unit` shows as `'2'`.

The log table has the full traceback of each failure:

```python
log_table = pl.from_arrow(logs.get_logs())
failures = log_table.filter(~pl.col("_log_success"))
print(len(failures))
# 3
print(failures["_log_traceback"][0].strip().splitlines()[-1])
# ValueError: implausible spike count: ...
```

It also has columns for what the function printed (`_log_stdout_log`, `_log_stderr_log`)
and logged (`_log_python_logs`) while it ran.

Here the observers write to in-memory databases. Give them a `DeltaTableDatabase` and the
record of every run is kept on disk next to your results.

## Fix and re-run

Once you know what went wrong, fix the function, or the data, and run again. Here the limit
was wrong, so the check is removed. Only the three rows that never got a result are
computed:

```python
@function_pod(output_keys="rate_hz")
def firing_rate(n_spikes: int, duration_s: float) -> float:
    calls.append(n_spikes)
    return n_spikes / duration_s


calls.clear()
job = build_job(database)
job.run()
print(job.nodes["rates"].as_table().num_rows, len(calls))
# 64 3
```

The 61 results stored earlier were reused, although the function body changed. In this case
that is what you want, since the calculation for those rows is the same. If your fix changes
what the function returns for rows that had succeeded, bump the major version (Part 6).

## What to remember

- Mistakes in how a pipeline is put together raise at the line that makes them. Print the
  stream's `keys()` and compare.
- Inside a job, a function that raises loses that row and the run continues. At the end a
  `RowFailuresWarning` and the run summary say how many rows failed. **Read them.**
- `SyncPipelineOrchestrator(error_policy="fail_fast")` stops at the first failure. Use it
  while developing.
- `StatusObserver` and `LoggingObserver` tell you which rows failed and why.
- Failed rows are retried on the next run; successful ones are not.

## Exercises

**1.** Each line below has one mistake. Predict the error, then run it and check. Fix each
one.

(a) `CSVSource(str(data_dir / "sessions.csv"), tag_columns=["subject", "session"])`<br>
(b) `firing_rate.pod(sessions)`<br>
(c) `units.join(units)`

??? success "Solution"
    ```python
    from orcapod.errors import InputValidationError

    try:
        CSVSource(str(data_dir / "sessions.csv"), tag_columns=["subject", "session"])
    except ValueError as err:
        print("a:", err)
    # a: tag_columns not found in table: {'subject'}. Available columns: ['subject_id', 'session', 'date', 'duration_s']

    try:
        firing_rate.pod(sessions)
    except ValueError as err:
        print("b:", str(err).split("): ")[-1])
    # b: required parameter 'n_spikes' has no matching column

    try:
        units.join(units)
    except InputValidationError as err:
        print("c:", str(err).split(", but")[0])
    # c: Input streams are not compatible: Datas should not have overlapping keys
    ```

    (a) The column is called `subject_id`. (b) `n_spikes` is in `units`, not `sessions`:
    pass both, `firing_rate.pod(units, sessions)`. (c) Both sides have the same data
    columns; a stream joined with itself is rarely what you want, but if two tables really
    share a column name, rename one side or use `MergeJoin` (Part 3).

**2.** Run the failing version of `firing_rate` (the one with the 1200 limit) in a job with
only a `StatusObserver`, and print how many rows ended in each state.

??? success "Solution"
    ```python
    @function_pod(output_keys="rate_hz")
    def firing_rate(n_spikes: int, duration_s: float) -> float:
        if n_spikes > 1200:
            raise ValueError(f"implausible spike count: {n_spikes}")
        return n_spikes / duration_s


    only_status = StatusObserver(status_database=InMemoryArrowDatabase())
    job = build_job(InMemoryArrowDatabase())
    job.run(observer=only_status)

    states = pl.from_arrow(only_status.get_status())
    print(states.group_by("_status_state").len().sort("_status_state").rows())
    # [('FAILED', 3), ('RUNNING', 64), ('SUCCESS', 61)]
    ```

**3.** A pipeline has two steps: `rates`, then a per-session mean over `rates`. If `rates`
fails for one unit under the default policy, what does the session mean of that unit's
session contain?

??? success "Solution"
    The mean of the units that succeeded. The failed row is absent from `rates`, so
    `group_by` gathers seven units for that session where there should be eight, and the
    mean is computed over seven. `job.run()` reports failed rows in its warning and its
    summary, but raises no exception, and nothing in the mean's row says that a unit is
    missing.

    ```python
    @function_pod(output_keys=["n_units", "mean_rate_hz"])
    def count_and_mean(rate_hz: list[float]) -> tuple[int, float]:
        return len(rate_hz), sum(rate_hz) / len(rate_hz)


    job = PipelineJob(name="two_steps", database=InMemoryArrowDatabase())
    with job:
        rates = firing_rate.pod(
            units.select_data_columns(["n_spikes"]),
            sessions.select_data_columns(["duration_s"]),
            label="rates",
        )
        count_and_mean.pod(
            rates.group_by(["subject_id", "session"]),
            label="session_means",
        )
    job.run()

    means = job.nodes["session_means"].as_pandas_df()
    print(means.sort_values(["subject_id", "session"], ignore_index=True)[["subject_id", "session", "n_units"]])
    #   subject_id session  n_units
    # 0   mouse_01      s1        8
    # 1   mouse_01      s2        6
    # 2   mouse_02      s1        8
    # 3   mouse_02      s2        7
    # 4   mouse_03      s1        8
    # 5   mouse_03      s2        8
    # 6   mouse_04      s1        8
    # 7   mouse_04      s2        8
    ```

    This is the strongest reason to look at the status table after a run, or to use
    `fail_fast`: a partial failure upstream becomes a plausible-looking number downstream.
    Returning the group size next to the summary, as here, is a cheap safeguard.

Next: [Part 10: Where next](10-where-next.md).
