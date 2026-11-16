# Part 8: Inspecting and tracing

A number in a figure raises questions. Which steps produced it? From which rows of which
table? When was it computed, and with what? Because Orcapod runs every step itself, it
keeps the answers. This part shows where to find them.

You will:

- walk backwards from a result to the sources it came from
- reveal the provenance columns that every stream carries
- read the record Orcapod keeps of each function call
- start a second pipeline from the stored results of the first

```python
from workshop_data import make_dataset
from orcapod import PipelineJob, function_pod
from orcapod.databases import InMemoryArrowDatabase
from orcapod.sources import CSVSource

data_dir = make_dataset()
database = InMemoryArrowDatabase()


@function_pod(output_keys="rate_hz")
def firing_rate(n_spikes: int, duration_s: float) -> float:
    return n_spikes / duration_s


@function_pod(output_keys="mean_rate_hz")
def mean_rate(rate_hz: list[float]) -> float:
    return sum(rate_hz) / len(rate_hz)


job = PipelineJob(name="workshop", database=database)
with job:
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
    rates = firing_rate.pod(
        units.select_data_columns(["n_spikes"]),
        sessions.select_data_columns(["duration_s"]),
        label="rates",
    )
    mean_rate.pod(
        rates.group_by(["subject_id", "session"]).select_data_columns(["rate_hz"]),
        label="session_means",
    )
job.run()
```

This is the full pipeline from Part 5.

## Which steps produced this?

Every node knows the nodes it reads from, in `node.upstreams`, and its own name, in
`node.label`. Following `upstreams` until there are none left takes you from a result back
to the sources:

```python
def print_lineage(node, depth=0):
    print("  " * depth + node.label)
    for upstream in node.upstreams:
        print_lineage(upstream, depth + 1)


print_lineage(job.nodes["session_means"])
# session_means
#   SelectDataColumns_2
#     GroupBy
#       rates
#         Join
#           SelectDataColumns_3
#             units
#           SelectDataColumns_1
#             sessions
```

Read from the bottom up, this is the pipeline as you wrote it, including the join that the
two-stream call to `firing_rate` added for you. When a job has several steps of the same
kind, it numbers them (`SelectDataColumns_1`, ...); the numbers tell the steps apart but do
not follow the order you wrote them in.

If you have Graphviz installed (the `dot` program), the job's pipeline can draw the same
graph as a picture:

<!-- docs-test: skip -->

```python
job.pipeline.show_graph()
```

`show_graph` is a method of `job.pipeline`, not of the job. Without Graphviz it raises
`ExecutableNotFound`; use `print_lineage` instead.

## Which rows did a value come from?

Every stream carries hidden columns that record where its rows and values came from. They
are left out by default. Ask for them with the `columns` argument of `as_table`.

**Source-info columns.** For each data column there is a companion column named
`_source_<column>`. On a source it holds the source's name, the row number and the column:

```python
table = job.nodes["units"].as_table(columns={"source": True})
print(table.column_names)
# ['subject_id', 'session', 'probe', 'unit', 'region', 'depth_um', 'n_spikes', '_source_region', '_source_depth_um', '_source_n_spikes']
print(table.column("_source_n_spikes").to_pylist()[5])
# units::row_5::n_spikes
```

Row 5 of the source you named `units`, column `n_spikes`. This is what the `source_id` you
have been passing since Part 1 is for.

**System tags.** Each source also adds two hidden tag columns to every row: one with the
source's name, one with an identifier of the row. They travel with the row through every
operator and function pod. When two streams are joined, the joined row carries the system
tags of both. So for any row of `rates` you can read off which sources it depends on:

```python
import polars as pl

table = pl.from_arrow(job.nodes["rates"].as_table(columns={"system_tags": True}))
source_columns = [c for c in table.columns if c.startswith("_tag_source_id")]
print(len(source_columns))
# 2
print(table.select(source_columns).unique().rows())
# [('units', 'sessions')]
```

Two system-tag columns, because two sources feed `rates`; every row depends on one row of
`sessions` and one row of `units`. The column names end in a long hash that identifies the
source's position in the pipeline, which is why the sample selects them by prefix.

The companion `_tag_record_id::...` columns identify the individual source row. They hold
compact binary identifiers that Orcapod uses internally to tell rows apart; they are not
meant to be read by eye.

!!! note "After `group_by`"
    A grouped row comes from many source rows. Its system tags are folded into a single
    digest per column, so they still distinguish one group from another but no longer name
    the individual rows. To see which rows went into a group, look at the stream before the
    grouping (`job.nodes["rates"]`): every row there with the group's `subject_id` and
    `session` is a member, and the list elements are in the order of their `probe` and
    `unit`, as you saw in Part 4.

`as_table(all_info=True)` shows every hidden column at once.

## How was it computed?

For each function, the database holds one record per computed result, with details of the
call. `get_all_records` returns them; ask for the `meta` columns to see the details:

```python
records = pl.from_arrow(job.nodes["rates"].get_all_records(columns={"meta": True}))
meta = [c for c in records.columns if c.startswith("__")]
print(meta)
# ['__data_id', '__input_data_hash', '__output_data_hash', '__data_context', '__computed', '__is_ephemeral', '__input_data_hash_right', '__pf_var_function_name', '__pf_var_function_version', '__pf_var_function_signature_hash', '__pf_var_function_content_hash', '__pf_var_git_hash', '__pf_exec_executor_type', '__pf_exec_executor_info', '__pf_exec_python_version', '__pf_exec_extra_info', '__pod_ts']
```

The ones you are most likely to want:

| Column | Meaning |
|---|---|
| `__pf_var_function_name` | The function that produced the result |
| `__pod_ts` | When the result was computed |
| `__pf_exec_python_version` | The Python version it ran under |
| `__pf_var_git_hash` | The git commit of the code, when run inside a git repository |
| `__computed` | `True` if this row was computed, `False` if it reused the result of an identical input |
| `__input_data_hash` | The hash of the input values: the key the result is stored under (Part 6) |

```python
print(records.select("__pf_var_function_name", "__pf_exec_executor_type").unique().rows())
# [('firing_rate', 'local')]
print(records["__computed"].sum(), "of", len(records))
# 63 of 64
print(records["__pod_ts"].dtype)
# Datetime(time_unit='us', time_zone='UTC')
```

63 of 64: the count from Part 5 again, now visible in the records. One row reused the
result of another row with the same inputs.

### `as_table` versus `get_all_records`

They answer different questions.

- `node.as_table()` is the node's output **for the inputs of this job's run**: 64 rows here.
  Before the job has run in this Python session it has no rows at all, even when the
  database already holds results.
- `node.get_all_records()` is **everything stored** for this function in this job's part of
  the database, including results from earlier runs over other inputs. It does not need a
  run.

After the "two more units" run of Part 6, a job over the original file, once run, has 64
rows in `as_table()`, while `get_all_records()` has 66.

## Starting a second pipeline from stored results

A finished analysis is often the input of the next one. `as_source()` turns the stored
results of a node into a source:

```python
session_means = job.nodes["session_means"].as_source()
print(type(session_means).__name__)
# DerivedSource
print(session_means.keys())
# (('subject_id', 'session'), ('mean_rate_hz',))
```

A `DerivedSource` is a source like those of Part 1, so it can be joined, filtered and fed to
pods. Here the next stage flags sessions with a low mean rate:

```python
@function_pod(output_keys="is_low")
def is_low(mean_rate_hz: float) -> bool:
    return mean_rate_hz < 5.5


flags = is_low.pod(session_means).as_pandas_df()
print(flags.sort_values(["subject_id", "session"], ignore_index=True))
#   subject_id session  is_low
# 0   mouse_01      s1   False
# 1   mouse_01      s2   False
# 2   mouse_02      s1   False
# 3   mouse_02      s2   False
# 4   mouse_03      s1    True
# 5   mouse_03      s2    True
# 6   mouse_04      s1    True
# 7   mouse_04      s2    True
```

The next stage does not re-run the first. It reads what the first one stored. This is how
you split a large analysis into stages that different people, or different days, can own.
`as_source()` reads what the database holds, whether or not the node has run in this Python
session; for a node whose results were never stored, it has no rows. It reads every stored
result of the node, including those of earlier runs over other inputs, like
`get_all_records()` above; when you want only this run's output, use `as_table()`.

!!! warning "Not yet: a stored joined result as the source of a new job"
    The stage above runs in memory, so its own results are not stored. Recording it in a new
    `PipelineJob` does not work yet for `session_means`, or for any node downstream of a
    join: its rows still carry the provenance of both joined sources, and a job refuses such
    a source with `SourceSpecMismatchError` when it is bound. Support is planned (ITL-1127).
    Until then, to store a later stage, add it to the first job.

## What to remember

- `node.upstreams` and `node.label` walk the pipeline backwards from any result.
- `as_table(columns={"source": True})` and `columns={"system_tags": True}` reveal where
  values and rows came from; `all_info=True` shows everything.
- `get_all_records(columns={"meta": True})` shows how and when each result was computed.
- `as_table()` is the output of this job's run; `get_all_records()` is everything stored.
- `node.as_source()` starts a new pipeline from stored results.

## Exercises

**1.** Print the lineage of the `rates` node. How many sources does it depend on?

??? success "Solution"
    ```python
    print_lineage(job.nodes["rates"])
    # rates
    #   Join
    #     SelectDataColumns_3
    #       units
    #     SelectDataColumns_1
    #       sessions
    ```

    Two: `units` and `sessions`.

**2.** Which row of `sessions.csv` did the duration of `mouse_03`, session `s2` come from?
Use the source-info column of the `sessions` node.

??? success "Solution"
    ```python
    table = pl.from_arrow(job.nodes["sessions"].as_table(columns={"source": True}))
    row = table.filter((pl.col("subject_id") == "mouse_03") & (pl.col("session") == "s2"))
    print(row["_source_duration_s"].to_list())
    # ['sessions::row_5::duration_s']
    ```

    Row 5, counting data rows from 0: the sixth line after the header.

    Note the plain Polars filter on a table you have already taken out of Orcapod. Inside a
    pipeline you would write the same test as `stream.polars_filter(...)` (Part 3).

**3.** Using `as_source()`, compute the mean of the session means per animal (one row per
`subject_id`). Reuse `group_by` from Part 4.

??? success "Solution"
    ```python
    @function_pod(output_keys="animal_mean_hz")
    def mean_of_means(mean_rate_hz: list[float]) -> float:
        return sum(mean_rate_hz) / len(mean_rate_hz)


    means = job.nodes["session_means"].as_source()
    result = mean_of_means.pod(means.group_by(["subject_id"])).as_pandas_df()
    print(result.sort_values("subject_id", ignore_index=True))
    #   subject_id  animal_mean_hz
    # 0   mouse_01        6.413542
    # 1   mouse_02        6.676042
    # 2   mouse_03        4.983160
    # 3   mouse_04        5.132812
    ```

Next: [Part 9: When things go wrong](09-when-things-go-wrong.md).
