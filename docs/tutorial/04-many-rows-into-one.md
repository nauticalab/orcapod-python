# Part 4: Many rows into one

So far every function has worked on one row and produced one row. A summary is different:
the mean firing rate of a session needs *all* the units of that session at once. In Orcapod
this takes two steps. An operator gathers the rows that belong together into a single row
whose values are lists. Then an ordinary function pod computes over those lists.

You will:

- gather rows that share tag values with `group_by`
- write a function pod that takes lists
- compute the mean firing rate of every session, the result this tutorial is building
- gather rows by count with `batch`

```python
from workshop_data import make_dataset
from orcapod import function_pod
from orcapod.sources import CSVSource

data_dir = make_dataset()

subjects = CSVSource(
    str(data_dir / "subjects.csv"), tag_columns=["subject_id"], source_id="subjects"
)
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


@function_pod(output_keys="rate_hz")
def firing_rate(n_spikes: int, duration_s: float) -> float:
    return n_spikes / duration_s


rates = firing_rate.pod(
    units.select_data_columns(["n_spikes"]),
    sessions.select_data_columns(["duration_s"]),
)
```

`rates` is the stream from Part 2: one row per unit, 64 rows, with tags
`subject_id, session, probe, unit` and one data column `rate_hz`.

## Group by tags

`group_by` takes a list of tag columns. All rows with the same values in those columns
become one row:

```python
per_session = rates.group_by(["subject_id", "session"])

print(per_session.output_schema())
# (Schema({'subject_id': str, 'session': str}), Schema({'rate_hz': list[float]}))
print(per_session.as_table().num_rows)
# 8
```

64 unit rows became 8 session rows. Compare the schema with the one you started from:

- The columns you grouped by, `subject_id` and `session`, are the tags of the result. One
  row per session.
- The other tags, `probe` and `unit`, are gone. A session row covers many probes and units,
  so no single value of them describes the row, and `group_by` drops them.
- Every data column became a list: `rate_hz` is now `list[float]`, one element per unit of
  the session.

Here is the first row:

```python
tag, data = next(iter(per_session.iter_data()))
print(tag.as_dict())
# {'subject_id': 'mouse_01', 'session': 's1'}
print([round(r, 2) for r in data.as_dict()["rate_hz"]])
# [2.48, 5.43, 11.03, 9.98, 3.58, 4.98, 2.25, 8.52]
```

These are the eight rates of `mouse_01`'s first session, in the order of the tags that were
dropped: probe `imec0` units 0 to 3, then probe `imec1` units 0 to 3 (compare the first rows
of `rates` in Part 2). The members of a group are always in this order, whatever order the
input rows arrived in, but the list itself no longer says which probe or unit each rate
came from. If a later step needs that, it has to be a data column before you group; see
[GroupBy](../concepts/operators.md#groupby).

`group_by` is an operator, so it computed nothing. Every number in those lists was already
in `rates`.

## A function pod over lists

A function that takes a list is written like any other. Annotate the parameter as
`list[float]`:

```python
@function_pod(output_keys="mean_rate_hz")
def mean_rate(rate_hz: list[float]) -> float:
    return sum(rate_hz) / len(rate_hz)


session_means = mean_rate.pod(per_session)
print(session_means.as_pandas_df())
#   subject_id session  mean_rate_hz
# 0   mouse_01      s1      6.033333
# 1   mouse_01      s2      6.793750
# 2   mouse_02      s1      5.845833
# 3   mouse_02      s2      7.506250
# 4   mouse_03      s1      4.906944
# 5   mouse_03      s2      5.059375
# 6   mouse_04      s1      5.136458
# 7   mouse_04      s2      5.129167
```

That is the result this tutorial set out to compute: the mean firing rate of every session.
The knockout animals, `mouse_03` and `mouse_04`, fire less, as the generator intended.

The grouped stream has one data column, `rate_hz`, and that is exactly the parameter of
`mean_rate`, so it goes straight into the pod.

The whole analysis so far, written as one chain:

```python
session_means = mean_rate.pod(
    firing_rate.pod(
        units.select_data_columns(["n_spikes"]),
        sessions.select_data_columns(["duration_s"]),
    )
    .group_by(["subject_id", "session"])
)
print(session_means.as_table().num_rows)
# 8
```

## What you can group by

Only tag columns that hold a single value per row. Grouping by a data column is an error,
and the message says so:

```python
from orcapod.errors import InputValidationError

try:
    rates.group_by(["rate_hz"])
except InputValidationError as err:
    print(err)
# GroupBy: ['rate_hz'] are not tag columns of the input stream. Available tag columns: ['subject_id', 'session', 'probe', 'unit']. (Data columns cannot be grouping keys: ['rate_hz'])
```

This matters when the thing you want to group by is stored as data. `genotype` is a data
column of `subjects`, so after a join it is still data, and you cannot group by it. Parts 1
and 3 said that choosing tags has consequences; this is the main one. Exercise 3 shows a way
through with what you know so far.

## Batch: group by count

`batch` gathers rows without looking at their values. With no argument, the whole stream
becomes a single row:

```python
everyone = subjects.batch()
for tag, data in everyone.iter_data():
    print(tag.as_dict())
    print(data.as_dict())
# {'subject_id': ['mouse_01', 'mouse_02', 'mouse_03', 'mouse_04']}
# {'sex': ['F', 'M', 'F', 'M'], 'age_weeks': [12, 14, 11, 13], 'genotype': ['wildtype', 'wildtype', 'knockout', 'knockout']}
```

Here the tags became lists too, since no column identifies the single row any more. With
`batch_size=N` you get rows of `N` members each; the last one may be smaller, unless you pass
`drop_partial_batch=True`:

```python
for tag, data in subjects.batch(batch_size=3).iter_data():
    print(tag.as_dict())
# {'subject_id': ['mouse_01', 'mouse_02', 'mouse_03']}
# {'subject_id': ['mouse_04']}
```

Use `group_by` when rows belong together because of what they are (all units of a session).
Use `batch` when you want everything, or fixed-size chunks, regardless of content: a grand
mean over all rows, or mini-batches for a model.

If you need both, group first and batch afterwards. A batched stream has list-valued tags,
and `group_by` cannot use those.

## What to remember

- `group_by([...tags])` turns the rows sharing those tag values into one row of lists.
  `batch()` does the same by count.
- Neither computes anything. A function pod with `list[...]` parameters does the computing.
- You can only group by tag columns.
- `group_by` keeps only the tags you group by; the other tags are dropped. `batch` keeps
  every tag, as a list.

## Exercises

**1.** Compute the mean firing rate **per probe** within each session: one row for
`mouse_01, s1, imec0`, one for `mouse_01, s1, imec1`, and so on. How many rows are there?

??? success "Solution"
    Add `probe` to the grouping columns. `mean_rate` works unchanged.

    ```python
    per_probe = mean_rate.pod(
        rates.group_by(["subject_id", "session", "probe"])
    )
    print(per_probe.as_pandas_df().head(4))
    #   subject_id session  probe  mean_rate_hz
    # 0   mouse_01      s1  imec0      7.233333
    # 1   mouse_01      s1  imec1      4.833333
    # 2   mouse_01      s2  imec0      6.564583
    # 3   mouse_01      s2  imec1      7.022917
    print(per_probe.as_table().num_rows)
    # 16
    ```

**2.** Write a pod `summarize` that takes the list of rates of a session and returns three
values: the number of units, the mean rate, and the highest rate. Apply it per session.

??? success "Solution"
    ```python
    @function_pod(output_keys=["n_units", "mean_rate_hz", "max_rate_hz"])
    def summarize(rate_hz: list[float]) -> tuple[int, float, float]:
        return len(rate_hz), sum(rate_hz) / len(rate_hz), max(rate_hz)


    summary = summarize.pod(per_session)
    print(summary.as_pandas_df().head(3))
    #   subject_id session  n_units  mean_rate_hz  max_rate_hz
    # 0   mouse_01      s1        8      6.033333    11.033333
    # 1   mouse_01      s2        8      6.793750    10.175000
    # 2   mouse_02      s1        8      5.845833    11.400000
    ```

**3.** Compute one number for each genotype: the mean of the session means of its animals.
`genotype` is a data column, so you cannot group by it. Instead, for each genotype, keep
that genotype's sessions and `batch` them. (Hint: `session_means.join(...)`, an equality
filter, then narrow, then `batch()`.)

??? success "Solution"
    ```python
    @function_pod(output_keys="genotype_mean_hz")
    def mean_of_means(mean_rate_hz: list[float]) -> float:
        return sum(mean_rate_hz) / len(mean_rate_hz)


    with_genotype = session_means.join(subjects.select_data_columns(["genotype"]))

    for genotype in ["wildtype", "knockout"]:
        one_genotype = (
            with_genotype.polars_filter(genotype=genotype)
            .select_data_columns(["mean_rate_hz"])
            .batch()
        )
        for _, data in mean_of_means.pod(one_genotype).iter_data():
            print(genotype, round(data.as_dict()["genotype_mean_hz"], 3))
    # wildtype 6.545
    # knockout 5.058
    ```

    Writing a loop over the genotypes works, but it is a sign that `genotype` wanted to be a
    tag. Had the subjects table been loaded with `tag_columns=["subject_id", "genotype"]`,
    `genotype` would have travelled through every join as a tag and a single
    `group_by(["genotype"])` would have done it. When you design your own sources, make a
    column a tag if you expect to group or match by it.

Next: [Part 5: Keeping results](05-pipeline-job.md).
