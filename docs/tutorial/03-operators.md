# Part 3: Reshaping with operators

Function pods compute new values. Everything else you do to a table (combine it with another
one, keep some rows, rename or drop columns) is done by **operators**. An operator
rearranges a stream; it never computes a new value from the data. Keeping the two apart is
deliberate: when a number in your result looks wrong, it can only have come from one of your
functions.

You will:

- combine tables with `join`
- keep a subset of rows with `polars_filter` and `semi_join`
- rename columns with `map_data`
- see what to do when two tables have a column of the same name

```python
from workshop_data import make_dataset
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
```

Most operators are available as methods on a stream, and that is the form used here. All of
them exist as classes in `orcapod.operators` (`Join`, `PolarsFilter`, ...); see
[Operators](../concepts/operators.md). `MergeJoin`, at the end of this page, is used as a
class.

## Join: combine tables on their shared tags

`join` matches rows of two streams that have the same values in the tag columns they share,
and puts their data columns side by side:

```python
unit_info = units.join(sessions)

print(unit_info.keys())
# (('subject_id', 'session', 'probe', 'unit'), ('date', 'duration_s', 'region', 'depth_um', 'n_spikes'))
unit_tags = ["subject_id", "session", "probe", "unit"]
narrow = unit_info.select_data_columns(["n_spikes", "duration_s"])
print(narrow.as_pandas_df().sort_values(unit_tags, ignore_index=True).head(3))
#   subject_id session  probe  unit  duration_s  n_spikes
# 0   mouse_01      s1  imec0     0        60.0       149
# 1   mouse_01      s1  imec0     1        60.0       326
# 2   mouse_01      s1  imec0     2        60.0       662
```

You do not say what to join on. `units` has tags `subject_id, session, probe, unit`;
`sessions` has `subject_id, session`; the shared ones are used. The result has the union of
the tags and all data columns of both. This is the step the two-stream function pod in Part 2
did for you.

The column order is chosen by Orcapod, not by the order you wrote the streams in: here the
columns of `sessions` come first. Refer to columns by name, never by position.

Four things to know:

- It does not promise a row order. The samples on this page sort by the tags before
  showing the first rows; do the same when the order matters to you.
- It is an **inner** join: a row with no partner in the other stream is dropped.
- The order of the streams does not matter. `units.join(sessions)` and
  `sessions.join(units)` give the same rows and the same columns.
- If the streams share **no** tag column, every row is paired with every row.

Joins chain. This attaches both the session and the animal to every unit:

```python
everything = units.join(sessions).join(subjects)
print(everything.keys()[1])
# ('sex', 'age_weeks', 'genotype', 'date', 'duration_s', 'region', 'depth_um', 'n_spikes')
print(everything.as_table().num_rows)
# 64
```

## Filter: keep some rows

`polars_filter` keeps the rows that pass a test. The simplest form is equality on one or more
columns, written as keyword arguments:

```python
knockouts = subjects.polars_filter(genotype="knockout")
print(knockouts.as_pandas_df())
#   subject_id sex  age_weeks  genotype
# 0   mouse_03   F         11  knockout
# 1   mouse_04   M         13  knockout
```

A filter may test tag columns and data columns alike. Join first, and you can filter on
columns from either table:

```python
knockout_units = units.join(subjects).polars_filter(genotype="knockout", probe="imec1")
print(knockout_units.as_table().num_rows)
# 16
```

For anything other than equality, pass a [Polars](https://docs.pola.rs/) expression:

```python
import polars as pl

busy = units.polars_filter(pl.col("n_spikes") > 1200)
print(busy.as_pandas_df())
#   subject_id session  probe  unit region  depth_um  n_spikes
# 0   mouse_01      s2  imec0     2     V1       920      1221
# 1   mouse_01      s2  imec1     2    CA1       120      1205
# 2   mouse_02      s2  imec1     3    CA1      2360      1233
```

The expression becomes part of the filtered stream's identity, so an expression filter can
be used anywhere an equality filter can: in a join, before a function pod, or inside a
`PipelineJob` (Part 5).

```python
print(busy.join(subjects).as_table().num_rows)
# 3
```

Orcapod has to be able to hash the expression, and it knows the common building blocks:
comparisons, arithmetic, `&`, `|`, `~`, `is_in`, `is_between`, `is_null` and a few string
tests. An expression that calls your own Python code (`map_elements`, `map_batches`) is
refused as soon as Orcapod needs to hash it (in a join, a pod or a job), with an error that
names the expression and lists what can be hashed; compute such a test in a function pod
instead. The
full list is in [PolarsFilter](../concepts/operators.md#polarsfilter).

One more filter, `drop_nulls`, keeps the rows that have a value in the columns you name. The
workshop data has no missing values, so it is not needed here; see
[DropNulls](../concepts/operators.md#dropnulls) for when it is.

## Semi-join: keep the rows that appear in another table

Sometimes the list of rows to keep is itself a table: a curation sheet, a list of sessions
that passed quality control. `semi_join` keeps the rows of the first stream whose tags appear
in the second, and adds nothing from the second.

Suppose three sessions passed quality control, and the list is in a CSV file:

```python
(data_dir / "passed_qc.csv").write_text(
    "subject_id,session,checked_by\n"
    "mouse_01,s1,ana\n"
    "mouse_03,s2,ana\n"
    "mouse_04,s1,ben\n"
)
passed = CSVSource(
    str(data_dir / "passed_qc.csv"),
    tag_columns=["subject_id", "session"],
    source_id="passed_qc",
)

good_units = units.semi_join(passed)
print(good_units.keys())
# (('subject_id', 'session', 'probe', 'unit'), ('region', 'depth_um', 'n_spikes'))
print(good_units.as_table().num_rows)
# 24
```

Three sessions, eight units each. The columns are those of `units`; `checked_by` was used
for nothing. Unlike `join`, the order matters: the first stream is the one being filtered.

Rows are matched only on the tag columns the two streams share, here `subject_id` and
`session`. Data columns of the second stream are never read. (If the two streams happen to
share a data column name, that column is not used for matching either, and Orcapod warns
about it with a `SemiJoinIgnoredColumnWarning`.)

The second stream can be any stream, for example the filtered `knockouts` from above. It
shares only `subject_id` with `units`:

```python
units_of_knockouts = units.semi_join(knockouts)
print(units_of_knockouts.keys())
# (('subject_id', 'session', 'probe', 'unit'), ('region', 'depth_um', 'n_spikes'))
print(units_of_knockouts.as_table().num_rows)
# 32
```

A `join` followed by an equality filter selects the same 32 rows, but also carries the
columns of `subjects` along:

```python
print(units.join(subjects).polars_filter(genotype="knockout").keys()[1])
# ('sex', 'age_weeks', 'genotype', 'region', 'depth_um', 'n_spikes')
```

## Rename and select

`select_data_columns` and `drop_data_columns` you know from Part 2. There are matching
`select_tag_columns` and `drop_tag_columns`.

`map_data` renames data columns and `map_tags` renames tag columns. Both take a dictionary of
old name to new name:

```python
renamed = subjects.map_data({"age_weeks": "age"})
print(renamed.keys())
# (('subject_id',), ('sex', 'age', 'genotype'))
```

or the same renames as keyword arguments, which build the same step:

```python
print(subjects.map_data(age_weeks="age").keys())
# (('subject_id',), ('sex', 'age', 'genotype'))
```

A name that is not a Python identifier, such as `"spike count"`, needs the dictionary.

Columns you do not rename are kept. A name that is not a column of that kind is refused: a
`map_data` of a tag column raises `InputValidationError` and says to use `map_tags`.
`map_columns` renames tag and data columns in one call. Pass `drop_unmapped=True` to keep only
the renamed ones:

```python
print(subjects.map_data({"age_weeks": "age"}, drop_unmapped=True).keys())
# (('subject_id',), ('age',))
```

## When two tables have the same data column

`join` refuses to combine streams that both have a data column of the same name, because it
cannot keep both under one name. To see this, split the units by probe. Each half has an
`n_spikes` column:

```python
imec0 = (
    units.polars_filter(probe="imec0")
    .drop_tag_columns(["probe"])
    .select_data_columns(["n_spikes"])
)
imec1 = (
    units.polars_filter(probe="imec1")
    .drop_tag_columns(["probe"])
    .select_data_columns(["n_spikes"])
)

from orcapod.errors import InputValidationError

try:
    imec0.join(imec1)
except InputValidationError as err:
    print(err)
# Input streams are not compatible: Datas should not have overlapping keys, but KeysView(Schema({'n_spikes': int})) found in SelectDataColumns and SelectDataColumns.
```

The split above only serves to produce the clash. Unit numbers restart on each probe
(Part 1), so unit 0 of `imec0` and unit 0 of `imec1` are different cells, and the two joins
below pair unrelated units: run them to see how the operators behave, not as an analysis.
The pairing means something when the two columns describe the same thing, for example the
spike counts of one unit from two spike-sorting runs.

There are two ways out, and they mean different things.

**Rename**, when the two columns are different quantities that happen to share a name:

```python
side_by_side = imec0.map_data({"n_spikes": "n_imec0"}).join(
    imec1.map_data({"n_spikes": "n_imec1"})
)
pair_tags = ["subject_id", "session", "unit"]
print(side_by_side.as_pandas_df().sort_values(pair_tags, ignore_index=True).head(3))
#   subject_id session  unit  n_imec0  n_imec1
# 0   mouse_01      s1     0      149      215
# 1   mouse_01      s1     1      326      299
# 2   mouse_01      s1     2      662      135
```

**`MergeJoin`**, when they are repeated measurements of the same quantity and you want them
collected into a list:

```python
from orcapod.operators import MergeJoin

merged = MergeJoin()(imec0, imec1)
print(merged.output_schema()[1])
# Schema({'n_spikes': list[int]})
print(merged.as_pandas_df().sort_values(pair_tags, ignore_index=True).head(3))
#   subject_id session  unit    n_spikes
# 0   mouse_01      s1     0  [149, 215]
# 1   mouse_01      s1     1  [299, 326]
# 2   mouse_01      s1     2  [135, 662]
```

The merged lists are sorted, so you can no longer tell which value came from which probe.
That is the point: `MergeJoin` is for values that are interchangeable.

## What to remember

- Operators rearrange streams; they never compute new values.
- `join` matches on shared tags, keeps only matching rows, and needs distinct data column
  names.
- `polars_filter` keeps rows by equality (`column=value`) or by a Polars expression; both
  work anywhere in a pipeline.
- `semi_join` filters one stream by the shared tags of another and adds no columns.
- `map_data` and `map_tags` rename columns and keep the rest unless you pass
  `drop_unmapped=True`.

## Exercises

**1.** Build a stream with one row per unit recorded on probe `imec1` in a knockout animal,
with exactly two data columns: `n_spikes` and `age_weeks`. How many rows does it have?

??? success "Solution"
    Join units to subjects, filter on one column from each, then narrow.

    ```python
    answer = (
        units.join(subjects)
        .polars_filter(genotype="knockout", probe="imec1")
        .select_data_columns(["n_spikes", "age_weeks"])
    )
    print(answer.as_pandas_df().sort_values(unit_tags, ignore_index=True).head(4))
    #   subject_id session  probe  unit  age_weeks  n_spikes
    # 0   mouse_03      s1  imec1     0         11       399
    # 1   mouse_03      s1  imec1     1         11       483
    # 2   mouse_03      s1  imec1     2         11       357
    # 3   mouse_03      s1  imec1     3         11       457
    print(answer.as_table().num_rows)
    # 16
    ```

**2.** `sessions` has a data column `date`. Rename it to `recorded_on`, keeping `duration_s`.

??? success "Solution"
    ```python
    print(sessions.map_data({"date": "recorded_on"}).keys()[1])
    # ('recorded_on', 'duration_s')
    ```

**3.** Using the `firing_rate` function from Part 2, compute firing rates only for the sessions
listed in `passed_qc.csv`. Use `semi_join` and a single-stream call to the pod.

??? success "Solution"
    Filter the units first, then join the durations. Narrowing to the function's two
    parameters is optional, since the pod would drop the other columns itself, but it shows
    at a glance what the function receives.

    ```python
    from orcapod import function_pod


    @function_pod(output_keys="rate_hz")
    def firing_rate(n_spikes: int, duration_s: float) -> float:
        return n_spikes / duration_s


    inputs = (
        units.semi_join(passed)
        .join(sessions)
        .select_data_columns(["n_spikes", "duration_s"])
    )
    rates = firing_rate.pod(inputs)
    print(rates.as_table().num_rows)
    # 24
    ```

Next: [Part 4: Many rows into one](04-many-rows-into-one.md).
