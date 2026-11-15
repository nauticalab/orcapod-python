# Part 6: Re-runs and caching

In Part 5 a second run computed nothing, because nothing had changed. Real work is a loop:
new data arrives, a value gets corrected, you improve a function. This part shows what
Orcapod recomputes in each case, and one case where it does **not** recompute although you
would expect it to. That case is the most common surprise for new users, so take the time to
run it.

You will:

- add rows and correct a value, and see only the affected rows computed
- edit a function and see the old results come back
- force recomputation by bumping the function's version
- learn the rule that explains all three

```python
from workshop_data import make_dataset
from orcapod import PipelineJob, function_pod
from orcapod.databases import InMemoryArrowDatabase
from orcapod.sources import CSVSource

data_dir = make_dataset()
database = InMemoryArrowDatabase()
calls = []

sessions = CSVSource(
    str(data_dir / "sessions.csv"),
    tag_columns=["subject_id", "session"],
    source_id="sessions",
)


def run(pod, units_file="units.csv"):
    """Build the firing-rate job over ``database``, run it, and return the rates as a DataFrame."""
    units = CSVSource(
        str(data_dir / units_file),
        tag_columns=["subject_id", "session", "probe", "unit"],
        source_id="units",
    )
    job = PipelineJob(name="workshop", database=database)
    with job:
        pod(
            units.select_data_columns(["n_spikes"]),
            sessions.select_data_columns(["duration_s"]),
            label="rates",
        )
    job.run()
    rates = job.nodes["rates"].as_pandas_df()
    return rates.sort_values(["subject_id", "session", "probe", "unit"], ignore_index=True)


@function_pod(output_keys="rate_hz")
def firing_rate(n_spikes: int, duration_s: float) -> float:
    calls.append("firing_rate")
    return n_spikes / duration_s


rates = run(firing_rate.pod)
print(len(rates), len(calls))
# 64 63
```

This part uses an in-memory database so that the page runs quickly. Everything shown behaves
the same with a `DeltaTableDatabase`.

The helper `run` builds the job from Part 5 (without the per-session mean, to keep the
output short), runs it over the one shared `database`, and returns the result sorted. The first
run computed 63 results for 64 units, as before.

## New rows: only the new rows are computed

Two more units were sorted on one probe. Write a new units file with two extra lines:

```python
original = (data_dir / "units.csv").read_text()
(data_dir / "units_more.csv").write_text(
    original
    + "mouse_01,s1,imec0,4,V1,1200,777\n"
    + "mouse_01,s1,imec0,5,V1,1400,888\n"
)

rates = run(firing_rate.pod, "units_more.csv")
print(len(rates), len(calls))
# 66 65
```

66 rows in the result, and two more calls. The other 64 rows were read from the database.

## A corrected value: only that row is computed

A spike count turns out to be wrong: unit 0 of the first probe had 150 spikes, not 149.

```python
(data_dir / "units_fixed.csv").write_text(
    original.replace("mouse_01,s1,imec0,0,V1,1580,149", "mouse_01,s1,imec0,0,V1,1580,150")
)

rates = run(firing_rate.pod, "units_fixed.csv")
print(len(calls))
# 66
print(rates.head(2))
#   subject_id session  probe  unit   rate_hz
# 0   mouse_01      s1  imec0     0  2.500000
# 1   mouse_01      s1  imec0     1  5.433333
```

One call, for the one row whose input changed, and the result shows the new value.

Now change a value the function does **not** use, the depth of that unit. Start from the
corrected file, so that the spike count stays 150 and only the depth differs:

```python
fixed = (data_dir / "units_fixed.csv").read_text()
(data_dir / "units_depth.csv").write_text(
    fixed.replace("mouse_01,s1,imec0,0,V1,1580,150", "mouse_01,s1,imec0,0,V1,9999,150")
)

rates = run(firing_rate.pod, "units_depth.csv")
print(len(calls))
# 66
```

No call. `firing_rate` only receives `n_spikes` and `duration_s`, and those are unchanged.
This is why Part 2's Rule 2 drops the data columns a function does not take before the pod
sees a row: a result depends on exactly the columns the function receives, so a change to any
other column leaves it valid. (The `select_data_columns` calls in `run` only make that
explicit; the pod would drop `depth_um` without them.)

## An edited function: nothing is recomputed

Now the surprise. Suppose you decide rates should be reported to one decimal place, and
edit the function:

```python
@function_pod(output_keys="rate_hz")
def firing_rate(n_spikes: int, duration_s: float) -> float:
    calls.append("firing_rate")
    return round(n_spikes / duration_s, 1)  # changed: rounded to 0.1 Hz


rates = run(firing_rate.pod)
print(len(calls))
# 66
print(rates.head(2))
#   subject_id session  probe  unit   rate_hz
# 0   mouse_01      s1  imec0     0  2.483333
# 1   mouse_01      s1  imec0     1  5.433333
```

No call, and the **old** values. There is no error and no warning.

!!! warning "Editing a function's code does not recompute its results"
    Orcapod does not look at a function's source code. It identifies a function by three
    things: its **name**, its **output** (the `output_keys` and return type), and its
    **major version**. As long as those are the same, stored results are reused, whatever
    the body now says.

## Bump the major version

Every function pod has a version, `"v0.0"` unless you say otherwise. The number before the
dot is the major version. When you change what a function computes, raise it:

```python
@function_pod(output_keys="rate_hz", version="v1.0")
def firing_rate(n_spikes: int, duration_s: float) -> float:
    calls.append("firing_rate")
    return round(n_spikes / duration_s, 1)


rates = run(firing_rate.pod)
print(len(calls))
# 129
print(rates.head(2))
#   subject_id session  probe  unit  rate_hz
# 0   mouse_01      s1  imec0     0      2.5
# 1   mouse_01      s1  imec0     1      5.4
```

63 new calls and new values. The results of version 0 are still in the database, untouched;
version 1 has its own. If you went back to the `v0.0` function you would get the old numbers
without any computation.

A change after the dot is **not** enough. `version="v0.1"` shares its stored results with
`"v0.0"`. Use the minor number for changes that cannot alter results, such as a comment or a
faster implementation of the same calculation.

Renaming the function, or renaming its output column, also starts from an empty slate,
because those are part of its identity too.

## The rule

A stored result is reused when **all** of these are the same as when it was computed:

1. the function's name, output and major version;
2. the values of the function's inputs for that row;
3. the job's `name` and database.

Change any one and the affected rows are computed, unless a result is already stored for
the new combination (going back to an earlier version finds that version's results). Change
none and nothing is. Where the
input values came from does not matter: which file, which source, which steps before the
function.

Orcapod implements this with hashes. A **hash** is a short fingerprint computed from some
content: the same content always gives the same hash, and different content gives, in
practice, a different one. Every stream carries two of them. You do not need them to use
Orcapod, but they explain the rule:

```python
units = CSVSource(
    str(data_dir / "units.csv"),
    tag_columns=["subject_id", "session", "probe", "unit"],
    source_id="units",
)
units_more = CSVSource(
    str(data_dir / "units_more.csv"),
    tag_columns=["subject_id", "session", "probe", "unit"],
    source_id="units",
)

print(units.pipeline_hash() == units_more.pipeline_hash())
# True
```

The **pipeline hash** covers structure only: column names and types, and the steps applied.
The two files have different rows but the same columns, so their pipeline hashes are equal.
The job uses it to name the table in which it records which inputs each function node
received, so runs of one pipeline over different files share that record.

The results themselves are filed under the function (its name, output and major version)
and a hash of the input values of each row. That is why the run over `units_more.csv` found
the results computed from `units.csv`, and how two extra rows meant two extra calls.

The second hash, the **content hash**, also covers which sources the data came from. See
[Identity & Hashing](../concepts/identity.md) for the full picture.

## What to remember

- New or changed input rows are computed; everything else is read from the database.
- **Editing a function body does not recompute.** Bump the major version (`"v0.0"` to
  `"v1.0"`), or rename the function.
- A minor version bump (`"v0.0"` to `"v0.1"`) does not recompute.
- When results look stale, ask: did the name, output or major version change? Did the input
  values change?

## Exercises

**1.** Predict, then check. Starting from the `v1.0` function above and the current database,
which of these four changes cause function calls on the next `run(...)`, and how many?

(a) Change the body to `return 60.0 * n_spikes / duration_s`, same version.<br>
(b) Change the version to `"v1.1"`.<br>
(c) Change the version to `"v2.0"`.<br>
(d) Keep `"v2.0"` and run over `units_more.csv`.

??? success "Solution"
    (a) None: the body is not part of the identity. (b) None: only the major version counts.
    (c) 63: a new major version has no stored results. (d) 2: the two new rows; the other 64
    were stored by (c).

    ```python
    @function_pod(output_keys="rate_hz", version="v1.0")
    def firing_rate(n_spikes: int, duration_s: float) -> float:
        calls.append("firing_rate")
        return 60.0 * n_spikes / duration_s


    before = len(calls)
    run(firing_rate.pod)
    print("a:", len(calls) - before)
    # a: 0


    @function_pod(output_keys="rate_hz", version="v1.1")
    def firing_rate(n_spikes: int, duration_s: float) -> float:
        calls.append("firing_rate")
        return 60.0 * n_spikes / duration_s


    before = len(calls)
    run(firing_rate.pod)
    print("b:", len(calls) - before)
    # b: 0


    @function_pod(output_keys="rate_hz", version="v2.0")
    def firing_rate(n_spikes: int, duration_s: float) -> float:
        calls.append("firing_rate")
        return 60.0 * n_spikes / duration_s


    before = len(calls)
    run(firing_rate.pod)
    print("c:", len(calls) - before)
    # c: 63

    before = len(calls)
    run(firing_rate.pod, "units_more.csv")
    print("d:", len(calls) - before)
    # d: 2
    ```

**2.** You fixed a bug in a function three days ago but forgot to bump the version, and have
run the pipeline since. Which stored results are wrong, and what do you do?

??? success "Solution"
    Rows computed before the fix hold values from the buggy code. Rows that were new after
    the fix hold correct values. Both are filed under the same function identity, so you
    cannot tell them apart from the results alone.

    Bump the major version and run again. Every row is then computed with the current code
    and stored under the new version. The mixed results stay in the database under the old
    version and are no longer used.

    The habit to build: change the behaviour of a function, change its major version, in
    the same edit.

**3.** A colleague runs the same pipeline code over the same database directory, but named
their job `"analysis"` instead of `"workshop"`. Are your stored results reused?

??? success "Solution"
    No. The job's name is the top level of where results are stored (Part 5 showed the
    `workshop_db/workshop/` directory), so a differently named job starts empty.

    ```python
    @function_pod(output_keys="rate_hz")
    def firing_rate(n_spikes: int, duration_s: float) -> float:
        calls.append("firing_rate")
        return n_spikes / duration_s


    other = PipelineJob(name="analysis", database=database)
    with other:
        firing_rate.pod(
            units.select_data_columns(["n_spikes"]),
            sessions.select_data_columns(["duration_s"]),
            label="rates",
        )
    before = len(calls)
    other.run()
    print(len(calls) - before)
    # 63
    ```

    Agree on a job name with the people you share a database with.

Next: [Part 7: Working with files](07-files.md).
