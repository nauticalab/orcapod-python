# Part 2: Computing with function pods

Loading tables is not analysis. The analysis is the functions you write: convert a unit,
compute a rate, fit a model. In Orcapod you write those as ordinary Python functions and
then let Orcapod apply them to every row of a stream. Because Orcapod is the one calling the
function, it knows exactly which inputs produced which output. Everything in later parts
(stored results, re-runs, tracing) rests on that.

You will:

- turn a function into a **function pod** and apply it to a stream
- learn the two rules a function has to follow
- return more than one value
- compute each unit's firing rate from two tables

```python
from workshop_data import make_dataset
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

## From a function to a function pod

Here is a function that converts a depth from micrometres to millimetres. The only Orcapod
part is the decorator on top:

```python
from orcapod import function_pod


@function_pod(output_keys="depth_mm")
def to_mm(depth_um: int) -> float:
    return depth_um / 1000
```

A **function pod** is a Python function wrapped so that it can be applied to a stream, one
row at a time. `output_keys` names the data column the result goes into.

The function is still a normal function. You can call it and test it as usual:

```python
print(to_mm(depth_um=1580))
# 1.58
```

The pod is the `.pod` attribute. Call it with a stream to get a new stream:

```python
depths = units.select_data_columns(["depth_um"])
result = to_mm.pod(depths)

print(result.output_schema())
# (Schema({'subject_id': str, 'session': str, 'probe': str, 'unit': int}), Schema({'depth_mm': float}))
print(result.as_pandas_df().head(3))
#   subject_id session  probe  unit  depth_mm
# 0   mouse_01      s1  imec0     0      1.58
# 1   mouse_01      s1  imec0     1      2.56
# 2   mouse_01      s1  imec0     2      0.60
```

Look at what happened to the columns:

- The **tags passed through untouched**. A function pod never sees tags and cannot change
  them. Each output row keeps the identity of the input row it was computed from.
- The **data columns were replaced** by the function's output. `depth_um` is gone;
  `depth_mm` is there instead.

`result` is a stream like any other, so everything from Part 1 works on it.

## Rule 1: annotate every parameter and the return value

Orcapod reads the type annotations to know the function's input and output schema. That is
how it could tell you the type of `depth_mm` above without running the function. A missing
annotation is an error at the moment you decorate the function:

```python
try:

    @function_pod(output_keys="depth_mm")
    def to_mm_untyped(depth_um) -> float:
        return depth_um / 1000

except ValueError as err:
    print(err)
# Parameter 'depth_um' has no type annotation and is not specified in input_types.
```

## Rule 2: every parameter needs a data column with its name and type

A function pod takes each data column of the incoming stream as the parameter of the same
name. Every parameter must find its column, and the column's type must fit the parameter's
annotation. A missing column or a wrong type is an error as soon as the pod is applied;
Exercises 1 and 3 below show both.

Data columns the function does *not* take are dropped. The sample above narrowed `units` to
`depth_um` with `select_data_columns` first, but the pod does that step for you:

```python
whole = to_mm.pod(units)
print(whole.output_schema())
# (Schema({'subject_id': str, 'session': str, 'probe': str, 'unit': int}), Schema({'depth_mm': float}))
print(whole.as_pandas_df().equals(result.as_pandas_df()))
# True
```

`region` and `n_spikes` were dropped before the function saw a row. The drop is not hidden:
Orcapod adds the same `select_data_columns` step you would have written by hand, so the two
calls build the same pipeline.

You can still narrow a stream yourself, and it often makes a pipeline easier to read: it
shows which columns a step uses. `select_data_columns` keeps the columns you name; its
counterpart `drop_data_columns` removes the columns you name and keeps the rest:

```python
print(units.select_data_columns(["depth_um"]).keys())
# (('subject_id', 'session', 'probe', 'unit'), ('depth_um',))
print(units.drop_data_columns(["region", "n_spikes"]).keys())
# (('subject_id', 'session', 'probe', 'unit'), ('depth_um',))
```

Neither touches the tags. The rule is about data columns only.

!!! note "Why drop the other columns?"
    A pod's result can only depend on the columns its function takes. The unused columns are
    dropped before the pod receives a row, so they are not part of the inputs Orcapod
    remembers a result by: a change in an unrelated column upstream does not make Orcapod
    compute the result again (Part 6 shows this). If you would rather have an extra column
    rejected than dropped, pass `strict=True`: `to_mm.pod(units, strict=True)` raises
    `ValueError`. See [Input columns](../concepts/function-pods.md#input-columns).

## Returning more than one value

Give `output_keys` a list, return a tuple, and annotate the return type as a tuple:

```python
@function_pod(output_keys=["depth_mm", "layer"])
def describe_depth(depth_um: int) -> tuple[float, str]:
    layer = "superficial" if depth_um < 1000 else "deep"
    return depth_um / 1000, layer


print(describe_depth.pod(depths).as_pandas_df().head(3))
#   subject_id session  probe  unit  depth_mm        layer
# 0   mouse_01      s1  imec0     0      1.58         deep
# 1   mouse_01      s1  imec0     1      2.56         deep
# 2   mouse_01      s1  imec0     2      0.60  superficial
```

## A function of two tables

A firing rate is a spike count divided by a duration. The count is in `units`; the duration
is in `sessions`. Pass both streams to the pod:

```python
@function_pod(output_keys="rate_hz")
def firing_rate(n_spikes: int, duration_s: float) -> float:
    return n_spikes / duration_s


rates = firing_rate.pod(
    units.select_data_columns(["n_spikes"]),
    sessions.select_data_columns(["duration_s"]),
)
unit_tags = ["subject_id", "session", "probe", "unit"]
print(rates.as_pandas_df().sort_values(unit_tags, ignore_index=True).head(5))
#   subject_id session  probe  unit    rate_hz
# 0   mouse_01      s1  imec0     0   2.483333
# 1   mouse_01      s1  imec0     1   5.433333
# 2   mouse_01      s1  imec0     2  11.033333
# 3   mouse_01      s1  imec0     3   9.983333
# 4   mouse_01      s1  imec1     0   3.583333
print(rates.as_table().num_rows)
# 64
```

Given several streams, a pod first combines them by matching rows on the tag columns they
share, here `subject_id` and `session`. Each unit row is paired with the row of its session,
and the function receives `n_spikes` from one and `duration_s` from the other. This matching
is a **join**; Part 3 covers it properly. Rule 2 applies to the combined stream. Narrowing
each input first is not required, since the pod would drop the combined stream's other
columns itself, but it shows at a glance which table each parameter comes from.

A join does not promise an order for its rows, so the sample sorts them by their tags
before showing the first five. Sort whenever you look at, or compare, rows that went through
a join.

Notice that this is why tags matter: you never wrote the code that looks up a unit's
session. Choosing the tags in Part 1 was enough.

## What to remember

- `@function_pod(output_keys=...)` turns a function into a pod; `my_function.pod(stream)`
  applies it to every row.
- Tags pass through unchanged. Data columns are replaced by the outputs.
- Rule 1: annotate every parameter and the return value.
- Rule 2: every parameter needs a data column with its name and a matching type. Data
  columns the function does not take are dropped.
- Nothing is stored yet. Every result on this page was computed in memory and is gone when
  Python exits. Part 5 changes that.

## Exercises

**1.** A colleague wanted every unit's firing rate and wrote this call, which fails. Run it,
read the message, and fix the call (not the function).

```python
try:
    firing_rate.pod(units)
except ValueError as err:
    print(err)
# Incoming data schema Schema({'n_spikes': int}) is not compatible with expected input schema Schema({'n_spikes': int, 'duration_s': float}): required parameter 'duration_s' has no matching column
```

??? success "Solution"
    Read the message as "the stream has these data columns; the function expects those", and
    the part after the colon names the first problem. `units` has no `duration_s` column:
    the duration is in `sessions`. (The message shows `units` after its unused columns,
    `region` and `depth_um`, were dropped.) Pass both streams:

    ```python
    rates_again = firing_rate.pod(units, sessions)
    rates_again_df = rates_again.as_pandas_df().sort_values(unit_tags, ignore_index=True)
    print(rates_again_df.head(3))
    #   subject_id session  probe  unit    rate_hz
    # 0   mouse_01      s1  imec0     0   2.483333
    # 1   mouse_01      s1  imec0     1   5.433333
    # 2   mouse_01      s1  imec0     2  11.033333
    rates_df = rates.as_pandas_df().sort_values(unit_tags, ignore_index=True)
    print(rates_again_df.equals(rates_df))
    # True
    ```

    The pod joins the two streams on `subject_id` and `session` and drops every data column
    except the two its function takes, so the rates are the ones computed above.

**2.** Write a pod `rate_and_activity` that returns two values for each unit: its firing rate
in Hz, and `True` if that rate is above 5 Hz. Apply it to the same two streams as
`firing_rate` and print the output schema.

??? success "Solution"
    ```python
    @function_pod(output_keys=["rate_hz", "is_active"])
    def rate_and_activity(n_spikes: int, duration_s: float) -> tuple[float, bool]:
        rate = n_spikes / duration_s
        return rate, rate > 5


    result = rate_and_activity.pod(
        units.select_data_columns(["n_spikes"]),
        sessions.select_data_columns(["duration_s"]),
    )
    print(result.output_schema()[1])
    # Schema({'rate_hz': float, 'is_active': bool})
    ```

**3.** This pod converts session durations to minutes:

```python
@function_pod(output_keys="duration_min")
def to_minutes(duration_s: float) -> float:
    return duration_s / 60


print(to_minutes.pod(sessions).as_pandas_df().head(3))
#   subject_id session  duration_min
# 0   mouse_01      s1           1.0
# 1   mouse_01      s2           2.0
# 2   mouse_02      s1           1.5
```

Change the annotation from `duration_s: float` to `duration_s: int` (give the function a new
name, `to_minutes_int`) and apply it to `sessions`. What happens, and when?

??? success "Solution"
    The column in the stream is a `float`, the parameter says `int`, and the pod refuses the
    stream. The error comes when the pod is applied, before any row is computed.

    ```python
    @function_pod(output_keys="duration_min")
    def to_minutes_int(duration_s: int) -> float:
        return duration_s / 60


    try:
        to_minutes_int.pod(sessions)
    except ValueError as err:
        print(err)
    # Incoming data schema Schema({'duration_s': float}) is not compatible with expected input schema Schema({'duration_s': int}): column 'duration_s' has type float, the function expects int
    ```

    Types are part of the match. Orcapod does not convert `60.0` to `60` for you.

Next: [Part 3: Reshaping with operators](03-operators.md).
