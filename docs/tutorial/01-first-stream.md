# Part 1: Your first stream

An analysis usually starts with tables: a list of animals, a list of sessions, a table of
measurements. Before Orcapod can compute anything, it needs to know two things about each
table: which columns say *what a row is about*, and which columns hold *the values*. This part
shows how to load a table and tell Orcapod that.

You will:

- load a CSV file and a list of Python dictionaries as **sources**
- choose **tag columns** and see what is left as **data columns**
- look at a **stream** four ways: its schema, its column names, row by row, and as a DataFrame

```python
from workshop_data import make_dataset

data_dir = make_dataset()
```

## A source is a table with named keys

A **source** is where data enters Orcapod. `CSVSource` reads a CSV file:

```python
from orcapod.sources import CSVSource

subjects = CSVSource(
    str(data_dir / "subjects.csv"),
    tag_columns=["subject_id"],
    source_id="subjects",
)
print(subjects.as_pandas_df())
#   subject_id sex  age_weeks  genotype
# 0   mouse_01   F         12  wildtype
# 1   mouse_02   M         14  wildtype
# 2   mouse_03   F         11  knockout
# 3   mouse_04   M         13  knockout
```

Three arguments:

- The file path, **as a string**. `CSVSource` takes a string, not a `pathlib.Path`, hence
  the `str(...)`.
- `tag_columns`: the columns that identify a row. More on this below.
- `source_id`: a name for this source. Orcapod uses it when it records where a value came
  from. Give every source a short, stable name.

## Tags and data

Every column of a source is either a tag column or a data column.

- **Tag columns** say what a row is about. They are the keys: the mouse, the session, the
  probe. If you have used a relational database, they play the part of a primary key. Orcapod
  uses tags to match rows when it combines tables.
- **Data columns** are everything else: the values that were measured or recorded.

You choose the tag columns; all remaining columns become data columns. `output_schema()`
returns both groups, with their types:

```python
tag_schema, data_schema = subjects.output_schema()
print(tag_schema)
# Schema({'subject_id': str})
print(data_schema)
# Schema({'sex': str, 'age_weeks': int, 'genotype': str})
```

A **schema** is the list of column names and their types. Orcapod worked the types out from
the file: `age_weeks` is an `int` because every value in that column is a whole number.

`keys()` gives only the names:

```python
tag_keys, data_keys = subjects.keys()
print(tag_keys)
# ('subject_id',)
print(data_keys)
# ('sex', 'age_weeks', 'genotype')
```

A table can need more than one tag column. A session is identified by the mouse *and* the
session name:

```python
sessions = CSVSource(
    str(data_dir / "sessions.csv"),
    tag_columns=["subject_id", "session"],
    source_id="sessions",
)
print(sessions.output_schema())
# (Schema({'subject_id': str, 'session': str}), Schema({'date': date, 'duration_s': float}))
```

## A source is a stream

What a source gives you is a **stream**: an ordered, read-only sequence of rows, each one a
pair of (tag, data). Every step you add later takes streams in and gives a stream back, so
the four ways of looking at a stream shown in this part work at every stage of a pipeline.

You have seen `output_schema()` and `keys()`. To walk through the rows, use `iter_data()`.
Each row comes as two objects; call `.as_dict()` on either to get a plain dictionary:

```python
for tag, data in sessions.iter_data():
    print(tag.as_dict(), "->", data.as_dict())
# {'subject_id': 'mouse_01', 'session': 's1'} -> {'date': datetime.date(2026, 10, 5), 'duration_s': 60.0}
# {'subject_id': 'mouse_01', 'session': 's2'} -> {'date': datetime.date(2026, 10, 12), 'duration_s': 120.0}
# {'subject_id': 'mouse_02', 'session': 's1'} -> {'date': datetime.date(2026, 10, 5), 'duration_s': 90.0}
# {'subject_id': 'mouse_02', 'session': 's2'} -> {'date': datetime.date(2026, 10, 12), 'duration_s': 120.0}
# {'subject_id': 'mouse_03', 'session': 's1'} -> {'date': datetime.date(2026, 10, 5), 'duration_s': 90.0}
# {'subject_id': 'mouse_03', 'session': 's2'} -> {'date': datetime.date(2026, 10, 12), 'duration_s': 120.0}
# {'subject_id': 'mouse_04', 'session': 's1'} -> {'date': datetime.date(2026, 10, 5), 'duration_s': 120.0}
# {'subject_id': 'mouse_04', 'session': 's2'} -> {'date': datetime.date(2026, 10, 12), 'duration_s': 60.0}
```

To see the whole stream at once, turn it into a DataFrame. `as_pandas_df()` gives a pandas
DataFrame, `as_polars_df()` a Polars one, and `as_table()` a PyArrow table:

```python
print(sessions.as_pandas_df().head(3))
#   subject_id session       date  duration_s
# 0   mouse_01      s1 2026-10-05        60.0
# 1   mouse_01      s2 2026-10-12       120.0
# 2   mouse_02      s1 2026-10-05        90.0
print(sessions.as_table().num_rows)
# 8
```

A stream cannot be changed. There is no method to edit a row or add a column in place. To
get different data you build a new stream from this one, which is what Parts 2 to 4 are
about.

## Sources from Python data

Not everything starts as a file. `DictSource` takes a list of dictionaries, one per row:

```python
from orcapod.sources import DictSource

probes = DictSource(
    data=[
        {"probe": "imec0", "region": "V1", "n_channels": 384},
        {"probe": "imec1", "region": "CA1", "n_channels": 384},
    ],
    tag_columns=["probe"],
    source_id="probes",
)
print(probes.as_pandas_df())
#    probe region  n_channels
# 0  imec0     V1         384
# 1  imec1    CA1         384
```

It behaves exactly like the CSV sources above. Orcapod has more source types (pandas and
Polars DataFrames, Delta tables, SQL tables); they differ only in where the rows come from.
See [Sources](../concepts/sources.md).

## What to remember

- A **source** brings a table into Orcapod. A source is a **stream** of (tag, data) rows.
- **Tag columns** identify a row; **data columns** hold the values. You choose the tags.
- Look at any stream with `output_schema()`, `keys()`, `iter_data()` and `as_pandas_df()`.
- Streams are read-only.

## Exercises

**1.** `workshop_data/units.csv` has one row per sorted unit, with columns `subject_id`,
`session`, `probe`, `unit`, `region`, `depth_um` and `n_spikes`. Load it as a source called
`units`. Which columns must be tags so that each row is identified? How many rows does it
have?

??? success "Solution"
    Unit numbers restart at 0 on every probe, and probe names repeat in every session, so
    all four of `subject_id`, `session`, `probe` and `unit` are needed. `region`, `depth_um`
    and `n_spikes` describe the unit; they are data.

    ```python
    units = CSVSource(
        str(data_dir / "units.csv"),
        tag_columns=["subject_id", "session", "probe", "unit"],
        source_id="units",
    )
    print(units.output_schema())
    # (Schema({'subject_id': str, 'session': str, 'probe': str, 'unit': int}), Schema({'region': str, 'depth_um': int, 'n_spikes': int}))
    print(units.as_table().num_rows)
    # 64
    ```

    You could argue that `region` belongs with the tags, because it says what the row is
    about. That is a real choice, and it has consequences: tags are what rows are matched on
    in Part 3. Here `region` is fully determined by `probe`, so it adds nothing as a key.

**2.** Two mistakes are common when a source is created. Make each one on purpose and read
the message: (a) name a tag column that does not exist in `subjects.csv`; (b) make every
column of `subjects.csv` a tag.

??? success "Solution"
    Both raise `ValueError` as soon as the source is created.

    ```python
    try:
        CSVSource(str(data_dir / "subjects.csv"), tag_columns=["mouse"])
    except ValueError as err:
        print(err)
    # tag_columns not found in table: {'mouse'}. Available columns: ['subject_id', 'sex', 'age_weeks', 'genotype']

    try:
        CSVSource(
            str(data_dir / "subjects.csv"),
            tag_columns=["subject_id", "sex", "age_weeks", "genotype"],
        )
    except ValueError as err:
        print(err)
    # No data columns found in the table. At least one data column is required.
    ```

    The first message lists the columns that do exist, which is usually enough to spot a
    typo. The second states a rule: a stream needs at least one data column.

**3.** Print the first three rows of `units` using `iter_data()`.

??? success "Solution"
    `iter_data()` is an ordinary Python iterator, so `itertools.islice` works on it.

    ```python
    from itertools import islice

    for tag, data in islice(units.iter_data(), 3):
        print(tag.as_dict(), "->", data.as_dict())
    # {'subject_id': 'mouse_01', 'session': 's1', 'probe': 'imec0', 'unit': 0} -> {'region': 'V1', 'depth_um': 1580, 'n_spikes': 149}
    # {'subject_id': 'mouse_01', 'session': 's1', 'probe': 'imec0', 'unit': 1} -> {'region': 'V1', 'depth_um': 2560, 'n_spikes': 326}
    # {'subject_id': 'mouse_01', 'session': 's1', 'probe': 'imec0', 'unit': 2} -> {'region': 'V1', 'depth_um': 600, 'n_spikes': 662}
    ```

Next: [Part 2: Computing with function pods](02-function-pods.md).
