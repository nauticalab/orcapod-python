# Streams

A stream is an immutable sequence of (Tag, Data) pairs backed by a PyArrow Table. Streams
are the universal data currency in Orcapod -- every [source](sources.md) produces a stream,
every [operator](operators.md) consumes and produces streams, and every
[function pod](function-pods.md) transforms data within a stream. Immutability guarantees
that once a stream is created, its data cannot change, which is essential for reproducible
pipelines.

## Tag columns vs Data columns

Every stream divides its columns into two groups:

**Tag columns** are join keys and metadata. They identify *which* record you are looking at
(e.g., `subject_id`, `session_date`). Operators like [Join](operators.md) match rows across
streams using shared tag columns.

**Data columns** are the data payload. They hold the actual values being processed
(e.g., `age`, `weight`, `spike_count`). [Function pods](function-pods.md) read data
columns as function inputs and write new data columns as outputs.

This separation is enforced throughout the framework:

- Operators inspect and restructure tags but never look inside data
- Function pods inspect and transform data but never look at tags

## Key classes

### `ArrowTableStream`

The primary stream implementation. Wraps a PyArrow Table with designated tag and data
columns. Created internally by sources and operators -- you rarely construct one directly.

### `StreamBase`

Abstract base class providing the stream interface. Both `ArrowTableStream` and stream-like
objects (sources, function pod streams, nodes) inherit from it.

A stream must have at least one data column; a table made only of tag columns is rejected
with `ValueError`.

## Core methods

Every stream exposes four key methods. The snippets below use this stream (a source is a
stream):

```python
from orcapod.sources import DictSource

stream = DictSource(
    data=[
        {"subject_id": "mouse_01", "age": 12, "weight": 25.3},
        {"subject_id": "mouse_02", "age": 8, "weight": 22.1},
    ],
    tag_columns=["subject_id"],
)
```

### `output_schema()`

Returns the `(tag_schema, data_schema)` tuple describing column names and their Python types:

```python
tag_schema, data_schema = stream.output_schema()
print(dict(tag_schema))   # {'subject_id': <class 'str'>}
print(dict(data_schema))  # {'age': <class 'int'>, 'weight': <class 'float'>}
```

### `keys()`

Returns column names as `(tag_keys, data_keys)`:

```python
tag_keys, data_keys = stream.keys()
# tag_keys = ('subject_id',)
# data_keys = ('age', 'weight')
```

### `iter_data()`

Iterates over (Tag, Data) pairs. Each Tag and Data is an immutable datagram that you can
inspect with `.as_dict()`:

```python
for tag, data in stream.iter_data():
    print(tag.as_dict(), data.as_dict())
# {'subject_id': 'mouse_01'} {'age': 12, 'weight': 25.3}
# {'subject_id': 'mouse_02'} {'age': 8, 'weight': 22.1}
```

### `as_table()`

Returns the full stream as a PyArrow Table, which integrates with Pandas, Polars, and other
Arrow-compatible tools:

```python
table = stream.as_table()
df = table.to_pandas()
```

Streams also have direct shortcuts: `as_pandas_df()`, `as_polars_df()` and `as_lazy_frame()`.

## Looking at data

In a notebook, a stream, source or job node shown at the end of a cell renders a short
table: its first rows in tag order, with a line saying what they are. That preview is
`head()` asked for `sort_by_tags` and rendered, and `head(n, columns={"sort_by_tags": True})`
gives the same rows to work with. Neither ever computes anything: they show
only what is readily available, never compute, poll or deliver a row (they read only what is held or stored, or make a bounded table read), never call your
code (a function, a predicate, a side effect) and never write to a database. `run()` computes
(see "Computing a stream outside a job" below).

With no rows to show, a head still has the columns and Arrow types the rows would have. To
learn them it applies each operator to empty (zero-row) tables, which processes no data: a
chain of N operators applies up to N operators to empty tables (about 60 ms for 20). A filter
is not applied at all, since its columns are its input's. A user-defined operator's
`static_process` is applied to zero-row tables and must not have effects. Two consequences: a pipeline whose
inputs disagree on a column's Arrow type (say `int8` against `int64`) raises from `head()`
the same `InputValidationError` its run raises (the notebook preview shows it as "could not
read"), and a filter whose predicate does not fit its columns shows a table in `head()` and
raises when it runs.

```python
from orcapod import function_pod
from orcapod.sources import DictSource

subjects = DictSource(
    data=[
        {"subject_id": f"mouse_{i:02d}", "age": 8 + i, "weight": 20.0 + i}
        for i in range(1, 26)
    ],
    tag_columns=["subject_id"],
    label="subjects",
)

head = subjects.head()
print(head.header())
# DictSource[subjects] — first 10 of 25 rows
print(head.status.value, len(head))
# rows 10
print(subjects.head(3).as_table().column("age").to_pylist())
# [9, 10, 11]
```

`head()` returns a `StreamView`: the rows as a PyArrow table (`.table`, `.as_table()`,
`.as_polars_df()`), the row count when it is known without computing (`.total_rows`), and a
`.status` that says where the rows came from. A stream downstream of a computation shows rows
only once they are computed:

```python
@function_pod(output_keys="age_months")
def to_months(age: int) -> int:
    return age * 12


months = to_months.pod(subjects, label="months")
print(months.head().header())
# FunctionPodStream[months] — not computed; run() computes it
print(months.head().status.value, months.head().table.column_names)
# not_computed ['subject_id', 'age_months']

view = months.run()  # computes every row
print(view.header())
# FunctionPodStream[months] — all 25 computed rows
print(months.head(3).header())
# FunctionPodStream[months] — first 3 of 25 computed rows
```

### Computing a stream outside a job: `run()`

Every stream, source and node has `run()`, with no arguments: it is the one way to compute
any of them outside a job (developer, November 13, 2026). It does exactly the work
`as_table()` does, through the same cache, and returns a `StreamView` of every row with the
default columns. A stream already computed is not computed a second time; one whose input
changed is recomputed. In a notebook, `stream.run()` at the end of a cell computes and displays.

The view's own methods choose what is shown. Each returns a new view and never reads or
computes anything; the header says which rows are shown:

```python
print(view.head(5).header())
# FunctionPodStream[months] — first 5 of 25 computed rows
print(view.tail(5).header())
# FunctionPodStream[months] — last 5 of 25 computed rows
print(view.head(20).tail(10).header())
# FunctionPodStream[months] — rows 11–20 of 25 computed rows
print(view.sort_by_tags().head(3).header())
# FunctionPodStream[months] — first 3 of 25 computed rows, sorted by tags
print(view.head(3).sort_by_tags().header())
# FunctionPodStream[months] — first 3 of 25 computed rows, then sorted by tags
```

`view.sort_by_tags()` orders rows as `as_table(columns={"sort_by_tags": True})` does,
except for rows with equal tags: those are ordered by their system tags, which a `run()`
view does not hold, so it keeps them in the order it has them. The display of a view cut by
`head(n)` or `tail(n)` (on the stream or on a view) shows all its rows; a view nobody cut,
such as the one `run()` returns, shows its first and last five rows when it has more than
ten, and its header gives the count.

A view holds the default column groups only. For system tags, source info, the data context
or content hashes, ask the stream after `run()`. A function pod's or operator's output stream,
a job node (a job's side-effect node included), an in-memory source and a `CachedSource` keep
the rows `run()` gave: their `as_table(columns=...)` and `head(columns=...)` read those rows,
compute nothing and deliver no side effect. Other streams do their `as_table()` work again: a
side-effect stream outside a job delivers its side effects again (its `head()` shows no rows),
a polling source polls again and a `SpiralDBTableSource` reads its whole table again. A view never asks its stream for more columns, because for those
streams asking again is not a read.

What `run()` does on each kind of object:

| Object | `run()` |
|---|---|
| An in-memory source (`DictSource`, `ArrowTableSource`, ...), a Delta or SQL table source | nothing to compute: a view of the rows it holds |
| A `SpiralDBTableSource` | reads the whole table into memory (`head()` is the bounded read); every call reads again |
| A `PollingSource` | polls, as `as_table()` does |
| A `CachedSource` | reads its cache and writes the wrapped source's new rows to it |
| A function pod's or operator's output stream | computes what is not computed yet |
| A job node | runs the node as a job run would (`as_table()` of a job node only reads), then returns the view |
| A job's side-effect node | runs the node as `job.run()` does: delivers what its invocation log does not record (every row again without `track_completion`), then returns the rows it passed on; every read after it only reads |
| A side-effect stream outside a job | delivers its side effects for every row, on every call, and returns the rows it passed on; `head()` still shows none |
| A pipeline blueprint node, an unbound input slot | raises, as `as_table()` does |

What each kind of object shows:

| Object | `head()` shows |
|---|---|
| A source | its first rows (a bounded read for a source that reads its table lazily) |
| An operator's or function pod's output stream | the rows already computed, or none with status `not_computed` |
| A job node | the rows it holds after `job.run()`, else the first of its stored rows, else none (`nothing_stored`) |
| A pipeline blueprint node, an unbound input slot | none (`unavailable`) |
| A job's side-effect node | the rows its last run passed on, else none (`not_computed`); it never delivers, and neither do its `as_table()`, `iter_data()` and other reads |
| A side-effect stream outside a job | none: showing it never delivers its side effects |

`head(n, columns=..., all_info=...)` takes the same column configuration as `as_table`,
sorting included, and applies it to the rows it has before taking the first `n`. The default
does not sort, as `as_table()` does not, so `head()` shows rows in the order the stream
holds them. The notebook preview asks for the sorted head, so it shows the same rows
whatever order they were computed in. With `sort_by_tags=True`, or `all_info=True`, rows are
sorted by tag first (nulls last; rows with equal tags are ordered by their system tags):

```python
from orcapod.sources import DictSource

unordered = DictSource(
    data=[{"subject_id": f"mouse_{i:02d}", "age": 8 + i} for i in (3, 1, 2)],
    tag_columns=["subject_id"],
    label="unordered",
)
print(unordered.head(2).as_table().column("subject_id").to_pylist())
# ['mouse_03', 'mouse_01']
print(unordered.head(2, all_info=True).as_table().column("subject_id").to_pylist())
# ['mouse_01', 'mouse_02']
```

A stream that has all of its rows (a job node's stored rows, a computed stream, an in-memory
source) then gives exactly the first `n` rows of `as_table` with the same configuration,
whatever order the rows were computed in. A stream that has only some of its rows sorts
those: a source that reads a bounded number of rows (such as a `SpiralDBTableSource`) shows
the rows it read, in tag order, and a stream computed only in part shows the rows computed so
far in tag order. It is not "the first `n` rows in tag order" of rows it never read, and the
preview's header line says so ("10 rows read, sorted by tags, of about 63,349"). A stream
with no tag columns previews its rows in stream order.

A head of an operator is never the operator applied to the heads of its inputs: a group over
the first ten rows has the wrong members. So an uncomputed stream shows its columns and
"not computed" until `run()`, `as_table()`, `iter_data()` or a job run computes it. An empty result
shows status `rows` with zero rows, so it is never mistaken for "not computed". A head without
rows because nothing is computed or stored yet shows the stream's declared columns: the
source-info (`_source_*`), `_context_key` and content-hash columns, which are known only from
rows, can be missing from it even when `columns` or `all_info` asks for them.

A stream or node records that its results are current when it has read its input to the end.
So after `job.run()` on either orchestrator, sync or async, every node shows the rows of that
run: `head()`, `as_table()` and `iter_data()` serve them from memory, without reading the
store (a function node whose run left no rows in memory reads its stored rows). Likewise, once a chain of streams such as `g.pod(f.pod(source))` has been read to the
end, reading it again computes nothing. A change upstream (`clear_cache()`) makes every stream
after it out of date, and its next read computes it again.
Two reads of one stream open at the same time, or a read that picks up one left partway, share
the stream's results: each row is computed once and every read yields the same rows, whatever
feeds the stream. An upstream finishing its first computation during the read is not a change;
a `clear_cache()` upstream between the reads is, and the next read starts again.

The display marks the tag columns, which come first: in a notebook their headers are bold
(in an accent colour), a vertical rule separates them from the data columns and a line under
the header says "bold = tag"; as text, each tag column's name starts with `*` ("* = tag").
Only the display is marked: the view's table and `as_table()` keep the plain names.

In the preview, large cells are summarised (an array as `ndarray float64 (2, 3)`, bytes as
`bytes (300 B)`, long text and long lists shortened); `head().table` keeps the full values.
In a terminal, `print(stream.head())` prints the same preview as text.

## Controlling column visibility with `ColumnConfig`

By default, streams only expose user-facing tag and data columns. Orcapod also maintains
hidden columns for provenance tracking and metadata. Use `ColumnConfig` (or the `all_info`
shortcut) to control which column groups are included.

| Config field | What it reveals | Column prefix |
|---|---|---|
| `meta` | System metadata columns, when the stream has any | `__` |
| `system_tags` | System tag columns (provenance tracking) | `_tag_source_id::`, `_tag_record_id::` |
| `source` | Source-info columns (per-data provenance tokens) | `_source_` |
| `context` | Data context column | `_context_key` |
| `content_hash` | Content hash column | `_content_hash` |
| `sort_by_tags` | Sort rows by tag columns | (ordering only) |

Pass config as a dict or a `ColumnConfig` object:

```python
from orcapod.sources import DictSource

source = DictSource(
    data=[
        {"subject_id": "mouse_01", "age": 12, "weight": 25.3},
        {"subject_id": "mouse_02", "age": 8, "weight": 22.1},
    ],
    tag_columns=["subject_id"],
)

# Default: user-facing columns only
table = source.as_table()
print(table.column_names)
# ['subject_id', 'age', 'weight']

# Include source-info columns
table = source.as_table(columns={"source": True})
print(table.column_names)
# ['subject_id', 'age', 'weight', '_source_age', '_source_weight']

# Include everything
table = source.as_table(all_info=True)
print(table.column_names)
# ['subject_id', 'age', 'weight', '_tag_source_id::...', '_tag_record_id::...', '_content_hash', '_context_key', '_source_age', '_source_weight']
```

## Code example

Inspect a stream produced by a source:

```python
from orcapod.sources import DictSource

source = DictSource(
    data=[
        {"subject_id": "mouse_01", "age": 12, "weight": 25.3},
        {"subject_id": "mouse_02", "age": 8, "weight": 22.1},
        {"subject_id": "mouse_03", "age": 15, "weight": 27.8},
    ],
    tag_columns=["subject_id"],
)

# Schema inspection
tag_schema, data_schema = source.output_schema()
print("Tag schema:", dict(tag_schema))
# Tag schema: {'subject_id': <class 'str'>}
print("Data schema:", dict(data_schema))
# Data schema: {'age': <class 'int'>, 'weight': <class 'float'>}

# Iterate over (Tag, Data) pairs
for tag, data in source.iter_data():
    print(f"  {tag.as_dict()} -> {data.as_dict()}")
# {'subject_id': 'mouse_01'} -> {'age': 12, 'weight': 25.3}
# {'subject_id': 'mouse_02'} -> {'age': 8, 'weight': 22.1}
# {'subject_id': 'mouse_03'} -> {'age': 15, 'weight': 27.8}

# Convert to a PyArrow table (interops with Pandas)
table = source.as_table()
print(table.to_pandas())
#   subject_id  age  weight
# 0   mouse_01   12    25.3
# 1   mouse_02    8    22.1
# 2   mouse_03   15    27.8
```

## How it connects to other concepts

- [Sources](sources.md) produce streams from external data
- [Operators](operators.md) consume one or more streams and produce a new stream
- [Function Pods](function-pods.md) transform data values within a stream
- Every stream carries [identity hashes](identity.md) for deduplication and DB-scoping
