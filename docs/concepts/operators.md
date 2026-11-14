# Operators

Operators are structural transforms that reshape [streams](streams.md) without inspecting or
synthesizing data values. They join, filter, batch, group, rename, and select columns --
operations that affect the *structure* of the data (which rows exist, which columns are
present, how columns are named) but never compute new values from data content. This is the
key distinction from [function pods](function-pods.md), which do the opposite: they transform
data values but never touch tags or stream structure.

## The operator / function pod boundary

This separation is a core Orcapod design principle:

|  | Operator | Function Pod |
|---|---|---|
| Inspects data content | Never, except filters (see below) | Yes |
| Inspects / uses tags | Yes | No |
| Can rename columns | Yes | No |
| Synthesizes new values | No | Yes |
| Stream arity | Configurable (1, 2, or N inputs) | Single in, single out |

This boundary ensures that structural operations (joins, filters) and value computations
(transformations, model inference) are cleanly separated, making pipelines easier to reason
about and optimize.

`GroupBy` is the one operator that reduces row count many-to-one by key: it collapses N rows
sharing a key into a single row with list-valued data members, and keeps only its key tags. It
stays on the operator side of the boundary because it synthesizes no new data values -- every
element of every emitted list came from an input row -- and it keys only on tags, never on data
content.

One rule holds for every operator: **no data element comes from a tag value**. An operator may
use tags to join, filter, group or order rows, but it never turns a tag column into a data
column, so a function pod never receives tag content.

The filters are the operators that read data columns: `PolarsFilter` evaluates its predicate
on values, and `DropNulls` reads only whether a value is present (its validity), never the
value. Both decide which rows exist; neither synthesizes a value.

No operator carries its inputs' meta columns: the `__`-prefixed columns, such as a cached
function pod's `__computed`, that describe how an input row was produced. An operator's
output row is produced by the operator, so its `Data` holds none of them and its predicted
output schema lists none, on the sync and the async path alike. A way to carry them forward
across operators is an open design question (ITL-867). The `__` prefix is reserved for meta
columns: a stream and a source refuse a tag column named with it, and `MapTags` and `MapData`
refuse it as the new name of a tag or data column.

## Operator categories

### `UnaryOperator` -- single input

Takes one stream, produces one stream. Used for filtering, column selection, renaming,
batching, and grouping.

### `BinaryOperator` -- two inputs

Takes exactly two streams. Used for `MergeJoin` and `SemiJoin`.

### `NonZeroInputOperator` -- one or more inputs

Takes one or more streams. Used for `Join`, which performs an N-ary inner join.

## Available operators

### Join

N-ary inner join on shared tag columns. Requires that input streams have non-overlapping
data columns (raises `InputValidationError` on collision). Join is **commutative** -- the
order of input streams does not affect the result. Like every operator, Join drops its
inputs' meta (`__`-prefixed) columns, so they never collide. Rows match on equality of every shared tag column, whatever
its type (lists and structs compare element by element). A row with a null in any shared tag
column matches nothing, a null list or struct included, and rows with null on both sides do
not match each other (the SQL convention for nulls; `MergeJoin` and `SemiJoin` follow it too, on the sync and the async path, while `GroupBy` is the exception by design and puts null keys in one group); a null *inside* a list or struct is an ordinary element that equals
another null, an empty list is not a null, and NaN equals NaN. The async path gives the same
rows.

Because a null key matches nothing, a shared tag column may differ between the inputs in
nullability alone: `T` in one input and `T | None` in another is accepted by `Join`, `MergeJoin`
and `SemiJoin`. In the output of `Join` and `MergeJoin` the column is `T` when any input has it
`T`, and `T | None` only when every input has it nullable; `SemiJoin`'s output keeps the left
input's type. Any other type difference is still an error, and a data column is never
reconciled this way.

A name is a tag or a data column, never both: if one input has `x` as a tag column and another has it as a data column, `Join` (and `MergeJoin`) raises `InputValidationError` when the graph is built; rename one with `map_tags` or `map_data`.

Column order follows each operator's argument symmetry: commutative operators (`Join`,
`MergeJoin`) list tag and data columns by first appearance across their inputs in canonical
pipeline-hash order, so `Join()(a, b)` and `Join()(b, a)` have the same columns in the same
order, while ordered operators such as `SemiJoin` keep operand order; either way, address
columns by name rather than by position.

```python
from orcapod.sources import DictSource
from orcapod.operators import Join

subjects = DictSource(
    data=[
        {"subject_id": "mouse_01", "age": 12},
        {"subject_id": "mouse_02", "age": 8},
    ],
    tag_columns=["subject_id"],
)

measurements = DictSource(
    data=[
        {"subject_id": "mouse_01", "weight": 25.3},
        {"subject_id": "mouse_02", "weight": 22.1},
    ],
    tag_columns=["subject_id"],
)

join = Join()
joined = join.process(subjects, measurements)
print(joined.as_table().to_pandas())
#   subject_id  age  weight
# 0   mouse_01   12    25.3
# 1   mouse_02    8    22.1
```

### MergeJoin

Binary join that handles colliding data columns by merging their values into sorted
`list[T]`. Both inputs must have the same type for any colliding data columns. MergeJoin
is **commutative** -- the order of the two input streams does not affect the result.

```python
from orcapod.sources import DictSource
from orcapod.operators import MergeJoin

scale_a = DictSource(
    data=[
        {"subject_id": "mouse_01", "weight": 25.3},
        {"subject_id": "mouse_02", "weight": 22.1},
    ],
    tag_columns=["subject_id"],
)
scale_b = DictSource(
    data=[
        {"subject_id": "mouse_01", "weight": 24.9},
        {"subject_id": "mouse_02", "weight": 22.6},
    ],
    tag_columns=["subject_id"],
)

merged = MergeJoin()(scale_a, scale_b)
for tag, data in merged.iter_data():
    print(tag.as_dict(), "->", data.as_dict())
# {'subject_id': 'mouse_01'} -> {'weight': [24.9, 25.3]}
# {'subject_id': 'mouse_02'} -> {'weight': [22.1, 22.6]}
```

Passing the same two streams to `Join` instead raises `InputValidationError`, because both
carry a `weight` data column.

### SemiJoin

Binary join that filters the left stream to only include rows whose tags match the right
stream. The right stream's data columns are discarded. SemiJoin is **not commutative** --
the order of inputs matters. The first stream is the one being filtered; the second stream
provides the set of matching tags.

The match keys are exactly the columns that are tag columns of both streams. A name the two
streams share as a data column, or a tag in one and a data column in the other, is not a key
and is ignored: the right stream's data columns are never read. If the streams share no tag
column, every left row is returned. Because such names were match keys before v0.3, SemiJoin
names them in a `SemiJoinIgnoredColumnWarning` (from `orcapod.errors`). This behaviour is
provisional in v0.3 and may become an error. To silence the warning, rename or drop the
column on the right stream before the semi join (`map_data`, `select_data_columns`,
`drop_data_columns`, or `map_tags` for a tag). A right stream must keep at least one data
column, so a filtered copy of the left stream (which shares every data name) can be reduced
to one renamed column, e.g. `subset.select_data_columns(["age"]).map_data({"age": "_age"})`.

```python
from orcapod.sources import DictSource
from orcapod.operators import SemiJoin

all_subjects = DictSource(
    data=[
        {"subject_id": "mouse_01", "age": 12},
        {"subject_id": "mouse_02", "age": 8},
        {"subject_id": "mouse_03", "age": 15},
    ],
    tag_columns=["subject_id"],
)
weighed = DictSource(
    data=[
        {"subject_id": "mouse_01", "weight": 25.3},
        {"subject_id": "mouse_02", "weight": 22.1},
    ],
    tag_columns=["subject_id"],
)

only_weighed = SemiJoin()(all_subjects, weighed)
for tag, data in only_weighed.iter_data():
    print(tag.as_dict(), "->", data.as_dict())
# {'subject_id': 'mouse_01'} -> {'age': 12}
# {'subject_id': 'mouse_02'} -> {'age': 8}
```

### Batch

Groups all rows into a single row (or fixed-size batches), converting column types from `T`
to `list[T]`. Useful for aggregation-style processing.

```python
from orcapod.sources import DictSource
from orcapod.operators import Batch

source = DictSource(
    data=[
        {"subject_id": "mouse_01", "age": 12},
        {"subject_id": "mouse_02", "age": 8},
    ],
    tag_columns=["subject_id"],
)

batch = Batch()
batched = batch.process(source)
for tag, data in batched.iter_data():
    print("Tags:", tag.as_dict())
    # Tags: {'subject_id': ['mouse_01', 'mouse_02']}
    print("Data:", data.as_dict())
    # Data: {'age': [12, 8]}
```

Pass `batch_size=N` to create fixed-size batches instead of grouping everything. The last
batch may be smaller; pass `drop_partial_batch=True` to discard it:

```python
batch = Batch(batch_size=10, drop_partial_batch=True)
```

`Batch` keeps every tag, list-valued. It batches blindly, by count: it has no key, so the lists
of tag values are the only thing that tells a batch's members apart. `GroupBy` (below) has a
key and keeps only that.

### GroupBy

Collapses every set of rows that share a tag tuple into a single row -- the one many-to-one
operator. Where `Batch` partitions by row count, `GroupBy` partitions by tag *value*, which is
what lets a downstream function pod receive a complete logical unit (all of a recording
session's probes, say) in one call.

```python
from orcapod.sources import DictSource
from orcapod.operators import GroupBy

source = DictSource(
    data=[
        {"subject_id": "mouse_01", "probe": "imec0", "spike_count": 1200},
        {"subject_id": "mouse_01", "probe": "imec1", "spike_count": 980},
        {"subject_id": "mouse_02", "probe": "imec0", "spike_count": 1450},
    ],
    tag_columns=["subject_id", "probe"],
)

grouped = GroupBy(by=["subject_id"])(source)
print(grouped.keys())
# (('subject_id',), ('spike_count',))
for tag, data in grouped.iter_data():
    print("Tags:", tag.as_dict(), "Data:", data.as_dict())
    # Tags: {'subject_id': 'mouse_01'} Data: {'spike_count': [1200, 980]}
    # Tags: {'subject_id': 'mouse_02'} Data: {'spike_count': [1450]}
```

The output schema follows three rules:

- **Group keys stay scalar tag columns** (`subject_id: str`), in the input's tag order. They are
  the only tag columns of the output.
- **A tag that is not a key is dropped** (`probe`). A list of the members' values is not a key
  for the group, just as SQL drops a non-key, non-aggregated column after `GROUP BY`.
- **Data columns become `list[T]`** (`spike_count: list[int]`), one element per member.

!!! warning "Member tag values are not carried"
    `GroupBy` does not keep the members' values of a non-key tag, as a tag or as data. If a
    later step needs to know which member each element came from, make the column data
    *before* grouping: declare it as data at the source, or derive a source that offers the tag
    as data with `DerivedSource(origin, tags_as_data={...})`.

```python
from orcapod.sources import DerivedSource

with_probe = DerivedSource(source, tags_as_data={"probe": "probe_name"})
by_subject = GroupBy(by=["subject_id"])(with_probe)
print(by_subject.keys())
# (('subject_id',), ('spike_count', 'probe_name'))
for tag, data in by_subject.iter_data():
    print(tag.as_dict(), data.as_dict())
    # {'subject_id': 'mouse_01'} {'spike_count': [1200, 980], 'probe_name': ['imec0', 'imec1']}
    # {'subject_id': 'mouse_02'} {'spike_count': [1450], 'probe_name': ['imec0']}
```

The data columns of the output are exactly the data columns of the input: a function pod that
follows `GroupBy` receives `spike_count` (and `probe_name`, when the source offers it), never a
tag.

Ordering is deterministic by construction, because orcapod hashes the emitted lists to build
cache keys: members are sorted by the input's non-key tag values (with the internal record id
as a final tiebreaker), even though those tags are dropped, and groups are emitted in group-key
order. A reordered input therefore produces a byte-identical output table.

A grouped stream can be grouped again on a coarser key; its data lists then nest one level
deeper (`list[list[T]]`). The members of the second grouping are ordered by the first
grouping's keys that are not in the coarser key, then by record id; a tag the first grouping
dropped no longer takes part.

Every name in `by` must be a **scalar tag column** of the input. `GroupBy` raises
`InputValidationError` for an unknown column, a data column, or a list-valued tag column --
the last usually means the stream came from `Batch`, so group *before* batching rather than
after.

Streams also expose this as a fluent method:

```python
grouped = source.group_by(["subject_id"])
```

### Column selection

Four operators for including or excluding columns:

- **`SelectTagColumns(columns=["col1", "col2"])`** -- keep only the specified tag columns
- **`SelectDataColumns(columns=["col1", "col2"])`** -- keep only the specified data columns
- **`DropTagColumns(columns=["col1"])`** -- remove the specified tag columns
- **`DropDataColumns(columns=["col1"])`** -- remove the specified data columns

```python
from orcapod.sources import DictSource
from orcapod.operators import SelectDataColumns

source = DictSource(
    data=[
        {"subject_id": "mouse_01", "age": 12, "weight": 25.3},
        {"subject_id": "mouse_02", "age": 8, "weight": 22.1},
        {"subject_id": "mouse_03", "age": 15, "weight": 27.8},
    ],
    tag_columns=["subject_id"],
)

select = SelectDataColumns(columns=["weight"])
result = select.process(source)
print(result.keys()[1])  # ('weight',)
```

Selecting a column that does not exist raises `InputValidationError`; pass `strict=False` to
ignore missing columns instead.

Two more act on named columns whether each is a tag or a data column:

- **`DropColumns(columns=["col1", "col2"])`** -- remove the named columns, tag or data
- **`SelectColumns(columns=["col1", "col2"])`** -- keep exactly the named columns, tag or
  data, and remove every other tag and data column

Streams offer them as `drop_columns` and `select_columns`, which take the names as one or
more strings or as one collection: `stream.drop_columns("a", "b")` is
`stream.drop_columns(["a", "b"])`. A removed data column's source-info column goes with it,
and system tags are kept, as with the four operators above.

```python
sessions = DictSource(
    data=[
        {"subject_id": "mouse_01", "session": 1, "age": 12, "weight": 25.3},
        {"subject_id": "mouse_02", "session": 2, "age": 8, "weight": 22.1},
    ],
    tag_columns=["subject_id", "session"],
)

dropped = sessions.drop_columns("session", "age")
print(dropped.keys())  # (('subject_id',), ('weight',))
```

```python
selected = sessions.select_columns(["session", "weight"])
print(selected.keys())  # (('session',), ('weight',))
```

A stream needs at least one data column, so `drop_columns` of every data column, or
`select_columns` naming no data column, raises `InputValidationError` when it is called.
Selecting no tag column is allowed.

`SelectDataColumns` is also how you narrow a stream before a
[function pod](function-pods.md), which accepts only the data columns its function declares.

### Column renaming

- **`MapTags(name_map={"old_name": "new_name"})`** -- rename tag columns
- **`MapData(name_map={"old_name": "new_name"})`** -- rename data columns

Columns that are not in `name_map` are kept unless you pass `drop_unmapped=True`.

```python
from orcapod.operators import MapData

renamed = MapData(name_map={"age": "age_weeks"})(source)
print(renamed.keys()[1])  # ('age_weeks', 'weight')
```

### PolarsFilter

Filter rows using Polars expressions:

```python
import polars as pl
from orcapod.operators import PolarsFilter

filt = PolarsFilter(predicates=[pl.col("age") > 10])
filtered = filt.process(source)
for tag, pkt in filtered.iter_data():
    print(f"{tag.as_dict()} -> {pkt.as_dict()}")
# {'subject_id': 'mouse_01'} -> {'age': 12, 'weight': 25.3}
# {'subject_id': 'mouse_03'} -> {'age': 15, 'weight': 27.8}
```

You can also filter by exact values using `constraints`:

```python
filt = PolarsFilter(constraints={"subject_id": "mouse_01"})
print(filt(source).as_table().to_pylist())
# [{'subject_id': 'mouse_01', 'age': 12, 'weight': 25.3}]
```

An expression predicate is part of the filter's identity, so a filtered stream can feed a
join, a function pod or a `PipelineJob`. Two filters written with the same expression share
their hashes, even when the expression is spelled differently:

```python
by_operator = PolarsFilter(predicates=[pl.col("age") > 10])(source)
by_method = source.polars_filter(pl.col("age").gt(10))
print(by_operator.pipeline_hash() == by_method.pipeline_hash())
# True
```

Orcapod hashes the expression through its own canonical encoding (label `polars_expr_v1`,
see the [hashing reference](../reference/hashing.md#1c-polars-expression-predicates)), which
covers column references; literals (numbers, strings, booleans, null, dates, times,
datetimes, durations, decimals, bytes, and Python lists of integers, floats, strings,
booleans, dates, times, datetimes or durations, but not of decimals or bytes); comparisons;
arithmetic;
`&`, `|`, `^` and `~`; `is_null`, `is_not_null`, `is_nan`, `is_not_nan`, `is_in`,
`is_between`, `all_horizontal`, `any_horizontal`, `str.contains`, `str.starts_with`,
`str.ends_with`; and `cast`. A predicate over several columns is supported when each column
is named, as in `(pl.col("x") > 0) & (pl.col("y") < 5)`.

Anything else (a Python function through `map_elements` or `map_batches`, a selector over
several columns such as `pl.all()` or `pl.col("x", "y")`, aggregations, `alias`,
`when/then/otherwise`, NaN or infinite literals, a `pl.Series` literal) raises `ValueError`
naming the expression as soon as the filter's identity is needed. The message suggests keyword
constraints for equality filters, or computing a boolean column in a function pod and
filtering on it:

```python
udf = source.polars_filter(
    pl.col("age").map_batches(lambda s: s > 10, return_dtype=pl.Boolean)
)
try:
    udf.pipeline_hash()
except ValueError as exc:
    print(str(exc).split(":")[0])
# Cannot hash the Polars expression 'col("age").python_udf()' for an Orcapod identity
```

### DropNulls

`DropNulls(columns)` keeps the rows where every named column is non-null, and narrows
each named column from `T | None` to `T`. `DropNulls()`, with no names, does this for every
tag and data column (see [Every column](#every-column) below). Use it when a column is nullable by specification
but a function needs a value: a pod whose parameter is a plain `int` refuses an `int | None`
column, and `PolarsFilter` can drop the null rows but leaves the type as it was.

```python
import pyarrow as pa
from orcapod import function_pod
from orcapod.operators import DropNulls
from orcapod.sources import ArrowTableSource

recordings = ArrowTableSource(
    pa.table(
        {
            "session": pa.array(["s1", "s2", "s3"], pa.large_string()),
            "start_ms": pa.array([1000, None, 3000], pa.int64()),
        },
        schema=pa.schema(
            [
                pa.field("session", pa.large_string(), nullable=False),
                pa.field("start_ms", pa.int64(), nullable=True),
            ]
        ),
    ),
    tag_columns=["session"],
)
print(recordings.output_schema()[1])
# Schema({'start_ms': int | None})


@function_pod(output_keys="start_s")
def to_seconds(start_ms: int) -> float:
    return start_ms / 1000


try:
    to_seconds.pod(recordings)
except ValueError as exc:
    print(str(exc).rsplit(": ", 1)[1])
# column 'start_ms' has type int | None, the function expects int

present = DropNulls("start_ms")(recordings)
print(present.output_schema()[1])
# Schema({'start_ms': int})
print(to_seconds.pod(present).as_table().to_pylist())
# [{'session': 's1', 'start_s': 1.0}, {'session': 's3', 'start_s': 3.0}]
```

The rules:

- **Columns.** Pass one name or a collection of names; a row survives only if every named
  column is non-null. The names may be tag columns, data columns, or both. A name that is
  not a tag or data column of the stream raises `InputValidationError`. With no argument
  the columns are every tag and data column of the input. An empty collection is not "no
  argument": it raises `ValueError`, so a list of names that came out empty never drops
  rows by surprise.
- **What counts as null.** Only the top-level value: a null list or a null struct is null. An
  empty list, a list that holds nulls, a struct whose fields are null and a floating-point
  NaN are not. Nested fields keep their nullability.
- **Already non-nullable columns** are accepted and left as they are, so applying the
  operator twice is the same as applying it once.
- **Everything else is unchanged**: other columns' types and nullability, logical types
  (a `Path | None` column becomes `Path`), system tags, source info and row order. A result
  with no rows has the same narrowed schema.
- **Identity** is the set of names: `DropNulls("a")`, `DropNulls(["a"])` and
  `DropNulls({"a"})` are one operator, and the order of names does not matter.
  `DropNulls()` has one fixed identity of its own, whatever stream it is applied to.
- **Row hashes.** Nullability is part of a row's Arrow schema, so a surviving row's data
  hash changes when a named data column was nullable (its tag hash, when a named tag column
  was). The new hash is the one the row has in a stream that declared the column
  non-nullable from the start.

Tag columns work the same way. A stream whose key column is `str | None` can be joined with
one whose key is `str`: a null key matches nothing, so the joined key comes out `str` (see
`Join`). Narrowing still matters wherever a column must be `T` before it is consumed, such
as a pod parameter typed `str`. The stream method is `drop_nulls`:

```python
by_method = recordings.drop_nulls(["session", "start_ms"])
by_operator = DropNulls(["start_ms", "session"])(recordings)
print(by_method.pipeline_hash() == by_operator.pipeline_hash())
# True
```

#### Every column

`DropNulls()` and `stream.drop_nulls()`, with no names, drop every row that has a null in
**any** tag or data column, and make every tag and data column non-nullable. System-tag,
source-info, context and meta columns are never tested (meta columns are not carried
at all, as by every operator).

```python
visits = ArrowTableSource(
    pa.table(
        {
            "session": pa.array(["s1", "s2", "s3", "s4"], pa.large_string()),
            "start_ms": pa.array([1000, None, 3000, 4000], pa.int64()),
            "notes": pa.array([None, "noisy", None, "ok"], pa.large_string()),
        }
    ),
    tag_columns=["session"],
)
print(visits.output_schema())
# (Schema({'session': str | None}), Schema({'start_ms': int | None, 'notes': str | None}))

complete = visits.drop_nulls()
print(complete.output_schema())
# (Schema({'session': str}), Schema({'start_ms': int, 'notes': str}))
print(complete.as_table().to_pylist())
# [{'session': 's4', 'start_ms': 4000, 'notes': 'ok'}]

timed = visits.drop_nulls("start_ms")
print(timed.as_table().to_pylist())
# [{'session': 's1', 'start_ms': 1000, 'notes': None}, {'session': 's3', 'start_ms': 3000, 'notes': None}, {'session': 's4', 'start_ms': 4000, 'notes': 'ok'}]
```

!!! warning "The no-argument form can drop most of a table"

    A null in any tag or data column drops the row, including a column that is empty for a
    good reason. Above, `notes` is empty for most sessions, so three of the four rows are
    gone although only one of them lacks `start_ms`. Sources whose columns are all nullable
    are common: SpiralDB stores every column as nullable. For such a source, name the columns
    a downstream step needs (`drop_nulls("start_ms")`); that is the precise form. Use the
    no-argument form only when every column must be present.

The column names are read from each input stream when the operator is applied, so one
`DropNulls()` object works on any stream. On the same stream it gives the rows and schema
of `DropNulls([every tag and data column])`, but the two are different operators: their
identities, and so the pipeline and content hashes of their output streams, differ.

```python
by_name = DropNulls(["session", "start_ms", "notes"])(visits)
print(complete.as_table() == by_name.as_table())
# True
print(complete.pipeline_hash() == by_name.pipeline_hash())
# False
```

### Pick and Index

`Pick(column, key)` extracts one entry from a struct- or dict-typed data column, and
`Index(column, i)` extracts one element from a list-typed data column (for example, a column
produced by `Batch` or `GroupBy`). See the [API reference](../api/operators.md).

With `out=None` the extracted value replaces the column in place; with `out="name"` it is added
as a new column and the original is kept. A row whose list is too short, or whose dict lacks the
key, is skipped (or raises `RuntimeError` with `fail_on_miss=True`).

Every other column keeps its input Arrow type, and the extracted column takes the input's
element or value Arrow type (a `list<int8>` column yields `int8`), both normalized to their
large variants as described in [Arrow types of operator outputs](#arrow-types-of-operator-outputs)
(a `list<string>` column yields `large_string`). The extracted column is
`NOT NULL`: its predicted type (`int`, not `int | None`) is not Optional, so an element or value
that is `None` raises `ValueError` naming the operator, the column and the row's tag, whatever
`fail_on_miss` is.

## Arrow types of operator outputs

`output_schema()` reports Python types, but every column also has an Arrow type, and
orcapod keeps it predictable.

**Type strictness.** Orcapod is type strict: it never expands or converts a type
implicitly. The only exception is normalization to large variants (below). When inputs
disagree on a column's type, that is an error naming the column and both types, never a
silent widening; convert explicitly.

- **Nothing is inferred from Python values.** An operator types each output column from its
  inputs' Arrow types, never from what PyArrow would guess for the values, so a result's
  types do not depend on the data, on whether it has rows, or on argument order.
- **Types are normalized to their large variants.** `string` becomes `large_string`,
  `binary` becomes `large_binary` and `list` becomes `large_list`, recursively (also inside
  structs). The Arrow hasher treats each pair as identical, so this never changes a hash.
- **Width, nullability and logical types are kept.** An `int8` column stays `int8`, a
  `float32` column stays `float32`, and a logical type such as `Path` keeps its logical type.
- **List columns built by `MergeJoin`, `Batch` and `GroupBy`** are `large_list<T>`, where `T`
  is the normalized input type, whether or not the result has rows. A list of a logical type
  is that type's logical list (`list[orcapod.path]` for `Path`). Their source-info columns
  are `large_list<large_string>`.
- **Inputs that disagree.** If the inputs of `MergeJoin` give a colliding data column, or
  the inputs of `Join` or `MergeJoin` give a shared tag column, different Arrow types, both
  are normalized first. If they are then the same (for example `string` and
  `large_string`), the join proceeds. Otherwise (for example `int16` and `int32`, or two
  timestamp units) it raises `InputValidationError` naming the column and both types; cast
  one input explicitly. Because `output_schema()` reports Python types, this is detected when
  the join runs, not when the pipeline is built.
- **Sync and async execution agree.** An operator run asynchronously (`async_execute`, as in
  an async pipeline) yields rows with the same Arrow types, column order, system tags and
  content hashes as the same operator run synchronously, whether its inputs are sources or
  the outputs of other operators and function pods. An input that delivers no rows gives no
  rows on both paths. One limit remains: an async channel carries rows, not a schema, so
  where the sync path returns an empty stream with typed columns the async path emits
  nothing, and an operator node that received no rows reads back columns typed from its
  Python output schema (ITL-858, ITL-912).

```python
import pyarrow as pa
from orcapod.sources import ArrowTableSource

def counts(values):
    table = pa.table(
        {
            "subject_id": pa.array(["mouse_01", "mouse_02"], type=pa.string()),
            "count": pa.array(values, type=pa.int8()),
        }
    )
    return ArrowTableSource(table, tag_columns=["subject_id"])

merged = MergeJoin()(counts([1, 2]), counts([3, 4]))
print(merged.as_table().schema.field("count").type)
# large_list<item: int8>
print(merged.as_table().schema.field("subject_id").type)
# large_string
```

## Using operators

All operators follow the same interface. Call `.process()` with one or more input streams:

```python
operator = Join()
result_stream = operator.process(subjects, measurements)
```

All standard operators also support `__call__` as a shorthand for `.process()`, so you can
write:

```python
result_stream = Join()(subjects, measurements)
```

The result is a new [stream](streams.md) that you can inspect, iterate, or pass to further
operators or function pods.

### Fluent methods

Every stream also exposes the operators as methods, which reads well when chaining:
`join`, `semi_join`, `batch`, `group_by`, `select_tag_columns`, `select_data_columns`,
`drop_tag_columns`, `drop_data_columns`, `map_tags`, `map_data`, `polars_filter`,
`drop_nulls`, `pick` and `index`.

```python
import polars as pl

adults = subjects.join(measurements).polars_filter(pl.col("age") > 10)
for tag, data in adults.iter_data():
    print(tag.as_dict(), "->", data.as_dict())
# {'subject_id': 'mouse_01'} -> {'age': 12, 'weight': 25.3}
```

!!! note
    The fluent `map_tags` and `map_data` methods and the `MapTags` and `MapData` operator
    classes all keep the columns they do not rename. Pass `drop_unmapped=True` to drop them.

## How it connects to other concepts

- Operators consume and produce [Streams](streams.md)
- [Sources](sources.md) produce the initial streams that operators transform
- [Function Pods](function-pods.md) handle value-level transforms (the complement of operators)
- Operators participate in the [identity chain](identity.md) -- each operator's hash includes
  its own identity plus its upstream hashes
