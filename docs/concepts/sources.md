# Sources

Sources are the entry points for external data into an Orcapod pipeline. Every pipeline begins
with one or more sources that load raw data -- from Python dicts, lists, CSV files, Delta Lake
tables, or Pandas DataFrames -- and present it as an immutable
[stream](streams.md) of (Tag, Data) pairs. Sources also attach provenance metadata
(source-info columns and system tag columns) so that every downstream value can be traced back
to its origin.

## Key classes

### `RootSource` (abstract base)

All sources inherit from `RootSource`. A root source is a pure stream with no upstream
dependencies -- it sits at the root of the computational graph. Key properties:

- `source.producer` returns `None` (no upstream pod)
- `source.upstreams` is always an empty tuple
- `source.source_id` is a canonical name used for provenance tracking and the source registry

### Concrete source types

All sources follow the same pattern: convert input data to a PyArrow Table, then pass it
through `SourceStreamBuilder` which handles enrichment (provenance columns, system tags,
hashing) and produces the final immutable stream.

| Source | Input type | Notes |
|---|---|---|
| `ArrowTableSource` | PyArrow `Table` | Accepts an Arrow table directly |
| `DictSource` | `list[dict]` | Each dict becomes one (Tag, Data) pair |
| `ListSource` | `list[Any]` | Each element stored under the data column given by `name`; the tag defaults to `element_index` |
| `DataFrameSource` | Pandas or Polars `DataFrame` | Converts via Arrow |
| `CSVSource` | File path (string) | Reads CSV into Arrow |
| `DeltaTableSource` | Directory path | Reads Delta Lake table |

`orcapod.sources` also provides database-backed sources (`SQLiteTableSource`,
`PostgreSQLTableSource`, `SpiralDBTableSource`) and `PollingSource` for data that arrives over
time; see the [API reference](../api/sources.md).

Two construction rules apply to every source and are the most common first errors:

- Every name in `tag_columns` must be a column of the data, otherwise the source raises
  `ValueError: tag_columns not found in table`.
- At least one column must be left over as a data column, otherwise it raises
  `ValueError: No data columns found in the table`.

Data with no rows has no columns to read, so `DictSource(data=[], ...)` raises
`ValueError: Table is empty`. To build a source with no rows, pass `data_schema=` so the
columns are known.

### SpiralDB tables

`SpiralDBTableSource` exposes a table in a [SpiralDB](https://spiraldb.com) project as a
source. Install the extra and log in once; the `spiral` command comes with the extra, and
pyspiral keeps the login in its per-user directory (`~/Library/Application Support/pyspiral/`
on macOS, `~/.config/pyspiral/` on Linux):

```bash
pip install "orcapod[spiraldb]"
spiral login
```

Log in before you build a source. Without a saved login, pyspiral starts a sign-in in the
browser and waits until it is completed.

To find the tables of a project and their key columns:

<!-- docs-test: skip -->
```python
from orcapod.databases import SpiralDBConnector

with SpiralDBConnector("edi-sample-v1-855632") as spiral:
    print(spiral.get_table_names())
    print(spiral.get_pk_columns("session"))
```

The table's key columns become the tag columns, and every other column is a data column:

<!-- docs-test: skip -->
```python
from orcapod.sources import SpiralDBTableSource

sessions = SpiralDBTableSource("edi-sample-v1-855632", "session")
experiments = SpiralDBTableSource("edi-sample-v1-855632", "experiment")
print(sessions.output_schema())
sessions.as_df()  # also as_polars_df(), as_pandas_df(), as_lazy_frame(), as_table()
```

Things to know about Spiral tables:

- **Nullability comes from the table's nullability record.** See
  [Nullability of Spiral columns](#nullability-of-spiral-columns): a recorded non-nullable
  column has the type `T`, every other column `T | None`.
- **Types are kept.** Arrow types come through unchanged (`uint32` stays `uint32`), except
  that strings, binaries and lists become their large variants. Joining two Spiral tables on
  their shared key columns works; joining with another source requires the same Arrow types on
  both sides (`uint32` against `int64` is an error, not a conversion).
- **The whole table is read when the source is built.** Use tables of up to a few hundred
  thousand rows. Very large tables (for example `spike_event`) need the streaming source that
  is still being designed.
- **Not supported:** columns of type `map`, which the Arrow hasher does not hash. Building
  the source raises `ValueError` naming each such column and its type; drop or convert
  those columns first. `fixed_size_list` columns are supported. Also not supported: time zones (SpiralDB stores
  timestamps without one) and Orcapod logical types such as `Path` (they come back as their
  storage type).
- **Source identity.** The default `source_id` is `<project id>/<dataset>/<table>`, for
  example `edi-sample-v1-855632/default/session`, so tables with the same name in different
  projects stay apart.
- **Record identity.** Without `record_id_column`, a row's record ID comes from its position
  in the table, which SpiralDB orders by key, so a row inserted before existing rows changes
  the IDs of the rows after it. Pass `record_id_column=` (a column whose value is unique per
  row) to derive the IDs from that column's values instead, so they stay with their rows.

#### Nullability of Spiral columns

SpiralDB reports every column as nullable, key columns included, and every field of a struct
column too, whatever nullability the data was written with; only list items keep a non-null
flag. Orcapod therefore keeps a **nullability record** for each table: the list of the table's
non-nullable columns and nested fields. The nullability record is an entry in the table's
key-value metadata, next to the data; it is not a row of the table.

- **A table with a nullability record** (written by `SpiralDBConnector`, by a
  `ConnectorArrowDatabase` on SpiralDB, or given one with `write_nullability_record`, as the
  tables of the workshop project are): the columns it lists have the type `T`, so a function
  pod can take `x: float`. Every batch read is checked: a null in a listed column raises
  `NullInNonNullableColumnError` naming the table and the column, so a wrong nullability
  record never hides a null. A nullability record that cannot be read in full (unknown
  version, missing part, a column the table does not have) raises `NullabilityRecordError`.
- **A table without a nullability record** (for example the EDI sample tables): every column
  has the type `T | None` (`str | None`, `datetime.datetime | None`, ...). A function pod
  receiving such a column must accept `None`: annotate the parameter `x: float | None`, not
  `x: float`, or the pod refuses the stream. Such a table reads exactly as it did before
  nullability records existed: same schema, same hashes.
- **Joins are type strict.** A key column that is `T` in a table with a nullability record and
  `T | None` in a table without one is an error, not a conversion.

`SpiralDBConnector` writes the nullability record on a table's first write, before the rows.
It refuses a write that holds a null in a column its schema declares non-nullable, nested
fields included (SpiralDB would store a null written to a non-nullable struct field as `0`),
and a later write whose non-nullable columns differ from the nullability record. It never adds
a nullability record to a table that already holds rows, and never changes one.

SpiralDB cannot store a null struct column. pyspiral refuses one (`Spiral does not support
null column groups`) unless the writer passes `push_down_nulls=True`, and then the null struct
reads back as a struct whose fields are all null: a different value with a different hash. (Both
were seen for a top-level struct column; nested structs were not tested.) `SpiralDBConnector`
refuses a write holding a null struct at any depth (a struct column, a struct field of a
struct, a struct inside a list) with `UnsupportedValueError` naming the table and the column,
and writes nothing. Null lists, null list items and null struct fields are stored as written.

Writes are not coordinated between writers: SpiralDB has no lock for them. If two writers make
the first write to the same new table at once and one of them fails, the table can be left
without a nullability record although rows were written. It then reads as a table without one
(every column `T | None`); a null is never hidden. To repair it, check that the non-nullable
columns hold no null and call `write_nullability_record` as below.

Other writers give a table its nullability record with `write_nullability_record`, passing the
Arrow schema the data was written with. It reads no data, so check first that no non-nullable
column holds a null. The same call replaces a wrong nullability record.

<!-- docs-test: skip -->
```python
import pyarrow as pa
import spiral

from orcapod.databases import read_nullability_record, write_nullability_record

table = spiral.Spiral().project("my-project-123456").table("default.session")
schema = pa.schema(
    [
        pa.field("subject_id", pa.string(), nullable=False),
        pa.field("weight", pa.float64(), nullable=False),
        pa.field("booth", pa.string(), nullable=True),
    ]
)
write_nullability_record(table, schema)  # True if it was written, False if already current
print(read_nullability_record(table.get_metadata()))
# frozenset({('subject_id',), ('weight',)})
```

**Adding or changing a nullability record changes the table's identity in Orcapod.** The
nullability is part of the schema, and the schema is part of a source's `pipeline_hash` and
`content_hash`. For the `session` table above:

<!-- docs-test: skip -->
```python
from orcapod.sources import SpiralDBTableSource

# Before write_nullability_record:
SpiralDBTableSource("my-project-123456", "session").output_schema()
# (Schema({'subject_id': str | None}), Schema({'weight': float | None, 'booth': str | None}))

# After it:
SpiralDBTableSource("my-project-123456", "session").output_schema()
# (Schema({'subject_id': str}), Schema({'weight': float, 'booth': str | None}))
```

The two sources have different `pipeline_hash` and `content_hash` values, and so do the nodes
built on them. A pipeline built on the table before it had a nullability record keeps its
stored results under its old identity: after the change, those nodes compute again under new
identities, and the earlier results are not reused (nothing stored is changed or deleted).
Pods annotated `weight: float | None` keep working, since they also accept `float`. So the
practical rule is: **write the nullability record when the table is created, before anyone
builds pipelines on it.**

The nullability record is stored under the metadata key `__orcapod_nullability__` (format
`orcapod.nullability`, version 1), split over further keys when a wide table needs more than
SpiralDB's 8 KiB per metadata entry. Other SpiralDB clients ignore it and still see every
column as nullable.

### `DerivedSource`

A `DerivedSource` is a source whose rows come from something Orcapod already has: another
source, or a node of a job. It has two uses.

**Reading a job's results.** A `DerivedSource` reads the stored results of a node of a
[`PipelineJob`](function-pods.md#pipelinejob-database-backed-cached-execution) that has run
and presents them as a new source. You get one by calling `as_source()` on the node, for
example `job.nodes["bmi"].as_source()`. This is useful for chaining pipeline stages: run a
job, then use one of its outputs as the input of a new pipeline.

**Using a tag value as an input to a computation.** Tags identify a row and data is what a
function computes on, so a function pod never receives a tag. Sometimes one value is needed
for both: a session's start time identifies the session, and it is also what you subtract to
get an elapsed time. Pass `tags_as_data` to get a source that keeps the tag and also offers
its value as a data column under a name you choose:

```python
from datetime import datetime, timezone

from orcapod import function_pod
from orcapod.sources import DerivedSource, DictSource

start_1 = datetime(2026, 1, 5, 9, 0, tzinfo=timezone.utc)
start_2 = datetime(2026, 1, 6, 9, 30, tzinfo=timezone.utc)

experiments = DictSource(
    data=[
        {"subject_id": "m1", "session_start": start_1, "experiment_index": 0,
         "experiment_start": datetime(2026, 1, 5, 9, 1, tzinfo=timezone.utc)},
        {"subject_id": "m1", "session_start": start_1, "experiment_index": 1,
         "experiment_start": datetime(2026, 1, 5, 9, 15, tzinfo=timezone.utc)},
        {"subject_id": "m1", "session_start": start_2, "experiment_index": 0,
         "experiment_start": datetime(2026, 1, 6, 9, 32, tzinfo=timezone.utc)},
    ],
    tag_columns=["subject_id", "session_start", "experiment_index"],
    source_id="experiments",
)

with_start = DerivedSource(
    experiments, tags_as_data={"session_start": "session_start_value"}
)

tag_keys, data_keys = with_start.keys()
print(tag_keys)
# ('subject_id', 'session_start', 'experiment_index')
print(data_keys)
# ('experiment_start', 'session_start_value')

@function_pod(output_keys="onset_s")
def onset_s(session_start_value: datetime, experiment_start: datetime) -> float:
    return (experiment_start - session_start_value).total_seconds()

onsets = onset_s.pod(with_start)
print(onsets.as_table().select(["subject_id", "experiment_index", "onset_s"]).to_pandas())
#   subject_id  experiment_index  onset_s
# 0         m1                 0     60.0
# 1         m1                 1    900.0
# 2         m1                 0    120.0
```

The result still has `session_start` as a tag, so it joins with other tables on that key as
before. Things to know:

- The mapping is `{tag column: new data column}`. The new name must not already be a column
  and must not start with `_`; a wrong name raises `InputValidationError` when the source is
  built.
- The copy has exactly the tag's type, including whether it may be null. If the tag may be
  null, annotate the function parameter as optional (`datetime | None`).
- The copy equals the tag when the source is read. After that it is ordinary data: a function
  may replace it and `map_data` may rename it, while the tag stays as it was.
- It works on any source (`DerivedSource(source, tags_as_data=...)`) and on a job node
  (`job.nodes["bmi"].as_source(tags_as_data=...)`).
- It is a **new source**, not a step in the origin's pipeline: `upstreams` is empty, and it
  has provenance columns of its own, like the result of handing the same rows to
  `ArrowTableSource`. Use it in place of the origin. Its default `source_id` is
  `tags_as_data:` followed by a labelled hash built from the origin's identity and the mapping, so
  it changes when either changes; pass `source_id=` to choose a name.
- A saved job reloads a derived source whenever its origin can be reloaded (a CSV file, a
  Delta table, a database table). Over an in-memory source it is saved but not reloaded, like
  the in-memory source itself.

```python
print(with_start.upstreams)
# ()
print(with_start.source_id)
# tags_as_data:...:...
```

Only a source can do this. An operator rearranges rows and columns, and every data value it
emits was already a data value of its input; turning a tag value into a data value creates
new data, which is what a source is for.

## How sources add provenance columns

When you create a source, Orcapod automatically adds two kinds of hidden columns to track
data lineage:

**Source-info columns** (prefix `_source_`) store a provenance token for each data column.
For example, a data column `weight` gets a companion `_source_weight` column. These tokens
identify which source originally produced each value.

**System tag columns** track which source and which source row contributed each row. Every
source adds two of them, `_tag_source_id::<hash>` and `_tag_record_id::<hash>`, where `<hash>`
is the source's pipeline hash. These columns are used internally during
[joins](operators.md) to maintain provenance through multi-stream operations.

Each row's system tag carries a record ID derived from the source's `source_id` and a
per-row token. By default that token is the row's position, so a row that moves to another
position under the same `source_id` gets a different record ID. Downstream pipeline entries
and `sink_pod` completion records are keyed on the system tags together with a hash of the
row's data, so the moved row is treated as a new entry: a function node records it again,
and its stored table then shows two entries with identical tag values but distinct record
IDs, and a `sink_pod` delivers it again. Because every key includes the hash of the row's own
data, no result is ever paired with the wrong row; a function's stored result for that data
is reused rather than recomputed.

Sources that accept `record_id_column` use that column's value instead, so the record ID
follows the row. Both halves of the record ID must be stable: when you omit `source_id` it
defaults to a hash of the table, which changes when the rows are reordered. Whenever row
order can vary between runs, pass an explicit, stable `source_id` **and** set
`record_id_column` to a column that is unique per row; this avoids both the duplicate entries
and the repeated deliveries.

These columns are hidden by default. You can reveal them using `ColumnConfig`:

```python
from orcapod.sources import DictSource

source = DictSource(
    data=[{"subject_id": "mouse_01", "age": 12, "weight": 25.3}],
    tag_columns=["subject_id"],
    source_id="mice",
)

# Show source-info columns
table = source.as_table(columns={"source": True})
print(table.column_names)
# ['subject_id', 'age', 'weight', '_source_age', '_source_weight']
print(table.column("_source_weight").to_pylist())
# ['mice::row_0::weight']

# Show system tag columns
tag_schema, data_schema = source.output_schema(columns={"system_tags": True})
print(list(tag_schema))
# ['subject_id', '_tag_source_id::...', '_tag_record_id::...']

# Show everything
table = source.as_table(all_info=True)
print(table.column_names)
# ['subject_id', 'age', 'weight', '_tag_source_id::...', '_tag_record_id::...', '_content_hash', '_context_key', '_source_age', '_source_weight']
```

Pass `source_id` to give the source a readable name in these provenance tokens. Without it,
Orcapod derives an id from the source's content hash.

## Code example

Create a `DictSource`, inspect its schema, and iterate over its stream:

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

# Inspect the schema
tag_schema, data_schema = source.output_schema()
print("Tag schema:", dict(tag_schema))
# Tag schema: {'subject_id': <class 'str'>}
print("Data schema:", dict(data_schema))
# Data schema: {'age': <class 'int'>, 'weight': <class 'float'>}

# Get column names
tag_keys, data_keys = source.keys()
print("Tag keys:", tag_keys)
# Tag keys: ('subject_id',)
print("Data keys:", data_keys)
# Data keys: ('age', 'weight')

# Iterate over (Tag, Data) pairs
for tag, data in source.iter_data():
    print(f"  Tag: {tag.as_dict()}, Data: {data.as_dict()}")
# Tag: {'subject_id': 'mouse_01'}, Data: {'age': 12, 'weight': 25.3}
# Tag: {'subject_id': 'mouse_02'}, Data: {'age': 8, 'weight': 22.1}
# Tag: {'subject_id': 'mouse_03'}, Data: {'age': 15, 'weight': 27.8}

# Convert to a PyArrow table
table = source.as_table()
print(table.to_pandas())
#   subject_id  age  weight
# 0   mouse_01   12    25.3
# 1   mouse_02    8    22.1
# 2   mouse_03   15    27.8
```

## How it connects to other concepts

- Sources produce [Streams](streams.md) -- immutable sequences of (Tag, Data) pairs
- Streams flow into [Operators](operators.md) for structural transforms (joins, filters,
  column selection)
- Streams flow into [Function Pods](function-pods.md) for value-level transforms
- Every source has a `content_hash()` and `pipeline_hash()` -- see
  [Identity & Hashing](identity.md) for how these work
