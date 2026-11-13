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
- **Not supported:** columns of type `map`, which the Arrow hasher does not hash. Building
  the source raises `ValueError` naming each such column and its type; drop or convert
  those columns first. `fixed_size_list` columns are supported. Also not supported: time zones (SpiralDB stores
  timestamps without one) and Orcapod logical types such as `Path` (they come back as their
  storage type).
- **Source identity.** A Spiral source is identified by where its table is, never by its
  rows. The default `source_id` is `<project id>/<dataset>.<table>@<Spiral table id>`, for
  example `edi-tutorial-v1-841226/default.session@table_Jo7J0lpkLcOM`. The table id is
  SpiralDB's own, so tables with the same name in different projects stay apart, and a table
  that is dropped and created again under the same name is a different source: nothing
  recorded for the old table is reused. A renamed table also gets a new `source_id`.
- **Record identity.** A row is identified by its primary key: its record ID is derived from
  the hash of its key columns. A row keeps its record ID when other rows are added, and has
  the same one whichever read returned it.

#### Reading a Spiral table

Building a `SpiralDBTableSource` reads no rows. It reads the table's schema, key columns and
table id, which takes about a second whatever the table's size. `output_schema()`, `keys()`,
`content_hash()` and `pipeline_hash()` read no rows either. Rows are read when you ask for
them, and every read scans the table again:

- `iter_data()` and a pipeline job hand the rows on in batches of at most `batch_rows`
  rows (default 8,192). SpiralDB sends a scan in batches that follow its stored fragments,
  whose sizes cannot be chosen; one of them is held while its rows are handed on.
- `as_table()`, `as_df()`, `as_polars_df()`, `as_pandas_df()` and `as_lazy_frame()` read
  the rows into memory and return them all.

To read part of a table, say so when you build the source. SpiralDB applies both
restrictions before any row is transferred:

<!-- docs-test: skip -->
```python
first = SpiralDBTableSource("edi-tutorial-v1-841226", "spike_event", limit=1000)
unit = SpiralDBTableSource(
    "edi-tutorial-v1-841226", "spike_event", where={"unit_id": 106}
)
print(first.row_count_estimate)  # rows in the whole table, without reading any
```

- `limit=n` reads the first `n` rows in key order.
- `where={column: value, ...}` reads the rows in which every named column equals its value.
- `asof=` reads the table as of an earlier version. Without it, a source reads the version
  the table had when the source was built, in every read.

**The row cap.** One read returns at most `max_rows` rows (default 100,000). A read that
would return more raises `SourceTooLargeError`, which gives the row count and the ways out:
`limit=`, `where=`, or a larger `max_rows` (`max_rows=None` removes the cap). When nothing
but `limit=` bounds the read, the source decides before reading any row, from the row count
SpiralDB records for the table. That count can include deleted rows that are not yet
compacted, so a table near the cap can be refused although a scan would return fewer rows;
the error says so. The cap is there
because a pipeline job keeps every row it processes in memory, about 5 KB per row for each
node, so 100,000 rows cost about 0.5 GB per node. A table of millions of rows can be iterated
with `max_rows=None`, but it cannot yet be run through a job on a laptop.

**Looking at a table in a notebook.** A Spiral source shown at the end of a notebook cell
(or through `view()`) reads only its first 10 rows, in one scan to which the row cap does not
apply, so a table of any size can be looked at. The header line says what is shown, for
example `SpiralDBTableSource[spike_event] — first 10 rows of about 63,349` (the table's
recorded row count, read without scanning), `all 4 rows` when the source reads no more than
that, or `first 10 matching rows (where unit_id=106); the table has about 63,349 rows` with
`where=`. Tag columns come first, marked with `*`. To see more rows, use `as_df()` with a
`limit=` or `where=` source.

**Restrictions are not part of identity.** `where`, `limit` and `asof` choose rows of the
same table, so two sources on one table have the same `source_id` and `content_hash`
whatever their restrictions, and a row's result is computed once and shared. One
consequence: with a shared store, reading a job's node back (`get_all_records()`, a loaded
job, and `as_table()` after an async run) returns every row that any job with the same
pipeline has read from that table, not only the rows of the latest restriction. The rows
sent downstream during a run are only the rows that run read.

**Running again.** A second run scans the table again and computes nothing for the rows it
has already seen. A source object keeps reading the version its table had when it was
built, so rows added to the Spiral table afterwards are seen only by a new source: build
the source (and the job around it) again, and the added rows are new keys, the only rows
computed.

**Key columns as data.** A function pod never receives a tag, and in a time-series table the
time is part of the key, so it is a tag. `tags_as_data` also offers named key columns as data
columns, copied as each batch is read (so the source still reads no rows when built, and
`limit=`, `where=`, `max_rows` and the notebook preview work as before). The tag keeps its
name; you name the copy, which comes after the table's data columns, with the key column's
type, never null:

<!-- docs-test: skip -->
```python
spikes = SpiralDBTableSource(
    "edi-tutorial-v1-841226", "spike_event", tags_as_data={"timestamp": "spike_time_us"}
)
unit_key = [
    "spike_extraction_hash", "subject_id", "session_start_datetime",
    "recording_index", "probe_index", "unit_id",
]
per_unit = spikes.group_by(unit_key).drop_tag_columns("timestamp")
print(per_unit.keys())
# (('spike_extraction_hash', 'subject_id', 'session_start_datetime', 'recording_index', 'probe_index', 'unit_id'), ('spike_amplitude_uv', 'spike_time_us'))
```

`per_unit` has one row per unit, and its `spike_time_us` list is in time order: `group_by`
orders the members of a group by the tags outside the group key, and on the full unit key
the only such tag is `timestamp`. `group_by(["unit_id"])` alone sorts by
`spike_extraction_hash`, subject, session, recording, probe, then `timestamp`, so time order
holds only within one session and probe; group on the full unit key.

The copies are data columns, so they are part of the schema, the `content_hash` and the
`pipeline_hash`; the mapping itself is part of the `content_hash` too, because two mappings
can give the same schema while the copy holds different values. The `source_id` and every
row's record id are the same as without the option. Compared with
`DerivedSource(spikes, tags_as_data=...)`, which reads the whole table as soon as its schema
or rows are asked for (building a job asks for the schema), the option keeps a Spiral source,
whose schema is known without a scan.

**Known limitations.**

- Each row's key is hashed with one call, about 0.2 ms per row (about 18 s per 100,000
  rows), in addition to about 0.1 ms per row to build the row.
- A job keeps its rows in memory (see the row cap above).
- Only equality on column values can be pushed down; there is no key range, no column
  selection (use `SelectDataColumns` after the source), and no polling for new rows within
  one run.
- A table that has only its key columns (for example one never written to) has no data
  column and cannot be a source.

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
follows the row. Both halves of the record ID must be stable: when you omit `source_id` an
in-memory source is named after a hash of its table, as
`<source kind>:<algorithm label>:<hex>` (for example `dict:arrow_v1:000002fe…`), which changes
when the rows are reordered. Whenever row
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
