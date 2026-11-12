# Changelog

## [0.3.0] — unreleased

### Breaking changes and migration

#### NumPy arrays: new storage format and new hashes, no automatic migration (ITL-888, ITL-883)

**The array format and every array hash changed in v0.3.** A store written by v0.2 that holds
`numpy.ndarray` columns is **not migrated**. Every node that handles arrays gets new tables,
so its v0.2 results are no longer found; and since every Arrow-derived hash changed too
(ITL-910, below), no v0.2 result is reused at all: v0.3 ignores the rows a v0.2 store holds and
computes every input again. Nothing written by v0.2 is changed or deleted, and those tables stay
readable with v0.2. A migration is planned after v0.3 (ITL-889).

- **Storage.** An array column is now the Arrow extension type `orcapod.ndarray` (metadata
  `{"version":1}`) over `struct<dtype: large_string, shape: large_list<int64>,
  data: large_binary>`: an Orcapod dtype tag, the shape, and the elements in C order,
  little-endian, packed. It replaces the `.npy` file under `numpy.ndarray`; that name is no
  longer registered or written.
- **Identity.** An array is its dtype, shape and element bits. A structured dtype's field
  names, types and order are part of the dtype, at every nesting depth. Memory layout, byte
  order, structured offsets and padding, and subclass are not part of it. Arrays read back
  canonical: C order, little-endian (`>f4` reads back `<f4`), structured fields packed in
  their declared order, as plain `np.ndarray`.
- **Hash.** `NumpyArrayHandler` returns `ContentHash("ndarray_v1", digest)`. The digest is
  SHA-256 over a length-prefixed encoding of the stored row (`docs/reference/hashing.md`,
  section 8), and the label replaces the bare `sha256`. Because the logical type name is now
  `orcapod.ndarray`, the type, schema and pipeline hashes of anything with an array column
  change too, and so do the table paths of those nodes. Pipeline hashes are chained, so the
  pipeline and operator tables of every node downstream of an array column move as well,
  even when it has no array column itself. A pod whose inputs and outputs hold no array
  keeps its function result table, and nodes with no array column upstream keep their tables;
  but an array-free pod does **not** reuse its v0.2 results: every Arrow-derived hash changed
  as well (ITL-910, below) and every table moved to a new layout (ITL-944, below), so the
  v0.2 tables are not read and the inputs are computed again.
- **Refused at write time** with `orcapod.errors.UnsupportedValueError` (a `ValueError`), naming
  the column and dtype: masked arrays (the mask used to be dropped silently), object dtypes,
  NumPy 2 `StringDType` (used to fail with a misleading pickle error), `longdouble` and
  `clongdouble`, datetimes and timedeltas without a unit, and structured dtypes with titles
  (whether to accept these last three kinds later is tracked in ITL-911).
- **Function pods.** A pod declared `-> np.ndarray` that returns something other than an array,
  such as a NumPy scalar, now raises `TypeError` naming the output.
- **MergeJoin** orders merged arrays by their stored `(dtype tag, shape, data)`, and lists of
  arrays (an array column after `GroupBy` or `Batch`) by their elements' keys, in order.
- **Pick** on a column whose logical type has no keyed access (an array, a dataclass, a
  pydantic model) raises `InputValidationError` naming the column, instead of
  `NotImplementedError` (ITL-883).
- `LogicalTypeRegistry.get_by_python_type` now picks the most specific registered class along
  the MRO, so a registered `np.ndarray` subclass wins over `np.ndarray` for itself and its
  subclasses, whatever the registration order.

#### A changed column type is refused when appending to a Delta table (ITL-884)

Before writing to an existing `DeltaTableDatabase` table, the flush now compares the new rows'
column types with the stored ones. If a column's type differs, the flush raises
`InputValidationError` naming the table, the column and both types, and writes nothing to that
table (other tables' pending rows in the same flush may still be committed). Before,
Delta's merge silently cast the new rows to the stored type (`int32` or `float64` rows written
into an `int64` column, `large_binary` into `large_string`), or, when an extension (logical) type
was involved, accepted rows the table could then no longer read. The comparison treats types the
way Delta stores them: `string` and `large_string` (and the other large and non-large layouts)
are the same type, types the Delta writer converts on the way in are compared as converted
(unsigned integers as signed, every timestamp in microseconds), an extension type is compared by
its storage type, and nullability is not compared. Struct fields are compared by name at any depth, including inside lists and maps.
New and missing columns, and new and missing struct fields, pass the check; whether Delta's
merge adds a new one follows the database's `allow_schema_evolution` setting, as before.

`DeltaTableDatabase.flush()` (and `add_records(..., flush=True)`) now raises a
`StoreReadError` or this `InputValidationError` as itself. Before, it wrapped every failure in a
`RuntimeError`. When several tables fail in one flush, the `StoreReadError` is raised, or else the
first `InputValidationError`, with each other failure attached as a note and logged. Failed rows
stay queued for the next flush. A flush that fails only for other reasons still raises
`RuntimeError`, with the first failure as its cause.

### Added

- **`orcapod.errors.StoreWriteError`** — raised when a node's results or records cannot be
  written to the store. It names the node, the row's tag and the table, and chains the
  database's own exception as its cause. (ITL-878 #287)
- **`orcapod.errors.StoreReadError`** — raised when a stored table exists but cannot be read.
  It names the table, and the node and row being processed when they are known, and chains
  the backend's exception as its cause. (ITL-884 #290)
- **`orcapod.errors.StoreError`** — the base of `StoreReadError` and `StoreWriteError`;
  `except StoreError` handles both. (ITL-884 #290)

### Fixed

#### A Delta table that cannot be read is no longer reported as having no rows (ITL-884)

When a `DeltaTableDatabase` table existed but could not be read (a missing or damaged data
file, a log the backend cannot load, data files whose column type no longer matches the
table), `get_all_records` and `get_records_by_ids` returned `None`, the same answer as for a
table that does not exist. A `PipelineJob` then warned that the stored results "may have been
deleted externally", swallowed the per-row read errors under `error_policy="continue"`, finished
without raising, and its nodes showed no records. Every read now raises
`orcapod.errors.StoreReadError`, which names the table and chains the backend's error, and a
store read failure fails the run under both orchestrators whatever the `error_policy`, also
when it happens while a row's result is being stored. A table that does not exist still reads as
empty.

A flush that could not load an existing table no longer treats it as absent. Before this fix a
`DeltaProtocolError` while loading the table made the flush write the batch with
`mode="overwrite"`, replacing the stored rows. The flush now raises and writes nothing.

A write that fails because the stored table cannot be read raises `StoreReadError`, not
`StoreWriteError`. When read and write failures happen in the same run, both orchestrators
raise the error of the node that comes first in the pipeline (ITL-884 #290).

#### A failed store write fails the run (ITL-878)

A failed store write fails the run. `PipelineJob.run()` used to return normally when a node's
result or record could not be written (the row was skipped like a crashed function under the
default `error_policy="continue"`), so the node read back fewer rows than it computed. It now
raises `StoreWriteError` at the first failed write, whatever `error_policy` is, on both
orchestrators (the async one no longer raises an exception group). Rows stored before the
failure are committed and kept; re-running does not recompute stored function results. A failed
flush of pending writes raises `StoreWriteError` with `flush=True` and names the store, not a
row. A function output that cannot be turned into a record (a naive `datetime`, a value not
matching the declared type) is still a row failure governed by `error_policy`, like a function
that raised. See *When the store cannot be written* in the function-pods guide. (ITL-878 #287)

#### Newcomer papercuts (ITL-821)

- `orcapod.enable_file_hash_caching` is exported at the top level (ITL-821).
- A source built from data with no rows and no schema (e.g. `DictSource(data=[])`) raises
  `ValueError: Table is empty` instead of `tag_columns not found in table` (ITL-821).
- Applying a function pod to a stream whose data schema does not fit no longer prints a stray
  `Key ... not found in parameter types.` line, and the error names the offending column,
  e.g. `Incoming data schema ... is not compatible with expected input schema ...: column
  'age' is not a parameter of the function` (it used to say `Incoming data data type`)
  (ITL-821).
- `examples/save_and_load_pipelines.py` runs again, rewritten around `PipelineJob`; the test
  suite now runs every example (ITL-821).
- `polars_filter` with a Polars expression predicate (`stream.polars_filter(pl.col("x") > 0)`)
  can be hashed, so it works upstream of a join and inside a `PipelineJob` (ITL-828). An
  expression hashes under the new label `polars_expr_v1`, through Orcapod's own canonical
  encoding of the expression tree rather than Polars' serialization, which is not stable
  across Polars versions. Column references, literals, comparisons, arithmetic, boolean
  operators, `is_null`/`is_not_null`/`is_nan`/`is_not_nan`, `is_in` (a Python list),
  `is_between`, `all_horizontal`/`any_horizontal`, `str.contains`/`starts_with`/`ends_with`
  and `cast` are supported. Anything else, notably Python functions (`map_elements`,
  `map_batches`) and selectors over several columns, raises `ValueError` naming the
  expression when the filter's identity is needed. Keyword-constraint filters hash as before.
- Inside a `PipelineJob`, a `PolarsFilter` (keyword constraints too) upstream of a `Join` no
  longer raises `InputValidationError` (`str` vs `str | None`): the filter now keeps every
  column's input nullability, instead of returning every column nullable (ITL-828). A filtered
  row's data hash now equals its source row's, so the data hashes of filtered rows change from
  `0.2.0`, and a function pod downstream of a `polars_filter` recomputes its results once
  against a `0.2.0` store.
- `PolarsFilter(predicates=[...])` and `stream.polars_filter(...)` with the same predicates now
  have the same hashes (`predicates` is stored as a tuple).

## [0.2.0] — 2026-11-10

**Distribution:** Orcapod is MIT licensed starting with `0.2.0` (it was proprietary on
`main` between #277 and #283; no release carried that license). `0.2.0` is a tag only;
`0.3.0` is the first release published to PyPI from this repository. Until then,
`pip install orcapod` and `uv add orcapod` install `0.1.4`, the latest release on PyPI;
`0.2.0` is the tag `v0.2.0` of the repository.

Everything merged to `main` since `v0.1.2`. `0.1.3` and `0.1.4` were patch releases cut
from a separate branch; their one fix is also in this release (see
*Fixed*), so upgrading from `0.1.4` loses nothing.

**Read *Breaking changes and migration* before upgrading an existing pipeline database.**
A pipeline or result database written by any `0.1.x` release cannot be read by `0.2.0`
until it is migrated or explicitly ignored.

### Breaking changes and migration

#### Upgrading a store written by `0.1.4`: what is still found

Checked on 2026-11-10 by writing a Delta Lake store with the released `orcapod==0.1.4` and
opening it with `0.2.0`. Sources whose columns are plain types (`int`, `str`, …) keep the
same schema, content and pipeline hashes and system-tag column names as on `0.1.4`.

| Change | What a `0.1.4` user sees | Are stored results still found? |
|---|---|---|
| Table paths gain `pdb_v1` / `rdb_v1` (ITL-535 #232) | `SchemaVersionError` the first time a node touches a `0.1.x` table | Result tables: yes, after `orcapod migrate result-db` or `migrate_result_v0_to_v1`. In the check, the migrated result tables served every row and no function ran again. |
| Pipeline-DB record IDs (ITL-533 #236) | Every `record_id` differs | Pipeline tables: no. Their migration fails on a `0.1.4` store (ITL-863), so set `ignore_schema=("v0",)` and let the node refill its pipeline table. With migrated result tables this re-runs no function, although the warning says all results will be recomputed. |
| Type-object hashes of `Path`, `UUID`, `DataFrame`, `list[Path]` and `set[UUID]` (ITL-638 #262) | A source with a column of such a type has a new schema hash, `pipeline_hash` and `content_hash`, and new system-tag column names (`_tag_record_id::<pipeline hash>`). Record-ID values and row data hashes are unchanged | Pipeline tables of function nodes downstream of such a source: no. The node's table path changes, so its `0.1.4` table is not seen at all: no `SchemaVersionError`, and `migrate_node` finds 0 rows. Result tables: yes, after migration, unless the function itself is annotated with one of these types (its signature hash changes; see below) |
| Stored Arrow type of `list[Path]` / `set[UUID]` (ITL-173 #251) | These columns are logical list types (`extension<list[orcapod.path]>`) | Nothing to find: `0.1.4` rejected such columns as not yet supported |
| `Batch` system tags folded to scalars (NPIPE-204 #250) | `Batch` output system-tag columns are scalars instead of lists, and their names gain a further `::<pipeline hash>` | Nothing to find for a function downstream of `Batch`: in `0.1.4` such a node failed inside a `PipelineJob` with `ArrowTypeError`. An operator-node table has no migration (see *Known limitations*) |

#### Stored hash columns are now `large_binary`; pipeline and result databases are versioned (ITL-535 #232, ITL-539 #241)

Every orcapod-produced hash column in the pipeline database (pdb) and result database
(rdb) is now stored as `large_binary` (the prefixed digest) instead of `large_string`:
`__node_content_hash`, `__input_data_hash`, `__output_data_hash`, the function variation
hashes `function_signature_hash` / `function_content_hash`, the operator node's
`__node_content_hash`, and the side-effect invocation log's `record_id_hash`.

Storage paths now carry a schema version: pipeline tables live under
`<node identity path>/pdb_v1`, result tables under `<pod uri>/rdb_v1`, and side-effect
tracking tables under a `tdb_v1` suffix. A table written by `0.1.x` (no suffix) is a
"v0" table.

**What you will see.** The first time a function node or result cache touches a v0 table
it raises `orcapod.errors.SchemaVersionError`. Nothing is read or overwritten.

**Migration — choose one per node:**

1. *Migrate in place* (keeps cached results). For Delta Lake databases, from the shell:

   ```bash
   orcapod migrate result-db   <result_db_path> <record_path> [<record_path> ...]
   orcapod migrate pipeline-db <pipeline_db_path> <result_db_path> <node_path> [<node_path> ...]
   ```

   Both accept `--dry-run` (count without writing) and `--json-summary`. For any other
   database backend, or from Python:

   ```python
   from orcapod.migrations import migrate_node, migrate_result_v0_to_v1

   migrate_result_v0_to_v1(result_db, result_path)   # the pod's result table
   migrate_node(function_job_node)                    # the node's pipeline table
   ```

   Migration copies rows to the versioned path and leaves the v0 table untouched; it is
   idempotent, so it can be re-run.

   **Known issue (ITL-863):** migrating the pipeline table of a function node written by a
   released `0.1.x` (reported on `0.1.4`) fails with `ArrowInvalid: Could not convert b'…'
   with type bytes: was not a utf8 string`, because the migration casts the binary
   system-tag record-ID column to a string. Migrating the result table succeeds. Until
   ITL-863 is fixed, use option 2 for such a node.

2. *Ignore and recompute.* Opt a node out of the check and let it recompute from scratch:

   ```python
   from orcapod.types import NodeConfig
   node.node_config = node.node_config.merge(NodeConfig(ignore_schema=("v0",)))
   ```

   `merge` keeps the node's other settings; assigning a bare `NodeConfig(...)` would reset
   `is_result_ephemeral` to its default.

**Not covered by the migration tool:** tables written by an *operator* node
(`OperatorJobNode`). Those have no version check and no migration; an existing table
fails with an Arrow schema mismatch on the next write and must be dropped and recreated.
See *Known limitations*.

`ArrowDatabaseProtocol` gains a required `table_exists(path) -> bool` method. Custom
database backends must implement it.

#### `record_id` no longer includes the node content hash (ITL-533 #236)

The preimage of a pipeline record's `record_id` is now the system tag columns plus
`__input_data_hash` only. `__node_content_hash` is no longer part of it (it is still
stored on each row, and still used to isolate nodes that share a table).

**Effect:** every `record_id` and `__pipeline_base_entry_id` value differs from the one
`0.1.x` computed for the same input. `orcapod migrate pipeline-db` /
`migrate_pipeline_v0_to_v1` recompute both, so a migrated database is consistent. Any
`record_id` you stored *outside* orcapod (logs, external tables, filenames) no longer
matches.

#### Function-pod signature hashes changed for orcapod logical types (ITL-638 #262)

A function pod's signature hash used to embed the full import path of an annotated
orcapod type (`orcapod.logical_types.file_type.File`). It now uses the type's stable
registered name (`orcapod.file`), so moving a module inside orcapod can no longer
invalidate caches.

The same change moved the semantic hash of the type objects `Path`, `UUID`, `DataFrame`,
`list[Path]` and `set[UUID]` (commit `7dbd0b66`). A `Schema` that contains one of them
hashes differently, so a source with such a column has a new `pipeline_hash`,
`content_hash` and system-tag column names; see the table above for what that does to
stored tables.

**Effect — one-time cache invalidation:** any function pod whose parameters or return
annotation mention `op.File`, `op.Directory`, `op.Path` or `op.UUID` (directly or inside
`list[...]`, `X | None`, etc.) gets a new signature hash. Its previously cached results
are not found and are recomputed on the next run. Migrating the database does not avoid
this. Functions annotated only with builtins and user-defined types are unaffected — their
hashes are byte-identical (enforced by golden-value tests).

#### `Schema` objects are hashed by `SchemaHandler` (ITL-639 #263)

`Schema` values now reach their dedicated handler instead of being hashed as a plain
mapping. **No hash value changes** — the eight golden schema hashes are byte-identical.
The one behavioural difference: hashing a `Schema` that contains a Python type with no
Arrow translation now raises a descriptive error at hash time instead of producing a
hash for a schema that could never be stored.

#### `op.Directory(ignore=...)` matches relative POSIX paths (ITL-603 #247)

Glob patterns are matched against the path relative to the directory root with
`PurePosixPath.match()`, not against the bare entry name. Bare patterns such as `"*.pyc"`
behave as before. Patterns containing `/` (for example `"build/*.o"`), which previously
never matched anything, now exclude files — which changes the directory's content hash.
A callable passed as `ignore` now receives a relative `PurePosixPath` rather than an
absolute `UPath`; callables that only read `.name` are unaffected.

#### `SideEffectFunctionPod` removed; context injection moved into `FunctionPod` (ITL-532 #231)

Only relevant if you tracked `main` between releases — none of this was in a published
version. The `orcapod.core.side_effect_function` module, `SideEffectFunctionNodeProtocol`,
`SideEffectFunctionInvocation` and `record_side_effect_function_pod_invocation` are gone.
Use `@function_pod(ctx_arg=...)` / `FunctionPod.from_fn(..., ctx_arg_name=...)`.
`@side_effect_function_pod` remains as a thin alias.

#### Operator outputs keep their inputs' Arrow types, normalized to large variants (ITL-813, ITL-832 #278)

`MergeJoin` no longer infers Arrow types from Python values, and it no longer fails on a
zero-row input: a result with a zero-row input has the same column types as a non-empty
one. Every output column of `MergeJoin` and `PolarsFilter` takes the large-normalized form
of its input Arrow type: `string` becomes `large_string`, `binary` becomes `large_binary`
and `list` becomes `large_list`, recursively. Width, nullability and logical types are
never changed. List columns built by `MergeJoin` (merged columns), `Batch` and `GroupBy`
are `large_list` of the normalized element, or, for a logical element such as `Path`, that
element's logical list type (`list[orcapod.path]`). Merged source-info columns are
`large_list<large_string>`.

| Column | 0.1.x | 0.2.0 |
|---|---|---|
| `MergeJoin` merged column, `int8` input | `list<int64>` | `large_list<int8>` |
| `MergeJoin` merged column, `float32` input | `list<double>` | `large_list<float>` |
| `MergeJoin` merged column, `timestamp[ms]` input | `list<timestamp[us]>` | `large_list<timestamp[ms]>` |
| `MergeJoin` merged column, `decimal128(5, 2)` input | precision taken from the data (e.g. `decimal128(3, 2)`) | `large_list<decimal128(5, 2)>` |
| `MergeJoin` merged column, `string` input | `list<string>` | `large_list<large_string>` |
| `MergeJoin` result with a zero-row input | raised `ValueError` | same types as a non-empty result |
| `Batch` / `GroupBy` list column, `int8` input | `list<int8>` | `large_list<int8>` |
| `MergeJoin` / `PolarsFilter` system-tag record ID | `large_binary` | `fixed_size_binary[16]`, as the input has |

**Inputs that disagree:** when the two inputs of `MergeJoin` give a colliding data column
or a shared tag column different Arrow types, both are normalized first. If they are then
the same (for example `string` and `large_string`), the join proceeds. Otherwise (for
example `int8` and `int64`, two timestamp units, or two decimal precisions) it raises
`InputValidationError` naming the column and both types; cast one input explicitly. This
used to produce a type inferred from the values, or a Polars `SchemaError`. The check runs
when the join executes: `output_schema()` reports Python types, so such a disagreement is
not visible when the graph is built.

**Hashes:** large and non-large variants hash identically, so normalization and `list` →
`large_list` change no hash. The change is confined to `MergeJoin` merged columns whose
inputs used to be re-inferred to a different type: integers and floats narrower than
`int64` / `double` (which used to widen), timestamps with a unit other than microseconds
(which became `timestamp[us]`), and decimals (whose precision came from the data). The data
content hash of each row containing such a merged column changes, and so does the stored
record of any operator node or function node that consumes it; a function pod downstream
misses its cache once and recomputes. Tag hashes, stream `content_hash` and `pipeline_hash`
are unchanged.

**Stored schemas:** an existing table written with `list<...>` aggregates (from `Batch`,
`GroupBy` or `MergeJoin`) will receive `large_list<...>` batches. Appending them is not yet
tested for every database backend; see *Known limitations* (ITL-851).

**Nested nullability:** `normalize_to_large_types` now keeps every nested field's
nullability and metadata. A column such as `list<item: int64 not null>` keeps its non-null
item flag through `Data`, operators and storage instead of losing it. `Data`, `Tag`, stream
and source hashes are unchanged; only the stored Arrow schema of such columns changes.

**Struct Python types:** `struct<b: string>` and `struct<b: large_string>` now map to one
synthesized Python class. Hashes are unchanged.

**Logical list elements:** an aggregating operator (`Batch`, `GroupBy`, `MergeJoin`) over a
column whose extension type has no logical list type in the type converter (for example
PyArrow's canonical `arrow.uuid`) raises a `ValueError` naming the column. It used to fail
with `ArrowNotImplementedError: extension`.

#### `Index` and `Pick` keep their input Arrow types (ITL-834 #279)

`Index` and `Pick` rebuilt every data column from Python values, so columns they only pass
through widened (`int8` → `int64`, `float32` → `double`, `list<int8>` →
`large_list<int64>`). They now keep the input Arrow types of every tag and data column, and
the extracted column takes the element's type (`Index` on `list<int8>` gives `int8`; `Pick`
gives the dict's value type). Outputs are large-normalized as above. An in-place `Index` /
`Pick` keeps the column in its position, and a result in which every row was skipped has
the input's Arrow schema. Extracting a `None` element where the predicted output type is
not optional raises `ValueError` naming the operator, the column and the row's tag.

**Hashes:** rows that pass through `Index` or `Pick` with columns narrower than `int64` /
`double` (or lists and structs of such types) get different data content hashes than on
`0.1.x`, and so does every function pod downstream of them: such pipelines miss their
cache once and recompute. Inputs that are already `int64`, `double` or strings give the
same types and hashes as before. Stream `content_hash` and `pipeline_hash` are unchanged.

The underlying datagram fixes:

- `Datagram.with_columns` and `update` keep the Arrow table whenever it is loaded: existing
  columns keep their exact Arrow fields, and `update` keeps each updated column's field.
  `with_columns` (and `Data.with_columns` / `DatagramProtocol.with_columns`) accepts a
  `pa.DataType` or a `pa.Field` in `column_types`; Python types behave as before.
- Meta (`__`-prefixed) columns keep their Arrow types through every mutator.
- `arrow_schema()` is no longer stale after a mutation of a datagram built from a dict.
  `FunctionPodStream.as_table()` could drop such a row's added tag column silently.
- `Index` and `Pick` use the input stream's data context to type the extracted column.

#### `Join` and `MergeJoin` column order follows argument symmetry (ITL-817 #272)

The order of tag and data columns in a `Join` or `MergeJoin` output now follows the
operator's declared argument symmetry. Both are commutative, so they take their inputs in
canonical `pipeline_hash` order and list columns by first appearance across them:
`Join()(a, b)` and `Join()(b, a)` give identical `keys()`, `output_schema()`, `as_table()`
columns and per-row `Tag` / `Data` keys, in every process. Ordered operators such as
`SemiJoin` keep operand order. Code that reads these columns by position may see a
different order than on `0.1.x`; address columns by name. No hash, system tag, record ID
or stored value changes, because hashing does not depend on column order.

#### `Join` gives the same rows on its async path as on its sync path (ITL-837 #280)

`Join`'s async path (`async_execute`, used by async pipelines) rebuilt every merged row from
Python values, so each column's Arrow type was inferred again: an `int16` tag or an `int8`
data column became `int64`, and a `dict` column could not be built at all. It now builds the
merged row from the input rows' Arrow tables, for two inputs and for three or more. Types,
nullability, column order, system tags and content hashes now equal those of the sync path.

**Hashes:** the `Tag` and `Data` content hashes of rows produced by an async `Join` over
columns narrower than `int64`/`double` change (they now equal the sync values), along with the
stored input hashes of any function node downstream of such a join. Sync hashes, stream content
hashes and pipeline hashes are unchanged.

**Behaviour change:** when a shared tag key has different Arrow types in two `Join` inputs
(for example `int16` and `int32`), `Join` now raises `InputValidationError` naming the column
and both types, on both paths. The sync path used to keep the right-hand input's type silently.
Types that differ only in layout (`string` and `large_string`) still join.

**Also fixed:** `Datagram.copy()` gave a copy of an Arrow-backed datagram an empty Python
`schema()` when the schema had not been computed yet; async `Index` and `Pick` returned such
rows (ITL-859).

#### `Join` no longer carries its inputs' meta columns (ITL-854, ITL-857 #280)

`Join` now drops every input's `Data` meta (`__`-prefixed) columns, on the sync and async
paths, for two inputs or more: for example a cached function pod's `__computed` flag, the
result cache's `__pf_*` and `__pod_ts` columns, and the `__` columns of a raw
`ArrowTableStream`, along with their `_source___*` source-info columns. Previously the sync
path kept them, renaming colliding names with an index suffix (`__computed_1`), while the
async path dropped them. That rename could also give two columns the same name and fail with
a Polars `DuplicateError` (ITL-857). Nothing downstream of a `Join` reads these columns, and
none of them is hashed: content hashes, pipeline hashes, record IDs and stored tables are
unchanged. `Join.output_schema()` no longer lists the `__` columns a raw `ArrowTableStream`
reports as data, matching the joined table. Two inputs that share only a meta column name now
join instead of failing validation.

#### Materialized rows keep their own source info (ITL-845 #282)

When rows were collected and turned back into a stream, every row got the source-info
(`_source_*`) tokens of the first row, so rows after the first were attributed to row 0. This
affected the sync orchestrator, which does this between every pair of nodes (so every
function-node tag table written by a sync `PipelineJob` recorded the wrong input provenance),
the async path of every operator that collects its input (`MergeJoin`, `Batch`, `GroupBy`,
`PolarsFilter` and the other barrier operators), sync `Index` and `Pick`, and operator nodes
(`OperatorJobNode.execute` and `async_execute`). After `Batch`, a batch could even list more
tokens than it had members. Each row now keeps its own tokens.

Only `_source_*` values change. Data values, Arrow types and stored schemas, content hashes,
pipeline hashes and function-node record IDs are unchanged. The one stored ID that covers
`_source_*` values is an operator node's per-row `_record_hash`: it changes for every row
whose token was wrong, and stays the same for rows whose token was right.

**Stored data is not repaired.** Rows stored before this fix (function-node tag tables, and
operator-node tables written with `cache_mode=LOG`) keep the wrong source-info token for every
row after the first; nothing else in them is wrong. Orcapod does not rewrite, repair or
recompute them: a re-run serves function-node rows from the cache unchanged, but stores
corrected operator-node rows next to the old ones, so that table then holds both versions of
those rows. To get correct stored values, drop the pipeline tables (the function-node tag
tables, whose paths end in `pdb_v1`, and any operator-node tables) and re-run the job. The
function result tables (under `_result/`, ending in `rdb_v1`) can be kept: the re-run then
serves every result from them and runs no function again. A table's path is the job name
followed by the node's `node_identity_path` (plus `pdb_v1` for a function node); in a
`DeltaTableDatabase` it is that directory under the root, and the SQL connectors name the
table by joining the path components with `__`, each with non-alphanumeric characters
replaced by `_`. Correcting stored values is tracked in ITL-853.

#### Shipped in 0.1.2 but never recorded here

These two breaking changes were released in `0.1.2` without a changelog entry. They are
listed for anyone upgrading from `0.1.1` or earlier.

**`NodeConfig` split into `PodConfig` and `NodeConfig` (ITL-512 #221).**

| Before | After |
|---|---|
| `NodeConfig(max_concurrency=N)` | `PodConfig(max_concurrency=N)` |
| `FunctionPod(pf, node_config=NodeConfig(...))` | `FunctionPod(pf, pod_config=PodConfig(...))` |
| `pod.node_config` | `pod.pod_config` |
| `resolve_concurrency(node_config, pipeline_config)` | `resolve_concurrency(pod_config, pipeline_config)` |
| `NodeConfig.is_result_ephemeral: bool` | `NodeConfig.is_result_ephemeral: bool \| None` |

`is_result_ephemeral` can no longer be set when constructing a `FunctionPod`. Set it on
the node (`node.node_config = node.node_config.merge(NodeConfig(is_result_ephemeral=True))`)
or for a whole job
(`job.apply_node_config(NodeConfig(is_result_ephemeral=True))`).

**SpikeInterface support moved to a separate package (ITL-473 #220).** Core `orcapod` no
longer contains or depends on any SpikeInterface code: `LogicalSIRecording`, the
`BaseSorting`, `Motion` and `SortingAnalyzer` logical types, and the `spikeinterface`
optional dependency are gone. They live in
[`orcapod-extension-spikeinterface`](https://github.com/nauticalab/orcapod-extension-spikeinterface),
which registers itself through the new `op.register_extension()` hook. That package is
not on PyPI at the time of writing; install it from its repository.

### Added

- **`GroupBy` operator** — the first many→one operator. `GroupBy(by=[...])` (also
  `stream.group_by(...)`) collapses rows sharing a tag tuple into one row; group keys stay
  scalar tags and every other column becomes list-valued. Output is deterministic across
  runs and input orderings. (NPIPE-204 #250)
- **`SideEffectPod`** with `track_completion` and `drop_on_failure`, plus the `@sink_pod`,
  `@tap_pod` and `@side_effect_pod` decorators. The user function receives an
  `InvocationContext` on every call. (ITL-525 #228)
- **Context injection for function pods** — `@function_pod(ctx_arg=...)` /
  `FunctionPod.from_fn(..., ctx_arg_name=...)` passes an `InvocationContext` to the
  function. (ITL-532 #231, #234)
- **Post-run hooks** — `FunctionPod.add_post_run_hook()` and
  `@function_pod(post_run_hooks=[...])` fire after every invocation (computed, cache hit,
  or error). New public types: `InvocationStatus`, `RunStats`, `PodContext`,
  `PostRunPayload`, `HookConfig`, `PostRunHook`. (ITL-523 #226)
- `PostRunPayload.invocation_context`; `InvocationContext` and `InvocationHashConfig` are
  exported from `orcapod`. (ITL-531 #237)
- **`EmptyData`** — a payload-free token emitted when an upstream ephemeral result is
  missing, so downstream nodes can still be served from their own cache. New errors
  `EmptyDataAccessError`, `EmptyDataHashMissingError`, `EphemeralResultMissingError`.
  Pipeline rows now persist `__output_data_hash`. (ITL-534 #229)
- **Schema versioning and migration** — `orcapod.migrations` package
  (`migrate_node`, `migrate_pipeline_v0_to_v1`, `migrate_result_v0_to_v1`,
  `MigrationResult`), the `orcapod migrate pipeline-db` / `result-db` CLI commands,
  `SchemaVersionError`, `NodeConfig.ignore_schema`, `ContentHash.from_prefixed_digest()`,
  and `ArrowDatabaseProtocol.table_exists()`. (ITL-535 #232)
- **`ListLogicalType`** — `list[T]` and `set[T]` columns whose element is a logical type
  (`list[uuid.UUID]`, `list[File]`, `list[MyDataclass]`, nested lists) now round-trip
  through Parquet and Delta Lake with the element type preserved. (ITL-173 #251)
- `match_mtime` flag on the file-hash cache backends and
  `enable_file_hash_caching(match_mtime=...)`; with `False`, lookups match on path and
  size only, for filesystems with unreliable mtimes. (ITL-522 #225)
- Hash computation site reference in the documentation (ITL-529 #238) and the
  side-effect pods design spec (ITL-524 #227).

### Changed

- All hash columns stored in pipeline, result and tracking tables are `large_binary`. See
  *Breaking changes and migration*. (ITL-535 #232, ITL-539 #241)
- `record_id` preimage no longer includes `__node_content_hash`. See *Breaking changes
  and migration*. (ITL-533 #236)
- `op.Directory(ignore=...)` matches relative POSIX paths. See *Breaking changes and
  migration*. (ITL-603 #247)
- Release workflow accepts an optional `branch` input (defaults to `main`). (#239)
- CI: the Python 3.11 release test job no longer destroys its own virtualenv; every
  GitHub Action is pinned to a commit SHA and upgraded to its Node 24 major.
  (ITL-618, ITL-619 #254; ITL-621 #256)
- `pygraphviz` removed from the runtime dependencies. It was declared but never
  imported, and it is built from source against the Graphviz C headers, so installing
  `orcapod` failed on systems without `libgraphviz-dev` (or with a Graphviz newer than
  `pygraphviz` supports). Installing `orcapod` no longer needs a C toolchain or Graphviz
  headers. Rendering pipeline graphs still uses the pure-Python `graphviz` package and
  therefore the optional Graphviz `dot` binary at render time. (ITL-134 #267)
- **Python 3.13 is supported.** Canonical type names, and the hashes derived from them,
  are identical on Python 3.11, 3.12 and 3.13, and unchanged on 3.11 and 3.12.
  (ITL-825 #275, merged through #276)
- The repository meets the organization's required CI: `ruff` lint and format,
  `basedpyright` with a baseline, and tests on Python 3.11–3.13. Six tests that had been
  hidden by duplicated test-class names now run. (ITL-826 #276)
- `MergeJoin`, `PolarsFilter`, `Batch` and `GroupBy` keep their inputs' Arrow types,
  normalized to large variants, and `MergeJoin` accepts zero-row inputs. See *Breaking
  changes and migration*. (ITL-813, ITL-832 #278)
- `Index` and `Pick` keep their input Arrow types, and `Datagram.with_columns` accepts Arrow
  types. See *Breaking changes and migration*. (ITL-834 #279)
- `Join` and `MergeJoin` column order follows argument symmetry. See *Breaking changes and
  migration*. (ITL-817 #272)
- `Join` raises `InputValidationError` when a shared tag key has different Arrow types in two
  inputs (layout-only differences such as `string` and `large_string` still join), its async
  path keeps the inputs' Arrow types, and it drops its inputs' meta (`__`) columns on both
  paths. See *Breaking changes and migration*. (ITL-837, ITL-854, ITL-857 #280)
- The license is MIT (`license = "MIT"`, the MIT text in `LICENSE`). (ITL-875 #283)
- **Hash values are pinned by a golden regression suite.** `tests/test_hashing/golden/`
  pins 608 hash values that Orcapod stores, uses as a cache or database key, or embeds in a
  column name or table path: semantic hashes of Python values and type objects, `Schema`
  hashes and stored Arrow types, Arrow table and schema hashes, datagram hashes, and the
  content hashes, pipeline hashes, system-tag column names, record IDs, row data hashes and
  database table paths of pipelines through every operator and a `PipelineJob`. Each value
  has its own test, so a failure names the value that moved, and the values are identical
  on Python 3.11, 3.12 and 3.13. A failing golden entry means a hash changed: it is treated
  as a bug in the change, and the files are regenerated only to add entries or for a hash
  change the developer has approved with a migration assessment, never to make a test pass.
  The values that differ from `0.1.4` are listed in the table at the top of *Breaking changes
  and migration*. (ITL-626 #284)
- Packaging: the `LICENSE` file ships in the wheel and the sdist; `pymongo` and `pyyaml` are removed from
  the runtime dependencies; `basedpyright` and `typing_extensions` move to the `dev` group;
  the sdist contains only the package source, `README.md`, `LICENSE` and the package
  metadata; `orcapod.__version__` is added. (ITL-827 #277)
- Documentation: every Python sample in `README.md`, `docs/index.md`,
  `docs/getting-started.md`, `docs/concepts/` and `docs/reference/` is executed by the test
  suite, and `mkdocs build --strict` runs in the tests. (PLT-4816 #271) A runbook for
  coordinating issue-specific agents is added for contributors (`AGENT_COORDINATION.md`).
  (ITL-818 #266)

### Fixed

- `RayExecutor` no longer fails when it has no remote options: `ray.remote` is applied
  directly instead of being called with an empty option set. This is the fix released as
  `0.1.3` / `0.1.4`. (#233, ITL-544 #235)
- Function-pod signature hashes no longer depend on orcapod's internal module layout.
  (ITL-638 #262)
- `Schema` hashing is routed through `SchemaHandler`; the handler was unreachable and was
  missing from the `v0.1` context spec. (ITL-639 #263)
- Empty Arrow tables keep the declared field nullability. Previously a function that
  failed under `error_policy="continue"` produced an all-nullable empty table, and a
  downstream `Join` aborted the whole pipeline. (ITL-563 #242)
- Extension-typed columns can be read back after Delta Lake compaction, which rewrites
  `large_string` storage as `string`. (ITL-602 #246)
- List-backed logical types (`list[File]`, `set[File]`, …): the Polars round-trip in
  `Join` / `MergeJoin` no longer raises, content hashing hashes file contents rather than
  path strings, and `MergeJoin` keeps the extension type on merged columns. (ITL-627 #257)
- `Data` can represent list-valued source-info provenance; `MergeJoin` inside a pipeline
  job no longer fails with `ArrowTypeError`. `Batch` and `GroupBy` can aggregate
  logical-typed columns such as `Path`. (NPIPE-204 #250)
- `PollingSource`:
  - runs its full asynchronous polling loop inside a `PipelineJob` instead of a single
    synchronous snapshot (ITL-615 #255);
  - no longer leaks a synthetic `_content_hash` column into its data schema, which raised
    `SchemaInconsistencyError` on the third fetch (ITL-616 #259);
  - no longer loses rows when `iter_data()` / `as_table()` are called while the async loop
    is running (ITL-617 #260);
  - no longer crashes on a zero-row batch; the Arrow schema is established once, from the
    declared schema or the first non-empty batch (ENG-952 #261).
- `ArrowTableStream.iter_data()` no longer drops rows or pairs a tag with another row's
  data when the tag and data columns have different chunk layouts (which result-cache
  hits could produce), and rows after the first record batch get their own source info.
  Public API, schemas, hashes and record ids are unchanged. (ITL-816 #270)
- `MergeJoin` no longer raises on a zero-row input. (ITL-813 #278)
- A function node whose output mixed cached and freshly computed rows could fail in the
  orchestrator with `ArrowInvalid: Schema at index N was different`, because cached rows
  came back with a different column order. Buffer materialization now aligns rows by
  column name. (ITL-817 #272)
- Rows collected and turned back into a stream (the sync orchestrator between nodes, the
  async path of barrier operators, sync `Index` / `Pick`, operator nodes) keep their own
  source-info tokens instead of all taking row 0's. Stored rows are not repaired; see
  *Breaking changes and migration*. (ITL-845 #282)
- `GroupBy` or `Batch` after `MergeJoin`, `Batch` or `GroupBy` no longer raises
  `ArrowTypeError: Expected bytes, got a 'list' object` when its rows are materialized. A
  list column becomes a list of its incoming list type (`large_list<large_list<int8>>`,
  element types unchanged, `Path` as `list[list[Path]]`), and its source info is a list of
  the members' source info. Hashes of pipelines that already worked are unchanged.
  (ITL-849 #281)
- `Join`'s async path no longer infers Arrow types from Python values (`int16` / `int8`
  became `int64`, and `dict` columns failed); a `Join` of three or more inputs no longer
  fails with a Polars `DuplicateError` when two inputs share a meta column name. See
  *Breaking changes and migration*. (ITL-837, ITL-857 #280)
- `Datagram.copy()` of an Arrow-backed datagram whose schema had not been computed yet no
  longer reports an empty Python `schema()`; async `Index` and `Pick` returned such rows.
  (ITL-859 #280)

### Known limitations

- **Migrating a function node's pipeline table written by a released `0.1.x` fails.**
  `orcapod migrate pipeline-db` and `migrate_pipeline_v0_to_v1` raise `ArrowInvalid` on a
  tag table written by `0.1.4`, because the binary system-tag record-ID column is cast to a
  string. Workaround: set `ignore_schema=("v0",)` on the node (the `merge` form under
  *Breaking changes and migration*) and let it recompute. (ITL-863)
- **Operator-node tables have no migration path.** `OperatorJobNode` stores
  `__node_content_hash` as `large_binary` but has neither a schema-version check nor a
  migration utility. An operator-node table written by `0.1.x` fails with an Arrow schema
  mismatch on the next write; drop and recreate it. (ITL-539; `DESIGN_ISSUES.md` ON1)
- **The migration CLI only opens Delta Lake databases.** Other backends must use the
  Python API in `orcapod.migrations`.
- **Migration does not rescue caches invalidated by ITL-638.** Pods annotated with
  orcapod logical types recompute once regardless.
- **Some hash values are not pinned by the golden suite.** Values that are new on every run
  by design (function result record IDs and the per-row token in a function output's source
  info, both UUIDv7, and `__pod_ts`) are left out, and so are the types the Arrow hasher
  cannot hash yet (ITL-848). (ITL-626 #284)
- **`dict[str, list[T]]` with a logical-type `T`** (a list nested inside a dict value)
  does not round-trip; two tests are marked expected-failure. (PLT-1732)
- **`set[T]` round-trips only for logical-type elements**, not primitives such as
  `set[int]`. (ITL-611, open)
- **Rendering a pipeline graph needs the Graphviz `dot` executable** on `PATH`. Everything
  else works without Graphviz.
- **Delta Lake on S3 is not covered by automated tests.** The S3 integration tests do not
  run in CI; they skip in every environment because the MinIO image they start no longer
  exists. (ITL-819)
- **Arrow types are not part of `Schema`, and the rule for inputs whose Arrow types disagree
  is not settled for every operator.** `output_schema()` reports Python types, so a
  disagreement such as `int8` vs `int64` or `string` vs `large_string` is not visible when
  the graph is built. `MergeJoin` (colliding data columns and shared tags) and `Join`
  (shared tag keys, sync and async) normalize both inputs and raise `InputValidationError`
  if they still differ (see *Breaking changes and migration*). `SemiJoin` does not apply that
  rule yet: disagreeing key types fail with Arrow's own `ArrowInvalid`. (ITL-835)
- **Large-variant normalization is not applied everywhere.** `MergeJoin`, `PolarsFilter`,
  `Index`, `Pick`, `Batch` and `GroupBy` emit large variants; `Join`, `SemiJoin`, `MapTags`,
  `MapData`, the column-selection operators and sources keep whatever layout their input
  has (`string` stays `string`). This is layout only and changes no hash. (ITL-841)
- **`MergeJoin` cannot merge non-empty struct columns**: a colliding data column holding
  struct values raises `TypeError`. Zero-row inputs work. (ITL-839)
- **`SemiJoin` fails when an input has a list or struct data column**, even one it only
  passes through: the join raises `ArrowInvalid: Data type list<...> is not supported in
  join non-key field`. (ITL-847)
- **Appending v0.2-layout rows to a table written before v0.2 is untested.** `list<...>`
  aggregates that are now `large_list<...>`, nested non-null flags that are now kept, and
  narrow columns that no longer widen have not been tested against each database backend
  when appended to an existing table. (ITL-851)
- **Stored source-info tokens written before the ITL-845 fix stay wrong.** Function-node tag
  tables and operator-node `LOG` tables keep row 0's `_source_*` token on every later row
  until those pipeline tables are dropped and refilled; result tables can be kept. Orcapod
  has no repair, and no policy yet for stored data that a fix leaves wrong. (ITL-853)
- **Meta (`__`) columns have no written rule.** Which meta columns each element writes and
  exposes, and what operators do with them, is not specified. `Join` now drops them on both
  paths, but other operators do not yet give the same meta columns on their sync and async
  paths: the async path of `MergeJoin`, `Batch`, `GroupBy` and `PolarsFilter` drops them,
  and the sync path of `SelectDataColumns`, `Index` and `Pick` drops some the async path
  keeps (expected-failure tests, ITL-846). A raw `ArrowTableStream` over a table with `__`
  columns treats them as data. (ITL-867)
- **The async path does not handle zero-row inputs like the sync path.** An async barrier
  operator (for example `PolarsFilter`) whose input yields no rows raises `ValueError:
  Cannot materialize an empty list of rows into a stream`, where the sync path returns an
  empty stream
  (ITL-865). An async `Join` whose input yields no rows cannot check its shared-key types,
  so disagreeing types give an empty result instead of the sync path's
  `InputValidationError` (ITL-856).
- **The Arrow hasher cannot hash some types**: `null`-typed columns, canonical PyArrow
  extension types such as `arrow.uuid`, and map and fixed-size-list columns raise
  `NotImplementedError`. (ITL-848)
- **The `SchemaVersionError` hint suggests `node.node_config = NodeConfig(ignore_schema=("v0",))`**,
  which resets the node's other settings. Use the `merge` form shown under *Breaking
  changes and migration*. (ITL-852)
- **`Index` and `Pick` validation carries masked type errors**, and `Pick`'s branch for
  logical-type columns cannot run, because no logical type implements `pick_field` yet.
  (ITL-850)
- **`Data.select` and `Data.drop` leave the removed column's source-info column**
  (`_source_<column>`) behind, visible with `all_info=True`. (ITL-838)
- **Arrow schema and data preservation is not yet audited across every operation**, sync and
  async. Each operator fixed in this release was checked by hand; a test harness covering
  every operator, pod, source, node and execution path is deferred (ITL-846), including its
  nullability part (ITL-842).
- **There is no written rule for which data context an element uses.** Operators use their
  input stream's context for data, but an operator's own context still decides its
  `content_hash` and `pipeline_hash`, and `MergeJoin` uses its left input's context without
  checking the right one. (ITL-843, ITL-844)
- **Column order cannot be chosen.** `Join` and `MergeJoin` list columns in their canonical
  order, which may differ from the order of the call; address columns by name. (ITL-840)
- **A source's default record ID is positional.** Without `record_id_column`, a row's
  record ID comes from its `source_id` and its position. A row that moves to another
  position under the same `source_id` is treated as a new entry: a function node records it
  again (same tag values, a different record ID) and a `sink_pod` delivers it again. A
  function's stored result for that data is reused, not recomputed, and no result is
  paired with the wrong row. When row order can vary between runs, pass a stable
  `source_id` and set `record_id_column` to a column unique per row. (ITL-817,
  `DESIGN_ISSUES.md` RS2)
- **CI tests only the locked dependency versions.** Users get the newest versions the
  `pyproject.toml` ranges allow, which CI does not exercise (ITL-823), and the policy that
  every install uses `uv.lock` is not yet enforced everywhere (ITL-822).
- **The SpiralDB backend is not verified by this release.** Its integration tests have
  been failing on `main` since at least 2026-08-26 with `403 Permission table:view
  denied` from the SpiralDB test project — a credentials problem on the test project, not
  a code change — so the `spiraldb` extra ships untested against a live service.
- **Tested on Python 3.11, 3.12 and 3.13 only.** `requires-python` is `>=3.11` with no
  upper bound; newer interpreters are not exercised in CI.
- **`ray` is pinned to `2.48.0`** in the `ray` extra.
- The SpikeInterface extension is not published on PyPI.

## [0.1.4] — 2026-07-17

Patch release cut from a maintenance branch, not from `main`.

### Fixed

- `RayExecutor`: build the Ray remote function inside the cache-miss branch so a new
  `ray.remote` decorator is not created on every call.

## [0.1.3] — 2026-07-17

Patch release cut from a maintenance branch, not from `main`.

### Fixed

- `RayExecutor`: `ray.remote` cannot be pre-configured with an empty option set; apply it
  directly when there are no remote options.

## [0.1.2] — 2026-07-10

Not itemised at the time. Its two breaking changes — the `NodeConfig` → `PodConfig` /
`NodeConfig` split (ITL-512) and the move of SpikeInterface support to a separate package
(ITL-473) — are described under *Breaking changes and migration* in `0.2.0`. For the full
list of changes see the
[v0.1.1...v0.1.2 comparison](https://github.com/metamorphic-labs/orcapod-python/compare/v0.1.1...v0.1.2).

## [0.1.1] — 2026-06-29

### New

#### `orcapod.File` — content-identified file type (ITL-450)

`orcapod.File` is a new first-class type for materialized files. It subclasses
`upath.extensions.ProxyUPath`, giving it full UPath-like behaviour across all
backends (local, S3, GCS, etc.) without polluting the global protocol registry.

**Construction validates eagerly:** `File(path)` raises `FileNotFoundError` if the path
does not exist, `IsADirectoryError` if it is a directory, and `ValueError` if it is not
a regular file. This means a `File` instance always refers to a file that existed at
construction time.

**Symlink handling:** By default (`follow_symlinks=True`) symlinks are followed. Pass
`follow_symlinks=False` to raise `ValueError` instead of following a symlink.

**Content hashing:** `orcapod.File` is the only type whose hash is derived from the
file's *content*. `pathlib.Path` and `upath.UPath` now hash from the path string only
(see Breaking Changes below).

**Arrow extension type:** `LogicalFile` stores `File` values as `large_string` (the path
string) with extension name `"orcapod.file"`. Re-reading a stored `File` column
re-validates existence, so missing files surface at read time.

```python
from orcapod import File

f = File("/data/results.csv")        # validates existence
f2 = File("s3://my-bucket/out.parquet")  # works with any UPath backend
```

**Exports:**
- `orcapod.File` — the `File` class (top-level package)
- `orcapod.extension_types.LogicalFile` — the Arrow extension type
- `orcapod.hashing.semantic_hashing.FileHandler` — the semantic hasher

### Changed

#### starfix v0.3.0 adoption + schema-metadata cleaner (PLT-1737)

Bumped `starfix` dependency to `~=0.3.0` and introduced a schema-metadata
cleaning step before all Arrow schema and table hashes.

**What changed:** `StarfixArrowHasher.hash_schema` and `hash_table` now strip
every metadata key that does not start with `ARROW:extension:` before passing
the schema to starfix. Only identity-bearing extension metadata (e.g.
`ARROW:extension:name`) is included in the hash. Unrelated keys (comments,
vendor annotations, source-file provenance, etc.) are ignored.

**Stability invariant:** Schemas with no `ARROW:extension:*` metadata anywhere
continue to produce byte-for-byte identical hashes to pre-v0.3.0 Orcapod.

**One-time hash invalidation:** Pipelines whose schemas contained
`ARROW:extension:*` keys *alongside* unrelated field metadata (e.g. a
`comment` key on the same field as `ARROW:extension:name`) will see a changed
hash. On-disk caches for those pipelines should be treated as stale and
recomputed.

**New utility:** `orcapod.hashing.schema_cleaner.clean_schema_for_hashing` and
`has_extension_metadata` are available as semi-public utilities for other
Orcapod code that needs to inspect or clean Arrow schema metadata.

### Breaking Changes

#### `pathlib.Path` and `upath.UPath` no longer hash file content (ITL-450)

`pathlib.Path` and `upath.UPath` values now hash from the **path string** only —
no file is read. This matches the semantics of all other string-like types in orcapod.

**Migration:** Replace any `Path`-typed pipeline column that requires content-based
identity with `orcapod.File`. `File` validates existence at construction and hashes
from file content, exactly as `Path` did before this change.

```python
# Before (Path hashed file content — now hashes path string instead)
{"input": Path("/data/results.csv")}

# After (File hashes file content, as Path previously did)
from orcapod import File
{"input": File("/data/results.csv")}
```

**Hash invalidation:** Any on-disk cache entry produced with a `Path`-typed data column
will no longer match. Treat those entries as stale and allow recomputation.

**Removed:** `PathHandler` and `UPathHandler` have been removed from
`orcapod.hashing.semantic_hashing`. Use `FileHandler` for content-based hashing.

#### `packets` → `data` rename (hard break)

All identifiers containing `packet`/`packets`/`Packet` have been renamed to
`data`/`Data`. No deprecation aliases. Pre-v0.1 artifacts will not load.

| Old name | New name |
|---|---|
| `Packet` | `Data` |
| `PacketProtocol` | `DataProtocol` |
| `PacketFunction` | `DataFunction` |
| `PacketFunctionBase` | `DataFunctionBase` |
| `PacketFunctionProtocol` | `DataFunctionProtocol` |
| `PacketFunctionProxy` | `DataFunctionProxy` |
| `PythonPacketFunction` | `PythonDataFunction` |
| `CachedPacketFunction` | `CachedDataFunction` |
| `PacketFunctionWrapper` | `DataFunctionWrapper` |
| `PacketFunctionExecutorProtocol` | `DataFunctionExecutorProtocol` |
| `PacketExecutionLoggerProtocol` | `DataExecutionLoggerProtocol` |
| `PacketLogger` | `DataLogger` |
| `SelectPacketColumns` | `SelectDataColumns` |
| `DropPacketColumns` | `DropDataColumns` |
| `MapPackets` | `MapData` |
| `iter_packets()` | `iter_data()` |
| `process_packet()` | `process_data()` |
| `async_process_packet()` | `async_process_data()` |
| `execute_packet()` | `execute_data()` |
| `map_packets()` | `map_data()` |
| `select_packet_columns()` | `select_data_columns()` |
| `drop_packet_columns()` | `drop_data_columns()` |
| `on_packet_start()` | `on_data_start()` |
| `on_packet_end()` | `on_data_end()` |
| `on_packet_crash()` | `on_data_crash()` |
| `INPUT_PACKET_HASH_COL` | `INPUT_DATA_HASH_COL` |
| `PACKET_RECORD_ID` | `DATA_RECORD_ID` |
