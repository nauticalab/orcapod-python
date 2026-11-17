# Changelog

## [0.3.0] — 2026-11-17

### Breaking changes and migration

#### Upgrading from 0.2.0

`0.3.0` is the first release on PyPI since `0.1.4`; `0.2.0` was a tag only. This section lists
every change a `0.2.0` user meets, grouped, each with what to do. The entries below it give
the details.

**Coming from `0.1.4`** (the previous PyPI release): also read the API changes in the
`## [0.2.0]` section, but skip its database migration steps. `orcapod migrate`,
`migrate_node` and `ignore_schema` are removed in 0.3, and a table without a layout
component (as `0.1.4` wrote) is counted and reported like any other old table, never read
(ITL-944). A `0.1.4` database is therefore treated exactly like a `0.2.0` one, as described
below.

**Stored data: nothing written by 0.2.0 is reused.**

- Every hash moved once in v0.3 (hash labels, ITL-945; starfix hash format 2, ITL-910;
  logical type identifiers, ITL-935; user classes, ITL-972; NumPy arrays, ITL-981; pandas
  values, ITL-897; the v1 hash definition, ITL-1154), and every table has a new layout and a
  `__data_context` column (ITL-944).
  The tables `0.2.0` wrote are not read as results and are never modified. Given the same
  database, the first v0.3 run computes every input again and writes new tables beside the
  old ones, at new paths. Each node reports the old tables it finds, by name (ITL-954). To do: nothing, beyond accepting one full recomputation; keep `0.2.0`
  to read the old tables. A migration tool is planned after v0.3 (ITL-913, ITL-914, ITL-1048).
- A database written by a development build of v0.3 (installed from git before `0.3.0`) is
  discarded, not migrated: every v0.3 hash was defined anew before the release, and the
  record-id column is now text (ITL-1154). Its results are computed again. **A side-effect
  node run by such a build delivers every input again**, and so does one given a new
  database. To do: run such a node only where a repeated delivery is acceptable.
- Side-effecting nodes (`@side_effect_pod`, `@sink_pod`, `@tap_pod`,
  `@function_pod(ctx_arg=...)`, `@side_effect_function_pod`) that find `0.2.0` tables raise
  `SideEffectLogDataContextError` before delivering anything (ITL-944, ITL-954). Keep `0.2.0`
  for such a node; a fresh database performs every side effect again.
- A job file saved by `0.2.0` with a database names it under `bindings.store`, and
  `PipelineJob.load(path)` raises `ValueError` for it: pass `database=` to `load`, or rename
  the key to `bindings.database` (ITL-1082). Once a `0.2.0` job or pipeline file loads, its
  nodes are under the current data context, with a warning, and show no rows until the job
  runs (ITL-944, ITL-945). A saved file
  whose schema text names a logical type by name only is rejected: save a new job from the
  original pipeline (ITL-904). `0.2.0` cannot load a file saved by `0.3.0` (ITL-1026).
- `std:v0.1:default` no longer resolves (`ContextResolutionError`), and a full data context
  key must match its manifest (ITL-944).
- Removed with no replacement: `SchemaVersionError`, `NodeConfig(ignore_schema=...)`,
  `orcapod.migrations` and the `orcapod migrate` command (ITL-944). Remove `ignore_schema`
  from your code.
- `CachedSource(cache_path=...)` stores under `cache_path + ("sdb_v1",)`; pass the path
  without the layout component (ITL-944).
- From `0.3.0` on, released hash values are protected by CI: a golden hash file a release
  listed never changes, and new golden values go into new files, in any release. A change to
  a released golden value, a logical type's own identifier change included, needs a new data
  context version, shipped only in a minor (before 1.0) or major release (ITL-946; see
  `docs/reference/golden-hashes.md`). To do: nothing; hashes written by `0.3.0` stay valid
  for every release of the `std:v1:default` data context.

**Requirements.**

- `pyarrow>=22.0.0` (was 20.0.0) and `pandas<3` (ITL-897); `starfix>=0.5.0,<0.6` (ITL-910);
  the `spiraldb` extra needs `pyspiral>=0.16.33` (ITL-926).

**Renamed or removed names (no aliases).**

- `store` is `database` everywhere: `PipelineJob(database=...)`, `from_pipeline`, `bind`,
  `load` and `job.database` (ITL-1082); `set_ephemeral_store` is `set_ephemeral_database`;
  `StoreError`, `StoreReadError`, `StoreWriteError` and `StoreContentWarning` are
  `OrcapodDatabaseError`, `OrcapodDatabaseReadError`, `OrcapodDatabaseWriteError` and
  `OrcapodDatabaseContentWarning`; `orcapod.utils.store_errors` is
  `orcapod.utils.database_errors`; `describe_store_table` is `describe_database_table`
  (ITL-1087). A job file with `bindings.store` raises `ValueError` on load: pass
  `database=` or rename the key (ITL-1082). `PipelineJob.set_ephemeral_database(...)` now
  works on a job with a side-effect node, which it skips (it raised `AttributeError`;
  ITL-1147).
- What `head()` returns is `StreamView` (was `StreamHead`), with `ViewStatus` (was
  `HeadStatus`) and `StreamViewProtocol`, in `orcapod.core.streams.view` (ITL-1081). The old
  `StreamBase.view()` is removed: use `head(n)` or `as_polars_df()` (ITL-991).
- `resolve_field` and `FieldNotResolvableError` are removed; read values through the source's
  stream (ITL-1078).
- Hash labels are `<algorithm>_v1`: `semantic_v1`, `arrow_v1`, `directory_v1` (ITL-945). A
  hash inside a name (function `uri[1]`, `schema:`, `node:` path components) is the bare
  digest: compare with `to_hex()`, not `to_string()` (ITL-945).

**Running a job.**

- `PipelineJob.run()` returns a `RunSummary`, not the job: write `job.run()`, not
  `job = job.run()` (ITL-997).
- A job node shows the current run only. Before a run in this process it is empty; read what
  is stored with `node.as_source()` (or `get_all_records()`), or run the job (ITL-1095).
- `run()` is state-driven: a node that holds a current run is not run again, so running an
  unchanged job a second time computes, reads and delivers nothing. A source whose data
  changes in memory needs a new source object, bound with `job.bind(sources=...)`; call
  `clear_cache()` on a node to run it again (ITL-1095). Reading or running a job's
  side-effect node no longer delivers every row again: deliver through `job.run()`; to
  repeat the deliveries of a side-effect pod declared `track_completion=False`, call
  `clear_cache()` on its node first (ITL-1091, ITL-1095).
- An operator stream's `run()` takes no arguments; job nodes' `run()` returns a `StreamView`
  (ITL-1081).
- Displaying a stream, source or node in a notebook never computes it: call `as_table()` or
  run the job (ITL-991).
- A database write or read failure fails the run whatever the `error_policy` (ITL-878,
  ITL-884). The async orchestrator honours `error_policy="fail_fast"` and raises the failing
  node's own exception, never an `ExceptionGroup`: replace `except*` / `except ExceptionGroup`
  with `except SomeError` (ITL-879).
- Appending rows whose column type differs from a stored Delta table's raises
  `InputValidationError` instead of casting (ITL-884).
- A connector database (`ConnectorArrowDatabase` over SQLite, PostgreSQL or SpiralDB) cannot
  hold a function node in `0.3.0`: the job refuses the node when it is built, naming it,
  with `UnstorableColumnError`, where it used to run the function and fail at commit
  (ITL-1162, ITL-1164). Use `DeltaTableDatabase` or `InMemoryArrowDatabase` for a job's
  database.

**Operators.**

- `GroupBy` keeps only its `by` tags; a non-key tag is dropped, no longer turned into a data
  column. Offer the tag as data before grouping
  (`DerivedSource(origin, tags_as_data={...})`), or drop the parameter that read it
  (ITL-1070).
- `SemiJoin` matches on the tag columns both inputs share; shared data columns are ignored,
  with a `SemiJoinIgnoredColumnWarning`. Rename or drop the column on the right input to
  silence it (ITL-1088, ITL-829).
- The fluent `map_tags` / `map_data` keep unmapped columns by default; pass
  `drop_unmapped=True` for the old behaviour (ITL-1027). The renames dict is their only
  positional argument: `drop_unmapped`, `strict` and `label` are keyword-only, so
  `stream.map_tags(names, True)` raises `TypeError`; and `map_data(name_map={...})` no
  longer names the dict: pass it by position (ITL-1160, ITL-1171).
- `map_tags` / `map_data` refuse a name that is not a column of the kind they rename, with
  `InputValidationError` (for example `map_tags` of a column that became data after
  `group_by`); it used to rename nothing. Fix the name, use the other method or the new
  `map_columns`, or pass `strict=False` (ITL-1160). A new name a stream would read as a
  system column (`_tag_...`, `_source_...`, `_context_key`) is refused (ITL-1160).
- A name that is a tag in one input and data in another is refused when the graph is built
  (`Join`, `MergeJoin`, `Pick`/`Index` `out=`, a pod output named like an input tag); rename
  with `map_tags` or `map_data` (ITL-940). A tag column named with the `__` prefix is refused
  (ITL-1080).
- Column selections are identified by the set of names: a pipeline that spelled them as a
  tuple, a set, or an unsorted list gets new tables (ITL-942).

**Function pods.**

- `pod(a, b)` now drops the data columns the function does not take (an automatic
  `SelectDataColumns` after the join). Use `pod(a, b, strict=True)` or `pod.process(...)` to
  keep rejecting extra columns (ITL-807).
- A function's return value must match its declared output type; a mismatch raises
  `OutputTypeError` (numeric narrowing, float for int, a set for a list, a plain tensor for
  `-> torch.nn.Parameter`). Convert explicitly in the function (ITL-895, ITL-871).
- A default parameter value the hasher cannot hash (an `Enum` member, a dataclass instance,
  `Path`, `datetime`, `Decimal`) raises `TypeError` when the pod is created: use a hashable
  default or supply the value as a column (ITL-953).
- `FunctionPod.from_fn` and `side_effect_function_pod` default to version `v0.0` (was
  `v1.0`); pass `version="v1.0"` to keep `/v1/` in paths (ITL-942).
- An ordinary `@dataclass` or pydantic model is identified by its exact class
  (`module.Class`): a class defined again (a re-run notebook cell) is a different type. Use
  `orcapod.dataclass` for a class identified by its name and fields. Changing a class's
  fields makes a new type with new tables; a recursive class raises `RecursiveTypeError`
  (ITL-972).
- A `set[T]` of a primitive (`set[int]`, `set[str]`) is stored as the native set container
  (Arrow extension type `set[int64]`) and reads back as a `set` on every path: a downstream
  pod, `node.as_source()`, `DerivedSource` and a job loaded in another process, where it
  read as a `list` before (ITL-611). `list[T]` is unchanged.

**Sources and binding.**

- Binding a source to a job slot checks column types, not just names; a different type, or a
  nullable column for a non-null slot, raises `SourceSpecMismatchError`. A joined or merged
  stream, or one without system tags, is refused too: wrap it with
  `ArrowTableSource(stream.as_table(), tag_columns=[...])` (ITL-1112).
- Every table has large Arrow types (`large_string`, `large_binary`, `large_list`): compare
  with `pa.large_string()`, not `pa.string()` (ITL-1122).
- In-memory sources without a `source_id` are named `<kind>:arrow_v1:<hex>`
  (`dict:arrow_v1:...`), and `node.as_source()` is `derived:semantic_v1:<hex>`. Pass an
  explicit `source_id` to keep a name stable (ITL-957).
- Default source ids of file and table sources name the kind of source:
  `csv:<path as passed>`, `delta_table:<path or URI as passed>` (was the folder name),
  `sqlite_table:<database path>:main.<table>`, `postgresql_table:<database>.<schema>.<table>`,
  `spiraldb_table:<project>.<dataset>.<table>@<table id>`, and
  `polling_source:semantic_v1:<hex>` (ITL-1154, ITL-1157, ITL-926). A `DBTableSource` used
  directly over a connector that gives no database address requires a `source_id`. A source
  id or column name holding `::`, and an empty source id, are refused. Pass `source_id=` to
  keep an earlier id.
- `record_id_column` is `record_id_columns` (one name or several, in key order; `()` for the
  row position) on every source (ITL-1154). A database or SpiralDB table identifies its rows
  by its primary key by default; a database table without one is refused unless
  `record_id_columns` is given (a SQLite table that relied on `rowid` passes
  `record_id_columns="rowid"`); a `PollingSource` requires it (ITL-1158). A key value must be
  a string, an integer, a boolean, a timestamp or a date; a null or repeated key is refused
  when the source is built.
- The record-id system tag holds the row identifier as text (`row_<n>`, or the canonical JSON
  of the key columns), no longer a `uuid5`, and a source-info token is
  `<source id>::<record id>::<column>` (ITL-1154). Update code that parsed these values.
- `SpiralDBTableSource` reads no rows when it is built; a table without a key schema is
  refused; reads are capped by `max_rows` (ITL-926, ITL-186).
- Every source built from a table takes the keyword-only option `tag_only_columns`. In
  `0.3.0` every tag is plain (a tag only), and the one accepted value is `True`, the default;
  any other value raises `ValueError`. In v0.4 a declared tag becomes referential by default
  (also the data column of the same name): to keep today's behaviour then, pass
  `tag_only_columns=True` now. Hashes, source ids and saved files are unchanged by it
  (ITL-1159).

**Values that are refused now.**

- A `None` inside a container where a read could not give it back (`list[list[int] | None]`,
  `list[Path | None]`, a field `x: UUID | None`) raises `ValueError` at declaration; a source
  given a `None` its declared type does not allow raises `UnsupportedValueError` (ITL-1144).
  Declare the inner type without `| None`, or wrap it in a dataclass.
- `frozenset[T]` is not supported: declare `set[T]` (ITL-611).
- A logical type or native container as a dict key or value, at any depth
  (`dict[str, set[int]]`, `dict[str, Path]`, `list[dict[str, set[int]]]`), raises
  `NestedTypeNotStorableError` (a `ValueError`) where it is declared: a pod's parameter or
  output, a source schema, a dataclass or pydantic field. It was stored as a plain list and
  read back as one. Hold the value in a dataclass field, or declare the plain type
  (`dict[str, list[int]]`) (ITL-1150).
- A dict or set value is stored in canonical order; a NaN key or element, keys or elements
  that cannot be compared, or two keys that convert to one stored value raise
  `CanonicalOrderError` naming the column (ITL-905).
- NumPy arrays: masked arrays, object dtypes, `StringDType`, `longdouble`, unit-less
  datetimes, field titles, overlapping structured fields and very large headers are refused
  at write (ITL-888, ITL-981).
- pandas values storage would change or not read back are refused at write, with
  `ValueError` (ITL-1054, ITL-897).
- An Arrow table with a null in a non-nullable field, a zero-row `map` column, or two
  columns at one path (duplicate names) raises `ValueError` naming the column; `null`,
  `map`, interval, view, union and run-end-encoded data raise `ValueError` instead of
  `NotImplementedError` (ITL-910).

**For authors of extensions.**

- A custom Arrow hasher must implement `ArrowHasherProtocol.hash_table_ordered` (ITL-897); a
  custom logical type not derived from `BaseLogicalType` must implement
  `readable_extension_metadata()` (ITL-935); a type handler that returned a plain
  `{"__type__": ...}` dict returns `Encoded(tag, fields)` (ITL-942).
  `StarfixArrowHasher.hash_table_ordered` returns the label `arrow_ordered_v1`, and a nested
  hash enters another hash with its label (ITL-1154).
- An operator's `async_execute` requires `input_pipeline_hashes` (ITL-906), and
  `FunctionNodeProtocol.async_execute` takes a keyword-only `error_policy` (ITL-879).

**Documentation.**

- The tutorial notebooks in `notebooks/tutorials/` are removed: follow the tutorial in
  `docs/tutorial/` (eleven short parts, with exercises and solutions), and see the new pages
  *Pipelines and Jobs* and *File and Path Types* (PLT-4864).

#### Other breaking changes

- `AsyncPipelineOrchestrator(error_policy="fail_fast")` now stops the run at the first row whose function raises and raises that row's own exception, as `SyncPipelineOrchestrator` does (ITL-879). Before, the async orchestrator ignored the policy: the run skipped the row, computed the others and returned. An async run with `fail_fast` that used to finish now raises; pass `error_policy="continue"` (the default) to keep the old behaviour. Rows run concurrently, so the row raised is the first to fail, which is the first in input order only with `max_concurrency=1`. Every stored row is a correct result of its input row and the failing row is never stored; which other rows are stored before the run stops is not specified (see `docs/concepts/function-pods.md`, "Stopping at the first failed row").
- An async run raises the failing node's own exception, never the task group's `ExceptionGroup` (ITL-879). This covers a side-effect pod with `on_error="raise"`, an operator that rejects its inputs, and any other node failure, as store errors already were. When several nodes fail before the run stops, one exception is raised and the others are added to it as notes (`error.__notes__`): a store error (`StoreError`) first, then a stored row that cannot be decoded as its class (`TypeIdentityError`), as before; among the other failures, the one of the node that comes first in the pipeline. Replace `except* SomeError` or `except ExceptionGroup` around `job.run()` / `job.run_async()` / `AsyncPipelineOrchestrator.run()` with `except SomeError`.
- `FunctionNodeProtocol.async_execute`, `FunctionJobNode.async_execute` and the function pods' `async_execute` take a keyword-only `error_policy` (default `"continue"`), as `execute` already did. A class implementing `FunctionNodeProtocol` must accept it.
- Saved pipeline/job schema text now includes the full logical type identifier as
  `extension<name;hex-metadata>`, including nested logical types. Loading validates it
  against the registered type; incompatible or malformed identifiers raise
  `LogicalTypeIdentifierError` naming the node and column. Name-only extension text from
  older saved files is rejected with an earlier-version error. Read those files with the
  version that wrote them, and save a new job from the original pipeline under this version.
  Stored table layouts, value decoding and hashes are unchanged. Unregistered logical types
  are reported by node and column; affected operators load as read-only stubs. (ITL-904)
- **What a v0.2 store looks like to v0.3: old tables are found by name, not by hash.** v0.3
  computes other hashes than v0.2, so a v0.2 table is at a path v0.3 does not compute. Each
  function and side-effect node lists the tables stored under its job and function or pod
  name (`<job>/<function>/`, `<job>/_result/<function>/`,
  `<job>/side_effect_function/<function>/`, `<job>/side_effects/<pod>/`) and treats
  every table that is not at a current layout as written by an earlier version. Function
  nodes report them in one warning per node (no number of rows: the tables are not
  read); side-effecting nodes raise `SideEffectLogDataContextError`. A job run adds one
  total of the old tables that none of its nodes reports: operator caches, cached sources
  stored under the job, and nodes that left the job. Because tables are found by name, an
  old table of another function or pod with the same name in the same job, or of another
  node of the same function, counts too; the warning and the error name the tables.
  (ITL-954)
- A store that cannot list its tables (a Delta store on `gs://` or `az://` without the
  matching `fsspec` package, a third-party database without `list_table_paths`) is not
  searched by name. A side-effecting node then logs one warning that it could not check
  for completions logged before v0.3, and runs. (ITL-954)
- **`PipelineJob.run()` returns a `RunSummary`, not the job.** Results are read from the
  job itself, as before (`job.nodes[...]`). Code written `job = job.run()` becomes
  `job.run()`; code that kept the return value as the job (`completed = job.run()`,
  `completed.nodes[...]`) uses the job instead. `run_async()` returns the same summary.
  (ITL-997)
- The `content_hash` of a `DerivedSource` built with `tags_as_data` now includes the mapping (ITL-1000). Before, two mappings that gave the same schema (two tag columns of one type copied to one name) and shared an explicit `source_id` had the same `content_hash` although the copy held different values; the sorted mapping items are now part of the identity. The `content_hash` moves whenever `tags_as_data` is given, with the default `source_id` as well as an explicit one, because the mapping is appended in both cases. The `source_id` (the default id was already built from the mapping), record IDs, source-info tokens and `pipeline_hash` do not move, and a `DerivedSource` without `tags_as_data` (a job node's stored records) is unchanged. `tags_as_data` was added in 0.3 and has not been released, so no released store holds a hash that moves. A store written by an unreleased 0.3 build with a `tags_as_data` `DerivedSource` gives that source, and the nodes downstream of it, a new node identity: those pipeline records are recomputed, and the old rows stay in place. The golden values `schema:derived_source_tags_as_data.{default_source_id,named_source_id,record_id_column}.content_hash` changed with it.
- `resolve_field(record_id, field_name)` is removed from `SourceProtocol`, `RootSource` and `SourceProxy`, and `orcapod.errors.FieldNotResolvableError` is removed with it. The method was added with `RootSource` (February 27, 2026) and its only implementation was dropped on March 13, 2026; since then every source raised `NotImplementedError`, so it never worked on a shipped source and nothing called it. There is no replacement: read a source's values through its stream (`as_table()`, `iter_data()`).
- `PipelineJob` takes its database as `database`, not `store`: `PipelineJob(name=..., database=db)`, `PipelineJob.from_pipeline(..., database=db)`, `job.bind(database=db)`, `PipelineJob.load(path, database=db)` and the `job.database` property. There is no `store` alias. Migrate with `store=` to `database=` on those calls and `job.store` to `job.database`. The saved job file writes the database config under `bindings.database` (was `bindings.store`); the format version `orcapod_pipeline_job_version` stays `0.2.0`, a version no release has shipped. `PipelineJob.load(path)` of a file that still has `bindings.store` raises `ValueError` naming both keys instead of loading without its database: pass `database=` to `load` (it overrides the saved block, so such a file then loads), or rename the key in the file and save it again. The old key is never read.
- `run()` on a job's side-effect node no longer delivers every side effect again on every call (ITL-1091). It does the work `job.run()` does for that node: with `track_completion=True` (the default) it delivers only the rows whose delivery the invocation log does not record, and with `track_completion=False` it delivers every row again, as a second `job.run()` does. Code that called `node.run()` or `node.as_table()` on a job's side-effect node to deliver again must call `job.run()` (with `track_completion=False` to repeat deliveries) or use the side-effect pod outside a job, whose stream still delivers on every read.
- A job node shows the current run; `node.as_source()` shows everything stored for it (developer, November 13, 2026; ITL-1095). Before a run in this process, a function or operator job node is empty: `as_table()`, `iter_data()`, the dataframe methods, `flow()`, `head()` and the notebook preview return zero rows with the node's columns, and `head()` (status `not_computed`) says the node has not run and points to `run()` and to `node.as_source()`. Before, a function node built over a store that already held results, or loaded with `PipelineJob.load`, returned every row stored under its key, and an operator node in `CacheMode.REPLAY` returned every logged run. The same holds after `clear_cache()`, after a run that raised, and while the node is stale. Migration: read stored results with `node.as_source()` (or `get_all_records()`), or run the job first; a run over a store that holds every result computes nothing.
- A function or operator job node that has not run in this process is stale (`is_stale` is `True`; it has no modified time), and a job node is stale also when any function or operator job node it depends on is. Binding a source to a job's source slot (`job.bind(sources=...)`) marks the slot modified, so every node fed by it is stale and empty until it runs again.
- A loaded operator node no longer raises from `as_table()` or `iter_data()`: it is empty, like every node that has not run. Its stored view (`as_source()`, `get_all_records()`) raises instead when the job file does not record its run's content hash and the store holds nothing under the hash the loaded job computes (ITL-1043).
- A job node's `run()` is state-driven and runs what the node depends on (developer, November 13, 2026; ITL-1095). A node that holds a completed run and is not stale runs nothing: `run()` returns the rows it holds. Otherwise every job node it depends on that holds no current run runs first, each once, as `job.run()` would run that part of the job, then the node; the run's writes are committed when it ends (before, `node.run()` read an upstream node's stored rows, or nothing, and left its last pipeline record and a `LOG` operator's output uncommitted). `run()` of a node whose function pod is a placeholder, or that was loaded without one, raises unless `load_only=True`; a `CACHE_ONLY` node's `run()` raises (its input is unavailable): read its stored results with `as_source()`. A job node's `run()` takes one keyword-only argument, `load_only`; every other stream's `run()` still takes none.
- A side-effect job node follows the same rule: it is stale before a run, and while it holds a current run neither its `run()`, nor a downstream node's `run()`, nor `job.run()` delivers anything; to deliver again without `track_completion`, call `clear_cache()` on it first. A load-only run passes its rows on and delivers nothing.
- `job.run()` and `job.run_async()` follow the same rule as `node.run()` (developer, November 14, 2026): a node that holds a current run is not run again, and its rows feed the nodes after it. Running an unchanged job a second time now computes nothing, reads and writes nothing in the store, reads no source rows to process them and delivers no side effect, even without `track_completion` (before, every `job.run()` re-read every source and re-processed every node, and delivered every untracked side effect again). Nodes run when they have not run in this process, were cleared, are below a cleared node or a node that ran again, or below a source bound again or whose own modified time moved. A re-run notices new data in a source whose object is unchanged through the source's start-of-run check (ITL-1102); a source that cannot see its data change (one built over data held in memory: a `DictSource` copies the list it is given) needs a new source object, bound with `job.bind(sources=...)`. The summary reports a node that was not run as held: "rowwise: 6 rows: held from the previous run" (`NodeRunSummary.held`). If the commit at the end of a run fails, the nodes it ran are cleared and the nodes it held keep their rows.
- If a run raises or is interrupted, or the commit at the end of `job.run()` or `node.run()` fails, every node the run ran is cleared (not run, stale) before the error propagates; nodes it held keep their rows. A run that left failed rows (`error_policy="continue"`) is never current: the next run computes only the failed rows and reads the rest from the store.
- Every table Orcapod presents or stores has the large Arrow variants of string, binary and list types (`large_string`, `large_binary`, `large_list`, recursively), with no exception (ITL-1122). A `string`, `binary` or `list` column of an Arrow table you give `ArrowTableSource` (or a raw `ArrowTableStream`), or of an Arrow schema you pass as `schema=` to `CSVSource` or `DataFrameSource` or as `data_schema=` to `DictSource`, now comes back as `large_string`, `large_binary` or `large_list`. Names, nullability, widths and logical types are kept. Code that compared an Arrow type to `pa.string()` compares it to `pa.large_string()` now. No hash moves: large and non-large variants hash identically, so row hashes, content hashes, pipeline hashes, record ids and default source ids stay as they were, and a Delta table written with either variant accepts rows of the other.
- A `None` inside a container is refused where a read could not give it back (ITL-1144). A list or set element, a dict value or a class field declared `X | None`, where `X` is a container (`list`, `set`, `dict`) or a logical type other than your own class (`Path`, `UPath`, `UUID`, `numpy.ndarray`, `torch.Tensor`, pandas and Polars values), now raises `ValueError` when the type is declared, as a function's parameter or return value, a source's `data_schema`, or a field of a dataclass or pydantic model. Examples: `list[set[int] | None]`, `list[list[int] | None]`, `dict[str, list[int] | None]`, `list[Path | None]`, `set[UUID | None]`, a field `x: UUID | None` or `x: list[int] | None`. The error names the column (or `Class.field`), the type and ITL-1144. Such a `None` was stored correctly, but read back as an empty container (`[]`, `set()`, `{}`), with a different row hash, or the read failed with a `TypeError`: through `as_source()`, a `DictSource`, a loaded job and a downstream function. An in-memory pod-to-pod chain that never stored or re-read the values did keep the `None`; it is refused too. Declare the inner type without `| None`, or hold the value in a dataclass and make that optional (`list[MyData | None]`). Still accepted, unchanged: a `None` inside a container where the inner type is a scalar (`list[int | None]`, `dict[str, float | None]`, a field `x: int | None`) or your own class (`list[MyData | None]`), and a `None` at the top level of any column (`list[Path] | None`). No hash, stored table or golden value changes.
- Reading a stored column that holds such a `None` raises `ValueError` naming the column, instead of returning a wrong value or a `TypeError` (ITL-1144). A null key/value entry of a stored dict raises too, instead of being dropped. This covers an Arrow table given to `ArrowTableSource` with a null inner list or struct, and `GroupBy` or `Batch` over a nullable list, set, `Path` or `UUID` column, whose output rows read as Python values now raise. `GroupBy` and `Batch` over a nullable scalar column (`list[int | None]`) are unchanged. Drop the nulls first (`DropNulls` on that column) or keep the value in a dataclass.
- A source given a `None` where its declared type does not allow one now refuses instead of storing an empty container or a wrong value (ITL-1144). A `None` for a list, set or dict whose declared type is not `| None`, inside a column's value (`[None, [1]]` for `list[list[int]]`, `{"k": None}` for `dict[str, list[int]]`, a dataclass field `xs: list[int]` given `None`) or as the column's own value, was stored as `[]`, `set()` or `{}`. A `None` for a logical type declared without `| None` reached the type's encoder: a `Path` was stored as the string `'None'` and read back as `Path('None')`, a `list[UUID]` as `[]`, and a `UUID` failed with an `AttributeError`; a user class raised `TypeError` and now raises the same `UnsupportedValueError`. `DictSource`, `ListSource` and any other source built from Python values now raise `UnsupportedValueError` (a `ValueError`) naming the column and the declared type. A function output already refused such a value (`OutputTypeError`). A `None` at a position declared `| None` is unchanged, and no hash, stored table or golden value of a valid value changes.
- Every v0.3 hash is defined anew before its first release by one rule: a hash always carries its label, except where it is written into a name, whose grammar fixes which algorithm produced it (ITL-1154, revising ITL-945). A nested hash enters a structure hash as `{"__type__": "hash", "label": ..., "digest": ...}`; a value-hashed cell of a logical-type column enters a table hash as `<identifier>::<label>:<digest>`; the input data hash in the entry-id preimage, the node content hash in the operator-cache row key, the run snapshot, the ordered Arrow digest inside a pandas value hash and a file's hash inside a directory hash all take the hash with its label. Renaming a label now moves every hash it feeds, and two algorithms that produce the same digest can no longer feed the same input (a `File` whose bytes are a value's canonical JSON no longer shares that value's hash inside a structure). Hashes inside names stay bare hex.
- `bytes` and `bytearray` values hash as `{"__type__": "bytes", "hex": ...}` and `uuid.UUID` values as `{"__type__": "uuid", "hex": ...}`, so a value, its bytes and the text of its hex digits never share a hash (ITL-1152). A UUID column is hashed as stored (its 16 bytes and the extension identity), no longer through the value handler. `frozenset` is hashed as its own type, apart from `set`, which also moves the identity of `Join`, `MergeJoin` and function nodes (they hold their inputs as a frozenset).
- `StarfixArrowHasher.hash_table_ordered` returns the label `arrow_ordered_v1`.
- Every source's default `source_id` names its kind, the class's `SOURCE_KIND` (which is also its config type key): `csv:<path as passed>`, the database and table forms below, and `polling_source:semantic_v1:<hex>` of `impl.identity()` (was `str()` of the identity, which is not stable). The in-memory and derived defaults keep their `<kind>:<label>:<hex>` form, at full length. A `source_id` you give is used exactly as given; give one to keep an earlier id.
- The record-id system tag holds the row identifier itself, as text: `row_<n>`, or the canonical JSON of the key columns in key order (`{"unit":"u1","time":1700000000}`). It is no longer a `uuid5` and holds no source id; a source-info token is `<source id>::<record id>::<column>`. The folded source id and record id of `Batch` / `GroupBy` are both `batch_v1:<hex>` / `group_v1:<hex>`: the operation's label and the SHA-256 of the canonical JSON member list.
- `record_id_column` is replaced by `record_id_columns` (a name or several, in key order; `()` for the position) on every source, now including `DictSource`, `DataFrameSource`, `ListSource`, `SpiralDBTableSource` and `PollingSource`. Database tables and SpiralDB tables identify rows by their primary key by default; a database table without one is refused unless `record_id_columns` is given (a SQLite table that relied on `rowid` passes `record_id_columns="rowid"`). A `PollingSource` requires `record_id_columns`: rows of different polled batches no longer share a record id (ITL-1158). A key value must be a string, an integer, a boolean, a timestamp or a date (the last two written as ISO-8601 text: a naive timestamp without an offset, a zoned one in UTC with `Z`); any other type, a null key value and a repeated key are refused when the source is built.
- Default source ids of file and table sources: `delta_table:<path or URI as passed>` (was the folder name), `sqlite_table:<db path as passed>:main.<table>`, `postgresql_table:<database>.<schema>.<table>`, `spiraldb_table:<project>.<dataset>.<table>@<table id>` (ITL-1157). A `DBTableSource` used directly, over a connector that gives no database address, has no default: it requires a `source_id`, because the table name alone does not say which database the table is in. A source id or a column name holding `::` is refused, and so is an empty source id.
- A root source's content hash includes its key columns, the resolved `record_id_columns` in key order (`()` for the position; the primary key of a database or SpiralDB table by default), so two sources with one `source_id` that name their rows differently are two sources: they no longer share a cache (`CachedSource`) or a node's stored records. The pipeline hash, the source id (given or default), the record ids and the source-info tokens are unchanged. This applies to every root source, including `ListSource`, `PollingSource`, `SpiralDBTableSource` and `DerivedSource` with `tags_as_data`.
- A store written by a development build of v0.3 is discarded, not migrated: the v0.3 table layouts were redefined before their first release (the record-id column is now text).
- These change 849 golden values of data context `v1`, which no release has shipped: v0.3.0 is its first release. Nothing needs migrating beyond what v0.3 already requires: data written by v0.1.4 or v0.2 is already stale under v0.3, and data written by a development build of v0.3 is recomputed (a moved entry id or path is not found; nothing is lost or read wrongly). **Side effects are delivered again:** a side-effect node finds its completed deliveries by entry id, so deliveries logged by a development build of v0.3 are not recognized and a run delivers them again. A new store does not avoid this: it holds no log at all, so a run delivers every input again too. Run a side-effect node that was run by a development build of v0.3 only where a repeated delivery is acceptable.
- `map_tags` / `MapTags` and `map_data` / `MapData` refuse, by default, a mapping key that is not a column of the kind they rename (ITL-1160). Before, such a key was accepted and renamed nothing, which hid mistakes such as `map_tags` of a column that had become data after `group_by`. The call now raises `InputValidationError` naming the missing names and the columns of that kind; a name that is a column of the other kind says so (`'age' is a data column: use map_data`). The check runs when the operator is called on a stream, before any row is processed, and on the async path against the first row. A raw stream's meta (`__`) column is not a data column to rename. Migration: fix the name, use the other method (or `map_columns`), or pass `strict=False` to keep the old behaviour.
- `strict` is part of the identity of `MapTags` and `MapData`, as it is of the column selection operators: an operator's identity is its full configuration (developer decision, ITL-1160). Their identity is now `(class name, name_map, drop_unmapped, strict)`, and it is saved in `to_config()`. This moves the content and pipeline hash of every `MapTags` and `MapData` step and of everything downstream of one, and with them the paths of the tables that are scoped by those hashes.
- The stream methods `map_tags`, `map_data` and `map_columns` take the renames dict as their only positional argument, and `drop_unmapped`, `strict` and `label` are keyword-only (ITL-1160, ITL-1171). `stream.map_tags(name_map, True)` and `stream.map_tags(name_map, False, "label")` raise `TypeError`: write `drop_unmapped=True`, `label="label"`. `stream.map_data(name_map={...})` no longer names the dict (it reads as a rename of a column called `name_map` and raises `TypeError`): pass the dict by position. The operator classes `MapTags`, `MapData` and `MapColumns` keep their arguments.

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

#### `SpiralDBTableSource` reads no rows when it is built, and reads in batches (ITL-186)

A Spiral source used to read its whole table when it was built, which made large tables
unusable (about 4.6 GB for an 8-million-row table). It is now built from the table's metadata
alone, in about a second, and reads rows only when asked: in batches of at most `batch_rows`
rows (default 8,192) for `iter_data()` and pipeline jobs, and all at once for `as_table()` and
the dataframe methods. See "Reading a Spiral table" in `docs/concepts/sources.md`.

What changes for existing code:

- **Default `source_id`.** It is now `<project id>/<dataset>.<table>@<Spiral table id>`, for
  example `edi-tutorial-v1-841226/default.session@table_Jo7J0lpkLcOM` (it was
  `<project id>/<dataset>/<table>`). SpiralDB's own table id makes it unique: a table dropped
  and created again under the same name is a different source. For a Spiral source built
  without `source_id`, the `content_hash`, every record ID and every source-info token
  change. `pipeline_hash` does not.
- **Record IDs come from the primary key.** A row's token is the Arrow hash of its key
  columns (for example `arrow_v1:000002...`), no longer its position (`row_0`). Record IDs
  and source-info tokens of every Spiral source change, also with an explicit `source_id`;
  `content_hash` and `pipeline_hash` of a source with an explicit `source_id` do not.
  Pipeline records stored earlier for a Spiral source are not found again: the rows are
  recorded once more, and no result is recomputed, because function results are found by
  the row's data.
- **`record_id_column` is removed** from `SpiralDBTableSource`: the primary key identifies
  the row.
- **A table without a key schema is refused**, also when `tag_columns` is given.
- **`SpiralDBTableSource` is no longer a `DBTableSource`.**
- **A read is capped at `max_rows` rows** (default 100,000); a larger read raises
  `SourceTooLargeError`. Pass `limit=`, `where=`, or `max_rows=None`.
- **A saved job** stores the table id. Loading it raises `ValueError` if the saved table name
  now leads to a table with another id. Loading reads no rows.

#### A set of a primitive is stored as the native set container; its rows hash differently (ITL-611)

A column declared `set[T]`, where `T` has no logical type (`set[int]`, `set[str]`,
`set[bytes]`, `set[datetime]`), is now stored under the Arrow extension type
`set[<element Arrow type>]` (for example `set[int64]`, metadata
`{"category": "set", "element_kind": "arrow"}`: the element type is in the name and is read
from the storage, `large_list<int64>`) instead of a plain `large_list`. This moves:

- the stored Arrow type of such a column (at the top level, as `set[T] | None`, and as the
  element of a list: `list[set[int]]` is stored as `list[set[int64]]`);
- the row hash of every row that holds such a column, and with it the input hash of a function
  pod that takes such a column (its results are computed again), and the default `source_id`
  and content hash of an in-memory source with such a column.

Type, schema and pipeline hashes do not move, nor does anything about `list[T]` or about a set
of a logical type. No released version wrote such a row: only stores written by development
builds of v0.3 hold a set of a primitive as a plain list. Such a store is not refused: a node
serves its stored values as lists, and a run that adds a row raises when the stored and the new
form are combined. Delete the stored tables of such nodes and run again.

#### `SemiJoin` keys on the shared tag columns only (ITL-1088)

`SemiJoin` used every name its two inputs share, tag or data, as a match key. A left row was
kept only when its tags **and** its values in every shared data column equalled those of some
right row, and a name that was a tag on one side and a data column on the other was a key too.
So the operator read data values, which only filters may do. The match keys are now exactly the
columns that are tag columns of both inputs. Any other shared name (data on both sides, or a
tag on one side and data on the other) is ignored, and the right input's data columns are
never read.

```python
left = DictSource(data=[{"id": 1, "y": 1}, {"id": 2, "y": 2}], tag_columns=["id"])
right = DictSource(data=[{"id": 1, "y": 1}, {"id": 2, "y": 9}], tag_columns=["id"])
SemiJoin()(left, right)
# v0.2.0: only id 1 (id and y must both match)
# now:    id 1 and id 2 (only the shared tag id decides), and a SemiJoinIgnoredColumnWarning
#         naming 'y'
```

- **What changes.** If the two `SemiJoin` inputs share a data column name, the result now keeps
  every left row whose shared tags match, whatever the data values. Before, rows whose shared
  data values differed were dropped, so the new result always contains the old one. Filtering a
  stream by a subset of itself (every data name shared, equal values) gives the same rows as
  before. A shared data column whose type or nullability differs no longer raises, because it
  is not a key.
- **The warning.** Each ignored shared name is listed in a `SemiJoinIgnoredColumnWarning`
  (new, in `orcapod.errors`), once per operator and set of names. Silence it by renaming or
  dropping the column on the right input before the semi join, or with
  `warnings.filterwarnings("ignore", category=SemiJoinIgnoredColumnWarning)`. This behaviour is
  provisional in v0.3 and may become an error.
- **Unchanged.** `SemiJoin`'s identity and pipeline hash do not change. The output schema is
  still the left input's. With no shared column at all, every left row is still returned.
- **Stored operator caches.** A cached `SemiJoin` node keeps its table path, so what it stored
  before the upgrade is still there. With caching off it computes the new result. With
  `CacheMode.LOG` it computes the new result on its next run, passes it downstream, and adds
  the rows it had not stored yet. With `CacheMode.REPLAY`, and for a pipeline loaded from a
  file, it serves the stored rows, which are the old, smaller result. To get the new result
  there, run the pipeline once with the operator cache in `LOG` mode, or clear the operator's
  cache.

#### pandas values: a strict, content-based hash; column order is identity (ITL-897, ITL-887)

A `pandas.DataFrame` or `pandas.Series` value now hashes under its own label,
`pandas_dataframe_v1` / `pandas_series_v1`, instead of as an Arrow table (`arrow_v1`).
Storage and hashing are separate: a value is stored with pyarrow's own conversion (unchanged),
and the hash is defined by Orcapod and is about the content of the value. The hash is strict:
everything about a value that can be changed is part of its identity, except a property
storage does not restore (developer, November 11 and 12, 2026).

Before, these pairs of values shared a hash, so a cached pod could be handed the result
computed for the other one (for example the wrong `df.columns[0]`):

| Property | Before | Now |
|---|---|---|
| Column order | not hashed | hashed |
| MultiIndex level order (frame and Series) | not hashed | hashed |
| Struct field order in a `pd.ArrowDtype` struct column | not hashed | hashed |
| `columns.name` / `columns.names` | not hashed | hashed |
| Dtype of the column axis, per level (an `int64` versus an `object` Index of the same labels) | not hashed | hashed |
| Column label type (`0` versus `"0"`), MultiIndex columns versus flat labels | not hashed | hashed |
| Series name type (`0` versus `"0"`); a frame versus its one-column Series | not hashed | hashed |
| The exact Arrow type of a non-nested `pd.ArrowDtype` column (`binary` versus `large_binary`, `null` versus `large_string`) | not hashed | hashed, as storage restores it (a nested type, and a decimal, which storage restores as `object`, are not distinguished; ITL-1054 decides nested types) |
| pandas dtype where the Arrow values coincide (`Int64` versus `int64`, `boolean` versus `bool`, `string` versus `object`) | not hashed | hashed |
| Categorical versus string; the category list (unused categories included), category order, `ordered` (ITL-887) | not hashed | hashed |
| `attrs` | not hashed | hashed, as the JSON object storage keeps |
| Values, row order, index values, index names and dtype, column dtype, Series name, `-0.0` versus `0.0` | hashed | hashed |
| `flags`, `RangeIndex` versus an equal `int64` index, an index's `freq`, unused MultiIndex levels, `string[python]` versus `string[pyarrow]`, key order of dicts in an `object` column | not hashed | never hashed: storage does not restore them (a `dict` is unordered) |
| Type of an index level name (`1` versus `"1"`) | hashed | never hashed: storage writes an index name as text, so both read back `"1"` |
| Types of column labels of mixed type (`0` beside `"b"`) | not hashed | never hashed: storage writes labels of mixed type as text |
| A masked, string or Arrow dtype of an index level (`Int64` versus `int64`) | not hashed | never hashed: storage restores an index level as NumPy |
| The dtype of a categorical's categories where storage changes it (`object` categories `[1, 2]` versus `int64`) | not hashed | never hashed: storage restores the categories from the Arrow dictionary |

A value read back from a store now always hashes like the value that was written. Before, an
`object` column of non-strings (`[1, 2]` reads back `int64`) hashed differently after the
round trip; such a column is now hashed in the form storage restores. The same holds for an
`object` column of lists or dicts (a list `[1, None]` reads back `[1.0, nan]`) and for index
level names (the name `1` reads back `"1"`).

Storing a pandas value that storage would corrupt now raises `ValueError`: column labels
and index names that collide once written as text (the index name `1` with a column `"1"`;
integer names of MultiIndex levels), and distinct labels that read back equal (the labels
`True` and `False`). Before, such a value was written and read back without its index (the
levels came back as columns) or with duplicate labels.

`attrs` must be JSON. `attrs` with an entry JSON cannot write (a `set`, a tuple as a key)
are refused by the hash and by storage with `ValueError` naming the key, on every supported
pyarrow version. Before, pyarrow dropped the whole `attrs` with a warning or raised
`TypeError`, depending on its version. A key that is not text is still accepted and is
written as text (`{1: "x"}` reads back `{"1": "x"}`).

The hash is `sha256(descriptor + b"\x00" + D)`: `D` is the Arrow hasher's ordered digest
(starfix 0.5 with `ordered_columns` and `ordered_struct_fields`) of the value's Arrow table
with positional field names, and the descriptor is canonical JSON in an Orcapod-owned grammar
of `attrs`, typed labels, `columns.names`, the column-axis dtype, index levels and pandas
dtypes. The definition and
the grammar are in `docs/reference/hashing.md`, section 10. A label or dtype outside the
grammar raises `ValueError` naming it.

#### pandas logical types are named `orcapod.pandas.dataframe` / `orcapod.pandas.series` (ITL-897)

The Arrow extension names of the two pandas logical types gain the `orcapod.` prefix (they
were `pandas.dataframe` / `pandas.series`), and the metadata is `{"ignore":[],"version":1}`:
the version, and the list of properties the data context configures the value hash to
ignore. Nothing is ignored by default. `hash_ignore` in the logical type registration file
can name `column_order`, `struct_field_order` and `attrs` for a frame, and `series_name`,
`struct_field_order` and `attrs` for a Series; two lists are two logical types, and a column
stored under another list is refused on read. The old names
are not registered any more: a column stored under them is refused on read, never read as
the current types or as raw bytes.

**What moves.** The hash of every pandas value, and with it the data hash and the cache keys
of every row that holds a pandas value. Because the name is part of the logical type
identifier, the type hash of `pd.DataFrame` / `pd.Series`, the schema hash, the pipeline hash
and so the table paths and system-tag column names of every node with a pandas column move
too, and of every node downstream of one. The stored bytes of a pandas value are unchanged.
v0.3 already recomputes everything a v0.2 store holds (ITL-910), and v0.2 tables are still
found by name, reported and left unchanged, so this adds no migration step for a released
store. A store written by an unreleased v0.3 development build computes nodes with a pandas
column once more, in new tables; the older tables stay in place, unchanged, and are not
reported.

#### Public interface: `ArrowHasherProtocol.hash_table_ordered` (ITL-897)

`ArrowHasherProtocol` gains a required method, `hash_table_ordered(table)`, implemented by
`StarfixArrowHasher`: the same procedure as `hash_table`, with column order and struct field
order at every depth part of the digest (`struct_field_order=False` leaves struct field
order out). `hash_table` is unchanged and stays order
independent (streams, datagrams, rows). A custom Arrow hasher must add the method.
`PANDAS_DATAFRAME_HASH_LABEL` and `PANDAS_SERIES_HASH_LABEL` in
`orcapod.hashing.pandas_value_hash` define the two labels, and
`PANDAS_IDENTITY_PROPERTIES` there lists every property and whether it is part of the hash.

#### pyarrow 22.0.0 or newer is required (ITL-897)

The minimum pyarrow version is raised from 20.0.0 to 22.0.0. pyarrow 20 and 21 do not
restore `DataFrame.attrs` when a frame is read back, so a frame with `attrs` would hash
differently after it was stored; from 22.0.0 `attrs` is restored. The locked version
(23.0.1) is unchanged.

#### A `dict` or `set` value hashes the same whatever order it was built in (ITL-905)

A `dict[K, V]` value is stored as `large_list<struct<key, value>>` and a plain `set[T]`
(element type without a logical type) as `large_list<T>`. Both were stored in Python
iteration order, so `{"a": 1, "b": 2}` and `{"b": 2, "a": 1}` had different row hashes,
and the same `set[str]` had a different row hash in every process (`PYTHONHASHSEED`).
Orcapod now stores dict entries sorted by key and set elements sorted, wherever a Python
value becomes Arrow; see the hashing reference, section 1e, for the order. A NaN key or
element, values that cannot be compared, and two keys that convert to the same stored
value raise `CanonicalOrderError` (a `ValueError`) naming the column. A datagram built from
Python values (and values set with `update` or `with_columns`) holds its dicts in the same
order, so a newly computed pod output and one served from the cache iterate alike. Dicts
nested in dataclass or pydantic instances are the exception: a fresh instance keeps insertion
order, while a read-back instance uses key order. An Arrow table is still hashed exactly as
stored: a key/value list that arrives in Arrow form is never re-sorted.

Set values passed to `update` after Arrow materialization, or to `with_columns` with an
explicit Arrow list field, are also stored in canonical order. The existing Arrow field
(width, nullability and metadata) is preserved; actual list values keep their supplied order.

Which hashes move because of this change, on top of the v0.3 hash step (ITL-910, ITL-945),
which moves every stored hash: only those of values holding a dict whose insertion order was
not key order, or a `set[T]` of two or more elements whose iteration order was not sorted
(for `str` and `bytes` that order changed with every process, so it had no stable value).
For such a row, its data hash moves, and with it the function pod cache key, the pipeline
record and entry ids, and the default `source_id` (with its per-row record ids) of a
source built without an explicit `source_id`. Schema hashes, `pipeline_hash` values and
explicit `source_id`s do not move for this reason, and a dict already built in key order is
stored byte-for-byte as before if every nested unordered container is also canonical.
In the golden hash suite exactly two entries moved:
`pipeline:index_pick.index.row[{"subject":"s1"}].data_hash` and `...s2...`, whose rows hold
`meta={"k": ..., "j": ...}`.

What a `0.2.0` store sees (checked against a Delta store written by `0.2.0`): nothing
changes because of this fix. v0.3 reads no table `0.2.0` wrote (every table is at a new path,
see "Data context `std:v1:default`", ITL-944), so every input is computed again whether its
dict was in key order or not, and no row stored in `0.2.0`'s insertion order is ever served.
The `0.2.0` tables stay where they are, untouched. Every dict and plain set a v0.3 store
holds from a pod output or a source built from Python values is in canonical order.

#### The async orchestrator names system-tag columns as the sync orchestrator does (ITL-906, ITL-885)

`MergeJoin`, `Batch` and `GroupBy` put their inputs' pipeline hashes into the names of the
system-tag columns they write. When one of these operators read an input that is not a
source (an operator's or a function pod's output), `AsyncPipelineOrchestrator` used a
different hash than `SyncPipelineOrchestrator`, so the same graph got different system-tag
column names, and different record ids, depending on which orchestrator ran it. The async
orchestrator now uses the input's pipeline hash, and its names and record ids are the ones
the sync orchestrator has always produced. Nothing a sync run produces changes, and graphs
where these operators read a source directly are unaffected.

Every operator's `async_execute` now requires the keyword argument `input_pipeline_hashes`,
one pipeline hash per input, as the operator node always passes it; a missing argument raises
`TypeError`. The operators that use the hashes (`Join`, `MergeJoin`, `Batch`, `GroupBy`,
`PolarsFilter`, and any operator on the default barrier path) raise `ValueError` when the count
does not match the inputs; the per-row operators and `SemiJoin` take the argument without
using it. Before, these
operators fell back without it to the schema-only hash, and `Join`'s async path to names built
from the input positions (`::0`, `::1`, ...), neither of which the sync path uses. Code that
calls an operator's `async_execute` directly must pass the inputs' pipeline hashes.

This also fixes ITL-885: a store written by one orchestrator can now be extended by the
other. Before, the function node downstream of such an operator wrote two sets of
system-tag columns into one pipeline table, and the second run failed while storing its
results.

Migration: stores written by v0.2 are not affected, because v0.3 never reads a table
written by v0.2. Only a store written by an async run of an unreleased v0.3 build, for a
graph with `MergeJoin`, `Batch` or `GroupBy` after an operator or a function pod, holds the
old names: a later run on that store fails when it writes the pipeline table of the function
node downstream. Delete that table, or use a fresh store, and run again.

#### Arrow hashing moves to starfix hash format 2: every Arrow-derived hash changes (ITL-910)

**Orcapod now requires `starfix>=0.5.0,<0.6`, and the Arrow hash label moves from `arrow_v0.1`
to `arrow_v1`.** starfix 0.4 (hash format 1) lost information for some inputs, so different
data could hash the same (ITL-848): the lengths of nested lists (`[[[1]], [[2], [3]]]` and
`[[[1], [2]], [[3]]]`), a null struct and a struct whose fields are all null, and Boolean
columns, whose hash also depended on how the table was split into chunks. starfix 0.5 (hash
format 2, prefix `00 00 02`) is lossless; this is a fix, so the old values are not kept.

- **What moves.** Every hash computed from an Arrow table: per-row data hashes, default
  `source_id`s and the `record_id`s and source content hashes built from them, function
  input-data hashes, pipeline base and entry ids, operator and cached-source record hashes,
  invocation hashes, and the hashes of `pa.Table`, pandas `DataFrame` and `Series` values. The
  hash label in front of each (`arrow_v1:000002...`) shows the new format. Pipeline hashes,
  schema hashes, system-tag column names and table paths under the default
  `table_scope="pipeline_hash"` do not move for this reason; they move in v0.3 because of
  the hash label rule (ITL-945, below). Paths that embed a content hash
  (`table_scope="content_hash"`, `CachedSource` tables) move with it. Where a `GroupBy` or
  `Batch` orders members by `record_id`, the order of members can change too.
- **Results stored by 0.2.0 are not reused.** Every table v0.3 writes is at a new path and
  every row names its data context; see "Data context `std:v1:default`" below (ITL-944) for
  what a v0.2 store looks like to v0.3.
- **Now hashable.** `fixed_size_list` columns, at any depth (ITL-927): a source or operator
  carrying one works, and its type is kept. A `fixed_size_list<T>[n]` hashes differently from a
  `large_list<T>` with the same values. pyarrow's canonical extension types (`arrow.uuid`,
  `arrow.fixed_shape_tensor`) hash with their extension name and metadata, so an `arrow.uuid`
  column never hashes like the `fixed_size_binary(16)` column holding the same bytes, and
  `StarfixArrowHasher.hash_schema` keeps the identity of extension columns (ITL-846).
- **Now refused** with `ValueError` naming the column path and type, where starfix 0.4 hashed
  silently: a non-nullable field holding a null; a zero-row table with a `map` column; two
  schema nodes sharing a path (duplicate column names, for example in a `pa.Table` value, or a
  column `"a/b"` next to a struct column `a` with a child `b`). Data of `null`, `map`,
  interval, `string_view` and `binary_view` columns, union and run-end-encoded types are
  still refused, now with `ValueError` instead of `NotImplementedError`. A source built from
  a table with such columns raises `ValueError` naming every refused column and its type.
- Column order and struct field order are still not part of an Arrow hash.

#### A Spiral source's default `source_id` is `<project id>/<dataset>/<table>` (ITL-926)

`SpiralDBTableSource` used the bare table name as its default `source_id`. Tables with the
same name in different projects (the EDI sample projects all share their table names) then
had the same `content_hash`, record IDs and source-info tokens. In one store, a job on one
project read back the other project's pipeline records along with its own (cached results
were never mixed up: they are keyed on the row's data). The default is now the project id,
the dataset and the table name, for example `edi-sample-v1-855632/default/session`.

For a Spiral source built without `source_id`, the `content_hash`, every record ID and every
source-info token change, so pipeline records stored by 0.2 for such a source are not found
again. The rows are recorded once more and no result is recomputed, because function results
are keyed on the data. For example, table `subject` (key `subject_id`, one row) in project
`edi-sample-v1-855632`:

- `source_id`: `subject` becomes `edi-sample-v1-855632/default/subject`.
- `content_hash`: `semantic_v0.1:0380725acb90...e7f4f6` becomes
  `semantic_v0.1:3a659ed610bb...29a93`.
- Row 0's record ID: `18d1efa53fbc52b6a40f406eca35ca81` becomes
  `096c4e22981c52daa2b088bffe729ee7`.

`pipeline_hash` does not change (it depends only on the schema), nor does anything for a
source given an explicit `source_id`. To keep 0.2 identities, pass the old value:
`SpiralDBTableSource(project, "subject", source_id="subject")`. Jobs saved by 0.2 store
their `source_id` explicitly and keep it when loaded.

#### The Delta store keeps every column's Arrow type; rebuild a Delta store written by an earlier v0.3 build (ITL-928)

Delta Lake has no unsigned integer, no `timestamp` in seconds, milliseconds or nanoseconds, no zone other than UTC, and no `date64`, `time` or `duration` type. `DeltaTableDatabase` used to let the Delta writer convert such a column, so a read gave back another type: `uint32` as `int32` (and a value above the signed maximum failed the write), `timestamp[s]`, `[ms]` and `[ns]` as `timestamp[us]` (losing nanosecond digits), `timestamp[us, tz=America/New_York]` as UTC; `date64`, `time` and `duration` columns could not be stored at all. A tag read back with another type changes the record ids computed from it, so on a Delta store a re-run over such a tag wrote the downstream nodes' pipeline rows a second time (a fresh job then read 6 rows for 3).

Now such a column's original Arrow type is recorded in the Delta field's metadata, an unsigned integer is written as the signed integer of the same width bit for bit, and `timestamp[ns]`, `date64`, `time` and `duration` are written as their integer value; every read casts the column back to its recorded type. Nested columns (`list<uint32>`, a struct with a `timestamp[ms]` field) are covered. Columns of every other type, and logical-type columns, are stored as before.

A Delta store written by an earlier unreleased v0.3 build has no recorded types: its tables keep reading back Delta's types, a re-run over such a tag still writes duplicate pipeline rows, rows it already duplicated stay, a function node whose rows mix rows read from it with newly computed rows raises `InputValidationError` naming the column and both types, and appending a `timestamp[ns]` column to one of its tables is refused (the stored `timestamp[us]` differs from the new integer form). Rows with an unsigned column are still appended to such a table's signed column, as before, and read back signed; a value the signed type cannot hold is refused with `InputValidationError` and nothing is written. Nothing is repaired automatically: rebuild such a store by running the pipeline into a new store. Stores holding none of these column types are not affected. No content hash, pipeline hash, record id or table path changes.

#### Logical type identifiers: every built-in type writes one, and all of it is hashed (ITL-935)

A logical type is now identified by its Arrow extension name **and** its Arrow extension
metadata, and the whole identifier is part of the column's data type
(`docs/concepts/logical-type-identifiers.md`). The identifier belongs to whoever implements
the type and changes only when an existing value would be stored or hashed differently.

- **Built-in types write an identifier.** `orcapod.path`, `orcapod.upath`, `orcapod.uuid`,
  `orcapod.file`, `orcapod.directory`, `pandas.dataframe` and `pandas.series` now write the
  extension metadata `{"version":1}` (they wrote none); `orcapod.ndarray` already did.
  `list[T]` and `set[T]` of a logical type (the native containers) have no version of their
  own, by design: their metadata carries the element's name and new metadata, so their
  identifier changes exactly when the element's does.
- **The whole identifier is hashed.** A type hash, a schema hash and a function signature
  used to contain the logical type's name only. They now contain
  `<extension name>;<hex of the metadata>` (`LogicalTypeIdentifier.text`). The hashed cell
  of a value-hashed type (UUID, file, directory, array, pandas) starts with the same text,
  where it used to start with the name alone, written with `:` for `.`.
- **Fixed: `[]` and `[None]` hashed the same in a list or set of a value-hashed type.** The
  hashed cell of a `list[T]` / `set[T]` column (T a UUID, file, directory, array or pandas
  type) joined its element hashes with a separator and wrote nothing for a null element, so
  an empty list, `[None]` and `[None, None]` shared one hash. The cell now holds the element
  count and, per element, a null marker or a length-prefixed element hash.
- **What moves, once, in v0.3.** For every node whose schema or function signature has a
  column of a logical type (including dataclass and pydantic columns): its schema, function
  and pipeline hashes, hence the paths of its stored tables, and the hashes of the rows that
  hold such values. Together with the other v0.3 hash changes this means no result stored by
  v0.2 is reused; nothing stored is changed or deleted. **Nodes with only plain types are
  not affected by this change.**
- **Reading.** A stored column is read only if its stored metadata is one the registered
  type declares readable (`readable_extension_metadata()`, new on `LogicalTypeProtocol`,
  with a default in `BaseLogicalType`). Otherwise `LogicalTypeIdentifierError` (a
  `ValueError`) names the column, the stored identifier and what this installation reads.
  Before, a column read as plain storage was wrapped as the registered type whatever its
  stored metadata said.
- **Data written before v0.3 stays readable.** The seven types above read columns stored
  with empty metadata as version 1 (the layout is the same), for example a Delta table
  written by v0.2 and opened as a source.
- **For authors of logical types.** `make_arrow_extension_type` takes `also_reads`: older
  metadata values the type still decodes. An extension name may not contain `;`
  (`register_logical_type` raises `ValueError`). A custom logical type that does not derive
  from `BaseLogicalType` must implement `readable_extension_metadata()`.

#### Wrong hashes fixed: values that collided or depended on data, process or spelling (ITL-942)

A hash-site audit (ITL-938) found hashes that were **wrong**: two different things shared a
hash, or one thing hashed differently depending on its data, the process or how it was
written. A wrong hash is a bug that overrides hash stability, so these values move. Each fix
changes only the values that were wrong.

| Fix | Before | Now | What moves |
|---|---|---|---|
| Batch / GroupBy source-id fold | members concatenated with no separator: `["ab", "c"]` folded like `["a", "bc"]`, and a `None` member like `""` | SHA-256 of the canonical JSON list of members, in order | the `_tag_source_id` value of every row after a `Batch` or `GroupBy`, and the entry ids downstream of it. Column names and paths do not move. Source-info columns are unchanged |
| Arrow hashing of nested types | the hashed type was rebuilt from the first non-null value, so nested nullability counted for an empty or all-null column and not for a column with data | the hashed type is the declared type: struct and list fields keep their nullability, at every depth, with or without data | row hashes of struct or list columns with non-nullable nested fields, and of all-null or zero-row `Path`, `File`, `Directory` and `UUID` columns. Key values only |
| Set order in structure hashes | items sorted by `str()`, so `{1, "1"}` hashed differently per process (`PYTHONHASHSEED`) | items sorted by their canonical JSON text | structure hashes of sets whose `str()` order differs from their JSON order (mixed types, sets of tuples) |
| Dict keys in structure hashes | keys turned into `str()`: `{1: "x"}` equalled `{"1": "x"}`, and `{1: "a", "1": "b"}` lost a key | a dict with only `str` keys is unchanged; any other dict hashes as a sorted list of `[key, value]` pairs | structure hashes of dicts with non-`str` keys |
| Dicts that spell out an encoding | `{"__type__": "tuple", "items": [1, 2]}` hashed like `(1, 2)` (also set, namedtuple, generic alias, union) | only the hasher and type handlers write `"__type__"` objects; a dict with a `"__type__"` key hashes as a pair list | structure hashes of dicts with a `"__type__"` key. Handlers that returned a plain `{"__type__": ...}` dict must return `orcapod.hashing.semantic_hashing.Encoded(tag, fields)` |
| Namedtuple field order | fields sorted by name: `Point(x=1, y=2)` hashed like a `Point` declared `(y, x)` holding `y=2, x=1`, although the tuples differ | fields hashed in declared order | structure hashes of namedtuples |
| Column selection operators | `SelectTagColumns`, `SelectDataColumns`, `DropTagColumns`, `DropDataColumns` hashed `["x"]`, `("x",)`, `{"x"}`, `["x", "x"]` differently, and `["y", "x"]` unlike `["x", "y"]`, although the output is the same | identity is the set of names, hashed as a sorted list | pipelines that spelled the columns as a tuple, a set, or an unsorted or repeating list: their hashes **and table paths** move. Sorted lists (the usual spelling) keep their hashes |
| Record-id system tag type | `fixed_size_binary[16]` out of a source, `large_binary` after a `Tag`, so one row had two operator-cache keys (`cache_mode` LOG/REPLAY, sync run versus async orchestrator) | `large_binary` everywhere | operator-cache and cached-source row keys, and the content hash of a raw stream holding source system tags. On Delta Lake the stored column type does not change: Delta has a single binary type |
| Default function version | `v0.0` through `@function_pod`, `v1.0` through `FunctionPod.from_fn` and `side_effect_function_pod` | `v0.0` for every entry point | **table paths** of pods built with `FunctionPod.from_fn` or `side_effect_function_pod` without an explicit version: from `.../v1/...` to `.../v0/...` |
| Value-hashed logical types with no value (follow-up to the nested-type fix, found on the combined v0.3 branch, ITL-967) | a column of a value-hashed logical type (UUID, `File`, `Directory`, array, `DataFrame`, `Series`, and lists or sets of them) that had zero rows or held only nulls hashed like a plain `large_binary` column and like every other such type | the hashed field keeps the logical type's identifier (extension name and metadata), with or without values | the Arrow hash of every table with such a column, with data too: row data hashes, default `source_id`s and what derives from them |

**Migration.** A pod built with `FunctionPod.from_fn` or `side_effect_function_pod` that relied
on the old default keeps the major-version component of its paths (`.../v1/...`) by passing
`version="v1.0"`. That alone does not make v0.3 find tables written by v0.2: every v0.3 path
also differs in its hashes and its layout component (ITL-945, ITL-944). A pipeline that spelled
selected columns as a tuple or a set gets new tables for that operator and everything
downstream; no stored row is changed or deleted. Every other fix changes values only, never a
path or a stored schema.

Reviewed and kept by decision: `MergeJoin(m1, m1)` (a stream joined with itself) has a
different pipeline hash from `MergeJoin(m1, m2)` with the same schema, because a pipeline hash
covers the pipeline's structure; two inputs with the same source identity count as one.
Function default values in the function-value hash are tracked in ITL-953. See
`docs/reference/hashing.md`.

#### Data context `std:v1:default`: a `__data_context` column on every table, and every table at a new path (ITL-944)

**v0.3 writes every stored table at a new path, and records on every row which generation of
hashing wrote it.** A store written by v0.2 is not migrated and not modified: v0.3 does not
read its tables as results, and computes every input again into its own tables.

- **One data context key for the whole of Orcapod's hashing.** The default data context is
  now `std:v1:default` (manifest `orcapod/contexts/data/v1.json`). The version increments
  when, and only when, a released hash value changes, for any reason (ITL-946). Everything
  stored before v0.3 (rows that carry `std:v0.1:default`, and tables without the new column)
  is the earlier generation. A manifest is frozen at the
  release of the library version that contains it, and may be edited in place only while
  that release is prepared: `orcapod/contexts/released_manifests.json` pins the digest of
  each released one without its `golden` history, and a test fails when a pinned manifest is
  edited (ITL-946). `v1.json` is pinned when v0.3.0 is tagged.
- **Manifest contents.** Besides the wiring of the hashers, a manifest now has
  `released_in` (the library version that froze it; `null` while open), `previous` (the data
  context it follows), `definitions` (a name for each definition that lives in code),
  `libraries` (the libraries the core hashes depend on, with versions), `golden` (the folder
  and the append-only history of the golden hash files, ITL-946) and `changes` (what differs
  from `previous`).
- **Logical types moved out of the manifest.** The registered logical types and the value
  handlers that hash their Python values are now in
  `orcapod/contexts/logical_types.json`, which is not frozen: a logical type is versioned by
  its own identifier, not by the data context. What the default context registers is
  unchanged. `JSONDataContextRegistry` and `contexts.create_registry` take
  `logical_types_file` to use another registration file; a manifest that lists its logical
  types inline is still built as written.
- **New column `__data_context`** (`large_string`) on every row of the function pipeline
  table, the function result table, the operator-cache table, the cached-source table and
  the side-effect invocation log. It **replaces** `__input_data_context_key` in the pipeline
  table and `_context_key` in the result table. In memory nothing is renamed:
  `ColumnConfig(context=True)` still shows `_context_key`.
- **New column `__pf_var_function_version`** in the function result table: the full version
  string the function was declared with (`"v3.7"`). Only the major part is in the table
  path, and until now no stored column held the version.
- **New table layouts, so new paths.** The last component of every table path moved:

  | Table | v0.2 | v0.3 |
  |---|---|---|
  | Function pipeline table | `…/schema:<pipeline hash>/pdb_v1` | `…/schema:<pipeline hash>/pdb_v2` |
  | Function result table | `…/_result/<function uri>/rdb_v1` | `…/_result/<function uri>/rdb_v2` |
  | Side-effect invocation log | `…/schema:<pipeline hash>/tdb_v1` | `…/schema:<pipeline hash>/tdb_v2` |
  | Operator-cache table | `<operator uri>/schema:<pipeline hash>` | `<operator uri>/schema:<pipeline hash>/odb_v1` |
  | Cached-source table | `source/node:<content hash>` | `source/node:<content hash>/sdb_v1` |

  The layout component is always the last one: `CachedSource(..., cache_path=("my", "cache"))`
  now stores at `("my", "cache", "sdb_v1")`, and `CachedSource.cache_path` returns that full
  path. Pass the path without the layout component as `cache_path`.
- **Reads and lookups use rows of the current data context only.** A result lookup matches
  on the input hash and the data context; every read of a whole table (a node's `as_table()`,
  `iter_data()`, `get_all_records()`, `as_source()`, `get_all_cached_outputs()` of a cached
  function, a stored operator node, a `CachedSource`) leaves rows of another data context
  out, leaves them unchanged, and logs one warning per table with their number (logger
  `orcapod.contexts.stored`).
- **What a v0.2 store looks like to v0.3.**
  - Its tables are at the old layout. v0.3 never reads them as results and never writes to
    them; they stay byte for byte as v0.2 left them and stay readable with v0.2.
  - The first v0.3 run **computes every input again**, also for functions and data that did
    not change, and stores the rows in the new tables. Before that run, reading a node
    returns no rows.
  - When v0.3 finds a table at an old layout, it logs one warning with the number of rows:
    `Found 3 rows in stored table 'my_job/.../pdb_v1' written by an earlier version of
    Orcapod (older table layout, data context std:v0.1:default). They are not used and are
    left unchanged; they can be migrated later.`
  - **Side-effecting nodes fail instead of delivering again.** A side effect is an external
    action, and a completion stored by v0.2 cannot be matched with current inputs. Both
    kinds of side-effecting node are covered: a side-effect pod (`@side_effect_pod`,
    `@sink_pod`, `@tap_pod`), whose completions are in its invocation log, and a function
    pod that takes the invocation context (`@function_pod(ctx_arg=...)`,
    `@side_effect_function_pod`), whose completions are in its function tables. Such a
    node that finds its tables at an old layout (or completions of another data context in
    its current tables) raises `orcapod.errors.SideEffectLogDataContextError` (a
    `StoreError`, so it fails the run whatever the `error_policy`) before it delivers or
    writes anything. The error names the tables. Other function, operator and source nodes
    of the same job are not refused. Keep using v0.2 for the node. A fresh store is not
    a safe way around the error: it holds no completions, so on it the node performs every
    side effect again; use one only when a repeated delivery is acceptable. The tables of
    an earlier version are found by listing the store (ITL-954): on a store that cannot
    list its tables the node logs one warning that it could not check, and runs, so the
    refusal cannot be relied on there.
  - `orcapod.contexts.stored_data_contexts(store, record_path)` counts the rows of a table
    by data context, for example `{"std:v1:default": 3}`; a table without the column reports
    its rows under `std:v0.1:default`.
- **`std:v0.1:default` is no longer shipped.** `orcapod/contexts/data/v0.1.json` is removed
  (it will be kept in `orcapod-sanctuary`), so `resolve_context("std:v0.1:default")` and
  `resolve_context("v0.1")` raise `ContextResolutionError` naming it as the data context of
  Orcapod before v0.3. A pipeline or job file saved by v0.2 still loads: each node that
  names `std:v0.1:default` is loaded under the current data context with a warning, and
  shows no rows until v0.3 has run (its v0.2 tables are at the old layout).
- **Removed: the v0 to v1 migration machinery.** `orcapod.errors.SchemaVersionError`,
  `NodeConfig(ignore_schema=...)` (and `NodeConfig.ignore_schema`), the package
  `orcapod.migrations` (`migrate_pipeline_db`, `migrate_result_db`, `MigrationResult`), the
  command `orcapod migrate` (`pipeline-db`, `result-db`), `ResultCache.base_record_path`,
  `ResultCache.set_ignore_schema` and `CachedFunctionPod.set_ignore_schema`. They converted
  tables without a layout component to `pdb_v1` / `rdb_v1`, which v0.3 does not read. A
  table without a layout component is now counted and reported like any other old layout,
  where it used to raise `SchemaVersionError`. Remove `ignore_schema` from your code; the
  migration to the v0.3 layouts is a later tool (ITL-913, ITL-914).
- **An input of another data context is refused.** A node, cache or cached source that
  would store a row for an input whose data context differs from its own raises
  `InputValidationError` naming both keys, before anything is computed, delivered or
  written. For a function node this is checked for the whole input, so it fails the run
  whatever the `error_policy`. Only one data context is shipped, so this needs a custom
  context registry to happen. It applies to nodes, caches and cached sources used
  directly. Inside a `PipelineJob` it is not reached: a job wraps every input in a source
  node of the default data context, so an input of another data context is not seen as
  such there (a known gap, ITL-951). `ResultCache.store` refuses an output of another
  data context in the same way.
- **A full data context key must match its manifest.** `resolve_context` selects a manifest
  by the version part of a key, and used to return it whatever the rest of the key said
  (`"custom:v1:production"` gave `std:v1:default`). A full key that is not the manifest's
  key now raises `ContextResolutionError`.
- **Hash values.** The content hash of a stream that has no producer (a bare
  `ArrowTableStream`) covers its `_context_key` column, so it changes with the key. No other
  hash value changes because of this entry.
- **Removed (introduced earlier in this unreleased cycle, never released):**
  `orcapod.hashing.stored_hash_labels` and `orcapod.errors.SideEffectLogHashLabelError`.
  Rows are told apart by `__data_context`, not by the label inside a hash.
- **Internal API.** `ResultCache.lookup`, `ResultCache.store` and
  `ResultCache.get_all_records` take the caller's `data_context_key`.

  **Migration:** none is needed to run the function, operator and source nodes of a v0.2
  store under v0.3, at the price of computing everything again; side-effect nodes fail
  on such a store, unless the store cannot list its tables (then the node warns and
  runs), and on a fresh store they perform every side effect again. A tool that converts old-layout tables is planned (ITL-913, ITL-914); until
  then they are kept, untouched, for it.

#### Hash label rule: labels renamed, out of hash inputs, bare digests inside names (ITL-945)

**Every structure hash, every table path and every key moves in v0.3.** One rule now holds
for every hash label (developer, November 11, 2026; `docs/reference/hashing.md`, "The hash
label rule"):

- **Labels are renamed to `<algorithm>_v1`.** `semantic_v0.1` becomes `semantic_v1`, the Arrow
  label is `arrow_v1` (it was `arrow_v0.1` in v0.2), `merkle_sha256` becomes `directory_v1`;
  `ndarray_v1` and `polars_expr_v1` keep their names; the file content hash keeps `sha256`.
  A label names the algorithm that produced a digest and changes only when that algorithm
  would give an existing input a different digest.
- **A label never enters a hash computation.** A nested hash now contributes its digest only:
  as `{"__type__": "hash", "digest": "<hex>"}` inside a structure hash, as
  `b"<extension name>;<hex metadata>::" + digest` for an extension-typed cell inside an Arrow
  hash (the name and metadata are the logical type identifier of ITL-935), and as the
  raw digest in the entry-id and operator-node record-hash preimages and in the run snapshot
  hash. Renaming a label later changes no digest. Until now `"<label>:<hex>"` was hashed, so
  this moves every `content_hash()` and `pipeline_hash()` whose structure holds a nested hash
  (types, schemas, pods, streams), every schema hash and so every system-tag column name,
  every Arrow hash over a logical-type cell, every pipeline base and entry id, and the run URI.
- **Hashes inside names are bare hex digests.** The function `uri` (its second component), the
  path components `schema:`, `instance:` and `node:`, and the slot name of a `SourceNode`
  built from a stream no longer hold a label. A source id is the exception: it is the
  source's name, so the default `source_id` of a `DerivedSource` (`derived:<label>:<hex>`)
  and of `node.as_source()` keeps the hash's label, now `semantic_v1`. A v0.3 function table is at
  `<job>/<function>/<64 hex>/v<major>/python.function.v0/schema:<64 hex>/pdb_v2`.
  `data_function.uri[1]` and `output_data_schema_hash` return the bare digest.
- **Hashes stored as values carry their label**, now including the record id (`_record_hash`)
  of operator-node and cached-source tables: `b"arrow_v1:" + digest` instead of hex text.
- **A producer-less `ArrowTableStream`** no longer hashes its derived `_content_hash` column
  into its content hash (the column's values carry the Arrow label).
- **`BasicDirectoryHasher(algorithm=...)`** raises `ValueError` for any algorithm but
  `"sha256"`: `directory_v1` names the SHA-256 procedure.
- **Each label is defined once, in code**: `SEMANTIC_HASH_LABEL`, `ARROW_HASH_LABEL`,
  `DIRECTORY_HASH_LABEL`, `NumpyArrayHandler.HASH_LABEL`, `POLARS_EXPR_HASH_LABEL`. The data
  context file no longer carries `hasher_id`; `hasher_id` remains a constructor argument of
  `SemanticAwarePythonHasher` and `StarfixArrowHasher` and defaults to the label.
- **Unchanged:** `ContentHash` and all its methods (`to_string`, `to_hex`,
  `to_prefixed_digest`, `from_string`, `from_prefixed_digest`); the digests of plain Arrow
  tables and schemas, of flat structures of primitives, and of ndarray, Polars expression,
  file and directory values (their label text changes, their digest does not);
  `InvocationContext.invocation_hash`, which keeps its `<label>:<hex>::<label>:<hex>` shape
  with the new label names; record ids.
- **Every v0.3 table path differs from v0.2's.** For the same node, v0.2 and v0.3 agree on
  the job name, the function or operator name, the major version and the wrapper kind, and
  differ in the table layout component (ITL-944) and in every hash inside the path: the
  output-schema hash of a
  function (`semantic_v0.1:<hex>` became another, bare, digest), `schema:<pipeline hash>`,
  `instance:<content hash>` and `node:<content hash>`. Nothing in v0.3 that looks for older
  data at its own table path can find a table written by v0.2. Tables written before v0.3 are
  therefore found by name, with a scan of the store under the job and function or pod name
  (ITL-954), and a node that delivers side effects raises `SideEffectLogDataContextError`
  when one is found (ITL-944).
- **Migration.** None is needed beyond the v0.3 step already described: a store written by
  v0.2 is an older generation whose rows v0.3 does not use. Function, operator and
  cached-source tables are at new paths, so their v0.2 tables are left untouched. Code that
  compared `uri[1]` or a path component with `content_hash().to_string()` must compare with
  `to_hex()`. A job file saved by v0.2 holds the old labels and paths; loaded by
  v0.3, its nodes point at no v0.3 table and show no rows.

#### Function hashes: structured signature, default values through the semantic hasher (ITL-953)

A function's signature is now hashed from one structured description: each parameter's name,
kind, annotation and default value, in declared order. Before, the parameters were one line of
text in which a default appeared as `repr(value)`.

| | Before | Now |
|---|---|---|
| Default values | `repr(value)`: a `frozenset` or `dict` default hashed differently per process (`PYTHONHASHSEED`) or per insertion order, an `object()` default embedded a memory address, and two values with one `repr()` shared a hash | the semantic hasher's hash of the value, like every other hashed value |
| Parameter kind | not hashed: `f(a, *, b)`, `f(a, /, b)` and `f(a, b)` shared a hash | hashed |
| Stored `function_signature_hash` | hash of a separate signature text | the hash of the function as a value: one description of a signature |

**What moves.** The hash of every function used as a value, and every newly written value of
the stored column `__pf_var_function_signature_hash`. Nothing reads that column, and stored
rows are not changed. A function pod's identity does not use this hash: no `uri`,
`pipeline_hash()`, `content_hash()` or table path moves.

**A default value the hasher cannot hash is now an error.** A function pod whose function has
a default of a type with no hash rule fails when it is created (`@function_pod`,
`FunctionPod.from_fn`, `PythonDataFunction`), with a `TypeError` naming the parameter and the
function. There is no fallback to `repr()` or to the type name, which would let two different
defaults share a hash. Hashable defaults: `None`, `bool`, `int`, `float`, `str`, `bytes`,
`uuid.UUID`, lists, tuples, sets and dicts of hashable values, functions, types,
`numpy.ndarray`, `pandas` objects, `File`, `Directory`. Not hashable today: a plain
`enum.Enum` member, a dataclass or pydantic instance, `pathlib.Path`, `datetime`, `date`,
`Decimal`, numpy scalars other than `numpy.float64`. Giving these a hash rule (a pod
parameter's default hashed as the column value it would be) is the follow-up ITL-971.

**Migration.** Use a default the hasher can hash (for example a `str` or `None`, building the
object inside the function), remove the default and supply the value as an input column, or
register a `PythonTypeHandlerProtocol` for the type. A custom `FunctionInfoExtractorProtocol`
that wants its defaults hashed returns `parameters` entries whose `default` is a
`ParameterDefault`.

#### Default source ids built from a hash say what kind of source they name (ITL-957)

A source id is the source's name, so it should make the source easy to identify (developer,
November 11, 2026). A default id built from a hash now has the form
`<source kind>:<algorithm label>:<hex>`. The one string is used unchanged as the `source_id`
system tag value and at the start of every `_source_<column>` value.

- **In-memory sources with no `source_id`.** `ArrowTableSource`, `DictSource`, `ListSource`
  and `DataFrameSource` were named by the bare hex of the table hash (`000002fe…`). They are
  now named `arrow_table:arrow_v1:<hex>`, `dict:arrow_v1:<hex>`, `list:arrow_v1:<hex>` and
  `data_frame:arrow_v1:<hex>`. The kind is the class's config type key and the label is the
  one the hash carries. `hashing.path_n_char`, when set, still shortens the hex, and only
  the hex.
- **`node.as_source()`.** `FunctionJobNode.as_source()` and `OperatorJobNode.as_source()` no
  longer build `<node path>:<label>:<hex>`. They take the `DerivedSource` default,
  `derived:semantic_v1:<hex>` of the node's content hash, the same id as
  `DerivedSource(node)`.
- **What moves** for an in-memory source with no `source_id`: the `source_id`, the values of
  its `_tag_source_id::…` column, its record ids, its `_source_<column>` values, the name of
  the source in a job, and the content hash of the source (not for `ListSource`, whose
  content hash does not include the source id) and of everything downstream of it. No table
  path holds a source id; paths that hold a content hash (a `table_scope="content_hash"`
  node, the default path of a cached source) move with that hash. Pipeline hashes, schema
  hashes, column names, data hashes and the paths of function tables do not move, and a
  function's stored result for the same input data is still found.
- **What moves for `as_source()`:** the `source_id` and the name of the source in a job, and
  nothing else: its rows are the node's stored records and its content hash is the node's.
- **Not changed:** a `source_id` you give is used exactly as given. Sources whose default is
  not a hash (CSV, Delta, database tables, SpiralDB, cached and polling sources) keep their
  defaults.
- **Migration:** to keep a name stable across releases, pass an explicit `source_id`. As
  rule 5a of the hash label rule states, a default id holds the hash's label, so a later
  rename of a label would move default ids and what derives from them.

#### Your own classes come in two kinds: exact and structural (ITL-972)

Orcapod 0.3 identifies a class you define by its kind, its name and its fields' names and
types, in declared order. The kind is chosen where the class is defined.

- **An ordinary `@dataclass` or a pydantic model is identified by its exact class.** Its
  name is the module path and class name (`pkg.models.Point`; `__main__.Point` for a class
  defined in a notebook or script). Two different classes are never the same type, even
  with the same name and fields: a pod built for a class refuses the class defined again
  (a re-run notebook cell), and the error names the column. A column of a subclass is
  accepted where its base class is expected, as before.
- **A class made with the new `@op.dataclass` is structural** (see "Added"): identified by
  its name and fields, whatever class object or module it is.
- **Hashes change for every pipeline that uses a dataclass or pydantic model.** The
  class's logical type name and Arrow extension name become
  `<name>@<SHA-256 of kind, name and fields>`, and the extension metadata holds that
  description; the stored struct lists fields in declared order, as before. Every schema
  hash, pipeline hash, table path, system-tag column name and data hash that involves such
  a value moves. The first run under 0.3 computes those pod and operator results again,
  into new tables. Tables written by earlier versions are left untouched and are not read
  by 0.3 pipelines; reading such a column directly raises `TypeIdentityError`. There is no
  migration tool. Results that involve no such value are unaffected.
- **Changing a class's fields makes a new type.** Adding, removing, renaming, retyping or
  reordering fields all count. Defaults, methods and pydantic computed fields are not part
  of the identity. The new type's pods get new tables; the results of the old definition
  stay where they are. Before, a changed class shared the old tables and a run returned 0
  rows with only log warnings.
- **A value is stored only as its own class.** Storing a value in a column of an ordinary
  dataclass or pydantic model requires an instance of exactly that class; a subclass
  instance or an unrelated object with the same attributes raises `TypeError`. Before,
  any object with the right attributes was stored, and a subclass instance lost its own
  fields. This holds for a pod's output read in memory as well: its columns are
  converted as the pod's declared output types, where the types were taken from the
  values before.
- **A stored shape that disagrees with the class raises.** Reading a stored column of an
  ordinary dataclass or pydantic model finds the class at its module path. If the class
  there has other fields, `DeltaTableDatabase` raises the new
  `orcapod.errors.TypeIdentityError` naming the column and both sets of fields. It is not
  a `StoreReadError`: the table is intact and the class differs. Like a store error, it
  fails the run under every `error_policy`, with both orchestrators: a stored row that
  cannot be decoded is never skipped as a failed row. If no class is found, reading raises
  `ImportError` naming the column, the class and its stored fields.
- **A subclass is its own type.** A dataclass or pydantic subclass was stored as its
  parent when the parent was registered first: its extra fields were silently dropped from
  stored data, and the pod's hash and table path depended on the registration order. It
  now has its own identity and keeps all its fields, in any registration order.
- **Recursive classes** raise `RecursiveTypeError` (a `TypeError`) naming the field path,
  for example `Node.next -> Node`.

#### NumPy arrays: stored as `.npy`, hashed strictly, with a per-type ignore list (ITL-981)

**This replaces the array storage and the array hash described under "NumPy arrays: new
storage format and new hashes" (ITL-888).** That format (the extension type
`orcapod.ndarray` over a `{dtype, shape, data}` struct, with a hash that ignored memory
layout and byte order) was never released; v0.3 ships the one described here. What that
entry says about v0.2 stores still holds: they are not migrated and not read.

The developer decided on November 12, 2026 that storage and hashing are separate, that the
hash is strict by default, and that what the hash ignores is configured per type.

- **Storage.** An array column is the Arrow extension type `orcapod.numpy.ndarray` over
  `large_binary`. Each cell is one `.npy` file written by NumPy
  (`numpy.lib.format.write_array`, format version 3.0, `allow_pickle=False`) and read with
  `allow_pickle=False`. An array reads back as it was given: dtype including byte order,
  shape, element bits, C or Fortran memory order and structured field offsets and item size
  are kept. Not kept: strides other than C or Fortran order (a step slice is stored
  C-contiguous), the subclass (read back as plain `np.ndarray`), the `writeable` flag, dtype
  `metadata` and the `align` flag of a structured dtype. Padding bytes between structured
  fields are not part of the value and are written as zero, so residual process memory
  never reaches a store. A cell that does not hold exactly the element bytes its `.npy`
  header declares is refused on read.
- **Hash.** The hash is defined on the value, never on the stored bytes, and keeps the label
  `ndarray_v1`. It is `SHA-256(lp("orcapod.numpy.ndarray") || E || memory order || byte
  order || structured layout)`, where `E` is the element digest of dtype, shape and element
  bits (`docs/reference/hashing.md`, section 8, with a reference implementation and pinned
  vectors). By default a C-ordered and a Fortran-ordered array, a little- and a big-endian
  array, and a packed and an aligned structured array with equal values now hash
  **differently**. What storage does not keep never changes the hash, so a value read back
  from a store hashes like the value written.
- **Ignore list.** The three properties `memory_order`, `byte_order` and `struct_layout`
  can be ignored. The list of ignored names is set per type in the data context
  (`hash_ignore` in `contexts/logical_types.json`; default: empty) and is written into the
  extension metadata, `{"ignore":[...],"version":1}`. Two lists are two logical types:
  different schema hashes, no shared type in a join, and a data context refuses to read a
  column stored under another list (`LogicalTypeIdentifierError`, naming the column and both
  lists). A bare value is hashed with the data context's list, a cell with the list of its
  own column. v0.3 has no user API to change the list.
- **Newly refused at write** (`UnsupportedValueError`, naming the column and the dtype): a
  structured dtype whose fields overlap or are not in offset order (`.npy` has no form for
  it; the previous format accepted it), and a dtype whose `.npy` header exceeds 10,000 bytes
  (several hundred fields). Masked arrays, object dtypes, `StringDType`, `longdouble`,
  unit-less datetimes and field titles are refused as before.
- **`MergeJoin`** orders the arrays it merges by their value hash (it used the stored dtype
  tag, shape and bytes).
- **What moves.** Every array value hash, and with the new extension name and metadata the
  type, schema and pipeline hashes of anything with an array column and the system tag
  column names built from those schema hashes. Nothing released is affected: v0.3 is not
  tagged, and v0.2 stores are not read by v0.3.

#### Showing a stream never computes it (ITL-991)

Displaying a stream, source or node in a notebook no longer computes anything. The preview
shows the first 10 rows that are already available: rows a source holds, rows cached from an
earlier computation, rows a job node holds after a run or has stored, or a bounded read of a
source's table. Otherwise it shows the columns and a note such as "not computed" or "nothing
stored; run the job". Call `as_table()` or run the job to compute. Before, displaying a
function pod's output called the function on every row, displaying an operator's output ran
the operator, displaying a side-effect stream delivered every side effect, and displaying a
`CachedSource` wrote its cache table; none of these happen now.

`StreamBase.view()` and `StreamView` are removed: use `head(n)` or `as_polars_df()`.

#### `map_tags` and `map_data` keep unmapped columns by default (ITL-1027)

The fluent `stream.map_tags(...)` and `stream.map_data(...)` methods defaulted to
`drop_unmapped=True`, so renaming one tag silently dropped every other tag. They now default to
`drop_unmapped=False`, as the `MapTags` and `MapData` operator classes always did. To get the
old behaviour, pass `drop_unmapped=True`. `drop_unmapped` is part of the operator's identity: a
call that passes `drop_unmapped=True` explicitly hashes exactly as before; a call that relied
on the old fluent default now hashes as the keep form.

#### pandas values storage would not restore are refused at write (ITL-1054)

A pandas `DataFrame` or `Series` that storage would change, or could not read back, is now
refused when it is written, with a `ValueError` that names the column, index level or
column-axis level and the reason. The value hash refuses the same values with the same message,
through one shared check. These values used to be written:

- **Written, then read back changed, with no error:** a MultiIndex whose level names are also
  level positions (`names=[1, 0]`; `1.0` and `True` count as `1`). pyarrow wrote it with the
  levels swapped, and the hash did not tell the two apart.
- **Written, then failed to read back:** a `pd.ArrowDtype` column or Series of a list, large
  list, fixed-size list, list view, struct, map, dictionary, run-end, union or fixed-size binary
  type, or of an Arrow extension type (`uuid`, `json`, `bool8`, a fixed-shape tensor). An index level of a `pd.ArrowDtype` list, large list, fixed-size list, struct, map,
  dictionary, fixed-size binary or `float16` type. A column axis (any level) that is
  categorical, period, timedelta, interval, `datetime.date` objects, or a `pd.ArrowDtype`
  timestamp, duration, time, bool, date, decimal or unsigned integer. This includes a Series named with a
  `pd.Timedelta`, `pd.Period` or `datetime.date`. A categorical column, Series or index
  whose categories are tuples (ITL-1115) or intervals. A `pd.ArrowDtype` `time64[ns]` index
  level holding a value that is not a whole number of microseconds (ITL-1118), and a
  MultiIndex level holding tuples or lists (ITL-1120); index levels are now converted once, in
  full, at write. In a pipeline, such a result stayed in the result
  table and could not be loaded on any rerun.
- **Written, then read back with another column label, with no error:** the label `False`
  (a one-column bool axis, a Series named `False`, or a `False` in a level of a MultiIndex
  column axis) read back as `True` (ITL-1116); an index level named like pyarrow's generated
  field names (`__index_level_0__`) read back unnamed (ITL-1119).

A value Arrow cannot convert (for example `complex128` or a sparse column) now raises
`ValueError` naming the column, where it used to raise `ArrowNotImplementedError`, `TypeError`
or `NotImplementedError`.

No hash of a value that is still accepted changes, and the logical type identifiers are
unchanged. Rows already stored before this change are not migrated: a value of the first kind
cannot be told apart from a correct one, and a value of the second kind cannot be read. The
migration tool reports both (ITL-1048).

#### The pandas ignore option `struct_field_order` is removed (ITL-1054)

`struct_field_order` is no longer an option of `orcapod.pandas.dataframe` or
`orcapod.pandas.series`. It acted only on `pd.ArrowDtype` struct columns, which are now
refused. A data context that names it under `hash_ignore` is refused with the unknown-option
error, and so is `ignore=["struct_field_order"]` on the logical type or its handler. No shipped
data context named it, so no identifier or hash moves. A column stored on unreleased `main`
under a custom data context that named it is refused on read (`LogicalTypeIdentifierError`);
such results are recomputed under a data context without it. The pandas types are provisional until
the v0.4 audit (ITL-1069). That audit is where struct columns and this option come back
together, if storage learns to restore them. The Polars types keep their `struct_field_order`
option.

#### `GroupBy` keeps only its `by` tags (ITL-955, ITL-1070)

`GroupBy` used to turn every tag that was not a grouping key into a list-valued **data** column,
with null source info, where a function pod could read it. That broke the rule that separates an
operator from a function pod: no data element may come from a tag value. A tag that is not in
`by` is now **dropped**: a list of the members' values is not a key for the group, just as SQL
drops a non-key, non-aggregated column after `GROUP BY`. The output's tag columns are the `by`
columns, scalar, in the input's tag order. `Batch` is unchanged: it has no key and keeps every
tag, list-valued.

| `GroupBy(by=["subject"])` on tags `subject, session, probe`, data `path` | v0.2.0 | Now |
|---|---|---|
| Tag columns | `subject` | `subject` |
| Data columns | `session: list[int]`, `probe: list[str]`, `path: list[str]` | `path: list[str]` |

**Member tag values are not carried.** If a later step needs to know which member each element
came from, make the column data *before* grouping: declare it as data at the source, or use
`DerivedSource(origin, tags_as_data={"probe": "probe_name"})` and group that.

- **What to change.** A function that took a non-key tag as a parameter
  (`def f(probe: list[str], path: list[str])`) now fails input-schema validation; drop the
  parameter, or offer the tag as data before grouping as above. A `select_data_columns` that was
  only there to drop the moved tags can go.
- **Unchanged.** `by` must still name scalar tag columns. Members are still ordered by the
  input's non-key tag values, then by record id, so every data list holds the same elements in
  the same order; groups are emitted in key order. The Arrow types of the list columns, the
  folded system tags and the data columns' list-valued source info are the same as before. The
  all-null `_source_<tag>` columns of the moved tags are gone.
- **Grouping again.** A grouped stream can be grouped again on a coarser key; its data lists
  nest (`list[list[T]]`). The second grouping orders its members by the first grouping's keys
  that are not in the coarser key, then by record id; a tag the first grouping dropped no
  longer takes part.
- **Hashes and stored results.** No pipeline hash, content hash or table path moves: the
  `GroupBy` stream hashes its operator and its input, not its output columns. Against v0.2.0,
  each grouped row's data hash changes where a tag was dropped, because the row's data no longer
  includes it; a grouping on every tag is byte-identical. A function whose input was already
  only the real data columns (`GroupBy` then `select_data_columns`) receives the same data and
  stores the same columns as with v0.2.0, so it reuses its stored results without computing
  again. A function whose signature has to change is a different function and computes once.
  `node.as_source()` over a grouped node has a new `pipeline_hash`, because a source's pipeline
  hash is its schema.
- **Stores written by a development build.** Between November 12 and this change, a
  development build kept non-key tags as list-valued tags. A store it wrote that holds a
  `GroupBy` over a non-key tag keeps that tag column in the operator cache and in the tables of
  the function nodes after it, at the same paths. With this release, writing to such a table
  fails with `StoreWriteError` (a non-null violation naming the dropped column), and reading it
  (`CacheMode.REPLAY`, `get_all_records`) returns the old column. Delete those tables. No
  released version wrote that shape.

#### `StreamHead` is now `StreamView`; operator streams' `run()` takes no arguments (ITL-1081)

The object `head()` returns is renamed: `StreamHead` is now `StreamView`, `HeadStatus` is now
`ViewStatus` (both in `orcapod.streams`), the protocol `StreamHeadProtocol` is now
`StreamViewProtocol` and the module `orcapod.core.streams.head` is now
`orcapod.core.streams.view`. There are no aliases: rename the imports. The method `head()`
keeps its name and arguments.

`DynamicPodStream.run(*args, **kwargs)` (an operator's output stream) accepted arguments it
never used and returned nothing. It now takes no arguments, like every stream's `run()`, and
returns a `StreamView`. `FunctionJobNode.run()` and `OperatorJobNode.run()` compute as before
and now return a `StreamView` instead of `None`.

The "not computed" caption of the notebook preview and of `head()` now names `run()`:
`<Kind>[label] — not computed; run() computes it`. The captions of a polling source not yet
polled, a cached source not yet read and a side-effect stream name `run()` too.

#### The remaining "store" names are "database" names (ITL-1087)

`PipelineJob(database=...)` already names the Arrow database; the names around it now do too. There is no alias for any old name. A saved job file, a stored table, a column, a layout and every hash are unchanged.

- `set_ephemeral_store(store)` on a pipeline, a node and the node protocols is `set_ephemeral_database(database)`. Rename the call and pass the database as `database=` if you used the keyword. The node attribute `ephemeral_result_store` is `ephemeral_result_database`.
- `StoreError` is `OrcapodDatabaseError`. Rename every `except StoreError`. `SideEffectLogDataContextError` keeps its name and derives from `OrcapodDatabaseError`.
- `StoreWriteError` is `OrcapodDatabaseWriteError` and `StoreReadError` is `OrcapodDatabaseReadError`.
- `StoreContentWarning` is `OrcapodDatabaseContentWarning`. Rename it in `warnings.filterwarnings` and `pytest.warns`.
- `describe_store_table` is `describe_database_table`, and the `store_path` parameter of `account_for_stale_tables` and `find_stale_tables` is `database_path` (all in `orcapod.contexts.stored`).
- The module `orcapod.utils.store_errors` is `orcapod.utils.database_errors`; `store_error_of` and `keep_store_error` are `database_error_of` and `keep_database_error`.
- Error and warning messages say "database" where they said "store".

#### Binding refuses a source whose column types the slot does not declare (ITL-1112)

`PipelineJob.bind(sources=...)` checked a source against its slot by column names only;
assigning `job.nodes[name].bound_source` and the file sources `PipelineJob.load` rebuilds were
not checked at all. A source with other column types was accepted: every pipeline hash, built
from the slot's declared schema, stayed the same, so a run wrote rows of the new types to the
stored paths of the declared ones. Every way of binding now compares the source's tag and data
columns with the slot's: names, role (tag or data) and Python type, the inputs of the pipeline
hash. A column must have the declared type (two `orcapod.dataclass` classes of one identity are
one type), or `T` where the slot declares `T | None`. Anything
else raises `SourceSpecMismatchError` naming the slot, the column and both types, before
anything is bound; a refused `bind` leaves the job as it was, its database included.

```python
slot = SourceNode(name="src", tag_schema={"id": int}, data_schema={"x": int})
job.bind(sources={"src": DictSource([{"id": 0, "x": "a"}], tag_columns=["id"])})
# v0.2.0: accepted; rows with x: str were stored under the path of x: int
# now:    SourceSpecMismatchError: ... data column 'x' is declared int but the source has str
```

Refused now, accepted before: a column of another type, and a nullable column (`int | None`)
for a slot that declares it non-null (`int`). A slot recorded from data without a null (a
`DictSource`, `ListSource`, `CSVSource` or `DataFrameSource` infers `int` then) refuses a
later source whose data has one; a `pa.table` and a Delta table written from one are nullable
(`int | None`) in every column, tags included, unless their schema says otherwise.
`SourceSpecMismatchError` is now an `InputValidationError` as well as a `ValueError`. A file
source that `PipelineJob.load` rebuilds and its slot refuses is handled like one that could not
be rebuilt: a warning names the slot and the slot stays unbound.

Migration: bind a source whose columns have the slot's types (convert them, or give the
Arrow table a schema with the slot's nullability), or record the pipeline with a slot of the
source's types, which gives it its own stored paths.

#### Binding refuses a joined or merged stream, and a stream without system tags (ITL-1126)

A slot's rows carry one source-id and one record-id system-tag column, under the slot's names.
A stream with one pair per input (the result of a `Join` or `MergeJoin`), or with none (an
`ArrowTableStream` over a table without them), cannot give its rows that way; it was bound
before with its own system-tag columns, so the system-tag columns of the slot's table, and
of every table after it, depended on what was bound. Every way of binding (`bind`, the
`bound_source` setter, `SourceJobNode(..., bound_source=...)`, `PipelineJob.load`) now refuses
it with `SourceSpecMismatchError`, naming the slot and the system-tag columns found, whatever
its column types; so is a stream with any other system-tag column, or whose source id is not
`str` or record id not `bytes`. So does building a pipeline or job directly over such a stream
(`with job: op(ArrowTableStream(...))`), which binds it to the pipeline's input slot.

Migration: wrap a raw stream's table as a source,
`ArrowTableSource(stream.as_table(), tag_columns=[...])`; make a join part of the pipeline
(bind its inputs to slots of their own), or store the joined stream and bind the stored
source. Binding such a stream properly is planned (ITL-1127).

#### A logical type or native container as a dict key or value is refused where it is declared (ITL-1150)

A dictionary is stored as a list of key/value structs, and Arrow cannot hold an extension type
inside a struct field. A set of a primitive there was stored silently as a plain list:
`dict[str, set[int]]` read back from `as_source()`, in a downstream pod and in a job loaded in
another process as `dict[str, list[int]]`, and a source declared with it took the schema
`dict[str, list[int]]`. Every other logical type in that position (`Path`, `UUID`, a user
class, `numpy.ndarray`, `list[Path]`, `set[Path]`) was already refused.

Now any logical type or native container as a dict key or value, at any depth, is refused
when it is declared: a function pod's parameter or output, a source's declared or inferred
schema, and a field of a dataclass, an `orcapod.dataclass` or a pydantic model. Refused, for
example: `dict[str, set[int]]`, `dict[str, set[str]]`, `dict[str, list[set[int]]]`,
`dict[set[int], int]`, `list[dict[str, set[int]]]`, a dataclass field typed
`dict[str, set[int]]`.

A tuple annotation was never accepted by a function pod or a source (registration rejects it
as an unsupported annotation, and still does). The only path that took a set inside a tuple
was a direct call of the type converter (`python_schema_to_arrow_schema({"x": tuple[int, set[int]]})`),
which stored the set as a plain list and now refuses it too.

The error, `NestedTypeNotStorableError` (a `ValueError`), names the
column or `Class.field`, the declared type and ITL-1150.

Migration: hold the set in a dataclass field (a field of a dataclass, an `orcapod.dataclass`
or a pydantic model keeps `set[int]`, `Path`, `UUID` and the other logical types on every
read path), or declare the plain type (`dict[str, list[int]]`). No stored data, stored path or
hash changes: only declarations that used to be accepted are refused.

### Added

- **`orcapod.errors.StoreWriteError`** — raised when a node's results or records cannot be
  written to the store. It names the node, the row's tag and the table, and chains the
  database's own exception as its cause. (ITL-878 #287)
- **`orcapod.errors.StoreReadError`** — raised when a stored table exists but cannot be read.
  It names the table, and the node and row being processed when they are known, and chains
  the backend's exception as its cause. (ITL-884 #290)
- **`orcapod.errors.StoreError`** — the base of `StoreReadError` and `StoreWriteError`;
  `except StoreError` handles both. (ITL-884 #290)
- `SpiralDBTableSource(..., where={column: value}, limit=n, asof=version)`: read part of a
  table. SpiralDB applies the restriction before any row is transferred. Restrictions are
  not part of the source's identity (ITL-186).
- `SpiralDBTableSource(..., max_rows=, batch_rows=)`, the `row_count_estimate` and `table_id`
  properties, and `orcapod.errors.SourceTooLargeError` (ITL-186).
- A `SpiralDBTableSource` shown in a notebook (its `head()`) reads only its first 10 rows,
  in one scan to which `max_rows` does not apply, and its header says how many rows the
  table has, for example `first 10 rows of about 63,349` (ITL-186).
- `SpiralDBConnector.scan_batches(table, where=, limit=, asof=)`, `get_arrow_schema`,
  `get_spiral_table_id`, `get_table_version` and `get_row_count_estimate`: a restricted scan
  and metadata-only reads (ITL-186).
- `orcapod.errors.OutputTypeError` identifies function-output mismatches by function,
  output column, declared type and returned type, with an explicit-conversion hint.
- **`PipelineJob.run(commit_every_rows=100)`** — how many rows a function node processes
  between commits of its results and pipeline records (an `int >= 1`). Nodes also commit when
  they finish, before a failed run raises, and at the end of the job; `1` commits after every
  row. A hard crash on a durable store loses at most the rows the running node processed
  since its last commit, and the rerun computes only those. (ITL-919)
- **SpiralDB nullability record** (ITL-936). SpiralDB reports every top-level column (key
  columns included) and every struct field as nullable, whatever was written; only list items
  keep a non-null flag. `SpiralDBConnector` now keeps a *nullability record* of each table's
  non-nullable columns and nested fields: an entry in the table's key-value metadata, not a
  data row (key `__orcapod_nullability__`, format `orcapod.nullability` version 1, chunked
  under SpiralDB's 8 KiB entry limit). It writes the nullability record on a table's first
  write, and `get_column_info` and `iter_batches` restore it, so a `SpiralDBTableSource` over
  such a table gives `T`, not `T | None`, and a function pod can take `x: float`. Every batch
  of such a table is checked: a null in a listed column raises
  `orcapod.errors.NullInNonNullableColumnError` naming the table and column. A write holding a
  null in a non-nullable column is refused before anything is written (SpiralDB stores a null
  written to a non-nullable struct field as `0`), and a write whose non-nullable columns differ
  from the nullability record raises `NullabilityRecordMismatchError`. An unreadable
  nullability record raises `NullabilityRecordError`.
  `orcapod.databases.write_nullability_record` and `read_nullability_record` let other writers
  give a table its nullability record. Tables without one read exactly as before (same schema
  and hashes); a table that gains one has a different schema, `pipeline_hash` and
  `content_hash`, so write it before building pipelines on the table. A
  `ConnectorArrowDatabase` on SpiralDB gives new result tables a nullability record, so results
  read back with their written nullability; existing result tables are not changed.
- **SpiralDB null structs are refused** (ITL-936). SpiralDB cannot store a null struct column:
  pyspiral refuses one, and with `push_down_nulls=True` it reads back as a struct of null fields
  (live, pyspiral 0.16.33, top-level struct columns). `SpiralDBConnector.upsert_records` now refuses a null struct at any depth
  with `orcapod.errors.UnsupportedValueError` naming the table and column, before anything is
  written; it never passes `push_down_nulls`.
- **`DerivedSource(origin, tags_as_data={tag: data_name})`** — a source derived from
  another source or from a job node that keeps the origin's tags and also offers chosen tag
  values as data columns, so a key (for example a session start time) can be an input to a
  function pod. Also available as `node.as_source(tags_as_data=...)`. The copy has the tag's
  exact Arrow type and nullability. The result is a new root source with its own provenance;
  its default `source_id` is `tags_as_data:<label>:<hex>`, a hash of the origin's identity and the
  mapping. A saved job
  reloads it whenever its origin can be reloaded. `DerivedSource` without `tags_as_data` is
  unchanged, except that giving it a source (rather than a job node) now raises `TypeError`
  when it is built instead of `AttributeError` when it is first read. (ITL-939)
- **`DropNulls` operator and `stream.drop_nulls(columns)`** — keeps the rows where
  every named column is non-null and narrows each named column from `T | None` to `T`, in
  `output_schema()` and in the Arrow table, so a pod whose parameter is a plain `T` can
  consume a column that is nullable by specification. The names may be tag columns, data
  columns or both; a name that is not a column of the stream raises `InputValidationError`,
  and a column that is already non-nullable is left as it is. With no argument
  (`DropNulls()`, `stream.drop_nulls()`) the columns are every tag and data column of the
  input stream: a row with a null in any of them is dropped and all of them become
  non-nullable. System-tag, source-info, context and meta columns are never included. On a
  source whose columns are all nullable (SpiralDB stores every column nullable) this form can
  drop most rows; naming the columns is the precise form. An empty collection is not "no
  argument" and raises `ValueError`. Only the top-level value is
  tested (a null list or struct; not list items, struct fields or NaN). Other columns, system
  tags, source info and row order are unchanged. The operator's identity is the set of names;
  the no-argument form has one fixed identity, different from that of any list of names.
  A surviving row's data hash (or tag hash, for a tag column) changes when the named column
  was nullable, because nullability is part of the row's Arrow schema; it becomes the hash
  the row has in a stream that declared the column non-nullable. No existing hash changes.
  (ITL-943)
- **`orcapod.errors.SideEffectLogDataContextError`** — a `StoreError` raised by a
  side-effecting node (a side-effect pod, or a function pod that takes the invocation
  context) whose stored completions are at an older table layout or belong to another data
  context, before anything is delivered. It has `record_paths` (every table found),
  `record_path` (the first), `found_context`, `current_context` and `node_label`. (ITL-944)
- **`orcapod.contexts.stored_data_contexts(database, record_path)`** — counts the rows of a
  stored table by the data context that wrote them. (ITL-944)
- **`list_table_paths(prefix=())` on every database** — a new method of
  `ArrowDatabaseProtocol`: the sorted paths of the tables stored under a path prefix,
  nested tables included, without opening a table. `InMemoryArrowDatabase` and
  `DeltaTableDatabase` on a local directory list exactly; `DeltaTableDatabase` on `s3://`
  lists through `s3fs` (one list request per directory), on `gs://` and `az://` only when
  the matching `fsspec` package is installed; `ConnectorArrowDatabase` (SQLite,
  PostgreSQL, SpiralDB) lists by table name, so path components come back as the backend
  spells them (`:` and `.` as `_`); `NoOpArrowDatabase` lists nothing. A database of your
  own that implements `ArrowDatabaseProtocol` needs this method to keep satisfying the
  protocol; without it Orcapod still runs and treats the store as one that cannot list
  its tables. (ITL-954)
- **`orcapod.errors.TableListingNotSupportedError`** — a `NotImplementedError` raised by
  `list_table_paths` when a store cannot enumerate its tables. (ITL-954)
- **The ignore-list mechanism for library value types** (`orcapod.logical_types.ignore_list`):
  `IgnoreListLogicalType` and `IgnoreListHandler`, the extension metadata shape
  `{"ignore":[...],"version":N}` (`ignore_metadata`, `parse_ignore_metadata`), and the
  `hash_ignore` object of the logical type registration file. `numpy.ndarray` is the first
  type built on it. (ITL-981)
- `head(n=10, columns=..., all_info=...)` on every stream, source and node returns a
  `StreamHead` (`orcapod.streams`): at most `n` rows as a PyArrow table with their exact
  types, the row count when it is known without computing, and a `status` (`HeadStatus`) that
  tells an empty result from one that is not computed. It never computes, polls or delivers a row (it reads only what is held or stored, or makes a bounded table read), never calls user code and never writes (see ITL-912 for how a head with no
  rows learns its column types). The notebook preview is
  `head()` rendered, with large cells (arrays, bytes, long text, long lists) summarised. A job
  node with nothing in memory reads its whole stored slice to show its first rows. (ITL-991)
- **`PipelineJob.run_async`** — `await job.run_async()` runs a job with the
  `AsyncPipelineOrchestrator` (its default) on the caller's event loop, for async code and
  notebook cells. It takes the same arguments as `run`; a `SyncPipelineOrchestrator` raises
  `TypeError`. (ITL-994)
- `SpiralDBTableSource(..., tags_as_data={"timestamp": "spike_time_us"})` also offers named
  key columns as data columns, copied in Arrow as each batch is read, so a function pod can
  use a key such as a spike's time (ITL-996). The tag keeps its name; the copy has the key
  column's Arrow type and field metadata and is never null. The source still reads no rows
  when built, and `limit=`, `where=`, `max_rows` and the notebook preview work as before. The
  copies are part of the schema, so the `content_hash` and `pipeline_hash` of a source built
  with the option differ from the same source without it; the mapping itself is also part of
  the `content_hash`. The `source_id`, record IDs and source-info tokens are unchanged, and
  without the option nothing changes. A mapping that names a column that is not a key tag
  column, or a new name that is already a column, repeated or reserved, raises
  `InputValidationError` when the source is built. `DerivedSource`'s `tags_as_data` errors no
  longer say "of the origin". See "Key columns as data" in `docs/concepts/sources.md`.
- **`RunSummary`** (`orcapod.pipeline`) — what a job run did, per function node: rows
  computed, rows read from the store, rows failed, and the first error (`NodeRunSummary`).
  Shown as a table at the end of a notebook cell and as text by `print`; kept as
  `job.last_run`. `job.last_run.print_traceback("<node>")` prints the full traceback of a
  node's first failed row. (ITL-997)
- **`RowFailuresWarning`** (`orcapod.errors`) — issued once at the end of a job run in which
  rows failed (with the default `error_policy="continue"`), naming each node with failed
  rows, how many failed out of how many, the first error, and how to see its traceback. A
  run with no failed rows issues none. (ITL-997)
- A row-alignment test suite, `tests/test_core/alignment/` (ITL-1034). For every row of every table Orcapod produces, it recomputes each per-row derived column from that row alone: `_content_hash`, source-info tokens, system tags and record ids, `_context_key`, function outputs, and the stored records' hashes and ids. It covers sources, function pod streams, function and operator job nodes (computed, rerun from the store and loaded), `head()`, `as_source()`, every operator and the job's run summary, across every column configuration. Async completion orders are forced deterministically with events, not sleeps. Input rows come shuffled, tag-sorted or reverse-sorted, with duplicate and null tags. Sync and async tables are compared row by row by record id.
- **`run.expected_rows` and `StoreContentWarning`.** A saved job file holds an optional,
  advisory section, `run.expected_rows`: for each function and operator node, how many rows
  the store held under its run's key when the job was saved, written only when that is
  certain (after a run that finished and committed; never invented when a loaded job is
  saved again). A loaded node always returns what its store holds; when the store holds
  another number of rows, the first read issues one `orcapod.errors.StoreContentWarning`
  naming the node, both numbers and the store. Deleting the section from the file changes
  nothing but this check. (ITL-1043)
- `orcapod.errors.ProvisionalHashWarning`, a `UserWarning` issued once per type per process
  when a library value type is first used (ITL-1068). Silence it with
  `warnings.filterwarnings("ignore", category=ProvisionalHashWarning)`.
- `run()` on every stream, source and node, with no arguments: it computes the stream (the
  work `as_table()` does, through the same cache, so nothing is computed twice) and returns a
  `StreamView` of all its rows. It is the one way to compute any stream outside a job;
  `head()` and the notebook preview still never compute. A source has nothing to compute (a
  `PollingSource` polls, a `CachedSource` reads and updates its cache, a
  `SpiralDBTableSource` reads its whole table), a job node runs as in a job, and a
  side-effect stream delivers its side effects on every call. (ITL-1081)
- `StreamView.head(n)`, `StreamView.tail(n)` and `StreamView.sort_by_tags()` return a new view
  of the view's own rows, with a header that says which rows are shown ("first 10 of 1,234
  computed rows", "rows 11–20 of 1,234 computed rows"). They never read or compute anything.
  The display of a long view nobody cut (such as the one `run()` returns) shows its first and
  last five rows. (ITL-1081)
- `stream.drop_columns(...)` and `stream.select_columns(...)`, with the operators `DropColumns` and `SelectColumns`, act on named columns whether each is a tag or a data column (ITL-1084). `drop_columns` removes the named columns; `select_columns` keeps exactly the named columns and removes every other tag and data column. The names are given as one or more strings or as one collection of strings: `stream.drop_columns("a", "b")` is `stream.drop_columns(["a", "b"])`; `strict` and `label` are keyword-only. For names of one group, `drop_columns` gives the rows of `DropTagColumns` or `DropDataColumns`; `select_columns` gives those of `SelectTagColumns` or `SelectDataColumns` when its names also include every column of the other group (named alone, one group's names remove the other group's columns). The four existing operators are unchanged, as are their hashes. Unlike them, a call whose result would have no data column raises `InputValidationError` at once, on the sync and async paths, and no data meta (`__`) column of the input is carried, as with `Join`.
- `job.run(load_only=True)` and `await job.run_async(load_only=True)` fill a job's nodes with what is already stored for this run's inputs and compute nothing (ITL-1095): sources are read, operators run on the rows that reach them (a `CacheMode.LOG` cache writes nothing), each function node takes the stored results of its inputs and skips the inputs without one, a side-effect node passes its rows on without delivering anything, and nothing is written to the store. `RunSummary.load_only`, `NodeRunSummary.not_stored` (inputs skipped) and `NodeRunSummary.inputs_incomplete` (the function nodes above a node that skipped inputs) report it; loaded rows count in `reused`, and the summary's text says "3 loaded, 1 not stored (skipped)".

- `node.run(load_only=True)`, and `node.last_run`, the `RunSummary` of a node's last `run()`. `ViewStatus.LOADED_ONLY`: the `head()` and `run()` view of a node filled by a load-only run, captioned "N loaded, M inputs without a stored result" and naming the function nodes above it that skipped inputs. `holds_current_run(load_only=...)` on function and operator job nodes says whether `run()` would run them.

- A job loaded with `PipelineJob.load` can be filled from its store: `loaded.run(load_only=True)` (and `node.run(load_only=True)` of a loaded node) reads the job's sources (rebuilt from files, or bound with `loaded.bind(sources=...)`), runs the rebuilt operators and gives each function node the results its store holds for those inputs; nodes below an unbound source stay empty. `loaded.run()` without `load_only` still raises, and says how to load.
- A run checks its sources for new data first (developer, November 14, 2026; ITL-1102). `job.run()`, `job.run_async()` and a job node's `run()` (a load-only run too; a source job node's `run()` too, a bare source's `run()` never) call `check_for_new_data()` once on every source feeding them before deciding which nodes to run. A source that finds new data takes it in and moves its modified time, so the nodes below it run again and compute only the inputs without a stored result; a source that finds nothing moves nothing, so a job whose sources are unchanged computes nothing. `head()` and the notebook preview never check. In-memory sources (`ArrowTableSource`, `DictSource`, `ListSource`, `DataFrameSource`) cannot change and do nothing; a `PollingSource` polls once; a `DeltaTableSource` reads the Delta log and, when the version moved, reads the table again and holds the new rows if they changed (compared as read, every column included; row order as below); a `SpiralDBTableSource` reads the table's version (no row) and pins a newer one, unless `asof` was given; a `CSVSource` reads the file again when its modification time or size changed; a `SQLiteTableSource`, `PostgreSQLTableSource` or `DBTableSource` reads the whole table again (one full read per run) and holds the new rows only if they changed (row order as below); a `CachedSource` checks the source it wraps and, when that source's modified time moved since the cache last took in its rows, moves only when the cache will gain rows (the check guarantees nothing for two cached sources that share one cache, the same `cache_path` or table: give each its own cache; a missed change means a run does not compute the new rows, and nothing wrong is stored; ITL-1143); a `DerivedSource` checks a source origin and reads its origin again when the origin's modified time moved since it last read it (two such sources over one origin both see a change, and a read that raised is tried again at the next run); a `SourceProxy` checks the source bound to it. Row order follows the record id rule (developer, November 14, 2026): without a `record_id_column` a row's record id is its position, so a change of row order is a change (the rows take the ids of their new positions and every function result is reused); with one, only content counts. The model is deliberately simple; a fuller design is ITL-1103 / ITL-1104. A table or file whose columns changed, a table that is gone or empty, or a SpiralDB table dropped and created again, fails the check; a column that loses its nulls keeps its nullable type. `RootSource.check_for_new_data()` is the hook a source class overrides.
- A check that raises does not fail the run: one `SourceCheckFailedWarning` (new, in `orcapod.errors`) per source names the source and the error, the source keeps its data and modified time, the run proceeds with the data the source already holds, and `RunSummary.unchecked_sources` lists the source ("sources not checked for new data (see the warning): ..."). A later failure to read the source raises as before; a `PollingSource` reads by polling, so a run that has to read one whose check failed polls it again and raises if that poll fails (reading its held rows instead is ITL-1139).
- Every source built from a table takes the keyword-only option `tag_only_columns`:
  `ArrowTableSource`, `DictSource`, `DataFrameSource`, `CSVSource`, `DeltaTableSource`,
  `DBTableSource`, `SQLiteTableSource`, `PostgreSQLTableSource` and `SpiralDBTableSource`
  (ITL-1159). In v0.3 every tag is plain (a tag only, never also a data column), and the option
  has one accepted value, `True` (all tags plain), which is also the default. A source built
  with `tag_only_columns=True` is the source built without it: the same content and pipeline
  hashes, source id, record ids, rows, stored paths and saved config. Any other value (`False`,
  an empty collection, column names, even all of them) raises `ValueError`, because referential
  tags arrive in v0.4. In v0.4 a declared tag becomes referential by default (a tag that is also
  the data column of the same name) and `tag_only_columns` keeps a tag plain, so code that must
  keep today's behaviour can pass `tag_only_columns=True` now. `ListSource`, `DerivedSource`,
  `CachedSource` and `PollingSource` do not take the option. No hash or golden value changes;
  the golden fixtures that build a source with tag columns now pass `tag_only_columns=True`
  explicitly.
- A saved pipeline or job file does not record `tag_only_columns`: v0.3 writes no such key, and
  both file format versions stay `0.2.0`. Plain tags are decided by
  the format version: every source in a file at `0.1.0` or `0.2.0` has all its tags plain. The
  format that brings referential tags (v0.4) writes `tag_only_columns` for every source, and its
  reader refuses a source config without it, so a v0.3 file reads back plain in v0.4.
- `stream.map_columns(name_map)` and the operator `MapColumns` rename tag and data columns in one step (ITL-1160). A renamed tag stays a tag and a renamed data column stays data, with its source-info column; system tags pass through. `drop_unmapped=True` drops every tag and data column not in the mapping. As with `map_tags` and `map_data`, a name that is neither a tag nor a data column is refused unless `strict=False`, a new name taken by a column that is not renamed, or given to two columns, is refused, a `__` new name is refused when the operator is built, and a result with no data column is refused. Like `SelectColumns`, it carries no data meta (`__`) column of the input. Its identity is `("MapColumns", name_map, drop_unmapped, strict)`.
- `map_tags`, `map_data` and `map_columns` take the renames as keyword arguments too: `stream.map_data(age="age_weeks")` builds the same step, with the same hashes, as `stream.map_data({"age": "age_weeks"})` (ITL-1171). Give one dict or keyword arguments, not both (`TypeError`). `drop_unmapped`, `strict` and `label` are options, never renames: a column with one of those names, or a name that is not a Python identifier, is renamed through the dict. An old or new name that is not a string raises `TypeError` in both forms.
- Documentation: a self-paced tutorial (`docs/tutorial/`) in eleven short parts on one
  synthetic dataset, from a first stream to a stored, re-runnable pipeline over files, with
  exercises and solutions; the dataset generator `docs/tutorial/workshop_data.py` uses only
  the standard library. New concept pages *Pipelines and Jobs* (`docs/concepts/pipeline-job.md`)
  and *File and Path Types* (`docs/concepts/file-types.md`), and a longer troubleshooting page.
  Every Python sample on these pages and on `docs/troubleshooting.md` is executed by the test
  suite. The tutorial notebooks in `notebooks/tutorials/`, written against an earlier API, are
  removed in favour of the tutorial. (PLT-4864 #274)

#### `polars.DataFrame` and `polars.Series` values (ITL-578 #286)

A function pod can take or return a `pl.DataFrame` or `pl.Series`; before, declaring one
raised `TypeError: No LogicalType or LogicalTypeFactory registered`. The logical types
`orcapod.polars.dataframe` and `orcapod.polars.series` store the value with Polars' own
`write_ipc` in `large_binary`, and a value reads back with exactly the schema it was stored
with (column order, struct field order, every dtype parameter). Chunking and sortedness flags
are not stored. `Object`, `Int128`, `UInt128` and Polars extension dtypes raise `ValueError`
naming the column when the value is stored.

The content hash (the cache key) is labelled `polars_dataframe_v1` / `polars_series_v1` and
is strict: it covers everything a pod can observe as the value's logical content, that is
row count, column names and order, dtypes (struct field order, Enum categories, a
`Categorical`'s `Categories`), a Series' name, and values, where a null struct differs from
a struct of nulls. Chunking, sortedness flags and physical `Categorical` codes are not part
of it. A data context can leave out `column_order` and `struct_field_order` for a frame, and
`series_name` and `struct_field_order` for a Series, through the `hash_ignore` lists of
`contexts/logical_types.json`. See *Polars DataFrame and Series values* in the hashing
reference. Storing a frame in a SQLite store is not supported yet (ITL-877).

#### PyTorch tensors as data values (ITL-871)

- **`torch.Tensor` is a value type**: a source column, a function pod parameter or return value,
  stored, cached and re-run, sync and async, in memory and in Delta stores, and inside
  `list[...]` (`Batch`, `GroupBy`, `MergeJoin`).
  PyTorch is optional: support works whenever `torch` can be imported, however it was
  installed; the new extra `orcapod[torch]` (`torch>=2.6`) installs it from PyPI. PyTorch 2.6
  is the first version whose restricted loader reads `uint16`, `uint32` and `uint64`
  tensors. Tested with PyTorch 2.15. The PyTorch build must support the installed NumPy, because a tensor's
  elements are hashed through NumPy.
  `import orcapod` never imports PyTorch: the type is set up on first use, and reading a
  stored tensor column without PyTorch raises `orcapod.errors.MissingOptionalDependencyError`.
- **Storage is PyTorch's own serialization.** Extension type `orcapod.torch.tensor` over
  `large_binary`: one cell holds what `torch.save` writes for one tensor. A cell is read with
  `torch.load(map_location="cpu", weights_only=True)`, PyTorch's restricted loader, always
  with both arguments explicit and with no unrestricted fallback. Stored and read back:
  dtype, shape, elements, `requires_grad`, strides (transposed and `channels_last` layouts),
  the lazy conjugate and negative bits, and the class (`torch.Tensor` or
  `torch.nn.Parameter`). Not stored: the device (a tensor reads back on the CPU) and the
  autograd history (`grad_fn`, `.grad`; a tensor reads back as a leaf).
- **Only the passed elements are stored.** A tensor that does not cover its underlying buffer
  exactly (a slice, a step slice, an expanded view) is stored as
  `value.clone(memory_format=torch.preserve_format)`, so the rest of the buffer is never
  written; such a view has the clone's strides and its lazy bits are resolved. The caller's
  tensor is never modified.
- **The hash is strict and defined by Orcapod**, never on the stored bytes.
  `TorchTensorHandler` returns `ContentHash("torch_tensor_v1", D)`, where
  `D = SHA-256(lp("orcapod.torch.tensor") || E || requires_grad || conj_bit || neg_bit || ndim
  || strides || lp(class name))` and `E` is the array value digest of the elements
  (`docs/reference/hashing.md` §8). Dtype, shape and every element bit always count.
  `requires_grad`, the memory layout, the lazy conjugate and negative bits and the class count
  by default. Device and autograd history never count, because storage does not keep them. A
  tensor and an ndarray with equal elements never hash the same, also inside an untyped
  Python structure.
- **What the hash ignores is an option of the type.** The five optional properties
  (`requires_grad`, `memory_layout`, `conj_bit`, `neg_bit`, `subclass`) can be named in the
  type's ignore list: `hash_ignore` in `orcapod/contexts/logical_types.json`, empty by
  default. The list is in the extension metadata (`{"ignore":[],"version":1}`), so it is part
  of the logical type identifier, and a data context refuses to read a tensor column stored
  under another list. An unknown option name raises when a tensor is first used. There is no
  user API for the list in v0.3.
- **Dtypes.** The NumPy-shared dtypes, plus `bfloat16`, `complex32` (real then imaginary
  `float16`) and `float8_e4m3fn`, `float8_e5m2`, `float8_e4m3fnuz`, `float8_e5m2fnuz`,
  `float8_e8m0fnu`, hashed as raw bits under the PyTorch dtype name.
- **`orcapod.errors.ValueConversionWarning`** (a `UserWarning`) reports a tensor on a non-CPU
  device or with autograd history (a `grad_fn`), which is stored as a CPU leaf tensor. It
  names the function and output (or the source and column), what was converted and how to
  avoid it (`value.detach().cpu()`). A function warns once per output and conversion, however
  many rows it processes; `DictSource` and `ListSource` warn once per column with the count.
  A leaf tensor with `requires_grad=True` and an `nn.Parameter` do not warn. After
  `warnings.simplefilter("error", ValueConversionWarning)` every conversion raises (Python's
  `-W` option cannot name the class: it is read before installed packages are importable).
- **Refused at write time**, naming the column and reason: sparse, mkldnn, quantized, nested
  and meta tensors, named tensors, other dtypes (sub-byte and packed types, `bcomplex32`),
  and subclasses other than `torch.nn.Parameter` (the restricted loader cannot read them
  back). Such a subclass is not hashed as a tensor either: a subclass with its own logical
  type needs its own value handler.
- **Declared outputs.** A pod declared `-> torch.Tensor` must return a tensor; an ndarray or a
  number raises `OutputTypeError`. A `torch.nn.Parameter` is accepted and is stored, read
  back and hashed as an `nn.Parameter`. A pod declared with a subclass must return that
  subclass.
- **Registration on first use.** `LogicalTypeRegistry(deferred_logical_types=...)` and
  `PythonTypeHandlerRegistry.register_deferred(...)` name a value type by its qualified class
  name and extension name and load it on first use, with the configuration (the ignore list)
  given in the entry. The tensor entries are in `orcapod/contexts/logical_types.json`
  (`deferred_logical_types`, `deferred_value_handlers`); the `std:v1:default` data context
  file is unchanged. No existing hash, schema or table path changes.

#### `op.dataclass` — a dataclass identified by its name and fields (ITL-972)

`@op.dataclass` is used like `dataclasses.dataclass` (bare, or with `name=` and the
dataclass options except `init=False`) and returns a real dataclass. Orcapod identifies it
by its name (default: the class name) and its fields, not by the class object or its
module:

- Two such classes with the same name and fields are one type. A class cell run again in
  a notebook, or a class moved from a notebook into a module, keeps its pods and its
  stored results.
- A pod annotated with one class accepts a column of another class of the same type and
  receives the value as it is. Orcapod never converts a value from one class to another:
  a structural class promises its fields, not its methods.
- Instances of two classes of the same type compare equal when their field values are
  equal. Equality and hashing always cover all stored fields: `field(compare=False)` and
  `field(hash=...)` are refused with `TypeError` where the class is defined.
- Stored values can be read in a process that does not define the class: Orcapod rebuilds
  a dataclass from the description stored with the column.
- `P.from_instance(other)` builds an instance from an instance of any other dataclass with
  exactly the same field names and types. It is the only conversion, and it is explicit.
- Every class inside an `op.dataclass` must be an `op.dataclass`.

See [Your Own Data Types](docs/concepts/your-own-data-types.md) for the two kinds and when
to use each.

Re-running a notebook cell that defines a dataclass or pydantic model, or reloading its
module, no longer raises `ValueError: … already bound`.

### Changed

- Calling a function pod, `pod(a, b)`, now drops the data columns its function does not take
  (ITL-807, ITL-193). The inputs are joined as before; when the joined stream carries data
  columns that are not parameters of the function, one automatic `SelectDataColumns` is
  placed after the automatic join and before the pod. It is an ordinary operator step: the
  graph, the hashes and the stored tables are those of a pipeline with that select written by
  hand (`pod(wide.select_data_columns([...]))`, or `pod(a.join(b).select_data_columns([...]))`), so a change in an unused column no longer causes a recomputation. When nothing would
  be dropped no step is added and the pipeline is the one built before. One message at `INFO`
  level names the dropped columns. `pod(a, b, strict=True)` rejects an extra column, and
  `pod.process(a, b)` is the exact primitive and always rejects it. Code that relied on
  `pod(wide)` raising for an extra column must use `strict=True` or `process`. A missing
  column for a parameter without a default is an error in every form; tag columns are never
  dropped; two inputs sharing a data column still fail at the join. Operators and
  `SideEffectPod` are unchanged: their `__call__` stays an alias of `process`.
- When persistent and ephemeral results exist for the same input, reads select the
  greatest recomputation index across both stores; persistent rows retain precedence
  for equal indices. Loaded REPLAY operators report an absent cache table explicitly,
  loaded functions reject `run()`, and loaded jobs report `is_runnable()` as false.
  Source schema restoration uses the saved data context's type converter. (ITL-904)
- `head()` contract (ITL-912): `head()` and the notebook preview never compute, poll or deliver a row (they read only what is held or stored, or make a bounded table read), never call user code (a function, a predicate, a side effect) and never write.
  With no rows to show, they now give the columns and Arrow types the rows would have: they
  apply each operator to empty (zero-row) tables, which processes no data (a chain of N
  operators applies up to N operators to empty tables, about 60 ms for 20), and a filter is not
  applied at all, since its columns are its input's. A user-defined operator's `static_process` is applied to zero-row tables and must not have effects. So a pipeline whose inputs disagree on a
  column's Arrow type (`int8` against `int64`) now raises from `head()` the
  `InputValidationError` its run raises; the notebook preview shows it as "could not read". A
  filter whose predicate does not fit its columns shows a table in `head()` and raises when it
  runs.
- **Released golden hash values are protected by CI (ITL-946).** The golden hash files of a
  data context live in one folder per data context version
  (`tests/test_hashing/golden/data/v1/<group>-NN.json`), and the manifest's `golden` section
  is an append-only history that lists every file with the sha256 of its bytes. A release
  appends a revision whose digest is pinned under `releases` in
  `orcapod/contexts/released_manifests.json`, which also pins the manifest without its
  `golden` section. A golden file a release listed never changes; new golden values go into
  new files and can be added in any release. A change to a released golden value, a logical
  type's own identifier change included, needs a new data context version, shipped only in a
  minor (before 1.0) or major release. A CI step (`scripts/check_golden_tags.py`) checks HEAD
  against every release tag of the current data context. See
  `docs/reference/golden-hashes.md`.
- Each in-memory source class and `DerivedSource` declares its kind once as `SOURCE_KIND`
  (`arrow_table`, `dict`, `list`, `data_frame`, `derived`), used for the default source id,
  for `to_config()["source_type"]` and for the source registry.
  `orcapod.core.sources.source_ids.hash_source_id` is the single definition of the form.
- **`StarfixArrowHasher.hash_schema` hashes a field under the same rule as `hash_table`.** A
  logical type with a value handler is hashed as a `large_binary` field carrying the type's
  identifier, so its storage type is no longer part of that hash. No library code calls this
  method, and no stored or pinned value moves. (ITL-981)
- **A failed row no longer logs its traceback at WARNING on the sync orchestrator.** The
  per-row log is at DEBUG, as on the async orchestrator, which logged nothing visible: a
  function failing on every row printed one traceback per row on the sync path and nothing
  at all on the async path. The run summary and `RowFailuresWarning` now report failures the
  same way on both. `error_policy="fail_fast"` is unchanged. (ITL-997)
- The repository is prepared for release from the public `nauticalab/orcapod-python`
  repository (ITL-1007). The project URLs in `pyproject.toml` and the links in `README.md` point
  there. `RELEASING.md` describes the flow: development stays on `metamorphic-labs`, `main` is
  pushed to `nauticalab` by fast-forward at a release point, and the Release workflow runs there.
- The Linear jobs, the Codecov uploads and the SpiralDB integration job run only on
  `metamorphic-labs`; a new `lint.yml` workflow runs ruff, the format check and basedpyright on
  `nauticalab`.
- Removed: the `workshop/` tutorial-dataset tool and its tests (`tests/test_workshop/`), and its
  implementation plan. They stay in git history.
- A saved job file and a saved pipeline file now carry `orcapod_version`, the value of `orcapod.__version__` of the release that wrote them, and both file format versions move from `0.1.0` to `0.2.0` (ITL-1026). Loading accepts `0.1.0` and `0.2.0`: a `0.1.0` file, written by v0.2.0, loads exactly as before, with unchanged content and hashes and no `orcapod_version`; any other version is rejected with the existing error. `orcapod_version` is informational and loading never reads it. v0.2.0 cannot load a file saved by v0.3.0 (it rejects `0.2.0`), which is intended, since hashes differ between the two releases. No hash, golden value or stored table changes.
- **A loaded function node no longer raises when its store lacks the run's results** (it
  returns what the store holds, with a `StoreContentWarning`). The store is the source of
  truth: a loaded job shows what the store holds for it. This supersedes the
  `run.node_has_stored_records` flag and its error described under ITL-904, both unreleased:
  the flag is no longer written, and a file that still has it loads without a check. A
  dangling reference, a pipeline record whose result is missing from the result store, is
  logged and left out, for a loaded node as for a running one. (ITL-1043)
- `head()` takes the column configuration of `as_table`, sorting included, and applies it to
  the rows it has before taking the first `n` (ITL-1053). `head(n, columns={"sort_by_tags":
  True})` no longer raises, and `head(n, all_info=True)` now sorts by tag, as `as_table`
  does; the default `head()` stays unsorted, as `as_table()` is. The notebook preview asks for the
  sorted head, so it shows rows in tag order whatever order they were computed in, and its
  header line says "sorted by tags" (for a bounded read, "10 rows read, sorted by tags, of
  about N"). A stream that has all
  its rows (a job node's stored rows, a computed stream, an in-memory source) gives exactly
  `as_table(...).slice(0, n)` for every configuration, whatever order its rows were computed
  in. A stream that has read or computed only part of its rows (a `SpiralDBTableSource`
  head, a partly computed stream) sorts the rows it has: the rows read, in tag order.
- No operator carries its inputs' `Data` meta (`__`-prefixed) columns anymore, on the sync and
  the async path alike, as `Join` already did (ITL-1080). An operator's output rows and its
  predicted output schema hold none of them, whether they came from a raw `ArrowTableStream`
  whose table has `__` columns or from the rows of a cached function pod or function node
  (`__computed`, `__pf_*`, `__pod_ts`). Before, `MergeJoin`, `Batch`, `GroupBy`, `PolarsFilter`,
  `SemiJoin`, `DropNulls`, `MapTags`, `MapData` and the column selections kept them on one path
  or both. No row content hash, record id or stored table changes: meta columns were never
  hashed, and job runs already stored operator rows without them. A mode that carries meta
  forward is ITL-867.
- A tag column whose name starts with `__`, the prefix reserved for meta columns, is refused with
  `InputValidationError` where it would be made: building an `ArrowTableStream`, building a
  source with such a name in `tag_columns` (before, a "tag_columns not found" error, since the
  source drops `__` columns), and `MapTags` renaming a tag to such a name (ITL-1080). `MapData`
  likewise refuses to rename a data column to such a name, and `Pick` and `Index` refuse such an
  `out` name (before, the picked column was silently dropped). Before, a raw stream could carry such
  a tag, and every path that rebuilt rows silently lost it, so sync and async runs differed.
- The display of a stream, source, node or view marks its tag columns again: in a notebook
  their headers are bold, a rule separates them from the data columns and a legend says
  "bold = tag"; as text, their names start with `*` ("* = tag"). This replaces the
  "tags: ..." line; tables and schemas keep the plain names. (ITL-1081)
- The `repr` of an operator stream, an operator node and a function node no longer nests the `repr` of its upstreams (ITL-1111). It shows its own class, label and operator or data function, and names each upstream by class and label only, for example `DynamicPodStream(label='Join', kernel=Join, upstreams=(<DynamicPodStream 'MapData'>, <DynamicPodStream 'MapData'>))`. The nested form named every path through the graph, so its length doubled with each stage that splits a stream and joins it again.
- A slot that declares `x: int | None` accepts a source whose `x` is `int` (ITL-1112). The slot
  gives that source's rows as rows of its declared schema: `output_schema()`, `as_table()`,
  `iter_data()`, `head()` and the async path show `x` nullable, and the system-tag columns
  carry the declared schema's names. The rows are exactly those of a source with a nullable
  `x`, the same values and the same source id (row hashes and record ids included), so both
  write to the same stored tables. Nothing downstream changes: every pipeline hash and
  predicted schema is the declared one. The source node's `content_hash` and source id stay
  the bound source's own.
- The pipeline determines every node's schema (ITL-1112): no accepted bind changes a node's
  stored path or the columns of any node's or stored table (names, tag or data roles, types,
  nullable flags, system-tag column names). A stream bound to a slot (a source passed
  through a filter or a rename, with one system-tag pair) gives its system-tag columns the
  slot's names; their values stay the stream's own. It passed through as it was before.
  Column order is not part of that: a source matches its slot by column name within the tag
  group and within the data group, whatever their order. As a convenience, a source whose
  columns come in another order is shown in the slot's declared order, its system tags
  source id first.
- A load-only run (`job.run(load_only=True)`, `run_async(load_only=True)`, `node.run(load_only=True)`, a source slot's `run(load_only=True)`) reads only job nodes, sources and streams that neither compute nor deliver (developer, November 14, 2026; ITL-1121). A pod's stream built outside the job is refused with a `ValueError` before anything is read: a function pod's stream (a cached pod's included), which computes when it is read, and a side-effect pod's stream or a `SideEffectNode` outside a job, which deliver their side effects (before, the run read them and delivered every row). An `ArrowTableStream` holds its rows and is read whatever its `upstreams` name. This applies whether a job node reads the stream, a source slot is bound to it or an operator's stream outside the job reads it. Build the pod inside the job, or run without `load_only`; a run without `load_only` is unchanged. ITL-1130 re-evaluates the choice.
- Large types no longer depend on the source kind or the path (ITL-1122). `CSVSource` gave `string` from a file. `CachedSource` gave the types its cache store kept: `string` from an in-memory cache, `large_string` from a Delta cache and with no rows. On a plain stream, the operators that pass rows through unchanged (`SelectTagColumns`, `SelectDataColumns`, `DropTagColumns`, `DropDataColumns`, `SelectColumns`, `DropColumns`, `MapTags`, `MapData`, `PolarsFilter` with no predicate, `Join`, `SemiJoin`, and `GroupBy`'s key columns) kept a `string` column, while the same operator inside a job gave `large_string`. `Join` of a `string` and a `large_string` input on one tag gave the type of whichever input came first in its canonical order. All of them now give large types, with rows and without, sync and async.
- A generic alias with no `__origin__` is refused (`TypeError`) instead of hashed by its `repr`, which is not stable. No `typing` or `collections.abc` alias lacks an origin on Python 3.11 to 3.13.

### Fixed

- A data context file (`contexts/logical_types.json` or a manifest) in which a JSON object
  repeats a key is refused with `ContextValidationError` naming the file and the key. Before,
  the last of the repeated keys silently replaced the others (ITL-578).
- **A `set[T]` of a primitive reads back as a set everywhere.** A downstream pod annotated
  `set[int]` received a `list`, even inside the job that computed it; `node.as_source()` read
  a `list` under a schema that said `set[int]`; and a job loaded in another process said
  `list[int]`. The row `{1, 2, 3}` as `set[int]` also hashed exactly like `[1, 2, 3]` as
  `list[int]`. A set now keeps its type on every path: the node, a downstream pod,
  `as_source()`, `DerivedSource`, and a job loaded in a fresh process, sync and async, on
  Delta and in memory. Its Python type is that of the element's Arrow type, as for a list:
  `set[np.int8]` and `set[int]` both read as `set[int]`, each column keeps its own width, and
  both can be declared in one process. `list[T]` of a primitive is unchanged (a plain
  `large_list`). (ITL-611)
- **A job loaded in a fresh process restores `list[T]` and `set[T]` columns of a logical type**
  (`set[Path]`, `set[UUID]`, `list[Path]`): their saved type could not be restored unless the
  type had been used in that process first, and the stored view read the raw storage, with a
  row hash that differed from the one in the process that wrote it. A saved schema now rebuilds
  such a container from its identifier. (ITL-611)

- **A stored column whose storage is not its logical type's storage raises
  `LogicalTypeIdentifierError`**, naming the column and both storage types, whether or not the
  type was registered in the process before (it raised a `TypeError` from Arrow when it was).
  (ITL-611)
- `Join`, `MergeJoin` and `SemiJoin` accept a shared tag key that is non-nullable in one input and nullable in another (ITL-829). They raised `InputValidationError` ("Type conflict for key 'id': <class 'int'> vs int | None", or "Types int | None and <class 'int'> are not compatible") when the graph was built. A null key matches nothing, so `Join` and `MergeJoin` now give the key `T` when any input has it `T`, and `T | None` only when every input has it nullable; `SemiJoin`'s output keeps the left input's schema as it is. Output prediction and computed Arrow fields agree on the sync and async paths. Any other type difference still raises, a data column that differs in nullability behaves as before in `Join` and `MergeJoin`, and every pipeline that worked before gives the same rows, schema and hashes.
- An async barrier operator (`PolarsFilter`, `GroupBy`, `MergeJoin`, and the default
  `async_execute` of `UnaryOperator`, `BinaryOperator` and `StaticOutputOperatorPod`) whose
  input delivers no rows emits no rows and finishes, as the sync path returns an empty
  stream. It raised `ValueError: Cannot materialize an empty list of rows into a stream`, so
  a pipeline with a filter that matches no row upstream of such an operator completed under
  `SyncPipelineOrchestrator` and failed under `AsyncPipelineOrchestrator`. (ITL-865)
- **A declared output class with a logical type requires an instance of it (ITL-895).** The
  output check required `isinstance` only for `np.ndarray`; for any other class with a
  registered logical type a wrong value was caught only if its conversion failed, so a plain
  tensor returned for `-> torch.nn.Parameter` was accepted. Such a value now raises
  `OutputTypeError` naming the function, the output, the declared type and the returned type.
- Under the async orchestrator a function node whose run stops on a row failure now calls the observer's `on_node_end` before raising, as the sync orchestrator does (ITL-879).
- A function node that the async orchestrator cancels because another node failed now commits the results it wrote before the cancellation propagates, as a failing node does, and then holds no rows: it reads as a node with no run, as a node whose run raised (ITL-879, ITL-1093). Before, its ephemeral results were never committed while the job committed its pipeline records, and on a Delta store its results were not committed ahead of its pipeline records.
- Function outputs are validated before iteration, hashing or storage on standalone,
  cached, sync and async paths (ITL-895, ITL-901). Float-for-int truncation and unordered
  set-for-list conversion are rejected. Nested values follow the same checks; exact and
  structural user classes reuse their logical type compatibility, and ndarray mismatches
  use the same error. Diagnostics do not expose returned mapping keys or converter values.
- Same-kind numeric values are accepted silently only when represented exactly, including
  NumPy scalars for Python numbers and in-range Python integers for NumPy integer types.
  Numeric narrowing, non-subclass kind changes and iterable-to-list require explicit
  conversion. Subclasses are accepted unchanged in memory; declared base storage omits
  subclass-only fields and their contribution to row hashes. This includes bool for int
  and datetime for date, whose stored form is the declared base type. Accepted numeric
  exceptions are normalized to the declared type immediately, so downstream pods see the
  value that is stored. A subclass value is the exception: downstream pods in memory
  receive the object as returned (a `datetime` declared as `date`, or a user-class
  subclass with its extra fields), while storage holds only the declared base form.
  Conforming-data hashes are unchanged.
- A declared `dict` accepts only a `dict` instance (other mappings such as
  `MappingProxyType` need `dict(value)`), and a declared `Literal` accepts only its
  members, matched by type and value with no numeric exception.
- A function node's `as_table()` keeps the Arrow type of every tag column. Before, it rebuilt the table from Python values, so after `job.run()` an `int8` or `uint32` tag came back as `int64`, `float32` as `double`, a naive `timestamp[us]` as `timestamp[us, tz=UTC]` and `date64` as `date32`, and a `time`, `duration`, `decimal`, `uuid` or `Path` tag made it raise. Joining a function node's output with another source on such a tag no longer fails with "Join inputs disagree on the Arrow type". `as_table(all_info=True)` of a function node now also has the `_source_<output>` and `_context_key` columns, as the same pod's stream outside a job does. When two of a node's rows disagree on a column's Arrow type, `as_table()` raises `InputValidationError` naming the node, the column and both types, instead of returning a type neither has.
- The table of a function pod's stream (the pod applied outside a job) no longer raises for a `uuid` or `Path` tag ("Expected bytes, got a 'UUID' object").
- On a Delta store, a re-run over a tag of an unsigned, non-microsecond or zoned timestamp type writes no pipeline row again, and a fresh job, a loaded job, an operator node's replay and a `CachedSource` read the tag's own type (see above).
- A name can no longer be a tag column in one input and a data column in another. `Join` and
  `MergeJoin` of such inputs, `Index` or `Pick` with `out=` equal to a tag name, and a function
  pod whose output key equals an input tag name now raise `InputValidationError` when the graph
  is built (and on the first row of the async path), naming the column and suggesting a rename
  with `map_tags` or `map_data`. Before, building succeeded and reading raised a raw `KeyError`
  (sync); the async `Join` yielded rows whose tag and data values of that name differed.
  `SemiJoin` is unchanged. (ITL-940)
- `get_function_signature(include_defaults=False)` no longer cuts a parameter at the first
  `=`, which truncated an annotation such as `Literal["a=b"]` (ITL-953).
- `arrow_type_to_python_type` and `arrow_schema_to_python_schema` no longer name a logical
  type's Python class for a column (or a member of a struct column) whose extension metadata
  that type does not read: they raise `LogicalTypeIdentifierError`, as reading the values
  already did. The check, and the one made before values are decoded
  (`arrow_table_to_python_dicts`), also holds for a type nested in a struct, list, map or
  union when an equal-looking type with readable metadata was converted before (ITL-975).
- `CachedDataFunction.call` and `async_call` refuse an input of another data context before
  the function is called, also with `skip_cache_lookup=True` (ITL-975).
- A side-effecting function node checks an ephemeral result store attached with
  `set_ephemeral_store` for completions of another data context, also when the store is
  attached after the node first read its stores (ITL-975).
- A second commit to a Delta table that holds a list of a type stored as binary
  (`list[uuid.UUID]`, `list[bytes]`) no longer fails with `type_coercion ... LargeList(LargeBinary)
  vs List(Binary)`. It failed whichever runner made it (a run on `job.dag`, which commits per row,
  or a later `job.run()` that adds rows). The append check now compares storage types, and the MERGE
  that handles a batch holding ids already stored is given its list columns in the stored layout.
  Stored tables, paths and hashes are unchanged.
- A function pod created with `pod_cache_database=` (a `CachedFunctionPod`) is now recorded when it is applied inside a `PipelineJob`: the job has the source and the function node, and runs, saves and loads like an uncached pod. Its inputs are joined once. The pod cache is honoured in a job (a row already in it is not recomputed). The pod cache database is not written into the saved job, like the node's own databases, so a loaded job does not use it.
- The table of a `CachedFunctionPod` stream has the same columns, types and order whether a row was computed or served from the cache, and no meta columns unless asked for (`columns={"meta": True}` or `all_info=True`); then both states carry only `__computed`. Before, a served row also carried `__pf_var_*`, `__pf_exec_*` and `__pod_ts`, and the plain table showed `__computed`.
- An operator's output stream computed again after one of its inputs changed now does so:
  before, it served the result kept from before the change (`DynamicPodStream` cleared a
  misspelled attribute instead of its kept result). (ITL-991)
- **`job.run(orchestrator=AsyncPipelineOrchestrator())` works inside a running event loop**,
  such as a Jupyter kernel or async code. It used to raise
  `RuntimeError: asyncio.run() cannot be called from a running event loop`. Inside a loop, the
  run now goes on a separate thread while the call waits, so the same line works in a script
  and in a notebook; it blocks that loop until the run ends (use `await job.run_async()` in
  async code). The interrupt button cancels the run and waits for functions in the middle of
  a row to return; pressing it again stops waiting at once, with a warning naming the node
  whose function has not returned. Nothing more is written to the store when it returns (an
  `async def` function that blocks the event loop is the exception, and the warning says
  so). Stored rows, record ids and hashes are the same as a run outside a loop. (ITL-994)
- A job node's table gave rows the content hash of other rows (ITL-1029). `node.as_table(all_info=True)`, or any `columns` that asked for `content_hash` together with `sort_by_tags`, sorted the rows by tag and then attached the `_content_hash` column in the order the node had computed its rows. Whenever that order differed from tag order, rows carried another row's hash: on the sync path when the node's input was not in tag order, and on the async path whenever rows finished out of order, which happens at random. The hashes are now attached before the sort, so each row keeps its own. Stored records, record ids and every stored or golden hash were never affected; only this in-memory table was. A function pod's own stream (`FunctionPodStream`) and operator nodes were already correct.
- `docs/reference/hashing.md` gave the wrong namespace for a source row's record id (site 5). It is `uuid5(NAMESPACE_URL, "https://orcapod.org/namespaces/source-record-id")`, as in the code and the golden values. A runnable sample on the page now recomputes a pinned record id from the documented recipe.
- A function pod stream that was read again after a partial iteration (a `for` loop with `break`, or `next(iter(stream))`) no longer drops rows (ITL-1039). The second read counted input rows from 0 again while the input resumed where it had stopped. It yielded the rows read first in place of the next ones, lost those, and cached later outputs under the wrong row, which `head()` showed. Every read, partial, repeated or interleaved, now yields every input row once, in input order, with its own output, and each row is computed once. A row whose function raised during a read is computed again by the next read instead of being skipped, and an upstream stream that raised mid-read is read again instead of being taken as exhausted. `CachedFunctionPod` streams had the same defect. A job's function nodes were not affected. No hash, record id or stored table changes.
- Async job runs no longer store an extra `_source_<column>` column for a data column dropped before a function pod, by the automatic select of `pod(stream)` or by an explicit `SelectDataColumns` or `DropDataColumns` (ITL-1040, ITL-838). `Data.drop` and `Data.select` kept the source info of the removed columns, and `Data.as_table(columns={"source": True})` still showed it. A selected `Data`'s source info is now in the order of its columns. Sync and async runs now store the same pipeline records. No hash, record id or stored path changes. Existing tables are not migrated in this release. A Delta pipeline table written by an async run before this fix keeps the extra column, and rows written from now on have null in it. Migrating stores written by earlier versions is the job of the migration tool planned after v0.3 (ITL-1048).
- The async `Join` matches shared tags whose values are lists, structs or nested lists, exactly as the sync `Join` does, instead of raising `TypeError: unhashable type: 'list'` (ITL-1044). Its keys now follow the sync equality on scalar tags as well: a null key never matches, and NaN matches NaN.
- The async `Join` no longer matches a row with a null in a shared tag column to a null on the other side; the sync `Join` never does, so the two paths now give the same rows (ITL-1050).
- `as_table` on a function pod stream, a pod-cached stream or a function node no longer returns rows from one computation with content hashes from another (ITL-1045). When the upstream was reset (`clear_cache()` on it) between `as_table()` and `as_table(columns={"content_hash": True})` and the function gives different values on a second call, the cached table was kept and its rows were given the hashes of the recomputed rows. A stale stream or node now clears its cache first and returns the current rows with their own hashes. No hash, stored table or public API changes.
- `Index` after `Batch` or `GroupBy` now gives the extracted element its own provenance token (ITL-1046). The `_source_<column>` value was the Python text of the whole token list followed by `[i]` (`"['src::x=1::x', 'src::x=2::x'][0]"`), which no resolver can parse. It is now the member's token (`'src::x=1::x'`), one list level removed at every depth (a column aggregated twice gives the inner list), and null when the row is skipped. A scalar token keeps the `<token>[i]` form. No hash, record id or golden value changes. Tables already written keep the old token text; upgrading stored data is the job of the migration tool (ITL-1048).
- A node's `head(n, all_info=True)` no longer differs from `as_table(all_info=True)`'s first
  rows when an async run completed rows out of tag order (the cause of an occasionally
  failing test, ITL-1001).
- Every tag sort is now one deterministic sort (`arrow_utils.sort_by_tag_columns`): by the
  tag columns, nulls last, rows with equal tags ordered by their system tag columns. A job
  node's and a function pod stream's sorted `as_table` used an unstable Polars sort that put
  nulls first and left the order of rows with equal tags unspecified; it now orders them as
  an `ArrowTableStream` does. Only the order of rows within equal or null tags changes: no
  values, record ids or hashes.
- The pandas storage round-trip test lets only the first write refuse a value. A read failure
  of a written value, a failed second round trip, or a column or index level read back in
  another position now fails the test, where it used to count as unsupported input.
  (ITL-1054)
- A result row a function node reads back from its store now carries the stored `__data_id` as its `datagram_uuid` (ITL-1060). Before, every read-back row (sync, async cache-only, loaded node) got a fresh uuid, so it was not the datagram its own source-info token names and its id changed on every read. Nothing stored changes: the id is read only from the output of a computation or a result-cache lookup, which already carried the stored id. No hash, record id or golden value moves.
- **`SideEffectPod` streams and job nodes: `as_table` honours every column configuration.**
  `SideEffectPod(fn)(stream).as_table(...)` and the table of a side-effect node in a job raised
  `Duplicate column name '_context_key'` for `columns={"context": True}` and `all_info=True`, left out the
  content-hash column for `content_hash`, and did not sort for `sort_by_tags`. Both now shape their
  table with the helpers the function pod streams use: the same column groups, order and types, the hash of
  each row staying with its row after a sort. `head()` is unchanged (no rows by design). (ITL-1066)
- `all_info` given inside a column configuration, `columns={"all_info": True}` or `columns=ColumnConfig(all_info=True)`, is honoured: `as_table`, `output_schema`, `keys` and `head` of every stream and node return the same columns as the `all_info=True` keyword, instead of silently returning only the user columns. A set `all_info` turns every column group on, whatever else the configuration says, and a `False` keyword does not cancel it. (ITL-1067)
- An async job run no longer hangs when one node feeds both inputs of a `SemiJoin` (ITL-1075). `SemiJoin` read its left input only after its right input had ended. When a node fed both inputs (for example `SemiJoin(C, A)`, with `C` computed from `A`), the bounded channel into the unread left input filled, every node between `A` and that channel blocked in turn, `A` could not finish, and the run waited forever. A computing function node absorbs its whole input, which hid this on a first run: the hang showed on a run that found every row stored, or on any run of a graph of operators only. `SemiJoin` now reads both inputs at once. It keeps the left rows that arrive before the right input ends (at most the whole left input, which the sync path and `MergeJoin` already hold), and then streams the rest of the left input as before. Output rows, their order, system tags and hashes are unchanged.
- An async job run no longer hangs on an operator node in `CacheMode.REPLAY` with more upstream rows than a channel holds (ITL-1075). The node served its stored rows without reading its input, so its upstream blocked on a full channel. It now reads and discards its input while it replays, so the upstream runs over every row, as it does with the sync orchestrator.
- An operator node whose async run produces no row keeps the sync run's columns and Arrow types
  (`int8` stays `int8`; system-tag and source-info columns are present), instead of a table
  built from the Python output schema (ITL-1080). When an input itself delivered no row, that
  input is rebuilt the same way on both paths alike, from its upstream's zero-row table
  (ITL-912).
- `DropDataColumns` and `SelectDataColumns` raise `InputValidationError` when their result would
  have no data column (every data column dropped, or `strict=False` matching none), when the
  operator is applied on the sync path and on the first row on the async path, with one message
  (ITL-1080). Before, the sync path failed only when the rows were read, with a `ValueError`, and
  the async path emitted rows with empty data.
- `MapData(..., drop_unmapped=True)` whose sources are not in the stream would leave no data
  column: it now raises the same `InputValidationError` on both paths, where both paths used to
  keep every data column while the prediction listed none. `MapTags(..., drop_unmapped=True)` in
  the same case drops every tag on both paths, as its prediction says; the async path used to
  keep them. A meta (`__`) source of `MapData` is never renamed and no longer reports a false
  collision, and a strict `SelectDataColumns` / `DropDataColumns` naming a meta column reports it
  missing on both paths (ITL-1080).
- `PipelineJob.set_ephemeral_database(...)` no longer raises `AttributeError` on a job that contains a side-effect node, live or loaded. The side-effect node takes no ephemeral database and is skipped; function and operator nodes receive it as before (ITL-1147).
- Reading a side-effect node of a `PipelineJob` after `job.run()` no longer delivers every side effect again (ITL-1091). Before, `iter_data()`, `async_iter_data()`, `as_table()`, the dataframe methods, `flow()` and `run()` of the node, and a downstream node's `run()`, each delivered every row again, without consulting or writing the invocation log, even with `track_completion=True`. The node now holds, in memory, the rows its last run passed on (on both orchestrators; the rows of that run only), and every read returns them and delivers nothing; `head()` and the notebook preview show them. A read with no such rows (no run in this process, the last run raised, or a node upstream changed since) returns no rows and delivers nothing. Rows whose delivery failed under `on_error="log"` are returned as they were passed downstream (left out with `drop_on_failure=True`, kept with `False`). The standalone side-effect stream outside a job still delivers on every read. No hash, stored table or layout changes.
- After `job.run()` on either orchestrator, a function job node's `as_table()`, `iter_data()`, `head()` and `run()`, and a stream built over the node, return exactly the rows of that run's inputs (ITL-1093). Before, after an async run a node returned every row stored under its key: a pipeline whose source rows changed between runs under one `source_id` showed the results of earlier runs too (for a function over `group_by`, the partial groups of an earlier run next to the full ones; rows of source rows no longer present). The sync orchestrator did the same when one job object ran again with its source rebound. Rows sent downstream during the run and the `RunSummary` were already right. Results of other inputs stay stored and are reused when those inputs come back; nothing stored, no hash and no table layout changes. A run that raised defines no rows: the node then reads as a node with no run in this process. A node read with no finished run in this process (a job built but not run, or a loaded job) still returns every row stored under its key.
- **A loaded job keeps its side-effect nodes.** `PipelineJob.load` and `Pipeline.load`
  dropped every side-effect node of the saved file: `loaded.nodes["<label>"]` was a
  `KeyError`, the node below one had no upstream (a load-only run of it could not start), and
  saving the loaded job again wrote a file without the node but with its edges. The node is
  now restored read-only, in its place in the graph, with its saved label, pipeline hash,
  output schema and the content hash the saved job's node had, as a loaded function node has. The file holds neither the side-effect function nor the
  pod's configuration, so a loaded side-effect node never delivers and never writes its
  invocation log. A load-only run (`loaded.run(load_only=True)`) passes its rows on to the
  nodes below it, which it holds as loaded only; any other run raises. `PipelineJob.save` now
  also records each side-effect node's content hash in the existing
  `run.node_job_content_hashes` map of the job file, which the loader reads by label; a job
  file saved before this change loads its side-effect nodes with the content hash their
  descriptor records (the blueprint's). No hash value or stored table changes; files saved
  by v0.2.0 (format `0.1.0`) restore their side-effect nodes too. Its schema under any
  column configuration is its restored upstream's (system tags included), as for a live
  side-effect node, and `as_pipeline()` of the loaded job keeps it. (ITL-1094)
- **A sync load-only run of a loaded job loads the function nodes below a Join.** On the
  sync orchestrator, an operator below a node with no pod (a loaded function node, a restored
  side-effect node) named its system-tag columns after a root's schema-only hash instead of
  the node's pipeline hash, so the function nodes below it found none of their stored
  entries (`not stored`), while the async orchestrator loaded them all. The sync path now
  names them as the async path and the live job do. No hash value changes. (ITL-1094)
- On the asynchronous orchestrator, an operator node in `CacheMode.REPLAY` stamps itself after its inputs close, so the rows it replayed are not judged out of date by the time the run ends.
- A `PollingSource` moves its modified time only when a fetch returned rows, and to the moment it found them. Before, a fetch that returned no rows also moved it, and a cursor's `modified_at` was used as the time: rows written before a downstream node's last run but fetched after it left that node current, so they were never processed. `Cursor.modified_at` is now informational.
- A `SourceProxy`'s modified time follows the source bound to it, and binding or unbinding a source moves it. Before, it kept the time the proxy was built, so the nodes below a loaded job's proxy were not made stale by their source.
- Defining a pipeline whose stages split a stream into two branches and join them again no longer takes time exponential in the number of stages (about 4x per stage before: 9 stages took 13 seconds to build, with no row computed; 20 stages now build in well under a second, as streams and inside a `PipelineJob`) (ITL-1111). Three graph walks asked a shared upstream once per path: every operator stream, function pod stream and node predicted its output schema again from its upstreams on each call, and a join asks each input several times; `head()` and the notebook preview walked every path to check whether reading the schema would poll a source; and each operator or function pod call formatted its input streams, whose description nests every upstream, into a debug log message even with debug logging off. A stream or node now keeps its predicted output schema per column selection (`output_schema()`, `keys()`), the poll check visits each upstream once, and the log message is formatted only when debug logging is on (see Changed for the description). Job compilation, `save`, `PipelineJob.load` and a run's schema queries are linear in the nodes as well: each node is asked a bounded number of times. A schema that includes the system tags is still as wide as the joins make it, because each join keeps both inputs' system-tag columns: stacked split-and-join stages over one source double their number per stage (ITL-1114). `head()` and the notebook preview of a stream that has not run build their table of no rows from a table that carries the system tags whatever columns are shown, so over such stacked stages they are as wide as the joins make them (ITL-1132). No hash, stored table or golden value changes. `PipelineJob.bind(sources=...)` makes every node of the job predict its schema again (a source whose column has another type is refused, ITL-1112); assigning `bound_source` on a source node directly, or setting a node's `upstreams`, does not reach the nodes downstream of it, so use `bind`.
- A source slot's predicted schema with system tags (`output_schema(columns={"system_tags": True})`,
  `all_info=True`) gave the record-id column as `str`; every source's rows have it as `bytes`
  (Arrow `large_binary`). It is `bytes` now, for an unbound slot and for every bind, so the
  slot's prediction and every prediction after it are what the rows are, whatever is bound
  (ITL-1112).
- An observer that read a function job node in `on_node_end` emptied it: the node was marked current only after that hook, so the read found it stale and cleared the run's rows (ITL-1121). The node is now current before the hook, on sync, async and load-only runs.
- An operator job node without a database, or in `CacheMode.OFF`, whose output failed to compute in `run()` kept claiming a current run and its reads raised; it now reads as not run, like every node whose run raised.
- The run summary said "nothing to do; every node holds a current run" when the run ran a side-effect node (and delivered) or an operator node below a held function node. It now says so only when the run ran no node; `RunSummary` takes `ran_nothing`.
- A side-effect job node whose load-only `execute()` failed on its input stayed current, and so did the nodes below it; it is now cleared, as on the async path.
- An operator job node without a database, run with `load_only=True`, showed its rows as a full run's (`rows` instead of `loaded_only`) and a later full `run()` took them as its own.
- A side-effect job node over a stream outside a job, with a store attached, kept its rows as a current run when its `run()` was interrupted (`KeyboardInterrupt`) while committing the invocation log, so the next `run()` neither ran nor committed again; it is now cleared.
- A table with no rows has its populated table's columns in the same order (ITL-1131).
  A `Join` or `MergeJoin` job node after a run whose input had no rows (sync and async)
  listed its data columns before its system tags, where the same node after a run with rows
  lists them after. `Index` and `Pick` over an input with no rows, or whose rows were all
  skipped, dropped the system-tag columns, and `Pick` also gave its source-info columns as
  nullable and lost the token column of an `out=` column. Every operator's zero-row table now
  equals its populated table in column names, order, Arrow types and nullability.
- An operator node has one column order on every path, in memory and stored (ITL-1133): the
  row order of a job run (user tags, system tags, data, source info). A node run on its own
  (`node.run()`, without a store or in `LOG`) kept and stored its operator's own order (a join
  lists its data before its system tags), so its table differed from the same node's in a job
  and from its own table before a run, and a store written by both refused the second append.
  An operator cache stored in the operator's order by an earlier build reads in row order and
  accepts new rows; it is not rewritten and its path does not change. No hash or record id
  changes: a record id does not depend on column order.
- `DropTagColumns` predicts the system-tag columns its result has (ITL-1135):
  `output_schema(columns={"system_tags": True})` and `output_schema(all_info=True)` left them
  out, though the computed table keeps them. No hash, record id or stored path changes.
- A schema that holds a user class (an ordinary dataclass, an `orcapod.dataclass` or a pydantic model, also nested in another class, inside `list[...]` or as `T | None`) hashes the same whether or not anything has registered the class yet (ITL-1137). A class gets its logical type when a function pod names it or a source is built from its values; before that, hashing a type looked the class up only and hashed it by module path and qualified name, with no kind and no fields, so two different classes of one name hashed alike and a pipeline hash taken before registration differed from the same hash taken after. Such a hash is kept by the node, so one pipeline could store one node at two paths, depending on what the process built first: a job looked at (a node's hash, path or preview) before its source existed, over a `SourceNode` slot followed by operators or by function pods that never name the class. Hashing a type now registers a class that a factory covers, so the hash before registration is the registered one; a registered class hashes exactly as before, and no golden value or stored path of a registered class moves. The same holds for a parameter annotated with such a class when a function is hashed (a function used as a value, and the function signature hash a function pod stores with every result row as `__pf_var_function_signature_hash`): a pod defined before its parameter's class was registered stored the name-based signature hash, one defined after it the registered one; now both store the registered one. The signature hash is a recorded column, not a lookup key, so stored results are found as before. A class Orcapod has no logical type for still hashes by its name. A schema hashed on its own no longer raises `TypeError` for a class not registered yet. A store written by a development build of `main` before this fix may hold, for such a node, function pipeline tables (`pdb_v2`) and operator caches (`odb_v1`) under a `schema:<hash>` path component computed from the unregistered class; those tables are not read again (the node now reads its tables at the registered path), and function results (`rdb_v2`, keyed by the function) are not affected. No release wrote such tables.
- A job over a column of a user class (an ordinary dataclass, an `orcapod.dataclass` or a pydantic model) loaded with `PipelineJob.load` in a process where nothing has registered the class yet reads like the job that saved it (ITL-1138). The load cannot restore such a column's saved type to a class, so the source slot held the saved type text: its pipeline hash was the hash of that text, and binding the same source was refused ("data column 'p' is declared extension<…> but the source has A") unless the class had been registered before the load. A loaded source slot now keeps its saved pipeline hash, and its saved content hash while no source is bound, as loaded function and operator nodes keep theirs, and restores such a column's type when a source is bound (building the source registers the class). Every node then has the saving process's hashes, schemas, system-tag column names and stored rows, and `run(load_only=True)` finds every stored result. A slot whose class is still not registered at bind refuses the source with an error that names the column and the saved class and says how to register it. Saving such a loaded job again (`job.save`), with or without a bind, keeps each such column's saved nullability: the type text cannot give it, so before this fix a nullable user-class column (`A | None`) was written as non-nullable and a later load restored it as `A`. A job file saved on a build before this fix from a job loaded before its class was registered may record a nullable user-class column as non-nullable; save it again from the code that defines the job. No hash, golden value or stored table changes; the job file format is unchanged, and a file without a slot's saved hashes still loads.
- `MapTags`, `MapData` and `MapColumns` refuse, when built, a new name that a stream reads as a system column: one that starts with the system-tag prefix (`_tag_`) or the source-info prefix (`_source_`), or the context key column (`_context_key`) (ITL-1160). Before, such a rename was accepted and the column then vanished from the stream or the stream failed on read with `No data columns found`.
- A `ConnectorArrowDatabase` over `SQLiteConnector` or `PostgreSQLConnector` now refuses a table it cannot store before anything of it is written, with `UnstorableColumnError` naming the connector, the column, its Arrow type and what to use instead (ITL-1162). A connector stores a column only when it reads it back as written: on SQLite `bool`, `int64`, `double`, `large_string` and `large_binary`; on PostgreSQL also `int32`, `float`, `date32`, `timestamp[us]` and `timestamp[us, tz=UTC]`. Before, a function node on such a database ran its function and then failed at commit (`sqlite3.ProgrammingError: ... type 'list' is not supported`), timestamps were stored as text and read back as strings, and narrower integers and floats read back widened, failing the next write. A job now refuses a node whose stored table such a database cannot hold (every function node; a side-effect node on SQLite) when the job is built, before anything runs, naming the node. The connectors no longer fall back to a `TEXT` column for a type they have no column type for.
- `PostgreSQLConnector` now refuses a column name longer than 63 bytes (UTF-8), which PostgreSQL truncates, so it would read back as another name (ITL-1162). Before, such a column was stored under the truncated name. A table name longer than 63 bytes is refused too, with `UnstorableTableNameError`, at every entry that creates, writes, reads or checks a table, so a truncated name is never sent: two tables whose names share their first 63 bytes would otherwise silently be one. `SQLiteConnector` refuses a table name starting with `sqlite_`, which SQLite reserves.
- A `ConnectorArrowDatabase` over `SpiralDBConnector` now refuses, by the same rule, a column SpiralDB cannot store: a value column whose name starts with `__` (the `__record_id` key is allowed) or any column whose name contains `.`, and a timestamp column with a time zone, which SpiralDB reads back without it (ITL-1164). Before, a function node on such a database ran and then failed at commit (`Column name '__input_data_hash' has reserved prefix`). A job refuses every function node, side-effect node, operator cache and cached source on such a database when it is built.

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

#### Delta reads return every row the database's handles wrote (ITL-864)

Right after a first `PipelineJob` run on a `DeltaTableDatabase`, `node.get_all_records()` could
return only some of the rows (1 of 2 or 3) on both orchestrators. Handles made with `at()` share
their pending writes, and any handle's flush writes all of them, but each handle kept its own
loaded copy of the table, so a sibling kept reading the version it had loaded before the flush.
The loaded tables and the stored-id sets are now shared by every handle of one root. Every read
method (`get_all_records`, `get_record_by_id`, `get_records_by_ids`,
`get_records_with_column_value`) returns what a flush would leave: the stored rows plus the
pending rows whose record id is not stored, or `None` when there are none. Before,
`get_records_by_ids` ignored pending rows, `get_records_with_column_value` ignored stored rows
while a pending batch existed, and a pending row whose id was already stored could be returned
twice or instead of the stored row. Writes made by another process or another root on the same
path are still seen only after the table is reloaded. (ITL-864 #293)

#### A script that read a local Delta store no longer hangs at exit (ITL-892)

A process that had read a Delta table through `DeltaTableDatabase` or `DeltaTableSource` could
finish its work and then never exit: it hung in Arrow's thread-pool destructor, or, on Linux with
Python 3.11 or 3.12, aborted with `terminate called without an active exception`. It happened in
up to three runs in four on macOS and about one in ten on Linux, on every supported pyarrow and
deltalake version. The cause is upstream (apache/arrow#50482, apache/arrow#54536). deltalake's
default reader goes through a filesystem written in Python, and Arrow can release one of its
buffers on a worker thread while the interpreter is shutting down.

Orcapod now reads local Delta tables through a native pyarrow filesystem, which returns the same
schema and values. Tables on cloud storage (`s3://`, `gs://`, `az://`) still use deltalake's
reader and stay exposed until pyarrow ships the fix; see
[Troubleshooting](docs/troubleshooting.md).

#### An operator node returns its rows after an async run (ITL-896)

After a job ran on `AsyncPipelineOrchestrator`, an operator node (`job.nodes[label]` for
`Join`, `MergeJoin`, `SemiJoin`, `Batch`, `GroupBy`, `MapTags`, `MapData`, `PolarsFilter` and
the column-selection operators) returned no rows from `as_table()` and `iter_data()`, without
an error. It now returns the same rows as after a sync run, whatever `materialize_results`
is, and a later run replaces those rows even when it produces none. With the default
`cache_mode=OFF` (the only mode a job sets) an operator node keeps its output in memory and
stores nothing, so `get_all_records()` returns `None` for it on both orchestrators.
(ITL-896)

#### A loaded job can read its results (ITL-904)

A job loaded with `PipelineJob.load` can read its results. A function node's `as_table()` and
`iter_data()` return the rows the saved run stored, with their stored types (they used to raise
"no function pod and no database attached" while `get_all_records()` worked). Without a store,
reading says to pass `PipelineJob.load(path, database=...)` or call `job.bind(database=...)`. `save()`
now records whether the store held records of each function node (`run.node_has_stored_records`,
an optional field; the format version is unchanged). A node saved with records raises, naming
the store, when the attached store holds none of them, instead of returning no rows. When an
entry has several records (it was recomputed), a function node now reads the one with the
greatest recomputation index, on a live node too; it used to keep whichever record the store
returned last. Loading a job in which an operator consumes a function node no longer crashes
with `InputValidationError`, and a loaded node's `output_schema()` reports Python types; a saved
type it cannot restore raises an error naming the column. Registered logical types such as
`Path` restore normally, so downstream operators remain available. Saved
`timestamp`, `duration`, `time` and `decimal` types are now read back. Reading a loaded operator
node without a readable cache raises instead of returning an empty table; operators can store
outputs when caching is enabled. In-memory sources (`DictSource`, `ListSource`,
`ArrowTableSource`, `DataFrameSource`) are
logged at INFO with the remedy instead of a "skipping" warning. A loaded job stays read-only and
cannot be run again. See *Saving and loading a job* in the function-pods guide. (ITL-904)

Stored-record expectations count only rows of the node's saved data context. Operator
descriptors without a reconstructed operator still lack the saved runtime identity needed
to find cached rows; the runtime-descriptor change is deferred to ITL-977. Direct execution
of loaded operator nodes is rejected. A newer unavailable ephemeral result yields an
`EmptyData` placeholder instead of exposing an older persistent result.

#### A table with no rows has the types of the rows (ITL-912)

An operator node read before its run, after `clear_cache()`, or after a run that produced no
row, returned a zero-row table built from its Python output schema: `int8` became `int64`,
a non-nullable list item became nullable, source-info columns changed nullability and the
system-tag columns were missing. A function pod's stream and a function node did the same for
their tag columns, and left out the `_context_key` and source-info columns; a side-effect
pod's stream or node that delivered no row did the same, and so did `head()` and the notebook
preview of a stream or node with no rows to show. Both orchestrators built an
operator node's input the same way when an upstream node delivered no row, so a `Join` of
such an input with a non-empty one whose key is, say, `int8` raised `InputValidationError`.

Each of these tables now has exactly the columns, Arrow types and nullability of the rows,
on both orchestrators, and that `Join` completes with typed zero rows on both paths. Nothing
is computed, read or called to get them: an operator applies
itself to its inputs' zero-row tables, a function pod combines its input's tags with its
declared output types, a side-effect pod gives its input's, and a source gives the table or
schema it already holds. Nothing is
stored for an empty table, so no hash, stored table or path changes.

#### A `PipelineJob` on an in-memory store runs in time linear in its rows (ITL-919)

A `PipelineJob` on an `InMemoryArrowDatabase` runs in time linear in its rows. Every row a
function node processed used to filter, copy or rebuild the whole stored table (the
result lookup, the recomputation-index lookup, the duplicate check and the flush, each
over a table that gained one chunk per row), so 1,000 rows took about a minute. Lookups
now use hash indexes over the stored rows and a flush appends without copying them;
stored rows, ids and hashes are unchanged.

#### A `PipelineJob` on a Delta store runs in time linear in its rows (ITL-919)

A `PipelineJob` on a `DeltaTableDatabase` committed every row of every function node as
its own Delta version (a MERGE that reads the whole table) and re-read the stored table for
each per-row lookup, so 1,000 rows took about eight minutes. Function nodes inside
`job.run()` now commit every `commit_every_rows` rows (default 100, see *Added*); a batch
whose record ids are all new is appended instead of merged; and lookups are answered from
an indexed in-memory copy of the committed rows, read once and extended with each commit.
Stored rows, record ids, hashes, paths and schemas are unchanged. Behaviour changes: a store
failure that only a commit reveals (an unreadable table, a failed commit) surfaces at the
node's next commit point and names the node but no row, and the rows computed since the
last commit are computed again on the next run; `commit_every_rows=1` restores per-row
surfacing. A `DeltaTableDatabase` object keeps answering from the table version it loaded
until its own next commit (as before; it no longer re-reads the table per lookup).

#### An asynchronous run whose node fails raises instead of hanging (ITL-932)

On the asynchronous orchestrator, a node failure while more rows were in flight than the
channel buffers hold (about 16 to 64 rows) hung the run forever: the failure cancelled the
readers, and closing a full channel waited for room that never came. Closing a channel no
longer blocks; readers still receive every item sent before the close.

#### An asynchronous rerun of an ephemeral-result node recomputes its rows (ITL-923)

Rerunning a node with `NodeConfig(is_result_ephemeral=True)` on the asynchronous
orchestrator, after its ephemeral results were gone, neither recomputed nor recorded the
rows, and reading the node afterwards raised `EmptyDataAccessError`. It now recomputes them,
as the synchronous orchestrator does.

#### A failed side-effect completion lookup names the row

A `StoreReadError` from a side-effect pod's check of earlier deliveries now names the pod
and the row, as a failed write of the delivery record does.

#### SpiralDB table source (ITL-926)

- The `spiraldb` extra requires `pyspiral>=0.16.33` (was `>=0.14.0`). pyspiral 0.14.9 does not
  use the login saved by a current `spiral login`; 0.16.33 does, and keeps every call
  `SpiralDBConnector` makes.
- `SpiralDBConnector.get_column_info` and `iter_batches` normalize every column to its large
  Arrow variants at every nesting level (`list<string>` becomes `large_list<large_string>`),
  so both report the same types (ITL-40). Before, only top-level `string` and `binary`
  columns were converted. Widths, nullability and nested field names are unchanged, and so
  are all hashes.
- Building any source converts only its `record_id_column` to Python, not the whole table:
  about half the peak memory (200,000 rows: +210 MB down to +115 MB). Tokens, record IDs and
  hashes are byte-identical.
- SpiralDB keeps no non-null flag except on list items, so a Spiral column arrives as `T`
  only when the table carries Orcapod's nullability record (ITL-936, under Added); otherwise
  every column is `T | None`. The pyspiral fake now behaves like the server in this respect.
- A source whose table has a column the Arrow hasher refuses (for example `map`) raises
  `ValueError` naming every such column and its Arrow type, chained to the hasher's error.
  Before, it raised the hasher's bare `Unsupported leaf type: ...`, which named no column.
  `fixed_size_list` columns are hashed since starfix 0.5 (ITL-910).
- `SpiralDBTableSource` errors name the project and dataset searched. Before, a wrong
  project, dataset or table all gave `Table 'x' not found in database.`
- The docstrings give the real location of the saved `spiral login`
  (`~/Library/Application Support/pyspiral/` on macOS). `docs/concepts/sources.md` has a
  SpiralDB section: setup, discovery, nullable columns, and current limits.

#### A nested class stored as `None` reads back (ITL-881)

A dataclass or pydantic model with an optional nested class (`child: Inner | None`) that
stored `None` raised `TypeError: 'NoneType' object is not subscriptable` when the value
was read back. It now reads back as `None`.

#### Stored dataclass and pydantic values stay readable after Delta compaction (ITL-881)

Delta compaction (`optimize.compact()`) rewrites a `large_string` struct field as
`string`. Reading a stored dataclass or pydantic column with a `str` or `bytes` field then
failed, and a cached run returned 0 rows. Extension columns now accept large and non-large
layouts of the same type at any nesting depth, as they already did at the top level
(ITL-602). Known limitation: such a non-large column read over Arrow IPC or the C stream
interface (not Parquet or Delta) gives invalid arrays, at any depth; Orcapod's own writers
never produce one (ITL-894).

#### A node's preview shows its rows after an async run (ITL-1042)

After a job ran on `AsyncPipelineOrchestrator`, a function node downstream of an operator
node (for example the automatic column selection a pod call adds) was taken to be out of
date. Its first `as_table()`, `iter_data()` or `head()` read the store again instead of the
results in memory, and from then on every operator node downstream of it showed
`head()` as "not computed: inputs changed since the last computation", with no rows, although
`as_table()` and `iter_data()` returned every row. After a run on either orchestrator no node
is now out of date: `head()` shows the rows `as_table()` returns, and none of them reads the
store. Nothing was ever computed twice. (ITL-1042)

#### A chain of function pod streams is not computed again on its second read (ITL-1052)

In a chain such as `g.pod(f.pod(source))`, the last stream was taken to be out of date right
after it was read to the end, so its next `iter_data()`, `as_table()` or `head()` called `g`
again on every row (or, for a cached pod, looked every row up in its store again). A pod
stream now records that its results are complete when it has read its input to the end, so a
second read computes nothing. A change upstream (`clear_cache()`), also one made while the
chain is being read, still makes every stream after it out of date, and its next read
computes it again. (ITL-1052)

#### A loaded job keeps every node's identity, and a loaded operator reads its stored rows (ITL-1043)

A job saved and loaded with `PipelineJob.load` could not read the stored output of an operator
node. With a nullable tag or data column (the default for a table built with `pa.table` or
from pandas) the loaded node looked for its cache under another table path and reported "the
cache table is absent"; otherwise it found the table and returned zero rows, although rows
were stored.

- The saved file now records which columns of each schema may be null (`T | None`): the Arrow
  type text it saves cannot say it, so `int | None` was saved as `int64` and loaded as `int`.
  A node descriptor's `output_schema` gains `"nullable": {"tag": [...], "data": [...]}`, and a
  source node's `source_config` and a source's identity config the same record. Loaded source,
  function and operator nodes report the run's output schema, and `Pipeline.load` followed by
  `PipelineJob.from_pipeline(sources=...)` reads and writes the run's operator cache table.
- The saved file records the run content hash of each operator node in
  `run.node_job_content_hashes`, as it did for function nodes. A loaded operator node
  attached in `CacheMode.REPLAY` reads exactly the rows its run stored.
- A loaded operator node takes the pipeline hash saved with the job, its run's, instead of
  recomputing it from inputs rebuilt from the file.
- The store is the source of truth: a loaded operator node attached in `CacheMode.REPLAY`
  reads exactly as the same node built from code does. When the store holds no rows under
  its key (no table, or none of the table's rows), the result is empty, with the node's
  columns, and `head()` reports `NOTHING_STORED`. A run whose result was empty stores
  nothing and reads back the same way.
- A loaded operator node whose job file does not record its run's content hash cannot name
  its run's rows. When the store holds none under the hash the loaded job computes,
  `as_table()` and `iter_data()` raise a `RuntimeError` saying so, instead of returning zero
  rows, and `head()` shows no rows with that reason (`UNAVAILABLE`) instead of raising.
  `get_cached_output()` follows the same rule.
- Whether a saved file has the nullability and run-hash records is decided by the records,
  not by the format version.

No hash, stored path or golden value of a running job changes: only what a loaded job
reconstructs.

#### Two reads of a pod stream open at once compute each row once (ITL-1065)

With two `iter_data()` iterators open on one function pod stream fed by an operator stream
(for example the column selection a pod call adds when its input has a column the function
does not take), the second iterator threw away the rows the first had computed and computed
them again: one call too many, and a row the first iterator had already handed out was no
longer the stream's row. A read left partway and read again did the same. Such reads now share
the stream's results, whatever feeds it: each row is computed once and every read yields the
same rows. An upstream changed with `clear_cache()` between the reads still makes the next
read start again. (ITL-1065)

#### An operator stream with nothing to do keeps its result after a read (ITL-1083)

An operator that has nothing to do (for example `SelectDataColumns` naming every data column,
`MapData` renaming no column, `PolarsFilter()` with no predicate, or `Join` of one stream)
passes its input through. Over a function pod stream that had not computed yet, such an
operator stream threw away its own result as soon as it was read: `head()` right after
`as_table()` showed no rows ("not computed"), and the next read ran the operator again. A
function pod stream reading it was stale after a full read and called its function again on
every row of the next read. The input completing its first computation is no longer taken for
a change: the operator stream stays current, `is_stale` is `False`, and a second read neither
runs the operator nor calls a function. An input changed after the read (`clear_cache()`) still
makes the next read run the operator again. (ITL-1083)

### Known limitations

- A pipeline job keeps every row it processes in memory (about 5 KB per row and node), so a
  Spiral table of millions of rows can be iterated but not yet run through a job; the
  `max_rows` cap guards against it (ITL-186).
- A Spiral row's key is hashed with one call per row: about 0.2 ms per row, 18 s per 100,000
  rows (ITL-186).
- A Spiral source does not poll for new rows within a run and has no watermark: a new run
  scans the (restricted) table again and computes only rows it has not seen (ITL-186).
- `frozenset[T]` is not supported: declare `set[T]`. Support is to be evaluated in ITL-1140.
  (ITL-611)
- An async channel carries rows, not a schema. Where the sync path returns an empty stream
  with typed columns, the async path emits nothing, and an operator node that received no
  rows reads back columns typed from its Python output schema (`int8` becomes `int64`).
  Giving async execution the stream's schema is ITL-858; the read-back types are ITL-912.
  (ITL-865)
- Previously stored rows remain unchanged and are not automatically recomputed or repaired.
  Python-value inputs (sources and datagrams) remain outside output validation (ITL-895).
- The declared type's logical type decides what is stored. A function that reads fields its
  declared parameter type does not have (the extra fields of a subclass returned for a base
  type, or the time of a `datetime` declared as `date`) gets results that can depend on the
  execution path and can be reused across values that differ only in those fields
  (ITL-978).
- A `pa.Table` or `pa.RecordBatch` given to the semantic hasher as a value still hashes
  without regard to column order and struct field order (`arrow_v1`). Such a table cannot be
  a pod input or output (ITL-897, second part).
- pandas is capped below 3 (`pandas>=2.2.3,<3`) for v0.3.0. The pandas value hash and its
  round trip through storage are verified on pandas 2 only; pandas 3 changes the default
  string dtype and other behaviour the hash depends on, and has not been checked. ITL-1054
  lifts the cap.
- A zero-row table is still built from the Python output schema (`int8` reads as `int64`,
  system-tag columns are absent) when an upstream cannot say its Arrow types without rows: a
  `PollingSource` before its first poll and an unbound input slot or `SourceProxy` (both are
  declared with a Python schema only), a `DerivedSource`, and a loaded job node with no live
  upstream and no stored table. `Index` and `Pick` over zero rows still lack the system-tag
  columns (ITL-985). These cases are tracked in ITL-1089. (ITL-912)
- A source-info column (`_source_*`) of an operator node's rows is nullable only when some row's
  provenance token is `None`, a rule that depends on the rows. A zero-row table has no token,
  so it declares the column non-nullable, as rows with a known token do; when the rows have an
  unknown (`None`) token, the field's nullability differs between the zero-row table and the
  rows. A row-independent rule would change the operator node's stored record hashes. Tracked in
  ITL-1089. (ITL-912)
- **A non-default ignore list is not usable end to end in a pipeline in v0.3.** A data
  context built from a custom logical type registration file hashes bare values and Arrow
  tables with its list, but streams and datagrams find their data context by key, so their
  rows are read by the default context, which refuses a column of another list. (ITL-981,
  ITL-965)
- A job or pipeline file written before these records (format `0.1.0`, or a `0.2.0` file
  written before this change) lacks the `"nullable"` record in every `output_schema`,
  `source_config` and source identity config, and lacks the operator nodes' entries in
  `run.node_job_content_hashes`. It still loads, lossy: every column reads as non-null, and
  a loaded operator node that finds no rows under the content hash it computes raises the
  error above. Saving such a loaded
  job again does not record what its file lacked. This change does not rewrite or upgrade
  old files; that is the job of the migration tool after v0.3 (ITL-1048). (ITL-1043)
- A `pd.ArrowDtype` decimal column is read back as an `object` column of `Decimal` values.
  Every value comes back exactly, digits and scale included. The dtype and its declared
  precision do not, so two precisions hash equal. (ITL-1054; left for a developer decision)
- The value hashes and logical type identifiers of the library value types are provisional:
  `numpy.ndarray`, `torch.Tensor`, and pandas and polars `DataFrame` and `Series`. They will
  be reviewed for v0.4 and may change then; results computed from such values under v0.3 may
  then be computed again (nothing stored is lost or wrong). Using one of these types issues
  `ProvisionalHashWarning` once per type per process: a value hashed, a source column, a
  function pod parameter or output, or stored results read back. The types are listed in the
  `provisional` list of `orcapod/contexts/logical_types.json`; the list changes no hash,
  identifier or stored path. See "Provisional value hashes" in `docs/concepts/arrays.md`
  (ITL-1068).
- `StreamView.sort_by_tags()` orders rows with equal tags by the system tags the view holds;
  the view `run()` returns holds none, so such rows keep their order, where
  `as_table(columns={"sort_by_tags": True})` orders them by their system tags. (ITL-1081)
- An operator that hands its input on unread (a select that drops nothing) over a stream not
  yet computed judges its own result stale once that input finishes computing: `head()` right
  after `run()` shows no rows, and the next read runs the operator again (ITL-1083).
- The bind check compares Python types, so a source that differs from its slot only in
  Arrow width (`int32` against `int64`) is still accepted (ITL-949).
- A connector job database cannot hold function nodes in v0.3.0: neither SQL connector (SQLite, PostgreSQL) nor SpiralDB. A function node's result table holds list-of-struct executor columns, which neither SQL connector stores, and `__`-prefixed value columns, which SpiralDB reserves (ITL-1164). A side-effect node is refused on SQLite (its log's UTC timestamp) and on SpiralDB, and an operator cache or cached source on SpiralDB (`__data_context`). Use a Delta database (`DeltaTableDatabase`) or `InMemoryArrowDatabase`. Storing such columns on connector databases is planned for a later release (ITL-1162, ITL-1164).
- On PostgreSQL, an operator cache with caching on is refused in v0.3.0 when it is written (before anything is stored): its system-tag column names (`_tag_source_id::` and a 64-character hash) are longer than PostgreSQL's 63-byte limit. Storing them under a mapped name is planned for a later release (ITL-1166).
- On PostgreSQL in v0.3.0, a table whose Orcapod path gives a name longer than 63 bytes is refused. Every node table and cached source path holds a 64-character hash, so only tables written directly through `ConnectorArrowDatabase` under short paths fit; reading an existing table with `PostgreSQLTableSource` is unaffected. Mapping long names is planned for a later release (ITL-1166).

#### Zero-row tables over non-large inputs (ITL-912)

A source holding `string`, `binary` or `list` columns gave a function node's or operator
node's table with no rows the non-large types, while the table with rows has the large ones
(`large_string`, ...). The zero-row table now takes its input's types in the large-normalized
form, as the populated table does, and an operator node's zero-row table lists its columns in
the order its rows do (a join's system tags before its data). A source's own table is not
changed.

A side-effect pod's stream takes its input's zero-row types in the same large form; a
`PolarsFilter()` with no predicate shows its input's types, as it passes the input through; and
a `CachedSource` over a source with no rows, with nothing stored, reads with the source's own
Arrow types and system-tag columns, as its `head()` shows them.

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
