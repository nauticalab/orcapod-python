# Orcapod Docs: Coverage Gap Report (PLT-4816, part 1)

Date: 2026-11-09. Audited against `main` at `61d0f18e`.

**Bottom line:** the docs now tell the truth about what they cover, but they cover only the
in-memory half of Orcapod. A newcomer can follow them through sources, function pods and
operators; for running a pipeline against a database there is now one correct section, and for
file types, side effects and troubleshooting there is nothing. The two existing hands-on
materials outside `docs/` (the tutorial notebook and the example script) do not run on `main`.
A workshop tutorial has to be written; an outline is at the end.

## What was audited, and how

- Every fenced `python` block in `README.md`, `docs/index.md`, `docs/getting-started.md`,
  `docs/concepts/*.md` and `docs/reference/*.md` was executed against `main`, and what each
  block prints was compared with the output the page shows. This is now a permanent test:
  `tests/test_docs/test_doc_samples.py`.
- Before the fixes: 45 blocks, 21 passing, 24 failing (20 raised, 4 showed wrong output),
  across 6 of the 8 pages that had samples.
- After the fixes: 55 blocks on 9 pages; 53 run and pass, 2 are skipped (both need a
  Postgres server). The 10 additional blocks are new samples and fragments made runnable.
- `mkdocs build --strict` failed with 11 warnings and now passes; it is also a test
  (`tests/test_docs/test_mkdocs_build.py`).
- Prose claims about behaviour were checked by running code, not by reading it. Where this
  report says the library does something, that was observed.

Not audited: `docs/reference/hashing.md` beyond running its one sample (487 lines of hash
derivations; it needs a reviewer who owns the hashing design), the auto-generated API pages
beyond "every name resolves and the build is clean", and the internal directories
(`docs/design`, `docs/plans`, `docs/specs`, `docs/metamorphic`).

## Coverage along the newcomer's path

| Step | State | Where |
|---|---|---|
| Install | Thin | `docs/index.md` |
| First pipeline | Covered well | `docs/getting-started.md` |
| Sources | Covered for in-memory sources; file/DB/polling sources missing | `docs/concepts/sources.md` |
| Function pods | Was wrong, now fixed | `docs/concepts/function-pods.md` |
| Operators (Join, Batch, GroupBy, ...) | Covered well; small gaps fixed | `docs/concepts/operators.md` |
| Running a `PipelineJob` with a database | Was wrong, now fixed; still only one section | `docs/concepts/function-pods.md` |
| Caching and re-runs | Was wrong, now fixed | `function-pods.md`, `identity.md` |
| Inspecting results | Partial | `streams.md`, `function-pods.md` |
| File and path types | Missing | (only `file-hash-caching.md`, which assumes you already use them) |
| Common errors | Missing as a page; a few now noted inline | — |

### Install — thin

`docs/index.md` shows `pip install orcapod` and `uv add orcapod`, nothing else. Fixed here:
the page also showed an install-from-GitHub command and `mkdocs.yml` had a `repo_url`, both
pointing at a stale location. The repository is private and Orcapod is distributed through
the organization's internal package index, so the git install, the PyPI link and the site's
repository link were removed. (Releases up to 0.1.4 are on public PyPI; the page does not
point readers there.) The section now lists the supported Python
versions (3.11, 3.12 and 3.13) and marks the exact install command as pending.

Missing: the exact install command and index URL, the optional extras (`redis`, `ray`,
`postgresql`, `spiraldb`), and a "check your install" snippet. Two things a workshop will hit
on day one:

- `pygraphviz` is a declared dependency and needs the Graphviz C headers to build. On a
  machine without them `uv sync` fails. (Being removed under ITL-134.)
- Importing Orcapod prints nine `UserWarning: Protobuf gencode version 5.29.5 is exactly one
  major version older than the runtime version 6.31.1` lines to stderr, and
  `DeltaTableDatabase` writes print `WARN deltalake_core::writer::stats Skipping column ...`
  lines. Harmless, but a newcomer will ask.

The index URL and install command are not known yet; fill them in once the internal registry
is set up (ITL-810 owns the version and install strings).

### First pipeline — covered well

`docs/getting-started.md` walks source → inspect → function pod → inspect result, and all 11
samples run. Fixed: the shown schema output was out of date in two samples
(`Schema({'experiment': <class 'str'>})` is now printed as `Schema({'experiment': str})`).
Added: the exact-column rule, that every parameter needs a type (an annotation or an
`input_schema` entry), and a pointer to `PipelineJob` for keeping results.

### Sources — partly covered

Covered well: `DictSource`, tag versus data columns, provenance columns, `ColumnConfig`.

Was wrong, now fixed:

- System tag columns are named `_tag_source_id::<hash>` and `_tag_record_id::<hash>`. The
  pages gave the prefix as `_tag::`, which matches no column. (`CLAUDE.md` and `.zed/rules`
  repeat the old prefix in their "Column naming conventions" table; not changed here.)
- The `ColumnConfig` sample used a `source` that was not defined yet.
- `DataFrameSource` accepts Polars as well as Pandas frames; `ListSource` requires `name`.
- A `DerivedSource` comes from `job.nodes[label].as_source()`, not from a `FunctionNode`.

Missing: a worked sample for each of `CSVSource`, `DeltaTableSource`, `DataFrameSource`,
`ListSource`; the database sources (`SQLiteTableSource`, `PostgreSQLTableSource`,
`SpiralDBTableSource`); `PollingSource` (data arriving over time, cursors); `CachedSource` and
`source.cached(db)`; what `source_id` means and when to set it. That last one matters more
than it looks — see "Things a newcomer will trip on".

### Function pods — was wrong, now fixed

- **Extra columns (ITL-807).** The page said a pod extracts the columns it needs from a wider
  stream. It does not: an extra data column, a missing column for a parameter without a
  default, or a column of an incompatible type raises `ValueError` (a parameter with a default
  may be left out; the default is used). The page now states the strict rule and shows the
  `SelectDataColumns` step.
  Whether to relax the behaviour is the open v0.3 decision.
- **Multiple input streams.** The sample used undefined names; it is now runnable.
- Every parameter needs a type, from an annotation or the decorator's `input_schema`
  (`ValueError` at decoration time otherwise); this was not stated.

Missing entirely:

- `FunctionPod.from_fn(...)` and context injection (`ctx_arg_name`, `InvocationContext`).
- Side effects: `side_effect_pod`, `sink_pod`, `tap_pod`, `SideEffectPod`.
- `PodConfig` versus `NodeConfig`, `job.apply_node_config(...)`.
- Ephemeral outputs (`set_ephemeral_store`).
- Executors (`LocalExecutor`, `RayExecutor`), post-run hooks, error policy
  (`fail_fast` versus continue).
- Returning several outputs, optional outputs, and what happens when a function raises (in
  memory, the exception propagates unchanged).

### Operators — covered well

Join, Batch and GroupBy were already documented with runnable samples and correct output.
Fixed: `MapTags`/`MapData` take `name_map=`, not `mapping=`; the column-selection and
`PolarsFilter` samples ran against the wrong `source` (they reused the GroupBy one, which has
no `age` or `weight` column); the "Using operators" samples used undefined streams. Added
directly, because each was one sample or one paragraph: runnable `MergeJoin`, `SemiJoin` and
`MapData` samples, the `Batch` partial-batch rule, a `Pick`/`Index` paragraph, and a section
on the fluent stream methods (`source.join(...)`, `.polars_filter(...)`, ...).

Missing: worked samples for `Pick` and `Index`; how system tags evolve through joins and
aggregations in user terms; when to pick `Batch` versus `GroupBy` with a realistic example
followed by a function pod that consumes the lists.

### Running a `PipelineJob` with a database — was wrong, now fixed, still thin

The page's database example constructed `FunctionNode(function_pod=..., input_stream=...,
pipeline_database=db, result_database=db)` and called `node.run()`. `FunctionNode` no longer
takes databases and has no `run()`. The section is replaced with a tested `PipelineJob`
example (build in a `with` block, `job.run()`, read `job.nodes[label]`).

Missing — this is the largest gap, and it is the core of the product:

- A page of its own for `Pipeline` and `PipelineJob`: blueprint versus job,
  `PipelineJob.from_pipeline(pipeline, sources=..., store=...)`, `job.bind(...)`, unbound
  sources and partial runs, labels and how nodes are named.
- `DeltaTableDatabase` on disk: where files go, how `name` scopes them, what is safe to
  delete.
- `job.save()` / `PipelineJob.load()` and what a loaded job can do (inspect, not run).
- Sync versus async orchestrators, observers (`StatusObserver`, `LoggingObserver`), captured
  logs.
- The `orcapod` CLI.

### Caching and re-runs — was wrong, now fixed

Three claims in `identity.md` did not match the code:

- "If you modify a function (change its code, ... bump its version) ... the pipeline hash
  changes." Observed: editing the body, or bumping `v0.0` to `v0.1`, changes nothing and the
  old stored result is returned. Only the name, the output schema and the **major** version
  are part of a function's identity.
- "Phase 1: read all existing records from the shared table and yield them." Observed: a
  node's stream holds the results for its current inputs only; `get_all_records()` returns
  the whole shared table.
- "For in-memory sources the content hash includes the data values." True only when
  `source_id` is left to default.

All three are corrected, each with a sample that asserts the behaviour.

Missing: how to clear or invalidate a cache deliberately (`clear_cache`, deleting a table),
`table_scope` (`pipeline_hash` versus `content_hash`), `is_stale` / `updated_since`.

### Inspecting results — partial

Covered: `output_schema()`, `keys()`, `iter_data()`, `as_table()`, `ColumnConfig`. Added:
`as_pandas_df()` / `as_polars_df()` / `as_lazy_frame()`, `job.nodes[label]`,
`get_all_records()`.

Missing: reading the meta columns (`__record_id`, `__input_data_hash`, ...), tracing a value
back to its source row through `_source_*` and the system tags, `show_graph()`, the
observability reader.

### File and path types — missing

Observed to work: `op.File("a.txt")` and `pathlib.Path` values in a `DictSource` (schema
shows `File` / `Path`), and function pods annotated `-> Path`. None of this is documented.
`docs/concepts/file-hash-caching.md` explains caching of file hashes in detail but assumes the
reader already knows `op.File` and `op.Directory`. It was also not in the site navigation
(now added) and every sample on it failed, because it called `op.enable_file_hash_caching`,
which does not exist at the top level (see the library findings).

Missing: a page on `op.File`, `op.Directory`, `op.Path`, `op.UPath`: what is hashed (content
versus location), how a changed file is detected, directory exclusion patterns, list-valued
file columns after `Batch`/`GroupBy`.

### Common errors — missing

There is no troubleshooting page. Observed messages worth collecting on one:

| What the user did | What they get |
|---|---|
| Stream has a data column the function does not take | `ValueError: Incoming data data type Schema({...}) is not compatible with expected input schema Schema({...})` |
| Parameter without a type annotation | `ValueError: Parameter 'age' has no type annotation and is not specified in input_types.` |
| `tag_columns` names a missing column | `ValueError: tag_columns not found in table: {'zzz'}. Available columns: ['a']` |
| Every column is a tag | `ValueError: No data columns found in the table. At least one data column is required.` |
| `Join` on streams sharing a data column | `InputValidationError: Input streams are not compatible: Datas should not have overlapping keys ...` (use `MergeJoin` or rename) |
| `job.run()` without a store | `ValueError: PipelineJob.run() requires a store. Call job.bind(store=db) before run(). ...` |
| Several `output_keys` but the function returns a single value | `ValueError: Values returned by function must be sequence-like if multiple output keys are specified` |
| Edited a function, results did not change | No error. Stale cached results; bump the major version. |

## Things a newcomer will trip on

These are behaviours, not doc errors. They are documented truthfully now, but each will cost
workshop time unless the tutorial addresses it head-on.

1. **Editing a function does not recompute.** By far the most likely surprise in a hands-on
   session, where people edit and re-run constantly.
2. **Function pods reject extra columns** (ITL-807). Nearly every realistic source is wider
   than one function's inputs, so `select_data_columns` is needed almost immediately.
3. **An explicit `source_id` is the source's identity.** Two `DictSource`s with the same
   `source_id` and schema but different rows have the same `content_hash()`.
4. **Default versions differ.** `@function_pod` defaults to `version="v0.0"`;
   `FunctionPod.from_fn` defaults to `"v1.0"`.
5. **`drop_unmapped` defaults differ.** `MapData(...)` keeps unmapped columns;
   `stream.map_data(...)` drops them.

## Suspected library problems found

Reported to the coordinator rather than fixed or worked around; the docs describe what the
code does today.

1. `orcapod.enable_file_hash_caching` does not exist. The page (and the function's own
   docstring, which speaks of `op.File`) reads as if a top-level export was intended.
   Reproduction: `import orcapod as op; op.enable_file_hash_caching()` →
   `AttributeError: module 'orcapod' has no attribute 'enable_file_hash_caching'`. Works as
   `from orcapod.contexts import enable_file_hash_caching`.
2. Empty data gives a misleading error. `DictSource(data=[], tag_columns=["a"])` →
   `ValueError: tag_columns not found in table: {'a'}. Available columns: []`. `CLAUDE.md`
   says this case raises `ValueError("Table is empty")`.
3. A schema mismatch writes a stray line to stderr before raising:
   `Key 'age' not found in parameter types.` (or `Type mismatch for key 'age': ...`).
   Reproduction: apply a pod taking `(weight, height)` to a stream that also has `age`.
4. The mismatch error text reads "Incoming data data type" (a doubled word).
5. `examples/save_and_load_pipelines.py` fails on `main`:
   `AttributeError: 'DeltaTableDatabase' object has no attribute 'register_tracker'`
   (it calls `Pipeline("my_pipeline", database)`; `Pipeline` no longer takes a database).
6. `notebooks/tutorials/01_introduction_to_orcapod.ipynb` uses the removed API
   (`Pipeline(name=..., pipeline_database=...)`, 19 uses of `FunctionNode(`). Not executed
   here, but it cannot run as written.
7. Items 4 and 5 under "Things a newcomer will trip on" (inconsistent defaults) look
   unintended.
8. Stale docstrings, fixed in this PR with the coordinator's approval because they broke the
   strict docs build: `DeltaTableDatabase.add_records` documented `arrow_table` and
   `id_columns`; `get_records_by_ids` documented `add_record_id_column` and
   `preserve_input_order`. Neither function has those parameters.

Also stale but outside this PR's ownership: the "Lower-level details on pipeline workflow
(tentative)" section of `README.md` (it describes `pipeline.run(execution_engine=...)`; one
plainly wrong line, the `auto_compile` default, was corrected), and the README has no user-facing
introduction at all.

## Proposed outline: self-paced workshop tutorial

Audience: scientists comfortable with Python, new to Orcapod. One running dataset throughout
(for example, a small set of recording sessions: a CSV of subjects, a folder of per-session
files). Each part is a page under `docs/tutorial/`, about 20 minutes, ends with two or three
exercises with solutions, and every sample runs under the docs test harness.

0. **Setup (10 min).** Install, extras, check the install, the warnings you can ignore.
1. **Your first stream.** `DictSource` and `CSVSource`; tags versus data; `output_schema`,
   `iter_data`, `as_pandas_df`. *Exercise:* choose tag columns for a given table; provoke and
   read the two construction errors.
2. **Computing with function pods.** `@function_pod`, type annotations, single and multiple
   outputs, the exact-column rule and `select_data_columns`. *Exercise:* fix a pod that fails
   on a wide stream.
3. **Reshaping with operators.** `join`, `semi_join`, `map_data`, `polars_filter`; `Join`
   versus `MergeJoin`. *Exercise:* combine subjects with measurements and filter.
4. **Many rows into one.** `GroupBy` and `Batch`, then a function pod over `list[T]` inputs
   (per-subject summary). *Exercise:* per-session mean from per-probe rows.
5. **Keeping results: `PipelineJob`.** Record in a `with` block, `job.run()`, `job.nodes`,
   `InMemoryArrowDatabase` versus `DeltaTableDatabase`. *Exercise:* run, quit Python, run
   again, show nothing was recomputed.
6. **Re-runs and caching.** Add rows and re-run; edit a function and see the stale result;
   bump the major version. What identity means (`content_hash` / `pipeline_hash`) in one
   page, linking to the concept page. *Exercise:* predict which of four edits recompute.
7. **Working with files.** `op.File`, `op.Directory`, `Path` outputs; pods that read and
   write files; file hash caching for large files. *Exercise:* a pod that writes one output
   file per session.
8. **Inspecting and tracing.** Meta and provenance columns, `get_all_records()`,
   `as_source()` to start a second pipeline from stored results, `show_graph()`.
9. **When things go wrong.** The common-errors table above as a troubleshooting page, plus
   error policy and captured logs.
10. **Where next (optional).** Side-effect pods (`sink_pod`), context injection, executors
    and Ray, `PollingSource`, saving and loading jobs.

Prerequisites to write it: the `PipelineJob` concept page and the file-types page listed above
(parts 5 and 7 need them), and a decision on ITL-807, since part 2 is written around the
strict rule. Replace or delete `notebooks/tutorials/01_...ipynb` and
`examples/save_and_load_pipelines.py` at the same time, so that no hands-on material on `main`
fails.
