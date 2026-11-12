# ITL-910 — Adopt starfix 0.5.0 (hash format 2) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move Orcapod's Arrow hashing onto starfix 0.5.0 (hash format 2, prefix `00 00 02`), bump the
Arrow hash label `arrow_v0.1` → `arrow_v0.2`, and regenerate the golden hashes once, explicitly.

**Architecture:** `StarfixArrowHasher` (`src/orcapod/hashing/arrow_hashers.py`) keeps calling
`ArrowDigester.hash_table` / `hash_schema` with the default (order-independent) options; only the
dependency, the label and the tests/docs/golden data change, plus whatever the new starfix rejections
require. No `ordered_columns` / `ordered_struct_fields` (developer decision 5). No legacy 0.4 mode.

**Tech Stack:** starfix 0.5.0 (pure Python, `pyarrow>=19`), pyarrow 23.0.1 (locked; Orcapod floor already
`>=20`, so no pyarrow floor change), pytest, uv.

Branch: `eywalker/itl-910-adopt-starfix-05-hash-format-2-in-orcapod` (Linear gitBranchName).

---

## Status log (for a successor)

- 2026-11-11: investigation done (below). Plan sent to coordinator via `ask` — WAIT for the answer
  before Task 4 (golden regeneration) and before any migration-related code.

## Investigation results

### starfix 0.5.0 API (installed wheel, README, `arrow_digester.py`)

- Same entry points as 0.4: `ArrowDigester(schema, ...)`, `update`, `finalize`, `hash_schema`,
  `hash_record_batch`, `hash_table`, `hash_array`. New keyword-only options `ordered_columns`,
  `ordered_struct_fields` (not used here). `include_metadata` unchanged.
- Every hash starts `00 00 02`; all values differ from 0.4.
- `ValueError` (was `NotImplementedError` in 0.4) for data of Null/Map/Interval/Utf8View/BinaryView,
  any row count; `ValueError` for ListView/Union/RunEndEncoded; new `ValueError`s: ambiguous node
  paths (duplicate names, `"a/b"` aliasing struct `a` child `b`), non-nullable field with a visible null,
  duplicate metadata keys with `include_metadata`. `TypeError` for wrong container type (Orcapod
  converts RecordBatch → Table first, unaffected).
- FixedSizeList data now hashes (any depth), distinct from `List`/`LargeList` of the same values.
- Canonical extension types (`arrow.uuid`, `arrow.fixed_shape_tensor`) now hash.

### Orcapod callers

`grep -rn starfix src`: only `src/orcapod/hashing/arrow_hashers.py` (`hash_schema`, `hash_table`).
Everything else (sources' default `source_id`, record ids, `__input_data_hash`, base/pipeline entry
ids, OperatorNode record hashes, `Data`/`Tag` content hashes, `ArrowTableHandler` for `pa.Table`,
pandas DataFrame/Series handlers) goes through `StarfixArrowHasher`.

### Behaviour changes visible to an Orcapod user (probe: `ArrowTableSource(t, tag_columns=["k"])`,
`hasher.hash_table`, `hasher.hash_schema`, 0.4 vs 0.5)

| Input | 0.4 | 0.5 |
|---|---|---|
| `fixed_size_list` column (top level, in struct, in list, with null slot) | `NotImplementedError` (source fails) | hashes; source constructs |
| homogeneous `tuple[T, T]` pod output (maps to `fixed_size_list`) | fails to hash | works |
| `arrow.uuid`, `arrow.fixed_shape_tensor` extension columns | `NotImplementedError` | hashes |
| `hash_schema` with null / view / interval types | `NotImplementedError` | hashes |
| **zero-row** table with a `map` column | hashes | `ValueError` |
| non-nullable field with a visible null | hashes silently | `ValueError: Non-nullable field 'x' contains nulls` |
| duplicate column names (raw hasher, e.g. a `pa.Table` value) | hashes | `ValueError: Ambiguous node path` (sources already failed with `KeyError`) |
| column `"a/b"` next to struct `a{b}` | hashes | `ValueError: Ambiguous node path 'a/b'` |
| `map` (rows), `null`, view, interval data | `NotImplementedError` | `ValueError` (same rejection, new class) |
| `CachedSource._build_merged_stream` | `except NotImplementedError` silently served from cache when live ingestion hit an unhashable type | now raises (the hashing error is no longer mistaken for "source cannot provide data") |

ITL-848 collisions through the public interface (`ArrowTableSource.iter_data()` row content hashes):
nested list lengths `[[[1]],[[2],[3]]]` vs `[[[1],[2]],[[3]]]` collide on 0.4, differ on 0.5; null struct vs
`{"a": None}` collide on 0.4, differ on 0.5; Bool column hash depends on chunking on 0.4, not on 0.5;
non-nullable `[True]` vs `[True, False, False]` collide on 0.4, differ on 0.5. Slice offsets are canonical
on both.

### Where the label lives

- `src/orcapod/hashing/versioned_hashers.py:40` `_CURRENT_ARROW_HASHER_ID`
- `src/orcapod/contexts/data/v0.1.json` `arrow_hasher._config.hasher_id` (+ changelog list)
- `src/orcapod/contexts/data/schemas/context_schema.json:178` (example value)
- tests: `tests/test_hashing/test_starfix_arrow_hasher.py` (label + three pinned format-1 digests),
  `tests/test_hashing/test_pandas_handlers.py` (label), `tests/test_core/operators/test_materialize_source_info.py`
  (three pinned format-1 digests), `tests/test_core/operators/test_async_sync_equivalence.py`
  (`PINNED_ROW_HASHES`, `PINNED_STREAM_CONTENT_HASH`),
  `tests/test_hashing/golden/cases.py` (docstring), golden data files.
- docs: `docs/reference/hashing.md` §3/§4 worked-example tables (regenerate with
  `superpowers/scripts/hash_audit_example.py`); `notebooks/tutorials/01_introduction_to_orcapod.ipynb`
  (outputs). Historical plans/specs under `superpowers/` are left as they are.
- Stored names: no table path embeds an Arrow hash under the default `table_scope="pipeline_hash"`.
  With `table_scope="content_hash"`, the path embeds the node content hash, which moves when a source's
  content hash moves.

### Golden estimate (simulated: starfix 0.5 overlay + label patched to `arrow_v0.2`)

| File | Entries | Changed | Unchanged | Keys added/removed |
|---|---|---|---|---|
| arrow.json | 53 | 53 | 0 | 0 |
| datagram.json | 171 | 40 | 131 | 0 |
| pipeline.json | 305 | 68 | 237 | 0 |
| schema.json | 92 | 22 | 70 | 0 |
| semantic.json | 64 | 1 (`value.dataframe.basic`, pandas via StarfixArrowHasher) | 63 | 0 |

Every changed entry is an Arrow-derived value (row data hashes, record/entry ids, source content hashes
and default source ids, `pa.Table`/DataFrame hashes). No pipeline hash, schema hash, system-tag column
name or table path moved.

### Migration finding (needs a decision)

Default `table_scope` paths do not move, so a v0.3 job pointed at a v0.2 store writes into the same
tables. Observed with `DeltaTableDatabase` and the docs' `bmi_demo` job: v0.3 `run()` recomputes both rows
(old record ids miss), and a later read without `run()` returns **4 rows for 2 inputs** (v0.2 + v0.3 mixed);
before the first v0.3 run, a read returns the v0.2 rows. Stored rows carry the label
(`__input_data_hash`, `__pipeline_base_entry_id` start `b"arrow_v0.1:"` vs `b"arrow_v0.2:"`). OperatorNode
REPLAY reads all rows matching the node content hash, so a v0.3 LOG-mode write next to v0.2 rows can
duplicate too.

### Full suite under starfix 0.5 overlay (label unchanged): 201 failed / 7400 passed / 96 skipped

- 186 golden entries (all Arrow-derived, see above).
- Pinned format-1 digests: `test_starfix_arrow_hasher.py` (5 tests), `test_materialize_source_info.py` (1),
  `test_async_sync_equivalence.py::test_issue_example_types_and_pinned_hashes`,
  `test_struct_layout_identity.py::TestStructHashesUnchanged` (4 params).
- `test_group_by.py::test_falls_back_to_record_id_when_key_covers_all_tags`: member order comes from
  record ids (uuid5 of the Arrow-hashed default `source_id`), so `["a","b"]` becomes `["b","a"]`. Arrow-derived.
- `test_array_v02_store.py` (2): the ITL-888 claim "an array-free pod reuses its v0.2 results" stops being
  true: every function node recomputes (input data hashes move) into the same tables (mixing finding).
- `test_async_sync_equivalence.py::test_arrow_hasher_hash_schema_with_extension_type` (strict xfail, ITL-846)
  now XPASSes: starfix 0.5 accepts a live extension type in `hash_schema`, but hashes it as its storage
  type, so `hash_schema(Path column) == hash_schema(large_string column)`.
- Same identity loss in `hash_table` for pyarrow's canonical extension types (`arrow.uuid`,
  `arrow.fixed_shape_tensor`): they are `pa.BaseExtensionType`, not `pa.ExtensionType`, so
  `normalize_extension_columns` skips them, `include_metadata` stays off, and an `arrow.uuid` table hashes
  equal to a `fixed_size_binary(16)` table. (On 0.4 they raised.)

## Tasks (after approval)

### Task 1: failing tests first
- [ ] New `tests/test_hashing/test_starfix_format2.py` (public interface: `ArrowTableSource`, `iter_data`,
  `content_hash`; the hasher itself via `resolve_context(None).arrow_hasher` for table-level checks):
  label `arrow_v0.2` and digest prefix `00 00 02`; the four ITL-848 cases above; Bool chunking; slice/offset
  and `combine_chunks` canonicality; `fixed_size_list` column hashes and differs from the same values as
  `large_list`; `ArrowTableSource` with `fixed_size_list` columns constructs, keeps the type, and runs through
  a function pod; pod returning `tuple[float, float]`; rejections with `ValueError` (zero-row map,
  non-nullable null, duplicate names via `pa.Table` value hash, `"a/b"` alias); `arrow.uuid` column hashes.
- [ ] Update `test_starfix_arrow_hasher.py` / `test_pandas_handlers.py` label assertions to `arrow_v0.2`.
- [ ] Run, confirm failures on starfix 0.4. WIP commit.

### Task 2: bump
- [ ] `pyproject.toml`: `starfix>=0.5.0,<0.6` (decision 6 floor); `uv lock`; report what moved in `uv.lock`.
- [ ] Label: `versioned_hashers.py`, `v0.1.json` (+ changelog line: "Hard cut (v0.3): arrow_v0.2 = starfix
  hash format 2 (starfix 0.5) …"), `context_schema.json` example.
- [ ] `StarfixArrowHasher` docstring: format 2, prefix `00 00 02`, rejections.
- [ ] Re-pin the format-1 digests in `test_starfix_arrow_hasher.py` and `test_materialize_source_info.py`
  (list old/new in the PR).
- [ ] Remove the generator header's "Known gaps (ITL-848)" items that are now hashable (fixed-size list,
  `arrow.uuid`); add golden cases for them (adding entries is allowed).

### Task 3: migration handling — per coordinator decision (see ask)

### Task 4: regenerate golden (approved regeneration)
- [ ] `uv run python tests/test_hashing/golden/generate_golden_hashes.py`; compare with
  `scratchpad/scripts/cmp.py` against main's files; every changed value must be Arrow-derived.

### Task 5: docs
- [ ] CHANGELOG `[0.3.0] — unreleased` → Breaking changes and migration: starfix format 2 entry.
- [ ] `docs/reference/hashing.md` worked-example tables, prefix note; notebook outputs.
- [ ] DESIGN_ISSUES: one appended entry (resolved, Fix note).

### Task 6: verification
- [ ] Full suite on 3.11, 3.12, 3.13, one at a time; ruff check/format; basedpyright exit code.
- [ ] Perf: 100k-row table hash, 0.4 vs 0.5; golden-test wall time before/after.

### Task 7: PR
- [ ] Merge origin/main, push, PR (`Fixes ITL-910`, mentions ITL-848, ITL-927), CodeRabbit rounds.

## D1 = D (developer, 2026-11-11): fail explicitly on rows hashed under another label

Design (tests first):
- New public `orcapod.errors.StoreHashLabelError(StoreError)` (always fails the run, whatever
  `error_policy`): attributes `record_path`, `found_label`, `current_label`; message names the table,
  both labels and the remedy (fresh store, or keep the version that wrote it; migration tool planned).
  No opt-out (`ignore_schema` does not disable it).
- Helpers in `src/orcapod/hashing/stored_hash_labels.py` return what they find (no raising):
  `foreign_arrow_label(values, current_label)` for `b"<label>:<digest>"` columns (only `arrow_v*`
  labels count); `foreign_hash_format(values, current_format_hex)` for hex-digest columns.
- Detection points (one full read per table per node/cache instance, D-ii; no protocol change):
  - FunctionJobNode pipeline table: `__pipeline_base_entry_id`; result table (via ResultCache):
    `__input_data_hash`. Checked at `execute` / `async_execute` start and on reads
    (`_fetch_joined_records`, `get_all_records`).
  - ResultCache `lookup` / `store` / `get_all_records` (covers standalone cached pods).
  - OperatorJobNode table: `_record_hash` hex, starfix format prefix vs current hasher's prefix.
  - CachedSource table: same hex prefix, before ingesting live data.
  - SideEffectJobNode invocation log: `record_id_hash` label, at execute/async_execute start
    (otherwise v0.2 completions are missed and side effects run again).
- `PipelineJob.run` pre-flight: every node's stored tables are checked before the orchestrator
  starts, so a run raises before computing or writing anything (array nodes on fresh tables would
  otherwise run first).

## Table path construction sites (sent to coordinator for ITL-938)

Where stored table paths are constructed (main + this branch; all relative to the store root unless noted). Job scoping: PipelineJob._distribute_databases / run (pipeline/job.py:411-412, 881-882): pipeline_db = store.at(*job_name); result_db = pipeline_db.at("_result").

1. Function node pipeline table: FunctionNodeBase.node_identity_path (core/nodes/function_node.py:337-358) = pod.uri + ("schema:{pipeline_hash}",) [+ ("instance:{content_hash}",) when table_scope="content_hash"]; FunctionJobNode._versioned_pipeline_path (:875) appends PIPELINE_DB_SCHEMA_VERSION "pdb_v1" (system_constants.py:23). Loaded jobs rebuild it from the descriptor: from_descriptor (:1152-1172, node_uri + schema: + instance:). pod.uri = data function uri (core/data_function.py:237): (canonical_function_name, output_data_schema_hash, "v{major_version}", data_function_type_id); FunctionPod.uri (core/function_pod.py:212) prefixes "side_effect_function" when a ctx arg is set.
   Legacy v0 path (unversioned node_identity_path) checked by _ensure_pdb_schema (:~890).
2. Function result table: CachedFunctionPod (core/cached_function_pod.py:55) -> ResultCache(record_path=pod.uri) under result_db ("<job>/_result/..."); ResultCache._versioned_record_path (core/result_cache.py:101) appends RESULT_DB_SCHEMA_VERSION "rdb_v1". Shared by every job/pipeline using the same function (not pipeline-hash scoped). Loaded stubs: _ResultDatabaseReader(record_path=descriptor node_uri) (function_node.py:644, :1034, :1152). Legacy v0 = unversioned path (_ensure_rdb_schema).
   CachedDataFunction (core/data_function.py:1010): ResultCache(record_path=record_path_prefix + data_function.uri) for function-level caching outside a node.
   Ephemeral results: FunctionJobNode._ephemeral_cached_pod (function_node.py:~1197) over ephemeral_database.
3. Operator node table: OperatorNodeBase.node_identity_path (core/nodes/operator_node.py:240-260) = node_uri + ("schema:{pipeline_hash}",) [+ ("instance:{content_hash}",)]; operator uri (core/operators/static_output_pod.py:54) = (ClassName, operator content_hash hex). No schema-version component. Row filter by __node_content_hash.
4. Side-effect invocation log: SideEffectJobNode.attach_databases (side_effects.py:~850) = node_uri + ("schema:{pipeline_hash}", TRACKING_DB_SCHEMA_VERSION "tdb_v1"); SideEffectPod.uri (side_effects.py:424) = ("side_effects", canonical_function_name). Rows keyed by record_id = b"<arrow label>:<digest>".
5. CachedSource cache table: CachedSource.cache_path (core/sources/cached_source.py:188) = explicit cache_path, else cache_path_prefix + ("source", "node:{source.content_hash()}"). Source content hash is Arrow-derived for default source_ids, so this path moves with the hash format.
6. Observers: LoggingObserver DEFAULT_LOG_PATH ("execution_logs",) (pipeline/logging_observer.py:65) and StatusObserver DEFAULT_STATUS_PATH ("execution_status",) (pipeline/status_observer.py:73), written inside a node-identity-scoped DB view (observer.contextualize(*node_identity_path)).
7. Backends map a path tuple to storage: DeltaTableDatabase (one directory per path, ':' stored as '!' on Windows), InMemoryArrowDatabase (key '/'.join), ConnectorArrowDatabase / SQLite / Postgres / Spiral (table name from path). DeltaTableDatabase also keeps pending writes per path.
8. Saved job files (PipelineJob.save/load, pipeline/serialization.py): descriptors carry node_uri, pipeline_hash, content_hash, table_scope, pipeline_path; loading reconstructs paths from these, not from live hashes.
9. Migrations / CLI: orcapod/migrations/pipeline_db.py, result_db.py (v0 -> v1 schema), cli/migrate.py (strips schema:/instance: components to find v0 paths).
10. Not in the store: file hash cachers (hashing/hash_cachers.py SQLite ~/.orcapod/file_hash_cache.db, table file_hash_cache, _SQLITE_SCHEMA_VERSION=1; postgres_hash_cacher.py _SCHEMA_VERSION=1).

Labels/versions currently embedded in path components: "semantic_v0.1:" (pipeline/content hashes in schema:/instance: and output_data_schema_hash), "v{major}", "python.function.v0", "pdb_v1"/"rdb_v1"/"tdb_v1". No path component carries the Arrow label; the D1 check (this branch) is isolated in hashing/stored_hash_labels.py plus one check_stored_hash_labels() method per table owner (FunctionJobNode, _ResultDatabaseReader, ResultCache/CachedFunctionPod, OperatorJobNode, SideEffectJobNode, CachedSource) and a pre-flight loop in PipelineJob.run, in its own commits (feat(store) ..., fix(store) ...).

Status 2026-11-11: D implemented and isolated; PR to be opened as DRAFT, v0.2-store handling awaits ITL-938.

## Worker report (2026-11-11)

PR: https://github.com/metamorphic-labs/orcapod-python/pull/309 (DRAFT; v0.2-store handling awaits ITL-938). CI green on
3.11/3.12/3.13, ruff, typecheck, license; CodeRabbit skipped the draft (not triggered; needs `@coderabbitai review` or
ready-for-review).

Verification: full suite 7695 passed / 96 skipped / 18 xfailed / 0 failed on each of 3.11, 3.12, 3.13 (skips: SpiralDB
integration, S3/MinIO, Graphviz, PostgreSQL opt-in); basedpyright exit 0; ruff clean; uv lock --check ok.

Golden: 184 values changed (arrow 53/53, datagram 40/171, pipeline 68/305, schema 22/92, semantic 1/64 =
value.dataframe.basic), 21 added (arrow.json), 0 removed; all changed values Arrow-derived. Golden files touched:
tests/test_hashing/golden/data/{arrow,datagram,pipeline,schema,semantic}.json, golden/cases.py,
golden/generate_golden_hashes.py.

Perf (100k rows, StarfixArrowHasher): numeric 50.7 -> 0.7 ms, large_string 44 -> 31 ms, large_list 4437 -> 500 ms,
struct 540 -> 534 ms; one-row hash 36 -> 43 us (+20%). Hashing-heavy tests 27.6 s vs 27.8 s.

PR #305 note: its `_unhashable_columns` wrapper (stream_builder.py) catches NotImplementedError; after both merge,
starfix raises ValueError naming column and type and fixed_size_list hashes: remove the wrapper (or catch ValueError) and
drop #305's 'fixed_size_list cannot be hashed' text; map stays unsupported.

Out-of-scope findings (not filed):
1. Function pods cannot declare tuple[T, T] / fixed-size-list parameters or outputs ('Unsupported annotation'), so
   fixed_size_list columns reach pods only via operators (ITL-927 'runs through a pipeline' is partial).
2. _checked_pdb_paths / _checked_rdb_paths (legacy v0 schema checks) key on the relative path only: two stores in one
   process share entries, so the second store's v0 check is skipped.
3. Operator-node and CachedSource record hashes store no label (only starfix's format prefix): a future label bump on the
   same starfix format is undetectable there.
4. Content-hash-scoped tables (table_scope='content_hash', CachedSource with default source_id) move paths, so their v0.2
   rows are neither found nor refused.
5. A projected/limited read on ArrowDatabaseProtocol would make the label check cheap (now one full read per table per run).
6. MetadataCapableProtocol (per-table metadata) is implemented by none of the main backends.
7. Tutorial notebook 01 still shows arrow_v0.1 values in two outputs (not re-executed).
8. The PipelineJob.run pre-flight also checks OFF-mode operator tables.
9. Correction to my first plan message: golden count is 184 (not 186) and tuple pods are not newly hashable.
