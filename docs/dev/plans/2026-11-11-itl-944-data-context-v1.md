# ITL-944: data context v1 (frozen manifest, `__data_context` column, new layouts)

Status: **implemented** (see "Outcome" at the end). Sections (a) to (h) below are the STEP 1 plan as sent; where the outcome differs, the outcome section wins.

- Linear: ITL-944. Design: ITL-938 design document, Parts 5.5, 6, 7, 8 (read at `bd7a4a6b`,
  re-checked unchanged before sending this plan).
- Branch `eywalker/itl-944-data-context-v1-frozen-manifest`, stacked on PR #309 (`b88f7eb3`).
- Probe script used for the measurements below (not committed): builds one job with a cached
  source, a function node, a stored operator and a sink on an in-memory store, prints every
  table path and schema, and saves the job.

## (a) Today (measured at `b88f7eb3`)

| Where | What is stored | Read back? |
|---|---|---|
| `contexts/registry.py` | one spec per `data/*.json`; `resolve_context_string` takes `None`, `v0.1`, `std:v0.1:default` (splits on `:` and takes part 2), `latest`. Default version hard-coded `"v0.1"` in `registry.py` and `contexts/__init__.py`. The JSON schema requires `v<int>.<int>`, so `v1` and `std:v1:default` are rejected today |
| `contexts/data/v0.1.json` | the only manifest. 44 commits touch it; 3 since tag `v0.2.0` (ITL-828, ITL-888, ITL-910). At HEAD it names `arrow_v0.2` and the new array format, so the file called `v0.1` no longer describes H0 |
| function pipeline table | `__input_data_context_key` (`large_string`), the input datagram's context key | never read |
| function result table | `_context_key` (`large_string`), the output datagram's context key | yes: `Datagram.__init__` takes the row's data context from it |
| operator cache, cached source, side-effect log | no context column | — |
| saved job file | `data_context_key` per node; read-only nodes resolve their context from it | yes |
| in-memory datagrams and streams | `_context_key` column under `ColumnConfig(context=True)` | yes |

**Finding 1 (differs from the brief's "no hash VALUE moves").** The context key string is
hashed in one place: the content hash of an `ArrowTableStream` without a producer covers
`as_table(all_info=True)`, which includes the `_context_key` column. Switching the default
key to `std:v1:default` (temporary experiment, reverted) moves exactly 3 golden values:
`schema:source_stream_builder.{default_source_id,named_source_id,record_id_column}.content_hash`.
No other golden value moves (737 of 742 pass; the other 2 failures are the process-independence
test comparing against the same 3). A function node fed by a bare stream inherits the change in
`__node_content_hash`; nodes fed by sources do not.

## (b) The manifest

- New file `contexts/data/v1.json`: `context_key` `std:v1:default`, `version` `v1`. Content =
  today's `v0.1.json` object specs (hashers, handler registry, type converter, logical types),
  with the changelog replaced by a `metadata` block stating what H1 is: labels
  (`semantic_v0.1`, `arrow_v0.2`, `ndarray_v1`, `polars_expr_v1`, `sha256`, `merkle_sha256`),
  libraries whose behaviour enters a hash (`starfix` 0.5.x, hash format 2), and the table
  layouts it writes. The full manifest format (identity compositions, recipes, naming grammar)
  is Part 8 item 2, a separate issue; not done here.
- `v0.1.json` is **removed** (proposal): it no longer describes H0, the code under it changed,
  and H0 belongs in `orcapod-sanctuary`. `std:v0.1:default` then fails to resolve with an error
  that says it is the pre-v0.3 generation. Alternative: keep it loadable (it would compute H1
  hashes under an H0 name).
- Default context becomes `v1`. JSON schema patterns relaxed to `v<int>` or `v<int>.<int>`.
- Frozen test: `contexts/data/released.json` maps each manifest version to the sha256 of its
  file bytes, or `null` while open. Tests: every manifest file is listed; a non-null digest
  must match; at most one open manifest and it is the default; if `orcapod.__version__` is a
  final release (no dev or local part, from hatch-vcs) no manifest may be open. Until the tag
  `v1` is `null`; the release-preparation commit sets the digest once. After that any edit to
  `v1.json` fails the test.

## (c) The column

- One column `__data_context`, `large_string`, non-null, on every row of: function pipeline,
  function result, operator cache, cached source, side-effect log.
- It **replaces** `__input_data_context_key` and the result table's `_context_key` (Part 8
  item 1).
- Value: the context key of the node or cache that writes the row (the context whose hashers
  made the row's keys). Mixed inputs (an input datagram whose context key differs from the
  writer's): proposal is to raise `InputValidationError` naming both keys before writing.
  Only one context ships, so this cannot happen without a custom registry.
- Operator cache and cached source: the column is appended after the per-row record hash is
  computed, so `_record_hash` values do not move.
- In memory nothing is renamed: `ColumnConfig(context=True)` still gives `_context_key`.
  A row read from a result table is filtered to the current context and built into `Data`
  with that context explicitly; the stored column is dropped.

## (d) Layout versions and paths

Constants: `pdb_v2`, `rdb_v2`, `tdb_v2`; new `odb_v1` (operator cache) and `sdb_v1` (cached
source), appended as the last component (also to an explicit `cache_path`).

Probe job `probe`, before and after (hashes shortened):

```
function pipeline  probe/double/semantic_v0.1:0180…/v0/python.function.v0/schema:semantic_v0.1:eda0…/pdb_v1  -> …/pdb_v2
function result    probe/_result/double/semantic_v0.1:0180…/v0/python.function.v0/rdb_v1                     -> …/rdb_v2
operator cache     probe/SelectDataColumns/6dc4…/schema:semantic_v0.1:742e…                                  -> …/odb_v1
cached source      probe/source/node:semantic_v0.1:d4f6…                                                     -> …/sdb_v1
side-effect log    probe/side_effects/_deliver/schema:semantic_v0.1:dd85…/tdb_v1                             -> …/tdb_v2
```

Existing v0 -> v1 machinery (`SchemaVersionError`, `NodeConfig(ignore_schema=...)`,
`orcapod.migrations`, `orcapod migrate`): it converts bare-path tables to `pdb_v1` / `rdb_v1`,
which v0.3 no longer reads, and its error contradicts "count and move on". Proposal: remove
all four; a bare-path (v0) table is an old layout like `pdb_v1`. Migration to H1 is ITL-913/914.

## (e) Reads and lookups

Filter on `__data_context == <current key>` in: the function node's whole-table pipeline read,
its recomputation-index query, `ResultCache.lookup` (an extra constraint) and
`ResultCache.get_all_records`, the operator-cache read, the cached-source read. Rows of
another context found in a current-layout table are counted and reported once per table.
At the tag every v2 table holds only `std:v1:default`, so the filter matters from context v2
on; tests write a second context through a test-only registry.

#309's label filtering is removed: `hash_labels_of`, `keep_rows_hashed_under`,
`report_skipped_rows`, `other_arrow_label`, `current_hash_format`, `stored_hash_labels` and
their tests. Replacement for listing generations (kept for the migration):
`orcapod.contexts.stored_data_contexts(database, record_path) -> dict[str, int]`; a table
without the column reports all its rows under `std:v0.1:default`.

Known limit, not solved here: record ids do not include the context, so a future context that
leaves a row's key unchanged collides with the old row on write. The design's answer is
migration relabels unaffected rows.

## (f) Old-layout tables

- Detection, once per table per process, by sibling path: `…/pdb_v1` or the bare path next to
  `…/pdb_v2`; same for `rdb`; the parent path of `odb_v1` / `sdb_v1`.
- Never read as current, never written. Rows counted, one warning per table:
  `Found N rows in stored table '<path>' written by an earlier version of Orcapod (older table
  layout, data context std:v0.1:default). They are not used and are left unchanged; they can be
  migrated later.` A job run adds one summary line with the total over its nodes.
- Side-effect log at `tdb_v1`, or rows of another context in `tdb_v2`: raise before any
  delivery. `SideEffectLogHashLabelError` is renamed `SideEffectLogDataContextError`
  (`record_path`, `found_context`, `current_context`, `node_label`).

## (g) What moves

- Golden: `pipeline:job.table_paths` and `pipeline:polars_filter_expr_job.table_paths`
  (2 entries, 4 paths each: layout component only), and the 3 content hashes of Finding 1.
  Nothing else.
- Saved job file: JSON layout unchanged (`data_context_key` per node stays); values become
  `std:v1:default` and paths end in the new components. A job saved by v0.2 names
  `std:v0.1:default`: proposal is to load it under the current context with a warning; its
  tables are old layout, so it shows no rows.
- Docs: `docs/reference/hashing.md`, `docs/concepts/function-pods.md`,
  `docs/concepts/arrays.md`, CHANGELOG "Breaking changes and migration", CLAUDE.md and
  `.zed/rules` (column table, layout names), DESIGN_ISSUES entry. About 18 test files name the
  old columns, layouts or key.

## (h) Order, cuts, conflicts

1. Manifest `v1.json`, registry and schema, default switch, frozen test.
2. Column on all five tables (replacing the two old columns).
3. Layout constants and paths.
4. Read filter; remove #309 label filtering; `stored_data_contexts`.
5. Old-layout detection and report; side-effect error.
6. Docs, CHANGELOG, DESIGN_ISSUES, CLAUDE.md.

Cut if time runs out: the job-level summary line (per-table warnings stay); removal of the
v0 -> v1 machinery (left in place, pointing at dead layouts); the mixed-context error.

Conflicts: ITL-935 edits the manifest JSON (logical types) and golden files; this branch
renames the file, so whichever lands second re-applies its edit to `v1.json`. ITL-942 edits
`function_node.py` and `arrow_utils.py` near the write path. Golden files: this branch
changes 5 entries; they should go into the shared regeneration.

## Answers (coordinator and developer, November 11, 2026)

| Q | Answer |
|---|---|
| Q1 `v0.1.json` | removed; `std:v0.1:default` raises, naming it as the pre-v0.3 data context (to live in `orcapod-sanctuary`) |
| Q2 moved content hashes | accepted as part of H0 to H1 |
| Q3 golden files | regenerated on this branch, diff limited to the listed entries |
| Q4 layout names | `odb_v1`, `sdb_v1`; also appended to an explicit `cache_path` |
| Q5 v0 to v1 machinery | removed (`SchemaVersionError`, `ignore_schema`, `orcapod.migrations`, `orcapod migrate`); a bare-path table is an old layout |
| Q6 mixed contexts | raise `InputValidationError` naming both keys before anything is written; eventual behaviour is ITL-951. The in-memory column name was not answered: unchanged, open |
| Q7 wording, error name | approved; `SideEffectLogDataContextError` |
| Q8 label filter, v0.2 job files | label filtering removed, `stored_data_contexts` added; a v0.2 job file loads under the current context with a warning |

Later instructions from the same session:

- Result table gains `__pf_var_function_version` (full version string).
- No hash label text hard-coded in new tests or docs; labels are renamed in PR #309's branch.
- Manifest fields `released_in`, `previous`, `definitions`, `libraries`, `golden`, `changes`
  (design document 7.4); logical types and their value handlers move to the unfrozen
  `contexts/logical_types.json`.
- The explicit failure covers both kinds of side-effecting node: `SideEffectPod` nodes and
  function nodes whose pod takes the invocation context (`ctx_arg`). Decided in one place:
  `contexts/stored.account_for_stale_tables`.

## Outcome

Commits on the branch, in order: plan; manifest `v1.json` and frozen check; two WIP
checkpoints and the feature commit for the column, layouts, read filter and old-layout
handling; manifest fields and the logical-type registration file; side-effecting function
nodes; removal of `v0.1.json` and the v0 to v1 machinery; refusal of inputs of another data
context.

Golden entries changed (`uv run python tests/test_hashing/golden/generate_golden_hashes.py`,
diff checked):

- values: `schema:source_stream_builder.{default_source_id,named_source_id,record_id_column}.content_hash`
  (the context key is hashed into the content hash of a stream without a producer);
  `pipeline:job.table_paths`, `pipeline:polars_filter_expr_job.table_paths` (layout
  component only).
- entry names only, values identical: the `table[<path>].record_ids` and
  `table[<path>].input_data_hashes` entries of those two jobs (4 names), because the name
  embeds the table path.
- also pinned outside the golden suite and updated: the three bare-stream content hashes in
  `tests/test_semantic_types/test_struct_layout_identity.py`.

Differences from the STEP 1 plan:

- `released_manifests.json` is in `contexts/`, not `contexts/data/` (every `*.json` in
  `data/` is loaded as a manifest).
- Old-layout tables are reported once per reader object, not once per process, and there is
  no job-level summary line (cut).
- Operator-cache and cached-source record hashes are unchanged; the column is appended
  after hashing.

Open points and findings are listed in the PR body and in `DESIGN_ISSUES.md` (DC1).
