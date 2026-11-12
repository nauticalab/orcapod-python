## `src/orcapod/contexts/` — data context of stored rows

### DC1 — a stored row did not say which generation of hashing wrote it
**Status:** resolved, with open follow-ups listed below
**Severity:** high
**Issue:** ITL-944

The data context key (`std:v0.1:default`) never changed although its manifest
`contexts/data/v0.1.json` was edited in place 44 times, so the key stored in result rows
(`_context_key`) and pipeline rows (`__input_data_context_key`) told a reader nothing.
Operator-cache, cached-source and side-effect tables stored no key at all. PR #309 told the
rows of v0.2 from the rows of v0.3 by the label inside each hash, which names an algorithm,
not a generation, and is missing from operator-cache and cached-source record ids.

**Fix:** (developer's decisions of November 11, 2026, ITL-938 design document Parts 5.5 to 8)
- The default data context is `std:v1:default`, manifest `contexts/data/v1.json`.
  `contexts/released_manifests.json` pins the sha256 of every released manifest (`null` while
  a manifest is open); `tests/test_contexts/test_data_context_v1.py` fails when a pinned file
  changes, when more than the default manifest is open, or when a final release ships an open
  manifest. `v1.json` is pinned once, in the v0.3.0 release-preparation commit.
- Every stored row carries `__data_context` (`large_string`): function pipeline and result
  tables, operator-cache tables, cached-source tables, side-effect invocation logs. It
  replaces `__input_data_context_key` and the result table's `_context_key`. The value is the
  key of the node, cache or source that writes the row.
- Layouts moved: `pdb_v2`, `rdb_v2`, `tdb_v2`, and new components `odb_v1` (operator cache)
  and `sdb_v1` (cached source, also after an explicit `cache_path`).
- Reads and lookups keep rows of the current data context only, through
  `contexts/stored.keep_rows_of_data_context` or a lookup constraint on the column. A table
  at an old layout is counted and reported once per reader object, never read as current,
  never written.
- What a node does about stored rows of other generations is decided in one place,
  `contexts/stored.account_for_stale_tables`: report and skip, or, for a node that delivers
  side effects, raise `SideEffectLogDataContextError` before anything is delivered. Both
  kinds of side-effecting node are covered (developer, November 11, 2026): `SideEffectPod`
  nodes (invocation log) and function nodes whose pod takes the invocation context
  (`ctx_arg`; pipeline and result tables).
- `orcapod.contexts.stored_data_contexts(database, record_path)` lists the data contexts a
  table holds. The label filtering of PR #309 (`hashing/stored_hash_labels.py`) is removed.
- The result table also stores `__pf_var_function_version`, the full function version.

Known limits, follow-ups:
- Row keys do not include the data context. A later data context under which a row's key is
  unchanged collides with the old row on write (`skip_duplicates`), and the new row is not
  stored. The design's answer is that migration relabels unaffected rows; until a migration
  exists this must be revisited before `std:v2:default` is introduced.
- The content hash of a stream without a producer covers its `_context_key` column, so it
  moves with the data context key.
- Old-layout tables are reported once per reader object, not once per process or job, and
  counting their rows reads the whole table (the store protocol has no row count).
- The manifest has the fields of the ITL-938 design document, section 7.4 (`released_in`,
  `previous`, `definitions`, `libraries`, `golden`, `changes`), and the logical types and
  their value handlers moved to the unfrozen `contexts/logical_types.json`. The names under
  `definitions` are labels only: nothing in the code reads them or checks that a definition
  changed, and `changes` lists what is true on this branch (starfix format 2); the entries
  for ITL-935 and ITL-942 are added when those land. `golden.digest` stays `null` until the
  manifest is frozen, because the golden files still move.
- `v0.1.json` is removed (to live in `orcapod-sanctuary`); `std:v0.1:default` raises
  `ContextResolutionError`, and a node saved under it loads under the current data context
  with a warning (`contexts.resolve_saved_context`).
- The v0 to v1 machinery is removed: `SchemaVersionError`, `NodeConfig(ignore_schema=)`,
  `orcapod.migrations`, `orcapod migrate`. A table without a layout component is an old
  layout like `pdb_v1`.
- An input of another data context raises `InputValidationError`
  (`contexts/stored.require_same_data_context`) before anything is written. The developer
  wants the node's data context to win eventually: ITL-951.
- Found while testing that rule, not fixed here: a data context is not carried reliably in
  memory. `ArrowTableStream` gives every row the stream object's data context and ignores
  the `_context_key` column of its table (the two can disagree); a source's `data_context`
  is not given to the rows of its stream; and `PipelineJob` wraps each input in a source
  node of the default data context. So inside a job an input of another data context is
  not seen as such, and the check only fires for nodes and caches driven directly.
- For the same reason there is no per-row data context check on the cache-hit path of a
  function node (CodeRabbit, PR #313): every row of a stream carries the stream object's
  data context, so the whole-input check covers hits and misses. To revisit with ITL-951.
- Open: whether the in-memory column `_context_key` (`ColumnConfig(context=True)`) is
  renamed to `__data_context` as well. It is unchanged here.
