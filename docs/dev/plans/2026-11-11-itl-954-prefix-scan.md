# ITL-954: find pre-v0.3 tables by scanning under a node's stable path prefix

> **For agentic workers:** executed inline in one session (sensei:executing-plans); steps use
> checkbox syntax. A successor continues from the first unchecked box and the notes at the end.

**Goal:** Tables written before v0.3 are found although every hash inside their path moved, so
side-effecting nodes fail before delivering and old tables are reported.

**Architecture:** One new protocol method, `ArrowDatabaseProtocol.list_table_paths(prefix)`,
lists the tables under a path prefix. `contexts/stored.py` tells old from current by the last
path component (the layout), and the existing single decision point
`account_for_stale_tables` receives the scanned tables in addition to the sibling-path ones.

**Tech stack:** pyarrow, deltalake, s3fs/fsspec (listing on cloud), pytest.

- Linear: ITL-954. Branch `eywalker/itl-954-find-tables-written-before-v03-by-scanning`,
  stacked on PR #313 (`8b050710`), which is stacked on #309.
- Starting point: #313's plan (`2026-11-11-itl-944-data-context-v1.md`), DESIGN_ISSUES DC1,
  #313's "Known gap".

## STEP 1 plan as sent to the coordinator

```
ITL-954 STEP 1 plan (no library code written yet). Reply "approved" or corrections per number.

1. METHOD: ArrowDatabaseProtocol.list_table_paths(prefix: tuple[str, ...] = ()) -> list[tuple[str, ...]] (name chosen to pair with table_exists). Returns, sorted, the path (relative to base_path, like table_exists) of every table whose path starts with prefix, including tables nested below another table. Only lists, never opens a table. A store that cannot enumerate raises a new TableListingNotSupportedError (NotImplementedError subclass, not a StoreError); a listing that is supported but fails raises StoreReadError (fails the run, like any failed read).
   - InMemoryArrowDatabase: exact (committed and pending keys).
   - DeltaTableDatabase, local: exact. Directory walk under the prefix; a directory holding _delta_log is a table; the walk continues below tables (v0.1 bare tables have pdb_v1 under them; odb_v1/sdb_v1 sit under a v0.2 table) and never enters _delta_log. Committed tables only, as table_exists.
   - DeltaTableDatabase, cloud: s3:// and s3a:// through s3fs (already a core dependency), credentials translated back from the deltalake storage options (key, secret, token, endpoint, region); same walk, one LIST per directory. gs://, az://, abfs://: through fsspec only if gcsfs/adlfs happens to be installed, otherwise TableListingNotSupportedError. S3 gets a test in the existing MinIO suite (needs Docker; skipped where absent, I will say whether it ran).
   - ConnectorArrowDatabase (SQLite, PostgreSQL, SpiralDB): table names from the connector's get_table_names() that start with the sanitized prefix + "__"; returned as prefix + the rest split on "__". Limits, documented: components after the prefix come back sanitized (":" and "." are "_"), a component containing "__" comes back split, a different path with the same sanitized name is matched too. The LAST component is exact for layout names, which is all the scan needs. PostgreSQL: identifiers over 63 bytes are truncated by the server, and a function table name with two 64-hex hashes is far longer; I cannot verify here (no server) whether such tables work at all, so I will state PostgreSQL as "lists what the catalog holds; a truncated name loses its layout suffix and counts as pre-v0.3" and report it as an out-of-scope finding.
   - ExtensionAwareDatabase: delegates. NoOpArrowDatabase: [].
   The existing list_sources() (in-memory and Delta only, not on the protocol, stops at the first Delta table) is left as is.

2. OLD VERSUS CURRENT: in contexts/stored.py, CURRENT_LAYOUTS = {pdb_v2, rdb_v2, tdb_v2, odb_v1, sdb_v1}. Under a node's name prefix, a table whose last component is in that set is current (of some node); every other table was written before v0.3.

3. WHERE AND HOW OFTEN:
   - Per node, in the existing once-per-node-object-and-attached-database check (before the node's first execution): function node scans <pipeline db>/<name>/ and <result db>/<name>/ (ctx_arg: side_effect_function/<name>/ in both); SideEffectPod node scans side_effects/<pod name>/. Prefix = the pod's uri up to and including the name, relative to the database the node was given, so a separate result database works too. Sibling-path check kept; results merged by path.
   - Side-effecting node (both kinds): any pre-v0.3 table under its prefix, or rows of another data context in its current table -> SideEffectLogDataContextError before any delivery, naming every table.
   - Ordinary function node: ONE warning per node (see 5).
   - PipelineJob.run(), once per call before the orchestrator starts: scan <job>/ and report one total of pre-v0.3 tables that no function or side-effect node of this job claims by name (operator caches, cached sources stored under the job, functions no longer in the job). Under the job root "pre-v0.3" needs a positive mark, because observers (_log, _status) and users store other tables there: last component is pdb_v1/rdb_v1/tdb_v1, or starts with schema: / instance: / node: (bare v0.1/v0.2 identity paths). Tables only, no row count (a row count means reading every table).
   - CachedSource outside a job keeps its sibling check and also scans <cache_path_prefix>/source/ for tables not at sdb_v1, one warning per source object. An explicit cache_path has no hash in it, so the sibling check is already exact there.

4. COST: local Delta: one scandir per directory under the prefix; a node's prefix holds about 5 directories per table generation; the job-level scan visits every directory under <job>/ once per run (thousands of tables: well under a second locally). S3: one LIST request per directory (plus one per 1000 files in a table directory): per-node scans are a handful of requests; the job-level scan is about 6 requests per table under the job, sequential, so roughly a minute per 1000 tables. Question 4a: is that acceptable, or should the job-level total be skipped on cloud stores (per-node scans stay)? My recommendation: keep it everywhere; it is one pass per run. Connector stores: one catalog query per scan. Row counts: sibling tables keep today's count (whole-table read, unchanged); tables found only by the scan are reported without rows.

5. WORDING:
   - function node: "Found N tables written by an earlier version of Orcapod under '<job>/<name>' (older table layout, data context std:v0.1:default): '<path>', '<path>' (and K more). They are not used and are left unchanged; they can be migrated later. Tables of another function or node with the same name are included."
   - job: "Store '<job>' holds N more tables written by an earlier version of Orcapod (operator caches, cached sources, or nodes not in this job). They are not used and are left unchanged; they can be migrated later."
   - SideEffectLogDataContextError: existing text, all tables named, plus one sentence: "Every table under '<prefix>' counts, so a table of another function or pod with the same name in this job, or of another node of the same function, raises this error too."
   - unlistable store, side-effecting node (warn-and-proceed default): "Store <type> cannot list its tables, so node '<label>' could not check for completions logged before Orcapod v0.3 under '<prefix>'. If the store holds such tables, every side effect is delivered again." Logged once per node object.

6. THE SWITCH (undecided point 5 of the brief): one module constant in contexts/stored.py, UNLISTABLE_STORE_POLICY: "warn" (default) or "fail"; account_for_stale_tables reads it in one place; "fail" raises SideEffectLogDataContextError-style StoreError naming the prefix. No per-node opt-out is built until the developer decides. Unlistable after this change: only Delta on gs/az without the fsspec package, and third-party databases that do not implement the method (checked with hasattr, treated as unlistable).

7. FIXTURE (already generated, 31 KB, real v0.2.0 installed from the tag with its own pins, starfix 0.4, via uv run --no-project --with 'orcapod @ git+file://<repo>@v0.2.0'): tests/test_arrays/data/v02_full_store.tar.gz, generator make_v02_full_store.py next to the existing one, pods in tests/test_arrays/v02_full_store_pods.py. Job v02full: plus_one and plus_two (function pods), notify (ctx_arg), SelectDataColumns with cache_mode LOG, sink_pod deliver, and a CachedSource stored under the job. Nine tables: 3x pdb_v1, 3x rdb_v1, tdb_v1, bare operator cache, bare source/node:... The existing fixture is untouched.

8. TESTS (first, shown failing): (a) list_table_paths per backend (in-memory, local Delta incl. nested tables, SQLite connector, wrapper, no-op, MinIO); (b) real v0.2 bytes: the same job under this version on the extracted fixture: sink and notify raise naming their tables, nothing delivered, bytes unchanged; plus_one/plus_two reported; job total reports the operator cache and cached source; (c) the combined stack simulated: the fixture's hash components renamed on disk (every semantic hash and schema hash replaced, Delta directories moved) so no path equals one this version computes, then the same assertions; sync and async; (d) false positives documented by test; (e) unlistable store: warn once and proceed, and the "fail" setting. PR body gets the re-verification command for the combined branch.

9. DOCS: protocol docstring, docs reference page for databases, CHANGELOG 0.3.0, CLAUDE.md + .zed/rules (stored tables section), DESIGN_ISSUES entry DC2 appended.
```

## Answers (coordinator and developer, November 11, 2026)

- 1 to 3, 5, 7 to 9: approved as written. The developer confirmed the protocol method and
  that side-effecting nodes of both kinds raise on pre-v0.3 tables.
- 4a: the job-level scan runs on every store; the measured cost goes into the PR body.
- 6: when a store cannot list its tables: warn once per node object and proceed. No
  `UNLISTABLE_STORE_POLICY`, no "fail" setting, no test for it.
- PostgreSQL identifier truncation: out-of-scope finding, reported in worker_done.

## Files

| File | Change |
|---|---|
| `src/orcapod/protocols/database_protocols.py` | `list_table_paths` on `ArrowDatabaseProtocol`, documented per backend |
| `src/orcapod/errors.py` | `TableListingNotSupportedError(NotImplementedError)` |
| `src/orcapod/databases/in_memory_databases.py`, `delta_lake_databases.py`, `connector_arrow_database.py`, `extension_aware_database.py`, `noop_database.py` | implementations |
| `src/orcapod/contexts/stored.py` | `CURRENT_LAYOUTS`, `find_pre_v03_tables`, scan results in `StaleTable` (`rows: int | None`), report wording, unlistable warning |
| `src/orcapod/core/nodes/function_node.py`, `core/result_cache.py`, `side_effects.py`, `core/sources/cached_source.py` | pass the name prefix; merge scan and sibling results |
| `src/orcapod/pipeline/job.py` | one store-level total per `run()` |
| `tests/test_databases/test_list_table_paths.py` | (a) the method per backend |
| `tests/test_pipeline/test_pre_v03_tables.py` | (b) to (e) on the real v0.2 fixture |
| `tests/test_arrays/v02_full_store_pods.py`, `tests/test_arrays/data/make_v02_full_store.py`, `data/v02_full_store.tar.gz` | fixture written by real v0.2.0 |
| docs, CHANGELOG, CLAUDE.md, `.zed/rules`, DESIGN_ISSUES (DC2) | documentation |

## Tasks

- [x] 0. Fixture: pods module, generator, archive generated with v0.2.0 from the tag.
- [x] 1. Tests (a) for `list_table_paths`; run, see them fail (no such method).
- [x] 2. Implement the method on the five database classes and the protocol; tests pass.
- [x] 3. Tests (b) to (e) on the fixture; run on this branch, record which fail and why
      (on this branch the hashes did not move, so the sibling check already finds the
      fixture's tables; the renamed-hash copy must fail before the fix).
- [x] 4. `contexts/stored.py`: layout rule, scan, reporting; wire the nodes, the result
      cache, the cached source and `PipelineJob.run()`. Tests pass.
- [x] 5. Docs, CHANGELOG, CLAUDE.md and `.zed/rules`, DESIGN_ISSUES DC2.
- [x] 6. ruff, basedpyright (exit code), full suite on one Python version at a time,
      golden hashes unchanged.
- [ ] 7. Merge `origin/main` if it moved, push, PR against `main`, CodeRabbit rounds.

## Notes for a successor

- Tasks 0 to 5 are done and pushed. The fixture is regenerated with
  `tests/test_arrays/data/make_v02_full_store.py` (command in its docstring).
- Differences from the STEP 1 plan:
  - The unlistable warning names the path under which tables would have been looked for,
    not the node label (the reporting function gets no label; the path holds the pod name).
    One warning per node covers its pipeline and result prefixes.
  - A listing that fails on cloud storage counts as "cannot list" (warn and proceed), not
    as `StoreReadError`: the `fsspec` credentials can differ from deltalake's, and the
    developer decided that an unlistable store only warns. A failed local directory listing
    raises `StoreReadError`.
  - On an unlistable store the job-level total logs at INFO, not WARNING.
- Measured cost (local Delta-shaped store, 4000 tables, 18000 directories, suite running in
  parallel): one node's scan 0.25 ms; the job-level listing 0.3 s per run. S3: one LIST
  request per directory, about 4.5 per table under the listed prefix.
- Suite at the last commit, Python 3.12: `uv run pytest tests/` 7866 passed, 97 skipped,
  18 xfailed; `test-objective/` passes; the new tests also pass on 3.11 and 3.13; ruff and
  basedpyright (exit 0) clean; golden hashes unchanged.
- No Docker on the worker machine: the MinIO test did not run locally.
