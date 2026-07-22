# Orcapod Python — Pre-v0.2 Guided Codebase Review

**The canonical living document for the "Orcapod Python: Pre-v0.2 Guided Codebase Review" Linear
project.** Every review issue (one per leg) appends its findings here — this file is never
forked per-issue.

- **Status:** in progress (Leg 0 complete).
- **Companion:** `pre-v0.2-thematic-findings.md` — the initial 5-anchor-area thematic pass
  (ITL-564) that seeded this review: cross-reference matrix, cross-cutting themes X1–X3,
  and the per-area analysis. Findings below cite those (Area 1–5, X1–X3).

## Methodology — guided, agent-led, `#!` / `#!?`

We visit one **leg** (a cluster of used-together modules) at a time, bottom-up so each leg's
vocabulary supports the next. Per file, the cycle is:

1. **Prep** — the agent reads the leg's modules and verifies claims (fanning out Explore
   subagents when the leg is large), then lists the exact files.
2. **Guided pass** — for each file the agent posts a "what it is / what to watch" briefing and
   inserts inline **`#!?`** annotations at attention points. The reviewer reads the file and
   marks confirmed problems with **`#!`**. They discuss.
3. **Enumerate** — the agent appends findings to the table at the bottom of this doc
   (`file:line`, note, proposed tier, reviewer disposition).
4. **Wrap** — files checked off below; a leg's issue closes when all its files are done and its
   findings are logged.

- Conventions: `#!` = reviewer-confirmed problem; `#!?` = agent attention/question. Annotations
  live on the review branch and are removed once findings are converted to tracked work.
- **Enumeration first.** Reviews don't chase fixes — but the reviewer may apply a fix in place
  when it is simple and low-risk (not restricted to docs). Anything larger becomes a follow-up
  filed by the synthesis issue.

## Review order & estimates

Order = leg order (dependency-driven). Estimate = review effort. Priority = importance to
review before the v0.2 cut (findings concentration). 🔴 = P0/P1 concentration; 🟡 = P2 cleanup.

| Order | Leg | Issue | Est | Priority |
|-------|-----|-------|-----|----------|
| 1 | 0 — Foundations & vocabulary 🟡 | ITL-564 (kickoff) | M | High |
| 2 | 1 — Type system & semantic conversion | ITL-565 | L | Medium |
| 3 | 2 — Hashing & identity infra 🟡 | ITL-566 | L | High |
| 4 | 3 — Protocols | ITL-567 | M | Medium |
| 5 | 4 — Datagrams 🔴 | ITL-568 | M | High |
| 6 | 5 — Streams & Sources | ITL-569 | L | Medium |
| 7 | 6 — Invocation & hooks 🔴 | ITL-570 | S | High |
| 8 | 7 — Data functions & executors 🔴 | ITL-571 | L | High |
| 9 | 8 — Nodes (function_node ⚠️) 🔴🔴 | ITL-572 | XL | Urgent |
| 10 | 9 — Operators | ITL-573 | M | Medium |
| 11 | 10 — Databases & migrations 🟡 | ITL-574 | L | Medium |
| 12 | 11 — Pipeline / orchestration / observability 🔴 | ITL-575 | L | High |
| — | Synthesis → file all findings as tracked work | ITL-576 | M | High |

---

## Leg 0 — Foundations & shared vocabulary  🟡
Everything downstream imports these. Establish the vocabulary first.
- [x] `system_constants.py` — column prefixes/separators; **dead constants** `POD_VERSION`, `EXECUTION_ENGINE`, `ENV_INFO`, `POD_ID_PREFIX` (X3) → I-1…I-5
- [x] `errors.py` — exception taxonomy; note the *absence* of execution/OOM error types (Area 3) → I-6…I-9
- [x] `types.py` — Schema, ColumnConfig, ContentHash, config dataclasses; **2 verified bugs** (unhashable Schema, `__eq__` raises) + dead exec config (X3) + missing retry fields (Area 3) → I-10…I-14
- [x] `config.py` — 2nd (global/TOML) config system; dead+broken merge; heavy duplication → I-17…I-19
- [ ] `utils/lazy_module.py`, `utils/name.py`, `utils/git_utils.py`, `utils/function_info.py`, `utils/object_spec.py`

## Leg 1 — Type system & semantic conversion (Python ↔ Arrow)
- [ ] `semantic_types/` — `type_inference.py`, `universal_converter.py`, `precomputed_converters.py`, `pydata_utils.py`
- [ ] `extension_types/` — logical types, `file_type.py`/`directory_type.py` (the `op.File`/`op.Dir` from ITL-557 image-rebuild friction), `numpy_type.py`, `pandas_type.py`, `registry.py`, `schema_walker.py`, `type_utils.py`
- [ ] `contexts/` — `core.py` (DataContext), `registry.py`
- [ ] `utils/schema_utils.py`, `utils/arrow_utils.py`, `utils/polars_data_utils.py`

## Leg 2 — Hashing & identity infrastructure  🟡
- [ ] `protocols/hashing_protocols.py` — `PipelineElementProtocol`, `ContentIdentifiableProtocol`
- [ ] `hashing/semantic_hashing/` — `semantic_hasher.py`, `builtin_handlers.py`, `type_handler_registry.py`, `function_info_extractors.py`, `content_identifiable_mixin.py`
- [ ] `hashing/` — `arrow_hashers.py`, `file_hashers.py`, `directory_hashers.py`, `hash_cachers.py`/`string_cachers.py`/`postgres_hash_cacher.py`, `versioned_hashers.py`, `defaults.py`, `schema_cleaner.py`, `visitors.py` (FileHasher perf — ITL-519/520/522)
- [ ] `core/base.py` — `ContentIdentifiableBase`, `PipelineElementBase`, `TraceableBase` — **the two identity chains** (`content_hash` vs `pipeline_hash`)

## Leg 3 — Protocols (the contract map, read before implementations)
- [ ] `protocols/core_protocols/` — `datagrams.py`, `streams.py`, `sources.py`, `pod.py`, `function_pod.py`, `operator_pod.py`, `side_effect_pod.py`, `data_function.py`, `executor.py`, `trackers.py`, `traceable.py`, `temporal.py`, `labelable.py`
- [ ] `protocols/database_protocols.py`, `db_connector_protocol.py` — **no transactional/referential primitive** (X1, Area 5)
- [ ] `protocols/observability_protocols.py` — **phantom `log_cache_hits`** (Area 1)
- [ ] `protocols/node_protocols.py`, `pipeline_protocols.py`, `semantic_types_protocols.py`

## Leg 4 — Datagrams (core data containers)  🔴
- [ ] `core/datagrams/datagram.py` — lazy dict↔Arrow backing
- [ ] `core/datagrams/tag_data.py` — `Tag`, `Data`, **`EmptyData`** (Area 4: inert `empty_source_info`, accessor guards)

## Leg 5 — Streams & Sources
- [ ] `core/streams/base.py`, `arrow_table_stream.py`
- [ ] `core/sources/base.py`, `arrow_table_source.py`
- [ ] `core/sources/derived_source.py` — **strips version/executor metadata on re-expose** (Area 2)
- [ ] delegating sources: `csv/dict/list/data_frame/delta_table/db_table/sqlite_table/postgresql_table/spiraldb_table_source.py`, `cached_source.py`, `source_proxy.py`, `stream_builder.py`
- [ ] `core/sources/polling_source.py` — the *only* existing backoff/retry (contrast w/ Area 3), `source_registry.py`
- [ ] `core/nodes/source_node.py`

## Leg 6 — Invocation identity & hooks (the identity channel)  🔴
- [ ] `invocation.py` — `InvocationContext`, `invocation_hash` format (Area 1/2)
- [ ] `hooks.py` — `PostRunPayload`, `RunStats`, **`InvocationStatus` single ERROR bucket** (Area 3), disjoint from structured logs (Area 1)

## Leg 7 — Data functions & executors (compute layer)  🔴
- [ ] `core/data_function.py` — `uri`/identity, `major_version` coarse control (by design), **missing fine-grained match** (Area 2, finding #5), `get_execution_data` schema drift
- [ ] `core/data_function_proxy.py`
- [ ] `core/executors/base.py`, `local.py`, `ray.py`, `capture_wrapper.py` — `with_options` never called (X3), no OOM/retry (Area 3)
- [ ] `execution_engines/ray_execution_engine.py` — **orphaned `RayEngine`** (X2)
- [ ] `core/function_pod.py` — `ctx_arg`/`InvocationContext`, post-run hooks, sync/async paths (Areas 1/2/3)
- [ ] `core/cached_function_pod.py` — write path
- [ ] `core/result_cache.py` — **`lookup` matches INPUT hash only** (Area 2 match granularity), `store` metadata columns, `RESULT_COMPUTED_FLAG`

## Leg 8 — Nodes (DB-backed execution)  🔴🔴 THE hotspot
- [ ] `core/operators/static_output_pod.py` — `StaticOutputPod` base, `DynamicPodStream`
- [ ] `core/nodes/function_node.py` — **the big one**: `_fetch_joined_records` (Area 5 silent drop = P0, Area 4 EmptyData path), `add_pipeline_record` (write ordering X1, dead `INPUT_DATA_HASH_COL`), `execute`/`_process_data_internal` + async twin (Area 3 error_policy divergence)
- [ ] `core/nodes/operator_node.py` — single inline table (Area 5 N/A), **no-op `set_ephemeral_store`** (Area 4, ITL-509)
- [ ] `core/tracker.py` — graph-construction tracking (not runtime logging)

## Leg 9 — Operators
- [ ] `core/operators/base.py` — `UnaryOperator`/`BinaryOperator`/`NonZeroInputOperator`, `argument_symmetry`
- [ ] `join.py`, `merge_join.py`, `semijoin.py`, `batch.py`, `column_selection.py`, `mappers.py`, `filters.py`, `index.py`, `pick.py`

## Leg 10 — Databases & migrations  🟡
- [ ] `databases/` — `in_memory_databases.py`, `delta_lake_databases.py`, `noop_database.py`, `connector_arrow_database.py`, `extension_aware_database.py`, `postgresql_connector.py`, `sqlite_connector.py`, `spiraldb_connector.py`, `storage_utils.py`, `file_utils.py`, `utils.py` — **no cross-store transaction** (X1)
- [ ] `migrations/` — `pipeline_db.py`, `result_db.py`, `types.py` (ITL-535 v0→v1)
- [ ] `cli/migrate.py`, `cli/warm_cache.py`

## Leg 11 — Pipeline, orchestration & observability  🔴
- [ ] `pipeline/graph.py`, `dag.py`, `networkx_backend.py` — graph model
- [ ] `pipeline/base.py` — `Pipeline`, `set_ephemeral_store` fan-out
- [ ] `pipeline/job.py` — **`run_id` generation** (Area 1 format inconsistency)
- [ ] `pipeline/sync_orchestrator.py`, `async_orchestrator.py` — **`error_policy` sync/async divergence** (Area 3, P1)
- [ ] `pipeline/execution_context.py`, `pod_invocation.py`, `result.py`, `serialization.py`
- [ ] `pipeline/observer.py`, `composite_observer.py`, `logging_observer.py`, `status_observer.py`, `logging_capture.py`, `observability_reader.py` — **structured logs lack record_id/invocation_hash** (Area 1, P1)
- [ ] `side_effects.py` — invocation-log write via ctx pods
- [ ] `channels.py`, `extensions.py`

---

## Enumerated issues (grows as we walk)

| ID | File:line | Note | Reviewer tier | Linked finding |
|----|-----------|------|---------------|----------------|
| I-1 | system_constants.py:5,16,17,21 | Dead constants + properties: `POD_ID_PREFIX`, `POD_VERSION`, `EXECUTION_ENGINE`, `ENV_INFO` (0 external uses) — intended version/executor/env system columns never wired. Reviewer: POD_VERSION/EXECUTION_ENGINE/ENV_INFO likely obsolete (superseded by DataFunction variation datagram) → verify & delete | _pending (lean delete)_ | X3, Area 2 |
| I-2 | system_constants.py:29 | `global_prefix` never set non-empty; entire prefixing machinery is inert. Reviewer: keep singleton design deliberately for future modeling | _keep (deliberate)_ | X3 |
| I-3 | system_constants.py:13,14 | `PF_` = legacy `PacketFunction` naming (confirmed CHANGELOG.md:102-109) baked into on-disk columns `pf_var_`/`pf_exec_`; renaming = schema migration, not cosmetic | _pending_ | Area 2 |
| I-4 | system_constants.py:31 | `POD_TIMESTAMP` live (result_cache.py:208/282, "latest result wins" ordering) but is really a result-cache column, not a pod column → `POD_` grouping inconsistent | _pending_ | — |
| I-5 | system_constants.py (whole) | Missing per-constant docstrings/section headers; schema-version trio (pdb/rdb/tdb_v1) should be its own subsection (they're legitimately the only bare-imported constants, correctly unprefixed) | _pending_ | doc-debt |
| I-6 | errors.py (whole) | No common `OrcapodError` base — 14 errors across Exception/ValueError/RuntimeError/LookupError; can't `except OrcapodError`. **Reviewer: CONFIRMED — add `OrcapodError(Exception)` root, all inherit from it** | _accepted_ | api-design |
| I-7 | errors.py + contexts/core.py + hashing/visitors.py | Error classes scattered outside errors.py. **Reviewer: CONFIRMED — all error/exception classes must be collected in this module** | _accepted_ | api-design |
| I-8 | errors.py (missing) | **No execution-failure taxonomy** (no PodExecution/OOM/Transient/Retryable) — groundwork gap for retry. **Reviewer: CONFIRMED — we want to add it** | _accepted (feeds ITL-557)_ | Area 3 |
| I-9 | errors.py:164, 76 | `EmptyDataHashMissingError` msg cited dead `INPUT_DATA_HASH_COL` → **reviewer FIXED in place**; `SourceSpecMismatchError` "preserved for compatibility" note still open (greenfield) | _partially applied_ | Area 4, cleanup |
| **I-10** | types.py:93 (Schema) | **BUG (verified): Schema is unhashable** — `__eq__` override w/o `__hash__`. **Reviewer: implement real `__hash__` eventually; until then keep "semantic-hasher-hashable" qualifier + make hashable-context use raise an *informative* error (not bare TypeError)** | _accepted (correctness)_ | new bug |
| **I-11** | types.py:163 (Schema.__eq__) | **BUG (verified): raises `NotImplementedError` instead of returning `NotImplemented`**. **Reviewer confirmed → return `NotImplemented` singleton** | _accepted → return NotImplemented_ | new bug |
| I-15 | types.py (whole) | Module too big; docstring stale. **Reviewer: split into a subpackage** — `Schema` own module, Arrow-related type aliases own module | _accepted (refactor follow-up)_ | tech-debt |
| I-16 | types.py:40-88 (aliases) | Type-alias hygiene: usage-analyze & prune unused aliases; simplify `DataValue` to leverage extension system (arbitrary types); evaluate `UPath` for `PathLike` (spike); convert `DataType` older-Union TODO to an issue | _accepted (follow-ups)_ | tech-debt |
| I-17 | config.py + types.py | Two config systems (`OrcapodConfig` global/lib vs `PipelineConfig`/`PodConfig`/`NodeConfig` object-config) with divergent merge. **Reviewer: both categories are legitimate — global-lib-config vs object-config — but interfaces should be as similar as possible; make the distinction explicit.** → see I-20 spike | _accepted → I-20_ | X3-adjacent |
| I-18 | config.py:37 (_SectionConfig.merge), 137 | `OrcapodConfig.merge`/`_SectionConfig.merge` **dead** (no external callers) AND can't express "reset to default". **Reviewer: consolidate config-merge logic across modules; clean up + fully document the inheritance hierarchy.** | _accepted → I-20_ | dead-code |
| I-19 | config.py:162 (from_dict), 236 (load_config) | ~150 lines of duplicated per-section parse/warn/filter; `load_config` reimplements `from_dict`; Mapping-vs-dict inconsistency; hardcoded section set ×3. **Reviewer: confirmed — targeted refactor issue(s); also add optional env-var config override (low priority).** | _accepted_ | DRY |
| **I-20** | config.py + types.py (design) | **Config-system unification design spike (reviewer-requested).** Evaluate: (a) two categories = global-lib-config vs object-config, with maximally-similar interfaces; (b) single consolidated merge; (c) **co-locating object-specific config WITH the object it configures**; (d) optional env-var overrides. Needs a spike before refactor. | _accepted (design spike)_ | design |
| I-12 | types.py:330-331 | Dead `PipelineConfig.execution_engine`/`execution_engine_opts` w/ misleading docstring (`with_options` never called) | _pending_ | X3, Area 2 |
| I-13 | types.py:335-366 | No retry/memory config fields on any config (Area 3); PodConfig-vs-NodeConfig axis undocumented; only NodeConfig has merge() | _pending_ | Area 3 |
| I-14 | types.py:474 (ColumnConfig) | Docstring Attributes omits real fields `content_hash`, `sort_by_tags` | _pending_ | doc-drift |
