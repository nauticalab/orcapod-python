# ITL-564 — Guided codebase review for v0.2 targeted revision/development

**Status:** Draft (findings complete, priorities proposed — pending review)
**Author:** Edgar Y. Walker (with Claude Code)
**Date:** 2026-07-22
**Linear:** [ITL-564](https://linear.app/metamorphic/issue/ITL-564)
**Branch:** `eywalker/itl-564-spike-guided-codebase-review-to-identify-targeted`

---

## How to read this document

This is a **directed** review anchored on five suspected problem areas (plus cross-cutting
themes and candidate net-new areas). It is not an open-ended audit. Each anchor area is written
to the ITL-564 design-axes template: **Current behavior** (cited by file/function) → **Intended
behavior** (for v0.2 coherence) → **Delta** → **Ripple effects** → **Risk of not fixing** →
**Priority + rationale**, with rough **sizing** (S/M/L) and **suggested follow-ups**.

Priorities are **proposed**, not final — they assume the v0.2 bar is "ships correct and
coherent; observability and ergonomics complete enough to run real workloads." Confirm/adjust
the P0/P1 cutoff against the actual feature-freeze bar.

Tiers: **P0** = blocker (ships broken/unsafe) · **P1** = must-have (coherence gap users hit) ·
**P2** = nice-to-have · **P3** = defer.

---

## Cross-reference matrix

Four of the five referenced issues are **already merged** — this review is largely an audit of
landed code plus its unresolved design axes, not speculation about unbuilt features.

| Issue | Topic | Status | How this review touches it |
|---|---|---|---|
| [ITL-534](https://linear.app/metamorphic/issue/ITL-534) | `EmptyData` + ephemeral propagation via cached input hash | ✅ Merged (PR #229) | Areas 4 & 5 audit what landed vs. its many deferred design axes |
| [ITL-535](https://linear.app/metamorphic/issue/ITL-535) | pdb/rdb v0→v1 schema versioning + migration | ✅ Merged (PR #232) | Area 4 (degraded old-format rows), Area 5 |
| [ITL-544](https://linear.app/metamorphic/issue/ITL-544) | `ctx_arg` side-effect path + ray empty-opts | ✅ Merged (PR #235) | Areas 2 & 3 (ctx_arg identity, Ray baseline) |
| [ITL-523](https://linear.app/metamorphic/issue/ITL-523) | Post-run hook + `PostRunPayload` | ✅ Merged (PR #226) | Area 1 (this is the observability primitive) |
| [ITL-557](https://linear.app/metamorphic/issue/ITL-557) | Auto-retry w/ memory scaling | 🕓 Backlog | Area 3 supplies its baseline + delta |
| ITL-509 | Operator ephemeral support | (referenced) | Area 4 (operator `set_ephemeral_store` no-op) |
| DESIGN_ISSUES P3/P6/P7 | version threading, match_tier, redundant hashing | logged | Area 2 |

---

## Cross-cutting themes

Three problems recur across multiple anchor areas. They are called out here once and referenced
from each area rather than repeated.

### X1. Non-atomic two-store write (result-then-tag) — no cross-store transaction
The result table is written **first** (`ResultCache.store` → `result_database.add_record`,
`result_cache.py:286`), the pipeline/tag table **second** (`add_pipeline_record`,
`function_node.py:1385/1404`), the in-memory cache **last** (`function_node.py:1418`). The two
DBs can be different backends; there is no enclosing transaction. A crash between the writes
leaves an **orphaned result row** (result exists, no tag row) that is permanent, invisible, and
never garbage-collected. This is the root cause behind findings in Areas 3, 4, and 5.
**Proposed: P1, size M.**

### X2. Two parallel Ray implementations
`core/executors/ray.py::RayExecutor` (id `"ray.v0"`, actually wired into the executor protocol)
vs. `execution_engines/ray_execution_engine.py::RayEngine` (exported but referenced by no
pod/node/data-function path). Divergent config surfaces, a maintenance trap, and an ambiguity
that ITL-557 must resolve before building retry/memory-scaling. **Proposed: P2, size S** (decide
which survives; likely delete `RayEngine`) — but it is a **prerequisite** for ITL-557.

### X3. Dead / misleading configuration surface
Multiple config knobs and constants are defined, documented, and never consumed:
`system_constants.py` `POD_VERSION` (`__pod_version`), `EXECUTION_ENGINE` (`__execution_engine`),
`ENV_INFO`, `POD_ID_PREFIX` (none referenced outside their definition);
`PipelineConfig.execution_engine` / `execution_engine_opts` (`types.py:330-331`, read nowhere);
`executor.with_options(**opts)` (never called from any path); the documented `log_cache_hits`
observability flag (referenced only in a docstring, `observability_protocols.py:40`, no
parameter exists). Either wire or remove; today they mislead readers into thinking capabilities
exist. **Proposed: P2, size S** (sweep).

---

## Area 1 — Data logging structure during pipeline execution

**Current behavior.** There are **two largely disconnected logging systems**, plus a third
identity channel:
- **System A — stdlib `logging`.** ~30 modules `getLogger(__name__)`; in the hot path e.g.
  `function_pod.py:334/558/599`, `data_function.py:988/1015` (cache checks at `INFO`),
  `cached_function_pod.py:133/182`, `function_node.py:875/1280/1942/1958/2011`,
  `ray_execution_engine.py:41-166`. F-string messages, no run_id/node/record_id fields, land
  wherever the host configures handlers (default: nowhere).
- **System B — structured observability.** `ExecutionObserverProtocol` +
  `DataExecutionLoggerProtocol` (`protocols/observability_protocols.py:36/51`). `DataLogger.record`
  (`pipeline/logging_observer.py:93`) writes one Arrow row per data execution to an
  `execution_logs` table: `_log_id`, `_log_run_id`, `_log_timestamp`, `_log_<field>` (stdout,
  stderr, python_logs, traceback, success), plus one column per tag key. Orchestrators own
  run-level hooks + `run_id` (`sync_orchestrator.py:76-129`, `async_orchestrator.py:136-250`);
  nodes own node/data hooks + logger creation (`function_node.py:1264-1291`); executors are what
  actually call `logger.record(...)` (`executors/local.py:51/123`, `executors/ray.py:282/306`).
  `sys.stdout/stderr` + root-logger capture bridges A→B *only when* capture is installed and an
  observer is active (`pipeline/logging_capture.py:150/181`).
- **Identity channel (disjoint).** `PostRunPayload` (`hooks.py:74`) carries
  `record_id_hash = str(output.datagram_uuid)` and `InvocationContext.invocation_hash =
  "{pipeline_hash}::{record_id_hash}[::{run_id}]"` (`invocation.py:106-127`). This is the only
  place record_id / invocation_hash exist at runtime — and it never reaches `DataLogger`.
- **Correlation IDs.** `run_id` is primary; generated as `uuid4().hex[:16]` in `job.py:823` but
  falls back to full dashed `uuid4()` in the orchestrators (`sync_orchestrator.py:76`) — **two
  formats for the same field.** `pipeline_uri` is stored by `StatusObserver` but not
  `LoggingObserver`.

**Intended behavior (v0.2).** A structured execution-log row should be **joinable to the exact
output record it describes** (via `record_id` / `invocation_hash`), correlated by a single
canonical `run_id` format, and should cover all node types at a defined granularity. The two
logging systems should have a clear relationship (diagnostic vs. structured), and structured-log
write failures should not be silently swallowed.

**Delta.** (1) `DataLogger` rows carry no `record_id`/`invocation_hash`/`node` columns — logs
can only be heuristically joined to outputs by (tag values + run_id + DB path). (2) The identity
channel (`PostRunPayload`) and the I/O-capture channel (`pkt_logger.record`) never meet. (3)
`run_id` has two formats. (4) Operators emit no data-level logs (`operator_node.py` has no
`create_data_logger`/`on_data_start`). (5) Cache hits skip logging entirely
(`function_node.py:1270`); the documented `log_cache_hits` knob doesn't exist (X3). (6)
`DataLogger.record` / `StatusObserver._write_event` swallow DB-write errors via
`logger.exception` (`logging_observer.py:131`, `status_observer.py:299`).

**Ripple effects.** Adding identity columns touches the observer protocol signature
(`create_data_logger`/`record`), both observer implementations, both executors' `record` calls,
the node/pod call sites, and the `PostRunPayload` build site (natural join point,
`function_pod.py:341-389`). run_id normalization touches `job.py` + both orchestrators.

**Risk of not fixing.** Execution logs that can't be joined to their outputs undercut the whole
provenance story the post-run hook / EDI work (PLT-1950) is meant to enable — you can log that
*something* ran but not prove *which record* it produced. Two run_id formats will silently break
joins across the two structured stores.

**Priority.** **P1** for the record_id/invocation_hash join gap + run_id normalization (size
**M**); **P2** for operator-log granularity, swallowed-write-errors, and the two-system bridge
clarity (size **M**); **P3** for the phantom `log_cache_hits` doc fix (size **S**).

**Follow-ups:** *"Thread record_id/invocation_hash into structured execution-log rows"* (P1);
*"Normalize run_id format across job/orchestrators"* (P1, small); *"Data-level logging for
operator nodes"* (P2).

---

## Area 2 — Data function + executor information behavior

**Current behavior.** A DataFunction's canonical identity is `uri`
(`data_function.py:188-195`): `(canonical_function_name, output_data_schema_hash,
f"v{major_version}", data_function_type_id)`. Both identity chains collapse to it
(`identity_structure`/`pipeline_identity_structure` both return `uri`). **Function code and git
hash are computed** (`_function_content_hash` `:467`, `_git_hash` `:450`) **but intentionally
excluded from identity** — stored only observationally via `get_function_variation_data()` and
persisted to the result DB `PF_VARIATION_*` columns (`result_cache.py:255-261`). Executor
identity/config is likewise observational-only: surfaced through `get_execution_data()`
(`data_function.py:494-517`) into `PF_EXECUTION_*` columns; **never enters any hash and never
reaches the pipeline/tag table.** The pipeline record (`add_pipeline_record`,
`function_node.py:1643-1760`) writes `NODE_CONTENT_HASH_COL`, `INPUT/OUTPUT_DATA_HASH_COL`,
`IS_EPHEMERAL_COL`, etc., but **no** version/executor/variation columns — version info is carried
only by the **table path** (`uri` embeds `f"v{major_version}"`). `DerivedSource` re-exposes a
node's records via `origin.get_all_records()` with default column config, stripping all
version/executor metadata (`derived_source.py:76-90`).

**Intended behavior (v0.2).**
- **Coarse cache-invalidation via `major_version` is correct and should stay** — it is the user's
  deliberate control over the granularity of change that triggers recomputation. Code edits
  *should not* invalidate the cache unless the author bumps the version.
- **Missing but intended:** an **opt-in fine-grained** control letting a pod author pin matching
  to the exact function content hash and/or git hash ("treat any code change as a new
  computation," or "match only this git commit"). The observational hashes are the raw material;
  the selectable-granularity matching layer is absent.
- Executor identity/config should be **coherently observable** (consistent schema) and, where it
  affects results, reachable from stored records — today it's inconsistent and path-only.

**Delta.**
1. **No fine-grained match granularity (feature gap, not a bug).** `ResultCache.lookup` matches
   on `INPUT_DATA_HASH_COL` only (`result_cache.py:185-195`); there is no per-pod config to
   escalate matching to `_function_content_hash`/`_git_hash`. This is the intended-but-missing
   capability (relates to `match_tier`, DESIGN_ISSUES P6). **NOT** the "stale cache = bug" framing.
2. **Executor identity absent from identity hash and pipeline record;** dead
   `__pod_version`/`__execution_engine` constants that were meant to carry it (X3).
3. **Inconsistent executor metadata schema:** `get_execution_data` hard-codes
   `executor_info: dict[str,str]` + json-stringifies (`data_function.py:502-526`), diverging from
   each executor's own `get_executor_data_schema` (e.g. `ray.py:348-361`), which is effectively
   unused.
4. **Duplicated identity state:** `DataFunctionWrapper` leaves `_major_version`/`_minor_version`
   as dead defaulted state (DESIGN_ISSUES P3); `NODE_CONTENT_HASH_COL` is both written to records
   and excluded from the entry-id preimage as "determined by path" (P7) — two sources of one fact.
5. `minor_version` parsed then discarded from identity (`data_function.py:149-156`) — consistent
   with the coarse-control design, but worth an explicit doc note.
6. Two Ray implementations (X2).

**Ripple effects.** A fine-grained match layer touches `ResultCache.lookup`/`store`, a new
`PodConfig`/`NodeConfig` granularity field (`types.py`), and docs (`hashing.md`). Wiring executor
identity touches `add_pipeline_record`, `system_constants.py` (revive or delete the dead
constants), and `get_execution_data`/executor schemas.

**Risk of not fixing.** The fine-grained control is a genuine capability gap for users who *want*
code-level cache invalidation on specific pods — without it they must abuse `major_version`. The
dead config actively misleads (X3). Executor-blind records weaken provenance for the
distributed/Ray case.

**Priority.** **P1** for the opt-in fine-grained match-granularity feature (size **M** — needs a
config surface + lookup path + tests). **P2** for executor-identity-in-records + dead-config
sweep + executor-schema consistency (size **M**). **P3** for minor-version doc note + P3/P7
cleanup.

**Follow-ups:** *"Opt-in fine-grained cache match granularity (function-content / git hash)"*
(P1); *"Wire or remove executor/version columns + dead constants"* (P2); *"Consolidate Ray
implementations"* (P2, = X2).

---

## Area 3 — Pipeline execution / node retry policy

**Current behavior.** **No retry logic exists anywhere** in the pod execution path (`git grep`
for `max_retries`/`retry_exceptions`/`max_task_retries` → zero hits; the only backoff is
source-polling, `polling_source.py:664-682`). Ray tasks dispatch with user-supplied
`_remote_opts` and nothing sets Ray's own `max_retries`, so Ray uses defaults (retry system
failures, not application errors). Execution path: `orchestrator.run` → `FunctionNode.execute`
(`function_node.py:1210`, the **only** execution-level `try/except`, `1275-1289`) →
`_process_data_internal` → `CachedFunctionPod.process_data` → `FunctionPod.process_data` →
`PythonDataFunction.call` → `executor.execute_callable` (local `local.py:45` `fn(**kwargs)`; Ray
`ray.py:185` `.remote` + `188` `ray.get`). Every handler catches **bare `Exception`** for
capture-and-re-raise; none recover. The top-level handler is binary: `error_policy=="fail_fast"`
re-raises, else (`"continue"`, default) **silently skips the failed row**.
`InvocationStatus` (`hooks.py:23-35`) has exactly `COMPUTED`/`HIT`/`ERROR` — one undifferentiated
error bucket, **no failure classification**, no OOM detection. Ray `memory` is a documented but
unused pass-through (`ray.py:67`).

**Intended behavior (v0.2 / ITL-557).** Snakemake-style: submit at a modest baseline memory, let
OOM outliers fail, auto-resubmit failed jobs with scaled memory (`base * attempt`), capped by
max-attempts and a memory ceiling — **only** for memory-class failures, never for bad-input/bug
failures. Retries visible in logs/stats.

**Delta.** (1) No retry loop / attempt counter anywhere. (2) No failure classification —
prerequisite for "retry only on OOM." (3) No OOM detection (Ray `OutOfMemoryError` /
`WorkerCrashedError` not inspected). (4) No per-attempt resource escalation (`with_options` exists
but is never called and never recomputes memory). (5) **Sync/async divergence** (see below). (6)
No config surface (`NodeConfig`/`PodConfig`/`PipelineConfig` have no `max_retries`/`mem_mb`/
scaling fields). (7) Partial-failure ordering risk (X1) — a retry layer must be idempotent w.r.t.
result-then-tag write ordering.

**Ripple effects.** Retry loop lands in `FunctionNode.execute`/`_process_data_internal` (+ async
twin); OOM detection in `RayExecutor._handle_worker_error`; new classification exceptions in
`errors.py`; `InvocationStatus`/`RunStats` extended in `hooks.py`; config fields in `types.py`;
the `error_policy` surface in both orchestrators becomes a retry policy. Must pick one Ray impl
(X2).

**Risk of not fixing.** This is the single highest-leverage feature from the ephys field trial —
memory is the real cluster bottleneck. Shipping v0.2 without it means users keep over-provisioning
memory and losing parallelism. But note: it is already scoped as its own spike (ITL-557), so the
review's job is to hand it a clean baseline, not to build it here.

**Priority.** The retry feature itself: **P2 for v0.2** (important, field-driven, but explicitly a
separate spike — don't let it block the release). The **sync/async error-policy divergence is a
separate real bug: P1** — the async streaming path always skips-and-continues
(`function_pod.py:557-561`, `function_node.py:2478-2489`), **ignoring `fail_fast`**, so the same
pipeline behaves differently sync vs. async. Failure classification groundwork: **P2, size M**
(unblocks ITL-557).

**Follow-ups:** feed baseline + delta into ITL-557; file *"Async execution path ignores
`error_policy=fail_fast`"* (P1, size S); *"Execution-failure classification (OOM/transient/
permanent)"* (P2, prerequisite for ITL-557).

---

## Area 4 — Interface for ephemeral storage

**Current behavior (what actually landed in ITL-534 / PR #229).**
- **`EmptyData(Data)`** (`datagrams/tag_data.py:483`) — a first-class missing-data token. Carries
  `cached_content_hash` + optional `empty_source_info`; **every payload accessor raises**
  `EmptyDataAccessError` (`as_dict`/`as_table`/`keys`/`schema`/`identity_structure`), so it can't
  be mistaken for a row of nulls. `content_hash()` returns the cached hash or raises
  `EmptyDataHashMissingError`.
- **Ephemerality is declared per-node**, not per-pod: `NodeConfig.is_result_ephemeral`
  (`types.py:365`) + a separately-injected store via `set_ephemeral_store` (`function_node.py:1118`;
  `Pipeline.set_ephemeral_store` fans out, `pipeline/base.py:141`). **Operators are no-op stubs**
  (`operator_node.py:589`, deferred to ITL-509) — only function-pod nodes support ephemerality.
- **Storage lifecycle:** ephemeral results live in whatever `ArrowDatabaseProtocol` is injected;
  a single pipeline tag table with an `IS_EPHEMERAL_COL` discriminator per row; two separate result
  DB instances (persistent vs. ephemeral). **No expiry/pruning/retention/TTL exists anywhere** —
  `grep expir|prune|retention|ttl|evict` finds only docstrings. The code only *reacts* to already-
  missing data.
- **Presence/absence reasoning:** `_fetch_joined_records` (`function_node.py:1862`) partitions tag
  rows by `IS_EPHEMERAL_COL`, inner-joins each against its store, and turns unmatched **ephemeral**
  rows into `EmptyData` keyed by `OUTPUT_DATA_HASH_COL`. Downstream (`_process_data_internal:1347`)
  guards on `isinstance(data, EmptyData)`, looks up by cached hash → hit emits cached result, miss
  raises `EphemeralResultMissingError`.

**Intended behavior (v0.2).** The ephemeral interface should have a coherent lifecycle contract
(even if expiry is external, the *interface* should state it), symmetric treatment across node
types (or an explicit "function-pods-only in v0.2" statement), validated provenance (an
`EmptyData` should be distinguishable from corruption), and a clean cross-session flow-through
story.

**Delta (impl vs. ITL-534 design).**
1. **Flow-through key silently changed INPUT→OUTPUT hash** during implementation
   (`function_node.py:2009`, comment rejects `INPUT_DATA_HASH_COL`). `INPUT_DATA_HASH_COL` is
   **written but never read** — dead write. Correctness is fine; the dead column + plan/tests
   drift is confusing. **P2, size S.**
2. **Downstream tag-row write on `EmptyData` cache-hit was intentionally dropped**
   (`function_node.py:1344-1346`) because `EmptyData.as_table()` raises. Consequence: a node that
   serves a cached result for an `EmptyData` input records **no tag row**, so a further-downstream
   node or a fresh cross-session read **cannot reconstruct the chain**. This is the crux of
   ITL-534's deferred tag-row-reconstruction extension. **P1** if cross-session ephemeral
   flow-through is a v0.2 use case; size **M**.
3. `empty_source_info` is defined but **never populated** (no producer in `src/`) — provenance
   surface is inert. **P3, size S** (data-model-only as designed).
4. **No upstream-ephemerality validation:** any `EmptyData` miss raises regardless of whether the
   upstream was actually declared ephemeral — corruption looks identical to legitimate absence.
   **P2, size S.**
5. **No lifecycle/retention interface** (see above) — the contract is silent on who deletes and
   when. **P2, size M** (at minimum document; ideally a retention hook).
6. **No read-back verification** and **no cross-store atomicity** (X1) between tag-row and result
   write.
7. Old-format rows lacking `OUTPUT_DATA_HASH_COL` → `cached_content_hash=None` + warning; backfill
   is ITL-535 (merged).

**Ripple effects.** Tag-row reconstruction touches `_process_data_internal` (both sync/async),
`add_pipeline_record`, and `EmptyData.empty_source_info`. Retention touches a new module (none
exists under `databases/`). Operator support is ITL-509.

**Risk of not fixing.** Without the downstream tag-row write, ephemeral flow-through works only
within a single in-memory session — the headline "survive missing intermediate data across runs"
benefit is not actually realized cross-session. Silent dead `INPUT_DATA_HASH_COL` and inert
`empty_source_info` are low-risk but confuse future implementers.

**Priority.** **P1**: cross-session tag-row reconstruction on `EmptyData` hit (delta 2). **P2**:
upstream-ephemerality validation, lifecycle/retention contract, dead-column cleanup. **P3**:
populate `empty_source_info`.

**Follow-ups:** *"Write reconstructed downstream tag row on EmptyData cache-hit (cross-session
flow-through)"* (P1); *"Validate EmptyData originates from a declared-ephemeral upstream"* (P2);
*"Define ephemeral-result lifecycle/retention interface"* (P2); *"Remove dead INPUT_DATA_HASH_COL
write / reconcile plan"* (P2).

---

## Area 5 — "Tag table present but result table missing" (degraded/mixed state)

**Scope note.** The two-table split exists **only for `FunctionJobNode`**. `OperatorJobNode`
stores tags + data inline in one table (`operator_node.py:837/885`) and reads with a plain
`get_all_records` — no join, so the mixed-state problem doesn't arise there. All findings below
are in `core/nodes/function_node.py`.

**Current behavior.** The single join site is `_fetch_joined_records`
(`function_node.py:1862-2067`): reads the tag table, partitions by `IS_EPHEMERAL_COL`, inner-joins
each partition against its result store on `DATA_RECORD_ID`. Miss handling is **asymmetric**:
- **Persistent tag row, result missing → silently dropped** with only a `logger.warning`
  ("These inputs will be recomputed", `:1958`). The inner join `:1950` discards unmatched rows.
  The "will be recomputed" claim holds **only in FULL execute mode**; in
  `iter_data`/`CACHE_ONLY`/`READ_ONLY`/`get_all_records` there is **no recompute** — the row just
  vanishes, and a downstream consumer receives a **short stream that looks complete**. This is the
  primary silent-correctness site.
- **Ephemeral tag row, result missing → `EmptyData`** (ITL-534, first-class).
- **Result row with no tag row (orphan) → silently excluded**, undetected, never GC'd (join is
  inner and tag-driven; no reverse anti-join). Created by the non-atomic write ordering (X1).

**Invariants:** essentially none — no count check, no referential integrity, no orphan detection,
no cross-table validation (`grep transaction|atomic|referential|integrity|orphan` finds only
unrelated single-DB rollback code). `get_all_records` returns `None` for both "zero rows" and "all
rows dropped due to missing results" (`:1836`) — **lossy conflation**.

**Intended behavior (v0.2).** Missing persistent results should be **loud, not silent** —
either a typed error, or a first-class degraded token (like `EmptyData`) that downstream must
handle explicitly, never a silently-shortened stream. Orphans should be detectable. The
empty-vs-degraded distinction should survive the `get_all_records` return.

**Delta.** (1) Asymmetric miss handling — persistent misses silently dropped vs. ephemeral →
`EmptyData`. (2) No orphan (result-without-tag) detection. (3) No cross-table invariant/count/
referential check. (4) Non-atomic two-DB write (X1). (5) Lossy `None` return conflates empty vs.
degraded. (6) `IS_EPHEMERAL_COL` is the sole routing authority — a wrong/missing flag routes a
recoverable case into the silent-drop path. (7) Degraded state emits only `logger.warning`, no
structured signal an orchestrator can react to.

**Ripple effects.** Touches `_fetch_joined_records`, `_load_cached_entries`, `get_all_records`,
`add_pipeline_record`, and the write path (`_process_data_internal` + async twin). A "persistent
result missing" typed state/error would live in `errors.py`; any transactional/referential
primitive would need `protocols/database_protocols.py` + a connector (none exists today).

**Risk of not fixing.** **This is the most dangerous finding in the review**: in read-only /
cache-only mode, lost persistent results produce a silently-shortened, apparently-complete
stream — wrong results with no error. For a data-provenance/pipeline tool, silent data loss is a
credibility-critical failure.

**Priority.** **P0** (proposed) for the silent-drop-of-persistent-misses in non-recompute read
modes — this should fail loudly or emit a degraded token, never silently shorten a stream (size
**M**). **P1** for cross-store write atomicity (X1) and orphan detection (size **M**). **P2** for
`get_all_records` empty-vs-degraded distinction and cross-table invariant checks (size **M**).

**Follow-ups:** *"Persistent result missing must not silently drop rows in read-only/cache-only
modes"* (P0); *"Cross-store write atomicity for result+tag"* (P1, = X1); *"Orphan-result
detection / consistency check"* (P2).

---

## Candidate net-new focus areas (ITL-564 caps at 2–3 — your call)

1. **Sync/async execution-path divergence.** Beyond the `error_policy` bug (Area 3), the sync and
   async paths differ in error handling, logging, and cleanup. Worth a dedicated
   reconciliation pass rather than fixing case-by-case. *Recommend adopting as a focus area.*
2. **Cross-store write atomicity / transactionality (X1).** Currently folded into Area 5, but it
   underpins Areas 3, 4, and 5 and may deserve its own design treatment. *Recommend keeping folded
   unless you want a standalone design.*

---

## Proposed priority summary

| # | Finding | Area | Tier | Size |
|---|---|---|---|---|
| 1 | Persistent result miss silently drops rows in read-only/cache-only modes | 5 | **P0** | M |
| 2 | Cross-store write atomicity (result+tag) / orphan creation (X1) | 5/3/4 | P1 | M |
| 3 | Structured log rows lack record_id/invocation_hash (can't join logs→outputs) | 1 | P1 | M |
| 4 | Async path ignores `error_policy=fail_fast` (sync/async divergence) | 3 | P1 | S |
| 5 | Opt-in fine-grained cache match granularity (fn-content/git hash) | 2 | P1 | M |
| 6 | Cross-session tag-row reconstruction on EmptyData hit | 4 | P1 | M |
| 7 | run_id format normalization | 1 | P1 | S |
| 8 | Failure classification groundwork (unblocks ITL-557) | 3 | P2 | M |
| 9 | Consolidate two Ray implementations (X2) | 2/3 | P2 | S |
| 10 | Wire-or-remove dead config/constants (X3) | 2/all | P2 | S |
| 11 | Upstream-ephemerality validation of EmptyData | 4 | P2 | S |
| 12 | Ephemeral lifecycle/retention interface | 4 | P2 | M |
| 13 | Orphan-result detection / cross-table invariants | 5 | P2 | M |
| 14 | Operator-node data-level logging | 1 | P2 | M |
| 15 | Dead INPUT_DATA_HASH_COL write cleanup | 4 | P2 | S |
| 16 | `get_all_records` empty-vs-degraded distinction | 5 | P2 | M |
| 17 | Auto-retry w/ memory scaling (→ ITL-557) | 3 | P2* | L |
| 18 | Populate `empty_source_info` / provenance surface | 4 | P3 | S |
| 19 | Phantom `log_cache_hits` doc fix | 1 | P3 | S |

\* Retry feature is a separate spike (ITL-557); tier reflects v0.2 release priority, not the
spike's own importance.

---

## Next steps

1. Review/adjust priorities against the real v0.2 feature-freeze bar (especially the single P0
   and the six P1s).
2. Decide the two candidate net-new focus areas (adopt / fold / drop).
3. On approval, file follow-up Linear issues for each P0/P1 (and P2s you want tracked) under
   *Orcapod Python v0.2 Feature Sprint*, linking back to this doc and cross-referencing
   ITL-534/535/544/557/523 where relevant. **No issues will be filed until you approve the list.**
