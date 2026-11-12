# Design & Implementation Issues

A running log of identified design problems, bugs, and code quality issues.
Each item has a status: `open`, `in progress`, or `resolved`.

**Severity guide:**
- **critical** — Correctness bugs, silent data loss, or security issues.
- **high** — Broken or incomplete features that affect users or downstream consumers.
- **medium** — Performance, error-handling, or code-quality issues worth fixing in the
  normal course of development.
- **low** — Nice-to-haves, cosmetic improvements, or speculative refactors.
- **trivial** — Typos, dead comments, purely cosmetic.

---

## Cross-cutting

### CC1 — Empty tables lose field nullability, aborting `error_policy="continue"` pipelines
**Status:** resolved
**Severity:** high
**Issue:** ITL-563

Five sites construct empty PyArrow tables by building a dict of `pa.array([], type=...)` and
passing it to `pa.table()`. Because `pa.table(dict_of_arrays)` marks every field
`nullable=True`, required fields (`str`, `int`, …) lose their non-nullable annotation.
When a failing function's empty output is fed into a `Join`, the mismatched schemas raise an
`InputValidationError` that bypasses `error_policy="continue"` and aborts orchestration.

Sites: `sync_orchestrator.py:_materialize_as_stream`, `operator_node.py:_make_empty_table`,
`side_effects.py:SideEffectPodStream.as_table`,
`side_effects.py:SideEffectNode.as_table`,
`derived_source.py:DerivedSource._get_stream`.

**Fix:** Extracted `arrow_utils.make_empty_table(python_schema, type_converter)` using
`python_schema_to_arrow_schema` + `pa.Table.from_batches([], schema=...)`. Replaced all five
buggy sites: `sync_orchestrator._materialize_as_stream`, `operator_node._make_empty_table`,
`SideEffectPodStream.as_table`, `SideEffectNode.as_table`, and `DerivedSource._get_stream`.
Also fixed a latent null-typed array bug in `Join.static_process`. Added unit tests,
integration test for the exact bug topology, and per-site regression tests. PR: ITL-563.

---

## `src/orcapod/core/nodes/source_node.py`

### SJN1 — `SourceJobNode.async_iter_data()` wraps `iter_data()` synchronously, bypassing `PollingSource` polling loop
**Status:** resolved
**Severity:** high
**Issue:** ITL-615

`SourceJobNode` does not override `async_iter_data()`. The base-class
`SourceNodeBase.async_iter_data()` wraps `self.iter_data()` (sync) as an async generator,
so `bound_source.async_iter_data()` is never called. For `PollingSource`,
`iter_data()` returns a static single-batch snapshot — the async polling loop is never started.
This contradicts the stated intent of `async_execute`'s own docstring.

**Fix:** Added `SourceJobNode.async_iter_data()` in `source_node.py` that delegates to
`self._bound_source.async_iter_data()`, consistent with the existing delegation pattern of
`iter_data()`, `output_schema()`, and `as_table()`. Also fixed `PollingSource.output_schema()`
and `keys()` to use the cached ``accumulated_stream`` directly when available, avoiding a
``_run_sync`` poll that races with the async polling loop (see PS1 below). Added integration
test `test_polling_source_pipeline_integration.py`.

---

## `src/orcapod/core/sources/polling_source.py`

### PS1 — `PollingSource.output_schema()` and `keys()` trigger `_run_sync` concurrently with the async polling loop, advancing the cursor and skipping batches
**Status:** resolved
**Severity:** high
**Issue:** ITL-615

`PollingSource.output_schema()` and `keys()` fall through to `_get_latest_stream()` whenever
either ``columns is not None`` or ``all_info=True``. `_get_latest_stream()` calls
`_run_sync(self._impl.poll, ...)` which spawns a `ThreadPoolExecutor` thread that runs
`asyncio.run(poll(...))`. This thread advances `self._cursor` independently of the async
polling loop in `async_iter_data()`.

The trigger is `FunctionJobNode.async_execute()` (line 2325 of `function_node.py`):

```python
tag_schema = self._input_stream.output_schema(columns={"system_tags": True})[0]
```

This is called once per `async_execute` invocation, which runs concurrently with the
source node's `async_execute` (both live inside the orchestrator's outer ``TaskGroup``).
Because ``columns`` is not ``None``, the old fast-path was missed, `_get_latest_stream()`
ran its poll+fetch cycle in a background thread, advancing the cursor from 0 → 1. When
the async polling loop then resumed at its next tick, it called ``poll(cursor=Cursor(1))``,
found no more data, and exited — processing only the first batch.

**Fix:** `output_schema()` and `keys()` now return from declared schemas directly when both
``tag_schema`` and ``data_schema`` were provided at construction and ``columns is None`` with
``all_info=False`` (removing the previous ``accumulated_stream is None`` guard from the
fast-path condition). When ``accumulated_stream`` is already populated (at least one batch
fetched by either path), the existing stream is used to answer schema queries without
triggering a new poll+fetch cycle. Schema is invariant across batches (enforced by
``_validate_combining_schemas``), so this is always correct. Schema is now declared by
``impl.schema()`` (returning a unified column schema split by ``tag_columns``) rather than by
``PollingSource`` constructor params.

### PS2 — Concurrent iteration over a single `PollingSource` has unguarded cursor/stream mutation
**Status:** open
**Severity:** high
**Issue:** ITL-625

`PollingSource` maintains shared mutable state (`_cursor` and `_accumulated_stream`) that is
updated without synchronization. Two simultaneous callers of `async_iter_data()` (or one async
and one sync caller) can therefore race:

- **Cursor interleaving** — both callers call `impl.poll(cursor=self._cursor)` concurrently.
  One receives a new cursor and advances `self._cursor`. The other then calls
  `impl.fetch(new_cursor)` instead of the cursor it intended.
- **Accumulated-stream clobbering** — `self._accumulated_stream = self._combine(...)` may be
  assigned by both callers, with one overwriting the other's in-flight result.

The iteration order guarantee — drain accumulated stream first, then poll from saved cursor —
is correct for a single caller, but breaks under concurrent callers because neither the drain
nor the advance is atomic.

**Fix:** Guard updates to `_cursor` and `_accumulated_stream` with an `asyncio.Lock` (async
path) and a `threading.Lock` (sync path, or use a single lock exposed via `_run_sync`). The
pre-seed loop in `async_iter_data` should take a snapshot of `_accumulated_stream` under the
lock before iterating, so later writes to the stream don't affect the snapshot mid-yield.

---

### PS3 — `_combine` leaks `_content_hash` into the data schema on the second accumulating fetch
**Status:** resolved
**Severity:** high
**Issue:** ITL-616

`_combine` calls `as_table(all_info=True)` on both streams before concatenating them.
`all_info=True` resolves to `ColumnConfig.all()`, which includes `content_hash=True`.
In `ArrowTableStream.as_table()`, `content_hash=True` dynamically appends a `_content_hash`
column to the output table. This is a synthetic column — computed on demand, not stored in
`ArrowTableStream._table`.

`pa.concat_tables` then includes `_content_hash` in the combined table, which is passed
directly to `ArrowTableStream.__init__`. Since `_content_hash` has no recognized prefix
(`_tag::`, `_source_`, `_context_key`), it lands in `_data_columns` as if it were user data.

On the next `_combine` call, `_validate_combining_schemas` compares:
- `existing.keys()` → includes `_content_hash` in data keys (baked in from previous combine)
- `new_stream.keys()` → no `_content_hash` (freshly built from raw fetched data)

This raises `SchemaInconsistencyError`. A polling source emitting one new row per poll will
change its data schema on the second new-data poll and crash on the third.

**Fix:** Added `_STREAM_COMBINE_COLUMNS = ColumnConfig(system_tags=True, source=True, context=True)` constant and replaced `as_table(all_info=True)` with `as_table(columns=_STREAM_COMBINE_COLUMNS)` in `_combine`. `content_hash` is intentionally excluded — it is a synthetic output column, never a stored one.

---

### PS4 — Concurrent sync access during async run silently loses rows
**Status:** resolved
**Severity:** critical
**Issue:** ITL-617

`_get_latest_stream()`, called by `iter_data()` or `as_table()` while
`async_iter_data()` is running, could advance `_cursor` and fold new rows into
`_accumulated_stream` without the async loop emitting them — permanent silent data
loss. `ITL-615` (PR #255) partially addressed this for `output_schema()` and `keys()`
by caching the stream reference. `ITL-617` fixes the root.

**Fix:** Replaced single `_accumulated_stream: ArrowTableStream | None` with
append-only `_batches: list[ArrowTableStream]` and `_state_lock: threading.Lock`.
All callers (sync and async) use an optimistic lock protocol: snapshot cursor (brief
lock) → perform I/O freely with no lock held → commit only if cursor is unchanged
(brief lock). The async loop tracks its yield position with a per-iterator
``local_batch_idx`` local variable and drains ``_batches`` at the top of each
iteration, ensuring rows committed by a concurrent sync caller are yielded even when
the async loop loses the commit race. The lock is never held across ``await``.

---

### PS5 — `PollingSource` re-infers Arrow schema nullability per batch, crashing on zero-row polls
**Status:** resolved
**Severity:** high
**Issue:** ENG-952

`_build_stream_from_df` called `infer_schema_nullable` on every batch. A zero-row batch has
`null_count == 0` for all columns, so every field was inferred non-nullable.
`_validate_combining_schemas` then rejected the batch against the accumulated stream's
nullable schema.

**Fix:** `_build_stream_from_df` now establishes a `_canonical_arrow_schema` exactly once —
from `impl.schema()` when declared (no inference, no warning), or from the first non-empty
batch otherwise (with a `WARNING`-level log). All subsequent batches are cast to the canonical
schema by column name. Per-batch nullability inference is eliminated. Zero-row frames before
canonical schema establishment are skipped on the infer-once path.

---

## `src/orcapod/core/nodes/function_node.py`

### FN1 — `FunctionNodeBase.as_table()` returned empty schema when no data existed
**Status:** resolved
**Severity:** high
**Issue:** ENG-572

`as_table()` on an unrun node returned a zero-row table with no columns because
the `if not all_tags: self._cached_output_table = pa.table({})` assignment was
immediately overwritten by the fall-through `hstack_tables` call with
`schema=None`.

**Fix:** Restructured `as_table()` to derive schema from `self.output_schema()`
upfront and branch immediately on empty vs. non-empty, so the empty case creates
a properly-schemed zero-row table. Extracted column-filtering logic into
`arrow_utils.apply_column_config()` standalone function.  Also fixed
`DATACLASS_TYPE_FIELD` sentinel rename (`__type` → `__dataclass.`) and
`arrow_schema_to_python_schema` returning `Any` for dataclass structs
(now synthesizes a concrete dataclass type).

---

## `src/orcapod/core/base.py`

### B1 — `PipelineElementBase` should be merged into `TraceableBase`
**Status:** resolved
**Severity:** medium

`TraceableBase` and `PipelineElementBase` co-occur in every active computation-node class
(`StreamBase`, `DataFunctionBase`, `_FunctionPodBase`). The two current exceptions are design
gaps rather than intentional choices:

- `StaticOutputPod(TraceableBase)` — should implement `PipelineElementProtocol`; its absence
  forced `DynamicPodStream.pipeline_identity_structure()` to include an `isinstance` check as
  a workaround.
- `Invocation(TraceableBase)` — legacy tracking mechanism, planned for revision.

Note: merging into `TraceableBase` is correct at the *computation-node* level.
`ContentIdentifiableBase` (which `TraceableBase` builds on) should **not** absorb
`PipelineElementBase` — data datagrams (`Tag`, `Data`) are legitimately content-identifiable
without being pipeline elements.

**Fix:** Added `PipelineElementBase` to `TraceableBase`'s bases. Added
`pipeline_identity_structure()` to `StaticOutputPod`. Removed redundant explicit
`PipelineElementBase` from `StreamBase`, `ArrowTableStream`, `DataFunctionBase`,
`_FunctionPodBase`, `FunctionPodStream`, `FunctionNode`, `OperatorNode`, and
`DynamicPodStream` declarations.

---

### B2 — Mutable `data_context` setter may invalidate cached state
**Status:** open
**Severity:** medium

`DataContextMixin.data_context` (line ~92) has a property setter that allows runtime context
changes. If a stream or pod has already cached schemas or hashes derived from the previous
context, those caches silently become stale.

Options: (1) remove the setter and make context immutable after construction, or (2) add cache
invalidation on context change and document when changing context is safe.

---

## `src/orcapod/core/data_function.py`

### P1 — `parse_function_outputs` is dead code
**Status:** resolved
**Severity:** medium
`parse_function_outputs` is a module-level function with a `self` parameter, suggesting it was
originally a method. It is never called. `PythonDataFunction.call` duplicates its logic verbatim.
Should be deleted or wired up as a method on `DataFunctionBase` and used from `call`.

**Fix:** Converted to a proper standalone function `parse_function_outputs(output_keys, values)`.
Replaced the duplicated unpacking block in `PythonDataFunction.call` with a call to it.
Updated tests accordingly.

---

### P2 — `CachedDataFunction.call` silently drops the `RESULT_COMPUTED_FLAG`
**Status:** resolved
**Severity:** high
On a cache miss, the flag is set but the result is discarded:
```python
output_data.with_meta_columns(**{self.RESULT_COMPUTED_FLAG: True})  # return value ignored
```
If `with_meta_columns` returns a new data (immutable update), the flag is never actually
attached.

**Fix:** Assigned the return value: `output_data = output_data.with_meta_columns(...)`.
Added tests verifying the flag is `True` on cache miss and `False` on cache hit.

---

### P3 — `DataFunctionWrapper.__init__` passes no `version` to `DataFunctionBase`
**Status:** open
**Severity:** medium
`DataFunctionBase.__init__` requires a `version` string and parses it into
`_major_version`/`_minor_version`. `DataFunctionWrapper` calls `super().__init__(**kwargs)`
without a `version`, so it either crashes (no version in kwargs) or silently defaults to `"v0.0"`.
Those parsed fields are then shadowed by the delegating properties, making them dead state.
Options: pass the inner function's version through, or avoid calling the base version-parsing
logic entirely.

---

### P4 — `PythonDataFunction` computes the output schema hash twice
**Status:** resolved
**Severity:** low
`__init__` stores `self._output_schema_hash` (line ~289). `DataFunctionBase` also lazily
caches `self._output_data_schema_hash` (different attribute name) via
`output_data_schema_hash`. Two fields holding the same value. One is redundant.

**Fix:** Removed the eager `self._output_schema_hash` assignment from
`PythonDataFunction.__init__`. The canonical schema hash is now computed and cached exclusively
by `DataFunctionBase.output_data_schema_hash` (stored in `_output_data_schema_hash`).

---

### P5 — Large dead commented-out block in `get_all_cached_outputs`
**Status:** resolved
**Severity:** low
The block commenting out `pod_id_columns` removal is leftover from an old design. It makes it
ambiguous whether system columns are actually filtered.

**Fix:** Deleted the commented-out block.

---

### P6 — Cache-matching policy (`match_tier`) accepted but never used
**Status:** open
**Severity:** high

`CachedDataFunction.get_cached_output_for_data()` (line ~547) accepts a `match_tier`
parameter that is documented in the interface but completely ignored. Cache lookups always use
exact matching. Two inline TODOs mark this:
- `# TODO: add match based on match_tier if specified`
- `# TODO: implement matching policy/strategy`

This means any caller passing a non-default `match_tier` silently gets exact-match behavior,
which could lead to unnecessary cache misses or incorrect assumptions about cache hit semantics.

Requires: design a matching strategy interface; implement at least exact and fuzzy tiers.

---

### P7 — `PythonDataFunction.__init__` unconditionally extracts git info
**Status:** open
**Severity:** medium

`PythonDataFunction.__init__()` (line ~324) always calls `get_git_info()`, which runs git
subprocess commands. This fails or significantly slows initialization in non-git environments
(CI containers, notebooks, deployed services).

`# TODO: turn this into optional addition`

Fix: add an `include_git_info=True` parameter; skip extraction when `False`.

---

## `src/orcapod/core/function_pod.py`

### F1 — `_FunctionPodBase.process` is `@abstractmethod` with unreachable body code
**Status:** resolved
**Severity:** high
The method is decorated `@abstractmethod` but has real logic after the `...` (handle_input_streams,
schema validation, tracker recording, FunctionPodStream construction). Since Python never executes
the body of an abstract method via normal dispatch, this code is unreachable. `FunctionPod`
then duplicates this logic verbatim.

**Fix:** Removed the unreachable body code from `_FunctionPodBase.process()`, keeping it as
a pure abstract method with only `...`. `FunctionPod.process()` retains its own concrete
implementation.

---

### F2 — Typo in `TrackedDataFunctionPod` docstring
**Status:** open
**Severity:** trivial
`"A think wrapper"` should be `"A thin wrapper"`.

---

### F3 — Dual URI computation paths in the class hierarchy
**Status:** resolved
**Severity:** low
`TrackedDataFunctionPod.uri` assembles the URI from `self.data_function.*` with its own lazy
schema-hash cache. `WrappedFunctionPod.uri` simply delegates to `self._function_pod.uri`. These
should agree (and do, after the `data_function` fix), but having two independent implementations
makes future changes fragile.

**Fix:** Removed `_output_schema_hash` cache from `_FunctionPodBase.__init__` and replaced the
`uri` property body with `return self.data_function.uri`. `_FunctionPodBase.uri` now delegates
to `DataFunctionBase.uri`, which owns the canonical hash computation and caching via
`output_data_schema_hash`.

---

### F4 — `FunctionPodNode` is not a subclass of `TrackedDataFunctionPod`
**Status:** open
**Severity:** medium
`FunctionPodNode` reimplements `process_data`, `process`, `__call__`, `output_schema`,
`validate_inputs`, and `argument_symmetry` from scratch rather than inheriting from
`SimpleFunctionPod`/`TrackedDataFunctionPod` and overriding the parts that differ
(fixed input stream, pipeline record writing). The result is a large amount of structural
duplication that diverges silently over time.

---

### F5 — `FunctionPodStream` and `FunctionPodNodeStream` are near-identical copy-pastes
**Status:** open
**Severity:** medium
`iter_data`, `as_table` (including content_hash and sort_by_tags logic), `keys`,
`output_schema`, `source`, and `upstreams` are duplicated almost line-for-line. The only
behavioural differences are:
- `FunctionPodNodeStream` has `refresh_cache()`
- `FunctionPodNodeStream.output_schema` reads from `_fp_node._cached_data_function` directly

A shared base stream class would eliminate the duplication.

---

### F6 — `WrappedFunctionPod.process` makes the wrapper transparent to observability
**Status:** open
**Severity:** medium
`process` simply calls `self._function_pod.process(...)`, so the returned stream's `source` is
the *inner* pod, not the `WrappedFunctionPod`. Anything that inspects `stream.source` (e.g.
tracking, lineage) will see the inner pod and be unaware of the wrapper. Whether this is
intentional should be documented; if not, `process` needs to construct a new stream whose source
is `self`.

---

### F7 — TOCTOU race in `FunctionPodNode.add_pipeline_record`
**Status:** resolved
**Severity:** medium
The method checks for an existing record with `get_record_by_id` and skips insertion if found.
But it then calls `add_record(..., skip_duplicates=False)`, which will raise on a duplicate. A
race between the lookup and the insert (e.g. two concurrent processes handling the same tag+data)
would cause a crash instead of a graceful skip. Should use `skip_duplicates=True` for consistency
with the intent.

**Fix:** ITL-508 redesigned `add_pipeline_record` to use indexed entry-ID versioning
(`max_index + 1`) with `skip_duplicates=True` on the versioned key. The
`skip_cache_lookup` parameter was removed entirely, eliminating both the TOCTOU
window and the stale-entry silent-skip failure described in ITL-513.

---

### F8 — `CallableWithPod` protocol placement breaks logical grouping
**Status:** open
**Severity:** low
`CallableWithPod` is defined between `FunctionPodStream` and `function_pod`, breaking the natural
grouping. It should be co-located with `function_pod` or moved to the protocols module.

---

### F11 — Schema validation raises `ValueError` instead of custom exception
**Status:** open
**Severity:** high

`_validate_input_schema()` in `_FunctionPodBase` raises a generic `ValueError` when the
incoming data schema is incompatible with the function's expected input schema:
```python
# TODO: use custom exception type for better error handling
raise ValueError(
    f"Incoming data data type {input_schema} is not compatible with ..."
)
```

Using `ValueError` means callers cannot distinguish schema incompatibility from other value
errors without string-matching the message.

Fix: define a new `SchemaCompatibilityError` (subclass of `InputValidationError`) in
`errors.py` and raise it from `_validate_input_schema()`. The existing
`SchemaInconsistencyError` covers batch/polling-source schema drift; this new type covers
function-pod input schema mismatch. Name the exception to reflect the cause:
incompatible upstream stream schema vs. function's expected input schema.

---

### F12 — System tag columns excluded from cache entry ID
**Status:** open
**Severity:** high

`FunctionPodNode.record_data_for_cache()` (line ~1077) builds a tag table for entry-ID
computation but excludes system tag columns:
```python
# TODO: add system tag columns
```

Two data with identical user tags but different provenance (arriving from different
pipeline branches, thus having different system tags) produce the same cache key. This can
cause cache collisions where a result computed for one pipeline branch is returned for
another.

Fix: include system tag columns in the `tag_with_hash` table before computing the entry ID hash.

---

### F13 — `_FunctionPodBase.output_schema()` omits source-info columns
**Status:** open
**Severity:** medium

`output_schema()` (line ~238) does not include source-info columns even when `ColumnConfig`
requests them:
```python
# TODO: handle and extend to include additional columns
```

This means callers using `columns={"source": True}` on a FunctionPod's output schema get an
incomplete schema, inconsistent with `as_table()` which does include source columns.

---

### F14 — `FunctionPodStream.as_table()` uses Polars detour for Arrow sorting
**Status:** open
**Severity:** medium

`as_table()` (line ~568) converts Arrow → Polars → sort → Arrow when sorting by tags:
```python
# TODO: reimplement using polars natively
```

The comment is misleading — the fix is actually to use PyArrow's native `.sort_by()` method
directly, eliminating the Polars dependency for this code path and reducing conversion overhead.

---

### F10 — `FunctionPodNodeStream.iter_data` recomputes every data on every call
**Status:** resolved
**Severity:** high
`iter_data` always iterates the full input stream and calls `process_data` for every data,
even when results are already stored in the result/pipeline databases.  This defeats the purpose
of the two-database design (result DB + pipeline DB) used to cache computed outputs.

**Fix:** Refactored `iter_data` to first call `FunctionPodNode.get_all_records(columns={"meta": True})`
to load already-computed (tag, output-data) pairs from the databases (mirroring the legacy
`PodNodeStream` design), yield those via `TableStream`, then collect the set of already-processed
`INPUT_PACKET_HASH` values and only call `process_data` for input data not yet in the DB.
Also added `FunctionPodNode.get_all_records(columns, all_info)` using `ColumnConfig` to control
which column groups (meta, source, system_tags) are returned.

---

### F15 — `FunctionPod` has no pod-level error policy; sync and async paths are inconsistent
**Status:** open
**Severity:** high
**Issue:** ITL-527

`FunctionPod` has no `on_error` configuration. The two execution paths behave differently:

- **Sync path** (`FunctionPodStream._iter_data_*`): no exception handling at all — exceptions
  propagate unconditionally out of the iterator.
- **Async path** (`_FunctionPodBase.async_execute()`): exceptions are caught, the observer is
  notified via `on_data_crash()`, and the item is **silently dropped from output** with no
  indication to the caller and no way to configure this behaviour.

The node-level `FunctionJobNode.execute()` adds `error_policy: Literal["continue", "fail_fast"]`,
but this is only available for DB-backed execution and uses different vocabulary from the rest
of the framework.

`SinkPod` and `TapPod` (ITL-524/ITL-525) introduced a clean `on_error: Literal["raise", "log"]`
vocabulary at the pod level. `FunctionPod` should adopt the same model: pod-level `on_error`
config, consistent behaviour between sync and async paths, and the same `"raise"` / `"log"`
vocabulary.

**Fix needed:** Add `on_error: Literal["raise", "log"] = "raise"` to `FunctionPodConfig` (or
equivalent). In the async path, replace the unconditional silent-drop with the configured
behaviour: `"raise"` propagates the exception; `"log"` logs at `WARNING` and drops the item
(current behaviour, but now explicit and configurable). Align `FunctionJobNode.error_policy`
to use the same vocabulary.

---

## `src/orcapod/core/cached_function_pod.py` / `src/orcapod/core/data_function.py`

### CFP1 — Extract shared result caching logic from CachedDataFunction and CachedFunctionPod
**Status:** resolved
**Severity:** medium

`CachedDataFunction` and `CachedFunctionPod` implement nearly identical result caching
logic: DB lookup by `INPUT_DATA_HASH_COL`, conflict resolution by most-recent timestamp,
record storage with variation/execution/timestamp columns, and a `RESULT_COMPUTED_FLAG`
meta column. The match tier / matching policy design (P6) will also need to apply to both.

**Fix:** Extracted `ResultCache` class (`src/orcapod/core/result_cache.py`) that owns the DB,
record path, lookup (with `additional_constraints` for future match tiers), store, conflict
resolution, and auto-flush logic. Both `CachedDataFunction` and `CachedFunctionPod` now
delegate to a `ResultCache` instance. The match tier strategy (P6) can be implemented once
on `ResultCache.lookup` via `additional_constraints`.

---

## `src/orcapod/core/nodes/function_node.py`

### FN1 — `FunctionNode.async_execute` Phase 2 was fully sequential
**Status:** resolved
**Severity:** high

`FunctionNode.async_execute` (formerly `PersistentFunctionNode`) had a fully sequential Phase 2
— each data was awaited one at a time in a simple `async for` loop.  This meant async data
functions (which can overlap I/O via `await`) got no concurrency benefit when run through the
Pipeline API.

The parent `FunctionNode.async_execute` already had the correct concurrent pattern using
`asyncio.Semaphore + TaskGroup`, but the persistent override did not replicate it.

**Fix:** Rewrote Phase 2 to use the same `Semaphore + TaskGroup` pattern as the parent class.
Phase 1 (replay cached results from DB) remains unchanged. Concurrency is controlled via
`NodeConfig.max_concurrency` and `PipelineConfig`, resolved through `resolve_concurrency()`.

---

## `src/orcapod/core/sources/`

### S1 — `source_name` and `source_id` are redundant and inconsistent
**Status:** resolved
**Severity:** high

`RootSource` defines `source_id` (canonical registry key, defaults to content hash).
`ArrowTableSource` defines `source_name` (provenance token prefix, defaults to `source_id`).
These are intended as the same concept — a stable name for the source — but they're two
separate parameters that can silently diverge:

- **Provenance tokens** embed `source_name` (e.g. `"heights::row_0"`)
- **SourceRegistry** is keyed by `source_id`
- If they differ, provenance tokens cannot be resolved via the registry

Delegating sources make this worse:
- `CSVSource` sets `source_name = file_path` but never sets `source_id` → registry key is a
  content hash while provenance tokens use the file path
- `DeltaTableSource` sets `source_name = resolved.name` but never sets `source_id` → same issue

Additionally, delegating sources all return `self._arrow_source.identity_structure()` which is
`("ArrowTableSource", tag_columns, table_hash)`. This means the outer source type (CSV, Delta,
etc.) is invisible to the content hash, and `source_id` (defaulting to content hash) will be
identical for a CSVSource and an ArrowTableSource with the same data.

**Fix:** Dropped `source_name` entirely. `source_id` is now the single identifier used for
provenance strings, registry key, and `computed_label()`. Delegating sources set `source_id`
to their meaningful default (`CSVSource` → `file_path`, `DeltaTableSource` → `resolved.name`).
All delegating sources now pass `source_id=self.source_id` to their inner `ArrowTableSource`.
Added `computed_label()` to `RootSource` returning `_explicit_source_id`.

---

### F9 — `as_table()` crashes with `KeyError` on empty stream
**Status:** resolved
**Severity:** high
Both `FunctionPodStream.as_table()` and `FunctionPodNodeStream.as_table()` unconditionally call
`.drop([constants.CONTEXT_KEY])` on the tags table built from the accumulated data. When the
stream is empty (e.g. because the data function is inactive), `iter_data()` yields nothing,
`tag_schema` stays `None`, and `pa.Table.from_pylist([], schema=None)` produces a zero-column
table. The subsequent `.drop([constants.CONTEXT_KEY])` then raises `KeyError` because the column
does not exist.

**Fix:** Guarded both `.drop([constants.CONTEXT_KEY])` calls in `FunctionPodStream.as_table()` and
`FunctionPodNodeStream.as_table()` with a column-existence check. Also made the final
`output_table = self._cached_output_table.drop(drop_columns)` safe by filtering `drop_columns`
to only columns that exist in the table.

---

## `src/orcapod/core/streams/`

### ST1 — `drop_data_columns` may leave orphan source-info columns
**Status:** open
**Severity:** medium

The `StreamProtocol.drop_data_columns()` method (line ~309) drops data columns but it is
unclear whether the corresponding `_source_<col>` columns are also removed:
```python
# TODO: check to make sure source columns are also dropped
```

If source-info columns survive after the data column is dropped, downstream consumers may see
stale provenance data or schema mismatches.

---

### ST2 — `iter_data()` does not support table batch streaming
**Status:** open
**Severity:** low

`ArrowTableStream.iter_data()` (line ~261) works only with fully materialized Arrow tables,
not with `RecordBatchReader` or lazy batch iteration:
```python
# TODO: make it work with table batch stream
```

Relevant for future streaming/chunked processing of large datasets.

---

### ST3 — Column selection operators duplicate `validate_unary_input()` five times
**Status:** open
**Severity:** medium

`SelectTagColumns`, `SelectDataColumns`, `DropTagColumns`, `DropDataColumns` (in
`column_selection.py:58`, `137`, `214`, `292`) and `PolarsFilterByDataColumns`
(`filters.py:135`) each have near-identical `validate_unary_input()` implementations. All are
marked:
```python
# TODO: remove redundant logic
```

The only difference between them is which key set (tag vs. data) is checked and the error
message text. A shared parameterized validation helper would eliminate the duplication.

---

## `src/orcapod/core/operators/` — Async execution

### O1 — Operators use barrier-mode `async_execute` only; streaming/incremental overrides needed
**Status:** in progress
**Severity:** medium

All operators originally used the default barrier-mode `async_execute` inherited from
`StaticOutputPod`: collect all input rows into memory, materialize to `ArrowTableStream`(s),
run the existing sync `static_process`, then emit results. This works correctly but negates the
latency and memory benefits of the push-based channel model.

Three categories of improvement are planned:

1. **Streaming overrides (row-by-row, zero buffering)** — for operators that process rows
   independently:
   - ~~`PolarsFilter` — evaluate predicate per row, emit or drop immediately~~ (kept barrier:
     Polars expressions require DataFrame context for evaluation)
   - `MapTags` / `MapData` — rename columns per row, emit immediately ✅
   - `SelectTagColumns` / `SelectDataColumns` — project columns per row, emit immediately ✅
   - `DropTagColumns` / `DropDataColumns` — drop columns per row, emit immediately ✅

2. **Incremental overrides (stateful, eager emit)** — for multi-input operators that can
   produce partial results before all inputs are consumed:
   - `Join` — symmetric hash join for 2 inputs (streaming, with correct
     system-tag name-extending via `input_pipeline_hashes` passed directly
     to `async_execute`); barrier fallback for N>2 inputs via `static_process`. ✅
   - `MergeJoin` — kept barrier: complex column-merging logic
   - `SemiJoin` — build right, stream left through hash lookup ✅

3. **Streaming accumulation:**
   - `Batch` — emit full batches as they accumulate (`batch_size > 0`); barrier fallback
     when `batch_size == 0` (batch everything) ✅

**Remaining:** `PolarsFilter` (barrier), `MergeJoin` (barrier) could receive incremental
overrides in the future but require careful handling of Polars expression evaluation and
system-tag evolution respectively.

`GroupBy` (NPIPE-204) is barrier-only by construction and is not a candidate for either category:
no group can be emitted before the input channel closes, because any row not yet seen could belong
to a group already started. Emitting early would require a guarantee that input arrives clustered
by group key, which orcapod streams do not carry.

---

### O2 — Operators silently discard a non-default `_context_key`
**Status:** open
**Severity:** medium

Every operator reads its input with
`stream.as_table(columns={"source": True, "system_tags": True})` — `batch.py:48`,
`merge_join.py:168`, `semijoin.py:60`, `column_selection.py:524`. That column set excludes
`_context_key`, so no operator ever sees the input's data context. `ArrowTableStream.__init__`
then finds no context column on the result and substitutes
`contexts.get_default_context_key()`.

Consequence: a stream carrying a non-default context key reverts to the default at the first
operator it crosses, with no warning. Function pods are unaffected — only the operator layer.

This is pre-existing and layer-wide rather than specific to any one operator. It may also be
intended (operators produce output in the ambient context rather than inheriting an input's), in
which case the fix is to document the rule rather than change behavior. Logged while working
NPIPE-204; deliberately not addressed there.

Fix: decide whether operator output should inherit the input context. If yes, request
`context: True` and propagate it, erroring when inputs disagree. If no, document the reset
explicitly on `StaticOutputPod`.

---

### O3 — `_materialize_to_stream` broadcasts row 0's provenance to every row
**Status:** resolved
**Severity:** high
**Issue:** ITL-845

`StaticOutputOperatorPod._materialize_to_stream` (`core/operators/static_output_pod.py:223`)
reads the source info of the *first* row and applies it to the whole concatenated table:

```python
# Preserve actual source_info provenance from the first row
# (all rows share the same data columns and source tokens).
source_info = rows[0][1].source_info()
```

The comment is half right. All rows do share the same data *column names*, but not the same
*source-info tokens* (the per-column `_source_*` values) — a source-info token embeds a per-row
identifier (`...::row_0::value` versus `...::row_1::value`, built by `_make_provenance_token`
in `core/sources/stream_builder.py`). So every row from index 1 onward is attributed to row 0.
`sync_orchestrator.py:207,222` repeats the pattern.

Observed directly in a `MergeJoin` round-trip: row 1 of the output carried `row_0` tokens.

This predates NPIPE-204 and is equally wrong for scalar tokens, so nothing in that change
caused it. NPIPE-204 did make it **more visible**: previously the list-valued case crashed at
`as_table` (see U1), so a broken operator never got far enough to mis-attribute provenance.
That crash is now fixed, which converts a loud failure into a quiet wrong answer.

Fix: build the source-info mapping per row rather than once from row 0, or push the
provenance into the concatenated table before constructing the stream.

**Fix:** Both materializers build their table with one helper,
`StaticOutputOperatorPod._rows_to_table`, and no longer pass `source_info` to
`ArrowTableStream`, so each row's own `_source_*` columns pass through. A `_source_*` field
is non-nullable unless some row's source-info token is `None`, which keeps stored schemas and
Arrow types as they were. The sync orchestrator materializes every node's buffer this way, so
the fix also corrects the input provenance written to function-node tag tables. Data values,
content and pipeline hashes, and function-node record IDs do not change; an operator node's
stored `_record_hash` changes for each row whose token was wrong. Rows stored before the fix
keep their wrong tokens and are not rewritten or recomputed; dropping the pipeline tables
(keeping `_result/`) and re-running refills them correctly without re-running functions.
Correcting stored values is tracked in ITL-853. Regression tests:
`tests/test_core/operators/test_materialize_source_info.py`,
`tests/test_pipeline/test_tag_table_source_info.py`.

---

### O4 — `GroupBy` member order is undefined for duplicate tag tuples
**Status:** open
**Severity:** medium

`GroupBy` sorts a group's members by their non-group-key tag values so the emitted lists are
stable across runs — orcapod hashes those lists to build the cache key, so an unstable order
causes spurious recomputes.

When `by` covers *every* tag column there is no non-key tag left to sort on, and the operator
falls back to the system `record_id`. That is a UUID v5 over the source id and a per-row
provenance token, so it encodes source *row position*. Permuting the source table therefore
changes member order, changes the list hash, and triggers a recompute. Measured: 79 distinct
member orders across all 120 permutations of a 5-row duplicate-tag input.

The branch is reachable with more than one member only when the input contains **duplicate tag
tuples** — `sort_columns` is empty exactly when the group key is the full tag tuple, so a
multi-member group requires duplicates.

This is not fixable inside `GroupBy`. The only remaining ordering signal is data content, and
the operator / function pod boundary forbids an operator from inspecting data.

The real root cause is upstream: `DuplicateTagError` (`errors.py:23`) is defined but **never
raised anywhere** in `src/` or `tests/`, so duplicate tag tuples are not prevented in the first
place. Tag uniqueness is assumed throughout the operator layer and enforced nowhere.

Fix: enforce tag uniqueness at stream construction (raising `DuplicateTagError`), which makes
this branch unreachable with more than one member.

---
### O5 — A reduction persists a result computed from an incomplete member set
**Status:** open
**Severity:** high

When an upstream pod's output changes, the first `job.run()` afterwards emits only the rows
that were recomputed in that pass. For a per-row pod that emission is *complete* — each row is
independent, so processing just the changed one is correct. For a many→one operator it is
*partial*: the group is reduced over only the members present in that pass, the reducing pod
executes, and its result is persisted with nothing marking it incomplete. The next run emits
the full set and the group is recomputed correctly.

Measured with two upstream pod stages (`source → sync_like → stringify → [GroupBy] → pod`),
one member of one group changed, same Delta store, fresh objects per run:

```
CONTROL (per-row, no GroupBy)
  changed run1   sync_like(99), stringify(sync_99), pod(sync_99.parquet)   <- converged
  changed run2   []

GROUPED
  changed run1   sync_like(99), stringify(sync_99), GROUP ['sync_99']      <- ONE member
  changed run2   GROUP ['sync_99', 'sync_1']                              <- correct
  changed run3   []
```

The control converges in a single run; only the grouped path needs a second one, and it
executes on an incomplete set first.

Consequence: a reducing pod with a side effect writes a complete-looking artifact from partial
input. For the motivating consumer (`common_clock_op`) an intermediate run can produce an
`alignment.json` built from one of a session's probes. It is replaced on the next run, so a
driver that runs to convergence never observes it — but a consumer reading between runs gets a
wrong answer with no signal.

`Batch` behaves identically, so this is pre-existing pipeline semantics surfaced by reduction
rather than something `GroupBy` introduced. It is more consequential for `GroupBy`, because
`GroupBy` exists specifically so a pod can reason over a *complete* group, whereas nobody
derives a result from `Batch` membership.

Not fixable inside the operator: an operator sees whatever its upstream emits and has no way to
know whether a group is complete. A fix needs a completeness signal at the node level — either
the upstream emitting its full cached set on every pass, or a reducing node deferring execution
until its inputs are known settled.

History: first reported as a cache-corruption bug (wrong group recomputing, tag/data
misalignment), then retracted as a propagation lag "identical with and without `GroupBy`". Both
framings are wrong. There is no tag/data misalignment and no cache defect — but the control
above shows the behaviour is *not* identical, because a partial emission is harmless per-row
and harmful for a reduction.

---

### O6 — `MapTags` / `MapData` with `drop_unmapped=True` predict a schema their output does not have when no key matches
**Status:** open
**Severity:** medium
**Issue:** ITL-820

When no key of `name_map` names a column of the stream, `unary_static_process` returns the
input stream unchanged (`core/operators/mappers.py:51-53` for `MapData`, `196-198` for
`MapTags`), ignoring `drop_unmapped`. The predicted output schema does apply `drop_unmapped`
and drops every data column (`MapData`) or every tag column (`MapTags`). So the schema and the
rows disagree:

```python
s = DictSource(data=[{"a": "x", "b": "y", "v": 1, "w": 2}], tag_columns=["a", "b"])
out = MapTags(name_map={"zz": "q"}, drop_unmapped=True)(s)
out.keys()                  # ((), ('v', 'w'))
out.as_table().column_names # ['a', 'b', 'v', 'w']
# MapData(name_map={"zz": "q"}, drop_unmapped=True): keys() == (('a', 'b'), ()), same table
```

Found by CodeRabbit on PR #271 (docs). The concepts page does not document this case until the
intended behaviour is decided: drop every unmapped column (what the flag says) or be a no-op.
A fix needs a regression test comparing `output_schema()` with
`unary_static_process(stream).output_schema()`.

---

## `src/orcapod/core/` — AddResult pod and Pod Groups

### G1 — `AddResult`: a first-class pod type for data enrichment
**Status:** open
**Severity:** medium

The most common pipeline pattern is *enrichment*: run a function on a data and append the
computed columns to the original data rather than replacing it. This is logically equivalent
to `FunctionPod → Join(original, computed)`, but implementing it as a first-class pod type is
both simpler and more efficient.

#### Why a dedicated pod type, not a composite

A naïve decomposition into `FunctionPod + Join` works but has unnecessary overhead:

1. **Materialization waste** — FunctionPod produces an intermediate stream that is only created
   to be immediately joined back. AddResult can compute new columns and merge them into the
   original data in a single pass, with no intermediate stream.
2. **Redundant tag matching** — Join must re-match tags that trivially correspond (they came
   from the same input row). AddResult already holds the (tag, data) pair and can skip the
   matching entirely.
3. **Simpler async path** — streams row-by-row like FunctionPod: read (tag, data), call
   the data function, merge original data columns + new columns, emit. No broadcast,
   passthrough channel, or rejoin wiring needed.

#### Provenance model: fused implementation, not a third category

The pipeline's provenance guarantees rest on a clean two-category model:

| Category | Produces new data? | Provenance role |
|---|---|---|
| **Source / FunctionPod** | Yes | Provenance tracked — new values are attributed |
| **Operator** | No | Provenance transparent — every output value traces to a Source or FunctionPod |

This is powerful because provenance tracking only happens at Source and FunctionPod boundaries.
Operators are "free" — they restructure but never create values that need attribution.

**AddResult does not introduce a third provenance category.** It is a *fused implementation*
of a pattern fully expressible in the existing model (`FunctionPod + Join`). Its provenance
semantics are *derived from* the decomposition, not an extension of the model:

- **Preserved columns** — passed through from upstream, provenance transparent (operator-like).
  Source-info columns pass through unchanged, exactly as Join would propagate them.
- **Computed columns** — produced by the wrapped DataFunction, provenance tracked
  (function-pod-like). Source-info columns reference the DataFunction, exactly as
  FunctionPod would attribute them.

There is no third kind of output column. Every column in an AddResult output has a clear
provenance story that maps directly to an existing category. The fusion is an optimization —
analogous to a database query optimizer fusing filter+project into a single scan without
changing relational algebra semantics.

This means the theoretical model stays clean (Source, Operator, FunctionPod), and AddResult
is justified as a performance/ergonomic optimization whose correctness can be verified by
checking equivalence with its decomposition.

#### API sketch

```python
# Sync
grade_pf = PythonDataFunction(compute_letter_grade, output_keys="letter_grade")
enriched = AddResult(grade_pf).process(stream)
# enriched has all original columns + "letter_grade"

# Async (streaming, row-by-row)
await AddResult(grade_pf).async_execute([input_ch], output_ch)
```

#### Implementation notes

- `output_schema()` returns `(input_tag_schema, input_data_schema | function_output_schema)`
  — the union of original data columns and new computed columns.
- Must raise `InputValidationError` if function output keys collide with existing data
  column names (same constraint as Join on overlapping data columns).
- `pipeline_hash` should behave as if the decomposition were performed — commits to the
  wrapped `DataFunction`'s identity plus the upstream's pipeline hash.
- Source-info on computed columns references the DataFunction (as FunctionPod would).
  Source-info on preserved columns passes through unchanged (as Join would).
- `async_execute` can use the same semaphore-based concurrency control as `FunctionPod`.

---

## `src/orcapod/hashing/semantic_hashing/`

### H1 — Semantic hasher does not support PEP 604 union types (`int | None`)
**Status:** open
**Severity:** medium

The `BaseSemanticHasher` raises `BeartypeDoorNonpepException` when hashing a
`PythonDataFunction` whose return type uses PEP 604 syntax (`int | None`).
The hasher's `_handle_unknown` path receives `types.UnionType` (the Python 3.10+ type for
`X | Y` expressions) and has no registered handler for it.

`typing.Optional[int]` also fails (different error path through beartype).

This means data functions cannot use union return types — a common pattern for functions
that may filter data by returning `None`.

**Workaround:** Use non-union return types and raise/return sentinel values instead.

**Fix needed:** Register a `TypeHandlerProtocol` for `types.UnionType` (and
`typing.Union`/`typing.Optional`) in the semantic hasher's type handler registry.

---

### G2 — Pod Group abstraction for other composite pod patterns
**Status:** open
**Severity:** low

Beyond AddResult (which warrants its own pod type — see G1), other composite patterns may
benefit from a general **PodGroup** abstraction that encapsulates a reusable sub-graph behind
a single pod-like interface.

A PodGroup:
- Accepts one or more input streams and produces one output stream (same interface as a pod)
- Internally contains a fixed sub-graph of pods, operators, and channels
- Hides the internal wiring from the user
- Participates in pipeline hashing as a single composite element

Potential patterns (to be designed as needs arise):
- **ConditionalPod** — route data to different pods based on a predicate, merge results
- **FanOutFanIn** — broadcast to N pods, collect and merge/concat results
- **FallbackPod** — try primary pod, fall back to secondary on error/None result

---

## `src/orcapod/databases/delta_lake_databases.py`

### D1 — `flush()` swallows individual batch write errors
**Status:** open
**Severity:** critical

`flush()` (line ~817) iterates over all pending batches, logs errors individually, but never
raises. Callers have no way to know that writes partially or fully failed:
```python
# TODO: capture and re-raise exceptions at the end
```

This is silent data loss — a batch write can fail and the caller proceeds as if everything
was persisted.

Fix: accumulate exceptions during the loop; raise an `ExceptionGroup` (or custom aggregate
error) at the end containing all failures.

---

### D2 — `flush_batch()` uses `mode="overwrite"` for new table creation
**Status:** open
**Severity:** high

`flush_batch()` (line ~856) creates new Delta tables with `mode="overwrite"`:
```python
# TODO: reconsider mode="overwrite" here
```

If a table already exists at that path (race condition, stale state, or misconfigured pipeline
path), existing data is silently destroyed. Should use `mode="error"` to fail fast, or
`mode="append"` with an explicit existence check.

---

### D3 — `_refresh_existing_ids_cache()` loads entire Delta table into memory
**Status:** open
**Severity:** high

The method (line ~252) calls `to_pyarrow_table()` on the full Delta table just to extract the
ID column:
```python
# TODO: replace this with more targetted loading of only the target column and in batches
```

For large tables, this is a critical memory bottleneck. Delta Lake supports column projection
(`columns=[id_col]`) and batch reading, which would reduce memory usage by orders of magnitude.

---

### D4 — `_refresh_existing_ids_cache()` catches missing column reactively
**Status:** open
**Severity:** high

In the same method (line ~257), if the ID column doesn't exist, a `KeyError` is caught as a
fallback:
```python
# TODO: replace this with proper checking of the table schema first!
```

Schema should be validated proactively by loading schema metadata before attempting to read
the column.

---

### D5 — `_handle_schema_compatibility()` uses naive equality and broad exception catching
**Status:** open
**Severity:** medium

The method (lines ~467, ~485) compares schemas with simple equality and catches all exceptions:
```python
# TODO: perform more careful check
# TODO: perform more careful error check
```

Should implement nuanced schema comparison (e.g., field-order invariance, nullable vs.
non-nullable promotion) and catch specific exceptions rather than bare `except`.

---

### D6 — `defaultdict` used for `_cache_dirty` is not serializable
**Status:** open
**Severity:** medium

`__init__()` (line ~69) initializes `_cache_dirty` as `defaultdict(bool)`:
```python
# TODO: reconsider this approach as this is NOT serializable
```

`defaultdict` is not pickle-serializable, which blocks multiprocessing or serialization
use cases. Fix: use a regular dict with `.get(key, False)`.

---

### D7 — Silent deduplication in `_deduplicate_within_table()`
**Status:** open
**Severity:** medium

The method (line ~383) silently drops duplicate rows with no warning:
```python
# TODO: consider erroring out if duplicates are found
```

Duplicates may indicate an upstream bug. Should at least log a warning; consider making
behavior configurable (warn, error, or silent).

---

### D8 — Cache lookups crash on `string_view` columns (missing PyArrow comparison kernels)
**Status:** in progress
**Severity:** high
**Issue:** ENG-601

`get_records_with_column_value()` pushes a filter predicate into a PyArrow scan
(`_read_delta_table`, line ~852: `dataset.to_table(filter=filter_expr)`). When a stored
string column is physically typed `string_view`, the scan crashes — PyArrow (verified on
both 23.0.1 and 24.0.0) has no comparison kernels (`equal` / `greater_equal` / `less_equal`)
for `string_view`:
```
pyarrow.lib.ArrowNotImplementedError: Function 'greater_equal'
  has no kernel matching input types (string_view, string_view)
```
This breaks every `ResultCache.lookup()`, so any rerun that hits already-stored records fails
(discovered in the spike-sorting pipeline: probes that succeeded on a first run failed on
rerun). `string_view` enters via `polars.DataFrame.to_arrow()`, which returns `large_string`
by default in polars 1.41.2 but `string_view` at `compat_level=newest` (newer polars makes it
the default). orcapod calls plain `df.to_arrow()` across the write path and never normalizes
`string_view` → `large_string` before persisting: `normalize_to_large_types` has no
`string_view` branch and is not applied on the write path. `as_large_types=True` only relabels
the advertised schema; the scanner still feeds `string_view` data to the predicate. No test
exercises `string_view`.

**Fix:** (read-side, primary) cast `string_view` → `large_string` after read / before
filtering and sorting in `_read_delta_table` — repairs already-persisted data and is agnostic
to the producer; also protects the `sort_by` / `take` path in `ResultCache.lookup`.
(write-side, hardening) add a `string_view` branch to `normalize_to_large_types` and normalize
before persisting so stored schemas stay canonical. Regression test: a Delta table whose filter
column is physically `string_view`, asserting `get_records_with_column_value` succeeds.

---

## `src/orcapod/hashing/`

### H1 — `FunctionSignatureExtractor` ignores `input_types` and `output_types` parameters
**Status:** open
**Severity:** critical

`extract_function_info()` (`function_info_extractors.py:36`) accepts `input_typespec` and
`output_typespec` but never incorporates them into the extracted signature string:
```python
# FIXME: Fix this implementation!!
# BUG: Currently this is not using the input_types and output_types parameters
```

The extracted signature is therefore type-agnostic — two functions with identical names but
different type annotations produce the same hash. This is a correctness bug that can cause
cache collisions between type-overloaded functions.

Fix: wire the type specs into the signature string; update tests.

---

### H2 — Arrow hasher processes full table at once
**Status:** open
**Severity:** medium

`SemanticArrowHasher._process_table_columns()` (`arrow_hashers.py:104`) calls `to_pylist()`
on the entire table, loading all rows into Python memory:
```python
# TODO: Process in batchwise/chunk-wise fashion for memory efficiency
```

For large tables, this is a significant memory bottleneck. Should use Arrow's `to_batches()`
for chunk-wise processing.

---

### H3 — Visitor pattern does not traverse map types
**Status:** open
**Severity:** medium

`visit_map()` in `visitors.py:225` is a pass-through that does not recurse into map
keys/values:
```python
TODO: Implement proper map traversal if needed for semantic types in keys/values.
```

Semantic types nested inside map columns will not be processed during hashing, leading to
incorrect or incomplete hash values.

---

### H4 — Legacy backwards-compatible exports in `hashing/__init__.py`
**Status:** open
**Severity:** low

The module (line ~141) re-exports old API names for backwards compatibility:
```python
# TODO: remove legacy section
```

Should be removed in the next breaking release. Consider adding deprecation warnings first.

---

### H5 — `_process_table_columns` materializes extension columns to Python even with no handler
**Status:** open
**Severity:** medium
**Issue:** ITL-433

`StarfixArrowHasher._process_table_columns` calls `to_pylist()` on every extension-typed
column before running `SemanticHashingVisitor`. For columns whose Python type has no registered
semantic handler (pydantic models, dataclasses, any unhandled extension type), the visitor
returns the value unchanged and the data is immediately re-serialized back to Arrow. The Python
roundtrip serves no purpose in this case and is O(rows) deserialization work wasted.

The fix is to short-circuit at the column level: before calling `to_pylist()`, check whether
`type_handler_registry.has_handler(python_type)` would be True for this column's extension
type. If not, call `normalize_extension_columns()` directly on the Arrow column (uses
`ExtensionArray.storage`, no Python materialization) and skip the visitor loop entirely.
Columns with a registered handler (e.g. `Path`) continue through the existing path unchanged.

The `normalize_extension_columns` utility landed in ITL-432.

---

## `src/orcapod/utils/`

### U1 — Source-info column type hard-coded to `large_string`
**Status:** resolved (`tag_data.py` half), open (`arrow_utils.py` half)
**Severity:** critical

Two sibling call sites assume source-info values are always scalar strings.

**`Data._ensure_source_info_table()` (`core/datagrams/tag_data.py:330`)** builds the Arrow schema
as `pa.field(k, pa.large_string())` for every key, and `Data.schema()` (line ~384) reports `str`
for every `_source_*` column. Any operator that produces list-valued source info therefore fails
inside `job.run()`:

```
pyarrow.lib.ArrowTypeError: Expected bytes, got a 'list' object
  core/datagrams/tag_data.py:342  _ensure_source_info_table
```

Two operators hit this: `MergeJoin`, which carries source columns along as parallel lists when
merging colliding data columns (`merge_join.py:262`), and `Batch`, which list-wraps every column.
Both were reproduced against `966d759a`.

**Fix:** landed in NPIPE-204 (`1b570e68`, `1ed22ee4`). `_source_info_arrow_type` and
`_source_info_python_type` in `core/datagrams/tag_data.py` derive the Arrow and Python types
from the stored value — `str`/`None` → `large_string`, list → `large_list(<elem>)`
recursively. The `SourceInfoValue` alias lives in `types.py` next to `TagValue`/`DataValue`,
which lets `DataProtocol` and `ArrowTableStream` reference it without inverting the
`protocols/` → `core/` layering. No pipeline-DB schema bump: a node's source-column type is
fixed by its own output schema, so existing nodes keep `large_string`. Job-level regression
coverage is in `tests/test_pipeline/test_aggregation_job.py`. See
`superpowers/specs/2026-08-07-npipe-204-batch-group-by-design.md`.

A third site, `polars_data_utils.add_source_info` (line 119), forces `dtype=pl.String()`. It is
dead code — nothing in `src/` calls it, and the tests importing `add_source_info` import it from
`arrow_utils` — and it carries a latent shadowing bug where `source_column` is rebound to a
`pl.Series` inside the per-column loop. NPIPE-204 deletes it rather than fixing it.

**Still open:** in `add_source_info_to_table()` (`arrow_utils.py:604`), when source info is a
collection it is unconditionally cast to `pa.list_(pa.large_string())`:
```python
# TODO: this won't work other data types!!!
```
Any non-string collection values will fail or silently corrupt data. The logic also has an
unclear nested isinstance check (line ~602: `# TODO: clean up the logic here`).

Fix: inspect collection element types to select the appropriate Arrow type; refactor the
conditional logic.

---

### U2 — Bare `except` in `get_git_info()`
**Status:** open
**Severity:** high

`get_git_info()` (`git_utils.py:55`) catches all exceptions including `KeyboardInterrupt` and
`SystemExit`:
```python
except:  # TODO: specify exception
```

Fix: catch `(OSError, subprocess.SubprocessError, FileNotFoundError)` specifically.

---

### U3 — `check_arrow_schema_compatibility()` lacks strict mode and type coercion
**Status:** open
**Severity:** high

The function (`arrow_utils.py:433`, `462`) documents strict vs. non-strict behavior but only
partially implements the non-strict path:
```python
# TODO: add strict comparison
# TODO: if not strict, allow type coercion
```

Currently, the function always raises on type mismatch instead of coercing compatible types
when in non-strict mode. Users cannot choose between strict field-order checking and permissive
type promotion.

---

### U4 — `is_subhint` does not handle type invariance
**Status:** open
**Severity:** high

`check_schema_compatibility()` (`schema_utils.py:37`) uses beartype's `is_subhint` which
treats all generics as covariant:
```python
# TODO: is_subhint does not handle invariance properly
```

For mutable containers (`list[int]` vs `list[float]`), this produces false positives —
schemas are reported as compatible when they are not (e.g., a `list[int]` field is accepted
where `list[float]` is expected, but appending a float to `list[int]` would fail at runtime).

Options: add an invariance-aware wrapper, switch to a stricter type comparison, or document
the limitation prominently.

---

## `src/orcapod/contexts/registry.py`

### C1 — Redundant manual validation duplicates JSON Schema checks
**Status:** open
**Severity:** medium

`_load_spec_file()` (line ~141) performs manual required-field checking followed by JSON Schema
validation:
```python
# TODO: clean this up -- sounds redundant to the validation performed by schema check
```

The manual check is fully subsumed by the JSON Schema validation. Either remove the manual
checks, or make the JSON Schema validation optional and keep manual checks as the fallback.

---

## `src/orcapod/core/tracker.py`

### T1 — `SourceNode.identity_structure()` assumes root source
**Status:** open
**Severity:** medium

The method (line ~163) delegates directly to `stream.identity_structure()`:
```python
# TODO: revisit this logic for case where stream is not a root source
```

For derived sources (e.g., `DerivedSource`), the stream may not have a meaningful
`identity_structure()`. Needs an isinstance check or protocol-based dispatch.

---

## `src/orcapod/core/cached_function_pod.py` / `src/orcapod/core/result_cache.py`

### RC1 — `ResultCache.record_path` not overridable via public API
**Status:** open
**Severity:** medium

When loading a pipeline with a `DataFunctionProxy` (PLT-931), the
`CachedFunctionPod`'s `record_path` may differ from the original because the
pod-level URI is recomputed from a deserialized schema (whose semantic hash
may diverge from the original due to the `Python type → Arrow string → Python type`
round-trip). The workaround reaches into private fields:
`node._cached_function_pod._cache._record_path = stored_path`.

**Fix:** Add a public `override_record_path(path)` method to `CachedFunctionPod`
(or `ResultCache`) that validates and sets the record path. Alternatively, accept
an optional `record_path_override` in `CachedFunctionPod.__init__` that takes
precedence over the computed path when provided.

---

## `src/orcapod/core/nodes/` — Config and context delegation chain

### T2 — `orcapod_config` not on any protocol; delegation chain needs review
**Status:** open
**Severity:** medium

Nodes (`SourceNode`, `FunctionNode`, `OperatorNode`) now delegate `data_context` to their
wrapped entity via property overrides, ensuring transparent context pass-through. However,
`orcapod_config` is only on `DataContextMixin` (concrete base), not on any protocol
(`StreamProtocol`, `PodProtocol`, etc.).

Open questions:
1. Should `orcapod_config` be added to a protocol (e.g. `TraceableProtocol`)?
   Adding it couples protocol consumers to the `Config` type. Leaving it off means
   nodes can't transparently delegate config the same way they delegate context.
2. Should `Pipeline` optionally hold `data_context` and/or `config` to allow
   pipeline-level overrides that propagate to all nodes during `compile()`?
3. The current chain is: Pipeline (no context) → Node (delegates to wrapped entity) →
   wrapped entity (owns context). Should there be a way for Pipeline to inject a
   context override?

---

## `src/orcapod/logical_types/`

### ET1 — `make_polars_extension_type` cannot accept a storage type containing nested extension types
**Status:** open
**Severity:** medium

`make_polars_extension_type` computes the Polars storage dtype by calling:
```python
pl.from_arrow(pa.array([], type=arrow_storage_type)).dtype
```
This fails with `ArrowNotImplementedError: extension` when `arrow_storage_type` is a struct
(or list) whose fields include any `pa.ExtensionType` node — for example, a dataclass whose
fields include `uuid.UUID` (stored as `orcapod.uuid` extension over `pa.large_binary()`).

Polars's Arrow IPC bridge handles top-level extension types via `pl.BaseExtension`, but has no
path for extension types *nested inside* a struct at dtype-inference time.

**Workaround:** `register_python_class` and `register_storage_type` both uphold a
*storage-safe* invariant: the returned type may be a `pa.ExtensionType` at the top level,
but struct fields and list value types at any depth are always plain (non-extension) types.
`DataclassLogicalTypeFactory.create_for_python_type` strips the top-level extension type
with a one-liner (`if isinstance(arrow_type, pa.ExtensionType): arrow_type = arrow_type.storage_type`)
before inserting it into the struct, so the struct passed to `make_polars_extension_type`
and `pa.Table.from_pylist` never contains nested extension types. The private
`_strip_ext_to_storage` recursive helper was removed in PLT-1720; the stripping is now
trivially correct because the storage-safe invariant guarantees `.storage_type` is always
already clean.

**Also affects `pa.Table.from_pylist`:** the same restriction applies to PyArrow's
`pa.Table.from_pylist` (and `pa.array`) — neither can build an array from a struct type
whose fields are `pa.ExtensionType` nodes, for the same underlying reason. The stripping
in `create_for_python_type` fixes both issues simultaneously.

**Polars round-trip fidelity:** once the storage struct contains only plain types (no
nested extension types), the full Arrow → Polars → Arrow round-trip for the *outermost*
extension type is faithful: extension name, metadata bytes, and storage struct are all
preserved. Only the inner field schema (already stripped) is absent.

**Fix needed:** Once PyArrow (and Polars) support nested extension types natively in struct
construction and Arrow↔Polars conversion, the stripping one-liner in `create_for_python_type`
can be removed and `make_polars_extension_type` can accept extension-typed storage directly.
Track upstream PyArrow / Polars issues.

### ET2 — Top-level `list[T]` / `dict[K, V]` columns lose extension-type schema metadata when `T`/`V` is a logical type
**Status:** in progress
**Severity:** medium
**Issue:** PLT-1732

When a logical type (e.g. `UUID`, a dataclass) appears as the element type of a `list[T]`
or `dict[K, V]` annotation, `register_python_class` now raises `ValueError` at
schema-construction time rather than silently stripping the extension type. The underlying
cause is that PyArrow does not allow extension types inside list value fields or struct
fields (ET1): `pa.array([], type=pa.large_list(extension_type))` raises
`ArrowNotImplementedError: extension`. If a caller manually strips to storage types and
writes `large_list(large_binary)` for `list[UUID]`, the stored Arrow schema carries no
`orcapod.uuid` marker; on a fresh read `register_storage_type` finds nothing to register,
and value conversion with `storage_to_python(..., list[UUID])` fails unless `UUID` was
registered manually beforehand.

**This does NOT affect logical types that are fields of a registered outer dataclass.**
Those are discovered and registered transitively: `register_discovered_extensions` finds
the outer dataclass extension type → `reconstruct_from_arrow` → `register_python_class`
per field annotation → inner type registered. The limitation applies only when the
outermost container (`list[T]`, `dict[K, V]`) is the top-level column type with no outer
dataclass wrapper.

**Empirically confirmed** (2026-06-17): `pa.array([], type=pa.large_list(extension_type))`
raises `ArrowNotImplementedError: extension` — identical to the ET1 struct-field
restriction. The `replace_logical_type` flag approach (preserving extension type inside
list value field) is therefore infeasible at the PyArrow level.

**Current behaviour:** `register_python_class(list[T])` raises `ValueError` when `T`
resolves to a logical type, pointing to this entry and PLT-1732. Use a direct `T` column
(no list wrapper) or wrap the list inside a dataclass field — the outer dataclass extension
type carries the annotation into the schema, and `reconstruct_from_arrow` re-registers `T`
transitively on read.

**Operator layer (NPIPE-204):** `Batch` and `GroupBy` previously called
`pa.list_(field.type)` directly, so they raised `ArrowNotImplementedError: extension` on any
extension-typed column even after ITL-173 landed `ListLogicalType`. Both now build their
output through `arrow_utils.build_aggregated_table`, which constructs the list over the
element's storage type and wraps it in the outer `list[<element>]` extension type. A pod
annotated `-> Path` groups into `list[orcapod.path]` and the downstream pod receives real
`Path` objects.

**Planned fix (PLT-1732, target v0.2):** Introduce `ListLogicalType` /
`ListLogicalTypeFactory` and `StructLogicalType` / `StructLogicalTypeFactory`. A
`list[UUID]` top-level column would be wrapped as a new extension type
`orcapod.list[orcapod.uuid]` with storage `large_list(large_binary)`. The extension type
sits at the outermost (list) level, not inside the list value field, so it satisfies ET1.
`register_storage_type` would dispatch to the new factory on read, auto-registering the
element type. See PLT-1732 for full design.

**Partial fix (ITL-173):** Introduced `ListLogicalType` and `ListLogicalTypeFactory` in
`src/orcapod/extension_types/list_logical_type_factory.py`. The entire
`list[T]`/`set[T]` is wrapped as a top-level Arrow extension type with storage
`large_list(<T storage>)`, embedding element reconstruction metadata in the
field-level metadata JSON. Wired into `_register_python_class_impl` and
`_convert_python_to_arrow` via `_make_or_get_list_logical_type`. Registered in
`v0.1.json` under categories `"list"` and `"set"`. Resolves the `list[T]`/`set[T]`
case. `dict[K, V]` with extension-type keys or values remains unsupported (raises
``ValueError``); tracked in PLT-1732.

---

## `src/orcapod/databases/connector_arrow_database.py`

### CA1 — SQL connectors silently lose Arrow extension-type field metadata on round-trip
**Status:** in progress
**Severity:** high
**Issue:** PLT-1795

`SQLiteConnector` (and any `DBConnectorProtocol` implementation that maps Arrow → SQL types)
does not preserve `ARROW:extension:name` / `ARROW:extension:metadata` field metadata. When a
column whose Arrow type is a `pa.ExtensionType` (e.g. `orcapod.path`, `orcapod.uuid`, or any
dataclass extension type) is written via `ConnectorArrowDatabase.add_records()` and then read
back, the column is returned as the raw storage type (e.g. `large_string`, `large_binary`,
`struct`) with no extension marker. This makes SQL connector round-trips impossible and causes silent data-type loss.

**Interim fix (PLT-1659 / ITL-471):** `DBConnectorProtocol` now declares a `validate_records`
hook. `SQLiteConnector` and `PostgreSQLConnector` implement it to reject extension-typed columns
at write time, surfacing the issue immediately rather than on a confusing read. `SpiralDBConnector`
overrides it as a no-op because it preserves field metadata via the native KV store.
Two representations are rejected by the SQL implementations:
- In-memory extension types: `isinstance(field.type, pa.ExtensionType)`.
- Metadata-only columns: plain storage type whose field metadata contains
  `b"ARROW:extension:name"` (the representation produced when reading a Parquet/IPC file
  with an unregistered extension type).

**Full fix (PLT-1795, target v0.2):** Preserve extension-type metadata in the SQL schema via
a companion metadata table (one row per column: `table_name`, `column_name`,
`extension_name`, `extension_metadata`). On `create_table_if_not_exists`, write rows for any
extension-typed columns; on `iter_batches`, join the metadata table and reconstruct the
`pa.ExtensionType` for affected columns before returning the batch. Once implemented, the
connectors override `validate_records` to a no-op.

---

### CA2 — Extension-type detection logic duplicated across multiple connectors
**Status:** open
**Severity:** low

The `b"ARROW:extension:name"` detection logic (checking both `isinstance(field.type, pa.ExtensionType)`
and `field.metadata.get(b"ARROW:extension:name")`) is copied verbatim into `SQLiteConnector.validate_records`,
`PostgreSQLConnector.validate_records`, and `MockDBConnector` in the test files. The constant
`b"ARROW:extension:name"` also appears inline in `extension_types/schema_walker.py` and other places
without a single canonical definition.

**Fix:** Extract into a shared helper `_check_extension_fields(schema) -> list[tuple[str, str]]`
(and/or a module-level constant `_ARROW_EXT_NAME_KEY`) reachable from all connector implementations.
When SQLite/PostgreSQL implement metadata preservation (PLT-1795), they'll override `validate_records`
to a no-op — at that point the duplication goes away naturally. Until then, the helper would keep
the detection logic in one place.

---

## `src/orcapod/semantic_types/universal_converter.py`

### UC1 — `python_type_to_arrow_type` raised on `typing.Any` from empty-container inference
**Status:** resolved
**Severity:** medium
**Issue:** ENG-389

`_infer_list_type` and `_infer_dict_type` in `pydata_utils.py` return `list[Any]` /
`dict[Any, Any]` when all sampled containers are empty (no elements to inspect). Passing
these inferred types to `python_type_to_arrow_type` raised `ValueError: Unsupported
Python type: typing.Any`.

**Fix:** Added `Any: pa.null()` to `_PYTHON_TO_ARROW_MAP` (forward path) and an explicit
`pa.types.is_null → Any` check to `_convert_arrow_to_python` (reverse path). `pa.null()`
is Arrow's canonical "unknown/no-type" marker; empty containers have no elements to
validate, so the encoding is semantically correct. The now-unreachable `Any`-specific hint
in the error branch was removed.

---

### UC2 — Premature rejection of union-typed function inputs
**Status:** resolved
**Severity:** high
**Issue:** ITL-452

`ensure_types_registered_for_schemas` forwarded each schema annotation directly
to `register_python_class`, which rejects complex union types (``str | Path``)
because Arrow has no native union storage type. This caused ``FunctionPod``
construction to fail with a ``ValueError`` whenever a function declared a
union-typed input argument, even though union inputs are semantically valid
(they express that the pod accepts either concrete type, with the concrete
branch resolved at stream-binding time).

**Fix:** Modified `ensure_types_registered_for_schemas` to detect union
annotations and register each non-``None`` branch individually, leaving
``register_python_class`` unchanged (it correctly rejects unions when invoked
directly for explicit type conversion).

---

### UC3 — Function signature hash order-dependent for union-typed annotations
**Status:** resolved
**Severity:** high
**Issue:** ITL-453

``get_function_signature()`` in ``hash_utils.py`` used ``str(param)`` to build
the signature string. For union type annotations, ``str(param)`` calls
``inspect.formatannotation``, which falls through to ``repr(annotation)`` —
reflecting declaration order. Two semantically identical signatures like
``foo(x: str | Path)`` and ``foo(x: Path | str)`` therefore produced different
``_function_signature_hash`` values, silently breaking content addressability
for any pipeline that refactored union member order.

**Fix:** Added ``_is_union_annotation`` and ``_canonical_annotation_str``
helpers to ``hash_utils.py``. Union members are now sorted byte-wise by their
``inspect.formatannotation`` string before being embedded in the signature.
Applied the same fix to ``FunctionSignatureExtractor.extract_function_info``.
Also added ``eval_str=True`` to ``inspect.signature()`` so that string
annotations produced by ``from __future__ import annotations`` (PEP 563) are
resolved to live type objects before union detection. Non-union annotations are
byte-for-byte unchanged.

---

## `pyspiral` dependency (SpiralDB integration)

### SP1 — pyspiral 0.11.7 broke against t3.storage.dev header-signing enforcement change
**Status:** resolved
**Severity:** high
**Issue:** PLT-1773

Around 2026-06-15, SpiralDB's object-storage backend (`t3.storage.dev`) began
strictly enforcing that every header present in a presigned S3-style GET request
must appear in `X-Amz-SignedHeaders`. pyspiral 0.11.7's embedded Rust HTTP
client sent additional unsigned headers alongside the presigned URL, causing all
Vortex file reads to return:

```
AccessDenied: There were headers present in the request which were not signed
```

All three `TestSpiralDBConnectorIntegration` tests failed on every CI push from
2026-06-15 onward. The runner image version was identical between the last green
and first red run, confirming this was a server-side enforcement change rather
than a runner regression.

**Fix:** Upgraded pyspiral from `0.11.7` to `0.14.9`. The 0.14.x line rewrote
the HTTP/auth stack to use Python-level `httpx` instead of the embedded Rust
client, eliminating the unsigned-header problem. The `pyproject.toml` minimum
was bumped from `>=0.11.0` to `>=0.14.0` to document the effective floor.
No connector code changes were needed — the public API is fully compatible
across this version range.

**Update (ITL-926, November 11, 2026):** the floor is now `>=0.16.33`, and the lock pins
pyspiral 0.16.33 and substrait 0.30.0. pyspiral 0.14.9 did not use the login saved by the
0.16.33 `spiral login` CLI: it started a fresh device sign-in. Every call the connector makes
has the same signature in 0.16.33 (`Table.key_schema` now returns a Spiral `Schema`; only
`.names` is used). pyspiral appears frozen at 0.16.33; its successor `spiral-sdk` (same import
name `spiral`) has a new table model and reads keyed (legacy) tables only through a private,
windowed call, so moving to it is a separate issue.

**Ongoing:** pyspiral releases frequently. See PLT-1785 for the tracking issue
covering routine version bumps.

---

## `src/orcapod/core/nodes/operator_node.py`

### ON1 — OperatorJobNode has no v0→v1 schema migration for `NODE_CONTENT_HASH_COL`
**Status:** open
**Severity:** high
**Issue:** ITL-539

`OperatorJobNode` stores `NODE_CONTENT_HASH_COL` as `large_binary` (changed in ITL-539),
but unlike `FunctionJobNode` and `ResultCache`, it has no `_ensure_schema()` guard and no
v0→v1 migration utility. Any existing pipeline DB written by the old code that stored
`NODE_CONTENT_HASH_COL` as `large_string` will fail with an Arrow schema mismatch on the
next write attempt. This is accepted as a breaking change for a pre-v0.1.0 project; users
must drop and recreate the affected pipeline DB tables manually.

A proper migration utility (analogous to `migrate_pipeline_v0_to_v1`) should be implemented
before v0.1.0 ship.

---

## `src/orcapod/core/operators/merge_join.py`

### MJ1 — `MergeJoin` raises when an input stream has zero rows
**Status:** resolved
**Severity:** high
**Issue:** ITL-813

`MergeJoin.binary_static_process` built two columns with `pa.array(...)` and no explicit
type. `pa.array([])` is `null`-typed, so with a zero-row input:

1. The `_common` helper join key was `null` on the empty side and `int64` on the other, and
   Polars raised `SchemaError: datatypes of join keys don't match`.
2. With both sides empty (or once 1 is fixed) the merged list column of a colliding data
   column, and its source-info column, were `null`-typed, and schema reconstruction raised
   `ValueError: A null type field may not be non-nullable`.

`Join` had the same helper-key bug in 0.1.4; it was fixed on `main` as a side effect of
ITL-563 but without tests.

**Fix:** The helper key is typed `pa.int8()` (as in `Join`). Merged columns and their
source-info columns are built with an explicit type taken from the inputs (see MJ2), never
inferred from the values, so a zero-row result has exactly the types of a non-empty one and
the predicted schema; the two concatenate. `tests/test_core/operators/test_empty_inputs.py`
covers zero-row inputs for every operator, comparing the materialized schema with the
predicted `output_schema()`; `test_merge_join_arrow_types.py` covers zero-row left, right
and both inputs for every element type.

---

## `docs/` (user-facing documentation)

### DOC1 — Documentation samples and behavioural claims had drifted from the code
**Status:** resolved
**Severity:** high
**Issue:** PLT-4816 (related: ITL-807)

Nothing executed the code samples in the user-facing docs, so they drifted as the library
changed. Against `main` at `61d0f18e`, 24 of 45 samples failed, and several prose claims
described behaviour the code does not have: that a function pod picks the columns it needs
out of a wider stream (it rejects extra data columns — ITL-807), that editing a function's
body invalidates its cached results (only its name, output schema and major version do), a
database-backed `FunctionNode(..., pipeline_database=..., result_database=...)` API that no
longer exists, `_tag::` as the system tag column prefix (the columns are `_tag_source_id::` and
`_tag_record_id::`), and `op.enable_file_hash_caching` (the function is in `orcapod.contexts`).
`mkdocs build --strict` also failed, on stale docstring parameters.

**Fix:** Added `tests/test_docs/`, which runs every fenced `python` block in `README.md`,
`docs/index.md`, `docs/getting-started.md`, `docs/concepts/` and `docs/reference/` (except the
two PostgreSQL samples in `docs/concepts/file-hash-caching.md`, marked
`<!-- docs-test: skip -->`) and checks the printed output against the output shown in the
sample, and which builds the site with `mkdocs build --strict`. Fixed every failing sample and the wrong prose to match the code;
library behaviour was not changed. What the docs still do not cover is listed in
`superpowers/plans/2026-11-09-docs-gap-report.md`.

---

## `src/orcapod/core/streams/arrow_table_stream.py` — row iteration

### ST4 — `iter_data` drops and mis-pairs rows when tag and data columns are chunked differently
**Status:** resolved
**Severity:** critical
**Issue:** ITL-816

`ArrowTableStream.iter_data` called `to_batches()` separately on the tag-column selection and
the data-column selection of the backing table and `zip`ped the two batch lists.
`Table.to_batches()` cuts at the union of the chunk boundaries of the *selected* columns, so
the two lists differ whenever tag and data columns have different chunk layouts — which is
what the orchestrator produces when it rebuilds a node's output with
`hstack_tables(concat_tables(tags), concat_tables(data))`. `zip` truncated to the shorter
list (rows silently dropped: 6 rows from `as_table()`, 4 from `iter_data()`), and batches of
unequal length paired a tag with another row's data or raised
`Table must contain exactly one row`.

A second bug of the same family sat in the same loop: per-row source info was read with
`self._source_info_table.slice(i, 1)` where `i` is the index *within the current batch*, so
in any multi-batch table every row after the first batch received the source info of a row in
the first batch.

**Fix:** `iter_data` now batches the whole backing table once
(`self._table.to_batches()`, whose batches are aligned across all columns by construction),
selects the tag and data columns out of each batch, and indexes source info by absolute row
position. No table copy and no per-row `Table.slice`; measured no slower than before on a
100k-row table. Regression tests: `tests/test_core/streams/test_iter_data_chunk_layouts.py`.

---

## `pyproject.toml`

### DEP1 — Unused `pygraphviz` dependency breaks installs without Graphviz headers
**Status:** resolved
**Severity:** high
**Issue:** ITL-134

`pygraphviz>=1.14` was listed in `[project] dependencies` although nothing imports it
(pipeline graph rendering uses the pure-Python `graphviz` package). It ships only as an
sdist, so every `uv sync` compiled it against the system Graphviz headers; when
`ubuntu-latest` moved to ubuntu-26.04 its newer `libgraphviz-dev` broke the build and every
CI job failed.

**Fix:** Removed `pygraphviz` from `pyproject.toml` and `uv.lock`, and dropped
`libgraphviz-dev` from all workflows (the `graphviz` apt package stays for `dot`).
`tests/test_packaging.py` guards against re-adding it.

---

### DEP2 — Packaging declared MIT, shipped a type checker to users and a 3.3 MB sdist
**Status:** resolved
**Severity:** high
**Issue:** ITL-827

`pyproject.toml` declared `MIT License` (with the OSI MIT classifier) although Orcapod is
proprietary to Metamorphic Labs, and no LICENSE file existed. `basedpyright` sat in the
runtime dependencies, so every `pip install orcapod` pulled in a type checker. The sdist
packed the whole repository (`tests/`, `superpowers/`, `docs/`, `notebooks/`, `CLAUDE.md`,
...), and `orcapod.__version__` did not exist.

**Fix:** Added the org template LICENSE and declared
`license = { text = "LicenseRef-Metamorphic-Proprietary" }`, the identifier the org license
check allows (and added it to the repo's own `_license-check.yml` allow-list). No license
classifier is declared: pip-licenses prefers classifiers, so `License :: Other/Proprietary
License` would fail both checks. Moved `basedpyright` and `typing_extensions` (only a test
imports it) to the dev group, dropped the unused `pymongo` and `pyyaml`, restricted the sdist
with `only-include` (3.37 MB to 0.47 MB), and added `orcapod.__version__`.
`tests/test_packaging.py` builds both distributions and pins their contents.

**Update (November 10, 2026, ITL-875):** the license decision was reversed: Orcapod is MIT
licensed from v0.2.0 on, published on public PyPI as `orcapod`. `LICENSE` holds the MIT text
and `pyproject.toml` declares the PEP 639 expression `license = "MIT"` (metadata
`License-Expression: MIT`, no license classifier).

---

## `src/orcapod/core/operators/merge_join.py` — Arrow types

### MJ2 — `MergeJoin` discards its inputs' Arrow types
**Status:** resolved
**Severity:** high
**Issue:** ITL-832 (with ITL-813; related: ITL-835, ITL-839)

`MergeJoin` built a merged (colliding) column with `pa.array(<python lists>)`, so Arrow inferred
the element type from the values instead of keeping the input's: `int8`/`int16`/`int32`/`uint8`
became `int64`, `float32` became `double`, nested lists widened their elements, and decimal
precision came from the data (`decimal128(5, 2)` became `decimal128(3, 2)`). PR #268 (ITL-813)
made the empty path mimic that inference with a hand-written table. When the two inputs gave a
column different Arrow types, the result depended on the values and on argument order, or the
Polars join raised.

**Fix:** No Arrow type is inferred from Python values. Every output column of `MergeJoin` and
`PolarsFilter` takes the large-normalized form of its input type
(`arrow_utils.normalize_to_large_types`: `string` to `large_string`, `list` to `large_list`,
recursively; hash-identical), keeping width, nullability and logical types. Merged columns,
and the list columns of `Batch` and `GroupBy`, are built by `arrow_utils.aggregated_list_type`:
`large_list<normalized input element>`, or for a logical element its logical list type from the
type converter (an error naming the column if the converter has none; the element is never
stripped to its storage type). Merged source-info columns are `large_list<large_string>`. A
column whose two inputs' types differ goes through `MergeJoin._resolve_disagreeing_types`:
equal after normalization proceeds, anything else raises `InputValidationError` naming the
column and both types, in `binary_static_process` before the join (`output_schema()` reports
Python types and cannot see it). Hashes: `list`/`large_list` and the other layout pairs hash
identically, so only a merged column narrower than `int64`/`double` changes its row hash (it no
longer widens). Rules recorded under "Arrow types" in `CLAUDE.md`.

`normalize_to_large_types` also dropped a list item's non-null flag (and map/union child
flags), which changes the Arrow schema hash of such a column; it now rebuilds every nested
field with `Field.with_type`, keeping name, nullability and metadata, so normalization is
hash-preserving (`tests/test_utils/test_normalize_to_large_types.py`). Measured on
`list<int64 not null>`, `struct<l: list<int8 not null>>` and
`list<list<string not null> not null>` columns: `Data.content_hash` (the stored
`input_data_hash`), `Tag.content_hash`, stream and source `content_hash` / `pipeline_hash`, and
`Batch` output hashes are all unchanged; only the Arrow types now keep the nested non-null
flags (a stored-schema change for such columns). The wider nullability audit across operators
and pods is ITL-842.

**Still open:** non-empty struct columns cannot be merged because values are sorted as Python
objects (ITL-839; pinned by a strict xfail).

---

## System tag run stability (ITL-817)

### RS1 — Tag column order through `Join` / `MergeJoin` depended on `PYTHONHASHSEED`
**Status:** resolved
**Severity:** high
**Issue:** ITL-817

`Join.static_process` and `MergeJoin.binary_static_process` built `tag_columns` from a `set`,
so the order of user tag columns on every emitted `Tag` changed from process to process.
`FunctionJobNode._load_cached_entries` rebuilds cached rows with the compile-time tag order,
so a node output mixing cached and freshly computed rows could not be concatenated:
`SyncPipelineOrchestrator._materialize_as_stream` raised
`ArrowInvalid: Schema at index N was different`.

**Fix:** Column order follows the operator's argument symmetry. `Join` and `MergeJoin` are
commutative, so they list tag and data columns by first appearance across the inputs in
canonical `pipeline_hash` order: `Join(a, b)` and `Join(b, a)` give the same keys, schema,
table columns and per-row keys, in every process and on the async path. The order is defined
once, in `output_schema` / `binary_output_schema`; `static_process` reads it back and lays
the table out to match. `SemiJoin` is ordered and keeps the left operand's columns.
`arrow_utils.concat_tables_by_name` aligns per-row tables by column name in both buffer
materializers, so any operator that emits a different column order is tolerated. Column
order only: `arrow_hasher.hash_table` and the semantic hasher ignore column and key order,
so no record ID, system tag, `content_hash` or `pipeline_hash` changes.

### RS2 — Default source record IDs are positional
**Status:** open
**Severity:** high
**Issue:** ITL-817

Without `record_id_column`, `stream_builder._make_provenance_token` derives a row's record ID
from its position (`row_{index}`). When the same rows arrive in a different order under the
same `source_id`, a moved row gets a different record ID while its user tags and data stay
correct. Every identity built on the system tags of a moved row then changes, and because
those identities also include the hash of the row's own data, the row is treated as a new
entry: a `sink_pod` re-delivers it on every run (all rows when every row moves) and a
function node records a second entry with identical tag values and a distinct record ID. No
result is paired with the wrong row. `record_id_column` alone does not prevent this when `source_id` is left to
default, because the default `source_id` is the table hash, which also changes with row order. This reproduces the signature reported
in ITL-817 exactly; whether it is the reporter's cause depends on how their sources are built.

The behavior is now documented on `ArrowTableSource`, `SourceStreamBuilder.build` and in
`docs/concepts/sources.md`, and `record_id_column` is pinned as order-independent by
`tests/test_pipeline/test_system_tag_run_stability.py`. Changing the default to a
content-derived record ID invalidates stored identities and is proposed, not implemented, in
`superpowers/specs/2026-11-09-itl-817-content-derived-record-ids.md`.

---

## `src/orcapod/utils/canonical_type_names.py`

### PY313 — Type names in identity strings depended on the Python version
**Status:** resolved
**Severity:** high
**Issue:** ITL-825

Python 3.13 moved the `pathlib` classes into the private `pathlib._local` module, so
`pathlib.Path.__module__` is `pathlib._local` there and `pathlib` on 3.11/3.12. Every
identity string built from a type's `__module__`, or from `repr`/`str`/
`inspect.formatannotation` of a type, therefore differed by interpreter:
`canonical_annotation_str`, `get_function_signature` and `get_function_components`
(the `function_signature_hash` and `function_content_hash` stored with every cached result
of a `PythonDataFunction`), `TypeObjectHandler` for unregistered types such as `PurePath`,
the schema strings in `PythonDataFunction.to_config()` (a pipeline config saved on 3.12
failed to bind on 3.13), and the `ListSource` tag-function hash in `signature` mode. `uri`, `content_hash`, `pipeline_hash` and record ids were not
affected, because `pathlib.Path` resolves to the registered `orcapod.path` logical type.

**Fix:** `canonical_module_name` resolves a standard-library type defined in a private
submodule to the public module that exports the same object, and all the sites above go
through it. Private submodules that were already the defining module on 3.11/3.12
(`concurrent.futures._base`, `zipfile._path`, ...) are frozen in
`_LEGACY_PRIVATE_STDLIB_MODULES`, so no string or hash changes on those versions.
`tests/test_hashing/test_canonical_type_names.py` pins the names and golden hashes.

**Known remainder:** `zipfile.Path` is `zipfile.Path` on 3.11 and `zipfile._path.Path` on
3.12/3.13. This predates 3.13 and is left as is, because aligning it would change existing
strings on 3.11 or 3.12.

---

## Tooling

### TOOL1 — Type-check debt recorded in the basedpyright baseline
**Status:** open
**Severity:** medium
**Issue:** ITL-826

The org required CI runs `basedpyright` in its default (`recommended`) mode. The code was
never type-checked, so ITL-826 recorded the existing errors in
`.basedpyright/baseline.json` instead of fixing them: the check passes and any new error
fails it. Baselined errors after the ITL-826 lint fixes and the ITL-825 merge: 1995 in
total (tests/ 1238, src/ 669, test-objective/ 84, examples/ 3, superpowers/ 1); the baseline
also records 30,279 warnings, which are reported but do not fail the check
(`failOnWarnings = false`). Largest error rules: `reportArgumentType`,
`reportMissingTypeArgument`, `reportAttributeAccessIssue`.

Pay it down directory by directory, starting with `src/`, and regenerate the baseline only
to remove fixed entries. Known items worth fixing first: `orcapod.hashing` exports nine
names from a `legacy_core` module that no longer exists, so they are always `None`
(`tests/test_hashing/generate_pathset_data_hashes.py` would call one); and 26 import
cycles reported in `src/`.

Protocols that lag their concrete classes. These baselined errors are not runtime bugs (the
concrete classes have the method or parameter), but the protocols misdescribe what callers
rely on. Paths are under `src/orcapod/`:
- `FunctionPodProtocol.process_data` / `async_process_data` lack the `logger` / `run_id`
  keyword arguments that callers pass: `core/cached_function_pod.py:138,189`,
  `core/nodes/function_node.py:1429,1566`.
- `StreamProtocol` lacks `data_context`, which operators use:
  `core/operators/batch.py:99`, `core/operators/group_by.py:217`,
  `core/operators/index.py:98`, `core/operators/pick.py:104`,
  `core/operators/merge_join.py:252`.
- `ArrowDatabaseProtocol` lacks `list_sources` (`pipeline/logging_observer.py:247`,
  `pipeline/status_observer.py:199`) and `to_config(db_registry=)`
  (`pipeline/logging_observer.py:278`, `pipeline/status_observer.py:231`).
- `DataFunctionProtocol` lacks `data_context`:
  `core/cached_function_pod.py:145,150,196,201`.

---

## `src/orcapod/core/datagrams/datagram.py`, `src/orcapod/core/operators/index.py`, `pick.py`

### DG1 — Datagram mutators discard known Arrow types; Index/Pick re-infer every column
**Status:** resolved
**Severity:** high
**Issue:** ITL-834

`Datagram.with_columns` and `update` took the Arrow-native path only when the dict cache was
not loaded. `Index`/`Pick` read `data[column]` first, so they fell into the dict path, which
dropped the Arrow table and rebuilt every column from Python types: `int8 -> int64`,
`float32 -> double`, `list<int8> -> large_list<int64>`, changing the content hash of columns
the operators only pass through. The extracted column was typed from the Python element type
(always `int64` for ints), and the all-rows-skipped path built its empty table from the
predicted Python schema, widening tags too. Two sibling defects of the same class:
`copy(include_cache=False)`, used by every mutator, discarded the meta Arrow table so narrow
meta columns widened on any mutation; and dict-path `with_columns`/`select`/`drop`/`rename`
copied a stale cached `_data_arrow_schema`, so `arrow_schema()` disagreed with `as_table()`
(which let `FunctionPodStream.as_table` silently drop a column of a mutated dict-born row).

**Fix:** `with_columns`/`update` use the Arrow table whenever it is loaded and carry existing
fields verbatim; only genuinely new columns are typed (from `column_types`, which now also
accepts a `pa.DataType` used verbatim) or inferred. The meta Arrow table is carried through
`copy` and the meta mutators keep untouched meta fields. Dict-path mutators reset the cached
Arrow schema. `Index`/`Pick` derive the extracted type from the input column's Arrow type,
keep the in-place column's position, and build the empty result from the input's Arrow
schema. Rule: whatever Arrow types the inputs have are respected; inferring from Python
values is reserved for when no Arrow type is known.

The extracted column's nullability is the same for every row and equals the predicted output
type's: `column_types` also accepts a `pa.Field` (used verbatim, nullability included), and
`Index`/`Pick` declare `NOT NULL` because an element's predicted type is never Optional. An
extracted `None` raises `ValueError` naming the operator, column and row (before, a `None` in
one row and a value in another gave the rows different nullability, so materializing failed in
`pa.concat_tables`). Known limit: the type converter maps `list[int | None]` and
`list[int]` to the same Arrow type and back to `list[int]`, so nullable elements cannot be
declared today (ITL-835 territory).

---

## `src/orcapod/semantic_types/universal_converter.py` — struct Python types

### TC1 — A struct's Python type depended on its children's string/list layouts
**Status:** resolved
**Severity:** medium
**Issue:** ITL-832 (follow-up: ITL-841)

The type converter synthesized one `TypedDict` class per exact Arrow struct type, so
`struct<b: string>` and `struct<b: large_string>` (hash-identical) became two different Python
classes with the same name. A stream with a `struct<b: string>` column therefore reported one
class in `output_schema()` and another in its own `Data` rows (which normalize to large types),
and an operator that normalizes its output (ITL-832) could not match its predicted schema.

**Fix:** The converter keys the class by `normalize_to_large_types(struct)` and maps it back to
the large-normalized struct, so both layouts share one class; width, nullability and logical
types still distinguish structs. Content and pipeline hashes of struct columns are pinned to
their values on `main` in `tests/test_semantic_types/test_struct_layout_identity.py`. Maps and
unions do not have this problem: their Python types do not depend on child layouts. This is a
workaround (`TODO(ITL-841)` in the code); ITL-841 revisits the treatment of large variants
everywhere.

---

## `src/orcapod/core/operators/join.py` — async path

### JA1 — `Join`'s async path rebuilt rows from Python values
**Status:** resolved
**Severity:** high
**Issue:** ITL-837

`Join.async_execute` merged each matched pair by building `Tag(merged_dict)` and
`Data(merged_dict)` from `as_dict()` values (`_merge_pair_rename` for two inputs,
`_merge_pair_passthrough` and `_rename_sys_tags` for the staggered path of three or more), so
every column's Arrow type was inferred again: `int16` and `int8` became `int64`, nullability
was lost, a `dict` column could not be built, and the Tag/Data content hashes differed from the
sync path for the same rows. `Data.content_hash()` is the input hash a function node stores, so
cache hits did not carry over between sync and async runs.

The developer's standing rules, which the fix follows:

- Never infer an Arrow type from Python values when the input already has one; no bare
  `pa.array(values)` / `from_pylist` without a schema, no hand-written inference table. Where
  no Arrow type is known, use the data context's type converter.
- Arrow types are normalized to their large variants (`arrow_utils.normalize_to_large_types`).
  Width, nullability (top level and nested), struct fields and logical types never change.
  Anything else that disagrees is an error, never a promotion.
- A list of a logical type is the logical list of that logical type.
- Operators take the data context from the stream or the datagrams they are given, never from
  `self.data_context` or a hand-called `contexts.resolve_context`; operators carry no
  converter state.
- `Join` is commutative: async output columns follow the sync path's canonical order.

**Fix:** the async merge builds each row from the inputs' Arrow tables (`as_table()`): tag
columns in canonical order with a shared key taking the right input's field as the sync path
does, data columns stacked with `hstack_tables`, source info merged, system tags renamed as
before. The new `Tag`/`Data` take the left (canonically first) input datagram's
`data_context_key`. A shared tag key whose Arrow types still differ after large-normalization
raises `InputValidationError` on both paths; the sync path used to keep the right input's type
silently. The rule (moved out of `MergeJoin` so both operators share it) is
`arrow_utils.shared_arrow_type`, which takes only the two types and returns the shared large
type or `None`; `core/operators/base.require_shared_arrow_type` raises the error naming the
operator, the column and both types, for every `Join` and `MergeJoin` call site.
`Datagram.copy()` no longer turns a lazy Python schema into an empty one (it broke `schema()`
of async `Index`/`Pick` rows; ITL-859).
`tests/test_core/operators/test_async_sync_equivalence.py` runs every operator with its own
async path through both paths and compares types, nullability, column order, system tags,
content and schema hashes; differences that are not type re-inference are strict xfails:
ITL-838 (`Data.select`/`drop` keep dropped columns' source info), ITL-846 (sync `SemiJoin`
over nested columns; `ArrowHasher.hash_schema` on extension types). The ITL-845 (O3, row-0
source info) xfails were removed once that fix merged; those provenance cases now pass. The
developer's principle behind the shared-key error: Orcapod is type strict and performs no
implicit type expansion or conversion, except normalization to large variants.
Meta columns (ITL-854, ITL-857; developer decision): `Join` drops every input's `Data` meta
(`__`) columns, and the `_source___*` source-info columns of a raw `ArrowTableStream`'s `__`
columns, on both paths. The sync path used to keep them and rename collisions with an index
suffix, which could duplicate a name an input already had (ITL-857). The async path used to
drop them. Nothing downstream of a `Join` reads or stores them, and meta is not hashed.
`output_schema()` leaves the `__` names a raw stream lists as data out of the prediction, and
two inputs that share only a meta name join. How meta columns should be handled across
operators and pods is a separate design question (ITL-867). `MergeJoin` still carries input
meta on its sync path (colliding names are merged into lists like data) and drops it on its
barrier async path (ITL-846).

---

## `src/orcapod/core/streams/arrow_table_stream.py`, `src/orcapod/utils/arrow_utils.py` — nested source info

### SI1 — Aggregating an already aggregated stream raised `ArrowTypeError`
**Status:** resolved
**Severity:** high
**Issue:** ITL-849

`GroupBy` or `Batch` over a stream whose source-info columns were already list-valued (the
output of `MergeJoin`, `Batch` or `GroupBy`) raised `ArrowTypeError: Expected bytes, got a
'list' object` as soon as its rows were materialized (`as_table`, `iter_data`, any hash). The
aggregation itself was correct: `build_aggregated_table` typed `_source_x` as
`large_list<large_list<large_string>>`. The crash was downstream of it.
`ArrowTableStream.iter_data` passed each row's source info to `Data` as Python values, and
`prepare_prefixed_columns` typed every list token as `list<large_string>`, one level too
shallow. All five allowed chains failed (MergeJoin→GroupBy, MergeJoin→Batch, Batch→Batch,
GroupBy→Batch, GroupBy→GroupBy), sync and async. Batch→GroupBy is rejected by design
(`GroupBy` keys only on scalar tags).

**Fix:** `iter_data` hands each row's source info to `Data` as Arrow columns, batched together
with the tag and data columns so rows still pair up across chunk layouts (ITL-816), so the
stream's type is kept instead of re-inferred. `arrow_utils.source_info_arrow_type` (moved from
`tag_data`) is the one source-info type rule and types Python-supplied tokens at any depth:
each list level takes its element type from the first element that is not `None` or an empty
list, so `[[], [["a"]]]` is three levels deep. `Data`'s Python source-info type is read off it.
The result is a list of the incoming list type (`large_list<large_list<...>>`, element types
exactly the large-normalized inputs, `Path` as `list[list[orcapod.path]]`). The predicted schema
equals the materialized one. Hashes of pipelines that already worked are unchanged.
Regressions: `tests/test_core/operators/test_chained_aggregation.py`. Open: on the async
barrier path, per-row source info is still row 0's (ITL-845); that assertion is a strict
xfail.

---

## `tests/test_hashing/golden/` — hash stability across releases

### HS1 — No regression test pinned hash values, so a hash could change silently
**Status:** resolved
**Severity:** critical
**Issue:** ITL-626 (consolidates ITL-168)

Schema hashes name system-tag columns, pipeline hashes name database tables, and content and
data hashes key stored records. Any change to a hasher input (a type's canonical name, `Schema`
handling, the Arrow hasher, a record-id preimage) changed these values with no failing test, so
data written by one release could become unreadable or mis-keyed in the next. Between v0.1.4
and v0.2, for example, the semantic hashes of the `Path`, `UUID`, `DataFrame`, `list[Path]` and
`set[UUID]` type objects moved (ITL-638), which renamed the system-tag columns of every source
carrying those types. That change was noticed only by comparing releases.

**Fix:** `tests/test_hashing/golden/` pins about 600 values in committed JSON files, one
generator regenerates them, and every entry is its own test, so a failure names the value that
moved. Coverage: semantic hashes of scalars, collections, type objects and logical-type values;
`Schema` hashes and stored Arrow types for scalar, nested, nullable and logical types
(`Path`, `UUID`, `ndarray`, `DataFrame`, dataclass, pydantic, `list[Path]`, `set[UUID]`);
`compute_schema_hash`, `compute_source_schema_hash`, system-tag column names and
`SourceStreamBuilder` output; `StarfixArrowHasher` table and schema hashes (widths, layouts,
nullability, nesting, column and row order, extension columns); and end-to-end identities
(content and pipeline hashes, system-tag columns, record ids, per-row data hashes, database
table paths) of representative pipelines and a `PipelineJob`. Values are checked in
subprocesses with different `PYTHONHASHSEED` values and were verified identical on Python
3.11, 3.12 and 3.13. The generator's header says when regeneration is legitimate: a changed
value needs a migration assessment and the developer's approval. Not pinned yet: the
`StarfixArrowHasher` gaps tracked in ITL-848.

---

## `src/orcapod/pipeline/job.py`, orchestrators, node write paths — store write failures

### SW1 — A failed store write did not fail `PipelineJob.run()`
**Status:** resolved
**Severity:** critical
**Issue:** ITL-878

A node's write to the store ran inside the per-row handler meant for function failures:
`FunctionNode.execute` (sync) and `_FunctionPodBase.async_execute.process_one` (async) wrap
compute *and* the result-table and pipeline-table writes in one `except Exception`. With the
default `error_policy="continue"` a failed write was skipped like a crashed function: the sync
path logged a WARNING, the async path logged only at DEBUG, `job.run()` returned normally and
the node read back fewer rows (none when every write failed, e.g. a logical-type output on
`SQLiteConnector`, ITL-877). An operator LOG write failure did raise, but before `job.run()`'s
final flush, so the upstream function node's pending pipeline rows were never committed while
its result rows were; async callers got an `ExceptionGroup`.

**Fix:** Contract, for now (developer, November 10, 2026): a failed store write always fails
the run, whatever `error_policy` is. Every node write site (`ResultCache.store`,
`FunctionNode.add_pipeline_record`, `OperatorNode._store_output_stream`, the side-effect
invocation log) raises `orcapod.errors.StoreWriteError`, chained from the backend error and
naming the node, the row's tag and the table. The per-row handlers re-raise it, so the run
stops at the first failed write; `error_policy` still decides only about rows whose function
raised. `AsyncPipelineOrchestrator` unwraps it from the `TaskGroup` exception group, so both
orchestrators raise the same error. `PipelineJob.run()` flushes the writes that succeeded
before re-raising (a flush failure there is logged, not raised over the original), and wraps a
failure of its final flush. Nothing stored is rewritten; no hash, path or schema change. An
opt-in mode that tolerates store failures is a possible later option. A fail-loud post-run
hook or an observer callback that raises while the store error propagates is logged and noted
on it (`orcapod.utils.store_errors`), not raised in its place, so a node function that runs
another cached pod cannot turn that pod's store failure into a skipped row. When concurrent
node tasks fail together, the async orchestrator raises the store error of the node first in
topological order, notes the other failures on it and logs other store errors at WARNING.
A failed flush (the result cache's per-row auto-flush or the job's final flush) raises
`StoreWriteError(flush=True)` naming the store and no row: views of one store share their
pending writes, so the failing batch may be an earlier row's or another table's.
The rule (developer, November 11, 2026): store failures fail the run; a function output that
cannot be stored (a naive `datetime`, a value not matching the declared type, rejected while
`ResultCache.store` prepares the record, before any write) is a row failure governed by
`error_policy` exactly like a function that raised, not a `StoreWriteError`: the value is
specific to that row, the store is healthy, and every re-run would fail at the same row. The
async orchestrator logs row failures at DEBUG and ignores `fail_fast` (ITL-879), unchanged here.
Regressions: `tests/test_pipeline/test_store_write_failure.py`. Related, not fixed here: the async
orchestrator ignores `error_policy` (ITL-879); connector and Delta `flush` drop a batch whose
write fails (ITL-880).

---

## `src/orcapod/core/operators/semijoin.py` — nested columns and string/large_string keys

### SJ1 — `SemiJoin` failed on nested columns and on `string` vs `large_string` keys
**Status:** resolved
**Severity:** high
**Issue:** ITL-847, ITL-829 (first case only)

The sync path used pyarrow's Acero hash join (`join_type="left semi"`), which rejects any nested
column: `ArrowInvalid: Data type list<item: int8> is not supported in join non-key field`. A list,
large_list, struct or struct-backed logical column (a dataclass) on either input failed, as did a
struct or list key. Acero also rejected a key that is `string` on one side and `large_string` on
the other (a `CSVSource` against a `DictSource`, ITL-829). The async path built its lookup from
`as_dict()` values, so struct and list keys were unhashable, and a null key matched a null.

**Fix:** the sync path joins in Polars (`how="semi"`, `maintain_order="left"`) against the right
input's key columns only, then casts the result to the left table's schema, so every column keeps
the left input's exact Arrow type, top-level and nested nullability, and logical type. Before the
join each key goes through `require_shared_arrow_type` (Polars would silently unify `int8` with
`int16`); sync and async raise `InputValidationError` naming the column and both types. A null key
never matches: Polars already does that for scalar keys but treats a null struct or list as equal
to another, so right rows with a null key are dropped first. The async path checks key types on
the first row of each side and builds its lookup from each key's Arrow storage value, frozen into
a hashable form (nulls inside a struct or list compare equal, NaN equals NaN, as in Polars).
Results of inputs that already worked are unchanged (content and pipeline hashes, per-row hashes,
schemas, tables and row order, sync and async). Regressions:
`tests/test_core/operators/test_semijoin_nested.py`; the ITL-846 strict xfail in
`test_async_sync_equivalence.py` is removed, and the SemiJoin meta case now uses nested meta
columns. Open: ITL-829's second case (a non-nullable key against a nullable one, raised by
`schema_utils.intersection_schemas` during input validation) is unchanged and keeps its strict
xfail. The async path checks key types on the first row of each input, because
`async_execute` receives only channels and no stream exposes its Arrow schema without rows.
So when either input is empty (left or right), a key type mismatch that the sync path reports
yields an empty result on the async path instead of an error. `Join`'s async key check
(`_check_shared_key_types`) has the same limit.

---

## `src/orcapod/databases/delta_lake_databases.py` — read failures

### DR1 — A Delta table that could not be read was reported as having no rows
**Status:** resolved
**Severity:** critical
**Issue:** ITL-884

`DeltaTableDatabase.get_all_records` logged a read error at WARNING and returned `None`, and
`get_records_by_ids` returned `None` on any error. Both conflated "the table does not exist"
with "the table exists but cannot be read". Callers treat `None` as no rows. A function node
recomputed with a "deleted externally" warning, its per-row lookups failed inside the
`error_policy="continue"` handler, and the run finished without raising, so stored results
looked dropped. `_flush_by_key` also treated a `DeltaProtocolError` on load as "no table" and
wrote the batch with `mode="overwrite"`. The case that exposed it: an append whose column type
differs from the stored table, which Delta's merge either casts silently to the stored type or,
with an extension type involved, accepts and then cannot read. Other backends were not
affected: the in-memory and connector databases (SQLite, PostgreSQL, SpiralDB) return `None`
only for a table that does not exist and let read errors propagate.

**Fix:** only `TableNotFoundError` means absent. Every other load or read error raises
`orcapod.errors.StoreReadError`, which names the table and chains the cause. `FunctionNode.execute`, the
async `FunctionPod` row handler and the async orchestrator let it fail the run whatever the
`error_policy`. Before merging into an existing table, `_flush_by_key` compares the batch with
the stored Delta schema through Delta's own type mapping (extension types by storage,
large-normalized, nullability ignored). On a difference it raises `InputValidationError` naming the
table, column and both types, and nothing is written; struct fields are compared by name at
every depth, so a new nested field is schema evolution, not a difference. The batch's types
are converted with the Delta writer's own conversion (`deltalake.writer._conversion`, the one
`write_deltalake` and `merge` use) before the comparison, and the stored schema is read without
field metadata, so a type the writer stores differently (timestamps in microseconds, unsigned as
signed) or a logical type whose Delta storage differs is never refused; the differential guard
`tests/test_databases/test_delta_append_types.py` appends an identical schema for every Arrow and
logical type Delta can store. `flush()` raises a `StoreReadError` or
that `InputValidationError` as itself, not wrapped in `RuntimeError`, with every other failed
batch noted on it, so a read failure in a per-row result-cache flush still fails the run. The
async orchestrator selects the `StoreReadError` from the `TaskGroup`'s exception group with a
plain `except`, noting the other failures, so a concurrent unrelated failure cannot turn it into
an `ExceptionGroup`. With ITL-878 (SW1) both errors derive from `orcapod.errors.StoreError`
and share one path: the per-row handlers, `orcapod.utils.store_errors` and both orchestrators
handle `StoreError`. A write that fails because the stored table cannot be read raises
`StoreReadError`, not `StoreWriteError`; a refused append is a `StoreWriteError` caused by the
`InputValidationError`. The orchestrators label a store error with the node it escaped from,
and the async orchestrator ranks read and write errors alike by node order. Regressions:
`tests/test_databases/test_delta_read_failures.py`,
`tests/test_pipeline/test_store_read_failure.py` and
`tests/test_pipeline/test_store_errors_together.py`. Open: whether a nullability-only change
should be refused (Delta's merge relaxes a non-null nested field to nullable) belongs to the
nullability audit, ITL-842.

---

## Newcomer papercuts found by the docs audit

### NP1 — Missing export, misleading errors, inconsistent defaults, broken example, write noise
**Status:** in progress
**Severity:** medium
**Issue:** ITL-821

The docs audit (PR #271) ran every sample against `main` and found behavior that trips a
newcomer. Re-checked on `b197544e` (v0.2.0 + #291):

1. `orcapod.enable_file_hash_caching` was not exported. **Fix:** re-exported from `orcapod`.
2. `DictSource(data=[], tag_columns=[...])` raised `tag_columns not found in table: {...}.
   Available columns: []`. **Fix:** `SourceStreamBuilder.build` raises `Table is empty` for
   a table with no columns; a zero-row source with `data_schema=` still builds.
3. A pod applied to a stream with an extra column printed `Key 'age' not found in parameter
   types.` (a `logger.warning` reaching logging's last-resort stderr handler) before raising
   `Incoming data data type ...`. **Fix:** `schema_utils.schema_incompatibility` returns the
   reason instead of logging; `FunctionPod` and `FunctionNode` raise `Incoming data schema
   ... : column 'age' is not a parameter of the function`.
4. Defaults that disagree: `@function_pod` `version="v0.0"` vs `FunctionPod.from_fn` /
   `side_effect_function_pod` `"v1.0"` (the major version is in the data function's `uri`, so
   it moves identity hashes and the result-store path); `MapData`/`MapTags`
   `drop_unmapped=False` vs `stream.map_data`/`map_tags` `True` (in the operator identity).
   **Open:** awaiting the developer's choice of default.
5. `examples/save_and_load_pipelines.py` passed a database to `Pipeline()` and failed.
   **Fix:** rewritten around `PipelineJob`; `tests/test_examples/test_examples.py` runs every
   example in a subprocess so an example cannot break silently again.
6. Noise: the protobuf gencode `UserWarning`s on import no longer reproduce (every
   `orcapod` module imported with and without all extras, `-W always`; protobuf is never
   loaded), so that half is closed. deltalake's `WARN deltalake_core::writer::stats Skipping
   column ... because it's a binary field` lines on every Delta write still appear.
   **Open:** awaiting the developer's choice of how to silence them.

---

## `src/orcapod/databases/delta_lake_databases.py` — reading back a handle family's own writes

### DW1 — A read through one `at()` handle could miss rows its family had just written
**Status:** resolved
**Severity:** high
**Issue:** ITL-864

Right after a first `PipelineJob` run on a `DeltaTableDatabase`, `node.get_all_records()`
returned 1 of 2 or 3 rows (plain `int -> int` pod, sync and async orchestrators), while
`iter_data` and a fresh handle saw every row. Every `at()` handle shares the root's pending
batches, and any handle's `flush()` writes every pending key, so the result-store handle
flushed the pipeline table's rows. But each handle kept its own loaded-table cache, so the
node's pipeline handle kept reading the table version it had loaded during the run. Every read
method missed the rows a sibling flushed. Separately, the read methods merged pending and stored
rows inconsistently: `get_records_by_ids` ignored pending rows, `get_records_with_column_value`
ignored stored rows whenever a pending batch existed (and returned an empty table instead of
`None`), and a pending row whose record id was already stored was returned as well, although a
flush keeps the stored row and drops the pending one.

**Fix:** the loaded-table cache and the committed snapshots (`_CommittedSnapshot`, which
replaced the existing-ids and cache-dirty dicts in ITL-919) are shared across the `at()`
family exactly as pending state (pending batches and their indexes) is. Every read method returns what a flush would leave: the
stored rows plus the pending rows whose record id is not stored (`_pending_not_stored`), and
`None` when there are none. These dicts are touched only from the thread that drives the
pipeline; the async orchestrator runs only user functions in worker threads. Regression tests:
`tests/test_databases/test_delta_read_your_writes.py` and
`tests/test_pipeline/test_delta_node_records_after_run.py`. Stored ids come from
`_stored_ids`: the snapshot of the loaded table version reads them on first use, through an
id-only scan (a one-column schema); a failed scan raises `StoreReadError` and leaves the
snapshot's ids unset (`None`), so the next read scans again instead of treating every stored
id as unstored and returning duplicates. A handle that loaded the table before it had any
stored ids (a fresh root over an existing table) reads them on its first read with pending
rows (`_pending_not_stored`). After the family's own commit the ids are extended from the
commit itself, with no re-read (ITL-919).

When stored and pending rows are read together, the result has the schema a flush would give:
both parts are large-normalized (`null` kept), then combined with
`pa.concat_tables(promote_options="default")`. A column missing on one side is null-filled, and a
`null`-typed column takes the other side's type. Columns whose types differ (`int64` vs `int32`,
a string vs an int, a timestamp unit, a decimal precision) raise `pyarrow.ArrowTypeError`; nothing
is widened. Before this, a read of stored rows next to pending rows with any schema difference,
including a pending `string` column next to the stored `large_string`, raised `ArrowInvalid`;
`get_all_records` already did so on `main`.
A `get_records_with_column_value` query on a column that only the stored or only the pending
rows have skips the side without it (after a flush that side holds null there, and
`column == value` never matches null), so it returns before a flush what it returns after; a
column neither side has still raises `ArrowInvalid`.

Recorded for the nullability audit (ITL-842) and ITL-890: Delta's merge relaxes nullability (a
non-null pending column into a nullable stored one stays nullable) and evolves struct children
(`struct<p>` stored + `struct<p, q>` pending becomes `struct<p, q>`, stored rows `q = null`). The
read path now mirrors both. Whether Orcapod should allow either at all is for those issues. A
pending all-null `null`-typed column reads back as `null`, but Delta cannot store it: the flush
fails and the batch stays pending (unchanged, pinned by
`test_pending_null_typed_column_reads_back_but_does_not_flush`).

Still open: a handle does not see writes made by another process, or by a separate
`DeltaTableDatabase` root on the same path, until its own next commit to that table reloads it;
this is unchanged. When it does reload (another writer committed in between), the stored ids
are read again with the id-only scan, and the stored rows on the next lookup, each once per
version (D3, now bounded by ITL-919's snapshot).

---

## `src/orcapod/databases/delta_lake_databases.py`, `src/orcapod/core/sources/delta_table_source.py` — exit after a Delta read

### DX1 — A process that read a Delta table can hang or abort at interpreter exit
**Status:** resolved (local stores); cloud stores remain exposed upstream
**Severity:** high
**Issue:** ITL-892

`DeltaTable.to_pyarrow_dataset()` (and `to_pyarrow_table()`) reads through deltalake's
`PyFileSystem(DeltaStorageHandler)`, a filesystem implemented in Python. After a scan returns,
Arrow finishes tearing down the scan on a thread-pool worker. If that worker drops the last
reference to a Python-backed buffer during interpreter finalization, CPython stops the thread,
and `ThreadPool::~ThreadPool` in `__cxa_finalize` waits for it forever. On Linux with Python
3.11/3.12 the process may abort with SIGABRT instead. This is upstream:
apache/arrow#50482 and #54536, with an open fix PR, #54467. The failure follows a scan, not a
write.

Measured over 100 runs each, locked versions: one write plus one scan fails 57% of runs on
macOS arm64 and 5–38% on Linux x86_64. A fresh `DeltaTableDatabase.get_all_records` fails 53%
on macOS and 8–23% on Linux. `DeltaTableSource` fails 4–10%. A `PipelineJob` run fails 0–4%.
Every pyarrow 20.0.0–26.0.1 × deltalake 1.0.2/1.6.6 combination fails, so no version pin helps.
Scanning local tables through a native pyarrow filesystem (`SubTreeFileSystem(table_path,
LocalFileSystem())`) gives 0/100 in every case. Its results are byte-identical. Cloud stores
keep the Python filesystem and stay exposed until pyarrow ships the fix. Findings, scripts and
raw results: `superpowers/specs/itl-892-exit-hang/FINDINGS.md`.

**Fix:** `storage_utils.delta_to_pyarrow_dataset` opens local Delta tables on a native pyarrow
filesystem (`SubTreeFileSystem` over `LocalFileSystem`). Every scan in `DeltaTableDatabase` and
`DeltaTableSource` goes through it. Cloud URIs keep deltalake's reader. The results are
schema- and value-identical: `tests/test_databases/test_delta_native_fs_read.py` compares them
with deltalake's reader for logical types (Path, dataclass, numpy, pandas), partitioned tables
and escaped partition values, and for table roots whose names need percent-encoding
(`table_uri` is percent-encoded, so the helper decodes it with `pyarrow.fs.FileSystem.from_uri`;
a root that is neither a `file://` URI nor an absolute POSIX path, such as a Windows drive path,
is untested and keeps deltalake's reader). `tests/test_databases/test_delta_exit.py` runs fresh
interpreters and fails if one does not exit cleanly; it failed before the fix. The remaining
cloud-store exposure is documented in `docs/troubleshooting.md`.

**Considered alternative, not implemented:** an `atexit` hook that releases the GIL briefly
(`time.sleep(0.05)`) before finalization, the idea of apache/arrow#54467. It measured 0/100 on
macOS and Linux and also covers cloud stores. But it is timing-based, so a loaded machine or a
large scan can outlast it. It would also run in every process that imports Orcapod. If cloud
stores need covering before pyarrow ships the fix, register it lazily on the first cloud-store
scan. When pyarrow ships #54467, bump the floor and reconsider whether the helper is still
needed.

---

## `src/orcapod/core/operators/filters.py` — `PolarsFilter` identity and nullability (ITL-828)

### PF1 — An expression predicate could not be hashed, and a filter broke a join inside a job
**Status:** resolved

**PF1a.** `stream.polars_filter(pl.col("x") > 0).pipeline_hash()` raised `TypeError` (no
semantic handler for `pl.Expr`), so an expression filter failed upstream of a join and anywhere
inside a `PipelineJob`.

**Fix (PF1a):** `PolarsExprHandler` hashes a `pl.Expr` as `polars_expr_v1`: a strict parser
turns `Expr.meta.serialize(format="json")` into a tree in an Orcapod-owned grammar
(`hashing/semantic_hashing/polars_expr.py`), hashed as sha256 of its canonical JSON. Polars'
serialization is not hashed directly: it changed layout between 1.20, 1.30 and 1.36. Unknown
nodes and unsupported expressions (Python functions, multi-column selectors, aggregations,
`alias`, `when/then`, non-finite literals, `pl.Series` literals) raise `ValueError` naming the
expression, so a Polars layout change fails loudly instead of moving a hash. `predicates` is
normalized to a tuple. The developer confirmed the identity design (November 11, 2026).

Also fixed: inside a `PipelineJob` any `PolarsFilter` (keyword constraints too) upstream of a
`Join` raised `InputValidationError` (`str` vs `str | None`), because Polars returns every field
nullable and the filter restored types but not nullability.

**PF1b.** Data columns of a filtered stream also came back nullable, which broke the rule that
operators never change nullability, and gave a filtered row a different data hash from its
source row.

**Fix (PF1b):** the filter restores every column's input nullability (developer, November 11,
2026). A filtered row's data hash now equals its source row's. The pinned
`pipeline:polars_filter` row data hashes moved (approved); they are the cache keys of any
function pod downstream of a filter, so a 0.2 store recomputes those results once.

---

## `src/orcapod/databases/` — PipelineJob run time quadratic in rows

### PQ1 — Every row a function node processes scans the rows already stored
**Status:** resolved
**Severity:** high
**Issue:** ITL-919

A function node in a `PipelineJob` does, per row: `ResultCache.lookup` (lookup by input data
hash in the result table), `ResultCache.store` with a flush of the whole store (both
tables), and `add_pipeline_record` (lookup by base entry id for the recomputation index,
then `add_record(skip_duplicates=True)`). Sync and async orchestrators share that path;
operator nodes write with one `add_records` and are not affected. Measured on `c8efc092`,
in-memory store, sync: 7.2 / 20.9 / 64 ms per row at 100 / 400 / 1,000 rows (standalone
pod: 0.09 ms).

- `InMemoryArrowDatabase`: each lookup filtered the whole table, the duplicate check built
  the set of every committed id, and each flush ran `is_in` over all ids, filtered and
  concatenated, adding one chunk per row, so every later Arrow operation cost O(rows).
  **Fix:** lazily built hash indexes over committed rows (binary and string columns, record
  ids), extended on append and rebuilt only after a flush overwrites an id; flush keeps the
  committed table at O(log N) record batches by binary-counter compaction. Stored rows, row
  order, ids and schemas are unchanged (differential check against `main` on a joined
  multi-node pipeline, both orchestrators). Store work per row is now independent of rows
  stored (tests count Arrow bytes allocated per row).
- `DeltaTableDatabase`: one Delta MERGE commit per row per table (cost grows with the
  table and leaves one file per row), lookups re-opened and read every file, the
  existing-id cache was re-read after every flush, and the pending batch was scanned per
  lookup (main: 80 / 198 / 509 ms per row at 100 / 400 / 1,000 rows, sync). **Fix:** function
  nodes inside `PipelineJob.run` commit every `commit_every_rows` rows (default 100), at node
  end, best-effort on failure and at job end, results before pipeline records; a node used
  on its own still commits per row. The store keeps, per loaded table version, the stored
  ids and rows in memory (indexed), extended after its own commits from the new data files
  only; the pending batch is compacted and indexed; a commit appends when every pending id is
  new in the version loaded for that commit, and MERGEs otherwise. Stored rows, ids, hashes,
  paths and schemas are unchanged (differential against `main`). Store failures that only a
  commit reveals now surface at the next commit point and name no row (developer decision,
  November 11, 2026). Found on the way: an async run hung on any node failure with more rows
  in flight than the channel buffers (ITL-932, fixed: closing a channel never blocks), and an
  async rerun of an ephemeral-result node skipped its rows (ITL-923, fixed). Remaining
  (ITL-923 item 3): a pending row written with `skip_duplicates=False` whose id is already
  stored is still dropped by the commit (Delta keeps the stored row; the in-memory store
  overwrites it). Memory: a store object keeps the committed rows of each table it looks rows
  up in for as long as it lives, all of them at once (main read one whole table per lookup
  and released it), so memory is cumulative over the tables: measured for an `int -> int`
  pod, about 820 bytes per stored row for the result table and 746 for the pipeline-record
  table, so about 1.6 GB per function node at 1M rows. Keeping only the lookup columns would
  need a read per matched row. A follow-up to bound it is for the coordinator to file. The store's own
  size-triggered flush (`batch_size`) now commits only the full table, and `PipelineJob.run`
  refuses `commit_every_rows >= batch_size`, so the results-first order holds.

---

## `src/orcapod/logical_types/numpy_type.py`, `src/orcapod/logical_types/array_format.py` — array format

### AR1 — `numpy.ndarray` stored as a `.npy` file and hashed by file bytes
**Status:** in progress
**Severity:** high
**Issue:** ITL-888 (absorbs ITL-883; design ITL-874)

Arrays were stored as the exact `np.save` file in a `large_binary` column under extension name
`numpy.ndarray`, and hashed as SHA-256 of that file under the bare label `sha256`. The hash
followed the file, not the value: equal arrays in C and Fortran order, in little- and big-endian
byte order, or with structured fields at other offsets hashed differently. Masked arrays lost
their mask silently. `StringDType` failed with a misleading pickle error. A pod declared
`-> np.ndarray` could return a NumPy scalar unchecked.

v0.3 moves to the value-based format of `superpowers/specs/2026-11-10-array-format-v0.3.md`:
extension `orcapod.ndarray` over `struct<dtype, shape, data>` (little-endian, C order), hashed
by value under the label `ndarray_v1` (developer's decisions, November 10 and 11, 2026). Old
data is neither detected nor migrated in v0.3; migration is deferred to ITL-889. Accepting
`longdouble`/`clongdouble`, unit-less datetimes and titled structured dtypes, refused in v0.3,
is reconsidered in ITL-911. The new logical type name moves the table paths of nodes with an
array column and the pipeline and operator tables of every node downstream of one (pipeline
hashes are Merkle-chained); an array-free pod downstream keeps its result table and reuses its
results.

Known compromise: the struct's children are nullable at the Arrow level, because Polars drops
nested non-null flags on its way back to Arrow (ITL-842). The logical type enforces non-null
instead. Tighten under a new storage version once nested nullability survives Polars.

## `src/orcapod/core/sources/spiraldb_table_source.py` — Spiral tables as a workshop source (ITL-926)

### SP2 — A Spiral table source was unverified on v0.3, and is limited for large or unusual tables
**Status:** in progress
**Severity:** high
**Issue:** ITL-926 (follow-ups: ITL-186 streaming source)

Re-verified against the EDI sample project (`edi-sample-v1-855632`, read-only) and a
disk-backed pyspiral fake (`tests/test_databases/fake_pyspiral.py`). The workshop path works:
a `SpiralDBTableSource` with a composite key, a `Join` of two Spiral sources, a function pod,
a Delta store, a re-run with no function calls, save and load, and the same hashes in a fresh
process. Findings, with repro scripts, are in `superpowers/specs/itl-926-live/FINDINGS.md`.

**Fixed:**

* Nested string, binary and list types are normalized to large variants on both read paths
  (ITL-40).
* Errors name the project and dataset.
* pyspiral is raised to 0.16.33.
* The default `source_id` is `<project id>/<dataset>/<table>` (approved hash change).
* Source building no longer calls `to_pylist()` on the whole table (byte-identical).
* Non-nullable columns stay non-nullable end to end: true only on the original pyspiral
  fake. SpiralDB keeps no non-null flag except on list items; fixed by the nullability record
  (SP3, ITL-936).

**Open (developer decisions pending):**

* Every column of the EDI sample tables is nullable, so a function pod over them must
  annotate `T | None`; recorded columns arrive as `T` (SP3, ITL-936).
* `fixed_size_list` columns cannot be hashed: not supported until Orcapod adopts starfix
  0.5.0 (ITL-910; starfix support is ITL-927). `map` columns fail the same way in starfix
  0.4.0 (none in the EDI tables).
* The whole table is read at construction, which rules out tables such as `spike_event`
  (8.1M rows); the streaming design is in
  `superpowers/specs/2026-11-11-itl-186-spiral-streaming-source-design.md`.
* Record IDs are positional; key-based IDs are approved for the streaming source work
  (ITL-186).
* `unit` (100 rows) takes about 35 s to build live, and the Spiral scan itself is slow
  (14.5 s for 3 rows); Orcapod's share is about 0.4 s. `scan_cost_probe.py` measures it.
* `PostgreSQLTableSource` and `SQLiteTableSource` still default `source_id` to the bare table
  name (to be filed separately).

## `src/orcapod/databases/spiraldb_connector.py` — SpiralDB stores every column nullable (ITL-936)

### SP3 — Non-nullable columns arrive as `T | None` from SpiralDB; a null in a non-null struct field is stored as 0
**Status:** in progress
**Severity:** high
**Issue:** ITL-936

Live, pyspiral 0.16.33, scratch table in `edi-tutorial-v1-841226` (November 11, 2026): a
table written with `k`, `v`, struct `g` and its field `g.x`, and list `l` with its items all
declared non-null reports `Schema({k=i64?, l=fixed_size_list(f32)[2]?, v=i32?, w=i32?,
g={x=i32?, y=i32?}?})`: every top-level column and struct field nullable; only list items keep
`not null`. Writing a null to top-level `v` declared non-null is refused by Arrow on the
client. Writing `{k: 4, v: 40, g: {x: null, y: null}}` with `g.x` declared non-null is
**accepted and reads back as `g.x = 0`**: silent data corruption on SpiralDB's side.

**Fix:** Orcapod records each table's non-nullable paths in the table's key-value metadata
(`__orcapod_nullability__`, format `orcapod.nullability` v1; `orcapod.databases.
write_nullability_record` / `read_nullability_record`). `SpiralDBConnector` writes the record
before the first rows of a table, checks every non-nullable path for nulls before any write,
refuses a write that disagrees with the record, and on read restores the record in
`get_column_info` and checks and restores every batch in `iter_batches`
(`NullInNonNullableColumnError` / `NullabilityRecordError`, naming the table). Tables without
a record are unchanged. The pyspiral fake now reports nullability as the server does. Open:
the corruption itself is SpiralDB's (to raise with Spiral).

Live check (coordinator, `superpowers/specs/itl-936-live/live_nullability.py write`): the
connector's nullability record, refusals, a Join and save/load pass. Also found, for a
top-level struct column (nested structs not tested): SpiralDB cannot store a null struct.
Written without `push_down_nulls`, pyspiral refuses it (`Spiral does not support null column
groups. Found a null in the column named "g".`); with `push_down_nulls=True`, `g = null` reads
back as `g = {x: null, y: null}` with the struct valid (value and hash change). Null lists,
null list items and null struct fields survive. The connector now refuses a null struct at any depth
with `UnsupportedValueError` before writing, and never passes `push_down_nulls`.

If the rows of a table's first write fail, the connector removes the nullability record it
wrote just before them (a record naming columns the table lacks would make every read, and
every result-store flush, raise). A record whose chunks were written but whose header was not
is no record.

Writers are not coordinated (SpiralDB has no lock; metadata writes are not versioned). The
rollback removes the record only while the table still has no value columns, but if another
writer's first rows land between that check and the removal, the table is left without a
record: it reads as `T | None` (never a hidden null) and is repaired with
`write_nullability_record` after checking the data.

Known limitation: the connector treats "the table has no value columns yet" as "the table has
no rows yet", to avoid a scan on every first write (a live Spiral scan costs seconds even for a
few rows). A table whose columns are all key columns, already holding rows without a record,
is recorded by the next write; if that write adds value columns declared non-nullable, the
earlier rows hold nulls there and every read raises `NullInNonNullableColumnError` (a null is
never hidden). Follow-up idea, unverified: a manifest-only row count
(`spiral.core.manifest_stats(snapshot).key_space_stats.total_rows`, see the ITL-186 design)
could replace the shortcut cheaply once confirmed live.

