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

## `src/orcapod/core/base.py`

### B1 — `PipelineElementBase` should be merged into `TraceableBase`
**Status:** resolved
**Severity:** medium

`TraceableBase` and `PipelineElementBase` co-occur in every active computation-node class
(`StreamBase`, `PacketFunctionBase`, `_FunctionPodBase`). The two current exceptions are design
gaps rather than intentional choices:

- `StaticOutputPod(TraceableBase)` — should implement `PipelineElementProtocol`; its absence
  forced `DynamicPodStream.pipeline_identity_structure()` to include an `isinstance` check as
  a workaround.
- `Invocation(TraceableBase)` — legacy tracking mechanism, planned for revision.

Note: merging into `TraceableBase` is correct at the *computation-node* level.
`ContentIdentifiableBase` (which `TraceableBase` builds on) should **not** absorb
`PipelineElementBase` — data datagrams (`Tag`, `Packet`) are legitimately content-identifiable
without being pipeline elements.

**Fix:** Added `PipelineElementBase` to `TraceableBase`'s bases. Added
`pipeline_identity_structure()` to `StaticOutputPod`. Removed redundant explicit
`PipelineElementBase` from `StreamBase`, `ArrowTableStream`, `PacketFunctionBase`,
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

## `src/orcapod/core/packet_function.py`

### P1 — `parse_function_outputs` is dead code
**Status:** resolved
**Severity:** medium
`parse_function_outputs` is a module-level function with a `self` parameter, suggesting it was
originally a method. It is never called. `PythonPacketFunction.call` duplicates its logic verbatim.
Should be deleted or wired up as a method on `PacketFunctionBase` and used from `call`.

**Fix:** Converted to a proper standalone function `parse_function_outputs(output_keys, values)`.
Replaced the duplicated unpacking block in `PythonPacketFunction.call` with a call to it.
Updated tests accordingly.

---

### P2 — `CachedPacketFunction.call` silently drops the `RESULT_COMPUTED_FLAG`
**Status:** resolved
**Severity:** high
On a cache miss, the flag is set but the result is discarded:
```python
output_packet.with_meta_columns(**{self.RESULT_COMPUTED_FLAG: True})  # return value ignored
```
If `with_meta_columns` returns a new packet (immutable update), the flag is never actually
attached.

**Fix:** Assigned the return value: `output_packet = output_packet.with_meta_columns(...)`.
Added tests verifying the flag is `True` on cache miss and `False` on cache hit.

---

### P3 — `PacketFunctionWrapper.__init__` passes no `version` to `PacketFunctionBase`
**Status:** open
**Severity:** medium
`PacketFunctionBase.__init__` requires a `version` string and parses it into
`_major_version`/`_minor_version`. `PacketFunctionWrapper` calls `super().__init__(**kwargs)`
without a `version`, so it either crashes (no version in kwargs) or silently defaults to `"v0.0"`.
Those parsed fields are then shadowed by the delegating properties, making them dead state.
Options: pass the inner function's version through, or avoid calling the base version-parsing
logic entirely.

---

### P4 — `PythonPacketFunction` computes the output schema hash twice
**Status:** open
**Severity:** low
`__init__` stores `self._output_schema_hash` (line ~289). `PacketFunctionBase` also lazily
caches `self._output_packet_schema_hash` (different attribute name) via
`output_packet_schema_hash`. Two fields holding the same value. One is redundant.

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

`CachedPacketFunction.get_cached_output_for_packet()` (line ~547) accepts a `match_tier`
parameter that is documented in the interface but completely ignored. Cache lookups always use
exact matching. Two inline TODOs mark this:
- `# TODO: add match based on match_tier if specified`
- `# TODO: implement matching policy/strategy`

This means any caller passing a non-default `match_tier` silently gets exact-match behavior,
which could lead to unnecessary cache misses or incorrect assumptions about cache hit semantics.

Requires: design a matching strategy interface; implement at least exact and fuzzy tiers.

---

### P7 — `PythonPacketFunction.__init__` unconditionally extracts git info
**Status:** open
**Severity:** medium

`PythonPacketFunction.__init__()` (line ~324) always calls `get_git_info()`, which runs git
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

### F2 — Typo in `TrackedPacketFunctionPod` docstring
**Status:** open
**Severity:** trivial
`"A think wrapper"` should be `"A thin wrapper"`.

---

### F3 — Dual URI computation paths in the class hierarchy
**Status:** open
**Severity:** low
`TrackedPacketFunctionPod.uri` assembles the URI from `self.packet_function.*` with its own lazy
schema-hash cache. `WrappedFunctionPod.uri` simply delegates to `self._function_pod.uri`. These
should agree (and do, after the `packet_function` fix), but having two independent implementations
makes future changes fragile.

---

### F4 — `FunctionPodNode` is not a subclass of `TrackedPacketFunctionPod`
**Status:** open
**Severity:** medium
`FunctionPodNode` reimplements `process_packet`, `process`, `__call__`, `output_schema`,
`validate_inputs`, and `argument_symmetry` from scratch rather than inheriting from
`SimpleFunctionPod`/`TrackedPacketFunctionPod` and overriding the parts that differ
(fixed input stream, pipeline record writing). The result is a large amount of structural
duplication that diverges silently over time.

---

### F5 — `FunctionPodStream` and `FunctionPodNodeStream` are near-identical copy-pastes
**Status:** open
**Severity:** medium
`iter_packets`, `as_table` (including content_hash and sort_by_tags logic), `keys`,
`output_schema`, `source`, and `upstreams` are duplicated almost line-for-line. The only
behavioural differences are:
- `FunctionPodNodeStream` has `refresh_cache()`
- `FunctionPodNodeStream.output_schema` reads from `_fp_node._cached_packet_function` directly

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
**Status:** open
**Severity:** medium
The method checks for an existing record with `get_record_by_id` and skips insertion if found.
But it then calls `add_record(..., skip_duplicates=False)`, which will raise on a duplicate. A
race between the lookup and the insert (e.g. two concurrent processes handling the same tag+packet)
would cause a crash instead of a graceful skip. Should use `skip_duplicates=True` for consistency
with the intent.

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

`_validate_input_schema()` (line ~162) raises a generic `ValueError` when the packet schema
is incompatible:
```python
# TODO: use custom exception type for better error handling
```

The codebase already has `InputValidationError` (in `errors.py`) which is the correct exception
for this case. Using `ValueError` means callers cannot distinguish schema incompatibility from
other value errors without string-matching the message.

Fix: change `ValueError` to `InputValidationError`.

---

### F12 — System tag columns excluded from cache entry ID
**Status:** open
**Severity:** high

`FunctionPodNode.record_packet_for_cache()` (line ~1077) builds a tag table for entry-ID
computation but excludes system tag columns:
```python
# TODO: add system tag columns
```

Two packets with identical user tags but different provenance (arriving from different
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

### F10 — `FunctionPodNodeStream.iter_packets` recomputes every packet on every call
**Status:** resolved
**Severity:** high
`iter_packets` always iterates the full input stream and calls `process_packet` for every packet,
even when results are already stored in the result/pipeline databases.  This defeats the purpose
of the two-database design (result DB + pipeline DB) used to cache computed outputs.

**Fix:** Refactored `iter_packets` to first call `FunctionPodNode.get_all_records(columns={"meta": True})`
to load already-computed (tag, output-packet) pairs from the databases (mirroring the legacy
`PodNodeStream` design), yield those via `TableStream`, then collect the set of already-processed
`INPUT_PACKET_HASH` values and only call `process_packet` for input packets not yet in the DB.
Also added `FunctionPodNode.get_all_records(columns, all_info)` using `ColumnConfig` to control
which column groups (meta, source, system_tags) are returned.

---

## `src/orcapod/core/nodes/function_node.py`

### FN1 — `PersistentFunctionNode.async_execute` Phase 2 was fully sequential
**Status:** resolved
**Severity:** high

`PersistentFunctionNode.async_execute` overrode the parent `FunctionNode.async_execute` with a
fully sequential Phase 2 — each packet was awaited one at a time in a simple `async for` loop.
This meant async packet functions (which can overlap I/O via `await`) got no concurrency benefit
when run through the Pipeline API, since `Pipeline.compile()` wraps all function pods in
`PersistentFunctionNode`.

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
`.drop([constants.CONTEXT_KEY])` on the tags table built from the accumulated packets. When the
stream is empty (e.g. because the packet function is inactive), `iter_packets()` yields nothing,
`tag_schema` stays `None`, and `pa.Table.from_pylist([], schema=None)` produces a zero-column
table. The subsequent `.drop([constants.CONTEXT_KEY])` then raises `KeyError` because the column
does not exist.

**Fix:** Guarded both `.drop([constants.CONTEXT_KEY])` calls in `FunctionPodStream.as_table()` and
`FunctionPodNodeStream.as_table()` with a column-existence check. Also made the final
`output_table = self._cached_output_table.drop(drop_columns)` safe by filtering `drop_columns`
to only columns that exist in the table.

---

## `src/orcapod/core/streams/`

### ST1 — `drop_packet_columns` may leave orphan source-info columns
**Status:** open
**Severity:** medium

The `StreamProtocol.drop_packet_columns()` method (line ~309) drops data columns but it is
unclear whether the corresponding `_source_<col>` columns are also removed:
```python
# TODO: check to make sure source columns are also dropped
```

If source-info columns survive after the data column is dropped, downstream consumers may see
stale provenance data or schema mismatches.

---

### ST2 — `iter_packets()` does not support table batch streaming
**Status:** open
**Severity:** low

`ArrowTableStream.iter_packets()` (line ~261) works only with fully materialized Arrow tables,
not with `RecordBatchReader` or lazy batch iteration:
```python
# TODO: make it work with table batch stream
```

Relevant for future streaming/chunked processing of large datasets.

---

### ST3 — Column selection operators duplicate `validate_unary_input()` five times
**Status:** open
**Severity:** medium

`SelectTagColumns`, `SelectPacketColumns`, `DropTagColumns`, `DropPacketColumns` (in
`column_selection.py:58`, `137`, `214`, `292`) and `PolarsFilterByPacketColumns`
(`filters.py:135`) each have near-identical `validate_unary_input()` implementations. All are
marked:
```python
# TODO: remove redundant logic
```

The only difference between them is which key set (tag vs. packet) is checked and the error
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
   - `MapTags` / `MapPackets` — rename columns per row, emit immediately ✅
   - `SelectTagColumns` / `SelectPacketColumns` — project columns per row, emit immediately ✅
   - `DropTagColumns` / `DropPacketColumns` — drop columns per row, emit immediately ✅

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

---

## `src/orcapod/core/` — AddResult pod and Pod Groups

### G1 — `AddResult`: a first-class pod type for packet enrichment
**Status:** open
**Severity:** medium

The most common pipeline pattern is *enrichment*: run a function on a packet and append the
computed columns to the original packet rather than replacing it. This is logically equivalent
to `FunctionPod → Join(original, computed)`, but implementing it as a first-class pod type is
both simpler and more efficient.

#### Why a dedicated pod type, not a composite

A naïve decomposition into `FunctionPod + Join` works but has unnecessary overhead:

1. **Materialization waste** — FunctionPod produces an intermediate stream that is only created
   to be immediately joined back. AddResult can compute new columns and merge them into the
   original packet in a single pass, with no intermediate stream.
2. **Redundant tag matching** — Join must re-match tags that trivially correspond (they came
   from the same input row). AddResult already holds the (tag, packet) pair and can skip the
   matching entirely.
3. **Simpler async path** — streams row-by-row like FunctionPod: read (tag, packet), call
   the packet function, merge original packet columns + new columns, emit. No broadcast,
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
- **Computed columns** — produced by the wrapped PacketFunction, provenance tracked
  (function-pod-like). Source-info columns reference the PacketFunction, exactly as
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
grade_pf = PythonPacketFunction(compute_letter_grade, output_keys="letter_grade")
enriched = AddResult(grade_pf).process(stream)
# enriched has all original columns + "letter_grade"

# Async (streaming, row-by-row)
await AddResult(grade_pf).async_execute([input_ch], output_ch)
```

#### Implementation notes

- `output_schema()` returns `(input_tag_schema, input_packet_schema | function_output_schema)`
  — the union of original packet columns and new computed columns.
- Must raise `InputValidationError` if function output keys collide with existing packet
  column names (same constraint as Join on overlapping packet columns).
- `pipeline_hash` should behave as if the decomposition were performed — commits to the
  wrapped `PacketFunction`'s identity plus the upstream's pipeline hash.
- Source-info on computed columns references the PacketFunction (as FunctionPod would).
  Source-info on preserved columns passes through unchanged (as Join would).
- `async_execute` can use the same semaphore-based concurrency control as `FunctionPod`.

---

## `src/orcapod/hashing/semantic_hashing/`

### H1 — Semantic hasher does not support PEP 604 union types (`int | None`)
**Status:** open
**Severity:** medium

The `BaseSemanticHasher` raises `BeartypeDoorNonpepException` when hashing a
`PythonPacketFunction` whose return type uses PEP 604 syntax (`int | None`).
The hasher's `_handle_unknown` path receives `types.UnionType` (the Python 3.10+ type for
`X | Y` expressions) and has no registered handler for it.

`typing.Optional[int]` also fails (different error path through beartype).

This means packet functions cannot use union return types — a common pattern for functions
that may filter packets by returning `None`.

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
- **ConditionalPod** — route packets to different pods based on a predicate, merge results
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

## `src/orcapod/utils/`

### U1 — Source-info column type hard-coded to `large_string`
**Status:** open
**Severity:** critical

In `add_source_info_to_table()` (`arrow_utils.py:604`), when source info is a collection it is
unconditionally cast to `pa.list_(pa.large_string())`:
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
