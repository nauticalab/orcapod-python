# Design & Implementation Issues

A running log of identified design problems, bugs, and code quality issues.
Each item has a status: `open`, `in progress`, or `resolved`.

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

## `src/orcapod/core/operators/` — Async execution

### O1 — Operators use barrier-mode `async_execute` only; streaming/incremental overrides needed
**Status:** open
**Severity:** medium

All operators currently use the default barrier-mode `async_execute` inherited from
`StaticOutputPod`: collect all input rows into memory, materialize to `ArrowTableStream`(s),
run the existing sync `static_process`, then emit results. This works correctly but negates the
latency and memory benefits of the push-based channel model.

Three categories of improvement are planned:

1. **Streaming overrides (row-by-row, zero buffering)** — for operators that process rows
   independently:
   - `PolarsFilter` — evaluate predicate per row, emit or drop immediately
   - `MapTags` / `MapPackets` — rename columns per row, emit immediately
   - `SelectTagColumns` / `SelectPacketColumns` — project columns per row, emit immediately
   - `DropTagColumns` / `DropPacketColumns` — drop columns per row, emit immediately

2. **Incremental overrides (stateful, eager emit)** — for multi-input operators that can
   produce partial results before all inputs are consumed:
   - `Join` — symmetric hash join: index each input by tag keys, emit matches as they arrive
   - `MergeJoin` — same approach, with list-merge on colliding packet columns
   - `SemiJoin` — buffer the right (filter) input fully, then stream the left input and emit
     matches (right must be fully consumed first, but left can stream)

3. **Barrier-only (no change needed):**
   - `Batch` — inherently requires all rows before grouping; barrier mode is correct

---

## `src/orcapod/core/` — Pod Groups (composite pod patterns)

### G1 — Pod Group abstraction for common multi-pod patterns
**Status:** open
**Severity:** medium

Several common pipeline patterns require wiring multiple pods and operators together in a
fixed topology. Users currently have to build these manually, which is verbose, error-prone,
and obscures intent. A **PodGroup** abstraction would encapsulate a reusable sub-graph of
pods/operators behind a single pod-like interface (`process` for sync, `async_execute` for
async).

A PodGroup:
- Accepts one or more input streams and produces one output stream (same interface as a pod)
- Internally contains a fixed sub-graph of pods, operators, and channels
- Hides the internal wiring from the user
- Participates in pipeline hashing as a single composite element

#### Planned patterns

1. **AddResult** (enrich/extend pattern) — the most common case. Runs a `FunctionPod` on
   the input and joins the result back with the original packet, so the output contains
   *all* original columns plus the new computed columns.

   Internal wiring:
   ```
   input ──► broadcast ──┬──► FunctionPod ──┐
                          │                  ├──► Join ──► enriched output
                          └── passthrough ───┘
   ```

   Without PodGroup (current manual approach):
   ```python
   grade_pf = PythonPacketFunction(compute_letter_grade, output_keys="letter_grade")
   grade_pod = FunctionPod(grade_pf)
   computed = grade_pod.process(stream)
   enriched = Join()(stream, computed)  # rejoin to get original + new columns
   ```

   With PodGroup:
   ```python
   grade_pf = PythonPacketFunction(compute_letter_grade, output_keys="letter_grade")
   enriched = AddResult(grade_pf).process(stream)
   # enriched has all original columns + "letter_grade"
   ```

2. **Other potential patterns** (to be designed as needs arise):
   - **ConditionalPod** — route packets to different pods based on a predicate, merge results
   - **FanOutFanIn** — broadcast to N pods, collect and merge/concat results
   - **FallbackPod** — try primary pod, fall back to secondary on error/None result

---
