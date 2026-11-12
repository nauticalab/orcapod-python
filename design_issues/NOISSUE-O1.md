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
