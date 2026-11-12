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
