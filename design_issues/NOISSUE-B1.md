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
