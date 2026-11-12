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
