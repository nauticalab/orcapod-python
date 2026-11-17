## `src/orcapod/core/function_pod.py`

### F6 — `WrappedFunctionPod.process` makes the wrapper transparent to observability
**Status:** open
**Severity:** medium
`process` simply calls `self._function_pod.process(...)`, so the returned stream's `source` is
the *inner* pod, not the `WrappedFunctionPod`. Anything that inspects `stream.source` (e.g.
tracking, lineage) will see the inner pod and be unaware of the wrapper. Whether this is
intentional should be documented; if not, `process` needs to construct a new stream whose source
is `self`.
