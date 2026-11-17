# Design: PollingSource async_iter_data Delegation Fix

**Issue:** ITL-615
**Date:** 2026-08-20
**Status:** Approved

---

## Overview

`PollingSource` bound into a `PipelineJob` never runs its async polling loop when executed
via `AsyncPipelineOrchestrator`. Instead, only a static single-batch snapshot is produced.
The root cause is a missing `async_iter_data()` override in `SourceJobNode`.

---

## Root Cause

When the async orchestrator runs a `SourceJobNode`, the call chain is:

```
AsyncPipelineOrchestrator
  → SourceJobNode.async_execute(writer)         [inherited from SourceNodeBase]
    → self.async_iter_data()                    [SourceNodeBase.async_iter_data()]
      → for pair in self.iter_data(): yield pair  ← wraps iter_data() as sync gen
        → self._bound_source.iter_data()
          → PollingSource.iter_data()
            → self._get_latest_stream().iter_data()  ← STATIC SNAPSHOT only
```

`PollingSource` has two distinct execution modes:

- **`iter_data()`** — sync snapshot: performs one `fetch()` call and returns cached rows.
- **`async_iter_data()`** — async polling loop: continuously polls on `cfg.interval`,
  calls `fetch()` on new data, and emits rows as they arrive.

`SourceJobNode` does not override `async_iter_data()`, so the base-class
implementation wraps `iter_data()` synchronously — `PollingSource.async_iter_data()` is
never called and the polling loop never starts.

---

## Goals & Success Criteria

- `SourceJobNode.async_iter_data()` delegates to `self._bound_source.async_iter_data()`
  when a source is bound, instead of falling through to the sync `iter_data()` wrapper.
- A `PollingSource` with multiple batches, bound to a `PipelineJob` and run via
  `AsyncPipelineOrchestrator`, produces one output row per batch (not just the first).
- Unbound `SourceJobNode` raises `UnboundSourceError` on `async_iter_data()` —
  consistent with `iter_data()` behavior.
- All existing tests continue to pass.

---

## Scope & Boundaries

In scope:

- `SourceJobNode.async_iter_data()` override in `src/orcapod/core/nodes/source_node.py`.
- Integration test in `tests/test_channels/test_polling_source_pipeline_integration.py`.
- `DESIGN_ISSUES.md` entry for this bug.

Out of scope:

- Changes to `SourceNodeBase` or `AsyncPipelineOrchestrator`.
- Any new scheduling, cancellation, or duration-propagation logic.
- Sync orchestrator path (unaffected — `iter_data()` is correct for sync).

> **Note (post-implementation):** `PollingSource` itself was also modified during
> implementation to fix a race condition where concurrent schema queries triggered
> an unintended poll+fetch cycle, and to add `schema()` to `DynamicSourceProtocol`
> so that declared schemas are available without fetching data.

---

## Fix

Add a single method override to `SourceJobNode` (consistent with its existing delegation
pattern for `iter_data()`, `output_schema()`, and `as_table()`):

```python
async def async_iter_data(self):
    """Delegate to ``bound_source.async_iter_data()`` when bound.

    Overrides ``SourceNodeBase.async_iter_data()`` to route through the
    bound source's own async generator instead of wrapping ``iter_data()``
    synchronously. This ensures that dynamic sources such as
    ``PollingSource`` run their async polling loop rather than returning
    a static snapshot.

    Raises:
        UnboundSourceError: When no concrete source is attached.
    """
    if self._bound_source is None:
        raise UnboundSourceError(
            f"SourceJobNode '{self._name}' has no concrete source bound. "
            "Call job.bind(sources={'<name>': source}) before running."
        )
    async for pair in self._bound_source.async_iter_data():
        yield pair
```

The core fix is a single method override in `SourceJobNode`. During implementation,
`PollingSource` and `DynamicSourceProtocol` were also updated to fix a concurrent
schema-query race condition and to add declared-schema support — see the post-implementation
note in the Scope section above.

---

## Testing Strategy

New file: `tests/test_channels/test_polling_source_pipeline_integration.py`

Two test cases:

1. **`test_async_orchestrator_runs_polling_loop`** — `PollingSource` with 2 batches, bound
   to a `PipelineJob` with a downstream `@function_pod`, run via
   `AsyncPipelineOrchestrator`. Asserts both batches reach the downstream node (2 DB
   records). A static snapshot would produce only 1.

2. **`test_unbound_source_job_node_async_iter_raises`** — Unbound `SourceJobNode`'s
   `async_iter_data()` raises `UnboundSourceError` on first iteration (regression guard).
