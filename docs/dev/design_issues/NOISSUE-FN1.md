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
