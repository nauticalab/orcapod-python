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
