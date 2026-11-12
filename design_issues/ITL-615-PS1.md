## `src/orcapod/core/sources/polling_source.py`

### PS1 — `PollingSource.output_schema()` and `keys()` trigger `_run_sync` concurrently with the async polling loop, advancing the cursor and skipping batches
**Status:** resolved
**Severity:** high
**Issue:** ITL-615

`PollingSource.output_schema()` and `keys()` fall through to `_get_latest_stream()` whenever
either ``columns is not None`` or ``all_info=True``. `_get_latest_stream()` calls
`_run_sync(self._impl.poll, ...)` which spawns a `ThreadPoolExecutor` thread that runs
`asyncio.run(poll(...))`. This thread advances `self._cursor` independently of the async
polling loop in `async_iter_data()`.

The trigger is `FunctionJobNode.async_execute()` (line 2325 of `function_node.py`):

```python
tag_schema = self._input_stream.output_schema(columns={"system_tags": True})[0]
```

This is called once per `async_execute` invocation, which runs concurrently with the
source node's `async_execute` (both live inside the orchestrator's outer ``TaskGroup``).
Because ``columns`` is not ``None``, the old fast-path was missed, `_get_latest_stream()`
ran its poll+fetch cycle in a background thread, advancing the cursor from 0 → 1. When
the async polling loop then resumed at its next tick, it called ``poll(cursor=Cursor(1))``,
found no more data, and exited — processing only the first batch.

**Fix:** `output_schema()` and `keys()` now return from declared schemas directly when both
``tag_schema`` and ``data_schema`` were provided at construction and ``columns is None`` with
``all_info=False`` (removing the previous ``accumulated_stream is None`` guard from the
fast-path condition). When ``accumulated_stream`` is already populated (at least one batch
fetched by either path), the existing stream is used to answer schema queries without
triggering a new poll+fetch cycle. Schema is invariant across batches (enforced by
``_validate_combining_schemas``), so this is always correct. Schema is now declared by
``impl.schema()`` (returning a unified column schema split by ``tag_columns``) rather than by
``PollingSource`` constructor params.
