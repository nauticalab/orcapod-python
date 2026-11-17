# ITL-516: Unify FunctionPod/FunctionJobNode async_execute() Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move `async_execute()` to `_FunctionPodBase`, redesign `FunctionJobNode.async_execute()` as a 3-stage concurrent pipeline (route → compute → record), eliminating duplicate dispatch loops.

**Architecture:** `_FunctionPodBase.async_execute()` becomes the single authoritative async dispatch loop (with observer support), used by all pod types via polymorphism. `FunctionJobNode.async_execute()` delegates computation to `execution_pod.async_execute()` via bounded `compute_channel`/`result_channel`, retaining only routing (cache hits vs misses), correlation-key stamping, `add_pipeline_record` calls, and key stripping. Three coroutines run concurrently in a `TaskGroup`.

**Tech Stack:** Python asyncio (`asyncio.TaskGroup`, `asyncio.Semaphore`), `orcapod.channels.Channel`, `pytest-asyncio`.

---

## File Structure

| File | Action | Responsibility |
|------|--------|----------------|
| `src/orcapod/core/function_pod.py` | Modify | Add `pod_config` to `_FunctionPodBase` and `WrappedFunctionPod`; move `async_execute()` from `FunctionPod` to `_FunctionPodBase` with observer hooks |
| `src/orcapod/core/nodes/function_node.py` | Modify | Add `_TAG_NODE_INPUT_REF` constant; replace `FunctionJobNode.async_execute()` with 3-stage pipeline; delete `_async_execute_one_data`; remove dead imports |
| `tests/test_core/test_regression_fixes.py` | Modify | Remove `TestAsyncExecuteBackpressure` (orphan test for orphan method) |
| `tests/test_core/nodes/test_function_job_node_async_execute.py` | Create | All async_execute coverage: no-DB, DB (persistent + ephemeral), cache hits/misses, correlation key absence, concurrency, observer hooks |

---

## Task 1: Move `async_execute()` to `_FunctionPodBase` with observer support

`FunctionPod.async_execute()` currently lives on `FunctionPod` (lines 344–383 of `function_pod.py`).
`CachedFunctionPod` inherits from `WrappedFunctionPod → _FunctionPodBase`, not from `FunctionPod`, so
it currently has no `async_execute()`. Moving the method to `_FunctionPodBase` gives all pod types
the dispatch loop via polymorphism.

Two supporting changes are needed first: a `pod_config` property on `_FunctionPodBase` (default
`PodConfig()`) and a delegating override on `WrappedFunctionPod`. Without these, `CachedFunctionPod`
would silently ignore concurrency limits set on the inner `FunctionPod`.

**Files:**
- Modify: `src/orcapod/core/function_pod.py`

- [ ] **Step 1: Write a failing test for observer hooks on `_FunctionPodBase.async_execute()`**

Create a temporary test in an existing test file or a scratch location. We'll move this to the
dedicated file in Task 4. For now, paste it into the bottom of
`tests/test_core/test_regression_fixes.py` (we'll relocate in Task 4).

```python
# Add these imports at the top of test_regression_fixes.py if not present:
# from orcapod.types import PodConfig

class TestFunctionPodBaseAsyncExecuteObserver:
    """Observer hooks fire correctly in _FunctionPodBase.async_execute()."""

    @pytest.mark.asyncio
    async def test_observer_on_data_start_and_end_fire_per_item(self):
        """on_data_start fires before and on_data_end fires after each item."""
        starts = []
        ends = []

        class _Spy:
            def contextualize(self, *path):
                return self
            def on_node_start(self, *a, **kw): pass
            def on_node_end(self, *a, **kw): pass
            def on_data_start(self, label, tag, data):
                starts.append((label, tag, data))
            def on_data_end(self, label, tag, inp, out, *, cached):
                ends.append((label, tag, inp, out, cached))
            def on_data_crash(self, label, tag, data, exc):
                pass
            def create_data_logger(self, tag, data):
                return None

        def double(x: int) -> int:
            return x * 2

        pf = PythonDataFunction(double, output_keys="result")
        pod = FunctionPod(pf)

        stream = make_stream(3)
        input_ch = Channel(buffer_size=16)
        output_ch = Channel(buffer_size=16)
        await feed_stream_to_channel(stream, input_ch)

        spy = _Spy()
        await pod.async_execute([input_ch.reader], output_ch.writer, observer=spy)

        results = await output_ch.reader.collect()
        assert len(results) == 3
        assert len(starts) == 3
        assert len(ends) == 3
        # All items were computed, so cached=False
        assert all(cached is False for _, _, _, _, cached in ends)

    @pytest.mark.asyncio
    async def test_observer_on_data_crash_fires_on_failure(self):
        """on_data_crash fires (not on_data_end) when async_process_data raises."""
        crashes = []
        ends = []

        class _Spy:
            def contextualize(self, *path): return self
            def on_node_start(self, *a, **kw): pass
            def on_node_end(self, *a, **kw): pass
            def on_data_start(self, *a): pass
            def on_data_end(self, label, tag, inp, out, *, cached):
                ends.append((label, tag, inp, out, cached))
            def on_data_crash(self, label, tag, data, exc):
                crashes.append(exc)
            def create_data_logger(self, tag, data):
                return None

        def boom(x: int) -> int:
            raise ValueError("explode")

        pf = PythonDataFunction(boom, output_keys="result")
        pod = FunctionPod(pf)

        stream = make_stream(2)
        input_ch = Channel(buffer_size=16)
        output_ch = Channel(buffer_size=16)
        await feed_stream_to_channel(stream, input_ch)

        spy = _Spy()
        await pod.async_execute([input_ch.reader], output_ch.writer, observer=spy)

        results = await output_ch.reader.collect()
        assert len(results) == 0       # all failed → nothing forwarded
        assert len(crashes) == 2       # crash fires per failed item
        assert len(ends) == 0          # on_data_end never fires on crash
```

- [ ] **Step 2: Run the tests to confirm they fail**

```bash
uv run pytest tests/test_core/test_regression_fixes.py::TestFunctionPodBaseAsyncExecuteObserver -v
```

Expected: `FAILED` — `pod.async_execute()` currently accepts no `observer` parameter.

- [ ] **Step 3: Add `pod_config` default to `_FunctionPodBase` and delegating override to `WrappedFunctionPod`**

In `src/orcapod/core/function_pod.py`, locate `_FunctionPodBase` (line 60) and add the property
**after** the `executor` setter (around line 103):

```python
# --- add to _FunctionPodBase, after the executor setter ---
@property
def pod_config(self) -> PodConfig:
    """Per-pod executor configuration. Defaults to no concurrency limits."""
    return PodConfig()
```

Then locate `WrappedFunctionPod` (line 750) and add a delegating override **after** `computed_label`:

```python
# --- add to WrappedFunctionPod, after computed_label ---
@property
def pod_config(self) -> PodConfig:
    """Delegate to the inner pod's config so CachedFunctionPod respects limits."""
    return getattr(self._function_pod, "pod_config", PodConfig())
```

- [ ] **Step 4: Add `ExecutionObserverProtocol` import to `function_pod.py`**

`function_pod.py` currently doesn't import `ExecutionObserverProtocol`. Add it to the `TYPE_CHECKING`
block (it's only used in a type hint):

```python
# In the TYPE_CHECKING block at the top of function_pod.py, add:
if TYPE_CHECKING:
    ...
    from orcapod.protocols.observability_protocols import ExecutionObserverProtocol
```

- [ ] **Step 5: Move `async_execute()` from `FunctionPod` to `_FunctionPodBase` with observer support**

Remove the existing `async_execute()` method from `FunctionPod` (lines 340–383, starting from the
`# Async channel execution (streaming mode)` section comment).

Then add the following method to `_FunctionPodBase` **just before the abstract `process()` method**
(around line 196 in the original file — adjust for line shifts from Step 3):

```python
async def async_execute(
    self,
    inputs: "Sequence[ReadableChannel[tuple[TagProtocol, DataProtocol]]]",
    output: "WritableChannel[tuple[TagProtocol, DataProtocol]]",
    pipeline_config: PipelineConfig | None = None,
    *,
    observer: "ExecutionObserverProtocol | None" = None,
) -> None:
    """Streaming async execution with per-data concurrency control.

    Each input (tag, data) is dispatched as an independent async task.
    A semaphore limits how many tasks are in-flight concurrently.
    Observer hooks fire per item: ``on_data_start`` before processing,
    ``on_data_end(cached=False)`` on success, ``on_data_crash`` on error.

    Args:
        inputs: Single-element sequence containing the input channel.
        output: Writable channel for output (tag, data) pairs.
        pipeline_config: Optional pipeline-level concurrency config.
        observer: Optional observer for per-item lifecycle hooks.
    """
    from orcapod.pipeline.observer import NoOpObserver

    try:
        pipeline_config = pipeline_config or PipelineConfig()
        max_concurrency = resolve_concurrency(self.pod_config, pipeline_config)
        obs = observer if observer is not None else NoOpObserver()
        pod_label = self.label

        sem = (
            asyncio.Semaphore(max_concurrency)
            if max_concurrency is not None
            else None
        )

        async def process_one(tag: TagProtocol, data: DataProtocol) -> None:
            obs.on_data_start(pod_label, tag, data)
            try:
                out_tag, result_data = await self.async_process_data(tag, data)
            except Exception as exc:
                logger.debug(
                    "Data processing failed, skipping: %s", exc, exc_info=True
                )
                obs.on_data_crash(pod_label, tag, data, exc)
            else:
                obs.on_data_end(pod_label, tag, data, result_data, cached=False)
                if result_data is not None:
                    await output.send((out_tag, result_data))
            finally:
                if sem is not None:
                    sem.release()

        async with asyncio.TaskGroup() as tg:
            async for tag, data in inputs[0]:
                if sem is not None:
                    await sem.acquire()
                tg.create_task(process_one(tag, data))
    finally:
        await output.close()
```

- [ ] **Step 6: Run ALL tests to verify nothing broke**

```bash
uv run pytest tests/ -v --timeout=30
```

Expected: all previously-passing tests still pass, the new observer tests pass.

Key tests to verify:
- `tests/test_core/test_regression_fixes.py::TestAsyncExecuteChannelCloseOnError` — still passes (FunctionPod inherits)
- `tests/test_core/test_regression_fixes.py::TestAsyncExecuteBackpressure` — still passes (FunctionPod still works)
- `tests/test_core/test_regression_fixes.py::TestFunctionPodBaseAsyncExecuteObserver` — now passes

- [ ] **Step 7: Commit**

```bash
git add src/orcapod/core/function_pod.py tests/test_core/test_regression_fixes.py
git commit -m "feat(function_pod): move async_execute to _FunctionPodBase with observer support

Promotes async_execute() from FunctionPod to _FunctionPodBase so all pod
types (FunctionPod, CachedFunctionPod) inherit the dispatch loop. Adds
observer hooks (on_data_start, on_data_end, on_data_crash) per item.
Adds pod_config property to _FunctionPodBase (default PodConfig()) and
WrappedFunctionPod (delegates to inner pod) so concurrency limits are
honoured through CachedFunctionPod.

Closes ITL-516 partial."
```

---

## Task 2: `FunctionJobNode.async_execute()` — no-DB delegation path

Currently the no-DB path in `FunctionJobNode.async_execute()` (lines 2147–2166) reimplements
the same semaphore-guarded TaskGroup loop that's already in `_FunctionPodBase.async_execute()`.
Replace it with a single delegation call. This mirrors the OperatorNode pattern.

**Files:**
- Modify: `src/orcapod/core/nodes/function_node.py`

- [ ] **Step 1: Write tests for the no-DB async delegation**

Create `tests/test_core/nodes/test_function_job_node_async_execute.py` with just the no-DB tests:

```python
"""Tests for FunctionJobNode.async_execute() — unified async execution path."""

from __future__ import annotations

import asyncio
from typing import Any

import pyarrow as pa
import pytest

from orcapod.channels import Channel
from orcapod.core.data_function import PythonDataFunction
from orcapod.core.function_pod import FunctionPod
from orcapod.core.nodes.function_node import FunctionJobNode
from orcapod.core.sources import ArrowTableSource
from orcapod.databases import InMemoryArrowDatabase
from orcapod.types import PodConfig


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _make_source(n: int = 3) -> ArrowTableSource:
    """ArrowTableSource with tag=key (large_string), data=value (int64)."""
    table = pa.table(
        {
            "key": pa.array([f"k{i}" for i in range(n)], type=pa.large_string()),
            "value": pa.array(list(range(n)), type=pa.int64()),
        }
    )
    return ArrowTableSource(table, tag_columns=["key"], infer_nullable=True)


def _make_no_db_node(n: int = 3, max_concurrency: int | None = None) -> FunctionJobNode:
    """FunctionJobNode with no DB attached."""
    src = _make_source(n)
    pod = FunctionPod(
        PythonDataFunction(lambda value: value * 2, output_keys="result"),
        pod_config=PodConfig(max_concurrency=max_concurrency),
    )
    return FunctionJobNode(pod, src)


async def _run_node(node: FunctionJobNode) -> list[tuple]:
    """Feed the node's own input stream through async_execute and collect results."""
    input_ch: Channel = Channel(buffer_size=32)
    output_ch: Channel = Channel(buffer_size=32)

    async def feed():
        for tag, data in node._input_stream.iter_data():
            await input_ch.writer.send((tag, data))
        await input_ch.writer.close()

    await asyncio.gather(
        feed(),
        node.async_execute(input_ch.reader, output_ch.writer),
    )
    return await output_ch.reader.collect()


# ---------------------------------------------------------------------------
# No-DB path
# ---------------------------------------------------------------------------


class TestFunctionJobNodeAsyncExecuteNoDB:
    @pytest.mark.asyncio
    async def test_all_items_processed_and_forwarded(self):
        """All input items reach the output channel."""
        node = _make_no_db_node(n=3)
        results = await _run_node(node)
        assert len(results) == 3
        values = sorted(data.as_dict()["result"] for _, data in results)
        assert values == [0, 2, 4]

    @pytest.mark.asyncio
    async def test_output_channel_closed_after_execution(self):
        """Output channel is closed even if execution completes without error."""
        node = _make_no_db_node(n=2)
        input_ch: Channel = Channel(buffer_size=8)
        output_ch: Channel = Channel(buffer_size=8)

        async def feed():
            for tag, data in node._input_stream.iter_data():
                await input_ch.writer.send((tag, data))
            await input_ch.writer.close()

        await asyncio.gather(feed(), node.async_execute(input_ch.reader, output_ch.writer))
        # collect() should return immediately (channel closed), not hang.
        results = await output_ch.reader.collect()
        assert isinstance(results, list)

    @pytest.mark.asyncio
    async def test_max_concurrency_limits_tasks(self):
        """With max_concurrency=1, at most one task runs at a time."""
        concurrent_count = 0
        max_observed = 0

        async def slow_double(value: int) -> int:
            nonlocal concurrent_count, max_observed
            concurrent_count += 1
            max_observed = max(max_observed, concurrent_count)
            await asyncio.sleep(0.01)
            concurrent_count -= 1
            return value * 2

        pf = PythonDataFunction(slow_double, output_keys="result")
        # Patch async_call to be the async function
        import functools
        original_async_call = pf.async_call

        async def patched_async_call(data, **kwargs):
            nonlocal concurrent_count, max_observed
            concurrent_count += 1
            max_observed = max(max_observed, concurrent_count)
            await asyncio.sleep(0.01)
            concurrent_count -= 1
            return await original_async_call(data, **kwargs)

        pf.async_call = patched_async_call  # type: ignore[method-assign]

        src = _make_source(5)
        pod = FunctionPod(pf, pod_config=PodConfig(max_concurrency=1))
        node = FunctionJobNode(pod, src)
        results = await _run_node(node)
        assert len(results) == 5
        assert max_observed <= 1
```

- [ ] **Step 2: Run the tests to confirm they pass (they should with existing code)**

```bash
uv run pytest tests/test_core/nodes/test_function_job_node_async_execute.py::TestFunctionJobNodeAsyncExecuteNoDB -v
```

Expected: all 3 tests PASS (existing code already handles no-DB). If any fail, investigate before
proceeding.

- [ ] **Step 3: Replace the no-DB path in `FunctionJobNode.async_execute()` with delegation**

In `src/orcapod/core/nodes/function_node.py`, find the else branch at lines 2146–2166:

```python
else:
    # Simple async execution without DB
    async with asyncio.TaskGroup() as tg:
        async for tag, data in input_channel:
            async def _guarded_simple(
                t: TagProtocol = tag, p: DataProtocol = data
            ) -> None:
                try:
                    await self._async_execute_one_data(
                        t, p, output,
                        observer=ctx_obs,
                        node_label=node_label,
                        node_hash=node_hash,
                    )
                finally:
                    if sem is not None:
                        sem.release()

            if sem is not None:
                await sem.acquire()
            tg.create_task(_guarded_simple())
```

Replace the entire else block with:

```python
else:
    # No-DB path: delegate dispatch entirely to the pod.
    await self._function_pod.async_execute(
        [input_channel], output, observer=ctx_obs
    )
    return  # output is already closed by the pod's finally; skip outer finally
```

Wait — there is a problem here. The outer `finally: await output.close()` will try to close the
output channel a second time (it was already closed by `_function_pod.async_execute()`). Looking
at `_ChannelWriter.close()`:

```python
async def close(self) -> None:
    if not self._channel._closed.is_set():
        self._channel._closed.set()
        await self._channel._queue.put(_CLOSED)
```

It's idempotent (guarded by `_closed.is_set()`). Double-close is safe. So `return` is not needed;
just replace the else block:

```python
else:
    # No-DB path: delegate dispatch entirely to the pod.
    # The pod's async_execute() closes `output` in its own finally.
    await self._function_pod.async_execute(
        [input_channel], output, observer=ctx_obs
    )
```

The outer `finally: await output.close()` will be a no-op since the channel is already closed.

Also remove the semaphore setup that now only applies to the DB path (lines 2089–2097):

```python
# REMOVE these lines from async_execute():
pod_config = getattr(self._function_pod, "pod_config", PodConfig())
max_concurrency = resolve_concurrency(pod_config, PipelineConfig())

sem = (
    asyncio.Semaphore(max_concurrency)
    if max_concurrency is not None
    else None
)
```

- [ ] **Step 4: Delete `_async_execute_one_data`**

Find and delete the entire `_async_execute_one_data` method (lines 2172–2201):

```python
async def _async_execute_one_data(
    self,
    tag: TagProtocol,
    data: DataProtocol,
    output: "WritableChannel[tuple[TagProtocol, DataProtocol]]",
    *,
    observer: ExecutionObserverProtocol,
    node_label: str,
    node_hash: str,
) -> None:
    ...
```

Delete this method entirely.

- [ ] **Step 5: Remove dead imports from `function_node.py`**

After removing the semaphore setup from `async_execute()`, `PipelineConfig`, `PodConfig`, and
`resolve_concurrency` are no longer referenced in `function_node.py`. Remove them from the import:

Before:
```python
from orcapod.types import (
    ColumnConfig,
    ContentHash,
    NodeConfig,
    PipelineConfig,
    PodConfig,
    Schema,
    resolve_concurrency,
)
```

After:
```python
from orcapod.types import (
    ColumnConfig,
    ContentHash,
    NodeConfig,
    Schema,
)
```

Verify by running a grep to confirm neither `PipelineConfig`, `PodConfig`, nor `resolve_concurrency`
appear elsewhere in the file:

```bash
grep -n "PipelineConfig\|PodConfig\|resolve_concurrency" \
    src/orcapod/core/nodes/function_node.py
```

Expected: no output.

- [ ] **Step 6: Run all tests**

```bash
uv run pytest tests/ -v --timeout=30
```

Expected: all tests still pass, including `TestFunctionJobNodeAsyncExecuteNoDB`.

- [ ] **Step 7: Commit**

```bash
git add src/orcapod/core/nodes/function_node.py \
        tests/test_core/nodes/test_function_job_node_async_execute.py
git commit -m "refactor(function_node): delegate no-DB async_execute to pod

FunctionJobNode.async_execute() no longer reimplements the semaphore
+TaskGroup dispatch loop for the no-DB path. It delegates directly to
self._function_pod.async_execute(), matching the OperatorNode pattern.
Deletes _async_execute_one_data (dead after delegation). Removes the
now-dead PipelineConfig, PodConfig, and resolve_concurrency imports."
```

---

## Task 3: `FunctionJobNode.async_execute()` — DB 3-stage concurrent pipeline

Redesign the DB path into three concurrent coroutines:

1. **`route_inputs`** — routes cache hits directly to output, stamps cache misses with a
   correlation key and forwards to `compute_channel`.
2. **`execution_pod.async_execute()`** — reads `compute_channel`, writes `result_channel`
   (this is now `_FunctionPodBase.async_execute()`).
3. **`record_and_forward`** — reads `result_channel`, calls `add_pipeline_record`, strips the
   correlation key from the output tag, writes to `output`.

Covers both the persistent (`_cached_function_pod`, `is_ephemeral=False`) and ephemeral
(`_ephemeral_cached_pod`, `is_ephemeral=True`) sub-paths — only the pod and the flag differ.

**Files:**
- Modify: `src/orcapod/core/nodes/function_node.py`
- Modify: `tests/test_core/nodes/test_function_job_node_async_execute.py`

- [ ] **Step 1: Write failing tests for the DB path**

Add the following test classes to `tests/test_core/nodes/test_function_job_node_async_execute.py`.

First, add the `_TAG_NODE_INPUT_REF` constant import at the top of the test file so we can assert
the correlation key is absent:

```python
# Add to imports at the top:
from orcapod.core.nodes.function_node import _TAG_NODE_INPUT_REF  # private constant
from orcapod.system_constants import constants as _constants
```

Then add these test classes at the bottom of the file:

```python
# ---------------------------------------------------------------------------
# Helpers — observer spy
# ---------------------------------------------------------------------------


class _SpyObserver:
    """Captures all observer hook invocations."""

    def __init__(self):
        self.node_starts: list[str] = []
        self.node_ends: list[str] = []
        self.data_starts: list[tuple] = []
        self.data_ends: list[tuple] = []    # (label, tag, input_data, output_data, cached)
        self.data_crashes: list[tuple] = []

    def contextualize(self, *path):
        return self

    def on_node_start(self, node_label, node_hash, *, tag_schema=None):
        self.node_starts.append(node_label)

    def on_node_end(self, node_label, node_hash):
        self.node_ends.append(node_label)

    def on_data_start(self, node_label, tag, data):
        self.data_starts.append((node_label, tag, data))

    def on_data_end(self, node_label, tag, input_data, output_data, *, cached):
        self.data_ends.append((node_label, tag, input_data, output_data, cached))

    def on_data_crash(self, node_label, tag, data, error):
        self.data_crashes.append((node_label, tag, data, error))

    def create_data_logger(self, tag, data):
        return None


# ---------------------------------------------------------------------------
# Helpers — DB node fixtures
# ---------------------------------------------------------------------------


def _make_db_node(n: int = 3) -> tuple[FunctionJobNode, InMemoryArrowDatabase, InMemoryArrowDatabase]:
    """FunctionJobNode with persistent pipeline + result databases."""
    src = _make_source(n)
    pod = FunctionPod(
        PythonDataFunction(lambda value: value * 2, output_keys="result"),
    )
    pipeline_db = InMemoryArrowDatabase()
    result_db = InMemoryArrowDatabase()
    node = FunctionJobNode(
        pod, src, pipeline_database=pipeline_db, result_database=result_db
    )
    return node, pipeline_db, result_db


# ---------------------------------------------------------------------------
# DB path — basic flow
# ---------------------------------------------------------------------------


class TestFunctionJobNodeAsyncExecuteDB:

    @pytest.mark.asyncio
    async def test_all_items_processed_and_forwarded(self):
        """All input items are computed and forwarded via the DB path."""
        node, pipeline_db, result_db = _make_db_node(3)
        results = await _run_node(node)
        assert len(results) == 3
        values = sorted(data.as_dict()["result"] for _, data in results)
        assert values == [0, 2, 4]

    @pytest.mark.asyncio
    async def test_pipeline_records_written_for_each_item(self):
        """add_pipeline_record is called once per cache miss → one row per item."""
        node, pipeline_db, _ = _make_db_node(3)
        await _run_node(node)
        records = pipeline_db.get_all_records(node.node_identity_path)
        assert records is not None
        assert records.num_rows == 3

    @pytest.mark.asyncio
    async def test_correlation_key_absent_from_output_tags(self):
        """Output tags must not contain the internal _tag_node_input_ref column."""
        node, _, _ = _make_db_node(3)
        results = await _run_node(node)
        full_key = f"{_constants.META_PREFIX}{_TAG_NODE_INPUT_REF}"
        for tag, _ in results:
            meta = tag.get_meta_info()
            assert full_key not in meta, (
                f"Output tag leaked internal correlation key {full_key!r}"
            )

    @pytest.mark.asyncio
    async def test_cache_hits_emitted_without_recomputation(self):
        """Items already in the pipeline DB are served from cache (no recomputation)."""
        node, _, _ = _make_db_node(3)

        # First run — populates both pipeline DB and result DB.
        first_results = await _run_node(node)
        assert len(first_results) == 3

        # Reset in-memory state so async_execute re-runs from scratch.
        node.clear_cache()

        call_count = 0
        original_process = node._function_pod.data_function.call

        def counted_call(data, **kw):
            nonlocal call_count
            call_count += 1
            return original_process(data, **kw)

        node._function_pod.data_function.call = counted_call  # type: ignore[method-assign]

        second_results = await _run_node(node)
        assert len(second_results) == 3
        # All 3 were hits — data function should NOT have been called again.
        assert call_count == 0

    @pytest.mark.asyncio
    async def test_observer_cached_false_for_misses_cached_true_for_hits(self):
        """Cache hits emit on_data_end(cached=True); misses emit on_data_end(cached=False)."""
        node, _, _ = _make_db_node(2)

        spy = _SpyObserver()

        # First run — both are misses.
        await _run_node(node)  # populates DB
        node.clear_cache()

        spy = _SpyObserver()
        input_ch: Channel = Channel(buffer_size=8)
        output_ch: Channel = Channel(buffer_size=8)

        async def feed():
            for tag, data in node._input_stream.iter_data():
                await input_ch.writer.send((tag, data))
            await input_ch.writer.close()

        await asyncio.gather(
            feed(),
            node.async_execute(input_ch.reader, output_ch.writer, observer=spy),
        )
        await output_ch.reader.collect()

        # Second run was all hits.
        assert len(spy.data_ends) == 2
        assert all(cached is True for _, _, _, _, cached in spy.data_ends)

    @pytest.mark.asyncio
    async def test_semaphore_not_leaked_on_computation_crash(self):
        """When a data function raises, processing continues for remaining items."""
        call_count = 0

        def sometimes_fail(value: int) -> int:
            nonlocal call_count
            call_count += 1
            if value == 1:
                raise ValueError("deliberate failure")
            return value * 2

        src = _make_source(3)
        pod = FunctionPod(
            PythonDataFunction(sometimes_fail, output_keys="result"),
            pod_config=PodConfig(max_concurrency=1),
        )
        pipeline_db = InMemoryArrowDatabase()
        result_db = InMemoryArrowDatabase()
        node = FunctionJobNode(pod, src, pipeline_database=pipeline_db, result_database=result_db)

        results = await _run_node(node)
        # value=0 → 0, value=1 → crash (skipped), value=2 → 4
        assert len(results) == 2
        values = sorted(data.as_dict()["result"] for _, data in results)
        assert values == [0, 4]


# ---------------------------------------------------------------------------
# DB path — ephemeral
# ---------------------------------------------------------------------------


class TestFunctionJobNodeAsyncExecuteEphemeral:

    @pytest.mark.asyncio
    async def test_ephemeral_pipeline_records_have_is_ephemeral_true(self):
        """Ephemeral execution writes pipeline records with is_ephemeral=True."""
        from orcapod.system_constants import constants
        from orcapod.types import NodeConfig

        src = _make_source(2)
        pod = FunctionPod(
            PythonDataFunction(lambda value: value * 2, output_keys="result"),
        )
        pipeline_db = InMemoryArrowDatabase()
        result_db = InMemoryArrowDatabase()
        ephemeral_db = InMemoryArrowDatabase()
        node = FunctionJobNode(
            pod, src, pipeline_database=pipeline_db, result_database=result_db
        )
        node.node_config = NodeConfig(is_result_ephemeral=True)
        node.set_ephemeral_store(ephemeral_db)

        results = await _run_node(node)
        assert len(results) == 2

        records = pipeline_db.get_all_records(node.node_identity_path)
        assert records is not None
        assert records.num_rows == 2
        # All rows should have is_ephemeral=True
        is_eph_col = records.column(constants.IS_EPHEMERAL_COL).to_pylist()
        assert all(v is True for v in is_eph_col)

    @pytest.mark.asyncio
    async def test_ephemeral_missing_store_raises_runtime_error(self):
        """Raising RuntimeError when is_result_ephemeral=True but no store set."""
        from orcapod.types import NodeConfig

        src = _make_source(1)
        pod = FunctionPod(
            PythonDataFunction(lambda value: value * 2, output_keys="result"),
        )
        pipeline_db = InMemoryArrowDatabase()
        result_db = InMemoryArrowDatabase()
        node = FunctionJobNode(
            pod, src, pipeline_database=pipeline_db, result_database=result_db
        )
        node.node_config = NodeConfig(is_result_ephemeral=True)
        # Deliberately do NOT call set_ephemeral_store()

        with pytest.raises(RuntimeError, match="ephemeral"):
            await _run_node(node)
```

- [ ] **Step 2: Run the tests to confirm they fail**

```bash
uv run pytest tests/test_core/nodes/test_function_job_node_async_execute.py::TestFunctionJobNodeAsyncExecuteDB \
             tests/test_core/nodes/test_function_job_node_async_execute.py::TestFunctionJobNodeAsyncExecuteEphemeral \
             -v
```

Expected: most or all tests FAIL because the 3-stage pipeline isn't implemented yet.
The test importing `_TAG_NODE_INPUT_REF` will cause an `ImportError` — that's expected.

- [ ] **Step 3: Add the `_TAG_NODE_INPUT_REF` module constant to `function_node.py`**

Near the top of `function_node.py` where the other private constants are defined (after
`_PIPELINE_RECOMPUTATION_INDEX_COL`), add:

```python
# Private meta-column name stamped into tags routed to the compute channel.
# Carries the correlation key (bytes) that links an execution result back to
# its original (tag, input_data) pair.  Stripped from output tags in
# record_and_forward() before downstream emission.
_TAG_NODE_INPUT_REF = "_tag_node_input_ref"
```

- [ ] **Step 4: Implement the 3-stage DB pipeline in `FunctionJobNode.async_execute()`**

Find the DB path block in `async_execute()` (the `if self._cached_function_pod is not None:` branch,
starting around line 2103 after earlier edits). Replace the entire block with the 3-stage pipeline.

The complete new `async_execute()` method body (replacing from after the UNAVAILABLE check to the
final `finally`) is:

```python
        from orcapod.pipeline.observer import NoOpObserver

        node_label = self.label
        node_hash = self.content_hash().to_string()

        obs = observer if observer is not None else NoOpObserver()
        ctx_obs = obs.contextualize(*self.node_identity_path)

        try:
            tag_schema = self._input_stream.output_schema(columns={"system_tags": True})[0]
            ctx_obs.on_node_start(node_label, node_hash, tag_schema=tag_schema)

            if self._cached_function_pod is not None:
                # ----------------------------------------------------------
                # DB path — 3-stage concurrent pipeline
                # ----------------------------------------------------------
                is_ephemeral = bool(self._node_config.is_result_ephemeral)
                if is_ephemeral:
                    if self._ephemeral_cached_pod is None:
                        raise RuntimeError(
                            f"FunctionJobNode '{self.label}' has is_result_ephemeral=True "
                            "but no ephemeral store has been assigned. Call "
                            "set_ephemeral_store() with an ArrowDatabaseProtocol before "
                            "executing this node."
                        )
                    execution_pod = self._ephemeral_cached_pod
                else:
                    execution_pod = self._cached_function_pod

                # Phase 1: load pipeline DB → in-memory cache keyed by base_entry_id.
                loaded = self._load_cached_entries()
                self._cached_output_datas.update(loaded)
                if loaded:
                    self._cached_output_table = None
                    self._cached_content_hash_column = None
                cached_by_base_entry_id: dict[bytes, tuple[TagProtocol, DataProtocol]] = dict(loaded)

                # Intermediate channels (bounded for backpressure).
                compute_channel: Channel[tuple[TagProtocol, DataProtocol]] = Channel(buffer_size=16)
                result_channel: Channel[tuple[TagProtocol, DataProtocol]] = Channel(buffer_size=16)

                # Local dict: correlation_key → (original_tag, original_input_data)
                input_store: dict[bytes, tuple[TagProtocol, DataProtocol]] = {}

                async def route_inputs() -> None:
                    """Stage 1: send cache hits to output; stamp misses for computation."""
                    try:
                        async for tag, data in input_channel:
                            base_entry_id = self.compute_base_entry_id(tag, data)
                            if base_entry_id in cached_by_base_entry_id:
                                cached_tag, cached_data = cached_by_base_entry_id[base_entry_id]
                                ctx_obs.on_data_start(node_label, tag, data)
                                ctx_obs.on_data_end(
                                    node_label, tag, data, cached_data, cached=True
                                )
                                await output.send((cached_tag, cached_data))
                            else:
                                correlation_key = uuid.uuid4().bytes
                                input_store[correlation_key] = (tag, data)
                                stamped_tag = tag.with_meta_columns(
                                    **{_TAG_NODE_INPUT_REF: correlation_key}
                                )
                                await compute_channel.writer.send((stamped_tag, data))
                    finally:
                        await compute_channel.writer.close()

                async def record_and_forward() -> None:
                    """Stage 3: record pipeline entry, strip key, emit to output."""
                    async for output_tag, output_data in result_channel.reader:
                        correlation_key = output_tag.get_meta_value(_TAG_NODE_INPUT_REF)
                        original_tag, input_data = input_store.pop(correlation_key)
                        result_computed = bool(
                            output_data.get_meta_value(
                                execution_pod.RESULT_COMPUTED_FLAG, False
                            )
                        )
                        self.add_pipeline_record(
                            original_tag,
                            input_data,
                            data_record_id=output_data.datagram_uuid,
                            computed=result_computed,
                            is_ephemeral=is_ephemeral,
                        )
                        # Update in-memory cache so iter_data() sees the result.
                        base_entry_id = self.compute_base_entry_id(original_tag, input_data)
                        clean_tag = output_tag.drop_meta_columns(_TAG_NODE_INPUT_REF)
                        self._cached_output_datas[base_entry_id] = (clean_tag, output_data)
                        self._cached_output_table = None
                        self._cached_content_hash_column = None
                        await output.send((clean_tag, output_data))

                async with asyncio.TaskGroup() as tg:
                    tg.create_task(route_inputs())
                    tg.create_task(
                        execution_pod.async_execute(
                            [compute_channel.reader],
                            result_channel.writer,
                            observer=ctx_obs,
                        )
                    )
                    tg.create_task(record_and_forward())

            else:
                # No-DB path: delegate dispatch entirely to the pod.
                await self._function_pod.async_execute(
                    [input_channel], output, observer=ctx_obs
                )

            ctx_obs.on_node_end(node_label, node_hash)
        finally:
            await output.close()
```

**Important:** The `Channel` type needs to be imported in `function_node.py`. Verify the import
already exists (it should since it's in `orcapod.channels`):

```python
from orcapod.channels import ReadableChannel, WritableChannel
```

You also need `Channel` itself for the intermediate channels. Add it:

```python
from orcapod.channels import Channel, ReadableChannel, WritableChannel
```

- [ ] **Step 5: Run all tests**

```bash
uv run pytest tests/ -v --timeout=30
```

Expected: all tests pass, including all new DB and ephemeral tests.

If `test_cache_hits_emitted_without_recomputation` fails, check that `clear_cache()` properly
resets `_cached_output_datas` (it should — it's an existing method).

If `test_correlation_key_absent_from_output_tags` fails, check that `drop_meta_columns` is
called with the right key in `record_and_forward`.

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/core/nodes/function_node.py \
        tests/test_core/nodes/test_function_job_node_async_execute.py
git commit -m "feat(function_node): redesign async_execute as 3-stage concurrent pipeline

FunctionJobNode.async_execute() now delegates computation to
execution_pod.async_execute() (CachedFunctionPod inheriting the loop
from _FunctionPodBase) via bounded compute_channel/result_channel.
Three asyncio.TaskGroup tasks run concurrently:
  1. route_inputs: cache hits → output, misses → compute_channel (stamped)
  2. execution_pod.async_execute: compute_channel → result_channel
  3. record_and_forward: add_pipeline_record, strip key, emit to output
Both persistent and ephemeral DB sub-paths use the same pipeline.
Adds _TAG_NODE_INPUT_REF module constant (private, never in public API).
Imports Channel for intermediate channels."
```

---

## Task 4: Cleanup — remove orphan test, relocate new observer tests

The `TestAsyncExecuteBackpressure` in `test_regression_fixes.py` tested
`FunctionPod.async_execute()` in isolation — a method that was an orphan (never called by
the orchestrator). Its equivalent coverage now lives in `TestFunctionJobNodeAsyncExecuteNoDB.test_max_concurrency_limits_tasks`. Remove it.

Also relocate the `TestFunctionPodBaseAsyncExecuteObserver` test added in Task 1 from
`test_regression_fixes.py` to its natural home (or keep in place — it tests pod-level observer
behavior, and `test_regression_fixes.py` is an acceptable home). Move it to the dedicated file
for cleanliness.

**Files:**
- Modify: `tests/test_core/test_regression_fixes.py`
- Modify: `tests/test_core/nodes/test_function_job_node_async_execute.py`

- [ ] **Step 1: Remove `TestAsyncExecuteBackpressure` from `test_regression_fixes.py`**

Find and delete the entire `TestAsyncExecuteBackpressure` class (lines 293–343 in the original
file, may have shifted). It begins with:

```python
class TestAsyncExecuteBackpressure:
    """With max_concurrency set, pending tasks should be bounded."""
    ...
```

Delete from the class definition through the end of the last test in the class.

Also remove the section header comment above it:
```python
# ===========================================================================
# 4. FunctionPod.async_execute backpressure bounds pending tasks
# ===========================================================================
```

Update the module-level docstring to remove item 4:

```python
"""
Regression tests for bugs fixed in the data-function-executor-system branch.

Covers:
1. async_execute output channel closed on exception (try/finally)
2. DataFunctionWrapper.direct_call/direct_async_call bypass executor routing
3. Concurrent iteration falls back to sequential inside a running event loop
4. _materialize_to_stream preserves source_info provenance tokens
5. RayExecutor._ensure_ray_initialized uses ray_address
6. DataFunctionExecutorProtocol uses DataFunctionProtocol (not Any)
"""
```

(Renumber items 5-7 → 4-6.)

- [ ] **Step 2: Move `TestFunctionPodBaseAsyncExecuteObserver` to the dedicated test file**

Remove `TestFunctionPodBaseAsyncExecuteObserver` from the bottom of `test_regression_fixes.py`
(added in Task 1).

Add it to `tests/test_core/nodes/test_function_job_node_async_execute.py` — or optionally to
a separate `tests/test_core/function_pod/test_function_pod_base_async_execute.py`. Since these
tests test pod behavior (not node behavior), place them in a new file:

Create `tests/test_core/function_pod/test_function_pod_async_execute.py`:

```python
"""Tests for _FunctionPodBase.async_execute() — observer hooks and backpressure."""

from __future__ import annotations

import asyncio

import pyarrow as pa
import pytest

from orcapod.channels import Channel
from orcapod.core.data_function import PythonDataFunction
from orcapod.core.function_pod import FunctionPod
from orcapod.core.streams.arrow_table_stream import ArrowTableStream
from orcapod.types import PodConfig


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_stream(n: int = 3) -> ArrowTableStream:
    schema = pa.schema(
        [pa.field("id", pa.int64(), nullable=False), pa.field("x", pa.int64(), nullable=False)]
    )
    table = pa.table(
        {
            "id": pa.array(list(range(n)), type=pa.int64()),
            "x": pa.array(list(range(n)), type=pa.int64()),
        },
        schema=schema,
    )
    return ArrowTableStream(table, tag_columns=["id"])


async def _feed(stream: ArrowTableStream, ch: Channel) -> None:
    for tag, data in stream.iter_data():
        await ch.writer.send((tag, data))
    await ch.writer.close()


class _SpyObserver:
    def __init__(self):
        self.starts = []
        self.ends = []   # [(label, tag, input, output, cached)]
        self.crashes = []

    def contextualize(self, *path):
        return self

    def on_node_start(self, *a, **kw): pass
    def on_node_end(self, *a, **kw): pass

    def on_data_start(self, label, tag, data):
        self.starts.append((label, tag, data))

    def on_data_end(self, label, tag, inp, out, *, cached):
        self.ends.append((label, tag, inp, out, cached))

    def on_data_crash(self, label, tag, data, exc):
        self.crashes.append(exc)

    def create_data_logger(self, tag, data):
        return None


# ---------------------------------------------------------------------------
# Observer hooks
# ---------------------------------------------------------------------------


class TestFunctionPodBaseAsyncExecuteObserver:
    @pytest.mark.asyncio
    async def test_on_data_start_and_end_fire_per_item(self):
        """on_data_start fires before and on_data_end(cached=False) after each item."""
        pf = PythonDataFunction(lambda x: x * 2, output_keys="result")
        pod = FunctionPod(pf)
        stream = _make_stream(3)
        input_ch: Channel = Channel(buffer_size=16)
        output_ch: Channel = Channel(buffer_size=16)
        await _feed(stream, input_ch)

        spy = _SpyObserver()
        await pod.async_execute([input_ch.reader], output_ch.writer, observer=spy)
        results = await output_ch.reader.collect()

        assert len(results) == 3
        assert len(spy.starts) == 3
        assert len(spy.ends) == 3
        assert all(cached is False for _, _, _, _, cached in spy.ends)

    @pytest.mark.asyncio
    async def test_on_data_crash_fires_not_on_data_end(self):
        """on_data_crash fires (and on_data_end does not) when processing raises."""
        def boom(x: int) -> int:
            raise ValueError("bang")

        pf = PythonDataFunction(boom, output_keys="result")
        pod = FunctionPod(pf)
        stream = _make_stream(2)
        input_ch: Channel = Channel(buffer_size=16)
        output_ch: Channel = Channel(buffer_size=16)
        await _feed(stream, input_ch)

        spy = _SpyObserver()
        await pod.async_execute([input_ch.reader], output_ch.writer, observer=spy)
        results = await output_ch.reader.collect()

        assert len(results) == 0
        assert len(spy.crashes) == 2
        assert len(spy.ends) == 0


# ---------------------------------------------------------------------------
# Backpressure
# ---------------------------------------------------------------------------


class TestFunctionPodBaseAsyncExecuteBackpressure:
    @pytest.mark.asyncio
    async def test_max_concurrency_limits_concurrent_tasks(self):
        """With max_concurrency=1, at most one task runs concurrently."""
        concurrent_count = 0
        max_observed = 0

        async def track(x: int) -> int:
            nonlocal concurrent_count, max_observed
            concurrent_count += 1
            max_observed = max(max_observed, concurrent_count)
            await asyncio.sleep(0.01)
            concurrent_count -= 1
            return x * 2

        pf = PythonDataFunction(lambda x: x * 2, output_keys="result")
        original_async_call = pf.async_call

        async def patched(data, **kw):
            nonlocal concurrent_count, max_observed
            concurrent_count += 1
            max_observed = max(max_observed, concurrent_count)
            await asyncio.sleep(0.01)
            concurrent_count -= 1
            return await original_async_call(data, **kw)

        pf.async_call = patched  # type: ignore[method-assign]

        pod = FunctionPod(pf, pod_config=PodConfig(max_concurrency=1))
        stream = _make_stream(5)
        input_ch: Channel = Channel(buffer_size=32)
        output_ch: Channel = Channel(buffer_size=32)
        await _feed(stream, input_ch)
        await pod.async_execute([input_ch.reader], output_ch.writer)
        results = await output_ch.reader.collect()
        assert len(results) == 5
        assert max_observed <= 1
```

- [ ] **Step 3: Run the full test suite**

```bash
uv run pytest tests/ -v --timeout=30
```

Expected: all tests pass. Verify:
- `TestAsyncExecuteBackpressure` is gone
- `TestFunctionPodBaseAsyncExecuteBackpressure` in the new file covers equivalent behavior
- All `TestFunctionJobNodeAsyncExecuteDB` and `TestFunctionJobNodeAsyncExecuteEphemeral` pass

- [ ] **Step 4: Final import cleanup — verify `function_pod.py`**

The observer types are used only in the `TYPE_CHECKING` block. Confirm the import is present
and correct:

```bash
grep -n "ExecutionObserverProtocol" src/orcapod/core/function_pod.py
```

Expected: one line in the `TYPE_CHECKING` block.

Also verify `PipelineConfig`, `PodConfig`, `resolve_concurrency` are still present in
`function_pod.py` (they ARE used there — `async_execute` on `_FunctionPodBase` still uses them):

```bash
grep -n "PipelineConfig\|resolve_concurrency" src/orcapod/core/function_pod.py
```

Expected: lines referencing both in the import and in `async_execute`.

- [ ] **Step 5: Commit**

```bash
git add tests/test_core/test_regression_fixes.py \
        tests/test_core/nodes/test_function_job_node_async_execute.py \
        tests/test_core/function_pod/test_function_pod_async_execute.py
git commit -m "test(async_execute): remove orphan backpressure test, add dedicated coverage

Removes TestAsyncExecuteBackpressure from test_regression_fixes.py
(tested an orphan FunctionPod method, never called by orchestrator).
Equivalent coverage lives in TestFunctionJobNodeAsyncExecuteNoDB and
TestFunctionPodBaseAsyncExecuteBackpressure. Adds:
- tests/test_core/function_pod/test_function_pod_async_execute.py:
  observer hooks and backpressure via _FunctionPodBase.async_execute()
- tests/test_core/nodes/test_function_job_node_async_execute.py:
  full FunctionJobNode.async_execute() coverage (no-DB, DB, ephemeral)"
```

---

## Self-Review

### Spec coverage

| Spec requirement | Task |
|---|---|
| `async_execute()` moves to `_FunctionPodBase` | Task 1 |
| `observer` parameter on `_FunctionPodBase.async_execute()` | Task 1 |
| `on_data_start`, `on_data_end`, `on_data_crash` per item | Task 1 |
| No-DB path delegates to `self._function_pod.async_execute()` | Task 2 |
| Delete orphan `_async_execute_one_data` | Task 2 |
| Dead import cleanup (PipelineConfig, PodConfig, resolve_concurrency) | Task 2 |
| `_TAG_NODE_INPUT_REF` private constant in `function_node.py` | Task 3 |
| 3-stage concurrent pipeline (route → compute → record) | Task 3 |
| route_inputs: cache hits → output, misses → compute_channel | Task 3 |
| record_and_forward: add_pipeline_record, strip key, emit | Task 3 |
| Bounded `compute_channel` + `result_channel` | Task 3 |
| Ephemeral path uses `_ephemeral_cached_pod`, `is_ephemeral=True` | Task 3 |
| RuntimeError guard when ephemeral=True but no store set | Task 3 |
| Output tags carry no `_tag_node_input_ref` | Task 3 (tested) |
| `add_pipeline_record` called once per cache miss | Task 3 (tested) |
| Remove `TestAsyncExecuteBackpressure` | Task 4 |
| New test file `test_function_job_node_async_execute.py` | Tasks 2–4 |
| CACHE_ONLY / UNAVAILABLE modes unchanged | ✓ preserved (no changes to those branches) |

### Key invariants verification

1. **Output tags carry no `_tag_node_input_ref`** — verified by `test_correlation_key_absent_from_output_tags`
2. **`add_pipeline_record` called once per cache miss** — verified by `test_pipeline_records_written_for_each_item`
3. **Cache hits bypass computation** — verified by `test_cache_hits_emitted_without_recomputation`
4. **`_FunctionPodBase.async_execute()` is unaware of pipeline DB** — architectural constraint, no DB imports in function_pod.py
5. **No-DB path uses `_function_pod`** — enforced by code structure (else branch)
6. **Ephemeral path uses `_ephemeral_cached_pod` + `is_ephemeral=True`** — verified by `test_ephemeral_pipeline_records_have_is_ephemeral_true`
7. **Persistent path uses `_cached_function_pod` + `is_ephemeral=False`** — verified by `test_pipeline_records_written_for_each_item`
