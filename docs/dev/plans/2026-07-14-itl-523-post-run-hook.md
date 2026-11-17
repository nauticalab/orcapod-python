# Post-Run Hook for Function Pods — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a per-pod post-run hook mechanism to `FunctionPod` so registered callables fire after every invocation with a typed payload describing what ran.

**Architecture:** New types live in `src/orcapod/hooks.py`. `_FunctionPodBase` gains a mutable hook list plus an `_invoke_with_hooks()` wrapper method; all call sites that previously called `process_data()` directly now call `_invoke_with_hooks()` instead. `CachedFunctionPod` overrides `_invoke_with_hooks()` to supply the correct `InvocationStatus.HIT` when a database cache hit occurs.

**Tech Stack:** Python 3.11+, PyArrow, pytest, uv (all commands via `uv run`)

**Spec:** `superpowers/specs/2026-07-14-itl-523-post-run-hook-design.md`

---

## File Map

| Action | Path | Responsibility |
|--------|------|----------------|
| **Create** | `src/orcapod/hooks.py` | All public hook types: `InvocationStatus`, `RunStats`, `PodContext`, `PostRunPayload`, `PostRunHookFn`, `HookConfig`, `PostRunHook` |
| **Modify** | `src/orcapod/core/function_pod.py` | Add `_post_run_hooks`, `add_post_run_hook()`, `_fire_post_run_hooks()`, `_invoke_with_hooks()`, `_async_invoke_with_hooks()` to `_FunctionPodBase`; update call sites in `FunctionPodStream._iter_data_sequential`, `_iter_data_concurrent`, and `async_execute` |
| **Modify** | `src/orcapod/core/cached_function_pod.py` | Override `_invoke_with_hooks()` and `_async_invoke_with_hooks()` to detect `InvocationStatus.HIT` via `RESULT_COMPUTED_FLAG` |
| **Modify** | `src/orcapod/__init__.py` | Re-export all public hook types |
| **Create** | `tests/test_core/function_pod/test_post_run_hooks.py` | Full test suite (10 test classes) |

---

## Task 1: Create `src/orcapod/hooks.py`

**Files:**
- Create: `src/orcapod/hooks.py`

- [ ] **Step 1: Create the hooks module**

```python
# src/orcapod/hooks.py
"""Post-run hook types for function pods.

Defines the payload, status, and hook configuration types used by the
post-run hook mechanism on function pods. Import these when writing or
registering hooks.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Callable
from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from orcapod.protocols.core_protocols import DataProtocol, TagProtocol


class InvocationStatus(str, Enum):
    """Status of a single function pod invocation.

    Attributes:
        COMPUTED: The function was invoked and produced a fresh result.
        HIT: The result was served from a pod-level database cache
            (``CachedFunctionPod``).
        ERROR: The function raised an exception.
    """

    COMPUTED = "computed"
    HIT = "hit"
    ERROR = "error"


@dataclasses.dataclass(frozen=True)
class RunStats:
    """Timing and status information for a single pod invocation.

    Attributes:
        duration_ms: Wall-clock time in milliseconds.
        status: Whether the result was freshly computed, a cache hit, or an error.
        started_at: UTC timestamp when the invocation started.
        finished_at: UTC timestamp when the invocation finished (after hooks fire).
        error: The exception raised, if ``status == ERROR``; ``None`` otherwise.
    """

    duration_ms: float
    status: InvocationStatus
    started_at: datetime
    finished_at: datetime
    error: Exception | None = None


@dataclasses.dataclass(frozen=True)
class PodContext:
    """Identity information about the pod that produced a result.

    Attributes:
        label: Human-readable pod label (``pod.label``); ``None`` if not set.
        pod_hash: Hex-string content hash of the pod (``pod.content_hash().to_string()``).
            Changes when the underlying function code or version changes.
    """

    label: str | None
    pod_hash: str


@dataclasses.dataclass(frozen=True)
class PostRunPayload:
    """Payload passed to every post-run hook after a pod invocation.

    Attributes:
        record_id_hash: String form of ``output.datagram_uuid`` — the same UUID
            used as the primary key when the result is stored in a backing database.
            ``None`` when ``output`` is ``None`` (filtered row or error).
        tag: The input tag for this invocation. Treat as read-only.
        input: The input data for this invocation. Treat as read-only.
        output: The output data; ``None`` if the function filtered the row out or raised.
        stats: Timing and status bundle.
        pod: Identity of the pod that produced this result.
    """

    record_id_hash: str | None
    tag: TagProtocol
    input: DataProtocol
    output: DataProtocol | None
    stats: RunStats
    pod: PodContext


PostRunHookFn = Callable[["PostRunPayload"], None]
"""A plain hook callable: ``(PostRunPayload) -> None``.

Defaults to fail-loud on error (exceptions propagate).
"""


@dataclasses.dataclass(frozen=True)
class HookConfig:
    """Hook callable with explicit error-handling behaviour.

    Use this instead of a plain callable when you want the hook to log and
    continue on failure rather than propagating the exception.

    Attributes:
        fn: The hook callable.
        on_error: ``"raise"`` (default) propagates exceptions; ``"log"`` logs at
            WARNING level and continues.

    Example:
        pod.add_post_run_hook(HookConfig(fn=my_hook, on_error="log"))
    """

    fn: PostRunHookFn
    on_error: Literal["raise", "log"] = "raise"


PostRunHook = PostRunHookFn | HookConfig
"""A hook is either a plain callable (fail-loud) or a ``HookConfig`` wrapper."""
```

- [ ] **Step 2: Verify module imports cleanly**

```bash
uv run python -c "from orcapod.hooks import PostRunPayload, HookConfig, InvocationStatus, RunStats, PodContext, PostRunHook, PostRunHookFn; print('OK')"
```

Expected: `OK`

- [ ] **Step 3: Commit**

```bash
cd /home/kurouto/kurouto-jobs/09c4ea39-6353-4e02-ba3a-a9b86b0e157b/orcapod-python
git add src/orcapod/hooks.py
git commit -m "feat(hooks): add PostRunPayload and hook types module (ITL-523)"
```

---

## Task 2: Hook infrastructure on `_FunctionPodBase` + sequential call site

Add `_post_run_hooks`, `add_post_run_hook`, `_fire_post_run_hooks`, and `_invoke_with_hooks` to `_FunctionPodBase`. Update `FunctionPodStream._iter_data_sequential` to call `_invoke_with_hooks`. This covers tests 1–4, 6, and 7.

**Files:**
- Create: `tests/test_core/function_pod/test_post_run_hooks.py`
- Modify: `src/orcapod/core/function_pod.py`

- [ ] **Step 1: Create the test file with failing tests**

```python
# tests/test_core/function_pod/test_post_run_hooks.py
"""Tests for FunctionPod post-run hooks (ITL-523).

Covers: registration, firing order, failure semantics, payload correctness,
filtered output, error status, cache hit status, parallel execution,
decorator convenience, and empty-hooks no-overhead path.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.core.cached_function_pod import CachedFunctionPod
from orcapod.core.data_function import PythonDataFunction
from orcapod.core.function_pod import FunctionPod, function_pod
from orcapod.core.streams.arrow_table_stream import ArrowTableStream
from orcapod.databases import InMemoryArrowDatabase
from orcapod.hooks import HookConfig, InvocationStatus, PostRunPayload


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_stream(n: int = 2) -> ArrowTableStream:
    schema = pa.schema([
        pa.field("id", pa.int64(), nullable=False),
        pa.field("x", pa.int64(), nullable=False),
    ])
    table = pa.table(
        {
            "id": pa.array(list(range(n)), type=pa.int64()),
            "x": pa.array(list(range(n)), type=pa.int64()),
        },
        schema=schema,
    )
    return ArrowTableStream(table, tag_columns=["id"])


def double(x: int) -> int:
    return x * 2


def _make_double_pod() -> FunctionPod:
    pf = PythonDataFunction(double, output_keys="result")
    return FunctionPod(pf)


# ---------------------------------------------------------------------------
# 1. Single hook fires with correct payload
# ---------------------------------------------------------------------------


class TestSingleHookPayload:
    def test_hook_fires_with_correct_payload(self):
        pod = _make_double_pod()
        payloads: list[PostRunPayload] = []
        pod.add_post_run_hook(payloads.append)

        stream = pod.process(_make_stream(n=1))
        list(stream.iter_data())

        assert len(payloads) == 1
        p = payloads[0]
        assert p.stats.status == InvocationStatus.COMPUTED
        assert p.stats.duration_ms >= 0
        assert p.stats.error is None
        assert p.output is not None
        assert p.record_id_hash == str(p.output.datagram_uuid)
        assert p.pod.label == pod.label
        assert p.pod.pod_hash == pod.content_hash().to_string()
        assert p.input is not None
        assert p.tag is not None

    def test_hook_fires_for_each_row(self):
        pod = _make_double_pod()
        payloads: list[PostRunPayload] = []
        pod.add_post_run_hook(payloads.append)

        stream = pod.process(_make_stream(n=3))
        list(stream.iter_data())

        assert len(payloads) == 3


# ---------------------------------------------------------------------------
# 2. Multiple hooks fire in registration order
# ---------------------------------------------------------------------------


class TestHookOrdering:
    def test_multiple_hooks_fire_in_order(self):
        pod = _make_double_pod()
        fired: list[str] = []
        pod.add_post_run_hook(lambda p: fired.append("first"))
        pod.add_post_run_hook(lambda p: fired.append("second"))

        stream = pod.process(_make_stream(n=1))
        list(stream.iter_data())

        assert fired == ["first", "second"]

    def test_hooks_fire_in_order_for_every_row(self):
        pod = _make_double_pod()
        fired: list[str] = []
        pod.add_post_run_hook(lambda p: fired.append("first"))
        pod.add_post_run_hook(lambda p: fired.append("second"))

        stream = pod.process(_make_stream(n=2))
        list(stream.iter_data())

        assert fired == ["first", "second", "first", "second"]


# ---------------------------------------------------------------------------
# 3. Fail-loud hook error
# ---------------------------------------------------------------------------


class TestHookFailureFast:
    def test_failing_hook_propagates_exception(self):
        pod = _make_double_pod()

        def bad_hook(p: PostRunPayload) -> None:
            raise ValueError("hook exploded")

        pod.add_post_run_hook(bad_hook)

        stream = pod.process(_make_stream(n=1))
        with pytest.raises(ValueError, match="hook exploded"):
            list(stream.iter_data())

    def test_failing_hook_stops_remaining_hooks(self):
        pod = _make_double_pod()
        second_fired: list[bool] = []

        def bad_hook(p: PostRunPayload) -> None:
            raise ValueError("stops here")

        pod.add_post_run_hook(bad_hook)
        pod.add_post_run_hook(lambda p: second_fired.append(True))

        stream = pod.process(_make_stream(n=1))
        with pytest.raises(ValueError):
            list(stream.iter_data())

        assert second_fired == []


# ---------------------------------------------------------------------------
# 4. Resilient hook error
# ---------------------------------------------------------------------------


class TestHookFailureResilient:
    def test_resilient_hook_suppresses_exception(self):
        pod = _make_double_pod()
        second_fired: list[bool] = []

        def bad_hook(p: PostRunPayload) -> None:
            raise ValueError("suppressed")

        def second_hook(p: PostRunPayload) -> None:
            second_fired.append(True)

        pod.add_post_run_hook(HookConfig(fn=bad_hook, on_error="log"))
        pod.add_post_run_hook(second_hook)

        stream = pod.process(_make_stream(n=1))
        results = list(stream.iter_data())

        assert results  # computation result returned
        assert second_fired == [True]  # next hook still fired

    def test_hookconfig_raise_is_same_as_plain_callable(self):
        pod = _make_double_pod()

        def bad_hook(p: PostRunPayload) -> None:
            raise ValueError("still loud")

        pod.add_post_run_hook(HookConfig(fn=bad_hook, on_error="raise"))

        stream = pod.process(_make_stream(n=1))
        with pytest.raises(ValueError, match="still loud"):
            list(stream.iter_data())


# ---------------------------------------------------------------------------
# 6. Error status (pod function raises)
# ---------------------------------------------------------------------------


class TestErrorStatus:
    def test_pod_error_fires_hook_with_error_status(self):
        def explodes(x: int) -> int:
            raise RuntimeError("boom")

        pf = PythonDataFunction(explodes, output_keys="result")
        pod = FunctionPod(pf)

        payloads: list[PostRunPayload] = []
        pod.add_post_run_hook(HookConfig(fn=payloads.append, on_error="log"))

        stream = pod.process(_make_stream(n=1))
        with pytest.raises(RuntimeError, match="boom"):
            list(stream.iter_data())

        assert len(payloads) == 1
        p = payloads[0]
        assert p.stats.status == InvocationStatus.ERROR
        assert isinstance(p.stats.error, RuntimeError)
        assert str(p.stats.error) == "boom"
        assert p.output is None
        assert p.record_id_hash is None

    def test_original_exception_reraises_after_hooks(self):
        def explodes(x: int) -> int:
            raise RuntimeError("original")

        pf = PythonDataFunction(explodes, output_keys="result")
        pod = FunctionPod(pf)
        pod.add_post_run_hook(HookConfig(fn=lambda p: None, on_error="log"))

        stream = pod.process(_make_stream(n=1))
        with pytest.raises(RuntimeError, match="original"):
            list(stream.iter_data())


# ---------------------------------------------------------------------------
# 7. Filtered output
# ---------------------------------------------------------------------------


class TestFilteredOutput:
    def test_filtered_row_fires_hook_with_none_output(self):
        def filter_all(x: int) -> int | None:
            return None

        pf = PythonDataFunction(filter_all, output_keys="result")
        pod = FunctionPod(pf)

        payloads: list[PostRunPayload] = []
        pod.add_post_run_hook(payloads.append)

        stream = pod.process(_make_stream(n=1))
        results = list(stream.iter_data())

        assert results == []
        assert len(payloads) == 1
        p = payloads[0]
        assert p.output is None
        assert p.record_id_hash is None
        assert p.stats.status == InvocationStatus.COMPUTED
        assert p.stats.error is None


# ---------------------------------------------------------------------------
# 10. Empty hooks — no overhead path
# ---------------------------------------------------------------------------


class TestEmptyHooks:
    def test_pod_with_no_hooks_has_empty_list(self):
        pod = _make_double_pod()
        assert pod._post_run_hooks == []

    def test_pod_with_no_hooks_processes_normally(self):
        pod = _make_double_pod()
        stream = pod.process(_make_stream(n=2))
        results = list(stream.iter_data())
        assert len(results) == 2
```

- [ ] **Step 2: Run the tests to confirm they fail with AttributeError**

```bash
uv run pytest tests/test_core/function_pod/test_post_run_hooks.py -x -q 2>&1 | head -20
```

Expected: `AttributeError: '_FunctionPodBase' object has no attribute 'add_post_run_hook'` (or similar)

- [ ] **Step 3: Add imports to `function_pod.py`**

At the top of `src/orcapod/core/function_pod.py`, add these imports after the existing import block (after line ~39):

```python
from datetime import datetime, timezone

from orcapod.hooks import (
    HookConfig,
    InvocationStatus,
    PodContext,
    PostRunPayload,
    PostRunHook,
    RunStats,
)
```

- [ ] **Step 4: Add `_post_run_hooks` initialisation to `_FunctionPodBase.__init__`**

In `_FunctionPodBase.__init__` (around line 76, after `self._data_function = data_function`), add:

```python
        self._post_run_hooks: list[PostRunHook] = []
```

- [ ] **Step 5: Add `add_post_run_hook`, `_fire_post_run_hooks`, and `_invoke_with_hooks` to `_FunctionPodBase`**

After the `process_data` method (around line 173 in `function_pod.py`), insert these three methods:

```python
    def add_post_run_hook(self, hook: PostRunHook) -> None:
        """Register a post-run hook on this pod.

        Hooks fire after every invocation (computed, cache hit, or error), in
        registration order, before the result is emitted downstream.

        A plain callable defaults to fail-loud (exceptions propagate, stopping
        the pod run). Wrap in ``HookConfig(fn=..., on_error="log")`` to log and
        continue on hook failure.

        Args:
            hook: A callable ``(PostRunPayload) -> None``, or a ``HookConfig``
                wrapping such a callable with explicit error handling.
        """
        self._post_run_hooks.append(hook)

    def _fire_post_run_hooks(self, payload: PostRunPayload) -> None:
        """Fire all registered hooks with payload in registration order.

        Args:
            payload: The post-run payload to pass to each hook.
        """
        for hook in self._post_run_hooks:
            fn = hook.fn if isinstance(hook, HookConfig) else hook
            on_error = hook.on_error if isinstance(hook, HookConfig) else "raise"
            try:
                fn(payload)
            except Exception as exc:
                if on_error == "raise":
                    raise
                logger.warning(
                    "Post-run hook %r raised and was suppressed: %s",
                    fn,
                    exc,
                    exc_info=True,
                )

    def _invoke_with_hooks(
        self,
        tag: TagProtocol,
        data: DataProtocol,
        *,
        logger: DataExecutionLoggerProtocol | None = None,
    ) -> tuple[TagProtocol, DataProtocol | None]:
        """Call ``process_data``, time it, and fire post-run hooks.

        This is the call site used by ``FunctionPodStream`` and
        ``async_execute``; ``process_data`` itself is unchanged. Override in
        subclasses (e.g. ``CachedFunctionPod``) to supply a different
        ``InvocationStatus``.

        Args:
            tag: The tag associated with the data.
            data: The input data to process.
            logger: Optional data execution logger forwarded to ``process_data``.

        Returns:
            A ``(tag, output_data)`` tuple.
        """
        started_at = datetime.now(timezone.utc)
        exc: Exception | None = None
        out_tag = tag
        output_data: DataProtocol | None = None

        try:
            out_tag, output_data = self.process_data(tag, data, logger=logger)
            status = InvocationStatus.COMPUTED
        except Exception as e:
            exc = e
            status = InvocationStatus.ERROR

        finished_at = datetime.now(timezone.utc)

        if self._post_run_hooks:
            record_id = (
                str(output_data.datagram_uuid) if output_data is not None else None
            )
            payload = PostRunPayload(
                record_id_hash=record_id,
                tag=tag,
                input=data,
                output=output_data,
                stats=RunStats(
                    duration_ms=(finished_at - started_at).total_seconds() * 1000,
                    status=status,
                    started_at=started_at,
                    finished_at=finished_at,
                    error=exc,
                ),
                pod=PodContext(
                    label=self.label,
                    pod_hash=self.content_hash().to_string(),
                ),
            )
            self._fire_post_run_hooks(payload)

        if exc is not None:
            raise exc
        return out_tag, output_data

    async def _async_invoke_with_hooks(
        self,
        tag: TagProtocol,
        data: DataProtocol,
        *,
        logger: DataExecutionLoggerProtocol | None = None,
    ) -> tuple[TagProtocol, DataProtocol | None]:
        """Async counterpart of ``_invoke_with_hooks``.

        Args:
            tag: The tag associated with the data.
            data: The input data to process.
            logger: Optional data execution logger forwarded to ``async_process_data``.

        Returns:
            A ``(tag, output_data)`` tuple.
        """
        started_at = datetime.now(timezone.utc)
        exc: Exception | None = None
        out_tag = tag
        output_data: DataProtocol | None = None

        try:
            out_tag, output_data = await self.async_process_data(
                tag, data, logger=logger
            )
            status = InvocationStatus.COMPUTED
        except Exception as e:
            exc = e
            status = InvocationStatus.ERROR

        finished_at = datetime.now(timezone.utc)

        if self._post_run_hooks:
            record_id = (
                str(output_data.datagram_uuid) if output_data is not None else None
            )
            payload = PostRunPayload(
                record_id_hash=record_id,
                tag=tag,
                input=data,
                output=output_data,
                stats=RunStats(
                    duration_ms=(finished_at - started_at).total_seconds() * 1000,
                    status=status,
                    started_at=started_at,
                    finished_at=finished_at,
                    error=exc,
                ),
                pod=PodContext(
                    label=self.label,
                    pod_hash=self.content_hash().to_string(),
                ),
            )
            self._fire_post_run_hooks(payload)

        if exc is not None:
            raise exc
        return out_tag, output_data
```

- [ ] **Step 6: Update `FunctionPodStream._iter_data_sequential` to call `_invoke_with_hooks`**

In `FunctionPodStream._iter_data_sequential` (around line 530), replace:

```python
                tag, output_data = self._function_pod.process_data(tag, data)
```

with:

```python
                tag, output_data = self._function_pod._invoke_with_hooks(tag, data)
```

- [ ] **Step 7: Run the tests — most should pass now**

```bash
uv run pytest tests/test_core/function_pod/test_post_run_hooks.py -x -q 2>&1 | head -30
```

Expected: `TestSingleHookPayload`, `TestHookOrdering`, `TestHookFailureFast`, `TestHookFailureResilient`, `TestErrorStatus`, `TestFilteredOutput`, `TestEmptyHooks` all pass. `TestCacheHitStatus`, `TestParallelExecution`, `TestDecoratorConvenience` will be added in later tasks.

- [ ] **Step 8: Run the existing function pod test suite to confirm no regressions**

```bash
uv run pytest tests/test_core/function_pod/ -q 2>&1 | tail -10
```

Expected: all existing tests pass.

- [ ] **Step 9: Commit**

```bash
git add src/orcapod/core/function_pod.py tests/test_core/function_pod/test_post_run_hooks.py
git commit -m "feat(function_pod): add post-run hook infrastructure and sequential call site (ITL-523)"
```

---

## Task 3: Update `FunctionPodStream._iter_data_concurrent` + parallel test

**Files:**
- Modify: `src/orcapod/core/function_pod.py` (two lines in `_iter_data_concurrent`)
- Modify: `tests/test_core/function_pod/test_post_run_hooks.py` (add `TestParallelExecution`)

- [ ] **Step 1: Add `TestParallelExecution` to the test file**

Append to `tests/test_core/function_pod/test_post_run_hooks.py`:

```python
# ---------------------------------------------------------------------------
# 8. Parallel execution (concurrent path via _iter_data_concurrent)
# ---------------------------------------------------------------------------


class _ConcurrentExecutor:
    """Minimal executor that marks supports_concurrent_execution=True."""

    @property
    def executor_type_id(self) -> str:
        return "test-concurrent"

    def supported_function_type_ids(self) -> frozenset[str]:
        return frozenset()

    @property
    def supports_concurrent_execution(self) -> bool:
        return True

    def execute(self, data_function, data):
        return data_function.direct_call(data)

    async def async_execute(self, data_function, data):
        return data_function.direct_call(data)

    def execute_callable(self, fn, kwargs, executor_options=None, **kw):
        return fn(**kwargs)

    async def async_execute_callable(self, fn, kwargs, executor_options=None, **kw):
        return fn(**kwargs)

    def with_options(self, **kwargs):
        return self


class TestParallelExecution:
    def test_hooks_fire_for_all_inputs_under_concurrent_executor(self):
        executor = _ConcurrentExecutor()
        pf = PythonDataFunction(double, output_keys="result", executor=executor)
        pod = FunctionPod(pf)

        payloads: list[PostRunPayload] = []
        pod.add_post_run_hook(payloads.append)

        stream = pod.process(_make_stream(n=4))
        results = list(stream.iter_data())

        assert len(results) == 4
        assert len(payloads) == 4
        assert all(
            p.stats.status == InvocationStatus.COMPUTED for p in payloads
        )
```

- [ ] **Step 2: Run the new test to confirm it fails**

```bash
uv run pytest tests/test_core/function_pod/test_post_run_hooks.py::TestParallelExecution -x -q 2>&1 | head -15
```

Expected: FAIL — hooks fire 0 times (concurrent path still calls `process_data` / `async_process_data` directly).

- [ ] **Step 3: Update `_iter_data_concurrent` — sync fallback path**

In `FunctionPodStream._iter_data_concurrent` (around line 564), replace:

```python
                results = [
                    self._function_pod.process_data(tag, pkt)
                    for _, tag, pkt in to_compute
                ]
```

with:

```python
                results = [
                    self._function_pod._invoke_with_hooks(tag, pkt)
                    for _, tag, pkt in to_compute
                ]
```

- [ ] **Step 4: Update `_iter_data_concurrent` — async gather path**

In `FunctionPodStream._iter_data_concurrent` (around line 570), replace:

```python
                async def _gather() -> list[tuple[TagProtocol, DataProtocol | None]]:
                    return list(
                        await asyncio.gather(
                            *[
                                self._function_pod.async_process_data(tag, pkt)
                                for _, tag, pkt in to_compute
                            ]
                        )
                    )
```

with:

```python
                async def _gather() -> list[tuple[TagProtocol, DataProtocol | None]]:
                    return list(
                        await asyncio.gather(
                            *[
                                self._function_pod._async_invoke_with_hooks(tag, pkt)
                                for _, tag, pkt in to_compute
                            ]
                        )
                    )
```

- [ ] **Step 5: Run the parallel test — should pass now**

```bash
uv run pytest tests/test_core/function_pod/test_post_run_hooks.py::TestParallelExecution -x -q
```

Expected: PASS

- [ ] **Step 6: Run full function pod suite for regressions**

```bash
uv run pytest tests/test_core/function_pod/ -q 2>&1 | tail -10
```

Expected: all pass.

- [ ] **Step 7: Commit**

```bash
git add src/orcapod/core/function_pod.py tests/test_core/function_pod/test_post_run_hooks.py
git commit -m "feat(function_pod): update concurrent call sites to use _invoke_with_hooks (ITL-523)"
```

---

## Task 4: Update `async_execute` call site

**Files:**
- Modify: `src/orcapod/core/function_pod.py` (`async_execute`)
- Modify: `tests/test_core/function_pod/test_post_run_hooks.py` (add `TestAsyncExecuteHooks`)

- [ ] **Step 1: Add `TestAsyncExecuteHooks` to the test file**

Append to `tests/test_core/function_pod/test_post_run_hooks.py`:

```python
# ---------------------------------------------------------------------------
# async_execute path
# ---------------------------------------------------------------------------


class TestAsyncExecuteHooks:
    @pytest.mark.asyncio
    async def test_hooks_fire_through_async_execute(self):
        from orcapod.channels import Channel

        pod = _make_double_pod()
        payloads: list[PostRunPayload] = []
        pod.add_post_run_hook(payloads.append)

        stream = _make_stream(n=3)
        input_ch: Channel = Channel()
        output_ch: Channel = Channel()

        async def feed() -> None:
            for tag, data in stream.iter_data():
                await input_ch.writer.send((tag, data))
            await input_ch.writer.close()

        import asyncio
        await asyncio.gather(
            feed(),
            pod.async_execute([input_ch.reader], output_ch.writer),
        )

        results = []
        async for item in output_ch.reader:
            results.append(item)

        assert len(results) == 3
        assert len(payloads) == 3
        assert all(p.stats.status == InvocationStatus.COMPUTED for p in payloads)
```

- [ ] **Step 2: Run to confirm it fails**

```bash
uv run pytest tests/test_core/function_pod/test_post_run_hooks.py::TestAsyncExecuteHooks -x -q 2>&1 | head -15
```

Expected: FAIL — hooks fire 0 times (async_execute still calls `async_process_data` directly).

- [ ] **Step 3: Update `async_execute` to call `_async_invoke_with_hooks`**

In `_FunctionPodBase.async_execute`, inside the `process_one` inner function (around line 241), replace:

```python
                    out_tag, result_data = await self.async_process_data(
                        tag, data, logger=pkt_logger
                    )
```

with:

```python
                    out_tag, result_data = await self._async_invoke_with_hooks(
                        tag, data, logger=pkt_logger
                    )
```

- [ ] **Step 4: Run the async_execute test — should pass**

```bash
uv run pytest tests/test_core/function_pod/test_post_run_hooks.py::TestAsyncExecuteHooks -x -q
```

Expected: PASS

- [ ] **Step 5: Run full function pod suite for regressions**

```bash
uv run pytest tests/test_core/function_pod/ -q 2>&1 | tail -10
```

Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/core/function_pod.py tests/test_core/function_pod/test_post_run_hooks.py
git commit -m "feat(function_pod): update async_execute to use _async_invoke_with_hooks (ITL-523)"
```

---

## Task 5: `CachedFunctionPod` override for cache hit status

**Files:**
- Modify: `src/orcapod/core/cached_function_pod.py`
- Modify: `tests/test_core/function_pod/test_post_run_hooks.py` (add `TestCacheHitStatus`)

- [ ] **Step 1: Add `TestCacheHitStatus` to the test file**

Append to `tests/test_core/function_pod/test_post_run_hooks.py`:

```python
# ---------------------------------------------------------------------------
# 5. Cache hit status
# ---------------------------------------------------------------------------


class TestCacheHitStatus:
    def test_second_call_fires_hook_with_hit_status(self):
        inner = _make_double_pod()
        db = InMemoryArrowDatabase()
        pod = CachedFunctionPod(function_pod=inner, result_database=db)

        payloads: list[PostRunPayload] = []
        pod.add_post_run_hook(payloads.append)

        stream1 = pod.process(_make_stream(n=1))
        list(stream1.iter_data())

        stream2 = pod.process(_make_stream(n=1))
        list(stream2.iter_data())

        assert len(payloads) == 2
        assert payloads[0].stats.status == InvocationStatus.COMPUTED
        assert payloads[1].stats.status == InvocationStatus.HIT

    def test_cache_hit_payload_has_record_id(self):
        inner = _make_double_pod()
        db = InMemoryArrowDatabase()
        pod = CachedFunctionPod(function_pod=inner, result_database=db)

        payloads: list[PostRunPayload] = []
        pod.add_post_run_hook(payloads.append)

        stream1 = pod.process(_make_stream(n=1))
        list(stream1.iter_data())
        stream2 = pod.process(_make_stream(n=1))
        list(stream2.iter_data())

        assert payloads[1].record_id_hash is not None
        assert payloads[1].output is not None
```

- [ ] **Step 2: Run to confirm it fails**

```bash
uv run pytest tests/test_core/function_pod/test_post_run_hooks.py::TestCacheHitStatus -x -q 2>&1 | head -15
```

Expected: FAIL — `payloads[1].stats.status == InvocationStatus.COMPUTED` instead of `HIT` (base class always returns `COMPUTED`).

- [ ] **Step 3: Add imports to `cached_function_pod.py`**

At the top of `src/orcapod/core/cached_function_pod.py`, add after existing imports:

```python
from datetime import datetime, timezone

from orcapod.hooks import (
    HookConfig,
    InvocationStatus,
    PodContext,
    PostRunPayload,
    RunStats,
)
from orcapod.types import ColumnConfig
```

- [ ] **Step 4: Add `_invoke_with_hooks` override to `CachedFunctionPod`**

After the `async_process_data` method in `CachedFunctionPod` (around line 149), add:

```python
    def _invoke_with_hooks(
        self,
        tag: TagProtocol,
        data: DataProtocol,
        *,
        logger: DataExecutionLoggerProtocol | None = None,
    ) -> tuple[TagProtocol, DataProtocol | None]:
        """Override to detect cache hit status from ``RESULT_COMPUTED_FLAG`` meta.

        Calls ``self.process_data()`` (which owns all cache lookup and store
        logic), then reads ``RESULT_COMPUTED_FLAG`` from the output data meta to
        determine ``InvocationStatus.HIT`` vs ``InvocationStatus.COMPUTED``.

        Args:
            tag: The tag associated with the data.
            data: The input data to process.
            logger: Optional data execution logger.

        Returns:
            A ``(tag, output_data)`` tuple.
        """
        started_at = datetime.now(timezone.utc)
        exc: Exception | None = None
        out_tag = tag
        output_data: DataProtocol | None = None
        status = InvocationStatus.COMPUTED

        try:
            out_tag, output_data = self.process_data(tag, data, logger=logger)
            if output_data is not None:
                meta = output_data.as_dict(columns=ColumnConfig(meta=True))
                if meta.get(self.RESULT_COMPUTED_FLAG) is False:
                    status = InvocationStatus.HIT
        except Exception as e:
            exc = e
            status = InvocationStatus.ERROR

        finished_at = datetime.now(timezone.utc)

        if self._post_run_hooks:
            record_id = (
                str(output_data.datagram_uuid) if output_data is not None else None
            )
            payload = PostRunPayload(
                record_id_hash=record_id,
                tag=tag,
                input=data,
                output=output_data,
                stats=RunStats(
                    duration_ms=(finished_at - started_at).total_seconds() * 1000,
                    status=status,
                    started_at=started_at,
                    finished_at=finished_at,
                    error=exc,
                ),
                pod=PodContext(
                    label=self.label,
                    pod_hash=self.content_hash().to_string(),
                ),
            )
            self._fire_post_run_hooks(payload)

        if exc is not None:
            raise exc
        return out_tag, output_data

    async def _async_invoke_with_hooks(
        self,
        tag: TagProtocol,
        data: DataProtocol,
        *,
        logger: DataExecutionLoggerProtocol | None = None,
    ) -> tuple[TagProtocol, DataProtocol | None]:
        """Async counterpart of ``_invoke_with_hooks`` for ``CachedFunctionPod``.

        Args:
            tag: The tag associated with the data.
            data: The input data to process.
            logger: Optional data execution logger.

        Returns:
            A ``(tag, output_data)`` tuple.
        """
        started_at = datetime.now(timezone.utc)
        exc: Exception | None = None
        out_tag = tag
        output_data: DataProtocol | None = None
        status = InvocationStatus.COMPUTED

        try:
            out_tag, output_data = await self.async_process_data(
                tag, data, logger=logger
            )
            if output_data is not None:
                meta = output_data.as_dict(columns=ColumnConfig(meta=True))
                if meta.get(self.RESULT_COMPUTED_FLAG) is False:
                    status = InvocationStatus.HIT
        except Exception as e:
            exc = e
            status = InvocationStatus.ERROR

        finished_at = datetime.now(timezone.utc)

        if self._post_run_hooks:
            record_id = (
                str(output_data.datagram_uuid) if output_data is not None else None
            )
            payload = PostRunPayload(
                record_id_hash=record_id,
                tag=tag,
                input=data,
                output=output_data,
                stats=RunStats(
                    duration_ms=(finished_at - started_at).total_seconds() * 1000,
                    status=status,
                    started_at=started_at,
                    finished_at=finished_at,
                    error=exc,
                ),
                pod=PodContext(
                    label=self.label,
                    pod_hash=self.content_hash().to_string(),
                ),
            )
            self._fire_post_run_hooks(payload)

        if exc is not None:
            raise exc
        return out_tag, output_data
```

You also need to add the missing import at the top of `cached_function_pod.py` — `DataExecutionLoggerProtocol` is already imported, but verify it is present:

```python
from orcapod.protocols.observability_protocols import DataExecutionLoggerProtocol
```

- [ ] **Step 5: Run the cache hit tests — should pass**

```bash
uv run pytest tests/test_core/function_pod/test_post_run_hooks.py::TestCacheHitStatus -x -q
```

Expected: PASS

- [ ] **Step 6: Run full test suite for regressions**

```bash
uv run pytest tests/test_core/function_pod/ -q 2>&1 | tail -10
```

Expected: all pass.

- [ ] **Step 7: Commit**

```bash
git add src/orcapod/core/cached_function_pod.py tests/test_core/function_pod/test_post_run_hooks.py
git commit -m "feat(cached_function_pod): override _invoke_with_hooks for cache hit status (ITL-523)"
```

---

## Task 6: `@function_pod` decorator + `orcapod.__init__` exports

**Files:**
- Modify: `src/orcapod/core/function_pod.py` (`function_pod` decorator signature)
- Modify: `src/orcapod/__init__.py`
- Modify: `tests/test_core/function_pod/test_post_run_hooks.py` (add `TestDecoratorConvenience`)

- [ ] **Step 1: Add `TestDecoratorConvenience` to the test file**

Append to `tests/test_core/function_pod/test_post_run_hooks.py`:

```python
# ---------------------------------------------------------------------------
# 9. Decorator convenience
# ---------------------------------------------------------------------------


class TestDecoratorConvenience:
    def test_decorator_post_run_hooks_fires_hook(self):
        payloads: list[PostRunPayload] = []

        @function_pod(output_keys="result", post_run_hooks=[payloads.append])
        def compute(x: int) -> int:
            return x * 3

        stream = compute.pod.process(_make_stream(n=2))
        list(stream.iter_data())

        assert len(payloads) == 2
        assert all(p.stats.status == InvocationStatus.COMPUTED for p in payloads)

    def test_decorator_hookconfig_works(self):
        payloads: list[PostRunPayload] = []

        @function_pod(
            output_keys="result",
            post_run_hooks=[HookConfig(fn=payloads.append, on_error="log")],
        )
        def compute2(x: int) -> int:
            return x + 1

        stream = compute2.pod.process(_make_stream(n=1))
        list(stream.iter_data())

        assert len(payloads) == 1

    def test_public_api_imports(self):
        import orcapod
        assert hasattr(orcapod, "PostRunPayload")
        assert hasattr(orcapod, "HookConfig")
        assert hasattr(orcapod, "InvocationStatus")
        assert hasattr(orcapod, "RunStats")
        assert hasattr(orcapod, "PodContext")
```

- [ ] **Step 2: Run to confirm decorator test fails**

```bash
uv run pytest tests/test_core/function_pod/test_post_run_hooks.py::TestDecoratorConvenience -x -q 2>&1 | head -15
```

Expected: `TypeError: function_pod() got an unexpected keyword argument 'post_run_hooks'`

- [ ] **Step 3: Update `function_pod` decorator signature**

In `src/orcapod/core/function_pod.py`, find the `function_pod` function definition (around line 700). Add `post_run_hooks` parameter before `**kwargs`:

```python
def function_pod(
    output_keys: str | Sequence[str] | None = None,
    function_name: str | None = None,
    version: str = "v0.0",
    label: str | None = None,
    result_database: ArrowDatabaseProtocol | None = None,
    pod_cache_database: ArrowDatabaseProtocol | None = None,
    executor: DataFunctionExecutorProtocol | None = None,
    post_run_hooks: Sequence[PostRunHook] | None = None,
    **kwargs,
) -> Callable[..., CallableWithPodProtocol]:
```

Also add the `Sequence` import if not already present — it's already in the imports at the top of `function_pod.py` (`from collections.abc import Callable, Collection, Iterator, Sequence`), so no change needed there.

- [ ] **Step 4: Register hooks in the decorator body**

Inside the `decorator` function, after the `CachedFunctionPod` wrapping block (after the `if pod_cache_database is not None:` block, around line 760), add:

```python
        if post_run_hooks:
            for hook in post_run_hooks:
                pod.add_post_run_hook(hook)
```

- [ ] **Step 5: Update `src/orcapod/__init__.py` to export hook types**

In `src/orcapod/__init__.py`, add after the existing imports:

```python
from .hooks import (
    HookConfig,
    InvocationStatus,
    PodContext,
    PostRunHook,
    PostRunHookFn,
    PostRunPayload,
    RunStats,
)
```

And add these names to `__all__`:

```python
    # Post-run hook types (ITL-523)
    "HookConfig",
    "InvocationStatus",
    "PodContext",
    "PostRunHook",
    "PostRunHookFn",
    "PostRunPayload",
    "RunStats",
```

- [ ] **Step 6: Run decorator and public API tests — should pass**

```bash
uv run pytest tests/test_core/function_pod/test_post_run_hooks.py::TestDecoratorConvenience -x -q
```

Expected: PASS

- [ ] **Step 7: Run the complete new test file**

```bash
uv run pytest tests/test_core/function_pod/test_post_run_hooks.py -v 2>&1 | tail -30
```

Expected: all 10 test classes pass.

- [ ] **Step 8: Commit**

```bash
git add src/orcapod/core/function_pod.py src/orcapod/__init__.py tests/test_core/function_pod/test_post_run_hooks.py
git commit -m "feat(function_pod): add post_run_hooks param to decorator and export public API (ITL-523)"
```

---

## Task 7: Full test suite + final commit

- [ ] **Step 1: Run the entire test suite**

```bash
uv run pytest tests/ -q 2>&1 | tail -20
```

Expected: all tests pass, no regressions.

- [ ] **Step 2: If any test fails, investigate and fix before proceeding**

Common failure modes:
- Import errors: check that all new imports in `function_pod.py` and `cached_function_pod.py` are at the top of the file, not inside methods.
- `AttributeError: '_post_run_hooks'`: the `__init__` assignment in `_FunctionPodBase` may not have been saved — re-check step 4 of Task 2.
- `TypeError` in `_iter_data_concurrent`: verify both sync fallback and async gather paths were updated in Task 3.

- [ ] **Step 3: Checkout the feature branch (if not already on it)**

```bash
git branch --show-current
```

Expected: `eywalker/itl-523-function-pods-configurable-post-run-hook-invoked-after-every`

If not on the branch:
```bash
git checkout -b eywalker/itl-523-function-pods-configurable-post-run-hook-invoked-after-every
```

- [ ] **Step 4: Push to remote**

```bash
git push -u origin eywalker/itl-523-function-pods-configurable-post-run-hook-invoked-after-every
```

Expected: branch pushed, ready for PR.
