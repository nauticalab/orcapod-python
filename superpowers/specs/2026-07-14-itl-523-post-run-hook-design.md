# Post-Run Hook for Function Pods — Design Spec

**Issue:** ITL-523  
**Date:** 2026-07-14  
**Status:** Approved

---

## Overview

Add a per-pod post-run hook mechanism to function pods. After each invocation of a
function pod — whether the result was freshly computed, served from a pod-level cache, or
resulted in an error — all hooks registered on that pod are called with a structured payload
describing the run. Hooks are the primitive for run-level logging, metrics collection, and
provenance stamping.

---

## New Types — `src/orcapod/hooks.py`

All public hook types live in a new top-level module `src/orcapod/hooks.py` and are
re-exported from `orcapod.__init__`.

### `InvocationStatus`

```python
class InvocationStatus(str, Enum):
    COMPUTED = "computed"  # function was invoked and produced a fresh result
    HIT = "hit"            # result served from pod-level cache (CachedFunctionPod)
    ERROR = "error"        # function raised an exception
```

### `RunStats`

```python
@dataclass(frozen=True)
class RunStats:
    duration_ms: float          # wall-clock time in milliseconds
    status: InvocationStatus
    started_at: datetime        # UTC
    finished_at: datetime       # UTC
    error: Exception | None     # populated when status == ERROR; None otherwise
```

### `PodContext`

```python
@dataclass(frozen=True)
class PodContext:
    label: str | None   # pod.label — human-readable name
    pod_hash: str       # pod.content_hash().to_string() — identifies the function version
```

### `PostRunPayload`

```python
@dataclass(frozen=True)
class PostRunPayload:
    record_id_hash: str | None  # str(output_data.datagram_uuid); None if filtered or error
    tag: TagProtocol            # input tag (immutable; do not mutate)
    input: DataProtocol         # input data (immutable; do not mutate)
    output: DataProtocol | None # output data; None if filtered out or error
    stats: RunStats
    pod: PodContext
```

`record_id_hash` is the UUID of the output datagram — the same identifier used as the
primary key when the result is stored in a backing database. It uniquely identifies this
specific output record. It is `None` when `output` is `None` (i.e. the function filtered
the row out, or the invocation raised an error). Hooks must treat `tag`, `input`, and
`output` as read-only; the payload is frozen but the referenced datagram objects are not
deeply immutable.

### Hook types

```python
PostRunHookFn = Callable[[PostRunPayload], None]

@dataclass(frozen=True)
class HookConfig:
    fn: PostRunHookFn
    on_error: Literal["raise", "log"] = "raise"

PostRunHook = PostRunHookFn | HookConfig
```

A plain `PostRunHookFn` defaults to fail-loud (`on_error="raise"`). Wrap in `HookConfig`
to opt into resilient behaviour per hook.

---

## Hook Registration API

### `_FunctionPodBase.add_post_run_hook()`

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
```

Hooks are stored in a plain `list[PostRunHook]` on `_FunctionPodBase`, initialised to `[]`
in `__init__`. This is intentionally mutable — hooks can be added after construction.

### `@function_pod` decorator parameter

```python
@function_pod(
    output_keys="result",
    post_run_hooks=[my_hook],           # convenience parameter
)
def compute(x: int) -> int:
    ...
```

The decorator accepts an optional `post_run_hooks: Sequence[PostRunHook] | None`
parameter. It calls `pod.add_post_run_hook(hook)` for each hook in order after the pod
is constructed (and after `CachedFunctionPod` wrapping, if applicable). This is strictly
a convenience — it is identical to calling `add_post_run_hook` manually after decoration.

---

## Firing Semantics

### When hooks fire

Hooks fire **after** the pod's compute-or-lookup step completes (or raises), and
**before** the result is emitted downstream. They fire on:

- Every freshly computed output (`InvocationStatus.COMPUTED`)
- Every pod-level cache hit (`InvocationStatus.HIT`) — i.e. when `CachedFunctionPod`
  finds the result in its backing database
- Every invocation that raises an exception (`InvocationStatus.ERROR`)

Hooks do **not** re-fire for `FunctionPodStream` in-memory re-iteration hits (when
`_cached_output_datas` already holds a result for an index within the same stream
object's lifetime). That is a low-level within-object optimisation, not a new pod
invocation — the hook already fired on the original computation.

### Hook failure semantics

Hooks run in registration order. Each hook runs to completion (or failure) before the
next. If a hook raises:

- `on_error="raise"` (default for plain callables and `HookConfig` default): the
  exception propagates immediately, stopping any remaining hooks and failing the pod
  invocation.
- `on_error="log"`: the exception is logged at `WARNING` level and execution continues
  with the next hook. The pod invocation result is still returned normally.

When the pod function itself raises (`InvocationStatus.ERROR`), hooks fire first with
`status=ERROR` and `stats.error` set to the exception. After all hooks complete, the
original exception is re-raised. This means a hook with `on_error="raise"` that also
raises will replace the original exception in the traceback.

### Ordering guarantees

Hooks fire in registration order within a single invocation. There is no ordering
guarantee across concurrent invocations (each invocation fires its own hooks
independently).

---

## Implementation — `_invoke_with_hooks()`

A new internal method `_invoke_with_hooks()` (and async counterpart
`_async_invoke_with_hooks()`) is added to `_FunctionPodBase`. All call sites that
previously called `process_data()` or `async_process_data()` directly are updated to
call `_invoke_with_hooks()` instead. `process_data()` and `async_process_data()` are
unchanged.

```
Call sites updated:
  FunctionPodStream._iter_data_sequential()    → _invoke_with_hooks()
  FunctionPodStream._iter_data_concurrent()    → _invoke_with_hooks() / _async_invoke_with_hooks()
  _FunctionPodBase.async_execute()             → _async_invoke_with_hooks()
```

The base class `_invoke_with_hooks()`:

1. Records `started_at`.
2. Calls `self.process_data(tag, data, logger=logger)`.
3. Records `finished_at`.
4. Determines `InvocationStatus` (always `COMPUTED` at the base class level).
5. If `self._post_run_hooks` is non-empty, constructs `PostRunPayload` and calls
   `_fire_post_run_hooks(payload)`.
6. Re-raises any exception after hooks fire.
7. Returns `(out_tag, output_data)`.

When `self._post_run_hooks` is empty, step 5 is skipped entirely — zero overhead for
pods with no hooks.

### `CachedFunctionPod` override

`CachedFunctionPod` overrides `_invoke_with_hooks()` (and its async counterpart) to
supply the correct `InvocationStatus`. It calls `self.process_data()` (which already
owns the cache lookup and store logic), then reads the `RESULT_COMPUTED_FLAG` meta field
on the output data to determine whether the result was a cache hit or fresh computation:

- `RESULT_COMPUTED_FLAG == False` → `InvocationStatus.HIT`
- `RESULT_COMPUTED_FLAG == True` or absent → `InvocationStatus.COMPUTED`
- exception raised → `InvocationStatus.ERROR`

This keeps `process_data()` as the single source of truth for caching behaviour.

---

## Attachment Point

**Per-pod only (v1).** There are no pipeline-level hooks in this release. Pipeline-level
hooks — where a single hook registration applies to every pod in a pipeline — are a
follow-up. They can be built on top of per-pod hooks without any API changes.

---

## Sync vs Async

**Synchronous hooks only (v1).** Hook callables are `(PostRunPayload) -> None` — plain
synchronous functions. They run on the critical path; slow hooks stall the pipeline.
Documentation must make this explicit.

Async hooks are out of scope for v1. The `async_execute()` path calls
`_async_invoke_with_hooks()` but the hooks themselves are still invoked synchronously
within it.

---

## Scope

In scope:

- `src/orcapod/hooks.py` — all new public types
- `src/orcapod/core/function_pod.py` — `_post_run_hooks` list, `add_post_run_hook()`,
  `_invoke_with_hooks()`, `_async_invoke_with_hooks()`, `_fire_post_run_hooks()`;
  updated call sites in `FunctionPodStream` and `async_execute()`
- `src/orcapod/core/cached_function_pod.py` — override `_invoke_with_hooks()` and
  `_async_invoke_with_hooks()`
- `src/orcapod/__init__.py` — re-export new public types
- `tests/test_core/function_pod/test_post_run_hooks.py` — full test suite
- Docstrings on all new public API

Out of scope:

- Pre-run hooks
- Operator pod hooks
- Pipeline-level hooks
- Async hooks
- Built-in hook implementations (logging sink, metrics, EDI provenance)
- Remote / cross-process hook execution

---

## Tests

All tests in `tests/test_core/function_pod/test_post_run_hooks.py`:

1. **Single hook fires with correct payload** — verify `record_id_hash`, `tag`, `input`,
   `output`, `stats.status == COMPUTED`, `stats.duration_ms > 0`, `pod.label`
2. **Multiple hooks fire in registration order** — two hooks append to a shared list;
   verify order matches registration order
3. **Fail-loud hook error** — plain callable hook raises; exception propagates, pod run
   fails
4. **Resilient hook error** — `HookConfig(on_error="log")` raises; logged and suppressed,
   computation result still returned, next hook still fires
5. **Cache hit status** — `CachedFunctionPod`, run same input twice; second call fires
   hook with `InvocationStatus.HIT`
6. **Error status** — function raises; hook fires with `InvocationStatus.ERROR`,
   `stats.error` is the exception, original exception re-raised after hooks
7. **Filtered output** — function returns `None` (filtered); hook fires with
   `output=None`, `record_id_hash=None`, `stats.status == COMPUTED`
8. **Parallel execution** — concurrent pod with multiple inputs; hooks fire for all
   inputs, no dropped calls
9. **Decorator convenience** — `@function_pod(post_run_hooks=[fn])` registers hook;
   fires identically to `pod.add_post_run_hook(fn)`
10. **Empty hooks — no overhead path** — pod with no hooks registered; no payload
    constructed (verified by confirming `_post_run_hooks` is empty and no side effects)
