# ctx_arg on function pod + empty-opts ray.remote hardening

**Issue:** ITL-544
**Date:** 2026-07-17
**Status:** Approved

---

## Overview

Two quick patches applied to Orcapod need to be hardened into properly tested,
documented, first-class capabilities:

1. **Patch 1 — `ctx_arg` on `@function_pod`:** The `ctx_arg` parameter that promotes a
   function pod to a side-effect pod (receiving an `InvocationContext`) is now the
   preferred authoring path. It needs full test coverage and the older
   `side_effect_function_pod` decorator needs to be documented as superseded.

2. **Patch 2 — `ray.remote(**opts)` empty-opts guard:** A guard that special-cases empty
   `opts` to call `ray.remote(fn)` directly (instead of `ray.remote(**{})(fn)`) is
   correct but breaks two existing tests that were written before the guard existed.

---

## Patch 2 — Fix Failing Ray Executor Tests

### Problem

In `RayExecutor._get_remote_fn()` (`src/orcapod/core/executors/ray.py`), the guard:

```python
self._remote_fn_cache[cache_key] = (
    ray.remote(**opts)(wrapper) if opts else ray.remote(wrapper)
)
```

is semantically correct — `ray.remote(**{})` raises in some Ray versions. However,
two tests in `tests/test_core/test_regression_fixes.py` mock `ray.remote` with a
`fake_remote` that only handles the `ray.remote(**opts)(fn)` calling convention. When
`opts` is empty, the guard now calls `ray.remote(fn)` directly, which the mock doesn't
support.

**Failing tests:**
- `test_get_remote_fn_caches_per_function_name`
- `test_get_remote_fn_sets_wrapper_name`

### Fix

Update both mocks so `fake_remote` handles both calling conventions:

- `fake_remote(**opts)` → returns a decorator (existing path, non-empty opts)
- `fake_remote(fn)` → directly returns a remote-wrapped callable (new path, empty opts)

No changes to `ray.py` itself — the guard is correct and must not regress.

---

## Patch 1 — `ctx_arg` Full Test Coverage

### Preferred Entry Point

`@function_pod(ctx_arg="ctx")` is now the preferred way to author side-effect pods.
It is equivalent to `@side_effect_function_pod(output_keys=[...], ctx_arg_name="ctx")`
in every observable way: same `InvocationContext` semantics, same invocation-hash
construction, same schema filtering (ctx param stripped from exposed input schema).

### New Test File

Create `tests/test_core/function_pod/test_function_pod_ctx_arg.py` with full parity
against the `side_effect_function_pod` test suite. Required scenarios:

| # | Scenario | What it validates |
|---|---|---|
| 1 | Schema inference — ctx stripped | ctx param absent from `input_data_schema` |
| 2 | Schema inference — ctx retained in data function | ctx param present in underlying `_data_function.input_data_schema` for hashing |
| 3 | Standalone sync execution (`FunctionPodStream`) | ctx injected per-row, output correct |
| 4 | Standalone async execution (`FunctionPodStream`) | async counterpart works |
| 5 | DB-backed sync execution (`FunctionJobNode`) | invocation tracked, results persisted |
| 6 | Full pipeline — compile + execute | end-to-end with `ctx_arg` pod as a node |
| 7 | Decorator form — direct call | `@function_pod(ctx_arg="ctx")` on function |
| 8 | Decorator form — factory form | `@function_pod(output_keys=[...], ctx_arg="ctx")` |
| 9 | Ctx collision with data columns | `ValueError` raised at construction time |
| 10 | Cached pod wrapping | wrap ctx_arg pod in cached function pod; verify InvocationContext still injected |

### Cached Pod Wrapping (Scenario 10)

When a `ctx_arg`-enabled `FunctionPod` is wrapped in a cached function pod, the cache
layer must not interfere with context injection. Specifically:

- The wrapped pod's `input_data_schema` (sans ctx) must remain correct.
- On execution, `InvocationContext` is still injected per-row by the inner pod.
- Cache hits must still produce the same output as a fresh execution.

---

## `side_effect_function_pod` — Docstring Update

This is a greenfield pre-v0.1.0 project. Per project policy, no runtime deprecation
warnings are added. The transition is communicated through the docstring only.

Update the `side_effect_function_pod` docstring to:
- Note it is superseded by `@function_pod(ctx_arg=...)`.
- Show the preferred equivalent usage with a short before/after example.

No runtime warning is emitted. No changes to existing tests are needed.

### Follow-up Issue

File a Linear follow-up issue: **"Remove `side_effect_function_pod` (post-ITL-544
cleanup)"** — tracking full removal from exports, deletion of the decorator, and
migration of remaining usages. This is out of scope for ITL-544.

---

## Goals & Success Criteria

- All tests pass, including the two previously failing Ray executor tests.
- `@function_pod(ctx_arg=...)` has a full test suite (10 scenarios) demonstrating it as
  a first-class, documented capability.
- `side_effect_function_pod` docstring points to the preferred path.
- A follow-up Linear issue is filed for the full `side_effect_function_pod` removal.
- No regression in the empty-opts Ray guard behavior.

---

## Out of Scope

- Removing `side_effect_function_pod` entirely (tracked in follow-up issue).
- Broader refactor of function-pod entry points.
- Changes to `ray.py` guard logic.
