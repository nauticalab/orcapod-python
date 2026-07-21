# Design: Align PostRunPayload with SideEffectPod InvocationContext

**Issue:** ITL-531  
**Date:** 2026-07-21  
**Status:** Approved

---

## Overview

`PostRunPayload` (in `hooks.py`) is the snapshot passed to every post-run hook
after a `FunctionPod` processes one row. It currently carries the output-keyed
`record_id_hash` (the output datagram UUID) but has no deterministic input-keyed
hash, no `pipeline_run_id`, and no formatting helper.

`InvocationContext` (in `side_effects.py`) already provides all three — an
`invocation_hash` property, a `format_id(config)` method, and `pipeline_run_id`
— built from `pipeline_hash :: record_id_hash` over the input row. Post-run hook
authors currently cannot do equivalent provenance logging or external-system
stamping.

This design adds `invocation_context: InvocationContext | None` to
`PostRunPayload`, making the same identification primitives available to all
function pod hooks. The change is purely additive — no existing consumers break.

---

## Goals & Success Criteria

- `PostRunPayload` exposes `invocation_context: InvocationContext | None`,
  populated on every payload produced by the pod (never `None` in practice when
  built via `_build_post_run_payload`).
- Hook authors can call `payload.invocation_context.invocation_hash`,
  `.format_id(config)`, and `.pipeline_run_id` with identical semantics to
  `SideEffectPod` / `FunctionPod` ctx-injection.
- `InvocationHashConfig` and `InvocationContext` live in a shared leaf module
  (`src/orcapod/invocation.py`) with no orcapod-internal imports at runtime
  (only `TYPE_CHECKING` guards), making them importable by both `hooks.py` and
  `side_effects.py` without circular-import risk.
- All existing `PostRunPayload` consumers and current tests continue to pass
  unmodified (additive field with `= None` default).

---

## Architecture

### New module: `src/orcapod/invocation.py`

A new leaf module housing the invocation-identity types. These are extracted
verbatim from `side_effects.py` with no behavioral changes.

**Contents:**
- `InvocationHashConfig` — encoding (`hex`/`base64`) and truncation config
- `_serialize_component(content_hash, config) -> str` — internal serialization helper
- `InvocationContext` — carries `pod_name`, `pipeline_run_id`,
  `_pipeline_hash_ch`, `_record_id_hash_ch`, `_hash_config`, `_track_completion`;
  exposes `invocation_hash` property and `format_id(config=None) -> str`

**Runtime imports:** stdlib only (`base64`, `dataclasses`, typing). `ContentHash`
is import-guarded under `TYPE_CHECKING`.

### Changes to `src/orcapod/side_effects.py`

Remove the definitions of `InvocationHashConfig`, `_serialize_component`, and
`InvocationContext`. Replace with:

```python
from orcapod.invocation import (
    InvocationContext,
    InvocationHashConfig,
    _serialize_component,
)
```

All internal uses remain unchanged.

### Changes to `src/orcapod/hooks.py`

Add import:
```python
from orcapod.invocation import InvocationContext
```

Add field to `PostRunPayload`:
```python
invocation_context: InvocationContext | None = None
```

Place after `pod: PodContext` so existing positional construction (if any) is
unaffected. Update class docstring.

### Changes to `src/orcapod/core/function_pod.py`

**`_build_post_run_payload`** gains `run_id: str | None = None` parameter.
It calls `self._build_invocation_context(tag, data, run_id=run_id)` and passes
the result as `invocation_context=` in the `PostRunPayload` constructor.

**`_invoke_with_hooks`** and **`_async_invoke_with_hooks`** already carry
`run_id`; both calls to `_build_post_run_payload` are updated to pass `run_id`.

**`_build_invocation_context`** local import switches from `orcapod.side_effects`
to `orcapod.invocation` for `InvocationContext` and `InvocationHashConfig`.
`_SIDE_EFFECT_RECOMPUTATION_INDEX_COL` stays imported from `orcapod.side_effects`
(it's a constant about the side-effect preimage format, not the invocation type).

The `InvocationContext` is built with `_track_completion=True` and default
`InvocationHashConfig()`, matching the existing ctx-injection path. This produces
a 2-component hash (`pipeline_hash :: record_id_hash`). `pipeline_run_id` is
always stored and accessible regardless.

### Changes to `src/orcapod/__init__.py`

Update the import of `InvocationContext` and `InvocationHashConfig` from
`.side_effects` to `.invocation`. The exported symbols and their behavior are
identical.

---

## Data Flow

```
_invoke_with_hooks(tag, data, run_id=run_id)
  │
  ├─► process_data(tag, data, run_id=run_id)
  │     └─► [if ctx_arg_name set] _build_invocation_context(tag, data, run_id)
  │           └─► InvocationContext injected into user fn
  │
  └─► _build_post_run_payload(tag, data, output, ..., run_id=run_id)
        └─► _build_invocation_context(tag, data, run_id)
              └─► InvocationContext attached to PostRunPayload.invocation_context
```

Note: `_build_invocation_context` is called twice for ctx-aware pods (once for
the function injection, once for the payload). The cost is small (one PyArrow
table hash) and only incurred when hooks are registered.

---

## Import Graph (after change)

```
invocation.py          ← stdlib only
    ↑           ↑
hooks.py    side_effects.py
    ↑           ↑
    └─ function_pod.py ──────────► invocation.py (direct, replaces local import)
```

No cycles.

---

## Error Handling

`_build_invocation_context` is called unconditionally in `_build_post_run_payload`,
including the error path in `_invoke_with_hooks`. The `tag` and `data` arguments
are always available even when the pod function raised, so the `InvocationContext`
is always constructible. If `_build_invocation_context` itself raises (e.g. due
to a corrupt hasher), the original exception semantics are unchanged — the new
exception would propagate as a hook-phase error.

---

## Testing

Add a new test class `TestInvocationContextOnPayload` in
`tests/test_core/function_pod/test_post_run_hooks.py`:

| Test | What it checks |
|------|----------------|
| `test_invocation_context_always_present` | `invocation_context` is not `None` on COMPUTED payload |
| `test_invocation_hash_is_nonempty_string` | `invocation_hash` is a non-empty string |
| `test_format_id_matches_invocation_hash` | `format_id()` == `invocation_hash` |
| `test_format_id_base64_differs_from_hex` | `format_id(InvocationHashConfig(encoding="base64"))` ≠ `invocation_hash` |
| `test_pipeline_run_id_is_none_standalone` | `pipeline_run_id` is `None` in standalone mode |
| `test_invocation_hash_deterministic` | same input twice → same `invocation_hash` |
| `test_error_payload_has_invocation_context` | ERROR-status payload also has `invocation_context` set |

Additionally verify that existing tests continue to pass without modification
(the `= None` default handles direct `PostRunPayload` construction in tests).

---

## Out of Scope

- Pre-run hooks or operator-pod hooks
- Async hook execution changes
- Changing the `SideEffectPod` / `InvocationContext` API
- Exposing a `pipeline_run_id` field *directly* on `PostRunPayload` (accessible
  via `payload.invocation_context.pipeline_run_id`)
