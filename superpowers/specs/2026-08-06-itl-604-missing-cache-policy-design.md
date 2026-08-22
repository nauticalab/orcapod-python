# ITL-604: `missing_cache_policy` — Result-Store Miss Handling

**Date:** 2026-08-06
**Issue:** [ITL-604](https://linear.app/metamorphic/issue/ITL-604/non-ephemeral-result-store-opt-in-permissive-mode-for-missing-entries)
**Follow-up:** [ITL-606](https://linear.app/metamorphic/issue/ITL-606/revisit-missing-cache-policy-nodeconfig-api-per-store-type-granularity) — per-store-type policy granularity (deferred)
**Depends on ITL-605** for the full `"as_empty"` round-trip when the downstream node also has no cached result.

---

## Overview

When a pipeline table entry exists (i.e. an input was previously computed) but the corresponding result-store entry is absent, `FunctionJobNode` must decide what to do. Today the behaviour differs by store type and is not configurable:

- **Non-ephemeral (persistent) store miss** — WARNING log, entry silently dropped from join, falls through to recomputation. In CACHE_ONLY mode the entry is simply omitted from the output.
- **Ephemeral store miss** — `EmptyData` sentinel created; in FULL mode it triggers recomputation from the original upstream data; in CACHE_ONLY mode `EmptyData` is forwarded downstream.

This design adds a unified, three-value `missing_cache_policy` field to `NodeConfig` that governs the miss behaviour for **both** store types, together with a new `CacheMissError` exception for the strict mode.

---

## Goals & Success Criteria

- `NodeConfig` gains `missing_cache_policy: Literal["recompute", "as_empty", "strict"] | None`.
- `None` inherits the default (`"recompute"`), preserving all existing behaviour exactly.
- `"as_empty"`: both non-ephemeral and ephemeral misses emit `EmptyData` directly — no recomputation triggered. The downstream node attempts to serve the result from its own cache.
- `"strict"`: non-ephemeral miss raises `CacheMissError` immediately (FULL and CACHE_ONLY modes). Ephemeral misses in `"strict"` mode still degrade gracefully to `EmptyData` — raising on an ephemeral miss would contradict the semantics of ephemeral storage.
- Ephemeral misses always log at INFO level (currently silent). Non-ephemeral misses always log at WARNING or ERROR.
- All existing tests pass unmodified. New tests cover each policy × store-type × mode combination.

---

## Background: when do misses occur?

A miss means the **pipeline table** (tag DB) has a record for an input — indicating it was previously computed — but the **result DB** does not have the corresponding data row. This is distinct from a first-time computation (no pipeline table entry at all).

There are two execution modes relevant to miss handling:

- **FULL mode** — the node has a live upstream input stream and a function pod; recomputation is possible.
- **CACHE_ONLY mode** — the node was loaded from a serialised pipeline whose upstream source is `UNAVAILABLE`; the node can only serve from its DB. Recomputation is not possible.

---

## Design

### 1. `NodeConfig` — `src/orcapod/types.py`

```python
@dataclass(frozen=True, slots=True)
class NodeConfig:
    is_result_ephemeral: bool | None = None
    ignore_schema: tuple[str, ...] | None = None
    missing_cache_policy: Literal["recompute", "as_empty", "strict"] | None = None
```

**Semantics of `None`:** inherits the default (`"recompute"`), consistent with how `is_result_ephemeral` and `ignore_schema` work — `None` means "not explicitly set; use the default for this node."

**`merge()` update:** `other.missing_cache_policy` wins when non-`None`, self's value is used otherwise — same pattern as existing fields.

**Docstring warning:** the docstring must explicitly state:
- `"recompute"` is the default and preserves current behaviour.
- `"as_empty"` is only appropriate when partial gaps are semantically expected (e.g. shared read-only stores, exploratory pipelines). It requires ITL-605 to handle the case where the downstream node also has no cached result.
- `"strict"` is appropriate for production pipelines where a missing durable result always indicates a bug or data loss.

### 2. New exception — `src/orcapod/errors.py`

```python
class CacheMissError(Exception):
    """Raised when a persistent (non-ephemeral) result-store entry is absent
    and ``NodeConfig.missing_cache_policy`` is ``"strict"``.

    A missing durable result indicates data loss or corruption. Set
    ``missing_cache_policy="recompute"`` (the default) to fall back to
    recomputation, or ``"as_empty"`` to propagate an ``EmptyData`` token
    downstream instead of raising.
    """
```

### 3. Behaviour matrix

| Policy | Non-ephemeral miss — FULL | Non-ephemeral miss — CACHE_ONLY | Ephemeral miss — FULL | Ephemeral miss — CACHE_ONLY |
|---|---|---|---|---|
| `"recompute"` *(default)* | WARNING + recompute | WARNING + omit entry | INFO + recompute | INFO + EmptyData forwarded *(unchanged)* |
| `"as_empty"` | WARNING + emit EmptyData | WARNING + emit EmptyData | INFO + emit EmptyData | INFO + EmptyData forwarded |
| `"strict"` | ERROR + raise `CacheMissError` | ERROR + raise `CacheMissError` | INFO + emit EmptyData | INFO + EmptyData forwarded |

Notes:
- `"strict"` never raises for ephemeral misses — ephemeral data is expected to vanish.
- `"as_empty"` changes CACHE_ONLY non-ephemeral miss from silent omission to `EmptyData` forwarding.
- In `"as_empty"` and `"strict"` modes, `EmptyData` in `_cached_output_datas` is emitted directly rather than used as a recompute sentinel. Because `"strict"` raises before creating EmptyData for non-ephemeral misses, any EmptyData seen in `execute()` under `"strict"` must have come from an ephemeral miss.

### 4. Log messages

| Situation | Level | Message |
|---|---|---|
| Non-ephemeral miss, `"recompute"` | WARNING | `"X pipeline DB entries have no match in persistent result DB — data may have been deleted externally. These inputs will be recomputed."` |
| Non-ephemeral miss, `"as_empty"` | WARNING | `"X pipeline DB entries have no match in persistent result DB — treating as Empty data (missing_cache_policy='as_empty'). Downstream nodes will attempt to serve from their own cache."` |
| Non-ephemeral miss, `"strict"` | ERROR | `"X pipeline DB entries have no match in persistent result DB — raising CacheMissError (missing_cache_policy='strict')."` |
| Ephemeral miss (any policy) | INFO | `"X pipeline DB entries have no match in ephemeral result DB — expected after cross-session store clear. Propagating as EmptyData."` |

### 5. `_fetch_joined_records()` — `src/orcapod/core/nodes/function_node.py`

Two existing non-ephemeral miss branches are updated:

**Branch A** — result DB is `None` (completely empty):
```python
# Before: always WARNING + drop
# After: branch on policy
if policy == "strict":
    raise CacheMissError(...)
elif policy == "as_empty":
    logger.warning(...)
    # create EmptyData for all persistent_taginfo_df rows (same loop as ephemeral path)
else:  # "recompute"
    logger.warning(...)  # existing message
```

**Branch B** — result DB has rows but anti-join finds gaps:
```python
# Before: always WARNING + drop
# After: branch on policy
if missing_count > 0:
    if policy == "strict":
        raise CacheMissError(...)
    elif policy == "as_empty":
        logger.warning(...)
        # create EmptyData for unmatched_persistent_df rows
    else:
        logger.warning(...)  # existing message
```

The EmptyData creation loop is identical to the existing ephemeral path. Extract it into a small shared helper `_emit_empty_data_for_rows(df, ...)` to avoid duplication.

Ephemeral miss path: add INFO log before the existing EmptyData creation loop (no other change).

### 6. `execute()` — `src/orcapod/core/nodes/function_node.py`

```python
# Current condition (EmptyData = recompute sentinel):
if base_entry_id in self._cached_output_datas and not isinstance(
    self._cached_output_datas[base_entry_id][1], EmptyData
):
    # cache hit

# New condition:
if base_entry_id in self._cached_output_datas:
    cached_tag, cached_pkt = self._cached_output_datas[base_entry_id]
    if not isinstance(cached_pkt, EmptyData):
        # real data cache hit (unchanged)
        ...
    elif policy in ("as_empty", "strict"):
        # opportunistic: emit EmptyData directly, no recompute
        output.append((cached_tag, cached_pkt))
    else:
        # "recompute": fall through to _process_data_internal
        ...
else:
    # not in cache: compute (unchanged)
    ...
```

The same structural change applies to `async_execute()` and its two-phase emission logic.

### 7. `async_execute()` — two-phase async path

In Phase 1 (emit cached results), `EmptyData` entries are forwarded to the output channel when `policy != "recompute"`. This mirrors the `execute()` change and ensures the async pipeline behaves identically to the sync path.

---

## Tests

New tests live alongside the existing ephemeral-result tests in
`tests/test_core/function_pod/test_ephemeral_result.py` (or a new
`test_missing_cache_policy.py` in the same directory).

| Test | What it covers |
|---|---|
| `"recompute"`, non-ephemeral miss → recomputes | Regression — existing behaviour unchanged |
| `"recompute"`, ephemeral miss in FULL → recomputes (INFO log present) | Regression + new INFO log |
| `"as_empty"`, non-ephemeral miss → WARNING + EmptyData emitted, function not called | Core new behaviour |
| `"as_empty"`, ephemeral miss in FULL → INFO + EmptyData emitted, function not called | Core new behaviour |
| `"as_empty"`, CACHE_ONLY non-ephemeral miss → EmptyData forwarded (not omitted) | CACHE_ONLY path |
| `"strict"`, non-ephemeral miss (FULL) → `CacheMissError` raised | Strict mode |
| `"strict"`, non-ephemeral miss (CACHE_ONLY) → `CacheMissError` raised | Strict mode in CACHE_ONLY |
| `"strict"`, ephemeral miss → INFO + EmptyData, no raise | Graceful degradation |
| `"as_empty"`, downstream has result cached → end-to-end serves from downstream cache | Happy path |
| `"as_empty"`, downstream cache miss → `EphemeralResultMissingError` | ITL-605 boundary |
| Policy honoured across multiple `execute()` calls | No silent reset between calls |
| `NodeConfig.merge()` with `missing_cache_policy` | Config merge semantics |

---

## Out of scope

- Per-store-type independent policies (deferred to ITL-606).
- Handling `EphemeralResultMissingError` when downstream has no cached result (ITL-605).
- Changing default behaviour for any existing caller — `None` always means `"recompute"`.
- CACHE_ONLY non-ephemeral miss omit-vs-EmptyData for the `"recompute"` policy (unchanged: still omits).
