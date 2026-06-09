# ENG-376: FunctionNode accesses FunctionPod through its public interface

**Date:** 2026-06-08
**Issue:** [ENG-376](https://linear.app/enigma-metamorphic/issue/ENG-376)

## Overview

`FunctionNode` should interact with its underlying `FunctionPod` exclusively through
the pod's public properties and methods. An audit of all `FunctionNode` code paths
identified one violation: `FunctionJobNode._fetch_joined_records()` accesses the
private `_result_database` property on `CachedFunctionPod`.

A second gap (`node_config` absent from `FunctionPodProtocol`) was identified during
the audit but is deferred to [ENG-582](https://linear.app/enigma-metamorphic/issue/ENG-582).

## Violation found

**`CachedFunctionPod._result_database`** — accessed on line 1476 of `function_node.py`:

```python
results = self._cached_function_pod._result_database.get_all_records(
    self._cached_function_pod.record_path,
    record_id_column=constants.DATA_RECORD_ID,
)
```

`_result_database` is a `@property` on `CachedFunctionPod` named with a leading
underscore (private by Python convention). Its docstring states *"The underlying
result database (for FunctionNode access)"* — the intent was always for FunctionNode
to use it, but the naming was inconsistent with that intent.

The companion `record_path` property on the same line is already public (no
underscore prefix) and is not a violation.

## Fix

Rename `_result_database` → `result_database` in every location where it is defined
or accessed:

1. **`CachedFunctionPod`** (`src/orcapod/core/cached_function_pod.py`):
   rename the `@property _result_database` to `result_database`.

2. **`_ResultDatabaseReader`** (`src/orcapod/core/nodes/function_node.py`):
   rename the `self._result_database` instance variable to `self.result_database`.

3. **`FunctionJobNode._fetch_joined_records()`** (`src/orcapod/core/nodes/function_node.py`,
   line 1476): update the access from `_result_database` to `result_database`.

No logic changes. No new API surface. Purely a visibility correction.

## Scope

In scope:
- Renaming the three occurrences described above
- Verifying existing tests still pass

Out of scope:
- Any other FunctionNode/FunctionPod interface changes
- The `node_config` protocol gap (tracked in ENG-582)
- Changes to `PacketFunction` / `DataFunction` interfaces
