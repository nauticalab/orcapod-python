## `src/orcapod/core/function_pod.py`

### F14 — `FunctionPodStream.as_table()` uses Polars detour for Arrow sorting
**Status:** open
**Severity:** medium

`as_table()` (line ~568) converts Arrow → Polars → sort → Arrow when sorting by tags:
```python
# TODO: reimplement using polars natively
```

The comment is misleading — the fix is actually to use PyArrow's native `.sort_by()` method
directly, eliminating the Polars dependency for this code path and reducing conversion overhead.
