## `src/orcapod/core/data_function.py`

### P2 — `CachedDataFunction.call` silently drops the `RESULT_COMPUTED_FLAG`
**Status:** resolved
**Severity:** high
On a cache miss, the flag is set but the result is discarded:
```python
output_data.with_meta_columns(**{self.RESULT_COMPUTED_FLAG: True})  # return value ignored
```
If `with_meta_columns` returns a new data (immutable update), the flag is never actually
attached.

**Fix:** Assigned the return value: `output_data = output_data.with_meta_columns(...)`.
Added tests verifying the flag is `True` on cache miss and `False` on cache hit.
