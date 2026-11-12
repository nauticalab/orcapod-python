## `src/orcapod/databases/delta_lake_databases.py`

### D1 — `flush()` swallows individual batch write errors
**Status:** open
**Severity:** critical

`flush()` (line ~817) iterates over all pending batches, logs errors individually, but never
raises. Callers have no way to know that writes partially or fully failed:
```python
# TODO: capture and re-raise exceptions at the end
```

This is silent data loss — a batch write can fail and the caller proceeds as if everything
was persisted.

Fix: accumulate exceptions during the loop; raise an `ExceptionGroup` (or custom aggregate
error) at the end containing all failures.
