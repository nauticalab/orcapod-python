## `src/orcapod/hashing/`

### H2 — Arrow hasher processes full table at once
**Status:** open
**Severity:** medium

`SemanticArrowHasher._process_table_columns()` (`arrow_hashers.py:104`) calls `to_pylist()`
on the entire table, loading all rows into Python memory:
```python
# TODO: Process in batchwise/chunk-wise fashion for memory efficiency
```

For large tables, this is a significant memory bottleneck. Should use Arrow's `to_batches()`
for chunk-wise processing.
