## `src/orcapod/databases/delta_lake_databases.py`

### D2 — `flush_batch()` uses `mode="overwrite"` for new table creation
**Status:** open
**Severity:** high

`flush_batch()` (line ~856) creates new Delta tables with `mode="overwrite"`:
```python
# TODO: reconsider mode="overwrite" here
```

If a table already exists at that path (race condition, stale state, or misconfigured pipeline
path), existing data is silently destroyed. Should use `mode="error"` to fail fast, or
`mode="append"` with an explicit existence check.
