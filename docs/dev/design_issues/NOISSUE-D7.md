## `src/orcapod/databases/delta_lake_databases.py`

### D7 — Silent deduplication in `_deduplicate_within_table()`
**Status:** open
**Severity:** medium

The method (line ~383) silently drops duplicate rows with no warning:
```python
# TODO: consider erroring out if duplicates are found
```

Duplicates may indicate an upstream bug. Should at least log a warning; consider making
behavior configurable (warn, error, or silent).
