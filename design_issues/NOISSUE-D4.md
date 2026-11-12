## `src/orcapod/databases/delta_lake_databases.py`

### D4 — `_refresh_existing_ids_cache()` catches missing column reactively
**Status:** open
**Severity:** high

In the same method (line ~257), if the ID column doesn't exist, a `KeyError` is caught as a
fallback:
```python
# TODO: replace this with proper checking of the table schema first!
```

Schema should be validated proactively by loading schema metadata before attempting to read
the column.
