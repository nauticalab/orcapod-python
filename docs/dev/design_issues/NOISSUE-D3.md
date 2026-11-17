## `src/orcapod/databases/delta_lake_databases.py`

### D3 — `_refresh_existing_ids_cache()` loads entire Delta table into memory
**Status:** open
**Severity:** high

The method (line ~252) calls `to_pyarrow_table()` on the full Delta table just to extract the
ID column:
```python
# TODO: replace this with more targetted loading of only the target column and in batches
```

For large tables, this is a critical memory bottleneck. Delta Lake supports column projection
(`columns=[id_col]`) and batch reading, which would reduce memory usage by orders of magnitude.
