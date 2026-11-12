## `src/orcapod/core/streams/`

### ST1 — `drop_data_columns` may leave orphan source-info columns
**Status:** open
**Severity:** medium

The `StreamProtocol.drop_data_columns()` method (line ~309) drops data columns but it is
unclear whether the corresponding `_source_<col>` columns are also removed:
```python
# TODO: check to make sure source columns are also dropped
```

If source-info columns survive after the data column is dropped, downstream consumers may see
stale provenance data or schema mismatches.
