## `src/orcapod/core/streams/`

### ST2 — `iter_data()` does not support table batch streaming
**Status:** open
**Severity:** low

`ArrowTableStream.iter_data()` (line ~261) works only with fully materialized Arrow tables,
not with `RecordBatchReader` or lazy batch iteration:
```python
# TODO: make it work with table batch stream
```

Relevant for future streaming/chunked processing of large datasets.
