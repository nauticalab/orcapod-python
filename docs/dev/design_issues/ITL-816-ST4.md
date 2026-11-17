## `src/orcapod/core/streams/arrow_table_stream.py` — row iteration

### ST4 — `iter_data` drops and mis-pairs rows when tag and data columns are chunked differently
**Status:** resolved
**Severity:** critical
**Issue:** ITL-816

`ArrowTableStream.iter_data` called `to_batches()` separately on the tag-column selection and
the data-column selection of the backing table and `zip`ped the two batch lists.
`Table.to_batches()` cuts at the union of the chunk boundaries of the *selected* columns, so
the two lists differ whenever tag and data columns have different chunk layouts — which is
what the orchestrator produces when it rebuilds a node's output with
`hstack_tables(concat_tables(tags), concat_tables(data))`. `zip` truncated to the shorter
list (rows silently dropped: 6 rows from `as_table()`, 4 from `iter_data()`), and batches of
unequal length paired a tag with another row's data or raised
`Table must contain exactly one row`.

A second bug of the same family sat in the same loop: per-row source info was read with
`self._source_info_table.slice(i, 1)` where `i` is the index *within the current batch*, so
in any multi-batch table every row after the first batch received the source info of a row in
the first batch.

**Fix:** `iter_data` now batches the whole backing table once
(`self._table.to_batches()`, whose batches are aligned across all columns by construction),
selects the tag and data columns out of each batch, and indexes source info by absolute row
position. No table copy and no per-row `Table.slice`; measured no slower than before on a
100k-row table. Regression tests: `tests/test_core/streams/test_iter_data_chunk_layouts.py`.
