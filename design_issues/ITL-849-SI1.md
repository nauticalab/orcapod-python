## `src/orcapod/core/streams/arrow_table_stream.py`, `src/orcapod/utils/arrow_utils.py` — nested source info

### SI1 — Aggregating an already aggregated stream raised `ArrowTypeError`
**Status:** resolved
**Severity:** high
**Issue:** ITL-849

`GroupBy` or `Batch` over a stream whose source-info columns were already list-valued (the
output of `MergeJoin`, `Batch` or `GroupBy`) raised `ArrowTypeError: Expected bytes, got a
'list' object` as soon as its rows were materialized (`as_table`, `iter_data`, any hash). The
aggregation itself was correct: `build_aggregated_table` typed `_source_x` as
`large_list<large_list<large_string>>`. The crash was downstream of it.
`ArrowTableStream.iter_data` passed each row's source info to `Data` as Python values, and
`prepare_prefixed_columns` typed every list token as `list<large_string>`, one level too
shallow. All five allowed chains failed (MergeJoin→GroupBy, MergeJoin→Batch, Batch→Batch,
GroupBy→Batch, GroupBy→GroupBy), sync and async. Batch→GroupBy is rejected by design
(`GroupBy` keys only on scalar tags).

**Fix:** `iter_data` hands each row's source info to `Data` as Arrow columns, batched together
with the tag and data columns so rows still pair up across chunk layouts (ITL-816), so the
stream's type is kept instead of re-inferred. `arrow_utils.source_info_arrow_type` (moved from
`tag_data`) is the one source-info type rule and types Python-supplied tokens at any depth:
each list level takes its element type from the first element that is not `None` or an empty
list, so `[[], [["a"]]]` is three levels deep. `Data`'s Python source-info type is read off it.
The result is a list of the incoming list type (`large_list<large_list<...>>`, element types
exactly the large-normalized inputs, `Path` as `list[list[orcapod.path]]`). The predicted schema
equals the materialized one. Hashes of pipelines that already worked are unchanged.
Regressions: `tests/test_core/operators/test_chained_aggregation.py`. Open: on the async
barrier path, per-row source info is still row 0's (ITL-845); that assertion is a strict
xfail.
