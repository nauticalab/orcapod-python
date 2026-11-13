# ITL-1046 plan: Index after Batch/GroupBy writes the element's own token

Worker notes (a successor can continue from here).

## Bug
`Index._process_one`: `new_src = f"{old_src}[{i}]" if old_src else None`. When `old_src` is a
list (token of a many->one stream) the f-string stringifies the list.

## Fix
- Scalar token: unchanged (`f"{old_src}[{i}]"`, null when falsy).
- List token: `old_src[effective_i]` (the element's own token; `effective_i` handles negatives; the
  index is in range, since out-of-range rows are skipped before this point). Nested lists: the
  element is the inner list, so one level of nesting is removed. A list token shorter than the
  value (cannot happen from Batch/GroupBy) yields null.
- Type: `Data.with_source_info` types the token from the Python value
  (`arrow_utils.source_info_arrow_type`), so one level is removed by construction.
- Pick: never sees a list token for a dict column from Batch? (Batch makes `list[dict]`, Pick needs
  a dict). Checked below.

## Tests (public interface, inputs not in tag order)
tests/test_core/operators/test_index_source_token.py: Batch->Index, GroupBy->Index, two
aggregations->Index, out-of-range (skip) , negative index, zero-row, async == sync.

## Out of scope
Stored tables already written keep old tokens (migration tool, ITL-1048).
