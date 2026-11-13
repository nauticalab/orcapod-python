# ITL-1066: SideEffectPodStream.as_table honours every ColumnConfig

## Problem
`SideEffectPodStream.as_table` (src/orcapod/side_effects.py) hands one ColumnConfig to
`tag.as_table` and `data.as_table` and hstacks: context/all_info raise (duplicate
`_context_key`), content_hash and sort_by_tags are ignored.

## Plan
1. Tests first (tests/test_core/side_effect_pod/test_side_effect_stream_as_table.py), public
   interface: every ColumnConfig flag alone + all_info, against the pass-through input stream's
   as_table (same columns, order, types), against FunctionPodStream where comparable, content
   hash belongs to its row after sort_by_tags (ITL-1029), hash-name collision as other streams.
2. Fix: build one full table (tags + system tags, data + source + meta + context) per the
   function-pod `_table_from_pairs` approach, shape with
   `arrow_utils.apply_column_config` + `with_content_hash_column`. Empty result: declared
   all_info schema, shaped the same way. head() untouched.
3. Check the side-effect job node as_table with the same configs; report; fix only if a few lines.
4. changelog.d/ITL-1066.md, design_issues entry, PR "Fixes ITL-1066", Copilot review.

## Notes
- Out of scope: hashes, stored schema, delivery log/semantics.
