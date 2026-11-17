# ITL-940: reject a tag/data name collision at graph-build time

Branch `eywalker/itl-940-join-and-mergejoin-accept-a-tag-column-and`; PR against `main`, Fixes ITL-940.
Contained bug fix: no hash, golden, stored-table or dependency change.

## Defect (measured on main)
1. `Join` / `MergeJoin` of tag `x` with data `x`: accepted at build; sync read raises raw `KeyError`; async
   `Join` silently yields rows with different tag/data `x`.
2. `Index(out=<tag>)` / `Pick(out=<tag>)` accepted at build, fail on read.
3. Function pod whose output key equals an input tag: accepted; read fails / mismatched values.
SemiJoin: matches on every common column (tag or data) by value and returns the left stream unchanged,
so a tag/data name overlap yields no duplicate column: not a gap (documented, tested, unchanged).

## Fix (InputValidationError naming the column, the roles, and the remedy)
- `operators/base.py`: `require_tag_data_roles_agree(first_tags, first_data, second_tags, second_data)`; the
  caller adds operator name / input positions.
- `Join._joined_user_schemas` (feeds `output_schema`, `validate_nonzero_inputs`, `static_process`): every pair
  of inputs. Async: `_binary_streaming_join` checks the first row of each side (also covers the staggered chain).
- `MergeJoin.binary_output_schema` (feeds `validate_binary_inputs` and `binary_static_process`, hence async).
- `Index` / `Pick.validate_unary_input`: `out` equal to a tag name. (In-place `out=None` replaces a data
  column and cannot collide.) The async path of these two has no stream and is only reachable after
  `validate_unary_input` in a pipeline, as before.
- Function pod: `schema_utils.require_output_keys_not_tags` at `_FunctionPodBase.validate_inputs`/`output_schema`,
  `FunctionPod.process`, `CachedFunctionPod.process`, `FunctionNode.__init__`; async: first row in
  `_FunctionPodBase.async_execute` (outside the row-failure handler, which would skip silently).
  The pod call's automatic join goes through `handle_input_streams` -> `process`, so it is covered.

## Tests (written first, shown failing)
`tests/test_core/operators/test_tag_data_collision.py` plus async cases in `test_async_sync_equivalence.py`.

## Also
design_issues entry, `changelog.d/ITL-940.md`, one line in `docs/concepts/operators.md` (Join).
