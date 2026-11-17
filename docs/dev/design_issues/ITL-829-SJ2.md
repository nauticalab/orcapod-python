## `src/orcapod/core/operators/semijoin.py` — nested columns and string/large_string keys

### SJ2 — Joins rejected a tag key that differs only in nullability
**Status:** resolved
**Severity:** medium
**Issue:** ITL-829 (second case)

`SemiJoin`, `Join` and `MergeJoin` raised `InputValidationError` when a shared tag key was `T`
in one input and `T | None` in another, e.g. a stream with a declared non-nullable key against
a table built without a schema ("Type conflict for key 'id': <class 'int'> vs int | None" from
`schema_utils.intersection_schemas`, or "Types int | None and <class 'int'> are not
compatible" from `union_schemas`). The graph could not be built, although a null key matches
nothing and the result is clear. The strict xfail `test_binary_op_delegation` in
`tests/test_channels/test_node_async_execute.py` recorded it for `SemiJoin`.

**Fix:** developer decision, November 13, 2026: the three joins accept such a key.
`core/operators/key_nullability.py` holds the rule: `reconcile_key_nullability` gives both tag
schemas the non-nullable type for a key whose types are equal once `None` is removed (only tag
schemas; any other difference is left to raise as before), and `key_nullable` makes the output
key nullable only when every input has it nullable. `Join` (prediction, sync field flags after
`restore_schema_nullability`, async `_merge_tag_tables`) and `MergeJoin` (prediction, sync
`output_nullable`) give the key `T` when any input has it `T`; `SemiJoin` validates with the
reconciled schemas and keeps the left input's schema. Flags are set by re-wrapping arrays,
never by a cast. Inputs with equal nullability take the unchanged path, so no stored value or
golden hash moves. Tests in `tests/test_core/operators/test_join_key_nullability.py`, cases in
`test_async_sync_equivalence.py`, golden entries `pipeline/join_key_nullability`.
