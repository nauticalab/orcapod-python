## `src/orcapod/core/nodes/operator_node.py`

### ON1 — OperatorJobNode has no v0→v1 schema migration for `NODE_CONTENT_HASH_COL`
**Status:** open
**Severity:** high
**Issue:** ITL-539

`OperatorJobNode` stores `NODE_CONTENT_HASH_COL` as `large_binary` (changed in ITL-539),
but unlike `FunctionJobNode` and `ResultCache`, it has no `_ensure_schema()` guard and no
v0→v1 migration utility. Any existing pipeline DB written by the old code that stored
`NODE_CONTENT_HASH_COL` as `large_string` will fail with an Arrow schema mismatch on the
next write attempt. This is accepted as a breaking change for a pre-v0.1.0 project; users
must drop and recreate the affected pipeline DB tables manually.

A proper migration utility (analogous to `migrate_pipeline_v0_to_v1`) should be implemented
before v0.1.0 ship.
