## Cross-cutting

### CC1 — Empty tables lose field nullability, aborting `error_policy="continue"` pipelines
**Status:** resolved
**Severity:** high
**Issue:** ITL-563

Five sites construct empty PyArrow tables by building a dict of `pa.array([], type=...)` and
passing it to `pa.table()`. Because `pa.table(dict_of_arrays)` marks every field
`nullable=True`, required fields (`str`, `int`, …) lose their non-nullable annotation.
When a failing function's empty output is fed into a `Join`, the mismatched schemas raise an
`InputValidationError` that bypasses `error_policy="continue"` and aborts orchestration.

Sites: `sync_orchestrator.py:_materialize_as_stream`, `operator_node.py:_make_empty_table`,
`side_effects.py:SideEffectPodStream.as_table`,
`side_effects.py:SideEffectNode.as_table`,
`derived_source.py:DerivedSource._get_stream`.

**Fix:** Extracted `arrow_utils.make_empty_table(python_schema, type_converter)` using
`python_schema_to_arrow_schema` + `pa.Table.from_batches([], schema=...)`. Replaced all five
buggy sites: `sync_orchestrator._materialize_as_stream`, `operator_node._make_empty_table`,
`SideEffectPodStream.as_table`, `SideEffectNode.as_table`, and `DerivedSource._get_stream`.
Also fixed a latent null-typed array bug in `Join.static_process`. Added unit tests,
integration test for the exact bug topology, and per-site regression tests. PR: ITL-563.
