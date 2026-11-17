# ITL-884 — remove message-only parameters (developer review of PR #290)

**Review:** comment 4536812218 on `src/orcapod/databases/delta_lake_databases.py`
(`_read_delta_table`'s new `record_path`): a function must not receive a value used only to
build an error message; the caller, which has the context, wraps the error.

**Goal:** no helper in the PR diff takes a parameter it uses only for a message. User-visible
behaviour and every `StoreReadError` / `InputValidationError` message stay exactly as they are.

## Audit of the PR diff (`git diff origin/main...HEAD`)

| Function | Parameter | Used for | Action |
|---|---|---|---|
| `DeltaTableDatabase._read_delta_table` | `record_path` | message only (`_read_failed`) | Remove; restore main's signature. The function raises a module-private `_DeltaTableReadError` (backend error as `__cause__`) for a failed scan; its three callers catch it and raise `self._read_failed(record_path) from exc.__cause__`. A bad filter still propagates as is. |
| `DeltaTableDatabase._require_compatible_append` | `table_uri` | message only | Becomes `_incompatible_append_column(delta_table, batch)`, returning `(column, stored_type, incoming_type)` or `None`; `flush_batch` (owns `table_uri`) raises the `InputValidationError` with the same message. |
| `_store_error_to_raise` | `record_key` in the `(record_key, error)` pairs | message only (the notes) | Takes `list[Exception]` and only selects; `flush` adds the notes naming the other batches' keys. |
| `async_orchestrator._single_store_error` | `topo_order` | nothing: ranks by `node_label`, which `StoreReadError` lacks, so the first error in the group always wins | Remove the parameter and the ranking (and the `topo_order` pre-init in `run`); select the first store error in group order. Same behaviour. |
| `DeltaTableDatabase._read_failed` | `record_path` | builds the error (its job); called only by owners of `record_path` | Keep. |
| `_get_delta_table`, `_refresh_existing_ids_cache`, `get_all_records` | `record_path` | locating the table (work) | Keep. |
| `flush_batch` | `table_uri`, `abs_path_components` | write location (work) | Keep. |
| `StoreReadError.__init__` | `record_path` | public attribute; not in the message, not read in src/tests | Keep as error payload (coordinator approved). |
| `FunctionJobNode` / `_FunctionPodBase` `StoreReadError` handlers | `node_label`, `pod_label` | observer callbacks (work) | Keep. |

## Steps

1. Ask the coordinator to approve the table (repo review process). **Status: approved** (item 8 kept:
   payload for a caller that catches the error, same shape as `StoreWriteError` on PR #287).
2. Edit `delta_lake_databases.py`, `async_orchestrator.py`; restore
   `tests/test_databases/test_delta_table_database.py:564` to main's call.
3. Tests unchanged in behaviour: `tests/test_databases/test_delta_read_failures.py`,
   `tests/test_pipeline/test_store_read_failure.py`, full suite on 3.11–3.13, ruff,
   basedpyright.
4. Update the earlier ITL-884 plans' descriptions where they name the removed parameters.
5. One commit, push, reply on thread 4536812218 (do not resolve), CodeRabbit round, CI.

## Notes for successors

- PR #293 calls `_read_delta_table`; after this it no longer passes `record_path`.

## Status

- Steps 2–4 done: suite 6926 passed / 96 skipped / 15 xfailed on 3.11, 3.12 and 3.13; ruff clean;
  basedpyright 0 errors (warnings 1595 → 1587).
- `_single_store_error` here is now `(group) -> StoreReadError | None`, picking
  `StoreReadError.in_group(group)[0]`. PR #287's version takes `topo_order` and ranks
  `StoreWriteError` by `node_label`; whichever PR merges second reconciles the two.

## CodeRabbit round on 0369cfcc (thread 4537325251, Major)

The append check compared whole struct types, so adding a nested struct field (which main accepts
and Delta's merge adds) was refused: a regression from this PR. Fix (coordinator approved): the
module function `_types_conflict(stored, incoming)` matches struct fields by name at every depth,
through list and map types; a field on one side only is not a conflict, like a top-level column.
`_incompatible_append_column` still returns what differs; `flush_batch` raises. New tests:
`test_nested_field_type_change`, `test_new_struct_field`, `test_new_struct_field_inside_a_list`
(stored schema and null back-fill asserted). Suite 6929 passed / 96 skipped / 15 xfailed on 3.11–3.13.

## Merge of main (#287, StoreWriteError) — approved design D1–D8 (coordinator, November 11, 2026)

- D1 `StoreError(RuntimeError)` base (public, exported, documented; name may be renamed by the
  developer), `in_group` on the base; all handlers type `StoreError`.
- D2 write sites let `StoreReadError` pass through; everything else still wrapped as
  `StoreWriteError`. Test: refused append is a `StoreWriteError` (cause `InputValidationError`);
  read failure during a write is a `StoreReadError` with its cause.
- D3 `node_label`/`tag` + `add_context` on the base; orchestrators annotate a `StoreError`
  escaping a node with its label. Nothing threaded into databases or `ResultCache`.
- D4 `StoreReadError` message names node and row when known.
- D5 `_single_store_error(group, topo_order)` ranks every `StoreError` by node order.
- D6 sync orchestrator catches `StoreError`.
- D7 CHANGELOG / DESIGN_ISSUES / function-pods concept page.
- D8 `tests/test_pipeline/test_store_errors_together.py` (sync/async × both orders, async
  order independence).

## CodeRabbit round on 4f0b4f1c (coordinator approved)

Accepted: CHANGELOG wording (refused append writes nothing *to that table*; new fields pass the
check, Delta adds them per `allow_schema_evolution`), concept page scoped to `DeltaTableDatabase`
and says a refused append in a run is a `StoreWriteError`; async `record_and_forward` names node
and row on a `StoreError`; `_start_node` labels store errors inside a node's exception group.
Declined: pending-rows read skip (fixed by #293). Deferred: table data without a `_delta_log`
and connector-DB read errors -> ITL-916.

## CodeRabbit round on cf029bbf (coordinator approved)

Regression: non-microsecond timestamps (and `list[Path]`, found by the new differential guard)
could not be appended to an existing Delta table. Fix: `_as_delta_writer_stores` applies the
writer's own conversion, `_stored_arrow_schema` reads the stored schema without field metadata.
Guard: `tests/test_databases/test_delta_append_types.py` (every Arrow/logical type, identical
schema, same view and reopened; 40 cases failed before the fix). Deferred: ExceptionGroup-wrapped
store errors -> ITL-918; fixed_size_list merge failure (also on main) -> ITL-916 (strict xfail).

## CodeRabbit round on ffe44481

Regression: rows whose column (or nested field / list item) is `null`-typed could not be appended
to a typed column; `main` accepts them. Fix: `_without_null_types` leaves `null` parts out of the
comparison (a `null` struct field is dropped; a column holding `null` list items or map entries is
not compared). Tests in `test_delta_append_types.py` (three null cases, all passing on `main`;
a `null` field does not hide a changed sibling).
- Round on 51dc9050: `_without_null_types` keeps a list or map whose items still have comparable
  fields (a changed sibling of a `null` field inside `list<struct>` is refused); the nested-null
  tests now build `{"a": None}` / `[None]` and assert the stored value. A `null` field inside
  `list<struct>` cannot be appended by Delta's merge itself (also on main), so it is not a case
  of the append test. CI's "Run Tests" / "Objective" / "PostgreSQL" / license-check workflows fail
  on every branch since ~14:25Z: the runners cannot resolve security.ubuntu.com (apt install of
  graphviz); "Python — Required CI" passes.
