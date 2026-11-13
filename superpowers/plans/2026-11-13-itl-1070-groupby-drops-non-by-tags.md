# ITL-1070 — GroupBy drops the tags that are not in `by`

Status: APPROVED by coordinator 2026-11-13 (option A: no identity change; delete changelog.d/ITL-955.md and make ITL-1070.md the net entry; keep _null_last_key recursion; stale-table gap filed as ITL-1072). Implemented.

Branch: `eywalker/itl-1070-groupby-drops-the-tags-that-are-not-in-by` (from origin/main 5c7a058f).
History: ITL-955 (#316) made non-key tags list-valued tags. Future opt-in mode: ITL-1071 (not ours).

## Goal

`GroupBy(by)` output tag columns are exactly `by`, in input tag order, scalar. Non-key tags are
dropped. Data columns, source info, system-tag folding, member order (still sorted by the
input's non-key tag values, record-id tiebreak) and group order are unchanged.

## Code sites (src/orcapod/core/operators/group_by.py)

- `unary_static_process`: keep sorting members on the input's non-key tags + record ids (rows
  from `to_pylist` still have them); exclude the non-key tags from `member_columns`; build the
  aggregated table from the input schema minus the dropped tags (`build_aggregated_table`
  iterates the whole input schema, so they must be removed from it, else they come back as
  null scalars); declare `tag_columns = [t for t in input tags if t in by]` (input order).
- `unary_output_schema`: drop non-key user tags; system tags keep the suffixed names.
- No `async_execute` of its own: the async path is the barrier path over `static_process`.
- System-tag naming/folding: untouched (fold of member values, `::{pipeline_hash}` suffix).
- `_null_last_key`: keep the recursion. It is no longer reached via re-grouping a GroupBy
  output, but list-valued tags still reach GroupBy as non-key tags from a source with a
  list-typed tag column or a Join with a `Batch` output; docstring updated.
- `identity_structure` / `to_config`: `("GroupBy", by)` only; no change.
- `streams/base.py` `group_by` docstring; class docstring.

## Measurement

Script: scratchpad `measure.py` (source -> GroupBy -> counting pod in a PipelineJob on Delta,
plus OperatorJobNode LOG cache, plus `as_source()`), run on main and on the change, and the
main store re-run with the change.

## Tests (failing first)

`tests/test_core/operators/test_group_by.py` (public `op(stream)`), async/sync equivalence,
chained aggregation (re-group coarser key), aggregation job (stored node), golden GroupBy entries.

## Docs

docs/concepts/operators.md, CLAUDE.md + .zed/rules (boundary paragraph, rule 3, GroupBy bullets),
changelog.d/ITL-1070.md, design_issues/ITL-1070-GB2.md.

## Measurement results (main vs prototype, 2026-11-13)

Prototype diff saved at scratchpad `proto.diff` (10 lines in group_by.py).

- Stream with a non-key tag: content hash, pipeline hash, system-tag column names and values,
  every row's data hash, data lists and their order, `_source_*` lists: IDENTICAL. Only the
  tag keys/schema lose `probe`. GroupBy op identity `('GroupBy', by)` / config unchanged.
  Downstream pod node: pipeline hash, content hash, table path unchanged; pdb table loses
  `probe` column; result (rdb) table unchanged. odb (LOG) path unchanged; table loses `probe`.
  `as_source()` DerivedSource: source_id and content hash unchanged; pipeline hash MOVES
  (root = schema).
- Stream with no non-key tag: everything byte-identical, including stored tables.
- Golden: no value moves. Six `pipeline:group_by.row[...]` keys are renamed (row label is the
  user-tag dict; `subject` list leaves it); values equal.
- Cross-version (store written on main, then the change): paths are the SAME, so old tables ARE
  found: pod: no recompute (rdb reused), but pdb write fails (Delta "Non-nullable column
  violation for probe") -> StoreWriteError; odb LOG write fails the same way; odb REPLAY serves
  rows that still carry `probe` while keys() lacks it; counter get_all_records returns `probe`.
  Only stores written by v0.3-dev after #316 (Nov 12) with a GroupBy over a non-key tag.
  v0.2-shape stores (GroupBy -> SelectDataColumns(path) -> pod) are served exactly
  (TestStoreWrittenBeforeTagsStayedTags second test passes except its keys assertion).
- Tests relying on list tags: 31 failures in targeted run (test_group_by TagsStayTags +
  Ordering, async_sync_equivalence, chained_aggregation x3, empty_inputs[GroupBy],
  aggregation_job legacy x2, spiraldb tags_as_data workshop case, tensor/array test_group_by,
  golden x9). docs/concepts/sources.md sample uses `.drop_tag_columns("timestamp")` after
  group_by (now gone). PR #346: `group_by_g` layout `all_but_g` -> `data_only`, checker
  comment on list-valued tags, job `grouped` node loses `n`.

## Notes for successor

Implemented, tests/docs/changelog/design issue done; ITL-1048 note posted. Next: PR, Copilot review, CI on 3.11/3.12/3.13.
