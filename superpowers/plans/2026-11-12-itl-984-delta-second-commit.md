# ITL-984 plan: second commit to a Delta table holding a list of a binary-stored type

Diagnosis: `.untracked/2026-11-12-itl-984-delta-second-commit-diagnosis.md` (copied from the
ITL-858 worktree) and the third Linear comment. Scope: `DeltaTableDatabase` only.

## Fix (src/orcapod/databases/delta_lake_databases.py)
1. `_stores_as_is`: reduce extension types to their storage types before `_as_delta_writer_stores`
   (logical list `list[uuid]` -> `large_list<large_binary>` -> Delta `list<binary>`), so new ids append.
2. MERGE source in the stored layout (`_merge_source_in_stored_layout`): list columns (logical lists
   reduced to storage) cast to the stored type. Chosen over drop-ids-and-append because it keeps the
   MERGE (`when_not_matched_insert_all` skips a stored id, a null id never matches) exactly as today
   for every case; the first attempt (drop and append) changed the operation pinned by
   `test_a_stored_id_goes_through_merge_and_keeps_the_stored_row`, so it was reverted.

## Tests first (tests/test_databases/test_delta_list_binary_commits.py)
- pod `-> list[uuid.UUID]`: SyncPipelineOrchestrator().run(dag), AsyncPipelineOrchestrator().run(dag),
  second job.run() adding rows; stored table equals a single commit; read back has logical type.
- plain `list[bytes]` with an id already stored (add_records twice).
- regression: scalar uuid, `list[str]`.

## Finish
Golden suite unchanged, full suite, changelog.d/ITL-984.md, design_issues entry only if one exists.
PR body: ITL-871 (#300, tests/test_torch/test_tensor_pipeline_stores.py) / ITL-981 (#328) xfails.
