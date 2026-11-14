# ITL-1095: a job node shows the current run; load-only run

Status: all four stages implemented on draft PR #380 (stage 1 da66b836, stage 2 c90eb966, stage 3 b693b50e, stage 4 3610fd20). Next: wait for #378 to merge, merge main, CI, CodeRabbit.
Base: PR #378 branch head `2e9972e0` (ITL-1093). Branch: `eywalker/itl-1095-design-what-a-job-node-shows-the-current`.

Rule (developer, November 13, 2026): a job node itself (`as_table`, `iter_data`, dataframe
methods, `flow`, `head`, the preview, `run`) is the CURRENT RUN view; `node.as_source()` is the
view of everything stored for the node. Before a run a node is empty; a run fills it, also when
everything is loaded. `run(load_only=True)` fills nodes from what is stored and computes nothing.
`node.run()` cascades to the job nodes it depends on (developer, later the same day).

## 1. Measurements on the base (`2e9972e0`)

Scratch scripts: `measure.py`, `replay.py`, `noderun.py` in the session scratchpad.
Pipeline: `DictSource(source_id="s", tags g,k)` -> `inc` (`rowwise`) -> `group_by("g")`
(`grouped`, operator) -> `total` (`summed`). Runs over 6 rows then 3 rows, one Delta store.

| Read | sync | async |
|---|---|---|
| after run over 3 rows: rowwise / grouped / summed `as_table` | 3 / 2 / 2 | 3 / 2 / 2 |
| `as_source()` of rowwise / summed | 6 / 4 | 6 / 4 |
| fresh job (3 source rows), NOT run: rowwise / grouped(OFF) / summed `as_table` | **6** / 0 / **4** | same |
| fresh job, not run: `head()` rowwise | `rows`, 6 of 6 (stored) | same |
| fresh job: `summed.run()` alone (upstream OFF operator not run) | 0 rows, nothing computed | - |
| fresh job: source node `as_table` | 3 (bound source) | same |

Operator node `grouped` before a run (fresh job over a store with two earlier runs):

| cache mode | `as_table` | `head` | `as_source` |
|---|---|---|---|
| OFF | 0 | not_computed "not run; cache mode OFF stores nothing" | 0 |
| LOG | 0 | not_computed "rows of earlier runs are stored but not replayed" | 4 (both runs) |
| REPLAY (after two LOG runs) | **4** (both runs) | rows 4 | 4 |

REPLAY after a run over 3 rows (sync and async alike): `grouped` holds **4 groups**
`(0,[1,21]) (0,[1,21,41]) (1,[11]) (1,[11,31,51])` -- every logged run, not the current run --
and `summed` downstream gets 4 rows. The operator cache (`odb_v1`) stores no run id or time, so
"the latest logged run" cannot be told apart without a new stored column.

`node.run()` with an upstream job node that has not run (chain `a` -> `b`, store holds a 6-row run,
source now 3 rows): `b.run()` reads `a`'s STORED rows (6, not the current 3) and returns 6 rows,
0 function calls; over an empty store it returns 0 rows silently. `node.run()` never runs upstream.

Loaded job (`PipelineJob.load`, in-memory source unbound): function nodes are `READ_ONLY` stubs
showing every stored row (rowwise 6, summed 4); the OFF operator raises `RuntimeError` from
`as_table` (head: unavailable); source node raises `UnboundSourceError`; `loaded.run()` raises.
Loaded `rowwise.as_source()` reads 6 rows.

Load statuses: a built job's function nodes are always `FULL`. A loaded node without a pod is
`READ_ONLY` (store attached) or `UNAVAILABLE`. `CACHE_ONLY` and `READ_ONLY` with a
`DataFunctionProxy` arise only from `FunctionJobNode.from_descriptor(..., function_pod=...)`,
which no public path calls (`PipelineJob.load` passes no pod).

Orchestrators: both pass rows between nodes in their own buffers/channels; no node read is
involved in `job.run()`. Only `node.run()` reads its upstream node.

Experiment (reverted): making every un-run read empty fails 400 of 4,464 job-related tests in
21 files (list in section 5).

## 2. Decisions (coordinator, November 13, 2026; proposal in 2026-11-13-itl-1095-proposal.md)

- D1 REPLAY: R1 for the tag (after a run: union of every logged run; only the before-run read
  becomes empty). ASKED of the developer.
- D2 (a): load-only uses only the node's own pipeline records; result-store-only hits are not stored.
- D3 (a)+(i): operators run on the rows that reach them (LOG writes nothing); summary and node
  status mark every node below a skipped input "inputs incomplete".
- D4: orchestrator passes side-effect node rows through uncalled in load-only; node reads empty.
- D5: NodeRunSummary.not_stored, .inputs_incomplete; RunSummary.load_only; loaded rows in reused;
  repr "rowwise: 6 rows: 3 loaded, 3 not stored (skipped)", "summed: 2 rows: 2 loaded (inputs incomplete: rowwise)".
- D6: amend ITL-1081 rule: run() takes no positional arguments; a job node's run() (and
  job.run/run_async) takes keyword-only load_only; non-job streams' run() stays argument-less.
- D7 + developer: state-driven run. A node (itself or ancestor) that holds a completed run and is
  not stale does not run again: run() returns its held view. Staleness = the existing
  modified-time mechanism; a freshly built or loaded job has every node stale. Load-only-filled
  is its own status (ViewStatus e.g. LOADED_ONLY, caption "N loaded, M inputs without a stored
  result"), never complete: a full run re-runs it in full. A load-only run that finds everything
  stays loaded-only by default (ASKED). Cascade = ancestor sub-DAG on the sync orchestrator, one
  mechanism shared with job.run. Summary on node.last_run. Commits per row + flush at end.
  job.run() behaviour NOT changed (give options). Side-effect re-delivery with
  track_completion=False: clear_cache() then run(). Table-backed/polling sources changing without
  modified time: known limit, documented.
- D8 loaded jobs as recommended. D9 as recommended. D10 remove NOTHING_STORED. D11 as recommended.

Order: 1) empty before a run for every node kind + test updates; 2) load_only on job.run/run_async
+ summary; 3) node.run cascade + state rule + status; 4) loaded jobs. Each stage its own commit,
report counts after each. Async tests also on 3.11. Golden unchanged.

## 3. Implementation sketch (after approval)

- `FunctionJobNode`: no-run read empty (all load statuses); `head` NOT_COMPUTED with note;
  `execute`/`async_execute` take `load_only` (lookup `get_cached_results` only, skip misses, count
  them, no `_process_data_internal`, no writes); `_last_run_skipped` count; run mode flag.
- `OperatorJobNode`: REPLAY before run empty; load-only: OFF/LOG run the operator, LOG writes
  nothing; REPLAY replays.
- Orchestrators: `load_only` threaded through; side-effect node passes rows through uncalled.
- `PipelineJob.run/run_async(*, load_only=False)`; loaded job allowed with `load_only=True`.
- One internal "run these job nodes" function shared by `job.run` and the `node.run` cascade.
- `RunSummary`/`NodeRunSummary` new field(s).
- Tests first (public interface, both orchestrators, 3.11 for async).
- Docs, changelog fragment, design_issues entry, CLAUDE.md + .zed/rules.

## 5. Tests and docs that change

Failing under the experiment (count per file): test_node_head_sorting 162,
test_loaded_operator_cache 74, test_function_node_arrow_types 58, test_loaded_job_readback 29,
test_delta_arrow_type_roundtrip 25, test_node_head 9, test_loaded_logical_schemas 7,
test_provisional_hash_warning 6, test_function_pod_node_stream 5, test_stored_data_context 4,
test_function_node_empty_data 4, test_job_node_rows_after_run 3, test_tensor_job_load_back 2,
test_materialize_source_info 2, test_function_node_caching 2, test_cached_function_pod_job 2,
test_doc_samples (function-pods.md) 1, test_operator_node 1, test_operator_node_non_active 1,
test_function_node_iteration 1, test_readback_datagram_id 1, test_polars_dataframe_values 1.
Plus `tests/test_core/streams/test_run.py::test_every_stream_class_has_run_without_arguments`
(a `load_only` keyword on a node's `run`). Full names: scratchpad `hack_failed_tests.txt`.

Docs: docs/concepts/streams.md (run table row "A job node", head table row "A job node", the
"stream that has all of its rows" paragraph, line ~262), docs/concepts/function-pods.md (339-341
no-run read; "Saving and loading a job" 662-746 and its sample at 689-694, the only doc sample
that reads a node without a run), docs/concepts/sources.md 169-172; docstrings of
`PipelineJob.load`/`run`, `FunctionJobNode.head/as_table/iter_data/run/clear_cache/_cannot_compute_error`,
`OperatorJobNode.iter_data/as_table/head`, `ViewStatus.NOTHING_STORED`, `StreamBase.head`.

### Appendix: tests failing under the empty-before-run experiment (parametrizations collapsed)

```
tests/test_core/function_pod/test_cached_function_pod_job.py::test_saved_job_equals_the_uncached_pods_and_loads
tests/test_core/function_pod/test_function_node_arrow_types.py::test_store_read_back_keeps_tag_type
tests/test_core/function_pod/test_function_node_caching.py::TestPhase1Phase2PipelineEntryId::test_all_existing_entry_ids_skipped_in_phase2
tests/test_core/function_pod/test_function_node_caching.py::TestPhase1Phase2PipelineEntryId::test_phase1_yields_existing_results
tests/test_core/function_pod/test_function_pod_node_stream.py::TestFunctionNodeInactive::test_as_table_returns_cached_results_when_data_function_inactive
tests/test_core/function_pod/test_function_pod_node_stream.py::TestFunctionNodeInactive::test_inactive_filled_db_serves_cached_results
tests/test_core/function_pod/test_function_pod_node_stream.py::TestFunctionNodeStaleness::test_as_table_auto_detects_stale_and_repopulates
tests/test_core/function_pod/test_function_pod_node_stream.py::TestFunctionNodeStaleness::test_clear_cache_produces_same_results_on_re_iteration
tests/test_core/function_pod/test_function_pod_node_stream.py::TestIterDatasDbPhase::test_db_served_results_have_correct_row_count
tests/test_core/function_pod/test_polars_dataframe_values.py::TestPipelineStores::test_saved_job_loaded_back_reads_the_frames
tests/test_core/function_pod/test_readback_datagram_id.py::test_a_fresh_job_on_the_store_reads_the_stored_ids
tests/test_core/nodes/test_function_node_empty_data.py::TestAsyncProcessDataInternalEmptyData::test_async_empty_data_cache_hit_returns_cached_result
tests/test_core/nodes/test_function_node_empty_data.py::TestAsyncProcessDataInternalEmptyData::test_async_empty_data_cache_miss_raises
tests/test_core/nodes/test_function_node_empty_data.py::TestProcessDataInternalEmptyData::test_cache_hit_returns_cached_result
tests/test_core/nodes/test_function_node_empty_data.py::TestProcessDataInternalEmptyData::test_cache_miss_raises_ephemeral_result_missing_error
tests/test_core/nodes/test_function_node_iteration.py::TestIterDatasReadOnly::test_iter_after_db_populated_hot_loads_without_compute
tests/test_core/operators/test_materialize_source_info.py::TestStoreWrittenBeforeFix::test_rerun_after_pre_fix_store
tests/test_core/operators/test_materialize_source_info.py::TestStoreWrittenBeforeFix::test_v014_store_is_not_read
tests/test_core/operators/test_operator_node_non_active.py::TestCacheModeNonActive::test_iter_data_replay_mode_returns_db_contents
tests/test_core/operators/test_operator_node.py::TestOperatorNodeRunAndStorage::test_replay_from_cache
tests/test_databases/test_delta_arrow_type_roundtrip.py::test_a_fresh_job_reads_the_tag_type
tests/test_databases/test_delta_arrow_type_roundtrip.py::test_a_loaded_job_reads_the_tag_type
tests/test_databases/test_delta_arrow_type_roundtrip.py::test_a_rerun_on_the_same_store_writes_no_pipeline_rows
tests/test_databases/test_delta_arrow_type_roundtrip.py::test_an_operator_replay_reads_the_tag_type
tests/test_docs/test_doc_samples.py::test_doc_page_samples_run
tests/test_logical_types/test_provisional_hash_warning.py::test_reading_stored_results_of_the_type_warns_once
tests/test_pipeline/loaded_job/test_loaded_job_readback.py::test_binding_a_store_makes_a_storeless_loaded_node_readable
tests/test_pipeline/loaded_job/test_loaded_job_readback.py::test_datetime_output_feeding_an_operator_loads_with_its_type
tests/test_pipeline/loaded_job/test_loaded_job_readback.py::test_file_without_the_expected_rows_section_reads_without_a_check
tests/test_pipeline/loaded_job/test_loaded_job_readback.py::test_latest_readable_result_wins_across_both_stores
tests/test_pipeline/loaded_job/test_loaded_job_readback.py::test_live_node_hot_load_reads_the_latest_recomputed_result
tests/test_pipeline/loaded_job/test_loaded_job_readback.py::test_loaded_function_node_as_table_all_info_has_stream_columns
tests/test_pipeline/loaded_job/test_loaded_job_readback.py::test_loaded_function_node_as_table_returns_stored_rows
tests/test_pipeline/loaded_job/test_loaded_job_readback.py::test_loaded_function_node_iter_data_yields_stored_rows
tests/test_pipeline/loaded_job/test_loaded_job_readback.py::test_loaded_node_with_ephemeral_results_does_not_crash
tests/test_pipeline/loaded_job/test_loaded_job_readback.py::test_loaded_operator_node_raises_instead_of_returning_empty
tests/test_pipeline/loaded_job/test_loaded_job_readback.py::test_logical_type_output_schema_fails_naming_the_column_and_saved_type
tests/test_pipeline/loaded_job/test_loaded_job_readback.py::test_newer_unavailable_result_does_not_expose_old_persistent_result
tests/test_pipeline/loaded_job/test_loaded_job_readback.py::test_other_context_records_do_not_satisfy_saved_expectation
tests/test_pipeline/loaded_job/test_loaded_job_readback.py::test_path_output_feeding_join_still_allows_job_load
tests/test_pipeline/loaded_job/test_loaded_job_readback.py::test_recomputed_entry_is_read_once_with_its_latest_result
tests/test_pipeline/loaded_job/test_loaded_job_readback.py::test_saving_a_loaded_job_keeps_the_expected_rows
tests/test_pipeline/loaded_job/test_loaded_job_readback.py::test_store_content_warning_names_a_delta_stores_location
tests/test_pipeline/loaded_job/test_loaded_job_readback.py::test_store_with_only_another_runs_records_reads_none_with_a_warning
tests/test_pipeline/loaded_job/test_loaded_job_readback.py::test_unreconstructable_file_source_is_a_warning_with_the_reason
tests/test_pipeline/loaded_job/test_loaded_logical_schemas.py::test_registered_logical_output_keeps_join_available
tests/test_pipeline/loaded_job/test_loaded_operator_cache.py::test_a_format_0_1_0_file_finds_its_join_table_and_never_shows_its_rows_as_none
tests/test_pipeline/loaded_job/test_loaded_operator_cache.py::test_a_loaded_run_with_an_empty_result_reads_as_empty
tests/test_pipeline/loaded_job/test_loaded_operator_cache.py::test_a_pipeline_loaded_and_bound_to_its_sources_reuses_the_operator_cache
tests/test_pipeline/loaded_job/test_loaded_operator_cache.py::test_a_store_holding_only_other_runs_rows_gives_an_empty_result
tests/test_pipeline/loaded_job/test_loaded_operator_cache.py::test_an_absent_cache_table_gives_an_empty_result
tests/test_pipeline/loaded_job/test_loaded_operator_cache.py::test_fewer_rows_than_expected_warn_once_and_read_what_the_store_holds
tests/test_pipeline/loaded_job/test_loaded_operator_cache.py::test_get_cached_output_of_a_loaded_node_never_caches_unreadable_rows_as_none
tests/test_pipeline/loaded_job/test_loaded_operator_cache.py::test_loaded_replay_operator_reads_the_rows_its_run_stored
tests/test_pipeline/loaded_job/test_loaded_operator_cache.py::test_matching_counts_and_zero_expected_zero_found_do_not_warn
tests/test_pipeline/loaded_job/test_loaded_operator_cache.py::test_more_rows_than_expected_warn_and_read_them_all
tests/test_pipeline/loaded_job/test_loaded_operator_cache.py::test_without_saved_nullability_the_operator_still_reads_its_runs_table
tests/test_pipeline/loaded_job/test_loaded_operator_cache.py::test_without_the_run_content_hash_an_absent_table_raises_too
tests/test_pipeline/loaded_job/test_loaded_operator_cache.py::test_without_the_run_content_hash_head_says_why_it_shows_no_rows
tests/test_pipeline/loaded_job/test_loaded_operator_cache.py::test_without_the_run_content_hash_the_loaded_operator_raises
tests/test_pipeline/test_job_node_rows_after_run.py::TestRaisedRun::test_raised_run_reads_as_no_run
tests/test_pipeline/test_node_head_sorting.py::test_a_function_nodes_head_is_the_prefix_of_as_table_in_any_completion_order
tests/test_pipeline/test_node_head_sorting.py::test_an_operator_nodes_head_is_the_prefix_of_as_table
tests/test_pipeline/test_node_head.py::test_a_loaded_job_shows_stored_rows_and_never_raises
tests/test_pipeline/test_node_head.py::test_a_new_job_over_a_filled_store_reads_stored_rows_without_computing
tests/test_pipeline/test_node_head.py::test_head_keeps_the_tag_arrow_type_of_the_input
tests/test_pipeline/test_node_head.py::test_nodes_before_a_run_show_no_rows_and_compute_nothing
tests/test_pipeline/test_node_head.py::test_operator_node_cache_modes
tests/test_pipeline/test_stored_data_context.py::TestRowsOfAnotherDataContext::test_function_node_reads_current_rows_only
tests/test_pipeline/test_stored_data_context.py::TestSideEffects::test_function_node_upstream_of_a_refused_sink_is_not_refused
tests/test_torch/test_tensor_job_load_back.py::test_a_loaded_job_reads_its_stored_tensors
```
