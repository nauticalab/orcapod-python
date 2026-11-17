# Test inventory: skipped, expected-to-fail, deselected and failing tests (ITL-1074)

Date: 2026-11-13. Base: `main` at `41798e87`. Facts come from the full CI job logs of the
`Run Tests` workflow on `main` (run 43483054834, commit `3bc5e2af`; 12461 passed there, 12519 on this PR's head, the last run with complete
logs at the time of writing), the `Run PostgreSQL Tests` and `Run Objective Tests` workflows of
the same commit, the static marks in the tree, and local runs of the files that hold xfails
(`--runxfail`, Python 3.12). `gh run view --log` truncates these logs at 78%; the full log is
`gh api --allow-escape-sequences repos/<repo>/actions/jobs/<job id>/logs`.

## 1. Totals

| Workflow (CI, per Python version) | passed | skipped | xfailed | xpassed | failed | deselected |
|---|---|---|---|---|---|---|
| `Run Tests` 3.11 | 12461 | 32 | 21 | 0 | 0 | 77 |
| `Run Tests` 3.12 | 12461 | 32 | 21 | 0 | 0 | 77 |
| `Run Tests` 3.13 | 12461 | 32 | 21 | 0 | 0 | 77 |
| `Run PostgreSQL Tests` (3.11, 3.12, 3.13) | 77 | 0 | 0 | 0 | 0 | 12514 |
| `Run Objective Tests` (`test-objective/`, 3.11, 3.12, 3.13) | 568 | 0 | 7 | 0 | 0 | 0 |

- **The counts are identical on all three Python versions.** No test runs on only one version
  (warning counts differ: 222 on 3.11, 259 on 3.12 and 3.13).
- The 77 deselected tests are exactly the `postgres`-marked tests (`-m "not postgres"`); the
  Postgres workflow runs them and all 77 pass on every version. They are not lost coverage.
- `Run Objective Tests` is a separate directory, `test-objective/`, that `pytest.ini`'s
  `testpaths` does not cover. Its 7 xfails are in section 4.
- **Failures:** none in the three pytest jobs. The `Run Tests` workflow itself is red on every
  push to `main` (40 of 40 recent runs) because of its `spiral-integration` job (section 3.2).
- Local runs of the whole suite quote "11,625 passed, 119 skipped, 19 expected failures". A
  local run has no `-m "not postgres"`: the root `conftest.py` turns the 77 postgres tests
  into *skips* unless `--postgres` is given, so they count as skipped (77 + 32 CI skips = 109,
  and a machine without `dot`, torch or a GPU adds the rest); 19 versus 21 xfailed is the two
  `PLT-1732` xfails added later; 11,625 versus 12,461 passed is the tests added between those
  runs and `3bc5e2af` plus the tests that run in CI but not on a laptop. This reconciliation is
  an inference from the counts, not a re-run of the old commits. `pytest --collect-only` on
  this commit reports 12,652 tests.

## 2. Skipped in CI (32, identical on every version)

| Group | Count | Stated reason | Verified cause | Verdict |
|---|---|---|---|---|
| Live Spiral source: `tests/test_core/sources/test_spiraldb_table_source_live.py` | 8 | "Set ORCAPOD_SPIRAL_LIVE_PROJECT to run" | The variable is set nowhere in `.github/`. The tests need a real Spiral project holding a `spike_event` table of more than 1,000 rows, credentials and network. | Correct to skip in `Run Tests`. Could run in the `spiral-integration` job once that job works and a project with the table exists (needs a developer decision and Spiral data). |
| Spiral connector integration: `test_spiraldb_connector_integration.py` | 9 | "Set SPIRAL_INTEGRATION_TESTS=1" | Run by the `spiral-integration` job (push to `main` only), which sets the variable. In that job all 9 **fail** (section 3.2). | Correct to skip in `Run Tests`; the job that runs them is broken. |
| S3 / MinIO: `test_delta_table_database_s3.py` (7), `test_list_table_paths.py::test_s3_lists_tables_and_nested_tables` (1) | 8 | "MinIO container could not be started: 404 ... pull access denied for minio/minio" | See 3.3. The fixture starts MinIO with testcontainers and skips on any exception; in CI the image cannot be pulled, so the S3 path is never tested. | Should run in CI but does not; needs a SpiralDB workload credential (3.3, ITL-1079, ITL-1008). |
| Delta tables under special-character roots: `test_delta_native_fs_read.py` (`x#y`, `q?r`, two tests each) | 4 | "deltalake cannot write a table at ..." | `_write_or_skip` skips when `deltalake` itself cannot write a table under a root named `x#y` or `q?r`. The other six special names run and pass. | Correct to skip: a limit of `deltalake`, not of Orcapod. Re-check on each `deltalake` upgrade. |
| GPU tensors: `test_tensor_round_trip.py` (MPS, CUDA), `test_tensor_conversion_warning.py` (CUDA or MPS) | 3 | "needs MPS / CUDA / a CUDA or MPS device" | GitHub's Ubuntu runners have no GPU and no MPS. | Correct to skip. Running them needs a GPU runner (a paid CI service): not recommended before v0.3.0. |

Conditional skips that do **not** fire in CI, so they are not in the 32 (checked against the CI
log, where each test passed): Graphviz `dot` (installed by the workflow with `apt-get`), torch
(a dev dependency; `test_ci_has_torch.py` fails the job if it is missing), `uv` for the packaging
test, mkdocs for the docs build test, `psycopg` (Postgres), `mkfifo`, the non-root permission
test, and the optional-dependency golden groups. On a machine without those, the matching
tests skip locally.

## 3. Findings

### 3.1 `Run Tests` is red on `main`
Every recent push to `main` fails the `Run Tests` workflow. The three pytest jobs pass; only
`spiral-integration` fails.

### 3.2 `spiral-integration` fails: all 9 tests, `HTTP request failed: error sending request`
The job points the Spiral client at `SPIRAL_SERVER_URL`, a repository variable set to
`http://api.spiraldb.dev` (March 2026); the workflow's own default is the same. From this
machine `http://api.spiraldb.dev` answers "Empty reply from server" while
`https://api.spiraldb.dev` answers (HTTP 404 on `/`, so the host is up). Evidence from CI: a
`workflow_dispatch` run of this branch with the URL set to `https` (run 43492939109, job
132129691920) reaches the server and fails differently, all 9 tests with
`401 Unauthorized`. So `http` was the connection failure; the next failure is
authentication (probably the OIDC workload trust being configured for `main` pushes, since this
was a dispatch on a branch; unverified). This PR changes the workflow to `https` (the
repository variable is no longer read, so it can be deleted) and the URL in two docstrings and
the integration test's default. Whether the 9 tests then pass on `main` is only known after
the merge. Tracked by ITL-1008.

### 3.3 S3 tests are skipped everywhere in CI
The full reason in this PR's CI log: `404 Client Error ... images/create?tag=RELEASE.2022-12-02T19-19-22Z&fromImage=minio/minio: pull access denied for minio/minio, repository does not exist`.
`testcontainers` 4.14.2 defaults to the Docker Hub image `minio/minio:RELEASE.2022-12-02T19-19-22Z`,
which Docker Hub no longer serves. The fixture turns every start failure into a skip, so no one noticed.
Tried in this PR: the same tag from `quay.io/minio/minio` (CI run of commit `90aa0fdf`): `unknown: failed to resolve reference`
(not served anonymously either); the change was reverted.
Developer direction (ITL-1079): CI does not need an S3 endpoint, a MinIO image or a MinIO binary. It needs a SpiralDB
workload credential that can access a project in a workspace, with an S3 volume associated with that project; the S3
credentials are held by the SpiralDB server with the volume. The same credential is what the `401 Unauthorized` of the
`spiral-integration` job needs (ITL-1008). No credential, project or volume is set up or named in this repository.
The fixture should also fail instead of skip when `CI` is set, as `test_ci_has_torch.py` does for torch.
A second, hidden defect: once a container starts, the fixture calls `minio.get_url()`, which `MinioContainer`
in `testcontainers` 4.14.2 does not have (`AttributeError`; the endpoint must be built from
`get_container_host_ip()` and `get_exposed_port(9000)`). The skip hid it. The image decision and this
fixture fix are ITL-1079.

**Resolved (ITL-1079, November 14, 2026):** the S3 tests now run against a moto server that the fixture starts as a subprocess (an in-process server deadlocks with deltalake); a fixture that cannot start errors instead of skipping. The text above is kept as history.

### 3.4 Wrong or closed issue named by an xfail
- `test_delta_append_types.py` (4 xfails) cited ITL-916 (now ITL-1076), which is "A Delta table directory whose
  `_delta_log` is missing is treated as absent and then overwritten". The real failure is
  `deltalake` refusing a `fixed_size_list<int64, 2>` append: "expected List(Field { name:
  "element" ... }) but found List(Field { name: "item" ... })". Different problem.
- `test_write_side_registration.py` (2 xfails) cited PLT-1732 (now ITL-1077), which Linear maps to ITL-173
  (Done). ITL-173 scoped nested containers out ("`list[list[UUID]]` in v0.2"), so the limit it
  fixed is not the one these tests hit (`dict[..., list[T]]` of a logical type), which remains.
- `test-objective/unit/test_sources.py` (7 xfails) named no issue (now ITL-1078).
The marks in this PR cite ITL-1076, ITL-1077 and ITL-1078.

## 4. Expected to fail (28 = 21 in `tests/` + 7 in `test-objective/`)

Every xfail was run with `--runxfail`: **all 28 still fail** (no xpass, nothing stale), and
each failure matches its stated reason. **All carry `strict=True`**, so a fix turns the test
red until the mark is removed; no non-strict xfail exists. `raises=` is set on the ones that
name an exception.

| Test(s) | Count | Issue (state) | Verified failure | Verdict |
|---|---|---|---|---|
| `test_user_types/test_read_path.py::test_ipc_read_of_non_large_nested_storage_is_valid` | 1 | ITL-894 (Backlog) | `pa.ArrowInvalid` on IPC read of non-large extension storage | Real defect, tracked |
| `test_channels/test_node_async_execute.py::TestOperatorNodeAsyncExecute::test_binary_op_delegation` | 1 | ITL-829 (In Progress) | `InputValidationError`: Type conflict for key 'id': `int` vs `int \| None` in SemiJoin | Real defect, tracked |
| `test_pipeline/test_operator_node_async_readback.py` (zero-row async run; `test_clear_cache_keeps_operator_node_arrow_types[sync]`, `[async]`) | 3 | ITL-858 (In Progress), ITL-912 (Backlog) | `int8` read back as `int64` and nullability changed | Real defect, tracked |
| `test_core/operators/test_async_sync_equivalence.py::test_async_meta_equals_sync[...]` (merge_join, batch_2, batch_all, group_by, polars_filter, select_data_columns, index, pick) | 8 | ITL-846 (Backlog, an audit issue) | Data content hash of async rows differs from sync rows; consistent with the stated cause (meta columns dropped on one path) | Real defect, tracked by an audit issue only |
| `test_core/operators/test_merge_join_arrow_types.py::TestMergedColumnType::test_struct_element_with_rows` | 1 | ITL-839 (Backlog) | `TypeError: '<' not supported between instances of 'dict' and 'dict'` in `merge_join.py` | Real defect, tracked |
| `test_databases/test_delta_append_types.py::test_identical_schema_appends[...fixed_size_list<int64, 2>...]` | 4 | ITL-916 cited, **wrong issue** | `RuntimeError: Failed to flush`; root cause `deltalake` list item field `element` vs `item` | Real defect, **needs its own issue** |
| `test_core/function_pod/test_polars_dataframe_values.py::TestPipelineStores::test_sqlite_store` | 1 | ITL-877 (Backlog) | `StoreWriteError` ... 'does not preserve Arrow metadata' | Real defect, tracked |
| `test_core/function_pod/test_write_side_registration.py::test_pod_declaration_with_doubly_nested_input / _output` | 2 | PLT-1732 = ITL-173 (**Done**) | `ValueError: 'dict[..., list[...]]' is not yet supported` | Real defect, **cited issue is closed: needs its own issue** |
| `test-objective/unit/test_sources.py::TestArrowTableSourceResolveField` (7 tests) | 7 | none | `NotImplementedError: ArrowTableSource does not implement resolve_field` (`core/sources/base.py:105`) | Real gap, **needs an issue**: either implement `resolve_field` on `ArrowTableSource` or remove the method from `SourceProtocol` and the 7 tests |

## 5. Flaky tests
`test_concurrent_with_io_bound_work` (ITL-1001) is the known timing test, listed only. No test
failed in the three pytest jobs of the run read here, and no test is marked flaky or retried
in the tree.

## 6. Slowest tests (CI, Python 3.12, run 43487290031; durations differ by runner)

| Seconds | Phase | Test |
|---|---|---|
| 29.27 | call | `test_torch/test_tensor_conversion_warning.py::test_ten_thousand_rows_give_one_warning` |
| 26.38 | call | `test_databases/test_delta_exit.py::test_process_exits_after_reading_a_local_delta_store[delta_table_source]` |
| 21.84 | call | `test_databases/test_delta_exit.py::test_process_exits_after_reading_a_local_delta_store[database_get_all_records]` |
| 13.99 | call | `test_torch/test_tensor_hash_stability.py::test_tensor_hashes_stable_across_processes_and_hash_seeds` |
| 13.73 | call | `test_docs/test_mkdocs_build.py::test_mkdocs_build_strict` |
| 12.53 | call | `test_hashing/test_function_default_hashing.py::TestAcrossProcessesAndHashSeeds::test_set_and_dict_defaults_hash_equally_under_every_seed` |
| 9.25 | setup | `test_core/operators/test_join_tag_order.py::test_operand_order_does_not_change_columns[merge_join]` |
| 9.24 | setup | `test_core/operators/test_join_tag_order.py::test_operand_order_does_not_change_columns[join]` |
| 9.02 | call | `test_hashing/test_wrong_hash_findings.py::TestStructureHasherProcessIndependence::test_set_with_str_equal_items` |
| 8.85 | call | `test_torch/test_tensor_cached_rerun.py::test_cached_rerun_in_fresh_process[async]` |
| 8.37 | call | `test_torch/test_tensor_cached_rerun.py::test_cached_rerun_in_fresh_process[sync]` |
| 7.71 | call | `test_pipeline/test_job_crash_recovery.py::test_crash_between_result_and_pipeline_commits` |
| 6.24 | call | `test_user_types/test_workshop_scenarios.py::test_w6_restart_with_changed_fields_recomputes` |
| 5.99 | call | `test_arrays/test_array_hash_stability.py::test_array_hashes_stable_across_processes_and_hash_seeds` |
| 5.92 | call | `test_pipeline/test_job_store_scaling.py::test_store_work_per_row_does_not_grow_with_rows[sync-in_memory]` |
| 5.21 | call | `test_pipeline/test_job_crash_recovery.py::test_crash_mid_batch_keeps_committed_batches[async]` |
| 5.18 | call | `test_examples/test_examples.py::test_example_runs[async_vs_sync_pipeline.py]` |
| 5.09 | call | `test_pipeline/test_job_crash_recovery.py::test_crash_mid_batch_keeps_committed_batches[sync]` |
| 4.98 | call | `test_pipeline/test_job_crash_recovery.py::test_crash_mid_batch_keeps_committed_batches[async-late-row]` |
| 4.97 | call | `test_hashing/test_cross_process_stability.py::TestSemanticHashCrossProcess::test_semantic_hash_independent_of_pythonhashseed` |

The whole job takes 11 to 15 minutes; these 20 are about 4 minutes of it.

## 7. Changes in this PR
- `.github/workflows/run-tests.yml`: `-rsxX --durations=20`, so every skip and xfail reason and the 20
  slowest tests are printed in the job log; the Spiral server URL is `https` (3.2).
- Xfail reasons cite ITL-1076, ITL-1077, ITL-1078 (3.4).
- CI counts of this PR's head: 12519 passed, 32 skipped, 21 xfailed, 77 deselected on 3.11, 3.12 and 3.13.
- This document.
