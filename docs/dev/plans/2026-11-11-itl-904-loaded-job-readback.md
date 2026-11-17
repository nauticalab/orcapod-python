# ITL-904 Loaded PipelineJob read-back Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** On a job loaded with `PipelineJob.load`, reading a node with `as_table()` or
`iter_data()` returns its stored rows, or raises an error that says what is wrong and what to
do. Loading never crashes on a valid saved job, and loading logs a clear message for each
source it cannot rebuild.

**Architecture:** A loaded function node is a stub with no function pod. It reads its
records through `_ResultDatabaseReader` once the store is attached. The work:
- `attach_databases` marks the stub readable (`LoadStatus.READ_ONLY`).
- The stub's `as_table()` builds an `ArrowTableStream` over the stored Arrow records.
- A stub's `output_schema()` and `data_context` come from the descriptor.
- A loaded operator node that has nothing stored raises.
- `PipelineJob.load` logs each source it cannot rebuild, with the consequence and the remedy.

**Tech Stack:** Python 3.11+, PyArrow, Polars, Delta Lake (`DeltaTableDatabase`), pytest.

**Linear:** ITL-904. **Branch:** `eywalker/itl-904-a-loaded-pipelinejob-cannot-read-its-nodes`.

---

## Coordinator decisions (November 11, 2026)

Approved contract (coordinator, answering the step-1 ask):

- **A.** A loaded function node with a store attached serves the stored rows. A row is
  stored for the node when its pipeline record carries `_node_content_hash` equal to the
  live content hash saved in `run.node_job_content_hashes`. This is the same set
  `get_all_records()` returns.
  - `iter_data()` yields the stored `(tag, data)` pairs.
  - `as_table()` is built from the stored Arrow records, honours `ColumnConfig`/`all_info`,
    and re-infers no types.
  - `load_status` is `READ_ONLY` while a store is attached and `UNAVAILABLE` without one.
  - Nothing stored: zero rows with the node's schema.
  - No store: `RuntimeError("FunctionJobNode 'dbl' was loaded without a store, so its stored
    results cannot be read. Pass the store to PipelineJob.load(path, store=...) or call
    job.bind(store=...).")`
  - `PipelineJob.run()` still rejects loaded stubs.
- **B.** A stub's `output_schema()` returns deserialized Python types, not serialized
  strings. This fixes `PipelineJob.load` crashing on pod-then-join. The `data_context`
  AttributeError on stubs is fixed as well.
- **C.** A loaded operator node with no stored output raises: "OperatorJobNode 'joined' has
  no stored results: a PipelineJob does not store operator outputs, so a loaded job cannot
  show them. Read the stored results of its input nodes ('dbl', 'asrc') instead." It never
  returns an empty table. Live, un-run operators keep returning empty.
- **D.** Source messages on load:
  - INFO for in-memory kinds (`NotImplementedError`).
  - WARNING with the exception for other failures.
  - WARNING for a store that cannot be rebuilt.
  - The exact wording is in Task 5.
- Mismatched-store detection, option (1): `run.node_has_stored_records`. This is an
  additive, optional saved-file field and needs the **developer's** approval. Prepare it as
  a separate, self-contained commit on top and **do not push it** until the coordinator
  confirms. The base behaviour is (3): zero rows.
- Docs: add a short executed section "Saving and loading a job" in
  `docs/concepts/function-pods.md` (in the PipelineJob part, after the store section).
- Example: edit main's `examples/save_and_load_pipelines.py` now; reconcile with PR #296
  when it lands.
- No saved-format, stored-schema or hash change in the base PR. The golden suite must pass
  unchanged.
- CLAUDE.md's stale line "Phase 1 returns ALL records": propose a correction in the PR and
  ask the coordinator before editing CLAUDE.md or `.zed/rules`.

## Root causes (step 1)

1. `FunctionJobNode.from_descriptor` (`src/orcapod/core/nodes/function_node.py`) sets
   `LoadStatus.UNAVAILABLE`. `attach_databases` wires a `_ResultDatabaseReader` but never
   updates the status, and `iter_data`, `run` and `async_execute` raise a hard-coded
   "no function pod and no database attached".
2. `FunctionNodeBase.output_schema()` and `OperatorNodeBase.output_schema()` return
   `Schema(self._stored_schema[...])`, whose values are serialized strings (`'int64'`).
   `Pipeline.load` builds a full-mode operator over a function stub, and `Join` validation
   then fails with "Types <class 'int'> and int64 are not compatible".
3. The `data_context` of a stub dereferences `_function_pod` / `_operator`, which is `None`.
4. `PipelineJob` runs operators with `cache_mode=OFF`, so nothing is stored for them, and the
   loaded node returns an empty table.
5. `PipelineJob.load` swallows every source reconstruction error into one WARNING.
   In-memory sources are not serializable by design.

## File map

- Modify `src/orcapod/core/nodes/function_node.py`:
  - `FunctionNodeBase.output_schema` and `data_context` for stubs.
  - `FunctionJobNode.attach_databases` sets READ_ONLY.
  - `iter_data` / `run` / `async_execute` use the new UNAVAILABLE message.
  - New `as_table` override for stubs, plus a shared `_stored_records_stream` helper
    used by `_load_cached_entries`.
- Modify `src/orcapod/core/nodes/operator_node.py`:
  - Stub `output_schema` and `data_context`.
  - A loaded marker; `iter_data` / `as_table` raise for a loaded operator with nothing to
    serve.
  - Keep `_input_streams` on read-only stubs.
- Modify `src/orcapod/pipeline/job.py`:
  - Source and store log messages in `load`.
  - The `run()` guard also rejects READ_ONLY function stubs.
  - The `load` docstring.
- Create `tests/test_pipeline/test_loaded_job_readback.py`. It saves in a subprocess (fresh
  process) and loads in the test process; it covers every source kind and reads each node.
- Modify `docs/concepts/function-pods.md`, `examples/save_and_load_pipelines.py`,
  `CHANGELOG.md` and `DESIGN_ISSUES.md`.

## Tasks

### Task 1: Failing tests (public interface)

`tests/test_pipeline/test_loaded_job_readback.py`. A module-scoped fixture writes a save
script into `tmp_path` and runs it with `subprocess.run([sys.executable, script, dir])`.
The script builds a job with:
- `DictSource` -> pod `dbl` -> `Join` with an `ArrowTableSource` (`joined`): the
  pod-then-join shape;
- `CSVSource` -> pod `inc`;
- `DeltaTableSource` -> pod `half`;
- `ListSource` -> pod `sq`;
- a `Delta` store.

It runs the job, writes each node's `as_table()` (sorted by tag) to parquet as the expected
values, then saves the job. Tests:

- `test_load_does_not_crash_on_pod_then_join`
- `test_function_node_as_table_returns_stored_rows[dbl|inc|half|sq]`: equal to the live
  table, sorted by tag; same schema.
- `test_function_node_as_table_all_info_matches_live`: same column set as live, all_info.
- `test_function_node_iter_data_yields_stored_rows`
- `test_function_node_load_status_read_only_with_store`
- `test_function_node_without_store_raises_clear_error`: load with a store that cannot be
  rebuilt (edit the JSON store block to an unknown type) -> the message names
  `PipelineJob.load(path, store=...)` and never says "no database attached".
- `test_function_node_nothing_stored_returns_zero_rows`: load with an empty
  `InMemoryArrowDatabase` as `store=` -> zero rows, columns = tag + data.
- `test_loaded_operator_node_raises_no_stored_results` (`as_table` and `iter_data`).
- `test_loaded_job_run_still_rejected`
- `test_in_memory_sources_logged_at_info_with_remedy` (caplog): the message contains
  `job.bind(sources={'dsrc': <source>})`; no WARNING for in-memory kinds.
- `test_unreconstructable_file_source_logged_at_warning_with_reason`: delete the CSV
  before loading.
- `test_csv_and_delta_source_nodes_readable_after_load`
- `test_in_memory_source_node_readable_after_bind`
- `test_stub_output_schema_is_python_types`

Run `uv run pytest tests/test_pipeline/test_loaded_job_readback.py -v` -> expect failures.
Commit as a WIP test commit.

### Task 2: Stub `output_schema` and `data_context` (B)

In `FunctionNodeBase.output_schema` (stub branch), use
`deserialize_schema(self._stored_schema.get("tag", {}), type_converter=self.data_context.type_converter)`,
and the same for data. `data_context` returns `self._data_context` when
`_function_pod is None`; `from_descriptor` already sets `_data_context`. Make the same
change in `OperatorNodeBase` (`_operator is None`). Run the pod-then-join and schema tests.

### Task 3: Function stub read-back (A)

- `attach_databases`, stub path: after creating the `_ResultDatabaseReader`, set
  `self._load_status = LoadStatus.READ_ONLY`.
- `_stored_records_stream(fetched) -> ArrowTableStream | None`: drop the `META_PREFIX`
  columns and `NODE_CONTENT_HASH_COL`, then build
  `ArrowTableStream(table, tag_columns=tag_keys)`. `_load_cached_entries` uses it (no
  duplication).
- `FunctionJobNode.as_table` override: if `self._function_pod is None`, require
  readability (the UNAVAILABLE error). If stored rows exist, return
  `stream.as_table(columns=columns, all_info=all_info)` (cached). Otherwise fall back to
  `super().as_table(...)`, which gives a zero-row table from `output_schema`.
- UNAVAILABLE message in `iter_data`, `run` and `async_execute`: the approved wording
  (one private helper that builds the error).
- `PipelineJob.run()` guard: also list `FunctionJobNode`s whose `_function_pod is None`.

### Task 4: Loaded operator raises (C)

- `OperatorJobNode.from_descriptor`: set `node._loaded = True` on both paths (`False` in
  `__init__`); keep `node._input_streams = tuple(input_streams)` on the read-only path.
- `iter_data` / `as_table`: when there is no cached stream and no DB stream, and
  `self._loaded` is true, raise `RuntimeError` with the approved message. Input labels come
  from `self._input_streams` (`label` of each).

### Task 5: Load messages (D)

In `PipelineJob.load`:

```python
except NotImplementedError:
    logger.info(
        "Source %r (%s) keeps its data in memory and is not saved with the job; the "
        "loaded job has no data for it. Stored results of all nodes are still readable. "
        "To read source node %r itself, bind a source: job.bind(sources={%r: <source>}).",
        spec_name, source_type, spec_name, spec_name,
    )
except Exception as exc:
    logger.warning(
        "Could not reconstruct source %r (%s) from its saved config: %s. Stored results "
        "of all nodes are still readable. To read source node %r itself, bind a source: "
        "job.bind(sources={%r: <source>}).",
        spec_name, source_type, exc, spec_name, spec_name,
    )
```

The store message: "Could not reconstruct the store from its saved config: %s. Node results
cannot be read until a store is given: PipelineJob.load(path, store=...) or
job.bind(store=...)." The docstring says a loaded job is for reading stored results and
cannot be run again.

### Task 6: Docs, example, CHANGELOG, DESIGN_ISSUES

- `docs/concepts/function-pods.md`: add "### Saving and loading a job" with an executed
  sample (`DeltaTableDatabase` in a temporary directory, save, `PipelineJob.load`,
  `as_table()` printed sorted by tag).
- `examples/save_and_load_pipelines.py`: a `PipelineJob` with a Delta store; run, save,
  load, print `as_table()`.
- CHANGELOG `[0.3.0] — unreleased` -> `### Fixed` entry.
- DESIGN_ISSUES.md: one appended entry, status resolved, with a **Fix:** note.

### Task 7: Verification

`uv run --with ruff ruff check .`, `ruff format --check .`,
`uv run --with basedpyright basedpyright` (exit code) and `uv run pytest tests/` (including
the golden and doc tests). Merge origin/main, push, open the PR (Fixes ITL-904), run
`orca linear attach` and `orca worktree set`, then do the CodeRabbit rounds.

### Task 8 (separate, unpushed until confirmed): option (1)

- `save()`: `run.node_has_stored_records[label] = pipeline_db.table_exists(node._versioned_pipeline_path)`.
- `load()`: pass the flag to the stub. If the flag is true and the attached store has no
  table at the node's path, reading raises an error naming the store path.
- Test: an older file without the field still loads and gives zero rows.

## Progress notes

- [x] Step 1 investigation and ask; approved November 11, 2026.
- [x] Tasks 1–6 and 8 done and pushed. The developer approved option (1) (additive
  `run.node_has_stored_records`), so it is in the PR.
- [x] Coordinator decision: the loaded `as_table(all_info=True)` follows the stream semantics
  (`_context_key`, `_source_*` included). The live `FunctionJobNode.as_table()` drops those
  columns; that is a separate bug, for the coordinator to file.
- [x] Merged origin/main (PR #296 landed). Conflicts in the example and in DESIGN_ISSUES.md
  are resolved: both entries are kept.
- [ ] Task 7: full suite, PR, CodeRabbit.
