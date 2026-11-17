# ITL-1094: a loaded job restores its side-effect nodes, read-only

Branch `eywalker/itl-1094-a-loaded-job-has-no-side-effect-nodes`, from `origin/main` `a9b50350`.
Status: option A APPROVED by the coordinator (2026-11-14). Implemented on main; draft PR.
Coordinator notes: B (record the run content hash) is a follow-up note in the PR body; #380
is now c1a97dea and puts SideEffectJobNode under the state rule (execute/async_execute take
load_only, rows pass on and are HELD as load-only; run(*, load_only=False) is the cascade;
the orchestrator special case is gone). PR stays draft until #380 merges and main is merged.

Implementation note: the loaded classes live in `orcapod/pipeline/loaded_side_effects.py`
(`LoadedSideEffectNode`, `LoadedSideEffectJobNode`), not as `from_descriptor` classmethods
in `side_effects.py`: importing `serialization` from `side_effects.py` is an import cycle for
basedpyright, and a separate module keeps `side_effects.py` (edited by #380 and #375) almost
untouched: only `_rows_data_context()` and the `producer` annotation.

## Merge of #380 (done: #380 head f02d72ae merged locally, at the coordinator's request)

- #380 f02d72ae: async load-only branch moved above the pod-config read, side-effect node
  shows `loaded_only` after a load-only run. The stub delegates execute/async_execute with
  load_only to the live path; `run(load_only=True)` calls `run_job_node` and wraps the view
  with `_with_run_status`, as the live node.
- #380's lost-input guard in `PipelineJob._prepare_run` stays (Pipeline.load still skips a
  node of an unknown `node_type`), reworded without ITL-1094; its two tests: the function
  case now drives the guard with an unknown node type, the Join case now checks that a
  load-only run reaches the operator and delivers nothing.

- #380 round 3 (8314c934) and its CI fix (7de3512b) merged locally. The guard now sits after
  the exclusion walk and skips excluded nodes; kept, wording without ITL-1094. Round 3's
  test of a broken unbound branch now drops the node by an unknown `node_type`.
- `tests/test_core/streams/test_run.py` (#380's): a job node's `run(*, load_only=False)`
  is decided by subclass, not class name, so `LoadedSideEffectJobNode` is covered
  (coordinator-approved; the red CI on dbf9c310).
- Stacked PR (coordinator, developer away): ready once CI is green, before #380 merges;
  act only on review findings in this PR's own files; forward findings in #380's code.

### Planned before the merge

- `LoadedSideEffectJobNode.execute` / `async_execute`: take `load_only`; with it, pass rows
  on and hold them as load-only exactly as the live node (call super's load-only path, which
  touches no pod); without it raise. `run(*, load_only=False)`: with load_only go through the
  shared cascade (super().run(load_only=True)); without it raise before anything runs.
- Test: bind the source, `loaded.run(load_only=True)` (sync + async): `down` loads 3 rows,
  zero deliveries, side-effect nodes hold the rows as loaded-only; `loaded.run()` raises.
- Docs: rewrite the side-effect bullet into #380's version of "Saving and loading a job".

## Reproduction (main a9b50350, public interface)

Job `s -> delivered (SideEffectPod) -> down (function pod) -> tail (SideEffectPod)`, run on a
Delta store, saved, loaded.

- The saved file records each side-effect node like every other node, under the key of its
  blueprint content hash, with exactly these fields: `node_type: "side_effect"`, `label`,
  `content_hash` (blueprint hash), `pipeline_hash` (equal to the job node's), `output_schema`
  (`tag`, `data`, `nullable`), `node_uri` (`["side_effects", <pod name>]`), `data_context_key`,
  `config` (the Orcapod config, `null` when default). The edges `s -> delivered` and
  `delivered -> down` are saved. `run.node_job_content_hashes` and `run.expected_rows` hold
  no entry for a side-effect node. The pod config (`on_error`, `track_completion`,
  `drop_on_failure`, `max_concurrency`) is not saved anywhere.
- `Pipeline.load` has no `side_effect` branch: the node is skipped. `loaded.nodes` is
  `['s', 'down']`; `loaded.nodes['delivered']` and `['tail']` are `KeyError`.
- `down` after the load: its `upstreams` is `(None,)`, and the job DAG has neither edge
  `s -> delivered` nor `delivered -> down`. Its content hash and pipeline hash EQUAL the saved
  ones (both come from the file: the run block's live hash and the descriptor), so it reads
  the same table: 3 rows. No silent identity change.
- Re-saving the loaded job writes a file WITHOUT the side-effect nodes but WITH their edges
  (3 dangling edges); loading that file gives `['s', 'down']` again.
- On the #380 branch (head 0b4c2316), `loaded.bind(sources=...)` then
  `loaded.run(load_only=True)` raises `ValueError: FunctionNode expects exactly 1 upstream,
  got 0` at `down`: the dropped node breaks a load-only run of anything below it.
- v0.2 (format 0.1.0) saved side-effect nodes with the same fields minus
  `output_schema.nullable`, with `semantic_v0.1` hashes; v0.2's load dropped them too. No test
  fixture holds a v0.2 job file with a side-effect node.

## Proposal

A read-only side-effect node, built from its descriptor, as the function and operator stubs are.

1. `SideEffectNode.from_descriptor(descriptor, input_stream)` (blueprint, used by
   `Pipeline.load`) and `SideEffectJobNode.from_descriptor(descriptor, input_stream)` (used by
   `PipelineJob.load`): no pod (`_pod = None`); label, content hash, pipeline hash, output
   schema (with the saved nullability, or unrecorded), node uri, data context and Orcapod
   config from the descriptor; upstream = the restored upstream node, so the DAG edges
   `s -> delivered -> down` are restored and `down.upstreams == (delivered,)`.
2. Identity: `pipeline_hash()` = the saved one (equal to the job node's; it names the
   invocation log table `side_effects/<pod>/schema:<hex>/tdb_v2`). `content_hash()` = the
   descriptor's `content_hash` (the blueprint hash the file records). The live job node's
   content hash (data-inclusive) is not in the file. Nothing stored is keyed by a side-effect
   node's content hash (log path and record ids use the pipeline hash and the row), so no
   table depends on it. Option B (not recommended for the tag): record it in
   `run.node_job_content_hashes` at save (an existing label -> hash map; no new key; older
   loaders ignore extra entries; older files fall back to the descriptor hash). It changes
   what the file records, so it is the coordinator's call.
3. Never delivers: no pod function. `execute`, `async_execute` and `node.run()` raise a
   `RuntimeError` saying the node was loaded and has no side-effect function. Reads follow
   `SideEffectJobNode` (#377): no held rows, so zero rows; `head()` not computed.
   `loaded.run()` raises as today (before any node runs).
4. Pod config and completion log: not restored; the file does not hold the pod config, and
   restoring it would be a saved-format change (not proposed). The database is attached as for
   any `SideEffectJobNode` (path known from node uri + pipeline hash), but the node never
   reads or writes the log. No public reader is added.
5. Re-save of a loaded job writes the node's descriptor back unchanged (and the edges become
   consistent again).
6. No new field in the saved file; no hash, golden value or stored table changes.

## After #380 (expected to merge first; then merge main here)

- #380's orchestrators pass a side-effect node's rows on in a load-only run without calling it
  (sync: `buffers[node] = upstream_buf`; async: `_pass_through`), so the restored node needs
  no code change: `loaded.run(load_only=True)` passes the rows through `delivered` to `down`,
  delivers nothing, writes nothing. On #380 head 0b4c2316 the side-effect node itself is NOT
  filled ("node reads empty", its D4); the brief's "holds them as loaded only" is not what
  #380 does yet.
- At the merge: docs section "Saving and loading a job" conflicts (both rewrite it); tests
  gain a load-only case (bind source, `loaded.run(load_only=True)`, `down` loads 3 rows,
  zero deliveries, side-effect node empty).

## Tasks (after approval)

1. Failing tests, public interface: `tests/test_pipeline/loaded_job/test_loaded_side_effect_nodes.py`
   - side-effect node in the middle and at the end: `loaded.nodes` lists them with label,
     content hash (= saved descriptor), pipeline hash (= saved job node), upstream.
   - `down`'s content/pipeline hash equal the saved job node's, `down.upstreams` is the
     restored node.
   - no read (`iter_data`, `as_table`, `head`, dataframes, `flow`) or run (`loaded.run()`,
     `node.run()`) delivers: count deliveries; reads give zero rows; runs raise.
   - re-save and reload keeps them; `Pipeline.load` lists them.
   - a 0.1.0-shaped file (no nullable) restores them.
2. Fix: `from_descriptor` on both classes; `Pipeline.load` and `PipelineJob.load` branches.
3. Docs: `docs/concepts/function-pods.md` "Saving and loading a job"; changelog fragment
   `changelog.d/ITL-1094.md` (Fixed); `design_issues/ITL-1094-<id>.md`.
4. Targeted tests (loaded_job, side_effect_pod, serialization, golden), ruff, basedpyright.

## Content hash of a loaded side-effect job node (coordinator probe, November 14, 2026)

A loaded `LoadedSideEffectJobNode` returned the descriptor's (blueprint, unbound) content hash,
while a loaded function node has the saved job's (bound) hash from `run.node_job_content_hashes`.
Approved fix: `PipelineJob.save` records each `SideEffectJobNode`'s `content_hash()` in that map;
`PipelineJob.load` passes it as `job_content_hash`, with the descriptor hash as the fallback for
files saved before. `Pipeline.load`'s blueprint node keeps the descriptor hash. The only consumer
is the run's `pipeline_uri` (hash of the end nodes), now equal to the live job's. Tests: identity
before bind, after bind and a load-only run, after save and load; the fallback; `pipeline_uri`.

## Follow-up: CodeRabbit round on 51853173 (schema under ColumnConfig)

Finding (thread 4575868248, `loaded_side_effects.py:179`): the loaded side-effect node classes
return the SAVED DEFAULT schema and keys whatever `columns` / `all_info` ask for. Outside-diff
finding in the review body: `PipelineJob.load(path).as_pipeline()` builds a plain
`SideEffectNode` with `_pod=None` and raises `AttributeError` (`None.argument_symmetry`).

Reproduced (public interface; job `s1 -> delivered (SE) -> {fa (fn), joined (Join with s2),
grouped (GroupBy g), batched (Batch 2)}`, live job from code vs loaded+bound, both load-only):

- `delivered`, `fa`, `joined`, `grouped`, `batched`: schema/keys equal under default, source,
  meta; differ under system_tags and all_info (loaded lacks the system tags upstream of SE).
- Sync load-only: the operators below the loaded SE node name system tags with the schema-only
  hash of s1 (`::07d1…`) instead of the SE node's pipeline hash (`::4888…`); a function node
  below such a Join loads 0 of 4 stored entries (async: 4 of 4). Cause:
  `StaticOutputOperatorPod._as_upstream_stream` rebuilds rows as
  `ArrowTableStream(producer=upstream.producer, upstreams=...)`; producer None -> schema-only
  pipeline hash. Same on main for any pod-less loaded function node (pre-existing, #380).
- Loaded pod-less `FunctionJobNode.output_schema(system_tags)` also returns the saved default
  (pre-existing on main).

Plan (pending coordinator reply):
1. Failing tests first in `tests/test_pipeline/loaded_job/test_loaded_side_effect_nodes.py`.
2. Delete the `output_schema` / `keys` overrides in both loaded classes (inherit the live
   pass-through to the input stream).
3. (if approved) pin the upstream's pipeline hash in `_as_upstream_stream`.
4. (if approved) `as_pipeline()` builds a `LoadedSideEffectNode` from the saved descriptor.

Coordinator approved 1, 2 and 4 (C out of scope, reported in worker_done). Done:
- Tests: `tests/test_pipeline/loaded_job/test_loaded_side_effect_column_configs.py` (7 of 12
  failed before the fix: system_tags/all_info schemas, sync load-only below the SE node and
  below a loaded function node, `_materialize_upstream` pipeline hash, `as_pipeline()`).
- Fix: overrides removed; `_as_upstream_stream` returns `_RowsOfStream` pinned to
  `upstream.pipeline_hash()` when `upstream.producer is None`; `as_pipeline()` builds
  `LoadedSideEffectNode` from the saved descriptor.
- After the fix only C remains in the table (`fa`, a loaded FunctionJobNode, under
  system_tags/all_info).
