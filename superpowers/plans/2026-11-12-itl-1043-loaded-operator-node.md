# ITL-1043 Loaded Operator Node Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A loaded operator node looks for its stored rows under the path its run wrote, and
never presents stored rows it cannot identify as zero rows.

**Architecture:** Investigation in `superpowers/specs/itl-1043/FINDINGS.md`. Option 1 (approved
by the coordinator, November 12, 2026; no saved-format change, no hash, path or golden change):
an operator node rebuilt from a descriptor WITH its operator takes its pipeline hash from the
descriptor's saved `pipeline_hash` (the run's), as read-only stubs and function stubs already
do; a loaded node in REPLAY whose content-hash filter keeps 0 of N>0 stored rows raises.
Options 2 and 3 (saved operator run content hash; saved top-level nullability, riding the
`0.2.0` format of ITL-1026) are HELD until the coordinator confirms the developer's decision.

**Tech Stack:** Python, PyArrow, pytest, `uv run`.

## Status / notes for a successor

- 2026-11-12: investigation done and reported (commit 5b07349d); option 1 approved and
  done (c7e3e4d6). Developer then chose option 3 (all three). Tasks 1-5 implemented locally;
  presence of the `nullable` record / the operator's entry in `node_job_content_hashes`
  decides lossless vs lossy load (never the version number). Pipeline._unrecorded_*_nullability (per node)
  keeps a lossy file lossy on re-save; OperatorJobNode._run_content_hash() is None for a
  loaded node without a saved run hash, so re-save leaves it out.
- Final shape (developer, Nov 12): store is the source of truth. Known key (run content
  hash in file) -> loaded node reads like live REPLAY (empty, NOTHING_STORED). Unknown key
  -> raise / head UNAVAILABLE (ITL-1048). No row count as truth; optional advisory
  run.expected_rows -> StoreContentWarning once per node. node_has_stored_records dropped
  (function nodes warn instead of raise). Dangling result refs: unchanged (logged, matches live).
- PUSH RULE (coordinator): do not push the format fields until PR #345 (ITL-1026, format
  0.2.0) has merged; then merge origin/main and push once, open the PR (Fixes ITL-1043).
- Pre-existing contradiction, not changed here: `OperatorJobNode.head` says "a loaded node
  never raises", but `_head_from_store` raises when a loaded REPLAY node's table is absent. The
  new 0-of-N case behaves like the absent table (raises from `head` too). Report it.

## Files

- Modify: `src/orcapod/core/nodes/operator_node.py`
  - `OperatorNodeBase.pipeline_hash` (line ~192): use `_stored_pipeline_hash` whenever set.
  - `OperatorNode.from_descriptor` full mode (~475) and `OperatorJobNode.from_descriptor` full
    mode (~789): set `_stored_pipeline_hash` from the descriptor.
  - `OperatorJobNode._read_stored_records` (~1020): split into
    `_read_current_context_records` (no content-hash filter) + filter.
  - `OperatorJobNode._load_cached_stream_from_db` (~1066): raise for a loaded node when the
    filter keeps 0 of N>0 rows.
- Create: `tests/test_pipeline/loaded_job/test_loaded_operator_cache.py`
- Create: `changelog.d/ITL-1043.md`
- Create: `design_issues/ITL-1043-LJ1.md` (lost nullability in the saved schema; open until
  option 3 lands or is declined)

### Task 1: failing tests (public interface)

**Files:** Create `tests/test_pipeline/loaded_job/test_loaded_operator_cache.py`

- [ ] **Step 1: Write the tests**

Job: `Join` of two `ArrowTableSource`s (tag `id`, data `a` / `b`), declared fields with the
tag nullable or not and the data nullable or not; operator node attached in `CacheMode.LOG`
(`node.attach_databases`, the public way); `job.run()`; `job.save`; `PipelineJob.load(path,
store=store)`; `loaded.nodes["joined"].attach_databases(store.at(NAME), CacheMode.REPLAY)`.
Stores: `InMemoryArrowDatabase`, `DeltaTableDatabase(tmp_path / "store")`.

```python
def test_loaded_operator_node_keeps_the_pipeline_hash_and_path_of_its_run(...):
    assert loaded_node.pipeline_hash() == run_node.pipeline_hash()
    assert loaded_node.node_identity_path == run_node.node_identity_path

@pytest.mark.parametrize("read", ["as_table", "iter_data", "head"])
def test_loaded_replay_node_raises_rather_than_show_no_rows(...):
    # In-memory sources are not saved, so the loaded node cannot reproduce the content
    # hash its run stored the rows under: the 2 stored rows must not read as 0 rows.
    with pytest.raises(RuntimeError, match=r"'joined'.*2 stored rows.*none"):
        ...
```

- [ ] **Step 2: Run, verify they fail**

`uv run pytest tests/test_pipeline/loaded_job/test_loaded_operator_cache.py -q`
Expected: nullable cases fail the hash test; every raise test fails (0 rows returned, or
"cache table is absent" for nullable cases).

- [ ] **Step 3: Commit (WIP, failing tests)**

### Task 2: loaded operator node takes the saved pipeline hash

- [ ] **Step 1: Implement**

```python
# OperatorNodeBase.pipeline_hash
if self._stored_pipeline_hash is not None:
    return ContentHash.from_string(self._stored_pipeline_hash)
return super().pipeline_hash(hasher)
```

and in both full-mode branches of `from_descriptor`:

```python
node._stored_pipeline_hash = descriptor.get("pipeline_hash")
```

(`_stored_pipeline_hash` is `None` for every node built by `__init__`, so a live node is
unchanged.)

- [ ] **Step 2: Run the hash test; expect PASS.**

### Task 3: 0 of N stored rows raises for a loaded node

- [ ] **Step 1: Implement**

```python
def _read_current_context_records(self) -> "pa.Table | None":
    # body of today's _read_stored_records, returning before the content-hash filter

def _read_stored_records(self) -> "pa.Table | None":
    records = self._read_current_context_records()
    return None if records is None else self._filter_by_content_hash(records)
```

In `_load_cached_stream_from_db`, read `_read_current_context_records()`, keep the absent-table
raise, filter, and:

```python
if self._loaded and records.num_rows == 0 and stored.num_rows > 0:
    raise RuntimeError(
        f"OperatorJobNode {self.label!r}: its cache table ({path}) holds "
        f"{stored.num_rows} stored rows, none of them under this node's content hash "
        f"({self.content_hash().to_string()}). They were stored by other runs, or by "
        "this job's run under a content hash the loaded job cannot reproduce (an input "
        "source whose data was not saved with the job). Build the job from code and "
        "attach the node with CacheMode.REPLAY to read them."
    )
```

- [ ] **Step 2: Run the new tests; expect PASS.**
- [ ] **Step 3: Run `tests/test_pipeline` and `tests/test_hashing/golden`; expect no change.**
- [ ] **Step 4: ruff format, ruff check, basedpyright (exit code 0). Commit, push.**

### Task 4: changelog fragment, design issue entry, docs

- [ ] `changelog.d/ITL-1043.md` under `### Fixed`.
- [ ] `design_issues/ITL-1043-LJ1.md`: the saved schema loses top-level nullability (status
  `open`; option 3 pending the developer).
- [ ] `docs/concepts/function-pods.md:688-693`: state that a loaded operator node raises when
  it cannot tell its run's stored rows.
- [ ] `uv run python scripts/assemble_changelog.py --check`,
  `uv run python scripts/design_issues.py --check`.

### Task 5 (HELD until the coordinator confirms): options 2 and 3

- Option 2: `PipelineJob.save` writes the run content hash of every `OperatorJobNode` into
  `run.node_job_content_hashes`; `load` passes it to `OperatorJobNode.from_descriptor`, which
  sets `_stored_content_hash`; `content_hash()` uses it whenever set (as `pipeline_hash`).
- Option 3: `serialize_schema` callers also write the names of top-level nullable columns as a
  sibling list (`output_schema.nullable`, `source_config.nullable_tag` / `nullable_data`);
  `deserialize_output_schema` / `Pipeline.load` restore `T | None` for them. `0.1.0` files load
  as today (lossy); operator rows of such files raise per Task 3.
- Round-trip test: every node of a job with nullable and non-null tag and data columns has the
  same pipeline hash, content hash, `node_identity_path` and `output_schema()` before save and
  after load; and `Pipeline.load` + `PipelineJob.from_pipeline(sources=<live>)` reads the
  operator node's rows in REPLAY.

### Task 6: finish

- [ ] Merge origin/main (no rebase), full `uv run pytest tests/` (at most 2 test processes),
  push, open PR against `main` ("Fixes ITL-1043" only if options 2-3 land; otherwise "Part of
  ITL-1043"), link with `orca linear attach` and `orca worktree set --pr`.
