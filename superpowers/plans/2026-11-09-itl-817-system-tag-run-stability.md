# ITL-817 System-Tag Run Stability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make tag column order deterministic through `Join` / `MergeJoin` and row materialization, pin run-stable completion tracking with regression tests, and document the positional default record id — without changing any stored identity.

**Architecture:** Investigation (below) showed system tags stay row-aligned through `Join`, the `FunctionJobNode` cache and the orchestrator. Two things were found instead: (1) the reported signature reproduces only when a source has a run-stable `source_id`, no `record_id_column`, and a varying row order, because the default record id is positional; (2) `Join` and `MergeJoin` build `tag_columns` from a `set`, so tag column order depends on `PYTHONHASHSEED`, which makes `pa.concat_tables` raise when a buffer mixes cached and fresh rows. This plan fixes (2), tests and documents (1), and writes up changing the default as a proposal.

**Tech Stack:** Python, PyArrow, Polars, deltalake, pytest, `uv run`.

**Coordinator decision (2026-11-09):** Option A approved. Zero stored identities may change. Option B (content-derived default record ids) is a proposal only. PR body says `Part of ITL-817`.

---

## Investigation record

Measured on `61d0f18e`, one process per run, one on-disk `DeltaTableDatabase`.

| Configuration | Result |
|---|---|
| 2 sources → `Join` (cross product) → function pod → `sink_pod`, same input order, 6 processes with different `PYTHONHASHSEED` | 9/0/0/0/0/0 |
| Same, shared tag key | 9/0/0 |
| Same, explicit `source_id` + `record_id_column`, rows shuffled each run | 9/0/0/0, system tags identical per key |
| Same, default `source_id` (table hash), rows shuffled | every id changes (source id itself changes) |
| Same, explicit `source_id`, **no** `record_id_column`, rows shuffled, ITL-816 neutralised with `combine_chunks` | **9/9/9, record ids move between rows, user tags and data correct — the reported signature** |
| 2 function pods, growing inputs (cached + fresh mix) | `ArrowInvalid: Schema at index N was different` in 7 of 10 runs (tag column order) |
| Same as previous, one run | tag chunks `[1]*6` vs data chunks `[1,1,1,1,1,0,0,0,0,0,1]` → `iter_data` yields 5 of 6 (ITL-816; result-cache hit returns `Data` with zero-length chunks) |

Code paths:

- Positional record id: `src/orcapod/core/sources/stream_builder.py` `_make_provenance_token` returns `row_{i}` when `record_id_column` is `None`; `_make_record_id` hashes `source_id::token`.
- Set-ordered tags: `src/orcapod/core/operators/join.py` `static_process` (`tag_keys` is a `set`, `tag_columns=tuple(tag_keys)`); `src/orcapod/core/operators/merge_join.py` `binary_static_process` (`all_tag_keys` is a `set`).
- Cached rows use compile-time order: `FunctionJobNode._load_cached_entries` takes `self._input_stream.keys()[0]`.
- Concatenation that fails: `SyncPipelineOrchestrator._materialize_as_stream`, `StaticOutputOperatorPod._materialize_to_stream`.
- `arrow_hasher.hash_table` ignores column order (verified), so tag order never changes a record id.

## File structure

- Modify `src/orcapod/core/operators/join.py` — tag order from input streams in call order.
- Modify `src/orcapod/core/operators/merge_join.py` (only the block after `sort_system_tag_values`; stay clear of lines ~220-226).
- Modify `src/orcapod/utils/arrow_utils.py` — add `concat_tables_by_name`.
- Modify `src/orcapod/pipeline/sync_orchestrator.py`, `src/orcapod/core/operators/static_output_pod.py` — use it.
- Modify `src/orcapod/core/sources/stream_builder.py`, `arrow_table_source.py` — docstrings.
- Create `tests/test_core/operators/test_join_tag_order.py`, `tests/test_pipeline/test_system_tag_run_stability.py`.
- Create `superpowers/specs/2026-11-09-itl-817-content-derived-record-ids.md`.
- Append one entry to `DESIGN_ISSUES.md`.

---

### Task 1: Deterministic tag order in Join and MergeJoin

**Files:** `tests/test_core/operators/test_join_tag_order.py`, `join.py`, `merge_join.py`

- [ ] **Step 1: failing test.** A subprocess script builds two `ArrowTableSource`s (`probe`/`x`, `method`/`w`), applies `Join()` (and `MergeJoin()` with a colliding data column) and prints `json.dumps(list(next(iter(stream.iter_data()))[0].keys()))`. The test runs it with `PYTHONHASHSEED` in `1..6` via `subprocess.run([sys.executable, "-c", script], env=...)` and asserts every output equals `["probe", "method"]`.
- [ ] **Step 2:** `uv run --no-sync pytest tests/test_core/operators/test_join_tag_order.py -v` → FAIL (seeds 1-3 give `["method", "probe"]`).
- [ ] **Step 3: implement.** In `Join.static_process` build

```python
tag_order: list[str] = []
for s in streams:
    for key in s.keys()[0]:
        if key not in tag_order:
            tag_order.append(key)
```

  and pass `tag_columns=tuple(tag_order)`. This is the order `output_schema` already predicts (`union_schemas` over the streams in call order). In `MergeJoin.binary_static_process` pass `tag_columns=tuple(left_tag_keys) + tuple(k for k in right_tag_keys if k not in left_tag_keys)`.
- [ ] **Step 4:** rerun → PASS. Run `tests/test_core/operators`.
- [ ] **Step 5:** commit `fix(operators): deterministic tag column order in Join and MergeJoin (ITL-817)`.

### Task 2: Materialization tolerates per-row column order

**Files:** `arrow_utils.py`, `sync_orchestrator.py`, `static_output_pod.py`, `tests/test_pipeline/test_system_tag_run_stability.py`

- [ ] **Step 1: failing tests.** Build two rows whose tags are `Tag({"a": 1, "b": 2})` and `Tag({"b": 3, "a": 4})`; call `StaticOutputOperatorPod._materialize_to_stream(rows)` and `SyncPipelineOrchestrator._materialize_as_stream(rows, node_stub)`; assert two rows come back with `a`/`b` values still paired.
- [ ] **Step 2:** run → FAIL with `ArrowInvalid: Schema at index 1 was different`.
- [ ] **Step 3: implement** `arrow_utils.concat_tables_by_name(tables)`: reorder every table to the first table's column order with `table.select(names)` when the name sets are equal, then `pa.concat_tables`; if the sets differ, leave the tables alone so Arrow raises as before. Use it for both tag and data concatenations in both materializers.
- [ ] **Step 4:** rerun → PASS.
- [ ] **Step 5:** commit `fix(pipeline): align per-row tables by column name when materializing buffers (ITL-817)`.

### Task 3: End-to-end run-stability tests

**Files:** `tests/test_pipeline/test_system_tag_run_stability.py`

- [ ] **Step 1:** module-level `scale` function pod; helper `run_once(db_path, probes, methods, shared_key)` that builds a fresh `PipelineJob(name="p", store=DeltaTableDatabase(db_path))` with two `ArrowTableSource`s (`source_id=`, `record_id_column=`), `Join`, `scale.pod`, and a `sink_pod` that records deliveries, and returns `(delivered, {user_tag_key: system_tags})` captured by wrapping `side_effects._execute_side_effect_row` with `monkeypatch`.
- [ ] **Step 2:** tests, parametrized over cross-product and shared-key joins: (a) same order twice → second run delivers 0; (b) rows shuffled between three runs → 9/0/0 and identical system tags per key; (c) growing inputs 1×2 → 2×3 → 3×3 → 3×3 delivers exactly the new rows each time and never changes an existing key's system tags (cached + fresh mix).
- [ ] **Step 3:** if (c) trips ITL-816 (`iter_data` truncation, owned by the ITL-816 worker), neutralise it in a fixture that wraps `ArrowTableStream.__init__` with `combine_chunks()` and say so in the fixture docstring; do not touch `arrow_table_stream.py`.
- [ ] **Step 4:** run → PASS; confirm (c) fails on the pre-fix commit (`git stash`-free: check out `HEAD~2 -- src` into a scratch worktree or revert the two fix commits temporarily).
- [ ] **Step 5:** commit `test(pipeline): pin run-stable sink completion under reorder and mixed cache (ITL-817)`.

### Task 4: Documentation, proposal, design log

- [ ] Docstrings: `stream_builder._make_provenance_token`, `SourceStreamBuilder.build`, `ArrowTableSource` — state that without `record_id_column` the record id is positional, so a reordered row gets a new record id and is treated as a new downstream entry (recorded and delivered again); pass `record_id_column` for order-independent identity.
- [ ] If `docs/` has a user-facing sources page mentioning `record_id_column`, add one paragraph only.
- [ ] Write `superpowers/specs/2026-11-09-itl-817-content-derived-record-ids.md` (blast radius, duplicate-row rule, migration option).
- [ ] Append one `DESIGN_ISSUES.md` entry for ITL-817.
- [ ] Mirror nothing in `CLAUDE.md` / `.zed/rules` unless an instruction changes (none does).
- [ ] commit `docs(sources): document positional default record ids; propose content-derived ids (ITL-817)`.

### Task 5: Verify and ship

- [ ] `uv run --no-sync pytest tests/` full suite; record failures and skips by name.
- [ ] Rebase on `origin/main` if it moved; rerun.
- [ ] Push, open PR against `main` with `Part of ITL-817`, attach to Linear, set worktree PR, send `worker_done`.

## Self-review

- Spec coverage: hash-seed regression test (Task 1), MergeJoin and other set-built tag orders checked (Task 1; `SemiJoin` already uses schema order), reorder + mixed-cache pins (Task 3), docs (Task 4), proposal (Task 4), no identity change (hasher is column-order-insensitive; no value, name or hash is touched).
- No placeholders; names used consistently (`concat_tables_by_name`).
