# User class kinds (ITL-972) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace PR #292's user-class identity with two kinds: exact classes (ordinary
dataclass, pydantic) and structural classes (`orcapod.dataclass`).

**Architecture:** Keep PR #292's identity record, `name@digest` extension name and shared
`UserClassLogicalType`; make tokens structured JSON; add kind `structural` with its own
factory, a synthesized class on read, and one conversion helper at the pod-input boundary.
Remove `register_type`, declared names and equivalence for exact classes.

**Tech Stack:** Python 3.11–3.13, pyarrow, polars, pydantic, deltalake, pytest, `uv run`.

**Design:** `superpowers/specs/2026-11-12-itl-972-user-class-kinds.md`, approved with changes
by the developer on November 12, 2026 (its section "Answers to the open points"). The
decisions removed the pod-input conversion and the `P(other)` form from the tasks below and
added structural equality.

---

## Successor notes (keep current)

- Branch `eywalker/itl-972-user-classes-an-ordinary-dataclass-is`, created at the ITL-967
  tip c4b1e4a7. The merge of `origin/eywalker/itl-881-user-class-identity-impl` (c6837a15)
  was tried and aborted; it is redone in Task 0.
- Conflicts found (6 files) and proposed resolutions: design, section "Merge conflicts".
- Dispatch: task `task_1a2dfeda545d`, dispatch `ctx_604897ad82ad`; questions go through
  `orca orchestration ask`.
- One pytest process at a time, one Python version at a time.
- November 12: design sent; O1-O7 are with the developer. Approved meanwhile: the merge
  (done, 89b4c61b, resolutions as in the design) and failing tests for the parts no
  decision changes.
- Baseline after the merge (Python 3.12): 8626 passed, 101 skipped, 19 xfailed, 37 failed.
  The 37: the 25 golden entries of `GoldenPoint`/`GoldenModel` (the ITL-967 golden files
  are kept until the one regeneration) plus `test_no_unpinned_entries[semantic|schema|
  datagram]` and `test_values_independent_of_process_and_hash_seed[0|4242]`; and 7 new
  tests in `tests/test_user_types/test_exact_classes.py` that fail by design.
  `tests/test_user_types/test_measured_case.py` does not import yet (`op.dataclass`).

- November 12, later: developer's answers applied (no automatic conversion, `from_instance`
  only, structural `__eq__`, subclass column accepted at a base parameter). Implemented:
  `logical_types/user_class_identity.py` (kinds, names, structured tokens, record),
  `logical_types/structural_class.py` (rebuild from metadata, equality, `from_instance`),
  `logical_types/structural_logical_type_factory.py`, `orcapod/structural.py` (decorator).
  ITL-967 tip 9743004d and `main` (#321, #310, #311) merged. Golden regenerated once: 25
  moved entries, listed in the design, section e. Checks: ruff, basedpyright exit 0, suite
  green except `tests/test_design_issues.py`, which fails on `design_issues/ITL-935-LTI1.md`
  and `ITL-942-WH1.md` (files of the ITL-967 branch, to be fixed there).
- `P.from_instance` is added at run time, so static type checkers do not see it.

## Files

| File | Change |
|---|---|
| `src/orcapod/logical_types/user_class_identity.py` | kinds (`dataclass`, `pydantic`, `structural`); names (module path; structural marker); structured tokens; record; token → annotation; remove `declare`, `_declared`, `default_name(qualified_name=)`, the `__main__` notice; `equivalent_hints` becomes exact for exact classes, by record for structural |
| `src/orcapod/logical_types/user_class_logical_type.py` | exact-class value check on write; read path by module lookup for exact classes |
| `src/orcapod/logical_types/structural_logical_type_factory.py` (new) | `StructuralLogicalType`, factory, category `orcapod.structural`, synthesis on read |
| `src/orcapod/structural.py` (new) | `dataclass` decorator |
| `src/orcapod/logical_types/structural_class.py` (new) | class from a stored record (synthesis), `structural_eq`, `from_instance` |
| `src/orcapod/user_types.py` | deleted |
| `src/orcapod/__init__.py` | export `dataclass`; drop `register_type` |
| `src/orcapod/utils/schema_utils.py` | `_accepts`: `is` for exact classes, record equality for structural |
| `src/orcapod/logical_types/registry.py`, `database_hooks.py`, `databases/delta_lake_databases.py` | merge resolutions; rebinding without the cross-module warning |
| `src/orcapod/contexts/logical_types.json` | register the structural factory |
| `tests/test_user_types/` | rewritten per kind (below) |
| `tests/test_hashing/golden/cases.py`, `data/*.json` | fixtures; one regeneration |
| `docs/concepts/your-own-data-types.md`, `identity.md`, `function-pods.md` | two kinds, when to use each |
| `changelog.d/ITL-972.md`, `design_issues/ITL-972-UCK1.md` | fragment and entry; `changelog.d/ITL-881.md` and `design_issues/ITL-881-UCI1.md` from the merge are folded into them and removed |
| `CLAUDE.md`, `.zed/rules` | a "User classes" section with the two-kind rules |

## Tasks

### Task 0: Merge and baseline
- [ ] `git merge --no-ff origin/eywalker/itl-881-user-class-identity-impl`; resolve as in the
  design; golden JSON: ITL-967 side.
- [ ] `uv run pytest tests/ -x -q`; record failures (expected: golden entries of user
  classes and nothing else); commit the merge, push.

### Task 1: Regression test for the measured case (fails first)
- [ ] `tests/test_user_types/test_measured_case.py`: QOld/QNew as structural classes; pod A
  `-> QOld`, pod B `p: QNew` calling `p.describe()`; in memory and through a Delta-backed
  `PipelineJob`; assert `type(p) is QNew` on both. Same pair as ordinary classes: B refuses
  A's output on both paths. Show it failing.

### Task 2: Exact classes
- [ ] Tests: name is module path (`__main__.X`, `__mp_main__` → `__main__`); schema check
  refuses a re-defined class, a subclass at a base parameter, accepts the same object;
  write refuses a value of another class; read by import; changed fields on read →
  `TypeIdentityError` with the column; subclass keeps its fields; re-run cell does not raise.
- [ ] Implement; delete `user_types.py` and `register_type` tests; pydantic follows.

### Task 3: Structured tokens
- [ ] Tests: token of each annotation form; logical-type field uses the identifier text;
  nested class embeds the record; token → annotation round trip; order is identity.
- [ ] Implement in `user_class_identity.py`.

### Task 4: `orcapod.dataclass`
- [ ] Tests: real dataclass; options pass through (`frozen`, `slots`, `kw_only`, `order`);
  `init=False`, already-dataclass, pydantic rejected; `name=` validation; two classes of
  one name and fields share identifier and hashes; different name → different; exact-class
  field rejected; local class allowed.
- [ ] Implement `structural.py` and the factory; register the category.

### Task 5: Conversion
- [ ] Tests: `from_instance` from ordinary and structural instances; exact field-name set;
  type disagreement; nested and container conversion; `P(other)` sugar and the first-field
  rule; `__post_init__` runs; pydantic refused; `inspect.signature(P)` unchanged.
- [ ] Implement; then the pod-input helper in `data_function.py` (sync, async).
- [ ] Task 1's tests pass.

### Task 6: Reading without the class
- [ ] Test (child process, `tests/test_user_types/child_harness.py`): write with a
  structural class, read in a process that does not define it → synthesized class with the
  stored fields; nested, container, optional, logical-type fields; then define the class
  and a pod parameter receives it.
- [ ] Implement synthesis.

### Task 7: Carried-over behaviour still holds
- [ ] `tests/test_pipeline/test_type_identity_failure.py`, Delta compaction, nested `None`,
  field order, workshop scenarios rewritten for the two kinds; operators that compare
  Python types (`MergeJoin`, `Join`) with two structural classes of one identity.

### Task 8: Golden
- [ ] Add fixtures; list entries that fail before regeneration, by cause; regenerate once;
  check the diff against the list; `tests/test_hashing/test_hash_label_rule.py` passes.

### Task 9: Docs, changelog, design issue, agent rules
- [ ] Pages (doc samples run in `tests/test_docs`), `changelog.d/ITL-972.md`,
  `design_issues/ITL-972-UCK1.md`, `CLAUDE.md` + `.zed/rules`.

### Task 10: Checks and PR
- [ ] `uv run ruff format .`, `uv run --with ruff ruff check .`,
  `uv run --with basedpyright basedpyright` (exit code), `uv run pytest tests/`.
- [ ] Merge `origin/main` if it moved; push; PR against `main` ("Stacked on the ITL-967
  pull request; supersedes #292"); CodeRabbit rounds; `worker_done` with the PR URL.
