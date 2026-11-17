# ITL-955 GroupBy Keeps Non-Key Tags As Tags — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:executing-plans to implement this plan
> task-by-task (inline; the machine is memory-limited, so no helper agents run test suites).
> Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** After `GroupBy`, no data column holds tag values: non-key tags stay in the tag group as
list-valued tag columns, exactly as `Batch` keeps them.

**Architecture:** `GroupBy.unary_static_process` already list-wraps every non-key column and builds
the right table; the bug is that it declares only `self.by` as the output's tag columns, so the
list-wrapped non-key tags fall into the data group (and gain scalar-null `_source_*` columns).
The fix declares all input tag columns as tags, mirrors that in `unary_output_schema`, and makes
the member sort key null-safe for list-valued tags (now reachable through `GroupBy` after
`GroupBy`).

**Tech Stack:** Python, PyArrow, pytest, `uv run`.

**Linear:** ITL-955. Branch `eywalker/itl-955-groupby-moves-non-key-tag-values-into-data`, PR
against `main`, "Fixes ITL-955".

---

## Findings (measured November 11, 2026, scratch probes, public interface)

Source: tags `(subject, session, probe)`, data `(path)`; `GroupBy(by=["subject"])`.

| | Before | After (prototype) |
|---|---|---|
| `keys()` | `(subject)`, `(session, probe, path)` | `(subject, session, probe)`, `(path)` |
| tag schema | `subject: str` | `subject: str, session: list[int], probe: list[str]` |
| data schema | `session: list[int], probe: list[str], path: list[str]` | `path: list[str]` |
| table columns | same list columns, plus `_source_session`, `_source_probe` (scalar null) | same list columns; those two `_source_*` columns are gone |

- Arrow types of the list columns are identical before and after (`large_list<T>`, non-null).
- Predicted schema equals `static_process(...).output_schema(...)` for `{}`, `system_tags`,
  `source + system_tags`. Zero-row output has the same types. Async rows equal sync rows
  (`GroupBy` has no `async_execute` of its own; the barrier path calls the same code).
- System tags (scalar fold, `::{pipeline_hash}` suffix) and the data columns' `_source_*` lists
  are unchanged.
- **Hashes.** The `GroupBy` stream's `pipeline_hash` and `content_hash` do NOT move (neither
  commits to the output schema), and neither does a downstream pod's `pipeline_hash`, so no
  database path moves. What moves: each grouped row's data content hash (fewer data columns) and
  its tag dict. Golden `pipeline.json`: `group_by.content_hash`, `group_by.pipeline_hash`,
  `group_by.system_tag_columns` unchanged; the six
  `group_by.row[{"cohort":…}].{_tag_record_id…, _tag_source_id…, data_hash}` entries are replaced
  by six keyed `row[{"cohort":"a","subject":["s1","s2"]}]` / `row[{"cohort":"b","subject":["s3"]}]`
  (system-tag values identical, `data_hash` values new).
- **Stored rows.** `source → GroupBy → select_data_columns(["path"]) → pod` run against a Delta
  store before the fix, then rerun after it on the same store: zero recomputation, cached results
  reused, the pod's output now carries the list-valued tags. A pod that took a promoted tag as a
  parameter (`def f(probe: list[int], path: list[str])`) now fails input-schema validation and
  must drop the parameter: that is the breaking change.
- **Downstream operators on list-valued tags** (all work): `Join`/`SemiJoin`/`MergeJoin` with a
  stream sharing only the scalar key; `Join` of two grouped streams sharing list-valued tags
  (joins on list equality, as two `Batch` outputs already do); `Batch` after `GroupBy` and
  `GroupBy` after `GroupBy` nest the list tags (`list[list[T]]`), as source info does;
  `select_tag_columns` / `drop_tag_columns` remove them. `GroupBy(by=[<list tag>])` still raises.
- **ITL-942** (stacked on ITL-910) changes `arrow_utils.fold_system_tag_values` and one assertion
  in `tests/test_core/operators/test_group_by.py::test_falls_back_to_record_id_when_key_covers_all_tags`;
  it does not touch `group_by.py`. No code overlap; its golden `group_by` row entries will need a
  merge on whichever lands second.
- **Audit** (rule: no data element comes from a tag value). Clean: `Batch`, `Join`, `MergeJoin`,
  `SemiJoin`, `SelectTagColumns`, `DropTagColumns`, `SelectDataColumns`, `DropDataColumns`,
  `MapTags`, `MapData`, `PolarsFilter`, `Index`, `Pick`. Only `GroupBy` leaks. Out-of-scope
  observations, reported not fixed: (1) `Join` / `MergeJoin` where a name is a tag on one side
  and data on the other raise a bare `KeyError: Field "t" exists 2 times in schema`, not
  `InputValidationError`; (2) `MapData(name_map={"<tag name>": ...})` is a silent no-op.

## Decisions (approved by the coordinator, November 11, 2026)

1. Non-key tags become list-valued **tag** columns, in input tag order (as `Batch`).
2. `by` must still name scalar tag columns; members still sort by non-key tag values, then every
   `record_id`.
3. The sort key becomes recursive so a list-valued non-key tag containing nulls compares.

## File map

- `src/orcapod/core/operators/group_by.py` — the fix, docstrings.
- `src/orcapod/core/streams/base.py` — `group_by` docstring.
- `tests/test_core/operators/test_group_by.py` — regression tests, updated shape tests.
- `tests/test_core/operators/test_async_sync_equivalence.py`, `test_chained_aggregation.py`,
  `test_aggregated_list_types.py`, `test_materialize_source_info.py`, `test_empty_inputs.py`,
  `tests/test_pipeline/test_aggregation_job.py`, `tests/test_arrays/test_array_operators.py`,
  `tests/test_core/datagrams/test_data_source_info_types.py` — update where they assert promotion.
- `tests/test_hashing/golden/data/pipeline.json` — six row entries replaced.
- `docs/concepts/operators.md`, `CLAUDE.md`, `.zed/rules`, `CHANGELOG.md`, `DESIGN_ISSUES.md`.

---

### Task 1: Failing regression tests

**Files:** Modify `tests/test_core/operators/test_group_by.py`

- [ ] **Step 1:** Replace `test_non_key_tags_promoted_to_list_data` and add the class below.

```python
class TestGroupByTagsStayTags:
    """ITL-955: no data element may come from a tag value."""

    def test_non_key_tags_stay_list_valued_tags(self, session_source):
        out = GroupBy(by=["subject", "date"])(session_source)
        tag_cols, data_cols = out.keys()
        assert tag_cols == ("subject", "date", "probe")
        assert data_cols == ("path",)
        assert out.as_table().column("probe").to_pylist() == [[0, 1], [0, 1]]

    def test_no_data_column_is_an_input_tag(self, session_source):
        in_tags, in_data = session_source.keys()
        out = GroupBy(by=["subject"])(session_source)
        assert set(out.keys()[1]) == set(in_data)
        assert not set(out.keys()[1]) & set(in_tags)

    def test_non_key_tag_schema_is_list_tag(self, session_source):
        tag_schema, data_schema = GroupBy(by=["subject", "date"])(
            session_source
        ).output_schema()
        assert "probe" in tag_schema and "probe" not in data_schema
        assert get_origin(tag_schema["probe"]) is list

    def test_non_key_tags_get_no_source_info_column(self, session_source):
        out = GroupBy(by=["subject", "date"])(session_source)
        names = out.as_table(columns={"source": True}).column_names
        assert f"{constants.SOURCE_PREFIX}probe" not in names
        assert f"{constants.SOURCE_PREFIX}path" in names

    def test_datagrams_carry_non_key_tags_on_the_tag(self, session_source):
        out = GroupBy(by=["subject", "date"])(session_source)
        for tag, data in out.iter_data():
            assert tag.as_dict()["probe"] == [0, 1]
            assert "probe" not in data.as_dict()

    def test_regroup_nests_list_tags(self, session_source):
        out = GroupBy(by=["subject"])(GroupBy(by=["subject", "date"])(session_source))
        assert out.keys() == (("subject", "date", "probe"), ("path",))
        row = out.as_table().to_pylist()[0]
        assert row["date"] == ["d1", "d2"]
        assert row["probe"] == [[0, 1], [0, 1]]

    def test_regroup_orders_list_tags_containing_nulls(self):
        table = pa.table(
            {
                "a": ["x", "x", "x", "x"],
                "b": [1, 1, 2, 2],
                "c": pa.array([None, 1, 2, 1], pa.int64()),
                "v": [1, 2, 3, 4],
            }
        )
        src = ArrowTableSource(table, tag_columns=["a", "b", "c"], infer_nullable=True)
        out = GroupBy(by=["a"])(GroupBy(by=["a", "b"])(src))
        row = out.as_table().to_pylist()[0]
        assert row["c"] == [[1, None], [1, 2]]
        assert row["v"] == [[2, 1], [4, 3]]
```

(Exact names of `constants` attributes and the fixture are checked against the file when
writing; `b` order `[1, 2]` puts `[1, None]` first.)

- [ ] **Step 2:** `uv run pytest tests/test_core/operators/test_group_by.py -q` — expect the new
  tests to FAIL (`probe` in data columns). Record the output in the PR.
- [ ] **Step 3:** Commit `test(group_by): non-key tags must stay tags (ITL-955)`.

### Task 2: The fix

**Files:** Modify `src/orcapod/core/operators/group_by.py`

- [ ] **Step 1:** In `unary_static_process`, pass `tag_columns=tag_columns` to `ArrowTableStream`
  (all input user tags, input order). Replace the two inline sort lambdas with a module-level
  recursive key:

```python
def _null_last_key(value: Any) -> tuple[Any, ...]:
    """Order key that sorts nulls last, at any list depth."""
    if value is None:
        return (1,)
    if isinstance(value, list):
        return (0, tuple(_null_last_key(v) for v in value))
    return (0, value)
```

  Check first that this gives the same order as `(v is None, v)` for scalars (it does: the
  leading element separates nulls, the second compares values) so no existing order moves.

- [ ] **Step 2:** In `unary_output_schema`, a non-key user tag goes to
  `out_tag_types[name] = list[col_type]`; iterate so the tag order equals the input's.
- [ ] **Step 3:** Rewrite the class and method docstrings (no "promoted").
- [ ] **Step 4:** `uv run pytest tests/test_core/operators/test_group_by.py -q` — PASS.
- [ ] **Step 5:** Commit `fix(group_by): keep non-key tags as list-valued tags (ITL-955)`.

### Task 3: Tests that asserted the old shape

- [ ] `uv run pytest tests/test_core tests/test_pipeline tests/test_arrays -q -x` (one process),
  fix each failure by asserting the corrected shape; pods in `test_aggregation_job.py` lose the
  `probe` parameter. Extend the `GroupBy` case in `test_async_sync_equivalence.py` to a source
  with a non-key tag if it has none. If a public-call test changes outcome for a reason other
  than the tag/data move, stop and ask.
- [ ] Commit `test: update GroupBy expectations for tags staying tags (ITL-955)`.

### Task 4: Golden entries

- [ ] `uv run pytest tests/test_hashing/golden -q` — expect only `pipeline.group_by` row entries
  to differ.
- [ ] `uv run python tests/test_hashing/golden/generate_golden_hashes.py`; `git diff --stat`
  must show only `pipeline.json`, and the diff only the six `group_by.row[...]` lines removed and
  six added. List them in the PR body.
- [ ] Commit `test(golden): regenerate group_by row entries (ITL-955)`.

### Task 5: Docs

- [ ] `docs/concepts/operators.md` GroupBy section: executed example showing `keys()` and the
  list-valued tag; `uv run pytest tests/test_docs -q`.
- [ ] `CLAUDE.md` and `.zed/rules` together: boundary paragraph, reducing rule, GroupBy bullet,
  and the rule "no data element comes from a tag value".
- [ ] `CHANGELOG.md` `[0.3.0] — unreleased`, "Breaking changes and migration".
- [ ] `DESIGN_ISSUES.md`: one entry appended at the end, status resolved.
- [ ] Commit `docs: GroupBy keeps non-key tags as tags (ITL-955)`.

### Task 6: Verify and ship

- [ ] `uv run ruff format . && uv run ruff check . && uv run --with basedpyright basedpyright`
  (exit code 0), then `uv run pytest tests/` once, single process.
- [ ] Merge `origin/main` if it moved, rerun, push, open the PR (create-pr skill), link it
  (`orca linear attach`, `orca worktree set --worktree current --pr <n>`), CodeRabbit round,
  `worker_done`.

## Progress notes

- 2026-11-11: investigation done, plan written, waiting for coordinator approval.
- 2026-11-11: coordinator approved (list-valued tags; keep the change easy to adjust if the
  developer prefers dropping). Conditions: only the six group_by row golden entries move; scalar
  sort order pinned by a test; stored-data rerun kept as a test; out-of-scope findings go to
  worker_done only.
- 2026-11-11: Tasks 1-5 done and pushed (tests, fix, golden, docs). Remaining: Task 6.
