# PR #280 review fixes: copy-schema test and shared Arrow type split — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Address the developer's two approved review comments on PR #280 (ITL-837, ITL-854, ITL-859) with no behaviour change.

**Architecture:** (1) Rewrite one datagram test to state and assert the ITL-859 bug through the public interface only. (2) Split `arrow_utils.resolve_disagreeing_types` into a pure inner function in `arrow_utils` that takes only the two Arrow types and returns the shared large type or `None`, and one outer helper in the operator layer that raises `InputValidationError` with the existing message; all four call sites (Join sync, Join async x2, MergeJoin) use the outer helper.

**Tech Stack:** Python 3.11-3.13, PyArrow, pytest, uv, ruff, basedpyright.

---

## File map

- Modify `src/orcapod/utils/arrow_utils.py`: replace `resolve_disagreeing_types` with `shared_arrow_type(left_type, right_type) -> pa.DataType | None`.
- Modify `src/orcapod/core/operators/base.py`: add `require_shared_arrow_type(operator, name, left_type, right_type) -> pa.DataType` (raises `InputValidationError`). `base.py` is imported by both `join.py` and `merge_join.py`, and importing `orcapod.errors` and `orcapod.utils.arrow_utils` from it adds no cycle.
- Modify `src/orcapod/core/operators/join.py` (3 call sites + 2 docstrings), `src/orcapod/core/operators/merge_join.py` (1 call site + 3 docstring/comment mentions).
- Modify `tests/test_core/operators/test_merge_join_arrow_types.py`: the direct tests of the old function become direct tests of `shared_arrow_type` (returns type / `None`, both orders).
- Modify `tests/test_core/datagrams/test_lazy_conversion.py`: rename and rewrite `test_copy_keeps_lazy_python_schema`.
- Modify `CLAUDE.md`, `.zed/rules`, `DESIGN_ISSUES.md` (line ~2101) where they name the old function. CHANGELOG does not name it.

## Task 1: Copy-schema test (comment 4530847226)

- [ ] Replace the test with:

```python
    @pytest.mark.parametrize("include_cache", [True, False])
    def test_copy_before_schema_is_computed_reports_correct_schema(
        self, include_cache
    ):
        """Copying an Arrow-backed datagram before its schema has been read gives
        the copy the right schema, not an empty one (ITL-859)."""
        d = Data(pa.table({"a": pa.array([1], pa.int8()), "b": pa.array(["x"])}))
        # Copy before the original's schema is first read: the state the bug needed.
        d2 = d.copy(include_cache=include_cache)
        assert d2.schema() == d.schema()
        assert d2.with_source_info(a="s").schema() == d.schema()
```

- [ ] Temporarily revert the one-line `Datagram.copy` fix (`Schema(dict(...)) if ... else None` back to `Schema(dict(...) if ... else None)`), run the test, show both parametrizations FAIL, restore with `git checkout src/orcapod/core/datagrams/datagram.py`, run again: PASS.

## Task 2: Inner function + direct tests (comment 4530865617)

- [ ] In `test_merge_join_arrow_types.py`, import `shared_arrow_type` instead of the old name; rewrite `TestResolveDisagreeingTypes` as `TestSharedArrowType`:
  - layout pairs (incl. struct) -> expected large type, both orders;
  - `MISMATCHED_PAIRS` (width, float width, timestamp unit, decimal precision, nested list width) -> `None`, both orders;
  - struct with different fields -> `None`;
  - nested nullability: `pa.list_(pa.field("item", pa.int8(), nullable=False))` vs `pa.list_(pa.int8())` -> `None`;
  - equal types -> the large-normalized type.
- [ ] Run: fails on ImportError.
- [ ] Implement in `arrow_utils.py`:

```python
def shared_arrow_type(
    left_type: "pa.DataType", right_type: "pa.DataType"
) -> "pa.DataType | None":
    """Return the large-normalized Arrow type two types share, or ``None``.
    ... (rule: normalize_to_large_types(keep_null=True) both; equal -> it; else None; order-independent)
    """
    normalized_left = normalize_to_large_types(left_type, keep_null=True)
    normalized_right = normalize_to_large_types(right_type, keep_null=True)
    return normalized_left if normalized_left == normalized_right else None
```

## Task 3: Outer helper + call sites

- [ ] In `core/operators/base.py`:

```python
def require_shared_arrow_type(
    operator: str, name: str, left_type: "pa.DataType", right_type: "pa.DataType"
) -> "pa.DataType":
    shared = arrow_utils.shared_arrow_type(left_type, right_type)
    if shared is None:
        raise InputValidationError(
            f"{operator} inputs disagree on the Arrow type of column {name!r}: "
            f"{left_type} in one input and {right_type} in the other. {operator} "
            f"never widens or promotes a type; cast one input explicitly so both "
            f"have the same Arrow type."
        )
    return shared
```

- [ ] Replace the four `arrow_utils.resolve_disagreeing_types(name, l, r, Op)` calls with `require_shared_arrow_type(Op, name, l, r)`; update docstrings/comments; delete the old function.
- [ ] `git grep resolve_disagreeing_types` -> only historical plan files under `superpowers/plans/` (and those name the earlier private `_resolve_disagreeing_types`, not this one); docs updated.
- [ ] Run `uv run pytest tests/test_core/operators tests/test_utils tests/test_core/datagrams -q`: PASS. Existing operator-level disagreeing-type tests unchanged.

## Task 4: Docs, checks, commit, push

- [ ] Update CLAUDE.md + .zed/rules "When two inputs give..." bullet, DESIGN_ISSUES ITL-837 entry wording.
- [ ] ruff check, ruff format --check, basedpyright (exit code), pytest on 3.11/3.12/3.13.
- [ ] One commit `refactor(operators): split shared Arrow type rule from the operator error (PR #280 review)` (+ test commit content), one push.
- [ ] Reply on both threads (signature lines), do not resolve; wait for CI; CodeRabbit; update PR description.

## Addendum (developer decision, 2026-11-10): Join drops its inputs' meta columns (ITL-854, ITL-857)

The developer decided that Join neither raises on nor renames colliding meta columns: it drops
every input's Data meta (`__`) columns, on both paths. Nothing downstream of a Join reads or
stores them (inventory sent to the coordinator). This replaces the ITL-854 "keep and rename"
behaviour and makes ITL-857 moot.

- Tests first (public call): new `tests/test_core/operators/test_join_meta_columns.py`. Join over
  (a) two cached function pod streams, compute and cache hit; (b) two FunctionNodes in a
  PipelineJob; (c) raw `ArrowTableStream`s with `__` columns, colliding and not; (d) three
  inputs, including the former ITL-857 crash. Each case runs sync and async and asserts: no
  upstream meta column, sync == async (`assert_equivalent`), and prediction equals
  `static_process(...).output_schema(...)`. Remove the ITL-857 and ITL-858 xfails and the
  rename tests.
- Code (`join.py`): a `_without_meta` helper drops `constants.META_PREFIX` columns from each input
  table in `static_process` (a raw `ArrowTableStream` returns `__` columns as data whatever the
  meta setting). `_joined_user_schemas` leaves `__` names out of the data schema and the
  overlap check. Delete `_collision_renames` and the rename/reference-schema code. The async
  `_merge_data` reads `as_table()` without meta and loses its `right_index` parameter.
  `static_process` always checks data overlap (it raises instead of renaming, as async does).
- Proof: dump stream content/pipeline hashes, per-row Tag/Data hashes and system tags for the
  equivalence cases, plus all stored tables of the s1->fa, s2->fb, Join, ->fc job, sync and
  async, before and after. Hashes and stored tables must be identical; only Join's emitted meta
  columns change.
- MergeJoin, SemiJoin: probe sync/async meta handling with cached pod and raw `__` inputs;
  report, and apply the same drop only if it is a small local change.
- Docs: Join docstrings, docs/concepts/operators.md, CHANGELOG (v0.2.0 changed), DESIGN_ISSUES
  JA1, PR body.
