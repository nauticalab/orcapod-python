# ITL-513 Stale-Entry Cleanup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close ITL-513 cleanly by marking DESIGN_ISSUES.md entry F7 resolved and adding an explicit `iter_data()` test for the cross-session stale-entry recompute scenario.

**Architecture:** No production code changes. ITL-508 already fixed the root cause via indexed entry-ID versioning. This plan only updates the design-issues log and adds a focused test that exercises the `iter_data()` read path after a cross-session miss recompute.

**Tech Stack:** Python, pytest, `InMemoryArrowDatabase`, `FunctionJobNode`, `PythonDataFunction`

---

### Task 1: Mark DESIGN_ISSUES.md F7 as resolved

**Files:**
- Modify: `DESIGN_ISSUES.md` (lines 248–255)

- [ ] **Step 1: Update F7 status and add Fix note**

Replace the existing F7 block:

```markdown
### F7 — TOCTOU race in `FunctionPodNode.add_pipeline_record`
**Status:** open
**Severity:** medium
The method checks for an existing record with `get_record_by_id` and skips insertion if found.
But it then calls `add_record(..., skip_duplicates=False)`, which will raise on a duplicate. A
race between the lookup and the insert (e.g. two concurrent processes handling the same tag+data)
would cause a crash instead of a graceful skip. Should use `skip_duplicates=True` for consistency
with the intent.
```

With:

```markdown
### F7 — TOCTOU race in `FunctionPodNode.add_pipeline_record`
**Status:** resolved
**Severity:** medium
The method checks for an existing record with `get_record_by_id` and skips insertion if found.
But it then calls `add_record(..., skip_duplicates=False)`, which will raise on a duplicate. A
race between the lookup and the insert (e.g. two concurrent processes handling the same tag+data)
would cause a crash instead of a graceful skip. Should use `skip_duplicates=True` for consistency
with the intent.

**Fix:** ITL-508 redesigned `add_pipeline_record` to use indexed entry-ID versioning
(`max_index + 1`) with `skip_duplicates=True` on the versioned key. The
`skip_cache_lookup` parameter was removed entirely, eliminating both the TOCTOU
window and the stale-entry silent-skip failure described in ITL-513.
```

- [ ] **Step 2: Verify the change looks correct**

```bash
grep -A 12 "### F7" DESIGN_ISSUES.md
```

Expected output contains `**Status:** resolved` and the Fix note.

- [ ] **Step 3: Commit**

```bash
git add DESIGN_ISSUES.md
git commit -m "docs(design-issues): mark F7 resolved — fixed by ITL-508 indexed versioning"
```

---

### Task 2: Add `test_iter_data_serves_result_after_cross_session_recompute`

**Files:**
- Modify: `tests/test_core/function_pod/test_ephemeral_result.py` (append to `TestEphemeralWritePath` class, after `test_recompute_after_ephemeral_miss_no_infinite_cycle`)

- [ ] **Step 1: Write the test**

Append the following method to the `TestEphemeralWritePath` class (after the existing `test_recompute_after_ephemeral_miss_no_infinite_cycle` method, before the closing of the class):

```python
    def test_iter_data_serves_result_after_cross_session_recompute(self):
        """After a cross-session ephemeral miss triggers recompute via execute(),
        iter_data() serves the fresh result without triggering additional computation.

        Covers ITL-513's explicit success criterion: "new test covers the stale-entry
        recompute case across iter_data calls." With iter_data() strictly read-only
        (since ENG-379), the test verifies that:
        - the recomputed result is accessible via iter_data() after execute()
        - repeated iter_data() calls do not trigger further recomputation
        """
        call_count = {"n": 0}

        def counting_double(x: int) -> int:
            call_count["n"] += 1
            return x * 2

        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()

        # Session 1: compute with ephemeral store 1 → pipeline DB gets index-0 record
        pf = PythonDataFunction(counting_double, output_keys="result")
        cfg = NodeConfig(is_result_ephemeral=True)
        pod = FunctionPod(pf, node_config=cfg)
        node1 = FunctionJobNode(
            function_pod=pod,
            input_stream=stream,
            pipeline_database=pipeline_db,
        )
        node1.set_ephemeral_store(InMemoryArrowDatabase())
        node1.execute(stream)
        assert call_count["n"] == 1

        # Session 2: fresh ephemeral store → cross-session miss → recompute → index-1 record written
        pf2 = PythonDataFunction(counting_double, output_keys="result")
        pod2 = FunctionPod(pf2, node_config=cfg)
        ephemeral2 = InMemoryArrowDatabase()
        node2 = FunctionJobNode(
            function_pod=pod2,
            input_stream=stream,
            pipeline_database=pipeline_db,
        )
        node2.set_ephemeral_store(ephemeral2)
        node2.execute(stream)
        assert call_count["n"] == 2  # recomputed once due to cross-session miss

        # iter_data() must serve the recomputed result — no additional computation
        results = list(node2.iter_data())
        assert len(results) == 1
        assert results[0][1].as_dict()["result"] == 20
        assert call_count["n"] == 2  # NOT recomputed again

        # A second iter_data() call must also return the same result without recomputing
        results2 = list(node2.iter_data())
        assert len(results2) == 1
        assert results2[0][1].as_dict()["result"] == 20
        assert call_count["n"] == 2  # still NOT recomputed
```

- [ ] **Step 2: Run the test to verify it passes**

```bash
uv run pytest tests/test_core/function_pod/test_ephemeral_result.py::TestEphemeralWritePath::test_iter_data_serves_result_after_cross_session_recompute -v
```

Expected output:
```
PASSED tests/test_core/function_pod/test_ephemeral_result.py::TestEphemeralWritePath::test_iter_data_serves_result_after_cross_session_recompute
```

- [ ] **Step 3: Run the full ephemeral result test suite to catch regressions**

```bash
uv run pytest tests/test_core/function_pod/test_ephemeral_result.py -v
```

Expected: all tests PASSED.

- [ ] **Step 4: Commit**

```bash
git add tests/test_core/function_pod/test_ephemeral_result.py
git commit -m "test(function_node): verify iter_data serves result after cross-session ephemeral recompute (ITL-513)"
```

---

### Task 3: Final verification

- [ ] **Step 1: Run the full function-pod test suite**

```bash
uv run pytest tests/test_core/function_pod/ -v
```

Expected: all tests PASSED, no regressions.

- [ ] **Step 2: Push the branch**

```bash
git push -u origin eywalker/itl-513-add_pipeline_record-skips-stale-entry-overwrite-due-to
```

- [ ] **Step 3: Create the PR**

```bash
gh pr create \
  --title "test(ITL-513): verify iter_data after cross-session recompute; mark F7 resolved" \
  --base main \
  --body "$(cat <<'EOF'
## Summary

- Marks `DESIGN_ISSUES.md` entry F7 as resolved (fixed by ITL-508 indexed entry-ID versioning).
- Adds `test_iter_data_serves_result_after_cross_session_recompute` to `TestEphemeralWritePath` in `test_ephemeral_result.py`, satisfying ITL-513's explicit success criterion for an `iter_data()` coverage test.

No production code changes — ITL-508 already fixed the root cause.

Closes ITL-513
EOF
)"
```
