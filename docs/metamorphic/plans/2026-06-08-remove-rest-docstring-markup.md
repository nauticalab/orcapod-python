# Remove ReST Docstring Cross-Reference Markup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove all ReST cross-reference roles from docstrings in `src/orcapod/` and replace them with plain double-backtick notation, so that `grep ':[a-z]\+:`' src/` returns zero matches.

**Architecture:** Two isolated, surgical line edits — one in `job.py` and one in `graph.py`. Both replace `:meth:\`save\`` with `` `save()` `` in a docstring `Args:` line. No logic changes; no other files touched.

**Tech Stack:** Python 3.x, uv (test runner)

---

## File Map

| File | Change |
|------|--------|
| `src/orcapod/pipeline/job.py` | Line 910: replace `:meth:\`save\`` → `` `save()` `` |
| `src/orcapod/pipeline/graph.py` | Line 234: replace `:meth:\`save\`` → `` `save()` `` |

No new files. No test files modified (tests/ is out of scope per PLT-1442).

---

### Task 1: Fix ReST markup in `job.py`

**Files:**
- Modify: `src/orcapod/pipeline/job.py:910`

- [ ] **Step 1: Confirm the exact line to change**

```bash
grep -n ":meth:" src/orcapod/pipeline/job.py
```

Expected output:
```
910:            path: Path to the JSON file produced by :meth:`save`.
```

- [ ] **Step 2: Apply the fix**

In `src/orcapod/pipeline/job.py` at line 910, change:

```python
            path: Path to the JSON file produced by :meth:`save`.
```

to:

```python
            path: Path to the JSON file produced by `save()`.
```

- [ ] **Step 3: Verify the fix and confirm no other instances remain in this file**

```bash
grep -n ":meth:\|:class:\|:exc:\|:attr:\|:func:\|:data:\|:mod:\|:obj:\|:ref:\|:doc:" src/orcapod/pipeline/job.py
```

Expected output: *(no output — zero matches)*

- [ ] **Step 4: Run the test suite to confirm no regressions**

```bash
uv run pytest tests/ -x -q 2>&1 | tail -20
```

Expected: all tests pass (or the same failures as baseline — no new failures).

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/pipeline/job.py
git commit -m "docs(job): replace :meth: ReST role with plain backtick notation"
```

---

### Task 2: Fix ReST markup in `graph.py`

**Files:**
- Modify: `src/orcapod/pipeline/graph.py:234`

- [ ] **Step 1: Confirm the exact line to change**

```bash
grep -n ":meth:" src/orcapod/pipeline/graph.py
```

Expected output:
```
234:            path: Path to the JSON file produced by :meth:`save`.
```

- [ ] **Step 2: Apply the fix**

In `src/orcapod/pipeline/graph.py` at line 234, change:

```python
            path: Path to the JSON file produced by :meth:`save`.
```

to:

```python
            path: Path to the JSON file produced by `save()`.
```

- [ ] **Step 3: Verify the fix and confirm no other instances remain in this file**

```bash
grep -n ":meth:\|:class:\|:exc:\|:attr:\|:func:\|:data:\|:mod:\|:obj:\|:ref:\|:doc:" src/orcapod/pipeline/graph.py
```

Expected output: *(no output — zero matches)*

- [ ] **Step 4: Run the full final scan across all of src/orcapod/ to confirm zero remaining ReST roles**

```bash
grep -rn ":[a-z]\+:\`" src/orcapod/
```

Expected output: *(no output — zero matches)*

- [ ] **Step 5: Run the test suite**

```bash
uv run pytest tests/ -x -q 2>&1 | tail -20
```

Expected: all tests pass (or the same failures as baseline — no new failures).

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/pipeline/graph.py
git commit -m "docs(graph): replace :meth: ReST role with plain backtick notation"
```
