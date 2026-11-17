# ENG-376: result_database rename Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rename `CachedFunctionPod._result_database` → `result_database` (and update all two definition/access sites) to fix the single FunctionNode/FunctionPod interface violation found in the ENG-376 audit.

**Architecture:** Pure rename across three locations with no logic changes. (1) The `@property` definition on `CachedFunctionPod`. (2) The instance variable `self._result_database` on `_ResultDatabaseReader`. (3) The call site `self._cached_function_pod._result_database` inside `FunctionJobNode._fetch_joined_records()`.

**Tech Stack:** Python 3.12, pytest via `uv run pytest`

---

### Task 1: Write a failing test that asserts `result_database` is a public property

The test accesses `cached_pod.result_database`. Before the rename this raises
`AttributeError` because the property is still named `_result_database`.

**Files:**
- Modify: `tests/test_core/function_pod/test_cached_function_pod.py`

- [ ] **Step 1: Add the test inside the existing `TestConstruction` class**

Open `tests/test_core/function_pod/test_cached_function_pod.py`.

The file already has a `TestConstruction` class and a `cached_pod` fixture that
creates a `CachedFunctionPod(function_pod=double_pod, result_database=cache_db)`.
Add the following method to `TestConstruction`:

```python
def test_result_database_is_public(self, cached_pod):
    """result_database must be accessible as a public property (no underscore prefix)."""
    # Will raise AttributeError until the rename in cached_function_pod.py is made.
    db = cached_pod.result_database
    assert db is not None
```

- [ ] **Step 2: Run the test to confirm it fails**

```bash
uv run pytest tests/test_core/function_pod/test_cached_function_pod.py::TestConstruction::test_result_database_is_public -v
```

Expected output (before the rename):
```
FAILED ... AttributeError: 'CachedFunctionPod' object has no attribute 'result_database'
```

---

### Task 2: Make the rename in all three locations

All three changes must be made together — they are a single logical unit. Making
only one or two will leave the code broken.

**Files:**
- Modify: `src/orcapod/core/cached_function_pod.py` (rename `@property`)
- Modify: `src/orcapod/core/nodes/function_node.py` (rename in `_ResultDatabaseReader.__init__`; update call site in `FunctionJobNode._fetch_joined_records()`)

- [ ] **Step 3: Rename the property on `CachedFunctionPod`**

In `src/orcapod/core/cached_function_pod.py`, change:

```python
    @property
    def _result_database(self) -> ArrowDatabaseProtocol:
        """The underlying result database (for FunctionNode access)."""
        return self._cache.result_database
```

to:

```python
    @property
    def result_database(self) -> ArrowDatabaseProtocol:
        """The underlying result database."""
        return self._cache.result_database
```

- [ ] **Step 4: Rename the instance variable on `_ResultDatabaseReader`**

In `src/orcapod/core/nodes/function_node.py`, inside `_ResultDatabaseReader.__init__`,
change:

```python
    def __init__(
        self,
        result_database: ArrowDatabaseProtocol,
        record_path: tuple[str, ...],
    ) -> None:
        self._result_database = result_database
        self._record_path = record_path
```

to:

```python
    def __init__(
        self,
        result_database: ArrowDatabaseProtocol,
        record_path: tuple[str, ...],
    ) -> None:
        self.result_database = result_database
        self._record_path = record_path
```

- [ ] **Step 5: Update the call site in `FunctionJobNode._fetch_joined_records()`**

In the same file (`function_node.py`), inside `_fetch_joined_records()`, change:

```python
        results = self._cached_function_pod._result_database.get_all_records(
            self._cached_function_pod.record_path,
            record_id_column=constants.DATA_RECORD_ID,
        )
```

to:

```python
        results = self._cached_function_pod.result_database.get_all_records(
            self._cached_function_pod.record_path,
            record_id_column=constants.DATA_RECORD_ID,
        )
```

- [ ] **Step 6: Run the new test to verify it now passes**

```bash
uv run pytest tests/test_core/function_pod/test_cached_function_pod.py::TestConstruction::test_result_database_is_public -v
```

Expected output:
```
PASSED
```

- [ ] **Step 7: Run the full test suite to confirm no regressions**

```bash
uv run pytest tests/ -v
```

Expected output: all tests pass. If any test fails with `AttributeError: '_result_database'`,
that test was directly accessing the private attribute and must be updated to use
`result_database` instead.

- [ ] **Step 8: Commit**

```bash
git add src/orcapod/core/cached_function_pod.py \
        src/orcapod/core/nodes/function_node.py \
        tests/test_core/function_pod/test_cached_function_pod.py
git commit -m "refactor(eng-376): rename CachedFunctionPod._result_database to result_database"
```
