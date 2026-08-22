# ITL-615: PollingSource PipelineJob Async Delegation Fix — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix `SourceJobNode.async_iter_data()` to delegate to `bound_source.async_iter_data()` so that a `PollingSource` bound into a `PipelineJob` runs its full async polling loop under `AsyncPipelineOrchestrator`.

**Architecture:** Add one 6-line method override to `SourceJobNode` in `source_node.py`. The method mirrors the existing delegation pattern used by `iter_data()`, `output_schema()`, and `as_table()`. Add an integration test file and a `DESIGN_ISSUES.md` entry.

**Tech Stack:** Python 3.11+, asyncio, pytest-asyncio, pyarrow, orcapod internal classes.

---

## File Map

| Action | Path | What changes |
|--------|------|-------------|
| Modify | `src/orcapod/core/nodes/source_node.py` | Add `async_iter_data()` to `SourceJobNode` |
| Create | `tests/test_channels/test_polling_source_pipeline_integration.py` | Integration tests |
| Modify | `DESIGN_ISSUES.md` | Add bug entry |

---

### Task 1: Add DESIGN_ISSUES.md entry

**Files:**
- Modify: `DESIGN_ISSUES.md`

- [ ] **Step 1: Open DESIGN_ISSUES.md and add the entry**

  Add the following block immediately after the `## Cross-cutting` header section
  (before the first `###` entry), or at the end of the file if no clear section
  applies. Insert after the `## Cross-cutting` section:

  ```markdown
  ## `src/orcapod/core/nodes/source_node.py`

  ### SJN1 — `SourceJobNode.async_iter_data()` wraps `iter_data()` synchronously, bypassing `PollingSource` polling loop
  **Status:** in progress
  **Severity:** high
  **Issue:** ITL-615

  `SourceJobNode` does not override `async_iter_data()`. The base-class
  `SourceNodeBase.async_iter_data()` wraps `self.iter_data()` (sync) as an async
  generator, so `bound_source.async_iter_data()` is never called. For
  `PollingSource`, `iter_data()` returns a static single-batch snapshot —
  the async polling loop is never started.

  **Fix:** Add `SourceJobNode.async_iter_data()` that delegates to
  `self._bound_source.async_iter_data()`, consistent with the existing delegation
  pattern of `iter_data()`, `output_schema()`, and `as_table()`.
  ```

- [ ] **Step 2: Commit**

  ```bash
  git add DESIGN_ISSUES.md
  git commit -m "docs(design-issues): log ITL-615 SourceJobNode async_iter_data bypass bug"
  ```

---

### Task 2: Write the failing integration test

**Files:**
- Create: `tests/test_channels/test_polling_source_pipeline_integration.py`

- [ ] **Step 1: Create the test file**

  Create `tests/test_channels/test_polling_source_pipeline_integration.py` with the
  following content:

  ```python
  """Integration tests: PollingSource bound into PipelineJob + AsyncPipelineOrchestrator.

  Verifies that the async polling loop runs (not just a static snapshot) when
  a PollingSource is bound as the data source for a PipelineJob and executed
  via the async orchestrator.
  """
  from __future__ import annotations

  import pytest
  import pyarrow as pa

  from orcapod import function_pod
  from orcapod.core.sources.polling_source import PollingSource
  from orcapod.databases import InMemoryArrowDatabase
  from orcapod.errors import UnboundSourceError
  from orcapod.pipeline.async_orchestrator import AsyncPipelineOrchestrator
  from orcapod.pipeline.job import PipelineJob
  from orcapod.core.nodes.source_node import SourceJobNode
  from orcapod.types import Cursor, PollingConfig, Schema


  # ---------------------------------------------------------------------------
  # Shared fake impl — two-batch DynamicSourceProtocol
  # ---------------------------------------------------------------------------

  class _TwoBatchImpl:
      """Serves exactly 2 batches then reports no new data.

      batch 0: id=1, val=10
      batch 1: id=2, val=20
      """

      _BATCHES = [
          {"id": pa.array([1], type=pa.int64()), "val": pa.array([10], type=pa.int64())},
          {"id": pa.array([2], type=pa.int64()), "val": pa.array([20], type=pa.int64())},
      ]

      def identity(self):
          return "_TwoBatchImpl"

      def to_config(self):
          return None

      @classmethod
      def from_config(cls, config):
          return cls()

      def schema(self):
          return Schema({"id": int, "val": int})

      async def poll(self, cursor=None):
          idx = cursor.value if cursor is not None else 0
          return idx < len(self._BATCHES)

      async def fetch(self, cursor=None):
          idx = cursor.value if cursor is not None else 0
          if idx >= len(self._BATCHES):
              return Cursor(value=idx), {}
          return Cursor(value=idx + 1), self._BATCHES[idx]

      async def close(self):
          pass


  # ---------------------------------------------------------------------------
  # Downstream function pod used by integration tests
  # ---------------------------------------------------------------------------

  @function_pod(output_keys="doubled")
  def _double_val(val: int) -> int:
      """Doubles the incoming value — used to produce a queryable DB record."""
      return val * 2


  # ---------------------------------------------------------------------------
  # Tests
  # ---------------------------------------------------------------------------

  class TestPollingSourcePipelineJobIntegration:
      @pytest.mark.asyncio
      async def test_async_orchestrator_runs_polling_loop(self):
          """PollingSource bound to PipelineJob runs the full async polling loop.

          A static snapshot would process only the first batch (id=1, val=10)
          and produce 1 DB record. The polling loop must process both batches and
          produce 2 DB records.
          """
          src = PollingSource(
              _TwoBatchImpl(),
              tag_columns="id",
              polling_config=PollingConfig(
                  interval=0.05,
                  duration=0.5,
                  max_missed_intervals=50,
              ),
              source_id="two_batch_src",
          )

          store = InMemoryArrowDatabase()
          job = PipelineJob(name="polling_integration_test", store=store)
          with job:
              _double_val.pod(src, label="doubled")

          await AsyncPipelineOrchestrator().run_async(job.dag)
          job.store.at(*job._name).flush()
          job.store.at(*job._name).at("_result").flush()

          records = job.nodes["doubled"].get_all_records()
          assert records is not None
          # Both batches must be processed — static snapshot yields only 1 row
          assert records.num_rows == 2, (
              f"Expected 2 rows (one per batch), got {records.num_rows}. "
              "This means only the static snapshot ran, not the polling loop."
          )
          vals = sorted(records.column("val").to_pylist())
          assert vals == [10, 20]
          doubled = sorted(records.column("doubled").to_pylist())
          assert doubled == [20, 40]

      @pytest.mark.asyncio
      async def test_unbound_source_job_node_async_iter_raises(self):
          """Unbound SourceJobNode.async_iter_data() raises UnboundSourceError."""
          node = SourceJobNode(
              name="unbound",
              tag_schema=Schema({"id": int}),
              data_schema=Schema({"val": int}),
              bound_source=None,
          )

          with pytest.raises(UnboundSourceError):
              async for _ in node.async_iter_data():
                  pass
  ```

- [ ] **Step 2: Run tests to confirm both fail**

  ```bash
  uv run pytest tests/test_channels/test_polling_source_pipeline_integration.py -v
  ```

  Expected output (both tests fail before the fix):
  - `test_async_orchestrator_runs_polling_loop` — FAIL with `AssertionError: Expected 2 rows (one per batch), got 1`
  - `test_unbound_source_job_node_async_iter_raises` — FAIL (no `UnboundSourceError` is raised because the base class iterates `iter_data()` which raises it at a different point, OR it passes for the wrong reason — either way the behavior before the fix needs verification)

  Note: The second test (`test_unbound_source_job_node_async_iter_raises`) may already pass
  if `iter_data()` raises `UnboundSourceError` synchronously when iterated in the base-class
  wrapper. That's acceptable — it becomes a regression guard.

- [ ] **Step 3: Commit the failing test**

  ```bash
  git add tests/test_channels/test_polling_source_pipeline_integration.py
  git commit -m "test(channels): add failing integration test for ITL-615 PollingSource PipelineJob polling loop"
  ```

---

### Task 3: Implement the fix

**Files:**
- Modify: `src/orcapod/core/nodes/source_node.py`

- [ ] **Step 1: Locate the insertion point**

  Open `src/orcapod/core/nodes/source_node.py`. Find the `SourceJobNode` class.
  Find the `iter_data()` method (around line 556). The new `async_iter_data()` method
  goes immediately after `iter_data()`.

- [ ] **Step 2: Add the `async_iter_data()` override**

  Insert the following method after `iter_data()` and before `output_schema()` in
  `SourceJobNode`:

  ```python
      async def async_iter_data(self):
          """Delegate to ``bound_source.async_iter_data()`` when bound.

          Overrides ``SourceNodeBase.async_iter_data()`` to route through the
          bound source's own async generator instead of wrapping ``iter_data()``
          synchronously. This ensures that dynamic sources such as
          ``PollingSource`` run their async polling loop rather than returning
          a static snapshot.

          Raises:
              UnboundSourceError: When no concrete source is attached.
          """
          if self._bound_source is None:
              raise UnboundSourceError(
                  f"SourceJobNode '{self._name}' has no concrete source bound. "
                  "Call job.bind(sources={'<name>': source}) before running."
              )
          async for pair in self._bound_source.async_iter_data():
              yield pair
  ```

  After this edit, `SourceJobNode` has these delegation methods (all consistent):

  | Method | Delegates to |
  |--------|-------------|
  | `iter_data()` | `self._bound_source.iter_data()` |
  | `async_iter_data()` | `self._bound_source.async_iter_data()` ← new |
  | `output_schema()` | `self._bound_source.output_schema()` |
  | `as_table()` | `self._bound_source.as_table()` |

- [ ] **Step 3: Run the integration tests — both should now pass**

  ```bash
  uv run pytest tests/test_channels/test_polling_source_pipeline_integration.py -v
  ```

  Expected output:
  ```
  tests/test_channels/test_polling_source_pipeline_integration.py::TestPollingSourcePipelineJobIntegration::test_async_orchestrator_runs_polling_loop PASSED
  tests/test_channels/test_polling_source_pipeline_integration.py::TestPollingSourcePipelineJobIntegration::test_unbound_source_job_node_async_iter_raises PASSED
  ```

- [ ] **Step 4: Run the full test suite to verify no regressions**

  ```bash
  uv run pytest -m "not postgres" -x -q 2>&1 | tail -20
  ```

  Expected: all existing tests pass. Pay special attention to:
  - `tests/test_channels/test_polling_source.py`
  - `tests/test_channels/test_pipeline_async_integration.py`
  - `tests/test_pipeline/test_pipeline_job.py`
  - `tests/test_pipeline/test_orchestrator.py`

- [ ] **Step 5: Update DESIGN_ISSUES.md entry status**

  Find the `SJN1` entry added in Task 1 and change `**Status:** in progress` to
  `**Status:** resolved`. Add a Fix note:

  ```markdown
  **Fix:** Added `SourceJobNode.async_iter_data()` in `source_node.py` that delegates
  to `self._bound_source.async_iter_data()`, consistent with the existing delegation
  pattern of `iter_data()`, `output_schema()`, and `as_table()`. Added integration
  test `test_polling_source_pipeline_integration.py`.
  ```

- [ ] **Step 6: Commit**

  ```bash
  git add src/orcapod/core/nodes/source_node.py DESIGN_ISSUES.md
  git commit -m "fix(source-node): delegate async_iter_data to bound_source so PollingSource runs polling loop in PipelineJob (ITL-615)"
  ```

---

## Self-Review Checklist

- [x] Spec coverage: `SourceJobNode.async_iter_data()` fix → Task 3 ✓. Integration test → Task 2 ✓. DESIGN_ISSUES.md → Task 1 ✓.
- [x] No placeholders: All code blocks are complete, no TODOs.
- [x] Type consistency: `UnboundSourceError` is imported in the test (line 10). The method signature matches `SourceNodeBase.async_iter_data()` (returns an async generator).
- [x] The `_TwoBatchImpl` class satisfies `DynamicSourceProtocol` (has `identity`, `to_config`, `from_config`, `schema`, `poll`, `fetch`, `close`).
