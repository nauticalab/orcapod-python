# ITL-507 Ephemeral Coverage Tests Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add targeted tests to close four coverage gaps in the ITL-507 ephemeral function-pod output feature, raising patch coverage from ~90% toward 100%.

**Architecture:** All new tests are appended to the existing `tests/test_core/function_pod/test_ephemeral_result.py` file. No new files. Each task covers a distinct uncovered code path identified by running `pytest --cov` locally against commit `4edd0c56`.

**Tech Stack:** pytest, pytest-asyncio, pyarrow, orcapod channels (`Channel`), `InMemoryArrowDatabase`

---

## Covered gaps

| Task | Lines in `function_node.py` | Code path |
|------|----------------------------|-----------|
| 1 | 1311–1342 | `_async_process_data_internal` ephemeral branch (happy path) |
| 2 | 1318–1324 | `_async_process_data_internal` RuntimeError guard (no store) |
| 3 | 1430, 1436–1439 | `add_pipeline_record` early return when duplicate detected (`skip_cache_lookup=False`) |
| 4 | 1611, 1637–1638 | `_fetch_joined_records` no-DB guard + backward-compat IS_EPHEMERAL_COL-absent branch |

---

## Files

- **Modify:** `tests/test_core/function_pod/test_ephemeral_result.py` — append four new test classes

---

## Task 1: Async ephemeral happy path

Covers `_async_process_data_internal` lines 1311–1342: the `if ephemeral_result:` branch that routes computation to `_ephemeral_cached_pod`.

**Files:**
- Modify: `tests/test_core/function_pod/test_ephemeral_result.py`

- [ ] **Step 1: Add imports at the top of the test file (after existing imports)**

  Read the current imports section first; then add `Channel` and `asyncio` if not already present. The imports section is around lines 1–16. Add:

  ```python
  import asyncio

  from orcapod.channels import Channel
  ```

  These two lines belong with the stdlib/third-party imports.

- [ ] **Step 2: Run import check**

  ```bash
  uv run python -c "from orcapod.channels import Channel; print('OK')"
  ```

  Expected: `OK`

- [ ] **Step 3: Append `TestAsyncEphemeralExecution` class to the end of the test file**

  ```python
  # ---------------------------------------------------------------------------
  # Task 11 tests: async ephemeral execution path
  # ---------------------------------------------------------------------------


  async def _collect_channel(output_ch: Channel) -> list[tuple]:
      """Drain a Channel and return the list of items."""
      return await output_ch.reader.collect()


  class TestAsyncEphemeralExecution:
      @pytest.mark.asyncio
      async def test_async_execute_ephemeral_happy_path(self):
          """async_execute with is_result_ephemeral=True writes to ephemeral store and emits results."""
          stream = _make_stream([{"id": 0, "x": 5}, {"id": 1, "x": 10}])
          pipeline_db = InMemoryArrowDatabase()
          ephemeral_store = InMemoryArrowDatabase()

          cfg = NodeConfig(is_result_ephemeral=True)
          pf = PythonDataFunction(double, output_keys="result")
          pod = FunctionPod(pf, node_config=cfg)
          node = FunctionJobNode(
              function_pod=pod,
              input_stream=stream,
              pipeline_database=pipeline_db,
          )
          node.set_ephemeral_store(ephemeral_store)

          input_ch = Channel(buffer_size=16)
          output_ch = Channel(buffer_size=16)

          for tag, data in stream.iter_data():
              await input_ch.writer.send((tag, data))
          await input_ch.writer.close()

          await node.async_execute(input_ch.reader, output_ch.writer)

          results = await _collect_channel(output_ch)
          assert len(results) == 2
          values = sorted(data.as_dict()["result"] for _, data in results)
          assert values == [10, 20]

          # Result records must be in the ephemeral store, not the persistent store
          eph_records = ephemeral_store.get_all_records(
              node._ephemeral_cached_pod.record_path,
          )
          assert eph_records is not None
          assert eph_records.num_rows == 2

      @pytest.mark.asyncio
      async def test_async_process_data_internal_raises_when_no_store(self):
          """_async_process_data_internal raises RuntimeError when is_result_ephemeral=True but no store."""
          stream = _make_stream([{"id": 0, "x": 5}])
          pipeline_db = InMemoryArrowDatabase()

          cfg = NodeConfig(is_result_ephemeral=True)
          pf = PythonDataFunction(double, output_keys="result")
          pod = FunctionPod(pf, node_config=cfg)
          node = FunctionJobNode(
              function_pod=pod,
              input_stream=stream,
              pipeline_database=pipeline_db,
          )
          # No set_ephemeral_store() call → _ephemeral_cached_pod is None

          tag, data = next(iter(stream.iter_data()))
          with pytest.raises(RuntimeError, match="is_result_ephemeral=True"):
              await node._async_process_data_internal(tag, data)
  ```

- [ ] **Step 4: Run both new tests**

  ```bash
  uv run pytest tests/test_core/function_pod/test_ephemeral_result.py::TestAsyncEphemeralExecution -xvs
  ```

  Expected: both PASS

- [ ] **Step 5: Verify coverage for lines 1311–1342**

  ```bash
  uv run pytest tests/test_core/function_pod/test_ephemeral_result.py::TestAsyncEphemeralExecution --cov=orcapod.core.nodes.function_node --cov-report=term-missing -q 2>&1 | grep "function_node"
  ```

  Line numbers 1311, 1314, 1318–1342 should no longer appear in the "Missing" column.

- [ ] **Step 6: Commit**

  ```bash
  git add tests/test_core/function_pod/test_ephemeral_result.py
  git commit -m "test(ITL-507): add async ephemeral execution path coverage"
  ```

---

## Task 2: `add_pipeline_record` duplicate-skip early return

Covers lines 1430, 1436–1439: the `skip_cache_lookup=False` path that checks for an existing pipeline record and returns early (debug log + `return`) when a duplicate is found. All internal callers use `skip_cache_lookup=True`, so this path requires a direct call in the test.

**Files:**
- Modify: `tests/test_core/function_pod/test_ephemeral_result.py`

- [ ] **Step 1: Add `import uuid` to the imports section of the test file**

  Check if `uuid` is already imported; add it if not:

  ```python
  import uuid
  ```

- [ ] **Step 2: Append `TestAddPipelineRecordDeduplication` class**

  ```python
  # ---------------------------------------------------------------------------
  # Task 12 tests: add_pipeline_record duplicate-skip early return
  # ---------------------------------------------------------------------------


  class TestAddPipelineRecordDeduplication:
      def test_duplicate_not_added_when_skip_cache_lookup_false(self):
          """add_pipeline_record with skip_cache_lookup=False is a no-op for already-seen entry_id."""
          stream = _make_stream([{"id": 0, "x": 10}])
          pipeline_db = InMemoryArrowDatabase()
          node, _ = _make_node(stream, pipeline_db=pipeline_db)

          # First execute — writes the pipeline record
          node.execute(stream)
          pipeline_db.flush()

          # Count committed records in the pipeline DB
          all_records = pipeline_db.get_all_records(node.node_identity_path)
          assert all_records is not None
          count_before = all_records.num_rows  # should be 1

          # Call add_pipeline_record again directly with skip_cache_lookup=False
          # (the default). The duplicate guard should detect the existing entry_id
          # and return without inserting a second row.
          tag, data = next(iter(stream.iter_data()))
          node.add_pipeline_record(
              tag,
              data,
              data_record_id=uuid.uuid4(),
              computed=True,
              skip_cache_lookup=False,
          )
          pipeline_db.flush()

          all_records_after = pipeline_db.get_all_records(node.node_identity_path)
          assert all_records_after is not None
          assert all_records_after.num_rows == count_before  # no new row added
  ```

- [ ] **Step 3: Run the new test**

  ```bash
  uv run pytest tests/test_core/function_pod/test_ephemeral_result.py::TestAddPipelineRecordDeduplication -xvs
  ```

  Expected: PASS

- [ ] **Step 4: Commit**

  ```bash
  git add tests/test_core/function_pod/test_ephemeral_result.py
  git commit -m "test(ITL-507): add add_pipeline_record duplicate-skip coverage"
  ```

---

## Task 3: `_fetch_joined_records` guards and backward-compat branch

Covers two uncovered lines:

- **Line 1611** (`return None`) — guard in `_fetch_joined_records` when `_cached_function_pod is None or _pipeline_database is None`. This is reached via `get_all_records()` on a no-DB node (the `get_cached_results()` method already has its own early-exit guard, but `get_all_records()` does not).
- **Lines 1637–1638** — backward-compat `else:` branch when `IS_EPHEMERAL_COL` is absent from taginfo. Simulated by executing a node, flushing, dropping the column from `_tables`, then re-executing.

**Files:**
- Modify: `tests/test_core/function_pod/test_ephemeral_result.py`

- [ ] **Step 1: Append `TestFetchJoinedRecordsGuards` class**

  ```python
  # ---------------------------------------------------------------------------
  # Task 13 tests: _fetch_joined_records guard and backward-compat branch
  # ---------------------------------------------------------------------------


  class TestFetchJoinedRecordsGuards:
      def test_get_all_records_no_db_returns_none(self):
          """get_all_records() on a FunctionJobNode with no pipeline_database returns None."""
          stream = _make_stream([{"id": 0, "x": 10}])
          pf = PythonDataFunction(double, output_keys="result")
          pod = FunctionPod(pf)
          # Intentionally no pipeline_database → _cached_function_pod is None
          node = FunctionJobNode(function_pod=pod, input_stream=stream)

          result = node.get_all_records()
          assert result is None

      def test_legacy_records_without_ephemeral_col_treated_as_persistent(self):
          """Records lacking IS_EPHEMERAL_COL are treated as persistent (backward compat)."""
          import pyarrow as pa as _pa

          stream = _make_stream([{"id": 0, "x": 10}])
          pipeline_db = InMemoryArrowDatabase()
          result_db = InMemoryArrowDatabase()

          # Session 1: write a normal persistent record
          node1, _ = _make_node(
              stream,
              pipeline_db=pipeline_db,
              result_db=result_db,
              is_result_ephemeral=False,
          )
          node1.execute(stream)
          pipeline_db.flush()

          # Drop IS_EPHEMERAL_COL from the committed table to simulate a legacy record
          from orcapod import system_constants as sc
          is_eph_col = sc.constants.IS_EPHEMERAL_COL
          record_key = "/".join(node1.node_identity_path)
          old_table = pipeline_db._tables[record_key]
          col_idx = old_table.schema.get_field_index(is_eph_col)
          assert col_idx >= 0, "IS_EPHEMERAL_COL must exist before we drop it"
          pipeline_db._tables[record_key] = old_table.remove_column(col_idx)

          # Session 2: new node with same DBs — should handle missing column gracefully
          pf2 = PythonDataFunction(double, output_keys="result")
          pod2 = FunctionPod(pf2)
          node2 = FunctionJobNode(
              function_pod=pod2,
              input_stream=stream,
              pipeline_database=pipeline_db,
              result_database=result_db,
          )
          results = node2.execute(stream)

          # Result must be served (either from cache or recomputed)
          assert len(results) == 1
          assert results[0][1].as_dict()["result"] == 20
  ```

  **Note on the import alias**: `import pyarrow as pa as _pa` is intentional — it avoids shadowing the module-level `pa` if it's already imported. If there's no top-level `pa` in this test file, use `import pyarrow as pa` instead and remove the alias.

  Actually, looking at the test file, `import pyarrow as pa` IS already at module scope (line 4). The `import pyarrow as pa as _pa` syntax is invalid Python. Remove the inner `import pyarrow as pa as _pa` line from the test — it's not needed since `pa` is already available at module scope.

  Corrected version of the `test_legacy_records_without_ephemeral_col_treated_as_persistent` test (no inner import of pyarrow needed):

  ```python
      def test_legacy_records_without_ephemeral_col_treated_as_persistent(self):
          """Records lacking IS_EPHEMERAL_COL are treated as persistent (backward compat)."""
          stream = _make_stream([{"id": 0, "x": 10}])
          pipeline_db = InMemoryArrowDatabase()
          result_db = InMemoryArrowDatabase()

          # Session 1: write a normal persistent record
          node1, _ = _make_node(
              stream,
              pipeline_db=pipeline_db,
              result_db=result_db,
              is_result_ephemeral=False,
          )
          node1.execute(stream)
          pipeline_db.flush()

          # Drop IS_EPHEMERAL_COL from the committed table to simulate a legacy record
          from orcapod import system_constants as sc
          is_eph_col = sc.constants.IS_EPHEMERAL_COL
          record_key = "/".join(node1.node_identity_path)
          old_table = pipeline_db._tables[record_key]
          col_idx = old_table.schema.get_field_index(is_eph_col)
          assert col_idx >= 0, "IS_EPHEMERAL_COL must exist before we drop it"
          pipeline_db._tables[record_key] = old_table.remove_column(col_idx)

          # Session 2: new node with same DBs — should handle missing column gracefully
          pf2 = PythonDataFunction(double, output_keys="result")
          pod2 = FunctionPod(pf2)
          node2 = FunctionJobNode(
              function_pod=pod2,
              input_stream=stream,
              pipeline_database=pipeline_db,
              result_database=result_db,
          )
          results = node2.execute(stream)

          # Result must be served (either from cache or recomputed)
          assert len(results) == 1
          assert results[0][1].as_dict()["result"] == 20
  ```

- [ ] **Step 2: Run both new tests**

  ```bash
  uv run pytest tests/test_core/function_pod/test_ephemeral_result.py::TestFetchJoinedRecordsGuards -xvs
  ```

  Expected: both PASS

- [ ] **Step 3: Run full ephemeral test suite to check for regressions**

  ```bash
  uv run pytest tests/test_core/function_pod/test_ephemeral_result.py -x -q
  ```

  Expected: all tests PASS

- [ ] **Step 4: Confirm the target lines are now covered**

  ```bash
  uv run pytest tests/test_core/function_pod/test_ephemeral_result.py \
    --cov=orcapod.core.nodes.function_node \
    --cov-report=term-missing -q 2>&1 | grep "function_node"
  ```

  Lines 1611, 1637, and 1638 should NOT appear in the Missing column.

- [ ] **Step 5: Commit**

  ```bash
  git add tests/test_core/function_pod/test_ephemeral_result.py
  git commit -m "test(ITL-507): cover _fetch_joined_records guards and backward-compat branch"
  ```

---

## Task 4: Final coverage check and push

- [ ] **Step 1: Run full targeted coverage check across all four new classes**

  ```bash
  uv run pytest tests/test_core/function_pod/test_ephemeral_result.py \
    --cov=orcapod.core.nodes.function_node \
    --cov=orcapod.pipeline.base \
    --cov-report=term-missing -q 2>&1 | tail -20
  ```

  The Missing column for `function_node.py` should no longer contain lines 1311–1342, 1430, 1436–1439, 1611, 1637–1638.

- [ ] **Step 2: Run broad test suite to verify no regressions**

  ```bash
  uv run pytest tests/ --ignore=tests/test_databases -x -q 2>&1 | tail -5
  ```

  Expected: all tests pass.

- [ ] **Step 3: Push**

  ```bash
  git push
  ```

---

## Self-review

**Spec coverage:** All four identified coverage gaps have a task. ✓

**Placeholder scan:** No TBD/TODO/similar in code blocks. ✓

**Type consistency:**
- `FunctionJobNode` constructor params match `__init__` signature throughout. ✓
- `_async_process_data_internal(tag, data)` signature is correct (no keyword-only logger here — it's called without logger). ✓
- `add_pipeline_record(tag, data, data_record_id=uuid.uuid4(), computed=True, skip_cache_lookup=False)` matches actual signature. ✓
- `node.node_identity_path` is a `tuple[str, ...]`, so `"/".join(...)` works. ✓
- `old_table.remove_column(col_idx)` — PyArrow `Table.remove_column` takes an integer column index. ✓
