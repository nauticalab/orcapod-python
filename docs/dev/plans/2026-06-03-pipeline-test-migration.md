# Pipeline Test Migration to PipelineJob API — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Migrate all ~107 skipped `tests/test_pipeline/` tests from the old `Pipeline` API to the new `PipelineJob` API, restoring full regression coverage for the orchestration and observer subsystems.

**Architecture:** Two small production changes (rename `ExecutorType` → `OrchestratorType`, add `orchestrator=` parameter to `PipelineJob.run()`) unlock the test migrations. Tests are migrated file-by-file in order of ascending complexity. No new features; no behavior changes.

**Tech Stack:** Python 3.12+, pytest, uv, orcapod internal APIs (`PipelineJob`, `SyncPipelineOrchestrator`, `AsyncPipelineOrchestrator`, observer classes).

**Spec:** `superpowers/specs/2026-06-03-pipeline-test-migration-design.md`

---

## File Map

| Action | File | Change |
|---|---|---|
| Modify | `src/orcapod/types.py` | Rename `ExecutorType` → `OrchestratorType`; `PipelineConfig.executor` → `.orchestrator` |
| Modify | `src/orcapod/pipeline/job.py` | Add `orchestrator=` param to `run()` |
| Modify | `tests/test_channels/test_channels.py` | Update `ExecutorType` → `OrchestratorType` references |
| Migrate | `tests/test_pipeline/test_sync_orchestrator.py` | 8 skipped classes |
| Migrate | `tests/test_pipeline/test_composite_observer.py` | 4 skipped classes |
| Migrate | `tests/test_pipeline/test_status_observer_integration.py` | 11 skipped classes |
| Migrate | `tests/test_pipeline/test_logging_observer_integration.py` | 9 skipped classes |
| Migrate | `tests/test_pipeline/test_orchestrator.py` | 8 skipped classes |
| Migrate | `tests/test_pipeline/test_graph_rendering.py` | 8 skipped classes |
| Migrate | `tests/test_pipeline/test_orchestrator_executor_matrix.py` | 7 skipped classes + rename refs |
| Verify | `tests/test_pipeline/test_serialization_helpers.py` | Confirm no skips remain |
| Verify | `tests/test_pipeline/test_integration_smoke.py` | Confirm no skips remain |

---

## API Translation Reference

Keep this table in mind throughout every migration task:

| Old (`Pipeline`) | New (`PipelineJob`) |
|---|---|
| `Pipeline(name, pipeline_database=db)` | `PipelineJob(name, store=db)` |
| `pipeline.run(orchestrator=orch, observer=obs)` | `job.run(orchestrator=orch, observer=obs)` |
| `pipeline.label.get_all_records()` | `job.nodes["label"].get_all_records()` |
| `pipeline.dag` | `job.pipeline.dag` |
| `pipeline.show_graph()` | `job.pipeline.show_graph()` |
| `AsyncPipelineOrchestrator().run(pipeline.dag)` | `job.run(orchestrator=AsyncPipelineOrchestrator())` |
| `pipeline.compile(); pipeline.flush()` | Not needed — `with job:` compiles; `run()` flushes |

---

## Task 1: Rename ExecutorType → OrchestratorType

**Files:**
- Modify: `src/orcapod/types.py`
- Modify: `tests/test_channels/test_channels.py:547-568`
- Modify: `tests/test_pipeline/test_orchestrator_executor_matrix.py` (import line 46, usages at 88, 98)

- [ ] **Step 1: Update `src/orcapod/types.py`**

  Replace the `ExecutorType` class and `PipelineConfig.executor` field.

  Find this block (lines ~274–309):
  ```python
  class ExecutorType(Enum):
      """Pipeline execution strategy.

      Attributes:
          SYNCHRONOUS: Current behavior -- ``static_process`` chain with
              pull-based materialization.
          ASYNC_CHANNELS: Push-based async channel execution via
              ``async_execute``.
      """

      SYNCHRONOUS = "synchronous"
      ASYNC_CHANNELS = "async_channels"


  @dataclass(frozen=True, slots=True)
  class PipelineConfig:
      """Pipeline-level execution configuration.

      Attributes:
          executor: Which execution strategy to use.
          channel_buffer_size: Max items buffered per channel edge.
          default_max_concurrency: Pipeline-wide default for per-node
              concurrency.  ``None`` means unlimited.
          execution_engine: Optional data-function executor applied to all
              function nodes (e.g. ``RayExecutor``).  ``None`` means in-process
              execution.
          execution_engine_opts: Resource/options dict forwarded to the engine
              via ``with_options()`` (e.g. ``{"num_cpus": 4}``).
      """

      executor: ExecutorType = ExecutorType.SYNCHRONOUS
      channel_buffer_size: int = 64
      default_max_concurrency: int | None = None
      execution_engine: DataFunctionExecutorProtocol | None = None
      execution_engine_opts: dict[str, Any] | None = None
  ```

  Replace with:
  ```python
  class OrchestratorType(Enum):
      """Pipeline orchestrator selection.

      Attributes:
          SYNCHRONOUS: Pull-based synchronous DAG walk via
              ``SyncPipelineOrchestrator``.
          ASYNC_CHANNELS: Push-based async channel execution via
              ``AsyncPipelineOrchestrator``.

      Note:
          This is distinct from ``DataFunction.executor``, which controls
          distributed execution of individual data functions (e.g. Ray).
          ``OrchestratorType`` selects the pipeline-level coordination
          strategy only.
      """

      SYNCHRONOUS = "synchronous"
      ASYNC_CHANNELS = "async_channels"


  @dataclass(frozen=True, slots=True)
  class PipelineConfig:
      """Pipeline-level execution configuration.

      Attributes:
          orchestrator: Which orchestrator strategy to use.
          channel_buffer_size: Max items buffered per channel edge.
          default_max_concurrency: Pipeline-wide default for per-node
              concurrency.  ``None`` means unlimited.
          execution_engine: Optional data-function executor applied to all
              function nodes (e.g. ``RayExecutor``).  ``None`` means in-process
              execution.
          execution_engine_opts: Resource/options dict forwarded to the engine
              via ``with_options()`` (e.g. ``{"num_cpus": 4}``).
      """

      orchestrator: OrchestratorType = OrchestratorType.SYNCHRONOUS
      channel_buffer_size: int = 64
      default_max_concurrency: int | None = None
      execution_engine: DataFunctionExecutorProtocol | None = None
      execution_engine_opts: dict[str, Any] | None = None
  ```

- [ ] **Step 2: Update `tests/test_channels/test_channels.py`**

  Find lines ~547–568 and update four occurrences:
  ```python
  # Before (3 separate inline imports + usages):
  from orcapod.types import ExecutorType
  assert ExecutorType.SYNCHRONOUS.value == "synchronous"
  assert ExecutorType.ASYNC_CHANNELS.value == "async_channels"

  from orcapod.types import ExecutorType, PipelineConfig
  cfg = PipelineConfig()
  assert cfg.executor == ExecutorType.SYNCHRONOUS

  from orcapod.types import ExecutorType, PipelineConfig
  cfg = PipelineConfig(
      executor=ExecutorType.ASYNC_CHANNELS,
      ...
  )
  assert cfg.executor == ExecutorType.ASYNC_CHANNELS
  ```

  Replace with:
  ```python
  from orcapod.types import OrchestratorType
  assert OrchestratorType.SYNCHRONOUS.value == "synchronous"
  assert OrchestratorType.ASYNC_CHANNELS.value == "async_channels"

  from orcapod.types import OrchestratorType, PipelineConfig
  cfg = PipelineConfig()
  assert cfg.orchestrator == OrchestratorType.SYNCHRONOUS

  from orcapod.types import OrchestratorType, PipelineConfig
  cfg = PipelineConfig(
      orchestrator=OrchestratorType.ASYNC_CHANNELS,
      ...
  )
  assert cfg.orchestrator == OrchestratorType.ASYNC_CHANNELS
  ```

- [ ] **Step 3: Update `tests/test_pipeline/test_orchestrator_executor_matrix.py` import**

  Find line ~46:
  ```python
  from orcapod.types import ExecutorType, NodeConfig, PipelineConfig
  ```
  Replace with:
  ```python
  from orcapod.types import NodeConfig, OrchestratorType, PipelineConfig
  ```

  Also find any remaining `ExecutorType.SYNCHRONOUS` / `ExecutorType.ASYNC_CHANNELS` references in this file (lines ~88, 98) and replace with `OrchestratorType.SYNCHRONOUS` / `OrchestratorType.ASYNC_CHANNELS`.

- [ ] **Step 4: Verify no remaining `ExecutorType` references**

  ```bash
  grep -rn "ExecutorType" src/ tests/ --include="*.py"
  ```
  Expected: no output.

- [ ] **Step 5: Run the full test suite to confirm no breakage**

  ```bash
  uv run pytest tests/ -x -q 2>&1 | tail -20
  ```
  Expected: same pass/fail counts as before (skipped tests remain skipped).

- [ ] **Step 6: Commit**

  ```bash
  git add src/orcapod/types.py tests/test_channels/test_channels.py tests/test_pipeline/test_orchestrator_executor_matrix.py
  git commit -m "refactor(types): rename ExecutorType to OrchestratorType

  ExecutorType was ambiguous — DataFunction also has an executor concept
  (for distributed backends like Ray). OrchestratorType makes the
  pipeline-level orchestration strategy explicit. Rename
  PipelineConfig.executor → PipelineConfig.orchestrator accordingly.

  Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
  ```

---

## Task 2: Add `orchestrator=` Parameter to `PipelineJob.run()`

**Files:**
- Modify: `src/orcapod/pipeline/job.py:643-775`
- Modify: `tests/test_pipeline/test_pipeline_job.py` (add orchestrator param test)

- [ ] **Step 1: Write a failing test in `tests/test_pipeline/test_pipeline_job.py`**

  Find the `TestPipelineJobRun` class and add this test at the end of it:

  ```python
  def test_run_accepts_explicit_sync_orchestrator(self, tmp_path):
      from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator

      db = InMemoryArrowDatabase()
      source = _make_source([{"x": 1}, {"x": 2}])

      @FunctionPod
      def doubler(x: int) -> int:
          return x * 2

      job = PipelineJob(name="orch_param", store=db)
      with job:
          doubler(source, label="doubler")

      completed = job.run(orchestrator=SyncPipelineOrchestrator())
      records = completed.nodes["doubler"].get_all_records()
      assert len(records) == 2

  def test_run_accepts_async_orchestrator(self, tmp_path):
      from orcapod.pipeline import AsyncPipelineOrchestrator

      db = InMemoryArrowDatabase()
      source = _make_source([{"x": 1}, {"x": 2}])

      @FunctionPod
      def doubler(x: int) -> int:
          return x * 2

      job = PipelineJob(name="async_orch", store=db)
      with job:
          doubler(source, label="doubler")

      completed = job.run(orchestrator=AsyncPipelineOrchestrator())
      records = completed.nodes["doubler"].get_all_records()
      assert len(records) == 2
  ```

- [ ] **Step 2: Run the new tests to confirm they fail**

  ```bash
  uv run pytest tests/test_pipeline/test_pipeline_job.py::TestPipelineJobRun::test_run_accepts_explicit_sync_orchestrator tests/test_pipeline/test_pipeline_job.py::TestPipelineJobRun::test_run_accepts_async_orchestrator -v
  ```
  Expected: `FAILED` — `run()` does not accept `orchestrator` keyword argument yet.

- [ ] **Step 3: Modify `run()` in `src/orcapod/pipeline/job.py`**

  Find the `run()` method signature (line ~643):
  ```python
  def run(
      self,
      observer: "ExecutionObserverProtocol | None" = None,
  ) -> "PipelineJob":
      """Execute the resolvable subgraph of this job in place.
      ...
      Args:
          observer: Optional execution observer.
      ...
      """
      import hashlib
      import uuid

      from orcapod.pipeline.observer import NoOpObserver
      from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator
  ```

  Replace with:
  ```python
  def run(
      self,
      orchestrator: "SyncPipelineOrchestrator | AsyncPipelineOrchestrator | None" = None,
      observer: "ExecutionObserverProtocol | None" = None,
  ) -> "PipelineJob":
      """Execute the resolvable subgraph of this job in place.
      ...
      Args:
          orchestrator: Orchestrator to use. Defaults to
              ``SyncPipelineOrchestrator`` when ``None``.
          observer: Optional execution observer.
      ...
      """
      import hashlib
      import uuid

      from orcapod.pipeline.observer import NoOpObserver
      from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator
  ```

  Then find the orchestrator call (line ~758):
  ```python
  SyncPipelineOrchestrator().run(
      exec_dag,
      observer=effective_observer,
      run_id=run_id,
      pipeline_uri=pipeline_uri,
  )
  ```

  Replace with:
  ```python
  effective_orchestrator = orchestrator or SyncPipelineOrchestrator()
  effective_orchestrator.run(
      exec_dag,
      observer=effective_observer,
      run_id=run_id,
      pipeline_uri=pipeline_uri,
  )
  ```

  Also add `AsyncPipelineOrchestrator` to the `TYPE_CHECKING` imports at the top of the file. Find the `if TYPE_CHECKING:` block near the top of `job.py` and add:
  ```python
  if TYPE_CHECKING:
      ...
      from orcapod.pipeline.async_orchestrator import AsyncPipelineOrchestrator
      from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator
  ```

- [ ] **Step 4: Run the new tests to confirm they pass**

  ```bash
  uv run pytest tests/test_pipeline/test_pipeline_job.py::TestPipelineJobRun::test_run_accepts_explicit_sync_orchestrator tests/test_pipeline/test_pipeline_job.py::TestPipelineJobRun::test_run_accepts_async_orchestrator -v
  ```
  Expected: both `PASSED`.

- [ ] **Step 5: Run the full pipeline test suite to confirm no regressions**

  ```bash
  uv run pytest tests/test_pipeline/ -x -q 2>&1 | tail -20
  ```
  Expected: same counts as before (skipped tests remain skipped, no new failures).

- [ ] **Step 6: Commit**

  ```bash
  git add src/orcapod/pipeline/job.py tests/test_pipeline/test_pipeline_job.py
  git commit -m "feat(pipeline): add orchestrator= parameter to PipelineJob.run()

  Defaults to SyncPipelineOrchestrator when None, preserving existing
  behavior. Allows async orchestrator tests to be migrated without
  calling orchestrator internals directly.

  Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
  ```

---

## Task 3: Migrate `test_sync_orchestrator.py`

**Files:**
- Migrate: `tests/test_pipeline/test_sync_orchestrator.py`

This file has 8 skipped classes. The migration pattern for all of them is:
- Replace `Pipeline(name, pipeline_database=db)` → `PipelineJob(name, store=db)`
- Replace `orch.run(pipeline.dag)` / `pipeline.run(orchestrator=orch)` → `job.run()` or `job.run(orchestrator=orch)`
- Replace `pipeline.label.get_all_records()` → `job.nodes["label"].get_all_records()`
- Replace `result.node_outputs` access → `job.nodes["label"].get_all_records()`

The `TestMaterializeResults` class tests `materialize_results=False` on the orchestrator. `PipelineJob.run()` exposes this parameter and passes it through to the orchestrator, so this class can be fully migrated. The migration pattern is the same as the other classes, plus the assertion must check `job.nodes["label"].get_all_records().num_rows` to confirm DB records are written regardless of the `materialize_results` setting.

- [ ] **Step 1: Read the skipped test classes**

  ```bash
  grep -n "class Test\|@pytest.mark.skip" tests/test_pipeline/test_sync_orchestrator.py
  ```
  Expected output lists 8 `@pytest.mark.skip` decorators before class definitions.

- [ ] **Step 2: Update imports at the top of the file**

  Find the old imports:
  ```python
  from orcapod.pipeline import Pipeline, SyncPipelineOrchestrator
  ```
  Replace with (keep any existing `PipelineJob` import, add if missing):
  ```python
  from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator
  from orcapod.pipeline.job import PipelineJob
  ```

- [ ] **Step 3: Remove `@pytest.mark.skip` from all 8 classes**

  For each class, delete the line:
  ```python
  @pytest.mark.skip(reason="Migrating to PipelineJob-based API — pending migration task")
  ```

- [ ] **Step 4: Update all fixtures and test methods to use `PipelineJob`**

  Apply these substitutions throughout every test method in the file:

  **Construction:**
  ```python
  # Before
  pipeline = Pipeline(name="linear", pipeline_database=InMemoryArrowDatabase())
  with pipeline:
      doubler(src, label="doubler")

  # After
  job = PipelineJob(name="linear", store=InMemoryArrowDatabase())
  with job:
      doubler(src, label="doubler")
  ```

  **Sync execution:**
  ```python
  # Before
  orch = SyncPipelineOrchestrator()
  orch.run(pipeline.dag)

  # After
  job.run()  # defaults to SyncPipelineOrchestrator
  ```

  **Async execution:**
  ```python
  # Before
  AsyncPipelineOrchestrator().run(pipeline.dag)

  # After
  job.run(orchestrator=AsyncPipelineOrchestrator())
  ```

  **Node result access:**
  ```python
  # Before
  records = pipeline.doubler.get_all_records()

  # After
  records = job.nodes["doubler"].get_all_records()
  ```

  **Observer injection** (TestSyncOrchestratorObserver, TestSyncObserverInjection):
  ```python
  # Before
  orch = SyncPipelineOrchestrator()
  orch.run(pipeline.dag, observer=recording_observer)

  # After
  job.run(observer=recording_observer)
  ```

  **TestSyncAsyncParity** (compares sync vs async results):
  ```python
  # Before
  pipeline_sync = Pipeline(name="sync", pipeline_database=db_sync)
  with pipeline_sync:
      pod(src, label="node")
  SyncPipelineOrchestrator().run(pipeline_sync.dag)
  sync_records = pipeline_sync.node.get_all_records()

  pipeline_async = Pipeline(name="async", pipeline_database=db_async)
  with pipeline_async:
      pod(src, label="node")
  AsyncPipelineOrchestrator().run(pipeline_async.dag)
  async_records = pipeline_async.node.get_all_records()

  # After
  job_sync = PipelineJob(name="sync", store=db_sync)
  with job_sync:
      pod(src, label="node")
  job_sync.run()
  sync_records = job_sync.nodes["node"].get_all_records()

  job_async = PipelineJob(name="async", store=db_async)
  with job_async:
      pod(src, label="node")
  job_async.run(orchestrator=AsyncPipelineOrchestrator())
  async_records = job_async.nodes["node"].get_all_records()
  ```

- [ ] **Step 5: Migrate `TestMaterializeResults`**

  Remove the `@pytest.mark.skip` decorator and migrate the class to use `PipelineJob`. Each test should call `job.run(materialize_results=False)` (or `True`) and verify that DB records are written regardless:

  ```python
  class TestMaterializeResults:
      def test_sync_materialize_false_db_records_persisted(self):
          job = self._make_job()
          job.run(materialize_results=False)
          records = job.nodes["doubler"].get_all_records()
          assert records is not None
          assert records.num_rows == 3

      def test_async_materialize_false_db_records_persisted(self):
          job = self._make_job()
          job.run(orchestrator=AsyncPipelineOrchestrator(), materialize_results=False)
          records = job.nodes["doubler"].get_all_records()
          assert records is not None
          assert records.num_rows == 3
  ```

- [ ] **Step 6: Run the migrated tests**

  ```bash
  uv run pytest tests/test_pipeline/test_sync_orchestrator.py -v 2>&1 | tail -30
  ```
  Expected: all previously-skipped tests now `PASSED`. Fix any failures before continuing.

- [ ] **Step 7: Commit**

  ```bash
  git add tests/test_pipeline/test_sync_orchestrator.py
  git commit -m "test(sync_orchestrator): migrate skipped tests to PipelineJob API

  Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
  ```

---

## Task 4: Migrate `test_composite_observer.py`

**Files:**
- Migrate: `tests/test_pipeline/test_composite_observer.py`

4 skipped classes: `TestLoggingAndStatusTogether`, `TestCreateDataLoggerDelegation`, `TestContextualizeReturnsComposite`, `TestCompositeWithFailures`.

All use `CompositeObserver(log_obs, status_obs)` passed to `pipeline.run()`. Migration: same PipelineJob substitution, pass observer to `job.run(observer=composite_obs)`.

- [ ] **Step 1: Update imports**

  Find:
  ```python
  from orcapod.pipeline import Pipeline, SyncPipelineOrchestrator
  ```
  Replace with:
  ```python
  from orcapod.pipeline import SyncPipelineOrchestrator
  from orcapod.pipeline.job import PipelineJob
  ```

- [ ] **Step 2: Remove `@pytest.mark.skip` from all 4 classes**

  Delete each line:
  ```python
  @pytest.mark.skip(reason="Migrating to PipelineJob-based API — pending migration task")
  ```

- [ ] **Step 3: Update fixtures and test bodies**

  The shared fixture (if any) and each test method follow this pattern:

  ```python
  # Before
  pipeline = Pipeline(name="test_composite", pipeline_database=obs_db.at("test_composite"))
  with pipeline:
      pod(source, label="doubler")

  composite = CompositeObserver(log_obs, status_obs)
  SyncPipelineOrchestrator().run(pipeline.dag, observer=composite)

  # After
  job = PipelineJob(name="test_composite", store=obs_db.at("test_composite"))
  with job:
      pod(source, label="doubler")

  composite = CompositeObserver(log_obs, status_obs)
  job.run(observer=composite)
  ```

  For `TestContextualizeReturnsComposite` which calls `observer.contextualize(...)` directly — no `run()` needed, just keep that test as-is (no Pipeline/PipelineJob involved). Remove the skip decorator only.

- [ ] **Step 4: Run the migrated tests**

  ```bash
  uv run pytest tests/test_pipeline/test_composite_observer.py -v 2>&1 | tail -20
  ```
  Expected: all 4 previously-skipped classes pass. Fix any failures.

- [ ] **Step 5: Commit**

  ```bash
  git add tests/test_pipeline/test_composite_observer.py
  git commit -m "test(composite_observer): migrate skipped tests to PipelineJob API

  Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
  ```

---

## Task 5: Migrate `test_status_observer_integration.py`

**Files:**
- Migrate: `tests/test_pipeline/test_status_observer_integration.py`

11 skipped classes covering `StatusObserver` integration: success/failure status events, flat storage, queryable columns, async orchestrator, fail-fast policy, mixed success/failure, multi-node, run ID tracking, schema validation.

- [ ] **Step 1: Update imports**

  Find:
  ```python
  from orcapod.pipeline import (
      AsyncPipelineOrchestrator, Pipeline, SyncPipelineOrchestrator,
  )
  ```
  Replace with:
  ```python
  from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator
  from orcapod.pipeline.job import PipelineJob
  ```

- [ ] **Step 2: Remove `@pytest.mark.skip` from all 11 classes**

  Delete each:
  ```python
  @pytest.mark.skip(reason="Migrating to PipelineJob-based API — pending migration task")
  ```

- [ ] **Step 3: Update fixtures and test bodies**

  **Sync orchestrator tests** (`TestSyncPipelineSuccessStatus`, `TestFailingDatasStatus`, `TestFlatStatusStorage`, `TestQueryableTagColumns`, `TestFailFastErrorPolicy`, `TestMixedSuccessFailure`, `TestMultipleFunctionNodesSeparateStatus`, `TestGetStatusNodeSpecific`, `TestStatusSchema`, `TestRunIdTracking`):

  ```python
  # Before
  pipeline = Pipeline(name="test_status", pipeline_database=db)
  with pipeline:
      pod(source, label="doubler")
  obs = StatusObserver(status_database=db)
  SyncPipelineOrchestrator().run(pipeline.dag, observer=obs)
  status = obs.get_status()

  # After
  job = PipelineJob(name="test_status", store=db)
  with job:
      pod(source, label="doubler")
  obs = StatusObserver(status_database=db)
  job.run(observer=obs)
  status = obs.get_status()
  ```

  **Async orchestrator tests** (`TestAsyncOrchestratorStatus`):

  ```python
  # Before
  AsyncPipelineOrchestrator().run(pipeline.dag, observer=obs)

  # After
  job.run(orchestrator=AsyncPipelineOrchestrator(), observer=obs)
  ```

  **Node result access where used:**
  ```python
  # Before
  records = pipeline.doubler.get_all_records()
  # After
  records = job.nodes["doubler"].get_all_records()
  ```

- [ ] **Step 4: Run the migrated tests**

  ```bash
  uv run pytest tests/test_pipeline/test_status_observer_integration.py -v 2>&1 | tail -30
  ```
  Expected: all 11 previously-skipped classes pass. Fix any failures.

- [ ] **Step 5: Commit**

  ```bash
  git add tests/test_pipeline/test_status_observer_integration.py
  git commit -m "test(status_observer): migrate skipped tests to PipelineJob API

  Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
  ```

---

## Task 6: Migrate `test_logging_observer_integration.py`

**Files:**
- Migrate: `tests/test_pipeline/test_logging_observer_integration.py`

9 skipped classes covering `LoggingObserver` integration: stdout/stderr capture, traceback on failure, flat log storage, queryable tag columns, async orchestrator logs, fail-fast, mixed success/failure, multi-node combined logs, per-node log query.

- [ ] **Step 1: Update imports**

  Find:
  ```python
  from orcapod.pipeline import (
      AsyncPipelineOrchestrator, Pipeline, SyncPipelineOrchestrator,
  )
  ```
  Replace with:
  ```python
  from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator
  from orcapod.pipeline.job import PipelineJob
  ```

- [ ] **Step 2: Remove `@pytest.mark.skip` from all 9 classes**

  Delete each:
  ```python
  @pytest.mark.skip(reason="Migrating to PipelineJob-based API — pending migration task")
  ```

- [ ] **Step 3: Update fixtures and test bodies**

  **Sync orchestrator tests** (`TestSyncPipelineSuccessLogs`, `TestFailingDatasLogged`, `TestFlatLogStorage`, `TestQueryableTagColumns`, `TestFailFastErrorPolicy`, `TestMixedSuccessFailure`, `TestMultipleFunctionNodesCombinedLogs`, `TestGetLogsNodeSpecific`):

  ```python
  # Before
  pipeline = Pipeline(name="test_logs", pipeline_database=db)
  with pipeline:
      pod(source, label="doubler")
  obs = LoggingObserver(log_database=db)
  SyncPipelineOrchestrator().run(pipeline.dag, observer=obs)
  logs = obs.get_logs()

  # After
  job = PipelineJob(name="test_logs", store=db)
  with job:
      pod(source, label="doubler")
  obs = LoggingObserver(log_database=db)
  job.run(observer=obs)
  logs = obs.get_logs()
  ```

  **Async orchestrator test** (`TestAsyncOrchestratorLogs`):

  ```python
  # Before
  AsyncPipelineOrchestrator().run(pipeline.dag, observer=obs)

  # After
  job.run(orchestrator=AsyncPipelineOrchestrator(), observer=obs)
  ```

- [ ] **Step 4: Run the migrated tests**

  ```bash
  uv run pytest tests/test_pipeline/test_logging_observer_integration.py -v 2>&1 | tail -30
  ```
  Expected: all 9 previously-skipped classes pass. Fix any failures.

- [ ] **Step 5: Commit**

  ```bash
  git add tests/test_pipeline/test_logging_observer_integration.py
  git commit -m "test(logging_observer): migrate skipped tests to PipelineJob API

  Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
  ```

---

## Task 7: Migrate `test_orchestrator.py` (Async Orchestrator)

**Files:**
- Migrate: `tests/test_pipeline/test_orchestrator.py`

8 skipped classes testing `AsyncPipelineOrchestrator`: linear, operator, diamond DAG, run_async from event loop, buffer size, fan-out, error propagation, observer injection.

The special case is `TestOrchestratorRunAsync` which used `await orchestrator.run_async(pipeline.dag)`. In the new API, `job.run(orchestrator=AsyncPipelineOrchestrator())` calls the sync entry point which internally drives the event loop. Migrate `TestOrchestratorRunAsync` to use `job.run(orchestrator=AsyncPipelineOrchestrator())` — the async execution path is still exercised. If the test specifically needs to call `run_async()` from within a running event loop, retain the `@pytest.mark.asyncio` decorator and call `await AsyncPipelineOrchestrator().run_async(exec_dag)` after building the dag via `job.pipeline.dag` as a fallback — but prefer `job.run()` first.

- [ ] **Step 1: Update imports**

  Find:
  ```python
  from orcapod.pipeline import AsyncPipelineOrchestrator, Pipeline
  ```
  Replace with:
  ```python
  from orcapod.pipeline import AsyncPipelineOrchestrator
  from orcapod.pipeline.job import PipelineJob
  ```

- [ ] **Step 2: Remove `@pytest.mark.skip` from all 8 classes**

  Delete each:
  ```python
  @pytest.mark.skip(reason="Migrating to PipelineJob-based API — pending migration task")
  ```

- [ ] **Step 3: Update fixtures and test bodies**

  **Standard async orchestrator tests** (`TestOrchestratorLinearPipeline`, `TestOrchestratorOperatorPipeline`, `TestOrchestratorDiamondDag`, `TestBufferSizeConfiguration`, `TestAsyncOrchestratorFanOut`, `TestAsyncOrchestratorErrorPropagation`, `TestAsyncOrchestratorObserverInjection`):

  ```python
  # Before
  pipeline = Pipeline(name="linear", pipeline_database=InMemoryArrowDatabase())
  with pipeline:
      pod(src, label="doubler")
  pipeline.compile()
  AsyncPipelineOrchestrator().run(pipeline.dag)
  pipeline.flush()
  records = pipeline.doubler.get_all_records()

  # After
  job = PipelineJob(name="linear", store=InMemoryArrowDatabase())
  with job:
      pod(src, label="doubler")
  job.run(orchestrator=AsyncPipelineOrchestrator())
  records = job.nodes["doubler"].get_all_records()
  ```

  **With custom buffer size:**
  ```python
  # Before
  AsyncPipelineOrchestrator(buffer_size=4).run(pipeline.dag)

  # After
  job.run(orchestrator=AsyncPipelineOrchestrator(buffer_size=4))
  ```

  **With observer:**
  ```python
  # Before
  AsyncPipelineOrchestrator().run(pipeline.dag, observer=recording_obs)

  # After
  job.run(orchestrator=AsyncPipelineOrchestrator(), observer=recording_obs)
  ```

  **`TestOrchestratorRunAsync`** — keep `@pytest.mark.asyncio`, migrate to use `job.run()`:
  ```python
  # Before
  async def test_run_async_from_event_loop(self):
      pipeline = Pipeline(name="async_loop", pipeline_database=db)
      with pipeline:
          pod(src, label="doubler")
      pipeline.compile()
      await AsyncPipelineOrchestrator().run_async(pipeline.dag)
      pipeline.flush()

  # After
  async def test_run_async_from_event_loop(self):
      # PipelineJob.run() drives the event loop synchronously.
      # Test that async execution produces correct results.
      job = PipelineJob(name="async_loop", store=db)
      with job:
          pod(src, label="doubler")
      job.run(orchestrator=AsyncPipelineOrchestrator())
      records = job.nodes["doubler"].get_all_records()
      assert len(records) > 0
  ```

- [ ] **Step 4: Run the migrated tests**

  ```bash
  uv run pytest tests/test_pipeline/test_orchestrator.py -v 2>&1 | tail -30
  ```
  Expected: all 8 previously-skipped classes pass. Fix any failures.

- [ ] **Step 5: Commit**

  ```bash
  git add tests/test_pipeline/test_orchestrator.py
  git commit -m "test(orchestrator): migrate async orchestrator tests to PipelineJob API

  Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
  ```

---

## Task 8: Migrate `test_graph_rendering.py`

**Files:**
- Migrate: `tests/test_pipeline/test_graph_rendering.py`

8 skipped classes testing `GraphRenderer`, node attributes, HTML labels, convenience functions, `Pipeline.show_graph()`, full render paths.

Key pattern difference: the old tests used `compiled_pipeline.dag` (a `Pipeline` fixture). In the new API, `job.pipeline.dag` is available after the `with` block exits — no `run()` call needed.

- [ ] **Step 1: Update imports**

  Find:
  ```python
  from orcapod.pipeline import Pipeline
  ```
  Replace with:
  ```python
  from orcapod.pipeline.job import PipelineJob
  ```

- [ ] **Step 2: Remove `@pytest.mark.skip` from all 8 classes**

  Delete each:
  ```python
  @pytest.mark.skip(reason="Migrating to PipelineJob-based API — pending migration task")
  ```

- [ ] **Step 3: Update shared fixtures**

  Find the `compiled_pipeline` and `node_graph` fixtures. They likely look like:
  ```python
  @pytest.fixture
  def compiled_pipeline(pipeline_db):
      pipeline = Pipeline(name="render_test", pipeline_database=pipeline_db)
      with pipeline:
          pod(source, label="node")
      return pipeline

  @pytest.fixture
  def node_graph(compiled_pipeline):
      return compiled_pipeline.dag
  ```

  Replace with:
  ```python
  @pytest.fixture
  def compiled_job(pipeline_db):
      job = PipelineJob(name="render_test", store=pipeline_db)
      with job:
          pod(source, label="node")
      return job

  @pytest.fixture
  def node_graph(compiled_job):
      return compiled_job.pipeline.dag
  ```

  Update all test methods that accept `compiled_pipeline` as a parameter to accept `compiled_job` instead, and replace `compiled_pipeline.dag` → `compiled_job.pipeline.dag` and `compiled_pipeline.show_graph()` → `compiled_job.pipeline.show_graph()`.

- [ ] **Step 4: Update `TestPipelineShowGraph` tests**

  ```python
  # Before
  def test_show_graph_raises_before_compile(self):
      pipeline = Pipeline(name="no_compile", pipeline_database=db)
      with pytest.raises(RuntimeError):
          pipeline.show_graph()

  def test_show_graph_returns_dot(self, compiled_pipeline):
      dot = compiled_pipeline.show_graph()
      assert "digraph" in dot

  # After
  def test_show_graph_raises_before_compile(self):
      job = PipelineJob(name="no_compile", store=db)
      with pytest.raises(RuntimeError):
          job.pipeline.show_graph()  # raises because job not compiled yet

  def test_show_graph_returns_dot(self, compiled_job):
      dot = compiled_job.pipeline.show_graph()
      assert "digraph" in dot
  ```

- [ ] **Step 5: Run the migrated tests**

  ```bash
  uv run pytest tests/test_pipeline/test_graph_rendering.py -v 2>&1 | tail -30
  ```
  Expected: all 8 previously-skipped classes pass. Fix any failures.

- [ ] **Step 6: Commit**

  ```bash
  git add tests/test_pipeline/test_graph_rendering.py
  git commit -m "test(graph_rendering): migrate skipped tests to PipelineJob API

  Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
  ```

---

## Task 9: Migrate `test_orchestrator_executor_matrix.py`

**Files:**
- Migrate: `tests/test_pipeline/test_orchestrator_executor_matrix.py`

7 skipped classes testing all combinations of orchestrator × function type. The two already-passing classes (`TestSyncOrchestratorSyncFunctionPipelineJob`) use PipelineJob already — keep them as-is. Also update any remaining `OrchestratorType` / `PipelineConfig.orchestrator` references from Task 1 (should already be done).

- [ ] **Step 1: Confirm `ExecutorType` rename is complete in this file**

  ```bash
  grep -n "ExecutorType" tests/test_pipeline/test_orchestrator_executor_matrix.py
  ```
  Expected: no output (renamed in Task 1).

- [ ] **Step 2: Remove `@pytest.mark.skip` from all 7 classes**

  Delete each:
  ```python
  @pytest.mark.skip(reason="Migrating to PipelineJob-based API — see ENG-491")
  ```

- [ ] **Step 3: Update imports**

  Find any remaining `Pipeline` imports:
  ```python
  from orcapod.pipeline import AsyncPipelineOrchestrator, Pipeline
  from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator
  ```
  Replace with:
  ```python
  from orcapod.pipeline import AsyncPipelineOrchestrator
  from orcapod.pipeline.job import PipelineJob
  from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator
  ```

- [ ] **Step 4: Replace all `PipelineConfig`-based dispatch with direct orchestrator instances**

  The matrix tests used `PipelineConfig(executor=...)` to select orchestrators. Replace all occurrences:

  ```python
  # Before (sync orchestrator cell)
  pipeline = Pipeline(name="matrix", pipeline_database=db, auto_compile=True)
  with pipeline:
      pod(source, label="node")
  pipeline.run(config=PipelineConfig(orchestrator=OrchestratorType.SYNCHRONOUS))
  records = pipeline.node.get_all_records()

  # After
  job = PipelineJob(name="matrix", store=db)
  with job:
      pod(source, label="node")
  job.run(orchestrator=SyncPipelineOrchestrator())
  records = job.nodes["node"].get_all_records()
  ```

  ```python
  # Before (async orchestrator cell)
  pipeline.run(config=PipelineConfig(orchestrator=OrchestratorType.ASYNC_CHANNELS))

  # After
  job.run(orchestrator=AsyncPipelineOrchestrator())
  ```

  For the concurrency/performance tests (`TestConcurrencyBenefitAcrossMatrix`, `TestAsyncAsyncVsAsyncSync`), update timing assertions if needed — `job.run(orchestrator=AsyncPipelineOrchestrator())` replaces the async channel path.

- [ ] **Step 5: Remove `PipelineConfig` import if no longer used**

  ```bash
  grep -n "PipelineConfig" tests/test_pipeline/test_orchestrator_executor_matrix.py
  ```
  If no usages remain, remove it from the import line.

- [ ] **Step 6: Run the migrated tests**

  ```bash
  uv run pytest tests/test_pipeline/test_orchestrator_executor_matrix.py -v 2>&1 | tail -30
  ```
  Expected: all 7 previously-skipped classes pass. Fix any failures.

- [ ] **Step 7: Commit**

  ```bash
  git add tests/test_pipeline/test_orchestrator_executor_matrix.py
  git commit -m "test(executor_matrix): migrate skipped tests to PipelineJob API

  Replace PipelineConfig-based orchestrator dispatch with direct orchestrator
  instances. All four execution matrix cells now use PipelineJob.run().

  Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
  ```

---

## Task 10: Final Verification and Skip-Reason Audit

**Files:**
- Verify: `tests/test_pipeline/test_serialization_helpers.py`
- Verify: `tests/test_pipeline/test_integration_smoke.py`
- Audit: all `tests/test_pipeline/` files

- [ ] **Step 1: Confirm Group E files have no skipped tests**

  ```bash
  grep -n "Migrating to PipelineJob-based API" \
    tests/test_pipeline/test_serialization_helpers.py \
    tests/test_pipeline/test_integration_smoke.py
  ```
  Expected: no output (pre-implementation exploration confirmed these files have no remaining migration skips). If any skips appear, apply the same PipelineJob migration pattern used in Tasks 3–9.

- [ ] **Step 2: Audit all pipeline test files for remaining skip reasons**

  ```bash
  grep -rn "Migrating to PipelineJob-based API" tests/test_pipeline/
  ```
  Expected: no output. All migration skip reasons must be gone.

- [ ] **Step 3: Run the full pipeline test suite**

  ```bash
  uv run pytest tests/test_pipeline/ -v 2>&1 | tail -40
  ```
  Expected: all previously-skipped tests now pass. Zero tests with the migration skip reason.

- [ ] **Step 4: Run the full test suite**

  ```bash
  uv run pytest tests/ -q 2>&1 | tail -20
  ```
  Expected: full suite passes. Note total pass count.

- [ ] **Step 5: Commit if any Group E changes were needed**

  ```bash
  git add tests/test_pipeline/test_serialization_helpers.py tests/test_pipeline/test_integration_smoke.py
  git commit -m "test(pipeline): confirm Group E files fully migrated

  Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
  ```
  (Only commit if files were actually changed.)

- [ ] **Step 6: Push the branch**

  ```bash
  git push -u origin eywalker/eng-491-migrate-remaining-skipped-pipeline-tests-to-pipelinejob-api
  ```
