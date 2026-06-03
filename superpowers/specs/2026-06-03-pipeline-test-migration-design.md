# Pipeline Test Migration to PipelineJob API

**Date:** 2026-06-03
**Issue:** [ENG-491](https://linear.app/enigma-metamorphic/issue/ENG-491)
**Status:** Approved

## Overview

After the ENG-456 Pipeline/PipelineJob refactor (PR #138), approximately 107 tests in
`tests/test_pipeline/` remain skipped with reason `"Migrating to PipelineJob-based API —
pending migration task"`. This spec describes the changes needed to migrate all of them
to the new `PipelineJob` API and restore regression coverage.

## Goals & Success Criteria

- All skipped tests in `tests/test_pipeline/` are migrated and passing, or explicitly
  deleted with documented rationale
- Zero tests carry the `"Migrating to PipelineJob-based API"` skip reason after this PR
- Full test suite continues to pass
- `PipelineJob.run()` gains an `orchestrator=` parameter so async orchestrator paths
  remain testable
- `ExecutorType` is renamed to `OrchestratorType` to eliminate naming confusion with
  `DataFunction.executor` (distributed execution)

## Background: API Differences

| Aspect | Old `Pipeline` API | New `PipelineJob` API |
|---|---|---|
| Construction | `Pipeline(name, pipeline_database=db)` | `PipelineJob(name, store=db)` |
| Execution | `pipeline.run(orchestrator=orch, observer=obs)` | `job.run(orchestrator=orch, observer=obs)` |
| Node access | `pipeline.label.get_all_records()` | `job.pipeline.nodes["label"].get_all_records()` |
| DAG access | `pipeline.dag` | `job.pipeline.dag` (available post-`with`, no `run()` needed) |
| Return value | `None` | `self` (same instance; see ENG-565 for follow-up) |

## Naming Confusion: ExecutorType vs Executor

Two distinct "executor" concepts exist in the codebase:

| Symbol | Level | Meaning |
|---|---|---|
| `ExecutorType` (old) / `OrchestratorType` (new) | Pipeline | Which orchestrator strategy to use: sync DAG walk vs async channels |
| `DataFunction.executor` | Data function | Optional distributed executor (e.g. Ray) for a single function |

These are orthogonal. Renaming `ExecutorType` → `OrchestratorType` removes the
ambiguity. `PipelineConfig.executor` is renamed to `PipelineConfig.orchestrator`
accordingly.

A follow-up issue (ENG-566) tracks whether `OrchestratorType` / `ExecutionContext`
config-based dispatch should be kept or replaced entirely by the `orchestrator=`
instance parameter.

## Production Code Changes

### 1. `src/orcapod/types.py`

Rename `ExecutorType` → `OrchestratorType` and update `PipelineConfig`:

```python
# Before
class ExecutorType(str, Enum):
    SYNCHRONOUS = "synchronous"
    ASYNC_CHANNELS = "async_channels"

class PipelineConfig(BaseModel):
    executor: ExecutorType = ExecutorType.SYNCHRONOUS

# After
class OrchestratorType(str, Enum):
    SYNCHRONOUS = "synchronous"
    ASYNC_CHANNELS = "async_channels"

class PipelineConfig(BaseModel):
    orchestrator: OrchestratorType = OrchestratorType.SYNCHRONOUS
```

Update all import sites and usages across the codebase.

### 2. `src/orcapod/pipeline/job.py`

Add `orchestrator=` parameter to `PipelineJob.run()`:

```python
# Before
def run(
    self,
    observer: "ExecutionObserverProtocol | None" = None,
) -> "PipelineJob":
    ...
    SyncPipelineOrchestrator().run(exec_dag, observer=effective_observer, ...)

# After
def run(
    self,
    orchestrator: "PipelineOrchestratorProtocol | None" = None,
    observer: "ExecutionObserverProtocol | None" = None,
) -> "PipelineJob":
    ...
    effective_orchestrator = orchestrator or SyncPipelineOrchestrator()
    effective_orchestrator.run(exec_dag, observer=effective_observer, ...)
```

Default behavior is unchanged — callers that pass no arguments continue to work.

Note: `run()` continues to return `self`. ENG-565 tracks the follow-up design decision
on whether this should change.

## Test Migration Patterns

### Group A — Sync orchestrator (direct `job.run()`)

Files: `test_sync_orchestrator.py`, `test_status_observer_integration.py` (sync
classes), `test_logging_observer_integration.py` (sync classes),
`test_composite_observer.py`, `test_integration_smoke.py`

```python
# Before
pipeline = Pipeline(name="test", pipeline_database=db)
with pipeline:
    pod(source, label="doubler")
pipeline.run(orchestrator=SyncPipelineOrchestrator(), observer=obs)
records = pipeline.doubler.get_all_records()

# After
job = PipelineJob(name="test", store=db)
with job:
    pod(source, label="doubler")
job.run(observer=obs)  # orchestrator defaults to SyncPipelineOrchestrator
records = job.pipeline.nodes["doubler"].get_all_records()
```

### Group B — Async orchestrator

Files: `test_orchestrator.py`, `test_logging_observer_integration.py` (async classes),
`test_status_observer_integration.py` (async classes)

```python
# Before
AsyncPipelineOrchestrator().run(pipeline.dag)

# After
job.run(orchestrator=AsyncPipelineOrchestrator(), observer=obs)
```

### Group C — Orchestrator/executor matrix (`test_orchestrator_executor_matrix.py`)

The two skipped cells (sync+sync and async+async) are migrated. The two already-passing
cells (sync+async, async+sync) have their `ExecutorType` references updated to
`OrchestratorType`. Config-based dispatch (`PipelineConfig(executor=...)`) is replaced
by direct orchestrator instances in all four cells.

```python
# Before (skipped cells 1 and 4)
pipeline.run(config=PipelineConfig(executor=ExecutorType.SYNCHRONOUS))
pipeline.run(config=PipelineConfig(executor=ExecutorType.ASYNC_CHANNELS))

# After
job.run(orchestrator=SyncPipelineOrchestrator())
job.run(orchestrator=AsyncPipelineOrchestrator())
```

### Group D — Graph rendering (`test_graph_rendering.py`)

`job.pipeline` is available immediately after the `with` block exits (compile runs on
`__exit__`; `pipeline` is lazily computed via `as_pipeline()` on first access). No
`run()` call is needed for rendering tests.

```python
# Before
pipeline = Pipeline(name="test", pipeline_database=db)
with pipeline:
    pod(source)
render_graph(pipeline.dag)
pipeline.show_graph()

# After
job = PipelineJob(name="test", store=db)
with job:
    pod(source)
render_graph(job.pipeline.dag)
job.pipeline.show_graph()
```

### Group E — Serialization helpers and integration smoke

`test_serialization_helpers.py` and `test_integration_smoke.py` show zero skipped tests
in the current codebase per pre-implementation exploration. During implementation, these
files will be read in full to confirm whether any migration work remains. If no skipped
tests are found, they will be noted as already complete.

## Implementation Order

1. **Types rename** — `ExecutorType` → `OrchestratorType` and all call sites; keeps
   the codebase internally consistent before other changes
2. **`PipelineJob.run()` orchestrator param** — add the parameter; existing tests must
   still pass (default is unchanged)
3. **Group A — sync orchestrator tests** — lowest risk; confirms the basic
   `job.run(observer=obs)` pattern end-to-end
4. **Group A continued — observer integration** — status, logging, composite observer
   sync classes
5. **Group B — async orchestrator tests** — exercises the new `orchestrator=` parameter
6. **Group D — graph rendering tests** — uses `job.pipeline.dag` without `run()`
7. **Group C — executor matrix tests** — all four cells with renamed types and new
   orchestrator param
8. **Group E — assess and close** — serialization helpers and integration smoke

## Follow-up Issues

| Issue | Description |
|---|---|
| [ENG-565](https://linear.app/enigma-metamorphic/issue/ENG-565) | Reconsider `PipelineJob.run()` return type (`return self` vs `None` vs `CompletedPipelineJob`) |
| [ENG-566](https://linear.app/enigma-metamorphic/issue/ENG-566) | Reconsider whether `OrchestratorType` / `ExecutionContext` config-based dispatch is the right long-term design |
