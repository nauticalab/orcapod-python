# ctx_arg + Ray empty-opts hardening implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix two failing Ray executor tests, add full test coverage for `@function_pod(ctx_arg=...)` as a first-class path (10 scenarios including cached pod wrapping), and update the `side_effect_function_pod` docstring to point to the preferred path.

**Architecture:** All implementation already exists; this plan is test + doc work. The Ray fix updates two test mocks to handle a dual calling convention. The ctx_arg tests are written to the new file `tests/test_core/function_pod/test_function_pod_ctx_arg.py`, mirroring the structure of `tests/test_core/side_effect_function/test_side_effect_function_pod.py`. The docstring update touches only `src/orcapod/core/function_pod.py`.

**Tech Stack:** Python, pytest (`uv run pytest`), PyArrow, orcapod internal APIs.

---

## File Map

| Action | Path | Responsibility |
|--------|------|----------------|
| Modify | `tests/test_core/test_regression_fixes.py` | Fix two Ray mocks — dual calling convention |
| Modify | `src/orcapod/core/function_pod.py` | Update `side_effect_function_pod` docstring |
| Create | `tests/test_core/function_pod/test_function_pod_ctx_arg.py` | 10 ctx_arg test scenarios |

---

## Task 1: Fix `test_get_remote_fn_caches_per_function_name`

**Files:**
- Modify: `tests/test_core/test_regression_fixes.py` (around line 591)

**Context:** The guard in `RayExecutor._get_remote_fn()` calls `ray.remote(wrapper)` directly
when `opts` is empty. The current mock sets `mock_ray.remote.return_value = lambda wrapper: ...`,
which means `mock_ray.remote(wrapper)` returns the lambda itself — not `lambda(wrapper)`.
Replace the `return_value` assignment with a function that handles both call patterns.

- [ ] **Step 1: Read the test to confirm the exact lines to change**

  Open `tests/test_core/test_regression_fixes.py` lines 591–616. Locate:
  ```python
  mock_ray.remote.return_value = lambda wrapper: MagicMock(name=f"remote_{wrapper.__name__}")
  ```

- [ ] **Step 2: Replace the mock with a dual-pattern fake**

  Replace those lines so the full test reads:
  ```python
  def test_get_remote_fn_caches_per_function_name(self):
      """_get_remote_fn must return distinct remote wrappers for different
      function names so Ray metrics report the correct name."""
      from unittest.mock import MagicMock, patch

      from orcapod.core.executors.ray import RayExecutor

      mock_ray = MagicMock()
      mock_ray.is_initialized.return_value = True

      def fake_remote(fn=None, **opts):
          # Called as ray.remote(fn) when opts is empty (direct path)
          if fn is not None:
              return MagicMock(name=f"remote_{fn.__name__}")
          # Called as ray.remote(**opts) when opts is non-empty (factory path)
          return lambda wrapper: MagicMock(name=f"remote_{wrapper.__name__}")

      mock_ray.remote = fake_remote

      with patch.dict("sys.modules", {"ray": mock_ray}):
          executor = RayExecutor.__new__(RayExecutor)
          executor._remote_opts = {}
          executor._remote_fn_cache = {}
          executor._remote_fn_cache_lock = __import__("threading").Lock()

          fn_a = executor._get_remote_fn(mock_ray, "transform_a")
          fn_b = executor._get_remote_fn(mock_ray, "transform_b")
          fn_a_again = executor._get_remote_fn(mock_ray, "transform_a")

          # Different names → different remote fns
          assert fn_a is not fn_b
          # Same name → cached (same object)
          assert fn_a is fn_a_again
  ```

- [ ] **Step 3: Run this test alone to verify it passes**

  ```bash
  uv run pytest tests/test_core/test_regression_fixes.py::TestRayExecutorRegressions::test_get_remote_fn_caches_per_function_name -v
  ```
  Expected: `PASSED`

---

## Task 2: Fix `test_get_remote_fn_sets_wrapper_name`

**Files:**
- Modify: `tests/test_core/test_regression_fixes.py` (around line 618)

**Context:** Same root cause as Task 1. `fake_remote` only accepts `**opts`; when the guard
calls `ray.remote(wrapper)` directly (positional argument), Python raises `TypeError:
fake_remote() takes 0 positional arguments but 1 was given`.

- [ ] **Step 1: Locate the `fake_remote` definition** (lines 627–631):
  ```python
  def fake_remote(**opts):
      def decorator(wrapper):
          captured_wrappers.append(wrapper)
          return MagicMock()
      return decorator
  ```

- [ ] **Step 2: Replace with a dual-pattern fake**

  Replace the full test with:
  ```python
  def test_get_remote_fn_sets_wrapper_name(self):
      """The capture wrapper created by _get_remote_fn should carry the
      original function name so Ray uses it in metrics labels."""
      from unittest.mock import MagicMock, patch

      from orcapod.core.executors.ray import RayExecutor

      captured_wrappers = []

      def fake_remote(fn=None, **opts):
          # Called as ray.remote(fn) when opts is empty (direct path)
          if fn is not None:
              captured_wrappers.append(fn)
              return MagicMock()
          # Called as ray.remote(**opts) when opts is non-empty (factory path)
          def decorator(wrapper):
              captured_wrappers.append(wrapper)
              return MagicMock()
          return decorator

      mock_ray = MagicMock()
      mock_ray.is_initialized.return_value = True
      mock_ray.remote = fake_remote

      with patch.dict("sys.modules", {"ray": mock_ray}):
          executor = RayExecutor.__new__(RayExecutor)
          executor._remote_opts = {}
          executor._remote_fn_cache = {}
          executor._remote_fn_cache_lock = __import__("threading").Lock()

          executor._get_remote_fn(mock_ray, "compute_features")

          assert len(captured_wrappers) == 1
          assert captured_wrappers[0].__name__ == "compute_features"
  ```

- [ ] **Step 3: Run both Ray tests together**

  ```bash
  uv run pytest tests/test_core/test_regression_fixes.py::TestRayExecutorRegressions::test_get_remote_fn_caches_per_function_name tests/test_core/test_regression_fixes.py::TestRayExecutorRegressions::test_get_remote_fn_sets_wrapper_name -v
  ```
  Expected: both `PASSED`

- [ ] **Step 4: Run the full regression test suite to confirm no regressions**

  ```bash
  uv run pytest tests/test_core/test_regression_fixes.py -v
  ```
  Expected: all pass

- [ ] **Step 5: Commit**

  ```bash
  git add tests/test_core/test_regression_fixes.py
  git commit -m "test(ray): fix two Ray executor mocks to handle empty-opts direct call path (ITL-544)"
  ```

---

## Task 3: Update `side_effect_function_pod` docstring

**Files:**
- Modify: `src/orcapod/core/function_pod.py` (around line 1220)

**Context:** The `side_effect_function_pod` decorator is superseded by `@function_pod(ctx_arg=...)`.
Update its docstring to note this with a before/after example. No runtime warning — this is
a greenfield pre-v0.1.0 project.

- [ ] **Step 1: Locate the current docstring** (lines 1228–1252):

  The current docstring starts:
  ```
  """Decorator wrapping a callable as a ctx-aware ``FunctionPod``.

  Equivalent to ``FunctionPod.from_fn(fn, output_keys=..., ctx_arg_name=...)``.
  ```

- [ ] **Step 2: Replace the docstring**

  Replace the docstring so it reads (keep all Args/Returns/Raises intact, add the note at the top):
  ```python
  """Decorator wrapping a callable as a ctx-aware ``FunctionPod``.

  .. note::
      This decorator is superseded by ``@function_pod(ctx_arg=<arg_name>)``,
      which is now the preferred way to author side-effect pods. The two
      forms are equivalent in behaviour.

      Preferred form::

          @function_pod(output_keys=["result"], ctx_arg="ctx")
          def my_fn(value: int, ctx: InvocationContext) -> str:
              ...

          assert isinstance(my_fn.pod, FunctionPod)

      Legacy form (this decorator)::

          @side_effect_function_pod(output_keys=["result"])
          def my_fn(value: int, ctx: InvocationContext) -> str:
              ...

          assert isinstance(my_fn, FunctionPod)

      Full removal of this decorator is tracked in a follow-up issue.

  Equivalent to ``FunctionPod.from_fn(fn, output_keys=..., ctx_arg_name=...)``.
  The decorated object is the ``FunctionPod`` itself (not a wrapper function),
  so it can be called directly as a pod.

  Args:
      fn: Optional function — if provided, decorates immediately.
      output_keys: Output column key(s).
      ctx_arg_name: Name of the ``InvocationContext`` parameter (default ``"ctx"``).
      name: Optional canonical function name override.
      version: Version string for the data function (default ``"v1.0"``).
      pod_config: Optional per-pod configuration.

  Returns:
      A ``FunctionPod`` with ``ctx_arg_name`` set, or a decorator if ``fn``
      is not provided.

  Raises:
      ValueError: If ``ctx_arg_name`` is not in ``fn``'s signature.
  """
  ```

  Note: Google-style docstrings don't use `.. note::` RST directives — use a plain
  `Note:` section instead. The final docstring should be:
  ```python
  """Decorator wrapping a callable as a ctx-aware ``FunctionPod``.

  Note:
      This decorator is superseded by ``@function_pod(ctx_arg=<arg_name>)``,
      which is now the preferred way to author side-effect pods. The two
      forms are equivalent in behaviour.

      Preferred form (use this instead)::

          @function_pod(output_keys=["result"], ctx_arg="ctx")
          def my_fn(value: int, ctx: InvocationContext) -> str:
              ...

          assert isinstance(my_fn.pod, FunctionPod)

      Legacy form (this decorator)::

          @side_effect_function_pod(output_keys=["result"])
          def my_fn(value: int, ctx: InvocationContext) -> str:
              ...

          assert isinstance(my_fn, FunctionPod)

      Full removal of this decorator is tracked separately.

  Equivalent to ``FunctionPod.from_fn(fn, output_keys=..., ctx_arg_name=...)``.
  The decorated object is the ``FunctionPod`` itself (not a wrapper function),
  so it can be called directly as a pod.

  Args:
      fn: Optional function — if provided, decorates immediately.
      output_keys: Output column key(s).
      ctx_arg_name: Name of the ``InvocationContext`` parameter (default ``"ctx"``).
      name: Optional canonical function name override.
      version: Version string for the data function (default ``"v1.0"``).
      pod_config: Optional per-pod configuration.

  Returns:
      A ``FunctionPod`` with ``ctx_arg_name`` set, or a decorator if ``fn``
      is not provided.

  Raises:
      ValueError: If ``ctx_arg_name`` is not in ``fn``'s signature.
  """
  ```

- [ ] **Step 3: Run the existing side_effect tests to confirm nothing broke**

  ```bash
  uv run pytest tests/test_core/side_effect_function/ -v
  ```
  Expected: all pass (no code changed, only docstring)

- [ ] **Step 4: Commit**

  ```bash
  git add src/orcapod/core/function_pod.py
  git commit -m "docs(function-pod): mark side_effect_function_pod as superseded by ctx_arg path (ITL-544)"
  ```

---

## Task 4: Create ctx_arg test file — schema scenarios (1–2)

**Files:**
- Create: `tests/test_core/function_pod/test_function_pod_ctx_arg.py`

**Context:** The `@function_pod(ctx_arg=...)` decorator (already implemented) wraps the
function in a `FunctionPod` with `ctx_arg_name` set. The pod filters `ctx` from its exposed
`input_data_schema` while the underlying `_data_function.input_data_schema` retains it for
hashing. These tests verify that schema contract via the decorator entry point.

- [ ] **Step 1: Create the test file with helpers and the first two tests**

  Create `tests/test_core/function_pod/test_function_pod_ctx_arg.py`:

  ```python
  # tests/test_core/function_pod/test_function_pod_ctx_arg.py
  """Tests for the ctx_arg parameter on the @function_pod decorator.

  Full parity with tests/test_core/side_effect_function/test_side_effect_function_pod.py,
  exercising @function_pod(ctx_arg=...) as the preferred side-effect pod entry point.
  """
  from __future__ import annotations

  import pytest
  import pyarrow as pa

  from orcapod.core.streams import ArrowTableStream
  from orcapod.side_effects import InvocationContext


  def _make_stream(n: int = 3) -> ArrowTableStream:
      """Simple stream: tag=id (int), data=value (int)."""
      schema = pa.schema([
          pa.field("id", pa.int64(), nullable=False),
          pa.field("value", pa.int64(), nullable=False),
      ])
      table = pa.table(
          {"id": list(range(n)), "value": list(range(n))},
          schema=schema,
      )
      return ArrowTableStream(table, tag_columns=["id"])


  def _make_pipeline_db():
      """Return a fresh in-memory ArrowDatabase."""
      from orcapod.databases.in_memory_databases import InMemoryArrowDatabase
      return InMemoryArrowDatabase()


  class TestCtxArgSchema:
      """Scenarios 1–2: schema inference and ctx stripping via @function_pod(ctx_arg=...)."""

      def test_ctx01_ctx_stripped_from_input_schema(self):
          """Scenario 1: ctx param absent from pod.input_data_schema; present in data function."""
          from orcapod.core.function_pod import function_pod

          @function_pod(ctx_arg="ctx")
          def my_fn(value: int, ctx: InvocationContext) -> str:
              return f"result_{value}"

          pod = my_fn.pod

          # Pod's exposed input schema excludes 'ctx'
          assert "ctx" not in pod.input_data_schema
          assert "value" in pod.input_data_schema
          assert pod.input_data_schema["value"] == int

          # Underlying data function retains full schema (ctx included for hashing)
          assert "ctx" in pod._data_function.input_data_schema

          # Output schema has the inferred key (function return type → str)
          assert "result" in pod._data_function.output_data_schema
          assert pod._data_function.output_data_schema["result"] == str

      def test_ctx02_ctx_arg_name_property(self):
          """Scenario 2: pod.ctx_arg_name reflects the configured arg name."""
          from orcapod.core.function_pod import function_pod

          @function_pod(ctx_arg="invocation_ctx")
          def my_fn(value: int, invocation_ctx: InvocationContext) -> str:
              return f"r_{value}"

          pod = my_fn.pod
          assert pod.ctx_arg_name == "invocation_ctx"
          assert "invocation_ctx" not in pod.input_data_schema
          assert "value" in pod.input_data_schema
  ```

- [ ] **Step 2: Run these two tests**

  ```bash
  uv run pytest tests/test_core/function_pod/test_function_pod_ctx_arg.py::TestCtxArgSchema -v
  ```
  Expected: both `PASSED`

---

## Task 5: Add execution scenarios (3–4) — sync and async standalone

**Files:**
- Modify: `tests/test_core/function_pod/test_function_pod_ctx_arg.py`

**Context:** Scenarios 3–4 verify that executing a `@function_pod(ctx_arg=...)` pod via
`pod.process(stream).iter_data()` correctly injects `InvocationContext` per row. Scenario 3
is synchronous, scenario 4 uses an async user function (the sync wrapper handles it).

- [ ] **Step 1: Append the execution test class to the file**

  ```python
  class TestCtxArgStandaloneExecution:
      """Scenarios 3–4: standalone execution via pod.process() / FunctionPodStream."""

      def test_ctx03_sync_execution_injects_ctx_per_row(self):
          """Scenario 3: ctx injected per row; output values correct; tags pass through."""
          from orcapod.core.function_pod import function_pod

          received_ctx: list[InvocationContext] = []

          @function_pod(ctx_arg="ctx")
          def my_fn(value: int, ctx: InvocationContext) -> str:
              received_ctx.append(ctx)
              return f"v{value}"

          pod = my_fn.pod
          stream = _make_stream(3)
          rows = list(pod.process(stream).iter_data())

          assert len(rows) == 3
          assert len(received_ctx) == 3

          # Each ctx is a proper InvocationContext
          for ctx in received_ctx:
              assert isinstance(ctx, InvocationContext)
              assert isinstance(ctx.invocation_hash, str)
              assert len(ctx.invocation_hash) > 0

          # Output values match expected
          for i, (tag, data) in enumerate(rows):
              assert data.as_dict()["result"] == f"v{i}"

          # Tags pass through unchanged
          assert rows[0][0].as_dict()["id"] == 0
          assert rows[2][0].as_dict()["id"] == 2

      def test_ctx04_async_fn_routed_through_sync_path(self):
          """Scenario 4: async user function runs correctly via synchronous wrapper."""
          import asyncio
          from orcapod.core.function_pod import function_pod

          @function_pod(ctx_arg="ctx")
          async def my_async_fn(value: int, ctx: InvocationContext) -> str:
              await asyncio.sleep(0)
              return f"async_{value}"

          pod = my_async_fn.pod
          stream = _make_stream(2)
          rows = list(pod.process(stream).iter_data())

          assert len(rows) == 2
          assert rows[0][1].as_dict()["result"] == "async_0"
          assert rows[1][1].as_dict()["result"] == "async_1"
  ```

- [ ] **Step 2: Run scenarios 1–4**

  ```bash
  uv run pytest tests/test_core/function_pod/test_function_pod_ctx_arg.py -v
  ```
  Expected: all 4 pass

---

## Task 6: Add DB-backed and pipeline scenarios (5–6)

**Files:**
- Modify: `tests/test_core/function_pod/test_function_pod_ctx_arg.py`

**Context:** Scenarios 5–6 verify that the `@function_pod(ctx_arg=...)` pod integrates with
DB-backed execution via `FunctionJobNode` and `PipelineJob`. These mirror SF-06 and SF-12
from the `side_effect_function_pod` suite.

- [ ] **Step 1: Append the DB-backed test class**

  ```python
  class TestCtxArgDBBacked:
      """Scenarios 5–6: DB-backed execution via FunctionJobNode and PipelineJob."""

      def test_ctx05_function_job_node_caches_output(self):
          """Scenario 5: second run uses cached result; fn not called again."""
          from orcapod.core.function_pod import function_pod
          from orcapod.core.nodes.function_node import FunctionJobNode

          call_count = 0

          @function_pod(ctx_arg="ctx")
          def my_fn(value: int, ctx: InvocationContext) -> str:
              nonlocal call_count
              call_count += 1
              return f"r{value}"

          pod = my_fn.pod
          stream = _make_stream(2)
          pipeline_db = _make_pipeline_db()

          node1 = FunctionJobNode(function_pod=pod, input_stream=stream)
          node1.attach_databases(pipeline_database=pipeline_db)
          results1 = node1.execute(stream)
          assert len(results1) == 2
          assert call_count == 2

          # Second run — same pod, same data, same DB — must NOT call fn again
          node2 = FunctionJobNode(function_pod=pod, input_stream=stream)
          node2.attach_databases(pipeline_database=pipeline_db)
          results2 = node2.execute(stream)
          assert len(results2) == 2
          assert call_count == 2  # NOT incremented

          # Both runs produce identical result values
          for (_, d1), (_, d2) in zip(results1, results2):
              assert d1.as_dict()["result"] == d2.as_dict()["result"]

      def test_ctx06_pipeline_job_integration(self):
          """Scenario 6: end-to-end PipelineJob compilation + execution with ctx_arg pod."""
          from orcapod.core.function_pod import function_pod
          from orcapod.pipeline.job import PipelineJob
          from orcapod.core.sources.dict_source import DictSource

          received_ctx: list[InvocationContext] = []

          @function_pod(ctx_arg="ctx")
          def transform(value: int, ctx: InvocationContext) -> str:
              received_ctx.append(ctx)
              return f"result_{value}"

          pod = transform.pod
          db = _make_pipeline_db()

          with PipelineJob(name="test_ctx_pod", store=db) as job:
              source = DictSource(
                  [{"id": 0, "value": 10}, {"id": 1, "value": 20}],
                  tag_columns=["id"],
              )
              pod.process(source)

          job.run()

          # fn called once per row, ctx injected each time
          assert len(received_ctx) == 2
          for ctx in received_ctx:
              assert isinstance(ctx, InvocationContext)
  ```

- [ ] **Step 2: Run scenarios 1–6**

  ```bash
  uv run pytest tests/test_core/function_pod/test_function_pod_ctx_arg.py -v
  ```
  Expected: all 6 pass

---

## Task 7: Add decorator and collision scenarios (7–9)

**Files:**
- Modify: `tests/test_core/function_pod/test_function_pod_ctx_arg.py`

**Context:** Scenarios 7–8 verify the two decorator forms (`ctx_arg` only, and `ctx_arg` with
explicit `output_keys`). Scenario 9 verifies that a `ctx_arg` name that collides with a data
column in the input stream raises `ValueError` at process time.

- [ ] **Step 1: Append the decorator and collision test classes**

  ```python
  class TestCtxArgDecoratorForms:
      """Scenarios 7–8: decorator usage with ctx_arg."""

      def test_ctx07_decorator_ctx_arg_only(self):
          """Scenario 7: @function_pod(ctx_arg='ctx') — output_keys inferred from return type."""
          from orcapod.core.function_pod import function_pod, FunctionPod

          @function_pod(ctx_arg="ctx")
          def my_fn(value: int, ctx: InvocationContext) -> str:
              return f"v{value}"

          # my_fn is still callable (wraps original fn)
          assert callable(my_fn)
          # .pod is attached
          pod = my_fn.pod
          assert isinstance(pod, FunctionPod)
          assert pod.ctx_arg_name == "ctx"
          # Output key inferred from return annotation
          assert "result" in pod._data_function.output_data_schema

      def test_ctx08_decorator_with_explicit_output_keys(self):
          """Scenario 8: @function_pod(output_keys=['out'], ctx_arg='ctx') — explicit output_keys."""
          from orcapod.core.function_pod import function_pod, FunctionPod

          @function_pod(output_keys=["out"], ctx_arg="ctx")
          def my_fn(value: int, ctx: InvocationContext) -> str:
              return f"v{value}"

          pod = my_fn.pod
          assert isinstance(pod, FunctionPod)
          assert pod.ctx_arg_name == "ctx"
          assert "out" in pod._data_function.output_data_schema
          assert "ctx" not in pod.input_data_schema


  class TestCtxArgCollision:
      """Scenario 9: ctx_arg names a param not in the function — ValueError at decoration time."""

      def test_ctx09_invalid_ctx_arg_raises_at_decoration(self):
          """Scenario 9: ValueError at decoration time if ctx_arg is not a parameter of the function.

          ``_FunctionPodBase.__init__`` raises immediately when ``ctx_arg_name`` is not
          in the underlying data function's input schema.
          """
          from orcapod.core.function_pod import function_pod

          with pytest.raises(ValueError, match="ctx_arg_name"):
              @function_pod(ctx_arg="nonexistent_param")
              def my_fn(value: int) -> str:
                  return str(value)
  ```

- [ ] **Step 2: Run scenarios 1–9**

  ```bash
  uv run pytest tests/test_core/function_pod/test_function_pod_ctx_arg.py -v
  ```
  Expected: all 9 pass

---

## Task 8: Add cached pod wrapping scenario (10)

**Files:**
- Modify: `tests/test_core/function_pod/test_function_pod_ctx_arg.py`

**Context:** Scenario 10 verifies that wrapping a `ctx_arg` pod in `CachedFunctionPod` (via
`pod_cache_database=`) does not break context injection. On a cache miss the inner pod's
`process_data` runs and injects `InvocationContext`. On a cache hit the cached output is
returned directly (fn not called). The wrapped pod's `input_data_schema` must still exclude ctx.

- [ ] **Step 1: Append the cached pod test class**

  ```python
  class TestCtxArgCachedPodWrapping:
      """Scenario 10: @function_pod(ctx_arg=..., pod_cache_database=...) — CachedFunctionPod wrapping."""

      def test_ctx10_cached_pod_injects_ctx_on_miss_skips_on_hit(self):
          """Scenario 10: CachedFunctionPod wrapping a ctx_arg pod:
          - ctx stripped from exposed schema
          - InvocationContext injected on cache miss
          - fn NOT called on cache hit; output same
          """
          from orcapod.core.function_pod import function_pod
          from orcapod.core.cached_function_pod import CachedFunctionPod

          received_ctx: list[InvocationContext] = []
          call_count = 0

          db = _make_pipeline_db()

          @function_pod(ctx_arg="ctx", pod_cache_database=db)
          def my_fn(value: int, ctx: InvocationContext) -> str:
              nonlocal call_count
              call_count += 1
              received_ctx.append(ctx)
              return f"v{value}"

          pod = my_fn.pod
          assert isinstance(pod, CachedFunctionPod)

          # Schema: ctx stripped from wrapped pod's exposed input schema
          assert "ctx" not in pod.input_data_schema
          assert "value" in pod.input_data_schema
          assert pod.ctx_arg_name == "ctx"

          stream = _make_stream(2)

          # First run: cache miss → fn called, InvocationContext injected
          rows1 = list(pod.process(stream).iter_data())
          assert call_count == 2
          assert len(received_ctx) == 2
          for ctx in received_ctx:
              assert isinstance(ctx, InvocationContext)
              assert len(ctx.invocation_hash) > 0

          # Second run: cache hit → fn NOT called
          rows2 = list(pod.process(stream).iter_data())
          assert call_count == 2  # unchanged — cache hit

          # Output values must match
          for (_, d1), (_, d2) in zip(rows1, rows2):
              assert d1.as_dict()["result"] == d2.as_dict()["result"]
  ```

- [ ] **Step 2: Run all 10 scenarios**

  ```bash
  uv run pytest tests/test_core/function_pod/test_function_pod_ctx_arg.py -v
  ```
  Expected: all 10 pass

- [ ] **Step 3: Run the full test suite to check for regressions**

  ```bash
  uv run pytest tests/ -v --tb=short -q
  ```
  Expected: all pass (or at minimum: no new failures beyond pre-existing ones)

- [ ] **Step 4: Commit the new test file**

  ```bash
  git add tests/test_core/function_pod/test_function_pod_ctx_arg.py
  git commit -m "test(function-pod): add full ctx_arg test coverage via @function_pod decorator (ITL-544)"
  ```

---

## Task 9: File follow-up Linear issue

**Context:** Per the spec, a follow-up issue must be filed to track full removal of
`side_effect_function_pod` (export removal, decorator deletion, usage migration).
Use the Linear MCP tool.

- [ ] **Step 1: File the follow-up issue**

  ```
  mcp__claude_ai_Linear__save_issue(
    title: "Remove side_effect_function_pod decorator (post-ITL-544 cleanup)",
    team: "Tools",
    description: """
  ## Overview

  ``side_effect_function_pod`` is superseded by ``@function_pod(ctx_arg=...)`` (ITL-544).
  This issue tracks full removal now that the preferred path is tested and documented.

  ## Goals & Success Criteria

  * ``side_effect_function_pod`` removed from ``src/orcapod/core/function_pod.py``.
  * Removed from ``__init__.py`` exports.
  * All existing usages (tests, docs, examples) migrated to ``@function_pod(ctx_arg=...)``.
  * No references to ``side_effect_function_pod`` remain in the codebase.

  ## Scope & Boundaries

  In scope:
  * Delete the ``side_effect_function_pod`` function.
  * Update all call sites to the ``@function_pod(ctx_arg=...)`` form.
  * Update ``tests/test_core/side_effect_function/test_side_effect_function_pod.py``
    to use the new decorator.

  Out of scope:
  * Changes to ``FunctionPod.from_fn`` or any other API surface.

  ## Dependencies & Risks

  * Depends on ITL-544 being merged first.
  """
  )
  ```

- [ ] **Step 2: Note the new issue ID for the PR description**

---

## Task 10: Create the PR

- [ ] **Step 1: Push the branch**

  ```bash
  git push -u origin eywalker/itl-544-fix-quick-patches-ctx_arg-on-function-pod-empty-opts
  ```

- [ ] **Step 2: Create the PR targeting `main`**

  ```bash
  gh pr create \
    --base main \
    --title "fix: ctx_arg on function pod + empty-opts ray.remote (ITL-544)" \
    --body "$(cat <<'EOF'
  ## Summary

  Closes ITL-544.

  Two quick patches promoted to properly tested, documented first-class capabilities:

  - **Ray executor fix:** Updated two mocks in `test_regression_fixes.py` to handle both `ray.remote(fn)` (empty-opts direct path) and `ray.remote(**opts)(fn)` (factory path). No changes to `ray.py` — the guard is correct.
  - **`ctx_arg` test coverage:** Added `tests/test_core/function_pod/test_function_pod_ctx_arg.py` with 10 scenarios covering schema inference, sync/async standalone execution, DB-backed execution, full pipeline, decorator forms, ctx collision, and cached pod wrapping.
  - **Docstring update:** `side_effect_function_pod` docstring now points to `@function_pod(ctx_arg=...)` as the preferred path. Follow-up issue filed for full removal.

  ## Test plan

  - [ ] `uv run pytest tests/test_core/test_regression_fixes.py -v` — all pass (including the two previously failing tests)
  - [ ] `uv run pytest tests/test_core/function_pod/test_function_pod_ctx_arg.py -v` — all 10 new tests pass
  - [ ] `uv run pytest tests/test_core/side_effect_function/ -v` — no regressions
  - [ ] `uv run pytest tests/ -q` — full suite clean

  🤖 Generated with [Claude Code](https://claude.com/claude-code)
  EOF
  )"
  ```
