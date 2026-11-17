# NodeConfig / PodConfig Split Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Split the old `NodeConfig` (which held `max_concurrency` and `is_result_ephemeral`) into `PodConfig` (pod/executor concern: `max_concurrency`) and a redefined `NodeConfig` (orchestrator concern: `is_result_ephemeral: bool | None`), removing `node_config` from `FunctionPod` and placing it directly on `FunctionJobNode`.

**Architecture:** Changes flow outward from the type layer: `types.py` first (new `PodConfig`, redefined `NodeConfig`), then `FunctionPod`, then `FunctionJobNode`, then the new `PipelineJob.apply_node_config()` API, then the protocol, and finally all call sites. Each task leaves the test suite passing before the next begins.

**Tech Stack:** Python 3.12+, `uv run pytest`, `pyarrow`, `asyncio`.

**Spec:** `superpowers/specs/2026-07-08-node-config-pod-config-split-design.md`

---

## File Map

| File | Action | What changes |
|---|---|---|
| `src/orcapod/types.py` | Modify | Add `PodConfig`; redefine `NodeConfig` with `is_result_ephemeral: bool \| None` and `merge()`; update `resolve_concurrency()` signature |
| `src/orcapod/core/function_pod.py` | Modify | Replace `node_config`/`_node_config` with `pod_config`/`_pod_config`; update `async_execute`, `to_config`, `from_config` |
| `src/orcapod/core/nodes/function_node.py` | Modify | Add `_node_config` field + `node_config` property; update `_process_data_internal`, `_async_process_data_internal`, `async_execute` |
| `src/orcapod/pipeline/job.py` | Modify | Add `_iter_function_job_nodes()` and `apply_node_config()` |
| `src/orcapod/protocols/node_protocols.py` | Modify | Add `node_config` read/write property to `FunctionNodeProtocol` |
| `tests/test_core/test_node_config.py` | Modify | Replace/add tests for `PodConfig` and new `NodeConfig.merge()` semantics |
| `tests/test_core/function_pod/test_ephemeral_result.py` | Modify | Replace `FunctionPod(pf, node_config=NodeConfig(is_result_ephemeral=True))` with `node.node_config = NodeConfig(...)` |
| `tests/test_channels/test_async_execute.py` | Modify | `NodeConfig(max_concurrency=N)` → `PodConfig(max_concurrency=N)` throughout |
| `tests/test_channels/test_node_async_execute.py` | Modify | Same `max_concurrency` → `PodConfig` migration |
| `tests/test_pipeline/test_orchestrator_executor_matrix.py` | Modify | Same `max_concurrency` → `PodConfig` migration |
| `tests/test_channels/test_copilot_review_issues.py` | Modify | Same `max_concurrency` → `PodConfig` migration |
| `tests/test_channels/test_channels.py` | Modify | Same `max_concurrency` → `PodConfig` migration |
| `tests/test_core/test_regression_fixes.py` | Modify | Same `max_concurrency` → `PodConfig` migration |
| `tests/test_channels/test_pipeline_example.py` | Modify | Same `max_concurrency` → `PodConfig` migration |
| `tests/test_pipeline/test_serialization_helpers.py` | Modify | `node_config` key → `pod_config` in serialised dicts |
| `tests/test_pipeline/test_apply_node_config.py` | Create | New tests for `PipelineJob.apply_node_config()` |

---

## Task 1: Introduce `PodConfig` and redefine `NodeConfig` in `types.py`

**Files:**
- Modify: `src/orcapod/types.py` (lines 334–372)
- Modify: `tests/test_core/test_node_config.py`

- [ ] **Step 1: Read the existing node config test file**

```bash
cat -n tests/test_core/test_node_config.py
```

Note which tests currently pass — these will need updating.

- [ ] **Step 2: Run existing tests to establish baseline**

```bash
uv run pytest tests/test_core/test_node_config.py -v
```

- [ ] **Step 3: Replace the contents of `tests/test_core/test_node_config.py` with updated tests**

```python
import pytest

from orcapod.types import NodeConfig, PipelineConfig, PodConfig, resolve_concurrency


class TestPodConfig:
    def test_defaults(self):
        config = PodConfig()
        assert config.max_concurrency is None

    def test_max_concurrency(self):
        config = PodConfig(max_concurrency=4)
        assert config.max_concurrency == 4

    def test_immutable(self):
        config = PodConfig(max_concurrency=4)
        with pytest.raises((AttributeError, TypeError)):
            config.max_concurrency = 8  # type: ignore[misc]


class TestNodeConfig:
    def test_defaults(self):
        config = NodeConfig()
        assert config.is_result_ephemeral is None

    def test_is_result_ephemeral_true(self):
        config = NodeConfig(is_result_ephemeral=True)
        assert config.is_result_ephemeral is True

    def test_is_result_ephemeral_false(self):
        config = NodeConfig(is_result_ephemeral=False)
        assert config.is_result_ephemeral is False

    def test_immutable(self):
        config = NodeConfig(is_result_ephemeral=True)
        with pytest.raises((AttributeError, TypeError)):
            config.is_result_ephemeral = False  # type: ignore[misc]

    def test_merge_none_in_other_self_wins(self):
        """None in other does not override self's value."""
        base = NodeConfig(is_result_ephemeral=True)
        result = base.merge(NodeConfig())
        assert result.is_result_ephemeral is True

    def test_merge_non_none_in_other_other_wins(self):
        """Non-None in other overrides self."""
        base = NodeConfig(is_result_ephemeral=True)
        result = base.merge(NodeConfig(is_result_ephemeral=False))
        assert result.is_result_ephemeral is False

    def test_merge_false_overrides_true(self):
        """Explicit False wins over True."""
        base = NodeConfig(is_result_ephemeral=True)
        result = base.merge(NodeConfig(is_result_ephemeral=False))
        assert result.is_result_ephemeral is False

    def test_merge_both_none(self):
        result = NodeConfig().merge(NodeConfig())
        assert result.is_result_ephemeral is None

    def test_merge_returns_new_instance(self):
        base = NodeConfig(is_result_ephemeral=True)
        result = base.merge(NodeConfig())
        assert result is not base


class TestResolveConcurrency:
    def test_pod_config_wins_over_pipeline(self):
        pod = PodConfig(max_concurrency=4)
        pipeline = PipelineConfig(default_max_concurrency=2)
        assert resolve_concurrency(pod, pipeline) == 4

    def test_falls_back_to_pipeline_when_pod_is_none(self):
        pod = PodConfig(max_concurrency=None)
        pipeline = PipelineConfig(default_max_concurrency=2)
        assert resolve_concurrency(pod, pipeline) == 2

    def test_both_none_returns_none(self):
        pod = PodConfig(max_concurrency=None)
        pipeline = PipelineConfig(default_max_concurrency=None)
        assert resolve_concurrency(pod, pipeline) is None

    def test_invalid_zero_raises(self):
        pod = PodConfig(max_concurrency=0)
        with pytest.raises(ValueError, match="max_concurrency must be >= 1"):
            resolve_concurrency(pod, PipelineConfig())

    def test_invalid_negative_raises(self):
        pod = PodConfig(max_concurrency=-1)
        with pytest.raises(ValueError, match="max_concurrency must be >= 1"):
            resolve_concurrency(pod, PipelineConfig())
```

- [ ] **Step 4: Run to confirm tests fail**

```bash
uv run pytest tests/test_core/test_node_config.py -v
```

Expected: FAIL — `PodConfig` not defined, `NodeConfig` still has `max_concurrency`, `merge()` does not exist.

- [ ] **Step 5: Replace lines 334–372 of `src/orcapod/types.py` with the new types**

```python
@dataclass(frozen=True, slots=True)
class PodConfig:
    """Per-pod executor configuration.

    Attributes:
        max_concurrency: Maximum concurrent function invocations for this pod.
            ``None`` inherits from ``PipelineConfig.default_max_concurrency``.
            ``1`` means sequential (rate-limited APIs, preserves ordering).
    """

    max_concurrency: int | None = None


@dataclass(frozen=True, slots=True)
class NodeConfig:
    """Per-node pipeline execution configuration.

    Attributes:
        is_result_ephemeral: ``None`` inherits the default (``False``).
            ``True`` writes new computation results to the pipeline-scoped
            ephemeral store instead of the persistent result database.
            Persistent cache hits are still served when available. Raises
            ``RuntimeError`` at execution time if ``True`` but no ephemeral
            store has been injected via ``set_ephemeral_store()``.
    """

    is_result_ephemeral: bool | None = None

    def merge(self, other: "NodeConfig") -> "NodeConfig":
        """Return a new ``NodeConfig`` with ``other``'s non-``None`` fields overriding self.

        ``None`` fields in ``other`` are treated as "not set" and leave
        self's value unchanged.

        Args:
            other: The ``NodeConfig`` whose non-``None`` fields take precedence.

        Returns:
            A new immutable ``NodeConfig``.

        Example:
            NodeConfig(is_result_ephemeral=True).merge(NodeConfig())
            # → NodeConfig(is_result_ephemeral=True)  (other's None leaves self unchanged)

            NodeConfig(is_result_ephemeral=True).merge(NodeConfig(is_result_ephemeral=False))
            # → NodeConfig(is_result_ephemeral=False)  (other's explicit False wins)
        """
        return NodeConfig(
            is_result_ephemeral=(
                other.is_result_ephemeral
                if other.is_result_ephemeral is not None
                else self.is_result_ephemeral
            ),
        )


def resolve_concurrency(
    pod_config: PodConfig, pipeline_config: PipelineConfig
) -> int | None:
    """Resolve effective concurrency from pod and pipeline configs.

    Returns:
        The concurrency limit to use, or ``None`` for unlimited.

    Raises:
        ValueError: If the resolved value is ``<= 0``.
    """
    if pod_config.max_concurrency is not None:
        result = pod_config.max_concurrency
    else:
        result = pipeline_config.default_max_concurrency
    if result is not None and result <= 0:
        raise ValueError(f"max_concurrency must be >= 1, got {result}")
    return result
```

- [ ] **Step 6: Check whether `PodConfig` needs to be exported from the top-level package**

```bash
grep -n "NodeConfig" src/orcapod/__init__.py 2>/dev/null || echo "not re-exported"
```

If `NodeConfig` is re-exported, add `PodConfig` to the same import line.

- [ ] **Step 7: Run tests to confirm passing**

```bash
uv run pytest tests/test_core/test_node_config.py -v
```

Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add src/orcapod/types.py tests/test_core/test_node_config.py
git commit -m "feat(types): introduce PodConfig and redefine NodeConfig with merge()"
```

---

## Task 2: Update `FunctionPod` to use `pod_config`

**Files:**
- Modify: `src/orcapod/core/function_pod.py` (lines 242–253, 291–340, ~359)
- Modify: `tests/test_channels/test_async_execute.py` (lines 44, 577, 597, 616, 622, 836, 843)
- Modify: `tests/test_pipeline/test_serialization_helpers.py` (line 481)

- [ ] **Step 1: Run baseline to see current failures caused by Task 1**

```bash
uv run pytest tests/test_channels/test_async_execute.py tests/test_pipeline/test_serialization_helpers.py -v 2>&1 | grep -E "FAILED|ERROR" | head -20
```

- [ ] **Step 2: Update imports in `src/orcapod/core/function_pod.py`**

Find the line that imports from `orcapod.types` and ensure it includes `PodConfig`:

```python
from orcapod.types import NodeConfig, PipelineConfig, PodConfig, resolve_concurrency
```

(Check the existing import — it likely already imports `NodeConfig`, `PipelineConfig`, and `resolve_concurrency`. Add `PodConfig`.)

- [ ] **Step 3: Replace `FunctionPod.__init__()` and add `.pod_config` property**

Replace lines 242–253 of `src/orcapod/core/function_pod.py`:

```python
def __init__(
    self,
    data_function: DataFunctionProtocol,
    pod_config: PodConfig | None = None,
    **kwargs,
) -> None:
    super().__init__(data_function, **kwargs)
    self._pod_config = pod_config or PodConfig()

@property
def pod_config(self) -> PodConfig:
    """Per-pod executor configuration."""
    return self._pod_config
```

- [ ] **Step 4: Update `async_execute()` at line ~359**

Change the `resolve_concurrency` call from `self._node_config` to `self._pod_config`:

```python
# Before (line ~359)
max_concurrency = resolve_concurrency(self._node_config, pipeline_config)

# After
max_concurrency = resolve_concurrency(self._pod_config, pipeline_config)
```

- [ ] **Step 5: Update `to_config()` (lines 291–310)**

Replace lines 291–310 with:

```python
def to_config(self) -> dict[str, Any]:
    """Serialize this function pod to a JSON-compatible config dict.

    Returns:
        A JSON-serializable dict containing the URI, data function config,
        and pod config for this function pod.
    """
    config: dict[str, Any] = {
        "uri": list(self.uri),
        "data_function": self.data_function.to_config(),
        "pod_config": None,
    }
    if self._pod_config.max_concurrency is not None:
        config["pod_config"] = {
            "max_concurrency": self._pod_config.max_concurrency,
        }
    return config
```

- [ ] **Step 6: Update `from_config()` (lines 312–340)**

Replace lines 312–340 with:

```python
@classmethod
def from_config(
    cls,
    config: dict[str, Any],
    *,
    fallback_to_proxy: bool = False,
) -> "FunctionPod":
    """Reconstruct a ``FunctionPod`` from a config dict.

    Args:
        config: A dict as produced by ``to_config``.
        fallback_to_proxy: If ``True`` and the data function cannot be
            resolved, use a ``DataFunctionProxy`` instead of raising.

    Returns:
        A new ``FunctionPod`` instance.
    """
    from orcapod.pipeline.serialization import resolve_data_function_from_config

    pf_config = config["data_function"]
    data_function = resolve_data_function_from_config(
        pf_config, fallback_to_proxy=fallback_to_proxy
    )

    pod_config = None
    if config.get("pod_config") is not None:
        pod_config = PodConfig(**config["pod_config"])

    return cls(data_function=data_function, pod_config=pod_config)
```

- [ ] **Step 7: Update `tests/test_channels/test_async_execute.py` (lines 44, 577, 597, 616, 622, 836, 843)**

Replace all `NodeConfig(max_concurrency=...)` with `PodConfig(max_concurrency=...)` and update the import:

```python
# Before
from orcapod.types import NodeConfig
pod = FunctionPod(pf, node_config=NodeConfig(max_concurrency=5))

# After
from orcapod.types import PodConfig
pod = FunctionPod(pf, pod_config=PodConfig(max_concurrency=5))
```

- [ ] **Step 8: Update `tests/test_pipeline/test_serialization_helpers.py` (line ~481)**

Update any serialised config dict that uses the `"node_config"` key:

```python
# Before
{"node_config": {"max_concurrency": 4}, "data_function": ..., "uri": ...}

# After
{"pod_config": {"max_concurrency": 4}, "data_function": ..., "uri": ...}
```

- [ ] **Step 9: Run tests to confirm passing**

```bash
uv run pytest tests/test_channels/test_async_execute.py tests/test_pipeline/test_serialization_helpers.py -v
```

Expected: PASS.

- [ ] **Step 10: Commit**

```bash
git add src/orcapod/core/function_pod.py \
        tests/test_channels/test_async_execute.py \
        tests/test_pipeline/test_serialization_helpers.py
git commit -m "refactor(function_pod): replace node_config with pod_config"
```

---

## Task 3: Update `FunctionJobNode` to own `node_config`

**Files:**
- Modify: `src/orcapod/core/nodes/function_node.py` (lines ~703–744, ~1203–1207, ~1320–1324, ~2080–2081)
- Create: `tests/test_core/function_pod/test_node_config_on_job_node.py`
- Modify: `tests/test_core/function_pod/test_ephemeral_result.py` (lines 19, 30, 32, 59, 185, 217–218, 247–248, 260, 324–325, 472, 489, 515–516, 528, 541, 563, 584, 687, 691, 693, 722, 724, 759, 761)
- Modify: `tests/test_channels/test_node_async_execute.py` (lines 35, 337, 374, 410)

- [ ] **Step 1: Write a failing test for the `node_config` property**

Create `tests/test_core/function_pod/test_node_config_on_job_node.py`:

```python
import pyarrow as pa
import pytest

from orcapod.core.data_function import PythonDataFunction
from orcapod.core.function_pod import FunctionPod
from orcapod.core.nodes.function_node import FunctionJobNode
from orcapod.core.sources import ArrowTableSource
from orcapod.databases import InMemoryArrowDatabase
from orcapod.types import NodeConfig


def _make_node() -> FunctionJobNode:
    def double(x: int) -> int:
        return x * 2

    pf = PythonDataFunction(double, output_keys="result")
    pod = FunctionPod(pf)
    table = pa.table({"id": [1], "x": [10]})
    source = ArrowTableSource(table, tag_columns=["id"])
    db = InMemoryArrowDatabase()
    return FunctionJobNode(pod, source, pipeline_database=db)


class TestFunctionJobNodeNodeConfig:
    def test_default_node_config(self):
        """FunctionJobNode initialises with NodeConfig() as default."""
        node = _make_node()
        assert isinstance(node.node_config, NodeConfig)
        assert node.node_config.is_result_ephemeral is None

    def test_node_config_setter(self):
        """node_config property setter replaces the config."""
        node = _make_node()
        new_config = NodeConfig(is_result_ephemeral=True)
        node.node_config = new_config
        assert node.node_config is new_config

    def test_set_then_replace(self):
        """Setting node_config twice replaces it cleanly."""
        node = _make_node()
        node.node_config = NodeConfig(is_result_ephemeral=True)
        node.node_config = NodeConfig(is_result_ephemeral=False)
        assert node.node_config.is_result_ephemeral is False

    def test_ephemeral_false_resolves_correctly(self):
        """is_result_ephemeral=None resolves to False at execution."""
        node = _make_node()
        # None should resolve to False via `or False`
        assert (node.node_config.is_result_ephemeral or False) is False

    def test_ephemeral_true_resolves_correctly(self):
        """is_result_ephemeral=True resolves to True at execution."""
        node = _make_node()
        node.node_config = NodeConfig(is_result_ephemeral=True)
        assert (node.node_config.is_result_ephemeral or False) is True
```

- [ ] **Step 2: Run to confirm failure**

```bash
uv run pytest tests/test_core/function_pod/test_node_config_on_job_node.py -v
```

Expected: FAIL — `FunctionJobNode` has no `node_config` attribute.

- [ ] **Step 3: Update imports in `src/orcapod/core/nodes/function_node.py`**

Find the import from `orcapod.types` and ensure it includes `NodeConfig` and `PodConfig`:

```python
from orcapod.types import NodeConfig, PipelineConfig, PodConfig, resolve_concurrency
```

- [ ] **Step 4: Add `_node_config` field to `FunctionJobNode.__init__()`**

In `FunctionJobNode.__init__()` (around line 740, before `self.attach_databases(...)`), add:

```python
# Node-level orchestrator config — applied post-construction via the
# .node_config setter or PipelineJob.apply_node_config().
self._node_config: NodeConfig = NodeConfig()
```

- [ ] **Step 5: Add the `node_config` property to `FunctionJobNode`**

Immediately after `__init__`, add:

```python
@property
def node_config(self) -> NodeConfig:
    """Per-node pipeline execution configuration."""
    return self._node_config

@node_config.setter
def node_config(self, value: NodeConfig) -> None:
    self._node_config = value
```

- [ ] **Step 6: Update `_process_data_internal()` (lines ~1203–1207)**

Replace:

```python
# Before
node_config = (
    self._function_pod.node_config if self._function_pod is not None else None
)
ephemeral_result = (
    node_config.is_result_ephemeral if node_config is not None else False
)

# After
ephemeral_result = self._node_config.is_result_ephemeral or False
```

- [ ] **Step 7: Update `_async_process_data_internal()` (lines ~1320–1324)**

Identical replacement as Step 6 — same two lines, same fix:

```python
# Before
node_config = (
    self._function_pod.node_config if self._function_pod is not None else None
)
ephemeral_result = (
    node_config.is_result_ephemeral if node_config is not None else False
)

# After
ephemeral_result = self._node_config.is_result_ephemeral or False
```

- [ ] **Step 8: Update `async_execute()` (lines ~2080–2081)**

Replace:

```python
# Before
node_config = getattr(self._function_pod, "node_config", NodeConfig())
max_concurrency = resolve_concurrency(node_config, PipelineConfig())

# After
pod_config = getattr(self._function_pod, "pod_config", PodConfig())
max_concurrency = resolve_concurrency(pod_config, PipelineConfig())
```

- [ ] **Step 9: Run the new property tests**

```bash
uv run pytest tests/test_core/function_pod/test_node_config_on_job_node.py -v
```

Expected: PASS.

- [ ] **Step 10: Update `tests/test_core/function_pod/test_ephemeral_result.py`**

Every place that currently passes `is_result_ephemeral=True` to `FunctionPod` must change to set it on the node after construction. Pattern:

```python
# Before
pod = FunctionPod(pf, node_config=NodeConfig(is_result_ephemeral=True))
node = FunctionJobNode(pod, stream, pipeline_database=db, ephemeral_database=edb)

# After
pod = FunctionPod(pf)
node = FunctionJobNode(pod, stream, pipeline_database=db, ephemeral_database=edb)
node.node_config = NodeConfig(is_result_ephemeral=True)
```

Apply this pattern to ALL usages at lines: 19, 30, 32, 59, 185, 217–218, 247–248, 260, 324–325, 472, 489, 515–516, 528, 541, 563, 584, 687, 691, 693, 722, 724, 759, 761.

Also update the import in this file — remove `NodeConfig` from the `FunctionPod` import context if it was only used as a constructor argument, and keep it for direct `NodeConfig(...)` usage.

- [ ] **Step 11: Update `tests/test_channels/test_node_async_execute.py` (lines 35, 337, 374, 410)**

Same pattern as Step 10 — remove `node_config=NodeConfig(is_result_ephemeral=True)` from `FunctionPod(...)` and set `node.node_config = NodeConfig(is_result_ephemeral=True)` on the job node after construction.

- [ ] **Step 12: Run all affected tests**

```bash
uv run pytest tests/test_core/function_pod/ tests/test_channels/test_node_async_execute.py -v
```

Expected: PASS.

- [ ] **Step 13: Commit**

```bash
git add src/orcapod/core/nodes/function_node.py \
        tests/test_core/function_pod/ \
        tests/test_channels/test_node_async_execute.py
git commit -m "refactor(function_node): move node_config ownership to FunctionJobNode"
```

---

## Task 4: Add `PipelineJob.apply_node_config()`

**Files:**
- Modify: `src/orcapod/pipeline/job.py`
- Create: `tests/test_pipeline/test_apply_node_config.py`

- [ ] **Step 1: Write failing tests**

Create `tests/test_pipeline/test_apply_node_config.py`:

```python
import pyarrow as pa
import pytest

from orcapod.core.data_function import PythonDataFunction
from orcapod.core.function_pod import FunctionPod
from orcapod.core.sources import ArrowTableSource
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline import PipelineJob
from orcapod.types import NodeConfig


def _make_job() -> PipelineJob:
    """Two-node pipeline: double → triple."""

    def double(x: int) -> int:
        return x * 2

    def triple(x: int) -> int:
        return x * 3

    pf1 = PythonDataFunction(double, output_keys="doubled")
    pf2 = PythonDataFunction(triple, output_keys="tripled")
    pod1 = FunctionPod(pf1)
    pod2 = FunctionPod(pf2)
    db = InMemoryArrowDatabase()
    table = pa.table({"id": [1, 2], "x": [10, 20]})
    source = ArrowTableSource(table, tag_columns=["id"])
    job = PipelineJob(name="test_job", store=db)
    with job:
        out1 = pod1(source)
        pod2(out1)
    return job


class TestApplyNodeConfig:
    def test_apply_sets_config_on_all_nodes(self):
        """apply_node_config sets config on every FunctionJobNode."""
        job = _make_job()
        config = NodeConfig(is_result_ephemeral=True)
        job.apply_node_config(config)
        nodes = list(job._iter_function_job_nodes())
        assert len(nodes) == 2
        for node in nodes:
            assert node.node_config.is_result_ephemeral is True

    def test_apply_replace_existing_false_wholesale(self):
        """replace_existing=False replaces each node's config wholesale."""
        job = _make_job()
        nodes = list(job._iter_function_job_nodes())
        # Pre-set one node to ephemeral=True
        nodes[0].node_config = NodeConfig(is_result_ephemeral=True)
        # Apply a new config wholesale — replaces whatever each node had
        job.apply_node_config(NodeConfig(is_result_ephemeral=False), replace_existing=False)
        for node in job._iter_function_job_nodes():
            assert node.node_config.is_result_ephemeral is False

    def test_apply_replace_existing_true_merges_none_no_op(self):
        """replace_existing=True: None in new config leaves existing values untouched."""
        job = _make_job()
        nodes = list(job._iter_function_job_nodes())
        nodes[0].node_config = NodeConfig(is_result_ephemeral=True)
        # Merge with a config that has None — should not override True
        job.apply_node_config(NodeConfig(), replace_existing=True)
        assert nodes[0].node_config.is_result_ephemeral is True

    def test_apply_replace_existing_true_overrides_with_explicit_value(self):
        """replace_existing=True: explicit False in new config overrides existing True."""
        job = _make_job()
        nodes = list(job._iter_function_job_nodes())
        nodes[0].node_config = NodeConfig(is_result_ephemeral=True)
        job.apply_node_config(NodeConfig(is_result_ephemeral=False), replace_existing=True)
        assert nodes[0].node_config.is_result_ephemeral is False

    def test_iter_function_job_nodes_yields_only_function_nodes(self):
        """_iter_function_job_nodes does not yield source or operator nodes."""
        from orcapod.core.nodes.function_node import FunctionJobNode
        job = _make_job()
        nodes = list(job._iter_function_job_nodes())
        assert all(isinstance(n, FunctionJobNode) for n in nodes)
```

- [ ] **Step 2: Run to confirm failure**

```bash
uv run pytest tests/test_pipeline/test_apply_node_config.py -v
```

Expected: FAIL — `apply_node_config` and `_iter_function_job_nodes` not defined on `PipelineJob`.

- [ ] **Step 3: Add imports to `src/orcapod/pipeline/job.py`**

At the top of `job.py`, add:

```python
from collections.abc import Iterator

from orcapod.types import NodeConfig
```

And inside the method (lazy import to avoid circularity):

```python
from orcapod.core.nodes.function_node import FunctionJobNode
```

- [ ] **Step 4: Add `_iter_function_job_nodes()` to `PipelineJob`**

```python
def _iter_function_job_nodes(self) -> "Iterator[FunctionJobNode]":
    """Iterate over all ``FunctionJobNode`` instances in the compiled pipeline.

    Returns:
        An iterator over every ``FunctionJobNode`` in the persistent node map.
    """
    from orcapod.core.nodes.function_node import FunctionJobNode

    return (
        node
        for node in self._persistent_node_map.values()
        if isinstance(node, FunctionJobNode)
    )
```

- [ ] **Step 5: Add `apply_node_config()` to `PipelineJob`**

```python
def apply_node_config(
    self,
    node_config: NodeConfig,
    replace_existing: bool = False,
) -> None:
    """Apply a ``NodeConfig`` to all ``FunctionJobNode``s in this pipeline.

    Args:
        node_config: The config to apply.
        replace_existing: If ``False``, directly sets ``node_config`` on
            every node, replacing whatever config each node currently holds.
            If ``True``, merges ``node_config`` into each node's existing
            config — non-``None`` fields in ``node_config`` override the
            node's current values, while ``None`` fields leave existing
            values unchanged.
    """
    for node in self._iter_function_job_nodes():
        if replace_existing:
            node.node_config = node.node_config.merge(node_config)
        else:
            node.node_config = node_config
```

- [ ] **Step 6: Run tests to confirm passing**

```bash
uv run pytest tests/test_pipeline/test_apply_node_config.py -v
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/orcapod/pipeline/job.py tests/test_pipeline/test_apply_node_config.py
git commit -m "feat(pipeline): add PipelineJob.apply_node_config() and _iter_function_job_nodes()"
```

---

## Task 5: Update `FunctionNodeProtocol`

**Files:**
- Modify: `src/orcapod/protocols/node_protocols.py`

- [ ] **Step 1: Add `node_config` read/write property to `FunctionNodeProtocol`**

Open `src/orcapod/protocols/node_protocols.py`. In the `FunctionNodeProtocol` class, add the following after the existing `pipeline_path` property and before `execute`:

```python
@property
def node_config(self) -> "NodeConfig": ...

@node_config.setter
def node_config(self, value: "NodeConfig") -> None: ...
```

In the `TYPE_CHECKING` block (or regular imports) at the top of the file, ensure `NodeConfig` is imported:

```python
if TYPE_CHECKING:
    from orcapod.types import NodeConfig
```

If `NodeConfig` is already a runtime import in this file, add it there instead.

- [ ] **Step 2: Run protocol-related tests**

```bash
uv run pytest tests/ -v -k "protocol" 2>&1 | tail -20
```

Expected: PASS (structural change, no runtime behaviour changed).

- [ ] **Step 3: Commit**

```bash
git add src/orcapod/protocols/node_protocols.py
git commit -m "refactor(protocols): add node_config property to FunctionNodeProtocol"
```

---

## Task 6: Fix remaining test construction sites

**Files:**
- Modify: `tests/test_pipeline/test_orchestrator_executor_matrix.py` (lines 47, 78)
- Modify: `tests/test_channels/test_copilot_review_issues.py` (lines 31, 86, 244, 248, 252, 256, 260, 266, 278)
- Modify: `tests/test_channels/test_channels.py` (lines 572–575, 579–595)
- Modify: `tests/test_core/test_regression_fixes.py` (lines 40, 330)
- Modify: `tests/test_channels/test_pipeline_example.py` (lines 36, 233)

- [ ] **Step 1: Run the full suite to see all remaining failures**

```bash
uv run pytest tests/ -v 2>&1 | grep -E "FAILED|ERROR"
```

- [ ] **Step 2: Update `tests/test_pipeline/test_orchestrator_executor_matrix.py` (lines 47, 78)**

```python
# Before
from orcapod.types import NodeConfig
pod = FunctionPod(pf, node_config=NodeConfig(max_concurrency=max_concurrency))

# After
from orcapod.types import PodConfig
pod = FunctionPod(pf, pod_config=PodConfig(max_concurrency=max_concurrency))
```

- [ ] **Step 3: Update `tests/test_channels/test_copilot_review_issues.py` (lines 31, 86, 244, 248, 252, 256, 260, 266, 278)**

```python
# Before
from orcapod.types import NodeConfig
pod = FunctionPod(pf, node_config=NodeConfig(max_concurrency=5))

# After
from orcapod.types import PodConfig
pod = FunctionPod(pf, pod_config=PodConfig(max_concurrency=5))
```

- [ ] **Step 4: Update `tests/test_channels/test_channels.py` (lines 572–575, 579–595)**

```python
# Before
from orcapod.types import NodeConfig
FunctionPod(pf, node_config=NodeConfig(max_concurrency=N))

# After
from orcapod.types import PodConfig
FunctionPod(pf, pod_config=PodConfig(max_concurrency=N))
```

- [ ] **Step 5: Update `tests/test_core/test_regression_fixes.py` (lines 40, 330)**

Same pattern as Step 4.

- [ ] **Step 6: Update `tests/test_channels/test_pipeline_example.py` (lines 36, 233)**

Same pattern as Step 4.

- [ ] **Step 7: Run the full test suite**

```bash
uv run pytest tests/ -v
```

Expected: all tests PASS.

- [ ] **Step 8: Commit**

```bash
git add tests/test_pipeline/test_orchestrator_executor_matrix.py \
        tests/test_channels/test_copilot_review_issues.py \
        tests/test_channels/test_channels.py \
        tests/test_core/test_regression_fixes.py \
        tests/test_channels/test_pipeline_example.py
git commit -m "refactor(tests): migrate all NodeConfig(max_concurrency) usages to PodConfig"
```
