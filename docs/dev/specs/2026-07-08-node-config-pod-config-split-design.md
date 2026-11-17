# Design: Split NodeConfig into PodConfig and NodeConfig

**Date:** 2026-07-08
**Issue:** [ITL-512 — Move node_config from FunctionPod to FunctionJobNode](https://linear.app/enigma-metamorphic/issue/ITL-512/move-node-config-from-functionpod-to-functionjobnode)
**Related:** [ITL-516 — Unify FunctionPod/FunctionJobNode async_execute() concurrency paths](https://linear.app/enigma-metamorphic/issue/ITL-516)

---

## Overview

`NodeConfig` currently lives on `FunctionPod` and carries two fields:

- `max_concurrency` — limits concurrent function invocations via an asyncio semaphore
- `is_result_ephemeral` — routes new results to the ephemeral store instead of the persistent DB

These two fields belong to different concerns. `max_concurrency` is an executor/pod capacity
constraint (how many concurrent calls the underlying function can safely handle). `is_result_ephemeral`
is an orchestrator/node execution policy (where the pipeline runner should write results).
Conflating them in a single config object on `FunctionPod` places orchestration policy on a pure
computation descriptor.

This design splits `NodeConfig` into two purpose-scoped config objects and moves the orchestration
concern to the appropriate layer.

---

## Guiding Principle

> **Pod concerns belong on `FunctionPod`. Orchestrator concerns belong on `FunctionJobNode`.**

- `FunctionPod` is a pure computation descriptor: it defines *what* a computation does, its
  identity, hashing, and execution capacity.
- `FunctionJobNode` is a pipeline execution node: it defines *how* the pipeline runner executes
  that computation — persistence, caching, storage routing, and concurrency scheduling.

---

## Design

### 1. Type Layer (`types.py`)

#### `PodConfig` (new)

Replaces the pod-level portion of the old `NodeConfig`. Lives on `FunctionPod`.

```python
@dataclass(frozen=True, slots=True)
class PodConfig:
    """Per-pod executor configuration.

    Attributes:
        max_concurrency: Maximum concurrent function invocations for this pod.
            None = unlimited (or inherits PipelineConfig.default_max_concurrency).
            1 = sequential execution. Applies both in standalone pod use
            (FunctionPod.async_execute) and in pipeline execution
            (FunctionJobNode.async_execute reads this to configure its semaphore).
    """
    max_concurrency: int | None = None
```

#### `NodeConfig` (redefined)

Retains the name but is now purely orchestrator-level. Moves to `FunctionJobNode`.

```python
@dataclass(frozen=True, slots=True)
class NodeConfig:
    """Per-node pipeline execution configuration.

    Attributes:
        is_result_ephemeral: None = inherit default (False).
            True = write new results to the pipeline-scoped ephemeral store.
            False = write to the persistent result database.
            Persistent cache hits are served regardless of this flag.
    """
    is_result_ephemeral: bool | None = None

    def merge(self, other: "NodeConfig") -> "NodeConfig":
        """Return a new NodeConfig with other's non-None fields overriding self.

        None fields in `other` are treated as "not set" and leave self's
        value unchanged. This allows partial overrides without losing
        explicitly configured fields.

        Example:
            NodeConfig(is_result_ephemeral=True).merge(NodeConfig()) 
            # → NodeConfig(is_result_ephemeral=True)  (other has None, self wins)

            NodeConfig(is_result_ephemeral=True).merge(NodeConfig(is_result_ephemeral=False))
            # → NodeConfig(is_result_ephemeral=False)  (other is explicit, other wins)
        """
        return NodeConfig(
            is_result_ephemeral=(
                other.is_result_ephemeral
                if other.is_result_ephemeral is not None
                else self.is_result_ephemeral
            ),
        )
```

**Resolution at call sites:** `None` resolves to `False` inline — `self._node_config.is_result_ephemeral or False`. No dedicated resolution method needed.

#### `resolve_concurrency()` (updated signature)

```python
def resolve_concurrency(pod_config: PodConfig, pipeline_config: PipelineConfig) -> int | None:
    ...
```

---

### 2. `FunctionPod` Changes

- **Remove** `node_config: NodeConfig | None` parameter from `__init__`
- **Remove** `_node_config` field and `.node_config` property
- **Add** `pod_config: PodConfig | None` parameter to `__init__`
- **Add** `_pod_config` field and `.pod_config` property
- `async_execute()` reads `self._pod_config.max_concurrency` for its semaphore (behaviour unchanged; standalone pod use only — never called by `FunctionJobNode`)
- `to_config()` serialises `pod_config`; `from_config()` deserialises it

**Construction site migration:**

```python
# Before
FunctionPod(pf, node_config=NodeConfig(max_concurrency=4))

# After
FunctionPod(pf, pod_config=PodConfig(max_concurrency=4))
```

`is_result_ephemeral` is removed from `FunctionPod` construction entirely. It is now set
post-construction on `FunctionJobNode` via `PipelineJob.apply_node_config()`.

---

### 3. `FunctionJobNode` Changes

- **Add** field `_node_config: NodeConfig = NodeConfig()` — not a constructor parameter; applied
  post-construction via the property setter
- **Add** property with getter and setter:

```python
@property
def node_config(self) -> NodeConfig:
    return self._node_config

@node_config.setter
def node_config(self, value: NodeConfig) -> None:
    self._node_config = value
```

- `_process_data_internal()` and `_async_process_data_internal()` replace the awkward:

```python
# Before
node_config = (
    self._function_pod.node_config if self._function_pod is not None else None
)
is_ephemeral = node_config.is_result_ephemeral if node_config is not None else False
```

with:

```python
# After
is_ephemeral = self._node_config.is_result_ephemeral or False
```

- `async_execute()` sources its concurrency semaphore from:

```python
max_concurrency = resolve_concurrency(self._function_pod.pod_config, PipelineConfig())
```

  The node respects the pod's declared execution capacity by reading `PodConfig` directly.
  `max_concurrency` remains a pod concern; `FunctionJobNode` does not own it.

---

### 4. `PipelineJob.apply_node_config()`

New method to apply `NodeConfig` uniformly across all `FunctionJobNode`s in the compiled pipeline.

```python
def apply_node_config(
    self,
    node_config: NodeConfig,
    replace_existing: bool = False,
) -> None:
    """Apply a NodeConfig to all FunctionJobNodes in this pipeline.

    Args:
        node_config: The config to apply.
        replace_existing: If False, directly sets node_config on every node,
            replacing whatever config each node currently holds. If True,
            merges node_config into each node's existing config — non-None
            fields in node_config override the node's current values, while
            None fields leave existing values unchanged.
    """
    for node in self._iter_function_job_nodes():
        if replace_existing:
            node.node_config = node.node_config.merge(node_config)
        else:
            node.node_config = node_config
```

**Usage patterns:**

```python
# Apply ephemeral storage to all nodes (wholesale replace)
job.apply_node_config(NodeConfig(is_result_ephemeral=True))

# Selectively override only is_result_ephemeral, preserving any other
# per-node config that may have been set individually
job.apply_node_config(NodeConfig(is_result_ephemeral=True), replace_existing=True)
```

---

### 5. `FunctionNodeProtocol` Changes

Add `node_config` as a read/write property so orchestrators and tests can interact with nodes
uniformly without knowing the concrete type:

```python
@property
def node_config(self) -> NodeConfig: ...

@node_config.setter
def node_config(self, value: NodeConfig) -> None: ...
```

---

## Concurrency Note

`FunctionPod.async_execute()` and `FunctionJobNode.async_execute()` are **completely independent**
execution paths — `FunctionJobNode` never delegates to `FunctionPod.async_execute()`. Both
implement their own asyncio semaphore sourced from `pod_config.max_concurrency`. This duplication
is acknowledged technical debt tracked in ITL-516 and is out of scope for this change.

---

## Breaking Changes

This is a deliberate breaking change. No backward-compatibility shims are added (project is pre-v0.1.0).

| Old | New |
|---|---|
| `NodeConfig(max_concurrency=N)` | `PodConfig(max_concurrency=N)` |
| `FunctionPod(pf, node_config=NodeConfig(...))` | `FunctionPod(pf, pod_config=PodConfig(...))` |
| `pod.node_config` | `pod.pod_config` |
| `resolve_concurrency(node_config, pipeline_config)` | `resolve_concurrency(pod_config, pipeline_config)` |
| `NodeConfig.is_result_ephemeral: bool` | `NodeConfig.is_result_ephemeral: bool \| None` |

Setting `is_result_ephemeral` on `FunctionPod` at construction is no longer possible.
Use `job.apply_node_config(NodeConfig(is_result_ephemeral=True))` after pipeline construction instead.

---

## Scope

**In scope:**

- Introducing `PodConfig` in `types.py`
- Redefining `NodeConfig` in `types.py` with `is_result_ephemeral: bool | None` and `merge()`
- Updating `resolve_concurrency()` to take `PodConfig`
- Removing `node_config` from `FunctionPod`; adding `pod_config`
- Adding `_node_config` field and `node_config` property (getter + setter) to `FunctionJobNode`
- Updating `_process_data_internal()` and `_async_process_data_internal()` to read from `self._node_config`
- Adding `PipelineJob.apply_node_config()`
- Adding `node_config` property to `FunctionNodeProtocol`
- Updating all construction sites and tests

**Out of scope:**

- Changing the semantics or fields of `PodConfig.max_concurrency`
- Unifying `FunctionPod.async_execute()` and `FunctionJobNode.async_execute()` (ITL-516)
- Per-node `apply_node_config` (individual node targeting vs. pipeline-wide apply)
- Adding new fields to `NodeConfig` or `PodConfig`
