# Ephemeral Function-Pod Output v1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `NodeConfig.ephemeral_result=True` mode to `FunctionJobNode` so new results route to an `InMemoryArrowDatabase` instead of the persistent result DB, while persistent cache hits remain available.

**Architecture:** Add an `IS_EPHEMERAL` boolean column to the pipeline tag table (replacing the "temp:" string prefix described in the spec, since `DATA_RECORD_ID` is `pa.large_binary()` and cannot carry a string prefix). `FunctionJobNode` gains an `_ephemeral_cached_pod` slot (a `CachedFunctionPod` backed by `InMemoryArrowDatabase`) created in `set_ephemeral_store()`. `_fetch_joined_records` is split into two independent Polars joins (one per store) merged with an anti-join that gives persistent results priority. `AbstractPipelineBase.set_ephemeral_store` propagates the store to every node.

**Tech Stack:** PyArrow, Polars (already used in `_fetch_joined_records`), existing `CachedFunctionPod`, `InMemoryArrowDatabase`, `ResultCache`.

---

## File Map

| File | Change |
|---|---|
| `src/orcapod/system_constants.py` | Add `IS_EPHEMERAL_COL` property to `SystemConstant` |
| `src/orcapod/types.py` | Add `ephemeral_result: bool = False` to `NodeConfig` |
| `src/orcapod/protocols/node_protocols.py` | Add `set_ephemeral_store()` to `FunctionNodeProtocol` and `OperatorNodeProtocol` |
| `src/orcapod/protocols/pipeline_protocols.py` | Add `set_ephemeral_store()` to `PipelineProtocol` |
| `src/orcapod/core/nodes/function_node.py` | No-op on `FunctionNodeBase`; override + two-store logic on `FunctionJobNode` |
| `src/orcapod/core/nodes/operator_node.py` | No-op `set_ephemeral_store()` on `OperatorNodeBase` |
| `src/orcapod/pipeline/base.py` | `AbstractPipelineBase.set_ephemeral_store()` propagating to all nodes |
| `tests/test_core/function_pod/test_ephemeral_result.py` | New file — all 17 test scenarios |

---

## Task 1: Add `IS_EPHEMERAL_COL` constant

**Files:**
- Modify: `src/orcapod/system_constants.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/test_system_constants.py  (append to existing file, or create if absent)
def test_is_ephemeral_col():
    from orcapod.system_constants import constants
    col = constants.IS_EPHEMERAL_COL
    assert col.startswith("__"), f"expected META_PREFIX, got {col!r}"
    assert "is_ephemeral" in col
```

- [ ] **Step 2: Run test to verify it fails**

```
uv run pytest tests/ -k "test_is_ephemeral_col" -v
```
Expected: FAIL with `AttributeError: 'SystemConstant' object has no attribute 'IS_EPHEMERAL_COL'`

- [ ] **Step 3: Add the constant**

In `src/orcapod/system_constants.py`, add at the module level (after the existing constants block, around line 17):

```python
IS_EPHEMERAL = "is_ephemeral"
```

Add the property to `SystemConstant` (after the `ENV_INFO` property, around line 101):

```python
@property
def IS_EPHEMERAL_COL(self) -> str:
    return f"{self._global_prefix}{SYSTEM_COLUMN_PREFIX}{IS_EPHEMERAL}"
```

- [ ] **Step 4: Run test to verify it passes**

```
uv run pytest tests/ -k "test_is_ephemeral_col" -v
```
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/system_constants.py tests/
git commit -m "feat(system_constants): add IS_EPHEMERAL_COL for ephemeral pipeline record routing"
```

---

## Task 2: `NodeConfig.ephemeral_result` field

**Files:**
- Modify: `src/orcapod/types.py:334-345`

- [ ] **Step 1: Write the failing test**

```python
# tests/test_core/test_node_config.py  (create if absent, or append)
def test_node_config_ephemeral_result_default_false():
    from orcapod.types import NodeConfig
    cfg = NodeConfig()
    assert cfg.ephemeral_result is False


def test_node_config_ephemeral_result_true():
    from orcapod.types import NodeConfig
    cfg = NodeConfig(ephemeral_result=True)
    assert cfg.ephemeral_result is True


def test_node_config_ephemeral_result_is_frozen():
    from orcapod.types import NodeConfig
    import dataclasses
    cfg = NodeConfig(ephemeral_result=True)
    with pytest.raises((dataclasses.FrozenInstanceError, TypeError)):
        cfg.ephemeral_result = False  # type: ignore[misc]
```

- [ ] **Step 2: Run test to verify it fails**

```
uv run pytest tests/ -k "test_node_config_ephemeral" -v
```
Expected: FAIL with `TypeError` (unexpected keyword argument) or `AttributeError`.

- [ ] **Step 3: Add the field**

In `src/orcapod/types.py`, edit the `NodeConfig` class (lines 334-345):

```python
@dataclass(frozen=True, slots=True)
class NodeConfig:
    """Per-node execution configuration.

    Attributes:
        max_concurrency: Override for this node's concurrency limit.
            ``None`` inherits from ``PipelineConfig.default_max_concurrency``.
            ``1`` means sequential (rate-limited APIs, preserves ordering).
        ephemeral_result: If ``True``, new computation results are written to
            the pipeline-scoped ephemeral store (``InMemoryArrowDatabase``)
            instead of the persistent result database. Persistent cache hits
            are still served when available. Raises ``RuntimeError`` at
            execution time if ``ephemeral_result=True`` but no ephemeral
            store has been injected via ``set_ephemeral_store()``.
    """

    max_concurrency: int | None = None
    ephemeral_result: bool = False
```

- [ ] **Step 4: Run test to verify it passes**

```
uv run pytest tests/ -k "test_node_config_ephemeral" -v
```
Expected: PASS

- [ ] **Step 5: Run the full test suite to catch regressions**

```
uv run pytest tests/ -x -q
```
Expected: all pass (no existing tests should break — new field has a default).

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/types.py tests/
git commit -m "feat(types): add ephemeral_result field to NodeConfig"
```

---

## Task 3: Protocol additions — `set_ephemeral_store`

**Files:**
- Modify: `src/orcapod/protocols/node_protocols.py`
- Modify: `src/orcapod/protocols/pipeline_protocols.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/test_protocols/test_ephemeral_store_protocol.py  (new file)
import pytest
from orcapod.protocols.node_protocols import FunctionNodeProtocol, OperatorNodeProtocol
from orcapod.protocols.pipeline_protocols import PipelineProtocol


def test_function_node_protocol_has_set_ephemeral_store():
    assert hasattr(FunctionNodeProtocol, "set_ephemeral_store")


def test_operator_node_protocol_has_set_ephemeral_store():
    assert hasattr(OperatorNodeProtocol, "set_ephemeral_store")


def test_pipeline_protocol_has_set_ephemeral_store():
    assert hasattr(PipelineProtocol, "set_ephemeral_store")
```

- [ ] **Step 2: Run test to verify it fails**

```
uv run pytest tests/test_protocols/ -k "test_ephemeral_store_protocol" -v
```
Expected: FAIL with `AttributeError`

- [ ] **Step 3: Add `set_ephemeral_store` to `node_protocols.py`**

In `src/orcapod/protocols/node_protocols.py`, add the import for `InMemoryArrowDatabase` under `TYPE_CHECKING` (it's already imported indirectly; add explicitly):

```python
if TYPE_CHECKING:
    from orcapod.channels import ReadableChannel, WritableChannel
    from orcapod.core.nodes import GraphNode
    from orcapod.databases.in_memory_databases import InMemoryArrowDatabase
    from orcapod.protocols.observability_protocols import ExecutionObserverProtocol
    from orcapod.protocols.core_protocols import (
        DataProtocol,
        StreamProtocol,
        TagProtocol,
    )
```

Add `set_ephemeral_store` to `FunctionNodeProtocol` (after the `async_execute` method, around line 90):

```python
def set_ephemeral_store(self, store: "InMemoryArrowDatabase | None") -> None:
    """Assign or remove the ephemeral result store for this node.

    Pass an ``InMemoryArrowDatabase`` to attach the store.
    Pass ``None`` to detach it — the node falls back to persistent-only
    behaviour for subsequent writes. No-op for node types that do not
    support ephemeral result storage (e.g. blueprint ``FunctionNode``).
    """
    ...
```

Add the same method to `OperatorNodeProtocol` (after `async_execute`, around line 116):

```python
def set_ephemeral_store(self, store: "InMemoryArrowDatabase | None") -> None:
    """Assign or remove the ephemeral result store for this node.

    No-op for operator nodes in v1 — full ephemeral support for operators
    is deferred to ITL-509.
    """
    ...
```

- [ ] **Step 4: Add `set_ephemeral_store` to `pipeline_protocols.py`**

In `src/orcapod/protocols/pipeline_protocols.py`, add `InMemoryArrowDatabase` under `TYPE_CHECKING`:

```python
if TYPE_CHECKING:
    import pyarrow as pa
    from orcapod.databases.in_memory_databases import InMemoryArrowDatabase
    from orcapod.pipeline.dag import GraphProtocol
```

Add the method to `PipelineProtocol` (after the `dag` property, around line 60):

```python
def set_ephemeral_store(self, store: "InMemoryArrowDatabase | None") -> None:
    """Assign or remove the ephemeral result store for all nodes.

    Propagates ``store`` to every node in the pipeline by calling
    ``node.set_ephemeral_store(store)`` on each. Nodes that do not
    support ephemeral storage (e.g. operator nodes in v1) treat this
    as a no-op.

    Pass ``None`` to detach the ephemeral store from all nodes,
    reverting them to persistent-only writes for subsequent runs.
    """
    ...
```

- [ ] **Step 5: Run test to verify it passes**

```
uv run pytest tests/test_protocols/ -k "test_ephemeral_store_protocol" -v
```
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/protocols/node_protocols.py src/orcapod/protocols/pipeline_protocols.py tests/
git commit -m "feat(protocols): add set_ephemeral_store to FunctionNodeProtocol, OperatorNodeProtocol, PipelineProtocol"
```

---

## Task 4: No-op `set_ephemeral_store` on base node classes

**Files:**
- Modify: `src/orcapod/core/nodes/function_node.py` (`FunctionNodeBase`, lines ~88-160)
- Modify: `src/orcapod/core/nodes/operator_node.py` (`OperatorNodeBase`, lines ~61-120)

- [ ] **Step 1: Write the failing test**

```python
# tests/test_core/function_pod/test_ephemeral_result.py  (create new file)
"""Tests for FunctionJobNode ephemeral result store — ITL-507."""
from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.core.data_function import PythonDataFunction
from orcapod.core.datagrams import Data, Tag
from orcapod.core.function_pod import FunctionPod
from orcapod.core.nodes import FunctionNode
from orcapod.core.nodes.function_node import FunctionJobNode
from orcapod.core.sources import ArrowTableSource
from orcapod.core.streams.arrow_table_stream import ArrowTableStream
from orcapod.databases import InMemoryArrowDatabase
from orcapod.types import NodeConfig


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def double(x: int) -> int:
    return x * 2


def _make_pod(config: NodeConfig | None = None):
    pf = PythonDataFunction(double, output_keys="result")
    return FunctionPod(pf, config=config)


def _make_stream(rows: list[dict], tag_columns: list[str] | None = None) -> ArrowTableStream:
    if tag_columns is None:
        tag_columns = ["id"]
    table = pa.table(
        {k: pa.array([r[k] for r in rows], type=pa.int64()) for k in rows[0]}
    )
    source = ArrowTableSource(table, tag_columns=tag_columns, source_id="test_src", infer_nullable=True)
    return source


def _make_node(stream, pipeline_db=None, result_db=None, ephemeral_result: bool = False):
    """Create a FunctionJobNode with given DB configuration."""
    cfg = NodeConfig(ephemeral_result=ephemeral_result)
    pod = _make_pod(config=cfg)
    if pipeline_db is None:
        pipeline_db = InMemoryArrowDatabase()
    return FunctionJobNode(
        function_pod=pod,
        input_stream=stream,
        pipeline_database=pipeline_db,
        result_database=result_db if result_db is not None else pipeline_db,
    ), pipeline_db


# ---------------------------------------------------------------------------
# Task 4 test: no-op set_ephemeral_store on blueprint node classes
# ---------------------------------------------------------------------------

class TestNoOpSetEphemeralStore:
    def test_function_node_has_set_ephemeral_store(self):
        """FunctionNode (blueprint) must have set_ephemeral_store as a no-op."""
        stream = _make_stream([{"id": 0, "x": 10}])
        pf = PythonDataFunction(double, output_keys="result")
        pod = FunctionPod(pf)
        node = FunctionNode(function_pod=pod, input_stream=stream)
        store = InMemoryArrowDatabase()
        # Must not raise
        node.set_ephemeral_store(store)
        node.set_ephemeral_store(None)

    def test_operator_node_has_set_ephemeral_store(self):
        """OperatorNode (blueprint) must have set_ephemeral_store as a no-op."""
        from orcapod.core.nodes import OperatorNode
        from orcapod.core.operators import Join

        stream_a = _make_stream([{"id": 0, "x": 10}])
        stream_b = _make_stream([{"id": 0, "y": 20}])
        op = Join()
        node = OperatorNode(operator=op, input_streams=(stream_a, stream_b))
        store = InMemoryArrowDatabase()
        # Must not raise
        node.set_ephemeral_store(store)
        node.set_ephemeral_store(None)
```

- [ ] **Step 2: Run test to verify it fails**

```
uv run pytest tests/test_core/function_pod/test_ephemeral_result.py::TestNoOpSetEphemeralStore -v
```
Expected: FAIL with `AttributeError: 'FunctionNode' object has no attribute 'set_ephemeral_store'`

- [ ] **Step 3: Add no-op to `FunctionNodeBase`**

In `src/orcapod/core/nodes/function_node.py`, add to `FunctionNodeBase` after the `__init__` method (and before `# ------------------------------------------------------------------`). Add `TYPE_CHECKING` import for `InMemoryArrowDatabase` at the top of the file — add it inside the existing `if TYPE_CHECKING:` block:

```python
if TYPE_CHECKING:
    import polars as pl
    import pyarrow as pa
    import pyarrow.compute as pc
    from orcapod.databases.in_memory_databases import InMemoryArrowDatabase
```

Then add the method to `FunctionNodeBase`:

```python
def set_ephemeral_store(self, store: "InMemoryArrowDatabase | None") -> None:
    """No-op for blueprint nodes — only ``FunctionJobNode`` uses ephemeral stores."""
```

- [ ] **Step 4: Add no-op to `OperatorNodeBase`**

In `src/orcapod/core/nodes/operator_node.py`, add to `OperatorNodeBase` similarly. First add to `TYPE_CHECKING` block:

```python
if TYPE_CHECKING:
    import pyarrow as pa
    import pyarrow.compute as pc
    from orcapod.databases.in_memory_databases import InMemoryArrowDatabase
    from orcapod.protocols.observability_protocols import ExecutionObserverProtocol
```

Then add the method to `OperatorNodeBase`:

```python
def set_ephemeral_store(self, store: "InMemoryArrowDatabase | None") -> None:
    """No-op — operator nodes do not support ephemeral result storage in v1 (ITL-509)."""
```

- [ ] **Step 5: Run test to verify it passes**

```
uv run pytest tests/test_core/function_pod/test_ephemeral_result.py::TestNoOpSetEphemeralStore -v
```
Expected: PASS

- [ ] **Step 6: Run full test suite**

```
uv run pytest tests/ -x -q
```
Expected: all pass

- [ ] **Step 7: Commit**

```bash
git add src/orcapod/core/nodes/function_node.py src/orcapod/core/nodes/operator_node.py tests/test_core/function_pod/test_ephemeral_result.py
git commit -m "feat(nodes): add no-op set_ephemeral_store to FunctionNodeBase and OperatorNodeBase"
```

---

## Task 5: `FunctionJobNode` — ephemeral slots and `set_ephemeral_store` override

**Files:**
- Modify: `src/orcapod/core/nodes/function_node.py` (`FunctionJobNode`, lines ~677-770)

- [ ] **Step 1: Add test**

Append to `tests/test_core/function_pod/test_ephemeral_result.py`:

```python
class TestSetEphemeralStore:
    def test_set_ephemeral_store_assigns_store(self):
        """set_ephemeral_store(store) assigns the ephemeral_result_store attribute."""
        stream = _make_stream([{"id": 0, "x": 10}])
        node, _ = _make_node(stream)
        store = InMemoryArrowDatabase()
        node.set_ephemeral_store(store)
        assert node.ephemeral_result_store is store

    def test_set_ephemeral_store_none_detaches(self):
        """set_ephemeral_store(None) removes the ephemeral store."""
        stream = _make_stream([{"id": 0, "x": 10}])
        node, _ = _make_node(stream)
        store = InMemoryArrowDatabase()
        node.set_ephemeral_store(store)
        node.set_ephemeral_store(None)
        assert node.ephemeral_result_store is None
```

- [ ] **Step 2: Run test to verify it fails**

```
uv run pytest tests/test_core/function_pod/test_ephemeral_result.py::TestSetEphemeralStore -v
```
Expected: FAIL with `AttributeError: 'FunctionJobNode' object has no attribute 'ephemeral_result_store'`

- [ ] **Step 3: Add slots and override to `FunctionJobNode`**

In `FunctionJobNode.__init__` (around line 706), add after the `self._pipeline_database` initialization:

```python
# Ephemeral result store (None until set_ephemeral_store() is called by the pipeline)
self.ephemeral_result_store: "InMemoryArrowDatabase | None" = None
self._ephemeral_cached_pod: CachedFunctionPod | None = None
```

After `FunctionJobNode.attach_databases` (around line 767), add:

```python
# ------------------------------------------------------------------
# set_ephemeral_store — override (FunctionJobNode has real behavior)
# ------------------------------------------------------------------

def set_ephemeral_store(self, store: "InMemoryArrowDatabase | None") -> None:
    """Assign or remove the ephemeral result store.

    When *store* is not ``None``, creates a ``CachedFunctionPod`` backed by
    *store* so that ephemeral writes use the same format as persistent writes.
    When *store* is ``None``, clears both the store and the ephemeral pod.

    Args:
        store: The ``InMemoryArrowDatabase`` to use for ephemeral result
            storage, or ``None`` to detach and revert to persistent-only writes.
    """
    self.ephemeral_result_store = store
    if store is not None and self._function_pod is not None:
        self._ephemeral_cached_pod = CachedFunctionPod(
            self._function_pod,
            result_database=store,
        )
    else:
        self._ephemeral_cached_pod = None
```

- [ ] **Step 4: Run test to verify it passes**

```
uv run pytest tests/test_core/function_pod/test_ephemeral_result.py::TestSetEphemeralStore -v
```
Expected: PASS

- [ ] **Step 5: Run full test suite**

```
uv run pytest tests/ -x -q
```
Expected: all pass

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/core/nodes/function_node.py tests/test_core/function_pod/test_ephemeral_result.py
git commit -m "feat(function_node): add ephemeral_result_store slot and set_ephemeral_store override to FunctionJobNode"
```

---

## Task 6: `add_pipeline_record` — add `is_ephemeral` and enforce `skip_cache_lookup=True` in Phase 2

**Files:**
- Modify: `src/orcapod/core/nodes/function_node.py` (lines ~1267-1341)

This task does two things:
1. Adds `is_ephemeral: bool = False` parameter and stores `IS_EPHEMERAL_COL` in the tag table row.
2. Updates the call-site in `_process_data_internal` to always use `skip_cache_lookup=True` (fixing the potential infinite-miss-cycle for both persistent and ephemeral paths — see spec §3 "Recompute-after-miss write strategy").

- [ ] **Step 1: Add test for IS_EPHEMERAL_COL written to pipeline DB**

Append to `tests/test_core/function_pod/test_ephemeral_result.py`:

```python
class TestAddPipelineRecord:
    def test_is_ephemeral_false_written_to_pipeline_db(self):
        """add_pipeline_record(is_ephemeral=False) stores IS_EPHEMERAL_COL = False."""
        from orcapod.system_constants import constants

        stream = _make_stream([{"id": 0, "x": 10}])
        node, db = _make_node(stream)
        results = node.execute(stream)
        assert len(results) == 1

        all_records = db.get_all_records(node.node_identity_path)
        assert all_records is not None
        assert constants.IS_EPHEMERAL_COL in all_records.column_names
        vals = all_records.column(constants.IS_EPHEMERAL_COL).to_pylist()
        assert all(v is False for v in vals)

    def test_is_ephemeral_true_written_to_pipeline_db(self):
        """When ephemeral_result=True, IS_EPHEMERAL_COL=True is stored in the tag table."""
        from orcapod.system_constants import constants

        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()
        ephemeral_store = InMemoryArrowDatabase()
        node, _ = _make_node(stream, pipeline_db=pipeline_db, ephemeral_result=True)
        node.set_ephemeral_store(ephemeral_store)

        results = node.execute(stream)
        assert len(results) == 1

        all_records = pipeline_db.get_all_records(node.node_identity_path)
        assert all_records is not None
        assert constants.IS_EPHEMERAL_COL in all_records.column_names
        vals = all_records.column(constants.IS_EPHEMERAL_COL).to_pylist()
        assert all(v is True for v in vals)
```

- [ ] **Step 2: Run test to verify it fails**

```
uv run pytest tests/test_core/function_pod/test_ephemeral_result.py::TestAddPipelineRecord -v
```
Expected: FAIL — `IS_EPHEMERAL_COL` not in pipeline DB columns.

- [ ] **Step 3: Update `add_pipeline_record` signature and body**

In `src/orcapod/core/nodes/function_node.py`, edit `add_pipeline_record` (lines ~1267-1341):

Change the signature to add `is_ephemeral: bool = False`:

```python
def add_pipeline_record(
    self,
    tag: TagProtocol,
    input_data: DataProtocol,
    data_record_id: uuid.UUID,
    computed: bool,
    skip_cache_lookup: bool = False,
    is_ephemeral: bool = False,
) -> None:
    """Add a pipeline record to the database for a processed data.

    The pipeline record stores:
    - Tag columns (including system tags)
    - All source columns of the input data (provenance, not data)
    - Output data record ID (for joining with result records)
    - Whether the result is stored in the ephemeral store
    - Input data data context key
    - Whether the result was freshly computed or cached
    """
```

In the `meta_table` construction (around line 1311), add `IS_EPHEMERAL_COL`:

```python
meta_table = pa.table(
    {
        constants.DATA_RECORD_ID: pa.array(
            [data_record_id.bytes], type=pa.large_binary()
        ),
        constants.NODE_CONTENT_HASH_COL: pa.array(
            [self.content_hash().to_string()], type=pa.large_string()
        ),
        f"{constants.META_PREFIX}input_data{constants.CONTEXT_KEY}": pa.array(
            [input_data.data_context_key], type=pa.large_string()
        ),
        f"{constants.META_PREFIX}computed": pa.array(
            [computed], type=pa.bool_()
        ),
        constants.IS_EPHEMERAL_COL: pa.array(
            [is_ephemeral], type=pa.bool_()
        ),
    }
)
```

- [ ] **Step 4: Update `_process_data_internal` call-site (persistent path)**

In `_process_data_internal` (lines ~1099-1143), update the existing `add_pipeline_record` call to add `skip_cache_lookup=True`:

```python
# OLD:
self.add_pipeline_record(
    tag,
    data,
    data_record_id=output_data.datagram_uuid,
    computed=result_computed,
)

# NEW:
self.add_pipeline_record(
    tag,
    data,
    data_record_id=output_data.datagram_uuid,
    computed=result_computed,
    skip_cache_lookup=True,
)
```

- [ ] **Step 5: Run test to verify it passes**

```
uv run pytest tests/test_core/function_pod/test_ephemeral_result.py::TestAddPipelineRecord -v
```
Expected: PASS

- [ ] **Step 6: Run full test suite**

```
uv run pytest tests/ -x -q
```
Expected: all pass

- [ ] **Step 7: Commit**

```bash
git add src/orcapod/core/nodes/function_node.py tests/test_core/function_pod/test_ephemeral_result.py
git commit -m "feat(function_node): add is_ephemeral column to pipeline records; enforce skip_cache_lookup=True in Phase 2"
```

---

## Task 7: `_fetch_joined_records` — two-store logic

This is the largest structural change. `_fetch_joined_records` (lines ~1437-1503) is restructured from a single-store join into a two-store join with anti-join priority merge.

**Files:**
- Modify: `src/orcapod/core/nodes/function_node.py:1437-1503`

- [ ] **Step 1: Add tests for the two-store logic**

Append to `tests/test_core/function_pod/test_ephemeral_result.py`:

```python
class TestBulkResolution:
    def test_ephemeral_false_unchanged(self):
        """ephemeral_result=False: execute() behaves identically to current implementation."""
        stream = _make_stream([{"id": 0, "x": 10}, {"id": 1, "x": 20}])
        node, _ = _make_node(stream)
        results = node.execute(stream)
        assert len(results) == 2
        vals = {tag.as_dict()["id"]: data.as_dict()["result"] for tag, data in results}
        assert vals == {0: 20, 1: 40}

    def test_ephemeral_result_written_to_memory_not_persistent_db(self):
        """With ephemeral_result=True, persistent DB has no result rows; ephemeral store does."""
        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()
        result_db = InMemoryArrowDatabase()
        ephemeral_store = InMemoryArrowDatabase()
        cfg = NodeConfig(ephemeral_result=True)
        pod = _make_pod(config=cfg)
        node = FunctionJobNode(
            function_pod=pod,
            input_stream=stream,
            pipeline_database=pipeline_db,
            result_database=result_db,
        )
        node.set_ephemeral_store(ephemeral_store)
        results = node.execute(stream)

        assert len(results) == 1
        assert results[0][1].as_dict()["result"] == 20

        # Persistent result DB must be empty (no writes there)
        from orcapod.core.cached_function_pod import CachedFunctionPod
        eph_cache = node._ephemeral_cached_pod
        assert eph_cache is not None
        assert eph_cache.result_database.get_all_records(eph_cache.record_path) is not None
        assert result_db.get_all_records(node._cached_function_pod.record_path) is None

    def test_within_session_ephemeral_hit(self):
        """Same node called twice: second call hits ephemeral store — no recomputation."""
        call_count = {"n": 0}

        def counting_double(x: int) -> int:
            call_count["n"] += 1
            return x * 2

        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()
        ephemeral_store = InMemoryArrowDatabase()
        pf = PythonDataFunction(counting_double, output_keys="result")
        cfg = NodeConfig(ephemeral_result=True)
        pod = FunctionPod(pf, config=cfg)
        node = FunctionJobNode(
            function_pod=pod,
            input_stream=stream,
            pipeline_database=pipeline_db,
        )
        node.set_ephemeral_store(ephemeral_store)

        node.execute(stream)
        assert call_count["n"] == 1

        # Second execution — same entry_id — must hit cache
        node._cached_output_datas.clear()  # clear in-memory cache to force DB lookup
        node.execute(stream)
        assert call_count["n"] == 1  # function must NOT have been called again

    def test_cross_session_miss_recomputes(self):
        """Fresh InMemoryArrowDatabase (new session): ephemeral miss triggers recomputation."""
        call_count = {"n": 0}

        def counting_double(x: int) -> int:
            call_count["n"] += 1
            return x * 2

        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()

        # Session 1: execute with ephemeral store
        pf = PythonDataFunction(counting_double, output_keys="result")
        cfg = NodeConfig(ephemeral_result=True)
        pod = FunctionPod(pf, config=cfg)
        node = FunctionJobNode(
            function_pod=pod,
            input_stream=stream,
            pipeline_database=pipeline_db,
        )
        node.set_ephemeral_store(InMemoryArrowDatabase())
        node.execute(stream)
        assert call_count["n"] == 1

        # Session 2: fresh in-memory node with a fresh ephemeral store
        # The pipeline_db still has the tag entry (IS_EPHEMERAL=True), but the
        # ephemeral store is fresh — this should trigger a cache miss and recompute.
        pf2 = PythonDataFunction(counting_double, output_keys="result")
        pod2 = FunctionPod(pf2, config=cfg)
        node2 = FunctionJobNode(
            function_pod=pod2,
            input_stream=stream,
            pipeline_database=pipeline_db,
        )
        node2.set_ephemeral_store(InMemoryArrowDatabase())  # fresh store
        node2.execute(stream)
        assert call_count["n"] == 2  # recomputed

    def test_persistent_hit_served_when_ephemeral_true(self):
        """A persistent result is still served from cache when ephemeral store is also set.

        Verifies that attaching an ephemeral store doesn't break Phase 1's persistent join.
        The node uses ephemeral_result=False (default), so Phase 2 writes to persistent DB.
        Phase 1 with two-store logic must still find the persistent result.
        """
        call_count = {"n": 0}

        def counting_double(x: int) -> int:
            call_count["n"] += 1
            return x * 2

        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()
        pf = PythonDataFunction(counting_double, output_keys="result")
        pod = FunctionPod(pf)  # ephemeral_result=False (default)
        node = FunctionJobNode(
            function_pod=pod,
            input_stream=stream,
            pipeline_database=pipeline_db,
        )
        # Attach an ephemeral store — must NOT break persistent reads
        node.set_ephemeral_store(InMemoryArrowDatabase())

        # Run 1: writes to persistent DB (ephemeral_result=False)
        results = node.execute(stream)
        assert len(results) == 1
        assert results[0][1].as_dict()["result"] == 20
        assert call_count["n"] == 1

        # Clear in-memory cache to force DB lookup on Run 2
        node._cached_output_datas.clear()

        # Run 2: Phase 1 must find persistent result — no recompute
        results2 = node.execute(stream)
        assert len(results2) == 1
        assert results2[0][1].as_dict()["result"] == 20
        assert call_count["n"] == 1  # NOT recomputed

    def test_bulk_resolution_mixed_stores(self):
        """Tag table has both persistent and ephemeral entries; both resolve correctly."""
        stream_a = _make_stream([{"id": 0, "x": 10}])
        stream_b = _make_stream([{"id": 1, "x": 20}])
        pipeline_db = InMemoryArrowDatabase()

        # id=0 → persistent
        node_p, _ = _make_node(stream_a, pipeline_db=pipeline_db, ephemeral_result=False)
        node_p.execute(stream_a)

        # id=1 → ephemeral, reusing same pipeline_db
        call_count = {"n": 0}

        def counting_double(x: int) -> int:
            call_count["n"] += 1
            return x * 2

        ephemeral_store = InMemoryArrowDatabase()
        pf = PythonDataFunction(counting_double, output_keys="result")
        cfg = NodeConfig(ephemeral_result=True)
        pod = FunctionPod(pf, config=cfg)
        node_e = FunctionJobNode(
            function_pod=pod,
            input_stream=stream_b,
            pipeline_database=pipeline_db,
        )
        node_e.set_ephemeral_store(ephemeral_store)
        node_e.execute(stream_b)

        # Now load both results via a combined stream
        stream_both = _make_stream([{"id": 0, "x": 10}, {"id": 1, "x": 20}])
        node_both = FunctionJobNode(
            function_pod=pod,
            input_stream=stream_both,
            pipeline_database=pipeline_db,
        )
        node_both.set_ephemeral_store(ephemeral_store)
        node_both._cached_output_datas.clear()

        results = node_both.execute(stream_both)
        assert len(results) == 2
        vals = {tag.as_dict()["id"]: data.as_dict()["result"] for tag, data in results}
        assert vals == {0: 20, 1: 40}

    def test_persistent_result_outcompetes_ephemeral(self):
        """When both persistent and ephemeral entries exist for the same entry_id, persistent wins.

        This tests the anti-join merge: a tag table row with IS_EPHEMERAL=True that shares
        an entry_id with an IS_EPHEMERAL=False row should be excluded from available_results.
        """
        # Use a single node to write both a persistent AND an ephemeral entry for id=0.
        # We do this by:
        #   1. Execute with ephemeral store attached but ephemeral_result=False → writes persistent entry
        #   2. Manually insert an ephemeral tag-table row + ephemeral store result to simulate
        #      the scenario where both exist (e.g. after a recovery run).
        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()
        ephemeral_store = InMemoryArrowDatabase()

        # Write a persistent entry
        call_count = {"n": 0}

        def counting_double(x: int) -> int:
            call_count["n"] += 1
            return x * 2

        pf = PythonDataFunction(counting_double, output_keys="result")
        pod = FunctionPod(pf)
        node = FunctionJobNode(
            function_pod=pod,
            input_stream=stream,
            pipeline_database=pipeline_db,
        )
        node.set_ephemeral_store(ephemeral_store)
        node.execute(stream)  # writes IS_EPHEMERAL=False persistent entry
        assert call_count["n"] == 1

        # Simulate an ephemeral entry also existing for the same input
        # by calling add_pipeline_record directly with is_ephemeral=True.
        # (This represents the "recovery scenario" from the spec.)
        import uuid as _uuid
        tag = Tag({"id": 0})
        data = Data({"x": 10})
        fake_uuid = _uuid.UUID(int=0)  # dummy UUID, no actual result stored
        node.add_pipeline_record(
            tag, data,
            data_record_id=fake_uuid,
            computed=True,
            skip_cache_lookup=True,
            is_ephemeral=True,
        )

        # Clear in-memory cache to force fresh DB lookup
        node._cached_output_datas.clear()

        # Phase 1: both persistent (result=20) and ephemeral (fake UUID, no actual result)
        # entries exist. Persistent should win — ephemeral entry excluded by anti-join.
        # Since fake_uuid has no actual result in ephemeral_store, the ephemeral join
        # produces no row for it. The persistent join wins, and result=20 is returned.
        results = node.execute(stream)
        assert len(results) == 1
        assert results[0][1].as_dict()["result"] == 20
        assert call_count["n"] == 1  # NOT recomputed
```

- [ ] **Step 2: Run the test to verify it fails**

```
uv run pytest tests/test_core/function_pod/test_ephemeral_result.py::TestBulkResolution -v
```
Expected: multiple failures — `IS_EPHEMERAL_COL` not used in `_fetch_joined_records`, result written to wrong store, etc.

- [ ] **Step 3: Restructure `_fetch_joined_records`**

Replace the entire body of `_fetch_joined_records` in `function_node.py` (lines ~1437-1503):

```python
def _fetch_joined_records(
    self,
    entry_ids: list[bytes] | None = None,
) -> _JoinedRecords | None:
    """Internal primitive: fetch both DBs and inner-join, supporting two stores.

    Fetches ``taginfo`` from the pipeline database, partitions rows by
    ``IS_EPHEMERAL_COL`` into persistent and ephemeral groups, performs two
    independent inner joins (one per store), merges with persistent priority
    via an anti-join, and returns the combined result.

    Persistent miss rows (tag entry with no matching result DB row) emit a
    WARNING-level log. Ephemeral miss rows (cross-session miss) are silently
    dropped.

    If ``entry_ids`` is provided, the result is filtered to matching
    ``_PIPELINE_ENTRY_ID_COL`` values before conversion to Arrow.

    Args:
        entry_ids: If given, return only rows whose
            ``_PIPELINE_ENTRY_ID_COL`` value is in this list.
            If ``None``, return all rows.

    Returns:
        A ``_JoinedRecords`` whose ``table`` always includes a
        ``_PIPELINE_ENTRY_ID_COL`` column, or ``None`` when either
        the pipeline database or cached function pod is absent. A 0-row
        table (not ``None``) is returned when both fetches succeed but
        no matching rows exist — callers check ``num_rows`` themselves.
    """
    if self._cached_function_pod is None or self._pipeline_database is None:
        return None

    taginfo = self._pipeline_database.get_all_records(
        self.node_identity_path,
        record_id_column=_PIPELINE_ENTRY_ID_COL,
    )

    if taginfo is None:
        return None

    taginfo_columns = tuple(taginfo.column_names)
    taginfo = self._filter_by_content_hash(taginfo)
    taginfo_schema = taginfo.schema

    is_ephemeral_col = constants.IS_EPHEMERAL_COL
    taginfo_df = pl.from_arrow(taginfo)

    # Partition by IS_EPHEMERAL_COL (backward-compat: missing col → all persistent)
    if is_ephemeral_col in taginfo.column_names:
        persistent_taginfo_df = taginfo_df.filter(
            ~pl.col(is_ephemeral_col).fill_null(False)
        )
        ephemeral_taginfo_df = taginfo_df.filter(
            pl.col(is_ephemeral_col).fill_null(False)
        )
    else:
        persistent_taginfo_df = taginfo_df
        ephemeral_taginfo_df = pl.DataFrame()

    # ------------------------------------------------------------------
    # Persistent join
    # ------------------------------------------------------------------
    results_schema = None
    persistent_df = pl.DataFrame()
    if persistent_taginfo_df.height > 0:
        results = self._cached_function_pod.result_database.get_all_records(
            self._cached_function_pod.record_path,
            record_id_column=constants.DATA_RECORD_ID,
        )
        if results is None:
            # Tag table has persistent entries but result DB is empty — data loss
            for row_dict in persistent_taginfo_df.to_dicts():
                logger.warning(
                    "Pipeline DB entry '%s' has no match in persistent result DB "
                    "— data may have been deleted externally. "
                    "This input will be recomputed.",
                    row_dict.get(_PIPELINE_ENTRY_ID_COL),
                )
        else:
            results_schema = results.schema
            full_persistent_df = persistent_taginfo_df.join(
                pl.from_arrow(results),
                on=constants.DATA_RECORD_ID,
                how="inner",
            )
            # Warn about persistent tag rows that found no match in the result DB
            if full_persistent_df.height < persistent_taginfo_df.height:
                matched_ids = set(
                    full_persistent_df.select(_PIPELINE_ENTRY_ID_COL)
                    .to_series()
                    .to_list()
                )
                for row_dict in persistent_taginfo_df.to_dicts():
                    if row_dict[_PIPELINE_ENTRY_ID_COL] not in matched_ids:
                        logger.warning(
                            "Pipeline DB entry '%s' has no match in persistent result DB "
                            "— data may have been deleted externally. "
                            "This input will be recomputed.",
                            row_dict[_PIPELINE_ENTRY_ID_COL],
                        )
            persistent_df = full_persistent_df

    # ------------------------------------------------------------------
    # Ephemeral join
    # ------------------------------------------------------------------
    ephemeral_df = pl.DataFrame()
    if ephemeral_taginfo_df.height > 0 and self._ephemeral_cached_pod is not None:
        eph_results = self._ephemeral_cached_pod.result_database.get_all_records(
            self._ephemeral_cached_pod.record_path,
            record_id_column=constants.DATA_RECORD_ID,
        )
        if eph_results is not None:
            # Cross-session miss → eph_results is None → silently drop (expected)
            if results_schema is None:
                results_schema = eph_results.schema
            ephemeral_df = ephemeral_taginfo_df.join(
                pl.from_arrow(eph_results),
                on=constants.DATA_RECORD_ID,
                how="inner",
            )

    # ------------------------------------------------------------------
    # Merge with persistent priority (anti-join + concat)
    # ------------------------------------------------------------------
    if ephemeral_df.height > 0 and persistent_df.height > 0:
        ephemeral_only_df = ephemeral_df.join(
            persistent_df.select([_PIPELINE_ENTRY_ID_COL]),
            on=_PIPELINE_ENTRY_ID_COL,
            how="anti",
        )
        merged_df = pl.concat([persistent_df, ephemeral_only_df], how="diagonal")
    elif ephemeral_df.height > 0:
        merged_df = ephemeral_df
    elif persistent_df.height > 0:
        merged_df = persistent_df
    else:
        # No results found in either store — return empty table preserving schema
        if taginfo.num_rows == 0 or results_schema is None:
            # Both tables empty — return 0-row table from taginfo schema
            empty_table = taginfo.slice(0, 0)
            return _JoinedRecords(table=empty_table, taginfo_columns=taginfo_columns)
        # We have tag rows but no matching results — also return empty
        empty_table = taginfo.slice(0, 0)
        return _JoinedRecords(table=empty_table, taginfo_columns=taginfo_columns)

    # Apply entry_id filter if requested
    if entry_ids is not None:
        merged_df = merged_df.filter(
            pl.col(_PIPELINE_ENTRY_ID_COL).is_in(entry_ids)
        )

    joined = merged_df.to_arrow()
    if results_schema is not None:
        joined = arrow_utils.restore_schema_nullability(
            joined, taginfo_schema, results_schema
        )
    return _JoinedRecords(table=joined, taginfo_columns=taginfo_columns)
```

Note: `pl.from_arrow()` is used instead of `pl.DataFrame()` for creating Polars DataFrames from Arrow tables — this is cleaner than `pl.DataFrame(arrow_table)`. Verify this API works in the Polars version used by this project. If `pl.from_arrow` is not available, use `pl.DataFrame(arrow_table)` (the original form).

- [ ] **Step 4: Run test to verify it passes**

```
uv run pytest tests/test_core/function_pod/test_ephemeral_result.py::TestBulkResolution -v
```
Expected: PASS for all tests in this class. Some tests may still fail if `_process_data_internal` hasn't been updated yet (Task 8).

- [ ] **Step 5: Run full test suite**

```
uv run pytest tests/ -x -q
```
Expected: all existing tests still pass.

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/core/nodes/function_node.py tests/test_core/function_pod/test_ephemeral_result.py
git commit -m "feat(function_node): restructure _fetch_joined_records for two-store ephemeral/persistent join"
```

---

## Task 8: `_process_data_internal` — ephemeral write path

**Files:**
- Modify: `src/orcapod/core/nodes/function_node.py:1099-1143`

- [ ] **Step 1: Add tests for the ephemeral write path**

Append to `tests/test_core/function_pod/test_ephemeral_result.py`:

```python
class TestEphemeralWritePath:
    def test_store_not_assigned_raises(self):
        """ephemeral_result=True but ephemeral_result_store=None raises RuntimeError."""
        stream = _make_stream([{"id": 0, "x": 10}])
        cfg = NodeConfig(ephemeral_result=True)
        pod = _make_pod(config=cfg)
        pipeline_db = InMemoryArrowDatabase()
        node = FunctionJobNode(
            function_pod=pod,
            input_stream=stream,
            pipeline_database=pipeline_db,
        )
        # set_ephemeral_store never called → ephemeral_result_store is None
        with pytest.raises(RuntimeError, match="ephemeral_result=True"):
            node.execute(stream)

    def test_ephemeral_only_node(self):
        """result_database=None, ephemeral_result=True: end-to-end works."""
        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()
        ephemeral_store = InMemoryArrowDatabase()
        cfg = NodeConfig(ephemeral_result=True)
        pod = _make_pod(config=cfg)
        # Pass pipeline_database but no result_database — ephemeral only
        node = FunctionJobNode(
            function_pod=pod,
            input_stream=stream,
            pipeline_database=pipeline_db,
        )
        node.set_ephemeral_store(ephemeral_store)
        results = node.execute(stream)
        assert len(results) == 1
        assert results[0][1].as_dict()["result"] == 20

    def test_recompute_after_ephemeral_miss_no_infinite_cycle(self):
        """Cross-session ephemeral miss → recomputed → served on next call (no cycle)."""
        call_count = {"n": 0}

        def counting_double(x: int) -> int:
            call_count["n"] += 1
            return x * 2

        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()

        # Session 1
        pf = PythonDataFunction(counting_double, output_keys="result")
        cfg = NodeConfig(ephemeral_result=True)
        pod = FunctionPod(pf, config=cfg)
        node1 = FunctionJobNode(
            function_pod=pod,
            input_stream=stream,
            pipeline_database=pipeline_db,
        )
        node1.set_ephemeral_store(InMemoryArrowDatabase())
        node1.execute(stream)
        assert call_count["n"] == 1

        # Session 2: fresh store → miss → recompute
        pf2 = PythonDataFunction(counting_double, output_keys="result")
        pod2 = FunctionPod(pf2, config=cfg)
        ephemeral2 = InMemoryArrowDatabase()
        node2 = FunctionJobNode(
            function_pod=pod2,
            input_stream=stream,
            pipeline_database=pipeline_db,
        )
        node2.set_ephemeral_store(ephemeral2)
        node2.execute(stream)
        assert call_count["n"] == 2

        # Session 3: same ephemeral store as session 2 → hit → no recompute
        pf3 = PythonDataFunction(counting_double, output_keys="result")
        pod3 = FunctionPod(pf3, config=cfg)
        node3 = FunctionJobNode(
            function_pod=pod3,
            input_stream=stream,
            pipeline_database=pipeline_db,
        )
        node3.set_ephemeral_store(ephemeral2)  # reuse session 2's store
        node3._cached_output_datas.clear()
        node3.execute(stream)
        assert call_count["n"] == 2  # NOT recomputed
```

- [ ] **Step 2: Run test to verify it fails**

```
uv run pytest tests/test_core/function_pod/test_ephemeral_result.py::TestEphemeralWritePath -v
```
Expected: `test_store_not_assigned_raises` fails (no RuntimeError), others may also fail.

- [ ] **Step 3: Update `_process_data_internal`**

In `src/orcapod/core/nodes/function_node.py`, replace the body of `_process_data_internal` (lines ~1099-1143):

```python
def _process_data_internal(
    self,
    tag: TagProtocol,
    data: DataProtocol,
    *,
    logger: DataExecutionLoggerProtocol | None = None,
) -> tuple[TagProtocol, DataProtocol | None]:
    """Core compute + persist + cache.

    Used by ``execute_data`` and ``execute``.
    Stores result in ``_cached_output_datas`` keyed by entry_id.
    Exceptions propagate to the caller — no error handling here.

    When ``node_config.ephemeral_result=True``:
    - Uses ``_ephemeral_cached_pod`` for both compute and storage.
    - Raises ``RuntimeError`` if no ephemeral store has been set.
    - Writes the pipeline record with ``is_ephemeral=True`` and
      ``skip_cache_lookup=True`` (append strategy for recompute-after-miss).

    When ``node_config.ephemeral_result=False`` (default):
    - Uses ``_cached_function_pod`` (persistent DB) or raw function pod.
    - Writes the pipeline record with ``skip_cache_lookup=True`` (prevents
      infinite miss cycle when a stale pipeline entry exists).

    Returns:
        A ``(tag, output_data)`` 2-tuple.
    """
    node_config = self._function_pod.config if self._function_pod is not None else None
    ephemeral_result = (
        node_config.ephemeral_result
        if node_config is not None
        else False
    )

    if ephemeral_result:
        if self._ephemeral_cached_pod is None:
            raise RuntimeError(
                f"FunctionJobNode '{self.label}' has ephemeral_result=True but no "
                "ephemeral store has been assigned. Call set_ephemeral_store() with "
                "an InMemoryArrowDatabase before executing this node."
            )
        tag_out, output_data = self._ephemeral_cached_pod.process_data(
            tag, data, logger=logger
        )
        if output_data is not None:
            result_computed = bool(
                output_data.get_meta_value(
                    self._ephemeral_cached_pod.RESULT_COMPUTED_FLAG, True
                )
            )
            if self._pipeline_database is not None:
                self.add_pipeline_record(
                    tag,
                    data,
                    data_record_id=output_data.datagram_uuid,
                    computed=result_computed,
                    skip_cache_lookup=True,
                    is_ephemeral=True,
                )
    elif self._cached_function_pod is not None:
        tag_out, output_data = self._cached_function_pod.process_data(
            tag, data, logger=logger
        )
        if output_data is not None:
            result_computed = bool(
                output_data.get_meta_value(
                    self._cached_function_pod.RESULT_COMPUTED_FLAG, False
                )
            )
            self.add_pipeline_record(
                tag,
                data,
                data_record_id=output_data.datagram_uuid,
                computed=result_computed,
                skip_cache_lookup=True,
            )
    else:
        tag_out, output_data = self._function_pod.process_data(
            tag, data, logger=logger
        )

    # Store by entry_id and invalidate derived caches
    entry_id = self.compute_pipeline_entry_id(tag, data)
    self._cached_output_datas[entry_id] = (tag_out, output_data)
    self._cached_output_table = None
    self._cached_content_hash_column = None

    return tag_out, output_data
```

Note: `self._function_pod.config` accesses the `NodeConfig` from the pod. Verify this attribute path by checking `FunctionPod.config` in `src/orcapod/core/function_pod.py`. If the path is different, adjust accordingly (it may be `self._function_pod.node_config` or similar). If `NodeConfig` is not accessible from the pod at this point, add a `self._node_config` slot to `FunctionJobNode.__init__` set from `function_pod.config` at construction time.

- [ ] **Step 4: Verify NodeConfig access path**

```
uv run python -c "
from orcapod.core.function_pod import FunctionPod
from orcapod.core.data_function import PythonDataFunction
from orcapod.types import NodeConfig
pf = PythonDataFunction(lambda x: x, output_keys='y')
cfg = NodeConfig(ephemeral_result=True)
pod = FunctionPod(pf, config=cfg)
print(type(pod.config))  # should print NodeConfig
print(pod.config.ephemeral_result)  # should print True
"
```

If `pod.config` raises `AttributeError`, find the correct attribute name:

```
uv run python -c "
from orcapod.core.function_pod import FunctionPod
from orcapod.core.data_function import PythonDataFunction
pf = PythonDataFunction(lambda x: x, output_keys='y')
pod = FunctionPod(pf)
print([a for a in dir(pod) if 'config' in a.lower() or 'node' in a.lower()])
"
```

Adjust the `ephemeral_result` lookup in `_process_data_internal` to use the correct attribute.

- [ ] **Step 5: Run test to verify it passes**

```
uv run pytest tests/test_core/function_pod/test_ephemeral_result.py::TestEphemeralWritePath -v
```
Expected: PASS for all three tests.

- [ ] **Step 6: Run full test suite**

```
uv run pytest tests/ -x -q
```
Expected: all pass.

- [ ] **Step 7: Commit**

```bash
git add src/orcapod/core/nodes/function_node.py tests/test_core/function_pod/test_ephemeral_result.py
git commit -m "feat(function_node): implement ephemeral write path in _process_data_internal"
```

---

## Task 9: `AbstractPipelineBase.set_ephemeral_store`

**Files:**
- Modify: `src/orcapod/pipeline/base.py`

- [ ] **Step 1: Add test**

Append to `tests/test_core/function_pod/test_ephemeral_result.py`:

```python
class TestPipelineInjectsStore:
    def test_pipeline_job_set_ephemeral_store_propagates(self):
        """PipelineJob.set_ephemeral_store propagates to all compiled nodes."""
        from orcapod.pipeline.job import PipelineJob

        stream = _make_stream([{"id": 0, "x": 10}, {"id": 1, "x": 20}])
        cfg = NodeConfig(ephemeral_result=True)
        pod = _make_pod(config=cfg)

        job = PipelineJob(name="test_pipeline")
        with job:
            output = pod(stream)

        ephemeral_store = InMemoryArrowDatabase()
        job.set_ephemeral_store(ephemeral_store)

        # Every function node in the compiled pipeline should now have the store
        for label, node in job.function_pods.items():
            assert node.ephemeral_result_store is ephemeral_store, (
                f"Node '{label}' did not receive the ephemeral store"
            )

    def test_pipeline_job_set_ephemeral_store_none_detaches(self):
        """PipelineJob.set_ephemeral_store(None) detaches the store from all nodes."""
        from orcapod.pipeline.job import PipelineJob

        stream = _make_stream([{"id": 0, "x": 10}])
        cfg = NodeConfig(ephemeral_result=True)
        pod = _make_pod(config=cfg)

        job = PipelineJob(name="test_pipeline")
        with job:
            pod(stream)

        store = InMemoryArrowDatabase()
        job.set_ephemeral_store(store)
        job.set_ephemeral_store(None)

        for label, node in job.function_pods.items():
            assert node.ephemeral_result_store is None, (
                f"Node '{label}' ephemeral store was not detached"
            )
```

- [ ] **Step 2: Run test to verify it fails**

```
uv run pytest tests/test_core/function_pod/test_ephemeral_result.py::TestPipelineInjectsStore -v
```
Expected: FAIL with `AttributeError: 'PipelineJob' object has no attribute 'set_ephemeral_store'`

- [ ] **Step 3: Add `set_ephemeral_store` to `AbstractPipelineBase`**

In `src/orcapod/pipeline/base.py`, add the `InMemoryArrowDatabase` import inside the `TYPE_CHECKING` block:

```python
if TYPE_CHECKING:
    import networkx as nx
    from orcapod.databases.in_memory_databases import InMemoryArrowDatabase
```

Add the method to `AbstractPipelineBase` after the `operator_pods` property (around line 137):

```python
def set_ephemeral_store(self, store: "InMemoryArrowDatabase | None") -> None:
    """Assign or remove the ephemeral result store for all compiled nodes.

    Propagates *store* to every compiled node in the pipeline by calling
    ``node.set_ephemeral_store(store)`` on each. Nodes that do not support
    ephemeral storage (e.g. operator nodes in v1, blueprint function nodes)
    treat this as a no-op.

    Pass ``None`` to detach the ephemeral store from all nodes, reverting
    them to persistent-only behaviour for subsequent writes.

    Args:
        store: The ``InMemoryArrowDatabase`` to use for ephemeral result
            storage, or ``None`` to detach from all nodes.
    """
    for node in self._nodes.values():
        node.set_ephemeral_store(store)
```

- [ ] **Step 4: Run test to verify it passes**

```
uv run pytest tests/test_core/function_pod/test_ephemeral_result.py::TestPipelineInjectsStore -v
```
Expected: PASS

- [ ] **Step 5: Run full test suite**

```
uv run pytest tests/ -x -q
```
Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/pipeline/base.py tests/test_core/function_pod/test_ephemeral_result.py
git commit -m "feat(pipeline): add set_ephemeral_store to AbstractPipelineBase propagating to all nodes"
```

---

## Task 10: Remaining tests — persistent miss warning and append-after-miss

- [ ] **Step 1: Add remaining tests**

Append to `tests/test_core/function_pod/test_ephemeral_result.py`:

```python
class TestPersistentMissWarning:
    def test_persistent_miss_warns_and_recomputes(self, caplog):
        """Tag table has a regular record_id but persistent DB was trimmed: WARNING emitted."""
        import logging

        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()
        result_db = InMemoryArrowDatabase()

        # Write a persistent record
        node, _ = _make_node(stream, pipeline_db=pipeline_db, result_db=result_db, ephemeral_result=False)
        node.execute(stream)

        # Wipe the result DB to simulate data loss
        result_db._tables.clear()
        result_db._pending_batches.clear()

        # Recreate node with same pipeline_db (tag entry still there) but empty result_db
        node2, _ = _make_node(stream, pipeline_db=pipeline_db, result_db=result_db, ephemeral_result=False)

        with caplog.at_level(logging.WARNING, logger="orcapod.core.nodes.function_node"):
            results = node2.execute(stream)

        assert len(results) == 1  # recomputed
        assert any("has no match in persistent result DB" in msg for msg in caplog.messages)

    def test_recompute_after_persistent_miss_appends_new_pipeline_record(self):
        """After persistent miss and recompute, tag table has two rows; next call hits."""
        call_count = {"n": 0}

        def counting_double(x: int) -> int:
            call_count["n"] += 1
            return x * 2

        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()
        result_db = InMemoryArrowDatabase()

        # Session 1: write persistent record
        pf = PythonDataFunction(counting_double, output_keys="result")
        pod = FunctionPod(pf)
        node1 = FunctionJobNode(
            function_pod=pod,
            input_stream=stream,
            pipeline_database=pipeline_db,
            result_database=result_db,
        )
        node1.execute(stream)
        assert call_count["n"] == 1

        # Simulate data loss
        result_db._tables.clear()
        result_db._pending_batches.clear()

        # Session 2: miss → recompute → appends new record to pipeline_db
        pf2 = PythonDataFunction(counting_double, output_keys="result")
        pod2 = FunctionPod(pf2)
        node2 = FunctionJobNode(
            function_pod=pod2,
            input_stream=stream,
            pipeline_database=pipeline_db,
            result_database=result_db,
        )
        node2.execute(stream)
        assert call_count["n"] == 2  # recomputed

        # Session 3: tag table now has two rows (stale + new); inner join resolves correctly
        pf3 = PythonDataFunction(counting_double, output_keys="result")
        pod3 = FunctionPod(pf3)
        node3 = FunctionJobNode(
            function_pod=pod3,
            input_stream=stream,
            pipeline_database=pipeline_db,
            result_database=result_db,
        )
        node3._cached_output_datas.clear()
        node3.execute(stream)
        assert call_count["n"] == 2  # NOT recomputed — new row was found


class TestEphemeralOnlyNode:
    def test_ephemeral_only_node_no_persistent_db(self):
        """NodeConfig(ephemeral_result=True) with no result_database works end-to-end."""
        stream = _make_stream([{"id": 0, "x": 10}, {"id": 1, "x": 20}])
        pipeline_db = InMemoryArrowDatabase()
        ephemeral_store = InMemoryArrowDatabase()
        cfg = NodeConfig(ephemeral_result=True)
        pf = PythonDataFunction(double, output_keys="result")
        pod = FunctionPod(pf, config=cfg)

        # No result_database — pipeline_db doubles as both
        node = FunctionJobNode(
            function_pod=pod,
            input_stream=stream,
            pipeline_database=pipeline_db,
        )
        node.set_ephemeral_store(ephemeral_store)
        results = node.execute(stream)

        assert len(results) == 2
        vals = {tag.as_dict()["id"]: data.as_dict()["result"] for tag, data in results}
        assert vals == {0: 20, 1: 40}
```

- [ ] **Step 2: Run new tests to verify they pass (or fail informatively)**

```
uv run pytest tests/test_core/function_pod/test_ephemeral_result.py::TestPersistentMissWarning tests/test_core/function_pod/test_ephemeral_result.py::TestEphemeralOnlyNode -v
```
Expected: PASS (the WARNING-emitting path was already added in Task 7).

- [ ] **Step 3: Run the complete ephemeral test file**

```
uv run pytest tests/test_core/function_pod/test_ephemeral_result.py -v
```
Expected: all tests PASS.

- [ ] **Step 4: Run full test suite**

```
uv run pytest tests/ -x -q
```
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add tests/test_core/function_pod/test_ephemeral_result.py
git commit -m "test(ephemeral_result): add persistent miss warning and append-after-miss tests"
```

---

## Task 11: Final validation and PR

- [ ] **Step 1: Run the full test suite one final time**

```
uv run pytest tests/ -q
```
Expected: all pass, no failures.

- [ ] **Step 2: Verify IS_EPHEMERAL_COL does not appear in any `get_all_records` user-facing output**

```
uv run python -c "
from orcapod.core.data_function import PythonDataFunction
from orcapod.core.function_pod import FunctionPod
from orcapod.core.nodes.function_node import FunctionJobNode
from orcapod.core.sources import ArrowTableSource
from orcapod.databases import InMemoryArrowDatabase
from orcapod.types import NodeConfig
import pyarrow as pa

table = pa.table({'id': pa.array([0], type=pa.int64()), 'x': pa.array([10], type=pa.int64())})
source = ArrowTableSource(table, tag_columns=['id'], source_id='src', infer_nullable=True)

pf = PythonDataFunction(lambda x: x * 2, output_keys='result')
cfg = NodeConfig(ephemeral_result=True)
pod = FunctionPod(pf, config=cfg)
pipeline_db = InMemoryArrowDatabase()
node = FunctionJobNode(function_pod=pod, input_stream=source, pipeline_database=pipeline_db)
node.set_ephemeral_store(InMemoryArrowDatabase())
node.execute(source)
records = node.get_all_records()
print('columns:', records.column_names)
print('result:', records.to_pydict())
"
```

Expected: `result` column present, `__is_ephemeral` NOT in `records.column_names` (it's an internal column and should be dropped by `get_all_records`' ColumnConfig filtering, since it starts with `META_PREFIX`).

If `__is_ephemeral` does appear in `get_all_records` output, add it to the `drop_cols` list in `_load_cached_entries` (around line 1560):

```python
drop_cols = [
    c
    for c in joined.column_names
    if c.startswith(constants.META_PREFIX)
    or c == constants.NODE_CONTENT_HASH_COL
]
```

Since `IS_EPHEMERAL_COL = f"{META_PREFIX}is_ephemeral"`, it starts with `META_PREFIX` and will be automatically excluded. No change needed.

- [ ] **Step 3: Push branch and open PR**

```bash
git push -u origin agent-kurodo/itl-507-ephemeral-function-pod-output
gh pr create \
  --title "feat(ITL-507): ephemeral function-pod output v1" \
  --base main \
  --body "$(cat <<'EOF'
## Summary

- Adds `NodeConfig.ephemeral_result: bool = False` field
- New `set_ephemeral_store(store: InMemoryArrowDatabase | None)` method on `FunctionNodeProtocol`, `OperatorNodeProtocol`, `PipelineProtocol`, and `AbstractPipelineBase`
- `FunctionJobNode` gains `ephemeral_result_store` slot and two-store read/write logic: `_fetch_joined_records` now performs two independent Polars inner-joins (one per store) merged with an anti-join that gives persistent results priority
- `IS_EPHEMERAL` boolean column in the pipeline tag table routes record lookups to the correct store (replaces the "temp:" string prefix from the spec, preserving `DATA_RECORD_ID` as `pa.large_binary()`)
- All Phase 2 writes use `skip_cache_lookup=True` to prevent infinite miss cycles on stale entries (v1 append strategy)
- Persistent DB miss → WARNING log; ephemeral store miss → silently recomputed (expected cross-session behavior)
- 17 test scenarios in `tests/test_core/function_pod/test_ephemeral_result.py`

## Test plan

- [ ] `uv run pytest tests/ -q` — all tests pass
- [ ] `test_ephemeral_result_written_to_memory_not_persistent_db` — result goes to ephemeral store only
- [ ] `test_within_session_ephemeral_hit` — second call does not recompute
- [ ] `test_cross_session_miss_recomputes` — fresh store triggers recompute
- [ ] `test_persistent_hit_served_when_ephemeral_true` — persistent cache still works
- [ ] `test_persistent_result_outcompetes_ephemeral` — persistent wins anti-join
- [ ] `test_persistent_miss_warns_and_recomputes` — WARNING log emitted
- [ ] `test_store_not_assigned_raises` — clear RuntimeError message

Fixes ITL-507
Related: ITL-508 (indexed recomputation for v0.2.0), ITL-509 (operator ephemeral support)
EOF
)"
```

---

## Implementation Notes

### NodeConfig access in `_process_data_internal`

The code reads `self._function_pod.config` to get `NodeConfig`. Verify this path first (Task 8, Step 4). The config is passed to `FunctionPod(pf, config=cfg)` at construction. If `FunctionPod.config` returns the `NodeConfig`, this is correct. If not, store `NodeConfig` at `FunctionJobNode.__init__` time:

```python
# In FunctionJobNode.__init__, after super().__init__:
self._node_config: NodeConfig = (
    function_pod.config if hasattr(function_pod, "config") and isinstance(function_pod.config, NodeConfig)
    else NodeConfig()
)
```

Then use `self._node_config.ephemeral_result` in `_process_data_internal`.

### Polars API compatibility

The plan uses `pl.from_arrow(table)` to create a Polars DataFrame from an Arrow table. This is the idiomatic Polars v1+ API. If the project uses an older Polars version, use `pl.DataFrame(table)` instead (same as the existing `_fetch_joined_records` code). Verify with:

```
uv run python -c "import polars as pl; print(pl.__version__)"
```

### Schema consistency in `_fetch_joined_records`

The anti-join + concat (`how="diagonal"`) handles the case where IS_EPHEMERAL_COL may differ between persistent and ephemeral rows (it will be `False` vs `True`). `how="diagonal"` pads missing columns with null. If both DataFrames always have IS_EPHEMERAL_COL (which they do since it's in taginfo), use `how="vertical"` instead (stricter, requires identical schemas).

### Backward compatibility of IS_EPHEMERAL_COL

Existing pipeline DBs written before this change will not have the IS_EPHEMERAL_COL column. The `_fetch_joined_records` implementation handles this with a fallback: if IS_EPHEMERAL_COL is absent from `taginfo.column_names`, all rows are treated as persistent. This ensures existing cached data remains accessible after the upgrade.
