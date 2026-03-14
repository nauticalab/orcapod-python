# Sync Pipeline Orchestrator Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement a synchronous pipeline orchestrator with per-packet observability, uniform compute/store node protocols, and Pipeline.run() integration.

**Architecture:** The orchestrator walks a compiled node graph topologically, maintaining materialized buffers between nodes. Each node type exposes a protocol (SourceNodeProtocol, FunctionNodeProtocol, OperatorNodeProtocol) with uniform `store_result` / `populate_cache` methods. TypeGuard functions provide runtime dispatch with static type narrowing. An ExecutionObserver protocol enables per-packet hooks via dependency injection.

**Tech Stack:** Python 3.12+, PyArrow, NetworkX, Polars (for DB joins), pytest + pytest-asyncio

**Spec:** `superpowers/specs/2026-03-14-sync-orchestrator-design.md`

---

## File Map

### New Files

| File | Responsibility |
|------|---------------|
| `src/orcapod/protocols/node_protocols.py` | SourceNodeProtocol, FunctionNodeProtocol, OperatorNodeProtocol, TypeGuard dispatch functions |
| `src/orcapod/pipeline/observer.py` | ExecutionObserver protocol, NoOpObserver default |
| `src/orcapod/pipeline/result.py` | OrchestratorResult dataclass |
| `src/orcapod/pipeline/sync_orchestrator.py` | SyncPipelineOrchestrator |
| `tests/test_pipeline/test_sync_orchestrator.py` | Orchestrator integration tests |
| `tests/test_pipeline/test_observer.py` | Observer hook tests |
| `tests/test_core/nodes/test_node_store_result.py` | store_result tests for all node types |
| `tests/test_core/nodes/test_node_populate_cache.py` | populate_cache tests for all node types |
| `tests/test_core/nodes/test_function_node_get_cached.py` | get_cached_results tests |

### Modified Files

| File | Changes |
|------|---------|
| `src/orcapod/core/nodes/source_node.py` | Add `store_result()`, `populate_cache()`, modify `iter_packets()` for cache check |
| `src/orcapod/core/nodes/operator_node.py` | Add `get_cached_output()`, `store_result()`, `populate_cache()` |
| `src/orcapod/core/nodes/function_node.py` | Extract pipeline record from `process_packet()` into `store_result()`, add `get_cached_results()`, `populate_cache()` |
| `src/orcapod/core/nodes/__init__.py` | Re-export GraphNode (no change needed, already correct) |
| `src/orcapod/pipeline/graph.py` | Update `Pipeline.run()`, add `_apply_results()` |
| `src/orcapod/pipeline/__init__.py` | Export SyncPipelineOrchestrator, update AsyncPipelineOrchestrator import |
| `src/orcapod/pipeline/orchestrator.py` | Rename to `async_orchestrator.py` |

---

## Chunk 1: Protocols, Observer, and Result Types

### Task 1: Node Protocols and TypeGuard Dispatch

**Files:**
- Create: `src/orcapod/protocols/node_protocols.py`
- Create: `tests/test_protocols/test_node_protocols.py`

- [ ] **Step 1: Write tests for TypeGuard dispatch**

```python
# tests/test_protocols/test_node_protocols.py
"""Tests for node protocol TypeGuard dispatch functions."""
from __future__ import annotations

import pytest

from orcapod.core.nodes import FunctionNode, OperatorNode, SourceNode
from orcapod.protocols.node_protocols import (
    is_function_node,
    is_operator_node,
    is_source_node,
)


class TestTypeGuardDispatch:
    """TypeGuard functions correctly narrow node types."""

    def test_is_source_node_true(self, source_node):
        assert is_source_node(source_node) is True

    def test_is_source_node_false_for_function(self, function_node):
        assert is_source_node(function_node) is False

    def test_is_function_node_true(self, function_node):
        assert is_function_node(function_node) is True

    def test_is_function_node_false_for_operator(self, operator_node):
        assert is_function_node(operator_node) is False

    def test_is_operator_node_true(self, operator_node):
        assert is_operator_node(operator_node) is True

    def test_is_operator_node_false_for_source(self, source_node):
        assert is_operator_node(source_node) is False


# --- Fixtures ---

@pytest.fixture
def _sample_source():
    import pyarrow as pa
    from orcapod.core.sources import ArrowTableSource

    table = pa.table({
        "key": pa.array(["a", "b"], type=pa.large_string()),
        "value": pa.array([1, 2], type=pa.int64()),
    })
    return ArrowTableSource(table, tag_columns=["key"])


@pytest.fixture
def source_node(_sample_source):
    return SourceNode(_sample_source)


@pytest.fixture
def function_node(_sample_source):
    from orcapod.core.function_pod import FunctionPod
    from orcapod.core.packet_function import PythonPacketFunction

    pf = PythonPacketFunction(lambda value: value * 2, output_keys="result")
    pod = FunctionPod(pf)
    return FunctionNode(pod, _sample_source)


@pytest.fixture
def operator_node(_sample_source):
    from orcapod.core.operators import SelectPacketColumns

    op = SelectPacketColumns(columns=["value"])
    return OperatorNode(op, input_streams=[_sample_source])
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_protocols/test_node_protocols.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'orcapod.protocols.node_protocols'`

- [ ] **Step 3: Implement node protocols and TypeGuard functions**

```python
# src/orcapod/protocols/node_protocols.py
"""Node protocols for orchestrator interaction.

Defines the three node protocols (Source, Function, Operator) that
formalize the interface between orchestrators and graph nodes, plus
TypeGuard dispatch functions for runtime type narrowing.
"""
from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING, Protocol, TypeGuard, runtime_checkable

if TYPE_CHECKING:
    from orcapod.core.nodes import GraphNode
    from orcapod.protocols.core_protocols import (
        PacketProtocol,
        StreamProtocol,
        TagProtocol,
    )
    from orcapod.protocols.core_protocols.operator_pod import OperatorPodProtocol


@runtime_checkable
class SourceNodeProtocol(Protocol):
    """Protocol for source nodes in orchestrated execution."""

    node_type: str

    def iter_packets(self) -> Iterator[tuple["TagProtocol", "PacketProtocol"]]: ...
    def store_result(
        self, results: list[tuple["TagProtocol", "PacketProtocol"]]
    ) -> None: ...
    def populate_cache(
        self, results: list[tuple["TagProtocol", "PacketProtocol"]]
    ) -> None: ...


@runtime_checkable
class FunctionNodeProtocol(Protocol):
    """Protocol for function nodes in orchestrated execution."""

    node_type: str

    def get_cached_results(
        self, entry_ids: list[str]
    ) -> dict[str, tuple["TagProtocol", "PacketProtocol"]]: ...

    def compute_pipeline_entry_id(
        self, tag: "TagProtocol", packet: "PacketProtocol"
    ) -> str: ...

    def process_packet(
        self, tag: "TagProtocol", packet: "PacketProtocol"
    ) -> tuple["TagProtocol", "PacketProtocol | None"]: ...

    def store_result(
        self,
        tag: "TagProtocol",
        input_packet: "PacketProtocol",
        output_packet: "PacketProtocol | None",
    ) -> None: ...

    def populate_cache(
        self, results: list[tuple["TagProtocol", "PacketProtocol"]]
    ) -> None: ...


@runtime_checkable
class OperatorNodeProtocol(Protocol):
    """Protocol for operator nodes in orchestrated execution."""

    node_type: str

    @property
    def operator(self) -> "OperatorPodProtocol": ...

    def get_cached_output(self) -> "StreamProtocol | None": ...
    def store_result(
        self, results: list[tuple["TagProtocol", "PacketProtocol"]]
    ) -> None: ...
    def populate_cache(
        self, results: list[tuple["TagProtocol", "PacketProtocol"]]
    ) -> None: ...


def is_source_node(node: "GraphNode") -> TypeGuard[SourceNodeProtocol]:
    """Check if a node is a source node."""
    return node.node_type == "source"


def is_function_node(node: "GraphNode") -> TypeGuard[FunctionNodeProtocol]:
    """Check if a node is a function node."""
    return node.node_type == "function"


def is_operator_node(node: "GraphNode") -> TypeGuard[OperatorNodeProtocol]:
    """Check if a node is an operator node."""
    return node.node_type == "operator"
```

Note: `OperatorNode` currently stores the operator as `self._operator` (private). Task 5
adds a public `operator` property. The protocol declares the public property so the
orchestrator uses `node.operator.process(...)` consistently.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_protocols/test_node_protocols.py -v`
Expected: PASS (all 6 tests)

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/protocols/node_protocols.py tests/test_protocols/test_node_protocols.py
git commit -m "feat(protocols): add node protocols and TypeGuard dispatch for orchestrator"
```

### Task 2: ExecutionObserver Protocol and NoOpObserver

**Files:**
- Create: `src/orcapod/pipeline/observer.py`
- Create: `tests/test_pipeline/test_observer.py`

- [ ] **Step 1: Write tests for NoOpObserver**

```python
# tests/test_pipeline/test_observer.py
"""Tests for ExecutionObserver protocol and NoOpObserver."""
from __future__ import annotations

from orcapod.pipeline.observer import ExecutionObserver, NoOpObserver


class TestNoOpObserver:
    """NoOpObserver satisfies the protocol and does nothing."""

    def test_satisfies_protocol(self):
        observer = NoOpObserver()
        assert isinstance(observer, ExecutionObserver)

    def test_on_node_start_noop(self):
        observer = NoOpObserver()
        observer.on_node_start(None)  # type: ignore[arg-type]

    def test_on_node_end_noop(self):
        observer = NoOpObserver()
        observer.on_node_end(None)  # type: ignore[arg-type]

    def test_on_packet_start_noop(self):
        observer = NoOpObserver()
        observer.on_packet_start(None, None, None)  # type: ignore[arg-type]

    def test_on_packet_end_noop(self):
        observer = NoOpObserver()
        observer.on_packet_end(None, None, None, None, cached=False)  # type: ignore[arg-type]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_pipeline/test_observer.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement observer**

```python
# src/orcapod/pipeline/observer.py
"""Execution observer protocol for pipeline orchestration.

Provides hooks for monitoring node and packet-level execution events
during orchestrated pipeline runs.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from orcapod.core.nodes import GraphNode
    from orcapod.protocols.core_protocols import PacketProtocol, TagProtocol


@runtime_checkable
class ExecutionObserver(Protocol):
    """Observer protocol for pipeline execution events.

    ``on_packet_start`` / ``on_packet_end`` are only invoked for function
    nodes. ``on_node_start`` / ``on_node_end`` are invoked for all node
    types.
    """

    def on_node_start(self, node: "GraphNode") -> None: ...
    def on_node_end(self, node: "GraphNode") -> None: ...
    def on_packet_start(
        self,
        node: "GraphNode",
        tag: "TagProtocol",
        packet: "PacketProtocol",
    ) -> None: ...
    def on_packet_end(
        self,
        node: "GraphNode",
        tag: "TagProtocol",
        input_packet: "PacketProtocol",
        output_packet: "PacketProtocol | None",
        cached: bool,
    ) -> None: ...


class NoOpObserver:
    """Default observer that does nothing."""

    def on_node_start(self, node: "GraphNode") -> None:
        pass

    def on_node_end(self, node: "GraphNode") -> None:
        pass

    def on_packet_start(
        self,
        node: "GraphNode",
        tag: "TagProtocol",
        packet: "PacketProtocol",
    ) -> None:
        pass

    def on_packet_end(
        self,
        node: "GraphNode",
        tag: "TagProtocol",
        input_packet: "PacketProtocol",
        output_packet: "PacketProtocol | None",
        cached: bool,
    ) -> None:
        pass
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_pipeline/test_observer.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/pipeline/observer.py tests/test_pipeline/test_observer.py
git commit -m "feat(pipeline): add ExecutionObserver protocol and NoOpObserver"
```

### Task 3: OrchestratorResult Dataclass

**Files:**
- Create: `src/orcapod/pipeline/result.py`

- [ ] **Step 1: Create the result dataclass**

```python
# src/orcapod/pipeline/result.py
"""Result type returned by pipeline orchestrators."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from orcapod.protocols.core_protocols import PacketProtocol, TagProtocol


@dataclass
class OrchestratorResult:
    """Result of an orchestrator run.

    Attributes:
        node_outputs: Mapping from graph node to its computed (tag, packet)
            pairs. Empty when ``materialize_results=False``.
    """

    node_outputs: dict[Any, list[tuple["TagProtocol", "PacketProtocol"]]] = field(
        default_factory=dict
    )
```

- [ ] **Step 2: Commit**

```bash
git add src/orcapod/pipeline/result.py
git commit -m "feat(pipeline): add OrchestratorResult dataclass"
```

---

## Chunk 2: Node Refactoring — SourceNode and OperatorNode

### Task 4: SourceNode — store_result and populate_cache

**Files:**
- Modify: `src/orcapod/core/nodes/source_node.py`
- Create: `tests/test_core/nodes/test_node_populate_cache.py`
- Create: `tests/test_core/nodes/test_node_store_result.py`

- [ ] **Step 1: Write tests for SourceNode.populate_cache**

```python
# tests/test_core/nodes/test_node_populate_cache.py
"""Tests for populate_cache on all node types."""
from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.core.nodes import SourceNode
from orcapod.core.sources import ArrowTableSource


@pytest.fixture
def source_and_node():
    table = pa.table({
        "key": pa.array(["a", "b"], type=pa.large_string()),
        "value": pa.array([1, 2], type=pa.int64()),
    })
    src = ArrowTableSource(table, tag_columns=["key"])
    node = SourceNode(src)
    return src, node


class TestSourceNodePopulateCache:
    def test_iter_packets_uses_cache_when_populated(self, source_and_node):
        src, node = source_and_node
        original = list(node.iter_packets())
        assert len(original) == 2

        # Populate cache with only the first packet
        node.populate_cache([original[0]])

        cached = list(node.iter_packets())
        assert len(cached) == 1

    def test_iter_packets_delegates_to_stream_when_no_cache(self, source_and_node):
        _, node = source_and_node
        result = list(node.iter_packets())
        assert len(result) == 2
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_core/nodes/test_node_populate_cache.py::TestSourceNodePopulateCache -v`
Expected: FAIL — `AttributeError: 'SourceNode' object has no attribute 'populate_cache'`

- [ ] **Step 3: Implement SourceNode.populate_cache and modify iter_packets**

In `src/orcapod/core/nodes/source_node.py`, add `_cached_results` field in `__init__`,
modify `iter_packets()` to check cache, and add `populate_cache()`:

In `__init__` after `self.stream = stream`:
```python
self._cached_results: list[tuple[cp.TagProtocol, cp.PacketProtocol]] | None = None
```

Replace `iter_packets`:
```python
def iter_packets(self) -> Iterator[tuple[cp.TagProtocol, cp.PacketProtocol]]:
    if self.stream is None:
        raise RuntimeError(
            "SourceNode in read-only mode has no stream data available"
        )
    if self._cached_results is not None:
        return iter(self._cached_results)
    return self.stream.iter_packets()
```

Add new method:
```python
def populate_cache(
    self, results: list[tuple[cp.TagProtocol, cp.PacketProtocol]]
) -> None:
    """Populate the in-memory cache with externally-provided results.

    After calling this, ``iter_packets()`` returns from the cache
    instead of delegating to the wrapped stream.
    """
    self._cached_results = list(results)
```

Also add `_cached_results = None` in the `from_descriptor` read-only path.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_core/nodes/test_node_populate_cache.py::TestSourceNodePopulateCache -v`
Expected: PASS

- [ ] **Step 5: Write tests for SourceNode.store_result**

```python
# tests/test_core/nodes/test_node_store_result.py
"""Tests for store_result on all node types."""
from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.core.nodes import SourceNode
from orcapod.core.sources import ArrowTableSource


@pytest.fixture
def source_and_node():
    table = pa.table({
        "key": pa.array(["a", "b"], type=pa.large_string()),
        "value": pa.array([1, 2], type=pa.int64()),
    })
    src = ArrowTableSource(table, tag_columns=["key"])
    node = SourceNode(src)
    return src, node


class TestSourceNodeStoreResult:
    def test_store_result_noop_without_db(self, source_and_node):
        """store_result should be a no-op when no DB is configured."""
        _, node = source_and_node
        packets = list(node.iter_packets())
        # Should not raise
        node.store_result(packets)
```

- [ ] **Step 6: Run test to verify it fails**

Run: `uv run pytest tests/test_core/nodes/test_node_store_result.py::TestSourceNodeStoreResult -v`
Expected: FAIL — `AttributeError: 'SourceNode' object has no attribute 'store_result'`

- [ ] **Step 7: Implement SourceNode.store_result**

In `src/orcapod/core/nodes/source_node.py`:

```python
def store_result(
    self, results: list[tuple[cp.TagProtocol, cp.PacketProtocol]]
) -> None:
    """Persist source data snapshot to the pipeline DB if configured.

    Currently a no-op. Future implementations may store a snapshot of
    what the pipeline consumed from this source.
    """
    pass
```

- [ ] **Step 8: Run tests to verify they pass**

Run: `uv run pytest tests/test_core/nodes/test_node_store_result.py::TestSourceNodeStoreResult tests/test_core/nodes/test_node_populate_cache.py::TestSourceNodePopulateCache -v`
Expected: PASS

- [ ] **Step 9: Run existing SourceNode tests to verify no regressions**

Run: `uv run pytest tests/test_core/sources/ tests/test_pipeline/ -v`
Expected: All existing tests PASS

- [ ] **Step 10: Commit**

```bash
git add src/orcapod/core/nodes/source_node.py tests/test_core/nodes/test_node_populate_cache.py tests/test_core/nodes/test_node_store_result.py
git commit -m "feat(source-node): add store_result and populate_cache for orchestrator support"
```

### Task 5: OperatorNode — get_cached_output, store_result, populate_cache

**Files:**
- Modify: `src/orcapod/core/nodes/operator_node.py`
- Modify: `tests/test_core/nodes/test_node_populate_cache.py`
- Modify: `tests/test_core/nodes/test_node_store_result.py`

- [ ] **Step 1: Write tests for OperatorNode.populate_cache**

Append to `tests/test_core/nodes/test_node_populate_cache.py`:

```python
from orcapod.core.nodes import OperatorNode
from orcapod.core.operators import SelectPacketColumns


@pytest.fixture
def operator_node_with_data():
    table = pa.table({
        "key": pa.array(["a", "b"], type=pa.large_string()),
        "value": pa.array([10, 20], type=pa.int64()),
    })
    src = ArrowTableSource(table, tag_columns=["key"])
    op = SelectPacketColumns(columns=["value"])
    node = OperatorNode(op, input_streams=[src])
    return node


class TestOperatorNodePopulateCache:
    def test_iter_packets_uses_cache_when_populated(self, operator_node_with_data):
        node = operator_node_with_data
        # Run normally first to get output
        node.run()
        original = list(node.iter_packets())
        assert len(original) == 2

        # Clear cache, then populate with subset
        node.clear_cache()
        node.populate_cache([original[0]])
        cached = list(node.iter_packets())
        assert len(cached) == 1
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_core/nodes/test_node_populate_cache.py::TestOperatorNodePopulateCache -v`
Expected: FAIL — `AttributeError: 'OperatorNode' object has no attribute 'populate_cache'`

- [ ] **Step 3: Write tests for OperatorNode.store_result and get_cached_output**

Append to `tests/test_core/nodes/test_node_store_result.py`:

```python
from orcapod.core.nodes import OperatorNode
from orcapod.core.operators import SelectPacketColumns
from orcapod.databases import InMemoryArrowDatabase
from orcapod.types import CacheMode


@pytest.fixture
def operator_with_db():
    table = pa.table({
        "key": pa.array(["a", "b"], type=pa.large_string()),
        "value": pa.array([10, 20], type=pa.int64()),
    })
    src = ArrowTableSource(table, tag_columns=["key"])
    op = SelectPacketColumns(columns=["value"])
    db = InMemoryArrowDatabase()
    node = OperatorNode(
        op, input_streams=[src],
        pipeline_database=db,
        cache_mode=CacheMode.LOG,
    )
    return node, db


@pytest.fixture
def operator_no_db():
    table = pa.table({
        "key": pa.array(["a", "b"], type=pa.large_string()),
        "value": pa.array([10, 20], type=pa.int64()),
    })
    src = ArrowTableSource(table, tag_columns=["key"])
    op = SelectPacketColumns(columns=["value"])
    return OperatorNode(op, input_streams=[src])


class TestOperatorNodeStoreResult:
    def test_store_result_writes_to_db_in_log_mode(self, operator_with_db):
        node, db = operator_with_db
        # Compute via operator directly (not through node.run)
        stream = node._operator.process(*node._input_streams)
        output = list(stream.iter_packets())
        node.store_result(output)

        records = node.get_all_records()
        assert records is not None
        assert records.num_rows == 2

    def test_store_result_noop_in_off_mode(self, operator_no_db):
        node = operator_no_db
        stream = node._operator.process(*node._input_streams)
        output = list(stream.iter_packets())
        # Should not raise
        node.store_result(output)

    def test_get_cached_output_returns_none_in_off_mode(self, operator_no_db):
        assert operator_no_db.get_cached_output() is None

    def test_get_cached_output_returns_none_in_log_mode(self, operator_with_db):
        node, _ = operator_with_db
        assert node.get_cached_output() is None

    def test_get_cached_output_returns_stream_in_replay_mode(self, operator_with_db):
        node, db = operator_with_db
        # First store some results
        stream = node._operator.process(*node._input_streams)
        output = list(stream.iter_packets())
        node.store_result(output)

        # Switch to REPLAY mode
        node._cache_mode = CacheMode.REPLAY
        cached = node.get_cached_output()
        assert cached is not None
        cached_packets = list(cached.iter_packets())
        assert len(cached_packets) == 2
```

- [ ] **Step 4: Run tests to verify they fail**

Run: `uv run pytest tests/test_core/nodes/test_node_store_result.py::TestOperatorNodeStoreResult -v`
Expected: FAIL — `AttributeError`

- [ ] **Step 5: Implement OperatorNode methods**

In `src/orcapod/core/nodes/operator_node.py`, add three new methods.

Add a public `operator` property (if not already present):
```python
@property
def operator(self) -> OperatorPodProtocol:
    """Return the wrapped operator pod."""
    return self._operator
```

Add `get_cached_output`:
```python
def get_cached_output(self) -> "StreamProtocol | None":
    """Return cached output stream in REPLAY mode, else None.

    Returns:
        The cached stream if REPLAY mode and DB records exist,
        otherwise None.
    """
    if self._pipeline_database is None:
        return None
    if self._cache_mode != CacheMode.REPLAY:
        return None
    self._replay_from_cache()
    return self._cached_output_stream
```

Add `store_result`:
```python
def store_result(
    self,
    results: "list[tuple[TagProtocol, PacketProtocol]]",
) -> None:
    """Persist computed results to the pipeline DB.

    Wraps the materialized results as an ArrowTableStream and stores
    via the existing ``_store_output_stream`` logic. No-op if no DB
    is attached or cache mode is OFF.

    Args:
        results: Materialized (tag, packet) pairs from computation.
    """
    if self._pipeline_database is None:
        return
    if self._cache_mode == CacheMode.OFF:
        return

    from orcapod.core.operators.static_output_pod import StaticOutputOperatorPod

    stream = StaticOutputOperatorPod._materialize_to_stream(results)
    self._store_output_stream(stream)
```

Add `populate_cache`:
```python
def populate_cache(
    self,
    results: "list[tuple[TagProtocol, PacketProtocol]]",
) -> None:
    """Populate in-memory cache from externally-provided results.

    After calling this, ``iter_packets()`` / ``as_table()`` return
    from the cache without recomputation. Empty lists clear the cache
    and set an empty stream.

    Args:
        results: Materialized (tag, packet) pairs.
    """
    if not results:
        self._cached_output_stream = None
        self._cached_output_table = None
        self._update_modified_time()
        return

    from orcapod.core.operators.static_output_pod import StaticOutputOperatorPod

    self._cached_output_stream = StaticOutputOperatorPod._materialize_to_stream(
        results
    )
    self._update_modified_time()
```

Note: `StaticOutputOperatorPod._materialize_to_stream` raises `ValueError` on empty lists,
so `populate_cache` handles the empty case explicitly. The method exists as a static helper
on the base operator class (verified at `static_output_pod.py:195`).

- [ ] **Step 6: Run tests to verify they pass**

Run: `uv run pytest tests/test_core/nodes/test_node_store_result.py::TestOperatorNodeStoreResult tests/test_core/nodes/test_node_populate_cache.py::TestOperatorNodePopulateCache -v`
Expected: PASS

- [ ] **Step 7: Run existing OperatorNode tests to verify no regressions**

Run: `uv run pytest tests/test_core/operators/ tests/test_pipeline/ -v`
Expected: All existing tests PASS

- [ ] **Step 8: Commit**

```bash
git add src/orcapod/core/nodes/operator_node.py tests/test_core/nodes/test_node_populate_cache.py tests/test_core/nodes/test_node_store_result.py
git commit -m "feat(operator-node): add get_cached_output, store_result, populate_cache"
```

---

## Chunk 3: FunctionNode Refactoring

### Task 6: FunctionNode — Pure process_packet, store_result, get_cached_results, populate_cache

The current `FunctionNode.process_packet()` bundles computation (via CachedFunctionPod)
with pipeline record writing (via `add_pipeline_record`). We need to:

1. Extract pipeline record writing from `process_packet` into `store_result`
2. Keep `process_packet` handling computation + function-level memoization (CachedFunctionPod)
3. Add `get_cached_results` factored out of `iter_packets()` Phase 1
4. Add `populate_cache`
5. Keep existing `iter_packets()` and `run()` working

**Key insight:** `process_packet` is NOT pure — it writes to the result DB via
CachedFunctionPod. But that's function-level memoization (the function pod's own concern).
What moves to `store_result` is ONLY the pipeline provenance record
(`add_pipeline_record`). This keeps a clean boundary: function-level concerns in
`process_packet`, pipeline-level concerns in `store_result`.

**Backward compatibility:** The existing `iter_packets()` calls `process_packet` then
`add_pipeline_record` inline. After this refactoring, we rename the original bundled
method to `_process_and_store_packet` and update all call sites in `iter_packets()`,
`_iter_packets_sequential()`, and `_iter_packets_concurrent()`.

**Files:**
- Modify: `src/orcapod/core/nodes/function_node.py`
- Create: `tests/test_core/nodes/test_function_node_get_cached.py`
- Modify: `tests/test_core/nodes/test_node_populate_cache.py`
- Modify: `tests/test_core/nodes/test_node_store_result.py`

- [ ] **Step 1: Write tests for FunctionNode.process_packet (no pipeline record) and store_result**

```python
# Append to tests/test_core/nodes/test_node_store_result.py

from orcapod.core.function_pod import FunctionPod
from orcapod.core.nodes import FunctionNode
from orcapod.core.packet_function import PythonPacketFunction


def double_value(value: int) -> int:
    return value * 2


@pytest.fixture
def function_node_with_db():
    table = pa.table({
        "key": pa.array(["a", "b"], type=pa.large_string()),
        "value": pa.array([1, 2], type=pa.int64()),
    })
    src = ArrowTableSource(table, tag_columns=["key"])
    pf = PythonPacketFunction(double_value, output_keys="result")
    pod = FunctionPod(pf)
    pipeline_db = InMemoryArrowDatabase()
    result_db = InMemoryArrowDatabase()
    node = FunctionNode(
        pod, src,
        pipeline_database=pipeline_db,
        result_database=result_db,
    )
    return node, pipeline_db, result_db


@pytest.fixture
def function_node_no_db():
    table = pa.table({
        "key": pa.array(["a", "b"], type=pa.large_string()),
        "value": pa.array([1, 2], type=pa.int64()),
    })
    src = ArrowTableSource(table, tag_columns=["key"])
    pf = PythonPacketFunction(double_value, output_keys="result")
    pod = FunctionPod(pf)
    return FunctionNode(pod, src)


class TestFunctionNodeProcessPacket:
    def test_process_packet_does_not_write_pipeline_record(self, function_node_with_db):
        """process_packet handles computation but NOT pipeline provenance."""
        node, pipeline_db, _ = function_node_with_db
        packets = list(node._input_stream.iter_packets())
        tag, packet = packets[0]

        tag_out, result = node.process_packet(tag, packet)
        assert result is not None

        # No pipeline record should exist (only store_result writes those)
        records = pipeline_db.get_all_records(node.pipeline_path)
        assert records is None

    def test_process_packet_writes_to_result_db(self, function_node_with_db):
        """process_packet handles function-level memoization via CachedFunctionPod."""
        node, _, result_db = function_node_with_db
        packets = list(node._input_stream.iter_packets())
        tag, packet = packets[0]

        tag_out, result = node.process_packet(tag, packet)
        assert result is not None

        # Result DB should have the cached computation
        cached_results = node._cached_function_pod.get_all_cached_outputs()
        assert cached_results is not None
        assert cached_results.num_rows == 1

    def test_process_packet_returns_correct_result(self, function_node_no_db):
        node = function_node_no_db
        packets = list(node._input_stream.iter_packets())
        tag, packet = packets[0]

        tag_out, result = node.process_packet(tag, packet)
        assert result is not None
        assert result.as_dict()["result"] == 2  # double of 1


class TestFunctionNodeStoreResult:
    def test_store_result_writes_pipeline_record(self, function_node_with_db):
        node, pipeline_db, _ = function_node_with_db
        packets = list(node._input_stream.iter_packets())
        tag, packet = packets[0]

        tag_out, result = node.process_packet(tag, packet)
        node.store_result(tag, packet, result)

        records = pipeline_db.get_all_records(node.pipeline_path)
        assert records is not None
        assert records.num_rows == 1

    def test_store_result_does_not_write_to_result_db(self, function_node_with_db):
        """store_result only writes pipeline records, not result cache."""
        node, _, result_db = function_node_with_db
        packets = list(node._input_stream.iter_packets())
        tag, packet = packets[0]

        # process_packet first (writes to result DB)
        tag_out, result = node.process_packet(tag, packet)
        cached_before = node._cached_function_pod.get_all_cached_outputs()
        count_before = cached_before.num_rows if cached_before is not None else 0

        # store_result should NOT add more to result DB
        node.store_result(tag, packet, result)
        cached_after = node._cached_function_pod.get_all_cached_outputs()
        count_after = cached_after.num_rows if cached_after is not None else 0
        assert count_after == count_before

    def test_store_result_noop_without_db(self, function_node_no_db):
        node = function_node_no_db
        packets = list(node._input_stream.iter_packets())
        tag, packet = packets[0]

        tag_out, result = node.process_packet(tag, packet)
        # Should not raise
        node.store_result(tag, packet, result)

    def test_store_result_handles_none_output(self, function_node_with_db):
        node, pipeline_db, _ = function_node_with_db
        packets = list(node._input_stream.iter_packets())
        tag, packet = packets[0]

        # Should not raise; should be a no-op for None output
        node.store_result(tag, packet, None)
        records = pipeline_db.get_all_records(node.pipeline_path)
        assert records is None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_core/nodes/test_node_store_result.py::TestFunctionNodeProcessPacket tests/test_core/nodes/test_node_store_result.py::TestFunctionNodeStoreResult -v`
Expected: FAIL — first test fails because `process_packet` still writes pipeline records

- [ ] **Step 3: Refactor FunctionNode.process_packet and add store_result**

In `src/orcapod/core/nodes/function_node.py`:

1. Rename current `process_packet` to `_process_and_store_packet` (used by existing
   `iter_packets()` for backward compatibility — keeps bundled compute+pipeline record).

2. Create new `process_packet` that handles computation + function-level memoization
   but NOT pipeline records:

```python
def process_packet(
    self,
    tag: TagProtocol,
    packet: PacketProtocol,
) -> tuple[TagProtocol, PacketProtocol | None]:
    """Process a single packet with function-level memoization.

    Delegates to ``CachedFunctionPod`` (when DB is attached) for
    computation and result-level caching, or to the raw ``FunctionPod``
    otherwise. Does NOT write pipeline provenance records — use
    ``store_result`` for that.

    Args:
        tag: The tag associated with the packet.
        packet: The input packet to process.

    Returns:
        A ``(tag, output_packet)`` tuple; output_packet is ``None`` if
        the function filters the packet out.
    """
    if self._cached_function_pod is not None:
        return self._cached_function_pod.process_packet(tag, packet)
    return self._function_pod.process_packet(tag, packet)
```

3. Add `store_result` (pipeline provenance only):

```python
def store_result(
    self,
    tag: TagProtocol,
    input_packet: PacketProtocol,
    output_packet: PacketProtocol | None,
) -> None:
    """Record pipeline provenance for a processed packet.

    Writes a pipeline record associating this (tag + system_tags +
    input_packet) with the output packet record ID. Does NOT write
    to the result DB — that is handled by ``process_packet`` via
    ``CachedFunctionPod``.

    No-op if no pipeline DB is attached or output is None.

    Args:
        tag: The tag associated with the packet.
        input_packet: The original input packet.
        output_packet: The computation result, or None if filtered.
    """
    if output_packet is None:
        return
    if self._pipeline_database is None:
        return

    result_computed = True
    if self._cached_function_pod is not None:
        result_computed = bool(
            output_packet.get_meta_value(
                self._cached_function_pod.RESULT_COMPUTED_FLAG, True
            )
        )

    self.add_pipeline_record(
        tag,
        input_packet,
        packet_record_id=output_packet.datagram_id,
        computed=result_computed,
    )
```

4. Update `iter_packets()` to call `_process_and_store_packet` instead of `process_packet`
   in Phase 2 (around line 787). Also update `_iter_packets_sequential` (line 826) **and
   `_iter_packets_concurrent` (line 854)** — all three call sites must use the bundled
   version for backward compatibility.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_core/nodes/test_node_store_result.py::TestFunctionNodeProcessPacket tests/test_core/nodes/test_node_store_result.py::TestFunctionNodeStoreResult -v`
Expected: PASS

- [ ] **Step 5: Run ALL existing FunctionNode tests to verify no regressions**

Run: `uv run pytest tests/test_core/function_pod/ tests/test_pipeline/ -v`
Expected: All existing tests PASS. The `iter_packets()` path uses `_process_and_store_packet`
which preserves the original bundled behavior.

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/core/nodes/function_node.py tests/test_core/nodes/test_node_store_result.py
git commit -m "refactor(function-node): separate pure process_packet from store_result"
```

- [ ] **Step 7: Write tests for get_cached_results**

```python
# tests/test_core/nodes/test_function_node_get_cached.py
"""Tests for FunctionNode.get_cached_results."""
from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.core.function_pod import FunctionPod
from orcapod.core.nodes import FunctionNode
from orcapod.core.packet_function import PythonPacketFunction
from orcapod.core.sources import ArrowTableSource
from orcapod.databases import InMemoryArrowDatabase


def double_value(value: int) -> int:
    return value * 2


@pytest.fixture
def function_node_with_db():
    table = pa.table({
        "key": pa.array(["a", "b"], type=pa.large_string()),
        "value": pa.array([1, 2], type=pa.int64()),
    })
    src = ArrowTableSource(table, tag_columns=["key"])
    pf = PythonPacketFunction(double_value, output_keys="result")
    pod = FunctionPod(pf)
    pipeline_db = InMemoryArrowDatabase()
    result_db = InMemoryArrowDatabase()
    node = FunctionNode(
        pod, src,
        pipeline_database=pipeline_db,
        result_database=result_db,
    )
    return node


class TestGetCachedResults:
    def test_returns_empty_dict_when_no_db(self):
        table = pa.table({
            "key": pa.array(["a"], type=pa.large_string()),
            "value": pa.array([1], type=pa.int64()),
        })
        src = ArrowTableSource(table, tag_columns=["key"])
        pf = PythonPacketFunction(double_value, output_keys="result")
        pod = FunctionPod(pf)
        node = FunctionNode(pod, src)

        result = node.get_cached_results([])
        assert result == {}

    def test_returns_empty_dict_when_db_empty(self, function_node_with_db):
        result = function_node_with_db.get_cached_results(["nonexistent"])
        assert result == {}

    def test_returns_cached_results_for_matching_entry_ids(self, function_node_with_db):
        node = function_node_with_db
        packets = list(node._input_stream.iter_packets())

        # Process and store two packets
        entry_ids = []
        for tag, packet in packets:
            tag_out, result = node.process_packet(tag, packet)
            node.store_result(tag, packet, result)
            entry_ids.append(node.compute_pipeline_entry_id(tag, packet))

        # Retrieve cached results for both
        cached = node.get_cached_results(entry_ids)
        assert len(cached) == 2
        assert all(eid in cached for eid in entry_ids)

    def test_filters_to_requested_entry_ids_only(self, function_node_with_db):
        node = function_node_with_db
        packets = list(node._input_stream.iter_packets())

        entry_ids = []
        for tag, packet in packets:
            tag_out, result = node.process_packet(tag, packet)
            node.store_result(tag, packet, result)
            entry_ids.append(node.compute_pipeline_entry_id(tag, packet))

        # Request only the first entry ID
        cached = node.get_cached_results([entry_ids[0]])
        assert len(cached) == 1
        assert entry_ids[0] in cached
        assert entry_ids[1] not in cached
```

- [ ] **Step 8: Run tests to verify they fail**

Run: `uv run pytest tests/test_core/nodes/test_function_node_get_cached.py -v`
Expected: FAIL — `AttributeError: 'FunctionNode' object has no attribute 'get_cached_results'`

- [ ] **Step 9: Implement get_cached_results**

In `src/orcapod/core/nodes/function_node.py`:

```python
def get_cached_results(
    self, entry_ids: list[str]
) -> dict[str, tuple[TagProtocol, PacketProtocol]]:
    """Retrieve cached results for specific pipeline entry IDs.

    Looks up the pipeline DB and result DB, joins them, and filters
    to the requested entry IDs. Returns a mapping from entry ID to
    (tag, output_packet).

    Args:
        entry_ids: Pipeline entry IDs to look up.

    Returns:
        Mapping from entry_id to (tag, output_packet) for found entries.
        Empty dict if no DB is attached or no matches found.
    """
    if self._cached_function_pod is None or not entry_ids:
        return {}

    PIPELINE_ENTRY_ID_COL = "__pipeline_entry_id"
    entry_id_set = set(entry_ids)

    taginfo = self._pipeline_database.get_all_records(
        self.pipeline_path,
        record_id_column=PIPELINE_ENTRY_ID_COL,
    )
    results = self._cached_function_pod._result_database.get_all_records(
        self._cached_function_pod.record_path,
        record_id_column=constants.PACKET_RECORD_ID,
    )

    if taginfo is None or results is None:
        return {}

    joined = (
        pl.DataFrame(taginfo)
        .join(
            pl.DataFrame(results),
            on=constants.PACKET_RECORD_ID,
            how="inner",
        )
        .to_arrow()
    )

    if joined.num_rows == 0:
        return {}

    # Filter to requested entry IDs
    all_entry_ids = joined.column(PIPELINE_ENTRY_ID_COL).to_pylist()
    mask = [eid in entry_id_set for eid in all_entry_ids]
    filtered = joined.filter(pa.array(mask))

    if filtered.num_rows == 0:
        return {}

    tag_keys = self._input_stream.keys()[0]
    drop_cols = [
        c for c in filtered.column_names
        if c.startswith(constants.META_PREFIX) or c == PIPELINE_ENTRY_ID_COL
    ]
    data_table = filtered.drop(
        [c for c in drop_cols if c in filtered.column_names]
    )

    from orcapod.core.streams.arrow_table_stream import ArrowTableStream

    stream = ArrowTableStream(data_table, tag_columns=tag_keys)
    filtered_entry_ids = [eid for eid, m in zip(all_entry_ids, mask) if m]

    result_dict: dict[str, tuple[TagProtocol, PacketProtocol]] = {}
    for entry_id, (tag, packet) in zip(
        filtered_entry_ids, stream.iter_packets()
    ):
        result_dict[entry_id] = (tag, packet)

    return result_dict
```

- [ ] **Step 10: Run tests to verify they pass**

Run: `uv run pytest tests/test_core/nodes/test_function_node_get_cached.py -v`
Expected: PASS

- [ ] **Step 11: Write tests for FunctionNode.populate_cache**

Append to `tests/test_core/nodes/test_node_populate_cache.py`:

```python
from orcapod.core.function_pod import FunctionPod
from orcapod.core.nodes import FunctionNode
from orcapod.core.packet_function import PythonPacketFunction


class TestFunctionNodePopulateCache:
    def test_iter_packets_uses_cache_when_populated(self):
        table = pa.table({
            "key": pa.array(["a", "b"], type=pa.large_string()),
            "value": pa.array([1, 2], type=pa.int64()),
        })
        src = ArrowTableSource(table, tag_columns=["key"])
        pf = PythonPacketFunction(lambda value: value * 2, output_keys="result")
        pod = FunctionPod(pf)
        node = FunctionNode(pod, src)

        # Run to get results
        original = list(node.iter_packets())
        assert len(original) == 2

        # Clear and populate with subset
        node.clear_cache()
        node.populate_cache([original[0]])
        cached = list(node.iter_packets())
        assert len(cached) == 1
```

- [ ] **Step 12: Run test to verify it fails**

Run: `uv run pytest tests/test_core/nodes/test_node_populate_cache.py::TestFunctionNodePopulateCache -v`
Expected: FAIL — `AttributeError: 'FunctionNode' object has no attribute 'populate_cache'`

- [ ] **Step 13: Implement FunctionNode.populate_cache**

In `src/orcapod/core/nodes/function_node.py`:

```python
def populate_cache(
    self, results: list[tuple[TagProtocol, PacketProtocol]]
) -> None:
    """Populate in-memory cache from externally-provided results.

    After calling this, ``iter_packets()`` returns from the cache
    without upstream iteration or computation.

    Args:
        results: Materialized (tag, packet) pairs.
    """
    self._cached_output_packets.clear()
    for i, (tag, packet) in enumerate(results):
        self._cached_output_packets[i] = (tag, packet)
    self._cached_input_iterator = None
    self._needs_iterator = False
    self._update_modified_time()
```

- [ ] **Step 14: Run ALL tests**

Run: `uv run pytest tests/test_core/nodes/ tests/test_core/function_pod/ tests/test_pipeline/ -v`
Expected: ALL PASS

- [ ] **Step 15: Commit**

```bash
git add src/orcapod/core/nodes/function_node.py tests/test_core/nodes/test_function_node_get_cached.py tests/test_core/nodes/test_node_populate_cache.py
git commit -m "feat(function-node): add get_cached_results, populate_cache for orchestrator"
```

---

## Chunk 4: SyncPipelineOrchestrator and Pipeline Integration

### Task 7: SyncPipelineOrchestrator

**Files:**
- Create: `src/orcapod/pipeline/sync_orchestrator.py`
- Create: `tests/test_pipeline/test_sync_orchestrator.py`

- [ ] **Step 1: Write test for basic linear pipeline (Source → FunctionPod)**

```python
# tests/test_pipeline/test_sync_orchestrator.py
"""Tests for the synchronous pipeline orchestrator."""
from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.core.function_pod import FunctionPod
from orcapod.core.nodes import FunctionNode, OperatorNode, SourceNode
from orcapod.core.operators import SelectPacketColumns
from orcapod.core.operators.join import Join
from orcapod.core.operators.mappers import MapPackets
from orcapod.core.packet_function import PythonPacketFunction
from orcapod.core.sources import ArrowTableSource
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline import Pipeline
from orcapod.pipeline.observer import ExecutionObserver
from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator


def _make_source(tag_col, packet_col, data):
    table = pa.table({
        tag_col: pa.array(data[tag_col], type=pa.large_string()),
        packet_col: pa.array(data[packet_col], type=pa.int64()),
    })
    return ArrowTableSource(table, tag_columns=[tag_col])


def double_value(value: int) -> int:
    return value * 2


def add_values(value: int, score: int) -> int:
    return value + score


class TestSyncOrchestratorLinear:
    """Source -> FunctionPod."""

    def test_linear_pipeline(self):
        src = _make_source("key", "value", {"key": ["a", "b", "c"], "value": [1, 2, 3]})
        pf = PythonPacketFunction(double_value, output_keys="result")
        pod = FunctionPod(pf)

        pipeline = Pipeline(name="linear", pipeline_database=InMemoryArrowDatabase())
        with pipeline:
            pod(src, label="doubler")

        orch = SyncPipelineOrchestrator()
        result = orch.run(pipeline._node_graph)

        # Verify results exist for all nodes
        assert len(result.node_outputs) > 0

        # Find the function node output
        fn_outputs = [
            v for k, v in result.node_outputs.items()
            if k.node_type == "function"
        ]
        assert len(fn_outputs) == 1
        assert len(fn_outputs[0]) == 3
        values = sorted([pkt.as_dict()["result"] for _, pkt in fn_outputs[0]])
        assert values == [2, 4, 6]


class TestSyncOrchestratorDiamond:
    """Two sources -> Join -> FunctionPod."""

    def test_diamond_dag(self):
        src_a = _make_source("key", "value", {"key": ["a", "b"], "value": [10, 20]})
        src_b = _make_source("key", "score", {"key": ["a", "b"], "score": [100, 200]})
        pf = PythonPacketFunction(add_values, output_keys="total")
        pod = FunctionPod(pf)

        pipeline = Pipeline(name="diamond", pipeline_database=InMemoryArrowDatabase())
        with pipeline:
            joined = Join()(src_a, src_b, label="join")
            pod(joined, label="adder")

        orch = SyncPipelineOrchestrator()
        result = orch.run(pipeline._node_graph)

        fn_outputs = [
            v for k, v in result.node_outputs.items()
            if k.node_type == "function"
        ]
        assert len(fn_outputs) == 1
        values = sorted([pkt.as_dict()["total"] for _, pkt in fn_outputs[0]])
        assert values == [110, 220]


class TestSyncOrchestratorObserver:
    """Observer hooks fire in correct order."""

    def test_observer_hooks_fire(self):
        src = _make_source("key", "value", {"key": ["a"], "value": [1]})
        pf = PythonPacketFunction(double_value, output_keys="result")
        pod = FunctionPod(pf)

        pipeline = Pipeline(name="obs", pipeline_database=InMemoryArrowDatabase())
        with pipeline:
            pod(src, label="doubler")

        events = []

        class RecordingObserver:
            def on_node_start(self, node):
                events.append(("node_start", node.node_type))

            def on_node_end(self, node):
                events.append(("node_end", node.node_type))

            def on_packet_start(self, node, tag, packet):
                events.append(("packet_start",))

            def on_packet_end(self, node, tag, input_pkt, output_pkt, cached):
                events.append(("packet_end", cached))

        orch = SyncPipelineOrchestrator(observer=RecordingObserver())
        orch.run(pipeline._node_graph)

        # Source: node_start, node_end
        # Function: node_start, packet_start, packet_end, node_end
        assert events[0] == ("node_start", "source")
        assert events[1] == ("node_end", "source")
        assert events[2] == ("node_start", "function")
        assert events[3] == ("packet_start",)
        assert events[4] == ("packet_end", False)
        assert events[5] == ("node_end", "function")


class TestSyncOrchestratorUnknownNodeType:
    """Unknown node types raise TypeError."""

    def test_raises_on_unknown_node_type(self):
        import networkx as nx

        class FakeNode:
            node_type = "unknown"

        G = nx.DiGraph()
        G.add_node(FakeNode())

        orch = SyncPipelineOrchestrator()
        with pytest.raises(TypeError, match="Unknown node type"):
            orch.run(G)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_pipeline/test_sync_orchestrator.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement SyncPipelineOrchestrator**

```python
# src/orcapod/pipeline/sync_orchestrator.py
"""Synchronous pipeline orchestrator.

Walks a compiled pipeline's node graph topologically, executing each node
with materialized buffers and per-packet observer hooks for function nodes.
"""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from orcapod.pipeline.observer import NoOpObserver
from orcapod.pipeline.result import OrchestratorResult
from orcapod.protocols.node_protocols import (
    is_function_node,
    is_operator_node,
    is_source_node,
)

if TYPE_CHECKING:
    import networkx as nx

    from orcapod.pipeline.observer import ExecutionObserver
    from orcapod.protocols.core_protocols import PacketProtocol, TagProtocol

logger = logging.getLogger(__name__)


class SyncPipelineOrchestrator:
    """Execute a compiled pipeline synchronously with observer hooks.

    Walks the node graph in topological order. For each node:
    - SourceNode: materializes iter_packets() into a buffer
    - FunctionNode: per-packet execution with cache lookup + observer hooks
    - OperatorNode: bulk execution via operator.process()

    All nodes have store_result called after computation. The orchestrator
    returns an OrchestratorResult with all node outputs.

    Args:
        observer: Optional execution observer for hooks. Defaults to
            NoOpObserver.
    """

    def __init__(self, observer: "ExecutionObserver | None" = None) -> None:
        self._observer = observer or NoOpObserver()

    def run(
        self,
        graph: "nx.DiGraph",
        materialize_results: bool = True,
    ) -> OrchestratorResult:
        """Execute the node graph synchronously.

        Args:
            graph: A NetworkX DiGraph with GraphNode objects as vertices.
            materialize_results: If True, keep all node outputs in memory
                and return them. If False, discard buffers after downstream
                consumption (only DB-persisted results survive).

        Returns:
            OrchestratorResult with node outputs.
        """
        import networkx as nx

        topo_order = list(nx.topological_sort(graph))
        buffers: dict[Any, list[tuple[TagProtocol, PacketProtocol]]] = {}
        processed: set[Any] = set()

        for node in topo_order:
            if is_source_node(node):
                buffers[node] = self._execute_source(node)
            elif is_function_node(node):
                upstream_buffer = self._gather_upstream(node, graph, buffers)
                buffers[node] = self._execute_function(node, upstream_buffer)
            elif is_operator_node(node):
                upstream_buffers = self._gather_upstream_multi(
                    node, graph, buffers
                )
                buffers[node] = self._execute_operator(node, upstream_buffers)
            else:
                raise TypeError(
                    f"Unknown node type: {getattr(node, 'node_type', None)!r}"
                )

            processed.add(node)

            if not materialize_results:
                self._gc_buffers(node, graph, buffers, processed)

        return OrchestratorResult(node_outputs=buffers)

    def _execute_source(self, node):
        """Execute a source node: materialize its packets."""
        self._observer.on_node_start(node)
        output = list(node.iter_packets())
        node.store_result(output)
        self._observer.on_node_end(node)
        return output

    def _execute_function(self, node, upstream_buffer):
        """Execute a function node with per-packet hooks."""
        self._observer.on_node_start(node)

        upstream_entries = [
            (tag, packet, node.compute_pipeline_entry_id(tag, packet))
            for tag, packet in upstream_buffer
        ]
        entry_ids = [eid for _, _, eid in upstream_entries]

        cached = node.get_cached_results(entry_ids=entry_ids)

        output = []
        for tag, packet, entry_id in upstream_entries:
            self._observer.on_packet_start(node, tag, packet)
            if entry_id in cached:
                tag_out, result = cached[entry_id]
                self._observer.on_packet_end(
                    node, tag, packet, result, cached=True
                )
                output.append((tag_out, result))
            else:
                tag_out, result = node.process_packet(tag, packet)
                node.store_result(tag, packet, result)
                self._observer.on_packet_end(
                    node, tag, packet, result, cached=False
                )
                if result is not None:
                    output.append((tag_out, result))

        self._observer.on_node_end(node)
        return output

    def _execute_operator(self, node, upstream_buffers):
        """Execute an operator node: bulk stream processing."""
        self._observer.on_node_start(node)

        cached = node.get_cached_output()
        if cached is not None:
            output = list(cached.iter_packets())
        else:
            input_streams = [
                self._materialize_as_stream(buf, upstream_node)
                for buf, upstream_node in upstream_buffers
            ]
            result_stream = node.operator.process(*input_streams)
            output = list(result_stream.iter_packets())
            node.store_result(output)

        self._observer.on_node_end(node)
        return output

    def _gather_upstream(self, node, graph, buffers):
        """Gather a single upstream buffer (for function nodes)."""
        predecessors = list(graph.predecessors(node))
        if len(predecessors) != 1:
            raise ValueError(
                f"FunctionNode expects exactly 1 upstream, got {len(predecessors)}"
            )
        return buffers[predecessors[0]]

    def _gather_upstream_multi(self, node, graph, buffers):
        """Gather multiple upstream buffers with their nodes (for operator nodes).

        Returns list of (buffer, upstream_node) tuples preserving the order
        that matches the operator's input_streams order.
        """
        predecessors = list(graph.predecessors(node))
        # Match predecessor order to the node's upstreams order
        upstream_order = {
            id(upstream): i for i, upstream in enumerate(node.upstreams)
        }
        sorted_preds = sorted(
            predecessors,
            key=lambda p: upstream_order.get(id(p), 0),
        )
        return [(buffers[p], p) for p in sorted_preds]

    @staticmethod
    def _materialize_as_stream(buf, upstream_node):
        """Wrap a (tag, packet) buffer as an ArrowTableStream.

        Args:
            buf: List of (tag, packet) tuples.
            upstream_node: The node that produced this buffer (used to
                determine tag column names).

        Returns:
            An ArrowTableStream.
        """
        from orcapod.core.streams.arrow_table_stream import ArrowTableStream
        from orcapod.utils import arrow_utils

        if not buf:
            raise ValueError("Cannot materialize empty buffer as stream")

        # Use selective columns matching the proven pattern in
        # StaticOutputOperatorPod._materialize_to_stream:
        # system_tags for tags, source info for packets.
        tag_tables = [
            tag.as_table(columns={"system_tags": True}) for tag, _ in buf
        ]
        packet_tables = [
            pkt.as_table(columns={"source": True}) for _, pkt in buf
        ]

        import pyarrow as pa

        combined_tags = pa.concat_tables(tag_tables)
        combined_packets = pa.concat_tables(packet_tables)

        user_tag_keys = tuple(buf[0][0].keys())
        source_info = buf[0][1].source_info()

        full_table = arrow_utils.hstack_tables(combined_tags, combined_packets)

        return ArrowTableStream(
            full_table,
            tag_columns=user_tag_keys,
            source_info=source_info,
        )

    @staticmethod
    def _gc_buffers(current_node, graph, buffers, processed):
        """Discard buffers no longer needed by any unprocessed downstream."""
        for pred in graph.predecessors(current_node):
            if pred not in buffers:
                continue
            all_successors_done = all(
                succ in processed for succ in graph.successors(pred)
            )
            if all_successors_done:
                del buffers[pred]
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_pipeline/test_sync_orchestrator.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/pipeline/sync_orchestrator.py tests/test_pipeline/test_sync_orchestrator.py
git commit -m "feat(pipeline): implement SyncPipelineOrchestrator"
```

### Task 8: Pipeline.run() Integration and File Reorganization

**Files:**
- Modify: `src/orcapod/pipeline/graph.py`
- Rename: `src/orcapod/pipeline/orchestrator.py` → `src/orcapod/pipeline/async_orchestrator.py`
- Modify: `src/orcapod/pipeline/__init__.py`

- [ ] **Step 1: Write test for Pipeline.run() with default orchestrator**

Append to `tests/test_pipeline/test_sync_orchestrator.py`:

```python
class TestPipelineRunIntegration:
    """Pipeline.run() with orchestrator parameter."""

    def test_default_run_uses_sync_orchestrator(self):
        """Pipeline.run() without args should use SyncPipelineOrchestrator."""
        src = _make_source("key", "value", {"key": ["a", "b"], "value": [1, 2]})
        pf = PythonPacketFunction(double_value, output_keys="result")
        pod = FunctionPod(pf)

        pipeline = Pipeline(name="default", pipeline_database=InMemoryArrowDatabase())
        with pipeline:
            pod(src, label="doubler")

        pipeline.run()

        records = pipeline.doubler.get_all_records()
        assert records is not None
        assert records.num_rows == 2
        values = sorted(records.column("result").to_pylist())
        assert values == [2, 4]

    def test_run_with_explicit_orchestrator(self):
        src = _make_source("key", "value", {"key": ["a", "b"], "value": [1, 2]})
        pf = PythonPacketFunction(double_value, output_keys="result")
        pod = FunctionPod(pf)

        pipeline = Pipeline(name="explicit", pipeline_database=InMemoryArrowDatabase())
        with pipeline:
            pod(src, label="doubler")

        events = []

        class RecordingObserver:
            def on_node_start(self, node):
                events.append(("node_start", node.node_type))
            def on_node_end(self, node):
                events.append(("node_end", node.node_type))
            def on_packet_start(self, node, tag, packet):
                events.append(("packet_start",))
            def on_packet_end(self, node, tag, input_pkt, output_pkt, cached):
                events.append(("packet_end",))

        orch = SyncPipelineOrchestrator(observer=RecordingObserver())
        pipeline.run(orchestrator=orch)

        # Observer events should have fired
        assert len(events) > 0

        # Results should be accessible via node
        records = pipeline.doubler.get_all_records()
        assert records is not None

    def test_run_populates_node_caches(self):
        """After run(), iter_packets()/as_table() should work on nodes."""
        src = _make_source("key", "value", {"key": ["a", "b"], "value": [1, 2]})
        pf = PythonPacketFunction(double_value, output_keys="result")
        pod = FunctionPod(pf)

        pipeline = Pipeline(name="cache", pipeline_database=InMemoryArrowDatabase())
        with pipeline:
            pod(src, label="doubler")

        pipeline.run()

        # as_table should work after orchestrated execution
        table = pipeline.doubler.as_table()
        assert table.num_rows == 2
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_pipeline/test_sync_orchestrator.py::TestPipelineRunIntegration -v`
Expected: FAIL (Pipeline.run signature doesn't accept orchestrator yet)

- [ ] **Step 3: Rename orchestrator.py to async_orchestrator.py**

```bash
git mv src/orcapod/pipeline/orchestrator.py src/orcapod/pipeline/async_orchestrator.py
```

- [ ] **Step 4: Update Pipeline.run() in graph.py**

In `src/orcapod/pipeline/graph.py`, update the `run` method to accept an orchestrator
parameter and the `_run_async` method to import from the new location:

```python
def run(
    self,
    orchestrator=None,
    config: PipelineConfig | None = None,
    execution_engine: cp.PacketFunctionExecutorProtocol | None = None,
    execution_engine_opts: "dict[str, Any] | None" = None,
) -> None:
    """Execute all compiled nodes.

    Args:
        orchestrator: Optional orchestrator instance. When provided,
            the orchestrator drives execution. When omitted and no
            async mode is requested, defaults to
            SyncPipelineOrchestrator.
        config: Pipeline configuration (legacy parameter).
        execution_engine: Optional packet-function executor (legacy).
        execution_engine_opts: Engine options dict (legacy).
    """
    from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator
    from orcapod.pipeline.result import OrchestratorResult
    from orcapod.types import ExecutorType, PipelineConfig

    explicit_config = config is not None
    config = config or PipelineConfig()

    effective_engine = (
        execution_engine
        if execution_engine is not None
        else config.execution_engine
    )
    effective_opts = (
        execution_engine_opts
        if execution_engine_opts is not None
        else config.execution_engine_opts
    )

    if not self._compiled:
        self.compile()

    if effective_engine is not None:
        self._apply_execution_engine(effective_engine, effective_opts)

    if orchestrator is not None:
        result = orchestrator.run(self._node_graph)
        self._apply_results(result)
    else:
        use_async = config.executor == ExecutorType.ASYNC_CHANNELS or (
            effective_engine is not None and not explicit_config
        )
        if use_async:
            self._run_async(config)
        else:
            orch = SyncPipelineOrchestrator()
            result = orch.run(self._node_graph)
            self._apply_results(result)

    self.flush()
```

Add the `_apply_results` method:

```python
def _apply_results(self, result: "OrchestratorResult") -> None:
    """Populate node caches from orchestrator results."""
    for node, outputs in result.node_outputs.items():
        if hasattr(node, "populate_cache"):
            node.populate_cache(outputs)
```

Update `_run_async` import path:

```python
def _run_async(self, config: PipelineConfig) -> None:
    """Run the pipeline asynchronously using the orchestrator."""
    from orcapod.pipeline.async_orchestrator import AsyncPipelineOrchestrator

    orchestrator = AsyncPipelineOrchestrator()
    orchestrator.run(self, config)
```

- [ ] **Step 5: Update pipeline __init__.py**

```python
# src/orcapod/pipeline/__init__.py
from .async_orchestrator import AsyncPipelineOrchestrator
from .graph import Pipeline
from .serialization import LoadStatus, PIPELINE_FORMAT_VERSION
from .sync_orchestrator import SyncPipelineOrchestrator

__all__ = [
    "AsyncPipelineOrchestrator",
    "LoadStatus",
    "PIPELINE_FORMAT_VERSION",
    "Pipeline",
    "SyncPipelineOrchestrator",
]
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `uv run pytest tests/test_pipeline/test_sync_orchestrator.py -v`
Expected: PASS

- [ ] **Step 7: Run ALL tests to verify no regressions**

Run: `uv run pytest tests/ -v`
Expected: ALL PASS. The async orchestrator tests should still pass since the import
path is updated in `__init__.py`.

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -m "feat(pipeline): integrate SyncPipelineOrchestrator into Pipeline.run()"
```

### Task 9: Sync vs Async Parity Tests

**Files:**
- Modify: `tests/test_pipeline/test_sync_orchestrator.py`

- [ ] **Step 1: Write parity tests**

Append to `tests/test_pipeline/test_sync_orchestrator.py`:

```python
class TestSyncAsyncParity:
    """Sync orchestrator should produce same DB results as async."""

    def test_linear_pipeline_parity(self):
        src = _make_source("key", "value", {"key": ["a", "b"], "value": [10, 20]})
        pf = PythonPacketFunction(double_value, output_keys="result")
        pod = FunctionPod(pf)

        # Sync via orchestrator
        sync_pipeline = Pipeline(
            name="sync", pipeline_database=InMemoryArrowDatabase()
        )
        with sync_pipeline:
            pod(src, label="doubler")
        sync_pipeline.run()
        sync_records = sync_pipeline.doubler.get_all_records()
        sync_values = sorted(sync_records.column("result").to_pylist())

        # Async
        from orcapod.pipeline import AsyncPipelineOrchestrator

        async_pipeline = Pipeline(
            name="async", pipeline_database=InMemoryArrowDatabase()
        )
        with async_pipeline:
            pod(src, label="doubler")
        AsyncPipelineOrchestrator().run(async_pipeline)
        async_records = async_pipeline.doubler.get_all_records()
        async_values = sorted(async_records.column("result").to_pylist())

        assert sync_values == async_values

    def test_diamond_pipeline_parity(self):
        src_a = _make_source("key", "value", {"key": ["a", "b"], "value": [10, 20]})
        src_b = _make_source("key", "score", {"key": ["a", "b"], "score": [100, 200]})
        pf = PythonPacketFunction(add_values, output_keys="total")
        pod = FunctionPod(pf)

        sync_pipeline = Pipeline(
            name="sync_d", pipeline_database=InMemoryArrowDatabase()
        )
        with sync_pipeline:
            joined = Join()(src_a, src_b, label="join")
            pod(joined, label="adder")
        sync_pipeline.run()
        sync_values = sorted(
            sync_pipeline.adder.get_all_records().column("total").to_pylist()
        )

        from orcapod.pipeline import AsyncPipelineOrchestrator

        async_pipeline = Pipeline(
            name="async_d", pipeline_database=InMemoryArrowDatabase()
        )
        with async_pipeline:
            joined = Join()(src_a, src_b, label="join")
            pod(joined, label="adder")
        AsyncPipelineOrchestrator().run(async_pipeline)
        async_values = sorted(
            async_pipeline.adder.get_all_records().column("total").to_pylist()
        )

        assert sync_values == async_values
```

- [ ] **Step 2: Run parity tests**

Run: `uv run pytest tests/test_pipeline/test_sync_orchestrator.py::TestSyncAsyncParity -v`
Expected: PASS

- [ ] **Step 3: Final full test suite run**

Run: `uv run pytest tests/ -v`
Expected: ALL PASS

- [ ] **Step 4: Commit**

```bash
git add tests/test_pipeline/test_sync_orchestrator.py
git commit -m "test(pipeline): add sync vs async parity tests"
```
