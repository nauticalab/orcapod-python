# Node Authority Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development or superpowers:executing-plans.

**Goal:** Make nodes self-validating and self-persisting. Remove `store_result`, add schema validation, add `process()` to FunctionNode and OperatorNode.

**Architecture:** Nodes validate input schemas, compute, persist, and cache internally. The orchestrator calls `process_packet` or `process` — nodes handle everything else. Pod vs Node distinction: pods return lazy streams, nodes return materialized lists.

**Spec:** `superpowers/specs/2026-03-14-node-authority-design.md`

---

## File Map

### Modified Files

| File | Changes |
|------|---------|
| `src/orcapod/protocols/node_protocols.py` | Remove `store_result`, remove `operator` property, add `process()` |
| `src/orcapod/core/nodes/function_node.py` | Revert process_packet to bundled, add schema validation, add `process()`, remove `store_result` |
| `src/orcapod/core/nodes/operator_node.py` | Add `process()` with validation, remove `store_result`, remove `operator` property |
| `src/orcapod/core/nodes/source_node.py` | Remove `store_result` |
| `src/orcapod/pipeline/sync_orchestrator.py` | Remove `store_result` calls, use `node.process()` for operators |
| `tests/test_core/nodes/test_node_store_result.py` | Rewrite as test_node_process.py |
| `tests/test_pipeline/test_sync_orchestrator.py` | Update if needed |

---

### Task 1: FunctionNode — revert process_packet, add schema validation, add process()

**Files:**
- Modify: `src/orcapod/core/nodes/function_node.py`
- Rename+Rewrite: `tests/test_core/nodes/test_node_store_result.py` → `tests/test_core/nodes/test_node_process.py`

- [ ] **Step 1: Write tests for reverted process_packet (bundled behavior)**

Create `tests/test_core/nodes/test_node_process.py` with FunctionNode tests:

```python
"""Tests for node process methods (schema validation, persistence, caching)."""
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
    def test_process_packet_returns_correct_result(self, function_node_no_db):
        node = function_node_no_db
        packets = list(node._input_stream.iter_packets())
        tag, packet = packets[0]
        tag_out, result = node.process_packet(tag, packet)
        assert result is not None
        assert result.as_dict()["result"] == 2

    def test_process_packet_writes_pipeline_record(self, function_node_with_db):
        """process_packet should write pipeline provenance record."""
        node, pipeline_db, _ = function_node_with_db
        packets = list(node._input_stream.iter_packets())
        tag, packet = packets[0]
        tag_out, result = node.process_packet(tag, packet)
        assert result is not None

        records = pipeline_db.get_all_records(node.pipeline_path)
        assert records is not None
        assert records.num_rows == 1

    def test_process_packet_writes_to_result_db(self, function_node_with_db):
        """process_packet should memoize via CachedFunctionPod."""
        node, _, _ = function_node_with_db
        packets = list(node._input_stream.iter_packets())
        tag, packet = packets[0]
        tag_out, result = node.process_packet(tag, packet)
        assert result is not None

        cached = node._cached_function_pod.get_all_cached_outputs()
        assert cached is not None
        assert cached.num_rows == 1

    def test_process_packet_caches_internally(self, function_node_with_db):
        """process_packet should populate _cached_output_packets."""
        node, _, _ = function_node_with_db
        packets = list(node._input_stream.iter_packets())
        tag, packet = packets[0]
        node.process_packet(tag, packet)
        assert len(node._cached_output_packets) == 1

    def test_process_packet_validates_schema(self, function_node_with_db):
        """process_packet should reject packets with wrong schema."""
        node, _, _ = function_node_with_db
        # Create a packet with wrong schema
        wrong_table = pa.table({
            "wrong_key": pa.array(["x"], type=pa.large_string()),
            "wrong_col": pa.array([99], type=pa.int64()),
        })
        wrong_src = ArrowTableSource(wrong_table, tag_columns=["wrong_key"])
        wrong_packets = list(wrong_src.iter_packets())
        wrong_tag, wrong_pkt = wrong_packets[0]

        with pytest.raises(Exception):  # InputValidationError or ValueError
            node.process_packet(wrong_tag, wrong_pkt)


class TestFunctionNodeProcess:
    def test_process_returns_materialized_results(self, function_node_with_db):
        node, _, _ = function_node_with_db
        results = node.process(node._input_stream)
        assert isinstance(results, list)
        assert len(results) == 2
        values = sorted([pkt.as_dict()["result"] for _, pkt in results])
        assert values == [2, 4]

    def test_process_writes_pipeline_records(self, function_node_with_db):
        node, pipeline_db, _ = function_node_with_db
        node.process(node._input_stream)
        records = pipeline_db.get_all_records(node.pipeline_path)
        assert records is not None
        assert records.num_rows == 2

    def test_process_caches_internally(self, function_node_with_db):
        node, _, _ = function_node_with_db
        node.process(node._input_stream)
        assert len(node._cached_output_packets) == 2

    def test_process_validates_stream_schema(self, function_node_with_db):
        node, _, _ = function_node_with_db
        wrong_table = pa.table({
            "wrong_key": pa.array(["x"], type=pa.large_string()),
            "wrong_col": pa.array([99], type=pa.int64()),
        })
        wrong_stream = ArrowTableSource(wrong_table, tag_columns=["wrong_key"])
        with pytest.raises(Exception):
            node.process(wrong_stream)
```

- [ ] **Step 2: Run tests, verify failures** (process_packet no longer writes pipeline records due to earlier refactor; process() doesn't exist yet; schema validation doesn't exist)

Run: `uv run pytest tests/test_core/nodes/test_node_process.py -v`

- [ ] **Step 3: Implement changes on FunctionNode**

In `src/orcapod/core/nodes/function_node.py`:

a. Add `_validate_input_schema(tag, packet)` method that checks tag keys, packet keys, and system tag column names against `self._input_stream.output_schema(columns={"system_tags": True})` / `self._input_stream.keys(all_info=True)`.

b. Add `_validate_stream_schema(input_stream)` method that validates the stream's output_schema against expected.

c. Refactor: rename `_process_and_store_packet` to `_process_packet_internal` — this is the core compute+persist+cache method (no schema validation).

d. Update `process_packet` to call `_validate_input_schema` then `_process_packet_internal`.

e. Add `process(input_stream)` that calls `_validate_stream_schema` once, then iterates calling `_process_packet_internal` per packet.

f. Remove `store_result()` method.

g. Update `iter_packets()`, `_iter_packets_sequential()`, `_iter_packets_concurrent()` to call `_process_packet_internal` (they already call `_process_and_store_packet` — just rename).

- [ ] **Step 4: Run tests**

Run: `uv run pytest tests/test_core/nodes/test_node_process.py -v`
Run: `uv run pytest tests/test_core/function_pod/ -v` (regression)

- [ ] **Step 5: Commit**

```
git commit -m "refactor(function-node): self-validating process_packet, add process(), remove store_result"
```

### Task 2: OperatorNode — add process(), remove store_result and operator property

**Files:**
- Modify: `src/orcapod/core/nodes/operator_node.py`
- Modify: `tests/test_core/nodes/test_node_process.py` (append)

- [ ] **Step 1: Write tests for OperatorNode.process()**

Append to `tests/test_core/nodes/test_node_process.py`:

```python
from orcapod.core.nodes import OperatorNode
from orcapod.core.operators import SelectPacketColumns
from orcapod.core.operators.join import Join
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
    return node, db, src


@pytest.fixture
def operator_no_db():
    table = pa.table({
        "key": pa.array(["a", "b"], type=pa.large_string()),
        "value": pa.array([10, 20], type=pa.int64()),
    })
    src = ArrowTableSource(table, tag_columns=["key"])
    op = SelectPacketColumns(columns=["value"])
    node = OperatorNode(op, input_streams=[src])
    return node, src


class TestOperatorNodeProcess:
    def test_process_returns_materialized_results(self, operator_no_db):
        node, src = operator_no_db
        results = node.process(src)
        assert isinstance(results, list)
        assert len(results) == 2

    def test_process_writes_to_db_in_log_mode(self, operator_with_db):
        node, db, src = operator_with_db
        node.process(src)
        records = node.get_all_records()
        assert records is not None
        assert records.num_rows == 2

    def test_process_caches_internally(self, operator_no_db):
        node, src = operator_no_db
        node.process(src)
        cached = list(node.iter_packets())
        assert len(cached) == 2

    def test_process_validates_stream_schema(self, operator_no_db):
        node, _ = operator_no_db
        wrong_table = pa.table({
            "wrong": pa.array(["x"], type=pa.large_string()),
            "bad": pa.array([1], type=pa.int64()),
        })
        wrong_stream = ArrowTableSource(wrong_table, tag_columns=["wrong"])
        with pytest.raises(Exception):
            node.process(wrong_stream)

    def test_process_noop_db_in_off_mode(self, operator_no_db):
        node, src = operator_no_db
        results = node.process(src)
        assert len(results) == 2
        # No DB, so no records
        assert node.get_all_records() is None
```

- [ ] **Step 2: Run tests, verify failures**

- [ ] **Step 3: Implement OperatorNode.process()**

In `src/orcapod/core/nodes/operator_node.py`:

a. Add `_validate_input_schemas(*input_streams)` — validates each stream's schema against the corresponding upstream's expected schema.

b. Add `process(*input_streams) → list[tuple[Tag, Packet]]`:
   1. Validate schemas
   2. `result_stream = self._operator.process(*input_streams)`
   3. Materialize: `output = list(result_stream.iter_packets())`
   4. Cache: `self._cached_output_stream = result_stream` (or re-materialize)
   5. DB persist if LOG mode: `self._store_output_stream(result_stream)`
   6. Return output

c. Remove `store_result()` method.
d. Remove `operator` property (orchestrator no longer needs it).

- [ ] **Step 4: Run tests**

Run: `uv run pytest tests/test_core/nodes/test_node_process.py -v`
Run: `uv run pytest tests/test_core/operators/ -v` (regression)

- [ ] **Step 5: Commit**

```
git commit -m "refactor(operator-node): add self-validating process(), remove store_result and operator property"
```

### Task 3: SourceNode — remove store_result

**Files:**
- Modify: `src/orcapod/core/nodes/source_node.py`

- [ ] **Step 1: Remove `store_result()` from SourceNode**
- [ ] **Step 2: Run tests, verify pass**
- [ ] **Step 3: Commit**

```
git commit -m "refactor(source-node): remove store_result"
```

### Task 4: Update protocols and orchestrator

**Files:**
- Modify: `src/orcapod/protocols/node_protocols.py`
- Modify: `src/orcapod/pipeline/sync_orchestrator.py`

- [ ] **Step 1: Update protocols**

Remove `store_result` from all protocols. Remove `operator` property from `OperatorNodeProtocol`. Add `process` to `FunctionNodeProtocol` and `OperatorNodeProtocol`.

- [ ] **Step 2: Update SyncPipelineOrchestrator**

- `_execute_source`: remove `node.store_result(output)` call
- `_execute_function`: remove `node.store_result(tag, packet, result)` call (process_packet handles it)
- `_execute_operator`: replace `node.operator.process(*input_streams)` + `node.store_result(output)` with `output = node.process(*input_streams)`
- Remove `_materialize_as_stream` helper (no longer needed — node.process accepts streams, and the orchestrator still needs to wrap buffers as streams... actually keep this, the orchestrator needs it to create streams from buffers before passing to node.process)

Wait — the orchestrator still needs `_materialize_as_stream` because it holds buffers (lists of tag/packet) and needs to wrap them as `StreamProtocol` before calling `node.process(*input_streams)`. Keep the helper.

- [ ] **Step 3: Delete old test file and clean up**

- Delete `tests/test_core/nodes/test_node_store_result.py`
- Run full suite: `uv run pytest tests/ --tb=short` (timeout=300000)

- [ ] **Step 4: Commit**

```
git commit -m "refactor(orchestrator): update protocols and orchestrator for node authority pattern"
```

### Task 5: Final verification

- [ ] Run full test suite: `uv run pytest tests/ -v --tb=short`
- [ ] Verify all sync orchestrator tests pass
- [ ] Verify all parity tests pass
- [ ] Verify backward compat: `node.run()` and `iter_packets()` still work
