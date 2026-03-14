# Remove populate_cache — Self-Caching Nodes Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development or superpowers:executing-plans.

**Goal:** Remove `populate_cache` from all nodes; make `store_result` and `get_cached_results` self-caching.

**Architecture:** Nodes build in-memory caches as a side effect of orchestrator calls. No external cache population step needed.

**Spec:** `superpowers/specs/2026-03-14-remove-populate-cache-design.md`

---

### Task 1: Update SourceNode — self-caching store_result, remove populate_cache

**Files:**
- Modify: `src/orcapod/core/nodes/source_node.py`
- Modify: `tests/test_core/nodes/test_node_store_result.py`
- Modify: `tests/test_core/nodes/test_node_populate_cache.py`

- [ ] Update `TestSourceNodeStoreResult` — add test verifying `store_result` populates cache:

```python
def test_store_result_populates_internal_cache(self, source_and_node):
    _, node = source_and_node
    packets = list(node.iter_packets())
    node.store_result(packets)
    # iter_packets should now return from cache
    cached = list(node.iter_packets())
    assert len(cached) == len(packets)
```

- [ ] Run test, verify it fails (store_result is currently a no-op)
- [ ] Update `SourceNode.store_result` to populate `self._cached_results`
- [ ] Remove `SourceNode.populate_cache` method
- [ ] Remove `TestSourceNodePopulateCache` from test_node_populate_cache.py
- [ ] Remove `populate_cache` from `SourceNodeProtocol` in node_protocols.py
- [ ] Run all tests, verify pass
- [ ] Commit

### Task 2: Update OperatorNode — self-caching store_result, remove populate_cache

**Files:**
- Modify: `src/orcapod/core/nodes/operator_node.py`
- Modify: `tests/test_core/nodes/test_node_store_result.py`
- Modify: `tests/test_core/nodes/test_node_populate_cache.py`

- [ ] Update `TestOperatorNodeStoreResult` — add test verifying `store_result` populates cache:

```python
def test_store_result_populates_internal_cache(self, operator_with_db):
    node, _ = operator_with_db
    stream = node._operator.process(*node._input_streams)
    output = list(stream.iter_packets())
    node.store_result(output)
    # iter_packets should work from cache
    cached = list(node.iter_packets())
    assert len(cached) == 2
```

- [ ] Run test, verify it fails
- [ ] Update `OperatorNode.store_result` to also set `_cached_output_stream`
- [ ] Remove `OperatorNode.populate_cache` method
- [ ] Remove `TestOperatorNodePopulateCache` from test_node_populate_cache.py
- [ ] Remove `populate_cache` from `OperatorNodeProtocol` in node_protocols.py
- [ ] Run all tests, verify pass
- [ ] Commit

### Task 3: Update FunctionNode — self-caching store_result + get_cached_results, remove populate_cache

**Files:**
- Modify: `src/orcapod/core/nodes/function_node.py`
- Modify: `tests/test_core/nodes/test_node_store_result.py`
- Modify: `tests/test_core/nodes/test_function_node_get_cached.py`
- Modify: `tests/test_core/nodes/test_node_populate_cache.py`

- [ ] Add test verifying `store_result` populates `_cached_output_packets`:

```python
def test_store_result_populates_internal_cache(self, function_node_with_db):
    node, _, _ = function_node_with_db
    packets = list(node._input_stream.iter_packets())
    tag, packet = packets[0]
    tag_out, result = node.process_packet(tag, packet)
    node.store_result(tag, packet, result)
    assert len(node._cached_output_packets) == 1
```

- [ ] Add test verifying `get_cached_results` populates `_cached_output_packets`:

```python
def test_get_cached_results_populates_internal_cache(self, function_node_with_db):
    node = function_node_with_db
    packets = list(node._input_stream.iter_packets())
    # Process and store all packets
    entry_ids = []
    for tag, packet in packets:
        tag_out, result = node.process_packet(tag, packet)
        node.store_result(tag, packet, result)
        entry_ids.append(node.compute_pipeline_entry_id(tag, packet))
    # Clear internal cache
    node._cached_output_packets.clear()
    # get_cached_results should repopulate it
    node.get_cached_results(entry_ids)
    assert len(node._cached_output_packets) == 2
```

- [ ] Run tests, verify they fail
- [ ] Update `FunctionNode.store_result` to append to `_cached_output_packets`
- [ ] Update `FunctionNode.get_cached_results` to populate `_cached_output_packets`
- [ ] Remove `FunctionNode.populate_cache` method
- [ ] Remove `TestFunctionNodePopulateCache` from test_node_populate_cache.py
- [ ] Remove `populate_cache` from `FunctionNodeProtocol` in node_protocols.py
- [ ] Run all tests, verify pass
- [ ] Commit

### Task 4: Remove Pipeline._apply_results and update Pipeline.run()

**Files:**
- Modify: `src/orcapod/pipeline/graph.py`
- Modify: `tests/test_pipeline/test_sync_orchestrator.py`

- [ ] Remove `_apply_results` method from Pipeline
- [ ] Update `Pipeline.run()` — remove `_apply_results` calls
- [ ] Verify `test_run_populates_node_caches` still passes (nodes self-cache now)
- [ ] Run all tests
- [ ] Commit

### Task 5: Clean up test_node_populate_cache.py

- [ ] If test_node_populate_cache.py is now empty, delete it
- [ ] Run all tests
- [ ] Commit
