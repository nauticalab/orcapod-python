# Async Orchestrator Refactor Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refactor both orchestrators to use slim node protocols where nodes own their execution and orchestrators are pure topology schedulers.

**Architecture:** Node protocols slim to `execute()` + `async_execute()` with observer injection. Orchestrators call these methods and collect results. Per-packet logic (cache lookup, observer hooks) moves inside nodes. AsyncPipelineOrchestrator adopts the same `run(graph) -> OrchestratorResult` interface as the sync orchestrator.

**Tech Stack:** Python, asyncio, networkx, pyarrow, pytest, pytest-asyncio

**Spec:** `superpowers/specs/2026-03-15-async-orchestrator-refactor-design.md`

---

## Chunk 1: Protocol Changes and SourceNode

### Task 1: Slim down node protocols

**Files:**
- Modify: `src/orcapod/protocols/node_protocols.py`

- [ ] **Step 1: Write failing test — protocols have new shape**

Add a test that imports the new protocol shapes and verifies TypeGuard dispatch still works.

```python
# tests/test_pipeline/test_node_protocols.py (new file)
"""Tests for revised node protocols."""

from __future__ import annotations

import pytest
from unittest.mock import MagicMock, AsyncMock

from orcapod.protocols.node_protocols import (
    SourceNodeProtocol,
    FunctionNodeProtocol,
    OperatorNodeProtocol,
    is_source_node,
    is_function_node,
    is_operator_node,
)


class TestSourceNodeProtocol:
    def test_requires_execute(self):
        """SourceNodeProtocol requires execute method."""

        class GoodSource:
            node_type = "source"

            def execute(self, *, observer=None):
                return []

            async def async_execute(self, output, *, observer=None):
                pass

        assert isinstance(GoodSource(), SourceNodeProtocol)

    def test_rejects_old_iter_packets_only(self):
        """SourceNodeProtocol no longer accepts iter_packets alone."""

        class OldSource:
            node_type = "source"

            def iter_packets(self):
                return iter([])

        assert not isinstance(OldSource(), SourceNodeProtocol)


class TestFunctionNodeProtocol:
    def test_requires_execute_and_async_execute(self):
        class GoodFunction:
            node_type = "function"

            def execute(self, input_stream, *, observer=None):
                return []

            async def async_execute(self, input_channel, output, *, observer=None):
                pass

        assert isinstance(GoodFunction(), FunctionNodeProtocol)

    def test_rejects_old_protocol(self):
        """Old protocol with get_cached_results etc. is not sufficient."""

        class OldFunction:
            node_type = "function"

            def get_cached_results(self, entry_ids):
                return {}

            def compute_pipeline_entry_id(self, tag, packet):
                return ""

            def execute_packet(self, tag, packet):
                return (tag, None)

            def execute(self, input_stream):
                return []

        # Missing async_execute → not a valid FunctionNodeProtocol
        assert not isinstance(OldFunction(), FunctionNodeProtocol)


class TestOperatorNodeProtocol:
    def test_requires_execute_and_async_execute(self):
        class GoodOperator:
            node_type = "operator"

            def execute(self, *input_streams, observer=None):
                return []

            async def async_execute(self, inputs, output, *, observer=None):
                pass

        assert isinstance(GoodOperator(), OperatorNodeProtocol)

    def test_rejects_old_protocol(self):
        """Old protocol with get_cached_output is not sufficient."""

        class OldOperator:
            node_type = "operator"

            def execute(self, *input_streams):
                return []

            def get_cached_output(self):
                return None

        # Missing async_execute → not valid
        assert not isinstance(OldOperator(), OperatorNodeProtocol)


class TestTypeGuardDispatch:
    def test_dispatch_source(self):
        node = MagicMock()
        node.node_type = "source"
        assert is_source_node(node)
        assert not is_function_node(node)
        assert not is_operator_node(node)

    def test_dispatch_function(self):
        node = MagicMock()
        node.node_type = "function"
        assert is_function_node(node)

    def test_dispatch_operator(self):
        node = MagicMock()
        node.node_type = "operator"
        assert is_operator_node(node)
```

Create `tests/test_pipeline/test_node_protocols.py` with the above content.

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_pipeline/test_node_protocols.py -v`
Expected: FAIL — protocol shapes don't match yet.

- [ ] **Step 3: Update node protocols**

Replace the contents of `src/orcapod/protocols/node_protocols.py`:

```python
"""Node protocols for orchestrator interaction.

Defines the three node protocols (Source, Function, Operator) that
formalize the interface between orchestrators and graph nodes, plus
TypeGuard dispatch functions for runtime type narrowing.

Each protocol exposes ``execute`` (sync) and ``async_execute`` (async).
Nodes own their execution — caching, per-packet logic, and persistence
are internal. Orchestrators are topology schedulers.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Protocol, TypeGuard, runtime_checkable

if TYPE_CHECKING:
    from orcapod.channels import ReadableChannel, WritableChannel
    from orcapod.core.nodes import GraphNode
    from orcapod.pipeline.observer import ExecutionObserver
    from orcapod.protocols.core_protocols import (
        PacketProtocol,
        StreamProtocol,
        TagProtocol,
    )


@runtime_checkable
class SourceNodeProtocol(Protocol):
    """Protocol for source nodes in orchestrated execution."""

    node_type: str

    def execute(
        self,
        *,
        observer: "ExecutionObserver | None" = None,
    ) -> list[tuple["TagProtocol", "PacketProtocol"]]: ...

    async def async_execute(
        self,
        output: "WritableChannel[tuple[TagProtocol, PacketProtocol]]",
        *,
        observer: "ExecutionObserver | None" = None,
    ) -> None: ...


@runtime_checkable
class FunctionNodeProtocol(Protocol):
    """Protocol for function nodes in orchestrated execution."""

    node_type: str

    def execute(
        self,
        input_stream: "StreamProtocol",
        *,
        observer: "ExecutionObserver | None" = None,
    ) -> list[tuple["TagProtocol", "PacketProtocol"]]: ...

    async def async_execute(
        self,
        input_channel: "ReadableChannel[tuple[TagProtocol, PacketProtocol]]",
        output: "WritableChannel[tuple[TagProtocol, PacketProtocol]]",
        *,
        observer: "ExecutionObserver | None" = None,
    ) -> None: ...


@runtime_checkable
class OperatorNodeProtocol(Protocol):
    """Protocol for operator nodes in orchestrated execution."""

    node_type: str

    def execute(
        self,
        *input_streams: "StreamProtocol",
        observer: "ExecutionObserver | None" = None,
    ) -> list[tuple["TagProtocol", "PacketProtocol"]]: ...

    async def async_execute(
        self,
        inputs: "Sequence[ReadableChannel[tuple[TagProtocol, PacketProtocol]]]",
        output: "WritableChannel[tuple[TagProtocol, PacketProtocol]]",
        *,
        observer: "ExecutionObserver | None" = None,
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

- [ ] **Step 4: Run protocol tests to verify they pass**

Run: `uv run pytest tests/test_pipeline/test_node_protocols.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add tests/test_pipeline/test_node_protocols.py src/orcapod/protocols/node_protocols.py
git commit -m "refactor(protocols): slim node protocols to execute + async_execute with observer (PLT-922)"
```

### Task 2: Delete AsyncExecutableProtocol

**Files:**
- Delete: `src/orcapod/protocols/core_protocols/async_executable.py`
- Modify: `src/orcapod/protocols/core_protocols/__init__.py`

- [ ] **Step 1: Remove AsyncExecutableProtocol import and re-export**

In `src/orcapod/protocols/core_protocols/__init__.py`, remove line 4
(`from .async_executable import AsyncExecutableProtocol`) and remove
`"AsyncExecutableProtocol"` from `__all__`.

- [ ] **Step 2: Delete the file**

```bash
rm src/orcapod/protocols/core_protocols/async_executable.py
```

- [ ] **Step 3: Check for other imports of AsyncExecutableProtocol**

Run: `uv run grep -r "AsyncExecutableProtocol" src/ tests/`

If any imports remain, remove them. This protocol was defined but not used
by any caller.

- [ ] **Step 4: Run full test suite to verify nothing breaks**

Run: `uv run pytest tests/ -x -q`
Expected: All tests pass.

- [ ] **Step 5: Commit**

```bash
git add -u
git commit -m "refactor(protocols): remove AsyncExecutableProtocol (PLT-922)"
```

### Task 3: Add SourceNode.execute() with observer injection

**Files:**
- Modify: `src/orcapod/core/nodes/source_node.py:228-255`
- Test: `tests/test_pipeline/test_node_protocols.py` (extend)

- [ ] **Step 1: Write failing test for SourceNode.execute()**

Append to `tests/test_pipeline/test_node_protocols.py`:

```python
import pyarrow as pa
from orcapod.core.sources import ArrowTableSource
from orcapod.core.nodes import SourceNode


class TestSourceNodeExecute:
    def _make_source_node(self):
        table = pa.table({
            "key": pa.array(["a", "b", "c"], type=pa.large_string()),
            "value": pa.array([1, 2, 3], type=pa.int64()),
        })
        src = ArrowTableSource(table, tag_columns=["key"])
        return SourceNode(src)

    def test_execute_returns_list(self):
        node = self._make_source_node()
        result = node.execute()
        assert isinstance(result, list)
        assert len(result) == 3

    def test_execute_populates_cached_results(self):
        node = self._make_source_node()
        node.execute()
        assert node._cached_results is not None
        assert len(node._cached_results) == 3

    def test_execute_with_observer(self):
        node = self._make_source_node()
        events = []

        class Obs:
            def on_node_start(self, n):
                events.append(("start", n.node_type))
            def on_node_end(self, n):
                events.append(("end", n.node_type))
            def on_packet_start(self, n, t, p):
                pass
            def on_packet_end(self, n, t, ip, op, cached):
                pass

        node.execute(observer=Obs())
        assert events == [("start", "source"), ("end", "source")]

    def test_execute_without_observer(self):
        """execute() works fine with no observer."""
        node = self._make_source_node()
        result = node.execute()
        assert len(result) == 3
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_pipeline/test_node_protocols.py::TestSourceNodeExecute -v`
Expected: FAIL — `execute()` method doesn't exist yet.

- [ ] **Step 3: Implement SourceNode.execute()**

Add to `src/orcapod/core/nodes/source_node.py`, before the `run()` method
(around line 237):

```python
def execute(
    self,
    *,
    observer: Any = None,
) -> list[tuple[cp.TagProtocol, cp.PacketProtocol]]:
    """Execute this source: materialize packets and return.

    Args:
        observer: Optional execution observer for hooks.

    Returns:
        List of (tag, packet) tuples.
    """
    if self.stream is None:
        raise RuntimeError(
            "SourceNode in read-only mode has no stream data available"
        )
    if observer is not None:
        observer.on_node_start(self)
    result = list(self.stream.iter_packets())
    self._cached_results = result
    if observer is not None:
        observer.on_node_end(self)
    return result
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_pipeline/test_node_protocols.py::TestSourceNodeExecute -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/core/nodes/source_node.py tests/test_pipeline/test_node_protocols.py
git commit -m "feat(source-node): add execute() with observer injection (PLT-922)"
```

### Task 4: Tighten SourceNode.async_execute() signature + observer

**Files:**
- Modify: `src/orcapod/core/nodes/source_node.py:240-254`
- Test: `tests/test_pipeline/test_node_protocols.py` (extend)

- [ ] **Step 1: Write failing test for tightened async_execute**

Append to `tests/test_pipeline/test_node_protocols.py`:

```python
import pytest
from orcapod.channels import Channel


class TestSourceNodeAsyncExecuteProtocol:
    @pytest.mark.asyncio
    async def test_tightened_signature(self):
        """async_execute takes output only, no inputs."""
        table = pa.table({
            "key": pa.array(["a", "b"], type=pa.large_string()),
            "value": pa.array([1, 2], type=pa.int64()),
        })
        src = ArrowTableSource(table, tag_columns=["key"])
        node = SourceNode(src)

        output_ch = Channel(buffer_size=16)
        # New signature: just output + observer
        await node.async_execute(output_ch.writer, observer=None)
        rows = await output_ch.reader.collect()
        assert len(rows) == 2

    @pytest.mark.asyncio
    async def test_async_execute_with_observer(self):
        table = pa.table({
            "key": pa.array(["a"], type=pa.large_string()),
            "value": pa.array([1], type=pa.int64()),
        })
        src = ArrowTableSource(table, tag_columns=["key"])
        node = SourceNode(src)
        events = []

        class Obs:
            def on_node_start(self, n):
                events.append("start")
            def on_node_end(self, n):
                events.append("end")
            def on_packet_start(self, n, t, p):
                pass
            def on_packet_end(self, n, t, ip, op, cached):
                pass

        output_ch = Channel(buffer_size=16)
        await node.async_execute(output_ch.writer, observer=Obs())
        assert events == ["start", "end"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_pipeline/test_node_protocols.py::TestSourceNodeAsyncExecuteProtocol -v`
Expected: FAIL — old signature takes `inputs, output`.

- [ ] **Step 3: Update SourceNode.async_execute()**

Replace the `async_execute` method in `src/orcapod/core/nodes/source_node.py`
(around line 240):

```python
async def async_execute(
    self,
    output: WritableChannel[tuple[cp.TagProtocol, cp.PacketProtocol]],
    *,
    observer: Any = None,
) -> None:
    """Push all (tag, packet) pairs from the wrapped stream to the output channel.

    Args:
        output: Channel to write results to.
        observer: Optional execution observer for hooks.
    """
    if self.stream is None:
        raise RuntimeError(
            "SourceNode in read-only mode has no stream data available"
        )
    try:
        if observer is not None:
            observer.on_node_start(self)
        for tag, packet in self.stream.iter_packets():
            await output.send((tag, packet))
        if observer is not None:
            observer.on_node_end(self)
    finally:
        await output.close()
```

Also remove `Sequence` from the imports since it's no longer needed for the
signature (keep `Iterator`).

- [ ] **Step 4: Run tests**

Run: `uv run pytest tests/test_pipeline/test_node_protocols.py -v`
Expected: All PASS.

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/core/nodes/source_node.py tests/test_pipeline/test_node_protocols.py
git commit -m "refactor(source-node): tighten async_execute signature + observer (PLT-922)"
```

## Chunk 2: FunctionNode and OperatorNode Changes

### Task 5: Add observer parameter to FunctionNode.execute()

**Files:**
- Modify: `src/orcapod/core/nodes/function_node.py:488-512`
- Test: `tests/test_pipeline/test_node_protocols.py` (extend)

- [ ] **Step 1: Write failing test**

Append to `tests/test_pipeline/test_node_protocols.py`:

```python
from orcapod.core.function_pod import FunctionPod
from orcapod.core.packet_function import PythonPacketFunction
from orcapod.core.nodes import FunctionNode


def double_value(value: int) -> int:
    return value * 2


class TestFunctionNodeExecute:
    def _make_function_node(self):
        table = pa.table({
            "key": pa.array(["a", "b"], type=pa.large_string()),
            "value": pa.array([1, 2], type=pa.int64()),
        })
        src = ArrowTableSource(table, tag_columns=["key"])
        pf = PythonPacketFunction(double_value, output_keys="result")
        pod = FunctionPod(pf)
        return FunctionNode(pod, src)

    def test_execute_with_observer(self):
        node = self._make_function_node()
        events = []

        class Obs:
            def on_node_start(self, n):
                events.append(("node_start", n.node_type))
            def on_node_end(self, n):
                events.append(("node_end", n.node_type))
            def on_packet_start(self, n, t, p):
                events.append(("packet_start",))
            def on_packet_end(self, n, t, ip, op, cached):
                events.append(("packet_end", cached))

        input_stream = node._input_stream
        result = node.execute(input_stream, observer=Obs())

        assert len(result) == 2
        assert events[0] == ("node_start", "function")
        assert events[-1] == ("node_end", "function")
        # Should have packet_start/packet_end for each packet
        packet_events = [e for e in events if e[0].startswith("packet")]
        assert len(packet_events) == 4  # 2 start + 2 end

    def test_execute_without_observer(self):
        node = self._make_function_node()
        input_stream = node._input_stream
        result = node.execute(input_stream)
        assert len(result) == 2
        values = sorted([pkt.as_dict()["result"] for _, pkt in result])
        assert values == [2, 4]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_pipeline/test_node_protocols.py::TestFunctionNodeExecute -v`
Expected: FAIL — `execute()` doesn't accept `observer` keyword.

- [ ] **Step 3: Update FunctionNode.execute()**

In `src/orcapod/core/nodes/function_node.py`, modify the `execute` method
(line 488) to add observer parameter and internal hooks. The method should:

1. Accept `*, observer=None` keyword parameter
2. Call `observer.on_node_start(self)` at the start
3. For each packet: compute entry ID, check cache, call
   `observer.on_packet_start` / `on_packet_end(cached=...)` around execution
4. Call `observer.on_node_end(self)` at the end

```python
def execute(
    self,
    input_stream: StreamProtocol,
    *,
    observer: Any = None,
) -> list[tuple[TagProtocol, PacketProtocol]]:
    """Execute all packets from a stream: compute, persist, and cache.

    Args:
        input_stream: The input stream to process.
        observer: Optional execution observer for hooks.

    Returns:
        Materialized list of (tag, output_packet) pairs, excluding
        None outputs.
    """
    if observer is not None:
        observer.on_node_start(self)

    # Gather entry IDs and check cache
    upstream_entries = [
        (tag, packet, self.compute_pipeline_entry_id(tag, packet))
        for tag, packet in input_stream.iter_packets()
    ]
    entry_ids = [eid for _, _, eid in upstream_entries]
    cached = self.get_cached_results(entry_ids=entry_ids)

    output: list[tuple[TagProtocol, PacketProtocol]] = []
    for tag, packet, entry_id in upstream_entries:
        if observer is not None:
            observer.on_packet_start(self, tag, packet)

        if entry_id in cached:
            tag_out, result = cached[entry_id]
            if observer is not None:
                observer.on_packet_end(self, tag, packet, result, cached=True)
            output.append((tag_out, result))
        else:
            tag_out, result = self._process_packet_internal(tag, packet)
            if observer is not None:
                observer.on_packet_end(self, tag, packet, result, cached=False)
            if result is not None:
                output.append((tag_out, result))

    if observer is not None:
        observer.on_node_end(self)
    return output
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_pipeline/test_node_protocols.py::TestFunctionNodeExecute -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/core/nodes/function_node.py tests/test_pipeline/test_node_protocols.py
git commit -m "feat(function-node): add observer injection to execute() (PLT-922)"
```

### Task 6: Tighten FunctionNode.async_execute() signature + observer

**Files:**
- Modify: `src/orcapod/core/nodes/function_node.py:1142-1263`
- Test: `tests/test_pipeline/test_node_protocols.py` (extend)

- [ ] **Step 1: Write failing test**

Append to `tests/test_pipeline/test_node_protocols.py`:

```python
class TestFunctionNodeAsyncExecute:
    @pytest.mark.asyncio
    async def test_tightened_signature(self):
        """async_execute takes single input_channel, not Sequence."""
        table = pa.table({
            "key": pa.array(["a", "b"], type=pa.large_string()),
            "value": pa.array([1, 2], type=pa.int64()),
        })
        src = ArrowTableSource(table, tag_columns=["key"])
        pf = PythonPacketFunction(double_value, output_keys="result")
        pod = FunctionPod(pf)
        node = FunctionNode(pod, src)

        input_ch = Channel(buffer_size=16)
        output_ch = Channel(buffer_size=16)

        for tag, packet in src.iter_packets():
            await input_ch.writer.send((tag, packet))
        await input_ch.writer.close()

        # New signature: single input_channel, not list
        await node.async_execute(input_ch.reader, output_ch.writer)
        rows = await output_ch.reader.collect()
        assert len(rows) == 2
        values = sorted([pkt.as_dict()["result"] for _, pkt in rows])
        assert values == [2, 4]

    @pytest.mark.asyncio
    async def test_async_execute_with_observer(self):
        table = pa.table({
            "key": pa.array(["a"], type=pa.large_string()),
            "value": pa.array([1], type=pa.int64()),
        })
        src = ArrowTableSource(table, tag_columns=["key"])
        pf = PythonPacketFunction(double_value, output_keys="result")
        pod = FunctionPod(pf)
        node = FunctionNode(pod, src)

        events = []
        class Obs:
            def on_node_start(self, n): events.append("node_start")
            def on_node_end(self, n): events.append("node_end")
            def on_packet_start(self, n, t, p): events.append("pkt_start")
            def on_packet_end(self, n, t, ip, op, cached): events.append("pkt_end")

        input_ch = Channel(buffer_size=16)
        output_ch = Channel(buffer_size=16)
        for tag, packet in src.iter_packets():
            await input_ch.writer.send((tag, packet))
        await input_ch.writer.close()

        await node.async_execute(input_ch.reader, output_ch.writer, observer=Obs())
        assert "node_start" in events
        assert "node_end" in events
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_pipeline/test_node_protocols.py::TestFunctionNodeAsyncExecute -v`
Expected: FAIL — old signature takes `inputs` (Sequence) and `pipeline_config`.

- [ ] **Step 3: Update FunctionNode.async_execute()**

Replace the `async_execute` method in `src/orcapod/core/nodes/function_node.py`
(line 1142). Key changes:
- First positional arg: `input_channel: ReadableChannel[...]` (not `inputs: Sequence[...]`)
- Remove `pipeline_config` parameter
- Add `*, observer=None` keyword
- Replace all `inputs[0]` references with `input_channel`
- Use hardcoded default concurrency (defer to PLT-930)
- Add observer hooks: `on_node_start`/`on_node_end` around the whole method,
  `on_packet_start`/`on_packet_end(cached=...)` around each packet in Phase 2

```python
async def async_execute(
    self,
    input_channel: ReadableChannel[tuple[TagProtocol, PacketProtocol]],
    output: WritableChannel[tuple[TagProtocol, PacketProtocol]],
    *,
    observer: Any = None,
) -> None:
    """Streaming async execution for FunctionNode.

    When a database is attached, uses two-phase execution: replay cached
    results first, then compute missing packets concurrently. Otherwise,
    routes each packet through ``_async_process_packet_internal`` directly.

    Args:
        input_channel: Single input channel to read from.
        output: Output channel to write results to.
        observer: Optional execution observer for hooks.
    """
    try:
        if observer is not None:
            observer.on_node_start(self)

        if self._cached_function_pod is not None:
            # Two-phase async execution with DB backing
            PIPELINE_ENTRY_ID_COL = "__pipeline_entry_id"
            existing_entry_ids: set[str] = set()

            taginfo = self._pipeline_database.get_all_records(
                self.pipeline_path,
                record_id_column=PIPELINE_ENTRY_ID_COL,
            )
            results = self._cached_function_pod._result_database.get_all_records(
                self._cached_function_pod.record_path,
                record_id_column=constants.PACKET_RECORD_ID,
            )

            if taginfo is not None and results is not None:
                joined = (
                    pl.DataFrame(taginfo)
                    .join(
                        pl.DataFrame(results),
                        on=constants.PACKET_RECORD_ID,
                        how="inner",
                    )
                    .to_arrow()
                )
                if joined.num_rows > 0:
                    tag_keys = self._input_stream.keys()[0]
                    existing_entry_ids = set(
                        cast(
                            list[str],
                            joined.column(PIPELINE_ENTRY_ID_COL).to_pylist(),
                        )
                    )
                    drop_cols = [
                        c
                        for c in joined.column_names
                        if c.startswith(constants.META_PREFIX)
                        or c == PIPELINE_ENTRY_ID_COL
                    ]
                    data_table = joined.drop(
                        [c for c in drop_cols if c in joined.column_names]
                    )
                    existing_stream = ArrowTableStream(
                        data_table, tag_columns=tag_keys
                    )
                    for tag, packet in existing_stream.iter_packets():
                        await output.send((tag, packet))

            # Phase 2: process new packets concurrently
            async def process_one_db(
                tag: TagProtocol, packet: PacketProtocol
            ) -> None:
                try:
                    if observer is not None:
                        observer.on_packet_start(self, tag, packet)
                    (
                        tag_out,
                        result_packet,
                    ) = await self._async_process_packet_internal(tag, packet)
                    if observer is not None:
                        observer.on_packet_end(
                            self, tag, packet, result_packet, cached=False
                        )
                    if result_packet is not None:
                        await output.send((tag_out, result_packet))
                finally:
                    pass

            async with asyncio.TaskGroup() as tg:
                async for tag, packet in input_channel:
                    entry_id = self.compute_pipeline_entry_id(tag, packet)
                    if entry_id in existing_entry_ids:
                        if observer is not None:
                            observer.on_packet_start(self, tag, packet)
                            observer.on_packet_end(
                                self, tag, packet, None, cached=True
                            )
                        continue
                    tg.create_task(process_one_db(tag, packet))
        else:
            # Simple async execution without DB
            async def process_one(
                tag: TagProtocol, packet: PacketProtocol
            ) -> None:
                if observer is not None:
                    observer.on_packet_start(self, tag, packet)
                (
                    tag_out,
                    result_packet,
                ) = await self._async_process_packet_internal(tag, packet)
                if observer is not None:
                    observer.on_packet_end(
                        self, tag, packet, result_packet, cached=False
                    )
                if result_packet is not None:
                    await output.send((tag_out, result_packet))

            async with asyncio.TaskGroup() as tg:
                async for tag, packet in input_channel:
                    tg.create_task(process_one(tag, packet))

        if observer is not None:
            observer.on_node_end(self)
    finally:
        await output.close()
```

Note: Concurrency limiting (semaphore) is removed for now. PLT-930 will
re-add it as node-level config.

- [ ] **Step 4: Run new tests**

Run: `uv run pytest tests/test_pipeline/test_node_protocols.py::TestFunctionNodeAsyncExecute -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/core/nodes/function_node.py tests/test_pipeline/test_node_protocols.py
git commit -m "refactor(function-node): tighten async_execute signature + observer (PLT-922)"
```

### Task 7: Add observer to OperatorNode.execute() + cache check

**Files:**
- Modify: `src/orcapod/core/nodes/operator_node.py:432-473`
- Test: `tests/test_pipeline/test_node_protocols.py` (extend)

- [ ] **Step 1: Write failing test**

Append to `tests/test_pipeline/test_node_protocols.py`:

```python
from orcapod.core.nodes import OperatorNode
from orcapod.core.operators.join import Join


class TestOperatorNodeExecute:
    def _make_join_node(self):
        table_a = pa.table({
            "key": pa.array(["a", "b"], type=pa.large_string()),
            "value": pa.array([10, 20], type=pa.int64()),
        })
        table_b = pa.table({
            "key": pa.array(["a", "b"], type=pa.large_string()),
            "score": pa.array([100, 200], type=pa.int64()),
        })
        src_a = ArrowTableSource(table_a, tag_columns=["key"])
        src_b = ArrowTableSource(table_b, tag_columns=["key"])
        return OperatorNode(Join(), input_streams=[src_a, src_b])

    def test_execute_with_observer(self):
        node = self._make_join_node()
        events = []

        class Obs:
            def on_node_start(self, n):
                events.append(("node_start", n.node_type))
            def on_node_end(self, n):
                events.append(("node_end", n.node_type))
            def on_packet_start(self, n, t, p):
                pass
            def on_packet_end(self, n, t, ip, op, cached):
                pass

        result = node.execute(
            *node._input_streams, observer=Obs()
        )
        assert len(result) == 2
        assert events == [("node_start", "operator"), ("node_end", "operator")]

    def test_execute_without_observer(self):
        node = self._make_join_node()
        result = node.execute(*node._input_streams)
        assert len(result) == 2
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_pipeline/test_node_protocols.py::TestOperatorNodeExecute -v`
Expected: FAIL — `execute()` doesn't accept `observer`.

- [ ] **Step 3: Update OperatorNode.execute()**

Modify `src/orcapod/core/nodes/operator_node.py` `execute` method (line 432):

```python
def execute(
    self,
    *input_streams: StreamProtocol,
    observer: Any = None,
) -> list[tuple[TagProtocol, PacketProtocol]]:
    """Execute input streams: compute, persist, and cache.

    Args:
        *input_streams: Input streams to execute.
        observer: Optional execution observer for hooks.

    Returns:
        Materialized list of (tag, packet) pairs.
    """
    if observer is not None:
        observer.on_node_start(self)

    # Check REPLAY cache first
    cached_output = self.get_cached_output()
    if cached_output is not None:
        output = list(cached_output.iter_packets())
        if observer is not None:
            observer.on_node_end(self)
        return output

    # Compute
    result_stream = self._operator.process(*input_streams)

    # Materialize
    output = list(result_stream.iter_packets())

    # Cache
    if output:
        self._cached_output_stream = StaticOutputOperatorPod._materialize_to_stream(
            output
        )
    else:
        self._cached_output_stream = result_stream

    self._update_modified_time()

    # Persist to DB only in LOG mode
    if (
        self._pipeline_database is not None
        and self._cache_mode == CacheMode.LOG
        and self._cached_output_stream is not None
    ):
        self._store_output_stream(self._cached_output_stream)

    if observer is not None:
        observer.on_node_end(self)
    return output
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_pipeline/test_node_protocols.py::TestOperatorNodeExecute -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/core/nodes/operator_node.py tests/test_pipeline/test_node_protocols.py
git commit -m "feat(operator-node): add observer + cache check to execute() (PLT-922)"
```

### Task 8: Add observer to OperatorNode.async_execute()

**Files:**
- Modify: `src/orcapod/core/nodes/operator_node.py:627-688`
- Test: `tests/test_pipeline/test_node_protocols.py` (extend)

- [ ] **Step 1: Write failing test**

Append to `tests/test_pipeline/test_node_protocols.py`:

```python
class TestOperatorNodeAsyncExecute:
    @pytest.mark.asyncio
    async def test_async_execute_with_observer(self):
        table_a = pa.table({
            "key": pa.array(["a", "b"], type=pa.large_string()),
            "value": pa.array([10, 20], type=pa.int64()),
        })
        src_a = ArrowTableSource(table_a, tag_columns=["key"])
        from orcapod.core.operators import SelectPacketColumns
        op = SelectPacketColumns(columns=["value"])
        op_node = OperatorNode(op, input_streams=[src_a])

        events = []
        class Obs:
            def on_node_start(self, n): events.append("start")
            def on_node_end(self, n): events.append("end")
            def on_packet_start(self, n, t, p): pass
            def on_packet_end(self, n, t, ip, op, cached): pass

        input_ch = Channel(buffer_size=16)
        output_ch = Channel(buffer_size=16)
        for tag, packet in src_a.iter_packets():
            await input_ch.writer.send((tag, packet))
        await input_ch.writer.close()

        await op_node.async_execute([input_ch.reader], output_ch.writer, observer=Obs())
        assert "start" in events
        assert "end" in events
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_pipeline/test_node_protocols.py::TestOperatorNodeAsyncExecute -v`
Expected: FAIL — `async_execute()` doesn't accept `observer`.

- [ ] **Step 3: Update OperatorNode.async_execute()**

Modify `src/orcapod/core/nodes/operator_node.py` `async_execute` (line 627)
to add `*, observer=None` keyword and call hooks:

```python
async def async_execute(
    self,
    inputs: Sequence[ReadableChannel[tuple[TagProtocol, PacketProtocol]]],
    output: WritableChannel[tuple[TagProtocol, PacketProtocol]],
    *,
    observer: Any = None,
) -> None:
```

Add `if observer: observer.on_node_start(self)` near the top of the try block,
and `if observer: observer.on_node_end(self)` before the `finally`.

- [ ] **Step 4: Run tests**

Run: `uv run pytest tests/test_pipeline/test_node_protocols.py::TestOperatorNodeAsyncExecute -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/core/nodes/operator_node.py tests/test_pipeline/test_node_protocols.py
git commit -m "feat(operator-node): add observer to async_execute() (PLT-922)"
```

## Chunk 3: Orchestrator Refactoring

### Task 9: Simplify SyncPipelineOrchestrator

**Files:**
- Modify: `src/orcapod/pipeline/sync_orchestrator.py`
- Modify: `tests/test_pipeline/test_sync_orchestrator.py`

- [ ] **Step 1: Update SyncPipelineOrchestrator to use node.execute()**

Rewrite `src/orcapod/pipeline/sync_orchestrator.py`. The `run()` method
calls `node.execute(...)` directly. Remove `_execute_source`,
`_execute_function`, `_execute_operator`. Keep `_materialize_as_stream`,
`_gather_upstream`, `_gather_upstream_multi`, `_gc_buffers`.

```python
def run(
    self,
    graph: "nx.DiGraph",
    materialize_results: bool = True,
) -> OrchestratorResult:
    """Execute the node graph synchronously.

    Args:
        graph: A NetworkX DiGraph with GraphNode objects as vertices.
        materialize_results: If True, keep all node outputs in memory.
            If False, discard buffers after downstream consumption.

    Returns:
        OrchestratorResult with node outputs.
    """
    import networkx as nx

    topo_order = list(nx.topological_sort(graph))
    buffers: dict[Any, list[tuple[TagProtocol, PacketProtocol]]] = {}
    processed: set[Any] = set()

    for node in topo_order:
        if is_source_node(node):
            buffers[node] = node.execute(observer=self._observer)
        elif is_function_node(node):
            upstream_buf = self._gather_upstream(node, graph, buffers)
            upstream_node = list(graph.predecessors(node))[0]
            input_stream = self._materialize_as_stream(upstream_buf, upstream_node)
            buffers[node] = node.execute(input_stream, observer=self._observer)
        elif is_operator_node(node):
            upstream_buffers = self._gather_upstream_multi(node, graph, buffers)
            input_streams = [
                self._materialize_as_stream(buf, upstream_node)
                for buf, upstream_node in upstream_buffers
            ]
            buffers[node] = node.execute(*input_streams, observer=self._observer)
        else:
            raise TypeError(
                f"Unknown node type: {getattr(node, 'node_type', None)!r}"
            )

        processed.add(node)

        if not materialize_results:
            self._gc_buffers(node, graph, buffers, processed)

    return OrchestratorResult(node_outputs=buffers)
```

- [ ] **Step 2: Run existing sync orchestrator tests**

Run: `uv run pytest tests/test_pipeline/test_sync_orchestrator.py -v`
Expected: All PASS — the simplified orchestrator should produce the same results.

- [ ] **Step 3: Commit**

```bash
git add src/orcapod/pipeline/sync_orchestrator.py
git commit -m "refactor(sync-orchestrator): delegate to node.execute(), remove per-packet logic (PLT-922)"
```

### Task 10: Refactor AsyncPipelineOrchestrator

**Files:**
- Modify: `src/orcapod/pipeline/async_orchestrator.py`
- Modify: `tests/test_pipeline/test_orchestrator.py`

- [ ] **Step 1: Rewrite AsyncPipelineOrchestrator**

Replace the contents of `src/orcapod/pipeline/async_orchestrator.py`:

```python
"""Async pipeline orchestrator for push-based channel execution.

Walks a compiled pipeline's node graph and launches all nodes concurrently
via ``asyncio.TaskGroup``, wiring them together with bounded channels.
Uses TypeGuard dispatch with tightened per-type async_execute signatures.
"""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from typing import TYPE_CHECKING, Any

from orcapod.channels import BroadcastChannel, Channel
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


class AsyncPipelineOrchestrator:
    """Execute a compiled pipeline asynchronously using channels.

    After compilation, the orchestrator:

    1. Walks the node graph in topological order.
    2. Creates bounded channels (or broadcast channels for fan-out)
       between connected nodes.
    3. Launches every node's ``async_execute`` concurrently via
       ``asyncio.TaskGroup``, using TypeGuard dispatch for per-type
       signatures.

    Args:
        observer: Optional execution observer for hooks.
        buffer_size: Channel buffer size. Defaults to 64.
    """

    def __init__(
        self,
        observer: "ExecutionObserver | None" = None,
        buffer_size: int = 64,
    ) -> None:
        self._observer = observer
        self._buffer_size = buffer_size

    def run(
        self,
        graph: "nx.DiGraph",
        materialize_results: bool = True,
    ) -> OrchestratorResult:
        """Synchronous entry point — runs the async pipeline to completion.

        Args:
            graph: A NetworkX DiGraph with GraphNode objects as vertices.
            materialize_results: If True, collect all node outputs into
                the result. If False, return empty node_outputs.

        Returns:
            OrchestratorResult with node outputs.
        """
        return asyncio.run(self._run_async(graph, materialize_results))

    async def run_async(
        self,
        graph: "nx.DiGraph",
        materialize_results: bool = True,
    ) -> OrchestratorResult:
        """Async entry point for callers already inside an event loop.

        Args:
            graph: A NetworkX DiGraph with GraphNode objects as vertices.
            materialize_results: If True, collect all node outputs.

        Returns:
            OrchestratorResult with node outputs.
        """
        return await self._run_async(graph, materialize_results)

    async def _run_async(
        self,
        graph: "nx.DiGraph",
        materialize_results: bool,
    ) -> OrchestratorResult:
        """Core async logic: wire channels, launch tasks, collect results."""
        import networkx as nx

        topo_order = list(nx.topological_sort(graph))
        buf = self._buffer_size

        # Build edge maps
        out_edges: dict[Any, list[Any]] = defaultdict(list)
        in_edges: dict[Any, list[Any]] = defaultdict(list)
        for upstream_node, downstream_node in graph.edges():
            out_edges[upstream_node].append(downstream_node)
            in_edges[downstream_node].append(upstream_node)

        # Create channels for each edge
        node_output_channels: dict[Any, Channel | BroadcastChannel] = {}
        edge_readers: dict[tuple[Any, Any], Any] = {}

        for node, downstreams in out_edges.items():
            if len(downstreams) == 1:
                ch = Channel(buffer_size=buf)
                node_output_channels[node] = ch
                edge_readers[(node, downstreams[0])] = ch.reader
            else:
                bch = BroadcastChannel(buffer_size=buf)
                node_output_channels[node] = bch
                for ds in downstreams:
                    edge_readers[(node, ds)] = bch.add_reader()

        # Terminal nodes need sink channels
        terminal_channels: list[Channel] = []
        for node in topo_order:
            if node not in node_output_channels:
                ch = Channel(buffer_size=buf)
                node_output_channels[node] = ch
                terminal_channels.append(ch)

        # Result collection: tap each node's output
        collectors: dict[Any, list[tuple[TagProtocol, PacketProtocol]]] = {}
        if materialize_results:
            for node in topo_order:
                collectors[node] = []

        # Launch all nodes concurrently
        async with asyncio.TaskGroup() as tg:
            for node in topo_order:
                writer = node_output_channels[node].writer

                if materialize_results:
                    # Wrap writer to collect items
                    collector = collectors[node]
                    writer = _CollectingWriter(writer, collector)

                if is_source_node(node):
                    tg.create_task(
                        node.async_execute(writer, observer=self._observer)
                    )
                elif is_function_node(node):
                    input_reader = edge_readers[
                        (list(in_edges[node])[0], node)
                    ]
                    tg.create_task(
                        node.async_execute(
                            input_reader, writer, observer=self._observer
                        )
                    )
                elif is_operator_node(node):
                    input_readers = [
                        edge_readers[(upstream, node)]
                        for upstream in in_edges.get(node, [])
                    ]
                    tg.create_task(
                        node.async_execute(
                            input_readers, writer, observer=self._observer
                        )
                    )
                else:
                    raise TypeError(
                        f"Unknown node type: {getattr(node, 'node_type', None)!r}"
                    )

        # Drain terminal channels
        for ch in terminal_channels:
            await ch.reader.collect()

        return OrchestratorResult(
            node_outputs=collectors if materialize_results else {}
        )


class _CollectingWriter:
    """Wrapper that collects items while forwarding to real writer."""

    def __init__(self, writer: Any, collector: list) -> None:
        self._writer = writer
        self._collector = collector

    async def send(self, item: Any) -> None:
        self._collector.append(item)
        await self._writer.send(item)

    async def close(self) -> None:
        await self._writer.close()
```

- [ ] **Step 2: Update async orchestrator tests**

Update `tests/test_pipeline/test_orchestrator.py` with these mechanical changes
throughout the file:

**Signature changes (find and replace):**
- `orchestrator.run(pipeline)` → `pipeline.compile(); orchestrator.run(pipeline._node_graph); pipeline.flush()`
- `orchestrator.run(pipeline, config=config)` → `pipeline.compile(); AsyncPipelineOrchestrator(buffer_size=config.channel_buffer_size).run(pipeline._node_graph); pipeline.flush()`
- `await orchestrator.run_async(pipeline)` → `pipeline.compile(); await orchestrator.run_async(pipeline._node_graph); pipeline.flush()`
- `await node.async_execute([], output_ch.writer)` → `await node.async_execute(output_ch.writer)`
- `await node.async_execute([input_ch.reader], output_ch.writer)` → `await node.async_execute(input_ch.reader, output_ch.writer)`

**Affected test classes and specific changes:**

`TestSourceNodeAsyncExecute`: Change `await node.async_execute([], output_ch.writer)` to
`await node.async_execute(output_ch.writer)` in both test methods.

`TestFunctionNodeAsyncExecute`: Change `await node.async_execute([input_ch.reader], output_ch.writer)`
to `await node.async_execute(input_ch.reader, output_ch.writer)`.

`TestOrchestratorLinearPipeline`: Both tests — add `pipeline.compile()` before
and `pipeline.flush()` after `orchestrator.run(pipeline._node_graph)`.

`TestOrchestratorOperatorPipeline`: Same compile/run/flush pattern.

`TestOrchestratorDiamondDag`: Both tests — same pattern.

`TestOrchestratorRunAsync`: Change `await orchestrator.run_async(pipeline)` to
`pipeline.compile(); await orchestrator.run_async(pipeline._node_graph); pipeline.flush()`.

`TestPipelineConfigIntegration`: Replace:
```python
config = PipelineConfig(executor=ExecutorType.ASYNC_CHANNELS, channel_buffer_size=4)
orchestrator = AsyncPipelineOrchestrator()
orchestrator.run(pipeline, config=config)
```
with:
```python
pipeline.compile()
orchestrator = AsyncPipelineOrchestrator(buffer_size=4)
orchestrator.run(pipeline._node_graph)
pipeline.flush()
```

- [ ] **Step 3: Run updated tests**

Run: `uv run pytest tests/test_pipeline/test_orchestrator.py -v`
Expected: All PASS.

- [ ] **Step 4: Commit**

```bash
git add src/orcapod/pipeline/async_orchestrator.py tests/test_pipeline/test_orchestrator.py
git commit -m "refactor(async-orchestrator): use node protocols, graph interface, OrchestratorResult (PLT-922)"
```

### Task 11: Update Pipeline.run()

**Files:**
- Modify: `src/orcapod/pipeline/graph.py:359-474`

- [ ] **Step 1: Update Pipeline.run() and remove _run_async()**

In `src/orcapod/pipeline/graph.py`:

1. In the `run()` method (line 412-429), change the async path to:
   ```python
   if use_async:
       from orcapod.pipeline.async_orchestrator import AsyncPipelineOrchestrator
       AsyncPipelineOrchestrator(
           buffer_size=config.channel_buffer_size,
       ).run(self._node_graph)
   ```
   And change the explicit orchestrator path (line 413) to also pass the graph:
   ```python
   if orchestrator is not None:
       orchestrator.run(self._node_graph)
   ```

2. Delete the `_run_async` method (lines 469-474).

- [ ] **Step 2: Run all pipeline tests**

Run: `uv run pytest tests/test_pipeline/ -v`
Expected: All PASS.

- [ ] **Step 3: Run full test suite**

Run: `uv run pytest tests/ -x -q`
Expected: All PASS.

- [ ] **Step 4: Commit**

```bash
git add src/orcapod/pipeline/graph.py
git commit -m "refactor(pipeline): update run() to pass graph to orchestrators, remove _run_async (PLT-922)"
```

## Chunk 4: Parity Tests and Cleanup

### Task 12: Update parity tests

**Files:**
- Modify: `tests/test_pipeline/test_sync_orchestrator.py`

- [ ] **Step 1: Update parity test signatures**

In `tests/test_pipeline/test_sync_orchestrator.py`, class `TestSyncAsyncParity`:

Update the async orchestrator calls to use the new interface:
```python
# Old:
AsyncPipelineOrchestrator().run(async_pipeline)

# New:
async_pipeline.compile()
AsyncPipelineOrchestrator().run(async_pipeline._node_graph)
async_pipeline.flush()
```

Do this for both `test_linear_pipeline_parity` and `test_diamond_pipeline_parity`.

- [ ] **Step 2: Run parity tests**

Run: `uv run pytest tests/test_pipeline/test_sync_orchestrator.py::TestSyncAsyncParity -v`
Expected: PASS

- [ ] **Step 3: Add materialize_results tests**

Append to `tests/test_pipeline/test_sync_orchestrator.py`:

```python
class TestMaterializeResults:
    def test_sync_materialize_false_returns_empty(self):
        src = _make_source("key", "value", {"key": ["a", "b"], "value": [1, 2]})
        pf = PythonPacketFunction(double_value, output_keys="result")
        pod = FunctionPod(pf)

        pipeline = Pipeline(name="mat", pipeline_database=InMemoryArrowDatabase())
        with pipeline:
            pod(src, label="doubler")

        orch = SyncPipelineOrchestrator()
        result = orch.run(pipeline._node_graph, materialize_results=False)
        assert result.node_outputs == {}

    def test_async_materialize_true_collects_all(self):
        src = _make_source("key", "value", {"key": ["a", "b"], "value": [1, 2]})
        pf = PythonPacketFunction(double_value, output_keys="result")
        pod = FunctionPod(pf)

        pipeline = Pipeline(name="mat_async", pipeline_database=InMemoryArrowDatabase())
        with pipeline:
            pod(src, label="doubler")

        pipeline.compile()
        orch = AsyncPipelineOrchestrator()
        result = orch.run(pipeline._node_graph, materialize_results=True)
        assert len(result.node_outputs) > 0

    def test_async_materialize_false_returns_empty(self):
        src = _make_source("key", "value", {"key": ["a", "b"], "value": [1, 2]})
        pf = PythonPacketFunction(double_value, output_keys="result")
        pod = FunctionPod(pf)

        pipeline = Pipeline(name="mat_async2", pipeline_database=InMemoryArrowDatabase())
        with pipeline:
            pod(src, label="doubler")

        pipeline.compile()
        orch = AsyncPipelineOrchestrator()
        result = orch.run(pipeline._node_graph, materialize_results=False)
        assert result.node_outputs == {}
```

- [ ] **Step 4: Run new tests**

Run: `uv run pytest tests/test_pipeline/test_sync_orchestrator.py::TestMaterializeResults -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add tests/test_pipeline/test_sync_orchestrator.py
git commit -m "test(orchestrator): update parity tests + add materialize_results tests (PLT-922)"
```

### Task 13: Add async-specific tests (fan-out, terminal, error)

**Files:**
- Modify: `tests/test_pipeline/test_orchestrator.py` (extend)

- [ ] **Step 1: Add fan-out, terminal node, and error propagation tests**

Append to `tests/test_pipeline/test_orchestrator.py`:

```python
class TestAsyncOrchestratorFanOut:
    """One source fans out to multiple downstream nodes."""

    def test_fan_out_source_to_two_functions(self):
        src = _make_source("key", "value", {"key": ["a", "b"], "value": [1, 2]})
        pf1 = PythonPacketFunction(double_value, output_keys="result")
        pod1 = FunctionPod(pf1)
        pf2 = PythonPacketFunction(double_value, output_keys="result")
        pod2 = FunctionPod(pf2)

        pipeline = Pipeline(name="fanout", pipeline_database=InMemoryArrowDatabase())
        with pipeline:
            pod1(src, label="doubler1")
            pod2(src, label="doubler2")

        pipeline.compile()
        orch = AsyncPipelineOrchestrator()
        result = orch.run(pipeline._node_graph, materialize_results=True)
        pipeline.flush()

        fn_outputs = [
            v for k, v in result.node_outputs.items() if k.node_type == "function"
        ]
        assert len(fn_outputs) == 2
        for output in fn_outputs:
            values = sorted([pkt.as_dict()["result"] for _, pkt in output])
            assert values == [2, 4]


class TestAsyncOrchestratorTerminalNode:
    """Terminal nodes with no downstream should work correctly."""

    def test_single_terminal_source(self):
        """A pipeline with just a source (terminal) should work."""
        src = _make_source("key", "value", {"key": ["a"], "value": [1]})
        pipeline = Pipeline(name="terminal", pipeline_database=InMemoryArrowDatabase())
        with pipeline:
            # Just register the source, no downstream
            pass

        # Manually build a minimal graph with just a source node
        import networkx as nx
        from orcapod.core.nodes import SourceNode

        node = SourceNode(src)
        G = nx.DiGraph()
        G.add_node(node)

        orch = AsyncPipelineOrchestrator()
        result = orch.run(G, materialize_results=True)
        assert len(result.node_outputs) == 1


class TestAsyncOrchestratorErrorPropagation:
    """Node failures should propagate correctly."""

    def test_node_failure_propagates(self):
        def failing_fn(value: int) -> int:
            raise ValueError("intentional failure")

        src = _make_source("key", "value", {"key": ["a"], "value": [1]})
        pf = PythonPacketFunction(failing_fn, output_keys="result")
        pod = FunctionPod(pf)

        pipeline = Pipeline(name="error", pipeline_database=InMemoryArrowDatabase())
        with pipeline:
            pod(src, label="failer")

        pipeline.compile()
        orch = AsyncPipelineOrchestrator()

        with pytest.raises(ExceptionGroup):
            orch.run(pipeline._node_graph)
```

- [ ] **Step 2: Run the new tests**

Run: `uv run pytest tests/test_pipeline/test_orchestrator.py::TestAsyncOrchestratorFanOut tests/test_pipeline/test_orchestrator.py::TestAsyncOrchestratorTerminalNode tests/test_pipeline/test_orchestrator.py::TestAsyncOrchestratorErrorPropagation -v`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add tests/test_pipeline/test_orchestrator.py
git commit -m "test(async-orchestrator): add fan-out, terminal, and error propagation tests (PLT-922)"
```

### Task 14: Full test suite verification and cleanup

- [ ] **Step 1: Verify no references to removed protocol methods**

Run these searches:
```bash
uv run grep -r "AsyncExecutableProtocol" src/ tests/
uv run grep -r "SourceNodeProtocol.*iter_packets" src/
uv run grep -r "FunctionNodeProtocol.*get_cached_results" src/
uv run grep -r "FunctionNodeProtocol.*execute_packet" src/
uv run grep -r "FunctionNodeProtocol.*compute_pipeline_entry_id" src/
uv run grep -r "OperatorNodeProtocol.*get_cached_output" src/
```

Expected: No matches.

- [ ] **Step 2: Run full test suite one final time**

Run: `uv run pytest tests/ -q`
Expected: All pass.

- [ ] **Step 3: Final commit if needed**

```bash
git add -u
git commit -m "chore(cleanup): remove stale references to old protocol methods (PLT-922)"
```
