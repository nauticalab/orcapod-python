# Pipeline & PipelineJob Invocation-Capture Refactor — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Unify invocation capture into a single `PodInvocation` primitive, move shared recording logic into `AbstractPipelineBase`, and replace the two divergent `compile()` implementations with one class-property–driven pass.

**Architecture:** Introduce `FunctionInvocation` / `OperatorInvocation` (subclasses of `ContentIdentifiableBase`) as the minimal recording primitive. `AbstractPipelineBase` gains `_invocation_lut` + `_source_streams` (both additive), a concrete `_record_invocation()`, concrete `record_function/operator_pod_invocation()`, abstract class-property node factories, a concrete `compile()`, `InvocationGraph` value object, and `from_invocations()` / `to_invocations()`. `Pipeline` and `PipelineJob` shed their redundant overrides and declare which node classes to use via class attributes. `SourceJobNode.from_stream()` gains three-way input logic. The legacy `_node_lut`, `_upstreams`, `_graph_edges` fields are kept as compiled artifacts (repopulated at end of `compile()`) so that `_build_execution_graph()` needs only a one-line update.

**Tech Stack:** Python, PyArrow, NetworkX; always run `uv run pytest` (never `pytest` directly).

---

## File Layout

**New files:**
- `src/orcapod/pipeline/pod_invocation.py` — `PodInvocation`, `FunctionInvocation`, `OperatorInvocation`
- `tests/test_pipeline/test_pod_invocation.py` — unit tests for the new primitives
- `tests/test_pipeline/test_pipeline_base_recording.py` — tests for unified recording path
- `tests/test_pipeline/test_invocation_transitions.py` — tests for to/from_invocations and cross-class transitions

**Modified files (in execution order):**
1. `src/orcapod/pipeline/base.py` — add `_invocation_lut`, `_source_streams`; concrete recording methods; abstract class-property factories; `InvocationGraph`; `from_invocations()`, `to_invocations()`; concrete `compile()`; `reset()` becomes no-op
2. `src/orcapod/pipeline/graph.py` — remove recording/compile overrides; add class-property attributes
3. `src/orcapod/core/nodes/source_node.py` — add `SourceJobNode.from_stream()` three-way logic
4. `src/orcapod/pipeline/job.py` — remove `_rec_*` fields/methods; add class-property attributes; thin `from_pipeline()`/`as_pipeline()`; lazy `compiled_pipeline`; thin `compile()` override; fix `_build_execution_graph()` one-liner

---

## Critical invariant: invocation hash == node hash

`StreamBase.identity_structure()` returns `(producer, producer.argument_symmetry(upstreams))` when a producer exists. Both `FunctionNodeBase` and `OperatorNodeBase` use this via `producer` and `upstreams` properties. The `PodInvocation.identity_structure()` returns `(pod, pod.argument_symmetry(input_streams))` — identical structure. So `FunctionInvocation(pod, (stream,)).content_hash() == FunctionNode(function_pod=pod, input_stream=stream).content_hash()`. This invariant means `_invocation_lut` keys match `_persistent_node_map` keys and `_hash_graph` vertex labels.

---

## Task 1: Write all failing tests

**Files:**
- Create: `tests/test_pipeline/test_pod_invocation.py`
- Create: `tests/test_pipeline/test_pipeline_base_recording.py`
- Create: `tests/test_pipeline/test_invocation_transitions.py`

- [ ] **Step 1.1: Create test_pod_invocation.py**

```python
"""Tests for PodInvocation, FunctionInvocation, OperatorInvocation."""
from __future__ import annotations

import pytest
import pyarrow as pa

from orcapod.core.function_pod import FunctionPod
from orcapod.core.operators import Join
from orcapod.core.data_function import PythonDataFunction
from orcapod.core.sources import ArrowTableSource


def _make_source(tag_col: str, data_col: str) -> ArrowTableSource:
    table = pa.table({
        tag_col: pa.array(["a", "b"], type=pa.large_string()),
        data_col: pa.array([1, 2], type=pa.int64()),
    })
    return ArrowTableSource(table, tag_columns=[tag_col], infer_nullable=True)


def _make_function_pod() -> FunctionPod:
    def double(value: int) -> int:
        return value * 2
    return FunctionPod(PythonDataFunction(double))


class TestFunctionInvocation:
    def test_import(self):
        from orcapod.pipeline.pod_invocation import FunctionInvocation
        assert FunctionInvocation is not None

    def test_hash_stability(self):
        from orcapod.pipeline.pod_invocation import FunctionInvocation
        pod = _make_function_pod()
        stream = _make_source("key", "value")
        inv1 = FunctionInvocation(pod=pod, input_streams=(stream,))
        inv2 = FunctionInvocation(pod=pod, input_streams=(stream,))
        assert inv1.content_hash() == inv2.content_hash()

    def test_hash_matches_function_node(self):
        """FunctionInvocation hash must equal FunctionNode hash — critical invariant."""
        from orcapod.pipeline.pod_invocation import FunctionInvocation
        from orcapod.core.nodes.function_node import FunctionNode
        pod = _make_function_pod()
        stream = _make_source("key", "value")
        inv = FunctionInvocation(pod=pod, input_streams=(stream,))
        node = FunctionNode(function_pod=pod, input_stream=stream)
        assert inv.content_hash() == node.content_hash()

    def test_label_stored(self):
        from orcapod.pipeline.pod_invocation import FunctionInvocation
        pod = _make_function_pod()
        stream = _make_source("key", "value")
        inv = FunctionInvocation(pod=pod, input_streams=(stream,), label="my_node")
        assert inv.label == "my_node"

    def test_pod_and_input_streams_accessible(self):
        from orcapod.pipeline.pod_invocation import FunctionInvocation
        pod = _make_function_pod()
        stream = _make_source("key", "value")
        inv = FunctionInvocation(pod=pod, input_streams=(stream,))
        assert inv.pod is pod
        assert inv.input_streams == (stream,)

    def test_isinstance_distinguishable_from_operator(self):
        from orcapod.pipeline.pod_invocation import FunctionInvocation, OperatorInvocation
        pod = _make_function_pod()
        stream = _make_source("key", "value")
        inv = FunctionInvocation(pod=pod, input_streams=(stream,))
        assert isinstance(inv, FunctionInvocation)
        assert not isinstance(inv, OperatorInvocation)


class TestOperatorInvocation:
    def test_import(self):
        from orcapod.pipeline.pod_invocation import OperatorInvocation
        assert OperatorInvocation is not None

    def test_hash_stability(self):
        from orcapod.pipeline.pod_invocation import OperatorInvocation
        join = Join()
        s1 = _make_source("key", "value")
        s2 = _make_source("key", "score")
        inv1 = OperatorInvocation(pod=join, input_streams=(s1, s2))
        inv2 = OperatorInvocation(pod=join, input_streams=(s1, s2))
        assert inv1.content_hash() == inv2.content_hash()

    def test_hash_matches_operator_node(self):
        """OperatorInvocation hash must equal OperatorNode hash — critical invariant."""
        from orcapod.pipeline.pod_invocation import OperatorInvocation
        from orcapod.core.nodes.operator_node import OperatorNode
        join = Join()
        s1 = _make_source("key", "value")
        s2 = _make_source("key", "score")
        inv = OperatorInvocation(pod=join, input_streams=(s1, s2))
        node = OperatorNode(operator=join, input_streams=(s1, s2))
        assert inv.content_hash() == node.content_hash()

    def test_commutative_operator_same_hash_regardless_of_input_order(self):
        """Join is commutative — hash must be order-independent."""
        from orcapod.pipeline.pod_invocation import OperatorInvocation
        join = Join()
        s1 = _make_source("key", "value")
        s2 = _make_source("key", "score")
        inv_ab = OperatorInvocation(pod=join, input_streams=(s1, s2))
        inv_ba = OperatorInvocation(pod=join, input_streams=(s2, s1))
        assert inv_ab.content_hash() == inv_ba.content_hash()

    def test_isinstance_distinguishable_from_function(self):
        from orcapod.pipeline.pod_invocation import FunctionInvocation, OperatorInvocation
        join = Join()
        s1 = _make_source("key", "value")
        s2 = _make_source("key", "score")
        inv = OperatorInvocation(pod=join, input_streams=(s1, s2))
        assert isinstance(inv, OperatorInvocation)
        assert not isinstance(inv, FunctionInvocation)
```

- [ ] **Step 1.2: Create test_pipeline_base_recording.py**

```python
"""Tests for the unified recording path in AbstractPipelineBase.

These tests verify the new _record_invocation / _invocation_lut / _source_streams
mechanics by exercising them through Pipeline (concrete subclass).
"""
from __future__ import annotations

import pytest
import pyarrow as pa

from orcapod.core.function_pod import FunctionPod
from orcapod.core.data_function import PythonDataFunction
from orcapod.core.operators import Join
from orcapod.core.sources import ArrowTableSource
from orcapod.pipeline import Pipeline


def _src(tag_col: str, data_col: str) -> ArrowTableSource:
    table = pa.table({
        tag_col: pa.array(["a", "b"], type=pa.large_string()),
        data_col: pa.array([1, 2], type=pa.int64()),
    })
    return ArrowTableSource(table, tag_columns=[tag_col], infer_nullable=True)


def _fn(name: str = "double") -> FunctionPod:
    def fn(value: int) -> int:
        return value * 2
    fn.__name__ = name
    return FunctionPod(PythonDataFunction(fn))


class TestRecordFunctionPodInvocation:
    def test_record_function_pod_adds_to_invocation_lut(self):
        """After recording a function pod, _invocation_lut must contain the invocation."""
        pod = _fn()
        stream = _src("key", "value")
        pipeline = Pipeline(name="test", auto_compile=False)
        with pipeline:
            pod(stream, label="out")
        # _invocation_lut is the new field; _node_lut is the legacy compiled field
        assert len(pipeline._invocation_lut) == 1

    def test_record_function_pod_captures_source_stream(self):
        """The upstream concrete stream must land in _source_streams."""
        pod = _fn()
        stream = _src("key", "value")
        pipeline = Pipeline(name="test", auto_compile=False)
        with pipeline:
            pod(stream, label="out")
        stream_hash = stream.content_hash().to_string()
        assert stream_hash in pipeline._source_streams

    def test_invocation_lut_additive_across_with_blocks(self):
        """Opening a second with-block appends to _invocation_lut, not replaces."""
        pod1 = _fn("fn1")
        pod2 = _fn("fn2")
        s = _src("key", "value")
        pipeline = Pipeline(name="test", auto_compile=False)
        with pipeline:
            out1 = pod1(s, label="step1")
        with pipeline:
            pod2(out1, label="step2")
        assert len(pipeline._invocation_lut) == 2

    def test_compile_rebuilds_persistent_node_map_from_scratch(self):
        """compile() must produce correct _persistent_node_map from _invocation_lut."""
        pod = _fn()
        stream = _src("key", "value")
        pipeline = Pipeline(name="test")
        with pipeline:
            pod(stream, label="out")
        assert "out" in pipeline._nodes
        assert len(pipeline._persistent_node_map) == 2  # 1 source + 1 function node

    def test_compile_creates_source_node_for_unregistered_upstream(self):
        """Streams with no recorded invocation must become SourceNode at compile time."""
        from orcapod.core.nodes.source_node import SourceNode
        pod = _fn()
        stream = _src("key", "value")
        pipeline = Pipeline(name="test")
        with pipeline:
            pod(stream, label="out")
        source_hash = stream.content_hash().to_string()
        assert isinstance(pipeline._persistent_node_map[source_hash], SourceNode)


class TestRecordOperatorPodInvocation:
    def test_record_operator_pod_adds_to_invocation_lut(self):
        """After recording an operator, _invocation_lut must contain the invocation."""
        join = Join()
        s1 = _src("key", "value")
        s2 = _src("key", "score")
        pipeline = Pipeline(name="test", auto_compile=False)
        with pipeline:
            join(s1, s2, label="joined")
        assert len(pipeline._invocation_lut) == 1

    def test_record_operator_captures_both_source_streams(self):
        join = Join()
        s1 = _src("key", "value")
        s2 = _src("key", "score")
        pipeline = Pipeline(name="test", auto_compile=False)
        with pipeline:
            join(s1, s2, label="joined")
        h1 = s1.content_hash().to_string()
        h2 = s2.content_hash().to_string()
        assert h1 in pipeline._source_streams
        assert h2 in pipeline._source_streams
```

- [ ] **Step 1.3: Create test_invocation_transitions.py**

```python
"""Tests for to_invocations / from_invocations and cross-class transitions."""
from __future__ import annotations

import pytest
import pyarrow as pa

from orcapod.core.function_pod import FunctionPod
from orcapod.core.data_function import PythonDataFunction
from orcapod.core.operators import Join
from orcapod.core.sources import ArrowTableSource
from orcapod.core.nodes.source_node import SourceNode, SourceJobNode
from orcapod.pipeline import Pipeline
from orcapod.pipeline.job import PipelineJob


def _src(tag_col: str, data_col: str) -> ArrowTableSource:
    table = pa.table({
        tag_col: pa.array(["a", "b"], type=pa.large_string()),
        data_col: pa.array([1, 2], type=pa.int64()),
    })
    return ArrowTableSource(table, tag_columns=[tag_col], infer_nullable=True)


def _fn() -> FunctionPod:
    def double(value: int) -> int:
        return value * 2
    return FunctionPod(PythonDataFunction(double))


class TestToInvocations:
    def test_returns_invocation_graph(self):
        from orcapod.pipeline.base import InvocationGraph
        pod = _fn()
        stream = _src("key", "value")
        pipeline = Pipeline(name="test")
        with pipeline:
            pod(stream, label="out")
        graph = pipeline.to_invocations()
        assert isinstance(graph, InvocationGraph)

    def test_invocation_graph_contains_one_invocation(self):
        from orcapod.pipeline.pod_invocation import FunctionInvocation
        pod = _fn()
        stream = _src("key", "value")
        pipeline = Pipeline(name="test")
        with pipeline:
            pod(stream, label="out")
        graph = pipeline.to_invocations()
        assert len(graph.invocations) == 1
        assert isinstance(graph.invocations[0], FunctionInvocation)

    def test_invocation_graph_source_streams_has_source_node(self):
        pod = _fn()
        stream = _src("key", "value")
        pipeline = Pipeline(name="test")
        with pipeline:
            pod(stream, label="out")
        graph = pipeline.to_invocations()
        assert len(graph.source_streams) == 1
        source = list(graph.source_streams.values())[0]
        assert isinstance(source, SourceNode)


class TestFromInvocations:
    def test_pipeline_from_invocations_roundtrip(self):
        """Pipeline → to_invocations() → Pipeline.from_invocations() must produce equivalent graph."""
        pod = _fn()
        stream = _src("key", "value")
        pipeline = Pipeline(name="test")
        with pipeline:
            pod(stream, label="out")
        graph = pipeline.to_invocations()
        pipeline2 = Pipeline.from_invocations(graph)
        assert "out" in pipeline2._nodes
        assert len(pipeline2._persistent_node_map) == len(pipeline._persistent_node_map)

    def test_pipeline_to_job_via_from_invocations(self):
        """PipelineJob.from_invocations(pipeline.to_invocations()) must produce bound-ready job."""
        pod = _fn()
        stream = _src("key", "value")
        pipeline = Pipeline(name="test")
        with pipeline:
            pod(stream, label="out")
        job = PipelineJob.from_invocations(pipeline.to_invocations())
        assert "out" in job._nodes
        # Source nodes should be unbound SourceJobNodes
        source_nodes = [
            n for n in job._persistent_node_map.values()
            if isinstance(n, SourceJobNode)
        ]
        assert len(source_nodes) == 1
        assert source_nodes[0].bound_source is None


class TestFromPipeline:
    def test_from_pipeline_thin_composition(self):
        """PipelineJob.from_pipeline(pipeline) must produce the same result as going via invocations."""
        pod = _fn()
        stream = _src("key", "value")
        pipeline = Pipeline(name="test")
        with pipeline:
            pod(stream, label="out")
        job = PipelineJob.from_pipeline(pipeline)
        assert "out" in job._nodes

    def test_as_pipeline_thin_composition(self):
        """job.as_pipeline() must produce a Pipeline structurally equivalent to the original."""
        pod = _fn()
        stream = _src("key", "value")
        pipeline = Pipeline(name="test")
        with pipeline:
            pod(stream, label="out")
        job = PipelineJob.from_pipeline(pipeline)
        pipeline2 = job.as_pipeline()
        assert "out" in pipeline2._nodes


class TestSourceJobNodeFromStream:
    def test_sjn_from_sjn_copies_bound_source(self):
        """SJN → SJN: bound_source is copied; the input SJN is NOT used as bound_source."""
        concrete = _src("key", "value")
        sjn = SourceJobNode(
            name="data",
            tag_schema=concrete.output_schema()[0],
            data_schema=concrete.output_schema()[1],
            bound_source=concrete,
        )
        sjn2 = SourceJobNode.from_stream(sjn)
        assert isinstance(sjn2, SourceJobNode)
        assert sjn2.bound_source is concrete  # bound_source copied, not sjn itself
        assert sjn2.bound_source is not sjn   # sjn itself is NOT the bound_source

    def test_sjn_from_source_node_creates_unbound(self):
        """SourceNode → SJN: creates unbound SourceJobNode with matching schema."""
        from orcapod.types import Schema
        sn = SourceNode(
            name="data",
            tag_schema=Schema({"key": str}),
            data_schema=Schema({"value": int}),
        )
        sjn = SourceJobNode.from_stream(sn)
        assert isinstance(sjn, SourceJobNode)
        assert sjn.bound_source is None
        assert sjn.name == "data"

    def test_sjn_from_concrete_stream_creates_bound(self):
        """Concrete stream → SJN: creates bound SourceJobNode; hash equals bound source hash."""
        concrete = _src("key", "value")
        sjn = SourceJobNode.from_stream(concrete)
        assert isinstance(sjn, SourceJobNode)
        assert sjn.bound_source is concrete
        assert sjn.content_hash() == concrete.content_hash()

    def test_unbound_sjn_hash_equals_source_node_hash(self):
        """Unbound SJN must have same content_hash as the corresponding SourceNode."""
        from orcapod.types import Schema
        sn = SourceNode(
            name="data",
            tag_schema=Schema({"key": str}),
            data_schema=Schema({"value": int}),
        )
        sjn = SourceJobNode.from_stream(sn)
        assert sjn.content_hash() == sn.content_hash()


class TestCrossWithBlockReconnection:
    def test_pipelinejob_same_concrete_source_in_two_blocks_same_hash(self):
        """PipelineJob: same concrete source in two with-blocks produces the same source node hash."""
        pod1 = _fn()
        pod2 = _fn()

        def double2(value: int) -> int:
            return value * 3

        pod2 = FunctionPod(PythonDataFunction(double2))
        concrete = _src("key", "value")

        job = PipelineJob(name="test", auto_compile=False)
        with job:
            out1 = pod1(concrete, label="step1")
        hash_after_first = concrete.content_hash().to_string()
        source_hash_first = list(
            h for h, n in job._persistent_node_map.items()
            if isinstance(n, SourceJobNode)
        )[0]
        with job:
            pod2(concrete, label="step2")
        source_hash_second = list(
            h for h, n in job._persistent_node_map.items()
            if isinstance(n, SourceJobNode)
        )[0]
        assert source_hash_first == source_hash_second

    def test_pipeline_source_hash_after_compile_differs_from_concrete_stream_hash(self):
        """Pipeline: SourceNode hash is schema-based, NOT the original concrete stream hash."""
        from orcapod.core.nodes.source_node import SourceNode
        pod = _fn()
        stream = _src("key", "value")
        pipeline = Pipeline(name="test")
        with pipeline:
            pod(stream, label="out")
        original_hash = stream.content_hash().to_string()
        # The source node in persistent_node_map should be a SourceNode whose hash
        # differs from the original concrete stream hash (schema-based normalization)
        source_nodes = {
            h: n for h, n in pipeline._persistent_node_map.items()
            if isinstance(n, SourceNode)
        }
        assert len(source_nodes) == 1
        source_node_hash = list(source_nodes.keys())[0]
        assert source_node_hash != original_hash
```

- [ ] **Step 1.4: Run all three test files and confirm they fail**

```bash
cd /home/kurouto/kurouto-jobs/c6a75f31-838c-4e23-9ec2-377eecdee49e/orcapod-python
uv run pytest tests/test_pipeline/test_pod_invocation.py tests/test_pipeline/test_pipeline_base_recording.py tests/test_pipeline/test_invocation_transitions.py -v 2>&1 | head -60
```

Expected: collection errors or `ImportError: cannot import name 'FunctionInvocation'`.

---

## Task 2: Create `pod_invocation.py`

**Files:**
- Create: `src/orcapod/pipeline/pod_invocation.py`

- [ ] **Step 2.1: Write pod_invocation.py**

```python
"""PodInvocation — minimal recording primitive for Pipeline and PipelineJob.

Records a single pod invocation (function or operator) against one or more
input streams.  Provides content-addressable identity so that the same
logical invocation always hashes to the same value regardless of the
recording order.

``identity_structure()`` mirrors ``StreamBase.identity_structure()`` —
``(pod, pod.argument_symmetry(input_streams))`` — which ensures that
``FunctionInvocation.content_hash() == FunctionNode.content_hash()`` and
``OperatorInvocation.content_hash() == OperatorNode.content_hash()`` for
the same pod and input streams.  This invariant lets ``_invocation_lut``
keys be reused as ``_persistent_node_map`` keys without a separate
translation step.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any

from orcapod.core.base import ContentIdentifiableBase

if TYPE_CHECKING:
    from orcapod.protocols.core_protocols import (
        FunctionPodProtocol,
        OperatorPodProtocol,
        StreamProtocol,
    )


class PodInvocation(ContentIdentifiableBase):
    """Abstract recording primitive for a pod applied to input streams.

    Args:
        pod: The pod being invoked (function or operator).
        input_streams: Tuple of upstream streams passed to the pod.
        label: Optional display label for the resulting compiled node.
    """

    def __init__(
        self,
        pod: Any,
        input_streams: "tuple[StreamProtocol, ...]",
        label: str | None = None,
    ) -> None:
        super().__init__()
        self._pod = pod
        self._input_streams = tuple(input_streams)
        self._label = label

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def pod(self) -> Any:
        """The pod being invoked."""
        return self._pod

    @property
    def input_streams(self) -> "tuple[StreamProtocol, ...]":
        """Upstream streams passed to this invocation."""
        return self._input_streams

    @property
    def label(self) -> str | None:
        """Optional display label for the compiled node."""
        return self._label

    # ------------------------------------------------------------------
    # Identity — mirrors StreamBase.identity_structure()
    # ------------------------------------------------------------------

    def identity_structure(self) -> Any:
        """Return ``(pod, pod.argument_symmetry(input_streams))``.

        This matches the identity structure of the corresponding compiled
        node, ensuring hash equality between invocation and node.
        """
        return (self._pod, self._pod.argument_symmetry(self._input_streams))

    def pipeline_identity_structure(self) -> Any:
        """Return same as ``identity_structure()``."""
        return self.identity_structure()


class FunctionInvocation(PodInvocation):
    """Invocation of a function pod against a single input stream.

    Args:
        pod: A ``FunctionPodProtocol`` instance.
        input_streams: Tuple with exactly one stream.
        label: Optional display label.
    """

    def __init__(
        self,
        pod: "FunctionPodProtocol",
        input_streams: "tuple[StreamProtocol, ...]",
        label: str | None = None,
    ) -> None:
        if len(input_streams) != 1:
            raise ValueError(
                f"FunctionInvocation requires exactly 1 input stream; "
                f"got {len(input_streams)}."
            )
        super().__init__(pod=pod, input_streams=input_streams, label=label)


class OperatorInvocation(PodInvocation):
    """Invocation of an operator pod against one or more input streams.

    Args:
        pod: An ``OperatorPodProtocol`` instance.
        input_streams: Tuple with one or more streams.
        label: Optional display label.
    """

    def __init__(
        self,
        pod: "OperatorPodProtocol",
        input_streams: "tuple[StreamProtocol, ...]",
        label: str | None = None,
    ) -> None:
        if len(input_streams) == 0:
            raise ValueError("OperatorInvocation requires at least 1 input stream.")
        super().__init__(pod=pod, input_streams=input_streams, label=label)
```

- [ ] **Step 2.2: Run PodInvocation tests — confirm tests 8a pass**

```bash
uv run pytest tests/test_pipeline/test_pod_invocation.py -v
```

Expected: all pass.

- [ ] **Step 2.3: Commit**

```bash
git add src/orcapod/pipeline/pod_invocation.py tests/test_pipeline/test_pod_invocation.py
git commit -m "feat(pipeline): add PodInvocation, FunctionInvocation, OperatorInvocation primitives"
```

---

## Task 3: Update `AbstractPipelineBase`

**Files:**
- Modify: `src/orcapod/pipeline/base.py`

This is the central change. Read `src/orcapod/pipeline/base.py` before editing.

The design uses a **separate** `_invocation_lut` field (stores `PodInvocation` objects during recording) rather than overloading `_node_lut`. After `compile()`, the legacy fields `_node_lut`, `_upstreams`, and `_graph_edges` are repopulated from `_persistent_node_map` and `_hash_graph` so that `_build_execution_graph()` in `PipelineJob` continues to work without modification.

- [ ] **Step 3.1: Replace base.py with the updated version**

Replace the entire file `src/orcapod/pipeline/base.py` with:

```python
"""AbstractPipelineBase — shared recording mechanism for Pipeline and PipelineJob."""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from orcapod.core.tracker import AutoRegisteringContextBasedTracker
from orcapod.pipeline.pod_invocation import (
    FunctionInvocation,
    OperatorInvocation,
    PodInvocation,
)
from orcapod.protocols import core_protocols as cp
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import networkx as nx
else:
    nx = LazyModule("networkx")

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class InvocationGraph:
    """Interchange value object between Pipeline and PipelineJob.

    Carries a topologically ordered tuple of ``PodInvocation`` objects and a
    mapping from content-hash string to the corresponding source stream.
    Both classes exchange representations by converting to this neutral form
    via ``to_invocations()`` and reconstructing via ``from_invocations()``.

    Args:
        invocations: Topologically ordered sequence of pod invocations
            (sources excluded).
        source_streams: Mapping of content-hash string → source stream
            (nodes whose invocation was not recorded in the pipeline).
    """

    invocations: tuple[PodInvocation, ...]
    source_streams: dict[str, Any]  # hash → StreamProtocol-compatible node


class AbstractPipelineBase(AutoRegisteringContextBasedTracker, ABC):
    """Shared recording mechanism and graph state for Pipeline and PipelineJob.

    Manages the ``with``-block recording phase: accumulating invocations into
    ``_invocation_lut``, capturing raw source streams into ``_source_streams``,
    and building the topology in ``_hash_graph``.  On context exit, ``compile()``
    materialises the accumulated state into a frozen DAG of node objects.

    ``_invocation_lut`` and ``_source_streams`` are **additive** — they persist
    across multiple ``with`` blocks so that repeated recording sessions extend
    the same graph rather than replacing it.

    Args:
        name: Pipeline name (string or tuple). Used to scope database paths.
        tracker_manager: Optional tracker manager override.
    """

    def __init__(
        self,
        name: str | tuple[str, ...] = "pipeline",
        tracker_manager: cp.TrackerManagerProtocol | None = None,
    ) -> None:
        super().__init__(tracker_manager=tracker_manager)
        self._name: tuple[str, ...] = (name,) if isinstance(name, str) else tuple(name)

        # --- Additive recording state (never cleared) -----------------
        # Maps content-hash-string → PodInvocation for each recorded invocation.
        self._invocation_lut: dict[str, PodInvocation] = {}
        # Maps content-hash-string → raw stream for each unregistered upstream.
        self._source_streams: dict[str, Any] = {}
        # Topology graph — vertices and edges are content-hash strings.
        # Additive: persists and grows across multiple with-blocks.
        self._hash_graph: "nx.DiGraph" = nx.DiGraph()

        # --- Compiled state (populated / replaced by compile()) --------
        self._persistent_node_map: dict[str, Any] = {}
        self._nodes: dict[str, Any] = {}
        self._node_graph: "nx.DiGraph | None" = None
        self._compiled: bool = False

        # --- Legacy fields kept for _build_execution_graph() compat ----
        # Populated from _persistent_node_map / _hash_graph at end of compile().
        self._node_lut: dict[str, Any] = {}       # hash → non-source compiled node
        self._upstreams: dict[str, Any] = {}      # hash → source compiled node
        self._graph_edges: list[tuple[str, str]] = []  # edge list from _hash_graph

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def name(self) -> tuple[str, ...]:
        """Pipeline name tuple."""
        return self._name

    @property
    def graph(self) -> "nx.DiGraph":
        """Directed hash graph of accumulated pipeline structure."""
        return self._hash_graph

    @property
    def compiled_nodes(self) -> dict[str, Any]:
        """Copy of the compiled nodes dict (label → node)."""
        return self._nodes.copy()

    # ------------------------------------------------------------------
    # Abstract — subclass node-factory declarations
    # ------------------------------------------------------------------

    @property
    @abstractmethod
    def source_node_class(self) -> type:
        """Node class to use for source (leaf) nodes — e.g. ``SourceNode``."""
        ...

    @property
    @abstractmethod
    def function_node_class(self) -> type:
        """Node class to use for function-pod invocations — e.g. ``FunctionNode``."""
        ...

    @property
    @abstractmethod
    def operator_node_class(self) -> type:
        """Node class to use for operator-pod invocations — e.g. ``OperatorNode``."""
        ...

    # ------------------------------------------------------------------
    # Recording
    # ------------------------------------------------------------------

    def record_function_pod_invocation(
        self,
        pod: "cp.FunctionPodProtocol",
        input_stream: "cp.StreamProtocol",
        label: str | None = None,
    ) -> None:
        """Record a function pod invocation into the graph.

        Args:
            pod: The function pod being invoked.
            input_stream: The upstream stream.
            label: Optional display label for the resulting compiled node.
        """
        self._record_invocation(FunctionInvocation(pod=pod, input_streams=(input_stream,), label=label))

    def record_operator_pod_invocation(
        self,
        pod: "cp.OperatorPodProtocol",
        upstreams: "tuple[cp.StreamProtocol, ...]" = (),
        label: str | None = None,
    ) -> None:
        """Record an operator pod invocation into the graph.

        Args:
            pod: The operator pod being invoked.
            upstreams: Upstream streams for this operator.
            label: Optional display label for the resulting compiled node.
        """
        self._record_invocation(OperatorInvocation(pod=pod, input_streams=tuple(upstreams), label=label))

    def _record_invocation(self, invocation: PodInvocation) -> None:
        """Store *invocation* and update the topology graph.

        The content hash of a ``PodInvocation`` equals the content hash of the
        corresponding compiled node (``FunctionNode`` / ``OperatorNode``), so the
        same hash key works in both ``_invocation_lut`` and ``_persistent_node_map``.

        Args:
            invocation: The pod invocation to record.
        """
        key = invocation.content_hash().to_string()
        self._invocation_lut[key] = invocation
        self._hash_graph.add_node(key)
        for upstream in invocation.input_streams:
            upstream_hash = upstream.content_hash().to_string()
            self._hash_graph.add_edge(upstream_hash, key)
            if upstream_hash not in self._source_streams:
                self._source_streams[upstream_hash] = upstream

    # ------------------------------------------------------------------
    # reset() — no-op after refactor
    # ------------------------------------------------------------------

    def reset(self) -> None:
        """No-op — all recording state is additive and persists across with-blocks.

        Note:
            ``_invocation_lut``, ``_source_streams``, and ``_hash_graph`` intentionally
            accumulate across ``with`` blocks. ``_persistent_node_map``, ``_node_lut``,
            ``_upstreams``, and ``_graph_edges`` are compiled artifacts overwritten by
            each ``compile()`` call.  Nothing needs to be cleared on re-entry.
        """

    def __exit__(self, exc_type=None, exc_value=None, traceback=None) -> None:
        """Exit the recording context, compiling if no exception occurred."""
        super().__exit__(exc_type, exc_value, traceback)
        if exc_type is None:
            self.compile()

    def __getattr__(self, item: str) -> Any:
        """Look up compiled nodes by label as attribute access."""
        if item.startswith("_"):
            raise AttributeError(item)
        nodes = object.__getattribute__(self, "_nodes")
        if item in nodes:
            return nodes[item]
        raise AttributeError(
            f"{type(self).__name__!r} has no attribute {item!r}. "
            f"Available node labels: {sorted(nodes.keys())}"
        )

    # ------------------------------------------------------------------
    # compile() — single-pass, class-property driven
    # ------------------------------------------------------------------

    def compile(self) -> None:
        """Compile recorded invocations into a frozen DAG.

        Walks ``_hash_graph`` topologically.  Streams that appear as inputs
        but have no registered invocation are promoted to source nodes via
        ``self.source_node_class.from_stream()``.  Each ``PodInvocation`` in
        ``_invocation_lut`` becomes a ``function_node_class`` or
        ``operator_node_class`` instance.

        After compilation:
        - ``_persistent_node_map`` — hash → compiled node (all node types)
        - ``_nodes`` — label → compiled node (labelled nodes only)
        - ``_node_graph`` — nx.DiGraph with node objects as vertices
        - Legacy fields ``_node_lut``, ``_upstreams``, ``_graph_edges``
          are repopulated for backward compat with ``_build_execution_graph()``.
        - ``_compiled`` is set to ``True``.

        This method always rebuilds from scratch; it does NOT perform
        incremental compilation.
        """
        import networkx as _nx

        source_node_cls = self.source_node_class

        # 1. Source hashes: inputs that have no registered invocation.
        source_hashes = set(self._source_streams.keys()) - set(self._invocation_lut.keys())

        # 2. Create source nodes.
        node_map: dict[str, Any] = {
            h: source_node_cls.from_stream(self._source_streams[h])
            for h in source_hashes
            if h in self._source_streams
        }

        # 3. Topological pass — create function / operator nodes.
        for key in _nx.topological_sort(self._hash_graph):
            if key in node_map:
                continue  # already a source node
            if key not in self._invocation_lut:
                continue  # vertex in hash_graph with no invocation (e.g. pure source hash)
            inv = self._invocation_lut[key]
            upstream_nodes = [
                node_map[up.content_hash().to_string()]
                for up in inv.input_streams
            ]
            if isinstance(inv, FunctionInvocation):
                node_map[key] = self.function_node_class(
                    function_pod=inv.pod,
                    input_stream=upstream_nodes[0],
                    label=inv.label,
                )
            else:
                node_map[key] = self.operator_node_class(
                    operator=inv.pod,
                    input_streams=tuple(upstream_nodes),
                    label=inv.label,
                )

        self._persistent_node_map = node_map

        # 4. Label disambiguation (preserves existing Pipeline.compile() behavior).
        name_candidates: dict[str, list] = {}
        for node in node_map.values():
            name_candidates.setdefault(node.label, []).append(node)

        self._nodes.clear()
        for label, nodes in name_candidates.items():
            if len(nodes) > 1:
                sorted_nodes = sorted(nodes, key=lambda n: n.content_hash().to_string())
                for i, node in enumerate(sorted_nodes, start=1):
                    key = f"{label}_{i}"
                    self._nodes[key] = node
                    node._label = key
            else:
                self._nodes[label] = nodes[0]

        # 5. Build node_graph (DiGraph with node objects as vertices).
        self._node_graph = _nx.DiGraph()
        for up_hash, down_hash in self._hash_graph.edges():
            up_node = node_map.get(up_hash)
            down_node = node_map.get(down_hash)
            if up_node is not None and down_node is not None:
                self._node_graph.add_edge(up_node, down_node)
        for node in node_map.values():
            if node not in self._node_graph:
                self._node_graph.add_node(node)

        # 6. Enrich hash_graph node attributes (used by GraphRenderer and serialization).
        for node_hash, node in node_map.items():
            if node_hash not in self._hash_graph:
                continue
            attrs = self._hash_graph.nodes[node_hash]
            if not attrs.get("node_type"):
                attrs["node_type"] = node.node_type
            if not attrs.get("label"):
                computed = node._label or (
                    node.computed_label() if hasattr(node, "computed_label") else None
                )
                if computed:
                    attrs["label"] = computed
            if not attrs.get("pipeline_hash"):
                attrs["pipeline_hash"] = node.pipeline_hash().to_string()

        # 7. Populate legacy fields for _build_execution_graph() backward compat.
        self._node_lut = {
            h: n for h, n in node_map.items()
            if not isinstance(n, source_node_cls)
        }
        self._upstreams = {
            h: n for h, n in node_map.items()
            if isinstance(n, source_node_cls)
        }
        self._graph_edges = list(self._hash_graph.edges())

        self._compiled = True

    # ------------------------------------------------------------------
    # InvocationGraph transitions
    # ------------------------------------------------------------------

    def to_invocations(self) -> InvocationGraph:
        """Extract an ``InvocationGraph`` from the compiled ``_persistent_node_map``.

        Reconstructs from ``_persistent_node_map`` (not raw ``_invocation_lut``) so
        that the in-memory path and the save/load path produce consistent results.
        Source nodes (leaves) go into ``source_streams``; function and operator
        nodes become ``FunctionInvocation`` / ``OperatorInvocation`` objects.

        Returns:
            An ``InvocationGraph`` with topologically ordered invocations and
            a mapping of hash → source node (which acts as its own stream).

        Raises:
            RuntimeError: If the pipeline has not been compiled.
        """
        import networkx as _nx

        if not self._compiled:
            raise RuntimeError(
                "Cannot call to_invocations() before compile(). "
                "Use 'with pipeline:' or call compile() first."
            )

        source_node_cls = self.source_node_class
        fn_node_cls = self.function_node_class
        source_streams: dict[str, Any] = {}
        invocations: list[PodInvocation] = []

        for node_hash, node in self._persistent_node_map.items():
            if isinstance(node, source_node_cls):
                source_streams[node_hash] = node  # source node IS its own stream
            elif isinstance(node, fn_node_cls):
                invocations.append(
                    FunctionInvocation(
                        pod=node._function_pod,
                        input_streams=(node.upstreams[0],),
                        label=node._label,
                    )
                )
            else:
                invocations.append(
                    OperatorInvocation(
                        pod=node._operator,
                        input_streams=tuple(node.upstreams),
                        label=node._label,
                    )
                )

        # Sort topologically using _hash_graph.
        topo_keys = list(_nx.topological_sort(self._hash_graph))
        inv_by_hash = {inv.content_hash().to_string(): inv for inv in invocations}
        ordered = [inv_by_hash[k] for k in topo_keys if k in inv_by_hash]

        return InvocationGraph(
            invocations=tuple(ordered),
            source_streams=source_streams,
        )

    @classmethod
    def from_invocations(
        cls,
        graph: InvocationGraph,
        name: str | tuple[str, ...] = "pipeline",
    ) -> "AbstractPipelineBase":
        """Reconstruct a Pipeline or PipelineJob from an ``InvocationGraph``.

        Calls ``cls(name=name)`` to properly initialise all fields, then
        populates ``_invocation_lut``, ``_source_streams``, and ``_hash_graph``
        from *graph* before calling ``compile()``.

        This classmethod lives on ``AbstractPipelineBase`` (not on the
        subclasses) so that neither ``Pipeline`` nor ``PipelineJob`` needs
        to access the other's private state directly.

        Args:
            graph: The ``InvocationGraph`` to reconstruct from.
            name: Pipeline name for the new instance.

        Returns:
            A compiled instance of ``cls``.
        """
        instance = cls(name=name)
        instance._invocation_lut = {
            inv.content_hash().to_string(): inv
            for inv in graph.invocations
        }
        instance._source_streams = dict(graph.source_streams)
        for inv in graph.invocations:
            instance._hash_graph.add_node(inv.content_hash().to_string())
            for upstream in inv.input_streams:
                instance._hash_graph.add_edge(
                    upstream.content_hash().to_string(),
                    inv.content_hash().to_string(),
                )
        # Also add source hashes as isolated nodes (no incoming edges)
        for h in graph.source_streams:
            if h not in instance._hash_graph:
                instance._hash_graph.add_node(h)
        instance.compile()
        return instance
```

- [ ] **Step 3.2: Run base recording tests**

```bash
uv run pytest tests/test_pipeline/test_pipeline_base_recording.py -v
```

Expected: most or all pass (Pipeline still has its own compile() — that gets removed in Task 4, but the recording path is now live).

Note: `test_invocation_lut_additive_across_with_blocks` will only pass once Pipeline's `__enter__` no longer calls `reset()` (it doesn't — Pipeline has no `__enter__` override, and `AutoRegisteringContextBasedTracker.__enter__` does not call `reset()`). If tests fail due to compile override still present in Pipeline, they will be fixed in Task 4.

- [ ] **Step 3.3: Commit base changes**

```bash
git add src/orcapod/pipeline/base.py tests/test_pipeline/test_pipeline_base_recording.py tests/test_pipeline/test_invocation_transitions.py
git commit -m "refactor(pipeline): unify recording in AbstractPipelineBase with _invocation_lut and shared compile()"
```

---

## Task 4: Update `Pipeline`

**Files:**
- Modify: `src/orcapod/pipeline/graph.py`

Read `src/orcapod/pipeline/graph.py` before editing. The Pipeline class starts at line 34. The GraphRenderer and rendering utilities begin at line 625 — **leave them entirely unchanged.**

- [ ] **Step 4.1: Remove recording overrides and compile() from Pipeline; add class-property attributes**

Replace the entire `Pipeline` class (lines 34–618 in `graph.py`, i.e., the `Pipeline` class definition through `_clone_for_execution` and `__dir__`, but before `GraphRenderer`) with the following. The GraphRenderer section that starts at the comment `# ===========================================================================` stays unchanged.

```python
class Pipeline(AbstractPipelineBase):
    """A pure computational blueprint recording operator and function pod invocations.

    During the ``with`` block, operator and function pod invocations are
    recorded into an internal graph via the unified ``_record_invocation()``
    path inherited from ``AbstractPipelineBase``. On context exit,
    ``compile()`` (also inherited) rewires the graph into a frozen DAG:

    - Leaf streams not registered as invocations → ``SourceNode`` declarations
    - Function pod invocations → ``FunctionNode``
    - Operator invocations → ``OperatorNode``

    To run a ``Pipeline``, use
    ``PipelineJob.from_pipeline(pipeline, sources=..., store=...)`` to create
    a ``PipelineJob``.

    Args:
        name: Pipeline name (string or tuple). Used as the path prefix for
            all cache/pipeline paths when the pipeline is run via a
            ``PipelineJob``.
        auto_compile: If ``True`` (default), ``compile()`` is called
            automatically when the context manager exits.
    """

    # ------------------------------------------------------------------
    # Node-factory class attributes (used by AbstractPipelineBase.compile())
    # ------------------------------------------------------------------

    @property
    def source_node_class(self) -> type:
        from orcapod.core.nodes.source_node import SourceNode
        return SourceNode

    @property
    def function_node_class(self) -> type:
        from orcapod.core.nodes.function_node import FunctionNode
        return FunctionNode

    @property
    def operator_node_class(self) -> type:
        from orcapod.core.nodes.operator_node import OperatorNode
        return OperatorNode

    def __init__(
        self,
        name: str | tuple[str, ...],
        tracker_manager: cp.TrackerManagerProtocol | None = None,
        auto_compile: bool = True,
    ) -> None:
        """Initialize a pure computational blueprint pipeline.

        Args:
            name: Pipeline name (string or tuple).
            tracker_manager: Optional tracker manager override.
            auto_compile: If ``True`` (default), ``compile()`` is called
                automatically when the context manager exits.
        """
        super().__init__(name=name, tracker_manager=tracker_manager)
        self._auto_compile = auto_compile

    @property
    def nodes(self) -> list:
        """Return the list of compiled non-source nodes (by label)."""
        return list(self._persistent_node_map.values())

    # ------------------------------------------------------------------
    # Context manager — respects auto_compile flag
    # ------------------------------------------------------------------

    def __exit__(self, exc_type=None, exc_value=None, traceback=None):
        # Call AutoRegisteringContextBasedTracker.__exit__ directly so we can
        # gate compile() on the auto_compile flag, bypassing the base class
        # __exit__ which always calls compile().
        AutoRegisteringContextBasedTracker.__exit__(self, exc_type, exc_value, traceback)
        if exc_type is None and self._auto_compile:
            self.compile()

    # ------------------------------------------------------------------
    # Graph display
    # ------------------------------------------------------------------

    def show_graph(self, **kwargs) -> str | None:
        """Render the pipeline's node graph.

        Args:
            **kwargs: Forwarded to ``render_graph``.

        Raises:
            RuntimeError: If the pipeline has not been compiled yet.
        """
        if self._node_graph is None:
            raise RuntimeError("Pipeline must be compiled before showing the graph.")
        return render_graph(self._node_graph, **kwargs)

    # ------------------------------------------------------------------
    # Serialization
    # ------------------------------------------------------------------

    def save(self, path: "str | Path") -> None:
        """Serialize the pure pipeline blueprint to a JSON file.

        Saves the full pipeline topology: SourceNode declarations, function
        and operator pod configurations, and all edge connections.  Runtime
        state — databases, execution context, and run metadata — is not
        persisted.

        Args:
            path: File path to write JSON output to.

        Raises:
            ValueError: If the pipeline has not been compiled.
        """
        if not self._compiled:
            raise ValueError(
                "Pipeline is not compiled. Call compile() or use "
                "auto_compile=True before saving."
            )

        import json as _json
        from orcapod.pipeline.serialization import (
            PIPELINE_FORMAT_VERSION,
            serialize_schema,
        )
        from orcapod.core.nodes import OperatorNode, FunctionNode
        from orcapod.core.nodes.source_node import SourceNode as SourceNodeClass

        nodes: dict[str, Any] = {}
        for content_hash_str, node in self._persistent_node_map.items():
            tag_schema, data_schema = node.output_schema()
            try:
                type_converter = node.data_context.type_converter
            except (AttributeError, TypeError):
                from orcapod.contexts import resolve_context
                type_converter = resolve_context(None).type_converter

            try:
                data_context_key = node.data_context_key
            except (AttributeError, TypeError):
                _dc = getattr(node, "_data_context", None)
                data_context_key = _dc.context_key if _dc is not None else None

            descriptor: dict[str, Any] = {
                "node_type": node.node_type,
                "label": node.label,
                "content_hash": node.content_hash().to_string(),
                "pipeline_hash": node.pipeline_hash().to_string(),
                "output_schema": {
                    "tag": serialize_schema(tag_schema, type_converter),
                    "data": serialize_schema(data_schema, type_converter),
                },
                "node_uri": list(node.node_uri),
                "data_context_key": data_context_key,
            }

            match node:
                case SourceNodeClass():
                    descriptor["source_config"] = {
                        "source_type": "node",
                        "name": node.name,
                        "tag_schema": serialize_schema(node.tag_schema, type_converter),
                        "data_schema": serialize_schema(node.data_schema, type_converter),
                    }
                    descriptor["reconstructable"] = True

                case FunctionNode():
                    if node._function_pod is not None:
                        descriptor["function_config"] = node._function_pod.to_config()
                    descriptor["table_scope"] = node._table_scope

                case OperatorNode():
                    if node._operator is not None:
                        descriptor["operator_config"] = node._operator.to_config()
                    descriptor["table_scope"] = node._table_scope

            nodes[content_hash_str] = descriptor

        output: dict[str, Any] = {
            "orcapod_pipeline_version": PIPELINE_FORMAT_VERSION,
            "pipeline": {"name": list(self._name)},
            "nodes": nodes,
            "edges": [list(edge) for edge in self._graph_edges],
        }

        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            _json.dump(output, f, indent=2)

    @classmethod
    def load(cls, path: "str | Path") -> "Pipeline":
        """Deserialize a pure pipeline blueprint from a JSON file.

        Reconstructs topology and SourceNode declarations. The loaded
        pipeline is topology-only — to run it, use
        ``PipelineJob.from_pipeline(pipeline, sources=..., store=...)``.

        Args:
            path: Path to the JSON file produced by :meth:`save`.

        Returns:
            A compiled ``Pipeline`` instance with SourceNode leaf nodes.

        Raises:
            ValueError: If the file's format version is unsupported.
        """
        import json as _json
        from orcapod.pipeline.serialization import (
            SUPPORTED_FORMAT_VERSIONS,
            deserialize_schema,
        )
        from orcapod.core.nodes import FunctionNode, OperatorNode
        from orcapod.core.nodes.source_node import SourceNode as SourceNodeClass
        from orcapod.types import Schema

        path = Path(path)
        with open(path) as f:
            data = _json.load(f)

        version = data.get("orcapod_pipeline_version", "")
        if version not in SUPPORTED_FORMAT_VERSIONS:
            raise ValueError(
                f"Unsupported pipeline format version {version!r}. "
                f"Supported: {sorted(SUPPORTED_FORMAT_VERSIONS)}"
            )

        pipeline_meta = data["pipeline"]
        name = tuple(pipeline_meta["name"])
        nodes_data = data["nodes"]
        edges = data["edges"]

        edge_graph: "nx.DiGraph" = nx.DiGraph()
        for up_hash, down_hash in edges:
            edge_graph.add_edge(up_hash, down_hash)
        for node_hash in nodes_data:
            if node_hash not in edge_graph:
                edge_graph.add_node(node_hash)
        topo_order = list(nx.topological_sort(edge_graph))

        upstream_map: dict[str, list[str]] = {}
        for up_hash, down_hash in edges:
            upstream_map.setdefault(down_hash, []).append(up_hash)

        reconstructed: dict[str, GraphNode] = {}

        for node_hash in topo_order:
            descriptor = nodes_data.get(node_hash)
            if descriptor is None:
                continue

            node_type = descriptor.get("node_type")
            source_config = descriptor.get("source_config") or {}

            if node_type == "source":
                source_type = source_config.get("source_type")
                if source_type == "node":
                    node_name = source_config.get("name") or source_config.get("node_name")
                    if not node_name:
                        node_name = descriptor.get("label") or "unknown"
                    if "tag_schema" in source_config and "data_schema" in source_config:
                        tag_schema = Schema(deserialize_schema(source_config["tag_schema"]))
                        data_schema = Schema(deserialize_schema(source_config["data_schema"]))
                    else:
                        tag_schema = Schema(deserialize_schema(descriptor["output_schema"]["tag"]))
                        data_schema = Schema(deserialize_schema(descriptor["output_schema"]["data"]))
                    node = SourceNodeClass(
                        name=node_name,
                        tag_schema=tag_schema,
                        data_schema=data_schema,
                        data_context=descriptor.get("data_context_key"),
                    )
                    stored_label = descriptor.get("label")
                    if stored_label and stored_label != node_name:
                        node._label = stored_label
                else:
                    raise ValueError(
                        f"Unknown source_type {source_type!r} in pipeline descriptor."
                    )
                reconstructed[node_hash] = node

            elif node_type == "function":
                up_hashes = upstream_map.get(node_hash, [])
                upstream_node = reconstructed.get(up_hashes[0]) if up_hashes else None
                node = FunctionNode.from_descriptor(
                    descriptor, function_pod=None, input_stream=upstream_node, databases={}
                )
                reconstructed[node_hash] = node

            elif node_type == "operator":
                up_hashes = upstream_map.get(node_hash, [])
                upstream_nodes = tuple(
                    reconstructed[h] for h in up_hashes if h in reconstructed
                )
                operator = None
                op_config = descriptor.get("operator_config")
                if op_config:
                    try:
                        from orcapod.pipeline.serialization import resolve_operator_from_config
                        operator = resolve_operator_from_config(op_config)
                    except Exception as exc:
                        logger.warning(
                            "Could not reconstruct operator %r from config — "
                            "node will be in read-only mode: %s",
                            op_config.get("class_name"),
                            exc,
                        )
                node = OperatorNode.from_descriptor(
                    descriptor, operator=operator, input_streams=upstream_nodes, databases={}
                )
                reconstructed[node_hash] = node

        # Build Pipeline instance — bypass the recording path entirely.
        pipeline = cls(name=name, auto_compile=False)
        pipeline._persistent_node_map = dict(reconstructed)

        nodes_by_label: dict[str, GraphNode] = {}
        for node in reconstructed.values():
            if node.label:
                if node.label not in nodes_by_label:
                    nodes_by_label[node.label] = node
                else:
                    logger.warning("Label collision in loaded pipeline: %r.", node.label)
        pipeline._nodes = nodes_by_label

        pipeline._node_graph = nx.DiGraph()
        for up_hash, down_hash in edges:
            up_node = reconstructed.get(up_hash)
            down_node = reconstructed.get(down_hash)
            if up_node is not None and down_node is not None:
                pipeline._node_graph.add_edge(up_node, down_node)
        for node in reconstructed.values():
            if node not in pipeline._node_graph:
                pipeline._node_graph.add_node(node)

        pipeline._graph_edges = [(up, down) for up, down in edges]
        pipeline._hash_graph = nx.DiGraph()
        for up_hash, down_hash in edges:
            pipeline._hash_graph.add_edge(up_hash, down_hash)
        for node_hash, node in reconstructed.items():
            if node_hash not in pipeline._hash_graph:
                pipeline._hash_graph.add_node(node_hash)
            attrs = pipeline._hash_graph.nodes[node_hash]
            attrs["node_type"] = node.node_type
            if node.label:
                attrs["label"] = node.label

        # Populate legacy fields (as produced by compile()) for _build_execution_graph().
        pipeline._node_lut = {
            h: n for h, n in reconstructed.items() if n.node_type != "source"
        }
        pipeline._upstreams = {
            h: n for h, n in reconstructed.items() if n.node_type == "source"
        }

        pipeline._compiled = True
        return pipeline

    def _clone_for_execution(self) -> "Pipeline":
        """Create a lightweight copy of this compiled pipeline for isolated execution.

        All structural state is shared read-only with the original.
        Only ``_nodes`` gets its own copy so that execution-node label
        mutations don't affect other jobs sharing this blueprint.

        Returns:
            A new ``Pipeline`` instance sharing read-only state with ``self``.
        """
        clone = Pipeline.__new__(Pipeline)
        clone._tracker_manager = self._tracker_manager
        clone._active = False
        clone._name = self._name
        clone._invocation_lut = self._invocation_lut
        clone._source_streams = self._source_streams
        clone._node_lut = self._node_lut
        clone._upstreams = self._upstreams
        clone._graph_edges = self._graph_edges
        clone._hash_graph = self._hash_graph
        clone._persistent_node_map = self._persistent_node_map
        clone._node_graph = self._node_graph
        clone._auto_compile = self._auto_compile
        clone._compiled = self._compiled
        clone._nodes = dict(self._nodes)  # own copy — mutations don't affect original
        return clone

    def __dir__(self) -> list[str]:
        return list(super().__dir__()) + list(self._nodes.keys())
```

- [ ] **Step 4.2: Run the full existing pipeline test suite**

```bash
uv run pytest tests/test_pipeline/ -v --tb=short 2>&1 | tail -40
```

Expected: existing tests should mostly pass. New tests in `test_pipeline_base_recording.py` and `test_invocation_transitions.py` should now start passing too (Pipeline no longer has its own compile() competing with the base).

- [ ] **Step 4.3: Fix any import errors in Pipeline**

The new `Pipeline` class body references `cp` (the `core_protocols` import) and `AutoRegisteringContextBasedTracker` — both are already imported at the top of `graph.py`. Confirm the imports at the top of `graph.py` still include:

```python
from orcapod.core.nodes import (
    FunctionNode,
    GraphNode,
    OperatorNode,
    SourceNode,
)
from orcapod.core.tracker import AutoRegisteringContextBasedTracker
from orcapod.pipeline.base import AbstractPipelineBase
from orcapod.protocols import core_protocols as cp
```

The node imports (`FunctionNode`, `GraphNode`, `OperatorNode`, `SourceNode`) are still needed by `save()` and `load()`. Leave them.

- [ ] **Step 4.4: Commit**

```bash
git add src/orcapod/pipeline/graph.py
git commit -m "refactor(pipeline): Pipeline sheds recording/compile overrides; uses AbstractPipelineBase.compile()"
```

---

## Task 5: Add `SourceJobNode.from_stream()` three-way logic

**Files:**
- Modify: `src/orcapod/core/nodes/source_node.py`

Read `src/orcapod/core/nodes/source_node.py` before editing. Locate the `SourceJobNode` class (around line 395). After the `as_node()` method (around line 511), add the new `from_stream()` classmethod.

- [ ] **Step 5.1: Run SourceJobNode.from_stream tests to confirm they fail**

```bash
uv run pytest tests/test_pipeline/test_invocation_transitions.py::TestSourceJobNodeFromStream -v
```

Expected: `AttributeError: type object 'SourceJobNode' has no attribute 'from_stream'`

- [ ] **Step 5.2: Add from_stream() to SourceJobNode**

In `src/orcapod/core/nodes/source_node.py`, add the following classmethod to `SourceJobNode` immediately after the `as_node()` method (after line 521):

```python
    @classmethod
    def from_stream(
        cls,
        stream: "StreamProtocol",
        name: str | None = None,
    ) -> "SourceJobNode":
        """Create a ``SourceJobNode`` from *stream*, applying three-way logic.

        The input type determines the binding behavior:

        * ``SourceJobNode`` input — copy schema and ``bound_source``; do **not**
          wrap the input SJN itself as the bound source (avoids double-wrapping).
        * ``SourceNode`` input — create an **unbound** ``SourceJobNode`` preserving
          the schema placeholder semantics. The unbound SJN has the same
          ``content_hash()`` as the original ``SourceNode`` (both schema-based).
        * Any other stream — create a **bound** ``SourceJobNode`` with the stream
          as ``bound_source``. The SJN's ``content_hash()`` equals the stream's.

        This three-way logic is critical for the Pipeline → PipelineJob transition:
        ``Pipeline.to_invocations()`` produces ``SourceNode``-backed source streams,
        and ``PipelineJob.from_invocations()`` must preserve the
        concrete/placeholder distinction when calling this method.

        Args:
            stream: The upstream stream to wrap.
            name: Optional explicit slot name. Defaults to ``stream.label`` for
                non-SJN/SourceNode inputs; ``stream.name`` for SJN and SourceNode.

        Returns:
            A new ``SourceJobNode`` with binding reflecting the input type.
        """
        if isinstance(stream, SourceJobNode):
            # SJN → SJN: copy bound_source (do NOT wrap the SJN itself)
            return cls(
                name=name if name is not None else stream.name,
                tag_schema=stream.tag_schema,
                data_schema=stream.data_schema,
                bound_source=stream.bound_source,
            )
        elif isinstance(stream, SourceNode):
            # SourceNode → unbound SJN: preserve schema placeholder
            return cls(
                name=name if name is not None else stream.name,
                tag_schema=stream.tag_schema,
                data_schema=stream.data_schema,
                bound_source=None,
            )
        else:
            # Concrete stream → bound SJN
            tag_schema, data_schema = stream.output_schema()
            slot_name = name if name is not None else stream.label
            return cls(
                name=slot_name,
                tag_schema=tag_schema,
                data_schema=data_schema,
                bound_source=stream,
            )
```

- [ ] **Step 5.3: Run SourceJobNode.from_stream tests**

```bash
uv run pytest tests/test_pipeline/test_invocation_transitions.py::TestSourceJobNodeFromStream -v
```

Expected: all 4 tests pass.

- [ ] **Step 5.4: Commit**

```bash
git add src/orcapod/core/nodes/source_node.py
git commit -m "feat(source_node): add SourceJobNode.from_stream() with three-way input logic"
```

---

## Task 6: Update `PipelineJob`

**Files:**
- Modify: `src/orcapod/pipeline/job.py`

Read `src/orcapod/pipeline/job.py` before editing. This is a large file — make targeted changes described below rather than rewriting the whole class.

- [ ] **Step 6.1: Update PipelineJob.__init__ — remove _rec_* fields**

In `__init__`, remove these six lines:
```python
        # Recording state (populated during with-block)
        self._rec_graph_edges: list[tuple[str, str]] = []
        self._rec_upstreams: dict[str, cp.StreamProtocol] = {}
        self._rec_node_lut: dict[str, "GraphNode"] = {}
        self._spec_by_name: dict[str, "SourceNode"] = {}
        self._unresolved_specs: list[str] = []
```

Keep `_unresolved_specs` — it is used by `run()` and `unresolved_specs` property. Only remove the `_rec_*` and `_spec_by_name` lines. The remaining `__init__` body should look like:

```python
    def __init__(
        self,
        name: str | tuple[str, ...] = "pipeline",
        store: "ArrowDatabaseProtocol | None" = None,
        execution_context: "ExecutionContext | None" = None,
        tracker_manager: cp.TrackerManagerProtocol | None = None,
        *,
        _pipeline: "Pipeline | None" = None,
        sources: "dict[str, cp.StreamProtocol] | None" = None,
    ) -> None:
        super().__init__(name=name, tracker_manager=tracker_manager)
        self._store = store
        self._execution_context = execution_context
        self._compiled_pipeline: "Pipeline | None" = _pipeline
        self._sources: dict[str, cp.StreamProtocol] = dict(sources or {})
        self._unresolved_specs: list[str] = []
        self._has_run: bool = False
        self._run_id: str | None = None

        # Job-node map initialised by AbstractPipelineBase.__init__; _compiled_pipeline
        # is set above for jobs created via from_pipeline() / load().
        self._persistent_node_map: "dict[str, Any] | None" = None
        self._nodes: "dict[str, Any]" = {}
```

- [ ] **Step 6.2: Add node-factory class properties and thin compile() override**

Add these after the `__init__` method, replacing the `__enter__` override (the whole `__enter__` method can be deleted — `super().__enter__()` is the only line that matters and it's inherited):

```python
    # ------------------------------------------------------------------
    # Node-factory class attributes (used by AbstractPipelineBase.compile())
    # ------------------------------------------------------------------

    @property
    def source_node_class(self) -> type:
        from orcapod.core.nodes.source_node import SourceJobNode
        return SourceJobNode

    @property
    def function_node_class(self) -> type:
        from orcapod.core.nodes.function_node import FunctionJobNode
        return FunctionJobNode

    @property
    def operator_node_class(self) -> type:
        from orcapod.core.nodes.operator_node import OperatorJobNode
        return OperatorJobNode

    # ------------------------------------------------------------------
    # Context manager — recording
    # ------------------------------------------------------------------

    def __enter__(self) -> "PipelineJob":
        return super().__enter__()  # type: ignore[return-value]

    # ------------------------------------------------------------------
    # compile() — thin override: runs base compile then resets pipeline cache
    # ------------------------------------------------------------------

    def compile(self) -> None:
        """Compile recorded invocations using the base class pass, then reset pipeline cache.

        The base ``compile()`` builds ``_persistent_node_map`` using
        ``SourceJobNode`` / ``FunctionJobNode`` / ``OperatorJobNode`` class
        properties.  This override resets ``_compiled_pipeline`` so that the
        lazily-computed ``compiled_pipeline`` property rebuilds it fresh on
        next access.

        Also wires databases to all job nodes if a store is already attached.
        """
        super().compile()
        self._compiled_pipeline = None  # reset cache; rebuilt lazily via compiled_pipeline
        if self._store is not None:
            self._distribute_databases()
```

- [ ] **Step 6.3: Replace the old compile() method**

Remove the existing `compile()` method body (lines 99–211 in the original file — the large method that built a Pipeline internally and then walked it). It is replaced by the thin override added in Step 6.2. Also remove `_ensure_source_node()` (the whole method), `_is_concrete_source()` (the whole staticmethod), `_to_node_stream()` (the whole method), and the two `record_*` override methods. These are all replaced by the inherited base-class implementations.

- [ ] **Step 6.4: Add compiled_pipeline lazy property; update pipeline property and _build_execution_graph()**

Replace the existing `pipeline` property:

```python
    @property
    def pipeline(self) -> "Pipeline":
        """The compiled pure Pipeline (SourceNode-only leaves).

        Raises:
            RuntimeError: If no DAG has been recorded yet.
        """
        return self.compiled_pipeline

    @property
    def compiled_pipeline(self) -> "Pipeline":
        """Lazily compute and cache the pure Pipeline blueprint for this job.

        Computed via ``as_pipeline()`` on first access after a compile.

        Raises:
            RuntimeError: If no DAG has been recorded or loaded yet.
        """
        if self._compiled_pipeline is None:
            if not self._compiled:
                raise RuntimeError(
                    "PipelineJob has no compiled pipeline yet. "
                    "Use 'with job:' to record a DAG first."
                )
            self._compiled_pipeline = self.as_pipeline()
        return self._compiled_pipeline
```

In `_build_execution_graph()`, change the first assignment inside the method from:
```python
        pipeline = self._compiled_pipeline
        if pipeline is None:
            raise RuntimeError("No compiled pipeline — use 'with job:' first.")
```
to:
```python
        pipeline = self.compiled_pipeline
```

(The `compiled_pipeline` property raises `RuntimeError` when appropriate, so the explicit check can be removed.)

- [ ] **Step 6.5: Replace from_pipeline() with thin composition**

Replace the entire `from_pipeline()` classmethod (which currently has ~80 lines of topological walk logic) with:

```python
    @classmethod
    def from_pipeline(
        cls,
        pipeline: "Pipeline",
        store: "ArrowDatabaseProtocol | None" = None,
        sources: "dict[str, cp.StreamProtocol] | None" = None,
        execution_context: "ExecutionContext | None" = None,
    ) -> "PipelineJob":
        """Create a runnable ``PipelineJob`` from a compiled ``Pipeline``.

        Converts *pipeline* to an ``InvocationGraph`` via ``to_invocations()``,
        then reconstructs a ``PipelineJob`` via ``from_invocations()``.  Source
        bindings and store are applied via ``bind()`` after construction.

        Args:
            pipeline: A compiled ``Pipeline`` (``pipeline._compiled`` must be ``True``).
            store: Database for result caching and operator records.
            sources: Mapping of ``SourceNode.name`` → concrete source.
            execution_context: Optional execution configuration.

        Returns:
            A new ``PipelineJob`` ready to run (or ``bind()`` further).

        Raises:
            ValueError: If *pipeline* has not been compiled.
        """
        if not pipeline._compiled:
            raise ValueError(
                "Pipeline must be compiled before creating a PipelineJob from it. "
                "Call pipeline.compile() or use auto_compile=True."
            )
        job: "PipelineJob" = cls.from_invocations(pipeline.to_invocations(), name=pipeline.name)
        job._store = store
        job._execution_context = execution_context
        if sources or store:
            job.bind(sources=sources, store=store)
        return job
```

- [ ] **Step 6.6: Replace as_pipeline() with thin composition**

Replace the entire `as_pipeline()` method body (which currently has ~70 lines of topological walk building FunctionNode/OperatorNode) with:

```python
    def as_pipeline(self) -> "Pipeline":
        """Return the lightweight ``Pipeline`` blueprint for this job.

        Converts this job's ``_persistent_node_map`` to an ``InvocationGraph``
        via ``to_invocations()``, then reconstructs a ``Pipeline`` via
        ``Pipeline.from_invocations()``.  This ensures the returned Pipeline's
        node hashes match those of this job.

        Returns:
            A compiled ``Pipeline`` whose ``_persistent_node_map`` contains
            only ``SourceNode`` / ``FunctionNode`` / ``OperatorNode`` objects.

        Raises:
            RuntimeError: If this job has not been compiled.
        """
        from orcapod.pipeline.graph import Pipeline

        if not self._compiled:
            raise RuntimeError(
                "PipelineJob has not been compiled. "
                "Either use 'with job:' to record a DAG, "
                "or create the job via PipelineJob.from_pipeline()."
            )
        return Pipeline.from_invocations(self.to_invocations(), name=self._name)
```

- [ ] **Step 6.7: Update from_pipeline() call in load() if needed**

Verify that `PipelineJob.load()` does not call the old `from_pipeline()` with positional args that are now changed. Search the `load()` method for `from_pipeline` — it should not be called there; `load()` constructs the job directly via `cls(...)`. No change needed.

- [ ] **Step 6.8: Run transition tests**

```bash
uv run pytest tests/test_pipeline/test_invocation_transitions.py -v
```

Expected: all pass.

- [ ] **Step 6.9: Commit**

```bash
git add src/orcapod/pipeline/job.py
git commit -m "refactor(pipeline): PipelineJob sheds _rec_* fields; from_pipeline/as_pipeline become thin compositions"
```

---

## Task 7: Full test suite — fix regressions

- [ ] **Step 7.1: Run the full pipeline test suite**

```bash
uv run pytest tests/test_pipeline/ -v --tb=short 2>&1 | tail -60
```

Fix any failures before proceeding. Common failure modes:
- `AttributeError: _rec_node_lut` — a test accesses the removed field; update test to use `_invocation_lut` or `_persistent_node_map` instead
- `AttributeError: _rec_graph_edges` — same; use `_graph_edges` (now a compiled artifact) or `_hash_graph`
- `AttributeError: _spec_by_name` — update test to inspect `_sources` instead
- Failures in `test_serialization.py` — Pipeline's `save()` / `load()` round-trip; run those in isolation and verify edge serialization still works via `_graph_edges` (now populated from `_hash_graph.edges()` at end of `compile()`)

- [ ] **Step 7.2: Run the full test suite**

```bash
uv run pytest tests/ -v --tb=short -q 2>&1 | tail -40
```

All tests should pass.

- [ ] **Step 7.3: Commit all fixes**

```bash
git add -p   # stage only fix changes
git commit -m "fix(pipeline): fix regressions after Pipeline/PipelineJob recording refactor"
```

---

## Self-Review

**Spec coverage check:**

| Spec requirement | Covered by |
|---|---|
| `PodInvocation` hierarchy (`FunctionInvocation`, `OperatorInvocation`) | Task 2 |
| `AbstractPipelineBase`: concrete recording methods, `_record_invocation` | Task 3 |
| `AbstractPipelineBase`: class-property factories (abstract) | Task 3 |
| `AbstractPipelineBase`: `InvocationGraph`, `from_invocations`, `to_invocations` | Task 3 |
| `AbstractPipelineBase`: unified `compile()` | Task 3 |
| `AbstractPipelineBase`: `_node_lut`/`_upstreams`/`_graph_edges` additive semantics updated | Task 3 |
| `AbstractPipelineBase`: `reset()` → no-op | Task 3 |
| `Pipeline`: remove overrides, add class properties | Task 4 |
| `SourceJobNode.from_stream()` three-way logic | Task 5 |
| `PipelineJob`: remove `_rec_*` fields, add class properties | Task 6 |
| `PipelineJob`: thin `from_pipeline()`/`as_pipeline()` | Task 6 |
| `PipelineJob`: lazy `compiled_pipeline` property | Task 6 |
| Tests-first | Task 1 (all test files written before production code) |
| `SourceJobNode.identity_structure()` bound-state dependent — already implemented, must not change | Verified in Task 5 (no changes to `identity_structure`) |

**Backward compat notes:**
- `Pipeline.save()` / `load()` — unchanged; `_graph_edges` is now populated from `_hash_graph.edges()` at end of `compile()` so save still works.
- `_build_execution_graph()` — one-line change (`self.compiled_pipeline` instead of `self._compiled_pipeline`) preserves its behavior via lazy property.
- `Pipeline._clone_for_execution()` — updated in Task 4 to copy `_invocation_lut` and `_source_streams` alongside the other shared fields.
