# SideEffectPod Implementation Plan

> **⚠ As-built notice (post-merge):** The invocation log design was updated during the PR
> review cycle. The code samples below in Tasks 7 and 8 still reference the original
> `status`/`error_message`-based schema and `get_records_with_column_value` completion check.
> The **actual implementation** differs:
> - Only **success** rows are written; failure writes nothing (so the next run retries).
> - **`record_id`** = `fip_hash.digest + b"::" + pod_content_hash.digest` (bytes, deterministic).
> - Completion check uses `get_record_by_id(table_path, record_id)` — no column scan.
> - Schema has no `status` or `error_message` columns.
>
> See `src/orcapod/side_effects.py` and `superpowers/specs/2026-07-14-itl-525-side-effect-pod-impl-design.md` for the authoritative as-built design.

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `SideEffectPod` as a first-class pipeline node for side effects (DB writes, logging, notifications) with pass-through output, deterministic invocation identity via `InvocationContext`, and optional idempotent completion tracking.

**Architecture:** `SideEffectPod` is the user-facing pod (no DB); at pipeline compile time it is promoted to `SideEffectJobNode` (DB-backed), mirroring the `FunctionPod` → `FunctionJobNode` pattern. `run_id` is passed as a call-time keyword from orchestrators (never stored as node state). `InvocationHashConfig` is purely user-facing — no footprint in persistent storage.

**Tech Stack:** PyArrow, asyncio.TaskGroup, `ArrowDatabaseProtocol`, `StreamBase`, `TraceableBase`, existing `PodInvocation` / `AbstractPipelineBase` / `BasicTrackerManager` patterns.

---

## File Map

| File | Action | Responsibility |
|---|---|---|
| `src/orcapod/side_effects.py` | **CREATE** | `InvocationHashConfig`, `SideEffectPodConfig`, `InvocationContext`, `SideEffectPodStream`, `SideEffectPod`, `SideEffectJobNode`, decorators |
| `src/orcapod/protocols/core_protocols/side_effect_pod.py` | **CREATE** | `SideEffectPodProtocol` |
| `src/orcapod/protocols/core_protocols/__init__.py` | **MODIFY** | re-export `SideEffectPodProtocol` |
| `src/orcapod/pipeline/pod_invocation.py` | **MODIFY** | add `SideEffectInvocation` |
| `src/orcapod/protocols/core_protocols/trackers.py` | **MODIFY** | add `record_side_effect_pod_invocation` to both protocols |
| `src/orcapod/core/tracker.py` | **MODIFY** | add `record_side_effect_pod_invocation` to `BasicTrackerManager` |
| `src/orcapod/pipeline/base.py` | **MODIFY** | add `record_side_effect_pod_invocation`, `side_effect_node_class` abstract property, `compile()` branch |
| `src/orcapod/pipeline/job.py` | **MODIFY** | `side_effect_node_class = SideEffectJobNode`, `_distribute_databases()` branch |
| `src/orcapod/protocols/node_protocols.py` | **MODIFY** | add `SideEffectNodeProtocol`, `is_side_effect_node()` |
| `src/orcapod/pipeline/sync_orchestrator.py` | **MODIFY** | add `elif is_side_effect_node` branch |
| `src/orcapod/pipeline/async_orchestrator.py` | **MODIFY** | add `elif is_side_effect_node` branch |
| `src/orcapod/__init__.py` | **MODIFY** | re-export public symbols |
| `tests/test_core/side_effect_pod/__init__.py` | **CREATE** | empty |
| `tests/test_core/side_effect_pod/test_side_effect_pod.py` | **CREATE** | T1–T18 |

---

## Task 1: Types — `InvocationHashConfig`, `SideEffectPodConfig`, `InvocationContext`

**Files:**
- Create: `src/orcapod/side_effects.py`
- Test: `tests/test_core/side_effect_pod/test_side_effect_pod.py`

- [ ] **Step 1: Create test file and test directory**

```bash
mkdir -p tests/test_core/side_effect_pod
touch tests/test_core/side_effect_pod/__init__.py
```

- [ ] **Step 2: Write failing tests for types**

```python
# tests/test_core/side_effect_pod/test_side_effect_pod.py
from __future__ import annotations

import pytest
import pyarrow as pa

from orcapod.core.streams import ArrowTableStream
from orcapod.core.datagrams import Data, Tag


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


# ---------------------------------------------------------------------------
# Task 1 tests
# ---------------------------------------------------------------------------

class TestInvocationHashConfig:
    def test_defaults(self):
        from orcapod.side_effects import InvocationHashConfig
        cfg = InvocationHashConfig()
        assert cfg.encoding == "hex"
        assert cfg.component_length is None

    def test_custom(self):
        from orcapod.side_effects import InvocationHashConfig
        cfg = InvocationHashConfig(encoding="base64", component_length=8)
        assert cfg.encoding == "base64"
        assert cfg.component_length == 8

    def test_frozen(self):
        from orcapod.side_effects import InvocationHashConfig
        cfg = InvocationHashConfig()
        with pytest.raises((AttributeError, TypeError)):
            cfg.encoding = "base64"  # type: ignore[misc]


class TestSideEffectPodConfig:
    def test_defaults(self):
        from orcapod.side_effects import SideEffectPodConfig
        cfg = SideEffectPodConfig()
        assert cfg.track_completion is True
        assert cfg.drop_on_failure is True
        assert cfg.on_error == "raise"

    def test_custom(self):
        from orcapod.side_effects import SideEffectPodConfig
        cfg = SideEffectPodConfig(track_completion=False, drop_on_failure=False)
        assert cfg.track_completion is False
        assert cfg.drop_on_failure is False
```

- [ ] **Step 3: Run to verify they fail**

```bash
uv run pytest tests/test_core/side_effect_pod/test_side_effect_pod.py::TestInvocationHashConfig tests/test_core/side_effect_pod/test_side_effect_pod.py::TestSideEffectPodConfig -v
```
Expected: FAIL with `ModuleNotFoundError` or `ImportError`

- [ ] **Step 4: Create `src/orcapod/side_effects.py` with types**

```python
# src/orcapod/side_effects.py
"""SideEffectPod — pass-through pipeline node for side effects.

Provides ``SideEffectPod``, ``SideEffectPodStream``, ``SideEffectJobNode``,
``InvocationContext``, ``InvocationHashConfig``, ``SideEffectPodConfig``,
and the ``side_effect_pod``, ``sink_pod``, ``tap_pod`` decorators.
"""
from __future__ import annotations

import asyncio
import base64
import dataclasses
import datetime
import logging
from collections.abc import Callable, Collection, Iterator, Sequence
from typing import TYPE_CHECKING, Any, Literal

from orcapod.core.base import TraceableBase
from orcapod.core.streams.base import StreamBase
from orcapod.core.tracker import DEFAULT_TRACKER_MANAGER
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa
    from orcapod.channels import ReadableChannel, WritableChannel
    from orcapod.protocols.core_protocols import (
        DataProtocol,
        StreamProtocol,
        TagProtocol,
        TrackerManagerProtocol,
    )
    from orcapod.protocols.database_protocols import ArrowDatabaseProtocol
    from orcapod.protocols.observability_protocols import ExecutionObserverProtocol
    from orcapod.types import ColumnConfig, ContentHash, Schema
else:
    pa = LazyModule("pyarrow")

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# InvocationHashConfig
# ---------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class InvocationHashConfig:
    """Controls how ``InvocationContext.invocation_hash`` is serialized.

    Args:
        encoding: Output encoding — ``"hex"`` (default), ``"base64"``, or
            ``"binary"`` (falls back to hex in string contexts).
        component_length: Bytes of raw digest to use per component. ``None``
            means full digest length. Applied identically to every
            ``::``-separated component.
    """

    encoding: Literal["hex", "base64", "binary"] = "hex"
    component_length: int | None = None


def _serialize_component(content_hash: ContentHash, config: InvocationHashConfig) -> str:
    """Serialize one ``ContentHash`` component per ``InvocationHashConfig``.

    Args:
        content_hash: The hash to serialize.
        config: Encoding and truncation config.

    Returns:
        A string representation of the (optionally truncated) digest.
    """
    raw: bytes = content_hash.digest
    if config.component_length is not None:
        raw = raw[: config.component_length]
    if config.encoding == "base64":
        return base64.b64encode(raw).decode("ascii")
    # "hex" and "binary" both produce hex strings in string contexts
    return raw.hex()


# ---------------------------------------------------------------------------
# SideEffectPodConfig
# ---------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class SideEffectPodConfig:
    """Configuration for a ``SideEffectPod``.

    Args:
        track_completion: If ``True`` (default), skip re-delivery for inputs
            that previously completed successfully.
        drop_on_failure: If ``True`` (default), drop rows whose delivery
            raised an exception from the downstream output.
        on_error: ``"raise"`` (default) re-raises delivery exceptions;
            ``"log"`` logs at WARNING and continues.
        hash_config: Controls encoding of ``InvocationContext.invocation_hash``.
    """

    track_completion: bool = True
    drop_on_failure: bool = True
    on_error: Literal["raise", "log"] = "raise"
    hash_config: InvocationHashConfig = dataclasses.field(
        default_factory=InvocationHashConfig
    )


# ---------------------------------------------------------------------------
# InvocationContext
# ---------------------------------------------------------------------------


class InvocationContext:
    """Per-invocation context passed to every side-effect pod function.

    Carries the deterministic ``invocation_hash`` string and metadata about
    the current delivery. ``format_id()`` re-serializes the hash with a
    caller-supplied ``InvocationHashConfig`` without recomputing.

    Public fields are read-only by convention (no public setters).

    Args:
        invocation_hash: Serialized compound hash string.
        pod_name: ``pod.label`` of the invoking pod.
        pod_content_hash: ``pod.content_hash().to_string()``.
        pipeline_run_id: The current pipeline run identifier, or ``None``
            for standalone / lazy pipelines.
    """

    def __init__(
        self,
        invocation_hash: str,
        pod_name: str,
        pod_content_hash: str,
        pipeline_run_id: str | None,
        _pipeline_hash_ch: ContentHash,
        _full_input_packet_hash_ch: ContentHash,
        _hash_config: InvocationHashConfig,
        _track_completion: bool,
    ) -> None:
        self.invocation_hash = invocation_hash
        self.pod_name = pod_name
        self.pod_content_hash = pod_content_hash
        self.pipeline_run_id = pipeline_run_id
        self._pipeline_hash_ch = _pipeline_hash_ch
        self._full_input_packet_hash_ch = _full_input_packet_hash_ch
        self._hash_config = _hash_config
        self._track_completion = _track_completion

    def format_id(self, config: InvocationHashConfig | None = None) -> str:
        """Return ``'orcapod-{hash}'`` with an optional format override.

        Re-serializes from the stored raw ``ContentHash`` components — no
        recomputation. Uses ``config`` if supplied, otherwise the pod's own
        ``InvocationHashConfig``.

        Args:
            config: Optional encoding/truncation override.

        Returns:
            A string of the form ``"orcapod-{component1}::{component2}"``
            (two components when ``track_completion=True``) or
            ``"orcapod-{c1}::{c2}::{run_id}"`` (three components when
            ``track_completion=False`` and ``pipeline_run_id`` is not ``None``).
        """
        cfg = config or self._hash_config
        c1 = _serialize_component(self._pipeline_hash_ch, cfg)
        c2 = _serialize_component(self._full_input_packet_hash_ch, cfg)
        if not self._track_completion and self.pipeline_run_id is not None:
            parts = f"{c1}::{c2}::{self.pipeline_run_id}"
        else:
            parts = f"{c1}::{c2}"
        return f"orcapod-{parts}"
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
uv run pytest tests/test_core/side_effect_pod/test_side_effect_pod.py::TestInvocationHashConfig tests/test_core/side_effect_pod/test_side_effect_pod.py::TestSideEffectPodConfig -v
```
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/side_effects.py tests/test_core/side_effect_pod/__init__.py tests/test_core/side_effect_pod/test_side_effect_pod.py
git commit -m "feat(side-effects): add InvocationHashConfig, SideEffectPodConfig, InvocationContext"
```

---

## Task 2: `SideEffectPodProtocol`

**Files:**
- Create: `src/orcapod/protocols/core_protocols/side_effect_pod.py`
- Modify: `src/orcapod/protocols/core_protocols/__init__.py`

- [ ] **Step 1: Create protocol file**

```python
# src/orcapod/protocols/core_protocols/side_effect_pod.py
"""SideEffectPodProtocol — protocol for side-effect pods."""
from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

from orcapod.protocols.hashing_protocols import PipelineElementProtocol

if TYPE_CHECKING:
    from orcapod.protocols.core_protocols.pod import ArgumentGroup, PodProtocol
    from orcapod.protocols.core_protocols.streams import StreamProtocol
    from orcapod.side_effects import SideEffectPodConfig, SideEffectPodStream
    from orcapod.types import ColumnConfig, Schema


@runtime_checkable
class SideEffectPodProtocol(PipelineElementProtocol, Protocol):
    """Protocol for side-effect pods.

    A side-effect pod wraps a ``(data: T, ctx: InvocationContext) -> None``
    callable. Its ``process()`` returns a pass-through stream. Output schema
    equals input schema.
    """

    @property
    def pod_config(self) -> "SideEffectPodConfig":
        """Pod-level configuration."""
        ...

    def process(
        self, *streams: "StreamProtocol", label: str | None = None
    ) -> "SideEffectPodStream":
        """Invoke the pod on input streams, returning a pass-through stream."""
        ...

    def output_schema(
        self,
        *streams: "StreamProtocol",
        columns: "ColumnConfig | dict[str, object] | None" = None,
        all_info: bool = False,
    ) -> "tuple[Schema, Schema]":
        """Return the input stream's schema unchanged (pass-through)."""
        ...

    def argument_symmetry(
        self, streams: "Collection[StreamProtocol]"
    ) -> "ArgumentGroup": ...
```

- [ ] **Step 2: Re-export from `core_protocols/__init__.py`**

Open `src/orcapod/protocols/core_protocols/__init__.py` and add:

```python
from .side_effect_pod import SideEffectPodProtocol
```

And add `"SideEffectPodProtocol"` to `__all__`.

- [ ] **Step 3: Verify import**

```bash
uv run python -c "from orcapod.protocols.core_protocols import SideEffectPodProtocol; print('ok')"
```
Expected: `ok`

- [ ] **Step 4: Commit**

```bash
git add src/orcapod/protocols/core_protocols/side_effect_pod.py src/orcapod/protocols/core_protocols/__init__.py
git commit -m "feat(protocols): add SideEffectPodProtocol"
```

---

## Task 3: `SideEffectInvocation`

**Files:**
- Modify: `src/orcapod/pipeline/pod_invocation.py`

- [ ] **Step 1: Write failing test**

Add to `tests/test_core/side_effect_pod/test_side_effect_pod.py`:

```python
class TestSideEffectInvocation:
    def test_construction(self):
        from orcapod.pipeline.pod_invocation import SideEffectInvocation
        from orcapod.side_effects import SideEffectPod, SideEffectPodConfig

        calls = []
        def fn(data, ctx):
            calls.append(data)

        pod = SideEffectPod(fn)
        stream = _make_stream()
        inv = SideEffectInvocation(pod=pod, input_streams=(stream,))
        assert inv.pod is pod
        assert inv.input_streams == (stream,)

    def test_requires_exactly_one_stream(self):
        from orcapod.pipeline.pod_invocation import SideEffectInvocation
        from orcapod.side_effects import SideEffectPod

        def fn(data, ctx): pass
        pod = SideEffectPod(fn)
        stream = _make_stream()

        with pytest.raises(ValueError):
            SideEffectInvocation(pod=pod, input_streams=())

        with pytest.raises(ValueError):
            SideEffectInvocation(pod=pod, input_streams=(stream, stream))
```

- [ ] **Step 2: Run to verify failure**

```bash
uv run pytest tests/test_core/side_effect_pod/test_side_effect_pod.py::TestSideEffectInvocation -v
```
Expected: FAIL (`ImportError` on `SideEffectInvocation`)

- [ ] **Step 3: Add `SideEffectInvocation` to `pod_invocation.py`**

Open `src/orcapod/pipeline/pod_invocation.py` and append after `OperatorInvocation`:

```python
class SideEffectInvocation(PodInvocation):
    """Invocation of a side-effect pod against exactly one input stream.

    Args:
        pod: A ``SideEffectPodProtocol`` instance.
        input_streams: Tuple with exactly one stream.
        label: Optional display label.

    Raises:
        ValueError: If ``input_streams`` does not contain exactly one element.
    """

    def __init__(
        self,
        pod: Any,
        input_streams: "tuple[StreamProtocol, ...]",
        label: str | None = None,
    ) -> None:
        if len(input_streams) != 1:
            raise ValueError(
                f"SideEffectInvocation requires exactly 1 input stream; "
                f"got {len(input_streams)}."
            )
        super().__init__(pod=pod, input_streams=input_streams, label=label)
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/test_core/side_effect_pod/test_side_effect_pod.py::TestSideEffectInvocation -v
```
Expected: PASS (note: `SideEffectPod` may not exist yet — acceptable FAIL with `ImportError` on `SideEffectPod` at this stage; the SideEffectInvocation logic itself is correct)

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/pipeline/pod_invocation.py
git commit -m "feat(pipeline): add SideEffectInvocation to pod_invocation"
```

---

## Task 4: Tracker chain — `record_side_effect_pod_invocation`

**Files:**
- Modify: `src/orcapod/protocols/core_protocols/trackers.py`
- Modify: `src/orcapod/core/tracker.py`
- Modify: `src/orcapod/pipeline/base.py`

- [ ] **Step 1: Add to `TrackerProtocol` and `TrackerManagerProtocol`**

Open `src/orcapod/protocols/core_protocols/trackers.py`.

Add the following method to **`TrackerProtocol`** after `record_function_pod_invocation`:

```python
    def record_side_effect_pod_invocation(
        self,
        pod: Any,
        input_stream: StreamProtocol,
        label: str | None = None,
    ) -> None:
        """Record a side-effect pod invocation in the computational graph.

        Args:
            pod: The side-effect pod being invoked.
            input_stream: The upstream stream.
            label: Optional display label.
        """
        ...
```

Add the same method to **`TrackerManagerProtocol`** after `record_function_pod_invocation`:

```python
    def record_side_effect_pod_invocation(
        self,
        pod: Any,
        input_stream: StreamProtocol,
        label: str | None = None,
    ) -> None:
        """Record a side-effect pod invocation in all active trackers.

        Args:
            pod: The side-effect pod to record.
            input_stream: The upstream stream.
            label: Optional display label.
        """
        ...
```

Note: use `Any` for `pod` type to avoid circular imports. Add `from typing import Any` if not already imported.

- [ ] **Step 2: Add to `BasicTrackerManager` in `core/tracker.py`**

Open `src/orcapod/core/tracker.py` and add after `record_function_pod_invocation`:

```python
    def record_side_effect_pod_invocation(
        self,
        pod: Any,
        input_stream: cp.StreamProtocol,
        label: str | None = None,
    ) -> None:
        """Record a side-effect pod invocation in all active trackers."""
        for tracker in self.get_active_trackers():
            tracker.record_side_effect_pod_invocation(pod, input_stream, label=label)
```

Add `from typing import Any` at the top of the file if not already present.

- [ ] **Step 3: Add `record_side_effect_pod_invocation` to `AbstractPipelineBase`**

Open `src/orcapod/pipeline/base.py`.

Add the following import at the top with the other invocation imports:
```python
from orcapod.pipeline.pod_invocation import (
    FunctionInvocation,
    OperatorInvocation,
    PodInvocation,
    SideEffectInvocation,  # add this
)
```

Add the new method after `record_operator_pod_invocation`:

```python
    def record_side_effect_pod_invocation(
        self,
        pod: Any,
        input_stream: "cp.StreamProtocol",
        label: str | None = None,
    ) -> None:
        """Record a side-effect pod invocation into the graph.

        Args:
            pod: The side-effect pod being invoked.
            input_stream: The upstream stream.
            label: Optional display label for the resulting compiled node.
        """
        self._record_invocation(
            SideEffectInvocation(pod=pod, input_streams=(input_stream,), label=label)
        )
```

Add `from typing import Any` if not already imported.

- [ ] **Step 4: Verify no import errors**

```bash
uv run python -c "from orcapod.pipeline.base import AbstractPipelineBase; print('ok')"
uv run python -c "from orcapod.core.tracker import BasicTrackerManager; print('ok')"
```
Expected: both print `ok`

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/protocols/core_protocols/trackers.py src/orcapod/core/tracker.py src/orcapod/pipeline/base.py
git commit -m "feat(tracker): add record_side_effect_pod_invocation to tracker chain"
```

---

## Task 5: `SideEffectPod`, `SideEffectPodStream`, and decorators

**Files:**
- Modify: `src/orcapod/side_effects.py`
- Test: `tests/test_core/side_effect_pod/test_side_effect_pod.py`

- [ ] **Step 1: Write failing tests T1–T4, T8–T10, T16–T18**

Add to `tests/test_core/side_effect_pod/test_side_effect_pod.py`:

```python
# ---------------------------------------------------------------------------
# Task 5 tests — standalone / lazy mode (no DB)
# ---------------------------------------------------------------------------


class TestSideEffectPodStandalone:
    """T1–T4, T8–T10: standalone execution via SideEffectPodStream."""

    def test_t1_passthrough_drop_on_failure_false(self):
        """T1: All rows emitted when drop_on_failure=False and no errors."""
        from orcapod.side_effects import SideEffectPod, SideEffectPodConfig

        calls = []
        def fn(data, ctx):
            calls.append(dict(data))

        pod = SideEffectPod(fn, config=SideEffectPodConfig(drop_on_failure=False))
        stream = _make_stream(3)
        out = list(pod.process(stream).iter_data())

        assert len(out) == 3
        assert len(calls) == 3

    def test_t2_passthrough_drop_on_failure_true_no_errors(self):
        """T2: All rows emitted when drop_on_failure=True and no errors."""
        from orcapod.side_effects import SideEffectPod, SideEffectPodConfig

        calls = []
        def fn(data, ctx):
            calls.append(dict(data))

        pod = SideEffectPod(fn)  # default: drop_on_failure=True
        stream = _make_stream(3)
        out = list(pod.process(stream).iter_data())

        assert len(out) == 3
        assert len(calls) == 3

    def test_t3_invocation_context_always_passed(self):
        """T3: InvocationContext always constructed and passed."""
        from orcapod.side_effects import SideEffectPod, InvocationContext

        received = []
        def fn(data, ctx):
            received.append(ctx)

        pod = SideEffectPod(fn)
        stream = _make_stream(1)
        list(pod.process(stream).iter_data())

        assert len(received) == 1
        ctx = received[0]
        assert isinstance(ctx, InvocationContext)
        assert isinstance(ctx.invocation_hash, str)
        assert len(ctx.invocation_hash) > 0
        assert ctx.format_id().startswith("orcapod-")

    def test_t4_invocation_context_ignored_by_callee(self):
        """T4: Pod works fine when callee ignores ctx."""
        from orcapod.side_effects import SideEffectPod

        calls = []
        def fn(data, _ctx):
            calls.append(True)

        pod = SideEffectPod(fn)
        stream = _make_stream(2)
        out = list(pod.process(stream).iter_data())

        assert len(out) == 2
        assert len(calls) == 2

    def test_t8_on_error_raise(self):
        """T8: on_error='raise' propagates the exception."""
        from orcapod.side_effects import SideEffectPod, SideEffectPodConfig

        def fn(data, ctx):
            raise RuntimeError("boom")

        pod = SideEffectPod(fn, config=SideEffectPodConfig(on_error="raise"))
        stream = _make_stream(1)

        with pytest.raises(RuntimeError, match="boom"):
            list(pod.process(stream).iter_data())

    def test_t9_on_error_log_drop_on_failure_true(self):
        """T9: on_error='log' + drop_on_failure=True drops failed rows."""
        from orcapod.side_effects import SideEffectPod, SideEffectPodConfig

        def fn(data, ctx):
            raise RuntimeError("oops")

        pod = SideEffectPod(
            fn,
            config=SideEffectPodConfig(on_error="log", drop_on_failure=True),
        )
        stream = _make_stream(3)
        out = list(pod.process(stream).iter_data())

        assert len(out) == 0  # all rows dropped

    def test_t10_on_error_log_drop_on_failure_false(self):
        """T10: on_error='log' + drop_on_failure=False passes through despite failure."""
        from orcapod.side_effects import SideEffectPod, SideEffectPodConfig

        def fn(data, ctx):
            raise RuntimeError("oops")

        pod = SideEffectPod(
            fn,
            config=SideEffectPodConfig(on_error="log", drop_on_failure=False),
        )
        stream = _make_stream(3)
        out = list(pod.process(stream).iter_data())

        assert len(out) == 3  # all rows still emitted


class TestDecorators:
    """T16–T18: @sink_pod, @tap_pod, @side_effect_pod."""

    def test_t16_sink_pod(self):
        """T16: @sink_pod sets track_completion=True, drop_on_failure=True."""
        from orcapod.side_effects import sink_pod, SideEffectPod

        @sink_pod
        def my_sink(data, ctx):
            pass

        assert isinstance(my_sink, SideEffectPod)
        assert my_sink.pod_config.track_completion is True
        assert my_sink.pod_config.drop_on_failure is True

    def test_t17_tap_pod(self):
        """T17: @tap_pod sets track_completion=False, drop_on_failure=False."""
        from orcapod.side_effects import tap_pod, SideEffectPod

        @tap_pod
        def my_tap(data, ctx):
            pass

        assert isinstance(my_tap, SideEffectPod)
        assert my_tap.pod_config.track_completion is False
        assert my_tap.pod_config.drop_on_failure is False

    def test_t18_side_effect_pod_config_combinations(self):
        """T18: @side_effect_pod(config=...) all four combinations."""
        from orcapod.side_effects import side_effect_pod, SideEffectPodConfig

        for tc in [True, False]:
            for dof in [True, False]:
                cfg = SideEffectPodConfig(track_completion=tc, drop_on_failure=dof)

                @side_effect_pod(config=cfg)
                def fn(data, ctx):
                    pass

                assert fn.pod_config.track_completion is tc
                assert fn.pod_config.drop_on_failure is dof

    def test_sink_pod_parameterised(self):
        """@sink_pod(config=...) with explicit config override."""
        from orcapod.side_effects import sink_pod, SideEffectPodConfig

        cfg = SideEffectPodConfig(on_error="log")

        @sink_pod(config=cfg)
        def my_sink(data, ctx):
            pass

        assert my_sink.pod_config.on_error == "log"
        assert my_sink.pod_config.track_completion is True
        assert my_sink.pod_config.drop_on_failure is True
```

- [ ] **Step 2: Run to verify failure**

```bash
uv run pytest tests/test_core/side_effect_pod/test_side_effect_pod.py::TestSideEffectPodStandalone tests/test_core/side_effect_pod/test_side_effect_pod.py::TestDecorators -v
```
Expected: FAIL (`ImportError` on `SideEffectPod`)

- [ ] **Step 3: Implement `SideEffectPodStream`, `SideEffectPod`, and decorators in `side_effects.py`**

Append the following to `src/orcapod/side_effects.py` (after the `InvocationContext` class):

```python
# ---------------------------------------------------------------------------
# SideEffectPodStream
# ---------------------------------------------------------------------------


class SideEffectPodStream(StreamBase):
    """Pass-through stream returned by ``SideEffectPod.process()`` in standalone mode.

    Iterates the upstream stream and calls the side-effect function per row.
    No invocation log is written in standalone mode (``pipeline_run_id=None``).
    """

    def __init__(
        self,
        side_effect_pod: SideEffectPod,
        input_stream: StreamProtocol,
        **kwargs: Any,
    ) -> None:
        self._pod = side_effect_pod
        self._input_stream = input_stream
        super().__init__(**kwargs)

    @property
    def producer(self):  # type: ignore[override]
        return self._pod

    @property
    def upstreams(self) -> tuple[StreamProtocol, ...]:
        return (self._input_stream,)

    def identity_structure(self) -> Any:
        return (self._pod, self._pod.argument_symmetry((self._input_stream,)))

    def pipeline_identity_structure(self) -> Any:
        return self.identity_structure()

    def output_schema(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        return self._input_stream.output_schema(columns=columns, all_info=all_info)

    def keys(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        return self._input_stream.keys(columns=columns, all_info=all_info)

    def iter_data(self) -> Iterator[tuple[TagProtocol, DataProtocol]]:
        for tag, data in self._input_stream.iter_data():
            result = _execute_side_effect_row(
                fn=self._pod._fn,
                tag=tag,
                data=data,
                pod_config=self._pod.pod_config,
                pipeline_hash_ch=self._pod.pipeline_hash(),
                pod_content_hash_str=self._pod.content_hash().to_string(),
                pod_name=self._pod.label,
                run_id=None,
                arrow_hasher=self._pod.data_context.arrow_hasher,
            )
            if result is not None:
                yield result

    def as_table(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> pa.Table:
        from orcapod.utils import arrow_utils

        tag_tables = []
        data_tables = []
        for tag, data in self.iter_data():
            tag_tables.append(tag.as_table(columns={"system_tags": True}))
            data_tables.append(data.as_table(columns={"source": True}))
        if not tag_tables:
            # Return an empty table with the correct schema
            tag_schema, data_schema = self.output_schema(
                columns={"system_tags": True, "source": True}
            )
            tc = self._pod.data_context.type_converter
            fields = {}
            for name, py_type in {**tag_schema, **data_schema}.items():
                fields[name] = pa.array(
                    [], type=tc.python_type_to_arrow_type(py_type)
                )
            return pa.table(fields)
        combined_tags = pa.concat_tables(tag_tables)
        combined_data = pa.concat_tables(data_tables)
        return arrow_utils.hstack_tables(combined_tags, combined_data)


# ---------------------------------------------------------------------------
# Shared row execution helper
# ---------------------------------------------------------------------------


def _execute_side_effect_row(
    *,
    fn: Callable,
    tag: TagProtocol,
    data: DataProtocol,
    pod_config: SideEffectPodConfig,
    pipeline_hash_ch: ContentHash,
    pod_content_hash_str: str,
    pod_name: str,
    run_id: str | None,
    arrow_hasher: Any,
    pipeline_database: ArrowDatabaseProtocol | None = None,
    table_path: tuple[str, ...] | None = None,
) -> tuple[TagProtocol, DataProtocol] | None:
    """Execute delivery for one (tag, data) row.

    Args:
        fn: The side-effect callable ``(data, ctx) -> None``.
        tag: Tag for this row.
        data: Data for this row.
        pod_config: Pod-level configuration.
        pipeline_hash_ch: Pipeline hash of the node (for invocation_hash c1).
        pod_content_hash_str: String form of the pod's content hash.
        pod_name: Label of the pod.
        run_id: Pipeline run identifier (or ``None`` in standalone mode).
        arrow_hasher: The ``arrow_hasher`` from the pod's data context.
        pipeline_database: Attached DB (or ``None`` for standalone mode).
        table_path: Path tuple for the invocation log table.

    Returns:
        ``(tag, data)`` to emit downstream, or ``None`` to drop the row.
    """
    from orcapod.utils import arrow_utils

    # 1. Compute full_input_packet_hash over all four column groups.
    tag_table = tag.as_table(columns={"system_tags": True})
    data_table = data.as_table(columns={"source": True})
    full_table = arrow_utils.hstack_tables(tag_table, data_table)
    fip_hash: ContentHash = arrow_hasher.hash_table(full_table)
    fip_hash_str = fip_hash.to_string()

    # 2. Serialize invocation_hash.
    cfg = pod_config.hash_config
    c1 = _serialize_component(pipeline_hash_ch, cfg)
    c2 = _serialize_component(fip_hash, cfg)
    if not pod_config.track_completion and run_id is not None:
        inv_hash = f"{c1}::{c2}::{run_id}"
    else:
        inv_hash = f"{c1}::{c2}"

    # ⚠ As-built: the completion check and write logic below was redesigned post-merge.
    # Actual implementation uses a deterministic record_id + get_record_by_id lookup.
    # Only success rows are written; failure writes nothing so the next run retries.
    # See src/orcapod/side_effects.py and the spec for the authoritative design.

    # 3. Deterministic record_id for completion lookup and write.
    record_id = fip_hash.digest + b"::" + pod_content_hash.digest

    # 4. Completion check — O(1) lookup by record_id; no column scan.
    if pod_config.track_completion and pipeline_database is not None and table_path is not None:
        prior = pipeline_database.get_record_by_id(table_path, record_id)
        if prior is not None:
            return (tag, data)  # already completed — re-emit without re-delivery

    # 5. Build InvocationContext (always).
    ctx = InvocationContext(
        invocation_hash=inv_hash,
        pod_name=pod_name,
        pod_content_hash=pod_content_hash.to_string(),
        pipeline_run_id=run_id,
        _pipeline_hash_ch=pipeline_hash_ch,
        _full_input_packet_hash_ch=fip_hash,
        _hash_config=cfg,
        _track_completion=pod_config.track_completion,
    )

    # 6. Call user function.
    try:
        fn(data, ctx)
        if pipeline_database is not None and table_path is not None:
            _write_invocation_row(
                pipeline_database=pipeline_database,
                table_path=table_path,
                record_id=record_id,
                fip_hash_str=fip_hash.to_string(),
                pod_content_hash_str=pod_content_hash.to_string(),
                run_id=run_id,
            )
        return (tag, data)
    except Exception as exc:
        # No DB write on failure — absence of record means the next run retries.
        if pod_config.on_error == "raise":
            raise
        logger.warning(
            "SideEffectPod %r delivery failed: %s", pod_name, exc, exc_info=True
        )
        if pod_config.drop_on_failure:
            return None
        return (tag, data)


def _write_invocation_row(
    *,
    pipeline_database: ArrowDatabaseProtocol,
    table_path: tuple[str, ...],
    fip_hash_str: str,
    pod_content_hash_str: str,
    run_id: str | None,
    status: str,
    error_message: str | None,
) -> None:
    """Write one row to the side-effect invocation log table."""
    record = pa.table(
        {
            "full_input_packet_hash": pa.array(
                [fip_hash_str], type=pa.large_string()
            ),
            "pod_content_hash": pa.array(
                [pod_content_hash_str], type=pa.large_string()
            ),
            "pipeline_run_id": pa.array(
                [run_id], type=pa.large_string()
            ),
            "executed_at": pa.array(
                [datetime.datetime.now(datetime.timezone.utc)],
                type=pa.timestamp("us", tz="UTC"),
            ),
            "status": pa.array([status], type=pa.large_string()),
            "error_message": pa.array(
                [error_message], type=pa.large_string()
            ),
        }
    )
    pipeline_database.add_records(
        table_path,
        record,
        record_id_column=None,
        skip_duplicates=False,
    )


# ---------------------------------------------------------------------------
# SideEffectPod
# ---------------------------------------------------------------------------


class SideEffectPod(TraceableBase):
    """A pipeline node whose primary purpose is a side effect.

    Wraps a ``(data: T, ctx: InvocationContext) -> None`` callable.
    ``InvocationContext`` is always constructed and passed — it is part of
    the function contract. Callees that do not need it may ignore it (name
    the parameter ``_ctx`` by convention).

    Returns a pass-through stream. When ``drop_on_failure=True``, only
    successfully-delivered rows flow downstream.

    In standalone mode (no ``PipelineJob``), executes row-by-row via
    ``SideEffectPodStream`` with no invocation logging.

    In pipeline mode, promoted to ``SideEffectJobNode`` at compile time,
    which adds DB-backed invocation logging and completion tracking.

    Args:
        fn: A callable ``(data, ctx: InvocationContext) -> None``.
        config: Pod-level configuration. Defaults to ``SideEffectPodConfig()``.
        tracker_manager: Optional tracker manager override.
        label: Optional display label.
        data_context: Optional data context override.
    """

    def __init__(
        self,
        fn: Callable,
        config: SideEffectPodConfig | None = None,
        tracker_manager: TrackerManagerProtocol | None = None,
        label: str | None = None,
        data_context: Any = None,
    ) -> None:
        super().__init__(label=label, data_context=data_context)
        self._fn = fn
        self._pod_config = config or SideEffectPodConfig()
        self.tracker_manager = tracker_manager or DEFAULT_TRACKER_MANAGER

    @property
    def pod_config(self) -> SideEffectPodConfig:
        """Pod-level configuration."""
        return self._pod_config

    def computed_label(self) -> str | None:
        """Use the callable's ``__name__`` as the default label."""
        return getattr(self._fn, "__name__", None)

    def identity_structure(self) -> Any:
        return ("SideEffectPod", self._fn, self._pod_config)

    def pipeline_identity_structure(self) -> Any:
        return self.identity_structure()

    @property
    def uri(self) -> tuple[str, ...]:
        """Canonical URI for this pod."""
        module = getattr(self._fn, "__module__", "unknown")
        name = getattr(self._fn, "__qualname__", getattr(self._fn, "__name__", "unknown"))
        return (module, name)

    def argument_symmetry(self, streams: Collection[StreamProtocol]) -> Any:
        """Single ordered input — return as an ordered tuple."""
        return tuple(streams)

    def output_schema(
        self,
        *streams: StreamProtocol,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        """Return the input stream's schema unchanged (pass-through).

        Args:
            *streams: Exactly one input stream.
            columns: Optional column config.
            all_info: Include all metadata columns.

        Returns:
            The input stream's ``(tag_schema, data_schema)`` unchanged.
        """
        if len(streams) != 1:
            raise ValueError(
                f"SideEffectPod expects exactly 1 input stream; got {len(streams)}."
            )
        return streams[0].output_schema(columns=columns, all_info=all_info)

    def process(
        self, *streams: StreamProtocol, label: str | None = None
    ) -> SideEffectPodStream:
        """Invoke the side-effect pod on the input stream.

        Registers a ``SideEffectInvocation`` with the tracker manager (if
        inside a ``with PipelineJob():`` block), then returns a
        ``SideEffectPodStream`` for standalone / lazy execution.

        Args:
            *streams: Exactly one input stream.
            label: Optional label for the compiled node.

        Returns:
            A ``SideEffectPodStream``.
        """
        if len(streams) != 1:
            raise ValueError(
                f"SideEffectPod.process() expects exactly 1 stream; got {len(streams)}."
            )
        input_stream = streams[0]
        self.tracker_manager.record_side_effect_pod_invocation(
            self, input_stream, label=label
        )
        return SideEffectPodStream(
            side_effect_pod=self,
            input_stream=input_stream,
            label=label,
        )

    def __call__(
        self, *streams: StreamProtocol, label: str | None = None
    ) -> SideEffectPodStream:
        """Convenience alias for ``process``."""
        return self.process(*streams, label=label)


# ---------------------------------------------------------------------------
# Decorators
# ---------------------------------------------------------------------------


def side_effect_pod(
    fn: Callable | None = None,
    *,
    config: SideEffectPodConfig | None = None,
) -> SideEffectPod | Callable[[Callable], SideEffectPod]:
    """Decorator that wraps a callable as a ``SideEffectPod``.

    Supports both bare (``@side_effect_pod``) and parameterised
    (``@side_effect_pod(config=...)``) usage.

    Args:
        fn: The callable to wrap (when used as a bare decorator).
        config: Optional ``SideEffectPodConfig`` to apply.

    Returns:
        A ``SideEffectPod`` (bare usage) or a decorator (parameterised usage).
    """
    def _wrap(f: Callable) -> SideEffectPod:
        return SideEffectPod(f, config=config)

    if fn is not None:
        return _wrap(fn)
    return _wrap


def sink_pod(
    fn: Callable | None = None,
    *,
    config: SideEffectPodConfig | None = None,
) -> SideEffectPod | Callable[[Callable], SideEffectPod]:
    """Decorator preset: ``track_completion=True``, ``drop_on_failure=True``.

    Caller-supplied ``config`` fields override the presets. Supports both
    bare (``@sink_pod``) and parameterised (``@sink_pod(config=...)``) usage.

    Args:
        fn: The callable to wrap (bare usage).
        config: Optional config override.

    Returns:
        A ``SideEffectPod`` or decorator.
    """
    preset = SideEffectPodConfig(track_completion=True, drop_on_failure=True)
    effective_config = _merge_config(preset, config)

    def _wrap(f: Callable) -> SideEffectPod:
        return SideEffectPod(f, config=effective_config)

    if fn is not None:
        return _wrap(fn)
    return _wrap


def tap_pod(
    fn: Callable | None = None,
    *,
    config: SideEffectPodConfig | None = None,
) -> SideEffectPod | Callable[[Callable], SideEffectPod]:
    """Decorator preset: ``track_completion=False``, ``drop_on_failure=False``.

    Caller-supplied ``config`` fields override the presets. Supports both
    bare (``@tap_pod``) and parameterised (``@tap_pod(config=...)``) usage.

    Args:
        fn: The callable to wrap (bare usage).
        config: Optional config override.

    Returns:
        A ``SideEffectPod`` or decorator.
    """
    preset = SideEffectPodConfig(track_completion=False, drop_on_failure=False)
    effective_config = _merge_config(preset, config)

    def _wrap(f: Callable) -> SideEffectPod:
        return SideEffectPod(f, config=effective_config)

    if fn is not None:
        return _wrap(fn)
    return _wrap


def _merge_config(
    preset: SideEffectPodConfig,
    override: SideEffectPodConfig | None,
) -> SideEffectPodConfig:
    """Merge *preset* with caller-supplied *override*.

    Non-default fields in *override* win over the preset.

    Args:
        preset: The decorator's pre-configured defaults.
        override: Optional caller-supplied config.

    Returns:
        A merged ``SideEffectPodConfig``.
    """
    if override is None:
        return preset
    default = SideEffectPodConfig()
    return SideEffectPodConfig(
        track_completion=(
            override.track_completion
            if override.track_completion != default.track_completion
            else preset.track_completion
        ),
        drop_on_failure=(
            override.drop_on_failure
            if override.drop_on_failure != default.drop_on_failure
            else preset.drop_on_failure
        ),
        on_error=(
            override.on_error
            if override.on_error != default.on_error
            else preset.on_error
        ),
        hash_config=(
            override.hash_config
            if override.hash_config != default.hash_config
            else preset.hash_config
        ),
    )
```

- [ ] **Step 4: Run standalone tests**

```bash
uv run pytest tests/test_core/side_effect_pod/test_side_effect_pod.py::TestSideEffectPodStandalone tests/test_core/side_effect_pod/test_side_effect_pod.py::TestDecorators -v
```
Expected: PASS

- [ ] **Step 5: Run Task 3 tests now that `SideEffectPod` exists**

```bash
uv run pytest tests/test_core/side_effect_pod/test_side_effect_pod.py::TestSideEffectInvocation -v
```
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/side_effects.py
git commit -m "feat(side-effects): add SideEffectPod, SideEffectPodStream, decorators"
```

---

## Task 6: `SideEffectNodeProtocol` + `is_side_effect_node()`

**Files:**
- Modify: `src/orcapod/protocols/node_protocols.py`

- [ ] **Step 1: Add `SideEffectNodeProtocol` and TypeGuard**

Open `src/orcapod/protocols/node_protocols.py`. After `OperatorNodeProtocol`, add:

```python
@runtime_checkable
class SideEffectNodeProtocol(Protocol):
    """Protocol for side-effect nodes in orchestrated execution."""

    node_type: str

    def execute(
        self,
        input_stream: "StreamProtocol",
        *,
        observer: "ExecutionObserverProtocol | None" = None,
        run_id: str | None = None,
    ) -> "list[tuple[TagProtocol, DataProtocol]]": ...

    async def async_execute(
        self,
        inputs: "Sequence[ReadableChannel[tuple[TagProtocol, DataProtocol]]]",
        output: "WritableChannel[tuple[TagProtocol, DataProtocol]]",
        *,
        observer: "ExecutionObserverProtocol | None" = None,
        run_id: str | None = None,
    ) -> None: ...

    def attach_databases(
        self,
        pipeline_database: "ArrowDatabaseProtocol | None" = None,
    ) -> None: ...
```

Then add the TypeGuard after `is_operator_node`:

```python
def is_side_effect_node(node: "GraphNode") -> "TypeGuard[SideEffectNodeProtocol]":
    """Check if a node is a side-effect node."""
    return node.node_type == "side_effect"
```

- [ ] **Step 2: Verify import**

```bash
uv run python -c "from orcapod.protocols.node_protocols import is_side_effect_node; print('ok')"
```
Expected: `ok`

- [ ] **Step 3: Commit**

```bash
git add src/orcapod/protocols/node_protocols.py
git commit -m "feat(protocols): add SideEffectNodeProtocol and is_side_effect_node TypeGuard"
```

---

## Task 7: `SideEffectJobNode` (sync execution + DB)

**Files:**
- Modify: `src/orcapod/side_effects.py`
- Test: `tests/test_core/side_effect_pod/test_side_effect_pod.py`

- [ ] **Step 1: Write failing DB tests T5–T7, T11–T12**

Add to `tests/test_core/side_effect_pod/test_side_effect_pod.py`:

```python
# ---------------------------------------------------------------------------
# Task 7 tests — DB-backed execution via SideEffectJobNode
# ---------------------------------------------------------------------------


def _make_in_memory_db():
    """Return a fresh in-memory ArrowDatabase."""
    from orcapod.databases.in_memory import InMemoryDatabase
    return InMemoryDatabase()


class TestSideEffectJobNodeSync:
    """T5–T7, T11–T12: DB-backed sync execution."""

    def _make_node_with_db(self, fn, config=None):
        """Helper: build a SideEffectJobNode with an in-memory DB attached."""
        from orcapod.side_effects import SideEffectPod, SideEffectJobNode

        pod = SideEffectPod(fn, config=config)
        stream = _make_stream(3)
        node = SideEffectJobNode(side_effect_pod=pod, input_stream=stream)
        db = _make_in_memory_db()
        node.attach_databases(pipeline_database=db)
        return node, stream, db

    def test_t5_invocation_log_written_on_success(self):
        """T5: DB row written with status='success'."""
        import polars as pl
        from orcapod.side_effects import SideEffectPod, SideEffectJobNode

        calls = []
        def fn(data, ctx):
            calls.append(True)

        node, stream, db = self._make_node_with_db(fn)
        results = node.execute(stream)
        assert len(results) == 3
        assert len(calls) == 3

        # Read log table
        table_path = (node.pipeline_hash().to_string(), "side_effect_invocations")
        records = db.get_all_records(table_path)
        assert records is not None
        df = pl.from_arrow(records)
        assert len(df) == 3
        assert all(df["status"] == "success")
        assert "full_input_packet_hash" in df.columns
        assert "invocation_hash" not in df.columns  # never stored

    def test_t6_track_completion_skips_on_rerun(self):
        """T6: Second run skips re-delivery; skipped row still emitted."""
        import polars as pl
        from orcapod.side_effects import SideEffectPod, SideEffectJobNode, SideEffectPodConfig

        calls = []
        def fn(data, ctx):
            calls.append(True)

        cfg = SideEffectPodConfig(track_completion=True)
        pod = SideEffectPod(fn, config=cfg)
        stream = _make_stream(2)
        db = _make_in_memory_db()

        node1 = SideEffectJobNode(side_effect_pod=pod, input_stream=stream)
        node1.attach_databases(pipeline_database=db)
        results1 = node1.execute(stream)
        assert len(results1) == 2
        assert len(calls) == 2

        # Second run with same DB
        node2 = SideEffectJobNode(side_effect_pod=pod, input_stream=stream)
        node2.attach_databases(pipeline_database=db)
        results2 = node2.execute(stream)
        assert len(results2) == 2  # rows still emitted (pass-through)
        assert len(calls) == 2  # fn NOT called again

        # Check log has skipped rows
        table_path = (node1.pipeline_hash().to_string(), "side_effect_invocations")
        records = db.get_all_records(table_path)
        df = pl.from_arrow(records)
        assert "skipped" in df["status"].to_list()

    def test_t7_no_track_completion_reruns_delivery(self):
        """T7: track_completion=False always re-delivers."""
        import polars as pl
        from orcapod.side_effects import SideEffectPod, SideEffectJobNode, SideEffectPodConfig

        calls = []
        def fn(data, ctx):
            calls.append(True)

        cfg = SideEffectPodConfig(track_completion=False)
        pod = SideEffectPod(fn, config=cfg)
        stream = _make_stream(2)
        db = _make_in_memory_db()

        node1 = SideEffectJobNode(side_effect_pod=pod, input_stream=stream)
        node1.attach_databases(pipeline_database=db)
        node1.execute(stream)

        node2 = SideEffectJobNode(side_effect_pod=pod, input_stream=stream)
        node2.attach_databases(pipeline_database=db)
        node2.execute(stream)

        assert len(calls) == 4  # called twice per run

        table_path = (node1.pipeline_hash().to_string(), "side_effect_invocations")
        records = db.get_all_records(table_path)
        df = pl.from_arrow(records)
        assert len(df) == 4  # two rows × two runs
        assert all(df["status"] == "success")

    def test_t11_invocation_hash_determinism(self):
        """T11: Identical inputs produce identical invocation_hash."""
        from orcapod.side_effects import SideEffectPod, SideEffectJobNode, InvocationContext

        ctx_list: list[InvocationContext] = []
        def fn(data, ctx):
            ctx_list.append(ctx)

        pod = SideEffectPod(fn)
        stream = _make_stream(1)

        node1 = SideEffectJobNode(side_effect_pod=pod, input_stream=stream)
        node1.attach_databases(pipeline_database=_make_in_memory_db())
        node1.execute(stream)
        hash1 = ctx_list[0].invocation_hash
        ctx_list.clear()

        node2 = SideEffectJobNode(side_effect_pod=pod, input_stream=stream)
        node2.attach_databases(pipeline_database=_make_in_memory_db())
        node2.execute(stream)
        hash2 = ctx_list[0].invocation_hash

        assert hash1 == hash2

    def test_t12_format_id_base64_override(self):
        """T12: format_id with base64 encoding returns valid compound."""
        from orcapod.side_effects import (
            SideEffectPod, SideEffectJobNode, InvocationHashConfig, InvocationContext
        )

        ctx_list: list[InvocationContext] = []
        def fn(data, ctx):
            ctx_list.append(ctx)

        pod = SideEffectPod(fn)
        stream = _make_stream(1)
        node = SideEffectJobNode(side_effect_pod=pod, input_stream=stream)
        node.attach_databases(pipeline_database=_make_in_memory_db())
        node.execute(stream)

        ctx = ctx_list[0]
        override = InvocationHashConfig(encoding="base64", component_length=8)
        fid = ctx.format_id(override)

        assert fid.startswith("orcapod-")
        # Two base64-encoded components of 8 bytes each (11 chars each in base64)
        parts = fid[len("orcapod-"):].split("::")
        assert len(parts) == 2
        import base64
        for part in parts:
            decoded = base64.b64decode(part)
            assert len(decoded) == 8
```

- [ ] **Step 2: Run to verify failure**

```bash
uv run pytest tests/test_core/side_effect_pod/test_side_effect_pod.py::TestSideEffectJobNodeSync -v
```
Expected: FAIL (`ImportError: cannot import name 'SideEffectJobNode'`)

- [ ] **Step 3: Implement `SideEffectJobNode` in `side_effects.py`**

Append to `src/orcapod/side_effects.py`:

```python
# ---------------------------------------------------------------------------
# SideEffectJobNode
# ---------------------------------------------------------------------------


class SideEffectJobNode(StreamBase):
    """DB-backed execution node for side-effect pods.

    Created at pipeline compile time by ``PipelineJob``. Receives a
    ``pipeline_database`` via ``attach_databases()``. ``run_id`` is passed
    as a call-time keyword argument from the orchestrator.

    Inherits from ``StreamBase`` for identity infrastructure and to satisfy
    the ``producer`` / ``upstreams`` / ``output_schema`` contract required
    by ``SyncPipelineOrchestrator._materialize_as_stream``.

    Args:
        side_effect_pod: The ``SideEffectPod`` this node wraps.
        input_stream: The upstream stream at compile time.
        label: Optional display label.
    """

    node_type = "side_effect"

    def __init__(
        self,
        side_effect_pod: SideEffectPod,
        input_stream: StreamProtocol,
        label: str | None = None,
    ) -> None:
        self._pod = side_effect_pod
        self._input_stream = input_stream
        super().__init__(label=label)
        self._pipeline_database: ArrowDatabaseProtocol | None = None
        self._table_path: tuple[str, ...] | None = None

    # ------------------------------------------------------------------
    # StreamBase interface
    # ------------------------------------------------------------------

    @property
    def producer(self):  # type: ignore[override]
        return self._pod

    @property
    def upstreams(self) -> tuple[StreamProtocol, ...]:
        return (self._input_stream,)

    def identity_structure(self) -> Any:
        return (self._pod, self._pod.argument_symmetry((self._input_stream,)))

    def pipeline_identity_structure(self) -> Any:
        return self.identity_structure()

    def computed_label(self) -> str | None:
        return self._pod.label

    def output_schema(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        return self._input_stream.output_schema(columns=columns, all_info=all_info)

    def keys(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        return self._input_stream.keys(columns=columns, all_info=all_info)

    def iter_data(self) -> Iterator[tuple[TagProtocol, DataProtocol]]:
        """Standalone iteration — executes delivery with no DB logging."""
        for tag, data in self._input_stream.iter_data():
            result = _execute_side_effect_row(
                fn=self._pod._fn,
                tag=tag,
                data=data,
                pod_config=self._pod.pod_config,
                pipeline_hash_ch=self.pipeline_hash(),
                pod_content_hash_str=self._pod.content_hash().to_string(),
                pod_name=self._pod.label,
                run_id=None,
                arrow_hasher=self._pod.data_context.arrow_hasher,
                pipeline_database=None,
                table_path=None,
            )
            if result is not None:
                yield result

    def as_table(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> pa.Table:
        """Collect all rows from ``iter_data()`` into an Arrow table."""
        from orcapod.utils import arrow_utils

        tag_tables = []
        data_tables = []
        for tag, data in self.iter_data():
            tag_tables.append(tag.as_table(columns={"system_tags": True}))
            data_tables.append(data.as_table(columns={"source": True}))
        if not tag_tables:
            return pa.table({})
        return arrow_utils.hstack_tables(
            pa.concat_tables(tag_tables),
            pa.concat_tables(data_tables),
        )

    # ------------------------------------------------------------------
    # DB attachment
    # ------------------------------------------------------------------

    def attach_databases(
        self,
        pipeline_database: ArrowDatabaseProtocol | None = None,
    ) -> None:
        """Attach or detach the pipeline database.

        Called by ``PipelineJob._distribute_databases()``. The table path is
        scoped to ``(self.pipeline_hash().to_string(), "side_effect_invocations")``.

        Args:
            pipeline_database: Pre-scoped pipeline DB (at pipeline root),
                or ``None`` to detach.
        """
        self._pipeline_database = pipeline_database
        if pipeline_database is not None:
            self._table_path = (
                self.pipeline_hash().to_string(),
                "side_effect_invocations",
            )
        else:
            self._table_path = None

    # ------------------------------------------------------------------
    # Sync execution
    # ------------------------------------------------------------------

    def execute(
        self,
        input_stream: StreamProtocol,
        *,
        observer: ExecutionObserverProtocol | None = None,
        run_id: str | None = None,
    ) -> list[tuple[TagProtocol, DataProtocol]]:
        """Execute side-effect delivery for all rows in ``input_stream``.

        Args:
            input_stream: Stream of ``(tag, data)`` pairs to process.
            observer: Optional execution observer (currently unused).
            run_id: Pipeline run identifier forwarded from the orchestrator.

        Returns:
            List of ``(tag, data)`` tuples — the pass-through rows.
        """
        results = []
        for tag, data in input_stream.iter_data():
            result = _execute_side_effect_row(
                fn=self._pod._fn,
                tag=tag,
                data=data,
                pod_config=self._pod.pod_config,
                pipeline_hash_ch=self.pipeline_hash(),
                pod_content_hash_str=self._pod.content_hash().to_string(),
                pod_name=self._pod.label,
                run_id=run_id,
                arrow_hasher=self._pod.data_context.arrow_hasher,
                pipeline_database=self._pipeline_database,
                table_path=self._table_path,
            )
            if result is not None:
                results.append(result)
        return results
```

- [ ] **Step 4: Run DB tests**

```bash
uv run pytest tests/test_core/side_effect_pod/test_side_effect_pod.py::TestSideEffectJobNodeSync -v
```
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/side_effects.py
git commit -m "feat(side-effects): add SideEffectJobNode with sync execute and DB logging"
```

---

## Task 8: `SideEffectJobNode.async_execute()`

**Files:**
- Modify: `src/orcapod/side_effects.py`
- Test: `tests/test_core/side_effect_pod/test_side_effect_pod.py`

- [ ] **Step 1: Write failing async tests T13–T14**

Add to `tests/test_core/side_effect_pod/test_side_effect_pod.py`:

```python
# ---------------------------------------------------------------------------
# Task 8 tests — async execution
# ---------------------------------------------------------------------------


class TestSideEffectJobNodeAsync:
    """T13–T14: async_execute via channels."""

    def test_t13_async_execute_basic(self):
        """T13: async_execute processes all rows and writes log."""
        import asyncio
        import polars as pl
        from orcapod.side_effects import SideEffectPod, SideEffectJobNode
        from orcapod.channels import Channel

        calls = []
        def fn(data, ctx):
            calls.append(True)

        pod = SideEffectPod(fn)
        stream = _make_stream(3)
        db = _make_in_memory_db()
        node = SideEffectJobNode(side_effect_pod=pod, input_stream=stream)
        node.attach_databases(pipeline_database=db)

        async def _run():
            ch_in = Channel(buffer_size=10)
            ch_out = Channel(buffer_size=10)

            async def feed():
                for tag, data in stream.iter_data():
                    await ch_in.writer.send((tag, data))
                await ch_in.writer.close()

            await asyncio.gather(
                feed(),
                node.async_execute(
                    [ch_in.reader], ch_out.writer, run_id="test-run-1"
                ),
            )
            return await ch_out.reader.collect()

        results = asyncio.run(_run())
        assert len(results) == 3
        assert len(calls) == 3

        table_path = (node.pipeline_hash().to_string(), "side_effect_invocations")
        records = db.get_all_records(table_path)
        df = pl.from_arrow(records)
        assert len(df) == 3
        assert all(df["status"] == "success")

    def test_t14_async_execute_parallel(self):
        """T14: Concurrent delivery via max_concurrency — all rows complete."""
        import asyncio
        from orcapod.side_effects import SideEffectPod, SideEffectJobNode
        from orcapod.channels import Channel

        results_store = []
        def fn(data, ctx):
            results_store.append(True)

        pod = SideEffectPod(fn)
        stream = _make_stream(10)
        db = _make_in_memory_db()
        node = SideEffectJobNode(side_effect_pod=pod, input_stream=stream)
        node.attach_databases(pipeline_database=db)

        async def _run():
            ch_in = Channel(buffer_size=20)
            ch_out = Channel(buffer_size=20)

            async def feed():
                for tag, data in stream.iter_data():
                    await ch_in.writer.send((tag, data))
                await ch_in.writer.close()

            await asyncio.gather(
                feed(),
                node.async_execute([ch_in.reader], ch_out.writer),
            )
            return await ch_out.reader.collect()

        out = asyncio.run(_run())
        assert len(out) == 10
        assert len(results_store) == 10
```

- [ ] **Step 2: Run to verify failure**

```bash
uv run pytest tests/test_core/side_effect_pod/test_side_effect_pod.py::TestSideEffectJobNodeAsync -v
```
Expected: FAIL (`AttributeError: 'SideEffectJobNode' has no attribute 'async_execute'`)

- [ ] **Step 3: Implement `async_execute` on `SideEffectJobNode`**

Inside the `SideEffectJobNode` class in `src/orcapod/side_effects.py`, add after `execute()`:

```python
    async def async_execute(
        self,
        inputs: Sequence[ReadableChannel[tuple[TagProtocol, DataProtocol]]],
        output: WritableChannel[tuple[TagProtocol, DataProtocol]],
        *,
        observer: ExecutionObserverProtocol | None = None,
        run_id: str | None = None,
    ) -> None:
        """Async side-effect delivery with per-row concurrency control.

        Reads from ``inputs[0]``, dispatches each row as an independent
        async task via ``asyncio.TaskGroup``. Emits non-``None`` results to
        ``output``. Always closes ``output`` in a ``finally`` block.

        Args:
            inputs: Single-element sequence containing the input channel.
            output: Writable channel for pass-through ``(tag, data)`` pairs.
            observer: Optional execution observer (currently unused).
            run_id: Pipeline run identifier from the orchestrator.
        """
        try:
            async def process_one(tag: TagProtocol, data: DataProtocol) -> None:
                result = _execute_side_effect_row(
                    fn=self._pod._fn,
                    tag=tag,
                    data=data,
                    pod_config=self._pod.pod_config,
                    pipeline_hash_ch=self.pipeline_hash(),
                    pod_content_hash_str=self._pod.content_hash().to_string(),
                    pod_name=self._pod.label,
                    run_id=run_id,
                    arrow_hasher=self._pod.data_context.arrow_hasher,
                    pipeline_database=self._pipeline_database,
                    table_path=self._table_path,
                )
                if result is not None:
                    await output.send(result)

            async with asyncio.TaskGroup() as tg:
                async for tag, data in inputs[0]:
                    tg.create_task(process_one(tag, data))
        finally:
            await output.close()
```

- [ ] **Step 4: Run async tests**

```bash
uv run pytest tests/test_core/side_effect_pod/test_side_effect_pod.py::TestSideEffectJobNodeAsync -v
```
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/side_effects.py
git commit -m "feat(side-effects): add SideEffectJobNode.async_execute"
```

---

## Task 9: Pipeline integration + orchestrators

**Files:**
- Modify: `src/orcapod/pipeline/base.py`
- Modify: `src/orcapod/pipeline/job.py`
- Modify: `src/orcapod/pipeline/sync_orchestrator.py`
- Modify: `src/orcapod/pipeline/async_orchestrator.py`
- Test: `tests/test_core/side_effect_pod/test_side_effect_pod.py`

- [ ] **Step 1: Write failing pipeline test T15**

Add to `tests/test_core/side_effect_pod/test_side_effect_pod.py`:

```python
# ---------------------------------------------------------------------------
# Task 9 tests — full pipeline integration
# ---------------------------------------------------------------------------


class TestSideEffectPodPipelineIntegration:
    """T15: Side-effect pod inside a PipelineJob."""

    def test_t15_pipeline_composition_mid_pipeline(self):
        """T15: Pod runs mid-pipeline; downstream node receives filtered output."""
        import polars as pl
        from orcapod.pipeline import PipelineJob
        from orcapod.core.sources import DictSource
        from orcapod.side_effects import SideEffectPod, SideEffectPodConfig
        from orcapod.databases.in_memory import InMemoryDatabase

        delivery_log = []

        @SideEffectPod
        def log_delivery(data, ctx):
            delivery_log.append(dict(data))

        db = InMemoryDatabase()

        with PipelineJob(name="test_pipeline", store=db) as job:
            source = DictSource(
                [{"id": 0, "value": 10}, {"id": 1, "value": 20}],
                tag_columns=["id"],
            )
            stream = source.stream()
            side_effect_stream = log_delivery.process(stream)

        job.run()

        # Delivery log was populated
        assert len(delivery_log) == 2

        # Invocation log written to DB
        from orcapod.pipeline.base import AbstractPipelineBase
        # Find the side_effect node's pipeline hash
        side_effect_nodes = [
            n for n in job._persistent_node_map.values()
            if n.node_type == "side_effect"
        ]
        assert len(side_effect_nodes) == 1
        node = side_effect_nodes[0]
        table_path = (node.pipeline_hash().to_string(), "side_effect_invocations")
        records = db.at(*job.name).get_all_records(table_path)
        assert records is not None
        df = pl.from_arrow(records)
        assert len(df) == 2
        assert all(df["status"] == "success")
```

- [ ] **Step 2: Run to verify failure**

```bash
uv run pytest tests/test_core/side_effect_pod/test_side_effect_pod.py::TestSideEffectPodPipelineIntegration -v
```
Expected: FAIL (TypeError: Unknown node type `"side_effect"`)

- [ ] **Step 3: Add `side_effect_node_class` abstract property to `AbstractPipelineBase`**

Open `src/orcapod/pipeline/base.py`. After `operator_node_class` abstract property, add:

```python
    @property
    @abstractmethod
    def side_effect_node_class(self) -> type:
        """Node class to use for side-effect pod invocations — e.g. ``SideEffectJobNode``."""
        ...
```

- [ ] **Step 4: Add `SideEffectInvocation` branch to `AbstractPipelineBase.compile()`**

In `src/orcapod/pipeline/base.py`, in the `compile()` method, change the final `else:` branch of the `isinstance` dispatch. Currently it reads:

```python
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
```

Change to:

```python
            if isinstance(inv, FunctionInvocation):
                node_map[key] = self.function_node_class(
                    function_pod=inv.pod,
                    input_stream=upstream_nodes[0],
                    label=inv.label,
                )
            elif isinstance(inv, SideEffectInvocation):
                node_map[key] = self.side_effect_node_class(
                    side_effect_pod=inv.pod,
                    input_stream=upstream_nodes[0],
                    label=inv.label,
                )
            else:
                node_map[key] = self.operator_node_class(
                    operator=inv.pod,
                    input_streams=tuple(upstream_nodes),
                    label=inv.label,
                )
```

- [ ] **Step 5: Add `side_effect_node_class` and `_distribute_databases` branch to `PipelineJob`**

Open `src/orcapod/pipeline/job.py`.

Add import at the top:
```python
from orcapod.side_effects import SideEffectJobNode
```

After `operator_node_class = OperatorJobNode`, add:
```python
    side_effect_node_class = SideEffectJobNode
```

In `_distribute_databases()`, after the `elif isinstance(node, OperatorJobNode):` block, add:
```python
            elif isinstance(node, SideEffectJobNode):
                node.attach_databases(pipeline_database=pipeline_db)
```

- [ ] **Step 6: Add `elif is_side_effect_node` branch to sync orchestrator**

Open `src/orcapod/pipeline/sync_orchestrator.py`.

Add import:
```python
from orcapod.protocols.node_protocols import (
    is_function_node,
    is_operator_node,
    is_side_effect_node,  # add
    is_source_node,
)
```

In `SyncPipelineOrchestrator.run()`, after the `elif is_operator_node(node):` block and before the `else:` raise, add:

```python
                elif is_side_effect_node(node):
                    upstream_buf = self._gather_upstream(node, graph, buffers)
                    upstream_node = list(graph.predecessors(node))[0]
                    input_stream = self._materialize_as_stream(upstream_buf, upstream_node)
                    buffers[node] = node.execute(
                        input_stream,
                        observer=effective_observer,
                        run_id=run_id,
                    )
```

- [ ] **Step 7: Add `elif is_side_effect_node` branch to async orchestrator**

Open `src/orcapod/pipeline/async_orchestrator.py`.

Add import:
```python
from orcapod.protocols.node_protocols import (
    is_function_node,
    is_operator_node,
    is_side_effect_node,  # add
    is_source_node,
)
```

In `_run_async()`, after the `elif is_operator_node(node):` block and before the `else:` raise, add:

```python
                    elif is_side_effect_node(node):
                        predecessors = in_edges.get(node, [])
                        if not predecessors:
                            raise ValueError(
                                f"SideEffectNode expects exactly 1 upstream, got 0"
                            )
                        input_reader = edge_readers[(predecessors[0], node)]
                        tg.create_task(
                            node.async_execute(
                                [input_reader],
                                writer,
                                observer=effective_observer,
                                run_id=run_id,
                            )
                        )
```

- [ ] **Step 8: Run pipeline integration test**

```bash
uv run pytest tests/test_core/side_effect_pod/test_side_effect_pod.py::TestSideEffectPodPipelineIntegration -v
```
Expected: PASS

- [ ] **Step 9: Run full test suite to check for regressions**

```bash
uv run pytest tests/ -x -q
```
Expected: All existing tests pass, new tests pass.

- [ ] **Step 10: Commit**

```bash
git add src/orcapod/pipeline/base.py src/orcapod/pipeline/job.py src/orcapod/pipeline/sync_orchestrator.py src/orcapod/pipeline/async_orchestrator.py
git commit -m "feat(pipeline): wire SideEffectJobNode into compile(), _distribute_databases(), and orchestrators"
```

---

## Task 10: `__init__.py` re-exports

**Files:**
- Modify: `src/orcapod/__init__.py`

- [ ] **Step 1: Add re-exports**

Open `src/orcapod/__init__.py`. Add after the `function_pod` import block:

```python
from .side_effects import (
    InvocationContext,
    InvocationHashConfig,
    SideEffectPod,
    SideEffectPodConfig,
    side_effect_pod,
    sink_pod,
    tap_pod,
)
```

Add these names to `__all__`:
```python
    "InvocationContext",
    "InvocationHashConfig",
    "SideEffectPod",
    "SideEffectPodConfig",
    "side_effect_pod",
    "sink_pod",
    "tap_pod",
```

- [ ] **Step 2: Verify re-exports work**

```bash
uv run python -c "
from orcapod import (
    InvocationContext, InvocationHashConfig, SideEffectPod,
    SideEffectPodConfig, side_effect_pod, sink_pod, tap_pod,
)
print('all re-exports ok')
"
```
Expected: `all re-exports ok`

- [ ] **Step 3: Run complete test suite one final time**

```bash
uv run pytest tests/ -q
```
Expected: All tests pass.

- [ ] **Step 4: Commit**

```bash
git add src/orcapod/__init__.py
git commit -m "feat(orcapod): re-export SideEffectPod public API from orcapod.__init__"
```

---

## Self-Review

### Spec coverage

| Spec section | Covered by task |
|---|---|
| `InvocationHashConfig` | Task 1 |
| `SideEffectPodConfig` | Task 1 |
| `InvocationContext` + `format_id()` | Task 1 |
| `SideEffectPodProtocol` | Task 2 |
| `SideEffectInvocation` | Task 3 |
| Tracker chain | Task 4 |
| `SideEffectPod` + `SideEffectPodStream` | Task 5 |
| Decorators `sink_pod`, `tap_pod`, `side_effect_pod` | Task 5 |
| `SideEffectNodeProtocol` + `is_side_effect_node()` | Task 6 |
| `SideEffectJobNode` sync + DB writes | Task 7 |
| Invocation table schema (no `invocation_hash` column) | Task 7 |
| `SideEffectJobNode.async_execute()` | Task 8 |
| `AbstractPipelineBase.compile()` branch | Task 9 |
| `PipelineJob._distribute_databases()` branch | Task 9 |
| Sync orchestrator dispatch | Task 9 |
| Async orchestrator dispatch | Task 9 |
| `__init__.py` re-exports | Task 10 |
| T1–T18 test scenarios | Tasks 5, 7, 8, 9 |
| `docs/concepts/side-effect-pods.md` | ⚠️ Not covered — doc stub is out-of-scope for implementation; add as a follow-up task if required |

### Placeholder scan

No placeholders found — all steps contain complete code.

### Type consistency

- `SideEffectJobNode.execute()` takes `input_stream: StreamProtocol` → matches `SideEffectNodeProtocol.execute()` signature ✓
- `SideEffectJobNode.async_execute()` takes `inputs: Sequence[ReadableChannel[...]]` → matches `SideEffectNodeProtocol.async_execute()` signature ✓
- `_execute_side_effect_row()` referenced in both `SideEffectPodStream.iter_data()` and `SideEffectJobNode.execute()` → same function ✓
- `_write_invocation_row()` uses `pa.large_string()` for all string columns → matches invocation table DDL in spec ✓
- `is_side_effect_node(node)` checks `node.node_type == "side_effect"` → `SideEffectJobNode.node_type = "side_effect"` ✓
- `SideEffectInvocation` imported in `pipeline/base.py` from `pod_invocation.py` → added in Task 3 ✓
- `side_effect_node_class` abstract on `AbstractPipelineBase` → concrete on `PipelineJob` ✓

---

## Execution Options

Plan complete and saved to `superpowers/plans/2026-07-14-itl-525-side-effect-pod.md`.

**1. Subagent-Driven (recommended)** — Fresh subagent per task, review between tasks, fast iteration

**2. Inline Execution** — Execute tasks in this session using executing-plans, batch execution with checkpoints

Which approach?
