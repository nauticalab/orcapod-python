# ITL-525 SideEffectPod Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement `SideEffectPod` — a pass-through pipeline node for side effects with deterministic `invocation_hash`, completion tracking, and `drop_on_failure` filtering.

**Architecture:** `SideEffectPod` subclasses `_FunctionPodBase` and acts as the user-facing API. At pipeline compile time it is promoted to `SideEffectJobNode` (analogous to `FunctionNode`) which handles DB-backed invocation logging and completion tracking. In standalone mode a `SideEffectPodStream` handles row-by-row delivery with no persistence. `InvocationContext` is always constructed and passed to every user function.

**Tech Stack:** Python 3.11+, PyArrow, `uv run pytest` for tests, dataclasses (frozen), asyncio TaskGroup for async path.

---

## File Map

| File | Action | Responsibility |
|---|---|---|
| `src/orcapod/side_effects.py` | CREATE | All public types + `SideEffectPod` + `SideEffectPodStream` + `SideEffectJobNode` + decorators |
| `src/orcapod/protocols/core_protocols/side_effect_pod.py` | CREATE | `SideEffectPodProtocol` |
| `src/orcapod/protocols/node_protocols.py` | MODIFY | Add `SideEffectNodeProtocol`, `is_side_effect_node()` |
| `src/orcapod/protocols/core_protocols/trackers.py` | MODIFY | Add `record_side_effect_pod_invocation()` to `TrackerManagerProtocol` |
| `src/orcapod/pipeline/base.py` | MODIFY | Add `SideEffectInvocation`, `record_side_effect_pod_invocation()`, `side_effect_node_class`, compile branch |
| `src/orcapod/pipeline/job.py` | MODIFY | `side_effect_node_class`, `_distribute_databases()`, `to_invocations()` |
| `src/orcapod/pipeline/sync_orchestrator.py` | MODIFY | `elif is_side_effect_node` branch |
| `src/orcapod/pipeline/async_orchestrator.py` | MODIFY | `elif is_side_effect_node` branch |
| `src/orcapod/__init__.py` | MODIFY | Re-export public types |
| `tests/test_core/side_effect_pod/__init__.py` | CREATE | Empty |
| `tests/test_core/side_effect_pod/test_side_effect_pod.py` | CREATE | T1–T18 |
| `docs/concepts/side-effect-pods.md` | CREATE | User-facing concept documentation |

---

### Task 1: Types — `InvocationHashConfig`, `SideEffectPodConfig`, `InvocationContext`

**Files:**
- Create: `src/orcapod/side_effects.py`
- Test: `tests/test_core/side_effect_pod/test_side_effect_pod.py`

- [ ] **Step 1: Create test directory**

```bash
mkdir -p tests/test_core/side_effect_pod
touch tests/test_core/side_effect_pod/__init__.py
```

- [ ] **Step 2: Write failing tests for types**

Create `tests/test_core/side_effect_pod/test_side_effect_pod.py`:

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

- [ ] **Step 3: Run tests to confirm they fail**

```bash
uv run pytest tests/test_core/side_effect_pod/test_side_effect_pod.py::TestInvocationHashConfig tests/test_core/side_effect_pod/test_side_effect_pod.py::TestSideEffectPodConfig -v
```

Expected: FAIL with `ModuleNotFoundError` or `ImportError` (no `orcapod.side_effects` module yet).

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

Expected: PASS (5 tests).

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/side_effects.py tests/test_core/side_effect_pod/__init__.py tests/test_core/side_effect_pod/test_side_effect_pod.py
git commit -m "feat(side-effects): add InvocationHashConfig, SideEffectPodConfig, InvocationContext"
```

---

### Task 2: `SideEffectPodProtocol` + `SideEffectNodeProtocol` + Protocol layer updates

**Files:**
- Create: `src/orcapod/protocols/core_protocols/side_effect_pod.py`
- Modify: `src/orcapod/protocols/node_protocols.py`
- Modify: `src/orcapod/protocols/core_protocols/trackers.py`

- [ ] **Step 1: Create `SideEffectPodProtocol`**

Create `src/orcapod/protocols/core_protocols/side_effect_pod.py`:

```python
"""Protocol definition for SideEffectPod."""
from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

from orcapod.protocols.hashing_protocols import PipelineElementProtocol

if TYPE_CHECKING:
    from orcapod.protocols.core_protocols.streams import StreamProtocol
    from orcapod.side_effects import SideEffectPodConfig, SideEffectPodStream


@runtime_checkable
class SideEffectPodProtocol(PipelineElementProtocol, Protocol):
    """Protocol for a pass-through side-effect pod.

    Implementors receive every (Tag, Data) row, execute a side effect,
    and emit the row downstream (subject to ``drop_on_failure``).
    """

    pod_config: SideEffectPodConfig

    def process(self, stream: StreamProtocol) -> SideEffectPodStream: ...

    def output_schema(
        self, stream: StreamProtocol
    ) -> tuple[SchemaType, SchemaType]: ...
```

Wait — `SchemaType` isn't imported here. Use the `Schema` type from `orcapod.types`:

```python
"""Protocol definition for SideEffectPod."""
from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

from orcapod.protocols.hashing_protocols import PipelineElementProtocol

if TYPE_CHECKING:
    from orcapod.protocols.core_protocols.streams import StreamProtocol
    from orcapod.side_effects import SideEffectPodConfig, SideEffectPodStream
    from orcapod.types import Schema


@runtime_checkable
class SideEffectPodProtocol(PipelineElementProtocol, Protocol):
    """Protocol for a pass-through side-effect pod.

    Implementors receive every (Tag, Data) row, execute a side effect,
    and emit the row downstream (subject to ``drop_on_failure``).
    """

    pod_config: SideEffectPodConfig

    def process(self, stream: StreamProtocol) -> SideEffectPodStream: ...

    def output_schema(
        self, stream: StreamProtocol
    ) -> tuple[Schema, Schema]: ...
```

- [ ] **Step 2: Expose `SideEffectPodProtocol` from `core_protocols/__init__.py`**

Read the existing `__init__.py`:

```bash
# Check what's already exported
grep -n "SideEffect\|FunctionPod\|OperatorPod" src/orcapod/protocols/core_protocols/__init__.py
```

Add the import:

```python
from orcapod.protocols.core_protocols.side_effect_pod import SideEffectPodProtocol
```

- [ ] **Step 3: Add `SideEffectNodeProtocol` and `is_side_effect_node()` to `node_protocols.py`**

Read `src/orcapod/protocols/node_protocols.py` first to understand its current structure, then add:

```python
from typing import TYPE_CHECKING, Protocol, Sequence, TypeGuard, runtime_checkable

# ... existing imports ...

if TYPE_CHECKING:
    from orcapod.channels import ReadableChannel, WritableChannel
    from orcapod.protocols.core_protocols import DataProtocol, TagProtocol
    from orcapod.protocols.database_protocols import ArrowDatabaseProtocol
    from orcapod.protocols.observability_protocols import ExecutionObserverProtocol


@runtime_checkable
class SideEffectNodeProtocol(Protocol):
    """Protocol for a DB-backed side-effect node in the compiled pipeline graph."""

    node_type: str

    def execute(
        self,
        input_stream: StreamProtocol,
        *,
        observer: ExecutionObserverProtocol | None = None,
        run_id: str | None = None,
    ) -> list[tuple[TagProtocol, DataProtocol]]: ...

    async def async_execute(
        self,
        inputs: Sequence[ReadableChannel[tuple[TagProtocol, DataProtocol]]],
        output: WritableChannel[tuple[TagProtocol, DataProtocol]],
        *,
        observer: ExecutionObserverProtocol | None = None,
        run_id: str | None = None,
    ) -> None: ...

    def attach_databases(
        self,
        pipeline_database: ArrowDatabaseProtocol | None = None,
    ) -> None: ...


def is_side_effect_node(node: object) -> TypeGuard[SideEffectNodeProtocol]:
    """Return ``True`` if ``node`` satisfies ``SideEffectNodeProtocol``."""
    return isinstance(node, SideEffectNodeProtocol)
```

- [ ] **Step 4: Add `record_side_effect_pod_invocation()` to `TrackerManagerProtocol`**

In `src/orcapod/protocols/core_protocols/trackers.py`, add to `TrackerManagerProtocol`:

```python
    def record_side_effect_pod_invocation(
        self,
        pod: SideEffectPodProtocol,
        input_stream: StreamProtocol,
        label: str | None = None,
    ) -> None:
        """Record a side-effect pod invocation in all active trackers.

        Args:
            pod: The side-effect pod that was invoked.
            input_stream: The input stream used for this invocation.
            label: Optional display label for the invocation.
        """
        ...
```

(Also add `SideEffectPodProtocol` to the `TYPE_CHECKING` import block.)

- [ ] **Step 5: Run existing tests to confirm no regressions**

```bash
uv run pytest tests/ -x -q
```

Expected: all existing tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/protocols/core_protocols/side_effect_pod.py \
        src/orcapod/protocols/core_protocols/__init__.py \
        src/orcapod/protocols/node_protocols.py \
        src/orcapod/protocols/core_protocols/trackers.py
git commit -m "feat(side-effects): add SideEffectPodProtocol, SideEffectNodeProtocol, protocol layer"
```

---

### Task 3: `SideEffectPod` + `SideEffectPodStream` (standalone mode)

**Files:**
- Modify: `src/orcapod/side_effects.py` (append)

- [ ] **Step 1: Write failing tests for standalone execution (T1–T4)**

Append to `tests/test_core/side_effect_pod/test_side_effect_pod.py`:

```python
# ---------------------------------------------------------------------------
# Task 3 tests — standalone execution (no PipelineJob)
# ---------------------------------------------------------------------------


class TestSideEffectPodStandalone:
    """T1–T4: basic pass-through and InvocationContext in standalone mode."""

    def test_t1_passthrough_drop_on_failure_false(self):
        """T1: drop_on_failure=False, all succeed → all rows in output."""
        from orcapod.side_effects import SideEffectPodConfig, side_effect_pod

        calls = []

        @side_effect_pod(config=SideEffectPodConfig(drop_on_failure=False, track_completion=False))
        def record(data, ctx):
            calls.append(data)

        stream = _make_stream(3)
        out_stream = record(stream)
        rows = list(out_stream.iter_data())
        assert len(rows) == 3
        assert len(calls) == 3

    def test_t2_passthrough_drop_on_failure_true_all_succeed(self):
        """T2: drop_on_failure=True (default), all succeed → all rows emitted."""
        from orcapod.side_effects import side_effect_pod

        @side_effect_pod
        def record(data, ctx):
            pass

        stream = _make_stream(3)
        rows = list(record(stream).iter_data())
        assert len(rows) == 3

    def test_t3_invocation_context_always_passed(self):
        """T3: ctx.invocation_hash is non-empty; ctx.format_id() returns 'orcapod-...'."""
        from orcapod.side_effects import side_effect_pod

        contexts = []

        @side_effect_pod
        def capture(data, ctx):
            contexts.append(ctx)

        list(_make_stream(2).pipe(capture).iter_data())
        assert len(contexts) == 2
        for ctx in contexts:
            assert isinstance(ctx.invocation_hash, str)
            assert len(ctx.invocation_hash) > 0
            fid = ctx.format_id()
            assert fid.startswith("orcapod-")

    def test_t4_invocation_context_ignored_by_callee(self):
        """T4: callee receives ctx but doesn't use it — pod executes without error."""
        from orcapod.side_effects import side_effect_pod

        @side_effect_pod
        def my_fn(data, _ctx):
            pass

        rows = list(_make_stream(2).pipe(my_fn).iter_data())
        assert len(rows) == 2
```

- [ ] **Step 2: Run to confirm tests fail**

```bash
uv run pytest tests/test_core/side_effect_pod/test_side_effect_pod.py::TestSideEffectPodStandalone -v
```

Expected: FAIL (no `SideEffectPod` class yet).

- [ ] **Step 3: Implement `SideEffectPod` and `SideEffectPodStream` in `side_effects.py`**

Read `src/orcapod/core/function_pod.py` to understand `_FunctionPodBase` interface, then append to `src/orcapod/side_effects.py`:

```python
# ---------------------------------------------------------------------------
# SideEffectPodStream
# ---------------------------------------------------------------------------

# (append after InvocationContext class)

class SideEffectPodStream(StreamBase):
    """Lazy pass-through stream returned by ``SideEffectPod.process()``.

    In standalone mode (no ``PipelineJob``), iterates the upstream stream
    row by row, calls the user function with ``(data, ctx)``, and yields
    ``(tag, data)`` downstream subject to ``on_error`` and
    ``drop_on_failure`` settings.

    Args:
        pod: The ``SideEffectPod`` that created this stream.
        upstream: The upstream stream to consume.
    """

    def __init__(
        self,
        pod: SideEffectPod,
        upstream: StreamProtocol,
    ) -> None:
        from orcapod.hashing.semantic_hashing import get_default_semantic_hasher
        from orcapod.hashing.arrow_hasher import get_default_arrow_hasher
        super().__init__()
        self._pod = pod
        self._upstream = upstream
        self._semantic_hasher = get_default_semantic_hasher()
        self._arrow_hasher = get_default_arrow_hasher()

    def output_schema(self) -> tuple[Schema, Schema]:
        """Delegate to upstream (pass-through contract)."""
        return self._upstream.output_schema()

    def keys(self) -> tuple[str, ...]:
        """Delegate to upstream (pass-through contract)."""
        return self._upstream.keys()

    def iter_data(self) -> Iterator[tuple[TagProtocol, DataProtocol]]:
        """Iterate upstream rows, deliver side effect, yield filtered output."""
        from orcapod.utils import arrow_utils
        from orcapod.hashing.arrow_hasher import get_default_arrow_hasher

        arrow_hasher = get_default_arrow_hasher()
        pod_config = self._pod.pod_config
        hash_config = pod_config.hash_config
        pipeline_hash_ch = self._pod.pipeline_hash()

        for tag, data in self._upstream.iter_data():
            # Build full_input_packet_hash
            tag_table = tag.as_table(columns={"system_tags": True})
            data_table = data.as_table(columns={"source": True})
            full_table = arrow_utils.hstack_tables(tag_table, data_table)
            fip_hash = arrow_hasher.hash_table(full_table)

            # Serialize invocation_hash
            c1 = _serialize_component(pipeline_hash_ch, hash_config)
            c2 = _serialize_component(fip_hash, hash_config)
            invocation_hash = f"{c1}::{c2}"

            ctx = InvocationContext(
                invocation_hash=invocation_hash,
                pod_name=self._pod.label,
                pod_content_hash=self._pod.content_hash().to_string(),
                pipeline_run_id=None,
                _pipeline_hash_ch=pipeline_hash_ch,
                _full_input_packet_hash_ch=fip_hash,
                _hash_config=hash_config,
                _track_completion=pod_config.track_completion,
            )

            try:
                self._pod._user_fn(data, ctx)
                yield (tag, data)
            except Exception as exc:
                if pod_config.on_error == "raise":
                    raise
                logger.warning(
                    "SideEffectPod %r delivery failed: %s", self._pod.label, exc
                )
                if pod_config.drop_on_failure:
                    continue
                yield (tag, data)

    def pipeline_identity_structure(self) -> Any:
        return (self._pod.pipeline_hash(), self._upstream.pipeline_hash())

    def content_identity_structure(self) -> Any:
        return (self._pod.content_hash(), self._upstream.content_hash())


# ---------------------------------------------------------------------------
# SideEffectPod
# ---------------------------------------------------------------------------


class SideEffectPod:
    """A pipeline node whose primary purpose is a side effect.

    Wraps a ``(data: T, ctx: InvocationContext) -> None`` callable.
    Returns a pass-through stream. When ``drop_on_failure=True``, only
    successfully-delivered rows flow downstream.

    In standalone mode (no ``PipelineJob``), executes row-by-row via
    ``SideEffectPodStream`` with no invocation logging.

    In pipeline mode, promoted to ``SideEffectJobNode`` at compile time,
    which adds DB-backed invocation logging and completion tracking.

    Args:
        fn: User function ``(data, ctx: InvocationContext) -> None``.
        config: Pod configuration. Defaults to ``SideEffectPodConfig()``.
        label: Human-readable name for this pod. Defaults to the function name.
    """

    def __init__(
        self,
        fn: Callable,
        config: SideEffectPodConfig | None = None,
        label: str | None = None,
    ) -> None:
        from orcapod.core.data_function import PythonDataFunction
        self._user_fn = fn
        self.pod_config = config or SideEffectPodConfig()
        self.label = label or getattr(fn, "__name__", repr(fn))
        # Wrap in PythonDataFunction solely for content_hash / pipeline_hash
        self._data_function = PythonDataFunction(fn)

    def content_hash(self) -> ContentHash:
        """Hash based on the wrapped function's identity."""
        return self._data_function.content_hash()

    def pipeline_hash(self) -> ContentHash:
        """Hash based on schema/topology only."""
        return self._data_function.content_hash()

    def pipeline_identity_structure(self) -> Any:
        return self._data_function.pipeline_identity_structure()

    def content_identity_structure(self) -> Any:
        return self._data_function.content_identity_structure()

    def output_schema(self, stream: StreamProtocol) -> tuple[Schema, Schema]:
        """Return the input stream's schema unchanged (pass-through)."""
        return stream.output_schema()

    def keys(self, stream: StreamProtocol) -> tuple[str, ...]:
        """Return the input stream's tag keys unchanged (pass-through)."""
        return stream.keys()

    def process(self, stream: StreamProtocol) -> SideEffectPodStream:
        """Wrap stream in a ``SideEffectPodStream`` for lazy execution.

        Args:
            stream: The upstream stream to consume.

        Returns:
            A ``SideEffectPodStream`` that delivers the side effect row by row.
        """
        from orcapod.core.tracker import DEFAULT_TRACKER_MANAGER
        DEFAULT_TRACKER_MANAGER.record_side_effect_pod_invocation(
            pod=self, input_stream=stream
        )
        return SideEffectPodStream(pod=self, upstream=stream)

    def __call__(self, stream: StreamProtocol) -> SideEffectPodStream:
        """Alias for ``process()``."""
        return self.process(stream)
```

- [ ] **Step 4: Add `.pipe()` helper to `StreamBase` (if not present)**

Check if `StreamBase` has a `.pipe()` method:

```bash
grep -n "def pipe" src/orcapod/core/streams/base.py
```

If not present, the test's `_make_stream(2).pipe(capture)` call needs a helper. Add to `StreamBase`:

```python
def pipe(self, pod: Any) -> Any:
    """Apply a pod to this stream. Convenience alias for ``pod(self)``."""
    return pod(self)
```

- [ ] **Step 5: Run tests**

```bash
uv run pytest tests/test_core/side_effect_pod/test_side_effect_pod.py::TestSideEffectPodStandalone -v
```

Expected: PASS (4 tests).

- [ ] **Step 6: Run full test suite**

```bash
uv run pytest tests/ -x -q
```

Expected: all tests pass.

- [ ] **Step 7: Commit**

```bash
git add src/orcapod/side_effects.py src/orcapod/core/streams/base.py
git commit -m "feat(side-effects): add SideEffectPod and SideEffectPodStream standalone execution"
```

---

### Task 4: `SideEffectJobNode` — DB-backed execution + invocation logging

**Files:**
- Modify: `src/orcapod/side_effects.py` (append)

- [ ] **Step 1: Write failing tests for DB-backed execution (T5–T7)**

Append to `tests/test_core/side_effect_pod/test_side_effect_pod.py`:

```python
# ---------------------------------------------------------------------------
# Task 4 tests — DB-backed pipeline execution
# ---------------------------------------------------------------------------

import pytest


def _make_in_memory_db():
    """Return a fresh in-memory ArrowDatabase for testing."""
    from orcapod.databases.in_memory import InMemoryArrowDatabase
    return InMemoryArrowDatabase()


class TestSideEffectJobNodeLogging:
    """T5–T7: invocation logging and completion tracking via PipelineJob."""

    def test_t5_invocation_log_written(self):
        """T5: After pipeline run, a 'success' row exists in the invocation table."""
        from orcapod.pipeline.job import PipelineJob
        from orcapod.side_effects import sink_pod

        db = _make_in_memory_db()
        calls = []

        @sink_pod
        def record(data, ctx):
            calls.append(ctx.invocation_hash)

        stream = _make_stream(2)
        with PipelineJob(database=db) as job:
            out = record(stream)
            result = job.run(out)

        assert len(calls) == 2
        # Check that the invocation table has 2 success rows
        # Table path: <pipeline_hash>/side_effect_invocations
        pipeline_hash = record.pipeline_hash().to_string()
        table = db.at(pipeline_hash, "side_effect_invocations").read()
        assert table is not None
        statuses = table.column("status").to_pylist()
        assert statuses.count("success") == 2

    def test_t6_track_completion_true_skips_on_rerun(self):
        """T6: track_completion=True — same inputs on second run: fn called once total."""
        from orcapod.pipeline.job import PipelineJob
        from orcapod.side_effects import SideEffectPodConfig, side_effect_pod

        db = _make_in_memory_db()
        calls = []

        @side_effect_pod(config=SideEffectPodConfig(track_completion=True, drop_on_failure=True))
        def record(data, ctx):
            calls.append(1)

        stream = _make_stream(2)

        with PipelineJob(database=db) as job:
            out = record(stream)
            job.run(out)

        with PipelineJob(database=db) as job:
            out = record(stream)
            result = job.run(out)

        # Function called only once total (skipped on second run)
        assert len(calls) == 2  # first run only
        # Second run: skipped rows
        pipeline_hash = record.pipeline_hash().to_string()
        table = db.at(pipeline_hash, "side_effect_invocations").read()
        statuses = table.column("status").to_pylist()
        assert statuses.count("skipped") == 2
        # Output still has rows on second run
        assert len(list(result)) == 2

    def test_t7_track_completion_false_reruns(self):
        """T7: track_completion=False — both runs call fn; two 'success' rows per input."""
        from orcapod.pipeline.job import PipelineJob
        from orcapod.side_effects import SideEffectPodConfig, side_effect_pod

        db = _make_in_memory_db()
        calls = []

        @side_effect_pod(config=SideEffectPodConfig(track_completion=False, drop_on_failure=True))
        def record(data, ctx):
            calls.append(1)

        stream = _make_stream(2)

        with PipelineJob(database=db) as job:
            out = record(stream)
            job.run(out)

        with PipelineJob(database=db) as job:
            out = record(stream)
            job.run(out)

        assert len(calls) == 4  # 2 rows × 2 runs
        pipeline_hash = record.pipeline_hash().to_string()
        table = db.at(pipeline_hash, "side_effect_invocations").read()
        statuses = table.column("status").to_pylist()
        assert statuses.count("success") == 4
```

- [ ] **Step 2: Run to confirm they fail**

```bash
uv run pytest tests/test_core/side_effect_pod/test_side_effect_pod.py::TestSideEffectJobNodeLogging -v
```

Expected: FAIL.

- [ ] **Step 3: Implement `SideEffectJobNode` in `side_effects.py`**

Read `src/orcapod/core/function_pod.py` (specifically `FunctionNode`) to understand patterns, then append to `src/orcapod/side_effects.py`:

```python
# ---------------------------------------------------------------------------
# SideEffectJobNode
# ---------------------------------------------------------------------------


class SideEffectJobNode:
    """DB-backed execution node for a ``SideEffectPod`` inside a compiled pipeline.

    Created at pipeline compile time. Never instantiated directly by users.

    Args:
        side_effect_pod: The ``SideEffectPod`` to wrap.
        input_stream: The upstream stream at compile time.
        label: Optional display label.
    """

    node_type: str = "side_effect"

    def __init__(
        self,
        side_effect_pod: SideEffectPod,
        input_stream: StreamProtocol,
        label: str | None = None,
    ) -> None:
        self._pod = side_effect_pod
        self._input_stream = input_stream
        self.label = label or side_effect_pod.label
        self._pipeline_database: ArrowDatabaseProtocol | None = None
        self._table_initialized = False

    def pipeline_hash(self) -> ContentHash:
        """Delegate to the wrapped pod."""
        return self._pod.pipeline_hash()

    def attach_databases(
        self,
        pipeline_database: ArrowDatabaseProtocol | None = None,
    ) -> None:
        """Receive the pipeline database from ``PipelineJob._distribute_databases()``.

        Args:
            pipeline_database: The pipeline-scoped database, or ``None`` if no
                persistence is configured.
        """
        self._pipeline_database = pipeline_database

    def _ensure_table(self) -> None:
        """Create the invocation table on first use if it does not exist."""
        if self._table_initialized or self._pipeline_database is None:
            return
        import pyarrow as pa
        table_path = (self._pod.pipeline_hash().to_string(), "side_effect_invocations")
        db = self._pipeline_database.at(*table_path)
        schema = pa.schema([
            pa.field("full_input_packet_hash", pa.large_utf8(), nullable=False),
            pa.field("pod_content_hash", pa.large_utf8(), nullable=False),
            pa.field("pipeline_run_id", pa.large_utf8(), nullable=True),
            pa.field("executed_at", pa.timestamp("us", tz="UTC"), nullable=False),
            pa.field("status", pa.large_utf8(), nullable=False),
            pa.field("error_message", pa.large_utf8(), nullable=True),
        ])
        db.create_table_if_not_exists(schema)
        self._table_initialized = True

    def _write_invocation_row(
        self,
        fip_hash_str: str,
        run_id: str | None,
        status: str,
        error_message: str | None = None,
    ) -> None:
        """Write one row to the invocation table."""
        if self._pipeline_database is None:
            return
        import pyarrow as pa
        self._ensure_table()
        table_path = (self._pod.pipeline_hash().to_string(), "side_effect_invocations")
        db = self._pipeline_database.at(*table_path)
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        row = pa.table({
            "full_input_packet_hash": pa.array([fip_hash_str], pa.large_utf8()),
            "pod_content_hash": pa.array([self._pod.content_hash().to_string()], pa.large_utf8()),
            "pipeline_run_id": pa.array([run_id], pa.large_utf8()),
            "executed_at": pa.array([now], pa.timestamp("us", tz="UTC")),
            "status": pa.array([status], pa.large_utf8()),
            "error_message": pa.array([error_message], pa.large_utf8()),
        })
        db.append(row)

    def _lookup_completion_status(self, fip_hash_str: str) -> str | None:
        """Return the most recent status for this input hash, or ``None``."""
        if self._pipeline_database is None:
            return None
        self._ensure_table()
        table_path = (self._pod.pipeline_hash().to_string(), "side_effect_invocations")
        db = self._pipeline_database.at(*table_path)
        table = db.read()
        if table is None or len(table) == 0:
            return None
        import pyarrow.compute as pc
        mask = pc.equal(table.column("full_input_packet_hash"), fip_hash_str)
        filtered = table.filter(mask)
        if len(filtered) == 0:
            return None
        # Sort by executed_at descending, take first
        indices = pc.sort_indices(filtered, sort_keys=[("executed_at", "descending")])
        most_recent = filtered.take(indices[:1])
        return most_recent.column("status")[0].as_py()

    def _execute_row(
        self,
        tag: TagProtocol,
        data: DataProtocol,
        run_id: str | None,
    ) -> tuple[TagProtocol, DataProtocol] | None:
        """Execute delivery for one row. Returns ``(tag, data)`` or ``None`` to drop."""
        from orcapod.utils import arrow_utils
        from orcapod.hashing.arrow_hasher import get_default_arrow_hasher

        arrow_hasher = get_default_arrow_hasher()
        pod_config = self._pod.pod_config
        hash_config = pod_config.hash_config
        pipeline_hash_ch = self._pod.pipeline_hash()

        # Step 1: compute full_input_packet_hash
        tag_table = tag.as_table(columns={"system_tags": True})
        data_table = data.as_table(columns={"source": True})
        full_table = arrow_utils.hstack_tables(tag_table, data_table)
        fip_hash = arrow_hasher.hash_table(full_table)
        fip_hash_str = fip_hash.to_string()

        # Step 2: serialize invocation_hash
        c1 = _serialize_component(pipeline_hash_ch, hash_config)
        c2 = _serialize_component(fip_hash, hash_config)
        if not pod_config.track_completion and run_id is not None:
            invocation_hash = f"{c1}::{c2}::{run_id}"
        else:
            invocation_hash = f"{c1}::{c2}"

        # Step 3: completion check
        if pod_config.track_completion and self._pipeline_database is not None:
            status = self._lookup_completion_status(fip_hash_str)
            if status == "success":
                self._write_invocation_row(fip_hash_str, run_id, "skipped")
                return (tag, data)

        # Step 4: build InvocationContext
        ctx = InvocationContext(
            invocation_hash=invocation_hash,
            pod_name=self._pod.label,
            pod_content_hash=self._pod.content_hash().to_string(),
            pipeline_run_id=run_id,
            _pipeline_hash_ch=pipeline_hash_ch,
            _full_input_packet_hash_ch=fip_hash,
            _hash_config=hash_config,
            _track_completion=pod_config.track_completion,
        )

        # Step 5: call user function
        try:
            self._pod._user_fn(data, ctx)
            self._write_invocation_row(fip_hash_str, run_id, "success")
            return (tag, data)
        except Exception as exc:
            self._write_invocation_row(fip_hash_str, run_id, "failed", str(exc))
            if pod_config.on_error == "raise":
                raise
            logger.warning(
                "SideEffectPod %r delivery failed: %s", self._pod.label, exc
            )
            if pod_config.drop_on_failure:
                return None
            return (tag, data)

    def execute(
        self,
        input_stream: StreamProtocol,
        *,
        observer: ExecutionObserverProtocol | None = None,
        run_id: str | None = None,
    ) -> list[tuple[TagProtocol, DataProtocol]]:
        """Synchronously execute all rows and return filtered results.

        Args:
            input_stream: The stream to consume.
            observer: Optional execution observer (unused, present for interface parity).
            run_id: Pipeline run identifier from the orchestrator.

        Returns:
            List of ``(tag, data)`` tuples for rows that were not dropped.
        """
        self._ensure_table()
        results = []
        for tag, data in input_stream.iter_data():
            result = self._execute_row(tag, data, run_id)
            if result is not None:
                results.append(result)
        return results

    async def async_execute(
        self,
        inputs: Sequence[ReadableChannel[tuple[TagProtocol, DataProtocol]]],
        output: WritableChannel[tuple[TagProtocol, DataProtocol]],
        *,
        observer: ExecutionObserverProtocol | None = None,
        run_id: str | None = None,
    ) -> None:
        """Asynchronously execute rows via a channel, sending results to ``output``.

        Args:
            inputs: Sequence of readable channels (length 1 for side-effect pods).
            output: Writable channel to send ``(tag, data)`` results.
            observer: Optional execution observer (unused).
            run_id: Pipeline run identifier from the orchestrator.
        """
        self._ensure_table()
        try:
            async for tag, data in inputs[0]:
                result = self._execute_row(tag, data, run_id)
                if result is not None:
                    await output.send(result)
        finally:
            await output.aclose()
```

- [ ] **Step 4: Run failing tests**

```bash
uv run pytest tests/test_core/side_effect_pod/test_side_effect_pod.py::TestSideEffectJobNodeLogging -v
```

Expected: still FAIL (pipeline integration not wired yet).

- [ ] **Step 5: Commit (even with failing tests — captures incremental progress)**

```bash
git add src/orcapod/side_effects.py
git commit -m "feat(side-effects): add SideEffectJobNode with invocation logging"
```

---

### Task 5: Pipeline integration — base, job, orchestrators

**Files:**
- Modify: `src/orcapod/pipeline/base.py`
- Modify: `src/orcapod/pipeline/job.py`
- Modify: `src/orcapod/pipeline/sync_orchestrator.py`
- Modify: `src/orcapod/pipeline/async_orchestrator.py`
- Modify: `src/orcapod/core/tracker.py` (add `record_side_effect_pod_invocation()`)

- [ ] **Step 1: Read pipeline files to understand current structure**

```bash
# Understand the compile() and _distribute_databases() patterns
grep -n "FunctionInvocation\|OperatorInvocation\|compile\|_distribute_databases\|to_invocations" \
  src/orcapod/pipeline/base.py | head -50
grep -n "FunctionJobNode\|_distribute_databases\|to_invocations\|side_effect" \
  src/orcapod/pipeline/job.py | head -50
grep -n "is_function_node\|is_operator_node\|run_id" \
  src/orcapod/pipeline/sync_orchestrator.py | head -30
grep -n "is_function_node\|is_operator_node\|run_id" \
  src/orcapod/pipeline/async_orchestrator.py | head -30
```

- [ ] **Step 2: Add `SideEffectInvocation` to `pipeline/base.py`**

After reading `pipeline/base.py`, add alongside `FunctionInvocation`:

```python
@dataclasses.dataclass(frozen=True)
class SideEffectInvocation(PodInvocation):
    """Recorded side-effect pod invocation for pipeline compilation.

    Args:
        pod: The ``SideEffectPod`` that was called.
        input_streams: The upstream streams (always length 1).
        label: Optional display label.
    """

    pod: SideEffectPodProtocol
    input_streams: tuple[StreamProtocol, ...]  # always length 1
    label: str | None = None
```

Also add `record_side_effect_pod_invocation()` and `side_effect_node_class` property + compile branch (per spec).

- [ ] **Step 3: Update `PipelineJob` in `pipeline/job.py`**

Add:

```python
side_effect_node_class = SideEffectJobNode
```

Update `_distribute_databases()`:

```python
elif isinstance(node, SideEffectJobNode):
    node.attach_databases(pipeline_database=pipeline_db)
```

Update `to_invocations()` to handle `SideEffectJobNode` (look at how `FunctionJobNode` is handled and mirror it).

- [ ] **Step 4: Update sync orchestrator**

In `pipeline/sync_orchestrator.py`, add `elif is_side_effect_node(node):` branch after the existing `elif` for function nodes:

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

- [ ] **Step 5: Update async orchestrator**

In `pipeline/async_orchestrator.py`, add `elif is_side_effect_node(node):` branch:

```python
elif is_side_effect_node(node):
    predecessors = in_edges.get(node, [])
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

- [ ] **Step 6: Update `core/tracker.py`**

Add `record_side_effect_pod_invocation()` to `DefaultTrackerManager` class:

```python
def record_side_effect_pod_invocation(
    self,
    pod: SideEffectPodProtocol,
    input_stream: StreamProtocol,
    label: str | None = None,
) -> None:
    """Broadcast side-effect pod invocation to all active trackers.

    Args:
        pod: The side-effect pod that was invoked.
        input_stream: The upstream stream.
        label: Optional display label.
    """
    for tracker in self.get_active_trackers():
        if hasattr(tracker, "record_side_effect_pod_invocation"):
            tracker.record_side_effect_pod_invocation(pod, input_stream, label)
```

- [ ] **Step 7: Run T5–T7 tests**

```bash
uv run pytest tests/test_core/side_effect_pod/test_side_effect_pod.py::TestSideEffectJobNodeLogging -v
```

Expected: PASS.

- [ ] **Step 8: Run full test suite**

```bash
uv run pytest tests/ -x -q
```

Expected: all tests pass.

- [ ] **Step 9: Commit**

```bash
git add src/orcapod/pipeline/base.py src/orcapod/pipeline/job.py \
        src/orcapod/pipeline/sync_orchestrator.py src/orcapod/pipeline/async_orchestrator.py \
        src/orcapod/core/tracker.py
git commit -m "feat(side-effects): wire SideEffectJobNode into pipeline compilation and orchestrators"
```

---

### Task 6: Error handling tests (T8–T10)

**Files:**
- Test: `tests/test_core/side_effect_pod/test_side_effect_pod.py`

- [ ] **Step 1: Write tests T8–T10**

Append to test file:

```python
class TestSideEffectPodErrorHandling:
    """T8–T10: on_error and drop_on_failure behavior."""

    def test_t8_on_error_raise_propagates(self):
        """T8: on_error='raise' — exception propagates to caller."""
        from orcapod.side_effects import SideEffectPodConfig, side_effect_pod

        @side_effect_pod(config=SideEffectPodConfig(on_error="raise", drop_on_failure=True))
        def bad_fn(data, ctx):
            raise ValueError("boom")

        with pytest.raises(ValueError, match="boom"):
            list(_make_stream(2).pipe(bad_fn).iter_data())

    def test_t9_on_error_log_drop_on_failure(self, caplog):
        """T9: on_error='log' + drop_on_failure=True — row dropped, warning logged."""
        import logging
        from orcapod.side_effects import SideEffectPodConfig, side_effect_pod

        @side_effect_pod(
            config=SideEffectPodConfig(on_error="log", drop_on_failure=True, track_completion=False)
        )
        def bad_fn(data, ctx):
            raise ValueError("oops")

        with caplog.at_level(logging.WARNING):
            rows = list(_make_stream(2).pipe(bad_fn).iter_data())

        assert len(rows) == 0
        assert any("oops" in r.message or "delivery failed" in r.message for r in caplog.records)

    def test_t10_on_error_log_pass_through(self, caplog):
        """T10: on_error='log' + drop_on_failure=False — row emitted despite error."""
        import logging
        from orcapod.side_effects import SideEffectPodConfig, side_effect_pod

        @side_effect_pod(
            config=SideEffectPodConfig(on_error="log", drop_on_failure=False, track_completion=False)
        )
        def bad_fn(data, ctx):
            raise RuntimeError("failing")

        with caplog.at_level(logging.WARNING):
            rows = list(_make_stream(2).pipe(bad_fn).iter_data())

        assert len(rows) == 2
        assert any("failing" in r.message or "delivery failed" in r.message for r in caplog.records)
```

- [ ] **Step 2: Run tests**

```bash
uv run pytest tests/test_core/side_effect_pod/test_side_effect_pod.py::TestSideEffectPodErrorHandling -v
```

Expected: PASS (the implementation already handles these cases).

- [ ] **Step 3: Commit**

```bash
git add tests/test_core/side_effect_pod/test_side_effect_pod.py
git commit -m "test(side-effects): add T8-T10 error handling tests"
```

---

### Task 7: `invocation_hash` determinism + `format_id` override (T11–T12)

**Files:**
- Test: `tests/test_core/side_effect_pod/test_side_effect_pod.py`

- [ ] **Step 1: Write tests T11–T12**

Append to test file:

```python
class TestInvocationHashDetails:
    """T11–T12: hash determinism and format_id override."""

    def test_t11_invocation_hash_determinism(self):
        """T11: same inputs + same code → identical invocation_hash on two runs."""
        from orcapod.side_effects import side_effect_pod

        hashes_run1 = []
        hashes_run2 = []

        @side_effect_pod
        def capture(data, ctx):
            pass

        # We need two separate instances with same fn to check determinism
        # Run 1
        for _, ctx in _collect_contexts(_make_stream(2), capture):
            hashes_run1.append(ctx.invocation_hash)

        # Run 2 (same stream)
        for _, ctx in _collect_contexts(_make_stream(2), capture):
            hashes_run2.append(ctx.invocation_hash)

        assert hashes_run1 == hashes_run2

    def test_t12_format_id_base64_override(self):
        """T12: format_id(InvocationHashConfig(encoding='base64', component_length=8)) returns valid base64."""
        import base64
        from orcapod.side_effects import InvocationHashConfig, side_effect_pod

        contexts = []

        @side_effect_pod
        def capture(data, ctx):
            contexts.append(ctx)

        list(_make_stream(1).pipe(capture).iter_data())
        ctx = contexts[0]
        override = InvocationHashConfig(encoding="base64", component_length=8)
        fid = ctx.format_id(override)
        assert fid.startswith("orcapod-")
        # Extract the hash part and split on ::
        hash_part = fid[len("orcapod-"):]
        components = hash_part.split("::")
        assert len(components) >= 2
        # Each component should be valid base64 of 8-byte input → 12 chars
        for c in components[:2]:
            decoded = base64.b64decode(c + "==")  # pad for safety
            assert len(decoded) == 8


def _collect_contexts(stream, pod):
    """Helper: run pod over stream and collect (ctx) for each row."""
    ctxs = []

    from orcapod.side_effects import SideEffectPod, SideEffectPodConfig, side_effect_pod

    captured = []

    @side_effect_pod
    def _cap(data, ctx):
        captured.append(ctx)

    # Re-use the pod's pipeline_hash by running it
    # Actually we need the original pod to keep the same pipeline_hash
    # Patch: run pod but intercept ctx
    rows = []
    for tag, data in stream.iter_data():
        rows.append((tag, data))

    return [(None, ctx) for ctx in captured]
```

Actually the helper is overly complex. Simplify T11 to use a straightforward approach:

```python
class TestInvocationHashDetails:
    """T11–T12: hash determinism and format_id override."""

    def test_t11_invocation_hash_determinism(self):
        """T11: identical inputs → identical invocation_hash across two iterations."""
        from orcapod.side_effects import side_effect_pod

        hashes_a: list[str] = []
        hashes_b: list[str] = []

        @side_effect_pod
        def capture_a(data, ctx):
            hashes_a.append(ctx.invocation_hash)

        @side_effect_pod
        def capture_b(data, ctx):
            hashes_b.append(ctx.invocation_hash)

        # Both pods wrap the same-named function, but different objects.
        # What we actually test: running the *same pod* twice over identical data
        # yields identical hashes. Use a single pod and two identical streams.
        run1_hashes: list[str] = []
        run2_hashes: list[str] = []

        @side_effect_pod
        def record(data, ctx):
            pass

        # Run over stream 1
        from orcapod.side_effects import SideEffectPodStream
        stream1 = _make_stream(2)
        ctxs1 = []
        @side_effect_pod
        def c1(data, ctx):
            ctxs1.append(ctx.invocation_hash)
        list(stream1.pipe(c1).iter_data())

        stream2 = _make_stream(2)
        ctxs2 = []
        @side_effect_pod
        def c2(data, ctx):
            ctxs2.append(ctx.invocation_hash)
        list(stream2.pipe(c2).iter_data())

        # c1 and c2 are different pod objects (different function identity) so hashes differ.
        # Instead test that running c1 twice over same data gives same hash:
        ctxs_r1: list[str] = []
        ctxs_r2: list[str] = []

        @side_effect_pod
        def stable(data, ctx):
            pass  # capture nothing — we call format_id later

        # Capture via a wrapper
        captured: list[str] = []

        @side_effect_pod
        def recorder(data, ctx):
            captured.append(ctx.invocation_hash)

        list(_make_stream(2).pipe(recorder).iter_data())
        first_run = list(captured)
        captured.clear()
        list(_make_stream(2).pipe(recorder).iter_data())
        second_run = list(captured)

        assert first_run == second_run

    def test_t12_format_id_base64_override(self):
        """T12: format_id with base64 + component_length=8 returns 'orcapod-<b64>::<b64>'."""
        import base64
        from orcapod.side_effects import InvocationHashConfig, side_effect_pod

        captured_ctx = []

        @side_effect_pod
        def rec(data, ctx):
            captured_ctx.append(ctx)

        list(_make_stream(1).pipe(rec).iter_data())
        ctx = captured_ctx[0]
        override = InvocationHashConfig(encoding="base64", component_length=8)
        fid = ctx.format_id(override)
        assert fid.startswith("orcapod-")
        hash_part = fid[len("orcapod-"):]
        components = hash_part.split("::")
        assert len(components) >= 2
        for c in components[:2]:
            raw = base64.b64decode(c)
            assert len(raw) == 8
```

- [ ] **Step 2: Run tests**

```bash
uv run pytest tests/test_core/side_effect_pod/test_side_effect_pod.py::TestInvocationHashDetails -v
```

Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/test_core/side_effect_pod/test_side_effect_pod.py
git commit -m "test(side-effects): add T11-T12 hash determinism and format_id override tests"
```

---

### Task 8: Decorator tests (T16–T18) + `@sink_pod`, `@tap_pod`, `@side_effect_pod` implementations

**Files:**
- Modify: `src/orcapod/side_effects.py` (append decorators)
- Test: `tests/test_core/side_effect_pod/test_side_effect_pod.py`

- [ ] **Step 1: Write failing decorator tests (T16–T18)**

Append to test file:

```python
class TestDecorators:
    """T16–T18: @sink_pod, @tap_pod, @side_effect_pod shortcuts."""

    def test_t16_sink_pod_defaults(self):
        """T16: @sink_pod → track_completion=True, drop_on_failure=True."""
        from orcapod.side_effects import SideEffectPod, sink_pod

        @sink_pod
        def fn(data, ctx):
            pass

        assert isinstance(fn, SideEffectPod)
        assert fn.pod_config.track_completion is True
        assert fn.pod_config.drop_on_failure is True

    def test_t17_tap_pod_defaults(self):
        """T17: @tap_pod → track_completion=False, drop_on_failure=False."""
        from orcapod.side_effects import SideEffectPod, tap_pod

        @tap_pod
        def fn(data, ctx):
            pass

        assert isinstance(fn, SideEffectPod)
        assert fn.pod_config.track_completion is False
        assert fn.pod_config.drop_on_failure is False

    def test_t18_side_effect_pod_all_four_combinations(self):
        """T18: @side_effect_pod(config=...) with all four track/drop combos."""
        from orcapod.side_effects import SideEffectPod, SideEffectPodConfig, side_effect_pod

        for track in (True, False):
            for drop in (True, False):
                @side_effect_pod(config=SideEffectPodConfig(
                    track_completion=track, drop_on_failure=drop
                ))
                def fn(data, ctx):
                    pass

                assert isinstance(fn, SideEffectPod)
                assert fn.pod_config.track_completion is track
                assert fn.pod_config.drop_on_failure is drop

    def test_sink_pod_with_config_override(self):
        """@sink_pod(config=...) overrides defaults with caller-supplied config."""
        from orcapod.side_effects import SideEffectPodConfig, sink_pod

        @sink_pod(config=SideEffectPodConfig(on_error="log"))
        def fn(data, ctx):
            pass

        assert fn.pod_config.on_error == "log"
        # Presets from sink_pod preserved
        assert fn.pod_config.track_completion is True
        assert fn.pod_config.drop_on_failure is True
```

- [ ] **Step 2: Run to confirm tests fail**

```bash
uv run pytest tests/test_core/side_effect_pod/test_side_effect_pod.py::TestDecorators -v
```

Expected: FAIL (`sink_pod`, `tap_pod` not yet defined).

- [ ] **Step 3: Append decorators to `side_effects.py`**

```python
# ---------------------------------------------------------------------------
# Decorators
# ---------------------------------------------------------------------------


def side_effect_pod(
    fn: Callable | None = None,
    *,
    config: SideEffectPodConfig | None = None,
) -> SideEffectPod | Callable[[Callable], SideEffectPod]:
    """Decorator that wraps a function as a ``SideEffectPod``.

    Supports both bare usage (``@side_effect_pod``) and parameterised usage
    (``@side_effect_pod(config=...)``).

    Args:
        fn: The user function to wrap, when used as a bare decorator.
        config: Optional ``SideEffectPodConfig``. Defaults to
            ``SideEffectPodConfig()`` (all defaults).

    Returns:
        A ``SideEffectPod`` when ``fn`` is provided, otherwise a decorator
        that returns a ``SideEffectPod``.
    """
    if fn is not None:
        # Bare @side_effect_pod usage
        return SideEffectPod(fn=fn, config=config)

    def decorator(f: Callable) -> SideEffectPod:
        return SideEffectPod(fn=f, config=config)

    return decorator


def sink_pod(
    fn: Callable | None = None,
    *,
    config: SideEffectPodConfig | None = None,
) -> SideEffectPod | Callable[[Callable], SideEffectPod]:
    """Decorator preset: ``track_completion=True``, ``drop_on_failure=True``.

    Suitable for write-once sinks where duplicate deliveries should be
    skipped and failed deliveries should be silently dropped from output.

    Args:
        fn: The user function to wrap, when used as a bare decorator.
        config: Optional ``SideEffectPodConfig`` to override specific fields.
            Fields not present take their ``SideEffectPodConfig`` defaults,
            except ``track_completion`` and ``drop_on_failure`` which are
            preset to ``True``.

    Returns:
        A ``SideEffectPod`` or decorator.
    """
    preset = SideEffectPodConfig(track_completion=True, drop_on_failure=True)
    if config is not None:
        # Merge: caller's config takes precedence
        preset = dataclasses.replace(
            preset,
            on_error=config.on_error,
            hash_config=config.hash_config,
        )

    if fn is not None:
        return SideEffectPod(fn=fn, config=preset)

    def decorator(f: Callable) -> SideEffectPod:
        return SideEffectPod(fn=f, config=preset)

    return decorator


def tap_pod(
    fn: Callable | None = None,
    *,
    config: SideEffectPodConfig | None = None,
) -> SideEffectPod | Callable[[Callable], SideEffectPod]:
    """Decorator preset: ``track_completion=False``, ``drop_on_failure=False``.

    Suitable for observability taps where every invocation should fire and
    failures should not affect downstream data.

    Args:
        fn: The user function to wrap, when used as a bare decorator.
        config: Optional ``SideEffectPodConfig`` to override specific fields.
            Fields not present take their ``SideEffectPodConfig`` defaults,
            except ``track_completion`` and ``drop_on_failure`` which are
            preset to ``False``.

    Returns:
        A ``SideEffectPod`` or decorator.
    """
    preset = SideEffectPodConfig(track_completion=False, drop_on_failure=False)
    if config is not None:
        preset = dataclasses.replace(
            preset,
            on_error=config.on_error,
            hash_config=config.hash_config,
        )

    if fn is not None:
        return SideEffectPod(fn=fn, config=preset)

    def decorator(f: Callable) -> SideEffectPod:
        return SideEffectPod(fn=f, config=preset)

    return decorator
```

- [ ] **Step 4: Run decorator tests**

```bash
uv run pytest tests/test_core/side_effect_pod/test_side_effect_pod.py::TestDecorators -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/side_effects.py tests/test_core/side_effect_pod/test_side_effect_pod.py
git commit -m "feat(side-effects): add side_effect_pod, sink_pod, tap_pod decorators"
```

---

### Task 9: Async + pipeline composition tests (T13–T15) + re-exports

**Files:**
- Test: `tests/test_core/side_effect_pod/test_side_effect_pod.py`
- Modify: `src/orcapod/__init__.py`

- [ ] **Step 1: Write tests T13–T15**

Append to test file:

```python
class TestAsyncAndComposition:
    """T13–T15: async_execute, parallel execution, pipeline composition."""

    def test_t13_async_execute(self):
        """T13: Pod runs correctly via async_execute channel path."""
        import asyncio
        from orcapod.pipeline.job import PipelineJob
        from orcapod.side_effects import sink_pod

        db = _make_in_memory_db()
        calls = []

        @sink_pod
        def record(data, ctx):
            calls.append(ctx.invocation_hash)

        stream = _make_stream(3)

        with PipelineJob(database=db) as job:
            out = record(stream)
            asyncio.run(job.async_run(out))

        assert len(calls) == 3
        pipeline_hash = record.pipeline_hash().to_string()
        table = db.at(pipeline_hash, "side_effect_invocations").read()
        assert len(table) == 3

    def test_t15_pipeline_composition(self):
        """T15: SideEffectPod mid-pipeline — downstream pod receives filtered output."""
        from orcapod.pipeline.job import PipelineJob
        from orcapod.core.function_pod import FunctionPod
        from orcapod.side_effects import SideEffectPodConfig, side_effect_pod

        db = _make_in_memory_db()
        side_calls = []
        downstream_rows = []

        @side_effect_pod(config=SideEffectPodConfig(
            track_completion=False, drop_on_failure=True
        ))
        def tap(data, ctx):
            side_calls.append(1)

        @FunctionPod
        def double(data):
            return {"value": data["value"] * 2}

        stream = _make_stream(3)
        with PipelineJob(database=db) as job:
            tapped = tap(stream)
            result = job.run(double(tapped))

        assert len(side_calls) == 3
        # Downstream double pod sees 3 rows
        for tag, data in result:
            downstream_rows.append(data["value"])
        assert len(downstream_rows) == 3


class TestReexports:
    """Verify public types are importable from orcapod top-level."""

    def test_imports_from_orcapod(self):
        from orcapod import (
            InvocationContext,
            InvocationHashConfig,
            SideEffectPod,
            SideEffectPodConfig,
            side_effect_pod,
            sink_pod,
            tap_pod,
        )
        assert InvocationHashConfig is not None
        assert SideEffectPodConfig is not None
        assert SideEffectPod is not None
```

- [ ] **Step 2: Add re-exports to `src/orcapod/__init__.py`**

Read current `__init__.py` to find the right insertion point, then add:

```python
from orcapod.side_effects import (
    InvocationContext,
    InvocationHashConfig,
    SideEffectPod,
    SideEffectPodConfig,
    side_effect_pod,
    sink_pod,
    tap_pod,
)
```

- [ ] **Step 3: Run T13–T15 and re-export tests**

```bash
uv run pytest tests/test_core/side_effect_pod/test_side_effect_pod.py::TestAsyncAndComposition tests/test_core/side_effect_pod/test_side_effect_pod.py::TestReexports -v
```

Expected: PASS.

- [ ] **Step 4: Run full test suite**

```bash
uv run pytest tests/ -x -q
```

Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add tests/test_core/side_effect_pod/test_side_effect_pod.py src/orcapod/__init__.py
git commit -m "feat(side-effects): add re-exports; test async, composition, re-exports (T13-T15)"
```

---

### Task 10: Documentation (`docs/concepts/side-effect-pods.md`)

**Files:**
- Create: `docs/concepts/side-effect-pods.md`

- [ ] **Step 1: Create concept doc**

Write `docs/concepts/side-effect-pods.md` per the spec outline (what they are, the 4 combinations, `InvocationContext`, `InvocationHashConfig`, usage examples, reverse-lookup walk-through, log growth guidance, hash rotation).

- [ ] **Step 2: Verify full test suite still passes**

```bash
uv run pytest tests/ -q
```

Expected: all tests pass.

- [ ] **Step 3: Final commit**

```bash
git add docs/concepts/side-effect-pods.md
git commit -m "docs(side-effects): add side-effect-pods concept documentation"
```
