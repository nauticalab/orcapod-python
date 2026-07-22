# SideEffectFunctionPod Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement `SideEffectFunctionPod/Node` — a hybrid pod wrapping `(ctx: InvocationContext, arg1, ...) -> OutputData`, producing a downstream data stream while receiving per-row `InvocationContext`. Includes a breaking change to `SideEffectPod`'s callable contract.

**Architecture:** `SideEffectFunctionPod` is a `TraceableBase` that strips the `ctx` parameter from function signature for schema inference (via `_strip_ctx_from_fn`), passes data fields as keyword arguments (like `FunctionPod`), and injects `InvocationContext` per-row. Pipeline integration mirrors `FunctionPod` (records invocations, compiles to `SideEffectFunctionJobNode`) while execution mirrors `SideEffectPod` (same preimage construction, invocation logging via `_write_invocation_row`).

**Tech Stack:** Python 3.11+, PyArrow, orcapod core (TraceableBase, StreamBase, ResultCache, InvocationContext, _write_invocation_row), uuid_utils, uv, pytest.

---

## File Map

| File | Change |
|------|--------|
| `src/orcapod/side_effects.py` | Modify — add `ctx_arg_name`, change call style to `**kwargs` |
| `src/orcapod/core/side_effect_function/__init__.py` | New |
| `src/orcapod/core/side_effect_function/side_effect_function_pod.py` | New — all classes + decorator |
| `src/orcapod/protocols/core_protocols/side_effect_function_pod.py` | New — protocol |
| `src/orcapod/protocols/core_protocols/__init__.py` | Modify — export |
| `src/orcapod/protocols/core_protocols/trackers.py` | Modify — new method |
| `src/orcapod/core/tracker.py` | Modify — new method |
| `src/orcapod/pipeline/pod_invocation.py` | Modify — new invocation type |
| `src/orcapod/pipeline/base.py` | Modify — recorder + compile + to_invocations |
| `src/orcapod/protocols/node_protocols.py` | Modify — new protocol + TypeGuard |
| `src/orcapod/pipeline/sync_orchestrator.py` | Modify — new elif branch |
| `src/orcapod/pipeline/async_orchestrator.py` | Modify — new elif branch |
| `src/orcapod/pipeline/graph.py` | Modify — class attribute |
| `src/orcapod/pipeline/job.py` | Modify — class attr + _distribute_databases + as_pipeline |
| `src/orcapod/__init__.py` | Modify — re-exports |
| `tests/test_core/side_effect_pod/test_side_effect_pod.py` | Modify — update for new call style |
| `tests/test_core/side_effect_function/__init__.py` | New |
| `tests/test_core/side_effect_function/test_side_effect_function_pod.py` | New — SF-01–SF-13 |

---

### Task 1: SideEffectPod Breaking Change — `ctx_arg_name` + New Call Style

Update `side_effects.py` to add `ctx_arg_name: str = "ctx"` and change the user function call from `fn(data, ctx)` to `fn(**{ctx_arg_name: ctx, **data.as_dict()})`. Update all existing tests to match.

**Files:**
- Modify: `src/orcapod/side_effects.py`
- Modify: `tests/test_core/side_effect_pod/test_side_effect_pod.py`

- [ ] **Step 1: Update all existing test functions from `fn(data, ctx)` to keyword-arg style**

The test stream `_make_stream(n)` has one data column `value: int64`. After the change, `fn` is called as `fn(ctx=ctx, value=42)` (via `fn(**{ctx_arg_name: ctx, **data.as_dict()})`). Update every `def fn(data, ctx)` pattern in the test file:

```python
# BEFORE — every test function using the old call style:
def fn(data, ctx):
    calls.append(dict(data))

# AFTER — receives individual keyword arguments:
def fn(value, ctx):
    calls.append({"value": value})

# BEFORE — pass-through ignore ctx:
def fn(data, _ctx):
    calls.append(True)

# AFTER:
def fn(value, _ctx):
    calls.append(True)

# BEFORE — raise inside fn:
def fn(data, ctx):
    raise RuntimeError("boom")

# AFTER:
def fn(value, ctx):
    raise RuntimeError("boom")

# BEFORE — T3 receive ctx:
def fn(data, ctx):
    received.append(ctx)

# AFTER:
def fn(value, ctx):
    received.append(ctx)
```

Apply this transformation to ALL test methods that define inline `fn` callables: `test_t1_*`, `test_t2_*`, `test_t3_*`, `test_t4_*`, `test_t8_*`, `test_t9_*`, `test_t10_*`, `test_t11_*`, `test_t12_*`, `test_t13_*`, `test_t14_*`, `test_t15_*`, and all decorator tests (T16-T18). Also update `test_failure_does_not_write_log_*` and `test_fail_then_succeed_then_skip` (their `fn(data, ctx)` patterns). For `_make_node_with_db` helper, update its inner `fn` too.

Also update `TestSideEffectInvocation.test_construction`: change `def fn(data, ctx): pass` → `def fn(value, ctx): pass`.

Add two new test classes at the bottom of the file for the new call style and `ctx_arg_name`:

```python
class TestSideEffectPodNewCallStyle:
    """SEP-UPDATE: Data fields passed as kwargs, not as DataProtocol."""

    def test_sep_update_kwargs_passed_to_fn(self):
        """Data fields unpacked as kwargs; ctx passed by ctx_arg_name."""
        from orcapod.side_effects import SideEffectPod

        received: dict = {}

        def fn(value, ctx):
            received["value"] = value
            received["ctx"] = ctx

        pod = SideEffectPod(fn)
        stream = _make_stream(1)  # stream has 'value' data column (int64)
        list(pod.process(stream).iter_data())

        assert "value" in received
        assert isinstance(received["value"], int)
        assert received["ctx"] is not None


class TestSideEffectPodCtxArgName:
    """SEP-CTX-NAME: ctx_arg_name routes InvocationContext to correct parameter."""

    def test_sep_ctx_name_custom(self):
        """ctx_arg_name='context' injects InvocationContext under that name."""
        from orcapod.side_effects import SideEffectPod, InvocationContext

        received: dict = {}

        def fn(value, context):
            received["context"] = context

        pod = SideEffectPod(fn, ctx_arg_name="context")
        stream = _make_stream(1)
        list(pod.process(stream).iter_data())

        assert isinstance(received["context"], InvocationContext)
```

- [ ] **Step 2: Run tests to confirm they fail (tests reference old call style)**

```bash
uv run pytest tests/test_core/side_effect_pod/test_side_effect_pod.py -x -q 2>&1 | head -40
```

Expected: `TypeError` — existing functions like `fn(data, ctx)` cannot receive `value=...` as a keyword argument.

- [ ] **Step 3: Add `ctx_arg_name` parameter to `SideEffectPod.__init__`**

In `src/orcapod/side_effects.py`, update `SideEffectPod.__init__`:

```python
def __init__(
    self,
    fn: Callable,
    config: SideEffectPodConfig | None = None,
    tracker_manager: TrackerManagerProtocol | None = None,
    name: str | None = None,
    label: str | None = None,
    data_context: Any = None,
    ctx_arg_name: str = "ctx",          # NEW
) -> None:
    super().__init__(label=label, data_context=data_context)
    self._fn = fn
    self._name: str = name if name is not None else getattr(fn, "__name__", "unknown")
    self._pod_config = config or SideEffectPodConfig()
    self.tracker_manager = tracker_manager or DEFAULT_TRACKER_MANAGER
    self._ctx_arg_name: str = ctx_arg_name   # NEW
```

- [ ] **Step 4: Update `SideEffectPod.identity_structure()` to include `ctx_arg_name`**

```python
def identity_structure(self) -> Any:
    return (
        self.uri,
        self._ctx_arg_name,                     # NEW — renaming ctx param changes hash
        self._pod_config.track_completion,
        self._pod_config.drop_on_failure,
    )
```

- [ ] **Step 5: Update `_execute_side_effect_row` — add `ctx_arg_name` param, change fn call**

Add `ctx_arg_name: str = "ctx"` to the function signature (after `arrow_hasher`):

```python
def _execute_side_effect_row(
    *,
    fn: Callable,
    tag: TagProtocol,
    data: DataProtocol,
    pod_config: SideEffectPodConfig,
    pipeline_hash_ch: ContentHash,
    node_content_hash_str: str,
    pod_name: str,
    run_id: str | None,
    arrow_hasher: Any,
    ctx_arg_name: str = "ctx",            # NEW
    pipeline_database: ArrowDatabaseProtocol | None = None,
    table_path: tuple[str, ...] | None = None,
) -> tuple[TagProtocol, DataProtocol] | None:
```

Change the fn call in step 5 of the function body from:
```python
fn(data, ctx)
```
to:
```python
fn(**{ctx_arg_name: ctx, **data.as_dict()})
```

- [ ] **Step 6: Pass `ctx_arg_name` in all four callers of `_execute_side_effect_row`**

```python
# 1. SideEffectPodStream.iter_data():
result = _execute_side_effect_row(
    fn=self._pod._fn,
    tag=tag,
    data=data,
    pod_config=self._pod.pod_config,
    pipeline_hash_ch=self.pipeline_hash(),
    node_content_hash_str=self._pod.content_hash().to_string(),
    pod_name=self._pod.label,
    run_id=None,
    arrow_hasher=self._pod.data_context.arrow_hasher,
    ctx_arg_name=self._pod._ctx_arg_name,       # NEW
)

# 2. SideEffectNode.iter_data():
result = _execute_side_effect_row(
    fn=self._pod._fn,
    tag=tag,
    data=data,
    pod_config=self._pod.pod_config,
    pipeline_hash_ch=self.pipeline_hash(),
    node_content_hash_str=self._pod.content_hash().to_string(),
    pod_name=self._pod.label,
    run_id=None,
    arrow_hasher=self._pod.data_context.arrow_hasher,
    ctx_arg_name=self._pod._ctx_arg_name,       # NEW
    pipeline_database=None,
    table_path=None,
)

# 3. SideEffectJobNode.execute():
result = _execute_side_effect_row(
    fn=self._pod._fn,
    tag=tag,
    data=data,
    pod_config=self._pod.pod_config,
    pipeline_hash_ch=self.pipeline_hash(),
    node_content_hash_str=self._pod.content_hash().to_string(),
    pod_name=self._pod.label,
    run_id=run_id,
    arrow_hasher=self._pod.data_context.arrow_hasher,
    ctx_arg_name=self._pod._ctx_arg_name,       # NEW
    pipeline_database=self._pipeline_database,
    table_path=self._table_path,
)

# 4. SideEffectJobNode.async_execute() — process_one inner function:
result = _execute_side_effect_row(
    fn=self._pod._fn,
    tag=tag,
    data=data,
    pod_config=self._pod.pod_config,
    pipeline_hash_ch=self.pipeline_hash(),
    node_content_hash_str=self._pod.content_hash().to_string(),
    pod_name=self._pod.label,
    run_id=run_id,
    arrow_hasher=self._pod.data_context.arrow_hasher,
    ctx_arg_name=self._pod._ctx_arg_name,       # NEW
    pipeline_database=self._pipeline_database,
    table_path=self._table_path,
)
```

- [ ] **Step 7: Add `ctx_arg_name` to the three decorators**

```python
def side_effect_pod(
    fn: Callable | None = None,
    *,
    config: SideEffectPodConfig | None = None,
    name: str | None = None,
    ctx_arg_name: str = "ctx",           # NEW
) -> SideEffectPod | Callable[[Callable], SideEffectPod]:
    def _wrap(f: Callable) -> SideEffectPod:
        return SideEffectPod(f, config=config, name=name, ctx_arg_name=ctx_arg_name)
    if fn is not None:
        return _wrap(fn)
    return _wrap

# Apply the same pattern to sink_pod and tap_pod.
```

- [ ] **Step 8: Run tests to confirm all pass**

```bash
uv run pytest tests/test_core/side_effect_pod/test_side_effect_pod.py -v
```

Expected: all tests pass including the two new `TestSideEffectPodNewCallStyle` and `TestSideEffectPodCtxArgName` tests.

- [ ] **Step 9: Run full test suite to confirm no regressions**

```bash
uv run pytest tests/ -x -q --tb=short 2>&1 | tail -10
```

- [ ] **Step 10: Commit**

```bash
git add src/orcapod/side_effects.py tests/test_core/side_effect_pod/test_side_effect_pod.py
git commit -m "feat(side_effects): add ctx_arg_name param, change call style to **kwargs (breaking)"
```

---

### Task 2: New Module Scaffold + `SideEffectFunctionPod` Core Class (SF-01–SF-03, SF-10)

Create `src/orcapod/core/side_effect_function/` with all classes as stubs, implement the `SideEffectFunctionPod` constructor including `_strip_ctx_from_fn`, schema extraction, and URI.

**Files:**
- Create: `src/orcapod/core/side_effect_function/__init__.py`
- Create: `src/orcapod/core/side_effect_function/side_effect_function_pod.py`
- Create: `tests/test_core/side_effect_function/__init__.py`
- Create: `tests/test_core/side_effect_function/test_side_effect_function_pod.py` (SF-01–SF-03, SF-10)

- [ ] **Step 1: Create test file with SF-01, SF-02, SF-03, SF-10**

Create `tests/test_core/side_effect_function/__init__.py` (empty).

Create `tests/test_core/side_effect_function/test_side_effect_function_pod.py`:

```python
# tests/test_core/side_effect_function/test_side_effect_function_pod.py
from __future__ import annotations

import pytest
import pyarrow as pa

from orcapod.core.streams import ArrowTableStream
from orcapod.side_effects import InvocationContext


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


def _make_in_memory_db():
    """Return a fresh in-memory ArrowDatabase."""
    from orcapod.databases.in_memory_databases import InMemoryArrowDatabase
    return InMemoryArrowDatabase()


class TestSideEffectFunctionPodSchema:
    """SF-01, SF-02, SF-03, SF-10: schema inference and ctx stripping."""

    def test_sf01_ctx_stripped_from_input_schema(self):
        """SF-01: 'ctx' param stripped; data params form the input schema."""
        from orcapod.core.side_effect_function import SideEffectFunctionPod

        def my_fn(value: int, ctx: InvocationContext) -> str:
            return f"result_{value}"

        pod = SideEffectFunctionPod(my_fn, output_keys=["result"])

        # Input schema excludes 'ctx'
        assert "ctx" not in pod.input_data_schema
        assert "value" in pod.input_data_schema
        assert pod.input_data_schema["value"] == int

        # Output schema has the declared key
        assert "result" in pod.output_data_schema
        assert pod.output_data_schema["result"] == str

    def test_sf02_custom_ctx_arg_name(self):
        """SF-02: ctx_arg_name='context' — stripped and injected by correct name."""
        from orcapod.core.side_effect_function import SideEffectFunctionPod

        def my_fn(value: int, context: InvocationContext) -> str:
            return f"r_{value}"

        pod = SideEffectFunctionPod(my_fn, output_keys=["result"], ctx_arg_name="context")
        assert "context" not in pod.input_data_schema
        assert "value" in pod.input_data_schema

    def test_sf03_missing_ctx_arg_raises_at_construction(self):
        """SF-03: Missing ctx_arg_name raises ValueError at construction time."""
        from orcapod.core.side_effect_function import SideEffectFunctionPod

        def my_fn(value: int) -> str:
            return str(value)

        with pytest.raises(ValueError, match="ctx_arg_name"):
            SideEffectFunctionPod(my_fn, output_keys=["result"])
            # Default ctx_arg_name="ctx" is missing from my_fn's signature

    def test_sf10_node_uri_shape(self):
        """SF-10: node_uri starts with 'side_effect_function' and has 5 elements."""
        from orcapod.core.side_effect_function import SideEffectFunctionPod

        def my_fn(value: int, ctx: InvocationContext) -> str:
            return str(value)

        pod = SideEffectFunctionPod(my_fn, output_keys=["result"])
        assert pod.uri[0] == "side_effect_function"
        assert pod.uri[-1] == "python_side_effect_function"
        assert len(pod.uri) == 5
        assert pod.uri[3] == "v1"
```

- [ ] **Step 2: Run tests to confirm they fail with ImportError**

```bash
uv run pytest tests/test_core/side_effect_function/test_side_effect_function_pod.py::TestSideEffectFunctionPodSchema -x -q 2>&1 | head -20
```

Expected: `ImportError` — module does not exist yet.

- [ ] **Step 3: Create `src/orcapod/core/side_effect_function/__init__.py`**

```python
"""SideEffectFunctionPod — function pod with per-row InvocationContext."""
from orcapod.core.side_effect_function.side_effect_function_pod import (
    SideEffectFunctionPod,
    SideEffectFunctionPodStream,
    SideEffectFunctionNode,
    SideEffectFunctionJobNode,
    side_effect_function_pod,
)

__all__ = [
    "SideEffectFunctionPod",
    "SideEffectFunctionPodStream",
    "SideEffectFunctionNode",
    "SideEffectFunctionJobNode",
    "side_effect_function_pod",
]
```

- [ ] **Step 4: Create `side_effect_function_pod.py` — implement `_strip_ctx_from_fn` and `SideEffectFunctionPod`**

Create `src/orcapod/core/side_effect_function/side_effect_function_pod.py`:

```python
"""SideEffectFunctionPod — hybrid of FunctionPod and SideEffectPod."""
from __future__ import annotations

import asyncio
import functools
import inspect
import logging
import sys
import uuid
from collections.abc import Callable, Collection, Iterator, Sequence
from typing import TYPE_CHECKING, Any

from uuid_utils import uuid7

from orcapod.core.base import TraceableBase
from orcapod.core.streams.base import StreamBase
from orcapod.core.tracker import DEFAULT_TRACKER_MANAGER
from orcapod.errors import PipelineJobRequiredError
from orcapod.side_effects import (
    InvocationContext,
    SideEffectPodConfig,
    _SIDE_EFFECT_RECOMPUTATION_INDEX_COL,
    _write_invocation_row,
)
from orcapod.utils.lazy_module import LazyModule
from orcapod.utils.schema_utils import extract_function_schemas

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
# _strip_ctx_from_fn — remove ctx param from signature for schema inference
# ---------------------------------------------------------------------------


def _strip_ctx_from_fn(fn: Callable, ctx_arg_name: str) -> Callable:
    """Return a wrapper of ``fn`` with ``ctx_arg_name`` removed from signature.

    The wrapper is passed to ``extract_function_schemas`` so the context
    parameter is transparent to schema inference. The original ``fn`` is used
    for actual calls and content hashing.

    Args:
        fn: The original user function.
        ctx_arg_name: Name of the parameter receiving ``InvocationContext``.

    Returns:
        A wrapper whose ``__signature__`` and ``__annotations__`` exclude
        ``ctx_arg_name``.

    Raises:
        ValueError: If ``ctx_arg_name`` is not in ``fn``'s signature.
    """
    sig = inspect.signature(fn)
    if ctx_arg_name not in sig.parameters:
        raise ValueError(
            f"ctx_arg_name {ctx_arg_name!r} not found in function signature "
            f"{fn.__name__!r}. Available parameters: {list(sig.parameters)}"
        )
    new_params = [p for n, p in sig.parameters.items() if n != ctx_arg_name]
    new_sig = sig.replace(parameters=new_params)

    @functools.wraps(fn)
    def wrapper(*args, **kwargs):  # pragma: no cover
        return fn(*args, **kwargs)

    wrapper.__signature__ = new_sig  # type: ignore[attr-defined]
    wrapper.__annotations__ = {
        k: v for k, v in fn.__annotations__.items() if k != ctx_arg_name
    }
    return wrapper


# ---------------------------------------------------------------------------
# _build_ctx_and_record_id — shared preimage helper
# ---------------------------------------------------------------------------


def _build_ctx_and_record_id(
    *,
    pod: "SideEffectFunctionPod",
    tag: "TagProtocol",
    data: "DataProtocol",
    pipeline_hash_ch: "ContentHash",
    run_id: str | None,
) -> "tuple[InvocationContext, ContentHash, bytes]":
    """Build InvocationContext + record ID for one (tag, data) row.

    Uses the same preimage as ``_execute_side_effect_row`` in
    ``side_effects.py``: system-tag columns + INPUT_DATA_HASH_COL +
    NODE_CONTENT_HASH_COL + recomputation index 0.

    Args:
        pod: The invoking pod.
        tag: Tag for this row.
        data: Data for this row.
        pipeline_hash_ch: Pipeline hash of the node.
        run_id: Pipeline run identifier, or ``None`` in standalone mode.

    Returns:
        ``(ctx, record_id_hash, record_id)`` where ``record_id`` is the
        prefixed digest of ``record_id_hash``.
    """
    from orcapod.system_constants import constants

    preimage = (
        tag.as_table(columns={"system_tags": True})
        .append_column(
            constants.INPUT_DATA_HASH_COL,
            pa.array([data.content_hash().to_string()], type=pa.large_string()),
        )
        .append_column(
            constants.NODE_CONTENT_HASH_COL,
            pa.array([pod.content_hash().to_string()], type=pa.large_string()),
        )
        .append_column(
            _SIDE_EFFECT_RECOMPUTATION_INDEX_COL,
            pa.array([0], type=pa.int32()),
        )
    )
    record_id_hash: ContentHash = pod.data_context.arrow_hasher.hash_table(preimage)
    record_id: bytes = record_id_hash.to_prefixed_digest()

    ctx = InvocationContext(
        pod_name=pod.label,
        pipeline_run_id=run_id,
        _pipeline_hash_ch=pipeline_hash_ch,
        _record_id_hash_ch=record_id_hash,
        _hash_config=pod.pod_config.hash_config,
        _track_completion=pod.pod_config.track_completion,
    )
    return ctx, record_id_hash, record_id


def _build_invocation_context(
    *,
    pod: "SideEffectFunctionPod",
    tag: "TagProtocol",
    data: "DataProtocol",
    pipeline_hash_ch: "ContentHash",
    run_id: str | None,
) -> InvocationContext:
    """Convenience wrapper — returns only the ``InvocationContext``.

    Used by ``SideEffectFunctionPodStream.iter_data()`` where the record ID
    is not needed.
    """
    ctx, _, _ = _build_ctx_and_record_id(
        pod=pod, tag=tag, data=data,
        pipeline_hash_ch=pipeline_hash_ch, run_id=run_id,
    )
    return ctx


# ---------------------------------------------------------------------------
# SideEffectFunctionPod
# ---------------------------------------------------------------------------


class SideEffectFunctionPod(TraceableBase):
    """Function pod that receives an ``InvocationContext`` per row.

    Wraps a callable ``(arg1: T1, ..., ctx: InvocationContext) -> OutputData``.
    The ``ctx`` parameter is stripped from schema inference; data fields are
    passed as keyword arguments. The pod produces a downstream data stream
    like ``FunctionPod``.

    Args:
        fn: Callable whose signature includes ``ctx_arg_name`` plus data args.
        output_keys: Key(s) mapping the return value(s) to output columns.
            A bare string is wrapped in a list.
        ctx_arg_name: Name of the context parameter (default ``"ctx"``).
        config: Pod-level ``SideEffectPodConfig``. Defaults to
            ``SideEffectPodConfig()``.
        name: Optional canonical function name override.
        version: Version integer in the URI (default ``1``).
        label: Optional display label.
        data_context: Optional data context override.

    Raises:
        ValueError: If ``ctx_arg_name`` is not in ``fn``'s signature.
    """

    def __init__(
        self,
        fn: Callable,
        output_keys: list[str] | str,
        ctx_arg_name: str = "ctx",
        config: SideEffectPodConfig | None = None,
        name: str | None = None,
        version: int = 1,
        label: str | None = None,
        data_context: Any = None,
    ) -> None:
        super().__init__(label=label, data_context=data_context)
        self._fn = fn
        self._ctx_arg_name = ctx_arg_name
        self._pod_config = config or SideEffectPodConfig()
        self._version = version
        self._name: str = name if name is not None else getattr(fn, "__name__", "unknown")
        self._output_keys: list[str] = (
            [output_keys] if isinstance(output_keys, str) else list(output_keys)
        )
        self.tracker_manager: "TrackerManagerProtocol" = DEFAULT_TRACKER_MANAGER
        self._is_async = inspect.iscoroutinefunction(fn)

        # Strip ctx for schema inference (raises ValueError if ctx_arg_name missing)
        stripped = _strip_ctx_from_fn(fn, ctx_arg_name)

        # Extract schemas from the stripped wrapper
        self.input_data_schema, self.output_data_schema = extract_function_schemas(
            stripped, output_keys=self._output_keys
        )

        # Pre-compute hashes for URI and result-cache variation data
        from orcapod.hashing.hash_utils import get_function_components, get_function_signature

        semantic_hasher = self.data_context.semantic_hasher
        self._function_signature_hash = semantic_hasher.hash_object(
            get_function_signature(fn)
        ).to_string()
        self._function_content_hash = semantic_hasher.hash_object(
            get_function_components(fn)
        ).to_string()
        self._output_schema_hash = semantic_hasher.hash_object(
            self.output_data_schema
        ).to_string()
        self._git_hash: str = ""  # stable empty string; populated by CI

        # Register Arrow types
        self.data_context.type_converter.ensure_types_registered_for_schemas(
            self.input_data_schema,
            self.output_data_schema,
        )

    # ------------------------------------------------------------------
    # Identity
    # ------------------------------------------------------------------

    @property
    def uri(self) -> tuple[str, ...]:
        """Canonical URI: ``("side_effect_function", name, schema_hash, "vN", "python_side_effect_function")``."""
        return (
            "side_effect_function",
            self.canonical_function_name,
            self._output_schema_hash,
            f"v{self._version}",
            "python_side_effect_function",
        )

    def identity_structure(self) -> Any:
        return self.uri

    def pipeline_identity_structure(self) -> Any:
        return self.identity_structure()

    # ------------------------------------------------------------------
    # Pod API
    # ------------------------------------------------------------------

    @property
    def pod_config(self) -> SideEffectPodConfig:
        """Pod-level configuration."""
        return self._pod_config

    @property
    def canonical_function_name(self) -> str:
        """Human-readable function identifier."""
        return self._name

    def computed_label(self) -> str | None:
        """Use the callable's ``__name__`` as the default label."""
        return getattr(self._fn, "__name__", None)

    def argument_symmetry(self, streams: Collection["StreamProtocol"]) -> Any:
        """Single ordered input — return as an ordered tuple."""
        return tuple(streams)

    def output_schema(
        self,
        *streams: "StreamProtocol",
        columns: "ColumnConfig | dict[str, Any] | None" = None,
        all_info: bool = False,
    ) -> "tuple[Schema, Schema]":
        """Return ``(tag_schema, output_data_schema)`` for the given input streams.

        Args:
            *streams: Exactly one input stream.
            columns: Optional column config.
            all_info: Include all metadata columns.

        Returns:
            ``(tag_schema, output_data_schema)`` — tags pass through unchanged.

        Raises:
            ValueError: If ``streams`` does not contain exactly one stream.
        """
        if len(streams) != 1:
            raise ValueError(
                f"SideEffectFunctionPod expects exactly 1 input stream; got {len(streams)}."
            )
        tag_schema, _ = streams[0].output_schema(columns=columns, all_info=all_info)
        return tag_schema, self.output_data_schema

    def process(
        self, *streams: "StreamProtocol", label: str | None = None
    ) -> "SideEffectFunctionPodStream":
        """Invoke the pod on the input stream.

        Records a ``SideEffectFunctionInvocation`` when inside a pipeline
        recording block, then returns a ``SideEffectFunctionPodStream``.

        Args:
            *streams: Exactly one input stream.
            label: Optional display label.

        Returns:
            A ``SideEffectFunctionPodStream``.

        Raises:
            ValueError: If ``streams`` does not contain exactly one stream.
        """
        if len(streams) != 1:
            raise ValueError(
                f"SideEffectFunctionPod.process() expects exactly 1 stream; got {len(streams)}."
            )
        input_stream = streams[0]
        self.tracker_manager.record_side_effect_function_pod_invocation(
            self, input_stream, label=label
        )
        return SideEffectFunctionPodStream(pod=self, input_stream=input_stream, label=label)

    def __call__(
        self, *streams: "StreamProtocol", label: str | None = None
    ) -> "SideEffectFunctionPodStream":
        """Convenience alias for ``process``."""
        return self.process(*streams, label=label)

    # ------------------------------------------------------------------
    # Internal execution helpers
    # ------------------------------------------------------------------

    def _call_with_ctx(self, data: "DataProtocol", ctx: InvocationContext) -> Any:
        """Call the user function with data kwargs and InvocationContext.

        Args:
            data: Input data row.
            ctx: Per-row ``InvocationContext``.

        Returns:
            Raw function return value.
        """
        kwargs = {self._ctx_arg_name: ctx, **data.as_dict()}
        if self._is_async:
            return self._call_async_sync(kwargs)
        return self._fn(**kwargs)

    def _call_async_sync(self, kwargs: dict[str, Any]) -> Any:
        """Run the async user function synchronously.

        Args:
            kwargs: Keyword arguments to pass to ``self._fn``.

        Returns:
            The coroutine's return value.
        """
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(self._fn(**kwargs))

        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(1) as executor:
            future = executor.submit(asyncio.run, self._fn(**kwargs))
            return future.result()

    def _build_output_data(self, raw_output: Any) -> "DataProtocol":
        """Wrap raw function return value in a ``Data`` object with source info.

        Args:
            raw_output: Raw return value from the user function.

        Returns:
            A ``Data`` with source info and a new UUID.
        """
        from orcapod.core.datagrams import Data
        from orcapod.core.data_function import parse_function_outputs

        output_dict = parse_function_outputs(self._output_keys, raw_output)
        new_uuid = uuid.UUID(bytes=uuid7().bytes)
        source_info = {
            k: f"{':'.join(self.uri)}::{new_uuid.hex}::{k}" for k in output_dict
        }
        return Data(
            output_dict,
            source_info=source_info,
            record_uuid=new_uuid,
            python_schema=self.output_data_schema,
            data_context=self.data_context,
        )

    # ------------------------------------------------------------------
    # Result cache metadata (mirrors PythonDataFunction)
    # ------------------------------------------------------------------

    def get_function_variation_data(self) -> dict[str, Any]:
        """Data defining function variation for ``ResultCache.store()``."""
        return {
            "function_name": self.canonical_function_name,
            "function_signature_hash": self._function_signature_hash,
            "function_content_hash": self._function_content_hash,
            "git_hash": self._git_hash,
        }

    def get_function_variation_data_schema(self) -> "Schema":
        """Schema for ``get_function_variation_data``."""
        from orcapod.types import Schema
        return Schema({
            "function_name": str,
            "function_signature_hash": str,
            "function_content_hash": str,
            "git_hash": str,
        })

    def get_execution_data(self) -> dict[str, Any]:
        """Data defining execution context for ``ResultCache.store()``."""
        vi = sys.version_info
        return {
            "executor_type": "none",
            "executor_info": {},
            "python_version": f"{vi.major}.{vi.minor}.{vi.micro}",
            "extra_info": {},
        }

    def get_execution_data_schema(self) -> "Schema":
        """Schema for ``get_execution_data``."""
        from orcapod.types import Schema
        return Schema({
            "executor_type": str,
            "executor_info": dict[str, str],
            "python_version": str,
            "extra_info": dict[str, str],
        })


# ---------------------------------------------------------------------------
# SideEffectFunctionPodStream — standalone execution (no DB)
# ---------------------------------------------------------------------------


class SideEffectFunctionPodStream(StreamBase):
    """Lazy stream returned by ``SideEffectFunctionPod.process()`` in standalone mode.

    Iterates the upstream stream, builds a per-row ``InvocationContext``,
    calls the user function, and yields ``(tag, output_data)`` pairs.
    No invocation log is written in standalone mode (``run_id=None``).

    Args:
        pod: The ``SideEffectFunctionPod`` this stream wraps.
        input_stream: The upstream stream.
    """

    node_type = "side_effect_function"

    def __init__(
        self,
        pod: SideEffectFunctionPod,
        input_stream: "StreamProtocol",
        **kwargs: Any,
    ) -> None:
        self._pod = pod
        self._input_stream = input_stream
        super().__init__(**kwargs)

    @property
    def producer(self) -> SideEffectFunctionPod:
        return self._pod

    @property
    def upstreams(self) -> "tuple[StreamProtocol, ...]":
        return (self._input_stream,)

    def identity_structure(self) -> Any:
        return (self._pod, self._pod.argument_symmetry((self._input_stream,)))

    def pipeline_identity_structure(self) -> Any:
        return self.identity_structure()

    def output_schema(
        self,
        *,
        columns: "ColumnConfig | dict[str, Any] | None" = None,
        all_info: bool = False,
    ) -> "tuple[Schema, Schema]":
        tag_schema, _ = self._input_stream.output_schema(columns=columns, all_info=all_info)
        return tag_schema, self._pod.output_data_schema

    def keys(
        self,
        *,
        columns: "ColumnConfig | dict[str, Any] | None" = None,
        all_info: bool = False,
    ) -> "tuple[tuple[str, ...], tuple[str, ...]]":
        tag_schema, data_schema = self.output_schema(columns=columns, all_info=all_info)
        return tuple(tag_schema.keys()), tuple(data_schema.keys())

    def iter_data(self) -> "Iterator[tuple[TagProtocol, DataProtocol]]":
        """Iterate the input stream, calling the pod function per row.

        Exceptions from the user function always propagate.
        """
        for tag, data in self._input_stream.iter_data():
            ctx = _build_invocation_context(
                pod=self._pod,
                tag=tag,
                data=data,
                pipeline_hash_ch=self.pipeline_hash(),
                run_id=None,
            )
            raw = self._pod._call_with_ctx(data, ctx)
            output_data = self._pod._build_output_data(raw)
            yield tag, output_data

    def as_table(
        self,
        *,
        columns: "ColumnConfig | dict[str, Any] | None" = None,
        all_info: bool = False,
    ) -> "pa.Table":
        """Collect all rows from ``iter_data()`` into an Arrow table."""
        from orcapod.types import ColumnConfig as _ColumnConfig
        from orcapod.utils import arrow_utils

        column_config = _ColumnConfig.handle_config(columns, all_info=all_info)
        tag_tables = []
        data_tables = []
        for tag, data in self.iter_data():
            tag_tables.append(tag.as_table(columns=column_config))
            data_tables.append(data.as_table(columns=column_config))
        if not tag_tables:
            tag_schema, data_schema = self.output_schema(columns=column_config)
            tc = self._pod.data_context.type_converter
            fields = {
                name: pa.array([], type=tc.python_type_to_arrow_type(py_type))
                for name, py_type in {**tag_schema, **data_schema}.items()
            }
            return pa.table(fields)
        return arrow_utils.hstack_tables(
            pa.concat_tables(tag_tables),
            pa.concat_tables(data_tables),
        )


# ---------------------------------------------------------------------------
# SideEffectFunctionNode — blueprint (raises on iter_data)
# ---------------------------------------------------------------------------


class SideEffectFunctionNode(StreamBase):
    """Lightweight blueprint node for side-effect function pods.

    Used by ``Pipeline`` to represent a ``SideEffectFunctionPod`` invocation
    without any DB attachment or execution logic. ``iter_data()`` raises
    ``PipelineJobRequiredError`` — use a ``PipelineJob`` to execute.

    Args:
        pod: The ``SideEffectFunctionPod`` this node wraps.
        input_stream: The upstream stream at compile time.
        label: Optional display label.
    """

    node_type = "side_effect_function"

    def __init__(
        self,
        pod: SideEffectFunctionPod,
        input_stream: "StreamProtocol",
        label: str | None = None,
    ) -> None:
        self._pod = pod
        self._input_stream = input_stream
        super().__init__(label=label)

    @property
    def producer(self) -> SideEffectFunctionPod:
        return self._pod

    @property
    def upstreams(self) -> "tuple[StreamProtocol, ...]":
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
        columns: "ColumnConfig | dict[str, Any] | None" = None,
        all_info: bool = False,
    ) -> "tuple[Schema, Schema]":
        tag_schema, _ = self._input_stream.output_schema(columns=columns, all_info=all_info)
        return tag_schema, self._pod.output_data_schema

    def keys(
        self,
        *,
        columns: "ColumnConfig | dict[str, Any] | None" = None,
        all_info: bool = False,
    ) -> "tuple[tuple[str, ...], tuple[str, ...]]":
        tag_schema, data_schema = self.output_schema(columns=columns, all_info=all_info)
        return tuple(tag_schema.keys()), tuple(data_schema.keys())

    def iter_data(self) -> "Iterator[tuple[TagProtocol, DataProtocol]]":
        raise PipelineJobRequiredError(
            "SideEffectFunctionNode.iter_data() requires a PipelineJob. "
            "Use pod.process(stream).iter_data() for standalone execution, "
            "or run via a PipelineJob."
        )

    def as_table(
        self,
        *,
        columns: "ColumnConfig | dict[str, Any] | None" = None,
        all_info: bool = False,
    ) -> "pa.Table":
        raise PipelineJobRequiredError(
            "SideEffectFunctionNode.as_table() requires a PipelineJob."
        )

    @property
    def node_uri(self) -> tuple[str, ...]:
        """Canonical URI identifying this node — same as ``pod.uri``."""
        return self._pod.uri

    def set_ephemeral_store(self, store: "ArrowDatabaseProtocol | None") -> None:
        """No-op for blueprint nodes."""


# ---------------------------------------------------------------------------
# SideEffectFunctionJobNode — DB-backed execution
# ---------------------------------------------------------------------------


class SideEffectFunctionJobNode(SideEffectFunctionNode):
    """DB-backed execution node for side-effect function pods.

    Created at pipeline compile time by ``PipelineJob``. Receives databases
    via ``attach_databases()``. On each ``execute()`` call:

    1. Cache hit check (if ``track_completion=True``).
    2. Build ``InvocationContext`` per row.
    3. Call user function — exceptions always propagate.
    4. Wrap output in ``Data`` and cache in result DB.
    5. Write invocation log row to pipeline DB.

    Args:
        pod: The ``SideEffectFunctionPod`` this node wraps.
        input_stream: The upstream stream at compile time.
        label: Optional display label.
    """

    def __init__(
        self,
        pod: SideEffectFunctionPod,
        input_stream: "StreamProtocol",
        label: str | None = None,
    ) -> None:
        super().__init__(pod=pod, input_stream=input_stream, label=label)
        self._pipeline_database: "ArrowDatabaseProtocol | None" = None
        self._result_cache: Any = None
        self._table_path: tuple[str, ...] | None = None

    def attach_databases(
        self,
        pipeline_database: "ArrowDatabaseProtocol | None" = None,
        result_database: "ArrowDatabaseProtocol | None" = None,
    ) -> None:
        """Attach pipeline and result databases.

        Sets up the result cache and the invocation log table path.
        Called by ``PipelineJob._distribute_databases()``.

        Args:
            pipeline_database: Pre-scoped pipeline DB for invocation logging.
            result_database: DB for output caching via ``ResultCache``.
        """
        from orcapod.core.result_cache import ResultCache

        self._pipeline_database = pipeline_database
        self._result_cache = (
            ResultCache(result_database, record_path=self.node_uri)
            if result_database is not None
            else None
        )
        if pipeline_database is not None:
            self._table_path = self.node_uri + (
                f"schema:{self.pipeline_hash().to_string()}",
            )
        else:
            self._table_path = None

    def execute(
        self,
        input_stream: "StreamProtocol",
        *,
        observer: "ExecutionObserverProtocol | None" = None,
        run_id: str | None = None,
    ) -> "list[tuple[TagProtocol, DataProtocol]]":
        """Execute side-effect function delivery for all rows in ``input_stream``.

        Args:
            input_stream: Stream of ``(tag, data)`` pairs to process.
            observer: Optional execution observer (currently unused).
            run_id: Pipeline run identifier from the orchestrator.

        Returns:
            List of ``(tag, output_data)`` tuples.
        """
        from orcapod.core.datagrams import Datagram

        results: list[tuple[TagProtocol, DataProtocol]] = []

        for tag, data in input_stream.iter_data():
            # 1. Cache hit check
            if (
                self._pod.pod_config.track_completion
                and self._result_cache is not None
            ):
                cached = self._result_cache.lookup(data)
                if cached is not None:
                    results.append((tag, cached))
                    continue

            # 2. Build InvocationContext + record_id (single preimage computation)
            ctx, record_id_hash, record_id = _build_ctx_and_record_id(
                pod=self._pod,
                tag=tag,
                data=data,
                pipeline_hash_ch=self.pipeline_hash(),
                run_id=run_id,
            )

            # 3. Call user function — always re-raise (no silent row suppression)
            try:
                raw = self._pod._call_with_ctx(data, ctx)
            except Exception as exc:
                if self._pod.pod_config.on_error == "log":
                    logger.warning(
                        "SideEffectFunctionPod %r failed on row: %s",
                        self._pod.label,
                        exc,
                        exc_info=True,
                    )
                raise  # always re-raise

            # 4. Wrap output and cache it
            output_data = self._pod._build_output_data(raw)
            if self._result_cache is not None:
                var_dg = Datagram(
                    self._pod.get_function_variation_data(),
                    python_schema=self._pod.get_function_variation_data_schema(),
                    data_context=self._pod.data_context,
                )
                exec_dg = Datagram(
                    self._pod.get_execution_data(),
                    python_schema=self._pod.get_execution_data_schema(),
                    data_context=self._pod.data_context,
                )
                self._result_cache.store(data, output_data, var_dg, exec_dg)

            # 5. Log invocation to pipeline database
            if self._pipeline_database is not None and self._table_path is not None:
                _write_invocation_row(
                    pipeline_database=self._pipeline_database,
                    table_path=self._table_path,
                    record_id=record_id,
                    record_id_hash_str=record_id_hash.to_string(),
                    run_id=run_id,
                )

            results.append((tag, output_data))

        return results

    async def async_execute(
        self,
        inputs: "Sequence[ReadableChannel[tuple[TagProtocol, DataProtocol]]]",
        output: "WritableChannel[tuple[TagProtocol, DataProtocol]]",
        *,
        observer: "ExecutionObserverProtocol | None" = None,
        run_id: str | None = None,
    ) -> None:
        """Async execution with semaphore-bounded concurrency.

        Reads from ``inputs[0]``, dispatches each row as an independent
        async task via ``asyncio.TaskGroup``. A semaphore bounds in-flight
        tasks at ``pod_config.max_concurrency`` (default 16). Always closes
        ``output`` in a ``finally`` block.

        Args:
            inputs: Single-element sequence with the input channel.
            output: Writable channel for output ``(tag, output_data)`` pairs.
            observer: Optional execution observer (currently unused).
            run_id: Pipeline run identifier from the orchestrator.

        Raises:
            ValueError: If ``inputs`` does not contain exactly one channel.
        """
        if len(inputs) != 1:
            raise ValueError(
                f"SideEffectFunctionJobNode.async_execute expects exactly 1 "
                f"input channel; got {len(inputs)}."
            )

        max_concurrency = self._pod.pod_config.max_concurrency
        sem = asyncio.Semaphore(max_concurrency) if max_concurrency is not None else None

        try:
            async def process_one(tag: "TagProtocol", data: "DataProtocol") -> None:
                try:
                    from orcapod.core.datagrams import Datagram

                    # Cache hit check
                    if (
                        self._pod.pod_config.track_completion
                        and self._result_cache is not None
                    ):
                        cached = self._result_cache.lookup(data)
                        if cached is not None:
                            await output.send((tag, cached))
                            return

                    ctx, record_id_hash, record_id = _build_ctx_and_record_id(
                        pod=self._pod,
                        tag=tag,
                        data=data,
                        pipeline_hash_ch=self.pipeline_hash(),
                        run_id=run_id,
                    )

                    try:
                        raw = self._pod._call_with_ctx(data, ctx)
                    except Exception as exc:
                        if self._pod.pod_config.on_error == "log":
                            logger.warning(
                                "SideEffectFunctionPod %r async failed: %s",
                                self._pod.label,
                                exc,
                                exc_info=True,
                            )
                        raise

                    output_data = self._pod._build_output_data(raw)

                    if self._result_cache is not None:
                        var_dg = Datagram(
                            self._pod.get_function_variation_data(),
                            python_schema=self._pod.get_function_variation_data_schema(),
                            data_context=self._pod.data_context,
                        )
                        exec_dg = Datagram(
                            self._pod.get_execution_data(),
                            python_schema=self._pod.get_execution_data_schema(),
                            data_context=self._pod.data_context,
                        )
                        self._result_cache.store(data, output_data, var_dg, exec_dg)

                    if self._pipeline_database is not None and self._table_path is not None:
                        _write_invocation_row(
                            pipeline_database=self._pipeline_database,
                            table_path=self._table_path,
                            record_id=record_id,
                            record_id_hash_str=record_id_hash.to_string(),
                            run_id=run_id,
                        )

                    await output.send((tag, output_data))
                finally:
                    if sem is not None:
                        sem.release()

            async with asyncio.TaskGroup() as tg:
                async for tag, data in inputs[0]:
                    if sem is not None:
                        await sem.acquire()
                    tg.create_task(process_one(tag, data))
        finally:
            await output.close()

    def set_ephemeral_store(self, store: "ArrowDatabaseProtocol | None") -> None:
        """No-op for this node type."""


# ---------------------------------------------------------------------------
# Decorator
# ---------------------------------------------------------------------------


def side_effect_function_pod(
    fn: Callable | None = None,
    *,
    output_keys: list[str] | str,
    ctx_arg_name: str = "ctx",
    config: SideEffectPodConfig | None = None,
    name: str | None = None,
    version: int = 1,
) -> "SideEffectFunctionPod | Callable":
    """Decorator wrapping a callable as a ``SideEffectFunctionPod``.

    Parameterised usage only — ``output_keys`` is always required:

    .. code-block:: python

        @side_effect_function_pod(output_keys=["artifact_path"])
        def write_artifact(value: int, ctx: InvocationContext) -> Path:
            path = Path(f"out/{ctx.invocation_hash}.bin")
            path.write_bytes(value.to_bytes(4, "big"))
            return path

    The decorated name is replaced with a ``SideEffectFunctionPod`` instance.
    The pod is callable (via ``__call__`` → ``process``).

    Args:
        fn: Internal — not for direct caller use.
        output_keys: Key(s) mapping the return value(s) to output columns.
        ctx_arg_name: Name of the context parameter (default ``"ctx"``).
        config: Optional ``SideEffectPodConfig``.
        name: Optional canonical function name override.
        version: Version integer for the URI (default ``1``).

    Returns:
        A ``SideEffectFunctionPod`` (or a one-argument decorator).
    """
    def _wrap(f: Callable) -> SideEffectFunctionPod:
        return SideEffectFunctionPod(
            f,
            output_keys=output_keys,
            ctx_arg_name=ctx_arg_name,
            config=config,
            name=name,
            version=version,
        )

    if fn is not None:
        return _wrap(fn)
    return _wrap
```

- [ ] **Step 5: Run tests to confirm SF-01, SF-02, SF-03, SF-10 pass**

```bash
uv run pytest tests/test_core/side_effect_function/test_side_effect_function_pod.py::TestSideEffectFunctionPodSchema -v
```

Expected: all 4 tests pass.

- [ ] **Step 6: Run full test suite to confirm no regressions**

```bash
uv run pytest tests/ -x -q --tb=short 2>&1 | tail -10
```

- [ ] **Step 7: Commit**

```bash
git add src/orcapod/core/side_effect_function/ tests/test_core/side_effect_function/
git commit -m "feat(side_effect_function): scaffold module with SideEffectFunctionPod + all node classes"
```

---

### Task 3: Invocation Type + Protocol + Tracker Plumbing (SF-04, SF-05)

Add `SideEffectFunctionInvocation`, `SideEffectFunctionPodProtocol`, and `record_side_effect_function_pod_invocation` to tracker and pipeline base. Then test standalone stream execution.

**Files:**
- Modify: `src/orcapod/pipeline/pod_invocation.py`
- Create: `src/orcapod/protocols/core_protocols/side_effect_function_pod.py`
- Modify: `src/orcapod/protocols/core_protocols/__init__.py`
- Modify: `src/orcapod/protocols/core_protocols/trackers.py`
- Modify: `src/orcapod/core/tracker.py`
- Modify: `src/orcapod/pipeline/base.py`
- Modify: `tests/test_core/side_effect_function/test_side_effect_function_pod.py` (SF-04, SF-05)

- [ ] **Step 1: Write failing tests SF-04 and SF-05**

Add to `tests/test_core/side_effect_function/test_side_effect_function_pod.py`:

```python
class TestSideEffectFunctionPodStreamStandalone:
    """SF-04, SF-05: standalone execution via SideEffectFunctionPodStream."""

    def test_sf04_iter_data_returns_correct_output(self):
        """SF-04: iter_data() returns correct (tag, output_data) per row."""
        from orcapod.core.side_effect_function import SideEffectFunctionPod

        def my_fn(value: int, ctx: InvocationContext) -> str:
            return f"v{value}"

        pod = SideEffectFunctionPod(my_fn, output_keys=["result"])
        stream = _make_stream(3)
        rows = list(pod.process(stream).iter_data())

        assert len(rows) == 3
        for i, (tag, data) in enumerate(rows):
            assert data.as_dict()["result"] == f"v{i}"
        # Tags pass through unchanged
        assert rows[0][0].as_dict()["id"] == 0
        assert rows[1][0].as_dict()["id"] == 1

    def test_sf05_invocation_context_fields_standalone(self):
        """SF-05: InvocationContext has pod_name, non-empty hash, pipeline_run_id=None."""
        from orcapod.core.side_effect_function import SideEffectFunctionPod

        received_ctx: list[InvocationContext] = []

        def my_fn(value: int, ctx: InvocationContext) -> str:
            received_ctx.append(ctx)
            return str(value)

        pod = SideEffectFunctionPod(my_fn, output_keys=["result"])
        stream = _make_stream(1)
        list(pod.process(stream).iter_data())

        assert len(received_ctx) == 1
        ctx = received_ctx[0]
        assert ctx.pod_name == pod.label
        assert isinstance(ctx.invocation_hash, str)
        assert len(ctx.invocation_hash) > 0
        assert "::" in ctx.invocation_hash
        assert ctx.pipeline_run_id is None  # standalone: no run_id
```

- [ ] **Step 2: Run tests to confirm they fail (tracker method missing)**

```bash
uv run pytest tests/test_core/side_effect_function/test_side_effect_function_pod.py::TestSideEffectFunctionPodStreamStandalone -x -q 2>&1 | head -20
```

Expected: `AttributeError: 'BasicTrackerManager' object has no attribute 'record_side_effect_function_pod_invocation'` — called from `SideEffectFunctionPod.process()`.

- [ ] **Step 3: Add `SideEffectFunctionInvocation` to `pod_invocation.py`**

Add at the end of `src/orcapod/pipeline/pod_invocation.py`:

```python
class SideEffectFunctionInvocation(PodInvocation):
    """Invocation of a side-effect function pod against exactly one input stream.

    Args:
        pod: A ``SideEffectFunctionPod`` instance.
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
                f"SideEffectFunctionInvocation requires exactly 1 input stream; "
                f"got {len(input_streams)}."
            )
        super().__init__(pod=pod, input_streams=input_streams, label=label)
```

- [ ] **Step 4: Create `SideEffectFunctionPodProtocol`**

Create `src/orcapod/protocols/core_protocols/side_effect_function_pod.py`:

```python
"""Protocol for SideEffectFunctionPod."""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from orcapod.protocols.hashing_protocols import PipelineElementProtocol

if TYPE_CHECKING:
    from orcapod.side_effects import SideEffectPodConfig
    from orcapod.protocols.core_protocols.streams import StreamProtocol
    from orcapod.types import Schema


@runtime_checkable
class SideEffectFunctionPodProtocol(PipelineElementProtocol, Protocol):
    """Protocol for side-effect function pods.

    Hybrid of ``FunctionPodProtocol`` and ``SideEffectPodProtocol``:
    receives per-row ``InvocationContext``, produces a downstream data stream.
    """

    @property
    def pod_config(self) -> "SideEffectPodConfig": ...

    @property
    def input_data_schema(self) -> "Schema": ...

    @property
    def output_data_schema(self) -> "Schema": ...

    def process(self, *streams: "StreamProtocol", label: str | None = None) -> Any: ...

    def output_schema(
        self,
        *streams: "StreamProtocol",
        columns: Any = None,
        all_info: bool = False,
    ) -> "tuple[Schema, Schema]": ...

    def argument_symmetry(self, streams: Any) -> Any: ...
```

- [ ] **Step 5: Export `SideEffectFunctionPodProtocol` from `core_protocols/__init__.py`**

Add to `src/orcapod/protocols/core_protocols/__init__.py`:

```python
from .side_effect_function_pod import SideEffectFunctionPodProtocol
```

Add `"SideEffectFunctionPodProtocol"` to `__all__`.

- [ ] **Step 6: Add `record_side_effect_function_pod_invocation` to both tracker protocols**

In `src/orcapod/protocols/core_protocols/trackers.py`, add to `TrackerProtocol`:

```python
def record_side_effect_function_pod_invocation(
    self,
    pod: Any,
    input_stream: StreamProtocol,
    label: str | None = None,
) -> None:
    """Record a side-effect function pod invocation in the computational graph.

    Args:
        pod: The ``SideEffectFunctionPod`` being invoked.
        input_stream: The upstream stream.
        label: Optional display label.
    """
    ...
```

Add same method to `TrackerManagerProtocol`:

```python
def record_side_effect_function_pod_invocation(
    self,
    pod: Any,
    input_stream: StreamProtocol,
    label: str | None = None,
) -> None:
    """Record a side-effect function pod invocation in all active trackers.

    Args:
        pod: The ``SideEffectFunctionPod`` to record.
        input_stream: The upstream stream.
        label: Optional display label.
    """
    ...
```

- [ ] **Step 7: Add `record_side_effect_function_pod_invocation` to `BasicTrackerManager` in `tracker.py`**

Add to `BasicTrackerManager` in `src/orcapod/core/tracker.py`:

```python
def record_side_effect_function_pod_invocation(
    self,
    pod: Any,
    input_stream: cp.StreamProtocol,
    label: str | None = None,
) -> None:
    """Record a side-effect function pod invocation in all active trackers."""
    for tracker in self.get_active_trackers():
        tracker.record_side_effect_function_pod_invocation(pod, input_stream, label=label)
```

- [ ] **Step 8: Wire `AbstractPipelineBase` — add recorder, abstract property, compile + to_invocations**

In `src/orcapod/pipeline/base.py`:

**8a. Add imports:**

```python
from orcapod.pipeline.pod_invocation import (
    FunctionInvocation,
    OperatorInvocation,
    PodInvocation,
    SideEffectFunctionInvocation,   # NEW
    SideEffectInvocation,
)
```

Also add a lazy import for `SideEffectFunctionNode` inside methods (to avoid circular imports).

**8b. Add abstract property after `side_effect_node_class`:**

```python
@property
@abstractmethod
def side_effect_function_node_class(self) -> type:
    """Node class for side-effect-function pod invocations."""
    ...
```

**8c. Add recording method (concrete, like `record_side_effect_pod_invocation`):**

```python
def record_side_effect_function_pod_invocation(
    self,
    pod: Any,
    input_stream: "cp.StreamProtocol",
    label: str | None = None,
) -> None:
    """Record a side-effect function pod invocation into the graph.

    Args:
        pod: The ``SideEffectFunctionPod`` being invoked.
        input_stream: The upstream stream.
        label: Optional display label for the resulting compiled node.
    """
    self._record_invocation(
        SideEffectFunctionInvocation(pod=pod, input_streams=(input_stream,), label=label)
    )
```

**8d. Update `compile()` topological pass** — add before the `else` (OperatorInvocation) branch:

```python
elif isinstance(inv, SideEffectFunctionInvocation):
    node_map[key] = self.side_effect_function_node_class(
        pod=inv.pod,
        input_stream=upstream_nodes[0],
        label=inv.label,
    )
```

**8e. Update `to_invocations()`** — add after the `SideEffectNode` branch and before the `else` branch:

```python
# Import at top of the method body or at module level:
from orcapod.core.side_effect_function import SideEffectFunctionNode as _SEFNode

# Then in the loop:
elif isinstance(node, _SEFNode):
    inv_by_node_hash[node_hash] = SideEffectFunctionInvocation(
        pod=node._pod,
        input_streams=(node.upstreams[0],),
        label=node._label,
    )
```

To avoid circular imports, use a local import inside the method:

```python
def to_invocations(self) -> InvocationGraph:
    ...
    from orcapod.core.side_effect_function.side_effect_function_pod import SideEffectFunctionNode as _SEFNode
    ...
    elif isinstance(node, _SEFNode):
        inv_by_node_hash[node_hash] = SideEffectFunctionInvocation(...)
```

- [ ] **Step 9: Run SF-04 and SF-05 to confirm they pass**

```bash
uv run pytest tests/test_core/side_effect_function/test_side_effect_function_pod.py::TestSideEffectFunctionPodStreamStandalone -v
```

Expected: both tests pass.

- [ ] **Step 10: Run full test suite**

```bash
uv run pytest tests/ -x -q --tb=short 2>&1 | tail -15
```

- [ ] **Step 11: Commit**

```bash
git add \
  src/orcapod/pipeline/pod_invocation.py \
  src/orcapod/protocols/core_protocols/side_effect_function_pod.py \
  src/orcapod/protocols/core_protocols/__init__.py \
  src/orcapod/protocols/core_protocols/trackers.py \
  src/orcapod/core/tracker.py \
  src/orcapod/pipeline/base.py \
  tests/test_core/side_effect_function/test_side_effect_function_pod.py
git commit -m "feat(side_effect_function): invocation type, protocol, tracker plumbing, pipeline base wiring"
```

---

### Task 4: Node Protocols + Orchestrators + `graph.py`/`job.py` Class Attributes

Add `SideEffectFunctionNodeProtocol` + `is_side_effect_function_node`, update both orchestrators and Pipeline/PipelineJob class attributes.

**Files:**
- Modify: `src/orcapod/protocols/node_protocols.py`
- Modify: `src/orcapod/pipeline/sync_orchestrator.py`
- Modify: `src/orcapod/pipeline/async_orchestrator.py`
- Modify: `src/orcapod/pipeline/graph.py`
- Modify: `src/orcapod/pipeline/job.py`

- [ ] **Step 1: Add `SideEffectFunctionNodeProtocol` and `is_side_effect_function_node` to `node_protocols.py`**

```python
# In src/orcapod/protocols/node_protocols.py, add after SideEffectNodeProtocol:

@runtime_checkable
class SideEffectFunctionNodeProtocol(Protocol):
    """Protocol for side-effect-function nodes in orchestrated execution.

    Combines function-pod output production with per-row ``InvocationContext``
    injection and optional DB-backed caching + invocation logging.
    """

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
        result_database: "ArrowDatabaseProtocol | None" = None,
    ) -> None: ...


def is_side_effect_function_node(node: "GraphNode") -> TypeGuard[SideEffectFunctionNodeProtocol]:
    """Check if a node is a side-effect-function node."""
    return node.node_type == "side_effect_function"
```

- [ ] **Step 2: Update `sync_orchestrator.py`**

Add import:
```python
from orcapod.protocols.node_protocols import (
    is_function_node,
    is_operator_node,
    is_side_effect_function_node,   # NEW
    is_side_effect_node,
    is_source_node,
)
```

Add branch in `run()` after `elif is_side_effect_node(node):` and before `else: raise TypeError`:

```python
elif is_side_effect_function_node(node):
    upstream_buf = self._gather_upstream(node, graph, buffers)
    upstream_node = list(graph.predecessors(node))[0]
    input_stream = self._materialize_as_stream(upstream_buf, upstream_node)
    buffers[node] = node.execute(
        input_stream,
        observer=effective_observer,
        run_id=run_id,
    )
```

- [ ] **Step 3: Update `async_orchestrator.py`**

Add same import change. Add branch after `elif is_side_effect_node(node):` and before `else: raise TypeError`:

```python
elif is_side_effect_function_node(node):
    predecessors = in_edges.get(node, [])
    if not predecessors:
        raise ValueError("SideEffectFunctionNode expects exactly 1 upstream, got 0")
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

- [ ] **Step 4: Update `graph.py` — add `side_effect_function_node_class` class attribute**

In `src/orcapod/pipeline/graph.py`:

Add import:
```python
from orcapod.core.side_effect_function.side_effect_function_pod import SideEffectFunctionNode
```

Add to `Pipeline` class:
```python
side_effect_function_node_class = SideEffectFunctionNode
```

- [ ] **Step 5: Update `job.py` — add class attr, `_distribute_databases`, `as_pipeline`**

In `src/orcapod/pipeline/job.py`:

Add import:
```python
from orcapod.core.side_effect_function.side_effect_function_pod import (
    SideEffectFunctionJobNode,
    SideEffectFunctionNode,
)
```

Add to `PipelineJob` class attributes:
```python
side_effect_function_node_class = SideEffectFunctionJobNode
```

In `_distribute_databases()`, add after the `elif isinstance(node, SideEffectJobNode):` branch:

```python
elif isinstance(node, SideEffectFunctionJobNode):
    node.attach_databases(
        pipeline_database=pipeline_db,
        result_database=result_db,
    )
```

In `as_pipeline()`, add after the `elif isinstance(job_node, SideEffectJobNode):` branch and before the final `else: node_map[node_hash] = job_node.as_node()`:

```python
elif isinstance(job_node, SideEffectFunctionJobNode):
    upstream_bp_hash = job_id_to_bp_hash[id(job_node._input_stream)]
    node_map[node_hash] = SideEffectFunctionNode(
        pod=job_node._pod,
        input_stream=node_map[upstream_bp_hash],
        label=job_node._label,
    )
```

- [ ] **Step 6: Run existing tests to confirm no regressions**

```bash
uv run pytest tests/ -x -q --tb=short 2>&1 | tail -15
```

Expected: all pass.

- [ ] **Step 7: Commit**

```bash
git add \
  src/orcapod/protocols/node_protocols.py \
  src/orcapod/pipeline/sync_orchestrator.py \
  src/orcapod/pipeline/async_orchestrator.py \
  src/orcapod/pipeline/graph.py \
  src/orcapod/pipeline/job.py
git commit -m "feat(side_effect_function): node protocol + orchestrator dispatch + pipeline class attrs"
```

---

### Task 5: DB-Backed Execution Tests (SF-06–SF-09) + Decorator + Re-exports + Pipeline Integration (SF-11–SF-13)

Add remaining tests and wire the decorator and public API. All implementation code already exists from Task 2 — these tests verify the already-implemented `SideEffectFunctionJobNode.execute()`, `async_execute()`, and `side_effect_function_pod` decorator.

**Files:**
- Modify: `tests/test_core/side_effect_function/test_side_effect_function_pod.py` (SF-06–SF-13)
- Modify: `src/orcapod/__init__.py`

- [ ] **Step 1: Add tests SF-06, SF-07, SF-08, SF-09 to the test file**

```python
class TestSideEffectFunctionJobNode:
    """SF-06, SF-07, SF-08, SF-09: DB-backed sync execution."""

    def test_sf06_output_cached_after_first_run(self):
        """SF-06: Output cached; second run returns cached result without re-calling fn."""
        from orcapod.core.side_effect_function import SideEffectFunctionPod, SideEffectFunctionJobNode

        call_count = 0

        def my_fn(value: int, ctx: InvocationContext) -> str:
            nonlocal call_count
            call_count += 1
            return f"r{value}"

        pod = SideEffectFunctionPod(my_fn, output_keys=["result"])
        stream = _make_stream(2)
        pipeline_db = _make_in_memory_db()
        result_db = _make_in_memory_db()

        node1 = SideEffectFunctionJobNode(pod=pod, input_stream=stream)
        node1.attach_databases(pipeline_database=pipeline_db, result_database=result_db)
        results1 = node1.execute(stream)
        assert len(results1) == 2
        assert call_count == 2

        # Second run — same pod, same data, same DBs — fn must NOT be called again
        node2 = SideEffectFunctionJobNode(pod=pod, input_stream=stream)
        node2.attach_databases(pipeline_database=pipeline_db, result_database=result_db)
        results2 = node2.execute(stream)
        assert len(results2) == 2
        assert call_count == 2  # NOT incremented — cache hit

        # Both runs produce equal result values
        for (_, d1), (_, d2) in zip(results1, results2):
            assert d1.as_dict()["result"] == d2.as_dict()["result"]

    def test_sf07_invocation_log_written_on_first_run(self):
        """SF-07: Invocation log row written to pipeline_database on first run."""
        import polars as pl
        from orcapod.core.side_effect_function import SideEffectFunctionPod, SideEffectFunctionJobNode

        def my_fn(value: int, ctx: InvocationContext) -> str:
            return f"r{value}"

        pod = SideEffectFunctionPod(my_fn, output_keys=["result"])
        stream = _make_stream(3)
        pipeline_db = _make_in_memory_db()
        result_db = _make_in_memory_db()
        node = SideEffectFunctionJobNode(pod=pod, input_stream=stream)
        node.attach_databases(pipeline_database=pipeline_db, result_database=result_db)
        results = node.execute(stream)

        assert len(results) == 3

        # Invocation log has 3 rows
        table_path = node._table_path
        records = pipeline_db.get_all_records(table_path)
        assert records is not None
        df = pl.from_arrow(records)
        assert len(df) == 3
        assert "record_id_hash" in df.columns
        assert "executed_at" in df.columns

    def test_sf08_track_completion_false_always_reruns(self):
        """SF-08: track_completion=False — fn called every run; invocation logged each time."""
        from orcapod.side_effects import SideEffectPodConfig
        from orcapod.core.side_effect_function import SideEffectFunctionPod, SideEffectFunctionJobNode

        call_count = 0

        def my_fn(value: int, ctx: InvocationContext) -> str:
            nonlocal call_count
            call_count += 1
            return f"r{value}"

        cfg = SideEffectPodConfig(track_completion=False)
        pod = SideEffectFunctionPod(my_fn, output_keys=["result"], config=cfg)
        stream = _make_stream(2)
        pipeline_db = _make_in_memory_db()
        result_db = _make_in_memory_db()

        node1 = SideEffectFunctionJobNode(pod=pod, input_stream=stream)
        node1.attach_databases(pipeline_database=pipeline_db, result_database=result_db)
        node1.execute(stream)
        assert call_count == 2

        node2 = SideEffectFunctionJobNode(pod=pod, input_stream=stream)
        node2.attach_databases(pipeline_database=pipeline_db, result_database=result_db)
        node2.execute(stream)
        assert call_count == 4  # called again — track_completion=False

    def test_sf09_on_error_log_reraises(self):
        """SF-09: on_error='log' — exception logged then always re-raised."""
        from orcapod.side_effects import SideEffectPodConfig
        from orcapod.core.side_effect_function import SideEffectFunctionPod, SideEffectFunctionJobNode

        def my_fn(value: int, ctx: InvocationContext) -> str:
            raise RuntimeError("test error")

        cfg = SideEffectPodConfig(on_error="log")
        pod = SideEffectFunctionPod(my_fn, output_keys=["result"], config=cfg)
        stream = _make_stream(1)
        pipeline_db = _make_in_memory_db()
        result_db = _make_in_memory_db()
        node = SideEffectFunctionJobNode(pod=pod, input_stream=stream)
        node.attach_databases(pipeline_database=pipeline_db, result_database=result_db)

        # Must propagate — no silent row suppression
        with pytest.raises(RuntimeError, match="test error"):
            node.execute(stream)
```

- [ ] **Step 2: Run SF-06–SF-09 tests**

```bash
uv run pytest tests/test_core/side_effect_function/test_side_effect_function_pod.py::TestSideEffectFunctionJobNode -v
```

Expected: all 4 tests pass.

- [ ] **Step 3: Add tests SF-11, SF-12, SF-13**

```python
class TestSideEffectFunctionPodDecorator:
    """SF-11: @side_effect_function_pod decorator."""

    def test_sf11_decorator_creates_correct_pod(self):
        """SF-11: Decorator creates a SideEffectFunctionPod with correct URI."""
        from orcapod.core.side_effect_function import (
            SideEffectFunctionPod,
            side_effect_function_pod,
        )

        @side_effect_function_pod(output_keys=["result"])
        def my_fn(value: int, ctx: InvocationContext) -> str:
            return str(value)

        assert isinstance(my_fn, SideEffectFunctionPod)
        assert my_fn.uri[0] == "side_effect_function"
        assert my_fn.canonical_function_name == "my_fn"

    def test_sf11_decorator_accessible_from_public_api(self):
        """SF-11: Decorator and pod class accessible from orcapod top-level."""
        import orcapod
        assert hasattr(orcapod, "side_effect_function_pod")
        assert hasattr(orcapod, "SideEffectFunctionPod")


class TestSideEffectFunctionPodPipelineIntegration:
    """SF-12: Full pipeline compilation and execution."""

    def test_sf12_pipeline_compilation_and_execution(self):
        """SF-12: SideEffectFunctionJobNode compiled, fn called, invocation logged."""
        import polars as pl
        from orcapod.pipeline.job import PipelineJob
        from orcapod.core.side_effect_function import SideEffectFunctionPod
        from orcapod.core.sources.dict_source import DictSource

        received_ctx: list[InvocationContext] = []

        def transform(value: int, ctx: InvocationContext) -> str:
            received_ctx.append(ctx)
            return f"result_{value}"

        pod = SideEffectFunctionPod(transform, output_keys=["result"])
        db = _make_in_memory_db()

        with PipelineJob(name="test_sef", store=db) as job:
            source = DictSource(
                [{"id": 0, "value": 10}, {"id": 1, "value": 20}],
                tag_columns=["id"],
            )
            pod.process(source)

        job.run()

        assert len(received_ctx) == 2  # fn called once per row

        # Find the side_effect_function node in the compiled graph
        sef_nodes = [
            n for n in job.dag.nodes()
            if getattr(n, "node_type", None) == "side_effect_function"
        ]
        assert len(sef_nodes) == 1
        node = sef_nodes[0]

        # Invocation log written to pipeline DB
        pipeline_db = db.at("test_sef")
        table_path = node._table_path
        records = pipeline_db.get_all_records(table_path)
        assert records is not None
        df = pl.from_arrow(records)
        assert len(df) == 2
        assert "record_id_hash" in df.columns

    def test_sf12_second_pipeline_run_uses_cache(self):
        """SF-12: Second pipeline run uses cached output; fn not called again."""
        from orcapod.pipeline.job import PipelineJob
        from orcapod.core.side_effect_function import SideEffectFunctionPod
        from orcapod.core.sources.dict_source import DictSource

        call_count = 0

        def transform(value: int, ctx: InvocationContext) -> str:
            nonlocal call_count
            call_count += 1
            return f"r{value}"

        pod = SideEffectFunctionPod(transform, output_keys=["result"])
        db = _make_in_memory_db()
        source_data = [{"id": 0, "value": 10}, {"id": 1, "value": 20}]

        with PipelineJob(name="test_sef_cache", store=db) as job1:
            source1 = DictSource(source_data, tag_columns=["id"])
            pod.process(source1)
        job1.run()
        assert call_count == 2

        with PipelineJob(name="test_sef_cache", store=db) as job2:
            source2 = DictSource(source_data, tag_columns=["id"])
            pod.process(source2)
        job2.run()
        assert call_count == 2  # NOT called again — cache hit


class TestSideEffectFunctionJobNodeAsync:
    """SF-13: async_execute produces same output as sync path."""

    def test_sf13_async_execute_basic(self):
        """SF-13: async_execute processes all rows, writes cache + log, returns correct output."""
        import asyncio
        import polars as pl
        from orcapod.core.side_effect_function import SideEffectFunctionPod, SideEffectFunctionJobNode
        from orcapod.channels import Channel

        call_count = 0

        def my_fn(value: int, ctx: InvocationContext) -> str:
            nonlocal call_count
            call_count += 1
            return f"async_{value}"

        pod = SideEffectFunctionPod(my_fn, output_keys=["result"])
        stream = _make_stream(3)
        pipeline_db = _make_in_memory_db()
        result_db = _make_in_memory_db()
        node = SideEffectFunctionJobNode(pod=pod, input_stream=stream)
        node.attach_databases(pipeline_database=pipeline_db, result_database=result_db)

        async def _run():
            ch_in = Channel(buffer_size=10)
            ch_out = Channel(buffer_size=10)

            async def feed():
                for tag, data in stream.iter_data():
                    await ch_in.writer.send((tag, data))
                await ch_in.writer.close()

            await asyncio.gather(
                feed(),
                node.async_execute([ch_in.reader], ch_out.writer, run_id="test-async"),
            )
            return await ch_out.reader.collect()

        results = asyncio.run(_run())
        assert len(results) == 3
        assert call_count == 3

        # Invocation log written
        records = pipeline_db.get_all_records(node._table_path)
        df = pl.from_arrow(records)
        assert len(df) == 3

        # Output values are correct
        result_values = {data.as_dict()["result"] for _, data in results}
        assert result_values == {f"async_{i}" for i in range(3)}
```

- [ ] **Step 4: Add re-exports to `src/orcapod/__init__.py`**

```python
# Add after the existing side_effects imports:
from orcapod.core.side_effect_function import (
    SideEffectFunctionPod,
    SideEffectFunctionNode,
    SideEffectFunctionJobNode,
    side_effect_function_pod,
)

# Add to __all__:
"SideEffectFunctionPod",
"SideEffectFunctionNode",
"SideEffectFunctionJobNode",
"side_effect_function_pod",
```

- [ ] **Step 5: Run all SF tests**

```bash
uv run pytest tests/test_core/side_effect_function/ -v
```

Expected: all 13 test cases (SF-01 through SF-13) pass.

- [ ] **Step 6: Run full test suite**

```bash
uv run pytest tests/ -q --tb=short 2>&1 | tail -20
```

Expected: all tests pass.

- [ ] **Step 7: Commit**

```bash
git add \
  tests/test_core/side_effect_function/test_side_effect_function_pod.py \
  src/orcapod/__init__.py
git commit -m "feat(side_effect_function): SF-06–SF-13 tests + public API re-exports"
```

---

## Self-Review

### 1. Spec Coverage

| Spec requirement | Task |
|-----------------|------|
| `SideEffectPod` breaking change: `ctx_arg_name` + `**kwargs` call style | Task 1 |
| `SideEffectPod` decorators gain `ctx_arg_name` | Task 1 |
| `identity_structure()` includes `ctx_arg_name` | Task 1 |
| `_strip_ctx_from_fn` helper | Task 2 |
| `SideEffectFunctionPod` constructor + schema extraction | Task 2 |
| URI: `("side_effect_function", name, schema_hash, "vN", "python_side_effect_function")` | Task 2 |
| `_call_with_ctx` + `_build_output_data` | Task 2 |
| `get_function_variation_data` + `get_execution_data` | Task 2 |
| `SideEffectFunctionPodStream.iter_data()` (standalone) | Task 2 |
| `_build_invocation_context` / `_build_ctx_and_record_id` | Task 2 |
| `SideEffectFunctionNode` (blueprint, raises on iter_data) | Task 2 |
| `SideEffectFunctionJobNode.attach_databases()` | Task 2 |
| `SideEffectFunctionJobNode.execute()` with cache + invocation log | Task 2 |
| `SideEffectFunctionJobNode.async_execute()` | Task 2 |
| `SideEffectFunctionInvocation` | Task 3 |
| `SideEffectFunctionPodProtocol` | Task 3 |
| `record_side_effect_function_pod_invocation` in tracker chain | Task 3 |
| `AbstractPipelineBase.compile()` handles `SideEffectFunctionInvocation` | Task 3 |
| `AbstractPipelineBase.to_invocations()` handles `SideEffectFunctionNode` | Task 3 |
| `SideEffectFunctionNodeProtocol` + `is_side_effect_function_node` | Task 4 |
| Sync orchestrator `elif is_side_effect_function_node(node):` branch | Task 4 |
| Async orchestrator `elif is_side_effect_function_node(node):` branch | Task 4 |
| `Pipeline.side_effect_function_node_class = SideEffectFunctionNode` | Task 4 |
| `PipelineJob.side_effect_function_node_class = SideEffectFunctionJobNode` | Task 4 |
| `PipelineJob._distribute_databases()` handles `SideEffectFunctionJobNode` | Task 4 |
| `PipelineJob.as_pipeline()` handles `SideEffectFunctionJobNode` | Task 4 |
| `@side_effect_function_pod` decorator | Task 2 (impl) / Task 5 (tests) |
| Re-exports from `orcapod.__init__` | Task 5 |
| Test suite SF-01–SF-13 + SEP-UPDATE + SEP-CTX-NAME | Tasks 1–5 |

All spec requirements covered. ✓

### 2. Placeholder Scan

No "TBD", "TODO", or incomplete code blocks. ✓

### 3. Type Consistency

- `SideEffectFunctionNode(pod=..., input_stream=..., label=...)` — used consistently in compile(), as_pipeline(), and to_invocations().
- `SideEffectFunctionJobNode(pod=..., input_stream=..., label=...)` — same constructor, used in compile() for PipelineJob.
- `_build_ctx_and_record_id()` returns `(InvocationContext, ContentHash, bytes)` — used identically in `execute()` and `async_execute()`.
- `node_type = "side_effect_function"` — used in `SideEffectFunctionNode` (and inherited by `SideEffectFunctionJobNode`), `SideEffectFunctionPodStream`, and checked by `is_side_effect_function_node()`. ✓
