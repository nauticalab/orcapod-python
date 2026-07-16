# ITL-532 Unified FunctionPod Design — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate the separate `SideEffectFunctionPod` class hierarchy by making `InvocationContext` injection an optional feature of `FunctionPod` controlled by `ctx_arg_name`. Drop PR #230, start fresh.

**Architecture:** When `ctx_arg_name` is set on a `FunctionPod`, the URI is prefixed with `"side_effect_function"`, `node_type` returns `"side_effect_function"`, and `process_data()` builds an `InvocationContext` per row and injects it as an extra kwarg. `FunctionJobNode.execute()` and `async_execute()` gain `run_id: str | None = None` threaded to ctx building. The entire `src/orcapod/core/side_effect_function/` module is deleted; the `side_effect_function_pod` decorator becomes a thin alias in `function_pod.py`.

**Tech Stack:** Python 3.11+, PyArrow, orcapod core (TraceableBase, CachedFunctionPod, FunctionJobNode, InvocationContext), uuid_utils, uv, pytest.

---

## File Map

| File | Change |
|------|--------|
| `src/orcapod/core/function_pod.py` | Modify — ctx_arg_name support, `from_fn` classmethod, `side_effect_function_pod` decorator |
| `src/orcapod/core/cached_function_pod.py` | Modify — thread `run_id` through `process_data` / `async_process_data` |
| `src/orcapod/core/nodes/function_node.py` | Modify — `node_type` property, `run_id` on `execute` / `async_execute` |
| `src/orcapod/protocols/node_protocols.py` | Modify — remove `SideEffectFunctionNodeProtocol`, add `run_id` to `FunctionNodeProtocol` |
| `src/orcapod/pipeline/sync_orchestrator.py` | Modify — collapse SEF dispatch into function_node branch |
| `src/orcapod/pipeline/async_orchestrator.py` | Modify — same |
| `src/orcapod/pipeline/base.py` | Modify — remove SEF abstract property, recorder, compile branch |
| `src/orcapod/pipeline/pod_invocation.py` | Modify — remove `SideEffectFunctionInvocation` |
| `src/orcapod/pipeline/job.py` | Modify — remove SEF class attr, distribute, as_pipeline branches |
| `src/orcapod/pipeline/graph.py` | Modify — remove SEF import, class attr, save() branch |
| `src/orcapod/core/tracker.py` | Modify — remove `record_side_effect_function_pod_invocation` |
| `src/orcapod/protocols/core_protocols/trackers.py` | Modify — remove same method from protocols |
| `src/orcapod/protocols/core_protocols/side_effect_function_pod.py` | **Delete** |
| `src/orcapod/protocols/core_protocols/__init__.py` | Modify — remove SEF protocol import |
| `src/orcapod/__init__.py` | Modify — remove SEF class exports, keep `side_effect_function_pod` |
| `src/orcapod/core/side_effect_function/` | **Delete entire directory** |
| `tests/test_core/side_effect_function/test_side_effect_function_pod.py` | Modify — use `FunctionPod.from_fn(..., ctx_arg_name="ctx")` |

---

### Task 1: Add ctx support to `_FunctionPodBase` and `FunctionPod`

This is the heart of the change. All ctx-aware logic from `SideEffectFunctionPod` moves into `_FunctionPodBase`/`FunctionPod`.

**Files:**
- Modify: `src/orcapod/core/function_pod.py`

- [ ] **Step 1: Add `_strip_ctx_from_fn` helper and import `inspect` at module level**

In `function_pod.py`, after the existing imports, add:

```python
import inspect

def _strip_ctx_from_fn(fn: Callable, ctx_arg_name: str) -> Callable:
    """Return a wrapper of ``fn`` with ``ctx_arg_name`` removed from its signature.

    Used for schema inference: the wrapper is passed to ``PythonDataFunction``
    so the context parameter is invisible to schema extraction.  The original
    ``fn`` is stored on the pod for actual invocation.

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
```

Note: `functools` is already imported at the top of the file. Add `import inspect` near the other stdlib imports.

- [ ] **Step 2: Extend `_FunctionPodBase.__init__` to accept ctx params**

Find the `_FunctionPodBase.__init__` method (currently ending around line 99). Modify its signature and body:

```python
def __init__(
    self,
    data_function: DataFunctionProtocol,
    tracker_manager: TrackerManagerProtocol | None = None,
    label: str | None = None,
    data_context: str | contexts.DataContext | None = None,
    config: OrcapodConfig | None = None,
    ctx_arg_name: str | None = None,
    _original_fn: Callable | None = None,
) -> None:
    super().__init__(
        label=label,
        data_context=data_context,
        config=config,
    )
    self.tracker_manager = tracker_manager or DEFAULT_TRACKER_MANAGER
    self._data_function = data_function
    self._post_run_hooks: list[PostRunHook] = []
    # ctx-injection support
    self._ctx_arg_name: str | None = ctx_arg_name
    self._original_fn: Callable | None = _original_fn
    self.data_context.type_converter.ensure_types_registered_for_schemas(
        data_function.input_data_schema,
        data_function.output_data_schema,
    )
```

- [ ] **Step 3: Override `uri` property and `identity_structure()` in `_FunctionPodBase`**

Add these overrides to `_FunctionPodBase` (after `computed_label`):

```python
@property
def uri(self) -> tuple[str, ...]:
    """Canonical URI, prefixed with ``"side_effect_function"`` when ctx is set."""
    base = self.data_function.uri
    if self._ctx_arg_name is not None:
        return ("side_effect_function",) + base
    return base

def identity_structure(self) -> Any:
    base = self.data_function.identity_structure()
    if self._ctx_arg_name is not None:
        return (base, self._ctx_arg_name)
    return base
```

Note: the existing `_FunctionPodBase` has `uri` and `identity_structure` defined — replace them (they're currently at lines ~129–131 in `function_pod.py`).

- [ ] **Step 4: Add InvocationContext helper methods to `_FunctionPodBase`**

Add these methods to `_FunctionPodBase` (after `pipeline_identity_structure`):

```python
def _build_invocation_context(
    self,
    tag: TagProtocol,
    data: DataProtocol,
    *,
    run_id: str | None = None,
) -> Any:
    """Build a per-row ``InvocationContext`` for ctx-aware pods.

    Uses the same preimage as ``SideEffectJobNode``: system-tag columns
    + ``INPUT_DATA_HASH_COL`` + ``NODE_CONTENT_HASH_COL`` +
    recomputation index 0.

    Args:
        tag: The input tag for this row.
        data: The input data for this row.
        run_id: Pipeline run identifier, or ``None`` in standalone mode.

    Returns:
        An ``InvocationContext`` instance.
    """
    import pyarrow as pa
    from orcapod.side_effects import InvocationContext, _SIDE_EFFECT_RECOMPUTATION_INDEX_COL

    preimage = (
        tag.as_table(columns={"system_tags": True})
        .append_column(
            constants.INPUT_DATA_HASH_COL,
            pa.array([data.content_hash().to_string()], type=pa.large_string()),
        )
        .append_column(
            constants.NODE_CONTENT_HASH_COL,
            pa.array([self.content_hash().to_string()], type=pa.large_string()),
        )
        .append_column(
            _SIDE_EFFECT_RECOMPUTATION_INDEX_COL,
            pa.array([0], type=pa.int32()),
        )
    )
    record_id_hash = self.data_context.arrow_hasher.hash_table(preimage)

    return InvocationContext(
        pod_name=self.label,
        pipeline_run_id=run_id,
        _pipeline_hash_ch=self.pipeline_hash(),
        _record_id_hash_ch=record_id_hash,
    )

def _call_with_ctx(self, data: DataProtocol, ctx: Any) -> Any:
    """Call the original function with data kwargs and InvocationContext.

    Args:
        data: Input data row.
        ctx: Per-row ``InvocationContext``.

    Returns:
        Raw function return value.

    Raises:
        ValueError: If ``_ctx_arg_name`` collides with a data column name.
    """
    assert self._ctx_arg_name is not None and self._original_fn is not None
    data_dict = data.as_dict()
    if self._ctx_arg_name in data_dict:
        raise ValueError(
            f"ctx_arg_name {self._ctx_arg_name!r} collides with data column "
            f"of the same name. Choose a different ctx_arg_name or rename the column."
        )
    kwargs = {self._ctx_arg_name: ctx, **data_dict}
    if inspect.iscoroutinefunction(self._original_fn):
        return self._call_async_sync(kwargs)
    return self._original_fn(**kwargs)

def _call_async_sync(self, kwargs: dict[str, Any]) -> Any:
    """Run the async original function synchronously.

    Args:
        kwargs: Keyword arguments to pass to ``self._original_fn``.

    Returns:
        The coroutine's return value.
    """
    assert self._original_fn is not None
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(self._original_fn(**kwargs))

    import concurrent.futures
    with concurrent.futures.ThreadPoolExecutor(1) as executor:
        future = executor.submit(lambda: asyncio.run(self._original_fn(**kwargs)))
        return future.result()

def _build_output_data(self, raw_output: Any) -> DataProtocol:
    """Wrap raw function return value in a ``Data`` object with source info.

    Args:
        raw_output: Raw return value from the user function.

    Returns:
        A ``Data`` with source info and a new UUID.
    """
    import uuid
    from uuid_utils import uuid7
    from orcapod.core.datagrams import Data
    from orcapod.core.data_function import parse_function_outputs

    output_keys = list(self._data_function.output_data_schema.keys())
    output_dict = parse_function_outputs(output_keys, raw_output)
    new_uuid = uuid.UUID(bytes=uuid7().bytes)
    source_info = {
        k: f"{':'.join(self.uri)}::{new_uuid.hex}::{k}" for k in output_dict
    }
    return Data(
        output_dict,
        source_info=source_info,
        record_uuid=new_uuid,
        python_schema=self._data_function.output_data_schema,
        data_context=self.data_context,
    )
```

- [ ] **Step 5: Modify `process_data` and `async_process_data` to handle ctx path**

Replace the current `process_data` method in `_FunctionPodBase`:

```python
def process_data(
    self,
    tag: TagProtocol,
    data: DataProtocol,
    *,
    logger: DataExecutionLoggerProtocol | None = None,
    run_id: str | None = None,
) -> tuple[TagProtocol, DataProtocol | None]:
    """Process a single data using the pod's data function.

    When ``_ctx_arg_name`` is set, builds a per-row ``InvocationContext``
    and injects it as an extra kwarg to the original function.

    Args:
        tag: The tag associated with the data.
        data: The input data to process.
        logger: Optional ``DataExecutionLoggerProtocol`` for I/O capture.
        run_id: Pipeline run identifier forwarded to ``InvocationContext``.
            Only used when ``_ctx_arg_name`` is set.

    Returns:
        A ``(tag, output_data)`` tuple; ``output_data`` is ``None`` if
        the function filters the data out.
    """
    if self._ctx_arg_name is not None:
        ctx = self._build_invocation_context(tag, data, run_id=run_id)
        raw = self._call_with_ctx(data, ctx)
        return tag, self._build_output_data(raw)
    result = self.data_function.call(data, logger=logger)
    return tag, result
```

Replace `async_process_data`:

```python
async def async_process_data(
    self,
    tag: TagProtocol,
    data: DataProtocol,
    *,
    logger: DataExecutionLoggerProtocol | None = None,
    run_id: str | None = None,
) -> tuple[TagProtocol, DataProtocol | None]:
    """Async counterpart of ``process_data``."""
    if self._ctx_arg_name is not None:
        ctx = self._build_invocation_context(tag, data, run_id=run_id)
        # _call_with_ctx handles both sync and async originals
        raw = self._call_with_ctx(data, ctx)
        return tag, self._build_output_data(raw)
    result = await self.data_function.async_call(data, logger=logger)
    return tag, result
```

- [ ] **Step 6: Thread `run_id` through `_invoke_with_hooks` and `_async_invoke_with_hooks`**

Replace `_invoke_with_hooks` signature (the body calls `process_data`, so add `run_id` to it):

```python
def _invoke_with_hooks(
    self,
    tag: TagProtocol,
    data: DataProtocol,
    *,
    logger: DataExecutionLoggerProtocol | None = None,
    run_id: str | None = None,
) -> tuple[TagProtocol, DataProtocol | None]:
    if not self._post_run_hooks:
        return self.process_data(tag, data, logger=logger, run_id=run_id)

    started_at = datetime.now(timezone.utc)
    out_tag = tag
    output_data: DataProtocol | None = None

    try:
        out_tag, output_data = self.process_data(tag, data, logger=logger, run_id=run_id)
    except Exception as exc:
        finished_at = datetime.now(timezone.utc)
        self._fire_post_run_hooks(
            self._build_post_run_payload(
                tag, data, None, started_at, finished_at,
                InvocationStatus.ERROR, exc,
            )
        )
        raise
    finished_at = datetime.now(timezone.utc)
    self._fire_post_run_hooks(
        self._build_post_run_payload(
            tag, data, output_data, started_at, finished_at,
            InvocationStatus.COMPUTED, None,
        )
    )
    return out_tag, output_data
```

Replace `_async_invoke_with_hooks` signature similarly:

```python
async def _async_invoke_with_hooks(
    self,
    tag: TagProtocol,
    data: DataProtocol,
    *,
    logger: DataExecutionLoggerProtocol | None = None,
    run_id: str | None = None,
) -> tuple[TagProtocol, DataProtocol | None]:
    if not self._post_run_hooks:
        return await self.async_process_data(tag, data, logger=logger, run_id=run_id)

    started_at = datetime.now(timezone.utc)
    out_tag = tag
    output_data: DataProtocol | None = None
    try:
        out_tag, output_data = await self.async_process_data(
            tag, data, logger=logger, run_id=run_id
        )
    except Exception as exc:
        finished_at = datetime.now(timezone.utc)
        self._fire_post_run_hooks(
            self._build_post_run_payload(
                tag, data, None, started_at, finished_at,
                InvocationStatus.ERROR, exc,
            )
        )
        raise
    finished_at = datetime.now(timezone.utc)
    self._fire_post_run_hooks(
        self._build_post_run_payload(
            tag, data, output_data, started_at, finished_at,
            InvocationStatus.COMPUTED, None,
        )
    )
    return out_tag, output_data
```

- [ ] **Step 7: Add `run_id` to `_FunctionPodBase.async_execute`**

The `async_execute` method (currently around line 396) calls `_async_invoke_with_hooks`. Add `run_id=None` and thread it through:

```python
async def async_execute(
    self,
    inputs: Sequence[ReadableChannel[tuple[TagProtocol, DataProtocol]]],
    output: WritableChannel[tuple[TagProtocol, DataProtocol]],
    pipeline_config: PipelineConfig | None = None,
    *,
    observer: ExecutionObserverProtocol | None = None,
    run_id: str | None = None,
) -> None:
    # ... existing docstring ...
    from orcapod.pipeline.observer import NoOpObserver

    try:
        pipeline_config = pipeline_config or PipelineConfig()
        max_concurrency = resolve_concurrency(self.pod_config, pipeline_config)
        obs = observer if observer is not None else NoOpObserver()
        pod_label = self.label

        sem = (
            asyncio.Semaphore(max_concurrency)
            if max_concurrency is not None
            else None
        )

        async def process_one(tag: TagProtocol, data: DataProtocol) -> None:
            obs.on_data_start(pod_label, tag, data)
            pkt_logger = obs.create_data_logger(tag, data)
            try:
                out_tag, result_data = await self._async_invoke_with_hooks(
                    tag, data, logger=pkt_logger, run_id=run_id
                )
            except Exception as exc:
                logger.debug(
                    "Data processing failed, skipping: %s", exc, exc_info=True
                )
                obs.on_data_crash(pod_label, tag, data, exc)
            else:
                obs.on_data_end(pod_label, tag, data, result_data, cached=False)
                if result_data is not None:
                    await output.send((out_tag, result_data))
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
```

- [ ] **Step 8: Update `FunctionPod.__init__` and add `from_fn` classmethod + `side_effect_function_pod` decorator**

Update `FunctionPod.__init__` to accept and forward the new params:

```python
class FunctionPod(_FunctionPodBase):
    def __init__(
        self,
        data_function: DataFunctionProtocol,
        pod_config: PodConfig | None = None,
        ctx_arg_name: str | None = None,
        _original_fn: Callable | None = None,
        **kwargs,
    ) -> None:
        super().__init__(
            data_function,
            ctx_arg_name=ctx_arg_name,
            _original_fn=_original_fn,
            **kwargs,
        )
        self._pod_config = pod_config or PodConfig()
```

Add `from_fn` classmethod after the existing constructor:

```python
@classmethod
def from_fn(
    cls,
    fn: Callable,
    output_keys: list[str] | str,
    *,
    ctx_arg_name: str | None = None,
    name: str | None = None,
    version: str = "v1.0",
    pod_config: PodConfig | None = None,
    label: str | None = None,
    **kwargs,
) -> "FunctionPod":
    """Construct a ``FunctionPod`` directly from a callable.

    Args:
        fn: The user function.
        output_keys: Output column key(s).
        ctx_arg_name: If set, strip this parameter from schema inference
            and inject an ``InvocationContext`` per row under this name.
        name: Optional canonical function name override.
        version: Version string (default ``"v1.0"``).
        pod_config: Optional per-pod config.
        label: Optional display label.
        **kwargs: Forwarded to ``_FunctionPodBase.__init__``.

    Returns:
        A new ``FunctionPod``.

    Raises:
        ValueError: If ``ctx_arg_name`` is set but not in ``fn``'s signature.
    """
    if ctx_arg_name is not None:
        original_fn = fn
        stripped = _strip_ctx_from_fn(fn, ctx_arg_name)
        data_function = PythonDataFunction(
            stripped,
            output_keys=output_keys,
            function_name=name or getattr(fn, "__name__", "unknown"),
            version=version,
            label=label,
        )
        return cls(
            data_function=data_function,
            pod_config=pod_config,
            ctx_arg_name=ctx_arg_name,
            _original_fn=original_fn,
            label=label,
            **kwargs,
        )
    data_function = PythonDataFunction(
        fn,
        output_keys=output_keys,
        function_name=name or getattr(fn, "__name__", "unknown"),
        version=version,
        label=label,
    )
    return cls(
        data_function=data_function,
        pod_config=pod_config,
        label=label,
        **kwargs,
    )
```

Add the `side_effect_function_pod` decorator near the bottom of `function_pod.py` (after the existing `function_pod` decorator):

```python
def side_effect_function_pod(
    fn: Callable | None = None,
    *,
    output_keys: list[str] | str,
    ctx_arg_name: str = "ctx",
    name: str | None = None,
    version: int = 1,
    pod_config: PodConfig | None = None,
) -> "FunctionPod | Callable":
    """Decorator wrapping a callable as a ctx-aware ``FunctionPod``.

    Equivalent to ``FunctionPod.from_fn(fn, output_keys=..., ctx_arg_name=...)``.
    The decorated object is the ``FunctionPod`` itself (not a wrapper function),
    so it can be called directly as a pod.

    Args:
        fn: Optional function — if provided, decorates immediately.
        output_keys: Output column key(s).
        ctx_arg_name: Name of the ``InvocationContext`` parameter (default ``"ctx"``).
        name: Optional canonical function name override.
        version: Version integer for the URI (default 1).
        pod_config: Optional per-pod configuration.

    Returns:
        A ``FunctionPod`` with ``ctx_arg_name`` set, or a decorator if ``fn``
        is not provided.

    Raises:
        ValueError: If ``ctx_arg_name`` is not in ``fn``'s signature.
    """
    def decorator(func: Callable) -> FunctionPod:
        return FunctionPod.from_fn(
            func,
            output_keys=output_keys,
            ctx_arg_name=ctx_arg_name,
            name=name,
            version=f"v{version}.0",
            pod_config=pod_config,
        )

    if fn is not None:
        return decorator(fn)
    return decorator
```

- [ ] **Step 9: Update `FunctionPod.process()` to use `record_function_pod_invocation` when ctx is set**

The current `FunctionPod.process()` already calls `self.tracker_manager.record_function_pod_invocation(self, input_stream, label=label)`. This is correct — ctx-aware pods record the same invocation type. No change needed here. ✓

- [ ] **Step 10: Run tests to verify the new ctx code paths work**

```bash
uv run pytest tests/test_core/function_pod/ -v
```

Expected: all existing tests still pass (no regressions). No new tests yet (coming in Task 13).

---

### Task 2: Thread `run_id` through `CachedFunctionPod`

**Files:**
- Modify: `src/orcapod/core/cached_function_pod.py`

- [ ] **Step 1: Add `run_id=None` to `CachedFunctionPod.process_data`**

```python
def process_data(
    self,
    tag: TagProtocol,
    data: DataProtocol,
    *,
    logger: DataExecutionLoggerProtocol | None = None,
    run_id: str | None = None,
) -> tuple[TagProtocol, DataProtocol | None]:
    # ... existing docstring ...
    cached = self._cache.lookup(data)
    if cached is not None:
        module_logger.info("Pod-level cache hit")
        cached = cached.with_meta_columns(**{self.RESULT_COMPUTED_FLAG: False})
        return tag, cached

    tag, output = self._function_pod.process_data(tag, data, logger=logger, run_id=run_id)
    if output is not None:
        pf = self._function_pod.data_function
        var_dg = Datagram(
            pf.get_function_variation_data(),
            python_schema=pf.get_function_variation_data_schema(),
            data_context=pf.data_context,
        )
        exec_dg = Datagram(
            pf.get_execution_data(),
            python_schema=pf.get_execution_data_schema(),
            data_context=pf.data_context,
        )
        self._cache.store(data, output, var_dg, exec_dg)
        output = output.with_meta_columns(**{self.RESULT_COMPUTED_FLAG: True})
    return tag, output
```

- [ ] **Step 2: Add `run_id=None` to `CachedFunctionPod.async_process_data`**

```python
async def async_process_data(
    self,
    tag: TagProtocol,
    data: DataProtocol,
    *,
    logger: DataExecutionLoggerProtocol | None = None,
    run_id: str | None = None,
) -> tuple[TagProtocol, DataProtocol | None]:
    # ... existing docstring ...
    cached = self._cache.lookup(data)
    if cached is not None:
        module_logger.info("Pod-level cache hit")
        cached = cached.with_meta_columns(**{self.RESULT_COMPUTED_FLAG: False})
        return tag, cached

    tag, output = await self._function_pod.async_process_data(
        tag, data, logger=logger, run_id=run_id
    )
    if output is not None:
        pf = self._function_pod.data_function
        var_dg = Datagram(
            pf.get_function_variation_data(),
            python_schema=pf.get_function_variation_data_schema(),
            data_context=pf.data_context,
        )
        exec_dg = Datagram(
            pf.get_execution_data(),
            python_schema=pf.get_execution_data_schema(),
            data_context=pf.data_context,
        )
        self._cache.store(data, output, var_dg, exec_dg)
        output = output.with_meta_columns(**{self.RESULT_COMPUTED_FLAG: True})
    return tag, output
```

- [ ] **Step 3: Run tests**

```bash
uv run pytest tests/test_core/function_pod/ tests/test_core/data_function/ -v
```

Expected: all pass.

---

### Task 3: Make `FunctionNodeBase.node_type` Dynamic + Add `run_id` to `FunctionJobNode`

**Files:**
- Modify: `src/orcapod/core/nodes/function_node.py`

- [ ] **Step 1: Change `node_type` from class variable to instance variable**

Currently `FunctionNodeBase` has:
```python
class FunctionNodeBase(StreamBase):
    node_type = "function"
```

Replace with an instance attribute set in `__init__`. Change `FunctionNodeBase.__init__` body to include:

```python
# Derive node_type from the pod's ctx_arg_name (supports is_side_effect_function_node check)
self._node_type: str = (
    "side_effect_function"
    if getattr(function_pod, "_ctx_arg_name", None) is not None
    else "function"
)
```

And add a `node_type` property:

```python
@property
def node_type(self) -> str:
    """Return ``"side_effect_function"`` when the pod injects ``InvocationContext``, else ``"function"``."""
    return self._node_type
```

Remove the class-level `node_type = "function"` line.

- [ ] **Step 2: Update `FunctionJobNode.from_descriptor()` to preserve `_node_type`**

In `from_descriptor()`, after setting other stored fields, add:
```python
# node_type comes from the stored URI: uri[0] == "side_effect_function" when ctx-aware
node._node_type = (
    "side_effect_function"
    if descriptor.get("node_uri", [""])[0] == "side_effect_function"
    else "function"
)
```

Search for the section in `from_descriptor()` that populates `_stored_*` fields and add this after them.

- [ ] **Step 3: Add `run_id=None` to `FunctionJobNode.execute()`**

Find `FunctionJobNode.execute()` (around line 1110). Change its signature:

```python
def execute(
    self,
    input_stream: StreamProtocol,
    *,
    observer: ExecutionObserverProtocol | None = None,
    error_policy: Literal["continue", "fail_fast"] = "continue",
    run_id: str | None = None,
) -> list[tuple[TagProtocol, DataProtocol]]:
```

And change the `_process_data_internal` call inside it to:
```python
tag_out, result = self._process_data_internal(
    tag, data, logger=pkt_logger, run_id=run_id
)
```

- [ ] **Step 4: Thread `run_id` through `_process_data_internal`**

Change `_process_data_internal` signature:

```python
def _process_data_internal(
    self,
    tag: TagProtocol,
    data: DataProtocol,
    *,
    logger: DataExecutionLoggerProtocol | None = None,
    run_id: str | None = None,
) -> tuple[TagProtocol, DataProtocol | None]:
```

Inside, thread `run_id` to every `process_data` call:

```python
if ephemeral_result:
    ...
    tag_out, output_data = self._ephemeral_cached_pod.process_data(
        tag, data, logger=logger, run_id=run_id
    )
    ...
elif self._cached_function_pod is not None:
    tag_out, output_data = self._cached_function_pod.process_data(
        tag, data, logger=logger, run_id=run_id
    )
    ...
else:
    tag_out, output_data = self._function_pod.process_data(
        tag, data, logger=logger, run_id=run_id
    )
```

- [ ] **Step 5: Add `run_id=None` to `FunctionJobNode.async_execute()`**

Find `async_execute` (around line 2047). Change signature:

```python
async def async_execute(
    self,
    input_channel: ReadableChannel[tuple[TagProtocol, DataProtocol]],
    output: WritableChannel[tuple[TagProtocol, DataProtocol]],
    *,
    observer: ExecutionObserverProtocol | None = None,
    run_id: str | None = None,
) -> None:
```

In the DB path, pass `run_id` to `execution_pod.async_execute(...)`:
```python
tg.create_task(
    execution_pod.async_execute(
        [compute_channel.reader],
        result_channel.writer,
        observer=_NodeLabelObserver(),
        run_id=run_id,
    )
)
```

In the no-DB path, pass `run_id` to `self._function_pod.async_execute(...)`:
```python
await self._function_pod.async_execute(
    [input_channel], output, observer=ctx_obs, run_id=run_id
)
```

- [ ] **Step 6: Run tests**

```bash
uv run pytest tests/test_core/function_pod/ -v
```

Expected: all pass.

- [ ] **Step 7: Commit**

```bash
git add src/orcapod/core/function_pod.py src/orcapod/core/cached_function_pod.py src/orcapod/core/nodes/function_node.py
git commit -m "feat(function-pod): add ctx_arg_name support, run_id threading, and side_effect_function_pod decorator"
```

---

### Task 4: Update `FunctionNodeProtocol` in `node_protocols.py`

**Files:**
- Modify: `src/orcapod/protocols/node_protocols.py`

- [ ] **Step 1: Remove `SideEffectFunctionNodeProtocol` and add `run_id` to `FunctionNodeProtocol`**

In `node_protocols.py`:

1. Add `run_id: str | None = None` to `FunctionNodeProtocol.execute()`:
```python
def execute(
    self,
    input_stream: StreamProtocol,
    *,
    observer: ExecutionObserverProtocol | None = None,
    error_policy: Literal["continue", "fail_fast"] = "continue",
    run_id: str | None = None,
) -> list[tuple[TagProtocol, DataProtocol]]: ...
```

2. Add `run_id: str | None = None` to `FunctionNodeProtocol.async_execute()`:
```python
async def async_execute(
    self,
    input_channel: ReadableChannel[tuple[TagProtocol, DataProtocol]],
    output: WritableChannel[tuple[TagProtocol, DataProtocol]],
    *,
    observer: ExecutionObserverProtocol | None = None,
    run_id: str | None = None,
) -> None: ...
```

3. Delete the entire `SideEffectFunctionNodeProtocol` class (lines 174–205).

4. Keep `is_side_effect_function_node()` TypeGuard — it works because `FunctionJobNode` with ctx set returns `node_type == "side_effect_function"`.

- [ ] **Step 2: Run tests**

```bash
uv run pytest tests/ -x -q
```

Expected: all pass (or only existing failures in SEF tests which we'll fix in Task 13).

---

### Task 5: Collapse Orchestrator Dispatch

**Files:**
- Modify: `src/orcapod/pipeline/sync_orchestrator.py`
- Modify: `src/orcapod/pipeline/async_orchestrator.py`

- [ ] **Step 1: Merge SEF branch into function_node branch in sync orchestrator**

In `sync_orchestrator.py`, find the dispatch loop. The current code has:
```python
elif is_function_node(node):
    ...
    buffers[node] = node.execute(
        input_stream,
        observer=effective_observer,
        error_policy=self._error_policy,
    )
...
elif is_side_effect_function_node(node):
    ...
    buffers[node] = node.execute(
        input_stream,
        observer=effective_observer,
        run_id=run_id,
    )
```

After the change, both node types are `FunctionJobNode` with the same `execute()` signature. Merge by adding `run_id` to the function_node branch and removing the SEF branch:

```python
elif is_function_node(node) or is_side_effect_function_node(node):
    upstream_buf = self._gather_upstream(node, graph, buffers)
    upstream_node = list(graph.predecessors(node))[0]
    input_stream = self._materialize_as_stream(upstream_buf, upstream_node)
    buffers[node] = node.execute(
        input_stream,
        observer=effective_observer,
        error_policy=self._error_policy,
        run_id=run_id,
    )
```

Remove the `is_side_effect_function_node` import from the top of the file.

- [ ] **Step 2: Merge SEF branch into function_node branch in async orchestrator**

In `async_orchestrator.py`, the current code has:
```python
elif is_function_node(node):
    predecessors = in_edges.get(node, [])
    ...
    input_reader = edge_readers[(predecessors[0], node)]
    tg.create_task(
        node.async_execute(
            input_reader, writer, observer=effective_observer
        )
    )
...
elif is_side_effect_function_node(node):
    predecessors = in_edges.get(node, [])
    ...
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

Note: The SEF branch passes `[input_reader]` (a list) but `FunctionJobNode.async_execute` takes a single `input_channel`. Merge into the function_node branch, passing `run_id` and a single reader:

```python
elif is_function_node(node) or is_side_effect_function_node(node):
    predecessors = in_edges.get(node, [])
    if len(predecessors) != 1:
        raise ValueError(
            f"FunctionNode expects exactly 1 upstream, got {len(predecessors)}"
        )
    input_reader = edge_readers[(predecessors[0], node)]
    tg.create_task(
        node.async_execute(
            input_reader, writer, observer=effective_observer, run_id=run_id
        )
    )
```

Remove the `is_side_effect_function_node` import from the top of the file.

- [ ] **Step 3: Run orchestrator tests**

```bash
uv run pytest tests/ -x -q -k "orchestrator or pipeline"
```

Expected: pass (or only failures in SEF-specific tests).

---

### Task 6: Update `pipeline/base.py`

Remove all SEF-specific recording, compile, and abstract property code.

**Files:**
- Modify: `src/orcapod/pipeline/base.py`

- [ ] **Step 1: Remove SEF imports and abstract property**

1. Remove `SideEffectFunctionInvocation` from the imports at the top.
2. Remove the `side_effect_function_node_class` abstract property:
```python
# DELETE this entire block:
@property
@abstractmethod
def side_effect_function_node_class(self) -> type:
    """Node class for side-effect-function pod invocations."""
    ...
```

- [ ] **Step 2: Remove `record_side_effect_function_pod_invocation`**

Delete the entire `record_side_effect_function_pod_invocation()` method from `AbstractPipelineBase`.

- [ ] **Step 3: Remove SEF branch from `compile()`**

In `compile()`, find:
```python
elif isinstance(inv, SideEffectFunctionInvocation):
    node_map[key] = self.side_effect_function_node_class(
        pod=inv.pod,
        input_stream=upstream_nodes[0],
        label=inv.label,
    )
```

Delete this `elif` block entirely. `FunctionInvocation` now covers ctx-aware pods too (since `FunctionPod.process()` calls `record_function_pod_invocation` regardless).

- [ ] **Step 4: Run tests**

```bash
uv run pytest tests/ -x -q
```

---

### Task 7: Update `pipeline/pod_invocation.py`

**Files:**
- Modify: `src/orcapod/pipeline/pod_invocation.py`

- [ ] **Step 1: Remove `SideEffectFunctionInvocation` class**

Delete the entire `SideEffectFunctionInvocation` class (the last class in the file) and its `SideEffectFunctionPodProtocol` import.

```python
# DELETE from TYPE_CHECKING imports:
from orcapod.protocols.core_protocols import (
    ...,
    SideEffectFunctionPodProtocol,  # <- remove this line
    ...
)

# DELETE the entire class:
class SideEffectFunctionInvocation(PodInvocation):
    ...
```

- [ ] **Step 2: Run tests**

```bash
uv run pytest tests/ -x -q
```

---

### Task 8: Update `pipeline/job.py`

**Files:**
- Modify: `src/orcapod/pipeline/job.py`

- [ ] **Step 1: Remove SEF imports and class attribute**

1. Remove the import:
```python
# DELETE:
from orcapod.core.side_effect_function.side_effect_function_pod import (
    SideEffectFunctionJobNode,
    SideEffectFunctionNode,
)
```

2. Remove the class attribute:
```python
# DELETE:
side_effect_function_node_class = SideEffectFunctionJobNode
```

- [ ] **Step 2: Remove SEF branch from `_distribute_databases()`**

Find:
```python
elif isinstance(node, SideEffectFunctionJobNode):
    node.attach_databases(result_database=result_db)
```

Delete this `elif` block. `FunctionJobNode` with ctx-aware pods is now wired the same as regular `FunctionJobNode`.

- [ ] **Step 3: Remove SEF branch from `as_pipeline()`**

Find:
```python
elif isinstance(job_node, SideEffectFunctionJobNode):
    upstream_bp_hash = job_id_to_bp_hash[id(job_node._input_stream)]
    node_map[node_hash] = SideEffectFunctionNode(
        pod=job_node._pod,
        input_stream=node_map[upstream_bp_hash],
        label=job_node._label,
    )
```

Delete this `elif` block. Ctx-aware `FunctionJobNode` are converted to `FunctionNode` by the existing `FunctionJobNode` branch (since `job_node._function_pod` holds the ctx-aware `FunctionPod`).

- [ ] **Step 4: Run tests**

```bash
uv run pytest tests/ -x -q
```

---

### Task 9: Update `pipeline/graph.py`

**Files:**
- Modify: `src/orcapod/pipeline/graph.py`

- [ ] **Step 1: Remove SEF import and class attribute**

1. Remove:
```python
from orcapod.core.side_effect_function.side_effect_function_pod import SideEffectFunctionNode
```

2. Remove:
```python
side_effect_function_node_class = SideEffectFunctionNode
```

- [ ] **Step 2: Update `save()` to remove SEF node_type check**

Find:
```python
case _ if node.node_type == "side_effect_function":
    raise NotImplementedError(
        f"Pipeline.save() does not support SideEffectFunctionNode ..."
    )
```

Delete this `case` block. Ctx-aware `FunctionNode` objects are now serialized via the `FunctionNode()` case (which already exists and handles `node._function_pod.to_config()`).

Note: `FunctionNode.to_config()` currently exists. For ctx-aware `FunctionPod`, `to_config()` should include `ctx_arg_name`. We'll need to update `FunctionPod.to_config()` and `FunctionPod.from_config()` to handle `ctx_arg_name`. Add to `FunctionPod.to_config()`:
```python
def to_config(self) -> dict[str, Any]:
    config: dict[str, Any] = {
        "uri": list(self.uri),
        "data_function": self.data_function.to_config(),
        "pod_config": None,
        "ctx_arg_name": self._ctx_arg_name,  # NEW
    }
    if self._pod_config.max_concurrency is not None:
        config["pod_config"] = {"max_concurrency": self._pod_config.max_concurrency}
    return config
```

For `from_config()`: currently ctx-aware pods require a live callable; `from_config()` is only called for deserialization (read-only stubs), so `ctx_arg_name` can be stored but `_original_fn` stays None. The deserialized node won't be able to compute, only serve cached results — which is consistent with `LoadStatus.READ_ONLY` behavior. Update `from_config()`:
```python
@classmethod
def from_config(cls, config: dict[str, Any], *, fallback_to_proxy: bool = False) -> "FunctionPod":
    from orcapod.pipeline.serialization import resolve_data_function_from_config
    
    pf_config = config["data_function"]
    data_function = resolve_data_function_from_config(pf_config, fallback_to_proxy=fallback_to_proxy)
    
    pod_config = None
    if config.get("pod_config") is not None:
        pod_config = PodConfig(**config["pod_config"])
    
    return cls(
        data_function=data_function,
        pod_config=pod_config,
        ctx_arg_name=config.get("ctx_arg_name"),
        # _original_fn stays None — this is a read-only stub
    )
```

- [ ] **Step 3: Run tests**

```bash
uv run pytest tests/ -x -q
```

---

### Task 10: Update `tracker.py` and Protocol Files

**Files:**
- Modify: `src/orcapod/core/tracker.py`
- Modify: `src/orcapod/protocols/core_protocols/trackers.py`
- Modify: `src/orcapod/protocols/core_protocols/__init__.py`
- Delete: `src/orcapod/protocols/core_protocols/side_effect_function_pod.py`

- [ ] **Step 1: Remove `record_side_effect_function_pod_invocation` from `BasicTrackerManager`**

In `tracker.py`, delete:
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

- [ ] **Step 2: Remove from `AutoRegisteringContextBasedTracker`**

Delete the abstract method:
```python
@abstractmethod
def record_side_effect_function_pod_invocation(
    self,
    pod: Any,
    input_stream: cp.StreamProtocol,
    label: str | None = None,
) -> None: ...
```

- [ ] **Step 3: Remove from `trackers.py` protocol**

In `src/orcapod/protocols/core_protocols/trackers.py`, remove `record_side_effect_function_pod_invocation` from both protocol classes (around line 112 and 241).

- [ ] **Step 4: Delete `side_effect_function_pod.py` and update `__init__.py`**

```bash
rm src/orcapod/protocols/core_protocols/side_effect_function_pod.py
```

In `src/orcapod/protocols/core_protocols/__init__.py`, remove:
```python
from .side_effect_function_pod import SideEffectFunctionPodProtocol
```

And remove `SideEffectFunctionPodProtocol` from `__all__` if present.

- [ ] **Step 5: Run tests**

```bash
uv run pytest tests/ -x -q
```

---

### Task 11: Update `orcapod/__init__.py`

**Files:**
- Modify: `src/orcapod/__init__.py`

- [ ] **Step 1: Remove SEF class exports, keep `side_effect_function_pod` decorator**

Remove:
```python
from .core.side_effect_function import (
    SideEffectFunctionPod,
    SideEffectFunctionNode,
    SideEffectFunctionJobNode,
    side_effect_function_pod,
)
```

Add import of the new decorator from `function_pod.py`:
```python
from .core.function_pod import (
    FunctionPod,
    function_pod,
    side_effect_function_pod,  # thin alias for FunctionPod.from_fn with ctx_arg_name
)
```

Remove from `__all__`:
- `"SideEffectFunctionPod"`
- `"SideEffectFunctionNode"`
- `"SideEffectFunctionJobNode"`

Keep `"side_effect_function_pod"` in `__all__`.

- [ ] **Step 2: Run tests**

```bash
uv run pytest tests/ -x -q
```

---

### Task 12: Delete `src/orcapod/core/side_effect_function/` Module

**Files:**
- Delete: `src/orcapod/core/side_effect_function/__init__.py`
- Delete: `src/orcapod/core/side_effect_function/side_effect_function_pod.py`
- Delete the directory: `src/orcapod/core/side_effect_function/`

- [ ] **Step 1: Delete the SEF module**

```bash
rm -rf src/orcapod/core/side_effect_function/
```

- [ ] **Step 2: Verify no remaining imports**

```bash
grep -r "side_effect_function" src/orcapod/ --include="*.py" | grep -v "__pycache__"
```

Expected: only occurrences in `function_pod.py` (the new `side_effect_function_pod` decorator) and maybe the `node_type` value `"side_effect_function"` in `function_node.py` and `node_protocols.py`.

- [ ] **Step 3: Run all tests**

```bash
uv run pytest tests/ -x -q
```

Expected: all tests pass except the SEF-specific test file (which we'll update in Task 13).

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "refactor(function-pod): eliminate SideEffectFunctionPod hierarchy, unify into FunctionPod"
```

---

### Task 13: Update Tests to Use Unified API

**Files:**
- Modify: `tests/test_core/side_effect_function/test_side_effect_function_pod.py`

All references to `SideEffectFunctionPod`, `SideEffectFunctionJobNode`, etc. must be updated. The complete updated test file:

- [ ] **Step 1: Write the updated test file**

Replace the entire file with:

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


def _make_pipeline_db():
    """Return a fresh in-memory ArrowDatabase."""
    from orcapod.databases.in_memory_databases import InMemoryArrowDatabase
    return InMemoryArrowDatabase()


class TestSideEffectFunctionPodSchema:
    """SF-01, SF-02, SF-03, SF-10: schema inference and ctx stripping."""

    def test_sf01_ctx_stripped_from_input_schema(self):
        """SF-01: 'ctx' param stripped; data params form the input schema."""
        from orcapod.core.function_pod import FunctionPod

        def my_fn(value: int, ctx: InvocationContext) -> str:
            return f"result_{value}"

        pod = FunctionPod.from_fn(my_fn, output_keys=["result"], ctx_arg_name="ctx")

        # Input schema excludes 'ctx'
        assert "ctx" not in pod.input_data_schema
        assert "value" in pod.input_data_schema
        assert pod.input_data_schema["value"] == int

        # Output schema has the declared key
        assert "result" in pod.output_data_schema
        assert pod.output_data_schema["result"] == str

    def test_sf02_custom_ctx_arg_name(self):
        """SF-02: ctx_arg_name='context' — stripped and injected by correct name."""
        from orcapod.core.function_pod import FunctionPod

        def my_fn(value: int, context: InvocationContext) -> str:
            return f"r_{value}"

        pod = FunctionPod.from_fn(my_fn, output_keys=["result"], ctx_arg_name="context")
        assert "context" not in pod.input_data_schema
        assert "value" in pod.input_data_schema

    def test_sf03_missing_ctx_arg_raises_at_construction(self):
        """SF-03: Missing ctx_arg_name raises ValueError at construction time."""
        from orcapod.core.function_pod import FunctionPod

        def my_fn(value: int) -> str:
            return str(value)

        with pytest.raises(ValueError, match="ctx_arg_name"):
            FunctionPod.from_fn(my_fn, output_keys=["result"], ctx_arg_name="ctx")

    def test_sf10_node_uri_shape(self):
        """SF-10: uri[0]=='side_effect_function', uri[-1]=='python.function.v0', len==5."""
        from orcapod.core.function_pod import FunctionPod

        def my_fn(value: int, ctx: InvocationContext) -> str:
            return str(value)

        pod = FunctionPod.from_fn(my_fn, output_keys=["result"], ctx_arg_name="ctx")
        assert pod.uri[0] == "side_effect_function"
        assert pod.uri[-1] == "python.function.v0"
        assert len(pod.uri) == 5
        assert pod.uri[3] == "v1.0"


class TestSideEffectFunctionPodStreamStandalone:
    """SF-04, SF-05: standalone execution via FunctionPodStream."""

    def test_sf04_iter_data_returns_correct_output(self):
        """SF-04: iter_data() returns correct (tag, output_data) per row."""
        from orcapod.core.function_pod import FunctionPod

        def my_fn(value: int, ctx: InvocationContext) -> str:
            return f"v{value}"

        pod = FunctionPod.from_fn(my_fn, output_keys=["result"], ctx_arg_name="ctx")
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
        from orcapod.core.function_pod import FunctionPod

        received_ctx: list[InvocationContext] = []

        def my_fn(value: int, ctx: InvocationContext) -> str:
            received_ctx.append(ctx)
            return str(value)

        pod = FunctionPod.from_fn(my_fn, output_keys=["result"], ctx_arg_name="ctx")
        stream = _make_stream(1)
        list(pod.process(stream).iter_data())

        assert len(received_ctx) == 1
        ctx = received_ctx[0]
        assert ctx.pod_name == pod.label
        assert isinstance(ctx.invocation_hash, str)
        assert len(ctx.invocation_hash) > 0
        assert "::" in ctx.invocation_hash
        assert ctx.pipeline_run_id is None  # standalone: no run_id

    def test_sf05b_async_fn_routed_through_sync_execute(self):
        """SF-05b: async user function executed correctly via _call_async_sync."""
        from orcapod.core.function_pod import FunctionPod

        import asyncio

        async def my_async_fn(value: int, ctx: InvocationContext) -> str:
            await asyncio.sleep(0)
            return f"async_{value}"

        pod = FunctionPod.from_fn(my_async_fn, output_keys=["result"], ctx_arg_name="ctx")
        stream = _make_stream(2)
        rows = list(pod.process(stream).iter_data())

        assert len(rows) == 2
        assert rows[0][1].as_dict()["result"] == "async_0"
        assert rows[1][1].as_dict()["result"] == "async_1"


class TestSideEffectFunctionJobNode:
    """SF-06, SF-07, SF-09: DB-backed sync execution via FunctionJobNode."""

    def test_sf06_output_cached_after_first_run(self):
        """SF-06: Output cached; second run returns cached result without re-calling fn."""
        from orcapod.core.function_pod import FunctionPod
        from orcapod.core.nodes.function_node import FunctionJobNode

        call_count = 0

        def my_fn(value: int, ctx: InvocationContext) -> str:
            nonlocal call_count
            call_count += 1
            return f"r{value}"

        pod = FunctionPod.from_fn(my_fn, output_keys=["result"], ctx_arg_name="ctx")
        stream = _make_stream(2)
        pipeline_db = _make_pipeline_db()

        node1 = FunctionJobNode(function_pod=pod, input_stream=stream)
        node1.attach_databases(pipeline_database=pipeline_db)
        results1 = node1.execute(stream)
        assert len(results1) == 2
        assert call_count == 2

        # Second run — same pod, same data, same DB — fn must NOT be called again
        node2 = FunctionJobNode(function_pod=pod, input_stream=stream)
        node2.attach_databases(pipeline_database=pipeline_db)
        results2 = node2.execute(stream)
        assert len(results2) == 2
        assert call_count == 2  # NOT incremented — cache hit

        # Both runs produce equal result values
        for (_, d1), (_, d2) in zip(results1, results2):
            assert d1.as_dict()["result"] == d2.as_dict()["result"]

    def test_sf07_data_function_accessible_and_uri_consistent(self):
        """SF-07: pod._data_function is a PythonDataFunction; uri starts with 'side_effect_function'."""
        from orcapod.core.function_pod import FunctionPod
        from orcapod.core.data_function import PythonDataFunction

        def my_fn(value: int, ctx: InvocationContext) -> str:
            return f"r{value}"

        pod = FunctionPod.from_fn(my_fn, output_keys=["result"], ctx_arg_name="ctx")
        assert isinstance(pod._data_function, PythonDataFunction)
        assert pod.uri[0] == "side_effect_function"
        assert pod._ctx_arg_name == "ctx"

    def test_sf09_on_error_reraises(self):
        """SF-09: exceptions from user function always propagate."""
        from orcapod.core.function_pod import FunctionPod
        from orcapod.core.nodes.function_node import FunctionJobNode

        def my_fn(value: int, ctx: InvocationContext) -> str:
            raise RuntimeError("test error")

        pod = FunctionPod.from_fn(my_fn, output_keys=["result"], ctx_arg_name="ctx")
        stream = _make_stream(1)
        pipeline_db = _make_pipeline_db()
        node = FunctionJobNode(function_pod=pod, input_stream=stream)
        node.attach_databases(pipeline_database=pipeline_db)

        # Must propagate — no silent row suppression
        with pytest.raises(RuntimeError, match="test error"):
            node.execute(stream)


class TestSideEffectFunctionPodDecorator:
    """SF-11: @side_effect_function_pod decorator."""

    def test_sf11_decorator_creates_correct_pod(self):
        """SF-11: Decorator creates a FunctionPod with correct URI."""
        from orcapod.core.function_pod import FunctionPod, side_effect_function_pod

        @side_effect_function_pod(output_keys=["result"])
        def my_fn(value: int, ctx: InvocationContext) -> str:
            return str(value)

        assert isinstance(my_fn, FunctionPod)
        assert my_fn.uri[0] == "side_effect_function"
        assert my_fn.canonical_function_name == "my_fn"

    def test_sf11_decorator_accessible_from_public_api(self):
        """SF-11: Decorator accessible from orcapod top-level; class removed."""
        import orcapod
        assert hasattr(orcapod, "side_effect_function_pod")
        assert hasattr(orcapod, "FunctionPod")
        # SideEffectFunctionPod class is no longer exported (unified into FunctionPod)
        assert not hasattr(orcapod, "SideEffectFunctionPod")


class TestSideEffectFunctionPodPipelineIntegration:
    """SF-12: Full pipeline compilation and execution."""

    def test_sf12_pipeline_compilation_and_execution(self):
        """SF-12: FunctionJobNode compiled for ctx-aware pod, fn called, ctx received."""
        from orcapod.pipeline.job import PipelineJob
        from orcapod.core.function_pod import FunctionPod
        from orcapod.core.sources.dict_source import DictSource

        received_ctx: list[InvocationContext] = []

        def transform(value: int, ctx: InvocationContext) -> str:
            received_ctx.append(ctx)
            return f"result_{value}"

        pod = FunctionPod.from_fn(transform, output_keys=["result"], ctx_arg_name="ctx")
        db = _make_pipeline_db()

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

    def test_sf12_second_pipeline_run_uses_cache(self):
        """SF-12: Second pipeline run uses cached output; fn not called again."""
        from orcapod.pipeline.job import PipelineJob
        from orcapod.core.function_pod import FunctionPod
        from orcapod.core.sources.dict_source import DictSource

        call_count = 0

        def transform(value: int, ctx: InvocationContext) -> str:
            nonlocal call_count
            call_count += 1
            return f"r{value}"

        pod = FunctionPod.from_fn(transform, output_keys=["result"], ctx_arg_name="ctx")
        db = _make_pipeline_db()
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
        """SF-13: async_execute processes all rows, writes cache, returns correct output."""
        import asyncio
        from orcapod.core.function_pod import FunctionPod
        from orcapod.core.nodes.function_node import FunctionJobNode
        from orcapod.channels import Channel

        call_count = 0

        def my_fn(value: int, ctx: InvocationContext) -> str:
            nonlocal call_count
            call_count += 1
            return f"async_{value}"

        pod = FunctionPod.from_fn(my_fn, output_keys=["result"], ctx_arg_name="ctx")
        stream = _make_stream(3)
        pipeline_db = _make_pipeline_db()
        node = FunctionJobNode(function_pod=pod, input_stream=stream)
        node.attach_databases(pipeline_database=pipeline_db)

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
                    ch_in.reader, ch_out.writer, run_id="test-run-sf13"
                ),
            )
            return await ch_out.reader.collect()

        results = asyncio.run(_run())
        assert len(results) == 3
        assert call_count == 3

        # Verify output values
        output_values = [data.as_dict()["result"] for _, data in results]
        for i in range(3):
            assert f"async_{i}" in output_values
```

- [ ] **Step 2: Run the updated test file**

```bash
uv run pytest tests/test_core/side_effect_function/test_side_effect_function_pod.py -v
```

Expected: all 13 test cases pass.

- [ ] **Step 3: Run the full test suite**

```bash
uv run pytest tests/ -v
```

Expected: all tests pass.

- [ ] **Step 4: Commit**

```bash
git add tests/test_core/side_effect_function/test_side_effect_function_pod.py
git commit -m "test(function-pod): update SEF tests to use unified FunctionPod.from_fn API"
```

---

### Task 14: Close PR #230 and Create New PR

- [ ] **Step 1: Close the old PR**

```bash
gh pr close 230 --comment "Superseded by unified FunctionPod design (ITL-532). This PR's approach has been replaced by ctx_arg_name support on FunctionPod directly."
```

- [ ] **Step 2: Push the new branch**

```bash
git push -u origin HEAD
```

- [ ] **Step 3: Create the new PR**

```bash
gh pr create --title "feat(function-pod): unified ctx-injection via FunctionPod.from_fn (ITL-532)" --body "$(cat <<'EOF'
## Summary

- Eliminates the separate `SideEffectFunctionPod` class hierarchy
- `InvocationContext` injection is now an optional feature of `FunctionPod` via `ctx_arg_name`
- When `ctx_arg_name` is set, `uri` is prefixed with `"side_effect_function"`, `node_type` returns `"side_effect_function"`, and `process_data()` builds and injects `InvocationContext` per row
- `FunctionJobNode.execute()` and `async_execute()` gain `run_id: str | None = None` threaded to ctx building
- `@side_effect_function_pod` decorator kept as a thin alias of `FunctionPod.from_fn`
- Entire `src/orcapod/core/side_effect_function/` module deleted
- `SideEffectFunctionNodeProtocol`, `SideEffectFunctionInvocation`, `record_side_effect_function_pod_invocation`, and `side_effect_function_node_class` all removed

Closes ITL-532

## Test plan
- [ ] `uv run pytest tests/test_core/side_effect_function/ -v` — all 13 SF tests pass
- [ ] `uv run pytest tests/ -v` — full suite passes

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

## Self-Review Checklist

**Spec coverage:**
- ✅ `ctx_arg_name` support in `FunctionPod` — Task 1
- ✅ URI prefixed with `"side_effect_function"` — Task 1 Step 3
- ✅ `identity_structure()` includes `ctx_arg_name` — Task 1 Step 3
- ✅ `node_type` returns `"side_effect_function"` dynamically — Task 3 Step 1
- ✅ `process_data()` builds InvocationContext and injects it — Task 1 Steps 4–5
- ✅ `run_id` on `FunctionJobNode.execute()` and `async_execute()` — Task 3 Steps 3–5
- ✅ `CachedFunctionPod` threads `run_id` — Task 2
- ✅ `_strip_ctx_from_fn` moved to `function_pod.py` — Task 1 Step 1
- ✅ `side_effect_function/` module deleted — Task 12
- ✅ `SideEffectFunctionNodeProtocol` deleted — Task 4
- ✅ `record_side_effect_function_pod_invocation` deleted — Task 10
- ✅ `SideEffectFunctionInvocation` deleted — Task 7
- ✅ `side_effect_function_node_class` deleted — Tasks 6, 8, 9
- ✅ Orchestrator dispatch collapsed — Task 5
- ✅ `@side_effect_function_pod` decorator kept as alias — Task 1 Step 8
- ✅ Tests updated — Task 13
- ✅ PR #230 closed — Task 14

**Type consistency check:**
- `FunctionPod.from_fn(fn, output_keys, *, ctx_arg_name, ...)` — used in all tests ✓
- `FunctionJobNode(function_pod=pod, input_stream=stream)` — consistent ✓
- `node.attach_databases(pipeline_database=pipeline_db)` — consistent ✓
- `node.async_execute(ch_in.reader, ch_out.writer, run_id=...)` — single reader, consistent with `FunctionJobNode.async_execute` signature ✓
- `process_data(tag, data, *, logger=None, run_id=None)` — consistent across `_FunctionPodBase`, `CachedFunctionPod` ✓

**`uri[3] == "v1.0"` note:** Test SF-10 currently checks `pod.uri[3] == "v1"`. After the change, `from_fn` uses `version="v1.0"` by default. The `PythonDataFunction` stores the version string verbatim in the URI. If the existing `SideEffectFunctionPod` used `version=f"v{version}.0"` (i.e., `"v1.0"` for the default `version=1`), then the URI would be `("side_effect_function", ..., "v1.0", "python.function.v0")`. The test expects `pod.uri[3] == "v1"` — this may need adjustment. In `from_fn`, use `version=f"v{version}.0"` when `version` is an int. In the classmethod, since `version` is already a string default `"v1.0"`, `uri[3]` would be `"v1.0"`. Update the test to check `pod.uri[3] == "v1.0"`.
