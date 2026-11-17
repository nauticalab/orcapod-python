# SideEffectFunctionPod Design
**Issue:** ITL-532
**Date:** 2026-07-16

---

## Overview

`SideEffectFunctionPod` is a hybrid of `FunctionPod` and `SideEffectPod`. It wraps a callable
with the signature `(ctx: InvocationContext, arg1: T1, ...) -> OutputData` and behaves like a
`FunctionPod` for all pipeline purposes (schema inference, output caching, DB-backed execution),
while also receiving a per-row `InvocationContext` — the same context object used by
`SideEffectPod`.

The primary use case is functions that both produce output **and** need to record provenance:
e.g. writing a file whose path is derived from the invocation hash, then returning a pointer
to that artifact as the function's output.

This design also includes a **breaking change to `SideEffectPod`**: the callable contract
changes from `fn(data: DataProtocol, ctx)` to `fn(**{ctx_arg_name: ctx, **data.as_dict()})`,
bringing it in line with `SideEffectFunctionPod`'s call style. A new `ctx_arg_name` constructor
argument (defaulting to `"ctx"`) makes the context parameter name flexible in both pod types.

---

## Goals & Success Criteria

- `SideEffectFunctionPod` wraps `(ctx: InvocationContext, arg1, ...) -> OutputData`; produces a
  new downstream data stream like `FunctionPod`.
- Data fields are passed as keyword arguments (same as `FunctionPod`), not as a single
  `DataProtocol` object.
- `InvocationContext` is constructed per-row with the same preimage as `SideEffectPod`.
- `ctx_arg_name` parameter (default `"ctx"`) controls which function parameter receives the
  context; stripping it from schema inference is transparent to the rest of the system.
- `node_uri = ("side_effect_function",) + <function-derived suffix>` — no collision with
  existing `FunctionNode` or `SideEffectNode` namespaces.
- `SideEffectFunctionJobNode` caches output in the result DB (like `FunctionJobNode`) **and**
  logs invocations in the side-effect invocation table (like `SideEffectJobNode`).
- `SideEffectPod` updated: new `ctx_arg_name` arg; callable called with unpacked data kwargs.
- Full test coverage: schema inference, ctx construction, caching/dedup, DB-backed execution,
  node_uri shape, decorator.

---

## Scope & Boundaries

**In scope:**
- `SideEffectFunctionPod` pod class
- `SideEffectFunctionPodStream` (standalone, no DB)
- `SideEffectFunctionNode` (blueprint, raises `PipelineJobRequiredError` on `iter_data`)
- `SideEffectFunctionJobNode` (DB-backed)
- `@side_effect_function_pod` decorator (bare + parameterised)
- `SideEffectFunctionPodProtocol`
- `SideEffectFunctionInvocation` (pipeline recording primitive)
- Tracker method `record_side_effect_function_pod_invocation`
- Breaking change: `SideEffectPod` callable now called with `fn(**{ctx_arg_name: ctx, **data.as_dict()})` + new `ctx_arg_name` arg
- Re-exports from `orcapod.__init__`
- Test suite

**Out of scope:**
- `drop_on_failure` row-dropping semantics (output always emitted or exception propagates)
- Transparency / pass-through behaviour
- Retry logic
- Async-only executor path (sync path sufficient for initial implementation; async mirrors
  `SideEffectJobNode.async_execute` pattern)

---

## Architecture

### Module location

```
src/orcapod/core/side_effect_function/
├── __init__.py                    # re-exports public symbols
└── side_effect_function_pod.py   # all classes + decorator
```

### Class hierarchy overview

```
TraceableBase
└── SideEffectFunctionPod          # user-facing pod

StreamBase
├── SideEffectFunctionPodStream    # standalone execution (no DB)
└── SideEffectFunctionNode         # blueprint (no DB, raises on iter_data)
    └── SideEffectFunctionJobNode  # DB-backed execution
```

---

## SideEffectPod Breaking Change

### What changes in `src/orcapod/side_effects.py`

**`SideEffectPod.__init__`** gains:
```python
ctx_arg_name: str = "ctx"
```
Stored as `self._ctx_arg_name`. Included in `identity_structure()` so that renaming the
context parameter changes the pod's content hash.

**`_execute_side_effect_row`** gains a `ctx_arg_name: str` parameter and changes the call
from:
```python
fn(data, ctx)
```
to:
```python
fn(**{ctx_arg_name: ctx, **data.as_dict()})
```

All internal callers of `_execute_side_effect_row` are updated to pass `ctx_arg_name`. The
decorators `@side_effect_pod`, `@sink_pod`, `@tap_pod` each gain a `ctx_arg_name: str = "ctx"`
keyword argument forwarded to `SideEffectPod`.

**`SideEffectPod.identity_structure`** changes from:
```python
return (self.uri, self._pod_config.track_completion, self._pod_config.drop_on_failure)
```
to:
```python
return (self.uri, self._ctx_arg_name, self._pod_config.track_completion, self._pod_config.drop_on_failure)
```

---

## SideEffectFunctionPod

### Constructor

```python
class SideEffectFunctionPod(TraceableBase):
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
```

### Schema extraction — stripping ctx

A helper `_strip_ctx_from_fn(fn, ctx_arg_name)` creates a lightweight wrapper whose
`__signature__` and `__annotations__` exclude `ctx_arg_name`. This wrapper is passed to
`extract_function_schemas`; the original `fn` is stored separately for actual calls and
content hashing.

```python
def _strip_ctx_from_fn(fn: Callable, ctx_arg_name: str) -> Callable:
    """Return a wrapper of fn with ctx_arg_name removed from signature/annotations."""
    sig = inspect.signature(fn)
    if ctx_arg_name not in sig.parameters:
        raise ValueError(
            f"ctx_arg_name '{ctx_arg_name}' not found in function signature. "
            f"Available parameters: {list(sig.parameters)}"
        )
    new_params = [p for n, p in sig.parameters.items() if n != ctx_arg_name]
    new_sig = sig.replace(parameters=new_params)

    @functools.wraps(fn)
    def wrapper(*args, **kwargs):  # never actually called
        return fn(*args, **kwargs)

    wrapper.__signature__ = new_sig  # type: ignore[attr-defined]
    wrapper.__annotations__ = {
        k: v for k, v in fn.__annotations__.items() if k != ctx_arg_name
    }
    return wrapper
```

### Content identity and URI

```python
# Computed in __init__ using the ORIGINAL fn (not the stripped wrapper):
_function_signature_hash = semantic_hasher.hash_object(get_function_signature(fn)).to_string()
_function_content_hash   = semantic_hasher.hash_object(get_function_components(fn)).to_string()
_output_schema_hash      = semantic_hasher.hash_object(output_data_schema).to_string()

@property
def uri(self) -> tuple[str, ...]:
    return (
        "side_effect_function",
        self.canonical_function_name,
        self._output_schema_hash,
        f"v{self._version}",
        "python_side_effect_function",
    )

def identity_structure(self) -> Any:
    return self.uri
```

`content_hash()` is computed by `ContentIdentifiableBase` from `identity_structure()` via the
semantic hasher — no additional override needed.

### Provenance metadata for result caching

`ResultCache.store()` requires `variation_datagram` and `execution_datagram`. The pod exposes:

```python
def get_function_variation_data(self) -> dict[str, Any]:
    return {
        "function_name": self.canonical_function_name,
        "function_signature_hash": self._function_signature_hash,
        "function_content_hash": self._function_content_hash,
        "git_hash": self._git_hash,
    }

def get_function_variation_data_schema(self) -> Schema: ...
def get_execution_data(self) -> dict[str, Any]: ...        # python version, executor type
def get_execution_data_schema(self) -> Schema: ...
```

These mirror `PythonDataFunction`'s equivalent methods.

### Calling the user function

```python
def _call_with_ctx(self, data: DataProtocol, ctx: InvocationContext) -> Any:
    kwargs = {self._ctx_arg_name: ctx, **data.as_dict()}
    if self._is_async:
        return _call_async_sync(self._fn, kwargs)   # same pattern as PythonDataFunction
    return self._fn(**kwargs)

def _build_output_data(self, raw_output: Any) -> DataProtocol:
    """Wrap raw function return value in a Data object with source info."""
    from orcapod.core.datagrams import Data
    from orcapod.core.data_function import parse_function_outputs
    output_dict = parse_function_outputs(self._output_keys, raw_output)
    new_uuid = uuid.UUID(bytes=uuid7().bytes)
    source_info = {k: f"{':'.join(self.uri)}::{new_uuid.hex}::{k}" for k in output_dict}
    return Data(
        output_dict,
        source_info=source_info,
        record_uuid=new_uuid,
        python_schema=self.output_data_schema,
        data_context=self.data_context,
    )
```

### Other pod API

```python
@property
def pod_config(self) -> SideEffectPodConfig: ...

@property
def canonical_function_name(self) -> str: ...

def computed_label(self) -> str | None:
    return getattr(self._fn, "__name__", None)

def argument_symmetry(self, streams) -> Any:
    return tuple(streams)          # single ordered input

def output_schema(self, *streams, ...) -> tuple[Schema, Schema]:
    if len(streams) != 1:
        raise ValueError(...)
    tag_schema, _ = streams[0].output_schema(...)
    return tag_schema, self.output_data_schema

def process(self, *streams, label=None) -> SideEffectFunctionPodStream:
    input_stream = streams[0]      # exactly 1 input stream
    self.tracker_manager.record_side_effect_function_pod_invocation(
        self, input_stream, label=label
    )
    return SideEffectFunctionPodStream(pod=self, input_stream=input_stream, label=label)
```

---

## SideEffectFunctionPodStream (standalone)

```python
class SideEffectFunctionPodStream(StreamBase):
    def iter_data(self) -> Iterator[tuple[TagProtocol, DataProtocol]]:
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

    def output_schema(self, ...) -> tuple[Schema, Schema]:
        tag_schema, _ = self._input_stream.output_schema(...)
        return tag_schema, self._pod.output_data_schema
```

`_build_invocation_context` is a module-level helper (shared by stream and job node) that
builds `InvocationContext` using the same preimage structure as `_execute_side_effect_row` in
`side_effects.py`.

---

## SideEffectFunctionNode (blueprint)

```python
class SideEffectFunctionNode(StreamBase):
    node_type = "side_effect_function"

    @property
    def node_uri(self) -> tuple[str, ...]:
        return self._pod.uri        # ("side_effect_function", name, schema_hash, "v1", ...)

    def iter_data(self):
        raise PipelineJobRequiredError(...)   # blueprint only

    def output_schema(self, ...) -> tuple[Schema, Schema]:
        tag_schema, _ = self._input_stream.output_schema(...)
        return tag_schema, self._pod.output_data_schema
```

---

## SideEffectFunctionJobNode (DB-backed)

```python
class SideEffectFunctionJobNode(SideEffectFunctionNode):

    def attach_databases(self, pipeline_database=None, result_database=None) -> None:
        self._pipeline_database = pipeline_database
        self._result_cache = (
            ResultCache(result_database, record_path=self.node_uri)
            if result_database is not None else None
        )
        if pipeline_database is not None:
            self._table_path = self.node_uri + (
                f"schema:{self.pipeline_hash().to_string()}",
            )
        else:
            self._table_path = None

    def execute(self, input_stream, *, run_id=None, observer=None):
        results = []
        for tag, data in input_stream.iter_data():
            # 1. Cache hit — skip re-execution
            if (
                self._pod.pod_config.track_completion
                and self._result_cache is not None
            ):
                cached = self._result_cache.lookup(data)
                if cached is not None:
                    results.append((tag, cached))
                    continue

            # 2. Build InvocationContext (same preimage as SideEffectJobNode)
            record_id_hash, record_id = _build_record_id(
                tag=tag, data=data, pod=self._pod,
                arrow_hasher=self._pod.data_context.arrow_hasher,
            )
            ctx = InvocationContext(
                pod_name=self._pod.label,
                pipeline_run_id=run_id,
                _pipeline_hash_ch=self.pipeline_hash(),
                _record_id_hash_ch=record_id_hash,
                _hash_config=self._pod.pod_config.hash_config,
                _track_completion=self._pod.pod_config.track_completion,
            )

            # 3. Call user function — exceptions always propagate (no silent row suppression)
            try:
                raw = self._pod._call_with_ctx(data, ctx)
            except Exception as exc:
                if self._pod.pod_config.on_error == "log":
                    logger.warning("SideEffectFunctionPod %r failed: %s", self._pod.label, exc)
                raise   # always re-raise — output must be produced or the pipeline fails

            # 4. Wrap output and cache it
            output_data = self._pod._build_output_data(raw)
            if self._result_cache is not None:
                var_dg = Datagram(self._pod.get_function_variation_data(), ...)
                exec_dg = Datagram(self._pod.get_execution_data(), ...)
                self._result_cache.store(data, output_data, var_dg, exec_dg)

            # 5. Log invocation (reuse _write_invocation_row from side_effects.py)
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
```

`async_execute` follows the same `asyncio.TaskGroup` + semaphore pattern as
`SideEffectJobNode.async_execute`, using `pod_config.max_concurrency`.

---

## Pipeline Integration

### New invocation type

```python
# pod_invocation.py
class SideEffectFunctionInvocation(PodInvocation):
    """Recording primitive for SideEffectFunctionPod invocations."""
    def __init__(self, pod, input_streams, label=None):
        if len(input_streams) != 1:
            raise ValueError(...)
        super().__init__(pod=pod, input_streams=input_streams, label=label)
```

### New protocol

```python
# protocols/core_protocols/side_effect_function_pod.py
class SideEffectFunctionPodProtocol(PipelineElementProtocol, Protocol):
    @property
    def pod_config(self) -> SideEffectPodConfig: ...
    def process(self, *streams, label=None) -> SideEffectFunctionPodStream: ...
    def output_schema(self, *streams, ...) -> tuple[Schema, Schema]: ...
    def argument_symmetry(self, streams) -> ArgumentGroup: ...
```

### Tracker

```python
# core/tracker.py — TrackerManager
def record_side_effect_function_pod_invocation(self, pod, input_stream, label=None):
    for tracker in self.get_active_trackers():
        tracker.record_side_effect_function_pod_invocation(pod, input_stream, label=label)
```

Abstract counterpart added to `TrackerProtocol`.

### pipeline/base.py

```python
def record_side_effect_function_pod_invocation(self, pod, input_stream, label=None):
    self._record_invocation(
        SideEffectFunctionInvocation(pod=pod, input_streams=(input_stream,), label=label)
    )
```

### pipeline/job.py

```python
side_effect_function_node_class = SideEffectFunctionJobNode

# _distribute_databases:
elif isinstance(node, SideEffectFunctionJobNode):
    node.attach_databases(
        pipeline_database=pipeline_db,
        result_database=result_db,          # same result DB as FunctionJobNode
    )

# as_pipeline:
elif isinstance(job_node, SideEffectFunctionJobNode):
    upstream_bp_hash = job_id_to_bp_hash[id(job_node._input_stream)]
    node_map[node_hash] = SideEffectFunctionNode(
        pod=job_node._pod,
        input_stream=node_map[upstream_bp_hash],
        label=job_node._label,
    )
```

### node_protocols.py

Both orchestrators dispatch via `is_function_node`, `is_side_effect_node`, etc., which check
`node.node_type`. A new `SideEffectFunctionNodeProtocol` and `is_side_effect_function_node`
TypeGuard must be added:

```python
@runtime_checkable
class SideEffectFunctionNodeProtocol(Protocol):
    """Protocol for side-effect-function nodes in orchestrated execution."""
    node_type: str

    def execute(self, input_stream, *, observer=None, run_id=None) -> list: ...
    async def async_execute(self, inputs, output, *, observer=None, run_id=None) -> None: ...
    def attach_databases(self, pipeline_database=None, result_database=None) -> None: ...


def is_side_effect_function_node(node) -> TypeGuard[SideEffectFunctionNodeProtocol]:
    return node.node_type == "side_effect_function"
```

### orchestrators (sync + async)

Both `sync_orchestrator.py` and `async_orchestrator.py` need an `elif is_side_effect_function_node(node):` branch that handles `SideEffectFunctionJobNode` identically to `SideEffectJobNode` (single upstream, call `execute(input_stream, run_id=run_id)` / `async_execute(inputs, output, run_id=run_id)`).

Import `is_side_effect_function_node` from `node_protocols`.

### graph.py and base.py

**`Pipeline.side_effect_function_node_class = SideEffectFunctionNode`** — new class attribute.

**`base.py` `from_invocations()` step 3** adds:
```python
elif isinstance(inv, SideEffectFunctionInvocation):
    node_map[key] = self.side_effect_function_node_class(
        pod=inv.pod,
        input_stream=upstream_nodes[0],
        label=inv.label,
    )
```

**`base.py` `to_invocations()`** adds:
```python
elif isinstance(node, SideEffectFunctionNode):
    inv_by_node_hash[node_hash] = SideEffectFunctionInvocation(
        pod=node._pod,
        input_streams=(node.upstreams[0],),
        label=node._label,
    )
```

**`graph.py` reconstruction** (from serialised pipeline files) adds a `"side_effect_function"` node-type branch — handled at implementation time since `SideEffectFunctionNode` has no `from_descriptor` serialization path needed for MVP (pipeline save/load for this node type is deferred).

---

## Decorator

```python
def side_effect_function_pod(
    fn: Callable | None = None,
    *,
    output_keys: list[str] | str,
    ctx_arg_name: str = "ctx",
    config: SideEffectPodConfig | None = None,
    name: str | None = None,
    version: int = 1,
) -> SideEffectFunctionPod | Callable:
    """Decorator wrapping a callable as a ``SideEffectFunctionPod``.

    Supports bare usage: ``@side_effect_function_pod(output_keys=["result"])``
    (parameterised only — output_keys is always required).
    Attaches the created pod as ``.pod`` on the decorated function.
    """
    def _wrap(f: Callable) -> SideEffectFunctionPod:
        pod = SideEffectFunctionPod(
            f, output_keys=output_keys, ctx_arg_name=ctx_arg_name,
            config=config, name=name, version=version,
        )
        f.pod = pod
        return pod

    if fn is not None:
        # Bare @side_effect_function_pod without output_keys would fail earlier
        return _wrap(fn)
    return _wrap
```

---

## Re-exports

```python
# src/orcapod/__init__.py — additions
from orcapod.core.side_effect_function import (
    SideEffectFunctionPod,
    SideEffectFunctionNode,
    SideEffectFunctionJobNode,
    side_effect_function_pod,
)
```

---

## Test Plan

`tests/test_core/side_effect_function/test_side_effect_function_pod.py`:

| ID | Scenario |
|----|----------|
| SF-01 | Schema inference: `ctx` stripped from input schema, data params correct |
| SF-02 | Custom `ctx_arg_name` ("context"): stripped and injected by correct name |
| SF-03 | Missing `ctx_arg_name` in function signature raises `ValueError` at construction |
| SF-04 | Standalone stream: `iter_data()` returns correct output data per row |
| SF-05 | InvocationContext fields: `pod_name`, `invocation_hash` non-empty, `pipeline_run_id=None` standalone |
| SF-06 | DB-backed: output cached after first run, second run returns cached result |
| SF-07 | DB-backed: invocation log row written on first run |
| SF-08 | `track_completion=False`: function called on every run, invocation logged each time |
| SF-09 | `on_error="log"`: exception logged then re-raised (no silent row suppression) |
| SF-10 | `node_uri` shape: `("side_effect_function", name, ...)` |
| SF-11 | `@side_effect_function_pod(output_keys=...)` decorator creates correct pod |
| SF-12 | Pipeline integration: `SideEffectFunctionJobNode` compiled correctly from invocation |
| SF-13 | Async execution path: `async_execute` produces same output as sync path |

`tests/test_core/side_effect_pod/test_side_effect_pod.py` (updated):

| ID | Scenario |
|----|----------|
| SEP-UPDATE | All 18 existing T1–T18 tests updated for new `fn(**{ctx_arg_name: ctx, **data.as_dict()})` call style |
| SEP-CTX-NAME | `ctx_arg_name="context"` routes InvocationContext to correct parameter |

---

## Files Changed

| File | Change type |
|------|------------|
| `src/orcapod/side_effects.py` | Modify — ctx_arg_name, new call style |
| `src/orcapod/core/side_effect_function/__init__.py` | New |
| `src/orcapod/core/side_effect_function/side_effect_function_pod.py` | New |
| `src/orcapod/protocols/core_protocols/side_effect_function_pod.py` | New |
| `src/orcapod/protocols/core_protocols/__init__.py` | Modify — export |
| `src/orcapod/protocols/core_protocols/trackers.py` | Modify — new method |
| `src/orcapod/core/tracker.py` | Modify — new method |
| `src/orcapod/pipeline/pod_invocation.py` | Modify — new invocation type |
| `src/orcapod/pipeline/base.py` | Modify — new recorder |
| `src/orcapod/pipeline/job.py` | Modify — compile + DB wiring |
| `src/orcapod/__init__.py` | Modify — re-exports |
| `tests/test_core/side_effect_function/` | New — SF-01 through SF-13 |
| `src/orcapod/protocols/node_protocols.py` | Modify — new protocol + TypeGuard |
| `src/orcapod/pipeline/sync_orchestrator.py` | Modify — side_effect_function branch |
| `src/orcapod/pipeline/async_orchestrator.py` | Modify — side_effect_function branch |
| `src/orcapod/pipeline/graph.py` | Modify — side_effect_function_node_class |
| `tests/test_core/side_effect_pod/test_side_effect_pod.py` | Modify — update for new call style |
