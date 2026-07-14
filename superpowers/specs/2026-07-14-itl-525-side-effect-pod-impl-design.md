# ITL-525: SideEffectPod Implementation Design

## Overview

This document is the implementation design for ITL-525, derived from the ITL-524 design
spike (`superpowers/specs/2026-07-14-itl-524-side-effect-pods-design.md`) and the
codebase exploration session. It records all architectural decisions made during
brainstorming and is the authoritative reference for the implementation plan.

The feature adds `SideEffectPod` as a first-class pipeline node whose primary purpose is
a side effect (log writing, DB inserts, notifications). Every invocation is linked back
to the originating pipeline run, pod version, and input data via a deterministic
`invocation_hash` embedded in the artifact and recorded in an Orcapod-managed invocation
table.

---

## Architectural Decisions

### A. DB injection — SideEffectJobNode wrapper (option B)

`SideEffectPod` itself carries no database reference. A `SideEffectJobNode` wrapper
(analogous to `FunctionJobNode`) is created at pipeline compile time and receives
databases via `attach_databases()`. The pod is the user-facing API; the node is the
execution engine.

Consequence: `SideEffectPod` is used identically to `FunctionPod` in user code and
inside `with PipelineJob():` blocks. Outside a PipelineJob (standalone / lazy mode),
`SideEffectPodStream.iter_data()` executes delivery without any invocation logging.

### B. `run_id` propagation — call-time parameter, not node state

`run_id` is generated once per `PipelineJob.run()` call and is in scope in both
orchestrators' dispatch loops. It is forwarded to `SideEffectJobNode.execute()` /
`async_execute()` as a call-time keyword argument in the new `elif is_side_effect_node`
branch. It is **never stored** as persistent node state — a compiled node may be invoked
across multiple runs.

### C. `full_input_packet_hash` — combined Arrow table hash

```python
full_table = arrow_utils.hstack_tables(
    tag.as_table(columns={"system_tags": True}),   # tag cols + _tag::* system tag cols
    data.as_table(columns={"source": True}),        # data cols + _source_* cols
)
full_input_packet_hash: ContentHash = arrow_hasher.hash_table(full_table)
```

This is a single `arrow_hasher.hash_table()` call over all four column groups. It
matches the pattern used by `FunctionJobNode._build_entry_id_preimage()` and avoids
`hash_object` dict indirection. Note: `data.content_hash()` and `tag.content_hash()`
both use the default `ColumnConfig` which excludes source info and system tags
respectively — the `as_table(columns=...)` calls above are required to include those
groups.

---

## Component Layout

```
src/orcapod/
├── side_effects.py                        NEW — all public types + SideEffectPod
│                                               + SideEffectPodStream + SideEffectJobNode
│                                               + decorators
├── protocols/
│   ├── core_protocols/
│   │   └── side_effect_pod.py             NEW — SideEffectPodProtocol
│   └── node_protocols.py                  MODIFIED — SideEffectNodeProtocol,
│                                                     is_side_effect_node()
├── pipeline/
│   ├── base.py                            MODIFIED — SideEffectInvocation,
│   │                                               record_side_effect_pod_invocation(),
│   │                                               side_effect_node_class property,
│   │                                               compile() branch
│   ├── job.py                             MODIFIED — side_effect_node_class,
│   │                                               _distribute_databases(),
│   │                                               to_invocations()
│   ├── sync_orchestrator.py               MODIFIED — elif is_side_effect_node branch
│   └── async_orchestrator.py             MODIFIED — elif is_side_effect_node branch
└── protocols/core_protocols/tracker.py   MODIFIED — record_side_effect_pod_invocation()

tests/
└── test_core/
    └── side_effect_pod/
        └── test_side_effect_pod.py        NEW — T1–T18

docs/
└── concepts/
    └── side-effect-pods.md                NEW — user-facing concept doc
```

---

## Public API (`src/orcapod/side_effects.py`)

### `InvocationHashConfig`

```python
@dataclasses.dataclass(frozen=True)
class InvocationHashConfig:
    encoding: Literal["hex", "base64", "binary"] = "hex"
    component_length: int | None = None
```

`component_length` is in **bytes of raw digest**, encoding-independent. `None` means
full digest length. Applied identically to every `::` -separated component.

Serialization of a single `ContentHash` component:
1. Extract raw digest bytes from `content_hash.to_prefixed_digest()` (strip the
   `method:` prefix — the digest is after the first `:`).
2. Truncate to `component_length` bytes if not `None`.
3. Encode: hex → `bytes.hex()`, base64 → `base64.b64encode(bytes).decode()`, binary →
   raw bytes.

The `::` separator is fixed regardless of encoding.

### `SideEffectPodConfig`

```python
@dataclasses.dataclass(frozen=True)
class SideEffectPodConfig:
    track_completion: bool = True
    drop_on_failure: bool = True
    on_error: Literal["raise", "log"] = "raise"
    hash_config: InvocationHashConfig = dataclasses.field(
        default_factory=InvocationHashConfig
    )
```

### `InvocationContext`

```python
class InvocationContext:
    """Per-invocation context injected into a side-effect pod function.

    Constructed by ``SideEffectJobNode`` (pipeline mode) or
    ``SideEffectPodStream`` (standalone mode) before each delivery call.
    Stores raw ``ContentHash`` components so ``format_id(config=...)``
    can re-serialize with caller-supplied options without recomputing hashes.
    """

    # Public fields
    invocation_hash: str        # serialized per pod's InvocationHashConfig
    pod_name: str               # pod.label
    pod_content_hash: str       # pod.content_hash().to_string()
    pipeline_run_id: str | None # None for standalone / lazy pipelines

    # Private raw components (not part of public API, used by format_id)
    _pipeline_hash_ch: ContentHash       # component 1
    _full_input_packet_hash_ch: ContentHash  # component 2
    _hash_config: InvocationHashConfig   # pod's own config

    def format_id(self, config: InvocationHashConfig | None = None) -> str:
        """Return ``'orcapod-{hash}'`` with optional format override.

        Uses ``config`` if supplied, otherwise the pod's own ``InvocationHashConfig``.
        Re-serializes from the stored raw ``ContentHash`` components — no recomputation.
        """
        ...
```

`invocation_hash` is the two-component string when `track_completion=True`:
```
{pipeline_hash_serialized}::{full_input_packet_hash_serialized}
```

Three-component when `track_completion=False` and `pipeline_run_id` is not `None`:
```
{pipeline_hash_serialized}::{full_input_packet_hash_serialized}::{pipeline_run_id}
```

The `pipeline_run_id` component is included verbatim (never hashed or encoded).

`InvocationContext` is **not** a frozen dataclass because it holds private state. It
should be treated as immutable by callers (no public setters).

### `SideEffectPod`

```python
class SideEffectPod(_FunctionPodBase):
    """A pipeline node whose primary purpose is a side effect.

    Wraps a ``(data: T, [ctx: InvocationContext]) -> None`` callable.
    Returns a pass-through stream. When ``drop_on_failure=True``, only
    successfully-delivered rows flow downstream.

    In standalone mode (no ``PipelineJob``), executes row-by-row via
    ``SideEffectPodStream`` with no invocation logging.

    In pipeline mode, promoted to ``SideEffectJobNode`` at compile time,
    which adds DB-backed invocation logging and completion tracking.
    """
```

Constructor accepts the user's callable directly. Internally wraps it in a
`PythonDataFunction` solely for `content_hash()` / `identity_structure()` purposes —
`process_data()` is fully overridden and never routes through `data_function.call()`.

`process()` returns a `SideEffectPodStream` and registers a `SideEffectInvocation`
with `tracker_manager` (if inside a `with PipelineJob():` block).

`output_schema()` returns the input stream's schema unchanged (pass-through contract).
`keys()` returns the input stream's tag keys unchanged.

`SideEffectPod` does **not** implement `FunctionPodProtocol` — it implements
`SideEffectPodProtocol` (see below).

### `SideEffectPodStream`

A `StreamBase` subclass returned by `SideEffectPod.process()` in standalone mode.

`iter_data()` iterates the upstream stream and, per row:
1. Builds `InvocationContext` if the user function declares one (detected by type
   annotation — same detection logic as injection in `SideEffectJobNode`).
2. Calls the user function.
3. On success: yields `(tag, data)` downstream.
4. On exception with `on_error="raise"`: re-raises; row aborted.
5. On exception with `on_error="log"` + `drop_on_failure=True`: logs; row dropped.
6. On exception with `on_error="log"` + `drop_on_failure=False`: logs; yields
   `(tag, data)` downstream anyway.

In standalone mode `pipeline_run_id=None` always. No invocation log is written.

`output_schema()` and `keys()` delegate to the upstream stream.

### Decorators

```python
def side_effect_pod(
    fn: Callable | None = None,
    *,
    config: SideEffectPodConfig | None = None,
) -> SideEffectPod | Callable[[Callable], SideEffectPod]: ...

def sink_pod(
    fn: Callable | None = None,
    *,
    config: SideEffectPodConfig | None = None,
) -> SideEffectPod | Callable[[Callable], SideEffectPod]:
    """Preset: track_completion=True, drop_on_failure=True."""
    ...

def tap_pod(
    fn: Callable | None = None,
    *,
    config: SideEffectPodConfig | None = None,
) -> SideEffectPod | Callable[[Callable], SideEffectPod]:
    """Preset: track_completion=False, drop_on_failure=False."""
    ...
```

All three support both bare (`@sink_pod`) and parameterised (`@sink_pod(config=...)`)
usage. Config fields not preset by `sink_pod` / `tap_pod` take their defaults from
`SideEffectPodConfig`. Caller-supplied `config` fields override the presets.

---

## Protocol Layer

### `SideEffectPodProtocol` (`protocols/core_protocols/side_effect_pod.py`)

Follows the same pattern as `FunctionPodProtocol`. Declares:
- `process(*streams) -> SideEffectPodStream`
- `output_schema()` (pass-through)
- `pod_config: SideEffectPodConfig`
- `content_hash()`, `pipeline_hash()` (inherited from `PipelineElementProtocol`)

### `SideEffectNodeProtocol` + `is_side_effect_node()` (`protocols/node_protocols.py`)

```python
@runtime_checkable
class SideEffectNodeProtocol(Protocol):
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


def is_side_effect_node(node: GraphNode) -> TypeGuard[SideEffectNodeProtocol]: ...
```

---

## `SideEffectJobNode` (in `src/orcapod/side_effects.py`)

The DB-backed execution node. Created at pipeline compile time. Never instantiated
directly by users.

### Construction

```python
class SideEffectJobNode:
    def __init__(
        self,
        side_effect_pod: SideEffectPod,
        input_stream: StreamProtocol,
        label: str | None = None,
    ) -> None: ...
```

### DB attachment

```python
def attach_databases(
    self,
    pipeline_database: ArrowDatabaseProtocol | None = None,
) -> None: ...
```

Called by `PipelineJob._distribute_databases()`. The database is pre-scoped to the
pipeline root (`pipeline_db = store.at(*pipeline_name)`). The node computes its own
table path as:

```python
table_path = (self.pipeline_hash().to_string(), "side_effect_invocations")
```

and accesses `pipeline_database.at(*table_path)` at first invocation.

### Per-row execution logic (shared between sync and async)

```python
def _execute_row(
    self,
    tag: TagProtocol,
    data: DataProtocol,
    run_id: str | None,
) -> tuple[TagProtocol, DataProtocol] | None:
    """Execute delivery for one row. Returns (tag, data) to emit, or None to drop."""
    ...
```

Full row-level logic:

```
1. Compute full_input_packet_hash:
       tag_table  = tag.as_table(columns={"system_tags": True})
       data_table = data.as_table(columns={"source": True})
       fip_hash   = arrow_hasher.hash_table(hstack(tag_table, data_table))

2. Serialize invocation_hash per pod's hash_config:
       component_1 = serialize(self.pipeline_hash(), hash_config)
       component_2 = serialize(fip_hash, hash_config)
       if track_completion=False and run_id is not None:
           invocation_hash = f"{component_1}::{component_2}::{run_id}"
       else:
           invocation_hash = f"{component_1}::{component_2}"

3. If track_completion=True and pipeline_database is not None:
       status = lookup_completion_status(fip_hash.to_string())
       if status == "success":
           write_invocation_row(status="skipped", ...)
           return (tag, data)   # re-emit without re-delivery
       # status == "failed" or None → fall through to delivery

4. Build InvocationContext if user function needs it:
       ctx = InvocationContext(
           invocation_hash=invocation_hash,
           pod_name=self._pod.label,
           pod_content_hash=self._pod.content_hash().to_string(),
           pipeline_run_id=run_id,
           _pipeline_hash_ch=self.pipeline_hash(),
           _full_input_packet_hash_ch=fip_hash,
           _hash_config=hash_config,
       ) if _needs_ctx(user_fn) else None

5. Try: call user_fn(data[, ctx])
       write_invocation_row(status="success", ...)
       return (tag, data)

6. Except Exception as exc:
       write_invocation_row(status="failed", error_message=str(exc), ...)
       if on_error == "raise":
           raise
       # on_error == "log":
       logger.warning("SideEffectPod %r delivery failed: %s", pod_name, exc)
       if drop_on_failure:
           return None   # drop row
       return (tag, data)  # pass through regardless
```

### `InvocationContext` injection detection

Detection is by **type annotation only**, not parameter name:

```python
def _needs_ctx(fn: Callable) -> bool:
    """Return True iff fn has a parameter annotated exactly as InvocationContext."""
    hints = get_type_hints(fn)
    return InvocationContext in hints.values()
```

If no `InvocationContext` parameter is declared, the function is called with data only
and no `InvocationContext` is constructed (zero overhead).

### Invocation table DDL

Created on first invocation via `pipeline_database.create_table_if_not_exists(...)`.

Schema:

| Column | Arrow type | Notes |
|---|---|---|
| `full_input_packet_hash` | `large_string` | Primary lookup key — raw `ContentHash.to_string()` |
| `pod_content_hash` | `large_string` | `pod.content_hash().to_string()` |
| `pipeline_run_id` | `large_string` (nullable) | `None` for lazy pipelines |
| `executed_at` | `timestamp(unit="us", tz="UTC")` | Wall-clock UTC time |
| `status` | `large_string` | `"success"` / `"failed"` / `"skipped"` |
| `error_message` | `large_string` (nullable) | Set when `status="failed"` |

`InvocationHashConfig` is purely a user-facing concern with zero footprint in persistent
storage. All columns store raw, stable values only. The `invocation_hash` compound string
is derived from table path + `full_input_packet_hash` + `pipeline_run_id` and is never
stored redundantly.

`"skipped"` rows are written when `track_completion=True` and the input was previously
succeeded — delivery is not re-attempted, but a log row is always written.

Completion-state lookup queries on `full_input_packet_hash` (exact match), ordering by
`executed_at` descending, returning the most recent `status`.

### Sync path — `execute()`

```python
def execute(
    self,
    input_stream: StreamProtocol,
    *,
    observer: ExecutionObserverProtocol | None = None,
    run_id: str | None = None,
) -> list[tuple[TagProtocol, DataProtocol]]:
    ...
```

Iterates `input_stream.iter_data()`, calls `_execute_row()` per row, collects
non-`None` returns.

### Async path — `async_execute()`

```python
async def async_execute(
    self,
    inputs: Sequence[ReadableChannel[tuple[TagProtocol, DataProtocol]]],
    output: WritableChannel[tuple[TagProtocol, DataProtocol]],
    *,
    observer: ExecutionObserverProtocol | None = None,
    run_id: str | None = None,
) -> None:
    ...
```

Uses the same semaphore-based concurrency model as `_FunctionPodBase.async_execute()`.
Calls `_execute_row()` per row inside a `TaskGroup`. Non-`None` results are sent to
`output`. Channel is closed in a `finally` block.

Invocation log writes inside concurrent tasks must be atomic per row (one write per row,
no batching). The `ArrowDatabaseProtocol` implementations in use are single-process and
serialise writes via the event loop — no additional locking required.

---

## Pipeline Integration

### `SideEffectInvocation` (`pipeline/base.py`)

```python
@dataclasses.dataclass(frozen=True)
class SideEffectInvocation(PodInvocation):
    pod: SideEffectPod
    input_streams: tuple[StreamProtocol, ...]  # always length 1
    label: str | None = None
```

Added alongside `FunctionInvocation` and `OperatorInvocation`.

### `TrackerManagerProtocol` (`protocols/core_protocols/tracker.py`)

New method added:

```python
def record_side_effect_pod_invocation(
    self,
    pod: SideEffectPod,
    input_stream: StreamProtocol,
    label: str | None = None,
) -> None: ...
```

### `AbstractPipelineBase` (`pipeline/base.py`)

New concrete method:

```python
def record_side_effect_pod_invocation(
    self,
    pod: SideEffectPodProtocol,
    input_stream: StreamProtocol,
    label: str | None = None,
) -> None:
    self._record_invocation(
        SideEffectInvocation(pod=pod, input_streams=(input_stream,), label=label)
    )
```

New abstract property:

```python
@property
@abstractmethod
def side_effect_node_class(self) -> type: ...
```

`compile()` gains a new branch:

```python
elif isinstance(inv, SideEffectInvocation):
    node_map[key] = self.side_effect_node_class(
        side_effect_pod=inv.pod,
        input_stream=upstream_nodes[0],
        label=inv.label,
    )
```

### `PipelineJob` (`pipeline/job.py`)

```python
side_effect_node_class = SideEffectJobNode
```

`_distribute_databases()` gains:

```python
elif isinstance(node, SideEffectJobNode):
    node.attach_databases(pipeline_database=pipeline_db)
```

`to_invocations()` gains handling for `SideEffectJobNode` instances (similar to
`FunctionJobNode` branch).

### Sync orchestrator (`pipeline/sync_orchestrator.py`)

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

`run_id` is already in scope at this point in `SyncPipelineOrchestrator.run()`.

### Async orchestrator (`pipeline/async_orchestrator.py`)

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

`run_id` is already in scope in `_run_async()`.

---

## `orcapod/__init__.py` Re-exports

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

`SideEffectJobNode` is **not** re-exported (internal implementation detail).

---

## Test Scenarios (T1–T18)

File: `tests/test_core/side_effect_pod/test_side_effect_pod.py`

| # | Scenario | Key assertions |
|---|---|---|
| T1 | Basic pass-through — `drop_on_failure=False`, success | All input rows in output |
| T2 | `drop_on_failure=True` (default), all succeed | All rows emitted |
| T3 | `InvocationContext` auto-injection | `ctx.invocation_hash` non-empty string; `ctx.format_id()` returns `"orcapod-{hash}"` |
| T4 | No-`ctx` function | Pod executes without error; `InvocationContext` not constructed |
| T5 | Invocation log written — DB-backed pipeline | Row at `<pipeline_hash>/side_effect_invocations`; `status="success"`; no `invocation_hash` column |
| T6 | `track_completion=True` — same inputs re-run | Second run: function called once total; `status="skipped"` row added; row still emitted |
| T7 | `track_completion=False` — same inputs re-run | Both runs: function called; two `status="success"` log rows |
| T8 | `on_error="raise"` — function raises | Exception propagates to caller; `status="failed"` in log |
| T9 | `on_error="log"` + `drop_on_failure=True` — function raises | Exception logged at WARNING; row dropped; `status="failed"` in log |
| T10 | `on_error="log"` + `drop_on_failure=False` — function raises | Exception logged at WARNING; row emitted downstream; `status="failed"` in log |
| T11 | `invocation_hash` determinism | Re-running with identical inputs + code → identical `invocation_hash` |
| T12 | `invocation_hash` format override | `ctx.format_id(InvocationHashConfig(encoding="base64", component_length=8))` returns valid base64 compound |
| T13 | Async channel execution (`async_execute`) | Pod runs correctly; log rows written |
| T14 | Parallel execution — `max_concurrency > 1` | All invocations complete; all log rows written; no data loss |
| T15 | Pipeline composition — pod mid-pipeline | Downstream pod receives filtered output; side effect runs |
| T16 | `@sink_pod` shortcut | `SideEffectPod` with `track_completion=True, drop_on_failure=True` |
| T17 | `@tap_pod` shortcut | `SideEffectPod` with `track_completion=False, drop_on_failure=False` |
| T18 | `@side_effect_pod(config=...)` all four combinations | Each combination behaves per spec |

---

## Documentation (`docs/concepts/side-effect-pods.md`)

Content outline:
1. What side-effect pods are and when to use them
2. The four `track_completion × drop_on_failure` combinations with use-case table
3. `InvocationContext` — accessing the hash, `format_id()`, embedding in artifacts
4. `InvocationHashConfig` — encoding and truncation options
5. Usage examples: `@sink_pod`, `@tap_pod`, `@side_effect_pod(config=...)`
6. Reverse-lookup walk-through (condensed from ITL-524 spec)
7. Log growth guidance — pruning / archiving when `track_completion=False`
8. What changes with pod code updates (`pipeline_hash` rotates, prior completions not seen)

---

## Out of Scope (per ITL-525)

- Retry logic (ITL-526) — `SideEffectPodConfig` fields left extensible
- Fire-and-forget delivery (ITL-528)
- `FunctionPod` error handling alignment (ITL-527)
- `SideEffectPod` serialisation / `save()` / `load()` round-trip — follow-up
- `DerivedSource` wrapping a `SideEffectJobNode` — not meaningful (no output data to re-source)
