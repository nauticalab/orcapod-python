# Function Execution Chain Improvements — Design & Implementation Plan

## Overview

This document captures design decisions for improving the executor integration in the
packet function / function pod / function node execution chain. The changes address four
areas:

1. **`with_options` semantics** — executors become immutable; `with_options()` always returns
   a new instance.
2. **`execution_engine_opts` ownership** — removed from FunctionNode; owned exclusively by
   the pipeline's executor-assignment logic.
3. **`CachedFunctionPod`** — a new pod-level caching wrapper complementing the existing
   `CachedPacketFunction` (packet-level caching).
4. **Type-safe executor dispatch via `Generic[E]` + `__init_subclass__`** — eliminates
   redundant `isinstance` checks in the hot path by resolving the executor protocol once at
   class definition time.

---

## 1. `with_options` Always Returns a New Instance

### Current state

`PacketFunctionExecutorBase.with_options()` returns `self` by default. `RayExecutor`
overrides it to return a new instance. This is inconsistent — callers cannot rely on
`with_options()` being side-effect-free without checking the concrete type.

### Design decision

`with_options()` **must always return a new executor instance**, even when no options change.
This makes executors effectively immutable value objects after construction — the same
executor can be safely shared across nodes, and `with_options()` produces a node-specific
variant without mutating the original.

### Changes

- **`PacketFunctionExecutorBase.with_options()`**: Default implementation returns
  `copy.copy(self)` (shallow clone) instead of `self`. Subclasses that carry mutable state
  (e.g. Ray handles) override to produce a properly configured new instance.
- **`PacketFunctionExecutorProtocol.with_options()`**: Docstring updated to specify "returns
  a **new** executor instance".
- **`LocalExecutor.with_options()`**: Returns a new `LocalExecutor()`. Trivial since it
  carries no state.

---

## 2. Remove `execution_engine_opts` from FunctionNode

### Current state

`FunctionNode.__init__` stores `self.execution_engine_opts: dict[str, Any] | None = None`.
This field is set externally (by the pipeline) and later read back during executor
assignment. The node becomes an awkward intermediary — it holds configuration that logically
belongs to the pipeline's executor-assignment step.

### Design decision

`execution_engine_opts` is **removed from FunctionNode entirely**. The pipeline's
`apply_executor` (or equivalent) logic is the sole owner: it reads per-node options from
the pipeline config, calls `executor.with_options(**merged_opts)`, and sets the resulting
executor directly on the packet function. The node never sees raw option dicts.

### Changes

- **`FunctionNode.__init__`**: Remove `self.execution_engine_opts` attribute.
- **`FunctionNode.from_descriptor`**: Stop reading/writing `execution_engine_opts` from
  descriptors. (Backward-compatible break is acceptable per project policy — pre-v0.1.0.)
- **Pipeline executor assignment** (in `pipeline/` module): Merge pipeline-level and
  per-node options, call `executor.with_options(**merged)`, then assign the resulting
  executor to `node.executor = configured_executor`.

---

## 3. `CachedFunctionPod` — Pod-Level Caching

### Current state

Caching exists only at the packet-function level (`CachedPacketFunction`), which wraps
`call()` / `async_call()` with DB lookup/insert. This works but cannot leverage tag
information (which is invisible to packet functions).

### Design decision

Add a **`CachedFunctionPod`** that wraps a `FunctionPod` and intercepts at the
`process_packet(tag, packet)` level. This complements `CachedPacketFunction`:

| Layer | `CachedPacketFunction` | `CachedFunctionPod` |
|-------|------------------------|---------------------|
| Intercepts at | `call(packet)` | `process_packet(tag, packet)` |
| Has tag access | No | Yes |
| Cache key includes | Packet content hash | Tag + packet content hash |
| Delegates to | Wrapped `PacketFunction.call()` | Inner `FunctionPod.process_packet()` |

Both are useful: `CachedPacketFunction` deduplicates purely on data content;
`CachedFunctionPod` can incorporate tag metadata into cache decisions.

### Implementation sketch

```python
class CachedFunctionPod(WrappedFunctionPod):
    """Pod-level caching wrapper that intercepts process_packet()."""

    def __init__(
        self,
        function_pod: FunctionPodProtocol,
        result_database: ArrowDatabaseProtocol,
        record_path_prefix: tuple[str, ...] = (),
        **kwargs,
    ) -> None:
        super().__init__(function_pod, **kwargs)
        self._result_database = result_database
        self._record_path_prefix = record_path_prefix

    def process_packet(
        self, tag: TagProtocol, packet: PacketProtocol
    ) -> tuple[TagProtocol, PacketProtocol | None]:
        # Cache key incorporates both tag and packet content
        cache_key = self._compute_cache_key(tag, packet)
        cached = self._lookup(cache_key)
        if cached is not None:
            return tag, cached
        tag, output = self._function_pod.process_packet(tag, packet)
        if output is not None:
            self._store(cache_key, tag, output)
        return tag, output
```

### Changes

- **New file**: `src/orcapod/core/cached_function_pod.py` containing `CachedFunctionPod`.
- **`function_pod` decorator**: Add `pod_cache_database` parameter that wraps the pod in
  `CachedFunctionPod` when provided (distinct from `result_database` which wraps the packet
  function in `CachedPacketFunction`).

---

## 4. Type-Safe Executor Dispatch via `Generic[E]` + `__init_subclass__`

### Problem

Currently, each `PacketFunctionBase` subclass that cares about executor-specific capabilities
must do `isinstance` checks in the hot path (`call()` / `direct_call()`). This is both
verbose and error-prone — forgetting to check means silent misuse.

In Rust, an `enum Executor { Python(PythonExecutor), Container(ContainerExecutor) }` with
`match` would give exhaustive, zero-cost dispatch. Python has no sum types with exhaustiveness
checking, but we can get close.

### Design decision

Use `Generic[E]` on `PacketFunctionBase` combined with `__init_subclass__` to resolve the
concrete executor protocol **once at class definition time**. The single `isinstance` check
moves to `set_executor()` (assignment boundary), and the hot path (`call()`) is clean.

### Mechanism

```python
from typing import Generic, TypeVar

E = TypeVar("E", bound=PacketFunctionExecutorProtocol)

class PacketFunctionBase(TraceableBase, Generic[E]):
    _resolved_executor_protocol: ClassVar[type]  # auto-set by __init_subclass__
    _executor: E | None = None

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        for base in cls.__orig_bases__:
            origin = typing.get_origin(base)
            if origin is PacketFunctionBase:
                args = typing.get_args(base)
                if args and not isinstance(args[0], TypeVar):
                    cls._resolved_executor_protocol = args[0]
                    return

    def set_executor(self, executor: PacketFunctionExecutorProtocol) -> None:
        """Single isinstance check at assignment boundary."""
        proto = getattr(type(self), '_resolved_executor_protocol', None)
        if proto is not None and not isinstance(executor, proto):
            raise TypeError(
                f"{type(self).__name__} requires {proto.__name__}, "
                f"got {type(executor).__name__}"
            )
        self._executor = executor  # type: ignore[assignment]
```

Subclasses declare the executor type **once** via the generic parameter:

```python
class PythonPacketFunction(PacketFunctionBase[PythonExecutorProtocol]):
    # No _executor_protocol ClassVar needed — __init_subclass__ extracts it
    # from Generic[PythonExecutorProtocol] automatically.
    ...
```

### Why this works

- **`__orig_bases__`** is set by Python's type machinery on every class that inherits from
  a `Generic`. It contains the parameterized base (e.g.
  `PacketFunctionBase[PythonExecutorProtocol]`).
- **`typing.get_args()`** extracts the type parameters.
- **`__init_subclass__`** runs at class definition time (import), not at instance creation.
  Zero per-instance overhead.
- The `isinstance(args[0], TypeVar)` guard skips intermediate abstract subclasses that
  haven't bound `E` yet (e.g. `PacketFunctionWrapper(PacketFunctionBase[E])`).

### Requirements

- Executor protocols used as type parameters **must** be decorated with
  `@runtime_checkable` (already the case for `PacketFunctionExecutorProtocol`).
- Any new executor protocol (e.g. `PythonExecutorProtocol`) needs `@runtime_checkable` too.

### Hot path after this change

```python
def call(self, packet: PacketProtocol) -> PacketProtocol | None:
    if self._executor is not None:
        # self._executor is statically typed as E (e.g. PythonExecutorProtocol).
        # No isinstance check needed — validated at set_executor() time.
        return self._executor.execute(self, packet)
    return self.direct_call(packet)
```

### Changes

- **`PacketFunctionBase`**: Add `Generic[E]`, `__init_subclass__` resolver,
  `set_executor()` method. Existing `executor` property setter delegates to `set_executor()`.
- **`PythonPacketFunction`**: Change to `PacketFunctionBase[PythonExecutorProtocol]`
  (or a more specific `PythonExecutorProtocol` if we introduce one).
- **`PacketFunctionWrapper`**: Change to `PacketFunctionBase[E]` (remains generic, passes
  through).
- **`CachedPacketFunction`**: Inherits from `PacketFunctionWrapper` — no changes needed
  since executor delegation already targets the wrapped leaf function.
- **Executor protocols**: Ensure `@runtime_checkable` on all protocols that will be used as
  generic parameters.

---

## 5. `FunctionPod.packet_function` Remains a Read-Only Property

### Decision

`FunctionPod.packet_function` stays as a property on the protocol (as currently implemented).
It is understood to be **read-only** — callers should not replace the packet function after
pod construction. The property exists for introspection and executor wiring, not mutation.

No code changes needed — this is a documentation/convention clarification.

---

## Implementation Plan

### Phase 1: Executor immutability + remove `execution_engine_opts`

1. Update `PacketFunctionExecutorBase.with_options()` default to return a shallow copy.
2. Update `LocalExecutor.with_options()` to return `LocalExecutor()`.
3. Verify `RayExecutor.with_options()` already returns a new instance (it does).
4. Remove `self.execution_engine_opts` from `FunctionNode.__init__`.
5. Remove `execution_engine_opts` from `FunctionNode.from_descriptor` read-only state.
6. Update pipeline executor-assignment logic to merge options externally and pass the
   configured executor in.
7. Update affected tests.

### Phase 2: Type-safe executor dispatch

1. Add `Generic[E]` and `__init_subclass__` to `PacketFunctionBase`.
2. Update `executor` setter to delegate to `set_executor()` with `__init_subclass__`-resolved
   protocol check.
3. Parameterize `PythonPacketFunction` as `PacketFunctionBase[PacketFunctionExecutorProtocol]`
   (or a narrower protocol if we introduce executor-type-specific protocols later).
4. Parameterize `PacketFunctionWrapper` as `PacketFunctionBase[E]`.
5. Ensure all executor protocols are `@runtime_checkable`.
6. Update tests to verify type checking at assignment time.

### Phase 3: `CachedFunctionPod`

1. Create `src/orcapod/core/cached_function_pod.py`.
2. Implement `CachedFunctionPod(WrappedFunctionPod)` with tag-aware cache key computation.
3. Add `pod_cache_database` parameter to `function_pod` decorator.
4. Add tests for pod-level vs packet-level caching interaction.

### Phase 4: Documentation and cleanup

1. Update `orcapod-design.md` with the new execution chain design.
2. Update `CLAUDE.md` architecture section if needed.
3. Check `DESIGN_ISSUES.md` for any resolved issues.

---

## Open Questions

- **Executor-type-specific protocols**: Currently all packet functions accept the base
  `PacketFunctionExecutorProtocol`. If we later want `PythonPacketFunction` to only accept
  executors with a `PythonExecutorProtocol` (which might require `execute(func, args)`
  rather than `execute(pf, packet)`), the `Generic[E]` mechanism already supports this —
  just parameterize with the narrower protocol.
- **`CachedFunctionPod` cache key design**: The exact composition of the cache key (which
  tag columns to include, whether to include system tags) needs detailed design during
  implementation. A reasonable default is tag content hash + packet content hash.
