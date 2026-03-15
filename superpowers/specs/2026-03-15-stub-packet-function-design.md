# PLT-931: PacketFunctionProxy for Read-Only Pipeline Loading

## Problem

When a pipeline is saved to JSON and loaded in an environment where the original
packet function (Python callable) is unavailable, `FunctionPod.from_config()` fails
and the loading code falls back to constructing a `FunctionNode` in read-only mode
with `function_pod=None`. This causes:

1. **`iter_packets()`** crashes — calls `_input_stream.iter_packets()` on `None`
2. **`get_all_records()`** returns `None` — short-circuits because
   `_cached_function_pod is None`, even though the DB and record paths are known
3. **`as_table()`** crashes — delegates to `iter_packets()`
4. **Downstream nodes** are marked read-only because upstream `load_status != FULL`,
   blocking all downstream computation even for operators and function pods whose
   functions *are* available

The root cause: `FunctionNode.from_descriptor()` bypasses `__init__` entirely when
`function_pod=None`, leaving all internal state as `None`. Every code path that
touches the function pod, packet function, input stream, or cached function pod
fails.

## Solution: PacketFunctionProxy

Introduce a `PacketFunctionProxy` (consistent with the existing `SourceProxy`
naming pattern) that carries all metadata of the original packet function
(schemas, identity, URI) but raises a clear error when invoked. This allows the
full `FunctionNode.__init__` → `attach_databases()` → `CachedFunctionPod` path
to work, so cached data access flows through existing code paths unchanged.

The proxy also supports **late binding**: a real packet function can be bound
to the proxy after loading via `bind()`. The proxy wraps (not replaces) the
bound function, preserving the fact that the function was not loaded originally.

### Design principles

- **No special-casing in FunctionNode**: The proxy satisfies
  `PacketFunctionProtocol`, so `FunctionNode.__init__`, `CachedFunctionPod`,
  `output_schema()`, `keys()`, `pipeline_path`, and `get_all_records()` all
  work without modification.
- **Clear failure at point of invocation**: `call()` / `async_call()` /
  `direct_call()` / `direct_async_call()` raise `PacketFunctionUnavailableError`
  with a message naming the function and explaining only cached results are
  available — unless a real function has been bound via `bind()`.
- **Correct identity**: The proxy reproduces the same `content_hash()` and
  `pipeline_hash()` as the original packet function, so DB path scoping and
  record lookups work correctly.
- **Downstream nodes can compute**: Because the `FunctionNode` with a proxy is
  fully constructed (with DB-backed `CachedFunctionPod`), it can serve cached
  data via `iter_packets()` Phase 1. Downstream operators and function pods
  (whose functions are available) can run in full mode on that data.
- **Late binding preserves provenance**: `bind()` wraps the bound function
  inside the proxy rather than replacing the proxy, so the proxy remains in the
  call chain. This preserves the information that the function was not loaded
  from the serialized config — useful for provenance tracking and debugging.
- **Binding validates identity**: `bind()` verifies that the provided packet
  function matches the proxy's stored identity (URI, schemas, content hash)
  before accepting it. If any identifying information mismatches, `bind()`
  raises `ValueError` with a clear message describing which fields differ.
  This prevents silently substituting a different function for the original.

## Components

### 1. `PacketFunctionUnavailableError` (in `errors.py`)

```python
class PacketFunctionUnavailableError(RuntimeError):
    """Raised when a packet function proxy is invoked without a bound function.

    This occurs when a pipeline is loaded in an environment where the
    original packet function is not available. Only cached results can
    be accessed.
    """
```

Subclasses `RuntimeError` so existing `except RuntimeError` handlers still catch
it, while allowing callers to catch this specific condition.

### 2. `PacketFunctionProxy` (new file: `src/orcapod/core/packet_function_proxy.py`)

A concrete implementation of `PacketFunctionProtocol` that stands in for an
unavailable packet function, preserving all identifying metadata and supporting
optional late binding of a real function.

#### Metadata and identity

- **Stores metadata from the serialized config**: `packet_function_type_id`,
  `canonical_function_name`, `major_version`, `minor_version_string`,
  `input_packet_schema`, `output_packet_schema`
- **Stores the original `uri` tuple**: Extracted from
  `config["uri"]` (the `FunctionPod.to_config()` output includes it).
  The `uri` property is overridden to return this stored value directly,
  avoiding any recomputation via `semantic_hasher.hash_object()`. This is
  critical because the schema round-trip (`Python type → Arrow string →
  Python type`) may not produce identical hash inputs, which would cause
  `CachedFunctionPod` to look in the wrong DB path.
- **Stores pre-computed hashes**: `content_hash` and `pipeline_hash` strings
  from the pipeline descriptor, overriding `content_hash()` and
  `pipeline_hash()` to return them directly (no recomputation needed)
- **`to_config()`** returns the original config dict (round-trip preservation),
  regardless of whether a function has been bound
- **`from_config()`** class method constructs from the same config dict that
  `PythonPacketFunction.to_config()` produces, extracting schemas and metadata
  without importing the callable

#### Invocation behavior

When **unbound** (no real function has been bound via `bind()`):
- `call()`, `async_call()`, `direct_call()`, `direct_async_call()` all raise
  `PacketFunctionUnavailableError` with a message like:
  *"Packet function 'my_func' is not available in this environment.
  Only cached results can be accessed."*
- `get_function_variation_data()` and `get_execution_data()` return empty dicts.
  Note: these are only called by `CachedFunctionPod.process_packet()` when
  *storing* a new result. Since the proxy raises
  `PacketFunctionUnavailableError` before the store call is reached, the empty
  dicts are never actually persisted.
- `executor` property returns `None`; setter is a **no-op** (silently ignored).
  This avoids conflicts with code paths like `Pipeline._apply_execution_engine`
  that iterate all function nodes and set executors — raising here would break
  pipeline loading unnecessarily.

When **bound** (a real function has been attached via `bind()`):
- `call()`, `async_call()`, `direct_call()`, `direct_async_call()` delegate
  to the bound function
- `get_function_variation_data()` and `get_execution_data()` delegate to the
  bound function
- `executor` property and setter delegate to the bound function

#### Constructor signature

```python
class PacketFunctionProxy(PacketFunctionBase):
    def __init__(
        self,
        config: dict[str, Any],
        content_hash_str: str | None = None,
        pipeline_hash_str: str | None = None,
    ) -> None:
```

The `config` parameter is the `packet_function` config dict from the serialized
pipeline (same format as `PythonPacketFunction.to_config()` output). The hash
strings come from the node descriptor.

**Version string handling**: `PacketFunctionBase.__init__` requires a `version`
string matching the regex `\D*(\d+)\.(.*)`. The proxy extracts this from
`config["config"]["version"]` (the same field `PythonPacketFunction.to_config()`
writes) and passes it to `super().__init__(version=...)`.

#### Schema reconstruction

The config contains `input_packet_schema` and `output_packet_schema` as
`dict[str, str]` (Arrow type strings). The proxy uses
`deserialize_schema()` from `pipeline.serialization` to reconstruct them
as `Schema` objects with Python types. **This must happen eagerly during
`__init__`** (not lazily), because `CachedFunctionPod` accesses
`output_packet_schema` immediately during its initialization chain via
`WrappedFunctionPod.uri → _FunctionPodBase.uri → semantic_hasher.hash_object(
self.packet_function.output_packet_schema)`.

However, since the proxy **overrides the `uri` property** to return the stored
original URI, the `semantic_hasher.hash_object()` call on the schema never
actually happens for the proxy. The schemas are still reconstructed eagerly
because `FunctionNode.__init__` calls `schema_utils.check_schema_compatibility()`
on input schema validation.

#### `bind()` — late binding of a real packet function

```python
def bind(self, packet_function: PacketFunctionProtocol) -> None:
    """Bind a real packet function to this proxy.

    Validates that the provided function matches the proxy's stored
    identity before accepting it. After binding, invocation methods
    delegate to the bound function.

    Args:
        packet_function: The real packet function to bind.

    Raises:
        ValueError: If the provided function's identifying information
            does not match the proxy's stored identity.
    """
```

**Validation checks** (all must pass):
- `canonical_function_name` matches
- `major_version` matches
- `packet_function_type_id` matches
- `input_packet_schema` matches
- `output_packet_schema` matches
- `uri` matches
- `content_hash()` matches (if the proxy has a stored content hash)

If any check fails, `bind()` raises `ValueError` with a message listing
the mismatched fields and their expected vs actual values.

The bound function is stored as `self._bound_function` (initially `None`).
The `is_bound` property exposes whether a function has been bound.

#### `unbind()` — reverting to proxy-only mode

```python
def unbind(self) -> None:
    """Remove the bound packet function, reverting to proxy-only mode."""
```

Sets `self._bound_function = None`. After unbinding, invocation methods
raise `PacketFunctionUnavailableError` again. Included for API consistency
with `SourceProxy`.

**Why wrap, not replace**: The proxy remains in the call chain even after
binding. This preserves the fact that the function was not loaded from the
serialized config — useful for provenance tracking, debugging, and
distinguishing "loaded from config" vs "bound after load" in downstream
logic.

### 3. Changes to `resolve_packet_function_from_config` (in `pipeline/serialization.py`)

Add a `fallback_to_proxy` parameter (default `False`):

```python
def resolve_packet_function_from_config(
    config: dict[str, Any],
    *,
    fallback_to_proxy: bool = False,
) -> PacketFunctionProtocol:
```

When `fallback_to_proxy=True` and the normal resolution fails (ImportError,
AttributeError, etc.), return a `PacketFunctionProxy` constructed from the config
instead of re-raising.

### 4. Changes to `FunctionPod.from_config` (in `function_pod.py`)

Add a `fallback_to_proxy` parameter that passes through to
`resolve_packet_function_from_config`:

```python
@classmethod
def from_config(
    cls,
    config: dict[str, Any],
    *,
    fallback_to_proxy: bool = False,
) -> "FunctionPod":
```

When the packet function resolves to a proxy, `FunctionPod` is constructed
normally — the proxy satisfies `PacketFunctionProtocol`.

### 5. Changes to `Pipeline._load_function_node` (in `pipeline/graph.py`)

The current logic:

```python
if mode == "full":
    if not upstream_usable:
        # fall back to read-only
    else:
        try:
            pod = FunctionPod.from_config(descriptor["function_pod"])
            return FunctionNode.from_descriptor(
                descriptor, function_pod=pod, input_stream=upstream_node,
                databases=databases,
            )
        except Exception:
            # fall back to read-only
```

Changes:
1. Call `FunctionPod.from_config(..., fallback_to_proxy=True)` so it never
   raises — it returns a FunctionPod with either a live or proxy packet function.
2. Determine `load_status` based on whether the packet function is a proxy.
3. Always construct `FunctionNode` via the normal `from_descriptor` full-mode
   path (with `function_pod` and `input_stream`), so `__init__` runs and
   `CachedFunctionPod` is created.

### 6. Changes to upstream usability check (in `Pipeline.load`)

Currently in `Pipeline.load()`, the upstream usability check requires
`LoadStatus.FULL`:

```python
upstream_usable = (
    upstream_node is not None
    and hasattr(upstream_node, "load_status")
    and upstream_node.load_status == LoadStatus.FULL
)
```

Change to also accept `READ_ONLY`:

```python
upstream_usable = (
    upstream_node is not None
    and hasattr(upstream_node, "load_status")
    and upstream_node.load_status in (LoadStatus.FULL, LoadStatus.READ_ONLY)
)
```

This allows downstream operators and function pods to compute on cached data
from upstream proxy-backed nodes.

The same change applies to the `all_upstreams_usable` check for operator nodes.

### 7. `FunctionNode.from_descriptor` and `load_status`

With the proxy approach, `from_descriptor` receives a real `function_pod`
(containing a proxy) and a real `input_stream` (upstream node). It takes
the normal full-mode `__init__` path.

After construction, `load_status` is set based on whether the packet function
is a proxy:

```python
from orcapod.core.packet_function_proxy import PacketFunctionProxy

if isinstance(pod.packet_function, PacketFunctionProxy):
    node._load_status = LoadStatus.READ_ONLY
else:
    node._load_status = LoadStatus.FULL
```

This is set in `_load_function_node` after the node is constructed.

### 8. `FunctionNode.iter_packets()` behavior with proxy

With a proxy-backed node:

- **Phase 1** (cached results from DB): Works unchanged — `CachedFunctionPod`
  reads from the result DB, pipeline DB join produces (tag, packet) pairs.
- **Phase 2** (compute missing packets): If any input packets are not in the
  cache, `_process_packet_internal` calls `CachedFunctionPod.process_packet()`,
  which calls `self._function_pod.process_packet(tag, packet)`, which calls
  `proxy.call(packet)` → raises `PacketFunctionUnavailableError`.

This is the correct behavior: cached data flows, new computation fails clearly.

### 9. Simplification of `FunctionNode.from_descriptor` read-only path

The current read-only path (lines 251–307) with `cls.__new__` and manual
attribute assignment becomes dead code for pipeline loading, since all loaded
function nodes now go through the normal `__init__` path (with either a live or
proxy packet function).

However, we should keep the read-only path for the case where the DB is also
unavailable (e.g., in-memory DB from original run). In that case, `load_status`
remains `UNAVAILABLE` and the node truly cannot serve any data.

The read-only `__new__` path is only used when `pipeline_db` is `None` or when
the DB type is in-memory (which warns and provides no cached data).

## Data flow summary

```
Pipeline.load()
  │
  ├─ Source node: reconstructed or proxy (unchanged)
  │
  ├─ Function node (function available):
  │   └─ FunctionPod(PythonPacketFunction) → FunctionNode (FULL)
  │
  ├─ Function node (function unavailable, DB available):
  │   └─ FunctionPod(PacketFunctionProxy) → FunctionNode (READ_ONLY)
  │       ├─ iter_packets() Phase 1: serves cached data ✓
  │       ├─ iter_packets() Phase 2: raises PacketFunctionUnavailableError ✗
  │       ├─ get_all_records(): works ✓
  │       └─ as_table(): works (from Phase 1 cache) ✓
  │
  ├─ Function node (function unavailable, DB unavailable):
  │   └─ from_descriptor read-only path → FunctionNode (UNAVAILABLE)
  │
  └─ Downstream operator/function pod:
      ├─ upstream FULL or READ_ONLY → construct in FULL mode
      └─ upstream UNAVAILABLE → construct in READ_ONLY/UNAVAILABLE mode
```

## Files changed

| File | Change |
|------|--------|
| `src/orcapod/errors.py` | Add `PacketFunctionUnavailableError` |
| `src/orcapod/core/packet_function_proxy.py` | New: `PacketFunctionProxy` class |
| `src/orcapod/pipeline/serialization.py` | `resolve_packet_function_from_config`: add `fallback_to_proxy` param |
| `src/orcapod/core/function_pod.py` | `FunctionPod.from_config`: add `fallback_to_proxy` param |
| `src/orcapod/pipeline/graph.py` | `_load_function_node`: use proxy fallback; `load()`: relax upstream usability check |
| `src/orcapod/core/nodes/function_node.py` | `from_descriptor`: set `load_status` based on proxy detection |

## Test plan

1. **PacketFunctionProxy unit tests**
   - Construction from a `PythonPacketFunction.to_config()` output
   - Correct `input_packet_schema`, `output_packet_schema`
   - `uri` returns stored original value (not recomputed)
   - `content_hash()` and `pipeline_hash()` return stored values
   - `call()` raises `PacketFunctionUnavailableError`
   - `async_call()` raises `PacketFunctionUnavailableError`
   - `to_config()` round-trips (returns original config)
   - `executor` returns `None`; setter is no-op (no error)

2. **FunctionPod with proxy**
   - `FunctionPod(PacketFunctionProxy(...))` constructs without error
   - `output_schema()` returns correct schemas
   - `process_packet()` raises `PacketFunctionUnavailableError`

3. **Pipeline load with unavailable function**
   - Save a pipeline with a function pod, load in environment where function
     is not importable
   - Function node has `load_status == READ_ONLY`
   - `get_all_records()` returns cached data
   - `iter_packets()` yields cached results (Phase 1)
   - `as_table()` returns cached data as table
   - Attempting to process a new (uncached) packet raises
     `PacketFunctionUnavailableError`

4. **Downstream computation from cached data**
   - Save a pipeline: source → function_pod → operator
   - Load with function unavailable but operator available
   - Function node is `READ_ONLY`, operator node is `FULL`
   - Operator can process cached data from function node
   - End-to-end: `operator_node.as_table()` returns correct results

5. **Downstream function pod with available function**
   - Save a pipeline: source → function_pod_A → function_pod_B
   - Load with function_A unavailable, function_B available
   - function_pod_A node is `READ_ONLY`, function_pod_B node is `FULL`
   - function_pod_B can compute on cached output from function_pod_A

6. **`bind()` — successful late binding**
   - Create a `PacketFunctionProxy` from a config, then `bind()` the
     matching `PythonPacketFunction`
   - `is_bound` returns `True`
   - `call()` delegates to the bound function (no error)
   - `executor` getter/setter delegate to the bound function

7. **`bind()` — identity mismatch rejection**
   - Create a `PacketFunctionProxy`, attempt `bind()` with a function
     that has a different `canonical_function_name` → `ValueError`
   - Same for mismatched `major_version`, `input_packet_schema`,
     `output_packet_schema`, `uri`, `content_hash`
   - Error message lists which fields differ

8. **`unbind()` — reverting to proxy-only mode**
   - Bind a function, then `unbind()`
   - `is_bound` returns `False`
   - `call()` raises `PacketFunctionUnavailableError` again

9. **Schema round-trip fidelity**
   - Verify that `deserialize_schema(serialize_schema(schema))` produces
     schemas that are functionally equivalent for all supported types
   - This guards against type-converter round-trip divergence

10. **Fully unavailable (no DB)**
   - Load pipeline with in-memory DB → function node is `UNAVAILABLE`
   - `iter_packets()` / `as_table()` raise `RuntimeError`
   - Downstream nodes are also `UNAVAILABLE` or `READ_ONLY`
