# ENG-374: Execution Context Schema Design

## Problem

The packet function records execution context metadata alongside cached results, but the
current implementation has two issues:

1. **`ResultCache.store()` assumes all metadata values are strings** — it stores every
   variation and execution field as `pa.large_string()`. This breaks for structured fields
   like `dict[str, str]`.
2. **Execution data is incomplete and loosely defined** — `PythonPacketFunction.get_execution_data()`
   returns ad-hoc fields without a declared schema, and doesn't capture executor metadata.

This is a greenfield project pre-v0.1.0 — no backward compatibility concerns with
existing cached data. The old `execution_context` field is simply replaced.

## Design

### Execution data contract

`PythonPacketFunction.get_execution_data()` returns:

```python
{
    "executor_type": str,            # e.g. "local", "ray.v0"
    "executor_info": dict[str, str], # stringified executor metadata
    "python_version": str,           # e.g. "3.12.0"
    "extra_info": dict[str, str],    # escape hatch for future metadata
}
```

`PythonPacketFunction.get_execution_data_schema()` returns:

```python
Schema({
    "executor_type": str,
    "executor_info": dict[str, str],
    "python_version": str,
    "extra_info": dict[str, str],
})
```

### Data flow: executor to storage

1. **Executor** returns `get_executor_data() -> dict[str, Any]` with whatever metadata it
   chooses (e.g. `{"executor_type": "ray.v0", "ray_address": "auto", "remote_opts": {...}}`).
2. **`PythonPacketFunction.get_execution_data()`** receives this dict, pops `executor_type`
   as a top-level string field, and stringifies all remaining values into a flat
   `dict[str, str]` stored as `executor_info`. Stringification uses `str()` for simple
   scalar values and `json.dumps()` for dicts, lists, and other compound types.
3. The stringification responsibility sits in the PacketFunction, not the executor. Executors
   are free to return rich types.

### Function variation data (unchanged)

`PythonPacketFunction.get_function_variation_data()` continues to return:

```python
{
    "function_name": str,
    "function_signature_hash": str,
    "function_content_hash": str,
    "git_hash": str,
}
```

With corresponding `get_function_variation_data_schema()` returning the matching `Schema`.

### Two independent column groups

Function variation data and execution data are independent column groups in the results
table. Each has its own schema method. They are stored with distinct column prefixes
(`PF_VARIATION_PREFIX`, `PF_EXECUTION_PREFIX`).

### Datagram-based metadata storage

Instead of passing raw dicts + schemas to `ResultCache.store()`, metadata is wrapped as
`Datagram` objects. This:

- Keeps `ResultCache` free of type-conversion concerns (just calls `.as_table()`)
- Mirrors how input/output packets are already handled
- Each Datagram carries its own `DataContext` with the universal converter, so
  `dict[str, str]` fields are automatically converted to the appropriate Arrow
  representation (e.g. `pa.map_(pa.large_string(), pa.large_string())`)

**Updated `ResultCache.store()` signature:**

```python
def store(
    self,
    input_packet: PacketProtocol,
    output_packet: PacketProtocol,
    variation_datagram: DatagramProtocol,
    execution_datagram: DatagramProtocol,
    skip_duplicates: bool = False,
) -> None:
```

Inside `store()`:
1. Call `.as_table()` on each datagram to get Arrow tables
2. Rename columns with `PF_VARIATION_PREFIX` / `PF_EXECUTION_PREFIX`
3. Concatenate with the output packet table, input hash, and timestamp

The prefix logic is contained entirely within `ResultCache.store()` — the fewest
possible places know about it.

### CachedPacketFunction helper

A private helper on `CachedPacketFunction` avoids repeating datagram construction
across `call()`, `async_call()`, and `record_packet()`:

```python
def _build_metadata_datagrams(self) -> tuple[Datagram, Datagram]:
    variation_datagram = Datagram(
        self.get_function_variation_data(),
        python_schema=self.get_function_variation_data_schema(),
        data_context=self.data_context,
    )
    execution_datagram = Datagram(
        self.get_execution_data(),
        python_schema=self.get_execution_data_schema(),
        data_context=self.data_context,
    )
    return variation_datagram, execution_datagram
```

The `data_context` comes from `CachedPacketFunction.data_context`, which is inherited
from `PacketFunctionBase` (via `TraceableBase`). In practice this is the wrapped
function's data context, since `CachedPacketFunction` is constructed with it.

### Executor implementations

No changes needed to executor data contracts. The existing `get_executor_data()` return
values are consumed and stringified by `PythonPacketFunction`.

- **`PythonFunctionExecutorBase`** — already returns `{"executor_type": self.executor_type_id}`.
  Schema method already returns `Schema({"executor_type": str})`.
- **`LocalPythonFunctionExecutor`** — inherits base behavior.
- **`RayExecutor`** — already returns Ray-specific fields. These flow into `executor_info`
  after stringification by `PythonPacketFunction`.

**Bug fix needed:** `RayExecutor.get_executor_data_schema()` currently attempts to mutate
a `Schema` object (which is immutable). This must be fixed to construct a new `Schema`
with the combined fields.

## Scope of changes

1. **`PythonPacketFunction`** — update `get_execution_data()` (replaces old
   `execution_context` field with the new schema); add `get_execution_data_schema()`.
   `get_function_variation_data_schema()` already exists — no change needed.
2. **`PacketFunctionBase`** — add abstract `get_execution_data_schema()` and
   `get_function_variation_data_schema()` (aligning the base class with the protocol)
3. **`PacketFunctionWrapper`** — add delegation for `get_function_variation_data_schema()`
   and `get_execution_data_schema()` to the wrapped function (neither exists today)
4. **`CachedPacketFunction`** — add `_build_metadata_datagrams()`, update `call()` /
   `async_call()` / `record_packet()` to use it
5. **`ResultCache.store()`** — accept datagrams instead of raw dicts, use `.as_table()` +
   prefix renaming
6. **`RayExecutor.get_executor_data_schema()`** — fix immutable `Schema` mutation bug
7. **Tests** — update any tests that call `store()` directly or mock execution/variation data

## Out of scope

- Changing how the packet function itself executes
- Full dependency lockfile capture
- Querying by `executor_info` fields in `ResultCache.lookup()` (future concern)
- Changes to `ResultCache.lookup()` (continues matching on `INPUT_PACKET_HASH_COL`)
