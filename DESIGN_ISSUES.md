# Design & Implementation Issues

A running log of identified design problems, bugs, and code quality issues.
Each item has a status: `open`, `in progress`, or `resolved`.

---

## `src/orcapod/core/packet_function.py`

### P1 — `parse_function_outputs` is dead code
**Status:** resolved
**Severity:** medium
`parse_function_outputs` is a module-level function with a `self` parameter, suggesting it was
originally a method. It is never called. `PythonPacketFunction.call` duplicates its logic verbatim.
Should be deleted or wired up as a method on `PacketFunctionBase` and used from `call`.

**Fix:** Converted to a proper standalone function `parse_function_outputs(output_keys, values)`.
Replaced the duplicated unpacking block in `PythonPacketFunction.call` with a call to it.
Updated tests accordingly.

---

### P2 — `CachedPacketFunction.call` silently drops the `RESULT_COMPUTED_FLAG`
**Status:** open
**Severity:** high
On a cache miss, the flag is set but the result is discarded:
```python
output_packet.with_meta_columns(**{self.RESULT_COMPUTED_FLAG: True})  # return value ignored
```
If `with_meta_columns` returns a new packet (immutable update), the flag is never actually
attached. Fix: `output_packet = output_packet.with_meta_columns(...)`.

---

### P3 — `PacketFunctionWrapper.__init__` passes no `version` to `PacketFunctionBase`
**Status:** open
**Severity:** medium
`PacketFunctionBase.__init__` requires a `version` string and parses it into
`_major_version`/`_minor_version`. `PacketFunctionWrapper` calls `super().__init__(**kwargs)`
without a `version`, so it either crashes (no version in kwargs) or silently defaults to `"v0.0"`.
Those parsed fields are then shadowed by the delegating properties, making them dead state.
Options: pass the inner function's version through, or avoid calling the base version-parsing
logic entirely.

---

### P4 — `PythonPacketFunction` computes the output schema hash twice
**Status:** open
**Severity:** low
`__init__` stores `self._output_schema_hash` (line ~289). `PacketFunctionBase` also lazily
caches `self._output_packet_schema_hash` (different attribute name) via
`output_packet_schema_hash`. Two fields holding the same value. One is redundant.

---

### P5 — Large dead commented-out block in `get_all_cached_outputs`
**Status:** open
**Severity:** low
The block commenting out `pod_id_columns` removal is leftover from an old design. It makes it
ambiguous whether system columns are actually filtered. Should be removed.

---

## `src/orcapod/core/function_pod.py`

### F1 — `TrackedPacketFunctionPod.process` is `@abstractmethod` with unreachable body code
**Status:** open
**Severity:** high
The method is decorated `@abstractmethod` but has real logic after the `...` (handle_input_streams,
schema validation, tracker recording, FunctionPodStream construction). Since Python never executes
the body of an abstract method via normal dispatch, this code is unreachable. `SimpleFunctionPod`
then duplicates this logic verbatim.

The base body should either be moved to a protected helper (e.g. `_build_output_stream`) that
subclasses call, or `process` should not be abstract and subclasses override only the parts that
differ.

---

### F2 — Typo in `TrackedPacketFunctionPod` docstring
**Status:** open
**Severity:** trivial
`"A think wrapper"` should be `"A thin wrapper"`.

---

### F3 — Dual URI computation paths in the class hierarchy
**Status:** open
**Severity:** low
`TrackedPacketFunctionPod.uri` assembles the URI from `self.packet_function.*` with its own lazy
schema-hash cache. `WrappedFunctionPod.uri` simply delegates to `self._function_pod.uri`. These
should agree (and do, after the `packet_function` fix), but having two independent implementations
makes future changes fragile.

---

### F4 — `FunctionPodNode` is not a subclass of `TrackedPacketFunctionPod`
**Status:** open
**Severity:** medium
`FunctionPodNode` reimplements `process_packet`, `process`, `__call__`, `output_schema`,
`validate_inputs`, and `argument_symmetry` from scratch rather than inheriting from
`SimpleFunctionPod`/`TrackedPacketFunctionPod` and overriding the parts that differ
(fixed input stream, pipeline record writing). The result is a large amount of structural
duplication that diverges silently over time.

---

### F5 — `FunctionPodStream` and `FunctionPodNodeStream` are near-identical copy-pastes
**Status:** open
**Severity:** medium
`iter_packets`, `as_table` (including content_hash and sort_by_tags logic), `keys`,
`output_schema`, `source`, and `upstreams` are duplicated almost line-for-line. The only
behavioural differences are:
- `FunctionPodNodeStream` has `refresh_cache()`
- `FunctionPodNodeStream.output_schema` reads from `_fp_node._cached_packet_function` directly

A shared base stream class would eliminate the duplication.

---

### F6 — `WrappedFunctionPod.process` makes the wrapper transparent to observability
**Status:** open
**Severity:** medium
`process` simply calls `self._function_pod.process(...)`, so the returned stream's `source` is
the *inner* pod, not the `WrappedFunctionPod`. Anything that inspects `stream.source` (e.g.
tracking, lineage) will see the inner pod and be unaware of the wrapper. Whether this is
intentional should be documented; if not, `process` needs to construct a new stream whose source
is `self`.

---

### F7 — TOCTOU race in `FunctionPodNode.add_pipeline_record`
**Status:** open
**Severity:** medium
The method checks for an existing record with `get_record_by_id` and skips insertion if found.
But it then calls `add_record(..., skip_duplicates=False)`, which will raise on a duplicate. A
race between the lookup and the insert (e.g. two concurrent processes handling the same tag+packet)
would cause a crash instead of a graceful skip. Should use `skip_duplicates=True` for consistency
with the intent.

---

### F8 — `CallableWithPod` protocol placement breaks logical grouping
**Status:** open
**Severity:** low
`CallableWithPod` is defined between `FunctionPodStream` and `function_pod`, breaking the natural
grouping. It should be co-located with `function_pod` or moved to the protocols module.

---

### F9 — `as_table()` crashes with `KeyError` on empty stream
**Status:** resolved
**Severity:** high
Both `FunctionPodStream.as_table()` and `FunctionPodNodeStream.as_table()` unconditionally call
`.drop([constants.CONTEXT_KEY])` on the tags table built from the accumulated packets. When the
stream is empty (e.g. because the packet function is inactive), `iter_packets()` yields nothing,
`tag_schema` stays `None`, and `pa.Table.from_pylist([], schema=None)` produces a zero-column
table. The subsequent `.drop([constants.CONTEXT_KEY])` then raises `KeyError` because the column
does not exist.

**Fix:** Guarded both `.drop([constants.CONTEXT_KEY])` calls in `FunctionPodStream.as_table()` and
`FunctionPodNodeStream.as_table()` with a column-existence check. Also made the final
`output_table = self._cached_output_table.drop(drop_columns)` safe by filtering `drop_columns`
to only columns that exist in the table.

---
