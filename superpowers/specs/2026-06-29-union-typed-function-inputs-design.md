# Union-Typed Function Inputs: Accept and Resolve at Bind Time

**Date:** 2026-06-29  
**Linear:** ITL-452  
**Status:** approved

---

## Problem

When a function pod's input argument is declared with a union type (e.g.
`def foo(x: str | Path) -> str`), constructing the `FunctionPod` raises a
`ValueError`:

```
ValueError: Complex unions with multiple non-None types are not supported:
str | pathlib.Path. Only Optional[T] (T | None) is allowed.
```

The error originates in `_FunctionPodBase.__init__`, which calls
`ensure_types_registered_for_schemas(input_data_schema, output_data_schema)`.
That method forwards each schema annotation to `register_python_class`, which
rejects complex unions because Arrow has no native union storage type.

This is a premature rejection. Union-typed inputs are semantically valid — they
express that the pod can consume either concrete type. The constraint (that the
type must be concrete) only kicks in at stream-binding time, when a specific
typed stream is fed in.

---

## Design

### Core principle

- **Pod construction:** union-typed input args are accepted. No Arrow
  registration is attempted for the union type itself.
- **Stream binding:** the incoming stream's concrete type is validated against
  the declared union via `check_schema_compatibility` / `beartype.door.is_subhint`,
  which already handles this correctly (`is_subhint(str, str | Path)` → True;
  `is_subhint(int, str | Path)` → False).
- **No new API surface** is needed. The concrete input type at any given
  binding is already accessible via `pod_stream.upstreams[0].output_schema()`.

### Fix: `ensure_types_registered_for_schemas`

`src/orcapod/semantic_types/universal_converter.py`

When `ensure_types_registered_for_schemas` encounters a complex union annotation
(origin is `typing.Union` or `types.UnionType`, with more than one non-`None`
arm), it registers each non-`None` branch individually instead of forwarding the
whole union to `register_python_class`.

```python
origin = get_origin(annotation)
if origin is typing.Union or origin is types.UnionType:
    # Union types (e.g. str | Path) are valid in function input schemas.
    # Register each concrete branch so its LogicalType is available when
    # a stream is bound; the union itself has no Arrow representation.
    for branch in get_args(annotation):
        if branch is not type(None):
            self.register_python_class(branch)
else:
    self.register_python_class(annotation)
```

`register_python_class` itself is unchanged — it correctly rejects complex
unions when called directly (right behaviour for output schemas and explicit
type conversion).

### Documentation

- Comment in `ensure_types_registered_for_schemas` explaining the union-branch
  walk.
- Note in `_register_python_class_impl` at the complex-union `raise` pointing
  callers to `ensure_types_registered_for_schemas` for schema-level registration.
- Comment in `_FunctionPodBase.__init__` explaining that union inputs are
  accepted and resolved at bind time.

### DESIGN_ISSUES.md

Add a new entry documenting the bug and marking it `resolved` after the fix.

---

## Tests

New file: `tests/test_core/function_pod/test_union_typed_inputs.py`

### Construction

| Test | Assertion |
|---|---|
| `test_union_input_pod_construction_succeeds` | `FunctionPod(PythonDataFunction(foo, ...))` does not raise |
| `test_union_input_pod_has_correct_input_schema` | `pod.data_function.input_data_schema['x'] == str \| Path` |

### Stream binding

| Test | Assertion |
|---|---|
| `test_bind_str_stream_succeeds` | `pod.process(str_stream)` succeeds; `pod_stream.upstreams[0].output_schema()[1]['x'] == str` |
| `test_bind_path_stream_succeeds` | `pod.process(path_stream)` succeeds; `pod_stream.upstreams[0].output_schema()[1]['x'] == Path` |
| `test_bind_incompatible_type_raises` | `pod.process(int_stream)` raises `ValueError` |

### Data processing

| Test | Assertion |
|---|---|
| `test_process_str_input_yields_correct_output` | Output data contains correct result for `str` input |
| `test_process_path_input_yields_correct_output` | Output data contains correct result for `Path` input |

---

## Scope

In scope:
- Bug fix in `ensure_types_registered_for_schemas`
- Test coverage for construction, binding, and processing with union-typed inputs
- Documentation comments
- DESIGN_ISSUES.md entry

Out of scope:
- Union type hashing order-independence (paired ITL issue)
- Coercion between union branches
- Three-or-more-way unions (same fix applies, but explicit test coverage is not required)
- `FunctionNode` (DB-backed) — same fix path applies transitively, no separate changes needed
