# Cleanup: `FunctionNodeBase.as_table()` + Dataclass Struct Encoding

**Issue:** ENG-572  
**Date:** 2026-06-04  
**Status:** In progress

---

## Overview

Three related changes, all motivated by the original bug (ENG-572) in which
`FunctionNodeBase.as_table()` returns a zero-row table with no columns when no
data exists:

1. **`as_table()` cleanup** — stop inferring schema from datagram content;
   derive it exclusively from `self.output_schema()`.  Add clean empty/non-empty
   branching.
2. **`__type` → `__dataclass.` sentinel rename** — improve discoverability:
   the sentinel field name now encodes *what* the struct represents.
3. **`arrow_schema_to_python_schema` fix** — when the converter encounters a
   dataclass struct, return a synthesized concrete dataclass type instead of `Any`.

Items 2 and 3 are connected: after the rename, the sentinel is more obviously a
type discriminator, and the converter fix means the round-trip
`arrow_schema → python_schema → arrow_schema` works correctly for dataclass columns.

---

## Item 1: `as_table()` cleanup in `FunctionNodeBase`

### Root cause of the original bug

In `FunctionNodeBase.as_table()`, when `iter_data()` yields nothing:

```python
tag_schema, data_schema = None, None
for tag, data in self.iter_data():    # never executes
    ...

if not all_tags:
    self._cached_output_table = pa.table({})   # placeholder…

# FALLS THROUGH — no else, no early return
struct_data = converter.python_dicts_to_struct_dicts([], python_schema=None)
all_tags_as_tables = pa.Table.from_pylist([], schema=None)   # no columns
all_data_as_tables = pa.Table.from_pylist([], schema=None)   # no columns
self._cached_output_table = hstack_tables(...)               # OVERWRITES with no columns
```

The `pa.table({})` placeholder is immediately overwritten.

### Additional problem in the non-empty branch

Even when data exists, the current code infers `data_schema` from the first
datagram (`data.arrow_schema(all_info=True)`).  This is fragile: the schema
could differ from the pod's declared output if the runtime type diverges from the
annotation.  It also requires a redundant `arrow_schema_to_python_schema` round-
trip.  The declared schema from `self.output_schema()[1]` is the authoritative
source and should be used directly.

### Design: clean branch + `_make_empty_table()`

**New method `FunctionNodeBase._make_empty_table()`:**

```python
def _make_empty_table(self) -> "pa.Table":
    """Build a zero-row PyArrow table matching this node's full output schema.

    Uses ``output_schema(all_info=True)`` for column names/types and
    ``data_context.type_converter`` for the Python → Arrow type mapping.

    Returns:
        A zero-row ``pa.Table`` whose schema matches the declared output.
        Falls back to ``pa.table({})`` for read-only nodes without a live pod.
    """
    if self._function_pod is None:
        return pa.table({})
    tag_schema, data_schema = self.output_schema(all_info=True)
    converter = self.data_context.type_converter
    tag_arrow_schema = converter.python_schema_to_arrow_schema(tag_schema)
    data_arrow_schema = converter.python_schema_to_arrow_schema(data_schema)
    empty_tag_table = pa.Table.from_pylist([], schema=tag_arrow_schema)
    empty_data_table = pa.Table.from_pylist([], schema=data_arrow_schema)
    return arrow_utils.hstack_tables(empty_tag_table, empty_data_table)
```

**Restructured `as_table()` (the `if self._cached_output_table is None` block):**

```python
if self._cached_output_table is None:
    all_tags = []
    all_data = []
    for tag, data in self.iter_data():
        all_tags.append(tag.as_dict(all_info=True))
        all_data.append(data.as_dict(all_info=True))

    if not all_tags:
        self._cached_output_table = self._make_empty_table()
    else:
        tag_schema, data_schema = self.output_schema(all_info=True)
        converter = self.data_context.type_converter
        tag_arrow_schema = converter.python_schema_to_arrow_schema(tag_schema)
        data_arrow_schema = converter.python_schema_to_arrow_schema(data_schema)

        all_tags_as_table = pa.Table.from_pylist(all_tags, schema=tag_arrow_schema)
        # _context_key is excluded by the schema; no explicit drop needed.

        struct_data = converter.python_dicts_to_struct_dicts(
            all_data, python_schema=data_schema
        )
        all_data_as_table = pa.Table.from_pylist(struct_data, schema=data_arrow_schema)

        self._cached_output_table = arrow_utils.hstack_tables(
            all_tags_as_table, all_data_as_table
        )
# Remove the now-unreachable fallback:
# if self._cached_output_table is None:
#     self._cached_output_table = pa.table({})
```

### What this removes

| Removed | Reason |
|---|---|
| `tag_schema = tag.arrow_schema(all_info=True)` inside the loop | Replaced by `output_schema()` |
| `data_schema = data.arrow_schema(all_info=True)` inside the loop | Replaced by `output_schema()` |
| `data_python_schema = converter.arrow_schema_to_python_schema(data_schema)` | No longer needed; `output_schema()[1]` is already a Python schema |
| `if constants.CONTEXT_KEY in all_tags_as_tables.column_names: drop(...)` | `output_schema()` tag schema excludes `_context_key` — `from_pylist` with an explicit schema silently ignores extra dict keys |
| Final `if self._cached_output_table is None: pa.table({})` fallback | Unreachable after the restructuring |

### Behavioral note: meta columns

The current `as_table(all_info=True)` exposes meta columns (`__data_id`,
`__pod_version`, etc.) because `tag.arrow_schema(all_info=True)` includes them.
After the cleanup, `output_schema()` is the authority — it does NOT include meta
columns (they live on datagrams, not on the declared schema).  `as_table(all_info=True)`
will no longer return meta columns.  This is more correct: the declared schema is
canonical; datagram-internal bookkeeping should not leak into the output table.

---

## Item 2: Rename `DATACLASS_TYPE_FIELD` from `"__type"` to `"__dataclass."`

### Motivation

The current sentinel field name `__type` is generic.  Changing it to `__dataclass.`
encodes *what kind of thing* the struct represents, making it unambiguous when
inspecting an Arrow schema.  The trailing dot is intentional: it makes pattern
matching (`field.name.startswith("__dataclass.")`) unambiguous and signals
structured namespace usage.

### Change

In `src/orcapod/semantic_types/dataclass_encoding.py`:

```python
# Before
DATACLASS_TYPE_FIELD = "__type"

# After
DATACLASS_TYPE_FIELD = "__dataclass."
```

All downstream uses already reference the constant (`has_dataclass_type_sentinel`,
`dataclass_to_struct_dict`, `struct_dict_to_dataclass`, `dataclass_to_arrow_struct_type`,
and the Arrow → Python converter in `universal_converter.py`) and require no further
edits beyond the constant.

### Impact

- Serialized data written with `"__type"` is incompatible.  Pre-v0.1.0, no
  backward-compatibility shims are needed (per project convention).
- `has_dataclass_type_sentinel()` automatically checks for `"__dataclass."` after
  the rename.
- Tests that assert on field names must be updated.

---

## Item 3: Fix `arrow_schema_to_python_schema` for dataclass structs

### Current behavior

In `UniversalTypeConverter._convert_arrow_to_python()`:

```python
if has_dataclass_type_sentinel(arrow_type):
    return Any   # loses all field-type information
```

`Any` is returned because the actual class is resolved per-row at decode time.
However, at the schema level we *do* know the field names and their types — they
are encoded in the Arrow struct.  Returning `Any` means `output_schema()` reports
`Any` for dataclass columns, which breaks `python_schema_to_arrow_schema` round-
trips and makes schema introspection useless for dataclass outputs.

### Fix: synthesize a concrete dataclass type

Replace the `return Any` with a synthesized concrete dataclass type whose fields
match the struct (excluding the sentinel):

```python
if has_dataclass_type_sentinel(arrow_type):
    # Build a synthesized dataclass type from the struct fields.
    # Excludes the sentinel field; converts each field's Arrow type to Python.
    fields = [
        (field.name, self.arrow_type_to_python_type(field.type))
        for field in arrow_type
        if field.name != DATACLASS_TYPE_FIELD
    ]
    return dataclasses.make_dataclass("_SynthesizedDataclass", fields)
```

The result is a proper `@dataclass` class.  It is automatically cached by
`arrow_type_to_python_type()`'s `_arrow_to_python_types` dict (keyed by
`pa.StructType`), so the same synthesized class is returned for the same struct
schema.

### Round-trip correctness

After the fix, `python_schema_to_arrow_schema` can convert the synthesized type
back to the original Arrow struct (because the type is a dataclass, and
`_convert_python_to_arrow` recognises dataclasses and delegates to
`dataclass_to_arrow_struct_type`).  The round-trip
`arrow_schema → python_schema → arrow_schema` is now correct for dataclass columns.

### Imports required in `universal_converter.py`

`dataclasses` and `DATACLASS_TYPE_FIELD` must be imported.  `dataclasses` is a
stdlib module; `DATACLASS_TYPE_FIELD` is already imported indirectly via
`has_dataclass_type_sentinel` from `dataclass_encoding` — add an explicit import
of the constant.

---

## Files changed

| File | Change |
|---|---|
| `src/orcapod/core/nodes/function_node.py` | Add `_make_empty_table()`; restructure `as_table()` |
| `src/orcapod/semantic_types/dataclass_encoding.py` | `DATACLASS_TYPE_FIELD = "__dataclass."` |
| `src/orcapod/semantic_types/universal_converter.py` | Fix `_convert_arrow_to_python` for dataclass structs; import `DATACLASS_TYPE_FIELD` |
| `tests/test_core/nodes/test_function_node_iteration.py` | Schema assertions in existing test; new empty-vs-non-empty schema test |
| `tests/test_semantic_types/test_dataclass_encoding.py` | Update sentinel field name assertions; add converter round-trip test |
| `DESIGN_ISSUES.md` | Add entry for items 1–3 |

---

## Tests

### `test_function_node_iteration.py`

**Update `test_as_table_fresh_node_returns_empty_no_compute`** — add:
```python
assert "id" in table.column_names      # tag column
assert "result" in table.column_names  # declared output column
```

**New `test_as_table_empty_schema_matches_non_empty_schema`**:
```python
def test_as_table_empty_schema_matches_non_empty_schema():
    db = InMemoryArrowDatabase()
    node_after = _make_node(db=db)
    node_after.run()
    full_table = node_after.as_table()

    node_before = _make_node()
    empty_table = node_before.as_table()

    assert empty_table.num_rows == 0
    assert full_table.num_rows > 0
    assert set(empty_table.column_names) == set(full_table.column_names)
```

### `test_dataclass_encoding.py`

- Replace all `"__type"` field-name assertions with `"__dataclass."`.
- **New test**: `arrow_schema_to_python_schema` for a struct with the sentinel
  returns a dataclass type (not `Any`) with the expected field names and types.

---

## Out of scope

- Behavior for `as_table(all_info=True)` returning meta columns — this is a
  pre-existing inconsistency that the cleanup removes as a side effect.
- Schema behavior for `SourceNode` or `OperatorNode`.
- Changes to the semantic type registry or `SemanticStructConverter` hierarchy.
