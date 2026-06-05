# Cleanup: `FunctionNodeBase.as_table()` + Dataclass Struct Encoding

**Issue:** ENG-572  
**Date:** 2026-06-04  
**Status:** In progress

---

## Overview

Four related changes, all motivated by the original bug (ENG-572) in which
`FunctionNodeBase.as_table()` returns a zero-row table with no columns when no
data exists:

1. **`as_table()` cleanup** — derive schema from `self.output_schema()` upfront;
   branch immediately on empty vs. non-empty; extract the column-filtering second
   half into a `StreamBase` helper.
2. **`Schema.__add__`** — convenience operator that delegates to `Schema.merge()`.
3. **`__type` → `__dataclass.` sentinel rename** — field name now encodes what the
   struct represents.
4. **`arrow_schema_to_python_schema` fix** — return a synthesized concrete dataclass
   type instead of `Any` for structs with the sentinel.

---

## Item 1: `as_table()` cleanup

### Root cause

In `FunctionNodeBase.as_table()`, when `iter_data()` yields nothing:

```python
tag_schema, data_schema = None, None
for tag, data in self.iter_data():    # never executes
    ...

if not all_tags:
    self._cached_output_table = pa.table({})   # placeholder…

# FALLS THROUGH — no else, no early return
all_tags_as_tables = pa.Table.from_pylist([], schema=None)   # no columns
all_data_as_tables = pa.Table.from_pylist([], schema=None)   # no columns
self._cached_output_table = hstack_tables(...)               # OVERWRITES with no columns
```

Additional problem in the non-empty branch: `data_schema` is inferred from the
first datagram (`data.arrow_schema(all_info=True)`) instead of from the pod's
declared output schema.  The declared schema is authoritative and more stable.

### Part A: `StreamBase._apply_column_config()`

The second half of `as_table()` — dropping system tags / source / context / meta
columns, optionally sorting — is purely table-manipulation logic that belongs on
`StreamBase`.  Content-hash handling (which requires `_cached_content_hash_column`
and re-iterates `iter_data()`) stays in `FunctionNodeBase`.

```python
# StreamBase
def _apply_column_config(
    self,
    table: "pa.Table",
    column_config: ColumnConfig,
) -> "pa.Table":
    """Apply ``ColumnConfig`` column filtering and optional tag-sort to ``table``.

    Args:
        table: A fully-materialized PyArrow table (all columns present).
        column_config: Resolved column configuration.

    Returns:
        A new table with the appropriate columns dropped and optionally
        sorted by tag columns.
    """
    drop_columns = []
    if not column_config.system_tags:
        drop_columns.extend(
            c for c in table.column_names
            if c.startswith(constants.SYSTEM_TAG_PREFIX)
        )
    if not column_config.source:
        drop_columns.extend(
            f"{constants.SOURCE_PREFIX}{c}" for c in self.keys()[1]
        )
    if not column_config.context:
        drop_columns.append(constants.CONTEXT_KEY)
    if not column_config.meta:
        drop_columns.extend(
            c for c in table.column_names if c.startswith(constants.META_PREFIX)
        )
    elif not isinstance(column_config.meta, bool):
        drop_columns.extend(
            c for c in table.column_names
            if c.startswith(constants.META_PREFIX)
            and not any(c.startswith(p) for p in column_config.meta)
        )
    output_table = table.drop(
        [c for c in drop_columns if c in table.column_names]
    )
    if column_config.sort_by_tags:
        output_table_schema = output_table.schema
        output_table = (
            pl.DataFrame(output_table)
            .sort(by=self.keys()[0], descending=False)
            .to_arrow()
        )
        output_table = arrow_utils.restore_schema_nullability(
            output_table, output_table_schema
        )
    return output_table
```

### Part B: Restructured `FunctionNodeBase.as_table()`

```python
def as_table(
    self,
    *,
    columns: ColumnConfig | dict[str, Any] | None = None,
    all_info: bool = False,
) -> "pa.Table":
    if self._cached_output_table is None:
        all_tags = []
        all_data = []
        tag_python_schema, data_python_schema = self.output_schema(all_info=True)
        for tag, data in self.iter_data():
            all_tags.append(tag.as_dict(all_info=True))
            all_data.append(data.as_dict(all_info=True))

        converter = self.data_context.type_converter
        tag_arrow_schema = converter.python_schema_to_arrow_schema(tag_python_schema)
        data_arrow_schema = converter.python_schema_to_arrow_schema(data_python_schema)

        if not all_tags:
            self._cached_output_table = pa.Table.from_pylist(
                [],
                schema=converter.python_schema_to_arrow_schema(
                    tag_python_schema + data_python_schema
                ),
            )
        else:
            struct_data = converter.python_dicts_to_struct_dicts(
                all_data, python_schema=data_python_schema
            )
            all_tags_as_tables = pa.Table.from_pylist(
                all_tags, schema=tag_arrow_schema
            )
            if constants.CONTEXT_KEY in all_tags_as_tables.column_names:
                all_tags_as_tables = all_tags_as_tables.drop([constants.CONTEXT_KEY])
            all_data_as_tables = pa.Table.from_pylist(
                struct_data, schema=data_arrow_schema
            )
            self._cached_output_table = arrow_utils.hstack_tables(
                all_tags_as_tables, all_data_as_tables
            )

    column_config = ColumnConfig.handle_config(columns, all_info=all_info)
    output_table = self._apply_column_config(self._cached_output_table, column_config)

    if column_config.content_hash:
        if self._cached_content_hash_column is None:
            content_hashes = []
            for tag, data in self.iter_data():
                content_hashes.append(data.content_hash().to_string())
            self._cached_content_hash_column = pa.array(
                content_hashes, type=pa.large_string()
            )
        assert self._cached_content_hash_column is not None
        hash_column_name = (
            "_content_hash"
            if column_config.content_hash is True
            else column_config.content_hash
        )
        output_table = output_table.append_column(
            hash_column_name, self._cached_content_hash_column
        )

    return output_table
```

Key differences from the old implementation:
- `output_schema(all_info=True)` is called **once**, before the loop — schema is never
  inferred from datagram content.
- Empty branch creates a zero-row flat table from the combined schema
  (`tag + data` via `Schema.__add__`) rather than hstacking two empty tables.
- Non-empty branch uses `data_python_schema` from `output_schema()` directly,
  eliminating the `arrow_schema_to_python_schema` round-trip.
- The `_context_key` guard remains as defensive code (the schema passed to
  `from_pylist` excludes it, but the explicit drop is kept for safety).
- The `sort_by_tags` block is removed from `as_table()` — handled by
  `_apply_column_config()`.
- The unreachable `if self._cached_output_table is None: pa.table({})` fallback
  at the end is deleted.

### Why `StreamBase`, not `FunctionNodeBase`, for the helper

`StreamBase` already declares `keys()` (abstract) and `iter_data()` (abstract).
The column-filtering helper uses only `self.keys()[1]` (data column names) and
operates on an already-materialized table.  It has no dependency on `data_context`
or any DB state.  Placing it on `StreamBase` makes it available to `OperatorJobNode`
and any future DB-backed node without code duplication.

`ArrowTableStream.as_table()` is unaffected — it already uses its own optimised
path and does not call this helper.

---

## Item 2: `Schema.__add__`

Add to `Schema` in `src/orcapod/types.py`:

```python
def __add__(self, other: object) -> Self:
    if isinstance(other, Schema):
        return self.merge(other)
    raise NotImplementedError(
        f"Adding {Schema} to {type(other)} is not supported"
    )
```

`merge()` already exists and raises `ValueError` on type conflicts, which is the
correct strict behaviour for schema concatenation.  `Self` is already imported from
`typing`.

---

## Item 3: Rename `DATACLASS_TYPE_FIELD` → `"__dataclass."`

In `src/orcapod/semantic_types/dataclass_encoding.py`:

```python
# Before
DATACLASS_TYPE_FIELD = "__type"

# After
DATACLASS_TYPE_FIELD = "__dataclass."
```

All downstream uses already reference the constant (`has_dataclass_type_sentinel`,
`dataclass_to_struct_dict`, `struct_dict_to_dataclass`,
`dataclass_to_arrow_struct_type`, and the converter in `universal_converter.py`).
No further code edits required beyond the constant and test assertions that check
the literal field name.

The trailing dot is intentional: it is both unambiguous and supports pattern
matching via `field.name.startswith("__dataclass.")`.

Serialized data written with `"__type"` is incompatible.  Pre-v0.1.0, no backward-
compatibility shims are added (per project convention).

---

## Item 4: Fix `arrow_schema_to_python_schema` for dataclass structs

### Current behavior

In `UniversalTypeConverter._convert_arrow_to_python()`:

```python
if has_dataclass_type_sentinel(arrow_type):
    return Any   # loses all field-type information
```

### Fix

```python
if has_dataclass_type_sentinel(arrow_type):
    # Synthesize a concrete dataclass type from the struct's fields.
    # The sentinel field is excluded; each remaining field's Arrow type
    # is recursively converted to Python via arrow_type_to_python_type().
    fields = [
        (field.name, self.arrow_type_to_python_type(field.type))
        for field in arrow_type
        if field.name != DATACLASS_TYPE_FIELD
    ]
    return dataclasses.make_dataclass("_SynthesizedDataclass", fields)
```

The result is a proper `@dataclass` type.  It is automatically cached by
`arrow_type_to_python_type()`'s `_arrow_to_python_types` dict (keyed by
`pa.StructType`), so the same synthesized class is returned for the same struct
schema — no extra caching needed.

`DATACLASS_TYPE_FIELD` must be imported explicitly in `universal_converter.py`
(it is currently referenced only indirectly via `has_dataclass_type_sentinel`).
`dataclasses` (stdlib) must also be imported.

### Round-trip correctness

After the fix, `arrow_schema → python_schema → arrow_schema` works correctly for
dataclass columns: `python_schema_to_arrow_schema` can convert the synthesized
type back to the original Arrow struct because `_convert_python_to_arrow`
recognises dataclasses and delegates to `dataclass_to_arrow_struct_type`.

---

## Files changed

| File | Change |
|---|---|
| `src/orcapod/core/streams/base.py` | Add `_apply_column_config()` |
| `src/orcapod/core/nodes/function_node.py` | Restructure `as_table()` to use `output_schema()` and `_apply_column_config()` |
| `src/orcapod/types.py` | Add `Schema.__add__` |
| `src/orcapod/semantic_types/dataclass_encoding.py` | `DATACLASS_TYPE_FIELD = "__dataclass."` |
| `src/orcapod/semantic_types/universal_converter.py` | Fix `_convert_arrow_to_python` for dataclass structs; import `dataclasses` and `DATACLASS_TYPE_FIELD` |
| `tests/test_core/nodes/test_function_node_iteration.py` | Schema assertions in existing test; new empty-vs-non-empty schema test |
| `tests/test_semantic_types/test_dataclass_encoding.py` | Update sentinel field name assertions; add converter round-trip test |
| `DESIGN_ISSUES.md` | Add entry |

---

## Tests

### `test_function_node_iteration.py`

**Update `test_as_table_fresh_node_returns_empty_no_compute`:**
```python
assert "id" in table.column_names
assert "result" in table.column_names
```

**New `test_as_table_empty_schema_matches_non_empty_schema`:**
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
- **New**: `arrow_schema_to_python_schema` for a dataclass struct returns a
  concrete dataclass type (not `Any`) with matching field names and types.

### `test_types.py` (or equivalent)

- `Schema.__add__` delegates to `merge()` — test that `s1 + s2` returns the
  correct combined schema and raises `ValueError` on type conflict.
- `Schema.__add__` with a non-`Schema` raises `NotImplementedError`.

---

## Out of scope

- `ArrowTableStream.as_table()` — already optimised; not touched.
- `OperatorJobNode.as_table()` — delegates to cached streams; not touched here.
  Could adopt `_apply_column_config()` in a follow-up.
- Schema behavior for `SourceNode` or other node types.
- Changes to the semantic type registry or `SemanticStructConverter` hierarchy.
