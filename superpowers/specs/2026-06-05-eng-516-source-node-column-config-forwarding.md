# ENG-516: SourceNodeBase column_config / all_info forwarding

**Date:** 2026-06-05
**Issue:** [ENG-516](https://linear.app/enigma-metamorphic/issue/ENG-516)

---

## Problem

`SourceNodeBase` (shared base for `SourceNode` and `SourceJobNode`) accepts `columns` and
`all_info` on `keys()` and `output_schema()` but uniformly ignores both. This is wrong in
two distinct ways:

1. **`SourceJobNode` (bound case)** — when a concrete `bound_source` is present, `keys()` and
   `output_schema()` should delegate to `bound_source.keys()` / `bound_source.output_schema()`
   with the same arguments, so callers get the same result as calling the source directly. Only
   `as_table()` already does this correctly; `keys()` and `output_schema()` do not.

2. **`SourceNodeBase` / `SourceNode` (unbound case)** — the node knows its `_tag_schema` and
   `_data_schema`. System tag column names and types are deterministically derivable from those
   schemas alone (via the same `schema_hash` computation used by `stream_builder.py`), so
   `system_tags=True` can and should be honoured without a live source.

`as_table()` on unbound nodes correctly raises `UnboundSourceError` and is unchanged.

---

## Design

### 1. New shared utilities

#### `schema_utils.compute_schema_hash(tag_schema, data_schema, semantic_hasher, char_count) -> str`

Extracted from the inline computation in `stream_builder.py`. Accepts only the minimal
inputs required — no whole `DataContext` or `OrcapodConfig` objects.

```python
def compute_schema_hash(
    tag_schema: Schema,
    data_schema: Schema,
    semantic_hasher: BaseSemanticHasher,
    char_count: int,
) -> str:
    """Compute the schema hash used for system-tag column naming.

    Args:
        tag_schema: Python tag schema.
        data_schema: Python data schema.
        semantic_hasher: Hasher from the active data context.
        char_count: Hex character count from OrcapodConfig.schema_hash_n_char.

    Returns:
        Hex string schema hash.
    """
    return semantic_hasher.hash_object(
        (tag_schema, data_schema)
    ).to_hex(char_count=char_count)
```

`stream_builder.py` is updated to call this instead of inlining the computation.

#### `arrow_utils.system_tag_column_names(schema_hash) -> tuple[str, str]`

Extracted from the name-construction lines inside `add_system_tag_columns()`. Returns the
two system-tag column names (`source_id_col`, `record_id_col`) without touching any table.

```python
def system_tag_column_names(schema_hash: str) -> tuple[str, str]:
    """Return the (source_id_col, record_id_col) system-tag column names for a schema hash.

    Args:
        schema_hash: Hex schema hash from ``compute_schema_hash()``.

    Returns:
        Tuple of (source_id_column_name, record_id_column_name).
    """
    source_id_col = f"{constants.SYSTEM_TAG_SOURCE_ID_PREFIX}{constants.BLOCK_SEPARATOR}{schema_hash}"
    record_id_col = f"{constants.SYSTEM_TAG_RECORD_ID_PREFIX}{constants.BLOCK_SEPARATOR}{schema_hash}"
    return source_id_col, record_id_col
```

`add_system_tag_columns()` is updated to call `system_tag_column_names()` internally.

---

### 2. `SourceNodeBase` — honour `system_tags` in `keys()` and `output_schema()`

A private helper caches the schema hash computation:

```python
def _schema_hash(self) -> str:
    from orcapod.utils.schema_utils import compute_schema_hash
    return compute_schema_hash(
        self._tag_schema,
        self._data_schema,
        self.data_context.semantic_hasher,
        self.orcapod_config.schema_hash_n_char,
    )
```

**`keys(columns, all_info)`** — replaces the silent-ignore with real work:

```python
columns_config = ColumnConfig.handle_config(columns, all_info=all_info)
tag_keys = tuple(self._tag_schema.keys())
if columns_config.system_tags:
    from orcapod.utils.arrow_utils import system_tag_column_names
    tag_keys += system_tag_column_names(self._schema_hash())
return tag_keys, tuple(self._data_schema.keys())
```

**`output_schema(columns, all_info)`** — same pattern:

```python
columns_config = ColumnConfig.handle_config(columns, all_info=all_info)
tag_schema = self._tag_schema
if columns_config.system_tags:
    from orcapod.utils.arrow_utils import system_tag_column_names
    source_id_col, record_id_col = system_tag_column_names(self._schema_hash())
    tag_schema = Schema({**dict(tag_schema), source_id_col: str, record_id_col: str})
return tag_schema, self._data_schema
```

Other `ColumnConfig` flags (`meta`, `context`, `source`, `content_hash`, `sort_by_tags`) are
no-ops at the node level — consistent with how `ArrowTableStream.output_schema()` and
`ArrowTableStream.keys()` treat these flags (they are only relevant in `as_table()`).

---

### 3. `SourceJobNode` — forwarding overrides for `keys()` and `output_schema()`

```python
def keys(self, *, columns=None, all_info=False):
    if self._bound_source is None:
        return super().keys(columns=columns, all_info=all_info)
    return self._bound_source.keys(columns=columns, all_info=all_info)

def output_schema(self, *, columns=None, all_info=False):
    if self._bound_source is None:
        return super().output_schema(columns=columns, all_info=all_info)
    return self._bound_source.output_schema(columns=columns, all_info=all_info)
```

`as_table()` is already correct and unchanged.

---

## Files changed

| File | Change |
|---|---|
| `src/orcapod/utils/schema_utils.py` | Add `compute_schema_hash()` |
| `src/orcapod/utils/arrow_utils.py` | Add `system_tag_column_names()`; update `add_system_tag_columns()` to use it |
| `src/orcapod/core/sources/stream_builder.py` | Call `compute_schema_hash()` instead of inlining |
| `src/orcapod/core/nodes/source_node.py` | Fix `SourceNodeBase.keys()`, `SourceNodeBase.output_schema()`; add `SourceJobNode.keys()`, `SourceJobNode.output_schema()` overrides; add `_schema_hash()` helper |
| `tests/test_core/nodes/test_source_node.py` | New test class for column_config forwarding |

---

## Tests

New test class `TestSourceNodeColumnConfig` in `tests/test_core/nodes/test_source_node.py`:

| Test | What it verifies |
|---|---|
| `test_unbound_keys_default` | No regression — schema keys returned unchanged without column_config |
| `test_unbound_keys_system_tags` | `SourceNode.keys(columns=ColumnConfig(system_tags=True))` includes both system-tag column names |
| `test_unbound_keys_all_info` | `SourceNode.keys(all_info=True)` produces same result as `system_tags=True` |
| `test_unbound_output_schema_system_tags` | `SourceNode.output_schema(system_tags=True)` tag schema includes both system-tag entries typed as `str` |
| `test_unbound_system_tag_names_match_bound` | Unbound `SourceJobNode.keys(system_tags=True)` returns the same column names as `bound_source.keys(system_tags=True)` for a source with the same schema |
| `test_bound_keys_forwarded` | Bound `SourceJobNode.keys()` delegates to `bound_source.keys()` |
| `test_bound_keys_all_info` | Bound `SourceJobNode.keys(all_info=True)` == `bound_source.keys(all_info=True)` |
| `test_bound_output_schema_forwarded` | Bound `SourceJobNode.output_schema()` delegates to `bound_source.output_schema()` |
| `test_bound_output_schema_all_info` | Bound `SourceJobNode.output_schema(all_info=True)` == `bound_source.output_schema(all_info=True)` |

---

## Out of scope

- Redesigning `ColumnConfig` shape.
- Changes to source implementations (`DictSource`, `ArrowTableSource`, etc.).
- Schema inference from `bound_source` — tracked in ENG-513.
- `DerivedSource` system-tag fix — tracked in PLT-924.
- Operator system-tag name-extending deduplication — tracked in ENG-576.
- System-tag field-name constant deduplication — tracked in ENG-577.
