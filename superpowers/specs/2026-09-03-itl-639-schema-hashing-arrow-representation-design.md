# Schema Hashing via Arrow Representation — Design

**Issue:** ITL-639
**Date:** 2026-09-03
**Status:** Approved

---

## Overview

Function pod output-directory naming was silently invalidated by a pure module rename of an
orcapod type (`extension_types.file_type.File` → `logical_types.file_type.File`) because
schema hashing went through Python class identity (`module.qualname`) instead of the type's
stable Arrow extension name.

ITL-638 fixed `TypeObjectHandler` to use `logical_type_name` for registered orcapod types.
This issue finalises the design by:

1. Implementing `SchemaHandler` as the explicit, documented canonical path for schema
   hashing (replacing the accidental `_expand_mapping` path).
2. Fixing the dispatch order so `SchemaHandler` is actually reached for `Schema` objects.
3. Adding an Arrow-translatability guard inside `SchemaHandler` to catch unregistered
   types early with a clear error.
4. Adding a regression test for the module-rename scenario at the schema level.
5. Documenting the convention and sweeping all call sites.

---

## Background: Current State (Post-ITL-638)

`Schema` is a `Mapping[str, DataType]`. When `hash_object(schema)` is called today:

```
hash_object(Schema) → _is_structure() = True (Schema is Mapping)
                    → _expand_mapping()
                    → for each (field_name, type): hash_object(type) → TypeObjectHandler
                    → TypeObjectHandler: get_logical_type(type)
                        → registered type: "type:{lt.logical_type_name}"  ← stable (ITL-638)
                        → unregistered:    "type:{module}.{qualname}"      ← fallback
                    → sorted dict → JSON → SHA-256
```

`SchemaHandler` is registered (`registry.register(Schema, SchemaHandler())`) but is **never
reached** because `_is_structure` intercepts `Schema` first. It raises `NotImplementedError`
and is dead code.

---

## Why the Current Behavior Is Correct-Enough but Not Explicit

After ITL-638, schema hashing is already stable for all types that can appear in pod schemas:

- **Native Python types** (`int`, `str`, `float`, `bool`, `bytes`, `datetime`, `date`) — in
  `_get_python_to_arrow_map()`. Their `module.qualname` is always `builtins.int`, etc. Stable.
- **Registered orcapod logical types** (`op.File`, `op.Directory`, etc.) — `TypeObjectHandler`
  uses `logical_type_name`, stable across module renames.
- **Generic aliases** (`list[int]`, `dict[str, str]`) — `GenericAliasHandler` recurses into
  the above.
- **Optional variants** (`int | None`, `op.File | None`) — `UnionTypeHandler` recurses.

Any other type (user-defined, unregistered) would have **already failed** at
`FunctionPod` construction via `ensure_types_registered_for_schemas`, which raises
`TypeError` if a type cannot be converted to Arrow. So the "unregistered type falls back
to module.qualname" concern is theoretical for pod schemas.

The problem is that the correctness is **accidental** — it falls out of `_expand_mapping`
treating `Schema` as a plain `Mapping`. There is no documented convention, no centralized
place to add validation, and no explicit statement of intent.

---

## Design

### 1. Reorder `hash_object` — handlers before `_is_structure`

In `semantic_hasher.py`, move the handler-dispatch check **before** the structure check:

```python
def hash_object(self, obj, resolver=None):
    # 1. ContentHash terminal
    if isinstance(obj, ContentHash):
        return obj

    # 2. Primitives
    if isinstance(obj, (type(None), bool, int, float, str)):
        return self._hash_to_content_hash(obj)

    # 3. Registered handler (BEFORE _is_structure — ensures SchemaHandler takes
    #    priority over _expand_mapping for Schema objects, which are Mappings).
    handler = self._registry.get_handler(obj)
    if handler is not None:
        result = handler.handle(obj, self)
        return self.hash_object(result, resolver=resolver)

    # 4. Generic structures
    if _is_structure(obj):
        expanded = self._expand_structure(obj, frozenset(), resolver=resolver)
        return self._hash_to_content_hash(expanded)

    # 5. ContentIdentifiableProtocol / fallback ...
```

**Safety:** `Schema` is the only type that is both a registered handler target and a
`Mapping`. No other handler covers a structure type. This reordering has no effect on any
other object.

### 2. Implement `SchemaHandler`

Replace the `NotImplementedError` stub:

```python
class SchemaHandler:
    """Hasher for ``Schema`` objects.

    Canonical, explicit path for schema hashing. For each field, hashes the Python
    type via ``hasher.hash_object`` (which dispatches to ``TypeObjectHandler``, using
    the stable ``logical_type_name`` / Arrow extension name for registered types and the
    stable ``builtins.*`` / stdlib path for native types). Field names are sorted so the
    hash is deterministic regardless of insertion order.

    This produces the same hash as the previous accidental path through ``_expand_mapping``
    (Schema is a Mapping), preserving all existing hash values.

    ``Schema.optional_fields`` is intentionally excluded from the hash. Two schemas with
    the same field names and types but different optionality are hash-equivalent. Optionality
    is a Python-level execution contract (which parameters have defaults), not part of the
    structural identity used for caching or pipeline routing.

    When ``type_converter`` is provided, every field type is verified to be
    Arrow-translatable before hashing. A type that cannot be converted raises ``TypeError``
    with a diagnostic message, catching unregistered types at hash time rather than
    silently falling back to a potentially unstable Python module path.

    Args:
        type_converter: Optional ``TypeConverterProtocol``. When provided, validates
            Arrow-translatability per field. When ``None`` (e.g. in tests without a
            full ``DataContext``), validation is skipped.
    """

    def __init__(self, type_converter: "TypeConverterProtocol | None" = None) -> None:
        self._type_converter = type_converter

    def handle(self, obj: Any, hasher: "SemanticHasherProtocol") -> Any:
        if not isinstance(obj, Schema):
            raise TypeError(
                f"SchemaHandler: expected a Schema, got {type(obj)!r}"
            )
        result: dict[str, str] = {}
        for field_name, python_type in obj.items():
            if self._type_converter is not None:
                try:
                    self._type_converter.python_type_to_arrow_type(python_type)
                except (TypeError, ValueError) as exc:
                    raise TypeError(
                        f"SchemaHandler: field {field_name!r} has type "
                        f"{python_type!r} that is not Arrow-translatable. "
                        f"Every type in a schema must be Arrow-convertible — "
                        f"register it as an orcapod logical type or use a "
                        f"supported native type (int, str, float, bool, bytes, "
                        f"datetime, date)."
                    ) from exc
            result[field_name] = hasher.hash_object(python_type).to_string()
        # Sort by field name for determinism (matches _expand_mapping's sort_keys).
        return dict(sorted(result.items()))
```

### 3. Wire `type_converter` into `SchemaHandler` registration

In `register_builtin_python_type_handlers`:

```python
# Before:
registry.register(Schema, SchemaHandler())

# After:
registry.register(Schema, SchemaHandler(type_converter=type_converter))
```

`type_converter` is already a parameter of `register_builtin_python_type_handlers` —
it was previously forwarded only to `TypeObjectHandler` and `FunctionSignatureExtractor`.

---

## Hash Stability

`SchemaHandler.handle()` returns `{"field_name": "semantic_v0.1:...hash_token..."}` —
exactly the same structure produced by `_expand_mapping` today. The subsequent
`hash_object(that_dict)` call expands it identically. **All existing schema hash golden
values remain valid.** No cache bust from this change.

---

## Call-Site Audit

All 9 call sites identified in the sweep flow through the semantic hasher and reach
`hash_object(Schema)`. After this change, each goes through `SchemaHandler` instead of
the accidental `_expand_mapping` path. The hash values are unchanged.

| Site | Location | Schema hashed | Notes |
|------|----------|--------------|-------|
| 1 | `data_function.py:182` | `output_data_schema` | Function pod URI / output dir |
| 2–5 | `schema_utils.py:411` → `compute_source_schema_hash` | `(tag_schema, data_schema)` tuple | System-tag column names |
| 6 | `base.py` → `RootSource.pipeline_identity_structure` | `(tag_schema, data_schema)` | Pipeline DB path Merkle base |
| 7 | `source_node.py` → `identity_structure` | `("source_node", name, tag_schema, data_schema)` | Source content identity |
| 8 | `source_node.py` → `pipeline_identity_structure` | `(tag_schema, data_schema)` | Source pipeline identity |
| 9 | `function_pod.py` → delegates to `DataFunction.uri` | via `output_data_schema_hash` | Function pod pipeline identity |

All sites are correct and stable. No changes needed at call sites beyond the handler fix.

---

## Convention (to be documented in `CLAUDE.md` / code comments)

> **Schema hashing convention:**
> All schema hashing goes through `SchemaHandler` → `TypeObjectHandler` per field.
> `TypeObjectHandler` uses `logical_type_name` for registered orcapod types and
> `builtins.*` / stdlib paths for native types. Both are stable across module renames.
>
> `Schema.optional_fields` is intentionally excluded from the hash.
>
> Every type in a schema must be Arrow-translatable. `SchemaHandler` enforces this when
> a `type_converter` is available, raising `TypeError` for unregistered types.
>
> To opt into raw Python-object hashing (rare, requires justification), call
> `hash_object` directly on a plain `dict` rather than a `Schema` object.

---

## Testing

### New tests

1. **Module-rename regression at schema level** (`test_type_annotation_golden.py` or new file):
   ```python
   def test_schema_hash_stable_across_module_rename():
       schema = Schema({"f": op.File})
       before = hasher.hash_object(schema).to_string()
       original = op.File.__module__
       try:
           op.File.__module__ = "orcapod.extension_types.file_type"  # simulate old path
           after = hasher.hash_object(schema).to_string()
       finally:
           op.File.__module__ = original
       assert before == after, "Schema hash must not change on module rename"
   ```

2. **SchemaHandler is reached** (not `_expand_mapping`):
   ```python
   def test_schema_routes_to_schema_handler(monkeypatch):
       called = []
       original_handle = SchemaHandler.handle
       def patched(self, obj, hasher):
           called.append(obj)
           return original_handle(self, obj, hasher)
       monkeypatch.setattr(SchemaHandler, "handle", patched)
       hasher.hash_object(Schema({"x": int}))
       assert len(called) == 1
   ```

3. **Arrow-translatability guard**:
   ```python
   def test_schema_handler_rejects_unregistered_type():
       class Unregistered: pass
       schema = Schema({"x": Unregistered})
       with pytest.raises(TypeError, match="not Arrow-translatable"):
           hasher_with_converter.hash_object(schema)
   ```

4. **`optional_fields` excluded** (explicit documentation test):
   ```python
   def test_schema_hash_ignores_optional_fields():
       s1 = Schema({"f": op.File})
       s2 = Schema({"f": op.File}, optional_fields={"f"})
       assert hasher.hash_object(s1).to_string() == hasher.hash_object(s2).to_string()
   ```

5. **Existing `TestSchemaHashStability` golden tests** must continue to pass without
   regenerating `schema_hash_golden.json`.

---

## Out of Scope

- Changing hash values (no intentional cache bust from this change).
- Including `optional_fields` in the schema hash (intentionally excluded — see above).
- Hashing schemas via Arrow IPC serialization (not needed; TypeObjectHandler already
  provides stable type identity via `logical_type_name`).
- Non-schema hashing surfaces (function signature hashing is handled separately by
  ITL-638 / `FunctionSignatureExtractor`).
