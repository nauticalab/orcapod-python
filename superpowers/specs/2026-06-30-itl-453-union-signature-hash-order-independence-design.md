# ITL-453: Union-Typed Signature Hash Order Independence

**Date:** 2026-06-30
**Issue:** [ITL-453](https://linear.app/enigma-metamorphic/issue/ITL-453)
**Status:** Design approved

---

## Overview

When a function pod's input (or return) type annotation is a union — e.g. `x: str | Path` — the
function signature hash must be order-independent over the union members. `def foo(x: str | Path)`
and `def foo(x: Path | str)` are semantically identical and must produce the same
`_function_signature_hash`.

The bug is confirmed: `get_function_signature()` in `hash_utils.py` builds the signature as a
plain string using `str(param)`, which calls `inspect.formatannotation(annotation)`. For union
types this falls through to `repr(annotation)`, which reflects declaration order. Two semantically
identical signatures therefore hash differently.

---

## Root Cause

```
PythonDataFunction.__init__
    └─ get_function_signature(function)  →  string (declaration-order union repr)
           └─ semantic_hasher.hash_object(string)  →  primitive hash
```

`str(param)` for `x: str | Path` → `"x: str | pathlib.Path"`
`str(param)` for `x: Path | str` → `"x: pathlib.Path | str"`

These are different strings, so they hash differently.

The semantic hasher *does* already handle union type *objects* in an order-independent way
(`UnionTypeHandler` / `GenericAliasHandler` both sort args before hashing), but
`_function_signature_hash` goes through a string primitive, bypassing those handlers entirely.

The same bug exists in `FunctionSignatureExtractor.extract_function_info()` for its `params`
string field. (Its `returns` field stores the actual type object and is already order-independent
via `UnionTypeHandler`.)

---

## Design (Option B — Canonical Annotation String)

### Approach

Add two private helpers to `src/orcapod/hashing/hash_utils.py` and apply them at both affected
call sites. The fix is surgical: only union-typed annotations are changed; all other annotations
produce exactly the same string as before.

### New Helpers in `hash_utils.py`

**`_is_union_annotation(annotation) -> bool`**

Returns `True` if `annotation` is:
- `types.UnionType` — PEP 604 syntax `X | Y` (Python 3.10+)
- A `typing._GenericAlias` with `__origin__ is typing.Union` — covers `typing.Union[X, Y]` and
  `typing.Optional[X]`

Returns `False` for all other types (plain types, generic aliases with non-Union origins, etc.).

**`_canonical_annotation_str(annotation) -> str`**

Returns a stable, canonical string for a type annotation:

- If `_is_union_annotation(annotation)`: extract `__args__`, recursively call
  `_canonical_annotation_str` on each member, sort the resulting strings byte-wise
  (lexicographic), join with `" | "`.
- Otherwise: `inspect.formatannotation(annotation)` — identical to what `str(param)` already
  produces for non-union types. No behavioral change for any non-union annotation.

**Canonical ordering key:** fully qualified type name as produced by `inspect.formatannotation`
(e.g., `"pathlib.Path"`, `"str"`, `"bytes"`). Sorted byte-wise. Stable across Python versions
and machines; does not depend on `id()`, `hash()`, or insertion order.

**Examples:**

| Input annotation | Canonical string |
|---|---|
| `str \| Path` | `"pathlib.Path \| str"` |
| `Path \| str` | `"pathlib.Path \| str"` |
| `str \| Path \| bytes` | `"bytes \| pathlib.Path \| str"` |
| `bytes \| str \| Path` | `"bytes \| pathlib.Path \| str"` |
| `int` | `"int"` (unchanged) |
| `pathlib.Path` | `"pathlib.Path"` (unchanged) |

### Changes to `get_function_signature()` in `hash_utils.py`

**Parameters:** For each parameter, call `str(param)` as before. Then, if
`_is_union_annotation(param.annotation)`, compute:
```
old_ann = inspect.formatannotation(param.annotation)
new_ann = _canonical_annotation_str(param.annotation)
```
and replace `": {old_ann}"` with `": {new_ann}"` in `param_str` (first occurrence only — safe,
because `": "` prefix distinguishes the annotation from any default value).

If the annotation is not a union, `old_ann == new_ann` (both use `inspect.formatannotation`),
so no substitution is made — the output is byte-for-byte identical to the current behavior.

**Return type:** If `_is_union_annotation(sig.return_annotation)`, emit
`f"-> {_canonical_annotation_str(ret)}"` instead of `f"-> {ret}"`. Otherwise, the existing
`f"-> {ret}"` is unchanged (preserves the current `"<class 'str'>"` format for plain types).

### Changes to `FunctionSignatureExtractor.extract_function_info()` in `function_info_extractors.py`

Apply the same param-string substitution as above to the `params` string field. Import
`_is_union_annotation` and `_canonical_annotation_str` from `hash_utils`.

The `returns` field stores the actual annotation object (not a string). The semantic hasher's
existing `UnionTypeHandler` already handles it order-independently. No change needed.

### Non-Regression Guarantee

For any function with **no union-typed parameters or return type**:
- `_is_union_annotation(annotation)` = `False` for every annotation
- No substitution is performed
- The output of `get_function_signature()` is byte-for-byte identical to the current output
- `_function_signature_hash` is unchanged

For functions with union-typed parameters or return type:
- The hash changes to a canonical (order-independent) value
- This is an intentional breaking change, acceptable under the v0.1 green-field stance
- Document in release notes

---

## Out of Scope

- Nested union canonicalization inside generic types (e.g. `list[str | Path]`) — top-level
  unions in parameter and return positions are sufficient for this issue.
- Type equivalence beyond union reordering (`int` vs `numpy.int64`, `dict` vs `Mapping`) —
  out of scope per the issue.
- Argument position reordering — `foo(x: int, y: str)` and `foo(y: str, x: int)` remain
  distinct.

---

## Test Coverage

### New test class in `tests/test_core/data_function/test_data_function.py`

`TestSignatureHashUnionOrderIndependence`:

1. **2-member union, input param**: `foo(x: str | Path)` and `foo(x: Path | str)` produce
   identical `_function_signature_hash`.
2. **3-member union, all permutations**: `foo(x: str | Path | bytes)`,
   `foo(x: bytes | str | Path)`, and `foo(x: Path | bytes | str)` all produce the same hash.
3. **Return-type union**: `foo() -> str | Path` and `foo() -> Path | str` produce the same hash.
4. **Non-union regression**: `foo(x: int)` produces the same hash as it did before the fix
   (captured as a golden assertion that the hash does not change when annotations are non-union).
5. **Canonical ordering assertion**: The canonical string for `str | Path` is
   `"pathlib.Path | str"` (P before s).

### New file `tests/test_hashing/test_hash_utils.py`

Lower-level tests of `get_function_signature` and `_canonical_annotation_str` directly:

1. `_canonical_annotation_str` returns `"pathlib.Path | str"` for both `str | Path` and
   `Path | str`.
2. `_canonical_annotation_str` on a non-union type returns the same string as
   `inspect.formatannotation`.
3. `get_function_signature` produces the same string for `str | Path` and `Path | str`
   parameter annotations.
4. `get_function_signature` produces the same string for `str | Path` and `Path | str`
   return-type annotations.

---

## Breaking Change Note

Functions whose parameter or return type annotations include a union type will receive a
different `_function_signature_hash` after this fix. Any cached pipeline outputs keyed on
a function-signature hash that contained a union will be invalidated. This is expected and
acceptable per the v0.1 green-field stance.
