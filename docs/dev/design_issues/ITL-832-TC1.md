## `src/orcapod/semantic_types/universal_converter.py` — struct Python types

### TC1 — A struct's Python type depended on its children's string/list layouts
**Status:** resolved
**Severity:** medium
**Issue:** ITL-832 (follow-up: ITL-841)

The type converter synthesized one `TypedDict` class per exact Arrow struct type, so
`struct<b: string>` and `struct<b: large_string>` (hash-identical) became two different Python
classes with the same name. A stream with a `struct<b: string>` column therefore reported one
class in `output_schema()` and another in its own `Data` rows (which normalize to large types),
and an operator that normalizes its output (ITL-832) could not match its predicted schema.

**Fix:** The converter keys the class by `normalize_to_large_types(struct)` and maps it back to
the large-normalized struct, so both layouts share one class; width, nullability and logical
types still distinguish structs. Content and pipeline hashes of struct columns are pinned to
their values on `main` in `tests/test_semantic_types/test_struct_layout_identity.py`. Maps and
unions do not have this problem: their Python types do not depend on child layouts. This is a
workaround (`TODO(ITL-841)` in the code); ITL-841 revisits the treatment of large variants
everywhere.
