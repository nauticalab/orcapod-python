## `src/orcapod/logical_types/`

### ET1 — `make_polars_extension_type` cannot accept a storage type containing nested extension types
**Status:** open
**Severity:** medium

`make_polars_extension_type` computes the Polars storage dtype by calling:
```python
pl.from_arrow(pa.array([], type=arrow_storage_type)).dtype
```
This fails with `ArrowNotImplementedError: extension` when `arrow_storage_type` is a struct
(or list) whose fields include any `pa.ExtensionType` node — for example, a dataclass whose
fields include `uuid.UUID` (stored as `orcapod.uuid` extension over `pa.large_binary()`).

Polars's Arrow IPC bridge handles top-level extension types via `pl.BaseExtension`, but has no
path for extension types *nested inside* a struct at dtype-inference time.

**Workaround:** `register_python_class` and `register_storage_type` both uphold a
*storage-safe* invariant: the returned type may be a `pa.ExtensionType` at the top level,
but struct fields and list value types at any depth are always plain (non-extension) types.
`DataclassLogicalTypeFactory.create_for_python_type` strips the top-level extension type
with a one-liner (`if isinstance(arrow_type, pa.ExtensionType): arrow_type = arrow_type.storage_type`)
before inserting it into the struct, so the struct passed to `make_polars_extension_type`
and `pa.Table.from_pylist` never contains nested extension types. The private
`_strip_ext_to_storage` recursive helper was removed in PLT-1720; the stripping is now
trivially correct because the storage-safe invariant guarantees `.storage_type` is always
already clean.

**Also affects `pa.Table.from_pylist`:** the same restriction applies to PyArrow's
`pa.Table.from_pylist` (and `pa.array`) — neither can build an array from a struct type
whose fields are `pa.ExtensionType` nodes, for the same underlying reason. The stripping
in `create_for_python_type` fixes both issues simultaneously.

**Polars round-trip fidelity:** once the storage struct contains only plain types (no
nested extension types), the full Arrow → Polars → Arrow round-trip for the *outermost*
extension type is faithful: extension name, metadata bytes, and storage struct are all
preserved. Only the inner field schema (already stripped) is absent.

**Fix needed:** Once PyArrow (and Polars) support nested extension types natively in struct
construction and Arrow↔Polars conversion, the stripping one-liner in `create_for_python_type`
can be removed and `make_polars_extension_type` can accept extension-typed storage directly.
Track upstream PyArrow / Polars issues.
