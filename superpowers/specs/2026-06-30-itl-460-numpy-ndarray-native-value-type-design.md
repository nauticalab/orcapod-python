# Design: `numpy.ndarray` as a native value type

**Date:** 2026-06-30
**Issue:** ITL-460
**Status:** Approved

---

## Overview

A pod cannot return (or a source cannot carry) a `numpy.ndarray` as a value. The universal
type converter falls through for unregistered types and raises:

```
ValueError: Unsupported Python type: <class 'numpy.ndarray'>.
```

numpy *scalar* dtypes (`np.int8`, `np.float64`, etc.) are already handled in
`universal_converter.py`. What is missing is support for `np.ndarray` itself — the
multi-dimensional array type.

This design adds a built-in logical type `LogicalNumpyArray` that maps `np.ndarray` ↔ Arrow
`large_binary` using numpy's own `.npy` binary format as the storage envelope, plus a
`NumpyArrayHandler` for content hashing. It uses a single, unparameterized extension type:
all `np.ndarray` instances — 1-D, 2-D, N-D, and structured (record) arrays — share the same
`numpy.ndarray` extension. Future issues may add parameterized variants
(e.g. `NDArray[np.float64]` → `numpy.ndarray[float64]`) as distinct logical types; the
registry supports this without breaking the baseline type.

---

## Design

### `LogicalNumpyArray`

**File:** `src/orcapod/extension_types/numpy_type.py` (new)

```
python_type:        numpy.ndarray
logical_type_name:  "numpy.ndarray"
arrow_ext_name:     "numpy.ndarray"
arrow storage type: pa.large_binary()
polars storage:     pa.large_binary()  (same pattern as LogicalUUID)
```

Note: the extension name `"numpy.ndarray"` intentionally borrows numpy's namespace rather
than the `orcapod.*` convention used by built-ins like `LogicalPath` and `LogicalUUID`.
The rationale is that `numpy.ndarray` is a highly stable, well-known name unlikely to
change, and using it makes the Arrow schema immediately self-documenting. Any future
ecosystem tools that independently serialize numpy arrays to Arrow may converge on the
same name, enabling interoperability.

**Serialization — `python_to_storage`:**

```python
import io
import numpy as np

buf = io.BytesIO()
np.save(buf, array)
return buf.getvalue()
```

numpy's `.npy` format is a stable, self-describing binary format that encodes dtype
(including structured/record dtype field names and types), shape, byte order, and the raw
data buffer. It handles 1-D, 2-D, N-D, and structured arrays without any custom parsing.

**Deserialization — `storage_to_python`:**

```python
buf = io.BytesIO(storage_value)
return np.load(buf, allow_pickle=False)
```

`allow_pickle=False` is required for security: it prevents deserializing pickled object
arrays. Object-dtype arrays (`dtype=object`) are therefore **not supported** — `np.save`
will raise when attempting to save them. This is documented as a known limitation.

**Class structure** follows the existing `LogicalPath` / `LogicalUUID` pattern:
- Class-level `_arrow_ext_class` and `_polars_ext_class` (created once via
  `make_arrow_extension_type` / `make_polars_extension_type`)
- Class-level `_arrow_ext` and `_polars_ext` instance caches (populated on first call)
- `get_arrow_extension_type()` and `get_polars_extension_type()` return cached singletons

---

### `NumpyArrayHandler`

**File:** `src/orcapod/hashing/semantic_hashing/builtin_handlers.py` (modified)

```python
class NumpyArrayHandler:
    """Hasher for ``numpy.ndarray`` — content hash via numpy's .npy binary format.

    Returns the raw .npy bytes, which encode dtype, shape, byte order, and data
    in numpy's stable on-disk format. This is identical to what ``LogicalNumpyArray``
    stores in Arrow, so the hash input and storage bytes are always consistent.
    """

    def handle(self, obj: Any, hasher: "SemanticHasherProtocol") -> bytes:
        import io
        import numpy as np
        if not isinstance(obj, np.ndarray):
            raise TypeError(
                f"NumpyArrayHandler: expected numpy.ndarray, got {type(obj)!r}"
            )
        buf = io.BytesIO()
        np.save(buf, obj)
        return buf.getvalue()
```

The `.npy` bytes are the canonical hash input because they are:
- **Self-describing**: dtype (including structured field names), shape, byte order, and data
  are all encoded
- **Content-stable**: same array content → same bytes → same hash
- **Consistent with storage**: the hash input is exactly what `LogicalNumpyArray` stores in
  Arrow — no divergence between "what we store" and "what we hash"

---

### Registration in `v0.1.json`

**File:** `src/orcapod/contexts/data/v0.1.json` (modified)

Two additions to the default context config:

**1. In `logical_types` list** (adds Arrow extension type to the default registry):

```json
{
    "_class": "orcapod.extension_types.numpy_type.LogicalNumpyArray",
    "_config": {}
}
```

**2. In `python_type_handler_registry.handlers` list** (adds semantic hash handler):

```json
[
    {"_type": "numpy.ndarray"},
    {"_class": "orcapod.hashing.semantic_hashing.builtin_handlers.NumpyArrayHandler", "_config": {}}
]
```

---

### Exports and public API

**File:** `src/orcapod/extension_types/__init__.py` (modified)

Add `LogicalNumpyArray` to the imports and `__all__` list, with an `# ITL-460` comment
following the pattern of `LogicalFile` (ITL-450) and `LogicalDirectory` (ITL-451).

No top-level `orcapod` namespace export is added — `np.ndarray` is already the user-facing
type. Users annotate their function signatures with `np.ndarray` directly; orcapod handles
the conversion transparently.

---

### Dependency

**File:** `pyproject.toml` (modified)

Add `numpy>=1.26.4` to `[project] dependencies`. numpy is already a transitive requirement
through both `pyarrow` and `polars` (both pin numpy as a dependency), so this makes the
implicit dependency explicit.

---

## Limitations

- **Object-dtype arrays (`dtype=object`) are not supported.** `np.save` pickles them;
  `np.load` with `allow_pickle=False` will raise. Users must store object arrays via another
  mechanism (e.g. serialize manually to `bytes`, or use a `File` value).
- **Future parameterized types.** `NDArray[np.float64]` and similar typed array annotations
  are not handled by this logical type. A future issue may add `numpy.ndarray[float64]`
  (and similar) as distinct logical types, registered alongside `numpy.ndarray` in the
  same registry without conflict.

---

## Testing

**New file:** `tests/test_extension_types/test_numpy_type.py`

- `LogicalNumpyArray()` satisfies `LogicalTypeProtocol` (isinstance check)
- `logical_type_name` is `"numpy.ndarray"`
- `python_type` is `numpy.ndarray`
- `get_arrow_extension_type().extension_name` is `"numpy.ndarray"`
- `get_arrow_extension_type().storage_type` is `pa.large_binary()`
- `get_arrow_extension_type()` returns the same cached object on repeated calls
- `get_polars_extension_type()` returns the same cached object on repeated calls
- `python_to_storage` returns `bytes`
- Round-trip cases (storage → python → storage produces equal array):
  - 1-D float64 array
  - 2-D int32 array
  - 3-D uint8 array
  - Zero-size array (`np.array([])`)
  - Single-element array
  - Structured (record) array with named fields (`np.dtype([('x', '<f8'), ('y', '<i8')])`)
  - F-order array (round-trip preserves data; `np.array_equal` passes)

**`NumpyArrayHandler` tests** — in `tests/test_hashing/` (new or existing file):

- Returns `bytes`
- Same array content → same bytes
- Different dtype → different bytes
- Same values, different shape → different bytes
- Structured array: field names and types are preserved in hash bytes
- Raises `TypeError` for non-ndarray input

**Integration test** — verify that a `FunctionPod` annotated with `-> np.ndarray` return
type can execute end-to-end (write to Arrow table, read back, compare with `np.array_equal`).
This can go in `tests/test_extension_types/test_roundtrips.py` or an equivalent integration
test file if one exists.
