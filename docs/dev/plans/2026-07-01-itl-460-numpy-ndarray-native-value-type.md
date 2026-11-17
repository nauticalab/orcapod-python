# numpy.ndarray Native Value Type Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `numpy.ndarray` as a first-class orcapod value type so pods can return arrays directly without wrapping them in `op.File`.

**Architecture:** Add `LogicalNumpyArray` (a new `BaseLogicalType` subclass) that maps `np.ndarray` ↔ Arrow `large_binary` using numpy's own `.npy` binary format, register it in the default context via `v0.1.json`, and add `NumpyArrayHandler` to the semantic hasher registry so arrays are content-hashed correctly. Object-dtype arrays are rejected eagerly at both write and hash time.

**Tech Stack:** Python, PyArrow (`pa.large_binary`, extension types), Polars, numpy (`np.save`/`np.load`), pytest.

---

## File Map

| Action | Path | Responsibility |
|---|---|---|
| Create | `src/orcapod/extension_types/numpy_type.py` | `LogicalNumpyArray` class |
| Modify | `src/orcapod/extension_types/__init__.py` | Re-export `LogicalNumpyArray` |
| Modify | `src/orcapod/hashing/semantic_hashing/builtin_handlers.py` | Add `NumpyArrayHandler` class |
| Modify | `src/orcapod/contexts/data/v0.1.json` | Register both in default context |
| Modify | `pyproject.toml` | Explicit numpy dependency |
| Create | `tests/test_extension_types/test_numpy_type.py` | Unit tests for `LogicalNumpyArray` |
| Create | `tests/test_hashing/test_numpy_handler.py` | Unit tests for `NumpyArrayHandler` |
| Modify | `tests/test_extension_types/test_roundtrips.py` | Integration round-trip test |

---

## Task 1: Add `LogicalNumpyArray` with failing unit tests

**Files:**
- Create: `tests/test_extension_types/test_numpy_type.py`
- Create: `src/orcapod/extension_types/numpy_type.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_extension_types/test_numpy_type.py`:

```python
"""Tests for LogicalNumpyArray."""
from __future__ import annotations

import io

import numpy as np
import pyarrow as pa
import pytest

from orcapod.extension_types.protocols import LogicalTypeProtocol


class TestLogicalNumpyArrayProtocol:
    def test_isinstance_logical_type(self):
        from orcapod.extension_types.numpy_type import LogicalNumpyArray
        assert isinstance(LogicalNumpyArray(), LogicalTypeProtocol)

    def test_logical_type_name(self):
        from orcapod.extension_types.numpy_type import LogicalNumpyArray
        assert LogicalNumpyArray().logical_type_name == "numpy.ndarray"

    def test_python_type(self):
        from orcapod.extension_types.numpy_type import LogicalNumpyArray
        assert LogicalNumpyArray().python_type is np.ndarray

    def test_arrow_ext_name(self):
        from orcapod.extension_types.numpy_type import LogicalNumpyArray
        assert LogicalNumpyArray().get_arrow_extension_type().extension_name == "numpy.ndarray"

    def test_arrow_ext_storage_type(self):
        from orcapod.extension_types.numpy_type import LogicalNumpyArray
        assert LogicalNumpyArray().get_arrow_extension_type().storage_type == pa.large_binary()

    def test_arrow_ext_is_cached(self):
        from orcapod.extension_types.numpy_type import LogicalNumpyArray
        lt = LogicalNumpyArray()
        assert lt.get_arrow_extension_type() is lt.get_arrow_extension_type()

    def test_polars_ext_is_cached(self):
        from orcapod.extension_types.numpy_type import LogicalNumpyArray
        lt = LogicalNumpyArray()
        assert lt.get_polars_extension_type() is lt.get_polars_extension_type()


class TestLogicalNumpyArrayStorage:
    def test_python_to_storage_returns_bytes(self):
        from orcapod.extension_types.numpy_type import LogicalNumpyArray
        arr = np.array([1.0, 2.0, 3.0])
        result = LogicalNumpyArray().python_to_storage(arr)
        assert isinstance(result, bytes)

    def test_object_dtype_raises_before_save(self):
        from orcapod.extension_types.numpy_type import LogicalNumpyArray
        arr = np.array([1, "hello", None], dtype=object)
        with pytest.raises(ValueError, match="object-dtype"):
            LogicalNumpyArray().python_to_storage(arr)

    def test_structured_with_object_field_raises(self):
        from orcapod.extension_types.numpy_type import LogicalNumpyArray
        arr = np.zeros(3, dtype=[("label", object), ("value", np.int32)])
        with pytest.raises((ValueError, Exception)):
            LogicalNumpyArray().python_to_storage(arr)


class TestLogicalNumpyArrayRoundTrip:
    def _rt(self, arr: np.ndarray) -> np.ndarray:
        from orcapod.extension_types.numpy_type import LogicalNumpyArray
        lt = LogicalNumpyArray()
        return lt.storage_to_python(lt.python_to_storage(arr))

    def test_1d_float64(self):
        arr = np.array([1.0, 2.0, 3.0], dtype=np.float64)
        assert np.array_equal(self._rt(arr), arr)
        assert self._rt(arr).dtype == arr.dtype

    def test_2d_int32(self):
        arr = np.array([[1, 2], [3, 4]], dtype=np.int32)
        result = self._rt(arr)
        assert np.array_equal(result, arr)
        assert result.shape == arr.shape
        assert result.dtype == arr.dtype

    def test_3d_uint8(self):
        arr = np.zeros((2, 3, 4), dtype=np.uint8)
        result = self._rt(arr)
        assert np.array_equal(result, arr)
        assert result.shape == arr.shape

    def test_zero_size(self):
        arr = np.array([], dtype=np.float32)
        result = self._rt(arr)
        assert np.array_equal(result, arr)
        assert result.dtype == arr.dtype

    def test_single_element(self):
        arr = np.array([42], dtype=np.int64)
        result = self._rt(arr)
        assert np.array_equal(result, arr)

    def test_structured_record_array(self):
        dt = np.dtype([("x", np.float64), ("y", np.int32)])
        arr = np.array([(1.0, 2), (3.0, 4)], dtype=dt)
        result = self._rt(arr)
        assert result.dtype == arr.dtype
        assert np.array_equal(result["x"], arr["x"])
        assert np.array_equal(result["y"], arr["y"])

    def test_fortran_order(self):
        arr = np.asfortranarray(np.array([[1, 2], [3, 4]], dtype=np.float64))
        result = self._rt(arr)
        assert np.array_equal(result, arr)
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
cd /path/to/orcapod-python
uv run pytest tests/test_extension_types/test_numpy_type.py -v 2>&1 | head -30
```

Expected: `ModuleNotFoundError` or `ImportError` — `numpy_type` does not exist yet.

- [ ] **Step 3: Create `src/orcapod/extension_types/numpy_type.py`**

```python
"""``numpy.ndarray`` logical type for orcapod.

``LogicalNumpyArray`` maps ``numpy.ndarray`` ↔ Arrow ``large_binary`` using
numpy's own ``.npy`` binary format as the storage envelope. This preserves
dtype (including structured/record field names and types), shape, byte order,
and the raw data buffer for all array kinds: 1-D, 2-D, N-D, and structured.

Object-dtype arrays (``dtype=object`` or structured dtypes containing object
fields) are explicitly rejected — they require pickling, which is disabled for
security. See the ``Limitations`` section in the ITL-460 design spec.
"""

from __future__ import annotations

import io
from typing import TYPE_CHECKING, Any

import numpy as np
import polars as pl
import pyarrow as pa

from orcapod.extension_types.base_logical_type import BaseLogicalType
from orcapod.extension_types.registry import make_arrow_extension_type, make_polars_extension_type

if TYPE_CHECKING:
    from orcapod.extension_types.protocols import TypeConverterProtocol


class LogicalNumpyArray(BaseLogicalType):
    """Logical type for ``numpy.ndarray``.

    Stores arrays as Arrow ``large_binary`` using numpy's ``.npy`` on-disk
    format as the storage envelope. The ``.npy`` format is self-describing:
    it encodes dtype (including structured/record field names), shape, byte
    order, and raw data — so no custom parsing is required and all array
    kinds round-trip exactly.

    The extension name ``"numpy.ndarray"`` is chosen over the ``orcapod.*``
    convention because it is a highly stable, well-known name that makes
    Arrow schemas immediately self-documenting and may enable interoperability
    with other ecosystem tools that independently serialise numpy arrays.

    Object-dtype arrays are not supported. ``python_to_storage`` raises
    ``ValueError`` immediately (before calling ``np.save``) when
    ``array.dtype.kind == "O"``. ``np.save(..., allow_pickle=False)`` acts as
    a secondary guard for structured dtypes that contain object-typed fields.

    Example:
        >>> lt = LogicalNumpyArray()
        >>> arr = np.array([[1.0, 2.0], [3.0, 4.0]], dtype=np.float64)
        >>> recovered = lt.storage_to_python(lt.python_to_storage(arr))
        >>> np.array_equal(recovered, arr)
        True
    """

    _arrow_ext_class = make_arrow_extension_type("numpy.ndarray", pa.large_binary())
    _arrow_ext: pa.ExtensionType | None = None
    _polars_ext_class = make_polars_extension_type("numpy.ndarray", pa.large_binary())
    _polars_ext: pl.BaseExtension | None = None

    logical_type_name: str = "numpy.ndarray"
    python_type: type = np.ndarray

    def get_arrow_extension_type(self) -> pa.ExtensionType:
        """Return the cached Arrow extension type for ``numpy.ndarray``.

        Returns:
            A ``pa.ExtensionType`` instance with extension name
            ``"numpy.ndarray"`` and storage type ``pa.large_binary()``.
        """
        if LogicalNumpyArray._arrow_ext is None:
            LogicalNumpyArray._arrow_ext = LogicalNumpyArray._arrow_ext_class()
        return LogicalNumpyArray._arrow_ext

    def get_polars_extension_type(self) -> pl.BaseExtension:
        """Return the cached Polars extension type for ``numpy.ndarray``.

        Returns:
            A ``pl.BaseExtension`` instance registered under
            ``"numpy.ndarray"``.
        """
        if LogicalNumpyArray._polars_ext is None:
            LogicalNumpyArray._polars_ext = LogicalNumpyArray._polars_ext_class()
        return LogicalNumpyArray._polars_ext

    def python_to_storage(self, value: Any, converter: TypeConverterProtocol | None = None) -> bytes:
        """Serialise a ``numpy.ndarray`` to its ``.npy`` byte representation.

        Args:
            value: A ``numpy.ndarray`` instance. Object-dtype arrays
                (``dtype.kind == "O"``) are rejected immediately.
            converter: Ignored. Present for protocol conformance.

        Returns:
            Raw bytes in numpy's ``.npy`` format encoding dtype, shape,
            byte order, and data.

        Raises:
            ValueError: If ``value.dtype.kind == "O"`` (object-dtype array),
                or if the array contains object-typed fields in a structured
                dtype (caught by ``allow_pickle=False`` in ``np.save``).
        """
        if value.dtype.kind == "O":
            raise ValueError(
                f"LogicalNumpyArray does not support object-dtype arrays "
                f"(dtype={value.dtype!r}). Object arrays require pickling, "
                "which is disabled for security. Serialise the array contents "
                "manually (e.g. to bytes or a File) before passing to orcapod."
            )
        buf = io.BytesIO()
        np.save(buf, value, allow_pickle=False)
        return buf.getvalue()

    def storage_to_python(self, storage_value: Any, converter: TypeConverterProtocol | None = None) -> np.ndarray:
        """Reconstruct a ``numpy.ndarray`` from its ``.npy`` byte representation.

        Args:
            storage_value: Raw bytes as stored in Arrow ``large_binary``.
            converter: Ignored. Present for protocol conformance.

        Returns:
            A ``numpy.ndarray`` with dtype and shape as originally stored.

        Raises:
            ValueError: If ``storage_value`` is not valid ``.npy`` bytes.
        """
        buf = io.BytesIO(bytes(storage_value))
        return np.load(buf, allow_pickle=False)
```

- [ ] **Step 4: Run tests to confirm they pass**

```bash
uv run pytest tests/test_extension_types/test_numpy_type.py -v
```

Expected: all tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/extension_types/numpy_type.py tests/test_extension_types/test_numpy_type.py
git commit -m "feat(extension_types): add LogicalNumpyArray for numpy.ndarray (ITL-460)"
```

---

## Task 2: Add `NumpyArrayHandler` to semantic hasher

**Files:**
- Modify: `src/orcapod/hashing/semantic_hashing/builtin_handlers.py`
- Create: `tests/test_hashing/test_numpy_handler.py`

- [ ] **Step 1: Write failing tests**

Create `tests/test_hashing/test_numpy_handler.py`:

```python
"""Tests for NumpyArrayHandler."""
from __future__ import annotations

import numpy as np
import pytest

from orcapod.hashing.semantic_hashing.builtin_handlers import NumpyArrayHandler


class TestNumpyArrayHandler:
    def test_returns_bytes(self):
        handler = NumpyArrayHandler()
        arr = np.array([1.0, 2.0, 3.0])
        result = handler.handle(arr, hasher=None)
        assert isinstance(result, bytes)

    def test_same_content_same_bytes(self):
        handler = NumpyArrayHandler()
        arr = np.array([1, 2, 3], dtype=np.int32)
        assert handler.handle(arr, None) == handler.handle(arr.copy(), None)

    def test_different_dtype_different_bytes(self):
        handler = NumpyArrayHandler()
        a = np.array([1, 2, 3], dtype=np.int32)
        b = np.array([1, 2, 3], dtype=np.float64)
        assert handler.handle(a, None) != handler.handle(b, None)

    def test_same_values_different_shape_different_bytes(self):
        handler = NumpyArrayHandler()
        a = np.array([1, 2, 3, 4], dtype=np.int32)
        b = np.array([[1, 2], [3, 4]], dtype=np.int32)
        assert handler.handle(a, None) != handler.handle(b, None)

    def test_structured_array_field_names_in_bytes(self):
        handler = NumpyArrayHandler()
        dt_xy = np.dtype([("x", np.float64), ("y", np.int32)])
        dt_ab = np.dtype([("a", np.float64), ("b", np.int32)])
        arr_xy = np.array([(1.0, 2)], dtype=dt_xy)
        arr_ab = np.array([(1.0, 2)], dtype=dt_ab)
        # Same values, same field types, different field names → different bytes
        assert handler.handle(arr_xy, None) != handler.handle(arr_ab, None)

    def test_raises_type_error_for_non_ndarray(self):
        handler = NumpyArrayHandler()
        with pytest.raises(TypeError, match="numpy.ndarray"):
            handler.handle([1, 2, 3], hasher=None)

    def test_raises_value_error_for_object_dtype(self):
        handler = NumpyArrayHandler()
        arr = np.array([1, "hello", None], dtype=object)
        with pytest.raises(ValueError, match="object-dtype"):
            handler.handle(arr, hasher=None)
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
uv run pytest tests/test_hashing/test_numpy_handler.py -v 2>&1 | head -20
```

Expected: `ImportError` — `NumpyArrayHandler` does not exist yet.

- [ ] **Step 3: Add `NumpyArrayHandler` to `builtin_handlers.py`**

Add the following class at the end of the file, just before the `register_builtin_python_type_handlers` function (i.e. after `DirectoryHandler`):

```python
class NumpyArrayHandler:
    """Hasher for ``numpy.ndarray`` — content hash via numpy's ``.npy`` binary format.

    Returns the raw ``.npy`` bytes produced by ``np.save``, which encode dtype
    (including structured/record field names), shape, byte order, and data in
    numpy's stable on-disk format. This is identical to what ``LogicalNumpyArray``
    stores in Arrow, so the hash input and storage bytes are always consistent.

    Object-dtype arrays are rejected with ``ValueError`` immediately (before
    ``np.save`` is called). Structured dtypes containing object-typed fields
    are caught by ``allow_pickle=False`` in ``np.save``.
    """

    def handle(self, obj: Any, hasher: "SemanticHasherProtocol") -> bytes:
        """Return the ``.npy`` bytes for ``obj``.

        Args:
            obj: A ``numpy.ndarray`` instance.
            hasher: Ignored. Present for protocol conformance.

        Returns:
            Raw ``.npy`` bytes encoding dtype, shape, byte order, and data.

        Raises:
            TypeError: If ``obj`` is not a ``numpy.ndarray``.
            ValueError: If ``obj`` has ``dtype.kind == "O"`` (object dtype).
        """
        import io
        import numpy as np
        if not isinstance(obj, np.ndarray):
            raise TypeError(
                f"NumpyArrayHandler: expected numpy.ndarray, got {type(obj)!r}"
            )
        if obj.dtype.kind == "O":
            raise ValueError(
                f"NumpyArrayHandler does not support object-dtype arrays "
                f"(dtype={obj.dtype!r}). Object arrays require pickling, "
                "which is disabled for security."
            )
        buf = io.BytesIO()
        np.save(buf, obj, allow_pickle=False)
        return buf.getvalue()
```

- [ ] **Step 4: Run tests to confirm they pass**

```bash
uv run pytest tests/test_hashing/test_numpy_handler.py -v
```

Expected: all tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/hashing/semantic_hashing/builtin_handlers.py tests/test_hashing/test_numpy_handler.py
git commit -m "feat(hashing): add NumpyArrayHandler for numpy.ndarray content hashing (ITL-460)"
```

---

## Task 3: Register in `v0.1.json` and export from `__init__.py`

**Files:**
- Modify: `src/orcapod/contexts/data/v0.1.json`
- Modify: `src/orcapod/extension_types/__init__.py`
- Modify: `pyproject.toml`

- [ ] **Step 1: Add `LogicalNumpyArray` to `v0.1.json` `logical_types` list**

Open `src/orcapod/contexts/data/v0.1.json`. In the `logical_type_registry._config.logical_types` array, add the following entry after the `LogicalDirectory` entry:

```json
{
    "_class": "orcapod.extension_types.numpy_type.LogicalNumpyArray",
    "_config": {}
}
```

The full `logical_types` array should now end with:
```json
{
    "_class": "orcapod.extension_types.file_type.LogicalFile",
    "_config": {}
},
{
    "_class": "orcapod.extension_types.directory_type.LogicalDirectory",
    "_config": {}
},
{
    "_class": "orcapod.extension_types.numpy_type.LogicalNumpyArray",
    "_config": {}
}
```

- [ ] **Step 2: Add `NumpyArrayHandler` to `v0.1.json` `handlers` list**

In the same file, inside `python_type_handler_registry._config.handlers`, add the following entry (after the `pyarrow.RecordBatch` entry):

```json
[{"_type": "numpy.ndarray"}, {"_class": "orcapod.hashing.semantic_hashing.builtin_handlers.NumpyArrayHandler", "_config": {}}]
```

- [ ] **Step 3: Add `LogicalNumpyArray` to `extension_types/__init__.py`**

Open `src/orcapod/extension_types/__init__.py`. Add the import after the `LogicalDirectory` import line:

```python
from .numpy_type import LogicalNumpyArray  # ITL-460
```

Add `"LogicalNumpyArray"` to `__all__` after `"LogicalDirectory"`:

```python
    # ITL-451
    "LogicalDirectory",
    # ITL-460
    "LogicalNumpyArray",
```

- [ ] **Step 4: Add numpy to `pyproject.toml` dependencies**

Open `pyproject.toml`. In the `[project] dependencies` list, add:

```toml
"numpy>=1.26.4",
```

Place it alongside the other scientific stack deps (near `pyarrow`, `polars`).

- [ ] **Step 5: Verify the default context loads without error**

```bash
uv run python -c "
from orcapod.contexts import get_default_context
ctx = get_default_context()
registry = ctx.type_converter._logical_type_registry
lt = registry.get_by_python_type(__import__('numpy').ndarray)
print('Registered logical type:', lt)
print('logical_type_name:', lt.logical_type_name)
"
```

Expected output:
```
Registered logical type: <orcapod.extension_types.numpy_type.LogicalNumpyArray object at 0x...>
logical_type_name: numpy.ndarray
```

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/contexts/data/v0.1.json src/orcapod/extension_types/__init__.py pyproject.toml
git commit -m "feat(contexts): register LogicalNumpyArray and NumpyArrayHandler in default context (ITL-460)"
```

---

## Task 4: Integration round-trip test

**Files:**
- Modify: `tests/test_extension_types/test_roundtrips.py`

- [ ] **Step 1: Add the round-trip test**

Open `tests/test_extension_types/test_roundtrips.py`. Add the following test function after `test_builtin_uuid_round_trip`:

```python
def test_builtin_ndarray_round_trip(storage_backend: _StorageBackend, tmp_path: Path) -> None:
    """numpy.ndarray round-trips through storage with extension name ``numpy.ndarray``.

    Tests both a simple 1-D float64 array and a structured (record) array with
    named fields, since structured arrays are a primary motivation for this type.
    The read-side converter already knows about ``numpy.ndarray`` because it is
    registered in the default context (``v0.1.json``).
    """
    import numpy as np

    arr_simple = np.array([1.0, 2.0, 3.0], dtype=np.float64)
    arr_struct = np.array([(1.0, 10), (2.0, 20)], dtype=np.dtype([("x", np.float64), ("y", np.int32)]))

    # Simple 1-D float64 array
    result, read_converter = _write_and_read(
        {"col": np.ndarray},
        [{"col": arr_simple}],
        storage_backend,
        tmp_path / "simple",
    )

    field = result.schema.field("col")
    assert hasattr(field.type, "extension_name"), (
        f"Expected extension type on field 'col', got plain type {field.type!r}"
    )
    assert field.type.extension_name == "numpy.ndarray"

    rows = read_converter.arrow_table_to_python_dicts(result)
    assert len(rows) == 1
    assert isinstance(rows[0]["col"], np.ndarray)
    assert np.array_equal(rows[0]["col"], arr_simple)
    assert rows[0]["col"].dtype == arr_simple.dtype

    # Structured (record) array
    result2, read_converter2 = _write_and_read(
        {"col": np.ndarray},
        [{"col": arr_struct}],
        storage_backend,
        tmp_path / "struct",
    )

    rows2 = read_converter2.arrow_table_to_python_dicts(result2)
    assert len(rows2) == 1
    recovered = rows2[0]["col"]
    assert isinstance(recovered, np.ndarray)
    assert recovered.dtype == arr_struct.dtype
    assert np.array_equal(recovered["x"], arr_struct["x"])
    assert np.array_equal(recovered["y"], arr_struct["y"])
```

Also add `import numpy as np` to the top-level imports section of the file if not already present (check first — the import is inside the test function above to be safe either way).

- [ ] **Step 2: Run the round-trip test**

```bash
uv run pytest tests/test_extension_types/test_roundtrips.py::test_builtin_ndarray_round_trip -v
```

Expected: PASS for both `parquet` and `delta` backends.

- [ ] **Step 3: Run the full test suite to check for regressions**

```bash
uv run pytest tests/ -x -q
```

Expected: all existing tests continue to pass.

- [ ] **Step 4: Commit**

```bash
git add tests/test_extension_types/test_roundtrips.py
git commit -m "test(extension_types): add numpy.ndarray round-trip integration test (ITL-460)"
```

---

## Task 5: Final verification and push

- [ ] **Step 1: Run the complete test suite one more time**

```bash
uv run pytest tests/ -q
```

Expected: all tests pass, no failures.

- [ ] **Step 2: Verify the full feature works end-to-end with a quick smoke test**

```bash
uv run python -c "
import numpy as np
from orcapod.extension_types.numpy_type import LogicalNumpyArray

lt = LogicalNumpyArray()

# 1-D
a = np.array([1.0, 2.0, 3.0], dtype=np.float64)
assert np.array_equal(lt.storage_to_python(lt.python_to_storage(a)), a)

# 2-D
b = np.array([[1, 2], [3, 4]], dtype=np.int32)
assert np.array_equal(lt.storage_to_python(lt.python_to_storage(b)), b)

# Structured
c = np.array([(1.0, 2)], dtype=[('x', np.float64), ('y', np.int32)])
r = lt.storage_to_python(lt.python_to_storage(c))
assert np.array_equal(r['x'], c['x'])
assert np.array_equal(r['y'], c['y'])

# Object-dtype rejection
try:
    lt.python_to_storage(np.array([1, 'a'], dtype=object))
    assert False, 'Should have raised'
except ValueError as e:
    assert 'object-dtype' in str(e)

print('All smoke tests passed.')
"
```

Expected: `All smoke tests passed.`

- [ ] **Step 3: Push the branch**

```bash
git push -u origin eywalker/itl-460-support-numpyndarray-as-a-native-value-type
```
