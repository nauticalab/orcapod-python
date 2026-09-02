# ITL-611: set[T] Native Element Round-Trip Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend `set[T]` round-trip support so that `set[int]`, `set[str]`, `set[float]`, `set[bool]`, `set[bytes]`, `set[datetime]`, and `set[date]` all preserve set semantics through Arrow/Parquet storage (currently they fall through to plain `large_list` and read back as `list`).

**Architecture:** Extend `ListLogicalType`'s unified constructor to accept a plain Python type alongside `LogicalTypeProtocol` via duck-typing dispatch (`hasattr(element, "get_arrow_extension_type")`). A new `_NATIVE_ELEMENT_TYPES` dict maps Python type names to types for metadata round-trips. The converter's `set[T]` branch adds a post-extension-check fallback that wraps native primitive `T` in a native-mode `ListLogicalType`. The `list[T]` branch is untouched.

**Tech Stack:** PyArrow, Python typing generics, pytest, `uv run`

---

## File Map

| File | Role |
|------|------|
| `src/orcapod/logical_types/list_logical_type_factory.py` | Add `_NATIVE_ELEMENT_TYPES`, `_get_native_element_arrow_type()`, extend `ListLogicalType.__init__`, update `reconstruct_from_arrow`, update `create_for_python_type` |
| `src/orcapod/semantic_types/universal_converter.py` | Add `_make_or_get_native_list_logical_type` helper, update `_register_python_class_impl` and `_convert_python_to_arrow` for `origin is set` |
| `tests/test_logical_types/test_list_logical_type.py` | Add unit tests for native mode constructor, metadata format, storage conversions |
| `tests/test_logical_types/test_roundtrips.py` | Add converter write-path tests, schema round-trip, regression, and 7 full end-to-end round-trip tests |

---

## Task 1: Native-mode `ListLogicalType` unit tests and implementation

**Files:**
- Modify: `tests/test_logical_types/test_list_logical_type.py` (append to end of file)
- Modify: `src/orcapod/logical_types/list_logical_type_factory.py`

### Overview

The `ListLogicalType.__init__` currently only accepts a `LogicalTypeProtocol` (extension mode). We extend it to also accept a plain Python type (native mode) via duck-typing: `if hasattr(element, "get_arrow_extension_type")` → extension mode, else → native mode. We also add the `_NATIVE_ELEMENT_TYPES` dict and `_get_native_element_arrow_type` helper.

- [ ] **Step 1.1: Write the failing unit tests**

Append to the end of `tests/test_logical_types/test_list_logical_type.py`:

```python


# ── Native element mode (ITL-611) ─────────────────────────────────────────────


def test_native_list_logical_type_name_int():
    """ListLogicalType(int, is_set=True) has logical_type_name 'set[int]'."""
    from orcapod.logical_types.list_logical_type_factory import ListLogicalType
    lt = ListLogicalType(int, is_set=True)
    assert lt.logical_type_name == "set[int]"


def test_native_list_logical_type_python_type_set():
    """ListLogicalType(int, is_set=True) has python_type == set[int]."""
    from orcapod.logical_types.list_logical_type_factory import ListLogicalType
    lt = ListLogicalType(int, is_set=True)
    assert lt.python_type == set[int]


def test_native_list_logical_type_python_type_list():
    """ListLogicalType(str, is_set=False) has python_type == list[str]."""
    from orcapod.logical_types.list_logical_type_factory import ListLogicalType
    lt = ListLogicalType(str, is_set=False)
    assert lt.python_type == list[str]


def test_native_list_logical_type_arrow_extension_name():
    """ListLogicalType(int, is_set=True) arrow extension name is 'set[int]'."""
    from orcapod.logical_types.list_logical_type_factory import ListLogicalType
    lt = ListLogicalType(int, is_set=True)
    ext = lt.get_arrow_extension_type()
    assert ext.extension_name == "set[int]"


def test_native_list_logical_type_storage_type_int():
    """ListLogicalType(int, is_set=True) storage type is large_list(int64)."""
    from orcapod.logical_types.list_logical_type_factory import ListLogicalType
    lt = ListLogicalType(int, is_set=True)
    ext = lt.get_arrow_extension_type()
    assert pa.types.is_large_list(ext.storage_type)
    assert ext.storage_type.value_type == pa.int64()


def test_native_list_logical_type_storage_type_str():
    """ListLogicalType(str) storage type is large_list(large_string)."""
    from orcapod.logical_types.list_logical_type_factory import ListLogicalType
    lt = ListLogicalType(str, is_set=True)
    ext = lt.get_arrow_extension_type()
    assert pa.types.is_large_list(ext.storage_type)
    assert ext.storage_type.value_type == pa.large_string()


def test_native_list_logical_type_metadata_format():
    """Native mode metadata contains element_kind='native' and element_python_type."""
    import json
    from orcapod.logical_types.list_logical_type_factory import ListLogicalType
    lt = ListLogicalType(int, is_set=True)
    ext = lt.get_arrow_extension_type()
    meta = json.loads(ext.__arrow_ext_serialize__().decode("utf-8"))
    assert meta["category"] == "set"
    assert meta["element_kind"] == "native"
    assert meta["element_python_type"] == "int"
    assert "element_ext_name" not in meta


def test_native_list_logical_type_storage_to_python_returns_set():
    """storage_to_python returns a set for native is_set=True."""
    from orcapod.logical_types.list_logical_type_factory import ListLogicalType
    lt = ListLogicalType(int, is_set=True)
    result = lt.storage_to_python([1, 2, 3], converter=None)
    assert isinstance(result, set)
    assert result == {1, 2, 3}


def test_native_list_logical_type_python_to_storage_sorted():
    """python_to_storage for native set[int] returns a sorted list."""
    from orcapod.logical_types.list_logical_type_factory import ListLogicalType
    lt = ListLogicalType(int, is_set=True)
    result = lt.python_to_storage({3, 1, 2}, converter=None)
    assert result == [1, 2, 3]


def test_native_list_logical_type_invalid_type_raises():
    """ListLogicalType with an unsupported native type raises ValueError."""
    from orcapod.logical_types.list_logical_type_factory import ListLogicalType
    with pytest.raises(ValueError, match="not in the native element map"):
        ListLogicalType(list, is_set=True)


def test_native_list_logical_type_all_supported_types():
    """All 7 native element types (int, str, float, bool, bytes, datetime, date) construct without error."""
    from datetime import date, datetime
    from orcapod.logical_types.list_logical_type_factory import ListLogicalType
    for py_type in (int, str, float, bool, bytes, datetime, date):
        lt = ListLogicalType(py_type, is_set=True)
        assert lt.python_type == set[py_type], f"Failed for {py_type}"


def test_native_list_logical_type_cached_arrow_ext():
    """get_arrow_extension_type() is cached (same object on repeated calls)."""
    from orcapod.logical_types.list_logical_type_factory import ListLogicalType
    lt = ListLogicalType(int, is_set=True)
    assert lt.get_arrow_extension_type() is lt.get_arrow_extension_type()


def test_native_and_extension_mode_independent():
    """ListLogicalType(int) and ListLogicalType(LogicalUUID()) are distinct types."""
    from orcapod.logical_types.list_logical_type_factory import ListLogicalType
    from orcapod.logical_types.builtin_logical_types import LogicalUUID
    native_lt = ListLogicalType(int, is_set=True)
    ext_lt = ListLogicalType(LogicalUUID(), is_set=True)
    assert native_lt.logical_type_name != ext_lt.logical_type_name
    assert native_lt.python_type != ext_lt.python_type
```

- [ ] **Step 1.2: Run the tests to verify they fail**

```bash
cd /home/kurouto/kurouto-jobs/db333753-f912-4ce5-b983-111a8a484ab9/orcapod-python && uv run pytest tests/test_logical_types/test_list_logical_type.py::test_native_list_logical_type_name_int tests/test_logical_types/test_list_logical_type.py::test_native_list_logical_type_python_type_set tests/test_logical_types/test_list_logical_type.py::test_native_list_logical_type_metadata_format -v 2>&1 | head -40
```

Expected: `FAILED` with `TypeError` (wrong number of args) or similar — the constructor currently only accepts `LogicalTypeProtocol`.

- [ ] **Step 1.3: Add imports and `_NATIVE_ELEMENT_TYPES` to the factory file**

In `src/orcapod/logical_types/list_logical_type_factory.py`, replace the `from __future__ import annotations` block header section (lines 1–35) with the following. The key additions are importing `date` and `datetime` from the standard library, and adding the `_NATIVE_ELEMENT_TYPES` dict and `_get_native_element_arrow_type` helper after the existing constants.

Add after line 17 (`from __future__ import annotations`), inserting the `date`/`datetime` import:

```python
from datetime import date, datetime
```

Then after line 38 (`SET_CATEGORY = "set"`), add:

```python

_NATIVE_ELEMENT_TYPES: dict[str, type] = {
    "int": int,
    "str": str,
    "float": float,
    "bool": bool,
    "bytes": bytes,
    "datetime": datetime,
    "date": date,
}


def _get_native_element_arrow_type(python_type: type) -> "pa.DataType":
    """Return the Arrow storage type for a native Python scalar type.

    Args:
        python_type: A Python type present in ``_NATIVE_ELEMENT_TYPES``.

    Returns:
        The corresponding ``pa.DataType``.

    Raises:
        ValueError: If ``python_type`` is not in the native element map.
    """
    _native_arrow_map: dict[type, pa.DataType] = {
        int: pa.int64(),
        str: pa.large_string(),
        float: pa.float64(),
        bool: pa.bool_(),
        bytes: pa.large_binary(),
        datetime: pa.timestamp("us", tz="UTC"),
        date: pa.date32(),
    }
    arrow_type = _native_arrow_map.get(python_type)
    if arrow_type is None:
        raise ValueError(
            f"ListLogicalType: native element type {python_type!r} is not in the native "
            f"element map. Supported native types: {list(_NATIVE_ELEMENT_TYPES.keys())!r}."
        )
    return arrow_type
```

- [ ] **Step 1.4: Replace `ListLogicalType.__init__` with the unified constructor**

In `src/orcapod/logical_types/list_logical_type_factory.py`, replace the entire `__init__` method (lines 68–99) with:

```python
    def __init__(
        self,
        element: "LogicalTypeProtocol | type",
        *,
        is_set: bool = False,
    ) -> None:
        self._is_set = is_set
        self._arrow_ext: pa.ExtensionType | None = None
        self._polars_ext: pl.BaseExtension | None = None

        category = SET_CATEGORY if is_set else LIST_CATEGORY

        if hasattr(element, "get_arrow_extension_type"):
            # Extension mode: element is a LogicalTypeProtocol instance.
            self._element_logical_type = element
            self._element_python_type = element.python_type

            element_ext_type = element.get_arrow_extension_type()
            element_ext_name = element_ext_type.extension_name
            raw_meta_bytes: bytes = element_ext_type.__arrow_ext_serialize__()
            element_ext_metadata: str | None = (
                raw_meta_bytes.decode("utf-8") if raw_meta_bytes else None
            )

            meta_dict = {
                "category": category,
                "element_ext_name": element_ext_name,
                "element_ext_metadata": element_ext_metadata,
            }
            self._metadata_bytes: bytes = json.dumps(meta_dict).encode("utf-8")

            element_storage = element_ext_type.storage_type
            self._storage_type = pa.large_list(element_storage)
            self._logical_type_name = f"{category}[{element_ext_name}]"
        else:
            # Native mode: element is a plain Python type.
            if not isinstance(element, type):
                raise ValueError(
                    f"ListLogicalType: element must be a LogicalTypeProtocol or a plain Python "
                    f"type, got {element!r}."
                )
            element_type_name = element.__name__
            if element_type_name not in _NATIVE_ELEMENT_TYPES:
                raise ValueError(
                    f"ListLogicalType: native element type {element!r} "
                    f"(name={element_type_name!r}) is not in the native element map. "
                    f"Supported native types: {list(_NATIVE_ELEMENT_TYPES.keys())!r}."
                )
            self._element_logical_type = None
            self._element_python_type = element

            meta_dict = {
                "category": category,
                "element_kind": "native",
                "element_python_type": element_type_name,
            }
            self._metadata_bytes = json.dumps(meta_dict).encode("utf-8")

            self._storage_type = pa.large_list(_get_native_element_arrow_type(element))
            self._logical_type_name = f"{category}[{element_type_name}]"
```

Also update the class docstring to reflect that `element` can be either a `LogicalTypeProtocol` or a plain Python type. Replace the existing `Args:` block in the class docstring:

```python
    Args:
        element: Either a ``LogicalTypeProtocol`` instance (e.g. ``LogicalUUID()``) for
            extension mode, or a plain Python type (e.g. ``int``) for native mode.
            In native mode, the Python type must be in ``_NATIVE_ELEMENT_TYPES``.
        is_set: If ``True``, uses ``set[T]`` semantics. Defaults to ``False``.
```

- [ ] **Step 1.5: Run the unit tests and verify they pass**

```bash
cd /home/kurouto/kurouto-jobs/db333753-f912-4ce5-b983-111a8a484ab9/orcapod-python && uv run pytest tests/test_logical_types/test_list_logical_type.py -v 2>&1 | tail -30
```

Expected: All tests pass, including the 13 new native-mode tests. The existing extension-mode tests (lines 80–289 of the original file) must still pass — they use `_logical_uuid()` which has `get_arrow_extension_type`, so they route to extension mode.

- [ ] **Step 1.6: Commit**

```bash
cd /home/kurouto/kurouto-jobs/db333753-f912-4ce5-b983-111a8a484ab9/orcapod-python && git add tests/test_logical_types/test_list_logical_type.py src/orcapod/logical_types/list_logical_type_factory.py && git commit -m "$(cat <<'EOF'
feat(logical_types): extend ListLogicalType to support native Python element types

Add _NATIVE_ELEMENT_TYPES, _get_native_element_arrow_type, and unified
__init__ that detects extension vs native mode via duck-typing. Metadata
format: {category, element_kind: native, element_python_type: "int"}.
EOF
)"
```

---

## Task 2: Factory read-path: `reconstruct_from_arrow` for native mode

**Files:**
- Modify: `tests/test_logical_types/test_list_logical_type.py` (append)
- Modify: `src/orcapod/logical_types/list_logical_type_factory.py`

### Overview

`ListLogicalTypeFactory.reconstruct_from_arrow` currently assumes `element_ext_name` is always present. We add a branch that checks for `element_kind == "native"` in the metadata and constructs a native-mode `ListLogicalType` directly, bypassing the extension-type reconstruction path.

- [ ] **Step 2.1: Write the failing factory read-path tests**

Append to `tests/test_logical_types/test_list_logical_type.py`:

```python


# ── Native mode reconstruct_from_arrow (ITL-611) ─────────────────────────────


def test_list_logical_type_factory_reconstruct_native_set_of_int():
    """reconstruct_from_arrow with element_kind=native rebuilds set[int]."""
    from orcapod.logical_types.list_logical_type_factory import (
        ListLogicalTypeFactory, SET_CATEGORY,
    )
    from orcapod.contexts import create_registry
    factory = ListLogicalTypeFactory()
    storage_type = pa.large_list(pa.int64())
    metadata = {
        "category": SET_CATEGORY,
        "element_kind": "native",
        "element_python_type": "int",
    }
    converter = create_registry().get_context().type_converter
    lt = factory.reconstruct_from_arrow("set[int]", storage_type, metadata, converter)
    assert lt.logical_type_name == "set[int]"
    assert lt.python_type == set[int]


def test_list_logical_type_factory_reconstruct_native_list_of_str():
    """reconstruct_from_arrow with element_kind=native rebuilds list[str]."""
    from orcapod.logical_types.list_logical_type_factory import (
        ListLogicalTypeFactory, LIST_CATEGORY,
    )
    from orcapod.contexts import create_registry
    factory = ListLogicalTypeFactory()
    storage_type = pa.large_list(pa.large_string())
    metadata = {
        "category": LIST_CATEGORY,
        "element_kind": "native",
        "element_python_type": "str",
    }
    converter = create_registry().get_context().type_converter
    lt = factory.reconstruct_from_arrow("list[str]", storage_type, metadata, converter)
    assert lt.logical_type_name == "list[str]"
    assert lt.python_type == list[str]


def test_list_logical_type_factory_reconstruct_native_unknown_type_raises():
    """reconstruct_from_arrow raises ValueError for unknown native element type."""
    from orcapod.logical_types.list_logical_type_factory import (
        ListLogicalTypeFactory, SET_CATEGORY,
    )
    from orcapod.contexts import create_registry
    factory = ListLogicalTypeFactory()
    storage_type = pa.large_list(pa.null())
    metadata = {
        "category": SET_CATEGORY,
        "element_kind": "native",
        "element_python_type": "frozenset",
    }
    converter = create_registry().get_context().type_converter
    with pytest.raises(ValueError, match="unknown native element type"):
        factory.reconstruct_from_arrow("set[frozenset]", storage_type, metadata, converter)


def test_list_logical_type_factory_reconstruct_native_missing_python_type_raises():
    """reconstruct_from_arrow raises ValueError if element_python_type is absent."""
    from orcapod.logical_types.list_logical_type_factory import (
        ListLogicalTypeFactory, SET_CATEGORY,
    )
    from orcapod.contexts import create_registry
    factory = ListLogicalTypeFactory()
    storage_type = pa.large_list(pa.int64())
    metadata = {"category": SET_CATEGORY, "element_kind": "native"}
    converter = create_registry().get_context().type_converter
    with pytest.raises(ValueError, match="element_python_type"):
        factory.reconstruct_from_arrow("set[int]", storage_type, metadata, converter)
```

- [ ] **Step 2.2: Run the tests to verify they fail**

```bash
cd /home/kurouto/kurouto-jobs/db333753-f912-4ce5-b983-111a8a484ab9/orcapod-python && uv run pytest tests/test_logical_types/test_list_logical_type.py::test_list_logical_type_factory_reconstruct_native_set_of_int tests/test_logical_types/test_list_logical_type.py::test_list_logical_type_factory_reconstruct_native_list_of_str -v 2>&1 | head -30
```

Expected: `FAILED` — `reconstruct_from_arrow` currently raises `ValueError: missing 'element_ext_name'` when `element_ext_name` is absent from native metadata.

- [ ] **Step 2.3: Update `reconstruct_from_arrow` to handle native mode**

In `src/orcapod/logical_types/list_logical_type_factory.py`, replace the body of `reconstruct_from_arrow` (the content after the storage type check, up to and including the final return). The new implementation adds a native-mode branch **before** the existing extension-mode logic:

```python
    def reconstruct_from_arrow(
        self,
        arrow_extension_name: str,
        storage_type: "pa.DataType",
        metadata: dict,
        converter: "TypeConverterProtocol",
    ) -> ListLogicalType:
        """Reconstruct a ``ListLogicalType`` from Arrow schema metadata (read path).

        Handles two metadata formats:

        - **Extension mode** (legacy): ``{"category": ..., "element_ext_name": ..., ...}``
        - **Native mode** (ITL-611): ``{"category": ..., "element_kind": "native", "element_python_type": "int"}``

        Args:
            arrow_extension_name: Extension name (e.g. ``"set[int]"`` or ``"list[orcapod.uuid]"``).
            storage_type: Outer storage type (``large_list(<element storage>)``).
            metadata: Parsed metadata dict; must contain ``"category"`` and either
                ``"element_ext_name"`` (extension mode) or ``"element_python_type"``
                with ``"element_kind": "native"`` (native mode).
            converter: Active converter for recursive element registration (extension mode only).

        Returns:
            A ``ListLogicalType`` ready for registration.

        Raises:
            ValueError: If ``storage_type`` is not a list type, or required metadata
                keys are missing.
        """
        if not (pa.types.is_large_list(storage_type) or pa.types.is_list(storage_type)):
            raise ValueError(
                f"ListLogicalTypeFactory.reconstruct_from_arrow: expected a list storage "
                f"type for {arrow_extension_name!r}, got {storage_type!r}."
            )

        is_set = metadata.get("category") == SET_CATEGORY
        element_kind = metadata.get("element_kind")

        if element_kind == "native":
            # Native mode: element is a plain Python type from _NATIVE_ELEMENT_TYPES.
            element_python_type_name = metadata.get("element_python_type")
            if not element_python_type_name:
                raise ValueError(
                    f"ListLogicalTypeFactory.reconstruct_from_arrow: missing "
                    f"'element_python_type' in native-mode metadata for "
                    f"{arrow_extension_name!r}. metadata={metadata!r}."
                )
            element_python_type = _NATIVE_ELEMENT_TYPES.get(element_python_type_name)
            if element_python_type is None:
                raise ValueError(
                    f"ListLogicalTypeFactory.reconstruct_from_arrow: unknown native element "
                    f"type {element_python_type_name!r} for {arrow_extension_name!r}. "
                    f"Supported: {list(_NATIVE_ELEMENT_TYPES.keys())!r}."
                )
            logger.debug(
                "ListLogicalTypeFactory: reconstructed %r from Arrow as native mode (is_set=%s)",
                arrow_extension_name,
                is_set,
            )
            return ListLogicalType(element_python_type, is_set=is_set)

        # Extension mode (existing logic — unchanged).
        element_ext_name = metadata.get("element_ext_name")
        if not element_ext_name:
            raise ValueError(
                f"ListLogicalTypeFactory.reconstruct_from_arrow: missing 'element_ext_name' "
                f"in metadata for {arrow_extension_name!r}. metadata={metadata!r}."
            )

        element_meta_str = metadata.get("element_ext_metadata")
        element_meta_bytes = (
            element_meta_str.encode("utf-8") if element_meta_str else b""
        )
        # Element storage is the value type of the outer list storage.
        element_storage_type = storage_type.value_type

        # Recursively register the element logical type (handles nesting).
        converter.register_logical_type_from_arrow_metadata(
            element_ext_name, element_meta_bytes, element_storage_type
        )

        # Retrieve the now-registered element logical type.
        element_logical_type = converter.get_logical_type_by_arrow_extension_name(element_ext_name)
        if element_logical_type is None:
            raise ValueError(
                f"ListLogicalTypeFactory.reconstruct_from_arrow: element extension "
                f"{element_ext_name!r} was registered but no logical type found. "
                f"This is a bug in register_logical_type_from_arrow_metadata."
            )

        logger.debug(
            "ListLogicalTypeFactory: reconstructed %r from Arrow (is_set=%s)",
            arrow_extension_name,
            is_set,
        )
        return ListLogicalType(element_logical_type, is_set=is_set)
```

- [ ] **Step 2.4: Run the factory tests and verify they pass**

```bash
cd /home/kurouto/kurouto-jobs/db333753-f912-4ce5-b983-111a8a484ab9/orcapod-python && uv run pytest tests/test_logical_types/test_list_logical_type.py -v 2>&1 | tail -30
```

Expected: All tests pass, including the 4 new reconstruct tests. All original tests must also still pass.

- [ ] **Step 2.5: Commit**

```bash
cd /home/kurouto/kurouto-jobs/db333753-f912-4ce5-b983-111a8a484ab9/orcapod-python && git add tests/test_logical_types/test_list_logical_type.py src/orcapod/logical_types/list_logical_type_factory.py && git commit -m "$(cat <<'EOF'
feat(logical_types): add native mode branch to ListLogicalTypeFactory.reconstruct_from_arrow

Handles {"element_kind": "native", "element_python_type": "int"} metadata
format. Backward-compatible: absence of element_kind falls through to
existing extension-mode logic.
EOF
)"
```

---

## Task 3: Converter write path and `create_for_python_type`

**Files:**
- Modify: `tests/test_logical_types/test_roundtrips.py` (append)
- Modify: `src/orcapod/semantic_types/universal_converter.py`
- Modify: `src/orcapod/logical_types/list_logical_type_factory.py` (`create_for_python_type`)

### Overview

Add `_make_or_get_native_list_logical_type` to `UniversalTypeConverter`, then update both `_register_python_class_impl` and `_convert_python_to_arrow` so that when `origin is set` and the element is a native type, a `ListLogicalType` is created and registered. Also update `create_for_python_type` so explicit calls like `factory.create_for_python_type(set[int], converter)` work.

- [ ] **Step 3.1: Write the failing converter write-path tests**

Append to `tests/test_logical_types/test_roundtrips.py`:

```python

# ── set[T] native element write-path unit tests (ITL-611) ─────────────────────


def test_converter_set_of_int_produces_extension_type() -> None:
    """converter.python_type_to_arrow_type(set[int]) returns Arrow extension type 'set[int]'."""
    from orcapod.contexts import create_registry
    converter = create_registry().get_context().type_converter
    arrow_type = converter.python_type_to_arrow_type(set[int])
    assert isinstance(arrow_type, pa.ExtensionType), (
        f"Expected pa.ExtensionType for set[int], got {arrow_type!r}"
    )
    assert arrow_type.extension_name == "set[int]"


def test_converter_set_of_str_produces_extension_type() -> None:
    """converter.python_type_to_arrow_type(set[str]) returns Arrow extension type 'set[str]'."""
    from orcapod.contexts import create_registry
    converter = create_registry().get_context().type_converter
    arrow_type = converter.python_type_to_arrow_type(set[str])
    assert isinstance(arrow_type, pa.ExtensionType)
    assert arrow_type.extension_name == "set[str]"


def test_converter_list_of_int_unchanged_regression() -> None:
    """list[int] still produces plain large_list(int64) — no ListLogicalType wrapping (regression)."""
    from orcapod.contexts import create_registry
    converter = create_registry().get_context().type_converter
    result = converter.python_type_to_arrow_type(list[int])
    assert not isinstance(result, pa.ExtensionType), (
        f"list[int] must NOT be wrapped as extension type, got {result!r}"
    )
    assert pa.types.is_large_list(result)
    assert result.value_type == pa.int64()


def test_schema_round_trip_set_of_int() -> None:
    """arrow_schema_to_python_schema reconstructs set[int] (not list[int] or set[Any])."""
    from orcapod.contexts import create_registry
    converter = create_registry().get_context().type_converter
    python_schema = {"s": set[int]}
    arrow_schema = converter.python_schema_to_arrow_schema(python_schema)
    recovered = converter.arrow_schema_to_python_schema(arrow_schema)
    assert recovered["s"] == set[int], (
        f"Expected set[int], got {recovered['s']!r}"
    )


def test_schema_round_trip_set_of_str() -> None:
    """arrow_schema_to_python_schema reconstructs set[str]."""
    from orcapod.contexts import create_registry
    converter = create_registry().get_context().type_converter
    python_schema = {"tags": set[str]}
    arrow_schema = converter.python_schema_to_arrow_schema(python_schema)
    recovered = converter.arrow_schema_to_python_schema(arrow_schema)
    assert recovered["tags"] == set[str]


def test_explicit_native_list_construction() -> None:
    """ListLogicalType(int, is_set=False) builds a functional list[int] extension type."""
    from orcapod.logical_types.list_logical_type_factory import ListLogicalType
    lt = ListLogicalType(int, is_set=False)
    assert lt.logical_type_name == "list[int]"
    assert lt.python_type == list[int]
    storage = lt.python_to_storage([1, 2, 3], converter=None)
    assert storage == [1, 2, 3]
    result = lt.storage_to_python([1, 2, 3], converter=None)
    assert result == [1, 2, 3]
```

- [ ] **Step 3.2: Run the tests to verify they fail**

```bash
cd /home/kurouto/kurouto-jobs/db333753-f912-4ce5-b983-111a8a484ab9/orcapod-python && uv run pytest tests/test_logical_types/test_roundtrips.py::test_converter_set_of_int_produces_extension_type tests/test_logical_types/test_roundtrips.py::test_converter_list_of_int_unchanged_regression tests/test_logical_types/test_roundtrips.py::test_schema_round_trip_set_of_int -v 2>&1 | head -40
```

Expected: `test_converter_set_of_int_produces_extension_type` FAILS (set[int] gives plain large_list, not extension); `test_converter_list_of_int_unchanged_regression` PASSES (existing behaviour); `test_schema_round_trip_set_of_int` FAILS.

- [ ] **Step 3.3: Add `_make_or_get_native_list_logical_type` to `UniversalTypeConverter`**

In `src/orcapod/semantic_types/universal_converter.py`, add the following method immediately after the existing `_make_or_get_list_logical_type` method (after line 472):

```python
    def _make_or_get_native_list_logical_type(
        self,
        python_type: type,
        is_set: bool,
    ) -> "pa.ExtensionType":
        """Return (creating and registering if needed) a native-mode ``ListLogicalType``.

        Mirrors ``_make_or_get_list_logical_type`` for plain Python types (e.g. ``int``,
        ``str``) that have no ``LogicalTypeProtocol``. The resulting extension type
        preserves set/list semantics through Parquet round-trips.

        Args:
            python_type: A plain Python type present in ``_NATIVE_ELEMENT_TYPES``
                (e.g. ``int``, ``str``, ``datetime``).
            is_set: ``True`` for ``set[T]``, ``False`` for ``list[T]``.

        Returns:
            The ``pa.ExtensionType`` of the created-or-existing native ``ListLogicalType``.
        """
        from orcapod.logical_types.list_logical_type_factory import ListLogicalType

        prefix = "set" if is_set else "list"
        list_ext_name = f"{prefix}[{python_type.__name__}]"

        # Idempotency: look up by extension name first.
        lt = self._logical_type_registry.get_by_arrow_extension_name(list_ext_name)
        if lt is None:
            lt = ListLogicalType(python_type, is_set=is_set)
            self._logical_type_registry.register_logical_type(lt)
        return lt.get_arrow_extension_type()
```

- [ ] **Step 3.4: Update `_register_python_class_impl` — `origin is set` branch**

In `src/orcapod/semantic_types/universal_converter.py`, find the `# set[T] → pa.large_list(T).` comment block (around line 361). The current code is:

```python
        # set[T] → pa.large_list(T). Same restriction as list[T] unless T has a LogicalType.
        if origin is set:
            if not args:
                raise ValueError(
                    "Unparameterized 'set' is not supported. Use 'set[T]' with a concrete "
                    "element type (e.g. set[int], set[str])."
                )
            inner = self.register_python_class(args[0])
            if self._logical_type_registry is not None and hasattr(inner, "extension_name"):
                element_lt = self._logical_type_registry.get_by_arrow_extension_name(inner.extension_name)
                if element_lt is not None:
                    return self._make_or_get_list_logical_type(element_lt, is_set=True)
            return pa.large_list(inner)
```

Replace it with:

```python
        # set[T] → pa.large_list(T). Same restriction as list[T] unless T has a LogicalType.
        if origin is set:
            if not args:
                raise ValueError(
                    "Unparameterized 'set' is not supported. Use 'set[T]' with a concrete "
                    "element type (e.g. set[int], set[str])."
                )
            inner = self.register_python_class(args[0])
            if self._logical_type_registry is not None and hasattr(inner, "extension_name"):
                element_lt = self._logical_type_registry.get_by_arrow_extension_name(inner.extension_name)
                if element_lt is not None:
                    return self._make_or_get_list_logical_type(element_lt, is_set=True)
            # NEW (ITL-611): wrap primitive T in native-mode ListLogicalType to preserve set semantics.
            if self._logical_type_registry is not None:
                from orcapod.logical_types.list_logical_type_factory import _NATIVE_ELEMENT_TYPES
                if isinstance(args[0], type) and args[0].__name__ in _NATIVE_ELEMENT_TYPES:
                    return self._make_or_get_native_list_logical_type(args[0], is_set=True)
            return pa.large_list(inner)
```

- [ ] **Step 3.5: Update `_convert_python_to_arrow` — `origin is set` branch**

In `src/orcapod/semantic_types/universal_converter.py`, find the `# Handle set types → lists` comment (around line 1263). The current code is:

```python
        # Handle set types → lists
        elif origin is set:
            if len(args) != 1:
                raise ValueError(
                    f"set type must have exactly one type argument, got: {args}"
                )
            element_type = self.python_type_to_arrow_type(args[0])
            if self._logical_type_registry is not None and hasattr(element_type, "extension_name"):
                element_lt = self._logical_type_registry.get_by_arrow_extension_name(element_type.extension_name)
                if element_lt is not None:
                    return self._make_or_get_list_logical_type(element_lt, is_set=True)
            return pa.large_list(element_type)
```

Replace it with:

```python
        # Handle set types → lists
        elif origin is set:
            if len(args) != 1:
                raise ValueError(
                    f"set type must have exactly one type argument, got: {args}"
                )
            element_type = self.python_type_to_arrow_type(args[0])
            if self._logical_type_registry is not None and hasattr(element_type, "extension_name"):
                element_lt = self._logical_type_registry.get_by_arrow_extension_name(element_type.extension_name)
                if element_lt is not None:
                    return self._make_or_get_list_logical_type(element_lt, is_set=True)
            # NEW (ITL-611): wrap primitive T in native-mode ListLogicalType to preserve set semantics.
            if self._logical_type_registry is not None:
                from orcapod.logical_types.list_logical_type_factory import _NATIVE_ELEMENT_TYPES
                if isinstance(args[0], type) and args[0].__name__ in _NATIVE_ELEMENT_TYPES:
                    return self._make_or_get_native_list_logical_type(args[0], is_set=True)
            return pa.large_list(element_type)
```

- [ ] **Step 3.6: Update `ListLogicalTypeFactory.create_for_python_type` for native `set[T]`**

In `src/orcapod/logical_types/list_logical_type_factory.py`, find the `create_for_python_type` method. The current end of the method is:

```python
        element_annotation = args[0]
        is_set = origin is set

        # Directly look up (and register if needed) the LogicalType for the element.
        element_lt = converter.get_logical_type_for_python_type(element_annotation)
        if element_lt is None:
            raise ValueError(
                f"ListLogicalTypeFactory.create_for_python_type: element type "
                f"{element_annotation!r} has no registered LogicalType. "
                f"Only list[T]/set[T] where T maps to a LogicalType are supported; "
                f"use plain list[{element_annotation}] for primitive element types."
            )

        return ListLogicalType(element_lt, is_set=is_set)
```

Replace with:

```python
        element_annotation = args[0]
        is_set = origin is set

        # Directly look up (and register if needed) the LogicalType for the element.
        element_lt = converter.get_logical_type_for_python_type(element_annotation)
        if element_lt is None:
            # NEW (ITL-611): for set[T] where T is a native primitive, use native mode.
            if (
                is_set
                and isinstance(element_annotation, type)
                and element_annotation.__name__ in _NATIVE_ELEMENT_TYPES
            ):
                return ListLogicalType(element_annotation, is_set=True)
            raise ValueError(
                f"ListLogicalTypeFactory.create_for_python_type: element type "
                f"{element_annotation!r} has no registered LogicalType. "
                f"Only list[T]/set[T] where T maps to a LogicalType are supported; "
                f"use plain list[{element_annotation}] for primitive element types."
            )

        return ListLogicalType(element_lt, is_set=is_set)
```

- [ ] **Step 3.7: Run the write-path tests and verify they pass**

```bash
cd /home/kurouto/kurouto-jobs/db333753-f912-4ce5-b983-111a8a484ab9/orcapod-python && uv run pytest tests/test_logical_types/test_roundtrips.py::test_converter_set_of_int_produces_extension_type tests/test_logical_types/test_roundtrips.py::test_converter_set_of_str_produces_extension_type tests/test_logical_types/test_roundtrips.py::test_converter_list_of_int_unchanged_regression tests/test_logical_types/test_roundtrips.py::test_schema_round_trip_set_of_int tests/test_logical_types/test_roundtrips.py::test_schema_round_trip_set_of_str tests/test_logical_types/test_roundtrips.py::test_explicit_native_list_construction -v 2>&1 | tail -20
```

Expected: All 6 tests PASS.

- [ ] **Step 3.8: Run the full test suite for the affected files to check for regressions**

```bash
cd /home/kurouto/kurouto-jobs/db333753-f912-4ce5-b983-111a8a484ab9/orcapod-python && uv run pytest tests/test_logical_types/ -v 2>&1 | tail -40
```

Expected: All tests pass. Pay special attention to the existing `test_list_of_int_produces_no_extension_type` test — it must still pass.

- [ ] **Step 3.9: Commit**

```bash
cd /home/kurouto/kurouto-jobs/db333753-f912-4ce5-b983-111a8a484ab9/orcapod-python && git add src/orcapod/semantic_types/universal_converter.py src/orcapod/logical_types/list_logical_type_factory.py tests/test_logical_types/test_roundtrips.py && git commit -m "$(cat <<'EOF'
feat(converter): wrap set[T] with native ListLogicalType to preserve set semantics

Add _make_or_get_native_list_logical_type helper; update both set[T] branches
in _register_python_class_impl and _convert_python_to_arrow; update
create_for_python_type. list[T] for primitive T remains unchanged.
EOF
)"
```

---

## Task 4: Full end-to-end round-trip tests

**Files:**
- Modify: `tests/test_logical_types/test_roundtrips.py` (append)

### Overview

Verify that `set[T]` for each of the 7 native types survives a complete write → storage → read round-trip through both Parquet and Delta backends. Also verify the "fresh converter" scenario (write with converter A, read with converter B that has no prior registration).

- [ ] **Step 4.1: Write the round-trip tests**

Append to `tests/test_logical_types/test_roundtrips.py`:

```python

# ── set[T] native element full round-trip tests (ITL-611) ─────────────────────


def test_set_of_int_round_trip(storage_backend: _StorageBackend, tmp_path: Path) -> None:
    """set[int] values round-trip as sets, not lists; extension name is 'set[int]'."""
    data = {1, 2, 3}
    result, read_converter = _write_and_read(
        {"s": set[int]},
        [{"s": data}],
        storage_backend,
        tmp_path,
    )
    field = result.schema.field("s")
    assert hasattr(field.type, "extension_name"), (
        f"Expected extension type on 's', got {field.type!r}"
    )
    assert field.type.extension_name == "set[int]"
    rows = read_converter.arrow_table_to_python_dicts(result)
    assert len(rows) == 1
    assert isinstance(rows[0]["s"], set), f"Expected set, got {type(rows[0]['s'])}"
    assert rows[0]["s"] == data


def test_set_of_str_round_trip(storage_backend: _StorageBackend, tmp_path: Path) -> None:
    """set[str] values round-trip as sets; extension name is 'set[str]'."""
    data = {"alpha", "beta", "gamma"}
    result, read_converter = _write_and_read(
        {"tags": set[str]},
        [{"tags": data}],
        storage_backend,
        tmp_path,
    )
    field = result.schema.field("tags")
    assert hasattr(field.type, "extension_name")
    assert field.type.extension_name == "set[str]"
    rows = read_converter.arrow_table_to_python_dicts(result)
    assert isinstance(rows[0]["tags"], set)
    assert rows[0]["tags"] == data


def test_set_of_float_round_trip(storage_backend: _StorageBackend, tmp_path: Path) -> None:
    """set[float] values round-trip as sets; extension name is 'set[float]'."""
    data = {1.0, 2.5, 3.14}
    result, read_converter = _write_and_read(
        {"values": set[float]},
        [{"values": data}],
        storage_backend,
        tmp_path,
    )
    field = result.schema.field("values")
    assert hasattr(field.type, "extension_name")
    assert field.type.extension_name == "set[float]"
    rows = read_converter.arrow_table_to_python_dicts(result)
    assert isinstance(rows[0]["values"], set)
    assert rows[0]["values"] == data


def test_set_of_bool_round_trip(storage_backend: _StorageBackend, tmp_path: Path) -> None:
    """set[bool] values round-trip as sets; extension name is 'set[bool]'."""
    data = {True, False}
    result, read_converter = _write_and_read(
        {"flags": set[bool]},
        [{"flags": data}],
        storage_backend,
        tmp_path,
    )
    field = result.schema.field("flags")
    assert hasattr(field.type, "extension_name")
    assert field.type.extension_name == "set[bool]"
    rows = read_converter.arrow_table_to_python_dicts(result)
    assert isinstance(rows[0]["flags"], set)
    assert rows[0]["flags"] == data


def test_set_of_bytes_round_trip(storage_backend: _StorageBackend, tmp_path: Path) -> None:
    """set[bytes] values round-trip as sets; extension name is 'set[bytes]'."""
    data = {b"foo", b"bar", b"baz"}
    result, read_converter = _write_and_read(
        {"blobs": set[bytes]},
        [{"blobs": data}],
        storage_backend,
        tmp_path,
    )
    field = result.schema.field("blobs")
    assert hasattr(field.type, "extension_name")
    assert field.type.extension_name == "set[bytes]"
    rows = read_converter.arrow_table_to_python_dicts(result)
    assert isinstance(rows[0]["blobs"], set)
    assert rows[0]["blobs"] == data


def test_set_of_datetime_round_trip(storage_backend: _StorageBackend, tmp_path: Path) -> None:
    """set[datetime] values round-trip as sets of timezone-aware datetimes."""
    from datetime import datetime, timezone
    dt1 = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    dt2 = datetime(2024, 6, 15, 12, 30, 0, tzinfo=timezone.utc)
    data = {dt1, dt2}
    result, read_converter = _write_and_read(
        {"timestamps": set[datetime]},
        [{"timestamps": data}],
        storage_backend,
        tmp_path,
    )
    field = result.schema.field("timestamps")
    assert hasattr(field.type, "extension_name")
    assert field.type.extension_name == "set[datetime]"
    rows = read_converter.arrow_table_to_python_dicts(result)
    assert isinstance(rows[0]["timestamps"], set)
    assert rows[0]["timestamps"] == data


def test_fresh_converter_reads_set_of_int(
    storage_backend: _StorageBackend, tmp_path: Path
) -> None:
    """A fresh converter (no prior registration) reconstructs set[int] via load_logical_types."""
    data = {1, 2, 3}

    # Write with converter A.
    write_converter = _fresh_converter()
    write_converter.register_python_class(set[int])
    arrow_schema = write_converter.python_schema_to_arrow_schema({"s": set[int]})
    table = write_converter.python_dicts_to_arrow_table([{"s": data}], arrow_schema=arrow_schema)
    storage_backend.write(table, tmp_path)

    # Read with converter B — no prior registration; load_logical_types triggers factory.
    read_converter = _fresh_converter()
    result = storage_backend.read(tmp_path, read_converter)

    field = result.schema.field("s")
    assert hasattr(field.type, "extension_name"), (
        f"Expected extension type after fresh-converter read, got {field.type!r}"
    )
    assert field.type.extension_name == "set[int]"

    rows = read_converter.arrow_table_to_python_dicts(result)
    assert isinstance(rows[0]["s"], set)
    assert rows[0]["s"] == data
```

- [ ] **Step 4.2: Run the new round-trip tests**

```bash
cd /home/kurouto/kurouto-jobs/db333753-f912-4ce5-b983-111a8a484ab9/orcapod-python && uv run pytest tests/test_logical_types/test_roundtrips.py -k "set_of_int or set_of_str or set_of_float or set_of_bool or set_of_bytes or set_of_datetime or fresh_converter_reads_set" -v 2>&1 | tail -40
```

Expected: All 15 tests (7 types × 2 backends + 1 fresh-converter × 2 backends = 14, plus the non-parametrised fresh-converter is parametrised too = 14) PASS. If any fail, diagnose before proceeding.

- [ ] **Step 4.3: Run the full test suite**

```bash
cd /home/kurouto/kurouto-jobs/db333753-f912-4ce5-b983-111a8a484ab9/orcapod-python && uv run pytest tests/test_logical_types/ -v 2>&1 | tail -50
```

Expected: All tests pass with no regressions.

- [ ] **Step 4.4: Run a broader test sweep to catch converter regressions**

```bash
cd /home/kurouto/kurouto-jobs/db333753-f912-4ce5-b983-111a8a484ab9/orcapod-python && uv run pytest tests/ -x --timeout=120 2>&1 | tail -30
```

Expected: All tests pass. Fix any failures before committing.

- [ ] **Step 4.5: Commit**

```bash
cd /home/kurouto/kurouto-jobs/db333753-f912-4ce5-b983-111a8a484ab9/orcapod-python && git add tests/test_logical_types/test_roundtrips.py && git commit -m "$(cat <<'EOF'
test(logical_types): add set[T] native element round-trip tests for ITL-611

Covers set[int|str|float|bool|bytes|datetime] through Parquet and Delta
backends, schema reconstruction, list[int] regression, and fresh-converter
read-back without prior registration.
EOF
)"
```

---

## Self-Review

### Spec coverage

| Spec requirement | Task covering it |
|-----------------|-----------------|
| `ListLogicalType` unified constructor (`LogicalTypeProtocol \| type`) | Task 1 |
| `_NATIVE_ELEMENT_TYPES` dict (7 types) | Task 1 |
| `_get_native_element_arrow_type` helper | Task 1 |
| Native metadata format `{category, element_kind, element_python_type}` | Task 1 |
| `reconstruct_from_arrow` native branch | Task 2 |
| Backward compatibility: old metadata without `element_kind` → extension mode | Task 2 (existing branch unchanged) |
| `_make_or_get_native_list_logical_type` helper in converter | Task 3 |
| `_register_python_class_impl` `set[T]` → native wrapping | Task 3 |
| `_convert_python_to_arrow` `set[T]` → native wrapping | Task 3 |
| `list[T]` for primitive T: **unchanged** | Task 3 regression test |
| `create_for_python_type` native `set[T]` support | Task 3 |
| `set[int]` through `set[datetime]` round-trips (Parquet + Delta) | Task 4 |
| Extension name assertion (`field.type.extension_name == "set[int]"`) | Task 4 |
| Schema round-trip `set[int]` | Task 3 |
| Fresh converter read-back | Task 4 |
| Explicit `ListLogicalType(int, is_set=False)` direct construction | Task 3 |

All spec requirements are covered. No gaps found.

### Placeholder scan

No TBD, TODO, "similar to above", or "implement later" strings in this plan.

### Type consistency

- `_NATIVE_ELEMENT_TYPES` is defined in Task 1 (`list_logical_type_factory.py`) and imported (lazily, inside function body) in Task 3 (`universal_converter.py`) — consistent.
- `_make_or_get_native_list_logical_type(python_type: type, is_set: bool)` defined in Task 3, called in Tasks 3 steps 3.4 and 3.5 — consistent.
- `ListLogicalType(element, *, is_set)` where `element: LogicalTypeProtocol | type` — used with `int` (Task 1 tests) and `LogicalUUID()` (existing tests, unchanged) — consistent.
- `_get_native_element_arrow_type(python_type: type)` used only inside `ListLogicalType.__init__` native branch — consistent.
