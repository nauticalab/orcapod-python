# PLT-1720: register_python_class storage-type cleanup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `register_python_class` and `register_storage_type` both return storage-safe Arrow types (extension type allowed at top level, no extension types nested inside struct/list fields), delete `_strip_ext_to_storage`, and fix `reconstruct_from_arrow` to register nested types so Parquet round-trips for nested dataclasses work in a fresh process.

**Architecture:** Three coordinated changes: (1) `_register_python_class_impl` container branches strip any extension type returned by recursive calls before embedding in list/dict; (2) `register_storage_type` strips extension types from struct/list fields when rebuilding the type; (3) `DataclassLogicalTypeFactory.create_for_python_type` replaces the recursive `_strip_ext_to_storage` call with a one-liner, and `reconstruct_from_arrow` adds `converter.register_python_class(annotation)` per field to trigger nested registration.

**Tech Stack:** Python 3.12+, PyArrow ≥ 20, `uv run pytest`

---

## File map

| File | Change |
|---|---|
| `src/orcapod/extension_types/protocols.py` | Docstring updates only |
| `src/orcapod/semantic_types/universal_converter.py` | `_register_python_class_impl` container branches; `register_storage_type` struct/list stripping |
| `src/orcapod/extension_types/dataclass_logical_type_factory.py` | Delete `_strip_ext_to_storage`; update `create_for_python_type`; update `reconstruct_from_arrow` |
| `DESIGN_ISSUES.md` | Mark ET1 in progress / update workaround note |
| `tests/test_semantic_types/test_universal_converter.py` | Fix `test_register_storage_type_nested_struct_with_extension` |
| `tests/test_extension_types/test_dataclass_logical_type_factory.py` | New tests: `test_reconstruct_from_arrow_registers_nested_types`, `test_nested_dataclass_parquet_roundtrip` |

---

## Task 1: Update docstrings in protocols.py

**Files:**
- Modify: `src/orcapod/extension_types/protocols.py:27-33`

- [ ] **Step 1: Update `register_python_class` docstring**

Replace lines 27–29:
```python
    def register_python_class(self, annotation: Any) -> "pa.DataType":
        """Traverse a Python annotation and return its Arrow type, registering as needed."""
        ...
```
With:
```python
    def register_python_class(self, annotation: Any) -> "pa.DataType":
        """Traverse a Python annotation, register any logical types found, and return
        the storage-safe Arrow type.

        The returned type may be a ``pa.ExtensionType`` at the top level for registered
        classes (e.g. ``UUID`` → ``orcapod.uuid`` extension type), but struct fields and
        list value types at any depth are always plain (non-extension) Arrow types.

        Args:
            annotation: A Python type or generic alias (e.g. ``list[str]``,
                ``Optional[uuid.UUID]``, a dataclass type).

        Returns:
            A storage-safe ``pa.DataType``. May be ``pa.ExtensionType`` at the top level;
            never contains nested extension types in struct/list fields.
        """
        ...
```

- [ ] **Step 2: Update `register_storage_type` docstring**

Replace lines 31–33:
```python
    def register_storage_type(self, arrow_type: "pa.DataType") -> "pa.DataType":
        """Traverse an Arrow type bottom-up, registering extension types, and return resolved type."""
        ...
```
With:
```python
    def register_storage_type(self, arrow_type: "pa.DataType") -> "pa.DataType":
        """Traverse an Arrow type bottom-up, registering extension types, and return a
        storage-safe type.

        The returned type may be a ``pa.ExtensionType`` at the top level, but struct fields
        and list value types at any depth are always plain (non-extension) Arrow types.
        This invariant makes the return value safe to use as a struct field or list element
        type without further stripping.

        Args:
            arrow_type: An Arrow type to traverse and register.

        Returns:
            A storage-safe ``pa.DataType``.
        """
        ...
```

- [ ] **Step 3: Run existing protocol tests to confirm no breakage**

```bash
uv run pytest tests/test_extension_types/test_protocols.py -v
```
Expected: all PASS

- [ ] **Step 4: Commit**

```bash
git add src/orcapod/extension_types/protocols.py
git commit -m "docs(extension-types): update register_python_class and register_storage_type docstrings for storage-safe contract"
```

---

## Task 2: Fix `register_storage_type` — strip extension types from struct/list fields

**Files:**
- Modify: `src/orcapod/semantic_types/universal_converter.py:441-468`
- Test: `tests/test_semantic_types/test_universal_converter.py`

The current `register_storage_type` builds new struct/list types with the recursed field types, but does **not** strip an extension type before embedding it in a struct field or list value. Under the storage-safe contract it must strip.

- [ ] **Step 1: Write the failing test first**

In `tests/test_semantic_types/test_universal_converter.py`, locate `test_register_storage_type_nested_struct_with_extension` (around line 931). The test currently asserts the extension type is **preserved** in the struct field. Under the new contract it must be **stripped**. Change the last two assertions:

```python
def test_register_storage_type_nested_struct_with_extension():
    """Extension type nested inside a struct field is stripped to storage type (ET1)."""
    import json
    import uuid as _u

    ext_name = f"test.nested.{_u.uuid4().hex[:8]}"
    category = "test.nested"
    metadata = json.dumps({"category": category}).encode()
    ArrowExt = make_arrow_extension_type(ext_name, pa.large_string(), metadata=metadata)
    PolarsExt = make_polars_extension_type(ext_name, pa.large_string())

    class _LT:
        logical_type_name = ext_name
        python_type = str
        def get_arrow_extension_type(self): return ArrowExt()
        def get_polars_extension_type(self): return PolarsExt()
        def python_to_storage(self, v, c=None): return str(v)
        def storage_to_python(self, v, c=None): return v

    class _Factory:
        def supports_class(self, t): return False
        def create_for_python_type(self, t, converter): pass
        def reconstruct_from_arrow(self, name, storage_type, meta, converter):
            return _LT()

    registry = _make_registry_with_builtins()
    registry.register_logical_type_factory(_Factory(), category=category)
    converter = _make_converter(registry)

    ext_instance = ArrowExt()
    struct_with_ext = pa.struct([pa.field("id", pa.int64()), pa.field("tag", ext_instance)])
    result = converter.register_storage_type(struct_with_ext)

    assert pa.types.is_struct(result)
    assert result.field("id").type == pa.int64()
    # Storage-safe: extension type inside struct field is stripped to its storage type
    assert result.field("tag").type == pa.large_string()
    assert not isinstance(result.field("tag").type, pa.ExtensionType)
    # Side effect: the extension type IS registered (check via registry)
    assert converter._logical_type_registry.get_by_arrow_extension_name(ext_name) is not None
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
uv run pytest tests/test_semantic_types/test_universal_converter.py::test_register_storage_type_nested_struct_with_extension -v
```
Expected: FAIL — test currently asserts `isinstance(result.field("tag").type, pa.ExtensionType)`.

- [ ] **Step 3: Fix `register_storage_type` in `universal_converter.py`**

Locate the struct branch (around line 443). Replace the struct and list branches:

**Old struct branch (lines ~443–451):**
```python
        # Struct type — recurse into each field, preserving field-level metadata
        if pa.types.is_struct(arrow_type):
            resolved_fields = []
            for i in range(arrow_type.num_fields):
                field = arrow_type.field(i)
                resolved_type = self.register_storage_type(field.type)
                resolved_fields.append(
                    pa.field(field.name, resolved_type, nullable=field.nullable, metadata=field.metadata)
                )
            return pa.struct(resolved_fields)
```

**New struct branch:**
```python
        # Struct type — recurse into each field, preserving field-level metadata.
        # Strip any extension type from field types before embedding (ET1: Arrow/Polars
        # cannot construct arrays whose struct fields are pa.ExtensionType nodes).
        if pa.types.is_struct(arrow_type):
            resolved_fields = []
            for i in range(arrow_type.num_fields):
                field = arrow_type.field(i)
                resolved_type = self.register_storage_type(field.type)
                if isinstance(resolved_type, pa.ExtensionType):
                    resolved_type = resolved_type.storage_type  # strip: ET1
                resolved_fields.append(
                    pa.field(field.name, resolved_type, nullable=field.nullable, metadata=field.metadata)
                )
            return pa.struct(resolved_fields)
```

**Old large_list branch (lines ~453–458):**
```python
        # Large list type — preserve value field metadata (used by ARROW:extension:* channel)
        if pa.types.is_large_list(arrow_type):
            vf = arrow_type.value_field
            resolved_value = self.register_storage_type(vf.type)
            return pa.large_list(
                pa.field(vf.name, resolved_value, nullable=vf.nullable, metadata=vf.metadata)
            )
```

**New large_list branch:**
```python
        # Large list type — preserve value field metadata (used by ARROW:extension:* channel).
        # Strip any extension type from the value type before embedding (ET1).
        if pa.types.is_large_list(arrow_type):
            vf = arrow_type.value_field
            resolved_value = self.register_storage_type(vf.type)
            if isinstance(resolved_value, pa.ExtensionType):
                resolved_value = resolved_value.storage_type  # strip: ET1
            return pa.large_list(
                pa.field(vf.name, resolved_value, nullable=vf.nullable, metadata=vf.metadata)
            )
```

**Old list branch (lines ~461–466):**
```python
        # List type
        if pa.types.is_list(arrow_type):
            vf = arrow_type.value_field
            resolved_value = self.register_storage_type(vf.type)
            return pa.list_(
                pa.field(vf.name, resolved_value, nullable=vf.nullable, metadata=vf.metadata)
            )
```

**New list branch:**
```python
        # List type — strip any extension type from the value type (ET1).
        if pa.types.is_list(arrow_type):
            vf = arrow_type.value_field
            resolved_value = self.register_storage_type(vf.type)
            if isinstance(resolved_value, pa.ExtensionType):
                resolved_value = resolved_value.storage_type  # strip: ET1
            return pa.list_(
                pa.field(vf.name, resolved_value, nullable=vf.nullable, metadata=vf.metadata)
            )
```

- [ ] **Step 4: Run the test to verify it passes**

```bash
uv run pytest tests/test_semantic_types/test_universal_converter.py::test_register_storage_type_nested_struct_with_extension -v
```
Expected: PASS

- [ ] **Step 5: Run the full `register_storage_type` suite**

```bash
uv run pytest tests/test_semantic_types/test_universal_converter.py -k "register_storage_type" -v
```
Expected: all PASS

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/semantic_types/universal_converter.py tests/test_semantic_types/test_universal_converter.py
git commit -m "fix(universal-converter): register_storage_type strips extension types from struct/list fields (ET1 storage-safe invariant)"
```

---

## Task 3: Fix `_register_python_class_impl` container branches — strip before embedding in list/dict

**Files:**
- Modify: `src/orcapod/semantic_types/universal_converter.py:298-326`
- Test: `tests/test_semantic_types/test_universal_converter.py`

Currently the list/set/dict branches call `self.register_python_class(...)` and embed the result directly in `pa.large_list(...)` or the dict struct. Since `register_python_class` may now return an extension type (e.g. `register_python_class(UUID)` → `orcapod.uuid`), the container branches must strip before embedding to maintain the storage-safe guarantee.

Note: the registry-hit and factory-dispatch return sites (`return lt.get_arrow_extension_type()`) are **already correct** — they return the extension type directly (top-level extension is allowed). No change needed there.

- [ ] **Step 1: Verify existing container tests pass before touching anything**

```bash
uv run pytest tests/test_semantic_types/test_universal_converter.py -k "register_python_class_list or register_python_class_dict or register_python_class_set" -v
```
Expected: PASS

- [ ] **Step 2: Write a new failing test for `list[UUID]` error behaviour**

Add at the end of the `register_python_class` block in `tests/test_semantic_types/test_universal_converter.py`:

```python
def test_register_python_class_list_of_uuid_raises():
    """list[UUID] raises ValueError: UUID is a logical type and cannot be preserved
    inside a list value field (ET2 in DESIGN_ISSUES.md). Tracked in PLT-1732."""
    converter = _make_converter()
    with pytest.raises(ValueError, match="PLT-1732"):
        converter.register_python_class(list[_uuid_module.UUID])


def test_register_python_class_dict_str_uuid_raises():
    """dict[str, UUID] raises ValueError: UUID is a logical type and cannot be preserved
    inside a struct field (ET1/ET2 in DESIGN_ISSUES.md). Tracked in PLT-1732."""
    converter = _make_converter()
    with pytest.raises(ValueError, match="PLT-1732"):
        converter.register_python_class(dict[str, _uuid_module.UUID])
```

- [ ] **Step 3: Run the new tests to verify they fail**

```bash
uv run pytest tests/test_semantic_types/test_universal_converter.py::test_register_python_class_list_of_uuid_raises tests/test_semantic_types/test_universal_converter.py::test_register_python_class_dict_str_uuid_raises -v
```
Expected: FAIL — the list/dict branches currently embed the extension type without raising.

- [ ] **Step 4: Fix the container branches in `_register_python_class_impl`**

Locate the list, set, and dict branches (lines ~297–325). Apply stripping after each recursive `register_python_class` call before embedding in a container:

**Old list branch (lines ~297–304):**
```python
        # list[T] → pa.large_list(T)
        if origin is list:
            if not args:
                raise ValueError(
                    "Unparameterized 'list' is not supported. Use 'list[T]' with a concrete "
                    "element type (e.g. list[int], list[str])."
                )
            return pa.large_list(self.register_python_class(args[0]))
```

**New list branch:**
```python
        # list[T] → pa.large_list(T).  Strip extension type from element (ET1: extension
        # types cannot be nested inside list value types).
        if origin is list:
            if not args:
                raise ValueError(
                    "Unparameterized 'list' is not supported. Use 'list[T]' with a concrete "
                    "element type (e.g. list[int], list[str])."
                )
            inner = self.register_python_class(args[0])
            if isinstance(inner, pa.ExtensionType):
                inner = inner.storage_type  # strip: ET1
            return pa.large_list(inner)
```

**Old set branch (lines ~306–313):**
```python
        # set[T] → pa.large_list(T)
        if origin is set:
            if not args:
                raise ValueError(
                    "Unparameterized 'set' is not supported. Use 'set[T]' with a concrete "
                    "element type (e.g. set[int], set[str])."
                )
            return pa.large_list(self.register_python_class(args[0]))
```

**New set branch:**
```python
        # set[T] → pa.large_list(T).  Strip extension type from element (ET1).
        if origin is set:
            if not args:
                raise ValueError(
                    "Unparameterized 'set' is not supported. Use 'set[T]' with a concrete "
                    "element type (e.g. set[int], set[str])."
                )
            inner = self.register_python_class(args[0])
            if isinstance(inner, pa.ExtensionType):
                inner = inner.storage_type  # strip: ET1
            return pa.large_list(inner)
```

**Old dict branch (lines ~315–325):**
```python
        # dict[K, V] → pa.large_list(struct{key: K, value: V})
        if origin is dict:
            if len(args) < 2:
                raise ValueError(
                    "Unparameterized 'dict' is not supported. Use 'dict[K, V]' with concrete "
                    "key and value types (e.g. dict[str, int])."
                )
            key_arrow = self.register_python_class(args[0])
            val_arrow = self.register_python_class(args[1])
            return pa.large_list(
                pa.struct([pa.field("key", key_arrow), pa.field("value", val_arrow)])
            )
```

**New dict branch:**
```python
        # dict[K, V] → pa.large_list(struct{key: K, value: V}).
        # Strip extension types from key and value before embedding in the struct (ET1).
        if origin is dict:
            if len(args) < 2:
                raise ValueError(
                    "Unparameterized 'dict' is not supported. Use 'dict[K, V]' with concrete "
                    "key and value types (e.g. dict[str, int])."
                )
            key_arrow = self.register_python_class(args[0])
            if isinstance(key_arrow, pa.ExtensionType):
                key_arrow = key_arrow.storage_type  # strip: ET1
            val_arrow = self.register_python_class(args[1])
            if isinstance(val_arrow, pa.ExtensionType):
                val_arrow = val_arrow.storage_type  # strip: ET1
            return pa.large_list(
                pa.struct([pa.field("key", key_arrow), pa.field("value", val_arrow)])
            )
```

- [ ] **Step 5: Run the new tests to verify they pass**

```bash
uv run pytest tests/test_semantic_types/test_universal_converter.py::test_register_python_class_list_of_uuid_raises tests/test_semantic_types/test_universal_converter.py::test_register_python_class_dict_str_uuid_raises -v
```
Expected: PASS

- [ ] **Step 6: Run the full `register_python_class` suite**

```bash
uv run pytest tests/test_semantic_types/test_universal_converter.py -k "register_python_class" -v
```
Expected: all PASS

- [ ] **Step 7: Commit**

```bash
git add src/orcapod/semantic_types/universal_converter.py tests/test_semantic_types/test_universal_converter.py
git commit -m "fix(universal-converter): strip extension types from list/dict container element types in register_python_class (ET1)"
```

---

## Task 4: Delete `_strip_ext_to_storage` and update `create_for_python_type`

**Files:**
- Modify: `src/orcapod/extension_types/dataclass_logical_type_factory.py:45-315`
- Test: `tests/test_extension_types/test_dataclass_logical_type_factory.py`

`_strip_ext_to_storage` (lines 45–90) is now redundant: `register_python_class` already returns a storage-safe type (no nested extension types). The `create_for_python_type` method should replace the recursive `_strip_ext_to_storage(arrow_type)` call with a one-liner strip.

- [ ] **Step 1: Verify the dataclass factory write-path tests pass before touching anything**

```bash
uv run pytest tests/test_extension_types/test_dataclass_logical_type_factory.py -v
```
Expected: all PASS

- [ ] **Step 2: Delete `_strip_ext_to_storage` and update `create_for_python_type`**

In `src/orcapod/extension_types/dataclass_logical_type_factory.py`:

**Delete** the entire `_strip_ext_to_storage` function (lines 45–90, inclusive of docstring).

**Old block in `create_for_python_type` (lines ~310–315):**
```python
            annotation = hints.get(field.name, Any)
            arrow_type = converter.register_python_class(annotation)
            # Strip extension types from struct field types: pa.array cannot build a
            # struct array when a field type is a pa.ExtensionType (see ET1 in
            # DESIGN_ISSUES.md). Value conversion is annotation-driven so stripping is safe.
            stripped_type = _strip_ext_to_storage(arrow_type)
            arrow_fields.append(pa.field(field.name, stripped_type))
```

**New block:**
```python
            annotation = hints.get(field.name, Any)
            arrow_type = converter.register_python_class(annotation)
            # register_python_class returns a storage-safe type: may be extension at the
            # top level, but struct fields are always plain. Strip the top-level extension
            # type here before inserting into the struct (ET1; see DESIGN_ISSUES.md).
            if isinstance(arrow_type, pa.ExtensionType):
                arrow_type = arrow_type.storage_type
            arrow_fields.append(pa.field(field.name, arrow_type))
```

Also update the comment in `DataclassLogicalType.__init__` (lines ~138–141) that references `_strip_ext_to_storage`:

**Old:**
```python
        # ``storage_type`` is already stripped of nested extension types by
        # ``DataclassLogicalTypeFactory.create_for_python_type`` (see ET1 in
        # DESIGN_ISSUES.md).  ``make_polars_extension_type`` and
        # ``pa.array`` both require plain storage types inside structs.
```

**New:**
```python
        # ``storage_type`` must not contain nested extension types (ET1 in DESIGN_ISSUES.md).
        # ``DataclassLogicalTypeFactory.create_for_python_type`` and ``reconstruct_from_arrow``
        # both guarantee this by stripping any top-level extension type from each field's
        # Arrow type before inserting it into the struct.
```

- [ ] **Step 3: Run the dataclass factory write-path tests to verify they still pass**

```bash
uv run pytest tests/test_extension_types/test_dataclass_logical_type_factory.py -v
```
Expected: all PASS

- [ ] **Step 4: Commit**

```bash
git add src/orcapod/extension_types/dataclass_logical_type_factory.py
git commit -m "refactor(dataclass-factory): delete _strip_ext_to_storage, replace with one-liner in create_for_python_type"
```

---

## Task 5: Fix `reconstruct_from_arrow` — register nested types (read-path completeness fix)

**Files:**
- Modify: `src/orcapod/extension_types/dataclass_logical_type_factory.py:367-372`
- Test: `tests/test_extension_types/test_dataclass_logical_type_factory.py`

`reconstruct_from_arrow` currently builds `field_annotations` but never calls `converter.register_python_class` for each annotation. This means nested dataclass types (e.g. `Inner` inside `Outer`) are never registered on the read path, causing `ValueError("Unsupported Python type: Inner.")` in a fresh process.

- [ ] **Step 1: Write the failing test**

Add `test_reconstruct_from_arrow_registers_nested_types` to `tests/test_extension_types/test_dataclass_logical_type_factory.py`. This test requires module-level dataclasses. Add them after the existing module-level dataclass definitions (around line 177), before the "DataclassLogicalTypeFactory write-path tests" section:

```python
@dataclasses.dataclass
class _InnerForRegistrationTest:
    """Module-level inner dataclass for registration completeness test."""
    value: int


@dataclasses.dataclass
class _OuterForRegistrationTest:
    """Module-level outer dataclass for registration completeness test."""
    inner: _InnerForRegistrationTest
    label: str
```

Then add the test:

```python
def test_reconstruct_from_arrow_registers_nested_types():
    """reconstruct_from_arrow for Outer must register Inner as a side effect."""
    from orcapod.extension_types.dataclass_logical_type_factory import DataclassLogicalTypeFactory

    # Build the storage type for _OuterForRegistrationTest manually (as it would come
    # from Parquet): outer struct with an inner struct field (Inner is stored as a struct,
    # NOT as an extension type inside the struct field — that's the ET1 constraint).
    inner_storage = pa.struct([pa.field("value", pa.int64())])
    outer_storage = pa.struct([
        pa.field("inner", inner_storage),
        pa.field("label", pa.large_string()),
    ])
    outer_fqcn = f"{_OuterForRegistrationTest.__module__}.{_OuterForRegistrationTest.__qualname__}"
    inner_fqcn = f"{_InnerForRegistrationTest.__module__}.{_InnerForRegistrationTest.__qualname__}"

    factory = DataclassLogicalTypeFactory()
    converter = _make_full_converter()

    # Inner is NOT pre-registered
    assert converter._logical_type_registry.get_by_python_type(_InnerForRegistrationTest) is None

    # reconstruct_from_arrow for Outer should trigger registration of Inner as a side effect
    lt = factory.reconstruct_from_arrow(outer_fqcn, outer_storage, {"category": "orcapod.dataclass"}, converter)

    # Inner must now be registered
    assert converter._logical_type_registry.get_by_python_type(_InnerForRegistrationTest) is not None
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
uv run pytest "tests/test_extension_types/test_dataclass_logical_type_factory.py::test_reconstruct_from_arrow_registers_nested_types" -v
```
Expected: FAIL — `_InnerForRegistrationTest` is not registered after `reconstruct_from_arrow`.

- [ ] **Step 3: Fix `reconstruct_from_arrow` in `dataclass_logical_type_factory.py`**

Locate the field-iteration loop inside `reconstruct_from_arrow` (around line 367):

**Old:**
```python
        field_annotations = []
        for field in dataclasses.fields(cls):
            if not field.init:
                continue
            annotation = hints.get(field.name, Any)
            field_annotations.append((field.name, annotation))
```

**New:**
```python
        field_annotations = []
        for field in dataclasses.fields(cls):
            if not field.init:
                continue
            annotation = hints.get(field.name, Any)
            # Register any logical type the field annotation maps to (registration
            # completeness invariant: all nested logical types must be registered when
            # the outer type is registered). The return value is discarded; only the
            # side effect of registration matters here.
            converter.register_python_class(annotation)
            field_annotations.append((field.name, annotation))
```

- [ ] **Step 4: Run the test to verify it passes**

```bash
uv run pytest "tests/test_extension_types/test_dataclass_logical_type_factory.py::test_reconstruct_from_arrow_registers_nested_types" -v
```
Expected: PASS

- [ ] **Step 5: Run the full dataclass factory test suite**

```bash
uv run pytest tests/test_extension_types/test_dataclass_logical_type_factory.py -v
```
Expected: all PASS

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/extension_types/dataclass_logical_type_factory.py tests/test_extension_types/test_dataclass_logical_type_factory.py
git commit -m "fix(dataclass-factory): reconstruct_from_arrow registers nested types (registration completeness invariant)"
```

---

## Task 6: Add Parquet round-trip test for nested dataclasses

**Files:**
- Test: `tests/test_extension_types/test_dataclass_logical_type_factory.py`

This test exercises the full fresh-process read path: write a nested dataclass to Parquet, read it back in a converter that has never seen the inner or outer type, call `register_discovered_extensions` + `apply_extension_types`, then convert back to Python. This is the end-to-end regression test for the bug fixed in Task 5.

The two module-level dataclasses needed (`_InnerForRegistrationTest`, `_OuterForRegistrationTest`) were already added in Task 5.

- [ ] **Step 1: Write the test**

Add `test_nested_dataclass_parquet_roundtrip` to `tests/test_extension_types/test_dataclass_logical_type_factory.py`:

```python
def test_nested_dataclass_parquet_roundtrip(tmp_path):
    """Fresh-process Parquet round-trip for a two-level nested dataclass.

    Verifies that register_discovered_extensions triggers the chain:
      register_arrow_extension("Outer") → reconstruct_from_arrow
        → register_python_class(Inner) → registers Inner
    so that storage_to_python can reconstruct the full nested object.
    """
    import pyarrow.parquet as pq
    from orcapod.extension_types.database_hooks import register_discovered_extensions, apply_extension_types

    # ── Write path ───────────────────────────────────────────────────────────
    write_converter = _make_full_converter()

    inner = _InnerForRegistrationTest(value=42)
    outer = _OuterForRegistrationTest(inner=inner, label="hello")

    # Register Outer (which also registers Inner via create_for_python_type)
    write_converter.register_python_class(_OuterForRegistrationTest)

    # Serialize to Arrow using python_schema_to_arrow_schema + python_dicts_to_arrow_table
    outer_fqcn = f"{_OuterForRegistrationTest.__module__}.{_OuterForRegistrationTest.__qualname__}"
    arrow_schema = write_converter.python_schema_to_arrow_schema({"item": _OuterForRegistrationTest})
    rows = [{"item": write_converter.python_to_storage(outer, _OuterForRegistrationTest)}]
    table = write_converter.python_dicts_to_arrow_table(rows, arrow_schema=arrow_schema)

    parquet_path = tmp_path / "nested.parquet"
    pq.write_table(table, parquet_path)

    # ── Read path (fresh converter — neither Inner nor Outer pre-registered) ──
    read_converter = _make_full_converter()
    read_table = pq.read_table(parquet_path)

    # register_discovered_extensions should trigger: Outer → reconstruct_from_arrow
    # → register_python_class(Inner) → registers Inner
    register_discovered_extensions(read_converter, read_table.schema)
    read_table = apply_extension_types(read_table, read_converter._logical_type_registry)

    # Both types must now be registered
    assert read_converter._logical_type_registry.get_by_python_type(_OuterForRegistrationTest) is not None
    assert read_converter._logical_type_registry.get_by_python_type(_InnerForRegistrationTest) is not None

    # Convert back to Python
    rows_out = read_converter.arrow_table_to_python_dicts(read_table)
    assert len(rows_out) == 1
    reconstructed = rows_out[0]["item"]
    assert isinstance(reconstructed, _OuterForRegistrationTest)
    assert isinstance(reconstructed.inner, _InnerForRegistrationTest)
    assert reconstructed.inner.value == 42
    assert reconstructed.label == "hello"
```

- [ ] **Step 2: Run the test to verify it fails before the fix is in place**

(This test should already pass since Task 5 fixed `reconstruct_from_arrow`. If running Tasks in order, it will pass. Run it now to confirm.)

```bash
uv run pytest "tests/test_extension_types/test_dataclass_logical_type_factory.py::test_nested_dataclass_parquet_roundtrip" -v
```
Expected: PASS (Task 5 already made this possible).

- [ ] **Step 3: Run the full dataclass factory test suite to confirm no regressions**

```bash
uv run pytest tests/test_extension_types/test_dataclass_logical_type_factory.py -v
```
Expected: all PASS

- [ ] **Step 4: Commit**

```bash
git add tests/test_extension_types/test_dataclass_logical_type_factory.py
git commit -m "test(dataclass-factory): add Parquet round-trip test for nested dataclasses"
```

---

## Task 7: Update `DESIGN_ISSUES.md` — mark ET1 workaround updated

**Files:**
- Modify: `DESIGN_ISSUES.md` (ET1 entry, around line 1003)

- [ ] **Step 1: Update ET1**

Find the ET1 entry. The **Workaround** section currently references `dataclass_handler._strip_ext_to_storage()`. Update it to reflect that `_strip_ext_to_storage` is gone, replaced by the storage-safe contract on `register_python_class` and `register_storage_type`.

Replace the **Workaround** paragraph in ET1:

**Old:**
```
**Workaround:** `dataclass_handler._strip_ext_to_storage()` recursively replaces all
`pa.ExtensionType` nodes with their plain storage types. This stripping is applied in
`DataclassHandlerFactory.create_for_python_type` when building the struct's field types —
so the stored Arrow schema (and thus the struct passed to `make_polars_extension_type` and
`pa.Table.from_pylist`) never contains nested extension types. The consequence is that the
schema for a dataclass extension column reports downgraded inner field types (e.g.
`large_binary` instead of `orcapod.uuid`). This is invisible through the normal conversion
path (all value conversion flows through `converter.storage_to_python`, which is
annotation-driven), but would mislead any code that directly introspects the raw Arrow
or Polars schema of a dataclass extension column's storage fields.

**Also affects `pa.Table.from_pylist`:** the same restriction applies to PyArrow's
`pa.Table.from_pylist` (and `pa.array`) — neither can build an array from a struct type
whose fields are `pa.ExtensionType` nodes, for the same underlying reason. The stripping
in `create_for_python_type` fixes both issues simultaneously.
```

**New:**
```
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
```

- [ ] **Step 2: Run the full test suite**

```bash
uv run pytest tests/ -x -q
```
Expected: all PASS

- [ ] **Step 3: Commit**

```bash
git add DESIGN_ISSUES.md
git commit -m "docs(design-issues): update ET1 workaround note to reflect removal of _strip_ext_to_storage (PLT-1720)"
```

---

## Task 8: Final verification and push

- [ ] **Step 1: Run the complete test suite**

```bash
uv run pytest tests/ -q
```
Expected: all PASS, no failures, no errors

- [ ] **Step 2: Verify the branch is on the right base**

```bash
git log --oneline extension-type-system..HEAD
```
Expected: 7 commits (Tasks 1–7), all on top of `extension-type-system`.

- [ ] **Step 3: Push the branch**

```bash
git push -u origin eywalker/plt-1720-cleanup-register_python_class-should-return-plain-storage
```

---

## Self-review checklist

**Spec coverage:**
- ✅ `register_python_class` container branches strip extension types (Task 3)
- ✅ `register_storage_type` strips extension types from struct/list fields (Task 2)
- ✅ `_strip_ext_to_storage` deleted (Task 4)
- ✅ `create_for_python_type` uses one-liner strip (Task 4)
- ✅ `reconstruct_from_arrow` calls `register_python_class` per field (Task 5)
- ✅ Protocol docstrings updated (Task 1)
- ✅ `DESIGN_ISSUES.md` ET1 updated (Task 7)
- ✅ `test_register_storage_type_nested_struct_with_extension` updated (Task 2)
- ✅ `test_register_python_class_list_of_uuid_raises` added (Task 3)
- ✅ `test_reconstruct_from_arrow_registers_nested_types` added (Task 5)
- ✅ `test_nested_dataclass_parquet_roundtrip` added (Task 6)
- ✅ `database_hooks.py` unchanged (no task needed — already uses `register_storage_type` return value)
- ✅ Existing `register_python_class` tests (`_registry_hit_path`, `_uuid_registry_hit`, `_factory_dispatch`) — these already assert `isinstance(result, pa.ExtensionType)`, which is still correct under the storage-safe contract. No updates needed.

**Type consistency:** All references to `register_python_class`, `register_storage_type`, `_strip_ext_to_storage`, `create_for_python_type`, and `reconstruct_from_arrow` use the same names as in the source files.

**No placeholders:** Every step has explicit code or commands.
