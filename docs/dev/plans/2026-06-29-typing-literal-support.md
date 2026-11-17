# typing.Literal Support in UniversalTypeConverter — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `typing.Literal` support to `UniversalTypeConverter` so pydantic models with `Literal[...]` fields can be registered and serialized as Arrow pipeline columns.

**Architecture:** Two surgical branch additions to `universal_converter.py` — one in `_register_python_class_impl` (type-registration path called by the pydantic factory) and one in `_convert_python_to_arrow` (schema-inference/serialization path called during value conversion). No new files. All tests added to the existing pydantic factory test file.

**Tech Stack:** Python `typing.Literal` / `get_origin` / `get_args`, PyArrow, pydantic v2, pytest. Run all commands via `uv run` (never bare `python` or `pytest`).

---

## File Map

| File | Action | What changes |
|---|---|---|
| `src/orcapod/semantic_types/universal_converter.py` | Modify | `Literal` branch in `_register_python_class_impl` (after line ~295); `Literal` branch in `_convert_python_to_arrow` (after line ~1075) |
| `tests/test_extension_types/test_pydantic_logical_type_factory.py` | Modify | Import `Literal`; 6 new module-level model classes; 7 new test functions |

---

## Task 1: Registration-path tests + fix `_register_python_class_impl`

**Files:**
- Modify: `tests/test_extension_types/test_pydantic_logical_type_factory.py`
- Modify: `src/orcapod/semantic_types/universal_converter.py`

- [ ] **Step 1.1: Add `Literal` to the import line in the test file**

Find line 5 in `tests/test_extension_types/test_pydantic_logical_type_factory.py`:

```python
from typing import Any
```

Replace with:

```python
from typing import Any, Literal
```

- [ ] **Step 1.2: Add 6 module-level model classes to the test file**

Insert immediately after the `_ModelWithPrivateAttr` class (around line 177), before the
`# ── Module-level models for read-path and round-trip tests ──` comment:

```python
class _LiteralStrModel(BaseModel):
    method: Literal["a", "b"]


class _LiteralIntModel(BaseModel):
    count: Literal[1, 2, 3]


class _LiteralNoneModel(BaseModel):
    status: Literal["active", None]


class _LiteralNoneOnlyModel(BaseModel):
    x: Literal[None]


class _MixedLiteralModel(BaseModel):
    val: Literal["a", 1]  # type: ignore[assignment]


class _LiteralRoundTripModel(BaseModel):
    method: Literal["a", "b"]
    count: int
```

- [ ] **Step 1.3: Write the 5 failing registration-path tests**

Append to the end of `tests/test_extension_types/test_pydantic_logical_type_factory.py`:

```python
# ── typing.Literal support tests (ITL-442) ───────────────────────────────────


def test_factory_create_model_with_literal_str_field():
    """Literal["a", "b"] field → large_string in the Arrow struct."""
    from orcapod.extension_types.pydantic_logical_type_factory import PydanticLogicalTypeFactory

    factory = PydanticLogicalTypeFactory()
    converter = _make_full_converter()
    lt = factory.create_for_python_type(_LiteralStrModel, converter=converter)

    storage = lt.get_arrow_extension_type().storage_type
    assert storage.field("method").type == pa.large_string()


def test_factory_create_model_with_literal_int_field():
    """Literal[1, 2, 3] field → int64 in the Arrow struct."""
    from orcapod.extension_types.pydantic_logical_type_factory import PydanticLogicalTypeFactory

    factory = PydanticLogicalTypeFactory()
    converter = _make_full_converter()
    lt = factory.create_for_python_type(_LiteralIntModel, converter=converter)

    storage = lt.get_arrow_extension_type().storage_type
    assert storage.field("count").type == pa.int64()


def test_factory_create_model_with_literal_none_field():
    """Literal["active", None] strips None → resolves to large_string."""
    from orcapod.extension_types.pydantic_logical_type_factory import PydanticLogicalTypeFactory

    factory = PydanticLogicalTypeFactory()
    converter = _make_full_converter()
    lt = factory.create_for_python_type(_LiteralNoneModel, converter=converter)

    storage = lt.get_arrow_extension_type().storage_type
    assert storage.field("status").type == pa.large_string()


def test_factory_rejects_literal_none_only():
    """Literal[None] has no concrete value type — raises ValueError."""
    from orcapod.extension_types.pydantic_logical_type_factory import PydanticLogicalTypeFactory

    factory = PydanticLogicalTypeFactory()
    converter = _make_full_converter()
    with pytest.raises(ValueError, match="Literal\\[None\\]"):
        factory.create_for_python_type(_LiteralNoneOnlyModel, converter=converter)


def test_factory_rejects_mixed_literal():
    """Literal["a", 1] mixes str and int — raises ValueError."""
    from orcapod.extension_types.pydantic_logical_type_factory import PydanticLogicalTypeFactory

    factory = PydanticLogicalTypeFactory()
    converter = _make_full_converter()
    with pytest.raises(ValueError, match="Mixed-type Literal"):
        factory.create_for_python_type(_MixedLiteralModel, converter=converter)
```

- [ ] **Step 1.4: Run the 5 tests and confirm they all FAIL**

```bash
uv run pytest tests/test_extension_types/test_pydantic_logical_type_factory.py \
    -k "literal" -v 2>&1 | tail -20
```

Expected: all 5 tests FAIL with `ValueError: Unsupported annotation: typing.Literal[...]`

- [ ] **Step 1.5: Add the `Literal` branch to `_register_python_class_impl`**

In `src/orcapod/semantic_types/universal_converter.py`, find `_register_python_class_impl`
(line 268). Locate the `Union/Optional` block — it looks like this (ends around line 295):

```python
        # Optional[T] / T | None → strip None arm
        if origin is typing.Union or origin is _types_mod.UnionType:
            non_none = [a for a in args if a is not type(None)]
            if len(non_none) == 1:
                return self.register_python_class(non_none[0])
            raise ValueError(
                f"Complex unions with multiple non-None types are not supported: "
                f"{annotation!r}. Only Optional[T] (T | None) is allowed."
            )

        # list[T] → pa.large_list(T).
```

Replace with (inserting the new `Literal` block between the `Union` block and the `list` comment):

```python
        # Optional[T] / T | None → strip None arm
        if origin is typing.Union or origin is _types_mod.UnionType:
            non_none = [a for a in args if a is not type(None)]
            if len(non_none) == 1:
                return self.register_python_class(non_none[0])
            raise ValueError(
                f"Complex unions with multiple non-None types are not supported: "
                f"{annotation!r}. Only Optional[T] (T | None) is allowed."
            )

        # typing.Literal[v1, v2, ...] → Arrow type of the literal values' type.
        # None members are stripped (treat as optional/nullable); mixed non-None types raise.
        if origin is typing.Literal:
            value_types = {type(a) for a in args if a is not None}
            if not value_types:
                raise ValueError(
                    f"Literal[None] is not supported as an Arrow type. "
                    f"Use Optional[T] to express nullability instead."
                )
            if len(value_types) != 1:
                raise ValueError(
                    f"Mixed-type Literal is not supported: {annotation!r}. "
                    f"All members must share one type (e.g. Literal['a', 'b'])."
                )
            return self.register_python_class(next(iter(value_types)))

        # list[T] → pa.large_list(T).
```

- [ ] **Step 1.6: Run the 5 tests and confirm they all PASS**

```bash
uv run pytest tests/test_extension_types/test_pydantic_logical_type_factory.py \
    -k "literal" -v 2>&1 | tail -20
```

Expected: all 5 tests PASS.

- [ ] **Step 1.7: Run the full pydantic factory test suite — confirm no regressions**

```bash
uv run pytest tests/test_extension_types/test_pydantic_logical_type_factory.py -v 2>&1 | tail -20
```

Expected: all tests PASS.

- [ ] **Step 1.8: Commit**

```bash
git add tests/test_extension_types/test_pydantic_logical_type_factory.py \
        src/orcapod/semantic_types/universal_converter.py
git commit -m "fix(types): support typing.Literal in _register_python_class_impl (ITL-442)"
```

---

## Task 2: Serialization-path test + fix `_convert_python_to_arrow`

**Files:**
- Modify: `tests/test_extension_types/test_pydantic_logical_type_factory.py`
- Modify: `src/orcapod/semantic_types/universal_converter.py`

**Why a second fix is needed:** `_create_python_to_arrow_converter` (the value-serialization path) calls `python_type_to_arrow_type` → `_convert_python_to_arrow` internally. Without this fix, value serialization of `Literal` fields inside pydantic models raises `ValueError: Unsupported generic type: typing.Literal` even though type registration now succeeds.

- [ ] **Step 2.1: Write the failing round-trip test**

Append to `tests/test_extension_types/test_pydantic_logical_type_factory.py`:

```python
def test_literal_model_round_trip():
    """python_to_storage → storage_to_python round-trip for a model with Literal fields."""
    from orcapod.extension_types.pydantic_logical_type_factory import PydanticLogicalTypeFactory

    factory = PydanticLogicalTypeFactory()
    converter = _make_full_converter()
    lt = factory.create_for_python_type(_LiteralRoundTripModel, converter=converter)
    converter.register_logical_type(lt)

    instance = _LiteralRoundTripModel(method="a", count=42)
    storage_value = lt.python_to_storage(instance, converter)
    assert storage_value == {"method": "a", "count": 42}

    reconstructed = lt.storage_to_python(storage_value, converter)
    assert isinstance(reconstructed, _LiteralRoundTripModel)
    assert reconstructed.method == "a"
    assert reconstructed.count == 42
```

- [ ] **Step 2.2: Run the round-trip test and confirm it FAILS**

```bash
uv run pytest tests/test_extension_types/test_pydantic_logical_type_factory.py \
    ::test_literal_model_round_trip -v 2>&1 | tail -20
```

Expected: FAIL with `ValueError: Unsupported generic type: typing.Literal`

- [ ] **Step 2.3: Add the `Literal` branch to `_convert_python_to_arrow`**

In `src/orcapod/semantic_types/universal_converter.py`, find `_convert_python_to_arrow`
(line ~997). Locate the `Union/Optional` elif block — it ends around line 1075 and is
immediately followed by `# Handle set types → lists`:

```python
        # Handle Union/Optional types
        elif origin is typing.Union or origin is types.UnionType:
            non_none_types = [t for t in args if t is not type(None)]
            if len(non_none_types) == 1:
                # Optional[T] → just T (nullability handled at field level)
                return self.python_type_to_arrow_type(non_none_types[0])
            else:
                raise ValueError(
                    f"Complex unions with multiple non-None types are not supported: {python_type}. "
                    f"Only Optional[T] (i.e., T | None) is allowed."
                )

        # Handle set types → lists
        elif origin is set:
```

Replace with (inserting the `Literal` elif between `Union` and `set`):

```python
        # Handle Union/Optional types
        elif origin is typing.Union or origin is types.UnionType:
            non_none_types = [t for t in args if t is not type(None)]
            if len(non_none_types) == 1:
                # Optional[T] → just T (nullability handled at field level)
                return self.python_type_to_arrow_type(non_none_types[0])
            else:
                raise ValueError(
                    f"Complex unions with multiple non-None types are not supported: {python_type}. "
                    f"Only Optional[T] (i.e., T | None) is allowed."
                )

        # typing.Literal[v1, v2, ...] → Arrow type of the literal values' type.
        # None members are stripped; mixed non-None types raise.
        elif origin is typing.Literal:
            value_types = {type(a) for a in args if a is not None}
            if not value_types:
                raise ValueError(
                    f"Literal[None] is not supported as an Arrow type. "
                    f"Use Optional[T] to express nullability instead."
                )
            if len(value_types) != 1:
                raise ValueError(
                    f"Mixed-type Literal is not supported: {python_type!r}. "
                    f"All members must share one type (e.g. Literal['a', 'b'])."
                )
            return self.python_type_to_arrow_type(next(iter(value_types)))

        # Handle set types → lists
        elif origin is set:
```

- [ ] **Step 2.4: Run the round-trip test and confirm it PASSES**

```bash
uv run pytest tests/test_extension_types/test_pydantic_logical_type_factory.py \
    ::test_literal_model_round_trip -v 2>&1 | tail -20
```

Expected: PASS.

- [ ] **Step 2.5: Run the full pydantic factory test suite — confirm no regressions**

```bash
uv run pytest tests/test_extension_types/test_pydantic_logical_type_factory.py -v 2>&1 | tail -20
```

Expected: all tests PASS.

- [ ] **Step 2.6: Commit**

```bash
git add tests/test_extension_types/test_pydantic_logical_type_factory.py \
        src/orcapod/semantic_types/universal_converter.py
git commit -m "fix(types): support typing.Literal in _convert_python_to_arrow (ITL-442)"
```

---

## Task 3: End-to-end Arrow table conversion test

**Files:**
- Modify: `tests/test_extension_types/test_pydantic_logical_type_factory.py`

This test exercises both fixes together via the full `register_python_class` →
`python_dicts_to_arrow_table` → `arrow_table_to_python_dicts` path, mirroring exactly what
`DictSource` does internally.

- [ ] **Step 3.1: Write the end-to-end test**

Append to `tests/test_extension_types/test_pydantic_logical_type_factory.py`:

```python
def test_literal_model_as_dictsource_column():
    """Full Arrow table round-trip for a model with a Literal field (ITL-442 repro).

    Exercises the complete path: register_python_class → python_dicts_to_arrow_table →
    arrow_table_to_python_dicts. This is the same sequence DictSource executes internally.
    Before the fix, register_python_class raised:
        ValueError: Unsupported annotation: typing.Literal['a', 'b']
    """
    converter = _make_full_converter()

    # Step A: register the model — previously raised ValueError
    converter.register_python_class(_LiteralStrModel)

    # Step B: convert Python dicts to Arrow table (mirrors DictSource.__init__)
    rows = [{"config": _LiteralStrModel(method="a")}]
    arrow_schema = converter.python_schema_to_arrow_schema({"config": _LiteralStrModel})
    table = converter.python_dicts_to_arrow_table(rows, arrow_schema=arrow_schema)

    assert table.num_rows == 1
    assert "config" in table.schema.names

    # Step C: round-trip back to Python dicts
    result = converter.arrow_table_to_python_dicts(table)
    assert len(result) == 1
    assert isinstance(result[0]["config"], _LiteralStrModel)
    assert result[0]["config"].method == "a"
```

- [ ] **Step 3.2: Run the end-to-end test and confirm it PASSES**

```bash
uv run pytest tests/test_extension_types/test_pydantic_logical_type_factory.py \
    ::test_literal_model_as_dictsource_column -v 2>&1 | tail -20
```

Expected: PASS.

- [ ] **Step 3.3: Run the full extension-types test suite**

```bash
uv run pytest tests/test_extension_types/ -v 2>&1 | tail -30
```

Expected: all tests PASS.

- [ ] **Step 3.4: Run the full test suite**

```bash
uv run pytest tests/ -x --ignore=tests/test_channels --ignore=tests/test_databases \
    -q 2>&1 | tail -30
```

(`--ignore` skips integration tests that require external services. `-x` stops on first
failure so errors are easy to read.)

Expected: all tests PASS.

- [ ] **Step 3.5: Commit**

```bash
git add tests/test_extension_types/test_pydantic_logical_type_factory.py
git commit -m "test(types): add end-to-end Literal model Arrow round-trip test (ITL-442)"
```

---

## Self-Review

**Spec coverage:**
- `_register_python_class_impl` fix → Task 1, Step 1.5 ✓
- `_convert_python_to_arrow` fix → Task 2, Step 2.3 ✓
- `Literal["a", "b"]` → `large_string` → Task 1, test 1 ✓
- `Literal[1, 2]` → `int64` → Task 1, test 2 ✓
- `Literal["a", None]` → `large_string` (None stripped) → Task 1, test 3 ✓
- `Literal[None]` raises `ValueError` → Task 1, test 4 ✓
- Mixed `Literal["a", 1]` raises `ValueError` → Task 1, test 5 ✓
- Round-trip value serialization → Task 2, test 6 ✓
- End-to-end DictSource path → Task 3, test 7 ✓

**Type consistency:** `_LiteralStrModel`, `_LiteralRoundTripModel`, etc. are defined once in
Step 1.2 and referenced identically in all subsequent tests. ✓

**No placeholders.** ✓
