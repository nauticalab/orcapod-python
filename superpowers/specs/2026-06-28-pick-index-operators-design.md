# Pick and Index Operators — Design Spec

**Issue:** ITL-140  
**Date:** 2026-06-28  
**Status:** Approved

---

## Overview

Introduce two new `UnaryOperator` subclasses — `Pick` and `Index` — that allow users to
project into structured column values: extracting a value from a struct/dict by key (`pick`)
or from a list by position (`index`). They participate in the pipeline DAG with full
packet-level provenance and no content hash.

Motivating example:

```python
stream.pick('column1', 'entry1').index('column1', 3)
# → column1 now holds the int at column1['entry1'][3]
```

---

## File Layout

### New files

| File | Purpose |
|---|---|
| `src/orcapod/extension_types/base_logical_type.py` | `BaseLogicalType` with default `NotImplementedError` impls |
| `src/orcapod/core/operators/pick.py` | `Pick` operator |
| `src/orcapod/core/operators/index.py` | `Index` operator |
| `tests/test_pick_index_operators.py` | Unit + integration tests |

### Modified files

| File | Change |
|---|---|
| `src/orcapod/extension_types/protocols.py` | Add `pick_field` / `index_element` signatures to `LogicalTypeProtocol` |
| `src/orcapod/extension_types/builtin_logical_types.py` | `LogicalPath`, `LogicalUPath`, `LogicalUUID` inherit `BaseLogicalType` |
| `src/orcapod/extension_types/dataclass_logical_type_factory.py` | `DataclassLogicalType` inherits `BaseLogicalType` |
| `src/orcapod/extension_types/pydantic_logical_type_factory.py` | `PydanticLogicalType` inherits `BaseLogicalType` |
| `src/orcapod/core/operators/__init__.py` | Export `Pick`, `Index` |
| `src/orcapod/operators/__init__.py` | Re-export `Pick`, `Index` |
| `src/orcapod/core/streams/base.py` | Add `.pick()` / `.index()` stream convenience methods |

---

## BaseLogicalType and LogicalTypeProtocol

### `src/orcapod/extension_types/base_logical_type.py` (new)

```python
class BaseLogicalType:
    """Shared base for all logical types.

    Provides default NotImplementedError implementations for structural
    projection methods. Concrete logical types that support pick/index
    override these methods.
    """

    def pick_field(self, key: str) -> type:
        """Return the Python type of field `key`.

        Raises NotImplementedError until implemented for this type.
        """
        raise NotImplementedError(
            f"{type(self).__name__} does not yet support pick (keyed field access). "
            "Support for this extension type is planned for a future issue."
        )

    def index_element(self) -> type:
        """Return the Python element type for positional access.

        Raises NotImplementedError until implemented for this type.
        """
        raise NotImplementedError(
            f"{type(self).__name__} does not yet support index (positional access). "
            "Support for this extension type is planned for a future issue."
        )
```

### `LogicalTypeProtocol` additions

Add two method signatures to the existing protocol:

```python
def pick_field(self, key: str) -> type:
    """Return the Python type of field `key` in this structured logical type.

    Raises:
        KeyError / InputValidationError: if the field does not exist.
        NotImplementedError: if this logical type does not support keyed access.
    """
    ...

def index_element(self) -> type:
    """Return the Python element type for positional list access.

    Raises:
        NotImplementedError: if this logical type does not support positional access.
    """
    ...
```

### Existing implementers

All five existing implementers gain `BaseLogicalType` as their base class (one-line change each):

- `DataclassLogicalType(BaseLogicalType)`
- `PydanticLogicalType(BaseLogicalType)`
- `LogicalPath(BaseLogicalType)`
- `LogicalUPath(BaseLogicalType)`
- `LogicalUUID(BaseLogicalType)`

No other code in those classes changes. Future issues (Pydantic/dataclass support) will
override `pick_field` / `index_element` with real implementations.

---

## Pick Operator

### Signature

```python
class Pick(UnaryOperator):
    def __init__(
        self,
        column: str,
        key: str,
        out: str | None = None,
        fail_on_miss: bool = False,
        **kwargs,
    ):
        self.column = column
        self.key = key
        self.out = out          # None → replace column in-place; str → add new column
        self.fail_on_miss = fail_on_miss   # not in identity_structure
        super().__init__(**kwargs)

    def identity_structure(self):
        return (self.__class__.__name__, self.column, self.key, self.out)
        # fail_on_miss excluded: it controls error behaviour, not functional semantics
```

### Type resolution at build time (`validate_unary_input`)

1. Confirm `column` exists in the stream's data schema → `InputValidationError` if absent.
2. Retrieve `col_type = data_schema[column]`.
3. Dispatch on type:
   - `typing.get_origin(col_type) is dict` → **map mode**. Output type = `typing.get_args(col_type)[1]` (the value type `V`). No static key check — keys are per-packet data.
   - Otherwise → **extension type mode**. Look up the logical type via the registry, call `logical_type.pick_field(key)`. This returns the field's Python type for a valid struct field, raises `InputValidationError` for a missing field (hard build-time error), or raises `NotImplementedError` for unsupported extension types (Pydantic/dataclass until future issues land).
4. Validate `out`:
   - If `out` is a string that already exists in the data schema → `InputValidationError` (no silent clobbering).
5. Store resolved output type in `self._output_type` and mode in `self._mode`.

### Schema prediction (`unary_output_schema`)

- `out=None`: same schema as input, `column` type replaced with `self._output_type`.
- `out='new_name'`: input schema plus new column `new_name: self._output_type`.

### Source-info rule

The source token always reflects the projection path — regardless of whether the column
name changes:

- `out=None`: update `_source_column` → `original_source_token['key']`
- `out='new_name'`: keep `_source_column` unchanged; add `_source_new_name` = `original_source_token['key']`

Example: original source `"src_id::rec_id::column"` → `"src_id::rec_id::column['entry1']"`.

### Streaming execution (`async_execute`)

```python
async for tag, data in inputs[0]:
    col_val = data[self.column]

    if self._mode == 'struct':
        # Statically guaranteed — field must exist
        extracted = col_val[self.key]

    elif self._mode == 'map':
        if self.key not in col_val:
            if self.fail_on_miss:
                raise RuntimeError(
                    f"Pick: key {self.key!r} not found in column {self.column!r} "
                    f"(fail_on_miss=True). See ITL-439."
                )
            logger.warning(
                "Pick: skipping packet — key %r not found in column %r",
                self.key, self.column,
            )
            continue
        extracted = col_val[self.key]

    src_token = data.source_info().get(self.column, "")
    new_src = f"{src_token}[{self.key!r}]" if src_token else None

    if self.out is None:
        new_data = (
            data.with_values({self.column: extracted})
                .with_source_info(**{self.column: new_src})
        )
    else:
        new_data = (
            data.with_values({self.out: extracted})
                .with_source_info(**{self.out: new_src})
        )

    await output.send((tag, new_data))
```

### Batch execution (`unary_static_process`)

Uses `pc.map_lookup` on the full Arrow table:

1. `extracted = pc.map_lookup(table[column], key, null_handling="skip")`
2. `valid_mask = pc.is_valid(extracted)` — True where key was found.
3. Count skipped rows; if `fail_on_miss=True` and `n_skipped > 0` → raise immediately.
4. Otherwise warn once with count, then filter: `table = table.filter(valid_mask)`.
5. Update source-info column(s) by appending `['key']` to the existing token values via `pc.binary_join_element_wise`.
6. Replace (`out=None`) or append (`out='new_name'`) the extracted column.
7. Return `ArrowTableStream(result_table, tag_columns=...)`.

---

## Index Operator

### Signature

```python
class Index(UnaryOperator):
    def __init__(
        self,
        column: str,
        i: int,
        out: str | None = None,
        fail_on_miss: bool = False,
        **kwargs,
    ):
        self.column = column
        self.i = i              # negative indices follow Python semantics
        self.out = out
        self.fail_on_miss = fail_on_miss   # not in identity_structure
        super().__init__(**kwargs)

    def identity_structure(self):
        return (self.__class__.__name__, self.column, self.i, self.out)
```

### Type resolution at build time (`validate_unary_input`)

1. Confirm `column` exists in data schema.
2. `typing.get_origin(col_type) is list` → **list mode**. Output type = `typing.get_args(col_type)[0]` (element type `T`).
3. Otherwise → extension type mode: call `logical_type.index_element()` → raises `NotImplementedError` for v1.
4. Same `out` collision check as `Pick`.
5. No static bounds check — list lengths are per-packet data.

### Source-info rule

Same pattern as `Pick`, with `[i]` appended instead of `['key']`:

- `out=None`: update `_source_column` → `original_source_token[i]`
- `out='new_name'`: add `_source_new_name` = `original_source_token[i]`

Example: `"src_id::rec_id::column[3]"`.

### Streaming execution (`async_execute`)

```python
async for tag, data in inputs[0]:
    col_val = data[self.column]
    length = len(col_val)
    effective_i = self.i if self.i >= 0 else length + self.i

    if effective_i < 0 or effective_i >= length:
        if self.fail_on_miss:
            raise RuntimeError(
                f"Index: index {self.i} out of bounds for column {self.column!r} "
                f"(length {length}, fail_on_miss=True). See ITL-439."
            )
        logger.warning(
            "Index: skipping packet — index %d out of bounds for column %r (length %d)",
            self.i, self.column, length,
        )
        continue

    extracted = col_val[self.i]   # Python handles negative indexing

    src_token = data.source_info().get(self.column, "")
    new_src = f"{src_token}[{self.i}]" if src_token else None

    if self.out is None:
        new_data = (
            data.with_values({self.column: extracted})
                .with_source_info(**{self.column: new_src})
        )
    else:
        new_data = (
            data.with_values({self.out: extracted})
                .with_source_info(**{self.out: new_src})
        )

    await output.send((tag, new_data))
```

### Batch execution (`unary_static_process`)

Uses PyArrow list compute operations:

1. `pc.list_slice(table[column], self.i, self.i + 1)` (supports negative offsets) to extract a single-element sublist per row.
2. Derive a validity mask by checking whether the slice is non-empty (handles OOB).
3. Count skipped rows; raise or warn per `fail_on_miss`.
4. `pc.list_flatten(sliced)` to unwrap the single-element lists into a flat array of type `T`.
5. Update source-info column(s) with `[i]` suffix.
6. Replace or append column; return `ArrowTableStream`.

---

## Stream Convenience Methods

Added to `src/orcapod/core/streams/base.py`:

```python
def pick(
    self,
    column: str,
    key: str,
    out: str | None = None,
    fail_on_miss: bool = False,
    label: str | None = None,
) -> "StreamBase":
    """Extract a value from a struct- or dict-typed column by key.

    Args:
        column: Data column to project into.
        key: Field name (struct) or dict key to extract.
        out: Output column name. None replaces `column` in-place; a string
             adds a new column alongside the original.
        fail_on_miss: If True, raise on missing key instead of skipping.
        label: Optional pipeline label for this node.
    """
    from orcapod.core.operators import Pick
    return Pick(column, key, out=out, fail_on_miss=fail_on_miss)(self, label=label)


def index(
    self,
    column: str,
    i: int,
    out: str | None = None,
    fail_on_miss: bool = False,
    label: str | None = None,
) -> "StreamBase":
    """Extract an element from a list-typed column by position.

    Args:
        column: Data column to project into.
        i: Position to extract. Negative indices follow Python semantics (-1 = last).
        out: Output column name. None replaces `column` in-place; a string
             adds a new column alongside the original.
        fail_on_miss: If True, raise on out-of-bounds instead of skipping.
        label: Optional pipeline label for this node.
    """
    from orcapod.core.operators import Index
    return Index(column, i, out=out, fail_on_miss=fail_on_miss)(self, label=label)
```

Enables natural chaining:

```python
stream.pick('column1', 'entry1').index('column1', 3)
# column1 → column1['entry1'] → column1['entry1'][3]
```

---

## Tests

All tests in `tests/test_pick_index_operators.py`.

### Pick tests

| Test | What it verifies |
|---|---|
| `test_pick_dict_default_out` | `pick` on `dict[str, int]` column, `out=None` — value extracted, column replaced, source token updated to `token['key']` |
| `test_pick_dict_explicit_out` | `pick` with `out='new_col'` — original column preserved, new column added with correct source token |
| `test_pick_dict_all_keys_present` | All 3 packets have the key — all pass through, no warning |
| `test_pick_dict_missing_key_skip` | 3-packet stream; middle packet's dict lacks the key; `fail_on_miss=False` — 2 packets in output, warning emitted, surviving packets correct |
| `test_pick_dict_missing_key_fail` | Same stream, `fail_on_miss=True` — raises on the missing-key packet |
| `test_pick_struct_static_error` | `pick` on Arrow struct with non-existent field → `InputValidationError` at build time |
| `test_pick_struct_valid` | `pick` on Arrow struct with valid field — static type resolved, correct output |
| `test_pick_extension_type_not_implemented` | `pick` on Pydantic / dataclass column → `NotImplementedError` at build time |
| `test_pick_invalid_column` | Column not in schema → `InputValidationError` at build time |
| `test_pick_out_collision` | `out` name already exists in schema → `InputValidationError` |

### Index tests

| Test | What it verifies |
|---|---|
| `test_index_list_default_out` | `index` on `list[int]`, `out=None` — element extracted, source token updated to `token[i]` |
| `test_index_list_explicit_out` | `index` with `out='new_col'` — original preserved, new column correct |
| `test_index_in_bounds_negative` | `i=-1` on length-3 list — returns last element, no skip |
| `test_index_oob_positive_skip` | `i=5` on length-3 list, `fail_on_miss=False` — packet skipped, warning emitted |
| `test_index_oob_positive_fail` | Same, `fail_on_miss=True` — raises |
| `test_index_oob_negative_skip` | `i=-5` on length-3 list, `fail_on_miss=False` — packet skipped, warning emitted |
| `test_index_oob_negative_fail` | Same, `fail_on_miss=True` — raises |
| `test_index_invalid_column` | Column not in schema → `InputValidationError` at build time |
| `test_index_out_collision` | `out` name already in schema → `InputValidationError` |

### Integration tests

| Test | What it verifies |
|---|---|
| `test_chained_pick_then_index` | `stream.pick('col', 'key').index('col', 3)` — end-to-end composition, correct final value and source token |
| `test_composition_with_join` | `pick` used alongside `join` in a larger pipeline |

Both `test_pick_dict_missing_key_skip` and all OOB index tests cover both the streaming
path (`async_execute`) and the batch/static path (`unary_static_process`).

---

## Out of Scope (v1)

- `slice` operator (range selection on lists) — separate follow-up.
- Pydantic / dataclass `pick_field` / `index_element` implementations — future issues.
- Configurable miss policy beyond `fail_on_miss` — tracked in ITL-439.
- Optimization: fusing chained `pick`/`index` into a single Arrow extraction.
- Tuple indexing, string slicing.

## Follow-up Issues to File After Working Implementation

Once the core `Pick` and `Index` operators are working (as part of ITL-140), file two
follow-up issues before closing the issue:

1. **Add `pick_field` support to `DataclassLogicalType`** — implement `pick_field(key)`
   on `DataclassLogicalType` to enable static type resolution and runtime Python-object
   field access for dataclass-typed columns.

2. **Add `pick_field` / `index_element` support to `PydanticLogicalType`** — implement
   both methods on `PydanticLogicalType` to enable static type resolution and runtime
   Python-object field access for Pydantic-model-typed columns.

Both issues should reference ITL-140 as the parent work and include tests for the static
type-resolution path (correct output type at build time, hard error on missing field) and
the runtime path (indexing directly on the Python object).
