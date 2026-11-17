# ITL-10: Dataclass → DataFrame Conversion — Test Coverage

**Issue:** ITL-10
**Date:** 2026-07-08
**Status:** In progress

---

## Overview

ITL-10 reported that a `FunctionNode` with a dataclass return type would fail on
`.as_df()` with `ValueError: Unsupported Python type: typing.Any`. That crash was
already fixed as part of the FN1 work (see `DESIGN_ISSUES.md`): `as_table()` now
derives its schema from `output_schema()` directly instead of round-tripping
through `arrow_schema_to_python_schema`.

This spec closes ITL-10 by adding the test coverage the issue's goals require.
No production code changes are needed.

---

## Staleness Assessment

The issue carries a `::Warning:: This issue is likely stale` notice. Investigation
confirmed:

- The exact reproducer from the issue runs without error and returns a valid Polars
  DataFrame.
- `arrow_schema_to_python_schema` correctly returns the original dataclass type
  (not `Any`) when the extension type wrapper is present — which it always is for
  in-memory and Parquet/Delta round-trips via the normal orcapod write path.
- When class reconstruction fails at read time (class not importable),
  `_import_from_fqcn` raises `ImportError` loudly — there is no silent fallback
  to `Any`.
- When a plain Arrow struct (no extension metadata) is passed to
  `arrow_schema_to_python_schema`, the converter creates a dynamic TypedDict — not
  `Any` — and maintains a bidirectional cache so the TypedDict can be written back
  to the original struct correctly.

The "polymorphic column" test case from the original issue goals is not applicable:
the current encoding uses one extension type per column (not a `__type` sentinel
field), so a single column cannot hold multiple dataclass types.

---

## Goals & Success Criteria

- `FunctionNode` with a dataclass return type → `.as_df()` returns a well-formed
  Polars DataFrame with the dataclass column at the correct type.
- An unrun `FunctionNode` → `.as_df()` returns a zero-row DataFrame whose schema
  matches the schema of a populated run — no missing columns, no `Any`.
- `arrow_schema_to_python_schema` on a plain Arrow struct (no extension metadata)
  returns a dynamic TypedDict, not `Any`.
- That dynamic TypedDict round-trips back to the identical Arrow struct type via
  `python_type_to_arrow_type`.
- `python_dicts_to_struct_dicts` with a TypedDict schema correctly produces Arrow-
  compatible struct dicts that `pa.Table.from_pylist` can consume.

---

## Scope & Boundaries

In scope:
- Two new test files (see below).
- `@dataclass` fixtures defined at module level (not inside test functions) to
  satisfy `get_type_hints` and the FQCN requirement.

Out of scope:
- Production code changes.
- Polymorphic dataclass columns (multiple types in one column) — not supported by
  the current extension-type encoding.
- Changes to `DESIGN_ISSUES.md` — FN1 is already marked resolved.
- Testing reconstruction failure / `ImportError` propagation (separate concern).

---

## Design

### File 1: `tests/test_core/nodes/test_function_node_dataclass.py`

End-to-end pipeline tests using `InMemoryArrowDatabase`, `PipelineJob`, and a real
`@function_pod` with a dataclass return type.

#### Fixture

```python
import dataclasses
import orcapod as op
from orcapod.databases import InMemoryArrowDatabase

@dataclasses.dataclass
class _Result:
    total: int
    delta: int

@op.function_pod("result")
def _take_sum(a: int, b: int) -> _Result:
    return _Result(a + b, a - b)
```

#### `test_as_df_monomorphic_dataclass_column`

```
store = InMemoryArrowDatabase()
job = PipelineJob(store=store)
source = DictSource([{"id": 0, "a": 5, "b": 3}], tag_columns=["id"])

with job:
    _take_sum.pod(source)

job.run()
df = job._take_sum.as_df()

assert df.shape == (1, 2)
assert "id" in df.columns
assert "result" in df.columns
# result column should NOT be Any or a plain TypedDict — it should be an extension type
assert df["result"].dtype != pl.Null
row = df["result"][0]
assert row["total"] == 8
assert row["delta"] == 2
```

#### `test_as_df_empty_node_has_correct_schema`

```
job = PipelineJob(store=InMemoryArrowDatabase())
source = DictSource([{"id": 0, "a": 5, "b": 3}], tag_columns=["id"])

with job:
    _take_sum.pod(source)

# Do NOT call job.run() — node has no results yet
df = job._take_sum.as_df()

assert df.shape[0] == 0         # zero rows
assert "id" in df.columns
assert "result" in df.columns   # column present even though no data
```

#### `test_empty_schema_matches_nonempty_schema`

```
source = DictSource([{"id": 0, "a": 5, "b": 3}], tag_columns=["id"])

# Populated node
store_a = InMemoryArrowDatabase()
job_a = PipelineJob(store=store_a)
with job_a:
    _take_sum.pod(source)
job_a.run()
full_df = job_a._take_sum.as_df()

# Unrun node (same topology)
store_b = InMemoryArrowDatabase()
job_b = PipelineJob(store=store_b)
with job_b:
    _take_sum.pod(source)
empty_df = job_b._take_sum.as_df()

assert full_df.num_rows > 0
assert empty_df.num_rows == 0
assert set(empty_df.columns) == set(full_df.columns)
assert empty_df.schema == full_df.schema
```

---

### File 2: `tests/test_extension_types/test_universal_converter_struct.py`

Converter-level unit tests. No pipeline machinery — exercises
`UniversalTypeConverter` directly.

#### Fixture

```python
import pyarrow as pa
from orcapod.contexts import get_default_context
import typing

@pytest.fixture
def converter():
    return get_default_context().type_converter

PLAIN_STRUCT = pa.struct([
    pa.field("total", pa.int64()),
    pa.field("delta", pa.int64()),
])
```

#### `test_plain_struct_infers_as_dynamic_typeddict`

```
schema = pa.schema([pa.field("result", PLAIN_STRUCT, nullable=False)])
python_schema = converter.arrow_schema_to_python_schema(schema)

result_type = python_schema["result"]
assert typing.is_typeddict(result_type)
assert converter.is_dynamic_typeddict(result_type)
assert not hasattr(result_type, "__dataclass_fields__")   # not a dataclass
assert result_type is not typing.Any
```

#### `test_dynamic_typeddict_roundtrips_to_struct`

```
schema = pa.schema([pa.field("result", PLAIN_STRUCT, nullable=False)])
python_schema = converter.arrow_schema_to_python_schema(schema)

result_type = python_schema["result"]
arrow_type_back = converter.python_type_to_arrow_type(result_type)

assert arrow_type_back == PLAIN_STRUCT
```

#### `test_dynamic_typeddict_write_back`

```
schema = pa.schema([pa.field("result", PLAIN_STRUCT, nullable=False)])
python_schema = converter.arrow_schema_to_python_schema(schema)

data = [{"result": {"total": 8, "delta": 2}}, {"result": {"total": 17, "delta": 3}}]
struct_dicts = converter.python_dicts_to_struct_dicts(data, python_schema=python_schema)
table = pa.Table.from_pylist(struct_dicts, schema=schema)

assert table.num_rows == 2
assert table.schema == schema
rows = table.column("result").to_pylist()
assert rows[0] == {"total": 8, "delta": 2}
assert rows[1] == {"total": 17, "delta": 3}
```

---

## Files Changed

| File | Change |
|---|---|
| `tests/test_core/nodes/test_function_node_dataclass.py` | New — 3 end-to-end pipeline tests |
| `tests/test_extension_types/test_universal_converter_struct.py` | New — 3 converter-level unit tests |

---

## Out of Scope

- `ArrowTableStream.as_table()` — already optimised; not touched.
- Polymorphic dataclass columns (not supported by extension-type encoding).
- Cross-converter-instance TypedDict identity (a single `DataContext` is used per
  session; this edge case is documented above but not tested here).
