# ITL-578 Polars DataFrame Logical Type Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Register `polars.DataFrame` as an Orcapod logical type so a function pod can take and
return a `pl.DataFrame`, and the value is stored, read back, hashed and cached.

**Architecture:** A new `LogicalPolarsDataFrame` (extension name `polars.dataframe`, storage
`large_binary`) stores the frame as Arrow IPC stream bytes, the same envelope as
`LogicalPandasDataFrame`. The IPC helpers move from `pandas_type.py` to a shared
`logical_types/arrow_ipc.py` that both types import. The type is wired into the default data
context (`contexts/data/v0.1.json`) like pandas and numpy. Hashing goes through a semantic
handler whose design is **pending a developer decision** (Task 6).

**Tech Stack:** polars 1.41.2, pyarrow 23.0.1, starfix 0.4.0, pytest.

---

## Findings that shape the plan (investigation, November 10, 2026)

Scratch scripts live in the session scratchpad; the numbers below come from them.

1. **Storage round trip is exact** with `df.to_arrow(compat_level=pl.CompatLevel.oldest())`,
   `combine_chunks()`, Arrow IPC stream, then `pl.from_arrow(...)`. Every dtype checked
   returns with an identical Polars schema and `assert_frame_equal` passes: Int8–Int64,
   UInt8–UInt64, Float16/32/64, String, Binary, Boolean, Categorical, Enum (category order kept,
   via the `_PL_ENUM_VALUES2` field metadata), Decimal(p, s), Datetime (unit and time zone),
   Date, Time, Duration (unit), List, Array (also 2-D), Struct (also with a nested Enum), Null,
   an empty frame with a schema, and a zero-column frame.
   `compat_level=oldest` is pinned so strings are written as `large_string`, never
   `string_view`, whatever the Polars default becomes.
2. **Not round-trippable, so rejected at write time:**
   - `Int128` / `UInt128`: `to_arrow` raises `ArrowInvalid: unsupported format string '_pli128'`.
   - `Object`: `to_arrow` silently writes `fixed_size_binary[8]` (pointer bytes) and reads back
     as `Binary`, which is silent corruption.
   - Polars extension dtypes (`pl.BaseExtension`, e.g. an Orcapod `Extension('numpy.ndarray')`
     column inside the frame): nested logical values inside a frame value are a separate design
     question; reject now, report as a follow-up.
   The check walks the dtype tree (List, Array, Struct children) so a nested `Object` is also
   caught, and the error names the top-level column and the offending dtype.
3. **The IPC bytes are not canonical.** Equal frames give different bytes when the chunk layout
   differs (fixable with `combine_chunks`) and when the bytes under null slots differ (not
   fixable without rebuilding buffers). The bytes also carry Polars-version-specific field
   metadata (`_PL_CATEGORICAL2`, `_PL_ENUM_VALUES2`). With no handler registered, the semantic
   visitor hashes the stored bytes, so a handler is required.
4. **Starfix alone is not enough for a frame value.** `StarfixArrowHasher.hash_table` (what
   `PandasDataFrameHandler` uses) is chunk-independent but, by starfix design, ignores column
   order and struct field order and hashes a dictionary column as its value type
   (Categorical == Enum(any categories) == String). It raises `NotImplementedError` for
   `fixed_size_list` (Polars Array) and `null`. The existing pandas handler has the same
   collisions on main (reported to the coordinator, out of scope here).

The hash design (option A, B or C in the coordinator question of November 10, 2026) is held
for the developer. Task 6 is blocked until that answer arrives; everything else proceeds.

## File structure

| File | Responsibility |
|---|---|
| Create `src/orcapod/logical_types/arrow_ipc.py` | `table_to_ipc_bytes`, `ipc_bytes_to_table` (moved from `pandas_type.py`, unchanged bodies) |
| Modify `src/orcapod/logical_types/pandas_type.py` | Import the helpers from `arrow_ipc` |
| Create `src/orcapod/logical_types/polars_type.py` | `LogicalPolarsDataFrame`, `check_polars_schema_storable` |
| Modify `src/orcapod/logical_types/__init__.py` | Export `LogicalPolarsDataFrame` |
| Modify `src/orcapod/contexts/data/v0.1.json` | Register the logical type (+ handler after Task 6), changelog line |
| Modify `src/orcapod/hashing/semantic_hashing/builtin_handlers.py` | `PolarsDataFrameHandler` (Task 6, design pending) |
| Create `tests/test_logical_types/test_polars_type.py` | Unit tests: protocol, per-dtype round trip, rejections |
| Modify `tests/test_logical_types/test_roundtrips.py` | Builtin round trip over Parquet and Delta (mirrors pandas) |
| Create `tests/test_core/function_pod/test_polars_dataframe_values.py` | Public-interface integration tests |
| Create `tests/test_hashing/test_polars_handlers.py` | Hash properties (Task 6) |
| Modify `docs/concepts/function-pods.md` | "Value types" section listing built-in logical types incl. `pl.DataFrame` |
| Modify `DESIGN_ISSUES.md` | One appended entry for ITL-578 |

No docs page lists pandas or numpy today (the brief assumed one). The plan adds a short
"Value types" section to `docs/concepts/function-pods.md` and reports this to the coordinator.

---

### Task 1: Move the IPC helpers to a shared module (refactor, no behavior change)

**Files:** Create `src/orcapod/logical_types/arrow_ipc.py`; modify `pandas_type.py`.

- [ ] **Step 1:** Create `arrow_ipc.py` with `table_to_ipc_bytes(table: pa.Table) -> bytes` and
  `ipc_bytes_to_table(data: Any) -> pa.Table`, bodies copied verbatim from
  `pandas_type._table_to_ipc_bytes` / `_ipc_bytes_to_table`, Google docstrings.
- [ ] **Step 2:** In `pandas_type.py`, delete the two private functions and
  `from orcapod.logical_types.arrow_ipc import ipc_bytes_to_table, table_to_ipc_bytes`;
  update the four call sites.
- [ ] **Step 3:** Run `uv run pytest tests/test_logical_types/test_pandas_type.py tests/test_hashing/test_pandas_handlers.py -q` — expect all pass.
- [ ] **Step 4:** Commit `refactor(logical_types): share the Arrow IPC helpers between frame types`.

### Task 2: Failing regression test for the decoration error

**Files:** Create `tests/test_core/function_pod/test_polars_dataframe_values.py`.

- [ ] **Step 1:** Write

```python
import polars as pl
from polars.testing import assert_frame_equal

from orcapod import function_pod  # verify the public import path when writing


def test_pod_can_return_a_polars_dataframe():
    @function_pod(output_keys="df")
    def make_frame(n: int) -> pl.DataFrame:
        return pl.DataFrame({"i": list(range(n))})

    assert make_frame is not None
```

- [ ] **Step 2:** Run it; expect `TypeError: No LogicalType or LogicalTypeFactory registered for <class 'polars.dataframe.frame.DataFrame'>`.

### Task 3: `LogicalPolarsDataFrame` storage (TDD)

**Files:** Create `src/orcapod/logical_types/polars_type.py`,
`tests/test_logical_types/test_polars_type.py`.

- [ ] **Step 1: Tests first.** In `test_polars_type.py`:
  - protocol: `isinstance(LogicalPolarsDataFrame(), LogicalTypeProtocol)`,
    `logical_type_name == "polars.dataframe"`, `python_type is pl.DataFrame`,
    Arrow extension name `polars.dataframe`, storage `pa.large_binary()`, Polars extension name.
  - `python_to_storage` returns `bytes`.
  - Parametrized round trip, one column per case, asserting
    `back.schema == df.schema` **and** `assert_frame_equal(back, df)`: Int8 with null, UInt8,
    UInt16, UInt32, UInt64, Float16, Float32, String with null, Binary with null, Boolean,
    Categorical, Enum(["y","x","z"]), Decimal(10,2) and Decimal(38,2), Datetime("ms",
    "America/New_York"), Datetime("ns"), Date, Time, Duration("ms"), List(Int64) with null,
    List(String) with empty list, List(Categorical), Array(Int16, 2), Array(Int16, (1, 2)),
    Struct({"a": Int64, "b": String}) with null, Struct({"e": Enum}), Null.
  - A wide frame of all the above in one frame: column order and names preserved.
  - Empty frame with schema (Int8 + Struct(Enum)): schema and zero height preserved.
  - Zero-column frame `pl.DataFrame()`.
  - Chunked frame (`pl.concat([...], rechunk=False)`, assert `n_chunks() == 2`) round trips.
  - Rejections (`pytest.raises(ValueError, match=...)` matching column name and dtype):
    `Object` column, `Int128`, `UInt128`, `List(Object)` nested, `Struct({"o": Object})`
    nested, a Polars extension dtype column (`pl.Series([b"x"]).ext.to(LogicalNumpyArray().get_polars_extension_type())`).
  - Non-DataFrame input (`pl.Series`, `pd.DataFrame`) raises `TypeError`.
- [ ] **Step 2:** Run `uv run pytest tests/test_logical_types/test_polars_type.py -q`; expect import failure.
- [ ] **Step 3: Implement** `polars_type.py`:

```python
_UNSUPPORTED_LEAVES = (pl.Object, pl.Int128, pl.UInt128)

def _find_unstorable_dtype(dtype: pl.DataType) -> pl.DataType | None:
    """Return the first nested dtype that cannot round-trip, or None."""
    if isinstance(dtype, pl.BaseExtension):
        return dtype
    if isinstance(dtype, _UNSUPPORTED_LEAVES) or dtype in _UNSUPPORTED_LEAVES:
        return dtype
    if isinstance(dtype, (pl.List, pl.Array)):
        return _find_unstorable_dtype(dtype.inner)
    if isinstance(dtype, pl.Struct):
        for field in dtype.fields:
            found = _find_unstorable_dtype(field.dtype)
            if found is not None:
                return found
    return None

def check_polars_schema_storable(schema: pl.Schema) -> None:
    """Raise ValueError naming the column and dtype that cannot round-trip exactly."""
    for name, dtype in schema.items():
        bad = _find_unstorable_dtype(dtype)
        if bad is not None:
            raise ValueError(
                f"LogicalPolarsDataFrame: column {name!r} has dtype {dtype} "
                f"(contains {bad}), which cannot be stored losslessly as Arrow IPC. "
                "Supported: integer up to 64 bits, float, string, binary, boolean, "
                "categorical, enum, decimal, date, time, datetime, duration, null, and "
                "list/array/struct of those."
            )

class LogicalPolarsDataFrame(BaseLogicalType):
    # extension name "polars.dataframe": library-qualified like "pandas.dataframe" and
    # "numpy.ndarray"; names the Python type users write, so stored schemas self-document.
    ...
    def python_to_storage(self, value, converter=None) -> bytes:
        if not isinstance(value, pl.DataFrame):
            raise TypeError(...)
        check_polars_schema_storable(value.schema)
        table = value.to_arrow(compat_level=pl.CompatLevel.oldest()).combine_chunks()
        return table_to_ipc_bytes(table)

    def storage_to_python(self, storage_value, converter=None) -> pl.DataFrame:
        frame = pl.from_arrow(ipc_bytes_to_table(storage_value))
        assert isinstance(frame, pl.DataFrame)  # a Table always gives a DataFrame
        return frame
```

  (Extension class caching mirrors `LogicalPandasDataFrame` exactly.)
- [ ] **Step 4:** Run the unit tests; expect pass.
- [ ] **Step 5:** Commit `feat(logical_types): add polars.DataFrame logical type with Arrow IPC storage`.

### Task 4: Register in the default data context

**Files:** `contexts/data/v0.1.json`, `logical_types/__init__.py`.

- [ ] **Step 1:** Before editing, record reference hashes with a scratch script (pipeline hash
  and content hash of a small pod pipeline over `int`, `pd.DataFrame`, `np.ndarray` values, and
  `get_default_context().semantic_hasher.hash_object(...)` of a few objects) to
  `scratchpad/hashes_before.json`.
- [ ] **Step 2:** Add `{"_class": "orcapod.logical_types.polars_type.LogicalPolarsDataFrame", "_config": {}}`
  after `LogicalPandasSeries`; append a changelog line; export from `__init__.py`.
- [ ] **Step 3:** Run Task 2's test; expect pass. Re-run the hash script; assert identical
  to `hashes_before.json` (acceptance: existing hashes unchanged). Run
  `uv run pytest tests/test_hashing tests/test_logical_types -q`.
- [ ] **Step 4:** Add the builtin round trip to `test_roundtrips.py` (Parquet + Delta), modelled
  on `test_builtin_dataframe_round_trip`, with a frame holding Enum, Datetime(tz) and Struct.
- [ ] **Step 5:** Commit `feat(contexts): register polars.DataFrame in the default data context`.

### Task 5: Public-interface integration tests (storage paths)

**File:** `tests/test_core/function_pod/test_polars_dataframe_values.py`.
Every test drives pods/operators by calling them and reads results through
`iter_data()` / `as_table()` / node `get_all_records()`, then converts through the public
stream API. Frames are compared with `assert_frame_equal` **and** schema equality.

- [ ] Pod output: `n: int -> pl.DataFrame` over a 3-row source; each emitted value is a
  `pl.DataFrame` equal to the expected frame (Enum + Datetime(tz) + Struct + List columns).
- [ ] Pod input: source column of frames (built via `DictSource`/`ListSource` with an explicit
  `pl.DataFrame` schema type) → `f(df: pl.DataFrame) -> int` returns `df.height`.
- [ ] Chained pods: produce → transform (`df.with_columns(...)`) → consume.
- [ ] `Batch` and `GroupBy` over a frame-valued data column: result column type is the logical
  list type (`list[polars.dataframe]`), every member frame equal and in documented order.
- [ ] `Join` pass-through: frame column joined against another stream on the tag; frame unchanged.
- [ ] `PipelineJob` with `SyncPipelineOrchestrator` and `AsyncPipelineOrchestrator`, each over
  `InMemoryArrowDatabase`, `DeltaTableDatabase(tmp_path)` and
  `ConnectorArrowDatabase(SQLiteConnector(tmp_path / "db.sqlite"))`: records read back from the
  node equal the produced frames.
- [ ] Cross-process cache hit: run a pipeline over a Delta store in a subprocess
  (`sys.executable -c`, pattern from `tests/test_hashing/test_cross_process_stability.py`),
  then in a second subprocess re-run it; the second run computes nothing (function call
  counter via a marker file stays at the first-run count) and the frames read back are equal.
  This test also needs Task 6 (the cache key is the hash).
- [ ] Commit `test(function_pod): polars.DataFrame values through pods, operators and databases`.

### Task 6: Hash handler — decided by the developer (November 10, 2026)

Decision: option A with corrections. Column order and struct field order are **not** part of
identity (starfix is deliberately order independent and Orcapod relies on it). Enum category
order **is**. The dtype descriptor is Orcapod's own versioned grammar, never Polars' repr or
`_PL_*` metadata. Values are hashed with starfix `hash_table` over a hash-only projection
(Array cast to `large_list`, top-level Null columns left out, row count in the descriptor).
`pl.Series` is approved as `polars.series` with the same scheme (name + dtype + values). The
pandas handler and every existing hash stay unchanged.

Mechanism: a semantic handler per Python type (`PolarsDataFrameHandler`,
`PolarsSeriesHandler`) registered in `v0.1.json`. The column-level `ARROW:extension:*`
metadata route cannot carry a per-value dtype, so the handler route is the only one that
applies to every `polars.dataframe` value wherever it is hashed. The descriptor and hash
functions live in `polars_type.py`, next to the storage format they describe; the handlers
delegate to them.

Grammar (canonical JSON, `sort_keys`, compact separators, UTF-8):
`{"v": 1, "kind": "polars.dataframe", "height": n, "columns": {name: dtype}}`, and for a
Series `{"v": 1, "kind": "polars.series", "name": s, "height": n, "dtype": dtype}`, where
`dtype` is one of `"int8"`…`"uint64"`, `"float16"`, `"float32"`, `"float64"`, `"boolean"`,
`"string"`, `"binary"`, `"date"`, `"time"`, `"null"`, `"categorical"`,
`{"enum": [categories in order]}`, `{"decimal": [precision, scale]}`,
`{"datetime": [unit, time_zone or null]}`, `{"duration": unit}`, `{"list": dtype}`,
`{"array": [dtype, size]}` (multi-dimensional arrays nest), `{"struct": {field: dtype}}`.
Anything else raises `ValueError` naming the column and dtype. Nested Null (inside List,
Array or Struct) cannot be left out without changing list offsets, so the projection casts it
to an all-null `bool` for hashing only; the descriptor still records `"null"`.
Digest: `sha256(descriptor_bytes + b"\x00" + starfix_digest)`, method `"sha256"`.

Tests to write first:
`tests/test_hashing/test_polars_handlers.py`

- equal frames hash identically regardless of chunk layout and of null-slot payload
  (`[1, None, 3]` vs `[1, 2, 3]` nulled by `when/then`);
- differ in a value, a dtype (Int64 vs Int32; String vs Categorical vs Enum(a,b) vs
  Enum(a,b,c)), a column name, column order, struct field order → different hashes;
- `Array` and `Null` columns hash without error;
- a frame read back from storage hashes the same as the original;
- cross-process stability: same hash in two interpreters with different `PYTHONHASHSEED`;
- a golden hex digest for one fixed frame, pinned in the test, to detect future drift.

Then register `PolarsDataFrameHandler` in `v0.1.json` under `{"_type": "polars.DataFrame"}`
and in `register_builtin_python_type_handlers`; re-run the Task 4 hash comparison.

### Task 7: Docs and design log

- [ ] Add a "Value types" section to `docs/concepts/function-pods.md`: a table of the built-in
  logical types (Python type, extension name, storage), including `polars.DataFrame`, the
  rejected dtypes, and an executed sample of a pod returning a `pl.DataFrame` with its printed
  output in `# ...` comments.
- [ ] `uv run pytest tests/test_docs -q` (doc samples + `mkdocs build --strict`).
- [ ] Append one DESIGN_ISSUES.md entry (ITL-578) recording the storage format, rejected
  dtypes and the hash design.
- [ ] Commit `docs: document polars.DataFrame as a function pod value type`.

### Task 8: Finish

- [ ] `uv run --with ruff ruff format . && uv run --with ruff ruff check .`,
  `uv run --with basedpyright basedpyright` (exit code 0), `uv run pytest tests/`.
- [ ] Merge `origin/main`, re-run the suite if it moved, push, open PR (`Fixes ITL-578`),
  `orca linear attach`, `orca worktree set --worktree current --pr <n>`, worker_done.

Out of scope, reported in worker_done: `pl.LazyFrame`; `pl.Series` (held with the hash
decision); the pandas handler's column-order / dtype collisions; nested logical (extension)
dtypes inside a frame.
