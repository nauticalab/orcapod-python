> Historical investigation and November 11 decisions. The November 12 policy in
> `docs/concepts/function-pods.md` supersedes warning conversions. Probe
> `09_warning_dedup.py` measures commit `5537c0a6`, not the current branch.

# ITL-895 root cause: a pod declared `-> int` stores `1.5` as `1`

Investigated on `b197544e` (origin/main, November 11, 2026). Each claim has a script in this
directory, and its captured output is in `outputs/`. Run a script with
`uv run python superpowers/specs/itl-895-float-to-int/<script>.py`. Scripts `01`-`08` are the
pre-fix probes: they show the behaviour **before** the fix, so run them at commit `07037243`.
After the fix, their function-output paths raise `OutputTypeError` instead (input paths are
unchanged). `09_warning_dedup.py` measures the fix's `ValueConversionWarning`; run it on the
current branch.

## TL;DR

1. **Why it happens.** Orcapod never checks a function's return value against its declared
   output type. It hands the raw Python value to pyarrow with the declared Arrow type, through
   `pa.Table.from_pylist(rows, schema=...)`. For an integer field, pyarrow's converter takes
   any object with `__int__`: it tries `__index__` first and then falls back to `__int__`. So
   it calls `float.__int__`, which truncates toward zero. The other mismatches raise because
   pyarrow's converters for those types have explicit checks (details below).
2. **Where.** The truncating call is `universal_converter.py:1106`. The value reaches it
   unchecked through the passthrough converter at `universal_converter.py:1586-1588`. Two
   stream-level `as_table()` builders repeat the same pattern:
   `function_pod.py:1031` and `function_node.py:425`.
3. **Worse than a wrong stored value.** The output `Data` keeps the raw dict, so `as_dict()`
   says 1.5 while `as_table()`, the stored row and `content_hash()` say 1. Downstream pods
   read `as_dict()`. The same graph gives z=15 standalone, z=10 in a sync PipelineJob and
   z=15 in an async PipelineJob.
4. **Since when.** Every release has it: v0.1.0rc1, v0.1.4 and v0.2.0 all store `[1]`. No
   commit ever checked returned values. A checker (`verify_data_schema`) has existed since
   June 2025, but nothing has ever called it.
5. **Input data is affected too.** `DictSource` / `ListSource` with a declared schema,
   `Datagram` / `Data` / `Tag` built from a dict, `Datagram.update` and
   `Datagram.with_columns` all truncate through the same line. Arrow-based sources and the
   operators do not, because they use a safe `Table.cast`.
6. **`None` for a non-optional output** (coordinator follow-up). It is the same gap. A NULL is
   written into an `int64 not null` field. The in-memory store accepts it, and the node can
   no longer be read back. Delta rejects it at flush.

## 1. Call chain, per path (`02_call_chain.py`)

Common prefix on every path:

```
user fn returns 1.5
 -> PythonDataFunction._build_output_data            core/data_function.py:569
      parse_function_outputs -> {"y": 1.5}            (no type check)
      Data({"y": 1.5}, python_schema={"y": int})      core/data_function.py:589
        Datagram keeps _data_dict, _data_table=None   core/datagrams/datagram.py:~120
```

The value is converted only when something asks for the Arrow table (lazy):

```
Data.as_table()                                       core/datagrams/tag_data.py:471
 -> Datagram.as_table()                               core/datagrams/datagram.py:493
 -> Datagram._ensure_data_table()                     core/datagrams/datagram.py:234
 -> UniversalTypeConverter.python_dicts_to_arrow_table
      python_dicts_to_struct_dicts: int converter is
        `lambda value: value` (passthrough)          semantic_types/universal_converter.py:1586-1588
      pa.Table.from_pylist(struct_dicts, schema=int64)  semantic_types/universal_converter.py:1106   <-- TRUNCATES
```

| Path | Who asks for the Arrow table (first truncation) | Second truncation (read path) |
|---|---|---|
| Standalone `FunctionPod` stream, sync fn | `iter_data()` alone does not convert; `data.as_table()` / `content_hash()` do (chain above) | `FunctionPodStream.as_table`: `pa.Table.from_pylist(struct_data, schema=data_schema)` at `core/function_pod.py:1031`, over `as_dict()` values |
| Standalone, `async def` fn | same | same, `function_pod.py:1031` |
| `function_pod(result_database=...)` (`CachedDataFunction`) | `CachedDataFunction.call` (`core/data_function.py:1017`) -> `ResultCache.store` (`core/result_cache.py:253`) -> chain above | `function_pod.py:1031` |
| `function_pod(pod_cache_database=...)` (`CachedFunctionPod`) | `CachedFunctionPod.process_data` (`core/cached_function_pod.py:152`) -> `result_cache.py:253` -> chain | `function_pod.py:1031` |
| `PipelineJob`, sync orchestrator (in-memory and Delta) | `FunctionNode.execute` (`core/nodes/function_node.py:1294`) -> `_process_data_internal` (`:1411`) -> `cached_function_pod.py:152` -> `result_cache.py:253` -> chain | `FunctionNode.as_table`: `pa.Table.from_pylist` at `core/nodes/function_node.py:425` |
| `PipelineJob`, async orchestrator (in-memory and Delta, sync or async fn) | `process_one` (`core/function_pod.py:587`) -> `CachedFunctionPod.async_process_data` (`cached_function_pod.py:203`) -> `result_cache.py:253` -> chain | `function_node.py:425` |

All of these are the same pyarrow call: `pa.Table.from_pylist(rows, schema=<declared Arrow schema>)`.
No `cast(safe=False)` is involved anywhere.

Minimal pyarrow-only reproduction (`01_pyarrow_minimal.py`):

```python
import pyarrow as pa
pa.Table.from_pylist([{"y": 1.5}], schema=pa.schema([("y", pa.int64())]))["y"]   # [1]
pa.array([1.5], type=pa.int64())                                                  # [1]
import pyarrow.compute as pc; pc.cast(pa.array([1.5]), pa.int64())  # raises ArrowInvalid (the safe cast does check)
```

## 2. Why float -> int truncates and the other mismatches raise (`01_pyarrow_minimal.py`)

pyarrow ships its C++ Python-conversion source in the wheel (`pyarrow/src/arrow/python/`).
The behaviour is identical on pyarrow **20.0.0** (the project floor) and **23.0.1** (locked);
see `outputs/01_pyarrow_minimal{,.pyarrow20}.txt`. The only difference in the outputs is the
wording of one decimal error message.

| Declared Arrow type | pyarrow converter (`python_to_arrow.cc` / `helpers.cc`) | Consequence |
|---|---|---|
| any integer | `PyValue::Convert(IntegerType)` -> `CIntFromPython` (helpers.cc:268). It rejects `bool` explicitly ("Expected integer, got bool"), then `CIntFromPythonImpl` (helpers.cc:196). For a non-`PyLong` it calls `PyObjectToPyInt` (helpers.cc:172), which tries `__index__` and **then falls back to `nb_int` (`__int__`)**, before range-checking. | `float`, `np.float64`, `np.float32`, `Decimal` and `Fraction` all truncate (1.5 -> 1, -1.5 -> -1). nan, inf and 1e30 raise (`__int__` fails or the value overflows). `'1'` raises (no `__int__`). 300 for int8 raises (range check). `True` raises (explicit bool check), but `np.bool_` passes (it is not `PyBool`). |
| `bool` | `Convert(BooleanType)`: only `Py_True` / `Py_False` / `np.bool_` | `1` and `1.0` raise |
| `double` | `Convert(DoubleType)`: float or numpy float as-is; an int only via `IntegerScalarToDoubleSafe` (helpers.cc:459, \|v\| <= 2**53) | `1 -> 1.0` and `True -> 1.0` are accepted; `2**53+1` raises; `Decimal` raises |
| `float32` | `Convert(FloatType)`: `static_cast<float>(double)` | `0.1` is silently rounded to 0.10000000149011612 |
| `large_string` / `large_binary` | accept str or bytes and encode/decode | `b'x' -> 'x'` and `'x' -> b'x'` silently; a number raises |
| timestamp / date32 / duration | non-datetime objects fall through to `CIntFromPython` | `1.5 -> 1970-01-01 00:00:00.000001`, `1.5 -> 1970-01-02` (numbers are read as epoch ticks/days, after truncation) |
| any field, nullability | `from_pylist` never checks `nullable=False`; neither does `Table.validate(full=True)` | `None` for a `not null` field is stored as NULL |

`pa.array` / `from_pylist` have no `safe=` switch for Python sequences. `safe` applies only to
numpy/pandas input. The safe/unsafe distinction exists only in `pc.cast` / `Table.cast`, which
orcapod's Arrow-native paths use. That is why those paths raise.

## 3. What a declared output type does with each returned value today (`05_pod_matrix.py`)

Columns: standalone `as_table()` | value stored by a `PipelineJob`. "converted" means an equal
value of a different Python type. "LOSSY" means the stored value differs from the returned one.
"no row stored" means the conversion raised, `FunctionNode.execute` logged it as a row failure,
and the run continued.

| declared | returned | standalone | PipelineJob |
|---|---|---|---|
| int | 1.5 / np.float64(1.5) | LOSSY 1 | LOSSY 1 |
| int | Decimal('1.5') | raises (inference) | LOSSY 1 |
| np.int8 | 1.5 | LOSSY 1 | LOSSY 1 |
| int \| None | 1.5 | LOSSY 1 | LOSSY 1 |
| list[int] | [1.5] | LOSSY [1] | LOSSY [1] |
| date | 1.5 | LOSSY 1970-01-02 | LOSSY 1970-01-02 |
| date | datetime(2020,1,1,12) | raises | LOSSY date(2020,1,1) (time dropped) |
| datetime | 1.5 | LOSSY 1970-01-01T00:00:00.000001 | row failure |
| str / bytes | b'x' / 'x' | LOSSY (decoded / encoded) | LOSSY |
| int | 2.0, np.int64(3), np.bool_(True) | converted | converted |
| float | 1, True, np.float32(0.1) | converted | converted |
| bool | np.bool_(True) | converted | converted |
| list[int] | (1, 2) | converted to [1, 2] | converted |
| int / float | None | NULL in a `not null` field | see section 6 |
| int | True, '1', 2**63 | raises | row failure |
| np.int8 | 300 | raises | row failure |
| float | 2**53+1, Decimal, '1.0' | raises | row failure |
| bool | 1, 1.0 | raises | row failure |
| str | 1 | raises | row failure |
| datetime | naive, date | raises | row failure |

The issue's second observation (the standalone `as_table()` "Complex unions ... int | float"
error) has a separate cause, shown at the end of `05`. `FunctionPodStream.as_table` calls
`python_dicts_to_struct_dicts(all_data)` **without** the declared schema
(`function_pod.py:1024`), so the schema is inferred from the values. When rows mix 1 and 2.5,
inference produces a union. `FunctionNode.as_table` passes the declared schema
(`function_node.py:415`).

## 4. Consequences beyond the stored value (`03_split_datagram.py`, `07_blast_radius.py`)

- One `Data`, two answers: `as_dict()` gives `{'y': 1.5}`; `as_table()` gives `[{'y': 1}]`.
  `content_hash()` is identical to that of `Data({'y': 1})`.
- Downstream pods read `as_dict()`. In the graph `down(y: int) -> int: return y * 10` over
  `up`, z is **15** standalone, **10** on the sync orchestrator and **15** on the async
  orchestrator. The sync and async results differ.
- In a standalone chain the upstream `Data` is never converted before the downstream function
  runs (0 conversions at call time). The downstream also returns 15.0 for its `-> int` and is
  truncated in turn.

## 5. Is input data affected? (`04_input_sites.py`)

| Site | Truncates? | Where |
|---|---|---|
| `DictSource(data_schema={...: int})`, data column | **yes** | `dict_source.py:47` -> `universal_converter.py:1106` |
| `DictSource(data_schema=pa.schema(...int64))` | **yes** | `dict_source.py:42` -> `:1106` |
| `DictSource`, **tag** column declared int | **yes** | same |
| `DictSource`, no schema, mixed 1 / 1.5 | no; raises "Complex unions" (inference) | `infer_python_schema_from_pylist_data` |
| `ListSource(data_schema={...: int})` | **yes** | `list_source.py:75` -> `:1106` |
| `Datagram` / `Data` / `Tag` built from a dict with `python_schema` | **yes** | `datagram.py:234` -> `:1106` |
| `Datagram.update(x=1.5)` on an int column (Arrow-backed or dict-backed) | **yes** | `datagram.py:812` / `:234` -> `:1106` |
| `Datagram.with_columns(column_types={'z': int or pa.int64()}, z=1.5)` | **yes** | `datagram.py:894` / `:234` -> `:1106` |
| `DataFrameSource(schema=int64)` over a float column | no; raises (safe `Table.cast`) | `data_frame_source.py:65` |
| `ArrowTableSource`, Delta / CSV / DB sources | no (Arrow in; `Table.cast` is safe) | |
| Source info | no (its type is derived from the value, never declared) | `tag_data.py:381` |
| Operators (`Batch`, `GroupBy`, `PolarsFilter`, joins, ...) | no (Arrow in, Arrow out; `build_aggregated_table` rows come from Arrow values of the same type) | `arrow_utils.py:1540` |
| `DictSource(data_schema={'x': int})` with `x=None` | NULL in an `int64 not null` field (the input-side form of section 6) | `:1106` |

## 6. `None` for a non-optional output (`08_none_output.py`)

`-> int` maps to `int64 not null`. `from_pylist` writes the NULL, and `validate(full=True)` passes.

- Standalone `as_table()`: returns `y=[None]` in a `not null` field.
- In-memory store, sync and async: **the run completes**, and the row is stored. Then
  `get_all_records()` / `as_table()` on the node raise `ValueError: Casting field 'y' with null
  values to non-nullable` (`arrow_utils.restore_schema_nullability`, `arrow_utils.py:1230`).
  The node is unreadable.
- Delta store, sync and async: the Delta writer refuses the batch at flush. On main this is
  logged as a row failure, and the final flush raises `RuntimeError`. PR #287 turns that into
  `StoreWriteError`, which stops the run. Per the ITL-878 decision, the developer would want
  this case to be a row failure, which it only becomes if it is rejected before the write.

## 7. Existing validation and why it misses this

- `PythonDataFunction._build_output_data` checks only the number of outputs
  (`parse_function_outputs`). PR #289 adds `_check_declared_ndarray_outputs` at the same spot,
  but only for ndarray-declared outputs.
- `Datagram.__init__` merges the declared schema over the inferred one
  (`python_schema.get(k, v)`). The declared type wins; nothing compares it to the value.
- `FunctionPod._validate_input_schema` (`function_pod.py:235`) compares **schemas** (beartype
  `is_subhint`) when the graph is built, never values.
- `schema_utils.verify_data_schema` (`schema_utils.py:14`) checks values with beartype
  `is_bearable`. It has existed since `27c616bb` (2025-06-13, as `verify_against_typespec`),
  but **no call site has ever existed** (git log -S). It would not be a sound check anyway:
  `is_bearable(True, int)` is True, and beartype checks one sampled element of a container,
  not every element.

## 8. When it was introduced (`06_released_versions.py`)

- Behaviour: v0.1.0rc1, v0.1.4 and v0.2.0 each built from their tag; all three print
  `y=[1] type=int64` (`outputs/06_released_versions.releases.txt`).
- It was never correct. The design dates from `6e2bdd7d` (2025-07-12, "implement pure
  immutable datagram": a dict-backed datagram converted lazily against a declared schema),
  together with the semantic converter's `from_pylist(..., schema=...)` (`83189f9e`,
  2025-08-05). Nothing has ever validated returned values.

## 9. Blast radius for existing stores (`07_blast_radius.py`)

- A truncated row is indistinguishable from a genuine one. The stored value,
  `__output_data_hash` and the record are those of the truncated value. Nothing records the
  original.
- Later runs serve the stored row **without calling the function** (`calls=0` on the second
  run). So no fix changes or flags already-stored rows. They keep being served until the user
  removes them or changes the pod's version. Per the standing rules, the fix must not repair
  or recompute them automatically.
- How a user can find affected rows: pick the pods whose declared outputs contain a type
  pyarrow coerces (integers, `date` / `datetime`, `str` / `bytes`, `np.float32`, lists or
  structs of these). Call the raw function on the node's stored inputs (the upstream node's
  `iter_data()`), and flag rows whose returned value is not exactly the declared type as
  candidates. Some candidates were stored correctly (a same-kind NumPy scalar, for example),
  so review each candidate's stored value and its conversion before deciding whether
  information was lost. `07` does this and finds exactly the two odd-input rows. This method
  needs a function that reproduces its historical outputs: if the function depends on time,
  randomness or external state it can now return a conforming value for an input whose
  stored row was truncated, so the check cannot reliably find every affected row. For `None` outputs on an in-memory store,
  the node fails to read at all; Delta never stored them.
- Downstream rows computed from such a value can also be wrong (z=15 vs z=10 above). They
  need the same check one level down.
- **Hashes of correct data.** For a value that already conforms to its declared type, the fix
  only adds a check. The Arrow bytes and every hash stay the same. Baseline: the golden suite
  passes (644) on this branch, and Step 2 must leave it unchanged. Rows that a stricter rule
  would reject (for example `np.int64` for `int`, or `1` for `float`) are stored with correct
  values today. A stricter rule changes no hash for them. It makes **future** computations of
  such rows into row failures, which is a behaviour change, not a hash change.

## 10. Fix options

The options differ in **where** a value is checked. **What** counts as conforming (section 11)
is a separate decision that applies to all of them.

### (a) Reject at the pod

The check goes in `PythonDataFunction._build_output_data` (`data_function.py:569`), the spot
where PR #289 adds the ndarray check. It generalizes that check to every declared output
type. It raises a new error (for example `OutputTypeError(TypeError)` in `orcapod.errors`)
naming the pod, the output key, the declared type and the value's type.

- It runs inside `call` / `async_call`, so it is a **function failure** on every path. In a
  PipelineJob, the existing handlers (`function_node.py:1294-1305`, async `function_pod.py:590`)
  apply `error_policy` and `observer.on_data_crash`. In a standalone stream, it raises from
  `iter_data()` like any function error. This matches ITL-878.
- The bad `Data` never exists, so there is no as_dict / as_table split and downstream never
  sees the value. It fixes the sync/async divergence.
- It does not protect input data: DictSource / ListSource with a declared schema,
  Datagram.update / with_columns and Tag still truncate.
- It needs its own value-against-type checker over orcapod's type grammar (primitives,
  numpy widths, `list` / `dict` / struct, `Optional`, logical types). That is a second
  definition of "conforms", next to the converter's, and the two can drift apart.

### (b) Make the Python -> Arrow conversion strict

The passthrough at `universal_converter.py:1586-1588` is replaced by strict per-type
converters. They raise a column-named error, reusing PR #289's `UnsupportedValueError`, which
`python_dicts_to_struct_dicts` already prefixes with the column name. `FunctionPodStream.as_table`
(`function_pod.py:1024`) must also pass the declared schema instead of inferring it, which
fixes the "Complex unions" message as well.

- Every caller is protected: function outputs, all the input sites in section 5, and tags.
- In a PipelineJob the error is raised at `result_cache.py:253` (record preparation, before
  the store write and outside PR #287's `StoreWriteError` wrapping), inside the per-row
  handler. So it is already a **row failure** under `error_policy`, logged as
  "Data execution failed in <node>: Column 'y': ...". It does not name the declared type and
  value type unless the converter puts them in the message.
- It does **not** fix the split `Data`. A standalone chain never converts the upstream `Data`,
  so downstream silently computes from 1.5 (z=15) until something calls `as_table()` /
  `content_hash()`. A standalone `iter_data()` reports nothing.

### (c) Both, built on one rule (recommended for the developer to consider)

The strict converter from (b) is the single definition of "conforms". `_build_output_data`
then converts the output `Data` to Arrow eagerly through it. It re-raises any failure as the
pod-level error from (a), naming the pod, column, declared type and value type. The `Data` is
born Arrow-backed and validated, so `as_dict()` equals what is stored and hashed.

- It combines the guarantees of (a) and (b) without a second checker.
- Costs: one eager conversion per output row. This is already paid on every store path, and
  it is new only for uncached standalone streams. Standalone downstream pods then see the
  Arrow round-tripped value rather than the raw object. For conforming values the two are
  equal. For logical types they are the converter's reconstructed objects, as they already
  are on every store path.

## 11. Proposed conformance rule (decisions needed whichever option is chosen)

Default from the type-strict rule: the value's type must be exactly the declared type. No
widening and no coercion. Items D1-D7 need the developer's call. Each says what happens today.

| # | Case | Today | Type-strict default |
|---|---|---|---|
| — | float, `np.float*`, `Decimal`, `Fraction` for an integer type | truncated | **reject** |
| — | integer out of range for the declared width | raises | reject (unchanged) |
| — | `bool` for int; int or float for bool | raises | reject (unchanged) |
| — | a number for date / datetime / duration | read as epoch ticks | **reject** |
| — | `bytes` for str / `str` for bytes | encoded / decoded | **reject** |
| D1 | `int` for a declared `float` (e.g. `return 0`) | accepted if \|v\| <= 2**53 | reject? Python typing's numeric tower allows it, and likely some existing user code relies on it; Step 2 will measure how much of our own tests and docs depends on it |
| D2 | numpy scalars of the same kind (`np.int64` for int, `np.bool_` for bool; `np.float64` is already a `float` subclass) | accepted, value-exact | accept same-kind value-exact, or reject all non-builtin scalars |
| D3 | `True` for a declared `float` | accepted as 1.0 | reject (int-for-bool is already rejected) |
| D4 | `None` for a non-optional output | NULL in a not-null field (section 6) | **reject** (coordinator: row failure) |
| D5 | `tuple` (or other sequence) for `list[T]` | converted to a list | reject, or accept a tuple only |
| D6 | `datetime` for a declared `date` (a `date` subclass) | time silently dropped on the store path | reject |
| D7 | Python `float` for a declared `np.float32` | rounded to float32 | accept (rounding is inherent to declaring float32), or require `np.float32` |
| — | aware non-UTC `datetime` for `datetime` | normalized to the UTC instant (the documented timezone policy) | keep |
| — | logical types (Path, UUID, ndarray, dataclass, pydantic) | the logical type's own `python_to_storage` | `isinstance` of the declared class (as PR #289 does for ndarray) |

## Out-of-scope findings (reported, not fixed)

1. `InMemoryArrowDatabase` accepts a NULL in a `not null` field, which leaves the node
   unreadable (section 6). Delta rejects the same write.
2. The `DictSource` / `ListSource` / `Datagram` input-side truncation and NULLs (section 5)
   are covered only if option (b) or (c) is chosen.
3. `schema_utils.verify_data_schema` is dead code and unsound as a value check (section 7).
4. Noticed while reading, not probed: `PollingSource` with a declared schema appears to apply
   only the declared **nullability** to the polled frame, not the declared types
   (`polling_source.py:653-696`).
