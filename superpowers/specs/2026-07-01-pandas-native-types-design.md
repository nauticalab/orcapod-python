# Design: pandas.DataFrame and pandas.Series as Native Value Types

**Date:** 2026-07-01
**Issue:** PLT-1869
**Status:** Approved

## Overview

orcapod's `UniversalTypeConverter` raises `ValueError: Unsupported Python type` when a
`pd.DataFrame` or `pd.Series` appears as a column value type. This forces users to
round-trip frames through `.parquet`/`.csv` + `op.File` instead of tracking them as
first-class values. This spec adds `LogicalPandasDataFrame` and `LogicalPandasSeries`
as native logical types, along with matching content-hashing handlers.

## Goals & Success Criteria

- `pd.DataFrame` and `pd.Series` are accepted by `UniversalTypeConverter` without raising.
- Both types round-trip losslessly for all Arrow-compatible dtypes (numeric, string/object
  columns, nullable integers, datetime, boolean, categorical).
- The DataFrame index — including named indices and MultiIndex — is preserved on round-trip.
- The Series name and index are preserved on round-trip.
- Content hashing (`PandasDataFrameHandler`, `PandasSeriesHandler`) produces a stable
  `ContentHash` for each value, consistent with the Arrow IPC serialisation used for storage.
- Both types are registered in the default data context (`v0.1.json`) and work end-to-end
  without any user configuration.
- `dict[str, pd.DataFrame]` (and other generic wrappers) work automatically once
  `pd.DataFrame` is registered as a known type.

## Scope & Boundaries

In scope:
- `LogicalPandasDataFrame` — `pd.DataFrame` ↔ Arrow `large_binary` via IPC stream.
- `LogicalPandasSeries` — `pd.Series` ↔ Arrow `large_binary` via IPC stream.
- `PandasDataFrameHandler` and `PandasSeriesHandler` for semantic content hashing.
- Registration in `contexts/data/v0.1.json`.
- Full test coverage mirroring `test_numpy_type.py`.

Out of scope:
- `pd.Index` or `pd.MultiIndex` as standalone value types.
- Configurable index handling (always `preserve_index=True`).
- Polars DataFrame/Series support.
- Compression of IPC bytes (not needed for in-memory `large_binary` storage).

## Architecture

### New file: `src/orcapod/extension_types/pandas_type.py`

Contains `LogicalPandasDataFrame` and `LogicalPandasSeries`, both subclassing
`BaseLogicalType`, following the exact structure of `numpy_type.py`.

### Modified files

| File | Change |
|------|--------|
| `src/orcapod/hashing/semantic_hashing/builtin_handlers.py` | Add `PandasDataFrameHandler` and `PandasSeriesHandler` |
| `src/orcapod/contexts/data/v0.1.json` | Register both logical types and both handlers |

### New test file: `tests/test_extension_types/test_pandas_type.py`

## Logical Types

### `LogicalPandasDataFrame`

```
logical_type_name = "pandas.dataframe"
python_type       = pd.DataFrame
arrow storage     = pa.large_binary()
```

**`python_to_storage(df, converter=None) → bytes`**

1. `table = pa.Table.from_pandas(df, preserve_index=True)`
2. Serialise `table` to IPC stream bytes via `pa.ipc.new_stream()`.
3. If `pa.lib.ArrowInvalid` is raised (e.g., column with mixed Python objects), re-raise
   as `ValueError` with a descriptive message listing the column and its dtype.

**`storage_to_python(buf, converter=None) → pd.DataFrame`**

1. Deserialise IPC bytes via `pa.ipc.open_stream()` → Arrow Table.
2. Return `table.to_pandas()`. The index is restored automatically from Arrow metadata.

**Extension naming:** `"pandas.dataframe"` — library-qualified, following the
`"numpy.ndarray"` precedent over the `"orcapod.*"` namespace. This makes schemas
self-documenting and leaves the orcapod namespace for orcapod-owned abstractions.

### `LogicalPandasSeries`

```
logical_type_name = "pandas.series"
python_type       = pd.Series
arrow storage     = pa.large_binary()
```

**`python_to_storage(s, converter=None) → bytes`**

1. If `s.name == "__orcapod:unnamed__"` raise `ValueError` — the sentinel is reserved.
2. Wrap the Series as a single-column DataFrame: use `s.name` as the column name when it
   is not `None`, otherwise use the internal sentinel `"__orcapod:unnamed__"`.
   The index is preserved.
3. Apply the same IPC stream serialisation as `LogicalPandasDataFrame`.

**`storage_to_python(buf, converter=None) → pd.Series`**

1. Deserialise IPC bytes → Arrow Table → `table.to_pandas()`.
2. Extract the single column: `series = df.iloc[:, 0]`. Restore the series name:
   if the column name equals the sentinel `"__orcapod:unnamed__"`, set `series.name = None`.

## Hashing Handlers

### `PandasDataFrameHandler`

Converts the DataFrame to a `pa.Table` (with `preserve_index=True`) and delegates
to the configured Arrow hasher (default: `StarfixArrowHasher`, which produces
`ContentHash(method="arrow_v0.1", ...)`). This keeps `pd.DataFrame` hashing
consistent with `pa.Table` hashing throughout orcapod: a DataFrame with identical
content and index produces the same hash as the equivalent Arrow table. Like
`ArrowTableHandler`, the arrow hasher is resolved lazily via `get_default_context()`
when none is provided at construction time.

### `PandasSeriesHandler`

Identical approach: guard against the reserved sentinel name, wrap as a single-column
DataFrame (same name-preservation logic as `LogicalPandasSeries.python_to_storage`),
convert to `pa.Table`, then delegate to the Arrow hasher → `ContentHash(method="arrow_v0.1")`.

## Registration (`v0.1.json`)

Appended to `logical_types`:

```json
{"_class": "orcapod.extension_types.pandas_type.LogicalPandasDataFrame", "_config": {}}
{"_class": "orcapod.extension_types.pandas_type.LogicalPandasSeries",     "_config": {}}
```

Appended to `python_type_handler_registry`:

```json
[{"_type": "pandas.core.frame.DataFrame"}, {"_class": "orcapod.hashing.semantic_hashing.builtin_handlers.PandasDataFrameHandler", "_config": {}}]
[{"_type": "pandas.core.series.Series"},   {"_class": "orcapod.hashing.semantic_hashing.builtin_handlers.PandasSeriesHandler",    "_config": {}}]
```

## Testing

File: `tests/test_extension_types/test_pandas_type.py`

Structure mirrors `test_numpy_type.py`:

**`TestLogicalPandasDataFrameProtocol`**
- Satisfies `LogicalTypeProtocol`
- Correct `logical_type_name`, `python_type`
- Arrow extension name `"pandas.dataframe"`, storage type `pa.large_binary()`
- Arrow and Polars extension types are cached (same instance)

**`TestLogicalPandasDataFrameStorage`**
- `python_to_storage` returns `bytes`
- Non-Arrow-serialisable column raises `ValueError`

**`TestLogicalPandasDataFrameRoundTrip`**
- Default `RangeIndex`
- Named index
- MultiIndex (two levels)
- Numeric dtypes (float64, int32, uint8)
- String (object) columns
- Nullable integer (`pd.Int64Dtype`)
- Boolean
- Datetime (`datetime64[ns]`)
- Categorical
- Empty DataFrame

**`TestLogicalPandasSeriesProtocol`** — same protocol checks for `LogicalPandasSeries`

**`TestLogicalPandasSeriesRoundTrip`**
- Unnamed Series (`name=None`)
- Named Series
- Series with named index
- Various dtypes (float, int, string, bool, datetime)
- Empty Series

## Error Handling

For column types that Arrow cannot convert (e.g., a column holding arbitrary Python
objects with mixed types that defeat Arrow's type inference), `pa.Table.from_pandas()`
raises `pa.lib.ArrowInvalid`. Both `LogicalPandasDataFrame.python_to_storage` and
`PandasDataFrameHandler.handle` catch this and re-raise as `ValueError` with a message
that names the problematic column and dtype, directing the user to serialise the column
manually before passing to orcapod.

## `DESIGN_ISSUES.md` / Changelog

Add a changelog entry to `v0.1.json`:
> "Added pandas.DataFrame and pandas.Series as native value types via LogicalPandasDataFrame
>  and LogicalPandasSeries (Arrow IPC / large_binary), with index preservation and
>  PandasDataFrameHandler / PandasSeriesHandler for content hashing (PLT-1869)"
