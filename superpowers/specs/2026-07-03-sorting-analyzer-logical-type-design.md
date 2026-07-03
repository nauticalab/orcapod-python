# LogicalSISortingAnalyzer Design

**Issue:** ITL-469  
**Date:** 2026-07-03  
**Status:** Approved

---

## Overview

Add `LogicalSISortingAnalyzer` as a registered `LogicalType` for SpikeInterface `SortingAnalyzer`
objects. Unlike `BaseRecording` / `BaseSorting`, `SortingAnalyzer` is not a `BaseExtractor`
subclass and has no `to_dict()` round-trip — it is exclusively folder-backed (`binary_folder` or
`zarr`). Orcapod stores a path reference (JSON with `folder` + `format`) rather than serializing
the object's content.

---

## Architecture & Components

### New objects in `spikeinterface_types.py`

**`LogicalSISortingAnalyzer`** — LogicalType

| Property | Value |
|---|---|
| Logical type name | `"spikeinterface.sorting_analyzer"` |
| Python type | `spikeinterface.core.SortingAnalyzer` |
| Arrow storage type | `pa.large_string()` |
| Arrow extension name | `"spikeinterface.sorting_analyzer"` |

**`SISortingAnalyzerHandler`** — semantic hash handler for `SortingAnalyzer`

### Wiring

Both objects are added to:

- `register_spikeinterface_types()` — fourth registration block, appended after Motion
- `contexts/data/v0.1.json` — `"_optional": true` entries alongside the other SI types
- `extension_types/__init__.py` — added to the conditional SI exports

---

## Data Flow

### `python_to_storage(analyzer)` → `str`

1. Check `analyzer.folder is None` → raise `ValueError` with a clear message directing the user
   to call `save_as()` first.
2. Return `json.dumps({"folder": str(analyzer.folder), "format": analyzer.format})`.

The `"format"` field (`"binary_folder"` or `"zarr"`) is stored for human readability and to
support future content hashing (ITL-476). It is **not** used during loading.

### `storage_to_python(storage_value)` → `SortingAnalyzer`

1. Parse JSON: `data = json.loads(storage_value)`.
2. Return `SortingAnalyzer.load(data["folder"])` — format auto-detected from folder suffix by SI.

### `SISortingAnalyzerHandler.handle(analyzer, hasher)` → `ContentHash`

1. Check `analyzer.folder is None` → raise `ValueError`.
2. Return `ContentHash(method="sha256", digest=hashlib.sha256(str(analyzer.folder).encode()).digest())`.

This is **phase 1** — path-string hashing. Content hashing of the folder is deferred to ITL-476.

---

## Error Handling

| Condition | Raised by | Error type | Message |
|---|---|---|---|
| `analyzer.folder is None` (in-memory) | `python_to_storage` | `ValueError` | `"SortingAnalyzer has no folder (in-memory). Call analyzer.save_as(format='binary_folder'\|'zarr', folder=<path>) first."` |
| `analyzer.folder is None` (in-memory) | `SISortingAnalyzerHandler.handle` | `ValueError` | `"Cannot hash in-memory SortingAnalyzer — call save_as() first."` |

---

## Testing

Tests added to `tests/test_extension_types/test_spikeinterface_types.py`, following the same
structure as `LogicalSISorting`.

### Fixture

```python
def _make_sorting_analyzer(tmp_path, format) -> SortingAnalyzer:
    # minimal NumpyRecording + NumpySorting
    # SortingAnalyzer.create(..., format=format, folder=tmp_path / "analyzer")
    # compute one lightweight extension (e.g. "random_spikes")
    # return saved analyzer
```

### Test cases

| Test | Coverage |
|---|---|
| `test_logical_si_sorting_analyzer_importable` | Extension name, python_type, storage type |
| `test_in_memory_analyzer_raises` | `python_to_storage` raises `ValueError` if `folder is None` |
| `test_binary_folder_analyzer_round_trip` | Full serialize → deserialize for `binary_folder` |
| `test_zarr_analyzer_round_trip` | Full serialize → deserialize for `zarr` |
| `test_si_sorting_analyzer_handler_in_memory_raises` | Handler raises on in-memory analyzer |
| `test_si_sorting_analyzer_handler_hash_stability` | Same analyzer → identical hash on repeated calls |
| `test_si_sorting_analyzer_handler_hash_changes_with_path` | Different folder paths → different hashes |
| `test_register_spikeinterface_types_includes_sorting_analyzer` | Type registered in default context |

---

## Out of Scope

- Auto-saving in-memory analyzers to disk
- Content hashing of the analyzer folder (deferred to ITL-476)
- Tracking the underlying recording or sorting dependencies separately
