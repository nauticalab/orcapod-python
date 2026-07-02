# SpikeInterface BaseSorting LogicalType — Design Spec

**Linear issue:** ITL-468  
**Date:** 2026-07-02  
**Author:** agent-kurodo[bot]

---

## Overview

Add native orcapod serialization support for SpikeInterface `BaseSorting` objects so pods can
accept and return sorting results without manual save/reload scaffolding. The implementation
mirrors the existing `LogicalSIRecording` / `SIRecordingHandler` pair (ITL-459) because
`BaseSorting` inherits from the same `BaseExtractor` base class as `BaseRecording` and exposes
an identical serialization API (`to_dict()`, `check_serializability()`, `load()`).

---

## Goals & Success Criteria

- `LogicalSISorting` registered `LogicalType` that stores a `BaseSorting` as a `large_string`
  Arrow column containing SpikeInterface's `to_dict()` JSON dump.
- `python_to_storage()` checks `check_serializability("json")`; raises a clear `ValueError` for
  in-memory sortings with instructions to call `sorting.save_to_zarr()` or
  `sorting.save_to_folder()` first.
- `storage_to_python()` reconstructs via `spikeinterface.core.load(dict)`.
- `SISortingHandler` hashes the same JSON bytes via SHA-256 (phase 1).
- `register_spikeinterface_types()` extended to wire in sorting types alongside recording types.
- Auto-registered in `v0.1.json` with `_optional: true` so it activates automatically when
  `spikeinterface` is installed.
- Round-trip tests covering zarr-backed and numpy_folder-backed sortings.
- Clear error for `NumpySorting` in both serialization and hash handler.

---

## Scope & Boundaries

In scope:

- `LogicalSISorting` class and `SISortingHandler` class in `spikeinterface_types.py`
- `BaseSorting` added to the import from `spikeinterface.core`
- `register_spikeinterface_types()` extended to also register sorting types
- `extension_types/__init__.py` export of `LogicalSISorting`
- `v0.1.json` entries for `LogicalSISorting` and `SISortingHandler` with `_optional: true`
- Comprehensive tests in `test_spikeinterface_types.py`
- Phase 1 hashing only: SHA-256 of JSON bytes

Out of scope:

- Auto-saving in-memory `NumpySorting` to disk (requires database artifact storage)
- Phase 2 hashing (JSON + source path hash)
- `npz_folder` or sorter-folder-specific tests (covered implicitly by the same round-trip path)

---

## Architecture

### File Changes

| File | Change |
|---|---|
| `src/orcapod/extension_types/spikeinterface_types.py` | Add `LogicalSISorting`, `SISortingHandler`; extend `register_spikeinterface_types()` |
| `src/orcapod/extension_types/__init__.py` | Export `LogicalSISorting` from conditional SI import block |
| `src/orcapod/contexts/data/v0.1.json` | Add `LogicalSISorting` + `SISortingHandler` `_optional` entries; add changelog entry |
| `tests/test_extension_types/test_spikeinterface_types.py` | Add sorting-specific test suite |

### `LogicalSISorting`

Mirrors `LogicalSIRecording` exactly, with sorting-specific names and messages:

- `logical_type_name = "spikeinterface.sorting"`
- `python_type = BaseSorting`
- Arrow/Polars extension names: `"spikeinterface.sorting"`, storage type `pa.large_string()`
- `python_to_storage()`:
  1. Calls `value.check_serializability("json")`; raises `ValueError` if `False`
  2. Serializes via `value.to_dict(recursive=True, include_annotations=True, include_properties=False)` + `SIJsonEncoder`
  3. Returns JSON string
- `storage_to_python()`:
  1. `json.loads(storage_value)` → raises `ValueError` on bad JSON
  2. `spikeinterface.core.load(si_dict)` → returns `BaseSorting`

### `SISortingHandler`

Mirrors `SIRecordingHandler`:

- Raises `TypeError` if `obj` is not a `BaseSorting`
- Raises `ValueError` if sorting is not JSON-serializable (in-memory)
- Returns `ContentHash(method="sha256", digest=hashlib.sha256(json_bytes).digest())`
- `json_bytes` = same JSON dump as `python_to_storage()`, encoded to bytes

### `register_spikeinterface_types()` extension

Extends the existing function to also register:

```python
lt_sorting = LogicalSISorting()
try:
    context.type_converter.register_logical_type(lt_sorting)
except ValueError as exc:
    if "already bound to" not in str(exc):
        raise
    # already registered — idempotent
context.semantic_hasher.type_handler_registry.register(BaseSorting, SISortingHandler())
```

### Import structure

The top-level `spikeinterface.core` import gains `BaseSorting`:

```python
from spikeinterface.core import BaseRecording, BaseSorting
```

### `v0.1.json` entries

Logical type entry (after `LogicalSIRecording`):

```json
{
    "_class": "orcapod.extension_types.spikeinterface_types.LogicalSISorting",
    "_config": {},
    "_optional": true
}
```

Handler entry (after `SIRecordingHandler`):

```json
[
    {"_type": "spikeinterface.core.BaseSorting", "_optional": true},
    {"_class": "orcapod.extension_types.spikeinterface_types.SISortingHandler", "_config": {}, "_optional": true}
]
```

---

## Error Handling

`python_to_storage()` error message (non-JSON-serializable sorting):

> "This BaseSorting is not JSON-serializable and cannot be stored by orcapod. This typically
> means it holds data in memory (e.g. NumpySorting). Sortings built on top of file-backed data
> (zarr, numpy_folder, npz_folder, etc.) are fine. If your sorting is in-memory, call
> sorting.save_to_zarr(path) or sorting.save_to_folder(path) first, then pass the returned
> extractor to the pod."

`SISortingHandler.handle()` error message:

> "Cannot hash an in-memory BaseSorting (check_serializability('json') is False). Save it to
> disk first with save_to_zarr() or save_to_folder()."

Both messages must include the phrase `"not JSON-serializable"` and `"file-backed"` so that the
existing test pattern (`match="not JSON-serializable"` / `match="file-backed"`) applies.

---

## Testing

Tests are added to `tests/test_extension_types/test_spikeinterface_types.py`. All SI-dependent
tests use `pytest.importorskip("spikeinterface")` so they skip gracefully when SI is not
installed.

### Helper

```python
def _make_numpy_sorting():
    """Create a small in-memory NumpySorting for use as a test source."""
    import spikeinterface.core as si_core
    rng = np.random.default_rng(42)
    n_samples = 1000
    spike_trains = {
        0: np.sort(rng.choice(n_samples, 20, replace=False)),
        1: np.sort(rng.choice(n_samples, 15, replace=False)),
    }
    return si_core.NumpySorting.from_unit_dict(spike_trains, sampling_frequency=30_000)
```

### Test list

| Test | Verifies |
|---|---|
| `test_logical_si_sorting_importable` | Extension name `"spikeinterface.sorting"`, `python_type` is `BaseSorting`, Arrow storage type is `pa.large_string()` |
| `test_in_memory_sorting_raises` | `NumpySorting` raises `ValueError` with "not JSON-serializable" and "file-backed" |
| `test_folder_sorting_round_trip` | numpy_folder-backed sorting round-trips; `get_unit_spike_train()` output matches |
| `test_zarr_sorting_round_trip` | zarr-backed sorting round-trips |
| `test_si_sorting_handler_hash_stability` | Same sorting → same `ContentHash` across two calls |
| `test_si_sorting_handler_hash_changes_with_content` | Two sortings with different spike trains → different hashes |
| `test_si_sorting_handler_in_memory_raises` | Handler raises `ValueError` for in-memory sorting |
| `test_register_spikeinterface_types` (extended) | Also verifies sorting type wired into context: `python_type_to_arrow_type`, handler lookup, and round-trip |

---

## Dependencies & Risks

- Depends on the `spikeinterface` extras group introduced by ITL-459 (already in place).
- `BaseSorting.to_dict()` accepts the same parameters as `BaseRecording.to_dict()` because both
  inherit from `BaseExtractor`. Verified from SI source.
- `NumpySorting.from_unit_dict()` is the canonical in-memory constructor in SI >= 0.101.
- `save_to_folder()` / `save_to_zarr()` on `BaseSorting` produce `NumpyFolderSorting` /
  `ZarrSortingExtractor` respectively, both of which pass `check_serializability("json")`.
