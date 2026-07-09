# SpikeInterface BaseRecording LogicalType — Design Spec

**Issue:** ITL-459  
**Date:** 2026-07-01  
**Status:** Approved

---

## Overview

SpikeInterface `BaseRecording` objects are not orcapod-serializable today — they hit
`ValueError: Unsupported Python type` in `universal_converter.py`. This forces every pod in
the `orcapod-spikesorting` pipeline to manually persist recordings to disk and return
`op.Directory` paths, with bespoke save/reload code in each pod.

This spec adds a `LogicalSIRecording` registered `LogicalType` that makes `BaseRecording` a
first-class orcapod type. The key insight: recordings that are backed by on-disk files (zarr, binary folder, npz folder,
etc.) are fully captured by SpikeInterface's own `to_dict()` JSON dump — the dump contains the
class name, the path to the backing artifact, and all constructor kwargs needed to reconstruct
it. orcapod stores that JSON string and delegates all disk I/O to SI.

---

## Goals & Success Criteria

- A `LogicalSIRecording` registered in the type registry so pods can annotate parameters and
  return values with `BaseRecording` without errors.
- `python_to_storage()` serializes via `recording.to_dict(recursive=True)` → JSON string.
  Raises a clear `ValueError` for in-memory recordings (`check_serializability("json") == False`).
- `storage_to_python()` reconstructs via `spikeinterface.core.load(dict)`.
- A `SIRecordingHandler` registered in the semantic hasher that hashes the JSON string
  (phase 1 — source-path content hashing is deferred to a follow-up issue).
- `spikeinterface` available as `pip install orcapod[spikeinterface]`; imports are lazy so
  users without the extras group pay no import cost.
- Tests: round-trip for zarr-backed and folder-backed recordings; clear error for
  in-memory `NumpyRecording`.

---

## Architecture

### Package location

New file: `src/orcapod/extension_types/spikeinterface_types.py`

`spikeinterface_types.py` imports `BaseRecording` at module level using a `try/except ImportError`
block that re-raises with a clear pip-install message if SI is absent. The
`extension_types/__init__.py` wraps the import in its own `try/except ImportError` so that
`import orcapod` never fails when SI is not installed — the SI types are silently omitted from
`__all__` in that case.

### Optional dependency

`pyproject.toml` gains a new extras group:

```toml
[project.optional-dependencies]
spikeinterface = ["spikeinterface>=0.101"]
```

### Registration

`LogicalSIRecording` and `SIRecordingHandler` are registered in `contexts/data/v0.1.json` with
`"_optional": true`. The `parse_objectspec` loader catches `ImportError` on optional entries and
returns `None`; both `LogicalTypeRegistry` and `PythonTypeHandlerRegistry` silently skip `None`
entries at construction time. This means:

- **SI installed** — types are auto-registered when the default context is first constructed.
  No explicit call is required.
- **SI absent** — entries resolve to `None` and are skipped. Context construction succeeds and
  all non-SI types continue to work normally.

`register_spikeinterface_types()` is retained for custom `DataContext` instances that are not
constructed from `v0.1.json`. It is idempotent — calling it on a context that already has SI
types registered (e.g. the default context) is safe and logs a debug message.

---

## Components

### `LogicalSIRecording`

Follows the same `BaseLogicalType` pattern as `LogicalDirectory` and `LogicalNumpyArray`.

**Arrow storage type:** `pa.large_string()` — stores a JSON string, consistent with
`LogicalFile` and `LogicalDirectory`.

**`logical_type_name`:** `"spikeinterface.recording"`

**`python_type`:** `BaseRecording` — the registry's MRO lookup automatically handles all
subclasses (`ZarrRecordingExtractor`, `BinaryFolderRecording`, `NumpyFolderRecording`, etc.)
without requiring a factory. This is the same binding strategy used by `LogicalNumpyArray`
for `np.ndarray`.

**`python_to_storage(value, converter)`:**

1. Calls `value.check_serializability("json")`. If `False`, raises:
   ```
   ValueError: This BaseRecording is not JSON-serializable and cannot be stored by orcapod.
   This typically means it holds data in memory (e.g. NumpyRecording). Lazy recordings
   built on top of file-backed data are fine and do not need to be materialized first.
   If your recording is in-memory, call recording.save_to_zarr(path) or
   recording.save_to_folder(path) first, then pass the returned extractor to the pod.
   ```
2. Calls `value.to_dict(recursive=True)` to obtain the SI dict representation.
   `recursive=True` ensures nested extractors (e.g. a recording composed from multiple
   sub-recordings) are also serialized by value, not by reference.
3. Returns `json.dumps(si_dict)`.

**`storage_to_python(storage_value, converter)`:**

1. Parses `json.loads(storage_value)` to recover the SI dict.
2. Calls `spikeinterface.core.load(si_dict)` and returns the result.
3. If the backing zarr/folder has been moved or deleted, SI raises naturally — no
   additional wrapping needed (same behaviour as `LogicalDirectory` with a deleted path).

### `SIRecordingHandler`

Registered in the semantic hasher alongside `FileHandler` and `DirectoryHandler`.

**Phase 1 (this issue):** Hash the JSON string produced by `to_dict(recursive=True)`.

```python
class SIRecordingHandler:
    def handle(self, obj: BaseRecording, hasher) -> ContentHash:
        if not obj.check_serializability("json"):
            raise ValueError(
                "Cannot hash an in-memory BaseRecording. Save it to disk first."
            )
        json_bytes = json.dumps(obj.to_dict(recursive=True)).encode()
        # Exact hashing API (hash_bytes or equivalent) to be confirmed
        # against the SemanticHasher interface during implementation.
        return hasher.hash_bytes(json_bytes)
```

**Phase 2 (deferred — separate issue):** Hash = hash(JSON string) + hash(backing source
directory contents). This requires the database-level artifact storage / efficient directory
hashing infrastructure to land first (ITL-467).

---

## Error Handling

| Situation | Behaviour |
|-----------|-----------|
| `NumpyRecording` or any recording with `check_serializability("json") == False` | `ValueError` clarifying that lazy file-backed recordings are fine but in-memory ones are not, with save instructions |
| `spikeinterface` not installed | `ImportError` on first use of `LogicalSIRecording`, message: `"Install spikeinterface: pip install orcapod[spikeinterface]"` |
| Backing zarr/folder deleted after storage | SI raises `FileNotFoundError` or similar on `load()` — propagates as-is |
| Corrupt JSON in storage | `json.JSONDecodeError` raised with the raw value in the message |

---

## Tests

File: `tests/test_extension_types/test_spikeinterface_types.py`

All tests that actually load/create SI recordings use `pytest.importorskip("spikeinterface")`
so they are skipped automatically when SI is not installed.

| Test | What it checks |
|------|----------------|
| `test_zarr_recording_round_trip` | zarr-backed recording → `python_to_storage` → `storage_to_python` → same data |
| `test_folder_recording_round_trip` | binary-folder-backed recording → same |
| `test_ephemeral_recording_round_trip` | lazy preprocessing chain on top of file-backed data (not materialized to zarr/folder) → round-trips correctly; `get_traces()` returns same values |
| `test_in_memory_recording_raises` | `NumpyRecording` (explicit `json=False`) → `ValueError` with clear message |
| `test_hash_stability` | same recording hashes identically across two calls |
| `test_hash_changes_with_content` | different recordings produce different hashes |
| `test_spikeinterface_not_installed` | `ImportError` raised when SI unavailable (mock import) |

---

## Out of Scope

- `BaseSorting`, `SortingAnalyzer`, `Motion` support (ITL-468, ITL-469, ITL-470)
- Phase 2 hashing (JSON + source-path content hash) — deferred to follow-up
- Auto-saving in-memory recordings to disk — requires database-level artifact storage
  protocol (ITL-467)
- Remote/S3 zarr storage — works transparently if SI supports it, no orcapod changes needed
- Relative path portability — recordings store absolute paths; portability is a future concern

---

## References

- `src/orcapod/extension_types/directory_type.py` — pattern for JSON-storage LogicalType
- `src/orcapod/extension_types/numpy_type.py` — pattern for direct python_type binding + hash handler
- `src/orcapod/extension_types/spikeinterface_types.py` — implementation (LogicalType, handler, registration)
- `src/orcapod/contexts/data/v0.1.json` — context config changelog entry (LogicalType/handler NOT wired in statically)
- SpikeInterface `BaseExtractor.to_dict()` / `check_serializability()`: `spikeinterface/core/base.py`
- ITL-467 — database-level artifact storage (phase 2 dependency)
