# SpikeInterface Motion LogicalType Design

**Issue:** ITL-470  
**Date:** 2026-07-02  
**Status:** Approved

---

## Overview

Add native orcapod serialization support for SpikeInterface `Motion` objects via a new
`LogicalSIMotion` registered `LogicalType`. `Motion` is a standalone class (not a
`BaseExtractor` subclass) whose full content is a small set of numpy arrays plus two scalar
metadata fields — making fully self-contained binary storage the right strategy.

---

## `Motion` data model

`spikeinterface.core.motion.Motion` is a pure in-memory container:

| Field | Type | Description |
|---|---|---|
| `displacement` | `list[ndarray (n_t, n_s)]` | Displacement in µm, one array per segment |
| `temporal_bins_s` | `list[ndarray (n_t,)]` | Temporal bin centres in seconds, one per segment |
| `spatial_bins_um` | `ndarray (n_s,)` | Spatial bin centres in µm, shared across segments |
| `direction` | `str` | `"x"`, `"y"`, or `"z"` |
| `interpolation_method` | `str` | e.g. `"linear"` |

`interpolators` and `temporal_bin_edges_s` are computed lazily and are not stored.

SpikeInterface provides two round-trip paths:

- `motion.save(folder)` / `Motion.load(folder)` — folder-based persistence (`.npy` files +
  `spikeinterface_info.json`)
- `motion.to_dict()` / `Motion.from_dict(d)` — in-memory dict exchange, but `to_dict()`
  returns raw numpy arrays that are not JSON-serializable

---

## Storage strategy: self-contained `.npz` in `large_binary`

Because `Motion` is fully realized computed data (not a lazy file-backed extractor), the
correct approach is to store the array content directly in Arrow — no external folder
required, fully portable wherever the database is accessible.

**Chosen approach:** `pa.large_binary()` column containing a NumPy `.npz` archive.

### Why `.npz` over alternatives

| Option | Size (1000×10 motion) | Deterministic | Notes |
|---|---|---|---|
| `.npz` in `large_binary` | ~90 KB | ✓ | Minimal overhead over raw; numpy-native |
| JSON + base64 `.npy` in `large_string` | ~118 KB | ✓ | 34% larger; more readable |

Both are deterministic across calls. `.npz` is preferred for compactness.

### `.npz` contents

```
spatial_bins_um          float64 ndarray  shape (n_spatial,)
displacement_{i}         float64 ndarray  shape (n_temporal, n_spatial)  — one per segment
temporal_bins_s_{i}      float64 ndarray  shape (n_temporal,)             — one per segment
direction                str ndarray      shape (1,)
interpolation_method     str ndarray      shape (1,)
num_segments             int ndarray      shape (1,)
```

---

## `LogicalSIMotion`

- `logical_type_name = "spikeinterface.motion"`
- `python_type = Motion` — no wrapper class needed
- Storage type: `pa.large_binary()`
- Arrow extension name: `"spikeinterface.motion"`

### `python_to_storage(motion)`

```python
buf = io.BytesIO()
kwargs = {
    "spatial_bins_um": motion.spatial_bins_um,
    "direction": np.array([motion.direction]),
    "interpolation_method": np.array([motion.interpolation_method]),
    "num_segments": np.array([motion.num_segments]),
}
for i in range(motion.num_segments):
    kwargs[f"displacement_{i}"] = motion.displacement[i]
    kwargs[f"temporal_bins_s_{i}"] = motion.temporal_bins_s[i]
np.savez(buf, **kwargs)
return buf.getvalue()
```

### `storage_to_python(bytes)`

```python
d = np.load(io.BytesIO(bytes), allow_pickle=False)
n = int(d["num_segments"][0])
return Motion(
    displacement=[d[f"displacement_{i}"] for i in range(n)],
    temporal_bins_s=[d[f"temporal_bins_s_{i}"] for i in range(n)],
    spatial_bins_um=d["spatial_bins_um"],
    direction=str(d["direction"][0]),
    interpolation_method=str(d["interpolation_method"][0]),
)
```

---

## `SIMotionHandler`

Computes a SHA-256 `ContentHash` of the same `.npz` bytes that `LogicalSIMotion` stores in
Arrow. This guarantees hash input ≡ storage representation — identical to the consistency
guarantee used by `SIRecordingHandler` and `SISortingHandler`.

The `hasher` argument is accepted for protocol conformance but not used.

```python
class SIMotionHandler:
    def handle(self, obj: Motion, hasher) -> ContentHash:
        if not isinstance(obj, Motion):
            raise TypeError(...)
        npz_bytes = _motion_to_npz_bytes(obj)  # same helper as python_to_storage
        return ContentHash(method="sha256", digest=hashlib.sha256(npz_bytes).digest())
```

---

## Registration

### `spikeinterface_types.py`

- Add `SIMotionHandler` and `LogicalSIMotion` classes.
- Add Motion import: `from spikeinterface.core.motion import Motion` in the existing
  `try` block alongside `BaseRecording` and `BaseSorting`.
- Extend `register_spikeinterface_types()` with the Motion logical type and handler,
  following the identical pattern used for Recording and Sorting.

### `contexts/data/v0.1.json`

Add to `logical_types`:
```json
{
    "_class": "orcapod.extension_types.spikeinterface_types.LogicalSIMotion",
    "_config": {},
    "_optional": true
}
```

Add to `handlers`:
```json
[
    {"_type": "spikeinterface.core.motion.Motion", "_optional": true},
    {"_class": "orcapod.extension_types.spikeinterface_types.SIMotionHandler", "_config": {}, "_optional": true}
]
```

### `extension_types/__init__.py`

- Add `LogicalSIMotion` and `SIMotionHandler` to the optional SI import block.
- Export both in `__all__` under the same conditional as `LogicalSIRecording`.

---

## Tests (`test_spikeinterface_types.py`)

| Test | What it checks |
|---|---|
| `test_logical_si_motion_importable` | Extension name, python type, storage type |
| `test_si_motion_round_trip` | `python_to_storage` → `storage_to_python` → `motion ==` original |
| `test_si_motion_multi_segment_round_trip` | Same but with 2-segment Motion |
| `test_si_motion_handler_hash_stability` | Same Motion → identical `ContentHash` |
| `test_si_motion_handler_hash_changes_with_content` | Different Motions → different hash |
| `test_si_motion_handler_type_error` | Non-Motion input → `TypeError` |
| `test_register_spikeinterface_types_includes_motion` | Integration: type lookup + hash + full round-trip |

---

## Files changed

| File | Change |
|---|---|
| `src/orcapod/extension_types/spikeinterface_types.py` | Add `LogicalSIMotion`, `SIMotionHandler`; update `register_spikeinterface_types()` |
| `src/orcapod/extension_types/__init__.py` | Add exports for new classes |
| `src/orcapod/contexts/data/v0.1.json` | Add LogicalSIMotion and SIMotionHandler entries |
| `tests/test_extension_types/test_spikeinterface_types.py` | Add Motion tests |
