# SpikeInterface Motion LogicalType Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `LogicalSIMotion` and `SIMotionHandler` so that `spikeinterface.core.motion.Motion` objects can be stored in and retrieved from orcapod databases portably, with no external folder dependency.

**Architecture:** `Motion` is fully in-memory computed data (numpy arrays + two strings), so it is serialised as a self-contained `.npz` archive stored in a `pa.large_binary()` Arrow column. A private helper `_motion_to_npz_bytes()` is shared by the logical type and the hash handler so the stored bytes and hash input are always identical.

**Tech Stack:** Python, NumPy (`.npz`), PyArrow (`large_binary`), Polars, SpikeInterface (`spikeinterface.core.motion.Motion`).

---

## File map

| File | Change |
|---|---|
| `src/orcapod/extension_types/spikeinterface_types.py` | Add `import io`, `import numpy as np`; add `Motion` to SI import; add `_motion_to_npz_bytes()`, `LogicalSIMotion`, `SIMotionHandler`; extend `register_spikeinterface_types()` and its docstring; update module docstring |
| `src/orcapod/extension_types/__init__.py` | Add `LogicalSIMotion` to the optional SI import block and `__all__` |
| `src/orcapod/contexts/data/v0.1.json` | Add `LogicalSIMotion` logical type entry; add `SIMotionHandler` handler entry; add changelog entry |
| `tests/test_extension_types/test_spikeinterface_types.py` | Add `_make_motion()` helper and 7 Motion tests |

---

## Task 1: `LogicalSIMotion` + round-trip tests

**Files:**
- Modify: `tests/test_extension_types/test_spikeinterface_types.py`
- Modify: `src/orcapod/extension_types/spikeinterface_types.py`

- [ ] **Step 1.1: Add the `_make_motion` test helper and three failing tests**

Append to `tests/test_extension_types/test_spikeinterface_types.py` (after the last existing test):

```python
# ── Motion helpers & fixtures ──────────────────────────────────────────────


def _make_motion(seed: int = 42) -> "spikeinterface.core.motion.Motion":
    """Create a small deterministic single-segment Motion for use in tests."""
    from spikeinterface.core.motion import Motion

    rng = np.random.default_rng(seed)
    n_temporal, n_spatial = 100, 5
    displacement = rng.standard_normal((n_temporal, n_spatial))
    temporal_bins = np.linspace(0, 10, n_temporal)
    spatial_bins = np.linspace(0, 3000, n_spatial)
    return Motion(displacement, temporal_bins, spatial_bins, direction="y")


# ── LogicalSIMotion tests ──────────────────────────────────────────────────


def test_logical_si_motion_importable():
    """LogicalSIMotion has the expected extension name, python_type, and storage type."""
    pytest.importorskip("spikeinterface", reason="spikeinterface not installed")
    from orcapod.extension_types.spikeinterface_types import LogicalSIMotion
    from spikeinterface.core.motion import Motion
    import pyarrow as pa

    lt = LogicalSIMotion()
    assert lt.logical_type_name == "spikeinterface.motion"
    assert lt.python_type is Motion
    assert lt.get_arrow_extension_type().storage_type == pa.large_binary()


def test_si_motion_round_trip():
    """Single-segment Motion round-trips through python_to_storage / storage_to_python."""
    pytest.importorskip("spikeinterface", reason="spikeinterface not installed")
    from orcapod.extension_types.spikeinterface_types import LogicalSIMotion

    motion = _make_motion()
    lt = LogicalSIMotion()

    storage = lt.python_to_storage(motion)
    assert isinstance(storage, bytes)

    recovered = lt.storage_to_python(storage)
    assert recovered == motion


def test_si_motion_multi_segment_round_trip():
    """Multi-segment Motion round-trips correctly, preserving segment count and direction."""
    pytest.importorskip("spikeinterface", reason="spikeinterface not installed")
    from orcapod.extension_types.spikeinterface_types import LogicalSIMotion
    from spikeinterface.core.motion import Motion

    rng = np.random.default_rng(99)
    n_t, n_s = 80, 4
    motion = Motion(
        displacement=[rng.standard_normal((n_t, n_s)), rng.standard_normal((n_t, n_s))],
        temporal_bins_s=[np.linspace(0, 8, n_t), np.linspace(0, 8, n_t)],
        spatial_bins_um=np.linspace(0, 2000, n_s),
        direction="z",
        interpolation_method="linear",
    )
    lt = LogicalSIMotion()
    recovered = lt.storage_to_python(lt.python_to_storage(motion))

    assert recovered == motion
    assert recovered.num_segments == 2
    assert recovered.direction == "z"
    assert recovered.interpolation_method == "linear"
```

- [ ] **Step 1.2: Run the three tests — expect failure**

```bash
uv run pytest tests/test_extension_types/test_spikeinterface_types.py \
    -k "motion" -v 2>&1 | tail -20
```

Expected: all three tests fail with `ImportError: cannot import name 'LogicalSIMotion'`.

- [ ] **Step 1.3: Add `import io` and `import numpy as np` to `spikeinterface_types.py`**

In `src/orcapod/extension_types/spikeinterface_types.py`, the current imports block is:

```python
from __future__ import annotations

import hashlib
import json
import logging
from typing import TYPE_CHECKING, Any
```

Replace it with:

```python
from __future__ import annotations

import hashlib
import io
import json
import logging
from typing import TYPE_CHECKING, Any

import numpy as np
```

- [ ] **Step 1.4: Add `Motion` to the SpikeInterface import block**

The current SI import block (lines ~38–44) reads:

```python
try:
    from spikeinterface.core import BaseRecording, BaseSorting
except ImportError as _exc:
    raise ImportError(
        "spikeinterface is not installed. "
        "Install it with: pip install orcapod[spikeinterface]"
    ) from _exc
```

Replace with:

```python
try:
    from spikeinterface.core import BaseRecording, BaseSorting
    from spikeinterface.core.motion import Motion
except ImportError as _exc:
    raise ImportError(
        "spikeinterface is not installed. "
        "Install it with: pip install orcapod[spikeinterface]"
    ) from _exc
```

- [ ] **Step 1.5: Add the `_motion_to_npz_bytes` private helper**

Insert this function immediately after the `logger = logging.getLogger(__name__)` line and before `class LogicalSIRecording`:

```python
def _motion_to_npz_bytes(motion: Motion) -> bytes:
    """Serialise a ``Motion`` to ``.npz`` bytes.

    Shared by ``LogicalSIMotion.python_to_storage`` and ``SIMotionHandler.handle``
    to guarantee the stored bytes and hash input are always identical.

    The ``.npz`` archive keys are: ``spatial_bins_um``, ``direction`` (length-1
    string array), ``interpolation_method`` (length-1 string array),
    ``num_segments`` (length-1 int array), ``displacement_{i}`` and
    ``temporal_bins_s_{i}`` for each segment index ``i``.

    Args:
        motion: A ``Motion`` instance.

    Returns:
        Raw bytes of a NumPy ``.npz`` archive.
    """
    buf = io.BytesIO()
    kwargs: dict[str, np.ndarray] = {
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

- [ ] **Step 1.6: Add the `LogicalSIMotion` class**

Insert this class after the `LogicalSISorting` class and before `class SIRecordingHandler`. The full class:

```python
class LogicalSIMotion(BaseLogicalType):
    """Logical type for ``spikeinterface.core.motion.Motion``.

    Stores ``Motion`` instances as Arrow ``large_binary`` columns tagged with
    extension name ``"spikeinterface.motion"``. The stored value is a NumPy
    ``.npz`` archive produced by ``_motion_to_npz_bytes()``, containing all
    displacement arrays, temporal/spatial bin arrays, and scalar metadata
    (``direction``, ``interpolation_method``, ``num_segments``). Loading
    reconstructs the ``Motion`` directly from those arrays.

    Because the entire content is embedded in the ``.npz`` bytes, no external
    folder is required — stored ``Motion`` objects are portable wherever the
    database is accessible.

    Example:
        >>> import numpy as np
        >>> from spikeinterface.core.motion import Motion
        >>> from orcapod.extension_types.spikeinterface_types import LogicalSIMotion
        >>> lt = LogicalSIMotion()
        >>> motion = Motion(np.zeros((10, 3)), np.linspace(0, 1, 10), np.array([0.0, 1.0, 2.0]))
        >>> recovered = lt.storage_to_python(lt.python_to_storage(motion))
        >>> recovered == motion
        True
    """

    _arrow_ext_class = make_arrow_extension_type("spikeinterface.motion", pa.large_binary())
    _arrow_ext: pa.ExtensionType | None = None
    _polars_ext_class = make_polars_extension_type("spikeinterface.motion", pa.large_binary())
    _polars_ext: pl.BaseExtension | None = None

    logical_type_name: str = "spikeinterface.motion"
    python_type: type = Motion

    def get_arrow_extension_type(self) -> pa.ExtensionType:
        """Return the cached Arrow extension type for ``Motion``.

        Returns:
            A ``pa.ExtensionType`` with extension name ``"spikeinterface.motion"``
            and storage type ``pa.large_binary()``.
        """
        if LogicalSIMotion._arrow_ext is None:
            LogicalSIMotion._arrow_ext = LogicalSIMotion._arrow_ext_class()
        return LogicalSIMotion._arrow_ext

    def get_polars_extension_type(self) -> pl.BaseExtension:
        """Return the cached Polars extension type for ``Motion``.

        Returns:
            A ``pl.BaseExtension`` registered under ``"spikeinterface.motion"``.
        """
        if LogicalSIMotion._polars_ext is None:
            LogicalSIMotion._polars_ext = LogicalSIMotion._polars_ext_class()
        return LogicalSIMotion._polars_ext

    def python_to_storage(
        self, value: Any, converter: TypeConverterProtocol | None = None
    ) -> bytes:
        """Serialise a ``Motion`` to its ``.npz`` storage representation.

        Args:
            value: A ``Motion`` instance.
            converter: Ignored. Present for protocol conformance.

        Returns:
            Raw bytes of a NumPy ``.npz`` archive.
        """
        return _motion_to_npz_bytes(value)

    def storage_to_python(
        self, storage_value: Any, converter: TypeConverterProtocol | None = None
    ) -> Motion:
        """Reconstruct a ``Motion`` from its ``.npz`` storage bytes.

        Args:
            storage_value: Raw ``.npz`` bytes as stored in Arrow ``large_binary``.
            converter: Ignored. Present for protocol conformance.

        Returns:
            A ``Motion`` instance reconstructed from the stored arrays.

        Raises:
            ValueError: If ``storage_value`` cannot be parsed as a valid
                ``.npz`` archive or is missing expected keys.
        """
        try:
            d = np.load(io.BytesIO(bytes(storage_value)), allow_pickle=False)
        except Exception as exc:
            raise ValueError(
                f"LogicalSIMotion: cannot deserialise storage value of type "
                f"{type(storage_value)!r}; expected raw .npz bytes."
            ) from exc
        try:
            n = int(d["num_segments"][0])
            return Motion(
                displacement=[d[f"displacement_{i}"] for i in range(n)],
                temporal_bins_s=[d[f"temporal_bins_s_{i}"] for i in range(n)],
                spatial_bins_um=d["spatial_bins_um"],
                direction=str(d["direction"][0]),
                interpolation_method=str(d["interpolation_method"][0]),
            )
        except KeyError as exc:
            raise ValueError(
                f"LogicalSIMotion: .npz archive is missing expected key {exc}. "
                f"The archive may have been produced by a different version of orcapod."
            ) from exc
```

- [ ] **Step 1.7: Run the three tests — expect all to pass**

```bash
uv run pytest tests/test_extension_types/test_spikeinterface_types.py \
    -k "motion" -v 2>&1 | tail -20
```

Expected:
```
PASSED tests/test_extension_types/test_spikeinterface_types.py::test_logical_si_motion_importable
PASSED tests/test_extension_types/test_spikeinterface_types.py::test_si_motion_round_trip
PASSED tests/test_extension_types/test_spikeinterface_types.py::test_si_motion_multi_segment_round_trip
3 passed
```

- [ ] **Step 1.8: Confirm no existing tests are broken**

```bash
uv run pytest tests/test_extension_types/test_spikeinterface_types.py -v 2>&1 | tail -10
```

Expected: all existing tests still pass.

- [ ] **Step 1.9: Commit**

```bash
git add \
    src/orcapod/extension_types/spikeinterface_types.py \
    tests/test_extension_types/test_spikeinterface_types.py
git commit -m "feat(extension_types): add LogicalSIMotion with .npz storage (ITL-470)"
```

---

## Task 2: `SIMotionHandler` + hash tests

**Files:**
- Modify: `tests/test_extension_types/test_spikeinterface_types.py`
- Modify: `src/orcapod/extension_types/spikeinterface_types.py`

- [ ] **Step 2.1: Add three failing handler tests**

Append to `tests/test_extension_types/test_spikeinterface_types.py`:

```python
# ── SIMotionHandler tests ──────────────────────────────────────────────────


def test_si_motion_handler_hash_stability():
    """Same Motion produces identical ContentHash across two calls."""
    pytest.importorskip("spikeinterface", reason="spikeinterface not installed")
    from orcapod.extension_types.spikeinterface_types import SIMotionHandler
    from orcapod.types import ContentHash

    motion = _make_motion()
    handler = SIMotionHandler()

    h1 = handler.handle(motion, hasher=None)
    h2 = handler.handle(motion, hasher=None)

    assert isinstance(h1, ContentHash)
    assert h1.method == "sha256"
    assert h1 == h2


def test_si_motion_handler_hash_changes_with_content():
    """Different Motion objects (different seeds) produce different ContentHash values."""
    pytest.importorskip("spikeinterface", reason="spikeinterface not installed")
    from orcapod.extension_types.spikeinterface_types import SIMotionHandler

    motion_a = _make_motion(seed=1)
    motion_b = _make_motion(seed=2)

    handler = SIMotionHandler()
    assert handler.handle(motion_a, hasher=None) != handler.handle(motion_b, hasher=None)


def test_si_motion_handler_type_error():
    """SIMotionHandler raises TypeError with class name in message for non-Motion input."""
    pytest.importorskip("spikeinterface", reason="spikeinterface not installed")
    from orcapod.extension_types.spikeinterface_types import SIMotionHandler

    handler = SIMotionHandler()
    with pytest.raises(TypeError, match="SIMotionHandler"):
        handler.handle("not a motion", hasher=None)
```

- [ ] **Step 2.2: Run the three handler tests — expect failure**

```bash
uv run pytest tests/test_extension_types/test_spikeinterface_types.py \
    -k "handler" -v 2>&1 | tail -20
```

Expected: the three new tests fail with `ImportError: cannot import name 'SIMotionHandler'`. Existing handler tests for Recording and Sorting still pass.

- [ ] **Step 2.3: Add the `SIMotionHandler` class**

Insert this class after `LogicalSIMotion` and before `class SIRecordingHandler`:

```python
class SIMotionHandler:
    """Semantic hash handler for ``spikeinterface.core.motion.Motion``.

    Computes a SHA-256 ``ContentHash`` of the ``.npz`` bytes produced by
    ``_motion_to_npz_bytes()``. This is identical to the bytes that
    ``LogicalSIMotion`` stores in Arrow, so hash input and storage
    representation are always consistent.

    The ``hasher`` argument is accepted for protocol conformance but not used —
    hashing is done directly via ``hashlib.sha256`` to avoid overhead.
    """

    def handle(self, obj: Any, hasher: SemanticHasherProtocol | None) -> ContentHash:
        """Return a SHA-256 ``ContentHash`` of the motion's ``.npz`` bytes.

        Args:
            obj: A ``Motion`` instance.
            hasher: Accepted for protocol conformance; not used.

        Returns:
            A ``ContentHash`` with ``method="sha256"`` and digest equal to the
            SHA-256 of the ``.npz`` bytes from ``_motion_to_npz_bytes()``.

        Raises:
            TypeError: If ``obj`` is not a ``Motion``.
        """
        if not isinstance(obj, Motion):
            raise TypeError(
                f"SIMotionHandler: expected Motion, got {type(obj)!r}"
            )
        npz_bytes = _motion_to_npz_bytes(obj)
        logger.debug("SIMotionHandler: hashing %d .npz bytes", len(npz_bytes))
        return ContentHash(
            method="sha256",
            digest=hashlib.sha256(npz_bytes).digest(),
        )
```

- [ ] **Step 2.4: Run all Motion tests — expect all to pass**

```bash
uv run pytest tests/test_extension_types/test_spikeinterface_types.py \
    -k "motion" -v 2>&1 | tail -15
```

Expected: all 6 Motion tests pass.

- [ ] **Step 2.5: Run the full SI test file — confirm no regressions**

```bash
uv run pytest tests/test_extension_types/test_spikeinterface_types.py -v 2>&1 | tail -10
```

Expected: all tests pass.

- [ ] **Step 2.6: Commit**

```bash
git add \
    src/orcapod/extension_types/spikeinterface_types.py \
    tests/test_extension_types/test_spikeinterface_types.py
git commit -m "feat(extension_types): add SIMotionHandler for Motion content hashing (ITL-470)"
```

---

## Task 3: Registration wiring + integration test

**Files:**
- Modify: `tests/test_extension_types/test_spikeinterface_types.py`
- Modify: `src/orcapod/extension_types/spikeinterface_types.py`
- Modify: `src/orcapod/extension_types/__init__.py`
- Modify: `src/orcapod/contexts/data/v0.1.json`

- [ ] **Step 3.1: Add the integration test — expect failure**

Append to `tests/test_extension_types/test_spikeinterface_types.py`:

```python
def test_register_spikeinterface_types_includes_motion():
    """register_spikeinterface_types() wires LogicalSIMotion and SIMotionHandler
    into the default context so they are found by type lookup."""
    pytest.importorskip("spikeinterface", reason="spikeinterface not installed")
    from orcapod.extension_types.spikeinterface_types import register_spikeinterface_types
    from orcapod.contexts import get_default_context
    from spikeinterface.core.motion import Motion
    from orcapod.types import ContentHash

    register_spikeinterface_types()
    ctx = get_default_context()

    motion = _make_motion()

    # LogicalType registered: type_converter can find it by python type
    arrow_type = ctx.type_converter.python_type_to_arrow_type(Motion)
    assert arrow_type.extension_name == "spikeinterface.motion"

    # Handler registered: semantic_hasher can find it by instance
    handler = ctx.semantic_hasher.type_handler_registry.get_handler(motion)
    assert handler is not None
    result = handler.handle(motion, hasher=None)
    assert isinstance(result, ContentHash)

    # Full round-trip through context type_converter
    storage = ctx.type_converter.python_to_storage(motion, Motion)
    recovered = ctx.type_converter.storage_to_python(storage, Motion)
    assert recovered == motion
```

Run it to confirm it fails:

```bash
uv run pytest tests/test_extension_types/test_spikeinterface_types.py \
    -k "register_spikeinterface_types_includes_motion" -v 2>&1 | tail -15
```

Expected: FAIL — `LogicalSIMotion` is not registered in context, type lookup raises.

- [ ] **Step 3.2: Extend `register_spikeinterface_types()` in `spikeinterface_types.py`**

The current function ends with the Sorting section (lines ~455–472). Append the Motion section immediately before the closing of the function body. The existing Sorting block ends with:

```python
    # Handler registration silently replaces an existing entry, so always safe.
    context.semantic_hasher.type_handler_registry.register(BaseSorting, SISortingHandler())
```

After that line, add:

```python
    # --- Motion ---
    lt_motion = LogicalSIMotion()
    try:
        context.type_converter.register_logical_type(lt_motion)
    except ValueError as exc:
        # A different LogicalSIMotion instance is already registered (e.g.
        # auto-registered from v0.1.json at context creation time). That is
        # fine — both instances are equivalent. Any other ValueError propagates.
        if "already bound to" not in str(exc):
            raise
        logger.debug(
            "register_spikeinterface_types: LogicalSIMotion already registered, skipping"
        )
    else:
        logger.debug("register_spikeinterface_types: registered LogicalSIMotion")

    # Handler registration silently replaces an existing entry, so always safe.
    context.semantic_hasher.type_handler_registry.register(Motion, SIMotionHandler())
```

Also update the function's docstring. The current first paragraph reads:

```
    Registers both ``LogicalSIRecording`` / ``SIRecordingHandler`` (ITL-459)
    and ``LogicalSISorting`` / ``SISortingHandler`` (ITL-468).
```

Replace with:

```
    Registers ``LogicalSIRecording`` / ``SIRecordingHandler`` (ITL-459),
    ``LogicalSISorting`` / ``SISortingHandler`` (ITL-468), and
    ``LogicalSIMotion`` / ``SIMotionHandler`` (ITL-470).
```

- [ ] **Step 3.3: Run the integration test — it should pass now (register path)**

```bash
uv run pytest tests/test_extension_types/test_spikeinterface_types.py \
    -k "register_spikeinterface_types_includes_motion" -v 2>&1 | tail -10
```

Expected: PASS.

- [ ] **Step 3.4: Update `extension_types/__init__.py` to export `LogicalSIMotion`**

The current optional SI import block (lines ~34–42) reads:

```python
# ITL-459, ITL-468 — SpikeInterface support (optional; requires pip install orcapod[spikeinterface])
try:
    from .spikeinterface_types import (
        LogicalSIRecording,
        LogicalSISorting,
        register_spikeinterface_types,
    )
    _SI_AVAILABLE = True
except ImportError:
    _SI_AVAILABLE = False
```

Replace with:

```python
# ITL-459, ITL-468, ITL-470 — SpikeInterface support (optional; requires pip install orcapod[spikeinterface])
try:
    from .spikeinterface_types import (
        LogicalSIRecording,
        LogicalSISorting,
        LogicalSIMotion,
        register_spikeinterface_types,
    )
    _SI_AVAILABLE = True
except ImportError:
    _SI_AVAILABLE = False
```

The `__all__` list already has the conditional expression. The current entry reads:

```python
    # ITL-459, ITL-468 (conditional — only present when spikeinterface is installed)
    *( ["LogicalSIRecording", "LogicalSISorting", "register_spikeinterface_types"] if _SI_AVAILABLE else [] ),
```

Replace with:

```python
    # ITL-459, ITL-468, ITL-470 (conditional — only present when spikeinterface is installed)
    *( ["LogicalSIRecording", "LogicalSISorting", "LogicalSIMotion", "register_spikeinterface_types"] if _SI_AVAILABLE else [] ),
```

- [ ] **Step 3.5: Add entries to `contexts/data/v0.1.json`**

**Logical type entry** — insert after the `LogicalSISorting` block (after the closing `}` on line ~56, before the closing `]` of `logical_types`). The current last two SI entries are:

```json
                        {
                            "_class": "orcapod.extension_types.spikeinterface_types.LogicalSIRecording",
                            "_config": {},
                            "_optional": true
                        },
                        {
                            "_class": "orcapod.extension_types.spikeinterface_types.LogicalSISorting",
                            "_config": {},
                            "_optional": true
                        },
```

Add the Motion entry immediately after `LogicalSISorting`:

```json
                        {
                            "_class": "orcapod.extension_types.spikeinterface_types.LogicalSIMotion",
                            "_config": {},
                            "_optional": true
                        },
```

**Handler entry** — the current last two SI handler entries (lines ~115–116) are:

```json
                [{"_type": "spikeinterface.core.BaseRecording", "_optional": true}, {"_class": "orcapod.extension_types.spikeinterface_types.SIRecordingHandler", "_config": {}, "_optional": true}],
                [{"_type": "spikeinterface.core.BaseSorting", "_optional": true}, {"_class": "orcapod.extension_types.spikeinterface_types.SISortingHandler", "_config": {}, "_optional": true}],
```

Add the Motion handler entry immediately after `SISortingHandler`:

```json
                [{"_type": "spikeinterface.core.motion.Motion", "_optional": true}, {"_class": "orcapod.extension_types.spikeinterface_types.SIMotionHandler", "_config": {}, "_optional": true}],
```

**Changelog entry** — the `"changelog"` array in the `"metadata"` section (currently ends with the ITL-468 entry). Append:

```json
"Added spikeinterface.core.motion.Motion as a native value type via LogicalSIMotion (large_binary/.npz) and SIMotionHandler (SHA-256 of .npz bytes); auto-registered when spikeinterface is installed via _optional entries in v0.1.json (ITL-470)"
```

- [ ] **Step 3.6: Update the module docstring in `spikeinterface_types.py`**

The current module docstring starts with:

```
"""SpikeInterface LogicalTypes and handlers for orcapod (ITL-459, ITL-468).

``LogicalSIRecording`` maps ``spikeinterface.core.BaseRecording`` ↔ Arrow
``large_string`` using SpikeInterface's own ``to_dict(recursive=True,
include_annotations=True, include_properties=False)`` JSON dump (encoded
via ``SIJsonEncoder``) as the storage envelope. ``SIRecordingHandler`` hashes
the same JSON bytes via SHA-256 for content identity.

``LogicalSISorting`` maps ``spikeinterface.core.BaseSorting`` ↔ Arrow
``large_string`` using the same serialization approach. ``SISortingHandler``
hashes the JSON bytes via SHA-256.

This module requires the optional ``spikeinterface`` extras group:
``pip install orcapod[spikeinterface]``

Register SI types into the default orcapod context before using them in
pods: call ``register_spikeinterface_types()`` once at startup.
"""
```

Replace with:

```
"""SpikeInterface LogicalTypes and handlers for orcapod (ITL-459, ITL-468, ITL-470).

``LogicalSIRecording`` maps ``spikeinterface.core.BaseRecording`` ↔ Arrow
``large_string`` using SpikeInterface's own ``to_dict(recursive=True,
include_annotations=True, include_properties=False)`` JSON dump (encoded
via ``SIJsonEncoder``) as the storage envelope. ``SIRecordingHandler`` hashes
the same JSON bytes via SHA-256 for content identity.

``LogicalSISorting`` maps ``spikeinterface.core.BaseSorting`` ↔ Arrow
``large_string`` using the same serialization approach. ``SISortingHandler``
hashes the JSON bytes via SHA-256.

``LogicalSIMotion`` maps ``spikeinterface.core.motion.Motion`` ↔ Arrow
``large_binary`` using a self-contained NumPy ``.npz`` archive as the storage
envelope. All displacement arrays, bin arrays, and scalar metadata are embedded
in the archive — no external folder is required. ``SIMotionHandler`` hashes
the same ``.npz`` bytes via SHA-256.

This module requires the optional ``spikeinterface`` extras group:
``pip install orcapod[spikeinterface]``

Register SI types into the default orcapod context before using them in
pods: call ``register_spikeinterface_types()`` once at startup.
"""
```

- [ ] **Step 3.7: Run the full SI test suite — confirm all 7 Motion tests pass**

```bash
uv run pytest tests/test_extension_types/test_spikeinterface_types.py -v 2>&1 | tail -25
```

Expected: all tests pass, including the 7 new Motion tests:
```
PASSED ...::test_logical_si_motion_importable
PASSED ...::test_si_motion_round_trip
PASSED ...::test_si_motion_multi_segment_round_trip
PASSED ...::test_si_motion_handler_hash_stability
PASSED ...::test_si_motion_handler_hash_changes_with_content
PASSED ...::test_si_motion_handler_type_error
PASSED ...::test_register_spikeinterface_types_includes_motion
```

- [ ] **Step 3.8: Run the broader extension_types tests to confirm no regressions**

```bash
uv run pytest tests/test_extension_types/ -v 2>&1 | tail -10
```

Expected: all tests pass.

- [ ] **Step 3.9: Commit**

```bash
git add \
    src/orcapod/extension_types/spikeinterface_types.py \
    src/orcapod/extension_types/__init__.py \
    src/orcapod/contexts/data/v0.1.json \
    tests/test_extension_types/test_spikeinterface_types.py
git commit -m "feat(extension_types): wire LogicalSIMotion registration and exports (ITL-470)"
```

---

## Self-review checklist

- **Spec coverage:**
  - ✓ `LogicalSIMotion` with `logical_type_name = "spikeinterface.motion"` — Task 1
  - ✓ `python_type = Motion`, no wrapper class — Task 1
  - ✓ `pa.large_binary()` storage type — Task 1
  - ✓ `.npz` archive with all named keys — Task 1 steps 1.5–1.6
  - ✓ `storage_to_python` via `np.load` with `allow_pickle=False` — Task 1 step 1.6
  - ✓ `ValueError` on malformed input — Task 1 step 1.6
  - ✓ `SIMotionHandler` SHA-256 of same `.npz` bytes — Task 2
  - ✓ `_motion_to_npz_bytes` shared helper — Task 1 step 1.5
  - ✓ `v0.1.json` logical type entry (`_optional: true`) — Task 3 step 3.5
  - ✓ `v0.1.json` handler entry (`_optional: true`) — Task 3 step 3.5
  - ✓ `__init__.py` export of `LogicalSIMotion` — Task 3 step 3.4
  - ✓ `register_spikeinterface_types()` extended — Task 3 step 3.2
  - ✓ All 7 tests from spec — Tasks 1, 2, 3
- **No placeholders** — all steps contain complete code.
- **Type consistency** — `_motion_to_npz_bytes` defined in Task 1 step 1.5; called identically in `LogicalSIMotion` (step 1.6) and `SIMotionHandler` (step 2.3).
