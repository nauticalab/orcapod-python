# SpikeInterface BaseRecording LogicalType — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `LogicalSIRecording` so that pods can accept and return SpikeInterface `BaseRecording` objects natively — stored as SI's JSON dump in Arrow, content-hashed by SHA-256 of that JSON.

**Architecture:** `spikeinterface_types.py` imports `BaseRecording` at module level (raises `ImportError` if SI absent, caught gracefully by `extension_types/__init__.py`). `LogicalSIRecording` stores `to_dict(recursive=True)` as JSON `large_string`. `SIRecordingHandler` hashes those same JSON bytes via SHA-256. Registration into the default context is explicit via `register_spikeinterface_types()`. No changes to `v0.1.json` or `builtin_handlers.py`.

**Tech Stack:** SpikeInterface ≥0.101, PyArrow, Polars, Python 3.12, pytest, uv

---

## File Map

| Action | Path | Responsibility |
|--------|------|----------------|
| Create | `src/orcapod/extension_types/spikeinterface_types.py` | `LogicalSIRecording`, `SIRecordingHandler`, `register_spikeinterface_types()` |
| Modify | `src/orcapod/extension_types/__init__.py` | Conditionally export `LogicalSIRecording`, `register_spikeinterface_types` |
| Modify | `pyproject.toml` | Add `spikeinterface` optional extras group |
| Create | `tests/test_extension_types/test_spikeinterface_types.py` | All SI type tests |

`builtin_handlers.py` and `contexts/data/v0.1.json` are **not touched**.

---

## Task 1: Add optional extras group to `pyproject.toml`

**Files:**
- Modify: `pyproject.toml`

- [ ] **Step 1: Add the extras group**

In `pyproject.toml`, find the `[project.optional-dependencies]` section and make these two changes:

```toml
spikeinterface = ["spikeinterface>=0.101"]
all = ["orcapod[redis]", "orcapod[ray]", "orcapod[postgresql]", "orcapod[spiraldb]", "orcapod[spikeinterface]"]
```

The `all` line replaces the existing `all` line (add `"orcapod[spikeinterface]"` to the end of the existing list).

- [ ] **Step 2: Verify the extras install correctly**

```bash
uv run --with spikeinterface python -c "import spikeinterface; print(spikeinterface.__version__)"
```

Expected output: a version string like `0.104.8` (no errors).

- [ ] **Step 3: Commit**

```bash
git add pyproject.toml
git commit -m "chore(deps): add spikeinterface optional extras group (ITL-459)"
```

---

## Task 2: Create `spikeinterface_types.py` with `LogicalSIRecording`

**Files:**
- Create: `src/orcapod/extension_types/spikeinterface_types.py`
- Create: `tests/test_extension_types/test_spikeinterface_types.py`

- [ ] **Step 1: Write the failing import test**

```python
# tests/test_extension_types/test_spikeinterface_types.py
"""Tests for LogicalSIRecording and SIRecordingHandler (ITL-459)."""

from __future__ import annotations

import json

import numpy as np
import pytest


def test_spikeinterface_not_installed_raises_import_error(monkeypatch):
    """Importing spikeinterface_types when SI is absent raises ImportError."""
    import sys
    import importlib

    # Remove any cached spikeinterface modules
    si_keys = [k for k in sys.modules if k == "spikeinterface" or k.startswith("spikeinterface.")]
    saved = {k: sys.modules.pop(k) for k in si_keys}
    # Block re-import
    monkeypatch.setitem(sys.modules, "spikeinterface", None)
    monkeypatch.setitem(sys.modules, "spikeinterface.core", None)

    try:
        # Remove the cached spikeinterface_types module so it re-imports
        si_types_key = "orcapod.extension_types.spikeinterface_types"
        saved_si_types = sys.modules.pop(si_types_key, None)
        with pytest.raises(ImportError, match="pip install orcapod\\[spikeinterface\\]"):
            importlib.import_module(si_types_key)
    finally:
        # Restore everything
        for k in list(sys.modules):
            if k == "spikeinterface" or k.startswith("spikeinterface."):
                del sys.modules[k]
        sys.modules.update(saved)
        if saved_si_types is not None:
            sys.modules[si_types_key] = saved_si_types
        else:
            sys.modules.pop(si_types_key, None)
            importlib.import_module(si_types_key)


# All tests below require spikeinterface — skip if not installed
si = pytest.importorskip("spikeinterface", reason="spikeinterface not installed")
import spikeinterface.core as si_core  # noqa: E402


def _make_numpy_recording() -> si_core.NumpyRecording:
    """Create a small in-memory NumpyRecording for use as a test source."""
    rng = np.random.default_rng(42)
    traces = rng.standard_normal((200, 4)).astype("float32")
    return si_core.NumpyRecording([traces], sampling_frequency=30_000)


def test_logical_si_recording_importable():
    from orcapod.extension_types.spikeinterface_types import LogicalSIRecording
    import pyarrow as pa

    lt = LogicalSIRecording()
    assert lt.logical_type_name == "spikeinterface.recording"
    assert lt.python_type is si_core.BaseRecording
    assert lt.get_arrow_extension_type().storage_type == pa.large_string()
```

- [ ] **Step 2: Run to confirm failure**

```bash
uv run --with spikeinterface pytest tests/test_extension_types/test_spikeinterface_types.py::test_logical_si_recording_importable -v
```

Expected: `FAILED` — `ModuleNotFoundError: No module named 'orcapod.extension_types.spikeinterface_types'`

- [ ] **Step 3: Create `spikeinterface_types.py`**

```python
# src/orcapod/extension_types/spikeinterface_types.py
"""SpikeInterface LogicalType and handler for orcapod (ITL-459).

``LogicalSIRecording`` maps ``spikeinterface.core.BaseRecording`` ↔ Arrow
``large_string`` using SpikeInterface's own ``to_dict(recursive=True)`` JSON
dump as the storage envelope.  ``SIRecordingHandler`` hashes the same JSON
bytes via SHA-256 for content identity.

This module requires the optional ``spikeinterface`` extras group::

    pip install orcapod[spikeinterface]

Register SI types into the default orcapod context before using them in
pods::

    from orcapod.extension_types.spikeinterface_types import register_spikeinterface_types
    register_spikeinterface_types()
"""

from __future__ import annotations

import hashlib
import json
import logging
from typing import TYPE_CHECKING, Any

import polars as pl
import pyarrow as pa

from orcapod.extension_types.base_logical_type import BaseLogicalType
from orcapod.extension_types.registry import make_arrow_extension_type, make_polars_extension_type
from orcapod.types import ContentHash

if TYPE_CHECKING:
    from orcapod.extension_types.protocols import TypeConverterProtocol
    from orcapod.protocols.hashing_protocols import SemanticHasherProtocol

try:
    from spikeinterface.core import BaseRecording
except ImportError as _exc:
    raise ImportError(
        "spikeinterface is not installed. "
        "Install it with: pip install orcapod[spikeinterface]"
    ) from _exc

logger = logging.getLogger(__name__)


class LogicalSIRecording(BaseLogicalType):
    """Logical type for ``spikeinterface.core.BaseRecording``.

    Stores ``BaseRecording`` instances as Arrow ``large_string`` columns
    tagged with extension name ``"spikeinterface.recording"``.  The stored
    value is SpikeInterface's own ``to_dict(recursive=True)`` output,
    JSON-serialised.  Loading reconstructs the recording via
    ``spikeinterface.core.load_extractor(dict)``.

    Only recordings whose ``check_serializability("json")`` returns ``True``
    are accepted.  Lazy recordings built on top of file-backed data (zarr,
    binary folder, etc.) qualify.  In-memory ``NumpyRecording`` objects do
    not and raise ``ValueError`` with clear save instructions.

    Example:
        >>> import tempfile, numpy as np
        >>> import spikeinterface.core as si
        >>> from orcapod.extension_types.spikeinterface_types import LogicalSIRecording
        >>> lt = LogicalSIRecording()
        >>> with tempfile.TemporaryDirectory() as tmp:
        ...     rec = si.NumpyRecording([np.zeros((100, 4), dtype="float32")], 30000)
        ...     saved = rec.save_to_folder(tmp + "/rec")
        ...     storage = lt.python_to_storage(saved)
        ...     recovered = lt.storage_to_python(storage)
        ...     saved.get_traces(segment_index=0).shape == recovered.get_traces(segment_index=0).shape
        True
    """

    _arrow_ext_class = make_arrow_extension_type("spikeinterface.recording", pa.large_string())
    _arrow_ext: pa.ExtensionType | None = None
    _polars_ext_class = make_polars_extension_type("spikeinterface.recording", pa.large_string())
    _polars_ext: pl.BaseExtension | None = None

    logical_type_name: str = "spikeinterface.recording"
    python_type: type = BaseRecording

    def get_arrow_extension_type(self) -> pa.ExtensionType:
        """Return the cached Arrow extension type for ``BaseRecording``.

        Returns:
            A ``pa.ExtensionType`` with extension name
            ``"spikeinterface.recording"`` and storage type ``pa.large_string()``.
        """
        if LogicalSIRecording._arrow_ext is None:
            LogicalSIRecording._arrow_ext = LogicalSIRecording._arrow_ext_class()
        return LogicalSIRecording._arrow_ext

    def get_polars_extension_type(self) -> pl.BaseExtension:
        """Return the cached Polars extension type for ``BaseRecording``.

        Returns:
            A ``pl.BaseExtension`` registered under ``"spikeinterface.recording"``.
        """
        if LogicalSIRecording._polars_ext is None:
            LogicalSIRecording._polars_ext = LogicalSIRecording._polars_ext_class()
        return LogicalSIRecording._polars_ext

    def python_to_storage(
        self, value: Any, converter: TypeConverterProtocol | None = None
    ) -> str:
        """Serialise a ``BaseRecording`` to its JSON storage representation.

        Args:
            value: A ``BaseRecording`` instance whose
                ``check_serializability("json")`` returns ``True``.
            converter: Ignored. Present for protocol conformance.

        Returns:
            A JSON string produced by ``recording.to_dict(recursive=True)``.

        Raises:
            ValueError: If the recording is not JSON-serialisable (e.g. an
                in-memory ``NumpyRecording``).
        """
        if not value.check_serializability("json"):
            raise ValueError(
                "This BaseRecording is not JSON-serializable and cannot be stored "
                "by orcapod. This typically means it holds data in memory (e.g. "
                "NumpyRecording). Lazy recordings built on top of file-backed data "
                "(zarr, binary folder, etc.) are fine and do not need to be "
                "materialized first. If your recording is in-memory, call "
                "recording.save_to_zarr(path) or recording.save_to_folder(path) "
                "first, then pass the returned extractor to the pod."
            )
        return json.dumps(value.to_dict(recursive=True))

    def storage_to_python(
        self, storage_value: Any, converter: TypeConverterProtocol | None = None
    ) -> BaseRecording:
        """Reconstruct a ``BaseRecording`` from its JSON storage string.

        Args:
            storage_value: A JSON string as stored in Arrow.
            converter: Ignored. Present for protocol conformance.

        Returns:
            A ``BaseRecording`` instance reconstructed via
            ``spikeinterface.core.load_extractor``.

        Raises:
            ValueError: If ``storage_value`` is not valid JSON.
            FileNotFoundError: If the backing zarr/folder no longer exists
                (raised by SpikeInterface, propagated as-is).
        """
        from spikeinterface.core import load_extractor
        try:
            si_dict = json.loads(storage_value)
        except (json.JSONDecodeError, TypeError) as exc:
            raise ValueError(
                f"LogicalSIRecording: cannot deserialise storage value "
                f"{storage_value!r}; expected a JSON string."
            ) from exc
        return load_extractor(si_dict)


class SIRecordingHandler:
    """Semantic hash handler for ``spikeinterface.core.BaseRecording``.

    Computes a SHA-256 ``ContentHash`` of the JSON bytes produced by
    ``recording.to_dict(recursive=True)``.  This is identical to the bytes
    that ``LogicalSIRecording`` stores in Arrow, so hash input and storage
    representation are always consistent.

    The ``hasher`` argument is accepted for protocol conformance but not used —
    hashing is done directly via ``hashlib.sha256`` to avoid overhead.

    Phase 2 (deferred — ITL-467): hash will additionally cover backing source
    directory contents once efficient directory hashing infrastructure lands.
    """

    def handle(self, obj: Any, hasher: SemanticHasherProtocol | None) -> ContentHash:
        """Return a SHA-256 ``ContentHash`` of the recording's JSON dump.

        Args:
            obj: A ``BaseRecording`` instance.
            hasher: Accepted for protocol conformance; not used.

        Returns:
            A ``ContentHash`` with ``method="sha256"`` and digest equal to the
            SHA-256 of ``json.dumps(recording.to_dict(recursive=True)).encode()``.

        Raises:
            TypeError: If ``obj`` is not a ``BaseRecording``.
            ValueError: If the recording is not JSON-serialisable (in-memory).
        """
        if not isinstance(obj, BaseRecording):
            raise TypeError(
                f"SIRecordingHandler: expected BaseRecording, got {type(obj)!r}"
            )
        if not obj.check_serializability("json"):
            raise ValueError(
                "Cannot hash an in-memory BaseRecording "
                "(check_serializability('json') is False). "
                "Save it to disk first with save_to_zarr() or save_to_folder()."
            )
        json_bytes = json.dumps(obj.to_dict(recursive=True)).encode()
        logger.debug("SIRecordingHandler: hashing %d JSON bytes", len(json_bytes))
        return ContentHash(
            method="sha256",
            digest=hashlib.sha256(json_bytes).digest(),
        )


def register_spikeinterface_types(context: Any = None) -> None:
    """Register SpikeInterface LogicalTypes into an orcapod ``DataContext``.

    Call this once at startup, before any pods that use SpikeInterface types
    are declared or executed.  If ``context`` is ``None``, the default context
    (from ``orcapod.contexts.get_default_context()``) is used.

    Args:
        context: A ``DataContext`` instance, or ``None`` to use the default.

    Example:
        >>> from orcapod.extension_types.spikeinterface_types import register_spikeinterface_types
        >>> register_spikeinterface_types()  # registers into the default context
    """
    if context is None:
        from orcapod.contexts import get_default_context
        context = get_default_context()

    lt = LogicalSIRecording()
    context.type_converter._logical_type_registry.register_logical_type(lt)
    context.semantic_hasher.type_handler_registry.register(BaseRecording, SIRecordingHandler())
    logger.debug(
        "register_spikeinterface_types: registered LogicalSIRecording and SIRecordingHandler"
    )
```

- [ ] **Step 4: Run both import tests**

```bash
uv run --with spikeinterface pytest tests/test_extension_types/test_spikeinterface_types.py::test_logical_si_recording_importable tests/test_extension_types/test_spikeinterface_types.py::test_spikeinterface_not_installed_raises_import_error -v
```

Expected: both `PASSED`

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/extension_types/spikeinterface_types.py tests/test_extension_types/test_spikeinterface_types.py
git commit -m "feat(extension_types): add LogicalSIRecording, SIRecordingHandler, register_spikeinterface_types (ITL-459)"
```

---

## Task 3: `python_to_storage` — in-memory recording raises `ValueError`

**Files:**
- Modify: `tests/test_extension_types/test_spikeinterface_types.py`

- [ ] **Step 1: Add the test**

Add to `tests/test_extension_types/test_spikeinterface_types.py` (after the `importorskip` line):

```python
def test_in_memory_recording_raises():
    """NumpyRecording (json=False) raises ValueError with clear instructions."""
    from orcapod.extension_types.spikeinterface_types import LogicalSIRecording

    rec = _make_numpy_recording()
    lt = LogicalSIRecording()

    with pytest.raises(ValueError, match="not JSON-serializable"):
        lt.python_to_storage(rec)

    # The error message must distinguish in-memory from lazy file-backed recordings
    with pytest.raises(ValueError, match="file-backed"):
        lt.python_to_storage(rec)
```

- [ ] **Step 2: Run to confirm it passes (implementation already present in the skeleton)**

```bash
uv run --with spikeinterface pytest tests/test_extension_types/test_spikeinterface_types.py::test_in_memory_recording_raises -v
```

Expected: `PASSED`

- [ ] **Step 3: Commit**

```bash
git add tests/test_extension_types/test_spikeinterface_types.py
git commit -m "test(spikeinterface_types): in-memory NumpyRecording raises ValueError (ITL-459)"
```

---

## Task 4: Round-trip tests — folder-backed, zarr-backed, ephemeral

**Files:**
- Modify: `tests/test_extension_types/test_spikeinterface_types.py`

- [ ] **Step 1: Add the three round-trip tests**

Add to `tests/test_extension_types/test_spikeinterface_types.py`:

```python
def test_folder_recording_round_trip(tmp_path):
    """Binary-folder-backed recording round-trips through python_to_storage / storage_to_python."""
    from orcapod.extension_types.spikeinterface_types import LogicalSIRecording

    saved = _make_numpy_recording().save_to_folder(str(tmp_path / "rec"))
    lt = LogicalSIRecording()

    storage = lt.python_to_storage(saved)
    assert isinstance(storage, str)
    data = json.loads(storage)
    assert "class" in data  # SI dict always has a "class" key

    recovered = lt.storage_to_python(storage)
    np.testing.assert_array_equal(
        saved.get_traces(segment_index=0),
        recovered.get_traces(segment_index=0),
    )


def test_zarr_recording_round_trip(tmp_path):
    """Zarr-backed recording round-trips through python_to_storage / storage_to_python."""
    from orcapod.extension_types.spikeinterface_types import LogicalSIRecording

    saved = _make_numpy_recording().save_to_zarr(str(tmp_path / "rec.zarr"))
    lt = LogicalSIRecording()

    storage = lt.python_to_storage(saved)
    assert isinstance(storage, str)

    recovered = lt.storage_to_python(storage)
    np.testing.assert_array_equal(
        saved.get_traces(segment_index=0),
        recovered.get_traces(segment_index=0),
    )


def test_ephemeral_recording_round_trip(tmp_path):
    """A lazy preprocessing chain on a file-backed recording round-trips correctly.

    The preprocessed recording is NOT materialized to zarr/folder but IS
    JSON-serializable because its root is file-backed.  On reload,
    SpikeInterface re-applies the preprocessing chain lazily.
    """
    import spikeinterface.preprocessing as spre
    from orcapod.extension_types.spikeinterface_types import LogicalSIRecording

    # Save the source to disk; apply a lazy preprocessing step on top
    source = _make_numpy_recording().save_to_folder(str(tmp_path / "source"))
    preprocessed = spre.bandpass_filter(source, freq_min=300.0, freq_max=6000.0)

    assert preprocessed.check_serializability("json"), (
        "Preprocessed recording on folder-backed source must be JSON-serializable"
    )

    lt = LogicalSIRecording()
    storage = lt.python_to_storage(preprocessed)
    recovered = lt.storage_to_python(storage)

    np.testing.assert_array_equal(
        preprocessed.get_traces(segment_index=0),
        recovered.get_traces(segment_index=0),
    )
```

- [ ] **Step 2: Run all three round-trip tests**

```bash
uv run --with spikeinterface pytest \
  tests/test_extension_types/test_spikeinterface_types.py::test_folder_recording_round_trip \
  tests/test_extension_types/test_spikeinterface_types.py::test_zarr_recording_round_trip \
  tests/test_extension_types/test_spikeinterface_types.py::test_ephemeral_recording_round_trip \
  -v
```

Expected: all three `PASSED`

- [ ] **Step 3: Commit**

```bash
git add tests/test_extension_types/test_spikeinterface_types.py
git commit -m "test(spikeinterface_types): round-trip tests for folder, zarr, ephemeral recordings (ITL-459)"
```

---

## Task 5: Hash stability and content-change tests

**Files:**
- Modify: `tests/test_extension_types/test_spikeinterface_types.py`

- [ ] **Step 1: Add hash tests**

Add to `tests/test_extension_types/test_spikeinterface_types.py`:

```python
def test_si_recording_handler_hash_stability(tmp_path):
    """Same recording produces identical ContentHash across two calls."""
    from orcapod.extension_types.spikeinterface_types import SIRecordingHandler
    from orcapod.types import ContentHash

    saved = _make_numpy_recording().save_to_folder(str(tmp_path / "rec"))
    handler = SIRecordingHandler()

    h1 = handler.handle(saved, hasher=None)
    h2 = handler.handle(saved, hasher=None)

    assert isinstance(h1, ContentHash)
    assert h1 == h2


def test_si_recording_handler_hash_changes_with_content(tmp_path):
    """Different recordings produce different ContentHash values."""
    from orcapod.extension_types.spikeinterface_types import SIRecordingHandler

    rng = np.random.default_rng(0)
    rec_a = si_core.NumpyRecording(
        [rng.standard_normal((200, 4)).astype("float32")], sampling_frequency=30_000
    ).save_to_folder(str(tmp_path / "rec_a"))
    rec_b = si_core.NumpyRecording(
        [rng.standard_normal((200, 4)).astype("float32")], sampling_frequency=30_000
    ).save_to_folder(str(tmp_path / "rec_b"))

    handler = SIRecordingHandler()
    assert handler.handle(rec_a, hasher=None) != handler.handle(rec_b, hasher=None)


def test_si_recording_handler_in_memory_raises():
    """SIRecordingHandler raises ValueError for in-memory recordings."""
    from orcapod.extension_types.spikeinterface_types import SIRecordingHandler

    rec = _make_numpy_recording()
    handler = SIRecordingHandler()
    with pytest.raises(ValueError, match="in-memory"):
        handler.handle(rec, hasher=None)
```

- [ ] **Step 2: Run hash tests**

```bash
uv run --with spikeinterface pytest \
  tests/test_extension_types/test_spikeinterface_types.py::test_si_recording_handler_hash_stability \
  tests/test_extension_types/test_spikeinterface_types.py::test_si_recording_handler_hash_changes_with_content \
  tests/test_extension_types/test_spikeinterface_types.py::test_si_recording_handler_in_memory_raises \
  -v
```

Expected: all three `PASSED`

- [ ] **Step 3: Commit**

```bash
git add tests/test_extension_types/test_spikeinterface_types.py
git commit -m "test(spikeinterface_types): hash stability and content-change tests (ITL-459)"
```

---

## Task 6: Integration test for `register_spikeinterface_types()`

**Files:**
- Modify: `tests/test_extension_types/test_spikeinterface_types.py`

- [ ] **Step 1: Add the integration test**

Add to `tests/test_extension_types/test_spikeinterface_types.py`:

```python
def test_register_spikeinterface_types(tmp_path):
    """register_spikeinterface_types() wires LogicalSIRecording and SIRecordingHandler
    into the default context so they are found by type lookup."""
    from orcapod.extension_types.spikeinterface_types import register_spikeinterface_types
    from orcapod.contexts import get_default_context

    register_spikeinterface_types()
    ctx = get_default_context()

    saved = _make_numpy_recording().save_to_folder(str(tmp_path / "rec"))

    # LogicalType registered: type_converter can find it
    arrow_type = ctx.type_converter.python_type_to_arrow_type(type(saved))
    assert arrow_type.extension_name == "spikeinterface.recording"

    # Handler registered: semantic_hasher can find it
    from orcapod.types import ContentHash
    handler = ctx.semantic_hasher.type_handler_registry.get_handler(saved)
    assert handler is not None
    result = handler.handle(saved, hasher=None)
    assert isinstance(result, ContentHash)

    # Full round-trip through python_to_storage / storage_to_python
    storage = ctx.type_converter.python_to_storage(saved, type(saved))
    recovered = ctx.type_converter.storage_to_python(storage, type(saved))
    np.testing.assert_array_equal(
        saved.get_traces(segment_index=0),
        recovered.get_traces(segment_index=0),
    )
```

- [ ] **Step 2: Run the integration test**

```bash
uv run --with spikeinterface pytest tests/test_extension_types/test_spikeinterface_types.py::test_register_spikeinterface_types -v
```

Expected: `PASSED`

- [ ] **Step 3: Commit**

```bash
git add tests/test_extension_types/test_spikeinterface_types.py
git commit -m "test(spikeinterface_types): integration test for register_spikeinterface_types (ITL-459)"
```

---

## Task 7: Export from `extension_types/__init__.py`

**Files:**
- Modify: `src/orcapod/extension_types/__init__.py`

- [ ] **Step 1: Add conditional import and export**

In `src/orcapod/extension_types/__init__.py`, add after the `from .numpy_type import LogicalNumpyArray  # ITL-460` line:

```python
# ITL-459 — SpikeInterface support (optional; requires pip install orcapod[spikeinterface])
try:
    from .spikeinterface_types import LogicalSIRecording, register_spikeinterface_types
    _SI_AVAILABLE = True
except ImportError:
    _SI_AVAILABLE = False
```

And at the end of `__all__`, add:

```python
    # ITL-459 (conditional — only present when spikeinterface is installed)
    *( ["LogicalSIRecording", "register_spikeinterface_types"] if _SI_AVAILABLE else [] ),
```

- [ ] **Step 2: Verify import works with SI installed**

```bash
uv run --with spikeinterface python -c "
from orcapod.extension_types import LogicalSIRecording, register_spikeinterface_types, __all__
assert 'LogicalSIRecording' in __all__
print('OK with SI:', LogicalSIRecording, register_spikeinterface_types)
"
```

Expected: prints the class and function without error.

- [ ] **Step 3: Verify import works without SI installed**

```bash
uv run python -c "
from orcapod.extension_types import __all__
assert 'LogicalSIRecording' not in __all__, f'should not be exported, got: {__all__}'
print('OK without SI: LogicalSIRecording not in __all__')
"
```

Expected: prints the OK message without error.

- [ ] **Step 4: Commit**

```bash
git add src/orcapod/extension_types/__init__.py
git commit -m "feat(extension_types): conditionally export LogicalSIRecording when SI available (ITL-459)"
```

---

## Task 8: Full suite check and changelog

**Files:**
- Modify: `src/orcapod/contexts/data/v0.1.json`

- [ ] **Step 1: Run all spikeinterface tests**

```bash
uv run --with spikeinterface pytest tests/test_extension_types/test_spikeinterface_types.py -v
```

Expected: all 10 tests `PASSED` (1 not-installed test + 9 SI tests).

- [ ] **Step 2: Run the full extension_types suite without SI to verify no regressions**

```bash
uv run pytest tests/test_extension_types/ -v
```

Expected: all pre-existing tests pass; SI tests that need SI are `SKIPPED` (not `FAILED`). The `test_spikeinterface_not_installed_raises_import_error` test runs and passes even without SI.

- [ ] **Step 3: Run the full test suite**

```bash
uv run pytest tests/ -v --timeout=60
```

Expected: all pre-existing tests pass; new SI tests either `PASSED` (with SI) or `SKIPPED` (without SI).

- [ ] **Step 4: Update `v0.1.json` changelog**

In `src/orcapod/contexts/data/v0.1.json`, append to the `"changelog"` array:

```json
"Added spikeinterface.BaseRecording as a native value type via LogicalSIRecording (large_string/JSON) and SIRecordingHandler (SHA-256 of JSON bytes); requires pip install orcapod[spikeinterface] and a register_spikeinterface_types() call (ITL-459)"
```

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/contexts/data/v0.1.json
git commit -m "docs(context): record LogicalSIRecording in v0.1.json changelog (ITL-459)"
```
