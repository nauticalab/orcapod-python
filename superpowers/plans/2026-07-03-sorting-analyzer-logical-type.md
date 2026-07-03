# LogicalSISortingAnalyzer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `LogicalSISortingAnalyzer` and `SISortingAnalyzerHandler` to orcapod as a registered LogicalType for SpikeInterface `SortingAnalyzer` objects.

**Architecture:** `SortingAnalyzer` is folder-backed only (no `to_dict()`), so storage is a JSON object `{"folder": "...", "format": "binary_folder"|"zarr"}` stored as `pa.large_string()`. `storage_to_python` calls `SortingAnalyzer.load(folder)` which auto-detects format from the folder suffix. Hash = SHA-256 of `str(analyzer.folder).encode()` (phase 1; content hashing deferred to ITL-476).

**Tech Stack:** Python 3.12, SpikeInterface ≥ 0.101, PyArrow, Polars, pytest, uv.

---

## Files

| File | Change |
|---|---|
| `src/orcapod/extension_types/spikeinterface_types.py` | Add `SortingAnalyzer` import, `LogicalSISortingAnalyzer`, `SISortingAnalyzerHandler`, update `register_spikeinterface_types()` and module docstring |
| `src/orcapod/extension_types/__init__.py` | Add `LogicalSISortingAnalyzer` and `SISortingAnalyzerHandler` to conditional SI imports and `__all__` |
| `src/orcapod/contexts/data/v0.1.json` | Add `_optional` entries for `LogicalSISortingAnalyzer` and `SISortingAnalyzerHandler`; add changelog entry |
| `tests/test_extension_types/test_spikeinterface_types.py` | Add `_make_sorting_analyzer()` helper + all 8 test cases |

---

## Task 1: Add `LogicalSISortingAnalyzer` class skeleton + importable test

**Files:**
- Modify: `src/orcapod/extension_types/spikeinterface_types.py`
- Test: `tests/test_extension_types/test_spikeinterface_types.py`

- [ ] **Step 1: Write the failing test**

Append to the end of `tests/test_extension_types/test_spikeinterface_types.py`:

```python
# ── SortingAnalyzer helpers ───────────────────────────────────────────────────

def _make_sorting_analyzer(tmp_path, fmt: str):
    """Create a minimal saved SortingAnalyzer for use as a test artifact.

    Args:
        tmp_path: Base directory. Created if it does not exist.
        fmt: ``"binary_folder"`` or ``"zarr"``.

    Returns:
        A ``SortingAnalyzer`` saved to ``tmp_path/analyzer`` (binary_folder)
        or ``tmp_path/analyzer.zarr`` (zarr).
    """
    import spikeinterface.core as si_core
    from pathlib import Path
    tmp_path = Path(tmp_path)
    tmp_path.mkdir(parents=True, exist_ok=True)
    rng = np.random.default_rng(42)
    traces = rng.standard_normal((200, 4)).astype("float32")
    recording = si_core.NumpyRecording([traces], sampling_frequency=30_000)
    spike_trains = {0: np.sort(rng.choice(200, 10, replace=False))}
    sorting = si_core.NumpySorting.from_unit_dict(spike_trains, sampling_frequency=30_000)
    if fmt == "zarr":
        folder = str(tmp_path / "analyzer.zarr")
    else:
        folder = str(tmp_path / "analyzer")
    return si_core.SortingAnalyzer.create(sorting, recording, format=fmt, folder=folder)


# ── LogicalSISortingAnalyzer tests ────────────────────────────────────────────

def test_logical_si_sorting_analyzer_importable():
    """``LogicalSISortingAnalyzer`` exposes the expected extension name, python_type,
    and Arrow storage type."""
    si_core = pytest.importorskip("spikeinterface.core", reason="spikeinterface not installed")
    from orcapod.extension_types.spikeinterface_types import LogicalSISortingAnalyzer
    import pyarrow as pa

    lt = LogicalSISortingAnalyzer()
    assert lt.logical_type_name == "spikeinterface.sorting_analyzer"
    assert lt.python_type is si_core.SortingAnalyzer
    assert lt.get_arrow_extension_type().storage_type == pa.large_string()
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/test_extension_types/test_spikeinterface_types.py::test_logical_si_sorting_analyzer_importable -v
```

Expected: `FAILED` — `ImportError` or `AttributeError` (class does not exist yet).

- [ ] **Step 3: Import `SortingAnalyzer` in `spikeinterface_types.py`**

In `src/orcapod/extension_types/spikeinterface_types.py`, change the import block at lines 48–54:

```python
try:
    from spikeinterface.core import BaseRecording, BaseSorting, SortingAnalyzer
    from spikeinterface.core.motion import Motion
except ImportError as _exc:
    raise ImportError(
        "spikeinterface is not installed. "
        "Install it with: pip install orcapod[spikeinterface]"
    ) from _exc
```

- [ ] **Step 4: Add `LogicalSISortingAnalyzer` class**

Append the following after `class SISortingHandler` (after line 597) and before `def register_spikeinterface_types`:

```python
class LogicalSISortingAnalyzer(BaseLogicalType):
    """Logical type for ``spikeinterface.core.SortingAnalyzer``.

    Stores ``SortingAnalyzer`` instances as Arrow ``large_string`` columns
    tagged with extension name ``"spikeinterface.sorting_analyzer"``. Unlike
    ``BaseRecording`` / ``BaseSorting``, ``SortingAnalyzer`` is not a
    ``BaseExtractor`` subclass and has no ``to_dict()`` round-trip — it is
    exclusively folder-backed (``binary_folder`` or ``zarr``). The stored value
    is a JSON object ``{"folder": "<path>", "format": "<binary_folder|zarr>"}``
    derived from ``analyzer.folder`` and ``analyzer.format``.

    The analyzer must be saved to disk before passing it to orcapod (i.e.
    ``analyzer.folder`` must not be ``None``). In-memory analyzers raise
    ``ValueError`` with save instructions. Loading reconstructs the analyzer
    via ``SortingAnalyzer.load(folder)`` with automatic format detection.

    Example:
        >>> import tempfile, numpy as np
        >>> import spikeinterface.core as si_core
        >>> from orcapod.extension_types.spikeinterface_types import LogicalSISortingAnalyzer
        >>> lt = LogicalSISortingAnalyzer()
        >>> with tempfile.TemporaryDirectory() as tmp:
        ...     recording = si_core.NumpyRecording(
        ...         [np.zeros((200, 4), dtype="float32")], 30000
        ...     )
        ...     sorting = si_core.NumpySorting.from_unit_dict(
        ...         {0: np.array([0, 50, 100])}, sampling_frequency=30000
        ...     )
        ...     analyzer = si_core.SortingAnalyzer.create(
        ...         sorting, recording, format="binary_folder", folder=tmp + "/analyzer"
        ...     )
        ...     storage = lt.python_to_storage(analyzer)
        ...     recovered = lt.storage_to_python(storage)
        ...     recovered.sorting.get_unit_ids().tolist() == analyzer.sorting.get_unit_ids().tolist()
        True
    """

    _arrow_ext_class = make_arrow_extension_type("spikeinterface.sorting_analyzer", pa.large_string())
    _arrow_ext: pa.ExtensionType | None = None
    _polars_ext_class = make_polars_extension_type("spikeinterface.sorting_analyzer", pa.large_string())
    _polars_ext: pl.BaseExtension | None = None

    logical_type_name: str = "spikeinterface.sorting_analyzer"
    python_type: type = SortingAnalyzer

    def get_arrow_extension_type(self) -> pa.ExtensionType:
        """Return the cached Arrow extension type for ``SortingAnalyzer``.

        Returns:
            A ``pa.ExtensionType`` with extension name
            ``"spikeinterface.sorting_analyzer"`` and storage type
            ``pa.large_string()``.
        """
        if LogicalSISortingAnalyzer._arrow_ext is None:
            LogicalSISortingAnalyzer._arrow_ext = LogicalSISortingAnalyzer._arrow_ext_class()
        return LogicalSISortingAnalyzer._arrow_ext

    def get_polars_extension_type(self) -> pl.BaseExtension:
        """Return the cached Polars extension type for ``SortingAnalyzer``.

        Returns:
            A ``pl.BaseExtension`` registered under
            ``"spikeinterface.sorting_analyzer"``.
        """
        if LogicalSISortingAnalyzer._polars_ext is None:
            LogicalSISortingAnalyzer._polars_ext = LogicalSISortingAnalyzer._polars_ext_class()
        return LogicalSISortingAnalyzer._polars_ext

    def python_to_storage(
        self, value: Any, converter: TypeConverterProtocol | None = None
    ) -> str:
        """Serialise a ``SortingAnalyzer`` to its JSON storage representation.

        Args:
            value: A ``SortingAnalyzer`` instance with a non-``None`` ``folder``
                attribute (i.e. saved via ``save_as()`` or created with
                ``format="binary_folder"`` or ``format="zarr"``).
            converter: Ignored. Present for protocol conformance.

        Returns:
            A JSON string ``{"folder": "<path>", "format": "<fmt>"}`` where
            ``<path>`` is ``str(analyzer.folder)`` and ``<fmt>`` is
            ``analyzer.format`` (``"binary_folder"`` or ``"zarr"``).

        Raises:
            ValueError: If ``analyzer.folder`` is ``None`` (in-memory analyzer
                that has not been saved to disk).
        """
        if value.folder is None:
            raise ValueError(
                "SortingAnalyzer has no folder (in-memory). "
                "Call analyzer.save_as(format='binary_folder'|'zarr', folder=<path>) "
                "first, then pass the saved analyzer to the pod."
            )
        return json.dumps({"folder": str(value.folder), "format": value.format})

    def storage_to_python(
        self, storage_value: Any, converter: TypeConverterProtocol | None = None
    ) -> SortingAnalyzer:
        """Reconstruct a ``SortingAnalyzer`` from its JSON storage string.

        Args:
            storage_value: A JSON string as stored in Arrow, with keys
                ``"folder"`` and ``"format"``.
            converter: Ignored. Present for protocol conformance.

        Returns:
            A ``SortingAnalyzer`` instance loaded via
            ``SortingAnalyzer.load(folder)``. The format is auto-detected from
            the folder suffix (``".zarr"`` → zarr, otherwise binary_folder).

        Raises:
            ValueError: If ``storage_value`` is not valid JSON.
            FileNotFoundError: If the analyzer folder no longer exists
                (raised by SpikeInterface, propagated as-is).
        """
        try:
            data = json.loads(storage_value)
        except (json.JSONDecodeError, TypeError) as exc:
            raise ValueError(
                f"LogicalSISortingAnalyzer: cannot deserialise storage value "
                f"{storage_value!r}; expected a JSON string."
            ) from exc
        return SortingAnalyzer.load(data["folder"])
```

- [ ] **Step 5: Run importable test to verify it passes**

```bash
uv run pytest tests/test_extension_types/test_spikeinterface_types.py::test_logical_si_sorting_analyzer_importable -v
```

Expected: `PASSED`

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/extension_types/spikeinterface_types.py \
        tests/test_extension_types/test_spikeinterface_types.py
git commit -m "feat(extension_types): add LogicalSISortingAnalyzer class (ITL-469)"
```

---

## Task 2: `python_to_storage` / `storage_to_python` tests

**Files:**
- Test: `tests/test_extension_types/test_spikeinterface_types.py`

- [ ] **Step 1: Write in-memory guard test + round-trip tests**

Append to `tests/test_extension_types/test_spikeinterface_types.py`:

```python
def test_in_memory_analyzer_raises():
    """In-memory SortingAnalyzer (folder=None) raises ValueError with clear save instructions."""
    si_core = pytest.importorskip("spikeinterface.core", reason="spikeinterface not installed")
    from orcapod.extension_types.spikeinterface_types import LogicalSISortingAnalyzer

    rng = np.random.default_rng(0)
    traces = rng.standard_normal((100, 2)).astype("float32")
    recording = si_core.NumpyRecording([traces], sampling_frequency=30_000)
    sorting = si_core.NumpySorting.from_unit_dict(
        {0: np.array([10, 50, 90])}, sampling_frequency=30_000
    )
    analyzer = si_core.SortingAnalyzer.create(sorting, recording, format="memory")

    lt = LogicalSISortingAnalyzer()
    with pytest.raises(ValueError, match="in-memory"):
        lt.python_to_storage(analyzer)


def test_binary_folder_analyzer_round_trip(tmp_path):
    """binary_folder-backed SortingAnalyzer round-trips through python_to_storage / storage_to_python."""
    pytest.importorskip("spikeinterface", reason="spikeinterface not installed")
    from orcapod.extension_types.spikeinterface_types import LogicalSISortingAnalyzer

    analyzer = _make_sorting_analyzer(tmp_path, "binary_folder")
    lt = LogicalSISortingAnalyzer()

    storage = lt.python_to_storage(analyzer)
    assert isinstance(storage, str)
    data = json.loads(storage)
    assert "folder" in data
    assert data["format"] == "binary_folder"

    recovered = lt.storage_to_python(storage)
    np.testing.assert_array_equal(
        analyzer.sorting.get_unit_spike_train(unit_id=0, segment_index=0),
        recovered.sorting.get_unit_spike_train(unit_id=0, segment_index=0),
    )


def test_zarr_analyzer_round_trip(tmp_path):
    """Zarr-backed SortingAnalyzer round-trips through python_to_storage / storage_to_python."""
    pytest.importorskip("spikeinterface", reason="spikeinterface not installed")
    from orcapod.extension_types.spikeinterface_types import LogicalSISortingAnalyzer

    analyzer = _make_sorting_analyzer(tmp_path, "zarr")
    lt = LogicalSISortingAnalyzer()

    storage = lt.python_to_storage(analyzer)
    assert isinstance(storage, str)
    data = json.loads(storage)
    assert "folder" in data
    assert data["format"] == "zarr"

    recovered = lt.storage_to_python(storage)
    np.testing.assert_array_equal(
        analyzer.sorting.get_unit_spike_train(unit_id=0, segment_index=0),
        recovered.sorting.get_unit_spike_train(unit_id=0, segment_index=0),
    )
```

- [ ] **Step 2: Run tests to verify they pass**

```bash
uv run pytest tests/test_extension_types/test_spikeinterface_types.py \
    -k "test_in_memory_analyzer_raises or test_binary_folder_analyzer_round_trip or test_zarr_analyzer_round_trip" -v
```

Expected: all 3 `PASSED` (the implementation was already added in Task 1 Step 4).

- [ ] **Step 3: Commit**

```bash
git add tests/test_extension_types/test_spikeinterface_types.py
git commit -m "test(extension_types): add LogicalSISortingAnalyzer serialization tests (ITL-469)"
```

---

## Task 3: `SISortingAnalyzerHandler` + handler tests

**Files:**
- Modify: `src/orcapod/extension_types/spikeinterface_types.py`
- Test: `tests/test_extension_types/test_spikeinterface_types.py`

- [ ] **Step 1: Write the failing handler tests**

Append to `tests/test_extension_types/test_spikeinterface_types.py`:

```python
# ── SISortingAnalyzerHandler tests ────────────────────────────────────────────

def test_si_sorting_analyzer_handler_hash_stability(tmp_path):
    """Same SortingAnalyzer produces identical ContentHash across two calls."""
    pytest.importorskip("spikeinterface", reason="spikeinterface not installed")
    from orcapod.extension_types.spikeinterface_types import SISortingAnalyzerHandler
    from orcapod.types import ContentHash

    analyzer = _make_sorting_analyzer(tmp_path, "binary_folder")
    handler = SISortingAnalyzerHandler()

    h1 = handler.handle(analyzer, hasher=None)
    h2 = handler.handle(analyzer, hasher=None)

    assert isinstance(h1, ContentHash)
    assert h1 == h2


def test_si_sorting_analyzer_handler_hash_changes_with_path(tmp_path):
    """SortingAnalyzers at different folder paths produce different ContentHash values."""
    pytest.importorskip("spikeinterface", reason="spikeinterface not installed")
    from orcapod.extension_types.spikeinterface_types import SISortingAnalyzerHandler

    analyzer_a = _make_sorting_analyzer(tmp_path / "a", "binary_folder")
    analyzer_b = _make_sorting_analyzer(tmp_path / "b", "binary_folder")

    handler = SISortingAnalyzerHandler()
    assert handler.handle(analyzer_a, hasher=None) != handler.handle(analyzer_b, hasher=None)


def test_si_sorting_analyzer_handler_in_memory_raises():
    """``SISortingAnalyzerHandler`` raises ValueError for in-memory analyzers."""
    si_core = pytest.importorskip("spikeinterface.core", reason="spikeinterface not installed")
    from orcapod.extension_types.spikeinterface_types import SISortingAnalyzerHandler

    rng = np.random.default_rng(0)
    traces = rng.standard_normal((100, 2)).astype("float32")
    recording = si_core.NumpyRecording([traces], sampling_frequency=30_000)
    sorting = si_core.NumpySorting.from_unit_dict(
        {0: np.array([10, 50, 90])}, sampling_frequency=30_000
    )
    analyzer = si_core.SortingAnalyzer.create(sorting, recording, format="memory")

    handler = SISortingAnalyzerHandler()
    with pytest.raises(ValueError, match="in-memory"):
        handler.handle(analyzer, hasher=None)
```

- [ ] **Step 2: Run handler tests to verify they fail**

```bash
uv run pytest tests/test_extension_types/test_spikeinterface_types.py \
    -k "test_si_sorting_analyzer_handler_hash_stability or test_si_sorting_analyzer_handler_hash_changes_with_path or test_si_sorting_analyzer_handler_in_memory_raises" -v
```

Expected: all 3 `FAILED` — `ImportError` (class does not exist yet).

- [ ] **Step 3: Add `SISortingAnalyzerHandler` class**

In `src/orcapod/extension_types/spikeinterface_types.py`, append after `LogicalSISortingAnalyzer` and before `def register_spikeinterface_types`:

```python
class SISortingAnalyzerHandler:
    """Semantic hash handler for ``spikeinterface.core.SortingAnalyzer``.

    Computes a SHA-256 ``ContentHash`` of the folder path string
    (``str(analyzer.folder).encode()``). This is phase 1 — path-string
    hashing. Content hashing of the folder contents is deferred to ITL-476.

    The ``hasher`` argument is accepted for protocol conformance but not used —
    hashing is done directly via ``hashlib.sha256`` to avoid overhead.
    """

    def handle(self, obj: Any, hasher: SemanticHasherProtocol | None) -> ContentHash:
        """Return a SHA-256 ``ContentHash`` of the analyzer's folder path string.

        Args:
            obj: A ``SortingAnalyzer`` instance.
            hasher: Accepted for protocol conformance; not used.

        Returns:
            A ``ContentHash`` with ``method="sha256"`` and digest equal to the
            SHA-256 of ``str(analyzer.folder).encode()``.

        Raises:
            ValueError: If ``analyzer.folder`` is ``None`` (in-memory analyzer).
        """
        if obj.folder is None:
            raise ValueError(
                "Cannot hash in-memory SortingAnalyzer (folder is None). "
                "Call save_as() first."
            )
        folder_bytes = str(obj.folder).encode()
        logger.debug("SISortingAnalyzerHandler: hashing folder path %r", str(obj.folder))
        return ContentHash(
            method="sha256",
            digest=hashlib.sha256(folder_bytes).digest(),
        )
```

- [ ] **Step 4: Run handler tests to verify they pass**

```bash
uv run pytest tests/test_extension_types/test_spikeinterface_types.py \
    -k "test_si_sorting_analyzer_handler_hash_stability or test_si_sorting_analyzer_handler_hash_changes_with_path or test_si_sorting_analyzer_handler_in_memory_raises" -v
```

Expected: all 3 `PASSED`

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/extension_types/spikeinterface_types.py \
        tests/test_extension_types/test_spikeinterface_types.py
git commit -m "feat(extension_types): add SISortingAnalyzerHandler (ITL-469)"
```

---

## Task 4: Wire into `register_spikeinterface_types()` + registration test

**Files:**
- Modify: `src/orcapod/extension_types/spikeinterface_types.py`
- Test: `tests/test_extension_types/test_spikeinterface_types.py`

- [ ] **Step 1: Write the failing registration test**

Append to `tests/test_extension_types/test_spikeinterface_types.py`:

```python
def test_register_spikeinterface_types_includes_sorting_analyzer(tmp_path):
    """``register_spikeinterface_types()`` wires ``LogicalSISortingAnalyzer`` and
    ``SISortingAnalyzerHandler`` into the default context so they are found by type lookup."""
    pytest.importorskip("spikeinterface", reason="spikeinterface not installed")
    from orcapod.extension_types.spikeinterface_types import register_spikeinterface_types
    from orcapod.contexts import get_default_context

    register_spikeinterface_types()
    ctx = get_default_context()

    analyzer = _make_sorting_analyzer(tmp_path, "binary_folder")

    # LogicalType registered: type_converter can find it
    arrow_type = ctx.type_converter.python_type_to_arrow_type(type(analyzer))
    assert arrow_type.extension_name == "spikeinterface.sorting_analyzer"

    # Handler registered: semantic_hasher can find it
    from orcapod.types import ContentHash
    handler = ctx.semantic_hasher.type_handler_registry.get_handler(analyzer)
    assert handler is not None
    result = handler.handle(analyzer, hasher=None)
    assert isinstance(result, ContentHash)

    # Full round-trip through type_converter
    storage = ctx.type_converter.python_to_storage(analyzer, type(analyzer))
    recovered = ctx.type_converter.storage_to_python(storage, type(analyzer))
    np.testing.assert_array_equal(
        analyzer.sorting.get_unit_spike_train(unit_id=0, segment_index=0),
        recovered.sorting.get_unit_spike_train(unit_id=0, segment_index=0),
    )
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/test_extension_types/test_spikeinterface_types.py::test_register_spikeinterface_types_includes_sorting_analyzer -v
```

Expected: `FAILED` — `AssertionError` because `register_spikeinterface_types` does not yet register `LogicalSISortingAnalyzer`.

- [ ] **Step 3: Add 4th block to `register_spikeinterface_types()`**

In `src/orcapod/extension_types/spikeinterface_types.py`, find `def register_spikeinterface_types`. Replace the last 2 lines (the Motion handler registration and the closing of the function body) with:

Current end of function (lines ~681–683):
```python
    # Handler registration silently replaces an existing entry, so always safe.
    context.semantic_hasher.type_handler_registry.register(Motion, SIMotionHandler())
```

Replace with:
```python
    # Handler registration silently replaces an existing entry, so always safe.
    context.semantic_hasher.type_handler_registry.register(Motion, SIMotionHandler())

    # --- SortingAnalyzer ---
    lt_sorting_analyzer = LogicalSISortingAnalyzer()
    try:
        context.type_converter.register_logical_type(lt_sorting_analyzer)
    except ValueError as exc:
        # A different LogicalSISortingAnalyzer instance is already registered (e.g.
        # auto-registered from v0.1.json at context creation time). That is
        # fine — both instances are equivalent. Any other ValueError propagates.
        if "already bound to" not in str(exc):
            raise
        logger.debug(
            "register_spikeinterface_types: LogicalSISortingAnalyzer already registered, skipping"
        )
    else:
        logger.debug("register_spikeinterface_types: registered LogicalSISortingAnalyzer")

    # Handler registration silently replaces an existing entry, so always safe.
    context.semantic_hasher.type_handler_registry.register(
        SortingAnalyzer, SISortingAnalyzerHandler()
    )
```

Also update the docstring of `register_spikeinterface_types` — change the first line:

```python
    """Register SpikeInterface LogicalTypes into an orcapod ``DataContext``.

    Registers ``LogicalSIRecording`` / ``SIRecordingHandler`` (ITL-459),
    ``LogicalSISorting`` / ``SISortingHandler`` (ITL-468),
    ``LogicalSIMotion`` / ``SIMotionHandler`` (ITL-470), and
    ``LogicalSISortingAnalyzer`` / ``SISortingAnalyzerHandler`` (ITL-469).
```

- [ ] **Step 4: Run test to verify it passes**

```bash
uv run pytest tests/test_extension_types/test_spikeinterface_types.py::test_register_spikeinterface_types_includes_sorting_analyzer -v
```

Expected: `PASSED`

- [ ] **Step 5: Run all SI tests to check no regressions**

```bash
uv run pytest tests/test_extension_types/test_spikeinterface_types.py -v
```

Expected: all tests `PASSED`

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/extension_types/spikeinterface_types.py \
        tests/test_extension_types/test_spikeinterface_types.py
git commit -m "feat(extension_types): wire LogicalSISortingAnalyzer into register_spikeinterface_types (ITL-469)"
```

---

## Task 5: Wire into `__init__.py` and `v0.1.json`

**Files:**
- Modify: `src/orcapod/extension_types/__init__.py`
- Modify: `src/orcapod/contexts/data/v0.1.json`

- [ ] **Step 1: Update `__init__.py` conditional imports**

In `src/orcapod/extension_types/__init__.py`, change the SI try-block (lines 34–44):

```python
# ITL-459, ITL-468, ITL-470, ITL-469 — SpikeInterface support (optional; requires pip install orcapod[spikeinterface])
try:
    from .spikeinterface_types import (
        LogicalSIRecording,
        LogicalSISorting,
        LogicalSIMotion,
        LogicalSISortingAnalyzer,
        SIMotionHandler,
        SISortingAnalyzerHandler,
        register_spikeinterface_types,
    )
    _SI_AVAILABLE = True
except ImportError:
    _SI_AVAILABLE = False
```

Change the `__all__` SI conditional block (lines 73–82):

```python
    # ITL-459, ITL-468, ITL-470, ITL-469 (conditional — only present when spikeinterface is installed)
    *(
        [
            "LogicalSIRecording",
            "LogicalSISorting",
            "LogicalSIMotion",
            "LogicalSISortingAnalyzer",
            "SIMotionHandler",
            "SISortingAnalyzerHandler",
            "register_spikeinterface_types",
        ] if _SI_AVAILABLE else []
    ),
```

- [ ] **Step 2: Add `LogicalSISortingAnalyzer` entry to `v0.1.json` logical_types**

In `src/orcapod/contexts/data/v0.1.json`, in the `"logical_types"` array, add after the `LogicalSIMotion` entry (after line 62):

```json
        {
            "_class": "orcapod.extension_types.spikeinterface_types.LogicalSISortingAnalyzer",
            "_config": {},
            "_optional": true
        },
```

- [ ] **Step 3: Add `SISortingAnalyzerHandler` entry to `v0.1.json` handlers**

In `src/orcapod/contexts/data/v0.1.json`, in the `"handlers"` array, add after the `SIMotionHandler` entry (after line 122):

```json
                [{"_type": "spikeinterface.core.SortingAnalyzer", "_optional": true}, {"_class": "orcapod.extension_types.spikeinterface_types.SISortingAnalyzerHandler", "_config": {}, "_optional": true}],
```

- [ ] **Step 4: Add changelog entry to `v0.1.json`**

In `src/orcapod/contexts/data/v0.1.json`, append to the `"changelog"` array:

```json
            "Added spikeinterface.core.SortingAnalyzer as a native value type via LogicalSISortingAnalyzer (large_string/JSON path reference {folder, format}) and SISortingAnalyzerHandler (SHA-256 of folder path string, phase 1); auto-registered when spikeinterface is installed via _optional entries in v0.1.json (ITL-469)"
```

- [ ] **Step 5: Update module docstring in `spikeinterface_types.py`**

In `src/orcapod/extension_types/spikeinterface_types.py`, change the first line of the module docstring from:

```python
"""SpikeInterface LogicalTypes and handlers for orcapod (ITL-459, ITL-468, ITL-470).
```

to:

```python
"""SpikeInterface LogicalTypes and handlers for orcapod (ITL-459, ITL-468, ITL-470, ITL-469).
```

Also add a paragraph after the Motion description:

```
``LogicalSISortingAnalyzer`` maps ``spikeinterface.core.SortingAnalyzer`` ↔ Arrow
``large_string`` using a JSON path-reference object
``{"folder": "<path>", "format": "<binary_folder|zarr>"}`` derived from
``analyzer.folder`` and ``analyzer.format``. Unlike Recording/Sorting, there is
no ``to_dict()`` round-trip — the analyzer is exclusively folder-backed.
``SISortingAnalyzerHandler`` hashes the folder path string via SHA-256 (phase 1).
```

- [ ] **Step 6: Run the full SI test suite**

```bash
uv run pytest tests/test_extension_types/test_spikeinterface_types.py -v
```

Expected: all tests `PASSED`

- [ ] **Step 7: Run the full test suite**

```bash
uv run pytest tests/ -x -q
```

Expected: all tests `PASSED` (no regressions)

- [ ] **Step 8: Commit**

```bash
git add src/orcapod/extension_types/__init__.py \
        src/orcapod/contexts/data/v0.1.json \
        src/orcapod/extension_types/spikeinterface_types.py
git commit -m "feat(extension_types): support SpikeInterface SortingAnalyzer objects (ITL-469)"
```
