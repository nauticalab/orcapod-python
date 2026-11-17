# SpikeInterface BaseSorting LogicalType Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `LogicalSISorting` and `SISortingHandler` to orcapod so pods can natively accept and return SpikeInterface `BaseSorting` objects without manual save/reload scaffolding.

**Architecture:** Mirror the existing `LogicalSIRecording` / `SIRecordingHandler` pattern from `spikeinterface_types.py` (ITL-459). `BaseSorting` and `BaseRecording` share the same `BaseExtractor` serialization API, so the implementation is structurally identical — different class names, extension name `"spikeinterface.sorting"`, and sorting-specific error messages. Auto-registration in `v0.1.json` with `_optional: true` activates the types whenever `spikeinterface` is installed.

**Tech Stack:** Python 3.12, PyArrow, Polars, SpikeInterface >= 0.101, pytest, `uv run` for all commands.

---

## File Map

| File | Action | Responsibility |
|---|---|---|
| `src/orcapod/extension_types/spikeinterface_types.py` | Modify (lines 34–36, append after line 268) | Add `LogicalSISorting`, `SISortingHandler`; extend `register_spikeinterface_types()` |
| `src/orcapod/extension_types/__init__.py` | Modify (lines 35–38, 67–68) | Export `LogicalSISorting` from conditional import block |
| `src/orcapod/contexts/data/v0.1.json` | Modify (lines 49–52, 110, 147–148) | Add `_optional` entries for sorting LogicalType and handler; add changelog entry |
| `tests/test_extension_types/test_spikeinterface_types.py` | Modify (append) | Add sorting-specific tests |

---

### Task 1: Add `LogicalSISorting` and `SISortingHandler` to `spikeinterface_types.py`

**Files:**
- Modify: `src/orcapod/extension_types/spikeinterface_types.py:34-36` (import line) and append after line 268

- [ ] **Step 1: Write the failing import test**

Add this test to `tests/test_extension_types/test_spikeinterface_types.py`:

```python
def test_logical_si_sorting_importable():
    si_core = pytest.importorskip("spikeinterface.core", reason="spikeinterface not installed")
    from orcapod.extension_types.spikeinterface_types import LogicalSISorting
    import pyarrow as pa

    lt = LogicalSISorting()
    assert lt.logical_type_name == "spikeinterface.sorting"
    assert lt.python_type is si_core.BaseSorting
    assert lt.get_arrow_extension_type().storage_type == pa.large_string()
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/test_extension_types/test_spikeinterface_types.py::test_logical_si_sorting_importable -v
```

Expected: FAIL with `ImportError: cannot import name 'LogicalSISorting'`

- [ ] **Step 3: Update the `spikeinterface.core` import line**

In `src/orcapod/extension_types/spikeinterface_types.py`, replace line 35:

```python
# Before (line 35):
    from spikeinterface.core import BaseRecording

# After:
    from spikeinterface.core import BaseRecording, BaseSorting
```

- [ ] **Step 4: Append `LogicalSISorting` class after line 168 (after `LogicalSIRecording` class ends)**

Append after the `LogicalSIRecording` class (before `class SIRecordingHandler`), i.e., insert between lines 168 and 171:

```python

class LogicalSISorting(BaseLogicalType):
    """Logical type for ``spikeinterface.core.BaseSorting``.

    Stores ``BaseSorting`` instances as Arrow ``large_string`` columns
    tagged with extension name ``"spikeinterface.sorting"``. The stored
    value is SpikeInterface's own ``to_dict(recursive=True,
    include_annotations=True, include_properties=False)`` output, encoded
    via ``SIJsonEncoder``. Loading reconstructs the sorting via
    ``spikeinterface.core.load(dict)``.

    Only sortings whose ``check_serializability("json")`` returns ``True``
    are accepted. File-backed sortings (zarr, numpy_folder, npz_folder,
    sorter folder) qualify. In-memory ``NumpySorting`` objects do not and
    raise ``ValueError`` with clear save instructions.

    Example:
        >>> import tempfile, numpy as np
        >>> import spikeinterface.core as si
        >>> from orcapod.extension_types.spikeinterface_types import LogicalSISorting
        >>> lt = LogicalSISorting()
        >>> with tempfile.TemporaryDirectory() as tmp:
        ...     sorting = si.NumpySorting.from_unit_dict(
        ...         {0: np.array([0, 100, 200])}, sampling_frequency=30000
        ...     )
        ...     saved = sorting.save_to_folder(tmp + "/sorting")
        ...     storage = lt.python_to_storage(saved)
        ...     recovered = lt.storage_to_python(storage)
        ...     saved.get_unit_ids().tolist() == recovered.get_unit_ids().tolist()
        True
    """

    _arrow_ext_class = make_arrow_extension_type("spikeinterface.sorting", pa.large_string())
    _arrow_ext: pa.ExtensionType | None = None
    _polars_ext_class = make_polars_extension_type("spikeinterface.sorting", pa.large_string())
    _polars_ext: pl.BaseExtension | None = None

    logical_type_name: str = "spikeinterface.sorting"
    python_type: type = BaseSorting

    def get_arrow_extension_type(self) -> pa.ExtensionType:
        """Return the cached Arrow extension type for ``BaseSorting``.

        Returns:
            A ``pa.ExtensionType`` with extension name
            ``"spikeinterface.sorting"`` and storage type ``pa.large_string()``.
        """
        if LogicalSISorting._arrow_ext is None:
            LogicalSISorting._arrow_ext = LogicalSISorting._arrow_ext_class()
        return LogicalSISorting._arrow_ext

    def get_polars_extension_type(self) -> pl.BaseExtension:
        """Return the cached Polars extension type for ``BaseSorting``.

        Returns:
            A ``pl.BaseExtension`` registered under ``"spikeinterface.sorting"``.
        """
        if LogicalSISorting._polars_ext is None:
            LogicalSISorting._polars_ext = LogicalSISorting._polars_ext_class()
        return LogicalSISorting._polars_ext

    def python_to_storage(
        self, value: Any, converter: TypeConverterProtocol | None = None
    ) -> str:
        """Serialise a ``BaseSorting`` to its JSON storage representation.

        Args:
            value: A ``BaseSorting`` instance whose
                ``check_serializability("json")`` returns ``True``.
            converter: Ignored. Present for protocol conformance.

        Returns:
            A JSON string produced by ``sorting.to_dict(recursive=True,
            include_annotations=True, include_properties=False)`` encoded
            via ``SIJsonEncoder``.

        Raises:
            ValueError: If the sorting is not JSON-serialisable (e.g. an
                in-memory ``NumpySorting``).
        """
        if not value.check_serializability("json"):
            raise ValueError(
                "This BaseSorting is not JSON-serializable and cannot be stored "
                "by orcapod. This typically means it holds data in memory (e.g. "
                "NumpySorting). Sortings built on top of file-backed data "
                "(zarr, numpy_folder, npz_folder, etc.) are fine and do not need "
                "to be materialized first. If your sorting is in-memory, call "
                "sorting.save_to_zarr(path) or sorting.save_to_folder(path) "
                "first, then pass the returned extractor to the pod."
            )
        from spikeinterface.core.core_tools import SIJsonEncoder
        return json.dumps(
            value.to_dict(
                include_annotations=True,
                include_properties=False,
                recursive=True,
            ),
            cls=SIJsonEncoder,
        )

    def storage_to_python(
        self, storage_value: Any, converter: TypeConverterProtocol | None = None
    ) -> BaseSorting:
        """Reconstruct a ``BaseSorting`` from its JSON storage string.

        Args:
            storage_value: A JSON string as stored in Arrow.
            converter: Ignored. Present for protocol conformance.

        Returns:
            A ``BaseSorting`` instance reconstructed via
            ``spikeinterface.core.load``.

        Raises:
            ValueError: If ``storage_value`` is not valid JSON.
            FileNotFoundError: If the backing zarr/folder no longer exists
                (raised by SpikeInterface, propagated as-is).
        """
        from spikeinterface.core import load as si_load
        try:
            si_dict = json.loads(storage_value)
        except (json.JSONDecodeError, TypeError) as exc:
            raise ValueError(
                f"LogicalSISorting: cannot deserialise storage value "
                f"{storage_value!r}; expected a JSON string."
            ) from exc
        return si_load(si_dict)
```

- [ ] **Step 5: Run test to verify it passes**

```bash
uv run pytest tests/test_extension_types/test_spikeinterface_types.py::test_logical_si_sorting_importable -v
```

Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/extension_types/spikeinterface_types.py tests/test_extension_types/test_spikeinterface_types.py
git commit -m "feat(extension_types): add LogicalSISorting class (ITL-468)"
```

---

### Task 2: Add `SISortingHandler`

**Files:**
- Modify: `src/orcapod/extension_types/spikeinterface_types.py` (append after `SIRecordingHandler`)

- [ ] **Step 1: Write the failing handler tests**

Append to `tests/test_extension_types/test_spikeinterface_types.py`:

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


def test_si_sorting_handler_hash_stability(tmp_path):
    """Same sorting produces identical ContentHash across two calls."""
    pytest.importorskip("spikeinterface", reason="spikeinterface not installed")
    from orcapod.extension_types.spikeinterface_types import SISortingHandler
    from orcapod.types import ContentHash

    saved = _make_numpy_sorting().save_to_folder(str(tmp_path / "sorting"))
    handler = SISortingHandler()

    h1 = handler.handle(saved, hasher=None)
    h2 = handler.handle(saved, hasher=None)

    assert isinstance(h1, ContentHash)
    assert h1 == h2


def test_si_sorting_handler_hash_changes_with_content(tmp_path):
    """Different sortings produce different ContentHash values."""
    si_core = pytest.importorskip("spikeinterface.core", reason="spikeinterface not installed")
    from orcapod.extension_types.spikeinterface_types import SISortingHandler

    rng = np.random.default_rng(0)
    n_samples = 1000
    sorting_a = si_core.NumpySorting.from_unit_dict(
        {0: np.sort(rng.choice(n_samples, 20, replace=False))},
        sampling_frequency=30_000,
    ).save_to_folder(str(tmp_path / "sorting_a"))
    sorting_b = si_core.NumpySorting.from_unit_dict(
        {0: np.sort(rng.choice(n_samples, 20, replace=False))},
        sampling_frequency=30_000,
    ).save_to_folder(str(tmp_path / "sorting_b"))

    handler = SISortingHandler()
    assert handler.handle(sorting_a, hasher=None) != handler.handle(sorting_b, hasher=None)


def test_si_sorting_handler_in_memory_raises():
    """SISortingHandler raises ValueError for in-memory sortings."""
    pytest.importorskip("spikeinterface", reason="spikeinterface not installed")
    from orcapod.extension_types.spikeinterface_types import SISortingHandler

    sorting = _make_numpy_sorting()
    handler = SISortingHandler()
    with pytest.raises(ValueError, match="in-memory"):
        handler.handle(sorting, hasher=None)
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/test_extension_types/test_spikeinterface_types.py::test_si_sorting_handler_hash_stability tests/test_extension_types/test_spikeinterface_types.py::test_si_sorting_handler_hash_changes_with_content tests/test_extension_types/test_spikeinterface_types.py::test_si_sorting_handler_in_memory_raises -v
```

Expected: FAIL with `ImportError: cannot import name 'SISortingHandler'`

- [ ] **Step 3: Append `SISortingHandler` class to `spikeinterface_types.py`**

Insert after `SIRecordingHandler` class (after line 221), before `register_spikeinterface_types`:

```python

class SISortingHandler:
    """Semantic hash handler for ``spikeinterface.core.BaseSorting``.

    Computes a SHA-256 ``ContentHash`` of the JSON bytes produced by
    ``sorting.to_dict(recursive=True, include_annotations=True,
    include_properties=False)`` encoded via ``SIJsonEncoder``. This is
    identical to the bytes that ``LogicalSISorting`` stores in Arrow, so
    hash input and storage representation are always consistent.

    The ``hasher`` argument is accepted for protocol conformance but not used —
    hashing is done directly via ``hashlib.sha256`` to avoid overhead.
    """

    def handle(self, obj: Any, hasher: SemanticHasherProtocol | None) -> ContentHash:
        """Return a SHA-256 ``ContentHash`` of the sorting's JSON dump.

        Args:
            obj: A ``BaseSorting`` instance.
            hasher: Accepted for protocol conformance; not used.

        Returns:
            A ``ContentHash`` with ``method="sha256"`` and digest equal to the
            SHA-256 of the JSON bytes from ``to_dict(recursive=True,
            include_annotations=True, include_properties=False)`` encoded
            via ``SIJsonEncoder``.

        Raises:
            TypeError: If ``obj`` is not a ``BaseSorting``.
            ValueError: If the sorting is not JSON-serialisable (in-memory).
        """
        if not isinstance(obj, BaseSorting):
            raise TypeError(
                f"SISortingHandler: expected BaseSorting, got {type(obj)!r}"
            )
        if not obj.check_serializability("json"):
            raise ValueError(
                "Cannot hash an in-memory BaseSorting "
                "(check_serializability('json') is False). "
                "Save it to disk first with save_to_zarr() or save_to_folder()."
            )
        # TODO(ITL-468): phase 2 — also hash backing source directory contents
        from spikeinterface.core.core_tools import SIJsonEncoder
        json_bytes = json.dumps(
            obj.to_dict(include_annotations=True, include_properties=False, recursive=True),
            cls=SIJsonEncoder,
        ).encode()
        logger.debug("SISortingHandler: hashing %d JSON bytes", len(json_bytes))
        return ContentHash(
            method="sha256",
            digest=hashlib.sha256(json_bytes).digest(),
        )
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/test_extension_types/test_spikeinterface_types.py::test_si_sorting_handler_hash_stability tests/test_extension_types/test_spikeinterface_types.py::test_si_sorting_handler_hash_changes_with_content tests/test_extension_types/test_spikeinterface_types.py::test_si_sorting_handler_in_memory_raises -v
```

Expected: all 3 PASS

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/extension_types/spikeinterface_types.py tests/test_extension_types/test_spikeinterface_types.py
git commit -m "feat(extension_types): add SISortingHandler (ITL-468)"
```

---

### Task 3: Add serialization tests for `LogicalSISorting`

**Files:**
- Modify: `tests/test_extension_types/test_spikeinterface_types.py` (append)

- [ ] **Step 1: Write the failing serialization tests**

Append to `tests/test_extension_types/test_spikeinterface_types.py`:

```python
def test_in_memory_sorting_raises():
    """NumpySorting (json=False) raises ValueError with clear instructions."""
    pytest.importorskip("spikeinterface", reason="spikeinterface not installed")
    from orcapod.extension_types.spikeinterface_types import LogicalSISorting

    sorting = _make_numpy_sorting()
    lt = LogicalSISorting()

    with pytest.raises(ValueError, match="not JSON-serializable"):
        lt.python_to_storage(sorting)

    with pytest.raises(ValueError, match="file-backed"):
        lt.python_to_storage(sorting)


def test_folder_sorting_round_trip(tmp_path):
    """numpy_folder-backed sorting round-trips through python_to_storage / storage_to_python."""
    pytest.importorskip("spikeinterface", reason="spikeinterface not installed")
    from orcapod.extension_types.spikeinterface_types import LogicalSISorting

    saved = _make_numpy_sorting().save_to_folder(str(tmp_path / "sorting"))
    lt = LogicalSISorting()

    storage = lt.python_to_storage(saved)
    assert isinstance(storage, str)
    data = json.loads(storage)
    assert "class" in data  # SI dict always has a "class" key

    recovered = lt.storage_to_python(storage)
    np.testing.assert_array_equal(
        saved.get_unit_spike_train(unit_id=0, segment_index=0),
        recovered.get_unit_spike_train(unit_id=0, segment_index=0),
    )


def test_zarr_sorting_round_trip(tmp_path):
    """Zarr-backed sorting round-trips through python_to_storage / storage_to_python."""
    pytest.importorskip("spikeinterface", reason="spikeinterface not installed")
    from orcapod.extension_types.spikeinterface_types import LogicalSISorting

    saved = _make_numpy_sorting().save_to_zarr(str(tmp_path / "sorting.zarr"))
    lt = LogicalSISorting()

    storage = lt.python_to_storage(saved)
    assert isinstance(storage, str)

    recovered = lt.storage_to_python(storage)
    np.testing.assert_array_equal(
        saved.get_unit_spike_train(unit_id=0, segment_index=0),
        recovered.get_unit_spike_train(unit_id=0, segment_index=0),
    )
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/test_extension_types/test_spikeinterface_types.py::test_in_memory_sorting_raises tests/test_extension_types/test_spikeinterface_types.py::test_folder_sorting_round_trip tests/test_extension_types/test_spikeinterface_types.py::test_zarr_sorting_round_trip -v
```

Expected: `test_in_memory_sorting_raises` PASS (class exists from Task 1), `test_folder_sorting_round_trip` and `test_zarr_sorting_round_trip` PASS — if all pass now, skip to Step 4.

- [ ] **Step 3: Run full test file to confirm no regressions**

```bash
uv run pytest tests/test_extension_types/test_spikeinterface_types.py -v
```

Expected: all tests PASS

- [ ] **Step 4: Commit**

```bash
git add tests/test_extension_types/test_spikeinterface_types.py
git commit -m "test(extension_types): add LogicalSISorting serialization round-trip tests (ITL-468)"
```

---

### Task 4: Extend `register_spikeinterface_types()` to include sorting

**Files:**
- Modify: `src/orcapod/extension_types/spikeinterface_types.py:224-268` (`register_spikeinterface_types`)

- [ ] **Step 1: Write the failing registration test**

Append to `tests/test_extension_types/test_spikeinterface_types.py`:

```python
def test_register_spikeinterface_types_includes_sorting(tmp_path):
    """register_spikeinterface_types() wires LogicalSISorting and SISortingHandler
    into the default context so they are found by type lookup."""
    pytest.importorskip("spikeinterface", reason="spikeinterface not installed")
    from orcapod.extension_types.spikeinterface_types import register_spikeinterface_types
    from orcapod.contexts import get_default_context

    register_spikeinterface_types()
    ctx = get_default_context()

    saved = _make_numpy_sorting().save_to_folder(str(tmp_path / "sorting"))

    # LogicalType registered: type_converter can find it
    arrow_type = ctx.type_converter.python_type_to_arrow_type(type(saved))
    assert arrow_type.extension_name == "spikeinterface.sorting"

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
        saved.get_unit_spike_train(unit_id=0, segment_index=0),
        recovered.get_unit_spike_train(unit_id=0, segment_index=0),
    )
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/test_extension_types/test_spikeinterface_types.py::test_register_spikeinterface_types_includes_sorting -v
```

Expected: FAIL — `arrow_type.extension_name` will be `None` or raise because `BaseSorting` is not yet registered.

- [ ] **Step 3: Extend `register_spikeinterface_types()` in `spikeinterface_types.py`**

Replace the entire `register_spikeinterface_types` function body (currently lines 224–268) with:

```python
def register_spikeinterface_types(context: Any = None) -> None:
    """Register SpikeInterface LogicalTypes into an orcapod ``DataContext``.

    Registers both ``LogicalSIRecording`` / ``SIRecordingHandler`` (ITL-459)
    and ``LogicalSISorting`` / ``SISortingHandler`` (ITL-468).

    For the default context this is called automatically at startup (the
    default ``v0.1.json`` context config lists all four with ``"_optional": true``,
    so they are wired in whenever ``spikeinterface`` is installed). Call this
    function explicitly only when working with a custom ``DataContext`` that was
    not constructed from the default config.

    If ``context`` is ``None``, the default context (from
    ``orcapod.contexts.get_default_context()``) is used. The function is
    idempotent — calling it more than once on the same context is safe.

    Args:
        context: A ``DataContext`` instance, or ``None`` to use the default.

    Example:
        >>> from orcapod.extension_types.spikeinterface_types import register_spikeinterface_types
        >>> register_spikeinterface_types()  # no-op if default context already has SI types
    """
    if context is None:
        from orcapod.contexts import get_default_context
        context = get_default_context()

    # --- Recording ---
    lt_recording = LogicalSIRecording()
    try:
        context.type_converter.register_logical_type(lt_recording)
    except ValueError as exc:
        if "already bound to" not in str(exc):
            raise
        logger.debug(
            "register_spikeinterface_types: LogicalSIRecording already registered, skipping"
        )
    else:
        logger.debug("register_spikeinterface_types: registered LogicalSIRecording")

    context.semantic_hasher.type_handler_registry.register(BaseRecording, SIRecordingHandler())

    # --- Sorting ---
    lt_sorting = LogicalSISorting()
    try:
        context.type_converter.register_logical_type(lt_sorting)
    except ValueError as exc:
        if "already bound to" not in str(exc):
            raise
        logger.debug(
            "register_spikeinterface_types: LogicalSISorting already registered, skipping"
        )
    else:
        logger.debug("register_spikeinterface_types: registered LogicalSISorting")

    context.semantic_hasher.type_handler_registry.register(BaseSorting, SISortingHandler())
```

- [ ] **Step 4: Run new test to verify it passes**

```bash
uv run pytest tests/test_extension_types/test_spikeinterface_types.py::test_register_spikeinterface_types_includes_sorting -v
```

Expected: PASS

- [ ] **Step 5: Run full test file to confirm no regressions**

```bash
uv run pytest tests/test_extension_types/test_spikeinterface_types.py -v
```

Expected: all tests PASS

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/extension_types/spikeinterface_types.py tests/test_extension_types/test_spikeinterface_types.py
git commit -m "feat(extension_types): extend register_spikeinterface_types() for BaseSorting (ITL-468)"
```

---

### Task 5: Update `extension_types/__init__.py` to export `LogicalSISorting`

**Files:**
- Modify: `src/orcapod/extension_types/__init__.py:35-38, 67-68`

- [ ] **Step 1: Update the conditional import block**

In `src/orcapod/extension_types/__init__.py`, replace lines 34–38:

```python
# Before:
# ITL-459 — SpikeInterface support (optional; requires pip install orcapod[spikeinterface])
try:
    from .spikeinterface_types import LogicalSIRecording, register_spikeinterface_types
    _SI_AVAILABLE = True
except ImportError:
    _SI_AVAILABLE = False
```

```python
# After:
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

- [ ] **Step 2: Update `__all__` to include `LogicalSISorting`**

In `src/orcapod/extension_types/__init__.py`, replace lines 67–68:

```python
# Before:
    # ITL-459 (conditional — only present when spikeinterface is installed)
    *( ["LogicalSIRecording", "register_spikeinterface_types"] if _SI_AVAILABLE else [] ),
```

```python
# After:
    # ITL-459, ITL-468 (conditional — only present when spikeinterface is installed)
    *( ["LogicalSIRecording", "LogicalSISorting", "register_spikeinterface_types"] if _SI_AVAILABLE else [] ),
```

- [ ] **Step 3: Verify the export is accessible**

```bash
uv run python -c "
from orcapod.extension_types import LogicalSISorting, LogicalSIRecording, register_spikeinterface_types
print('LogicalSISorting:', LogicalSISorting)
print('OK')
"
```

Expected: prints `LogicalSISorting: <class '...LogicalSISorting'>` and `OK`

- [ ] **Step 4: Run full test suite to confirm no regressions**

```bash
uv run pytest tests/test_extension_types/ -v
```

Expected: all tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/extension_types/__init__.py
git commit -m "feat(extension_types): export LogicalSISorting from extension_types package (ITL-468)"
```

---

### Task 6: Update `v0.1.json` to auto-register sorting types

**Files:**
- Modify: `src/orcapod/contexts/data/v0.1.json`

- [ ] **Step 1: Add `LogicalSISorting` to the logical types list**

In `src/orcapod/contexts/data/v0.1.json`, after the `LogicalSIRecording` entry (lines 48–52):

```json
        {
            "_class": "orcapod.extension_types.spikeinterface_types.LogicalSIRecording",
            "_config": {},
            "_optional": true
        },
```

Add immediately after:

```json
        {
            "_class": "orcapod.extension_types.spikeinterface_types.LogicalSISorting",
            "_config": {},
            "_optional": true
        },
```

- [ ] **Step 2: Add `SISortingHandler` to the handler list**

In `src/orcapod/contexts/data/v0.1.json`, after the `SIRecordingHandler` entry (line 110):

```json
                [{"_type": "spikeinterface.core.BaseRecording", "_optional": true}, {"_class": "orcapod.extension_types.spikeinterface_types.SIRecordingHandler", "_config": {}, "_optional": true}],
```

Add immediately after:

```json
                [{"_type": "spikeinterface.core.BaseSorting", "_optional": true}, {"_class": "orcapod.extension_types.spikeinterface_types.SISortingHandler", "_config": {}, "_optional": true}],
```

- [ ] **Step 3: Add changelog entry**

In `src/orcapod/contexts/data/v0.1.json`, append to the `"changelog"` array (after the last entry):

```json
            "Added spikeinterface.BaseSorting as a native value type via LogicalSISorting (large_string/JSON) and SISortingHandler (SHA-256 of JSON bytes); auto-registered when spikeinterface is installed via _optional entries in v0.1.json (ITL-468)"
```

- [ ] **Step 4: Verify JSON is valid**

```bash
uv run python -c "import json; json.load(open('src/orcapod/contexts/data/v0.1.json')); print('JSON valid')"
```

Expected: `JSON valid`

- [ ] **Step 5: Verify auto-registration works from a fresh context**

```bash
uv run python -c "
# Reset default context to force reload from config
import orcapod.contexts as ctx_mod
ctx_mod._default_context = None  # force fresh construction if attr exists

from orcapod.contexts import get_default_context
ctx = get_default_context()

import spikeinterface.core as si
import numpy as np
rng = np.random.default_rng(0)
import tempfile
with tempfile.TemporaryDirectory() as tmp:
    sorting = si.NumpySorting.from_unit_dict(
        {0: np.sort(rng.choice(1000, 10, replace=False))},
        sampling_frequency=30000
    ).save_to_folder(tmp + '/s')
    arrow_type = ctx.type_converter.python_type_to_arrow_type(type(sorting))
    print('extension_name:', arrow_type.extension_name)
    assert arrow_type.extension_name == 'spikeinterface.sorting', arrow_type.extension_name
    print('Auto-registration OK')
"
```

Expected: prints `extension_name: spikeinterface.sorting` and `Auto-registration OK`

- [ ] **Step 6: Run full test suite**

```bash
uv run pytest tests/test_extension_types/test_spikeinterface_types.py -v
```

Expected: all tests PASS

- [ ] **Step 7: Commit**

```bash
git add src/orcapod/contexts/data/v0.1.json
git commit -m "feat(contexts): auto-register LogicalSISorting and SISortingHandler in v0.1.json (ITL-468)"
```

---

### Task 7: Final integration — run the full test suite

**Files:** None (verification only)

- [ ] **Step 1: Run the full test suite**

```bash
uv run pytest tests/ -v --tb=short 2>&1 | tail -30
```

Expected: all tests PASS, no regressions.

- [ ] **Step 2: If any test fails, investigate and fix before proceeding**

Common failure modes:
- `already bound to` error in registration tests → idempotency guard working, check test isolation
- `FileNotFoundError` in round-trip tests → `tmp_path` is cleaned up; ensure `saved` stays in scope for the full test
- Arrow extension type conflict → two `make_arrow_extension_type` calls with the same name create the same class; class-level cache (`_arrow_ext`) prevents double-registration

- [ ] **Step 3: Push branch and open PR**

```bash
git push -u origin eywalker/itl-468-support-spikeinterface-basesorting-objects
gh pr create \
  --base main \
  --title "feat(extension_types): support SpikeInterface BaseSorting objects (ITL-468)" \
  --body "$(cat <<'EOF'
## Summary

- Adds `LogicalSISorting` logical type mapping `BaseSorting` ↔ Arrow `large_string` via SpikeInterface's `to_dict()` JSON dump
- Adds `SISortingHandler` for SHA-256 content hashing (phase 1)
- Extends `register_spikeinterface_types()` to also wire in sorting types
- Auto-registers both types in `v0.1.json` with `_optional: true`
- Round-trip tests for numpy_folder-backed and zarr-backed sortings; clear error for in-memory `NumpySorting`

Closes ITL-468

## Test plan
- [ ] `uv run pytest tests/test_extension_types/test_spikeinterface_types.py -v` — all pass
- [ ] `uv run pytest tests/ -v` — no regressions

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```
