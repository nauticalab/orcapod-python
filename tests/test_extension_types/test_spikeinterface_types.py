"""Tests for LogicalSIRecording, LogicalSISorting, and their handlers (ITL-459, ITL-468)."""

from __future__ import annotations

import json

import numpy as np
import pytest


def test_spikeinterface_not_installed_raises_import_error(monkeypatch):
    """Importing spikeinterface_types when SI is absent raises ImportError.

    This test runs regardless of whether spikeinterface is installed — it
    simulates absence by blocking the import via sys.modules.
    """
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
            # Re-import only if SI is genuinely available; if not, there is
            # nothing to restore and the ImportError is expected.
            try:
                importlib.import_module(si_types_key)
            except ImportError:
                pass


def _make_numpy_recording():
    """Create a small in-memory NumpyRecording for use as a test source."""
    import spikeinterface.core as si_core
    rng = np.random.default_rng(42)
    traces = rng.standard_normal((200, 4)).astype("float32")
    return si_core.NumpyRecording([traces], sampling_frequency=30_000)


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


def test_logical_si_recording_importable():
    si_core = pytest.importorskip("spikeinterface.core", reason="spikeinterface not installed")
    from orcapod.extension_types.spikeinterface_types import LogicalSIRecording
    import pyarrow as pa

    lt = LogicalSIRecording()
    assert lt.logical_type_name == "spikeinterface.recording"
    assert lt.python_type is si_core.BaseRecording
    assert lt.get_arrow_extension_type().storage_type == pa.large_string()


def test_in_memory_recording_raises():
    """NumpyRecording (json=False) raises ValueError with clear instructions."""
    pytest.importorskip("spikeinterface", reason="spikeinterface not installed")
    from orcapod.extension_types.spikeinterface_types import LogicalSIRecording

    rec = _make_numpy_recording()
    lt = LogicalSIRecording()

    with pytest.raises(ValueError, match="not JSON-serializable"):
        lt.python_to_storage(rec)

    # The error message must distinguish in-memory from lazy file-backed recordings
    with pytest.raises(ValueError, match="file-backed"):
        lt.python_to_storage(rec)


def test_folder_recording_round_trip(tmp_path):
    """Binary-folder-backed recording round-trips through python_to_storage / storage_to_python."""
    pytest.importorskip("spikeinterface", reason="spikeinterface not installed")
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
    pytest.importorskip("spikeinterface", reason="spikeinterface not installed")
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
    SpikeInterface re-applies the preprocessing chain lazily (using zscore
    which requires no optional dependencies and is fully deterministic).
    """
    pytest.importorskip("spikeinterface", reason="spikeinterface not installed")
    import spikeinterface.preprocessing as spre
    from orcapod.extension_types.spikeinterface_types import LogicalSIRecording

    # Save the source to disk; apply a lazy preprocessing step on top
    source = _make_numpy_recording().save_to_folder(str(tmp_path / "source"))
    preprocessed = spre.zscore(source)

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


def test_si_recording_handler_hash_stability(tmp_path):
    """Same recording produces identical ContentHash across two calls."""
    pytest.importorskip("spikeinterface", reason="spikeinterface not installed")
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
    si_core = pytest.importorskip("spikeinterface.core", reason="spikeinterface not installed")
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
    pytest.importorskip("spikeinterface", reason="spikeinterface not installed")
    from orcapod.extension_types.spikeinterface_types import SIRecordingHandler

    rec = _make_numpy_recording()
    handler = SIRecordingHandler()
    with pytest.raises(ValueError, match="in-memory"):
        handler.handle(rec, hasher=None)


def test_logical_si_sorting_importable():
    """``LogicalSISorting`` is importable and exposes the expected extension name and storage type."""
    si_core = pytest.importorskip("spikeinterface.core", reason="spikeinterface not installed")
    from orcapod.extension_types.spikeinterface_types import LogicalSISorting
    import pyarrow as pa

    lt = LogicalSISorting()
    assert lt.logical_type_name == "spikeinterface.sorting"
    assert lt.python_type is si_core.BaseSorting
    assert lt.get_arrow_extension_type().storage_type == pa.large_string()


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
    """``SISortingHandler`` raises ValueError for in-memory sortings."""
    pytest.importorskip("spikeinterface", reason="spikeinterface not installed")
    from orcapod.extension_types.spikeinterface_types import SISortingHandler

    sorting = _make_numpy_sorting()
    handler = SISortingHandler()
    with pytest.raises(ValueError, match="in-memory"):
        handler.handle(sorting, hasher=None)


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


def test_register_spikeinterface_types(tmp_path):
    """register_spikeinterface_types() wires LogicalSIRecording and SIRecordingHandler
    into the default context so they are found by type lookup."""
    pytest.importorskip("spikeinterface", reason="spikeinterface not installed")
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


def test_register_spikeinterface_types_includes_sorting(tmp_path):
    """``register_spikeinterface_types()`` wires ``LogicalSISorting`` and ``SISortingHandler``
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


def test_logical_si_motion_python_to_storage_type_error():
    """LogicalSIMotion.python_to_storage raises TypeError for non-Motion input."""
    pytest.importorskip("spikeinterface", reason="spikeinterface not installed")
    from orcapod.extension_types.spikeinterface_types import LogicalSIMotion

    lt = LogicalSIMotion()
    with pytest.raises(TypeError, match="LogicalSIMotion"):
        lt.python_to_storage("not a motion")


def test_si_motion_storage_to_python_missing_key():
    """storage_to_python raises ValueError when .npz is missing an expected key."""
    pytest.importorskip("spikeinterface", reason="spikeinterface not installed")
    import io
    import numpy as np
    from orcapod.extension_types.spikeinterface_types import LogicalSIMotion

    # Build a valid .npz but deliberately omit 'num_segments'
    buf = io.BytesIO()
    np.savez(buf, spatial_bins_um=np.array([0.0, 1.0]), direction=np.array(["y"]))
    truncated_bytes = buf.getvalue()

    lt = LogicalSIMotion()
    with pytest.raises(ValueError, match="missing expected key"):
        lt.storage_to_python(truncated_bytes)


def test_si_motion_storage_to_python_invalid_bytes():
    """storage_to_python raises ValueError when passed bytes that are not a .npz archive."""
    pytest.importorskip("spikeinterface", reason="spikeinterface not installed")
    from orcapod.extension_types.spikeinterface_types import LogicalSIMotion

    lt = LogicalSIMotion()
    with pytest.raises(ValueError, match="cannot deserialise"):
        lt.storage_to_python(b"this is not a npz archive at all")


def test_register_spikeinterface_types_motion_fresh_registration():
    """register_spikeinterface_types() takes the else-branch (first-time registration)
    when LogicalSIMotion is not already in the context."""
    pytest.importorskip("spikeinterface", reason="spikeinterface not installed")
    from unittest.mock import MagicMock
    from orcapod.extension_types.spikeinterface_types import (
        register_spikeinterface_types,
        LogicalSIMotion,
        SIMotionHandler,
    )

    # A fully mock context: register_logical_type returns normally (no ValueError)
    # which causes the else-branch (line 679) to execute for all three types.
    mock_ctx = MagicMock()
    register_spikeinterface_types(context=mock_ctx)

    # Verify LogicalSIMotion was passed to register_logical_type
    registered_types = [
        call.args[0].__class__
        for call in mock_ctx.type_converter.register_logical_type.call_args_list
    ]
    assert LogicalSIMotion in registered_types

    # Verify SIMotionHandler was registered with the handler registry
    from spikeinterface.core.motion import Motion
    handler_calls = mock_ctx.semantic_hasher.type_handler_registry.register.call_args_list
    registered_python_types = [call.args[0] for call in handler_calls]
    assert Motion in registered_python_types


def test_register_spikeinterface_types_motion_reraises_unexpected_error():
    """register_spikeinterface_types() re-raises ValueError from register_logical_type
    when the message does not contain 'already bound to'."""
    pytest.importorskip("spikeinterface", reason="spikeinterface not installed")
    from unittest.mock import MagicMock
    from orcapod.extension_types.spikeinterface_types import (
        register_spikeinterface_types,
        LogicalSIMotion,
    )

    # Side-effect: raise an unexpected ValueError only when called with LogicalSIMotion
    def raise_on_motion(logical_type):
        if isinstance(logical_type, LogicalSIMotion):
            raise ValueError("something completely unexpected happened")

    mock_ctx = MagicMock()
    mock_ctx.type_converter.register_logical_type.side_effect = raise_on_motion

    with pytest.raises(ValueError, match="something completely unexpected"):
        register_spikeinterface_types(context=mock_ctx)


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
    n_chan = 4
    traces = rng.standard_normal((200, n_chan)).astype("float32")
    recording = si_core.NumpyRecording([traces], sampling_frequency=30_000)
    # SortingAnalyzer.create requires a probe to be attached to the recording
    locations = np.column_stack([np.zeros(n_chan), np.arange(n_chan) * 25.0])
    recording.set_dummy_probe_from_locations(locations)
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


def test_in_memory_analyzer_raises():
    """In-memory SortingAnalyzer (folder=None) raises ValueError with clear save instructions."""
    si_core = pytest.importorskip("spikeinterface.core", reason="spikeinterface not installed")
    from orcapod.extension_types.spikeinterface_types import LogicalSISortingAnalyzer

    rng = np.random.default_rng(0)
    n_chan = 2
    traces = rng.standard_normal((100, n_chan)).astype("float32")
    recording = si_core.NumpyRecording([traces], sampling_frequency=30_000)
    # SortingAnalyzer.create requires a probe to be attached to the recording
    locations = np.column_stack([np.zeros(n_chan), np.arange(n_chan) * 25.0])
    recording.set_dummy_probe_from_locations(locations)
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
