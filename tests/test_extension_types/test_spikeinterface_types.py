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
