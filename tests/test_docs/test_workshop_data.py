"""The workshop dataset generator must be deterministic, small and runnable.

``docs/tutorial/workshop_data.py`` is what every tutorial page and every
workshop attendee runs first, so it is tested like code.
"""

from __future__ import annotations

import csv
import importlib.util
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
GENERATOR = REPO_ROOT / "docs" / "tutorial" / "workshop_data.py"


def _load_generator():
    spec = importlib.util.spec_from_file_location("workshop_data", GENERATOR)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _snapshot(root: Path) -> dict[str, bytes]:
    return {
        str(p.relative_to(root)): p.read_bytes()
        for p in sorted(root.rglob("*"))
        if p.is_file()
    }


def _rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def test_two_runs_are_byte_identical(tmp_path: Path):
    make_dataset = _load_generator().make_dataset
    first = _snapshot(make_dataset(tmp_path / "a"))
    second = _snapshot(make_dataset(tmp_path / "b"))
    assert first == second
    assert len(first) == 27


def test_rerun_in_place_changes_nothing(tmp_path: Path):
    make_dataset = _load_generator().make_dataset
    before = _snapshot(make_dataset(tmp_path / "a"))
    assert _snapshot(make_dataset(tmp_path / "a")) == before


def test_dataset_is_small(tmp_path: Path):
    files = _snapshot(_load_generator().make_dataset(tmp_path / "a"))
    assert sum(len(b) for b in files.values()) < 2 * 1024 * 1024


def test_tables_have_the_documented_shape(tmp_path: Path):
    root = _load_generator().make_dataset(tmp_path / "a")
    subjects = _rows(root / "subjects.csv")
    sessions = _rows(root / "sessions.csv")
    units = _rows(root / "units.csv")
    assert list(subjects[0]) == ["subject_id", "sex", "age_weeks", "genotype"]
    assert list(sessions[0]) == ["subject_id", "session", "date", "duration_s"]
    assert list(units[0]) == [
        "subject_id",
        "session",
        "probe",
        "unit",
        "region",
        "depth_um",
        "n_spikes",
    ]
    assert (len(subjects), len(sessions), len(units)) == (4, 8, 64)


def test_unit_table_agrees_with_spike_files(tmp_path: Path):
    """``n_spikes`` in units.csv is the row count of that unit in its spike file."""
    root = _load_generator().make_dataset(tmp_path / "a")
    for unit in _rows(root / "units.csv"):
        spike_file = (
            root
            / "sessions"
            / f"{unit['subject_id']}_{unit['session']}"
            / f"spikes_{unit['probe']}.csv"
        )
        counted = sum(1 for s in _rows(spike_file) if s["unit"] == unit["unit"])
        assert counted == int(unit["n_spikes"])


def test_command_line_writes_the_dataset(tmp_path: Path):
    proc = subprocess.run(
        [sys.executable, str(GENERATOR)],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert proc.returncode == 0, proc.stderr
    assert "Wrote 27 files" in proc.stdout
    assert (tmp_path / "workshop_data" / "subjects.csv").is_file()
