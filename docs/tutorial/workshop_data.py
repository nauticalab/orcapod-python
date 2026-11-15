"""Generate the small synthetic dataset used by the Orcapod workshop tutorial.

The data imitates a tiny extracellular-recording study: four mice, two recording
sessions each, two probes per session. Nothing here is real data. The generator
uses only the Python standard library and a fixed random seed, so every run
produces byte-identical files.

Run it from the directory you want to work in:

    python workshop_data.py

which writes ``./workshop_data/``. Or call it from Python:

    from workshop_data import make_dataset
    data_dir = make_dataset("workshop_data")

Files written (27 files, about 0.4 MB in total):

    workshop_data/
        subjects.csv                  one row per mouse
        sessions.csv                  one row per recording session
        units.csv                     one row per sorted unit, per probe
        sessions/<subject>_<session>/
            notes.txt                 free-text notes for the session
            spikes_<probe>.csv        spike times, one row per spike
"""

from __future__ import annotations

import csv
import random
import sys
from collections.abc import Sequence
from pathlib import Path

SEED = 20261109

SUBJECTS = [
    # subject_id, sex, age_weeks, genotype
    ("mouse_01", "F", 12, "wildtype"),
    ("mouse_02", "M", 14, "wildtype"),
    ("mouse_03", "F", 11, "knockout"),
    ("mouse_04", "M", 13, "knockout"),
]
SESSIONS = ["s1", "s2"]
PROBES = ["imec0", "imec1"]
REGIONS = {"imec0": "V1", "imec1": "CA1"}
UNITS_PER_PROBE = 4


def _write_csv(path: Path, header: list[str], rows: Sequence[Sequence[object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow(header)
        writer.writerows(rows)


def make_dataset(root: str | Path = "workshop_data") -> Path:
    """Write the workshop dataset under ``root`` and return its path.

    Existing files are overwritten with identical content, so calling this
    again is harmless.

    Args:
        root: Directory to create. Defaults to ``./workshop_data``.

    Returns:
        The dataset directory as a ``pathlib.Path``.
    """
    root = Path(root)
    rng = random.Random(SEED)

    _write_csv(
        root / "subjects.csv",
        ["subject_id", "sex", "age_weeks", "genotype"],
        [list(s) for s in SUBJECTS],
    )

    session_rows: list[list[object]] = []
    unit_rows: list[list[object]] = []
    for subject_id, _sex, _age, genotype in SUBJECTS:
        for i, session in enumerate(SESSIONS):
            duration_s = rng.choice([60.0, 90.0, 120.0])
            session_rows.append(
                [subject_id, session, f"2026-10-{5 + 7 * i:02d}", duration_s]
            )
            session_dir = root / "sessions" / f"{subject_id}_{session}"
            session_dir.mkdir(parents=True, exist_ok=True)
            (session_dir / "notes.txt").write_text(
                f"subject: {subject_id}\nsession: {session}\n"
                f"genotype: {genotype}\nduration_s: {duration_s}\n",
                encoding="utf-8",
                newline="\n",
            )
            for probe in PROBES:
                spikes: list[tuple[float, int]] = []
                for unit in range(UNITS_PER_PROBE):
                    # Knockout animals fire a little less in this made-up study.
                    base = rng.uniform(2.0, 12.0)
                    rate_hz = base * (0.7 if genotype == "knockout" else 1.0)
                    n_spikes = max(1, round(rate_hz * duration_s))
                    depth_um = rng.randrange(100, 3800, 20)
                    unit_rows.append(
                        [
                            subject_id,
                            session,
                            probe,
                            unit,
                            REGIONS[probe],
                            depth_um,
                            n_spikes,
                        ]
                    )
                    spikes.extend(
                        (round(rng.uniform(0.0, duration_s), 4), unit)
                        for _ in range(n_spikes)
                    )
                spikes.sort()
                _write_csv(
                    session_dir / f"spikes_{probe}.csv",
                    ["time_s", "unit"],
                    [list(s) for s in spikes],
                )

    _write_csv(
        root / "sessions.csv",
        ["subject_id", "session", "date", "duration_s"],
        session_rows,
    )
    _write_csv(
        root / "units.csv",
        ["subject_id", "session", "probe", "unit", "region", "depth_um", "n_spikes"],
        unit_rows,
    )
    return root


def main(argv: list[str]) -> int:
    """Command-line entry point: ``python workshop_data.py [directory]``."""
    root = make_dataset(argv[1] if len(argv) > 1 else "workshop_data")
    files = [p for p in root.rglob("*") if p.is_file()]
    size_kb = sum(p.stat().st_size for p in files) / 1024
    print(f"Wrote {len(files)} files ({size_kb:.0f} kB) to {root.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
