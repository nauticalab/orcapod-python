"""ITL-897 claim (d): which golden entries the proposed pandas hash moves.

Runs the golden generator twice into scratch directories, once as released and once with the
prototypes in ``prototype_v1.py`` patched in, each in its own process, and lists every key
whose value differs, for each variant in ``VARIANTS``. ``tests/test_hashing/golden/data`` is never written.

Run: ``uv run python superpowers/scripts/itl_897/claim_d_golden_diff.py``
"""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
HERE = Path(__file__).resolve().parent
GENERATOR = ROOT / "tests/test_hashing/golden/generate_golden_hashes.py"

VARIANTS = {
    "pandas": "pandas_dataframe_v1 / pandas_series_v1 prototype only",
    "pandas_full": "full-descriptor pandas_dataframe_v1 / pandas_series_v1 (claim i)",
    "table_naive": "arrow_table_v1 in ArrowTableHandler, datagrams and streams unchanged",
    "table_separated": "arrow_table_v1 for values; datagram and stream identities separated",
    "both_separated": "pandas prototype plus separated arrow_table_v1",
}

RUNNER = """
import runpy, sys
sys.path.insert(0, {here!r})
import prototype_v1
if {patch!r} in ("pandas", "both_separated"):
    prototype_v1.install()
if {patch!r} == "pandas_full":
    prototype_v1.install_full_descriptor()
if {patch!r} == "table_naive":
    prototype_v1.install_arrow_table(separated=False)
if {patch!r} in ("table_separated", "both_separated"):
    prototype_v1.install_arrow_table(separated=True)
sys.argv = [{gen!r}, "--output-dir", {out!r}]
runpy.run_path({gen!r}, run_name="__main__")
"""


def _generate(out: Path, patch: str) -> dict[str, dict[str, object]]:
    code = RUNNER.format(here=str(HERE), patch=patch, gen=str(GENERATOR), out=str(out))
    subprocess.run(
        [sys.executable, "-c", code], check=True, cwd=ROOT, capture_output=True
    )
    return {p.stem: json.loads(p.read_text()) for p in sorted(out.glob("*.json"))}


def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        released = _generate(Path(tmp) / "released", patch="none")
        proposed = {
            variant: _generate(Path(tmp) / variant, patch=variant)
            for variant in VARIANTS
        }
    pinned = {
        p.stem: json.loads(p.read_text())
        for p in sorted((ROOT / "tests/test_hashing/golden/data").glob("*.json"))
    }
    assert released == pinned, "released run does not reproduce the pinned golden files"
    total = sum(len(v) for v in pinned.values())
    for variant, description in VARIANTS.items():
        moved = [
            (group, key)
            for group, entries in pinned.items()
            for key, value in entries.items()
            if proposed[variant][group].get(key) != value
        ]
        by_group: dict[str, int] = {}
        for group, _ in moved:
            by_group[group] = by_group.get(group, 0) + 1
        print(f"\n## {variant}: {description}")
        print(
            f"{len(moved)} of {total} pinned golden entries move; by file: {by_group}"
        )
        for group, key in moved[:12]:
            print(f"  {group}.json  {key}")
        if len(moved) > 12:
            print(f"  ... and {len(moved) - 12} more")


if __name__ == "__main__":
    main()
