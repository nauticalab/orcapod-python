"""Render the harness results as Markdown tables.

Usage (repository root):
    uv run python superpowers/specs/itl-892-exit-hang/summarize.py

Reads ``results.jsonl`` (macOS runs) and ``linux/*/results.jsonl`` (downloaded from the
temporary CI probe). The last entry wins when a label was run more than once.
"""

from __future__ import annotations

import json
from pathlib import Path

HERE = Path(__file__).resolve().parent


def load(path: Path) -> dict[str, dict[str, object]]:
    """Load a ``results.jsonl`` file, keeping the last entry for each label.

    Args:
        path: The file.

    Returns:
        Entries keyed by label.
    """
    rows: dict[str, dict[str, object]] = {}
    for line in path.read_text().splitlines():
        if line.strip():
            entry = json.loads(line)
            rows[str(entry["label"])] = entry
    return rows


def render(title: str, rows: dict[str, dict[str, object]]) -> str:
    """Render one platform's entries as a Markdown table.

    Args:
        title: Heading for the table.
        rows: Entries keyed by label.

    Returns:
        The Markdown.
    """
    out = [
        f"### {title}",
        "",
        "| cell | runs | hung | aborted (rc<0) | other rc!=0 | failed |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for label, e in rows.items():
        rc: dict[str, int] = e["rc"]  # pyright: ignore[reportAssignmentType]
        hung = rc.get("HUNG", 0)
        aborted = sum(
            v for k, v in rc.items() if k.lstrip("-").isdigit() and int(k) < 0
        )
        other = sum(v for k, v in rc.items() if k.isdigit() and int(k) > 0)
        n = int(e["n"])  # pyright: ignore[reportArgumentType]
        failed = hung + aborted + other
        out.append(
            f"| {label} | {n} | {hung} | {aborted} | {other} | **{100 * failed / n:.0f}%** |"
        )
    return "\n".join(out) + "\n"


def main() -> None:
    """Print the hang-rate tables for macOS and each Linux round."""
    print(render("macOS arm64 (this machine)", load(HERE / "results.jsonl")))
    for path in sorted((HERE / "linux").glob("*/results.jsonl")):
        print(
            render(
                f"Linux x86_64, GitHub ubuntu-latest ({path.parent.name})", load(path)
            )
        )


if __name__ == "__main__":
    main()
