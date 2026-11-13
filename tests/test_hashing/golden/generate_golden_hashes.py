"""Regenerate the golden hash files checked by ``test_golden_hashes.py`` (ITL-626).

Usage::

    uv run python tests/test_hashing/golden/generate_golden_hashes.py

writes ``tests/test_hashing/golden/data/<group>.json`` for every group in
``cases.GROUPS``.

READ THIS BEFORE REGENERATING
=============================

Every value in the golden files is a hash that Orcapod stores, uses as a cache or database
key, or embeds in a column name or a database table path. A changed value means data written
by an earlier release is no longer found, is keyed differently, or is read under another
column name. **A changed value is a breaking change.**

Regenerating is legitimate only when:

1. You ADD a case or entry (existing values must come out unchanged; check the diff), or
2. A hash change is intentional, has a written migration assessment (which stored data it
   strands and how users recover it), and the developer has approved it. Name the approving
   decision and the migration plan in the PR description.

Never regenerate to make a failing test pass without that approval. If a test fails and you
did not mean to change a hash, the code change is the bug.

Values must be identical on every supported Python version (3.11, 3.12, 3.13) and in every
process, whatever ``PYTHONHASHSEED`` is. A value that differs by interpreter is a defect to
fix, never something to pin per version.

Not pinned, because starfix rejects their data (ITL-848): ``null``-typed and map columns.
Since starfix 0.5 (hash format 2, label ``arrow_v1``, ITL-910), ``hash_schema`` on extension
columns, the canonical ``arrow.uuid`` and ``arrow.fixed_shape_tensor`` types, and
fixed-size-list values are hashable and pinned.

Comparing releases
------------------

To list the values an older checkout produces (for release notes), run this script with
that checkout's environment and this checkout's test package on the path::

    uv run --directory <old-checkout> python <this-checkout>/tests/test_hashing/golden/generate_golden_hashes.py \\
        --allow-errors --output-dir <scratch-dir>

then diff ``<scratch-dir>`` against ``tests/test_hashing/golden/data``. ``--allow-errors``
records ``ERROR: ...`` for a case the old API cannot express instead of stopping.
"""

from __future__ import annotations

import argparse
import json
import pathlib
import sys
from typing import Any

# Put the project root on sys.path so the cases import as ``tests.test_hashing.golden.cases``,
# the same module name pytest uses: function pods and dataclasses hash with their module path.
_PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[3]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from tests.test_hashing.golden import cases  # noqa: E402

DATA_DIR = pathlib.Path(__file__).resolve().parent / "data"


def golden_path(group: str, data_dir: pathlib.Path = DATA_DIR) -> pathlib.Path:
    """Return the golden file path for ``group``.

    Args:
        group: One of ``cases.GROUPS``.
        data_dir: Directory holding the golden files.

    Returns:
        The path ``<data_dir>/<group>.json``.
    """
    return data_dir / f"{group}.json"


def dump(values: dict[str, Any]) -> str:
    """Serialize one group's entries the way the golden files are stored.

    Args:
        values: Entries of one group.

    Returns:
        Indented JSON with sorted keys and a trailing newline.
    """
    return json.dumps(values, indent=2, sort_keys=True, ensure_ascii=False) + "\n"


def main(argv: list[str] | None = None) -> None:
    """Compute the golden groups and write (or print) them.

    Args:
        argv: Command-line arguments; ``None`` reads ``sys.argv``.
    """
    parser = argparse.ArgumentParser(
        description="Regenerate the golden hash files (ITL-626)."
    )
    _ = parser.add_argument("--output-dir", type=pathlib.Path, default=DATA_DIR)
    _ = parser.add_argument("--group", choices=cases.GROUPS, action="append")
    _ = parser.add_argument("--allow-errors", action="store_true")
    _ = parser.add_argument(
        "--stdout",
        action="store_true",
        help="print all groups as one JSON object instead of writing files",
    )
    args = parser.parse_args(argv)

    unavailable = [g for g in args.group or [] if not cases.group_available(g)]
    if unavailable:
        parser.error(
            f"group(s) {unavailable} need an optional dependency that is not installed "
            f"({[cases.OPTIONAL_GROUPS[g] for g in unavailable]})"
        )
    groups = args.group or list(cases.available_groups())
    skipped = [
        g for g in cases.GROUPS if g not in groups and not cases.group_available(g)
    ]
    if skipped and not args.stdout:
        # Never rewrite an optional group's file without its dependency (ITL-871).
        print(
            f"skipped {skipped}: optional dependency not installed; files left as they are"
        )
    computed = {
        g: cases.compute_group(g, allow_errors=args.allow_errors) for g in groups
    }
    if args.stdout:
        sys.stdout.write(dump(computed))
        return
    args.output_dir.mkdir(parents=True, exist_ok=True)
    for group, values in computed.items():
        path = golden_path(group, args.output_dir)
        missing = cases.unavailable_entries(group)
        if missing:
            # Keep the pins this environment cannot compute (an older optional
            # dependency), from the target file or else the repository's, so a
            # golden file is never written without them.
            source = path if path.exists() else golden_path(group)
            pinned = (
                json.loads(source.read_text(encoding="utf-8"))
                if source.exists()
                else {}
            )
            lost = sorted(e for e in missing if e not in pinned)
            if lost:
                raise SystemExit(
                    f"cannot write {path}: this environment cannot compute {lost} "
                    "and no existing golden file pins them; install the newer "
                    "optional dependency"
                )
            values = {**values, **{e: pinned[e] for e in missing}}
        path.write_text(dump(values), encoding="utf-8")
        print(f"wrote {len(values)} entries to {path}")


if __name__ == "__main__":
    main()
