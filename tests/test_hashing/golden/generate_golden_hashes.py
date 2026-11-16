"""Add, check and release the golden hash files checked by ``test_golden_hashes.py``.

Usage::

    uv run python tests/test_hashing/golden/generate_golden_hashes.py            # add
    uv run python tests/test_hashing/golden/generate_golden_hashes.py --check    # report
    uv run python tests/test_hashing/golden/generate_golden_hashes.py --release 0.3.0
    uv run python tests/test_hashing/golden/generate_golden_hashes.py --remove <entry>

The golden values of the current data context live in
``tests/test_hashing/golden/data/<folder>/<group>-NN.json``, and the manifest of the data
context (``src/orcapod/contexts/data/<version>.json``) lists every file, with the sha256 of
its bytes, in its ``golden.history`` (see ``store.py`` and ``README.md`` in this directory).

READ THIS BEFORE REGENERATING
=============================

Every value in the golden files is a hash that Orcapod stores, uses as a cache or database
key, or embeds in a column name or a database table path. A changed value means data written
by an earlier release is no longer found, is keyed differently, or is read under another
column name. **A changed value is a breaking change.**

The rules (developer, November 15, 2026, ITL-946):

1. A golden file listed by a released revision never changes and is never removed. This
   script never rewrites one.
2. Adding values is allowed in any release. ``--add`` (the default) writes the entries the
   cases compute and no file pins yet: into the newest file of their group when no release
   lists that file yet, otherwise into a new ``<group>-NN.json`` with the next counter. It
   records the file in the open revision of the history (appending one after a release).
   Existing values are never rewritten, so check that the diff only adds.
3. A change to a released value has no exemption, not even a logical type's own identifier
   change: it needs a new data context version, with a new manifest and a new golden folder,
   shipped only in a minor (before 1.0) or major release. The old context's golden files and
   code move to ``orcapod-sanctuary``.
4. Before a release, a value no release lists may change only with the developer's approval
   and a written migration assessment: ``--remove <entry>`` takes the entry out of its
   unreleased file and records that file again in the open revision (it refuses an entry a
   release lists), then ``--add`` writes the new value.

Never edit a golden file to make a failing test pass. If a test fails and you did not mean to
change a hash, the code change is the bug.

Values must be identical on every supported Python version (3.11, 3.12, 3.13) and in every
process, whatever ``PYTHONHASHSEED`` is. A value that differs by interpreter is a defect to
fix, never something to pin per version.

Not pinned, because starfix rejects their data (ITL-848): ``null``-typed and map columns.
Since starfix 0.5 (hash format 2, label ``arrow_v1``, ITL-910), ``hash_schema`` on extension
columns, the canonical ``arrow.uuid`` and ``arrow.fixed_shape_tensor`` types, and
fixed-size-list values are hashable and pinned.

Releasing
---------

``--release X.Y.Z`` is the golden step of a release pull request: it appends the release
revision (``added`` empty), pins its digest under ``releases`` in
``src/orcapod/contexts/released_manifests.json``, and, at the data context's first release,
sets ``released_in`` and pins the digest of the manifest without ``golden``. It computes no
hash. Try it on a copy first with ``--root`` (``store.copy_tree`` makes one).

Comparing releases
------------------

To list the values an older checkout produces (for release notes), run this script with
that checkout's environment and this checkout's test package on the path::

    uv run --directory <old-checkout> python <this-checkout>/tests/test_hashing/golden/generate_golden_hashes.py \\
        --stdout --allow-errors > <scratch-file>

and compare ``<scratch-file>`` with the golden folder. ``--allow-errors`` records
``ERROR: ...`` for a case the old API cannot express instead of stopping.
"""

from __future__ import annotations

import argparse
import datetime
import hashlib
import json
import pathlib
import sys
from typing import Any

# Put the project root on sys.path so the cases import as ``tests.test_hashing.golden.cases``,
# the same module name pytest uses: function pods and dataclasses hash with their module path.
_PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[3]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from orcapod.contexts import get_default_context  # noqa: E402
from tests.test_hashing.golden import cases, store  # noqa: E402


def dump(values: dict[str, Any]) -> str:
    """Serialize one golden file's entries the way the golden files are stored.

    Args:
        values: Entries of one file.

    Returns:
        Indented JSON with sorted keys and a trailing newline.
    """
    return json.dumps(values, indent=2, sort_keys=True, ensure_ascii=False) + "\n"


def _today() -> str:
    return datetime.date.today().isoformat()


def _require_sound(root: pathlib.Path, version: str) -> None:
    """Stop when the tree already breaks the golden rules; nothing is written then."""
    problems = store.tree_problems(root, version)
    if problems:
        raise SystemExit(
            "the golden history does not match the files; nothing written:\n  "
            + "\n  ".join(problems)
        )


def add(root: pathlib.Path, version: str, groups: list[str]) -> list[str]:
    """Write the computed entries that no golden file pins yet.

    Args:
        root: Project root.
        version: Data context version.
        groups: Groups to compute.

    Returns:
        Names of the files written.
    """
    _require_sound(root, version)
    path = store.manifest_path(root, version)
    manifest = store.read_json(path)
    folder = store.golden_folder(root, manifest)
    pinned = store.load_folder(folder)
    owner = {entry: group for group, values in pinned.items() for entry in values}
    files = store.group_files(folder)
    history = manifest["golden"]["history"]
    released = store.released_files(history)
    # Compute and plan every group before writing anything, so a failure leaves the files
    # and their history as they were.
    planned: list[tuple[pathlib.Path, bytes, int]] = []
    for group in groups:
        live = cases.compute_group(group)
        clash = sorted(e for e in live if owner.get(e, group) != group)
        if clash:
            raise SystemExit(f"{group} entries pinned under another group: {clash}")
        new = {e: v for e, v in live.items() if e not in owner}
        if not new:
            continue
        # An entry name appears once in a folder: claim it before the next group plans.
        owner.update(dict.fromkeys(new, group))
        existing = files.get(group, [])
        latest = existing[-1] if existing else None
        if latest is not None and latest.name not in released:
            target = latest
            values = {**store.read_json(latest), **new}
        else:
            number = 1
            if latest is not None:
                match = store.FILE_NAME.match(latest.name)
                assert match is not None
                number = int(match["number"]) + 1
            if number > 99:
                raise SystemExit(f"{group} has no free file counter")
            target = folder / f"{group}-{number:02d}.json"
            values = new
        data = dump(values).encode("utf-8")
        history = store.with_file(
            history, target.name, hashlib.sha256(data).hexdigest(), _today()
        )
        planned.append((target, data, len(new)))
    if not planned:
        print("nothing to add: every computed entry is pinned")
        return []
    for target, data, count in planned:
        _ = target.write_bytes(data)
        print(f"added {count} entries to {target}")
    manifest["golden"]["history"] = history
    store.write_json(path, manifest)
    return [target.name for target, _, _ in planned]


def check(root: pathlib.Path, version: str, groups: list[str]) -> list[str]:
    """Compare the computed values with the golden folder; write nothing.

    Args:
        root: Project root.
        version: Data context version.
        groups: Groups to compute.

    Returns:
        One sentence per difference or rule problem.
    """
    problems = store.tree_problems(root, version)
    manifest = store.read_json(store.manifest_path(root, version))
    try:
        pinned = store.load_folder(store.golden_folder(root, manifest))
    except store.GoldenLayoutError as exc:
        return [*problems, str(exc)]
    for group in groups:
        live = cases.compute_group(group)
        pins = pinned.get(group, {})
        skipped = cases.unavailable_entries(group)
        for entry in sorted(set(live) | set(pins)):
            if entry in skipped:
                continue
            if entry not in pins:
                problems.append(f"{group}:{entry} is computed but not pinned")
            elif entry not in live:
                problems.append(f"{group}:{entry} is pinned but no longer produced")
            elif live[entry] != pins[entry]:
                problems.append(
                    f"{group}:{entry} moved: pinned {pins[entry]!r}, live {live[entry]!r}"
                )
    return problems


def remove(root: pathlib.Path, version: str, entries: list[str]) -> list[str]:
    """Remove entries from golden files no release lists, recording the files again.

    The first step of an approved change to an unreleased value: remove the entry, then
    ``--add`` writes its new value. An entry in a file a release lists is refused, and
    nothing is written unless every entry can be removed.

    Args:
        root: Project root.
        version: Data context version.
        entries: Entry names to remove.

    Returns:
        Names of the files rewritten.
    """
    _require_sound(root, version)
    path = store.manifest_path(root, version)
    manifest = store.read_json(path)
    folder = store.golden_folder(root, manifest)
    history = manifest["golden"]["history"]
    released = store.released_files(history)
    where = {
        entry: file
        for files in store.group_files(folder).values()
        for file in files
        for entry in store.read_json(file)
    }
    unknown = sorted(e for e in entries if e not in where)
    if unknown:
        raise SystemExit(f"no golden file pins {unknown}; nothing written")
    frozen = sorted(e for e in entries if where[e].name in released)
    if frozen:
        raise SystemExit(
            f"{frozen} are in files a release lists; a released value never changes "
            "(a change needs a new data context version); nothing written"
        )
    planned: dict[pathlib.Path, dict[str, Any]] = {}
    for entry in dict.fromkeys(entries):
        file = where[entry]
        values = planned.setdefault(file, store.read_json(file))
        _ = values.pop(entry)
    writes: list[tuple[pathlib.Path, bytes]] = []
    for file, values in planned.items():
        data = dump(values).encode("utf-8")
        history = store.with_file(
            history, file.name, hashlib.sha256(data).hexdigest(), _today()
        )
        writes.append((file, data))
    for file, data in writes:
        _ = file.write_bytes(data)
        print(f"removed entries from {file}")
    manifest["golden"]["history"] = history
    store.write_json(path, manifest)
    return [file.name for file, _ in writes]


def release(root: pathlib.Path, version: str, release_text: str) -> str:
    """Record a library release in the golden history and the pins.

    Args:
        root: Project root.
        version: Data context version.
        release_text: The library version, e.g. ``0.3.0``.

    Returns:
        The digest of the release revision.
    """
    try:
        name = store.normalize_release(release_text)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    _require_sound(root, version)
    path = store.manifest_path(root, version)
    manifest = store.read_json(path)
    pins = store.read_json(store.pins_path(root))
    pin = pins[version]
    if name in pin["releases"]:
        raise SystemExit(f"{name} is already released")
    history = manifest["golden"]["history"]
    if not history:
        raise SystemExit("the golden history lists no file")
    history = store.with_release(history, name, _today())
    manifest["golden"]["history"] = history
    if manifest["released_in"] is None:
        manifest["released_in"] = name
    fixed = store.fixed_manifest_digest(manifest)
    pins[version] = {
        "manifest": fixed,
        "releases": {**pin["releases"], name: history[-1]["digest"]},
    }
    store.write_json(path, manifest)
    store.write_json(store.pins_path(root), pins)
    print(f"released {name}: revision {history[-1]['digest']}, manifest {fixed}")
    return history[-1]["digest"]


def main(argv: list[str] | None = None) -> None:
    """Add, check or release the golden files, or print the computed values.

    Args:
        argv: Command-line arguments; ``None`` reads ``sys.argv``.
    """
    parser = argparse.ArgumentParser(
        description="Add, check or release the golden hash files (ITL-626, ITL-946)."
    )
    mode = parser.add_mutually_exclusive_group()
    _ = mode.add_argument(
        "--add",
        action="store_true",
        help="write computed entries no golden file pins yet (the default)",
    )
    _ = mode.add_argument(
        "--check",
        action="store_true",
        help="report moved, unpinned and missing entries; write nothing",
    )
    _ = mode.add_argument(
        "--release",
        metavar="X.Y.Z",
        help="append the release revision and pin it; computes no hash",
    )
    _ = mode.add_argument(
        "--remove",
        metavar="ENTRY",
        action="append",
        help=(
            "remove an entry from a file no release lists (repeatable); with the "
            "developer's approval, the first step of changing an unreleased value"
        ),
    )
    _ = mode.add_argument(
        "--stdout",
        action="store_true",
        help="print all computed groups as one JSON object; write nothing",
    )
    _ = parser.add_argument("--group", choices=cases.GROUPS, action="append")
    _ = parser.add_argument(
        "--allow-errors",
        action="store_true",
        help="with --stdout: record a failing case as ERROR instead of stopping",
    )
    _ = parser.add_argument(
        "--root",
        type=pathlib.Path,
        default=_PROJECT_ROOT,
        help="project root holding the manifest, the pins and the golden folder",
    )
    args = parser.parse_args(argv)
    if args.allow_errors and not args.stdout:
        parser.error("--allow-errors is only for --stdout: errors are never pinned")

    version = get_default_context().version
    if args.remove:
        _ = remove(args.root, version, args.remove)
        return
    if args.release is not None:
        _ = release(args.root, version, args.release)
        return

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
        print(f"skipped {skipped}: optional dependency not installed")
    if args.stdout:
        computed = {
            g: cases.compute_group(g, allow_errors=args.allow_errors) for g in groups
        }
        _ = sys.stdout.write(dump(computed))
        return
    if args.check:
        problems = check(args.root, version, groups)
        if problems:
            raise SystemExit(
                f"{len(problems)} golden problem(s):\n  " + "\n  ".join(problems)
            )
        print(f"golden values of {', '.join(groups)} match")
        return
    _ = add(args.root, version, groups)


if __name__ == "__main__":
    main()
