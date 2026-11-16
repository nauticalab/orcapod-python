"""The golden hash files of a data context and the history that pins them (ITL-946).

The golden values of a data context version live in one folder,
``tests/test_hashing/golden/data/<folder>/``, named by the ``golden.folder`` field of the
version's manifest (``src/orcapod/contexts/data/<version>.json``). Each file is
``<group>-NN.json``: the entries of one group of ``cases.GROUPS`` and a two-digit counter.
The unit of immutability is the file: a file listed by a released revision never changes,
and new values go into a new file.

The manifest's ``golden.history`` is an append-only chain of revisions::

    {"date": "YYYY-MM-DD", "release": "X.Y.Z" or null,
     "added": {"<file name>": "<sha256 of its bytes>"},
     "previous": <digest of the revision before, or null>,
     "digest": <sha256 of the canonical JSON of the other four fields>}

A release appends a revision with ``added`` empty and ``release`` set; its digest is pinned
in ``src/orcapod/contexts/released_manifests.json``. Revisions after the last release
revision may still be edited (their digests are recomputed); the ones up to it may not.

This module holds the rules: reading the folder, computing digests, and listing what in a
tree breaks the rules. ``generate_golden_hashes.py`` writes the files and the history,
``test_golden_hashes.py`` and ``tests/test_contexts/test_data_context_v1.py`` check them,
and ``scripts/check_golden_tags.py`` compares them with release tags.
"""

from __future__ import annotations

import hashlib
import json
import re
import shutil
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from packaging.version import InvalidVersion, Version

from tests.test_hashing.golden.cases import GROUPS

PROJECT_ROOT = Path(__file__).resolve().parents[3]

CONTEXTS_DIR = Path("src/orcapod/contexts")
"""The data context directory, relative to a project root."""

PINS_FILE = CONTEXTS_DIR / "released_manifests.json"
"""The pins of released manifests, relative to a project root."""

GOLDEN_ROOT = Path("tests/test_hashing/golden/data")
"""The directory holding one golden folder per data context version, relative to a root."""

FILE_NAME = re.compile(r"^(?P<group>[a-z][a-z0-9_]*)-(?P<number>[0-9]{2})\.json$")
"""A golden file name: ``<group>-NN.json``."""

REVISION_FIELDS = ("date", "release", "added", "previous")
"""The fields of a revision its digest covers, besides the digest itself."""

_DATE = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")


class GoldenLayoutError(ValueError):
    """The golden folder breaks the file rules (an unknown file name, a duplicate entry)."""


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------


def manifest_path(root: Path, version: str) -> Path:
    """The manifest file of a data context version.

    Args:
        root: Project root (the repository, or a copy of its golden tree).
        version: Data context version, e.g. ``v1``.

    Returns:
        ``<root>/src/orcapod/contexts/data/<version>.json``.
    """
    return root / CONTEXTS_DIR / "data" / f"{version}.json"


def pins_path(root: Path) -> Path:
    """The ``released_manifests.json`` file under ``root``."""
    return root / PINS_FILE


def golden_folder(root: Path, manifest: Mapping[str, Any]) -> Path:
    """The golden folder a manifest names.

    Args:
        root: Project root.
        manifest: Parsed manifest.

    Returns:
        ``<root>/tests/test_hashing/golden/data/<golden.folder>``.
    """
    return root / GOLDEN_ROOT / manifest["golden"]["folder"]


def _unique_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """Build a JSON object, refusing a key that appears twice (JSON keeps the last)."""
    out: dict[str, Any] = {}
    for key, value in pairs:
        if key in out:
            raise GoldenLayoutError(f"the key {key!r} appears twice in one JSON object")
        out[key] = value
    return out


def read_json(path: Path) -> Any:
    """Parse a JSON file, refusing an object that repeats a key.

    Raises:
        GoldenLayoutError: An object in the file repeats a key; the message names the file.
    """
    try:
        return json.loads(
            path.read_text(encoding="utf-8"), object_pairs_hook=_unique_keys
        )
    except GoldenLayoutError as exc:
        raise GoldenLayoutError(f"{path}: {exc}") from exc


def write_json(path: Path, value: Any) -> None:
    """Write a manifest or pin file the way the repository stores it.

    Four-space indentation, keys in their given order, a trailing newline.
    """
    _ = path.write_bytes(
        (json.dumps(value, indent=4, ensure_ascii=False) + "\n").encode("utf-8")
    )


def copy_tree(source_root: Path, target_root: Path, version: str) -> None:
    """Copy what the golden rules cover to another root, for a trial run.

    Copies the version's manifest, ``released_manifests.json`` and the version's golden
    folder, keeping their paths relative to the root. ``generate_golden_hashes.py --root``
    can then add or release on the copy without touching the repository.

    Args:
        source_root: Project root to copy from.
        target_root: Root to copy to; created if missing.
        version: Data context version whose files are copied.
    """
    manifest = read_json(manifest_path(source_root, version))
    for path in (manifest_path(source_root, version), pins_path(source_root)):
        target = target_root / path.relative_to(source_root)
        target.parent.mkdir(parents=True, exist_ok=True)
        _ = shutil.copyfile(path, target)
    folder = golden_folder(source_root, manifest)
    _ = shutil.copytree(folder, target_root / folder.relative_to(source_root))


def reopen_tree(root: Path, version: str, date: str) -> None:
    """Give a copied tree a fixed open starting point, whatever the repository released.

    The history becomes one open revision listing every file of the golden folder with its
    sha256; ``released_in`` is cleared and the pin becomes ``{"manifest": null,
    "releases": {}}``. The golden files are not touched. Tests that add or release on a
    copy start from this state, so they keep working after the repository's own releases.

    Args:
        root: Root of a copy made by ``copy_tree``; never the repository.
        version: Data context version.
        date: Date of the open revision, ``YYYY-MM-DD``.
    """
    path = manifest_path(root, version)
    manifest = read_json(path)
    folder = golden_folder(root, manifest)
    revision: dict[str, Any] = {
        "date": date,
        "release": None,
        "added": {p.name: file_digest(p) for p in sorted(folder.iterdir())},
        "previous": None,
    }
    revision["digest"] = revision_digest(revision)
    manifest["golden"]["history"] = [revision]
    manifest["released_in"] = None
    write_json(path, manifest)
    pins = read_json(pins_path(root))
    pins[version] = {"manifest": None, "releases": {}}
    write_json(pins_path(root), pins)


# ---------------------------------------------------------------------------
# Digests
# ---------------------------------------------------------------------------


def canonical_json(value: Any) -> bytes:
    """The canonical JSON bytes of a value: sorted keys, no spaces."""
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


def revision_digest(revision: Mapping[str, Any]) -> str:
    """The digest of a revision: sha256 of the canonical JSON of its four fields.

    Args:
        revision: A revision; only ``date``, ``release``, ``added`` and ``previous`` are
            read.

    Returns:
        The hex digest.
    """
    body = {name: revision[name] for name in REVISION_FIELDS}
    return hashlib.sha256(canonical_json(body)).hexdigest()


def fixed_manifest_digest(manifest: Mapping[str, Any]) -> str:
    """The digest pinned for a released manifest: everything but ``golden``.

    Args:
        manifest: Parsed manifest.

    Returns:
        sha256 of the canonical JSON of the manifest without its ``golden`` field.
    """
    fixed = {name: value for name, value in manifest.items() if name != "golden"}
    return hashlib.sha256(canonical_json(fixed)).hexdigest()


def file_digest(path: Path) -> str:
    """sha256 of a file's bytes."""
    return hashlib.sha256(path.read_bytes()).hexdigest()


# ---------------------------------------------------------------------------
# The folder
# ---------------------------------------------------------------------------


def group_files(folder: Path) -> dict[str, list[Path]]:
    """The golden files of a folder by group, each group's files in counter order.

    Args:
        folder: A golden folder.

    Returns:
        Group name to its files.

    Raises:
        GoldenLayoutError: The folder holds something that is not a ``<group>-NN.json``
            file.
    """
    files: dict[str, list[Path]] = {}
    for path in sorted(folder.iterdir()):
        match = FILE_NAME.match(path.name)
        if match is None or not path.is_file():
            raise GoldenLayoutError(
                f"{path} is not a golden file: a golden folder holds only files named "
                "<group>-NN.json"
            )
        files.setdefault(match["group"], []).append(path)
    return files


def next_file_name(name: str) -> str:
    """The name of a group's next golden file, e.g. ``schema-02.json`` after ``schema-01.json``.

    Args:
        name: A golden file name, ``<group>-NN.json``.

    Returns:
        The same group with the counter one higher.

    Raises:
        GoldenLayoutError: ``name`` is not a golden file name.
    """
    match = FILE_NAME.match(name)
    if match is None:
        raise GoldenLayoutError(f"{name} is not a golden file name")
    return f"{match['group']}-{int(match['number']) + 1:02d}.json"


def load_folder(folder: Path) -> dict[str, dict[str, Any]]:
    """Read every golden file of a folder.

    Args:
        folder: A golden folder.

    Returns:
        Group name to its entries, merged over the group's files.

    Raises:
        GoldenLayoutError: A file name is not ``<group>-NN.json``, a file is not a JSON
            object, or an entry name appears in two files of the folder.
    """
    values: dict[str, dict[str, Any]] = {}
    where: dict[str, str] = {}
    for group, paths in group_files(folder).items():
        merged = values.setdefault(group, {})
        for path in paths:
            entries = read_json(path)
            if not isinstance(entries, dict):
                raise GoldenLayoutError(f"{path} does not hold a JSON object")
            for entry, value in entries.items():
                if entry in where:
                    raise GoldenLayoutError(
                        f"golden entry {entry!r} is pinned twice, in {where[entry]} and "
                        f"{path.name}; an entry name appears once in a folder"
                    )
                where[entry] = path.name
                merged[entry] = value
    return values


# ---------------------------------------------------------------------------
# The history
# ---------------------------------------------------------------------------


def last_release_index(history: Sequence[Mapping[str, Any]]) -> int | None:
    """Index of the last release revision, or ``None`` when nothing was released."""
    releases = [i for i, revision in enumerate(history) if revision["release"]]
    return releases[-1] if releases else None


def released_files(history: Sequence[Mapping[str, Any]]) -> set[str]:
    """Names of the files listed by revisions up to the last release revision."""
    end = last_release_index(history)
    if end is None:
        return set()
    return {name for revision in history[: end + 1] for name in revision["added"]}


def rechain(history: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Recompute ``previous`` and ``digest`` of the revisions after the last release.

    The revisions up to the last release revision are returned unchanged.

    Args:
        history: Revisions, oldest first.

    Returns:
        A new list of revisions.
    """
    end = last_release_index(history)
    start = 0 if end is None else end + 1
    out = [dict(revision) for revision in history]
    for i in range(start, len(out)):
        out[i]["previous"] = out[i - 1]["digest"] if i else None
        out[i]["digest"] = revision_digest(out[i])
    return out


def with_file(
    history: Sequence[Mapping[str, Any]], name: str, digest: str, date: str
) -> list[dict[str, Any]]:
    """Record a new or rewritten golden file in the open revision.

    The open revision is the last one when it is not a release; otherwise a new revision
    is appended. A file listed by an open revision has its digest updated in place.

    Args:
        history: Revisions, oldest first.
        name: File name, ``<group>-NN.json``.
        digest: sha256 of the file's bytes.
        date: The date of the change, ``YYYY-MM-DD``; it becomes the revision's date.

    Returns:
        The new history, rechained.

    Raises:
        ValueError: ``name`` is listed by a released revision.
    """
    if name in released_files(history):
        raise ValueError(f"{name} is listed by a released revision and never changes")
    out = [dict(revision) for revision in history]
    owner = next((r for r in out if name in r["added"]), None)
    if owner is None:
        if not out or out[-1]["release"]:
            out.append({"date": date, "release": None, "added": {}, "previous": None})
        owner = out[-1]
    owner["added"] = dict(sorted({**owner["added"], name: digest}.items()))
    owner["date"] = date
    return rechain(out)


def with_release(
    history: Sequence[Mapping[str, Any]], release: str, date: str
) -> list[dict[str, Any]]:
    """Append the release revision of a library version.

    Args:
        history: Revisions, oldest first.
        release: The library version, normalized.
        date: The release date, ``YYYY-MM-DD``.

    Returns:
        The new history.
    """
    out = [dict(revision) for revision in history]
    revision: dict[str, Any] = {
        "date": date,
        "release": release,
        "added": {},
        "previous": out[-1]["digest"] if out else None,
    }
    revision["digest"] = revision_digest(revision)
    return [*out, revision]


# ---------------------------------------------------------------------------
# The rules
# ---------------------------------------------------------------------------


def history_problems(history: Any) -> list[str]:
    """What breaks the shape or the chain of a history.

    Every revision has exactly the fields ``date``, ``release``, ``added``, ``previous``
    and ``digest``; a release revision adds no file and any other revision adds at least
    one; ``previous`` is the digest of the revision before (``null`` for the first); the
    digest recomputes; and no library version is released twice.

    Args:
        history: The ``golden.history`` value of a manifest.

    Returns:
        One sentence per problem; empty when the history is sound.
    """
    if not isinstance(history, list):
        return ["golden.history is not a list"]
    problems: list[str] = []
    expected_fields = {*REVISION_FIELDS, "digest"}
    previous: str | None = None
    seen_releases: set[str] = set()
    for i, revision in enumerate(history):
        label = f"revision {i}"
        if not isinstance(revision, dict) or set(revision) != expected_fields:
            problems.append(
                f"{label} does not have exactly the fields {sorted(expected_fields)}"
            )
            previous = None
            continue
        if not isinstance(revision["date"], str) or not _DATE.match(revision["date"]):
            problems.append(f"{label} has no YYYY-MM-DD date")
        added = revision["added"]
        if not isinstance(added, dict) or not all(
            isinstance(name, str) and isinstance(digest, str)
            for name, digest in added.items()
        ):
            problems.append(f"{label}: added is not a mapping of file names to digests")
            added = {}
        release = revision["release"]
        if release is not None:
            if not isinstance(release, str) or not release:
                problems.append(f"{label}: release is neither null nor a version")
            elif release in seen_releases:
                problems.append(f"{label}: release {release} appears twice")
            else:
                seen_releases.add(release)
            if added:
                problems.append(f"{label} is a release revision and adds files")
        elif not added:
            problems.append(f"{label} adds no file and is not a release revision")
        if revision["previous"] != previous:
            problems.append(
                f"{label}: previous is not the digest of the revision before"
            )
        if revision["digest"] != revision_digest(revision):
            problems.append(f"{label}: digest does not recompute")
        previous = revision["digest"]
    return problems


def folder_problems(history: Sequence[Mapping[str, Any]], folder: Path) -> list[str]:
    """What breaks the agreement between a history and its golden folder.

    Every listed file exists and has the listed sha256; every file in the folder is listed
    by exactly one revision, is named ``<group>-NN.json`` for a group of ``cases.GROUPS``,
    and holds a JSON object; and no entry name appears in two files.

    Args:
        history: A history without ``history_problems``.
        folder: The golden folder the manifest names.

    Returns:
        One sentence per problem.
    """
    problems: list[str] = []
    listed: dict[str, int] = {}
    end = last_release_index(history)
    for i, revision in enumerate(history):
        for name, digest in revision["added"].items():
            if name in listed:
                problems.append(f"{name} is listed by revisions {listed[name]} and {i}")
                continue
            listed[name] = i
            match = FILE_NAME.match(name)
            if match is None or match["group"] not in GROUPS:
                problems.append(
                    f"{name}, listed by revision {i}, is not a golden file name "
                    "(<group>-NN.json of a known group, in the golden folder)"
                )
                continue
            path = folder / name
            if not path.is_file():
                problems.append(f"{name}, listed by revision {i}, is missing")
            elif file_digest(path) != digest:
                problems.append(
                    f"{name} does not have the sha256 revision {i} lists: "
                    + (
                        "a released golden file never changes"
                        if end is not None and i <= end
                        else "restore the file (git checkout); a value change no release "
                        "lists needs approval, then generate_golden_hashes.py --remove "
                        "and --add on the restored tree"
                    )
                )
    if not folder.is_dir():
        return [*problems, f"the golden folder {folder} does not exist"]
    for path in sorted(folder.iterdir()):
        if path.name not in listed:
            problems.append(f"{path.name} is in {folder} but no revision lists it")
        match = FILE_NAME.match(path.name)
        if match is not None and match["group"] not in GROUPS:
            problems.append(
                f"{path.name} belongs to no known group (cases.GROUPS: {list(GROUPS)})"
            )
    try:
        _ = load_folder(folder)
    except GoldenLayoutError as exc:
        problems.append(str(exc))
    return problems


def pin_problems(
    manifest: Mapping[str, Any], pin: Any, history: Sequence[Mapping[str, Any]]
) -> list[str]:
    """What breaks the agreement between a manifest, its history and its pin.

    The pin is ``{"manifest": <sha256 or null>, "releases": {<version>: <digest>}}``. The
    pinned releases are exactly the release revisions, each with its digest; the manifest
    pin is set exactly when ``released_in`` is, and then equals the digest of the manifest
    without ``golden``; ``released_in`` is the first release; and a manifest with releases
    is pinned.

    Args:
        manifest: Parsed manifest.
        pin: The version's value in ``released_manifests.json``.
        history: The manifest's history, without ``history_problems``.

    Returns:
        One sentence per problem.
    """
    if (
        not isinstance(pin, dict)
        or set(pin) != {"manifest", "releases"}
        or not isinstance(pin["releases"], dict)
    ):
        return ['the pin is not {"manifest": ..., "releases": {...}}']
    problems: list[str] = []
    revisions = {r["release"]: r["digest"] for r in history if r["release"]}
    for release, digest in pin["releases"].items():
        if release not in revisions:
            problems.append(f"pinned release {release} is not a release revision")
        elif revisions[release] != digest:
            problems.append(
                f"pinned release {release} is not the digest of its release revision"
            )
    for release in revisions:
        if release not in pin["releases"]:
            problems.append(f"release revision {release} is not pinned")
    released_in = manifest.get("released_in")
    if (pin["manifest"] is None) != (released_in is None):
        problems.append("released_in is set exactly when the manifest is pinned")
    if (pin["manifest"] is None) != (not revisions):
        problems.append("the manifest is pinned exactly when it has a release")
    if pin["manifest"] is not None and pin["manifest"] != fixed_manifest_digest(
        manifest
    ):
        problems.append(
            "the manifest without golden differs from its pin: a released manifest is "
            "frozen; a hashing change needs a new data context version"
        )
    if released_in is not None and revisions and released_in != next(iter(revisions)):
        problems.append("released_in is not the first release revision")
    return problems


def final_release_problems(pin: Mapping[str, Any], package_version: str) -> list[str]:
    """What a final release of the library lacks in its data context's pin.

    A final release (not a development, pre-release or local version) has its own
    ``releases`` line and a pinned manifest.

    Args:
        pin: The current data context's value in ``released_manifests.json``.
        package_version: The library version, e.g. ``orcapod.__version__``.

    Returns:
        One sentence per problem; empty for a version that is not a final release.
    """
    version = Version(package_version)
    if version.is_prerelease or version.is_devrelease or version.local is not None:
        return []
    problems: list[str] = []
    if pin["manifest"] is None:
        problems.append(f"release {version} ships an open manifest")
    if str(version) not in pin["releases"]:
        problems.append(
            f"release {version} has no releases line: run "
            f"generate_golden_hashes.py --release {version}"
        )
    return problems


def normalize_release(text: str) -> str:
    """The normalized public version of a release, e.g. ``0.3.0``.

    Args:
        text: A version, optionally with a leading ``v``.

    Returns:
        The normalized version.

    Raises:
        ValueError: ``text`` is not a version, or is a local or development version.
    """
    try:
        version = Version(text.removeprefix("v"))
    except InvalidVersion as exc:
        raise ValueError(f"{text!r} is not a version") from exc
    if version.local is not None or version.is_devrelease:
        raise ValueError(f"{text!r} is a local or development version, never released")
    return str(version)


def tree_problems(root: Path, version: str) -> list[str]:
    """Every problem of a tree's golden history, folder and pin for one version.

    Args:
        root: Project root.
        version: Data context version.

    Returns:
        One sentence per problem.
    """
    manifest = read_json(manifest_path(root, version))
    golden = manifest.get("golden")
    if not isinstance(golden, dict) or set(golden) != {"folder", "history"}:
        return ['the manifest golden field is not {"folder": ..., "history": [...]}']
    history = golden["history"]
    problems = history_problems(history)
    if problems:
        return problems
    problems = folder_problems(history, golden_folder(root, manifest))
    pins = read_json(pins_path(root))
    if version not in pins:
        return [*problems, f"{version} is not listed in released_manifests.json"]
    return [*problems, *pin_problems(manifest, pins[version], history)]
