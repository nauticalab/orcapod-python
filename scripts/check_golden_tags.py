"""Check HEAD against every release tag of the current data context (ITL-946).

A golden file that a release listed never changes, and the pins of a release never change.
The tests in the tree check the golden files against the manifest's history and the history
against ``released_manifests.json``; this check anchors all three to the release tags,
which no pull request can move. For each tag ``v<X.Y.Z>`` whose
``src/orcapod/contexts/released_manifests.json`` pins release ``X.Y.Z`` of the current data
context, it checks in HEAD:

- every golden file listed by the revisions up to that release exists with the bytes it has
  at the tag;
- the revisions up to that release are present unchanged in the golden history;
- the pins of the current data context at the tag (the manifest pin and every ``releases``
  line) are present unchanged.

The current data context is the one manifest in ``src/orcapod/contexts/data/`` at HEAD.

Usage:

    uv run python scripts/check_golden_tags.py [--repo DIR]

It exits 1 on a difference. Without git, history or tags (not a repository, a shallow
clone, no tags) it exits 1 in CI (``CI=true``) and 0 with a message elsewhere. The script
uses the standard library only.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path, PurePosixPath
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
CONTEXTS = PurePosixPath("src/orcapod/contexts")
PINS = CONTEXTS / "released_manifests.json"
GOLDEN_ROOT = PurePosixPath("tests/test_hashing/golden/data")


class GitUnavailableError(RuntimeError):
    """git, the history or the tags cannot be read."""


# Variables that point git at another repository than ``--repo`` (git(1), "Environment
# Variables"); they are removed so that the repository read is the one named, and
# ``--no-replace-objects`` keeps replace refs from substituting the objects read.
_GIT_LOCATION_VARIABLES = frozenset(
    {
        "GIT_DIR",
        "GIT_WORK_TREE",
        "GIT_COMMON_DIR",
        "GIT_INDEX_FILE",
        "GIT_OBJECT_DIRECTORY",
        "GIT_ALTERNATE_OBJECT_DIRECTORIES",
        "GIT_NAMESPACE",
        "GIT_GRAFT_FILE",
        "GIT_SHALLOW_FILE",
    }
)


def _git(repo: Path, *args: str) -> subprocess.CompletedProcess[bytes]:
    env = {k: v for k, v in os.environ.items() if k not in _GIT_LOCATION_VARIABLES}
    try:
        return subprocess.run(
            ["git", "--no-replace-objects", "-C", str(repo), *args],
            capture_output=True,
            check=False,
            env=env,
        )
    except FileNotFoundError as exc:
        raise GitUnavailableError("git is not installed") from exc


def _show(repo: Path, rev: str, path: PurePosixPath) -> bytes | None:
    """The bytes of ``path`` at ``rev``, or ``None`` when it has no such file."""
    result = _git(repo, "cat-file", "blob", f"{rev}:{path}")
    return result.stdout if result.returncode == 0 else None


def _show_json(repo: Path, rev: str, path: PurePosixPath) -> Any:
    """The parsed JSON of ``path`` at ``rev``, or ``None`` when absent or not JSON."""
    data = _show(repo, rev, path)
    if data is None:
        return None
    try:
        return json.loads(data)
    except ValueError:
        return None


def _require_history(repo: Path) -> list[str]:
    """The repository's tags; raises when git, its history or its tags are missing."""
    inside = _git(repo, "rev-parse", "--is-inside-work-tree")
    if inside.returncode != 0 or inside.stdout.strip() != b"true":
        raise GitUnavailableError(f"{repo} is not a git work tree")
    shallow = _git(repo, "rev-parse", "--is-shallow-repository")
    if shallow.stdout.strip() != b"false":
        raise GitUnavailableError(
            "the clone is shallow; check out with the full history and tags "
            "(actions/checkout fetch-depth: 0)"
        )
    tags = _git(repo, "tag", "--list")
    names = tags.stdout.decode().split()
    if tags.returncode != 0 or not names:
        raise GitUnavailableError("the repository has no tags; fetch them")
    return names


def _current_version(repo: Path) -> str:
    """The version of the one data context manifest at HEAD."""
    listing = _git(repo, "ls-tree", "--name-only", "HEAD", f"{CONTEXTS / 'data'}/")
    names = [
        PurePosixPath(line).name
        for line in listing.stdout.decode().splitlines()
        if line.endswith(".json")
    ]
    if listing.returncode != 0 or len(names) != 1:
        raise SystemExit(
            f"cannot tell the current data context: manifests at HEAD are {names}"
        )
    return names[0].removesuffix(".json")


def check_tag(repo: Path, tag: str, version: str) -> list[str] | None:
    """Check HEAD against one tag.

    Args:
        repo: Repository root.
        tag: Tag name.
        version: Current data context version.

    Returns:
        ``None`` when the tag is not a release of ``version``; otherwise one sentence per
        difference (empty when HEAD keeps everything the tag released).
    """
    release = tag.removeprefix("v")
    tag_pins = _show_json(repo, tag, PINS)
    pin = tag_pins.get(version) if isinstance(tag_pins, dict) else None
    if not isinstance(pin, dict) or not isinstance(pin.get("releases"), dict):
        return None
    if release not in pin["releases"]:
        return None
    problems: list[str] = []
    head_pins = _show_json(repo, "HEAD", PINS)
    head_pin = head_pins.get(version) if isinstance(head_pins, dict) else None
    if not isinstance(head_pin, dict):
        return [f"{tag}: HEAD has no pin of data context {version}"]
    if head_pin.get("manifest") != pin.get("manifest"):
        problems.append(f"{tag}: the manifest pin of {version} changed")
    head_releases = head_pin.get("releases")
    head_releases = head_releases if isinstance(head_releases, dict) else {}
    for name, digest in pin["releases"].items():
        if head_releases.get(name) != digest:
            problems.append(f"{tag}: the releases line {name} of {version} changed")

    manifest_path = CONTEXTS / "data" / f"{version}.json"
    manifest = _show_json(repo, tag, manifest_path)
    golden = manifest.get("golden") if isinstance(manifest, dict) else None
    if not isinstance(golden, dict):
        return [*problems, f"{tag}: {manifest_path} has no golden section"]
    history = golden.get("history")
    if not isinstance(history, list):
        return [*problems, f"{tag}: {manifest_path} has no golden history"]
    end = next(
        (
            i
            for i, revision in enumerate(history)
            if isinstance(revision, dict)
            and revision.get("release") == release
            and revision.get("digest") == pin["releases"][release]
        ),
        None,
    )
    if end is None:
        return [*problems, f"{tag}: the history has no release revision {release}"]
    released = history[: end + 1]
    head_manifest = _show_json(repo, "HEAD", manifest_path)
    head_golden = (
        head_manifest.get("golden") if isinstance(head_manifest, dict) else None
    )
    head_history = head_golden.get("history") if isinstance(head_golden, dict) else None
    if not isinstance(head_history, list) or head_history[: end + 1] != released:
        problems.append(
            f"{tag}: the golden history up to release {release} changed in HEAD"
        )
    folder = GOLDEN_ROOT / str(golden.get("folder"))
    for revision in released:
        added = revision.get("added") if isinstance(revision, dict) else None
        if not isinstance(added, dict):
            problems.append(f"{tag}: a released revision lists no files")
            continue
        for name in added:
            path = folder / name
            at_tag = _show(repo, tag, path)
            at_head = _show(repo, "HEAD", path)
            if at_head is None:
                problems.append(f"{tag}: {path} is missing in HEAD")
            elif at_head != at_tag:
                problems.append(f"{tag}: {path} differs from the released file")
    return problems


def check_release(repo: Path, release: str) -> list[str]:
    """Check that HEAD records a release of the current data context, before it is tagged.

    The Release workflow runs this before it creates the tag: HEAD's
    ``released_manifests.json`` pins ``release`` under the current context's
    ``releases``, its manifest pin is set, and the pinned digest is the digest of the
    release revision of ``release`` in HEAD's golden history, which is its last revision
    (no golden file added after the release step ships unreleased).

    Args:
        repo: Repository root.
        release: The library version being released, without a leading ``v``.

    Returns:
        One sentence per problem; empty when HEAD records the release.
    """
    version = _current_version(repo)
    step = (
        f"run generate_golden_hashes.py --release {release} in the release pull request"
    )
    pins = _show_json(repo, "HEAD", PINS)
    pin = pins.get(version) if isinstance(pins, dict) else None
    releases = pin.get("releases") if isinstance(pin, dict) else None
    if not isinstance(pin, dict) or not isinstance(releases, dict):
        return [f"HEAD has no pin of data context {version}: {step}"]
    if release not in releases:
        return [
            f"HEAD does not record release {release} of data context {version}: {step}"
        ]
    problems: list[str] = []
    if pin.get("manifest") is None:
        problems.append(f"the manifest of data context {version} is not pinned: {step}")
    manifest = _show_json(repo, "HEAD", CONTEXTS / "data" / f"{version}.json")
    golden = manifest.get("golden") if isinstance(manifest, dict) else None
    history = golden.get("history") if isinstance(golden, dict) else None
    entries: list[Any] = history if isinstance(history, list) else []
    revisions = [
        revision
        for revision in entries
        if isinstance(revision, dict) and revision.get("release") == release
    ]
    if [revision.get("digest") for revision in revisions] != [releases[release]]:
        problems.append(
            f"the pinned digest of {release} is not the release revision of {release} "
            "in the golden history"
        )
    elif entries[-1] is not revisions[0]:
        problems.append(
            f"the release revision of {release} is not the last revision of the golden "
            "history: a golden file added after the release step would ship unreleased; "
            f"tag the commit of the release step, or run --release {release} again after "
            "the addition (a new version)"
        )
    return problems


def main(argv: Sequence[str] | None = None) -> int:
    """Run the check.

    Args:
        argv: Command-line arguments; ``None`` reads ``sys.argv``.

    Returns:
        The exit status.
    """
    parser = argparse.ArgumentParser(
        description="Check HEAD against every release tag of the current data context."
    )
    _ = parser.add_argument("--repo", type=Path, default=REPO_ROOT)
    _ = parser.add_argument(
        "--release-version",
        metavar="X.Y.Z",
        help=(
            "instead of comparing with the tags, check that HEAD records this release "
            "(the Release workflow's gate before it tags); fails without git"
        ),
    )
    args = parser.parse_args(argv)
    if args.release_version is not None:
        try:
            problems = check_release(args.repo, args.release_version.removeprefix("v"))
        except GitUnavailableError as exc:
            print(f"error: cannot read HEAD: {exc}", file=sys.stderr)
            return 1
        if problems:
            print(
                f"release {args.release_version} is not recorded in the golden history:",
                file=sys.stderr,
            )
            for problem in problems:
                print(f"  {problem}", file=sys.stderr)
            return 1
        print(f"HEAD records release {args.release_version} in the golden history")
        return 0
    in_ci = os.environ.get("CI", "").lower() == "true"
    try:
        tags = _require_history(args.repo)
    except GitUnavailableError as exc:
        if in_ci:
            print(f"error: cannot check the release tags: {exc}", file=sys.stderr)
            return 1
        print(f"skipped: cannot check the release tags: {exc}")
        return 0
    version = _current_version(args.repo)
    checked: list[str] = []
    problems: list[str] = []
    for tag in sorted(tags):
        found = check_tag(args.repo, tag, version)
        if found is None:
            continue
        checked.append(tag)
        problems.extend(found)
    if problems:
        print(
            "HEAD changes what a release of data context "
            f"{version} shipped; a released golden file, revision or pin never changes:",
            file=sys.stderr,
        )
        for problem in problems:
            print(f"  {problem}", file=sys.stderr)
        return 1
    if checked:
        print(f"HEAD keeps what {', '.join(checked)} released (data context {version})")
    else:
        print(f"no release tag of data context {version}: nothing to check")
    return 0


if __name__ == "__main__":
    sys.exit(main())
