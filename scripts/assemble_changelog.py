"""Fold the changelog fragments in ``changelog.d/`` into ``CHANGELOG.md``.

A pull request never edits ``CHANGELOG.md``. It adds one fragment,
``changelog.d/<ISSUE-ID>.md``, holding the category headings and text that belong under
the release heading (see ``changelog.d/README.md``). The release pull request runs this
script to move every fragment into the release section and delete it.

Usage:

    uv run python scripts/assemble_changelog.py --check
    uv run python scripts/assemble_changelog.py --version 0.3.0

``--check`` only validates the fragments and changes nothing. Without it, the fragments
are folded into the ``## [<version>]`` section of ``CHANGELOG.md`` and deleted. The
script uses the standard library only.
"""

from __future__ import annotations

import argparse
import re
import sys
from collections.abc import Sequence
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

# The category headings CHANGELOG.md uses, in the order they appear in a release section.
CATEGORIES = (
    "### Breaking changes and migration",
    "### Added",
    "### Changed",
    "### Fixed",
    "### Known limitations",
)

README_NAME = "README.md"
_FRAGMENT_NAME = re.compile(r"^([A-Z][A-Z0-9]*)-([0-9]+)\.md$")
_HEADING = re.compile(r"^(#{1,6})\s")
_FENCE = re.compile(r"^ {0,3}(`{3,}|~{3,})(.*)$")
_BULLET = re.compile(r"^[-*+] ")


class FragmentError(ValueError):
    """A fragment's text does not follow the convention.

    Attributes:
        problems: One sentence fragment per problem found, without the file name.
    """

    def __init__(self, problems: Sequence[str]) -> None:
        super().__init__("; ".join(problems))
        self.problems = tuple(problems)


def _scan_fences(lines: Sequence[str]) -> tuple[list[bool], bool]:
    """Find the fenced code blocks in Markdown lines.

    A block is closed only by a fence of the same character that is at least as long
    as the one that opened it, so a three-backtick line inside a four-backtick block
    is part of the block.

    Args:
        lines: The lines of a Markdown text.

    Returns:
        A mask that is true for each line outside every fenced code block (fence
        lines themselves are inside), and whether the last block is never closed.
    """
    mask: list[bool] = []
    opener: str | None = None
    for line in lines:
        fence = _FENCE.match(line)
        if opener is None:
            # A backtick fence cannot have a backtick in its info string.
            if fence and not (fence.group(1)[0] == "`" and "`" in fence.group(2)):
                opener = fence.group(1)
                mask.append(False)
            else:
                mask.append(True)
            continue
        mask.append(False)
        if (
            fence
            and fence.group(1)[0] == opener[0]
            and len(fence.group(1)) >= len(opener)
            and not fence.group(2).strip()
        ):
            opener = None
    return mask, opener is not None


def _outside_fences(lines: Sequence[str]) -> list[bool]:
    """Mark the lines that are not inside (or delimiting) a fenced code block."""
    return _scan_fences(lines)[0]


def _strip_blank_edges(lines: Sequence[str]) -> list[str]:
    start, end = 0, len(lines)
    while start < end and not lines[start].strip():
        start += 1
    while end > start and not lines[end - 1].strip():
        end -= 1
    return list(lines[start:end])


def parse_fragment(text: str) -> dict[str, list[str]]:
    """Split a fragment into its categories.

    Args:
        text: The fragment's full text.

    Returns:
        A mapping from category heading (``### Added``) to that category's lines,
        without surrounding blank lines and without line endings.

    Raises:
        FragmentError: If the fragment is empty, has text before its first category
            heading, has a level 1 or 2 heading, has an unknown or repeated category
            heading, has a category with no text, or has a code fence that is never
            closed.
    """
    if not text.strip():
        raise FragmentError(["is empty"])

    lines = text.splitlines()
    problems: list[str] = []
    categories: dict[str, list[str]] = {}
    current: list[str] | None = None
    mask, unclosed_fence = _scan_fences(lines)
    if unclosed_fence:
        problems.append("has a code fence that is never closed")
    for line, is_markdown in zip(lines, mask, strict=True):
        heading = _HEADING.match(line) if is_markdown else None
        level = len(heading.group(1)) if heading else 0
        if level in (1, 2):
            problems.append(
                f"has the heading {line.rstrip()!r}; a fragment holds only category "
                "headings, no title, version or date"
            )
        elif level == 3:
            name = line.rstrip()
            current = []
            if name not in CATEGORIES:
                allowed = ", ".join(repr(c) for c in CATEGORIES)
                problems.append(
                    f"has the unknown category heading {name!r} (allowed: {allowed})"
                )
            elif name in categories:
                problems.append(f"repeats the category heading {name!r}")
            else:
                categories[name] = current
        elif current is not None:
            current.append(line)
        elif line.strip():
            problems.append(f"has text before the first category heading: {line!r}")

    for name, body in categories.items():
        categories[name] = _strip_blank_edges(body)
        if not categories[name]:
            problems.append(f"has no text under {name!r}")
    if problems:
        raise FragmentError(problems)
    return categories


def _fragment_order(path: Path) -> tuple[str, int]:
    match = _FRAGMENT_NAME.match(path.name)
    assert match is not None  # callers pass only validated fragment paths
    return match.group(1), int(match.group(2))


def load_fragments(
    fragments_dir: Path,
) -> tuple[list[tuple[Path, dict[str, list[str]]]], list[str]]:
    """Read and validate every fragment in a directory.

    ``README.md`` and dotfiles are skipped. Every other entry must be a fragment.

    Args:
        fragments_dir: The ``changelog.d`` directory.

    Returns:
        The valid fragments as ``(path, categories)`` ordered by team key and issue
        number, and one message per problem found.
    """
    fragments: list[tuple[Path, dict[str, list[str]]]] = []
    problems: list[str] = []
    for path in sorted(fragments_dir.iterdir()):
        if path.name == README_NAME or path.name.startswith("."):
            continue
        if not path.is_file() or not _FRAGMENT_NAME.match(path.name):
            problems.append(
                f"{path}: not a fragment name; expected <ISSUE-ID>.md, "
                "for example ITL-955.md"
            )
            continue
        try:
            fragments.append((path, parse_fragment(path.read_text(encoding="utf-8"))))
        except FragmentError as error:
            problems.extend(f"{path}: {problem}" for problem in error.problems)
    fragments.sort(key=lambda item: _fragment_order(item[0]))
    return fragments, problems


def _ends_in_bullet(lines: Sequence[str]) -> bool:
    """Whether the last block of ``lines`` is a top-level list item."""
    for line in reversed(lines):
        if not line.strip():
            return False
        if _BULLET.match(line):
            return True
    return False


def _append_block(existing: list[str], block: Sequence[str]) -> list[str]:
    """Append ``block`` to ``existing`` text: one list if both are bullets."""
    if not existing:
        return list(block)
    joined_list = _ends_in_bullet(existing) and bool(_BULLET.match(block[0]))
    return [*existing, *([] if joined_list else [""]), *block]


def _section_bounds(lines: Sequence[str], version: str) -> tuple[int, int] | None:
    """Find ``## [<version>]``: the heading's index and the section's end index."""
    mask = _outside_fences(lines)
    prefix = f"## [{version}]"
    start = next(
        (i for i, line in enumerate(lines) if mask[i] and line.startswith(prefix)),
        None,
    )
    if start is None:
        return None
    end = next(
        (
            i
            for i in range(start + 1, len(lines))
            if mask[i] and lines[i].startswith("## ")
        ),
        len(lines),
    )
    return start, end


def _insert_section(lines: list[str], version: str) -> list[str]:
    """Add an empty ``## [<version>] — unreleased`` section above the newest one."""
    mask = _outside_fences(lines)
    index = next(
        (i for i, line in enumerate(lines) if mask[i] and line.startswith("## [")),
        len(lines),
    )
    before = lines[:index]
    if before and before[-1].strip():
        before.append("")
    return [*before, f"## [{version}] — unreleased", "", *lines[index:]]


def _fold_category(
    lines: list[str], version: str, category: str, block: Sequence[str]
) -> list[str]:
    """Append ``block`` to ``category`` of the release section, adding it if absent."""
    bounds = _section_bounds(lines, version)
    assert bounds is not None  # the caller creates the section first
    start, end = bounds
    mask = _outside_fences(lines)
    headings = [
        i for i in range(start + 1, end) if mask[i] and lines[i].startswith("### ")
    ]

    existing = next((i for i in headings if lines[i].rstrip() == category), None)
    if existing is not None:
        body_end = next((i for i in headings if i > existing), end)
        last = body_end
        while last > existing + 1 and not lines[last - 1].strip():
            last -= 1
        body = lines[existing + 1 : last]
        if _strip_blank_edges(body):
            new_body = _append_block(body, block)
        else:
            new_body = ["", *block]
        return [*lines[: existing + 1], *new_body, *lines[last:]]

    rank = CATEGORIES.index(category)
    later = {c for c in CATEGORIES[rank + 1 :]}
    at = next((i for i in headings if lines[i].rstrip() in later), end)
    before = lines[:at]
    if before and before[-1].strip():
        before.append("")
    after = lines[at:]
    return [*before, category, "", *block, *([""] if after else []), *after]


def fold(
    changelog: str, version: str, fragments: Sequence[dict[str, list[str]]]
) -> str:
    """Fold parsed fragments into one release section of a changelog.

    Each category's text is appended, fragment by fragment in the order given, at the
    end of that category in the ``## [<version>]`` section. Existing lines keep their
    order and content. A category the section lacks is inserted in the order of
    ``CATEGORIES``; a missing section is created above the newest one.

    Args:
        changelog: The full text of ``CHANGELOG.md``.
        version: The release whose section receives the text, for example ``0.3.0``.
        fragments: The parsed fragments, as returned by ``parse_fragment``.

    Returns:
        The new full text of the changelog.
    """
    lines = changelog.splitlines()
    if _section_bounds(lines, version) is None:
        lines = _insert_section(lines, version)
    for category in CATEGORIES:
        block: list[str] = []
        for fragment in fragments:
            if category in fragment:
                block = _append_block(block, fragment[category])
        if block:
            lines = _fold_category(lines, version, category, block)
    return "\n".join(lines) + "\n"


def main(argv: Sequence[str] | None = None) -> int:
    """Validate the fragments, and unless ``--check`` is given, fold and delete them.

    Args:
        argv: Command-line arguments; ``sys.argv[1:]`` when omitted.

    Returns:
        The process exit code: 0 on success, 1 if any fragment is malformed.
    """
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n\n")[0])
    parser.add_argument(
        "--version",
        help="release section to fold into, e.g. 0.3.0 (required unless --check)",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="only validate the fragments; change nothing",
    )
    parser.add_argument(
        "--changelog", type=Path, default=REPO_ROOT / "CHANGELOG.md", help="changelog"
    )
    parser.add_argument(
        "--fragments-dir",
        type=Path,
        default=REPO_ROOT / "changelog.d",
        help="directory holding the fragments",
    )
    args = parser.parse_args(argv)
    if not args.check and not args.version:
        parser.error("--version is required unless --check is given")

    fragments, problems = load_fragments(args.fragments_dir)
    if problems:
        print("\n".join(problems), file=sys.stderr)
        return 1
    if args.check:
        print(f"{len(fragments)} changelog fragment(s) OK")
        return 0
    if not fragments:
        print("No changelog fragments to fold.")
        return 0

    version = args.version.removeprefix("v")
    text = args.changelog.read_text(encoding="utf-8")
    new_text = fold(text, version, [categories for _, categories in fragments])
    args.changelog.write_text(new_text, encoding="utf-8")
    for path, _ in fragments:
        path.unlink()
    print(
        f"Folded {len(fragments)} fragment(s) into [{version}] of {args.changelog} "
        "and deleted them."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
