"""Read and check the design issues log in ``docs/dev/design_issues/``.

The log is one file per entry, ``docs/dev/design_issues/<LINEAR-ISSUE-ID>-<ENTRY-ID>.md``
(see ``docs/dev/design_issues/README.md``). This script reads the log as a whole. The log
was at the top-level ``design_issues/`` until ITL-1169 and in a single
``DESIGN_ISSUES.md`` before ITL-968; a file at either old location is an error in every
mode.

Usage:

    uv run python scripts/design_issues.py
    uv run python scripts/design_issues.py --status open
    uv run python scripts/design_issues.py --full
    uv run python scripts/design_issues.py --check

Without an option it prints an index: every entry by area, with its id, status, severity,
Linear issue, file name and title. ``--full`` prints the text of every entry, grouped by area, as one
Markdown document. ``--check`` only validates the entry files. The script changes nothing
and uses the standard library only.
"""

from __future__ import annotations

import argparse
import re
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = REPO_ROOT / "docs" / "dev" / "design_issues"
OLD_LOG_DIR = REPO_ROOT / "design_issues"
OLD_LOG_FILE = REPO_ROOT / "DESIGN_ISSUES.md"

STATUSES = ("open", "in progress", "resolved")
SEVERITIES = ("critical", "high", "medium", "low", "trivial")

README_NAME = "README.md"
# The prefix of an entry that has no Linear issue (entries older than the issue rule).
NO_ISSUE = "NOISSUE"

_ISSUE_ID = r"[A-Z][A-Z0-9]*-[0-9]+"
# An optional "-<n>" after the entry id tells apart two entries that share prefix and id.
_ENTRY_NAME = re.compile(
    rf"^({NO_ISSUE}|{_ISSUE_ID})-([A-Z][A-Z0-9]*)(?:-([0-9]+))?\.md$"
)
# Entries older than this check that have no severity; their text is kept as written.
_NO_SEVERITY_ALLOWED = frozenset({"NOISSUE-PF1.md"})
_ENTRY_HEADING = re.compile(r"^### ([A-Z][A-Z0-9]*) — (\S.*)$")
_FIELD = re.compile(r"^\*\*(Status|Severity|Issue):\*\*\s*(.*)$")
_FENCE = re.compile(r"^ {0,3}(`{3,}|~{3,})(.*)$")
_ID_PARTS = re.compile(r"^([A-Z]+)([0-9]*)(.*)$")


class EntryError(ValueError):
    """An entry's text does not follow the convention.

    Attributes:
        problems: One sentence fragment per problem found, without the file name.
    """

    def __init__(self, problems: Sequence[str]) -> None:
        super().__init__("; ".join(problems))
        self.problems = tuple(problems)


@dataclass(frozen=True)
class Entry:
    """One design issue, as its file states it.

    Attributes:
        area: The ``## `` area heading line the entry belongs under.
        entry_id: The id in the ``### `` heading, for example ``GB1``.
        title: The heading text after the id.
        status: The full ``**Status:**`` value.
        severity: The ``**Severity:**`` value, or ``None`` when the entry has none.
        issue: The full ``**Issue:**`` value, or ``None`` when the entry has none.
        body: The entry's lines from its ``### `` heading to its last line.
    """

    area: str
    entry_id: str
    title: str
    status: str
    severity: str | None
    issue: str | None
    body: tuple[str, ...]

    @property
    def status_word(self) -> str:
        """The status without its qualification: one of ``STATUSES``."""
        return next(s for s in STATUSES if _starts_with_word(self.status, s))

    @property
    def issue_ids(self) -> tuple[str, ...]:
        """The Linear issue ids on the ``**Issue:**`` line, in order."""
        return tuple(re.findall(rf"\b{_ISSUE_ID}\b", self.issue or ""))


def outside_fences(lines: Sequence[str]) -> tuple[list[bool], bool]:
    """Find the fenced code blocks in Markdown lines.

    A block is closed only by a fence of the same character that is at least as long
    as the one that opened it.

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


def _starts_with_word(value: str, word: str) -> bool:
    """Whether ``value`` is ``word``, or ``word`` followed by a qualification."""
    return value == word or (value.startswith(word) and not value[len(word)].isalnum())


def parse_entry(text: str) -> Entry:
    """Read one entry file's text.

    Args:
        text: The file's full text.

    Returns:
        The entry.

    Raises:
        EntryError: If the text is empty, does not start with one ``## `` area heading
            followed by one ``### <ENTRY-ID> — <title>`` heading, lacks a valid
            ``**Status:**`` line directly under that heading, has a ``**Severity:**``
            value that is not a severity, has no text after its field lines, does not end with a newline, or has a code
            fence that is never closed.
    """
    if not text.strip():
        raise EntryError(["is empty"])

    lines = text.splitlines()
    problems: list[str] = []
    mask, unclosed_fence = outside_fences(lines)
    if unclosed_fence:
        problems.append("has a code fence that is never closed")
    if not text.endswith("\n"):
        problems.append("does not end with a newline")

    area_lines = [i for i, line in enumerate(lines) if mask[i] and line[:3] == "## "]
    entry_lines = [i for i, line in enumerate(lines) if mask[i] and line[:4] == "### "]
    if not area_lines or area_lines[0] != 0:
        problems.append("does not start with its '## <area heading>' line")
    elif not lines[0][3:].strip():
        problems.append("has an area heading with no text after '## '")
    if len(area_lines) > 1:
        problems.append("has more than one area heading ('## '); a file holds one")
    if len(entry_lines) > 1:
        problems.append("has more than one '### ' heading; a file holds one entry")

    heading = _ENTRY_HEADING.match(lines[entry_lines[0]]) if entry_lines else None
    if heading is None:
        problems.append("has no '### <ENTRY-ID> — <title>' heading")
        raise EntryError(problems)
    start = entry_lines[0]
    if any(line.strip() for line in lines[1:start]):
        problems.append("has text between the area heading and the entry heading")
    elif start == 1:
        problems.append("has no blank line between the area heading and the entry")

    fields: dict[str, str] = {}
    order: list[str] = []
    end = start + 1
    while end < len(lines) and (field := _FIELD.match(lines[end])):
        if field.group(1) in fields:
            problems.append(f"repeats the '**{field.group(1)}:**' line")
        fields[field.group(1)] = field.group(2).strip()
        order.append(field.group(1))
        end += 1
    if "Status" not in fields:
        problems.append("has no '**Status:**' line directly under the entry heading")
    expected = [name for name in ("Status", "Severity", "Issue") if name in fields]
    if list(dict.fromkeys(order)) != expected:
        problems.append("has its field lines out of order: Status, Severity, Issue")

    status = fields.get("Status")
    if status is not None and not any(_starts_with_word(status, s) for s in STATUSES):
        allowed = ", ".join(repr(s) for s in STATUSES)
        problems.append(f"has the unknown status {status!r} (allowed: {allowed})")
    severity = fields.get("Severity")
    if severity is not None and severity not in SEVERITIES:
        allowed = ", ".join(repr(s) for s in SEVERITIES)
        problems.append(f"has the unknown severity {severity!r} (allowed: {allowed})")
    if not any(line.strip() for line in lines[end:]):
        problems.append("has no text after its field lines")

    if problems or status is None:
        raise EntryError(problems)
    return Entry(
        area=lines[0].rstrip(),
        entry_id=heading.group(1),
        title=heading.group(2).rstrip(),
        status=status,
        severity=severity,
        issue=fields.get("Issue"),
        body=tuple(lines[start:]),
    )


def _file_problems(name: str, entry: Entry) -> list[str]:
    """Check an entry against the name of the file that holds it."""
    match = _ENTRY_NAME.match(name)
    assert match is not None  # callers pass only entry file names
    prefix, entry_id, _ = match.groups()
    problems: list[str] = []
    if entry.severity is None and name not in _NO_SEVERITY_ALLOWED:
        problems.append("has no '**Severity:**' line directly under the status line")
    if entry.entry_id != entry_id:
        problems.append(
            f"is named for entry {entry_id} but its heading says {entry.entry_id}"
        )
    if entry.issue is None and prefix != NO_ISSUE:
        problems.append(
            f"is named for {prefix} but has no '**Issue:**' line naming that issue"
        )
    elif entry.issue is not None and prefix == NO_ISSUE:
        first = (entry.issue_ids or ("<LINEAR-ISSUE-ID>",))[0]
        problems.append(
            f"has an '**Issue:**' line, so the prefix {NO_ISSUE} is wrong; "
            f"name it {first}-{entry.entry_id}.md"
        )
    elif entry.issue is not None and prefix not in entry.issue_ids:
        problems.append(
            f"is named for {prefix} but its '**Issue:**' line is {entry.issue!r}"
        )
    return problems


def load_entries(entries_dir: Path) -> tuple[list[tuple[Path, Entry]], list[str]]:
    """Read and validate every entry file in a directory.

    ``README.md`` and dotfiles are skipped. Every other item must be an entry file.

    Args:
        entries_dir: The ``design_issues`` directory.

    Returns:
        The valid entries as ``(path, entry)``, ordered by area and then entry id, and
        one message per problem found.
    """
    entries: list[tuple[Path, Entry]] = []
    problems: list[str] = []
    for path in sorted(entries_dir.iterdir()):
        if path.name == README_NAME or path.name.startswith("."):
            continue
        name = _ENTRY_NAME.match(path.name)
        if not path.is_file() or name is None:
            problems.append(
                f"{path}: not an entry file name; expected "
                "<LINEAR-ISSUE-ID>-<ENTRY-ID>.md, for example ITL-955-GB1.md "
                f"({NO_ISSUE}-<ENTRY-ID>.md for an old entry with no Linear issue)"
            )
            continue
        prefix, entry_id, number = name.groups()
        first = f"{prefix}-{entry_id}.md"
        if number is not None and not (
            int(number) >= 2 and (entries_dir / first).is_file()
        ):
            problems.append(
                f"{path}: a '-<n>' after the entry id (2 or more) is only for a "
                f"second entry with the same name, and there is no {first}"
            )
        try:
            entry = parse_entry(path.read_text(encoding="utf-8"))
        except EntryError as error:
            problems.extend(f"{path}: {problem}" for problem in error.problems)
            continue
        found = _file_problems(path.name, entry)
        problems.extend(f"{path}: {problem}" for problem in found)
        if not found:
            entries.append((path, entry))
    entries.sort(key=lambda item: (*_entry_order(item[1]), item[0].name))
    return entries, problems


def files_at_old_location() -> list[str]:
    """List the files left at the log's old locations.

    The old locations are the single-file log ``DESIGN_ISSUES.md`` (split in ITL-968) and
    the top-level ``design_issues/`` (moved in ITL-1169), both at the repository root.
    Dotfiles (such as ``.DS_Store``) are not counted.

    Returns:
        One message per file found, naming the new location; empty if there is none.
    """
    new = LOG_DIR.relative_to(REPO_ROOT).as_posix()
    problems: list[str] = []
    if OLD_LOG_FILE.is_file():
        problems.append(
            f"{OLD_LOG_FILE}: the design issues log is one file per entry in {new}/ "
            "(ITL-968, ITL-1169); move its entries there and delete this file"
        )
    if OLD_LOG_DIR.is_dir():
        problems.extend(
            f"{path}: the design issues log moved to {new}/ (ITL-1169); "
            f"move this file there (git mv design_issues/{path.name} {new}/)"
            for path in sorted(OLD_LOG_DIR.iterdir())
            if not path.name.startswith(".")
        )
    return problems


def _entry_order(entry: Entry) -> tuple[str, str, str, int, str]:
    """Sort key: area text without Markdown marks, the area heading, then the id.

    The heading itself keeps the entries of one area together when two headings
    differ only in their marks; the id orders by its letters, then its number.
    """
    area = re.sub(r"[`*_]", "", entry.area.removeprefix("## ")).casefold()
    parts = _ID_PARTS.match(entry.entry_id)
    assert parts is not None  # an entry id starts with a capital letter
    return area, entry.area, parts.group(1), int(parts.group(2) or 0), parts.group(3)


def format_index(named: Sequence[tuple[str, Entry]]) -> str:
    """Format entries as an index: one line per entry under its area heading.

    Args:
        named: Each entry with the name of its file, in the order to list them.

    Returns:
        The index text, ending with a count by status.
    """
    entries = [entry for _, entry in named]
    widths = [
        max((len(value) for value in column), default=0)
        for column in (
            [e.entry_id for e in entries],
            [e.status_word for e in entries],
            [e.severity or "-" for e in entries],
            [(e.issue_ids or ("-",))[0] for e in entries],
            [name for name, _ in named],
        )
    ]
    out: list[str] = []
    area: str | None = None
    for name, entry in named:
        if entry.area != area:
            area = entry.area
            out.extend(["", area, ""] if out else [area, ""])
        cells = (
            entry.entry_id,
            entry.status_word,
            entry.severity or "-",
            (entry.issue_ids or ("-",))[0],
            name,
        )
        padded = "  ".join(cell.ljust(width) for cell, width in zip(cells, widths))
        out.append(f"{padded}  {entry.title}")
    counts = ", ".join(
        f"{sum(e.status_word == status for e in entries)} {status}"
        for status in STATUSES
    )
    noun = "design issue" if len(entries) == 1 else "design issues"
    out.extend(["", f"{len(entries)} {noun}: {counts}"] if out else [f"0 {noun}"])
    return "\n".join(out) + "\n"


def format_full(entries: Sequence[Entry]) -> str:
    """Format entries as one Markdown document: each area heading, then its entries.

    Args:
        entries: The entries, in the order to print them.

    Returns:
        The document text.
    """
    out: list[str] = []
    area: str | None = None
    for entry in entries:
        if entry.area != area:
            area = entry.area
            out.extend(["---", "", area, ""] if out else [area, ""])
        out.extend([*entry.body, ""])
    return "\n".join(out)


def main(argv: Sequence[str] | None = None) -> int:
    """Validate the entry files and print the index, the full log, or a count.

    Args:
        argv: Command-line arguments; ``sys.argv[1:]`` when omitted.

    Returns:
        The process exit code: 0 on success, 1 if any entry file is malformed.
    """
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n\n")[0])
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--check", action="store_true", help="only validate the entry files"
    )
    mode.add_argument(
        "--full", action="store_true", help="print every entry's text, by area"
    )
    parser.add_argument(
        "--status", choices=STATUSES, help="list only the entries with this status"
    )
    parser.add_argument(
        "--dir",
        type=Path,
        default=LOG_DIR,
        help="directory holding the entry files",
    )
    args = parser.parse_args(argv)

    loaded, problems = load_entries(args.dir)
    problems = files_at_old_location() + problems
    if problems:
        print("\n".join(problems), file=sys.stderr)
        return 1
    named = [
        (path.name, entry)
        for path, entry in loaded
        if args.status is None or entry.status_word == args.status
    ]
    if args.check:
        noun = "design issue" if len(loaded) == 1 else "design issues"
        print(f"{len(loaded)} {noun} OK")
    elif args.full:
        sys.stdout.write(format_full([entry for _, entry in named]))
    else:
        sys.stdout.write(format_index(named))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
