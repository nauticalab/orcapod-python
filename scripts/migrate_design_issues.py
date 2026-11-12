"""Split a single-file design issues log into one file per entry (ITL-968).

``DESIGN_ISSUES.md`` used to hold every entry. This script moves each entry, text
unchanged, into ``design_issues/<PREFIX>-<ENTRY-ID>.md`` (see ``design_issues/README.md``):

- The file holds the entry's ``## `` area heading, a blank line, and the entry exactly as
  it stood in the log, from its ``### `` heading to its last line.
- ``<PREFIX>`` is the first Linear issue id on the entry's ``**Issue:**`` line, or
  ``NOISSUE`` for an entry with no such line.
- When two entries would get the same name, the later one in the log gets ``-2`` (then
  ``-3``, ...) after its entry id.

Usage:

    uv run python scripts/migrate_design_issues.py --source DESIGN_ISSUES.md
    uv run python scripts/migrate_design_issues.py --source DESIGN_ISSUES.md --verify

The first command writes the entry files (it never deletes the source or any other
file). ``--verify`` writes nothing: it rebuilds the source from its own preamble and
separators plus the entry files on disk, and fails unless the result equals the source
byte for byte.

A branch that still edits an entry in ``DESIGN_ISSUES.md`` can use the script while
merging ``main``; ``design_issues/README.md`` gives the steps. The branch's copy is older
than ``main`` for every entry ``main`` changed since, so keep only the rewritten files of
the entries the branch itself changed. The script uses the standard library only.
"""

from __future__ import annotations

import argparse
import re
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

# This one-off script stands alone: it shares no code with scripts/design_issues.py, which
# checks the files it writes.
NO_ISSUE = "NOISSUE"
_ENTRY_HEADING = re.compile(r"^### ([A-Z][A-Z0-9]*) — \S")
_ISSUE_LINE = re.compile(r"^\*\*Issue:\*\*\s*(.*)$")
_ISSUE_ID = re.compile(r"\b[A-Z][A-Z0-9]*-[0-9]+\b")
_FENCE = re.compile(r"^ {0,3}(`{3,}|~{3,})(.*)$")


class LogError(ValueError):
    """The single-file log cannot be split without losing or guessing text."""


@dataclass(frozen=True)
class Piece:
    """A run of consecutive lines of the log.

    Attributes:
        text: The lines, with their line endings.
        area: For an entry, the ``## `` heading line it stands under (no line ending);
            ``None`` for text between entries (the preamble, blank lines, ``---``
            separators and area headings).
    """

    text: str
    area: str | None = None


def outside_fences(lines: Sequence[str]) -> list[bool]:
    """Mark the lines that are not inside (or delimiting) a fenced code block.

    A block is closed only by a fence of the same character that is at least as long
    as the one that opened it.
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
    return mask


def split_log(text: str) -> list[Piece]:
    """Split the log into entries and the text between them.

    Joining ``piece.text`` over the result gives ``text`` back exactly. An entry runs
    from its ``### `` heading to the last non-blank line before the next ``### ``
    heading, ``## `` heading or ``---`` separator. Lines inside code fences never count
    as headings or separators.

    Args:
        text: The full text of the log.

    Returns:
        The pieces in order.

    Raises:
        LogError: If an entry comes before the first area heading, or text other than
            the preamble stands outside every entry.
    """
    lines = text.splitlines(keepends=True)
    bare = [line.rstrip("\r\n") for line in lines]
    mask = outside_fences(bare)

    pieces: list[Piece] = []
    between: list[str] = []
    entry: list[str] | None = None
    area: str | None = None

    def close_entry() -> None:
        """Add the open entry to ``pieces``; its trailing blank lines go to ``between``."""
        nonlocal entry
        if entry is None:
            return
        end = len(entry)
        while not entry[end - 1].strip():
            end -= 1
        assert area is not None  # an entry is opened only under an area heading
        pieces.append(Piece("".join(entry[:end]), area))
        between.extend(entry[end:])
        entry = None

    for number, (line, text_line, is_markdown) in enumerate(
        zip(lines, bare, mask, strict=True), start=1
    ):
        if is_markdown and text_line.startswith("### "):
            close_entry()
            if area is None:
                raise LogError(f"line {number}: an entry before the first area heading")
            if between:
                pieces.append(Piece("".join(between)))
                between = []
            entry = [line]
        elif is_markdown and (text_line.startswith("## ") or text_line == "---"):
            close_entry()
            between.append(line)
            if text_line.startswith("## "):
                area = text_line
        elif entry is not None:
            entry.append(line)
        elif area is not None and text_line.strip():
            raise LogError(f"line {number}: text outside every entry: {text_line!r}")
        else:
            between.append(line)
    close_entry()
    if between:
        pieces.append(Piece("".join(between)))
    return pieces


def entry_file_text(piece: Piece) -> str:
    """The text of the file that holds an entry: area heading, blank line, entry."""
    return f"{piece.area}\n\n{piece.text}"


def name_entries(pieces: Sequence[Piece]) -> list[str | None]:
    """Choose the file name of each entry.

    Args:
        pieces: The pieces of the log, in order.

    Returns:
        For each piece, its file name, or ``None`` for text between entries.

    Raises:
        LogError: If an entry's heading is not ``### <ENTRY-ID> — <title>``, or its
            ``**Issue:**`` line names no Linear issue.
    """
    names: list[str | None] = []
    used: dict[str, int] = {}
    for piece in pieces:
        if piece.area is None:
            names.append(None)
            continue
        lines = piece.text.splitlines()
        heading = _ENTRY_HEADING.match(lines[0])
        if heading is None:
            raise LogError(f"{lines[0]!r}: not a '### <ENTRY-ID> — <title>' heading")
        # The field lines stand directly under the heading, before the first blank line.
        fields = lines[1 : lines.index("") if "" in lines else len(lines)]
        issue = next((m.group(1) for f in fields if (m := _ISSUE_LINE.match(f))), None)
        if issue is None:
            prefix = NO_ISSUE
        elif found := _ISSUE_ID.search(issue):
            prefix = found.group(0)
        else:
            raise LogError(f"{lines[0]!r}: no Linear issue id in {issue!r}")
        stem = f"{prefix}-{heading.group(1)}"
        used[stem] = used.get(stem, 0) + 1
        names.append(f"{stem}.md" if used[stem] == 1 else f"{stem}-{used[stem]}.md")
    return names


def main(argv: Sequence[str] | None = None) -> int:
    """Write the entry files, or with ``--verify`` check them against the source.

    Args:
        argv: Command-line arguments; ``sys.argv[1:]`` when omitted.

    Returns:
        The process exit code: 0 on success, 1 if the log cannot be split or the
        entry files do not reproduce it.
    """
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n\n")[0])
    parser.add_argument(
        "--source", type=Path, required=True, help="the single-file log to split"
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=REPO_ROOT / "design_issues",
        help="directory that receives the entry files",
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="write nothing; check that the entry files reproduce the source",
    )
    args = parser.parse_args(argv)

    source = args.source.read_bytes().decode("utf-8")
    try:
        pieces = split_log(source)
        names = name_entries(pieces)
    except LogError as error:
        print(f"{args.source}: {error}", file=sys.stderr)
        return 1
    entries = [(name, piece) for name, piece in zip(names, pieces) if name is not None]
    if not entries:
        print(f"{args.source}: no '### ' entry found", file=sys.stderr)
        return 1

    if args.verify:
        return _verify(source, pieces, names, args.out)

    args.out.mkdir(parents=True, exist_ok=True)
    counts = {"created": 0, "updated": 0, "unchanged": 0}
    for name, piece in entries:
        path = args.out / name
        if path.is_symlink():
            print(f"{path}: is a symbolic link; not written", file=sys.stderr)
            return 1
        new = entry_file_text(piece).encode("utf-8")
        if not path.exists():
            outcome = "created"
        elif path.read_bytes() == new:
            outcome = "unchanged"
        else:
            outcome = "updated"
        counts[outcome] += 1
        if outcome != "unchanged":
            path.write_bytes(new)
            print(f"{outcome}: {path}")
    summary = ", ".join(f"{count} {outcome}" for outcome, count in counts.items())
    print(f"{len(entries)} entries from {args.source}: {summary}")
    return 0


def _verify(
    source: str, pieces: Sequence[Piece], names: Sequence[str | None], out: Path
) -> int:
    """Rebuild the source from the entry files on disk and compare."""
    rebuilt: list[str] = []
    problems: list[str] = []
    for name, piece in zip(names, pieces, strict=True):
        if name is None:
            rebuilt.append(piece.text)
            continue
        path = out / name
        if not path.is_file():
            problems.append(f"{path}: missing")
            continue
        text = path.read_bytes().decode("utf-8")
        area, blank, entry = text.partition("\n\n")
        if area != piece.area or not blank:
            problems.append(
                f"{path}: does not start with the area heading {piece.area!r}"
            )
        if entry != piece.text:
            problems.append(f"{path}: the entry text differs from the source")
        rebuilt.append(entry)
    if not problems and "".join(rebuilt) != source:
        problems.append("the rebuilt text differs from the source")
    if problems:
        print("\n".join(problems), file=sys.stderr)
        return 1

    between = "".join(p.text for p in pieces if p.area is None).splitlines()
    first_area = next(
        (i for i, line in enumerate(between) if line.startswith("## ")), len(between)
    )
    rest = between[first_area:]
    total = len(source.encode("utf-8"))
    in_entries = sum(len(p.text.encode("utf-8")) for p in pieces if p.area is not None)
    print(
        f"{sum(name is not None for name in names)} entry files reproduce the source "
        f"byte for byte: {total} bytes = {in_entries} in entries + "
        f"{total - in_entries} outside them"
    )
    print(
        f"outside the entries: {first_area} preamble lines, "
        f"{sum(line.startswith('## ') for line in rest)} '## ' area heading lines, "
        f"{sum(line == '---' for line in rest)} '---' lines, "
        f"{sum(not line.strip() for line in rest)} blank lines"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
