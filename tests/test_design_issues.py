"""The design issues log (``docs/dev/design_issues/``), one file per entry (ITL-968).

Every test drives ``scripts/design_issues.py`` as a command, the way a developer and CI
use it.
"""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "scripts" / "design_issues.py"

ENTRY = """\
## `src/x.py` — things

### AB1 — A title with `code`
**Status:** open
**Severity:** medium
**Issue:** ITL-7

Body text.
"""


def _run(script: Path, *args: str | Path) -> subprocess.CompletedProcess[str]:
    """Run a script with the given arguments and capture its output."""
    return subprocess.run(
        [sys.executable, str(script), *map(str, args)],
        capture_output=True,
        text=True,
        check=False,
    )


@pytest.fixture
def entries(tmp_path: Path) -> Path:
    """An entries directory holding only its README."""
    directory = tmp_path / "design_issues"
    directory.mkdir()
    (directory / "README.md").write_text("# Design issues\n", encoding="utf-8")
    return directory


def _check(directory: Path) -> subprocess.CompletedProcess[str]:
    """Run ``design_issues.py --check`` on a directory."""
    return _run(SCRIPT, "--check", "--dir", directory)


def _write(directory: Path, name: str, text: str) -> None:
    """Write one file into an entries directory."""
    (directory / name).write_text(text, encoding="utf-8")


# --- the repository's own log -------------------------------------------------------


def test_repository_entries_are_well_formed() -> None:
    """Every file in ``docs/dev/design_issues/`` except ``README.md`` is a valid entry."""
    result = _run(SCRIPT, "--check")
    assert result.returncode == 0, result.stderr


def test_repository_log_has_a_readme() -> None:
    assert (REPO_ROOT / "docs" / "dev" / "design_issues" / "README.md").is_file()


def test_the_single_file_log_is_not_brought_back() -> None:
    """Entries live in ``docs/dev/design_issues/``; ``DESIGN_ISSUES.md`` no longer exists."""
    assert not (REPO_ROOT / "DESIGN_ISSUES.md").exists()


@pytest.fixture
def repo_copy(tmp_path: Path) -> Path:
    """A repository holding the script and an empty log at ``docs/dev/design_issues``."""
    (tmp_path / "scripts").mkdir()
    shutil.copy(SCRIPT, tmp_path / "scripts" / SCRIPT.name)
    log = tmp_path / "docs" / "dev" / "design_issues"
    log.mkdir(parents=True)
    (log / "README.md").write_text("# Design issues\n", encoding="utf-8")
    return tmp_path


def test_check_refuses_entries_at_the_old_location(repo_copy: Path) -> None:
    """An entry added at the top-level ``design_issues/`` (moved in ITL-1169) fails."""
    old = repo_copy / "design_issues"
    old.mkdir()
    _write(old, "ITL-7-AB1.md", ENTRY)
    result = _run(repo_copy / "scripts" / SCRIPT.name, "--check")
    assert result.returncode == 1
    assert "ITL-7-AB1.md" in result.stderr
    assert "docs/dev/design_issues" in result.stderr


def test_check_refuses_a_restored_single_file_log(repo_copy: Path) -> None:
    """A ``DESIGN_ISSUES.md`` at the repository root fails beside a valid log."""
    (repo_copy / "DESIGN_ISSUES.md").write_text("# Design issues\n", encoding="utf-8")
    result = _run(repo_copy / "scripts" / SCRIPT.name, "--check")
    assert result.returncode == 1
    assert "DESIGN_ISSUES.md" in result.stderr
    assert "docs/dev/design_issues" in result.stderr


def test_check_refuses_a_top_level_superpowers_directory(repo_copy: Path) -> None:
    """Development documents live under ``docs/dev/`` (ITL-1169): a file left in a
    top-level ``superpowers/`` fails, naming where it belongs."""
    plans = repo_copy / "superpowers" / "plans"
    plans.mkdir(parents=True)
    (plans / "2026-11-16-a-plan.md").write_text("# A plan\n", encoding="utf-8")
    result = _run(repo_copy / "scripts" / SCRIPT.name, "--check")
    assert result.returncode == 1
    assert "2026-11-16-a-plan.md" in result.stderr
    assert "docs/dev/plans/2026-11-16-a-plan.md" in result.stderr


def test_check_ignores_an_old_location_holding_only_dotfiles(repo_copy: Path) -> None:
    for name in ("design_issues", "superpowers"):
        old = repo_copy / name
        old.mkdir()
        (old / ".DS_Store").write_text("", encoding="utf-8")
    result = _run(repo_copy / "scripts" / SCRIPT.name, "--check")
    assert result.returncode == 0, result.stderr


# --- --check ------------------------------------------------------------------------


def test_check_accepts_a_directory_with_only_the_readme(entries: Path) -> None:
    result = _check(entries)
    assert result.returncode == 0, result.stderr


def test_check_accepts_a_valid_entry(entries: Path) -> None:
    _write(entries, "ITL-7-AB1.md", ENTRY)
    result = _check(entries)
    assert result.returncode == 0, result.stderr
    assert "1 design issue" in result.stdout


def test_check_accepts_an_entry_without_a_linear_issue(entries: Path) -> None:
    _write(entries, "NOISSUE-AB1.md", ENTRY.replace("**Issue:** ITL-7\n", ""))
    result = _check(entries)
    assert result.returncode == 0, result.stderr


def test_check_accepts_a_numbered_name_for_a_repeated_entry_id(entries: Path) -> None:
    """Two old entries share the id ``H1``; the second file is ``NOISSUE-H1-2.md``."""
    text = ENTRY.replace("**Issue:** ITL-7\n", "")
    _write(entries, "NOISSUE-AB1.md", text)
    _write(entries, "NOISSUE-AB1-2.md", text.replace("A title", "Another title"))
    result = _check(entries)
    assert result.returncode == 0, result.stderr
    assert "2 design issues" in result.stdout


@pytest.mark.parametrize("name", ["ITL-7-AB1-2.md", "ITL-7-AB1-1.md"])
def test_check_rejects_a_numbered_name_that_tells_nothing_apart(
    entries: Path, name: str
) -> None:
    """``-2`` needs the unnumbered file beside it, and ``-1`` is never a second."""
    if name.endswith("-1.md"):
        _write(entries, "ITL-7-AB1.md", ENTRY)
    _write(entries, name, ENTRY)
    result = _check(entries)
    assert result.returncode == 1
    assert name in result.stderr
    assert "ITL-7-AB1.md" in result.stderr


@pytest.mark.parametrize(
    "issue_line",
    ["ITL-7, ITL-9 (first case only)", "PLT-4816 (related: ITL-7)"],
)
def test_check_accepts_a_name_that_is_any_id_on_the_issue_line(
    entries: Path, issue_line: str
) -> None:
    _write(entries, "ITL-7-AB1.md", ENTRY.replace("ITL-7\n", issue_line + "\n"))
    result = _check(entries)
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize(
    "status",
    ["open", "in progress", "resolved", "resolved (local stores); cloud stores remain"],
)
def test_check_accepts_each_status(entries: Path, status: str) -> None:
    _write(entries, "ITL-7-AB1.md", ENTRY.replace("open", status))
    result = _check(entries)
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize(
    "name",
    [
        "AB1.md",
        "ITL-7.md",
        "itl-7-AB1.md",
        "ITL-7-ab1.md",
        "ITL-7-AB1.txt",
        "ITL_7-AB1.md",
        "ITL-7-AB1-b.md",
        "NOISSUE.md",
        "notes.md",
    ],
)
def test_check_rejects_a_wrong_file_name(entries: Path, name: str) -> None:
    _write(entries, name, ENTRY)
    result = _check(entries)
    assert result.returncode == 1
    assert name in result.stderr


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("", "empty"),
        ("\n \n", "empty"),
        (ENTRY.removeprefix("## `src/x.py` — things\n\n"), "area heading"),
        (ENTRY.replace("## `src/x.py` — things", "## "), "no text after '## '"),
        (ENTRY.replace("things\n\n###", "things\n###"), "no blank line"),
        (ENTRY.replace("things\n\n###", "things\nStray.\n\n###"), "text between"),
        (ENTRY.replace("### AB1 — A title", "### AB1 A title"), "### <ENTRY-ID> — "),
        (ENTRY.replace("### AB1", "### AB2"), "AB2"),
        (ENTRY.replace("**Status:** open\n", ""), "**Status:**"),
        (ENTRY.replace("open", "done"), "'done'"),
        (ENTRY.replace("open", "Open"), "'Open'"),
        (ENTRY.replace("open", "opened"), "'opened'"),
        (ENTRY.replace("**Severity:** medium\n", ""), "**Severity:**"),
        (ENTRY.replace("medium", "urgent"), "'urgent'"),
        (ENTRY.replace("**Issue:** ITL-7\n", ""), "**Issue:**"),
        (ENTRY.replace("ITL-7\n", "ITL-70\n"), "ITL-7"),
        (ENTRY.replace("\nBody text.\n", ""), "no text"),
        (ENTRY + "\n### AB2 — Another\n**Status:** open\n", "one entry"),
        (ENTRY + "\n## Another area\n", "one area heading"),
        (ENTRY + "\n```python\nx = 1\n", "never closed"),
    ],
)
def test_check_rejects_a_malformed_entry(
    entries: Path, text: str, expected: str
) -> None:
    _write(entries, "ITL-7-AB1.md", text)
    result = _check(entries)
    assert result.returncode == 1
    assert "ITL-7-AB1.md" in result.stderr
    assert expected in result.stderr


def test_check_rejects_an_issue_line_in_a_file_named_noissue(entries: Path) -> None:
    _write(entries, "NOISSUE-AB1.md", ENTRY)
    result = _check(entries)
    assert result.returncode == 1
    assert "NOISSUE-AB1.md" in result.stderr
    assert "ITL-7-AB1.md" in result.stderr  # the name it should have


def test_check_ignores_heading_like_lines_inside_a_code_fence(entries: Path) -> None:
    _write(
        entries,
        "ITL-7-AB1.md",
        ENTRY + "\n```python\n# TODO: a comment\n## not an area\n### ZZ9 — no\n```\n",
    )
    result = _check(entries)
    assert result.returncode == 0, result.stderr


def test_check_reports_every_problem(entries: Path) -> None:
    _write(entries, "ITL-7-AB1.md", ENTRY.replace("open", "done"))
    _write(entries, "wrong.md", ENTRY)
    result = _check(entries)
    assert result.returncode == 1
    assert "ITL-7-AB1.md" in result.stderr
    assert "wrong.md" in result.stderr


# --- reading the log as a whole -----------------------------------------------------


@pytest.fixture
def log(entries: Path) -> Path:
    """Three entries in two areas."""
    _write(entries, "ITL-7-AB10.md", ENTRY.replace("AB1", "AB10"))
    _write(
        entries,
        "NOISSUE-AB2.md",
        ENTRY.replace("AB1 — A title with `code`", "AB2 — Second")
        .replace("open", "resolved")
        .replace("medium", "high")
        .replace("**Issue:** ITL-7\n", ""),
    )
    _write(
        entries,
        "PLT-3-Z1.md",
        "## An area that sorts first\n\n### Z1 — Third\n**Status:** in progress\n"
        "**Severity:** low\n**Issue:** PLT-3\n\nThird body.\n",
    )
    return entries


def test_index_lists_every_entry_by_area(log: Path) -> None:
    result = _run(SCRIPT, "--dir", log)
    assert result.returncode == 0, result.stderr
    lines = [line for line in result.stdout.splitlines() if line.strip()]
    assert lines[0] == "## An area that sorts first"
    assert lines[1].split() == [
        "Z1",
        "in",
        "progress",
        "low",
        "PLT-3",
        "PLT-3-Z1.md",
        "Third",
    ]
    assert lines[2] == "## `src/x.py` — things"
    # Entry ids order by their number: AB2 before AB10.
    assert lines[3].split() == [
        "AB2",
        "resolved",
        "high",
        "-",
        "NOISSUE-AB2.md",
        "Second",
    ]
    assert lines[4].split()[:5] == [
        "AB10",
        "open",
        "medium",
        "ITL-7",
        "ITL-7-AB10.md",
    ]
    assert lines[4].endswith("A title with `code`")
    assert lines[5] == "3 design issues: 1 open, 1 in progress, 1 resolved"


def test_full_keeps_an_area_together_when_headings_differ_only_in_marks(
    entries: Path,
) -> None:
    for number, area in ((1, "## Foo"), (2, "## `Foo`"), (3, "## Foo")):
        _write(
            entries,
            f"ITL-7-AB{number}.md",
            ENTRY.replace("## `src/x.py` — things", area).replace("AB1", f"AB{number}"),
        )
    result = _run(SCRIPT, "--dir", entries, "--full")
    assert result.returncode == 0, result.stderr
    assert result.stdout.count("## Foo\n") == 1
    assert result.stdout.count("## `Foo`\n") == 1


def test_index_filters_by_status(log: Path) -> None:
    result = _run(SCRIPT, "--dir", log, "--status", "open")
    assert result.returncode == 0, result.stderr
    assert "AB10" in result.stdout
    assert "AB2 " not in result.stdout
    assert "Z1" not in result.stdout


def test_full_prints_the_whole_log_with_each_area_heading_once(log: Path) -> None:
    result = _run(SCRIPT, "--dir", log, "--full")
    assert result.returncode == 0, result.stderr
    assert result.stdout.count("## `src/x.py` — things\n") == 1
    assert result.stdout.index("### Z1 — Third") < result.stdout.index("### AB2 — ")
    assert result.stdout.index("### AB2 — ") < result.stdout.index("### AB10 — ")
    assert "Third body.\n" in result.stdout
    assert result.stdout.count("Body text.\n") == 2


def test_index_refuses_a_malformed_log(log: Path) -> None:
    _write(log, "wrong.md", ENTRY)
    result = _run(SCRIPT, "--dir", log)
    assert result.returncode == 1
    assert "wrong.md" in result.stderr
