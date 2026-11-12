"""Changelog fragments (``changelog.d/``) and the release fold (ITL-961).

Every test drives ``scripts/assemble_changelog.py`` as a command, the way a release PR
and CI use it.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "scripts" / "assemble_changelog.py"

CHANGELOG = """\
# Changelog

## [0.3.0] — unreleased

### Breaking changes and migration

#### Arrays changed (ITL-1)

Existing paragraph.

### Added

- Existing added one. (ITL-2)
- Existing added two,
  wrapped. (ITL-3)

### Fixed

#### A fix with a sub-heading (ITL-4)

Existing fixed paragraph.

## [0.2.0] — 2026-11-10

Intro text.

### Added

- Old added. (ITL-0)

### Changed

- Old changed.
"""


def _run(*args: str | Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCRIPT), *map(str, args)],
        capture_output=True,
        text=True,
        check=False,
    )


@pytest.fixture
def workspace(tmp_path: Path) -> tuple[Path, Path]:
    """A changelog file and a fragments directory holding only its README."""
    changelog = tmp_path / "CHANGELOG.md"
    changelog.write_text(CHANGELOG, encoding="utf-8")
    fragments = tmp_path / "changelog.d"
    fragments.mkdir()
    (fragments / "README.md").write_text("# Fragments\n", encoding="utf-8")
    return changelog, fragments


def _check(fragments: Path) -> subprocess.CompletedProcess[str]:
    return _run("--check", "--fragments-dir", fragments)


def _fold(
    changelog: Path, fragments: Path, version: str = "0.3.0"
) -> subprocess.CompletedProcess[str]:
    return _run(
        "--version", version, "--changelog", changelog, "--fragments-dir", fragments
    )


# --- the repository's own fragments -------------------------------------------------


def test_repository_fragments_are_well_formed() -> None:
    """Every file in ``changelog.d/`` except ``README.md`` is a valid fragment."""
    result = _run("--check")
    assert result.returncode == 0, result.stderr


def test_repository_fragments_directory_has_a_readme() -> None:
    assert (REPO_ROOT / "changelog.d" / "README.md").is_file()


# --- --check ------------------------------------------------------------------------


def test_check_accepts_a_directory_with_only_the_readme(
    workspace: tuple[Path, Path],
) -> None:
    result = _check(workspace[1])
    assert result.returncode == 0, result.stderr


def test_check_accepts_a_valid_fragment(workspace: tuple[Path, Path]) -> None:
    _, fragments = workspace
    (fragments / "ITL-955.md").write_text(
        "### Added\n\n- New thing. (ITL-955)\n\n"
        "### Fixed\n\n#### A sub-heading\n\nParagraph.\n",
        encoding="utf-8",
    )
    result = _check(fragments)
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize(
    "name",
    [
        "itl-955.md",
        "ITL-955.txt",
        "955.md",
        "ITL-955-extra.md",
        "notes.md",
        "ITL_955.md",
    ],
)
def test_check_rejects_a_wrong_file_name(
    workspace: tuple[Path, Path], name: str
) -> None:
    _, fragments = workspace
    (fragments / name).write_text("### Added\n\n- Thing.\n", encoding="utf-8")
    result = _check(fragments)
    assert result.returncode == 1
    assert name in result.stderr


@pytest.mark.parametrize(
    ("body", "expected"),
    [
        ("### Add\n\n- Thing.\n", "### Add"),
        ("### Breaking changes\n\n- Thing.\n", "### Breaking changes"),
        ("### added\n\n- Thing.\n", "### added"),
        ("## [0.3.0] — unreleased\n\n### Added\n\n- Thing.\n", "## [0.3.0]"),
        ("- Thing with no heading.\n", "before the first"),
        ("### Added\n\n### Fixed\n\n- Thing.\n", "### Added"),
        ("### Added\n\n- One.\n\n### Added\n\n- Two.\n", "### Added"),
        ("", "empty"),
        ("\n\n  \n", "empty"),
    ],
)
def test_check_rejects_a_malformed_fragment(
    workspace: tuple[Path, Path], body: str, expected: str
) -> None:
    _, fragments = workspace
    (fragments / "ITL-7.md").write_text(body, encoding="utf-8")
    result = _check(fragments)
    assert result.returncode == 1
    assert "ITL-7.md" in result.stderr
    assert expected in result.stderr


def test_check_ignores_heading_like_lines_inside_a_code_fence(
    workspace: tuple[Path, Path],
) -> None:
    _, fragments = workspace
    (fragments / "ITL-7.md").write_text(
        "### Added\n\n- Thing:\n\n```python\n### not a heading\n## nor this\n```\n",
        encoding="utf-8",
    )
    result = _check(fragments)
    assert result.returncode == 0, result.stderr


def test_check_keeps_a_shorter_fence_inside_a_longer_one(
    workspace: tuple[Path, Path],
) -> None:
    """A three-backtick line does not close a four-backtick block."""
    _, fragments = workspace
    (fragments / "ITL-7.md").write_text(
        "### Added\n\n- Thing:\n\n````markdown\n```\n### Nope\n```\n````\n",
        encoding="utf-8",
    )
    result = _check(fragments)
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("fence", ["```", "~~~", "````text"])
def test_check_rejects_an_unclosed_code_fence(
    workspace: tuple[Path, Path], fence: str
) -> None:
    _, fragments = workspace
    (fragments / "ITL-7.md").write_text(
        f"### Added\n\n- Thing:\n\n{fence}\nx = 1\n", encoding="utf-8"
    )
    result = _check(fragments)
    assert result.returncode == 1
    assert "ITL-7.md" in result.stderr
    assert "never closed" in result.stderr


def test_fold_refuses_a_fragment_with_an_unclosed_code_fence(
    workspace: tuple[Path, Path],
) -> None:
    changelog, fragments = workspace
    (fragments / "ITL-7.md").write_text(
        "### Added\n\n- Thing:\n\n```\nx = 1\n", encoding="utf-8"
    )
    result = _fold(changelog, fragments)
    assert result.returncode == 1
    assert "never closed" in result.stderr
    assert changelog.read_text(encoding="utf-8") == CHANGELOG
    assert (fragments / "ITL-7.md").exists()


def test_check_reports_every_problem(workspace: tuple[Path, Path]) -> None:
    _, fragments = workspace
    (fragments / "ITL-7.md").write_text("", encoding="utf-8")
    (fragments / "wrong.md").write_text("### Added\n\n- Thing.\n", encoding="utf-8")
    result = _check(fragments)
    assert result.returncode == 1
    assert "ITL-7.md" in result.stderr
    assert "wrong.md" in result.stderr


def test_check_changes_nothing(workspace: tuple[Path, Path]) -> None:
    changelog, fragments = workspace
    (fragments / "ITL-7.md").write_text("### Added\n\n- Thing.\n", encoding="utf-8")
    result = _run("--check", "--changelog", changelog, "--fragments-dir", fragments)
    assert result.returncode == 0, result.stderr
    assert changelog.read_text(encoding="utf-8") == CHANGELOG
    assert (fragments / "ITL-7.md").exists()


# --- fold ---------------------------------------------------------------------------


def test_fold_appends_to_existing_categories_in_issue_order(
    workspace: tuple[Path, Path],
) -> None:
    changelog, fragments = workspace
    (fragments / "ITL-100.md").write_text(
        "### Added\n\n- Added by 100. (ITL-100)\n\n"
        "### Fixed\n\n- Fixed by 100. (ITL-100)\n",
        encoding="utf-8",
    )
    # 20 sorts before 100 numerically, although "ITL-100" < "ITL-20" as text.
    (fragments / "ITL-20.md").write_text(
        "### Added\n\n- Added by 20,\n  wrapped. (ITL-20)\n\n"
        "### Breaking changes and migration\n\n#### Break by 20 (ITL-20)\n\nWhy.\n",
        encoding="utf-8",
    )

    result = _fold(changelog, fragments)

    assert result.returncode == 0, result.stderr
    assert (
        changelog.read_text(encoding="utf-8")
        == """\
# Changelog

## [0.3.0] — unreleased

### Breaking changes and migration

#### Arrays changed (ITL-1)

Existing paragraph.

#### Break by 20 (ITL-20)

Why.

### Added

- Existing added one. (ITL-2)
- Existing added two,
  wrapped. (ITL-3)
- Added by 20,
  wrapped. (ITL-20)
- Added by 100. (ITL-100)

### Fixed

#### A fix with a sub-heading (ITL-4)

Existing fixed paragraph.

- Fixed by 100. (ITL-100)

## [0.2.0] — 2026-11-10

Intro text.

### Added

- Old added. (ITL-0)

### Changed

- Old changed.
"""
    )


def test_fold_deletes_the_fragments_and_keeps_the_readme(
    workspace: tuple[Path, Path],
) -> None:
    changelog, fragments = workspace
    (fragments / "ITL-7.md").write_text("### Added\n\n- Thing.\n", encoding="utf-8")
    (fragments / "PLT-8.md").write_text("### Fixed\n\n- Other.\n", encoding="utf-8")

    result = _fold(changelog, fragments)

    assert result.returncode == 0, result.stderr
    assert sorted(p.name for p in fragments.iterdir()) == ["README.md"]


def test_fold_inserts_a_missing_category_in_canonical_order(
    workspace: tuple[Path, Path],
) -> None:
    changelog, fragments = workspace
    (fragments / "ITL-7.md").write_text(
        "### Known limitations\n\n- A limit. (ITL-7)\n\n"
        "### Changed\n\n- A change. (ITL-7)\n",
        encoding="utf-8",
    )

    result = _fold(changelog, fragments)

    assert result.returncode == 0, result.stderr
    text = changelog.read_text(encoding="utf-8")
    section = text[: text.index("## [0.2.0]")]
    headings = [line for line in section.splitlines() if line.startswith("### ")]
    assert headings == [
        "### Breaking changes and migration",
        "### Added",
        "### Changed",
        "### Fixed",
        "### Known limitations",
    ]
    assert "### Changed\n\n- A change. (ITL-7)\n\n### Fixed\n" in section
    assert section.endswith("### Known limitations\n\n- A limit. (ITL-7)\n\n")
    # The released section is untouched.
    assert (
        text[text.index("## [0.2.0]") :] == CHANGELOG[CHANGELOG.index("## [0.2.0]") :]
    )


def test_fold_targets_only_the_named_section(workspace: tuple[Path, Path]) -> None:
    changelog, fragments = workspace
    (fragments / "ITL-7.md").write_text(
        "### Changed\n\n- Late. (ITL-7)\n", encoding="utf-8"
    )

    result = _fold(changelog, fragments, version="0.2.0")

    assert result.returncode == 0, result.stderr
    text = changelog.read_text(encoding="utf-8")
    cut = CHANGELOG.index("## [0.2.0]")
    assert text[:cut] == CHANGELOG[:cut]
    assert text.endswith("### Changed\n\n- Old changed.\n- Late. (ITL-7)\n")


def test_fold_creates_a_missing_release_section_on_top(
    workspace: tuple[Path, Path],
) -> None:
    changelog, fragments = workspace
    (fragments / "ITL-7.md").write_text(
        "### Fixed\n\n- A fix. (ITL-7)\n\n### Added\n\n- A thing. (ITL-7)\n",
        encoding="utf-8",
    )

    result = _fold(changelog, fragments, version="0.4.0")

    assert result.returncode == 0, result.stderr
    assert changelog.read_text(encoding="utf-8") == CHANGELOG.replace(
        "## [0.3.0] — unreleased\n",
        "## [0.4.0] — unreleased\n\n"
        "### Added\n\n- A thing. (ITL-7)\n\n"
        "### Fixed\n\n- A fix. (ITL-7)\n\n"
        "## [0.3.0] — unreleased\n",
    )


def test_fold_does_not_treat_a_code_fence_line_as_a_heading(
    workspace: tuple[Path, Path],
) -> None:
    changelog, fragments = workspace
    changelog.write_text(
        CHANGELOG.replace(
            "Existing paragraph.\n",
            "Existing paragraph.\n\n```text\n### Added\n## [0.2.0]\n```\n",
        ),
        encoding="utf-8",
    )
    (fragments / "ITL-7.md").write_text(
        "### Added\n\n- A thing. (ITL-7)\n", encoding="utf-8"
    )

    result = _fold(changelog, fragments)

    assert result.returncode == 0, result.stderr
    text = changelog.read_text(encoding="utf-8")
    assert "  wrapped. (ITL-3)\n- A thing. (ITL-7)\n\n### Fixed\n" in text
    assert "```text\n### Added\n## [0.2.0]\n```\n\n### Added\n" in text


def test_fold_refuses_and_changes_nothing_when_a_fragment_is_malformed(
    workspace: tuple[Path, Path],
) -> None:
    changelog, fragments = workspace
    (fragments / "ITL-7.md").write_text("### Added\n\n- Thing.\n", encoding="utf-8")
    (fragments / "ITL-8.md").write_text("### Nope\n\n- Thing.\n", encoding="utf-8")

    result = _fold(changelog, fragments)

    assert result.returncode == 1
    assert "ITL-8.md" in result.stderr
    assert changelog.read_text(encoding="utf-8") == CHANGELOG
    assert (fragments / "ITL-7.md").exists()
    assert (fragments / "ITL-8.md").exists()


def test_fold_with_no_fragments_changes_nothing(workspace: tuple[Path, Path]) -> None:
    changelog, fragments = workspace
    result = _fold(changelog, fragments)
    assert result.returncode == 0, result.stderr
    assert changelog.read_text(encoding="utf-8") == CHANGELOG


def test_fold_requires_a_version(workspace: tuple[Path, Path]) -> None:
    changelog, fragments = workspace
    result = _run("--changelog", changelog, "--fragments-dir", fragments)
    assert result.returncode != 0
    assert "--version" in result.stderr
