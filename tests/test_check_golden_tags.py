"""The release-tag check of the golden files (ITL-946): ``scripts/check_golden_tags.py``.

Every test runs the script as a command against a temporary git repository holding a copy of
the golden tree, released with ``generate_golden_hashes.py --release`` and tagged.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

from orcapod.contexts import get_default_context
from tests.test_hashing.golden import store
from tests.test_hashing.golden.generate_golden_hashes import dump, main

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "scripts" / "check_golden_tags.py"
VERSION = get_default_context().version


def _env(*, ci: bool, ceiling: Path | None = None) -> dict[str, str]:
    env = {
        k: v for k, v in os.environ.items() if k != "CI" and not k.startswith("GIT_")
    }
    env.update(
        GIT_CONFIG_GLOBAL=os.devnull,
        GIT_CONFIG_NOSYSTEM="1",
        GIT_AUTHOR_NAME="test",
        GIT_AUTHOR_EMAIL="test@example.invalid",
        GIT_COMMITTER_NAME="test",
        GIT_COMMITTER_EMAIL="test@example.invalid",
    )
    if ci:
        env["CI"] = "true"
    if ceiling is not None:
        env["GIT_CEILING_DIRECTORIES"] = str(ceiling)
    return env


def _git(repo: Path, *args: str) -> None:
    _ = subprocess.run(
        ["git", "-C", str(repo), *args],
        check=True,
        capture_output=True,
        env=_env(ci=False),
    )


def _commit(repo: Path, message: str) -> None:
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "--no-gpg-sign", "-m", message)


def _check(repo: Path, *, ci: bool = False) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCRIPT), "--repo", str(repo)],
        capture_output=True,
        text=True,
        check=False,
        env=_env(ci=ci, ceiling=repo.parent),
    )


def _folder(repo: Path) -> Path:
    return store.golden_folder(
        repo, store.read_json(store.manifest_path(repo, VERSION))
    )


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    """A repository with an open commit tagged ``v0.2.0`` and a release tagged ``v0.3.0``."""
    root = tmp_path / "repo"
    store.copy_tree(store.PROJECT_ROOT, root, VERSION)
    store.reopen_tree(root, VERSION, "2026-11-15")
    _git(root.parent, "init", "-q", str(root))
    _commit(root, "open data context")
    _git(root, "tag", "v0.2.0")
    main(["--root", str(root), "--release", "0.3.0"])
    _commit(root, "release 0.3.0")
    _git(root, "tag", "v0.3.0")
    return root


def test_head_at_the_release_passes(repo: Path) -> None:
    result = _check(repo, ci=True)
    assert result.returncode == 0, result.stderr
    assert "v0.3.0" in result.stdout


def test_a_tag_that_released_nothing_is_not_checked(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    store.copy_tree(store.PROJECT_ROOT, root, VERSION)
    store.reopen_tree(root, VERSION, "2026-11-15")
    _git(root.parent, "init", "-q", str(root))
    _commit(root, "open data context")
    _git(root, "tag", "v0.2.0")
    result = _check(root, ci=True)
    assert result.returncode == 0, result.stderr
    assert "nothing to check" in result.stdout


def test_a_golden_file_added_after_the_release_passes(repo: Path) -> None:
    folder = _folder(repo)
    name = store.next_file_name(store.group_files(folder)["schema"][-1].name)
    _ = (folder / name).write_text(dump({"new.entry": "x"}))
    manifest = store.read_json(store.manifest_path(repo, VERSION))
    manifest["golden"]["history"] = store.with_file(
        manifest["golden"]["history"],
        name,
        store.file_digest(folder / name),
        "2026-11-20",
    )
    store.write_json(store.manifest_path(repo, VERSION), manifest)
    _commit(repo, f"add {name}")
    assert store.tree_problems(repo, VERSION) == []
    result = _check(repo, ci=True)
    assert result.returncode == 0, result.stderr


def test_an_edited_released_file_fails(repo: Path) -> None:
    path = _folder(repo) / "semantic-01.json"
    _ = path.write_text(path.read_text().replace("semantic_v1", "semantic_v2", 1))
    _commit(repo, "edit a released file")
    result = _check(repo)
    assert result.returncode == 1
    assert "semantic-01.json differs from the released file" in result.stderr


def test_a_released_file_rewritten_with_its_history_and_pins_fails(repo: Path) -> None:
    """A rewrite of file, revision and pins inside the tree still differs from the tag."""
    folder = _folder(repo)
    path = folder / "semantic-01.json"
    _ = path.write_text(path.read_text().replace("semantic_v1", "semantic_v2", 1))
    manifest = store.read_json(store.manifest_path(repo, VERSION))
    history = manifest["golden"]["history"]
    history[0]["added"]["semantic-01.json"] = store.file_digest(path)
    for i, revision in enumerate(history):
        revision["previous"] = history[i - 1]["digest"] if i else None
        revision["digest"] = store.revision_digest(revision)
    store.write_json(store.manifest_path(repo, VERSION), manifest)
    pins = store.read_json(store.pins_path(repo))
    pins[VERSION]["releases"]["0.3.0"] = history[-1]["digest"]
    store.write_json(store.pins_path(repo), pins)
    assert store.tree_problems(repo, VERSION) == []
    _commit(repo, "rewrite the release")
    result = _check(repo)
    assert result.returncode == 1
    assert "semantic-01.json differs" in result.stderr
    assert "the releases line 0.3.0" in result.stderr
    assert "history up to release 0.3.0 changed" in result.stderr


def test_a_deleted_released_file_fails(repo: Path) -> None:
    (_folder(repo) / "torch-01.json").unlink()
    _commit(repo, "delete a released file")
    result = _check(repo)
    assert result.returncode == 1
    assert "torch-01.json is missing in HEAD" in result.stderr


def test_a_removed_releases_line_fails(repo: Path) -> None:
    pins = store.read_json(store.pins_path(repo))
    pins[VERSION]["releases"].clear()
    store.write_json(store.pins_path(repo), pins)
    _commit(repo, "drop the releases line")
    result = _check(repo)
    assert result.returncode == 1
    assert "the releases line 0.3.0" in result.stderr


def test_a_changed_manifest_pin_fails(repo: Path) -> None:
    pins = store.read_json(store.pins_path(repo))
    pins[VERSION]["manifest"] = "0" * 64
    store.write_json(store.pins_path(repo), pins)
    _commit(repo, "change the manifest pin")
    result = _check(repo)
    assert result.returncode == 1
    assert "manifest pin" in result.stderr


def test_outside_a_repository_it_skips_locally_and_fails_in_ci(tmp_path: Path) -> None:
    plain = tmp_path / "plain"
    store.copy_tree(store.PROJECT_ROOT, plain, VERSION)
    store.reopen_tree(plain, VERSION, "2026-11-15")
    local = _check(plain)
    assert local.returncode == 0
    assert "skipped" in local.stdout
    ci = _check(plain, ci=True)
    assert ci.returncode == 1
    assert "not a git work tree" in ci.stderr


def test_a_shallow_clone_fails_in_ci(repo: Path, tmp_path: Path) -> None:
    clone = tmp_path / "clone"
    _ = subprocess.run(
        ["git", "clone", "-q", "--depth", "1", f"file://{repo}", str(clone)],
        check=True,
        capture_output=True,
        env=_env(ci=False),
    )
    result = _check(clone, ci=True)
    assert result.returncode == 1
    assert "shallow" in result.stderr
    assert _check(clone).returncode == 0


def test_a_repository_without_tags_fails_in_ci(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    store.copy_tree(store.PROJECT_ROOT, root, VERSION)
    store.reopen_tree(root, VERSION, "2026-11-15")
    _git(root.parent, "init", "-q", str(root))
    _commit(root, "open data context")
    result = _check(root, ci=True)
    assert result.returncode == 1
    assert "no tags" in result.stderr


# ---------------------------------------------------------------------------
# --release-version: the Release workflow's gate before a tag is made
# ---------------------------------------------------------------------------


def _gate(repo: Path, version: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--repo",
            str(repo),
            "--release-version",
            version,
        ],
        capture_output=True,
        text=True,
        check=False,
        env=_env(ci=False, ceiling=repo.parent),
    )


def test_the_gate_passes_a_recorded_release(repo: Path) -> None:
    result = _gate(repo, "0.3.0")
    assert result.returncode == 0, result.stderr
    assert "0.3.0" in result.stdout


def test_the_gate_refuses_a_release_without_its_golden_step(repo: Path) -> None:
    result = _gate(repo, "0.3.1")
    assert result.returncode == 1
    assert "generate_golden_hashes.py --release 0.3.1" in result.stderr


def test_the_gate_refuses_an_open_data_context(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    store.copy_tree(store.PROJECT_ROOT, root, VERSION)
    store.reopen_tree(root, VERSION, "2026-11-15")
    _git(root.parent, "init", "-q", str(root))
    _commit(root, "open data context")
    result = _gate(root, "0.3.0")
    assert result.returncode == 1
    assert "0.3.0" in result.stderr


def test_the_gate_refuses_a_pin_that_is_not_the_release_revision(repo: Path) -> None:
    pins = store.read_json(store.pins_path(repo))
    pins[VERSION]["releases"]["0.3.0"] = "0" * 64
    store.write_json(store.pins_path(repo), pins)
    _commit(repo, "tamper with the pin")
    result = _gate(repo, "0.3.0")
    assert result.returncode == 1
    assert "release revision" in result.stderr


def test_the_gate_fails_outside_a_repository_even_locally(tmp_path: Path) -> None:
    plain = tmp_path / "plain"
    store.copy_tree(store.PROJECT_ROOT, plain, VERSION)
    result = _gate(plain, "0.3.0")
    assert result.returncode == 1


def test_the_gate_refuses_a_golden_addition_after_the_release_step(repo: Path) -> None:
    """The tagged commit's last revision is the release: no unreleased file ships with it."""
    folder = _folder(repo)
    name = store.next_file_name(store.group_files(folder)["schema"][-1].name)
    _ = (folder / name).write_text(dump({"new.entry": "x"}))
    manifest = store.read_json(store.manifest_path(repo, VERSION))
    manifest["golden"]["history"] = store.with_file(
        manifest["golden"]["history"],
        name,
        store.file_digest(folder / name),
        "2026-11-20",
    )
    store.write_json(store.manifest_path(repo, VERSION), manifest)
    _commit(repo, f"add {name} after the release step")
    result = _gate(repo, "0.3.0")
    assert result.returncode == 1
    assert "last revision" in result.stderr


def test_inherited_git_location_variables_do_not_redirect_the_check(
    repo: Path, tmp_path: Path
) -> None:
    """``--repo`` is the repository read, whatever ``GIT_DIR`` and friends point at."""
    other = tmp_path / "other"
    _ = subprocess.run(
        ["git", "clone", "-q", str(repo), str(other)],
        check=True,
        capture_output=True,
        env=_env(ci=False),
    )
    path = _folder(other) / "semantic-01.json"
    _ = path.write_text(path.read_text().replace("semantic_v1", "semantic_v2", 1))
    _commit(other, "edit a released file")
    _git(other, "fetch", "-q", "--tags")
    assert _check(other).returncode == 1
    env = _env(ci=True, ceiling=tmp_path)
    env.update(
        GIT_DIR=str(repo / ".git"),
        GIT_WORK_TREE=str(other),
        GIT_COMMON_DIR=str(repo / ".git"),
        GIT_OBJECT_DIRECTORY=str(repo / ".git" / "objects"),
    )
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--repo", str(other)],
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )
    assert result.returncode == 1, result.stdout
    assert "semantic-01.json differs" in result.stderr


def test_tags_of_earlier_data_contexts_pass_with_nothing_to_check(
    tmp_path: Path,
) -> None:
    """The Release workflow's first release of a context: older tags only, none of it.

    As on the public repository before v0.3.0: tags whose commits have no
    ``released_manifests.json`` (v0.1.x) or the old ``{"v1": null}`` form (v0.2.0). In CI
    the check passes with nothing to check; it never fails for want of a release tag.
    """
    root = tmp_path / "repo"
    pins = root / store.PINS_FILE
    pins.parent.mkdir(parents=True)
    _ = (root / "README.md").write_text("old release\n")
    _git(root.parent, "init", "-q", str(root))
    _commit(root, "v0.1.x: no released_manifests.json")
    _git(root, "tag", "v0.1.4")
    _ = pins.write_text('{\n    "v1": null\n}\n')
    _commit(root, "v0.2.0: the old pin form")
    _git(root, "tag", "v0.2.0")
    store.copy_tree(store.PROJECT_ROOT, root, VERSION)
    store.reopen_tree(root, VERSION, "2026-11-15")
    _commit(root, "the open v1 data context")
    result = _check(root, ci=True)
    assert result.returncode == 0, result.stderr
    assert "nothing to check" in result.stdout
