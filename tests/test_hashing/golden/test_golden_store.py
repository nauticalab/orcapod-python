"""The golden folder, its history and the generator's add, check and release (ITL-946).

The generator runs on a copy of the repository's golden tree (``--root``), never on the
repository itself.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pytest

from orcapod.contexts import get_default_context
from tests.test_hashing.golden import cases, store
from tests.test_hashing.golden.generate_golden_hashes import dump, main

VERSION = get_default_context().version


# The trial trees copy the repository's golden folder, so the newest file of a group and the
# next counter come from that inventory, never from fixed names.
_REPOSITORY_FILES = store.group_files(
    store.golden_folder(
        store.PROJECT_ROOT,
        store.read_json(store.manifest_path(store.PROJECT_ROOT, VERSION)),
    )
)
NEWEST_SCHEMA = _REPOSITORY_FILES["schema"][-1].name
NEXT_SCHEMA = store.next_file_name(NEWEST_SCHEMA)


def _write(folder: Path, name: str, values: dict[str, Any]) -> None:
    folder.mkdir(parents=True, exist_ok=True)
    _ = (folder / name).write_text(dump(values), encoding="utf-8")


@pytest.fixture
def tree(tmp_path: Path) -> Path:
    """A copy of the repository's golden tree, reopened: one open revision, no release."""
    root = tmp_path / "tree"
    store.copy_tree(store.PROJECT_ROOT, root, VERSION)
    store.reopen_tree(root, VERSION, "2026-11-15")
    return root


def _manifest(root: Path) -> dict[str, Any]:
    return store.read_json(store.manifest_path(root, VERSION))


def _folder(root: Path) -> Path:
    return store.golden_folder(root, _manifest(root))


def _history(root: Path) -> list[dict[str, Any]]:
    return _manifest(root)["golden"]["history"]


def _pin(root: Path) -> dict[str, Any]:
    return store.read_json(store.pins_path(root))[VERSION]


def _record(root: Path, name: str, values: dict[str, Any]) -> None:
    """Write an unreleased golden file and record it, as an earlier ``--add`` would."""
    folder = _folder(root)
    _write(folder, name, values)
    manifest = _manifest(root)
    manifest["golden"]["history"] = store.with_file(
        manifest["golden"]["history"],
        name,
        store.file_digest(folder / name),
        "2026-11-15",
    )
    store.write_json(store.manifest_path(root, VERSION), manifest)


def _drop_entries(root: Path, name: str, count: int) -> dict[str, Any]:
    """Remove the last ``count`` entries of an unreleased file; return them."""
    values = store.read_json(_folder(root) / name)
    dropped = dict(list(values.items())[-count:])
    _record(root, name, {k: v for k, v in values.items() if k not in dropped})
    return dropped


def _snapshot(folder: Path) -> dict[str, bytes]:
    return {
        path.name: path.read_bytes()
        for path in sorted(folder.iterdir())
        if path.is_file()
    }


# ---------------------------------------------------------------------------
# The folder
# ---------------------------------------------------------------------------


def test_the_repository_tree_is_sound() -> None:
    """The repository's manifest, pins and golden folder agree."""
    assert store.tree_problems(store.PROJECT_ROOT, VERSION) == []


@pytest.mark.parametrize(
    ("first", "second"),
    [("arrow-01.json", "arrow-02.json"), ("arrow-01.json", "schema-01.json")],
)
def test_an_entry_pinned_in_two_files_is_an_error(
    tmp_path: Path, first: str, second: str
) -> None:
    """An entry name appears once in a folder, across the files of every group."""
    _write(tmp_path, first, {"a": 1, "b": 2})
    _write(tmp_path, second, {"b": 2})
    with pytest.raises(store.GoldenLayoutError, match="'b' is pinned twice"):
        _ = store.load_folder(tmp_path)


def test_entries_of_a_group_merge_over_its_files(tmp_path: Path) -> None:
    _write(tmp_path, "arrow-01.json", {"a": 1})
    _write(tmp_path, "arrow-02.json", {"b": 2})
    _write(tmp_path, "schema-01.json", {"c": 3})
    assert store.load_folder(tmp_path) == {
        "arrow": {"a": 1, "b": 2},
        "schema": {"c": 3},
    }


@pytest.mark.parametrize(
    "name", ["arrow.json", "arrow-1.json", "arrow-001.json", "notes.txt"]
)
def test_a_file_not_named_group_and_counter_is_an_error(
    tmp_path: Path, name: str
) -> None:
    _ = (tmp_path / name).write_text("{}")
    with pytest.raises(store.GoldenLayoutError, match="<group>-NN.json"):
        _ = store.load_folder(tmp_path)


def test_an_edited_listed_file_is_reported(tree: Path) -> None:
    """The problem says to restore the file, never to record the edit with the generator."""
    path = _folder(tree) / "arrow-01.json"
    _ = path.write_bytes(path.read_bytes().replace(b"\n", b"\r\n"))
    [problem] = [
        p
        for p in store.tree_problems(tree, VERSION)
        if "arrow-01.json does not have the sha256" in p
    ]
    assert "restore" in problem
    assert "--remove" in problem
    assert "to record it" not in problem


def test_an_unlisted_file_is_reported(tree: Path) -> None:
    name = store.next_file_name(_REPOSITORY_FILES["arrow"][-1].name)
    _write(_folder(tree), name, {"new": 1})
    assert f"{name} is in" in " ".join(store.tree_problems(tree, VERSION))


def test_a_recorded_file_repeating_an_entry_name_is_reported(tree: Path) -> None:
    """A duplicate entry name in correctly recorded files is a problem, and no release."""
    entry, value = next(iter(store.read_json(_folder(tree) / NEWEST_SCHEMA).items()))
    _record(tree, NEXT_SCHEMA, {entry: value})
    assert any("pinned twice" in p for p in store.tree_problems(tree, VERSION))
    with pytest.raises(SystemExit, match="pinned twice"):
        main(["--root", str(tree), "--release", "0.3.0"])


def test_a_recorded_file_of_an_unknown_group_is_reported(tree: Path) -> None:
    """A file whose group is not one of ``cases.GROUPS`` is a problem, even when recorded."""
    _record(tree, "obsolete-01.json", {"obsolete.entry": "x"})
    assert any(
        "obsolete-01.json" in p and "known group" in p
        for p in store.tree_problems(tree, VERSION)
    )


def test_a_key_repeated_inside_one_file_is_an_error(tmp_path: Path) -> None:
    """JSON keeps the last of two equal keys; the store refuses the file instead."""
    _ = (tmp_path / "arrow-01.json").write_text(
        '{"example.entry": "old", "example.entry": "current"}\n'
    )
    with pytest.raises(store.GoldenLayoutError, match="example.entry"):
        _ = store.load_folder(tmp_path)


def test_a_key_repeated_inside_a_recorded_file_is_reported(tree: Path) -> None:
    name = NEXT_SCHEMA
    folder = _folder(tree)
    _ = (folder / name).write_bytes(b'{"dup.entry": "a", "dup.entry": "b"}\n')
    manifest = _manifest(tree)
    manifest["golden"]["history"] = store.with_file(
        manifest["golden"]["history"],
        name,
        store.file_digest(folder / name),
        "2026-11-15",
    )
    store.write_json(store.manifest_path(tree, VERSION), manifest)
    assert any("dup.entry" in p for p in store.tree_problems(tree, VERSION))


@pytest.mark.parametrize(
    "name", ["../arrow-01.json", "sub/arrow-01.json", "obsolete-01.json", "arrow.json"]
)
def test_a_listed_name_that_is_not_a_golden_file_name_is_reported(
    tree: Path, name: str
) -> None:
    """A history entry names a ``<group>-NN.json`` file of a known group, in the folder."""
    history = _history(tree)
    history[0]["added"][name] = "0" * 64
    history = store.rechain(history)
    problems = store.folder_problems(history, _folder(tree))
    assert any(name in p and "not a golden file name" in p for p in problems)


def test_a_missing_listed_file_is_reported(tree: Path) -> None:
    (_folder(tree) / "torch-01.json").unlink()
    assert any(
        "torch-01.json" in p and "missing" in p
        for p in store.tree_problems(tree, VERSION)
    )


def test_a_file_listed_twice_is_reported(tree: Path) -> None:
    history = _history(tree)
    added = history[0]["added"]
    history = store.with_release(history, "0.3.0", "2026-11-16")
    history.append(
        {"date": "2026-11-17", "release": None, "added": dict(added), "previous": None}
    )
    history = store.rechain(history)
    assert store.history_problems(history) == []
    assert any(
        "listed by revisions 0 and 2" in p
        for p in store.folder_problems(history, _folder(tree))
    )


# ---------------------------------------------------------------------------
# The history
# ---------------------------------------------------------------------------


def _released_history(tree: Path) -> list[dict[str, Any]]:
    return store.with_release(_history(tree), "0.3.0", "2026-11-16")


def test_a_rechained_history_is_sound(tree: Path) -> None:
    assert store.history_problems(_released_history(tree)) == []


@pytest.mark.parametrize(
    ("change", "problem"),
    [
        (
            lambda h: h[0].update(date="2026-11-14"),
            "revision 0: digest does not recompute",
        ),
        (lambda h: h[1].update(previous=None), "revision 1: previous is not"),
        (
            lambda h: h[1].update(added={"x-01.json": "00"}),
            "release revision and adds files",
        ),
        (lambda h: h[0].update(added={}), "adds no file and is not a release"),
        (lambda h: h[1].pop("digest"), "does not have exactly the fields"),
        (
            lambda h: h.append(dict(h[1], previous=h[1]["digest"])),
            "release 0.3.0 appears twice",
        ),
        (lambda h: h[0].update(date="15 November"), "no YYYY-MM-DD date"),
    ],
)
def test_a_broken_history_is_reported(tree: Path, change: Any, problem: str) -> None:
    history = _released_history(tree)
    change(history)
    assert problem in " ".join(store.history_problems(history))


def test_a_released_file_cannot_be_recorded_again(tree: Path) -> None:
    with pytest.raises(ValueError, match="never changes"):
        _ = store.with_file(
            _released_history(tree), "arrow-01.json", "00", "2026-11-17"
        )


def test_rechain_leaves_released_revisions_alone(tree: Path) -> None:
    history = _released_history(tree)
    tampered = [dict(history[0], date="2000-01-01"), *history[1:]]
    assert store.rechain(tampered)[0]["digest"] == history[0]["digest"]


# ---------------------------------------------------------------------------
# The pins
# ---------------------------------------------------------------------------


def test_a_final_release_needs_its_releases_line_and_a_pinned_manifest() -> None:
    pin = {"manifest": None, "releases": {}}
    problems = store.final_release_problems(pin, "0.3.0")
    assert "release 0.3.0 ships an open manifest" in problems
    assert any("no releases line" in p for p in problems)


@pytest.mark.parametrize("version", ["0.3.1.dev4+g1234abc", "0.3.0rc1", "0.3.0+local"])
def test_a_build_that_is_not_a_final_release_needs_no_releases_line(
    version: str,
) -> None:
    assert (
        store.final_release_problems({"manifest": None, "releases": {}}, version) == []
    )


def test_the_repository_pin_is_consistent() -> None:
    """The repository's pin, manifest and history agree, open or released."""
    pin = _pin(store.PROJECT_ROOT)
    manifest = _manifest(store.PROJECT_ROOT)
    assert store.pin_problems(manifest, pin, manifest["golden"]["history"]) == []


# ---------------------------------------------------------------------------
# The generator on a copy: add, check, release
# ---------------------------------------------------------------------------


def test_add_writes_new_entries_into_the_open_file_of_their_group(tree: Path) -> None:
    """A group's newest file that no release lists takes the new entries."""
    dropped = _drop_entries(tree, NEWEST_SCHEMA, 3)
    before = _snapshot(_folder(tree))
    main(["--root", str(tree), "--group", "schema"])
    after = _snapshot(_folder(tree))
    assert set(after) == set(before)
    schema = json.loads(after[NEWEST_SCHEMA])
    assert dropped.items() <= schema.items()
    assert {n: b for n, b in after.items() if n != NEWEST_SCHEMA} == {
        n: b for n, b in before.items() if n != NEWEST_SCHEMA
    }
    assert len(_history(tree)) == 1
    assert store.tree_problems(tree, VERSION) == []


def test_add_after_a_release_writes_a_new_file_and_a_new_revision(tree: Path) -> None:
    """A released file never changes: new entries go to the next ``<group>-NN.json``."""
    dropped = _drop_entries(tree, NEWEST_SCHEMA, 2)
    main(["--root", str(tree), "--release", "0.3.0"])
    before = _snapshot(_folder(tree))
    main(["--root", str(tree), "--group", "schema"])
    after = _snapshot(_folder(tree))
    assert {n: after[n] for n in before} == before
    assert set(after) - set(before) == {NEXT_SCHEMA}
    assert json.loads(after[NEXT_SCHEMA]) == dropped
    history = _history(tree)
    assert [r["release"] for r in history] == [None, "0.3.0", None]
    assert history[2]["previous"] == history[1]["digest"]
    assert list(history[2]["added"]) == [NEXT_SCHEMA]
    assert store.tree_problems(tree, VERSION) == []


def test_add_changes_nothing_when_a_later_group_fails(
    tree: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Every group is computed before anything is written: files and history stay in step."""
    _ = _drop_entries(tree, NEWEST_SCHEMA, 1)
    before = _snapshot(_folder(tree)), store.manifest_path(tree, VERSION).read_bytes()
    compute = cases.compute_group

    def failing(group: str, **kwargs: Any) -> dict[str, Any]:
        if group == "semantic":
            raise RuntimeError("semantic failed")
        return compute(group, **kwargs)

    monkeypatch.setattr(cases, "compute_group", failing)
    with pytest.raises(RuntimeError, match="semantic failed"):
        main(["--root", str(tree), "--group", "schema", "--group", "semantic"])
    after = _snapshot(_folder(tree)), store.manifest_path(tree, VERSION).read_bytes()
    assert after == before
    assert store.tree_problems(tree, VERSION) == []


def test_add_refuses_one_new_entry_name_in_two_groups(
    tree: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Two groups that compute the same new entry name in one run write nothing."""
    before = _snapshot(_folder(tree)), store.manifest_path(tree, VERSION).read_bytes()
    compute = cases.compute_group

    def with_shared_entry(group: str, **kwargs: Any) -> dict[str, Any]:
        return {**compute(group, **kwargs), "shared.new_entry": group}

    monkeypatch.setattr(cases, "compute_group", with_shared_entry)
    with pytest.raises(SystemExit, match="shared.new_entry"):
        main(["--root", str(tree), "--group", "schema", "--group", "arrow"])
    after = _snapshot(_folder(tree)), store.manifest_path(tree, VERSION).read_bytes()
    assert after == before


def test_add_writes_nothing_when_every_entry_is_pinned(tree: Path) -> None:
    before = _snapshot(tree / store.CONTEXTS_DIR / "data"), _snapshot(_folder(tree))
    main(["--root", str(tree), "--group", "schema"])
    assert (
        _snapshot(tree / store.CONTEXTS_DIR / "data"),
        _snapshot(_folder(tree)),
    ) == before


def test_add_refuses_a_tree_that_breaks_the_rules(tree: Path) -> None:
    path = _folder(tree) / NEWEST_SCHEMA
    _ = path.write_text(path.read_text() + " ")
    manifest = store.manifest_path(tree, VERSION).read_bytes()
    with pytest.raises(SystemExit, match="nothing written"):
        main(["--root", str(tree), "--group", "schema"])
    assert store.manifest_path(tree, VERSION).read_bytes() == manifest


def test_remove_then_add_changes_an_unreleased_value(tree: Path) -> None:
    """The approved-change procedure: ``--remove`` the entry, then ``--add`` writes it."""
    name = NEWEST_SCHEMA
    values = store.read_json(_folder(tree) / name)
    entry = next(iter(values))
    _record(tree, name, {**values, entry: "old value"})
    main(["--root", str(tree), "--remove", entry])
    assert entry not in store.read_json(_folder(tree) / name)
    assert store.tree_problems(tree, VERSION) == []
    main(["--root", str(tree), "--group", "schema"])
    assert store.read_json(_folder(tree) / name) == values
    assert len(_history(tree)) == 1
    assert store.tree_problems(tree, VERSION) == []


def test_remove_refuses_an_entry_a_release_lists(tree: Path) -> None:
    main(["--root", str(tree), "--release", "0.3.0"])
    entry = next(iter(store.read_json(_folder(tree) / NEWEST_SCHEMA)))
    before = _snapshot(_folder(tree)), _snapshot(tree / store.CONTEXTS_DIR / "data")
    with pytest.raises(SystemExit, match="released"):
        main(["--root", str(tree), "--remove", entry])
    after = _snapshot(_folder(tree)), _snapshot(tree / store.CONTEXTS_DIR / "data")
    assert after == before


def test_remove_refuses_an_unknown_entry_and_writes_nothing(tree: Path) -> None:
    entry = next(iter(store.read_json(_folder(tree) / NEWEST_SCHEMA)))
    before = _snapshot(_folder(tree)), _snapshot(tree / store.CONTEXTS_DIR / "data")
    with pytest.raises(SystemExit, match="no.such.entry"):
        main(["--root", str(tree), "--remove", entry, "--remove", "no.such.entry"])
    after = _snapshot(_folder(tree)), _snapshot(tree / store.CONTEXTS_DIR / "data")
    assert after == before


def test_check_reports_a_moved_value_and_writes_nothing(tree: Path) -> None:
    name = NEWEST_SCHEMA
    values = store.read_json(_folder(tree) / name)
    entry = next(iter(values))
    _record(tree, name, {**values, entry: "moved"})
    before = _snapshot(_folder(tree)), store.manifest_path(tree, VERSION).read_bytes()
    with pytest.raises(SystemExit, match=re.escape(f"schema:{entry} moved")):
        main(["--root", str(tree), "--check", "--group", "schema"])
    after = _snapshot(_folder(tree)), store.manifest_path(tree, VERSION).read_bytes()
    assert after == before


def test_check_reports_an_unpinned_entry(tree: Path) -> None:
    dropped = _drop_entries(tree, NEWEST_SCHEMA, 1)
    with pytest.raises(
        SystemExit, match=re.escape(f"schema:{next(iter(dropped))} is computed")
    ):
        main(["--root", str(tree), "--check", "--group", "schema"])


def test_check_passes_on_the_copy(tree: Path) -> None:
    main(["--root", str(tree), "--check", "--group", "schema"])


def test_release_freezes_the_manifest_and_pins_the_release(tree: Path) -> None:
    """The freeze step, on a copy: release revision, pins and ``released_in``."""
    files = _snapshot(_folder(tree))
    main(["--root", str(tree), "--release", "v0.3.0"])
    manifest = _manifest(tree)
    history = manifest["golden"]["history"]
    pin = _pin(tree)
    assert history[-1]["release"] == "0.3.0"
    assert history[-1]["added"] == {}
    assert manifest["released_in"] == "0.3.0"
    assert pin == {
        "manifest": store.fixed_manifest_digest(manifest),
        "releases": {"0.3.0": history[-1]["digest"]},
    }
    assert _snapshot(_folder(tree)) == files
    assert store.tree_problems(tree, VERSION) == []
    assert store.final_release_problems(pin, "0.3.0") == []


def test_a_later_release_keeps_released_in_and_the_manifest_pin(tree: Path) -> None:
    main(["--root", str(tree), "--release", "0.3.0"])
    first = _pin(tree)
    main(["--root", str(tree), "--release", "0.3.1"])
    pin = _pin(tree)
    assert _manifest(tree)["released_in"] == "0.3.0"
    assert pin["manifest"] == first["manifest"]
    assert pin["releases"]["0.3.0"] == first["releases"]["0.3.0"]
    assert set(pin["releases"]) == {"0.3.0", "0.3.1"}
    assert store.tree_problems(tree, VERSION) == []


def test_a_released_manifest_edited_afterwards_is_reported(tree: Path) -> None:
    main(["--root", str(tree), "--release", "0.3.0"])
    manifest = _manifest(tree)
    manifest["description"] += " (edited)"
    store.write_json(store.manifest_path(tree, VERSION), manifest)
    assert any("differs from its pin" in p for p in store.tree_problems(tree, VERSION))


def test_a_released_golden_file_edited_afterwards_is_reported(tree: Path) -> None:
    main(["--root", str(tree), "--release", "0.3.0"])
    path = _folder(tree) / "semantic-01.json"
    _ = path.write_text(path.read_text().replace("semantic_v1", "semantic_v2", 1))
    assert any(
        "a released golden file never changes" in p
        for p in store.tree_problems(tree, VERSION)
    )


@pytest.mark.parametrize(
    "change",
    [
        lambda pin, h: pin["releases"].update({"0.3.1": h[-1]["digest"]}),
        lambda pin, h: pin["releases"].clear(),
        lambda pin, h: pin["releases"].update({"0.3.0": h[0]["digest"]}),
        lambda pin, h: pin.update(manifest=None),
    ],
)
def test_a_pin_that_disagrees_with_the_history_is_reported(
    tree: Path, change: Any
) -> None:
    main(["--root", str(tree), "--release", "0.3.0"])
    pins = store.read_json(store.pins_path(tree))
    change(pins[VERSION], _history(tree))
    store.write_json(store.pins_path(tree), pins)
    assert store.tree_problems(tree, VERSION)


@pytest.mark.parametrize(
    "version", ["0.3.0", "0.3.1+local", "0.3.1.dev1", "not-a-version"]
)
def test_release_refuses_a_version_it_cannot_record(tree: Path, version: str) -> None:
    main(["--root", str(tree), "--release", "0.3.0"])
    before = (
        _snapshot(tree / store.CONTEXTS_DIR),
        _snapshot(tree / store.CONTEXTS_DIR / "data"),
    )
    with pytest.raises(SystemExit):
        main(["--root", str(tree), "--release", version])
    after = (
        _snapshot(tree / store.CONTEXTS_DIR),
        _snapshot(tree / store.CONTEXTS_DIR / "data"),
    )
    assert after == before


def test_allow_errors_is_only_for_stdout(tree: Path) -> None:
    with pytest.raises(SystemExit):
        main(["--root", str(tree), "--allow-errors", "--group", "schema"])
