"""No data context file holds a JSON object with a repeated key (ITL-578).

``json.load`` keeps the last of two equal keys and drops the others without a word. Git can
merge two branches that each add the same top-level object to
``contexts/logical_types.json`` (``"hash_ignore"``, one entry per library value type) into a
file with that object twice and report no conflict; the first object's entries then vanish.
These tests check the shipped files, and that the context loader refuses such a file.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

import pytest

import orcapod.contexts as contexts_package
from orcapod.contexts.core import ContextValidationError
from orcapod.contexts.registry import JSONDataContextRegistry, _read_json_file

CONTEXTS_DIR = Path(contexts_package.__file__).parent
SHIPPED_JSON_FILES = sorted(CONTEXTS_DIR.rglob("*.json"))


def _fail_on_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """Build a JSON object, failing the test if a key occurs twice."""
    keys = [key for key, _ in pairs]
    repeated = sorted({key for key in keys if keys.count(key) > 1})
    assert not repeated, f"JSON object repeats the keys {repeated}"
    return dict(pairs)


def test_the_shipped_files_are_found() -> None:
    names = {path.relative_to(CONTEXTS_DIR).as_posix() for path in SHIPPED_JSON_FILES}
    assert {"logical_types.json", "data/v1.json"} <= names


@pytest.mark.parametrize(
    "path", SHIPPED_JSON_FILES, ids=lambda p: p.relative_to(CONTEXTS_DIR).as_posix()
)
def test_shipped_file_has_no_duplicate_key(path: Path) -> None:
    """No object in the file, at any depth, repeats a key."""
    json.loads(path.read_text(), object_pairs_hook=_fail_on_duplicate_keys)


def test_every_hash_ignore_entry_is_referenced_by_its_type_and_handler() -> None:
    """Each ``hash_ignore`` name is used by one logical type entry and one handler entry."""
    registration = json.loads((CONTEXTS_DIR / "logical_types.json").read_text())
    text = json.dumps(registration)
    for name in registration["hash_ignore"]:
        reference = json.dumps({"_ref": f"hash_ignore:{name}"})
        assert text.count(reference) == 2, name


def _with_repeated_first_key(source: Path, target: Path) -> str:
    """Write ``source`` to ``target`` with its first top-level member written twice.

    Returns:
        The repeated key.
    """
    members = list(json.loads(source.read_text()).items())
    key, value = members[0]
    body = ",".join(
        f"{json.dumps(k)}:{json.dumps(v)}" for k, v in [(key, value), *members]
    )
    target.write_text("{" + body + "}")
    return key


def test_loader_refuses_a_logical_type_file_with_a_repeated_key(
    tmp_path: Path,
) -> None:
    """The error names the file and the key."""
    custom = tmp_path / "logical_types.json"
    key = _with_repeated_first_key(CONTEXTS_DIR / "logical_types.json", custom)
    registry = JSONDataContextRegistry(logical_types_file=custom)
    with pytest.raises(ContextValidationError) as raised:
        registry.get_context()
    assert str(custom) in str(raised.value)
    assert repr(key) in str(raised.value)


def test_loader_refuses_a_repeated_nested_key(tmp_path: Path) -> None:
    """A repeated key inside a nested object is refused too."""
    custom = tmp_path / "logical_types.json"
    text = (CONTEXTS_DIR / "logical_types.json").read_text()
    marker = '"hash_ignore": {'
    assert text.count(marker) == 1
    custom.write_text(text.replace(marker, marker + '"a.b": [], "a.b": [],'))
    registry = JSONDataContextRegistry(logical_types_file=custom)
    with pytest.raises(ContextValidationError, match="'a.b'"):
        registry.get_context()


def test_loader_refuses_a_manifest_with_a_repeated_key(tmp_path: Path) -> None:
    """A data context manifest with a repeated key is refused, naming file and key."""
    manifests = tmp_path / "data"
    manifests.mkdir()
    manifest = manifests / "v1.json"
    key = _with_repeated_first_key(CONTEXTS_DIR / "data" / "v1.json", manifest)
    shutil.copytree(CONTEXTS_DIR / "data" / "schemas", manifests / "schemas")
    with pytest.raises(ContextValidationError) as raised:
        JSONDataContextRegistry(contexts_dir=manifests)
    assert str(manifest) in str(raised.value)
    assert repr(key) in str(raised.value)


def test_loader_refuses_a_schema_file_with_a_repeated_key(tmp_path: Path) -> None:
    """A JSON schema file with a repeated key fails registry construction, not just a log."""
    schema = tmp_path / "context_schema.json"
    key = _with_repeated_first_key(
        CONTEXTS_DIR / "data" / "schemas" / "context_schema.json", schema
    )
    with pytest.raises(ContextValidationError) as raised:
        JSONDataContextRegistry(schema_file=schema)
    assert str(schema) in str(raised.value)
    assert repr(key) in str(raised.value)


def test_context_files_are_read_as_utf8(tmp_path: Path) -> None:
    """A non-ASCII character in a context file is read the same under any locale."""
    custom = tmp_path / "logical_types.json"
    registration = json.loads((CONTEXTS_DIR / "logical_types.json").read_text("utf-8"))
    registration["description"] = "Größe naïve 数据"
    custom.write_text(json.dumps(registration, ensure_ascii=False), encoding="utf-8")
    registry = JSONDataContextRegistry(logical_types_file=custom)
    # Non-public: the reader is the unit under test; the registry keeps no description.
    assert _read_json_file(custom)["description"] == "Größe naïve 数据"
    assert registry.get_context() is not None
