"""The grammar the hash encodings rest on (ITL-1154).

The logical-type cell of an Arrow hash is ``<identifier>::<label>:<digest>`` and a container
cell is a count, then per element a ``00``/``01`` marker, a u64 length and the element's
token. They decode in one way only because:

* the registry refuses ``;`` in an extension name, so the identifier text
  ``<extension name>;<hex of the metadata>`` splits at its one ``;``;
* the extension metadata is written as hex, so the identifier text has no ``:`` after it;
* a label contains no ``:``, so ``<label>:<digest>`` splits at its first ``:``.

These tests fail if any of the three stops holding.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

import orcapod
from orcapod.contexts import get_default_context
from orcapod.logical_types.identifier import LogicalTypeIdentifier
from tests.test_logical_types.test_logical_type_identifiers import (
    test_extension_name_with_a_semicolon_is_refused as _semicolon_refused,
)

SOURCE_ROOT = Path(orcapod.__file__).parent
LABEL_FORMAT = re.compile(r"[a-z][a-z0-9_]*_v[1-9][0-9]*")
KNOWN_LABELS = {
    "semantic_v1",
    "arrow_v1",
    "arrow_ordered_v1",
    "directory_v1",
    "ndarray_v1",
    "polars_expr_v1",
    "polars_dataframe_v1",
    "polars_series_v1",
    "pandas_dataframe_v1",
    "pandas_series_v1",
    "torch_tensor_v1",
    "group_v1",
    "batch_v1",
}


def _label_constants() -> dict[str, str]:
    """Every string constant in the package whose name ends in ``LABEL``.

    Module-level and class-level assignments (``SEMANTIC_HASH_LABEL = "semantic_v1"``,
    ``HASH_LABEL = "ndarray_v1"``), with the file that defines each.
    """
    found: dict[str, str] = {}
    for path in sorted(SOURCE_ROOT.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            targets: list[ast.expr] = []
            value: ast.expr | None = None
            if isinstance(node, ast.Assign):
                targets, value = list(node.targets), node.value
            elif isinstance(node, ast.AnnAssign):
                targets, value = [node.target], node.value
            if not (isinstance(value, ast.Constant) and isinstance(value.value, str)):
                continue
            for target in targets:
                if isinstance(target, ast.Name) and target.id.endswith("LABEL"):
                    found[value.value] = path.relative_to(SOURCE_ROOT).as_posix()
    return found


def test_the_scan_finds_every_known_label() -> None:
    """The constant scan sees every label the library defines."""
    assert KNOWN_LABELS <= set(_label_constants())


def test_no_label_contains_a_colon() -> None:
    """Every label is ``<algorithm>_v<N>``, so none contains ``:``."""
    labels = _label_constants()
    bad = {label: where for label, where in labels.items() if ":" in label}
    assert not bad
    off_format = {
        label: where
        for label, where in labels.items()
        if not LABEL_FORMAT.fullmatch(label)
    }
    assert not off_format


def test_the_file_hash_label_contains_no_colon(tmp_path: Path) -> None:
    """The one stated exception, the file hash's ``sha256``, has no ``:`` either."""
    path = tmp_path / "f.txt"
    path.write_text("x")
    label = get_default_context().semantic_hasher.hash_object(orcapod.File(path)).method
    assert label == "sha256"


def test_the_registry_refuses_a_semicolon_in_an_extension_name() -> None:
    """An extension name with ``;`` is refused when its logical type is registered."""
    _semicolon_refused()


@pytest.mark.parametrize(
    ("name", "metadata"),
    [("orcapod.path", b'{"version":1}'), ("x.y", b""), ("a::b", b"::;")],
)
def test_identifier_text_has_one_semicolon_and_hex_metadata(
    name: str, metadata: bytes
) -> None:
    """The identifier text is ``<name>;<hex>``: it splits at its last ``;``, and the part
    after it has no ``:``, whatever bytes the metadata holds."""
    text = LogicalTypeIdentifier(name, metadata).text
    head, _, tail = text.rpartition(";")
    assert head == name
    assert bytes.fromhex(tail) == metadata
    assert ":" not in tail
