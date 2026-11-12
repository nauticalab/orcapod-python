"""Identity record, identity string, type tokens and names of user classes (ITL-881, ITL-972).

These tests target ``orcapod.logical_types.user_class_identity`` directly: they pin the
documented identity format, which is that module's own interface.
"""

from __future__ import annotations

import dataclasses
import datetime
import typing
from pathlib import Path
from typing import Literal, Optional

import pytest

from orcapod.contexts import get_default_context
from orcapod.logical_types import user_class_identity as uci
from orcapod.logical_types.structural_class import annotation_from_token


@pytest.fixture
def converter():
    """Return the default data context's type converter."""
    return get_default_context().type_converter


def _make_class(module: str, qualname: str = "Point") -> type:
    """Return a one-field dataclass with the given module and qualified name."""
    cls = dataclasses.make_dataclass(qualname, [("x", int)])
    cls.__module__ = module
    cls.__qualname__ = qualname
    return cls


class TestIdentityString:
    """The identity record, its canonical JSON and the identity string."""

    def test_documented_example(self) -> None:
        """The documented example record gives the documented identity string."""
        record = uci.identity_record(
            "dataclass", "Point", [("x", "int"), ("y", "float")]
        )
        assert uci.identity_string(record) == (
            "Point@76d961e40619a836535e3fdebdead7ea65aa9413955a889f3d5b51a7a94b65eb"
        )

    def test_canonical_json_is_compact_and_keeps_field_order(self) -> None:
        """Canonical JSON is compact, sorts keys and keeps the field order."""
        record = uci.identity_record("pydantic", "P", [("b", "int"), ("a", "str")])
        assert (
            uci.canonical_json(record)
            == '{"fields":[["b","int"],["a","str"]],"kind":"pydantic","name":"P"}'
        )

    def test_field_order_is_part_of_the_identity(self) -> None:
        """Records differing only in field order have different identity strings."""
        a = uci.identity_record("dataclass", "P", [("x", "int"), ("y", "str")])
        b = uci.identity_record("dataclass", "P", [("y", "str"), ("x", "int")])
        assert uci.identity_string(a) != uci.identity_string(b)

    @pytest.mark.parametrize(
        "other",
        [
            ("pydantic", "Point", [("x", "int"), ("y", "float")]),
            ("dataclass", "Point2", [("x", "int"), ("y", "float")]),
            ("dataclass", "Point", [("x", "int"), ("y", "int")]),
            ("dataclass", "Point", [("x", "int"), ("z", "float")]),
            ("dataclass", "Point", [("x", "int")]),
            ("dataclass", "Point", [("y", "float"), ("x", "int")]),
        ],
    )
    def test_each_component_changes_the_identity(
        self, other: tuple[str, str, list[tuple[str, str]]]
    ) -> None:
        """Changing the kind, the name or any field changes the identity string."""
        base = uci.identity_record("dataclass", "Point", [("x", "int"), ("y", "float")])
        assert uci.identity_string(base) != uci.identity_string(
            uci.identity_record(*other)
        )

    def test_describe_fields_keeps_the_given_order(self) -> None:
        """``describe_fields`` renders the fields in the order given."""
        assert (
            uci.describe_record(
                uci.identity_record("dataclass", "P", [("y", "float"), ("x", "int")])
            )
            == "P (dataclass) {y: float, x: int}"
        )


class TestTypeTokens:
    """Type tokens of field annotations."""

    @pytest.mark.parametrize(
        ("annotation", "token"),
        [
            (int, "int"),
            (str, "str"),
            (bool, "bool"),
            (bytes, "bytes"),
            (float, "float"),
            (datetime.datetime, "datetime.datetime"),
            (int | None, {"union": ["int", "None"]}),
            (Optional[int], {"union": ["int", "None"]}),
            (None | int, {"union": ["int", "None"]}),
            (str | int, {"union": ["int", "str"]}),
            (list[int], {"generic": "list", "args": ["int"]}),
            (typing.List[int], {"generic": "list", "args": ["int"]}),  # noqa: UP006 - the typing alias is the case under test
            (set[str], {"generic": "set", "args": ["str"]}),
            (dict[str, float], {"generic": "dict", "args": ["str", "float"]}),
            (
                list[int | None],
                {"generic": "list", "args": [{"union": ["int", "None"]}]},
            ),
            (Literal["b", "a"], {"literal": ["'a'", "'b'"]}),
            (tuple[int, ...], {"generic": "tuple", "args": ["int", "..."]}),
        ],
    )
    def test_token(self, converter, annotation: object, token: object) -> None:
        """Each annotation maps to its canonical type token."""
        assert uci.type_token(annotation, converter) == token

    def test_logical_type_token_is_the_whole_identifier(self, converter) -> None:
        """A type with a logical type contributes its name and its metadata."""
        from orcapod.logical_types.identifier import logical_type_identifier

        lt = converter.get_logical_type_for_python_type(Path)
        assert uci.type_token(Path, converter) == {
            "logical": logical_type_identifier(lt).text
        }
        assert uci.type_token(Path, converter)["logical"].startswith("orcapod.path;")

    def test_nested_class_token_is_its_record(self, converter) -> None:
        """A field typed by a user class embeds that class's whole record."""
        inner = _make_class("pkg.models", "Inner")
        assert uci.type_token(inner, converter) == {
            "class": {
                "kind": "dataclass",
                "name": "pkg.models.Inner",
                "fields": [["x", "int"]],
            }
        }

    @pytest.mark.parametrize(
        "annotation",
        [
            int,
            datetime.datetime,
            Path,
            int | None,
            list[int],
            dict[str, float],
            list[Path | None],
            tuple[int, str],
            tuple[int, ...],
            Literal["a", "b"],
        ],
    )
    def test_annotation_is_rebuilt_from_its_token(
        self, converter, annotation: object
    ) -> None:
        """A token gives back an annotation with the same token."""
        token = uci.type_token(annotation, converter)
        rebuilt = annotation_from_token(token, converter)
        assert rebuilt == annotation
        assert uci.type_token(rebuilt, converter) == token


class TestNames:
    """Name validation and the names of exact classes."""

    @pytest.mark.parametrize(
        "name", ["", "a@b", "a b", "a;b", "orcapod.thing", "tab\tname", 3]
    )
    def test_invalid_names_rejected(self, name: object) -> None:
        """Empty, non-string, reserved, ``@``, ``;`` and whitespace names are rejected."""
        with pytest.raises((ValueError, TypeError)):
            uci.validate_name(name)  # pyright: ignore[reportArgumentType]

    @pytest.mark.parametrize("name", ["Point", "geo.Point", "my-types.Point_2"])
    def test_valid_names_accepted(self, name: str) -> None:
        """Dotted names with letters, digits, ``-`` and ``_`` are accepted."""
        assert uci.validate_name(name) == name

    def test_names_are_nfc_normalized(self) -> None:
        """Composed and decomposed spellings of a name are one NFC name."""
        composed, decomposed = "Caf\u00e9", "Cafe\u0301"
        assert uci.validate_name(decomposed) == uci.validate_name(composed) == composed

    def test_exact_class_is_named_by_module_path(self) -> None:
        """An exact class is named by its module path and class name."""
        assert uci.class_name(_make_class("pkg.models")) == "pkg.models.Point"

    @pytest.mark.parametrize("module", ["__main__", "__mp_main__"])
    def test_main_class_is_named_under_main(self, module: str) -> None:
        """A class in ``__main__`` or ``__mp_main__`` is named ``__main__.<name>``."""
        assert uci.class_name(_make_class(module)) == "__main__.Point"
        assert (
            uci.class_name(_make_class(module, "Outer.Point")) == "__main__.Outer.Point"
        )

    def test_structural_class_is_named_by_its_declared_name(self) -> None:
        """A structural class is named by its declared name, not its module."""
        import orcapod as op

        @op.dataclass(name="geo.Point")
        class Point:
            x: int

        assert uci.class_kind(Point) == "structural"
        assert uci.class_name(Point) == "geo.Point"
