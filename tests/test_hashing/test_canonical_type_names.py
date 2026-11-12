"""Version-independent canonical type names and derived hashes (ITL-825).

Python 3.13 moved the ``pathlib`` classes into the private ``pathlib._local``
module, so ``pathlib.Path.__module__`` differs between interpreter versions.
Every expected value below is a literal captured on Python 3.12 from the
code as it was before the fix; the same literal must be produced on every
supported Python version.
"""

# ``golden_fn`` and ``golden_pod_fn`` take parameters only for their annotations.
# Their source text is part of the golden hashes, so a line-level ignore inside
# them would change the values under test; the rule is disabled for this file.
# pyright: reportUnusedParameter=false

import collections.abc as cabc
import concurrent.futures
import datetime
import decimal
import enum
import fractions
import io
import ipaddress
import json
import pathlib as P
import re
import typing
import uuid
from collections.abc import Callable
from pathlib import Path

import pytest
import upath

from orcapod.contexts import get_default_context
from orcapod.core.data_function import PythonDataFunction
from orcapod.hashing.hash_utils import (
    canonical_annotation_str,
    get_function_components,
    get_function_signature,
)
from orcapod.hashing.semantic_hashing.builtin_handlers import TypeObjectHandler
from orcapod.hashing.semantic_hashing.function_info_extractors import (
    FunctionSignatureExtractor,
)
from orcapod.utils import function_info
from orcapod.utils.canonical_type_names import (
    canonical_module_name,
    canonical_type_name,
    stabilize_type_names,
)

# (annotation, canonical string on Python 3.12)
EXPECTED_CANONICAL: list[tuple[object, str]] = [
    # pathlib: defined in the private ``pathlib._local`` on 3.13
    (P.Path, "pathlib.Path"),
    (P.PosixPath, "pathlib.PosixPath"),
    (P.WindowsPath, "pathlib.WindowsPath"),
    (P.PurePath, "pathlib.PurePath"),
    (P.PurePosixPath, "pathlib.PurePosixPath"),
    (P.PureWindowsPath, "pathlib.PureWindowsPath"),
    (P.Path | str, "pathlib.Path | str"),
    (typing.Optional[P.Path], "NoneType | pathlib.Path"),  # pyright: ignore[reportDeprecated]  # legacy form under test
    (list[P.Path], "list[pathlib.Path]"),
    (dict[str, P.Path], "dict[str, pathlib.Path]"),
    (tuple[P.Path, ...], "tuple[pathlib.Path, Ellipsis]"),
    (typing.List[P.PurePath], "list[pathlib.PurePath]"),  # pyright: ignore[reportDeprecated]  # legacy form under test
    (cabc.Iterable[P.Path], "collections.abc.Iterable[pathlib.Path]"),
    # other stdlib types users plausibly annotate with
    (datetime.datetime, "datetime.datetime"),
    (datetime.date, "datetime.date"),
    (datetime.time, "datetime.time"),
    (datetime.timedelta, "datetime.timedelta"),
    (datetime.timezone, "datetime.timezone"),
    (decimal.Decimal, "decimal.Decimal"),
    (uuid.UUID, "uuid.UUID"),
    (fractions.Fraction, "fractions.Fraction"),
    (ipaddress.IPv4Address, "ipaddress.IPv4Address"),
    (ipaddress.IPv6Network, "ipaddress.IPv6Network"),
    (re.Pattern, "re.Pattern"),
    (re.Pattern[str], "re.Pattern[str]"),
    (re.Match, "re.Match"),
    (enum.Enum, "enum.Enum"),
    (enum.IntEnum, "enum.IntEnum"),
    (enum.StrEnum, "enum.StrEnum"),
    (cabc.Sequence, "collections.abc.Sequence"),
    (cabc.Sequence[int], "collections.abc.Sequence[int]"),
    (cabc.Mapping[str, int], "collections.abc.Mapping[str, int]"),
    (cabc.Callable[[int], str], "collections.abc.Callable[int, str]"),
    (typing.Any, "Any"),
    (typing.Sequence[int], "collections.abc.Sequence[int]"),  # pyright: ignore[reportDeprecated]  # legacy form under test
    (typing.Literal["a", "b"], "Literal['a', 'b']"),
    (typing.Union[int, str], "int | str"),  # pyright: ignore[reportDeprecated]  # legacy form under test
    (type(None), "NoneType"),
    (int, "int"),
    (bytes, "bytes"),
    # private defining modules that were ALREADY private on 3.11/3.12 must
    # keep their historical names, or existing caches would be invalidated
    (concurrent.futures.Future, "concurrent.futures._base.Future"),
    (io.StringIO, "_io.StringIO"),
]


class _UserDefined:
    class Nested:
        pass


def golden_fn(
    a: Path, b: Path | str, c: list[Path], d: dict[str, Path], e: datetime.datetime
) -> Path:
    return a


def golden_pod_fn(a: Path, b: Path | str, c: list[Path], e: datetime.datetime) -> Path:
    return a


# Pin the module so the goldens do not depend on how pytest imports this file.
golden_fn.__module__ = "itl825_golden"
golden_pod_fn.__module__ = "itl825_golden"


class TestCanonicalAnnotationStrings:
    @pytest.mark.parametrize(
        ("annotation", "expected"),
        EXPECTED_CANONICAL,
        ids=[expected for _, expected in EXPECTED_CANONICAL],
    )
    def test_canonical_string_is_version_independent(
        self, annotation: object, expected: str
    ):
        assert canonical_annotation_str(annotation) == expected

    def test_registered_logical_type_still_wins(self):
        tc = get_default_context().type_converter
        assert (
            canonical_annotation_str(P.Path, tc)
            == "orcapod.path;7b2276657273696f6e223a317d"
        )
        assert canonical_annotation_str(P.PurePath, tc) == "pathlib.PurePath"


class TestCanonicalModuleName:
    def test_pathlib_resolves_to_public_module(self):
        assert canonical_module_name(P.Path) == "pathlib"
        assert canonical_type_name(P.PurePosixPath) == "pathlib.PurePosixPath"

    def test_user_defined_types_are_untouched(self):
        assert canonical_module_name(_UserDefined) == _UserDefined.__module__
        assert canonical_type_name(_UserDefined.Nested) == (
            f"{__name__}._UserDefined.Nested"
        )

    def test_user_type_in_private_module_is_untouched(self):
        cls = type("Thing", (), {"__module__": "mypkg._impl"})
        assert canonical_module_name(cls) == "mypkg._impl"

    def test_third_party_types_are_untouched(self):
        assert canonical_module_name(upath.UPath) == upath.UPath.__module__
        assert canonical_module_name(pytest.Item) == pytest.Item.__module__

    def test_legacy_private_stdlib_modules_keep_their_names(self):
        assert canonical_module_name(concurrent.futures.Future) == (
            "concurrent.futures._base"
        )
        assert canonical_module_name(io.StringIO) == "_io"

    def test_private_stdlib_class_not_exported_publicly_is_untouched(self):
        cls = type("NotExported", (), {"__module__": "pathlib._nonexistent"})
        assert canonical_module_name(cls) == "pathlib._nonexistent"

    def test_stabilize_rewrites_nested_reprs(self):
        ann = dict[str, list[P.Path]]
        assert stabilize_type_names(repr(ann), ann) == "dict[str, list[pathlib.Path]]"
        assert stabilize_type_names(str(P.Path), P.Path) == "<class 'pathlib.Path'>"

    def test_stabilize_is_a_noop_without_relocated_types(self):
        text = "x: tests._local.Thing = <class 'int'>"
        assert stabilize_type_names(text, int, _UserDefined) == text


class _Exported:
    """Stands in for a stdlib class defined privately and re-exported."""

    class Inner:
        pass


class _ExportedExtra:
    """A private class whose name extends ``_Exported`` but is not re-exported."""


@pytest.fixture
def relocated_json_types(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make ``_Exported`` look like ``json._impl._Exported``, re-exported by ``json``."""
    for cls in (_Exported, _Exported.Inner, _ExportedExtra):
        monkeypatch.setattr(cls, "__module__", "json._impl")
    monkeypatch.setattr(json, "_Exported", _Exported, raising=False)


@pytest.mark.usefixtures("relocated_json_types")
class TestStabilizeOverlappingNames:
    """A relocated name must only be rewritten where it is a whole qualified name."""

    def test_fixture_resolves_only_the_exported_classes(self):
        assert canonical_module_name(_Exported) == "json"
        assert canonical_module_name(_Exported.Inner) == "json"
        assert canonical_module_name(_ExportedExtra) == "json._impl"

    def test_longer_unresolved_name_sharing_a_prefix_is_untouched(self):
        text = "json._impl._Exported | json._impl._ExportedExtra"
        assert stabilize_type_names(text, _Exported, _ExportedExtra) == (
            "json._Exported | json._impl._ExportedExtra"
        )

    def test_nested_class_is_rewritten_once(self):
        text = "json._impl._Exported.Inner, json._impl._Exported"
        assert stabilize_type_names(text, _Exported, _Exported.Inner) == (
            "json._Exported.Inner, json._Exported"
        )

    def test_name_inside_a_longer_dotted_path_is_untouched(self):
        text = "x.json._impl._Exported"
        assert stabilize_type_names(text, _Exported) == text


class TestTypeHandlers:
    def test_type_object_handler_uses_public_module(self):
        handler = TypeObjectHandler()
        hasher = get_default_context().semantic_hasher
        assert handler.handle(P.PurePath, hasher) == "type:pathlib.PurePath"
        assert handler.handle(P.Path, hasher) == "type:pathlib.Path"

    @pytest.mark.parametrize(
        ("tp", "expected"),
        [
            (
                P.PurePath,
                "semantic_v1:95b3cb1d44e6a5188889fcaa814f532204f81f56cc3b4fa9378b403cf5b09016",
            ),
            (
                P.PurePosixPath,
                "semantic_v1:6c6d15ff81ab50995637664c092336f90e3f6b1b139783064eac25a0637042c5",
            ),
            (
                P.Path,
                "semantic_v1:7f0da67eaf9d048c9d1791fbe223333cfcc8018c66878647c8b7ae8571c72bd3",
            ),
        ],
    )
    def test_type_object_hash_golden(self, tp: type[object], expected: str):
        hasher = get_default_context().semantic_hasher
        assert hasher.hash_object(tp).to_string() == expected


class TestFunctionSignatureGolden:
    """Golden values for a function annotated with pathlib and datetime types."""

    def test_signature_string(self):
        assert get_function_signature(golden_fn) == (
            "itl825_golden.golden_fn(a: pathlib.Path, b: pathlib.Path | str, "
            "c: list[pathlib.Path], d: dict[str, pathlib.Path], "
            "e: datetime.datetime)-> <class 'pathlib.Path'>"
        )

    def test_signature_hash(self):
        hasher = get_default_context().semantic_hasher
        assert hasher.hash_object(get_function_signature(golden_fn)).to_string() == (
            "semantic_v1:37a126e33aaa8d14d9cda65ebeb3b7b914eb3eced9751aa9b273ba515230a0cc"
        )

    @pytest.mark.parametrize(
        "get_components",
        [get_function_components, function_info.get_function_components],
        ids=["hash_utils", "function_info"],
    )
    def test_components_annotations_line(
        self, get_components: Callable[[Callable[..., object]], list[str]]
    ):
        annotations = [
            c for c in get_components(golden_fn) if c.startswith("annotations:")
        ]
        assert annotations == [
            (
                "annotations:a:<class 'pathlib.Path'>;b:pathlib.Path | str;"
                "c:list[pathlib.Path];d:dict[str, pathlib.Path];"
                "e:<class 'datetime.datetime'>;return:<class 'pathlib.Path'>"
            )
        ]

    def test_extractor_without_converter(self):
        info = FunctionSignatureExtractor().extract_function_info(golden_fn)
        assert [(p["name"], p["annotation"]) for p in info["parameters"]] == [
            ("a", "pathlib.Path"),
            ("b", "pathlib.Path | str"),
            ("c", "list[pathlib.Path]"),
            ("d", "dict[str, pathlib.Path]"),
            ("e", "datetime.datetime"),
        ]

    def test_function_object_hash(self):
        hasher = get_default_context().semantic_hasher
        assert hasher.hash_object(golden_fn).to_string() == (
            "semantic_v1:a55cef45f6b09c6d4189f958f97153bcfe1b510ea620695098e9b6c44695868b"
        )


class TestFunctionPodGolden:
    """A real ``PythonDataFunction`` hashes identically on every Python version."""

    @pytest.fixture(scope="class")
    def data_function(self) -> PythonDataFunction:
        return PythonDataFunction(golden_pod_fn, output_keys="out")

    def test_function_signature_hash(self, data_function: PythonDataFunction):
        variation = data_function.get_function_variation_data()
        assert variation["function_signature_hash"] == (
            b"semantic_v1:\x0bHO\x1c\xc0K\xb9qp\x9cf\xcd\xb3\x15\x90T2fS<\xd4\xe5.\xcf\x05\x88\n\xdf*\x1d&\x9e"
        )

    def test_function_content_hash(self, data_function: PythonDataFunction):
        variation = data_function.get_function_variation_data()
        # Covers the ``annotations:`` component, which embeds ``str(type)``.
        assert variation["function_content_hash"] == (
            b"semantic_v1:p\x08\xe2\xa0>\xe2\x08\xc1\x9a\xacx\x16\xaa\xcbq\x0c`-d"
            b"\xa2V\xe2\x05\xcf\x82\xe0H\x96\x0f\xd5\xc6\xa1"
        )

    def test_uri(self, data_function: PythonDataFunction):
        assert data_function.uri == (
            "golden_pod_fn",
            "7323b2a60208dd00f290ea4a14aac02285017eedc669ea4eb0bb6aa66b94b700",
            "v0",
            "python.function.v0",
        )
