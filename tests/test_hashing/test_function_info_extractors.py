"""Tests for FunctionNameExtractor, FunctionSignatureExtractor, and FunctionInfoExtractorFactory."""

from __future__ import annotations

import types as _types
import typing
import uuid
from pathlib import Path

import pytest
import orcapod as op
from orcapod.contexts import get_default_context
from orcapod.hashing.semantic_hashing.function_info_extractors import ParameterDefault


def _annotations(info: dict[str, typing.Any]) -> dict[str, str]:
    """Map each parameter name of an extracted description to its annotation text."""
    return {entry["name"]: entry["annotation"] for entry in info["parameters"]}


class TestFunctionNameExtractor:
    def _make(self):
        from orcapod.hashing.semantic_hashing.function_info_extractors import (
            FunctionNameExtractor,
        )

        return FunctionNameExtractor()

    def test_returns_function_name(self):
        def my_func():
            pass

        result = self._make().extract_function_info(my_func)
        assert result == {"name": "my_func"}

    def test_custom_function_name_overrides(self):
        def my_func():
            pass

        result = self._make().extract_function_info(my_func, function_name="custom")
        assert result["name"] == "custom"

    def test_non_callable_raises_type_error(self):
        with pytest.raises(TypeError, match="not callable"):
            self._make().extract_function_info("not a function")

    def test_lambda_name(self):
        fn = lambda x: x  # noqa: E731
        result = self._make().extract_function_info(fn)
        assert "name" in result

    def test_ignores_typespec_args(self):
        """input_typespec and output_typespec are accepted but not used."""

        def fn(x: int) -> str:
            return str(x)

        result = self._make().extract_function_info(
            fn, input_typespec={"x": int}, output_typespec={"return": str}
        )
        assert result == {"name": "fn"}


class TestFunctionSignatureExtractor:
    def _make(self, **kwargs):
        from orcapod.hashing.semantic_hashing.function_info_extractors import (
            FunctionSignatureExtractor,
        )

        return FunctionSignatureExtractor(**kwargs)

    def test_basic_extraction_returns_dict_with_name_and_parameters(self):
        """The description holds the name, the module and one entry per parameter."""

        def my_func(x: int, y: str) -> bool:
            return True

        result = self._make().extract_function_info(my_func)
        assert "name" in result
        assert result["name"] == "my_func"
        assert result["parameters"] == [
            {"name": "x", "kind": "POSITIONAL_OR_KEYWORD", "annotation": "int"},
            {"name": "y", "kind": "POSITIONAL_OR_KEYWORD", "annotation": "str"},
        ]
        assert "module" in result

    def test_non_callable_raises_type_error(self):
        with pytest.raises(TypeError, match="not callable"):
            self._make().extract_function_info(42)

    def test_include_module_false_omits_module(self):
        def fn():
            pass

        result = self._make(include_module=False).extract_function_info(fn)
        assert "module" not in result

    def test_include_module_true_adds_module(self):
        def fn():
            pass

        result = self._make(include_module=True).extract_function_info(fn)
        assert "module" in result

    def test_include_defaults_false_strips_defaults(self):
        """With ``include_defaults=False`` no entry reports a default."""

        def fn(x: int = 42, y: str = "hi") -> None:
            pass

        result = self._make(include_defaults=False).extract_function_info(fn)
        assert _annotations(result) == {"x": "int", "y": "str"}
        assert all("default" not in entry for entry in result["parameters"])

    def test_include_defaults_true_keeps_defaults(self):
        """A default is reported as a ``ParameterDefault`` holding the value."""

        def fn(x: int = 42) -> None:
            pass

        result = self._make(include_defaults=True).extract_function_info(fn)
        assert result["parameters"] == [
            {
                "name": "x",
                "kind": "POSITIONAL_OR_KEYWORD",
                "annotation": "int",
                "default": ParameterDefault(42),
            }
        ]

    def test_default_none_is_reported_and_no_default_is_absent(self):
        """``y=None`` has a ``default`` entry; a required parameter has none."""

        def fn(x: int, y: int | None = None) -> None:
            pass

        x, y = self._make().extract_function_info(fn)["parameters"]
        assert "default" not in x
        assert y["default"] == ParameterDefault(None)

    def test_unannotated_parameter_has_no_annotation_entry(self):
        """A parameter without an annotation has no ``annotation`` member."""

        def fn(x, y=1):  # noqa: ANN001, ANN202  (unannotated on purpose)
            pass

        x, y = self._make().extract_function_info(fn)["parameters"]
        assert x == {"name": "x", "kind": "POSITIONAL_OR_KEYWORD"}
        assert y == {
            "name": "y",
            "kind": "POSITIONAL_OR_KEYWORD",
            "default": ParameterDefault(1),
        }

    def test_return_annotation_present(self):
        def fn() -> str:
            return "hi"

        result = self._make().extract_function_info(fn)
        assert "returns" in result
        assert result["returns"] is str

    def test_no_return_annotation_omits_returns(self):
        def fn():
            pass

        result = self._make().extract_function_info(fn)
        assert "returns" not in result

    def test_function_name_override(self):
        def fn():
            pass

        result = self._make().extract_function_info(fn, function_name="overridden")
        assert result["name"] == "overridden"

    def test_union_annotation_canonicalized(self):
        """Union annotations are sorted byte-wise for order stability."""

        def fn1(x: str | Path) -> None:
            pass

        def fn2(x: Path | str) -> None:
            pass

        r1 = self._make(include_module=False).extract_function_info(
            fn1, function_name="fn"
        )
        r2 = self._make(include_module=False).extract_function_info(
            fn2, function_name="fn"
        )
        assert _annotations(r1) == {"x": "pathlib.Path | str"}
        assert _annotations(r2) == {"x": "pathlib.Path | str"}

    def test_annotation_containing_equals_preserved_when_defaults_stripped(self):
        """Annotations containing ``=`` are not truncated when include_defaults=False.

        The annotation and the default are separate members of the parameter's
        entry, so no text is ever cut at ``=``.
        """
        from typing import Literal

        def fn(x: Literal["a=b"] = "default_value") -> None:
            pass

        result = self._make(include_defaults=False).extract_function_info(fn)
        # With from __future__ import annotations the annotation is stored as a
        # string; eval_str=True cannot resolve Literal from fn's globals, so it
        # falls back and formatannotation wraps the string literal in quotes.
        # The key property is that the full annotation is present and the default
        # value ("default_value") is absent — verified exactly here.
        (entry,) = result["parameters"]
        assert "Literal" in entry["annotation"] and "a=b" in entry["annotation"]
        assert "default" not in entry

    def test_varargs_and_kwargs_with_annotations(self):
        """*args and **kwargs are told apart by their kind, not by a name prefix."""

        def fn(*args: int, **kwargs: str) -> None:
            pass

        result = self._make().extract_function_info(fn)
        assert result["parameters"] == [
            {"name": "args", "kind": "VAR_POSITIONAL", "annotation": "int"},
            {"name": "kwargs", "kind": "VAR_KEYWORD", "annotation": "str"},
        ]

    def test_keyword_only_and_positional_only_kinds_are_recorded(self):
        """``f(a, /, b, *, c)``: each parameter carries its kind (ITL-953)."""

        def fn(a: int, /, b: int, *, c: str) -> None:
            pass

        result = self._make().extract_function_info(fn)
        assert [(e["name"], e["kind"]) for e in result["parameters"]] == [
            ("a", "POSITIONAL_ONLY"),
            ("b", "POSITIONAL_OR_KEYWORD"),
            ("c", "KEYWORD_ONLY"),
        ]

    def test_eval_str_fallback_on_unresolvable_annotation(self):
        """When eval_str=True fails, falls back to unresolved signature."""
        # Create a function with an annotation that cannot be resolved at eval time
        # by using exec with a forward reference that doesn't exist
        code = "def fn(x: 'NonExistentType123') -> None: pass"
        ns = {"__name__": "__test_module__"}
        exec(compile(code, "<string>", "exec"), ns)
        fn = ns["fn"]
        # eval_str=True will try to resolve 'NonExistentType123' from the function's
        # __globals__, which won't have it → NameError → fallback
        # This should not raise
        result = self._make().extract_function_info(fn)
        assert [entry["name"] for entry in result["parameters"]] == ["x"]


class TestFunctionInfoExtractorFactory:
    def _factory(self):
        from orcapod.hashing.semantic_hashing.function_info_extractors import (
            FunctionInfoExtractorFactory,
        )

        return FunctionInfoExtractorFactory

    def test_name_strategy_returns_function_name_extractor(self):
        from orcapod.hashing.semantic_hashing.function_info_extractors import (
            FunctionNameExtractor,
        )

        extractor = self._factory().create_function_info_extractor("name")
        assert isinstance(extractor, FunctionNameExtractor)

    def test_signature_strategy_returns_function_signature_extractor(self):
        from orcapod.hashing.semantic_hashing.function_info_extractors import (
            FunctionSignatureExtractor,
        )

        extractor = self._factory().create_function_info_extractor("signature")
        assert isinstance(extractor, FunctionSignatureExtractor)

    def test_default_strategy_is_signature(self):
        from orcapod.hashing.semantic_hashing.function_info_extractors import (
            FunctionSignatureExtractor,
        )

        extractor = self._factory().create_function_info_extractor()
        assert isinstance(extractor, FunctionSignatureExtractor)

    def test_invalid_strategy_raises_value_error(self):
        with pytest.raises(ValueError, match="Unknown strategy"):
            self._factory().create_function_info_extractor("invalid_strategy")


class TestFunctionSignatureExtractorWithRegistry:
    """FunctionSignatureExtractor canonicalizes both param and return annotations."""

    @pytest.fixture
    def extractor(self):
        from orcapod.hashing.semantic_hashing.function_info_extractors import (
            FunctionSignatureExtractor,
        )

        return FunctionSignatureExtractor(
            include_module=True,
            include_defaults=True,
            type_converter=get_default_context().type_converter,
        )

    def test_return_annotation_registered_type_is_type_object(self, extractor):
        """parts['returns'] is the raw type object even for registered orcapod types.

        TypeObjectHandler (wired with the same type_converter) canonicalises it to
        ``"type:orcapod.file"`` at hash time — no special casing in the extractor.
        """

        def fn(s: str) -> op.File: ...

        info = extractor.extract_function_info(fn)
        assert not isinstance(info["returns"], str), (
            f"parts['returns'] must not be a string — got {info['returns']!r}"
        )
        assert info["returns"] is op.File, (
            f"Expected op.File type object, got {type(info['returns'])}: {info['returns']!r}"
        )

    def test_return_annotation_builtin_keeps_type_object(self, extractor):
        """Non-registered return types keep their type-object form (hash unchanged)."""

        def fn(x: int) -> float: ...

        info = extractor.extract_function_info(fn)
        assert not isinstance(info["returns"], str), (
            f"parts['returns'] must not be a string — got {info['returns']!r}"
        )
        assert info["returns"] is float, (
            f"Expected float type object, got {info['returns']!r}"
        )

    def test_param_annotation_is_canonical_string(self, extractor):
        """Parameter annotation for a registered orcapod type uses logical_type_name."""

        def fn(f: op.File) -> str: ...

        info = extractor.extract_function_info(fn)
        assert _annotations(info) == {"f": "orcapod.file;7b2276657273696f6e223a317d"}

    def test_generic_param_annotation_canonical(self, extractor):
        """list[op.File] in a parameter is canonicalized."""

        def fn(files: list[op.File]) -> str: ...

        info = extractor.extract_function_info(fn)
        assert _annotations(info) == {
            "files": "list[orcapod.file;7b2276657273696f6e223a317d]"
        }

    def test_union_return_annotation_is_raw_union(self, extractor):
        """op.File | None return annotation is stored as a raw union type object.

        TypeObjectHandler handles each union member individually at hash time;
        no string conversion happens in the extractor.
        """

        def fn(s: str) -> op.File | None: ...

        info = extractor.extract_function_info(fn)
        assert not isinstance(info["returns"], str), (
            f"parts['returns'] must not be a string — got {info['returns']!r}"
        )
        assert isinstance(info["returns"], _types.UnionType), (
            f"Expected UnionType, got {type(info['returns'])}: {info['returns']!r}"
        )

    def test_return_annotation_is_never_a_string(self, extractor):
        """``parts['returns']`` must never be a plain ``str`` for any annotation shape.

        Regression guard: an earlier implementation converted registered return
        types to their ``logical_type_name`` string (e.g. ``"orcapod.file"``) and
        stored that in ``parts["returns"]``, losing the ``"type:"`` prefix that
        ``TypeObjectHandler`` would normally add.  Every case below asserts the
        stored value is NOT a str, regardless of whether the annotation contains
        a registered orcapod type, a builtin, a union, or a generic alias.
        """

        def fn_bare_file(s: str) -> op.File: ...
        def fn_bare_directory(s: str) -> op.Directory: ...
        def fn_union_file_none(s: str) -> op.File | None: ...
        def fn_optional_file(s: str) -> typing.Optional[op.File]: ...  # noqa: UP007
        def fn_union_two(s: str) -> op.File | op.Directory: ...
        def fn_generic_file(s: str) -> list[op.File]: ...
        def fn_float(s: str) -> float: ...
        def fn_list_int(s: str) -> list[int]: ...

        cases = [
            (fn_bare_file, "op.File bare registered type"),
            (fn_bare_directory, "op.Directory bare registered type"),
            (fn_union_file_none, "op.File | None union"),
            (fn_optional_file, "typing.Optional[op.File] union"),
            (fn_union_two, "op.File | op.Directory multi-registered union"),
            (fn_generic_file, "list[op.File] generic wrapping registered type"),
            (fn_float, "float builtin"),
            (fn_list_int, "list[int] builtin generic"),
        ]

        for fn, label in cases:
            info = extractor.extract_function_info(fn, function_name="fn")
            assert "returns" in info, f"{label}: 'returns' key missing from info dict"
            assert not isinstance(info["returns"], str), (
                f"{label}: parts['returns'] must not be a plain string — "
                f"TypeObjectHandler is responsible for serialisation, not the extractor. "
                f"Got {type(info['returns'])!r}: {info['returns']!r}"
            )

    def test_builtin_annotations_unchanged(self, extractor):
        """Functions with only builtin annotations are unaffected.

        In particular, the 'returns' for float stays as the float type object
        so existing cached hashes are not invalidated.
        """

        def fn(x: int, y: str) -> float: ...

        info = extractor.extract_function_info(fn)
        assert _annotations(info) == {"x": "int", "y": "str"}
        assert info["returns"] is float  # type object, not string "float"

    def test_param_canonical_string_and_return_type_object(self, extractor):
        """op.File in a param uses canonical string; op.File as return is type object.

        A parameter's annotation is its canonical text (``canonical_annotation_str``
        replaces the module path).  Return annotations are raw objects — ``TypeObjectHandler``
        canonicalises them (to ``"type:orcapod.file"``) at hash time.
        """

        def fn_param(f: op.File) -> str: ...

        def fn_return(s: str) -> op.File: ...

        info_param = extractor.extract_function_info(fn_param)
        info_return = extractor.extract_function_info(fn_return)

        assert _annotations(info_param) == {
            "f": "orcapod.file;7b2276657273696f6e223a317d"
        }
        assert info_return["returns"] is op.File

    def test_simulated_relocation_stable(self, extractor):
        """Patching __module__ on op.File does not change the extracted info."""

        def fn(f: op.File) -> op.File: ...

        info_before = extractor.extract_function_info(fn)

        original = op.File.__module__
        try:
            op.File.__module__ = "orcapod.extension_types.file_type"
            info_after = extractor.extract_function_info(fn)
        finally:
            op.File.__module__ = original

        assert info_before["parameters"] == info_after["parameters"]
        assert info_before["returns"] == info_after["returns"]

    def test_path_param_annotation_is_canonical_string(self, extractor):
        """pathlib.Path in a param annotation is canonicalized to 'orcapod.path'.

        op.Path is pathlib.Path — the same type object registered under
        logical_type_name 'orcapod.path'.  With the registry wired in, the
        extractor must emit the canonical name rather than the module path.
        """

        def fn(p: Path) -> str: ...

        info = extractor.extract_function_info(fn)
        assert _annotations(info) == {"p": "orcapod.path;7b2276657273696f6e223a317d"}

    def test_uuid_param_annotation_is_canonical_string(self, extractor):
        """uuid.UUID in a param annotation is canonicalized to 'orcapod.uuid'.

        op.UUID is uuid.UUID — the same type object registered under
        logical_type_name 'orcapod.uuid'.
        """

        def fn(u: uuid.UUID) -> str: ...

        info = extractor.extract_function_info(fn)
        assert _annotations(info) == {"u": "orcapod.uuid;7b2276657273696f6e223a317d"}

    def test_union_with_path_canonical(self, extractor):
        """str | Path union param: Path is canonicalized to 'orcapod.path'.

        Without the registry, str | Path produces 'pathlib.Path | str' (sorting
        by the repr string).  With the registry, Path maps to 'orcapod.path',
        so the output is 'orcapod.path | str' — sort order is preserved since
        'orcapod.path' < 'str' lexicographically.
        """

        def fn(x: str | Path) -> None: ...

        info = extractor.extract_function_info(fn)
        assert _annotations(info) == {
            "x": "orcapod.path;7b2276657273696f6e223a317d | str"
        }

    def test_path_return_annotation_type_object_hashes_to_canonical(self, extractor):
        """pathlib.Path as return: extractor stores raw type, TypeObjectHandler gives 'type:orcapod.path'.

        Two-step verification:
        1. The extractor stores the raw ``pathlib.Path`` type object in ``parts["returns"]``
           (not a string — TypeObjectHandler, not the extractor, is responsible for serialisation).
        2. When that raw type object is passed through ``TypeObjectHandler`` (wired with the
           same ``type_converter``), it produces ``"type:orcapod.path"`` — the stable canonical
           form — not ``"type:pathlib.Path"`` (the old module-path form).
        """
        from orcapod.hashing.semantic_hashing.builtin_handlers import TypeObjectHandler

        def fn(s: str) -> Path: ...

        info = extractor.extract_function_info(fn)

        # Step 1: extractor stores raw type object, never a string
        assert info["returns"] is Path, (
            f"Expected pathlib.Path type object in returns, got {info['returns']!r}"
        )

        # Step 2: TypeObjectHandler serializes it to the canonical name
        tc = get_default_context().type_converter
        handler = TypeObjectHandler(type_converter=tc)
        hasher = get_default_context().semantic_hasher
        serialized = handler.handle(info["returns"], hasher)
        assert serialized == "type:orcapod.path;7b2276657273696f6e223a317d", (
            f"Expected the identifier text of orcapod.path, got {serialized!r}"
        )
