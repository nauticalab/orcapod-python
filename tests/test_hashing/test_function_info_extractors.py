"""Tests for FunctionNameExtractor, FunctionSignatureExtractor, and FunctionInfoExtractorFactory."""

from __future__ import annotations

from pathlib import Path

import pytest
import orcapod as op


class TestFunctionNameExtractor:
    def _make(self):
        from orcapod.hashing.semantic_hashing.function_info_extractors import FunctionNameExtractor
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

        result = self._make().extract_function_info(fn, input_typespec={"x": int}, output_typespec={"return": str})
        assert result == {"name": "fn"}


class TestFunctionSignatureExtractor:
    def _make(self, **kwargs):
        from orcapod.hashing.semantic_hashing.function_info_extractors import FunctionSignatureExtractor
        return FunctionSignatureExtractor(**kwargs)

    def test_basic_extraction_returns_dict_with_name_and_params(self):
        def my_func(x: int, y: str) -> bool:
            return True

        result = self._make().extract_function_info(my_func)
        assert "name" in result
        assert result["name"] == "my_func"
        assert "params" in result
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
        def fn(x: int = 42, y: str = "hi") -> None:
            pass

        result = self._make(include_defaults=False).extract_function_info(fn)
        # Default values should be stripped
        assert "42" not in result["params"]
        assert "hi" not in result["params"]
        # Parameter names should still be present
        assert "x" in result["params"]
        assert "y" in result["params"]

    def test_include_defaults_true_keeps_defaults(self):
        def fn(x: int = 42) -> None:
            pass

        result = self._make(include_defaults=True).extract_function_info(fn)
        assert "42" in result["params"]

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
        """Union annotations are canonicalized for order stability."""
        def fn1(x: str | Path) -> None:
            pass

        def fn2(x: Path | str) -> None:
            pass

        r1 = self._make(include_module=False).extract_function_info(fn1, function_name="fn")
        r2 = self._make(include_module=False).extract_function_info(fn2, function_name="fn")
        assert r1["params"] == r2["params"]

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
        assert "params" in result


class TestFunctionInfoExtractorFactory:
    def _factory(self):
        from orcapod.hashing.semantic_hashing.function_info_extractors import FunctionInfoExtractorFactory
        return FunctionInfoExtractorFactory

    def test_name_strategy_returns_function_name_extractor(self):
        from orcapod.hashing.semantic_hashing.function_info_extractors import FunctionNameExtractor
        extractor = self._factory().create_function_info_extractor("name")
        assert isinstance(extractor, FunctionNameExtractor)

    def test_signature_strategy_returns_function_signature_extractor(self):
        from orcapod.hashing.semantic_hashing.function_info_extractors import FunctionSignatureExtractor
        extractor = self._factory().create_function_info_extractor("signature")
        assert isinstance(extractor, FunctionSignatureExtractor)

    def test_default_strategy_is_signature(self):
        from orcapod.hashing.semantic_hashing.function_info_extractors import FunctionSignatureExtractor
        extractor = self._factory().create_function_info_extractor()
        assert isinstance(extractor, FunctionSignatureExtractor)

    def test_invalid_strategy_raises_value_error(self):
        with pytest.raises(ValueError, match="Unknown strategy"):
            self._factory().create_function_info_extractor("invalid_strategy")


class TestFunctionSignatureExtractorWithRegistry:
    """FunctionSignatureExtractor canonicalizes both param and return annotations."""

    @pytest.fixture
    def extractor(self):
        from orcapod.contexts import get_default_context
        from orcapod.hashing.semantic_hashing.function_info_extractors import (
            FunctionSignatureExtractor,
        )
        return FunctionSignatureExtractor(
            include_module=True,
            include_defaults=True,
            type_converter=get_default_context().type_converter,
        )

    def test_return_annotation_is_canonical_string(self, extractor):
        """parts['returns'] is a canonical string for a registered orcapod type."""
        def fn(s: str) -> op.File:
            ...

        info = extractor.extract_function_info(fn)
        assert isinstance(info["returns"], str), (
            f"Expected str, got {type(info['returns'])}: {info['returns']!r}"
        )
        assert info["returns"] == "orcapod.file"

    def test_return_annotation_builtin_keeps_type_object(self, extractor):
        """Non-registered return types keep their type-object form (hash unchanged)."""
        def fn(x: int) -> float:
            ...

        info = extractor.extract_function_info(fn)
        assert info["returns"] is float, (
            f"Expected float type object, got {info['returns']!r}"
        )

    def test_param_annotation_is_canonical_string(self, extractor):
        """Parameter annotation for a registered orcapod type uses logical_type_name."""
        def fn(f: op.File) -> str:
            ...

        info = extractor.extract_function_info(fn)
        assert "orcapod.file" in info["params"], (
            f"Expected 'orcapod.file' in params, got: {info['params']!r}"
        )
        assert "logical_types" not in info["params"], (
            f"Module path leaked into params: {info['params']!r}"
        )

    def test_generic_param_annotation_canonical(self, extractor):
        """list[op.File] in a parameter is canonicalized."""
        def fn(files: list[op.File]) -> str:
            ...

        info = extractor.extract_function_info(fn)
        assert "orcapod.file" in info["params"]
        assert "logical_types" not in info["params"]

    def test_union_return_annotation_canonical(self, extractor):
        """op.File | None return is a canonical string."""
        def fn(s: str) -> op.File | None:
            ...

        info = extractor.extract_function_info(fn)
        assert isinstance(info["returns"], str)
        assert "orcapod.file" in info["returns"]
        assert "logical_types" not in info["returns"]

    def test_builtin_annotations_unchanged(self, extractor):
        """Functions with only builtin annotations are unaffected.

        In particular, the 'returns' for float stays as the float type object
        so existing cached hashes are not invalidated.
        """
        def fn(x: int, y: str) -> float:
            ...

        info = extractor.extract_function_info(fn)
        assert "int" in info["params"]
        assert "str" in info["params"]
        assert info["returns"] is float  # type object, not string "float"

    def test_return_and_param_use_same_canonical_form(self, extractor):
        """op.File in param and op.File as return use the same canonical string."""
        def fn_param(f: op.File) -> str:
            ...

        def fn_return(s: str) -> op.File:
            ...

        info_param = extractor.extract_function_info(fn_param)
        info_return = extractor.extract_function_info(fn_return)

        assert "orcapod.file" in info_param["params"]
        assert info_return["returns"] == "orcapod.file"

    def test_simulated_relocation_stable(self, extractor):
        """Patching __module__ on op.File does not change the extracted info."""
        def fn(f: op.File) -> op.File:
            ...

        info_before = extractor.extract_function_info(fn)

        original = op.File.__module__
        try:
            op.File.__module__ = "orcapod.extension_types.file_type"
            info_after = extractor.extract_function_info(fn)
        finally:
            op.File.__module__ = original

        assert info_before["params"] == info_after["params"]
        assert info_before["returns"] == info_after["returns"]
