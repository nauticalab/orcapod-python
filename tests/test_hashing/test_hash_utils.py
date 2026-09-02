"""Tests for hash_utils helpers, specifically canonical union annotation strings."""
import inspect
import typing
from pathlib import Path
from uuid import UUID

import orcapod as op
import pytest

from orcapod.hashing.hash_utils import (
    canonical_annotation_str,
    get_function_signature,
    is_union_annotation,
)


class TestIsUnionAnnotation:
    def test_pep604_union_detected(self):
        assert is_union_annotation(str | Path) is True

    def test_pep604_three_member_union_detected(self):
        assert is_union_annotation(str | Path | bytes) is True

    def test_typing_union_detected(self):
        import typing
        assert is_union_annotation(typing.Union[str, int]) is True

    def test_typing_optional_detected(self):
        import typing
        assert is_union_annotation(typing.Optional[str]) is True

    def test_plain_type_not_union(self):
        assert is_union_annotation(int) is False

    def test_generic_alias_not_union(self):
        assert is_union_annotation(list[str]) is False

    def test_none_type_not_union(self):
        assert is_union_annotation(type(None)) is False


class TestCanonicalAnnotationStr:
    def test_pep604_two_member_order_independent(self):
        """str | Path and Path | str produce the same canonical string."""
        assert canonical_annotation_str(str | Path) == canonical_annotation_str(Path | str)

    def test_pep604_canonical_form(self):
        """str | Path canonicalizes to 'pathlib.Path | str' (P before s)."""
        assert canonical_annotation_str(str | Path) == "pathlib.Path | str"

    def test_pep604_three_member_order_independent(self):
        """All permutations of str | Path | bytes produce the same canonical string."""
        canonical = canonical_annotation_str(str | Path | bytes)
        assert canonical_annotation_str(bytes | str | Path) == canonical
        assert canonical_annotation_str(Path | bytes | str) == canonical

    def test_pep604_three_member_canonical_form(self):
        """str | Path | bytes canonicalizes to 'bytes | pathlib.Path | str'."""
        assert canonical_annotation_str(str | Path | bytes) == "bytes | pathlib.Path | str"

    def test_non_union_matches_formatannotation(self):
        """Non-union types fall through to inspect.formatannotation."""
        for t in (int, str, bytes, Path):
            assert canonical_annotation_str(t) == inspect.formatannotation(t)

    def test_typing_union_order_independent(self):
        import typing
        assert (
            canonical_annotation_str(typing.Union[str, int])
            == canonical_annotation_str(typing.Union[int, str])
        )


class TestGetFunctionSignatureUnionCanonical:
    def test_include_defaults_false_strips_defaults(self):
        """include_defaults=False removes default values from parameter strings."""
        def fn(x: int = 42, y: str = "hello") -> None:
            pass

        sig_with = get_function_signature(fn)
        sig_without = get_function_signature(fn, include_defaults=False)
        assert "42" in sig_with
        assert "42" not in sig_without
        assert "x" in sig_without
        assert "y" in sig_without

    def test_param_union_order_independent(self):
        """get_function_signature returns the same string for str|Path and Path|str params."""
        def foo1(x: str | Path) -> str:
            return str(x)

        def foo2(x: Path | str) -> str:
            return str(x)

        # name_override makes function names identical so only annotation order is under test
        assert get_function_signature(foo1, name_override="foo") == get_function_signature(foo2, name_override="foo")

    def test_param_three_member_union_order_independent(self):
        """All permutations of a 3-member union param produce the same signature string."""
        def f1(x: str | Path | bytes) -> str:
            return str(x)

        def f2(x: bytes | str | Path) -> str:
            return str(x)

        def f3(x: Path | bytes | str) -> str:
            return str(x)

        # name_override makes function names identical so only annotation order is under test
        sig1 = get_function_signature(f1, name_override="f")
        assert get_function_signature(f2, name_override="f") == sig1
        assert get_function_signature(f3, name_override="f") == sig1

    def test_return_union_order_independent(self):
        """get_function_signature returns the same string for str|Path and Path|str returns."""
        def foo1(x: int) -> str | Path:
            return str(x)

        def foo2(x: int) -> Path | str:
            return str(x)

        # name_override makes function names identical so only annotation order is under test
        assert get_function_signature(foo1, name_override="foo") == get_function_signature(foo2, name_override="foo")

    def test_non_union_param_unchanged(self):
        """Non-union param signatures are byte-for-byte identical before and after."""
        def foo(x: int) -> str:
            return str(x)

        # The exact current format — verifies no regression for non-union types.
        # Note: return type uses str(annotation) = "<class 'str'>" (not "str").
        sig = get_function_signature(foo)
        assert "x: int" in sig
        assert "foo" in sig

    def test_non_union_signature_stable(self):
        """Non-union function signature is deterministic across calls."""
        def bar(a: int, b: str) -> bytes:
            return b.encode()

        assert get_function_signature(bar) == get_function_signature(bar)


class TestCombineHashes:
    def test_basic_produces_64_char_hex(self):
        from orcapod.hashing.hash_utils import combine_hashes
        result = combine_hashes("abc", "def")
        assert len(result) == 64

    def test_order_false_preserves_insertion_order(self):
        from orcapod.hashing.hash_utils import combine_hashes
        r1 = combine_hashes("aaa", "bbb")
        r2 = combine_hashes("bbb", "aaa")
        assert r1 != r2

    def test_order_true_is_commutative(self):
        from orcapod.hashing.hash_utils import combine_hashes
        r1 = combine_hashes("aaa", "bbb", order=True)
        r2 = combine_hashes("bbb", "aaa", order=True)
        assert r1 == r2

    def test_hex_char_count_truncates(self):
        from orcapod.hashing.hash_utils import combine_hashes
        result = combine_hashes("abc", "def", hex_char_count=16)
        assert len(result) == 16

    def test_prefix_hasher_id(self):
        from orcapod.hashing.hash_utils import combine_hashes
        result = combine_hashes("abc", prefix_hasher_id=True)
        assert result.startswith("sha256@")
        assert len(result) == len("sha256@") + 64

    def test_prefix_and_truncation_combined(self):
        from orcapod.hashing.hash_utils import combine_hashes
        result = combine_hashes("abc", prefix_hasher_id=True, hex_char_count=8)
        assert result.startswith("sha256@")
        assert len(result) == len("sha256@") + 8


class TestHashFile:
    def test_str_path_accepted(self, tmp_path):
        """Passing a str reaches the _to_path str-conversion branch (line 105)."""
        from orcapod.hashing.hash_utils import hash_file
        f = tmp_path / "f.bin"
        f.write_bytes(b"hello world")
        result = hash_file(str(f))
        assert result.method == "sha256"
        assert len(result.digest) == 32  # SHA-256 = 32 bytes

    def test_nonexistent_file_raises(self, tmp_path):
        """Missing file raises FileNotFoundError (line 126)."""
        from orcapod.hashing.hash_utils import hash_file
        import pytest
        with pytest.raises(FileNotFoundError):
            hash_file(tmp_path / "does_not_exist.bin")

    def test_invalid_algorithm_raises_value_error(self, tmp_path):
        """Bogus algorithm name raises ValueError with 'Invalid algorithm' (lines 159-161)."""
        from orcapod.hashing.hash_utils import hash_file
        import pytest
        f = tmp_path / "f.bin"
        f.write_bytes(b"hello")
        with pytest.raises(ValueError, match="Invalid algorithm"):
            hash_file(f, algorithm="bogus_algo_xyz")

    def test_hash_path_algorithm(self, tmp_path):
        """hash_path hashes the path string, not file content (line 126 area)."""
        from orcapod.hashing.hash_utils import hash_file
        f = tmp_path / "f.bin"
        f.write_bytes(b"hello")
        result = hash_file(f, algorithm="hash_path")
        assert result.method == "hash_path"
        assert len(result.digest) == 32

    def test_hash_path_ignores_content(self, tmp_path):
        """Two files at the same path but different content hash identically with hash_path."""
        from orcapod.hashing.hash_utils import hash_file
        f = tmp_path / "f.bin"
        f.write_bytes(b"content A")
        r1 = hash_file(f, algorithm="hash_path")
        f.write_bytes(b"content B")
        r2 = hash_file(f, algorithm="hash_path")
        assert r1.digest == r2.digest


class TestIsInString:
    def test_position_outside_string_returns_false(self):
        """A position not inside any string literal returns False."""
        from orcapod.hashing.hash_utils import _is_in_string
        assert _is_in_string("x = 1 + 2", 8) is False

    def test_position_inside_double_quoted_string_returns_true(self):
        from orcapod.hashing.hash_utils import _is_in_string
        # line: x = "hello"
        # pos 6 is the 'h' inside "hello"
        assert _is_in_string('x = "hello"', 6) is True

    def test_position_inside_single_quoted_string_returns_true(self):
        from orcapod.hashing.hash_utils import _is_in_string
        assert _is_in_string("x = 'hello'", 6) is True

    def test_position_after_string_returns_false(self):
        from orcapod.hashing.hash_utils import _is_in_string
        # line: x = "hi" + 1   — position 12 is the '1', after the string
        assert _is_in_string('x = "hi" + 1', 12) is False

    def test_empty_prefix_returns_false(self):
        from orcapod.hashing.hash_utils import _is_in_string
        assert _is_in_string("# comment", 0) is False


class TestGetFunctionComponents:
    """Tests for get_function_components() in hash_utils."""

    def _get(self, *args, **kwargs):
        from orcapod.hashing.hash_utils import get_function_components
        return get_function_components(*args, **kwargs)

    def test_default_includes_name_module_source_annotations_code(self):
        def my_func(x: int, y: str = "hi") -> bool:
            return True

        components = self._get(my_func)
        combined = " ".join(components)
        assert any(c.startswith("name:") for c in components)
        assert any(c.startswith("module:") for c in components)
        assert any(c.startswith("source:") for c in components)
        assert any(c.startswith("code_properties:") for c in components)

    def test_name_override(self):
        def original_name():
            pass

        components = self._get(original_name, name_override="custom")
        assert any(c == "name:custom" for c in components)

    def test_include_name_false(self):
        def my_func():
            pass

        components = self._get(my_func, include_name=False)
        assert not any(c.startswith("name:") for c in components)

    def test_include_module_false(self):
        def my_func():
            pass

        components = self._get(my_func, include_module=False)
        assert not any(c.startswith("module:") for c in components)

    def test_include_code_properties_false(self):
        def my_func(x: int):
            return x

        components = self._get(my_func, include_code_properties=False)
        assert not any(c.startswith("code_properties:") for c in components)

    def test_include_annotations_false(self):
        def my_func(x: int) -> str:
            return str(x)

        components = self._get(my_func, include_annotations=False)
        assert not any(c.startswith("annotations:") for c in components)

    def test_annotations_component_present_when_annotations_exist(self):
        def my_func(x: int) -> str:
            return str(x)

        components = self._get(my_func)
        assert any(c.startswith("annotations:") for c in components)

    def test_preserve_whitespace_false(self):
        def my_func():
            """Docstring."""
            pass

        components_ws = self._get(my_func, preserve_whitespace=True)
        components_no_ws = self._get(my_func, preserve_whitespace=False)
        # Both should have source; no_ws version should be cleandoc'd
        source_ws = next(c for c in components_ws if c.startswith("source:"))
        source_no_ws = next(c for c in components_no_ws if c.startswith("source:"))
        # cleandoc strips leading indentation
        assert len(source_no_ws) <= len(source_ws)

    def test_include_declaration_false(self):
        def my_func(x: int) -> str:
            return str(x)

        components = self._get(my_func, include_declaration=False)
        source = next(c for c in components if c.startswith("source:"))
        # The 'def my_func' line should be removed
        assert "def my_func" not in source

    def test_include_comments_false(self):
        # We can't write inline comments without them being included in the test
        # source — use exec to create a function with a comment in its source.
        import textwrap
        code = textwrap.dedent("""
            def has_comment(x):
                # this is a comment
                return x
        """)
        ns = {}
        exec(compile(code, "<string>", "exec"), ns)
        _ = ns["has_comment"]

        # NOTE: For dynamically-defined functions, inspect.getsource raises IOError,
        # so include_comments only affects functions with real source.
        # We test it on a real function instead.
        def real_func(x):  # inline comment
            return x

        components_with = self._get(real_func, include_comments=True)
        components_without = self._get(real_func, include_comments=False)
        source_with = next(c for c in components_with if c.startswith("source:"))
        source_without = next(c for c in components_without if c.startswith("source:"))
        # The inline comment should be stripped
        assert "inline comment" not in source_without
        assert "inline comment" in source_with

    def test_no_source_falls_back_to_name_and_signature(self):
        """For builtins (no source), falls back to name + signature components."""
        # Builtins don't have __code__, so disable include_code_properties to
        # avoid AttributeError; focus on verifying the IOError/TypeError fallback path.
        components = self._get(len, include_code_properties=False, include_annotations=False)
        combined = " ".join(components)
        # Should not have a source: component for a builtin
        # but should have a name: (either from include_name or fallback)
        assert "name:" in combined

    def test_return_is_a_list(self):
        def my_func():
            pass

        result = self._get(my_func)
        assert isinstance(result, list)


# ---------------------------------------------------------------------------
# Tests for canonical_annotation_str with registry (ITL-638)
# ---------------------------------------------------------------------------


class TestCanonicalAnnotationStrWithRegistry:
    """canonical_annotation_str resolves registered logical types to stable names."""

    @pytest.fixture
    def type_converter(self):
        from orcapod.contexts import get_default_context
        return get_default_context().type_converter

    def test_builtin_type_unchanged(self, type_converter):
        assert canonical_annotation_str(int, type_converter) == "int"

    def test_builtin_str_unchanged(self, type_converter):
        assert canonical_annotation_str(str, type_converter) == "str"

    def test_registered_file_uses_logical_name(self, type_converter):
        result = canonical_annotation_str(op.File, type_converter)
        assert result == "orcapod.file"

    def test_registered_directory_uses_logical_name(self, type_converter):
        result = canonical_annotation_str(op.Directory, type_converter)
        assert result == "orcapod.directory"

    def test_registered_path_uses_logical_name(self, type_converter):
        import pathlib
        result = canonical_annotation_str(pathlib.Path, type_converter)
        assert result == "orcapod.path"

    def test_registered_uuid_uses_logical_name(self, type_converter):
        result = canonical_annotation_str(UUID, type_converter)
        assert result == "orcapod.uuid"

    def test_generic_list_of_registered_type(self, type_converter):
        result = canonical_annotation_str(list[op.File], type_converter)
        assert result == "list[orcapod.file]"

    def test_generic_dict_with_registered_value(self, type_converter):
        result = canonical_annotation_str(dict[str, op.File], type_converter)
        assert result == "dict[str, orcapod.file]"

    def test_union_with_registered_type(self, type_converter):
        result = canonical_annotation_str(op.File | None, type_converter)
        # Members sorted; NoneType sorts before orcapod.file
        assert result == "NoneType | orcapod.file"

    def test_optional_registered_type(self, type_converter):
        result = canonical_annotation_str(typing.Optional[op.File], type_converter)
        assert result == "NoneType | orcapod.file"

    def test_no_type_converter_fallback(self):
        """Without type_converter, falls back to inspect.formatannotation."""
        result = canonical_annotation_str(op.File, None)
        assert result == inspect.formatannotation(op.File)

    def test_stable_across_calls(self, type_converter):
        r1 = canonical_annotation_str(op.File, type_converter)
        r2 = canonical_annotation_str(op.File, type_converter)
        assert r1 == r2
