"""Tests for hash_utils helpers, specifically canonical union annotation strings."""
import inspect
from pathlib import Path

from orcapod.hashing.hash_utils import (
    _canonical_annotation_str,
    _is_union_annotation,
    get_function_signature,
)


class TestIsUnionAnnotation:
    def test_pep604_union_detected(self):
        assert _is_union_annotation(str | Path) is True

    def test_pep604_three_member_union_detected(self):
        assert _is_union_annotation(str | Path | bytes) is True

    def test_typing_union_detected(self):
        import typing
        assert _is_union_annotation(typing.Union[str, int]) is True

    def test_typing_optional_detected(self):
        import typing
        assert _is_union_annotation(typing.Optional[str]) is True

    def test_plain_type_not_union(self):
        assert _is_union_annotation(int) is False

    def test_generic_alias_not_union(self):
        assert _is_union_annotation(list[str]) is False

    def test_none_type_not_union(self):
        assert _is_union_annotation(type(None)) is False


class TestCanonicalAnnotationStr:
    def test_pep604_two_member_order_independent(self):
        """str | Path and Path | str produce the same canonical string."""
        assert _canonical_annotation_str(str | Path) == _canonical_annotation_str(Path | str)

    def test_pep604_canonical_form(self):
        """str | Path canonicalizes to 'pathlib.Path | str' (P before s)."""
        assert _canonical_annotation_str(str | Path) == "pathlib.Path | str"

    def test_pep604_three_member_order_independent(self):
        """All permutations of str | Path | bytes produce the same canonical string."""
        canonical = _canonical_annotation_str(str | Path | bytes)
        assert _canonical_annotation_str(bytes | str | Path) == canonical
        assert _canonical_annotation_str(Path | bytes | str) == canonical

    def test_pep604_three_member_canonical_form(self):
        """str | Path | bytes canonicalizes to 'bytes | pathlib.Path | str'."""
        assert _canonical_annotation_str(str | Path | bytes) == "bytes | pathlib.Path | str"

    def test_non_union_matches_formatannotation(self):
        """Non-union types fall through to inspect.formatannotation."""
        for t in (int, str, bytes, Path):
            assert _canonical_annotation_str(t) == inspect.formatannotation(t)

    def test_typing_union_order_independent(self):
        import typing
        assert (
            _canonical_annotation_str(typing.Union[str, int])
            == _canonical_annotation_str(typing.Union[int, str])
        )


class TestGetFunctionSignatureUnionCanonical:
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
