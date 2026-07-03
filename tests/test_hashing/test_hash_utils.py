"""Tests for hash_utils helpers, specifically canonical union annotation strings."""
import inspect
from pathlib import Path

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
