"""Tests for extension_types.type_utils helpers."""

from __future__ import annotations

from typing import Optional, Union

from orcapod.extension_types.type_utils import extract_leaf_classes


class _A:
    pass


class _B:
    pass


def test_plain_class():
    assert list(extract_leaf_classes(int)) == [int]


def test_plain_custom_class():
    assert list(extract_leaf_classes(_A)) == [_A]


def test_list_of_class():
    assert list(extract_leaf_classes(list[int])) == [int]


def test_dict_of_classes():
    result = set(extract_leaf_classes(dict[str, int]))
    assert result == {str, int}


def test_optional_unwraps_none():
    """Optional[X] yields X but not NoneType."""
    result = list(extract_leaf_classes(Optional[int]))
    assert result == [int]


def test_union_yields_all_non_none():
    result = set(extract_leaf_classes(Union[int, str]))
    assert result == {int, str}


def test_union_with_none_excludes_none():
    result = set(extract_leaf_classes(Union[int, None]))
    assert type(None) not in result
    assert int in result


def test_nested_list_of_dict():
    """list[dict[_A, list[_B]]] yields _A and _B."""
    result = set(extract_leaf_classes(list[dict[_A, list[_B]]]))
    assert result == {_A, _B}


def test_deeply_nested():
    """list[dict[str, list[dict[int, _A]]]] yields str, int, _A."""
    result = set(extract_leaf_classes(list[dict[str, list[dict[int, _A]]]]))
    assert result == {str, int, _A}


def test_non_generic_non_type_is_skipped():
    """Annotations that are not types and not generic aliases yield nothing."""
    # e.g. a string annotation that failed resolution — should not crash
    result = list(extract_leaf_classes("unresolved_string"))
    assert result == []


def test_none_type_plain():
    """type(None) itself yields type(None) as a leaf (not filtered at this level)."""
    result = list(extract_leaf_classes(type(None)))
    assert result == [type(None)]
