"""Tests for extension_types.type_utils helpers."""

from __future__ import annotations

from typing import Optional, Union

from orcapod.extension_types.type_utils import _extract_leaf_classes as extract_leaf_classes


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


# ── _walk_fqcn tests ─────────────────────────────────────────────────────────

import dataclasses
import pytest


def test_walk_fqcn_resolves_module_level_class():
    """_walk_fqcn resolves a top-level class from its FQCN."""
    from orcapod.extension_types.type_utils import _walk_fqcn
    import pathlib
    obj = _walk_fqcn("pathlib.Path")
    assert obj is pathlib.Path


def test_walk_fqcn_resolves_nested_attribute():
    """_walk_fqcn walks nested attribute chains (e.g. module.Outer.Inner)."""
    from orcapod.extension_types.type_utils import _walk_fqcn
    import os.path
    # os.path.join is a function reachable via attribute walk
    obj = _walk_fqcn("os.path.join")
    assert obj is os.path.join


def test_walk_fqcn_raises_import_error_on_bad_module():
    """_walk_fqcn raises ImportError when no module prefix can be imported."""
    from orcapod.extension_types.type_utils import _walk_fqcn
    with pytest.raises(ImportError):
        _walk_fqcn("nonexistent.module.NoSuchClass")


def test_walk_fqcn_raises_import_error_on_missing_attr():
    """_walk_fqcn raises ImportError when module exists but attribute does not."""
    from orcapod.extension_types.type_utils import _walk_fqcn
    with pytest.raises(ImportError):
        _walk_fqcn("pathlib.NoSuchClass")


def test_walk_fqcn_raises_import_error_on_single_part():
    """_walk_fqcn raises ImportError when FQCN has no module separator."""
    from orcapod.extension_types.type_utils import _walk_fqcn
    with pytest.raises(ImportError):
        _walk_fqcn("justname")
