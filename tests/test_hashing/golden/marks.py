"""The one skip a golden entry may carry, and the check that refuses any other (ITL-946).

A golden entry is never skipped or expected to fail, except by the optional-package rule
(ITL-871): a group whose optional package is not installed is skipped. ``conftest.py`` in
this directory applies the check when tests are collected, outside the golden test module,
so a mark on that module cannot switch it off.
"""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from typing import Any

import pytest

from tests.test_hashing.golden import cases

SKIP_OR_XFAIL = frozenset({"skip", "skipif", "xfail"})
"""The marks that skip a test or expect it to fail."""

GOLDEN_ENTRY_TEST = "test_golden_entry"
"""The test function, in ``test_golden_hashes.py``, that checks one golden entry."""

GOLDEN_TEST_MODULE = Path(__file__).resolve().with_name("test_golden_hashes.py")
"""The module holding ``GOLDEN_ENTRY_TEST``; items of any other module are ignored."""


def needs(group: str) -> Any:
    """A skip mark for a group whose optional dependency is not installed (ITL-871)."""
    module = cases.OPTIONAL_GROUPS.get(group)
    return pytest.mark.skipif(
        not cases.group_available(group),
        reason=f"golden group {group!r} needs the optional package {module!r}",
    )


def entry_marks(group: str) -> list[Any]:
    """The only marks a golden entry may carry: the optional-package skip (ITL-871)."""
    return [needs(group)] if group in cases.OPTIONAL_GROUPS else []


def disallowed_marks(group: str, marks: Iterable[Any]) -> list[str]:
    """The skip and xfail marks on a golden entry of ``group`` that the rules forbid.

    The one allowed mark is the optional-package skip of an optional group, exactly as
    ``needs`` makes it.

    Args:
        group: The entry's group.
        marks: The entry's marks, as ``pytest.Mark`` or ``pytest.MarkDecorator`` objects.

    Returns:
        A description of each forbidden mark.
    """
    allowed = [mark.mark for mark in entry_marks(group)]
    found = [getattr(mark, "mark", mark) for mark in marks]
    return [
        f"{mark.name}{mark.args}{mark.kwargs}"
        for mark in found
        if mark.name in SKIP_OR_XFAIL and mark not in allowed
    ]


def golden_entry_violations(items: Iterable[Any]) -> dict[str, list[str]]:
    """The collected golden entry tests that carry a forbidden skip or xfail.

    Args:
        items: Collected test items (any others are ignored).

    Returns:
        Node id to the forbidden marks it carries; empty when there are none.
    """
    found: dict[str, list[str]] = {}
    for item in items:
        if getattr(item, "originalname", None) != GOLDEN_ENTRY_TEST:
            continue
        if Path(str(getattr(item, "path", ""))).resolve() != GOLDEN_TEST_MODULE:
            continue
        group = str(item.callspec.params["group"])
        bad = disallowed_marks(group, item.iter_markers())
        if bad:
            found[item.nodeid] = bad
    return found
