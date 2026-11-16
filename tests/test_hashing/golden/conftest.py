"""Collection-time refusal of a skipped or xfailed golden entry (ITL-946).

The check lives here, not in ``test_golden_hashes.py``, so a mark on that module (a
``pytestmark`` skip) cannot switch it off together with the entries.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from tests.test_hashing.golden import store
from tests.test_hashing.golden.marks import golden_entry_violations

GOLDEN_ROOT: Path = store.PROJECT_ROOT
"""The project root whose golden store the collection hook checks."""


def golden_store_problems() -> list[str]:
    """What breaks the golden store of the current data context (``store.tree_problems``).

    Returns:
        One sentence per problem: a golden file that cannot be read (an entry repeated
        inside it), a file the history does not list or lists with another digest, a pin
        that does not match.
    """
    from orcapod.contexts import get_default_context

    return store.tree_problems(GOLDEN_ROOT, get_default_context().version)


@pytest.hookimpl(trylast=True)
def pytest_collection_modifyitems(config: pytest.Config, items: list[Any]) -> None:
    """Fail the collection when a golden entry carries a forbidden skip or xfail.

    Runs after the other collection hooks, so marks they add are seen.

    Args:
        config: The pytest configuration (unused).
        items: The collected items.

    Raises:
        pytest.UsageError: A golden entry is marked skip or xfail other than by the
            optional-package rule.
    """
    found = golden_entry_violations(items)
    if found:
        lines = "\n".join(f"  {node}: {marks}" for node, marks in sorted(found.items()))
        # A store that cannot be read leaves no entry to parametrize, and pytest marks the
        # empty parameter set skipped: name the store problem first, it is the cause.
        problems = golden_store_problems()
        cause = (
            "the golden store is broken, so its entries cannot be collected:\n"
            + "".join(f"  {problem}\n" for problem in problems)
            if problems
            else ""
        )
        raise pytest.UsageError(
            f"{cause}golden entries may not be skipped or expected to fail, except by "
            f"the optional-package rule (ITL-871, ITL-946):\n{lines}"
        )
