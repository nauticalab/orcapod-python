"""Collection-time refusal of a skipped or xfailed golden entry (ITL-946).

The check lives here, not in ``test_golden_hashes.py``, so a mark on that module (a
``pytestmark`` skip) cannot switch it off together with the entries.
"""

from __future__ import annotations

from typing import Any

import pytest

from tests.test_hashing.golden.marks import golden_entry_violations


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
        raise pytest.UsageError(
            "golden entries may not be skipped or expected to fail, except by the "
            f"optional-package rule (ITL-871, ITL-946):\n{lines}"
        )
