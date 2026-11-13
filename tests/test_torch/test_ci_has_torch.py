"""Required CI runs the torch tests and never skips them (ITL-871).

The torch test modules skip themselves (``pytest.importorskip``) where torch is not
installed, which is right for a user's environment but would hide them in CI. CI
installs torch through the dev group; this test fails the run if it ever does not.
"""

from __future__ import annotations

import importlib.util
import os

import pytest


def test_torch_is_installed_in_ci() -> None:
    """Under CI (the ``CI`` environment variable is set) torch must be importable."""
    if not os.environ.get("CI"):
        pytest.skip("enforced only in CI, where the CI environment variable is set")
    assert importlib.util.find_spec("torch") is not None, (
        "torch is not installed in CI, so every torch test would be skipped; the dev "
        "dependency group must pull orcapod[torch]"
    )
