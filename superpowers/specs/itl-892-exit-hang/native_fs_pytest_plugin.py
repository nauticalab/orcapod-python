"""Pytest plugin: run the suite with option (c) applied, without editing ``src/``.

Usage (repository root):
    PYTHONPATH=superpowers/specs/itl-892-exit-hang \
        uv run pytest -p native_fs_pytest_plugin tests/
"""

import importlib

# Imported by name: this directory is put on PYTHONPATH by the command above.
importlib.import_module("repro_orcapod").apply_patch("native-fs")
