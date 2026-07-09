# ITL-473: orcapod-extension-spikeinterface Split Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extract all SpikeInterface code from `orcapod-python` into a new standalone pip package `orcapod-extension-spikeinterface`, and introduce a normalized `OrcapodExtension` protocol + `op.register_extension()` API in core orcapod.

**Architecture:** Phase 1 adds `OrcapodExtension`/`register_extension` to orcapod-python and strips all SI wiring. Phase 2 creates the `nauticalab/orcapod-extension-spikeinterface` GitHub repo with the moved code, a `SpikeInterfaceExtension` singleton, and mirrored CI. Both repos are worked on in the same agent session from the working directory `/home/kurouto/kurouto-jobs/17b9f34a-40ec-489c-9ccd-6c96c7a9cdf0/`.

**Tech Stack:** Python 3.11+, uv, hatchling + hatch-vcs, pytest, GitHub Actions (OIDC Trusted Publishing), Linear release action.

---

## File Map

### orcapod-python (modify/delete)

| Action | Path |
|--------|------|
| Create | `src/orcapod/extensions.py` |
| Modify | `src/orcapod/__init__.py` |
| Modify | `src/orcapod/extension_types/__init__.py` |
| Modify | `src/orcapod/contexts/data/v0.1.json` |
| Modify | `pyproject.toml` |
| Modify | `.github/workflows/_license-check.yml` |
| Delete | `src/orcapod/extension_types/spikeinterface_types.py` |
| Delete | `tests/test_extension_types/test_spikeinterface_types.py` |
| Create | `tests/test_extensions.py` |
| Git-move | `superpowers/specs/2026-07-01-spikeinterface-baserecording-design.md` |
| Git-move | `superpowers/specs/2026-07-02-si-sorting-logical-type-design.md` |
| Git-move | `superpowers/specs/2026-07-02-spikeinterface-motion-design.md` |
| Git-move | `superpowers/specs/2026-07-03-sorting-analyzer-logical-type-design.md` |
| Regenerate | `uv.lock` |

### orcapod-extension-spikeinterface (create new repo)

| Action | Path |
|--------|------|
| Create | `src/orcapod_extension_spikeinterface/__init__.py` |
| Create | `src/orcapod_extension_spikeinterface/_spikeinterface_types.py` |
| Create | `tests/__init__.py` |
| Create | `tests/test_spikeinterface_types.py` |
| Create | `pyproject.toml` |
| Create | `pytest.ini` |
| Create | `.pre-commit-config.yaml` |
| Create | `.github/workflows/run-tests.yml` |
| Create | `.github/workflows/_license-check.yml` |
| Create | `.github/workflows/release.yml` |
| Create | `.github/workflows/release-sync.yml` |
| Create | `CHANGELOG.md` |
| Create | `CLAUDE.md` |
| Create | `README.md` |
| Create | `superpowers/specs/` (four moved SI design specs) |

---

## Phase 1: orcapod-python Changes

---

### Task 1: Create feature branch

- [ ] **Step 1: Check out the feature branch**

```bash
cd /home/kurouto/kurouto-jobs/17b9f34a-40ec-489c-9ccd-6c96c7a9cdf0/orcapod-python
git checkout -b eywalker/itl-473-split-spikeinterface-extension-into-separate-orcapod
git branch --show-current
```

Expected output: `eywalker/itl-473-split-spikeinterface-extension-into-separate-orcapod`

---

### Task 2: Add `OrcapodExtension` protocol and `register_extension()`

**Files:**
- Create: `src/orcapod/extensions.py`
- Create: `tests/test_extensions.py`
- Modify: `src/orcapod/__init__.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_extensions.py`:

```python
"""Tests for OrcapodExtension protocol and register_extension() (ITL-473)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from orcapod.extensions import OrcapodExtension, register_extension


class _MockExtension:
    """Minimal concrete implementation of OrcapodExtension for testing."""

    name = "mock"

    def register(self, context) -> None:
        pass


def test_mock_extension_satisfies_protocol():
    """A class with name and register() satisfies OrcapodExtension at runtime."""
    ext = _MockExtension()
    assert isinstance(ext, OrcapodExtension)


def test_register_extension_passes_explicit_context():
    """register_extension passes an explicit context directly to extension.register."""
    ext = MagicMock(spec=OrcapodExtension)
    mock_context = MagicMock()

    register_extension(ext, context=mock_context)

    ext.register.assert_called_once_with(mock_context)


def test_register_extension_resolves_default_context_when_none():
    """register_extension calls get_default_context() and passes result when context=None."""
    ext = MagicMock(spec=OrcapodExtension)
    mock_context = MagicMock()

    with patch("orcapod.extensions.get_default_context", return_value=mock_context) as mock_get:
        register_extension(ext, context=None)

    mock_get.assert_called_once()
    ext.register.assert_called_once_with(mock_context)


def test_register_extension_context_defaults_to_none():
    """register_extension resolves default context when called without context kwarg."""
    ext = MagicMock(spec=OrcapodExtension)
    mock_context = MagicMock()

    with patch("orcapod.extensions.get_default_context", return_value=mock_context):
        register_extension(ext)  # no context arg

    ext.register.assert_called_once_with(mock_context)


def test_register_extension_does_not_call_get_default_context_when_context_provided():
    """register_extension never calls get_default_context when a context is supplied."""
    ext = MagicMock(spec=OrcapodExtension)
    mock_context = MagicMock()

    with patch("orcapod.extensions.get_default_context") as mock_get:
        register_extension(ext, context=mock_context)

    mock_get.assert_not_called()
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/test_extensions.py -v
```

Expected: `ModuleNotFoundError: No module named 'orcapod.extensions'`

- [ ] **Step 3: Create `src/orcapod/extensions.py`**

```python
"""Normalized extension registration API for orcapod (ITL-473).

Third-party extension packages expose a module-level singleton that implements
``OrcapodExtension`` and register it via ``op.register_extension()``.

Example:
    >>> import orcapod as op
    >>> from orcapod_extension_spikeinterface import spikeinterface_extension
    >>> op.register_extension(spikeinterface_extension)
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from orcapod.contexts import DataContext


@runtime_checkable
class OrcapodExtension(Protocol):
    """Protocol that all orcapod extension objects must implement.

    Extension packages expose a module-level singleton instance of a class
    that implements this protocol and register it via ``op.register_extension()``.

    Example:
        >>> import orcapod as op
        >>> from orcapod_extension_spikeinterface import spikeinterface_extension
        >>> op.register_extension(spikeinterface_extension)

    Attributes:
        name: Short identifier used in log messages (e.g. ``"spikeinterface"``).
    """

    name: str

    def register(self, context: DataContext) -> None:
        """Register this extension's types into ``context``.

        ``context`` is always a concrete ``DataContext`` — never ``None``.
        Context resolution (default vs. explicit) is handled by
        ``register_extension`` before this method is called.

        Args:
            context: Target ``DataContext`` to register types into.
        """
        ...


def register_extension(
    extension: OrcapodExtension,
    context: DataContext | None = None,
) -> None:
    """Register an extension into a data context.

    Resolves ``context`` to the default context when ``None``, then delegates
    to ``extension.register(context)`` with the concrete context. Extensions
    never receive ``None`` for ``context``.

    Args:
        extension: An object implementing ``OrcapodExtension``.
        context: Target ``DataContext``. Resolves to the default context
            if ``None``.

    Example:
        >>> import orcapod as op
        >>> from orcapod_extension_spikeinterface import spikeinterface_extension
        >>> op.register_extension(spikeinterface_extension)
        >>> # or against a specific context:
        >>> op.register_extension(spikeinterface_extension, context=my_context)
    """
    from orcapod.contexts import get_default_context

    if context is None:
        context = get_default_context()
    extension.register(context)
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/test_extensions.py -v
```

Expected: 5 tests PASSED.

- [ ] **Step 5: Export from `src/orcapod/__init__.py`**

Add to `src/orcapod/__init__.py` after the existing imports (after line 35, before `__all__`):

```python
from .extensions import OrcapodExtension, register_extension
```

And add to `__all__`:

```python
    "OrcapodExtension",
    "register_extension",
```

The final `__init__.py` should look like:

```python
from .config import (
    DEFAULT_CONFIG,
    DisplayConfig,
    HashingConfig,
    OrcapodConfig,
    load_config,
)
from .core.function_pod import (
    FunctionPod,
    function_pod,
)
from .core.nodes.source_node import SourceNode
from .pipeline import Pipeline, PipelineJob
# Subpackage re-exports for clean public API
from . import databases  # noqa: F401
from . import nodes  # noqa: F401
from . import operators  # noqa: F401
from . import sources  # noqa: F401
from . import streams  # noqa: F401
from . import types  # noqa: F401

# Stable type aliases — preferred over importing directly from pathlib/upath/uuid.
#
# These aliases are the recommended way to reference these types in orcapod user code.
# Even if an upstream library is renamed or restructured, these symbols remain stable
# at ``orcapod.Path``, ``orcapod.UPath``, ``orcapod.UUID``, ``orcapod.File``, and
# ``orcapod.Directory``. Their Arrow extension types are registered under the
# ``orcapod.*`` namespace (``"orcapod.path"``, ``"orcapod.upath"``, ``"orcapod.uuid"``,
# ``"orcapod.file"``, ``"orcapod.directory"``), so on-disk identity is also decoupled
# from upstream module paths.
from pathlib import Path
from upath import UPath
from uuid import UUID
from orcapod.extension_types.file_type import File
from orcapod.extension_types.directory_type import Directory

# Extension registration API (ITL-473)
from .extensions import OrcapodExtension, register_extension

__all__ = [
    "DEFAULT_CONFIG",
    "DisplayConfig",
    "HashingConfig",
    "OrcapodConfig",
    "load_config",
    "FunctionPod",
    "function_pod",
    "Pipeline",
    "PipelineJob",
    "SourceNode",
    "databases",
    "nodes",
    "operators",
    "sources",
    "streams",
    "types",
    # Stable type aliases
    "Directory",
    "File",
    "Path",
    "UPath",
    "UUID",
    # Extension registration API (ITL-473)
    "OrcapodExtension",
    "register_extension",
]
```

- [ ] **Step 6: Verify the export works**

```bash
uv run python -c "import orcapod as op; print(op.register_extension, op.OrcapodExtension)"
```

Expected: prints `<function register_extension ...> <class 'orcapod.extensions.OrcapodExtension'>`

- [ ] **Step 7: Commit**

```bash
git add src/orcapod/extensions.py src/orcapod/__init__.py tests/test_extensions.py
git commit -m "feat(extensions): add OrcapodExtension protocol and register_extension() (ITL-473)"
```

---

### Task 3: Strip SI block from `extension_types/__init__.py`

**Files:**
- Modify: `src/orcapod/extension_types/__init__.py`

- [ ] **Step 1: Remove the try/except SI block**

Open `src/orcapod/extension_types/__init__.py`. Remove this entire block (currently near the bottom of the imports, after `LogicalPandasDataFrame`):

```python
# ITL-459, ITL-468, ITL-470, ITL-469 — SpikeInterface support (optional; requires pip install orcapod[spikeinterface])
try:
    from .spikeinterface_types import (
        LogicalSIRecording,
        LogicalSISorting,
        LogicalSIMotion,
        LogicalSISortingAnalyzer,
        SIMotionHandler,
        SISortingAnalyzerHandler,
        register_spikeinterface_types,
    )
    _SI_AVAILABLE = True
except ImportError:
    _SI_AVAILABLE = False
```

Also remove the conditional `__all__` extension at the bottom:

```python
    # ITL-459, ITL-468, ITL-470, ITL-469 (conditional — only present when spikeinterface is installed)
    *(
        [
            "LogicalSIRecording",
            "LogicalSISorting",
            "LogicalSIMotion",
            "LogicalSISortingAnalyzer",
            "SIMotionHandler",
            "SISortingAnalyzerHandler",
            "register_spikeinterface_types",
        ] if _SI_AVAILABLE else []
    ),
```

- [ ] **Step 2: Verify the module imports cleanly**

```bash
uv run python -c "from orcapod.extension_types import LogicalTypeRegistry; print('ok')"
```

Expected: `ok`

- [ ] **Step 3: Commit**

```bash
git add src/orcapod/extension_types/__init__.py
git commit -m "refactor(extension_types): remove SI conditional re-export block (ITL-473)"
```

---

### Task 4: Strip SI entries from `v0.1.json`

**Files:**
- Modify: `src/orcapod/contexts/data/v0.1.json`

- [ ] **Step 1: Remove the four SI logical_type entries**

In `src/orcapod/contexts/data/v0.1.json`, under `type_converter._config.logical_type_registry._config.logical_types`, remove these four objects (they have `"_optional": true`):

```json
{
    "_class": "orcapod.extension_types.spikeinterface_types.LogicalSIRecording",
    "_config": {},
    "_optional": true
},
{
    "_class": "orcapod.extension_types.spikeinterface_types.LogicalSISorting",
    "_config": {},
    "_optional": true
},
{
    "_class": "orcapod.extension_types.spikeinterface_types.LogicalSIMotion",
    "_config": {},
    "_optional": true
},
{
    "_class": "orcapod.extension_types.spikeinterface_types.LogicalSISortingAnalyzer",
    "_config": {},
    "_optional": true
},
```

- [ ] **Step 2: Remove the four SI handler entries**

In the same file, under `python_type_handler_registry._config.handlers`, remove these four pairs (they have `"_optional": true`):

```json
[{"_type": "spikeinterface.core.BaseRecording", "_optional": true}, {"_class": "orcapod.extension_types.spikeinterface_types.SIRecordingHandler", "_config": {}, "_optional": true}],
[{"_type": "spikeinterface.core.BaseSorting", "_optional": true}, {"_class": "orcapod.extension_types.spikeinterface_types.SISortingHandler", "_config": {}, "_optional": true}],
[{"_type": "spikeinterface.core.motion.Motion", "_optional": true}, {"_class": "orcapod.extension_types.spikeinterface_types.SIMotionHandler", "_config": {}, "_optional": true}],
[{"_type": "spikeinterface.core.SortingAnalyzer", "_optional": true}, {"_class": "orcapod.extension_types.spikeinterface_types.SISortingAnalyzerHandler", "_config": {}, "_optional": true}],
```

- [ ] **Step 3: Verify the JSON is valid and the default context loads**

```bash
uv run python -c "from orcapod.contexts import get_default_context; ctx = get_default_context(); print('context_key:', ctx.context_key)"
```

Expected: `context_key: std:v0.1:default`

- [ ] **Step 4: Commit**

```bash
git add src/orcapod/contexts/data/v0.1.json
git commit -m "refactor(contexts): remove SI _optional entries from v0.1.json (ITL-473)"
```

---

### Task 5: Remove spikeinterface from `pyproject.toml` and license workflow

**Files:**
- Modify: `pyproject.toml`
- Modify: `.github/workflows/_license-check.yml`

- [ ] **Step 1: Remove spikeinterface from `pyproject.toml`**

In `pyproject.toml`, make three changes:

1. Remove the `spikeinterface` optional-dependency line:
```toml
spikeinterface = ["spikeinterface>=0.101"]
```

2. Remove `"orcapod[spikeinterface]"` from the `all` extras list:
```toml
all = ["orcapod[redis]", "orcapod[ray]", "orcapod[postgresql]", "orcapod[spiraldb]", "orcapod[spikeinterface]"]
```
becomes:
```toml
all = ["orcapod[redis]", "orcapod[ray]", "orcapod[postgresql]", "orcapod[spiraldb]"]
```

3. Remove `"spikeinterface>=0.101"` from `[dependency-groups] dev`:
```toml
    "spikeinterface>=0.101",
```

- [ ] **Step 2: Remove `--ignore-packages quantities` from license check**

In `.github/workflows/_license-check.yml`, the last line of the `pip-licenses` command reads:
```yaml
          --ignore-packages quantities
```

Remove `--ignore-packages quantities` from that invocation. The final command should end at `--partial-match`.

- [ ] **Step 3: Commit**

```bash
git add pyproject.toml .github/workflows/_license-check.yml
git commit -m "chore(deps): remove spikeinterface from orcapod-python deps and license check (ITL-473)"
```

---

### Task 6: Delete SI source file, test file, and regenerate lock

**Files:**
- Delete: `src/orcapod/extension_types/spikeinterface_types.py`
- Delete: `tests/test_extension_types/test_spikeinterface_types.py`
- Regenerate: `uv.lock`

- [ ] **Step 1: Delete the files**

```bash
git rm src/orcapod/extension_types/spikeinterface_types.py
git rm tests/test_extension_types/test_spikeinterface_types.py
```

- [ ] **Step 2: Regenerate `uv.lock`**

```bash
uv lock
```

This may take a minute — uv is resolving dependencies without spikeinterface and its transitive deps (neo, quantities, probeinterface, zarr, numcodecs, fasteners, threadpoolctl, asciitree). The lock file will shrink substantially.

- [ ] **Step 3: Run the core test suite (without spikeinterface)**

```bash
uv run pytest -m "not postgres" --tb=short -q
```

Expected: All tests pass. No SI-related import errors or skips. The deleted SI test file is gone so no SI tests run.

- [ ] **Step 4: Commit**

```bash
git add uv.lock
git commit -m "feat(itl-473): delete spikeinterface_types from core, regenerate lock"
```

---

### Task 7: Move SI design specs

**Files:**
- Git-move 4 files from `superpowers/specs/` to a staging location (they'll be put in the new repo in Phase 2)

- [ ] **Step 1: Move the four SI spec files out of orcapod-python**

```bash
git mv superpowers/specs/2026-07-01-spikeinterface-baserecording-design.md superpowers/specs/_moved_to_extension_repo_2026-07-01-spikeinterface-baserecording-design.md
git mv superpowers/specs/2026-07-02-si-sorting-logical-type-design.md superpowers/specs/_moved_to_extension_repo_2026-07-02-si-sorting-logical-type-design.md
git mv superpowers/specs/2026-07-02-spikeinterface-motion-design.md superpowers/specs/_moved_to_extension_repo_2026-07-02-spikeinterface-motion-design.md
git mv superpowers/specs/2026-07-03-sorting-analyzer-logical-type-design.md superpowers/specs/_moved_to_extension_repo_2026-07-03-sorting-analyzer-logical-type-design.md
```

Note: the `_moved_to_extension_repo_` prefix is temporary — we'll copy the content into the new repo in Phase 2, then delete these from orcapod-python.

- [ ] **Step 2: Commit**

```bash
git add superpowers/specs/
git commit -m "docs(itl-473): mark SI design specs as moved to extension repo"
```

---

### Task 8: Open orcapod-python PR

- [ ] **Step 1: Push the feature branch**

```bash
git push -u origin eywalker/itl-473-split-spikeinterface-extension-into-separate-orcapod
```

- [ ] **Step 2: Open the PR**

```bash
gh pr create \
  --title "feat(itl-473): split spikeinterface into orcapod-extension-spikeinterface" \
  --body "$(cat <<'EOF'
## Summary

- Adds `OrcapodExtension` protocol and `op.register_extension()` for normalized extension registration (ITL-473)
- Removes all SpikeInterface wiring from core orcapod: `spikeinterface_types.py`, `v0.1.json` entries, `extension_types/__init__.py` re-export, optional deps, and license check exclusion
- Regenerates `uv.lock` without spikeinterface transitive deps
- SI code moves to new `nauticalab/orcapod-extension-spikeinterface` package

## Test plan
- [ ] Core test suite passes without spikeinterface installed (`uv run pytest -m "not postgres"`)
- [ ] `import orcapod as op; op.register_extension` is accessible
- [ ] `from orcapod.contexts import get_default_context; get_default_context()` works (no SI import errors)
- [ ] License check passes without `--ignore-packages quantities`

Closes ITL-473
EOF
)"
```

Record the PR URL returned by the command.

---

## Phase 2: Create `orcapod-extension-spikeinterface` Repo

---

### Task 9: Create and clone the new GitHub repository

- [ ] **Step 1: Create the repo**

```bash
gh repo create nauticalab/orcapod-extension-spikeinterface \
  --public \
  --description "OrcaPod extension for SpikeInterface types (BaseRecording, BaseSorting, Motion, SortingAnalyzer)"
```

- [ ] **Step 2: Clone into the working directory**

```bash
cd /home/kurouto/kurouto-jobs/17b9f34a-40ec-489c-9ccd-6c96c7a9cdf0
gh repo clone nauticalab/orcapod-extension-spikeinterface
cd orcapod-extension-spikeinterface
git config user.name "agent-kurodo[bot]"
git config user.email "268466204+agent-kurodo[bot]@users.noreply.github.com"
```

- [ ] **Step 3: Create the directory structure**

```bash
mkdir -p src/orcapod_extension_spikeinterface
mkdir -p tests
mkdir -p .github/workflows
mkdir -p superpowers/specs
```

---

### Task 10: Create `pyproject.toml` and boilerplate

**Files:**
- Create: `pyproject.toml`
- Create: `pytest.ini`
- Create: `.pre-commit-config.yaml`
- Create: `CHANGELOG.md`
- Create: `README.md`
- Create: `CLAUDE.md`

- [ ] **Step 1: Create `pyproject.toml`**

```toml
[build-system]
requires = ["hatchling>=1.21.0", "hatch-vcs>=0.4.0"]
build-backend = "hatchling.build"

[project]
name = "orcapod-extension-spikeinterface"
description = "OrcaPod extension for SpikeInterface types (BaseRecording, BaseSorting, Motion, SortingAnalyzer)"
dynamic = ["version"]
dependencies = [
    "orcapod>=0.1",
    "spikeinterface>=0.101",
]
readme = "README.md"
requires-python = ">=3.11.0"
license = { text = "MIT License" }
classifiers = [
    "Programming Language :: Python :: 3",
    "License :: OSI Approved :: MIT License",
    "Operating System :: OS Independent",
]

[project.urls]
Homepage = "https://github.com/nauticalab/orcapod-extension-spikeinterface"

[tool.hatch.version]
source = "vcs"

[tool.hatch.build.hooks.vcs]
version-file = "src/orcapod_extension_spikeinterface/_version.py"

[tool.hatch.build.targets.wheel]
packages = ["src/orcapod_extension_spikeinterface"]


[dependency-groups]
dev = [
    "pytest>=8.3.5",
    "pytest-cov>=6.1.1",
    "ruff>=0.14.4",
    "pre-commit>=4.4.0",
    "pre-commit-hooks>=6.0.0",
    "pip-licenses>=5.0.0",
]


[tool.coverage.run]
omit = []

[tool.coverage.report]
exclude_also = [
    "if TYPE_CHECKING:",
]
```

- [ ] **Step 2: Create `pytest.ini`**

```ini
[pytest]
testpaths = tests
python_files = test_*.py
python_classes = Test*
python_functions = test_*
addopts = -v
pythonpath = src
```

- [ ] **Step 3: Create `.pre-commit-config.yaml`**

```yaml
repos:
  - repo: https://github.com/tsvikas/sync-with-uv
    rev: v0.4.0
    hooks:
      - id: sync-with-uv
  - repo: https://github.com/astral-sh/ruff-pre-commit
    rev: v0.14.4
    hooks:
      - id: ruff-format
        types_or: [ python, pyi ]

  - repo: https://github.com/pre-commit/pre-commit-hooks
    rev: v6.0.0
    hooks:
      - id: trailing-whitespace
        types_or: [ python, pyi ]
      - id: end-of-file-fixer
        types_or: [ python, pyi ]
      - id: check-yaml
      - id: check-added-large-files
      - id: check-merge-conflict
```

- [ ] **Step 4: Create `CHANGELOG.md`**

```markdown
# Changelog

## Unreleased

### Added

- Initial release: extracted from `orcapod-python` (ITL-473)
- `LogicalSIRecording`, `LogicalSISorting`, `LogicalSIMotion`, `LogicalSISortingAnalyzer` logical types
- `SIRecordingHandler`, `SISortingHandler`, `SIMotionHandler`, `SISortingAnalyzerHandler` semantic hashers
- `SpikeInterfaceExtension` class and `spikeinterface_extension` singleton
- `op.register_extension(spikeinterface_extension)` registration API
```

- [ ] **Step 5: Create `README.md`**

```markdown
# orcapod-extension-spikeinterface

OrcaPod extension for [SpikeInterface](https://spikeinterface.readthedocs.io/) types.

Adds native support for `BaseRecording`, `BaseSorting`, `Motion`, and `SortingAnalyzer`
objects as first-class orcapod value types.

## Installation

```bash
pip install orcapod-extension-spikeinterface
```

## Usage

```python
import orcapod as op
from orcapod_extension_spikeinterface import spikeinterface_extension

# Register SI types into the default orcapod context at startup
op.register_extension(spikeinterface_extension)
```

After registration, SpikeInterface objects can be used as typed inputs/outputs in
orcapod `FunctionPod` functions.

## License

MIT
```

- [ ] **Step 6: Create `CLAUDE.md`**

```markdown
# Claude Code instructions for orcapod-extension-spikeinterface

## Running commands

Always run Python commands via `uv run`, e.g.:

```
uv run pytest tests/
uv run python -c "..."
```

Never use `python`, `pytest`, or `python3` directly.

## Superpowers artifacts

Place all superpowers-related artifacts (design specs, plans, etc.) in the `superpowers/`
directory at the project root.

- Specs go in `superpowers/specs/`
- Plans go in `superpowers/plans/`

## Docstrings

Use [Google style](https://google.github.io/styleguide/pyguide.html#38-comments-and-docstrings)
Python docstrings everywhere. Never mix in ReST markup.

## Git commits

Always use [Conventional Commits](https://www.conventionalcommits.org/) style:

```
<type>(<optional scope>): <short description>
```
```

- [ ] **Step 7: Commit boilerplate**

```bash
git add pyproject.toml pytest.ini .pre-commit-config.yaml CHANGELOG.md README.md CLAUDE.md
git commit -m "chore: initial repo scaffold for orcapod-extension-spikeinterface (ITL-473)"
```

---

### Task 11: Create `_spikeinterface_types.py` (moved and updated)

**Files:**
- Create: `src/orcapod_extension_spikeinterface/_spikeinterface_types.py`

- [ ] **Step 1: Copy the source file from orcapod-python**

```bash
cp /home/kurouto/kurouto-jobs/17b9f34a-40ec-489c-9ccd-6c96c7a9cdf0/orcapod-python/src/orcapod/extension_types/spikeinterface_types.py \
   src/orcapod_extension_spikeinterface/_spikeinterface_types.py
```

- [ ] **Step 2: Update the module docstring**

At the top of `src/orcapod_extension_spikeinterface/_spikeinterface_types.py`, replace:

```python
This module requires the optional ``spikeinterface`` extras group:
``pip install orcapod[spikeinterface]``

Register SI types into the default orcapod context before using them in
pods: call ``register_spikeinterface_types()`` once at startup.
```

with:

```python
This module is part of the ``orcapod-extension-spikeinterface`` package:
``pip install orcapod-extension-spikeinterface``

Register SI types into the default orcapod context before using them in
pods via the normalized extension API::

    import orcapod as op
    from orcapod_extension_spikeinterface import spikeinterface_extension
    op.register_extension(spikeinterface_extension)
```

- [ ] **Step 3: Update the ImportError message**

Find and replace the ImportError message in the `try/except` block near the top of the file:

```python
    raise ImportError(
        "spikeinterface is not installed. "
        "Install it with: pip install orcapod[spikeinterface]"
```

Replace with:

```python
    raise ImportError(
        "spikeinterface is not installed. "
        "Install it with: pip install orcapod-extension-spikeinterface"
```

- [ ] **Step 4: Rename `register_spikeinterface_types` → `_register_spikeinterface_types` and update signature**

Find the function definition at the bottom of the file:

```python
def register_spikeinterface_types(context: Any = None) -> None:
    """Register SpikeInterface LogicalTypes into an orcapod ``DataContext``.
    ...
    If ``context`` is ``None``, the default context (from
    ``orcapod.contexts.get_default_context()``) is used. The function is
    idempotent — calling it more than once on the same context is safe.

    Args:
        context: A ``DataContext`` instance, or ``None`` to use the default.

    Example:
        >>> from orcapod.extension_types.spikeinterface_types import register_spikeinterface_types
        >>> register_spikeinterface_types()  # no-op if default context already has SI types
    """
    if context is None:
        from orcapod.contexts import get_default_context
        context = get_default_context()
```

Replace with:

```python
def _register_spikeinterface_types(context: "DataContext") -> None:
    """Register SpikeInterface LogicalTypes into an orcapod ``DataContext``.

    Registers ``LogicalSIRecording`` / ``SIRecordingHandler`` (ITL-459),
    ``LogicalSISorting`` / ``SISortingHandler`` (ITL-468),
    ``LogicalSIMotion`` / ``SIMotionHandler`` (ITL-470), and
    ``LogicalSISortingAnalyzer`` / ``SISortingAnalyzerHandler`` (ITL-469).

    This is an internal implementation function. Call via
    ``op.register_extension(spikeinterface_extension)`` instead.

    The function is idempotent — calling it more than once on the same
    context is safe.

    Args:
        context: A concrete ``DataContext`` instance. Never ``None`` —
            context resolution is handled by ``op.register_extension``.
    """
```

Remove the `if context is None:` block entirely (the three lines after the old docstring).

Also update the `TYPE_CHECKING` guard at the top if `Any` is only used in the old signature — remove `Any` from the import if it's no longer needed.

- [ ] **Step 5: Commit**

```bash
git add src/orcapod_extension_spikeinterface/_spikeinterface_types.py
git commit -m "feat(spikeinterface_types): move and update from orcapod-python (ITL-473)"
```

---

### Task 12: Create `__init__.py` for the new package

**Files:**
- Create: `src/orcapod_extension_spikeinterface/__init__.py`

- [ ] **Step 1: Create the file**

```python
"""OrcaPod extension for SpikeInterface types (ITL-473).

Provides ``LogicalSIRecording``, ``LogicalSISorting``, ``LogicalSIMotion``,
``LogicalSISortingAnalyzer`` and their corresponding semantic hashers as
first-class orcapod value types.

Register via the normalized extension API::

    import orcapod as op
    from orcapod_extension_spikeinterface import spikeinterface_extension
    op.register_extension(spikeinterface_extension)
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from ._spikeinterface_types import (
    LogicalSIRecording,
    LogicalSISorting,
    LogicalSIMotion,
    LogicalSISortingAnalyzer,
    SIRecordingHandler,
    SISortingHandler,
    SIMotionHandler,
    SISortingAnalyzerHandler,
    _register_spikeinterface_types,
)

if TYPE_CHECKING:
    from orcapod.contexts import DataContext


class SpikeInterfaceExtension:
    """OrcaPod extension object for SpikeInterface types.

    Implements ``orcapod.OrcapodExtension``. The canonical way to use this
    extension is via the module-level ``spikeinterface_extension`` singleton::

        import orcapod as op
        from orcapod_extension_spikeinterface import spikeinterface_extension
        op.register_extension(spikeinterface_extension)

    Attributes:
        name: Extension identifier (``"spikeinterface"``).
    """

    name = "spikeinterface"

    def register(self, context: DataContext) -> None:
        """Register all SpikeInterface types into ``context``.

        Registers ``LogicalSIRecording``, ``LogicalSISorting``,
        ``LogicalSIMotion``, ``LogicalSISortingAnalyzer`` and their handlers.
        Idempotent — safe to call multiple times on the same context.

        Args:
            context: Target ``DataContext``. Always a concrete context —
                resolution from ``None`` is handled by ``op.register_extension``.
        """
        _register_spikeinterface_types(context)


#: Module-level singleton — the canonical extension object.
#: Pass to ``op.register_extension()`` to wire SI types into a context.
spikeinterface_extension = SpikeInterfaceExtension()

__all__ = [
    "SpikeInterfaceExtension",
    "spikeinterface_extension",
    "LogicalSIRecording",
    "LogicalSISorting",
    "LogicalSIMotion",
    "LogicalSISortingAnalyzer",
    "SIRecordingHandler",
    "SISortingHandler",
    "SIMotionHandler",
    "SISortingAnalyzerHandler",
]
```

- [ ] **Step 2: Create empty `tests/__init__.py`**

```bash
touch tests/__init__.py
```

- [ ] **Step 3: Commit**

```bash
git add src/orcapod_extension_spikeinterface/__init__.py tests/__init__.py
git commit -m "feat: add SpikeInterfaceExtension class and spikeinterface_extension singleton (ITL-473)"
```

---

### Task 13: Port and update the test file

**Files:**
- Create: `tests/test_spikeinterface_types.py`

- [ ] **Step 1: Copy the test file from orcapod-python**

```bash
cp /home/kurouto/kurouto-jobs/17b9f34a-40ec-489c-9ccd-6c96c7a9cdf0/orcapod-python/tests/test_extension_types/test_spikeinterface_types.py \
   tests/test_spikeinterface_types.py
```

- [ ] **Step 2: Update the module-key string in the import-error test**

In `tests/test_spikeinterface_types.py`, find `test_spikeinterface_not_installed_raises_import_error`. Change:

```python
        si_types_key = "orcapod.extension_types.spikeinterface_types"
```

to:

```python
        si_types_key = "orcapod_extension_spikeinterface._spikeinterface_types"
```

Also update the expected error message assertion from:

```python
        assert "pip install orcapod[spikeinterface]" in str(exc_info.value)
```

to:

```python
        assert "pip install orcapod-extension-spikeinterface" in str(exc_info.value)
```

- [ ] **Step 3: Update all `from orcapod.extension_types.spikeinterface_types import` to the new path**

Replace all occurrences of:

```python
from orcapod.extension_types.spikeinterface_types import
```

with:

```python
from orcapod_extension_spikeinterface._spikeinterface_types import
```

This affects approximately 30 import statements across the test functions. Use a find-and-replace across the whole file.

- [ ] **Step 4: Update the registration tests to use the new API**

Find `test_register_spikeinterface_types` and all other `test_register_spikeinterface_types_*` functions. They currently call:

```python
from orcapod.extension_types.spikeinterface_types import register_spikeinterface_types
...
register_spikeinterface_types()
```

Replace the import and call in each registration test with the new API:

```python
import orcapod as op
from orcapod_extension_spikeinterface import spikeinterface_extension
from orcapod.contexts import get_default_context
...
op.register_extension(spikeinterface_extension)
ctx = get_default_context()
```

There are 7 registration tests (lines 322, 354, 535, 565, 587, 791, 833). Update each one.

- [ ] **Step 5: Commit**

```bash
git add tests/test_spikeinterface_types.py
git commit -m "test: port spikeinterface_types tests to new package (ITL-473)"
```

---

### Task 14: Create CI workflows

**Files:**
- Create: `.github/workflows/run-tests.yml`
- Create: `.github/workflows/_license-check.yml`
- Create: `.github/workflows/release.yml`
- Create: `.github/workflows/release-sync.yml`

- [ ] **Step 1: Create `run-tests.yml`**

```yaml
name: Run Tests

on:
  push:
    branches: [main, dev]
  pull_request:
  workflow_dispatch:

jobs:
  license-check:
    uses: ./.github/workflows/_license-check.yml

  test:
    needs: license-check
    runs-on: ubuntu-latest
    strategy:
      matrix:
        python-version: ["3.11", "3.12"]

    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0

      - name: Install uv
        uses: astral-sh/setup-uv@v5

      - name: Set up Python ${{ matrix.python-version }}
        uses: actions/setup-python@v5
        with:
          python-version: ${{ matrix.python-version }}

      - name: Install dependencies
        run: uv sync --locked --all-groups

      - name: Run tests
        run: uv run pytest --cov=src --cov-report=term-missing --cov-report=xml

      - name: Upload coverage reports to Codecov
        uses: codecov/codecov-action@v5
        with:
          token: ${{ secrets.CODECOV_TOKEN }}

  dependency-review:
    name: Dependency review
    runs-on: ubuntu-latest
    if: github.event_name == 'pull_request'
    steps:
      - uses: actions/checkout@v4

      - name: Dependency review
        uses: actions/dependency-review-action@v4
        with:
          deny-licenses: >-
            GPL-2.0-only, GPL-2.0-or-later,
            GPL-3.0-only, GPL-3.0-or-later,
            AGPL-3.0-only, AGPL-3.0-or-later,
            LGPL-2.0-only, LGPL-2.0-or-later,
            LGPL-2.1-only, LGPL-2.1-or-later,
            LGPL-3.0-only, LGPL-3.0-or-later
```

- [ ] **Step 2: Create `_license-check.yml`**

```yaml
name: License check

on:
  workflow_call:

jobs:
  license-check:
    name: License check
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0

      - name: Install uv
        uses: astral-sh/setup-uv@v5

      - name: Install dependencies
        run: uv sync --locked --dev

      - name: Check dependency licenses
        run: >-
          uv run pip-licenses
          --allow-only="MIT;Apache-2.0;Apache 2.0;Apache Software License;BSD-2-Clause;BSD-3-Clause;BSD License;BSD;ISC;LGPL-3.0-only;MPL-2.0;Mozilla Public License;Python-2.0;PSF-2.0;Python Software Foundation License;Unlicense"
          --partial-match
```

Note: `quantities` is **not** ignored here — it is a legitimate first-class transitive dep of spikeinterface in this package.

- [ ] **Step 3: Create `release.yml`**

```yaml
name: Release

on:
  workflow_dispatch:
    inputs:
      version:
        description: 'Release version (e.g. 0.1.0 or v0.1.0 — leading v is stripped automatically)'
        required: true
        type: string

jobs:
  test:
    name: Test (Python ${{ matrix.python-version }})
    runs-on: ubuntu-latest
    timeout-minutes: 30
    strategy:
      fail-fast: true
      matrix:
        python-version: ["3.11", "3.12"]
    steps:
      - uses: actions/checkout@34e114876b0b11c390a56381ad16ebd13914f8d5  # v4
        with:
          fetch-depth: 0

      - name: Install uv
        uses: astral-sh/setup-uv@e58605a9b6da7c637471fab8847a5e5a6b8df081  # v5

      - name: Install dependencies
        run: uv sync --locked --dev --python ${{ matrix.python-version }}

      - name: Run tests
        run: uv run --python ${{ matrix.python-version }} pytest --tb=short -q

  license-check:
    uses: ./.github/workflows/_license-check.yml

  build:
    name: Build distribution
    needs: [test, license-check]
    runs-on: ubuntu-latest
    timeout-minutes: 20
    permissions:
      contents: write
    outputs:
      version: ${{ steps.normalize.outputs.version }}
    steps:
      - name: Normalize version
        id: normalize
        run: |
          VERSION="${{ inputs.version }}"
          VERSION="${VERSION#v}"
          echo "version=${VERSION}" >> "$GITHUB_OUTPUT"

      - uses: actions/checkout@34e114876b0b11c390a56381ad16ebd13914f8d5  # v4
        with:
          fetch-depth: 0

      - name: Configure git identity
        run: |
          git config user.name "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"

      - name: Create local release tag
        run: git tag "v${{ steps.normalize.outputs.version }}"

      - name: Install uv
        uses: astral-sh/setup-uv@e58605a9b6da7c637471fab8847a5e5a6b8df081  # v5

      - name: Build wheel and sdist
        run: uv build

      - name: Guard against duplicate tag
        run: |
          if git ls-remote --tags origin "refs/tags/v${{ steps.normalize.outputs.version }}" | grep -q .; then
            echo "::error::Tag v${{ steps.normalize.outputs.version }} already exists on origin. Aborting."
            exit 1
          fi

      - name: Push release tag
        run: git push origin "v${{ steps.normalize.outputs.version }}"

      - name: Upload dist artifact
        uses: actions/upload-artifact@ea165f8d65b6e75b540449e92b4886f43607fa02  # v4.6.2
        with:
          name: dist
          path: dist/
          if-no-files-found: error

  publish-testpypi:
    name: Publish → TestPyPI
    needs: build
    runs-on: ubuntu-latest
    timeout-minutes: 10
    environment:
      name: testpypi
      url: https://test.pypi.org/p/orcapod-extension-spikeinterface
    permissions:
      id-token: write
    steps:
      - name: Install uv
        uses: astral-sh/setup-uv@e58605a9b6da7c637471fab8847a5e5a6b8df081  # v5

      - name: Download dist artifact
        uses: actions/download-artifact@d3f86a106a0bac45b974a628896c90dbdf5c8093  # v4.3.0
        with:
          name: dist
          path: dist/

      - name: Publish to TestPyPI
        run: uv publish --publish-url https://test.pypi.org/legacy/ dist/*

  publish-pypi:
    name: Publish → PyPI
    needs: [build, publish-testpypi]
    runs-on: ubuntu-latest
    timeout-minutes: 10
    environment:
      name: pypi
      url: https://pypi.org/p/orcapod-extension-spikeinterface
    permissions:
      id-token: write
      contents: write
    steps:
      - name: Install uv
        uses: astral-sh/setup-uv@e58605a9b6da7c637471fab8847a5e5a6b8df081  # v5

      - name: Download dist artifact
        uses: actions/download-artifact@d3f86a106a0bac45b974a628896c90dbdf5c8093  # v4.3.0
        with:
          name: dist
          path: dist/

      - name: Publish to PyPI
        run: uv publish dist/*

      - name: Create GitHub Release
        uses: softprops/action-gh-release@3bb12739c298aeb8a4eeaf626c5b8d85266b0e65  # v2.6.2
        with:
          tag_name: "v${{ needs.build.outputs.version }}"
          generate_release_notes: true
          files: dist/*

  linear-sync:
    name: Sync Linear release
    needs: build
    runs-on: ubuntu-latest
    timeout-minutes: 5
    permissions:
      contents: read
    steps:
      - name: Checkout
        uses: actions/checkout@34e114876b0b11c390a56381ad16ebd13914f8d5  # v4
        with:
          fetch-depth: 0
      - name: Sync Linear release
        uses: linear/linear-release-action@c0cb8354a362c24c6d3e0948f37fd66d07588e3f  # v0
        with:
          access_key: ${{ secrets.LINEAR_ACCESS_KEY }}
          command: sync
          version: "v${{ needs.build.outputs.version }}"

  linear-complete:
    name: Complete Linear release
    needs: [build, publish-pypi, linear-sync]
    runs-on: ubuntu-latest
    timeout-minutes: 5
    permissions:
      contents: read
    steps:
      - name: Checkout
        uses: actions/checkout@34e114876b0b11c390a56381ad16ebd13914f8d5  # v4
        with:
          fetch-depth: 0
      - name: Complete Linear release
        uses: linear/linear-release-action@c0cb8354a362c24c6d3e0948f37fd66d07588e3f  # v0
        with:
          access_key: ${{ secrets.LINEAR_ACCESS_KEY }}
          command: complete
          version: "v${{ needs.build.outputs.version }}"
```

- [ ] **Step 4: Create `release-sync.yml`**

```yaml
name: Linear release sync

on:
  push:
    branches:
      - main

jobs:
  sync:
    runs-on: ubuntu-latest
    timeout-minutes: 5
    permissions:
      contents: read
    steps:
      - name: Checkout
        uses: actions/checkout@11bd71901bbe5b1630ceea73d27597364c9af683  # v4.2.2
        with:
          fetch-depth: 0
      - name: Sync Linear release
        uses: linear/linear-release-action@c0cb8354a362c24c6d3e0948f37fd66d07588e3f  # v0
        with:
          access_key: ${{ secrets.LINEAR_ACCESS_KEY }}
          command: sync
```

- [ ] **Step 5: Commit CI workflows**

```bash
git add .github/workflows/
git commit -m "ci: add run-tests, license-check, release, release-sync workflows (ITL-473)"
```

---

### Task 15: Move SI design specs into new repo

- [ ] **Step 1: Copy the spec files from orcapod-python**

```bash
cp /home/kurouto/kurouto-jobs/17b9f34a-40ec-489c-9ccd-6c96c7a9cdf0/orcapod-python/superpowers/specs/_moved_to_extension_repo_2026-07-01-spikeinterface-baserecording-design.md \
   superpowers/specs/2026-07-01-spikeinterface-baserecording-design.md
cp /home/kurouto/kurouto-jobs/17b9f34a-40ec-489c-9ccd-6c96c7a9cdf0/orcapod-python/superpowers/specs/_moved_to_extension_repo_2026-07-02-si-sorting-logical-type-design.md \
   superpowers/specs/2026-07-02-si-sorting-logical-type-design.md
cp /home/kurouto/kurouto-jobs/17b9f34a-40ec-489c-9ccd-6c96c7a9cdf0/orcapod-python/superpowers/specs/_moved_to_extension_repo_2026-07-02-spikeinterface-motion-design.md \
   superpowers/specs/2026-07-02-spikeinterface-motion-design.md
cp /home/kurouto/kurouto-jobs/17b9f34a-40ec-489c-9ccd-6c96c7a9cdf0/orcapod-python/superpowers/specs/_moved_to_extension_repo_2026-07-03-sorting-analyzer-logical-type-design.md \
   superpowers/specs/2026-07-03-sorting-analyzer-logical-type-design.md
```

- [ ] **Step 2: Remove the staged-for-deletion copies from orcapod-python**

```bash
cd /home/kurouto/kurouto-jobs/17b9f34a-40ec-489c-9ccd-6c96c7a9cdf0/orcapod-python
git rm superpowers/specs/_moved_to_extension_repo_2026-07-01-spikeinterface-baserecording-design.md
git rm superpowers/specs/_moved_to_extension_repo_2026-07-02-si-sorting-logical-type-design.md
git rm superpowers/specs/_moved_to_extension_repo_2026-07-02-spikeinterface-motion-design.md
git rm superpowers/specs/_moved_to_extension_repo_2026-07-03-sorting-analyzer-logical-type-design.md
git commit -m "docs(itl-473): remove SI design specs moved to extension repo"
```

- [ ] **Step 3: Commit specs into new repo**

```bash
cd /home/kurouto/kurouto-jobs/17b9f34a-40ec-489c-9ccd-6c96c7a9cdf0/orcapod-extension-spikeinterface
git add superpowers/specs/
git commit -m "docs: add SI design specs moved from orcapod-python (ITL-473)"
```

---

### Task 16: Install deps and run tests in new package

- [ ] **Step 1: Initialize uv project and install deps**

```bash
cd /home/kurouto/kurouto-jobs/17b9f34a-40ec-489c-9ccd-6c96c7a9cdf0/orcapod-extension-spikeinterface
uv sync --dev
```

- [ ] **Step 2: Run the tests**

```bash
uv run pytest -v --tb=short
```

Expected: All tests pass (spikeinterface is installed as a required dep; tests that previously required `pytest.importorskip("spikeinterface")` will now always run).

If any test fails due to import path issues, fix the remaining `from orcapod.extension_types.spikeinterface_types import` references in `tests/test_spikeinterface_types.py`.

- [ ] **Step 3: Verify the public API works end-to-end**

```bash
uv run python -c "
import orcapod as op
from orcapod_extension_spikeinterface import spikeinterface_extension
op.register_extension(spikeinterface_extension)
from orcapod.contexts import get_default_context
ctx = get_default_context()
print('context_key:', ctx.context_key)
print('register_extension OK')
"
```

Expected:
```
context_key: std:v0.1:default
register_extension OK
```

---

### Task 17: Push new repo and finalize orcapod-python PR

- [ ] **Step 1: Push the new repo to GitHub**

```bash
cd /home/kurouto/kurouto-jobs/17b9f34a-40ec-489c-9ccd-6c96c7a9cdf0/orcapod-extension-spikeinterface
git push -u origin main
```

- [ ] **Step 2: Push any remaining orcapod-python changes**

```bash
cd /home/kurouto/kurouto-jobs/17b9f34a-40ec-489c-9ccd-6c96c7a9cdf0/orcapod-python
git push origin eywalker/itl-473-split-spikeinterface-extension-into-separate-orcapod
```

- [ ] **Step 3: Verify the PR is up-to-date**

```bash
gh pr view --web
```

Confirm all commits appear in the PR diff.
