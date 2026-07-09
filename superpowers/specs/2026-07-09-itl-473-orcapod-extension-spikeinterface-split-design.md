# Design: Split SpikeInterface Extension into `orcapod-extension-spikeinterface` Package

**Issue:** ITL-473
**Date:** 2026-07-09
**Status:** Approved

---

## Overview

`LogicalSIRecording`, `LogicalSISorting`, `LogicalSIMotion`, `LogicalSISortingAnalyzer` and
their corresponding handlers are currently implemented in `orcapod-python` under
`src/orcapod/extension_types/spikeinterface_types.py`. Because SpikeInterface is a large,
domain-specific dependency, the goal is to move all SI-related code into a standalone pip
package `orcapod-extension-spikeinterface` that declares `orcapod` as a base dependency, so
that `pip install orcapod` installs no spikeinterface-related code at all.

This split also introduces a normalized extension registration API: `orcapod` gains a thin
`OrcapodExtension` protocol and an `op.register_extension()` function so that all future
extension packages register themselves through a uniform interface.

---

## Goals & Success Criteria

* `orcapod-extension-spikeinterface` is a standalone pip package published to PyPI, with its
  own `pyproject.toml`, `src/`, `tests/`, and CI.
* `pip install orcapod` installs zero spikeinterface-related code or transitive deps.
* `pip install orcapod-extension-spikeinterface` installs the extension and `orcapod` as a
  base dependency; registering with `op.register_extension(spikeinterface_extension)` wires
  the SI types into the default (or any specified) context.
* The core `orcapod` test suite (without spikeinterface installed) continues to pass with no
  SI-related skips or conditional imports.
* The new repo's CI mirrors orcapod-python: matrix tests on Python 3.11 and 3.12, license
  check, and the same OIDC Trusted Publishing release flow to PyPI.

---

## Scope & Boundaries

In scope:

* Add `OrcapodExtension` protocol and `register_extension()` function to `orcapod-python`.
* Create new GitHub repository `nauticalab/orcapod-extension-spikeinterface`.
* Move `src/orcapod/extension_types/spikeinterface_types.py` to
  `src/orcapod_extension_spikeinterface/_spikeinterface_types.py` in the new repo.
* Move `tests/test_extension_types/test_spikeinterface_types.py` to
  `tests/test_spikeinterface_types.py` in the new repo.
* Move the four SI design specs from `superpowers/specs/` in orcapod-python to the
  equivalent directory in the new repo.
* Remove all SI wiring from `orcapod-python` (detailed below).

Out of scope:

* `BaseSorting`, `SortingAnalyzer`, `Motion` extensions are already included in the current
  `spikeinterface_types.py` and move with it. Future SI types (ITL-468/469/470) will be added
  to the extension package directly.
* Entry-point / plugin discovery infrastructure in core `orcapod`.
* The `_optional` flag mechanism in `parse_objectspec` — evaluated separately once SI is
  removed; keep it for now in case other optional extensions use it in future.

---

## Normalized Extension API (new in `orcapod-python`)

### Protocol

A new file `src/orcapod/extensions.py` defines the minimal interface all orcapod extensions
must implement:

```python
# src/orcapod/extensions.py
from __future__ import annotations
from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from orcapod.contexts.data_context import DataContext


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

`OrcapodExtension` and `register_extension` are exported from `orcapod.__init__` so that
`import orcapod as op; op.register_extension(...)` works.

### Extension object contract

* `name` — a short, lowercase string identifying the extension (e.g. `"spikeinterface"`).
  Used in debug log messages only; not a unique key.
* `register(context: DataContext)` — performs all type registrations into the given context.
  Always receives a concrete `DataContext` — context resolution is the responsibility of
  `register_extension`, not of the extension itself. Must be idempotent (re-registering an
  already registered type is a no-op with a debug log, matching the existing behaviour of
  `register_spikeinterface_types()`).

---

## New Package Design

### Repository

* **GitHub repo:** `nauticalab/orcapod-extension-spikeinterface`
* **Default branch:** `main`
* **Linear team:** Tools (same as orcapod-python)

### Directory layout

```
orcapod-extension-spikeinterface/
├── .github/
│   └── workflows/
│       ├── run-tests.yml         # CI matrix (Python 3.11, 3.12); no extra services needed
│       ├── _license-check.yml    # License check; quantities is a required dep here — no ignore
│       ├── release.yml           # OIDC Trusted Publishing → TestPyPI → PyPI + GitHub Release
│       └── release-sync.yml      # Linear release sync on push to main
├── src/
│   └── orcapod_extension_spikeinterface/
│       ├── __init__.py           # Public API re-exports + SpikeInterfaceExtension singleton
│       └── _spikeinterface_types.py  # Moved from orcapod-python
├── tests/
│   ├── __init__.py
│   └── test_spikeinterface_types.py  # Moved from orcapod-python
├── superpowers/
│   └── specs/                    # Four SI design specs moved from orcapod-python
├── .pre-commit-config.yaml       # Same hooks: ruff-format, sync-with-uv, trailing-whitespace
├── CHANGELOG.md
├── CLAUDE.md
├── pyproject.toml
├── pytest.ini
└── README.md
```

### Python import namespace

The package uses a flat top-level module `orcapod_extension_spikeinterface`. This avoids
Python namespace-package complexity (which would require `orcapod` itself to be a namespace
package) and is unambiguous:

```python
from orcapod_extension_spikeinterface import spikeinterface_extension
import orcapod as op
op.register_extension(spikeinterface_extension)
```

### `pyproject.toml` (new package)

```toml
[project]
name = "orcapod-extension-spikeinterface"
dependencies = [
    "orcapod>=0.1",
    "spikeinterface>=0.101",
]
# Same hatchling + hatch-vcs setup as orcapod-python
# No optional extras — spikeinterface is a required dep of this package
```

### `SpikeInterfaceExtension` class and singleton

```python
# src/orcapod_extension_spikeinterface/__init__.py

from ._spikeinterface_types import (
    LogicalSIRecording,
    LogicalSISorting,
    LogicalSIMotion,
    LogicalSISortingAnalyzer,
    SIRecordingHandler,
    SISortingHandler,
    SIMotionHandler,
    SISortingAnalyzerHandler,
    _register_spikeinterface_types,   # renamed to private
)


class SpikeInterfaceExtension:
    """OrcaPod extension for SpikeInterface types.

    Implements ``OrcapodExtension``. Register via::

        import orcapod as op
        from orcapod_extension_spikeinterface import spikeinterface_extension
        op.register_extension(spikeinterface_extension)
    """

    name = "spikeinterface"

    def register(self, context: DataContext) -> None:
        """Register all SpikeInterface types into ``context``.

        Args:
            context: Target ``DataContext``. Always a concrete context —
                resolution from ``None`` is handled by ``op.register_extension``.
        """
        _register_spikeinterface_types(context)


#: Module-level singleton — the canonical extension object.
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

The old `register_spikeinterface_types()` function is renamed to `_register_spikeinterface_types`
(private) inside `_spikeinterface_types.py` — it remains the implementation workhorse but is
no longer part of the public API. Its docstring is updated to reference
`pip install orcapod-extension-spikeinterface`.

### CI: `run-tests.yml`

Mirrors orcapod-python's `run-tests.yml` with these differences:
* No MinIO service (not needed for SI tests)
* No graphviz system dep (not needed)
* No SpiralDB integration job
* License check does **not** ignore `quantities` (it's a first-class transitive dep here)

### CI: `release.yml`

Identical structure to orcapod-python's `release.yml`:
* Manual `workflow_dispatch` trigger
* Test matrix (3.11, 3.12) + license-check as prerequisites
* Build with `uv build` + hatch-vcs tag
* Publish to TestPyPI then PyPI via OIDC Trusted Publishing
* Create GitHub Release with generated notes
* Linear release sync + complete steps

---

## Changes to `orcapod-python`

### 0. Add extension protocol and `register_extension()` function

* Create `src/orcapod/extensions.py` with `OrcapodExtension` protocol and
  `register_extension()` function (see Normalized Extension API section above).
* Export `OrcapodExtension` and `register_extension` from `src/orcapod/__init__.py`.
* Add a unit test `tests/test_extensions.py` verifying `register_extension` delegates
  correctly to `extension.register(context)`.

### 1. Delete source file

```
src/orcapod/extension_types/spikeinterface_types.py  → deleted
```

### 2. Delete test file

```
tests/test_extension_types/test_spikeinterface_types.py  → deleted
```

### 3. Remove SI block from `extension_types/__init__.py`

Remove the `try/except ImportError` block and the conditional `__all__` entries for the
seven SI symbols.

### 4. Remove SI entries from `v0.1.json`

Remove from `logical_types` array: the four entries with `"_optional": true` pointing to
`orcapod.extension_types.spikeinterface_types.*`.

Remove from the `handlers` array in `python_type_handler_registry`: the four pairs with
`"_optional": true` for `spikeinterface.core.BaseRecording`, `BaseSorting`,
`spikeinterface.core.motion.Motion`, and `SortingAnalyzer`.

### 5. Remove spikeinterface from `pyproject.toml`

* Remove `spikeinterface = ["spikeinterface>=0.101"]` from `[project.optional-dependencies]`
* Remove `"orcapod[spikeinterface]"` from the `all` extras list
* Remove `"spikeinterface>=0.101"` from `[dependency-groups] dev`

### 6. Remove `quantities` ignore from license check workflow

In `.github/workflows/_license-check.yml`, remove `--ignore-packages quantities` from the
`pip-licenses` invocation.

### 7. Move SI design specs

Move the following four files from `superpowers/specs/` in orcapod-python to
`superpowers/specs/` in the new repo:
* `2026-07-01-spikeinterface-baserecording-design.md`
* `2026-07-02-si-sorting-logical-type-design.md`
* `2026-07-02-spikeinterface-motion-design.md`
* `2026-07-03-sorting-analyzer-logical-type-design.md`

### 8. Regenerate `uv.lock`

Run `uv lock` in `orcapod-python` after removing spikeinterface and its transitive deps
(neo, quantities, probeinterface, zarr, numcodecs, fasteners, threadpoolctl, asciitree).

---

## Dependencies & Risks

* The `_optional` flag mechanism in `parse_objectspec` remains in orcapod-python for now;
  it can be removed later if no other optional extensions use it.
* ITL-468/469/470 (`BaseSorting`, `SortingAnalyzer`, `Motion`) are already included in the
  current `spikeinterface_types.py` so they move with it automatically — no follow-up split
  needed for those.
* `orcapod>=0.1` version pin is a placeholder; update to the actual first published version
  once known.

---

## Resources & References

* ITL-473 — this issue
* ITL-459 — original `LogicalSIRecording` implementation
* `src/orcapod/extension_types/spikeinterface_types.py` — code to move
* `src/orcapod/contexts/data/v0.1.json` — entries to remove
* `src/orcapod/extension_types/__init__.py` — re-export block to remove
* `pyproject.toml` — optional extras + dev dep to remove
* `.github/workflows/_license-check.yml` — `quantities` ignore to remove
