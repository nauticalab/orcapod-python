# Rename Config → OrcapodConfig Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rename the `Config` class in `orcapod/config.py` to `OrcapodConfig` everywhere it appears — class definition, all internal usages, all test usages — and add it to the top-level public API.

**Architecture:** Pure identifier rename with no logic changes. `config.py` is the single source of truth; all 15 consumer source files and 2 test files reference the class by import. The top-level `__init__.py` does not currently expose `Config`, but will expose `OrcapodConfig` after this rename. No backward-compat alias — project CLAUDE.md prohibits shims pre-v0.1.0.

**Tech Stack:** Python 3.12+, `uv run` for all commands, `pytest` for tests.

---

## File Map

| Action | File |
|---|---|
| Modify | `src/orcapod/config.py` — rename class + update internal refs |
| Modify | `src/orcapod/__init__.py` — add OrcapodConfig to public API |
| Modify | `src/orcapod/core/base.py` — import + 3 type hints |
| Modify | `src/orcapod/core/datagrams/datagram.py` — import + 1 type hint |
| Modify | `src/orcapod/core/data_function.py` — import + 2 type hints |
| Modify | `src/orcapod/core/function_pod.py` — import + 1 type hint |
| Modify | `src/orcapod/core/nodes/function_node.py` — import + 3 type hints |
| Modify | `src/orcapod/core/nodes/operator_node.py` — import + 2 type hints |
| Modify | `src/orcapod/core/nodes/source_node.py` — import + 1 type hint |
| Modify | `src/orcapod/core/operators/static_output_pod.py` — import + 1 type hint |
| Modify | `src/orcapod/core/sources/base.py` — import + 1 type hint |
| Modify | `src/orcapod/core/sources/cached_source.py` — import + 1 type hint |
| Modify | `src/orcapod/core/sources/db_table_source.py` — TYPE_CHECKING import + 1 type hint |
| Modify | `src/orcapod/core/sources/postgresql_table_source.py` — TYPE_CHECKING import + 1 type hint |
| Modify | `src/orcapod/core/sources/spiraldb_table_source.py` — TYPE_CHECKING import + 1 type hint |
| Modify | `src/orcapod/core/sources/sqlite_table_source.py` — TYPE_CHECKING import + 1 type hint |
| Modify | `src/orcapod/core/sources/stream_builder.py` — TYPE_CHECKING import + 1 type hint |
| Modify | `tests/test_core/operators/test_operators.py` — 2 local imports + usages |
| Modify | `tests/test_core/operators/test_merge_join.py` — 1 local import + usage |
| Create | `tests/test_orcapod_config.py` — new tests for OrcapodConfig name |

---

### Task 1: Write the Failing Test

**Files:**
- Create: `tests/test_orcapod_config.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/test_orcapod_config.py
"""Tests for OrcapodConfig class naming (ENG-514)."""


class TestOrcapodConfigModule:
    def test_can_import_orcapod_config_from_config_module(self):
        from orcapod.config import OrcapodConfig  # noqa: F401

    def test_orcapod_config_is_instantiable_with_defaults(self):
        from orcapod.config import OrcapodConfig

        cfg = OrcapodConfig()
        assert cfg.system_tag_hash_n_char == 12
        assert cfg.schema_hash_n_char == 12
        assert cfg.path_hash_n_char == 20

    def test_default_config_is_orcapod_config_instance(self):
        from orcapod.config import DEFAULT_CONFIG, OrcapodConfig

        assert isinstance(DEFAULT_CONFIG, OrcapodConfig)

    def test_orcapod_config_with_updates(self):
        from orcapod.config import OrcapodConfig

        cfg = OrcapodConfig()
        updated = cfg.with_updates(system_tag_hash_n_char=8)
        assert updated.system_tag_hash_n_char == 8
        assert cfg.system_tag_hash_n_char == 12  # original unchanged

    def test_orcapod_config_merge(self):
        from orcapod.config import OrcapodConfig

        base = OrcapodConfig()
        other = OrcapodConfig(system_tag_hash_n_char=8)
        merged = base.merge(other)
        assert merged.system_tag_hash_n_char == 8
        assert merged.schema_hash_n_char == 12  # unchanged default

    def test_orcapod_config_merge_type_error(self):
        import pytest

        from orcapod.config import OrcapodConfig

        cfg = OrcapodConfig()
        with pytest.raises(TypeError):
            cfg.merge("not a config")  # type: ignore[arg-type]


class TestOrcapodConfigTopLevelExport:
    def test_can_import_orcapod_config_from_orcapod(self):
        from orcapod import OrcapodConfig  # noqa: F401

    def test_orcapod_config_in_all(self):
        import orcapod

        assert "OrcapodConfig" in orcapod.__all__
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd /path/to/orcapod-python  # replace with actual repo path
uv run pytest tests/test_orcapod_config.py -v
```

Expected: FAIL — `ImportError: cannot import name 'OrcapodConfig' from 'orcapod.config'`

---

### Task 2: Rename Class in config.py

**Files:**
- Modify: `src/orcapod/config.py`

- [ ] **Step 1: Replace the entire file with the renamed version**

Replace the full contents of `src/orcapod/config.py` with:

```python
# config.py
from dataclasses import dataclass, replace
from typing import Self


@dataclass(frozen=True)
class OrcapodConfig:
    """Immutable OrcaPod configuration object."""

    system_tag_hash_n_char: int = 12
    schema_hash_n_char: int = 12
    path_hash_n_char: int = 20

    def with_updates(self, **kwargs) -> Self:
        """Create a new ``OrcapodConfig`` instance with updated values."""
        return replace(self, **kwargs)

    def merge(self, other: "OrcapodConfig") -> "OrcapodConfig":
        """Merge with another config, other takes precedence."""
        if not isinstance(other, OrcapodConfig):
            raise TypeError("Can only merge with another OrcapodConfig instance")

        # Get all non-default values from other
        defaults = OrcapodConfig()
        updates = {}
        for field_name in self.__dataclass_fields__:
            other_value = getattr(other, field_name)
            default_value = getattr(defaults, field_name)
            if other_value != default_value:
                updates[field_name] = other_value

        return self.with_updates(**updates)


# Module-level default config - created at import time
DEFAULT_CONFIG = OrcapodConfig()
```

- [ ] **Step 2: Run the new tests to verify they pass (except the top-level export tests)**

```bash
uv run pytest tests/test_orcapod_config.py::TestOrcapodConfigModule -v
```

Expected: All 6 tests in `TestOrcapodConfigModule` PASS.
`TestOrcapodConfigTopLevelExport` will still fail — that is expected until Task 4.

- [ ] **Step 3: Commit**

```bash
git add src/orcapod/config.py tests/test_orcapod_config.py
git commit -m "refactor(config): rename Config → OrcapodConfig"
```

---

### Task 3: Update All Consumer Source and Test Files

**Files:**
- Modify: all 15 source files that import `Config`
- Modify: 2 test files that locally import `Config`

Every file that imports `Config` from `orcapod.config` needs two kinds of edits:
1. The import line: `Config` → `OrcapodConfig`
2. Every type hint or usage: `Config` → `OrcapodConfig`

`ColumnConfig`, `NodeConfig`, `PipelineConfig` are distinct classes and must **not** be changed.
The `sed` pattern `\bConfig\b` (whole-word match) safely renames only the bare word `Config`.

- [ ] **Step 1: Update the 10 source files with direct (non-TYPE_CHECKING) imports**

For each file listed below, make the two edits shown.

**`src/orcapod/core/base.py`** — import line 9, type hints lines 78, 88, 120, 363:

```
Old: from orcapod.config import DEFAULT_CONFIG, Config
New: from orcapod.config import DEFAULT_CONFIG, OrcapodConfig
```
Then replace every remaining `Config` (type hint only — `ColumnConfig`, `NodeConfig` are not present in this file):
- `config: Config | None = None,` → `config: OrcapodConfig | None = None,` (lines 78, 120, 363)
- `def orcapod_config(self) -> Config:` → `def orcapod_config(self) -> OrcapodConfig:` (line 88)

**`src/orcapod/core/datagrams/datagram.py`** — import line 29, type hint line 71:

```
Old: from orcapod.config import Config
New: from orcapod.config import OrcapodConfig
```
- `config: Config | None = None,` → `config: OrcapodConfig | None = None,` (line 71)

**`src/orcapod/core/data_function.py`** — import line 14, type hints lines 138, 377:

```
Old: from orcapod.config import Config
New: from orcapod.config import OrcapodConfig
```
- `config: Config | None = None,` → `config: OrcapodConfig | None = None,` (lines 138, 377)

**`src/orcapod/core/function_pod.py`** — import line 12, type hint line 68:

```
Old: from orcapod.config import Config
New: from orcapod.config import OrcapodConfig
```
- `config: Config | None = None,` → `config: OrcapodConfig | None = None,` (line 68)

**`src/orcapod/core/nodes/function_node.py`** — import line 26, type hints lines 99, 453, 667:

```
Old: from orcapod.config import Config
New: from orcapod.config import OrcapodConfig
```
- `config: Config | None = None,` → `config: OrcapodConfig | None = None,` (lines 99, 453, 667)

**`src/orcapod/core/nodes/operator_node.py`** — import line 26, type hints lines 77, 520:

```
Old: from orcapod.config import Config
New: from orcapod.config import OrcapodConfig
```
- `config: Config | None = None,` → `config: OrcapodConfig | None = None,` (lines 77, 520)

**`src/orcapod/core/nodes/source_node.py`** — import line 15, type hint line 58:

```
Old: from orcapod.config import Config
New: from orcapod.config import OrcapodConfig
```
- `config: Config | None = None,` → `config: OrcapodConfig | None = None,` (line 58)

**`src/orcapod/core/operators/static_output_pod.py`** — import line 11, type hint line 272:

```
Old: from orcapod.config import Config
New: from orcapod.config import OrcapodConfig
```
- `config: Config | None = None,` → `config: OrcapodConfig | None = None,` (line 272)

**`src/orcapod/core/sources/base.py`** — import line 8, type hint line 63:

```
Old: from orcapod.config import Config
New: from orcapod.config import OrcapodConfig
```
- `config: Config | None = None,` → `config: OrcapodConfig | None = None,` (line 63)

**`src/orcapod/core/sources/cached_source.py`** — import line 8, type hint line 62:

```
Old: from orcapod.config import Config
New: from orcapod.config import OrcapodConfig
```
- `config: Config | None = None,` → `config: OrcapodConfig | None = None,` (line 62)

- [ ] **Step 2: Update the 5 TYPE_CHECKING source files**

These files import `Config` inside `if TYPE_CHECKING:` blocks. Same two edits each.

**`src/orcapod/core/sources/db_table_source.py`** — TYPE_CHECKING import line 26, type hint line 72:

```
Old:     from orcapod.config import Config
New:     from orcapod.config import OrcapodConfig
```
- `config: Config | None = None,` → `config: OrcapodConfig | None = None,` (line 72)

**`src/orcapod/core/sources/postgresql_table_source.py`** — TYPE_CHECKING import line 22, type hint line 72:

```
Old:     from orcapod.config import Config
New:     from orcapod.config import OrcapodConfig
```
- `config: Config | None = None,` → `config: OrcapodConfig | None = None,` (line 72)

**`src/orcapod/core/sources/spiraldb_table_source.py`** — TYPE_CHECKING import line 45, type hint line 101:

```
Old:     from orcapod.config import Config
New:     from orcapod.config import OrcapodConfig
```
- `config: "Config | None" = None,` → `config: "OrcapodConfig | None" = None,` (line 101)

**`src/orcapod/core/sources/sqlite_table_source.py`** — TYPE_CHECKING import line 32, type hint line 82:

```
Old:     from orcapod.config import Config
New:     from orcapod.config import OrcapodConfig
```
- `config: "Config | None" = None,` → `config: "OrcapodConfig | None" = None,` (line 82)

**`src/orcapod/core/sources/stream_builder.py`** — TYPE_CHECKING import line 24, type hint line 62:

```
Old:     from orcapod.config import Config
New:     from orcapod.config import OrcapodConfig
```
- `config: Config` → `config: OrcapodConfig` (line 62)

- [ ] **Step 3: Update the 2 test files with local Config imports**

**`tests/test_core/operators/test_operators.py`** — 2 local imports at lines 1316 and 1411:

At line 1316 (inside a test function body):
```
Old:         from orcapod.config import Config
             ...
             n_char = Config().system_tag_hash_n_char
New:         from orcapod.config import OrcapodConfig
             ...
             n_char = OrcapodConfig().system_tag_hash_n_char
```

At line 1411 (inside a second test function body):
```
Old:         from orcapod.config import Config
             ...
             n_char = Config().system_tag_hash_n_char
New:         from orcapod.config import OrcapodConfig
             ...
             n_char = OrcapodConfig().system_tag_hash_n_char
```

**`tests/test_core/operators/test_merge_join.py`** — 1 local import at line 644:

```
Old:         from orcapod.config import Config
             ...
             n_char = Config().system_tag_hash_n_char
New:         from orcapod.config import OrcapodConfig
             ...
             n_char = OrcapodConfig().system_tag_hash_n_char
```

- [ ] **Step 4: Run the full test suite to verify no regressions**

```bash
uv run pytest tests/ -v --ignore=tests/test_orcapod_config.py::TestOrcapodConfigTopLevelExport -x
```

Expected: All existing tests PASS. The `TestOrcapodConfigTopLevelExport` class in `test_orcapod_config.py` will still fail — skip or ignore it for now; it passes after Task 4.

- [ ] **Step 5: Commit**

```bash
git add \
  src/orcapod/core/base.py \
  src/orcapod/core/datagrams/datagram.py \
  src/orcapod/core/data_function.py \
  src/orcapod/core/function_pod.py \
  src/orcapod/core/nodes/function_node.py \
  src/orcapod/core/nodes/operator_node.py \
  src/orcapod/core/nodes/source_node.py \
  src/orcapod/core/operators/static_output_pod.py \
  src/orcapod/core/sources/base.py \
  src/orcapod/core/sources/cached_source.py \
  src/orcapod/core/sources/db_table_source.py \
  src/orcapod/core/sources/postgresql_table_source.py \
  src/orcapod/core/sources/spiraldb_table_source.py \
  src/orcapod/core/sources/sqlite_table_source.py \
  src/orcapod/core/sources/stream_builder.py \
  tests/test_core/operators/test_operators.py \
  tests/test_core/operators/test_merge_join.py
git commit -m "refactor(config): update all Config → OrcapodConfig references"
```

---

### Task 4: Add OrcapodConfig to Top-Level __init__.py

**Files:**
- Modify: `src/orcapod/__init__.py`

- [ ] **Step 1: Add the import and __all__ entry**

Current `src/orcapod/__init__.py`:

```python
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

__all__ = [
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
]
```

New `src/orcapod/__init__.py` — add `OrcapodConfig` import after the existing imports and add it to `__all__`:

```python
from .config import OrcapodConfig
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

__all__ = [
    "OrcapodConfig",
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
]
```

- [ ] **Step 2: Run the full test suite including top-level export tests**

```bash
uv run pytest tests/ -v -x
```

Expected: ALL tests PASS, including `TestOrcapodConfigTopLevelExport`.

- [ ] **Step 3: Commit**

```bash
git add src/orcapod/__init__.py
git commit -m "feat(config): export OrcapodConfig from top-level orcapod package"
```

---

### Task 5: Final Verification

- [ ] **Step 1: Run the full test suite one final time**

```bash
uv run pytest tests/ -v
```

Expected: All tests PASS with zero failures.

- [ ] **Step 2: Verify the public API by hand**

```bash
uv run python -c "
from orcapod import OrcapodConfig
from orcapod.config import OrcapodConfig, DEFAULT_CONFIG
cfg = OrcapodConfig(system_tag_hash_n_char=8)
print('OrcapodConfig:', cfg)
print('DEFAULT_CONFIG:', DEFAULT_CONFIG)
print('isinstance check:', isinstance(DEFAULT_CONFIG, OrcapodConfig))
"
```

Expected output:
```
OrcapodConfig: OrcapodConfig(system_tag_hash_n_char=8, schema_hash_n_char=12, path_hash_n_char=20)
DEFAULT_CONFIG: OrcapodConfig(system_tag_hash_n_char=12, schema_hash_n_char=12, path_hash_n_char=20)
isinstance check: True
```

- [ ] **Step 3: Confirm no stale `Config` references remain in source or tests**

```bash
grep -rn '\bConfig\b' src/orcapod/ tests/ \
  --include="*.py" \
  | grep -v "ColumnConfig\|NodeConfig\|PipelineConfig\|CacheConfig\|DatabaseConfig\|# config\|_config\b"
```

Expected: Zero lines output. If any lines appear, fix them before creating the PR.

---

## Self-Review Checklist

- **Spec coverage:**
  - ✅ Class definition site renamed (`config.py`)
  - ✅ All internal usages — imports, type hints, isinstance checks, `__init__` defaults, factory functions (`config.py` `merge()` method, `DEFAULT_CONFIG`)
  - ✅ Public re-export — `OrcapodConfig` added to top-level `orcapod.__init__`
  - ✅ Tests and fixtures updated (`test_operators.py`, `test_merge_join.py`)
  - ✅ New tests written (`test_orcapod_config.py`)
  - ✅ No backward-compat alias (per project CLAUDE.md — greenfield pre-v0.1.0)
  - ✅ All tests pass

- **Out of scope (do NOT touch):**
  - `ColumnConfig`, `NodeConfig`, `PipelineConfig` — distinct classes, unchanged
  - Any restructuring of fields, defaults, or behavior of OrcapodConfig
