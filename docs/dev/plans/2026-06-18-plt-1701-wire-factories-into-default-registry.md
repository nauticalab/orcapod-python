# PLT-1701: Wire Factories into Default LogicalTypeRegistry Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wire `DataclassLogicalTypeFactory` and `PydanticLogicalTypeFactory` into the default `LogicalTypeRegistry` so dataclass- and pydantic-annotated pod fields are handled automatically with zero user-side setup.

**Architecture:** Four targeted changes: (1) promote pydantic to a required dep, (2) harden `PydanticLogicalTypeFactory.supports_class` by dropping the `try/except ImportError` guard, (3) add a `factories` constructor parameter to `LogicalTypeRegistry` that calls `register_logical_type_factory` for each entry, (4) wire both factories into `v0.1.json`. A new test file verifies registry construction and default-context end-to-end behaviour.

**Tech Stack:** Python 3.11+, PyArrow, pydantic v2, `uv` for dependency management.

---

## File Map

| File | Action | What changes |
|---|---|---|
| `pyproject.toml` | Modify | Move pydantic from optional to required dependency |
| `src/orcapod/extension_types/pydantic_logical_type_factory.py` | Modify | `supports_class`: drop `try/except ImportError`, import pydantic directly |
| `src/orcapod/extension_types/registry.py` | Modify | `LogicalTypeRegistry.__init__`: add `factories` parameter |
| `src/orcapod/contexts/data/v0.1.json` | Modify | Add `factories` list under `logical_type_registry._config` |
| `tests/test_extension_types/test_default_context_factories.py` | Create | Registry unit tests + default-context integration tests |

---

## Task 0: Create and check out the feature branch

**Files:** (none — git only)

- [ ] **Step 1: Create and check out the branch from `extension-type-system`**

```bash
git checkout extension-type-system
git checkout -b eywalker/plt-1701-wire-dataclasshandlerfactory-into-the-default
git branch --show-current
```

Expected: prints `eywalker/plt-1701-wire-dataclasshandlerfactory-into-the-default`.

---

## Task 1: Promote pydantic to a required dependency

**Files:**
- Modify: `pyproject.toml`

- [ ] **Step 1: Move pydantic into `[project.dependencies]`**

In `pyproject.toml`, add `"pydantic>=2.0"` to `[project.dependencies]` and remove the `pydantic` entry from `[project.optional-dependencies]` (keep the `all` extra but remove `"orcapod[pydantic]"` from it):

```toml
[project]
dependencies = [
    "xxhash",
    "networkx",
    "typing_extensions",
    "matplotlib>=3.10.3",
    "pandas>=2.2.3",
    "pyyaml>=6.0.2",
    "pyarrow>=20.0.0",
    "polars>=1.36.0",
    "beartype>=0.21.0",
    "deltalake>=1.0.2",
    "graphviz>=0.21",
    "gitpython>=3.1.45",
    "universal-pathlib>=0.3.8",
    "starfix>=0.2.0",
    "pygraphviz>=1.14",
    "tzdata>=2024.1",
    "uuid-utils>=0.11.1",
    "s3fs>=2025.12.0",
    "pymongo>=4.15.5",
    "basedpyright>=1.38.1",
    "pydantic>=2.0",
]

[project.optional-dependencies]
redis = ["redis>=6.2.0"]
ray = ["ray[default]==2.48.0", "ipywidgets>=8.1.7"]
postgresql = ["psycopg[binary]>=3.0"]
spiraldb = [
    "pyspiral>=0.11.0",
]
all = ["orcapod[redis]", "orcapod[ray]", "orcapod[postgresql]", "orcapod[spiraldb]"]
```

- [ ] **Step 2: Re-sync the environment**

```bash
uv sync
```

Expected: pydantic is resolved as a required dep. No errors.

- [ ] **Step 3: Verify pydantic is available**

```bash
uv run python -c "import pydantic; print(pydantic.__version__)"
```

Expected: prints a version string starting with `2.`.

- [ ] **Step 4: Run the existing pydantic factory tests to confirm nothing broke**

```bash
uv run pytest tests/test_extension_types/test_pydantic_logical_type_factory.py -v
```

Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add pyproject.toml
git commit -m "chore(deps): promote pydantic to required dependency"
```

---

## Task 2: Harden `PydanticLogicalTypeFactory.supports_class`

**Files:**
- Modify: `src/orcapod/extension_types/pydantic_logical_type_factory.py:211-225`

The current `supports_class` wraps its pydantic import in a `try/except ImportError` that silently returns `False` when pydantic is absent. Now that pydantic is required, this guard is dead code and should be removed. The behaviour when pydantic IS installed is identical — no new failing test is needed; the existing `test_pydantic_logical_type_factory.py` suite covers it.

- [ ] **Step 1: Update `supports_class` in `pydantic_logical_type_factory.py`**

Replace the current `supports_class` method (lines ~211–225):

```python
def supports_class(self, python_type: type) -> bool:
    """Return True if ``python_type`` is a pydantic ``BaseModel`` subclass.

    Args:
        python_type: Any Python type.

    Returns:
        True if ``python_type`` is a ``BaseModel`` subclass.
    """
    from pydantic import BaseModel
    return isinstance(python_type, type) and issubclass(python_type, BaseModel)
```

- [ ] **Step 2: Verify pydantic tests still pass**

```bash
uv run pytest tests/test_extension_types/test_pydantic_logical_type_factory.py -v
```

Expected: all tests pass.

- [ ] **Step 3: Commit**

```bash
git add src/orcapod/extension_types/pydantic_logical_type_factory.py
git commit -m "fix(pydantic-factory): drop try/except in supports_class — pydantic is now required"
```

---

## Task 3: Add `factories` parameter to `LogicalTypeRegistry.__init__`

**Files:**
- Modify: `src/orcapod/extension_types/registry.py:205-213`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_extension_types/test_default_context_factories.py` with just the registry unit tests:

```python
"""Tests for LogicalTypeRegistry factories parameter and default context factory wiring."""

from __future__ import annotations

import dataclasses

import pytest

from orcapod.extension_types.dataclass_logical_type_factory import (
    DataclassLogicalTypeFactory,
    DATACLASS_CATEGORY,
)
from orcapod.extension_types.pydantic_logical_type_factory import (
    PydanticLogicalTypeFactory,
    PYDANTIC_CATEGORY,
)
from orcapod.extension_types.registry import LogicalTypeRegistry


# ── Module-level dataclasses (local classes cannot be registered) ────────────

@dataclasses.dataclass
class _SimplePoint:
    x: int
    y: int


# ── Registry constructor unit tests ─────────────────────────────────────────

def test_registry_factories_param_registers_category():
    """factories param registers the factory under the given category."""
    factory = DataclassLogicalTypeFactory()
    registry = LogicalTypeRegistry(
        factories=[{"factory": factory, "category": DATACLASS_CATEGORY, "python_bases": [object]}]
    )
    assert registry._category_factories.get(DATACLASS_CATEGORY) is factory


def test_registry_factories_param_registers_python_base():
    """factories param registers the factory under each python_base."""
    factory = DataclassLogicalTypeFactory()
    registry = LogicalTypeRegistry(
        factories=[{"factory": factory, "category": DATACLASS_CATEGORY, "python_bases": [object]}]
    )
    assert registry._python_class_factories.get(object) is factory


def test_registry_factories_param_empty_list_is_noop():
    """factories=[] constructs successfully with no registered factories."""
    registry = LogicalTypeRegistry(factories=[])
    assert registry._category_factories == {}
    assert registry._python_class_factories == {}


def test_registry_factories_param_none_is_noop():
    """factories=None (default) constructs successfully."""
    registry = LogicalTypeRegistry(factories=None)
    assert registry._category_factories == {}
```

- [ ] **Step 2: Run the tests to confirm they fail**

```bash
uv run pytest tests/test_extension_types/test_default_context_factories.py::test_registry_factories_param_registers_category -v
```

Expected: `FAILED` — `LogicalTypeRegistry.__init__` does not yet accept `factories`.

- [ ] **Step 3: Update `LogicalTypeRegistry.__init__` in `registry.py`**

Replace the current `__init__` signature and body (lines ~205–212):

```python
def __init__(
    self,
    logical_types: list[LogicalTypeProtocol] | None = None,
    factories: list[dict] | None = None,
) -> None:
    self._by_logical_name: dict[str, LogicalTypeProtocol] = {}
    self._by_arrow_name: dict[str, LogicalTypeProtocol] = {}
    self._by_python_type: dict[type, LogicalTypeProtocol] = {}
    self._category_factories: dict[str, LogicalTypeFactoryProtocol] = {}
    self._python_class_factories: dict[type, LogicalTypeFactoryProtocol] = {}
    for lt in (logical_types or []):
        self.register_logical_type(lt)
    for entry in (factories or []):
        self.register_logical_type_factory(
            entry["factory"],
            category=entry.get("category"),
            python_bases=entry.get("python_bases", []),
        )
```

- [ ] **Step 4: Run the registry unit tests**

```bash
uv run pytest tests/test_extension_types/test_default_context_factories.py::test_registry_factories_param_registers_category tests/test_extension_types/test_default_context_factories.py::test_registry_factories_param_registers_python_base tests/test_extension_types/test_default_context_factories.py::test_registry_factories_param_empty_list_is_noop tests/test_extension_types/test_default_context_factories.py::test_registry_factories_param_none_is_noop -v
```

Expected: all 4 tests pass.

- [ ] **Step 5: Run the existing registry tests to confirm no regressions**

```bash
uv run pytest tests/test_extension_types/test_registry.py -v
```

Expected: all tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/extension_types/registry.py tests/test_extension_types/test_default_context_factories.py
git commit -m "feat(registry): add factories parameter to LogicalTypeRegistry.__init__"
```

---

## Task 4: Wire both factories into `v0.1.json`

**Files:**
- Modify: `src/orcapod/contexts/data/v0.1.json`

- [ ] **Step 1: Write the failing default-context tests**

Append these tests to `tests/test_extension_types/test_default_context_factories.py`:

```python
# ── Default context integration tests ────────────────────────────────────────
#
# All tests use create_registry().get_context() — NOT get_default_context() —
# to avoid cross-test contamination via the global singleton cache.

from orcapod.contexts import create_registry


def test_default_context_has_dataclass_factory():
    """Default context registers DataclassLogicalTypeFactory under orcapod.dataclass."""
    ctx = create_registry().get_context()
    registry = ctx.type_converter._logical_type_registry
    factory = registry._category_factories.get(DATACLASS_CATEGORY)
    assert isinstance(factory, DataclassLogicalTypeFactory)


def test_default_context_has_pydantic_factory():
    """Default context registers PydanticLogicalTypeFactory under orcapod.pydantic."""
    ctx = create_registry().get_context()
    registry = ctx.type_converter._logical_type_registry
    factory = registry._category_factories.get(PYDANTIC_CATEGORY)
    assert isinstance(factory, PydanticLogicalTypeFactory)
```

- [ ] **Step 2: Run those two tests to confirm they fail**

```bash
uv run pytest tests/test_extension_types/test_default_context_factories.py::test_default_context_has_dataclass_factory tests/test_extension_types/test_default_context_factories.py::test_default_context_has_pydantic_factory -v
```

Expected: both `FAILED` — factories not yet in `v0.1.json`.

- [ ] **Step 3: Add the `factories` list to `v0.1.json`**

In `src/orcapod/contexts/data/v0.1.json`, find the `logical_type_registry` object spec
(under `type_converter._config`) and add `"factories"` alongside `"logical_types"`:

```json
"logical_type_registry": {
    "_class": "orcapod.extension_types.registry.LogicalTypeRegistry",
    "_config": {
        "logical_types": [
            {
                "_class": "orcapod.extension_types.builtin_logical_types.LogicalPath",
                "_config": {}
            },
            {
                "_class": "orcapod.extension_types.builtin_logical_types.LogicalUPath",
                "_config": {}
            },
            {
                "_class": "orcapod.extension_types.builtin_logical_types.LogicalUUID",
                "_config": {}
            }
        ],
        "factories": [
            {
                "factory": {
                    "_class": "orcapod.extension_types.dataclass_logical_type_factory.DataclassLogicalTypeFactory",
                    "_config": {}
                },
                "category": "orcapod.dataclass",
                "python_bases": [{"_type": "builtins.object"}]
            },
            {
                "factory": {
                    "_class": "orcapod.extension_types.pydantic_logical_type_factory.PydanticLogicalTypeFactory",
                    "_config": {}
                },
                "category": "orcapod.pydantic",
                "python_bases": [{"_type": "pydantic.BaseModel"}]
            }
        ]
    }
}
```

`{"_type": "builtins.object"}` resolves to the `object` class via `parse_objectspec`.
`{"_type": "pydantic.BaseModel"}` resolves to `pydantic.BaseModel` the same way — no
instance is created, the class itself is passed as a `python_bases` entry.

- [ ] **Step 4: Run the default-context factory tests**

```bash
uv run pytest tests/test_extension_types/test_default_context_factories.py::test_default_context_has_dataclass_factory tests/test_extension_types/test_default_context_factories.py::test_default_context_has_pydantic_factory -v
```

Expected: both pass.

- [ ] **Step 5: Verify the existing context tests still pass**

```bash
uv run pytest test-objective/unit/test_contexts.py -v
```

Expected: all tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/contexts/data/v0.1.json tests/test_extension_types/test_default_context_factories.py
git commit -m "feat(contexts): wire DataclassLogicalTypeFactory and PydanticLogicalTypeFactory into v0.1 default context"
```

---

## Task 5: Add end-to-end integration tests via the default context

**Files:**
- Modify: `tests/test_extension_types/test_default_context_factories.py`

These tests prove that a user can define a dataclass or pydantic model and use it immediately as a pod field type via the default context — no manual factory registration.

- [ ] **Step 1: Add module-level pydantic model to the test file**

At the top of `tests/test_extension_types/test_default_context_factories.py`, after the existing module-level dataclass, add:

```python
from pydantic import BaseModel


class _SimpleModel(BaseModel):
    name: str
    score: float
```

- [ ] **Step 2: Add the auto-registration tests**

Append to `tests/test_extension_types/test_default_context_factories.py`:

```python
import pyarrow as pa
from orcapod.extension_types.database_hooks import apply_extension_types, register_discovered_extensions


def test_default_context_dataclass_auto_registered_on_use():
    """register_python_class on a dataclass works zero-setup via the default context."""
    converter = create_registry().get_context().type_converter
    arrow_type = converter.register_python_class(_SimplePoint)
    assert isinstance(arrow_type, pa.ExtensionType)
    fqcn = f"{_SimplePoint.__module__}.{_SimplePoint.__qualname__}"
    assert arrow_type.extension_name == fqcn


def test_default_context_pydantic_auto_registered_on_use():
    """register_python_class on a pydantic model works zero-setup via the default context."""
    converter = create_registry().get_context().type_converter
    arrow_type = converter.register_python_class(_SimpleModel)
    assert isinstance(arrow_type, pa.ExtensionType)
    fqcn = f"{_SimpleModel.__module__}.{_SimpleModel.__qualname__}"
    assert arrow_type.extension_name == fqcn
```

- [ ] **Step 3: Add the Parquet round-trip tests**

Append to `tests/test_extension_types/test_default_context_factories.py`:

```python
import pyarrow.parquet as pq


def test_default_context_dataclass_parquet_roundtrip(tmp_path):
    """Dataclass round-trips through Parquet with no manual factory registration."""
    # Write path — fresh context, no manual factory setup
    write_converter = create_registry().get_context().type_converter
    arrow_schema = write_converter.python_schema_to_arrow_schema({"point": _SimplePoint})
    rows = [{"point": _SimplePoint(x=3, y=7)}]
    table = write_converter.python_dicts_to_arrow_table(rows, arrow_schema=arrow_schema)

    parquet_path = tmp_path / "point.parquet"
    pq.write_table(table, parquet_path)

    # Read path — another fresh context, no manual factory setup
    read_converter = create_registry().get_context().type_converter
    read_table = pq.read_table(parquet_path)
    register_discovered_extensions(read_converter, read_table.schema)
    read_table = apply_extension_types(read_table, read_converter._logical_type_registry)

    rows_out = read_converter.arrow_table_to_python_dicts(read_table)
    assert len(rows_out) == 1
    result = rows_out[0]["point"]
    assert isinstance(result, _SimplePoint)
    assert result.x == 3
    assert result.y == 7


def test_default_context_pydantic_parquet_roundtrip(tmp_path):
    """Pydantic model round-trips through Parquet with no manual factory registration."""
    # Write path — fresh context, no manual factory setup
    write_converter = create_registry().get_context().type_converter
    arrow_schema = write_converter.python_schema_to_arrow_schema({"model": _SimpleModel})
    rows = [{"model": _SimpleModel(name="alice", score=9.5)}]
    table = write_converter.python_dicts_to_arrow_table(rows, arrow_schema=arrow_schema)

    parquet_path = tmp_path / "model.parquet"
    pq.write_table(table, parquet_path)

    # Read path — another fresh context, no manual factory setup
    read_converter = create_registry().get_context().type_converter
    read_table = pq.read_table(parquet_path)
    register_discovered_extensions(read_converter, read_table.schema)
    read_table = apply_extension_types(read_table, read_converter._logical_type_registry)

    rows_out = read_converter.arrow_table_to_python_dicts(read_table)
    assert len(rows_out) == 1
    result = rows_out[0]["model"]
    assert isinstance(result, _SimpleModel)
    assert result.name == "alice"
    assert result.score == 9.5
```

- [ ] **Step 4: Run all tests in the new test file**

```bash
uv run pytest tests/test_extension_types/test_default_context_factories.py -v
```

Expected: all tests pass.

- [ ] **Step 5: Run the full extension_types test suite to check for regressions**

```bash
uv run pytest tests/test_extension_types/ -v
```

Expected: all tests pass (the existing xfail on `test_list_of_nested_dataclass_parquet_roundtrip` still xfails as expected).

- [ ] **Step 6: Commit**

```bash
git add tests/test_extension_types/test_default_context_factories.py
git commit -m "test(registry): add default context factory registration and Parquet round-trip tests"
```

---

## Task 6: Final verification and PR

- [ ] **Step 1: Run the complete test suite**

```bash
uv run pytest tests/ test-objective/ -v
```

Expected: all tests pass. No new failures.

- [ ] **Step 2: Create the PR**

```bash
gh pr create \
  --base extension-type-system \
  --title "feat(registry): wire DataclassLogicalTypeFactory and PydanticLogicalTypeFactory into default context (PLT-1701)" \
  --body "$(cat <<'EOF'
## Summary

- Promotes pydantic to a required dependency (was optional extra)
- Adds `factories` parameter to `LogicalTypeRegistry.__init__` — accepts a list of dicts with `factory`, `category`, and `python_bases` keys; each entry is registered via `register_logical_type_factory` at construction time
- Drops `try/except ImportError` guard in `PydanticLogicalTypeFactory.supports_class` — pydantic is now always available
- Wires `DataclassLogicalTypeFactory` and `PydanticLogicalTypeFactory` into `v0.1.json` under `logical_type_registry._config.factories`; uses `{"_type": "..."}` object-specs for `python_bases` so `parse_objectspec` resolves them to actual type objects
- Adds integration tests verifying zero-setup dataclass/pydantic auto-registration and Parquet round-trips via the default context

## Test plan

- [ ] `uv run pytest tests/test_extension_types/ -v` — all pass
- [ ] `uv run pytest test-objective/unit/test_contexts.py -v` — all pass
- [ ] `uv run pytest tests/ test-objective/ -v` — full suite passes

Closes PLT-1701
EOF
)"
```

Expected: PR URL printed. Verify it targets `extension-type-system`, not `main`.
