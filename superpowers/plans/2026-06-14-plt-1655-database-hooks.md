# PLT-1655: Database Hooks Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the peek-schema → register → read pattern to both database read paths so that Arrow extension types found in stored schemas are automatically registered before any table data is returned.

**Architecture:** A stateless `ensure_extensions_registered(schema)` hook in `database_hooks.py` walks the schema using the existing `walk_schema` utility, then delegates each discovered type to `LogicalTypeRegistry.prepare_extension_type`. The registry owns all dispatch logic: it checks its own `_by_arrow_name` dict as a per-process cache (step 1), then parses JSON metadata, dispatches to a `LogicalTypeFactory` by category string, and calls `self.register()`. Two new protocols (`LogicalTypeFactory`) and two new methods on `LogicalTypeRegistry` (`register_logical_type_factory`, `prepare_extension_type`) complete the contract.

**Tech Stack:** Python 3.12+, PyArrow, Polars, `pytest`, `json` (stdlib)

---

## File Map

| File | Action | Responsibility |
|---|---|---|
| `src/orcapod/extension_types/protocols.py` | Modify | Add `LogicalTypeFactory` Protocol |
| `src/orcapod/extension_types/registry.py` | Modify | Add logging, `import json`, `LogicalTypeFactory` import, `_factories` dict, `register_logical_type_factory`, `prepare_extension_type`, move `default_logical_type_registry` singleton here |
| `src/orcapod/extension_types/__init__.py` | Modify | Import `default_logical_type_registry` from `.registry`, add `LogicalTypeFactory` and `ensure_extensions_registered` to exports |
| `src/orcapod/extension_types/database_hooks.py` | **Create** | `ensure_extensions_registered(schema)` stateless hook |
| `src/orcapod/databases/delta_lake_databases.py` | Modify | Add `ensure_extensions_registered` call in `_read_delta_table` |
| `src/orcapod/databases/connector_arrow_database.py` | Modify | Add `import logging`, `logger`, `ensure_extensions_registered` call in `_get_committed_table` |
| `tests/test_extension_types/test_protocols.py` | Modify | Add `LogicalTypeFactory` conformance tests |
| `tests/test_extension_types/test_registry.py` | Modify | Add `_make_stub_factory` helper + 9 tests for new registry methods |
| `tests/test_extension_types/test_database_hooks.py` | **Create** | 9 tests for `ensure_extensions_registered` |

---

## Task 1: `LogicalTypeFactory` Protocol + registry logging infrastructure

**Files:**
- Modify: `src/orcapod/extension_types/protocols.py`
- Modify: `src/orcapod/extension_types/registry.py` (lines 1–21: imports section)
- Test: `tests/test_extension_types/test_protocols.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_extension_types/test_protocols.py` — after the existing `_StubLogicalType` class:

```python
class _StubFactory:
    """Minimal conforming implementation of LogicalTypeFactory for use in tests."""

    def create_logical_type(self, arrow_extension_name, storage_type, metadata):
        return _StubLogicalType()


def test_logical_type_factory_protocol_is_importable():
    """LogicalTypeFactory can be imported from extension_types.protocols."""
    from orcapod.extension_types.protocols import LogicalTypeFactory
    assert LogicalTypeFactory is not None


def test_logical_type_factory_conforming_class_satisfies_protocol():
    """A conforming class is recognized as a LogicalTypeFactory instance."""
    from orcapod.extension_types.protocols import LogicalTypeFactory
    assert isinstance(_StubFactory(), LogicalTypeFactory)


def test_logical_type_factory_create_returns_logical_type():
    """A conforming factory returns a LogicalType from create_logical_type."""
    from orcapod.extension_types.protocols import LogicalTypeFactory, LogicalType
    factory: LogicalTypeFactory = _StubFactory()
    result = factory.create_logical_type(
        "test.ext", pa.large_utf8(), {"category": "Test"}
    )
    assert isinstance(result, LogicalType)
```

- [ ] **Step 2: Run tests to verify they fail**

```
uv run pytest tests/test_extension_types/test_protocols.py -v -k "factory"
```

Expected: FAIL — `ImportError: cannot import name 'LogicalTypeFactory' from 'orcapod.extension_types.protocols'`

- [ ] **Step 3: Add `LogicalTypeFactory` to protocols.py**

Open `src/orcapod/extension_types/protocols.py`. After the closing `...` of `LogicalType`, append:

```python
@runtime_checkable
class LogicalTypeFactory(Protocol):
    """Protocol for factories that auto-construct ``LogicalType`` instances from Arrow schema metadata.

    A ``LogicalTypeFactory`` constructs a ``LogicalType`` from the Arrow extension
    type name, its underlying storage type, and the full parsed JSON metadata dict.
    The dispatch key (``"category"`` value from the metadata JSON) that routes to this
    factory is declared at registration time via
    ``LogicalTypeRegistry.register_logical_type_factory``; the factory itself has no
    knowledge of its dispatch key but receives the full metadata dict so it can read
    additional hints beyond ``"category"``.

    This protocol is ``@runtime_checkable``, consistent with ``LogicalType``.
    """

    def create_logical_type(
        self,
        arrow_extension_name: str,
        storage_type: pa.DataType,
        metadata: dict,
    ) -> LogicalType:
        """Construct a ``LogicalType`` for the given Arrow extension name and storage type.

        Args:
            arrow_extension_name: The Arrow extension type name extracted from the
                schema (i.e. the value of ``ARROW:extension:name`` field metadata).
            storage_type: The underlying Arrow storage type for this extension field.
            metadata: The full parsed JSON metadata dict. Always contains at least a
                ``"category"`` key. May contain additional keys the factory uses (e.g.
                ``"protocol"``, ``"pydantic_version"``).

        Returns:
            A fully constructed ``LogicalType`` ready to be passed to
            ``LogicalTypeRegistry.register()``.

        Raises:
            ValueError: If this factory cannot construct a logical type for the given
                extension name (e.g. the Python class cannot be resolved by name).
        """
        ...
```

- [ ] **Step 4: Add logging infrastructure to registry.py**

Open `src/orcapod/extension_types/registry.py`. The current imports block starts at line 1:

```python
from __future__ import annotations

import re
from typing import TYPE_CHECKING

from orcapod.extension_types.protocols import LogicalType
```

Replace the imports block with:

```python
from __future__ import annotations

import json
import logging
import re
from typing import TYPE_CHECKING

from orcapod.extension_types.protocols import LogicalType, LogicalTypeFactory
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import polars as pl
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")
    pl = LazyModule("polars")

logger = logging.getLogger(__name__)
```

- [ ] **Step 5: Run tests to verify they pass**

```
uv run pytest tests/test_extension_types/test_protocols.py -v
```

Expected: all tests PASS (including the 3 new factory tests)

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/extension_types/protocols.py src/orcapod/extension_types/registry.py tests/test_extension_types/test_protocols.py
git commit -m "feat(extension_types): add LogicalTypeFactory protocol and registry logging setup"
```

---

## Task 2: Move `default_logical_type_registry` singleton to registry.py

**Context:** `database_hooks.py` (Task 5) will import `default_logical_type_registry` from `registry.py`. If it imported from `orcapod.extension_types` (the package `__init__.py`), a circular import would occur because `__init__.py` will later import from `database_hooks`. Moving the singleton to `registry.py` breaks the cycle.

**Files:**
- Modify: `src/orcapod/extension_types/registry.py` (add singleton at bottom)
- Modify: `src/orcapod/extension_types/__init__.py` (import instead of create)
- Test: `tests/test_extension_types/test_registry.py` (add one new import-path test)

- [ ] **Step 1: Write the new import-path test**

Add to the bottom of `tests/test_extension_types/test_registry.py` (after the existing `default_logical_type_registry` tests):

```python
def test_default_registry_accessible_from_registry_module():
    """default_logical_type_registry imported from registry module is same object as from package."""
    from orcapod.extension_types.registry import default_logical_type_registry as from_registry
    from orcapod.extension_types import default_logical_type_registry as from_package
    assert from_registry is from_package
```

- [ ] **Step 2: Run test to verify it fails**

```
uv run pytest tests/test_extension_types/test_registry.py::test_default_registry_accessible_from_registry_module -v
```

Expected: FAIL — `ImportError: cannot import name 'default_logical_type_registry' from 'orcapod.extension_types.registry'`

- [ ] **Step 3: Add singleton to the bottom of registry.py**

Open `src/orcapod/extension_types/registry.py`. Append after the `LogicalTypeRegistry` class:

```python
# Module-level singleton — per-process registry used by database_hooks and
# application code. Defined here (not in __init__.py) to avoid the circular
# import that would arise if database_hooks imported from the package __init__.
default_logical_type_registry = LogicalTypeRegistry()
```

- [ ] **Step 4: Update __init__.py to import singleton from registry**

Open `src/orcapod/extension_types/__init__.py`. The current content is:

```python
"""Arrow/Polars extension type system for orcapod.
...
"""

from __future__ import annotations

from .protocols import LogicalType
from .registry import LogicalTypeRegistry, make_arrow_extension_type
from .schema_walker import ExtensionTypeInfo, walk_field, walk_schema

default_logical_type_registry = LogicalTypeRegistry()

__all__ = [
    "LogicalType",
    "LogicalTypeRegistry",
    "make_arrow_extension_type",
    "default_logical_type_registry",
    # PLT-1654
    "ExtensionTypeInfo",
    "walk_schema",
    "walk_field",
]
```

Replace with:

```python
"""Arrow/Polars extension type system for orcapod.

This subpackage provides the registry and protocol for logical types that map
between Python objects and their Arrow/Polars extension type representation.

The module-level ``default_logical_type_registry`` instance is the process default.
Built-in registrations (``Path``, ``UPath``, ``UUID``) are added by PLT-1656.
``DataContext`` wiring is added by PLT-1660.
"""

from __future__ import annotations

from .protocols import LogicalType, LogicalTypeFactory
from .registry import LogicalTypeRegistry, make_arrow_extension_type, default_logical_type_registry
from .schema_walker import ExtensionTypeInfo, walk_field, walk_schema

__all__ = [
    "LogicalType",
    "LogicalTypeFactory",
    "LogicalTypeRegistry",
    "make_arrow_extension_type",
    "default_logical_type_registry",
    # PLT-1654
    "ExtensionTypeInfo",
    "walk_schema",
    "walk_field",
]
```

- [ ] **Step 5: Run all extension_types tests to verify no regressions**

```
uv run pytest tests/test_extension_types/ -v
```

Expected: all existing tests PASS including the new `test_default_registry_accessible_from_registry_module`

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/extension_types/registry.py src/orcapod/extension_types/__init__.py tests/test_extension_types/test_registry.py
git commit -m "refactor(extension_types): move default_logical_type_registry singleton to registry.py"
```

---

## Task 3: `_factories` dict + `register_logical_type_factory` method

**Files:**
- Modify: `src/orcapod/extension_types/registry.py`
- Test: `tests/test_extension_types/test_registry.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_extension_types/test_registry.py` — after the `_make_stub` helper:

```python
def _make_stub_factory(return_lt: LogicalType | None = None) -> LogicalTypeFactory:
    """Factory for minimal LogicalTypeFactory conforming stubs.

    If ``return_lt`` is given, ``create_logical_type`` returns it; otherwise
    it creates a fresh stub using ``_make_stub`` keyed on the arrow name.
    ``calls`` records every invocation as ``(arrow_extension_name, storage_type, metadata)``.
    """
    from orcapod.extension_types.protocols import LogicalTypeFactory
    _return_lt = return_lt

    class _Factory:
        def __init__(self):
            self.calls: list[tuple] = []

        def create_logical_type(self, arrow_extension_name, storage_type, metadata):
            self.calls.append((arrow_extension_name, storage_type, metadata))
            if _return_lt is not None:
                return _return_lt
            return _make_stub(arrow_name=arrow_extension_name, storage=storage_type)

    return _Factory()
```

Then add these tests (before the `# end-to-end integration tests` section):

```python
# ---------------------------------------------------------------------------
# register_logical_type_factory tests
# ---------------------------------------------------------------------------

def test_register_logical_type_factory_no_error():
    """register_logical_type_factory completes without raising."""
    registry = LogicalTypeRegistry()
    factory = _make_stub_factory()
    registry.register_logical_type_factory("TestCat", factory)  # should not raise


def test_register_logical_type_factory_same_instance_idempotent():
    """Re-registering the same factory instance for the same category does not raise."""
    registry = LogicalTypeRegistry()
    factory = _make_stub_factory()
    registry.register_logical_type_factory("Cat", factory)
    registry.register_logical_type_factory("Cat", factory)  # should not raise


def test_register_duplicate_category_raises():
    """Registering a different factory for an already-registered category raises ValueError."""
    registry = LogicalTypeRegistry()
    f1 = _make_stub_factory()
    f2 = _make_stub_factory()
    registry.register_logical_type_factory("Cat", f1)
    with pytest.raises(ValueError, match="Cat"):
        registry.register_logical_type_factory("Cat", f2)
```

- [ ] **Step 2: Run tests to verify they fail**

```
uv run pytest tests/test_extension_types/test_registry.py -v -k "factory" --no-header 2>&1 | tail -20
```

Expected: FAIL — `AttributeError: 'LogicalTypeRegistry' object has no attribute 'register_logical_type_factory'`

- [ ] **Step 3: Add `_factories` dict and `register_logical_type_factory` to LogicalTypeRegistry**

In `src/orcapod/extension_types/registry.py`, inside the `LogicalTypeRegistry` class, update `__init__`:

```python
    def __init__(self) -> None:
        self._by_logical_name: dict[str, LogicalType] = {}
        self._by_arrow_name: dict[str, LogicalType] = {}
        self._by_python_type: dict[type, LogicalType] = {}
        self._factories: dict[str, LogicalTypeFactory] = {}
```

Then add the new method after `get_by_arrow_extension_name`:

```python
    def register_logical_type_factory(
        self,
        category: str,
        factory: LogicalTypeFactory,
    ) -> None:
        """Register a factory for the given metadata category string.

        When ``prepare_extension_type`` encounters an Arrow extension type whose
        ``extension_metadata`` JSON contains ``{"category": "<category>", ...}``,
        it calls ``factory.create_logical_type(arrow_extension_name, storage_type,
        metadata_dict)`` to construct the logical type and then registers it.

        Args:
            category: The ``"category"`` value from the extension metadata JSON that
                identifies this category (e.g. ``"Dataclass"``).
            factory: A ``LogicalTypeFactory`` instance responsible for constructing
                logical types for this category.

        Raises:
            ValueError: If ``category`` is already registered to a different factory.
        """
        existing = self._factories.get(category)
        if existing is not None and existing is not factory:
            raise ValueError(
                f"Cannot register factory for category {category!r}: "
                f"a different factory is already registered for this category."
            )
        if existing is factory:
            return
        self._factories[category] = factory
        logger.debug(
            "registered LogicalTypeFactory for category %r: %r", category, factory
        )
```

- [ ] **Step 4: Run tests to verify they pass**

```
uv run pytest tests/test_extension_types/test_registry.py -v -k "factory" --no-header
```

Expected: the 3 new factory tests PASS

- [ ] **Step 5: Run full extension_types test suite to check for regressions**

```
uv run pytest tests/test_extension_types/ -v --no-header 2>&1 | tail -10
```

Expected: all tests PASS

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/extension_types/registry.py tests/test_extension_types/test_registry.py
git commit -m "feat(extension_types): add _factories dict and register_logical_type_factory to LogicalTypeRegistry"
```

---

## Task 4: `prepare_extension_type` — full implementation (all 7 steps)

**Files:**
- Modify: `src/orcapod/extension_types/registry.py`
- Test: `tests/test_extension_types/test_registry.py`

- [ ] **Step 1: Write ALL failing tests (happy path + error paths)**

Add to `tests/test_extension_types/test_registry.py`, after the `register_logical_type_factory` tests. Note the `import json` needed at the top of the file:

First add `import json` to the existing import block at the top of test_registry.py (after `import uuid`).

Then add these tests:

```python
# ---------------------------------------------------------------------------
# prepare_extension_type tests
# ---------------------------------------------------------------------------

def test_register_logical_type_factory_dispatches_on_prepare():
    """prepare_extension_type dispatches to the registered factory and registers the result."""
    import json
    registry = LogicalTypeRegistry()
    factory = _make_stub_factory()
    registry.register_logical_type_factory("TestCat", factory)

    arrow_name = _unique_name()
    metadata_bytes = json.dumps({"category": "TestCat"}).encode()
    registry.prepare_extension_type(arrow_name, metadata_bytes, pa.large_utf8())

    assert len(factory.calls) == 1
    assert factory.calls[0][0] == arrow_name
    assert registry.get_by_arrow_extension_name(arrow_name) is not None


def test_factory_receives_full_metadata_dict():
    """The factory's create_logical_type receives the full parsed JSON dict, not just category."""
    import json
    registry = LogicalTypeRegistry()
    factory = _make_stub_factory()
    registry.register_logical_type_factory("TestCat", factory)

    arrow_name = _unique_name()
    metadata_bytes = json.dumps(
        {"category": "TestCat", "protocol": 5, "version": "1.0"}
    ).encode()
    registry.prepare_extension_type(arrow_name, metadata_bytes, pa.large_utf8())

    assert len(factory.calls) == 1
    _, _, received_metadata = factory.calls[0]
    assert received_metadata == {"category": "TestCat", "protocol": 5, "version": "1.0"}


def test_prepare_already_registered_noop():
    """prepare_extension_type called twice does not raise and does not call the factory again."""
    import json
    registry = LogicalTypeRegistry()
    factory = _make_stub_factory()
    registry.register_logical_type_factory("TestCat", factory)

    arrow_name = _unique_name()
    metadata_bytes = json.dumps({"category": "TestCat"}).encode()

    registry.prepare_extension_type(arrow_name, metadata_bytes, pa.large_utf8())
    registry.prepare_extension_type(arrow_name, metadata_bytes, pa.large_utf8())  # second call

    assert len(factory.calls) == 1  # factory called exactly once


def test_prepare_already_registered_none_metadata_noop():
    """Type pre-registered via register(); None metadata on prepare call is a silent no-op."""
    registry = LogicalTypeRegistry()
    lt = _make_stub()
    registry.register(lt)

    arrow_name = lt.get_arrow_extension_type().extension_name
    registry.prepare_extension_type(arrow_name, None, pa.large_utf8())  # should not raise


def test_prepare_none_metadata_not_registered_raises():
    """None metadata for an unregistered extension type raises ValueError."""
    registry = LogicalTypeRegistry()
    arrow_name = _unique_name()

    with pytest.raises(ValueError, match="must be pre-registered explicitly"):
        registry.prepare_extension_type(arrow_name, None, pa.large_utf8())


def test_prepare_invalid_json_raises():
    """Non-UTF-8-JSON extension_metadata raises ValueError with raw bytes and parse error."""
    registry = LogicalTypeRegistry()
    arrow_name = _unique_name()
    bad_metadata = b"not-json!"

    with pytest.raises(ValueError, match="not valid UTF-8 JSON"):
        registry.prepare_extension_type(arrow_name, bad_metadata, pa.large_utf8())


def test_prepare_json_missing_category_raises():
    """Valid JSON metadata without a 'category' key raises ValueError."""
    import json
    registry = LogicalTypeRegistry()
    arrow_name = _unique_name()
    no_category = json.dumps({"version": 1}).encode()

    with pytest.raises(ValueError, match='"category"'):
        registry.prepare_extension_type(arrow_name, no_category, pa.large_utf8())


def test_prepare_unknown_category_raises():
    """Valid JSON with 'category' but no matching factory raises ValueError."""
    import json
    registry = LogicalTypeRegistry()
    arrow_name = _unique_name()
    unknown = json.dumps({"category": "NoSuchFactory"}).encode()

    with pytest.raises(ValueError, match="NoSuchFactory"):
        registry.prepare_extension_type(arrow_name, unknown, pa.large_utf8())
```

- [ ] **Step 2: Run tests to verify they fail**

```
uv run pytest tests/test_extension_types/test_registry.py -v -k "prepare" --no-header 2>&1 | tail -20
```

Expected: FAIL — `AttributeError: 'LogicalTypeRegistry' object has no attribute 'prepare_extension_type'`

- [ ] **Step 3: Implement `prepare_extension_type` in registry.py**

Add this method to `LogicalTypeRegistry` (after `register_logical_type_factory`):

```python
    def prepare_extension_type(
        self,
        arrow_extension_name: str,
        extension_metadata: bytes | None,
        storage_type: pa.DataType,
    ) -> None:
        """Ensure the Arrow extension type identified by ``arrow_extension_name``
        is registered as a ``LogicalType``.

        This is the single entry point called by ``ensure_extensions_registered``
        in ``database_hooks``. The registry owns all dispatch logic.

        Args:
            arrow_extension_name: Arrow extension type name (``ARROW:extension:name``).
            extension_metadata: Raw metadata bytes (``ARROW:extension:metadata``),
                expected to be UTF-8 JSON containing at least a ``"category"`` key.
                ``None`` if absent.
            storage_type: Underlying Arrow storage type for this extension field.

        Raises:
            ValueError: If ``extension_metadata`` is ``None`` and the type is not
                already registered.
            ValueError: If ``extension_metadata`` is not valid UTF-8 JSON.
            ValueError: If the parsed JSON has no ``"category"`` key.
            ValueError: If no factory is registered for the ``"category"`` value.
            ValueError: Propagated from the factory if it cannot construct a type.
        """
        # Step 1: per-process cache hit — no-op regardless of metadata content.
        if self.get_by_arrow_extension_name(arrow_extension_name) is not None:
            logger.debug(
                "prepare_extension_type: %r already registered, skipping",
                arrow_extension_name,
            )
            return

        # Step 2: None metadata — cannot auto-register; must be pre-registered.
        if extension_metadata is None:
            raise ValueError(
                f"Extension type {arrow_extension_name!r} has no extension metadata "
                f"(metadata is None).\n"
                f"Types without a metadata category tag cannot be auto-registered via "
                f"a factory — they must be pre-registered explicitly via "
                f"default_logical_type_registry.register(logical_type)."
            )

        # Step 3: Parse JSON.
        try:
            metadata_dict = json.loads(extension_metadata.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError(
                f"Extension type {arrow_extension_name!r} has extension metadata that "
                f"is not valid UTF-8 JSON: {extension_metadata!r}. "
                f"Parse error: {exc}.\n"
                f'Extension metadata must be a JSON object with at least a "category" '
                f'key, e.g. {{"category": "Dataclass"}}.'
            ) from exc

        # Step 4: Require "category" key.
        if "category" not in metadata_dict:
            raise ValueError(
                f"Extension type {arrow_extension_name!r} has extension metadata JSON "
                f'with no "category" key: {metadata_dict}. Extension metadata must be '
                f'a JSON object with at least a "category" key, e.g. '
                f'{{"category": "Dataclass"}}.'
            )

        category = metadata_dict["category"]

        # Step 5: Look up factory.
        factory = self._factories.get(category)
        if factory is None:
            raise ValueError(
                f"No LogicalTypeFactory is registered for category {category!r}.\n"
                f"Cannot prepare extension type {arrow_extension_name!r} for "
                f"registration.\n"
                f"Register a factory via "
                f"default_logical_type_registry.register_logical_type_factory(\n"
                f"    {category!r}, factory\n"
                f")."
            )

        # Step 6: Construct logical type via factory.
        logger.debug(
            "prepare_extension_type: %r not registered — dispatching to category %r factory",
            arrow_extension_name,
            category,
        )
        logical_type = factory.create_logical_type(
            arrow_extension_name, storage_type, metadata_dict
        )

        # Step 7: Register in all three bindings + PA/Polars global registries.
        self.register(logical_type)
        logger.debug(
            "prepare_extension_type: successfully registered %r via %r factory",
            arrow_extension_name,
            category,
        )
```

- [ ] **Step 4: Run tests to verify they all pass**

```
uv run pytest tests/test_extension_types/test_registry.py -v --no-header 2>&1 | tail -15
```

Expected: all tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/extension_types/registry.py tests/test_extension_types/test_registry.py
git commit -m "feat(extension_types): add prepare_extension_type to LogicalTypeRegistry"
```

---

## Task 5: `database_hooks.py` module + `__init__.py` exports + test suite

**Files:**
- Create: `src/orcapod/extension_types/database_hooks.py`
- Modify: `src/orcapod/extension_types/__init__.py`
- Create: `tests/test_extension_types/test_database_hooks.py`

- [ ] **Step 1: Write the failing test file**

Create `tests/test_extension_types/test_database_hooks.py`:

```python
"""Tests for ensure_extensions_registered in database_hooks."""

from __future__ import annotations

import json
import uuid

import pyarrow as pa
import pytest

from orcapod.extension_types.registry import LogicalTypeRegistry, make_arrow_extension_type


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _unique_name() -> str:
    """Unique Arrow extension name to avoid cross-test global-registry collisions."""
    return f"test.hooks.{uuid.uuid4().hex[:8]}"


def _make_ext_schema(
    arrow_name: str,
    metadata: bytes | None = None,
    storage: pa.DataType | None = None,
) -> pa.Schema:
    """Build a ``pa.Schema`` with one extension-typed field using ``make_arrow_extension_type``.

    Only call this when you have control over the metadata content — the resulting
    field's type is an in-memory ``pa.ExtensionType`` instance, not raw field metadata.
    """
    _storage = storage or pa.large_utf8()
    ext_cls = make_arrow_extension_type(arrow_name, _storage, metadata=metadata)
    return pa.schema([pa.field("col", ext_cls())])


def _make_field_metadata_schema(
    arrow_name: str,
    metadata: bytes,
    storage: pa.DataType | None = None,
) -> pa.Schema:
    """Build a schema where the extension is described by raw Arrow field metadata.

    This simulates a Parquet/IPC read where the extension type was not registered
    in the current process, so ``field.type`` is a plain Arrow storage type rather
    than a ``pa.ExtensionType`` instance.
    """
    _storage = storage or pa.large_utf8()
    field = pa.field("col", _storage).with_metadata({
        b"ARROW:extension:name": arrow_name.encode(),
        b"ARROW:extension:metadata": metadata,
    })
    return pa.schema([field])


def _make_stub_factory(registry: LogicalTypeRegistry):
    """Return a minimal LogicalTypeFactory stub whose calls are recorded.

    The factory auto-creates a fresh ``LogicalType`` stub keyed by arrow name.
    Registering this factory in *registry* causes it to also register a Polars
    extension type, which requires the Arrow ext type to be in PyArrow's global
    registry.  To avoid cross-test collisions, each test uses a unique arrow name.
    """
    class _Factory:
        def __init__(self):
            self.calls: list[tuple] = []

        def create_logical_type(self, arrow_extension_name, storage_type, metadata):
            import polars as pl
            from orcapod.extension_types.registry import make_arrow_extension_type

            self.calls.append((arrow_extension_name, storage_type, metadata))

            _name = arrow_extension_name
            _arrow_cls = make_arrow_extension_type(_name, storage_type)
            _pl_storage = pl.from_arrow(pa.array([], type=storage_type)).dtype

            class _PolarsExt(pl.BaseExtension):
                def __init__(self):
                    super().__init__(_name, _pl_storage, None)
                @classmethod
                def ext_from_params(cls, ext_name, storage_dtype, metadata_str):
                    return cls()

            class _StubLT:
                @property
                def logical_type_name(self):
                    return _name
                @property
                def python_type(self):
                    return str
                def get_arrow_extension_type(self):
                    return _arrow_cls()
                def get_polars_extension_type(self):
                    return _PolarsExt()
                def python_to_storage(self, value):
                    return str(value)
                def storage_to_python(self, storage_value):
                    return storage_value

            return _StubLT()

    return _Factory()


# ---------------------------------------------------------------------------
# Fixture
# ---------------------------------------------------------------------------

@pytest.fixture
def fresh_registry(monkeypatch):
    """A fresh LogicalTypeRegistry monkeypatched into database_hooks module."""
    import orcapod.extension_types.database_hooks as hooks
    registry = LogicalTypeRegistry()
    monkeypatch.setattr(hooks, "default_logical_type_registry", registry)
    return registry


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_no_extension_types_is_noop(fresh_registry):
    """Schema with only primitives — ensure_extensions_registered returns without touching registry."""
    from orcapod.extension_types.database_hooks import ensure_extensions_registered

    schema = pa.schema([
        pa.field("id", pa.int64()),
        pa.field("name", pa.large_utf8()),
    ])
    ensure_extensions_registered(schema)
    # fresh_registry is empty — no error means no spurious lookup was triggered
    assert fresh_registry.get_by_arrow_extension_name("anything") is None


def test_known_type_is_registered(fresh_registry):
    """Schema with one extension type whose factory is registered — type is registered after call."""
    from orcapod.extension_types.database_hooks import ensure_extensions_registered

    arrow_name = _unique_name()
    factory = _make_stub_factory(fresh_registry)
    fresh_registry.register_logical_type_factory("TestCat", factory)

    metadata_bytes = json.dumps({"category": "TestCat"}).encode()
    schema = _make_ext_schema(arrow_name, metadata=metadata_bytes)

    ensure_extensions_registered(schema)

    assert fresh_registry.get_by_arrow_extension_name(arrow_name) is not None
    assert len(factory.calls) == 1


def test_already_registered_is_skipped(fresh_registry):
    """Calling ensure_extensions_registered twice does not raise and factory is called once."""
    from orcapod.extension_types.database_hooks import ensure_extensions_registered

    arrow_name = _unique_name()
    factory = _make_stub_factory(fresh_registry)
    fresh_registry.register_logical_type_factory("TestCat", factory)

    metadata_bytes = json.dumps({"category": "TestCat"}).encode()
    schema = _make_ext_schema(arrow_name, metadata=metadata_bytes)

    ensure_extensions_registered(schema)
    ensure_extensions_registered(schema)  # second call

    assert len(factory.calls) == 1  # factory invoked exactly once


def test_none_metadata_already_registered_noop(fresh_registry):
    """Extension type with None metadata that IS already in the registry — silent no-op."""
    from orcapod.extension_types.database_hooks import ensure_extensions_registered

    arrow_name = _unique_name()
    factory = _make_stub_factory(fresh_registry)
    fresh_registry.register_logical_type_factory("TestCat", factory)

    # First: register via metadata so it ends up in the registry.
    metadata_bytes = json.dumps({"category": "TestCat"}).encode()
    schema_with_meta = _make_ext_schema(arrow_name, metadata=metadata_bytes)
    ensure_extensions_registered(schema_with_meta)

    # Now: same arrow name but with no metadata (simulates reading the schema without
    # metadata — e.g. after an IPC round-trip where the type is now registered in-process).
    schema_no_meta = _make_ext_schema(arrow_name, metadata=None)  # metadata=None → b""
    ensure_extensions_registered(schema_no_meta)  # should NOT raise


def test_none_metadata_not_registered_raises(fresh_registry):
    """Unregistered extension type with None metadata raises ValueError."""
    from orcapod.extension_types.database_hooks import ensure_extensions_registered

    arrow_name = _unique_name()
    schema = _make_ext_schema(arrow_name, metadata=None)  # metadata=None → b"" → walker normalizes to None

    with pytest.raises(ValueError, match="must be pre-registered explicitly"):
        ensure_extensions_registered(schema)


def test_metadata_not_json_raises(fresh_registry):
    """Unregistered extension type with non-JSON metadata bytes raises ValueError."""
    from orcapod.extension_types.database_hooks import ensure_extensions_registered

    arrow_name = _unique_name()
    schema = _make_field_metadata_schema(arrow_name, metadata=b"not-json!")

    with pytest.raises(ValueError, match="not valid UTF-8 JSON"):
        ensure_extensions_registered(schema)


def test_metadata_json_missing_category_raises(fresh_registry):
    """Unregistered extension type with valid JSON but no 'category' key raises ValueError."""
    from orcapod.extension_types.database_hooks import ensure_extensions_registered

    arrow_name = _unique_name()
    schema = _make_field_metadata_schema(
        arrow_name, metadata=json.dumps({"version": 1}).encode()
    )

    with pytest.raises(ValueError, match='"category"'):
        ensure_extensions_registered(schema)


def test_unknown_metadata_raises(fresh_registry):
    """Unregistered extension type with valid JSON and 'category' but no matching factory raises ValueError."""
    from orcapod.extension_types.database_hooks import ensure_extensions_registered

    arrow_name = _unique_name()
    schema = _make_field_metadata_schema(
        arrow_name, metadata=json.dumps({"category": "NoSuchFactory"}).encode()
    )

    with pytest.raises(ValueError, match="NoSuchFactory"):
        ensure_extensions_registered(schema)


def test_nested_extension_type(fresh_registry):
    """Extension type inside a struct column is discovered and registered."""
    from orcapod.extension_types.database_hooks import ensure_extensions_registered

    arrow_name = _unique_name()
    factory = _make_stub_factory(fresh_registry)
    fresh_registry.register_logical_type_factory("TestCat", factory)

    metadata_bytes = json.dumps({"category": "TestCat"}).encode()
    inner_ext_cls = make_arrow_extension_type(arrow_name, pa.large_utf8(), metadata=metadata_bytes)

    struct_type = pa.struct([pa.field("inner", inner_ext_cls())])
    schema = pa.schema([pa.field("outer", struct_type)])

    ensure_extensions_registered(schema)

    assert fresh_registry.get_by_arrow_extension_name(arrow_name) is not None
    assert len(factory.calls) == 1
```

- [ ] **Step 2: Run tests to verify they fail**

```
uv run pytest tests/test_extension_types/test_database_hooks.py -v --no-header 2>&1 | tail -15
```

Expected: FAIL — `ModuleNotFoundError: No module named 'orcapod.extension_types.database_hooks'`

- [ ] **Step 3: Create `database_hooks.py`**

Create `src/orcapod/extension_types/database_hooks.py`:

```python
"""Peek-schema hook for extension type auto-registration at database read time.

Call ``ensure_extensions_registered(schema)`` before returning any Arrow table
from a database read path. It is a no-op when the schema contains no extension
types.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from orcapod.extension_types.registry import default_logical_type_registry
from orcapod.extension_types.schema_walker import walk_schema

if TYPE_CHECKING:
    import pyarrow as pa

logger = logging.getLogger(__name__)


def ensure_extensions_registered(schema: pa.Schema) -> None:
    """Register any extension types found in ``schema`` that are not yet known.

    Walks ``schema`` recursively to discover all Arrow extension types at any
    nesting depth. For each discovered type, delegates to
    ``default_logical_type_registry.prepare_extension_type``.

    Already-registered types are detected and skipped inside the registry —
    this function itself is stateless.

    Args:
        schema: The Arrow schema to inspect. May contain no extension types,
            in which case this call is a no-op.

    Raises:
        ValueError: Propagated from the registry if an extension type's metadata
            has no registered factory or is malformed.
    """
    found = walk_schema(schema)
    if not found:
        logger.debug("ensure_extensions_registered: no extension types in schema")
        return
    logger.debug(
        "ensure_extensions_registered: found %d extension type(s) in schema: %s",
        len(found),
        [info.extension_name for info in found],
    )
    for info in found:
        default_logical_type_registry.prepare_extension_type(
            info.extension_name,
            info.extension_metadata,
            info.storage_type,
        )
```

- [ ] **Step 4: Add `ensure_extensions_registered` to `__init__.py` exports**

In `src/orcapod/extension_types/__init__.py`, add the import and export:

```python
from .database_hooks import ensure_extensions_registered
```

Add `"ensure_extensions_registered"` to `__all__`.

The final `__init__.py` should look like:

```python
"""Arrow/Polars extension type system for orcapod.

This subpackage provides the registry and protocol for logical types that map
between Python objects and their Arrow/Polars extension type representation.

The module-level ``default_logical_type_registry`` instance is the process default.
Built-in registrations (``Path``, ``UPath``, ``UUID``) are added by PLT-1656.
``DataContext`` wiring is added by PLT-1660.
"""

from __future__ import annotations

from .protocols import LogicalType, LogicalTypeFactory
from .registry import LogicalTypeRegistry, make_arrow_extension_type, default_logical_type_registry
from .schema_walker import ExtensionTypeInfo, walk_field, walk_schema
from .database_hooks import ensure_extensions_registered

__all__ = [
    "LogicalType",
    "LogicalTypeFactory",
    "LogicalTypeRegistry",
    "make_arrow_extension_type",
    "default_logical_type_registry",
    # PLT-1654
    "ExtensionTypeInfo",
    "walk_schema",
    "walk_field",
    # PLT-1655
    "ensure_extensions_registered",
]
```

- [ ] **Step 5: Run all tests to verify they pass**

```
uv run pytest tests/test_extension_types/ -v --no-header 2>&1 | tail -20
```

Expected: all tests PASS

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/extension_types/database_hooks.py src/orcapod/extension_types/__init__.py tests/test_extension_types/test_database_hooks.py
git commit -m "feat(extension_types): add database_hooks.ensure_extensions_registered and update exports"
```

---

## Task 6: Hook `DeltaTableDatabase._read_delta_table`

**Context:** `delta_lake_databases.py` already has `import logging` and `logger = logging.getLogger(__name__)`. Only a new import and a single hook call are needed.

**Files:**
- Modify: `src/orcapod/databases/delta_lake_databases.py`

- [ ] **Step 1: Add the import for `ensure_extensions_registered`**

In `src/orcapod/databases/delta_lake_databases.py`, find the existing imports block. The file starts with:

```python
from __future__ import annotations

import logging
from collections import defaultdict
from collections.abc import Collection, Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, cast

from orcapod.databases.utils import coerce_record_id
from orcapod.databases.storage_utils import is_cloud_uri, parse_base_path
from orcapod.utils import arrow_utils
from orcapod.utils.lazy_module import LazyModule
```

Add the new import after the existing `orcapod` imports:

```python
from orcapod.extension_types.database_hooks import ensure_extensions_registered
```

- [ ] **Step 2: Add the hook call in `_read_delta_table`**

Find `_read_delta_table` (around line 818). The current code after the method docstring is:

```python
        filter_expr = None
        # Use to_pyarrow_dataset with as_large_types for Polars compatible arrow table loading
        dataset = delta_table.to_pyarrow_dataset(as_large_types=True)
        if filters and expression is None:
```

Replace with (adding 2 lines after the dataset assignment):

```python
        filter_expr = None
        # Use to_pyarrow_dataset with as_large_types for Polars compatible arrow table loading
        dataset = delta_table.to_pyarrow_dataset(as_large_types=True)
        logger.debug("_read_delta_table: peeking schema for extension type registration")
        ensure_extensions_registered(delta_table.schema().to_arrow())
        if filters and expression is None:
```

- [ ] **Step 3: Run the full test suite**

```
uv run pytest tests/ -v --no-header -q 2>&1 | tail -20
```

Expected: all tests PASS

- [ ] **Step 4: Commit**

```bash
git add src/orcapod/databases/delta_lake_databases.py
git commit -m "feat(databases): call ensure_extensions_registered in DeltaTableDatabase._read_delta_table"
```

---

## Task 7: Hook `ConnectorArrowDatabase._get_committed_table`

**Context:** `connector_arrow_database.py` currently has no `logger`. Add it alongside the hook import.

**Files:**
- Modify: `src/orcapod/databases/connector_arrow_database.py`

- [ ] **Step 1: Add `import logging`, `logger`, and hook import**

In `src/orcapod/databases/connector_arrow_database.py`, the current imports block begins:

```python
from __future__ import annotations

import re
from collections import defaultdict
from collections.abc import Collection, Mapping
from typing import TYPE_CHECKING, Any, cast

from orcapod.databases.utils import coerce_record_id
from orcapod.protocols.db_connector_protocol import ColumnInfo, DBConnectorProtocol
from orcapod.utils.lazy_module import LazyModule
```

Replace with:

```python
from __future__ import annotations

import logging
import re
from collections import defaultdict
from collections.abc import Collection, Mapping
from typing import TYPE_CHECKING, Any, cast

from orcapod.databases.utils import coerce_record_id
from orcapod.extension_types.database_hooks import ensure_extensions_registered
from orcapod.protocols.db_connector_protocol import ColumnInfo, DBConnectorProtocol
from orcapod.utils.lazy_module import LazyModule

logger = logging.getLogger(__name__)
```

- [ ] **Step 2: Add the hook call in `_get_committed_table`**

Find `_get_committed_table` (around line 176). The current implementation is:

```python
    def _get_committed_table(
        self, record_path: tuple[str, ...]
    ) -> pa.Table | None:
        """Fetch all committed records for a path from the connector."""
        table_name = self._path_to_table_name(self._path_prefix + record_path)
        if table_name not in self._connector.get_table_names():
            return None
        batches = list(
            self._connector.iter_batches(f'SELECT * FROM "{table_name}"')
        )
        if not batches:
            return None
        return pa.Table.from_batches(batches)
```

Replace with:

```python
    def _get_committed_table(
        self, record_path: tuple[str, ...]
    ) -> pa.Table | None:
        """Fetch all committed records for a path from the connector."""
        table_name = self._path_to_table_name(self._path_prefix + record_path)
        if table_name not in self._connector.get_table_names():
            return None
        batches = list(
            self._connector.iter_batches(f'SELECT * FROM "{table_name}"')
        )
        if not batches:
            return None
        logger.debug("_get_committed_table: peeking schema for extension type registration")
        ensure_extensions_registered(batches[0].schema)
        return pa.Table.from_batches(batches)
```

- [ ] **Step 3: Run the full test suite**

```
uv run pytest tests/ -v --no-header -q 2>&1 | tail -20
```

Expected: all tests PASS

- [ ] **Step 4: Commit**

```bash
git add src/orcapod/databases/connector_arrow_database.py
git commit -m "feat(databases): add logger and ensure_extensions_registered hook to ConnectorArrowDatabase._get_committed_table"
```

---

## Final Verification

- [ ] **Run the complete test suite one final time**

```
uv run pytest tests/ -q --no-header 2>&1 | tail -5
```

Expected: all tests PASS, no warnings about new code

- [ ] **Create PR targeting `extension-type-system` branch**

```bash
gh pr create \
  --base extension-type-system \
  --title "feat(PLT-1655): add peek-schema → register → read pattern with per-process cache" \
  --body "$(cat <<'EOF'
## Summary

* Adds `LogicalTypeFactory` Protocol — a pure factory that constructs a `LogicalType` from an Arrow extension name, storage type, and full parsed JSON metadata dict.
* Adds `register_logical_type_factory(category, factory)` and `prepare_extension_type(arrow_extension_name, metadata, storage_type)` to `LogicalTypeRegistry`. The registry's `_by_arrow_name` dict acts as the per-process cache (step 1: already-registered → immediate no-op regardless of metadata).
* Adds stateless `ensure_extensions_registered(schema)` in `extension_types/database_hooks.py`. Walks the schema, delegates each extension type to `prepare_extension_type`.
* Wires the hook into `DeltaTableDatabase._read_delta_table` (schema peek via `DeltaTable.schema().to_arrow()`) and `ConnectorArrowDatabase._get_committed_table` (schema peek via `batches[0].schema`).
* Moves `default_logical_type_registry` singleton from `__init__.py` to `registry.py` to break the circular import that would arise with `database_hooks`.
* Sufficient DEBUG-level logging throughout: discovery, cache hit, factory dispatch, successful registration.

## Test plan

- [ ] `uv run pytest tests/test_extension_types/ -v` — all new unit tests pass
- [ ] `uv run pytest tests/ -q` — no regressions

Fixes PLT-1655
EOF
)"
```
