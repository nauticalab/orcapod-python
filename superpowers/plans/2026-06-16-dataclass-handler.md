# Dataclass Category Handler Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement `DataclassHandlerFactory` — a `LogicalTypeFactoryProtocol` that constructs `DataclassLogicalType` instances for any Python dataclass on the write path (annotation-driven) and read path (Arrow schema metadata), with cross-factory cycle detection via `ResolutionContext`.

**Architecture:** Three-phase rollout. Phase 1 extends the protocol and registry: adds `ResolutionContext`, `supports_class`, and optional `registry`/`context` params to factory methods, changes `_python_class_factories` from a single-factory dict to a list-per-base dict with `supports_class` probing. Phase 2 implements `DataclassLogicalType` (concrete `LogicalTypeProtocol`) and `DataclassHandlerFactory` (stateless, registers against `object`). Phase 3 is comprehensive tests.

**Tech Stack:** Python `dataclasses`, `typing.get_type_hints`, `importlib`, PyArrow (`pa.struct`, `pa.ExtensionType`), Polars (`pl.BaseExtension`), `uv run pytest`.

---

## File Map

| Action | Path | Responsibility |
|--------|------|----------------|
| Modify | `src/orcapod/extension_types/protocols.py` | Add `ResolutionContext` frozen dataclass; add `supports_class` + optional `registry`/`context` params to `LogicalTypeFactoryProtocol` |
| Modify | `src/orcapod/extension_types/registry.py` | `_python_class_factories` → `dict[type, list[...]]`; add `_python_class_cache`; `supports_class` dispatch in MRO walk; pass `registry=self, context=context` to factory calls |
| Create | `src/orcapod/extension_types/dataclass_handler.py` | `DataclassLogicalType` + `DataclassHandlerFactory` |
| Modify | `tests/test_extension_types/test_protocols.py` | Update `_StubFactory` to add `supports_class` and `**kwargs` |
| Modify | `tests/test_extension_types/test_registry.py` | Update `_make_stub_factory` stub; remove/replace the "duplicate factory raises" test |
| Create | `tests/test_extension_types/test_dataclass_handler.py` | All dataclass handler tests |

---

### Task 1: Add `ResolutionContext` to `protocols.py`

**Files:**
- Modify: `src/orcapod/extension_types/protocols.py`

- [ ] **Step 1: Write failing test**

Add this test to `tests/test_extension_types/test_protocols.py`:

```python
def test_resolution_context_is_importable_and_frozen():
    from orcapod.extension_types.protocols import ResolutionContext
    import dataclasses
    ctx = ResolutionContext()
    assert ctx.visited_types == frozenset()
    assert ctx.visited_arrow_names == frozenset()
    assert dataclasses.is_dataclass(ctx)
    # Verify it is frozen (mutation raises FrozenInstanceError)
    import pytest
    with pytest.raises(Exception):
        ctx.visited_types = frozenset()  # type: ignore[misc]


def test_resolution_context_update_produces_new_instance():
    from orcapod.extension_types.protocols import ResolutionContext
    import dataclasses
    ctx = ResolutionContext()
    ctx2 = dataclasses.replace(ctx, visited_types=frozenset({int}))
    assert int in ctx2.visited_types
    assert ctx.visited_types == frozenset()  # original unchanged
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/test_extension_types/test_protocols.py::test_resolution_context_is_importable_and_frozen -v
```

Expected: `ImportError: cannot import name 'ResolutionContext'`

- [ ] **Step 3: Add `ResolutionContext` to `protocols.py`**

In `src/orcapod/extension_types/protocols.py`, add after the existing imports and before `LogicalTypeProtocol`:

```python
from __future__ import annotations

import dataclasses
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    import polars as pl
    import pyarrow as pa


@dataclass(frozen=True)
class ResolutionContext:
    """Immutable context for cycle detection during ``LogicalType`` resolution.

    Passed through the factory call chain so that circular references are
    detected across factory boundaries (e.g. a dataclass ``A`` containing a
    field of type ``B`` which itself contains a field of type ``A``).

    Updates always produce new instances via ``dataclasses.replace(...)``.

    Attributes:
        visited_types: Python types currently being resolved on the call stack.
        visited_arrow_names: Arrow extension names currently being resolved
            on the call stack.
    """

    visited_types: frozenset[type] = frozenset()
    visited_arrow_names: frozenset[str] = frozenset()
```

(The existing `from __future__ import annotations` and `typing` import lines are already there; add `import dataclasses` and `from dataclasses import dataclass`.)

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/test_extension_types/test_protocols.py::test_resolution_context_is_importable_and_frozen tests/test_extension_types/test_protocols.py::test_resolution_context_update_produces_new_instance -v
```

Expected: both PASS

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/extension_types/protocols.py tests/test_extension_types/test_protocols.py
git commit -m "feat(extension_types): add ResolutionContext frozen dataclass to protocols"
```

---

### Task 2: Add `supports_class` and optional `registry`/`context` params to `LogicalTypeFactoryProtocol`; update all stubs

**Files:**
- Modify: `src/orcapod/extension_types/protocols.py`
- Modify: `tests/test_extension_types/test_protocols.py`
- Modify: `tests/test_extension_types/test_registry.py`

- [ ] **Step 1: Update `LogicalTypeFactoryProtocol` in `protocols.py`**

Replace the entire `LogicalTypeFactoryProtocol` class body. The new version adds `supports_class` and extends both factory methods with optional `registry` and `context` params. Note that `LogicalTypeRegistry` is imported only under `TYPE_CHECKING` (it lives in `registry.py` which imports `protocols.py` — importing it at runtime would be circular):

```python
if TYPE_CHECKING:
    import polars as pl
    import pyarrow as pa
    from orcapod.extension_types.registry import LogicalTypeRegistry
```

Replace the `LogicalTypeFactoryProtocol` class with:

```python
@runtime_checkable
class LogicalTypeFactoryProtocol(Protocol):
    """Protocol for factories that synthesize or reconstruct ``LogicalTypeProtocol`` instances.

    Bridges two directions: the write path (``create_for_python_type`` — synthesizes a
    ``LogicalTypeProtocol`` from a Python class) and the read path
    (``reconstruct_from_arrow`` — reconstructs a ``LogicalTypeProtocol`` from Arrow schema
    metadata).

    ``supports_class`` is the write-side gate: the registry calls it during the MRO walk
    to confirm whether this factory should handle a given Python type before committing.
    Read-side dispatch (via ``"category"`` metadata) bypasses ``supports_class`` entirely —
    the category string is fully definitive.

    ``registry`` and ``context`` are optional on both factory methods so that simple
    factories that don't recurse can ignore them. Factories that handle recursive types
    (e.g. nested dataclasses) use ``registry`` to register sub-types as a side effect and
    ``context`` to propagate cycle detection across factory boundaries.

    This protocol is ``@runtime_checkable``, consistent with ``LogicalTypeProtocol``.
    """

    def supports_class(self, python_type: type) -> bool:
        """Return ``True`` if this factory handles *python_type* (write-side gate).

        Called by the registry during the MRO walk after a base class registered
        for this factory is found in the target type's MRO. The first factory
        (in registration order) that returns ``True`` wins.

        Read-side dispatch via ``"category"`` metadata does NOT call this method.

        Args:
            python_type: The concrete Python class being resolved.

        Returns:
            ``True`` if this factory can synthesize a ``LogicalTypeProtocol``
            for *python_type*; ``False`` otherwise.
        """
        ...

    def reconstruct_from_arrow(
        self,
        arrow_extension_name: str,
        storage_type: pa.DataType,
        metadata: dict[str, Any],
        registry: LogicalTypeRegistry | None = None,
        context: ResolutionContext = ResolutionContext(),
    ) -> LogicalTypeProtocol:
        """Reconstruct a LogicalType from Arrow schema metadata (read path).

        Called by the registry when a schema walk encounters an extension type
        whose metadata ``"category"`` value matches this factory's registered
        category. All Arrow schema information is already known.

        Args:
            arrow_extension_name: The Arrow extension type name from the schema.
            storage_type: The underlying Arrow storage type.
            metadata: Full parsed metadata JSON dict. Always contains ``"category"``.
            registry: The ``LogicalTypeRegistry`` to register sub-types into as a
                side effect. ``None`` if the caller has no registry.
            context: Immutable cycle-detection context. Updated with the current
                Arrow extension name before recursing.

        Returns:
            A fully constructed ``LogicalTypeProtocol`` ready for registration.

        Raises:
            ValueError: If this factory cannot reconstruct a type for the given name.
        """
        ...

    def create_for_python_type(
        self,
        python_type: type,
        registry: LogicalTypeRegistry | None = None,
        context: ResolutionContext = ResolutionContext(),
    ) -> LogicalTypeProtocol:
        """Synthesize a LogicalType for the given Python class (write path).

        Called by the registry when pod declaration encounters an unregistered
        class whose MRO intersects a base registered for this factory and
        ``supports_class`` returned ``True``. The factory derives all Arrow
        metadata (extension name, storage type, metadata dict) from the
        Python class itself.

        Args:
            python_type: The concrete Python class to synthesize a LogicalType for.
            registry: The ``LogicalTypeRegistry`` to register sub-types into as a
                side effect. ``None`` if the caller has no registry.
            context: Immutable cycle-detection context. Updated with *python_type*
                before recursing into field resolution.

        Returns:
            A fully constructed ``LogicalTypeProtocol`` ready for registration.

        Raises:
            ValueError: If this factory cannot construct a type for the given class.
        """
        ...
```

- [ ] **Step 2: Update `_StubFactory` in `test_protocols.py`**

Replace the existing `_StubFactory` class:

```python
class _StubFactory:
    """Minimal conforming implementation of LogicalTypeFactoryProtocol for use in tests."""

    def supports_class(self, python_type: type) -> bool:
        return True

    def reconstruct_from_arrow(self, arrow_extension_name, storage_type, metadata, **kwargs):
        return _StubLogicalType()

    def create_for_python_type(self, python_type, **kwargs):
        return _StubLogicalType()
```

- [ ] **Step 3: Update `_make_stub_factory` in `test_registry.py`**

Replace the `_Factory` class inside `_make_stub_factory`:

```python
def _make_stub_factory(return_lt: LogicalTypeProtocol | None = None) -> LogicalTypeFactoryProtocol:
    """Factory for minimal LogicalTypeFactoryProtocol conforming stubs.

    If ``return_lt`` is given, ``reconstruct_from_arrow`` returns it; otherwise
    it creates a fresh stub using ``_make_stub`` keyed on the arrow name.
    ``calls`` records every invocation as ``(arrow_extension_name, storage_type, metadata)``.
    ``python_type_calls`` records every ``create_for_python_type`` invocation.
    """
    _return_lt = return_lt

    class _Factory:
        def __init__(self):
            self.calls: list[tuple] = []
            self.python_type_calls: list[type] = []

        def supports_class(self, python_type: type) -> bool:
            return True

        def reconstruct_from_arrow(self, arrow_extension_name, storage_type, metadata, **kwargs):
            self.calls.append((arrow_extension_name, storage_type, metadata))
            if _return_lt is not None:
                return _return_lt
            return _make_stub(arrow_name=arrow_extension_name, storage=storage_type)

        def create_for_python_type(self, python_type, **kwargs):
            self.python_type_calls.append(python_type)
            if _return_lt is not None:
                return _return_lt
            return _make_stub(py_type=python_type)

    return _Factory()
```

- [ ] **Step 4: Delete the now-invalid duplicate-factory test**

The test `test_register_logical_type_factory_python_base_duplicate_different_factory_raises` asserts that registering a different factory for the same base raises `ValueError`. This behaviour **changes** in Task 3 (multiple factories per base are now allowed). Delete this test function entirely from `tests/test_extension_types/test_registry.py`.

- [ ] **Step 5: Run the full protocol + registry test suites**

```bash
uv run pytest tests/test_extension_types/test_protocols.py tests/test_extension_types/test_registry.py -v
```

Expected: all tests PASS (none should fail — the stub updates are purely additive).

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/extension_types/protocols.py tests/test_extension_types/test_protocols.py tests/test_extension_types/test_registry.py
git commit -m "feat(extension_types): add supports_class + registry/context params to LogicalTypeFactoryProtocol; update stubs"
```

---

### Task 3: Update `registry.py` — list-per-base factories, `supports_class` dispatch, `context` forwarding

**Files:**
- Modify: `src/orcapod/extension_types/registry.py`

**Background on what changes:**

1. `_python_class_factories: dict[type, LogicalTypeFactoryProtocol]` → `dict[type, list[LogicalTypeFactoryProtocol]]`
2. New `_python_class_cache: dict[type, LogicalTypeFactoryProtocol]` — caches the winning factory after `supports_class` probing so subsequent calls skip the MRO scan.
3. `register_logical_type_factory` — validation for `python_bases` changes: instead of raising when a different factory is already registered, multiple factories are appended to the list (idempotent for the exact same instance).
4. `ensure_logical_type_for_python_class` — the MRO walk iterates the factory list and calls `supports_class`; the winner is cached; `registry=self` and `context` are forwarded to the factory call.
5. `ensure_extension_type` — forwards `registry=self` and a fresh `ResolutionContext()` to `reconstruct_from_arrow`.
6. `ensure_logical_type_for_python_class` gains an optional `context: ResolutionContext = ResolutionContext()` param and forwards it.

- [ ] **Step 1: Write failing tests for new multi-factory behavior**

Add these tests to `tests/test_extension_types/test_registry.py` (at the end of the `register_logical_type_factory` section, around line 355):

```python
def test_register_logical_type_factory_python_base_multiple_factories_allowed():
    """Multiple different factories can be registered for the same python_base."""
    from orcapod.extension_types.protocols import LogicalTypeFactoryProtocol
    registry = LogicalTypeRegistry()
    f1 = _make_stub_factory()
    f2 = _make_stub_factory()
    # Both registrations should succeed without raising
    registry.register_logical_type_factory(f1, python_bases=[object])
    registry.register_logical_type_factory(f2, python_bases=[object])  # must not raise


def test_register_logical_type_factory_supports_class_selects_first_match():
    """When multiple factories are registered for the same base, the first whose
    supports_class returns True wins (registration order)."""
    import dataclasses as _dc

    @_dc.dataclass
    class _DC:
        x: int

    class _PickyFactory:
        """Only handles dataclasses."""
        def supports_class(self, python_type: type) -> bool:
            return _dc.is_dataclass(python_type)
        def create_for_python_type(self, python_type, **kwargs):
            return _make_stub(py_type=python_type)
        def reconstruct_from_arrow(self, *args, **kwargs):
            return _make_stub()

    class _FallbackFactory:
        """Handles everything."""
        def __init__(self):
            self.called = False
        def supports_class(self, python_type: type) -> bool:
            return True
        def create_for_python_type(self, python_type, **kwargs):
            self.called = True
            return _make_stub(py_type=python_type)
        def reconstruct_from_arrow(self, *args, **kwargs):
            return _make_stub()

    registry = LogicalTypeRegistry()
    picky = _PickyFactory()
    fallback = _FallbackFactory()
    registry.register_logical_type_factory(picky, python_bases=[object])
    registry.register_logical_type_factory(fallback, python_bases=[object])

    registry.ensure_logical_type_for_python_class(_DC)
    # Picky factory wins — fallback should NOT have been called
    assert not fallback.called


def test_register_logical_type_factory_supports_class_falls_through_to_next():
    """When the first factory's supports_class returns False, the next factory is tried."""
    class _PlainClass:
        pass

    class _RejectingFactory:
        def supports_class(self, python_type: type) -> bool:
            return False  # never accepts
        def create_for_python_type(self, python_type, **kwargs):
            raise AssertionError("should not be called")
        def reconstruct_from_arrow(self, *args, **kwargs):
            return _make_stub()

    class _AcceptingFactory:
        def __init__(self):
            self.called_with: list[type] = []
        def supports_class(self, python_type: type) -> bool:
            return True
        def create_for_python_type(self, python_type, **kwargs):
            self.called_with.append(python_type)
            return _make_stub(py_type=python_type)
        def reconstruct_from_arrow(self, *args, **kwargs):
            return _make_stub()

    registry = LogicalTypeRegistry()
    rejecting = _RejectingFactory()
    accepting = _AcceptingFactory()
    registry.register_logical_type_factory(rejecting, python_bases=[object])
    registry.register_logical_type_factory(accepting, python_bases=[object])

    registry.ensure_logical_type_for_python_class(_PlainClass)
    assert _PlainClass in accepting.called_with
```

- [ ] **Step 2: Run new tests to confirm they fail**

```bash
uv run pytest tests/test_extension_types/test_registry.py::test_register_logical_type_factory_python_base_multiple_factories_allowed tests/test_extension_types/test_registry.py::test_register_logical_type_factory_supports_class_selects_first_match tests/test_extension_types/test_registry.py::test_register_logical_type_factory_supports_class_falls_through_to_next -v
```

Expected: FAIL (current code raises `ValueError` for multiple factories per base).

- [ ] **Step 3: Update `registry.py` — `__init__`, `register_logical_type_factory`, `ensure_logical_type_for_python_class`, `ensure_extension_type`**

In `registry.py`, add `ResolutionContext` to the import from `protocols`:

```python
from orcapod.extension_types.protocols import LogicalTypeProtocol, LogicalTypeFactoryProtocol, ResolutionContext
```

Replace `LogicalTypeRegistry.__init__`:

```python
def __init__(self, logical_types: list[LogicalTypeProtocol] | None = None) -> None:
    self._by_logical_name: dict[str, LogicalTypeProtocol] = {}
    self._by_arrow_name: dict[str, LogicalTypeProtocol] = {}
    self._by_python_type: dict[type, LogicalTypeProtocol] = {}
    self._category_factories: dict[str, LogicalTypeFactoryProtocol] = {}
    self._python_class_factories: dict[type, list[LogicalTypeFactoryProtocol]] = {}
    self._python_class_cache: dict[type, LogicalTypeFactoryProtocol] = {}
    for lt in (logical_types or []):
        self.register_logical_type(lt)
```

Replace the `python_bases` block in `register_logical_type_factory`. Find the section starting with `# Validate all bases before writing any`:

```python
# Multiple factories per base are allowed; append if not already present.
for base in python_bases_list:
    lst = self._python_class_factories.setdefault(base, [])
    if factory not in lst:
        lst.append(factory)
        logger.debug(
            "registered LogicalTypeFactory for python base %r: %r", base, factory
        )
```

Also remove the old validate-then-write two-pass loop and the ValueError for different factories on the same base. The full updated `register_logical_type_factory` python_bases block (lines ~339–353) becomes just the above.

Also update the docstring for `register_logical_type_factory` to remove the "Raises ValueError if a different factory is already registered for a given key" note for `python_bases` (that constraint is gone; it only remains for `category`).

Replace `ensure_logical_type_for_python_class` with this new version:

```python
def ensure_logical_type_for_python_class(
    self,
    python_type: type,
    context: ResolutionContext = ResolutionContext(),
) -> LogicalTypeProtocol:
    """Ensure a LogicalType exists for ``python_type``, synthesizing via factory if needed.

    Resolution algorithm:

    1. Exact-match shortcut: if ``python_type`` is already in ``_by_python_type``,
       return immediately.
    2. Factory cache shortcut: if a winning factory for this type was previously
       cached in ``_python_class_cache``, call it directly without re-probing.
    3. Walk ``python_type.__mro__``. Track the first (most-specific) hit in
       ``_by_python_type`` (concrete) and ``_python_class_factories`` (factory)
       separately, recording the MRO index of each. For factory hits, iterate
       through the factory list in registration order and call
       ``factory.supports_class(python_type)``; the first True wins.
    4. After the MRO walk, if no factory was found, do a fallback ``issubclass``
       scan over ``_python_class_factories`` keys to catch ABCs with
       ``__subclasshook__``. Assign these the least-specific index
       (``len(python_type.__mro__)``) so they lose to any direct MRO match.
    5. Resolution rule: if both concrete and factory are found, compare MRO indices —
       lower index wins. Ties (same class) → concrete wins.
    6. If factory wins (or only factory found): cache the factory in
       ``_python_class_cache[python_type]``, call
       ``factory.create_for_python_type(python_type, registry=self, context=context)``,
       register the result via ``register_logical_type``, and return it.
    7. If nothing found: raise ``TypeError``.

    Args:
        python_type: The Python class to resolve.
        context: Immutable cycle-detection context forwarded to the factory.

    Returns:
        The registered or newly synthesized ``LogicalTypeProtocol``.

    Raises:
        TypeError: If no ``LogicalType`` and no factory is found for
            ``python_type`` or any of its bases.
    """
    # Step 1: exact-match shortcut.
    exact = self._by_python_type.get(python_type)
    if exact is not None:
        return exact

    # Step 2: factory cache shortcut (skips supports_class re-probing).
    cached_factory = self._python_class_cache.get(python_type)
    if cached_factory is not None:
        lt = cached_factory.create_for_python_type(python_type, registry=self, context=context)
        self.register_logical_type(lt)
        logger.debug(
            "ensure_logical_type_for_python_class: synthesized %r for %r (factory cache hit)",
            lt.logical_type_name,
            python_type,
        )
        return lt

    best_concrete_idx: int | None = None
    best_concrete: LogicalTypeProtocol | None = None
    best_factory_idx: int | None = None
    best_factory: LogicalTypeFactoryProtocol | None = None

    # Step 3: Walk MRO for direct hits.
    for i, base in enumerate(python_type.__mro__):
        if best_concrete is None and base in self._by_python_type:
            best_concrete_idx = i
            best_concrete = self._by_python_type[base]
        if best_factory is None and base in self._python_class_factories:
            for factory in self._python_class_factories[base]:
                if factory.supports_class(python_type):
                    best_factory_idx = i
                    best_factory = factory
                    break
        if best_concrete is not None and best_factory is not None:
            break

    # Step 4: issubclass fallback scan for ABCs with __subclasshook__.
    if best_factory is None:
        for base_class, factory_list in self._python_class_factories.items():
            try:
                if issubclass(python_type, base_class):
                    for factory in factory_list:
                        if factory.supports_class(python_type):
                            best_factory = factory
                            best_factory_idx = len(python_type.__mro__)
                            break
                    if best_factory is not None:
                        break
            except TypeError:
                continue

    # Step 5: Nothing found — hard error.
    if best_concrete is None and best_factory is None:
        raise TypeError(
            f"No LogicalType or LogicalTypeFactory is registered for type "
            f"{python_type!r}.\n"
            f"To handle this type, register a factory for its base class:\n"
            f"  registry.register_logical_type_factory(\n"
            f"      factory, python_bases=[<base of {python_type.__name__}>]\n"
            f"  )\n"
            f"Or register a concrete LogicalType directly:\n"
            f"  registry.register_logical_type(my_logical_type)"
        )

    # Only concrete found.
    if best_factory is None:
        assert best_concrete is not None
        return best_concrete

    def _synthesize(factory: LogicalTypeFactoryProtocol) -> LogicalTypeProtocol:
        self._python_class_cache[python_type] = factory
        lt = factory.create_for_python_type(python_type, registry=self, context=context)
        self.register_logical_type(lt)
        logger.debug(
            "ensure_logical_type_for_python_class: synthesized %r for %r",
            lt.logical_type_name,
            python_type,
        )
        return lt

    # Only factory found — synthesize and cache.
    if best_concrete is None:
        assert best_factory is not None
        return _synthesize(best_factory)

    # Both found — compare MRO specificity (lower index = more specific).
    assert best_concrete_idx is not None
    assert best_factory_idx is not None
    if best_concrete_idx <= best_factory_idx:
        return best_concrete
    else:
        assert best_factory is not None
        return _synthesize(best_factory)
```

In `ensure_extension_type`, update the factory call site (near line 457) to forward registry and a fresh context:

```python
logical_type = factory.reconstruct_from_arrow(
    arrow_extension_name, storage_type, metadata_dict,
    registry=self,
    context=ResolutionContext(),
)
```

- [ ] **Step 4: Run all registry tests**

```bash
uv run pytest tests/test_extension_types/test_registry.py -v
```

Expected: all tests PASS (including the three new ones).

- [ ] **Step 5: Run full extension_types test suite**

```bash
uv run pytest tests/test_extension_types/ -v
```

Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/extension_types/registry.py tests/test_extension_types/test_registry.py
git commit -m "feat(extension_types): support multiple factories per base with supports_class dispatch; forward registry+context to factory calls"
```

---

### Task 4: Implement `DataclassLogicalType`

**Files:**
- Create: `src/orcapod/extension_types/dataclass_handler.py`
- Create: `tests/test_extension_types/test_dataclass_handler.py`

- [ ] **Step 1: Write failing tests for `DataclassLogicalType`**

Create `tests/test_extension_types/test_dataclass_handler.py`:

```python
"""Tests for DataclassLogicalType and DataclassHandlerFactory (PLT-1657)."""

from __future__ import annotations

import dataclasses
import uuid

import pyarrow as pa
import polars as pl
import pytest

from orcapod.extension_types.protocols import LogicalTypeProtocol, ResolutionContext


# ---------------------------------------------------------------------------
# Shared dataclass fixtures (module-level so Arrow extension names are stable)
# ---------------------------------------------------------------------------

@dataclasses.dataclass
class Flat:
    x: int
    y: str


@dataclasses.dataclass
class AllPrimitives:
    i: int
    f: float
    s: str
    b: bool
    by: bytes


@dataclasses.dataclass
class WithList:
    items: list[int]


@dataclasses.dataclass
class Inner:
    a: int


@dataclasses.dataclass
class Outer:
    inner: Inner
    z: str


# ---------------------------------------------------------------------------
# DataclassLogicalType — unit tests
# ---------------------------------------------------------------------------

def _make_flat_lt():
    """Construct a DataclassLogicalType for Flat without using the full factory."""
    from orcapod.extension_types.dataclass_handler import DataclassLogicalType
    import pyarrow as pa
    storage = pa.struct([pa.field("x", pa.int64()), pa.field("y", pa.large_string())])
    identity = lambda v: v
    field_converters = [("x", identity, identity), ("y", identity, identity)]
    fqcn = f"{Flat.__module__}.{Flat.__qualname__}"
    return DataclassLogicalType(fqcn, Flat, storage, field_converters)


def test_dataclass_logical_type_satisfies_protocol():
    lt = _make_flat_lt()
    assert isinstance(lt, LogicalTypeProtocol)


def test_dataclass_logical_type_logical_name():
    lt = _make_flat_lt()
    expected = f"{Flat.__module__}.{Flat.__qualname__}"
    assert lt.logical_type_name == expected


def test_dataclass_logical_type_python_type():
    lt = _make_flat_lt()
    assert lt.python_type is Flat


def test_dataclass_logical_type_get_arrow_extension_type():
    lt = _make_flat_lt()
    ext = lt.get_arrow_extension_type()
    assert isinstance(ext, pa.ExtensionType)
    assert ext.extension_name == lt.logical_type_name
    expected_storage = pa.struct([pa.field("x", pa.int64()), pa.field("y", pa.large_string())])
    assert ext.storage_type == expected_storage


def test_dataclass_logical_type_get_arrow_extension_type_cached():
    lt = _make_flat_lt()
    ext1 = lt.get_arrow_extension_type()
    ext2 = lt.get_arrow_extension_type()
    assert ext1 is ext2


def test_dataclass_logical_type_get_polars_extension_type():
    lt = _make_flat_lt()
    polars_ext = lt.get_polars_extension_type()
    assert isinstance(polars_ext, pl.BaseExtension)


def test_dataclass_logical_type_get_polars_extension_type_cached():
    lt = _make_flat_lt()
    p1 = lt.get_polars_extension_type()
    p2 = lt.get_polars_extension_type()
    assert p1 is p2


def test_dataclass_logical_type_arrow_metadata_contains_category():
    lt = _make_flat_lt()
    ext = lt.get_arrow_extension_type()
    import json
    meta = json.loads(ext.__arrow_ext_serialize__().decode("utf-8"))
    assert meta["category"] == "orcapod.dataclass"


def test_dataclass_logical_type_python_to_storage():
    lt = _make_flat_lt()
    result = lt.python_to_storage(Flat(x=7, y="hello"))
    assert result == {"x": 7, "y": "hello"}


def test_dataclass_logical_type_storage_to_python():
    lt = _make_flat_lt()
    result = lt.storage_to_python({"x": 7, "y": "hello"})
    assert result == Flat(x=7, y="hello")
    assert isinstance(result, Flat)
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
uv run pytest tests/test_extension_types/test_dataclass_handler.py -v
```

Expected: `ImportError: cannot import name 'DataclassLogicalType'`

- [ ] **Step 3: Create `dataclass_handler.py` with `DataclassLogicalType`**

Create `src/orcapod/extension_types/dataclass_handler.py`:

```python
"""Handler factory for Python dataclasses.

Implements ``DataclassHandlerFactory`` — a ``LogicalTypeFactoryProtocol`` that
constructs ``DataclassLogicalType`` instances for any Python dataclass on both
the write path (annotation-driven) and read path (Arrow schema metadata).

Registration example::

    factory = DataclassHandlerFactory()
    registry.register_logical_type_factory(
        factory,
        category="orcapod.dataclass",
        python_bases=[object],
    )

Note:
    Nested dataclasses are stored as plain Arrow sub-structs (not extension
    types). Only the outermost column is self-describing via the extension
    type metadata. Supporting nested extension types inside struct sub-fields
    is tracked as a v0.2 issue (PLT-1700).

    Registered logical types (e.g. ``pathlib.Path``, ``uuid.UUID``) used as
    dataclass field annotations are not supported in this version and will
    raise ``TypeError``. A follow-up issue will add registry-lookup support.
"""

from __future__ import annotations

import dataclasses
import importlib
from typing import TYPE_CHECKING, Any, Callable

from orcapod.extension_types.protocols import ResolutionContext
from orcapod.extension_types.registry import make_arrow_extension_type, make_polars_extension_type
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import polars as pl
    import pyarrow as pa
    from orcapod.extension_types.protocols import LogicalTypeProtocol
    from orcapod.extension_types.registry import LogicalTypeRegistry
else:
    pa = LazyModule("pyarrow")
    pl = LazyModule("polars")

_DATACLASS_CATEGORY_METADATA = b'{"category": "orcapod.dataclass"}'

# Mapping from primitive Python type → Arrow type.
_PRIMITIVE_ARROW: dict[type, Any] = {}


def _primitive_arrow_map() -> dict[type, Any]:
    """Return (and lazily build) the primitive-type → Arrow-type lookup table."""
    global _PRIMITIVE_ARROW
    if not _PRIMITIVE_ARROW:
        _PRIMITIVE_ARROW = {
            int: pa.int64(),
            float: pa.float64(),
            str: pa.large_string(),
            bool: pa.bool_(),
            bytes: pa.large_binary(),
        }
    return _PRIMITIVE_ARROW


class DataclassLogicalType:
    """Concrete ``LogicalTypeProtocol`` for a specific Python dataclass.

    Constructed once by ``DataclassHandlerFactory``; holds no registry reference —
    all conversion logic is baked in at construction time via pre-built field
    converters.

    Args:
        logical_name: Fully qualified class name (e.g. ``"my.module.Data1"``).
        python_type: The dataclass class.
        storage_type: ``pa.struct([...])`` describing the Arrow layout.
        field_converters: Ordered list of ``(field_name, to_storage_fn,
            from_storage_fn)`` tuples. Primitive fields use identity functions;
            nested dataclass fields use their own ``python_to_storage`` /
            ``storage_to_python`` methods; ``list[T]`` fields use element-wise
            converters.
    """

    def __init__(
        self,
        logical_name: str,
        python_type: type,
        storage_type: pa.DataType,
        field_converters: list[tuple[str, Callable[..., Any], Callable[..., Any]]],
    ) -> None:
        self._logical_name = logical_name
        self._python_type = python_type
        self._storage_type = storage_type
        self._field_converters = field_converters
        self._arrow_ext_class = make_arrow_extension_type(
            logical_name, storage_type, _DATACLASS_CATEGORY_METADATA
        )
        self._polars_ext_class = make_polars_extension_type(logical_name, storage_type)
        self._arrow_ext: pa.ExtensionType | None = None
        self._polars_ext: pl.BaseExtension | None = None

    @property
    def logical_type_name(self) -> str:
        """Fully qualified class name used as the Arrow extension name."""
        return self._logical_name

    @property
    def python_type(self) -> type:
        """The dataclass class this logical type represents."""
        return self._python_type

    def get_arrow_extension_type(self) -> pa.ExtensionType:
        """Return a cached Arrow extension type instance for this dataclass.

        Returns:
            A ``pa.ExtensionType`` with extension name equal to the FQCN and
            storage type ``pa.struct([...])``. Metadata bytes encode
            ``{"category": "orcapod.dataclass"}`` for read-path dispatch.
        """
        if self._arrow_ext is None:
            self._arrow_ext = self._arrow_ext_class()
        return self._arrow_ext

    def get_polars_extension_type(self) -> pl.BaseExtension:
        """Return a cached Polars extension type instance for this dataclass.

        Returns:
            A ``pl.BaseExtension`` registered under the FQCN.
        """
        if self._polars_ext is None:
            self._polars_ext = self._polars_ext_class()
        return self._polars_ext

    def python_to_storage(self, value: Any) -> dict[str, Any]:
        """Convert a dataclass instance to a Python dict for Arrow struct storage.

        Args:
            value: A Python instance of ``python_type``.

        Returns:
            A dict mapping field names to their storage-converted values.
        """
        return {
            name: to_fn(getattr(value, name))
            for name, to_fn, _ in self._field_converters
        }

    def storage_to_python(self, storage_value: Any) -> Any:
        """Reconstruct the dataclass from an Arrow struct ``.as_py()`` dict.

        Args:
            storage_value: A Python dict as returned by ``scalar.as_py()`` for
                a struct-storage Arrow scalar.

        Returns:
            A fully reconstructed instance of ``python_type``.
        """
        return self._python_type(**{
            name: from_fn(storage_value[name])
            for name, _, from_fn in self._field_converters
        })
```

- [ ] **Step 4: Run tests to confirm they pass**

```bash
uv run pytest tests/test_extension_types/test_dataclass_handler.py -v -k "logical_type"
```

Expected: all `test_dataclass_logical_type_*` tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/extension_types/dataclass_handler.py tests/test_extension_types/test_dataclass_handler.py
git commit -m "feat(extension_types): implement DataclassLogicalType"
```

---

### Task 5: Implement `DataclassHandlerFactory` write path (flat + list + nested)

**Files:**
- Modify: `src/orcapod/extension_types/dataclass_handler.py`
- Modify: `tests/test_extension_types/test_dataclass_handler.py`

- [ ] **Step 1: Write failing tests for write path**

Append to `tests/test_extension_types/test_dataclass_handler.py`:

```python
# ---------------------------------------------------------------------------
# DataclassHandlerFactory — protocol conformance
# ---------------------------------------------------------------------------

def test_handler_factory_satisfies_protocol():
    from orcapod.extension_types.protocols import LogicalTypeFactoryProtocol
    from orcapod.extension_types.dataclass_handler import DataclassHandlerFactory
    assert isinstance(DataclassHandlerFactory(), LogicalTypeFactoryProtocol)


def test_handler_factory_supports_class_true_for_dataclass():
    from orcapod.extension_types.dataclass_handler import DataclassHandlerFactory
    factory = DataclassHandlerFactory()
    assert factory.supports_class(Flat) is True


def test_handler_factory_supports_class_false_for_plain_class():
    from orcapod.extension_types.dataclass_handler import DataclassHandlerFactory
    class _Plain:
        pass
    factory = DataclassHandlerFactory()
    assert factory.supports_class(_Plain) is False


# ---------------------------------------------------------------------------
# Write path — flat dataclass with primitives
# ---------------------------------------------------------------------------

def test_create_for_python_type_logical_name():
    from orcapod.extension_types.dataclass_handler import DataclassHandlerFactory
    factory = DataclassHandlerFactory()
    lt = factory.create_for_python_type(Flat)
    assert lt.logical_type_name == f"{Flat.__module__}.{Flat.__qualname__}"


def test_create_for_python_type_python_type():
    from orcapod.extension_types.dataclass_handler import DataclassHandlerFactory
    factory = DataclassHandlerFactory()
    lt = factory.create_for_python_type(Flat)
    assert lt.python_type is Flat


def test_create_for_python_type_arrow_struct_layout():
    from orcapod.extension_types.dataclass_handler import DataclassHandlerFactory
    factory = DataclassHandlerFactory()
    lt = factory.create_for_python_type(Flat)
    expected = pa.struct([pa.field("x", pa.int64()), pa.field("y", pa.large_string())])
    assert lt.get_arrow_extension_type().storage_type == expected


def test_create_for_python_type_not_dataclass_raises():
    from orcapod.extension_types.dataclass_handler import DataclassHandlerFactory
    factory = DataclassHandlerFactory()
    with pytest.raises(ValueError, match="not a dataclass"):
        factory.create_for_python_type(str)


def test_all_primitives_round_trip():
    from orcapod.extension_types.dataclass_handler import DataclassHandlerFactory
    factory = DataclassHandlerFactory()
    lt = factory.create_for_python_type(AllPrimitives)
    original = AllPrimitives(i=1, f=2.5, s="hi", b=True, by=b"\x00\x01")
    storage = lt.python_to_storage(original)
    assert storage == {"i": 1, "f": 2.5, "s": "hi", "b": True, "by": b"\x00\x01"}
    reconstructed = lt.storage_to_python(storage)
    assert reconstructed == original


def test_all_primitives_arrow_types():
    from orcapod.extension_types.dataclass_handler import DataclassHandlerFactory
    factory = DataclassHandlerFactory()
    lt = factory.create_for_python_type(AllPrimitives)
    ext = lt.get_arrow_extension_type()
    struct_type = ext.storage_type
    assert struct_type.field("i").type == pa.int64()
    assert struct_type.field("f").type == pa.float64()
    assert struct_type.field("s").type == pa.large_string()
    assert struct_type.field("b").type == pa.bool_()
    assert struct_type.field("by").type == pa.large_binary()


# ---------------------------------------------------------------------------
# Write path — list[T] fields
# ---------------------------------------------------------------------------

def test_list_int_field_arrow_type():
    from orcapod.extension_types.dataclass_handler import DataclassHandlerFactory
    factory = DataclassHandlerFactory()
    lt = factory.create_for_python_type(WithList)
    struct_type = lt.get_arrow_extension_type().storage_type
    assert struct_type.field("items").type == pa.list_(pa.int64())


def test_list_int_field_round_trip():
    from orcapod.extension_types.dataclass_handler import DataclassHandlerFactory
    factory = DataclassHandlerFactory()
    lt = factory.create_for_python_type(WithList)
    original = WithList(items=[1, 2, 3])
    storage = lt.python_to_storage(original)
    assert storage == {"items": [1, 2, 3]}
    reconstructed = lt.storage_to_python(storage)
    assert reconstructed == original


# ---------------------------------------------------------------------------
# Write path — nested dataclass
# ---------------------------------------------------------------------------

def test_nested_dataclass_arrow_type():
    from orcapod.extension_types.dataclass_handler import DataclassHandlerFactory
    factory = DataclassHandlerFactory()
    lt = factory.create_for_python_type(Outer)
    struct_type = lt.get_arrow_extension_type().storage_type
    # inner field should be a plain struct, not an extension type
    inner_field_type = struct_type.field("inner").type
    assert inner_field_type == pa.struct([pa.field("a", pa.int64())])
    assert struct_type.field("z").type == pa.large_string()


def test_nested_dataclass_round_trip():
    from orcapod.extension_types.dataclass_handler import DataclassHandlerFactory
    factory = DataclassHandlerFactory()
    lt = factory.create_for_python_type(Outer)
    original = Outer(inner=Inner(a=42), z="world")
    storage = lt.python_to_storage(original)
    assert storage == {"inner": {"a": 42}, "z": "world"}
    reconstructed = lt.storage_to_python(storage)
    assert reconstructed == original


def test_nested_dataclass_registers_inner_in_registry():
    from orcapod.extension_types.dataclass_handler import DataclassHandlerFactory
    from orcapod.extension_types.registry import LogicalTypeRegistry
    factory = DataclassHandlerFactory()
    registry = LogicalTypeRegistry()
    lt = factory.create_for_python_type(Outer, registry=registry)
    registry.register_logical_type(lt)
    # Inner was registered as a side effect
    inner_lt = registry.get_by_python_type(Inner)
    assert inner_lt is not None
    assert inner_lt.python_type is Inner
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
uv run pytest tests/test_extension_types/test_dataclass_handler.py -v -k "factory or create or round_trip or nested or list or primitive"
```

Expected: `ImportError: cannot import name 'DataclassHandlerFactory'`

- [ ] **Step 3: Add `DataclassHandlerFactory` and `_resolve_field` to `dataclass_handler.py`**

Append to `src/orcapod/extension_types/dataclass_handler.py` after `DataclassLogicalType`:

```python
class DataclassHandlerFactory:
    """Factory that synthesizes ``DataclassLogicalType`` instances for Python dataclasses.

    Stateless — holds no registry reference. Registers against ``object`` in the
    ``LogicalTypeRegistry`` write-side dispatch, with ``supports_class`` as the gate
    that confirms the Python type is actually a dataclass.

    Registration::

        factory = DataclassHandlerFactory()
        registry.register_logical_type_factory(
            factory,
            category="orcapod.dataclass",
            python_bases=[object],
        )
    """

    def supports_class(self, python_type: type) -> bool:
        """Return ``True`` if *python_type* is a Python dataclass.

        Called by the registry during the MRO walk after hitting ``object``.
        Not called on the read path.

        Args:
            python_type: The Python class to test.

        Returns:
            ``True`` if ``dataclasses.is_dataclass(python_type)`` is ``True``.
        """
        return dataclasses.is_dataclass(python_type)

    def _resolve_field(
        self,
        annotation: Any,
        registry: LogicalTypeRegistry | None,
        context: ResolutionContext,
    ) -> tuple[pa.DataType, Callable[..., Any], Callable[..., Any]]:
        """Resolve one field annotation to an Arrow type and a pair of converters.

        Args:
            annotation: The Python type annotation for a dataclass field.
            registry: Optional registry for side-effect registration of nested types.
            context: Current cycle-detection context (outer class already added).

        Returns:
            A ``(arrow_type, to_storage, from_storage)`` triple.

        Raises:
            TypeError: If the annotation is not a supported type.
        """
        import typing

        primitive_map = _primitive_arrow_map()

        # Primitive types
        if annotation in primitive_map:
            arrow_type = primitive_map[annotation]
            identity: Callable[..., Any] = lambda v: v
            return arrow_type, identity, identity

        # list[T]
        origin = typing.get_origin(annotation)
        if origin is list:
            args = typing.get_args(annotation)
            if not args:
                raise TypeError(
                    f"Unsupported field annotation: bare 'list' with no type argument. "
                    f"Use list[T] with a concrete element type."
                )
            elem_arrow, elem_to, elem_from = self._resolve_field(args[0], registry, context)
            arrow_type = pa.list_(elem_arrow)

            def to_storage_list(val: Any, _to: Callable[..., Any] = elem_to) -> list[Any]:
                return [_to(x) for x in val]

            def from_storage_list(val: Any, _from: Callable[..., Any] = elem_from) -> list[Any]:
                return [_from(x) for x in val]

            return arrow_type, to_storage_list, from_storage_list

        # Nested dataclass
        if isinstance(annotation, type) and dataclasses.is_dataclass(annotation):
            nested_lt = self.create_for_python_type(annotation, registry, context)
            if registry is not None:
                registry.register_logical_type(nested_lt)
            # Use raw struct storage type, NOT the extension type — nested structs
            # are plain sub-structs to avoid unsupported nested extension types (PLT-1700).
            nested_storage = nested_lt.get_arrow_extension_type().storage_type
            return nested_storage, nested_lt.python_to_storage, nested_lt.storage_to_python

        raise TypeError(
            f"Unsupported field type annotation: {annotation!r}. "
            f"Supported types: int, float, str, bool, bytes, list[T], and nested "
            f"dataclasses. Registered logical types (e.g. pathlib.Path, uuid.UUID) "
            f"as field types are not yet supported — see follow-up issue."
        )

    def create_for_python_type(
        self,
        python_type: type,
        registry: LogicalTypeRegistry | None = None,
        context: ResolutionContext = ResolutionContext(),
    ) -> DataclassLogicalType:
        """Synthesize a ``DataclassLogicalType`` for *python_type* (write path).

        Derives the Arrow struct layout and field converters from the class
        annotations. Registers nested dataclass types in *registry* as a side
        effect so they are available for subsequent lookups.

        Args:
            python_type: A Python dataclass class.
            registry: Optional registry; if provided, nested dataclass types are
                registered as a side effect.
            context: Cycle-detection context. Any class already in
                ``context.visited_types`` will trigger a ``TypeError``.

        Returns:
            A fully constructed ``DataclassLogicalType``.

        Raises:
            ValueError: If *python_type* is not a dataclass.
            TypeError: If a circular reference is detected via *context*.
            TypeError: If a field uses an unsupported annotation.
        """
        from typing import get_type_hints

        if not dataclasses.is_dataclass(python_type):
            raise ValueError(
                f"{python_type!r} is not a dataclass. "
                f"DataclassHandlerFactory only handles @dataclass-decorated classes."
            )

        if python_type in context.visited_types:
            raise TypeError(
                f"Circular reference detected: {python_type!r} is already being "
                f"resolved. Dataclass fields cannot form circular references because "
                f"Arrow struct storage sizes must be finite."
            )

        # Update context BEFORE resolving fields so nested calls see this type.
        context = dataclasses.replace(
            context,
            visited_types=context.visited_types | {python_type},
        )

        hints = get_type_hints(python_type)
        struct_fields: list[pa.Field] = []
        field_converters: list[tuple[str, Callable[..., Any], Callable[..., Any]]] = []

        for field in dataclasses.fields(python_type):
            if not field.init:
                continue
            annotation = hints[field.name]
            arrow_type, to_fn, from_fn = self._resolve_field(annotation, registry, context)
            struct_fields.append(pa.field(field.name, arrow_type))
            field_converters.append((field.name, to_fn, from_fn))

        storage_type = pa.struct(struct_fields)
        fqcn = f"{python_type.__module__}.{python_type.__qualname__}"
        return DataclassLogicalType(fqcn, python_type, storage_type, field_converters)

    def reconstruct_from_arrow(
        self,
        arrow_extension_name: str,
        storage_type: pa.DataType,
        metadata: dict[str, Any],
        registry: LogicalTypeRegistry | None = None,
        context: ResolutionContext = ResolutionContext(),
    ) -> DataclassLogicalType:
        """Reconstruct a ``DataclassLogicalType`` from Arrow schema metadata (read path).

        Imports the dataclass class by its FQCN (the Arrow extension name), then
        builds field converters from the class annotations. The *storage_type* from
        the schema is used as-is — it is not re-derived from the class.

        Args:
            arrow_extension_name: FQCN of the dataclass (e.g. ``"my.module.MyClass"``).
            storage_type: Arrow struct storage type from the schema.
            metadata: Full parsed metadata JSON dict (must contain ``"category"``).
            registry: Optional registry; nested types are registered as a side effect.
            context: Cycle-detection context.

        Returns:
            A fully constructed ``DataclassLogicalType``.

        Raises:
            ValueError: If the FQCN cannot be imported or is not a dataclass.
            ValueError: If a circular reference is detected via *context*.
        """
        from typing import get_type_hints

        if arrow_extension_name in context.visited_arrow_names:
            raise ValueError(
                f"Circular reference detected: {arrow_extension_name!r} is already "
                f"being resolved on the read path."
            )

        context = dataclasses.replace(
            context,
            visited_arrow_names=context.visited_arrow_names | {arrow_extension_name},
        )

        # Import the class by FQCN (split on last dot).
        last_dot = arrow_extension_name.rfind(".")
        if last_dot == -1:
            raise ValueError(
                f"Cannot import class from FQCN {arrow_extension_name!r}: "
                f"no module separator (dot) found. "
                f"Expected a fully qualified name such as 'my.module.MyClass'."
            )
        module_path = arrow_extension_name[:last_dot]
        class_name = arrow_extension_name[last_dot + 1:]

        try:
            module = importlib.import_module(module_path)
        except ImportError as exc:
            raise ValueError(
                f"Cannot import module {module_path!r} to reconstruct "
                f"{arrow_extension_name!r}: {exc}"
            ) from exc

        try:
            imported_class = getattr(module, class_name)
        except AttributeError as exc:
            raise ValueError(
                f"Cannot find class {class_name!r} in module {module_path!r} "
                f"to reconstruct {arrow_extension_name!r}: {exc}"
            ) from exc

        if not dataclasses.is_dataclass(imported_class):
            raise ValueError(
                f"Imported class {arrow_extension_name!r} is not a Python dataclass. "
                f"Only @dataclass-decorated classes can be reconstructed by "
                f"DataclassHandlerFactory."
            )

        hints = get_type_hints(imported_class)
        field_converters: list[tuple[str, Callable[..., Any], Callable[..., Any]]] = []

        # Build converters from annotations; use storage_type from schema as-is.
        # Pass the write-path context (visited_types) via a fresh ResolutionContext
        # so that nested type resolution also participates in cycle detection.
        write_context = ResolutionContext(visited_types=frozenset({imported_class}))

        for field in dataclasses.fields(imported_class):
            if not field.init:
                continue
            annotation = hints[field.name]
            _, to_fn, from_fn = self._resolve_field(annotation, registry, write_context)
            field_converters.append((field.name, to_fn, from_fn))

        return DataclassLogicalType(
            arrow_extension_name, imported_class, storage_type, field_converters
        )
```

- [ ] **Step 4: Run write-path tests**

```bash
uv run pytest tests/test_extension_types/test_dataclass_handler.py -v
```

Expected: all existing + new tests PASS (the read-path tests are not yet written).

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/extension_types/dataclass_handler.py tests/test_extension_types/test_dataclass_handler.py
git commit -m "feat(extension_types): implement DataclassHandlerFactory write path with _resolve_field and nested dataclass support"
```

---

### Task 6: Cycle detection and unsupported field types

**Files:**
- Modify: `tests/test_extension_types/test_dataclass_handler.py`

**Important:** Cycle detection tests require module-level dataclass fixtures. With
`from __future__ import annotations` (PEP 563) in effect throughout the test file, all
annotations become strings at class-definition time. `typing.get_type_hints` resolves
those strings against the class's **module** globals — not the local function scope.
So a local `@dataclass class _SelfRef` whose annotation references `_SelfRef` by name
would raise `NameError` at resolution time, because `_SelfRef` is only in the function's
local scope, not in `tests.test_extension_types.test_dataclass_handler`'s globals.

The fix: add the cyclic fixtures to the **module-level fixtures block** at the top of the
test file (near `Flat`, `Inner`, `Outer`, etc.).

- [ ] **Step 1: Add cyclic fixtures to module-level fixtures block in `test_dataclass_handler.py`**

Add these four class definitions to the "Shared dataclass fixtures" section near the top
of the file, after `Outer`:

```python
# Cyclic fixtures — must be module-level so get_type_hints resolves the string
# annotations ('_SelfRef', '_IndirectB', '_IndirectA') in module globals.

@dataclasses.dataclass
class _SelfRef:
    value: int
    child: _SelfRef  # type: ignore[name-defined]  # PEP 563 → string; resolved at get_type_hints time


@dataclasses.dataclass
class _IndirectA:
    value: int
    b: _IndirectB  # type: ignore[name-defined]  # forward ref; _IndirectB defined below


@dataclasses.dataclass
class _IndirectB:
    a: _IndirectA
```

With `from __future__ import annotations`:
- `_SelfRef.child: _SelfRef` → stored as string `'_SelfRef'`; `get_type_hints` resolves it to
  the class object because `_SelfRef` is in module globals by call time.
- `_IndirectA.b: _IndirectB` → string `'_IndirectB'`; `_IndirectB` defined below in the same
  module, so it's in globals by call time. `_IndirectB.a: _IndirectA` → string `'_IndirectA'`;
  also in globals.

- [ ] **Step 2: Write cycle detection and bad-type tests**

Append to `tests/test_extension_types/test_dataclass_handler.py`:

```python
# ---------------------------------------------------------------------------
# Cycle detection (write path)
# ---------------------------------------------------------------------------

def test_self_referential_dataclass_raises_type_error():
    """A dataclass with a self-referential field raises TypeError."""
    from orcapod.extension_types.dataclass_handler import DataclassHandlerFactory
    factory = DataclassHandlerFactory()
    with pytest.raises(TypeError, match="[Cc]ircular"):
        factory.create_for_python_type(_SelfRef)


def test_indirect_cycle_raises_type_error():
    """An A → B → A cycle raises TypeError."""
    from orcapod.extension_types.dataclass_handler import DataclassHandlerFactory
    factory = DataclassHandlerFactory()
    with pytest.raises(TypeError, match="[Cc]ircular"):
        factory.create_for_python_type(_IndirectA)


# ---------------------------------------------------------------------------
# Unsupported field types
# ---------------------------------------------------------------------------

# NOTE: We use uuid.UUID here because `uuid` is imported at module level.
# With `from __future__ import annotations`, `u: uuid.UUID` becomes the string
# `'uuid.UUID'`, which get_type_hints resolves via the module's globals where
# `uuid` IS present. Using pathlib.Path would fail unless pathlib is also
# imported at module level.

def test_unsupported_field_type_raises_type_error():
    """A field annotated with an unsupported type (uuid.UUID) raises TypeError."""
    from orcapod.extension_types.dataclass_handler import DataclassHandlerFactory

    @dataclasses.dataclass
    class _Bad:
        u: uuid.UUID

    factory = DataclassHandlerFactory()
    with pytest.raises(TypeError, match="[Uu]nsupported"):
        factory.create_for_python_type(_Bad)


def test_unsupported_field_type_error_mentions_annotation():
    """TypeError message names the unsupported annotation."""
    from orcapod.extension_types.dataclass_handler import DataclassHandlerFactory

    @dataclasses.dataclass
    class _Bad:
        u: uuid.UUID

    factory = DataclassHandlerFactory()
    with pytest.raises(TypeError) as exc_info:
        factory.create_for_python_type(_Bad)
    assert "UUID" in str(exc_info.value)
```

- [ ] **Step 3: Run tests to confirm they pass (cycle detection is already implemented)**

```bash
uv run pytest tests/test_extension_types/test_dataclass_handler.py -v -k "cycle or unsupported"
```

Expected: all PASS (the implementation already covers these paths).

- [ ] **Step 4: Commit**

```bash
git add tests/test_extension_types/test_dataclass_handler.py
git commit -m "test(extension_types): add cycle detection and unsupported field type tests for DataclassHandlerFactory"
```

---

### Task 7: Read path (`reconstruct_from_arrow`) tests

**Files:**
- Modify: `tests/test_extension_types/test_dataclass_handler.py`

- [ ] **Step 1: Write read-path tests**

Append to `tests/test_extension_types/test_dataclass_handler.py`:

```python
# ---------------------------------------------------------------------------
# Read path — reconstruct_from_arrow
# ---------------------------------------------------------------------------

def test_reconstruct_from_arrow_returns_dataclass_logical_type():
    from orcapod.extension_types.dataclass_handler import DataclassHandlerFactory, DataclassLogicalType
    factory = DataclassHandlerFactory()
    fqcn = f"{Flat.__module__}.{Flat.__qualname__}"
    storage = pa.struct([pa.field("x", pa.int64()), pa.field("y", pa.large_string())])
    lt = factory.reconstruct_from_arrow(fqcn, storage, {"category": "orcapod.dataclass"})
    assert isinstance(lt, DataclassLogicalType)
    assert lt.python_type is Flat
    assert lt.logical_type_name == fqcn


def test_reconstruct_from_arrow_converters_work():
    from orcapod.extension_types.dataclass_handler import DataclassHandlerFactory
    factory = DataclassHandlerFactory()
    fqcn = f"{Flat.__module__}.{Flat.__qualname__}"
    storage = pa.struct([pa.field("x", pa.int64()), pa.field("y", pa.large_string())])
    lt = factory.reconstruct_from_arrow(fqcn, storage, {"category": "orcapod.dataclass"})
    original = Flat(x=5, y="test")
    assert lt.storage_to_python(lt.python_to_storage(original)) == original


def test_reconstruct_from_arrow_uses_schema_storage_type():
    """reconstruct_from_arrow uses the storage_type from the schema, not re-derived."""
    from orcapod.extension_types.dataclass_handler import DataclassHandlerFactory
    factory = DataclassHandlerFactory()
    fqcn = f"{Flat.__module__}.{Flat.__qualname__}"
    # Intentionally use storage_type from the schema
    storage = pa.struct([pa.field("x", pa.int64()), pa.field("y", pa.large_string())])
    lt = factory.reconstruct_from_arrow(fqcn, storage, {"category": "orcapod.dataclass"})
    assert lt.get_arrow_extension_type().storage_type == storage


def test_reconstruct_from_arrow_bad_module_raises_value_error():
    from orcapod.extension_types.dataclass_handler import DataclassHandlerFactory
    factory = DataclassHandlerFactory()
    storage = pa.struct([pa.field("x", pa.int64())])
    with pytest.raises(ValueError, match="[Cc]annot import"):
        factory.reconstruct_from_arrow(
            "no.such.module.Foo", storage, {"category": "orcapod.dataclass"}
        )


def test_reconstruct_from_arrow_bad_class_name_raises_value_error():
    from orcapod.extension_types.dataclass_handler import DataclassHandlerFactory
    # Use a valid module but nonexistent class
    factory = DataclassHandlerFactory()
    storage = pa.struct([pa.field("x", pa.int64())])
    with pytest.raises(ValueError, match="[Cc]annot find"):
        factory.reconstruct_from_arrow(
            "builtins.NoSuchClass", storage, {"category": "orcapod.dataclass"}
        )


def test_reconstruct_from_arrow_non_dataclass_raises_value_error():
    from orcapod.extension_types.dataclass_handler import DataclassHandlerFactory
    factory = DataclassHandlerFactory()
    # str is a builtin — definitely not a dataclass
    storage = pa.struct([pa.field("x", pa.int64())])
    with pytest.raises(ValueError, match="not a.*dataclass"):
        factory.reconstruct_from_arrow(
            "builtins.str", storage, {"category": "orcapod.dataclass"}
        )


def test_reconstruct_from_arrow_no_dot_in_fqcn_raises_value_error():
    from orcapod.extension_types.dataclass_handler import DataclassHandlerFactory
    factory = DataclassHandlerFactory()
    storage = pa.struct([])
    with pytest.raises(ValueError, match="no module separator"):
        factory.reconstruct_from_arrow(
            "NoDotInName", storage, {"category": "orcapod.dataclass"}
        )


def test_reconstruct_from_arrow_cycle_detection():
    from orcapod.extension_types.dataclass_handler import DataclassHandlerFactory
    factory = DataclassHandlerFactory()
    fqcn = f"{Flat.__module__}.{Flat.__qualname__}"
    storage = pa.struct([pa.field("x", pa.int64()), pa.field("y", pa.large_string())])
    ctx = ResolutionContext(visited_arrow_names=frozenset({fqcn}))
    with pytest.raises(ValueError, match="[Cc]ircular"):
        factory.reconstruct_from_arrow(
            fqcn, storage, {"category": "orcapod.dataclass"}, context=ctx
        )
```

- [ ] **Step 2: Run read-path tests**

```bash
uv run pytest tests/test_extension_types/test_dataclass_handler.py -v -k "reconstruct"
```

Expected: all PASS (the `reconstruct_from_arrow` implementation was already added in Task 5).

- [ ] **Step 3: Commit**

```bash
git add tests/test_extension_types/test_dataclass_handler.py
git commit -m "test(extension_types): add read path tests for DataclassHandlerFactory.reconstruct_from_arrow"
```

---

### Task 8: Arrow array round-trip and full-suite verification

**Files:**
- Modify: `tests/test_extension_types/test_dataclass_handler.py`

- [ ] **Step 1: Write Arrow array round-trip test**

Append to `tests/test_extension_types/test_dataclass_handler.py`:

```python
# ---------------------------------------------------------------------------
# Arrow array round-trip
# ---------------------------------------------------------------------------

def test_arrow_struct_array_round_trip_flat():
    """Build a PyArrow struct array from storage dicts; verify round-trip via storage_to_python."""
    from orcapod.extension_types.dataclass_handler import DataclassHandlerFactory
    factory = DataclassHandlerFactory()
    lt = factory.create_for_python_type(Flat)

    instances = [Flat(x=1, y="a"), Flat(x=2, y="b"), Flat(x=3, y="c")]
    storage_dicts = [lt.python_to_storage(inst) for inst in instances]

    struct_arr = pa.array(storage_dicts, type=lt.get_arrow_extension_type().storage_type)
    results = [lt.storage_to_python(struct_arr[i].as_py()) for i in range(len(struct_arr))]
    assert results == instances


def test_arrow_struct_array_round_trip_nested():
    """Nested dataclass round-trips through Arrow struct array correctly."""
    from orcapod.extension_types.dataclass_handler import DataclassHandlerFactory
    factory = DataclassHandlerFactory()
    lt = factory.create_for_python_type(Outer)

    instances = [Outer(inner=Inner(a=10), z="x"), Outer(inner=Inner(a=20), z="y")]
    storage_dicts = [lt.python_to_storage(inst) for inst in instances]
    struct_arr = pa.array(storage_dicts, type=lt.get_arrow_extension_type().storage_type)
    results = [lt.storage_to_python(struct_arr[i].as_py()) for i in range(len(struct_arr))]
    assert results == instances


def test_arrow_struct_array_round_trip_with_list():
    """list[T] field round-trips through Arrow list array correctly."""
    from orcapod.extension_types.dataclass_handler import DataclassHandlerFactory
    factory = DataclassHandlerFactory()
    lt = factory.create_for_python_type(WithList)

    instances = [WithList(items=[1, 2]), WithList(items=[]), WithList(items=[9])]
    storage_dicts = [lt.python_to_storage(inst) for inst in instances]
    struct_arr = pa.array(storage_dicts, type=lt.get_arrow_extension_type().storage_type)
    results = [lt.storage_to_python(struct_arr[i].as_py()) for i in range(len(struct_arr))]
    assert results == instances


# ---------------------------------------------------------------------------
# ResolutionContext propagation across factory boundary
# ---------------------------------------------------------------------------

def test_resolution_context_cycle_across_factories():
    """Demonstrates that a context with visited_types from another factory scope
    propagates correctly into DataclassHandlerFactory.create_for_python_type."""
    from orcapod.extension_types.dataclass_handler import DataclassHandlerFactory

    @dataclasses.dataclass
    class _X:
        n: int

    factory = DataclassHandlerFactory()
    # Pre-populate context as if another factory already put _X in visited_types
    ctx = ResolutionContext(visited_types=frozenset({_X}))
    with pytest.raises(TypeError, match="[Cc]ircular"):
        factory.create_for_python_type(_X, context=ctx)
```

- [ ] **Step 2: Run all Arrow round-trip tests**

```bash
uv run pytest tests/test_extension_types/test_dataclass_handler.py -v -k "arrow or round_trip or context_cycle"
```

Expected: all PASS.

- [ ] **Step 3: Run the complete test suite**

```bash
uv run pytest tests/ -v
```

Expected: all tests PASS. No regressions.

- [ ] **Step 4: Commit**

```bash
git add tests/test_extension_types/test_dataclass_handler.py
git commit -m "test(extension_types): add Arrow array round-trip and ResolutionContext propagation tests"
```

---

## Post-implementation checklist

- [ ] All tests pass: `uv run pytest tests/ -v`
- [ ] `DataclassHandlerFactory` satisfies `LogicalTypeFactoryProtocol` via isinstance
- [ ] `DataclassLogicalType` satisfies `LogicalTypeProtocol` via isinstance
- [ ] Spec coverage:
  - [x] `ResolutionContext` frozen dataclass (Task 1)
  - [x] `supports_class` on `LogicalTypeFactoryProtocol` (Task 2)
  - [x] Optional `registry` + `context` params on factory methods (Task 2)
  - [x] `_python_class_factories` as list-per-base with `supports_class` dispatch (Task 3)
  - [x] `_python_class_cache` for fast factory re-lookup (Task 3)
  - [x] `ensure_extension_type` forwards `registry=self, context=ResolutionContext()` (Task 3)
  - [x] `DataclassLogicalType` with baked-in field converters (Task 4)
  - [x] `DataclassHandlerFactory._resolve_field` for all supported types (Task 5)
  - [x] Write path with primitives, `list[T]`, nested dataclass, cycle detection (Tasks 5, 6)
  - [x] Read path `reconstruct_from_arrow` with FQCN import, error cases, cycle detection (Task 7)
  - [x] Arrow array round-trip (Task 8)
  - [x] `ResolutionContext` propagation across factory boundaries (Task 8)
