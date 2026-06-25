# PLT-1672: Write-Side Logical Type Factory Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Python-class-keyed write-side factory dispatch to `LogicalTypeRegistry` and wire it into `UniversalTypeConverter` and `_FunctionPodBase` so that unregistered Python types are auto-registered via a factory at function pod declaration time.

**Architecture:** Two new factory dispatch axes (category-keyed for reads, python-class-keyed for writes) are unified in `LogicalTypeRegistry`'s `ensure_logical_type_for_python_class` with a shared MRO resolution algorithm. A recursive `_extract_leaf_classes` unwrapper in a new `type_utils.py` feeds the write-side trigger in `_FunctionPodBase.__init__`. `UniversalTypeConverter` is extended with a one-line priority check so registered extension types take precedence over the old shape-based system at encoding time.

**Tech Stack:** Python 3.12+, PyArrow, Polars, `typing.get_origin`/`get_args` for generic annotation unwrapping. All tests via `uv run pytest`.

---

## File Map

| File | Action | Responsibility |
|---|---|---|
| `src/orcapod/extension_types/protocols.py` | Modify | Rename `create_logical_type` → `reconstruct_from_arrow`; add `create_for_python_type` |
| `src/orcapod/extension_types/registry.py` | Modify | Rename `_factories` → `_category_factories`; add `_python_class_factories`; extend `register_logical_type_factory`; add `ensure_logical_type_for_python_class` |
| `src/orcapod/extension_types/type_utils.py` | Create | `_extract_leaf_classes(annotation)` — recursive generic annotation unwrapper |
| `src/orcapod/extension_types/__init__.py` | Modify | Export `_extract_leaf_classes` |
| `src/orcapod/semantic_types/universal_converter.py` | Modify | Add `_logical_type_registry` attribute; insert priority check before `semantic_registry` in `_convert_python_to_arrow` |
| `src/orcapod/contexts/core.py` | Modify | Add `DataContext.__post_init__` to wire `logical_type_registry` into `type_converter` |
| `src/orcapod/core/function_pod.py` | Modify | Add `_ARROW_NATIVE_TYPES`, `_trigger_write_side_registration`; call from `_FunctionPodBase.__init__` |
| `tests/test_extension_types/test_protocols.py` | Modify | Update `_StubFactory` stub; add `create_for_python_type` conformance test |
| `tests/test_extension_types/test_registry.py` | Modify | Update all `register_logical_type_factory` call sites; add `ensure_logical_type_for_python_class` tests |
| `tests/test_extension_types/test_type_utils.py` | Create | Tests for `_extract_leaf_classes` |
| `tests/test_semantic_types/test_universal_converter.py` | Modify | Add `_logical_type_registry` priority check tests |
| `tests/test_core/function_pod/test_write_side_registration.py` | Create | End-to-end pod-declaration trigger tests |

---

## Task 1: Rename `create_logical_type` → `reconstruct_from_arrow` in `LogicalTypeFactoryProtocol`

**Files:**
- Modify: `src/orcapod/extension_types/protocols.py`
- Modify: `src/orcapod/extension_types/registry.py` (call site)
- Modify: `tests/test_extension_types/test_protocols.py`
- Modify: `tests/test_extension_types/test_registry.py` (all uses of `create_logical_type`)

- [ ] **Step 1: Update `_StubFactory` in test_protocols.py to use the new name**

Edit `tests/test_extension_types/test_protocols.py`. Replace the `_StubFactory` class body:

```python
class _StubFactory:
    """Minimal conforming implementation of LogicalTypeFactoryProtocol for use in tests."""

    def reconstruct_from_arrow(self, arrow_extension_name, storage_type, metadata):
        return _StubLogicalType()
```

Also update `test_logical_type_factory_create_returns_logical_type` to call `reconstruct_from_arrow`:

```python
def test_logical_type_factory_create_returns_logical_type():
    """A conforming factory returns a LogicalTypeProtocol from reconstruct_from_arrow."""
    from orcapod.extension_types.protocols import LogicalTypeFactoryProtocol, LogicalTypeProtocol
    factory: LogicalTypeFactoryProtocol = _StubFactory()
    result = factory.reconstruct_from_arrow(
        "test.ext", pa.large_utf8(), {"category": "Test"}
    )
    assert isinstance(result, LogicalTypeProtocol)
```

- [ ] **Step 2: Run the conformance test to confirm it fails (Protocol still expects `create_logical_type`)**

```bash
uv run pytest tests/test_extension_types/test_protocols.py::test_logical_type_factory_conforming_class_satisfies_protocol -v
```

Expected: FAIL — `_StubFactory` is no longer recognized as `LogicalTypeFactoryProtocol` because it lacks `create_logical_type`.

- [ ] **Step 3: Rename the method in `LogicalTypeFactoryProtocol`**

In `src/orcapod/extension_types/protocols.py`, rename `create_logical_type` to `reconstruct_from_arrow` in the `LogicalTypeFactoryProtocol` class. The full updated method:

```python
    def reconstruct_from_arrow(
        self,
        arrow_extension_name: str,
        storage_type: pa.DataType,
        metadata: dict[str, Any],
    ) -> LogicalTypeProtocol:
        """Reconstruct a LogicalType from Arrow schema metadata (read path).

        Called by the registry when a schema walk encounters an extension type
        whose metadata ``"category"`` value matches this factory's registered
        category. All Arrow schema information is already known.

        Args:
            arrow_extension_name: The Arrow extension type name from the schema.
            storage_type: The underlying Arrow storage type.
            metadata: Full parsed metadata JSON dict. Always contains ``"category"``.

        Returns:
            A fully constructed ``LogicalTypeProtocol`` ready for registration.

        Raises:
            ValueError: If this factory cannot reconstruct a type for the given name.
        """
        ...
```

- [ ] **Step 4: Update the call site in `registry.py`**

In `src/orcapod/extension_types/registry.py`, find `ensure_extension_type`. Replace:

```python
        logical_type = factory.create_logical_type(
            arrow_extension_name, storage_type, metadata_dict
        )
```

with:

```python
        logical_type = factory.reconstruct_from_arrow(
            arrow_extension_name, storage_type, metadata_dict
        )
```

- [ ] **Step 5: Update `_make_stub_factory` in `test_registry.py`**

In `tests/test_extension_types/test_registry.py`, find `_make_stub_factory`. Replace `create_logical_type` with `reconstruct_from_arrow` in the inner `_Factory` class:

```python
        def reconstruct_from_arrow(self, arrow_extension_name, storage_type, metadata):
            self.calls.append((arrow_extension_name, storage_type, metadata))
            if _return_lt is not None:
                return _return_lt
            return _make_stub(arrow_name=arrow_extension_name, storage=storage_type)
```

- [ ] **Step 6: Run the full test suite for extension_types to confirm all pass**

```bash
uv run pytest tests/test_extension_types/ -v
```

Expected: All previously passing tests still pass.

- [ ] **Step 7: Commit**

```bash
git add src/orcapod/extension_types/protocols.py \
        src/orcapod/extension_types/registry.py \
        tests/test_extension_types/test_protocols.py \
        tests/test_extension_types/test_registry.py
git commit -m "refactor(extension_types): rename create_logical_type to reconstruct_from_arrow in LogicalTypeFactoryProtocol"
```

---

## Task 2: Add `create_for_python_type` to `LogicalTypeFactoryProtocol`

**Files:**
- Modify: `src/orcapod/extension_types/protocols.py`
- Modify: `tests/test_extension_types/test_protocols.py`

- [ ] **Step 1: Write the failing conformance test**

Add to `tests/test_extension_types/test_protocols.py`. First update `_StubFactory` to add the new method:

```python
class _StubFactory:
    """Minimal conforming implementation of LogicalTypeFactoryProtocol for use in tests."""

    def reconstruct_from_arrow(self, arrow_extension_name, storage_type, metadata):
        return _StubLogicalType()

    def create_for_python_type(self, python_type):
        return _StubLogicalType()
```

Then add the test:

```python
def test_factory_create_for_python_type_conformance():
    """A conforming factory implements create_for_python_type and returns LogicalTypeProtocol."""
    from orcapod.extension_types.protocols import LogicalTypeFactoryProtocol, LogicalTypeProtocol
    factory: LogicalTypeFactoryProtocol = _StubFactory()
    assert isinstance(factory, LogicalTypeFactoryProtocol)
    result = factory.create_for_python_type(str)
    assert isinstance(result, LogicalTypeProtocol)
```

- [ ] **Step 2: Run to confirm it fails (Protocol does not yet require `create_for_python_type`)**

```bash
uv run pytest tests/test_extension_types/test_protocols.py::test_factory_create_for_python_type_conformance -v
```

Expected: FAIL — `LogicalTypeFactoryProtocol` does not yet define `create_for_python_type`, so the `isinstance` check passes but calling an undefined method would fail; or the test passes vacuously — either way, add the method to the Protocol so it becomes structurally required.

- [ ] **Step 3: Add `create_for_python_type` to `LogicalTypeFactoryProtocol` in `protocols.py`**

```python
    def create_for_python_type(
        self,
        python_type: type,
    ) -> LogicalTypeProtocol:
        """Synthesize a LogicalType for the given Python class (write path).

        Called by the registry when pod declaration encounters an unregistered
        class whose MRO intersects this factory's registered ``python_bases``.
        The factory derives all Arrow metadata (extension name, storage type,
        metadata dict) from the Python class itself.

        The returned LogicalType must round-trip: the extension name and metadata
        it produces must route back to this same factory's ``reconstruct_from_arrow``
        on a subsequent read.

        Args:
            python_type: The concrete Python class to synthesize a LogicalType for.

        Returns:
            A fully constructed ``LogicalTypeProtocol`` ready for registration.

        Raises:
            ValueError: If this factory cannot construct a type for the given class.
        """
        ...
```

- [ ] **Step 4: Run to confirm the test passes**

```bash
uv run pytest tests/test_extension_types/test_protocols.py -v
```

Expected: All pass.

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/extension_types/protocols.py \
        tests/test_extension_types/test_protocols.py
git commit -m "feat(extension_types): add create_for_python_type to LogicalTypeFactoryProtocol"
```

---

## Task 3: Extend `LogicalTypeRegistry` internals and `register_logical_type_factory`

**Files:**
- Modify: `src/orcapod/extension_types/registry.py`
- Modify: `tests/test_extension_types/test_registry.py`

This task renames `_factories` → `_category_factories`, adds `_python_class_factories`, changes the `register_logical_type_factory` signature, and updates all existing call sites.

- [ ] **Step 1: Write the new `register_logical_type_factory` tests**

Add to `tests/test_extension_types/test_registry.py`:

```python
# ── register_logical_type_factory extended API ───────────────────────────────

def test_register_logical_type_factory_keyword_category():
    """register_logical_type_factory accepts factory as first arg, category as keyword."""
    registry = LogicalTypeRegistry()
    factory = _make_stub_factory()
    registry.register_logical_type_factory(factory, category="TestCat")  # no error


def test_register_logical_type_factory_keyword_python_bases():
    """register_logical_type_factory accepts python_bases as keyword."""
    registry = LogicalTypeRegistry()
    factory = _make_stub_factory()
    registry.register_logical_type_factory(factory, python_bases=[str])  # no error


def test_register_logical_type_factory_both_axes():
    """register_logical_type_factory accepts both category and python_bases."""
    registry = LogicalTypeRegistry()
    factory = _make_stub_factory()
    registry.register_logical_type_factory(factory, category="Cat", python_bases=[str, int])


def test_register_logical_type_factory_no_axes_raises():
    """register_logical_type_factory raises ValueError when called with no axes."""
    registry = LogicalTypeRegistry()
    factory = _make_stub_factory()
    with pytest.raises(ValueError, match="At least one of"):
        registry.register_logical_type_factory(factory)


def test_register_logical_type_factory_python_base_duplicate_different_factory_raises():
    """Registering a different factory for the same python_base raises ValueError."""
    registry = LogicalTypeRegistry()
    f1 = _make_stub_factory()
    f2 = _make_stub_factory()
    registry.register_logical_type_factory(f1, python_bases=[str])
    with pytest.raises(ValueError):
        registry.register_logical_type_factory(f2, python_bases=[str])


def test_register_logical_type_factory_python_base_same_factory_idempotent():
    """Registering the same factory twice for the same python_base is a no-op."""
    registry = LogicalTypeRegistry()
    factory = _make_stub_factory()
    registry.register_logical_type_factory(factory, python_bases=[str])
    registry.register_logical_type_factory(factory, python_bases=[str])  # no error
```

- [ ] **Step 2: Run to confirm new tests fail**

```bash
uv run pytest tests/test_extension_types/test_registry.py -k "keyword_category or keyword_python_bases or both_axes or no_axes or python_base" -v
```

Expected: FAIL — `register_logical_type_factory` currently takes `(category, factory)` positionally.

- [ ] **Step 3: Update existing `register_logical_type_factory` call sites in test_registry.py**

Search for all existing calls to `register_logical_type_factory` that use the old positional signature `(category, factory)` and update them to the new keyword form `(factory, category=...)`.

Run this to find them:
```bash
grep -n "register_logical_type_factory" tests/test_extension_types/test_registry.py
```

For each occurrence of the form `registry.register_logical_type_factory("SomeCategory", factory)`, replace with `registry.register_logical_type_factory(factory, category="SomeCategory")`.

- [ ] **Step 4: Update `_make_stub_factory` to also add `create_for_python_type`**

In `test_registry.py`, update `_make_stub_factory` so the inner `_Factory` class also implements `create_for_python_type` (required by the updated `LogicalTypeFactoryProtocol`):

```python
def _make_stub_factory(return_lt: LogicalTypeProtocol | None = None) -> LogicalTypeFactoryProtocol:
    """Factory for minimal LogicalTypeFactoryProtocol conforming stubs."""
    _return_lt = return_lt

    class _Factory:
        def __init__(self):
            self.calls: list[tuple] = []
            self.python_type_calls: list[type] = []

        def reconstruct_from_arrow(self, arrow_extension_name, storage_type, metadata):
            self.calls.append((arrow_extension_name, storage_type, metadata))
            if _return_lt is not None:
                return _return_lt
            return _make_stub(arrow_name=arrow_extension_name, storage=storage_type)

        def create_for_python_type(self, python_type):
            self.python_type_calls.append(python_type)
            if _return_lt is not None:
                return _return_lt
            return _make_stub(py_type=python_type)

    return _Factory()
```

- [ ] **Step 5: Implement the changes in `registry.py`**

In `LogicalTypeRegistry.__init__`, rename `_factories` → `_category_factories` and add `_python_class_factories`:

```python
    def __init__(self, logical_types: list[LogicalTypeProtocol] | None = None) -> None:
        self._by_logical_name: dict[str, LogicalTypeProtocol] = {}
        self._by_arrow_name: dict[str, LogicalTypeProtocol] = {}
        self._by_python_type: dict[type, LogicalTypeProtocol] = {}
        self._category_factories: dict[str, LogicalTypeFactoryProtocol] = {}
        self._python_class_factories: dict[type, LogicalTypeFactoryProtocol] = {}
        for lt in (logical_types or []):
            self.register_logical_type(lt)
```

Replace `register_logical_type_factory` with the new signature. Find the existing method and replace it entirely:

```python
    def register_logical_type_factory(
        self,
        factory: LogicalTypeFactoryProtocol,
        *,
        category: str | None = None,
        python_bases: Iterable[type] = (),
    ) -> None:
        """Register a factory on one or both dispatch axes.

        Args:
            factory: The factory to register.
            category: If given, registers factory as the read-side handler for Arrow
                extension types whose metadata contains this category string. Raises
                ``ValueError`` if a different factory is already registered for this
                category.
            python_bases: Zero or more Python base classes. Registers factory as the
                write-side handler for each. Raises ``ValueError`` if a different
                factory is already registered for a given base.

        Raises:
            ValueError: If neither ``category`` nor ``python_bases`` is provided.
            ValueError: If a different factory is already registered for a given key.
        """
        if category is None and not python_bases:
            raise ValueError(
                "At least one of 'category' or 'python_bases' must be provided."
            )
        if category is not None:
            existing = self._category_factories.get(category)
            if existing is not None and existing is not factory:
                raise ValueError(
                    f"Cannot register factory for category {category!r}: "
                    f"a different factory is already registered for this category."
                )
            if existing is not factory:
                self._category_factories[category] = factory
                logger.debug(
                    "registered LogicalTypeFactory for category %r: %r", category, factory
                )
        for base in python_bases:
            existing = self._python_class_factories.get(base)
            if existing is not None and existing is not factory:
                raise ValueError(
                    f"Cannot register factory for python base {base!r}: "
                    f"a different factory is already registered for this base."
                )
            if existing is not factory:
                self._python_class_factories[base] = factory
                logger.debug(
                    "registered LogicalTypeFactory for python base %r: %r", base, factory
                )
```

Also update the `ensure_extension_type` method: replace any reference to `self._factories` with `self._category_factories`.

- [ ] **Step 6: Run all registry tests to confirm they pass**

```bash
uv run pytest tests/test_extension_types/test_registry.py -v
```

Expected: All pass. (Any test using the old positional signature was updated in Step 3.)

- [ ] **Step 7: Run the full extension_types test suite**

```bash
uv run pytest tests/test_extension_types/ -v
```

Expected: All pass.

- [ ] **Step 8: Commit**

```bash
git add src/orcapod/extension_types/registry.py \
        tests/test_extension_types/test_registry.py
git commit -m "feat(extension_types): add python_class_factories axis to LogicalTypeRegistry; extend register_logical_type_factory"
```

---

## Task 4: Add `ensure_logical_type_for_python_class` to `LogicalTypeRegistry`

**Files:**
- Modify: `src/orcapod/extension_types/registry.py`
- Modify: `tests/test_extension_types/test_registry.py`

- [ ] **Step 1: Write all failing tests for `ensure_logical_type_for_python_class`**

Add this block to `tests/test_extension_types/test_registry.py`:

```python
# ── ensure_logical_type_for_python_class tests ───────────────────────────────

class _A:
    pass


class _B(_A):
    pass


class _C(_B):
    pass


def test_ensure_for_python_class_concrete_exact_match():
    """Returns the concrete LogicalType when exact Python type is registered."""
    registry = LogicalTypeRegistry()
    lt = _make_stub(py_type=_A)
    registry.register_logical_type(lt)
    result = registry.ensure_logical_type_for_python_class(_A)
    assert result is lt


def test_ensure_for_python_class_concrete_mro_match():
    """Returns concrete LogicalType registered for a parent class via MRO walk."""
    registry = LogicalTypeRegistry()
    lt = _make_stub(py_type=_A)
    registry.register_logical_type(lt)
    result = registry.ensure_logical_type_for_python_class(_C)
    assert result is lt


def test_ensure_for_python_class_factory_synthesis():
    """Calls factory.create_for_python_type and registers the result."""
    registry = LogicalTypeRegistry()
    factory = _make_stub_factory()
    registry.register_logical_type_factory(factory, python_bases=[_A])
    result = registry.ensure_logical_type_for_python_class(_C)
    assert len(factory.python_type_calls) == 1
    assert factory.python_type_calls[0] is _C
    # Synthesized type is now registered — second call hits cache
    cached = registry.ensure_logical_type_for_python_class(_C)
    assert cached is result
    assert len(factory.python_type_calls) == 1  # factory NOT called again


def test_ensure_for_python_class_concrete_beats_factory_same_mro_level():
    """When concrete type and factory are registered for the same class, concrete wins."""
    registry = LogicalTypeRegistry()
    lt = _make_stub(py_type=_A)
    registry.register_logical_type(lt)
    factory = _make_stub_factory()
    registry.register_logical_type_factory(factory, python_bases=[_A])
    result = registry.ensure_logical_type_for_python_class(_A)
    assert result is lt
    assert len(factory.python_type_calls) == 0  # factory never called


def test_ensure_for_python_class_factory_more_specific_than_concrete():
    """Factory registered for a subclass beats concrete registered for a parent."""
    registry = LogicalTypeRegistry()
    lt_a = _make_stub(py_type=_A)
    registry.register_logical_type(lt_a)  # concrete for _A
    factory = _make_stub_factory()
    registry.register_logical_type_factory(factory, python_bases=[_B])  # factory for _B
    # Query _C: factory at _B (MRO index 1) beats concrete at _A (MRO index 2)
    result = registry.ensure_logical_type_for_python_class(_C)
    assert len(factory.python_type_calls) == 1
    assert factory.python_type_calls[0] is _C


def test_ensure_for_python_class_concrete_more_specific_than_factory():
    """Concrete registered for a subclass beats factory registered for a parent."""
    registry = LogicalTypeRegistry()
    factory = _make_stub_factory()
    registry.register_logical_type_factory(factory, python_bases=[_A])  # factory for _A
    lt_b = _make_stub(py_type=_B)
    registry.register_logical_type(lt_b)  # concrete for _B
    # Query _C: concrete at _B (MRO index 1) beats factory at _A (MRO index 2)
    result = registry.ensure_logical_type_for_python_class(_C)
    assert result is lt_b
    assert len(factory.python_type_calls) == 0


def test_ensure_for_python_class_abc_subclasshook():
    """issubclass fallback scan catches ABCs with __subclasshook__."""
    from abc import ABCMeta

    class _StructuralABC(metaclass=ABCMeta):
        @classmethod
        def __subclasshook__(cls, C):
            return hasattr(C, "_MARKER")

    class _MarkedClass:
        _MARKER = True

    registry = LogicalTypeRegistry()
    factory = _make_stub_factory()
    registry.register_logical_type_factory(factory, python_bases=[_StructuralABC])
    result = registry.ensure_logical_type_for_python_class(_MarkedClass)
    assert len(factory.python_type_calls) == 1
    assert factory.python_type_calls[0] is _MarkedClass


def test_ensure_for_python_class_no_match_raises_type_error():
    """TypeError raised when no LogicalType and no factory match the type."""
    registry = LogicalTypeRegistry()

    with pytest.raises(TypeError, match="No LogicalType or LogicalTypeFactory"):
        registry.ensure_logical_type_for_python_class(_C)
```

- [ ] **Step 2: Run to confirm all fail**

```bash
uv run pytest tests/test_extension_types/test_registry.py -k "ensure_for_python_class" -v
```

Expected: All FAIL with `AttributeError: 'LogicalTypeRegistry' has no attribute 'ensure_logical_type_for_python_class'`.

- [ ] **Step 3: Implement `ensure_logical_type_for_python_class` in `registry.py`**

Add the method to `LogicalTypeRegistry` after `ensure_extension_type`:

```python
    def ensure_logical_type_for_python_class(
        self,
        python_type: type,
    ) -> LogicalTypeProtocol:
        """Ensure a LogicalType exists for python_type, synthesizing via factory if needed.

        Resolution algorithm:
        1. Walk ``python_type.__mro__``. Track the first (most-specific) hit in
           ``_by_python_type`` (concrete) and ``_python_class_factories`` (factory)
           separately, recording the MRO index of each.
        2. After the MRO walk, if no factory was found, do a fallback ``issubclass``
           scan over ``_python_class_factories`` keys to catch ABCs with
           ``__subclasshook__``. Assign these the least-specific MRO index
           (len of __mro__) so they lose to any direct MRO match.
        3. Resolution rule: if both concrete and factory found, compare MRO indices —
           lower index wins. Ties (same class) → concrete wins.
        4. If factory wins (or only factory found): call
           ``factory.create_for_python_type(python_type)``, register the result,
           and return it. The registration caches it in ``_by_python_type[python_type]``.
        5. If nothing found: raise ``TypeError``.

        Args:
            python_type: The Python class to resolve.

        Returns:
            The registered or newly synthesized ``LogicalTypeProtocol``.

        Raises:
            TypeError: If no ``LogicalType`` and no factory is found.
        """
        best_concrete_idx: int | None = None
        best_concrete: LogicalTypeProtocol | None = None
        best_factory_idx: int | None = None
        best_factory: LogicalTypeFactoryProtocol | None = None

        # Step 1: Walk MRO
        for i, base in enumerate(python_type.__mro__):
            if best_concrete is None and base in self._by_python_type:
                best_concrete_idx = i
                best_concrete = self._by_python_type[base]
            if best_factory is None and base in self._python_class_factories:
                best_factory_idx = i
                best_factory = self._python_class_factories[base]
            if best_concrete is not None and best_factory is not None:
                break

        # Step 2: issubclass fallback scan for ABCs with __subclasshook__
        if best_factory is None:
            for base_class, factory in self._python_class_factories.items():
                try:
                    if issubclass(python_type, base_class):
                        best_factory = factory
                        # ABC match — less specific than any direct MRO hit
                        best_factory_idx = len(python_type.__mro__)
                        break
                except TypeError:
                    continue

        # Step 3: Resolution
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

        if best_factory is None:
            # Only concrete found
            assert best_concrete is not None
            return best_concrete

        if best_concrete is None:
            # Only factory found — synthesize
            assert best_factory is not None
            lt = best_factory.create_for_python_type(python_type)
            self.register_logical_type(lt)
            logger.debug(
                "ensure_logical_type_for_python_class: synthesized %r for %r",
                lt.logical_type_name,
                python_type,
            )
            return lt

        # Both found — compare specificity (lower MRO index = more specific)
        assert best_concrete_idx is not None
        assert best_factory_idx is not None
        if best_concrete_idx <= best_factory_idx:
            # Concrete is same level (ties → concrete wins) or more specific
            return best_concrete
        else:
            # Factory is more specific — synthesize
            lt = best_factory.create_for_python_type(python_type)
            self.register_logical_type(lt)
            logger.debug(
                "ensure_logical_type_for_python_class: synthesized %r for %r via more-specific factory",
                lt.logical_type_name,
                python_type,
            )
            return lt
```

- [ ] **Step 4: Run the new tests**

```bash
uv run pytest tests/test_extension_types/test_registry.py -k "ensure_for_python_class" -v
```

Expected: All pass.

- [ ] **Step 5: Run the full extension_types suite**

```bash
uv run pytest tests/test_extension_types/ -v
```

Expected: All pass.

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/extension_types/registry.py \
        tests/test_extension_types/test_registry.py
git commit -m "feat(extension_types): add ensure_logical_type_for_python_class with unified MRO resolution"
```

---

## Task 5: Add `_extract_leaf_classes` in `type_utils.py`

**Files:**
- Create: `src/orcapod/extension_types/type_utils.py`
- Modify: `src/orcapod/extension_types/__init__.py`
- Create: `tests/test_extension_types/test_type_utils.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_extension_types/test_type_utils.py`:

```python
"""Tests for extension_types.type_utils helpers."""

from __future__ import annotations

from typing import Optional, Union

from orcapod.extension_types.type_utils import _extract_leaf_classes


class _A:
    pass


class _B:
    pass


def test_plain_class():
    assert list(_extract_leaf_classes(int)) == [int]


def test_plain_custom_class():
    assert list(_extract_leaf_classes(_A)) == [_A]


def test_list_of_class():
    assert list(_extract_leaf_classes(list[int])) == [int]


def test_dict_of_classes():
    result = set(_extract_leaf_classes(dict[str, int]))
    assert result == {str, int}


def test_optional_unwraps_none():
    """Optional[X] yields X but not NoneType."""
    result = list(_extract_leaf_classes(Optional[int]))
    assert result == [int]


def test_union_yields_all_non_none():
    result = set(_extract_leaf_classes(Union[int, str]))
    assert result == {int, str}


def test_union_with_none_excludes_none():
    result = set(_extract_leaf_classes(Union[int, None]))
    assert type(None) not in result
    assert int in result


def test_nested_list_of_dict():
    """list[dict[_A, list[_B]]] yields _A and _B."""
    result = set(_extract_leaf_classes(list[dict[_A, list[_B]]]))
    assert result == {_A, _B}


def test_deeply_nested():
    """list[dict[str, list[dict[int, _A]]]] yields str, int, _A."""
    result = set(_extract_leaf_classes(list[dict[str, list[dict[int, _A]]]]))
    assert result == {str, int, _A}


def test_non_generic_non_type_is_skipped():
    """Annotations that are not types and not generic aliases yield nothing."""
    # e.g. a string annotation that failed resolution — should not crash
    result = list(_extract_leaf_classes("unresolved_string"))
    assert result == []


def test_none_type_plain():
    """type(None) itself yields type(None) as a leaf (not filtered at this level)."""
    result = list(_extract_leaf_classes(type(None)))
    assert result == [type(None)]
```

- [ ] **Step 2: Run to confirm all fail**

```bash
uv run pytest tests/test_extension_types/test_type_utils.py -v
```

Expected: All FAIL with `ModuleNotFoundError` or `ImportError`.

- [ ] **Step 3: Create `src/orcapod/extension_types/type_utils.py`**

```python
"""Utility helpers for Python type annotation inspection.

Used by the write-side registration trigger to extract leaf Python classes from
complex generic annotations like ``list[dict[A, list[B]]]``.
"""

from __future__ import annotations

import typing
from typing import Any, Iterator


def _extract_leaf_classes(annotation: Any) -> Iterator[type]:
    """Recursively yield all concrete leaf Python classes from a type annotation.

    Unwraps generic aliases (``list[T]``, ``dict[K, V]``, ``Optional[T]``,
    ``Union[A, B]``, etc.) using ``typing.get_origin`` and ``typing.get_args``
    and yields every non-generic leaf found. ``NoneType`` (from ``Optional``
    and ``Union[..., None]``) is yielded as-is — callers that want to skip it
    should filter on ``type(None)``.

    Non-type, non-generic values (e.g. unresolved string annotations) are
    silently skipped.

    Args:
        annotation: A Python type or generic alias to inspect.

    Yields:
        Concrete Python ``type`` objects found at leaf positions.

    Examples:
        >>> list(_extract_leaf_classes(list[int]))
        [<class 'int'>]
        >>> set(_extract_leaf_classes(dict[str, list[MyClass]]))
        {<class 'str'>, <class 'MyClass'>}
    """
    origin = typing.get_origin(annotation)

    if origin is None:
        # Not a generic alias. Yield only if it is a plain type.
        if isinstance(annotation, type):
            yield annotation
        return

    # Generic alias — recurse into every type argument.
    for arg in typing.get_args(annotation):
        yield from _extract_leaf_classes(arg)
```

- [ ] **Step 4: Export from `__init__.py`**

In `src/orcapod/extension_types/__init__.py`, add to the imports and `__all__`:

```python
from .type_utils import _extract_leaf_classes
```

And add `"_extract_leaf_classes"` to `__all__`.

- [ ] **Step 5: Run to confirm all tests pass**

```bash
uv run pytest tests/test_extension_types/test_type_utils.py -v
```

Expected: All pass.

- [ ] **Step 6: Run the full extension_types suite**

```bash
uv run pytest tests/test_extension_types/ -v
```

Expected: All pass.

- [ ] **Step 7: Commit**

```bash
git add src/orcapod/extension_types/type_utils.py \
        src/orcapod/extension_types/__init__.py \
        tests/test_extension_types/test_type_utils.py
git commit -m "feat(extension_types): add _extract_leaf_classes for recursive generic annotation unwrapping"
```

---

## Task 6: Wire `LogicalTypeRegistry` into `UniversalTypeConverter` and `DataContext`

**Files:**
- Modify: `src/orcapod/semantic_types/universal_converter.py`
- Modify: `src/orcapod/contexts/core.py`
- Modify: `tests/test_semantic_types/test_universal_converter.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_semantic_types/test_universal_converter.py`:

```python
# ── LogicalTypeRegistry priority tests ───────────────────────────────────────

import pyarrow as pa
import polars as pl

from orcapod.extension_types.registry import (
    LogicalTypeRegistry,
    make_arrow_extension_type,
    make_polars_extension_type,
)
from orcapod.semantic_types.universal_converter import UniversalTypeConverter


def _make_logical_type_stub(py_type: type, arrow_name: str) -> object:
    """Return a minimal LogicalTypeProtocol conforming stub."""
    _ArrowExtClass = make_arrow_extension_type(arrow_name, pa.large_string())
    _pl_dtype = pl.String

    class _PolarsExt(pl.BaseExtension):
        def __init__(self):
            super().__init__(arrow_name, _pl_dtype, None)
        @classmethod
        def ext_from_params(cls, ext_name, storage_dtype, metadata_str):
            return cls()

    class _Stub:
        logical_type_name = arrow_name
        python_type = py_type

        def get_arrow_extension_type(self):
            return _ArrowExtClass()

        def get_polars_extension_type(self):
            return _PolarsExt()

        def python_to_storage(self, value):
            return str(value)

        def storage_to_python(self, storage_value):
            return storage_value

    return _Stub()


class _MyCustomClass:
    pass


def test_converter_uses_logical_type_registry_for_registered_type():
    """When a LogicalType is registered, converter returns its Arrow extension type."""
    import uuid as _uuid
    arrow_name = f"test.MyCustomClass.{_uuid.uuid4().hex[:8]}"
    lt = _make_logical_type_stub(_MyCustomClass, arrow_name)

    registry = LogicalTypeRegistry()
    registry.register_logical_type(lt)

    converter = UniversalTypeConverter()
    converter._logical_type_registry = registry

    result = converter.python_type_to_arrow_type(_MyCustomClass)
    expected_ext = lt.get_arrow_extension_type()
    assert result == expected_ext


def test_converter_falls_through_for_unregistered_type():
    """If type not in LogicalTypeRegistry, converter falls through to old system (int → int64)."""
    registry = LogicalTypeRegistry()
    converter = UniversalTypeConverter()
    converter._logical_type_registry = registry

    result = converter.python_type_to_arrow_type(int)
    assert result == pa.int64()


def test_converter_without_registry_unchanged():
    """With no _logical_type_registry set, converter behaves exactly as before."""
    converter = UniversalTypeConverter()
    assert converter.python_type_to_arrow_type(str) == pa.large_string()


def test_data_context_wires_registry_into_converter():
    """DataContext.__post_init__ wires logical_type_registry into type_converter."""
    from orcapod.contexts import get_default_context
    ctx = get_default_context()
    assert hasattr(ctx.type_converter, "_logical_type_registry")
    assert ctx.type_converter._logical_type_registry is ctx.logical_type_registry
```

- [ ] **Step 2: Run to confirm tests fail**

```bash
uv run pytest tests/test_semantic_types/test_universal_converter.py -k "logical_type_registry or data_context_wires" -v
```

Expected: FAIL — `UniversalTypeConverter` has no `_logical_type_registry` attribute.

- [ ] **Step 3: Add `_logical_type_registry` to `UniversalTypeConverter.__init__`**

In `src/orcapod/semantic_types/universal_converter.py`, update `__init__`:

```python
    def __init__(
        self,
        semantic_registry: SemanticTypeRegistry | None = None,
        datetime_timezone: typing.Literal["strict", "coerce_utc"] = "strict",
    ):
        """
        Args:
            semantic_registry: Optional registry of semantic type converters.
            datetime_timezone: How to handle naive (timezone-less) ``datetime``
                values when converting Python → Arrow.

                ``"strict"`` (default) — raise ``ValueError`` immediately so
                callers are forced to be explicit about timezone semantics.

                ``"coerce_utc"`` — silently attach ``timezone.utc`` to naive
                datetimes before writing to Arrow. Use this when you know that
                all naive datetimes in your data represent UTC.
        """
        self.semantic_registry = semantic_registry
        self._datetime_timezone = datetime_timezone
        self._logical_type_registry = None  # set by DataContext.__post_init__
        # ... rest of existing __init__ unchanged ...
```

- [ ] **Step 4: Insert the priority check in `_convert_python_to_arrow`**

In `src/orcapod/semantic_types/universal_converter.py`, find `_convert_python_to_arrow` (around line 411). After the `type_map` check and before the `semantic_registry` check, insert:

```python
        # Check LogicalTypeRegistry first — extension-type identity takes priority
        if self._logical_type_registry is not None:
            lt = self._logical_type_registry.get_by_python_type(python_type)
            if lt is not None:
                return lt.get_arrow_extension_type()
```

The surrounding context should look like:

```python
    def _convert_python_to_arrow(self, python_type: DataType) -> pa.DataType:
        """Core Python → Arrow type conversion logic."""
        type_map = _get_python_to_arrow_map()
        if python_type in type_map:
            return type_map[python_type]

        # Check LogicalTypeRegistry first — extension-type identity takes priority
        if self._logical_type_registry is not None:
            lt = self._logical_type_registry.get_by_python_type(python_type)
            if lt is not None:
                return lt.get_arrow_extension_type()

        # Check semantic registry for registered types
        if self.semantic_registry:
            converter = self.semantic_registry.get_converter_for_python_type(python_type)
            if converter:
                return converter.arrow_struct_type
        # ... rest unchanged ...
```

- [ ] **Step 5: Add `DataContext.__post_init__` in `contexts/core.py`**

In `src/orcapod/contexts/core.py`, add a `__post_init__` method to `DataContext`:

```python
    def __post_init__(self) -> None:
        """Wire components together after dataclass construction.

        Injects ``logical_type_registry`` into ``type_converter`` so that
        registered ``LogicalType`` instances take priority over the old
        shape-based ``semantic_registry`` at encoding time.
        """
        if hasattr(self.type_converter, "_logical_type_registry"):
            self.type_converter._logical_type_registry = self.logical_type_registry
```

- [ ] **Step 6: Run the new tests**

```bash
uv run pytest tests/test_semantic_types/test_universal_converter.py -k "logical_type_registry or data_context_wires" -v
```

Expected: All pass.

- [ ] **Step 7: Run the full test suite to confirm no regressions**

```bash
uv run pytest tests/ -v --tb=short -q
```

Expected: All previously passing tests still pass.

- [ ] **Step 8: Commit**

```bash
git add src/orcapod/semantic_types/universal_converter.py \
        src/orcapod/contexts/core.py \
        tests/test_semantic_types/test_universal_converter.py
git commit -m "feat(extension_types): wire LogicalTypeRegistry into UniversalTypeConverter and DataContext"
```

---

## Task 7: Add write-side trigger to `_FunctionPodBase`

**Files:**
- Modify: `src/orcapod/core/function_pod.py`
- Create: `tests/test_core/function_pod/test_write_side_registration.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_core/function_pod/test_write_side_registration.py`:

```python
"""Tests for write-side LogicalType auto-registration at function pod declaration.

These tests verify that _FunctionPodBase.__init__ triggers factory synthesis for
any non-native Python types in the pod's input/output schemas, and raises TypeError
at declaration time when no factory is registered.
"""

from __future__ import annotations

import dataclasses
import pathlib
import uuid as _uuid_module
from typing import Optional

import pyarrow as pa
import polars as pl
import pytest

from orcapod.contexts import get_default_context
from orcapod.core.data_function import PythonDataFunction
from orcapod.core.function_pod import FunctionPod
from orcapod.extension_types.protocols import LogicalTypeProtocol
from orcapod.extension_types.registry import (
    LogicalTypeRegistry,
    make_arrow_extension_type,
    make_polars_extension_type,
)


# ── Helpers ──────────────────────────────────────────────────────────────────

def _make_registry_with_factory(target_base: type) -> tuple[LogicalTypeRegistry, list]:
    """Return a registry with a factory for target_base and a call log."""
    call_log: list[type] = []

    def _make_lt(py_type: type) -> LogicalTypeProtocol:
        arrow_name = f"{py_type.__module__}.{py_type.__qualname__}.{_uuid_module.uuid4().hex[:6]}"
        ArrowExt = make_arrow_extension_type(arrow_name, pa.large_string())
        pl_dtype = pl.String

        class _PolarsExt(pl.BaseExtension):
            def __init__(self):
                super().__init__(arrow_name, pl_dtype, None)
            @classmethod
            def ext_from_params(cls, ext_name, storage_dtype, metadata_str):
                return cls()

        class _LT:
            logical_type_name = arrow_name
            python_type = py_type
            def get_arrow_extension_type(self): return ArrowExt()
            def get_polars_extension_type(self): return _PolarsExt()
            def python_to_storage(self, v): return str(v)
            def storage_to_python(self, v): return v

        return _LT()

    class _Factory:
        def reconstruct_from_arrow(self, name, storage, meta):
            return _make_lt(object)  # unused in these tests

        def create_for_python_type(self, python_type):
            call_log.append(python_type)
            return _make_lt(python_type)

    registry = LogicalTypeRegistry()
    registry.register_logical_type_factory(_Factory(), python_bases=[target_base])
    return registry, call_log


# ── Custom classes used in tests ─────────────────────────────────────────────

class _MyBase:
    pass


class _MyChild(_MyBase):
    pass


# ── Tests ────────────────────────────────────────────────────────────────────

def test_pod_declaration_triggers_factory_for_unregistered_class():
    """Declaring a FunctionPod with an unregistered type causes factory synthesis."""
    registry, call_log = _make_registry_with_factory(_MyBase)
    from orcapod.contexts.core import DataContext
    from orcapod.contexts import get_default_context
    # Build a context with our test registry
    base_ctx = get_default_context()
    ctx = DataContext(
        context_key="test",
        version="test",
        description="test",
        type_converter=base_ctx.type_converter,
        arrow_hasher=base_ctx.arrow_hasher,
        semantic_hasher=base_ctx.semantic_hasher,
        type_handler_registry=base_ctx.type_handler_registry,
        logical_type_registry=registry,
    )

    def my_func(x: _MyChild) -> str:
        return str(x)

    # Pod declaration should trigger factory for _MyChild
    pod = FunctionPod(
        func=my_func,
        output_keys=["result"],
        data_context=ctx,
    )
    assert _MyChild in call_log
    # The synthesized LogicalType is now in the registry
    assert registry.get_by_python_type(_MyChild) is not None


def test_pod_declaration_with_nested_list_type():
    """list[_MyChild] in the schema causes factory synthesis for _MyChild."""
    registry, call_log = _make_registry_with_factory(_MyBase)
    from orcapod.contexts.core import DataContext
    from orcapod.contexts import get_default_context
    base_ctx = get_default_context()
    ctx = DataContext(
        context_key="test",
        version="test",
        description="test",
        type_converter=base_ctx.type_converter,
        arrow_hasher=base_ctx.arrow_hasher,
        semantic_hasher=base_ctx.semantic_hasher,
        type_handler_registry=base_ctx.type_handler_registry,
        logical_type_registry=registry,
    )

    def my_func(items: list[_MyChild]) -> str:
        return ""

    FunctionPod(func=my_func, output_keys=["result"], data_context=ctx)
    assert _MyChild in call_log


def test_pod_declaration_native_types_no_factory_call():
    """Pods using only native types (int, str, etc.) never trigger factory lookup."""
    registry = LogicalTypeRegistry()

    class _NeverCalledFactory:
        def reconstruct_from_arrow(self, *a): ...
        def create_for_python_type(self, pt):
            raise AssertionError(f"factory called for {pt!r}")

    registry.register_logical_type_factory(
        _NeverCalledFactory(), python_bases=[object]
    )
    from orcapod.contexts.core import DataContext
    from orcapod.contexts import get_default_context
    base_ctx = get_default_context()
    ctx = DataContext(
        context_key="test", version="test", description="test",
        type_converter=base_ctx.type_converter,
        arrow_hasher=base_ctx.arrow_hasher,
        semantic_hasher=base_ctx.semantic_hasher,
        type_handler_registry=base_ctx.type_handler_registry,
        logical_type_registry=registry,
    )

    def my_func(x: int, y: str) -> float:
        return 0.0

    # Should not raise — int, str, float are native
    FunctionPod(func=my_func, output_keys=["result"], data_context=ctx)


def test_pod_declaration_raises_type_error_for_unhandled_class():
    """Pod with a type that has no registered factory raises TypeError at declaration."""
    registry = LogicalTypeRegistry()  # empty — no factories
    from orcapod.contexts.core import DataContext
    from orcapod.contexts import get_default_context
    base_ctx = get_default_context()
    ctx = DataContext(
        context_key="test", version="test", description="test",
        type_converter=base_ctx.type_converter,
        arrow_hasher=base_ctx.arrow_hasher,
        semantic_hasher=base_ctx.semantic_hasher,
        type_handler_registry=base_ctx.type_handler_registry,
        logical_type_registry=registry,
    )

    def my_func(x: _MyChild) -> str:
        return ""

    with pytest.raises(TypeError, match="No LogicalType or LogicalTypeFactory"):
        FunctionPod(func=my_func, output_keys=["result"], data_context=ctx)


def test_pod_declaration_already_registered_type_no_factory_call():
    """Pre-registered types are not passed to the factory."""
    registry, call_log = _make_registry_with_factory(_MyBase)
    # Pre-register _MyChild directly
    from orcapod.extension_types.registry import make_arrow_extension_type
    ArrowExt = make_arrow_extension_type(f"test.MyChild.{_uuid_module.uuid4().hex[:6]}", pa.large_string())

    class _PreLT:
        logical_type_name = f"test.{_uuid_module.uuid4().hex[:6]}"
        python_type = _MyChild
        def get_arrow_extension_type(self): return ArrowExt()
        def get_polars_extension_type(self):
            class P(pl.BaseExtension):
                def __init__(self): super().__init__(self.logical_type_name, pl.String, None)
                @classmethod
                def ext_from_params(cls, *a): return cls()
            return P()
        def python_to_storage(self, v): return str(v)
        def storage_to_python(self, v): return v

    registry.register_logical_type(_PreLT())
    from orcapod.contexts.core import DataContext
    from orcapod.contexts import get_default_context
    base_ctx = get_default_context()
    ctx = DataContext(
        context_key="test", version="test", description="test",
        type_converter=base_ctx.type_converter,
        arrow_hasher=base_ctx.arrow_hasher,
        semantic_hasher=base_ctx.semantic_hasher,
        type_handler_registry=base_ctx.type_handler_registry,
        logical_type_registry=registry,
    )

    def my_func(x: _MyChild) -> str:
        return ""

    FunctionPod(func=my_func, output_keys=["result"], data_context=ctx)
    # Factory was NOT called — _MyChild was already registered
    assert _MyChild not in call_log
```

- [ ] **Step 2: Run to confirm all fail**

```bash
uv run pytest tests/test_core/function_pod/test_write_side_registration.py -v
```

Expected: All FAIL — the trigger does not exist yet.

- [ ] **Step 3: Implement the trigger in `function_pod.py`**

Add imports at the top of `src/orcapod/core/function_pod.py` (with the existing imports):

```python
from orcapod.extension_types.type_utils import _extract_leaf_classes
from orcapod.extension_types.registry import LogicalTypeRegistry
```

Add the module-level constant and helper function before the `_FunctionPodBase` class definition:

```python
# Python types that Arrow handles natively — no LogicalType registration needed.
_ARROW_NATIVE_TYPES: frozenset[type] = frozenset({
    int, float, str, bytes, bool, type(None),
})


def _trigger_write_side_registration(
    input_schema: Schema,
    output_schema: Schema,
    registry: LogicalTypeRegistry | None,
) -> None:
    """Ensure a LogicalType is registered for every non-native leaf class in the schemas.

    Called once at pod declaration time. Recursively unwraps generic annotations
    (``list[T]``, ``dict[K, V]``, etc.) to find leaf classes. Skips Arrow-native
    types and already-registered types. Raises ``TypeError`` at declaration time
    if no factory is registered for a leaf class — this is intentional.

    Args:
        input_schema: The pod's input data schema (column name → Python type annotation).
        output_schema: The pod's output data schema.
        registry: The ``LogicalTypeRegistry`` from the pod's ``DataContext``.
            If ``None``, this function is a no-op.
    """
    if registry is None:
        return
    for schema in (input_schema, output_schema):
        for annotation in schema.values():
            for leaf_class in _extract_leaf_classes(annotation):
                if leaf_class in _ARROW_NATIVE_TYPES:
                    continue
                if registry.get_by_python_type(leaf_class) is not None:
                    continue  # already registered — O(1) cache hit
                registry.ensure_logical_type_for_python_class(leaf_class)
                # TypeError propagates if no factory matches — intentional hard error
```

In `_FunctionPodBase.__init__`, add the trigger call after `self._data_function = data_function`:

```python
        self._data_function = data_function
        _trigger_write_side_registration(
            data_function.input_data_schema,
            data_function.output_data_schema,
            self.data_context.logical_type_registry,
        )
```

- [ ] **Step 4: Run the new tests**

```bash
uv run pytest tests/test_core/function_pod/test_write_side_registration.py -v
```

Expected: All pass.

- [ ] **Step 5: Run the full test suite**

```bash
uv run pytest tests/ -v --tb=short -q
```

Expected: All previously passing tests still pass. The trigger is a no-op for native types and already-registered built-ins (Path, UPath, UUID), so existing pod tests are unaffected.

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/core/function_pod.py \
        tests/test_core/function_pod/test_write_side_registration.py
git commit -m "feat(extension_types): add write-side registration trigger in _FunctionPodBase.__init__"
```

---

## Self-Review Checklist

**Spec coverage:**

| Spec section | Covered by task |
|---|---|
| `reconstruct_from_arrow` rename | Task 1 |
| `create_for_python_type` new method | Task 2 |
| `_category_factories` rename, `_python_class_factories`, extended `register_logical_type_factory` | Task 3 |
| `ensure_logical_type_for_python_class` with unified MRO resolution, caching, TypeError | Task 4 |
| `_extract_leaf_classes` for complex nested annotations | Task 5 |
| `UniversalTypeConverter` priority check + `DataContext` wiring | Task 6 |
| `_trigger_write_side_registration`, `_ARROW_NATIVE_TYPES`, `_FunctionPodBase.__init__` call | Task 7 |
| Failure mode: hard TypeError at declaration time | Task 7 tests |
| Symmetry with read side (protocol contract documented) | Task 2 docstring |
| Built-in types unaffected | Task 7 tests (native types test, pre-registered test) |

**Type consistency across tasks:**
- `reconstruct_from_arrow` defined in Task 1, used in Task 3 (factory stub) — consistent ✓
- `create_for_python_type` defined in Task 2, tested in Task 4 (`python_type_calls`) — consistent ✓
- `_category_factories` introduced in Task 3, referenced in `ensure_logical_type_for_python_class` Task 4 — consistent ✓
- `_python_class_factories` introduced in Task 3, used in Task 4 — consistent ✓
- `_extract_leaf_classes` created in Task 5, imported in Task 7 — consistent ✓
- `_logical_type_registry` attribute name defined in Task 6, checked in Task 6's DataContext test — consistent ✓
- `LogicalTypeRegistry` import added to `function_pod.py` in Task 7 type annotation — consistent ✓
