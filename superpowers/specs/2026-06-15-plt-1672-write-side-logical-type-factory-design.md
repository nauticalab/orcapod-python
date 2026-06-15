# PLT-1672: Write-Side Logical Type Factory Design

**Issue:** PLT-1672
**Date:** 2026-06-15
**Project:** Orcapod: Arrow/Polars Extension Type Semantic Type System
**Depends on:** PLT-1668 (LogicalType/LogicalTypeRegistry — already on `extension-type-system`)

---

## Overview

The `LogicalTypeFactory` mechanism today only fires on the **read path**: when the database
read hook encounters an Arrow extension type with an unknown name, it dispatches to a factory
keyed by the `"category"` string in the extension metadata JSON.

The **write path** has no equivalent. When a user declares a function pod whose input or output
is typed with a Python class that is not yet registered in `LogicalTypeRegistry`, there is no
mechanism to detect this and auto-register a `LogicalType` on the fly. This breaks the
ergonomic goal of "declare a dataclass, use it."

This spec adds a second factory dispatch axis — **Python-class-keyed** — and wires a
write-side trigger at function pod declaration time.

---

## Design decisions summary

| Question | Decision |
|---|---|
| Factory protocol extension | Add `create_for_python_type(python_type)` as a new method; rename existing `create_logical_type` → `reconstruct_from_arrow` |
| Registration API | Extend `register_logical_type_factory` signature to accept both `category` and `python_bases` in one call |
| Trigger location | `_FunctionPodBase.__init__()` — at pod declaration time |
| Failure mode | Hard `TypeError` at declaration time if no factory matches |
| MRO resolution | Unified MRO walk across both concrete types and factory keys; most-specific wins; concrete beats factory at same MRO level |

---

## Section 1: Protocol changes — `LogicalTypeFactoryProtocol`

**File:** `src/orcapod/extension_types/protocols.py`

The existing `create_logical_type` method is **renamed** to `reconstruct_from_arrow` to make its
role unambiguous (read-path reconstructor from Arrow schema). A new `create_for_python_type`
method is added for the write path.

```python
class LogicalTypeFactoryProtocol(Protocol):

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
        on a subsequent read, ensuring write → Parquet → read consistency.

        Args:
            python_type: The concrete Python class to synthesize a LogicalType for.

        Returns:
            A fully constructed ``LogicalTypeProtocol`` ready for registration.

        Raises:
            ValueError: If this factory cannot construct a type for the given class.
        """
        ...
```

**Breaking change:** `create_logical_type` → `reconstruct_from_arrow`. The single internal call
site in `registry.ensure_extension_type()` is updated. Any existing factory implementations
(none yet in the codebase beyond tests) must update the method name.

The existing test stub in `test_protocols.py` (`_StubFactory.create_logical_type`) is updated
to `reconstruct_from_arrow` and a conformance test for `create_for_python_type` is added.

---

## Section 2: Registry API changes — `LogicalTypeRegistry`

**File:** `src/orcapod/extension_types/registry.py`

### New internal state

```python
class LogicalTypeRegistry:
    def __init__(self, logical_types=None):
        self._by_logical_name: dict[str, LogicalTypeProtocol] = {}
        self._by_arrow_name: dict[str, LogicalTypeProtocol] = {}
        self._by_python_type: dict[type, LogicalTypeProtocol] = {}
        self._category_factories: dict[str, LogicalTypeFactoryProtocol] = {}       # was _factories
        self._python_class_factories: dict[type, LogicalTypeFactoryProtocol] = {}  # new
```

`_factories` is renamed to `_category_factories` for clarity. No external API references it
directly.

### `register_logical_type_factory` — extended signature

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
        category: If given, registers factory as the read-side handler for
            Arrow extension types whose metadata contains this category string.
            Raises ``ValueError`` if a different factory is already registered
            for this category.
        python_bases: Zero or more Python base classes. Registers factory as
            the write-side handler for each. The factory's
            ``create_for_python_type`` will be called when a pod declares a
            type that is a subclass of one of these bases and no concrete
            ``LogicalType`` is yet registered for that type.
            Raises ``ValueError`` if a different factory is already registered
            for a given base.

    At least one of ``category`` or ``python_bases`` must be provided.
    Registering the same factory object twice for the same key is a no-op.
    """
```

**Signature change:** `factory` becomes the first positional argument and `category` becomes
keyword-only. Existing call sites using `register_logical_type_factory("Dataclass", factory)`
(positional) update to `register_logical_type_factory(factory, category="Dataclass")`.

A typical dual-axis registration (as the dataclass factory will use):

```python
registry.register_logical_type_factory(
    dataclass_factory,
    category="Dataclass",
    python_bases=[DataclassSentinelABC],
)
```

### `ensure_extension_type` — one-line update

The internal call changes from `factory.create_logical_type(...)` to
`factory.reconstruct_from_arrow(...)`. No other logic changes.

### New: `ensure_logical_type_for_python_class`

```python
def ensure_logical_type_for_python_class(
    self,
    python_type: type,
) -> LogicalTypeProtocol:
    """Ensure a LogicalType exists for python_type, synthesizing via factory if needed.

    This is the write-side counterpart to ``ensure_extension_type`` (the read-side
    trigger). It is called at function pod declaration time for every non-native
    type in the pod's input and output schemas.

    Resolution algorithm (unified MRO walk):

    1. Walk ``python_type.__mro__``. At each MRO step, check:
       - ``_by_python_type`` for a concrete registered ``LogicalType``
       - ``_python_class_factories`` for a registered factory
       Track the first (most-specific) hit in each dict separately.

    2. After the MRO walk, if no factory was found in step 1, do a fallback
       ``issubclass`` scan over ``_python_class_factories`` keys. This catches
       ABCs that use ``__subclasshook__`` for structural dispatch (e.g. a
       ``_DataclassSentinelABC`` whose hook returns ``is_dataclass(C)``).

    3. Resolution rule:
       - If only a concrete type found → return it immediately (O(1) after first hit).
       - If only a factory found → call ``factory.create_for_python_type(python_type)``,
         register the result via ``register_logical_type()``, return it.
         Registration caches in ``_by_python_type[python_type]`` — next lookup is O(1).
       - If both found at the same MRO level (same class in MRO) → concrete wins.
       - If concrete is more specific (lower MRO index) → return concrete.
       - If factory is more specific (lower MRO index) → synthesize and register.

    4. If nothing found (no concrete type, no factory): raise ``TypeError``.

    Args:
        python_type: The Python class to resolve.

    Returns:
        The registered or newly synthesized ``LogicalTypeProtocol``.

    Raises:
        TypeError: If no ``LogicalType`` and no factory is found for ``python_type``.
            Message includes guidance on how to register a factory.
    """
```

**Caching:** once a factory synthesizes a `LogicalType` for a concrete class and
`register_logical_type` stores it in `_by_python_type[python_type]`, all future calls for that
exact class are O(1) exact-match dict lookups. No factory call, no MRO walk. This per-process
cache is intentionally shared with the read-side cache — they are one and the same
`_by_python_type` dict.

---

## Section 3: Trigger point — `_FunctionPodBase.__init__()`

**File:** `src/orcapod/core/function_pod.py`

A module-level helper is added and called from `_FunctionPodBase.__init__()` after the data
function is assigned:

```python
# Types that Arrow handles natively without a LogicalType
_ARROW_NATIVE_TYPES: frozenset[type] = frozenset({
    int, float, str, bytes, bool, type(None),
})


def _trigger_write_side_registration(
    input_schema: Schema,
    output_schema: Schema,
    registry: LogicalTypeRegistry | None,
) -> None:
    """Walk pod schemas and ensure a LogicalType is registered for every non-native type.

    Called once at pod declaration time. Arrow-native types (int, str, etc.) are
    skipped. Already-registered types are skipped via a fast O(1) dict check.
    Unregistered non-native types trigger factory synthesis. Raises TypeError if
    no factory is found — this is an intentional hard error at declaration time.

    Args:
        input_schema: The pod's input data schema (column name → Python type).
        output_schema: The pod's output data schema.
        registry: The LogicalTypeRegistry from the pod's DataContext. No-op if None.
    """
    if registry is None:
        return
    for schema in (input_schema, output_schema):
        for python_type in schema.values():
            if python_type in _ARROW_NATIVE_TYPES:
                continue
            if registry.get_by_python_type(python_type) is not None:
                continue  # already registered — O(1) cache hit, skip MRO walk
            registry.ensure_logical_type_for_python_class(python_type)
            # TypeError propagates if no factory matches — intentional
```

In `_FunctionPodBase.__init__()`:

```python
self._data_function = data_function
_trigger_write_side_registration(
    data_function.input_data_schema,
    data_function.output_data_schema,
    self.data_context.logical_type_registry,
)
```

**Single chokepoint:** every function pod, whether `FunctionPod` or `FunctionNode`, is
constructed through `_FunctionPodBase.__init__()`. There is no other code path to reach.

---

## Section 4: Failure modes

**No factory found at pod declaration time:**

```
TypeError: No LogicalType or LogicalTypeFactory is registered for type
'myapp.models.Foo'.
To handle this type, register a factory for its base class on the registry:
  registry.register_logical_type_factory(factory, python_bases=[<base of Foo>])
Or register a concrete LogicalType directly:
  registry.register_logical_type(my_logical_type)
```

This error is raised from `ensure_logical_type_for_python_class` and surfaces at the
`_FunctionPodBase.__init__()` call site. There is no fallback, no implicit pickle, no silent
pass-through. The failure is deliberate and loud.

**Registry is None:** `_trigger_write_side_registration` is a no-op. This handles contexts
without type registration (e.g. test environments that construct pods without a full
DataContext).

---

## Section 5: Symmetry with the read side

By protocol contract, `create_for_python_type(T)` must produce a `LogicalType` whose Arrow
extension name and metadata JSON are identical to what `reconstruct_from_arrow` expects to
receive when reading that data back. Concretely for the dataclass factory:

| Direction | Method | Extension name | Metadata |
|---|---|---|---|
| Write | `create_for_python_type(MyEvent)` | `"myapp.models.MyEvent"` | `{"category": "Dataclass"}` |
| Read | `reconstruct_from_arrow("myapp.models.MyEvent", struct_type, {"category": "Dataclass"})` | same | same |

The registry routes the read path via `_category_factories["Dataclass"]` and the write path via
`_python_class_factories[DataclassSentinelABC]` — the same factory object, different dispatch
keys. Round-trip consistency is enforced by integration tests (write → Parquet → read), not by
the registry itself.

---

## Section 6: Built-in types (Path, UPath, UUID) — confirmed unaffected

Built-ins are registered as concrete `LogicalType` instances against their exact Python types
(`pathlib.Path`, `upath.UPath`, `uuid.UUID`) in the `DataContext` at startup. When a pod
declares a `pathlib.Path`-typed column:

1. `_ARROW_NATIVE_TYPES` check: fails (Path is not a primitive)
2. `registry.get_by_python_type(pathlib.Path)` → hits exact match → skips factory
3. No factory involved, no MRO walk, no synthesis

Built-ins continue to work exactly as before. ✓

---

## Section 7: What this issue does NOT implement

The following are explicitly deferred:

- **Dataclass factory (`orcapod.dataclass`):** PLT-1657 implements the concrete factory and
  registers it via `register_logical_type_factory(factory, category="Dataclass", python_bases=[DataclassSentinelABC])`.
  PLT-1657 also defines `DataclassSentinelABC` (the ABC with `__subclasshook__` that returns
  `is_dataclass(C)`). PLT-1672 defines the slot; PLT-1657 fills it.
- **Pydantic factory:** future issue. The framework accommodates it by design.
- **Picklable factory as fallback:** deferred. The failure-mode section deliberately makes
  no-match a hard error for now.

---

## Implementation scope

All changes are contained to three files:

| File | Change |
|---|---|
| `src/orcapod/extension_types/protocols.py` | Rename `create_logical_type` → `reconstruct_from_arrow`; add `create_for_python_type` |
| `src/orcapod/extension_types/registry.py` | Rename `_factories` → `_category_factories`; extend `register_logical_type_factory` signature; update `ensure_extension_type` call site; add `ensure_logical_type_for_python_class` |
| `src/orcapod/core/function_pod.py` | Add `_ARROW_NATIVE_TYPES`, `_trigger_write_side_registration`; call from `_FunctionPodBase.__init__()` |

Tests updated / added:

| File | Change |
|---|---|
| `tests/test_extension_types/test_protocols.py` | Update `_StubFactory.create_logical_type` → `reconstruct_from_arrow`; add `create_for_python_type` stub and conformance test |
| `tests/test_extension_types/test_registry.py` | Update `register_logical_type_factory` call sites; add tests for `ensure_logical_type_for_python_class` (MRO walk, factory synthesis, caching, TypeError) |
| `tests/test_core/function_pod/test_write_side_registration.py` | New: end-to-end tests verifying pod declaration triggers factory synthesis for unregistered types; hard error when no factory matches |

---

## PLT-1660 cleanup items (deferred)

None — this issue adds new code only, consistent with the parallel-build strategy.
