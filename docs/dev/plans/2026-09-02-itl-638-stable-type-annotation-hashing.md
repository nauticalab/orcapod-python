# Stable Type Annotation Hashing (ITL-638) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace module-path-based type serialization in function pod signature hashing with stable canonical names from `LogicalTypeRegistry`, so that internal module reorganizations (e.g. `op.File` moving between subpackages) no longer invalidate cached function pod signatures.

**Architecture:** Three coordinated changes: (1) extend `canonical_annotation_str` in `hash_utils.py` to accept a `LogicalTypeRegistry` and resolve registered types to their stable `logical_type_name`; (2) fix `TypeObjectHandler` to use this registry for bare type objects; (3) fix `FunctionSignatureExtractor` to canonicalize both parameter annotation strings and return annotation through the same helper. Guarded by pre-fix golden-value fixtures that confirm the change only touches orcapod logical types (e.g. `op.File`) and leaves builtins untouched.

**Tech Stack:** Python 3.11+, pytest via `uv run`, `orcapod.logical_types.registry.LogicalTypeRegistry`, `orcapod.hashing`

**Branch:** `eywalker/itl-638-function-pod-signature-hashing-uses-full-type-import-paths`

---

## File Map

| File | Action | Purpose |
|------|--------|---------|
| `tests/test_hashing/generate_type_annotation_golden.py` | Create | Script to generate pre-fix golden hashes |
| `tests/test_hashing/hash_samples/type_annotation_golden.json` | Generate+commit | Frozen pre-fix hash values |
| `tests/test_hashing/test_type_annotation_golden.py` | Create | Golden comparison + diff regression tests |
| `src/orcapod/hashing/hash_utils.py` | Modify | Extend `canonical_annotation_str` with registry param |
| `src/orcapod/hashing/semantic_hashing/builtin_handlers.py` | Modify | Fix `TypeObjectHandler` + wire registry in `register_builtin_python_type_handlers` |
| `src/orcapod/hashing/semantic_hashing/function_info_extractors.py` | Modify | Fix `FunctionSignatureExtractor` params + returns |

---

### Task 1: Generate and commit pre-fix golden values

**Files:**
- Create: `tests/test_hashing/generate_type_annotation_golden.py`
- Generate: `tests/test_hashing/hash_samples/type_annotation_golden.json`

This task captures the current (broken) hash values before any fix. The JSON is committed as an immutable record.

- [ ] **Step 1: Write the golden generator script**

Create `tests/test_hashing/generate_type_annotation_golden.py`:

```python
"""Generate pre-fix golden hash values for type annotation hashing.

Run once (before the ITL-638 fix) with:
    uv run python tests/test_hashing/generate_type_annotation_golden.py

Outputs: tests/test_hashing/hash_samples/type_annotation_golden.json
"""
from __future__ import annotations

import inspect
import json
import pathlib
import typing
from uuid import UUID

import orcapod as op
from orcapod.hashing.defaults import get_default_semantic_hasher
from orcapod.hashing.semantic_hashing.function_info_extractors import (
    FunctionSignatureExtractor,
)
from orcapod.logical_types.file_type import File
from orcapod.logical_types.directory_type import Directory

GOLDEN_PATH = pathlib.Path(__file__).parent / "hash_samples" / "type_annotation_golden.json"

# ---------------------------------------------------------------------------
# Annotation types to hash individually (bare type objects)
# ---------------------------------------------------------------------------
ANNOTATION_CASES: dict[str, object] = {
    # Builtins
    "int": int,
    "str": str,
    "float": float,
    "bytes": bytes,
    # orcapod logical types
    "op.File": op.File,
    "op.Directory": op.Directory,
    "op.Path": op.Path,
    "op.UUID": UUID,
    # Generic aliases
    "list[int]": list[int],
    "dict[str, int]": dict[str, int],
    "list[op.File]": list[op.File],
    "dict[str, op.File]": dict[str, op.File],
    # Unions
    "int | str": int | str,
    "op.File | None": op.File | None,
    "Optional[op.File]": typing.Optional[op.File],
}

# ---------------------------------------------------------------------------
# Functions whose full hash_object(func) and extract_function_info output
# are both captured.
# ---------------------------------------------------------------------------

def fn_no_annotations():
    return None

def fn_builtin_param(x: int, y: str) -> float:
    return float(x)

def fn_orcapod_param(f: op.File) -> str:
    return str(f)

def fn_orcapod_return(s: str) -> op.File:
    return op.File(s)  # type: ignore[arg-type]

def fn_generic_orcapod(files: list[op.File]) -> list[str]:
    return []

def fn_union_orcapod(f: op.File | None) -> op.File | None:
    return f

def fn_mixed(f: op.File, n: int) -> op.Directory:
    return op.Directory(str(f))  # type: ignore[arg-type]

FUNCTION_CASES: dict[str, object] = {
    "fn_no_annotations": fn_no_annotations,
    "fn_builtin_param": fn_builtin_param,
    "fn_orcapod_param": fn_orcapod_param,
    "fn_orcapod_return": fn_orcapod_return,
    "fn_generic_orcapod": fn_generic_orcapod,
    "fn_union_orcapod": fn_union_orcapod,
    "fn_mixed": fn_mixed,
}


def main() -> None:
    hasher = get_default_semantic_hasher()
    extractor = FunctionSignatureExtractor(include_module=True, include_defaults=True)

    result: dict = {"annotation_hashes": {}, "function_info_hashes": {}, "function_object_hashes": {}}

    # Hash bare annotations through TypeObjectHandler
    for key, ann in ANNOTATION_CASES.items():
        result["annotation_hashes"][key] = hasher.hash_object(ann).to_string()

    # Hash FunctionSignatureExtractor output dict, and full function object
    for key, func in FUNCTION_CASES.items():
        info = extractor.extract_function_info(func)
        result["function_info_hashes"][key] = hasher.hash_object(info).to_string()
        result["function_object_hashes"][key] = hasher.hash_object(func).to_string()

    GOLDEN_PATH.parent.mkdir(parents=True, exist_ok=True)
    GOLDEN_PATH.write_text(json.dumps(result, indent=2) + "\n")
    print(f"Wrote golden values to {GOLDEN_PATH}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Run the generator to produce the golden JSON**

```bash
cd /path/to/orcapod-python
uv run python tests/test_hashing/generate_type_annotation_golden.py
```

Expected output:
```
Wrote golden values to tests/test_hashing/hash_samples/type_annotation_golden.json
```

- [ ] **Step 3: Inspect the golden JSON to verify it looks reasonable**

```bash
uv run python -c "
import json, pathlib
d = json.loads(pathlib.Path('tests/test_hashing/hash_samples/type_annotation_golden.json').read_text())
for section, entries in d.items():
    print(f'\\n=== {section} ===')
    for k, v in entries.items():
        print(f'  {k}: {v}')
"
```

Verify that:
- `op.File`, `op.Directory`, `op.Path`, `op.UUID` annotation hashes contain the full module path (e.g. the hash input is `"type:orcapod.logical_types.file_type.File"`)
- Builtin hashes (`int`, `str`, etc.) are present

- [ ] **Step 4: Commit the generator script and the golden JSON**

```bash
git add tests/test_hashing/generate_type_annotation_golden.py
git add tests/test_hashing/hash_samples/type_annotation_golden.json
git commit -m "test(hashing): add pre-fix golden hash values for type annotation hashing (ITL-638)"
```

---

### Task 2: Write golden-comparison test skeleton (failing)

**Files:**
- Create: `tests/test_hashing/test_type_annotation_golden.py`

This test loads the golden JSON and will assert (post-fix) that builtins are unchanged and orcapod types changed to canonical names. Writing it now (before the fix) means the "changed" assertions will fail until the fix lands.

- [ ] **Step 1: Write the test file**

Create `tests/test_hashing/test_type_annotation_golden.py`:

```python
"""Golden-value regression tests for type annotation hashing (ITL-638).

Two test classes:
  TestGoldenStability   -- builtins must be UNCHANGED after the fix.
  TestGoldenCanonical   -- orcapod logical types must produce NEW canonical hashes.

The golden JSON was generated pre-fix by generate_type_annotation_golden.py.
"""
from __future__ import annotations

import json
import pathlib
import typing
from uuid import UUID

import pytest

import orcapod as op
from orcapod.hashing.defaults import get_default_semantic_hasher
from orcapod.hashing.semantic_hashing.function_info_extractors import (
    FunctionSignatureExtractor,
)

GOLDEN_PATH = (
    pathlib.Path(__file__).parent / "hash_samples" / "type_annotation_golden.json"
)

# ---------------------------------------------------------------------------
# The same annotation and function cases as the generator — must stay in sync.
# ---------------------------------------------------------------------------

ANNOTATION_CASES: dict[str, object] = {
    "int": int,
    "str": str,
    "float": float,
    "bytes": bytes,
    "op.File": op.File,
    "op.Directory": op.Directory,
    "op.Path": op.Path,
    "op.UUID": UUID,
    "list[int]": list[int],
    "dict[str, int]": dict[str, int],
    "list[op.File]": list[op.File],
    "dict[str, op.File]": dict[str, op.File],
    "int | str": int | str,
    "op.File | None": op.File | None,
    "Optional[op.File]": typing.Optional[op.File],
}

# Annotation keys that are expected to change after the fix.
# Any key not in this set must have an UNCHANGED hash.
EXPECTED_CHANGED_KEYS: frozenset[str] = frozenset({
    "op.File",
    "op.Directory",
    "op.Path",
    "op.UUID",
    "list[op.File]",
    "dict[str, op.File]",
    "op.File | None",
    "Optional[op.File]",
})


def fn_no_annotations():
    return None

def fn_builtin_param(x: int, y: str) -> float:
    return float(x)

def fn_orcapod_param(f: op.File) -> str:
    return str(f)

def fn_orcapod_return(s: str) -> op.File:
    return op.File(s)  # type: ignore[arg-type]

def fn_generic_orcapod(files: list[op.File]) -> list[str]:
    return []

def fn_union_orcapod(f: op.File | None) -> op.File | None:
    return f

def fn_mixed(f: op.File, n: int) -> op.Directory:
    return op.Directory(str(f))  # type: ignore[arg-type]

FUNCTION_CASES: dict[str, object] = {
    "fn_no_annotations": fn_no_annotations,
    "fn_builtin_param": fn_builtin_param,
    "fn_orcapod_param": fn_orcapod_param,
    "fn_orcapod_return": fn_orcapod_return,
    "fn_generic_orcapod": fn_generic_orcapod,
    "fn_union_orcapod": fn_union_orcapod,
    "fn_mixed": fn_mixed,
}

# Functions expected to have different hashes after the fix.
EXPECTED_CHANGED_FUNCTIONS: frozenset[str] = frozenset({
    "fn_orcapod_param",
    "fn_orcapod_return",
    "fn_generic_orcapod",
    "fn_union_orcapod",
    "fn_mixed",
})


@pytest.fixture(scope="module")
def golden() -> dict:
    assert GOLDEN_PATH.exists(), (
        f"Golden file not found: {GOLDEN_PATH}. "
        "Run generate_type_annotation_golden.py first."
    )
    return json.loads(GOLDEN_PATH.read_text())


@pytest.fixture(scope="module")
def hasher():
    return get_default_semantic_hasher()


@pytest.fixture(scope="module")
def extractor():
    return FunctionSignatureExtractor(include_module=True, include_defaults=True)


class TestGoldenStability:
    """Builtins and non-orcapod annotations must hash identically before and after the fix."""

    def test_builtin_annotation_hashes_unchanged(self, golden, hasher):
        stable_keys = {k for k in ANNOTATION_CASES if k not in EXPECTED_CHANGED_KEYS}
        mismatches = {}
        for key in stable_keys:
            ann = ANNOTATION_CASES[key]
            current = hasher.hash_object(ann).to_string()
            expected = golden["annotation_hashes"][key]
            if current != expected:
                mismatches[key] = {"expected": expected, "current": current}
        assert not mismatches, (
            f"Unexpected hash changes in stable annotations:\n"
            + "\n".join(f"  {k}: {v}" for k, v in mismatches.items())
        )

    def test_builtin_function_hashes_unchanged(self, golden, hasher, extractor):
        stable_fns = {k for k in FUNCTION_CASES if k not in EXPECTED_CHANGED_FUNCTIONS}
        mismatches = {}
        for key in stable_fns:
            func = FUNCTION_CASES[key]
            info = extractor.extract_function_info(func)
            current = hasher.hash_object(info).to_string()
            expected = golden["function_info_hashes"][key]
            if current != expected:
                mismatches[key] = {"expected": expected, "current": current}
        assert not mismatches, (
            f"Unexpected hash changes in stable functions:\n"
            + "\n".join(f"  {k}: {v}" for k, v in mismatches.items())
        )


class TestGoldenCanonical:
    """orcapod logical types must produce NEW hashes (canonical name, not module path)."""

    def test_orcapod_annotation_hashes_changed(self, golden, hasher):
        """After the fix, orcapod type annotation hashes must DIFFER from golden."""
        unchanged = {}
        for key in EXPECTED_CHANGED_KEYS:
            if key not in ANNOTATION_CASES:
                continue
            ann = ANNOTATION_CASES[key]
            current = hasher.hash_object(ann).to_string()
            expected = golden["annotation_hashes"][key]
            if current == expected:
                unchanged[key] = current
        assert not unchanged, (
            f"Expected these annotation hashes to change after the fix, but they didn't:\n"
            + "\n".join(f"  {k}: {v}" for k, v in unchanged.items())
        )

    def test_orcapod_function_hashes_changed(self, golden, hasher, extractor):
        """After the fix, functions using orcapod types must DIFFER from golden."""
        unchanged = {}
        for key in EXPECTED_CHANGED_FUNCTIONS:
            func = FUNCTION_CASES[key]
            info = extractor.extract_function_info(func)
            current = hasher.hash_object(info).to_string()
            expected = golden["function_info_hashes"][key]
            if current == expected:
                unchanged[key] = current
        assert not unchanged, (
            f"Expected these function hashes to change after the fix, but they didn't:\n"
            + "\n".join(f"  {k}: {v}" for k, v in unchanged.items())
        )
```

- [ ] **Step 2: Verify the stability tests pass (pre-fix) and canonical tests fail**

```bash
uv run pytest tests/test_hashing/test_type_annotation_golden.py::TestGoldenStability -v
```

Expected: All PASS (hashes match golden before any fix)

```bash
uv run pytest tests/test_hashing/test_type_annotation_golden.py::TestGoldenCanonical -v
```

Expected: All FAIL (hashes still match golden — the fix hasn't landed yet)

- [ ] **Step 3: Commit**

```bash
git add tests/test_hashing/test_type_annotation_golden.py
git commit -m "test(hashing): add golden-diff regression tests for type annotation canonicalization (ITL-638)"
```

---

### Task 3: Extend `canonical_annotation_str` with registry support

**Files:**
- Modify: `src/orcapod/hashing/hash_utils.py`
- Test: `tests/test_hashing/test_hash_utils.py`

The existing `canonical_annotation_str` only handles union ordering. We extend it to accept an optional `LogicalTypeRegistry` and also handle generic alias recursion.

- [ ] **Step 1: Write failing tests for the extended function**

Add to `tests/test_hashing/test_hash_utils.py` (append to existing file — do not replace existing tests):

```python
# ---------------------------------------------------------------------------
# Tests for canonical_annotation_str with registry (ITL-638)
# ---------------------------------------------------------------------------
import typing
from uuid import UUID

import orcapod as op
from orcapod.contexts import get_default_logical_type_registry
from orcapod.hashing.hash_utils import canonical_annotation_str


class TestCanonicalAnnotationStrWithRegistry:
    """canonical_annotation_str resolves registered logical types to stable names."""

    @pytest.fixture
    def registry(self):
        return get_default_logical_type_registry()

    def test_builtin_type_unchanged(self, registry):
        assert canonical_annotation_str(int, registry) == "int"

    def test_builtin_str_unchanged(self, registry):
        assert canonical_annotation_str(str, registry) == "str"

    def test_registered_type_uses_logical_name(self, registry):
        result = canonical_annotation_str(op.File, registry)
        assert result == "orcapod.file"

    def test_registered_directory_uses_logical_name(self, registry):
        result = canonical_annotation_str(op.Directory, registry)
        assert result == "orcapod.directory"

    def test_registered_path_uses_logical_name(self, registry):
        import pathlib
        result = canonical_annotation_str(pathlib.Path, registry)
        assert result == "orcapod.path"

    def test_registered_uuid_uses_logical_name(self, registry):
        result = canonical_annotation_str(UUID, registry)
        assert result == "orcapod.uuid"

    def test_generic_list_of_registered_type(self, registry):
        result = canonical_annotation_str(list[op.File], registry)
        assert result == "list[orcapod.file]"

    def test_generic_dict_with_registered_value(self, registry):
        result = canonical_annotation_str(dict[str, op.File], registry)
        assert result == "dict[str, orcapod.file]"

    def test_union_with_registered_type(self, registry):
        result = canonical_annotation_str(op.File | None, registry)
        # Members sorted; NoneType sorts before orcapod.file
        assert result == "NoneType | orcapod.file"

    def test_optional_registered_type(self, registry):
        result = canonical_annotation_str(typing.Optional[op.File], registry)
        assert result == "NoneType | orcapod.file"

    def test_no_registry_fallback(self):
        """Without registry, behaviour is identical to the existing function."""
        import inspect
        result = canonical_annotation_str(op.File, None)
        assert result == inspect.formatannotation(op.File)

    def test_stable_across_calls(self, registry):
        r1 = canonical_annotation_str(op.File, registry)
        r2 = canonical_annotation_str(op.File, registry)
        assert r1 == r2
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
uv run pytest tests/test_hashing/test_hash_utils.py::TestCanonicalAnnotationStrWithRegistry -v
```

Expected: All FAIL (function not yet extended)

- [ ] **Step 3: Extend `canonical_annotation_str` in `hash_utils.py`**

Replace the existing `canonical_annotation_str` function (currently lines 35-58) with this extended version. Add the `TYPE_CHECKING` guard at the top of the file if not already present:

```python
# At top of file, inside the existing imports block:
from __future__ import annotations
import inspect
import types as _types
import typing
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from orcapod.logical_types.registry import LogicalTypeRegistry
```

Replace the `canonical_annotation_str` function body:

```python
def canonical_annotation_str(
    annotation: object,
    registry: "LogicalTypeRegistry | None" = None,
) -> str:
    """Return a stable, canonical string for a type annotation.

    Resolves types registered in *registry* (e.g. ``orcapod.File``) to their
    stable ``logical_type_name`` (e.g. ``"orcapod.file"``) so that internal
    module relocations do not change the string representation.

    For union types (both PEP 604 ``X | Y`` and ``typing.Union[X, Y]``),
    members are sorted byte-wise so that ``str | Path`` and ``Path | str``
    produce the same canonical string.

    For generic aliases (``list[X]``, ``dict[K, V]``), args are recursed with
    the same registry so nested orcapod types are also canonicalized.

    Non-union, non-generic types not found in the registry fall through to
    ``inspect.formatannotation``, preserving existing behaviour exactly.

    Args:
        annotation: A type annotation object.
        registry: Optional ``LogicalTypeRegistry``. When provided, registered
            logical types resolve to their stable ``logical_type_name``.

    Returns:
        A canonical string representation.
    """
    # Registered logical type: use stable canonical name (e.g. "orcapod.file")
    if registry is not None and isinstance(annotation, type):
        lt = registry.get_by_python_type(annotation)
        if lt is not None:
            return lt.logical_type_name

    # Union types (PEP 604 X | Y and typing.Union): sort members for order-independence
    if is_union_annotation(annotation):
        args = getattr(annotation, "__args__", ()) or ()
        member_strs = sorted(canonical_annotation_str(a, registry) for a in args)
        return " | ".join(member_strs)

    # Generic aliases (list[X], dict[K, V], typing.List[X], etc.): recurse args
    origin = getattr(annotation, "__origin__", None)
    if origin is not None and not is_union_annotation(annotation):
        args = getattr(annotation, "__args__", None) or ()
        origin_str = canonical_annotation_str(origin, registry)
        if args:
            args_str = ", ".join(canonical_annotation_str(a, registry) for a in args)
            return f"{origin_str}[{args_str}]"
        return origin_str

    return inspect.formatannotation(annotation)
```

- [ ] **Step 4: Run the new tests to confirm they pass**

```bash
uv run pytest tests/test_hashing/test_hash_utils.py::TestCanonicalAnnotationStrWithRegistry -v
```

Expected: All PASS

- [ ] **Step 5: Run the full `test_hash_utils.py` to confirm no regressions**

```bash
uv run pytest tests/test_hashing/test_hash_utils.py -v
```

Expected: All PASS

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/hashing/hash_utils.py tests/test_hashing/test_hash_utils.py
git commit -m "feat(hashing): extend canonical_annotation_str to resolve registered logical types (ITL-638)"
```

---

### Task 4: Fix `TypeObjectHandler` to use registry lookup

**Files:**
- Modify: `src/orcapod/hashing/semantic_hashing/builtin_handlers.py`
- Test: `tests/test_hashing/test_semantic_hasher.py`

- [ ] **Step 1: Write failing tests for registry-aware `TypeObjectHandler`**

Add to `tests/test_hashing/test_semantic_hasher.py` (append after the existing `TestTypeObjectHandler` class):

```python
class TestTypeObjectHandlerWithRegistry:
    """TypeObjectHandler uses stable canonical names for registered logical types."""

    @pytest.fixture
    def registry(self):
        from orcapod.contexts import get_default_logical_type_registry
        return get_default_logical_type_registry()

    @pytest.fixture
    def handler(self, registry):
        from orcapod.hashing.semantic_hashing.builtin_handlers import TypeObjectHandler
        return TypeObjectHandler(logical_type_registry=registry)

    def test_registered_type_returns_canonical_name(self, handler, hasher):
        import orcapod as op
        result = handler.handle(op.File, hasher)
        assert result == "type:orcapod.file"

    def test_registered_directory_returns_canonical_name(self, handler, hasher):
        import orcapod as op
        result = handler.handle(op.Directory, hasher)
        assert result == "type:orcapod.directory"

    def test_unregistered_type_falls_back_to_module_qualname(self, handler, hasher):
        result = handler.handle(int, hasher)
        assert result == "type:builtins.int"

    def test_custom_class_falls_back_to_module_qualname(self, handler, hasher):
        class _Local:
            pass
        result = handler.handle(_Local, hasher)
        assert "type:" in result
        assert "_Local" in result

    def test_hash_stable_across_calls(self, registry):
        """Hashing op.File twice with registry produces identical ContentHash."""
        from orcapod.hashing.semantic_hashing.builtin_handlers import (
            TypeObjectHandler,
            register_builtin_python_type_handlers,
        )
        from orcapod.hashing.semantic_hashing.type_handler_registry import (
            PythonTypeHandlerRegistry,
        )
        from orcapod.hashing.semantic_hashing.semantic_hasher import (
            SemanticAwarePythonHasher,
        )
        import orcapod as op

        reg = PythonTypeHandlerRegistry()
        register_builtin_python_type_handlers(reg, logical_type_registry=registry)
        h = SemanticAwarePythonHasher(hasher_id="test_v1", type_handler_registry=reg)
        assert h.hash_object(op.File) == h.hash_object(op.File)

    def test_simulated_module_relocation_stable(self, registry):
        """Relocating a class's __module__ does not change the hash if its
        logical_type_name is unchanged in the registry.
        """
        from orcapod.hashing.semantic_hashing.builtin_handlers import (
            register_builtin_python_type_handlers,
        )
        from orcapod.hashing.semantic_hashing.type_handler_registry import (
            PythonTypeHandlerRegistry,
        )
        from orcapod.hashing.semantic_hashing.semantic_hasher import (
            SemanticAwarePythonHasher,
        )
        import orcapod as op

        reg = PythonTypeHandlerRegistry()
        register_builtin_python_type_handlers(reg, logical_type_registry=registry)
        h = SemanticAwarePythonHasher(hasher_id="test_v1", type_handler_registry=reg)

        hash_before = h.hash_object(op.File)

        # Simulate module relocation by temporarily patching __module__
        original_module = op.File.__module__
        try:
            op.File.__module__ = "orcapod.extension_types.file_type"
            hash_after = h.hash_object(op.File)
        finally:
            op.File.__module__ = original_module

        assert hash_before == hash_after, (
            "Hash changed when op.File.__module__ was altered — "
            "registry lookup is not being used."
        )
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
uv run pytest tests/test_hashing/test_semantic_hasher.py::TestTypeObjectHandlerWithRegistry -v
```

Expected: All FAIL (`TypeObjectHandler.__init__` does not accept `logical_type_registry` yet)

- [ ] **Step 3: Update `TypeObjectHandler` in `builtin_handlers.py`**

Replace the `TypeObjectHandler` class (currently lines 83-96):

```python
class TypeObjectHandler:
    """Hasher for type objects (classes passed as values).

    Resolves types registered in *logical_type_registry* to their stable
    ``logical_type_name`` (e.g. ``"type:orcapod.file"`` for ``op.File``).
    Falls back to ``"type:<module>.<qualname>"`` for unregistered types.

    Args:
        logical_type_registry: Optional ``LogicalTypeRegistry``. When ``None``,
            the default context's registry is resolved lazily at call time,
            following the same pattern as ``ArrowTableHandler``.
    """

    def __init__(self, logical_type_registry: Any = None) -> None:
        self._logical_type_registry = logical_type_registry

    def _get_registry(self) -> Any:
        if self._logical_type_registry is not None:
            return self._logical_type_registry
        from orcapod.contexts import get_default_context
        return get_default_context().logical_type_registry

    def handle(self, obj: Any, hasher: "SemanticHasherProtocol") -> Any:
        if not isinstance(obj, type):
            raise TypeError(
                f"TypeObjectHandler: expected a type/class, got {type(obj)!r}"
            )
        registry = self._get_registry()
        lt = registry.get_by_python_type(obj)
        if lt is not None:
            return f"type:{lt.logical_type_name}"
        module: str = obj.__module__ or "<unknown>"
        qualname: str = obj.__qualname__
        return f"type:{module}.{qualname}"
```

- [ ] **Step 4: Run the new tests to confirm they pass**

```bash
uv run pytest tests/test_hashing/test_semantic_hasher.py::TestTypeObjectHandlerWithRegistry -v
```

Expected: All PASS

- [ ] **Step 5: Run the full `test_semantic_hasher.py` to confirm no regressions**

```bash
uv run pytest tests/test_hashing/test_semantic_hasher.py -v
```

Expected: All PASS

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/hashing/semantic_hashing/builtin_handlers.py \
        tests/test_hashing/test_semantic_hasher.py
git commit -m "feat(hashing): make TypeObjectHandler resolve logical types to canonical names (ITL-638)"
```

---

### Task 5: Fix `FunctionSignatureExtractor` — consistent canonicalization for params and returns

**Files:**
- Modify: `src/orcapod/hashing/semantic_hashing/function_info_extractors.py`
- Test: `tests/test_hashing/test_function_info_extractors.py`

Currently: params embed annotation string via `str(param)` (module path), returns stores raw type object. After fix: both use `canonical_annotation_str(annotation, registry)`.

- [ ] **Step 1: Write failing tests for registry-aware `FunctionSignatureExtractor`**

Add to `tests/test_hashing/test_function_info_extractors.py` (append after existing tests):

```python
class TestFunctionSignatureExtractorWithRegistry:
    """FunctionSignatureExtractor canonicalizes both param and return annotations."""

    @pytest.fixture
    def registry(self):
        from orcapod.contexts import get_default_logical_type_registry
        return get_default_logical_type_registry()

    @pytest.fixture
    def extractor(self, registry):
        from orcapod.hashing.semantic_hashing.function_info_extractors import (
            FunctionSignatureExtractor,
        )
        return FunctionSignatureExtractor(
            include_module=True,
            include_defaults=True,
            logical_type_registry=registry,
        )

    def test_return_annotation_is_canonical_string(self, extractor):
        """parts['returns'] must be a string, not a type object."""
        import orcapod as op

        def fn(s: str) -> op.File:
            ...

        info = extractor.extract_function_info(fn)
        assert isinstance(info["returns"], str), (
            f"Expected str, got {type(info['returns'])}: {info['returns']!r}"
        )
        assert info["returns"] == "orcapod.file"

    def test_param_annotation_is_canonical_string(self, extractor):
        """Parameter annotation in params string uses logical_type_name."""
        import orcapod as op

        def fn(f: op.File) -> str:
            ...

        info = extractor.extract_function_info(fn)
        assert "orcapod.file" in info["params"], (
            f"Expected 'orcapod.file' in params, got: {info['params']!r}"
        )
        assert "logical_types" not in info["params"], (
            f"Module path leaked into params: {info['params']!r}"
        )

    def test_generic_param_annotation_canonical(self, extractor):
        """list[op.File] in a parameter is canonicalized."""
        import orcapod as op

        def fn(files: list[op.File]) -> str:
            ...

        info = extractor.extract_function_info(fn)
        assert "orcapod.file" in info["params"]
        assert "logical_types" not in info["params"]

    def test_union_return_annotation_canonical(self, extractor):
        """op.File | None return is canonicalized."""
        import orcapod as op

        def fn(s: str) -> op.File | None:
            ...

        info = extractor.extract_function_info(fn)
        assert isinstance(info["returns"], str)
        assert "orcapod.file" in info["returns"]
        assert "logical_types" not in info["returns"]

    def test_builtin_annotations_unchanged(self, extractor):
        """Functions with only builtin annotations are unaffected."""
        def fn(x: int, y: str) -> float:
            ...

        info = extractor.extract_function_info(fn)
        assert "int" in info["params"]
        assert "str" in info["params"]
        assert info["returns"] == "float"

    def test_return_and_param_use_same_canonical_form(self, extractor):
        """op.File in param and op.File as return use the same canonical string."""
        import orcapod as op

        def fn_param(f: op.File) -> str:
            ...

        def fn_return(s: str) -> op.File:
            ...

        info_param = extractor.extract_function_info(fn_param)
        info_return = extractor.extract_function_info(fn_return)

        # Both should contain "orcapod.file" in their respective places
        assert "orcapod.file" in info_param["params"]
        assert info_return["returns"] == "orcapod.file"

    def test_simulated_relocation_stable(self, extractor):
        """Patching __module__ on op.File does not change the extracted info."""
        import orcapod as op

        def fn(f: op.File) -> op.File:
            ...

        info_before = extractor.extract_function_info(fn)

        original = op.File.__module__
        try:
            op.File.__module__ = "orcapod.extension_types.file_type"
            info_after = extractor.extract_function_info(fn)
        finally:
            op.File.__module__ = original

        assert info_before["params"] == info_after["params"]
        assert info_before["returns"] == info_after["returns"]

    def test_no_registry_behaves_like_before(self):
        """Without registry, returns is still a type object (pre-fix behavior)."""
        import orcapod as op
        from orcapod.hashing.semantic_hashing.function_info_extractors import (
            FunctionSignatureExtractor,
        )

        extractor_no_reg = FunctionSignatureExtractor(
            include_module=True, include_defaults=True, logical_type_registry=None
        )

        def fn(s: str) -> op.File:
            ...

        info = extractor_no_reg.extract_function_info(fn)
        # Without registry, returns is the raw type object (legacy behavior)
        assert isinstance(info["returns"], type)
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
uv run pytest tests/test_hashing/test_function_info_extractors.py::TestFunctionSignatureExtractorWithRegistry -v
```

Expected: All FAIL (`logical_type_registry` param not accepted, `returns` is a type object not a string)

- [ ] **Step 3: Update `FunctionSignatureExtractor` in `function_info_extractors.py`**

Replace the `FunctionSignatureExtractor` class entirely:

```python
class FunctionSignatureExtractor:
    """Extractor that uses the function signature for information extraction.

    Canonicalizes type annotations for both parameters and the return type
    using ``canonical_annotation_str``. When *logical_type_registry* is
    provided (or resolved lazily from the default context), registered
    orcapod types (e.g. ``op.File``) are serialized to their stable
    ``logical_type_name`` (e.g. ``"orcapod.file"``) rather than to their
    fully-qualified import path.

    This ensures that internal module reorganizations (e.g. moving
    ``File`` between subpackages) do not invalidate function pod caches.

    Args:
        include_module: Include ``func.__module__`` in the extracted info.
        include_defaults: Include default parameter values.
        logical_type_registry: Optional ``LogicalTypeRegistry``. When
            ``None``, the default context's registry is resolved lazily at
            call time, following the same pattern as ``ArrowTableHandler``.
    """

    def __init__(
        self,
        include_module: bool = True,
        include_defaults: bool = True,
        logical_type_registry: Any = None,
    ) -> None:
        self.include_module = include_module
        self.include_defaults = include_defaults
        self._logical_type_registry = logical_type_registry

    def _get_registry(self) -> Any:
        if self._logical_type_registry is not None:
            return self._logical_type_registry
        from orcapod.contexts import get_default_context
        return get_default_context().logical_type_registry

    # FIXME: Fix this implementation!!
    # BUG: Currently this is not using the input_types and output_types parameters
    def extract_function_info(
        self,
        func: Callable[..., Any],
        function_name: str | None = None,
        input_typespec: Schema | None = None,
        output_typespec: Schema | None = None,
    ) -> dict[str, Any]:
        if not callable(func):
            raise TypeError("Provided object is not callable")

        # Use eval_str=True so that string annotations produced by
        # ``from __future__ import annotations`` (PEP 563) are resolved to live
        # type objects before canonicalization.
        try:
            sig = inspect.signature(func, eval_str=True)
        except (NameError, TypeError, AttributeError, SyntaxError):
            sig = inspect.signature(func)

        registry = self._get_registry()
        parts: dict[str, Any] = {}

        if self.include_module and hasattr(func, "__module__"):
            parts["module"] = func.__module__

        parts["name"] = function_name or func.__name__

        param_strs = []
        for name, param in sig.parameters.items():
            param_str = str(param)
            annotation = param.annotation
            if annotation is not inspect.Parameter.empty:
                old_ann = inspect.formatannotation(annotation)
                new_ann = canonical_annotation_str(annotation, registry)
                if old_ann != new_ann:
                    # Replace ": <old_ann>" with ": <new_ann>" (first occurrence).
                    # The ": " prefix distinguishes the annotation from the default.
                    param_str = param_str.replace(f": {old_ann}", f": {new_ann}", 1)
            if not self.include_defaults and "=" in param_str:
                param_str = param_str.split("=")[0].strip()
            param_strs.append(param_str)

        parts["params"] = ", ".join(param_strs)

        if sig.return_annotation is not inspect.Signature.empty:
            # Store as canonical string (not raw type object) for consistency
            # with the params representation and for registry-stable hashing.
            parts["returns"] = canonical_annotation_str(
                sig.return_annotation, registry
            )

        return parts
```

Add the import of `canonical_annotation_str` at the top of `function_info_extractors.py` (it's already imported from `hash_utils`; verify the import line includes it):

```python
from orcapod.hashing.hash_utils import canonical_annotation_str, is_union_annotation
```

Also add `Any` to the imports if not present:

```python
from typing import Any, Literal
```

- [ ] **Step 4: Run the new tests to confirm they pass**

```bash
uv run pytest tests/test_hashing/test_function_info_extractors.py::TestFunctionSignatureExtractorWithRegistry -v
```

Expected: All PASS

- [ ] **Step 5: Run the full `test_function_info_extractors.py` to confirm no regressions**

```bash
uv run pytest tests/test_hashing/test_function_info_extractors.py -v
```

Expected: All PASS

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/hashing/semantic_hashing/function_info_extractors.py \
        tests/test_hashing/test_function_info_extractors.py
git commit -m "feat(hashing): canonicalize param and return annotations in FunctionSignatureExtractor (ITL-638)"
```

---

### Task 6: Wire registry into `register_builtin_python_type_handlers`

**Files:**
- Modify: `src/orcapod/hashing/semantic_hashing/builtin_handlers.py`

Pass the `LogicalTypeRegistry` through to both `TypeObjectHandler` and `FunctionSignatureExtractor` so callers who already have a registry don't rely on lazy context lookup.

- [ ] **Step 1: Update `register_builtin_python_type_handlers` signature and body**

In `builtin_handlers.py`, update the function signature (currently around line 415):

```python
def register_builtin_python_type_handlers(
    registry: "HandlerRegistryProtocol",
    file_hasher: Any = None,
    function_info_extractor: Any = None,
    arrow_hasher: "ArrowHasherProtocol | None" = None,
    directory_hasher: Any = None,
    logical_type_registry: Any = None,
) -> None:
    """Register all built-in semantic hashers into *registry*.

    Args:
        registry: The ``HandlerRegistryProtocol`` instance to populate.
        file_hasher: Optional ``FileContentHasherProtocol`` for file content hashing.
            Defaults to ``FileHasher(sha256)``.
        function_info_extractor: Optional ``FunctionInfoExtractorProtocol``.
            Defaults to ``FunctionSignatureExtractor``.
        arrow_hasher: Optional ``ArrowHasherProtocol`` for nested table hashing.
            When ``None``, lazy resolution via the default context is used.
        directory_hasher: Optional ``DirectoryHasherProtocol`` for directory tree hashing.
            Defaults to ``BasicDirectoryHasher(sha256)``.
        logical_type_registry: Optional ``LogicalTypeRegistry`` forwarded to
            ``TypeObjectHandler`` and ``FunctionSignatureExtractor`` for stable
            canonical-name resolution. When ``None``, both handlers resolve the
            default context's registry lazily at call time.
    """
```

Then inside the function body, update the two relevant instantiation lines:

```python
    # Replace the existing FunctionSignatureExtractor instantiation:
    if function_info_extractor is None:
        from orcapod.hashing.semantic_hashing.function_info_extractors import (
            FunctionSignatureExtractor,
        )
        function_info_extractor = FunctionSignatureExtractor(
            include_module=True,
            include_defaults=True,
            logical_type_registry=logical_type_registry,
        )

    # Replace the existing TypeObjectHandler registration line:
    registry.register(type, TypeObjectHandler(logical_type_registry=logical_type_registry))
```

All other lines in the function body remain unchanged.

- [ ] **Step 2: Run the full hashing test suite to confirm nothing is broken**

```bash
uv run pytest tests/test_hashing/ -v --tb=short 2>&1 | tail -40
```

Expected: All PASS

- [ ] **Step 3: Commit**

```bash
git add src/orcapod/hashing/semantic_hashing/builtin_handlers.py
git commit -m "feat(hashing): thread LogicalTypeRegistry through register_builtin_python_type_handlers (ITL-638)"
```

---

### Task 7: Run golden-diff tests and verify impact is limited to orcapod types

At this point all three changes are live. The golden-diff tests from Task 2 should now behave correctly.

- [ ] **Step 1: Run `TestGoldenStability` — builtins must be unchanged**

```bash
uv run pytest tests/test_hashing/test_type_annotation_golden.py::TestGoldenStability -v
```

Expected: All PASS (builtin hashes unchanged — `int`, `str`, `list[int]`, `dict[str, int]`, `int | str`, pure-builtin functions)

- [ ] **Step 2: Run `TestGoldenCanonical` — orcapod types must have changed**

```bash
uv run pytest tests/test_hashing/test_type_annotation_golden.py::TestGoldenCanonical -v
```

Expected: All PASS (orcapod type hashes differ from golden — `op.File`, `op.Directory`, `op.Path`, `op.UUID` and any function using them)

- [ ] **Step 3: If any `TestGoldenStability` test fails, investigate before proceeding**

A failure in `TestGoldenStability` means an unintended hash change. Diagnose by printing the `info` dict from `extract_function_info` for the failing case and comparing to what the golden fixture captured. Fix the root cause; do not adjust the golden file.

- [ ] **Step 4: Run the full test suite**

```bash
uv run pytest tests/ -v --tb=short -q 2>&1 | tail -60
```

Expected: All PASS

- [ ] **Step 5: Commit the final test results note**

No code change needed here; just record that validation passed.

---

### Task 8: Create PR

- [ ] **Step 1: Push the branch**

```bash
git push -u origin eywalker/itl-638-function-pod-signature-hashing-uses-full-type-import-paths
```

- [ ] **Step 2: Open the PR**

```bash
gh pr create \
  --base main \
  --title "fix(hashing): use stable canonical names for type annotation hashing (ITL-638)" \
  --body "$(cat <<'EOF'
## Summary

Fixes ITL-638. Function pod signature hashing previously encoded types by their fully-qualified import path (e.g. `type:orcapod.logical_types.file_type.File`), causing cache invalidation whenever a type moved between internal modules.

Changes:
- **`hash_utils.py`**: Extended `canonical_annotation_str` with an optional `LogicalTypeRegistry` parameter. Registered orcapod types resolve to their stable `logical_type_name` (e.g. `"orcapod.file"`). Generic aliases (`list[op.File]`) and union types (`op.File | None`) are recursed so nested types are also canonicalized.
- **`builtin_handlers.py`**: `TypeObjectHandler` now accepts a `LogicalTypeRegistry` (lazy fallback to default context). Registered types return `"type:orcapod.file"` instead of `"type:orcapod.logical_types.file_type.File"`.
- **`function_info_extractors.py`**: `FunctionSignatureExtractor` accepts a `LogicalTypeRegistry` and uses `canonical_annotation_str` for **both** parameter annotation strings and the return annotation (previously stored as a raw type object — now consistently a canonical string).
- **`register_builtin_python_type_handlers`**: Threads `logical_type_registry` through to both handlers.

**Backward compatibility:** One-time cache invalidation for any function pod whose signature contained `op.File`, `op.Directory`, `op.Path`, or `op.UUID`. This is accepted (pre-v0.1.0, per CLAUDE.md). Builtins and non-orcapod types are unaffected (verified by golden-diff tests).

## Test plan

- [ ] `TestGoldenStability` passes — builtin annotation hashes unchanged
- [ ] `TestGoldenCanonical` passes — orcapod type hashes differ from pre-fix golden
- [ ] `TestTypeObjectHandlerWithRegistry` passes — includes module-relocation stability test
- [ ] `TestFunctionSignatureExtractorWithRegistry` passes — params and returns use same canonical form
- [ ] `TestCanonicalAnnotationStrWithRegistry` passes — registry-aware helper works for all annotation forms
- [ ] Full test suite passes

Fixes ITL-638
EOF
)"
```

---

## Self-Review Checklist

**Spec coverage:**
- ✅ Locate where hashing serializes types → Tasks 3–5 (all three sites)
- ✅ Replace path-based identity with canonical name → TypeObjectHandler (Task 4) + FunctionSignatureExtractor (Task 5)
- ✅ Canonical source of truth → `LogicalTypeRegistry.logical_type_name` threaded via injection
- ✅ Consistent treatment of param and return annotations → Task 5 uses same `canonical_annotation_str` for both
- ✅ Regression tests for module-relocation scenario → `test_simulated_relocation_stable` in Tasks 4 and 5
- ✅ Golden-value capture + diff assertion → Tasks 1 and 7
- ✅ Backward-compat decision documented → PR body; one-time bust accepted

**Placeholder scan:** None found.

**Type consistency:**
- `canonical_annotation_str(annotation, registry)` — consistent across Tasks 3, 5
- `TypeObjectHandler(logical_type_registry=...)` — consistent across Tasks 4, 6
- `FunctionSignatureExtractor(logical_type_registry=...)` — consistent across Tasks 5, 6
- `register_builtin_python_type_handlers(..., logical_type_registry=...)` — Task 6 only
